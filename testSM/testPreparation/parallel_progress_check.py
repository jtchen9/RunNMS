"""Offline concurrent preparation tests. No installed config, Redis, or robots.
Run: python testSM/testPreparation/parallel_progress_check.py
Requires the existing step5_check.py and fakeredis.
"""
import asyncio
import contextlib
import copy
from datetime import datetime
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from step5_check import Base,module,NOW,A,B
import nms_preparation_gate as gate
import nms_preparation_runtime as runtime
import nms_preparation_registration as registration
import nms_preparation_positioning as positioning
from nms_preparation_batch import PathBarrier,run_batch,segment_distance,BatchFailure
from nms_preparation_lifecycle import Conflict


class Port:
    final_tolerance_m=.3
    def __init__(self,targets):
        self.targets={p['scanner']:dict(p) for p in targets}
        self.data={r:dict(state='s0idle',stop=False,pose=dict(location_ok=True,source='apriltag',x_m=p['x_m'],y_m=p['y_m']-.5,heading_deg=270.),evidence={}) for r,p in self.targets.items()}
        self.scopes={};self.commands=[];self.moves=set();self.all_moves=asyncio.Event();self.serial=0;self.tasks=[]
    def local_ts(self):return NOW
    def open(self,r,*,scope,owns_attempt):
        assert owns_attempt() and gate.execution_allowed(r)
        self.scopes[r]=scope
    def close(self,r,*,scope):
        if self.scopes.get(r)==scope:del self.scopes[r]
    def snapshot(self,r):return copy.deepcopy(self.data[r])
    def issue(self,r,action,args,*,scope,owns_attempt):
        assert owns_attempt() and gate.execution_allowed(r)
        self.commands.append((r,action));self.data[r]['state']='s1waiting_report'
        asyncio.get_running_loop().call_soon(self.finish_report,r,scope)
        return {'status':'ok'}
    def finish_report(self,r,scope):
        self.serial+=1;d=self.data[r];ts=datetime.strptime(NOW,'%Y-%m-%d-%H:%M:%S').timestamp()
        report=dict(last_command='mobility.report.location',last_exec_status='completed',last_command_received_ts=ts,last_command_finished_ts=ts+.1,
                    last_location_result=dict(ok=True,snapshots={'front':f'front-{self.serial}','rear':f'rear-{self.serial}'}))
        d['evidence']=dict(scope=scope,solve_id=str(self.serial),pose=copy.deepcopy(d['pose']),report=report,issued_at=NOW,solved_at=NOW)
        d['state']='s0idle'
    def check_path(self,r,start,target):return {'path_ok':True}
    def move(self,r,target,*,initial_evidence,scope,owns_attempt):
        assert owns_attempt() and gate.execution_allowed(r)
        self.commands.append((r,'mobility.move'));self.moves.add(r);self.data[r]['state']='s1waiting_report'
        if self.moves==set(self.targets):self.all_moves.set()
        async def finish():
            await self.all_moves.wait()
            self.data[r]['pose'].update(target);self.data[r]['state']='s0idle'
        self.tasks.append(asyncio.create_task(finish()))
        return {'status':'ok'}


class SweetPath(Base):
    def run_robots(self,count):
        names=[f'twin-scout-{s}' for s in ('alpha','bravo','charlie','delta')[:count]]
        targets=[dict(scanner=r,x_m=2.+2*i,y_m=3.,heading_deg=270.,row_number=i+2) for i,r in enumerate(names)]
        def replace(d):
            d['runtime']['manifest']['initial_positions']=targets
            d['runtime']['manifest']['commands']=[dict(scanner=r,t_offset_sec=0,row_number=i+2,category='scan',action='scan.start',args={}) for i,r in enumerate(names)]
            return d
        self.st.owned_edit(self.uid,replace);self.begin();self.mark_registered()
        class Lock:
            def acquire(self,**kw):return True
            def owned(self):return True
            def release(self):pass
        async def health(*a,**kw):return {r:'PASS' for r in names}
        async def exercise():
            port=Port(targets)
            modules={'nms_static_readiness':module('nms_static_readiness',check_targets=health),
                     'nms_experiment_completion':module('nms_experiment_completion',lifecycle_lock=Lock,start_registration=lambda *a:None),
                     'm8mobility_trace':module('m8mobility_trace',experiment_trace_boundary=contextlib.nullcontext)}
            with patch.dict(sys.modules,modules),patch.object(runtime,'store',return_value=self.st),patch.object(runtime,'reset'),patch.object(runtime,'freeze'),patch.object(runtime,'now',return_value=NOW),patch.object(positioning,'NmsPositioningPort',return_value=port),patch.object(registration,'compile_operations',side_effect=self.ops):
                await asyncio.wait_for(runtime._attempt(self.uid,self.d),4)
            await asyncio.gather(*port.tasks)
            return port
        port=asyncio.run(exercise());doc=self.st.load(self.uid)
        self.assertTrue(doc['runtime']['released']);self.assertEqual(set(doc['runtime']['ready']),set(names))
        self.assertFalse(port.scopes)
        moves=[i for i,(_,a) in enumerate(port.commands) if a=='mobility.move']
        self.assertEqual(len(moves),count)
        # Every robot's first measurement and pre-move report precede ALL moves.
        self.assertEqual(moves[0],count*2)
        # Fake moves cannot finish until ALL robots have received their move.
        self.assertLess(moves[-1],next(i for i,(_,a) in enumerate(port.commands) if i>moves[0] and a=='mobility.report.location'))
        queue=doc['state_queue'];seen=set()
        for s in queue:
            prep=s['preparation'];seen.add(prep.get('step_number'))
            if s['state']=='preparing':self.assertIsNone(s['start_at'])
            for row in prep.get('robot_steps',{}).values():seen.add(row.get('step_number'))
        self.assertTrue(set(range(1,7))<=seen)
        self.assertEqual(doc['snapshot']['preparation']['step_label'],'Preparation complete')
        for r in names:self.assertEqual(doc['snapshot']['preparation']['robot_steps'][r]['status'],'ready')
        return doc,port
    def test_one_robot_normal_path(self):self.run_robots(1)
    def test_four_robots_move_before_any_finishes(self):self.run_robots(4)


class ProgressAndAdmission(Base):
    def setUp(self):
        super().setUp();self.begin();self.stage('health');self.st.stage(self.uid,gate.OWNER,self.token,'preparing',NOW,robots=[A,B])
    def update(self,robot,step,stamp=NOW):return self.st.progress(self.uid,gate.OWNER,self.token,robot,step,stamp)
    def test_progress_does_not_overwrite_other_robot(self):
        self.update(A,'moving_to_start');d=self.update(B,'confirming_location','2026-11-20-18:00:10')
        rows=d['snapshot']['preparation']['robot_steps']
        self.assertEqual(rows[A]['step_number'],4);self.assertEqual(rows[A]['step_started_at'],NOW)
        self.assertEqual(rows[B]['step_number'],5)
    def test_poll_reports_same_accepted_revision(self):
        d=self.update(A,'moving_to_start')
        self.assertEqual(runtime.current_snapshot()['preparation'],d['snapshot']['preparation'])
        self.assertEqual(runtime.current_snapshot()['update_seq'],d['snapshot']['update_seq'])
    def test_only_batch_members_authorized(self):
        self.assertTrue(gate.execution_allowed(A));self.assertTrue(gate.execution_allowed(B))
        self.assertFalse(gate.execution_allowed('twin-scout-echo'))
        self.assertFalse(gate.row_allowed(A,dict(action='scan.start')))
    def test_duplicate_progress_does_not_reset_elapsed_or_sequence(self):
        d=self.update(A,'moving_to_start');later=self.update(A,'moving_to_start','2026-11-20-18:00:10')
        self.assertEqual(d['snapshot'],later['snapshot'])
    def test_failed_is_not_step_seven_and_retry_clears_old_progress(self):
        self.update(A,'moving_to_start');d=self.st.fail(self.uid,gate.OWNER,self.token,NOW,'test',A)
        self.assertIsNone(d['snapshot']['preparation']['step_number'])
        self.assertEqual(d['snapshot']['preparation']['robot_steps'][A]['status'],'failed')
        self.assertFalse(gate.execution_allowed(A))
        d=self.st.begin(self.uid,gate.OWNER,'2026-11-20-18:03:00')
        self.assertEqual(d['runtime']['robot_progress'],{})
    def test_old_attempt_and_cancel_cannot_publish_progress(self):
        with self.assertRaises(Conflict):self.st.progress(self.uid,gate.OWNER,'old',A,'moving_to_start',NOW)
        self.st.request_cancel(self.uid,NOW)
        with self.assertRaises(Conflict):self.update(A,'moving_to_start')


class BarrierAndCleanup(unittest.IsolatedAsyncioTestCase):
    async def test_crossing_paths_block_both_before_move(self):
        port=types.SimpleNamespace(check_path=lambda *a:None);barrier=PathBarrier([A,B],port,.5)
        done=[]
        paths={A:({'x_m':0.,'y_m':0.},{'x_m':1.,'y_m':1.}),B:({'x_m':0.,'y_m':1.},{'x_m':1.,'y_m':0.})}
        async def job(r):await barrier.wait(r,*paths[r]);done.append(r)
        with self.assertRaises(Exception):await run_batch([A,B],job)
        self.assertEqual(done,[])
    async def test_waits_for_every_robot_then_releases(self):
        barrier=PathBarrier([A,B],types.SimpleNamespace(check_path=lambda *a:None),.5)
        a=asyncio.create_task(barrier.wait(A,{'x_m':0.,'y_m':0.},{'x_m':1.,'y_m':0.}))
        await asyncio.sleep(0);self.assertFalse(a.done())
        await barrier.wait(B,{'x_m':0.,'y_m':2.},{'x_m':1.,'y_m':2.});await a
    async def test_failed_batch_cancels_and_joins_sibling(self):
        started=asyncio.Event();closed=[]
        async def job(r):
            if r==A:
                try:started.set();await asyncio.Event().wait()
                finally:closed.append(r)
            await started.wait();raise BatchFailure(B,'failure')
        with self.assertRaises(BatchFailure):await run_batch([A,B],job)
        self.assertEqual(closed,[A])
    async def test_stationary_and_collinear_geometry(self):
        self.assertEqual(segment_distance((0,0),(1,0),(.5,0),(2,0)),0)
        self.assertEqual(segment_distance((0,0),(0,0),(0,2),(1,2)),2)

if __name__=='__main__':unittest.main(verbosity=2)
