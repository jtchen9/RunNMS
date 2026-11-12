"""Offline Step 2 integration check. Run in VS Code. No NMS imports, Redis or robots.
Executes selected function bodies from the three patched files, with an in-memory
stream, fake clock and injected evidence. The scheduler tick and poll paths are real
function bodies; the HTTP service, sensors, planner and Redis server are not run.
"""
import ast
import json
import math
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace as NS
from typing import Any, Dict, Optional
from unittest.mock import Mock

def find_nms_root():
    modules = ("m8mobility_state.py", "m8mobility.py", "m4Commands.py")
    for directory in Path(__file__).resolve().parents:
        if all((directory / name).is_file() for name in modules):
            return directory
    raise FileNotFoundError(
        "Keep this test inside the NMS checkout containing " + ", ".join(modules)
    )


ROOT = find_nms_root()

def load(file, names, ns, constants=()):
    t=ast.parse((ROOT/file).read_text(encoding="utf-8-sig"))
    keep=[]
    for n in t.body:
        if isinstance(n,ast.FunctionDef) and n.name in names:
            n.decorator_list=[]; keep.append(n)
        if isinstance(n,ast.Assign) and any(isinstance(v,ast.Name) and v.id in constants for v in n.targets):
            keep.append(n)
    t.body=keep
    exec(compile(t,file,'exec'),ns)
    for name in names:
        if name not in ns: raise RuntimeError(f"Missing {file}: {name}. Apply the Step 2 patch first.")

class Step2(unittest.TestCase):
    def setUp(self):
        self.h={}; self.q={'robot':{},'schedule':{}}; self.now=1000.
        self.stop={'stop':False}; self.count=0; self.robot='bravo'; self.macro={}
        self.target={'location_ok':True,'x_m':4.,'y_m':3.1,'heading_deg':235.}
        self.pose=self.loc(4.688,2.903,'LOW'); self.trace=[];self.raw={'last_exec_status':'completed'}
        def fields(key,d):
            for k,v in d.items(): self.h[key,k]=json.dumps(v) if isinstance(v,dict) else v
        def hsetnx(key,k,v):
            if (key,k) in self.h:return 0
            self.h[key,k]=v;return 1
        def xdel(key,id):return self.q[key].pop(id,None) is not None
        self.store=NS(hsetnx=hsetnx,xdel=xdel,xrange=lambda key,count:list(self.q[key].items())[:count],
                      smembers=lambda key:[self.robot])
        self.config=NS(r=self.store,key_cmd_stream=lambda s:'robot',KEY_REGISTRY='registry',
                       KEY_MOBILITY_CMD_STREAM='schedule',CMD_EXPIRE_SEC=180,MOBILITY_POLL_STALE_SEC=30)
        self.util=NS(_hget=lambda key,k,default:self.h.get((key,k),default),_hset_many=fields,
                     parse_local_dt=datetime.fromisoformat,local_ts=lambda:'2026-11-11-23:04:03')
        noop=lambda *a,**kw:None
        self.n=dict(Optional=Optional,Any=Any,Dict=Dict,config=self.config,utility=self.util,json=json,math=math,uuid=uuid,
            time=NS(time=lambda:self.now),key_state=lambda s:'state',key_time=lambda s:'time',
            _load_stop=lambda:self.stop,_save_stop=lambda flag,reason:self.stop.update(stop=flag,reason=reason),
            _set_state=lambda s,state,detail:fields('state',dict(state=state,state_detail=detail)),
            _get_correction_counter=lambda s:self.count,_load_report_json=lambda s:self.raw,
            _load_planned=lambda s:self.target,_load_true=lambda s:self.pose,
            _trace_active_id=lambda s:'parent',_trace_active_snapshot=lambda s:{'start_pose':{'x_m':4.093,'y_m':3.003}},
            _load_site_macro_context=lambda s:self.macro,_site_macro_landing_issue=lambda *a:None,
            _s3_save_true_location=lambda s,loc:setattr(self,'pose',loc),_update_10s_report=noop,
            _is_loc_ok=lambda p:p.get('location_ok'),_pose_error=lambda p,q:{'dpos_m':math.hypot(p['x_m']-q['x_m'],p['y_m']-q['y_m'])},
            decide_followup_correction=lambda **kw:dict(go=(kw['location_result']['confidence']['level']=='HIGH' and
                math.hypot(kw['location_result']['x_m']-self.target['x_m'],kw['location_result']['y_m']-self.target['y_m'])>.15),reason_code='test policy'),
            collect_rare_case_reasons=lambda **kw:[], _log_location_rare_case_noexcept=noop,
            _trace_followup_decision=lambda *a,**kw:self.trace.append(kw['outcome']),
            _clear_pending_sequence=noop,_clear_outgoing_command_preview=noop,
            _clear_s0_command_arg_overrides=noop,_clear_site_macro_context=noop,process_s1_event=Mock(),
            s0idle=lambda s:{'state':'s0idle'},link=NS(disposition=lambda *a:'ready'))
        n=self.n
        load('m8mobility_state.py','_get_state _s8_episode _s8_prepare _s8_expired _s8_stop _s8_strategy s8recovery s7stopped _s5_stop_experiment _s5_clear_runtime_safety_stop s5computing_correction _s5_compute_correction run_state_machine'.split(),n,
             ('S0_IDLE','S1_WAITING_REPORT','S2_EVALUATING_POLICY','S3_SOLVING_TRUE_LOCATION','S4_WAITING_LOCATION_RETRY','S5_COMPUTING_CORRECTION','S6_ISSUING_CORRECTION','S7_STOPPED','S8_RECOVERY','VALID_STATES','CORRECTION_ATTEMPT_LIMIT'))
        load('m8mobility.py','should_block_command on_command_issued on_report_received _dispatch_due_mobility_command movement_delivery_blocked _check_s8_deadlines'.split(),n)
        n['m8mobility']=NS(movement_delivery_blocked=n['movement_delivery_blocked'])
        load('m4Commands.py',['_collect_due_commands'],n)
        fields('state',dict(last_s0_command_action='mobility.move',state=n['S5_COMPUTING_CORRECTION']))
    @staticmethod
    def loc(x,y,c):
        return dict(location_ok=True,x_m=x,y_m=y,heading_deg=260.,source='apriltag_componentwise',confidence={'level':c})
    def call(self,fn,*args):
        return self.n[fn](self.robot,*args)
    def waiting(self):
        self.config.MOBILITY_S8_ENABLED=True
        self.n['_s8_strategy']=lambda *a:{'status':'waiting'}
        self.assertEqual(self.call('run_state_machine')['state'],'s8recovery')
        return self.call('_s8_episode')
    def poll(self):
        return self.n['_collect_due_commands'](self.robot,20,'2026-11-11T23:04:03')['commands']
    def put(self,xid,**more):
        self.q['robot'][xid]=dict(category='mobility',action='mobility.turn',execute_at='2026-11-11T23:04:02',**more)
    def test_default_off(self):
        self.assertEqual(self.call('run_state_machine')['state'],'s7stopped')
        self.assertFalse(self.call('_s8_episode'));self.assertTrue(self.stop['stop'])
    def test_no_installed_action_stops(self):
        self.config.MOBILITY_S8_ENABLED=True
        self.assertEqual(self.call('run_state_machine')['state'],'s7stopped')
        self.assertIn('no recovery action',self.stop['reason'])
    def test_queue_isolation_and_timeout(self):
        e=self.waiting();row=dict(scanner='bravo',action='mobility.move',args_json='{}')
        self.q['schedule']['next']=row
        self.n['_dispatch_due_mobility_command']('next',row)
        self.assertIn('next',self.q['schedule'])
        self.put('wrong',mobility_s1_managed='1',s8_episode_id='old')
        self.put('right',mobility_s1_managed='1',s8_episode_id=e['id'])
        self.assertEqual(self.poll(),[])  # Episode ID alone cannot authorize a physical child.
        self.assertEqual(self.poll(),[])
        self.n['_s8_strategy']=Mock(side_effect=AssertionError('no timer-driven actuator'))
        self.now=e['deadline'];self.n['_check_s8_deadlines']();self.assertTrue(self.stop['stop'])
        self.call('on_report_received');self.n['process_s1_event'].assert_not_called()
        self.put('managed',mobility_s1_managed='1');self.put('manual')
        self.assertEqual(self.poll(),[]);self.assertEqual(self.call('_get_state'),'s7stopped')
    def test_trusted_return_via_s5_and_release_once(self):
        self.waiting();q=self.target.copy()
        self.n['_s8_strategy']=lambda s,e:dict(status='observed',verified=True,episode_id=e['id'],
                                            pose=self.loc(4.04,3.01,'HIGH'))
        self.assertEqual(self.call('run_state_machine')['state'],'s0idle')
        self.assertEqual(self.target,q);self.assertEqual(self.count,0);self.assertEqual(self.trace[-1],'accepted_without_correction')
        row=dict(scanner='bravo',action='mobility.move',args_json='{}');self.q['schedule']['next']=row
        self.n['enter_s0idle_on_command']=Mock(return_value={'status':'ok'})
        self.n['_dispatch_due_mobility_command']('next',row)
        self.assertNotIn('next',self.q['schedule']);self.n['enter_s0idle_on_command'].assert_called_once()
    def test_no_second_attempt(self):
        self.waiting();self.n['_s8_strategy']=lambda s,e:dict(status='observed',verified=True,
            episode_id=e['id'],pose=self.loc(4.30055,3.1,'HIGH'))
        self.assertEqual(self.call('run_state_machine')['state'],'s7stopped')
        self.assertEqual(self.trace.count('entered_s8_before_first_correction'),1)
    def test_unrelated_result(self):
        self.waiting();self.n['_s8_strategy']=lambda s,e:dict(status='observed',verified=True,
            episode_id='another',pose=self.loc(4.04,3.01,'HIGH'))
        self.assertEqual(self.call('run_state_machine')['state'],'s7stopped')
    def test_phase_a_landing(self):
        self.macro=dict(action='mobility.in2out',phase='post_cross_landing_verified')
        self.h['state','last_s0_command_action']='mobility.in2out'
        self.target.update(x_m=9.038583786251442,y_m=6.403754795460579)
        self.pose=self.loc(9.074206929095231,6.105320840443929,'HIGH')
        self.assertEqual(self.waiting()['kind'],'check_short_correction')
    def test_global_stop_during_wait(self):
        self.waiting();self.stop.update(stop=True,reason='other robot stopped')
        self.n['_check_s8_deadlines']()
        self.assertEqual(self.call('_get_state'),'s7stopped');self.assertEqual(self.stop['reason'],'other robot stopped')
    def test_s1_hold_and_delivery(self):
        self.h['state','state']='s1waiting_report'
        row=dict(scanner='bravo',action='mobility.move',args_json='{}');self.q['schedule']['next']=row
        self.n['_dispatch_due_mobility_command']('next',row)
        self.assertIn('next',self.q['schedule']);self.put('active',mobility_s1_managed='1')
        self.assertEqual([x for x,_ in self.poll()],['active'])
if __name__=='__main__':unittest.main(verbosity=2)
