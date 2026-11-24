"""Offline preparation positioning checks. No robot/Redis/network commands.
Run: python testSM/testPreparation/step3_check.py
"""
import ast
import asyncio
from copy import deepcopy
from datetime import datetime, timedelta
import json
import math
from pathlib import Path
import sys
import types
import unittest
import uuid
from typing import Any, Dict
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT/'sitemap/CommonCheckers'))
from nms_preparation_positioning import (position_once, measured_evidence, preparation_report_fresh,
    NmsPositioningPort, PositioningError, prepare_controller_robot)
from nms_experiment_preparation import Preparation, PreparationError
from checker import static_safety_core  # Load before temporary import substitutions.

A='twin-scout-alpha'
TIME_FMT='%Y-%m-%d-%H:%M:%S'
START=datetime(2026,11,20,18,0)


def pose(x=5.,y=3.,source='apriltag'):
    return dict(location_ok=True,x_m=x,y_m=y,heading_deg=270.,source=source,confidence='HIGH')


def report(stamp, serial):
    return dict(last_exec_status='completed',last_command='mobility.report.location',last_command_args={},
        last_command_received_ts=stamp,last_command_finished_ts=stamp,
        last_location_result=dict(ok=True,snapshots={'front':f'/tmp/front_{serial}.jpg','rear':f'/tmp/rear_{serial}.jpg'}))


class Port:
    final_tolerance_m=.3
    def __init__(self):
        self.seconds=0.;self.n=0;self.commands=[];self.closed=False
        self.initial=pose(4.6,3);self.final=pose(5,3);self.mode='';self.scope='';self.checks=0
        self.state='s0idle';self.evidence={};self.current_pose={};self.global_stop=False
    def local_ts(self): return (START+timedelta(seconds=self.seconds)).strftime(TIME_FMT)
    async def sleep(self, seconds): self.seconds+=seconds
    def open(self, scanner, *, scope, owns_attempt):
        if self.mode=='busy': raise PositioningError('not idle')
        self.scope=scope
    def close(self, scanner, *, scope): self.closed=True
    def snapshot(self,scanner):
        return deepcopy(dict(state=self.state,stop=self.global_stop,reason='test stop' if self.global_stop else '',pose=self.current_pose,evidence=self.evidence))
    def issue(self, scanner, action, args, **kw):
        self.commands.append((action,deepcopy(args)))
        if self.mode=='timeout': self.state='s1waiting_report'; return {'status':'ok'}
        self.seconds+=1;self.n+=1
        self.current_pose=deepcopy(self.initial if self.n==1 else self.final)
        stamp=self.local_ts(); rep=report(stamp,self.n)
        self.evidence=dict(scope=self.scope,solve_id=str(self.n),pose=deepcopy(self.current_pose),
                           issued_at=stamp,solved_at=stamp,report=rep)
        if self.mode=='stale': rep['last_command_received_ts']='2026-11-19-18:00:00'
        if self.mode=='no_tag': self.current_pose['location_ok']=False;self.evidence['pose']=deepcopy(self.current_pose)
        if self.mode=='reused' and self.n>1: rep['last_location_result']['snapshots']['front']='/tmp/front_1.jpg'
        if self.mode=='wrong_scope': self.evidence['scope']='other'
        self.state='s0idle';return {'status':'ok'}
    def check_path(self,scanner,start,target):
        self.checks+=1
        if self.mode in ('static','dynamic','bump','no_heading'): raise PositioningError(self.mode)
        return {'path_ok':True,'preferred_heading_deg':235}
    def move(self,scanner,target,**kw):
        if not kw['owns_attempt'](): raise PositioningError('cancelled')
        self.commands.append(('mobility.move',deepcopy(target)))
        self.seconds+=1
        if self.mode=='move_error': self.state='s7stopped';self.global_stop=True
        return {'status':'ok'}


class Attempt(unittest.TestCase):
    def run_one(self,p=None,owns=lambda:True):
        p=p or Port()
        return asyncio.run(position_once(scanner=A,target={'x_m':5,'y_m':3},port=p,
            owns_attempt=owns,monotonic=lambda:p.seconds,sleep=p.sleep,timeout_seconds=5))
    def test_report_move_confirm_success(self):
        p=Port();r=self.run_one(p)
        self.assertEqual(r['status'],'ready');self.assertTrue(p.closed)
        self.assertEqual([c[0] for c in p.commands],['mobility.report.location','mobility.move','mobility.report.location'])
        self.assertEqual(p.commands[1][1],{'x_m':5.,'y_m':3.})
        self.assertNotIn('heading_deg',p.commands[1][1])
        print('\nSingle-attempt sequence: report -> 0.400m placement -> checked move -> fresh confirmation -> READY.')
    def test_one_meter_allowed(self):
        p=Port();p.initial=pose(4,3);self.assertEqual(self.run_one(p)['status'],'ready')
    def test_beyond_one_meter_no_movement(self):
        p=Port();p.initial=pose(3.999,3);r=self.run_one(p)
        self.assertEqual(r['status'],'failed');self.assertEqual(len(p.commands),1)
    def test_diagonal_distance_limit(self):
        p=Port();p.initial=pose(4.2,2.2);self.assertEqual(self.run_one(p)['status'],'failed')
    def test_no_location_no_movement(self):
        p=Port();p.mode='no_tag';self.assertEqual(self.run_one(p)['status'],'failed');self.assertEqual(len(p.commands),1)
    def test_stale_report_no_movement(self):
        p=Port();p.mode='stale';self.assertEqual(self.run_one(p)['status'],'failed');self.assertEqual(len(p.commands),1)
    def test_predicted_pose_not_used(self):
        p=Port();p.initial['source']='propagated';self.assertEqual(self.run_one(p)['status'],'failed')
    def test_low_confidence_valid_measured_pose_not_extra_veto(self):
        p=Port();p.initial['confidence']='LOW';p.final['confidence']='LOW'
        self.assertEqual(self.run_one(p)['status'],'ready')
    def test_blocked_paths_and_missing_heading_no_move(self):
        for mode in ('static','dynamic','bump','no_heading'):
            with self.subTest(mode=mode):
                p=Port();p.mode=mode;self.assertEqual(self.run_one(p)['status'],'failed');self.assertEqual(len(p.commands),1)
    def test_move_failure_does_not_clear_stop(self):
        p=Port();p.mode='move_error';self.assertEqual(self.run_one(p)['status'],'failed')
        self.assertTrue(p.global_stop);self.assertEqual(len(p.commands),2)
    def test_final_30cm_accepted_301mm_rejected(self):
        for delta, expected in ((.3,'ready'),(.301,'failed')):
            p=Port();p.final=pose(5+delta,3);self.assertEqual(self.run_one(p)['status'],expected)
    def test_confirmation_requires_new_capture(self):
        p=Port();p.mode='reused';self.assertEqual(self.run_one(p)['status'],'failed')
    def test_wrong_scope_rejected(self):
        p=Port();p.mode='wrong_scope';self.assertEqual(self.run_one(p)['status'],'failed')
    def test_cancel_during_wait_no_followup(self):
        p=Port();p.mode='timeout'
        self.assertEqual(self.run_one(p,lambda:p.seconds<1)['status'],'failed')
        self.assertEqual(len(p.commands),1);self.assertTrue(p.closed)
    def test_timeout_does_not_issue_retry(self):
        p=Port();p.mode='timeout';r=self.run_one(p)
        self.assertIn('timed out',r['reason']);self.assertEqual(p.seconds,5);self.assertEqual(len(p.commands),1)
    def test_busy_robot_not_reset(self):
        p=Port();p.mode='busy';self.assertEqual(self.run_one(p)['status'],'failed');self.assertEqual(p.commands,[])
    def test_nonfinite_pose_rejected(self):
        p=Port();p.initial['x_m']=float('nan');self.assertEqual(self.run_one(p)['status'],'failed')
    def test_step2_controller_accepts_only_after_confirmation(self):
        man={'schema_version':'experiment_csv_v1','initial_positions':[dict(scanner=A,x_m=5,y_m=3)],
             'commands':[dict(scanner=A,t_offset_sec=0)]}
        c=Preparation(lab_id='DemoRoom',experiment_id='test',session_id='',manifest=man,now=START)
        c.health_passed(token=c.token,now=START);c.begin_attempt(now=START,all_robots_idle=True,stop_latched=False)
        p=Port();r=asyncio.run(prepare_controller_robot(c,A,p,monotonic=lambda:p.seconds,sleep=p.sleep))
        self.assertEqual(r['status'],'ready');self.assertEqual(c.phase,'ready')
        self.assertEqual(c.schedule_candidate(now=START+timedelta(seconds=5))['start_at'],'2026-11-20-18:01:00')
    def test_step2_failed_attempt_waits_for_operator(self):
        man={'schema_version':'experiment_csv_v1','initial_positions':[dict(scanner=A,x_m=5,y_m=3)],
             'commands':[dict(scanner=A,t_offset_sec=0)]}
        c=Preparation(lab_id='DemoRoom',experiment_id='test',session_id='',manifest=man,now=START)
        c.health_passed(token=c.token,now=START);c.begin_attempt(now=START,all_robots_idle=True,stop_latched=False)
        p=Port();p.initial=pose(3,3)
        asyncio.run(prepare_controller_robot(c,A,p,monotonic=lambda:p.seconds,sleep=p.sleep))
        self.assertEqual(c.phase,'retry_wait');self.assertEqual(c.snapshot(now=START+timedelta(seconds=1))['retry_in_sec'],60)

    def test_s7_is_retryable_after_wait_and_reset(self):
        man={'schema_version':'experiment_csv_v1','initial_positions':[dict(scanner=A,x_m=5,y_m=3)],
             'commands':[dict(scanner=A,t_offset_sec=0)]}
        c=Preparation(lab_id='DemoRoom',experiment_id='test',session_id='',manifest=man,now=START)
        c.health_passed(token=c.token,now=START);old=c.begin_attempt(now=START,all_robots_idle=True,stop_latched=False)
        p=Port();p.mode='move_error'
        asyncio.run(prepare_controller_robot(c,A,p,monotonic=lambda:p.seconds,sleep=p.sleep))
        self.assertEqual(c.phase,'retry_wait');self.assertTrue(c.reset_required)
        with self.assertRaises(PreparationError):
            c.begin_attempt(now=START+timedelta(seconds=30),all_robots_idle=True,stop_latched=False,reset_completed=True)
        with self.assertRaises(PreparationError):
            c.begin_attempt(now=START+timedelta(seconds=200),all_robots_idle=True,stop_latched=False)
        with self.assertRaises(PreparationError):
            c.begin_attempt(now=START+timedelta(seconds=200),all_robots_idle=True,stop_latched=True,reset_completed=True)
        new=c.begin_attempt(now=START+timedelta(seconds=200),all_robots_idle=True,stop_latched=False,reset_completed=True)
        self.assertNotEqual(old,new);self.assertFalse(c.reset_required)
        self.assertFalse(c.report_ready(A,token=old,now=START+timedelta(seconds=201)))
        # Represents the caller's completed runtime reset, followed by a fresh attempt.
        p=Port();p.seconds=200
        asyncio.run(prepare_controller_robot(c,A,p,monotonic=lambda:p.seconds,sleep=p.sleep))
        self.assertEqual(c.phase,'ready')

    def test_delete_during_s7_wait_prevents_restart(self):
        man={'schema_version':'experiment_csv_v1','initial_positions':[dict(scanner=A,x_m=5,y_m=3)],
             'commands':[dict(scanner=A,t_offset_sec=0)]}
        c=Preparation(lab_id='DemoRoom',experiment_id='test',session_id='',manifest=man,now=START)
        c.health_passed(token=c.token,now=START);t=c.begin_attempt(now=START,all_robots_idle=True,stop_latched=False)
        c.attempt_failed(token=t,now=START,reason='s7',reset_required=True)
        c.cancel(now=START+timedelta(seconds=1))
        with self.assertRaises(PreparationError):
            c.begin_attempt(now=START+timedelta(seconds=200),all_robots_idle=True,stop_latched=False,reset_completed=True)


def extracted(*names):
    tree=ast.parse((ROOT/'m8mobility_state.py').read_text(encoding='utf-8-sig'))
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==len(names)
    return compile(ast.Module(body=nodes,type_ignores=[]),'m8mobility_state.py','exec')


class StateMachineRegression(unittest.TestCase):
    def test_s3_records_scoped_evidence_without_changing_pose(self):
        for scope in ('','test-scope'):
            writes={}; stamp=START.strftime(TIME_FMT); measured=pose()
            fields={'preparation_scope':scope,'last_planned_command_issued_at':stamp}
            util=types.SimpleNamespace(_hget=lambda k,f,d='':fields.get(f,d),local_ts=lambda:stamp,
                    _hset_many=lambda key,data:writes.update(data))
            ns={'Dict':Dict,'Any':Any,'uuid':uuid,'utility':util,'key_pose':lambda s:s,
                'key_time':lambda s:s,'key_state':lambda s:s,'_load_report_json':lambda s:report(stamp,1)}
            exec(extracted('_s3_save_true_location'),ns)
            ns['_s3_save_true_location'](A,measured)
            self.assertEqual(writes['true_location_json'],measured)
            if scope:
                evidence=writes['preparation_evidence_json']
                self.assertEqual(evidence['scope'],scope)
                snap={'pose':measured,'evidence':evidence}
                self.assertEqual(measured_evidence(snap,scope=scope,since=stamp)[0],measured)
            else:self.assertNotIn('preparation_evidence_json',writes)
    def s3(self, source, semantic, planned):
        self.saved=[];self.states=[]
        util=types.SimpleNamespace(_hget=lambda k,f,d='':semantic if f=='last_s0_command_action' else d,
              _hset_many=lambda *a:None,_deg_norm_360=lambda v:v%360)
        ns={'Dict':Dict,'Any':Any,'utility':util,'key_state':lambda s:s,
            '_s3_solve_true_location':lambda s:pose(), '_trace_location_summary':lambda p:p,
            '_load_report_json':lambda s:{},'append_trace_event':lambda **k:None,
            '_trace_active_id':lambda s:'','_s3_dump_location_helper_data':lambda *a:None,
            '_s3_save_true_location':lambda *a:None,'_update_10s_report':lambda *a:None,
            '_last_issued_source':lambda s:source,'_load_planned':lambda s:planned,
            '_is_loc_ok':lambda p:p.get('location_ok') is True,
            '_save_planned':lambda s,p:self.saved.append(p),
            '_clear_pending_sequence':lambda *a:None,'_clear_outgoing_command_preview':lambda *a:None,
            '_reset_correction_counter':lambda *a:None,'_clear_location_recovery_fields':lambda *a:None,
            '_set_state':lambda s,state,detail:self.states.append(state),
            'S0_IDLE':'s0idle','S5_COMPUTING_CORRECTION':'s5computing_correction',
            's0idle':lambda s:{'state':'s0idle'},'run_state_machine':lambda s:{'state':self.states[-1]}}
        exec(extracted('s3solving_true_location','_is_location_precondition_source','_is_location_recovery_source'),ns)
        return ns['s3solving_true_location'](A)
    def test_direct_report_with_no_planned_pose_returns_idle(self):
        self.assertEqual(self.s3('location_precondition','mobility.report.location',{})['state'],'s0idle')
        self.assertEqual(self.saved[0]['x_m'],5.)
    def test_timeout_recovery_of_report_only_returns_idle(self):
        for source in ('location_recovery_report','location_recovery_visibility_turn'):
            with self.subTest(source=source):
                self.assertEqual(self.s3(source,'mobility.report.location',{})['state'],'s0idle')
                self.assertEqual(self.saved[0]['x_m'],5.)
    def test_report_only_preserves_existing_target(self):
        self.s3('location_recovery_report','mobility.report.location',pose(8,8))
        self.assertEqual(self.saved,[])
    def test_recovery_of_move_still_returns_s5(self):
        self.assertEqual(self.s3('location_recovery_report','mobility.move',pose(8,8))['state'],'s5computing_correction')
        self.assertEqual(self.saved,[])
    def test_normal_move_report_still_returns_s5(self):
        self.assertEqual(self.s3('mobility','mobility.move',pose(8,8))['state'],'s5computing_correction')
    def test_freshness_gate_blocks_old_report_before_s3(self):
        stamp=START.strftime(TIME_FMT);old=report('2026-11-19-18:00:00',1)
        self.assertFalse(preparation_report_fresh(old,stamp,{})[0])
        current=report(stamp,2);self.assertTrue(preparation_report_fresh(current,stamp,old)[0])
        self.assertFalse(preparation_report_fresh(current,stamp,current)[0])
    def test_fresh_error_still_reaches_existing_policy(self):
        stamp=START.strftime(TIME_FMT);r=report(stamp,1);r['last_exec_status']='error';r.pop('last_location_result')
        self.assertTrue(preparation_report_fresh(r,stamp,{})[0])
    def test_actual_s1_gate_is_scoped(self):
        for scope,expected in (('',True),('test-scope',False)):
            fields={'last_planned_command_action':'mobility.report.location','last_planned_command_args_json':'{}',
                    'preparation_scope':scope,'last_planned_command_issued_at':START.strftime(TIME_FMT)}
            ns={'Dict':Dict,'Any':Any,'json':json,
                'utility':types.SimpleNamespace(_hget=lambda k,f,d='':fields.get(f,d),_hget_json=lambda *a:{}),
                'key_pose':lambda s:s,'key_state':lambda s:s,'key_time':lambda s:s,
                '_load_report_json':lambda s:report('2026-11-19-18:00:00',1),
                '_normalize_mobility_command':lambda a,b:(a,b)}
            exec(extracted('_s1_report_matches_expected_command'),ns)
            self.assertEqual(ns['_s1_report_matches_expected_command'](A)[0],expected)
    def s5(self,distance,count):
        ns={'Dict':Dict,'Any':Any,'config':types.SimpleNamespace(MOBILITY_FIRST_CORRECTION_MAX_M=.4,MOBILITY_POST_CORRECTION_FAIL_THRESH_M=.3,MOBILITY_POS_IGNORE_THRESH_M=.05),
            '_load_true':lambda s:pose(5+distance,3),'_load_planned':lambda s:pose(),
            '_is_loc_ok':lambda p:p.get('location_ok') is True,'_pose_error':lambda a,b:{'dpos_m':distance},
            '_get_correction_counter':lambda s:count,'_load_site_macro_context':lambda s:{},
            'decide_followup_correction':lambda **kw:{'go':False,'reason_code':'LOW'},
            'collect_rare_case_reasons':lambda **kw:[],'_log_location_rare_case_noexcept':lambda *a,**kw:None,
            '_trace_followup_decision':lambda *a,**kw:None,
            '_s5_stop_experiment':lambda s,**kw:{'status':'stop',**kw},'CORRECTION_ATTEMPT_LIMIT':1,
            '_clear_pending_sequence':lambda *a:None,'_clear_outgoing_command_preview':lambda *a:None,
            '_clear_s0_command_arg_overrides':lambda *a:None,'_clear_site_macro_context':lambda *a:None,
            'S0_IDLE':'s0idle','S7_STOPPED':'s7stopped','S5_COMPUTING_CORRECTION':'s5computing_correction'}
        exec(extracted('_s5_compute_correction'),ns)
        return ns['_s5_compute_correction'](A)
    def test_first_correction_limit_unchanged(self):
        self.assertEqual(self.s5(.401,0)['status'],'stop')
        self.assertEqual(self.s5(.399,0)['status'],'ok')
    def test_final_correction_limit_unchanged(self):
        self.assertEqual(self.s5(.301,1)['status'],'stop')
        self.assertEqual(self.s5(.299,1)['status'],'ok')


class Gateway(unittest.TestCase):
    def port(self, blocked=False, ramp=None, heading=True):
        p=NmsPositioningPort.__new__(NmsPositioningPort)
        policy={'ramp_geometry':{
            'restriction_zone':{'x_min_m':8.6,'x_max_m':9.6,'y_min_m':4.6,'y_max_m':6.0},
            'bump_core':{'x_min_m':8.7,'x_max_m':9.5,'y_min_m':5.2,'y_max_m':5.4}}}
        p.state=types.SimpleNamespace(_load_runtime_ramp_policy=lambda:policy if ramp is None else ramp)
        self.map_module=types.ModuleType('m8mobility_map')
        self.map_module._load_site_config=lambda:{'world_width_m':11.4,'world_height_m':11.4}
        self.map_module._is_path_clear_debug=lambda *a,**k:(not blocked,[(0,0)] if blocked else [],{'blocked_count':int(blocked)})
        self.heading_module=types.ModuleType('preferred_direction_helper')
        self.heading_module.lookup_preferred_direction=lambda **kw:{'ok':heading,'preferred_heading_deg':235}
        return p
    def check(self,p,start,target):
        with patch.dict(sys.modules,{'m8mobility_map':self.map_module,'preferred_direction_helper':self.heading_module}):
            return p.check_path(A,start,target)
    def test_runtime_path_and_preferred_heading(self):
        p=self.port();r=self.check(p,pose(4.6,3),{'x_m':5,'y_m':3})
        self.assertEqual(r['preferred_heading_deg'],235);self.assertTrue(r['path_ok'])
    def test_static_or_dynamic_block_refused(self):
        p=self.port(blocked=True)
        with self.assertRaises(PositioningError):self.check(p,pose(4.6,3),{'x_m':5,'y_m':3})
    def test_out_of_bounds_not_clamped_into_map(self):
        p=self.port()
        with self.assertRaises(PositioningError):self.check(p,pose(-.01,3),{'x_m':.1,'y_m':3})
    def test_missing_heading_refused(self):
        p=self.port(heading=False)
        with self.assertRaises(PositioningError):self.check(p,pose(4.6,3),{'x_m':5,'y_m':3})
    def test_actual_shared_bump_rule_blocks_entry(self):
        p=self.port()
        with self.assertRaises(PositioningError):self.check(p,pose(9.1,4.3),{'x_m':9.1,'y_m':4.8})
    def test_missing_bump_policy_fails_closed(self):
        p=self.port(ramp={})
        with self.assertRaises(PositioningError):self.check(p,pose(4.6,3),{'x_m':5,'y_m':3})
    def test_planned_seed_uses_actual_pose_not_target(self):
        p=NmsPositioningPort.__new__(NmsPositioningPort);saved=[];sent=[]
        measured=pose(4.6,3);proof={'pose':measured,'solve_id':'one'}
        stored=deepcopy(proof)
        p.snapshot=lambda s:{'pose':deepcopy(measured),'evidence':deepcopy(stored)}
        p.check_path=lambda *a:{'path_ok':True}
        p._with_lock=lambda s,scope,owns,callback:callback()
        p.store=types.SimpleNamespace(_save_planned=lambda s,loc:saved.append(loc))
        p.mobility=types.SimpleNamespace(_on_command_issued_locked=lambda s,a,args:sent.append((a,args)) or {'status':'ok'})
        p.move(A,{'x_m':5,'y_m':3},initial_evidence=proof,scope='test',owns_attempt=lambda:True)
        self.assertEqual(saved[0]['x_m'],4.6);self.assertEqual(sent[0][1],{'x_m':5,'y_m':3})
        proof['solve_id']='old'
        with self.assertRaises(PositioningError):
            p.move(A,{'x_m':5,'y_m':3},initial_evidence=proof,scope='test',owns_attempt=lambda:True)


if __name__=='__main__': unittest.main(verbosity=2)
