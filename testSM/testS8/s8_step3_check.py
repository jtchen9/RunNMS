"""Run this file in VS Code. Uses the installed planner and DemoRoom runtime map.
No NMS import, Redis, Wi-Fi, or robot. Fake clock/queue, peer map and new reports.
Requires numpy (as does the NMS) and s8_step2_check.py in this directory.
"""
import json
import math
import sys
import unittest
from unittest.mock import Mock
import numpy as np
import s8_step2_check as base
ROOT = base.ROOT
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT/'sitemap/CommonCheckers'))
from checker import static_safety_core as core
import m8mobility_s8_landing as rules

class Landing(unittest.TestCase):
    call, loc = base.Step2.call, staticmethod(base.Step2.loc)
    def setUp(self):
        base.Step2.setUp(self)
        self.config.MOBILITY_S8_ENABLED=True
        self.config.MOBILITY_S8_LANDING_ENABLED=True
        self.target.update(x_m=9.038583786251442,y_m=6.403754795460579,heading_deg=90.)
        self.pose=self.loc(9.074206929095231,6.105320840443929,'HIGH')
        self.pose['heading_deg']=55.197114260411865
        self.raw=dict(last_exec_status='completed',last_error_code='',last_command=rules.ACTION,
                      last_command_args={'pre_angle':144.6495628139623,'distance_m':2.,'post_angle':-35.,'move_profile':'bump_crossing_up'},
                      last_command_finished_ts=1794269047.9408522)
        self.site=ROOT/'sitemap/DemoRoom'
        self.policy=json.loads((self.site/'script_authoring/config/macro_policy.json').read_text(encoding='utf-8-sig'))
        self.macro=dict(action='mobility.in2out',phase='post_cross_landing_verified',
                        macro_cfg=self.policy['macros']['mobility.in2out'])
        self.h['state','last_s0_command_action']='mobility.in2out'
        self.static=np.load(self.site/'restriction_map.npy',allow_pickle=False)
        self.peer=np.zeros_like(self.static)
        self.saved=[]; self.seq=[]; self.overrides={};self.paths=[]
        self.util._deg_norm_360=lambda x:x%360
        self.util._wrap_angle_deg=lambda x:(x+180)%360-180
        self.util._deg_to_rad=math.radians
        def inc(s):
            self.count+=1
            return self.count
        def issue(s,action,args):
            self.saved.append((action,dict(args)))
            return '2026-11-10-08:04:19'
        def pending(s,seq,comment):self.seq=list(seq)
        n=self.n; n.update(landing_rules=rules,np=np,
            _load_s0_command_arg_overrides=lambda s:self.overrides,
            _update_site_macro_phase=lambda *a,**kw:self.fail('must not reissue a bump crossing'),
            # Actual target-cell heading from the Phase A trace, no live LUT needed.
            lookup_preferred_direction=lambda **kw:{'ok':True,'preferred_heading_deg':55.0},
            _load_runtime_ramp_policy=lambda:self.policy,ramp_restriction_movement_issues=core.ramp_restriction_movement_issues,
            _build_effective_map=lambda exclude_scanner:(self.static,self.peer,np.maximum(self.static,self.peer)),
            _load_site_config=lambda:{'grid_resolution_m':.1},sample_path_cells_legacy=core.sample_path_cells_legacy,
            world_to_grid_clamped=core.world_to_grid_clamped,_save_pending_sequence=pending,
            _load_pending_sequence=lambda s:self.seq,_trace_record_s5_command=lambda s,**kw:self.paths.append(kw),
            _save_outgoing_command_preview=lambda *a,**kw:None,_save_last_issued_command=issue,
            _inc_correction_counter=inc,append_trace_event=lambda **kw:None,_start_s1_timer=lambda *a:None)
        base.load('m8mobility_command_model.py',['_normalize_mobility_command','_build_command_from_true_to_target'],n)
        base.load('m8mobility_pose.py','_apply_mobility_command_to_pose _apply_turn_move_turn _apply_turn _pose_error'.split(),n)
        base.load('m8mobility_map.py','_is_path_clear_debug _world_to_grid _grid_resolution_m _sample_path_cells'.split(),n)
        base.load('m8mobility_state.py','_s6_issue_correction s6issuing_correction _site_macro_landing_issue'.split(),n)
    def go(self):return self.call('run_state_machine')['state']
    def issue(self):
        self.assertEqual(self.go(),'s1waiting_report');self.assertEqual(len(self.saved),1)
        self.assertEqual(self.count,1);return self.saved[0][1]
    def respond(self,d=.05,confidence='HIGH',**changes):
        self.raw=dict(last_exec_status='completed',last_command=rules.ACTION,last_command_args=self.saved[0][1],
                      last_command_received_ts=1794269060.,last_command_finished_ts=1794269080.)
        self.raw.update(changes)
        self.pose=self.loc(self.target['x_m']+d,self.target['y_m'],confidence)
        self.h['state','state']=self.n['S5_COMPUTING_CORRECTION'] # inject post-S3 evidence
        return self.go()
    def test_trace_to_one_checked_command_then_report(self):
        original=self.target.copy();args=self.issue()
        self.assertEqual(set(args),{'pre_angle','distance_m','post_angle'})
        self.assertGreater(args['distance_m'],.29999);self.assertLessEqual(args['distance_m'],.3)
        self.assertAlmostEqual(args['pre_angle'],41.61,delta=.04)
        path=self.paths[0];end=path['simulated_endpoint']
        self.assertTrue(path['path_summary']['checked'] and path['path_summary']['path_ok'])
        self.assertLess(math.hypot(end['x_m']-original['x_m'],end['y_m']-original['y_m']),.00056)
        print(f"Phase A: {args['distance_m']:.6f} m, pre-turn {args['pre_angle']:.3f} deg, "
              f"endpoint ({end['x_m']:.6f}, {end['y_m']:.6f}); waiting for a new report.")
        self.assertEqual(self.respond(),'s0idle');self.assertEqual(original,self.target);self.assertEqual(len(self.saved),1)
    def test_confirmed_too_far_after_correction(self):
        self.issue();self.assertEqual(self.respond(.20),'s7stopped');self.assertEqual(len(self.saved),1)
    def test_weak_pose_after_correction(self):
        self.issue();self.assertEqual(self.respond(.01,'LOW'),'s7stopped')
    def test_old_report(self):
        self.issue();self.assertEqual(self.respond(last_command_received_ts=1794269040.),'s7stopped')
    def test_mismatched_command(self):
        self.issue();self.assertEqual(self.respond(last_command_args={'distance_m':2.}),'s7stopped')
    def test_dynamic_block(self):
        self.peer[62,90]=1;self.assertEqual(self.go(),'s7stopped');self.assertFalse(self.saved)
    def test_static_block(self):
        self.static[62,90]=1;self.assertEqual(self.go(),'s7stopped');self.assertFalse(self.saved)
    def test_normal_ramp_check_even_inside_macro(self):
        self.policy['ramp_geometry']['restriction_zone'].update(y_min_m=6.2,y_max_m=6.3)
        self.assertEqual(self.go(),'s7stopped');self.assertFalse(self.saved)
    def test_no_second_issue(self):
        args=self.issue();self.seq=[dict(action=rules.ACTION,args=args)];self.count=0
        self.assertEqual(self.call('_s6_issue_correction')['transition_to'],'s7stopped');self.assertEqual(len(self.saved),1)
    def test_landing_gate_failure(self):
        self.pose['y_m']=5.99
        self.assertEqual(self.go(),'s7stopped');self.assertFalse(self.saved)
    def test_pending_bump_override(self):
        self.overrides={'move_profile':'bump_crossing_up'}
        self.assertEqual(self.go(),'s7stopped');self.assertFalse(self.saved)
    def test_larger_residual_not_authorized(self):
        self.pose['x_m']+=.02;self.assertEqual(self.go(),'s7stopped');self.assertFalse(self.saved)
    def test_disabled(self):
        self.config.MOBILITY_S8_LANDING_ENABLED=False
        self.assertEqual(self.go(),'s7stopped');self.assertFalse(self.saved)
    def test_watchdog_while_waiting_for_report(self):
        args=self.issue();self.now+=121
        self.assertTrue(self.call('movement_delivery_blocked',{'mobility_s1_managed':'1','action':rules.ACTION,'args_json':json.dumps(args)}))
        self.n['_check_s8_deadlines']();self.assertTrue(self.stop['stop']);self.assertEqual(self.call('_get_state'),'s7stopped')
    def test_poll_is_specific_to_planned_command(self):
        args=self.issue();fields={'mobility_s1_managed':'1','action':rules.ACTION,'args_json':json.dumps(args)}
        self.assertFalse(self.call('movement_delivery_blocked',fields))
        self.assertTrue(self.call('movement_delivery_blocked',{**fields,'args_json':'{"distance_m":2}'}))
        self.assertTrue(self.call('movement_delivery_blocked',{**fields,'mobility_s1_managed':'0'}))

if __name__=='__main__':unittest.main(verbosity=2)
