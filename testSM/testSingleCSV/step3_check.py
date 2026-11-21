"""Re-entry planning checks; no robot commands, Redis or Excel required."""
import csv
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMMON = ROOT / 'sitemap/CommonCheckers'
SITE = ROOT / 'sitemap/DemoRoom'
sys.path.insert(0, str(COMMON))
from checker.checker_runner import validate_script
from checker.registration_csv import CSV_COLUMNS
from checker.script_model import InitialPose, ScriptRow
from checker.concurrent_movement_rules import planned_segments
from checker.timeline_rules import check_global_moving_command_spacing
from checker.movement_rules import check_max_single_mobility_move_distance
from single_csv_authoring import prepare_export

A, B = 'twin-scout-alpha', 'twin-scout-bravo'
MACROS = json.loads((SITE/'script_authoring/config/macro_policy.json').read_text())
TIMING = {'moving_mobility_actions': ['mobility.move', 'mobility.in2out', 'mobility.out2in'],
          'global_min_seconds_between_moving_mobility_commands': 180,
          'concurrent_movement_min_path_separation_m': 1.0}

def row(t, action, args=None, scanner=A):
    return [scanner, str(t), action.split('.')[0], action,
            json.dumps(args or {}), '', '', '', '', '']
def move(t, x, y, scanner=A):
    return row(t, 'mobility.move', {'x_m': x, 'y_m': y}, scanner)
def pose(name, x, y): return InitialPose(2, name, x, y, 0, .2, 10)
def model(n, t, action, args=None, scanner=A):
    return ScriptRow(n, scanner, t, action.split('.')[0], action, args or {})
def m(n, t, x, y, scanner=A):
    return model(n, t, 'mobility.move', {'x_m': x, 'y_m': y}, scanner)
def entry(x=8.8, y=3):
    return [row(0,'system.shutdown'), row(600,'mobility.report.location'), move(660,x,y)]

class Checker(unittest.TestCase):
    def check(self, rows, roster=None, edit=None):
        rows = [list(r) for r in rows]
        for i, (name, xy) in enumerate((roster or {A:(5,3)}).items()):
            while len(rows) <= i: rows.append(['']*10)
            rows[i][8:] = [name, str(xy)]
        f=io.StringIO(newline='');csv.writer(f).writerows([CSV_COLUMNS]+rows)
        data=prepare_export(f.getvalue().encode('utf-8-sig'),MACROS)
        if edit: data=edit(data)
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'commands.csv';p.write_bytes(data)
            result=validate_script(script_csv=p,site_dir=SITE,common_dir=COMMON)
            self.assertEqual(p.read_bytes(),data,'Checker rewrote authored commands')
        return result
    def codes(self,r):return {i['code'] for i in r['issues']}
    def test_manual_relocation_does_not_draw_path_through_furniture(self):
        r=self.check(entry());self.assertTrue(r['ok'],r['issues'])
        self.assertIn('REENTRY_APPROACH_REQUIRES_RUNTIME_CHECK',self.codes(r))
    def test_same_move_without_shutdown_still_checks_full_path(self):
        r=self.check([move(660,8.8,3)])
        self.assertFalse(r['ok']);self.assertIn('MOBILITY_MOVE_DISTANCE_TOO_LONG',self.codes(r))
        self.assertIn('PATH_CROSSES_RESTRICTION_MAP',self.codes(r))
    def test_reentry_static_restricted_endpoint_rejected(self):
        r=self.check(entry(6,3));self.assertFalse(r['ok']);self.assertIn('PATH_CROSSES_RESTRICTION_MAP',self.codes(r))
    def test_reentry_out_of_bounds_endpoint_rejected(self):
        r=self.check(entry(100,100));self.assertIn('PATH_ENDPOINT_OUT_OF_BOUNDS',self.codes(r))
    def test_reentry_bump_endpoint_rejected(self):
        r=self.check(entry(9.1,5));self.assertIn('PLANNING_BUMP_MOVE',self.codes(r))
    def test_subsequent_path_still_checked(self):
        r=self.check(entry()+[move(900,7.5,3)])
        self.assertIn('PATH_CROSSES_RESTRICTION_MAP',self.codes(r))
    def test_subsequent_distance_still_checked(self):
        r=self.check(entry()+[move(900,5,3)])
        self.assertIn('MOBILITY_MOVE_DISTANCE_TOO_LONG',self.codes(r))
    def test_report_remains_required(self):
        r=self.check([row(0,'system.shutdown'),move(660,8.8,3)])
        self.assertIn('REENTRY_REQUIRES_LOCATION_REPORT',self.codes(r))
    def test_report_spacing_remains_required(self):
        r=self.check([row(0,'system.shutdown'),row(650,'mobility.report.location'),move(660,8.8,3)])
        self.assertFalse(r['ok'])
        self.assertTrue(any('SOON' in c or 'SPACING' in c for c in self.codes(r)),r['issues'])
    def test_macro_after_staging_passes(self):
        r=self.check(entry('IN2OUT','IN2OUT')+[row(900,'mobility.in2out')])
        self.assertTrue(r['ok'],r['issues'])
    def test_macro_wrong_staging_rejected(self):
        r=self.check(entry()+[row(900,'mobility.in2out')]);self.assertFalse(r['ok'])
        self.assertTrue(any('MACRO' in c and 'START' in c for c in self.codes(r)),r['issues'])
    def test_macro_first_rejected_even_with_forged_valid_reminder(self):
        def edit(data):
            rr=list(csv.reader(io.StringIO(data.decode('utf-8-sig'))))
            rr[3][3:5]=['mobility.in2out','{}']
            f=io.StringIO(newline='');csv.writer(f).writerows(rr);return f.getvalue().encode('utf-8-sig')
        r=self.check(entry(),edit=edit)
        self.assertFalse(r['ok']);self.assertTrue(any('REENTRY' in c for c in self.codes(r)))
    def test_g_reminder_cannot_change_geometry(self):
        def edit(data):
            rr=list(csv.reader(io.StringIO(data.decode('utf-8-sig'))));rr[2][6]='(8.8,3)'
            f=io.StringIO(newline='');csv.writer(f).writerows(rr);return f.getvalue().encode('utf-8-sig')
        r=self.check(entry(6,3),edit=edit)
        self.assertIn('REENTRY_REMINDER_MISMATCH',self.codes(r));self.assertIn('PATH_CROSSES_RESTRICTION_MAP',self.codes(r))
    def test_multiple_cycles_warn_once_per_unknown_approach(self):
        r=self.check(entry()+[row(900,'system.shutdown'),row(1500,'scan.start'),row(1560,'mobility.report.location'),move(1620,5,3)])
        self.assertTrue(r['ok'],r['issues'])
        self.assertEqual(sum(i['code']=='REENTRY_APPROACH_REQUIRES_RUNTIME_CHECK' for i in r['issues']),2)
    def test_known_other_robot_at_destination_rejected(self):
        r=self.check(entry()+[row(0,'scan.start',scanner=B)],{A:(5,3),B:(8.8,3)})
        self.assertFalse(r['ok']);self.assertIn('ROBOT_FINAL_POSE_TOO_CLOSE',self.codes(r))
    def test_offline_robot_old_position_not_used_as_known_obstacle(self):
        r=self.check([row(0,'system.shutdown',scanner=B)]+entry(),{A:(5,3),B:(8.8,3)})
        self.assertTrue(r['ok'],r['issues'])
    def test_no_movement_reentry_needs_no_geometry(self):
        r=self.check([row(0,'system.shutdown'),row(600,'scan.start')]);self.assertTrue(r['ok'],r['issues'])
        self.assertNotIn('REENTRY_APPROACH_REQUIRES_RUNTIME_CHECK',self.codes(r))

class Geometry(unittest.TestCase):
    def setUp(self):self.poses={A:pose(A,0,0),B:pose(B,20,20)}
    def rows(self):return [model(2,0,'system.shutdown'),model(3,500,'mobility.report.location'),m(4,600,1,1),m(5,900,2,1)]
    def test_unknown_approach_has_no_segment_subsequent_move_does(self):
        s=planned_segments(self.rows(),self.poses,{});self.assertNotIn(4,s);self.assertEqual(s[5],((1.,1.),(2.,1.)))
    def test_report_does_not_invent_a_measured_pose(self):
        self.assertEqual(planned_segments(self.rows()[:3],self.poses,{}),{})
    def test_unknown_approach_cannot_waive_concurrent_spacing(self):
        rows=self.rows()[:3]+[m(6,600,21,20,B)]
        r=check_global_moving_command_spacing(rows,TIMING,self.poses,{})
        self.assertEqual(len(r),1);self.assertIsNone(r[0]['actual_path_separation_m'])
    def test_known_segments_can_still_run_concurrently(self):
        r=check_global_moving_command_spacing(self.rows()+[m(6,900,21,20,B)],TIMING,self.poses,{})
        self.assertEqual(r,[])
    def test_chronological_order_and_equal_time_row_order(self):
        rows=[m(2,0,1,0),model(3,0,'system.shutdown'),model(4,0,'mobility.report.location'),m(5,0,10,10)]
        s=planned_segments(rows[::-1],self.poses,{})
        self.assertIn(2,s);self.assertNotIn(5,s)
    def test_other_robot_geometry_unchanged(self):
        s=planned_segments(self.rows()+[m(6,700,21,20,B)],self.poses,{})
        self.assertEqual(s[6],((20.,20.),(21.,20.)))
    def test_relative_first_move_cannot_use_pre_shutdown_pose(self):
        rows=self.rows()[:2]+[model(4,600,'mobility.move',{'dx_m':1})]
        r=check_max_single_mobility_move_distance(rows,self.poses,{})
        self.assertEqual(r[0]['code'],'REENTRY_REQUIRES_ABSOLUTE_TARGET')
    def test_distance_after_macro_uses_macro_landing(self):
        macro={'macros':{'mobility.in2out':{'start_x_m':0,'start_y_m':0,'target_heading_deg':90,'distance_m':2}}}
        rows=[model(2,0,'mobility.in2out'),m(3,300,0,4)]
        self.assertEqual(check_max_single_mobility_move_distance(rows,self.poses,{'max_single_mobility_move_distance_m':3},macro),[])

if __name__=='__main__':unittest.main(verbosity=2)
