"""Offline workbook bridge checks. Does not run Excel, Redis or robots."""
import csv
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
COMMON=ROOT/'sitemap/CommonCheckers'
AUTHOR=ROOT/'sitemap/DemoRoom/script_authoring'
sys.path.insert(0,str(COMMON));sys.path.insert(0,str(ROOT))
from experiment_csv import parse_registration_csv, CsvFormatError, CSV_COLUMNS
from single_csv_authoring import prepare_export, reminder_issues, planned_poses_bytes, expected_reminders
from checker.checker_runner import validate_script
A,B='twin-scout-alpha','twin-scout-bravo'
POLICY=json.loads((AUTHOR/'config/macro_policy.json').read_text())

def cmd(robot=A,t=0,action='scan.start',args='{}',f='',g='',initial='',xy=''):
    return [robot,str(t),action.split('.')[0],action,args,f,g,'',initial,xy]
def raw(rows):
    out=io.StringIO(newline='');csv.writer(out).writerows([CSV_COLUMNS]+rows)
    return out.getvalue().encode('utf-8-sig')
def start():return cmd(initial=A,xy='(5,3)')
def parsed(data):return list(csv.reader(io.StringIO(data.decode('utf-8-sig'))))
def export(rows):return prepare_export(raw(rows),POLICY)

class Export(unittest.TestCase):
    def test_a_e_fields_and_order_unchanged(self):
        rows=[start(),cmd(t=600,action='mobility.move',args=' { "x_m":5,"y_m":2.8 } '),cmd(t=60,action='system.shutdown'),cmd(t=500,action='mobility.report.location')]
        before=parsed(raw(rows));after=parsed(export(rows))
        self.assertEqual([r[:5] for r in before],[r[:5] for r in after])
    def test_reentry_on_first_command_not_first_move(self):
        m=parse_registration_csv(export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600),cmd(t=660,action='mobility.report.location'),cmd(t=720,action='mobility.move',args='{"x_m":5,"y_m":2.8}')]))
        self.assertEqual([(r['row_number'],r['x_m'],r['y_m']) for r in m['reentry_reminders']],[(4,5.,2.8)])
    def test_timestamp_order_used_without_reordering_rows(self):
        m=parse_registration_csv(export([start(),cmd(t=600),cmd(t=60,action='system.shutdown'),cmd(t=700,action='mobility.move',args='{"x_m":5,"y_m":3}')]))
        self.assertEqual(m['reentry_reminders'][0]['row_number'],3)
    def test_equal_time_uses_csv_row_order(self):
        m=parse_registration_csv(export([start(),cmd(t=60,action='system.shutdown'),cmd(t=60)]))
        self.assertEqual(m['reentry_reminders'][0]['row_number'],4)
    def test_next_robot_does_not_steal_reminder(self):
        rows=[start(),cmd(B,initial=B,xy='(8.8,3)'),cmd(t=60,action='system.shutdown'),cmd(B,100),cmd(t=600)]
        m=parse_registration_csv(export(rows));self.assertEqual(m['reentry_reminders'][0]['row_number'],6)
    def test_scan_only_reentry_blank_g(self):
        m=parse_registration_csv(export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600)]))
        self.assertIsNone(m['reentry_reminders'][0]['x_m'])
    def test_last_shutdown_has_no_reentry(self):self.assertEqual(parse_registration_csv(export([start(),cmd(t=60,action='system.shutdown')]))['reentry_reminders'],[])
    def test_repeated_cycles(self):
        m=parse_registration_csv(export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600),cmd(t=900,action='system.shutdown'),cmd(t=1500)]))
        self.assertEqual([r['row_number'] for r in m['reentry_reminders']],[4,6])
    def test_next_shutdown_stops_destination_search(self):
        m=parse_registration_csv(export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600),cmd(t=900,action='system.shutdown'),cmd(t=1500,action='mobility.move',args='{"x_m":5,"y_m":3}')]))
        self.assertIsNone(m['reentry_reminders'][0]['x_m']);self.assertEqual(m['reentry_reminders'][1]['x_m'],5)
    def test_symbolic_launch_resolves_only_reminder(self):
        text='{"x_m":"IN2OUT","y_m":"IN2OUT"}'
        m=parse_registration_csv(export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600,action='mobility.report.location'),cmd(t=660,action='mobility.move',args=text),cmd(t=900,action='mobility.in2out')]))
        self.assertEqual(m['commands'][3]['args']['x_m'],'IN2OUT')
        self.assertEqual(m['reentry_reminders'][0]['y_m'],POLICY['macros']['mobility.in2out']['start_y_m'])
    def test_macro_first_rejected(self):
        with self.assertRaises(CsvFormatError):export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600,action='mobility.in2out')])
    def test_unknown_launch_rejected(self):
        with self.assertRaises(CsvFormatError):export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600,action='mobility.move',args='{"x_m":"bad","y_m":"bad"}')])
    def test_relative_reentry_target_rejected(self):
        with self.assertRaises(CsvFormatError):export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600,action='mobility.move',args='{"dx_m":1,"dy_m":0}')])
    def test_stale_reminders_regenerated(self):
        m=parse_registration_csv(export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600,f=B,g='not coordinates')]))
        self.assertEqual(m['reentry_reminders'][0]['scanner'],A);self.assertIsNone(m['reentry_reminders'][0]['x_m'])
    def test_formulas_and_roster_only_row(self):
        rows=[start(),['']*8+[B,'(8.8,3)'],cmd(B,60)]
        rr=parsed(export(rows));self.assertEqual(rr[1][7],'=$H$1+B2/86400');self.assertEqual(rr[2][7],'');self.assertEqual(rr[3][7],'=$H$1+B4/86400')
        self.assertEqual(rr[2][8:],[B,'(8.8,3)'])
    def test_multiple_initial_roster_independent_of_reminder(self):
        rr=parsed(export([cmd(action='system.shutdown',initial=A,xy='(5,3)'),cmd(t=600,initial=B,xy='(8.8,3)'),cmd(B,600)]))
        self.assertEqual(rr[2][5],A);self.assertEqual(rr[2][8],B)
    def test_missing_initial_location_rejected(self):
        with self.assertRaises(CsvFormatError):export([cmd()])
    def test_repeat_export_idempotent(self):
        data=export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600)])
        self.assertEqual(prepare_export(data,POLICY),data)
    def test_import_validation_does_not_generate_commands(self):
        data=export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600)])
        self.assertEqual(parse_registration_csv(data)['command_count'],3)
    def test_reminder_missing_diagnosed(self):self.assertEqual(reminder_issues(parse_registration_csv(raw([start(),cmd(t=60,action='system.shutdown'),cmd(t=600)])),POLICY)[0]['code'],'REENTRY_REMINDER_MISMATCH')
    def test_wrong_reminder_coordinate_diagnosed(self):
        m=parse_registration_csv(export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600,action='mobility.move',args='{"x_m":5,"y_m":3}')]))
        m['reentry_reminders'][0]['x_m']=4;self.assertTrue(reminder_issues(m,POLICY))
    def test_good_reminders_pass(self):
        m=parse_registration_csv(export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600)]));self.assertEqual(reminder_issues(m,POLICY),[])

class Integration(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.path=Path(self.temp.name)
    def validate(self,data,normalized=None):
        p=self.path/'experiment.csv';p.write_bytes(data)
        return validate_script(script_csv=p,site_dir=AUTHOR.parent,common_dir=COMMON,normalized_script_csv=normalized)
    def test_checker_reads_ij_without_poses_file(self):
        r=self.validate(export([cmd(action='mobility.move',args='{"x_m":5,"y_m":2.8}',initial=A,xy='(5,3)')]))
        self.assertTrue(r['ok'],r['issues']);self.assertFalse(r['csv_rewritten'])
    def test_checker_does_not_overwrite_input(self):
        data=export([start()]);self.validate(data,self.path/'experiment.csv');self.assertEqual((self.path/'experiment.csv').read_bytes(),data)
    def test_explicit_output_retains_ten_columns(self):
        data=export([start()]);self.validate(data,self.path/'copied.csv');self.assertEqual((self.path/'copied.csv').read_bytes(),data)
    def test_checker_still_rejects_restricted_move(self):
        r=self.validate(export([cmd(action='mobility.move',args='{"x_m":0,"y_m":0}',initial=A,xy='(5,3)')]))
        self.assertFalse(r['ok'])
    def test_checker_reentry_missing_report_rejected(self):
        r=self.validate(export([start(),cmd(t=60,action='system.shutdown'),cmd(t=600,action='mobility.move',args='{"x_m":5,"y_m":3}')]))
        self.assertIn('REENTRY_REQUIRES_LOCATION_REPORT',[i['code'] for i in r['issues']])
    def test_cli_export(self):
        p=self.path/'input.csv';p.write_bytes(raw([start(),cmd(t=60,action='system.shutdown'),cmd(t=600)]));o=self.path/'output.csv'
        r=subprocess.run([sys.executable,str(AUTHOR/'autolab_single_csv.py'),'--root',str(ROOT),'--input',str(p),'--output',str(o)],capture_output=True,text=True)
        self.assertEqual(r.returncode,0,r.stderr);self.assertEqual(parse_registration_csv(o.read_bytes())['reentry_reminders'][0]['row_number'],4)
    def test_cli_import_keeps_bytes(self):
        p=self.path/'input.csv';data=export([start()]);p.write_bytes(data);o=self.path/'output.csv'
        r=subprocess.run([sys.executable,str(AUTHOR/'autolab_single_csv.py'),'--root',str(ROOT),'--input',str(p),'--output',str(o),'--validate-only'],capture_output=True,text=True)
        self.assertEqual(r.returncode,0,r.stderr);self.assertEqual(o.read_bytes(),data)
    def test_cli_bad_import_does_not_create_output(self):
        p=self.path/'input.csv';p.write_text('bad');o=self.path/'output.csv'
        r=subprocess.run([sys.executable,str(AUTHOR/'autolab_single_csv.py'),'--root',str(ROOT),'--input',str(p),'--output',str(o),'--validate-only'],capture_output=True,text=True)
        self.assertNotEqual(r.returncode,0);self.assertFalse(o.exists())
    def test_checker_cli_one_file_and_feedback(self):
        p=self.path/'input.csv';p.write_bytes(export([start()]));report=self.path/'report.json';feedback=self.path/'feedback.csv'
        r=subprocess.run([sys.executable,str(AUTHOR/'autolab_xlsm_run_checker.py'),'--script_csv',str(p),'--site_dir',str(AUTHOR.parent),'--common_dir',str(COMMON),'--report_json',str(report),'--feedback_csv',str(feedback)],capture_output=True,text=True)
        self.assertEqual(r.returncode,0,r.stdout+r.stderr);self.assertTrue(json.loads(report.read_text())['ok']);self.assertTrue(feedback.exists())
    def reconstruct(self,rows):
        from route_reconstruct import reconstruct
        data=export(rows);p=self.path/'commands.csv';p.write_bytes(data);poses=self.path/'internal.csv';poses.write_bytes(planned_poses_bytes(parse_registration_csv(data)))
        return reconstruct(p,poses,macro_policy=AUTHOR/'config/macro_policy.json')
    def test_route_breaks_at_shutdown(self):
        r=self.reconstruct([cmd(action='mobility.move',args='{"x_m":5,"y_m":2.8}',initial=A,xy='(5,3)'),cmd(t=60,action='system.shutdown'),cmd(t=600,action='mobility.report.location'),cmd(t=660,action='mobility.move',args='{"x_m":8.8,"y_m":3}')])
        points=r['robots'][A]['points'];self.assertEqual(len(points),2);self.assertTrue(points[0]['segment_known']);self.assertFalse(points[1]['segment_known']);self.assertEqual(points[1]['status'],'destination_only')
    def test_route_understands_authored_launch_constant(self):
        cfg=POLICY['macros']['mobility.in2out']
        r=self.reconstruct([cmd(action='mobility.move',args='{"x_m":"IN2OUT","y_m":"IN2OUT"}',initial=A,xy=f"({cfg['start_x_m']},{cfg['start_y_m']})"),cmd(t=240,action='mobility.in2out')])
        self.assertEqual(r['status'],'ok',r['diagnostics']);self.assertEqual(r['robots'][A]['points'][0]['y_m'],cfg['start_y_m'])

class ExcelComparison(unittest.TestCase):
    def data(self):return (Path(__file__).parent/'step2_fixtures/RoundTrip_AlphaBravo.csv').read_bytes()
    def test_reference_compares_equal(self):
        from check_excel_export import compare
        self.assertTrue(compare(self.data(),self.data(),POLICY).startswith('PASS:'))
    def test_changed_command_detected(self):
        from check_excel_export import compare
        altered=self.data().replace(b'scan.start',b'scan.stop',1)
        with self.assertRaises(ValueError):compare(self.data(),altered,POLICY)
    def test_changed_roster_detected(self):
        from check_excel_export import compare
        altered=self.data().replace(b'"(5,3)"',b'"(4,3)"',1)
        with self.assertRaises(ValueError):compare(self.data(),altered,POLICY)
    def test_changed_time_formula_detected(self):
        from check_excel_export import compare
        altered=self.data().replace(b'=$H$1+B2/86400',b'=$H$1+B3/86400',1)
        with self.assertRaises(ValueError):compare(self.data(),altered,POLICY)

if __name__=='__main__':unittest.main(verbosity=2)
