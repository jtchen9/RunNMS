"""Placement-helper checks. Reads the workbook but never launches Excel or robots."""
import csv
import hashlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
COMMON=ROOT/'sitemap/CommonCheckers'
AUTHOR=ROOT/'sitemap/DemoRoom/script_authoring'
sys.path.insert(0,str(COMMON))
from placement_render import placement_records, marker_groups, marker_label, render_placement
from checker.registration_csv import CSV_COLUMNS
from single_csv_authoring import prepare_export

A,B='twin-scout-alpha','twin-scout-bravo'
POLICY=json.loads((AUTHOR/'config/macro_policy.json').read_text())
FIXTURE=Path(__file__).parent/'step2_fixtures/RoundTrip_AlphaBravo.csv'
WORKBOOK=AUTHOR/'AutoLab_DemoRoom_Template.xlsm'

def command(t,action,args=None,scanner=A):
    return [scanner,str(t),action.split('.')[0],action,json.dumps(args or {}),'','','','','']
def data(rows,roster=None):
    rows=[list(r) for r in rows]
    for i,(scanner,xy) in enumerate((roster or {A:(5,3)}).items()):
        while len(rows)<=i:rows.append(['']*10)
        rows[i][8:]=[scanner,str(xy)]
    f=io.StringIO(newline='');csv.writer(f).writerows([CSV_COLUMNS]+rows)
    return prepare_export(f.getvalue().encode('utf-8-sig'),POLICY)

class Metadata(unittest.TestCase):
    def test_fixture_initials_and_reentry_row(self):
        r=placement_records(FIXTURE.read_bytes(),POLICY)
        self.assertEqual([(p['kind'],p['scanner'],p['x_m'],p['y_m'],p['csv_row'],p['time_cell']) for p in r],
                         [('initial',A,5,3,2,None),('initial',B,8.8,3,3,None),('reentry',A,5,3,6,'H6')])
    def test_coincident_initial_and_reentry_label_preserves_both(self):
        r=placement_records(FIXTURE.read_bytes(),POLICY);g=marker_groups(r)
        self.assertEqual(len(g),2);self.assertEqual(marker_label(g[(5,3)]),'Alpha initial, R6')
    def test_blank_g_has_note_but_no_invented_marker(self):
        r=placement_records(data([command(0,'system.shutdown'),command(600,'scan.start')]),POLICY)
        self.assertEqual(len(r),2);self.assertIsNone(r[-1]['x_m']);self.assertEqual(r[-1]['time_cell'],'H3')
        self.assertEqual(len(marker_groups(r)),1)
    def test_relocated_robot_has_two_independent_markers(self):
        r=placement_records(data([command(0,'system.shutdown'),command(600,'mobility.report.location'),command(660,'mobility.move',{'x_m':8.8,'y_m':3})]),POLICY)
        self.assertEqual(set(marker_groups(r)),{(5.,3.),(8.8,3.)})
    def test_repeated_same_point_retains_every_row(self):
        rows=[command(0,'system.shutdown'),command(600,'mobility.report.location'),command(660,'mobility.move',{'x_m':5,'y_m':3}),command(900,'system.shutdown'),command(1500,'mobility.report.location'),command(1560,'mobility.move',{'x_m':5,'y_m':3})]
        r=placement_records(data(rows),POLICY)
        self.assertEqual(marker_label(marker_groups(r)[(5,3)]),'Alpha initial, R3, R6')
    def test_stale_reminder_rejected_instead_of_silently_changed(self):
        rr=list(csv.reader(io.StringIO(FIXTURE.read_text(encoding='utf-8-sig'))));rr[5][6]='(4,3)'
        f=io.StringIO(newline='');csv.writer(f).writerows(rr)
        with self.assertRaises(ValueError):placement_records(f.getvalue().encode(),POLICY)
    def test_h1_is_not_used_as_initial_placement_time(self):
        rr=list(csv.reader(io.StringIO(FIXTURE.read_text(encoding='utf-8-sig'))));rr[0][7]='2026-11-22 20:00:00'
        f=io.StringIO(newline='');csv.writer(f).writerows(rr)
        records=placement_records(f.getvalue().encode(),POLICY)
        self.assertTrue(all(r['time_cell'] is None for r in records if r['kind']=='initial'))
        self.assertEqual(records[-1]['time_cell'],'H6')
    def test_metadata_only_roster_rows_do_not_shift_row_labels(self):
        rows=[command(0,'system.shutdown'),['']*10,command(600,'scan.start'),command(0,'scan.start',scanner=B)]
        records=placement_records(data(rows,{A:(5,3),B:(8.8,3)}),POLICY)
        self.assertEqual(records[-1]['csv_row'],4)

class Rendering(unittest.TestCase):
    def setUp(self):
        self.assertTrue(WORKBOOK.exists(),f'Missing {WORKBOOK}; use your current workbook at this path.')
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.folder=Path(self.tmp.name);self.csv=self.folder/'commands.csv';self.csv.write_bytes(FIXTURE.read_bytes())
        self.out=self.folder/'guide.png'
        self.before=hashlib.sha256(WORKBOOK.read_bytes()).digest()
    def tearDown(self):
        self.assertEqual(hashlib.sha256(WORKBOOK.read_bytes()).digest(),self.before,'Workbook was changed')
        self.assertEqual(self.csv.read_bytes(),getattr(self,'expected_csv',FIXTURE.read_bytes()))
    def test_real_png_and_no_travel_lines(self):
        # Any attempt to plot a route line is a regression in a placement guide.
        with patch('matplotlib.axes.Axes.plot',side_effect=AssertionError('No travel paths allowed')):
            r=render_placement(self.csv,WORKBOOK,self.out,POLICY,dpi=72)
        self.assertEqual(self.out.read_bytes()[:8],b'\x89PNG\r\n\x1a\n')
        self.assertEqual(r['travel_lines'],0);self.assertEqual(r['marker_group_count'],2)
        self.assertFalse(r['execution_authorized']);self.assertFalse(r['safety_checked'])
        self.assertEqual(json.loads(self.out.with_suffix('.json').read_text())['records'],r['records'])
    def test_failed_render_keeps_workbook_csv_and_previous_png(self):
        self.out.write_bytes(b'previous guide')
        with patch('matplotlib.figure.Figure.savefig',side_effect=OSError('simulated export failure')):
            with self.assertRaises(OSError):render_placement(self.csv,WORKBOOK,self.out,POLICY,dpi=72)
        self.assertEqual(self.out.read_bytes(),b'previous guide')
        self.assertFalse(list(self.folder.glob('placement_*')))
    def test_unreliable_map_fails_without_output(self):
        with patch('placement_render.read_static_map',return_value={'messages':['Unsupported fill']}):
            with self.assertRaisesRegex(ValueError,'StaticMap'):render_placement(self.csv,WORKBOOK,self.out,POLICY)
        self.assertFalse(self.out.exists())
    def test_outside_map_fails_without_clamping_marker(self):
        self.expected_csv=data([command(0,'scan.start')],{A:(100,3)});self.csv.write_bytes(self.expected_csv)
        with self.assertRaisesRegex(ValueError,'outside StaticMap'):render_placement(self.csv,WORKBOOK,self.out,POLICY)
        self.assertFalse(self.out.exists())
    def test_input_overwrite_is_rejected(self):
        with self.assertRaises(ValueError):render_placement(self.csv,WORKBOOK,WORKBOOK,POLICY)
    def test_excel_bridge_entry_point(self):
        r=subprocess.run([sys.executable,str(AUTHOR/'autolab_placement_map.py'),'--root',str(ROOT),'--workbook',str(WORKBOOK),'--csv',str(self.csv),'--output',str(self.out)],capture_output=True,text=True)
        self.assertEqual(r.returncode,0,r.stdout+r.stderr);self.assertTrue(self.out.exists())

if __name__=='__main__':unittest.main(verbosity=2)
