"""Offline single-CSV contract/preparation tests. No installed config or robot I/O.
Run from NMS root: python testSM/testSingleCSV/step1_check.py
Requires the project's existing fastapi and numpy dependencies.
"""
import ast
import asyncio
import copy
import csv
import io
import sys
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'sitemap/CommonCheckers'))
from experiment_csv import parse_registration_csv, CsvFormatError, CSV_COLUMNS
from nms_preparation_positioning import position_once, PositioningError
from checker.initialization_rules import check_prepared_mobility_sequence
from checker.script_model import ScriptRow
A,B='twin-scout-alpha','twin-scout-bravo'


def command(robot=A,offset=0,action='scan.start',args='{}',reminder='',xy='',initial='',initial_xy=''):
    return [robot,str(offset),action.split('.')[0],action,args,reminder,xy,'',initial,initial_xy]

def encode(rows,header=None):
    f=io.StringIO(newline='');csv.writer(f).writerows([header or CSV_COLUMNS]+rows)
    return f.getvalue().encode('utf-8-sig')

def basic():return [command(initial=A,initial_xy='(5,3)')]

class Format(unittest.TestCase):
    def bad(self,rows,code,header=None,poses=None):
        with self.assertRaises(CsvFormatError) as caught:parse_registration_csv(encode(rows,header),poses)
        self.assertEqual(caught.exception.code,code)
    def test_supplied_format_fixtures(self):
        files=list((Path(__file__).parent/'fixtures').glob('*.csv'))
        self.assertEqual(len(files),6)
        for path in files:
            with self.subTest(fixture=path.name):
                m=parse_registration_csv(path.read_bytes())
                self.assertFalse(m['execution_authorized'])
                self.assertGreater(m['command_count'],0)
    def test_single_robot(self):
        m=parse_registration_csv(encode(basic()))
        self.assertEqual(m['schema_version'],'experiment_csv_v2')
        self.assertEqual(m['initial_positions'][0]['x_m'],5)
        self.assertIsNone(m['initial_positions'][0]['heading_deg'])
        self.assertFalse(m['execution_authorized'])
    def test_four_scan_only_robots(self):
        robots=[A,B,'twin-scout-charlie','twin-scout-delta']
        m=parse_registration_csv(encode([command(r,initial=r,initial_xy=f'({i},3)') for i,r in enumerate(robots)]))
        self.assertEqual(m['robot_count'],4)
    def test_roster_longer_than_commands(self):
        m=parse_registration_csv(encode([command(initial=A,initial_xy='(5,3)'),['']*8+[B,'(4,3)'],command(B,10)]))
        self.assertEqual([c['row_number'] for c in m['commands']],[2,4])
        self.assertEqual(m['robot_count'],2)
    def test_initial_roster_and_reminder_share_row_without_collision(self):
        rows=[command(action='system.shutdown',initial=A,initial_xy='(5,3)'),
              command(A,600,reminder=A,initial=B,initial_xy='(4,3)'),command(B,600)]
        m=parse_registration_csv(encode(rows))
        self.assertEqual(m['initial_positions'][1]['scanner'],B)
        self.assertEqual(m['reentry_reminders'][0]['scanner'],A)
        self.assertIsNone(m['reentry_reminders'][0]['x_m'])
    def test_repeated_reentry(self):
        rows=basic()+[command(A,60,'system.shutdown'),command(A,600,'mobility.report.location',reminder=A,xy='(5,3)'),
                      command(A,900,'system.shutdown'),command(A,1500,'scan.start',reminder=A)]
        m=parse_registration_csv(encode(rows))
        self.assertEqual([r['row_number'] for r in m['reentry_reminders']],[4,6])
        self.assertTrue(all(r['execution_effect']=='none' for r in m['reentry_reminders']))
    def test_a_to_e_and_formulas_preserved_not_evaluated(self):
        rows=basic();rows[0][7]='=$H$1+B2/86400';rows[0][4]=' { "note": "x" } '
        header=CSV_COLUMNS.copy();header[7]='2026-11-21 18:00:00'
        m=parse_registration_csv(encode(rows,header))
        self.assertEqual(m['commands'][0]['original_a_to_e'],rows[0][:5])
        self.assertEqual(m['commands'][0]['display_time_cell'],rows[0][7])
        self.assertEqual(m['start_time_cell'],header[7])
    def test_symbolic_move_remains_authored_until_checker(self):
        rows=basic()+[command(A,120,'mobility.move','{"x_m":"IN2OUT","y_m":"IN2OUT"}')]
        self.assertEqual(parse_registration_csv(encode(rows))['commands'][1]['args']['x_m'],'IN2OUT')
    def test_ap_not_in_robot_roster(self):
        rows=basic()+[command('ap-test',0,'ap.backhaul.steer')]
        self.assertEqual(parse_registration_csv(encode(rows))['robot_count'],1)
    def test_metadata_does_not_create_commands(self):
        rows=basic()+[command(A,600,reminder=A,xy='(999,999)')]
        m=parse_registration_csv(encode(rows))
        self.assertEqual(m['command_count'],2)
        self.assertNotIn('x_m',m['commands'][1]['args'])
    def test_old_format_rejected(self):self.bad(basic(),'SINGLE_CSV_HEADER',CSV_COLUMNS[:8])
    def test_separate_poses_rejected(self):self.bad(basic(),'OBSOLETE_POSES_FILE',poses=b'anything')
    def test_missing_roster(self):self.bad([command()],'MISSING_INITIAL_BLOCK')
    def test_missing_robot(self):self.bad(basic()+[command(B)],'MISSING_INITIAL_ROBOTS')
    def test_unused_robot(self):self.bad(basic()+[['']*8+[B,'(4,3)']],'UNUSED_INITIAL_ROBOTS')
    def test_duplicate_robot(self):self.bad(basic()+[command(A,60,initial=A,initial_xy='(4,3)')],'DUPLICATE_INITIAL_ROBOT')
    def test_roster_gap(self):self.bad(basic()+[command(A,60),command(B,120,initial=B,initial_xy='(4,3)')],'INITIAL_ROSTER_GAP')
    def test_incomplete_roster(self):self.bad([command(initial=A)],'INCOMPLETE_INITIAL_PAIR')
    def test_bad_initial_xy(self):self.bad([command(initial=A,initial_xy='5,3')],'BAD_XY')
    def test_bad_reminder_xy(self):self.bad(basic()+[command(A,60,reminder=A,xy='(nan,3)')],'BAD_XY')
    def test_overflow_xy(self):self.bad([command(initial=A,initial_xy='(1e999,3)')],'BAD_NUMBER')
    def test_g_without_f(self):self.bad(basic()+[command(A,60,xy='(5,3)')],'REMINDER_WITHOUT_ROBOT')
    def test_unknown_reminder(self):self.bad(basic()+[command(A,60,reminder=B)],'UNKNOWN_REMINDER_ROBOT')
    def test_wrong_robot_reminder(self):self.bad(basic()+[command(B,60,initial=B,initial_xy='(4,3)',reminder=A)],'REMINDER_ROBOT_MISMATCH')
    def test_reminder_without_command(self):self.bad(basic()+[['']*5+[A,'','','','']],'REMINDER_WITHOUT_COMMAND')
    def test_extra_column(self):self.bad([basic()[0]+['x']],'EXTRA_COLUMNS')
    def test_invalid_offsets(self):
        for v in ('-1','1.5','tomorrow'):
            rows=basic();rows[0][1]=v
            with self.subTest(v=v):self.bad(rows,'BAD_OFFSET')
    def test_bad_json(self):
        for v in ('[]','{"x": NaN}','{"x":1e999}','bad'):
            rows=basic();rows[0][4]=v
            with self.subTest(v=v):self.bad(rows,'BAD_ARGS_JSON')
    def test_empty_script(self):self.bad([['']*8+[A,'(5,3)']],'EMPTY_SCRIPT')

class Sequence(unittest.TestCase):
    policy={'moving_mobility_actions':['mobility.move','mobility.in2out','mobility.out2in'],
            'min_seconds_after_report_location_before_first_move':60}
    def issues(self,items):
        rows=[ScriptRow(i+2,A,t,a.split('.')[0],a,{}) for i,(t,a) in enumerate(items)]
        return [x['code'] for x in check_prepared_mobility_sequence(rows,self.policy)]
    def test_initial_move_needs_no_script_report(self):self.assertEqual(self.issues([(0,'mobility.move')]),[])
    def test_later_report_not_compared_to_initial_move(self):self.assertEqual(self.issues([(0,'mobility.move'),(60,'mobility.report.location')]),[])
    def test_reentry_requires_new_report(self):self.assertIn('REENTRY_REQUIRES_LOCATION_REPORT',self.issues([(0,'mobility.report.location'),(60,'system.shutdown'),(600,'mobility.move')]))
    def test_reentry_report_then_move(self):self.assertEqual(self.issues([(0,'system.shutdown'),(600,'scan.start'),(610,'mobility.report.location'),(670,'mobility.move')]),[])
    def test_reentry_macro_needs_staging(self):self.assertIn('REENTRY_REQUIRES_STAGING_MOVE',self.issues([(0,'system.shutdown'),(600,'mobility.report.location'),(660,'mobility.in2out')]))
    def test_scan_only_reentry_needs_no_report(self):self.assertEqual(self.issues([(0,'system.shutdown'),(600,'scan.start')]),[])
    def test_report_spacing_retained(self):self.assertIn('FIRST_MOVE_TOO_SOON_AFTER_REPORT_LOCATION',self.issues([(0,'system.shutdown'),(600,'mobility.report.location'),(601,'mobility.move')]))
    def test_second_reentry_resets_report(self):self.assertIn('REENTRY_REQUIRES_LOCATION_REPORT',self.issues([(0,'system.shutdown'),(600,'mobility.report.location'),(660,'mobility.move'),(800,'system.shutdown'),(1400,'mobility.move')]))

class PublicValidation(unittest.TestCase):
    def validate(self,rows):
        from nms_preparation_registration import validate
        helper=types.ModuleType('preferred_direction_helper')
        helper.lookup_preferred_direction=lambda **kw:dict(ok=True,preferred_heading_deg=270)
        with patch.dict(sys.modules,{'preferred_direction_helper':helper}):
            return validate(encode(rows),None,ROOT,'DemoRoom')
    def test_move_at_zero_passes_real_checker_without_initial_report(self):
        m,r=self.validate([command(action='mobility.move',args='{"x_m":5,"y_m":2.8}',initial=A,initial_xy='(5,3)')])
        self.assertTrue(r['ok']);self.assertEqual(m['commands'][0]['row_number'],2)
    def test_restricted_path_still_rejected(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):self.validate([command(action='mobility.move',args='{"x_m":0,"y_m":0}',initial=A,initial_xy='(5,3)')])
    def test_forbidden_primitive_still_rejected(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):self.validate([command(action='mobility.turn',args='{"angle_deg":90}',initial=A,initial_xy='(5,3)')])
    def test_metadata_only_row_preserves_original_command_number(self):
        m,r=self.validate([['']*8+[A,'(5,3)'],command(action='mobility.move',args='{"x_m":5,"y_m":2.8}')])
        self.assertEqual(m['commands'][0]['row_number'],3)

class Port:
    """Fresh camera evidence without any real robot, configuration or Redis."""
    final_tolerance_m=.3
    def __init__(self,x=3.,y=3.,blocked=False,final_x=5.):
        self.initial=(x,y);self.final=(final_x,3.);self.blocked=blocked
        self.seconds=0;self.count=0;self.commands=[];self.evidence={};self.pose={};self.closed=False
    def local_ts(self):return (datetime(2026,11,21,18)+timedelta(seconds=self.seconds)).strftime('%Y-%m-%d-%H:%M:%S')
    async def sleep(self,seconds):self.seconds+=seconds
    def open(self,scanner,**kw):self.scope=kw['scope']
    def close(self,scanner,**kw):self.closed=True
    def snapshot(self,scanner):return copy.deepcopy(dict(state='s0idle',pose=self.pose,evidence=self.evidence))
    def issue(self,scanner,action,args,**kw):
        self.commands.append(action);self.seconds+=1;self.count+=1
        x,y=self.initial if self.count==1 else self.final
        self.pose=dict(location_ok=True,source='apriltag',x_m=x,y_m=y,heading_deg=270,confidence='HIGH')
        stamp=self.local_ts()
        report=dict(last_exec_status='completed',last_command_received_ts=stamp,last_command_finished_ts=stamp,
                    last_location_result=dict(ok=True,snapshots={'front':f'/tmp/{self.count}.jpg'}))
        self.evidence=dict(scope=self.scope,solve_id=str(self.count),pose=copy.deepcopy(self.pose),issued_at=stamp,solved_at=stamp,report=report)
        return {'status':'ok'}
    def check_path(self,*args):
        if self.blocked:raise PositioningError('blocked path')
        return {'path_ok':True}
    def move(self,*args,**kw):self.commands.append('mobility.move');return {'status':'ok'}

class Placement(unittest.TestCase):
    def run_port(self,p):return asyncio.run(position_once(scanner=A,target={'x_m':5,'y_m':3},port=p,owns_attempt=lambda:True,monotonic=lambda:p.seconds,sleep=p.sleep))
    def test_exact_two_meters(self):self.assertEqual(self.run_port(Port())['status'],'ready')
    def test_between_one_and_two(self):self.assertEqual(self.run_port(Port(x=3.5))['status'],'ready')
    def test_beyond_two_no_move(self):
        p=Port(x=2.999);self.assertEqual(self.run_port(p)['status'],'failed');self.assertEqual(p.commands,['mobility.report.location'])
    def test_euclidean_distance(self):self.assertEqual(self.run_port(Port(x=3.5,y=1.5))['status'],'failed')
    def test_path_rejection_retained(self):
        p=Port(blocked=True);self.assertEqual(self.run_port(p)['status'],'failed');self.assertNotIn('mobility.move',p.commands)
    def test_final_tolerance_unchanged(self):
        for x,expected in ((5.3,'ready'),(5.301,'failed')):
            with self.subTest(x=x):self.assertEqual(self.run_port(Port(final_x=x))['status'],expected)

class Endpoint(unittest.TestCase):
    def preview(self,raw,poses=None):
        from fastapi import HTTPException
        class Upload:
            filename='test.csv'
            def __init__(self,content):self.content=content
            async def read(self):return self.content
        tree=ast.parse((ROOT/'m4Commands.py').read_text(encoding='utf-8-sig'))
        fn=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='cmd_preview_registration_csv')
        fn.decorator_list=[]
        env=dict(UploadFile=Upload,File=lambda *a,**kw:None,Optional=Optional,Dict=Dict,Any=Any,HTTPException=HTTPException)
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'m4Commands.py','exec'),env)
        return asyncio.run(env[fn.name](Upload(raw),None if poses is None else Upload(poses)))
    def test_preview_reads_new_roster_without_registration(self):
        r=self.preview(encode(basic()))
        self.assertFalse(r['registered']);self.assertEqual(r['commands_enqueued'],0)
        self.assertEqual(r['manifest']['initial_positions'][0]['scanner'],A)
    def test_preview_rejects_old_format_with_422(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as caught:self.preview(encode(basic(),CSV_COLUMNS[:8]))
        self.assertEqual(caught.exception.status_code,422)
    def test_preview_rejects_extra_pose_upload(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as caught:self.preview(encode(basic()),b'poses')
        self.assertEqual(caught.exception.detail['code'],'OBSOLETE_POSES_FILE')
    def test_manifest_enters_preparation_with_ij_target(self):
        from nms_experiment_preparation import Preparation
        m=parse_registration_csv(encode(basic()))
        prep=Preparation(lab_id='DemoRoom',experiment_id='test',session_id='test',manifest=m,now=datetime(2026,11,21,18))
        self.assertEqual(prep.target_for(A),{'x_m':5.,'y_m':3.})
    def test_compiler_ignores_reminder_and_h_and_preserves_order(self):
        from nms_preparation_registration import compile_operations
        rows=basic()+[command(A,600,reminder=A,xy='(999,999)'),command(A,600,'scan.stop')]
        rows[1][7]='invalid display time'
        m=parse_registration_csv(encode(rows))
        def mod(name,**values):
            value=types.ModuleType(name);value.__dict__.update(values);return value
        config=mod('config',TIME_FMT='%Y-%m-%d-%H:%M:%S',key_cmd_stream=lambda x:'cmd:'+x)
        utility=mod('utility',local_ts=lambda:'2026-11-21-18:00:00')
        m4=mod('m4Commands',_resolve_operator_args=lambda s,a,v:v,_resolve_ap_sta_alias=lambda a,v:v)
        with patch.dict(sys.modules,{'config':config,'utility':utility,'m4Commands':m4}):
            ops=compile_operations(m,datetime(2026,11,21,18),dict(lab_id='DemoRoom',experiment_id='test',session_id='s',registration_uid='u'))
        self.assertEqual([op[2]['action'] for op in ops],['scan.start','scan.start','scan.stop'])
        self.assertEqual(ops[1][2]['execute_at'],'2026-11-21-18:10:00')
        self.assertNotIn('999',ops[1][2]['args_json'])

if __name__=='__main__':unittest.main(verbosity=2)
