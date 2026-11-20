"""Run from any directory: python testSM/testPreparation/step1_check.py.

Tests the installed parser and the actual new endpoint function (isolated from
unrelated NMS imports). No Redis, robot, registration or network activity.
"""
import ast
import asyncio
import csv
import io
import json
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from experiment_csv import CsvFormatError, parse_registration_csv

HEADER = ['scanner','t_offset_sec','category','action','args_json','initial_robot','initial_xy','']
A='twin-scout-alpha'
B='twin-scout-bravo'


def encoded(rows, encoding='utf-8-sig'):
    f=io.StringIO(newline=''); csv.writer(f).writerows(rows); return f.getvalue().encode(encoding)


def example():
    return [HEADER[:],
            [A,'0','mobility','mobility.report.location','{}',A,'(5, 3)','= $H$1+B2/86400'],
            [B,'0','mobility','mobility.report.location','{}',B,'(8.8, 3)','= $H$1+B3/86400'],
            [A,'60','mobility','mobility.move','{"x_m":5,"y_m":2.8}','','',''],
            [B,'900','mobility','mobility.report.location','{}',B,'(8.8, 2.8)','= $H$1+B5/86400'],
            [B,'960','mobility','mobility.move','{"x_m":8.8,"y_m":2.8}','','','']]


def legacy():
    return encoded([['scanner','intended_x_m','intended_y_m','intended_heading_deg','position_tolerance_m','heading_tolerance_deg'],
                    [A,5,3,270,'',''],[B,8.8,3,270,'','']])


class Format(unittest.TestCase):
    def bad(self, rows, code, poses=None):
        with self.assertRaises(CsvFormatError) as cm:parse_registration_csv(encoded(rows),poses)
        self.assertEqual(cm.exception.code,code)

    def test_two_robots_and_passive_reminder(self):
        x=parse_registration_csv(encoded(example()))
        self.assertEqual(x['robot_count'],2);self.assertEqual(x['command_count'],5)
        self.assertEqual(len(x['reentry_reminders']),1)
        self.assertEqual(x['reentry_reminders'][0]['execution_effect'],'none')
        self.assertIsNone(x['initial_positions'][0]['heading_deg'])
        self.assertFalse(x['execution_authorized'])

    def test_four_robot_initial_roster_independent_of_command_robot(self):
        rows=[HEADER[:]]
        names=[A,B,'twin-scout-charlie','twin-scout-delta']
        for i,n in enumerate(names):rows.append([names[::-1][i],0,'scan','scan.start','{}',n,f'({i+2}, 3)',''])
        x=parse_registration_csv(encoded(rows));self.assertEqual(x['robot_count'],4)
        self.assertEqual([p['scanner'] for p in x['initial_positions']],names)

    def test_commands_identical_to_legacy(self):
        rows=example();x=parse_registration_csv(encoded(rows))
        for r in rows[1:]:r[5:7]=['','']
        y=parse_registration_csv(encoded(rows),legacy())
        self.assertEqual(x['commands'],y['commands'])
        self.assertEqual(y['input_format'],'two_csv')
        self.assertEqual(y['initial_positions'][0]['heading_deg'],270)

    def test_preserves_original_rows_and_h(self):
        rows=example();rows[0][7]='2026-11-20 15:00';rows.insert(4,[])
        x=parse_registration_csv(encoded(rows))
        self.assertEqual(x['reentry_reminders'][0]['row_number'],6)
        self.assertEqual(x['start_time_cell'],'2026-11-20 15:00')
        self.assertEqual(x['commands'][0]['original_a_to_e'],rows[1][:5])
        self.assertEqual(x['commands'][0]['display_time_cell'],rows[1][7])

    def test_metadata_only_initial_rows(self):
        rows=[HEADER[:],['','','','','',A,'(5,3)',''],[A,0,'scan','scan.start','{}','','','']]
        self.assertEqual(parse_registration_csv(encoded(rows))['command_count'],1)

    def test_legacy_five_column_command_file(self):
        rows=[r[:5] for r in example()]
        self.assertEqual(parse_registration_csv(encoded(rows),legacy())['command_count'],5)

    def test_empty_file(self):self.bad([],'COMMAND_HEADER')
    def test_no_commands(self):self.bad([HEADER],'EMPTY_SCRIPT')
    def test_bad_header(self):
        r=example();r[0][0]='robot';self.bad(r,'COMMAND_HEADER')
    def test_unquoted_coordinates(self):
        r=example();r[1][6:7]=['(5','3)'];self.bad(r,'EXTRA_COLUMNS')
    def test_partial_pair(self):
        r=example();r[1][6]='';self.bad(r,'INCOMPLETE_POSITION_PAIR')
    def test_duplicate_initial_robot(self):
        r=example();r[2][5]=A;self.bad(r,'DUPLICATE_INITIAL_ROBOT')
    def test_missing_robot(self):
        r=example();r[2][5:7]=['',''];self.bad(r,'MISSING_INITIAL_ROBOTS')
    def test_unknown_reminder(self):
        r=example();r[4][5]='twin-scout-echo';self.bad(r,'UNKNOWN_REMINDER_ROBOT')
    def test_reminder_not_an_implicit_command(self):
        r=example();r[4][0]=A;x=parse_registration_csv(encoded(r))
        self.assertEqual(x['command_count'],5)
        self.assertEqual(x['warnings'][0]['code'],'REMINDER_REPORT_MISMATCH')
    def test_no_mixed_position_sources(self):self.bad(example(),'AMBIGUOUS_POSITIONS',legacy())
    def test_bad_coordinate(self):
        for value in ['(NaN, 3)','(1e999, 3)','(__import__("os"), 3)','[5, 3]']:
            with self.subTest(value=value):
                r=example();r[1][6]=value
                with self.assertRaises(CsvFormatError):parse_registration_csv(encoded(r))
    def test_negative_and_fractional_offset(self):
        for value in ['-1','1.5','NaN']:
            r=example();r[1][1]=value;self.bad(r,'BAD_OFFSET')
    def test_json_must_be_finite_object(self):
        for value in ['[]','null','{"x":NaN}','{"x":1e999}','{bad']:
            r=example();r[1][4]=value;self.bad(r,'BAD_ARGS_JSON')
    def test_reentry_without_command_rejected(self):
        r=example();r[4][:5]=['']*5;self.bad(r,'REMINDER_WITHOUT_COMMAND')
    def test_unused_roster_robot(self):
        r=example();r.insert(3,['','','','','','twin-scout-echo','(5,5)','']);self.bad(r,'UNUSED_INITIAL_ROBOTS')
    def test_ap_does_not_require_robot_pose(self):
        r=example();r.append(['AP1',0,'ap','ap.txpower.set','{}','','',''])
        self.assertEqual(parse_registration_csv(encoded(r))['robot_count'],2)
    def test_ap_only_legacy(self):
        r=[HEADER,['AP1',0,'ap','ap.txpower.set','{}','','','']]
        emptyposes=encoded([['scanner','intended_x_m','intended_y_m','intended_heading_deg']])
        self.assertEqual(parse_registration_csv(encoded(r),emptyposes)['robot_count'],0)
    def test_ap_only_single_csv_needs_no_robot_roster(self):
        rows=[HEADER,['AP1',0,'ap','ap.txpower.set','{}','','','']]
        self.assertEqual(parse_registration_csv(encoded(rows))['robot_count'],0)
    def test_cp950_and_bom(self):
        for enc in ['utf-8','utf-8-sig','cp950']:
            r=example();r[1][4]=json.dumps({'note':'測試'},ensure_ascii=False)
            self.assertEqual(parse_registration_csv(encoded(r,enc))['commands'][0]['args']['note'],'測試')
    def test_header_blank_fg_accepted(self):
        r=example();r[0][5:7]=['',''];self.assertEqual(parse_registration_csv(encoded(r))['robot_count'],2)


class Endpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from fastapi import APIRouter, HTTPException, File, UploadFile
        cls.UploadFile=UploadFile;cls.HTTPException=HTTPException
        tree=ast.parse((ROOT/'m4Commands.py').read_text(encoding='utf-8-sig'))
        node=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='cmd_preview_registration_csv')
        ns=dict(router=APIRouter(),HTTPException=HTTPException,File=File,UploadFile=UploadFile,Optional=Optional,Dict=Dict,Any=Any)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'m4Commands.py','exec'),ns)
        cls.fn=staticmethod(ns['cmd_preview_registration_csv'])
        cls.router=ns['router']

    def upload(self,raw,name='test.csv'):return self.UploadFile(file=io.BytesIO(raw),filename=name)
    def test_preview_cannot_register(self):
        out=asyncio.run(self.fn(self.upload(encoded(example())),None))
        self.assertTrue(out['preview_only']);self.assertFalse(out['registered']);self.assertEqual(out['commands_enqueued'],0)
    def test_legacy_preview(self):
        r=[r[:5] for r in example()]
        out=asyncio.run(self.fn(self.upload(encoded(r)),self.upload(legacy(),'poses.csv')))
        self.assertEqual(out['manifest']['input_format'],'two_csv')
    def test_http_upload_and_optional_legacy_file(self):
        from fastapi import FastAPI
        try:
            from fastapi.testclient import TestClient
        except ImportError:
            self.skipTest('Optional HTTP test requires the TestClient HTTP dependency; parser/direct endpoint tests still run.')
        app=FastAPI();app.include_router(self.router)
        with TestClient(app) as client:
            out=client.post('/cmd/_preview_registration_csv',files={'csv_file':('example.csv',encoded(example()),'text/csv')})
            self.assertEqual(out.status_code,200)
            self.assertEqual(out.json()['manifest']['command_count'],5)
            self.assertFalse(out.json()['registered'])
            rows=[r[:5] for r in example()]
            old=client.post('/cmd/_preview_registration_csv',files={'csv_file':('old.csv',encoded(rows),'text/csv'),'initial_poses_file':('poses.csv',legacy(),'text/csv')})
            self.assertEqual(old.status_code,200)
            self.assertEqual(old.json()['manifest']['commands'],parse_registration_csv(encoded(rows),legacy())['commands'])
            bad=client.post('/cmd/_preview_registration_csv',files={'csv_file':('bad.csv',b'wrong','text/csv')})
            self.assertEqual(bad.status_code,422)
    def test_bad_extension(self):
        with self.assertRaises(self.HTTPException) as cm:asyncio.run(self.fn(self.upload(b'','x.txt'),None))
        self.assertEqual(cm.exception.status_code,400)
    def test_bad_input_has_row_error(self):
        r=example();r[1][6]='bad'
        with self.assertRaises(self.HTTPException) as cm:asyncio.run(self.fn(self.upload(encoded(r)),None))
        self.assertEqual(cm.exception.status_code,422);self.assertEqual(cm.exception.detail['row_number'],2)


if __name__=='__main__':unittest.main(verbosity=2)
