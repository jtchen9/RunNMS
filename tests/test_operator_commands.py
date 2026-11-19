"""Run from NMS root: python -m unittest discover -s tests -p test_operator_commands.py -v"""
import ast
import copy
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
import unittest
from fastapi import HTTPException
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[1]
COMMON = ROOT / 'sitemap' / 'CommonCheckers'
sys.path.insert(0, str(COMMON))
from checker.operator_args import normalize_operator_args
from checker.argument_rules import check_command_arguments
from checker.script_model import ScriptRow

POLICY = json.loads((COMMON / 'config/script_policy.json').read_text(encoding='utf-8-sig'))

class Redis:
    def __init__(self): self.writes = []
    def hexists(self, *args): return True
    def xadd(self, key, fields, **kwargs):
        self.writes.append((key, copy.deepcopy(fields))); return '1-0'

class OperatorTests(unittest.TestCase):
    def setUp(self):
        self.redis = Redis()
        # Execute the production queue functions without starting unrelated NMS services.
        names = {'Cmd', '_resolve_ap_sta_alias', '_resolve_operator_args', '_cmd_enqueue_core',
                 '_analyze_csv_rows_for_experiment', '_enqueue_script_or_csv_item'}
        tree = ast.parse((ROOT / 'm4Commands.py').read_text(encoding='utf-8-sig'))
        nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names]
        import re
        self.env = dict(Any=Any, Dict=Dict, List=List, Optional=Optional, BaseModel=BaseModel, Field=Field,
            HTTPException=HTTPException, Path=Path, json=json, sys=sys, re=re, timedelta=timedelta,
            __file__=str(ROOT / 'm4Commands.py'),
            config=SimpleNamespace(r=self.redis, KEY_WHITELIST_SCANNER_META='white', TIME_FMT='%Y-%m-%d-%H:%M:%S', key_cmd_stream=lambda x:'cmd:'+x),
            m1Registry=SimpleNamespace(require_whitelisted=lambda x:None),
            utility=SimpleNamespace(local_ts=lambda:'2026-11-19-12:00:00'),
            _session_id_to_redis=lambda x:x or '')
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'm4Commands.py', 'exec'), self.env)

    def manual(self, action, args, scanner='AP5', text=False):
        cmd = self.env['Cmd'](category='system' if action=='system.shutdown' else 'ap', action=action,
            **({'args_json_text':json.dumps(args)} if text else {'args':args}))
        return self.env['_cmd_enqueue_core'](scanner, cmd)

    def test_all_18_mappings_and_case(self):
        for ap, bands in POLICY['backhaul_mac_by_ap_band'].items():
            for band, mac in bands.items():
                scanner='AP2' if ap=='AP1' else 'AP1'
                args={'target_ap':ap.lower(), 'band':' '+band.lower()+' '}
                self.assertEqual(normalize_operator_args(scanner,'ap.backhaul.steer',args,POLICY),{'backhaul_mac':mac})
                self.assertNotIn('backhaul_mac',args)

    def test_manual_dict_and_json_text(self):
        for text in (False, True):
            self.manual('ap.backhaul.steer',{'target_ap':'AP1','band':'6G'},text=text)
            self.assertEqual(json.loads(self.redis.writes[-1][1]['args_json']),{'backhaul_mac':'ac:91:9b:ff:f8:c0'})

    def test_direct_mac_compatibility(self):
        self.manual('ap.backhaul.steer',{'backhaul_mac':'AC:91:9B:FF:F8:C0'})
        self.assertEqual(json.loads(self.redis.writes[-1][1]['args_json'])['backhaul_mac'],'ac:91:9b:ff:f8:c0')

    def test_rejected_before_queue_write(self):
        cases=[{}, {'target_ap':'AP1'}, {'target_ap':'AP9','band':'6G'}, {'target_ap':'AP1','band':'7G'},
               {'target_ap':'AP5','band':'6G'}, {'backhaul_mac':'b8:82:f2:45:f3:a8'},
               {'target_ap':'AP1','band':'6G','backhaul_mac':'ac:91:9b:ff:f8:c0'},
               {'backhaul_mac':'bad'}, {'target_ap':1,'band':'6G'}, {'target_ap':'AP1','band':'6G','typo':1}]
        for args in cases:
            with self.subTest(args=args):
                with self.assertRaises(HTTPException) as ctx:self.manual('ap.backhaul.steer',args)
                self.assertEqual(ctx.exception.status_code,400)
        self.assertEqual(self.redis.writes,[])

    def test_csv_analyze_and_delivery_metadata(self):
        row={'scanner':'AP5','t_offset_sec':'20','category':'ap','action':'ap.backhaul.steer','args_json':'{"target_ap":"AP6","band":"6G"}'}
        analysis=self.env['_analyze_csv_rows_for_experiment']([row],t0_dt=datetime(2026,11,19,12))
        self.assertEqual(self.redis.writes,[])
        item=analysis['accepted_items'][0]
        self.env['_enqueue_script_or_csv_item'](scanner=item['scanner'],category=item['category'],action=item['action'],execute_at=item['execute_at'],args=item['args'],lab_id='DemoRoom',experiment_id='Test',session_id=None)
        fields=self.redis.writes[-1][1];args=json.loads(fields['args_json'])
        self.assertEqual(args['backhaul_mac'],'b8:82:f2:45:f5:b8')
        self.assertEqual(args['_experiment']['experiment_id'],'Test')
        self.assertNotIn('target_ap',args)

    def test_csv_bad_mapping_no_mutation(self):
        rows=[dict(scanner='AP5', t_offset_sec='0', category='ap',action='ap.traffic.enable',args_json='{}'),
              dict(scanner='AP5',t_offset_sec='20',category='ap',action='ap.backhaul.steer',args_json='{"target_ap":"AP9","band":"6G"}')]
        with self.assertRaises(HTTPException):self.env['_analyze_csv_rows_for_experiment'](rows,t0_dt=datetime(2026,11,19,12))
        self.assertEqual(self.redis.writes,[])

    def test_shutdown_default_and_explicit(self):
        for args, expected in [({},300),({'time_to_wait':600},600)]:
            self.manual('system.shutdown',args,scanner='twin-scout-echo')
            self.assertEqual(json.loads(self.redis.writes[-1][1]['args_json'])['time_to_wait'],expected)

    def test_shutdown_bad_values(self):
        for wait in (True,False,0,-1,1.5,'600',None):
            with self.subTest(wait=wait):
                with self.assertRaises(HTTPException):self.manual('system.shutdown',{'time_to_wait':wait},scanner='twin-scout-echo')
        self.assertEqual(self.redis.writes,[])

    def test_shared_checker_contract(self):
        for category, action, args, valid in [
            ('ap','ap.backhaul.steer',{'target_ap':'ap1','band':'6g'},True),
            ('ap','ap.backhaul.steer',{'backhaul_mac':'ac:91:9b:ff:f8:c0'},True),
            ('ap','ap.backhaul.steer',{'target_ap':'AP5','band':'6G'},False),
            ('ap','ap.backhaul.steer',{'target_ap':'AP1'},False),
            ('system','system.shutdown',{},True),
            ('system','system.shutdown',{'time_to_wait':600},True),
            ('system','system.shutdown',{'time_to_wait':True},False)]:
            row=ScriptRow(2,'AP5' if category=='ap' else 'twin-scout-echo',0,category,action,args)
            self.assertEqual(not check_command_arguments([row],POLICY),valid)

if __name__=='__main__': unittest.main()
