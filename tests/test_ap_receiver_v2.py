import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from fastapi import FastAPI

class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.redis = MagicMock()
        config = types.SimpleNamespace(r=self.redis, TIME_FMT='%Y-%m-%d-%H:%M:%S', AP_UPLINK_MAXLEN=1000,
            KEY_REGISTRY='registry', key_scanner_meta=lambda s:'meta:'+s, key_ap_uplink_stream=lambda s:'uplink:'+s)
        utility = types.SimpleNamespace(parse_local_dt=lambda s:datetime.strptime(s,config.TIME_FMT),
            local_ts=lambda:'2026-11-13-10:00:00',normalize_mac=lambda s:s.lower())
        deps={'config':config,'utility':utility,'m1Registry':types.SimpleNamespace(require_whitelisted=lambda s:None),
            'm4Commands':types.SimpleNamespace(_collect_due_commands=lambda **kw:{'commands':[],'skipped':0})}
        path=Path(__file__).resolve().parents[1]/'m6AP.py'
        if not path.exists(): path=Path('m6AP.py')
        spec=importlib.util.spec_from_file_location('ap_receiver_test_subject',path)
        module=importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules,deps): spec.loader.exec_module(module)
        app=FastAPI();app.include_router(module.router);self.client=TestClient(app)
        self.times={'time_start':'2026-11-13-09:59:00','time_end':'2026-11-13-10:00:00'}
    def stored(self):return json.loads(self.redis.xadd.call_args.args[1]['payload_text'])
    def test_v2_preserves_nested_null_and_future_fields(self):
        body=dict(self.times,format_version=2,radios=[{'channel_time_ms':{'value':None,'type':'unavailable'}}],
            peers=[{'peer_mac':'aa','records':[{'direction':'ap_rx','category':'BACKHAUL','packets':{'value':2,'type':'measured'}},{'direction':'ap_rx','category_coverage':{'uncovered_packets':{'value':None}}}]}],future={'x':[None,False,0]})
        response=self.client.post('/ap/traffic/AP6',json=body)
        self.assertEqual(response.status_code,200,response.text);self.assertEqual(self.stored(),body)
        self.assertEqual(response.json()['record_count'],2)
    def test_legacy_preserved(self):
        body=dict(self.times,records=[{'sta_mac':'aa','ac':'BK','avg_frame_duration_us':48.8,'frame_count':3,'future':None}])
        self.assertEqual(self.client.post('/ap/traffic/AP1',json=body).status_code,200)
        self.assertEqual(self.stored(),body)
    def test_empty_legacy(self):
        self.assertEqual(self.client.post('/ap/traffic/AP1',json=self.times).status_code,200)
        self.assertEqual(self.stored()['records'],[])
    def test_invalid_reports_not_queued(self):
        for extra in [{'format_version':3},{'format_version':2,'radios':[]},{'records':[{'sta_mac':'aa'}]},
                      {'format_version':2,'radios':[],'peers':[{'records':None}]}]:
            self.assertEqual(self.client.post('/ap/traffic/AP1',json=dict(self.times,**extra)).status_code,422)
        self.redis.xadd.assert_not_called()
    def test_invalid_timestamp_not_queued(self):
        self.assertEqual(self.client.post('/ap/traffic/AP1',json=dict(self.times,time_end='bad')).status_code,400)
        self.redis.xadd.assert_not_called()
    def test_storage_failure_not_acknowledged(self):
        self.redis.xadd.side_effect=RuntimeError('offline')
        self.assertEqual(self.client.post('/ap/traffic/AP1',json=self.times).status_code,500)
    def test_status_new_fields_and_disconnect(self):
        macs={'backhaul_sta':{'6g':'aa'},'backhaul_bss':{'6g':'bb'}}
        body={'time':'2026-11-13-10:00:00','status':{'mac':'AA','backhaul_mac':'bb','wireless_macs':macs}}
        self.assertEqual(self.client.post('/ap/poll/AP6',json=body).status_code,200)
        meta=self.redis.hset.call_args.kwargs['mapping']
        self.assertEqual(meta['backhaul_mac'],'bb');self.assertEqual(json.loads(meta['wireless_macs_json']),macs)
        body['status']['backhaul_mac']=''
        self.client.post('/ap/poll/AP6',json=body)
        self.assertEqual(self.redis.hset.call_args.kwargs['mapping']['backhaul_mac'],'')
    def test_old_status_optional_fields_not_erased(self):
        self.client.post('/ap/poll/AP1',json={'time':'2026-11-13-10:00:00','status':{'mac':'AA'}})
        meta=self.redis.hset.call_args.kwargs['mapping']
        self.assertNotIn('backhaul_mac',meta);self.assertNotIn('wireless_macs_json',meta)

if __name__=='__main__':unittest.main()
