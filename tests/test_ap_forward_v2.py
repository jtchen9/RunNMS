import importlib.util,json,sys,types,unittest
from pathlib import Path
from unittest.mock import MagicMock,patch

class Redis:
    def __init__(self):self.values={};self.streams={};self.meta={};self.deleted=[]
    def smembers(self,k):return {'AP1','AP6'}
    def get(self,k):return self.values.get(k)
    def set(self,k,v):self.values[k]=v
    def expire(self,*a):pass
    def hgetall(self,k):return self.meta.get(k,{})
    def xrange(self,k,count=200):return self.streams.get(k,[])[:count]
    def xdel(self,k,*ids):
        self.deleted.extend((k,i) for i in ids);self.streams[k]=[(i,f) for i,f in self.streams.get(k,[]) if i not in ids];return len(ids)

class ForwardTests(unittest.TestCase):
    def setUp(self):
        self.r=Redis();self.c=types.SimpleNamespace(r=self.r,KEY_REGISTRY='registry',KEY_PREFIX='nms:',
            TIME_FMT='%Y-%m-%d-%H:%M:%S',AP_UPLOAD_MAX_BYTES=200000,AP_UPLOAD_MAX_POSTS_PER_CYCLE=12,
            UPLOAD_BATCH_MAX_BYTES=100000,WEB_API_KEY='',NMS_ID='Office',WEB_NMS_UPLOAD_URL='https://test',
            KEY_NB_LAST_UPLOAD_PAYLOAD='debug',NB_DEBUG_TTL_SEC=60,WEB_VERIFY_TLS=True)
        self.c.key_ap_uplink_stream=lambda s:'ap:'+s;self.c.key_scanner_meta=lambda s:'meta:'+s
        deps={'requests':types.SimpleNamespace(post=MagicMock()),'urllib3':types.SimpleNamespace(), 'config':self.c,'utility':types.SimpleNamespace(local_ts=lambda:'2026-11-13-10:00:00'),
            'm8mobility_state_store':types.SimpleNamespace(key_pose=lambda s:s,key_state=lambda s:s,KEY_STOP='stop')}
        p=Path(__file__).resolve().parents[1]/'m5Northbound.py'
        spec=importlib.util.spec_from_file_location('forward_subject',p);self.m=importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules,deps):spec.loader.exec_module(self.m)
        self.m._build_experiment_snapshot=lambda:{'experiment_id':'Test'};self.m._northbound_lab_id=lambda:'Office'
        self.response=MagicMock();self.response.json.return_value={'status':'ok','accepted':0,'rejected':0}
        self.post=patch.object(self.m.requests,'post',return_value=self.response).start();self.addCleanup(patch.stopall)
    def put(self,ap,xid,raw):self.r.streams.setdefault('ap:'+ap,[]).append((xid,{'payload_text':json.dumps(raw)}))
    def bodies(self):return [json.loads(c.kwargs['data']) for c in self.post.call_args_list]
    def test_large_nested_report_is_lossless(self):
        raw={'format_version':2,'radios':[{'unknown':None}],'peers':[],'padding':'漢'*45000}
        self.put('AP6','1',raw);res=self.m._upload_ap_reports_once()
        self.assertEqual(res['sent_ap_traffic_reports'],1);self.assertEqual(self.bodies()[0]['ap_traffic_reports'][0]['raw'],raw)
        self.assertLessEqual(len(self.post.call_args.kwargs['data']),200000)
    def test_separate_posts_exact_size_and_identity(self):
        for ap in ['AP1','AP6']:self.put(ap,'1',{'old_or_new':'x'*110000})
        self.r.meta['meta:AP6']={'ap_alias':'Agent6'}
        res=self.m._upload_ap_reports_once();self.assertEqual(res['sent_ap_traffic_reports'],2)
        self.assertEqual(len(self.bodies()),2);self.assertEqual(self.bodies()[1]['ap_traffic_reports'][0]['ap_id'],'Agent6')
    def test_network_failure_retains_report(self):
        self.put('AP1','1',{'a':None});self.post.side_effect=TimeoutError('timeout')
        res=self.m._upload_ap_reports_once();self.assertTrue(res['ap_upload_errors']);self.assertEqual(self.r.deleted,[])
    def test_rejection_retains_report(self):
        self.put('AP1','1',{});self.response.json.return_value={'status':'ok','rejected':1}
        self.m._upload_ap_reports_once();self.assertEqual(self.r.deleted,[])
    def test_oversized_does_not_block_following_report_or_ap(self):
        self.put('AP1','1',{'p':'x'*300000});self.put('AP1','2',{'ok':1});self.put('AP6','1',{'ok':2})
        res=self.m._upload_ap_reports_once();self.assertEqual(res['sent_ap_traffic_reports'],2)
        self.assertEqual(len(res['ap_upload_oversized']),1);self.assertEqual(self.r.streams['ap:AP1'][0][0],'1')
    def test_envelope_over_limit_is_not_sent_or_deleted(self):
        self.c.AP_UPLOAD_MAX_BYTES=100;self.put('AP1','1',{})
        self.m._upload_ap_reports_once();self.post.assert_not_called();self.assertEqual(self.r.deleted,[])
    def test_round_robin_bounded_posts(self):
        self.c.AP_UPLOAD_MAX_POSTS_PER_CYCLE=1
        self.put('AP1','1',{});self.put('AP1','2',{});self.put('AP6','1',{})
        self.m._upload_ap_reports_once();self.m._upload_ap_reports_once()
        self.assertEqual([b['ap_traffic_reports'][0]['ap_id'] for b in self.bodies()],['AP1','AP6'])
    def test_malformed_preserved_and_following_sent(self):
        self.r.streams['ap:AP1']=[('bad',{'payload_text':'oops'})];self.put('AP1','good',{})
        res=self.m._upload_ap_reports_once();self.assertEqual(res['sent_ap_traffic_reports'],1)
        self.assertTrue(res['ap_upload_errors']);self.assertEqual(self.r.streams['ap:AP1'][0][0],'bad')
    def test_scan_failure_does_not_prevent_ap_upload(self):
        self.m._northbound_upload_scans_once=lambda:{'status':'fail'};self.put('AP6','1',{})
        res=self.m._northbound_upload_once();self.assertEqual(res['sent_ap_traffic_reports'],1)
    def test_retry_success_deletes_only_once(self):
        self.put('AP6','1',{});self.post.side_effect=TimeoutError();self.m._upload_ap_reports_once()
        self.post.side_effect=None;self.m._upload_ap_reports_once();self.m._upload_ap_reports_once()
        self.assertEqual(self.r.deleted,[('ap:AP6','1')])

    def test_status_backhaul_fields_forwarded(self):
        self.c.AP_STALE_TIMEOUT_SEC=120
        for ap in ['AP1','AP6']:
            self.r.meta['meta:'+ap]={'device_type':'ap','backhaul_mac':'aa' if ap=='AP6' else '',
                'wireless_macs_json':json.dumps({'backhaul_sta':{'6g':'bb'}})}
        self.m._freshness_state=lambda *a:'active'
        self.m._load_ap_location_from_meta=lambda *a:{}
        self.m._build_experiment_snapshot=lambda *a:{}
        self.m._reported_experiment_stop_state=lambda:{}
        snap=self.m._build_status_snapshot([])
        self.assertEqual(snap['aps'][1]['backhaul_mac'],'aa')
        self.assertEqual(snap['aps'][1]['wireless_macs']['backhaul_sta']['6g'],'bb')
    def test_partial_upload_success_only_removes_successful(self):
        self.put('AP1','1',{'a':1});self.put('AP6','1',{'a':2})
        self.post.side_effect=[self.response,TimeoutError()]
        result=self.m._upload_ap_reports_once()
        self.assertEqual(result['sent_ap_traffic_reports'],1)
        self.assertEqual(self.r.deleted,[('ap:AP1','1')])
    def test_radio_and_peer_values_not_interpreted(self):
        raw={'format_version':2,'radios':[], 'peers':[{'records':[{'category':'ALL','packets':{'value':10}},
            {'category':'BK','packets':{'value':10}}, {'category_coverage':{'uncovered_packets':{'value':None}}}]}]}
        self.put('AP6','1',raw);self.m._upload_ap_reports_once()
        self.assertEqual(self.bodies()[0]['ap_traffic_reports'][0]['raw'],raw)

if __name__=='__main__':unittest.main()
