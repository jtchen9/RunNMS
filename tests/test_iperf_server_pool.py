import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
CFG = types.SimpleNamespace(TRAFFIC_LOCAL_SERVER_ENABLED=True, TRAFFIC_SERVER_PORTS=list(range(5200,5240)),
    TRAFFIC_LOCAL_IPERF3_BIN='D:/iperf3.exe',TRAFFIC_LOCAL_INTERFACE_ALIAS='Ethernet',
    TRAFFIC_LOCAL_BIND_IP='',TRAFFIC_LOCAL_EXCLUDED_SUBNET='192.168.11.0/24',TRAFFIC_LOCAL_CHECK_SECONDS=5)
def load(name, filename):
    spec=importlib.util.spec_from_file_location(name,ROOT/filename)
    mod=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules,{'config':CFG,'utility':types.SimpleNamespace(local_ts=lambda:'now')}):
        spec.loader.exec_module(mod)
    return mod
M=load('server_subject','m7TrafficServer.py')
ORIGINAL_LISTENER_OWNERS=M.listener_owners
class Proc:
    counter=100
    def __init__(self,*args,**kwargs):
        Proc.counter+=1;self.pid=Proc.counter;self.returncode=None;self.terminated=False;self.args=args[0]
    def poll(self):return self.returncode
    def terminate(self):self.terminated=True;self.returncode=0
    def kill(self):self.returncode=-9
    def wait(self,timeout=None):return self.returncode
class Job:
    def __init__(self):self.attached=[]
    def attach(self,p):self.attached.append(p)
class PoolTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.pool=M.Pool();self.pool.log_dir=Path(self.tmp.name);self.pool.job=Job()
        self.host='10.220.95.138';self.owners={}
        self.patches=[patch.object(M,'discover_address',return_value=self.host),
                      patch.object(M,'listener_owners',side_effect=lambda:self.owners),
                      patch.object(M.subprocess,'Popen',side_effect=Proc)]
        for p in self.patches:p.start()
    def tearDown(self):
        self.pool._stop_children()
        for p in reversed(self.patches):p.stop()
        self.tmp.cleanup()
    def ready(self):
        self.pool.tick();self.owners={(self.host,p):e[0].pid for p,e in self.pool.processes.items()};self.pool.tick()
    def test_40_owned_servers_and_verified_readiness(self):
        self.pool.tick();self.assertEqual(len(self.pool.processes),40);self.assertEqual(self.pool.ready,[])
        self.owners={(self.host,p):e[0].pid for p,e in self.pool.processes.items()};self.pool.tick()
        host,ports=self.pool.endpoint();self.assertEqual(host,self.host);self.assertEqual(ports,list(range(5200,5240)))
        self.assertEqual(len(self.pool.job.attached),40)
    def test_manual_server_not_adopted_or_killed(self):
        self.owners={(self.host,5201):999};self.pool.tick()
        self.assertNotIn(5201,self.pool.processes);self.assertIn('occupied',self.pool.errors['5201'])
    def test_unrelated_listener_cannot_mark_child_ready(self):
        self.pool.tick();self.owners={(self.host,5200):999};self.pool.tick()
        self.assertNotIn(5200,self.pool.ready)
    def test_exit_restarts_without_advertising_dead_process(self):
        self.ready();old=self.pool.processes[5200][0];old.returncode=1
        self.assertNotIn(5200,self.pool.endpoint()[1]);self.owners.pop((self.host,5200));self.pool.tick()
        self.assertNotEqual(old.pid,self.pool.processes[5200][0].pid);self.assertNotIn(5200,self.pool.ready)
    def test_dhcp_change_stops_owned_servers(self):
        self.ready();old=[p for p,f in self.pool.processes.values()]
        with patch.object(M,'discover_address',return_value='10.220.95.140'):self.pool.tick()
        self.assertTrue(all(p.terminated for p in old));self.assertEqual(self.pool.host,'10.220.95.140')
        self.assertEqual(self.pool.ready,[])
    def test_no_readiness_does_not_fall_back_to_public_server(self):
        with self.assertRaisesRegex(RuntimeError,'unavailable'):self.pool.endpoint()
    def test_process_attach_failure_cleans_child(self):
        made=[]
        def launch(*a,**kw):p=Proc(*a,**kw);made.append(p);return p
        with patch.object(M.subprocess,'Popen',side_effect=launch),patch.object(self.pool.job,'attach',side_effect=OSError('job denied')):
            self.pool.tick()
        self.assertEqual(self.pool.processes,{});self.assertTrue(all(p.returncode==-9 for p in made))
    def test_cleanup_only_owned_and_closes_logs(self):
        self.pool.tick();entries=list(self.pool.processes.values());self.pool._stop_children()
        self.assertTrue(all(p.terminated and f.closed for p,f in entries));self.assertEqual(self.pool.processes,{})
    def test_address_selection_rejects_mesh_linklocal_and_ambiguity(self):
        self.assertEqual(M.select_address(['192.168.11.51','169.254.2.1',self.host]),self.host)
        for addresses in [[],['192.168.11.51'],[self.host,'10.220.95.140']]:
            with self.assertRaises(RuntimeError):M.select_address(addresses)
        with self.assertRaises(RuntimeError):M.select_address([self.host],'10.220.95.140')
    def test_port_validation(self):
        self.assertEqual(len(M.validated_ports(list(range(5200,5240)))),40)
        for ports in [[],[1,1],[True],[0],[65536]]:
            with self.assertRaises(ValueError):M.validated_ports(ports)
    def test_listener_parser_without_active_probe(self):
        output=' TCP    10.220.95.138:5201  0.0.0.0:0 LISTENING 123\n TCP 10.220.95.138:5202 10.1.1.1:80 ESTABLISHED 124\n'
        with patch.object(M,'_run',return_value=output):
            self.assertEqual(ORIGINAL_LISTENER_OWNERS(),{('10.220.95.138',5201):123})

class DispatchTests(unittest.TestCase):
    def setUp(self):
        self.remote=load('remote_subject','m7TrafficRemote.py')
        self.oldvars=dict(vars(CFG))
        CFG.TRAFFIC_REMOTE_SCANNERS=['twin-scout-echo'];CFG.TRAFFIC_SERVER_HOST=''
        CFG.KEY_PREFIX='nms:';CFG.key_traffic_temp_running=lambda *x:'running'
        CFG.key_cmd_stream=lambda *x:'commands';CFG.KEY_TRAFFIC_CMD_STREAM='traffic'
        self.db=types.SimpleNamespace(incr=lambda key:2,eval=lambda *args:args)
        CFG.r=self.db
    def tearDown(self):CFG.__dict__.clear();CFG.__dict__.update(self.oldvars)
    def test_managed_mode_enabled_even_with_blank_host(self):
        self.assertTrue(self.remote.remote_enabled('twin-scout-echo'))
        self.assertFalse(self.remote.remote_enabled('other'))
    def test_dispatch_injects_discovered_host_and_rotates_ready_pool(self):
        calls=[];self.db.eval=lambda *a:calls.append(a)
        pool=types.SimpleNamespace(endpoint=lambda:('10.220.95.138',[5200,5202,5203]))
        with patch.dict(sys.modules,{'m7TrafficServer':types.SimpleNamespace(pool=pool)}):
            self.remote.dispatch_due('1-0',{'scanner':'twin-scout-echo','action':'traffic.session.start',
                'execute_at':'now','args_json':json.dumps({'session_id':'test'})})
        flat=calls[0][10:];delivery=dict(zip(flat[::2],flat[1::2]));args=json.loads(delivery['args_json'])
        self.assertEqual(args['server_host'],'10.220.95.138');self.assertEqual(args['server_ports'],[5202,5203,5200])
    def test_disabled_manager_keeps_external_mode(self):
        CFG.TRAFFIC_LOCAL_SERVER_ENABLED=False;CFG.TRAFFIC_SERVER_HOST='example.test'
        self.assertTrue(self.remote.remote_enabled('twin-scout-echo'))
        CFG.TRAFFIC_SERVER_HOST='';self.assertFalse(self.remote.remote_enabled('twin-scout-echo'))

if __name__=='__main__':unittest.main()
