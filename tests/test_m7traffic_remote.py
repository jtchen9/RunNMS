"""Redis/Lua integration tests; requires fakeredis[lua], no running NMS."""
import ast
from datetime import datetime
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

import fakeredis

ROOT = Path(__file__).resolve().parents[1]


def functions(filename, names, namespace):
    tree = ast.parse((ROOT / filename).read_text())
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    for node in nodes:
        if isinstance(node, ast.FunctionDef):
            node.decorator_list = []
    header = ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[header] + nodes, type_ignores=[])), filename, 'exec'), namespace)
    return namespace


class RemoteTrafficTests(unittest.TestCase):
    def setUp(self):
        self.config = types.ModuleType('config')
        c = self.config
        c.r = fakeredis.FakeRedis(decode_responses=True)
        c.KEY_PREFIX = 'test:'
        c.TRAFFIC_SERVER_HOST = 'example.test'
        c.TRAFFIC_SERVER_PORTS = [5201, 5202]
        c.TRAFFIC_REMOTE_SCANNERS = ['echo', 'alpha']
        c.KEY_TRAFFIC_CMD_STREAM = 'test:traffic:cmd'
        c.KEY_TRAFFIC_RESULT_STREAM = 'test:traffic:result'
        c.KEY_TRAFFIC_EVENT_STREAM = 'test:traffic:event'
        c.KEY_TRAFFIC_EVENT_TEMP_STREAM = 'test:traffic:debug'
        c.TRAFFIC_EVENT_TEMP_MAXLEN = 100
        c.KEY_REGISTRY = 'test:registry'
        c.CMD_EXPIRE_SEC = 3600
        c.TIME_FMT = '%Y-%m-%d-%H:%M:%S'
        c.key_cmd_stream = lambda scanner: 'test:cmd:' + scanner
        c.key_scanner_meta = lambda scanner: 'test:meta:' + scanner
        c.key_traffic_temp_running = lambda scanner, session: 'test:running:' + scanner + ':' + session
        self.utility = types.SimpleNamespace(local_ts=lambda: '2026-10-27-09:30:00',
                    parse_local_dt=lambda x: datetime.strptime(x, c.TIME_FMT),
                    _json_bytes=lambda x: len(json.dumps(x).encode()))
        spec = importlib.util.spec_from_file_location('m7TrafficRemote', ROOT / 'm7TrafficRemote.py')
        self.remote = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'config': c, 'utility': self.utility}):
            spec.loader.exec_module(self.remote)
    def dispatch(self, scanner='echo', session='S1', action='traffic.session.start'):
        args = dict(session_id=session, reverse=True, protocol='udp', ac='bk',
                    duration_sec=180, report_interval_sec=60, bitrate='5M', packet_size=1200)
        fields = dict(scanner=scanner, action=action, args_json=json.dumps(args),
                      execute_at='2026-10-27-09:30:00', lab_id='DemoRoom', experiment_id='Test', session_id='default')
        xid = self.config.r.xadd(self.config.KEY_TRAFFIC_CMD_STREAM, fields)
        self.remote.dispatch_due(xid, fields)
        return xid, fields, self.config.r.xrange(self.config.key_cmd_stream(scanner))[-1]
    def record(self, delivery, record_id='a'*32, kind='result'):
        fields = delivery[1]
        return dict(record_id=record_id, scanner='echo', session_id='S1',
                    execution_id=fields['traffic_execution_id'], kind=kind, status='ok',
                    completion_time='2026-10-27-09:33:00', action='traffic.session.start',
                    raw={'start': {'test_start': {'reverse': 0}}, 'end': {'sum_received': {'bytes': 37500000}}})
    def test_atomic_handoff_and_duplicate_worker(self):
        xid, fields, delivery = self.dispatch()
        self.remote.dispatch_due(xid, fields)
        self.assertEqual(self.config.r.xlen(self.config.key_cmd_stream('echo')), 1)
        self.assertEqual(self.config.r.xlen(self.config.KEY_TRAFFIC_CMD_STREAM), 0)
        self.assertEqual(delivery[1]['experiment_id'], 'Test')
    def test_source_cancelled_before_handoff(self):
        fields = dict(scanner='echo', action='traffic.session.start', args_json=json.dumps(dict(session_id='S1', reverse=True)), execute_at=self.utility.local_ts())
        self.remote.dispatch_due('1-0', fields)
        self.assertEqual(self.config.r.xlen(self.config.key_cmd_stream('echo')), 0)
    def test_distinct_initial_ports_for_concurrent_jobs(self):
        _, _, one = self.dispatch()
        _, _, two = self.dispatch(scanner='alpha')
        self.assertNotEqual(json.loads(one[1]['args_json'])['server_port'], json.loads(two[1]['args_json'])['server_port'])
    def test_replay_ack_only_one_result_and_cleanup(self):
        _, _, delivery = self.dispatch()
        record = self.record(delivery)
        first = self.remote.ingest_records('echo', [record])
        second = self.remote.ingest_records('echo', [record])
        self.assertEqual(first, (['a'*32], []))
        self.assertEqual(first, second)
        self.assertEqual(self.config.r.xlen(self.config.KEY_TRAFFIC_RESULT_STREAM), 1)
        self.assertIsNone(self.config.r.get(self.config.key_traffic_temp_running('echo', 'S1')))
    def test_storage_failure_is_not_acknowledged(self):
        _, _, delivery = self.dispatch()
        self.config.r.set(self.config.KEY_TRAFFIC_RESULT_STREAM, 'wrong type')
        self.assertEqual(self.remote.ingest_records('echo', [self.record(delivery)]), ([], []))
        self.config.r.delete(self.config.KEY_TRAFFIC_RESULT_STREAM)
        self.assertEqual(self.remote.ingest_records('echo', [self.record(delivery)])[0], ['a'*32])
    def test_unknown_manual_records_quarantined_not_uploaded(self):
        _, _, delivery = self.dispatch()
        record = self.record(delivery)
        record['execution_id'] = 'manual-old'
        accepted, rejected = self.remote.ingest_records('echo', [record])
        self.assertEqual(accepted, [])
        self.assertEqual(rejected[0]['record_id'], 'a'*32)
        self.assertEqual(self.config.r.xlen(self.config.KEY_TRAFFIC_RESULT_STREAM), 0)
    def test_event_replay_only_one_event_and_mirror(self):
        _, _, delivery = self.dispatch()
        record = self.record(delivery, kind='event')
        self.remote.ingest_records('echo', [record])
        self.remote.ingest_records('echo', [record])
        self.assertEqual(self.config.r.xlen(self.config.KEY_TRAFFIC_EVENT_STREAM), 1)
        self.assertEqual(self.config.r.xlen(self.config.KEY_TRAFFIC_EVENT_TEMP_STREAM), 1)
    def test_stop_targets_exact_execution(self):
        _, _, delivery = self.dispatch()
        _, _, stop = self.dispatch(action='traffic.session.stop')
        self.assertEqual(json.loads(stop[1]['args_json'])['execution_id'], delivery[1]['traffic_execution_id'])
    def test_old_result_does_not_clear_new_running_session(self):
        _, _, delivery = self.dispatch()
        self.config.r.set(self.config.key_traffic_temp_running('echo', 'S1'), json.dumps(dict(remote=True, execution_id='new')))
        self.remote.ingest_records('echo', [self.record(delivery)])
        self.assertEqual(json.loads(self.config.r.get(self.config.key_traffic_temp_running('echo', 'S1')))['execution_id'], 'new')
    def test_poll_delivers_command_and_ingests_record(self):
        _, _, delivery = self.dispatch()
        ns = dict(config=self.config, utility=self.utility, json=json,
                  m1Registry=types.SimpleNamespace(require_whitelisted=lambda x: None),
                  _finalize_due_experiments_from_poll=lambda x: {})
        functions('m4Commands.py', ['_collect_due_commands', 'cmd_poll'], ns)
        request = types.SimpleNamespace(client=None)
        req = types.SimpleNamespace(limit=20, av_streaming=None, av_detail=None, boot_id=None,
                                    status_report={}, mobility_report=None, traffic_records=[self.record(delivery)])
        with patch.dict(sys.modules, {'m7TrafficRemote': self.remote}):
            response = ns['cmd_poll']('echo', req, request)
        self.assertEqual(response['traffic_ack_ids'], ['a'*32])
        self.assertEqual(response['commands'][0][1]['traffic_execution_id'], delivery[1]['traffic_execution_id'])
    def test_northbound_preserves_roles_and_provenance(self):
        _, _, delivery = self.dispatch()
        self.remote.ingest_records('echo', [self.record(delivery)])
        ns = dict(config=self.config, utility=self.utility, json=json)
        functions('m5Northbound.py', ['_optional_int', '_optional_json_dict', '_attach_traffic_origin', '_collect_iperf3_sessions_for_upload'], ns)
        reports, ids, bad = ns['_collect_iperf3_sessions_for_upload'](100000)
        self.assertEqual(len(reports), 1)
        self.assertTrue(reports[0]['reverse'])
        self.assertFalse(reports[0]['iperf_reverse'])
        self.assertEqual(reports[0]['raw']['start']['test_start']['reverse'], 0)
        self.assertEqual(reports[0]['experiment_id'], 'Test')
    def test_opt_in_only_selected_robot(self):
        self.assertTrue(self.remote.remote_enabled('echo'))
        self.assertFalse(self.remote.remote_enabled('bravo'))
        self.config.TRAFFIC_SERVER_HOST = ''
        self.assertFalse(self.remote.remote_enabled('echo'))

if __name__ == '__main__':
    unittest.main()
