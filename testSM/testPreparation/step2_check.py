"""Offline checks: python testSM/testPreparation/step2_check.py.
No running Redis, NMS, robot or DataLake connection is used.
"""
import asyncio
import csv
import importlib.util
import io
import json
import sys
import types
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from experiment_csv import parse_registration_csv
from nms_experiment_preparation import Preparation, PreparationError, ceil_minute, TIME_FMT

A, B = 'twin-scout-alpha', 'twin-scout-bravo'
START = datetime(2026, 11, 20, 18, 0, 0)


def manifest():
    f = io.StringIO()
    csv.writer(f).writerows([
        ['scanner','t_offset_sec','category','action','args_json','initial_robot','initial_xy',''],
        [A,0,'scan','scan.start','{}',A,'(5,3)','=$H$1+B2/86400'],
        [B,0,'scan','scan.start','{}',B,'(8.8,3)','=$H$1+B3/86400'],
        [A,60,'mobility','mobility.move','{"x_m":5,"y_m":2.5}','','',''],
        [B,900,'mobility','mobility.report.location','{}',B,'(8.8,3)',''],
        [A,960,'system','system.shutdown','{}','','',''],
        [B,960,'system','system.shutdown','{}','','',''],
    ])
    return parse_registration_csv(f.getvalue().encode())


def new(man=None):
    return Preparation(lab_id='DemoRoom', experiment_id='PreparationTest',
                       session_id='test-session', manifest=man or manifest(), now=START)


def begin(p, at=50):
    p.health_passed(token=p.token, now=START+timedelta(seconds=at))
    return p.begin_attempt(now=START+timedelta(seconds=at), all_robots_idle=True, stop_latched=False)


def ready(p, at=160):
    token = begin(p)
    for robot in (A, B):
        p.report_ready(robot, token=token, now=START+timedelta(seconds=at))
    return token


class Lifecycle(unittest.TestCase):
    def test_health_50_preparation_110_rounds_to_three_minutes(self):
        p = new(); ready(p)
        out = p.schedule_candidate(now=START+timedelta(seconds=160))
        self.assertEqual(out['start_at'], '2026-11-20-18:03:00')
        self.assertEqual(out['end_at'], '2026-11-20-18:19:00')
        self.assertEqual(out['commands_enqueued'], 0)
        print('\nTiming example: health 50s + preparation 110s -> t0 18:03:00; end 18:19:00.')

    def test_script_held_during_health_and_partial_readiness(self):
        p = new()
        with self.assertRaises(PreparationError): p.schedule_candidate(now=START)
        token = begin(p)
        p.report_ready(A, token=token, now=START+timedelta(seconds=60))
        with self.assertRaises(PreparationError): p.schedule_candidate(now=START+timedelta(seconds=60))
        self.assertEqual(p.snapshot(now=START)['commands_held'], 6)

    def test_cannot_prepare_before_health_pass(self):
        with self.assertRaises(PreparationError):
            new().begin_attempt(now=START, all_robots_idle=True, stop_latched=False)

    def test_retry_waits_full_60_seconds(self):
        p = new(); token = begin(p)
        p.attempt_failed(token=token, now=START+timedelta(seconds=100), reason='no pose', scanner=B)
        self.assertEqual(p.snapshot(now=START+timedelta(seconds=101))['retry_in_sec'], 59)
        with self.assertRaises(PreparationError):
            p.begin_attempt(now=START+timedelta(seconds=159), all_robots_idle=True, stop_latched=False)
        p.begin_attempt(now=START+timedelta(seconds=160), all_robots_idle=True, stop_latched=False)
        self.assertEqual(p.attempt, 2)

    def test_retry_reconfirms_already_ready_robot(self):
        p = new(); token = begin(p)
        p.report_ready(A, token=token, now=START+timedelta(seconds=80))
        p.attempt_failed(token=token, now=START+timedelta(seconds=100), reason='no pose', scanner=B)
        token = p.begin_attempt(now=START+timedelta(seconds=160), all_robots_idle=True, stop_latched=False)
        p.report_ready(B, token=token, now=START+timedelta(seconds=300))
        self.assertEqual(p.snapshot(now=START)['pending_robots'], [A])
        with self.assertRaises(PreparationError): p.schedule_candidate(now=START+timedelta(seconds=300))

    def test_multiple_retry_durations_counted_once(self):
        p = new(); token = begin(p)
        p.attempt_failed(token=token, now=START+timedelta(seconds=100), reason='pose')
        token = p.begin_attempt(now=START+timedelta(seconds=160), all_robots_idle=True, stop_latched=False)
        p.attempt_failed(token=token, now=START+timedelta(seconds=310), reason='pose')
        token = p.begin_attempt(now=START+timedelta(seconds=490), all_robots_idle=True, stop_latched=False)
        for robot in (A,B): p.report_ready(robot, token=token, now=START+timedelta(seconds=520))
        self.assertEqual(p.schedule_candidate(now=START+timedelta(seconds=520))['start_at'], '2026-11-20-18:09:00')

    def test_late_result_from_old_attempt_ignored(self):
        p = new(); old = begin(p)
        p.attempt_failed(token=old, now=START+timedelta(seconds=100), reason='timeout')
        p.begin_attempt(now=START+timedelta(seconds=160), all_robots_idle=True, stop_latched=False)
        self.assertFalse(p.report_ready(A, token=old, now=START+timedelta(seconds=290)))
        self.assertFalse(p.attempt_failed(token=old, now=START+timedelta(seconds=290), reason='late'))

    def test_reused_experiment_name_rejects_old_registration_token(self):
        old = new(); token = begin(old)
        p = new(); begin(p)
        self.assertNotEqual(old.identity, p.identity)
        self.assertFalse(p.report_ready(A, token=token, now=START+timedelta(seconds=80)))

    def test_duplicate_report_cannot_complete_other_robot(self):
        p = new(); token = begin(p)
        for _ in range(3): p.report_ready(A, token=token, now=START+timedelta(seconds=80))
        self.assertEqual(p.phase, 'preparing')

    def test_no_overlapping_attempt_even_after_timeout(self):
        p = new(); token = begin(p)
        p.attempt_failed(token=token, now=START+timedelta(seconds=100), reason='timeout')
        with self.assertRaises(PreparationError):
            p.begin_attempt(now=START+timedelta(seconds=600), all_robots_idle=False, stop_latched=False)
        self.assertEqual(p.attempt, 1)

    def test_latched_stop_not_cleared_by_retry(self):
        p = new(); p.health_passed(token=p.token, now=START)
        with self.assertRaises(PreparationError):
            p.begin_attempt(now=START, all_robots_idle=True, stop_latched=True)
        self.assertEqual(p.attempt, 0)

    def test_delete_blocks_late_report_and_schedule(self):
        p = new(); token = begin(p); p.cancel(now=START+timedelta(seconds=80))
        self.assertFalse(p.report_ready(A, token=token, now=START+timedelta(seconds=81)))
        with self.assertRaises(PreparationError): p.schedule_candidate(now=START+timedelta(seconds=90))
        with self.assertRaises(PreparationError):
            p.begin_attempt(now=START+timedelta(seconds=400), all_robots_idle=True, stop_latched=False)

    def test_stop_is_terminal(self):
        p = new(); ready(p); p.stop(now=START+timedelta(seconds=161), reason='operator stop')
        with self.assertRaises(PreparationError): p.schedule_candidate(now=START+timedelta(seconds=162))

    def test_late_handoff_does_not_use_past_t0(self):
        p = new(); ready(p)
        self.assertEqual(p.schedule_candidate(now=START+timedelta(seconds=241))['start_at'], '2026-11-20-18:05:00')

    def test_rounding_at_exact_minute_and_midnight(self):
        self.assertEqual(ceil_minute(START), START)
        self.assertEqual(ceil_minute(START+timedelta(microseconds=1)), START+timedelta(minutes=1))
        self.assertEqual(ceil_minute(datetime(2026,11,20,23,59,59)), datetime(2026,11,21))

    def test_reminders_do_not_create_commands_or_shift_offsets(self):
        man = manifest(); p = new(man); ready(p)
        out = p.schedule_candidate(now=START+timedelta(seconds=160))
        self.assertEqual(len(out['commands']), 6)
        self.assertEqual(out['commands'][3]['execute_at'], '2026-11-20-18:18:00')
        self.assertEqual([c['t_offset_sec'] for c in out['commands']], [0,0,60,900,960,960])
        self.assertNotIn('execute_at', man['commands'][0])
        self.assertEqual(out['commands'][0]['args'], {})

    def test_candidate_mutation_does_not_change_held_script(self):
        p = new(); ready(p)
        out = p.schedule_candidate(now=START+timedelta(seconds=160)); out['commands'][0]['args']['bad'] = True
        self.assertEqual(p.schedule_candidate(now=START+timedelta(seconds=160))['commands'][0]['args'], {})

    def test_ap_only_schedule_needs_no_robot_positions(self):
        man = manifest(); man['initial_positions'] = []; man['commands'] = [dict(scanner='AP1', t_offset_sec=0)]
        p = new(man); begin(p)
        self.assertEqual(p.phase, 'ready')
        self.assertEqual(p.schedule_candidate(now=START+timedelta(seconds=50))['start_at'], '2026-11-20-18:01:00')

    def test_status_has_pending_start_and_failure_details(self):
        p = new(); token = begin(p)
        p.attempt_failed(token=token, now=START+timedelta(seconds=100), reason='camera missing', scanner=B)
        s = p.snapshot(now=START+timedelta(seconds=130))
        self.assertEqual((s['failed_robot'], s['reason'], s['retry_in_sec']), (B,'camera missing',30))
        self.assertIsNone(s['start_at']); self.assertIsNone(s['end_at'])
        json.dumps(s)  # Ready for a future northbound adapter, not sent by this test.

    def test_unknown_robot_rejected(self):
        p = new(); token = begin(p)
        with self.assertRaises(PreparationError): p.report_ready('stranger', token=token, now=START)


class FakeRedis:
    def __init__(self, clock):
        self.clock = clock; self.sent = []; self.deleted = []; self.reply = 'pass'; self.offline = set()
    def ping(self): return True
    def hgetall(self, key):
        return {'last_seen': (START-timedelta(minutes=5) if key in self.offline else START).strftime(TIME_FMT),
                'device_type': 'ap' if key == 'AP1' else 'robot'}
    def xrevrange(self, key, count): return []
    def xadd(self, key, fields):
        xid = f'{len(self.sent)+1}-0'; self.sent.append((key, xid, fields)); return xid
    def xrange(self, key, min, count):
        if self.reply == 'none': return []
        for robot, xid, fields in self.sent:
            if key == robot + ':ack':
                args = json.loads(fields['args_json'])
                detail = {'schema':'static_health_v1','request_id':args['request_id'],'status':'PASS'}
                if self.reply == 'wrong_nonce': detail['request_id'] = 'unrelated'
                if self.reply == 'fail': detail.update(status='FAIL', reason='gyro')
                return [('99-0', {'cmd_id':xid, 'status':'ok', 'detail':json.dumps(detail)})]
        return []
    def xdel(self, key, xid): self.deleted.append((key, xid))


class Readiness(unittest.TestCase):
    def setUp(self):
        self.clock = types.SimpleNamespace(seconds=0.0)
        self.redis = FakeRedis(self.clock)
        conf = types.ModuleType('config'); conf.r = self.redis
        conf.key_scanner_meta = lambda n:n
        conf.key_cmd_stream = lambda n:n
        conf.key_cmdack_stream = lambda n:n+':ack'
        util = types.ModuleType('utility')
        util.local_ts = lambda:(START+timedelta(seconds=self.clock.seconds)).strftime(TIME_FMT)
        util.parse_local_dt = lambda s:datetime.strptime(s,TIME_FMT)
        spec = importlib.util.spec_from_file_location('readiness_under_test', ROOT/'nms_static_readiness.py')
        self.mod = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'config':conf,'utility':util}): spec.loader.exec_module(self.mod)
        self.mod.time = types.SimpleNamespace(monotonic=lambda:self.clock.seconds, time=lambda:1000+self.clock.seconds)
        async def sleep(seconds): self.clock.seconds += seconds
        self.mod.asyncio = types.SimpleNamespace(sleep=sleep)

    def run_check(self, names=(A,B), **kw): return asyncio.run(self.mod.check_targets(names, **kw))

    def test_no_t0_all_robots_health_checked(self):
        result = self.run_check()
        self.assertEqual(result, {A:'static PASS',B:'static PASS'})
        self.assertEqual(len(self.redis.sent),2)
        for _,_,f in self.redis.sent:
            self.assertEqual(f['action'],'health.check.static')
            self.assertEqual(json.loads(f['args_json'])['expires_ts'],1150)

    def test_new_health_timeout_is_150_seconds_and_cleans_only_owned_commands(self):
        self.redis.reply = 'none'
        with self.assertRaises(self.mod.HTTPException) as cm: self.run_check()
        self.assertEqual(self.clock.seconds,150)
        self.assertEqual(self.redis.deleted,[(A,'1-0'),(B,'2-0')])
        self.assertNotIn('choose a later t0',cm.exception.detail)

    def test_legacy_t0_preserves_short_budget(self):
        self.redis.reply = 'none'
        with self.assertRaises(self.mod.HTTPException): self.run_check(t0=START+timedelta(seconds=20))
        self.assertEqual(self.clock.seconds,15)

    def test_legacy_t0_too_close_rejected_before_any_command(self):
        with self.assertRaises(self.mod.HTTPException): self.run_check(t0=START+timedelta(seconds=5))
        self.assertEqual(self.redis.sent,[])

    def test_invalid_nonce_cannot_pass(self):
        self.redis.reply = 'wrong_nonce'
        with self.assertRaises(self.mod.HTTPException): self.run_check()
        self.assertEqual(len(self.redis.deleted),2)

    def test_failed_static_check_rejects_and_cleans_pending(self):
        self.redis.reply = 'fail'
        with self.assertRaises(self.mod.HTTPException) as cm: self.run_check()
        self.assertIn('gyro',cm.exception.detail)
        self.assertEqual(len(self.redis.deleted),2)

    def test_offline_robot_rejected_before_health_commands(self):
        self.redis.offline.add(B)
        with self.assertRaises(self.mod.HTTPException): self.run_check()
        self.assertEqual(self.redis.sent,[])

    def test_ap_is_checked_without_robot_health_command(self):
        self.assertEqual(self.run_check(('AP1',)),{'AP1':'connected'})
        self.assertEqual(self.redis.sent,[])


if __name__ == '__main__':
    unittest.main(verbosity=2)
