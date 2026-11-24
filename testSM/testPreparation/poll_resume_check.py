"""Offline checks; no Redis, network, robot commands or real waiting.
Run: python testSM/testPreparation/poll_resume_check.py
"""
import asyncio
from pathlib import Path
import sys
import unittest
from types import SimpleNamespace
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from step3_check import Port, A
from nms_preparation_positioning import position_once, NmsPositioningPort, PositioningError

class PollPort(Port):
    def __init__(self, ready_at=0):
        super().__init__()
        self.ready_at=ready_at
        self.open_calls=0
        self.first_issue_at=None
        self.cancelled=False
        self.stop_at=None
        self.cancel_at=None
        self.registration_blocked=False
        self.admit=NmsPositioningPort.__new__(NmsPositioningPort)
        self.admit.store=SimpleNamespace(_load_stop=lambda:{'stop':self.global_stop})
        self.admit.state=SimpleNamespace(_get_state=lambda scanner:self.state)
        self.admit.mobility=SimpleNamespace(
            registration_blocks_commands=lambda scanner:self.registration_blocked,
            link=SimpleNamespace(poll_fresh=lambda scanner:self.seconds>=self.ready_at))
    def open(self, scanner, *, scope, owns_attempt):
        self.open_calls+=1
        self.admit._admit(scanner,owns_attempt)
        super().open(scanner,scope=scope,owns_attempt=owns_attempt)
    async def sleep(self,seconds):
        await super().sleep(seconds)
        if self.cancel_at is not None and self.seconds>=self.cancel_at:self.cancelled=True
        if self.stop_at is not None and self.seconds>=self.stop_at:self.global_stop=True
    def issue(self,*args,**kwargs):
        if self.first_issue_at is None:self.first_issue_at=self.seconds
        return super().issue(*args,**kwargs)

class Resume(unittest.TestCase):
    def run_case(self,p,timeout=240):
        return asyncio.run(position_once(scanner=A,target={'x_m':5,'y_m':3},port=p,
            owns_attempt=lambda:not p.cancelled,monotonic=lambda:p.seconds,
            sleep=p.sleep,timeout_seconds=timeout))
    def test_fresh_poll_has_no_added_wait(self):
        p=PollPort();self.assertEqual(self.run_case(p)['status'],'ready')
        self.assertEqual(p.first_issue_at,0);self.assertEqual(p.open_calls,1)
    def test_health_ack_then_ten_second_gap(self):
        p=PollPort(10);self.assertEqual(self.run_case(p)['status'],'ready')
        self.assertEqual(p.first_issue_at,10)
        self.assertEqual([c[0] for c in p.commands],['mobility.report.location','mobility.move','mobility.report.location'])
    def test_fresh_at_grace_boundary(self):
        p=PollPort(30);self.assertEqual(self.run_case(p)['status'],'ready');self.assertEqual(p.first_issue_at,30)
    def test_permanent_outage_is_bounded(self):
        p=PollPort(999);r=self.run_case(p)
        self.assertEqual(r['status'],'failed');self.assertIn('30s',r['reason'])
        self.assertEqual(p.seconds,30);self.assertEqual(p.commands,[]);self.assertFalse(p.closed)
    def test_attempt_deadline_remains_stricter(self):
        p=PollPort(999);r=self.run_case(p,5)
        self.assertEqual(r['status'],'failed');self.assertIn('timed out',r['reason']);self.assertEqual(p.seconds,5)
        self.assertEqual(p.commands,[])
    def test_deletion_during_wait_issues_nothing(self):
        p=PollPort(10);p.cancel_at=1;r=self.run_case(p)
        self.assertEqual(r['status'],'failed');self.assertIn('cancelled',r['reason']);self.assertEqual(p.commands,[])
    def test_stop_during_wait_issues_nothing(self):
        p=PollPort(10);p.stop_at=1;r=self.run_case(p)
        self.assertEqual(r['status'],'failed');self.assertIn('Stop is latched',r['reason']);self.assertEqual(p.commands,[])
    def test_registration_block_is_not_retried(self):
        p=PollPort(10);p.registration_blocked=True;r=self.run_case(p)
        self.assertEqual(r['status'],'failed');self.assertEqual(p.seconds,0);self.assertEqual(p.open_calls,1)
    def test_busy_is_not_retried(self):
        p=PollPort(10);p.state='s1waiting_report';r=self.run_case(p)
        self.assertEqual(r['status'],'failed');self.assertEqual(p.seconds,0);self.assertEqual(p.commands,[])

if __name__=='__main__':unittest.main(verbosity=2)
