"""Offline timestamp regression; no Redis, robot, or network access.
Run from repository root: python testSM/testPreparation/timestamp_check.py
"""
import ast
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
import types
import unittest
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from nms_preparation_positioning import preparation_report_fresh, measured_evidence, PositioningError
FMT = "%Y-%m-%d-%H:%M:%S"
FIXTURE = json.loads(Path(__file__).with_name("alpha_timestamp_fixture.json").read_text())
# Same instants as the Taipei log, rendered in the test host's timezone.
ISSUED = datetime.fromtimestamp(1795176317).strftime(FMT)
SOLVED = datetime.fromtimestamp(1795176345).strftime(FMT)

class TimestampChecks(unittest.TestCase):
    def setUp(self):
        self.report = deepcopy(FIXTURE["report"])
        self.previous = deepcopy(FIXTURE["previous_report"])

    def evidence(self):
        pose = dict(location_ok=True, source="apriltag", x_m=5., y_m=2.5, heading_deg=270.)
        # Pose is synthetic: only timestamp/evidence validation is tested here.
        return dict(pose=pose, evidence=dict(scope="attempt3", solve_id="new-solve",
            pose=deepcopy(pose), report=self.report, issued_at=ISSUED, solved_at=SOLVED))

    def test_actual_alpha_numeric_report_accepted(self):
        self.assertEqual(preparation_report_fresh(self.report, ISSUED, self.previous), (True, ""))

    def test_actual_s1_command_matcher_accepts_report(self):
        tree=ast.parse((ROOT/"m8mobility_state.py").read_text(encoding="utf-8"))
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=="_s1_report_matches_expected_command")
        fields=dict(last_planned_command_action="mobility.report.location", last_planned_command_args_json="{}",
                    preparation_scope="attempt3", last_planned_command_issued_at=ISSUED)
        ns=dict(Dict=Dict, Any=Any, json=json,
            utility=types.SimpleNamespace(_hget=lambda k,f,d="":fields.get(f,d),
                _hget_json=lambda k,f:self.previous if f=="preparation_baseline_report_json" else {}),
            key_pose=lambda s:s, key_state=lambda s:s, key_time=lambda s:s,
            _load_report_json=lambda s:self.report, _normalize_mobility_command=lambda a,b:(a,b))
        exec(compile(ast.Module(body=[node],type_ignores=[]),"actual_s1_gate","exec"),ns)
        self.assertTrue(ns[node.name]("twin-scout-alpha")[0])
        self.report["last_command"]="mobility.move"
        self.assertFalse(ns[node.name]("twin-scout-alpha")[0])

    def test_numeric_report_reaches_evidence_check(self):
        s=self.evidence()
        self.assertEqual(measured_evidence(s,scope="attempt3",since=ISSUED)[0],s["pose"])

    def test_fractional_finish_within_solve_second(self):
        s=self.evidence()
        s["evidence"]["solved_at"]=datetime.fromtimestamp(self.report["last_command_finished_ts"]).strftime(FMT)
        measured_evidence(s,scope="attempt3",since=ISSUED)

    def test_finish_after_solve_second_rejected(self):
        s=self.evidence()
        s["evidence"]["solved_at"]=datetime.fromtimestamp(self.report["last_command_finished_ts"]-1).strftime(FMT)
        with self.assertRaises(PositioningError): measured_evidence(s,scope="attempt3",since=ISSUED)

    def test_previous_attempt_report_rejected(self):
        self.assertFalse(preparation_report_fresh(self.previous,ISSUED,{})[0])

    def test_reused_camera_captures_rejected(self):
        self.assertFalse(preparation_report_fresh(self.report,ISSUED,self.report)[0])

    def test_reversed_execution_order_rejected(self):
        self.report["last_command_finished_ts"]=self.report["last_command_received_ts"]-.001
        self.assertFalse(preparation_report_fresh(self.report,ISSUED,{})[0])

    def test_legacy_formatted_report_still_accepted(self):
        for key in ("last_command_received_ts","last_command_finished_ts"):
            self.report[key]=datetime.fromtimestamp(self.report[key]).strftime(FMT)
        self.assertTrue(preparation_report_fresh(self.report,ISSUED,{})[0])
        measured_evidence(self.evidence(),scope="attempt3",since=ISSUED)

    def test_integer_unix_seconds_accepted(self):
        for key in ("last_command_received_ts","last_command_finished_ts"):
            self.report[key]=int(self.report[key])
        self.assertTrue(preparation_report_fresh(self.report,ISSUED,{})[0])

    def test_invalid_values_rejected(self):
        for value in (None,True,False,"",[],{},float("nan"),float("inf"),-1,0,1795176326051,1e300):
            with self.subTest(value=value):
                r=deepcopy(self.report);r["last_command_received_ts"]=value
                self.assertFalse(preparation_report_fresh(r,ISSUED,{})[0])

    def test_missing_execution_timestamp_rejected(self):
        self.report.pop("last_command_received_ts")
        self.assertFalse(preparation_report_fresh(self.report,ISSUED,{})[0])

    def test_fresh_error_reaches_existing_error_handling(self):
        self.report["last_exec_status"]="error"
        self.report.pop("last_location_result")
        self.assertTrue(preparation_report_fresh(self.report,ISSUED,{})[0])

    def test_old_scope_and_repeated_solve_rejected(self):
        s=self.evidence()
        with self.assertRaises(PositioningError): measured_evidence(s,scope="different",since=ISSUED)
        with self.assertRaises(PositioningError): measured_evidence(s,scope="attempt3",since=ISSUED,previous=s["evidence"])

if __name__=="__main__": unittest.main(verbosity=2)
