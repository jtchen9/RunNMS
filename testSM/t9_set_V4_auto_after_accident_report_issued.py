"""
Arm V4 automatically after the NMS has issued the accident-recovery
mobility.report.location command.

Usage sequence:
  1) python .\testSM\t9_set_V3_collision_on_next_forward.py
  2) In another terminal, start this watcher:
       python .\testSM\t9_set_V4_auto_after_accident_report_issued.py
  3) Run the normal forward test script.

This script waits until Redis shows:
  outgoing_command_action == mobility.report.location
  outgoing_command_source == location_recovery_report
  location_recovery_context == accident
  location_recovery_phase == after_report_location

Then it arms an intercept rule so the next mobility.report.location report is
patched to LOCATION_CAPTURE_FAIL. This removes the need for a human to time the
V4 intercept and prevents V4 from accidentally catching the next test's
precondition location report.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict

NMS_ROOT = Path(__file__).resolve().parents[1]
if str(NMS_ROOT) not in sys.path:
    sys.path.insert(0, str(NMS_ROOT))

import config  # noqa: E402
import utility  # noqa: E402
from m8mobility_state_store import key_state, key_pose, key_report, key_time  # noqa: E402
from t9_mobility_report_intercept import arm_rule  # noqa: E402


SCANNER = "twin-scout-charlie"
TIMEOUT_SEC = 120
POLL_SEC = 0.25


def _hget(key: str, field: str, default: str = "") -> str:
    v = config.r.hget(key, field)
    if v is None:
        return default
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v)


def _json_field(key: str, field: str) -> Any:
    raw = _hget(key, field, "")
    if not raw:
        return ""
    try:
        return json.loads(raw)
    except Exception:
        return raw


def _last_report_dict(report_key: str) -> Dict[str, Any]:
    obj = _json_field(report_key, "last_mobility_report_json")
    return obj if isinstance(obj, dict) else {}


def snapshot(scanner: str) -> Dict[str, Any]:
    state_key = key_state(scanner)
    pose_key = key_pose(scanner)
    report_key = key_report(scanner)
    time_key = key_time(scanner)
    last_report = _last_report_dict(report_key)

    return {
        "ts": utility.local_ts(),
        "state": _hget(state_key, "state"),
        "state_detail": _hget(state_key, "state_detail"),
        "robot_safety_state": _hget(state_key, "robot_safety_state"),
        "outgoing_command_action": _hget(state_key, "outgoing_command_action"),
        "outgoing_command_source": _hget(state_key, "outgoing_command_source"),
        "outgoing_command_args_json": _json_field(state_key, "outgoing_command_args_json"),
        "collision_veto_count": _hget(state_key, "collision_veto_count"),
        "exec_fail_count": _hget(state_key, "exec_fail_count"),
        "visibility_turn_count": _hget(state_key, "visibility_turn_count"),
        "location_recovery_context": _hget(state_key, "location_recovery_context"),
        "location_recovery_phase": _hget(state_key, "location_recovery_phase"),
        "last_planned_command_action": _hget(pose_key, "last_planned_command_action"),
        "last_planned_command_args_json": _json_field(pose_key, "last_planned_command_args_json"),
        "last_report_action": last_report.get("last_command", ""),
        "last_report_error": last_report.get("last_error_code", ""),
        "last_mobility_report_at": _hget(time_key, "last_mobility_report_at"),
    }


def main() -> None:
    start = time.time()
    last_print = 0.0

    while time.time() - start < TIMEOUT_SEC:
        snap = snapshot(SCANNER)

        is_accident_report_issued = (
            snap.get("outgoing_command_action") == "mobility.report.location"
            and snap.get("outgoing_command_source") == "location_recovery_report"
            and snap.get("location_recovery_context") == "accident"
            and snap.get("location_recovery_phase") == "after_report_location"
        )

        if is_accident_report_issued:
            rule = {
                "mode": "patch",
                "match_action": "mobility.report.location",
                "once": True,
                "patch": {
                    "last_exec_status": "failed",
                    "last_error_code": "LOCATION_CAPTURE_FAIL",
                    "last_error_detail": "V4 auto injected report.location failure",
                    "last_location_result": {
                        "ok": False,
                        "error": "V4 auto injected LOCATION_CAPTURE_FAIL",
                        "apriltag": {
                            "ok": False,
                            "count": 0,
                            "tags": [],
                        },
                    },
                },
            }
            armed = arm_rule(SCANNER, rule)
            print(json.dumps({"status": "armed", "armed": armed, "rule": rule, "trigger_snapshot": snap}, indent=2, ensure_ascii=False))
            return

        if time.time() - last_print >= 5.0:
            print(json.dumps({"status": "waiting_for_accident_report", "elapsed_sec": int(time.time() - start), "snapshot": snap}, indent=2, ensure_ascii=False))
            last_print = time.time()

        time.sleep(POLL_SEC)

    print(json.dumps({"status": "timeout", "detail": "accident recovery report.location was not detected before timeout", "last_snapshot": snapshot(SCANNER)}, indent=2, ensure_ascii=False))
    raise SystemExit(2)


if __name__ == "__main__":
    main()
