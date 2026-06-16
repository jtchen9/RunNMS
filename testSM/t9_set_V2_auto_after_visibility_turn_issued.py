"""
Arm V2 automatically after the NMS has issued the visibility-recovery turn.

Usage sequence:
  1) python .\testSM\t9_set_V1_no_tag_on_next_forward.py
  2) In another terminal, start this watcher:
       python .\testSM\t9_set_V2_auto_after_visibility_turn_issued.py
  3) Run the normal forward test script.

This script waits until Redis shows:
  outgoing_command_action == mobility.turn
  outgoing_command_source in {location_recovery_visibility_turn, location_precondition_visibility_turn, visibility_turn_recovery}
  visibility_turn_count == 1

Then it arms an intercept rule so the next mobility.turn report is patched to
NO_TAG_VISIBLE. This removes the need for a human to time the V2 intercept.
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


def snapshot(scanner: str) -> Dict[str, Any]:
    state_key = key_state(scanner)
    pose_key = key_pose(scanner)
    report_key = key_report(scanner)
    time_key = key_time(scanner)

    def _json_field(key: str, field: str) -> Any:
        raw = _hget(key, field, "")
        if not raw:
            return ""
        try:
            return json.loads(raw)
        except Exception:
            return raw

    return {
        "ts": utility.local_ts(),
        "state": _hget(state_key, "state"),
        "state_detail": _hget(state_key, "state_detail"),
        "outgoing_command_action": _hget(state_key, "outgoing_command_action"),
        "outgoing_command_source": _hget(state_key, "outgoing_command_source"),
        "outgoing_command_args_json": _json_field(state_key, "outgoing_command_args_json"),
        "visibility_turn_count": _hget(state_key, "visibility_turn_count"),
        "location_recovery_context": _hget(state_key, "location_recovery_context"),
        "location_recovery_phase": _hget(state_key, "location_recovery_phase"),
        "last_planned_command_action": _hget(pose_key, "last_planned_command_action"),
        "last_planned_command_args_json": _json_field(pose_key, "last_planned_command_args_json"),
        "last_report_action": (_json_field(report_key, "last_mobility_report_json") or {}).get("last_command", "") if isinstance(_json_field(report_key, "last_mobility_report_json"), dict) else "",
        "last_report_error": (_json_field(report_key, "last_mobility_report_json") or {}).get("last_error_code", "") if isinstance(_json_field(report_key, "last_mobility_report_json"), dict) else "",
        "last_mobility_report_at": _hget(time_key, "last_mobility_report_at"),
    }


def main() -> None:
    start = time.time()
    armed = None
    last_print = 0.0

    while time.time() - start < TIMEOUT_SEC:
        snap = snapshot(SCANNER)

        is_visibility_turn_issued = (
            snap.get("outgoing_command_action") == "mobility.turn"
            and snap.get("outgoing_command_source") in {"location_recovery_visibility_turn", "location_precondition_visibility_turn", "visibility_turn_recovery"}
            and str(snap.get("visibility_turn_count") or "") == "1"
        )

        if is_visibility_turn_issued:
            rule = {
                "mode": "patch",
                "match_action": "mobility.turn",
                "once": True,
                "patch": {
                    "last_exec_status": "failed",
                    "last_error_code": "NO_TAG_VISIBLE",
                    "last_error_detail": "V2 auto injected no-tag on visibility turn",
                    "last_location_result": {
                        "ok": False,
                        "error": "V2 auto injected no tags on visibility turn",
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

        # Print a heartbeat every 5 seconds so the operator knows the watcher is alive.
        if time.time() - last_print >= 5.0:
            print(json.dumps({"status": "waiting_for_visibility_turn", "elapsed_sec": int(time.time() - start), "snapshot": snap}, indent=2, ensure_ascii=False))
            last_print = time.time()

        time.sleep(POLL_SEC)

    print(json.dumps({"status": "timeout", "detail": "visibility turn was not detected before timeout", "last_snapshot": snapshot(SCANNER)}, indent=2, ensure_ascii=False))
    raise SystemExit(2)


if __name__ == "__main__":
    main()
