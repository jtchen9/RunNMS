from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(str(PROJECT_ROOT))

import config
import utility
from m8mobility_state_store import key_state, key_time, key_pose, key_report


SCANNER = os.environ.get("SCANNER", "twin-scout-charlie")


def hget(hash_key: str, field: str, default: str = "") -> str:
    v = config.r.hget(hash_key, field)
    return default if v is None else str(v)


def parse_json_maybe(text: str) -> Any:
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def pose_ok(p: Any) -> bool:
    return (
        isinstance(p, dict)
        and p.get("location_ok", True) is not False
        and p.get("x_m") is not None
        and p.get("y_m") is not None
        and p.get("heading_deg") is not None
    )


def clear_stream(key: str) -> int:
    try:
        old_len = int(config.r.xlen(key))
    except Exception:
        old_len = -1
    try:
        config.r.delete(key)
    except Exception:
        pass
    return old_len


def delete_key(k: str) -> int:
    try:
        return int(config.r.delete(k) or 0)
    except Exception:
        return 0


def main() -> None:
    now = utility.local_ts()

    s_key = key_state(SCANNER)
    t_key = key_time(SCANNER)
    p_key = key_pose(SCANNER)
    r_key = key_report(SCANNER)

    before_state = hget(s_key, "state", "")
    before_detail = hget(s_key, "state_detail", "")
    before_stop = hget(s_key, "stop_experiment", "")
    before_safety = hget(s_key, "robot_safety_state", "")

    old_robot_cmd_len = clear_stream(config.key_cmd_stream(SCANNER))
    old_mobility_cmd_len = clear_stream(config.KEY_MOBILITY_CMD_STREAM)

    # Clear debug outlet from previous tests/runs.
    prefix = getattr(config, "MOBILITY_REPORT_INTERCEPT_KEY_PREFIX", f"{config.KEY_PREFIX}debug:mobility_report_intercept:")
    enable_base = getattr(config, "MOBILITY_REPORT_INTERCEPT_ENABLE_KEY", f"{config.KEY_PREFIX}debug:mobility_report_intercept:enabled")
    event_stream = getattr(config, "MOBILITY_REPORT_INTERCEPT_EVENT_STREAM", f"{config.KEY_PREFIX}debug:mobility_report_intercept:events")

    deleted_debug = 0
    debug_keys = [
        f"{prefix}{SCANNER}",
        f"{enable_base}:{SCANNER}",
        enable_base,
        event_stream,
    ]
    for k in debug_keys:
        deleted_debug += delete_key(k)

    # Align planned=true if there is a valid current true pose. This prevents stale planned target chase.
    true_loc = parse_json_maybe(hget(p_key, "true_location_json"))
    aligned = False
    if pose_ok(true_loc):
        planned_loc = dict(true_loc)
        planned_loc["source"] = planned_loc.get("source") or "testSM.hard_reset.align_true"
        planned_loc["updated_at"] = now
        config.r.hset(p_key, mapping={"planned_location_json": json.dumps(planned_loc, ensure_ascii=False)})
        aligned = True

    # Hard-reset state-machine control state.
    config.r.hset(
        s_key,
        mapping={
            "state": "s0idle",
            "state_detail": "testSM hard reset before script run",
            "mobility_ready": "true",
            "mobility_ready_reason": "testSM hard reset",
            "robot_safety_state": "NORMAL",
            "stop_experiment": "false",
            "stop_reason": "",
            "need_location_retry": "false",
            "retry_count": "0",
            "busy_count": "0",
            "collision_veto_count": "0",
            "exec_fail_count": "0",
            # Leave correction enabled by default for script-run diagnostics.
            # A separate smoke-only test may set this to 999.
            "correction_attempt_count": "-1",
            "outgoing_command_action": "",
            "outgoing_command_args_json": "",
            "outgoing_command_source": "",
            "outgoing_command_updated_at": "",
            "pending_sequence_json": "",
            "pending_sequence_len": "0",
            "pending_sequence_reason": "",
            "last_error_code": "",
            "last_error_detail": "",
            "s2_entry_reason": "",
            "true_propagation_applied": "",
            "true_propagation_detail": "",
            "true_propagation_time": "",
        },
    )

    config.r.hset(
        t_key,
        mapping={
            "policy_updated_at": now,
            "s1_timer_token": "",
            "s1_timer_started_at": "",
            "busy_retry_token": "",
            "busy_retry_started_at": "",
            # Keep historical location timestamps; clear only active control timers.
        },
    )

    # Do not delete the last report. It is useful for diagnostics.
    # But clear any stale outgoing command mirror fields in report hash is not needed.

    after = {
        "state": hget(s_key, "state"),
        "state_detail": hget(s_key, "state_detail"),
        "robot_safety_state": hget(s_key, "robot_safety_state"),
        "stop_experiment": hget(s_key, "stop_experiment"),
        "stop_reason": hget(s_key, "stop_reason"),
        "outgoing_command_action": hget(s_key, "outgoing_command_action"),
        "correction_attempt_count": hget(s_key, "correction_attempt_count"),
    }

    ok = (
        after["state"] == "s0idle"
        and after["robot_safety_state"] == "NORMAL"
        and after["stop_experiment"] == "false"
        and after["outgoing_command_action"] == ""
    )

    # Enable pass-through logging after the hard reset.
    config.r.delete(event_stream)
    config.r.set(f"{enable_base}:{SCANNER}", "true")
    config.r.set(enable_base, "true")
    config.r.set(
        f"{prefix}{SCANNER}",
        json.dumps({
            "mode": "pass",
            "once": False,
            "note": "M2v3 pass-through report capture after hard reset",
        }, ensure_ascii=False),
    )

    print("M2v3 HARD RESET + PASS-CAPTURE PREP")
    print("scanner =", SCANNER)
    print("before_state =", before_state)
    print("before_detail =", before_detail)
    print("before_stop_experiment =", before_stop)
    print("before_robot_safety_state =", before_safety)
    print("cleared robot command stream len =", old_robot_cmd_len)
    print("cleared mobility scheduled stream len =", old_mobility_cmd_len)
    print("deleted debug/intercept keys =", deleted_debug)
    print("planned=true aligned from current true_location =", aligned)
    print("after =", json.dumps(after, ensure_ascii=False))
    print("ready_for_new_script_run =", ok)
    print("pass_capture_enabled =", True)
    print("intercept_rule_key =", f"{prefix}{SCANNER}")
    print("scanner_enable_key =", f"{enable_base}:{SCANNER}")
    print("global_enable_key =", enable_base)
    print("event_stream =", event_stream)

    if not ok:
        raise SystemExit("RESET FAILED: system is not ready for script run")


if __name__ == "__main__":
    main()
