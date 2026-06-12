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

from m8mobility_state_store import key_state, key_time, key_pose


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


def main() -> None:
    s_key = key_state(SCANNER)
    t_key = key_time(SCANNER)
    p_key = key_pose(SCANNER)

    old_robot_cmd_len = clear_stream(config.key_cmd_stream(SCANNER))
    old_mobility_cmd_len = clear_stream(config.KEY_MOBILITY_CMD_STREAM)

    prefix = getattr(config, "MOBILITY_REPORT_INTERCEPT_KEY_PREFIX", f"{config.KEY_PREFIX}debug:mobility_report_intercept:")
    enable_base = getattr(config, "MOBILITY_REPORT_INTERCEPT_ENABLE_KEY", f"{config.KEY_PREFIX}debug:mobility_report_intercept:enabled")
    event_stream = getattr(config, "MOBILITY_REPORT_INTERCEPT_EVENT_STREAM", f"{config.KEY_PREFIX}debug:mobility_report_intercept:events")

    rule_key = f"{prefix}{SCANNER}"
    enable_key = f"{enable_base}:{SCANNER}"

    config.r.delete(event_stream)
    config.r.set(enable_key, "true")
    config.r.set(
        rule_key,
        json.dumps(
            {
                "mode": "pass",
                "once": False,
                "note": "M2 pass-through report capture: log all mobility reports, do not modify reports",
            },
            ensure_ascii=False,
        ),
    )

    true_loc = parse_json_maybe(hget(p_key, "true_location_json"))
    aligned = False
    if pose_ok(true_loc):
        planned_loc = dict(true_loc)
        planned_loc["source"] = planned_loc.get("source") or "testSM.M2.align_before_csv"
        planned_loc["updated_at"] = utility.local_ts()
        config.r.hset(p_key, mapping={"planned_location_json": json.dumps(planned_loc, ensure_ascii=False)})
        aligned = True

    config.r.hset(
        s_key,
        mapping={
            "state": "s0idle",
            "state_detail": "testSM M2 prepared for CSV script-run with pass-through report capture",
            "mobility_ready": "true",
            "robot_safety_state": "NORMAL",
            "stop_experiment": "false",
            "stop_reason": "",
            "need_location_retry": "false",
            "retry_count": "0",
            "busy_count": "0",
            "collision_veto_count": "0",
            "exec_fail_count": "0",
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
        },
    )

    config.r.hset(
        t_key,
        mapping={
            "policy_updated_at": utility.local_ts(),
            "s1_timer_token": "",
            "s1_timer_started_at": "",
            "busy_retry_token": "",
            "busy_retry_started_at": "",
        },
    )

    print("M2 prepared CSV script-run with pass-through mobility report capture")
    print("scanner =", SCANNER)
    print("cleared robot command stream len =", old_robot_cmd_len)
    print("cleared mobility scheduled stream len =", old_mobility_cmd_len)
    print("planned=true aligned from current true_location =", aligned)
    print("intercept rule key =", rule_key)
    print("intercept enable key =", enable_key)
    print("event stream =", event_stream)
    print("rule = pass-through, once=false, no match_action; all mobility reports logged")


if __name__ == "__main__":
    main()
