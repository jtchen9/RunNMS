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


SCANNER = os.environ.get("SCANNER", "twin-scout-delta")


def hgetall(hash_key: str) -> Dict[str, Any]:
    d = config.r.hgetall(hash_key)
    return dict(d) if isinstance(d, dict) else {}


def hget(hash_key: str, field: str, default: str = "") -> str:
    v = config.r.hget(hash_key, field)
    if v is None:
        return default
    return str(v)


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

    # Clear debug intercept state so script-run is not affected by previous tests.
    intercept_keys = [
        f"{config.KEY_PREFIX}debug:mobility_report_intercept:{SCANNER}",
        f"{config.KEY_PREFIX}debug:mobility_report_intercept:enabled:{SCANNER}",
        f"{config.KEY_PREFIX}debug:mobility_report_intercept:enabled",
    ]
    deleted_intercepts = 0
    for k in intercept_keys:
        deleted_intercepts += int(config.r.delete(k) or 0)

    true_loc = parse_json_maybe(hget(p_key, "true_location_json"))
    if pose_ok(true_loc):
        planned_loc = dict(true_loc)
        planned_loc["source"] = planned_loc.get("source") or "testSM.M0.align"
        planned_loc["updated_at"] = utility.local_ts()
        config.r.hset(
            p_key,
            mapping={
                "planned_location_json": json.dumps(planned_loc, ensure_ascii=False),
            },
        )
        aligned = True
    else:
        aligned = False

    config.r.hset(
        s_key,
        mapping={
            "state": "s0idle",
            "state_detail": "testSM M0 prepared for mobility CSV script-run",
            "mobility_ready": "true",
            "robot_safety_state": "NORMAL",
            "stop_experiment": "false",
            "stop_reason": "",
            "need_location_retry": "false",
            "retry_count": "0",
            "busy_count": "0",
            "collision_veto_count": "0",
            "exec_fail_count": "0",
            # Let script-run exercise normal correction behavior if needed.
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

    print("M0 prepared mobility script-run")
    print("scanner =", SCANNER)
    print("cleared robot command stream len =", old_robot_cmd_len)
    print("cleared mobility scheduled stream len =", old_mobility_cmd_len)
    print("cleared intercept keys =", deleted_intercepts)
    print("planned=true aligned =", aligned)
    print("state = s0idle")
    print("correction_attempt_count = -1")


if __name__ == "__main__":
    main()
