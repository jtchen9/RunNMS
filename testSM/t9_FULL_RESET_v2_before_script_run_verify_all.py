from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

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


def parse_json_maybe(text: Any) -> Any:
    if not text:
        return None
    if isinstance(text, (dict, list)):
        return text
    try:
        return json.loads(str(text))
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


def redis_type(key: str) -> str:
    try:
        return str(config.r.type(key))
    except Exception:
        return "unknown"


def key_count(key: str) -> int:
    t = redis_type(key)
    try:
        if t == "stream":
            return int(config.r.xlen(key))
        if t == "list":
            return int(config.r.llen(key))
        if t == "zset":
            return int(config.r.zcard(key))
        if t in ("none", "None"):
            return 0
        return int(config.r.exists(key))
    except Exception:
        return -1


def delete_key(key: str) -> Tuple[int, int, str]:
    before = key_count(key)
    t = redis_type(key)
    try:
        config.r.delete(key)
    except Exception:
        pass
    after = key_count(key)
    return before, after, t


def get_string(key: str) -> str:
    try:
        v = config.r.get(key)
        return "" if v is None else str(v)
    except Exception:
        return ""


def find_matching_keys(patterns: List[str]) -> List[str]:
    found: List[str] = []
    for pat in patterns:
        try:
            for k in config.r.scan_iter(pat):
                ks = str(k)
                if ks not in found:
                    found.append(ks)
        except Exception:
            pass
    return sorted(found)


def main() -> None:
    now = utility.local_ts()
    key_prefix = getattr(config, "KEY_PREFIX", "nms:")

    s_key = key_state(SCANNER)
    t_key = key_time(SCANNER)
    p_key = key_pose(SCANNER)

    print("SCRIPT-RUN FULL RESET V2 WITH GLOBAL STOP-LATCH CLEAR")
    print("scanner =", SCANNER)
    print("time =", now)
    print()

    print("BEFORE STATE")
    before_state = {
        "state": hget(s_key, "state"),
        "state_detail": hget(s_key, "state_detail"),
        "robot_safety_state": hget(s_key, "robot_safety_state"),
        "stop_experiment": hget(s_key, "stop_experiment"),
        "stop_reason": hget(s_key, "stop_reason"),
        "outgoing_command_action": hget(s_key, "outgoing_command_action"),
    }
    print(json.dumps(before_state, ensure_ascii=False, indent=2))
    print()

    # Command/schedule queues
    queue_keys: List[str] = []
    try:
        queue_keys.append(config.key_cmd_stream(SCANNER))
    except Exception:
        queue_keys.append(f"{key_prefix}cmd:{SCANNER}")

    if hasattr(config, "KEY_MOBILITY_CMD_STREAM"):
        queue_keys.append(config.KEY_MOBILITY_CMD_STREAM)

    queue_keys.extend([
        f"{key_prefix}mobility:cmd",
        f"{key_prefix}mobility_cmd",
        f"{key_prefix}cmd:mobility",
        "mobility:cmd",
    ])

    for k in find_matching_keys([
        f"{key_prefix}*mobility*cmd*",
        f"{key_prefix}*cmd*mobility*",
        "*mobility:cmd*",
    ]):
        if k not in queue_keys:
            queue_keys.append(k)

    print("COMMAND/SCHEDULE QUEUES TO CLEAR")
    queue_results = []
    for k in queue_keys:
        before, after, typ = delete_key(k)
        queue_results.append((k, before, after, typ))
        print(f"  {k}: type={typ}, before={before}, after={after}")

    # Global stop latches / experiment stop keys. This is the key missed by the first reset.
    stop_keys = [
        f"{key_prefix}mobility:experiment_stop_state_json",
        f"{key_prefix}mobility:experiment_stop",
        f"{key_prefix}mobility:stop_experiment",
        "mobility:experiment_stop_state_json",
    ]
    for k in find_matching_keys([
        f"{key_prefix}*experiment*stop*",
        f"{key_prefix}*stop*experiment*",
        "*experiment_stop_state_json*",
    ]):
        if k not in stop_keys:
            stop_keys.append(k)

    print()
    print("GLOBAL STOP-LATCH KEYS TO CLEAR")
    stop_results = []
    for k in stop_keys:
        before_val = get_string(k)
        before, after, typ = delete_key(k)
        stop_results.append((k, before, after, typ, before_val))
        print(f"  {k}: type={typ}, before={before}, after={after}, before_value={before_val[:240]}")

    # Debug/intercept keys
    prefix = getattr(config, "MOBILITY_REPORT_INTERCEPT_KEY_PREFIX", f"{key_prefix}debug:mobility_report_intercept:")
    enable_base = getattr(config, "MOBILITY_REPORT_INTERCEPT_ENABLE_KEY", f"{key_prefix}debug:mobility_report_intercept:enabled")
    event_stream = getattr(config, "MOBILITY_REPORT_INTERCEPT_EVENT_STREAM", f"{key_prefix}debug:mobility_report_intercept:events")

    debug_keys = [
        f"{prefix}{SCANNER}",
        f"{enable_base}:{SCANNER}",
        enable_base,
        event_stream,
    ]
    print()
    print("DEBUG/INTERCEPT KEYS TO CLEAR")
    for k in debug_keys:
        before, after, typ = delete_key(k)
        print(f"  {k}: type={typ}, before={before}, after={after}")

    # Align planned=true from current true pose if valid.
    true_loc = parse_json_maybe(hget(p_key, "true_location_json"))
    aligned = False
    if pose_ok(true_loc):
        planned_loc = dict(true_loc)
        planned_loc["source"] = planned_loc.get("source") or "testSM.full_reset_v2.align_true"
        planned_loc["updated_at"] = now
        config.r.hset(p_key, mapping={"planned_location_json": json.dumps(planned_loc, ensure_ascii=False)})
        aligned = True

    # Per-robot hard reset.
    config.r.hset(
        s_key,
        mapping={
            "state": "s0idle",
            "state_detail": "testSM full hard reset v2 before script run",
            "mobility_ready": "true",
            "mobility_ready_reason": "testSM full hard reset v2",
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
        },
    )

    after_state = {
        "state": hget(s_key, "state"),
        "state_detail": hget(s_key, "state_detail"),
        "robot_safety_state": hget(s_key, "robot_safety_state"),
        "stop_experiment": hget(s_key, "stop_experiment"),
        "stop_reason": hget(s_key, "stop_reason"),
        "outgoing_command_action": hget(s_key, "outgoing_command_action"),
        "correction_attempt_count": hget(s_key, "correction_attempt_count"),
    }

    queue_failures = [(k, key_count(k)) for k, *_ in queue_results if key_count(k) != 0]
    stop_failures = [(k, key_count(k), get_string(k)) for k, *_ in stop_results if key_count(k) != 0]

    state_ok = (
        after_state["state"] == "s0idle"
        and after_state["robot_safety_state"] == "NORMAL"
        and after_state["stop_experiment"] == "false"
        and after_state["outgoing_command_action"] == ""
    )

    ok = state_ok and not queue_failures and not stop_failures

    print()
    print("AFTER STATE")
    print(json.dumps(after_state, ensure_ascii=False, indent=2))
    print("planned=true aligned from current true_location =", aligned)

    print()
    print("FINAL QUEUE VERIFICATION")
    for k, *_ in queue_results:
        print(f"  {k}: final_count={key_count(k)}")

    print()
    print("FINAL GLOBAL STOP-LATCH VERIFICATION")
    for k, *_ in stop_results:
        print(f"  {k}: final_count={key_count(k)}, value={get_string(k)[:240]}")

    print()
    print("ready_for_new_script_run =", ok)

    if queue_failures:
        print("QUEUE FAILURES =", queue_failures)
    if stop_failures:
        print("STOP-LATCH FAILURES =", stop_failures)

    if not ok:
        raise SystemExit("RESET FAILED: state, queues, or global stop latch are not clean")


if __name__ == "__main__":
    main()
