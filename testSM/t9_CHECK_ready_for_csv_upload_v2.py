from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import List

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(str(PROJECT_ROOT))

import config
from m8mobility_state_store import key_state


SCANNER = os.environ.get("SCANNER", "twin-scout-charlie")


def hget(hash_key: str, field: str, default: str = "") -> str:
    v = config.r.hget(hash_key, field)
    return default if v is None else str(v)


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
    key_prefix = getattr(config, "KEY_PREFIX", "nms:")
    s_key = key_state(SCANNER)

    state = {
        "state": hget(s_key, "state"),
        "state_detail": hget(s_key, "state_detail"),
        "robot_safety_state": hget(s_key, "robot_safety_state"),
        "stop_experiment": hget(s_key, "stop_experiment"),
        "stop_reason": hget(s_key, "stop_reason"),
        "outgoing_command_action": hget(s_key, "outgoing_command_action"),
        "correction_attempt_count": hget(s_key, "correction_attempt_count"),
    }

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

    print("SCRIPT-RUN READY CHECK V2")
    print("scanner =", SCANNER)
    print("state =", json.dumps(state, ensure_ascii=False, indent=2))

    print()
    print("command/schedule queues:")
    queue_bad = []
    for k in queue_keys:
        c = key_count(k)
        print(f"  {k}: count={c}, type={redis_type(k)}")
        if c != 0:
            queue_bad.append((k, c))

    print()
    print("global stop-latch keys:")
    stop_bad = []
    for k in stop_keys:
        c = key_count(k)
        v = get_string(k)
        print(f"  {k}: count={c}, type={redis_type(k)}, value={v[:240]}")
        if c != 0:
            stop_bad.append((k, c, v))

    state_ok = (
        state["state"] == "s0idle"
        and state["robot_safety_state"] == "NORMAL"
        and state["stop_experiment"] == "false"
        and state["outgoing_command_action"] == ""
    )

    ok = state_ok and not queue_bad and not stop_bad

    print()
    print("ready_for_csv_upload =", ok)

    if not ok:
        if not state_ok:
            print("STATE_NOT_READY")
        if queue_bad:
            print("QUEUE_NOT_EMPTY =", queue_bad)
        if stop_bad:
            print("GLOBAL_STOP_LATCH_NOT_EMPTY =", stop_bad)
        raise SystemExit("NOT READY: run t9_FULL_RESET_v2_before_script_run_verify_all.py first")


if __name__ == "__main__":
    main()
