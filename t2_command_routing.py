from __future__ import annotations

import json
from typing import Any, Dict, List

import config
import m4Commands
import m8mobility
import utility
from m8mobility_state_store import key_state, key_time, key_report, key_pose, _set_state, _save_stop
from m8mobility_state import S0_IDLE

# ============================================================
# Helpers
# ============================================================
def _require_real_whitelist(scanner: str) -> None:
    if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, scanner):
        raise RuntimeError(
            f"{scanner} is not whitelisted. "
            "Do not let tests create whitelist entries."
        )

    raw = config.r.hget(config.KEY_WHITELIST_SCANNER_META, scanner)

    if raw is None:
        raise RuntimeError(f"{scanner} whitelist entry missing")

    if str(raw).strip() in ("1", "true", "True"):
        raise RuntimeError(
            f"{scanner} whitelist entry is corrupted: {raw!r}. "
            "Restore whitelist from backup before running tests."
        )
    

def _reset(scanner: str):
    _require_real_whitelist(scanner)

    keys = [
        config.key_scanner_meta(scanner),
        config.key_cmd_stream(scanner),
        config.key_cmdack_stream(scanner),
        key_state(scanner),
        key_time(scanner),
        key_report(scanner),
        key_pose(scanner),
        config.KEY_TRAFFIC_CMD_STREAM,
    ]

    if hasattr(config, "KEY_MOBILITY_CMD_STREAM"):
        keys.append(config.KEY_MOBILITY_CMD_STREAM)

    for k in keys:
        config.r.delete(k)

    _save_stop(False, "")

    config.r.sadd(config.KEY_REGISTRY, scanner)
    config.r.hset(
        config.key_scanner_meta(scanner),
        mapping={
            "device_type": "robot",
            "last_seen": utility.local_ts(),
        },
    )

    _set_state(scanner, S0_IDLE, "test seed")

    utility._hset_many(
        key_state(scanner),
        {
            "retry_count": "0",
            "collision_veto_count": "0",
            "busy_count": "0",
            "exec_fail_count": "0",
            "correction_attempt_count": "-1",
            "s2_entry_reason": "",
            "stop_experiment": "false",
            "stop_reason": "",
            "need_location_retry": "false",
            "robot_safety_state": "NORMAL",
        },
    )

    utility._hset_many(
        key_pose(scanner),
        {
            "true_location_json": {
                "location_ok": True,
                "x_m": 1.0,
                "y_m": 1.0,
                "heading_deg": 0.0,
            }
        },
    )

    utility._hset_many(
        key_time(scanner),
        {
            "last_mobility_report_at": utility.local_ts(),
        },
    )


def _dump_stream(key: str):
    rows = config.r.xrange(key, count=10)
    print(f"\n[{key}]")
    if not rows:
        print("(empty)")
        return
    for xid, fields in rows:
        print("-", xid, fields)


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def _count_stream(key: str) -> int:
    return len(config.r.xrange(key, count=100))


# ============================================================
# Tests
# ============================================================

def test_traffic_routing(scanner: str):
    print("\n=== TEST: traffic routing ===")

    _reset(scanner)

    m4Commands._enqueue_script_or_csv_item(
        scanner=scanner,
        category="traffic",
        action="traffic.session.start",
        execute_at=utility.local_ts(),
        args={"duration_sec": 60},
    )

    _dump_stream(config.KEY_TRAFFIC_CMD_STREAM)
    _dump_stream(config.key_cmd_stream(scanner))

    _assert(_count_stream(config.KEY_TRAFFIC_CMD_STREAM) == 1, "Traffic should go to traffic stream")
    _assert(_count_stream(config.key_cmd_stream(scanner)) == 0, "Traffic must NOT go to robot command stream")

    print("PASS")


def test_mobility_routing_immediate(scanner: str):
    print("\n=== TEST: mobility immediate routing ===")

    _reset(scanner)

    result = m8mobility.on_command_issued(
        scanner,
        "mobility.turn_move_turn.forward",
        {"distance_m": 1.0, "pre_angle": 0, "post_angle": 0},
    )

    print(json.dumps(result, indent=2))

    _dump_stream(config.key_cmd_stream(scanner))

    _assert(_count_stream(config.key_cmd_stream(scanner)) == 1, "Mobility should emit command via state machine")

    print("PASS")


def test_mobility_routing_script(scanner: str):
    print("\n=== TEST: mobility script routing ===")

    _reset(scanner)

    m4Commands._enqueue_script_or_csv_item(
        scanner=scanner,
        category="mobility",
        action="mobility.turn_move_turn.forward",
        execute_at=utility.local_ts(),
        args={"distance_m": 1.0},
    )

    _dump_stream(config.KEY_MOBILITY_CMD_STREAM)
    _dump_stream(config.key_cmd_stream(scanner))

    _assert(_count_stream(config.KEY_MOBILITY_CMD_STREAM) == 1, "Mobility script should go to mobility stream")
    _assert(_count_stream(config.key_cmd_stream(scanner)) == 0, "Mobility script must NOT go directly to robot command stream")

    print("PASS")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    SCANNER = "twin-scout-bravo"
    tests = {
        "test_traffic_routing": test_traffic_routing,
        "test_mobility_routing_immediate": test_mobility_routing_immediate,
        "test_mobility_routing_script": test_mobility_routing_script,
    }
    TEST_TO_RUN = "test_mobility_routing_script"

    fn = tests.get(TEST_TO_RUN)
    if fn is None:
        raise ValueError("Invalid TEST_TO_RUN")

    fn(SCANNER)
