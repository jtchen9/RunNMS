
"""
Layer 3 tests for mobility scheduled-command dispatcher.

Usage:
- Edit TEST_TO_RUN below
- Run this file directly in VS Code

Notes:
- No argparse
- Redis-backed tests
- Directly tests m8mobility scheduled dispatch helpers, not the infinite async loop
"""

from __future__ import annotations

import json
from typing import Any, List

import config
import utility
import m8mobility

from m8mobility_state_store import (
    key_state,
    key_time,
    key_report,
    key_pose,
    _set_state,
    _save_stop,
)
from m8mobility_state import (
    S0_IDLE,
    S1_WAITING_REPORT,
    S2_EVALUATING_POLICY,
)


def _print_title(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _dump_hash(key: str) -> None:
    print(f"\n[{key}]")
    data = config.r.hgetall(key)
    if not data:
        print("(empty)")
        return
    for k in sorted(data.keys()):
        print(f"{k}: {data[k]}")


def _dump_stream(key: str, count: int = 20) -> None:
    print(f"\n[{key}]")
    rows = config.r.xrange(key, count=count)
    if not rows:
        print("(empty)")
        return
    for xid, fields in rows:
        print(f"- {xid}: {fields}")


def _stream_count(key: str) -> int:
    return int(config.r.xlen(key))


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
    

def _reset(scanner: str) -> None:
    _require_real_whitelist(scanner)
    
    keys = [
        config.key_scanner_meta(scanner),
        config.key_cmd_stream(scanner),
        config.key_cmdack_stream(scanner),
        key_state(scanner),
        key_time(scanner),
        key_report(scanner),
        key_pose(scanner),
    ]

    if hasattr(config, "KEY_MOBILITY_CMD_STREAM"):
        keys.append(config.KEY_MOBILITY_CMD_STREAM)

    if hasattr(config, "KEY_TRAFFIC_CMD_STREAM"):
        keys.append(config.KEY_TRAFFIC_CMD_STREAM)

    for k in keys:
        config.r.delete(k)

    try:
        _save_stop(False, "")
    except Exception:
        pass

    config.r.sadd(config.KEY_REGISTRY, scanner)
    config.r.hset(
        config.key_scanner_meta(scanner),
        mapping={
            "device_type": "robot",
            "last_seen": utility.local_ts(),
        },
    )


def _seed_idle_with_true(scanner: str) -> None:
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
        key_time(scanner),
        {
            "s1_timer_token": "",
            "s1_timer_started_at": "",
            "busy_retry_token": "",
            "busy_retry_started_at": "",
            "last_mobility_report_at": utility.local_ts(),
            "last_planned_command_issued_at": "",
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
            },
        },
    )


def _seed_not_idle(scanner: str) -> None:
    _seed_idle_with_true(scanner)
    _set_state(scanner, S2_EVALUATING_POLICY, "test non-idle")


def _enqueue_due_mobility_row(scanner: str, action: str = "mobility.turn") -> str:
    execute_at = utility.local_ts()
    xid = config.r.xadd(
        config.KEY_MOBILITY_CMD_STREAM,
        {
            "scanner": scanner,
            "action": action,
            "execute_at": execute_at,
            "created_at": execute_at,
            "args_json": json.dumps({"angle_deg": 10}, ensure_ascii=False),
            "source": "test",
        },
        maxlen=20000,
        approximate=True,
    )
    return xid


def _show(scanner: str) -> None:
    _dump_stream(config.KEY_MOBILITY_CMD_STREAM)
    _dump_stream(config.key_cmd_stream(scanner))
    _dump_hash(key_state(scanner))
    _dump_hash(key_time(scanner))
    _dump_hash(key_pose(scanner))


def _collect_due() -> List[Any]:
    return m8mobility._collect_due_mobility_commands(utility.local_ts())


def test_due_idle_dispatches_and_deletes(scanner: str) -> None:
    _print_title("TEST: due mobility row dispatches when scanner is idle")

    _reset(scanner)
    _seed_idle_with_true(scanner)
    xid = _enqueue_due_mobility_row(scanner)

    due = _collect_due()
    print("\nDue rows:")
    print(due)

    _assert(len(due) >= 1, "Expected one due mobility row")

    for row_id, fields in due:
        if row_id == xid:
            m8mobility._dispatch_due_mobility_command(row_id, fields)

    _show(scanner)

    state_now = config.r.hget(key_state(scanner), "state") or ""

    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 0, "Expected due mobility row to be deleted after consumed dispatch")
    _assert(state_now in (S0_IDLE, S1_WAITING_REPORT), "Expected consumed dispatch to end in S0 or S1")

    if state_now == S1_WAITING_REPORT:
        _assert(
            _stream_count(config.key_cmd_stream(scanner)) >= 1,
            "Expected robot command stream to receive emitted command when state enters S1",
        )
    else:
        _assert(
            _stream_count(config.key_cmd_stream(scanner)) == 0,
            "Expected no robot command for no-op mobility command ending in S0",
        )

    print("\nPASS")


def test_due_not_idle_keeps_row(scanner: str) -> None:
    _print_title("TEST: due mobility row is kept when scanner is not idle")

    _reset(scanner)
    _seed_not_idle(scanner)
    xid = _enqueue_due_mobility_row(scanner)

    due = _collect_due()
    print("\nDue rows:")
    print(due)

    _assert(len(due) >= 1, "Expected one due mobility row")

    for row_id, fields in due:
        if row_id == xid:
            m8mobility._dispatch_due_mobility_command(row_id, fields)

    _show(scanner)

    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 1, "Expected blocked mobility row to remain scheduled")
    _assert(_stream_count(config.key_cmd_stream(scanner)) == 0, "Expected no robot command while scanner is not idle")

    state_now = config.r.hget(key_state(scanner), "state") or ""
    _assert(state_now == S2_EVALUATING_POLICY, "Expected state to remain non-idle")

    print("\nPASS")


def test_blocked_row_later_succeeds(scanner: str) -> None:
    _print_title("TEST: blocked mobility row later dispatches after scanner becomes idle")

    _reset(scanner)
    _seed_not_idle(scanner)
    xid = _enqueue_due_mobility_row(scanner)

    due = _collect_due()
    for row_id, fields in due:
        if row_id == xid:
            m8mobility._dispatch_due_mobility_command(row_id, fields)

    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 1, "Expected row to remain after blocked dispatch")
    _assert(_stream_count(config.key_cmd_stream(scanner)) == 0, "Expected no robot command after blocked dispatch")

    _seed_idle_with_true(scanner)

    due = _collect_due()
    for row_id, fields in due:
        if row_id == xid:
            m8mobility._dispatch_due_mobility_command(row_id, fields)

    _show(scanner)

    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 0, "Expected row deleted after later accepted dispatch")
    _assert(_stream_count(config.key_cmd_stream(scanner)) >= 1, "Expected robot command after later accepted dispatch")

    state_now = config.r.hget(key_state(scanner), "state") or ""
    _assert(state_now == S1_WAITING_REPORT, "Expected scanner to move to S1 after later dispatch")

    print("\nPASS")


if __name__ == "__main__":
    SCANNER = "twin-scout-bravo"
    tests = {
        "test_due_idle_dispatches_and_deletes": test_due_idle_dispatches_and_deletes,
        "test_due_not_idle_keeps_row": test_due_not_idle_keeps_row,
        "test_blocked_row_later_succeeds": test_blocked_row_later_succeeds,
    }
    TEST_TO_RUN = "test_blocked_row_later_succeeds"

    fn = tests.get(TEST_TO_RUN)
    if fn is None:
        raise ValueError(f"Unknown TEST_TO_RUN: {TEST_TO_RUN}")

    fn(SCANNER)
