
"""
Layer 5 tests: CSV/script pipeline integration.

Goal:
Verify end-to-end NMS-side flow without HTTP and without infinite background loops.

Pipeline under test:
CSV-style row / script helper
-> m4Commands._enqueue_script_or_csv_item()
-> experiment registry
-> mobility scheduled stream / traffic stream / normal cmd stream
-> m8mobility scheduler dispatch
-> mobility state machine
-> robot command stream

Safety rules:
- Tests NEVER write to KEY_WHITELIST_SCANNER_META.
- Scanner must already be whitelisted.
- Tests only reset runtime/test keys.
- No argparse; edit variables below and run in VS Code.
"""

from __future__ import annotations

from datetime import timedelta
import json
from typing import Any, Dict, List

import config
import utility
import m4Commands
import m8mobility
import m7Traffic

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


# ============================================================
# Safety / utility helpers
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


def _stream_rows(key: str, count: int = 100) -> List[Any]:
    return config.r.xrange(key, count=count)


def _stream_count(key: str) -> int:
    return int(config.r.xlen(key))


def _ts(offset_sec: int = 0) -> str:
    return (utility.parse_local_dt(utility.local_ts()) + timedelta(seconds=offset_sec)).strftime(config.TIME_FMT)


def _reset_runtime(scanner: str) -> None:
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
        config.KEY_TRAFFIC_EVENT_STREAM,
        config.KEY_TRAFFIC_RESULT_STREAM,
        config.KEY_EXPERIMENT_REGISTRY,
    ]

    if hasattr(config, "KEY_MOBILITY_CMD_STREAM"):
        keys.append(config.KEY_MOBILITY_CMD_STREAM)

    for k in keys:
        try:
            config.r.delete(k)
        except Exception:
            pass

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
            # give traffic tests an IP-like value; we do not execute traffic here
            "ip": "192.168.11.180",
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


def _register_test_experiment(t0: str, last_execute_at: str, item_count: int, scenario_name: str) -> Dict[str, Any]:
    return m4Commands._register_experiment_status(
        t0_str=t0,
        last_execute_at_str=last_execute_at,
        item_count=item_count,
        source="test_pipeline",
        scenario_name=scenario_name,
    )


def _dispatch_all_due_mobility_once() -> None:
    due = m8mobility._collect_due_mobility_commands(utility.local_ts())
    print("\nDue mobility rows:")
    print(due)

    for xid, fields in due:
        m8mobility._dispatch_due_mobility_command(xid, fields)


def _show(scanner: str) -> None:
    _dump_stream(config.KEY_EXPERIMENT_REGISTRY)
    _dump_stream(config.KEY_MOBILITY_CMD_STREAM)
    _dump_stream(config.KEY_TRAFFIC_CMD_STREAM)
    _dump_stream(config.key_cmd_stream(scanner))
    _dump_hash(key_state(scanner))
    _dump_hash(key_time(scanner))
    _dump_hash(key_pose(scanner))


# ============================================================
# Tests
# ============================================================

def test_single_mobility_pipeline(scanner: str) -> None:
    """
    One CSV-style mobility row:
    - goes to mobility scheduled stream
    - experiment registry receives one experiment
    - scheduler dispatches it into mobility subsystem
    - robot command stream gets emitted command
    """
    _print_title("TEST: single mobility pipeline")

    _reset_runtime(scanner)
    _seed_idle_with_true(scanner)

    t0 = _ts(0)
    last = _ts(0)

    m4Commands._enqueue_script_or_csv_item(
        scanner=scanner,
        category="mobility",
        action="mobility.turn",
        execute_at=t0,
        args={"angle_deg": 10},
    )

    exp = _register_test_experiment(
        t0=t0,
        last_execute_at=last,
        item_count=1,
        scenario_name="single_mobility.csv",
    )
    print("\nRegistered experiment:")
    print(json.dumps(exp, ensure_ascii=False, indent=2))

    _assert(_stream_count(config.KEY_EXPERIMENT_REGISTRY) == 1, "Expected one experiment registry row")
    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 1, "Expected one scheduled mobility row before dispatch")
    _assert(_stream_count(config.key_cmd_stream(scanner)) == 0, "Expected no direct robot command before mobility dispatch")

    _dispatch_all_due_mobility_once()
    _show(scanner)

    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 0, "Expected mobility scheduled row consumed")
    _assert(_stream_count(config.key_cmd_stream(scanner)) >= 1, "Expected emitted robot mobility command")

    state_now = config.r.hget(key_state(scanner), "state") or ""
    _assert(state_now == S1_WAITING_REPORT, "Expected scanner to wait for report after command emission")

    print("\nPASS")


def test_mixed_mobility_and_traffic_pipeline(scanner: str) -> None:
    """
    Mixed CSV-style rows:
    - mobility row -> mobility scheduled stream -> robot command after dispatch
    - traffic row -> traffic stream only
    - traffic never goes into robot command stream directly
    """
    _print_title("TEST: mixed mobility and traffic pipeline")

    _reset_runtime(scanner)
    _seed_idle_with_true(scanner)

    t0 = _ts(0)

    m4Commands._enqueue_script_or_csv_item(
        scanner=scanner,
        category="mobility",
        action="mobility.turn",
        execute_at=t0,
        args={"angle_deg": 10},
    )

    m4Commands._enqueue_script_or_csv_item(
        scanner=scanner,
        category="traffic",
        action="traffic.session.start",
        execute_at=t0,
        args={
            "session_id": "test-traffic-001",
            "protocol": "udp",
            "ac": "vi",
            "duration_sec": 60,
            "report_interval_sec": 60,
            "reverse": True,
        },
    )

    exp = _register_test_experiment(
        t0=t0,
        last_execute_at=t0,
        item_count=2,
        scenario_name="mixed_mobility_traffic.csv",
    )
    print("\nRegistered experiment:")
    print(json.dumps(exp, ensure_ascii=False, indent=2))

    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 1, "Expected one mobility scheduled row")
    _assert(_stream_count(config.KEY_TRAFFIC_CMD_STREAM) == 1, "Expected one traffic scheduled row")
    _assert(_stream_count(config.key_cmd_stream(scanner)) == 0, "Expected no robot command before mobility dispatch")

    _dispatch_all_due_mobility_once()
    _show(scanner)

    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 0, "Expected mobility row consumed")
    _assert(_stream_count(config.KEY_TRAFFIC_CMD_STREAM) == 1, "Expected traffic row to remain in traffic stream")
    _assert(_stream_count(config.key_cmd_stream(scanner)) >= 1, "Expected robot command only from mobility emission")

    traffic_rows = _stream_rows(config.KEY_TRAFFIC_CMD_STREAM)
    _assert(traffic_rows[0][1].get("action") == "traffic.session.start", "Expected traffic start action in traffic stream")

    print("\nPASS")


def test_time_ordering_due_only(scanner: str) -> None:
    """
    Two mobility rows:
    - one due now
    - one future
    Scheduler should dispatch only due row.
    """
    _print_title("TEST: time ordering dispatches only due mobility row")

    _reset_runtime(scanner)
    _seed_idle_with_true(scanner)

    now = _ts(0)
    future = _ts(600)

    m4Commands._enqueue_script_or_csv_item(
        scanner=scanner,
        category="mobility",
        action="mobility.turn",
        execute_at=now,
        args={"angle_deg": 10},
    )

    m4Commands._enqueue_script_or_csv_item(
        scanner=scanner,
        category="mobility",
        action="mobility.turn",
        execute_at=future,
        args={"angle_deg": 20},
    )

    _register_test_experiment(
        t0=now,
        last_execute_at=future,
        item_count=2,
        scenario_name="time_ordering.csv",
    )

    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 2, "Expected two scheduled mobility rows before dispatch")

    _dispatch_all_due_mobility_once()
    _show(scanner)

    # After first dispatch, scanner is S1 waiting, so only first due command should be consumed.
    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 1, "Expected future mobility row to remain")
    _assert(_stream_count(config.key_cmd_stream(scanner)) >= 1, "Expected due command to emit robot command")

    remaining = _stream_rows(config.KEY_MOBILITY_CMD_STREAM)
    _assert(len(remaining) == 1, "Expected exactly one remaining row")
    _assert(remaining[0][1].get("execute_at") == future, "Expected remaining row to be the future row")

    print("\nPASS")


def test_blocked_mobility_pipeline_then_retry(scanner: str) -> None:
    """
    Mobility row loaded while scanner is not idle:
    - first scheduler dispatch keeps row
    - after scanner returns to S0, next dispatch consumes row
    """
    _print_title("TEST: blocked mobility pipeline then retry")

    _reset_runtime(scanner)
    _seed_not_idle(scanner)

    t0 = _ts(0)

    m4Commands._enqueue_script_or_csv_item(
        scanner=scanner,
        category="mobility",
        action="mobility.turn",
        execute_at=t0,
        args={"angle_deg": 10},
    )

    _register_test_experiment(
        t0=t0,
        last_execute_at=t0,
        item_count=1,
        scenario_name="blocked_then_retry.csv",
    )

    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 1, "Expected one scheduled mobility row")

    _dispatch_all_due_mobility_once()

    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 1, "Expected row kept while scanner non-idle")
    _assert(_stream_count(config.key_cmd_stream(scanner)) == 0, "Expected no robot command while scanner non-idle")

    _seed_idle_with_true(scanner)
    _dispatch_all_due_mobility_once()
    _show(scanner)

    _assert(_stream_count(config.KEY_MOBILITY_CMD_STREAM) == 0, "Expected row consumed after scanner becomes idle")
    _assert(_stream_count(config.key_cmd_stream(scanner)) >= 1, "Expected robot command after retry")

    state_now = config.r.hget(key_state(scanner), "state") or ""
    _assert(state_now == S1_WAITING_REPORT, "Expected S1 after retry dispatch")

    print("\nPASS")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
# ============================================================
# Editable test selection
# ============================================================

    SCANNER = "twin-scout-julia"
    tests = {
        "test_single_mobility_pipeline": test_single_mobility_pipeline,
        "test_mixed_mobility_and_traffic_pipeline": test_mixed_mobility_and_traffic_pipeline,
        "test_time_ordering_due_only": test_time_ordering_due_only,
        "test_blocked_mobility_pipeline_then_retry": test_blocked_mobility_pipeline_then_retry,
    }
    TEST_TO_RUN = "test_blocked_mobility_pipeline_then_retry"

    fn = tests.get(TEST_TO_RUN)
    if fn is None:
        raise ValueError(f"Unknown TEST_TO_RUN: {TEST_TO_RUN}")

    fn(SCANNER)
