
"""
Layer 1 tests for mobility state machine.

Usage:
- Edit TEST_TO_RUN below
- Run this file directly in VS Code

Notes:
- No argparse
- Redis-backed tests
- Focus on direct state-machine behavior, not HTTP
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

import config
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
    S7_STOPPED,
)
import utility
from m8mobility_state_store import _load_stop

# ============================================================
# Helpers
# ============================================================

def _print_title(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def _dump_hash(key: str) -> None:
    print(f"\n[{key}]")
    data = config.r.hgetall(key)
    if not data:
        print("(empty)")
        return
    for k in sorted(data.keys()):
        print(f"{k}: {data[k]}")


def _dump_stream(key: str, count: int = 10) -> None:
    print(f"\n[{key}]")
    rows = config.r.xrange(key, count=count)
    if not rows:
        print("(empty)")
        return
    for xid, fields in rows:
        print(f"- {xid}: {fields}")


def _delete_key_if_exists(key: str) -> None:
    try:
        config.r.delete(key)
    except Exception:
        pass


def _reset_one_scanner(scanner: str) -> None:
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
    for k in keys:
        _delete_key_if_exists(k)

    # global-ish keys used in these tests
    if hasattr(config, "KEY_MOBILITY_CMD_STREAM"):
        _delete_key_if_exists(config.KEY_MOBILITY_CMD_STREAM)
    if hasattr(config, "KEY_EXPERIMENT_REGISTRY"):
        _delete_key_if_exists(config.KEY_EXPERIMENT_REGISTRY)

    # clear experiment stop key if present
    try:
        _save_stop(False, "")
    except Exception:
        pass


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


def _seed_registry_and_meta(scanner: str) -> None:
    _require_real_whitelist(scanner)

    config.r.sadd(config.KEY_REGISTRY, scanner)
    config.r.hset(
        config.key_scanner_meta(scanner),
        mapping={
            "device_type": "robot",
            "last_seen": utility.local_ts(),
        },
    )


def _seed_true_location(scanner: str, x_m: float = 1.0, y_m: float = 1.0, heading_deg: float = 0.0) -> None:
    utility._hset_many(
        key_pose(scanner),
        {
            "true_location_json": {
                "location_ok": True,
                "x_m": float(x_m),
                "y_m": float(y_m),
                "heading_deg": float(heading_deg),
            }
        },
    )
    utility._hset_many(
        key_time(scanner),
        {
            "last_mobility_report_at": utility.local_ts(),
        },
    )


def _seed_idle_state(scanner: str) -> None:
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
            "last_planned_command_issued_at": "",
        },
    )


def _latest_cmd_rows(scanner: str) -> List[Dict[str, Any]]:
    rows = config.r.xrange(config.key_cmd_stream(scanner), count=20)
    out: List[Dict[str, Any]] = []
    for xid, fields in rows:
        item = {"cmd_id": xid, **fields}
        out.append(item)
    return out


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _make_matching_report(action: str, args: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "last_command": action,
        "last_command_args": args,
        "last_command_received_ts": 1770000000.0,
        "last_command_finished_ts": 1770000005.0,
        "last_exec_status": "completed",
        "last_error_code": "",
        "last_error_detail": "",
        "last_location_result": {
            "ok": True,
            "apriltag": {
                "ok": True,
                "count": 2,
                "tags": [
                    {"id": 3, "distance_m": 1.5, "angle_deg": -10.0, "yaw_deg": 2.0},
                    {"id": 4, "distance_m": 2.0, "angle_deg": 12.0, "yaw_deg": -1.0},
                ],
            },
        },
    }


def _store_report(scanner: str, report: Dict[str, Any]) -> None:
    utility._hset_many(
        key_report(scanner),
        {
            "last_mobility_report_json": report,
        },
    )
    utility._hset_many(
        key_time(scanner),
        {
            "last_mobility_report_at": utility.local_ts(),
        },
    )


def _show_core_keys(scanner: str) -> None:
    _dump_hash(key_state(scanner))
    _dump_hash(key_time(scanner))
    _dump_hash(key_report(scanner))
    _dump_hash(key_pose(scanner))
    _dump_stream(config.key_cmd_stream(scanner), count=10)


# ============================================================
# Tests
# ============================================================

def test_s0_requires_initial_true_location(scanner: str) -> None:
    _print_title("TEST: S0 rejects first command when initial true location is missing")

    _reset_one_scanner(scanner)
    _seed_registry_and_meta(scanner)
    _seed_idle_state(scanner)

    result = m8mobility.on_command_issued(
        scanner,
        "mobility.turn_move_turn.forward",
        {"distance_m": 1.0, "pre_angle": 0, "post_angle": 0},
    )

    print("\nResult:")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    _show_core_keys(scanner)

    _assert(result.get("state") == S7_STOPPED, "Expected S7 stop when initial true location is missing")
    _assert(result.get("status") == "stopped", "Expected stopped status")
    _assert("missing initial true location" in str(result.get("detail")), "Expected missing initial true location detail")

    print("\nPASS")


def test_s0_to_s1_happy_path(scanner: str) -> None:
    _print_title("TEST: S0 accepts first command and reaches S1 with one emitted robot command")

    _reset_one_scanner(scanner)
    _seed_registry_and_meta(scanner)
    _seed_idle_state(scanner)
    _seed_true_location(scanner, x_m=1.0, y_m=1.0, heading_deg=0.0)

    result = m8mobility.on_command_issued(
        scanner,
        "mobility.turn_move_turn.forward",
        {"distance_m": 1.0, "pre_angle": 0, "post_angle": 0},
    )

    print("\nResult:")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    _show_core_keys(scanner)

    state_hash = config.r.hgetall(key_state(scanner))
    time_hash = config.r.hgetall(key_time(scanner))
    cmd_rows = _latest_cmd_rows(scanner)

    _assert(state_hash.get("state") == S1_WAITING_REPORT, "Expected scanner to stop in S1 waiting for report")
    _assert(str(state_hash.get("outgoing_command_action", "")).strip() != "", "Expected outgoing command preview")
    _assert(str(time_hash.get("last_planned_command_issued_at", "")).strip() != "", "Expected issued timestamp")
    _assert(str(time_hash.get("s1_timer_token", "")).strip() != "", "Expected S1 timer token")
    _assert(len(cmd_rows) >= 1, "Expected at least one command in normal robot command stream")

    print("\nPASS")


def test_s1_accepts_matching_report(scanner: str) -> None:
    _print_title("TEST: S1 accepts matching report and leaves waiting state")

    _reset_one_scanner(scanner)
    _seed_registry_and_meta(scanner)
    _seed_idle_state(scanner)
    _seed_true_location(scanner, x_m=1.0, y_m=1.0, heading_deg=0.0)

    start_result = m8mobility.on_command_issued(
        scanner,
        "mobility.turn_move_turn.forward",
        {"distance_m": 1.0, "pre_angle": 0, "post_angle": 0},
    )
    print("\nStart result:")
    print(json.dumps(start_result, ensure_ascii=False, indent=2))

    pose_hash = config.r.hgetall(key_pose(scanner))
    expected_action = pose_hash.get("last_planned_command_action", "")
    expected_args_text = pose_hash.get("last_planned_command_args_json", "{}")
    try:
        expected_args = json.loads(expected_args_text) if isinstance(expected_args_text, str) else expected_args_text
    except Exception:
        expected_args = {}

    report = _make_matching_report(expected_action, expected_args)
    _store_report(scanner, report)

    result = m8mobility.on_report_received(scanner)

    print("\nReport result:")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    _show_core_keys(scanner)

    state_now = config.r.hget(key_state(scanner), "state") or ""

    _assert(str(config.r.hget(key_report(scanner), "last_mobility_report_json") or "").strip() != "", "Expected stored mobility report")
    _assert(config.r.hget(key_time(scanner), "s1_timer_token") in ("", None), "Expected S1 timer to be cleared on accepted report")
    _assert(state_now != "", "Expected state to remain valid after processing report")

    print("\nPASS (state after report = %s)" % state_now)


def test_s1_rejects_mismatched_report(scanner: str) -> None:
    _print_title("TEST: S1 ignores mismatched report and keeps waiting")

    _reset_one_scanner(scanner)
    _seed_registry_and_meta(scanner)
    _seed_idle_state(scanner)
    _seed_true_location(scanner, x_m=1.0, y_m=1.0, heading_deg=0.0)

    start_result = m8mobility.on_command_issued(
        scanner,
        "mobility.turn_move_turn.forward",
        {"distance_m": 1.0, "pre_angle": 0, "post_angle": 0},
    )
    print("\nStart result:")
    print(json.dumps(start_result, ensure_ascii=False, indent=2))

    bad_report = _make_matching_report(
        "mobility.turn",
        {"angle_deg": 90},
    )
    _store_report(scanner, bad_report)

    result = m8mobility.on_report_received(scanner)

    print("\nReport result:")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    _show_core_keys(scanner)

    state_now = config.r.hget(key_state(scanner), "state") or ""
    timer_token = config.r.hget(key_time(scanner), "s1_timer_token") or ""

    _assert(state_now == S1_WAITING_REPORT, "Expected state to remain S1 on mismatched report")
    _assert(str(timer_token).strip() != "", "Expected S1 timer to remain active on mismatched report")

    print("\nPASS")


def test_s1_timeout_to_s4(scanner: str) -> None:
    _print_title("TEST: S1 timeout escalates to S4 retry path")

    _reset_one_scanner(scanner)
    _seed_registry_and_meta(scanner)
    _seed_idle_state(scanner)
    _seed_true_location(scanner, x_m=1.0, y_m=1.0, heading_deg=0.0)

    start_result = m8mobility.on_command_issued(
        scanner,
        "mobility.turn_move_turn.forward",
        {"distance_m": 1.0, "pre_angle": 0, "post_angle": 0},
    )
    print("\nStart result:")
    print(json.dumps(start_result, ensure_ascii=False, indent=2))

    timer_token = config.r.hget(key_time(scanner), "s1_timer_token") or ""
    _assert(str(timer_token).strip() != "", "Expected S1 timer token before timeout test")

    from m8mobility_state import process_s1_event
    result = process_s1_event(scanner, source="timeout", timer_token=timer_token)

    print("\nTimeout result:")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    _show_core_keys(scanner)

    state_now = config.r.hget(key_state(scanner), "state") or ""
    busy_count = config.r.hget(key_state(scanner), "busy_count") or ""

    _assert(state_now == S1_WAITING_REPORT, "Expected machine to re-enter S1 after S4 issues location report")
    _assert(str(busy_count) == "1", "Expected busy_count to increase on missing-report timeout")

    cmd_rows = _latest_cmd_rows(scanner)
    _assert(len(cmd_rows) >= 2, "Expected follow-up mobility.location.report to be issued after timeout")
    _assert(
        any(row.get("action") == "mobility.location.report" for row in cmd_rows),
        "Expected mobility.location.report in command stream",
    )

    print("\nPASS")


def test_s2_immediate_stop(scanner: str) -> None:
    _print_title("TEST: S2 immediate-stop error enters S7")

    _reset_one_scanner(scanner)
    _seed_registry_and_meta(scanner)
    _seed_idle_state(scanner)
    _seed_true_location(scanner, x_m=1.0, y_m=1.0, heading_deg=0.0)

    start_result = m8mobility.on_command_issued(
        scanner,
        "mobility.turn_move_turn.forward",
        {"distance_m": 1.0, "pre_angle": 0, "post_angle": 0},
    )
    print("\nStart result:")
    print(json.dumps(start_result, ensure_ascii=False, indent=2))

    pose_hash = config.r.hgetall(key_pose(scanner))
    expected_action = pose_hash.get("last_planned_command_action", "")
    expected_args_text = pose_hash.get("last_planned_command_args_json", "{}")
    try:
        expected_args = json.loads(expected_args_text) if isinstance(expected_args_text, str) else expected_args_text
    except Exception:
        expected_args = {}

    report = _make_matching_report(expected_action, expected_args)
    report["last_exec_status"] = "failed"
    report["last_error_code"] = "TOF_SENSOR_FAIL"
    report["last_error_detail"] = "distance sensor failure"

    _store_report(scanner, report)

    result = m8mobility.on_report_received(scanner)

    print("\nReport result:")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    _show_core_keys(scanner)

    state_now = config.r.hget(key_state(scanner), "state") or ""
    stop_state = _load_stop()

    _assert(state_now == S7_STOPPED, "Expected immediate-stop error to enter S7")
    _assert(bool(stop_state.get("stop")) is True, "Expected global stop key to be asserted")

    print("\nPASS")


def test_s2_no_tag_visible(scanner: str) -> None:
    _print_title("TEST: S2 NO_TAG_VISIBLE goes through S3 normal path")

    _reset_one_scanner(scanner)
    _seed_registry_and_meta(scanner)
    _seed_idle_state(scanner)
    _seed_true_location(scanner, x_m=1.0, y_m=1.0, heading_deg=0.0)

    start_result = m8mobility.on_command_issued(
        scanner,
        "mobility.turn_move_turn.forward",
        {"distance_m": 1.0, "pre_angle": 0, "post_angle": 0},
    )
    print("\nStart result:")
    print(json.dumps(start_result, ensure_ascii=False, indent=2))

    pose_hash = config.r.hgetall(key_pose(scanner))
    expected_action = pose_hash.get("last_planned_command_action", "")
    expected_args_text = pose_hash.get("last_planned_command_args_json", "{}")
    try:
        expected_args = json.loads(expected_args_text) if isinstance(expected_args_text, str) else expected_args_text
    except Exception:
        expected_args = {}

    report = _make_matching_report(expected_action, expected_args)
    report["last_error_code"] = "NO_TAG_VISIBLE"
    report["last_location_result"] = {
        "ok": True,
        "apriltag": {
            "ok": False,
            "count": 0,
            "tags": [],
        },
    }

    _store_report(scanner, report)

    result = m8mobility.on_report_received(scanner)

    print("\nReport result:")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    _show_core_keys(scanner)

    state_now = config.r.hget(key_state(scanner), "state") or ""

    _assert(
        state_now in (S0_IDLE, S1_WAITING_REPORT),
        "Expected NO_TAG_VISIBLE to continue through normal policy path, not stop",
    )

    print("\nPASS (state after report = %s)" % state_now)


def test_s2_mobility_busy(scanner: str) -> None:
    _print_title("TEST: S2 MOBILITY_BUSY starts busy-retry timer path")

    _reset_one_scanner(scanner)
    _seed_registry_and_meta(scanner)
    _seed_idle_state(scanner)
    _seed_true_location(scanner, x_m=1.0, y_m=1.0, heading_deg=0.0)

    start_result = m8mobility.on_command_issued(
        scanner,
        "mobility.turn_move_turn.forward",
        {"distance_m": 1.0, "pre_angle": 0, "post_angle": 0},
    )
    print("\nStart result:")
    print(json.dumps(start_result, ensure_ascii=False, indent=2))

    pose_hash = config.r.hgetall(key_pose(scanner))
    expected_action = pose_hash.get("last_planned_command_action", "")
    expected_args_text = pose_hash.get("last_planned_command_args_json", "{}")
    try:
        expected_args = json.loads(expected_args_text) if isinstance(expected_args_text, str) else expected_args_text
    except Exception:
        expected_args = {}

    report = _make_matching_report(expected_action, expected_args)
    report["last_exec_status"] = "failed"
    report["last_error_code"] = "MOBILITY_BUSY"
    report["last_error_detail"] = "robot still executing previous command"
    report["last_location_result"] = {
        "ok": False,
        "apriltag": {
            "ok": False,
            "count": 0,
            "tags": [],
        },
    }

    _store_report(scanner, report)

    result = m8mobility.on_report_received(scanner)

    print("\nReport result:")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    _show_core_keys(scanner)

    state_now = config.r.hget(key_state(scanner), "state") or ""
    busy_count = config.r.hget(key_state(scanner), "busy_count") or ""
    entry_reason = config.r.hget(key_state(scanner), "s2_entry_reason") or ""
    busy_retry_token = config.r.hget(key_time(scanner), "busy_retry_token") or ""

    _assert(state_now == "s2evaluating_policy", "Expected busy path to park in S2 waiting for retry timer")
    _assert(str(busy_count) == "1", "Expected busy_count to increase on MOBILITY_BUSY")
    _assert(str(entry_reason) == "busy_retry_timer", "Expected S2 busy-retry marker to be set")
    _assert(str(busy_retry_token).strip() != "", "Expected busy-retry timer token to be created")

    print("\nPASS")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    tests = {
        "test_s0_requires_initial_true_location": test_s0_requires_initial_true_location,
        "test_s0_to_s1_happy_path": test_s0_to_s1_happy_path,
        "test_s1_accepts_matching_report": test_s1_accepts_matching_report,
        "test_s1_rejects_mismatched_report": test_s1_rejects_mismatched_report,
        "test_s1_timeout_to_s4": test_s1_timeout_to_s4,
        "test_s2_immediate_stop": test_s2_immediate_stop,
        "test_s2_no_tag_visible": test_s2_no_tag_visible,
        "test_s2_mobility_busy": test_s2_mobility_busy,
    }

    # ============================================================
    # Editable test selection
    # ============================================================
    SCANNER = "twin-scout-bravo"
    TEST_TO_RUN = "test_s2_mobility_busy"

    fn = tests.get(TEST_TO_RUN)
    if fn is None:
        raise ValueError(f"Unknown TEST_TO_RUN: {TEST_TO_RUN}")

    fn(SCANNER)
