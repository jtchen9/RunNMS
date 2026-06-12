from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

# ============================================================
# Allow import from NMS project root
# ============================================================

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(str(PROJECT_ROOT))

import config
import utility
import m8mobility

from m8mobility_state import (
    S1_WAITING_REPORT,
    S7_STOPPED,
    UNEXPECTED_EVENT_SUM_LIMIT,
)
from m8mobility_state_store import (
    key_state,
    key_time,
    key_report,
    key_pose,
    _set_state,
    _save_stop,
)


# ============================================================
# Test settings
# ============================================================

SCANNER = os.environ.get("SCANNER", "twin-scout-charlie")
TEST_ID = "I2"
TEST_NAME = "turn_exec_fail_counter_limit_direct"

ACTION = "mobility.turn_move_turn.forward"
ARGS = {
    "pre_angle": 0.0,
    "distance_m": 0.2,
    "post_angle": 0.0,
}

EXPECTED_ERROR_CODE = "TURN_EXEC_FAIL"
OUT_DIR = THIS_FILE.parent / "testSM"

# The test validates the limit transition itself:
#   old exec_fail_count = UNEXPECTED_EVENT_SUM_LIMIT - 1
#   next TURN_EXEC_FAIL increments it to UNEXPECTED_EVENT_SUM_LIMIT
#   state must stop.
SEEDED_EXEC_FAIL_COUNT = UNEXPECTED_EVENT_SUM_LIMIT - 1


# ============================================================
# Evidence recorder
# ============================================================

class Evidence:
    def __init__(self) -> None:
        self.started_at = utility.local_ts()
        self.data: Dict[str, Any] = {
            "test_id": TEST_ID,
            "test_name": TEST_NAME,
            "scanner": SCANNER,
            "started_at": self.started_at,
            "ended_at": "",
            "pass": False,
            "failure_reason": "",
            "config": {
                "ACTION": ACTION,
                "ARGS": ARGS,
                "expected_error_code": EXPECTED_ERROR_CODE,
                "unexpected_event_sum_limit": UNEXPECTED_EVENT_SUM_LIMIT,
                "seeded_exec_fail_count": SEEDED_EXEC_FAIL_COUNT,
            },
            "events": [],
            "snapshots": {},
        }

    def event(self, phase: str, detail: str, extra: Any = None) -> None:
        item = {
            "ts": utility.local_ts(),
            "phase": phase,
            "detail": detail,
        }
        if extra is not None:
            item["extra"] = extra
        self.data["events"].append(item)

    def snapshot(self, name: str, snap: Dict[str, Any]) -> None:
        self.data["snapshots"][name] = snap

    def finish(self, passed: bool, reason: str = "") -> Path:
        self.data["ended_at"] = utility.local_ts()
        self.data["pass"] = bool(passed)
        self.data["failure_reason"] = reason

        OUT_DIR.mkdir(parents=True, exist_ok=True)
        ts = self.data["ended_at"].replace("-", "").replace(":", "")
        out_path = OUT_DIR / f"{TEST_ID}_{TEST_NAME}_{SCANNER}_{ts}.json"
        out_path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")
        return out_path


EV = Evidence()


# ============================================================
# Redis helpers
# ============================================================

def hgetall(hash_key: str) -> Dict[str, Any]:
    d = config.r.hgetall(hash_key)
    return d if isinstance(d, dict) else {}


def hset(hash_key: str, mapping: Dict[str, Any]) -> None:
    fixed: Dict[str, str] = {}
    for k, v in mapping.items():
        if isinstance(v, (dict, list)):
            fixed[k] = json.dumps(v, ensure_ascii=False)
        else:
            fixed[k] = str(v)
    config.r.hset(hash_key, mapping=fixed)


def parse_json_maybe(text: str) -> Any:
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return text


def load_json_field(hash_key: str, field: str) -> Dict[str, Any]:
    raw = config.r.hget(hash_key, field) or ""
    obj = parse_json_maybe(str(raw))
    return obj if isinstance(obj, dict) else {}


def cmd_key(scanner: str) -> str:
    return config.key_cmd_stream(scanner)


def clear_cmd_stream(reason: str) -> None:
    k = cmd_key(SCANNER)
    old_len = int(config.r.xlen(k))
    config.r.delete(k)
    EV.event(reason, f"cleared {k}, old_len={old_len}")


def latest_report() -> Dict[str, Any]:
    return load_json_field(key_report(SCANNER), "last_mobility_report_json")


def newest_stream_actions(limit: int = 10) -> List[str]:
    rows = config.r.xrevrange(cmd_key(SCANNER), "+", "-", count=limit)
    actions: List[str] = []
    for _xid, fields in rows:
        action = fields.get("action", "")
        if action:
            actions.append(str(action))
    return actions


def compact_snapshot() -> Dict[str, Any]:
    st = hgetall(key_state(SCANNER))
    tm = hgetall(key_time(SCANNER))
    pose = hgetall(key_pose(SCANNER))
    rep = latest_report()

    return {
        "ts": utility.local_ts(),
        "state": st.get("state", ""),
        "state_detail": st.get("state_detail", ""),
        "mobility_ready": st.get("mobility_ready", ""),
        "robot_safety_state": st.get("robot_safety_state", ""),
        "stop_experiment": st.get("stop_experiment", ""),
        "stop_reason": st.get("stop_reason", ""),
        "need_location_retry": st.get("need_location_retry", ""),
        "retry_count": st.get("retry_count", ""),
        "busy_count": st.get("busy_count", ""),
        "collision_veto_count": st.get("collision_veto_count", ""),
        "exec_fail_count": st.get("exec_fail_count", ""),
        "last_error_code": st.get("last_error_code", ""),
        "last_error_detail": st.get("last_error_detail", ""),
        "outgoing_command_action": st.get("outgoing_command_action", ""),
        "outgoing_command_args_json": st.get("outgoing_command_args_json", ""),
        "outgoing_command_source": st.get("outgoing_command_source", ""),
        "s1_timer_token": tm.get("s1_timer_token", ""),
        "busy_retry_token": tm.get("busy_retry_token", ""),
        "last_planned_command_issued_at": tm.get("last_planned_command_issued_at", ""),
        "last_mobility_report_at": tm.get("last_mobility_report_at", ""),
        "last_planned_command_action": pose.get("last_planned_command_action", ""),
        "last_planned_command_args_json": pose.get("last_planned_command_args_json", ""),
        "last_report_command": rep.get("last_command", ""),
        "last_report_args": rep.get("last_command_args", {}),
        "last_report_status": rep.get("last_exec_status", ""),
        "last_report_error_code": rep.get("last_error_code", ""),
        "last_report_error_detail": rep.get("last_error_detail", ""),
        "cmd_stream_len": int(config.r.xlen(cmd_key(SCANNER))),
        "newest_actions": newest_stream_actions(limit=10),
    }


def full_snapshot() -> Dict[str, Any]:
    return {
        "ts": utility.local_ts(),
        "state": hgetall(key_state(SCANNER)),
        "time": hgetall(key_time(SCANNER)),
        "pose": hgetall(key_pose(SCANNER)),
        "report": latest_report(),
        "cmd_stream": {
            "key": cmd_key(SCANNER),
            "len": int(config.r.xlen(cmd_key(SCANNER))),
            "newest_actions": newest_stream_actions(limit=10),
        },
    }


def print_compact(label: str) -> None:
    snap = compact_snapshot()
    print(f"\n{label}:")
    print(json.dumps(snap, ensure_ascii=False, indent=2))
    EV.snapshot(label, full_snapshot())


def fail(msg: str) -> None:
    print("\nFAIL:")
    print(msg)
    out = EV.finish(False, msg)
    print(f"\nEvidence saved to: {out}")
    raise SystemExit(1)


# ============================================================
# Test setup
# ============================================================

def clear_intercept_keys() -> None:
    # Defensive cleanup: this deterministic test does not use report intercept.
    keys = [
        f"{config.KEY_PREFIX}debug:mobility_report_intercept:{SCANNER}",
        f"{config.KEY_PREFIX}debug:mobility_report_intercept:enabled:{SCANNER}",
        f"{config.KEY_PREFIX}debug:mobility_report_intercept:enabled",
    ]
    deleted = 0
    for k in keys:
        deleted += int(config.r.delete(k) or 0)
    EV.event("clear_intercept_keys", "cleared mobility report intercept keys", {"deleted": deleted, "keys": keys})


def seed_s1_waiting_for_failed_forward_report() -> Dict[str, Any]:
    """
    Seed NMS as if one TURN_EXEC_FAIL has already been counted and NMS is
    currently waiting for the next forward command report.

    Then inject the next matching TURN_EXEC_FAIL report. This should hit
    UNEXPECTED_EVENT_SUM_LIMIT and stop.
    """
    now = utility.local_ts()

    _save_stop(False, "")
    clear_cmd_stream("RESET CMD STREAM")
    clear_intercept_keys()

    hset(
        key_state(SCANNER),
        {
            "state": S1_WAITING_REPORT,
            "state_detail": "testSM I2 seeded S1: exec_fail_count at limit-1",
            "mobility_ready": "true",
            "robot_safety_state": "NORMAL",
            "stop_experiment": "false",
            "stop_reason": "",
            "need_location_retry": "false",
            "retry_count": "0",
            "busy_count": "0",
            "collision_veto_count": "0",
            "exec_fail_count": str(SEEDED_EXEC_FAIL_COUNT),
            "last_error_code": "",
            "last_error_detail": "",
            "outgoing_command_action": ACTION,
            "outgoing_command_args_json": json.dumps(ARGS, ensure_ascii=False),
            "outgoing_command_source": "testSM.F2.seeded_expected_command",
            "outgoing_command_updated_at": now,
            "s2_entry_reason": "",
        },
    )

    hset(
        key_pose(SCANNER),
        {
            "last_planned_command_action": ACTION,
            "last_planned_command_args_json": json.dumps(ARGS, ensure_ascii=False),
        },
    )

    hset(
        key_time(SCANNER),
        {
            "last_planned_command_issued_at": now,
            "last_mobility_report_at": now,
            "s1_timer_token": "testSM-I2-no-real-timer",
            "s1_timer_started_at": now,
            "busy_retry_token": "",
            "busy_retry_started_at": "",
        },
    )

    report = {
        "last_command": ACTION,
        "last_command_args": ARGS,
        "last_command_received_ts": time.time(),
        "last_command_finished_ts": time.time(),
        "last_exec_status": "failed",
        "last_error_code": EXPECTED_ERROR_CODE,
        "last_error_detail": "testSM injected TURN_EXEC_FAIL to hit exec_fail counter limit",
        "last_location_result": {
            "ok": False,
            "error": "testSM injected TURN_EXEC_FAIL to hit exec_fail counter limit",
        },
    }

    hset(
        key_report(SCANNER),
        {
            "last_mobility_report_json": report,
        },
    )

    EV.event(
        "seed",
        "seeded S1 with exec_fail_count=limit-1 and injected matching TURN_EXEC_FAIL report",
        {
            "now": now,
            "state": S1_WAITING_REPORT,
            "seeded_exec_fail_count": SEEDED_EXEC_FAIL_COUNT,
            "expected_final_exec_fail_count": UNEXPECTED_EVENT_SUM_LIMIT,
            "report": report,
        },
    )

    return report


# ============================================================
# Verification
# ============================================================

def verify_final(result: Dict[str, Any]) -> None:
    final = compact_snapshot()
    EV.snapshot("FINAL", full_snapshot())

    errors: List[str] = []

    if final["state"] != S7_STOPPED:
        errors.append(f"state should be {S7_STOPPED}, got {final['state']!r}")

    if final["robot_safety_state"] != "UNSAFE_STOP":
        errors.append(f"robot_safety_state should be UNSAFE_STOP, got {final['robot_safety_state']!r}")

    if final["stop_experiment"] != "true":
        errors.append(f"stop_experiment should be true, got {final['stop_experiment']!r}")

    stop_reason = str(final["stop_reason"] or "")
    if not (
        "UNEXPECTED_EVENT_SUM_LIMIT" in stop_reason
        or (EXPECTED_ERROR_CODE in stop_reason and f"sum={UNEXPECTED_EVENT_SUM_LIMIT}" in stop_reason)
    ):
        errors.append(
            f"stop_reason should mention {EXPECTED_ERROR_CODE} counter limit, got {final['stop_reason']!r}"
        )

    if final["exec_fail_count"] != str(UNEXPECTED_EVENT_SUM_LIMIT):
        errors.append(
            f"exec_fail_count should be {UNEXPECTED_EVENT_SUM_LIMIT}, got {final['exec_fail_count']!r}"
        )

    if final["busy_count"] != "0":
        errors.append(f"busy_count should stay 0, got {final['busy_count']!r}")

    if final["collision_veto_count"] != "0":
        errors.append(f"collision_veto_count should stay 0, got {final['collision_veto_count']!r}")

    if final["need_location_retry"] != "false":
        errors.append(f"need_location_retry should be false after stop cleanup, got {final['need_location_retry']!r}")

    if final["outgoing_command_action"]:
        errors.append(f"outgoing_command_action should be cleared, got {final['outgoing_command_action']!r}")

    if final["cmd_stream_len"] != 0:
        errors.append(
            f"command stream should be empty, got len={final['cmd_stream_len']}, actions={final['newest_actions']}"
        )

    if final["last_report_command"] != ACTION:
        errors.append(f"last_report_command should be {ACTION}, got {final['last_report_command']!r}")

    if final["last_report_error_code"] != EXPECTED_ERROR_CODE:
        errors.append(
            f"last_report_error_code should be {EXPECTED_ERROR_CODE}, got {final['last_report_error_code']!r}"
        )

    if not isinstance(result, dict):
        errors.append(f"on_report_received result should be dict, got {type(result).__name__}")
    else:
        if result.get("state") != S7_STOPPED:
            errors.append(f"on_report_received result.state should be {S7_STOPPED}, got {result.get('state')!r}")
        if result.get("status") != "stopped":
            errors.append(f"on_report_received result.status should be stopped, got {result.get('status')!r}")

    if errors:
        fail("I2 exec_fail counter-limit verification failed:\n- " + "\n- ".join(errors))

    print("\nPASS:")
    print(
        f"{SCANNER} completed I2 deterministic counter-limit test: "
        f"seeded exec_fail_count={SEEDED_EXEC_FAIL_COUNT}, injected {EXPECTED_ERROR_CODE}, "
        f"and NMS stopped at UNEXPECTED_EVENT_SUM_LIMIT={UNEXPECTED_EVENT_SUM_LIMIT}."
    )
    print("\nSTATE MACHINE RESULT:")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("\nFINAL:")
    print(json.dumps(final, ensure_ascii=False, indent=2))

    out = EV.finish(True, "")
    print(f"Evidence saved to: {out}")


# ============================================================
# Main
# ============================================================

def run() -> None:
    print("=" * 72)
    print(f"{TEST_ID}: {TEST_NAME}")
    print("=" * 72)
    print(
        "This deterministic test does not move the robot.\n"
        "It seeds exec_fail_count at limit-1, injects one matching TURN_EXEC_FAIL report,\n"
        "then calls m8mobility.on_report_received() to verify the S2 counter-limit branch."
    )

    EV.event("start", "test started")
    print_compact("PRE")

    seed_s1_waiting_for_failed_forward_report()
    print_compact("SEEDED")

    result = m8mobility.on_report_received(SCANNER)
    EV.event("on_report_received", "called m8mobility.on_report_received", result)

    verify_final(result)


if __name__ == "__main__":
    run()
