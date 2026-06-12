from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

# ============================================================
# Import from NMS project root
# ============================================================

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(str(PROJECT_ROOT))

import config
import utility

from m8mobility_state import (
    S0_IDLE,
    S5_COMPUTING_CORRECTION,
    CORRECTION_ATTEMPT_LIMIT,
    s5computing_correction,
)
from m8mobility_state_store import key_state, key_time, key_pose
from m8mobility_pose import _apply_mobility_command_to_pose

# ============================================================
# Settings
# ============================================================

SCANNER = os.environ.get("SCANNER", "twin-scout-charlie")

TEST_ID = "H3"
TEST_NAME = "correction_limit_no_endless_correction"

ACTION = "mobility.turn_move_turn.forward"

# A synthetic planned target far enough from true location that S5 would
# normally compute a correction if correction_attempt_count were below limit.
SYNTHETIC_RESIDUAL_ARGS = {
    "pre_angle": 45.0,
    "distance_m": 0.5,
    "post_angle": -45.0,
}

OUT_DIR = THIS_FILE.parent / "testSM"


# ============================================================
# Redis helpers
# ============================================================

def hgetall(hash_key: str) -> Dict[str, Any]:
    d = config.r.hgetall(hash_key)
    return dict(d) if isinstance(d, dict) else {}


def hset(hash_key: str, mapping: Dict[str, Any]) -> None:
    fixed: Dict[str, str] = {}
    for k, v in mapping.items():
        if isinstance(v, (dict, list)):
            fixed[k] = json.dumps(v, ensure_ascii=False)
        else:
            fixed[k] = str(v)
    config.r.hset(hash_key, mapping=fixed)


def delete_fields(hash_key: str, fields: List[str]) -> None:
    if fields:
        config.r.hdel(hash_key, *fields)


def restore_hash(hash_key: str, before: Dict[str, Any]) -> None:
    current = hgetall(hash_key)
    current_fields = set(str(k) for k in current.keys())
    before_fields = set(str(k) for k in before.keys())

    extra = sorted(current_fields - before_fields)
    delete_fields(hash_key, extra)

    if before:
        config.r.hset(hash_key, mapping={str(k): str(v) for k, v in before.items()})


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
        return text


def load_json_field(hash_key: str, field: str) -> Dict[str, Any]:
    obj = parse_json_maybe(hget(hash_key, field))
    return obj if isinstance(obj, dict) else {}


def pose_ok(p: Dict[str, Any]) -> bool:
    return (
        isinstance(p, dict)
        and p.get("location_ok", True) is not False
        and p.get("x_m") is not None
        and p.get("y_m") is not None
        and p.get("heading_deg") is not None
    )


def pose_minimal(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "location_ok": True,
        "x_m": float(p["x_m"]),
        "y_m": float(p["y_m"]),
        "heading_deg": float(p["heading_deg"]),
        "source": "testSM.H3.seed",
        "updated_at": utility.local_ts(),
    }


def cmd_stream_key(scanner: str) -> str:
    return config.key_cmd_stream(scanner)


def cmd_stream_len(scanner: str) -> int:
    try:
        return int(config.r.xlen(cmd_stream_key(scanner)))
    except Exception:
        return -1


def clear_cmd_stream(scanner: str) -> int:
    key = cmd_stream_key(scanner)
    old_len = cmd_stream_len(scanner)
    try:
        config.r.delete(key)
    except Exception:
        pass
    return old_len


def snapshot(scanner: str) -> Dict[str, Any]:
    s_key = key_state(scanner)
    t_key = key_time(scanner)
    p_key = key_pose(scanner)

    state = hgetall(s_key)
    time = hgetall(t_key)
    pose = hgetall(p_key)

    out = {
        "ts": utility.local_ts(),
        "state": {
            "state": state.get("state", ""),
            "state_detail": state.get("state_detail", ""),
            "mobility_ready": state.get("mobility_ready", ""),
            "robot_safety_state": state.get("robot_safety_state", ""),
            "stop_experiment": state.get("stop_experiment", ""),
            "stop_reason": state.get("stop_reason", ""),
            "need_location_retry": state.get("need_location_retry", ""),
            "retry_count": state.get("retry_count", ""),
            "busy_count": state.get("busy_count", ""),
            "collision_veto_count": state.get("collision_veto_count", ""),
            "exec_fail_count": state.get("exec_fail_count", ""),
            "correction_attempt_count": state.get("correction_attempt_count", ""),
            "outgoing_command_action": state.get("outgoing_command_action", ""),
            "outgoing_command_args_json": state.get("outgoing_command_args_json", ""),
            "outgoing_command_source": state.get("outgoing_command_source", ""),
            "pending_sequence_len": state.get("pending_sequence_len", ""),
            "pending_sequence_json": state.get("pending_sequence_json", ""),
            "last_error_code": state.get("last_error_code", ""),
            "last_error_detail": state.get("last_error_detail", ""),
            "last_correction_issued_at": state.get("last_correction_issued_at", ""),
        },
        "time": {
            "policy_updated_at": time.get("policy_updated_at", ""),
            "last_planned_command_issued_at": time.get("last_planned_command_issued_at", ""),
            "s1_timer_token": time.get("s1_timer_token", ""),
            "busy_retry_token": time.get("busy_retry_token", ""),
        },
        "pose": {
            "true_location_json": parse_json_maybe(pose.get("true_location_json", "")),
            "planned_location_json": parse_json_maybe(pose.get("planned_location_json", "")),
            "last_planned_command_action": pose.get("last_planned_command_action", ""),
            "last_planned_command_args_json": pose.get("last_planned_command_args_json", ""),
        },
        "cmd_stream": {
            "key": cmd_stream_key(scanner),
            "len": cmd_stream_len(scanner),
        },
    }
    return out


# ============================================================
# Evidence
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
                "synthetic_residual_args": SYNTHETIC_RESIDUAL_ARGS,
                "correction_attempt_limit": CORRECTION_ATTEMPT_LIMIT,
                "mode": "no movement; seed S5 at correction limit; verify no command is issued",
            },
            "events": [],
            "snapshots": {},
            "checks": {},
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

    def snap(self, name: str, data: Any) -> None:
        self.data["snapshots"][name] = data

    def check(self, name: str, data: Any) -> None:
        self.data["checks"][name] = data

    def finish(self, passed: bool, reason: str = "") -> Path:
        self.data["ended_at"] = utility.local_ts()
        self.data["pass"] = bool(passed)
        self.data["failure_reason"] = reason
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        ts = self.data["ended_at"].replace("-", "").replace(":", "")
        out = OUT_DIR / f"{TEST_ID}_{TEST_NAME}_{SCANNER}_{ts}.json"
        out.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")
        return out


EV = Evidence()


# ============================================================
# Test body
# ============================================================

def main() -> None:
    print("=" * 72)
    print("H3: correction_limit_no_endless_correction")
    print("=" * 72)
    print("This test does not move the robot.")
    print("It seeds a remaining true/planned error but sets correction_attempt_count")
    print(f"to the production limit ({CORRECTION_ATTEMPT_LIMIT}).")
    print("Expected: S5 returns to s0idle and issues no command.")
    print()

    s_key = key_state(SCANNER)
    t_key = key_time(SCANNER)
    p_key = key_pose(SCANNER)

    before_state = hgetall(s_key)
    before_time = hgetall(t_key)
    before_pose = hgetall(p_key)
    before_cmd_len = cmd_stream_len(SCANNER)

    EV.snap("PRE", snapshot(SCANNER))

    base_true = load_json_field(p_key, "true_location_json")
    if not pose_ok(base_true):
        base_true = load_json_field(p_key, "planned_location_json")

    if not pose_ok(base_true):
        reason = f"No valid true/planned pose available for {SCANNER}; cannot seed H3."
        path = EV.finish(False, reason)
        print("FAIL:")
        print(reason)
        print("Evidence saved to:", path)
        return

    true_loc = pose_minimal(base_true)
    planned_loc = _apply_mobility_command_to_pose(
        true_loc,
        ACTION,
        SYNTHETIC_RESIDUAL_ARGS,
    )
    planned_loc["source"] = "testSM.H3.synthetic_planned_target"
    planned_loc["updated_at"] = utility.local_ts()

    old_cmd_len = clear_cmd_stream(SCANNER)
    EV.event("clear_cmd_stream", "cleared command stream before no-endless-correction test", {
        "old_len": old_cmd_len,
        "key": cmd_stream_key(SCANNER),
    })

    now = utility.local_ts()

    hset(p_key, {
        "true_location_json": true_loc,
        "planned_location_json": planned_loc,
        "last_planned_command_action": ACTION,
        "last_planned_command_args_json": SYNTHETIC_RESIDUAL_ARGS,
    })

    hset(s_key, {
        "state": S5_COMPUTING_CORRECTION,
        "state_detail": "testSM H3 seeded S5 at correction limit",
        "mobility_ready": "true",
        "robot_safety_state": "NORMAL",
        "stop_experiment": "false",
        "stop_reason": "",
        "need_location_retry": "false",
        "retry_count": "0",
        "busy_count": "0",
        "collision_veto_count": "0",
        "exec_fail_count": "0",
        "correction_attempt_count": str(CORRECTION_ATTEMPT_LIMIT),
        "pending_sequence_json": "",
        "pending_sequence_len": "0",
        "pending_sequence_reason": "",
        "outgoing_command_action": "",
        "outgoing_command_args_json": "",
        "outgoing_command_source": "",
        "outgoing_command_updated_at": "",
        "last_error_code": "",
        "last_error_detail": "",
    })

    hset(t_key, {
        "policy_updated_at": now,
        "s1_timer_token": "",
        "s1_timer_started_at": "",
        "busy_retry_token": "",
        "busy_retry_started_at": "",
    })

    EV.event("seed", "seeded remaining error with correction_attempt_count at limit", {
        "true_location": true_loc,
        "planned_location": planned_loc,
        "correction_attempt_count": CORRECTION_ATTEMPT_LIMIT,
    })

    EV.snap("SEEDED", snapshot(SCANNER))

    result = None
    final_before_restore = None
    errors: List[str] = []

    try:
        result = s5computing_correction(SCANNER)
        EV.event("s5computing_correction", "called production S5 wrapper", result)
        final_before_restore = snapshot(SCANNER)
        EV.snap("FINAL_BEFORE_RESTORE", final_before_restore)

        st = final_before_restore["state"]

        check = {
            "result": result,
            "final_state": st,
            "expected": {
                "state": S0_IDLE,
                "state_detail_contains": f"correction limit reached ({CORRECTION_ATTEMPT_LIMIT})",
                "cmd_stream_len": 0,
                "outgoing_command_action": "",
                "pending_sequence_len": "0",
            },
        }

        if result.get("transition_to") != S0_IDLE:
            errors.append(f"S5 should transition to {S0_IDLE}, got {result.get('transition_to')!r}")

        if st["state"] != S0_IDLE:
            errors.append(f"state should be {S0_IDLE}, got {st['state']!r}")

        if f"correction limit reached ({CORRECTION_ATTEMPT_LIMIT})" not in str(st["state_detail"]):
            errors.append(
                f"state_detail should contain correction limit reached ({CORRECTION_ATTEMPT_LIMIT}), got {st['state_detail']!r}"
            )

        if st["outgoing_command_action"]:
            errors.append(f"outgoing_command_action should be empty, got {st['outgoing_command_action']!r}")

        if st["outgoing_command_args_json"]:
            errors.append(f"outgoing_command_args_json should be empty, got {st['outgoing_command_args_json']!r}")

        if st["pending_sequence_len"] not in ("", "0"):
            errors.append(f"pending_sequence_len should be 0/empty, got {st['pending_sequence_len']!r}")

        if final_before_restore["cmd_stream"]["len"] != 0:
            errors.append(f"cmd_stream_len should be 0, got {final_before_restore['cmd_stream']['len']!r}")

        if st["stop_experiment"] == "true":
            errors.append("stop_experiment should not become true for correction limit; expected graceful s0idle")

        check["ok"] = not errors
        EV.check("H3_no_endless_correction_after_limit", check)

    finally:
        restore_hash(s_key, before_state)
        restore_hash(t_key, before_time)
        restore_hash(p_key, before_pose)

        # Do not restore old command stream contents; previous tests also clear it.
        # The important assertion is that this test did not add a command.
        EV.event("restore", "restored state/time/pose hashes after H3; command stream left empty by design", {
            "before_cmd_len": before_cmd_len,
            "after_cmd_len": cmd_stream_len(SCANNER),
        })

    if errors:
        reason = "H3 no-endless-correction verification failed:\n- " + "\n- ".join(errors)
        path = EV.finish(False, reason)
        print("FAIL:")
        print(reason)
        print("Evidence saved to:", path)
        return

    path = EV.finish(True, "")
    print("PASS:")
    print(f"{SCANNER} completed H3: correction limit reached and no further command was issued.")
    print()
    print("STATE MACHINE RESULT:")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print()
    print("FINAL BEFORE RESTORE:")
    print(json.dumps(final_before_restore["state"], ensure_ascii=False, indent=2))
    print()
    print("Evidence saved to:", path)


if __name__ == "__main__":
    main()
