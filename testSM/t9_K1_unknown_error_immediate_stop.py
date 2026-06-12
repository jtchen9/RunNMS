from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Tuple

# ============================================================
# Allow import from NMS project root
# ============================================================

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config
import utility
from m8mobility_state import S0_IDLE, S1_WAITING_REPORT
from m8mobility_state_store import (
    key_state,
    key_time,
    key_report,
    key_pose,
    _set_state,
    _save_stop,
)

from t9_mobility_report_intercept import read_recent_intercept_events


# ============================================================
# Editable settings
# ============================================================

import os
SCANNER = os.environ.get("SCANNER", "twin-scout-charlie")
NMS_BASE = "http://localhost:8000"

TEST_ID = "K1"
TEST_NAME = "unknown_error_immediate_stop"

ACTION = "mobility.turn_move_turn.forward"
ARGS = {
    "pre_angle": 10.0,
    "distance_m": 0.2,
    "post_angle": -10.0,
}

EXPECTED_ERROR_CODE = "DEBUG_UNKNOWN_ERROR_CODE"

OUT_DIR = THIS_FILE.parent / "testSM"

LOCATION_ACTION = "mobility.report.location"
LOCATION_ARGS: Dict[str, Any] = {}

MAX_WAIT_LOCATION_SEC = 60
MAX_WAIT_ACTION_SEC = 120
POLL_EVERY_SEC = 1.0

CORRECTION_FENCE_COUNT = 999


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
            "nms_base": NMS_BASE,
            "started_at": self.started_at,
            "ended_at": "",
            "pass": False,
            "failure_reason": "",
            "config": {
                "ACTION": ACTION,
                "ARGS": ARGS,
                "expected_error_code": EXPECTED_ERROR_CODE,
                "correction_fence_count": CORRECTION_FENCE_COUNT,
                "max_wait_location_sec": MAX_WAIT_LOCATION_SEC,
                "max_wait_action_sec": MAX_WAIT_ACTION_SEC,
            },
            "events": [],
            "snapshots": {},
            "trace": [],
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

    def trace(self, phase: str, compact: Dict[str, Any]) -> None:
        row = {"phase": phase, **compact}
        self.data["trace"].append(row)

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
# Redis helper
# ============================================================

def hget(hash_key: str, field: str, default: str = "") -> str:
    v = config.r.hget(hash_key, field)
    if v is None:
        return default
    return str(v)


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
    raw = hget(hash_key, field)
    obj = parse_json_maybe(raw)
    return obj if isinstance(obj, dict) else {}


def cmd_key(scanner: str) -> str:
    return config.key_cmd_stream(scanner)


def clear_cmd_stream(reason: str) -> None:
    k = cmd_key(SCANNER)
    old_len = int(config.r.xlen(k))
    config.r.delete(k)
    msg = f"cleared {k}, old_len={old_len}"
    print(f"{reason}: {msg}")
    EV.event(reason, msg)


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


def full_snapshot() -> Dict[str, Any]:
    st = hgetall(key_state(SCANNER))
    tm = hgetall(key_time(SCANNER))
    pose = hgetall(key_pose(SCANNER))
    rep = latest_report()

    return {
        "ts": utility.local_ts(),
        "state": st,
        "time": tm,
        "pose": {
            **pose,
            "true_location_json": parse_json_maybe(pose.get("true_location_json", "")),
            "planned_location_json": parse_json_maybe(pose.get("planned_location_json", "")),
        },
        "report": rep,
        "cmd_stream": {
            "key": cmd_key(SCANNER),
            "len": int(config.r.xlen(cmd_key(SCANNER))),
            "newest_actions": newest_stream_actions(limit=10),
        },
    }


def compact_snapshot() -> Dict[str, Any]:
    st = hgetall(key_state(SCANNER))
    tm = hgetall(key_time(SCANNER))
    pose = hgetall(key_pose(SCANNER))
    rep = latest_report()

    true_loc = parse_json_maybe(pose.get("true_location_json", ""))
    planned_loc = parse_json_maybe(pose.get("planned_location_json", ""))

    def pose_short(p: Any) -> Dict[str, Any]:
        if not isinstance(p, dict):
            return {}
        return {
            "x_m": p.get("x_m"),
            "y_m": p.get("y_m"),
            "heading_deg": p.get("heading_deg"),
            "source": p.get("source"),
            "tag_count": p.get("tag_count"),
            "detail": p.get("detail"),
        }

    return {
        "ts": utility.local_ts(),
        "state": st.get("state", ""),
        "state_detail": st.get("state_detail", ""),
        "mobility_ready": st.get("mobility_ready", ""),
        "robot_safety_state": st.get("robot_safety_state", ""),
        "stop_experiment": st.get("stop_experiment", ""),
        "stop_reason": st.get("stop_reason", ""),
        "need_location_retry": st.get("need_location_retry", ""),
        "correction_attempt_count": st.get("correction_attempt_count", ""),
        "retry_count": st.get("retry_count", ""),
        "collision_veto_count": st.get("collision_veto_count", ""),
        "busy_count": st.get("busy_count", ""),
        "exec_fail_count": st.get("exec_fail_count", ""),
        "last_error_code": st.get("last_error_code", ""),
        "last_error_detail": st.get("last_error_detail", ""),
        "outgoing_command_action": st.get("outgoing_command_action", ""),
        "outgoing_command_args_json": st.get("outgoing_command_args_json", ""),
        "outgoing_command_source": st.get("outgoing_command_source", ""),
        "outgoing_command_updated_at": st.get("outgoing_command_updated_at", ""),
        "s1_timer_token": tm.get("s1_timer_token", ""),
        "busy_retry_token": tm.get("busy_retry_token", ""),
        "last_planned_command_issued_at": tm.get("last_planned_command_issued_at", ""),
        "last_mobility_report_at": tm.get("last_mobility_report_at", ""),
        "last_planned_command_action": pose.get("last_planned_command_action", ""),
        "last_planned_command_args_json": pose.get("last_planned_command_args_json", ""),
        "true_location": pose_short(true_loc),
        "planned_location": pose_short(planned_loc),
        "last_report_command": rep.get("last_command", ""),
        "last_report_args": rep.get("last_command_args", {}),
        "last_report_status": rep.get("last_exec_status", ""),
        "last_report_error_code": rep.get("last_error_code", ""),
        "last_report_error_detail": rep.get("last_error_detail", ""),
        "cmd_stream_len": int(config.r.xlen(cmd_key(SCANNER))),
        "newest_actions": newest_stream_actions(limit=10),
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
# HTTP helper
# ============================================================

def post_json(url: str, body: Dict[str, Any], timeout_sec: int = 10) -> Tuple[int, Any]:
    raw = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=raw,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            text = resp.read().decode("utf-8", errors="replace")
            try:
                return resp.status, json.loads(text)
            except Exception:
                return resp.status, text

    except urllib.error.HTTPError as e:
        text = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(text)
        except Exception:
            return e.code, text

    except urllib.error.URLError as e:
        return 0, {
            "status": "error",
            "detail": f"NMS not reachable at {url}",
            "exception": str(e),
        }


def enqueue_action() -> Tuple[int, Any]:
    url = f"{NMS_BASE}/cmd/_enqueue/{SCANNER}"
    body = {
        "category": "mobility",
        "action": ACTION,
        "args": ARGS,
    }
    return post_json(url, body)


# ============================================================
# Test fixture setup
# ============================================================

def reset_test_stop_state() -> None:
    """
    Test-only recovery from previous S7 stop.

    This does not issue any robot command.
    It only clears NMS stop/safety bookkeeping so the test can first
    request a fresh mobility.report.location.
    """
    _save_stop(False, "")
    clear_cmd_stream("RESET STOP CLEAR CMD STREAM")

    hset(
        key_state(SCANNER),
        {
            "state": S0_IDLE,
            "state_detail": "testSM reset previous stop before precondition",
            "robot_safety_state": "NORMAL",
            "stop_experiment": "false",
            "stop_reason": "",
            "need_location_retry": "false",
            "retry_count": "0",
            "collision_veto_count": "0",
            "busy_count": "0",
            "exec_fail_count": "0",
            "last_error_code": "",
            "last_error_detail": "",
            "s2_entry_reason": "",
        },
    )

    hset(
        key_time(SCANNER),
        {
            "s1_timer_token": "",
            "s1_timer_started_at": "",
            "busy_retry_token": "",
            "busy_retry_started_at": "",
        },
    )

    EV.event("reset_stop", "cleared previous s7stopped/UNSAFE_STOP for test fixture")


def check_basic_preconditions() -> None:
    s = compact_snapshot()
    errors: List[str] = []

    if s["mobility_ready"] != "true":
        errors.append(f"mobility_ready must be true, got {s['mobility_ready']!r}")

    if s["robot_safety_state"] not in ("", "NORMAL"):
        errors.append(f"robot_safety_state must be NORMAL/empty, got {s['robot_safety_state']!r}")

    if s["busy_retry_token"]:
        errors.append(f"busy_retry_token must be empty, got {s['busy_retry_token']!r}")

    if s["s1_timer_token"]:
        errors.append(f"s1_timer_token must be empty, got {s['s1_timer_token']!r}")

    if errors:
        fail("Precondition failed:\n- " + "\n- ".join(errors))


def prepare_nms_to_accept_location_report() -> str:
    now = utility.local_ts()
    token = f"testSM-location-{int(time.time())}"

    _set_state(SCANNER, S1_WAITING_REPORT, "testSM waiting for location report")

    hset(
        key_state(SCANNER),
        {
            "correction_attempt_count": str(CORRECTION_FENCE_COUNT),
            "retry_count": "0",
            "collision_veto_count": "0",
            "busy_count": "0",
            "exec_fail_count": "0",
            "s2_entry_reason": "",
            "need_location_retry": "false",
            "stop_experiment": "false",
            "robot_safety_state": "NORMAL",
        },
    )

    hset(
        key_pose(SCANNER),
        {
            "last_planned_command_action": LOCATION_ACTION,
            "last_planned_command_args_json": json.dumps(LOCATION_ARGS, ensure_ascii=False),
        },
    )

    hset(
        key_time(SCANNER),
        {
            "last_planned_command_issued_at": now,
            "s1_timer_token": token,
            "s1_timer_started_at": now,
            "busy_retry_token": "",
            "busy_retry_started_at": "",
        },
    )

    EV.event("location_prepare", "NMS prepared to accept mobility.report.location", {"token": token})
    return now


def inject_raw_location_command() -> str:
    now = utility.local_ts()
    fields = {
        "category": "mobility",
        "action": LOCATION_ACTION,
        "args_json": json.dumps(LOCATION_ARGS, ensure_ascii=False),
        "created_at": now,
        "execute_at": now,
        "source": "testSM.location_precondition",
    }
    xid = config.r.xadd(cmd_key(SCANNER), fields)

    EV.event("location_inject", "raw location command injected", {"xid": xid, "fields": fields})
    print("\nInjected raw location command:")
    print(json.dumps({"xid": xid, "fields": fields}, ensure_ascii=False, indent=2))
    return xid


def wait_for_location_report(start_ts: str) -> None:
    print("\nWaiting for fresh mobility.report.location...")
    phase_start = time.time()

    while time.time() - phase_start <= MAX_WAIT_LOCATION_SEC:
        s = compact_snapshot()
        EV.trace("location_wait", s)

        elapsed = int(time.time() - phase_start)
        print(
            f"[LOC {elapsed:03d}s] "
            f"state={s['state']!r} "
            f"detail={s['state_detail']!r} "
            f"report={s['last_report_command']!r}/"
            f"{s['last_report_status']!r}/"
            f"{s['last_report_error_code']!r} "
            f"report_at={s['last_mobility_report_at']!r}"
        )

        fresh = str(s["last_mobility_report_at"]) >= str(start_ts)
        matching = s["last_report_command"] == LOCATION_ACTION
        completed = s["last_report_status"] == "completed"
        true_loc = s["true_location"]
        true_ok = bool(true_loc.get("x_m") is not None)

        if fresh and matching and completed and true_ok:
            EV.event("location_done", "fresh location report received", {"elapsed_sec": elapsed})
            return

        time.sleep(POLL_EVERY_SEC)

    fail("Timed out waiting for fresh mobility.report.location and true_location_json.")


def align_planned_to_true_for_test() -> Dict[str, Any]:
    true_loc = load_json_field(key_pose(SCANNER), "true_location_json")
    if not true_loc:
        fail("Cannot align planned=true because true_location_json is missing.")

    hset(key_pose(SCANNER), {"planned_location_json": true_loc})

    hset(
        key_state(SCANNER),
        {
            "correction_attempt_count": "-1",
            "retry_count": "0",
            "collision_veto_count": "0",
            "busy_count": "0",
            "exec_fail_count": "0",
            "s2_entry_reason": "",
            "need_location_retry": "false",
            "stop_experiment": "false",
            "stop_reason": "",
            "robot_safety_state": "NORMAL",
            "last_error_code": "",
            "last_error_detail": "",
        },
    )

    _set_state(SCANNER, S0_IDLE, f"testSM planned=true, ready for {ACTION}")

    hset(
        key_time(SCANNER),
        {
            "s1_timer_token": "",
            "s1_timer_started_at": "",
            "busy_retry_token": "",
            "busy_retry_started_at": "",
        },
    )

    EV.event("align", "planned_location_json copied from true_location_json")
    return true_loc


# ============================================================
# Action test
# ============================================================

def fence_followup_correction() -> None:
    hset(key_state(SCANNER), {"correction_attempt_count": str(CORRECTION_FENCE_COUNT)})
    msg = f"correction_attempt_count={CORRECTION_FENCE_COUNT}"
    print(f"\nCorrection fence set: {msg}")
    EV.event("fence", "post-report correction fenced", msg)


def wait_for_immediate_stop(start_ts: str) -> None:
    """
    K1 DEBUG_UNKNOWN_ERROR_CODE test.

    Expected sequence:
      1. NMS issues mobility.turn_move_turn.forward.
      2. Robot really executes and reports completed.
      3. Debug outlet patches that report into:
             failed / DEBUG_UNKNOWN_ERROR_CODE
      4. NMS must immediately enter s7stopped / UNSAFE_STOP.
      5. NMS must not issue mobility.report.location.
      6. stop_experiment must become true.
    """
    print(f"\nWaiting for {EXPECTED_ERROR_CODE} immediate-stop handling for {ACTION}.")
    phase_start = time.time()

    saw_unknown_error = False

    while time.time() - phase_start <= MAX_WAIT_ACTION_SEC:
        s = compact_snapshot()
        EV.trace("action_wait_e1_immediate_stop", s)

        elapsed = int(time.time() - phase_start)
        actions = s["newest_actions"]

        print(
            f"[E1 {elapsed:03d}s] "
            f"state={s['state']!r} "
            f"detail={s['state_detail']!r} "
            f"safety={s['robot_safety_state']!r} "
            f"stop={s['stop_experiment']!r}/{s['stop_reason']!r} "
            f"cmd_len={s['cmd_stream_len']} "
            f"actions={actions} "
            f"outgoing={s['outgoing_command_action']!r} "
            f"report={s['last_report_command']!r}/"
            f"{s['last_report_status']!r}/"
            f"{s['last_report_error_code']!r} "
            f"report_at={s['last_mobility_report_at']!r}"
        )

        fresh = str(s["last_mobility_report_at"]) >= str(start_ts)

        if (
            fresh
            and s["last_report_command"] == ACTION
            and s["last_report_status"] == "failed"
            and s["last_report_error_code"] == EXPECTED_ERROR_CODE
        ):
            if not saw_unknown_error:
                saw_unknown_error = True
                EV.event(
                    "unknown_error_seen",
                    f"fresh matching ACTION report patched to {EXPECTED_ERROR_CODE}",
                    {
                        "elapsed_sec": elapsed,
                        "state": s["state"],
                        "state_detail": s["state_detail"],
                        "robot_safety_state": s["robot_safety_state"],
                        "stop_experiment": s["stop_experiment"],
                        "stop_reason": s["stop_reason"],
                    },
                )

        if saw_unknown_error and s["state"] == "s7stopped":
            final = compact_snapshot()
            EV.snapshot("FINAL", full_snapshot())

            errors: List[str] = []

            if final["robot_safety_state"] != "UNSAFE_STOP":
                errors.append(
                    f"robot_safety_state should be UNSAFE_STOP, got {final['robot_safety_state']!r}"
                )

            if final["stop_experiment"] != "true":
                errors.append(
                    f"stop_experiment should be true, got {final['stop_experiment']!r}"
                )

            stop_reason = str(final["stop_reason"] or "")
            if EXPECTED_ERROR_CODE not in stop_reason:
                errors.append(
                    f"stop_reason should contain {EXPECTED_ERROR_CODE}, got {final['stop_reason']!r}"
                )

            if final["need_location_retry"] != "false":
                errors.append(
                    f"need_location_retry should be false, got {final['need_location_retry']!r}"
                )

            if final["outgoing_command_action"] == LOCATION_ACTION:
                errors.append(
                    f"NMS should not issue {LOCATION_ACTION} after immediate-stop error"
                )

            if LOCATION_ACTION in final["newest_actions"]:
                errors.append(
                    f"command stream should not contain {LOCATION_ACTION}, actions={final['newest_actions']}"
                )

            if final["cmd_stream_len"] != 0:
                errors.append(
                    f"command stream should be empty after s7stopped cleanup, got len={final['cmd_stream_len']}, actions={final['newest_actions']}"
                )

            if final["last_report_command"] != ACTION:
                errors.append(
                    f"last_report_command should remain {ACTION}, got {final['last_report_command']!r}"
                )

            if final["last_report_error_code"] != EXPECTED_ERROR_CODE:
                errors.append(
                    f"last_report_error_code should be {EXPECTED_ERROR_CODE}, got {final['last_report_error_code']!r}"
                )

            if errors:
                fail("Immediate-stop verification failed:\n- " + "\n- ".join(errors))

            print()
            print("PASS:")
            print(
                f"{SCANNER} completed K1: {ACTION} args={json.dumps(ARGS, ensure_ascii=False)} "
                f"was patched to {EXPECTED_ERROR_CODE}; NMS entered s7stopped / UNSAFE_STOP "
                f"without issuing {LOCATION_ACTION}."
            )

            print("\nFINAL:")
            print(json.dumps(final, ensure_ascii=False, indent=2))

            out = EV.finish(True, "")
            print(f"Evidence saved to: {out}")
            return

        # Strong failure: NMS issued location retry after immediate-stop error.
        if saw_unknown_error and (
            s["outgoing_command_action"] == LOCATION_ACTION or LOCATION_ACTION in actions
        ):
            fail(
                f"NMS incorrectly issued {LOCATION_ACTION} after {EXPECTED_ERROR_CODE}: "
                f"state={s['state']}, detail={s['state_detail']}, actions={actions}"
            )

        time.sleep(POLL_EVERY_SEC)

    print()
    print("Recent mobility-report intercept events at E1 timeout:")
    try:
        for ev in read_recent_intercept_events(10):
            print(json.dumps(ev, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"Failed to read intercept events: {type(e).__name__}: {e}")

    fail(
        f"Timed out waiting for {EXPECTED_ERROR_CODE} immediate-stop sequence: "
        f"saw_unknown_error={saw_unknown_error}. "
        "See recent intercept events printed above."
    )


# ============================================================
# Main
# ============================================================

def run() -> None:
    print("=" * 72)
    print(f"{TEST_ID}: {TEST_NAME}")
    print("=" * 72)

    EV.event("start", "test started")
    print_compact("PRE")

    reset_test_stop_state()
    print_compact("AFTER RESET")

    check_basic_preconditions()

    clear_cmd_stream("BEFORE LOCATION")
    loc_start_ts = prepare_nms_to_accept_location_report()
    inject_raw_location_command()
    wait_for_location_report(loc_start_ts)

    print_compact("AFTER LOCATION")

    true_loc = align_planned_to_true_for_test()
    EV.event(
        "align_detail",
        "aligned planned=true",
        {
            "x_m": true_loc.get("x_m"),
            "y_m": true_loc.get("y_m"),
            "heading_deg": true_loc.get("heading_deg"),
            "source": true_loc.get("source"),
            "tag_count": true_loc.get("tag_count"),
            "detail": true_loc.get("detail"),
        },
    )

    print_compact("BEFORE ACTION")

    clear_cmd_stream("BEFORE ACTION")
    action_start_ts = utility.local_ts()

    status, payload = enqueue_action()
    EV.event("enqueue_action", "action command enqueue response", {"http_status": status, "payload": payload})

    print("\nENQUEUE ACTION RESPONSE:")
    print(json.dumps({"http_status": status, "payload": payload}, ensure_ascii=False, indent=2))

    if status != 200:
        fail(f"enqueue action failed http={status}")

    if not isinstance(payload, dict):
        fail("enqueue action response is not JSON dict")

    if payload.get("status") == "stopped" or payload.get("state") == "s7stopped":
        fail(
            f"NMS stopped before issuing {ACTION}: "
            f"state={payload.get('state')}, "
            f"status={payload.get('status')}, "
            f"detail={payload.get('detail')}"
        )

    issued = payload.get("issued_command", {})
    if not isinstance(issued, dict):
        fail("enqueue response missing issued_command")

    issued_action = issued.get("action", "")
    if issued_action != ACTION:
        clear_cmd_stream("ABORT: WRONG ISSUED ACTION")
        fail(f"Expected NMS to issue {ACTION}, but it issued {issued_action}: {issued}")

    fence_followup_correction()
    wait_for_immediate_stop(action_start_ts)


if __name__ == "__main__":
    run()
