from __future__ import annotations

import json
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

import config
import utility
from m8mobility_state import S0_IDLE, S1_WAITING_REPORT
from m8mobility_state_store import key_state, key_time, key_report, key_pose, _set_state, _save_stop
from t9_mobility_report_intercept import read_recent_intercept_events


# ============================================================
# Editable settings
# ============================================================

SCANNER = "twin-scout-charlie"

TEST_ID = "C2"
TEST_NAME = "location_capture_fail_then_success"

LOCATION_ACTION = "mobility.report.location"
LOCATION_ARGS: Dict[str, Any] = {}
EXPECTED_ERROR_CODE = "LOCATION_CAPTURE_FAIL"

OUT_DIR = Path("testSM")

MAX_WAIT_SEC = 180
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
            "started_at": self.started_at,
            "ended_at": "",
            "pass": False,
            "failure_reason": "",
            "config": {
                "location_action": LOCATION_ACTION,
                "location_args": LOCATION_ARGS,
                "expected_error_code": EXPECTED_ERROR_CODE,
                "correction_fence_count": CORRECTION_FENCE_COUNT,
                "max_wait_sec": MAX_WAIT_SEC,
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
    raw = config.r.hget(hash_key, field)
    raw_s = "" if raw is None else str(raw)
    obj = parse_json_maybe(raw_s)
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
        "correction_attempt_count": st.get("correction_attempt_count", ""),
        "retry_count": st.get("retry_count", ""),
        "collision_veto_count": st.get("collision_veto_count", ""),
        "busy_count": st.get("busy_count", ""),
        "exec_fail_count": st.get("exec_fail_count", ""),
        "need_location_retry": st.get("need_location_retry", ""),
        "stop_experiment": st.get("stop_experiment", ""),
        "stop_reason": st.get("stop_reason", ""),
        "s1_timer_token": tm.get("s1_timer_token", ""),
        "busy_retry_token": tm.get("busy_retry_token", ""),
        "last_planned_command_issued_at": tm.get("last_planned_command_issued_at", ""),
        "last_mobility_report_at": tm.get("last_mobility_report_at", ""),
        "outgoing_command_action": st.get("outgoing_command_action", ""),
        "outgoing_command_args_json": st.get("outgoing_command_args_json", ""),
        "outgoing_command_source": st.get("outgoing_command_source", ""),
        "outgoing_command_updated_at": st.get("outgoing_command_updated_at", ""),
        "last_planned_command_action": pose.get("last_planned_command_action", ""),
        "last_planned_command_args_json": pose.get("last_planned_command_args_json", ""),
        "true_location": pose_short(true_loc),
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
# Test fixture setup
# ============================================================

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


def reset_test_stop_state() -> None:
    """
    Test-only recovery from previous S7 stop.

    This does not issue any robot command.
    It only clears NMS stop/safety bookkeeping so the test can request
    a fresh mobility.report.location.
    """
    _save_stop(False, "")
    clear_cmd_stream("RESET STOP CLEAR CMD STREAM")

    hset(
        key_state(SCANNER),
        {
            "state": S0_IDLE,
            "state_detail": "testSM reset previous stop before C2",
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

    EV.event("reset_stop", "cleared previous s7stopped/UNSAFE_STOP for C2 test")


def prepare_nms_to_accept_location_report() -> str:
    now = utility.local_ts()
    token = f"testSM-C2-location-{int(time.time())}"

    _set_state(SCANNER, S1_WAITING_REPORT, "testSM C2 waiting for first location report")

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
            "stop_reason": "",
            "robot_safety_state": "NORMAL",
            "last_error_code": "",
            "last_error_detail": "",
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

    EV.event("location_prepare", "NMS prepared to accept first mobility.report.location", {"token": token})
    return now


def inject_raw_location_command(source: str) -> str:
    now = utility.local_ts()
    fields = {
        "category": "mobility",
        "action": LOCATION_ACTION,
        "args_json": json.dumps(LOCATION_ARGS, ensure_ascii=False),
        "created_at": now,
        "execute_at": now,
        "source": source,
    }
    xid = config.r.xadd(cmd_key(SCANNER), fields)

    EV.event("location_inject", "raw location command injected", {"xid": xid, "fields": fields})
    print("\nInjected raw location command:")
    print(json.dumps({"xid": xid, "fields": fields}, ensure_ascii=False, indent=2))
    return xid


# ============================================================
# C2 wait logic
# ============================================================

def wait_for_location_fail_then_success(start_ts: str) -> None:
    """
    C2 LOCATION_CAPTURE_FAIL then success.

    Expected sequence:
      1. First mobility.report.location report is patched to:
             failed / LOCATION_CAPTURE_FAIL
      2. NMS treats this as recoverable, not immediate stop.
      3. NMS issues another mobility.report.location.
      4. Second real location report completes.
      5. NMS returns to a safe non-stopped state.
    """
    print("\nWaiting for C2: LOCATION_CAPTURE_FAIL followed by successful location retry.")
    phase_start = time.time()

    saw_location_fail = False
    saw_retry_issued = False
    saw_location_success = False
    fail_report_at = ""

    while time.time() - phase_start <= MAX_WAIT_SEC:
        s = compact_snapshot()
        EV.trace("c2_wait", s)

        elapsed = int(time.time() - phase_start)
        actions = s["newest_actions"]

        print(
            f"[C2 {elapsed:03d}s] "
            f"state={s['state']!r} "
            f"detail={s['state_detail']!r} "
            f"cmd_len={s['cmd_stream_len']} "
            f"actions={actions} "
            f"retry_count={s.get('retry_count', '')!r} "
            f"robot_safety_state={s.get('robot_safety_state', '')!r} "
            f"outgoing={s.get('outgoing_command_action', '')!r} "
            f"report={s['last_report_command']!r}/"
            f"{s['last_report_status']!r}/"
            f"{s['last_report_error_code']!r} "
            f"report_at={s['last_mobility_report_at']!r}"
        )

        fresh = str(s["last_mobility_report_at"]) >= str(start_ts)

        # ---------------------------------------------------------
        # 1. Expected injected first location failure
        # ---------------------------------------------------------
        if (
            fresh
            and s["last_report_command"] == LOCATION_ACTION
            and s["last_report_status"] == "failed"
            and s["last_report_error_code"] == EXPECTED_ERROR_CODE
        ):
            if not saw_location_fail:
                saw_location_fail = True
                fail_report_at = str(s["last_mobility_report_at"])

                EV.event(
                    "location_capture_fail_seen",
                    "first mobility.report.location patched to LOCATION_CAPTURE_FAIL",
                    {
                        "elapsed_sec": elapsed,
                        "state": s["state"],
                        "state_detail": s["state_detail"],
                        "retry_count": s.get("retry_count", ""),
                        "robot_safety_state": s.get("robot_safety_state", ""),
                    },
                )

                if s["state"] == "s7stopped":
                    fail("LOCATION_CAPTURE_FAIL incorrectly caused immediate s7stopped")

        # ---------------------------------------------------------
        # 2. NMS should issue another location command
        # ---------------------------------------------------------
        if saw_location_fail:
            outgoing_is_location = s.get("outgoing_command_action", "") == LOCATION_ACTION
            queued_location = LOCATION_ACTION in actions
            retry_count_ok = str(s.get("retry_count", "")) not in {"", "0"}

            if outgoing_is_location or queued_location or retry_count_ok:
                if not saw_retry_issued:
                    saw_retry_issued = True
                    EV.event(
                        "location_retry_issued",
                        "NMS issued another mobility.report.location after LOCATION_CAPTURE_FAIL",
                        {
                            "elapsed_sec": elapsed,
                            "state": s["state"],
                            "state_detail": s["state_detail"],
                            "outgoing_command_action": s.get("outgoing_command_action", ""),
                            "actions": actions,
                            "retry_count": s.get("retry_count", ""),
                        },
                    )

        # ---------------------------------------------------------
        # 3. Second location report completed
        # ---------------------------------------------------------
        if (
            saw_location_fail
            and saw_retry_issued
            and fresh
            and str(s["last_mobility_report_at"]) > str(fail_report_at)
            and s["last_report_command"] == LOCATION_ACTION
            and s["last_report_status"] == "completed"
            and not s["last_report_error_code"]
            and bool(s["true_location"].get("x_m") is not None)
        ):
            saw_location_success = True

            EV.event(
                "location_retry_completed",
                "second mobility.report.location completed after LOCATION_CAPTURE_FAIL",
                {
                    "elapsed_sec": elapsed,
                    "state": s["state"],
                    "state_detail": s["state_detail"],
                    "retry_count": s.get("retry_count", ""),
                    "robot_safety_state": s.get("robot_safety_state", ""),
                    "true_location": s["true_location"],
                },
            )

            final = compact_snapshot()
            EV.snapshot("FINAL", full_snapshot())

            if final["state"] == "s7stopped":
                fail(f"NMS stopped after successful location retry: {final['state_detail']}")

            if final["last_report_command"] != LOCATION_ACTION:
                fail(f"Final report is not location report: {final['last_report_command']}")

            if final["last_report_status"] != "completed" or final["last_report_error_code"]:
                fail(
                    f"Final location retry did not complete cleanly: "
                    f"{final['last_report_status']} / {final['last_report_error_code']}"
                )

            if final["robot_safety_state"] not in ("", "NORMAL"):
                fail(f"Final robot_safety_state is not NORMAL/empty: {final['robot_safety_state']}")

            print("\nPASS:")
            print(
                f"{SCANNER} completed C2: first {LOCATION_ACTION} was patched to "
                f"{EXPECTED_ERROR_CODE}, then NMS issued another {LOCATION_ACTION} and recovered."
            )
            return

        # ---------------------------------------------------------
        # 4. Bad stop before recovery
        # ---------------------------------------------------------
        if s["state"] == "s7stopped":
            if not saw_location_fail:
                fail(f"NMS entered s7stopped before injected location failure: {s['state_detail']}")
            if not saw_retry_issued:
                fail(
                    f"NMS entered s7stopped after LOCATION_CAPTURE_FAIL but before issuing retry: "
                    f"{s['state_detail']}"
                )
            if not saw_location_success:
                fail(f"NMS entered s7stopped before location retry completed: {s['state_detail']}")

        time.sleep(POLL_EVERY_SEC)

    print("\nRecent mobility-report intercept events at C2 timeout:")
    try:
        for ev in read_recent_intercept_events(10):
            print(json.dumps(ev, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"Failed to read intercept events: {type(e).__name__}: {e}")

    fail(
        "Timed out waiting for LOCATION_CAPTURE_FAIL retry sequence: "
        f"saw_location_fail={saw_location_fail}, "
        f"saw_retry_issued={saw_retry_issued}, "
        f"saw_location_success={saw_location_success}. "
        "See recent intercept events printed above."
    )


# ============================================================
# Main
# ============================================================

def run() -> None:
    print("=" * 72)
    print("C2: location capture fail once, then recover with successful retry")
    print("=" * 72)

    EV.event("start", "test started")
    print_compact("PRE")

    reset_test_stop_state()
    print_compact("AFTER RESET")

    check_basic_preconditions()

    clear_cmd_stream("BEFORE FIRST LOCATION")
    start_ts = prepare_nms_to_accept_location_report()
    inject_raw_location_command("testSM.C2.first_location")

    wait_for_location_fail_then_success(start_ts)

    print_compact("FINAL")

    out = EV.finish(True, "")
    print(f"Evidence saved to: {out}")


if __name__ == "__main__":
    run()
