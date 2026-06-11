from __future__ import annotations

import json
import math
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
from m8mobility_state_store import key_state, key_time, key_report, key_pose, _set_state, _save_stop


# ============================================================
# Editable settings
# ============================================================

SCANNER = "twin-scout-charlie"
NMS_BASE = "http://localhost:8000"

TEST_ID = "D1"
TEST_NAME = "no_tag_visible_true_propagation"

ACTION = "mobility.turn_move_turn.forward"
ARGS = {
    "pre_angle": 10.0,
    "distance_m": 0.2,
    "post_angle": -10.0
}
OUT_DIR = Path("testSM")

LOCATION_ACTION = "mobility.report.location"
LOCATION_ARGS: Dict[str, Any] = {}

MAX_WAIT_LOCATION_SEC = 60
MAX_WAIT_ACTION_SEC = 180
POLL_EVERY_SEC = 1.0

CORRECTION_FENCE_COUNT = 999
EXPECTED_ERROR_CODE = "NO_TAG_VISIBLE"
POS_TOL_M = 0.03
HEADING_TOL_DEG = 1.0


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
                "location_action": LOCATION_ACTION,
                "ACTION": ACTION,
                "ARGS": ARGS,
                "expected_error_code": EXPECTED_ERROR_CODE,
                "pos_tol_m": POS_TOL_M,
                "heading_tol_deg": HEADING_TOL_DEG,
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
        "correction_attempt_count": st.get("correction_attempt_count", ""),
        "retry_count": st.get("retry_count", ""),
        "collision_veto_count": st.get("collision_veto_count", ""),
        "busy_count": st.get("busy_count", ""),
        "exec_fail_count": st.get("exec_fail_count", ""),
        "need_location_retry": st.get("need_location_retry", ""),
        "stop_experiment": st.get("stop_experiment", ""),
        "stop_reason": st.get("stop_reason", ""),
        "true_propagation_applied": st.get("true_propagation_applied", ""),
        "true_propagation_detail": st.get("true_propagation_detail", ""),
        "true_propagation_time": st.get("true_propagation_time", ""),
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
# Independent propagation checker
# ============================================================

def deg_norm_360(angle_deg: float) -> float:
    return float(angle_deg) % 360.0


def angle_diff_deg(a: float, b: float) -> float:
    return (float(a) - float(b) + 180.0) % 360.0 - 180.0


def independent_apply_command(loc: Dict[str, Any], action: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Independent test-side calculation of the pose after the issued command.

    This intentionally does not import _apply_mobility_command_to_pose(), because
    D1 is a safety test for that same propagation behavior.
    """
    x0 = float(loc["x_m"])
    y0 = float(loc["y_m"])
    h0 = float(loc["heading_deg"])

    if action == "mobility.turn":
        return {
            "location_ok": True,
            "x_m": x0,
            "y_m": y0,
            "heading_deg": deg_norm_360(h0 + float(args["angle_deg"])),
        }

    if action in ("mobility.turn_move_turn.forward", "mobility.turn_move_turn.backward"):
        pre = float(args["pre_angle"])
        distance = float(args["distance_m"])
        post = float(args["post_angle"])
        direction = 1.0 if action == "mobility.turn_move_turn.forward" else -1.0

        h1 = deg_norm_360(h0 + pre)
        rad = math.radians(h1)
        dx = direction * distance * math.cos(rad)
        dy = direction * distance * math.sin(rad)

        return {
            "location_ok": True,
            "x_m": x0 + dx,
            "y_m": y0 + dy,
            "heading_deg": deg_norm_360(h1 + post),
            "debug": {
                "x0": x0,
                "y0": y0,
                "h0": h0,
                "h1_after_pre_angle": h1,
                "dx_m": dx,
                "dy_m": dy,
                "direction": direction,
            },
        }

    if action == "mobility.report.location":
        return {
            "location_ok": True,
            "x_m": x0,
            "y_m": y0,
            "heading_deg": h0,
        }

    raise ValueError(f"unsupported action in independent_apply_command: {action}")


def pose_delta(actual: Dict[str, Any], expected: Dict[str, Any]) -> Dict[str, float]:
    dx = float(actual["x_m"]) - float(expected["x_m"])
    dy = float(actual["y_m"]) - float(expected["y_m"])
    return {
        "dx_m": dx,
        "dy_m": dy,
        "dpos_m": math.hypot(dx, dy),
        "dhead_deg": angle_diff_deg(float(actual["heading_deg"]), float(expected["heading_deg"])),
    }


def require_pose_close(actual: Dict[str, Any], expected: Dict[str, Any], context: Dict[str, Any]) -> None:
    d = pose_delta(actual, expected)
    ok_pos = abs(float(d["dpos_m"])) <= POS_TOL_M
    ok_head = abs(float(d["dhead_deg"])) <= HEADING_TOL_DEG

    EV.event(
        "propagation_numeric_check",
        "compared NMS propagated true_location against independent command-vector calculation",
        {
            "actual": actual,
            "expected": expected,
            "delta": d,
            **context,
        },
    )

    if not ok_pos or not ok_head:
        fail(
            "NO_TAG_VISIBLE true-location propagation vector is wrong.\n"
            f"actual={json.dumps(actual, ensure_ascii=False)}\n"
            f"expected={json.dumps(expected, ensure_ascii=False)}\n"
            f"delta={json.dumps(d, ensure_ascii=False)}\n"
            f"context={json.dumps(context, ensure_ascii=False)}"
        )

# ============================================================
# Action test
# ============================================================

def fence_followup_correction() -> None:
    hset(key_state(SCANNER), {"correction_attempt_count": str(CORRECTION_FENCE_COUNT)})
    msg = f"correction_attempt_count={CORRECTION_FENCE_COUNT}"
    print(f"\nCorrection fence set: {msg}")
    EV.event("fence", "post-report correction fenced", msg)


def wait_for_action_report(start_ts: str, true_before_action: Dict[str, Any], issued_action: str, issued_args: Dict[str, Any]) -> None:
    """
    D1 NO_TAG_VISIBLE true-location propagation test.

    Expected sequence:
      1. NMS issues a real movement command.
      2. Robot returns a matching movement report.
      3. Debug outlet patches that report to completed / NO_TAG_VISIBLE.
      4. S2 treats NO_TAG_VISIBLE as normal sparse-tag operation.
      5. S3 cannot solve AprilTag and propagates true_location by the last issued command.
      6. The propagated true_location must equal an independent test-side calculation:
             true_before_action + issued command vector
      7. NMS must not issue a location retry and must not stop.
    """
    print(f"\nWaiting for D1 NO_TAG_VISIBLE propagation after {ACTION}...")
    phase_start = time.time()

    saw_no_tag = False
    saw_propagation = False
    no_tag_report_at = ""

    expected_true = independent_apply_command(true_before_action, issued_action, issued_args)
    EV.event(
        "expected_propagated_true",
        "independent expected true_location calculated before waiting for report",
        {
            "true_before_action": true_before_action,
            "issued_action": issued_action,
            "issued_args": issued_args,
            "expected_true": expected_true,
        },
    )

    while time.time() - phase_start <= MAX_WAIT_ACTION_SEC:
        s = compact_snapshot()
        EV.trace("d1_wait_no_tag_true_propagation", s)

        elapsed = int(time.time() - phase_start)
        actions = s["newest_actions"]

        print(
            f"[D1 {elapsed:03d}s] "
            f"state={s['state']!r} "
            f"detail={s['state_detail']!r} "
            f"cmd_len={s['cmd_stream_len']} "
            f"actions={actions} "
            f"safety={s.get('robot_safety_state', '')!r} "
            f"prop={s.get('true_propagation_applied', '')!r} "
            f"retry_count={s.get('retry_count', '')!r} "
            f"report={s['last_report_command']!r}/"
            f"{s['last_report_status']!r}/"
            f"{s['last_report_error_code']!r} "
            f"report_at={s['last_mobility_report_at']!r} "
            f"true=({s['true_location'].get('x_m')}, {s['true_location'].get('y_m')}, {s['true_location'].get('heading_deg')})"
        )

        fresh = str(s["last_mobility_report_at"]) >= str(start_ts)
        matching = s["last_report_command"] == ACTION

        # ---------------------------------------------------------
        # 1. Expected injected no-tag report
        # ---------------------------------------------------------
        if (
            fresh
            and matching
            and s["last_report_status"] in ("completed", "ok", "accepted", "")
            and s["last_report_error_code"] == EXPECTED_ERROR_CODE
        ):
            if not saw_no_tag:
                saw_no_tag = True
                no_tag_report_at = str(s["last_mobility_report_at"])
                EV.event(
                    "no_tag_visible_seen",
                    "movement report patched to NO_TAG_VISIBLE",
                    {
                        "elapsed_sec": elapsed,
                        "state": s["state"],
                        "state_detail": s["state_detail"],
                        "robot_safety_state": s.get("robot_safety_state", ""),
                    },
                )

                if s["state"] == "s7stopped":
                    fail("NO_TAG_VISIBLE incorrectly caused immediate s7stopped")

        # ---------------------------------------------------------
        # 2. NMS must propagate true by issued command, not retry location
        # ---------------------------------------------------------
        if saw_no_tag:
            if s.get("last_planned_command_action", "") == LOCATION_ACTION:
                fail(
                    "NO_TAG_VISIBLE incorrectly caused NMS to issue mobility.report.location. "
                    "D1 expects true-location propagation, not location retry."
                )

            if LOCATION_ACTION in actions:
                fail(
                    "NO_TAG_VISIBLE incorrectly queued mobility.report.location. "
                    "D1 expects true-location propagation, not location retry."
                )

            actual_true = s.get("true_location") or {}
            if (
                s.get("true_propagation_applied", "") == "true"
                and actual_true.get("x_m") is not None
                and str(s["last_mobility_report_at"]) >= str(no_tag_report_at)
            ):
                saw_propagation = True

                require_pose_close(
                    actual=actual_true,
                    expected=expected_true,
                    context={
                        "elapsed_sec": elapsed,
                        "no_tag_report_at": no_tag_report_at,
                        "state": s["state"],
                        "state_detail": s["state_detail"],
                        "robot_safety_state": s.get("robot_safety_state", ""),
                        "true_propagation_detail": s.get("true_propagation_detail", ""),
                    },
                )

                final = compact_snapshot()
                EV.snapshot("FINAL", full_snapshot())

                if final["state"] == "s7stopped":
                    fail(f"NMS stopped after valid NO_TAG_VISIBLE propagation: {final['state_detail']}")

                if final["robot_safety_state"] not in ("", "NORMAL", "NORMAL_NO_TAG"):
                    fail(f"Unexpected final robot_safety_state: {final['robot_safety_state']}")

                if final.get("need_location_retry", "") == "true":
                    fail("need_location_retry should be false after successful no-tag propagation")

                print("\nPASS:")
                print(
                    f"{SCANNER} completed D1: {ACTION} args={json.dumps(issued_args, ensure_ascii=False)} "
                    f"was patched to {EXPECTED_ERROR_CODE}; NMS propagated true_location by the exact issued vector."
                )
                print("\nPropagation check:")
                print(json.dumps({
                    "true_before_action": true_before_action,
                    "issued_action": issued_action,
                    "issued_args": issued_args,
                    "expected_true": expected_true,
                    "actual_true": actual_true,
                    "delta": pose_delta(actual_true, expected_true),
                }, ensure_ascii=False, indent=2))
                return

        if s["state"] == "s7stopped":
            if not saw_no_tag:
                fail(f"NMS entered s7stopped before seeing injected NO_TAG_VISIBLE: {s['state_detail']}")
            if not saw_propagation:
                fail(f"NMS entered s7stopped before true propagation completed: {s['state_detail']}")

        time.sleep(POLL_EVERY_SEC)

    fail(
        "Timed out waiting for NO_TAG_VISIBLE true-location propagation: "
        f"saw_no_tag={saw_no_tag}, saw_propagation={saw_propagation}"
    )

def run() -> None:
    print("=" * 72)
    print("D1: NO_TAG_VISIBLE true-location propagation vector check")
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

    true_before_action = load_json_field(key_pose(SCANNER), "true_location_json")
    if not true_before_action:
        fail("Cannot run D1 because true_location_json is missing before action.")

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

    # NEW: NMS rejected/stopped before issuing any robot command.
    if payload.get("status") == "stopped" or payload.get("state") == "s7stopped":
        fail(
            f"NMS stopped before issuing {ACTION}: "
            f"state={payload.get('state')}, "
            f"status={payload.get('status')}, "
            f"detail={payload.get('detail')}"
        )

    # Existing code continues here.
    issued = payload.get("issued_command", {})
    if not isinstance(issued, dict):
        fail("enqueue response missing issued_command")

    issued_action = issued.get("action", "")
    if issued_action != ACTION:
        clear_cmd_stream("ABORT: WRONG ISSUED ACTION")
        fail(f"Expected NMS to issue {ACTION}, but it issued {issued_action}: {issued}")

    issued_args = issued.get("args", {})
    if not isinstance(issued_args, dict):
        fail(f"issued args is not dict: {issued_args!r}")

    EV.event(
        "issued_command_for_propagation_check",
        "captured original true_location and exact issued command before NO_TAG_VISIBLE report",
        {
            "true_before_action": true_before_action,
            "issued_action": issued_action,
            "issued_args": issued_args,
        },
    )

    fence_followup_correction()
    wait_for_action_report(action_start_ts, true_before_action, issued_action, issued_args)

    print_compact("FINAL")

    out = EV.finish(True, "")

    print(f"Evidence saved to: {out}")

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


if __name__ == "__main__":
    run()
