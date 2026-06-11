from __future__ import annotations

import json
import math
import os
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

# m8mobility_map loads sitemap/DemoRoom/site.json using a relative path.
# When this test is launched from testSM, the current directory is testSM,
# so normalize the working directory to the NMS project root.
os.chdir(str(PROJECT_ROOT))

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

from m8mobility_command_model import _normalize_mobility_command
from m8mobility_pose import _apply_mobility_command_to_pose
from m8mobility_map import _is_path_clear_debug


# ============================================================
# Editable settings
# ============================================================

SCANNER = "twin-scout-charlie"
NMS_BASE = "http://localhost:8000"

TEST_ID = "H1H2"
TEST_NAME = "s5_vector_initial_and_correction"

# Default only. The script will overwrite ARGS at runtime with a short
# restriction-map-safe vector chosen from Charlie's current true pose.
ACTION = "mobility.turn_move_turn.forward"
ARGS = {
    "pre_angle": 0.0,
    "distance_m": 0.3,
    "post_angle": 0.0,
}

LOCATION_ACTION = "mobility.report.location"
LOCATION_ARGS: Dict[str, Any] = {}

OUT_DIR = Path("testSM")

MAX_WAIT_LOCATION_SEC = 60
MAX_WAIT_ACTION_SEC = 120
MAX_WAIT_CORRECTION_SEC = 120
POLL_EVERY_SEC = 1.0

# Math-to-math comparison thresholds.
# These should be tight because both expected and actual are calculated,
# not measured by the robot.
DIST_TOL_M = 1e-6
ANGLE_TOL_DEG = 1e-6


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
                "location_action": LOCATION_ACTION,
                "dist_tol_m": DIST_TOL_M,
                "angle_tol_deg": ANGLE_TOL_DEG,
                "max_wait_location_sec": MAX_WAIT_LOCATION_SEC,
                "max_wait_action_sec": MAX_WAIT_ACTION_SEC,
                "max_wait_correction_sec": MAX_WAIT_CORRECTION_SEC,
            },
            "events": [],
            "snapshots": {},
            "trace": [],
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

    def snapshot(self, name: str, snap: Dict[str, Any]) -> None:
        self.data["snapshots"][name] = snap

    def trace(self, phase: str, compact: Dict[str, Any]) -> None:
        self.data["trace"].append({"phase": phase, **compact})

    def check(self, name: str, data: Dict[str, Any]) -> None:
        self.data["checks"][name] = data

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


def newest_stream_rows(limit: int = 10) -> List[Dict[str, Any]]:
    rows = config.r.xrevrange(cmd_key(SCANNER), "+", "-", count=limit)
    out: List[Dict[str, Any]] = []

    for xid, fields in rows:
        item = {"xid": str(xid)}
        for k, v in fields.items():
            item[str(k)] = v
        out.append(item)

    return out


def newest_stream_actions(limit: int = 10) -> List[str]:
    return [str(r.get("action", "")) for r in newest_stream_rows(limit) if r.get("action", "")]


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
            "newest": newest_stream_rows(limit=10),
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
            "location_ok": p.get("location_ok"),
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
        "busy_count": st.get("busy_count", ""),
        "collision_veto_count": st.get("collision_veto_count", ""),
        "exec_fail_count": st.get("exec_fail_count", ""),
        "s1_timer_token": tm.get("s1_timer_token", ""),
        "busy_retry_token": tm.get("busy_retry_token", ""),
        "last_planned_command_issued_at": tm.get("last_planned_command_issued_at", ""),
        "last_mobility_report_at": tm.get("last_mobility_report_at", ""),
        "last_correction_issued_at": st.get("last_correction_issued_at", ""),
        "outgoing_command_action": st.get("outgoing_command_action", ""),
        "outgoing_command_args_json": st.get("outgoing_command_args_json", ""),
        "outgoing_command_source": st.get("outgoing_command_source", ""),
        "outgoing_command_updated_at": st.get("outgoing_command_updated_at", ""),
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
# Geometry helpers
# ============================================================

def deg_norm_360(a: float) -> float:
    return float(a) % 360.0


def angle_diff_deg(a: float, b: float) -> float:
    """
    Signed shortest difference a-b in degrees, in [-180, 180).
    """
    return ((float(a) - float(b) + 180.0) % 360.0) - 180.0


def pose_ok(p: Dict[str, Any]) -> bool:
    return (
        isinstance(p, dict)
        and p.get("x_m") is not None
        and p.get("y_m") is not None
        and p.get("heading_deg") is not None
    )


def pose_minimal(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "location_ok": True,
        "x_m": float(p["x_m"]),
        "y_m": float(p["y_m"]),
        "heading_deg": deg_norm_360(float(p["heading_deg"])),
    }


def expected_tmt_forward_command(true_loc: Dict[str, Any], planned_loc: Dict[str, Any]) -> Tuple[str, Dict[str, float], Dict[str, Any]]:
    """
    Independent command-vector calculation for S5.

    This checks the core chasing logic:
        correction vector = planned - true
    """
    tx = float(true_loc["x_m"])
    ty = float(true_loc["y_m"])
    th = deg_norm_360(float(true_loc["heading_deg"]))

    px = float(planned_loc["x_m"])
    py = float(planned_loc["y_m"])
    ph = deg_norm_360(float(planned_loc["heading_deg"]))

    dx = px - tx
    dy = py - ty
    distance_m = math.hypot(dx, dy)

    travel_heading_deg = deg_norm_360(math.degrees(math.atan2(dy, dx)))
    pre_angle = angle_diff_deg(travel_heading_deg, th)
    post_angle = angle_diff_deg(ph, travel_heading_deg)

    action = "mobility.turn_move_turn.forward"
    args = {
        "pre_angle": float(pre_angle),
        "distance_m": float(distance_m),
        "post_angle": float(post_angle),
    }

    debug = {
        "true_x_m": tx,
        "true_y_m": ty,
        "true_heading_deg": th,
        "planned_x_m": px,
        "planned_y_m": py,
        "planned_heading_deg": ph,
        "dx_m": dx,
        "dy_m": dy,
        "distance_m": distance_m,
        "travel_heading_deg": travel_heading_deg,
    }

    return action, args, debug


def normalize_expected_command(action: str, args: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    action_n, args_n = _normalize_mobility_command(action, args)
    return action_n, args_n


def command_delta(expected_action: str, expected_args: Dict[str, Any], actual_action: str, actual_args: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "expected_action": expected_action,
        "actual_action": actual_action,
        "action_match": expected_action == actual_action,
        "pre_angle_diff_deg": None,
        "distance_diff_m": None,
        "post_angle_diff_deg": None,
        "ok": False,
    }

    if expected_action != actual_action:
        return out

    try:
        out["pre_angle_diff_deg"] = angle_diff_deg(float(actual_args.get("pre_angle", 0.0)), float(expected_args.get("pre_angle", 0.0)))
        out["distance_diff_m"] = float(actual_args.get("distance_m", 0.0)) - float(expected_args.get("distance_m", 0.0))
        out["post_angle_diff_deg"] = angle_diff_deg(float(actual_args.get("post_angle", 0.0)), float(expected_args.get("post_angle", 0.0)))
        out["ok"] = (
            abs(float(out["pre_angle_diff_deg"])) <= ANGLE_TOL_DEG
            and abs(float(out["distance_diff_m"])) <= DIST_TOL_M
            and abs(float(out["post_angle_diff_deg"])) <= ANGLE_TOL_DEG
        )
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"

    return out


def assert_command_matches(name: str, true_loc: Dict[str, Any], planned_loc: Dict[str, Any], actual_action: str, actual_args: Dict[str, Any]) -> Dict[str, Any]:
    exp_action, exp_args, debug = expected_tmt_forward_command(true_loc, planned_loc)
    exp_action_n, exp_args_n = normalize_expected_command(exp_action, exp_args)
    act_action_n, act_args_n = normalize_expected_command(actual_action, actual_args)

    delta = command_delta(exp_action_n, exp_args_n, act_action_n, act_args_n)

    check = {
        "true_location": true_loc,
        "planned_location": planned_loc,
        "expected_command": {
            "action": exp_action_n,
            "args": exp_args_n,
            "debug": debug,
        },
        "actual_command": {
            "action": act_action_n,
            "args": act_args_n,
        },
        "delta": delta,
    }

    EV.check(name, check)

    if not delta.get("ok"):
        fail(f"{name} S5 vector mismatch:\n" + json.dumps(check, ensure_ascii=False, indent=2))

    print(f"\n{name} vector check PASS:")
    print(json.dumps(check, ensure_ascii=False, indent=2))
    return check


def choose_safe_script_command(true_loc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pick a short safe test command from current true pose.

    This avoids blindly issuing "forward 0.5 m" when Charlie happens to face
    a wall. The candidate is checked against the same restriction-map path
    checker used by S5.
    """
    if not pose_ok(true_loc):
        fail("Cannot choose safe command because true_loc is invalid.")

    t = pose_minimal(true_loc)
    tx = float(t["x_m"])
    ty = float(t["y_m"])

    # Prefer small vectors first. Keep post_angle=-pre_angle so the final
    # planned heading remains the same as the starting heading.
    distances = [0.20, 0.30, 0.40, 0.50]
    pre_angles = [
        0.0,
        30.0, -30.0,
        60.0, -60.0,
        90.0, -90.0,
        120.0, -120.0,
        150.0, -150.0,
        180.0,
    ]

    candidates: List[Dict[str, Any]] = []

    for d in distances:
        for pre in pre_angles:
            args = {
                "pre_angle": float(pre),
                "distance_m": float(d),
                "post_angle": float(-pre),
            }

            target = _apply_mobility_command_to_pose(t, ACTION, args)
            px = float(target["x_m"])
            py = float(target["y_m"])

            ok, blocked, path_debug = _is_path_clear_debug(
                tx,
                ty,
                px,
                py,
                exclude_scanner=SCANNER,
            )

            item = {
                "action": ACTION,
                "args": args,
                "target": target,
                "path_ok": bool(ok),
                "blocked_count": len(blocked),
                "start_grid": path_debug.get("start", {}).get("grid"),
                "target_grid": path_debug.get("target", {}).get("grid"),
                "blocked_cells_first10": path_debug.get("blocked_cells", [])[:10],
            }
            candidates.append(item)

            if ok:
                EV.check(
                    "safe_command_selection",
                    {
                        "selected": item,
                        "candidate_count_tested": len(candidates),
                        "true_location": t,
                    },
                )
                print("\nSelected safe test command:")
                print(json.dumps(item, ensure_ascii=False, indent=2))
                return item

    EV.check(
        "safe_command_selection",
        {
            "selected": None,
            "candidate_count_tested": len(candidates),
            "true_location": t,
            "candidates_first20": candidates[:20],
            "candidates_last10": candidates[-10:],
        },
    )

    fail(
        "No short safe H1/H2 test command found from current true pose. "
        "Charlie may be located inside/too close to a restricted cell, or all "
        "short candidate directions are blocked. Move Charlie to a more open "
        "area and run mobility.report.location again."
    )


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
    Test-only recovery from previous stop.
    """
    _save_stop(False, "")
    clear_cmd_stream("RESET STOP CLEAR CMD STREAM")

    hset(
        key_state(SCANNER),
        {
            "state": S0_IDLE,
            "state_detail": "testSM reset previous stop before H1H2",
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
            "true_propagation_applied": "",
            "true_propagation_detail": "",
            "true_propagation_time": "",
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

    EV.event("reset_stop", "cleared previous stop/safety state")


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

    # Protect the precondition location step from stale planned_location_json.
    #
    # If Charlie was manually moved, the old planned pose may be far away.
    # A fresh mobility.report.location would otherwise make S5 chase that old
    # planned pose before the real H1/H2 test starts.
    current_true = load_json_field(key_pose(SCANNER), "true_location_json")
    if pose_ok(current_true):
        hset(key_pose(SCANNER), {"planned_location_json": current_true})

    _set_state(SCANNER, S1_WAITING_REPORT, "testSM waiting for location report")

    hset(
        key_state(SCANNER),
        {
            # Fence only the precondition location stage.
            # align_planned_to_true_for_test() resets this to -1 before the
            # real script command so H1/H2 correction behavior is still enabled.
            "correction_attempt_count": "999",
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

    # IMPORTANT:
    # Do NOT set correction_attempt_count = 999 here.
    # This test must allow normal S5/S6 behavior:
    #   -1 before initial issue
    #    0 after initial issue
    #    1 after correction issue
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
# H1/H2 core
# ============================================================

def wait_for_first_action_report(start_ts: str) -> None:
    print(f"\nWaiting for first {ACTION} report...")
    phase_start = time.time()

    while time.time() - phase_start <= MAX_WAIT_ACTION_SEC:
        s = compact_snapshot()
        EV.trace("first_action_wait", s)

        elapsed = int(time.time() - phase_start)
        print(
            f"[H1 action {elapsed:03d}s] "
            f"state={s['state']!r} "
            f"detail={s['state_detail']!r} "
            f"corr={s['correction_attempt_count']!r} "
            f"cmd_len={s['cmd_stream_len']} "
            f"actions={s['newest_actions']} "
            f"report={s['last_report_command']!r}/"
            f"{s['last_report_status']!r}/"
            f"{s['last_report_error_code']!r} "
            f"report_at={s['last_mobility_report_at']!r}"
        )

        fresh = str(s["last_mobility_report_at"]) >= str(start_ts)
        if fresh and s["last_report_command"] == ACTION:
            if s["last_report_status"] != "completed" or s["last_report_error_code"]:
                fail(
                    f"First action report failed unexpectedly: "
                    f"{s['last_report_status']!r} / {s['last_report_error_code']!r}"
                )
            EV.event("first_action_report_done", "fresh first action report received", {"elapsed_sec": elapsed})
            return

        if s["state"] == "s7stopped":
            fail(f"NMS stopped while waiting for first action report: {s['state_detail']}")

        time.sleep(POLL_EVERY_SEC)

    fail(f"Timed out waiting for first {ACTION} report.")


def wait_for_second_correction_or_done(first_report_ts: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Returns:
      (True, issued_correction) if S5/S6 issued a second command.
      (False, final_snapshot) if first action already close enough and no correction was needed.
    """
    print("\nWaiting for second-round correction command or clean completion...")
    phase_start = time.time()

    while time.time() - phase_start <= MAX_WAIT_CORRECTION_SEC:
        s = compact_snapshot()
        EV.trace("correction_wait", s)

        elapsed = int(time.time() - phase_start)
        print(
            f"[H2 correction {elapsed:03d}s] "
            f"state={s['state']!r} "
            f"detail={s['state_detail']!r} "
            f"corr={s['correction_attempt_count']!r} "
            f"cmd_len={s['cmd_stream_len']} "
            f"actions={s['newest_actions']} "
            f"outgoing={s['outgoing_command_action']!r} "
            f"report={s['last_report_command']!r}/"
            f"{s['last_report_status']!r}/"
            f"{s['last_report_error_code']!r}"
        )

        # If S6 issued a correction, it will be recorded as last planned command.
        if (
            s["state"] == S1_WAITING_REPORT
            and s["last_planned_command_action"] == ACTION
            and s["correction_attempt_count"] == "1"
            and str(s["last_planned_command_issued_at"]) >= str(first_report_ts)
        ):
            try:
                args = json.loads(s["last_planned_command_args_json"])
            except Exception as e:
                fail(f"Correction args JSON decode failed: {type(e).__name__}: {e}")

            if not isinstance(args, dict):
                fail(f"Correction args not dict: {type(args).__name__}")

            issued = {
                "action": s["last_planned_command_action"],
                "args": args,
                "issued_at": s["last_planned_command_issued_at"],
            }

            EV.event("correction_issued", "second-round correction command issued", issued)
            return True, issued

        # If S5 decided the first movement was already close enough,
        # no second command exists. This is not a failure for H1.
        if s["state"] == S0_IDLE and "already close enough" in str(s["state_detail"]):
            EV.event("correction_skipped", "first movement already close enough; no second correction needed")
            return False, s

        # If the correction command was very short, it may be issued, executed,
        # reported, and processed before this polling loop observes S1.
        #
        # In that case the final state is usually:
        #   state=s0idle
        #   state_detail="correction limit reached (1)"
        #   correction_attempt_count=1
        #   last_planned_command_action=mobility.turn_move_turn.forward
        #   last_report_command=mobility.turn_move_turn.forward
        #
        # This is NOT "no correction needed"; it is "correction already completed".
        if s["state"] == S0_IDLE and "correction limit reached" in str(s["state_detail"]):
            issued_at = str(s["last_planned_command_issued_at"] or "")
            is_after_first_report = issued_at >= str(first_report_ts)
            looks_like_completed_correction = (
                s["correction_attempt_count"] == "1"
                and s["last_planned_command_action"] == ACTION
                and s["last_report_command"] == ACTION
                and s["last_report_status"] == "completed"
                and not s["last_report_error_code"]
                and is_after_first_report
            )

            if looks_like_completed_correction:
                try:
                    args = json.loads(s["last_planned_command_args_json"])
                except Exception as e:
                    fail(f"Completed correction args JSON decode failed: {type(e).__name__}: {e}")

                if not isinstance(args, dict):
                    fail(f"Completed correction args not dict: {type(args).__name__}")

                issued = {
                    "action": s["last_planned_command_action"],
                    "args": args,
                    "issued_at": issued_at,
                    "detected_after_completion": True,
                    "last_report_args": s["last_report_args"],
                }

                EV.event(
                    "correction_completed_before_poll",
                    "second-round correction was already completed before polling observed S1",
                    issued,
                )
                return True, issued

            EV.event(
                "correction_skipped_limit",
                "correction limit reached but no completed correction command could be identified",
                s,
            )
            return False, s

        if s["state"] == "s7stopped":
            fail(f"NMS stopped while waiting for correction command: {s['state_detail']}")

        time.sleep(POLL_EVERY_SEC)

    fail("Timed out waiting for second-round correction command or clean completion.")


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

    true_before = align_planned_to_true_for_test()
    true_before_min = pose_minimal(true_before)

    # Choose a safe relative script command from Charlie's current pose.
    # This prevents the test from blindly driving toward a wall.
    global ARGS
    safe_choice = choose_safe_script_command(true_before_min)
    ARGS = dict(safe_choice["args"])
    EV.event("safe_command_selected", "selected restriction-map-safe H1/H2 command", safe_choice)

    print_compact("BEFORE SCRIPT ACTION")

    clear_cmd_stream("BEFORE ACTION")

    # Script intent updates planned location in S0 and S5/S6 issues the first command.
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
    issued_args = issued.get("args") or {}
    if not isinstance(issued_args, dict):
        fail("issued_command args is not dict")

    planned_after_script = load_json_field(key_pose(SCANNER), "planned_location_json")
    if not pose_ok(planned_after_script):
        fail("planned_location_json invalid after script enqueue")

    # H1: initial S5 vector check.
    assert_command_matches(
        "H1_initial_s5_vector",
        true_before_min,
        pose_minimal(planned_after_script),
        issued_action,
        issued_args,
    )

    action_start_ts = utility.local_ts()
    wait_for_first_action_report(action_start_ts)

    after_first_report = compact_snapshot()
    first_report_ts = after_first_report["last_mobility_report_at"]

    true_after_first = load_json_field(key_pose(SCANNER), "true_location_json")
    planned_target = load_json_field(key_pose(SCANNER), "planned_location_json")

    if not pose_ok(true_after_first):
        fail("true_location_json invalid after first action report")

    if not pose_ok(planned_target):
        fail("planned_location_json invalid after first action report")

    correction_issued, correction_info = wait_for_second_correction_or_done(first_report_ts)

    if correction_issued:
        # H2: second-round correction vector check.
        assert_command_matches(
            "H2_second_round_s5_vector",
            pose_minimal(true_after_first),
            pose_minimal(planned_target),
            correction_info["action"],
            correction_info["args"],
        )
        h2_status = "PASS"
    else:
        h2_status = "SKIPPED_NO_OBSERVABLE_CORRECTION"
        EV.check(
            "H2_second_round_s5_vector",
            {
                "status": h2_status,
                "reason": "No observable second correction command was identified. If the robot physically moved twice, this is a test-harness detection failure and should be investigated.",
                "final_snapshot": correction_info,
                "true_after_first": true_after_first,
                "planned_target": planned_target,
            },
        )
        print("\nH2 second-round correction vector check SKIPPED:")
        print(json.dumps(EV.data["checks"]["H2_second_round_s5_vector"], ensure_ascii=False, indent=2))

    print_compact("FINAL")

    out = EV.finish(True, "")

    print("\nPASS:")
    print(f"{SCANNER} completed H1/H2 S5 vector test.")
    print("H1 initial S5 vector: PASS")
    print(f"H2 second-round S5 vector: {h2_status}")
    print(f"Evidence saved to: {out}")


if __name__ == "__main__":
    run()
