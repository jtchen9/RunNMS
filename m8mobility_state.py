"""
Mobility subsystem state logic.

Division rule:
- State orchestration lives here, in state order.
- Shared services are imported from mobility-specific service files.
- No wildcard imports.

Round 1 note:
- Structural extraction only; logic is copied from the old monolith as directly as practical.
- Unfinished integration seams remain explicitly marked.
"""
from typing import Dict, Any
import math
import utility
import config

from utility import _hget, _hset_many, _to_int, _deg_norm_360, _deg_to_rad, _angle_diff_deg, _circular_mean_deg
from m8mobility_state_store import (
    key_state, key_time, key_pose, _get_state, _set_state, _load_stop, _is_anchor_fresh,
    _load_report_json, _save_policy_time, _save_pending_sequence, _clear_pending_sequence,
    _load_pending_sequence, _save_last_issued_command, _save_outgoing_command_preview,
    _clear_outgoing_command_preview, _reset_correction_counter, _inc_correction_counter,
    _get_correction_counter, _update_10s_report
)
from m8mobility_pose import (
    _is_loc_ok, _load_true, _load_planned, _save_true, _save_planned,
    _apply_mobility_command_to_pose, _pose_error
)
from m8mobility_map import _is_path_clear, _check_motion_path, _load_tag_map
from m8mobility_command_model import (
    _normalize_mobility_command, _build_command_from_true_to_planned,
    _build_turn_only_command, _build_turn_move_turn_forward_command,
    _propagate_true_by_last_command, _should_propagate_true
)

# ===== state constants =====

S0_IDLE = "s0idle"
S1_WAITING_REPORT = "s1waiting_report"
S2_EVALUATING_POLICY = "s2evaluating_policy"
S3_SOLVING_TRUE_LOCATION = "s3solving_true_location"
S4_WAITING_LOCATION_RETRY = "s4waiting_location_retry"
S5_COMPUTING_CORRECTION = "s5computing_correction"
S6_ISSUING_CORRECTION = "s6issuing_correction"
S7_STOPPED = "s7stopped"

VALID_STATES = {
    S0_IDLE,
    S1_WAITING_REPORT,
    S2_EVALUATING_POLICY,
    S3_SOLVING_TRUE_LOCATION,
    S4_WAITING_LOCATION_RETRY,
    S5_COMPUTING_CORRECTION,
    S6_ISSUING_CORRECTION,
    S7_STOPPED,
}


# ===== policy thresholds =====

LOCATION_RETRY_LIMIT = 2
COLLISION_VETO_STOP_THRESHOLD = 2
BUSY_STOP_THRESHOLD = 2
EXEC_FAIL_STOP_THRESHOLD = 2
IMMEDIATE_STOP_ERROR_CODES = {
    "TOF_SENSOR_FAIL",
    "BAD_COMMAND_ARGS",
    "UNEXPECTED_EXCEPTION",
}
RECOVERABLE_ERROR_CODES = {
    "MOVE_EXEC_FAIL",
    "TURN_EXEC_FAIL",
    "LOCATION_CAPTURE_FAIL",
    "NO_TAG_VISIBLE",
}
COLLISION_ERROR_CODES = {
    "COLLISION_BLOCKED_AT_START",
    "COLLISION_STOP_DURING_MOVE",
}

MULTI_TAG_POS_THRESH_M = 0.5
MULTI_TAG_HEADING_THRESH_DEG = 20.0


# ===== s0idle =====

def s0idle(scanner: str) -> Dict[str, Any]:
    return {"state": S0_IDLE, "status": "ok", "detail": "idle"}

def enter_s0idle_on_command():
    """
    Unfinished temporary function
    """
    pass

def _s0_init_planned(scanner: str) -> Dict[str, Any]:
    planned = _load_planned(scanner)
    if _is_loc_ok(planned):
        return planned

    true_loc = _load_true(scanner)
    if not _is_loc_ok(true_loc):
        raise ValueError("true_location_json invalid")

    ok, reason = _is_anchor_fresh(scanner)
    if not ok:
        raise ValueError(reason)

    planned = {
        "location_ok": True,
        "x_m": float(true_loc["x_m"]),
        "y_m": float(true_loc["y_m"]),
        "heading_deg": _deg_norm_360(float(true_loc["heading_deg"])),
    }

    _save_planned(scanner, planned)
    return planned


# ===== s1waiting_report =====

def s1waiting_report(scanner: str) -> Dict[str, Any]:
    ok, reason = _s1_has_fresh_report(scanner)

    if not ok:
        _hset_many(
            key_state(scanner),
            {
                "state_detail": f"s1 waiting: {reason}",
                "state_updated_at": utility.local_ts(),
            },
        )
        return {
            "status": "waiting",
            "state": S1_WAITING_REPORT,
            "detail": reason,
        }

    _set_state(scanner, S2_EVALUATING_POLICY, "fresh report received")
    return {
        "status": "ok",
        "state": S2_EVALUATING_POLICY,
        "detail": "fresh report received",
    }

def enter_s1waiting_report_on_report():
    """
    Unfinished temporary function
    """
    pass

def _s1_has_fresh_report(scanner: str) -> tuple[bool, str]:
    report_ts = _hget(key_time(scanner), "last_mobility_report_at", "")
    issued_ts = _hget(key_time(scanner), "last_planned_command_issued_at", "")

    if not issued_ts:
        return False, "missing last_planned_command_issued_at"

    if not report_ts:
        return False, "missing last_mobility_report_at"

    if report_ts < issued_ts:
        return False, "report not fresh yet"

    return True, ""

# ===== s2evaluating_policy =====

def s2evaluating_policy(scanner: str) -> Dict[str, Any]:
    result = _s2_evaluate_policy(scanner)

    transition_to = result["transition_to"]
    _set_state(scanner, transition_to, result["detail"])

    return {
        "state": transition_to,
        "status": result["status"],
        "detail": result["detail"],
    }

def _s2_evaluate_policy(scanner: str) -> Dict[str, Any]:
    report = _load_report_json(scanner)

    last_exec_status = str(report.get("last_exec_status") or "").strip()
    last_error_code = str(report.get("last_error_code") or "").strip()
    last_error_detail = str(report.get("last_error_detail") or "").strip()

    state_hash = key_state(scanner)
    old_retry_count = int(_hget(state_hash, "retry_count", "0") or "0")
    old_collision_veto_count = int(_hget(state_hash, "collision_veto_count", "0") or "0")
    old_busy_count = int(_hget(state_hash, "busy_count", "0") or "0")
    old_exec_fail_count = int(_hget(state_hash, "exec_fail_count", "0") or "0")

    out = {
        "last_error_code": last_error_code,
        "last_error_detail": last_error_detail[:300],
        "need_location_retry": "false",
        "stop_experiment": "false",
        "stop_reason": "",
        "robot_safety_state": "NORMAL",
        "retry_count": str(old_retry_count),
        "collision_veto_count": str(old_collision_veto_count),
        "busy_count": str(old_busy_count),
        "exec_fail_count": str(old_exec_fail_count),
    }

    # Healthy path
    if not last_error_code and last_exec_status.lower() in ("completed", "accepted", "ok", ""):
        out.update({
            "need_location_retry": "false",
            "stop_experiment": "false",
            "stop_reason": "",
            "robot_safety_state": "NORMAL",
            "retry_count": "0",
            "collision_veto_count": "0",
            "busy_count": "0",
            "exec_fail_count": "0",
        })
        _hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "ok",
            "transition_to": S3_SOLVING_TRUE_LOCATION,
            "detail": "healthy mobility report",
        }

    # Immediate stop
    if last_error_code in IMMEDIATE_STOP_ERROR_CODES:
        out.update({
            "need_location_retry": "false",
            "stop_experiment": "true",
            "stop_reason": last_error_code,
            "robot_safety_state": "UNSAFE_STOP",
        })
        _hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": f"immediate stop due to {last_error_code}",
        }

    # Busy
    if last_error_code == "MOBILITY_BUSY":
        busy_count = old_busy_count + 1
        out.update({
            "busy_count": str(busy_count),
            "need_location_retry": "false",
            "stop_experiment": "true" if busy_count >= BUSY_STOP_THRESHOLD else "false",
            "stop_reason": "MOBILITY_BUSY_PERSISTENT" if busy_count >= BUSY_STOP_THRESHOLD else "",
            "robot_safety_state": "UNSAFE_STOP" if busy_count >= BUSY_STOP_THRESHOLD else "WAITING_PREVIOUS_MOTION",
        })
        _hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "stop" if busy_count >= BUSY_STOP_THRESHOLD else "wait",
            "transition_to": S7_STOPPED if busy_count >= BUSY_STOP_THRESHOLD else S1_WAITING_REPORT,
            "detail": f"MOBILITY_BUSY count={busy_count}",
        }

    # Collision vetoes
    if last_error_code in COLLISION_ERROR_CODES:
        veto_count = old_collision_veto_count + 1
        out.update({
            "collision_veto_count": str(veto_count),
            "need_location_retry": "true",
            "stop_experiment": "true" if veto_count >= COLLISION_VETO_STOP_THRESHOLD else "false",
            "stop_reason": f"{last_error_code}_PERSISTENT" if veto_count >= COLLISION_VETO_STOP_THRESHOLD else "",
            "robot_safety_state": "UNSAFE_STOP" if veto_count >= COLLISION_VETO_STOP_THRESHOLD else "COLLISION_BLOCKED",
        })
        _hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "stop" if veto_count >= COLLISION_VETO_STOP_THRESHOLD else "retry",
            "transition_to": S7_STOPPED if veto_count >= COLLISION_VETO_STOP_THRESHOLD else S4_WAITING_LOCATION_RETRY,
            "detail": f"{last_error_code} count={veto_count}",
        }

    # Recoverable failures
    if last_error_code in RECOVERABLE_ERROR_CODES:
        retry_count = old_retry_count + 1
        exec_fail_count = old_exec_fail_count

        if last_error_code in ("MOVE_EXEC_FAIL", "TURN_EXEC_FAIL"):
            exec_fail_count += 1

        stop = (retry_count >= LOCATION_RETRY_LIMIT) or (exec_fail_count >= EXEC_FAIL_STOP_THRESHOLD)

        out.update({
            "retry_count": str(retry_count),
            "exec_fail_count": str(exec_fail_count),
            "need_location_retry": "true",
            "stop_experiment": "true" if stop else "false",
            "stop_reason": (
                f"{last_error_code}_REPEATED" if exec_fail_count >= EXEC_FAIL_STOP_THRESHOLD
                else f"{last_error_code}_RETRY_EXCEEDED" if retry_count >= LOCATION_RETRY_LIMIT
                else ""
            ),
            "robot_safety_state": "UNSAFE_STOP" if stop else "LOCATION_RECOVERY_NEEDED",
        })
        _hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "stop" if stop else "retry",
            "transition_to": S7_STOPPED if stop else S4_WAITING_LOCATION_RETRY,
            "detail": f"{last_error_code} retry_count={retry_count} exec_fail_count={exec_fail_count}",
        }

    # Unknown error code -> conservative stop
    if last_error_code:
        out.update({
            "need_location_retry": "false",
            "stop_experiment": "true",
            "stop_reason": f"UNKNOWN_ERROR_CODE:{last_error_code}",
            "robot_safety_state": "UNSAFE_STOP",
        })
        _hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": f"unknown error code {last_error_code}",
        }

    # Strange status but no error code -> retry once
    retry_count = old_retry_count + 1
    stop = retry_count >= LOCATION_RETRY_LIMIT

    out.update({
        "retry_count": str(retry_count),
        "need_location_retry": "true",
        "stop_experiment": "true" if stop else "false",
        "stop_reason": "STATUS_RETRY_EXCEEDED" if stop else "",
        "robot_safety_state": "UNSAFE_STOP" if stop else "LOCATION_RECOVERY_NEEDED",
    })
    _hset_many(state_hash, out)
    _save_policy_time(scanner)
    return {
        "status": "stop" if stop else "retry",
        "transition_to": S7_STOPPED if stop else S4_WAITING_LOCATION_RETRY,
        "detail": f"unexpected status '{last_exec_status}'",
    }

# ===== s3solving_true_location =====

def s3solving_true_location(scanner: str) -> Dict[str, Any]:
    loc = _s3_solve_true_location(scanner)

    # Case A: AprilTag solve succeeds -> trust it fully
    if loc.get("location_ok") is True:
        _s3_save_true_location(scanner, loc)
        _update_10s_report(scanner)
        _set_state(scanner, S5_COMPUTING_CORRECTION, "true location solved")
        return {
            "state": S5_COMPUTING_CORRECTION,
            "status": "ok",
            "detail": "true location solved",
            "true_location": loc,
        }

    # Case B: no AprilTag solve -> propagate true_location by LAST ISSUED command
    if _should_propagate_true(scanner):
        propagated = _propagate_true_by_last_command(scanner)
        if _is_loc_ok(propagated):
            _hset_many(
                key_state(scanner),
                {
                    "true_propagation_applied": "true",
                    "true_propagation_detail": loc.get("detail", ""),
                    "true_propagation_time": utility.local_ts(),
                },
            )
            _update_10s_report(scanner)
            _set_state(scanner, S0_IDLE, "no apriltag update, true propagated")
            return {
                "state": S0_IDLE,
                "status": "ok",
                "detail": f"no apriltag update, true propagated: {loc.get('detail', '')}",
                "true_location": propagated,
            }

    # Case C: cannot solve and cannot propagate -> retry path
    _hset_many(
        key_state(scanner),
        {
            "true_propagation_applied": "false",
            "true_propagation_detail": loc.get("detail", ""),
            "true_propagation_time": utility.local_ts(),
        },
    )
    _set_state(scanner, S4_WAITING_LOCATION_RETRY, f"solve failed: {loc.get('detail', '')}")
    return {
        "state": S4_WAITING_LOCATION_RETRY,
        "status": "retry",
        "detail": loc.get("detail", ""),
        "true_location": loc,
    }

def _s3_solve_true_location(scanner: str) -> Dict[str, Any]:
    tag_map = _load_tag_map()
    tags_map = tag_map.get("tags") or {}
    if not isinstance(tags_map, dict):
        return {
            "location_ok": False,
            "detail": "invalid tag_map_json",
            "tags_used": [],
            "tag_count": 0,
            "solver_stage": "single_tag",
            "source": "apriltag",
            "updated_at": utility.local_ts(),
        }

    visible = _s3_extract_visible_tags(scanner)
    if not visible:
        return {
            "location_ok": False,
            "detail": "no usable apriltag observation in latest report",
            "tags_used": [],
            "tag_count": 0,
            "solver_stage": "single_tag",
            "source": "apriltag",
            "updated_at": utility.local_ts(),
        }

    cands = []
    for obs in visible:
        tag_world = tags_map.get(str(obs["id"]))
        if not isinstance(tag_world, dict):
            continue
        try:
            cands.append(_s3_solve_single_tag(obs, tag_world))
        except Exception:
            continue

    if not cands:
        return {
            "location_ok": False,
            "detail": "visible tags not found in tag_map_json",
            "tags_used": [],
            "tag_count": 0,
            "solver_stage": "single_tag",
            "source": "apriltag",
            "updated_at": utility.local_ts(),
        }

    return _s3_fuse_candidates(cands)

def _s3_extract_visible_tags(scanner: str) -> list[Dict[str, Any]]:
    report = _load_report_json(scanner)

    loc = report.get("last_location_result") or {}
    if not isinstance(loc, dict) or not bool(loc.get("ok")):
        return []

    apr = loc.get("apriltag") or {}
    if not isinstance(apr, dict) or not bool(apr.get("ok")):
        return []

    tags = apr.get("tags") or []
    if not isinstance(tags, list):
        return []

    out = []
    for t in tags:
        if not isinstance(t, dict):
            continue
        try:
            out.append({
                "id": int(t["id"]),
                "distance_m": float(t["distance_m"]),
                "angle_deg_cw": float(t["angle_deg"]),
                "yaw_deg": float(t["yaw_deg"]),
            })
        except Exception:
            continue
    return out

def _s3_solve_single_tag(obs: Dict[str, Any], tag_world: Dict[str, Any]) -> Dict[str, Any]:
    tag_x = float(tag_world["x_m"])
    tag_y = float(tag_world["y_m"])
    tag_yaw_world = float(tag_world["yaw_deg"])

    distance_m = float(obs["distance_m"])
    angle_deg_cw = float(obs["angle_deg_cw"])
    yaw_deg = float(obs["yaw_deg"])

    # Robot report angle is clockwise-positive, world is CCW-positive
    bearing_robot_to_tag_deg_ccw = -angle_deg_cw

    # heading estimate
    heading_deg = _deg_norm_360(tag_yaw_world + 180.0 - yaw_deg)

    # world bearing robot -> tag
    world_bearing_deg = _deg_norm_360(heading_deg + bearing_robot_to_tag_deg_ccw)
    world_bearing_rad = _deg_to_rad(world_bearing_deg)

    robot_x = tag_x - distance_m * math.cos(world_bearing_rad)
    robot_y = tag_y - distance_m * math.sin(world_bearing_rad)

    return {
        "tag_id": int(obs["id"]),
        "x_m": robot_x,
        "y_m": robot_y,
        "heading_deg": heading_deg,
        "distance_m": distance_m,
        "angle_deg_cw": angle_deg_cw,
        "yaw_deg": yaw_deg,
    }

def _s3_candidate_pos_distance(c0: Dict[str, Any], c1: Dict[str, Any]) -> float:
    return math.hypot(float(c0["x_m"]) - float(c1["x_m"]), float(c0["y_m"]) - float(c1["y_m"]))

def _s3_candidate_heading_distance(c0: Dict[str, Any], c1: Dict[str, Any]) -> float:
    return abs(_angle_diff_deg(float(c0["heading_deg"]), float(c1["heading_deg"])))

def _s3_fuse_candidates(cands: list[Dict[str, Any]]) -> Dict[str, Any]:
    if not cands:
        return {
            "location_ok": False,
            "detail": "no candidates",
            "tags_used": [],
            "tag_count": 0,
            "solver_stage": "single_tag",
            "source": "apriltag",
            "updated_at": utility.local_ts(),
        }

    if len(cands) == 1:
        c = cands[0]
        return {
            "location_ok": True,
            "x_m": c["x_m"],
            "y_m": c["y_m"],
            "heading_deg": _deg_norm_360(c["heading_deg"]),
            "source": "apriltag",
            "tags_used": [c["tag_id"]],
            "tag_count": 1,
            "solver_stage": "single_tag",
            "updated_at": utility.local_ts(),
            "detail": "",
            "candidates": cands,
        }

    if len(cands) == 2:
        pos_d = _s3_candidate_pos_distance(cands[0], cands[1])
        hdg_d = _s3_candidate_heading_distance(cands[0], cands[1])

        if pos_d > MULTI_TAG_POS_THRESH_M or hdg_d > MULTI_TAG_HEADING_THRESH_DEG:
            return {
                "location_ok": False,
                "source": "apriltag",
                "tags_used": [c["tag_id"] for c in cands],
                "tag_count": 2,
                "solver_stage": "multi_tag",
                "updated_at": utility.local_ts(),
                "detail": f"two-tag candidates inconsistent: pos_d={pos_d:.3f}m hdg_d={hdg_d:.3f}deg",
                "candidates": cands,
            }

        return {
            "location_ok": True,
            "x_m": (cands[0]["x_m"] + cands[1]["x_m"]) / 2.0,
            "y_m": (cands[0]["y_m"] + cands[1]["y_m"]) / 2.0,
            "heading_deg": _circular_mean_deg([cands[0]["heading_deg"], cands[1]["heading_deg"]]),
            "source": "apriltag",
            "tags_used": [c["tag_id"] for c in cands],
            "tag_count": 2,
            "solver_stage": "multi_tag",
            "updated_at": utility.local_ts(),
            "detail": "",
            "candidates": cands,
        }

    # 3+ tags
    xs = sorted(float(c["x_m"]) for c in cands)
    ys = sorted(float(c["y_m"]) for c in cands)

    def _median(vals: list[float]) -> float:
        n = len(vals)
        if n % 2 == 1:
            return vals[n // 2]
        return 0.5 * (vals[n // 2 - 1] + vals[n // 2])

    med_x = _median(xs)
    med_y = _median(ys)
    mean_h = _circular_mean_deg([float(c["heading_deg"]) for c in cands])

    kept = []
    rejected = []

    for c in cands:
        pos_d = math.hypot(float(c["x_m"]) - med_x, float(c["y_m"]) - med_y)
        hdg_d = abs(_angle_diff_deg(float(c["heading_deg"]), mean_h))
        if pos_d <= MULTI_TAG_POS_THRESH_M and hdg_d <= MULTI_TAG_HEADING_THRESH_DEG:
            kept.append(c)
        else:
            rejected.append(c)

    if not kept:
        return {
            "location_ok": False,
            "source": "apriltag",
            "tags_used": [],
            "tag_count": 0,
            "solver_stage": "multi_tag",
            "updated_at": utility.local_ts(),
            "detail": "all multi-tag candidates rejected",
            "candidates": cands,
            "rejected_candidates": rejected,
        }

    return {
        "location_ok": True,
        "x_m": sum(c["x_m"] for c in kept) / len(kept),
        "y_m": sum(c["y_m"] for c in kept) / len(kept),
        "heading_deg": _circular_mean_deg([c["heading_deg"] for c in kept]),
        "source": "apriltag",
        "tags_used": [c["tag_id"] for c in kept],
        "tag_count": len(kept),
        "solver_stage": "multi_tag" if len(kept) > 1 else "single_tag_after_rejection",
        "updated_at": utility.local_ts(),
        "detail": "" if not rejected else f"rejected {len(rejected)} outlier candidate(s)",
        "candidates": cands,
        "rejected_candidates": rejected,
    }

def _s3_save_true_location(scanner: str, loc: Dict[str, Any]) -> None:
    _hset_many(
        key_pose(scanner),
        {
            "true_location_json": loc,
        },
    )
    _hset_many(
        key_time(scanner),
        {
            "true_location_updated_at": utility.local_ts(),
        },
    )


# ===== s4waiting_location_retry =====

def s4waiting_location_retry(scanner: str) -> Dict[str, Any]:
    result = _s4_handle_location_retry(scanner)

    transition_to = result["transition_to"]
    _set_state(scanner, transition_to, result["detail"])

    return {
        "state": transition_to,
        "status": result["status"],
        "detail": result["detail"],
        **({"last_retry_requested_at": result["last_retry_requested_at"]} if "last_retry_requested_at" in result else {}),
    }

def _s4_handle_location_retry(scanner: str) -> Dict[str, Any]:
    state_hash = key_state(scanner)
    time_hash = key_time(scanner)

    retry_count = _to_int(_hget(state_hash, "retry_count", "0"), 0)
    stop_experiment = (_hget(state_hash, "stop_experiment", "false").lower() == "true")
    stop_reason = _hget(state_hash, "stop_reason", "")
    need_location_retry = (_hget(state_hash, "need_location_retry", "false").lower() == "true")

    # If policy already says stop, go straight to stopped
    if stop_experiment:
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": stop_reason or "policy requested stop",
        }

    # If no retry is needed anymore, return to policy phase caller flow
    if not need_location_retry:
        return {
            "status": "ok",
            "transition_to": S0_IDLE,
            "detail": "no location retry needed",
        }

    # Retry still allowed
    if retry_count < LOCATION_RETRY_LIMIT:
        now_ts = utility.local_ts()
        _hset_many(
            time_hash,
            {
                "last_retry_requested_at": now_ts,
            },
        )
        return {
            "status": "retry",
            "transition_to": S4_WAITING_LOCATION_RETRY,
            "detail": f"location retry allowed, retry_count={retry_count}",
            "last_retry_requested_at": now_ts,
        }

    # Defensive fallback: retry exhausted -> stop
    return {
        "status": "stop",
        "transition_to": S7_STOPPED,
        "detail": "location retry exhausted",
    }

# ===== s5computing_correction =====

def s5computing_correction(scanner: str) -> Dict[str, Any]:
    result = _s5_compute_correction(scanner)

    _set_state(scanner, result["transition_to"], result["detail"])

    return {
        "state": result["transition_to"],
        "status": result["status"],
        "detail": result["detail"],
        "error": result["error"],
        "pending_sequence": result["pending_sequence"],
        **({"correction_detail": result["correction_detail"]} if "correction_detail" in result else {}),
    }

def _s5_compute_correction(scanner: str) -> Dict[str, Any]:
    
    if _get_correction_counter(scanner) >= 1:
        _clear_pending_sequence(scanner)
        return {
            "status": "ok",
            "transition_to": S0_IDLE,
            "detail": "correction already attempted, skip further correction",
            "error": {},
            "pending_sequence": [],
        }

    true_loc = _load_true(scanner)
    planned_loc = _load_planned(scanner)

    if not _is_loc_ok(true_loc):
        _clear_pending_sequence(scanner)
        return {
            "status": "ok",
            "transition_to": S0_IDLE,
            "detail": "true_location unavailable, skip correction",
            "error": {},
            "pending_sequence": [],
        }

    if not _is_loc_ok(planned_loc):
        _clear_pending_sequence(scanner)
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": "planned_location_json invalid",
            "error": {},
            "pending_sequence": [],
        }

    err = _pose_error(true_loc, planned_loc)

    pos_ignore = float(config.MOBILITY_POS_IGNORE_THRESH_M)
    pos_correct = float(config.MOBILITY_POS_CORRECT_THRESH_M)
    pos_max = float(config.MOBILITY_POS_CORRECT_MAX_M)

    ang_ignore = float(config.MOBILITY_ANGLE_IGNORE_THRESH_DEG)
    ang_max = float(config.MOBILITY_ANGLE_CORRECT_MAX_DEG)

    dpos = abs(float(err["dpos_m"]))
    dhead = abs(float(err["dhead_deg"]))

    # Case 1: no correction needed
    if dpos <= pos_ignore and dhead <= ang_ignore:
        _clear_pending_sequence(scanner)
        return {
            "status": "ok",
            "transition_to": S0_IDLE,
            "detail": "correction not needed",
            "error": err,
            "pending_sequence": [],
        }

    # Case 2: heading-only correction
    if dpos <= pos_ignore and dhead > ang_ignore:
        if dhead > ang_max:
            _clear_pending_sequence(scanner)
            return {
                "status": "stop",
                "transition_to": S7_STOPPED,
                "detail": f"heading correction too large: {dhead:.3f} deg",
                "error": err,
                "pending_sequence": [],
            }

        seq = _build_turn_only_command(true_loc, planned_loc)
        _save_pending_sequence(scanner, seq, "heading_only")
        return {
            "status": "ok",
            "transition_to": S6_ISSUING_CORRECTION,
            "detail": "heading-only correction prepared",
            "error": err,
            "pending_sequence": seq,
        }

    # Case 3: full correction
    if dpos > pos_correct:
        if dpos > pos_max:
            _clear_pending_sequence(scanner)
            return {
                "status": "stop",
                "transition_to": S7_STOPPED,
                "detail": f"position correction too large: {dpos:.3f} m",
                "error": err,
                "pending_sequence": [],
            }

        seq, corr_detail = _build_turn_move_turn_forward_command(true_loc, planned_loc)

        tx = float(true_loc["x_m"])
        ty = float(true_loc["y_m"])
        px = float(planned_loc["x_m"])
        py = float(planned_loc["y_m"])

        path_clear, blocked = _is_path_clear(tx, ty, px, py, exclude_scanner=scanner)

        if not path_clear:
            _clear_pending_sequence(scanner)
            return {
                "status": "stop",
                "transition_to": S7_STOPPED,
                "detail": f"correction path unsafe, blocked_cells={len(blocked)}",
                "error": err,
                "pending_sequence": [],
            }

        _save_pending_sequence(scanner, seq, "full_correction")
        return {
            "status": "ok",
            "transition_to": S6_ISSUING_CORRECTION,
            "detail": "full correction prepared",
            "error": err,
            "pending_sequence": seq,
            "correction_detail": corr_detail,
        }

    # Case 4: small residual error not worth correcting
    _clear_pending_sequence(scanner)
    return {
        "status": "ok",
        "transition_to": S0_IDLE,
        "detail": "correction not needed (small residual error)",
        "error": err,
        "pending_sequence": [],
    }


# ===== s6issuing_correction =====

def s6issuing_correction(scanner: str) -> Dict[str, Any]:
    result = _s6_issue_correction(scanner)

    _set_state(scanner, result["transition_to"], result["detail"])

    return {
        "state": result["transition_to"],
        "status": result["status"],
        "detail": result["detail"],
        "issued_command": result["issued_command"],
        **({"remaining_count": result["remaining_count"]} if "remaining_count" in result else {}),
        **({"issued_at": result["issued_at"]} if "issued_at" in result else {}),
        **({"new_planned": result["new_planned"]} if "new_planned" in result else {}),
        **({"new_true": result["new_true"]} if "new_true" in result else {}),
    }

def _s6_issue_correction(scanner: str) -> Dict[str, Any]:
    seq = _load_pending_sequence(scanner)

    if not seq:
        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        return {
            "status": "ok",
            "transition_to": S0_IDLE,
            "detail": "no pending correction command",
            "issued_command": {},
        }

    cmd = seq[0]
    rest = seq[1:]

    if not isinstance(cmd, dict):
        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": "invalid pending correction command",
            "issued_command": {},
        }

    action = str(cmd.get("action") or "").strip()
    args = cmd.get("args") or {}

    try:
        action, args = _normalize_mobility_command(action, args)
    except Exception as e:
        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": f"invalid correction command: {e}",
            "issued_command": {},
        }

    # ---------------------------------------------------------
    # Update planned immediately (correct behavior)
    # ---------------------------------------------------------
    planned = _load_planned(scanner)
    if not _is_loc_ok(planned):
        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": "planned_location_json invalid while issuing correction",
            "issued_command": {},
        }

    new_planned = _apply_mobility_command_to_pose(planned, action, args)
    _save_planned(scanner, new_planned)

    # ---------------------------------------------------------
    # ❌ DO NOT update true_location here anymore
    # true_location will be updated AFTER report in s3
    # ---------------------------------------------------------
    new_true = {}

    # ---------------------------------------------------------
    # Save queue-preview (for debug)
    # ---------------------------------------------------------
    _save_outgoing_command_preview(scanner, action, args, "correction")

    # ---------------------------------------------------------
    # Record issued command (this is what s3 will use!)
    # ---------------------------------------------------------
    issued_at = _save_last_issued_command(scanner, action, args)

    # ---------------------------------------------------------
    # 🔥 Increment correction counter (NEW)
    # ---------------------------------------------------------
    count = _inc_correction_counter(scanner)

    _hset_many(
        key_state(scanner),
        {
            "correction_attempt_count": str(count),
            "last_correction_issued_at": issued_at,
        },
    )

    # ---------------------------------------------------------
    # Save remaining sequence
    # ---------------------------------------------------------
    _save_pending_sequence(scanner, rest, "remaining_after_issue")

    return {
        "status": "ok",
        "transition_to": S1_WAITING_REPORT,
        "detail": f"issued correction command {action}",
        "issued_command": {
            "action": action,
            "args": args,
        },
        "remaining_count": len(rest),
        "issued_at": issued_at,
        "new_planned": new_planned,
        "new_true": new_true,  # always empty now
        "correction_count": count,
    }


# ===== s7stopped =====

def s7stopped(scanner: str) -> Dict[str, Any]:
    stop = _load_stop()
    reason = str(stop.get("reason") or "").strip()

    _hset_many(
        key_state(scanner),
        {
            "state_detail": f"stopped: {reason}" if reason else "stopped",
            "state_updated_at": utility.local_ts(),
        },
    )

    return {
        "state": S7_STOPPED,
        "status": "stopped",
        "detail": reason if reason else "manual reset required",
    }

