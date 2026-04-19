"""
Mobility subsystem command model helpers.

Division rule:
- Mobility command normalization, command derivation, and propagation-by-last-command live here.
- No state transitions here.
"""
from typing import Dict, Any
import math

from utility import _hget, _hget_json, _wrap_angle_deg, _deg_norm_360

from m8mobility_map import _is_path_clear
from m8mobility_state_store import key_pose, key_time, _load_true, _load_planned, _save_true, _is_loc_ok
from m8mobility_pose import _apply_mobility_command_to_pose

# ===== command normalization =====

def _normalize_mobility_command(action: str, args: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
    """
    New command set only:
      - mobility.turn
      - mobility.turn_move_turn.forward
      - mobility.turn_move_turn.backward
      - mobility.report.location
    """
    action = (action or "").strip()
    args = args or {}

    if action == "mobility.turn":
        return action, {
            "angle_deg": float(args["angle_deg"]),
        }

    if action == "mobility.turn_move_turn.forward":
        return action, {
            "pre_angle": float(args["pre_angle"]),
            "distance_m": float(args["distance_m"]),
            "post_angle": float(args["post_angle"]),
        }

    if action == "mobility.turn_move_turn.backward":
        return action, {
            "pre_angle": float(args["pre_angle"]),
            "distance_m": float(args["distance_m"]),
            "post_angle": float(args["post_angle"]),
        }

    if action == "mobility.report.location":
        return action, {}

    raise ValueError(f"unsupported mobility action: {action}")


# ===== command builders from pose relations =====

def _build_turn_only_command(true_loc: Dict[str, Any], planned_loc: Dict[str, Any]) -> list[Dict[str, Any]]:
    dhead = _wrap_angle_deg(float(planned_loc["heading_deg"]) - float(true_loc["heading_deg"]))
    if abs(dhead) <= 1e-9:
        return []

    return [
        {
            "action": "mobility.turn",
            "args": {
                "angle_deg": float(dhead)
            }
        }
    ]

def _build_turn_move_turn_forward_command(true_loc: Dict[str, Any], planned_loc: Dict[str, Any]) -> tuple[list[Dict[str, Any]], Dict[str, Any]]:
    tx = float(true_loc["x_m"])
    ty = float(true_loc["y_m"])
    th = float(true_loc["heading_deg"])

    px = float(planned_loc["x_m"])
    py = float(planned_loc["y_m"])
    ph = float(planned_loc["heading_deg"])

    dx = px - tx
    dy = py - ty
    dpos = math.hypot(dx, dy)

    travel_heading = _deg_norm_360(math.degrees(math.atan2(dy, dx)))
    pre_angle = _wrap_angle_deg(travel_heading - th)
    post_angle = _wrap_angle_deg(ph - travel_heading)

    cmd = {
        "action": "mobility.turn_move_turn.forward",
        "args": {
            "pre_angle": float(pre_angle),
            "distance_m": float(dpos),
            "post_angle": float(post_angle),
        }
    }

    return [cmd], {
        "travel_heading_deg": travel_heading,
        "distance_m": dpos,
        "pre_angle": pre_angle,
        "post_angle": post_angle,
    }

def _build_command_from_true_to_planned(scanner: str) -> tuple[str, Dict[str, Any], Dict[str, Any]]:
    true_loc = _load_true(scanner)
    planned = _load_planned(scanner)

    debug = {}

    if not _is_loc_ok(true_loc):
        # fallback: use planned as start (ONLY if true does not exist at all)
        start = planned
        debug["start_source"] = "planned"
    else:
        start = true_loc
        debug["start_source"] = "true"

    tx = float(start["x_m"])
    ty = float(start["y_m"])
    th = float(start["heading_deg"])

    px = float(planned["x_m"])
    py = float(planned["y_m"])
    ph = float(planned["heading_deg"])

    dx = px - tx
    dy = py - ty

    distance = math.hypot(dx, dy)

    travel_heading = _deg_norm_360(math.degrees(math.atan2(dy, dx)))
    pre_angle = _wrap_angle_deg(travel_heading - th)
    post_angle = _wrap_angle_deg(ph - travel_heading)

    debug.update({
        "distance": distance,
        "pre_angle": pre_angle,
        "post_angle": post_angle,
    })

    # thresholds
    DIST_EPS = 0.05
    ANG_EPS = 1.0

    # Case 1: pure turn
    if distance < DIST_EPS:
        if abs(post_angle) < ANG_EPS:
            # dummy command
            return "mobility.turn", {"angle_deg": 0.0}, debug

        return "mobility.turn", {"angle_deg": float(_wrap_angle_deg(ph - th))}, debug

    # Case 2: normal motion
    return "mobility.turn_move_turn.forward", {
        "pre_angle": float(pre_angle),
        "distance_m": float(distance),
        "post_angle": float(post_angle),
    }, debug


# ===== motion-path validation helper =====

def _check_motion_path(scanner: str, start, planned) -> bool:
    tx = float(start["x_m"])
    ty = float(start["y_m"])
    px = float(planned["x_m"])
    py = float(planned["y_m"])

    ok, _ = _is_path_clear(tx, ty, px, py, exclude_scanner=scanner)
    return ok


# ===== true-propagation helpers =====

def _should_propagate_true(scanner: str) -> bool:
    """
    Ensure propagation happens EXACTLY ONCE per report.
    """
    report_ts = _hget(key_time(scanner), "last_mobility_report_at", "")
    true_ts = _hget(key_time(scanner), "true_location_updated_at", "")

    if not report_ts:
        return False

    if not true_ts:
        return True

    return true_ts < report_ts

def _propagate_true_by_last_command(scanner: str) -> Dict[str, Any]:
    """
    Apply the LAST ISSUED command to true_location.
    Only used when NO AprilTag update is available.

    Must be applied exactly once per report.
    """
    true_loc = _load_true(scanner)
    if not _is_loc_ok(true_loc):
        return {}

    action = _hget(key_pose(scanner), "last_planned_command_action", "")
    args = _hget_json(key_pose(scanner), "last_planned_command_args_json")

    if not action:
        return {}

    try:
        action, args = _normalize_mobility_command(action, args)
    except Exception:
        return {}
    
    new_true = _apply_mobility_command_to_pose(true_loc, action, args)
    _save_true(scanner, new_true)

    return new_true


# ===== any circular/angle solver =====

def _angle_diff_deg(a: float, b: float) -> float:
    d = (a - b + 180.0) % 360.0 - 180.0
    return d

def _circular_mean_deg(vals: list[float]) -> float:
    if not vals:
        raise ValueError("empty heading list")
    sx = 0.0
    sy = 0.0
    for v in vals:
        r = math.radians(v)
        sx += math.cos(r)
        sy += math.sin(r)
    if abs(sx) < 1e-12 and abs(sy) < 1e-12:
        return _deg_norm_360(vals[0])
    return _deg_norm_360(math.degrees(math.atan2(sy, sx)))

