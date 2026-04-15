"""
Mobility subsystem pose helpers.

Division rule:
- Helpers for true/planned pose persistence and pose propagation live here.
- No state transitions or policy decisions here.
"""
from typing import Dict, Any
import math
from m8mobility_command_model import _normalize_mobility_command
import utility
from utility import _deg_norm_360, _deg_to_rad, _wrap_angle_deg, _hget_json, _hset_many
from m8mobility_state_store import key_pose, key_time

# ===== load/save pose =====

def _load_true(scanner: str) -> Dict[str, Any]:
    return _hget_json(key_pose(scanner), "true_location_json")

def _load_planned(scanner: str) -> Dict[str, Any]:
    return _hget_json(key_pose(scanner), "planned_location_json")

def _save_true(scanner: str, loc: Dict[str, Any]) -> None:
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

def _save_planned(scanner: str, loc: Dict[str, Any]) -> None:
    _hset_many(
        key_pose(scanner),
        {
            "planned_location_json": loc,
        },
    )
    _hset_many(
        key_time(scanner),
        {
            "planned_location_updated_at": utility.local_ts(),
        },
    )


# ===== pose validity check =====

def _is_loc_ok(loc: Dict[str, Any]) -> bool:
    return isinstance(loc, dict) and loc.get("location_ok") is True


# ===== pose comparison =====

def _pose_error(true_loc: Dict[str, Any], planned_loc: Dict[str, Any]) -> Dict[str, Any]:
    dx = float(planned_loc["x_m"]) - float(true_loc["x_m"])
    dy = float(planned_loc["y_m"]) - float(true_loc["y_m"])
    dpos = math.hypot(dx, dy)

    true_h = float(true_loc["heading_deg"])
    planned_h = float(planned_loc["heading_deg"])
    dhead = _wrap_angle_deg(planned_h - true_h)

    return {
        "dx_m": dx,
        "dy_m": dy,
        "dpos_m": dpos,
        "dhead_deg": dhead,
    }


# ===== motion application =====

def _apply_turn(loc: Dict[str, Any], angle_deg: float) -> Dict[str, Any]:
    return {
        "location_ok": True,
        "x_m": float(loc["x_m"]),
        "y_m": float(loc["y_m"]),
        "heading_deg": _deg_norm_360(float(loc["heading_deg"]) + float(angle_deg)),
    }

def _apply_turn_move_turn(loc: Dict[str, Any], pre_angle: float, distance_m: float, post_angle: float, forward: bool) -> Dict[str, Any]:
    x0 = float(loc["x_m"])
    y0 = float(loc["y_m"])
    h0 = float(loc["heading_deg"])

    h1 = _deg_norm_360(h0 + float(pre_angle))
    rad = _deg_to_rad(h1)

    direction = 1.0 if forward else -1.0
    dx = direction * float(distance_m) * math.cos(rad)
    dy = direction * float(distance_m) * math.sin(rad)

    x1 = x0 + dx
    y1 = y0 + dy
    h2 = _deg_norm_360(h1 + float(post_angle))

    return {
        "location_ok": True,
        "x_m": x1,
        "y_m": y1,
        "heading_deg": h2,
    }

def _apply_mobility_command_to_pose(loc: Dict[str, Any], action: str, args: Dict[str, Any]) -> Dict[str, Any]:
    if not _is_loc_ok(loc):
        raise ValueError("location is not usable")

    action, args = _normalize_mobility_command(action, args)

    if action == "mobility.turn":
        return _apply_turn(loc, args["angle_deg"])

    if action == "mobility.turn_move_turn.forward":
        return _apply_turn_move_turn(
            loc,
            args["pre_angle"],
            args["distance_m"],
            args["post_angle"],
            forward=True,
        )

    if action == "mobility.turn_move_turn.backward":
        return _apply_turn_move_turn(
            loc,
            args["pre_angle"],
            args["distance_m"],
            args["post_angle"],
            forward=False,
        )

    if action == "mobility.report.location":
        return {
            "location_ok": True,
            "x_m": float(loc["x_m"]),
            "y_m": float(loc["y_m"]),
            "heading_deg": float(loc["heading_deg"]),
        }

    raise ValueError(f"unsupported mobility action: {action}")

