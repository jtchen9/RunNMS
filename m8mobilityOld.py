from typing import Dict, Any
import json
import math
from pathlib import Path
import numpy as np
import config
import utility
from fastapi import APIRouter

import config

router = APIRouter()

# ================================
# 8) Mobility (Clean State Machine)
# ================================

# -------- Policy thresholds --------

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


# -------- Redis key helpers --------

def key_state(scanner: str) -> str:
    return f"{config.KEY_PREFIX}scanner:{scanner}:mobility:state"

def key_time(scanner: str) -> str:
    return f"{config.KEY_PREFIX}scanner:{scanner}:mobility:time"

def key_report(scanner: str) -> str:
    return f"{config.KEY_PREFIX}scanner:{scanner}:mobility:report"

def key_pose(scanner: str) -> str:
    return f"{config.KEY_PREFIX}scanner:{scanner}:mobility:pose"

def _wrap_angle_deg(deg: float) -> float:
    d = (deg + 180.0) % 360.0 - 180.0
    return d

def _deg_norm_360(deg: float) -> float:
    x = deg % 360.0
    return x + 360.0 if x < 0 else x

def _deg_to_rad(deg: float) -> float:
    return math.radians(deg)


# -------- Redis helpers --------

def _hgetall(key: str) -> Dict[str, Any]:
    return config.r.hgetall(key) or {}

def _hget(key: str, field: str, default: str = "") -> str:
    data = _hgetall(key)
    return str(data.get(field) or default)

def _hset_many(key: str, mapping: Dict[str, Any]) -> None:
    out = {}
    for k, v in mapping.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        elif v is None:
            out[k] = ""
        else:
            out[k] = str(v)
    config.r.hset(key, mapping=out)

def _hget_json(key: str, field: str) -> Dict[str, Any]:
    raw = _hget(key, field, "")
    if not raw.strip():
        return {}
    try:
        j = json.loads(raw)
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}

def _hset_json(key: str, field: str, value: Dict[str, Any]) -> None:
    _hset_many(key, {field: value})


# -------- State definitions --------

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


# -------- State access --------

def _get_state(scanner: str) -> str:
    s = _hget(key_state(scanner), "state", S0_IDLE)
    return s if s in VALID_STATES else S0_IDLE

def _set_state(scanner: str, state: str, detail: str = "") -> None:
    _hset_many(
        key_state(scanner),
        {
            "state": state,
            "state_updated_at": utility.local_ts(),
            "state_detail": detail[:300],
        },
    )


# -------- Global stop state --------

KEY_STOP = f"{config.KEY_PREFIX}mobility:experiment_stop_state_json"

def _load_stop() -> Dict[str, Any]:
    raw = config.r.get(KEY_STOP) or ""
    if not raw.strip():
        return {"stop": False, "reason": ""}
    try:
        return json.loads(raw)
    except Exception:
        return {"stop": False, "reason": ""}


# -------- Utility functions --------

def _deg_norm_360(deg: float) -> float:
    x = deg % 360.0
    return x + 360.0 if x < 0 else x

def _deg_to_rad(deg: float) -> float:
    return math.radians(deg)

def _is_loc_ok(loc: Dict[str, Any]) -> bool:
    return isinstance(loc, dict) and loc.get("location_ok") is True

def _to_int(s: str, default: int = 0) -> int:
    try:
        return int(s)
    except Exception:
        return default
    

# -------- Load / Save pose --------

def _load_true(scanner: str) -> Dict[str, Any]:
    return _hget_json(key_pose(scanner), "true_location_json")

def _load_planned(scanner: str) -> Dict[str, Any]:
    return _hget_json(key_pose(scanner), "planned_location_json")

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


# -------- Timestamp check --------

def _is_anchor_fresh(scanner: str) -> tuple[bool, str]:
    report_ts = _hget(key_time(scanner), "last_mobility_report_at", "")
    issued_ts = _hget(key_time(scanner), "last_planned_command_issued_at", "")

    if not report_ts:
        return False, "missing last_mobility_report_at"

    if issued_ts and report_ts < issued_ts:
        return False, "stale true_location (report older than last command)"

    return True, ""


# -------- Map helpers --------

def _load_static_map() -> np.ndarray:
    """
    Load the static restriction map from config.MOBILITY_STATIC_RESTRICTION_MAP_NPY.

    Requirements:
    - .npy file
    - square 2-D array
    - uint8 values 0/1
    """
    path = Path(config.MOBILITY_STATIC_RESTRICTION_MAP_NPY)
    if not path.exists():
        raise FileNotFoundError(f"static restriction map not found: {path}")

    arr = np.load(str(path), allow_pickle=False)

    if not isinstance(arr, np.ndarray):
        raise ValueError("static restriction map is not a numpy array")
    if arr.ndim != 2:
        raise ValueError("static restriction map must be 2-D")
    if arr.shape[0] != arr.shape[1]:
        raise ValueError("static restriction map must be square")

    if arr.dtype != np.uint8:
        arr = arr.astype(np.uint8)

    arr = (arr > 0).astype(np.uint8)

    n = int(arr.shape[0])
    expected_res = float(config.MOBILITY_WORLD_SIZE_M) / float(n)
    desired_res = float(config.MOBILITY_GRID_RESOLUTION_M)

    # Loose validation only; keep simple for now.
    if abs(expected_res - desired_res) > 1e-9:
        raise ValueError(
            f"static map resolution mismatch: "
            f"world/grid gives {expected_res:.6f} m, config wants {desired_res:.6f} m"
        )

    return arr


def _grid_size(static_map: np.ndarray) -> int:
    return int(static_map.shape[0])


def _grid_resolution_m(static_map: np.ndarray) -> float:
    return float(config.MOBILITY_WORLD_SIZE_M) / float(_grid_size(static_map))


def _world_to_grid(x_m: float, y_m: float, static_map: np.ndarray) -> tuple[int, int]:
    """
    World frame:
      origin bottom-left
      +x right
      +y up

    Grid:
      row 0 = top
      col 0 = left
    """
    n = _grid_size(static_map)
    res = _grid_resolution_m(static_map)

    col = int(math.floor(x_m / res))
    row_from_bottom = int(math.floor(y_m / res))
    row = n - 1 - row_from_bottom

    col = max(0, min(n - 1, col))
    row = max(0, min(n - 1, row))
    return row, col


def _is_location_ok(loc: Dict[str, Any]) -> bool:
    return isinstance(loc, dict) and loc.get("location_ok") is True


def _rasterize_circle(mask: np.ndarray, x_m: float, y_m: float, radius_m: float) -> None:
    """
    Mark a filled circle into a uint8 map.
    """
    n = int(mask.shape[0])
    res = _grid_resolution_m(mask)

    center_row, center_col = _world_to_grid(x_m, y_m, mask)
    r_cells = max(1, int(math.ceil(radius_m / res)))

    for dr in range(-r_cells, r_cells + 1):
        for dc in range(-r_cells, r_cells + 1):
            rr = center_row + dr
            cc = center_col + dc

            if rr < 0 or rr >= n or cc < 0 or cc >= n:
                continue

            dx = dc * res
            dy = -dr * res  # row increases downward
            if dx * dx + dy * dy <= radius_m * radius_m:
                mask[rr, cc] = 1


def _build_dynamic_map(exclude_scanner: str = "") -> np.ndarray:
    """
    Build dynamic keep-out map from OTHER robots' true/planned locations.
    Uses both true_location_json and planned_location_json (conservative union).
    """
    static_map = _load_static_map()
    dynamic = np.zeros_like(static_map, dtype=np.uint8)

    scanners = sorted(list(config.r.smembers(config.KEY_REGISTRY)))

    for scanner in scanners:
        if exclude_scanner and scanner == exclude_scanner:
            continue

        pose_key = key_pose(scanner)

        true_loc = _hget_json(pose_key, "true_location_json")
        planned_loc = _hget_json(pose_key, "planned_location_json")

        if _is_location_ok(true_loc):
            _rasterize_circle(
                dynamic,
                float(true_loc["x_m"]),
                float(true_loc["y_m"]),
                float(config.MOBILITY_ROBOT_RESTRICT_RADIUS_M),
            )

        if _is_location_ok(planned_loc):
            _rasterize_circle(
                dynamic,
                float(planned_loc["x_m"]),
                float(planned_loc["y_m"]),
                float(config.MOBILITY_ROBOT_RESTRICT_RADIUS_M),
            )

    return dynamic


def _build_effective_map(exclude_scanner: str = "") -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Returns:
      static_map, dynamic_map, effective_map
    """
    static_map = _load_static_map()
    dynamic_map = _build_dynamic_map(exclude_scanner=exclude_scanner)
    effective_map = ((static_map > 0) | (dynamic_map > 0)).astype(np.uint8)
    return static_map, dynamic_map, effective_map


def _sample_path_cells(x0_m: float, y0_m: float, x1_m: float, y1_m: float, static_map: np.ndarray) -> list[tuple[int, int]]:
    """
    Sample a straight path at grid resolution.
    """
    res = _grid_resolution_m(static_map)
    dist = math.hypot(x1_m - x0_m, y1_m - y0_m)

    steps = max(1, int(math.ceil(dist / res)))
    out = []
    seen = set()

    for i in range(steps + 1):
        t = i / steps
        x = x0_m + t * (x1_m - x0_m)
        y = y0_m + t * (y1_m - y0_m)
        rc = _world_to_grid(x, y, static_map)
        if rc not in seen:
            seen.add(rc)
            out.append(rc)

    return out


def _is_path_clear(x0_m: float, y0_m: float, x1_m: float, y1_m: float, exclude_scanner: str = "") -> tuple[bool, list[tuple[int, int]]]:
    """
    Check straight-line path safety against effective restriction map.
    Returns:
      (is_clear, blocked_cells)
    """
    static_map, dynamic_map, effective_map = _build_effective_map(exclude_scanner=exclude_scanner)
    cells = _sample_path_cells(x0_m, y0_m, x1_m, y1_m, static_map)

    blocked = []
    for row, col in cells:
        if effective_map[row, col] != 0:
            blocked.append((row, col))

    return (len(blocked) == 0), blocked


def _is_loc_ok(loc: Dict[str, Any]) -> bool:
    return isinstance(loc, dict) and loc.get("location_ok") is True


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


def _save_pending_sequence(scanner: str, seq: list[Dict[str, Any]], reason: str = "") -> None:
    _hset_many(
        key_state(scanner),
        {
            "pending_sequence_json": seq,
            "pending_sequence_len": str(len(seq)),
            "pending_sequence_reason": reason,
        },
    )


def _clear_pending_sequence(scanner: str) -> None:
    _hset_many(
        key_state(scanner),
        {
            "pending_sequence_json": "",
            "pending_sequence_len": "0",
            "pending_sequence_reason": "",
        },
    )


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


def _load_pending_sequence(scanner: str) -> list[Dict[str, Any]]:
    raw = _hget(key_state(scanner), "pending_sequence_json", "")
    if not raw.strip():
        return []
    try:
        seq = json.loads(raw)
        return seq if isinstance(seq, list) else []
    except Exception:
        return []


def _save_last_issued_command(scanner: str, action: str, args: Dict[str, Any]) -> str:
    ts = utility.local_ts()

    _hset_many(
        key_time(scanner),
        {
            "last_planned_command_issued_at": ts,
        },
    )

    _hset_many(
        key_pose(scanner),
        {
            "last_planned_command_action": action,
            "last_planned_command_args_json": args,
        },
    )

    return ts


def _save_outgoing_command_preview(scanner: str, action: str, args: Dict[str, Any], source: str) -> None:
    """
    Temporary placeholder before real queue hookup.
    Lets you inspect in RedisInsight what command s6 wants to send next.
    """
    _hset_many(
        key_state(scanner),
        {
            "outgoing_command_action": action,
            "outgoing_command_args_json": args,
            "outgoing_command_source": source,
            "outgoing_command_updated_at": utility.local_ts(),
        },
    )


def _clear_outgoing_command_preview(scanner: str) -> None:
    _hset_many(
        key_state(scanner),
        {
            "outgoing_command_action": "",
            "outgoing_command_args_json": "",
            "outgoing_command_source": "",
            "outgoing_command_updated_at": "",
        },
    )


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


# ====================================
# Public entry points (ONLY THESE)
# ====================================

def on_command_issued(scanner: str, action: str, args: Dict[str, Any]) -> Dict[str, Any]:
    state = _get_state(scanner)
    if state != S0_IDLE:
        _hset_many(
            key_state(scanner),
            {
                "state_detail": f"blocked: not idle ({state})",
                "state_updated_at": utility.local_ts(),
            },
        )
        return {"status": "blocked", "state": state}

    stop = _load_stop()
    if stop.get("stop"):
        _set_state(scanner, S7_STOPPED, stop.get("reason", ""))
        return {"status": "blocked", "reason": "experiment stopped"}

    try:
        # ---------------------------------------------------------
        # Step 1: normalize script command
        # ---------------------------------------------------------
        action, args = _normalize_mobility_command(action, args)

        # ---------------------------------------------------------
        # Step 2: update planned location (script intent)
        # ---------------------------------------------------------
        planned = _s0_init_planned(scanner)
        new_planned = _apply_mobility_command_to_pose(planned, action, args)
        _save_planned(scanner, new_planned)

        # ---------------------------------------------------------
        # Step 3: build actual motion command (NEW CORE)
        # ---------------------------------------------------------
        issued_action, issued_args, build_debug = _build_command_from_true_to_planned(scanner)

        # ---------------------------------------------------------
        # Step 4: determine start pose (for path check)
        # ---------------------------------------------------------
        start = _load_true(scanner)
        if not _is_loc_ok(start):
            # fallback ONLY if true does not exist at all
            start = new_planned

        # ---------------------------------------------------------
        # Step 5: path safety check
        # ---------------------------------------------------------
        tx = float(start["x_m"])
        ty = float(start["y_m"])
        px = float(new_planned["x_m"])
        py = float(new_planned["y_m"])

        path_ok, blocked = _is_path_clear(tx, ty, px, py, exclude_scanner=scanner)

        if not path_ok:
            _set_state(scanner, S7_STOPPED, f"script command path unsafe, blocked={len(blocked)}")
            return {
                "status": "stop",
                "reason": "path unsafe",
                "blocked_cells": len(blocked),
                "debug": build_debug,
            }

        # ---------------------------------------------------------
        # Step 6: record issued command (IMPORTANT: modified command)
        # ---------------------------------------------------------
        ts = utility.local_ts()

        _hset_many(
            key_time(scanner),
            {
                "last_planned_command_issued_at": ts,
            },
        )

        _hset_many(
            key_pose(scanner),
            {
                "last_planned_command_action": issued_action,
                "last_planned_command_args_json": issued_args,
            },
        )

        # ---------------------------------------------------------
        # Step 7: reset correction counter (new script command)
        # ---------------------------------------------------------
        _reset_correction_counter(scanner)

        # ---------------------------------------------------------
        # Step 8: debug info
        # ---------------------------------------------------------
        _hset_many(
            key_state(scanner),
            {
                "last_command_build_debug": build_debug,
                "outgoing_command_action": issued_action,
                "outgoing_command_args_json": issued_args,
                "outgoing_command_source": "script",
                "outgoing_command_updated_at": ts,
            },
        )

        # ---------------------------------------------------------
        # Step 9: transition
        # ---------------------------------------------------------
        _set_state(scanner, S1_WAITING_REPORT, f"issued {issued_action}")

        return {
            "status": "ok",
            "issued_action": issued_action,
            "issued_args": issued_args,
            "new_planned": new_planned,
            "state_after": S1_WAITING_REPORT,
            "debug": build_debug,
        }

    except Exception as e:
        _set_state(scanner, S0_IDLE, f"s0 error: {e}")
        return {"status": "error", "detail": str(e)}
    

def on_report_received(scanner: str) -> Dict[str, Any]:
    """
    Entry when robot sends mobility report.
    In Phase 3, just run the state machine from s1waiting_report.
    """
    return run_state_machine(scanner)


def should_block_command(category: str) -> bool:
    stop = _load_stop()
    if not stop.get("stop"):
        return False
    return (category or "").lower() != "av"


def run_state_machine(scanner: str) -> Dict[str, Any]:
    state = _get_state(scanner)

    if state == S0_IDLE:
        return s0idle(scanner)
    if state == S1_WAITING_REPORT:
        return s1waiting_report(scanner)
    if state == S2_EVALUATING_POLICY:
        return s2evaluating_policy(scanner)
    if state == S3_SOLVING_TRUE_LOCATION:
        return s3solving_true_location(scanner)
    if state == S4_WAITING_LOCATION_RETRY:
        return s4waiting_location_retry(scanner)
    if state == S5_COMPUTING_CORRECTION:
        return s5computing_correction(scanner)
    if state == S6_ISSUING_CORRECTION:
        return s6issuing_correction(scanner)
    if state == S7_STOPPED:
        return s7stopped(scanner)

    _set_state(scanner, S0_IDLE, "invalid state reset")
    return s0idle(scanner)


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


def _check_motion_path(scanner: str, start, planned) -> bool:
    tx = float(start["x_m"])
    ty = float(start["y_m"])
    px = float(planned["x_m"])
    py = float(planned["y_m"])

    ok, _ = _is_path_clear(tx, ty, px, py, exclude_scanner=scanner)
    return ok


# ====================================
# State handlers (EMPTY FIRST)
# ====================================

# -------- s0idle --------

def s0idle(scanner: str) -> Dict[str, Any]:
    return {"state": S0_IDLE, "status": "ok", "detail": "idle"}


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


def _s0_apply_motion(loc: Dict[str, Any], action: str, args: Dict[str, Any]) -> Dict[str, Any]:
    x = float(loc["x_m"])
    y = float(loc["y_m"])
    h = float(loc["heading_deg"])

    if action == "mobility.move.forward":
        d = float(args["distance_m"])
        x += d * math.cos(_deg_to_rad(h))
        y += d * math.sin(_deg_to_rad(h))

    elif action == "mobility.move.backward":
        d = float(args["distance_m"])
        x -= d * math.cos(_deg_to_rad(h))
        y -= d * math.sin(_deg_to_rad(h))

    elif action == "mobility.turn.left":
        h = _deg_norm_360(h + float(args["angle_deg"]))

    elif action == "mobility.turn.right":
        h = _deg_norm_360(h - float(args["angle_deg"]))

    elif action == "mobility.report.location":
        pass

    else:
        raise ValueError("unsupported action")

    return {
        "location_ok": True,
        "x_m": x,
        "y_m": y,
        "heading_deg": h,
    }


# -------- s1waiting_report --------

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


# -------- s2evaluating_policy --------

def s2evaluating_policy(scanner: str) -> Dict[str, Any]:
    result = _s2_evaluate_policy(scanner)

    transition_to = result["transition_to"]
    _set_state(scanner, transition_to, result["detail"])

    return {
        "state": transition_to,
        "status": result["status"],
        "detail": result["detail"],
    }


def _load_report_json(scanner: str) -> Dict[str, Any]:
    return _hget_json(key_report(scanner), "last_mobility_report_json")


def _save_policy_time(scanner: str) -> None:
    _hset_many(
        key_time(scanner),
        {
            "policy_updated_at": utility.local_ts(),
        },
    )


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


# -------- s3solving_true_location --------

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


def _deg_norm_360(deg: float) -> float:
    x = deg % 360.0
    return x + 360.0 if x < 0 else x


def _deg_to_rad(deg: float) -> float:
    return math.radians(deg)


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


def _load_tag_map() -> Dict[str, Any]:
    raw = config.r.get(f"{config.KEY_PREFIX}mobility:tag_map_json") or ""
    if not raw.strip():
        return {}
    try:
        j = json.loads(raw)
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}
    

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


MULTI_TAG_POS_THRESH_M = 0.5
MULTI_TAG_HEADING_THRESH_DEG = 20.0


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


# -------- s4waiting_location_retry --------

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


# -------- s5computing_correction --------

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


def _reset_correction_counter(scanner: str) -> None:
    _hset_many(
        key_state(scanner),
        {
            "correction_attempt_count": "0",
        },
    )


def _inc_correction_counter(scanner: str) -> int:
    val = _hget(key_state(scanner), "correction_attempt_count", "0")
    try:
        n = int(val)
    except Exception:
        n = 0

    n += 1

    _hset_many(
        key_state(scanner),
        {
            "correction_attempt_count": str(n),
        },
    )

    return n


def _get_correction_counter(scanner: str) -> int:
    val = _hget(key_state(scanner), "correction_attempt_count", "0")
    try:
        return int(val)
    except Exception:
        return 0
    

# -------- s6issuing_correction --------

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

# -------- s7stopped --------

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


def manual_resume(scanner: str) -> Dict[str, Any]:
    """
    Manual recovery from s7stopped.
    Does not reconstruct pose; only clears stop state for this scanner and returns to s0idle.
    """
    _clear_pending_sequence(scanner)
    _clear_outgoing_command_preview(scanner)
    _reset_correction_counter(scanner)
    _set_state(scanner, S0_IDLE, "manual resume")

    return {
        "status": "ok",
        "scanner": scanner,
        "state": S0_IDLE,
        "detail": "manual resume complete",
    }


# ====================================
# Update 10-second report
# ====================================

def _update_10s_report(scanner: str) -> None:
    true_loc = _load_true(scanner)

    payload = {
        "scanner": scanner,
        "time": utility.local_ts(),
        "true_location": true_loc if _is_loc_ok(true_loc) else {},
    }

    _hset_many(
        key_report(scanner),
        {
            "last_10s_report_json": payload,
            "last_10s_report_at": payload["time"],
        },
    )