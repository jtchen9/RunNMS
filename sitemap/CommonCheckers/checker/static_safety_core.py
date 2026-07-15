from __future__ import annotations

"""
Shared static safety core for preflight and runtime.

Design boundary:
- No CSV parsing.
- No Redis access.
- No NMS state-machine access.
- No validation-report formatting.
- No assumption that poses are planned or true.

Adapters add context:
- preflight adapters add row_number/scanner/action.
- runtime adapters add command id, state-machine context, true-pose freshness, etc.

This module intentionally mirrors the existing runtime restriction-map convention:
- map is a 2-D numpy array
- nonzero means restricted
- row = floor((y_m - origin_y_m) / resolution_m)
- col = floor((x_m - origin_x_m) / resolution_m)

For safety, this shared core does not silently clamp out-of-map points. A point
outside the map returns out_of_map=True so adapters can reject it explicitly.
"""

import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


Pose = Dict[str, float]
Issue = Dict[str, Any]


def deg_norm_360(v: float) -> float:
    return float(v) % 360.0


def normalize_restriction_map_array(arr: np.ndarray) -> np.ndarray:
    """
    Runtime-compatible normalization.

    Existing runtime m8mobility_map.py accepts uint8-ish 0/1 maps and normalizes
    any nonzero value to 1. Keep the same convention here.
    """
    if not isinstance(arr, np.ndarray):
        raise ValueError("restriction map is not a numpy array")
    if arr.ndim != 2:
        raise ValueError(f"restriction map must be 2-D, got shape {arr.shape}")
    if arr.dtype != np.uint8:
        arr = arr.astype(np.uint8)
    return (arr > 0).astype(np.uint8)


def load_restriction_map_file(path: str | Path) -> np.ndarray:
    """
    Load and normalize a static restriction map from a .npy file.

    This is pure file/array logic. Runtime config path resolution remains outside
    this function.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"restriction map not found: {p}")
    return normalize_restriction_map_array(np.load(str(p), allow_pickle=False))


def validate_map_resolution(
    *,
    map_shape: tuple[int, int],
    world_width_m: float,
    world_height_m: float,
    grid_resolution_m: float,
    tolerance: float = 1e-9,
) -> Optional[str]:
    """
    Return None if map resolution matches the site config, otherwise a message.

    This mirrors the existing runtime check but keeps it reusable.
    """
    rows, cols = int(map_shape[0]), int(map_shape[1])
    expected_res_x = float(world_width_m) / float(cols)
    expected_res_y = float(world_height_m) / float(rows)
    desired_res = float(grid_resolution_m)

    if abs(expected_res_x - desired_res) > tolerance or abs(expected_res_y - desired_res) > tolerance:
        return (
            f"static map resolution mismatch: "
            f"x={expected_res_x:.6f} y={expected_res_y:.6f}, "
            f"site config wants {desired_res:.6f}"
        )
    return None


def load_restriction_map_from_policy(
    site_dir: str | Path,
    path_policy: Dict[str, Any],
) -> Tuple[Optional[np.ndarray], Dict[str, Any], List[Issue]]:
    """
    Preflight-friendly loader using path_policy.json.

    Runtime can keep using config.mobility_restriction_map_path(), then call
    load_restriction_map_file() directly.
    """
    cfg = dict(path_policy.get("restriction_map", {}) or {})
    if not cfg.get("enabled", False):
        return None, cfg, []

    map_path = Path(site_dir) / str(cfg.get("relative_path", "restriction_map.npy"))

    try:
        data = load_restriction_map_file(map_path)
    except FileNotFoundError:
        return None, cfg, [{
            "level": "error",
            "code": "RESTRICTION_MAP_MISSING",
            "row_number": 0,
            "message": f"restriction map file not found: {map_path}",
            "suggestion": "Put restriction_map.npy in the site folder or disable restriction_map.enabled.",
        }]
    except Exception as e:
        return None, cfg, [{
            "level": "error",
            "code": "RESTRICTION_MAP_LOAD_FAILED",
            "row_number": 0,
            "message": f"failed to load restriction map {map_path}: {type(e).__name__}: {e}",
            "suggestion": "Verify restriction_map.npy is a valid 2-D NumPy array file.",
        }]

    return data, cfg, []


def world_to_grid_unclamped(
    x_m: float,
    y_m: float,
    *,
    resolution_m: float,
    origin_x_m: float = 0.0,
    origin_y_m: float = 0.0,
) -> tuple[int, int]:
    """
    Runtime-compatible floor conversion, without clamping.

    Existing runtime _world_to_grid() clamps after conversion. This shared helper
    intentionally does not clamp so safety callers can reject out-of-map points.
    """
    row = int(math.floor((float(y_m) - float(origin_y_m)) / float(resolution_m)))
    col = int(math.floor((float(x_m) - float(origin_x_m)) / float(resolution_m)))
    return row, col


def world_to_grid_clamped(
    x_m: float,
    y_m: float,
    *,
    static_map: np.ndarray,
    resolution_m: float,
    origin_x_m: float = 0.0,
    origin_y_m: float = 0.0,
) -> tuple[int, int]:
    """
    Compatibility helper matching old runtime behavior.

    Use only for diagnostics or legacy behavior. New safety decisions should
    prefer point_restriction_status(), which exposes out_of_map explicitly.
    """
    row, col = world_to_grid_unclamped(
        x_m,
        y_m,
        resolution_m=resolution_m,
        origin_x_m=origin_x_m,
        origin_y_m=origin_y_m,
    )
    rows, cols = int(static_map.shape[0]), int(static_map.shape[1])
    row = max(0, min(rows - 1, row))
    col = max(0, min(cols - 1, col))
    return row, col


def _resolution_from_cfg(cfg: Dict[str, Any]) -> float:
    return float(cfg.get("resolution_m", cfg.get("grid_resolution_m", 0.1)))


def point_restriction_status(
    data: np.ndarray,
    cfg: Dict[str, Any],
    x_m: float,
    y_m: float,
) -> Dict[str, Any]:
    """
    Return restriction status for a world point.

    Does not clamp. If out of map, restricted follows cfg['out_of_map_is_restricted'].
    """
    res = _resolution_from_cfg(cfg)
    ox = float(cfg.get("origin_x_m", 0.0))
    oy = float(cfg.get("origin_y_m", 0.0))
    row, col = world_to_grid_unclamped(x_m, y_m, resolution_m=res, origin_x_m=ox, origin_y_m=oy)

    out_of_map = row < 0 or col < 0 or row >= int(data.shape[0]) or col >= int(data.shape[1])
    if out_of_map:
        return {
            "restricted": bool(cfg.get("out_of_map_is_restricted", True)),
            "out_of_map": True,
            "row": row,
            "col": col,
            "value": None,
        }

    value = int(data[row, col])
    return {
        "restricted": value == int(cfg.get("restricted_value", 1)),
        "out_of_map": False,
        "row": row,
        "col": col,
        "value": value,
    }


def first_restricted_sample_on_segment(
    data: Optional[np.ndarray],
    cfg: Dict[str, Any],
    x0_m: float,
    y0_m: float,
    x1_m: float,
    y1_m: float,
) -> Optional[Dict[str, Any]]:
    """
    Sample a straight-line segment and return the first restricted sample.

    Runtime currently samples at grid resolution. Preflight policy may choose a
    smaller segment_sample_step_m, e.g. 0.05 m. Both should eventually use the
    same policy value.
    """
    if data is None:
        return None

    step = float(cfg.get("segment_sample_step_m", cfg.get("resolution_m", 0.1)))
    dist = math.hypot(float(x1_m) - float(x0_m), float(y1_m) - float(y0_m))
    steps = max(1, int(math.ceil(dist / max(step, 1e-6))))

    for i in range(steps + 1):
        t = i / steps
        x = float(x0_m) + t * (float(x1_m) - float(x0_m))
        y = float(y0_m) + t * (float(y1_m) - float(y0_m))
        status = point_restriction_status(data, cfg, x, y)
        if status["restricted"]:
            return {
                "x_m": x,
                "y_m": y,
                "row": status["row"],
                "col": status["col"],
                "value": status["value"],
                "out_of_map": status["out_of_map"],
                "sample_index": i,
                "sample_count": steps,
            }

    return None


def sample_path_cells_legacy(
    x0_m: float,
    y0_m: float,
    x1_m: float,
    y1_m: float,
    static_map: np.ndarray,
    *,
    resolution_m: float,
    origin_x_m: float = 0.0,
    origin_y_m: float = 0.0,
) -> List[tuple[int, int]]:
    """
    Compatibility helper for the current runtime-style cell sampling.

    Samples at grid resolution and returns unique clamped cells, matching the
    existing m8mobility_map.py behavior closely.
    """
    dist = math.hypot(float(x1_m) - float(x0_m), float(y1_m) - float(y0_m))
    steps = max(1, int(math.ceil(dist / float(resolution_m))))
    out: List[tuple[int, int]] = []
    seen = set()

    for i in range(steps + 1):
        t = i / steps
        x = float(x0_m) + t * (float(x1_m) - float(x0_m))
        y = float(y0_m) + t * (float(y1_m) - float(y0_m))
        rc = world_to_grid_clamped(
            x,
            y,
            static_map=static_map,
            resolution_m=resolution_m,
            origin_x_m=origin_x_m,
            origin_y_m=origin_y_m,
        )
        if rc not in seen:
            seen.add(rc)
            out.append(rc)

    return out


def point_in_bounds(x: float, y: float, bounds: Dict[str, Any]) -> bool:
    return (
        float(bounds["x_min_m"]) <= float(x) <= float(bounds["x_max_m"])
        and float(bounds["y_min_m"]) <= float(y) <= float(bounds["y_max_m"])
    )


def deg_norm_360(v: float) -> float:
    return float(v) % 360.0


def distance_point_to_segment(
    px: float,
    py: float,
    x0: float,
    y0: float,
    x1: float,
    y1: float,
) -> float:
    dx = float(x1) - float(x0)
    dy = float(y1) - float(y0)
    denom = dx * dx + dy * dy

    if denom <= 1e-12:
        return math.hypot(float(px) - float(x0), float(py) - float(y0))

    t = ((float(px) - float(x0)) * dx + (float(py) - float(y0)) * dy) / denom
    t = max(0.0, min(1.0, t))
    cx = float(x0) + t * dx
    cy = float(y0) + t * dy
    return math.hypot(float(px) - cx, float(py) - cy)


def point_inside_circle(x: float, y: float, circle: Dict[str, Any]) -> bool:
    return math.hypot(
        float(x) - float(circle["center_x_m"]),
        float(y) - float(circle["center_y_m"]),
    ) <= float(circle["radius_m"])


def segment_intersects_circle(
    x0: float,
    y0: float,
    x1: float,
    y1: float,
    circle: Dict[str, Any],
) -> bool:
    d = distance_point_to_segment(
        float(circle["center_x_m"]),
        float(circle["center_y_m"]),
        x0,
        y0,
        x1,
        y1,
    )
    return d <= float(circle["radius_m"])


def point_inside_axis_aligned_rect(x: float, y: float, rect: Dict[str, Any]) -> bool:
    return (
        float(rect["x_min_m"]) <= float(x) <= float(rect["x_max_m"])
        and float(rect["y_min_m"]) <= float(y) <= float(rect["y_max_m"])
    )


def segment_intersects_axis_aligned_rect(
    x0: float,
    y0: float,
    x1: float,
    y1: float,
    rect: Dict[str, Any],
) -> bool:
    """
    Conservative segment-vs-axis-aligned-rectangle intersection.

    Returns True if the segment touches or crosses the rectangle, including
    endpoints inside the rectangle.
    """
    if point_inside_axis_aligned_rect(x0, y0, rect):
        return True
    if point_inside_axis_aligned_rect(x1, y1, rect):
        return True

    x_min = float(rect["x_min_m"])
    x_max = float(rect["x_max_m"])
    y_min = float(rect["y_min_m"])
    y_max = float(rect["y_max_m"])

    dx = float(x1) - float(x0)
    dy = float(y1) - float(y0)
    t0 = 0.0
    t1 = 1.0

    checks = [
        (-dx, float(x0) - x_min),
        ( dx, x_max - float(x0)),
        (-dy, float(y0) - y_min),
        ( dy, y_max - float(y0)),
    ]

    for p, q in checks:
        if abs(p) < 1e-12:
            if q < 0:
                return False
            continue

        r = q / p
        if p < 0:
            if r > t1:
                return False
            if r > t0:
                t0 = r
        else:
            if r < t0:
                return False
            if r < t1:
                t1 = r

    return t0 <= t1


def apply_mobility_move_pose(
    current: Pose,
    args: Dict[str, Any],
) -> Tuple[Pose, List[Issue]]:
    """
    Shared mobility.move pose calculation.

    Returns (new_pose, issue_fragments). Wrappers add row or runtime context.
    """
    issues: List[Issue] = []
    args = args or {}

    has_abs_xy = (
        ("x_m" in args and "y_m" in args)
        or ("target_x_m" in args and "target_y_m" in args)
    )
    has_delta_xy = "dx_m" in args or "dy_m" in args

    if has_abs_xy and has_delta_xy:
        issues.append({
            "level": "error",
            "code": "MOVE_ARGS_MIX_ABSOLUTE_AND_DELTA",
            "message": "mobility.move must use either absolute target x/y or dx/dy, not both.",
            "suggestion": "Use x_m/y_m for an absolute target, or dx_m/dy_m for a relative move.",
        })
        return current, issues

    try:
        if has_abs_xy:
            x_m = float(args.get("x_m", args.get("target_x_m")))
            y_m = float(args.get("y_m", args.get("target_y_m")))
        elif has_delta_xy:
            x_m = float(current["x_m"]) + float(args.get("dx_m", 0.0) or 0.0)
            y_m = float(current["y_m"]) + float(args.get("dy_m", 0.0) or 0.0)
        else:
            issues.append({
                "level": "error",
                "code": "MOVE_ARGS_MISSING_TARGET",
                "message": "mobility.move requires x_m/y_m, target_x_m/target_y_m, or dx_m/dy_m.",
                "suggestion": "Add args_json such as {\"x_m\": 2.0, \"y_m\": 4.4}.",
            })
            return current, issues

        heading_deg = (
            deg_norm_360(float(args["heading_deg"]))
            if "heading_deg" in args
            else deg_norm_360(float(current["heading_deg"]))
        )
    except Exception as e:
        issues.append({
            "level": "error",
            "code": "MOVE_ARGS_BAD_NUMERIC_VALUE",
            "message": f"mobility.move has a bad numeric value: {type(e).__name__}: {e}",
            "suggestion": "Use numeric meters/degrees in args_json.",
        })
        return current, issues

    return {
        "x_m": x_m,
        "y_m": y_m,
        "heading_deg": heading_deg,
    }, issues


def macro_planned_pose(macro_cfg: Dict[str, Any]) -> Pose:
    start_x = float(macro_cfg["start_x_m"])
    start_y = float(macro_cfg["start_y_m"])
    heading = deg_norm_360(float(macro_cfg["target_heading_deg"]))
    distance = float(macro_cfg["distance_m"])

    rad = math.radians(heading)
    return {
        "x_m": start_x + distance * math.cos(rad),
        "y_m": start_y + distance * math.sin(rad),
        "heading_deg": heading,
    }


def macro_start_pose_issues(
    current: Pose,
    macro_cfg: Dict[str, Any],
) -> List[Issue]:
    """
    Shared macro-start spatial check.

    Returns issue fragments without preflight row or runtime command context.
    """
    try:
        start_x = float(macro_cfg["start_x_m"])
        start_y = float(macro_cfg["start_y_m"])
        tol = float(macro_cfg.get("start_tolerance_m", 0.20))
        err = math.hypot(float(current["x_m"]) - start_x, float(current["y_m"]) - start_y)
    except Exception as e:
        return [{
            "level": "error",
            "code": "MACRO_CONFIG_BAD_NUMERIC_VALUE",
            "message": f"macro config has a bad numeric value: {type(e).__name__}: {e}",
            "suggestion": "Fix macro_policy.json.",
        }]

    if err <= tol:
        return []

    return [{
        "level": "error",
        "code": "MACRO_START_POSE_MISMATCH",
        "message": (
            f"planned/true pose ({float(current['x_m']):.3f}, {float(current['y_m']):.3f}) "
            f"is {err:.3f} m from macro start "
            f"({start_x:.3f}, {start_y:.3f}); tolerance={tol:.3f} m."
        ),
        "suggestion": "Move the robot to the macro start point before this macro.",
        "start_error_m": err,
        "start_tolerance_m": tol,
    }]


def bump_guard_crossing_issues(
    start_pose: Pose,
    end_pose: Pose,
    bump_guard_zones: Dict[str, Any],
) -> List[Issue]:
    """
    Shared bump-zone crossing check for normal mobility.move paths.
    """
    issues: List[Issue] = []

    for rect in (bump_guard_zones.get("zones", []) or []):
        if str(rect.get("type", "")).strip() != "axis_aligned_rectangle":
            continue

        if segment_intersects_axis_aligned_rect(
            float(start_pose["x_m"]),
            float(start_pose["y_m"]),
            float(end_pose["x_m"]),
            float(end_pose["y_m"]),
            rect,
        ):
            issues.append({
                "level": "error",
                "code": "MOVE_CROSSES_BUMP_GUARD_ZONE",
                "message": (
                    f"mobility.move path crosses bump guard zone "
                    f"{rect.get('name', '')}; bump crossing is allowed "
                    "only through mobility.in2out or mobility.out2in."
                ),
                "suggestion": "Use the site macro mobility.in2out or mobility.out2in for bump crossing.",
                "zone": rect.get("name", ""),
            })

    return issues


def charging_zone_crossing_issues(
    start_pose: Pose,
    end_pose: Pose,
    charging_zones: List[Dict[str, Any]],
) -> List[Issue]:
    """
    Shared charging-zone semantic check.

    restriction_map.npy is still the authoritative blocked-zone source. These
    circles provide clear reason codes for charging/rest areas.
    """
    issues: List[Issue] = []

    x0, y0 = float(start_pose["x_m"]), float(start_pose["y_m"])
    x1, y1 = float(end_pose["x_m"]), float(end_pose["y_m"])

    for zone in charging_zones:
        zone_name = str(zone.get("name", ""))

        if point_inside_circle(x0, y0, zone):
            issues.append({
                "level": "error",
                "code": "PATH_STARTS_INSIDE_CHARGING_ZONE",
                "message": (
                    f"path starts inside charging zone {zone_name}. Charging zones "
                    "remain restricted; move the robot out before experiment start."
                ),
                "suggestion": "Use a pose outside the charging zone.",
                "zone": zone_name,
            })
        elif point_inside_circle(x1, y1, zone):
            issues.append({
                "level": "error",
                "code": "PATH_ENDS_INSIDE_CHARGING_ZONE",
                "message": f"path ends inside charging zone {zone_name}, which remains restricted.",
                "suggestion": "Choose an endpoint outside charging zones.",
                "zone": zone_name,
            })
        elif segment_intersects_circle(x0, y0, x1, y1, zone):
            issues.append({
                "level": "error",
                "code": "PATH_CROSSES_CHARGING_ZONE",
                "message": f"path crosses charging zone {zone_name}, which remains restricted.",
                "suggestion": "Choose a path/target that does not pass through charging zones.",
                "zone": zone_name,
            })

    return issues
