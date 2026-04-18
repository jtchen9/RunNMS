"""
Mobility subsystem map and tag-world helpers.

Division rule:
- Static restriction map, dynamic keep-out map, path checks, and AprilTag world-map loading live here.
- Initialization should call readiness checks here.
"""
from typing import Dict, Any
from pathlib import Path
import json
import math
import numpy as np
import config

from utility import _hget_json

from m8mobility_state_store import key_pose
from m8mobility_pose import _is_loc_ok

# ===== asset loaders =====

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

def _load_tag_map() -> Dict[str, Any]:
    raw = config.r.get(f"{config.KEY_PREFIX}mobility:tag_map_json") or ""
    if not raw.strip():
        return {}
    try:
        j = json.loads(raw)
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}
    
def _ensure_mobility_assets_ready():
    """
    unfinished temporary function
    need to check 
    1) tag info is loaded
    2) static robot restriction map is loaded
    """
    pass


# ===== grid helpers =====

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


# ===== raster/map construction =====

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

        if _is_loc_ok(true_loc):
            _rasterize_circle(
                dynamic,
                float(true_loc["x_m"]),
                float(true_loc["y_m"]),
                float(config.MOBILITY_ROBOT_RESTRICT_RADIUS_M),
            )

        if _is_loc_ok(planned_loc):
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


# ===== path check =====

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
