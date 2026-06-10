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
import re

import utility

from m8mobility_state_store import key_pose, _is_loc_ok


# ===== asset loaders =====
KEY_TAG_MAP_JSON = f"{config.KEY_PREFIX}mobility:tag_map_json"

def _load_static_map() -> np.ndarray:
    """
    Load the static restriction map from config.MOBILITY_STATIC_RESTRICTION_MAP_NPY.

    Requirements:
    - .npy file
    - square 2-D array
    - uint8 values 0/1
    """
    site_cfg = _load_site_config()
    path = Path(config.mobility_restriction_map_path())
    if not path.exists():
        raise FileNotFoundError(f"static restriction map not found: {path}")

    arr = np.load(str(path), allow_pickle=False)

    if not isinstance(arr, np.ndarray):
        raise ValueError("static restriction map is not a numpy array")
    if arr.ndim != 2:
        raise ValueError("static restriction map must be 2-D")
    # Rectangular maps are allowed, but DemoRoom is currently square.

    if arr.dtype != np.uint8:
        arr = arr.astype(np.uint8)

    arr = (arr > 0).astype(np.uint8)

    rows, cols = int(arr.shape[0]), int(arr.shape[1])
    expected_res_x = float(site_cfg["world_width_m"]) / float(cols)
    expected_res_y = float(site_cfg["world_height_m"]) / float(rows)
    desired_res = float(site_cfg["grid_resolution_m"])

    if abs(expected_res_x - desired_res) > 1e-9 or abs(expected_res_y - desired_res) > 1e-9:
        raise ValueError(
            f"static map resolution mismatch: "
            f"x={expected_res_x:.6f} y={expected_res_y:.6f}, site.json wants {desired_res:.6f}"
        )

    return arr

_TAG_RE = re.compile(
    r"Tag#\s*(\d+)\s*:\s*\(\s*([-+]?\d+(?:\.\d+)?)\s*,\s*([-+]?\d+(?:\.\d+)?)\s*\)",
    re.IGNORECASE,
)

_FACING_YAW = {
    "right": 0.0,
    "up": 90.0,
    "left": 180.0,
    "down": 270.0,
}


def _load_site_config() -> Dict[str, Any]:
    path = Path(config.mobility_site_json_path())
    if not path.exists():
        raise FileNotFoundError(f"site config not found: {path}")

    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise ValueError(f"failed to parse site config {path}: {e}")

    if not isinstance(obj, dict):
        raise ValueError("site.json must be a JSON object")

    width = float(obj.get("world_width_m"))
    height = float(obj.get("world_height_m"))
    res = float(obj.get("grid_resolution_m"))

    if width <= 0 or height <= 0 or res <= 0:
        raise ValueError("site.json requires positive world_width_m, world_height_m, grid_resolution_m")

    return {
        "site_name": str(obj.get("site_name") or config.MOBILITY_SITE_NAME),
        "world_width_m": width,
        "world_height_m": height,
        "grid_resolution_m": res,
        "origin": str(obj.get("origin") or "bottom_left"),
        "path": str(path),
    }


def _parse_tag_location_txt(path: Path, site_cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"tag location file not found: {path}")

    text = path.read_text(encoding="utf-8", errors="ignore")
    tags: Dict[str, Dict[str, Any]] = {}
    current_facing = ""

    for line_no, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue

        lower = line.lower()

        if "tags facing" in lower:
            current_facing = ""
            for name in _FACING_YAW:
                if name in lower:
                    current_facing = name
                    break
            continue

        m = _TAG_RE.search(line)
        if not m:
            continue

        if current_facing not in _FACING_YAW:
            raise ValueError(f"tag line before valid facing section at line {line_no}: {line}")

        tag_id = int(m.group(1))
        tags[str(tag_id)] = {
            "x_m": float(m.group(2)),
            "y_m": float(m.group(3)),
            "yaw_deg": float(_FACING_YAW[current_facing]),
            "facing": current_facing,
            "source_line": line_no,
        }

    if not tags:
        raise ValueError(f"no tags parsed from {path}")

    return {
        "source": str(path),
        "site_name": site_cfg["site_name"],
        "loaded_at": utility.local_ts(),
        "world_width_m": site_cfg["world_width_m"],
        "world_height_m": site_cfg["world_height_m"],
        "grid_resolution_m": site_cfg["grid_resolution_m"],
        "origin": site_cfg["origin"],
        "tag_count": len(tags),
        "tags": tags,
    }


def _load_tag_location_txt_to_redis(site_cfg: Dict[str, Any]) -> Dict[str, Any]:
    tag_map = _parse_tag_location_txt(Path(config.mobility_tag_location_path()), site_cfg)
    config.r.set(KEY_TAG_MAP_JSON, json.dumps(tag_map, ensure_ascii=False))
    return tag_map


def _load_tag_map() -> Dict[str, Any]:
    raw = config.r.get(KEY_TAG_MAP_JSON) or ""
    if not raw.strip():
        return {}
    try:
        j = json.loads(raw)
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}


def _ensure_mobility_assets_ready() -> Dict[str, Any]:
    site_cfg = _load_site_config()
    static_map = _load_static_map()
    tag_map = _load_tag_location_txt_to_redis(site_cfg)

    tags = tag_map.get("tags") if isinstance(tag_map, dict) else None
    if not isinstance(tags, dict) or len(tags) == 0:
        raise ValueError("tag_location.txt parsed but produced empty tags")

    return {
        "status": "ok",
        "detail": "mobility assets ready",
        "site_name": site_cfg["site_name"],
        "site_dir": str(config.mobility_site_dir()),
        "site_json_path": str(config.mobility_site_json_path()),
        "static_map_path": str(config.mobility_restriction_map_path()),
        "tag_location_path": str(config.mobility_tag_location_path()),
        "static_map_shape": [int(static_map.shape[0]), int(static_map.shape[1])],
        "world_width_m": site_cfg["world_width_m"],
        "world_height_m": site_cfg["world_height_m"],
        "grid_resolution_m": site_cfg["grid_resolution_m"],
        "tag_count": len(tags),
        "tag_map_key": KEY_TAG_MAP_JSON,
    }


# ===== grid helpers =====

def _grid_size(static_map: np.ndarray) -> int:
    return int(static_map.shape[0])

def _grid_resolution_m(static_map: np.ndarray) -> float:
    return float(_load_site_config()["grid_resolution_m"])

def _world_to_grid(x_m: float, y_m: float, static_map: np.ndarray) -> tuple[int, int]:
    site_cfg = _load_site_config()
    rows = int(static_map.shape[0])
    cols = int(static_map.shape[1])
    res = float(site_cfg["grid_resolution_m"])

    col = int(math.floor(x_m / res))
    row_from_bottom = int(math.floor(y_m / res))
    row = rows - 1 - row_from_bottom

    col = max(0, min(cols - 1, col))
    row = max(0, min(rows - 1, row))
    return row, col


# ===== raster/map construction =====

def _rasterize_circle(mask: np.ndarray, x_m: float, y_m: float, radius_m: float) -> None:
    """
    Mark a filled circle into a uint8 map.
    """
    rows, cols = int(mask.shape[0]), int(mask.shape[1])
    res = _grid_resolution_m(mask)

    center_row, center_col = _world_to_grid(x_m, y_m, mask)
    r_cells = max(1, int(math.ceil(radius_m / res)))

    for dr in range(-r_cells, r_cells + 1):
        for dc in range(-r_cells, r_cells + 1):
            rr = center_row + dr
            cc = center_col + dc

            if rr < 0 or rr >= rows or cc < 0 or cc >= cols:
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

        true_loc = utility._hget_json(pose_key, "true_location_json")
        planned_loc = utility._hget_json(pose_key, "planned_location_json")

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

def _is_path_clear_debug(
    x0_m: float,
    y0_m: float,
    x1_m: float,
    y1_m: float,
    exclude_scanner: str = "",
) -> tuple[bool, list[tuple[int, int]], Dict[str, Any]]:
    """
    Debug version of _is_path_clear().

    Purpose:
    - distinguish static-map blockage from dynamic robot keep-out blockage
    - show sampled cells
    - show start/target world coordinates
    """
    static_map, dynamic_map, effective_map = _build_effective_map(exclude_scanner=exclude_scanner)
    cells = _sample_path_cells(x0_m, y0_m, x1_m, y1_m, static_map)

    blocked: list[tuple[int, int]] = []
    cell_details: list[Dict[str, Any]] = []

    for row, col in cells:
        s = int(static_map[row, col] != 0)
        d = int(dynamic_map[row, col] != 0)
        e = int(effective_map[row, col] != 0)

        item = {
            "row": int(row),
            "col": int(col),
            "static": s,
            "dynamic": d,
            "effective": e,
        }
        cell_details.append(item)

        if e:
            blocked.append((row, col))

    debug = {
        "start": {
            "x_m": float(x0_m),
            "y_m": float(y0_m),
            "grid": _world_to_grid(x0_m, y0_m, static_map),
        },
        "target": {
            "x_m": float(x1_m),
            "y_m": float(y1_m),
            "grid": _world_to_grid(x1_m, y1_m, static_map),
        },
        "sample_count": len(cells),
        "sample_cells": cell_details[:30],
        "blocked_count": len(blocked),
        "blocked_cells": [
            d for d in cell_details
            if int(d.get("effective", 0)) != 0
        ][:30],
    }

    return (len(blocked) == 0), blocked, debug
