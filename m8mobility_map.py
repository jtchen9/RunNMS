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
import sys

import utility

from m8mobility_state_store import key_pose, key_time, key_state, _is_loc_ok


def _ensure_common_checkers_on_path() -> None:
    """
    Allow production runtime modules at the NMS root to import the shared
    checker safety core under sitemap/CommonCheckers.

    This keeps m8mobility_map.py usable from the existing runtime location
    without requiring global PYTHONPATH changes.
    """
    root = Path(getattr(config, "MOBILITY_SITEMAP_ROOT", Path(".") / "sitemap"))
    common_dir = root / "CommonCheckers"
    common_s = str(common_dir)
    if common_dir.exists() and common_s not in sys.path:
        sys.path.insert(0, common_s)


_ensure_common_checkers_on_path()

from checker.static_safety_core import (  # type: ignore # noqa: E402
    normalize_restriction_map_array,
    sample_path_cells_legacy,
    validate_map_resolution,
    world_to_grid_clamped,
)


# ===== asset loaders =====
KEY_TAG_MAP_JSON = f"{config.KEY_PREFIX}mobility:tag_map_json"

def _load_static_map() -> np.ndarray:
    """
    Load the static restriction map from config.MOBILITY_STATIC_RESTRICTION_MAP_NPY.

    Requirements:
    - .npy file
    - 2-D array
    - uint8-compatible values 0/1

    P5c-b1 refactor:
    - keep the existing runtime API and error behavior
    - delegate array normalization and resolution validation to the shared
      static_safety_core.py so preflight/runtime use the same semantics
    """
    site_cfg = _load_site_config()
    path = Path(config.mobility_restriction_map_path())
    if not path.exists():
        raise FileNotFoundError(f"static restriction map not found: {path}")

    arr = np.load(str(path), allow_pickle=False)
    arr = normalize_restriction_map_array(arr)

    mismatch = validate_map_resolution(
        map_shape=(int(arr.shape[0]), int(arr.shape[1])),
        world_width_m=float(site_cfg["world_width_m"]),
        world_height_m=float(site_cfg["world_height_m"]),
        grid_resolution_m=float(site_cfg["grid_resolution_m"]),
    )
    if mismatch:
        raise ValueError(mismatch)

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
    """
    Convert world coordinates to a clamped grid cell.

    Preserves the previous runtime behavior exactly: coordinates outside the
    map are clamped to the closest valid cell. New safety code should prefer the
    non-clamped helpers in static_safety_core.py, but this legacy helper remains
    unchanged for callers that expect clamping.
    """
    site_cfg = _load_site_config()
    res = float(site_cfg["grid_resolution_m"])
    return world_to_grid_clamped(
        x_m,
        y_m,
        static_map=static_map,
        resolution_m=res,
        origin_x_m=0.0,
        origin_y_m=0.0,
    )


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

def _dynamic_obstacle_ttl_sec() -> int:
    """
    How fresh another robot must be to act as a dynamic blocker.

    Lab rule:
    - Powered-on / active robots may block each other.
    - Powered-off robots are put away and must not remain as phantom blockers.
    """
    try:
        return int(getattr(config, "MOBILITY_DYNAMIC_OBSTACLE_TTL_SEC", 120))
    except Exception:
        return 120

def _age_sec_from_local_ts(ts: str) -> float:
    """
    Return age in seconds for an NMS local timestamp.
    Unknown/bad timestamps are treated as infinitely stale.
    """
    try:
        dt = utility.parse_local_dt(ts)
        now = utility.parse_local_dt(utility.local_ts())
        return max(0.0, float((now - dt).total_seconds()))
    except Exception:
        return 1e99

def _scanner_is_active_for_dynamic_obstacle(scanner: str) -> tuple[bool, str]:
    """
    A registered scanner is NOT automatically a dynamic obstacle.

    Dynamic obstacle rule:
    - must be a robot, not AP
    - must be polling recently
    - must not be explicitly stopped/disabled/offline/lost
    - stale powered-off robots must be ignored
    """
    ttl = _dynamic_obstacle_ttl_sec()

    meta = config.r.hgetall(config.key_scanner_meta(scanner)) or {}
    state = config.r.hgetall(key_state(scanner)) or {}

    device_type = str(meta.get("device_type") or "").strip().lower()
    if device_type and device_type != "robot":
        return False, f"not_robot device_type={device_type}"

    safety = str(state.get("robot_safety_state") or "").strip().upper()
    if safety in {"OFFLINE", "LOST", "DISABLED"}:
        return False, f"robot_safety_state={safety}"

    last_poll = str(meta.get("last_poll") or "").strip()
    last_seen = str(meta.get("last_seen") or "").strip()

    # Prefer last_poll because it proves the robot agent is alive.
    # Fall back to last_seen for boot/register edge cases.
    active_ts = last_poll or last_seen
    if not active_ts:
        return False, "missing last_poll/last_seen"

    age = _age_sec_from_local_ts(active_ts)
    if age > ttl:
        return False, f"stale_active_ts age_sec={age:.1f} ttl_sec={ttl}"

    return True, f"active age_sec={age:.1f} ttl_sec={ttl}"

def _pose_is_fresh_for_dynamic_obstacle(scanner: str, field_name: str) -> tuple[bool, str]:
    """
    Pose freshness guard.

    true_location_json uses true_location_updated_at.
    planned_location_json uses planned_location_updated_at.

    This prevents old registration/planned poses from becoming permanent phantom walls.
    """
    ttl = _dynamic_obstacle_ttl_sec()
    time_key = key_time(scanner)

    if field_name == "true_location_json":
        ts_field = "true_location_updated_at"
    elif field_name == "planned_location_json":
        ts_field = "planned_location_updated_at"
    else:
        return False, f"unknown pose field {field_name}"

    ts = utility._hget(time_key, ts_field, "")
    if not ts:
        return False, f"missing {ts_field}"

    age = _age_sec_from_local_ts(ts)
    if age > ttl:
        return False, f"stale {ts_field} age_sec={age:.1f} ttl_sec={ttl}"

    return True, f"fresh {ts_field} age_sec={age:.1f} ttl_sec={ttl}"

def _build_dynamic_map(exclude_scanner: str = "") -> np.ndarray:
    """
    Build dynamic keep-out map from OTHER ACTIVE robots only.

    Lab rule:
    - A registered robot is not automatically a dynamic blocker.
    - Powered-off robots are put away.
    - Therefore stale/offline robots must not create phantom dynamic obstacles.

    Uses both true_location_json and planned_location_json for active robots,
    but only when the corresponding pose timestamp is fresh.
    """
    static_map = _load_static_map()
    dynamic = np.zeros_like(static_map, dtype=np.uint8)

    scanners = sorted(list(config.r.smembers(config.KEY_REGISTRY)))

    for scanner in scanners:
        if exclude_scanner and scanner == exclude_scanner:
            continue

        active_ok, _active_detail = _scanner_is_active_for_dynamic_obstacle(scanner)
        if not active_ok:
            continue

        pose_key = key_pose(scanner)

        true_loc = utility._hget_json(pose_key, "true_location_json")
        planned_loc = utility._hget_json(pose_key, "planned_location_json")

        true_fresh, _true_detail = _pose_is_fresh_for_dynamic_obstacle(
            scanner,
            "true_location_json",
        )
        if true_fresh and _is_loc_ok(true_loc):
            _rasterize_circle(
                dynamic,
                float(true_loc["x_m"]),
                float(true_loc["y_m"]),
                float(config.MOBILITY_ROBOT_RESTRICT_RADIUS_M),
            )

        planned_fresh, _planned_detail = _pose_is_fresh_for_dynamic_obstacle(
            scanner,
            "planned_location_json",
        )
        if planned_fresh and _is_loc_ok(planned_loc):
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

    Preserves the previous runtime behavior:
    - sample at site grid resolution
    - clamp world coordinates into valid map cells
    - return unique cells in traversal order

    The implementation now calls shared static_safety_core.py so preflight and
    runtime remain aligned on coordinate conversion and sampling conventions.
    """
    res = _grid_resolution_m(static_map)
    return sample_path_cells_legacy(
        x0_m,
        y0_m,
        x1_m,
        y1_m,
        static_map,
        resolution_m=res,
        origin_x_m=0.0,
        origin_y_m=0.0,
    )

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

    P6b note:
    - This function is still diagnostic only.
    - It does not change the safety decision.
    - It adds clearer static/dynamic/effective summaries so lab tests can tell
      whether a blockage came from the static map or another active robot.
    """
    static_map, dynamic_map, effective_map = _build_effective_map(exclude_scanner=exclude_scanner)
    cells = _sample_path_cells(x0_m, y0_m, x1_m, y1_m, static_map)

    blocked: list[tuple[int, int]] = []
    cell_details: list[Dict[str, Any]] = []

    static_blocked: list[Dict[str, Any]] = []
    dynamic_blocked: list[Dict[str, Any]] = []
    effective_blocked: list[Dict[str, Any]] = []
    static_only_blocked: list[Dict[str, Any]] = []
    dynamic_only_blocked: list[Dict[str, Any]] = []
    static_and_dynamic_blocked: list[Dict[str, Any]] = []

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

        if s:
            static_blocked.append(item)
        if d:
            dynamic_blocked.append(item)
        if e:
            blocked.append((row, col))
            effective_blocked.append(item)

        if s and d:
            static_and_dynamic_blocked.append(item)
        elif s:
            static_only_blocked.append(item)
        elif d:
            dynamic_only_blocked.append(item)

    dynamic_radius = float(
        getattr(
            config,
            "MOBILITY_ROBOT_RESTRICT_RADIUS_M",
            0.25,
        )
    )

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
        "exclude_scanner": str(exclude_scanner or ""),
        "dynamic_obstacle_radius_m": dynamic_radius,
        "sample_count": len(cells),
        "sample_cells": cell_details[:30],
        "blocked_count": len(blocked),
        "static_blocked_count": len(static_blocked),
        "dynamic_blocked_count": len(dynamic_blocked),
        "static_only_blocked_count": len(static_only_blocked),
        "dynamic_only_blocked_count": len(dynamic_only_blocked),
        "static_and_dynamic_blocked_count": len(static_and_dynamic_blocked),
        "blocked_cells": effective_blocked[:30],
        "static_blocked_cells": static_blocked[:30],
        "dynamic_blocked_cells": dynamic_blocked[:30],
        "static_only_blocked_cells": static_only_blocked[:30],
        "dynamic_only_blocked_cells": dynamic_only_blocked[:30],
        "static_and_dynamic_blocked_cells": static_and_dynamic_blocked[:30],
        "diagnostic_note": (
            "P6b diagnostic only: path decision is still effective_map != 0; "
            "dynamic cells come from active/fresh robot true/planned poses."
        ),
    }

    return (len(blocked) == 0), blocked, debug
