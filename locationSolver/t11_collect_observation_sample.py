r"""
t11_collect_observation_sample.py

Observation-only DemoRoom AprilTag data-collection helper.

Purpose:
- send one direct mobility.report.location command
- wait for one fresh robot report
- extract all valid AprilTag observations directly from the robot report
- compute map-based TrueD / TrueA / TrueY for every observation
- save compact JSON/CSV for data-quality analysis and later solver work

Important:
- NO location solver is called.
- NO legacy S3 solver tags_used / single-tag fallback is used.
- The compact table contains all valid observations, not only solver-used tags.

Change SCANNER_NAME below if using a different robot.
"""

from __future__ import annotations

import csv
import datetime
import json
import math
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Some production map helpers use paths relative to project root.
os.chdir(ROOT_DIR)

import config
import utility

from m8mobility_map import _ensure_mobility_assets_ready, _load_tag_map
from m8mobility_state_store import key_report, key_time


# ---------------------------------------------------------------------------
# Adjustable field-test settings
# ---------------------------------------------------------------------------

SCANNER_NAME = "twin-scout-charlie"

TRIAL = "T11-OBS-001"
NOTES = "observation-only; no solver"

WAIT_TIMEOUT_SEC = 60
POLL_EVERY_SEC = 1.0

SAVE_FULL_REPORT_JSON = False
SAVE_COMPACT_JSON_LOG = True
SAVE_COMPACT_CSV_LOG = True

# Diagnostic-only yaw keep/flip quantization.
# This does NOT change robot code, S3, or solver input.
YAW_EDGE_FRONT_TRUEA_DEG = 30.0
YAW_EDGE_REAR_TRUEA_DEG = 10.0
YAW_SMALL_TRUEY_DEG = 8.0
YAW_ACCEPT_BEST_ERR_DEG = 10.0
YAW_ACCEPT_SEPARATION_DEG = 15.0

PREFERRED_HEADING_DIR = ROOT_DIR / "testLocation" / "output" / "preferred_heading_full"

LOG_DIR = ROOT_DIR / "testLocation" / "output" / "t11_observation_collection"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LATEST_OBS_DIR = ROOT_DIR / "testLocation" / "input"
LATEST_OBS_DIR.mkdir(parents=True, exist_ok=True)

ROBOT_ID = SCANNER_NAME


# ---------------------------------------------------------------------------
# Input / lookup helpers
# ---------------------------------------------------------------------------

def _prompt_float(label: str) -> float:
    while True:
        text = input(label).strip()
        try:
            return float(text)
        except ValueError:
            print("Please enter a number.")


def _prompt_optional_float(label: str) -> Optional[float]:
    text = input(label).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        print("Invalid number; ignored.")
        return None


def _find_preferred_heading_files(preferred_dir: Path) -> list[Path]:
    if not preferred_dir.exists():
        return []
    files: list[Path] = []
    for suffix in ("*.csv", "*.json"):
        files.extend(preferred_dir.rglob(suffix))

    def priority(p: Path) -> int:
        name = p.name.lower()
        if "preferred_heading_lookup" in name:
            return 0
        if "preferred" in name and "heading" in name:
            return 1
        if "heading" in name:
            return 2
        return 3

    return sorted(files, key=priority)


def _lookup_preferred_heading_from_csv(path: Path, x_m: float, y_m: float) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        return None

    if not rows:
        return None

    fieldnames = rows[0].keys()

    def first(candidates: list[str]) -> Optional[str]:
        for c in candidates:
            if c in fieldnames:
                return c
        return None

    x_col = first(["x_m", "x", "grid_x_m", "robot_x", "center_x"])
    y_col = first(["y_m", "y", "grid_y_m", "robot_y", "center_y"])
    h_col = first([
        "preferred_heading_deg",
        "best_heading_deg",
        "heading_deg",
        "preferred_heading",
        "best_heading",
        "heading",
    ])

    if x_col is None or y_col is None or h_col is None:
        return None

    best = None
    best_d2 = None
    for r in rows:
        try:
            rx = float(r[x_col])
            ry = float(r[y_col])
            rh = float(r[h_col])
        except Exception:
            continue
        d2 = (rx - x_m) ** 2 + (ry - y_m) ** 2
        if best is None or d2 < best_d2:
            best = {
                "heading_deg": rh % 360.0,
                "source_file": str(path),
                "nearest_x_m": rx,
                "nearest_y_m": ry,
                "nearest_dist_m": math.sqrt(d2),
            }
            best_d2 = d2
    return best


def _lookup_preferred_heading(x_m: float, y_m: float) -> Optional[Dict[str, Any]]:
    for p in _find_preferred_heading_files(PREFERRED_HEADING_DIR):
        if p.suffix.lower() == ".csv":
            result = _lookup_preferred_heading_from_csv(p, x_m, y_m)
        else:
            result = None
        if result is not None:
            return result
    return None


# ---------------------------------------------------------------------------
# Report command helpers
# ---------------------------------------------------------------------------

def _read_report_ts(scanner: str) -> str:
    return utility._hget(key_time(scanner), "last_mobility_report_at", "")


def _read_report(scanner: str) -> Dict[str, Any]:
    return utility._hget_json(key_report(scanner), "last_mobility_report_json")


def _send_direct_location_request(scanner: str) -> str:
    now = utility.local_ts()
    web_cmd_id = f"phase2a-{uuid.uuid4().hex[:12]}"

    fields = {
        "category": "mobility",
        "action": "mobility.report.location",
        "execute_at": now,
        "created_at": now,
        "args_json": "{}",
        "source": "t11_observation_only_direct",
        "web_cmd_id": web_cmd_id,
    }

    stream_id = config.r.xadd(
        config.key_cmd_stream(scanner),
        fields,
        maxlen=5000,
        approximate=True,
    )

    print("\nDirect command enqueued:")
    print(json.dumps({
        "scanner": scanner,
        "stream": config.key_cmd_stream(scanner),
        "stream_id": stream_id,
        "fields": fields,
    }, ensure_ascii=False, indent=2))

    return str(stream_id)


def _wait_for_new_report(scanner: str, old_ts: str) -> Dict[str, Any]:
    deadline = time.time() + float(WAIT_TIMEOUT_SEC)

    while time.time() < deadline:
        cur_ts = _read_report_ts(scanner)
        if cur_ts and cur_ts != old_ts:
            report = _read_report(scanner)
            if report:
                return report
        time.sleep(float(POLL_EVERY_SEC))

    raise TimeoutError(
        f"Timed out waiting for fresh mobility report from {scanner}. "
        f"old_ts={old_ts!r}, timeout={WAIT_TIMEOUT_SEC}s"
    )


# ---------------------------------------------------------------------------
# Geometry / extraction helpers
# ---------------------------------------------------------------------------

def _fmt_float(v: Any, nd: int = 3, width: int = 8) -> str:
    try:
        if v is None:
            return "-".rjust(width)
        return f"{float(v):{width}.{nd}f}"
    except Exception:
        return str(v)[:width].rjust(width)


def _lookup_tag_world(tag_map: Dict[str, Any], tag_id: Any) -> Dict[str, Any]:
    tags = tag_map.get("tags") or {}
    if not isinstance(tags, dict):
        return {}
    return tags.get(str(tag_id)) if isinstance(tags.get(str(tag_id)), dict) else {}


def _camera_geometry(gt_x_m: float, gt_y_m: float, gt_heading_deg: float, camera_role: str) -> Dict[str, float]:
    role = str(camera_role or "").strip().lower()
    if role == "rear":
        camera_heading_deg = utility._deg_norm_360(float(gt_heading_deg) + 180.0)
        camera_forward_offset_m = -0.075
    else:
        camera_heading_deg = utility._deg_norm_360(float(gt_heading_deg))
        camera_forward_offset_m = 0.055

    hrad = utility._deg_to_rad(float(gt_heading_deg))
    camera_x_m = float(gt_x_m) + camera_forward_offset_m * math.cos(hrad)
    camera_y_m = float(gt_y_m) + camera_forward_offset_m * math.sin(hrad)

    return {
        "camera_x_m": camera_x_m,
        "camera_y_m": camera_y_m,
        "camera_heading_deg": camera_heading_deg,
        "camera_forward_offset_m": camera_forward_offset_m,
    }


def _truth_for_observation(
    tag_world: Dict[str, Any],
    gt_x_m: float,
    gt_y_m: float,
    gt_heading_deg: float,
    camera_role: str,
) -> Dict[str, Any]:
    try:
        tag_x = float(tag_world["x_m"])
        tag_y = float(tag_world["y_m"])
        tag_yaw = float(tag_world["yaw_deg"])
    except Exception:
        return {}

    cg = _camera_geometry(gt_x_m, gt_y_m, gt_heading_deg, camera_role)
    cam_x = cg["camera_x_m"]
    cam_y = cg["camera_y_m"]
    cam_h = cg["camera_heading_deg"]

    dx = tag_x - cam_x
    dy = tag_y - cam_y
    true_distance = math.hypot(dx, dy)
    world_bearing = utility._deg_norm_360(math.degrees(math.atan2(dy, dx)))

    # Production convention: positive angle means tag is on camera right side.
    true_angle_cw = utility._wrap_angle_deg(cam_h - world_bearing)

    # Same geometry-side yaw convention used by previous t11 diagnostics.
    true_yaw = utility._wrap_angle_deg(tag_yaw + 180.0 - cam_h + true_angle_cw)

    return {
        "true_distance_m": true_distance,
        "true_angle_deg": true_angle_cw,
        "true_yaw_deg": true_yaw,
        "camera_x_m": cam_x,
        "camera_y_m": cam_y,
        "camera_heading_deg": cam_h,
        "camera_forward_offset_m": cg["camera_forward_offset_m"],
        "world_bearing_deg": world_bearing,
    }


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _angle_diff(a: Any, b: Any) -> Optional[float]:
    try:
        return utility._wrap_angle_deg(float(a) - float(b))
    except Exception:
        return None


def _num_diff(a: Any, b: Any) -> Optional[float]:
    try:
        return float(a) - float(b)
    except Exception:
        return None


def _yaw_keep_flip_quantization(
    meas_yaw_deg: Any,
    true_yaw_deg: Any,
    true_angle_deg: Any,
    camera_role: str,
) -> Dict[str, Any]:
    """
    Diagnostic-only yaw branch analysis.

    Observed failure mode:
    - calibrated geometry/arccos yaw is usually on the right magnitude branch
    - but its sign can flip

    Therefore we compare only two candidates:
    - keep: measured yaw
    - flip: -measured yaw

    If the branch choice is reliable, yaw_quantized_deg is snapped to TrueY
    for diagnostic evaluation. This does not modify solver input.
    """
    y = _safe_float(meas_yaw_deg)
    ty = _safe_float(true_yaw_deg)
    ta = _safe_float(true_angle_deg)

    if y is None or ty is None or ta is None:
        return {
            "yaw_quant_available": False,
            "yaw_accepted": False,
            "yaw_decision": "missing_truth_or_yaw",
        }

    role = str(camera_role or "").strip().lower()
    edge_limit = YAW_EDGE_REAR_TRUEA_DEG if role == "rear" else YAW_EDGE_FRONT_TRUEA_DEG
    truea_abs = abs(float(ta))

    yaw_keep = utility._wrap_angle_deg(float(y))
    yaw_flip = utility._wrap_angle_deg(-float(y))

    err_keep = utility._wrap_angle_deg(yaw_keep - float(ty))
    err_flip = utility._wrap_angle_deg(yaw_flip - float(ty))

    abs_keep = abs(float(err_keep))
    abs_flip = abs(float(err_flip))

    if abs_keep <= abs_flip:
        best_mode = "keep"
        best_yaw = yaw_keep
        best_err = err_keep
        best_abs_err = abs_keep
    else:
        best_mode = "flip"
        best_yaw = yaw_flip
        best_err = err_flip
        best_abs_err = abs_flip

    separation = abs(abs_keep - abs_flip)
    flip_gain = abs_keep - abs_flip

    edge_flag = truea_abs > edge_limit
    small_truey_flag = abs(float(ty)) < YAW_SMALL_TRUEY_DEG

    accepted = False
    quantized_yaw = None
    quantized_error = None

    if edge_flag:
        decision = "reject_edge"
    elif small_truey_flag:
        decision = "weak_small_true_yaw"
    elif best_abs_err <= YAW_ACCEPT_BEST_ERR_DEG and separation >= YAW_ACCEPT_SEPARATION_DEG:
        accepted = True
        decision = f"accept_{best_mode}_quantized"
        quantized_yaw = float(ty)
        quantized_error = 0.0
    elif best_abs_err <= YAW_ACCEPT_BEST_ERR_DEG:
        decision = "ambiguous_low_separation"
    else:
        decision = "poor_keep_flip_match"

    return {
        "yaw_quant_available": True,

        "yaw_keep_deg": yaw_keep,
        "yaw_flip_deg": yaw_flip,

        "yaw_err_keep_deg": err_keep,
        "yaw_err_flip_deg": err_flip,
        "yaw_abs_err_keep_deg": abs_keep,
        "yaw_abs_err_flip_deg": abs_flip,

        "yaw_best_mode": best_mode,
        "yaw_best_deg": best_yaw,
        "yaw_best_err_deg": best_err,
        "yaw_best_abs_err_deg": best_abs_err,

        "yaw_flip_gain_deg": flip_gain,
        "yaw_branch_separation_deg": separation,

        "yaw_edge_flag": edge_flag,
        "yaw_edge_limit_deg": edge_limit,
        "yaw_small_truey_flag": small_truey_flag,
        "yaw_small_truey_limit_deg": YAW_SMALL_TRUEY_DEG,

        "yaw_accepted": accepted,
        "yaw_decision": decision,

        "yaw_sign_corrected_deg": best_yaw if accepted else None,
        "yaw_quantized_deg": quantized_yaw,
        "yaw_quantized_error_deg": quantized_error,
    }


def _mean(values: list[float]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def _median(values: list[float]) -> Optional[float]:
    vals = sorted(float(v) for v in values if v is not None)
    if not vals:
        return None
    n = len(vals)
    mid = n // 2
    if n % 2:
        return vals[mid]
    return 0.5 * (vals[mid - 1] + vals[mid])


def _p90(values: list[float]) -> Optional[float]:
    vals = sorted(float(v) for v in values if v is not None)
    if not vals:
        return None
    idx = int(math.ceil(0.90 * len(vals))) - 1
    idx = max(0, min(idx, len(vals) - 1))
    return vals[idx]


def _summarize_yaw_quantization(observations: list[Dict[str, Any]]) -> Dict[str, Any]:
    qlist = [
        d.get("yaw_keep_flip_quantization") or {}
        for d in observations
        if isinstance(d.get("yaw_keep_flip_quantization"), dict)
        and bool((d.get("yaw_keep_flip_quantization") or {}).get("yaw_quant_available"))
    ]

    counts: Dict[str, int] = {}
    for q in qlist:
        decision = str(q.get("yaw_decision") or "unknown")
        counts[decision] = counts.get(decision, 0) + 1

    accepted = [q for q in qlist if bool(q.get("yaw_accepted"))]
    before_all = [q.get("yaw_abs_err_keep_deg") for q in qlist]
    best_all = [q.get("yaw_best_abs_err_deg") for q in qlist]
    before_accepted = [q.get("yaw_abs_err_keep_deg") for q in accepted]
    best_accepted = [q.get("yaw_best_abs_err_deg") for q in accepted]

    return {
        "enabled": True,
        "diagnostic_only": True,
        "thresholds": {
            "front_edge_truea_deg": YAW_EDGE_FRONT_TRUEA_DEG,
            "rear_edge_truea_deg": YAW_EDGE_REAR_TRUEA_DEG,
            "small_truey_deg": YAW_SMALL_TRUEY_DEG,
            "accept_best_err_deg": YAW_ACCEPT_BEST_ERR_DEG,
            "accept_separation_deg": YAW_ACCEPT_SEPARATION_DEG,
        },
        "count_total_observations": len(observations),
        "count_available": len(qlist),
        "count_accepted": len(accepted),
        "count_accept_keep": sum(1 for q in accepted if q.get("yaw_best_mode") == "keep"),
        "count_accept_flip": sum(1 for q in accepted if q.get("yaw_best_mode") == "flip"),
        "count_by_decision": counts,
        "all_available_abs_error_deg": {
            "before_keep_mean": _mean(before_all),
            "before_keep_median": _median(before_all),
            "before_keep_p90": _p90(before_all),
            "after_best_sign_mean": _mean(best_all),
            "after_best_sign_median": _median(best_all),
            "after_best_sign_p90": _p90(best_all),
        },
        "accepted_only_abs_error_deg": {
            "before_keep_mean": _mean(before_accepted),
            "before_keep_median": _median(before_accepted),
            "before_keep_p90": _p90(before_accepted),
            "after_best_sign_mean": _mean(best_accepted),
            "after_best_sign_median": _median(best_accepted),
            "after_best_sign_p90": _p90(best_accepted),
            "after_quantized_mean": 0.0 if accepted else None,
            "after_quantized_median": 0.0 if accepted else None,
            "after_quantized_p90": 0.0 if accepted else None,
        },
    }


def _extract_apriltag_observations_from_report(
    report: Dict[str, Any],
    tag_map: Dict[str, Any],
    gt_x_m: float,
    gt_y_m: float,
    gt_heading_deg: float,
) -> list[Dict[str, Any]]:
    """
    Extract observations directly from the robot report, independent of solver.
    """
    loc = report.get("last_location_result") or {}
    apr = loc.get("apriltag") or {}
    tags = apr.get("tags") or []
    if not isinstance(tags, list):
        tags = []

    out: list[Dict[str, Any]] = []
    for t in tags:
        if not isinstance(t, dict):
            continue
        try:
            tag_id = int(t.get("id"))
            camera_role = str(t.get("camera_role") or "").strip().lower()
            if camera_role not in ("front", "rear"):
                continue
        except Exception:
            continue

        lib = t.get("library_pose") or {}
        cal = t.get("calibrated_pose") or {}
        geo = t.get("image_geometry") or {}
        if not isinstance(lib, dict):
            lib = {}
        if not isinstance(cal, dict):
            cal = {}
        if not isinstance(geo, dict):
            geo = {}

        meas_distance = _safe_float(cal.get("distance_m", lib.get("distance_m")))
        meas_angle = _safe_float(cal.get("angle_deg", lib.get("angle_deg")))
        meas_yaw = _safe_float(cal.get("yaw_deg", lib.get("yaw_deg")))

        # Keep only observations with the core calibrated/library pose values.
        if meas_distance is None or meas_angle is None or meas_yaw is None:
            continue

        tag_world = _lookup_tag_world(tag_map, tag_id)
        truth = _truth_for_observation(tag_world, gt_x_m, gt_y_m, gt_heading_deg, camera_role)

        yaw_quant = _yaw_keep_flip_quantization(
            meas_yaw_deg=meas_yaw,
            true_yaw_deg=truth.get("true_yaw_deg"),
            true_angle_deg=truth.get("true_angle_deg"),
            camera_role=camera_role,
        )

        item = {
            "tag_id": tag_id,
            "camera_role": camera_role,

            "measured": {
                "distance_m": meas_distance,
                "angle_deg": meas_angle,
                "yaw_deg": meas_yaw,
            },
            "truth": truth,
            "errors": {
                "distance_error_m": _num_diff(meas_distance, truth.get("true_distance_m")),
                "angle_error_deg": _angle_diff(meas_angle, truth.get("true_angle_deg")),
                "yaw_error_deg": _angle_diff(meas_yaw, truth.get("true_yaw_deg")),
            },
            "yaw_keep_flip_quantization": yaw_quant,
            "tag_world": {
                "x_m": tag_world.get("x_m"),
                "y_m": tag_world.get("y_m"),
                "yaw_deg": tag_world.get("yaw_deg"),
                "facing": tag_world.get("facing"),
            },
            "raw_vs_calibrated": {
                "raw_distance_m": lib.get("distance_m"),
                "cal_distance_m": cal.get("distance_m"),
                "raw_angle_deg": lib.get("angle_deg"),
                "cal_angle_deg": cal.get("angle_deg"),
                "raw_yaw_deg": lib.get("yaw_deg"),
                "cal_yaw_deg": cal.get("yaw_deg"),
            },
            "image_geometry": {
                "center_x": geo.get("center_x"),
                "center_y": geo.get("center_y"),
                "avg_width_px": geo.get("avg_width_px"),
                "avg_height_px": geo.get("avg_height_px"),
                "width_height_ratio": geo.get("width_height_ratio"),
                "perspective_skew_lr": geo.get("perspective_skew_lr"),
                "perspective_skew_tb": geo.get("perspective_skew_tb"),
            },
            "snapshot_path": str(t.get("snapshot_path") or ""),
            "source": "robot_report_apriltag_tags",
        }
        out.append(item)

    out.sort(key=lambda d: (str(d.get("camera_role")), int(d.get("tag_id", -1))))
    return out


def _count_by_camera(observations: list[Dict[str, Any]]) -> tuple[int, int]:
    front = sum(1 for x in observations if x.get("camera_role") == "front")
    rear = sum(1 for x in observations if x.get("camera_role") == "rear")
    return front, rear


def _print_observation_error_table(observations: list[Dict[str, Any]]) -> None:
    print("\nObservation-Only Tag Measurement Check")
    print("=" * 118)
    print(
        f"{'Tag':>4} {'Cam':>5} "
        f"{'Dist':>7} {'TrueD':>7} {'DErr':>7} "
        f"{'Ang':>7} {'TrueA':>7} {'AErr':>7} "
        f"{'Yaw':>7} {'TrueY':>7} {'YErr':>7}"
    )
    print("-" * 118)

    if not observations:
        print("(no valid AprilTag observations in robot report)")
        print("=" * 118)
        return

    for d in observations:
        meas = d.get("measured") or {}
        truth = d.get("truth") or {}
        err = d.get("errors") or {}
        print(
            f"{int(d.get('tag_id')):>4} {str(d.get('camera_role') or '')[:5]:>5} "
            f"{_fmt_float(meas.get('distance_m'), 3, 7)} "
            f"{_fmt_float(truth.get('true_distance_m'), 3, 7)} "
            f"{_fmt_float(err.get('distance_error_m'), 3, 7)} "
            f"{_fmt_float(meas.get('angle_deg'), 1, 7)} "
            f"{_fmt_float(truth.get('true_angle_deg'), 1, 7)} "
            f"{_fmt_float(err.get('angle_error_deg'), 1, 7)} "
            f"{_fmt_float(meas.get('yaw_deg'), 1, 7)} "
            f"{_fmt_float(truth.get('true_yaw_deg'), 1, 7)} "
            f"{_fmt_float(err.get('yaw_error_deg'), 1, 7)}"
        )
    print("=" * 118)


def _print_yaw_keep_flip_quantization_table(observations: list[Dict[str, Any]]) -> None:
    print("")
    print("Yaw Keep/Flip Quantization Diagnostic")
    print("=" * 154)
    print(
        f"{'Tag':>4} {'Cam':>5} "
        f"{'TrueA':>7} {'Yaw':>7} {'TrueY':>7} "
        f"{'KeepE':>7} {'FlipE':>7} {'Best':>5} {'BestE':>7} "
        f"{'Gain':>7} {'Sep':>7} {'QYaw':>7} {'Decision':>26}"
    )
    print("-" * 154)

    if not observations:
        print("(no valid AprilTag observations in robot report)")
        print("=" * 154)
        return

    for d in observations:
        meas = d.get("measured") or {}
        truth = d.get("truth") or {}
        q = d.get("yaw_keep_flip_quantization") or {}
        print(
            f"{int(d.get('tag_id')):>4} {str(d.get('camera_role') or '')[:5]:>5} "
            f"{_fmt_float(truth.get('true_angle_deg'), 1, 7)} "
            f"{_fmt_float(meas.get('yaw_deg'), 1, 7)} "
            f"{_fmt_float(truth.get('true_yaw_deg'), 1, 7)} "
            f"{_fmt_float(q.get('yaw_err_keep_deg'), 1, 7)} "
            f"{_fmt_float(q.get('yaw_err_flip_deg'), 1, 7)} "
            f"{str(q.get('yaw_best_mode') or '-')[:5]:>5} "
            f"{_fmt_float(q.get('yaw_best_err_deg'), 1, 7)} "
            f"{_fmt_float(q.get('yaw_flip_gain_deg'), 1, 7)} "
            f"{_fmt_float(q.get('yaw_branch_separation_deg'), 1, 7)} "
            f"{_fmt_float(q.get('yaw_quantized_deg'), 1, 7)} "
            f"{str(q.get('yaw_decision') or '')[:26]:>26}"
        )
    print("=" * 154)


def _write_observation_csv(path: Path, observations: list[Dict[str, Any]]) -> None:
    rows = []
    for d in observations:
        meas = d.get("measured") or {}
        truth = d.get("truth") or {}
        err = d.get("errors") or {}
        tag_world = d.get("tag_world") or {}
        rc = d.get("raw_vs_calibrated") or {}
        geo = d.get("image_geometry") or {}
        rows.append({
            "tag_id": d.get("tag_id"),
            "camera_role": d.get("camera_role"),

            "meas_distance_m": meas.get("distance_m"),
            "true_distance_m": truth.get("true_distance_m"),
            "distance_error_m": err.get("distance_error_m"),

            "meas_angle_deg": meas.get("angle_deg"),
            "true_angle_deg": truth.get("true_angle_deg"),
            "angle_error_deg": err.get("angle_error_deg"),

            "meas_yaw_deg": meas.get("yaw_deg"),
            "true_yaw_deg": truth.get("true_yaw_deg"),
            "yaw_error_deg": err.get("yaw_error_deg"),

            "yaw_keep_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_keep_deg"),
            "yaw_flip_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_flip_deg"),
            "yaw_err_keep_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_err_keep_deg"),
            "yaw_err_flip_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_err_flip_deg"),
            "yaw_abs_err_keep_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_abs_err_keep_deg"),
            "yaw_abs_err_flip_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_abs_err_flip_deg"),
            "yaw_best_mode": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_best_mode"),
            "yaw_best_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_best_deg"),
            "yaw_best_err_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_best_err_deg"),
            "yaw_best_abs_err_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_best_abs_err_deg"),
            "yaw_flip_gain_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_flip_gain_deg"),
            "yaw_branch_separation_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_branch_separation_deg"),
            "yaw_edge_flag": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_edge_flag"),
            "yaw_small_truey_flag": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_small_truey_flag"),
            "yaw_accepted": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_accepted"),
            "yaw_decision": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_decision"),
            "yaw_sign_corrected_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_sign_corrected_deg"),
            "yaw_quantized_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_quantized_deg"),
            "yaw_quantized_error_deg": (d.get("yaw_keep_flip_quantization") or {}).get("yaw_quantized_error_deg"),

            "tag_x_m": tag_world.get("x_m"),
            "tag_y_m": tag_world.get("y_m"),
            "tag_yaw_deg": tag_world.get("yaw_deg"),
            "tag_facing": tag_world.get("facing"),

            "camera_x_m": truth.get("camera_x_m"),
            "camera_y_m": truth.get("camera_y_m"),
            "camera_heading_deg": truth.get("camera_heading_deg"),
            "camera_forward_offset_m": truth.get("camera_forward_offset_m"),
            "world_bearing_deg": truth.get("world_bearing_deg"),

            "raw_distance_m": rc.get("raw_distance_m"),
            "cal_distance_m": rc.get("cal_distance_m"),
            "raw_angle_deg": rc.get("raw_angle_deg"),
            "cal_angle_deg": rc.get("cal_angle_deg"),
            "raw_yaw_deg": rc.get("raw_yaw_deg"),
            "cal_yaw_deg": rc.get("cal_yaw_deg"),

            "center_x": geo.get("center_x"),
            "center_y": geo.get("center_y"),
            "avg_width_px": geo.get("avg_width_px"),
            "avg_height_px": geo.get("avg_height_px"),
            "width_height_ratio": geo.get("width_height_ratio"),
            "perspective_skew_lr": geo.get("perspective_skew_lr"),
            "perspective_skew_tb": geo.get("perspective_skew_tb"),

            "snapshot_path": d.get("snapshot_path"),
        })

    if not rows:
        path.write_text("", encoding="utf-8")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def _make_flat_observations_for_solver_input(observations: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    out = []
    for d in observations:
        meas = d.get("measured") or {}
        out.append({
            "tag_id": int(d.get("tag_id")),
            "camera_role": d.get("camera_role"),
            "distance_m": meas.get("distance_m"),
            "angle_deg": meas.get("angle_deg"),
            "yaw_deg": meas.get("yaw_deg"),
            "source": "t11_observation_only",
        })
    return out


# ---------------------------------------------------------------------------
# Main collection flow
# ---------------------------------------------------------------------------

def run_once(gt_x_m: float, gt_y_m: float, gt_heading_deg: float) -> Dict[str, Any]:
    if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, ROBOT_ID):
        raise RuntimeError(f"{ROBOT_ID} is not whitelisted")

    assets = _ensure_mobility_assets_ready()
    tag_map = _load_tag_map()

    old_ts = _read_report_ts(ROBOT_ID)
    direct_cmd_id = _send_direct_location_request(ROBOT_ID)
    report = _wait_for_new_report(ROBOT_ID, old_ts)

    observations = _extract_apriltag_observations_from_report(
        report=report,
        tag_map=tag_map,
        gt_x_m=gt_x_m,
        gt_y_m=gt_y_m,
        gt_heading_deg=gt_heading_deg,
    )
    front_count, rear_count = _count_by_camera(observations)
    yaw_quant_summary = _summarize_yaw_quantization(observations)

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    full_report_file = LOG_DIR / f"full_report_x{gt_x_m:.2f}_y{gt_y_m:.2f}_h{gt_heading_deg:.0f}_{ts}.json"
    compact_json_file = LOG_DIR / f"obscheck_x{gt_x_m:.2f}_y{gt_y_m:.2f}_h{gt_heading_deg:.0f}_{ts}.json"
    compact_csv_file = LOG_DIR / f"obscheck_x{gt_x_m:.2f}_y{gt_y_m:.2f}_h{gt_heading_deg:.0f}_{ts}.csv"
    obs_file = LOG_DIR / f"obs_x{gt_x_m:.2f}_y{gt_y_m:.2f}_h{gt_heading_deg:.0f}_{ts}.json"
    latest_obs_file = LATEST_OBS_DIR / f"latest_location_observation_{ROBOT_ID}.json"

    payload = {
        "script_version": "t11_observation_only_v2_yaw_keep_flip_quant",
        "robot_id": ROBOT_ID,
        "ground_truth": {
            "x_m": gt_x_m,
            "y_m": gt_y_m,
            "heading_deg": gt_heading_deg,
        },
        "summary": {
            "solver_used": False,
            "tag_count": len(observations),
            "front_count": front_count,
            "rear_count": rear_count,
            "tag_ids": [d.get("tag_id") for d in observations],
            "front_tag_ids": [d.get("tag_id") for d in observations if d.get("camera_role") == "front"],
            "rear_tag_ids": [d.get("tag_id") for d in observations if d.get("camera_role") == "rear"],
            "yaw_keep_flip_quantization": yaw_quant_summary,
        },
        "observations": observations,
        "assets": assets,
        "metadata": {
            "report_time": _read_report_ts(ROBOT_ID),
            "direct_command_stream_id": direct_cmd_id,
            "trial": TRIAL,
            "notes": NOTES,
        },
    }

    if SAVE_FULL_REPORT_JSON:
        with full_report_file.open("w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    else:
        full_report_file = None

    if SAVE_COMPACT_JSON_LOG:
        with compact_json_file.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    if SAVE_COMPACT_CSV_LOG:
        _write_observation_csv(compact_csv_file, observations)

    obs_payload = {
        "script_version": "t11_observation_only_v2_yaw_keep_flip_quant",
        "robot_id": ROBOT_ID,
        "ground_truth": {
            "x_m": gt_x_m,
            "y_m": gt_y_m,
            "heading_deg": gt_heading_deg,
        },
        "observations": _make_flat_observations_for_solver_input(observations),
        "source_log_file": str(compact_json_file),
        "report_time": _read_report_ts(ROBOT_ID),
    }

    with obs_file.open("w", encoding="utf-8") as f:
        json.dump(obs_payload, f, ensure_ascii=False, indent=2)

    with latest_obs_file.open("w", encoding="utf-8") as f:
        json.dump(obs_payload, f, ensure_ascii=False, indent=2)

    # Console output.
    print()
    print(f"GT:             ({gt_x_m:.3f}, {gt_y_m:.3f}, {gt_heading_deg:.1f})")
    print("Location Solver: NOT USED")
    print()
    print(f"Tags Observed  : {len(observations)}")
    print(f"Front / Rear   : {front_count} / {rear_count}")
    print(f"Tag IDs        : {[d.get('tag_id') for d in observations]}")
    if full_report_file is not None:
        print(f"Full Report    : {full_report_file}")
    print(f"ObsCheck JSON  : {compact_json_file}")
    print(f"ObsCheck CSV   : {compact_csv_file}")
    print(f"Obs JSON       : {obs_file}")
    print(f"Latest Obs     : {latest_obs_file}")

    _print_observation_error_table(observations)
    _print_yaw_keep_flip_quantization_table(observations)
    print()

    return payload


if __name__ == "__main__":
    print("")
    print("=" * 72)
    print("T11: DemoRoom observation-only location data collection")
    print("=" * 72)
    print(f"ROOT_DIR              = {ROOT_DIR}")
    print(f"SCANNER_NAME          = {SCANNER_NAME}")
    print(f"PREFERRED_HEADING_DIR = {PREFERRED_HEADING_DIR}")
    print(f"LOG_DIR               = {LOG_DIR}")
    print("Location Solver       = NOT USED")
    print("")

    gt_x_m = _prompt_float("Input ground-truth x_m: ")
    gt_y_m = _prompt_float("Input ground-truth y_m: ")

    preferred = _lookup_preferred_heading(gt_x_m, gt_y_m)
    if preferred is None:
        print("")
        print("Preferred-heading lookup failed.")
        target_heading_deg = _prompt_float("Manually input target heading_deg: ")
    else:
        target_heading_deg = float(preferred["heading_deg"])
        print("")
        print("-" * 72)
        print(
            f"Preferred heading at x={gt_x_m:.3f}, y={gt_y_m:.3f}: "
            f"{target_heading_deg:.1f} deg"
        )
        print(f"Lookup source: {preferred['source_file']}")
        print(
            f"Nearest lookup point: x={preferred['nearest_x_m']:.3f}, "
            f"y={preferred['nearest_y_m']:.3f}, "
            f"distance={preferred['nearest_dist_m']:.3f} m"
        )
        print("-" * 72)

    print("")
    print(f"Please rotate {SCANNER_NAME} to heading {target_heading_deg:.1f} deg.")
    input("Press Enter to send mobility.report.location...")

    actual_heading = _prompt_optional_float(
        f"Input actual heading_deg, or press Enter to use {target_heading_deg:.1f}: "
    )
    if actual_heading is None:
        actual_heading = target_heading_deg

    print("")
    print("Collecting one fresh mobility.report.location sample...")
    run_once(
        gt_x_m=float(gt_x_m),
        gt_y_m=float(gt_y_m),
        gt_heading_deg=float(actual_heading),
    )
