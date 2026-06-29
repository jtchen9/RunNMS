r"""
t11_collect_location_sample.py

Field data-collection helper for DemoRoom AprilTag localization.

This version is optimized for field collection:
- sends one direct mobility.report.location command
- waits for fresh robot report
- prints short console summary
- prints compact true-vs-measured tag table including True Distance
- saves compact JSON/CSV and normalized observation JSON for later t12 analysis
- does NOT print long debug tables by default
- does NOT save huge full JSON by default

Change SCANNER_NAME below if using a different robot.
"""


from __future__ import annotations

import json
import csv
import math
import time
import uuid
import sys
import os
from pathlib import Path
from typing import Any, Dict, Optional
import datetime

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# m8mobility_map uses relative sitemap paths; force project root as cwd.
os.chdir(ROOT_DIR)

import config
import utility

from m8mobility_map import _ensure_mobility_assets_ready, _load_tag_map
from m8mobility_state import _s3_extract_visible_tags, _s3_solve_true_location
from m8mobility_state_store import key_report, key_time


# ---------------------------------------------------------------------------
# Adjustable field-test settings
# ---------------------------------------------------------------------------

SCANNER_NAME = "twin-scout-charlie"

TRIAL = "T11-COLLECT-001"
NOTES = ""

WAIT_TIMEOUT_SEC = 60
POLL_EVERY_SEC = 1.0

# Field-mode console/log switches.
PRINT_LONG_TABLES = False
SAVE_FULL_JSON_LOG = False
SAVE_COMPACT_JSON_LOG = True
SAVE_COMPACT_CSV_LOG = True

TAG_FILE = ROOT_DIR / "sitemap" / "DemoRoom" / "tag_location.txt"

PREFERRED_HEADING_DIR = (
    ROOT_DIR / "testLocation" / "output" / "preferred_heading_full"
)

LOG_DIR = (
    ROOT_DIR / "testLocation" / "output" / "t11_location_collection"
)
LOG_DIR.mkdir(parents=True, exist_ok=True)

LATEST_OBS_DIR = ROOT_DIR / "testLocation" / "input"
LATEST_OBS_DIR.mkdir(parents=True, exist_ok=True)

ROBOT_ID = SCANNER_NAME


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
    files = []
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


def _normalize_observations_for_t12(observations: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    out = []
    for obs in observations:
        if not isinstance(obs, dict):
            continue
        try:
            out.append({
                "tag_id": int(obs.get("id", obs.get("tag_id"))),
                "camera_role": str(obs.get("camera_role") or ""),
                "distance_m": obs.get("distance_m"),
                "angle_deg": obs.get("angle_deg", obs.get("angle_deg_cw")),
                "yaw_deg": obs.get("yaw_deg"),
                "source": "t11_collect_location_sample",
            })
        except Exception:
            continue
    return out


def _read_report_ts(scanner: str) -> str:
    return utility._hget(key_time(scanner), "last_mobility_report_at", "")


def _read_report(scanner: str) -> Dict[str, Any]:
    return utility._hget_json(key_report(scanner), "last_mobility_report_json")


def _send_direct_location_request(scanner: str) -> str:
    """
    Directly enqueue mobility.report.location to the robot command stream.

    This intentionally bypasses:
    - m4Commands._cmd_enqueue_core()
    - m8mobility.on_command_issued()
    - S0/S1/.../S7 state checks

    The robot still receives the command through the normal command polling path.
    """
    now = utility.local_ts()
    web_cmd_id = f"phase2a-{uuid.uuid4().hex[:12]}"

    fields = {
        "category": "mobility",
        "action": "mobility.report.location",
        "execute_at": now,
        "created_at": now,
        "args_json": "{}",
        "source": "phase2a_direct",
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


def _angle_error_deg(est: Optional[float], gt: Optional[float]) -> Optional[float]:
    if est is None or gt is None:
        return None
    return abs(utility._wrap_angle_deg(float(est) - float(gt)))


def _pos_error_m(est_x: Optional[float], est_y: Optional[float], gt_x: float, gt_y: float) -> Optional[float]:
    if est_x is None or est_y is None:
        return None
    return math.hypot(float(est_x) - float(gt_x), float(est_y) - float(gt_y))


def _count_by_camera(obs: list[Dict[str, Any]]) -> tuple[int, int]:
    front = sum(1 for x in obs if x.get("camera_role") == "front")
    rear = sum(1 for x in obs if x.get("camera_role") == "rear")
    return front, rear




def _fmt_float(v: Any, nd: int = 3, width: int = 8) -> str:
    """Format numbers for compact on-site console tables."""
    try:
        if v is None:
            return "-".rjust(width)
        return f"{float(v):{width}.{nd}f}"
    except Exception:
        return str(v)[:width].rjust(width)


def _fmt_tag_list(xs: Any) -> str:
    if not isinstance(xs, list):
        return "[]"
    return "[" + ",".join(str(x) for x in xs) + "]"


def _candidate_key(c: Dict[str, Any]) -> tuple[int, str]:
    return (
        int(c.get("tag_id", -1)),
        str(c.get("camera_role") or ""),
    )


def _build_used_key_set(estimate: Dict[str, Any]) -> set[tuple[int, str]]:
    used = estimate.get("used_candidates") or []
    if isinstance(used, list) and used:
        return {_candidate_key(c) for c in used if isinstance(c, dict)}

    # Fallback for older solver output: tags_used has only tag id.
    tag_ids = estimate.get("tags_used") or []
    out = set()
    if isinstance(tag_ids, list):
        for tid in tag_ids:
            try:
                out.add((int(tid), ""))
            except Exception:
                pass
    return out


def _lookup_tag_world(tag_map: Dict[str, Any], tag_id: Any) -> Dict[str, Any]:
    tags = tag_map.get("tags") or {}
    if not isinstance(tags, dict):
        return {}
    return tags.get(str(tag_id)) if isinstance(tags.get(str(tag_id)), dict) else {}




def _extract_raw_tag_measurements_from_report(report: Dict[str, Any]) -> Dict[tuple[int, str], Dict[str, Any]]:
    """
    Extract raw/library_pose, calibrated_pose, and image_geometry directly
    from the robot mobility report.

    This is intentionally separate from _s3_extract_visible_tags(), because
    _s3_extract_visible_tags() only returns the production-calibrated values
    used by the localization solver.  For calibration debugging we need both
    raw and calibrated values side by side, especially for yaw sign/branch checks.
    """
    out: Dict[tuple[int, str], Dict[str, Any]] = {}

    loc = report.get("last_location_result") or {}
    if not isinstance(loc, dict):
        return out

    apr = loc.get("apriltag") or {}
    if not isinstance(apr, dict):
        return out

    tags = apr.get("tags") or []
    if not isinstance(tags, list):
        return out

    for t in tags:
        if not isinstance(t, dict):
            continue
        try:
            tag_id = int(t.get("id"))
            camera_role = str(t.get("camera_role") or "").strip().lower()
            if camera_role not in ("front", "rear"):
                continue

            lib = t.get("library_pose") or {}
            cal = t.get("calibrated_pose") or {}
            geo = t.get("image_geometry") or {}

            out[(tag_id, camera_role)] = {
                "tag_id": tag_id,
                "camera_role": camera_role,
                "library_pose": lib if isinstance(lib, dict) else {},
                "calibrated_pose": cal if isinstance(cal, dict) else {},
                "image_geometry": geo if isinstance(geo, dict) else {},
                "snapshot_path": str(t.get("snapshot_path") or ""),
            }
        except Exception:
            continue

    return out


def _yaw_branch_flag(true_yaw: Any, raw_yaw: Any, cal_yaw: Any) -> str:
    """
    Flag suspicious yaw behavior for on-site debugging.
    The main case we want to catch is sign/branch flip, e.g.
    true=-15 but raw/cal=+16.
    """
    try:
        ty = float(true_yaw)
        ry = float(raw_yaw)
        cy = float(cal_yaw)
    except Exception:
        return "-"

    # Ignore near-zero truth where sign is ambiguous / dead-zone may apply.
    if abs(ty) < 8.0:
        return "ZERO"

    raw_flip = (ty * ry < 0.0) and abs(ry) > 8.0
    cal_flip = (ty * cy < 0.0) and abs(cy) > 8.0

    if raw_flip and cal_flip:
        return "RAW+CAL_FLIP"
    if raw_flip:
        return "RAW_FLIP"
    if cal_flip:
        return "CAL_FLIP"

    if abs(cy - ty) > 15.0:
        return "BIG_ERR"

    return "OK"


def _make_tag_diagnostics(
    observations: list[Dict[str, Any]],
    estimate: Dict[str, Any],
    tag_map: Dict[str, Any],
    raw_measurements: Dict[tuple[int, str], Dict[str, Any]],
) -> list[Dict[str, Any]]:
    """
    Build one compact diagnostic record per single-tag candidate.

    observations: calibrated camera measurements extracted from latest report
    estimate.candidates: per-tag single-tag robot pose solutions
    estimate.used_candidates: candidates kept by fusion, with residuals if available
    """
    obs_by_key: Dict[tuple[int, str], Dict[str, Any]] = {}
    for obs in observations:
        try:
            obs_by_key[(int(obs.get("id")), str(obs.get("camera_role") or ""))] = obs
        except Exception:
            continue

    candidates = estimate.get("candidates") or []
    if not isinstance(candidates, list):
        candidates = []

    used_keys = _build_used_key_set(estimate)
    hard_keys = {
        _candidate_key(c)
        for c in (estimate.get("hard_outliers") or [])
        if isinstance(c, dict)
    }
    soft_keys = {
        _candidate_key(c)
        for c in (estimate.get("soft_outliers") or [])
        if isinstance(c, dict)
    }

    used_diag_by_key = {}
    for c in (estimate.get("used_candidates") or []):
        if isinstance(c, dict):
            used_diag_by_key[_candidate_key(c)] = c

    est_ok = bool(estimate.get("location_ok"))
    est_x = float(estimate["x_m"]) if est_ok else None
    est_y = float(estimate["y_m"]) if est_ok else None
    est_h = float(estimate["heading_deg"]) if est_ok else None

    out = []
    for c in candidates:
        if not isinstance(c, dict):
            continue

        key = _candidate_key(c)
        obs = obs_by_key.get(key, {})
        raw = raw_measurements.get(key, {}) if isinstance(raw_measurements, dict) else {}
        lib_pose = raw.get("library_pose") or {}
        cal_pose = raw.get("calibrated_pose") or {}
        img_geo = raw.get("image_geometry") or {}
        tag_world = _lookup_tag_world(tag_map, c.get("tag_id"))
        used_diag = used_diag_by_key.get(key, {})

        cx = float(c.get("x_m"))
        cy = float(c.get("y_m"))
        ch = float(c.get("heading_deg"))

        if est_ok:
            pos_res = math.hypot(cx - float(est_x), cy - float(est_y))
            head_res = abs(utility._wrap_angle_deg(ch - float(est_h)))
        else:
            pos_res = None
            head_res = None

        item = {
            "tag_id": int(c.get("tag_id")),
            "camera_role": str(c.get("camera_role") or ""),
            "used_in_solution": (key in used_keys) or ((int(c.get("tag_id")), "") in used_keys),
            "hard_outlier": key in hard_keys,
            "soft_outlier": key in soft_keys,

            "tag_world": {
                "x_m": tag_world.get("x_m"),
                "y_m": tag_world.get("y_m"),
                "yaw_deg": tag_world.get("yaw_deg"),
                "facing": tag_world.get("facing"),
            },

            "observed": {
                "distance_m": obs.get("distance_m", c.get("distance_m")),
                "angle_deg_cw": obs.get("angle_deg_cw", c.get("angle_deg_cw")),
                "yaw_deg": obs.get("yaw_deg", c.get("yaw_deg")),
                "bearing_robot_deg_ccw": obs.get("bearing_robot_deg_ccw", c.get("bearing_robot_deg_ccw")),
                "camera_forward_offset_m": obs.get("camera_forward_offset_m", c.get("camera_forward_offset_m")),
            },

            "raw_vs_calibrated": {
                "raw_distance_m": lib_pose.get("distance_m"),
                "cal_distance_m": cal_pose.get("distance_m", obs.get("distance_m", c.get("distance_m"))),
                "raw_angle_deg": lib_pose.get("angle_deg"),
                "cal_angle_deg": cal_pose.get("angle_deg", obs.get("angle_deg_cw", c.get("angle_deg_cw"))),
                "raw_yaw_deg": lib_pose.get("yaw_deg"),
                "cal_yaw_deg": cal_pose.get("yaw_deg", obs.get("yaw_deg", c.get("yaw_deg"))),
            },

            "image_geometry": {
                "center_x": img_geo.get("center_x"),
                "center_y": img_geo.get("center_y"),
                "avg_width_px": img_geo.get("avg_width_px"),
                "avg_height_px": img_geo.get("avg_height_px"),
                "width_height_ratio": img_geo.get("width_height_ratio"),
                "perspective_skew_lr": img_geo.get("perspective_skew_lr"),
                "perspective_skew_tb": img_geo.get("perspective_skew_tb"),
            },

            "single_tag_solution": {
                "x_m": c.get("x_m"),
                "y_m": c.get("y_m"),
                "heading_deg": c.get("heading_deg"),
                "camera_x_m": c.get("camera_x_m"),
                "camera_y_m": c.get("camera_y_m"),
            },

            "residual_to_final": {
                "position_m": used_diag.get("position_residual_m", pos_res),
                "heading_deg": used_diag.get("heading_residual_deg", head_res),
            },

            "weight": used_diag.get("weight"),
            "snapshot_path": c.get("snapshot_path") or obs.get("snapshot_path") or "",
        }
        out.append(item)

    out.sort(
        key=lambda d: (
            float(d["residual_to_final"].get("position_m") or 0.0),
            float(d["residual_to_final"].get("heading_deg") or 0.0),
        ),
        reverse=True,
    )
    return out


def _print_visible_tag_table(observations: list[Dict[str, Any]], tag_map: Dict[str, Any]) -> None:
    print("\nVisible Tag Observations")
    print("=" * 118)
    print(
        f"{'Tag':>4} {'Cam':>5} "
        f"{'TagX':>7} {'TagY':>7} {'TagYaw':>7} {'Face':>6} "
        f"{'Dist':>7} {'AngCW':>7} {'Yaw':>7} {'BearCCW':>8} {'CamOff':>7}"
    )
    print("-" * 118)

    if not observations:
        print("(no usable tag observations)")
        return

    for obs in sorted(observations, key=lambda x: (str(x.get("camera_role")), int(x.get("id", -1)))):
        tw = _lookup_tag_world(tag_map, obs.get("id"))
        print(
            f"{int(obs.get('id')):>4} {str(obs.get('camera_role') or '')[:5]:>5} "
            f"{_fmt_float(tw.get('x_m'), 2, 7)} {_fmt_float(tw.get('y_m'), 2, 7)} "
            f"{_fmt_float(tw.get('yaw_deg'), 1, 7)} {str(tw.get('facing') or '-')[:6]:>6} "
            f"{_fmt_float(obs.get('distance_m'), 3, 7)} "
            f"{_fmt_float(obs.get('angle_deg_cw'), 1, 7)} "
            f"{_fmt_float(obs.get('yaw_deg'), 1, 7)} "
            f"{_fmt_float(obs.get('bearing_robot_deg_ccw'), 1, 8)} "
            f"{_fmt_float(obs.get('camera_forward_offset_m'), 3, 7)}"
        )
    print("=" * 118)


def _print_single_tag_solution_table(tag_diagnostics: list[Dict[str, Any]]) -> None:
    print("\nSingle-Tag Robot Pose Solutions")
    print("=" * 118)
    print(
        f"{'Tag':>4} {'Cam':>5} {'Use':>4} {'Out':>5} "
        f"{'X':>8} {'Y':>8} {'Head':>8} "
        f"{'CamX':>8} {'CamY':>8} {'Weight':>8} {'Snapshot':<20}"
    )
    print("-" * 118)

    if not tag_diagnostics:
        print("(no single-tag candidates)")
        return

    for d in sorted(tag_diagnostics, key=lambda x: (x.get("camera_role", ""), int(x.get("tag_id", -1)))):
        sol = d.get("single_tag_solution") or {}
        use = "Y" if d.get("used_in_solution") else "N"
        if d.get("hard_outlier"):
            out = "HARD"
        elif d.get("soft_outlier"):
            out = "SOFT"
        else:
            out = "-"
        print(
            f"{int(d.get('tag_id')):>4} {str(d.get('camera_role') or '')[:5]:>5} {use:>4} {out:>5} "
            f"{_fmt_float(sol.get('x_m'), 3, 8)} {_fmt_float(sol.get('y_m'), 3, 8)} "
            f"{_fmt_float(sol.get('heading_deg'), 1, 8)} "
            f"{_fmt_float(sol.get('camera_x_m'), 3, 8)} {_fmt_float(sol.get('camera_y_m'), 3, 8)} "
            f"{_fmt_float(d.get('weight'), 3, 8)} "
            f"{str(d.get('snapshot_path') or '')[-20:]:<20}"
        )
    print("=" * 118)


def _print_final_residual_table(tag_diagnostics: list[Dict[str, Any]]) -> None:
    print("\nResiduals To Final Fused Solution  (sorted worst first)")
    print("=" * 86)
    print(
        f"{'Tag':>4} {'Cam':>5} {'Use':>4} {'Out':>5} "
        f"{'PosRes(m)':>10} {'HeadRes':>8} {'Dist':>7} {'AngCW':>7} {'Yaw':>7}"
    )
    print("-" * 86)

    if not tag_diagnostics:
        print("(no residuals)")
        return

    for d in tag_diagnostics:
        obs = d.get("observed") or {}
        res = d.get("residual_to_final") or {}
        use = "Y" if d.get("used_in_solution") else "N"
        if d.get("hard_outlier"):
            out = "HARD"
        elif d.get("soft_outlier"):
            out = "SOFT"
        else:
            out = "-"
        print(
            f"{int(d.get('tag_id')):>4} {str(d.get('camera_role') or '')[:5]:>5} {use:>4} {out:>5} "
            f"{_fmt_float(res.get('position_m'), 3, 10)} "
            f"{_fmt_float(res.get('heading_deg'), 1, 8)} "
            f"{_fmt_float(obs.get('distance_m'), 3, 7)} "
            f"{_fmt_float(obs.get('angle_deg_cw'), 1, 7)} "
            f"{_fmt_float(obs.get('yaw_deg'), 1, 7)}"
        )
    print("=" * 86)




def _print_yaw_calibration_debug_table(tag_diagnostics: list[Dict[str, Any]]) -> None:
    """
    Dedicated yaw debug table.

    Purpose:
    - Show whether yaw sign/branch problem exists before calibration or is
      introduced by calibration.
    - Compare TrueYaw (from ground-truth robot pose + tag map) with raw yaw
      and calibrated yaw from the robot report.
    """
    print("\nYaw Calibration / Sign-Branch Debug  (sorted by |CalYaw-TrueYaw|)")
    print("=" * 132)
    print(
        f"{'Tag':>4} {'Cam':>5} "
        f"{'TrueYaw':>8} {'RawYaw':>8} {'CalYaw':>8} "
        f"{'RawErr':>8} {'CalErr':>8} {'Flag':>13} "
        f"{'TrueAng':>8} {'RawAng':>8} {'CalAng':>8} "
        f"{'RawD':>7} {'CalD':>7} {'WH':>6} {'Skew':>7}"
    )
    print("-" * 132)

    if not tag_diagnostics:
        print("(no yaw diagnostics)")
        return

    rows = []
    for d in tag_diagnostics:
        obs_truth = d.get("manual_truth") or {}
        rc = d.get("raw_vs_calibrated") or {}
        geo = d.get("image_geometry") or {}

        true_yaw = obs_truth.get("true_yaw_deg")
        raw_yaw = rc.get("raw_yaw_deg")
        cal_yaw = rc.get("cal_yaw_deg")

        try:
            raw_err = utility._wrap_angle_deg(float(raw_yaw) - float(true_yaw))
        except Exception:
            raw_err = None
        try:
            cal_err = utility._wrap_angle_deg(float(cal_yaw) - float(true_yaw))
        except Exception:
            cal_err = None

        flag = _yaw_branch_flag(true_yaw, raw_yaw, cal_yaw)
        rows.append((abs(float(cal_err)) if cal_err is not None else -1.0, d, raw_err, cal_err, flag))

    rows.sort(key=lambda x: x[0], reverse=True)

    for _, d, raw_err, cal_err, flag in rows:
        obs_truth = d.get("manual_truth") or {}
        rc = d.get("raw_vs_calibrated") or {}
        geo = d.get("image_geometry") or {}
        print(
            f"{int(d.get('tag_id')):>4} {str(d.get('camera_role') or '')[:5]:>5} "
            f"{_fmt_float(obs_truth.get('true_yaw_deg'), 1, 8)} "
            f"{_fmt_float(rc.get('raw_yaw_deg'), 1, 8)} "
            f"{_fmt_float(rc.get('cal_yaw_deg'), 1, 8)} "
            f"{_fmt_float(raw_err, 1, 8)} "
            f"{_fmt_float(cal_err, 1, 8)} "
            f"{flag[:13]:>13} "
            f"{_fmt_float(obs_truth.get('true_angle_deg'), 1, 8)} "
            f"{_fmt_float(rc.get('raw_angle_deg'), 1, 8)} "
            f"{_fmt_float(rc.get('cal_angle_deg'), 1, 8)} "
            f"{_fmt_float(rc.get('raw_distance_m'), 3, 7)} "
            f"{_fmt_float(rc.get('cal_distance_m'), 3, 7)} "
            f"{_fmt_float(geo.get('width_height_ratio'), 3, 6)} "
            f"{_fmt_float(geo.get('perspective_skew_lr'), 3, 7)}"
        )
    print("=" * 132)


def _add_manual_truth_to_tag_diagnostics(
    tag_diagnostics: list[Dict[str, Any]],
    gt_x_m: float,
    gt_y_m: float,
    gt_heading_deg: float,
) -> None:
    """
    Add true distance/angle/yaw implied by the manually supplied robot pose
    and the tag map. This is independent from the localization solver.
    """
    for d in tag_diagnostics:
        tw = d.get("tag_world") or {}
        try:
            tag_x = float(tw["x_m"])
            tag_y = float(tw["y_m"])
            tag_yaw = float(tw["yaw_deg"])
        except Exception:
            d["manual_truth"] = {}
            continue

        try:
            cam_role = str(d.get("camera_role") or "").strip().lower()
            # Same mechanical offsets/convention as production solver.
            if cam_role == "front":
                cam_head = utility._deg_norm_360(float(gt_heading_deg))
                cam_off_m = 0.055
            elif cam_role == "rear":
                cam_head = utility._deg_norm_360(float(gt_heading_deg) + 180.0)
                cam_off_m = -0.075
            else:
                cam_head = utility._deg_norm_360(float(gt_heading_deg))
                cam_off_m = 0.0

            # Camera center from robot center.
            hrad = utility._deg_to_rad(float(gt_heading_deg))
            cam_x = float(gt_x_m) + cam_off_m * math.cos(hrad)
            cam_y = float(gt_y_m) + cam_off_m * math.sin(hrad)

            dx = tag_x - cam_x
            dy = tag_y - cam_y
            true_dist = math.hypot(dx, dy)
            world_bearing = utility._deg_norm_360(math.degrees(math.atan2(dy, dx)))

            # Production convention: angle_deg > 0 means tag is on camera right side.
            # World math is CCW-positive, so angle_cw = camera_heading - world_bearing.
            true_angle_cw = utility._wrap_angle_deg(cam_head - world_bearing)

            # Approximate the same physical relative tag yaw quantity used by solver:
            # yaw_corrected = tag_yaw_world + 180 - camera_heading.
            # The measured yaw before correction roughly includes angle coupling, but this
            # is the best geometry-side truth for sign/branch debugging.
            true_yaw = utility._wrap_angle_deg(tag_yaw + 180.0 - cam_head + true_angle_cw)

            d["manual_truth"] = {
                "true_distance_m": true_dist,
                "true_angle_deg": true_angle_cw,
                "true_yaw_deg": true_yaw,
                "camera_x_m": cam_x,
                "camera_y_m": cam_y,
                "camera_heading_deg": cam_head,
            }
        except Exception:
            d["manual_truth"] = {}




def _print_compact_tag_error_table(tag_diagnostics: list[Dict[str, Any]]) -> None:
    print("\nCompact Tag Measurement Check")
    print("=" * 112)
    print(
        f"{'Tag':>4} {'Cam':>5} {'Use':>4} "
        f"{'Dist':>7} {'TrueD':>7} {'DErr':>7} "
        f"{'Ang':>7} {'TrueA':>7} {'AErr':>7} "
        f"{'Yaw':>7} {'TrueY':>7} {'YErr':>7}"
    )
    print("-" * 112)

    if not tag_diagnostics:
        print("(no tag diagnostics)")
        print("=" * 112)
        return

    for d in sorted(tag_diagnostics, key=lambda x: (str(x.get("camera_role")), int(x.get("tag_id", -1)))):
        obs = d.get("observed") or {}
        truth = d.get("manual_truth") or {}

        dist = obs.get("distance_m")
        true_dist = truth.get("true_distance_m")
        ang = obs.get("angle_deg_cw")
        true_ang = truth.get("true_angle_deg")
        yaw = obs.get("yaw_deg")
        true_yaw = truth.get("true_yaw_deg")

        try:
            dist_err = float(dist) - float(true_dist)
        except Exception:
            dist_err = None
        try:
            ang_err = utility._wrap_angle_deg(float(ang) - float(true_ang))
        except Exception:
            ang_err = None
        try:
            yaw_err = utility._wrap_angle_deg(float(yaw) - float(true_yaw))
        except Exception:
            yaw_err = None

        use = "Y" if d.get("used_in_solution") else "N"

        print(
            f"{int(d.get('tag_id')):>4} {str(d.get('camera_role') or '')[:5]:>5} {use:>4} "
            f"{_fmt_float(dist, 3, 7)} {_fmt_float(true_dist, 3, 7)} {_fmt_float(dist_err, 3, 7)} "
            f"{_fmt_float(ang, 1, 7)} {_fmt_float(true_ang, 1, 7)} {_fmt_float(ang_err, 1, 7)} "
            f"{_fmt_float(yaw, 1, 7)} {_fmt_float(true_yaw, 1, 7)} {_fmt_float(yaw_err, 1, 7)}"
        )

    print("=" * 112)


def _write_compact_tag_csv(path: Path, tag_diagnostics: list[Dict[str, Any]]) -> None:
    rows = []

    def safe_err(a, b, angle=False):
        try:
            if angle:
                return utility._wrap_angle_deg(float(a) - float(b))
            return float(a) - float(b)
        except Exception:
            return None

    for d in tag_diagnostics:
        obs = d.get("observed") or {}
        truth = d.get("manual_truth") or {}
        rc = d.get("raw_vs_calibrated") or {}
        res = d.get("residual_to_final") or {}

        rows.append({
            "tag_id": d.get("tag_id"),
            "camera_role": d.get("camera_role"),
            "used_in_solution": d.get("used_in_solution"),
            "hard_outlier": d.get("hard_outlier"),
            "soft_outlier": d.get("soft_outlier"),
            "meas_distance_m": obs.get("distance_m"),
            "true_distance_m": truth.get("true_distance_m"),
            "distance_error_m": safe_err(obs.get("distance_m"), truth.get("true_distance_m")),
            "meas_angle_deg": obs.get("angle_deg_cw"),
            "true_angle_deg": truth.get("true_angle_deg"),
            "angle_error_deg": safe_err(obs.get("angle_deg_cw"), truth.get("true_angle_deg"), angle=True),
            "meas_yaw_deg": obs.get("yaw_deg"),
            "true_yaw_deg": truth.get("true_yaw_deg"),
            "yaw_error_deg": safe_err(obs.get("yaw_deg"), truth.get("true_yaw_deg"), angle=True),
            "raw_distance_m": rc.get("raw_distance_m"),
            "cal_distance_m": rc.get("cal_distance_m"),
            "raw_angle_deg": rc.get("raw_angle_deg"),
            "cal_angle_deg": rc.get("cal_angle_deg"),
            "raw_yaw_deg": rc.get("raw_yaw_deg"),
            "cal_yaw_deg": rc.get("cal_yaw_deg"),
            "residual_to_final_position_m": res.get("position_m"),
            "residual_to_final_heading_deg": res.get("heading_deg"),
            "snapshot_path": d.get("snapshot_path"),
        })

    if not rows:
        return

    keys = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)

def run_once(gt_x_m: float, gt_y_m: float, gt_heading_deg: float) -> Dict[str, Any]:
    if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, ROBOT_ID):
        raise RuntimeError(f"{ROBOT_ID} is not whitelisted")

    assets = _ensure_mobility_assets_ready()

    old_ts = _read_report_ts(ROBOT_ID)
    direct_cmd_id = _send_direct_location_request(ROBOT_ID)

    report = _wait_for_new_report(ROBOT_ID, old_ts)

    observations = _s3_extract_visible_tags(ROBOT_ID)
    front_count, rear_count = _count_by_camera(observations)
    raw_measurements = _extract_raw_tag_measurements_from_report(report)

    estimate = _s3_solve_true_location(ROBOT_ID)
    tag_map = _load_tag_map()
    tag_diagnostics = _make_tag_diagnostics(observations, estimate, tag_map, raw_measurements)
    _add_manual_truth_to_tag_diagnostics(tag_diagnostics, gt_x_m, gt_y_m, gt_heading_deg)

    est_ok = bool(estimate.get("location_ok"))

    est_x = float(estimate["x_m"]) if est_ok else None
    est_y = float(estimate["y_m"]) if est_ok else None
    est_h = float(estimate["heading_deg"]) if est_ok else None

    pos_err = _pos_error_m(est_x, est_y, gt_x_m, gt_y_m)
    heading_err = _angle_error_deg(est_h, gt_heading_deg)

    # --------------------------------------------------
    # Save complete JSON log
    # --------------------------------------------------

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    log_file = (
        LOG_DIR /
        f"loc_x{gt_x_m:.2f}_y{gt_y_m:.2f}_h{gt_heading_deg:.0f}_{ts}.json"
    )

    compact_json_file = (
        LOG_DIR /
        f"compact_x{gt_x_m:.2f}_y{gt_y_m:.2f}_h{gt_heading_deg:.0f}_{ts}.json"
    )

    compact_csv_file = (
        LOG_DIR /
        f"compact_x{gt_x_m:.2f}_y{gt_y_m:.2f}_h{gt_heading_deg:.0f}_{ts}.csv"
    )

    payload = {
        "script_version": "phase2a_v4_yaw_raw_cal_debug",
        "robot_id": ROBOT_ID,

        "ground_truth": {
            "x_m": gt_x_m,
            "y_m": gt_y_m,
            "heading_deg": gt_heading_deg,
        },

        "errors": {
            "position_error_m": pos_err,
            "heading_error_deg": heading_err,
        },

        "summary": {
            "estimate_ok": est_ok,
            "tag_count": len(observations),
            "front_count": front_count,
            "rear_count": rear_count,
            "tags_used": estimate.get("tags_used", []),
        },

        "assets": assets,
        "report": report,
        "observations": observations,
        "raw_measurements": [v for v in raw_measurements.values()],
        "estimate": estimate,
        "tag_diagnostics": tag_diagnostics,

        "metadata": {
            "report_time": _read_report_ts(ROBOT_ID),
            "direct_command_stream_id": direct_cmd_id,
            "trial": TRIAL,
            "notes": NOTES,
        },
    }

    if SAVE_FULL_JSON_LOG:
        with log_file.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    else:
        log_file = None

    compact_payload = {
        "script_version": "t11_collect_location_sample_compact",
        "robot_id": ROBOT_ID,
        "ground_truth": {
            "x_m": gt_x_m,
            "y_m": gt_y_m,
            "heading_deg": gt_heading_deg,
        },
        "errors": {
            "position_error_m": pos_err,
            "heading_error_deg": heading_err,
        },
        "summary": {
            "estimate_ok": est_ok,
            "tag_count": len(observations),
            "front_count": front_count,
            "rear_count": rear_count,
            "tags_used": estimate.get("tags_used", []),
            "solver_stage": estimate.get("solver_stage", ""),
            "solver_detail": estimate.get("detail", ""),
        },
        "observations": _normalize_observations_for_t12(observations),
        "tag_diagnostics": tag_diagnostics,
        "metadata": {
            "report_time": _read_report_ts(ROBOT_ID),
            "direct_command_stream_id": direct_cmd_id,
            "trial": TRIAL,
            "notes": NOTES,
        },
    }

    if SAVE_COMPACT_JSON_LOG:
        with compact_json_file.open("w", encoding="utf-8") as f:
            json.dump(compact_payload, f, ensure_ascii=False, indent=2)

    if SAVE_COMPACT_CSV_LOG:
        _write_compact_tag_csv(compact_csv_file, tag_diagnostics)

    normalized_observations = _normalize_observations_for_t12(observations)
    obs_payload = {
        "script_version": "t11_collect_location_sample",
        "robot_id": ROBOT_ID,
        "ground_truth": {
            "x_m": gt_x_m,
            "y_m": gt_y_m,
            "heading_deg": gt_heading_deg,
        },
        "observations": normalized_observations,
        "source_log_file": str(compact_json_file),
        "report_time": _read_report_ts(ROBOT_ID),
    }

    obs_file = LOG_DIR / f"obs_x{gt_x_m:.2f}_y{gt_y_m:.2f}_h{gt_heading_deg:.0f}_{ts}.json"
    latest_obs_file = LATEST_OBS_DIR / f"latest_location_observation_{ROBOT_ID}.json"

    with obs_file.open("w", encoding="utf-8") as f:
        json.dump(obs_payload, f, ensure_ascii=False, indent=2)

    with latest_obs_file.open("w", encoding="utf-8") as f:
        json.dump(obs_payload, f, ensure_ascii=False, indent=2)

    # --------------------------------------------------
    # Console output (short version)
    # --------------------------------------------------

    print()

    print(
        f"GT:        "
        f"({gt_x_m:.3f}, {gt_y_m:.3f}, {gt_heading_deg:.1f})"
    )

    if est_ok:
        print(
            f"Estimate:  "
            f"({est_x:.3f}, {est_y:.3f}, {est_h:.1f})"
        )

        print()
        print(f"Position Error : {pos_err:.3f} m")
        print(f"Heading Error  : {heading_err:.2f} deg")

    else:
        print("Estimate:  FAILED")

        print()
        print("Position Error : N/A")
        print("Heading Error  : N/A")

    print()
    print(f"Tags Seen      : {len(observations)}")
    print(f"Front / Rear   : {front_count} / {rear_count}")
    print(f"Tags Used      : {estimate.get('tags_used', [])}")
    print(f"Solver Stage   : {estimate.get('solver_stage', '')}")
    print(f"Solver Detail  : {estimate.get('detail', '')}")
    if log_file is not None:
        print(f"Full JSON Log  : {log_file}")
    print(f"Compact JSON   : {compact_json_file}")
    print(f"Compact CSV    : {compact_csv_file}")
    print(f"Obs JSON       : {obs_file}")
    print(f"Latest Obs     : {latest_obs_file}")

    _print_compact_tag_error_table(tag_diagnostics)

    if PRINT_LONG_TABLES:
        _print_visible_tag_table(observations, tag_map)
        _print_yaw_calibration_debug_table(tag_diagnostics)
        _print_single_tag_solution_table(tag_diagnostics)
        _print_final_residual_table(tag_diagnostics)

    print()

    return payload


if __name__ == "__main__":
    print("")
    print("=" * 72)
    print("T11: DemoRoom location data collection")
    print("=" * 72)
    print(f"ROOT_DIR              = {ROOT_DIR}")
    print(f"SCANNER_NAME          = {SCANNER_NAME}")
    print(f"PREFERRED_HEADING_DIR = {PREFERRED_HEADING_DIR}")
    print(f"LOG_DIR               = {LOG_DIR}")
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
