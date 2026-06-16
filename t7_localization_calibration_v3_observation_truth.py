"""
Phase 2A helper: AprilTag localization calibration, direct-command version.

This script bypasses the mobility state machine completely.

It directly sends one command to the robot command stream:

    category = mobility
    action   = mobility.report.location

Then it waits for the robot to report front/rear AprilTag observations through
the normal /cmd/poll/{scanner} mobility_report path. After the report arrives,
it calls the NMS S3 localization solver and appends one CSV row comparing:

    estimated pose vs manually supplied ground-truth pose

Usage:

    python phase2a_localization_calibration.py --x 5.45 --y 2.0 --h 270

Arguments:
    --x : ground-truth robot x coordinate in meters
    --y : ground-truth robot y coordinate in meters
    --h : ground-truth robot heading in degrees
"""

from __future__ import annotations

import argparse
import json
import math
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional
import datetime

import config
import utility

from m8mobility_map import _ensure_mobility_assets_ready, _load_tag_map
from m8mobility_state import _s3_extract_visible_tags, _s3_solve_true_location
from m8mobility_state_store import key_report, key_time


LOG_DIR = Path("location_data")
LOG_DIR.mkdir(parents=True, exist_ok=True)

ROBOT_ID = "twin-scout-delta"
TRIAL = "P2A-001"
NOTES = ""

WAIT_TIMEOUT_SEC = 60
POLL_EVERY_SEC = 1.0


def _parse_args():
    p = argparse.ArgumentParser(
        description="Phase 2A AprilTag localization calibration helper"
    )
    p.add_argument("--x", type=float, required=True, help="Ground-truth x coordinate in meters")
    p.add_argument("--y", type=float, required=True, help="Ground-truth y coordinate in meters")
    p.add_argument("--h", type=float, required=True, help="Ground-truth heading in degrees")
    return p.parse_args()


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


def _extract_report_tag_details(report: Dict[str, Any]) -> Dict[tuple[int, str], Dict[str, Any]]:
    """
    Extract raw/library pose, calibrated pose, and image geometry from the original
    mobility report.  This is independent from _s3_extract_visible_tags(), which
    intentionally returns only calibrated production observations.
    """
    out: Dict[tuple[int, str], Dict[str, Any]] = {}

    loc = report.get("last_location_result") or {}
    if not isinstance(loc, dict):
        return out

    apr = loc.get("apriltag") or {}
    if not isinstance(apr, dict):
        return out

    tags = apr.get("tags") or []
    if isinstance(tags, list):
        for t in tags:
            if not isinstance(t, dict):
                continue
            try:
                tag_id = int(t.get("id"))
                role = str(t.get("camera_role") or "").strip().lower()
                if role not in ("front", "rear"):
                    continue
                out[(tag_id, role)] = {
                    "library_pose": t.get("library_pose") or {},
                    "calibrated_pose": t.get("calibrated_pose") or {},
                    "image_geometry": t.get("image_geometry") or {},
                    "snapshot_path": str(t.get("snapshot_path") or ""),
                }
            except Exception:
                continue

    # Some older reports keep detailed tag geometry under apriltag.front.tags / apriltag.rear.tags.
    # Merge those details if present.
    for role in ("front", "rear"):
        sub = apr.get(role) or {}
        if not isinstance(sub, dict):
            continue
        sub_tags = sub.get("tags") or []
        if not isinstance(sub_tags, list):
            continue
        for t in sub_tags:
            if not isinstance(t, dict):
                continue
            try:
                tag_id = int(t.get("id"))
                key = (tag_id, role)
                item = out.setdefault(key, {})
                item.setdefault("library_pose", t.get("library_pose") or {})
                item.setdefault("calibrated_pose", t.get("calibrated_pose") or {})
                if t.get("image_geometry"):
                    item["image_geometry"] = t.get("image_geometry") or {}
                if not item.get("snapshot_path"):
                    item["snapshot_path"] = str(t.get("snapshot_path") or sub.get("snapshot_path") or "")
            except Exception:
                continue

    return out


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _camera_center_from_gt(
    gt_x_m: float,
    gt_y_m: float,
    gt_heading_deg: float,
    camera_forward_offset_m: float,
) -> tuple[float, float]:
    h_rad = utility._deg_to_rad(float(gt_heading_deg))
    return (
        float(gt_x_m) + float(camera_forward_offset_m) * math.cos(h_rad),
        float(gt_y_m) + float(camera_forward_offset_m) * math.sin(h_rad),
    )


def _expected_observation_from_ground_truth(
    obs: Dict[str, Any],
    tag_world: Dict[str, Any],
    gt_x_m: float,
    gt_y_m: float,
    gt_heading_deg: float,
) -> Dict[str, Any]:
    """
    Compute the expected camera-to-tag distance/angle/yaw from the manually supplied
    ground-truth robot pose and the tag map.  This is the direct test of whether
    the tag observation itself is plausible before fusion or map-solving hides it.
    """
    if not isinstance(tag_world, dict) or not tag_world:
        return {}

    try:
        tag_x = float(tag_world["x_m"])
        tag_y = float(tag_world["y_m"])
        tag_yaw = float(tag_world["yaw_deg"])

        camera_role = str(obs.get("camera_role") or "").strip().lower()
        camera_offset_deg = float(obs.get("camera_offset_deg") or (180.0 if camera_role == "rear" else 0.0))
        camera_forward_offset_m = float(obs.get("camera_forward_offset_m") or 0.0)

        cam_x, cam_y = _camera_center_from_gt(
            gt_x_m,
            gt_y_m,
            gt_heading_deg,
            camera_forward_offset_m,
        )

        dx = tag_x - cam_x
        dy = tag_y - cam_y
        distance_m = math.hypot(dx, dy)

        world_bearing_deg = utility._deg_norm_360(math.degrees(math.atan2(dy, dx)))
        camera_heading_deg = utility._deg_norm_360(float(gt_heading_deg) + camera_offset_deg)

        # Existing production convention: angle_deg > 0 means tag is on camera's right side.
        angle_deg_cw = utility._wrap_angle_deg(camera_heading_deg - world_bearing_deg)

        # Inverse of production yaw relation:
        #   yaw_corrected = yaw - angle_cw
        #   camera_heading = tag_yaw + 180 - yaw_corrected
        # therefore:
        #   yaw = tag_yaw + 180 - camera_heading + angle_cw
        yaw_deg = utility._wrap_angle_deg(tag_yaw + 180.0 - camera_heading_deg + angle_deg_cw)

        return {
            "camera_x_m": cam_x,
            "camera_y_m": cam_y,
            "distance_m": distance_m,
            "angle_deg_cw": angle_deg_cw,
            "yaw_deg": yaw_deg,
            "world_bearing_deg": world_bearing_deg,
            "camera_heading_deg": camera_heading_deg,
        }
    except Exception:
        return {}


def _make_tag_diagnostics(
    observations: list[Dict[str, Any]],
    estimate: Dict[str, Any],
    tag_map: Dict[str, Any],
    report: Dict[str, Any],
    gt_x_m: float,
    gt_y_m: float,
    gt_heading_deg: float,
) -> list[Dict[str, Any]]:
    """
    Build one diagnostic record per single-tag candidate.

    New v3 additions:
      - raw/library vs calibrated distance/angle/yaw from the actual robot report
      - image geometry, if available
      - expected distance/angle/yaw from manually supplied ground truth
      - observation residual = measured calibrated value - expected true value

    The observation residual is the important "reality check" before map fusion.
    """
    report_details = _extract_report_tag_details(report)

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
        tag_world = _lookup_tag_world(tag_map, c.get("tag_id"))
        used_diag = used_diag_by_key.get(key, {})
        report_detail = report_details.get(key, {})

        lp = report_detail.get("library_pose") or {}
        cp = report_detail.get("calibrated_pose") or {}
        ig = report_detail.get("image_geometry") or {}

        cx = float(c.get("x_m"))
        cy = float(c.get("y_m"))
        ch = float(c.get("heading_deg"))

        if est_ok:
            pos_res = math.hypot(cx - float(est_x), cy - float(est_y))
            head_res = abs(utility._wrap_angle_deg(ch - float(est_h)))
        else:
            pos_res = None
            head_res = None

        expected = _expected_observation_from_ground_truth(
            obs=obs or c,
            tag_world=tag_world,
            gt_x_m=gt_x_m,
            gt_y_m=gt_y_m,
            gt_heading_deg=gt_heading_deg,
        )

        cal_dist = _safe_float(obs.get("distance_m", c.get("distance_m")))
        cal_angle = _safe_float(obs.get("angle_deg_cw", c.get("angle_deg_cw")))
        cal_yaw = _safe_float(obs.get("yaw_deg", c.get("yaw_deg")))

        raw_dist = _safe_float(lp.get("distance_m"))
        raw_angle = _safe_float(lp.get("angle_deg"))
        raw_yaw = _safe_float(lp.get("yaw_deg"))

        exp_dist = _safe_float(expected.get("distance_m"))
        exp_angle = _safe_float(expected.get("angle_deg_cw"))
        exp_yaw = _safe_float(expected.get("yaw_deg"))

        dist_err = (cal_dist - exp_dist) if cal_dist is not None and exp_dist is not None else None
        raw_dist_err = (raw_dist - exp_dist) if raw_dist is not None and exp_dist is not None else None
        angle_err = utility._wrap_angle_deg(cal_angle - exp_angle) if cal_angle is not None and exp_angle is not None else None
        yaw_err = utility._wrap_angle_deg(cal_yaw - exp_yaw) if cal_yaw is not None and exp_yaw is not None else None

        correction_ratio = (cal_dist / raw_dist) if cal_dist is not None and raw_dist not in (None, 0.0) else None

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
                "distance_m": cal_dist,
                "angle_deg_cw": cal_angle,
                "yaw_deg": cal_yaw,
                "bearing_robot_deg_ccw": obs.get("bearing_robot_deg_ccw", c.get("bearing_robot_deg_ccw")),
                "camera_forward_offset_m": obs.get("camera_forward_offset_m", c.get("camera_forward_offset_m")),
            },

            "raw_observed": {
                "distance_m": raw_dist,
                "angle_deg": raw_angle,
                "yaw_deg": raw_yaw,
            },

            "calibrated_pose_from_report": cp,
            "expected_from_ground_truth": expected,
            "observation_error_to_ground_truth": {
                "distance_m": dist_err,
                "raw_distance_m": raw_dist_err,
                "angle_deg_cw": angle_err,
                "yaw_deg": yaw_err,
                "distance_correction_ratio": correction_ratio,
            },

            "image_geometry": {
                "center_x": ig.get("center_x"),
                "center_y": ig.get("center_y"),
                "avg_width_px": ig.get("avg_width_px"),
                "avg_height_px": ig.get("avg_height_px"),
                "width_height_ratio": ig.get("width_height_ratio"),
                "perspective_skew_lr": ig.get("perspective_skew_lr"),
                "perspective_skew_tb": ig.get("perspective_skew_tb"),
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
            "snapshot_path": c.get("snapshot_path") or report_detail.get("snapshot_path") or obs.get("snapshot_path") or "",
        }
        out.append(item)

    out.sort(
        key=lambda d: (
            abs(float(d["observation_error_to_ground_truth"].get("distance_m") or 0.0)),
            float(d["residual_to_final"].get("position_m") or 0.0),
        ),
        reverse=True,
    )
    return out

def _print_visible_tag_table(observations: list[Dict[str, Any]], tag_map: Dict[str, Any]) -> None:
    print("\nVisible Tag Observations  (production-calibrated values)")
    print("=" * 118)
    print(
        f"{'Tag':>4} {'Cam':>5} "
        f"{'TagX':>7} {'TagY':>7} {'TagYaw':>7} {'Face':>6} "
        f"{'CalDist':>7} {'CalAng':>7} {'CalYaw':>7} {'BearCCW':>8} {'CamOff':>7}"
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


def _print_observation_truth_table(tag_diagnostics: list[Dict[str, Any]]) -> None:
    """
    Most important on-site table.
    It compares what the camera reports against what the manually supplied
    ground-truth robot pose and tag map predict.  This exposes bad tag observations
    before fusion, map solving, or final pose estimation can hide the issue.
    """
    print("\nObservation Reality Check vs Manual Ground Truth  (sorted by |CalDist-TrueDist|)")
    print("=" * 132)
    print(
        f"{'Tag':>4} {'Cam':>5} "
        f"{'TrueD':>7} {'RawD':>7} {'CalD':>7} {'Cal-True':>9} {'Raw-True':>9} {'Ratio':>6} "
        f"{'TrueAng':>8} {'CalAng':>7} {'AngErr':>7} "
        f"{'TrueYaw':>8} {'CalYaw':>7} {'YawErr':>7}"
    )
    print("-" * 132)

    if not tag_diagnostics:
        print("(no tag diagnostics)")
        return

    rows = sorted(
        tag_diagnostics,
        key=lambda d: abs(float((d.get("observation_error_to_ground_truth") or {}).get("distance_m") or 0.0)),
        reverse=True,
    )

    for d in rows:
        obs = d.get("observed") or {}
        raw = d.get("raw_observed") or {}
        exp = d.get("expected_from_ground_truth") or {}
        err = d.get("observation_error_to_ground_truth") or {}
        print(
            f"{int(d.get('tag_id')):>4} {str(d.get('camera_role') or '')[:5]:>5} "
            f"{_fmt_float(exp.get('distance_m'), 3, 7)} "
            f"{_fmt_float(raw.get('distance_m'), 3, 7)} "
            f"{_fmt_float(obs.get('distance_m'), 3, 7)} "
            f"{_fmt_float(err.get('distance_m'), 3, 9)} "
            f"{_fmt_float(err.get('raw_distance_m'), 3, 9)} "
            f"{_fmt_float(err.get('distance_correction_ratio'), 3, 6)} "
            f"{_fmt_float(exp.get('angle_deg_cw'), 1, 8)} "
            f"{_fmt_float(obs.get('angle_deg_cw'), 1, 7)} "
            f"{_fmt_float(err.get('angle_deg_cw'), 1, 7)} "
            f"{_fmt_float(exp.get('yaw_deg'), 1, 8)} "
            f"{_fmt_float(obs.get('yaw_deg'), 1, 7)} "
            f"{_fmt_float(err.get('yaw_deg'), 1, 7)}"
        )
    print("=" * 132)


def _print_image_geometry_table(tag_diagnostics: list[Dict[str, Any]]) -> None:
    print("\nImage Geometry Clues  (look for edge/FOV/skew/small-tag failures)")
    print("=" * 100)
    print(
        f"{'Tag':>4} {'Cam':>5} {'Cx':>8} {'Cy':>8} {'Wpx':>7} {'Hpx':>7} "
        f"{'WH':>7} {'SkewLR':>8} {'SkewTB':>8} {'Snapshot':<20}"
    )
    print("-" * 100)

    if not tag_diagnostics:
        print("(no image geometry)")
        return

    for d in sorted(tag_diagnostics, key=lambda x: (str(x.get("camera_role")), int(x.get("tag_id", -1)))):
        ig = d.get("image_geometry") or {}
        print(
            f"{int(d.get('tag_id')):>4} {str(d.get('camera_role') or '')[:5]:>5} "
            f"{_fmt_float(ig.get('center_x'), 1, 8)} "
            f"{_fmt_float(ig.get('center_y'), 1, 8)} "
            f"{_fmt_float(ig.get('avg_width_px'), 1, 7)} "
            f"{_fmt_float(ig.get('avg_height_px'), 1, 7)} "
            f"{_fmt_float(ig.get('width_height_ratio'), 3, 7)} "
            f"{_fmt_float(ig.get('perspective_skew_lr'), 3, 8)} "
            f"{_fmt_float(ig.get('perspective_skew_tb'), 3, 8)} "
            f"{str(d.get('snapshot_path') or '')[-20:]:<20}"
        )
    print("=" * 100)


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


def run_once(gt_x_m: float, gt_y_m: float, gt_heading_deg: float) -> Dict[str, Any]:
    if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, ROBOT_ID):
        raise RuntimeError(f"{ROBOT_ID} is not whitelisted")

    assets = _ensure_mobility_assets_ready()

    old_ts = _read_report_ts(ROBOT_ID)
    direct_cmd_id = _send_direct_location_request(ROBOT_ID)

    report = _wait_for_new_report(ROBOT_ID, old_ts)

    observations = _s3_extract_visible_tags(ROBOT_ID)
    front_count, rear_count = _count_by_camera(observations)

    estimate = _s3_solve_true_location(ROBOT_ID)
    tag_map = _load_tag_map()
    tag_diagnostics = _make_tag_diagnostics(
        observations=observations,
        estimate=estimate,
        tag_map=tag_map,
        report=report,
        gt_x_m=gt_x_m,
        gt_y_m=gt_y_m,
        gt_heading_deg=gt_heading_deg,
    )

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

    payload = {
        "script_version": "phase2a_v3_observation_reality_check",
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
        "estimate": estimate,
        "tag_diagnostics": tag_diagnostics,

        "metadata": {
            "report_time": _read_report_ts(ROBOT_ID),
            "direct_command_stream_id": direct_cmd_id,
            "trial": TRIAL,
            "notes": NOTES,
        },
    }

    with log_file.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

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
    print(f"JSON Log       : {log_file}")

    _print_visible_tag_table(observations, tag_map)
    _print_observation_truth_table(tag_diagnostics)
    _print_image_geometry_table(tag_diagnostics)
    _print_single_tag_solution_table(tag_diagnostics)
    _print_final_residual_table(tag_diagnostics)

    print()

    return payload


if __name__ == "__main__":
    args = _parse_args()
    run_once(
        gt_x_m=float(args.x),
        gt_y_m=float(args.y),
        gt_heading_deg=float(args.h),
    )
