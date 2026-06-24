#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import time
import uuid
from typing import Any, Dict, List

import config
import utility

from m8mobility_map import _ensure_mobility_assets_ready, _load_tag_map
from m8mobility_state import _s3_extract_visible_tags, _s3_solve_true_location
from m8mobility_state_store import key_report, key_time


ROBOT_ID = "twin-scout-delta"
WAIT_TIMEOUT_SEC = 60
POLL_EVERY_SEC = 1.0
CAMERA_OFFSET_M = 0.055


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--x", type=float, required=True)
    p.add_argument("--y", type=float, required=True)
    p.add_argument("--h", type=float, required=True)
    p.add_argument("--robot", default=ROBOT_ID)
    p.add_argument("--top", type=int, default=8)
    return p.parse_args()


def _read_report_ts(scanner: str) -> str:
    return utility._hget(key_time(scanner), "last_mobility_report_at", "")


def _read_report(scanner: str) -> Dict[str, Any]:
    return utility._hget_json(key_report(scanner), "last_mobility_report_json")


def _send_location_request(scanner: str) -> str:
    now = utility.local_ts()
    web_cmd_id = f"t7lite-{uuid.uuid4().hex[:10]}"

    fields = {
        "category": "mobility",
        "action": "mobility.report.location",
        "execute_at": now,
        "created_at": now,
        "args_json": "{}",
        "source": "t7_lite",
        "web_cmd_id": web_cmd_id,
    }

    sid = config.r.xadd(
        config.key_cmd_stream(scanner),
        fields,
        maxlen=5000,
        approximate=True,
    )
    return str(sid)


def _wait_for_new_report(scanner: str, old_ts: str) -> Dict[str, Any]:
    deadline = time.time() + WAIT_TIMEOUT_SEC
    while time.time() < deadline:
        ts = _read_report_ts(scanner)
        if ts and ts != old_ts:
            rpt = _read_report(scanner)
            if rpt:
                return rpt
        time.sleep(POLL_EVERY_SEC)

    raise TimeoutError(f"no fresh mobility report from {scanner}")


def _tag_map_tags(tag_map: Dict[str, Any]) -> Dict[str, Any]:
    tags = tag_map.get("tags") or {}
    return tags if isinstance(tags, dict) else {}


def _camera_pose_from_robot(
    robot_x: float,
    robot_y: float,
    robot_h: float,
    camera_role: str,
):
    h_rad = math.radians(robot_h)

    if camera_role == "front":
        cam_h = utility._deg_norm_360(robot_h)
        off = CAMERA_OFFSET_M
    elif camera_role == "rear":
        cam_h = utility._deg_norm_360(robot_h + 180.0)
        off = -CAMERA_OFFSET_M
    else:
        cam_h = utility._deg_norm_360(robot_h)
        off = 0.0

    cam_x = robot_x + off * math.cos(h_rad)
    cam_y = robot_y + off * math.sin(h_rad)

    return cam_x, cam_y, cam_h


def _expected_obs_from_truth(
    gt_x: float,
    gt_y: float,
    gt_h: float,
    camera_role: str,
    tag_world: Dict[str, Any],
):
    tag_x = float(tag_world["x_m"])
    tag_y = float(tag_world["y_m"])
    tag_yaw = float(tag_world["yaw_deg"])

    cam_x, cam_y, cam_h = _camera_pose_from_robot(
        gt_x, gt_y, gt_h, camera_role
    )

    dx = tag_x - cam_x
    dy = tag_y - cam_y

    true_d = math.hypot(dx, dy)
    bearing_world = utility._deg_norm_360(math.degrees(math.atan2(dy, dx)))

    # positive = tag appears on camera right side
    true_angle = utility._wrap_angle_deg(cam_h - bearing_world)

    # same yaw convention used by production solver
    true_yaw = utility._wrap_angle_deg(
        tag_yaw + 180.0 - cam_h + true_angle
    )

    return {
        "true_distance_m": true_d,
        "true_angle_deg": true_angle,
        "true_yaw_deg": true_yaw,
        "camera_x_m": cam_x,
        "camera_y_m": cam_y,
        "camera_heading_deg": cam_h,
    }


def _score_row(d_err_cm: float, a_err: float, y_err: float, obs: Dict[str, Any]):
    # Debug score only; not production solver weight.
    score = abs(d_err_cm) / 5.0 + abs(a_err) / 3.0 + abs(y_err) / 5.0

    flags = []
    if abs(d_err_cm) > 15:
        flags.append("DIST")
    if abs(a_err) > 8:
        flags.append("ANGLE")
    if abs(y_err) > 12:
        flags.append("YAW")
    if abs(float(obs.get("yaw_deg") or 0.0)) < 10:
        flags.append("LOW_YAW")
    if abs(float(obs.get("angle_deg_cw") or 0.0)) > 30:
        flags.append("EDGE")
    if float(obs.get("distance_m") or 0.0) > 3.0:
        flags.append("FAR")

    return score, "|".join(flags) if flags else "OK"


def _build_observation_rows(
    observations: List[Dict[str, Any]],
    tag_map: Dict[str, Any],
    gt_x: float,
    gt_y: float,
    gt_h: float,
):
    tags = _tag_map_tags(tag_map)
    rows = []

    for obs in observations:
        try:
            tag_id = int(obs["id"])
            cam = str(obs.get("camera_role") or "")
            tw = tags.get(str(tag_id))
            if not isinstance(tw, dict):
                continue

            truth = _expected_obs_from_truth(gt_x, gt_y, gt_h, cam, tw)

            meas_d = float(obs["distance_m"])
            meas_a = float(obs["angle_deg_cw"])
            meas_y = float(obs["yaw_deg"])

            d_err_cm = 100.0 * (meas_d - truth["true_distance_m"])
            a_err = utility._wrap_angle_deg(meas_a - truth["true_angle_deg"])
            y_err = utility._wrap_angle_deg(meas_y - truth["true_yaw_deg"])

            score, flag = _score_row(d_err_cm, a_err, y_err, obs)

            rows.append({
                "tag": tag_id,
                "cam": cam,
                "true_d": truth["true_distance_m"],
                "meas_d": meas_d,
                "d_err_cm": d_err_cm,
                "true_a": truth["true_angle_deg"],
                "meas_a": meas_a,
                "a_err": a_err,
                "true_y": truth["true_yaw_deg"],
                "meas_y": meas_y,
                "y_err": y_err,
                "score": score,
                "flag": flag,
            })
        except Exception:
            continue

    rows.sort(key=lambda r: float(r["score"]), reverse=True)
    return rows


def _pose_error(est: Dict[str, Any], gt_x: float, gt_y: float, gt_h: float):
    if not est.get("location_ok"):
        return None, None

    ex = float(est["x_m"])
    ey = float(est["y_m"])
    eh = float(est["heading_deg"])

    pos_cm = 100.0 * math.hypot(ex - gt_x, ey - gt_y)
    h_err = abs(utility._wrap_angle_deg(eh - gt_h))
    return pos_cm, h_err


def _count_by_camera(observations):
    f = sum(1 for o in observations if o.get("camera_role") == "front")
    r = sum(1 for o in observations if o.get("camera_role") == "rear")
    return f, r


def _print_summary(scanner, gt_x, gt_y, gt_h, estimate, observations, rows):
    pos_cm, h_err = _pose_error(estimate, gt_x, gt_y, gt_h)
    fcnt, rcnt = _count_by_camera(observations)

    print()
    print("T7-LITE LOCATION OBSERVATION")
    print("=" * 96)
    print(f"Robot      : {scanner}")
    print(f"GT         : x={gt_x:.3f} y={gt_y:.3f} h={gt_h:.1f}")

    if estimate.get("location_ok"):
        print(
            f"Estimate   : x={float(estimate['x_m']):.3f} "
            f"y={float(estimate['y_m']):.3f} "
            f"h={float(estimate['heading_deg']):.1f}"
        )
        print(f"Error      : pos={pos_cm:.1f} cm  heading={h_err:.2f} deg")
    else:
        print("Estimate   : FAILED")
        print("Error      : N/A")

    print(
        f"Tags       : total={len(observations)} "
        f"front={fcnt} rear={rcnt} used={estimate.get('tags_used', [])}"
    )
    print(f"Solver     : {estimate.get('solver_stage', '')}")
    print(f"Detail     : {estimate.get('detail', '')}")

    if rows:
        worst = rows[0]
        print(
            f"Worst tag  : tag={worst['tag']} cam={worst['cam']} "
            f"score={worst['score']:.2f} flag={worst['flag']}"
        )

    if pos_cm is not None:
        if pos_cm <= 10.0 and h_err <= 2.0:
            print("Verdict    : GOOD_POSE")
        elif pos_cm <= 25.0 and h_err <= 5.0:
            print("Verdict    : MARGINAL_POSE")
        else:
            print("Verdict    : BAD_POSE")
    print("=" * 96)


def _print_observation_table(rows, top_n: int):
    print()
    print(f"Worst Observation Errors  top={top_n}")
    print("=" * 132)
    print(
        f"{'Tag':>4} {'Cam':>5} "
        f"{'TrueD':>7} {'MeasD':>7} {'Derr':>7} "
        f"{'TrueA':>7} {'MeasA':>7} {'Aerr':>7} "
        f"{'TrueY':>7} {'MeasY':>7} {'Yerr':>7} "
        f"{'Score':>7} {'Flag':>18}"
    )
    print("-" * 132)

    if not rows:
        print("(no comparable observations)")
        print("=" * 132)
        return

    for r in rows[:top_n]:
        print(
            f"{r['tag']:>4} {r['cam'][:5]:>5} "
            f"{r['true_d']:7.3f} {r['meas_d']:7.3f} {r['d_err_cm']:7.1f} "
            f"{r['true_a']:7.1f} {r['meas_a']:7.1f} {r['a_err']:7.1f} "
            f"{r['true_y']:7.1f} {r['meas_y']:7.1f} {r['y_err']:7.1f} "
            f"{r['score']:7.2f} {r['flag'][:18]:>18}"
        )

    print("=" * 132)


def _print_csv_summary(gt_x, gt_y, gt_h, estimate, observations, rows):
    pos_cm, h_err = _pose_error(estimate, gt_x, gt_y, gt_h)
    fcnt, rcnt = _count_by_camera(observations)

    worst = rows[0] if rows else {}
    print()
    print("CSV_SUMMARY")
    print(
        "x,y,h,ok,pos_err_cm,heading_err_deg,total_tags,front_tags,rear_tags,"
        "worst_tag,worst_cam,worst_score,worst_flag"
    )
    print(
        f"{gt_x:.3f},{gt_y:.3f},{gt_h:.1f},"
        f"{bool(estimate.get('location_ok'))},"
        f"{'' if pos_cm is None else f'{pos_cm:.1f}'},"
        f"{'' if h_err is None else f'{h_err:.2f}'},"
        f"{len(observations)},{fcnt},{rcnt},"
        f"{worst.get('tag','')},{worst.get('cam','')},"
        f"{'' if not worst else f'{worst.get('score'):.2f}'},"
        f"{worst.get('flag','')}"
    )


def run_once(scanner: str, gt_x: float, gt_y: float, gt_h: float, top_n: int):
    if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, scanner):
        raise RuntimeError(f"{scanner} is not whitelisted")

    _ensure_mobility_assets_ready()
    tag_map = _load_tag_map()

    old_ts = _read_report_ts(scanner)
    _send_location_request(scanner)
    _wait_for_new_report(scanner, old_ts)

    observations = _s3_extract_visible_tags(scanner)
    estimate = _s3_solve_true_location(scanner)

    rows = _build_observation_rows(
        observations=observations,
        tag_map=tag_map,
        gt_x=gt_x,
        gt_y=gt_y,
        gt_h=gt_h,
    )

    _print_summary(scanner, gt_x, gt_y, gt_h, estimate, observations, rows)
    _print_observation_table(rows, top_n)
    _print_csv_summary(gt_x, gt_y, gt_h, estimate, observations, rows)


if __name__ == "__main__":
    args = _parse_args()
    run_once(
        scanner=args.robot,
        gt_x=float(args.x),
        gt_y=float(args.y),
        gt_h=float(args.h),
        top_n=int(args.top),
    )
