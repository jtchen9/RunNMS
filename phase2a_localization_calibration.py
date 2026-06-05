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
import csv
import json
import math
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

import config
import utility

from m8mobility_map import _ensure_mobility_assets_ready
from m8mobility_state import _s3_extract_visible_tags, _s3_solve_true_location
from m8mobility_state_store import key_report, key_time


ROBOT_ID = "twin-scout-delta"
TRIAL = "P2A-001"
NOTES = ""

OUT_CSV = Path(r"D:\Data\_Action\_RunNMS\Phase2A_Localization.csv")

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


def _write_csv_row(row: Dict[str, Any]) -> None:
    fields = [
        "trial",
        "robot_id",
        "gt_x_m",
        "gt_y_m",
        "gt_heading_deg",
        "estimate_ok",
        "estimated_x_m",
        "estimated_y_m",
        "estimated_heading_deg",
        "pos_error_m",
        "heading_error_deg",
        "tag_count",
        "front_count",
        "rear_count",
        "tags_used_json",
        "observations_json",
        "detail",
        "report_time",
        "direct_command_stream_id",
        "notes",
    ]

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    exists = OUT_CSV.exists()

    with OUT_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerow(row)


def run_once(gt_x_m: float, gt_y_m: float, gt_heading_deg: float) -> Dict[str, Any]:
    if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, ROBOT_ID):
        raise RuntimeError(f"{ROBOT_ID} is not whitelisted")

    assets = _ensure_mobility_assets_ready()
    print("\nMobility assets loaded:")
    print(json.dumps(assets, ensure_ascii=False, indent=2))

    old_ts = _read_report_ts(ROBOT_ID)
    direct_cmd_id = _send_direct_location_request(ROBOT_ID)

    report = _wait_for_new_report(ROBOT_ID, old_ts)

    print("\nFresh mobility report received:")
    print(json.dumps({
        "last_command": report.get("last_command"),
        "last_exec_status": report.get("last_exec_status"),
        "last_error_code": report.get("last_error_code"),
        "last_error_detail": report.get("last_error_detail"),
    }, ensure_ascii=False, indent=2))

    observations = _s3_extract_visible_tags(ROBOT_ID)
    front_count, rear_count = _count_by_camera(observations)

    print("\nParsed observations:")
    print(json.dumps({
        "tag_count": len(observations),
        "front_count": front_count,
        "rear_count": rear_count,
        "tag_ids": [x.get("id") for x in observations],
    }, ensure_ascii=False, indent=2))

    estimate = _s3_solve_true_location(ROBOT_ID)

    est_ok = bool(estimate.get("location_ok"))
    est_x = float(estimate["x_m"]) if est_ok else None
    est_y = float(estimate["y_m"]) if est_ok else None
    est_h = float(estimate["heading_deg"]) if est_ok else None

    pos_err = _pos_error_m(est_x, est_y, gt_x_m, gt_y_m)
    heading_err = _angle_error_deg(est_h, gt_heading_deg)

    row = {
        "trial": TRIAL,
        "robot_id": ROBOT_ID,
        "gt_x_m": gt_x_m,
        "gt_y_m": gt_y_m,
        "gt_heading_deg": gt_heading_deg,
        "estimate_ok": est_ok,
        "estimated_x_m": est_x,
        "estimated_y_m": est_y,
        "estimated_heading_deg": est_h,
        "pos_error_m": pos_err,
        "heading_error_deg": heading_err,
        "tag_count": len(observations),
        "front_count": front_count,
        "rear_count": rear_count,
        "tags_used_json": json.dumps(estimate.get("tags_used", []), ensure_ascii=False),
        "observations_json": json.dumps(observations, ensure_ascii=False),
        "detail": estimate.get("detail", ""),
        "report_time": _read_report_ts(ROBOT_ID),
        "direct_command_stream_id": direct_cmd_id,
        "notes": NOTES,
    }

    _write_csv_row(row)

    print("\nEstimate:")
    print(json.dumps(estimate, ensure_ascii=False, indent=2))

    print("\nCSV row appended:")
    print(json.dumps(row, ensure_ascii=False, indent=2))

    return row


if __name__ == "__main__":
    args = _parse_args()
    run_once(
        gt_x_m=float(args.x),
        gt_y_m=float(args.y),
        gt_heading_deg=float(args.h),
    )
