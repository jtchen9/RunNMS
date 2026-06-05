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

from m8mobility_map import _ensure_mobility_assets_ready
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
        "script_version": "phase2a_v1",
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
    print(f"Tags Used      : {estimate.get('tags_used', [])}")

    print()

    return payload


if __name__ == "__main__":
    args = _parse_args()
    run_once(
        gt_x_m=float(args.x),
        gt_y_m=float(args.y),
        gt_heading_deg=float(args.h),
    )
