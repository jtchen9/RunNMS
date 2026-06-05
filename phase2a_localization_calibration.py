"""
Phase 2A helper: robot AprilTag localization calibration.

Purpose:
- Ask one robot to execute mobility.report.location.
- Wait until the robot posts a fresh mobility_report to NMS.
- Run the NMS S3 tag solver on that report.
- Compare estimated pose against manually entered ground truth.
- Append one CSV row.

Usage style:
- Edit the variables below.
- Run directly in VS Code.
- No argparse.

Important:
- This helper assumes m8mobility_state.py has been updated to the new
  dual-camera calibrated_pose parser.
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Dict, Optional

import config
import utility
import m4Commands
import argparse

from m8mobility_state import _s3_solve_true_location, _s3_extract_visible_tags
from m8mobility_state_store import key_report, key_time
from m8mobility_map import _ensure_mobility_assets_ready

ROBOT_ID = "twin-scout-delta"

TRIAL = "P001"


def _parse_args():
    p = argparse.ArgumentParser(
        description="Phase2A localization calibration"
    )

    p.add_argument(
        "--x",
        type=float,
        required=True,
        help="Ground-truth X coordinate in meters",
    )

    p.add_argument(
        "--y",
        type=float,
        required=True,
        help="Ground-truth Y coordinate in meters",
    )

    p.add_argument(
        "--h",
        type=float,
        required=True,
        help="Ground-truth heading in degrees",
    )

    return p.parse_args()

_ARGS = _parse_args()

GT_X_M = float(_ARGS.x)
GT_Y_M = float(_ARGS.y)
GT_HEADING_DEG = float(_ARGS.h)

OUT_CSV = Path(r"D:\Data\_Action\_RunNMS\Phase2A_Localization.csv")

WAIT_TIMEOUT_SEC = 45
POLL_EVERY_SEC = 1.0


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _angle_error_deg(est: Optional[float], gt: Optional[float]) -> Optional[float]:
    if est is None or gt is None:
        return None
    return abs(utility._wrap_angle_deg(float(est) - float(gt)))


def _pos_error_m(est_x: Optional[float], est_y: Optional[float], gt_x: float, gt_y: float) -> Optional[float]:
    if est_x is None or est_y is None:
        return None
    return math.hypot(float(est_x) - float(gt_x), float(est_y) - float(gt_y))


def _read_report_ts(scanner: str) -> str:
    return utility._hget(key_time(scanner), "last_mobility_report_at", "")


def _read_report(scanner: str) -> Dict[str, Any]:
    return utility._hget_json(key_report(scanner), "last_mobility_report_json")


def _enqueue_location_report(scanner: str) -> None:
    cmd = m4Commands.Cmd(
        category="mobility",
        action="mobility.report.location",
        execute_at=utility.local_ts(),
        args={},
    )
    result = m4Commands._cmd_enqueue_core(scanner, cmd)
    print("Enqueued command:")
    print(json.dumps(result, ensure_ascii=False, indent=2))


def _wait_for_new_report(scanner: str, old_ts: str) -> Dict[str, Any]:
    import time

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
        f"old_ts={old_ts!r}"
    )


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
        "notes",
    ]

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    exists = OUT_CSV.exists()

    with OUT_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerow(row)


def _count_by_camera(obs: list[Dict[str, Any]]) -> tuple[int, int]:
    front = sum(1 for x in obs if x.get("camera_role") == "front")
    rear = sum(1 for x in obs if x.get("camera_role") == "rear")
    return front, rear

def run_once() -> Dict[str, Any]:
    _assert(config.r.hexists(config.KEY_WHITELIST_SCANNER_META, ROBOT_ID), f"{ROBOT_ID} not whitelisted")

    assets = _ensure_mobility_assets_ready()
    print("\nMobility assets loaded:")
    print(json.dumps(assets, ensure_ascii=False, indent=2))

    old_ts = _read_report_ts(ROBOT_ID)

    _enqueue_location_report(ROBOT_ID)
    report = _wait_for_new_report(ROBOT_ID, old_ts)

    print("\nFresh mobility report received:")
    print(json.dumps({
        "last_command": report.get("last_command"),
        "last_exec_status": report.get("last_exec_status"),
        "last_error_code": report.get("last_error_code"),
    }, ensure_ascii=False, indent=2))

    observations = _s3_extract_visible_tags(ROBOT_ID)
    front_count, rear_count = _count_by_camera(observations)

    estimate = _s3_solve_true_location(ROBOT_ID)

    est_ok = bool(estimate.get("location_ok"))
    est_x = float(estimate["x_m"]) if est_ok else None
    est_y = float(estimate["y_m"]) if est_ok else None
    est_h = float(estimate["heading_deg"]) if est_ok else None

    pos_err = _pos_error_m(est_x, est_y, GT_X_M, GT_Y_M)
    heading_err = _angle_error_deg(est_h, GT_HEADING_DEG)

    row = {
        "trial": TRIAL,
        "robot_id": ROBOT_ID,
        "gt_x_m": GT_X_M,
        "gt_y_m": GT_Y_M,
        "gt_heading_deg": GT_HEADING_DEG,
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
        "notes": "",
    }

    _write_csv_row(row)

    print("\nEstimate:")
    print(json.dumps(estimate, ensure_ascii=False, indent=2))

    print("\nCSV row appended:")
    print(json.dumps(row, ensure_ascii=False, indent=2))

    return row


if __name__ == "__main__":
    run_once()
