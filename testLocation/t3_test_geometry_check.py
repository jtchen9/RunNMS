"""
t3_test_geometry_check.py

Test script for Part 1 and Part 2 of the 8-part location solver.

This file belongs under:
    D:\Data\_Action\_RunNMS\testLocation

It tests reusable functions from:
    D:\Data\_Action\_RunNMS\locationSolver

No argparse is used. Adjustable parameters and test paths are under:
    if __name__ == "__main__":

Expected input:
    D:\Data\_Action\_RunNMS\testLocation\input\step1_observations.csv
    D:\Data\_Action\_RunNMS\sitemap\DemoRoom\tag_location.txt

Output:
    D:\Data\_Action\_RunNMS\testLocation\output\geometry_check
"""

from __future__ import annotations

from pathlib import Path
import csv
import json
import math
import sys

import pandas as pd


# Allow direct run from VS Code:
#     python t3_test_geometry_check.py
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from locationSolver.location_config import CAMERA_CONFIG
from locationSolver.location_tagmap import load_tag_map
from locationSolver.location_geometry import (
    RobotPose,
    predict_tag_measurement,
    compute_measurement_residual,
    wrap_angle_deg,
)


def _first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _float_value(row, col: str | None) -> float:
    if col is None:
        return float("nan")
    try:
        return float(row[col])
    except Exception:
        return float("nan")


def _str_value(row, col: str | None) -> str:
    if col is None:
        return ""
    try:
        return str(row[col])
    except Exception:
        return ""


def build_geometry_check_rows(
    df: pd.DataFrame,
    tag_map,
    camera_config,
) -> list[dict]:
    """
    Recompute predicted distance/angle/yaw for every row using gt pose.

    This function is test logic, not production solver logic.
    """
    col_dataset = _first_existing_col(df, ["dataset", "source_dataset"])
    col_gt_x = _first_existing_col(df, ["gt_x", "true_robot_x", "robot_x"])
    col_gt_y = _first_existing_col(df, ["gt_y", "true_robot_y", "robot_y"])
    col_gt_h = _first_existing_col(df, ["gt_h", "gt_heading", "true_robot_h", "robot_heading"])
    col_tag = _first_existing_col(df, ["tag_id", "tag", "tag_no"])
    col_cam = _first_existing_col(df, ["camera_role", "camera", "cam_role"])

    col_meas_d = _first_existing_col(df, ["meas_distance_m", "distance_m", "measured_distance_m"])
    col_meas_a = _first_existing_col(df, ["meas_angle_deg", "angle_deg", "measured_angle_deg", "angle_deg_cw"])
    col_meas_yaw = _first_existing_col(df, ["meas_yaw_deg", "yaw_deg", "measured_yaw_deg"])

    col_true_d = _first_existing_col(df, ["true_distance_m", "expected_distance_m"])
    col_true_a = _first_existing_col(df, ["true_angle_deg", "expected_angle_deg"])
    col_true_yaw = _first_existing_col(df, ["true_yaw_deg", "expected_yaw_deg"])

    required = {
        "gt_x": col_gt_x,
        "gt_y": col_gt_y,
        "gt_h": col_gt_h,
        "tag_id": col_tag,
        "camera_role": col_cam,
    }
    missing = [name for name, col in required.items() if col is None]
    if missing:
        raise ValueError(f"Missing required columns {missing}. CSV columns={list(df.columns)}")

    rows: list[dict] = []

    for row_index, r in df.iterrows():
        dataset = _str_value(r, col_dataset)
        gt_x = _float_value(r, col_gt_x)
        gt_y = _float_value(r, col_gt_y)
        gt_h = _float_value(r, col_gt_h)

        try:
            tag_id = int(r[col_tag])
        except Exception:
            tag_id = -1

        camera_role = _str_value(r, col_cam).strip().lower()

        out = {
            "source_row": int(row_index),
            "dataset": dataset,
            "gt_x": gt_x,
            "gt_y": gt_y,
            "gt_h": gt_h,
            "tag_id": tag_id,
            "camera_role": camera_role,
        }

        if tag_id not in tag_map:
            out.update({"ok": False, "reason": "tag_missing_from_map"})
            rows.append(out)
            continue

        if camera_role not in camera_config:
            out.update({"ok": False, "reason": "unknown_camera_role"})
            rows.append(out)
            continue

        if not all(math.isfinite(v) for v in [gt_x, gt_y, gt_h]):
            out.update({"ok": False, "reason": "bad_ground_truth_pose"})
            rows.append(out)
            continue

        tag_world = tag_map[tag_id]
        robot_pose = RobotPose(x_m=gt_x, y_m=gt_y, heading_deg=gt_h)

        pred = predict_tag_measurement(
            robot_pose=robot_pose,
            tag_world=tag_world,
            camera_cfg=camera_config[camera_role],
        )

        meas_d = _float_value(r, col_meas_d)
        meas_a = _float_value(r, col_meas_a)
        meas_yaw = _float_value(r, col_meas_yaw)

        true_d = _float_value(r, col_true_d)
        true_a = _float_value(r, col_true_a)
        true_yaw = _float_value(r, col_true_yaw)

        if all(math.isfinite(v) for v in [meas_d, meas_a, meas_yaw]):
            meas_res = compute_measurement_residual(
                measured_distance_m=meas_d,
                measured_angle_deg=meas_a,
                measured_yaw_deg=meas_yaw,
                predicted=pred,
            )
            meas_distance_residual_m = meas_res.distance_residual_m
            meas_angle_residual_deg = meas_res.angle_residual_deg
            meas_yaw_residual_deg = meas_res.yaw_residual_deg
        else:
            meas_distance_residual_m = float("nan")
            meas_angle_residual_deg = float("nan")
            meas_yaw_residual_deg = float("nan")

        true_distance_delta_m = true_d - pred.distance_m if math.isfinite(true_d) else float("nan")
        true_angle_delta_deg = wrap_angle_deg(true_a - pred.angle_deg) if math.isfinite(true_a) else float("nan")
        true_yaw_delta_deg = wrap_angle_deg(true_yaw - pred.yaw_deg) if math.isfinite(true_yaw) else float("nan")

        out.update({
            "ok": True,
            "reason": "",

            "tag_x_m": tag_world.x_m,
            "tag_y_m": tag_world.y_m,
            "tag_yaw_deg": tag_world.yaw_deg,
            "tag_room": tag_world.room,
            "tag_facing": tag_world.facing,

            "camera_x_m": pred.camera_pose.x_m,
            "camera_y_m": pred.camera_pose.y_m,
            "camera_heading_deg": pred.camera_pose.heading_deg,
            "bearing_world_deg": pred.bearing_world_deg,

            "pred_distance_m": pred.distance_m,
            "pred_angle_deg": pred.angle_deg,
            "pred_yaw_deg": pred.yaw_deg,

            "meas_distance_m": meas_d,
            "meas_angle_deg": meas_a,
            "meas_yaw_deg": meas_yaw,
            "meas_distance_residual_m": meas_distance_residual_m,
            "meas_distance_residual_cm": meas_distance_residual_m * 100.0,
            "meas_angle_residual_deg": meas_angle_residual_deg,
            "meas_yaw_residual_deg": meas_yaw_residual_deg,

            "true_distance_m": true_d,
            "true_angle_deg": true_a,
            "true_yaw_deg": true_yaw,
            "true_distance_delta_m": true_distance_delta_m,
            "true_distance_delta_cm": true_distance_delta_m * 100.0,
            "true_angle_delta_deg": true_angle_delta_deg,
            "true_yaw_delta_deg": true_yaw_delta_deg,
        })

        rows.append(out)

    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        return

    keys: list[str] = []
    for row in rows:
        for k in row.keys():
            if k not in keys:
                keys.append(k)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(rows: list[dict], output_dir: Path) -> None:
    df = pd.DataFrame(rows)
    ok_df = df[df["ok"] == True].copy()

    lines: list[str] = []
    lines.append("GEOMETRY CHECK SUMMARY")
    lines.append("=" * 80)
    lines.append(f"rows: {len(df)}")
    lines.append(f"ok rows: {len(ok_df)}")
    lines.append(f"bad rows: {len(df) - len(ok_df)}")
    lines.append("")

    def add_stat(label: str, col: str, unit: str) -> None:
        if col not in ok_df.columns:
            return
        vals = pd.to_numeric(ok_df[col], errors="coerce").dropna()
        if len(vals) == 0:
            return
        lines.append(
            f"{label}: mean={vals.mean():.3f}{unit}, "
            f"median={vals.median():.3f}{unit}, "
            f"p90_abs={vals.abs().quantile(0.90):.3f}{unit}, "
            f"max_abs={vals.abs().max():.3f}{unit}"
        )

    lines.append("Measured - predicted residuals:")
    add_stat("distance", "meas_distance_residual_cm", " cm")
    add_stat("angle", "meas_angle_residual_deg", " deg")
    add_stat("yaw", "meas_yaw_residual_deg", " deg")

    lines.append("")
    lines.append("Existing true_* - newly predicted deltas:")
    lines.append("These may not be near zero if step1_observations.csv was built with old tag coordinates or old camera offsets.")
    add_stat("true distance delta", "true_distance_delta_cm", " cm")
    add_stat("true angle delta", "true_angle_delta_deg", " deg")
    add_stat("true yaw delta", "true_yaw_delta_deg", " deg")

    lines.append("")
    lines.append("Per-camera measured residual summary:")
    if len(ok_df):
        for camera_role, g in ok_df.groupby("camera_role"):
            lines.append(f"[{camera_role}] n={len(g)}")
            for col, unit in [
                ("meas_distance_residual_cm", " cm"),
                ("meas_angle_residual_deg", " deg"),
                ("meas_yaw_residual_deg", " deg"),
            ]:
                vals = pd.to_numeric(g[col], errors="coerce").dropna()
                if len(vals):
                    lines.append(
                        f"  {col}: mean={vals.mean():.3f}{unit}, "
                        f"median={vals.median():.3f}{unit}, "
                        f"p90_abs={vals.abs().quantile(0.90):.3f}{unit}"
                    )

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "geometry_check_summary.txt").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


def write_worst_rows(rows: list[dict], output_dir: Path, worst_n: int) -> None:
    df = pd.DataFrame(rows)
    ok_df = df[df["ok"] == True].copy()
    if len(ok_df) == 0:
        return

    ok_df["worst_score"] = (
        pd.to_numeric(ok_df["meas_distance_residual_cm"], errors="coerce").abs().fillna(0.0) / 5.0
        + pd.to_numeric(ok_df["meas_angle_residual_deg"], errors="coerce").abs().fillna(0.0) / 3.0
        + pd.to_numeric(ok_df["meas_yaw_residual_deg"], errors="coerce").abs().fillna(0.0) / 10.0
    )

    worst_df = ok_df.sort_values("worst_score", ascending=False).head(worst_n)
    worst_df.to_csv(output_dir / "geometry_check_worst_rows.csv", index=False)


if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Adjustable test parameters
    # -------------------------------------------------------------------------
    INPUT_CSV = ROOT_DIR / "testLocation" / "input" / "step1_observations.csv"
    TAG_FILE = ROOT_DIR / "sitemap" / "DemoRoom" / "tag_location.txt"
    OUTPUT_DIR = ROOT_DIR / "testLocation" / "output" / "geometry_check"

    WORST_N = 50

    # Usually use the shared config directly.
    # Override here only for temporary experiments.
    camera_config = CAMERA_CONFIG

    # -------------------------------------------------------------------------
    # Run test
    # -------------------------------------------------------------------------
    print("Running t3 geometry check")
    print(f"ROOT_DIR   = {ROOT_DIR}")
    print(f"INPUT_CSV  = {INPUT_CSV}")
    print(f"TAG_FILE   = {TAG_FILE}")
    print(f"OUTPUT_DIR = {OUTPUT_DIR}")

    tag_map = load_tag_map(TAG_FILE)
    df = pd.read_csv(INPUT_CSV)

    rows = build_geometry_check_rows(
        df=df,
        tag_map=tag_map,
        camera_config=camera_config,
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    write_csv(rows, OUTPUT_DIR / "geometry_check_rows.csv")
    write_summary(rows, OUTPUT_DIR)
    write_worst_rows(rows, OUTPUT_DIR, WORST_N)

    config_dump = {
        "input_csv": str(INPUT_CSV),
        "tag_file": str(TAG_FILE),
        "output_dir": str(OUTPUT_DIR),
        "camera_config": camera_config,
    }
    (OUTPUT_DIR / "geometry_check_config.json").write_text(
        json.dumps(config_dump, indent=2),
        encoding="utf-8",
    )

    print("")
    print("Wrote:")
    print(f"  {OUTPUT_DIR / 'geometry_check_rows.csv'}")
    print(f"  {OUTPUT_DIR / 'geometry_check_summary.txt'}")
    print(f"  {OUTPUT_DIR / 'geometry_check_worst_rows.csv'}")
    print(f"  {OUTPUT_DIR / 'geometry_check_config.json'}")
