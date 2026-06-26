"""
t7_test_distance_angle_yaw_solver.py

Test script for Part 6:
Joint distance + view-angle + weak-yaw solver.

This file belongs under:
    D:\Data\_Action\_RunNMS\testLocation

Expected input:
    D:\Data\_Action\_RunNMS\testLocation\input\step1_observations.csv
    D:\Data\_Action\_RunNMS\sitemap\DemoRoom\tag_location.txt

Output:
    D:\Data\_Action\_RunNMS\testLocation\output\distance_angle_yaw_solver

No argparse is used. Adjustable parameters and paths are under:
    if __name__ == "__main__":

Important diagnostic note
-------------------------
Yaw is deliberately weak. This script also compares Part 6 against Part 5.
If adding yaw changes the pose drastically, yaw weight is too high or yaw data
is not trustworthy.
"""

from __future__ import annotations

from pathlib import Path
import csv
import json
import math
import sys

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from locationSolver.location_config import CAMERA_CONFIG
from locationSolver.location_items import DEFAULT_MEASUREMENT_WEIGHTS, make_tag_observation
from locationSolver.location_tagmap import load_tag_map
from locationSolver.location_geometry import abs_angle_error_deg
from locationSolver.location_joint_solver import JointSolverConfig, solve_distance_angle_pose
from locationSolver.location_joint_solver_yaw import JointYawSolverConfig, solve_distance_angle_yaw_pose


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


def build_observation_records_from_step1_df(df: pd.DataFrame) -> list[dict]:
    col_dataset = _first_existing_col(df, ["dataset", "source_dataset"])
    col_gt_x = _first_existing_col(df, ["gt_x", "true_robot_x", "robot_x"])
    col_gt_y = _first_existing_col(df, ["gt_y", "true_robot_y", "robot_y"])
    col_gt_h = _first_existing_col(df, ["gt_h", "gt_heading", "true_robot_h", "robot_heading"])
    col_tag = _first_existing_col(df, ["tag_id", "tag", "tag_no"])
    col_cam = _first_existing_col(df, ["camera_role", "camera", "cam_role"])
    col_d = _first_existing_col(df, ["meas_distance_m", "distance_m", "measured_distance_m"])
    col_a = _first_existing_col(df, ["meas_angle_deg", "angle_deg", "measured_angle_deg", "angle_deg_cw"])
    col_yaw = _first_existing_col(df, ["meas_yaw_deg", "yaw_deg", "measured_yaw_deg"])

    required = {
        "gt_x": col_gt_x,
        "gt_y": col_gt_y,
        "gt_h": col_gt_h,
        "tag_id": col_tag,
        "camera_role": col_cam,
        "meas_distance_m": col_d,
        "meas_angle_deg": col_a,
        "meas_yaw_deg": col_yaw,
    }
    missing = [name for name, col in required.items() if col is None]
    if missing:
        raise ValueError(f"Missing required columns {missing}. CSV columns={list(df.columns)}")

    records = []
    for row_index, r in df.iterrows():
        obs = make_tag_observation(
            tag_id=int(r[col_tag]),
            camera_role=_str_value(r, col_cam),
            distance_m=_float_value(r, col_d),
            angle_deg=_float_value(r, col_a),
            yaw_deg=_float_value(r, col_yaw),
            source=f"row={row_index}",
        )

        records.append({
            "row_index": int(row_index),
            "dataset": _str_value(r, col_dataset),
            "gt_x": _float_value(r, col_gt_x),
            "gt_y": _float_value(r, col_gt_y),
            "gt_h": _float_value(r, col_gt_h),
            "observation": obs,
        })

    return records


def group_records(records: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for rec in records:
        key = (
            f"{rec['dataset']}"
            f"_x{rec['gt_x']:.3f}"
            f"_y{rec['gt_y']:.3f}"
            f"_h{rec['gt_h']:.1f}"
        )
        groups.setdefault(key, []).append(rec)
    return groups


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


def result_summary_row(group_key: str, records: list[dict], result_no_yaw, result_with_yaw, initial_x: float, initial_y: float, initial_h: float) -> dict:
    gt_x = records[0]["gt_x"]
    gt_y = records[0]["gt_y"]
    gt_h = records[0]["gt_h"]

    pos_err_no_yaw_cm = 100.0 * math.hypot(result_no_yaw.x_m - gt_x, result_no_yaw.y_m - gt_y)
    pos_err_with_yaw_cm = 100.0 * math.hypot(result_with_yaw.x_m - gt_x, result_with_yaw.y_m - gt_y)

    heading_err_no_yaw_deg = abs_angle_error_deg(result_no_yaw.heading_deg, gt_h)
    heading_err_with_yaw_deg = abs_angle_error_deg(result_with_yaw.heading_deg, gt_h)

    yaw_shift_pos_cm = 100.0 * math.hypot(result_with_yaw.x_m - result_no_yaw.x_m, result_with_yaw.y_m - result_no_yaw.y_m)
    yaw_shift_heading_deg = abs_angle_error_deg(result_with_yaw.heading_deg, result_no_yaw.heading_deg)

    return {
        "group_key": group_key,
        "dataset": records[0]["dataset"],
        "gt_x": gt_x,
        "gt_y": gt_y,
        "gt_h": gt_h,

        "initial_x": initial_x,
        "initial_y": initial_y,
        "initial_h": initial_h,

        "no_yaw_ok": result_no_yaw.ok,
        "with_yaw_ok": result_with_yaw.ok,

        "no_yaw_est_x": result_no_yaw.x_m,
        "no_yaw_est_y": result_no_yaw.y_m,
        "no_yaw_est_h": result_no_yaw.heading_deg,
        "no_yaw_pos_err_cm": pos_err_no_yaw_cm,
        "no_yaw_heading_err_deg": heading_err_no_yaw_deg,
        "no_yaw_distance_rms_cm": result_no_yaw.distance_rms_cm,
        "no_yaw_angle_rms_deg": result_no_yaw.angle_rms_deg,

        "with_yaw_est_x": result_with_yaw.x_m,
        "with_yaw_est_y": result_with_yaw.y_m,
        "with_yaw_est_h": result_with_yaw.heading_deg,
        "with_yaw_pos_err_cm": pos_err_with_yaw_cm,
        "with_yaw_heading_err_deg": heading_err_with_yaw_deg,
        "with_yaw_distance_rms_cm": result_with_yaw.distance_rms_cm,
        "with_yaw_angle_rms_deg": result_with_yaw.angle_rms_deg,
        "with_yaw_yaw_rms_deg": result_with_yaw.yaw_rms_deg,

        "yaw_shift_pos_cm": yaw_shift_pos_cm,
        "yaw_shift_heading_deg": yaw_shift_heading_deg,

        "used_tag_count": result_with_yaw.used_tag_count,
        "used_distance_count": result_with_yaw.used_distance_count,
        "used_angle_count": result_with_yaw.used_angle_count,
        "used_yaw_count": result_with_yaw.used_yaw_count,

        "with_yaw_cost": result_with_yaw.cost,
        "with_yaw_evaluations": result_with_yaw.evaluations,
    }


def residual_rows_for_group(group_key: str, records: list[dict], result) -> list[dict]:
    out = []
    gt_x = records[0]["gt_x"]
    gt_y = records[0]["gt_y"]
    gt_h = records[0]["gt_h"]

    for r in result.residuals:
        out.append({
            "group_key": group_key,
            "dataset": records[0]["dataset"],
            "gt_x": gt_x,
            "gt_y": gt_y,
            "gt_h": gt_h,
            "est_x": result.x_m,
            "est_y": result.y_m,
            "est_h": result.heading_deg,

            "tag_id": r.tag_id,
            "camera_role": r.camera_role,

            "measured_distance_m": r.measured_distance_m,
            "predicted_distance_m": r.predicted_distance_m,
            "distance_residual_m": r.distance_residual_m,
            "distance_residual_cm": r.distance_residual_cm,
            "distance_weight": r.distance_weight,

            "measured_angle_deg": r.measured_angle_deg,
            "predicted_angle_deg": r.predicted_angle_deg,
            "angle_residual_deg": r.angle_residual_deg,
            "angle_weight": r.angle_weight,

            "measured_yaw_deg": r.measured_yaw_deg,
            "predicted_yaw_deg": r.predicted_yaw_deg,
            "yaw_residual_deg": r.yaw_residual_deg,
            "yaw_weight": r.yaw_weight,
        })
    return out


def write_report(summary_rows: list[dict], output_dir: Path) -> None:
    df = pd.DataFrame(summary_rows)

    lines: list[str] = []
    lines.append("DISTANCE + ANGLE + WEAK YAW SOLVER TEST SUMMARY")
    lines.append("=" * 80)
    lines.append(f"group count: {len(df)}")
    lines.append("Part 6 compares no-yaw result against weak-yaw result.")
    lines.append("")

    if len(df):
        ok_df = df[df["with_yaw_ok"] == True].copy()
        lines.append(f"ok groups: {len(ok_df)}")
        lines.append(f"failed groups: {len(df) - len(ok_df)}")
        lines.append("")

        for col, unit in [
            ("with_yaw_pos_err_cm", " cm"),
            ("with_yaw_heading_err_deg", " deg"),
            ("with_yaw_distance_rms_cm", " cm"),
            ("with_yaw_angle_rms_deg", " deg"),
            ("with_yaw_yaw_rms_deg", " deg"),
            ("yaw_shift_pos_cm", " cm"),
            ("yaw_shift_heading_deg", " deg"),
        ]:
            vals = pd.to_numeric(ok_df[col], errors="coerce").dropna()
            if len(vals):
                lines.append(
                    f"{col}: mean={vals.mean():.3f}{unit}, "
                    f"median={vals.median():.3f}{unit}, "
                    f"p90={vals.quantile(0.90):.3f}{unit}, "
                    f"max={vals.max():.3f}{unit}"
                )

        lines.append("")
        lines.append("Worst 20 groups by yaw-induced position shift:")
        cols = [
            "group_key",
            "gt_x", "gt_y", "gt_h",
            "no_yaw_est_x", "no_yaw_est_y", "no_yaw_est_h",
            "with_yaw_est_x", "with_yaw_est_y", "with_yaw_est_h",
            "yaw_shift_pos_cm", "yaw_shift_heading_deg",
            "with_yaw_pos_err_cm", "with_yaw_heading_err_deg",
            "with_yaw_yaw_rms_deg",
        ]
        lines.append(
            ok_df.sort_values("yaw_shift_pos_cm", ascending=False)
                 .head(20)[cols]
                 .to_string(index=False)
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "distance_angle_yaw_solver_summary.txt").write_text(
        "\n".join(lines),
        encoding="utf-8",
    )
    print("\n".join(lines))


if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Adjustable test parameters
    # -------------------------------------------------------------------------
    INPUT_CSV = ROOT_DIR / "testLocation" / "input" / "step1_observations.csv"
    TAG_FILE = ROOT_DIR / "sitemap" / "DemoRoom" / "tag_location.txt"
    OUTPUT_DIR = ROOT_DIR / "testLocation" / "output" / "distance_angle_yaw_solver"

    camera_config = CAMERA_CONFIG

    # Start from default weights. Yaw is deliberately weak:
    #   front yaw 0.05, rear yaw 0.01
    measurement_weights = DEFAULT_MEASUREMENT_WEIGHTS

    solver_config_no_yaw = JointSolverConfig(
        sigma_distance_m=0.05,
        sigma_angle_deg=3.0,
        search_initial_step_xy_m=0.20,
        search_initial_step_h_deg=10.0,
        search_min_step_xy_m=0.002,
        search_min_step_h_deg=0.10,
        max_iterations=250,
        robust_clip_sigma=3.0,
        x_min=-1.0,
        x_max=12.5,
        y_min=-1.0,
        y_max=12.5,
        local_heading_seed_offsets_deg=(-30.0, -15.0, 0.0, 15.0, 30.0),
        use_global_heading_seeds=True,
        global_heading_seed_step_deg=45.0,
    )

    solver_config_with_yaw = JointYawSolverConfig(
        sigma_distance_m=0.05,
        sigma_angle_deg=3.0,
        sigma_yaw_deg=10.0,
        search_initial_step_xy_m=0.20,
        search_initial_step_h_deg=10.0,
        search_min_step_xy_m=0.002,
        search_min_step_h_deg=0.10,
        max_iterations=250,
        robust_clip_sigma=3.0,
        x_min=-1.0,
        x_max=12.5,
        y_min=-1.0,
        y_max=12.5,
        local_heading_seed_offsets_deg=(-30.0, -15.0, 0.0, 15.0, 30.0),
        use_global_heading_seeds=True,
        global_heading_seed_step_deg=45.0,
    )

    INITIAL_MODE = "perturbed"
    INITIAL_X_OFFSET_M = 0.20
    INITIAL_Y_OFFSET_M = -0.15
    INITIAL_H_OFFSET_DEG = 20.0

    WRITE_DETAIL_RESIDUALS = True

    # -------------------------------------------------------------------------
    # Run test
    # -------------------------------------------------------------------------
    print("Running t7 distance+angle+weak-yaw solver test")
    print(f"ROOT_DIR   = {ROOT_DIR}")
    print(f"INPUT_CSV  = {INPUT_CSV}")
    print(f"TAG_FILE   = {TAG_FILE}")
    print(f"OUTPUT_DIR = {OUTPUT_DIR}")
    print(f"INITIAL_MODE = {INITIAL_MODE}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    tag_map = load_tag_map(TAG_FILE)
    df = pd.read_csv(INPUT_CSV)
    records = build_observation_records_from_step1_df(df)
    groups = group_records(records)

    summary_rows = []
    detail_rows = []

    for group_key, group_records in groups.items():
        gt_x = group_records[0]["gt_x"]
        gt_y = group_records[0]["gt_y"]
        gt_h = group_records[0]["gt_h"]

        if INITIAL_MODE == "gt":
            initial_x = gt_x
            initial_y = gt_y
            initial_h = gt_h
        elif INITIAL_MODE == "perturbed":
            initial_x = gt_x + INITIAL_X_OFFSET_M
            initial_y = gt_y + INITIAL_Y_OFFSET_M
            initial_h = (gt_h + INITIAL_H_OFFSET_DEG) % 360.0
        else:
            raise ValueError(f"Unknown INITIAL_MODE={INITIAL_MODE!r}")

        observations = [r["observation"] for r in group_records]

        result_no_yaw = solve_distance_angle_pose(
            observations=observations,
            tag_map=tag_map,
            camera_config=camera_config,
            measurement_weights=measurement_weights,
            initial_x_m=initial_x,
            initial_y_m=initial_y,
            initial_heading_deg=initial_h,
            solver_config=solver_config_no_yaw,
        )

        result_with_yaw = solve_distance_angle_yaw_pose(
            observations=observations,
            tag_map=tag_map,
            camera_config=camera_config,
            measurement_weights=measurement_weights,
            initial_x_m=initial_x,
            initial_y_m=initial_y,
            initial_heading_deg=initial_h,
            solver_config=solver_config_with_yaw,
        )

        summary_rows.append(
            result_summary_row(
                group_key=group_key,
                records=group_records,
                result_no_yaw=result_no_yaw,
                result_with_yaw=result_with_yaw,
                initial_x=initial_x,
                initial_y=initial_y,
                initial_h=initial_h,
            )
        )

        if WRITE_DETAIL_RESIDUALS:
            detail_rows.extend(
                residual_rows_for_group(
                    group_key=group_key,
                    records=group_records,
                    result=result_with_yaw,
                )
            )

    write_csv(summary_rows, OUTPUT_DIR / "distance_angle_yaw_solver_group_summary.csv")

    if WRITE_DETAIL_RESIDUALS:
        write_csv(detail_rows, OUTPUT_DIR / "distance_angle_yaw_solver_residuals.csv")

    config_dump = {
        "input_csv": str(INPUT_CSV),
        "tag_file": str(TAG_FILE),
        "output_dir": str(OUTPUT_DIR),
        "initial_mode": INITIAL_MODE,
        "initial_x_offset_m": INITIAL_X_OFFSET_M,
        "initial_y_offset_m": INITIAL_Y_OFFSET_M,
        "initial_h_offset_deg": INITIAL_H_OFFSET_DEG,
        "camera_config": camera_config,
        "measurement_weights": measurement_weights,
        "solver_config_no_yaw": solver_config_no_yaw.__dict__,
        "solver_config_with_yaw": solver_config_with_yaw.__dict__,
    }
    (OUTPUT_DIR / "distance_angle_yaw_solver_config.json").write_text(
        json.dumps(config_dump, indent=2),
        encoding="utf-8",
    )

    write_report(summary_rows, OUTPUT_DIR)

    print("")
    print("Wrote:")
    print(f"  {OUTPUT_DIR / 'distance_angle_yaw_solver_group_summary.csv'}")
    if WRITE_DETAIL_RESIDUALS:
        print(f"  {OUTPUT_DIR / 'distance_angle_yaw_solver_residuals.csv'}")
    print(f"  {OUTPUT_DIR / 'distance_angle_yaw_solver_config.json'}")
    print(f"  {OUTPUT_DIR / 'distance_angle_yaw_solver_summary.txt'}")
