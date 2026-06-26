"""
t6_test_distance_angle_solver.py

Test script for Part 5:
Joint distance + view-angle solver.

This file belongs under:
    D:\Data\_Action\_RunNMS\testLocation

Expected input:
    D:\Data\_Action\_RunNMS\testLocation\input\step1_observations.csv
    D:\Data\_Action\_RunNMS\sitemap\DemoRoom\tag_location.txt

Output:
    D:\Data\_Action\_RunNMS\testLocation\output\distance_angle_solver

No argparse is used. Adjustable parameters and paths are under:
    if __name__ == "__main__":

Important diagnostic note
-------------------------
This old data is polluted by previous tag-map and rear-camera issues.
Use this test to confirm mechanics and output format first, not final accuracy.
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
from locationSolver.location_joint_solver import (
    JointSolverConfig,
    solve_distance_angle_pose,
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


def result_summary_row(group_key: str, records: list[dict], result, initial_x: float, initial_y: float, initial_h: float) -> dict:
    gt_x = records[0]["gt_x"]
    gt_y = records[0]["gt_y"]
    gt_h = records[0]["gt_h"]

    pos_err_cm = 100.0 * math.hypot(result.x_m - gt_x, result.y_m - gt_y)
    initial_pos_err_cm = 100.0 * math.hypot(initial_x - gt_x, initial_y - gt_y)
    heading_err_deg = abs_angle_error_deg(result.heading_deg, gt_h)
    initial_heading_err_deg = abs_angle_error_deg(initial_h, gt_h)

    return {
        "group_key": group_key,
        "dataset": records[0]["dataset"],
        "gt_x": gt_x,
        "gt_y": gt_y,
        "gt_h": gt_h,

        "initial_x": initial_x,
        "initial_y": initial_y,
        "initial_h": initial_h,

        "est_x": result.x_m,
        "est_y": result.y_m,
        "est_h": result.heading_deg,

        "ok": result.ok,
        "detail": result.detail,
        "observation_count": len(records),
        "used_tag_count": result.used_tag_count,
        "used_distance_count": result.used_distance_count,
        "used_angle_count": result.used_angle_count,

        "initial_pos_err_cm": initial_pos_err_cm,
        "pos_err_cm": pos_err_cm,
        "initial_heading_err_deg": initial_heading_err_deg,
        "heading_err_deg": heading_err_deg,

        "distance_rms_cm": result.distance_rms_cm,
        "distance_max_abs_cm": result.distance_max_abs_cm,
        "angle_rms_deg": result.angle_rms_deg,
        "angle_max_abs_deg": result.angle_max_abs_deg,

        "cost": result.cost,
        "seed_count": result.seed_count,
        "best_seed_heading_deg": result.best_seed_heading_deg,
        "iterations": result.iterations,
        "evaluations": result.evaluations,
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
        })
    return out


def write_report(summary_rows: list[dict], output_dir: Path) -> None:
    df = pd.DataFrame(summary_rows)

    lines: list[str] = []
    lines.append("DISTANCE + ANGLE SOLVER TEST SUMMARY")
    lines.append("=" * 80)
    lines.append(f"group count: {len(df)}")
    lines.append("yaw: not used in Part 5")
    lines.append("")

    if len(df):
        ok_df = df[df["ok"] == True].copy()
        lines.append(f"ok groups: {len(ok_df)}")
        lines.append(f"failed groups: {len(df) - len(ok_df)}")
        lines.append("")

        for col, unit in [
            ("pos_err_cm", " cm"),
            ("heading_err_deg", " deg"),
            ("distance_rms_cm", " cm"),
            ("angle_rms_deg", " deg"),
            ("used_tag_count", ""),
            ("evaluations", ""),
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
        lines.append("Worst 20 groups by position error:")
        cols = [
            "group_key",
            "gt_x", "gt_y", "gt_h",
            "est_x", "est_y", "est_h",
            "pos_err_cm", "heading_err_deg",
            "used_tag_count",
            "distance_rms_cm",
            "angle_rms_deg",
        ]
        lines.append(
            ok_df.sort_values("pos_err_cm", ascending=False)
                 .head(20)[cols]
                 .to_string(index=False)
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "distance_angle_solver_summary.txt").write_text(
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
    OUTPUT_DIR = ROOT_DIR / "testLocation" / "output" / "distance_angle_solver"

    camera_config = CAMERA_CONFIG
    measurement_weights = DEFAULT_MEASUREMENT_WEIGHTS

    solver_config = JointSolverConfig(
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

    # Start from a deliberately perturbed pose to check convergence.
    INITIAL_MODE = "perturbed"

    INITIAL_X_OFFSET_M = 0.20
    INITIAL_Y_OFFSET_M = -0.15
    INITIAL_H_OFFSET_DEG = 20.0

    WRITE_DETAIL_RESIDUALS = True

    # -------------------------------------------------------------------------
    # Run test
    # -------------------------------------------------------------------------
    print("Running t6 distance+angle solver test")
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

        result = solve_distance_angle_pose(
            observations=observations,
            tag_map=tag_map,
            camera_config=camera_config,
            measurement_weights=measurement_weights,
            initial_x_m=initial_x,
            initial_y_m=initial_y,
            initial_heading_deg=initial_h,
            solver_config=solver_config,
        )

        summary_rows.append(
            result_summary_row(
                group_key=group_key,
                records=group_records,
                result=result,
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
                    result=result,
                )
            )

    write_csv(summary_rows, OUTPUT_DIR / "distance_angle_solver_group_summary.csv")

    if WRITE_DETAIL_RESIDUALS:
        write_csv(detail_rows, OUTPUT_DIR / "distance_angle_solver_residuals.csv")

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
        "solver_config": solver_config.__dict__,
    }
    (OUTPUT_DIR / "distance_angle_solver_config.json").write_text(
        json.dumps(config_dump, indent=2),
        encoding="utf-8",
    )

    write_report(summary_rows, OUTPUT_DIR)

    print("")
    print("Wrote:")
    print(f"  {OUTPUT_DIR / 'distance_angle_solver_group_summary.csv'}")
    if WRITE_DETAIL_RESIDUALS:
        print(f"  {OUTPUT_DIR / 'distance_angle_solver_residuals.csv'}")
    print(f"  {OUTPUT_DIR / 'distance_angle_solver_config.json'}")
    print(f"  {OUTPUT_DIR / 'distance_angle_solver_summary.txt'}")
