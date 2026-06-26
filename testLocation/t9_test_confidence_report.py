"""
t9_test_confidence_report.py

Test script for Part 8:
Confidence and diversity report.

This file belongs under:
    D:\Data\_Action\_RunNMS\testLocation

Expected input:
    D:\Data\_Action\_RunNMS\testLocation\input\step1_observations.csv
    D:\Data\_Action\_RunNMS\sitemap\DemoRoom\tag_location.txt

Output:
    D:\Data\_Action\_RunNMS\testLocation\output\confidence_report

No argparse is used. Adjustable parameters and paths are under:
    if __name__ == "__main__":

Important diagnostic note
-------------------------
The old data tries to solve every robot orientation. Many headings are
naturally poor for localization. Therefore confidence is expected to reject
many groups as BAD or FAILED. That is the correct behavior.
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
from locationSolver.location_joint_solver_yaw import JointYawSolverConfig, solve_distance_angle_yaw_pose
from locationSolver.location_item_filter import (
    ItemFilterConfig,
    build_filter_states_from_joint_yaw_residuals,
    count_filter_states,
)
from locationSolver.location_filtered_solver import solve_filtered_pose
from locationSolver.location_confidence import ConfidenceConfig, evaluate_pose_confidence


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
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


def summary_row(group_key: str, records: list[dict], filtered, confidence_report, filter_counts: dict) -> dict:
    gt_x = records[0]["gt_x"]
    gt_y = records[0]["gt_y"]
    gt_h = records[0]["gt_h"]

    pos_err_cm = 100.0 * math.hypot(filtered.x_m - gt_x, filtered.y_m - gt_y)
    heading_err_deg = abs_angle_error_deg(filtered.heading_deg, gt_h)

    d = confidence_report.to_dict()

    return {
        "group_key": group_key,
        "dataset": records[0]["dataset"],
        "gt_x": gt_x,
        "gt_y": gt_y,
        "gt_h": gt_h,

        "est_x": filtered.x_m,
        "est_y": filtered.y_m,
        "est_h": filtered.heading_deg,

        "pos_err_cm": pos_err_cm,
        "heading_err_deg": heading_err_deg,

        "confidence": d["confidence"],
        "ok_for_true_location_update": d["ok_for_true_location_update"],
        "ok_for_correction_motion": d["ok_for_correction_motion"],
        "reason": d["reason"],

        "distance_items": d["distance_items"],
        "angle_items": d["angle_items"],
        "yaw_items": d["yaw_items"],
        "distance_tag_count": d["distance_tag_count"],
        "angle_tag_count": d["angle_tag_count"],
        "yaw_tag_count": d["yaw_tag_count"],

        "distance_bearing_span_deg": d["distance_bearing_span_deg"],
        "angle_bearing_span_deg": d["angle_bearing_span_deg"],

        "distance_rms_cm": d["distance_rms_cm"],
        "distance_max_abs_cm": d["distance_max_abs_cm"],
        "angle_rms_deg": d["angle_rms_deg"],
        "angle_max_abs_deg": d["angle_max_abs_deg"],
        "yaw_rms_deg": d["yaw_rms_deg"],
        "yaw_max_abs_deg": d["yaw_max_abs_deg"],

        "filter_enabled": filter_counts["enabled"],
        "filter_disabled": filter_counts["disabled"],
        "distance_disabled": filter_counts["distance_disabled"],
        "angle_disabled": filter_counts["angle_disabled"],
        "yaw_disabled": filter_counts["yaw_disabled"],
    }


def write_report(summary_rows: list[dict], output_dir: Path) -> None:
    df = pd.DataFrame(summary_rows)

    lines: list[str] = []
    lines.append("CONFIDENCE REPORT TEST SUMMARY")
    lines.append("=" * 80)
    lines.append(f"group count: {len(df)}")
    lines.append("Old data includes many non-preferred orientations, so many BAD/FAILED results are expected.")
    lines.append("")

    if len(df):
        lines.append("Confidence counts:")
        lines.append(df["confidence"].value_counts().to_string())
        lines.append("")

        lines.append("Update/correction permission counts:")
        lines.append(f"ok_for_true_location_update: {int(df['ok_for_true_location_update'].sum())}")
        lines.append(f"ok_for_correction_motion: {int(df['ok_for_correction_motion'].sum())}")
        lines.append("")

        for conf in ["GOOD", "MARGINAL", "BAD", "FAILED"]:
            g = df[df["confidence"] == conf]
            if len(g) == 0:
                continue
            lines.append(f"[{conf}] n={len(g)}")
            for col, unit in [
                ("pos_err_cm", " cm"),
                ("heading_err_deg", " deg"),
                ("distance_rms_cm", " cm"),
                ("angle_rms_deg", " deg"),
                ("distance_bearing_span_deg", " deg"),
            ]:
                vals = pd.to_numeric(g[col], errors="coerce").dropna()
                if len(vals):
                    lines.append(
                        f"  {col}: median={vals.median():.3f}{unit}, "
                        f"p90={vals.quantile(0.90):.3f}{unit}, "
                        f"max={vals.max():.3f}{unit}"
                    )
            lines.append("")

        lines.append("Worst 20 accepted-for-update groups by position error:")
        accepted = df[df["ok_for_true_location_update"] == True].copy()
        if len(accepted):
            cols = [
                "group_key",
                "confidence",
                "gt_x", "gt_y", "gt_h",
                "est_x", "est_y", "est_h",
                "pos_err_cm", "heading_err_deg",
                "distance_items", "angle_items",
                "distance_bearing_span_deg",
                "distance_rms_cm", "angle_rms_deg",
                "reason",
            ]
            lines.append(
                accepted.sort_values("pos_err_cm", ascending=False)
                        .head(20)[cols]
                        .to_string(index=False)
            )
        else:
            lines.append("  none")

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "confidence_report_summary.txt").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Adjustable test parameters
    # -------------------------------------------------------------------------
    INPUT_CSV = ROOT_DIR / "testLocation" / "input" / "step1_observations.csv"
    TAG_FILE = ROOT_DIR / "sitemap" / "DemoRoom" / "tag_location.txt"
    OUTPUT_DIR = ROOT_DIR / "testLocation" / "output" / "confidence_report"

    camera_config = CAMERA_CONFIG
    measurement_weights = DEFAULT_MEASUREMENT_WEIGHTS

    solver_config = JointYawSolverConfig(
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

    filter_config = ItemFilterConfig(
        distance_good_m=0.08,
        distance_weak_m=0.20,
        distance_severe_m=0.35,
        angle_good_deg=5.0,
        angle_weak_deg=12.0,
        angle_severe_deg=20.0,
        yaw_good_deg=10.0,
        yaw_weak_deg=25.0,
        yaw_severe_deg=45.0,
        min_distance_items_to_keep=3,
        min_angle_items_to_keep=2,
        weak_multiplier=0.50,
        bad_but_needed_multiplier=0.25,
        yaw_weak_multiplier=0.25,
    )

    confidence_config = ConfidenceConfig(
        good_min_distance_items=3,
        good_min_angle_items=2,
        good_min_distance_bearing_span_deg=45.0,
        good_min_angle_bearing_span_deg=30.0,
        good_max_distance_rms_cm=8.0,
        good_max_angle_rms_deg=6.0,

        marginal_min_distance_items=2,
        marginal_min_angle_items=1,
        marginal_min_distance_bearing_span_deg=25.0,
        marginal_max_distance_rms_cm=15.0,
        marginal_max_angle_rms_deg=12.0,

        yaw_coherent_rms_deg=20.0,
        bad_max_distance_rms_cm=30.0,
        bad_max_angle_rms_deg=25.0,
    )

    INITIAL_MODE = "perturbed"
    INITIAL_X_OFFSET_M = 0.20
    INITIAL_Y_OFFSET_M = -0.15
    INITIAL_H_OFFSET_DEG = 20.0

    # -------------------------------------------------------------------------
    # Run test
    # -------------------------------------------------------------------------
    print("Running t9 confidence report test")
    print(f"ROOT_DIR   = {ROOT_DIR}")
    print(f"INPUT_CSV  = {INPUT_CSV}")
    print(f"TAG_FILE   = {TAG_FILE}")
    print(f"OUTPUT_DIR = {OUTPUT_DIR}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    tag_map = load_tag_map(TAG_FILE)
    df = pd.read_csv(INPUT_CSV)
    records = build_observation_records_from_step1_df(df)
    groups = group_records(records)

    rows = []

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

        pass1 = solve_distance_angle_yaw_pose(
            observations=observations,
            tag_map=tag_map,
            camera_config=camera_config,
            measurement_weights=measurement_weights,
            initial_x_m=initial_x,
            initial_y_m=initial_y,
            initial_heading_deg=initial_h,
            solver_config=solver_config,
        )

        filter_states = build_filter_states_from_joint_yaw_residuals(
            residual_records=pass1.residuals,
            cfg=filter_config,
        )
        filter_counts = count_filter_states(filter_states.values())

        filtered = solve_filtered_pose(
            observations=observations,
            tag_map=tag_map,
            camera_config=camera_config,
            measurement_weights=measurement_weights,
            filter_states=filter_states,
            initial_x_m=pass1.x_m,
            initial_y_m=pass1.y_m,
            initial_heading_deg=pass1.heading_deg,
            solver_config=solver_config,
        )

        confidence_report = evaluate_pose_confidence(
            final_result=filtered,
            tag_map=tag_map,
            config=confidence_config,
        )

        rows.append(
            summary_row(
                group_key=group_key,
                records=group_records,
                filtered=filtered,
                confidence_report=confidence_report,
                filter_counts=filter_counts,
            )
        )

    write_csv(rows, OUTPUT_DIR / "confidence_report_group_summary.csv")

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
        "filter_config": filter_config.__dict__,
        "confidence_config": confidence_config.__dict__,
    }
    (OUTPUT_DIR / "confidence_report_config.json").write_text(
        json.dumps(config_dump, indent=2),
        encoding="utf-8",
    )

    write_report(rows, OUTPUT_DIR)

    print("")
    print("Wrote:")
    print(f"  {OUTPUT_DIR / 'confidence_report_group_summary.csv'}")
    print(f"  {OUTPUT_DIR / 'confidence_report_config.json'}")
    print(f"  {OUTPUT_DIR / 'confidence_report_summary.txt'}")
