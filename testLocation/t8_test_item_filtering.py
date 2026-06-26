"""
t8_test_item_filtering.py

Test script for Part 7:
Item-level residual filtering and re-solve.

This file belongs under:
    D:\Data\_Action\_RunNMS\testLocation

Expected input:
    D:\Data\_Action\_RunNMS\testLocation\input\step1_observations.csv
    D:\Data\_Action\_RunNMS\sitemap\DemoRoom\tag_location.txt

Output:
    D:\Data\_Action\_RunNMS\testLocation\output\item_filtering

No argparse is used. Adjustable parameters and paths are under:
    if __name__ == "__main__":
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


def result_summary_row(group_key: str, records: list[dict], pass1, filtered, filter_counts: dict) -> dict:
    gt_x = records[0]["gt_x"]
    gt_y = records[0]["gt_y"]
    gt_h = records[0]["gt_h"]

    pass1_pos_err_cm = 100.0 * math.hypot(pass1.x_m - gt_x, pass1.y_m - gt_y)
    filtered_pos_err_cm = 100.0 * math.hypot(filtered.x_m - gt_x, filtered.y_m - gt_y)

    pass1_h_err = abs_angle_error_deg(pass1.heading_deg, gt_h)
    filtered_h_err = abs_angle_error_deg(filtered.heading_deg, gt_h)

    shift_cm = 100.0 * math.hypot(filtered.x_m - pass1.x_m, filtered.y_m - pass1.y_m)
    shift_h = abs_angle_error_deg(filtered.heading_deg, pass1.heading_deg)

    return {
        "group_key": group_key,
        "dataset": records[0]["dataset"],
        "gt_x": gt_x,
        "gt_y": gt_y,
        "gt_h": gt_h,

        "pass1_ok": pass1.ok,
        "filtered_ok": filtered.ok,

        "pass1_x": pass1.x_m,
        "pass1_y": pass1.y_m,
        "pass1_h": pass1.heading_deg,
        "pass1_pos_err_cm": pass1_pos_err_cm,
        "pass1_heading_err_deg": pass1_h_err,
        "pass1_distance_rms_cm": pass1.distance_rms_cm,
        "pass1_angle_rms_deg": pass1.angle_rms_deg,
        "pass1_yaw_rms_deg": pass1.yaw_rms_deg,

        "filtered_x": filtered.x_m,
        "filtered_y": filtered.y_m,
        "filtered_h": filtered.heading_deg,
        "filtered_pos_err_cm": filtered_pos_err_cm,
        "filtered_heading_err_deg": filtered_h_err,
        "filtered_distance_rms_cm": filtered.distance_rms_cm,
        "filtered_angle_rms_deg": filtered.angle_rms_deg,
        "filtered_yaw_rms_deg": filtered.yaw_rms_deg,

        "filter_shift_pos_cm": shift_cm,
        "filter_shift_heading_deg": shift_h,

        "filter_total": filter_counts["total"],
        "filter_enabled": filter_counts["enabled"],
        "filter_disabled": filter_counts["disabled"],
        "distance_enabled": filter_counts["distance_enabled"],
        "angle_enabled": filter_counts["angle_enabled"],
        "yaw_enabled": filter_counts["yaw_enabled"],
        "distance_disabled": filter_counts["distance_disabled"],
        "angle_disabled": filter_counts["angle_disabled"],
        "yaw_disabled": filter_counts["yaw_disabled"],
    }


def filter_state_rows(group_key: str, records: list[dict], filter_states) -> list[dict]:
    out = []
    meta = {
        "dataset": records[0]["dataset"],
        "gt_x": records[0]["gt_x"],
        "gt_y": records[0]["gt_y"],
        "gt_h": records[0]["gt_h"],
    }
    for state in filter_states.values():
        out.append({
            "group_key": group_key,
            **meta,
            **state.to_dict(),
        })
    return out


def residual_rows(group_key: str, records: list[dict], result) -> list[dict]:
    out = []
    meta = {
        "dataset": records[0]["dataset"],
        "gt_x": records[0]["gt_x"],
        "gt_y": records[0]["gt_y"],
        "gt_h": records[0]["gt_h"],
        "est_x": result.x_m,
        "est_y": result.y_m,
        "est_h": result.heading_deg,
    }
    for r in result.residuals:
        out.append({
            "group_key": group_key,
            **meta,
            "tag_id": r.tag_id,
            "camera_role": r.camera_role,
            "distance_residual_cm": r.distance_residual_cm,
            "distance_weight": r.distance_weight,
            "angle_residual_deg": r.angle_residual_deg,
            "angle_weight": r.angle_weight,
            "yaw_residual_deg": r.yaw_residual_deg,
            "yaw_weight": r.yaw_weight,
        })
    return out


def write_report(summary_rows: list[dict], output_dir: Path) -> None:
    df = pd.DataFrame(summary_rows)
    lines: list[str] = []

    lines.append("ITEM-LEVEL FILTERING TEST SUMMARY")
    lines.append("=" * 80)
    lines.append(f"group count: {len(df)}")
    lines.append("pass1: distance + angle + weak yaw")
    lines.append("filtered: pass1 residuals classified, bad/weak items adjusted, then re-solved")
    lines.append("")

    if len(df):
        ok_df = df[df["filtered_ok"] == True].copy()
        lines.append(f"ok groups: {len(ok_df)}")
        lines.append(f"failed groups: {len(df) - len(ok_df)}")
        lines.append("")

        for col, unit in [
            ("filtered_pos_err_cm", " cm"),
            ("filtered_heading_err_deg", " deg"),
            ("filtered_distance_rms_cm", " cm"),
            ("filtered_angle_rms_deg", " deg"),
            ("filter_shift_pos_cm", " cm"),
            ("filter_shift_heading_deg", " deg"),
            ("filter_disabled", ""),
            ("yaw_disabled", ""),
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
        lines.append("Worst 20 by filtered position error:")
        cols = [
            "group_key",
            "gt_x", "gt_y", "gt_h",
            "filtered_x", "filtered_y", "filtered_h",
            "filtered_pos_err_cm", "filtered_heading_err_deg",
            "filtered_distance_rms_cm", "filtered_angle_rms_deg",
            "filter_disabled", "yaw_disabled",
        ]
        lines.append(
            ok_df.sort_values("filtered_pos_err_cm", ascending=False)
                 .head(20)[cols]
                 .to_string(index=False)
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "item_filtering_summary.txt").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Adjustable test parameters
    # -------------------------------------------------------------------------
    INPUT_CSV = ROOT_DIR / "testLocation" / "input" / "step1_observations.csv"
    TAG_FILE = ROOT_DIR / "sitemap" / "DemoRoom" / "tag_location.txt"
    OUTPUT_DIR = ROOT_DIR / "testLocation" / "output" / "item_filtering"

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

    INITIAL_MODE = "perturbed"
    INITIAL_X_OFFSET_M = 0.20
    INITIAL_Y_OFFSET_M = -0.15
    INITIAL_H_OFFSET_DEG = 20.0

    WRITE_DETAIL = True

    # -------------------------------------------------------------------------
    # Run test
    # -------------------------------------------------------------------------
    print("Running t8 item-level filtering test")
    print(f"ROOT_DIR   = {ROOT_DIR}")
    print(f"INPUT_CSV  = {INPUT_CSV}")
    print(f"TAG_FILE   = {TAG_FILE}")
    print(f"OUTPUT_DIR = {OUTPUT_DIR}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    tag_map = load_tag_map(TAG_FILE)
    df = pd.read_csv(INPUT_CSV)
    records = build_observation_records_from_step1_df(df)
    groups = group_records(records)

    summary_rows = []
    filter_rows = []
    filtered_residual_rows = []

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
        counts = count_filter_states(filter_states.values())

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

        summary_rows.append(
            result_summary_row(
                group_key=group_key,
                records=group_records,
                pass1=pass1,
                filtered=filtered,
                filter_counts=counts,
            )
        )

        if WRITE_DETAIL:
            filter_rows.extend(filter_state_rows(group_key, group_records, filter_states))
            filtered_residual_rows.extend(residual_rows(group_key, group_records, filtered))

    write_csv(summary_rows, OUTPUT_DIR / "item_filtering_group_summary.csv")
    if WRITE_DETAIL:
        write_csv(filter_rows, OUTPUT_DIR / "item_filtering_item_states.csv")
        write_csv(filtered_residual_rows, OUTPUT_DIR / "item_filtering_filtered_residuals.csv")

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
    }
    (OUTPUT_DIR / "item_filtering_config.json").write_text(
        json.dumps(config_dump, indent=2),
        encoding="utf-8",
    )

    write_report(summary_rows, OUTPUT_DIR)

    print("")
    print("Wrote:")
    print(f"  {OUTPUT_DIR / 'item_filtering_group_summary.csv'}")
    if WRITE_DETAIL:
        print(f"  {OUTPUT_DIR / 'item_filtering_item_states.csv'}")
        print(f"  {OUTPUT_DIR / 'item_filtering_filtered_residuals.csv'}")
    print(f"  {OUTPUT_DIR / 'item_filtering_config.json'}")
    print(f"  {OUTPUT_DIR / 'item_filtering_summary.txt'}")
