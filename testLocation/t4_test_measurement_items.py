"""
t4_test_measurement_items.py

Test script for Part 3:
Convert tag observations into independent measurement items.

This file belongs under:
    D:\Data\_Action\_RunNMS\testLocation

It imports reusable logic from:
    D:\Data\_Action\_RunNMS\locationSolver

Expected input:
    D:\Data\_Action\_RunNMS\testLocation\input\step1_observations.csv

Output:
    D:\Data\_Action\_RunNMS\testLocation\output\measurement_items

No argparse is used. Adjustable parameters and paths are under:
    if __name__ == "__main__":
"""

from __future__ import annotations

from pathlib import Path
import csv
import json
import sys

import pandas as pd


# Allow direct run from VS Code:
#     python t4_test_measurement_items.py
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from locationSolver.location_items import (
    DEFAULT_ITEM_SIGMA,
    DEFAULT_MEASUREMENT_WEIGHTS,
    make_tag_observation,
    build_measurement_items,
    count_items_by_kind,
    count_items_by_camera_and_kind,
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


def build_observations_from_step1_df(df: pd.DataFrame):
    """
    Convert step1_observations.csv rows into TagObservation objects.

    This is test/data-adapter logic only.
    The production solver will receive observations from mobility reports.
    """
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


def group_observation_records(records: list[dict]) -> dict[str, list[dict]]:
    """
    Group observations by dataset + ground-truth robot pose.

    This is only for testing with step1_observations.csv.
    """
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


def flatten_items_for_csv(group_key: str, records: list[dict], mode: str, items) -> list[dict]:
    """
    Convert MeasurementItem objects into CSV rows for inspection.
    """
    # Map tag/camera/source metadata for readability.
    obs_meta = {}
    for rec in records:
        obs = rec["observation"]
        obs_meta[(obs.tag_id, obs.camera_role)] = rec

    out = []
    for item in items:
        rec = obs_meta.get((item.tag_id, item.camera_role), {})
        out.append({
            "group_key": group_key,
            "mode": mode,
            "dataset": rec.get("dataset", ""),
            "gt_x": rec.get("gt_x", ""),
            "gt_y": rec.get("gt_y", ""),
            "gt_h": rec.get("gt_h", ""),
            **item.to_dict(),
        })
    return out


if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Adjustable test parameters
    # -------------------------------------------------------------------------
    INPUT_CSV = ROOT_DIR / "testLocation" / "input" / "step1_observations.csv"
    OUTPUT_DIR = ROOT_DIR / "testLocation" / "output" / "measurement_items"

    # Test all three modes.
    MODES_TO_TEST = [
        "distance_only",
        "distance_angle",
        "distance_angle_yaw",
    ]

    # Limit how many grouped poses get detailed item CSV rows.
    # Set to None to dump all groups.
    MAX_DETAIL_GROUPS = 10

    # For now use default weights and sigmas.
    # Later these can move to location_config.py if we want one shared solver config.
    measurement_weights = DEFAULT_MEASUREMENT_WEIGHTS
    item_sigma = DEFAULT_ITEM_SIGMA

    # -------------------------------------------------------------------------
    # Run test
    # -------------------------------------------------------------------------
    print("Running t4 measurement item test")
    print(f"ROOT_DIR   = {ROOT_DIR}")
    print(f"INPUT_CSV  = {INPUT_CSV}")
    print(f"OUTPUT_DIR = {OUTPUT_DIR}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_CSV)
    records = build_observations_from_step1_df(df)
    groups = group_observation_records(records)

    summary_rows = []
    detail_rows = []

    for mode in MODES_TO_TEST:
        mode_group_count = 0
        for group_key, group_records in groups.items():
            observations = [r["observation"] for r in group_records]

            items = build_measurement_items(
                observations=observations,
                measurement_weights=measurement_weights,
                item_sigma=item_sigma,
                mode=mode,
            )

            kind_counts = count_items_by_kind(items)
            cam_counts = count_items_by_camera_and_kind(items)

            summary_rows.append({
                "mode": mode,
                "group_key": group_key,
                "observation_count": len(observations),
                "item_total": kind_counts["total"],
                "distance_items": kind_counts["distance"],
                "angle_items": kind_counts["angle"],
                "yaw_items": kind_counts["yaw"],
                "front_total": cam_counts.get("front", {}).get("total", 0),
                "front_distance": cam_counts.get("front", {}).get("distance", 0),
                "front_angle": cam_counts.get("front", {}).get("angle", 0),
                "front_yaw": cam_counts.get("front", {}).get("yaw", 0),
                "rear_total": cam_counts.get("rear", {}).get("total", 0),
                "rear_distance": cam_counts.get("rear", {}).get("distance", 0),
                "rear_angle": cam_counts.get("rear", {}).get("angle", 0),
                "rear_yaw": cam_counts.get("rear", {}).get("yaw", 0),
            })

            if MAX_DETAIL_GROUPS is None or mode_group_count < MAX_DETAIL_GROUPS:
                detail_rows.extend(flatten_items_for_csv(group_key, group_records, mode, items))

            mode_group_count += 1

    write_csv(summary_rows, OUTPUT_DIR / "measurement_item_group_summary.csv")
    write_csv(detail_rows, OUTPUT_DIR / "measurement_item_detail_sample.csv")

    config_dump = {
        "input_csv": str(INPUT_CSV),
        "output_dir": str(OUTPUT_DIR),
        "modes_tested": MODES_TO_TEST,
        "max_detail_groups": MAX_DETAIL_GROUPS,
        "measurement_weights": measurement_weights,
        "item_sigma": item_sigma,
    }
    (OUTPUT_DIR / "measurement_item_config.json").write_text(
        json.dumps(config_dump, indent=2),
        encoding="utf-8",
    )

    # Human-readable report.
    report_lines = []
    report_lines.append("MEASUREMENT ITEM TEST SUMMARY")
    report_lines.append("=" * 80)
    report_lines.append(f"input rows: {len(df)}")
    report_lines.append(f"group count: {len(groups)}")
    report_lines.append("")

    summary_df = pd.DataFrame(summary_rows)
    for mode in MODES_TO_TEST:
        g = summary_df[summary_df["mode"] == mode]
        report_lines.append(f"[{mode}]")
        report_lines.append(f"  groups: {len(g)}")
        report_lines.append(f"  avg observations/group: {g['observation_count'].mean():.2f}")
        report_lines.append(f"  avg items/group: {g['item_total'].mean():.2f}")
        report_lines.append(f"  avg distance items/group: {g['distance_items'].mean():.2f}")
        report_lines.append(f"  avg angle items/group: {g['angle_items'].mean():.2f}")
        report_lines.append(f"  avg yaw items/group: {g['yaw_items'].mean():.2f}")
        report_lines.append("")

    (OUTPUT_DIR / "measurement_item_summary.txt").write_text(
        "\n".join(report_lines),
        encoding="utf-8",
    )

    print("\n".join(report_lines))
    print("Wrote:")
    print(f"  {OUTPUT_DIR / 'measurement_item_group_summary.csv'}")
    print(f"  {OUTPUT_DIR / 'measurement_item_detail_sample.csv'}")
    print(f"  {OUTPUT_DIR / 'measurement_item_config.json'}")
    print(f"  {OUTPUT_DIR / 'measurement_item_summary.txt'}")
