r"""
t11g_analyze_tag_measurement_errors.py

Case-study AprilTag measurement errors before solver optimization.

Purpose:
    Separate three error classes:
      1. tag-map / physical installation coordinate bias
      2. yaw branch/sign flip or AprilTag pose ambiguity
      3. physical camera edge/distortion limitation

Inputs:
    By default:
        testLocation\output\t11_console_condensed\tag_measurements.csv

    You may also copy other tag_measurements*.csv files into:
        testLocation\output\t11_case_study_input\

Outputs:
    testLocation\output\t11_tag_error_case_study\
        tag_signed_error_summary.csv
        suspicious_map_candidates.csv
        tag_map_shift_estimate.csv
        yaw_flip_cases.csv
        edge_yaw_cases.csv
        case_study_report.txt

Run:
    cd /d D:\Data\_Action\_RunNMS
    python testLocation\t11g_analyze_tag_measurement_errors.py
"""

from __future__ import annotations

import csv
import glob
import math
import os
import statistics
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Adjustable settings
# ---------------------------------------------------------------------------

# Add extra CSV files here if needed. Empty list means use default candidates.
EXTRA_INPUT_FILES: List[str] = [
    # r"D:\Data\_Action\_RunNMS\testLocation\output\t11_console_condensed_new\tag_measurements.csv",
]

# Thresholds for diagnosis.
MIN_OBS_FOR_MAP_CANDIDATE = 2
MAP_BIAS_DIST_ERR_M = 0.08
MAP_BIAS_MAX_MEAN_ABS_ANGLE_ERR_DEG = 5.0
MAP_BIAS_MAX_LS_RMSE_M = 0.05

YAW_FLIP_ABS_ERR_DEG = 45.0
YAW_EDGE_ABS_TRUE_ANGLE_DEG = 30.0
GOOD_DIST_FOR_YAW_FLIP_M = 0.15
GOOD_ANGLE_FOR_YAW_FLIP_DEG = 8.0


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
os.chdir(ROOT_DIR)

TEST_DIR = ROOT_DIR / "testLocation"
DEFAULT_INPUT = TEST_DIR / "output" / "t11_console_condensed" / "tag_measurements.csv"
OPTIONAL_INPUT_DIR = TEST_DIR / "output" / "t11_case_study_input"
OUT_DIR = TEST_DIR / "output" / "t11_tag_error_case_study"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def wrap_angle_deg(a: float) -> float:
    return float((a + 180.0) % 360.0 - 180.0)


def read_all_inputs() -> pd.DataFrame:
    files: List[Path] = []

    if DEFAULT_INPUT.exists():
        files.append(DEFAULT_INPUT)

    if OPTIONAL_INPUT_DIR.exists():
        for p in sorted(OPTIONAL_INPUT_DIR.glob("tag_measurements*.csv")):
            files.append(p)

    for s in EXTRA_INPUT_FILES:
        p = Path(s)
        if p.exists():
            files.append(p)

    # remove duplicates while preserving order
    seen = set()
    uniq = []
    for p in files:
        rp = str(p.resolve()).lower()
        if rp not in seen:
            seen.add(rp)
            uniq.append(p)

    if not uniq:
        raise FileNotFoundError(
            "No tag_measurements.csv input found. Expected at least:\n"
            f"  {DEFAULT_INPUT}\n"
            "or put files under:\n"
            f"  {OPTIONAL_INPUT_DIR}"
        )

    dfs = []
    for p in uniq:
        df = pd.read_csv(p)
        df["source_file"] = str(p)
        dfs.append(df)

    out = pd.concat(dfs, ignore_index=True)
    out.attrs["source_files_loaded"] = [str(p) for p in uniq]
    return out


def camera_pose(gt_x: float, gt_y: float, gt_h: float, camera_role: str) -> Tuple[float, float, float]:
    h_rad = math.radians(gt_h)
    if str(camera_role).lower() == "rear":
        off = -0.075
        ch = (gt_h + 180.0) % 360.0
    else:
        off = 0.055
        ch = gt_h % 360.0

    cx = gt_x + off * math.cos(h_rad)
    cy = gt_y + off * math.sin(h_rad)
    return cx, cy, ch


def add_geometry_columns(df: pd.DataFrame) -> pd.DataFrame:
    ux = []
    uy = []
    cam_x = []
    cam_y = []
    bearing_world = []

    for _, r in df.iterrows():
        cx, cy, ch = camera_pose(
            float(r["gt_x_m"]),
            float(r["gt_y_m"]),
            float(r["gt_heading_deg"]),
            str(r["camera_role"]),
        )
        # Convention used by t11/t2:
        # true_angle = camera_heading - bearing_world
        b = (ch - float(r["true_angle_deg"])) % 360.0
        ux.append(math.cos(math.radians(b)))
        uy.append(math.sin(math.radians(b)))
        cam_x.append(cx)
        cam_y.append(cy)
        bearing_world.append(b)

    out = df.copy()
    out["abs_distance_error_m"] = out["distance_error_m"].abs()
    out["abs_angle_error_deg"] = out["angle_error_deg"].abs()
    out["abs_yaw_error_deg"] = out["yaw_error_deg"].abs()
    out["camera_x_m"] = cam_x
    out["camera_y_m"] = cam_y
    out["bearing_world_deg"] = bearing_world
    out["unit_to_tag_x"] = ux
    out["unit_to_tag_y"] = uy
    out["used_bool"] = out["used"].astype(str).str.upper().eq("Y")
    return out


def pctl(vals: List[float], p: float) -> Optional[float]:
    vals = sorted([float(v) for v in vals if pd.notna(v) and math.isfinite(float(v))])
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    idx = (len(vals) - 1) * p / 100.0
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return vals[lo]
    return vals[lo] * (hi - idx) + vals[hi] * (idx - lo)


def estimate_tag_shift(g: pd.DataFrame) -> Dict[str, Any]:
    """
    Linearized coordinate correction:
        measured_distance - map_distance ~= u_i dot delta_tag

    where u_i is the unit vector from camera to the mapped tag.
    """
    if len(g) < 2:
        return {
            "shift_dx_m": "",
            "shift_dy_m": "",
            "shift_norm_m": "",
            "shift_fit_rmse_m": "",
        }

    A = g[["unit_to_tag_x", "unit_to_tag_y"]].to_numpy(dtype=float)
    b = g["distance_error_m"].to_numpy(dtype=float)

    try:
        delta, *_ = np.linalg.lstsq(A, b, rcond=None)
        pred = A @ delta
        rmse = float(np.sqrt(np.mean((pred - b) ** 2)))
        return {
            "shift_dx_m": round(float(delta[0]), 4),
            "shift_dy_m": round(float(delta[1]), 4),
            "shift_norm_m": round(float(np.linalg.norm(delta)), 4),
            "shift_fit_rmse_m": round(rmse, 4),
        }
    except Exception:
        return {
            "shift_dx_m": "",
            "shift_dy_m": "",
            "shift_norm_m": "",
            "shift_fit_rmse_m": "",
        }


def summarize_tags(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (tag_id, cam), g in df.groupby(["tag_id", "camera_role"], dropna=False):
        shift = estimate_tag_shift(g)

        rows.append({
            "tag_id": int(tag_id),
            "camera_role": cam,
            "obs_count": int(len(g)),
            "used_count": int(g["used_bool"].sum()),
            "reject_count": int((~g["used_bool"]).sum()),
            "reject_rate": round(float((~g["used_bool"]).mean()), 3),

            "mean_distance_error_m": round(float(g["distance_error_m"].mean()), 4),
            "median_distance_error_m": round(float(g["distance_error_m"].median()), 4),
            "mean_abs_distance_error_m": round(float(g["abs_distance_error_m"].mean()), 4),
            "p90_abs_distance_error_m": round(float(pctl(g["abs_distance_error_m"].tolist(), 90) or 0.0), 4),

            "mean_angle_error_deg": round(float(g["angle_error_deg"].mean()), 2),
            "mean_abs_angle_error_deg": round(float(g["abs_angle_error_deg"].mean()), 2),
            "p90_abs_angle_error_deg": round(float(pctl(g["abs_angle_error_deg"].tolist(), 90) or 0.0), 2),

            "mean_yaw_error_deg": round(float(g["yaw_error_deg"].mean()), 2),
            "mean_abs_yaw_error_deg": round(float(g["abs_yaw_error_deg"].mean()), 2),
            "p90_abs_yaw_error_deg": round(float(pctl(g["abs_yaw_error_deg"].tolist(), 90) or 0.0), 2),

            **shift,
        })

    return pd.DataFrame(rows).sort_values(["tag_id", "camera_role"])


def suspicious_map_candidates(summary: pd.DataFrame) -> pd.DataFrame:
    s = summary.copy()
    for c in ["shift_fit_rmse_m", "shift_norm_m"]:
        s[c] = pd.to_numeric(s[c], errors="coerce")

    out = s[
        (s["obs_count"] >= MIN_OBS_FOR_MAP_CANDIDATE)
        & (s["mean_distance_error_m"].abs() >= MAP_BIAS_DIST_ERR_M)
        & (s["mean_abs_angle_error_deg"] <= MAP_BIAS_MAX_MEAN_ABS_ANGLE_ERR_DEG)
        & (s["shift_fit_rmse_m"] <= MAP_BIAS_MAX_LS_RMSE_M)
    ].copy()

    out["diagnosis"] = (
        "consistent signed distance bias with small angle error; "
        "likely tag-map / physical installation coordinate bias"
    )

    return out.sort_values(["mean_abs_distance_error_m"], ascending=False)


def yaw_flip_cases(df: pd.DataFrame) -> pd.DataFrame:
    out = df[
        (df["abs_yaw_error_deg"] >= YAW_FLIP_ABS_ERR_DEG)
        & (df["abs_distance_error_m"] <= GOOD_DIST_FOR_YAW_FLIP_M)
        & (df["abs_angle_error_deg"] <= GOOD_ANGLE_FOR_YAW_FLIP_DEG)
    ].copy()

    out["diagnosis"] = (
        "yaw branch/sign flip or AprilTag pose ambiguity; distance/angle look usable"
    )

    cols = [
        "source_file", "sample_index", "gt_x_m", "gt_y_m", "gt_heading_deg",
        "tag_id", "camera_role", "used", "meas_distance_m", "true_distance_m",
        "distance_error_m", "meas_angle_deg", "true_angle_deg", "angle_error_deg",
        "meas_yaw_deg", "true_yaw_deg", "yaw_error_deg", "diagnosis",
    ]
    out = out.sort_values(["abs_yaw_error_deg"], ascending=False)
    return out[cols]


def edge_yaw_cases(df: pd.DataFrame) -> pd.DataFrame:
    out = df[
        (df["abs_yaw_error_deg"] >= 25.0)
        & (df["true_angle_deg"].abs() >= YAW_EDGE_ABS_TRUE_ANGLE_DEG)
    ].copy()

    out["diagnosis"] = (
        "yaw error at large camera view angle; likely physical edge/pose limitation"
    )

    cols = [
        "source_file", "sample_index", "gt_x_m", "gt_y_m", "gt_heading_deg",
        "tag_id", "camera_role", "used", "meas_distance_m", "true_distance_m",
        "distance_error_m", "meas_angle_deg", "true_angle_deg", "angle_error_deg",
        "meas_yaw_deg", "true_yaw_deg", "yaw_error_deg", "diagnosis",
    ]
    out = out.sort_values(["abs_yaw_error_deg"], ascending=False)
    return out[cols]


def write_report(df: pd.DataFrame, summary: pd.DataFrame, map_cand: pd.DataFrame, yaw_flip: pd.DataFrame, edge_yaw: pd.DataFrame) -> None:
    path = OUT_DIR / "case_study_report.txt"

    with path.open("w", encoding="utf-8") as f:
        f.write("T11G AprilTag measurement error case study\n")
        f.write("=" * 72 + "\n\n")
        f.write(f"Total observations: {len(df)}\n")
        f.write(f"Unique tag/camera groups: {len(summary)}\n\n")

        f.write("Most likely tag-map / physical-installation candidates\n")
        f.write("-" * 72 + "\n")
        if map_cand.empty:
            f.write("(none by current thresholds)\n")
        else:
            for _, r in map_cand.head(20).iterrows():
                f.write(
                    f"Tag {int(r['tag_id'])} {r['camera_role']}: "
                    f"n={int(r['obs_count'])}, "
                    f"mean_dist_err={r['mean_distance_error_m']} m, "
                    f"mean_abs_angle_err={r['mean_abs_angle_error_deg']} deg, "
                    f"shift≈({r['shift_dx_m']}, {r['shift_dy_m']}) m, "
                    f"fit_rmse={r['shift_fit_rmse_m']} m\n"
                )
        f.write("\n")

        f.write("Yaw flip / yaw ambiguity cases\n")
        f.write("-" * 72 + "\n")
        if yaw_flip.empty:
            f.write("(none by current thresholds)\n")
        else:
            for _, r in yaw_flip.head(20).iterrows():
                f.write(
                    f"sample={r['sample_index']} point=({r['gt_x_m']},{r['gt_y_m']}) "
                    f"h={r['gt_heading_deg']} tag={int(r['tag_id'])} {r['camera_role']} used={r['used']} "
                    f"DErr={r['distance_error_m']} AErr={r['angle_error_deg']} "
                    f"YErr={r['yaw_error_deg']}\n"
                )
        f.write("\n")

        f.write("Edge yaw cases\n")
        f.write("-" * 72 + "\n")
        if edge_yaw.empty:
            f.write("(none by current thresholds)\n")
        else:
            for _, r in edge_yaw.head(20).iterrows():
                f.write(
                    f"sample={r['sample_index']} point=({r['gt_x_m']},{r['gt_y_m']}) "
                    f"h={r['gt_heading_deg']} tag={int(r['tag_id'])} {r['camera_role']} used={r['used']} "
                    f"true_angle={r['true_angle_deg']} YErr={r['yaw_error_deg']}\n"
                )


def main() -> None:
    raw = read_all_inputs()
    source_files_loaded = raw.attrs.get("source_files_loaded", [])
    df = add_geometry_columns(raw)
    summary = summarize_tags(df)
    map_cand = suspicious_map_candidates(summary)
    yaw_flip = yaw_flip_cases(df)
    edge_yaw = edge_yaw_cases(df)

    summary.to_csv(OUT_DIR / "tag_signed_error_summary.csv", index=False)
    map_cand.to_csv(OUT_DIR / "suspicious_map_candidates.csv", index=False)
    summary[[
        "tag_id", "camera_role", "obs_count", "mean_distance_error_m",
        "mean_abs_angle_error_deg", "shift_dx_m", "shift_dy_m",
        "shift_norm_m", "shift_fit_rmse_m"
    ]].to_csv(OUT_DIR / "tag_map_shift_estimate.csv", index=False)
    yaw_flip.to_csv(OUT_DIR / "yaw_flip_cases.csv", index=False)
    edge_yaw.to_csv(OUT_DIR / "edge_yaw_cases.csv", index=False)
    write_report(df, summary, map_cand, yaw_flip, edge_yaw)

    print("=" * 72)
    print("T11G AprilTag measurement error case study")
    print("=" * 72)
    print(f"Observations loaded: {len(df)}")
    print("Source files:")
    for p in source_files_loaded:
        print(f"  {p}")
    print(f"Output dir         : {OUT_DIR}")
    print("")
    print("Top map/installation candidates:")
    if map_cand.empty:
        print("  (none by current thresholds)")
    else:
        for _, r in map_cand.head(12).iterrows():
            print(
                f"  Tag {int(r['tag_id']):>2} {r['camera_role']:<5} "
                f"n={int(r['obs_count']):>2} "
                f"mean_DErr={r['mean_distance_error_m']:>7} m "
                f"mean_abs_AErr={r['mean_abs_angle_error_deg']:>5} deg "
                f"shift=({r['shift_dx_m']}, {r['shift_dy_m']}) m "
                f"rmse={r['shift_fit_rmse_m']} m"
            )

    print("")
    print(f"Yaw flip / ambiguity cases: {len(yaw_flip)}")
    print(f"Edge yaw cases           : {len(edge_yaw)}")
    print("")
    print("Files written:")
    for name in [
        "tag_signed_error_summary.csv",
        "suspicious_map_candidates.csv",
        "tag_map_shift_estimate.csv",
        "yaw_flip_cases.csv",
        "edge_yaw_cases.csv",
        "case_study_report.txt",
    ]:
        print(f"  {OUT_DIR / name}")


if __name__ == "__main__":
    main()
