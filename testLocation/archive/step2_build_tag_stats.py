#!/usr/bin/env python3
"""
Step 2: build per-tag/per-camera residual statistics from step1_observations.csv.

Run:
    python step2_build_tag_stats.py

Inputs:
    step1_observations.csv

Outputs:
    step2_tag_camera_metric_stats.csv
    step2_tag_camera_diagnosis.csv
    step2_tag_summary.csv
    step2_camera_summary.csv
    step2_report.txt
"""
from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parent
df = pd.read_csv(BASE / "step1_observations.csv")
df["tag_id"] = df["tag_id"].astype(int)
df["camera_role"] = df["camera_role"].astype(str).str.lower()

metrics = [
    "distance_error_cm",
    "angle_error_deg",
    "yaw_error_deg",
    "single_pos_err_cm",
    "single_heading_err_deg",
]

def robust_stats(series):
    x = pd.to_numeric(series, errors="coerce").dropna().astype(float).to_numpy()
    n = len(x)
    if n == 0:
        return dict(count=0, mean_raw=np.nan, median=np.nan, std_raw=np.nan,
                    mad=np.nan, robust_sigma=np.nan, outlier_count=0,
                    mean_clean=np.nan, std_clean=np.nan, min_clean=np.nan, max_clean=np.nan)
    med = float(np.median(x))
    mad = float(np.median(np.abs(x - med)))
    robust_sigma = 1.4826 * mad
    raw_std = float(np.std(x, ddof=1)) if n > 1 else 0.0
    sigma = raw_std if robust_sigma < 1e-9 else robust_sigma
    if sigma < 1e-9:
        clean = x
        outliers = 0
    else:
        mask = np.abs(x - med) <= 3.0 * sigma
        clean = x[mask]
        outliers = int((~mask).sum())
    return dict(
        count=int(n),
        mean_raw=float(np.mean(x)),
        median=med,
        std_raw=raw_std,
        mad=mad,
        robust_sigma=float(robust_sigma),
        outlier_count=outliers,
        mean_clean=float(np.mean(clean)) if len(clean) else np.nan,
        std_clean=float(np.std(clean, ddof=1)) if len(clean) > 1 else 0.0,
        min_clean=float(np.min(clean)) if len(clean) else np.nan,
        max_clean=float(np.max(clean)) if len(clean) else np.nan,
    )

long_rows = []
for (tag, cam), g in df.groupby(["tag_id", "camera_role"], dropna=False):
    for metric in metrics:
        long_rows.append({"tag_id": tag, "camera_role": cam, "metric": metric, **robust_stats(g[metric])})
stats_long = pd.DataFrame(long_rows).sort_values(["tag_id", "camera_role", "metric"])

diag_rows = []
for (tag, cam), g in df.groupby(["tag_id", "camera_role"], dropna=False):
    row = {
        "tag_id": tag,
        "camera_role": cam,
        "obs_count": len(g),
        "datasets": ",".join(sorted(map(str, g["dataset"].dropna().unique()))),
        "gt_pose_count": g[["dataset", "gt_x", "gt_y", "gt_h"]].drop_duplicates().shape[0],
    }
    for metric in metrics:
        st = robust_stats(g[metric])
        prefix = metric.replace("_error", "").replace("_cm", "").replace("_deg", "")
        row[f"{prefix}_mean_clean"] = st["mean_clean"]
        row[f"{prefix}_median"] = st["median"]
        row[f"{prefix}_std_clean"] = st["std_clean"]
        row[f"{prefix}_mad"] = st["mad"]
        row[f"{prefix}_outliers"] = st["outlier_count"]

    notes = []
    if len(g) < 3:
        notes.append("LOW_COUNT")

    checks = {
        "dist_bias": abs(row["distance_mean_clean"]),
        "dist_std": row["distance_std_clean"],
        "angle_bias": abs(row["angle_mean_clean"]),
        "angle_std": row["angle_std_clean"],
        "yaw_bias": abs(row["yaw_mean_clean"]),
        "yaw_std": row["yaw_std_clean"],
        "single_pos_med": row["single_pos_err_median"],
        "single_h_med": row["single_heading_err_median"],
    }

    if pd.notna(checks["dist_bias"]) and pd.notna(checks["dist_std"]) and checks["dist_bias"] > 8 and checks["dist_std"] <= 5:
        notes.append("DIST_STEADY_BIAS")
    if pd.notna(checks["angle_bias"]) and pd.notna(checks["angle_std"]) and checks["angle_bias"] > 4 and checks["angle_std"] <= 3:
        notes.append("ANGLE_STEADY_BIAS")
    if pd.notna(checks["yaw_bias"]) and pd.notna(checks["yaw_std"]) and checks["yaw_bias"] > 8 and checks["yaw_std"] <= 5:
        notes.append("YAW_STEADY_BIAS")
    if pd.notna(checks["dist_std"]) and checks["dist_std"] > 10:
        notes.append("DIST_UNSTABLE")
    if pd.notna(checks["angle_std"]) and checks["angle_std"] > 6:
        notes.append("ANGLE_UNSTABLE")
    if pd.notna(checks["yaw_std"]) and checks["yaw_std"] > 12:
        notes.append("YAW_UNSTABLE")
    if pd.notna(checks["single_pos_med"]) and checks["single_pos_med"] > 50:
        notes.append("SINGLE_POSE_BAD_POS")
    if pd.notna(checks["single_h_med"]) and checks["single_h_med"] > 10:
        notes.append("SINGLE_POSE_BAD_HEADING")

    if not notes:
        notes.append("OK_FIRST_PASS")

    row["diagnosis_flags"] = "|".join(notes)

    if any(n in notes for n in ["DIST_UNSTABLE", "ANGLE_UNSTABLE", "DIST_STEADY_BIAS", "ANGLE_STEADY_BIAS"]):
        row["suggested_use"] = "WEAK_OR_INSPECT"
    elif any(n in notes for n in ["YAW_UNSTABLE", "YAW_STEADY_BIAS", "SINGLE_POSE_BAD_HEADING"]):
        row["suggested_use"] = "USE_DIST_ANGLE_WEAK_YAW"
    else:
        row["suggested_use"] = "TRUSTED_FIRST_PASS"

    diag_rows.append(row)

diag = pd.DataFrame(diag_rows).sort_values(["tag_id", "camera_role"])

def grouped_summary(group_cols):
    rows = []
    for keys, g in df.groupby(group_cols):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        row["obs_count"] = len(g)
        row["unique_tags"] = g["tag_id"].nunique()
        row["gt_pose_count"] = g[["dataset", "gt_x", "gt_y", "gt_h"]].drop_duplicates().shape[0]
        for metric in metrics:
            st = robust_stats(g[metric])
            prefix = metric.replace("_error", "").replace("_cm", "").replace("_deg", "")
            row[f"{prefix}_mean_clean"] = st["mean_clean"]
            row[f"{prefix}_median"] = st["median"]
            row[f"{prefix}_std_clean"] = st["std_clean"]
            row[f"{prefix}_outliers"] = st["outlier_count"]
        rows.append(row)
    return pd.DataFrame(rows)

tag_summary = grouped_summary(["tag_id"]).sort_values("tag_id")
camera_summary = grouped_summary(["camera_role"]).sort_values("camera_role")

stats_long.to_csv(BASE / "step2_tag_camera_metric_stats.csv", index=False)
diag.to_csv(BASE / "step2_tag_camera_diagnosis.csv", index=False)
tag_summary.to_csv(BASE / "step2_tag_summary.csv", index=False)
camera_summary.to_csv(BASE / "step2_camera_summary.csv", index=False)

lines = []
lines.append("STEP 2 TAG/CAMERA RESIDUAL STATISTICS")
lines.append("=" * 72)
lines.append(f"Input rows: {len(df)}")
lines.append(f"Unique tags: {df['tag_id'].nunique()}")
lines.append(f"Tag/camera groups: {diag.shape[0]}")
lines.append("")
lines.append("Camera-level summary:")
lines.append(camera_summary.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
lines.append("")
lines.append("Suggested-use counts:")
lines.append(diag["suggested_use"].value_counts().to_string())
lines.append("")

cols = ["tag_id", "camera_role", "obs_count", "single_pos_err_median", "single_heading_err_median",
        "distance_mean_clean", "angle_mean_clean", "yaw_mean_clean", "diagnosis_flags", "suggested_use"]
lines.append("Top 15 worst tag/camera groups by median single-tag position error:")
lines.append(diag.sort_values("single_pos_err_median", ascending=False).head(15)[cols].to_string(index=False, float_format=lambda x: f"{x:.3f}"))

cols2 = ["tag_id", "camera_role", "obs_count", "yaw_mean_clean", "yaw_std_clean", "yaw_outliers",
         "single_heading_err_median", "diagnosis_flags", "suggested_use"]
lines.append("")
lines.append("Top 15 yaw-unstable tag/camera groups:")
lines.append(diag.sort_values("yaw_std_clean", ascending=False).head(15)[cols2].to_string(index=False, float_format=lambda x: f"{x:.3f}"))

cols3 = ["tag_id", "camera_role", "obs_count", "distance_mean_clean", "distance_std_clean",
         "distance_outliers", "single_pos_err_median", "diagnosis_flags", "suggested_use"]
lines.append("")
lines.append("Top 15 distance-unstable tag/camera groups:")
lines.append(diag.sort_values("distance_std_clean", ascending=False).head(15)[cols3].to_string(index=False, float_format=lambda x: f"{x:.3f}"))

cols4 = ["tag_id", "camera_role", "obs_count", "angle_mean_clean", "angle_std_clean",
         "angle_outliers", "single_pos_err_median", "diagnosis_flags", "suggested_use"]
lines.append("")
lines.append("Top 15 angle-unstable tag/camera groups:")
lines.append(diag.sort_values("angle_std_clean", ascending=False).head(15)[cols4].to_string(index=False, float_format=lambda x: f"{x:.3f}"))

(BASE / "step2_report.txt").write_text("\n".join(lines), encoding="utf-8")
print(script_path)
