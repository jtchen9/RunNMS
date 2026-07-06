from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from src.common.tag_map_loader import load_tag_xy_map
from src.solvers.solver_2_distance_angle import (
    DistanceAngleSolverConfig,
    solve_distance_angle,
)


# ---------------------------------------------------------------------------
# Observable-only collection gates.
# These gates decide KEEP / RETAKE.
# They intentionally do NOT use:
#   - true_x / true_y / true_heading
#   - true_distance / true_angle / true_yaw
#   - distance_error / angle_error / yaw_error
#   - Candidate v1 error against ground truth
# ---------------------------------------------------------------------------

FRONT_MAX_DISTANCE_M = 5.5
REAR_MAX_DISTANCE_M = 3.0

FRONT_MAX_ABS_ANGLE_DEG = 30.0
REAR_MAX_ABS_ANGLE_DEG = 15.0

MIN_USABLE_OBSERVATIONS = 4
PREFERRED_USABLE_OBSERVATIONS = 5
MIN_UNIQUE_TAGS = 4

# Near-edge is only a warning, not an automatic row rejection beyond the
# established hard angle gates above.
FRONT_NEAR_EDGE_DEG = 25.0
REAR_NEAR_EDGE_DEG = 12.0
MAX_NEAR_EDGE_FRACTION_FOR_CLEAN_SAMPLE = 0.50


def _pick_latest_csv(folder: Path) -> Path:
    candidates = sorted(
        folder.glob("obscheck_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No obscheck_*.csv found in {folder}"
        )
    return candidates[0]


def _measurement_columns(df: pd.DataFrame) -> Tuple[str, str]:
    distance_candidates = [
        "meas_distance_m",
        "cal_distance_m",
    ]
    angle_candidates = [
        "meas_angle_deg",
        "cal_angle_deg",
    ]

    dcol = next(
        (c for c in distance_candidates if c in df.columns),
        None,
    )
    acol = next(
        (c for c in angle_candidates if c in df.columns),
        None,
    )

    if dcol is None or acol is None:
        raise ValueError(
            "CSV must contain measured/calibrated distance and angle columns. "
            f"Found columns: {list(df.columns)}"
        )

    return dcol, acol


def _row_gate(
    row: pd.Series,
    distance_col: str,
    angle_col: str,
) -> Tuple[bool, str, bool]:
    try:
        tag_id = int(row["tag_id"])
        camera_role = str(row["camera_role"]).strip().lower()
        d = float(row[distance_col])
        a = float(row[angle_col])
    except Exception:
        return False, "invalid_required_value", False

    if tag_id <= 0:
        return False, "invalid_tag_id", False

    if camera_role not in {"front", "rear"}:
        return False, "unknown_camera_role", False

    if not math.isfinite(d) or d <= 0.0:
        return False, "invalid_distance", False

    if not math.isfinite(a):
        return False, "invalid_angle", False

    if camera_role == "front":
        if d > FRONT_MAX_DISTANCE_M:
            return False, "front_distance_gt_5.5m", False
        if abs(a) > FRONT_MAX_ABS_ANGLE_DEG:
            return False, "front_abs_angle_gt_30deg", False
        near_edge = abs(a) > FRONT_NEAR_EDGE_DEG
    else:
        if d > REAR_MAX_DISTANCE_M:
            return False, "rear_distance_gt_3.0m", False
        if abs(a) > REAR_MAX_ABS_ANGLE_DEG:
            return False, "rear_abs_angle_gt_15deg", False
        near_edge = abs(a) > REAR_NEAR_EDGE_DEG

    return True, "usable", near_edge


def _build_solver2_sample(
    usable_df: pd.DataFrame,
    distance_col: str,
    angle_col: str,
    sample_uid: str,
) -> Dict[str, Any]:
    observations: List[Dict[str, Any]] = []

    for idx, row in usable_df.iterrows():
        observations.append({
            "observation_uid": f"{sample_uid}_obs_{idx}",
            "tag_id": int(row["tag_id"]),
            "camera_role": str(row["camera_role"]).strip().lower(),
            "measured": {
                "distance_m": float(row[distance_col]),
                "angle_deg": float(row[angle_col]),
            },
            "weights": {
                "distance": 1.0,
                "angle": 1.0,
            },
            "flags": {},
        })

    return {
        "sample_uid": sample_uid,
        "observations": observations,
        "observation_count": len(observations),
    }


def check_measurement(
    csv_path: Path,
    tag_map_path: Path,
) -> None:
    df = pd.read_csv(csv_path)

    required = {"tag_id", "camera_role"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )

    distance_col, angle_col = _measurement_columns(df)

    gate_rows = []
    for idx, row in df.iterrows():
        usable, reason, near_edge = _row_gate(
            row,
            distance_col,
            angle_col,
        )
        gate_rows.append({
            "row_index": idx,
            "usable": usable,
            "reason": reason,
            "near_edge": near_edge,
        })

    gate_df = pd.DataFrame(gate_rows)
    work = df.copy()
    work["_usable"] = gate_df["usable"]
    work["_reason"] = gate_df["reason"]
    work["_near_edge"] = gate_df["near_edge"]

    usable_df = work[work["_usable"]].copy()

    usable_count = len(usable_df)
    unique_tags = usable_df["tag_id"].nunique()
    front_count = int(
        (usable_df["camera_role"].astype(str).str.lower() == "front").sum()
    )
    rear_count = int(
        (usable_df["camera_role"].astype(str).str.lower() == "rear").sum()
    )

    duplicate_pairs = int(
        usable_df.duplicated(
            subset=["tag_id", "camera_role"],
            keep=False,
        ).sum()
    )

    near_edge_count = int(usable_df["_near_edge"].sum())
    near_edge_fraction = (
        near_edge_count / usable_count
        if usable_count > 0 else 0.0
    )

    reasons = (
        work.loc[~work["_usable"], "_reason"]
        .value_counts()
        .to_dict()
    )

    # KEEP / RETAKE decision uses observable acquisition integrity only.
    keep_reasons: List[str] = []
    caution_reasons: List[str] = []

    if usable_count < MIN_USABLE_OBSERVATIONS:
        keep_reasons.append(
            f"usable observations {usable_count} < "
            f"{MIN_USABLE_OBSERVATIONS}"
        )

    if unique_tags < MIN_UNIQUE_TAGS:
        keep_reasons.append(
            f"unique tags {unique_tags} < {MIN_UNIQUE_TAGS}"
        )

    if duplicate_pairs > 0:
        caution_reasons.append(
            f"{duplicate_pairs} usable rows belong to duplicate "
            "(tag_id, camera_role) pairs"
        )

    if usable_count < PREFERRED_USABLE_OBSERVATIONS:
        caution_reasons.append(
            f"only {usable_count} usable observations; "
            f"{PREFERRED_USABLE_OBSERVATIONS}+ preferred"
        )

    if (
        usable_count > 0
        and near_edge_fraction
        > MAX_NEAR_EDGE_FRACTION_FOR_CLEAN_SAMPLE
    ):
        caution_reasons.append(
            f"near-edge fraction {near_edge_fraction:.0%} > 50%"
        )

    collection_decision = (
        "RETAKE"
        if keep_reasons
        else ("KEEP_WITH_CAUTION" if caution_reasons else "KEEP")
    )

    # Diagnostic pose estimate:
    # Solver #2 uses distance + view angle only and no GT.
    # It is printed for field sanity checking but does NOT affect KEEP/RETAKE.
    estimate_text = "not available"
    solver_result = None

    if usable_count >= 2:
        tag_xy_map = load_tag_xy_map(tag_map_path)

        sample = _build_solver2_sample(
            usable_df=usable_df,
            distance_col=distance_col,
            angle_col=angle_col,
            sample_uid=csv_path.stem,
        )

        config = DistanceAngleSolverConfig(
            distance_sigma_m=0.05,
            angle_sigma_deg=2.5,
            distance_global_weight=1.0,
            angle_global_weight=1.0,
        )

        solver_result = solve_distance_angle(
            sample=sample,
            tag_xy_map=tag_xy_map,
            config=config,
        )

        if solver_result.success:
            estimate_text = (
                f"x={solver_result.estimated_x_m:.3f} m, "
                f"y={solver_result.estimated_y_m:.3f} m, "
                f"h={solver_result.estimated_heading_deg % 360.0:.1f} deg"
            )
        else:
            estimate_text = (
                f"FAILED ({solver_result.failure_reason})"
            )

    print("")
    print("=" * 72)
    print("Validation Measurement Quick Check")
    print("=" * 72)
    print(f"CSV              : {csv_path}")
    print(f"Rows read        : {len(df)}")
    print(f"Usable obs       : {usable_count}")
    print(f"Unique tags      : {unique_tags}")
    print(f"Front / rear     : {front_count} / {rear_count}")
    print(
        f"Near-edge usable : {near_edge_count} "
        f"({near_edge_fraction:.0%})"
    )

    if reasons:
        print(f"Rows gated out   : {sum(reasons.values())}")
        print(
            "Gate reasons     : "
            + ", ".join(f"{k}={v}" for k, v in reasons.items())
        )
    else:
        print("Rows gated out   : 0")

    print("")
    print(f"QUICK POSE       : {estimate_text}")
    print(
        "NOTE             : pose estimate is diagnostic only; "
        "it does not decide KEEP/RETAKE"
    )
    print("")

    print(f"COLLECTION       : {collection_decision}")

    if keep_reasons:
        print(
            "Why retake       : "
            + "; ".join(keep_reasons)
        )

    if caution_reasons:
        print(
            "Cautions         : "
            + "; ".join(caution_reasons)
        )

    if collection_decision == "KEEP":
        print(
            "Conclusion       : acquisition looks suitable for "
            "independent validation."
        )
    elif collection_decision == "KEEP_WITH_CAUTION":
        print(
            "Conclusion       : keep the sample, but record the cautions; "
            "do not retune Candidate v1 from this result."
        )
    else:
        print(
            "Conclusion       : acquisition coverage is insufficient; "
            "retake before adding to validation."
        )

    print("=" * 72)
    print("")


if __name__ == "__main__":
    # Option A: set an exact file.
    INPUT_CSV = None
    # Example:
    # INPUT_CSV = Path(
    #     r"D:\Data\_Action\_RunNMS\locationSolver\data\validation_raw"
    #     r"\obscheck_x2.50_y1.50_h135_YYYYMMDD_HHMMSS.csv"
    # )

    # Option B: with INPUT_CSV=None, use latest obscheck_*.csv here.
    INPUT_DIR = (
        ROOT_DIR
        / "data"
        / "validation_raw"
    )

    TAG_MAP_PATH = (
        ROOT_DIR.parent
        / "sitemap"
        / "DemoRoom"
        / "tag_location.txt"
    )

    csv_path = (
        Path(INPUT_CSV)
        if INPUT_CSV is not None
        else _pick_latest_csv(INPUT_DIR)
    )

    check_measurement(
        csv_path=csv_path,
        tag_map_path=TAG_MAP_PATH,
    )
