from __future__ import annotations

"""
Step 21: raw -> loose tag-wise first screen -> unchanged Solver #2 Pass 1
         -> signed component residual dataset for Layer-2 study.

This script is diagnostic/research code only.
It does NOT modify Solver #2, Solver #3, Solver #5, or production state machine.

First-screen rule (whole-tag deletion):
    reject if:
        camera_role == "front"
        AND abs(measured angle) > 30 deg
    OR
        camera_role == "front"
        AND measured distance > 5.5 m

No rear-distance rule is applied.

Pass 1:
    unchanged src.solvers.solver_2_distance_angle.solve_distance_angle
    with the original Solver #2 default config.

Outputs include signed D and A residuals from the rough no-yaw pose so the
second-screen algorithm can be studied from new data.
"""

import csv
import math
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.dataset_builder import build_prepared_samples
from src.common.evaluator import evaluate_solver_results
from src.common.geometry import (
    predicted_tag_angle_deg,
    predicted_tag_distance_m,
    wrap_angle_deg,
)
from src.common.run_manifest import make_run_id, write_manifest
from src.common.run_output import write_json, write_rows_csv
from src.common.tag_map_loader import load_tag_xy_map
from src.solvers.solver_2_distance_angle import (
    DistanceAngleSolverConfig,
    solve_distance_angle,
)


# ---------------------------------------------------------------------------
# Frozen Step-21 first-screen rules
# ---------------------------------------------------------------------------

FRONT_MAX_ABS_ANGLE_DEG = 30.0
FRONT_MAX_DISTANCE_M = 5.5


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        x = float(value)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fields: List[str] = []
    seen = set()

    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fields.append(key)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fields,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def _first_screen_reason(row: Dict[str, str]) -> str:
    role = str(row.get("camera_role") or "").strip().lower()
    d = _safe_float(row.get("meas_distance_m"))
    a = _safe_float(row.get("meas_angle_deg"))

    # Invalid D/A cannot be passed to Solver #2 anyway.
    # Keep this reason explicit rather than silently dropping later.
    if d is None or d <= 0.0:
        return "invalid_distance"
    if a is None:
        return "invalid_angle"

    if role == "front" and abs(a) > FRONT_MAX_ABS_ANGLE_DEG:
        return "front_edge_abs_angle_gt30"

    if role == "front" and d > FRONT_MAX_DISTANCE_M:
        return "front_too_far_gt5p5"

    return ""


def _screen_raw_rows(
    raw_rows: List[Dict[str, str]],
) -> Tuple[
    List[Dict[str, str]],
    List[Dict[str, Any]],
]:
    kept: List[Dict[str, str]] = []
    audit: List[Dict[str, Any]] = []

    for row in raw_rows:
        reason = _first_screen_reason(row)
        rejected = bool(reason)

        audit_row: Dict[str, Any] = dict(row)
        audit_row["step21_first_screen_rejected"] = rejected
        audit_row["step21_first_screen_reason"] = reason or "pass"
        audit.append(audit_row)

        if not rejected:
            kept.append(dict(row))

    return kept, audit


def _truth_lookup(
    raw_rows: List[Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}

    for row in raw_rows:
        uid = str(row.get("observation_uid") or "").strip()
        if uid:
            out[uid] = row

    return out


def _pass1_residual_rows(
    sample: Dict[str, Any],
    result,
    tag_xy_map: Dict[int, Tuple[float, float]],
    config: DistanceAngleSolverConfig,
    truth_by_uid: Dict[str, Dict[str, str]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    if not result.success:
        return rows

    x = float(result.estimated_x_m)
    y = float(result.estimated_y_m)
    h = float(result.estimated_heading_deg)

    for obs in list(sample.get("observations") or []):
        tag_id = int(obs["tag_id"])
        if tag_id not in tag_xy_map:
            continue

        measured = obs.get("measured") or {}
        weights = obs.get("weights") or {}

        d = _safe_float(measured.get("distance_m"))
        a = _safe_float(measured.get("angle_deg"))

        if d is None or d <= 0.0 or a is None:
            continue

        wd = float(weights.get("distance", 1.0))
        wa = float(weights.get("angle", 1.0))

        if wd <= 0.0 or wa <= 0.0:
            continue

        tag_x, tag_y = tag_xy_map[tag_id]

        pred_d = predicted_tag_distance_m(
            x,
            y,
            h,
            obs["camera_role"],
            tag_x,
            tag_y,
        )

        pred_a = predicted_tag_angle_deg(
            x,
            y,
            h,
            obs["camera_role"],
            tag_x,
            tag_y,
        )

        # Keep signs.
        rd_m = pred_d - d
        ra_deg = wrap_angle_deg(pred_a - a)

        nd = (
            math.sqrt(config.distance_global_weight * wd)
            * rd_m
            / config.distance_sigma_m
        )
        na = (
            math.sqrt(config.angle_global_weight * wa)
            * ra_deg
            / config.angle_sigma_deg
        )

        uid = str(obs.get("observation_uid") or "")
        truth = truth_by_uid.get(uid, {})

        rows.append({
            "sample_uid": str(sample["sample_uid"]),
            "observation_uid": uid,
            "tag_id": tag_id,
            "camera_role": str(obs.get("camera_role") or ""),

            "pass1_estimated_x_m": x,
            "pass1_estimated_y_m": y,
            "pass1_estimated_heading_deg": h,

            "measured_distance_m": d,
            "predicted_distance_m": pred_d,
            "distance_residual_m_signed": rd_m,
            "distance_residual_sigma_signed": nd,
            "distance_residual_sigma_abs": abs(nd),

            "measured_angle_deg": a,
            "predicted_angle_deg": pred_a,
            "angle_residual_deg_signed": ra_deg,
            "angle_residual_sigma_signed": na,
            "angle_residual_sigma_abs": abs(na),

            # GT attached post-hoc only for forensic Layer-2 study.
            "gt_distance_error_m": _safe_float(
                truth.get("distance_error_m")
            ),
            "gt_angle_error_deg": _safe_float(
                truth.get("angle_error_deg")
            ),
            "gt_yaw_error_deg": _safe_float(
                truth.get("yaw_error_deg")
            ),

            # Preserve raw yaw for later GT-free keep/flip study.
            "measured_yaw_deg": _safe_float(
                truth.get("meas_yaw_deg")
            ),
        })

    return rows


def run_frontend(
    raw_csv_path: Path,
    tag_map_path: Path,
) -> None:
    raw_rows = _read_csv(raw_csv_path)
    truth_by_uid = _truth_lookup(raw_rows)

    screened_rows, audit_rows = _screen_raw_rows(raw_rows)

    # build_prepared_samples performs no new screening.
    # It only converts the surviving long-format rows into sample objects
    # expected by Solver #2.
    samples = build_prepared_samples(screened_rows)

    tag_xy_map = load_tag_xy_map(tag_map_path)

    # Original Solver #2 default configuration.
    config = DistanceAngleSolverConfig()

    results = []
    residual_rows_all: List[Dict[str, Any]] = []

    for sample in samples:
        result = solve_distance_angle(
            sample=sample,
            tag_xy_map=tag_xy_map,
            config=config,
        )

        results.append(result)

        residual_rows_all.extend(
            _pass1_residual_rows(
                sample=sample,
                result=result,
                tag_xy_map=tag_xy_map,
                config=config,
                truth_by_uid=truth_by_uid,
            )
        )

    sample_rows, summary = evaluate_solver_results(
        samples,
        results,
    )

    run_id = make_run_id(
        "step21_rawscreen_pass1",
        "raw242",
    )
    out_dir = ROOT_DIR / "output" / "diagnostics" / run_id
    out_dir.mkdir(parents=True, exist_ok=False)

    kept_uid_set = {
        str(row.get("observation_uid") or "")
        for row in screened_rows
    }

    removed_rows = [
        row for row in audit_rows
        if bool(row.get("step21_first_screen_rejected"))
    ]

    kept_audit_rows = [
        row for row in audit_rows
        if str(row.get("observation_uid") or "") in kept_uid_set
    ]

    _write_csv(
        out_dir / "first_screen_all_242_audit.csv",
        audit_rows,
    )
    _write_csv(
        out_dir / "first_screen_rejected_rows.csv",
        removed_rows,
    )
    _write_csv(
        out_dir / "first_screen_surviving_rows.csv",
        kept_audit_rows,
    )

    write_rows_csv(
        out_dir / "pass1_sample_results.csv",
        sample_rows,
    )

    _write_csv(
        out_dir / "pass1_signed_component_residuals.csv",
        residual_rows_all,
    )

    # Convenience rankings for Layer-2 investigation.
    ranked_d = sorted(
        residual_rows_all,
        key=lambda r: float(
            r.get("distance_residual_sigma_abs") or 0.0
        ),
        reverse=True,
    )
    ranked_a = sorted(
        residual_rows_all,
        key=lambda r: float(
            r.get("angle_residual_sigma_abs") or 0.0
        ),
        reverse=True,
    )

    _write_csv(
        out_dir / "pass1_ranked_distance_residuals.csv",
        ranked_d,
    )
    _write_csv(
        out_dir / "pass1_ranked_angle_residuals.csv",
        ranked_a,
    )

    write_json(
        out_dir / "summary.json",
        {
            "raw_observation_count": len(raw_rows),
            "first_screen_survivor_count": len(screened_rows),
            "first_screen_rejected_count": len(removed_rows),
            "prepared_sample_count": len(samples),
            "pass1_success_count": sum(
                1 for r in results if r.success
            ),
            "pass1_failure_count": sum(
                1 for r in results if not r.success
            ),
            "pass1_residual_row_count": len(residual_rows_all),
            "pass1_evaluation_summary": summary,
        },
    )

    write_json(
        out_dir / "run_config.json",
        {
            "raw_csv_path": str(raw_csv_path),
            "tag_map_path": str(tag_map_path),
            "first_screen": {
                "whole_tag_rejection": True,
                "rules": [
                    "front AND abs(meas_angle_deg) > 30",
                    "front AND meas_distance_m > 5.5",
                ],
                "rear_distance_rule": None,
            },
            "pass1_solver": {
                "name": "solver_2_distance_angle",
                "implementation_modified": False,
                "config": asdict(config),
            },
            "gt_audit": {
                "first_screen_uses_gt": False,
                "pass1_solver_uses_gt": False,
                "residual_calculation_uses_gt": False,
                "gt_errors_attached_posthoc_for_forensics": True,
            },
        },
    )

    write_manifest(
        out_dir / "run_manifest.json",
        {
            "run_id": run_id,
            "raw_observation_count": len(raw_rows),
            "first_screen_survivor_count": len(screened_rows),
            "first_screen_rejected_count": len(removed_rows),
            "prepared_sample_count": len(samples),
            "pass1_success_count": sum(
                1 for r in results if r.success
            ),
            "output_dir": str(out_dir),
        },
    )

    print("")
    print("=" * 94)
    print("STEP 21: RAW SCREEN -> UNCHANGED SOLVER #2 PASS 1")
    print("=" * 94)
    print(f"Raw observations      : {len(raw_rows)}")
    print(f"First-screen survivors: {len(screened_rows)}")
    print(f"First-screen rejected : {len(removed_rows)}")
    print(f"Prepared samples      : {len(samples)}")
    print(
        f"Pass-1 success        : "
        f"{sum(1 for r in results if r.success)}/{len(results)}"
    )
    print(f"Residual rows         : {len(residual_rows_all)}")
    print(f"Output directory      : {out_dir}")
    print("")
    print("First screen:")
    print("  reject whole tag if front |angle| > 30 deg")
    print("  reject whole tag if front distance > 5.5 m")
    print("  no rear-distance rule")
    print("")
    print("Pass 1:")
    print("  unchanged Solver #2")
    print("  original default Solver #2 config")
    print("  no yaw")
    print("  no GT")
    print("")
    print("Key Layer-2 study files:")
    print("  pass1_signed_component_residuals.csv")
    print("  pass1_ranked_distance_residuals.csv")
    print("  pass1_ranked_angle_residuals.csv")
    print("=" * 94)
    print("")


if __name__ == "__main__":
    RAW_CSV_PATH = (
        ROOT_DIR
        / "data"
        / "input"
        / "all_observations_long.csv"
    )

    TAG_MAP_PATH = (
        ROOT_DIR.parent
        / "sitemap"
        / "DemoRoom"
        / "tag_location.txt"
    )

    run_frontend(
        raw_csv_path=RAW_CSV_PATH,
        tag_map_path=TAG_MAP_PATH,
    )
