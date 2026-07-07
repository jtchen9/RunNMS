from __future__ import annotations

"""
run_candidate_exclusion_diagnostic.py

Forensic diagnostic only. Does NOT modify frozen Stage-5.

Purpose
-------
The current Stage-5 one-outlier screen scores each observation against an
all-in Pass-1 pose. A bad observation can bias that pose, shrink its own
residual, and inflate opposite-signed residuals of good observations.

This experiment tests each observation independently:

    for candidate j:
        fit D+A pose using all observations except j
        H_j = held-out candidate disagreement against that independent pose
        C_j = retained-set disagreement around that same pose

Then export:
    H_j
    C_j
    H_j / C_j
    H_j - C_j
    signed distance and angle residuals
    post-hoc GT measurement-error labels for forensic checking only

No GT is used in any candidate-exclusion solve or score.
GT columns are attached only after scoring, to answer:
    - do top-ranked candidates cover known measurement villains?
    - do large measurement errors survive current Stage-5 screening?
"""

import csv
import json
import math
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.geometry import (
    predicted_tag_angle_deg,
    predicted_tag_distance_m,
    wrap_angle_deg,
)
from src.common.prepared_loader import load_prepared_dataset
from src.common.run_manifest import make_run_id, write_manifest
from src.common.run_output import write_json
from src.common.tag_map_loader import (
    build_tag_pose_map,
    load_tag_xy_map,
    load_tag_yaw_json,
)
from src.solvers.solver_2_distance_angle import (
    DistanceAngleSolverConfig,
    solve_distance_angle,
)
from src.solvers.solver_5_gtfree_yaw_twopass import (
    Stage5Config,
    solve_stage5_gtfree_yaw,
)


# ---------------------------------------------------------------------------
# Frozen D+A scales used by Stage-5 Pass 1
# ---------------------------------------------------------------------------

DA_CONFIG = DistanceAngleSolverConfig(
    distance_sigma_m=0.05,
    angle_sigma_deg=2.5,
    distance_global_weight=1.0,
    angle_global_weight=1.0,
)

# Exact frozen Stage-5, used only to record current reject/survive status.
STAGE5_CONFIG = Stage5Config(
    distance_sigma_m=0.05,
    angle_sigma_deg=2.5,
    yaw_sigma_deg=3.0,
    distance_global_weight=1.0,
    angle_global_weight=1.0,
    yaw_global_weight=0.75,
    rejection_score_threshold=0.80,
    min_score_gap=0.50,
    max_rejections=1,
    min_observations_after_rejection=3,
    min_abs_predicted_yaw_deg=8.0,
    yaw_accept_best_error_deg=10.0,
    yaw_accept_separation_deg=15.0,
)

EPS = 1e-9


# ---------------------------------------------------------------------------
# Exact observable gates used before Stage-5 Pass 1
# ---------------------------------------------------------------------------

FRONT_MAX_DISTANCE_M = 5.5
REAR_MAX_DISTANCE_M = 3.0
FRONT_MAX_ABS_ANGLE_DEG = 30.0
REAR_MAX_ABS_ANGLE_DEG = 15.0


def _phase1_observable_gate(
    obs: Dict[str, Any],
) -> Tuple[bool, str]:
    """
    Keep only observations eligible to enter Stage-5 Pass 1.

    Observable-only:
      front: distance <= 5.5 m and |angle| <= 30 deg
      rear : distance <= 3.0 m and |angle| <= 15 deg

    No GT is used.
    """
    role = str(obs.get("camera_role") or "").strip().lower()
    measured = obs.get("measured") or {}

    d = _safe_float(measured.get("distance_m"))
    a = _safe_float(measured.get("angle_deg"))

    if role not in {"front", "rear"}:
        return False, "unknown_camera_role"

    if d is None or d <= 0.0:
        return False, "invalid_distance"

    if a is None:
        return False, "invalid_angle"

    if role == "front":
        if d > FRONT_MAX_DISTANCE_M:
            return False, "front_distance_gt_5.5m"
        if abs(a) > FRONT_MAX_ABS_ANGLE_DEG:
            return False, "front_abs_angle_gt_30deg"
        return True, "eligible"

    if d > REAR_MAX_DISTANCE_M:
        return False, "rear_distance_gt_3.0m"
    if abs(a) > REAR_MAX_ABS_ANGLE_DEG:
        return False, "rear_abs_angle_gt_15deg"

    return True, "eligible"


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
                fields.append(key)
                seen.add(key)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _load_truth_by_observation_uid(
    source_csv_path: Path,
) -> Dict[str, Dict[str, Any]]:
    """
    Post-hoc forensic truth lookup only.
    Not used by any solve or score.
    """
    out: Dict[str, Dict[str, Any]] = {}

    if not source_csv_path.exists():
        return out

    with source_csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            uid = str(row.get("observation_uid") or "")
            if uid:
                out[uid] = row

    return out


def _score_observation_at_pose(
    obs: Dict[str, Any],
    x_m: float,
    y_m: float,
    heading_deg: float,
    tag_xy_map: Dict[int, Tuple[float, float]],
) -> Dict[str, float]:
    tag_id = int(obs["tag_id"])
    tag_x, tag_y = tag_xy_map[tag_id]

    measured = obs.get("measured") or {}
    weights = obs.get("weights") or {}

    meas_d = float(measured["distance_m"])
    meas_a = float(measured["angle_deg"])

    pred_d = predicted_tag_distance_m(
        x_m, y_m, heading_deg,
        obs["camera_role"],
        tag_x, tag_y,
    )
    pred_a = predicted_tag_angle_deg(
        x_m, y_m, heading_deg,
        obs["camera_role"],
        tag_x, tag_y,
    )

    rd = pred_d - meas_d
    ra = wrap_angle_deg(pred_a - meas_a)

    wd = float(weights.get("distance", 1.0))
    wa = float(weights.get("angle", 1.0))

    zd = (
        math.sqrt(DA_CONFIG.distance_global_weight * wd)
        * rd / DA_CONFIG.distance_sigma_m
    )
    za = (
        math.sqrt(DA_CONFIG.angle_global_weight * wa)
        * ra / DA_CONFIG.angle_sigma_deg
    )

    score = math.sqrt((zd * zd + za * za) / 2.0)

    return {
        "predicted_distance_m": pred_d,
        "distance_residual_m": rd,
        "normalized_distance_residual": zd,
        "predicted_angle_deg": pred_a,
        "angle_residual_deg": ra,
        "normalized_angle_residual": za,
        "combined_da_score": score,
    }


def _candidate_exclusion_rows_for_sample(
    sample: Dict[str, Any],
    tag_xy_map: Dict[int, Tuple[float, float]],
    current_rejected_uids: set[str],
    truth_by_uid: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    observations_all = list(sample.get("observations") or [])

    observations: List[Dict[str, Any]] = []
    for obs in observations_all:
        ok, _reason = _phase1_observable_gate(obs)
        if ok:
            observations.append(obs)

    n = len(observations)
    rows: List[Dict[str, Any]] = []

    for j, candidate in enumerate(observations):
        candidate_uid = str(candidate.get("observation_uid") or "")
        retained = [
            obs for k, obs in enumerate(observations)
            if k != j
        ]

        sample_minus_j = dict(sample)
        sample_minus_j["observations"] = retained
        sample_minus_j["observation_count"] = len(retained)
        sample_minus_j["sample_uid"] = (
            f"{sample['sample_uid']}__holdout__{candidate_uid}"
        )

        result = solve_distance_angle(
            sample=sample_minus_j,
            tag_xy_map=tag_xy_map,
            config=DA_CONFIG,
        )

        base: Dict[str, Any] = {
            "sample_uid": str(sample["sample_uid"]),
            "observation_uid": candidate_uid,
            "tag_id": int(candidate["tag_id"]),
            "camera_role": str(candidate.get("camera_role") or ""),
            "sample_observation_count_before_explicit_gate": len(observations_all),
            "sample_observation_count_phase1_eligible": n,
            "retained_observation_count": len(retained),
            "phase1_observable_eligible": "Y",
            "candidate_fit_success": bool(result.success),
            "current_stage5_rejected": (
                "Y" if candidate_uid in current_rejected_uids else "N"
            ),
            "current_stage5_survived": (
                "N" if candidate_uid in current_rejected_uids else "Y"
            ),
        }

        truth = truth_by_uid.get(candidate_uid, {})
        distance_error = _safe_float(truth.get("distance_error_m"))
        angle_error = _safe_float(truth.get("angle_error_deg"))

        base.update({
            # Post-hoc forensic truth only.
            "gt_distance_error_m": distance_error,
            "gt_abs_distance_error_m": (
                None if distance_error is None else abs(distance_error)
            ),
            "gt_angle_error_deg": angle_error,
            "gt_abs_angle_error_deg": (
                None if angle_error is None else abs(angle_error)
            ),
            "gt_distance_villain_gt8cm": (
                ""
                if distance_error is None
                else ("Y" if abs(distance_error) > 0.08 else "N")
            ),
            "gt_angle_villain_gt8deg": (
                ""
                if angle_error is None
                else ("Y" if abs(angle_error) > 8.0 else "N")
            ),
            "gt_any_da_villain": (
                ""
                if distance_error is None and angle_error is None
                else (
                    "Y"
                    if (
                        (distance_error is not None and abs(distance_error) > 0.08)
                        or
                        (angle_error is not None and abs(angle_error) > 8.0)
                    )
                    else "N"
                )
            ),
        })

        if not result.success:
            base.update({
                "heldout_H_score": None,
                "retained_C_rms": None,
                "retained_C_max": None,
                "H_over_C": None,
                "H_minus_C": None,
                "heldout_distance_residual_m": None,
                "heldout_angle_residual_deg": None,
            })
            rows.append(base)
            continue

        x = float(result.estimated_x_m)
        y = float(result.estimated_y_m)
        h = float(result.estimated_heading_deg)

        heldout = _score_observation_at_pose(
            candidate, x, y, h, tag_xy_map
        )

        retained_scores = [
            _score_observation_at_pose(obs, x, y, h, tag_xy_map)
            for obs in retained
        ]

        c_rms = math.sqrt(
            sum(
                float(r["combined_da_score"]) ** 2
                for r in retained_scores
            )
            / max(1, len(retained_scores))
        )
        c_max = max(
            (
                float(r["combined_da_score"])
                for r in retained_scores
            ),
            default=0.0,
        )
        H = float(heldout["combined_da_score"])

        base.update({
            "holdout_fit_x_m": x,
            "holdout_fit_y_m": y,
            "holdout_fit_heading_deg": h,

            "heldout_predicted_distance_m":
                heldout["predicted_distance_m"],
            "heldout_distance_residual_m":
                heldout["distance_residual_m"],
            "heldout_normalized_distance_residual":
                heldout["normalized_distance_residual"],

            "heldout_predicted_angle_deg":
                heldout["predicted_angle_deg"],
            "heldout_angle_residual_deg":
                heldout["angle_residual_deg"],
            "heldout_normalized_angle_residual":
                heldout["normalized_angle_residual"],

            "heldout_H_score": H,
            "retained_C_rms": c_rms,
            "retained_C_max": c_max,

            # Export several simple rankings. Do not choose/tune one yet.
            "H_over_C": H / max(c_rms, EPS),
            "H_minus_C": H - c_rms,
            "H_over_1plus_C": H / (1.0 + c_rms),
        })

        rows.append(base)

    return rows


def _add_ranks(rows: List[Dict[str, Any]]) -> None:
    metrics = [
        "heldout_H_score",
        "H_over_C",
        "H_minus_C",
        "H_over_1plus_C",
    ]

    for metric in metrics:
        valid = [
            r for r in rows
            if _safe_float(r.get(metric)) is not None
        ]
        valid.sort(
            key=lambda r: float(r[metric]),
            reverse=True,
        )

        rank_key = f"global_rank_{metric}"
        pct_key = f"global_percentile_{metric}"

        total = len(valid)
        for rank, row in enumerate(valid, start=1):
            row[rank_key] = rank
            row[pct_key] = (
                1.0 if total <= 1
                else 1.0 - (rank - 1) / (total - 1)
            )


def _coverage_summary(
    rows: List[Dict[str, Any]],
    metric: str,
) -> Dict[str, Any]:
    valid = [
        r for r in rows
        if _safe_float(r.get(metric)) is not None
    ]
    valid.sort(key=lambda r: float(r[metric]), reverse=True)

    villains = [
        r for r in valid
        if str(r.get("gt_any_da_villain") or "") == "Y"
    ]
    villain_uids = {r["observation_uid"] for r in villains}

    out: Dict[str, Any] = {
        "metric": metric,
        "valid_row_count": len(valid),
        "villain_count": len(villains),
        "top_fraction_coverage": {},
    }

    for frac in (0.05, 0.10, 0.20, 0.30):
        k = max(1, math.ceil(len(valid) * frac))
        top = valid[:k]
        covered = {
            r["observation_uid"]
            for r in top
            if r["observation_uid"] in villain_uids
        }

        out["top_fraction_coverage"][f"top_{int(frac*100)}pct"] = {
            "top_k": k,
            "villains_covered": len(covered),
            "villain_total": len(villains),
            "coverage_rate": (
                None if len(villains) == 0
                else len(covered) / len(villains)
            ),
        }

    return out


def run_diagnostic(
    prepared_subdir: str,
    source_csv_path: Path,
    tag_map_path: Path,
    tag_yaw_map_path: Path,
) -> None:
    prepared_path = (
        ROOT_DIR
        / "data"
        / "prepared"
        / prepared_subdir
        / "samples.json"
    )

    payload = load_prepared_dataset(prepared_path)
    samples = payload["samples"]

    tag_xy_map = load_tag_xy_map(tag_map_path)
    tag_yaw_map = load_tag_yaw_json(tag_yaw_map_path)
    tag_pose_map = build_tag_pose_map(tag_xy_map, tag_yaw_map)

    truth_by_uid = _load_truth_by_observation_uid(source_csv_path)

    all_rows: List[Dict[str, Any]] = []
    excluded_rows: List[Dict[str, Any]] = []

    for sample in samples:
        observations_all = list(sample.get("observations") or [])
        eligible_obs: List[Dict[str, Any]] = []

        for obs in observations_all:
            ok, reason = _phase1_observable_gate(obs)
            if ok:
                eligible_obs.append(obs)
            else:
                measured = obs.get("measured") or {}
                truth = truth_by_uid.get(
                    str(obs.get("observation_uid") or ""),
                    {},
                )
                excluded_rows.append({
                    "sample_uid": str(sample["sample_uid"]),
                    "observation_uid": str(obs.get("observation_uid") or ""),
                    "tag_id": int(obs["tag_id"]),
                    "camera_role": str(obs.get("camera_role") or ""),
                    "measured_distance_m": measured.get("distance_m"),
                    "measured_angle_deg": measured.get("angle_deg"),
                    "phase1_exclusion_reason": reason,
                    "gt_distance_error_m": truth.get("distance_error_m"),
                    "gt_angle_error_deg": truth.get("angle_error_deg"),
                })

        sample_gated = dict(sample)
        sample_gated["observations"] = eligible_obs
        sample_gated["observation_count"] = len(eligible_obs)

        # Current frozen Stage-5 status, for survivor audit only,
        # on exactly the same Phase-1-eligible observation set.
        s5_result, _, da_rows, _ = solve_stage5_gtfree_yaw(
            sample=sample_gated,
            tag_pose_map=tag_pose_map,
            config=STAGE5_CONFIG,
        )

        current_rejected_uids = {
            str(r["observation_uid"])
            for r in da_rows
            if bool(r.get("rejected"))
        }

        sample_rows = _candidate_exclusion_rows_for_sample(
            sample=sample_gated,
            tag_xy_map=tag_xy_map,
            current_rejected_uids=current_rejected_uids,
            truth_by_uid=truth_by_uid,
        )

        # Per-sample ranks are also useful.
        for metric in (
            "heldout_H_score",
            "H_over_C",
            "H_minus_C",
            "H_over_1plus_C",
        ):
            valid = [
                r for r in sample_rows
                if _safe_float(r.get(metric)) is not None
            ]
            valid.sort(
                key=lambda r: float(r[metric]),
                reverse=True,
            )
            for rank, row in enumerate(valid, start=1):
                row[f"sample_rank_{metric}"] = rank

        all_rows.extend(sample_rows)

    _add_ranks(all_rows)

    run_id = make_run_id(
        "candidate_exclusion_diagnostic",
        prepared_subdir,
    )
    out_dir = ROOT_DIR / "output" / "diagnostics" / run_id
    out_dir.mkdir(parents=True, exist_ok=False)

    # Global rankings.
    _write_csv(
        out_dir / "candidate_exclusion_all.csv",
        all_rows,
    )
    _write_csv(
        out_dir / "phase1_observable_excluded.csv",
        excluded_rows,
    )

    for metric in (
        "heldout_H_score",
        "H_over_C",
        "H_minus_C",
        "H_over_1plus_C",
    ):
        ranked = [
            r for r in all_rows
            if _safe_float(r.get(metric)) is not None
        ]
        ranked.sort(
            key=lambda r: float(r[metric]),
            reverse=True,
        )
        _write_csv(
            out_dir / f"ranked_by_{metric}.csv",
            ranked,
        )

    # Known villains that survived frozen Stage-5 screen.
    villain_survivors = [
        r for r in all_rows
        if str(r.get("gt_any_da_villain") or "") == "Y"
        and str(r.get("current_stage5_survived") or "") == "Y"
    ]
    villain_survivors.sort(
        key=lambda r: float(r.get("H_over_C") or -1e99),
        reverse=True,
    )
    _write_csv(
        out_dir / "gt_villains_surviving_stage5_screen.csv",
        villain_survivors,
    )

    # All GT villains, for coverage checking.
    villains = [
        r for r in all_rows
        if str(r.get("gt_any_da_villain") or "") == "Y"
    ]
    villains.sort(
        key=lambda r: float(r.get("H_over_C") or -1e99),
        reverse=True,
    )
    _write_csv(
        out_dir / "all_gt_da_villains.csv",
        villains,
    )

    coverage = {
        metric: _coverage_summary(all_rows, metric)
        for metric in (
            "heldout_H_score",
            "H_over_C",
            "H_minus_C",
            "H_over_1plus_C",
        )
    }

    write_json(
        out_dir / "coverage_summary.json",
        coverage,
    )

    write_json(
        out_dir / "run_config.json",
        {
            "prepared_subdir": prepared_subdir,
            "prepared_path": str(prepared_path),
            "source_csv_path": str(source_csv_path),
            "tag_map_path": str(tag_map_path),
            "da_config": asdict(DA_CONFIG),
            "frozen_stage5_config": asdict(STAGE5_CONFIG),
            "phase1_observable_gates": {
                "front_max_distance_m": FRONT_MAX_DISTANCE_M,
                "rear_max_distance_m": REAR_MAX_DISTANCE_M,
                "front_max_abs_angle_deg": FRONT_MAX_ABS_ANGLE_DEG,
                "rear_max_abs_angle_deg": REAR_MAX_ABS_ANGLE_DEG,
            },
            "definitions": {
                "H_j": (
                    "Held-out candidate normalized D/A RMS score against "
                    "pose fitted without candidate j."
                ),
                "C_j": (
                    "RMS of retained observations' normalized D/A scores "
                    "around the same pose fitted without candidate j."
                ),
                "H_over_C": "H_j / C_j",
                "H_minus_C": "H_j - C_j",
                "H_over_1plus_C": "H_j / (1 + C_j)",
            },
            "gt_audit": {
                "candidate_exclusion_solves_use_gt": False,
                "H_j_uses_gt": False,
                "C_j_uses_gt": False,
                "GT_columns_attached_after_scoring": True,
            },
            "villain_definition_for_forensic_audit_only": (
                "|distance_error| > 0.08 m OR |angle_error| > 8 deg"
            ),
        },
    )

    write_manifest(
        out_dir / "run_manifest.json",
        {
            "run_id": run_id,
            "sample_count": len(samples),
            "candidate_test_count": len(all_rows),
            "phase1_observable_excluded_count": len(excluded_rows),
            "gt_villain_count": len(villains),
            "gt_villain_surviving_stage5_count":
                len(villain_survivors),
        },
    )

    print("")
    print("=" * 94)
    print("CANDIDATE-EXCLUSION FORENSIC DIAGNOSTIC — PHASE-1 SCREENED")
    print("=" * 94)
    print(f"Prepared dataset      : {prepared_path}")
    print(f"Candidate tests       : {len(all_rows)}")
    print(f"Pre-Phase1 excluded   : {len(excluded_rows)}")
    print(f"Output directory      : {out_dir}")
    print("")
    print("Each observation j:")
    print("  fit D+A using all observations except j")
    print("  H_j = held-out candidate disagreement")
    print("  C_j = retained-set RMS disagreement")
    print("")
    print("Exported rankings:")
    print("  H_j")
    print("  H_j / C_j")
    print("  H_j - C_j")
    print("  H_j / (1 + C_j)")
    print("")
    print("Only Phase-1-eligible observations are tested.")
    print("Edge/far observations are excluded before any H_j/C_j calculation.")
    print("GT is used only afterward to check villain coverage.")
    print(f"GT D/A villains       : {len(villains)}")
    print(
        f"Villains surviving S5 : {len(villain_survivors)}"
    )
    print("")
    print("Key files:")
    print("  candidate_exclusion_all.csv")
    print("  ranked_by_H_over_C.csv")
    print("  gt_villains_surviving_stage5_screen.csv")
    print("  coverage_summary.json")
    print("=" * 94)
    print("")


if __name__ == "__main__":
    PREPARED_SUBDIR = "normal_diversity"

    SOURCE_CSV_PATH = (
        ROOT_DIR
        / "data"
        / "input"
        / "normal_diversity_v1.csv"
    )

    TAG_MAP_PATH = (
        ROOT_DIR.parent
        / "sitemap"
        / "DemoRoom"
        / "tag_location.txt"
    )

    TAG_YAW_MAP_PATH = (
        ROOT_DIR
        / "config"
        / "datasets"
        / "demoroom_tag_yaw_v1.json"
    )

    run_diagnostic(
        prepared_subdir=PREPARED_SUBDIR,
        source_csv_path=SOURCE_CSV_PATH,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
    )
