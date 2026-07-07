from __future__ import annotations

"""
t11_collect_validation_final_componentwise.py

Interactive human-operated validation collector for the CURRENT final solver.

This is intentionally a thin companion around the proven:
    t11_collect_observation_sample.py

Existing collection flow remains unchanged:
    1. operator enters x_m and y_m
    2. preferred heading is looked up
    3. operator physically rotates robot
    4. press Enter
    5. collect one fresh mobility.report.location report
    6. existing collector prints/saves per-tag measurements

Then this companion adds:
    A. observable-only acquisition-quality conclusion
    B. current frozen no-GT localization pipeline
    C. entered-vs-estimated difference for human inspection

VALIDATION INTEGRITY RULE:
    GT-vs-estimate error is NEVER used by automatic KEEP / RETAKE logic.

CURRENT ESTIMATION PIPELINE:
    Layer 1, whole-tag:
        reject front |angle| > 30 deg
        reject front distance > 5.5 m
        no rear-distance rule
        no rear-angle rule

    Pass 1:
        unchanged Solver #2 distance + angle, no yaw

    Distance second screen:
        M2_j = z_d,j - median(peer z_d)
        reject distance component if |M2_j| >= 0.95
        NO middle holdout solver

    Angle:
        no second-stage angle screening

    Yaw:
        same GT-free keep / flip / reject logic used before
        from Pass-1 pose
        |pred yaw| >= 8 deg
        best branch error <= 10 deg
        branch separation >= 15 deg

    Final:
        component-wise D/A/Y nonlinear solver
        sigma D/A/Y = 0.05 m / 2.5 deg / 3 deg
        global weights = 1.0 / 1.0 / 0.75

No truth-assisted yaw fields are used.
"""

import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------

THIS_DIR = Path(__file__).resolve().parent
ROOT_DIR = THIS_DIR.parent
LOCATION_SOLVER_DIR = ROOT_DIR / "locationSolver"

for p in (ROOT_DIR, LOCATION_SOLVER_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


# ---------------------------------------------------------------------------
# Reuse proven collection flow. Do not duplicate/rewrite it.
# ---------------------------------------------------------------------------

import t11_collect_observation_sample as collector


# ---------------------------------------------------------------------------
# Current final-pipeline imports
# ---------------------------------------------------------------------------

from src.common.component_preparation import (
    M2DistanceScreenConfig,
    YawAdmissionConfig,
    compute_pass1_component_rows,
    decide_distance_use_from_m2,
    prepare_final_componentwise_sample,
)
from src.common.tag_map_loader import (
    build_tag_pose_map,
    load_tag_xy_map,
    load_tag_yaw_json,
)
from src.solvers.solver_2_distance_angle import (
    DistanceAngleSolverConfig,
    solve_distance_angle,
)
from src.solvers.solver_6_componentwise_distance_angle_yaw import (
    ComponentwiseDistanceAngleYawSolverConfig,
    solve_componentwise_distance_angle_yaw,
)


# ---------------------------------------------------------------------------
# Known DemoRoom infrastructure
# ---------------------------------------------------------------------------

TAG_MAP_PATH = (
    ROOT_DIR
    / "sitemap"
    / "DemoRoom"
    / "tag_location.txt"
)

TAG_YAW_MAP_PATH = (
    LOCATION_SOLVER_DIR
    / "config"
    / "datasets"
    / "demoroom_tag_yaw_v1.json"
)


# ---------------------------------------------------------------------------
# Frozen current-pipeline parameters
# ---------------------------------------------------------------------------

FRONT_MAX_DISTANCE_M = 5.5
FRONT_MAX_ABS_ANGLE_DEG = 30.0

MIN_USABLE_OBSERVATIONS = 4
MIN_UNIQUE_TAGS = 4
PREFERRED_USABLE_OBSERVATIONS = 5

PASS1_CONFIG = DistanceAngleSolverConfig()

M2_CONFIG = M2DistanceScreenConfig(
    abs_m2_threshold=0.95,
)

YAW_CONFIG = YawAdmissionConfig(
    min_abs_predicted_yaw_deg=8.0,
    yaw_accept_best_error_deg=10.0,
    yaw_accept_separation_deg=15.0,
)

FINAL_CONFIG = ComponentwiseDistanceAngleYawSolverConfig(
    distance_sigma_m=0.05,
    angle_sigma_deg=2.5,
    yaw_sigma_deg=3.0,
    distance_global_weight=1.0,
    angle_global_weight=1.0,
    yaw_global_weight=0.75,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _wrap_angle_deg(a: float) -> float:
    return float((a + 180.0) % 360.0 - 180.0)


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _screen_observation_current(
    obs: Dict[str, Any],
) -> Tuple[bool, str]:
    """
    CURRENT observable-only Layer-1 whole-tag screen.

    Reject only:
      front |angle| > 30 deg
      front distance > 5.5 m

    No rear-distance rule.
    No rear-angle rule.
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

    return True, "usable"


def _repair_positive_component_weights(
    obs: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Preserve positive existing weights.
    Repair non-positive/missing D/A weights to 1.0 for rows surviving
    the CURRENT Layer-1 screen, so old hidden gates cannot suppress them.
    """
    obs2 = dict(obs)
    measured2 = dict(obs.get("measured") or {})
    weights2 = dict(obs.get("weights") or {})
    flags2 = dict(obs.get("flags") or {})

    wd = _safe_float(weights2.get("distance"))
    wa = _safe_float(weights2.get("angle"))
    wy = _safe_float(weights2.get("yaw"))

    if wd is None or wd <= 0.0:
        weights2["distance"] = 1.0
    if wa is None or wa <= 0.0:
        weights2["angle"] = 1.0

    # Raw yaw remains raw here. Yaw admission later decides use.
    if wy is None:
        weights2["yaw"] = 1.0

    # Explicitly disable old truth-assisted yaw path.
    flags2["yaw_use_offline_label"] = False
    measured2["yaw_sign_corrected_deg"] = None

    obs2["measured"] = measured2
    obs2["weights"] = weights2
    obs2["flags"] = flags2
    return obs2


def _build_current_sample(
    observations: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, int]]:
    """
    Build one in-memory sample from the live collector payload.

    Returns:
      sample of current Layer-1 survivors
      rejected audit rows
      rejected-reason counts
    """
    usable: List[Dict[str, Any]] = []
    rejected_rows: List[Dict[str, Any]] = []
    reason_counts: Dict[str, int] = {}

    for idx, obs in enumerate(observations, start=1):
        ok, reason = _screen_observation_current(obs)

        if not ok:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            rejected_rows.append({
                "tag_id": obs.get("tag_id"),
                "camera_role": obs.get("camera_role"),
                "reason": reason,
            })
            continue

        obs2 = _repair_positive_component_weights(obs)

        if not obs2.get("observation_uid"):
            obs2["observation_uid"] = f"field_obs_{idx:03d}"

        usable.append(obs2)

    sample = {
        "sample_uid": "interactive_final_validation",
        "observations": usable,
        "observation_count": len(usable),
    }
    return sample, rejected_rows, reason_counts


def _automatic_acquisition_conclusion(
    observations: List[Dict[str, Any]],
) -> Tuple[str, Dict[str, int], int, int, int, List[str]]:
    """
    Automatic KEEP / RETAKE decision.

    Observable acquisition quality only.
    Does NOT use entered GT or localization error.
    """
    sample, _, gated_reasons = _build_current_sample(observations)
    usable = list(sample.get("observations") or [])

    unique_tags = len({
        int(obs["tag_id"])
        for obs in usable
    })

    front_count = sum(
        1 for obs in usable
        if str(obs.get("camera_role") or "").strip().lower() == "front"
    )
    rear_count = sum(
        1 for obs in usable
        if str(obs.get("camera_role") or "").strip().lower() == "rear"
    )

    hard_reasons: List[str] = []
    cautions: List[str] = []

    if len(usable) < MIN_USABLE_OBSERVATIONS:
        hard_reasons.append(
            f"usable observations={len(usable)} < {MIN_USABLE_OBSERVATIONS}"
        )

    if unique_tags < MIN_UNIQUE_TAGS:
        hard_reasons.append(
            f"unique tags={unique_tags} < {MIN_UNIQUE_TAGS}"
        )

    if (
        len(usable) >= MIN_USABLE_OBSERVATIONS
        and len(usable) < PREFERRED_USABLE_OBSERVATIONS
    ):
        cautions.append(
            f"only {len(usable)} usable observations; "
            f"{PREFERRED_USABLE_OBSERVATIONS}+ preferred"
        )

    if hard_reasons:
        conclusion = "RETAKE"
    elif cautions:
        conclusion = "KEEP_WITH_CAUTION"
    else:
        conclusion = "KEEP"

    return (
        conclusion,
        gated_reasons,
        len(usable),
        front_count,
        rear_count,
        hard_reasons + cautions,
    )


def _run_current_final_pipeline(
    observations: List[Dict[str, Any]],
):
    sample, rejected_rows, gated_reasons = _build_current_sample(
        observations
    )

    if len(sample["observations"]) < 2:
        return {
            "sample": sample,
            "rejected_rows": rejected_rows,
            "gated_reasons": gated_reasons,
            "pass1": None,
            "component_rows": [],
            "distance_decisions": {},
            "final_sample": None,
            "final_audit": [],
            "final_result": None,
        }

    tag_xy_map = load_tag_xy_map(TAG_MAP_PATH)
    tag_yaw_map = load_tag_yaw_json(TAG_YAW_MAP_PATH)
    tag_pose_map = build_tag_pose_map(
        tag_xy_map,
        tag_yaw_map,
    )

    pass1 = solve_distance_angle(
        sample=sample,
        tag_xy_map=tag_xy_map,
        config=PASS1_CONFIG,
    )

    if not pass1.success:
        return {
            "sample": sample,
            "rejected_rows": rejected_rows,
            "gated_reasons": gated_reasons,
            "pass1": pass1,
            "component_rows": [],
            "distance_decisions": {},
            "final_sample": None,
            "final_audit": [],
            "final_result": None,
        }

    component_rows = compute_pass1_component_rows(
        sample=sample,
        pass1=pass1,
        tag_xy_map=tag_xy_map,
        solver_config=PASS1_CONFIG,
    )

    distance_decisions = decide_distance_use_from_m2(
        component_rows=component_rows,
        config=M2_CONFIG,
    )

    final_sample, final_audit = prepare_final_componentwise_sample(
        sample=sample,
        pass1=pass1,
        distance_decisions_by_uid=distance_decisions,
        tag_pose_map=tag_pose_map,
        yaw_config=YAW_CONFIG,
    )

    final_result = solve_componentwise_distance_angle_yaw(
        sample=final_sample,
        tag_pose_map=tag_pose_map,
        config=FINAL_CONFIG,
    )

    return {
        "sample": sample,
        "rejected_rows": rejected_rows,
        "gated_reasons": gated_reasons,
        "pass1": pass1,
        "component_rows": component_rows,
        "distance_decisions": distance_decisions,
        "final_sample": final_sample,
        "final_audit": final_audit,
        "final_result": final_result,
    }


def _print_current_field_check(
    payload: Dict[str, Any],
    gt_x_m: float,
    gt_y_m: float,
    gt_heading_deg: float,
) -> None:
    observations = list(payload.get("observations") or [])

    (
        collection_conclusion,
        gated_reasons,
        usable_count,
        front_count,
        rear_count,
        notes,
    ) = _automatic_acquisition_conclusion(observations)

    run = _run_current_final_pipeline(observations)

    sample = run["sample"]
    pass1 = run["pass1"]
    final_audit = run["final_audit"]
    final_result = run["final_result"]

    unique_tags = len({
        int(obs["tag_id"])
        for obs in sample.get("observations") or []
    })

    distance_rejected = [
        row for row in final_audit
        if not bool(row.get("distance_use"))
    ]
    yaw_accepted = [
        row for row in final_audit
        if bool(row.get("yaw_use"))
    ]
    yaw_keep = [
        row for row in yaw_accepted
        if row.get("yaw_mode") == "keep"
    ]
    yaw_flip = [
        row for row in yaw_accepted
        if row.get("yaw_mode") == "flip"
    ]

    print("")
    print("=" * 88)
    print("VALIDATION FIELD CHECK — CURRENT FINAL COMPONENT-WISE SOLVER")
    print("=" * 88)
    print(
        f"Observed / Layer-1 usable : "
        f"{len(observations)} / {usable_count}"
    )
    print(f"Unique usable tags         : {unique_tags}")
    print(
        f"Usable front/rear          : "
        f"{front_count} / {rear_count}"
    )

    if gated_reasons:
        print(
            "Layer-1 gated out          : "
            + ", ".join(
                f"{k}={v}"
                for k, v in gated_reasons.items()
            )
        )
    else:
        print("Layer-1 gated out          : 0")

    print("")
    print("Frozen current pipeline:")
    print("  Layer 1                  : front |A|>30° OR front D>5.5m")
    print("  Rear Layer-1 gate        : NONE")
    print("  Pass 1                   : original Solver #2 D+A, no yaw")
    print("  Distance second screen   : |M2| >= 0.95")
    print("  Middle holdout solver    : NONE")
    print("  Angle second screen      : NONE")
    print("  Yaw gates                : |pred|>=8°, best<=10°, sep>=15°")
    print("  Final sigmas D/A/Y       : 0.05m / 2.5° / 3°")
    print("  Final weights D/A/Y      : 1.0 / 1.0 / 0.75")
    print("  GT in estimator          : NONE")

    print("")

    if pass1 is not None and pass1.success:
        print(
            f"Pass-1 pose                : "
            f"x={float(pass1.estimated_x_m):.3f}, "
            f"y={float(pass1.estimated_y_m):.3f}, "
            f"h={float(pass1.estimated_heading_deg)%360.0:.1f}°"
        )
    else:
        reason = (
            "unavailable"
            if pass1 is None
            else str(pass1.failure_reason)
        )
        print(f"Pass-1 pose                : FAILED ({reason})")

    if final_result is not None and final_result.success:
        est_x = float(final_result.estimated_x_m)
        est_y = float(final_result.estimated_y_m)
        est_h = float(final_result.estimated_heading_deg) % 360.0

        pos_diff = math.hypot(
            est_x - float(gt_x_m),
            est_y - float(gt_y_m),
        )
        heading_diff = abs(
            _wrap_angle_deg(
                est_h - float(gt_heading_deg)
            )
        )

        print(
            f"Final pose                 : "
            f"x={est_x:.3f}, "
            f"y={est_y:.3f}, "
            f"h={est_h:.1f}°"
        )
        print(
            f"Human reference            : "
            f"position diff={pos_diff*100.0:.1f} cm, "
            f"heading diff={heading_diff:.1f}°"
        )
    else:
        reason = (
            "unavailable"
            if final_result is None
            else str(final_result.failure_reason)
        )
        print(f"Final pose                 : FAILED ({reason})")

    print("")
    print(
        f"Distance components cut    : {len(distance_rejected)}"
        + (
            " ["
            + ", ".join(
                f"Tag{int(r['tag_id'])}"
                for r in distance_rejected
            )
            + "]"
            if distance_rejected
            else ""
        )
    )

    print(
        f"Angle components cut       : 0"
    )

    print(
        f"Yaw admitted               : {len(yaw_accepted)} "
        f"(keep={len(yaw_keep)}, flip={len(yaw_flip)})"
    )

    if yaw_flip:
        print(
            "Yaw flips                  : "
            + ", ".join(
                f"Tag{int(r['tag_id'])}"
                for r in yaw_flip
            )
        )

    print("")
    print(f"COLLECTION                  : {collection_conclusion}")

    if notes:
        print("Notes                       : " + "; ".join(notes))

    print(
        "Decision rule               : observable acquisition quality only"
    )
    print(
        "Human reference             : GT-vs-final difference is display-only; "
        "NOT a KEEP/RETAKE gate"
    )
    print("=" * 88)
    print("")


# ---------------------------------------------------------------------------
# Interactive flow
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("")
    print("=" * 88)
    print("T11: Interactive validation collection with CURRENT final solver")
    print("=" * 88)
    print(f"ROOT_DIR              = {ROOT_DIR}")
    print(f"SCANNER_NAME          = {collector.SCANNER_NAME}")
    print(
        f"PREFERRED_HEADING_DIR = "
        f"{collector.PREFERRED_HEADING_DIR}"
    )
    print(f"LOG_DIR               = {collector.LOG_DIR}")
    print("")
    print("Current frozen estimator:")
    print("  Layer-1 front D      <= 5.5 m")
    print("  Layer-1 front |A|    <= 30 deg")
    print("  Layer-1 rear gate    = NONE")
    print("  Pass-1               = original Solver #2 D+A")
    print("  M2 reject            = |M2| >= 0.95")
    print("  angle second screen  = NONE")
    print("  yaw gates            = 8 / 10 / 15 deg")
    print("  final sigma D/A/Y    = 0.05 / 2.5 / 3")
    print("  final weights D/A/Y  = 1.0 / 1.0 / 0.75")
    print("  middle holdout       = NONE")
    print("  GT in estimator      = NONE")
    print("")

    gt_x_m = collector._prompt_float(
        "Input ground-truth x_m: "
    )
    gt_y_m = collector._prompt_float(
        "Input ground-truth y_m: "
    )

    preferred = collector._lookup_preferred_heading(
        gt_x_m,
        gt_y_m,
    )

    if preferred is None:
        print("")
        print("Preferred-heading lookup failed.")
        target_heading_deg = collector._prompt_float(
            "Manually input target heading_deg: "
        )
    else:
        target_heading_deg = float(
            preferred["heading_deg"]
        )

        print("")
        print("-" * 88)
        print(
            f"Preferred heading at "
            f"x={gt_x_m:.3f}, y={gt_y_m:.3f}: "
            f"{target_heading_deg:.1f} deg"
        )
        print(
            f"Lookup source: "
            f"{preferred['source_file']}"
        )
        print(
            f"Nearest lookup point: "
            f"x={preferred['nearest_x_m']:.3f}, "
            f"y={preferred['nearest_y_m']:.3f}, "
            f"distance={preferred['nearest_dist_m']:.3f} m"
        )
        print("-" * 88)

    print("")
    print(
        f"Please rotate {collector.SCANNER_NAME} "
        f"to heading {target_heading_deg:.1f} deg."
    )
    input(
        "Press Enter to send mobility.report.location..."
    )

    actual_heading = collector._prompt_optional_float(
        f"Input actual heading_deg, or press Enter "
        f"to use {target_heading_deg:.1f}: "
    )
    if actual_heading is None:
        actual_heading = target_heading_deg

    print("")
    print(
        "Collecting one fresh mobility.report.location sample..."
    )

    # Proven collector remains untouched.
    payload = collector.run_once(
        gt_x_m=float(gt_x_m),
        gt_y_m=float(gt_y_m),
        gt_heading_deg=float(actual_heading),
    )

    # Current final-pipeline post-collection display.
    _print_current_field_check(
        payload=payload,
        gt_x_m=float(gt_x_m),
        gt_y_m=float(gt_y_m),
        gt_heading_deg=float(actual_heading),
    )
