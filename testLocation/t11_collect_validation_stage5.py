from __future__ import annotations

"""
t11_collect_validation_stage5.py

Interactive human-operated validation collector.

This is a thin companion around the proven:
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
    B. exact frozen Stage-5 GT-free pose estimate
    C. entered-vs-estimated difference for human inspection only

VALIDATION INTEGRITY RULE:
    GT-vs-estimate error is NEVER used by automatic KEEP / RETAKE logic.

Stage-5 estimation path:
    Pass 1:
        distance + angle only
    Between passes:
        D/A-only standout outlier screening
        GT-free raw-yaw keep/flip decision from Pass-1 geometry
    Pass 2:
        cleaned D/A + qualified GT-free yaw

No truth-assisted yaw fields are used by Stage-5.
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
# Frozen Stage-5 imports
# ---------------------------------------------------------------------------

from src.common.tag_map_loader import (
    build_tag_pose_map,
    load_tag_xy_map,
    load_tag_yaw_json,
)
from src.solvers.solver_5_gtfree_yaw_twopass import (
    Stage5Config,
    solve_stage5_gtfree_yaw,
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
# Observable-only acquisition gates
# ---------------------------------------------------------------------------

FRONT_MAX_DISTANCE_M = 5.5
REAR_MAX_DISTANCE_M = 3.0

FRONT_MAX_ABS_ANGLE_DEG = 30.0
REAR_MAX_ABS_ANGLE_DEG = 15.0

FRONT_NEAR_EDGE_DEG = 25.0
REAR_NEAR_EDGE_DEG = 12.0

MIN_USABLE_OBSERVATIONS = 4
MIN_UNIQUE_TAGS = 4
PREFERRED_USABLE_OBSERVATIONS = 5


# ---------------------------------------------------------------------------
# Exact frozen Stage-5 configuration
# ---------------------------------------------------------------------------

STAGE5_CONFIG = Stage5Config(
    # Measurement scales
    distance_sigma_m=0.05,
    angle_sigma_deg=2.5,
    yaw_sigma_deg=3.0,

    # Relative weights
    distance_global_weight=1.0,
    angle_global_weight=1.0,
    yaw_global_weight=0.75,

    # D/A-only robust standout rule
    rejection_score_threshold=0.80,
    min_score_gap=0.50,
    max_rejections=1,
    min_observations_after_rejection=3,

    # GT-free yaw qualification
    min_abs_predicted_yaw_deg=8.0,
    yaw_accept_best_error_deg=10.0,
    yaw_accept_separation_deg=15.0,
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


def _screen_observation(
    obs: Dict[str, Any],
) -> Tuple[bool, str, bool]:
    """
    Observable-only row gate.

    Returns:
        usable
        reason
        near_edge
    """
    role = str(obs.get("camera_role") or "").strip().lower()
    measured = obs.get("measured") or {}

    d = _safe_float(measured.get("distance_m"))
    a = _safe_float(measured.get("angle_deg"))

    if role not in {"front", "rear"}:
        return False, "unknown_camera_role", False

    if d is None or d <= 0.0:
        return False, "invalid_distance", False

    if a is None:
        return False, "invalid_angle", False

    if role == "front":
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


def _build_stage5_sample(
    usable_observations: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build a Stage-5 sample from current measured observations.

    Important:
      - raw yaw_deg is preserved
      - truth-assisted yaw flags are forcibly disabled
      - existing yaw_sign_corrected_deg is forcibly cleared
      - no GT pose is included
    """
    observations_out: List[Dict[str, Any]] = []

    for idx, obs in enumerate(usable_observations, start=1):
        measured_in = dict(obs.get("measured") or {})
        weights_in = dict(obs.get("weights") or {})

        measured_out = {
            "distance_m": float(measured_in["distance_m"]),
            "angle_deg": float(measured_in["angle_deg"]),

            # Stage-5 uses raw yaw only.
            "yaw_deg": measured_in.get("yaw_deg"),

            # Explicitly prevent truth-assisted path.
            "yaw_sign_corrected_deg": None,
        }

        observations_out.append({
            "observation_uid": str(
                obs.get("observation_uid")
                or f"field_obs_{idx:03d}"
            ),
            "tag_id": int(obs["tag_id"]),
            "camera_role": str(
                obs.get("camera_role") or ""
            ).strip().lower(),
            "measured": measured_out,
            "weights": {
                "distance": float(weights_in.get("distance", 1.0)),
                "angle": float(weights_in.get("angle", 1.0)),
                "yaw": float(weights_in.get("yaw", 1.0)),
            },
            "flags": {
                # Must remain false on input.
                "yaw_use_offline_label": False,
            },
        })

    return {
        "sample_uid": "interactive_stage5_validation",
        "observations": observations_out,
        "observation_count": len(observations_out),
    }


def _run_frozen_stage5(
    usable_observations: List[Dict[str, Any]],
):
    if len(usable_observations) < 2:
        return None, None, [], []

    tag_xy_map = load_tag_xy_map(TAG_MAP_PATH)
    tag_yaw_map = load_tag_yaw_json(TAG_YAW_MAP_PATH)
    tag_pose_map = build_tag_pose_map(
        tag_xy_map,
        tag_yaw_map,
    )

    sample = _build_stage5_sample(
        usable_observations
    )

    return solve_stage5_gtfree_yaw(
        sample=sample,
        tag_pose_map=tag_pose_map,
        config=STAGE5_CONFIG,
    )


def _automatic_acquisition_conclusion(
    observations: List[Dict[str, Any]],
) -> Tuple[
    str,
    List[Dict[str, Any]],
    Dict[str, int],
    int,
    int,
    int,
    List[str],
]:
    """
    Automatic KEEP / RETAKE decision.

    Uses observable acquisition quality only.
    Does NOT use:
      - entered GT pose
      - Stage-5 localization error
      - evaluator metrics
      - true yaw
    """
    usable: List[Dict[str, Any]] = []
    gated_reasons: Dict[str, int] = {}
    near_edge_count = 0

    for obs in observations:
        ok, reason, near_edge = _screen_observation(obs)

        if ok:
            usable.append(obs)
            if near_edge:
                near_edge_count += 1
        else:
            gated_reasons[reason] = (
                gated_reasons.get(reason, 0) + 1
            )

    unique_tags = len({
        int(obs["tag_id"])
        for obs in usable
    })

    front_count = sum(
        1 for obs in usable
        if str(obs.get("camera_role") or "").strip().lower()
        == "front"
    )
    rear_count = sum(
        1 for obs in usable
        if str(obs.get("camera_role") or "").strip().lower()
        == "rear"
    )

    hard_reasons: List[str] = []
    cautions: List[str] = []

    if len(usable) < MIN_USABLE_OBSERVATIONS:
        hard_reasons.append(
            f"usable observations={len(usable)} "
            f"< {MIN_USABLE_OBSERVATIONS}"
        )

    if unique_tags < MIN_UNIQUE_TAGS:
        hard_reasons.append(
            f"unique tags={unique_tags} "
            f"< {MIN_UNIQUE_TAGS}"
        )

    if (
        len(usable) >= MIN_USABLE_OBSERVATIONS
        and len(usable) < PREFERRED_USABLE_OBSERVATIONS
    ):
        cautions.append(
            f"only {len(usable)} usable observations; "
            f"{PREFERRED_USABLE_OBSERVATIONS}+ preferred"
        )

    if len(usable) > 0:
        near_edge_fraction = near_edge_count / len(usable)
        if near_edge_fraction > 0.50:
            cautions.append(
                f"near-edge fraction={near_edge_fraction:.0%}"
            )

    if hard_reasons:
        conclusion = "RETAKE"
    elif cautions:
        conclusion = "KEEP_WITH_CAUTION"
    else:
        conclusion = "KEEP"

    return (
        conclusion,
        usable,
        gated_reasons,
        near_edge_count,
        front_count,
        rear_count,
        hard_reasons + cautions,
    )


def _print_stage5_field_check(
    payload: Dict[str, Any],
    gt_x_m: float,
    gt_y_m: float,
    gt_heading_deg: float,
) -> None:
    observations = list(payload.get("observations") or [])

    (
        collection_conclusion,
        usable,
        gated_reasons,
        near_edge_count,
        front_count,
        rear_count,
        notes,
    ) = _automatic_acquisition_conclusion(observations)

    unique_tags = len({
        int(obs["tag_id"])
        for obs in usable
    })

    stage5_result = None
    pass1_result = None
    da_rows = []
    yaw_rows = []

    if len(usable) >= 2:
        (
            stage5_result,
            pass1_result,
            da_rows,
            yaw_rows,
        ) = _run_frozen_stage5(usable)

    rejected_da = [
        row for row in da_rows
        if bool(row.get("rejected"))
    ]
    accepted_yaw = [
        row for row in yaw_rows
        if bool(row.get("accepted"))
    ]
    accepted_keep = [
        row for row in accepted_yaw
        if row.get("best_mode") == "keep"
    ]
    accepted_flip = [
        row for row in accepted_yaw
        if row.get("best_mode") == "flip"
    ]

    print("")
    print("=" * 82)
    print("VALIDATION FIELD CHECK — FROZEN STAGE-5")
    print("=" * 82)
    print(
        f"Observed / usable : "
        f"{len(observations)} / {len(usable)}"
    )
    print(f"Unique tags       : {unique_tags}")
    print(
        f"Usable front/rear : "
        f"{front_count} / {rear_count}"
    )
    print(f"Near-edge usable  : {near_edge_count}")

    if gated_reasons:
        print(
            "Gated out         : "
            + ", ".join(
                f"{k}={v}"
                for k, v in gated_reasons.items()
            )
        )
    else:
        print("Gated out         : 0")

    print("")
    print("Frozen Stage-5:")
    print("  Pass 1          : distance + angle only")
    print("  D/A rejection   : threshold=0.80, gap=0.50, max=1")
    print("  Yaw             : raw keep/flip from Pass-1 geometry")
    print("  Yaw gates       : |pred|>=8°, best<=10°, separation>=15°")
    print("  Pass 2 weights  : D=1.0, A=1.0, Y=0.75")
    print("  GT in estimator : NONE")

    print("")

    if pass1_result is not None and pass1_result.success:
        print(
            f"Pass-1 pose       : "
            f"x={float(pass1_result.estimated_x_m):.3f}, "
            f"y={float(pass1_result.estimated_y_m):.3f}, "
            f"h={float(pass1_result.estimated_heading_deg)%360.0:.1f}°"
        )
    else:
        print("Pass-1 pose       : FAILED / unavailable")

    if stage5_result is not None and stage5_result.success:
        est_x = float(stage5_result.estimated_x_m)
        est_y = float(stage5_result.estimated_y_m)
        est_h = float(stage5_result.estimated_heading_deg) % 360.0

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
            f"Stage-5 pose      : "
            f"x={est_x:.3f}, "
            f"y={est_y:.3f}, "
            f"h={est_h:.1f}°"
        )
        print(
            f"Human reference   : "
            f"position diff={pos_diff*100.0:.1f} cm, "
            f"heading diff={heading_diff:.1f}°"
        )
    else:
        reason = (
            "unavailable"
            if stage5_result is None
            else str(stage5_result.failure_reason)
        )
        print(f"Stage-5 pose      : FAILED ({reason})")

    print("")
    print(
        f"D/A rejected      : {len(rejected_da)}"
        + (
            " ["
            + ", ".join(
                f"Tag{int(r['tag_id'])}"
                for r in rejected_da
            )
            + "]"
            if rejected_da
            else ""
        )
    )
    print(
        f"Yaw accepted      : {len(accepted_yaw)} "
        f"(keep={len(accepted_keep)}, flip={len(accepted_flip)})"
    )

    if accepted_flip:
        print(
            "Yaw flips         : "
            + ", ".join(
                f"Tag{int(r['tag_id'])}"
                for r in accepted_flip
            )
        )

    print("")
    print(f"COLLECTION        : {collection_conclusion}")

    if notes:
        print("Notes             : " + "; ".join(notes))

    print(
        "Decision rule      : observable acquisition quality only"
    )
    print(
        "Human reference    : GT-vs-Stage5 difference is display-only; "
        "NOT a KEEP/RETAKE gate"
    )
    print("=" * 82)
    print("")


# ---------------------------------------------------------------------------
# Interactive flow
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("")
    print("=" * 82)
    print("T11: Interactive validation collection with frozen Stage-5")
    print("=" * 82)
    print(f"ROOT_DIR              = {ROOT_DIR}")
    print(f"SCANNER_NAME          = {collector.SCANNER_NAME}")
    print(
        f"PREFERRED_HEADING_DIR = "
        f"{collector.PREFERRED_HEADING_DIR}"
    )
    print(f"LOG_DIR               = {collector.LOG_DIR}")
    print("")
    print("Frozen Stage-5 estimator:")
    print("  distance sigma       = 0.05 m")
    print("  angle sigma          = 2.5 deg")
    print("  yaw sigma            = 3.0 deg")
    print("  weights D/A/Y        = 1.0 / 1.0 / 0.75")
    print("  reject threshold     = 0.80")
    print("  reject min gap       = 0.50")
    print("  max rejection        = 1")
    print("  min |pred yaw|       = 8 deg")
    print("  yaw best error       <= 10 deg")
    print("  yaw branch separation>= 15 deg")
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
        print("-" * 82)
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
        print("-" * 82)

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

    # New Stage-5 post-collection validation display.
    _print_stage5_field_check(
        payload=payload,
        gt_x_m=float(gt_x_m),
        gt_y_m=float(gt_y_m),
        gt_heading_deg=float(actual_heading),
    )
