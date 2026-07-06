from __future__ import annotations

"""
t11_collect_validation_sample.py

Interactive validation collector built as a thin companion around the proven
`t11_collect_observation_sample.py`.

The existing collection flow is preserved:
  1. enter x_m / y_m
  2. look up preferred heading
  3. rotate robot
  4. press Enter
  5. collect one fresh mobility.report.location report
  6. print the existing per-tag measurement table

This companion then adds:
  - observable-only measurement screening
  - quick distance+angle pose estimate (NO ground truth in solver)
  - entered-vs-estimated pose difference for human inspection only
  - short KEEP / KEEP_WITH_CAUTION / RETAKE conclusion

Important validation rule:
  The automatic collection conclusion does NOT use GT-vs-estimate error.
"""

import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------

# This file is intended to live beside:
#   testLocation\t11_collect_observation_sample.py
THIS_DIR = Path(__file__).resolve().parent
ROOT_DIR = THIS_DIR.parent
LOCATION_SOLVER_DIR = ROOT_DIR / "locationSolver"

for p in (ROOT_DIR, LOCATION_SOLVER_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


# ---------------------------------------------------------------------------
# Reuse the proven collection script. Do not duplicate/rewrite collection flow.
# ---------------------------------------------------------------------------

import t11_collect_observation_sample as collector


# ---------------------------------------------------------------------------
# Location-solver imports
# ---------------------------------------------------------------------------

from locationSolver.src.common.tag_map_loader import load_tag_xy_map
from locationSolver.src.solvers.solver_2_distance_angle import (
    DistanceAngleSolverConfig,
    solve_distance_angle,
)


# ---------------------------------------------------------------------------
# Field-check settings
# ---------------------------------------------------------------------------

TAG_MAP_PATH = (
    ROOT_DIR
    / "sitemap"
    / "DemoRoom"
    / "tag_location.txt"
)

# Established observable geometry gates.
FRONT_MAX_DISTANCE_M = 5.5
REAR_MAX_DISTANCE_M = 3.0

FRONT_MAX_ABS_ANGLE_DEG = 30.0
REAR_MAX_ABS_ANGLE_DEG = 15.0

# Warnings only.
FRONT_NEAR_EDGE_DEG = 25.0
REAR_NEAR_EDGE_DEG = 12.0

MIN_USABLE_OBSERVATIONS = 4
MIN_UNIQUE_TAGS = 4
PREFERRED_USABLE_OBSERVATIONS = 5

# Quick diagnostic solver:
# distance + view angle only.
# These are the frozen candidate-v1 distance/angle scales.
QUICK_DISTANCE_SIGMA_M = 0.05
QUICK_ANGLE_SIGMA_DEG = 2.5


# ---------------------------------------------------------------------------
# Small helpers
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
    meas = obs.get("measured") or {}

    d = _safe_float(meas.get("distance_m"))
    a = _safe_float(meas.get("angle_deg"))

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


def _build_quick_solver_sample(
    observations: List[Dict[str, Any]],
) -> Dict[str, Any]:
    out = []

    for idx, obs in enumerate(observations, start=1):
        meas = obs.get("measured") or {}

        out.append({
            "observation_uid": f"field_obs_{idx:03d}",
            "tag_id": int(obs["tag_id"]),
            "camera_role": str(
                obs.get("camera_role") or ""
            ).strip().lower(),
            "measured": {
                "distance_m": float(meas["distance_m"]),
                "angle_deg": float(meas["angle_deg"]),
            },
            "weights": {
                "distance": 1.0,
                "angle": 1.0,
            },
            "flags": {},
        })

    return {
        "sample_uid": "interactive_validation_field_check",
        "observations": out,
        "observation_count": len(out),
    }


def _quick_pose_estimate(
    usable_observations: List[Dict[str, Any]],
):
    if len(usable_observations) < 2:
        return None

    tag_xy_map = load_tag_xy_map(TAG_MAP_PATH)

    sample = _build_quick_solver_sample(
        usable_observations
    )

    config = DistanceAngleSolverConfig(
        distance_sigma_m=QUICK_DISTANCE_SIGMA_M,
        angle_sigma_deg=QUICK_ANGLE_SIGMA_DEG,
        distance_global_weight=1.0,
        angle_global_weight=1.0,
    )

    return solve_distance_angle(
        sample=sample,
        tag_xy_map=tag_xy_map,
        config=config,
    )


def _print_short_conclusion(
    payload: Dict[str, Any],
    gt_x_m: float,
    gt_y_m: float,
    gt_heading_deg: float,
) -> None:
    observations = list(payload.get("observations") or [])

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
        if str(obs.get("camera_role") or "").lower()
        == "front"
    )
    rear_count = sum(
        1 for obs in usable
        if str(obs.get("camera_role") or "").lower()
        == "rear"
    )

    result = _quick_pose_estimate(usable)

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

    if result is None or not result.success:
        hard_reasons.append(
            "quick distance+angle solver failed"
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
        edge_fraction = near_edge_count / len(usable)
        if edge_fraction > 0.50:
            cautions.append(
                f"near-edge fraction={edge_fraction:.0%}"
            )

    if hard_reasons:
        conclusion = "RETAKE"
    elif cautions:
        conclusion = "KEEP_WITH_CAUTION"
    else:
        conclusion = "KEEP"

    print("")
    print("=" * 78)
    print("VALIDATION FIELD CHECK")
    print("=" * 78)
    print(
        f"Observed / usable : "
        f"{len(observations)} / {len(usable)}"
    )
    print(
        f"Unique tags       : {unique_tags}"
    )
    print(
        f"Usable front/rear : "
        f"{front_count} / {rear_count}"
    )
    print(
        f"Near-edge usable  : {near_edge_count}"
    )

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

    if result is not None and result.success:
        est_x = float(result.estimated_x_m)
        est_y = float(result.estimated_y_m)
        est_h = float(result.estimated_heading_deg) % 360.0

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
            "Quick Solver      : "
            "distance + view angle only; GT NOT used"
        )
        print(
            f"Entered pose      : "
            f"x={gt_x_m:.3f}, "
            f"y={gt_y_m:.3f}, "
            f"h={gt_heading_deg:.1f} deg"
        )
        print(
            f"Estimated pose    : "
            f"x={est_x:.3f}, "
            f"y={est_y:.3f}, "
            f"h={est_h:.1f} deg"
        )
        print(
            f"Human reference   : "
            f"position diff={pos_diff*100.0:.1f} cm, "
            f"heading diff={heading_diff:.1f} deg"
        )
    else:
        reason = (
            "not enough usable data"
            if result is None
            else str(result.failure_reason)
        )
        print(
            f"Quick Solver      : FAILED ({reason})"
        )

    print("")
    print(
        f"COLLECTION        : {conclusion}"
    )

    if hard_reasons:
        print(
            "Why retake        : "
            + "; ".join(hard_reasons)
        )

    if cautions:
        print(
            "Cautions          : "
            + "; ".join(cautions)
        )

    print(
        "Decision rule      : observable quality only; "
        "GT-vs-estimate error is NOT a KEEP/RETAKE gate"
    )
    print("=" * 78)
    print("")


# ---------------------------------------------------------------------------
# Interactive flow
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("")
    print("=" * 78)
    print("T11: Interactive validation measurement + quick solver check")
    print("=" * 78)
    print(f"ROOT_DIR              = {ROOT_DIR}")
    print(f"SCANNER_NAME          = {collector.SCANNER_NAME}")
    print(
        f"PREFERRED_HEADING_DIR = "
        f"{collector.PREFERRED_HEADING_DIR}"
    )
    print(f"LOG_DIR               = {collector.LOG_DIR}")
    print(
        "Quick Solver         = distance + angle only "
        "(no GT, no offline yaw labels)"
    )
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
        print("-" * 78)
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
        print("-" * 78)

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

    # New concise post-collection field check.
    _print_short_conclusion(
        payload=payload,
        gt_x_m=float(gt_x_m),
        gt_y_m=float(gt_y_m),
        gt_heading_deg=float(actual_heading),
    )
