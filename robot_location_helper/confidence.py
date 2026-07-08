from __future__ import annotations

"""
Observable confidence metrics for robot_location_v1_1.

Important:
    This is NOT a calibrated probability of true localization error.
    Ground truth is unavailable during live operation.

Purpose:
    1. expose interpretable evidence for future debugging
    2. classify HIGH / MEDIUM / LOW confidence conservatively
    3. identify rare cases worth logging

The thresholds below are intentionally explicit and centralized.
They should be replay-audited before state-machine integration.
"""

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from .final_solver import (
    ComponentwiseDistanceAngleYawSolverConfig,
)
from .geometry import (
    predicted_tag_angle_deg,
    predicted_tag_distance_m,
    predicted_tag_yaw_deg,
    wrap_angle_deg,
)


@dataclass(frozen=True)
class ConfidenceConfig:
    # Strong evidence of weak range anchoring.
    low_min_active_distances: int = 3
    low_min_used_tags: int = 3

    # Pass1 -> final stability.
    medium_pass1_final_shift_m: float = 0.06
    low_pass1_final_shift_m: float = 0.12

    # Final normalized residual consistency.
    medium_final_normalized_rms: float = 1.50
    low_final_normalized_rms: float = 2.50

    # Distance-screen stress.
    medium_distance_rejected_fraction: float = 0.50

    # Very narrow tag geometry is a warning, not by itself LOW.
    medium_min_bearing_span_deg: float = 30.0


DEFAULT_CONFIDENCE_CONFIG = ConfidenceConfig()


def _rms(values: List[float]) -> float | None:
    if not values:
        return None
    return math.sqrt(
        sum(float(v) ** 2 for v in values)
        / len(values)
    )


def _max_abs(values: List[float]) -> float | None:
    if not values:
        return None
    return max(abs(float(v)) for v in values)


def _circular_span_deg(values: List[float]) -> float:
    if len(values) <= 1:
        return 0.0

    vals = sorted(
        float(v) % 360.0
        for v in values
    )

    gaps = [
        vals[i + 1] - vals[i]
        for i in range(len(vals) - 1)
    ]
    gaps.append(
        vals[0] + 360.0 - vals[-1]
    )

    return float(360.0 - max(gaps))


def compute_final_residual_metrics(
    *,
    final_sample: Dict[str, Any],
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
    x_m: float,
    y_m: float,
    heading_deg: float,
    solver_config: ComponentwiseDistanceAngleYawSolverConfig,
) -> Dict[str, Any]:
    """
    Recompute residuals at the final pose using only active components.

    Returns both physical-unit and normalized summaries.
    """
    distance_residuals_m: List[float] = []
    angle_residuals_deg: List[float] = []
    yaw_residuals_deg: List[float] = []

    normalized: List[float] = []

    used_tag_bearings: List[float] = []
    used_tag_ids = set()
    front_used = 0
    rear_used = 0

    for obs in list(final_sample.get("observations") or []):
        try:
            tag_id = int(obs["tag_id"])
        except Exception:
            continue

        if tag_id not in tag_pose_map:
            continue

        tag_x, tag_y, tag_yaw = tag_pose_map[tag_id]

        flags = obs.get("flags") or {}
        measured = obs.get("measured") or {}
        weights = obs.get("weights") or {}

        role = str(
            obs.get("camera_role") or ""
        ).strip().lower()

        active_here = False

        if bool(flags.get("distance_use")):
            try:
                d_meas = float(measured["distance_m"])
                wd = float(weights.get("distance", 1.0))
                d_pred = predicted_tag_distance_m(
                    x_m, y_m, heading_deg,
                    role, tag_x, tag_y,
                )
                rd = float(d_pred - d_meas)
                distance_residuals_m.append(rd)
                normalized.append(
                    math.sqrt(
                        solver_config.distance_global_weight
                        * wd
                    )
                    * rd
                    / solver_config.distance_sigma_m
                )
                active_here = True
            except Exception:
                pass

        if bool(flags.get("angle_use")):
            try:
                a_meas = float(measured["angle_deg"])
                wa = float(weights.get("angle", 1.0))
                a_pred = predicted_tag_angle_deg(
                    x_m, y_m, heading_deg,
                    role, tag_x, tag_y,
                )
                ra = wrap_angle_deg(
                    a_pred - a_meas
                )
                angle_residuals_deg.append(ra)
                normalized.append(
                    math.sqrt(
                        solver_config.angle_global_weight
                        * wa
                    )
                    * ra
                    / solver_config.angle_sigma_deg
                )
                active_here = True
            except Exception:
                pass

        if bool(flags.get("yaw_use")):
            try:
                y_meas = float(
                    measured["yaw_resolved_deg"]
                )
                wy = float(weights.get("yaw", 1.0))

                if tag_yaw is not None:
                    y_pred = predicted_tag_yaw_deg(
                        x_m, y_m, heading_deg,
                        role, tag_x, tag_y, tag_yaw,
                    )
                    ry = wrap_angle_deg(
                        y_pred - y_meas
                    )
                    yaw_residuals_deg.append(ry)
                    normalized.append(
                        math.sqrt(
                            solver_config.yaw_global_weight
                            * wy
                        )
                        * ry
                        / solver_config.yaw_sigma_deg
                    )
                    active_here = True
            except Exception:
                pass

        if active_here:
            used_tag_ids.add(tag_id)

            if role == "front":
                front_used += 1
            elif role == "rear":
                rear_used += 1

            used_tag_bearings.append(
                math.degrees(
                    math.atan2(
                        float(tag_y) - float(y_m),
                        float(tag_x) - float(x_m),
                    )
                ) % 360.0
            )

    active_component_count = len(normalized)

    return {
        "final_distance_rms_m":
            _rms(distance_residuals_m),

        "final_distance_max_abs_m":
            _max_abs(distance_residuals_m),

        "final_angle_rms_deg":
            _rms(angle_residuals_deg),

        "final_angle_max_abs_deg":
            _max_abs(angle_residuals_deg),

        "final_yaw_rms_deg":
            _rms(yaw_residuals_deg),

        "final_yaw_max_abs_deg":
            _max_abs(yaw_residuals_deg),

        "final_normalized_rms":
            _rms(normalized),

        "active_component_count":
            active_component_count,

        "used_tag_count":
            len(used_tag_ids),

        "front_used_count":
            front_used,

        "rear_used_count":
            rear_used,

        "used_tag_bearing_span_deg":
            _circular_span_deg(
                used_tag_bearings
            ),
    }


def assess_location_confidence(
    *,
    metrics: Dict[str, Any],
    config: ConfidenceConfig = DEFAULT_CONFIDENCE_CONFIG,
) -> Dict[str, Any]:
    """
    Classify HIGH / MEDIUM / LOW from observable evidence only.

    LOW is reserved for strong warning evidence.
    MEDIUM means usable but worth caution.
    HIGH means no configured warning threshold fired.

    This is intentionally interpretable rather than probabilistic.
    """
    warnings: List[str] = []
    reasons: List[str] = []

    active_d = int(
        metrics.get("active_distance_count") or 0
    )
    used_tags = int(
        metrics.get("used_tag_count") or 0
    )

    shift_m = metrics.get(
        "pass1_to_final_shift_m"
    )
    norm_rms = metrics.get(
        "final_normalized_rms"
    )
    rejected_fraction = metrics.get(
        "distance_rejected_fraction"
    )
    bearing_span = metrics.get(
        "used_tag_bearing_span_deg"
    )

    low = False
    medium = False

    if active_d < config.low_min_active_distances:
        warnings.append(
            "WEAK_DISTANCE_SUPPORT"
        )
        low = True
    else:
        reasons.append(
            "ADEQUATE_DISTANCE_SUPPORT"
        )

    if used_tags < config.low_min_used_tags:
        warnings.append(
            "TOO_FEW_USED_TAGS"
        )
        low = True
    else:
        reasons.append(
            "ADEQUATE_TAG_SUPPORT"
        )

    if shift_m is not None:
        shift_m = float(shift_m)

        if shift_m > config.low_pass1_final_shift_m:
            warnings.append(
                "LARGE_PASS1_FINAL_POSITION_SHIFT"
            )
            low = True

        elif shift_m > config.medium_pass1_final_shift_m:
            warnings.append(
                "MODERATE_PASS1_FINAL_POSITION_SHIFT"
            )
            medium = True

        else:
            reasons.append(
                "STABLE_PASS1_TO_FINAL"
            )

    if norm_rms is not None:
        norm_rms = float(norm_rms)

        if norm_rms > config.low_final_normalized_rms:
            warnings.append(
                "POOR_FINAL_RESIDUAL_CONSISTENCY"
            )
            low = True

        elif norm_rms > config.medium_final_normalized_rms:
            warnings.append(
                "ELEVATED_FINAL_RESIDUALS"
            )
            medium = True

        else:
            reasons.append(
                "CONSISTENT_FINAL_RESIDUALS"
            )

    if bool(
        metrics.get("distance_floor_activated")
    ):
        warnings.append(
            "M2_DISTANCE_FLOOR_ACTIVATED"
        )
        medium = True

    if rejected_fraction is not None:
        if float(rejected_fraction) >= (
            config.medium_distance_rejected_fraction
        ):
            warnings.append(
                "HIGH_DISTANCE_REJECTION_FRACTION"
            )
            medium = True

    if bearing_span is not None:
        if float(bearing_span) < (
            config.medium_min_bearing_span_deg
        ):
            warnings.append(
                "NARROW_USED_TAG_BEARING_SPAN"
            )
            medium = True
        else:
            reasons.append(
                "ADEQUATE_TAG_GEOMETRY_SPAN"
            )

    if low:
        level = "LOW"
    elif medium:
        level = "MEDIUM"
    else:
        level = "HIGH"

    return {
        "level": level,
        "reason_codes": reasons,
        "warning_codes": warnings,
        "metrics": metrics,
        "config": {
            "low_min_active_distances":
                config.low_min_active_distances,

            "low_min_used_tags":
                config.low_min_used_tags,

            "medium_pass1_final_shift_m":
                config.medium_pass1_final_shift_m,

            "low_pass1_final_shift_m":
                config.low_pass1_final_shift_m,

            "medium_final_normalized_rms":
                config.medium_final_normalized_rms,

            "low_final_normalized_rms":
                config.low_final_normalized_rms,

            "medium_distance_rejected_fraction":
                config.medium_distance_rejected_fraction,

            "medium_min_bearing_span_deg":
                config.medium_min_bearing_span_deg,
        },
    }


def rare_case_reasons_from_confidence(
    confidence: Dict[str, Any],
) -> List[str]:
    """
    Convert confidence evidence into sparse-log triggers.

    HIGH ordinary cases produce no trigger unless a specific warning exists.
    """
    out: List[str] = []

    level = str(
        confidence.get("level") or ""
    ).upper()

    if level == "LOW":
        out.append(
            "LOW_LOCATION_CONFIDENCE"
        )

    for code in list(
        confidence.get("warning_codes") or []
    ):
        if code in {
            "M2_DISTANCE_FLOOR_ACTIVATED",
            "HIGH_DISTANCE_REJECTION_FRACTION",
            "LARGE_PASS1_FINAL_POSITION_SHIFT",
            "POOR_FINAL_RESIDUAL_CONSISTENCY",
            "WEAK_DISTANCE_SUPPORT",
            "TOO_FEW_USED_TAGS",
        }:
            out.append(str(code))

    # Stable order, no duplicates.
    return list(dict.fromkeys(out))
