"""
location_confidence.py

Part 8 of the location solver:
Confidence and diversity report.

Purpose
-------
After item-level filtering and final solve, classify the result as:

    GOOD
    MARGINAL
    BAD
    FAILED

This module does not solve pose. It only evaluates result quality.

The confidence decision is based on:
    - enabled distance item count
    - enabled angle item count
    - bearing diversity of distance/angle tags
    - final distance residual RMS
    - final angle residual RMS
    - yaw coherence, as weak diagnostic only
    - solver ok/fail status

This module contains reusable production-evolving logic only.
It does not read/write files and does not use pandas.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
import math
from typing import Iterable, Mapping

from .location_geometry import bearing_world_deg, RobotPose


@dataclass(frozen=True)
class ConfidenceConfig:
    # GOOD thresholds.
    good_min_distance_items: int = 3
    good_min_angle_items: int = 2
    good_min_distance_bearing_span_deg: float = 45.0
    good_min_angle_bearing_span_deg: float = 30.0
    good_max_distance_rms_cm: float = 8.0
    good_max_angle_rms_deg: float = 6.0

    # MARGINAL thresholds.
    marginal_min_distance_items: int = 2
    marginal_min_angle_items: int = 1
    marginal_min_distance_bearing_span_deg: float = 25.0
    marginal_max_distance_rms_cm: float = 15.0
    marginal_max_angle_rms_deg: float = 12.0

    # Diagnostic yaw threshold. Yaw does not dominate confidence.
    yaw_coherent_rms_deg: float = 20.0

    # Gross failure thresholds.
    bad_max_distance_rms_cm: float = 30.0
    bad_max_angle_rms_deg: float = 25.0


@dataclass(frozen=True)
class ConfidenceReport:
    confidence: str
    ok_for_true_location_update: bool
    ok_for_correction_motion: bool

    reason: str

    distance_items: int
    angle_items: int
    yaw_items: int

    distance_tag_count: int
    angle_tag_count: int
    yaw_tag_count: int

    distance_bearing_span_deg: float
    angle_bearing_span_deg: float

    distance_rms_cm: float
    distance_max_abs_cm: float
    angle_rms_deg: float
    angle_max_abs_deg: float
    yaw_rms_deg: float
    yaw_max_abs_deg: float

    solver_ok: bool
    solver_detail: str

    def to_dict(self) -> dict:
        return asdict(self)


def _deg_norm_360(a: float) -> float:
    return float(a % 360.0)


def _circular_span_deg(angles_deg: list[float]) -> float:
    if len(angles_deg) <= 1:
        return 0.0

    vals = sorted(_deg_norm_360(v) for v in angles_deg)
    gaps = []
    for i in range(len(vals) - 1):
        gaps.append(vals[i + 1] - vals[i])
    gaps.append(vals[0] + 360.0 - vals[-1])
    return float(360.0 - max(gaps))


def _enabled_residuals_by_kind(final_result, kind: str):
    """
    Extract residual records by kind from JointYawSolveResult-style residuals.

    final_result.residuals contains one record per tag/camera with
    distance/angle/yaw residual and effective weights.
    """
    vals = []

    for r in final_result.residuals:
        if kind == "distance" and r.distance_weight > 0.0:
            vals.append(r)
        elif kind == "angle" and r.angle_weight > 0.0:
            vals.append(r)
        elif kind == "yaw" and r.yaw_weight > 0.0:
            vals.append(r)

    return vals


def _unique_tags(records) -> set[int]:
    return {int(r.tag_id) for r in records}


def _bearing_span_for_records(
    records,
    robot_x_m: float,
    robot_y_m: float,
    tag_map,
) -> float:
    bearings = []
    seen_tags = set()

    for r in records:
        tag_id = int(r.tag_id)
        if tag_id in seen_tags:
            continue
        if tag_id not in tag_map:
            continue
        seen_tags.add(tag_id)
        tag = tag_map[tag_id]
        bearings.append(bearing_world_deg(robot_x_m, robot_y_m, tag.x_m, tag.y_m))

    return _circular_span_deg(bearings)


def evaluate_pose_confidence(
    final_result,
    tag_map,
    config: ConfidenceConfig | None = None,
) -> ConfidenceReport:
    """
    Evaluate final filtered solve result and return GOOD/MARGINAL/BAD/FAILED.

    final_result is expected to be a JointYawSolveResult-compatible object from
    location_filtered_solver.solve_filtered_pose().
    """
    cfg = config or ConfidenceConfig()

    if not final_result.ok:
        return ConfidenceReport(
            confidence="FAILED",
            ok_for_true_location_update=False,
            ok_for_correction_motion=False,
            reason=f"solver_failed: {final_result.detail}",
            distance_items=0,
            angle_items=0,
            yaw_items=0,
            distance_tag_count=0,
            angle_tag_count=0,
            yaw_tag_count=0,
            distance_bearing_span_deg=0.0,
            angle_bearing_span_deg=0.0,
            distance_rms_cm=float("nan"),
            distance_max_abs_cm=float("nan"),
            angle_rms_deg=float("nan"),
            angle_max_abs_deg=float("nan"),
            yaw_rms_deg=float("nan"),
            yaw_max_abs_deg=float("nan"),
            solver_ok=False,
            solver_detail=final_result.detail,
        )

    d_records = _enabled_residuals_by_kind(final_result, "distance")
    a_records = _enabled_residuals_by_kind(final_result, "angle")
    y_records = _enabled_residuals_by_kind(final_result, "yaw")

    distance_items = len(d_records)
    angle_items = len(a_records)
    yaw_items = len(y_records)

    distance_tag_count = len(_unique_tags(d_records))
    angle_tag_count = len(_unique_tags(a_records))
    yaw_tag_count = len(_unique_tags(y_records))

    distance_span = _bearing_span_for_records(
        d_records, final_result.x_m, final_result.y_m, tag_map
    )
    angle_span = _bearing_span_for_records(
        a_records, final_result.x_m, final_result.y_m, tag_map
    )

    distance_rms_cm = float(final_result.distance_rms_cm)
    distance_max_abs_cm = float(final_result.distance_max_abs_cm)
    angle_rms_deg = float(final_result.angle_rms_deg)
    angle_max_abs_deg = float(final_result.angle_max_abs_deg)
    yaw_rms_deg = float(final_result.yaw_rms_deg)
    yaw_max_abs_deg = float(final_result.yaw_max_abs_deg)

    yaw_coherent = (
        math.isfinite(yaw_rms_deg)
        and yaw_items > 0
        and yaw_rms_deg <= cfg.yaw_coherent_rms_deg
    )

    # FAILED: not enough core information.
    if distance_items < cfg.marginal_min_distance_items:
        return ConfidenceReport(
            confidence="FAILED",
            ok_for_true_location_update=False,
            ok_for_correction_motion=False,
            reason="not_enough_distance_items",
            distance_items=distance_items,
            angle_items=angle_items,
            yaw_items=yaw_items,
            distance_tag_count=distance_tag_count,
            angle_tag_count=angle_tag_count,
            yaw_tag_count=yaw_tag_count,
            distance_bearing_span_deg=distance_span,
            angle_bearing_span_deg=angle_span,
            distance_rms_cm=distance_rms_cm,
            distance_max_abs_cm=distance_max_abs_cm,
            angle_rms_deg=angle_rms_deg,
            angle_max_abs_deg=angle_max_abs_deg,
            yaw_rms_deg=yaw_rms_deg,
            yaw_max_abs_deg=yaw_max_abs_deg,
            solver_ok=True,
            solver_detail=final_result.detail,
        )

    if angle_items < cfg.marginal_min_angle_items:
        return ConfidenceReport(
            confidence="FAILED",
            ok_for_true_location_update=False,
            ok_for_correction_motion=False,
            reason="not_enough_angle_items_for_heading",
            distance_items=distance_items,
            angle_items=angle_items,
            yaw_items=yaw_items,
            distance_tag_count=distance_tag_count,
            angle_tag_count=angle_tag_count,
            yaw_tag_count=yaw_tag_count,
            distance_bearing_span_deg=distance_span,
            angle_bearing_span_deg=angle_span,
            distance_rms_cm=distance_rms_cm,
            distance_max_abs_cm=distance_max_abs_cm,
            angle_rms_deg=angle_rms_deg,
            angle_max_abs_deg=angle_max_abs_deg,
            yaw_rms_deg=yaw_rms_deg,
            yaw_max_abs_deg=yaw_max_abs_deg,
            solver_ok=True,
            solver_detail=final_result.detail,
        )

    # GOOD.
    if (
        distance_items >= cfg.good_min_distance_items
        and angle_items >= cfg.good_min_angle_items
        and distance_span >= cfg.good_min_distance_bearing_span_deg
        and angle_span >= cfg.good_min_angle_bearing_span_deg
        and distance_rms_cm <= cfg.good_max_distance_rms_cm
        and angle_rms_deg <= cfg.good_max_angle_rms_deg
    ):
        reason = "good_distance_angle_geometry"
        if yaw_items > 0:
            reason += "_yaw_coherent" if yaw_coherent else "_yaw_not_coherent_but_weak"
        return ConfidenceReport(
            confidence="GOOD",
            ok_for_true_location_update=True,
            ok_for_correction_motion=True,
            reason=reason,
            distance_items=distance_items,
            angle_items=angle_items,
            yaw_items=yaw_items,
            distance_tag_count=distance_tag_count,
            angle_tag_count=angle_tag_count,
            yaw_tag_count=yaw_tag_count,
            distance_bearing_span_deg=distance_span,
            angle_bearing_span_deg=angle_span,
            distance_rms_cm=distance_rms_cm,
            distance_max_abs_cm=distance_max_abs_cm,
            angle_rms_deg=angle_rms_deg,
            angle_max_abs_deg=angle_max_abs_deg,
            yaw_rms_deg=yaw_rms_deg,
            yaw_max_abs_deg=yaw_max_abs_deg,
            solver_ok=True,
            solver_detail=final_result.detail,
        )

    # MARGINAL.
    if (
        distance_items >= cfg.marginal_min_distance_items
        and angle_items >= cfg.marginal_min_angle_items
        and distance_span >= cfg.marginal_min_distance_bearing_span_deg
        and distance_rms_cm <= cfg.marginal_max_distance_rms_cm
        and angle_rms_deg <= cfg.marginal_max_angle_rms_deg
    ):
        return ConfidenceReport(
            confidence="MARGINAL",
            ok_for_true_location_update=True,
            ok_for_correction_motion=False,
            reason="marginal_but_usable_for_true_location_not_correction",
            distance_items=distance_items,
            angle_items=angle_items,
            yaw_items=yaw_items,
            distance_tag_count=distance_tag_count,
            angle_tag_count=angle_tag_count,
            yaw_tag_count=yaw_tag_count,
            distance_bearing_span_deg=distance_span,
            angle_bearing_span_deg=angle_span,
            distance_rms_cm=distance_rms_cm,
            distance_max_abs_cm=distance_max_abs_cm,
            angle_rms_deg=angle_rms_deg,
            angle_max_abs_deg=angle_max_abs_deg,
            yaw_rms_deg=yaw_rms_deg,
            yaw_max_abs_deg=yaw_max_abs_deg,
            solver_ok=True,
            solver_detail=final_result.detail,
        )

    # BAD vs FAILED: solution exists but not trustworthy.
    if (
        distance_rms_cm > cfg.bad_max_distance_rms_cm
        or angle_rms_deg > cfg.bad_max_angle_rms_deg
    ):
        reason = "residuals_too_large"
    elif distance_span < cfg.marginal_min_distance_bearing_span_deg:
        reason = "poor_distance_geometry_diversity"
    else:
        reason = "does_not_meet_good_or_marginal_thresholds"

    return ConfidenceReport(
        confidence="BAD",
        ok_for_true_location_update=False,
        ok_for_correction_motion=False,
        reason=reason,
        distance_items=distance_items,
        angle_items=angle_items,
        yaw_items=yaw_items,
        distance_tag_count=distance_tag_count,
        angle_tag_count=angle_tag_count,
        yaw_tag_count=yaw_tag_count,
        distance_bearing_span_deg=distance_span,
        angle_bearing_span_deg=angle_span,
        distance_rms_cm=distance_rms_cm,
        distance_max_abs_cm=distance_max_abs_cm,
        angle_rms_deg=angle_rms_deg,
        angle_max_abs_deg=angle_max_abs_deg,
        yaw_rms_deg=yaw_rms_deg,
        yaw_max_abs_deg=yaw_max_abs_deg,
        solver_ok=True,
        solver_detail=final_result.detail,
    )
