from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from src.common.geometry import (
    predicted_tag_angle_deg,
    predicted_tag_distance_m,
    wrap_angle_deg,
)
from src.common.solver_result import SolverResult


@dataclass(frozen=True)
class DistanceAngleSolverConfig:
    """
    Solver #2 baseline configuration.

    Residuals are normalized before weighting because distance is in meters
    while angle is in degrees.

    No robust loss and no outlier rejection are used in this baseline.
    """

    x_min_m: float = 0.0
    x_max_m: float = 11.4
    y_min_m: float = 0.0
    y_max_m: float = 11.4

    min_observations: int = 2

    # Baseline normalization scales; tune later through controlled sweeps.
    distance_sigma_m: float = 0.05
    angle_sigma_deg: float = 3.0

    distance_global_weight: float = 1.0
    angle_global_weight: float = 1.0

    heading_starts_deg: Tuple[float, ...] = (
        0.0, 45.0, 90.0, 135.0,
        180.0, 225.0, 270.0, 315.0,
    )

    max_nfev: int = 500
    ftol: float = 1e-10
    xtol: float = 1e-10
    gtol: float = 1e-10


def _require_scipy():
    try:
        from scipy.optimize import least_squares
    except ImportError as exc:
        raise ImportError(
            "Solver #2 requires scipy in the Python environment used by VS Code."
        ) from exc
    return least_squares


def _initial_xy_from_tags(
    observations: List[Dict[str, Any]],
    tag_xy_map: Dict[int, Tuple[float, float]],
    config: DistanceAngleSolverConfig,
) -> Tuple[float, float]:
    pts = [
        tag_xy_map[int(obs["tag_id"])]
        for obs in observations
        if int(obs["tag_id"]) in tag_xy_map
    ]

    if not pts:
        return (
            0.5 * (config.x_min_m + config.x_max_m),
            0.5 * (config.y_min_m + config.y_max_m),
        )

    x0 = sum(p[0] for p in pts) / len(pts)
    y0 = sum(p[1] for p in pts) / len(pts)

    return (
        min(config.x_max_m, max(config.x_min_m, x0)),
        min(config.y_max_m, max(config.y_min_m, y0)),
    )


def solve_distance_angle(
    sample: Dict[str, Any],
    tag_xy_map: Dict[int, Tuple[float, float]],
    config: DistanceAngleSolverConfig,
) -> SolverResult:
    """
    Solver #2 baseline:
      - inputs: calibrated distance + calibrated view angle
      - unknowns: robot x, y, heading
      - front/rear camera offsets and headings are modeled
      - normalized weighted least squares
      - no ground truth
      - no robust loss
      - no outlier rejection
    """
    least_squares = _require_scipy()

    sample_uid = str(sample["sample_uid"])
    observations = list(sample.get("observations") or [])

    usable = []
    missing_tags = []

    for obs in observations:
        tag_id = int(obs["tag_id"])
        if tag_id not in tag_xy_map:
            missing_tags.append(tag_id)
            continue

        measured = obs.get("measured") or {}
        weights = obs.get("weights") or {}

        distance_m = measured.get("distance_m")
        angle_deg = measured.get("angle_deg")
        distance_weight = float(weights.get("distance", 1.0))
        angle_weight = float(weights.get("angle", 1.0))

        if distance_m is None or angle_deg is None:
            continue

        distance_m = float(distance_m)
        angle_deg = float(angle_deg)

        if not math.isfinite(distance_m) or distance_m <= 0.0:
            continue
        if not math.isfinite(angle_deg):
            continue
        if distance_weight <= 0.0 or angle_weight <= 0.0:
            continue

        usable.append(obs)

    tags_input = [int(obs["tag_id"]) for obs in observations]
    tags_used = [int(obs["tag_id"]) for obs in usable]

    if len(usable) < config.min_observations:
        return SolverResult(
            sample_uid=sample_uid,
            success=False,
            failure_reason=(
                f"insufficient_usable_distance_angle_observations:"
                f"{len(usable)}<{config.min_observations}"
            ),
            tags_input=tags_input,
            tags_used=tags_used,
            tags_rejected=[],
            extra={"missing_tag_ids": sorted(set(missing_tags))},
        )

    x0, y0 = _initial_xy_from_tags(usable, tag_xy_map, config)

    def residuals(params):
        robot_x_m = float(params[0])
        robot_y_m = float(params[1])
        robot_heading_deg = float(params[2])

        out = []

        for obs in usable:
            tag_id = int(obs["tag_id"])
            tag_x_m, tag_y_m = tag_xy_map[tag_id]

            measured = obs["measured"]
            weights = obs.get("weights") or {}

            measured_distance_m = float(measured["distance_m"])
            measured_angle_deg = float(measured["angle_deg"])

            distance_weight = float(weights.get("distance", 1.0))
            angle_weight = float(weights.get("angle", 1.0))

            pred_distance_m = predicted_tag_distance_m(
                robot_x_m,
                robot_y_m,
                robot_heading_deg,
                obs["camera_role"],
                tag_x_m,
                tag_y_m,
            )

            pred_angle_deg = predicted_tag_angle_deg(
                robot_x_m,
                robot_y_m,
                robot_heading_deg,
                obs["camera_role"],
                tag_x_m,
                tag_y_m,
            )

            distance_residual = (
                pred_distance_m - measured_distance_m
            ) / config.distance_sigma_m

            angle_residual = wrap_angle_deg(
                pred_angle_deg - measured_angle_deg
            ) / config.angle_sigma_deg

            out.append(
                math.sqrt(
                    config.distance_global_weight * distance_weight
                ) * distance_residual
            )

            out.append(
                math.sqrt(
                    config.angle_global_weight * angle_weight
                ) * angle_residual
            )

        return out

    best = None
    best_cost = math.inf
    total_nfev = 0

    start_time = time.perf_counter()

    for heading0 in config.heading_starts_deg:
        result = least_squares(
            residuals,
            x0=[x0, y0, float(heading0)],
            bounds=(
                [config.x_min_m, config.y_min_m, -360.0],
                [config.x_max_m, config.y_max_m, 720.0],
            ),
            loss="linear",
            max_nfev=config.max_nfev,
            ftol=config.ftol,
            xtol=config.xtol,
            gtol=config.gtol,
        )

        total_nfev += int(result.nfev)

        if math.isfinite(float(result.cost)) and float(result.cost) < best_cost:
            best = result
            best_cost = float(result.cost)

    runtime_ms = (time.perf_counter() - start_time) * 1000.0

    if best is None or not bool(best.success):
        message = "no_successful_multistart_solution"
        if best is not None:
            message += f":{best.message}"

        return SolverResult(
            sample_uid=sample_uid,
            success=False,
            failure_reason=message,
            iterations=total_nfev,
            runtime_ms=runtime_ms,
            objective_value=(None if best is None else best_cost),
            tags_input=tags_input,
            tags_used=tags_used,
            tags_rejected=[],
            extra={"missing_tag_ids": sorted(set(missing_tags))},
        )

    return SolverResult(
        sample_uid=sample_uid,
        success=True,
        estimated_x_m=float(best.x[0]),
        estimated_y_m=float(best.x[1]),
        estimated_heading_deg=float(best.x[2]) % 360.0,
        iterations=total_nfev,
        runtime_ms=runtime_ms,
        objective_value=best_cost,
        tags_input=tags_input,
        tags_used=tags_used,
        tags_rejected=[],
        extra={
            "missing_tag_ids": sorted(set(missing_tags)),
            "best_optimizer_message": str(best.message),
        },
    )
