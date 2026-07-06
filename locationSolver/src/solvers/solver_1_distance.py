from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from src.common.geometry import predicted_tag_distance_m
from src.common.solver_result import SolverResult


@dataclass(frozen=True)
class DistanceSolverConfig:
    """
    Fixed baseline configuration for Solver #1.

    No robust loss and no outlier rejection are used here.
    """

    x_min_m: float = 0.0
    x_max_m: float = 11.4
    y_min_m: float = 0.0
    y_max_m: float = 11.4

    min_observations: int = 3

    # Multi-start heading seeds. This is important because camera offsets
    # make heading enter the distance model only weakly.
    heading_starts_deg: Tuple[float, ...] = (0.0, 90.0, 180.0, 270.0)

    max_nfev: int = 500
    ftol: float = 1e-10
    xtol: float = 1e-10
    gtol: float = 1e-10


def _require_scipy():
    try:
        from scipy.optimize import least_squares
    except ImportError as exc:
        raise ImportError(
            "Solver #1 requires scipy. Install it into the same Python "
            "environment used by VS Code, e.g. pip install scipy"
        ) from exc
    return least_squares


def _initial_xy_from_tags(
    observations: List[Dict[str, Any]],
    tag_xy_map: Dict[int, Tuple[float, float]],
    config: DistanceSolverConfig,
) -> Tuple[float, float]:
    """
    Use only known tag coordinates for initialization.
    Ground truth is never used.
    """
    pts = []
    for obs in observations:
        tag_id = int(obs["tag_id"])
        if tag_id in tag_xy_map:
            pts.append(tag_xy_map[tag_id])

    if not pts:
        return (
            0.5 * (config.x_min_m + config.x_max_m),
            0.5 * (config.y_min_m + config.y_max_m),
        )

    x0 = sum(p[0] for p in pts) / len(pts)
    y0 = sum(p[1] for p in pts) / len(pts)

    x0 = min(config.x_max_m, max(config.x_min_m, x0))
    y0 = min(config.y_max_m, max(config.y_min_m, y0))
    return x0, y0


def solve_distance_only(
    sample: Dict[str, Any],
    tag_xy_map: Dict[int, Tuple[float, float]],
    config: DistanceSolverConfig,
) -> SolverResult:
    """
    Solver #1 baseline:
      - inputs: calibrated distance only
      - unknowns: robot x, y, heading
      - model includes front/rear camera center offsets
      - weighted ordinary least squares
      - no outlier rejection
      - no ground-truth use
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
        weight = weights.get("distance", 1.0)

        if distance_m is None:
            continue

        distance_m = float(distance_m)
        weight = float(weight)

        if not math.isfinite(distance_m) or distance_m <= 0.0:
            continue
        if not math.isfinite(weight) or weight <= 0.0:
            continue

        usable.append(obs)

    tags_input = [int(obs["tag_id"]) for obs in observations]
    tags_used = [int(obs["tag_id"]) for obs in usable]

    if len(usable) < config.min_observations:
        return SolverResult(
            sample_uid=sample_uid,
            success=False,
            failure_reason=(
                f"insufficient_usable_distance_observations:"
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

            measured_distance_m = float(
                obs["measured"]["distance_m"]
            )
            weight = float(
                (obs.get("weights") or {}).get("distance", 1.0)
            )

            predicted_distance_m = predicted_tag_distance_m(
                robot_x_m=robot_x_m,
                robot_y_m=robot_y_m,
                robot_heading_deg=robot_heading_deg,
                camera_role=obs["camera_role"],
                tag_x_m=tag_x_m,
                tag_y_m=tag_y_m,
            )

            # sqrt(weight) gives the conventional weighted least-squares form.
            out.append(
                math.sqrt(weight)
                * (predicted_distance_m - measured_distance_m)
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

    est_x_m = float(best.x[0])
    est_y_m = float(best.x[1])
    est_heading_deg = float(best.x[2]) % 360.0

    return SolverResult(
        sample_uid=sample_uid,
        success=True,
        estimated_x_m=est_x_m,
        estimated_y_m=est_y_m,
        estimated_heading_deg=est_heading_deg,
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
