"""
location_distance_solver.py

Part 4 of the location solver:
Distance-only robot-position solver.

Purpose
-------
Estimate robot center position (x, y) using distance measurement items only.

Important limitation
--------------------
Distance-only solving does NOT solve robot heading.

The camera position depends weakly on heading because the front/rear cameras
are offset from robot center. Therefore this solver requires a fixed heading.

In normal production use, this fixed heading should usually be:
    preferred localization heading from lookup table
or:
    planned heading / last known heading

In this Part 4 test, we use ground-truth heading only as a diagnostic to test
the distance-only solver mechanics.

This module is reusable production-evolving code.
It contains no CSV logic, no pandas, no output files, and no argparse.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
import math
from typing import Iterable, Mapping, Sequence

from .location_geometry import RobotPose, predict_tag_measurement
from .location_items import TagObservation


@dataclass(frozen=True)
class DistanceSolverConfig:
    """
    Config for distance-only solver.

    sigma_distance_m:
        residual normalization scale. 0.05 means 5 cm.

    search_initial_step_m:
        initial coordinate-search step size.

    search_min_step_m:
        stop when step becomes smaller than this.

    max_iterations:
        maximum coordinate-search iterations.

    robust_clip_sigma:
        Huber-like clipping threshold in normalized residual units.
    """
    sigma_distance_m: float = 0.05
    search_initial_step_m: float = 0.20
    search_min_step_m: float = 0.002
    max_iterations: int = 200
    robust_clip_sigma: float = 3.0

    x_min: float = -1.0
    x_max: float = 12.5
    y_min: float = -1.0
    y_max: float = 12.5


@dataclass(frozen=True)
class DistanceResidualRecord:
    tag_id: int
    camera_role: str
    measured_distance_m: float
    predicted_distance_m: float
    residual_m: float
    residual_cm: float
    weight: float


@dataclass(frozen=True)
class DistanceSolveResult:
    ok: bool
    x_m: float
    y_m: float
    heading_deg: float
    cost: float
    iterations: int
    evaluations: int
    used_distance_count: int
    distance_rms_m: float
    distance_rms_cm: float
    distance_max_abs_m: float
    distance_max_abs_cm: float
    residuals: list[DistanceResidualRecord]
    detail: str

    def to_dict(self) -> dict:
        d = asdict(self)
        d["residuals"] = [asdict(r) for r in self.residuals]
        return d


def _huber_cost(z: float, clip: float) -> float:
    """
    Huber-like cost for normalized residual z.
    """
    az = abs(z)
    if az <= clip:
        return 0.5 * z * z
    return clip * (az - 0.5 * clip)


def _camera_distance_weight(
    camera_role: str,
    measurement_weights: Mapping[str, Mapping[str, float]],
) -> float:
    cam = str(camera_role).strip().lower()
    return float(measurement_weights.get(cam, {}).get("distance", 0.0))


def compute_distance_residuals(
    x_m: float,
    y_m: float,
    fixed_heading_deg: float,
    observations: Iterable[TagObservation],
    tag_map,
    camera_config: Mapping[str, Mapping[str, float]],
    measurement_weights: Mapping[str, Mapping[str, float]],
) -> list[DistanceResidualRecord]:
    """
    Compute distance residuals for all valid observations.

    Residual convention:
        residual = measured - predicted
    """
    robot_pose = RobotPose(x_m=float(x_m), y_m=float(y_m), heading_deg=float(fixed_heading_deg))
    residuals: list[DistanceResidualRecord] = []

    for obs in observations:
        camera_role = str(obs.camera_role).strip().lower()

        if obs.tag_id not in tag_map:
            continue
        if camera_role not in camera_config:
            continue

        weight = _camera_distance_weight(camera_role, measurement_weights)
        if weight <= 0.0:
            continue

        tag_world = tag_map[obs.tag_id]
        pred = predict_tag_measurement(
            robot_pose=robot_pose,
            tag_world=tag_world,
            camera_cfg=camera_config[camera_role],
        )

        residual_m = float(obs.distance_m - pred.distance_m)

        residuals.append(
            DistanceResidualRecord(
                tag_id=int(obs.tag_id),
                camera_role=camera_role,
                measured_distance_m=float(obs.distance_m),
                predicted_distance_m=float(pred.distance_m),
                residual_m=residual_m,
                residual_cm=100.0 * residual_m,
                weight=weight,
            )
        )

    return residuals


def distance_only_cost(
    x_m: float,
    y_m: float,
    fixed_heading_deg: float,
    observations: Iterable[TagObservation],
    tag_map,
    camera_config: Mapping[str, Mapping[str, float]],
    measurement_weights: Mapping[str, Mapping[str, float]],
    solver_config: DistanceSolverConfig,
) -> float:
    """
    Weighted robust distance-only cost.
    """
    residuals = compute_distance_residuals(
        x_m=x_m,
        y_m=y_m,
        fixed_heading_deg=fixed_heading_deg,
        observations=observations,
        tag_map=tag_map,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
    )

    if not residuals:
        return float("inf")

    total = 0.0
    for r in residuals:
        z = r.residual_m / solver_config.sigma_distance_m
        total += r.weight * _huber_cost(z, solver_config.robust_clip_sigma)

    return float(total)


def _summarize_residuals(residuals: list[DistanceResidualRecord]) -> dict:
    if not residuals:
        return {
            "distance_rms_m": float("nan"),
            "distance_rms_cm": float("nan"),
            "distance_max_abs_m": float("nan"),
            "distance_max_abs_cm": float("nan"),
        }

    vals = [r.residual_m for r in residuals]
    rms_m = math.sqrt(sum(v * v for v in vals) / len(vals))
    max_abs_m = max(abs(v) for v in vals)

    return {
        "distance_rms_m": float(rms_m),
        "distance_rms_cm": float(100.0 * rms_m),
        "distance_max_abs_m": float(max_abs_m),
        "distance_max_abs_cm": float(100.0 * max_abs_m),
    }


def solve_distance_only_position(
    observations: Sequence[TagObservation],
    tag_map,
    camera_config: Mapping[str, Mapping[str, float]],
    measurement_weights: Mapping[str, Mapping[str, float]],
    initial_x_m: float,
    initial_y_m: float,
    fixed_heading_deg: float,
    solver_config: DistanceSolverConfig | None = None,
) -> DistanceSolveResult:
    """
    Solve robot center position (x, y) from distance measurements only.

    Method:
        deterministic coordinate search around the initial position.

    This avoids adding scipy as a required dependency. It is small and adequate
    for the local refinement use case, especially because planned location or
    last known location is usually already close.
    """
    cfg = solver_config or DistanceSolverConfig()

    # Count usable distance observations before solving.
    init_residuals = compute_distance_residuals(
        x_m=initial_x_m,
        y_m=initial_y_m,
        fixed_heading_deg=fixed_heading_deg,
        observations=observations,
        tag_map=tag_map,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
    )

    if len(init_residuals) < 2:
        return DistanceSolveResult(
            ok=False,
            x_m=float(initial_x_m),
            y_m=float(initial_y_m),
            heading_deg=float(fixed_heading_deg),
            cost=float("inf"),
            iterations=0,
            evaluations=0,
            used_distance_count=len(init_residuals),
            distance_rms_m=float("nan"),
            distance_rms_cm=float("nan"),
            distance_max_abs_m=float("nan"),
            distance_max_abs_cm=float("nan"),
            residuals=init_residuals,
            detail="not enough distance observations",
        )

    x = min(max(float(initial_x_m), cfg.x_min), cfg.x_max)
    y = min(max(float(initial_y_m), cfg.y_min), cfg.y_max)

    step = float(cfg.search_initial_step_m)
    min_step = float(cfg.search_min_step_m)

    cost = distance_only_cost(
        x_m=x,
        y_m=y,
        fixed_heading_deg=fixed_heading_deg,
        observations=observations,
        tag_map=tag_map,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
        solver_config=cfg,
    )
    evaluations = 1
    iterations = 0

    # Coordinate-search directions. Include diagonals so convergence is not too slow.
    directions = [
        (1.0, 0.0),
        (-1.0, 0.0),
        (0.0, 1.0),
        (0.0, -1.0),
        (1.0, 1.0),
        (1.0, -1.0),
        (-1.0, 1.0),
        (-1.0, -1.0),
    ]

    while step >= min_step and iterations < cfg.max_iterations:
        iterations += 1
        improved = False

        best_x = x
        best_y = y
        best_cost = cost

        for dx, dy in directions:
            cand_x = min(max(x + step * dx, cfg.x_min), cfg.x_max)
            cand_y = min(max(y + step * dy, cfg.y_min), cfg.y_max)

            cand_cost = distance_only_cost(
                x_m=cand_x,
                y_m=cand_y,
                fixed_heading_deg=fixed_heading_deg,
                observations=observations,
                tag_map=tag_map,
                camera_config=camera_config,
                measurement_weights=measurement_weights,
                solver_config=cfg,
            )
            evaluations += 1

            if cand_cost < best_cost:
                best_x = cand_x
                best_y = cand_y
                best_cost = cand_cost
                improved = True

        if improved:
            x = best_x
            y = best_y
            cost = best_cost
        else:
            step *= 0.5

    final_residuals = compute_distance_residuals(
        x_m=x,
        y_m=y,
        fixed_heading_deg=fixed_heading_deg,
        observations=observations,
        tag_map=tag_map,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
    )
    summary = _summarize_residuals(final_residuals)

    return DistanceSolveResult(
        ok=True,
        x_m=float(x),
        y_m=float(y),
        heading_deg=float(fixed_heading_deg),
        cost=float(cost),
        iterations=int(iterations),
        evaluations=int(evaluations),
        used_distance_count=len(final_residuals),
        distance_rms_m=summary["distance_rms_m"],
        distance_rms_cm=summary["distance_rms_cm"],
        distance_max_abs_m=summary["distance_max_abs_m"],
        distance_max_abs_cm=summary["distance_max_abs_cm"],
        residuals=final_residuals,
        detail="ok",
    )
