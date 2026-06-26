"""
location_joint_solver.py

Part 5 of the location solver:
Joint distance + view-angle solver.

Purpose
-------
Estimate robot pose (x, y, heading) jointly using:
    distance items
    angle items

Yaw is NOT used in Part 5.

This is expected to become the main practical solver foundation because:
    - distance mainly anchors x,y
    - view-angle mainly anchors heading
    - yaw will be added later only as a weak helper in Part 6

This module is reusable production-evolving code.
It contains no CSV logic, no pandas, no output files, and no argparse.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
import math
from typing import Iterable, Mapping, Sequence

from .location_geometry import (
    RobotPose,
    predict_tag_measurement,
    wrap_angle_deg,
    abs_angle_error_deg,
)
from .location_items import TagObservation


@dataclass(frozen=True)
class JointSolverConfig:
    """
    Config for distance+angle joint solver.
    """
    sigma_distance_m: float = 0.05
    sigma_angle_deg: float = 3.0

    search_initial_step_xy_m: float = 0.20
    search_initial_step_h_deg: float = 10.0

    search_min_step_xy_m: float = 0.002
    search_min_step_h_deg: float = 0.10

    max_iterations: int = 250
    robust_clip_sigma: float = 3.0

    x_min: float = -1.0
    x_max: float = 12.5
    y_min: float = -1.0
    y_max: float = 12.5

    # Initial heading seeds around given initial heading.
    local_heading_seed_offsets_deg: tuple[float, ...] = (-30.0, -15.0, 0.0, 15.0, 30.0)

    # Add global seeds to avoid local minimum.
    use_global_heading_seeds: bool = True
    global_heading_seed_step_deg: float = 45.0


@dataclass(frozen=True)
class JointResidualRecord:
    tag_id: int
    camera_role: str

    measured_distance_m: float
    predicted_distance_m: float
    distance_residual_m: float
    distance_residual_cm: float
    distance_weight: float

    measured_angle_deg: float
    predicted_angle_deg: float
    angle_residual_deg: float
    angle_weight: float


@dataclass(frozen=True)
class JointSolveResult:
    ok: bool
    x_m: float
    y_m: float
    heading_deg: float

    cost: float
    iterations: int
    evaluations: int
    seed_count: int
    best_seed_x_m: float
    best_seed_y_m: float
    best_seed_heading_deg: float

    used_tag_count: int
    used_distance_count: int
    used_angle_count: int

    distance_rms_m: float
    distance_rms_cm: float
    distance_max_abs_m: float
    distance_max_abs_cm: float

    angle_rms_deg: float
    angle_max_abs_deg: float

    residuals: list[JointResidualRecord]
    detail: str

    def to_dict(self) -> dict:
        d = asdict(self)
        d["residuals"] = [asdict(r) for r in self.residuals]
        return d


def _huber_cost(z: float, clip: float) -> float:
    az = abs(z)
    if az <= clip:
        return 0.5 * z * z
    return clip * (az - 0.5 * clip)


def _camera_weight(
    camera_role: str,
    kind: str,
    measurement_weights: Mapping[str, Mapping[str, float]],
) -> float:
    cam = str(camera_role).strip().lower()
    return float(measurement_weights.get(cam, {}).get(kind, 0.0))


def _norm_heading_deg(h: float) -> float:
    return float(h % 360.0)


def compute_joint_residuals(
    x_m: float,
    y_m: float,
    heading_deg: float,
    observations: Iterable[TagObservation],
    tag_map,
    camera_config: Mapping[str, Mapping[str, float]],
    measurement_weights: Mapping[str, Mapping[str, float]],
) -> list[JointResidualRecord]:
    """
    Compute distance and angle residuals for all valid observations.

    Residual convention:
        residual = measured - predicted
    """
    robot_pose = RobotPose(x_m=float(x_m), y_m=float(y_m), heading_deg=float(heading_deg))
    residuals: list[JointResidualRecord] = []

    for obs in observations:
        camera_role = str(obs.camera_role).strip().lower()

        if obs.tag_id not in tag_map:
            continue
        if camera_role not in camera_config:
            continue

        distance_weight = _camera_weight(camera_role, "distance", measurement_weights)
        angle_weight = _camera_weight(camera_role, "angle", measurement_weights)

        # For Part 5, we need at least one of distance/angle to be useful.
        if distance_weight <= 0.0 and angle_weight <= 0.0:
            continue

        tag_world = tag_map[obs.tag_id]
        pred = predict_tag_measurement(
            robot_pose=robot_pose,
            tag_world=tag_world,
            camera_cfg=camera_config[camera_role],
        )

        distance_residual_m = float(obs.distance_m - pred.distance_m)
        angle_residual_deg = float(wrap_angle_deg(obs.angle_deg - pred.angle_deg))

        residuals.append(
            JointResidualRecord(
                tag_id=int(obs.tag_id),
                camera_role=camera_role,

                measured_distance_m=float(obs.distance_m),
                predicted_distance_m=float(pred.distance_m),
                distance_residual_m=distance_residual_m,
                distance_residual_cm=100.0 * distance_residual_m,
                distance_weight=distance_weight,

                measured_angle_deg=float(obs.angle_deg),
                predicted_angle_deg=float(pred.angle_deg),
                angle_residual_deg=angle_residual_deg,
                angle_weight=angle_weight,
            )
        )

    return residuals


def joint_distance_angle_cost(
    x_m: float,
    y_m: float,
    heading_deg: float,
    observations: Iterable[TagObservation],
    tag_map,
    camera_config: Mapping[str, Mapping[str, float]],
    measurement_weights: Mapping[str, Mapping[str, float]],
    solver_config: JointSolverConfig,
) -> float:
    residuals = compute_joint_residuals(
        x_m=x_m,
        y_m=y_m,
        heading_deg=heading_deg,
        observations=observations,
        tag_map=tag_map,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
    )

    if not residuals:
        return float("inf")

    total = 0.0
    for r in residuals:
        if r.distance_weight > 0.0:
            z_d = r.distance_residual_m / solver_config.sigma_distance_m
            total += r.distance_weight * _huber_cost(z_d, solver_config.robust_clip_sigma)

        if r.angle_weight > 0.0:
            z_a = r.angle_residual_deg / solver_config.sigma_angle_deg
            total += r.angle_weight * _huber_cost(z_a, solver_config.robust_clip_sigma)

    return float(total)


def _summarize_residuals(residuals: list[JointResidualRecord]) -> dict:
    distance_vals = [r.distance_residual_m for r in residuals if r.distance_weight > 0.0]
    angle_vals = [r.angle_residual_deg for r in residuals if r.angle_weight > 0.0]

    if distance_vals:
        distance_rms_m = math.sqrt(sum(v * v for v in distance_vals) / len(distance_vals))
        distance_max_abs_m = max(abs(v) for v in distance_vals)
    else:
        distance_rms_m = float("nan")
        distance_max_abs_m = float("nan")

    if angle_vals:
        angle_rms_deg = math.sqrt(sum(v * v for v in angle_vals) / len(angle_vals))
        angle_max_abs_deg = max(abs(v) for v in angle_vals)
    else:
        angle_rms_deg = float("nan")
        angle_max_abs_deg = float("nan")

    return {
        "distance_rms_m": float(distance_rms_m),
        "distance_rms_cm": float(100.0 * distance_rms_m) if math.isfinite(distance_rms_m) else float("nan"),
        "distance_max_abs_m": float(distance_max_abs_m),
        "distance_max_abs_cm": float(100.0 * distance_max_abs_m) if math.isfinite(distance_max_abs_m) else float("nan"),
        "angle_rms_deg": float(angle_rms_deg),
        "angle_max_abs_deg": float(angle_max_abs_deg),
    }


def _make_heading_seeds(initial_heading_deg: float, cfg: JointSolverConfig) -> list[float]:
    seeds: list[float] = []

    for off in cfg.local_heading_seed_offsets_deg:
        seeds.append(_norm_heading_deg(initial_heading_deg + off))

    if cfg.use_global_heading_seeds:
        n = int(round(360.0 / cfg.global_heading_seed_step_deg))
        for i in range(n):
            seeds.append(_norm_heading_deg(i * cfg.global_heading_seed_step_deg))

    # Deduplicate.
    out = []
    seen = set()
    for h in seeds:
        k = round(h, 6)
        if k not in seen:
            seen.add(k)
            out.append(h)

    return out


def _coordinate_search_from_seed(
    observations: Sequence[TagObservation],
    tag_map,
    camera_config: Mapping[str, Mapping[str, float]],
    measurement_weights: Mapping[str, Mapping[str, float]],
    initial_x_m: float,
    initial_y_m: float,
    initial_heading_deg: float,
    solver_config: JointSolverConfig,
) -> dict:
    """
    Deterministic coordinate search in x, y, heading.

    This avoids requiring scipy and is adequate for local pose refinement.
    """
    cfg = solver_config

    x = min(max(float(initial_x_m), cfg.x_min), cfg.x_max)
    y = min(max(float(initial_y_m), cfg.y_min), cfg.y_max)
    h = _norm_heading_deg(float(initial_heading_deg))

    step_xy = float(cfg.search_initial_step_xy_m)
    step_h = float(cfg.search_initial_step_h_deg)

    cost = joint_distance_angle_cost(
        x_m=x,
        y_m=y,
        heading_deg=h,
        observations=observations,
        tag_map=tag_map,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
        solver_config=cfg,
    )
    evaluations = 1
    iterations = 0

    # Candidate moves. Try x/y and heading separately plus diagonal x/y moves.
    while (
        (step_xy >= cfg.search_min_step_xy_m or step_h >= cfg.search_min_step_h_deg)
        and iterations < cfg.max_iterations
    ):
        iterations += 1
        improved = False

        candidates = []

        if step_xy >= cfg.search_min_step_xy_m:
            for dx, dy in [
                (1.0, 0.0), (-1.0, 0.0),
                (0.0, 1.0), (0.0, -1.0),
                (1.0, 1.0), (1.0, -1.0),
                (-1.0, 1.0), (-1.0, -1.0),
            ]:
                candidates.append((
                    min(max(x + step_xy * dx, cfg.x_min), cfg.x_max),
                    min(max(y + step_xy * dy, cfg.y_min), cfg.y_max),
                    h,
                ))

        if step_h >= cfg.search_min_step_h_deg:
            candidates.append((x, y, _norm_heading_deg(h + step_h)))
            candidates.append((x, y, _norm_heading_deg(h - step_h)))

        best_x, best_y, best_h = x, y, h
        best_cost = cost

        for cand_x, cand_y, cand_h in candidates:
            cand_cost = joint_distance_angle_cost(
                x_m=cand_x,
                y_m=cand_y,
                heading_deg=cand_h,
                observations=observations,
                tag_map=tag_map,
                camera_config=camera_config,
                measurement_weights=measurement_weights,
                solver_config=cfg,
            )
            evaluations += 1

            if cand_cost < best_cost:
                best_x, best_y, best_h = cand_x, cand_y, cand_h
                best_cost = cand_cost
                improved = True

        if improved:
            x, y, h = best_x, best_y, best_h
            cost = best_cost
        else:
            step_xy *= 0.5
            step_h *= 0.5

    return {
        "x_m": float(x),
        "y_m": float(y),
        "heading_deg": float(h),
        "cost": float(cost),
        "iterations": int(iterations),
        "evaluations": int(evaluations),
    }


def solve_distance_angle_pose(
    observations: Sequence[TagObservation],
    tag_map,
    camera_config: Mapping[str, Mapping[str, float]],
    measurement_weights: Mapping[str, Mapping[str, float]],
    initial_x_m: float,
    initial_y_m: float,
    initial_heading_deg: float,
    solver_config: JointSolverConfig | None = None,
) -> JointSolveResult:
    """
    Solve robot pose (x, y, heading) using distance + angle.

    Yaw is not used in Part 5.
    """
    cfg = solver_config or JointSolverConfig()

    initial_residuals = compute_joint_residuals(
        x_m=initial_x_m,
        y_m=initial_y_m,
        heading_deg=initial_heading_deg,
        observations=observations,
        tag_map=tag_map,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
    )

    used_distance_count = sum(1 for r in initial_residuals if r.distance_weight > 0.0)
    used_angle_count = sum(1 for r in initial_residuals if r.angle_weight > 0.0)

    if used_distance_count < 2 or used_angle_count < 1:
        summary = _summarize_residuals(initial_residuals)
        return JointSolveResult(
            ok=False,
            x_m=float(initial_x_m),
            y_m=float(initial_y_m),
            heading_deg=_norm_heading_deg(initial_heading_deg),
            cost=float("inf"),
            iterations=0,
            evaluations=0,
            seed_count=0,
            best_seed_x_m=float(initial_x_m),
            best_seed_y_m=float(initial_y_m),
            best_seed_heading_deg=_norm_heading_deg(initial_heading_deg),
            used_tag_count=len({r.tag_id for r in initial_residuals}),
            used_distance_count=used_distance_count,
            used_angle_count=used_angle_count,
            distance_rms_m=summary["distance_rms_m"],
            distance_rms_cm=summary["distance_rms_cm"],
            distance_max_abs_m=summary["distance_max_abs_m"],
            distance_max_abs_cm=summary["distance_max_abs_cm"],
            angle_rms_deg=summary["angle_rms_deg"],
            angle_max_abs_deg=summary["angle_max_abs_deg"],
            residuals=initial_residuals,
            detail="not enough distance/angle observations",
        )

    heading_seeds = _make_heading_seeds(initial_heading_deg, cfg)

    seed_results = []
    total_evaluations = 0
    total_iterations = 0

    for h_seed in heading_seeds:
        result = _coordinate_search_from_seed(
            observations=observations,
            tag_map=tag_map,
            camera_config=camera_config,
            measurement_weights=measurement_weights,
            initial_x_m=initial_x_m,
            initial_y_m=initial_y_m,
            initial_heading_deg=h_seed,
            solver_config=cfg,
        )
        result["seed_x_m"] = float(initial_x_m)
        result["seed_y_m"] = float(initial_y_m)
        result["seed_heading_deg"] = float(h_seed)
        seed_results.append(result)
        total_evaluations += result["evaluations"]
        total_iterations += result["iterations"]

    best = min(seed_results, key=lambda r: r["cost"])

    final_residuals = compute_joint_residuals(
        x_m=best["x_m"],
        y_m=best["y_m"],
        heading_deg=best["heading_deg"],
        observations=observations,
        tag_map=tag_map,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
    )
    summary = _summarize_residuals(final_residuals)

    return JointSolveResult(
        ok=True,
        x_m=float(best["x_m"]),
        y_m=float(best["y_m"]),
        heading_deg=float(best["heading_deg"]),
        cost=float(best["cost"]),
        iterations=int(total_iterations),
        evaluations=int(total_evaluations),
        seed_count=len(heading_seeds),
        best_seed_x_m=float(best["seed_x_m"]),
        best_seed_y_m=float(best["seed_y_m"]),
        best_seed_heading_deg=float(best["seed_heading_deg"]),
        used_tag_count=len({r.tag_id for r in final_residuals}),
        used_distance_count=sum(1 for r in final_residuals if r.distance_weight > 0.0),
        used_angle_count=sum(1 for r in final_residuals if r.angle_weight > 0.0),
        distance_rms_m=summary["distance_rms_m"],
        distance_rms_cm=summary["distance_rms_cm"],
        distance_max_abs_m=summary["distance_max_abs_m"],
        distance_max_abs_cm=summary["distance_max_abs_cm"],
        angle_rms_deg=summary["angle_rms_deg"],
        angle_max_abs_deg=summary["angle_max_abs_deg"],
        residuals=final_residuals,
        detail="ok",
    )
