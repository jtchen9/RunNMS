"""
location_filtered_solver.py

Part 7 of the location solver:
Re-solve pose using item-level filter states.

This module uses distance + angle + yaw, but each item has its own:
    enabled flag
    weight multiplier

This allows:
    keep distance
    keep angle
    disable yaw

for the same tag observation.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
import math
from typing import Iterable, Mapping, Sequence

from .location_geometry import RobotPose, predict_tag_measurement, wrap_angle_deg
from .location_items import TagObservation
from .location_item_filter import ItemFilterState
from .location_joint_solver_yaw import JointYawSolverConfig, JointYawResidualRecord, JointYawSolveResult


def _huber_cost(z: float, clip: float) -> float:
    az = abs(z)
    if az <= clip:
        return 0.5 * z * z
    return clip * (az - 0.5 * clip)


def _camera_weight(camera_role: str, kind: str, measurement_weights: Mapping[str, Mapping[str, float]]) -> float:
    cam = str(camera_role).strip().lower()
    return float(measurement_weights.get(cam, {}).get(kind, 0.0))


def _norm_heading_deg(h: float) -> float:
    return float(h % 360.0)


def _item_multiplier(
    tag_id: int,
    camera_role: str,
    kind: str,
    filter_states: Mapping[tuple[int, str, str], ItemFilterState],
) -> float:
    key = (int(tag_id), str(camera_role).strip().lower(), str(kind))
    s = filter_states.get(key)
    if s is None:
        return 1.0
    if not s.enabled:
        return 0.0
    return float(s.weight_multiplier)


def compute_filtered_residuals(
    x_m: float,
    y_m: float,
    heading_deg: float,
    observations: Iterable[TagObservation],
    tag_map,
    camera_config: Mapping[str, Mapping[str, float]],
    measurement_weights: Mapping[str, Mapping[str, float]],
    filter_states: Mapping[tuple[int, str, str], ItemFilterState],
) -> list[JointYawResidualRecord]:
    robot_pose = RobotPose(x_m=float(x_m), y_m=float(y_m), heading_deg=float(heading_deg))
    residuals: list[JointYawResidualRecord] = []

    for obs in observations:
        camera_role = str(obs.camera_role).strip().lower()

        if obs.tag_id not in tag_map:
            continue
        if camera_role not in camera_config:
            continue

        d_weight = _camera_weight(camera_role, "distance", measurement_weights)
        a_weight = _camera_weight(camera_role, "angle", measurement_weights)
        y_weight = _camera_weight(camera_role, "yaw", measurement_weights)

        d_weight *= _item_multiplier(obs.tag_id, camera_role, "distance", filter_states)
        a_weight *= _item_multiplier(obs.tag_id, camera_role, "angle", filter_states)
        y_weight *= _item_multiplier(obs.tag_id, camera_role, "yaw", filter_states)

        if d_weight <= 0.0 and a_weight <= 0.0 and y_weight <= 0.0:
            continue

        tag_world = tag_map[obs.tag_id]
        pred = predict_tag_measurement(
            robot_pose=robot_pose,
            tag_world=tag_world,
            camera_cfg=camera_config[camera_role],
        )

        d_res = float(obs.distance_m - pred.distance_m)
        a_res = float(wrap_angle_deg(obs.angle_deg - pred.angle_deg))
        y_res = float(wrap_angle_deg(obs.yaw_deg - pred.yaw_deg))

        residuals.append(
            JointYawResidualRecord(
                tag_id=int(obs.tag_id),
                camera_role=camera_role,

                measured_distance_m=float(obs.distance_m),
                predicted_distance_m=float(pred.distance_m),
                distance_residual_m=d_res,
                distance_residual_cm=100.0 * d_res,
                distance_weight=d_weight,

                measured_angle_deg=float(obs.angle_deg),
                predicted_angle_deg=float(pred.angle_deg),
                angle_residual_deg=a_res,
                angle_weight=a_weight,

                measured_yaw_deg=float(obs.yaw_deg),
                predicted_yaw_deg=float(pred.yaw_deg),
                yaw_residual_deg=y_res,
                yaw_weight=y_weight,
            )
        )

    return residuals


def filtered_cost(
    x_m: float,
    y_m: float,
    heading_deg: float,
    observations: Iterable[TagObservation],
    tag_map,
    camera_config: Mapping[str, Mapping[str, float]],
    measurement_weights: Mapping[str, Mapping[str, float]],
    filter_states: Mapping[tuple[int, str, str], ItemFilterState],
    solver_config: JointYawSolverConfig,
) -> float:
    residuals = compute_filtered_residuals(
        x_m=x_m,
        y_m=y_m,
        heading_deg=heading_deg,
        observations=observations,
        tag_map=tag_map,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
        filter_states=filter_states,
    )

    if not residuals:
        return float("inf")

    total = 0.0
    for r in residuals:
        if r.distance_weight > 0.0:
            z = r.distance_residual_m / solver_config.sigma_distance_m
            total += r.distance_weight * _huber_cost(z, solver_config.robust_clip_sigma)
        if r.angle_weight > 0.0:
            z = r.angle_residual_deg / solver_config.sigma_angle_deg
            total += r.angle_weight * _huber_cost(z, solver_config.robust_clip_sigma)
        if r.yaw_weight > 0.0:
            z = r.yaw_residual_deg / solver_config.sigma_yaw_deg
            total += r.yaw_weight * _huber_cost(z, solver_config.robust_clip_sigma)

    return float(total)


def _summarize_residuals(residuals: list[JointYawResidualRecord]) -> dict:
    d_vals = [r.distance_residual_m for r in residuals if r.distance_weight > 0.0]
    a_vals = [r.angle_residual_deg for r in residuals if r.angle_weight > 0.0]
    y_vals = [r.yaw_residual_deg for r in residuals if r.yaw_weight > 0.0]

    def rms(vals):
        if not vals:
            return float("nan")
        return math.sqrt(sum(v * v for v in vals) / len(vals))

    def max_abs(vals):
        if not vals:
            return float("nan")
        return max(abs(v) for v in vals)

    d_rms = rms(d_vals)
    d_max = max_abs(d_vals)
    a_rms = rms(a_vals)
    a_max = max_abs(a_vals)
    y_rms = rms(y_vals)
    y_max = max_abs(y_vals)

    return {
        "distance_rms_m": d_rms,
        "distance_rms_cm": 100.0 * d_rms if math.isfinite(d_rms) else float("nan"),
        "distance_max_abs_m": d_max,
        "distance_max_abs_cm": 100.0 * d_max if math.isfinite(d_max) else float("nan"),
        "angle_rms_deg": a_rms,
        "angle_max_abs_deg": a_max,
        "yaw_rms_deg": y_rms,
        "yaw_max_abs_deg": y_max,
    }


def _make_heading_seeds(initial_heading_deg: float, cfg: JointYawSolverConfig) -> list[float]:
    seeds = []
    for off in cfg.local_heading_seed_offsets_deg:
        seeds.append(_norm_heading_deg(initial_heading_deg + off))
    if cfg.use_global_heading_seeds:
        n = int(round(360.0 / cfg.global_heading_seed_step_deg))
        for i in range(n):
            seeds.append(_norm_heading_deg(i * cfg.global_heading_seed_step_deg))

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
    filter_states: Mapping[tuple[int, str, str], ItemFilterState],
    initial_x_m: float,
    initial_y_m: float,
    initial_heading_deg: float,
    solver_config: JointYawSolverConfig,
) -> dict:
    cfg = solver_config

    x = min(max(float(initial_x_m), cfg.x_min), cfg.x_max)
    y = min(max(float(initial_y_m), cfg.y_min), cfg.y_max)
    h = _norm_heading_deg(float(initial_heading_deg))

    step_xy = float(cfg.search_initial_step_xy_m)
    step_h = float(cfg.search_initial_step_h_deg)

    cost = filtered_cost(
        x, y, h,
        observations, tag_map, camera_config, measurement_weights, filter_states, cfg,
    )
    evaluations = 1
    iterations = 0

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

        for cx, cy, ch in candidates:
            c = filtered_cost(
                cx, cy, ch,
                observations, tag_map, camera_config, measurement_weights, filter_states, cfg,
            )
            evaluations += 1
            if c < best_cost:
                best_x, best_y, best_h = cx, cy, ch
                best_cost = c
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


def solve_filtered_pose(
    observations: Sequence[TagObservation],
    tag_map,
    camera_config: Mapping[str, Mapping[str, float]],
    measurement_weights: Mapping[str, Mapping[str, float]],
    filter_states: Mapping[tuple[int, str, str], ItemFilterState],
    initial_x_m: float,
    initial_y_m: float,
    initial_heading_deg: float,
    solver_config: JointYawSolverConfig | None = None,
) -> JointYawSolveResult:
    cfg = solver_config or JointYawSolverConfig()

    initial_res = compute_filtered_residuals(
        initial_x_m, initial_y_m, initial_heading_deg,
        observations, tag_map, camera_config, measurement_weights, filter_states,
    )

    used_d = sum(1 for r in initial_res if r.distance_weight > 0.0)
    used_a = sum(1 for r in initial_res if r.angle_weight > 0.0)
    used_y = sum(1 for r in initial_res if r.yaw_weight > 0.0)

    if used_d < 2 or used_a < 1:
        summary = _summarize_residuals(initial_res)
        return JointYawSolveResult(
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
            used_tag_count=len({r.tag_id for r in initial_res}),
            used_distance_count=used_d,
            used_angle_count=used_a,
            used_yaw_count=used_y,
            distance_rms_m=summary["distance_rms_m"],
            distance_rms_cm=summary["distance_rms_cm"],
            distance_max_abs_m=summary["distance_max_abs_m"],
            distance_max_abs_cm=summary["distance_max_abs_cm"],
            angle_rms_deg=summary["angle_rms_deg"],
            angle_max_abs_deg=summary["angle_max_abs_deg"],
            yaw_rms_deg=summary["yaw_rms_deg"],
            yaw_max_abs_deg=summary["yaw_max_abs_deg"],
            residuals=initial_res,
            detail="not enough enabled distance/angle items after filtering",
        )

    heading_seeds = _make_heading_seeds(initial_heading_deg, cfg)

    seed_results = []
    total_eval = 0
    total_iter = 0

    for h_seed in heading_seeds:
        r = _coordinate_search_from_seed(
            observations=observations,
            tag_map=tag_map,
            camera_config=camera_config,
            measurement_weights=measurement_weights,
            filter_states=filter_states,
            initial_x_m=initial_x_m,
            initial_y_m=initial_y_m,
            initial_heading_deg=h_seed,
            solver_config=cfg,
        )
        r["seed_heading_deg"] = h_seed
        seed_results.append(r)
        total_eval += r["evaluations"]
        total_iter += r["iterations"]

    best = min(seed_results, key=lambda r: r["cost"])

    final_res = compute_filtered_residuals(
        best["x_m"], best["y_m"], best["heading_deg"],
        observations, tag_map, camera_config, measurement_weights, filter_states,
    )
    summary = _summarize_residuals(final_res)

    return JointYawSolveResult(
        ok=True,
        x_m=float(best["x_m"]),
        y_m=float(best["y_m"]),
        heading_deg=float(best["heading_deg"]),
        cost=float(best["cost"]),
        iterations=int(total_iter),
        evaluations=int(total_eval),
        seed_count=len(heading_seeds),
        best_seed_x_m=float(initial_x_m),
        best_seed_y_m=float(initial_y_m),
        best_seed_heading_deg=float(best["seed_heading_deg"]),
        used_tag_count=len({r.tag_id for r in final_res}),
        used_distance_count=sum(1 for r in final_res if r.distance_weight > 0.0),
        used_angle_count=sum(1 for r in final_res if r.angle_weight > 0.0),
        used_yaw_count=sum(1 for r in final_res if r.yaw_weight > 0.0),
        distance_rms_m=summary["distance_rms_m"],
        distance_rms_cm=summary["distance_rms_cm"],
        distance_max_abs_m=summary["distance_max_abs_m"],
        distance_max_abs_cm=summary["distance_max_abs_cm"],
        angle_rms_deg=summary["angle_rms_deg"],
        angle_max_abs_deg=summary["angle_max_abs_deg"],
        yaw_rms_deg=summary["yaw_rms_deg"],
        yaw_max_abs_deg=summary["yaw_max_abs_deg"],
        residuals=final_res,
        detail="ok",
    )
