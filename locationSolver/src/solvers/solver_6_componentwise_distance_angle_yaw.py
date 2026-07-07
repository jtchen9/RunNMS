from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from src.common.geometry import (
    predicted_tag_angle_deg,
    predicted_tag_distance_m,
    predicted_tag_yaw_deg,
    wrap_angle_deg,
)
from src.common.solver_result import SolverResult


@dataclass(frozen=True)
class ComponentwiseDistanceAngleYawSolverConfig:
    """
    Final component-wise D/A/Y solver.

    Each observation may independently contribute:
      flags["distance_use"]
      flags["angle_use"]
      flags["yaw_use"]

    Resolved GT-free yaw is read only from:
      measured["yaw_resolved_deg"]

    The solver intentionally does NOT read:
      flags["yaw_use_offline_label"]
      measured["yaw_sign_corrected_deg"]
      ground_truth
      evaluation
    """

    x_min_m: float = 0.0
    x_max_m: float = 11.4
    y_min_m: float = 0.0
    y_max_m: float = 11.4

    min_active_components: int = 3

    distance_sigma_m: float = 0.05
    angle_sigma_deg: float = 2.5
    yaw_sigma_deg: float = 3.0

    distance_global_weight: float = 1.0
    angle_global_weight: float = 1.0
    yaw_global_weight: float = 0.75

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
            "Final component-wise D/A/Y solver requires scipy."
        ) from exc
    return least_squares


def _finite_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        x = float(value)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _component_state(
    obs: Dict[str, Any],
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
) -> Dict[str, bool]:
    """
    Validate D/A/Y independently.

    A tag may therefore contribute any subset:
      D+A+Y, D+A, A+Y, D+Y, D, A, Y, or nothing.
    """
    state = {
        "distance": False,
        "angle": False,
        "yaw": False,
    }

    try:
        tag_id = int(obs["tag_id"])
    except Exception:
        return state

    if tag_id not in tag_pose_map:
        return state

    flags = obs.get("flags") or {}
    measured = obs.get("measured") or {}
    weights = obs.get("weights") or {}

    d = _finite_float(measured.get("distance_m"))
    a = _finite_float(measured.get("angle_deg"))
    y = _finite_float(measured.get("yaw_resolved_deg"))

    wd = _finite_float(weights.get("distance", 1.0))
    wa = _finite_float(weights.get("angle", 1.0))
    wy = _finite_float(weights.get("yaw", 1.0))

    if bool(flags.get("distance_use")):
        state["distance"] = (
            d is not None
            and d > 0.0
            and wd is not None
            and wd > 0.0
        )

    if bool(flags.get("angle_use")):
        state["angle"] = (
            a is not None
            and wa is not None
            and wa > 0.0
        )

    tag_yaw_deg = tag_pose_map[tag_id][2]
    if bool(flags.get("yaw_use")):
        state["yaw"] = (
            y is not None
            and tag_yaw_deg is not None
            and wy is not None
            and wy > 0.0
        )

    return state


def _obs_uid(
    sample_uid: str,
    row_index: int,
    obs: Dict[str, Any],
) -> str:
    return str(
        obs.get("observation_uid")
        or f"{sample_uid}_row{row_index:03d}"
    )


def _initial_xy_from_active_tags(
    observations: List[Dict[str, Any]],
    active_by_uid: Dict[str, Dict[str, bool]],
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
    config: ComponentwiseDistanceAngleYawSolverConfig,
    sample_uid: str,
) -> Tuple[float, float]:
    """
    Same initialization philosophy as earlier solvers:
    centroid of tags that retain at least one active component.

    Heading is handled separately by 8-way multistart.
    """
    pts: List[Tuple[float, float]] = []

    for row_index, obs in enumerate(observations, start=1):
        uid = _obs_uid(sample_uid, row_index, obs)
        state = active_by_uid.get(uid) or {}

        if not any(
            bool(state.get(k))
            for k in ("distance", "angle", "yaw")
        ):
            continue

        try:
            tag_id = int(obs["tag_id"])
        except Exception:
            continue

        if tag_id not in tag_pose_map:
            continue

        tag_x, tag_y, _ = tag_pose_map[tag_id]
        pts.append((float(tag_x), float(tag_y)))

    if not pts:
        return (
            0.5 * (config.x_min_m + config.x_max_m),
            0.5 * (config.y_min_m + config.y_max_m),
        )

    x0 = sum(x for x, _ in pts) / len(pts)
    y0 = sum(y for _, y in pts) / len(pts)

    return (
        min(config.x_max_m, max(config.x_min_m, x0)),
        min(config.y_max_m, max(config.y_min_m, y0)),
    )


def solve_componentwise_distance_angle_yaw(
    sample: Dict[str, Any],
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
    config: ComponentwiseDistanceAngleYawSolverConfig,
) -> SolverResult:
    """
    Solve robot (x, y, heading) from arbitrary active D/A/Y components.

    No GT is used.
    """
    least_squares = _require_scipy()

    sample_uid = str(sample["sample_uid"])
    observations = list(sample.get("observations") or [])

    missing_tags: List[int] = []
    active_by_uid: Dict[str, Dict[str, bool]] = {}

    distance_count = 0
    angle_count = 0
    yaw_count = 0

    component_pattern_counts: Dict[str, int] = {}

    for row_index, obs in enumerate(observations, start=1):
        try:
            tag_id = int(obs["tag_id"])
        except Exception:
            continue

        if tag_id not in tag_pose_map:
            missing_tags.append(tag_id)

        uid = _obs_uid(sample_uid, row_index, obs)
        state = _component_state(obs, tag_pose_map)
        active_by_uid[uid] = state

        distance_count += int(state["distance"])
        angle_count += int(state["angle"])
        yaw_count += int(state["yaw"])

        pattern = "".join(
            label
            for label, key in (
                ("D", "distance"),
                ("A", "angle"),
                ("Y", "yaw"),
            )
            if state[key]
        ) or "NONE"
        component_pattern_counts[pattern] = (
            component_pattern_counts.get(pattern, 0) + 1
        )

    active_component_count = (
        distance_count + angle_count + yaw_count
    )

    tags_input = []
    tags_used = []
    tags_fully_inactive = []

    for row_index, obs in enumerate(observations, start=1):
        try:
            tag_id = int(obs["tag_id"])
        except Exception:
            continue

        tags_input.append(tag_id)

        uid = _obs_uid(sample_uid, row_index, obs)
        state = active_by_uid.get(uid) or {}

        if any(
            bool(state.get(k))
            for k in ("distance", "angle", "yaw")
        ):
            tags_used.append(tag_id)
        else:
            tags_fully_inactive.append(tag_id)

    common_extra = {
        "missing_tag_ids": sorted(set(missing_tags)),
        "active_distance_count": distance_count,
        "active_angle_count": angle_count,
        "active_yaw_count": yaw_count,
        "active_component_count": active_component_count,
        "component_pattern_counts": component_pattern_counts,
        "fully_inactive_tag_ids": tags_fully_inactive,
        "componentwise_solver": True,
    }

    if active_component_count < config.min_active_components:
        return SolverResult(
            sample_uid=sample_uid,
            success=False,
            failure_reason=(
                "insufficient_active_components:"
                f"{active_component_count}<"
                f"{config.min_active_components}"
            ),
            tags_input=tags_input,
            tags_used=tags_used,
            tags_rejected=tags_fully_inactive,
            extra=common_extra,
        )

    x0, y0 = _initial_xy_from_active_tags(
        observations=observations,
        active_by_uid=active_by_uid,
        tag_pose_map=tag_pose_map,
        config=config,
        sample_uid=sample_uid,
    )

    def residuals(params):
        robot_x_m = float(params[0])
        robot_y_m = float(params[1])
        robot_heading_deg = float(params[2])

        out: List[float] = []

        for row_index, obs in enumerate(observations, start=1):
            uid = _obs_uid(sample_uid, row_index, obs)
            state = active_by_uid.get(uid) or {}

            if not any(
                bool(state.get(k))
                for k in ("distance", "angle", "yaw")
            ):
                continue

            tag_id = int(obs["tag_id"])
            tag_x_m, tag_y_m, tag_yaw_deg = tag_pose_map[tag_id]

            measured = obs.get("measured") or {}
            weights = obs.get("weights") or {}

            if state["distance"]:
                measured_distance_m = float(
                    measured["distance_m"]
                )
                distance_weight = float(
                    weights.get("distance", 1.0)
                )

                pred_distance_m = predicted_tag_distance_m(
                    robot_x_m,
                    robot_y_m,
                    robot_heading_deg,
                    obs["camera_role"],
                    tag_x_m,
                    tag_y_m,
                )

                out.append(
                    math.sqrt(
                        config.distance_global_weight
                        * distance_weight
                    )
                    * (
                        pred_distance_m - measured_distance_m
                    )
                    / config.distance_sigma_m
                )

            if state["angle"]:
                measured_angle_deg = float(
                    measured["angle_deg"]
                )
                angle_weight = float(
                    weights.get("angle", 1.0)
                )

                pred_angle_deg = predicted_tag_angle_deg(
                    robot_x_m,
                    robot_y_m,
                    robot_heading_deg,
                    obs["camera_role"],
                    tag_x_m,
                    tag_y_m,
                )

                out.append(
                    math.sqrt(
                        config.angle_global_weight
                        * angle_weight
                    )
                    * wrap_angle_deg(
                        pred_angle_deg - measured_angle_deg
                    )
                    / config.angle_sigma_deg
                )

            if state["yaw"]:
                measured_yaw_deg = float(
                    measured["yaw_resolved_deg"]
                )
                yaw_weight = float(
                    weights.get("yaw", 1.0)
                )

                pred_yaw_deg = predicted_tag_yaw_deg(
                    robot_x_m,
                    robot_y_m,
                    robot_heading_deg,
                    obs["camera_role"],
                    tag_x_m,
                    tag_y_m,
                    float(tag_yaw_deg),
                )

                out.append(
                    math.sqrt(
                        config.yaw_global_weight
                        * yaw_weight
                    )
                    * wrap_angle_deg(
                        pred_yaw_deg - measured_yaw_deg
                    )
                    / config.yaw_sigma_deg
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

        if (
            bool(result.success)
            and math.isfinite(float(result.cost))
            and float(result.cost) < best_cost
        ):
            best = result
            best_cost = float(result.cost)

    runtime_ms = (
        time.perf_counter() - start_time
    ) * 1000.0

    if best is None:
        return SolverResult(
            sample_uid=sample_uid,
            success=False,
            failure_reason="no_successful_multistart_solution",
            iterations=total_nfev,
            runtime_ms=runtime_ms,
            objective_value=None,
            tags_input=tags_input,
            tags_used=tags_used,
            tags_rejected=tags_fully_inactive,
            extra=common_extra,
        )

    final_extra = dict(common_extra)
    final_extra.update({
        "best_optimizer_message": str(best.message),
        "initial_x_m": x0,
        "initial_y_m": y0,
        "heading_starts_deg": list(config.heading_starts_deg),
    })

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
        tags_rejected=tags_fully_inactive,
        extra=final_extra,
    )
