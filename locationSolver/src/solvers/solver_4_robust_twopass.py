from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from src.common.geometry import (
    predicted_tag_angle_deg,
    predicted_tag_distance_m,
    predicted_tag_yaw_deg,
    wrap_angle_deg,
)
from src.common.solver_result import SolverResult
from src.solvers.solver_3_distance_angle_yaw import (
    DistanceAngleYawSolverConfig,
    solve_distance_angle_yaw,
)


@dataclass(frozen=True)
class RobustTwoPassConfig:
    """
    Solver #4 baseline.

    Pass 1:
        run Solver #3 unchanged

    Residual screening:
        compute per-observation normalized residual score
        using only measurements, tag map and Pass-1 estimated pose

    Pass 2:
        reject at most max_rejections observations above threshold
        then rerun Solver #3

    No ground truth is used for rejection.
    """

    rejection_score_threshold: float = 3.0
    max_rejections: int = 1
    min_observations_after_rejection: int = 3

    # Require the worst score to stand out from the second worst.
    min_score_gap: float = 0.75


def _observation_residual_record(
    obs: Dict[str, Any],
    result: SolverResult,
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
    base_config: DistanceAngleYawSolverConfig,
) -> Dict[str, Any]:
    if not result.success:
        raise ValueError("Pass-1 result must be successful")

    x = float(result.estimated_x_m)
    y = float(result.estimated_y_m)
    h = float(result.estimated_heading_deg)

    tag_id = int(obs["tag_id"])
    tag_x, tag_y, tag_yaw = tag_pose_map[tag_id]

    measured = obs.get("measured") or {}
    weights = obs.get("weights") or {}
    flags = obs.get("flags") or {}

    meas_d = float(measured["distance_m"])
    meas_a = float(measured["angle_deg"])

    pred_d = predicted_tag_distance_m(
        x, y, h, obs["camera_role"], tag_x, tag_y
    )
    pred_a = predicted_tag_angle_deg(
        x, y, h, obs["camera_role"], tag_x, tag_y
    )

    raw_d = pred_d - meas_d
    raw_a = wrap_angle_deg(pred_a - meas_a)

    wd = float(weights.get("distance", 1.0))
    wa = float(weights.get("angle", 1.0))

    nd = (
        math.sqrt(base_config.distance_global_weight * wd)
        * raw_d
        / base_config.distance_sigma_m
    )
    na = (
        math.sqrt(base_config.angle_global_weight * wa)
        * raw_a
        / base_config.angle_sigma_deg
    )

    yaw_used = False
    meas_y = None
    pred_y = None
    raw_y = None
    ny = None

    if (
        bool(flags.get("yaw_use_offline_label"))
        and measured.get("yaw_sign_corrected_deg") is not None
        and tag_yaw is not None
    ):
        wy = float(weights.get("yaw", 1.0))
        if wy > 0.0:
            yaw_used = True
            meas_y = float(measured["yaw_sign_corrected_deg"])
            pred_y = predicted_tag_yaw_deg(
                x,
                y,
                h,
                obs["camera_role"],
                tag_x,
                tag_y,
                float(tag_yaw),
            )
            raw_y = wrap_angle_deg(pred_y - meas_y)
            ny = (
                math.sqrt(base_config.yaw_global_weight * wy)
                * raw_y
                / base_config.yaw_sigma_deg
            )

    components = [nd, na]
    if ny is not None:
        components.append(ny)

    # RMS normalized residual prevents an observation with yaw from being
    # penalized merely because it has one extra component.
    score = math.sqrt(
        sum(v * v for v in components) / len(components)
    )

    return {
        "sample_uid": result.sample_uid,
        "observation_uid": str(obs.get("observation_uid") or ""),
        "tag_id": tag_id,
        "camera_role": str(obs.get("camera_role") or ""),

        "measured_distance_m": meas_d,
        "predicted_distance_m": pred_d,
        "distance_residual_m": raw_d,
        "normalized_distance_residual": nd,

        "measured_angle_deg": meas_a,
        "predicted_angle_deg": pred_a,
        "angle_residual_deg": raw_a,
        "normalized_angle_residual": na,

        "yaw_used": yaw_used,
        "measured_yaw_deg": meas_y,
        "predicted_yaw_deg": pred_y,
        "yaw_residual_deg": raw_y,
        "normalized_yaw_residual": ny,

        "combined_score": score,

        "rejected": False,
        "rejection_reason": "",
    }


def solve_robust_twopass(
    sample: Dict[str, Any],
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
    base_config: DistanceAngleYawSolverConfig,
    robust_config: RobustTwoPassConfig,
) -> Tuple[SolverResult, List[Dict[str, Any]]]:
    """
    Return:
        (final SolverResult, per-observation residual records)
    """
    pass1 = solve_distance_angle_yaw(
        sample=sample,
        tag_pose_map=tag_pose_map,
        config=base_config,
    )

    observations = list(sample.get("observations") or [])

    if not pass1.success:
        return pass1, []

    residual_rows = [
        _observation_residual_record(
            obs=obs,
            result=pass1,
            tag_pose_map=tag_pose_map,
            base_config=base_config,
        )
        for obs in observations
        if int(obs["tag_id"]) in tag_pose_map
    ]

    ranked = sorted(
        residual_rows,
        key=lambda r: float(r["combined_score"]),
        reverse=True,
    )

    reject_uids: List[str] = []

    if ranked and len(observations) > robust_config.min_observations_after_rejection:
        worst = ranked[0]
        second_score = (
            float(ranked[1]["combined_score"])
            if len(ranked) >= 2
            else 0.0
        )

        worst_score = float(worst["combined_score"])
        score_gap = worst_score - second_score

        if (
            worst_score >= robust_config.rejection_score_threshold
            and score_gap >= robust_config.min_score_gap
        ):
            reject_uids.append(str(worst["observation_uid"]))

    # Baseline implementation intentionally rejects at most one observation.
    reject_uids = reject_uids[: robust_config.max_rejections]

    if not reject_uids:
        pass1.extra = dict(pass1.extra or {})
        pass1.extra.update({
            "robust_passes": 1,
            "rejected_observation_uids": [],
        })
        return pass1, residual_rows

    filtered_observations = [
        obs
        for obs in observations
        if str(obs.get("observation_uid") or "") not in set(reject_uids)
    ]

    if len(filtered_observations) < robust_config.min_observations_after_rejection:
        pass1.extra = dict(pass1.extra or {})
        pass1.extra.update({
            "robust_passes": 1,
            "rejected_observation_uids": [],
            "rejection_skipped_reason": "too_few_observations_after_rejection",
        })
        return pass1, residual_rows

    sample2 = dict(sample)
    sample2["observations"] = filtered_observations
    sample2["observation_count"] = len(filtered_observations)

    pass2 = solve_distance_angle_yaw(
        sample=sample2,
        tag_pose_map=tag_pose_map,
        config=base_config,
    )

    if not pass2.success:
        # Conservative fallback: keep Pass 1 rather than turn a valid solve
        # into a failure because the robust retry failed.
        pass1.extra = dict(pass1.extra or {})
        pass1.extra.update({
            "robust_passes": 2,
            "rejected_observation_uids": reject_uids,
            "pass2_failed_fallback_to_pass1": True,
        })
        return pass1, residual_rows

    rejected_tag_ids = []
    for row in residual_rows:
        if str(row["observation_uid"]) in set(reject_uids):
            row["rejected"] = True
            row["rejection_reason"] = "worst_normalized_residual"
            rejected_tag_ids.append(int(row["tag_id"]))

    pass2.tags_input = list(pass1.tags_input)
    pass2.tags_rejected = rejected_tag_ids
    pass2.extra = dict(pass2.extra or {})
    pass2.extra.update({
        "robust_passes": 2,
        "rejected_observation_uids": reject_uids,
    })

    return pass2, residual_rows
