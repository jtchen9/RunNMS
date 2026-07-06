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
class ComponentAwareRobustConfig:
    """
    Component-aware robust rejection.

    Main idea:
      - distance + angle define a "core" inconsistency score
      - yaw is supporting evidence only
      - yaw alone cannot trigger rejection when core geometry is too weak

    Eligibility:
      A) core_score >= core_reject_threshold
         OR
      B) core_score >= core_support_threshold
         AND abs(normalized_yaw_residual) >= yaw_support_threshold

    Ranking among eligible candidates:
      decision_score =
          core_score
          + yaw_bonus_weight * max(
                0,
                abs(normalized_yaw_residual) - yaw_support_threshold
            )

    At most one observation is rejected in this first version.
    """

    core_reject_threshold: float = 1.00
    core_support_threshold: float = 0.60
    yaw_support_threshold: float = 2.00
    yaw_bonus_weight: float = 0.25

    min_decision_score_gap: float = 0.50

    max_rejections: int = 1
    min_observations_after_rejection: int = 3


def _residual_record(
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

    core_score = math.sqrt((nd * nd + na * na) / 2.0)

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

        "core_score": core_score,

        "eligible_for_rejection": False,
        "eligibility_reason": "",
        "decision_score": None,

        "rejected": False,
        "rejection_reason": "",
    }


def _apply_component_aware_decision(
    row: Dict[str, Any],
    config: ComponentAwareRobustConfig,
) -> None:
    core = float(row["core_score"])
    ny = row.get("normalized_yaw_residual")
    yaw_abs = None if ny is None else abs(float(ny))

    eligible = False
    reason = ""

    if core >= config.core_reject_threshold:
        eligible = True
        reason = "core_threshold"
    elif (
        core >= config.core_support_threshold
        and yaw_abs is not None
        and yaw_abs >= config.yaw_support_threshold
    ):
        eligible = True
        reason = "core_plus_yaw_support"

    row["eligible_for_rejection"] = eligible
    row["eligibility_reason"] = reason

    if not eligible:
        row["decision_score"] = core
        return

    yaw_bonus = 0.0
    if yaw_abs is not None:
        yaw_bonus = config.yaw_bonus_weight * max(
            0.0,
            yaw_abs - config.yaw_support_threshold,
        )

    row["decision_score"] = core + yaw_bonus


def solve_component_aware_twopass(
    sample: Dict[str, Any],
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
    base_config: DistanceAngleYawSolverConfig,
    robust_config: ComponentAwareRobustConfig,
) -> Tuple[SolverResult, List[Dict[str, Any]]]:

    pass1 = solve_distance_angle_yaw(
        sample=sample,
        tag_pose_map=tag_pose_map,
        config=base_config,
    )

    observations = list(sample.get("observations") or [])

    if not pass1.success:
        return pass1, []

    residual_rows = [
        _residual_record(
            obs=obs,
            result=pass1,
            tag_pose_map=tag_pose_map,
            base_config=base_config,
        )
        for obs in observations
        if int(obs["tag_id"]) in tag_pose_map
    ]

    for row in residual_rows:
        _apply_component_aware_decision(row, robust_config)

    eligible = [
        row for row in residual_rows
        if bool(row["eligible_for_rejection"])
    ]

    ranked = sorted(
        eligible,
        key=lambda r: float(r["decision_score"]),
        reverse=True,
    )

    reject_uids: List[str] = []

    if (
        ranked
        and len(observations)
        > robust_config.min_observations_after_rejection
    ):
        worst = ranked[0]
        second_score = (
            float(ranked[1]["decision_score"])
            if len(ranked) >= 2
            else 0.0
        )

        worst_score = float(worst["decision_score"])
        gap = worst_score - second_score

        if gap >= robust_config.min_decision_score_gap:
            reject_uids.append(str(worst["observation_uid"]))

    reject_uids = reject_uids[: robust_config.max_rejections]

    if not reject_uids:
        pass1.extra = dict(pass1.extra or {})
        pass1.extra.update({
            "robust_passes": 1,
            "rejected_observation_uids": [],
            "component_aware": True,
        })
        return pass1, residual_rows

    reject_set = set(reject_uids)

    filtered_observations = [
        obs
        for obs in observations
        if str(obs.get("observation_uid") or "") not in reject_set
    ]

    if (
        len(filtered_observations)
        < robust_config.min_observations_after_rejection
    ):
        pass1.extra = dict(pass1.extra or {})
        pass1.extra.update({
            "robust_passes": 1,
            "rejected_observation_uids": [],
            "component_aware": True,
            "rejection_skipped_reason":
                "too_few_observations_after_rejection",
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
        pass1.extra = dict(pass1.extra or {})
        pass1.extra.update({
            "robust_passes": 2,
            "rejected_observation_uids": reject_uids,
            "component_aware": True,
            "pass2_failed_fallback_to_pass1": True,
        })
        return pass1, residual_rows

    rejected_tag_ids = []

    for row in residual_rows:
        if str(row["observation_uid"]) in reject_set:
            row["rejected"] = True
            row["rejection_reason"] = "component_aware_worst_candidate"
            rejected_tag_ids.append(int(row["tag_id"]))

    pass2.tags_input = list(pass1.tags_input)
    pass2.tags_rejected = rejected_tag_ids
    pass2.extra = dict(pass2.extra or {})
    pass2.extra.update({
        "robust_passes": 2,
        "rejected_observation_uids": reject_uids,
        "component_aware": True,
    })

    return pass2, residual_rows
