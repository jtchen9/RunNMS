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
from src.solvers.solver_2_distance_angle import (
    DistanceAngleSolverConfig,
    solve_distance_angle,
)
from src.solvers.solver_3_distance_angle_yaw import (
    DistanceAngleYawSolverConfig,
    solve_distance_angle_yaw,
)


@dataclass(frozen=True)
class Stage5Config:
    """
    Solver #5: GT-free yaw two-pass estimator.

    Pass 1:
      - distance + view angle only
      - no yaw
      - no ground truth

    Between passes:
      - screen at most one distance/angle outlier from Pass-1 residuals
      - resolve raw yaw sign from Pass-1 predicted yaw
      - accept yaw only when keep/flip choice is confident

    Pass 2:
      - cleaned distance + angle observations
      - qualified GT-free sign-corrected yaw
      - full distance + angle + yaw solve

    No per-sample GT is used anywhere in the estimation path.
    """

    # Frozen, conservative Stage-12 choice.
    distance_sigma_m: float = 0.05
    angle_sigma_deg: float = 2.5
    yaw_sigma_deg: float = 3.0

    distance_global_weight: float = 1.0
    angle_global_weight: float = 1.0
    yaw_global_weight: float = 0.75

    # Distance/angle-only robust standout rule.
    rejection_score_threshold: float = 0.80
    min_score_gap: float = 0.50
    max_rejections: int = 1
    min_observations_after_rejection: int = 3

    # GT-free yaw qualification.
    #
    # These mirror the earlier physical yaw diagnostic gates, except:
    #   - predicted yaw from Pass 1 replaces true yaw
    #   - measured view angle / existing geometry gates replace true-angle use
    min_abs_predicted_yaw_deg: float = 8.0
    yaw_accept_best_error_deg: float = 10.0
    yaw_accept_separation_deg: float = 15.0


def _make_pass1_config(config: Stage5Config) -> DistanceAngleSolverConfig:
    return DistanceAngleSolverConfig(
        distance_sigma_m=config.distance_sigma_m,
        angle_sigma_deg=config.angle_sigma_deg,
        distance_global_weight=config.distance_global_weight,
        angle_global_weight=config.angle_global_weight,
    )


def _make_pass2_config(config: Stage5Config) -> DistanceAngleYawSolverConfig:
    return DistanceAngleYawSolverConfig(
        distance_sigma_m=config.distance_sigma_m,
        angle_sigma_deg=config.angle_sigma_deg,
        yaw_sigma_deg=config.yaw_sigma_deg,
        distance_global_weight=config.distance_global_weight,
        angle_global_weight=config.angle_global_weight,
        yaw_global_weight=config.yaw_global_weight,
    )


def _da_residual_record(
    obs: Dict[str, Any],
    pass1: SolverResult,
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
    config: Stage5Config,
) -> Dict[str, Any]:
    x = float(pass1.estimated_x_m)
    y = float(pass1.estimated_y_m)
    h = float(pass1.estimated_heading_deg)

    tag_id = int(obs["tag_id"])
    tag_x, tag_y, _ = tag_pose_map[tag_id]

    measured = obs.get("measured") or {}
    weights = obs.get("weights") or {}

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
        math.sqrt(config.distance_global_weight * wd)
        * raw_d
        / config.distance_sigma_m
    )
    na = (
        math.sqrt(config.angle_global_weight * wa)
        * raw_a
        / config.angle_sigma_deg
    )

    score = math.sqrt((nd * nd + na * na) / 2.0)

    return {
        "sample_uid": pass1.sample_uid,
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

        "da_combined_score": score,

        "rejected": False,
        "rejection_reason": "",
    }


def _choose_da_rejection(
    residual_rows: List[Dict[str, Any]],
    observation_count: int,
    config: Stage5Config,
) -> List[str]:
    if (
        config.max_rejections <= 0
        or observation_count <= config.min_observations_after_rejection
        or not residual_rows
    ):
        return []

    ranked = sorted(
        residual_rows,
        key=lambda r: float(r["da_combined_score"]),
        reverse=True,
    )

    worst = ranked[0]
    second_score = (
        float(ranked[1]["da_combined_score"])
        if len(ranked) >= 2
        else 0.0
    )

    worst_score = float(worst["da_combined_score"])
    score_gap = worst_score - second_score

    if (
        worst_score >= config.rejection_score_threshold
        and score_gap >= config.min_score_gap
    ):
        return [str(worst["observation_uid"])][:config.max_rejections]

    return []


def _resolve_yaw_gtfree(
    obs: Dict[str, Any],
    pass1: SolverResult,
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
    config: Stage5Config,
) -> Dict[str, Any]:
    """
    Resolve raw measured yaw sign using Pass-1 geometry only.

    Candidate branches:
        keep = +raw_yaw
        flip = -raw_yaw

    Reference:
        predicted yaw from Pass-1 x/y/heading and known tag facing.

    No GT pose, true yaw, offline yaw label, or sign-corrected yaw is read.
    """
    uid = str(obs.get("observation_uid") or "")
    tag_id = int(obs["tag_id"])
    role = str(obs.get("camera_role") or "")
    measured = obs.get("measured") or {}

    raw_yaw = measured.get("yaw_deg")
    tag_x, tag_y, tag_yaw = tag_pose_map[tag_id]

    row = {
        "sample_uid": pass1.sample_uid,
        "observation_uid": uid,
        "tag_id": tag_id,
        "camera_role": role,

        "raw_yaw_deg": None,
        "predicted_yaw_deg": None,

        "yaw_keep_deg": None,
        "yaw_flip_deg": None,

        "yaw_keep_error_deg": None,
        "yaw_flip_error_deg": None,
        "yaw_keep_abs_error_deg": None,
        "yaw_flip_abs_error_deg": None,

        "best_mode": "",
        "best_yaw_deg": None,
        "best_abs_error_deg": None,
        "branch_separation_deg": None,

        "accepted": False,
        "decision": "",
    }

    if raw_yaw is None:
        row["decision"] = "reject_missing_raw_yaw"
        return row

    try:
        raw_yaw = float(raw_yaw)
    except Exception:
        row["decision"] = "reject_invalid_raw_yaw"
        return row

    if not math.isfinite(raw_yaw):
        row["decision"] = "reject_invalid_raw_yaw"
        return row

    if tag_yaw is None:
        row["decision"] = "reject_missing_tag_yaw"
        return row

    pred_yaw = predicted_tag_yaw_deg(
        float(pass1.estimated_x_m),
        float(pass1.estimated_y_m),
        float(pass1.estimated_heading_deg),
        role,
        float(tag_x),
        float(tag_y),
        float(tag_yaw),
    )

    keep = wrap_angle_deg(raw_yaw)
    flip = wrap_angle_deg(-raw_yaw)

    err_keep = wrap_angle_deg(keep - pred_yaw)
    err_flip = wrap_angle_deg(flip - pred_yaw)

    abs_keep = abs(float(err_keep))
    abs_flip = abs(float(err_flip))

    if abs_keep <= abs_flip:
        best_mode = "keep"
        best_yaw = keep
        best_abs = abs_keep
    else:
        best_mode = "flip"
        best_yaw = flip
        best_abs = abs_flip

    separation = abs(abs_keep - abs_flip)

    row.update({
        "raw_yaw_deg": raw_yaw,
        "predicted_yaw_deg": pred_yaw,

        "yaw_keep_deg": keep,
        "yaw_flip_deg": flip,

        "yaw_keep_error_deg": err_keep,
        "yaw_flip_error_deg": err_flip,
        "yaw_keep_abs_error_deg": abs_keep,
        "yaw_flip_abs_error_deg": abs_flip,

        "best_mode": best_mode,
        "best_yaw_deg": best_yaw,
        "best_abs_error_deg": best_abs,
        "branch_separation_deg": separation,
    })

    # Small predicted yaw is intrinsically weak for sign discrimination.
    if abs(float(pred_yaw)) < config.min_abs_predicted_yaw_deg:
        row["decision"] = "reject_small_predicted_yaw"
        return row

    if best_abs > config.yaw_accept_best_error_deg:
        row["decision"] = "reject_poor_keep_flip_match"
        return row

    if separation < config.yaw_accept_separation_deg:
        row["decision"] = "reject_ambiguous_low_separation"
        return row

    row["accepted"] = True
    row["decision"] = f"accept_{best_mode}"
    return row


def _build_pass2_sample(
    sample: Dict[str, Any],
    reject_uids: List[str],
    yaw_decisions_by_uid: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    reject_set = set(reject_uids)
    observations2: List[Dict[str, Any]] = []

    for obs in list(sample.get("observations") or []):
        uid = str(obs.get("observation_uid") or "")
        if uid in reject_set:
            continue

        obs2 = dict(obs)
        measured2 = dict(obs.get("measured") or {})
        flags2 = dict(obs.get("flags") or {})

        # Explicitly remove truth-assisted yaw path.
        flags2["yaw_use_offline_label"] = False
        measured2["yaw_sign_corrected_deg"] = None

        decision = yaw_decisions_by_uid.get(uid)
        if decision and bool(decision.get("accepted")):
            flags2["yaw_use_offline_label"] = True
            measured2["yaw_sign_corrected_deg"] = float(
                decision["best_yaw_deg"]
            )

        obs2["measured"] = measured2
        obs2["flags"] = flags2
        observations2.append(obs2)

    sample2 = dict(sample)
    sample2["observations"] = observations2
    sample2["observation_count"] = len(observations2)
    return sample2


def solve_stage5_gtfree_yaw(
    sample: Dict[str, Any],
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
    config: Stage5Config,
) -> Tuple[
    SolverResult,
    SolverResult,
    List[Dict[str, Any]],
    List[Dict[str, Any]],
]:
    """
    Returns:
        final_result
        pass1_result
        da_residual_rows
        yaw_decision_rows
    """
    tag_xy_map = {
        int(tag_id): (float(pose[0]), float(pose[1]))
        for tag_id, pose in tag_pose_map.items()
    }

    # ------------------------------------------------------------------
    # Pass 1: distance + angle only. No yaw. No GT.
    # ------------------------------------------------------------------
    pass1 = solve_distance_angle(
        sample=sample,
        tag_xy_map=tag_xy_map,
        config=_make_pass1_config(config),
    )

    observations = list(sample.get("observations") or [])

    if not pass1.success:
        return pass1, pass1, [], []

    # ------------------------------------------------------------------
    # D/A-only residual screening.
    # ------------------------------------------------------------------
    da_rows = [
        _da_residual_record(
            obs=obs,
            pass1=pass1,
            tag_pose_map=tag_pose_map,
            config=config,
        )
        for obs in observations
        if int(obs["tag_id"]) in tag_pose_map
    ]

    reject_uids = _choose_da_rejection(
        residual_rows=da_rows,
        observation_count=len(observations),
        config=config,
    )

    reject_set = set(reject_uids)

    for row in da_rows:
        if str(row["observation_uid"]) in reject_set:
            row["rejected"] = True
            row["rejection_reason"] = "worst_da_normalized_residual"

    # ------------------------------------------------------------------
    # GT-free yaw sign resolution from Pass-1 pose.
    # Do not use offline yaw flags or sign-corrected yaw.
    # ------------------------------------------------------------------
    yaw_rows = [
        _resolve_yaw_gtfree(
            obs=obs,
            pass1=pass1,
            tag_pose_map=tag_pose_map,
            config=config,
        )
        for obs in observations
        if int(obs["tag_id"]) in tag_pose_map
    ]

    # Rejected D/A observation cannot contribute yaw to Pass 2.
    for row in yaw_rows:
        if str(row["observation_uid"]) in reject_set:
            row["accepted"] = False
            row["decision"] = "reject_da_outlier"

    yaw_by_uid = {
        str(row["observation_uid"]): row
        for row in yaw_rows
    }

    # ------------------------------------------------------------------
    # Pass 2: cleaned D/A + qualified GT-free yaw.
    # Always run Pass 2, even when no D/A outlier was rejected, because
    # qualified yaw may now be added.
    # ------------------------------------------------------------------
    sample2 = _build_pass2_sample(
        sample=sample,
        reject_uids=reject_uids,
        yaw_decisions_by_uid=yaw_by_uid,
    )

    if (
        len(sample2["observations"])
        < config.min_observations_after_rejection
    ):
        pass1.extra = dict(pass1.extra or {})
        pass1.extra.update({
            "stage5_passes": 1,
            "stage5_rejected_observation_uids": [],
            "stage5_yaw_accepted_count": 0,
            "stage5_fallback_reason":
                "too_few_observations_after_rejection",
        })
        return pass1, pass1, da_rows, yaw_rows

    pass2 = solve_distance_angle_yaw(
        sample=sample2,
        tag_pose_map=tag_pose_map,
        config=_make_pass2_config(config),
    )

    accepted_yaw_count = sum(
        1 for row in yaw_rows
        if bool(row.get("accepted"))
    )

    if not pass2.success:
        pass1.extra = dict(pass1.extra or {})
        pass1.extra.update({
            "stage5_passes": 2,
            "stage5_rejected_observation_uids": reject_uids,
            "stage5_yaw_accepted_count": accepted_yaw_count,
            "stage5_pass2_failed_fallback_to_pass1": True,
        })
        return pass1, pass1, da_rows, yaw_rows

    rejected_tag_ids = [
        int(row["tag_id"])
        for row in da_rows
        if bool(row.get("rejected"))
    ]

    pass2.tags_input = list(pass1.tags_input)
    pass2.tags_rejected = rejected_tag_ids
    pass2.extra = dict(pass2.extra or {})
    pass2.extra.update({
        "stage5_passes": 2,
        "stage5_rejected_observation_uids": reject_uids,
        "stage5_yaw_accepted_count": accepted_yaw_count,
        "stage5_yaw_accept_keep_count": sum(
            1 for row in yaw_rows
            if bool(row.get("accepted"))
            and row.get("best_mode") == "keep"
        ),
        "stage5_yaw_accept_flip_count": sum(
            1 for row in yaw_rows
            if bool(row.get("accepted"))
            and row.get("best_mode") == "flip"
        ),
        "stage5_gtfree": True,
    })

    return pass2, pass1, da_rows, yaw_rows
