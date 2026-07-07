from __future__ import annotations

import math
from copy import deepcopy
from dataclasses import dataclass
from statistics import median
from typing import Any, Dict, List, Tuple

from src.common.geometry import (
    predicted_tag_angle_deg,
    predicted_tag_distance_m,
    predicted_tag_yaw_deg,
    wrap_angle_deg,
)
from src.common.solver_result import SolverResult
from src.solvers.solver_2_distance_angle import DistanceAngleSolverConfig


@dataclass(frozen=True)
class FirstScreenConfig:
    front_max_abs_angle_deg: float = 30.0
    front_max_distance_m: float = 5.5


@dataclass(frozen=True)
class M2DistanceScreenConfig:
    abs_m2_threshold: float = 0.95


@dataclass(frozen=True)
class YawAdmissionConfig:
    min_abs_predicted_yaw_deg: float = 8.0
    yaw_accept_best_error_deg: float = 10.0
    yaw_accept_separation_deg: float = 15.0


def _finite_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        x = float(value)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def first_screen_reason(
    row: Dict[str, Any],
    config: FirstScreenConfig,
) -> str:
    """
    Whole-tag first screen.

    Reject only:
      - front AND |measured angle| > 30 deg
      - front AND measured distance > 5.5 m

    No rear-distance rule.
    """
    role = str(row.get("camera_role") or "").strip().lower()
    d = _finite_float(row.get("meas_distance_m"))
    a = _finite_float(row.get("meas_angle_deg"))

    if d is None or d <= 0.0:
        return "invalid_distance"
    if a is None:
        return "invalid_angle"

    if role == "front" and abs(a) > config.front_max_abs_angle_deg:
        return "front_edge_abs_angle_gt30"

    if role == "front" and d > config.front_max_distance_m:
        return "front_too_far_gt5p5"

    return ""


def first_screen_raw_rows(
    raw_rows: List[Dict[str, str]],
    config: FirstScreenConfig,
) -> Tuple[List[Dict[str, str]], List[Dict[str, Any]]]:
    """
    Returns:
      surviving rows
      full audit rows

    Important:
    Positive legacy weights are preserved.
    Legacy non-positive D/A weights are repaired to 1.0 for survivors,
    because the new first screen—not old hidden zero weights—defines
    whether a row enters the unchanged Stage-2 solver.
    """
    survivors: List[Dict[str, str]] = []
    audit: List[Dict[str, Any]] = []

    for row in raw_rows:
        reason = first_screen_reason(row, config)
        rejected = bool(reason)

        audit_row: Dict[str, Any] = dict(row)
        audit_row["new_first_screen_rejected"] = rejected
        audit_row["new_first_screen_reason"] = reason or "pass"

        if rejected:
            audit.append(audit_row)
            continue

        row2 = dict(row)

        old_dw = _finite_float(row2.get("distance_weight_default"))
        old_aw = _finite_float(row2.get("angle_weight_default"))

        repaired_d = old_dw is None or old_dw <= 0.0
        repaired_a = old_aw is None or old_aw <= 0.0

        # Preserve positive historical weighting, but remove hidden old
        # screening encoded as zero/non-positive weight.
        if repaired_d:
            row2["distance_weight_default"] = "1.0"
        if repaired_a:
            row2["angle_weight_default"] = "1.0"

        audit_row["distance_weight_repaired_for_new_pipeline"] = repaired_d
        audit_row["angle_weight_repaired_for_new_pipeline"] = repaired_a
        audit_row["effective_distance_weight"] = row2.get(
            "distance_weight_default"
        )
        audit_row["effective_angle_weight"] = row2.get(
            "angle_weight_default"
        )

        survivors.append(row2)
        audit.append(audit_row)

    return survivors, audit


def compute_pass1_component_rows(
    sample: Dict[str, Any],
    pass1: SolverResult,
    tag_xy_map: Dict[int, Tuple[float, float]],
    solver_config: DistanceAngleSolverConfig,
) -> List[Dict[str, Any]]:
    """
    Compute signed Pass-1 D/A residuals and M2 distance metric.

    M2_j = z_d,j - median(z_d,k for k != j)

    No GT is read.
    """
    if not pass1.success:
        return []

    x = float(pass1.estimated_x_m)
    y = float(pass1.estimated_y_m)
    h = float(pass1.estimated_heading_deg)

    rows: List[Dict[str, Any]] = []

    for obs in list(sample.get("observations") or []):
        tag_id = int(obs["tag_id"])
        if tag_id not in tag_xy_map:
            continue

        measured = obs.get("measured") or {}
        weights = obs.get("weights") or {}

        d = _finite_float(measured.get("distance_m"))
        a = _finite_float(measured.get("angle_deg"))
        wd = _finite_float(weights.get("distance", 1.0))
        wa = _finite_float(weights.get("angle", 1.0))

        if d is None or d <= 0.0 or a is None:
            continue
        if wd is None or wd <= 0.0 or wa is None or wa <= 0.0:
            continue

        tag_x, tag_y = tag_xy_map[tag_id]

        pred_d = predicted_tag_distance_m(
            x, y, h, obs["camera_role"], tag_x, tag_y
        )
        pred_a = predicted_tag_angle_deg(
            x, y, h, obs["camera_role"], tag_x, tag_y
        )

        rd_m = pred_d - d
        ra_deg = wrap_angle_deg(pred_a - a)

        zd = (
            math.sqrt(solver_config.distance_global_weight * wd)
            * rd_m
            / solver_config.distance_sigma_m
        )
        za = (
            math.sqrt(solver_config.angle_global_weight * wa)
            * ra_deg
            / solver_config.angle_sigma_deg
        )

        rows.append({
            "sample_uid": str(sample["sample_uid"]),
            "observation_uid": str(obs.get("observation_uid") or ""),
            "tag_id": tag_id,
            "camera_role": str(obs.get("camera_role") or ""),
            "distance_residual_m_signed": rd_m,
            "distance_residual_sigma_signed": zd,
            "angle_residual_deg_signed": ra_deg,
            "angle_residual_sigma_signed": za,
        })

    for row in rows:
        peers = [
            float(other["distance_residual_sigma_signed"])
            for other in rows
            if other["observation_uid"] != row["observation_uid"]
        ]

        peer_median = median(peers) if peers else 0.0
        m2 = (
            float(row["distance_residual_sigma_signed"])
            - peer_median
        )

        row["distance_peer_median_sigma"] = peer_median
        row["distance_m2_signed"] = m2
        row["distance_m2_abs"] = abs(m2)

    return rows


def decide_distance_use_from_m2(
    component_rows: List[Dict[str, Any]],
    config: M2DistanceScreenConfig,
) -> Dict[str, Dict[str, Any]]:
    """
    Direct M2 distance rejection. No middle holdout solver.
    """
    out: Dict[str, Dict[str, Any]] = {}

    for row in component_rows:
        uid = str(row["observation_uid"])
        m2 = float(row["distance_m2_signed"])
        reject = abs(m2) >= config.abs_m2_threshold

        out[uid] = {
            "distance_use": not reject,
            "distance_use_reason": (
                "rejected_m2_abs_ge_0p95"
                if reject
                else "kept_m2_abs_lt_0p95"
            ),
            "distance_m2_signed": m2,
            "distance_m2_abs": abs(m2),
        }

    return out


def resolve_yaw_gtfree(
    obs: Dict[str, Any],
    pass1: SolverResult,
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
    config: YawAdmissionConfig,
) -> Dict[str, Any]:
    """
    Same GT-free keep/flip/reject logic used in Stage-5.

    Reads raw measured yaw_deg only.
    Does not read:
      yaw_use_offline_label
      yaw_sign_corrected_deg
      GT pose
      true yaw
    """
    uid = str(obs.get("observation_uid") or "")
    tag_id = int(obs["tag_id"])
    role = str(obs.get("camera_role") or "")
    measured = obs.get("measured") or {}

    raw_yaw = _finite_float(measured.get("yaw_deg"))

    row: Dict[str, Any] = {
        "observation_uid": uid,
        "tag_id": tag_id,
        "yaw_use": False,
        "yaw_resolved_deg": None,
        "yaw_mode": "",
        "yaw_use_reason": "",
        "predicted_yaw_deg": None,
        "yaw_keep_abs_error_deg": None,
        "yaw_flip_abs_error_deg": None,
        "yaw_branch_separation_deg": None,
    }

    if raw_yaw is None:
        row["yaw_use_reason"] = "reject_missing_or_invalid_raw_yaw"
        return row

    if tag_id not in tag_pose_map:
        row["yaw_use_reason"] = "reject_missing_tag_pose"
        return row

    tag_x, tag_y, tag_yaw = tag_pose_map[tag_id]

    if tag_yaw is None:
        row["yaw_use_reason"] = "reject_missing_tag_yaw"
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
        "predicted_yaw_deg": pred_yaw,
        "yaw_keep_abs_error_deg": abs_keep,
        "yaw_flip_abs_error_deg": abs_flip,
        "yaw_branch_separation_deg": separation,
        "yaw_mode": best_mode,
    })

    if abs(float(pred_yaw)) < config.min_abs_predicted_yaw_deg:
        row["yaw_use_reason"] = "reject_small_predicted_yaw"
        return row

    if best_abs > config.yaw_accept_best_error_deg:
        row["yaw_use_reason"] = "reject_poor_keep_flip_match"
        return row

    if separation < config.yaw_accept_separation_deg:
        row["yaw_use_reason"] = "reject_ambiguous_low_separation"
        return row

    row["yaw_use"] = True
    row["yaw_resolved_deg"] = float(best_yaw)
    row["yaw_use_reason"] = f"accept_{best_mode}"
    return row


def prepare_final_componentwise_sample(
    sample: Dict[str, Any],
    pass1: SolverResult,
    distance_decisions_by_uid: Dict[str, Dict[str, Any]],
    tag_pose_map: Dict[int, Tuple[float, float, float | None]],
    yaw_config: YawAdmissionConfig,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Translate all settled decisions into the final solver contract.

    For each Layer-1-surviving observation:
      distance_use: direct M2 decision
      angle_use: True
      yaw_use: GT-free Stage-5 keep/flip/reject decision

    Output yaw:
      measured["yaw_resolved_deg"]

    This function intentionally removes dependence on old offline yaw fields.
    """
    sample2 = deepcopy(sample)
    observations2: List[Dict[str, Any]] = []
    audit_rows: List[Dict[str, Any]] = []

    for obs in list(sample.get("observations") or []):
        uid = str(obs.get("observation_uid") or "")

        obs2 = deepcopy(obs)
        flags2 = dict(obs2.get("flags") or {})
        measured2 = dict(obs2.get("measured") or {})

        distance_decision = distance_decisions_by_uid.get(uid)

        if distance_decision is None:
            distance_use = True
            distance_reason = "kept_no_m2_decision"
            m2_signed = None
            m2_abs = None
        else:
            distance_use = bool(distance_decision["distance_use"])
            distance_reason = str(
                distance_decision["distance_use_reason"]
            )
            m2_signed = distance_decision.get(
                "distance_m2_signed"
            )
            m2_abs = distance_decision.get(
                "distance_m2_abs"
            )

        # Settled decision: no second-stage angle screening.
        angle_use = True
        angle_reason = "kept_no_second_screen"

        yaw_decision = resolve_yaw_gtfree(
            obs=obs,
            pass1=pass1,
            tag_pose_map=tag_pose_map,
            config=yaw_config,
        )

        yaw_use = bool(yaw_decision["yaw_use"])
        yaw_reason = str(yaw_decision["yaw_use_reason"])

        flags2["distance_use"] = distance_use
        flags2["angle_use"] = angle_use
        flags2["yaw_use"] = yaw_use

        flags2["distance_use_reason"] = distance_reason
        flags2["angle_use_reason"] = angle_reason
        flags2["yaw_use_reason"] = yaw_reason

        # Explicitly disable the old truth-assisted yaw path.
        flags2["yaw_use_offline_label"] = False
        measured2["yaw_sign_corrected_deg"] = None

        measured2["yaw_resolved_deg"] = (
            float(yaw_decision["yaw_resolved_deg"])
            if yaw_use
            else None
        )

        # If yaw is admitted, do not allow an old zero yaw weight to
        # silently suppress it in the final solver.
        weights2 = dict(obs2.get("weights") or {})
        if yaw_use:
            old_yw = _finite_float(weights2.get("yaw", 1.0))
            if old_yw is None or old_yw <= 0.0:
                weights2["yaw"] = 1.0

        obs2["flags"] = flags2
        obs2["measured"] = measured2
        obs2["weights"] = weights2

        observations2.append(obs2)

        audit_rows.append({
            "sample_uid": str(sample["sample_uid"]),
            "observation_uid": uid,
            "tag_id": int(obs["tag_id"]),
            "camera_role": str(obs.get("camera_role") or ""),

            "distance_use": distance_use,
            "distance_use_reason": distance_reason,
            "distance_m2_signed": m2_signed,
            "distance_m2_abs": m2_abs,

            "angle_use": angle_use,
            "angle_use_reason": angle_reason,

            "yaw_use": yaw_use,
            "yaw_use_reason": yaw_reason,
            "yaw_mode": yaw_decision.get("yaw_mode"),
            "yaw_resolved_deg": yaw_decision.get(
                "yaw_resolved_deg"
            ),
            "predicted_yaw_deg": yaw_decision.get(
                "predicted_yaw_deg"
            ),
        })

    sample2["observations"] = observations2
    sample2["observation_count"] = len(observations2)

    return sample2, audit_rows
