from __future__ import annotations

"""
Public robot-location helper API.

This module owns only:
    - tag_location.txt loading
    - robot pose estimation from normalized S3 visible-tag observations

It does not:
    - read Redis
    - load mobility reports
    - save true_location_json
    - propagate old poses
    - issue commands
    - change state
    - read restriction_map.npy
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .component_preparation import (
    compute_pass1_component_rows,
    decide_distance_use_from_m2,
    prepare_final_componentwise_sample,
)
from .config import (
    FINAL_CONFIG,
    M2_CONFIG,
    PASS1_CONFIG,
    SOLVER_VERSION,
    YAW_CONFIG,
)
from .final_solver import (
    solve_componentwise_distance_angle_yaw,
)
from .confidence import (
    assess_location_confidence,
    compute_final_residual_metrics,
    rare_case_reasons_from_confidence,
)
from .observation import build_solver_sample
from .pass1_solver import solve_distance_angle
from .tag_map import (
    build_tag_xy_map,
    load_tag_pose_map,
)
from .geometry import wrap_angle_deg


ROOT_DIR = Path(__file__).resolve().parents[1]

DEFAULT_TAG_MAP_PATH = (
    ROOT_DIR
    / "sitemap"
    / "DemoRoom"
    / "tag_location.txt"
)


def _local_ts() -> str:
    return datetime.now().strftime(
        "%Y-%m-%d-%H:%M:%S"
    )


def _failure(
    *,
    detail: str,
    tags_observed: List[int],
    tags_layer1_usable: List[int],
    layer1_rejected: List[Dict[str, Any]],
    layer1_rejected_reason_counts: Dict[str, int],
    pass1: Any = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "location_ok": False,
        "source": "apriltag_componentwise",
        "solver_version": SOLVER_VERSION,
        "solver_stage": SOLVER_VERSION,
        "updated_at": _local_ts(),
        "detail": str(detail),

        "tags_observed": tags_observed,
        "tags_layer1_usable": tags_layer1_usable,
        "tags_used": [],
        "tag_count": 0,

        "layer1_rejected": layer1_rejected,
        "layer1_rejected_reason_counts":
            layer1_rejected_reason_counts,
    }

    if pass1 is not None:
        out["pass1"] = (
            pass1.to_dict()
            if hasattr(pass1, "to_dict")
            else pass1
        )

    return out


def solve_robot_location(
    visible_tags: List[Dict[str, Any]],
    *,
    tag_map_path: Optional[Path | str] = None,
    sample_uid: str = "s3_live_location",
) -> Dict[str, Any]:
    """
    Solve robot location from the exact normalized S3 visible-tag list.

    Input item contract:
        {
            "id": 31,
            "camera_role": "front" | "rear",
            "distance_m": 2.6387,
            "angle_deg_cw": -12.9471,
            "yaw_deg": 18.4852,
            "snapshot_path": "...",       # optional

            # Extra S3 fields are allowed and ignored:
            "camera_offset_deg": ...,
            "camera_forward_offset_m": ...,
            "bearing_robot_deg_ccw": ...
        }

    Output keeps the existing S3 compatibility fields:
        location_ok
        x_m
        y_m
        heading_deg
        source
        tags_used
        tag_count
        solver_stage
        updated_at
        detail

    Rich diagnostics are additive and may initially be ignored by S3.
    """
    visible = list(visible_tags or [])

    tags_observed = []
    for obs in visible:
        try:
            tags_observed.append(int(obs["id"]))
        except Exception:
            pass

    sample, layer1_rejected, reason_counts = (
        build_solver_sample(
            visible_tags=visible,
            sample_uid=sample_uid,
        )
    )

    tags_layer1_usable = [
        int(obs["tag_id"])
        for obs in sample["observations"]
    ]

    if len(sample["observations"]) < 2:
        return _failure(
            detail=(
                "insufficient Layer-1 usable "
                "distance+angle observations"
            ),
            tags_observed=tags_observed,
            tags_layer1_usable=tags_layer1_usable,
            layer1_rejected=layer1_rejected,
            layer1_rejected_reason_counts=reason_counts,
        )

    map_path = Path(
        tag_map_path or DEFAULT_TAG_MAP_PATH
    )

    try:
        tag_pose_map = load_tag_pose_map(
            map_path
        )
    except Exception as exc:
        return _failure(
            detail=(
                f"tag map load failed: "
                f"{type(exc).__name__}: {exc}"
            ),
            tags_observed=tags_observed,
            tags_layer1_usable=tags_layer1_usable,
            layer1_rejected=layer1_rejected,
            layer1_rejected_reason_counts=reason_counts,
        )

    tag_xy_map = build_tag_xy_map(
        tag_pose_map
    )

    pass1 = solve_distance_angle(
        sample=sample,
        tag_xy_map=tag_xy_map,
        config=PASS1_CONFIG,
    )

    if not pass1.success:
        return _failure(
            detail=(
                "Pass-1 D+A solve failed: "
                f"{pass1.failure_reason}"
            ),
            tags_observed=tags_observed,
            tags_layer1_usable=tags_layer1_usable,
            layer1_rejected=layer1_rejected,
            layer1_rejected_reason_counts=reason_counts,
            pass1=pass1,
        )

    component_rows = compute_pass1_component_rows(
        sample=sample,
        pass1=pass1,
        tag_xy_map=tag_xy_map,
        solver_config=PASS1_CONFIG,
    )

    distance_decisions = (
        decide_distance_use_from_m2(
            component_rows=component_rows,
            config=M2_CONFIG,
        )
    )

    final_sample, final_audit = (
        prepare_final_componentwise_sample(
            sample=sample,
            pass1=pass1,
            distance_decisions_by_uid=
                distance_decisions,
            tag_pose_map=tag_pose_map,
            yaw_config=YAW_CONFIG,
        )
    )

    final_result = (
        solve_componentwise_distance_angle_yaw(
            sample=final_sample,
            tag_pose_map=tag_pose_map,
            config=FINAL_CONFIG,
        )
    )

    if not final_result.success:
        return _failure(
            detail=(
                "final component-wise solve failed: "
                f"{final_result.failure_reason}"
            ),
            tags_observed=tags_observed,
            tags_layer1_usable=tags_layer1_usable,
            layer1_rejected=layer1_rejected,
            layer1_rejected_reason_counts=reason_counts,
            pass1=pass1,
        )

    distance_rejected_tags = [
        int(row["tag_id"])
        for row in final_audit
        if not bool(row.get("distance_use"))
    ]

    yaw_keep_tags = [
        int(row["tag_id"])
        for row in final_audit
        if bool(row.get("yaw_use"))
        and row.get("yaw_mode") == "keep"
    ]

    yaw_flip_tags = [
        int(row["tag_id"])
        for row in final_audit
        if bool(row.get("yaw_use"))
        and row.get("yaw_mode") == "flip"
    ]

    yaw_rejected_tags = [
        int(row["tag_id"])
        for row in final_audit
        if not bool(row.get("yaw_use"))
    ]

    active_distance = sum(
        1 for row in final_audit
        if bool(row.get("distance_use"))
    )
    active_angle = sum(
        1 for row in final_audit
        if bool(row.get("angle_use"))
    )
    active_yaw = sum(
        1 for row in final_audit
        if bool(row.get("yaw_use"))
    )

    floor_rows = [
        d for d in distance_decisions.values()
        if bool(
            d.get("kept_by_min_active_distance_floor")
        )
    ]

    tags_used = sorted({
        int(obs["tag_id"])
        for obs in final_sample.get(
            "observations"
        ) or []
    })

    pass1_to_final_shift_m = (
        (
            (
                float(final_result.estimated_x_m)
                - float(pass1.estimated_x_m)
            ) ** 2
            +
            (
                float(final_result.estimated_y_m)
                - float(pass1.estimated_y_m)
            ) ** 2
        ) ** 0.5
    )


    distance_candidate_count = sum(
        1
        for d in distance_decisions.values()
        if bool(d.get("m2_candidate"))
    )

    distance_rejected_fraction = (
        len(distance_rejected_tags)
        / max(1, len(final_audit))
    )

    pass1_heading_shift_deg = abs(
        wrap_angle_deg(
            float(final_result.estimated_heading_deg)
            - float(pass1.estimated_heading_deg)
        )
    )

    residual_metrics = (
        compute_final_residual_metrics(
            final_sample=final_sample,
            tag_pose_map=tag_pose_map,
            x_m=float(final_result.estimated_x_m),
            y_m=float(final_result.estimated_y_m),
            heading_deg=float(
                final_result.estimated_heading_deg
            ),
            solver_config=FINAL_CONFIG,
        )
    )

    confidence_metrics = {
        "tags_observed_count":
            len(tags_observed),

        "tags_layer1_usable_count":
            len(tags_layer1_usable),

        "active_distance_count":
            active_distance,

        "active_angle_count":
            active_angle,

        "active_yaw_count":
            active_yaw,

        "distance_m2_candidate_count":
            distance_candidate_count,

        "distance_rejected_count":
            len(distance_rejected_tags),

        "distance_rejected_fraction":
            distance_rejected_fraction,

        "distance_floor_activated":
            bool(floor_rows),

        "distance_kept_by_floor_count":
            len(floor_rows),

        "yaw_admitted_count":
            active_yaw,

        "yaw_rejected_count":
            len(yaw_rejected_tags),

        "pass1_to_final_shift_m":
            pass1_to_final_shift_m,

        "pass1_to_final_heading_shift_deg":
            pass1_heading_shift_deg,

        **residual_metrics,
    }

    confidence = assess_location_confidence(
        metrics=confidence_metrics,
    )

    rare_case_reasons = (
        rare_case_reasons_from_confidence(
            confidence
        )
    )

    return {
        # Existing S3 compatibility fields.
        "location_ok": True,

        "x_m": float(
            final_result.estimated_x_m
        ),
        "y_m": float(
            final_result.estimated_y_m
        ),
        "heading_deg": float(
            final_result.estimated_heading_deg
        ) % 360.0,

        "source": "apriltag_componentwise",
        "tags_used": tags_used,
        "tag_count": len(tags_used),
        "solver_stage": SOLVER_VERSION,
        "updated_at": _local_ts(),
        "detail": "component-wise location solved",

        # Additive operational diagnostics.
        "solver_version": SOLVER_VERSION,

        "tags_observed": tags_observed,
        "tags_layer1_usable":
            tags_layer1_usable,

        "layer1_rejected":
            layer1_rejected,

        "layer1_rejected_reason_counts":
            reason_counts,

        "active_components": {
            "distance": active_distance,
            "angle": active_angle,
            "yaw": active_yaw,
        },

        "distance_rejected_tags":
            distance_rejected_tags,

        "distance_floor_activated":
            bool(floor_rows),

        "distance_kept_by_floor_tags": [
            int(row["tag_id"])
            for row in floor_rows
        ],

        "yaw_keep_tags": yaw_keep_tags,
        "yaw_flip_tags": yaw_flip_tags,
        "yaw_rejected_tags":
            yaw_rejected_tags,

        "pass1": {
            "x_m": float(
                pass1.estimated_x_m
            ),
            "y_m": float(
                pass1.estimated_y_m
            ),
            "heading_deg": float(
                pass1.estimated_heading_deg
            ) % 360.0,
            "objective_value":
                pass1.objective_value,
        },

        "diagnostics": {
            "pass1_to_final_shift_m":
                pass1_to_final_shift_m,

            "pass1_to_final_heading_shift_deg":
                pass1_heading_shift_deg,

            "final_objective_value":
                final_result.objective_value,

            "m2_threshold":
                M2_CONFIG.abs_m2_threshold,

            "min_active_distances_after_m2":
                M2_CONFIG
                .min_active_distances_after_m2,

            **residual_metrics,
        },

        "confidence": confidence,

        "rare_case_reasons":
            rare_case_reasons,

        # Replay/debug material. S3 may ignore these.
        "component_audit": final_audit,
    }
