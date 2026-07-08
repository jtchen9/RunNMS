from __future__ import annotations

"""
Adapter from the exact S3 visible-tag interface to the frozen solver sample.
"""

import math
from typing import Any, Dict, List, Tuple

from .config import (
    FRONT_MAX_ABS_ANGLE_DEG,
    FRONT_MAX_DISTANCE_M,
)


def _finite_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        x = float(value)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def screen_visible_tag(
    obs: Dict[str, Any],
) -> Tuple[bool, str]:
    """
    Frozen Layer-1 whole-tag screen.

    Reject only:
      - invalid camera role / distance / angle
      - front |angle| > 30 deg
      - front distance > 5.5 m

    No rear-distance gate.
    No rear-angle gate.
    """
    role = str(
        obs.get("camera_role") or ""
    ).strip().lower()

    d = _finite_float(
        obs.get("distance_m")
    )
    a = _finite_float(
        obs.get("angle_deg_cw")
    )

    if role not in {"front", "rear"}:
        return False, "unknown_camera_role"

    if d is None or d <= 0.0:
        return False, "invalid_distance"

    if a is None:
        return False, "invalid_angle"

    if role == "front":
        if d > FRONT_MAX_DISTANCE_M:
            return False, "front_distance_gt_5.5m"

        if abs(a) > FRONT_MAX_ABS_ANGLE_DEG:
            return False, "front_abs_angle_gt_30deg"

    return True, "usable"


def build_solver_sample(
    visible_tags: List[Dict[str, Any]],
    sample_uid: str,
) -> Tuple[
    Dict[str, Any],
    List[Dict[str, Any]],
    Dict[str, int],
]:
    """
    Convert exact S3 visible-tag dictionaries into solver observations.

    Required S3 fields:
        id
        camera_role
        distance_m
        angle_deg_cw
        yaw_deg

    snapshot_path is preserved when present.
    """
    usable = []
    rejected_rows = []
    reason_counts: Dict[str, int] = {}

    for idx, obs in enumerate(
        list(visible_tags or []),
        start=1,
    ):
        ok, reason = screen_visible_tag(obs)

        if not ok:
            reason_counts[reason] = (
                reason_counts.get(reason, 0) + 1
            )

            rejected_rows.append({
                "tag_id": obs.get("id"),
                "camera_role": obs.get("camera_role"),
                "reason": reason,
            })
            continue

        yaw = _finite_float(
            obs.get("yaw_deg")
        )

        row = {
            "observation_uid":
                f"{sample_uid}_obs_{idx:03d}",

            "tag_id": int(obs["id"]),

            "camera_role": str(
                obs.get("camera_role") or ""
            ).strip().lower(),

            "measured": {
                "distance_m": float(obs["distance_m"]),
                "angle_deg": float(obs["angle_deg_cw"]),
                "yaw_deg": yaw,

                # Explicitly disable old truth-assisted fields.
                "yaw_sign_corrected_deg": None,
                "yaw_resolved_deg": None,
            },

            # Live S3 input has no historical hidden weights.
            "weights": {
                "distance": 1.0,
                "angle": 1.0,
                "yaw": 1.0,
            },

            "flags": {
                "yaw_use_offline_label": False,
            },

            "snapshot_path": str(
                obs.get("snapshot_path") or ""
            ),
        }

        usable.append(row)

    sample = {
        "sample_uid": str(sample_uid),
        "observation_count": len(usable),
        "observations": usable,
    }

    return (
        sample,
        rejected_rows,
        reason_counts,
    )
