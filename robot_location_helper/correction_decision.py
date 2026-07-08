from __future__ import annotations

"""
S5-facing follow-up correction decision.

The state machine will later consume only:
    result["go"]

No heading-only trigger is implemented.

Rationale:
    The robot may intentionally finish at a preferred direction that differs
    from planned orientation. Therefore this first policy is position-only.
"""

import math
from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class CorrectionDecisionConfig:
    position_error_go_threshold_m: float = 0.08


DEFAULT_CORRECTION_DECISION_CONFIG = (
    CorrectionDecisionConfig()
)


def decide_followup_correction(
    *,
    location_result: Dict[str, Any],
    planned_location: Dict[str, Any],
    config: CorrectionDecisionConfig = (
        DEFAULT_CORRECTION_DECISION_CONFIG
    ),
) -> Dict[str, Any]:
    """
    Three-gate rule:

      1. dpos <= 8 cm
            -> NO_GO

      2. dpos > 8 cm and confidence LOW
            -> NO_GO

      3. dpos > 8 cm and confidence MEDIUM/HIGH
            -> GO

    Heading discrepancy is intentionally ignored.
    """
    if not bool(location_result.get("location_ok")):
        return {
            "go": False,
            "decision": "NO_GO",
            "reason_code": "LOCATION_RESULT_NOT_OK",
            "position_error_m": None,
            "position_threshold_m":
                config.position_error_go_threshold_m,
            "location_confidence_level": "LOW",
            "detail": (
                "follow-up correction blocked because "
                "location result is not usable"
            ),
        }

    try:
        tx = float(location_result["x_m"])
        ty = float(location_result["y_m"])

        px = float(planned_location["x_m"])
        py = float(planned_location["y_m"])
    except Exception as exc:
        return {
            "go": False,
            "decision": "NO_GO",
            "reason_code":
                "INVALID_LOCATION_OR_PLANNED_POSE",

            "position_error_m": None,

            "position_threshold_m":
                config.position_error_go_threshold_m,

            "location_confidence_level": "LOW",

            "detail": (
                "follow-up correction blocked: "
                f"{type(exc).__name__}: {exc}"
            ),
        }

    dpos = math.hypot(
        tx - px,
        ty - py,
    )

    confidence = (
        location_result.get("confidence")
        or {}
    )

    level = str(
        confidence.get("level") or "LOW"
    ).upper()

    threshold = float(
        config.position_error_go_threshold_m
    )

    if dpos <= threshold:
        return {
            "go": False,
            "decision": "NO_GO",

            "reason_code":
                "POSITION_ERROR_WITHIN_8CM",

            "position_error_m": dpos,
            "position_threshold_m": threshold,

            "location_confidence_level":
                level,

            "detail": (
                "follow-up correction not justified: "
                "position discrepancy is within 8 cm"
            ),
        }

    if level == "LOW":
        return {
            "go": False,
            "decision": "NO_GO",

            "reason_code":
                "LARGE_ERROR_BUT_LOW_LOCATION_CONFIDENCE",

            "position_error_m": dpos,
            "position_threshold_m": threshold,

            "location_confidence_level":
                level,

            "detail": (
                "follow-up correction blocked: "
                "position discrepancy exceeds 8 cm "
                "but location confidence is LOW"
            ),
        }

    return {
        "go": True,
        "decision": "GO",

        "reason_code":
            "POSITION_ERROR_GT_8CM_AND_CONFIDENCE_SUFFICIENT",

        "position_error_m": dpos,
        "position_threshold_m": threshold,

        "location_confidence_level":
            level,

        "detail": (
            "follow-up correction justified"
        ),
    }


def rare_case_reasons_from_decision(
    decision: Dict[str, Any],
) -> list[str]:
    """
    Sparse-log triggers produced by the correction policy.
    """
    code = str(
        decision.get("reason_code") or ""
    )

    out = []

    if code == (
        "LARGE_ERROR_BUT_LOW_LOCATION_CONFIDENCE"
    ):
        out.append(
            "S5_LARGE_DISCREPANCY_BLOCKED_LOW_CONFIDENCE"
        )

    elif code == (
        "POSITION_ERROR_GT_8CM_AND_CONFIDENCE_SUFFICIENT"
    ):
        out.append(
            "S5_POSITION_DISCREPANCY_GT_8CM"
        )

    return out
