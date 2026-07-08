from __future__ import annotations

"""
Operational rare-case reason policy.

This module combines:
    - localization-confidence rare reasons
    - optional S5 decision rare reasons

It performs no file I/O.
"""

from typing import Any, Dict, List, Optional

from .correction_decision import (
    rare_case_reasons_from_decision,
)


def collect_rare_case_reasons(
    *,
    location_result: Dict[str, Any],
    correction_decision: Optional[
        Dict[str, Any]
    ] = None,
) -> List[str]:
    """
    Return stable, de-duplicated rare-case reason codes.

    Position discrepancy > 8 cm is intentionally NOT a generic trigger.
    """
    out = list(
        location_result.get("rare_case_reasons")
        or []
    )

    if correction_decision is not None:
        out.extend(
            rare_case_reasons_from_decision(
                correction_decision
            )
        )

    return list(
        dict.fromkeys(
            str(x)
            for x in out
            if str(x).strip()
        )
    )
