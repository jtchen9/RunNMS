#!/usr/bin/env python3
from __future__ import annotations

"""
Small deterministic self-test for the operational rare-case and S5 policy.
No state machine and no external data required.
"""

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from robot_location_helper import (
    collect_rare_case_reasons,
    decide_followup_correction,
)


def make_loc(
    *,
    x: float,
    y: float,
    level: str,
    rare: list[str],
) -> dict:
    return {
        "location_ok": True,
        "x_m": x,
        "y_m": y,
        "confidence": {
            "level": level,
        },
        "rare_case_reasons": rare,
    }


def main() -> None:
    planned = {
        "x_m": 0.0,
        "y_m": 0.0,
    }

    # <=8 cm -> NO_GO
    d1 = decide_followup_correction(
        location_result=make_loc(
            x=0.05,
            y=0.0,
            level="HIGH",
            rare=[],
        ),
        planned_location=planned,
    )
    assert d1["go"] is False

    # >8 cm + HIGH -> GO, but NOT a generic rare-case reason
    loc2 = make_loc(
        x=0.12,
        y=0.0,
        level="HIGH",
        rare=[],
    )
    d2 = decide_followup_correction(
        location_result=loc2,
        planned_location=planned,
    )
    assert d2["go"] is True
    assert collect_rare_case_reasons(
        location_result=loc2,
        correction_decision=d2,
    ) == []

    # >8 cm + LOW -> NO_GO and rare-case reason
    loc3 = make_loc(
        x=0.12,
        y=0.0,
        level="LOW",
        rare=["LOW_LOCATION_CONFIDENCE"],
    )
    d3 = decide_followup_correction(
        location_result=loc3,
        planned_location=planned,
    )
    reasons3 = collect_rare_case_reasons(
        location_result=loc3,
        correction_decision=d3,
    )

    assert d3["go"] is False
    assert (
        "S5_LARGE_DISCREPANCY_BLOCKED_LOW_CONFIDENCE"
        in reasons3
    )

    print("RESULT: PASS")


if __name__ == "__main__":
    main()
