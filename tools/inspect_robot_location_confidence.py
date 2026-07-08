#!/usr/bin/env python3
from __future__ import annotations

r"""
Inspect confidence and GO/NO_GO without changing the state machine.

Example:
    python tools\inspect_robot_location_confidence.py ^
      --input-json "locationSolver\output\2026-07-08-09_15_33_twin-scout-charlie_input.json" ^
      --planned-x 9.0 ^
      --planned-y 1.5

The input JSON is the passive S3 dump:
    {"payload": [ ... visible tags ... ]}
"""

import argparse
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from robot_location_helper import (
    decide_followup_correction,
    solve_robot_location,
)


def main() -> None:
    p = argparse.ArgumentParser()

    p.add_argument(
        "--input-json",
        required=True,
    )

    p.add_argument(
        "--planned-x",
        type=float,
        default=None,
    )

    p.add_argument(
        "--planned-y",
        type=float,
        default=None,
    )

    args = p.parse_args()

    obj = json.loads(
        Path(args.input_json).read_text(
            encoding="utf-8"
        )
    )

    visible = obj.get("payload")
    if not isinstance(visible, list):
        raise ValueError(
            "Expected input JSON with list payload"
        )

    loc = solve_robot_location(
        visible_tags=visible,
        sample_uid="confidence_inspect",
    )

    print("")
    print("=" * 88)
    print("ROBOT LOCATION CONFIDENCE")
    print("=" * 88)
    print(
        json.dumps(
            {
                "location_ok":
                    loc.get("location_ok"),

                "x_m":
                    loc.get("x_m"),

                "y_m":
                    loc.get("y_m"),

                "heading_deg":
                    loc.get("heading_deg"),

                "confidence":
                    loc.get("confidence"),

                "rare_case_reasons":
                    loc.get("rare_case_reasons"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    if (
        args.planned_x is not None
        and args.planned_y is not None
    ):
        decision = decide_followup_correction(
            location_result=loc,
            planned_location={
                "x_m": args.planned_x,
                "y_m": args.planned_y,
            },
        )

        print("")
        print("=" * 88)
        print("S5 FOLLOW-UP CORRECTION DECISION")
        print("=" * 88)
        print(
            json.dumps(
                decision,
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
