#!/usr/bin/env python3
from __future__ import annotations

r"""
Cross-check robot_location_v1_1 helper against the current proven
interactive final pipeline.

Default placement:
    D:\Data\_Action\_RunNMS\tools\crosscheck_robot_location_helper.py

Usage:
    python tools\crosscheck_robot_location_helper.py ^
      --input-json "path\to\latest_input_twin-scout-charlie.json"

The input JSON is the passive S3 dump created earlier:
    {
        "dump_kind": "input",
        "scanner": "...",
        "payload": [ ... visible tags ... ]
    }

The script compares:
    A. new concise helper
    B. existing t11_collect_validation_final_componentwise
       current final pipeline

No state-machine change is made.
"""

import argparse
import math
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
LOCATION_SOLVER_DIR = ROOT_DIR / "locationSolver"
TEST_LOCATION_DIR = ROOT_DIR / "testLocation"

for p in (
    ROOT_DIR,
    LOCATION_SOLVER_DIR,
    TEST_LOCATION_DIR,
):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from robot_location_helper import (
    solve_robot_location,
)

import locationSolver.output.t11_collect_validation_final_componentwise as current_validation


def wrap_angle_deg(a: float) -> float:
    return float(
        (float(a) + 180.0) % 360.0 - 180.0
    )


def s3_visible_to_current_observations(
    visible: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Adapt exact S3 visible-tag dump to the observation dictionaries
    expected by the proven interactive current pipeline.
    """
    out = []

    for idx, obs in enumerate(
        visible,
        start=1,
    ):
        out.append({
            "observation_uid":
                f"crosscheck_obs_{idx:03d}",

            "tag_id": int(obs["id"]),

            "camera_role": str(
                obs.get("camera_role") or ""
            ).strip().lower(),

            "measured": {
                "distance_m":
                    float(obs["distance_m"]),

                "angle_deg":
                    float(obs["angle_deg_cw"]),

                "yaw_deg":
                    (
                        None
                        if obs.get("yaw_deg") is None
                        else float(obs["yaw_deg"])
                    ),

                "yaw_sign_corrected_deg": None,
            },

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
        })

    return out


def main() -> None:
    p = argparse.ArgumentParser()

    p.add_argument(
        "--input-json",
        required=True,
    )

    args = p.parse_args()

    import json

    obj = json.loads(
        Path(args.input_json).read_text(
            encoding="utf-8"
        )
    )

    visible = obj.get("payload")
    if not isinstance(visible, list):
        raise ValueError(
            "Expected S3 input dump with list payload"
        )

    new_result = solve_robot_location(
        visible_tags=visible,
        sample_uid="crosscheck_new_helper",
    )

    old_observations = (
        s3_visible_to_current_observations(
            visible
        )
    )

    old_run = (
        current_validation
        ._run_current_final_pipeline(
            old_observations
        )
    )

    old_result = old_run.get(
        "final_result"
    )

    print("")
    print("=" * 88)
    print("ROBOT LOCATION HELPER CROSS-CHECK")
    print("=" * 88)

    if not new_result.get("location_ok"):
        print(
            "NEW HELPER FAILED:",
            new_result.get("detail"),
        )
        raise SystemExit(1)

    if old_result is None or not old_result.success:
        print(
            "PROVEN CURRENT PIPELINE FAILED"
        )
        raise SystemExit(1)

    nx = float(new_result["x_m"])
    ny = float(new_result["y_m"])
    nh = float(new_result["heading_deg"])

    ox = float(old_result.estimated_x_m)
    oy = float(old_result.estimated_y_m)
    oh = float(old_result.estimated_heading_deg)

    dpos = math.hypot(
        nx - ox,
        ny - oy,
    )

    dhead = abs(
        wrap_angle_deg(
            nh - oh
        )
    )

    print(
        f"New helper : "
        f"x={nx:.9f}, y={ny:.9f}, h={nh:.9f}"
    )

    print(
        f"Proven old : "
        f"x={ox:.9f}, y={oy:.9f}, h={oh:.9f}"
    )

    print("")
    print(
        f"Position difference : "
        f"{dpos:.12f} m"
    )
    print(
        f"Heading difference  : "
        f"{dhead:.12f} deg"
    )

    passed = (
        dpos < 1e-9
        and dhead < 1e-9
    )

    print("")
    print(
        "RESULT:",
        "PASS" if passed else "FAIL",
    )

    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
