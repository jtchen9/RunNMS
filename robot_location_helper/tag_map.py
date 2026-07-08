from __future__ import annotations

"""
Tag-location map loader for the robot-location helper.

Reads only:
    sitemap/<site>/tag_location.txt

Does not read:
    restriction_map.npy
"""

import re
from pathlib import Path
from typing import Dict, Tuple


FACING_TO_YAW = {
    "right": 0.0,
    "up": 90.0,
    "left": 180.0,
    "down": 270.0,
}


def load_tag_pose_map(
    path: Path | str,
) -> Dict[int, Tuple[float, float, float]]:
    """
    Return:
        tag_id -> (x_m, y_m, yaw_deg)

    Accepts both known coordinate spellings:
        Tag#40: (1.200, 4.703)
        Tag#40: (1.200 4.703)
    """
    p = Path(path)

    if not p.exists():
        raise FileNotFoundError(
            f"Tag location map not found: {p}"
        )

    lines = p.read_text(
        encoding="utf-8",
        errors="ignore",
    ).splitlines()

    facing = None
    out: Dict[int, Tuple[float, float, float]] = {}

    facing_re = re.compile(
        r"^\s*===\s*Tags Facing\s+"
        r"(Down|Right|Up|Left)\s*$",
        re.I,
    )

    tag_re = re.compile(
        r"Tag#\s*(\d+)\s*:\s*"
        r"\(\s*([-+]?\d+(?:\.\d+)?)"
        r"\s*,?\s+"
        r"([-+]?\d+(?:\.\d+)?)\s*\)"
    )

    for line in lines:
        m = facing_re.search(line)
        if m:
            facing = m.group(1).lower()
            continue

        m = tag_re.search(line)
        if m and facing:
            tag_id = int(m.group(1))
            out[tag_id] = (
                float(m.group(2)),
                float(m.group(3)),
                float(FACING_TO_YAW[facing]),
            )

    if not out:
        raise ValueError(
            f"No tags parsed from: {p}"
        )

    return out


def build_tag_xy_map(
    tag_pose_map: Dict[int, Tuple[float, float, float]],
) -> Dict[int, Tuple[float, float]]:
    return {
        tag_id: (pose[0], pose[1])
        for tag_id, pose in tag_pose_map.items()
    }
