"""
location_tagmap.py

Tag-map parser for DemoRoom tag_location.txt.

This module is reusable. It does not generate reports and does not depend on
test files. It only parses a tag-location text file when called.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict

from .location_config import TAG_YAW_BY_FACING


@dataclass(frozen=True)
class TagWorld:
    tag_id: int
    x_m: float
    y_m: float
    yaw_deg: float
    facing: str
    room: str


def load_tag_map(tag_file_path: Path | str) -> Dict[int, TagWorld]:
    """
    Parse sitemap/DemoRoom/tag_location.txt.

    Returns:
        dict[tag_id] = TagWorld(...)
    """
    path = Path(tag_file_path)
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()

    room = None
    facing = None
    tag_map: Dict[int, TagWorld] = {}

    room_re = re.compile(r"^\s*(Outer Room|Inner Room)\s*$", re.I)
    facing_re = re.compile(r"^\s*===\s*Tags Facing\s+(Down|Right|Up|Left)\s*$", re.I)
    tag_re = re.compile(
        r"Tag#\s*(\d+)\s*:\s*\(\s*([-+]?\d+(?:\.\d+)?)\s*,\s*([-+]?\d+(?:\.\d+)?)\s*\)"
    )

    for line in lines:
        m = room_re.search(line)
        if m:
            room = "outer" if m.group(1).lower().startswith("outer") else "inner"
            continue

        m = facing_re.search(line)
        if m:
            facing = m.group(1).lower()
            continue

        m = tag_re.search(line)
        if m and room and facing:
            tag_id = int(m.group(1))
            x_m = float(m.group(2))
            y_m = float(m.group(3))
            yaw_deg = TAG_YAW_BY_FACING[facing]

            tag_map[tag_id] = TagWorld(
                tag_id=tag_id,
                x_m=x_m,
                y_m=y_m,
                yaw_deg=yaw_deg,
                facing=facing,
                room=room,
            )

    if not tag_map:
        raise ValueError(f"No tags were parsed from {path}")

    return tag_map
