from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, Tuple


_TAG_XY_RE = re.compile(
    r"Tag#(?P<tag_id>\d+)\s*:\s*"
    r"\(\s*(?P<x>[-+]?\d+(?:\.\d+)?)\s*,?\s*"
    r"(?P<y>[-+]?\d+(?:\.\d+)?)\s*\)"
)


def load_tag_xy_map(path: Path) -> Dict[int, Tuple[float, float]]:
    """
    Parse only tag x/y from tag_location.txt.

    Accepts both:
        Tag#40: (1.200, 4.703)
        Tag#40: (1.200 4.703)
    """
    if not path.exists():
        raise FileNotFoundError(f"Tag map not found: {path}")

    out: Dict[int, Tuple[float, float]] = {}

    for line_no, line in enumerate(
        path.read_text(encoding="utf-8-sig").splitlines(),
        start=1,
    ):
        m = _TAG_XY_RE.search(line)
        if not m:
            continue

        tag_id = int(m.group("tag_id"))
        x_m = float(m.group("x"))
        y_m = float(m.group("y"))

        if tag_id in out:
            raise ValueError(
                f"Duplicate Tag#{tag_id} in {path} at line {line_no}"
            )

        out[tag_id] = (x_m, y_m)

    if not out:
        raise ValueError(f"No tag coordinates parsed from: {path}")

    return out


def load_tag_yaw_json(path: Path) -> Dict[int, float]:
    """
    Load explicit tag yaw/facing map from JSON.

    Expected form:
        {
          "31": 270.0,
          "32": 270.0
        }

    Missing tag IDs are allowed.
    """
    if not path.exists():
        raise FileNotFoundError(f"Tag yaw map not found: {path}")

    raw = json.loads(path.read_text(encoding="utf-8"))

    if not isinstance(raw, dict):
        raise ValueError(f"Tag yaw JSON must be an object: {path}")

    out: Dict[int, float] = {}

    for key, value in raw.items():
        tag_id = int(key)
        out[tag_id] = float(value) % 360.0

    return out


def build_tag_pose_map(
    tag_xy_map: Dict[int, Tuple[float, float]],
    tag_yaw_map: Dict[int, float],
) -> Dict[int, Tuple[float, float, float | None]]:
    """
    Combine canonical x/y map with a separate yaw map.

    A tag can have yaw=None when its orientation is not supplied.
    Solver #3 will simply not use yaw for that tag.
    """
    out: Dict[int, Tuple[float, float, float | None]] = {}

    for tag_id, (x_m, y_m) in tag_xy_map.items():
        out[tag_id] = (
            float(x_m),
            float(y_m),
            (
                None
                if tag_id not in tag_yaw_map
                else float(tag_yaw_map[tag_id]) % 360.0
            ),
        )

    return out
