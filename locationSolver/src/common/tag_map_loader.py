from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Tuple


_TAG_RE = re.compile(
    r"Tag#(?P<tag_id>\d+)\s*:\s*"
    r"\(\s*(?P<x>[-+]?\d+(?:\.\d+)?)\s*,?\s*"
    r"(?P<y>[-+]?\d+(?:\.\d+)?)\s*\)"
)


def load_tag_xy_map(path: Path) -> Dict[int, Tuple[float, float]]:
    """
    Parse tag_location.txt and return:
        {tag_id: (x_m, y_m)}

    The parser accepts both:
        Tag#40: (1.200, 4.703)
    and the older missing-comma form:
        Tag#40: (1.200 4.703)
    """
    if not path.exists():
        raise FileNotFoundError(f"Tag map not found: {path}")

    out: Dict[int, Tuple[float, float]] = {}

    for line_no, line in enumerate(
        path.read_text(encoding="utf-8-sig").splitlines(),
        start=1,
    ):
        m = _TAG_RE.search(line)
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
