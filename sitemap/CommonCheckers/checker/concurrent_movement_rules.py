"""Planned center-path geometry; no assumption about speed or dispatch order."""
from __future__ import annotations

import math
from .static_safety_core import (
    apply_mobility_move_pose, macro_planned_pose, distance_point_to_segment,
)


def planned_segments(rows, initial_poses, macro_policy):
    poses = {name: dict(x_m=p.x_m, y_m=p.y_m, heading_deg=p.heading_deg)
             for name, p in initial_poses.items()}
    macros = macro_policy.get("macros", {})
    segments = {}
    for row in rows:
        current = poses.get(row.scanner)
        if current is None:
            continue
        try:
            if row.action == "mobility.move":
                target, issues = apply_mobility_move_pose(current, row.args)
                if issues:
                    raise ValueError("Invalid move")
            elif row.action in macros:
                # Same offline macro contract as path_rules.py; macro rules
                # independently require the correct staging position.
                target = macro_planned_pose(macros[row.action])
            else:
                raise ValueError("Unknown moving action")
            segment = ((float(current["x_m"]), float(current["y_m"])),
                       (float(target["x_m"]), float(target["y_m"])))
            if not all(math.isfinite(v) for point in segment for v in point):
                raise ValueError("Non-finite geometry")
        except (KeyError, TypeError, ValueError, OverflowError):
            # Do not reuse a stale pose for subsequent movements of this robot.
            poses.pop(row.scanner, None)
            continue
        segments[row.row_number] = segment
        poses[row.scanner] = target
    return segments


def segment_distance(a, b):
    """Exact 2D segment distance, including crossings and zero-length moves."""
    p, q = a
    r, s = b

    def cross(u, v, w):
        return (v[0]-u[0])*(w[1]-u[1]) - (v[1]-u[1])*(w[0]-u[0])

    c1, c2 = cross(p, q, r), cross(p, q, s)
    c3, c4 = cross(r, s, p), cross(r, s, q)
    if ((c1 > 0 > c2 or c2 > 0 > c1) and
            (c3 > 0 > c4 or c4 > 0 > c3)):
        return 0.0
    return min(distance_point_to_segment(*p, *r, *s),
               distance_point_to_segment(*q, *r, *s),
               distance_point_to_segment(*r, *p, *q),
               distance_point_to_segment(*s, *p, *q))
