from __future__ import annotations

import math
from typing import Any, Dict, List

from .script_model import InitialPose, ScriptRow
from .static_safety_core import apply_mobility_move_pose, deg_norm_360


def _with_row_context(issue: Dict[str, Any], row: ScriptRow) -> Dict[str, Any]:
    out = dict(issue)
    out["row_number"] = row.row_number
    out["scanner"] = row.scanner
    out["action"] = row.action
    return out


def check_max_single_mobility_move_distance(
    rows: List[ScriptRow],
    initial_poses: Dict[str, InitialPose],
    policy: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Reject any single script-level mobility.move whose planned distance is too long.

    This is an offline/preflight rule. It simulates planned poses from the
    script writer's initial_poses.csv and checks only semantic mobility.move
    rows. Site macros are checked by macro_rules.py / path_rules.py because
    their distance and legality are defined by site macro policy.
    """
    issues: List[Dict[str, Any]] = []

    try:
        max_distance_m = float(policy.get("max_single_mobility_move_distance_m", 3.0))
    except Exception:
        max_distance_m = 3.0

    planned_by_scanner: Dict[str, Dict[str, float]] = {
        scanner: {
            "x_m": float(pose.x_m),
            "y_m": float(pose.y_m),
            "heading_deg": deg_norm_360(float(pose.heading_deg)),
        }
        for scanner, pose in initial_poses.items()
    }

    for row in sorted(rows, key=lambda r: (r.t_offset_sec, r.row_number)):
        if row.category != "mobility":
            continue

        current = planned_by_scanner.get(row.scanner)
        if current is None:
            # check_initial_poses_exist() already reports this.
            continue

        if row.action == "mobility.report.location":
            # Offline checker assumes report.location confirms the intended pose.
            continue

        if row.action != "mobility.move":
            # Low-level commands are vocabulary errors; site macros are handled by
            # macro/path rules.
            continue

        new_pose, move_issues = apply_mobility_move_pose(current, row.args)
        for issue in move_issues:
            issues.append(_with_row_context(issue, row))
        if move_issues:
            continue

        distance_m = math.hypot(
            float(new_pose["x_m"]) - float(current["x_m"]),
            float(new_pose["y_m"]) - float(current["y_m"]),
        )

        if distance_m > max_distance_m:
            issues.append({
                "level": "error",
                "code": "MOBILITY_MOVE_DISTANCE_TOO_LONG",
                "row_number": row.row_number,
                "scanner": row.scanner,
                "action": row.action,
                "message": (
                    f"mobility.move at row {row.row_number} moves {distance_m:.3f} m; "
                    f"maximum allowed single move is {max_distance_m:.3f} m."
                ),
                "suggestion": (
                    "Split this movement into shorter mobility.move rows, each no "
                    f"longer than {max_distance_m:.3f} m, with enough time between movements."
                ),
                "distance_m": distance_m,
                "max_distance_m": max_distance_m,
                "start_x_m": float(current["x_m"]),
                "start_y_m": float(current["y_m"]),
                "target_x_m": float(new_pose["x_m"]),
                "target_y_m": float(new_pose["y_m"]),
            })

        planned_by_scanner[row.scanner] = new_pose

    return issues
