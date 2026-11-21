from __future__ import annotations

import math
from typing import Any, Dict, List

from .script_model import InitialPose, ScriptRow
from .planned_reentry import move_target
from .static_safety_core import deg_norm_360, macro_planned_pose


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
    macro_policy: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """
    Reject any single script-level mobility.move whose planned distance is too long.

    This is an offline/preflight rule. It simulates planned poses from the
    script writer's I/J roster and checks only semantic mobility.move
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
        if row.category == "system" and row.action == "system.shutdown":
            planned_by_scanner.pop(row.scanner, None)
            continue
        if row.category != "mobility":
            continue

        current = planned_by_scanner.get(row.scanner)
        if row.scanner not in initial_poses:
            continue

        if row.action == "mobility.report.location":
            # A report has no measured result offline; unknown stays unknown.
            continue

        if row.action in (macro_policy or {}).get("macros", {}):
            if current is not None:
                planned_by_scanner[row.scanner] = macro_planned_pose(macro_policy["macros"][row.action])
            continue

        if row.action != "mobility.move":
            # Low-level commands are vocabulary errors; site macros are handled by
            # macro/path rules.
            continue

        new_pose, move_issues = move_target(current, row.args)
        for issue in move_issues:
            issues.append(_with_row_context(issue, row))
        if move_issues:
            continue

        if current is None:
            # Endpoint known, incoming distance unknown until runtime report.
            planned_by_scanner[row.scanner] = new_pose
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
