from __future__ import annotations

from typing import Any, Dict, List

from .script_model import InitialPose, ScriptRow
from .static_safety_core import (
    apply_mobility_move_pose,
    bump_guard_crossing_issues,
    deg_norm_360,
    macro_planned_pose,
    macro_start_pose_issues,
)


def _with_row_context(issue: Dict[str, Any], row: ScriptRow) -> Dict[str, Any]:
    out = dict(issue)
    out["row_number"] = row.row_number
    out["scanner"] = row.scanner
    out["action"] = row.action
    return out


def check_macro_and_bump_rules(
    rows: List[ScriptRow],
    initial_poses: Dict[str, InitialPose],
    macro_policy: Dict[str, Any],
    bump_guard_zones: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    P4 preflight adapter.

    The spatial logic is shared in static_safety_core.py so the future runtime
    safety gate can use the same bump and macro-start checks with true poses.

    Rules:
    - known macros accept no custom args in v1
    - macro planned-current pose must be within configured start tolerance
    - normal mobility.move must not cross any bump guard rectangle
    - macro rows may cross the bump guard zone by design
    """
    issues: List[Dict[str, Any]] = []

    macro_cfg_by_action = dict(macro_policy.get("macros", {}) or {})

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

        # If the initial pose is missing, P2 already reports that. Avoid noisy
        # follow-on geometry errors here.
        if current is None:
            continue

        if row.action == "mobility.report.location":
            # Offline checker assumes report.location confirms the intended pose.
            # Runtime will compare learned true pose with this intended pose.
            continue

        if row.action == "mobility.move":
            new_pose, move_issues = apply_mobility_move_pose(current, row.args)
            for issue in move_issues:
                issues.append(_with_row_context(issue, row))
            if move_issues:
                continue

            for issue in bump_guard_crossing_issues(
                start_pose=current,
                end_pose=new_pose,
                bump_guard_zones=bump_guard_zones,
            ):
                issues.append(_with_row_context(issue, row))

            planned_by_scanner[row.scanner] = new_pose
            continue

        if row.action in macro_cfg_by_action:
            cfg = macro_cfg_by_action[row.action]

            if cfg.get("args_allowed") is False and row.args:
                issues.append({
                    "level": "error",
                    "code": "MACRO_ARGS_NOT_ALLOWED",
                    "row_number": row.row_number,
                    "scanner": row.scanner,
                    "action": row.action,
                    "message": f"{row.action} does not accept args_json in macro v1.",
                    "suggestion": "Use empty args_json {} for this macro.",
                })

            macro_start_issues = macro_start_pose_issues(current, cfg)
            for issue in macro_start_issues:
                issues.append(_with_row_context(issue, row))

            if any(x.get("code") == "MACRO_CONFIG_BAD_NUMERIC_VALUE" for x in macro_start_issues):
                continue

            planned_by_scanner[row.scanner] = macro_planned_pose(cfg)
            continue

    return issues
