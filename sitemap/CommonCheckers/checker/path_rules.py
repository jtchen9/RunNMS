from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from .script_model import InitialPose, ScriptRow
from .static_safety_core import (
    apply_mobility_move_pose,
    charging_zone_crossing_issues,
    deg_norm_360,
    first_restricted_sample_on_segment,
    load_restriction_map_from_policy,
    macro_planned_pose,
    point_in_bounds,
    point_restriction_status,
    robot_clearance_issues,
)


def _with_row_context(issue: Dict[str, Any], row: ScriptRow) -> Dict[str, Any]:
    out = dict(issue)
    out["row_number"] = row.row_number
    out["scanner"] = row.scanner
    out["action"] = row.action
    return out


def _append_out_of_bounds_issue(
    issues: List[Dict[str, Any]],
    *,
    row: ScriptRow,
    pose: Dict[str, float],
    bounds: Dict[str, Any],
    endpoint_name: str,
) -> None:
    issues.append({
        "level": "error",
        "code": "PATH_ENDPOINT_OUT_OF_BOUNDS",
        "row_number": row.row_number,
        "scanner": row.scanner,
        "action": row.action,
        "message": (
            f"{endpoint_name} pose ({pose['x_m']:.3f}, {pose['y_m']:.3f}) is outside "
            f"workspace bounds x=[{bounds['x_min_m']}, {bounds['x_max_m']}], "
            f"y=[{bounds['y_min_m']}, {bounds['y_max_m']}]."
        ),
        "suggestion": "Choose a target inside the site workspace bounds.",
    })


def _append_restriction_map_issue(
    issues: List[Dict[str, Any]],
    *,
    row: ScriptRow,
    restricted_sample: Dict[str, Any],
) -> None:
    if restricted_sample.get("out_of_map"):
        code = "PATH_LEAVES_RESTRICTION_MAP"
        detail = (
            f"planned path leaves restriction_map.npy near "
            f"({restricted_sample['x_m']:.3f}, {restricted_sample['y_m']:.3f}), "
            f"row={restricted_sample['row']}, col={restricted_sample['col']}."
        )
        suggestion = "Choose a target/path inside the site map."
    else:
        code = "PATH_CROSSES_RESTRICTION_MAP"
        detail = (
            "planned path touches a restricted cell in restriction_map.npy "
            f"near ({restricted_sample['x_m']:.3f}, {restricted_sample['y_m']:.3f}), "
            f"row={restricted_sample['row']}, col={restricted_sample['col']}."
        )
        suggestion = "Choose a target/path that avoids restricted map cells."

    issues.append({
        "level": "error",
        "code": code,
        "row_number": row.row_number,
        "scanner": row.scanner,
        "action": row.action,
        "message": detail,
        "suggestion": suggestion,
        "sample": restricted_sample,
    })


def check_planned_path_rules(
    rows: List[ScriptRow],
    initial_poses: Dict[str, InitialPose],
    macro_policy: Dict[str, Any],
    zone_policy: Dict[str, Any],
    path_policy: Dict[str, Any],
    safety_policy: Dict[str, Any],
    site_dir: str | Path,
) -> List[Dict[str, Any]]:
    """
    P5 preflight planned-path adapter.

    This wrapper simulates planned poses from CSV rows, then calls shared
    static_safety_core.py functions. Runtime safety should use the same shared
    core with true poses and runtime command context.

    Checks:
    - initial poses are inside workspace bounds and outside restriction_map.npy
    - normal mobility.move endpoints are inside workspace bounds
    - macro endpoints are inside workspace bounds
    - normal mobility.move paths do not enter/cross charging-zone circles
    - normal mobility.move paths do not cross restriction_map.npy restricted cells
    - macro path legality is handled by P4 and macro endpoints are checked here
    - moving robot path/target keeps clearance from other planned robot poses
    """
    issues: List[Dict[str, Any]] = []

    site_dir = Path(site_dir)
    bounds = path_policy.get("workspace_bounds", {}) or {}
    charging_zones = list(zone_policy.get("charging_zones", []) or [])
    macro_cfg_by_action = dict(macro_policy.get("macros", {}) or {})
    robot_safety_radius_m = float(safety_policy.get("robot_safety_radius_m", 0.25))
    restriction_map, restriction_cfg, map_issues = load_restriction_map_from_policy(site_dir, path_policy)
    issues.extend(map_issues)

    planned_by_scanner: Dict[str, Dict[str, float]] = {}

    for scanner, pose in initial_poses.items():
        p = {
            "x_m": float(pose.x_m),
            "y_m": float(pose.y_m),
            "heading_deg": deg_norm_360(float(pose.heading_deg)),
        }
        planned_by_scanner[scanner] = p

        if bounds and not point_in_bounds(p["x_m"], p["y_m"], bounds):
            issues.append({
                "level": "error",
                "code": "INITIAL_POSE_OUT_OF_BOUNDS",
                "row_number": pose.row_number,
                "scanner": scanner,
                "message": (
                    f"initial pose ({p['x_m']:.3f}, {p['y_m']:.3f}) is outside "
                    f"workspace bounds x=[{bounds['x_min_m']}, {bounds['x_max_m']}], "
                    f"y=[{bounds['y_min_m']}, {bounds['y_max_m']}]."
                ),
                "suggestion": "Choose an initial pose inside the site workspace bounds.",
            })

        if restriction_map is not None:
            status = point_restriction_status(
                restriction_map,
                restriction_cfg,
                p["x_m"],
                p["y_m"],
            )
            if status["restricted"]:
                code = "INITIAL_POSE_OUT_OF_RESTRICTION_MAP" if status["out_of_map"] else "INITIAL_POSE_IN_RESTRICTION_MAP"
                issues.append({
                    "level": "error",
                    "code": code,
                    "row_number": pose.row_number,
                    "scanner": scanner,
                    "message": (
                        f"initial pose ({p['x_m']:.3f}, {p['y_m']:.3f}) is unsafe "
                        f"for restriction_map.npy row={status['row']}, col={status['col']}."
                    ),
                    "suggestion": "Move the robot out of the restricted/charging zone before the experiment.",
                    "map_value": status["value"],
                    "out_of_map": status["out_of_map"],
                })

    for row in sorted(rows, key=lambda r: (r.t_offset_sec, r.row_number)):
        if row.category != "mobility":
            continue

        current = planned_by_scanner.get(row.scanner)

        if current is None or row.action == "mobility.report.location":
            continue

        if row.action == "mobility.move":
            new_pose, move_issues = apply_mobility_move_pose(current, row.args)
            for issue in move_issues:
                issues.append(_with_row_context(issue, row))
            if move_issues:
                continue

            if bounds and not point_in_bounds(new_pose["x_m"], new_pose["y_m"], bounds):
                _append_out_of_bounds_issue(
                    issues,
                    row=row,
                    pose=new_pose,
                    bounds=bounds,
                    endpoint_name="target",
                )

            for issue in charging_zone_crossing_issues(
                start_pose=current,
                end_pose=new_pose,
                charging_zones=charging_zones,
            ):
                issues.append(_with_row_context(issue, row))

            other_robot_poses = {
                other_scanner: pose
                for other_scanner, pose in planned_by_scanner.items()
                if other_scanner != row.scanner
            }
            for issue in robot_clearance_issues(
                moving_scanner=row.scanner,
                start_pose=current,
                end_pose=new_pose,
                other_robot_poses=other_robot_poses,
                safety_radius_m=robot_safety_radius_m,
            ):
                issues.append(_with_row_context(issue, row))

            restricted_sample = first_restricted_sample_on_segment(
                restriction_map,
                restriction_cfg,
                float(current["x_m"]),
                float(current["y_m"]),
                float(new_pose["x_m"]),
                float(new_pose["y_m"]),
            )
            if restricted_sample is not None:
                _append_restriction_map_issue(
                    issues,
                    row=row,
                    restricted_sample=restricted_sample,
                )

            planned_by_scanner[row.scanner] = new_pose
            continue

        if row.action in macro_cfg_by_action:
            new_pose = macro_planned_pose(macro_cfg_by_action[row.action])

            if bounds and not point_in_bounds(new_pose["x_m"], new_pose["y_m"], bounds):
                _append_out_of_bounds_issue(
                    issues,
                    row=row,
                    pose=new_pose,
                    bounds=bounds,
                    endpoint_name="macro target",
                )

            other_robot_poses = {
                other_scanner: pose
                for other_scanner, pose in planned_by_scanner.items()
                if other_scanner != row.scanner
            }
            for issue in robot_clearance_issues(
                moving_scanner=row.scanner,
                start_pose=current,
                end_pose=new_pose,
                other_robot_poses=other_robot_poses,
                safety_radius_m=robot_safety_radius_m,
            ):
                issues.append(_with_row_context(issue, row))

            planned_by_scanner[row.scanner] = new_pose
            continue

    return issues
