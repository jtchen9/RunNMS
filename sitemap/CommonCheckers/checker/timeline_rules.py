from __future__ import annotations

from typing import Any, Dict, List

from .script_model import ScriptRow


def check_first_move_after_report_location_spacing(
    rows: List[ScriptRow],
    policy: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Strict timing rule:
    for each scanner, the first moving mobility command must be at least
    min_seconds_after_report_location_before_first_move after that scanner's
    earliest mobility.report.location row.
    """
    issues: List[Dict[str, Any]] = []

    moving = set(policy.get("moving_mobility_actions", []))
    report_action = str(policy.get("first_mobility_action_required", "mobility.report.location"))
    min_gap = int(policy.get("min_seconds_after_report_location_before_first_move", 60))

    by_scanner: Dict[str, List[ScriptRow]] = {}
    for row in rows:
        if row.category == "mobility":
            by_scanner.setdefault(row.scanner, []).append(row)

    for scanner, scanner_rows in sorted(by_scanner.items()):
        ordered = sorted(scanner_rows, key=lambda r: (r.t_offset_sec, r.row_number))

        report_rows = [r for r in ordered if r.action == report_action]
        if not report_rows:
            # check_first_mobility_command() reports the missing/bad first row.
            continue

        first_report = min(report_rows, key=lambda r: (r.t_offset_sec, r.row_number))
        moving_rows = [r for r in ordered if r.action in moving]
        if not moving_rows:
            continue

        first_move = min(moving_rows, key=lambda r: (r.t_offset_sec, r.row_number))
        gap = int(first_move.t_offset_sec) - int(first_report.t_offset_sec)

        if gap < min_gap:
            issues.append({
                "level": "error",
                "code": "FIRST_MOVE_TOO_SOON_AFTER_REPORT_LOCATION",
                "row_number": first_move.row_number,
                "scanner": first_move.scanner,
                "action": first_move.action,
                "message": (
                    f"first moving mobility command for {scanner} at row {first_move.row_number} "
                    f"starts {gap} seconds after mobility.report.location row "
                    f"{first_report.row_number}; minimum is {min_gap} seconds."
                ),
                "suggestion": (
                    f"Move row {first_move.row_number} to at least "
                    f"t_offset_sec={first_report.t_offset_sec + min_gap}."
                ),
                "report_location_row_number": first_report.row_number,
                "minimum_gap_sec": min_gap,
                "actual_gap_sec": gap,
            })

    return issues


def check_global_moving_command_spacing(
    rows, policy, initial_poses=None, macro_policy=None,
):
    """Keep the public name; permit nearby-in-time moves only with safe paths.

    Missing geometry fails closed to the original timing rule. All pairs in
    the time window are checked, including non-adjacent rows and same robots.
    """
    from .concurrent_movement_rules import planned_segments, segment_distance
    import math

    moving = set(policy.get("moving_mobility_actions", []))
    min_gap = int(policy.get("global_min_seconds_between_moving_mobility_commands", 180))
    clearance = float(policy.get("concurrent_movement_min_path_separation_m", 1.0))
    if min_gap <= 0 or not math.isfinite(clearance) or clearance < 1.0:
        raise ValueError("Movement safety policy requires positive spacing and separation >= 1 m")
    ordered = sorted(
        [r for r in rows if r.category == "mobility" and r.action in moving],
        key=lambda r: (r.t_offset_sec, r.row_number),
    )
    segments = planned_segments(ordered, initial_poses or {}, macro_policy or {})
    issues = []
    for i, cur in enumerate(ordered):
        for prev in reversed(ordered[:i]):
            gap = cur.t_offset_sec - prev.t_offset_sec
            if gap >= min_gap:
                break
            distance = None
            if cur.scanner != prev.scanner:
                a, b = segments.get(prev.row_number), segments.get(cur.row_number)
                if a is not None and b is not None:
                    distance = segment_distance(a, b)
                    if distance >= clearance:
                        continue
            reason = (
                "same robot" if cur.scanner == prev.scanner else
                "path geometry unavailable" if distance is None else
                f"minimum path separation {distance:.6f} m < {clearance:g} m"
            )
            issues.append({
                "level": "error", "code": "MOVING_COMMANDS_TOO_CLOSE",
                "row_number": cur.row_number, "scanner": cur.scanner,
                "action": cur.action, "previous_row_number": prev.row_number,
                "previous_scanner": prev.scanner, "previous_action": prev.action,
                "minimum_gap_sec": min_gap, "actual_gap_sec": gap,
                "minimum_path_separation_m": clearance,
                "actual_path_separation_m": distance,
                "message": f"Rows {prev.row_number} and {cur.row_number}: {reason}; "
                           f"start times are {gap}s apart, require {min_gap}s.",
                "suggestion": f"Schedule row {cur.row_number} at or after "
                              f"t_offset_sec={prev.t_offset_sec + min_gap}, "
                              "or separate different robots' entire paths.",
            })
    return issues
