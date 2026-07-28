from __future__ import annotations

from typing import Dict, List

from .script_model import InitialPose, ScriptRow


def _issue(row: ScriptRow, code: str, message: str, suggestion: str, *, level: str = "error", **details):
    out = {
        "level": level,
        "code": code,
        "row_number": row.row_number,
        "scanner": row.scanner,
        "category": row.category,
        "action": row.action,
        "message": message,
        "suggestion": suggestion,
    }
    out.update(details)
    return out


def check_traffic_sessions(
    rows: List[ScriptRow],
    initial_poses: Dict[str, InitialPose],
) -> List[dict]:
    """Validate the experiment-wide traffic session registry."""
    issues: List[dict] = []
    traffic_rows = sorted(
        [row for row in rows if row.category == "traffic"],
        key=lambda row: (row.t_offset_sec, row.row_number),
    )

    starts: Dict[str, ScriptRow] = {}
    for row in traffic_rows:
        if row.scanner not in initial_poses:
            issues.append(
                _issue(
                    row,
                    "TRAFFIC_TARGET_NOT_ROBOT",
                    f"traffic target is not an enabled robot in InitialPoses: {row.scanner}.",
                    "Choose an enabled DeviceType=robot target.",
                )
            )

        if row.action != "traffic.session.start":
            continue

        session_id = str((row.args or {}).get("session_id") or "").strip()
        if not session_id:
            continue

        first = starts.get(session_id)
        if first is None:
            starts[session_id] = row
        else:
            issues.append(
                _issue(
                    row,
                    "TRAFFIC_SESSION_ID_DUPLICATE",
                    (
                        f"traffic session_id {session_id} is already used by row "
                        f"{first.row_number} for {first.scanner}."
                    ),
                    "Use a session_id that is unique across the entire experiment script.",
                    session_id=session_id,
                    first_row_number=first.row_number,
                    first_scanner=first.scanner,
                )
            )

    stops_seen: Dict[str, int] = {}
    for row in traffic_rows:
        if row.action != "traffic.session.stop":
            continue

        session_id = str((row.args or {}).get("session_id") or "").strip()
        if not session_id:
            continue

        start = starts.get(session_id)
        if start is None:
            issues.append(
                _issue(
                    row,
                    "TRAFFIC_STOP_UNKNOWN_SESSION",
                    f"traffic.session.stop refers to unknown session_id {session_id}.",
                    "Confirm the ID or add an earlier traffic.session.start row.",
                    level="warning",
                    session_id=session_id,
                )
            )
            continue

        if row.scanner != start.scanner:
            issues.append(
                _issue(
                    row,
                    "TRAFFIC_STOP_TARGET_MISMATCH",
                    (
                        f"stop for {session_id} targets {row.scanner}, but its start "
                        f"row targets {start.scanner}."
                    ),
                    f"Set the stop target to {start.scanner}.",
                    session_id=session_id,
                    start_row_number=start.row_number,
                    start_scanner=start.scanner,
                )
            )

        if row.t_offset_sec <= start.t_offset_sec:
            issues.append(
                _issue(
                    row,
                    "TRAFFIC_STOP_NOT_AFTER_START",
                    (
                        f"stop for {session_id} must occur after its start at "
                        f"t_offset_sec={start.t_offset_sec}."
                    ),
                    "Schedule the stop at a strictly later time.",
                    session_id=session_id,
                    start_row_number=start.row_number,
                    start_t_offset_sec=start.t_offset_sec,
                )
            )

        stops_seen[session_id] = stops_seen.get(session_id, 0) + 1
        if stops_seen[session_id] > 1:
            issues.append(
                _issue(
                    row,
                    "TRAFFIC_SESSION_MULTIPLE_STOPS",
                    f"traffic session {session_id} has more than one stop command.",
                    "Usually only one stop command is needed.",
                    level="warning",
                    session_id=session_id,
                )
            )

    return issues
