"""Offline re-entry geometry only. Never supplies a measured robot position."""
from .static_safety_core import apply_mobility_move_pose


def move_target(current, args):
    """Resolve an absolute endpoint even when the approach start is unknown.

    A shutdown invalidates the planned position: the operator may carry the robot.
    A report command has no measured result at authoring time. F/G reminders are
    deliberately not inputs here. A destination restores subsequent XY geometry,
    but does not establish the path taken to that destination.
    """
    if current is not None:
        return apply_mobility_move_pose(current, args)
    args = args or {}
    absolute = (("x_m" in args and "y_m" in args) or
                ("target_x_m" in args and "target_y_m" in args))
    if not absolute:
        return None, [{
            "level": "error", "code": "REENTRY_REQUIRES_ABSOLUTE_TARGET",
            "message": "The first move after shutdown needs an absolute destination; its starting position is unknown offline.",
            "suggestion": "Author a location report followed by mobility.move with an absolute x/y destination.",
        }]
    # Placeholder used ONLY to parse an absolute target, never as a path start.
    return apply_mobility_move_pose(
        {"x_m": 0.0, "y_m": 0.0, "heading_deg": 0.0}, args)


def unknown_approach_issue():
    return {
        "level": "warning", "code": "REENTRY_APPROACH_REQUIRES_RUNTIME_CHECK",
        "message": "Re-entry destination checked; approach distance/path cannot be checked offline because placement after shutdown is unknown. Positions of other robots after shutdown are also unknown until their next planned destination.",
        "suggestion": "Place the robot with a clear approach and power it on early. The authored location report and existing runtime checks must check the actual move.",
    }
