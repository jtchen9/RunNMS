"""Pure checks for the Phase A recovery only; no robot, Redis, or I/O."""
import math

ACTION = "mobility.turn_move_turn.forward"
MAX_STEP = 0.30
MAX_EXCESS = 0.001
ARRIVAL = 0.15


def number(x):
    return type(x) in (float, int) and math.isfinite(x)


def measured(p):
    return (p.get("location_ok") is True and p.get("source") in ("apriltag", "apriltag_componentwise")
            and (p.get("confidence") or {}).get("level") == "HIGH"
            and all(number(p.get(k)) for k in ("x_m", "y_m", "heading_deg")))


def endpoint(start, planned, limit):
    if not measured(start) or not number(limit) or not 0 < limit <= MAX_STEP:
        raise ValueError("invalid landing pose or first-correction limit")
    dx, dy = planned["x_m"] - start["x_m"], planned["y_m"] - start["y_m"]
    d = math.hypot(dx, dy)
    if not limit < d <= limit + MAX_EXCESS:
        raise ValueError("outside the near-miss band")
    return (start["x_m"] + limit*dx/d, start["y_m"] + limit*dy/d)


def check_report(report, cmd, crossing_finish):
    received, finished = (report.get("last_command_received_ts"), report.get("last_command_finished_ts"))
    return (report.get("last_exec_status") == "completed" and not report.get("last_error_code")
            and report.get("last_command") == ACTION and report.get("last_command_args") == cmd
            and number(received) and number(finished) and number(crossing_finish)
            and 0 < crossing_finish < received <= finished)
