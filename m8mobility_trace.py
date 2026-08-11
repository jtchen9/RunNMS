"""Passive, append-only mobility transaction tracing.

Tracing is deliberately isolated from mobility behavior: callers may use these
helpers from any state, and every filesystem/serialization failure is swallowed.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict
import json
import math
import threading
import uuid


TRACE_VERSION = "mobility_trace_v1"
TRACE_PATH = (
    Path(__file__).resolve().parent
    / "mobilityTrace"
    / "output"
    / "mobility_trace.jsonl"
)
_TRACE_LOCK = threading.Lock()


def new_trace_id(scanner: str) -> str:
    safe_scanner = "".join(
        ch if ch.isalnum() or ch in "._-" else "_"
        for ch in str(scanner or "unknown")
    )
    return f"{safe_scanner}-{uuid.uuid4().hex}"


def compact_pose(pose: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only pose fields needed for command/result comparison."""
    try:
        return {
            "location_ok": bool(pose.get("location_ok")),
            "x_m": pose.get("x_m"),
            "y_m": pose.get("y_m"),
            "heading_deg": pose.get("heading_deg"),
        }
    except Exception as exc:
        return {"trace_summary_error": f"{type(exc).__name__}: {exc}"[:300]}


def preferred_heading_comparison(
    *,
    preferred_heading_deg: Any,
    actual_heading_deg: Any,
) -> Dict[str, Any]:
    """Compare the LUT heading with the S3 heading without imposing policy."""
    try:
        preferred = float(preferred_heading_deg)
        actual = float(actual_heading_deg)
        signed_error = (actual - preferred + 180.0) % 360.0 - 180.0
        return {
            "comparison_ok": True,
            "preferred_heading_deg": preferred,
            "actual_heading_deg": actual,
            "actual_minus_preferred_deg": signed_error,
            "absolute_error_deg": abs(signed_error),
        }
    except Exception as exc:
        return {
            "comparison_ok": False,
            "detail": f"{type(exc).__name__}: {exc}",
        }


def append_trace_event(
    *,
    event: str,
    scanner: str,
    trace_id: str = "",
    data: Dict[str, Any] | None = None,
) -> bool:
    """Append one compact JSONL event; never raise into mobility runtime."""
    try:
        record = {
            "trace_version": TRACE_VERSION,
            "logged_at": datetime.now().strftime("%Y-%m-%d-%H:%M:%S.%f"),
            "event": str(event or ""),
            "scanner": str(scanner or ""),
            "trace_id": str(trace_id or ""),
            "data": data or {},
        }
        encoded = json.dumps(
            record,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        with _TRACE_LOCK:
            TRACE_PATH.parent.mkdir(parents=True, exist_ok=True)
            with TRACE_PATH.open("a", encoding="utf-8") as stream:
                stream.write(encoded + "\n")
        return True
    except Exception:
        return False


def endpoint_comparison(
    *,
    start_pose: Dict[str, Any],
    expected_pose: Dict[str, Any],
    actual_pose: Dict[str, Any],
    commanded_distance_m: float,
) -> Dict[str, Any]:
    """Return along/cross-track and endpoint errors for one issued movement."""
    try:
        sx = float(start_pose["x_m"])
        sy = float(start_pose["y_m"])
        ex = float(expected_pose["x_m"])
        ey = float(expected_pose["y_m"])
        eh = float(expected_pose["heading_deg"])
        ax = float(actual_pose["x_m"])
        ay = float(actual_pose["y_m"])
        ah = float(actual_pose["heading_deg"])

        vx = ex - sx
        vy = ey - sy
        norm = math.hypot(vx, vy)
        if norm <= 1e-12:
            ux, uy = 0.0, 0.0
        else:
            ux, uy = vx / norm, vy / norm

        actual_dx = ax - sx
        actual_dy = ay - sy
        along = actual_dx * ux + actual_dy * uy
        cross = actual_dx * (-uy) + actual_dy * ux
        commanded = float(commanded_distance_m)

        return {
            "expected_to_actual_position_error_m": math.hypot(ax - ex, ay - ey),
            "expected_to_actual_heading_error_deg": (
                (ah - eh + 180.0) % 360.0 - 180.0
            ),
            "actual_displacement_m": math.hypot(actual_dx, actual_dy),
            "along_track_displacement_m": along,
            "cross_track_displacement_m": cross,
            "commanded_distance_m": commanded,
            "commanded_distance_completion_ratio": (
                along / commanded if abs(commanded) > 1e-12 else None
            ),
        }
    except Exception as exc:
        return {
            "comparison_ok": False,
            "detail": f"{type(exc).__name__}: {exc}",
        }
