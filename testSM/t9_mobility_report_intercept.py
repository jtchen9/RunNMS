"""
1. Is MOBILITY_REPORT_INTERCEPT_AVAILABLE True?
      no  → normal path
      yes → continue

2. Is Redis enable key true for this scanner?
      no  → normal path
      yes → continue

3. Is there a Redis rule key for this scanner?
      no  → normal path
      yes → continue

4. If rule has match_action, does report.last_command match?
      no  → normal path, with not_matched event
      yes → apply rule

5. Apply mode:
      drop    → do not store report, do not call on_report_received()
      patch   → modify selected fields, then normal processing
      replace → replace whole report, then normal processing
      pass    → no mutation, but record that outlet was active

====================
testSM helper for the NMS mobility-report interception outlet.

The production outlet lives in:

    m4Commands.cmd_poll()

The outlet is located before:

    key_report(scanner).last_mobility_report_json
    key_time(scanner).last_mobility_report_at
    m8mobility.on_report_received(scanner)

Therefore it can simulate:
    - wrong report action
    - dropped report / S1 timeout
    - robot busy
    - collision veto
    - location solve failure

Redis keys
----------
For scanner twin-scout-charlie:

1. Runtime enable key:

    nms:debug:mobility_report_intercept:enabled:twin-scout-charlie

2. One-shot rule key:

    nms:debug:mobility_report_intercept:twin-scout-charlie

3. Event stream:

    nms:debug:mobility_report_intercept:events

Spelling note:
    The word is "mobility", not "mobiltiy".

Rule fields
-----------
    mode:
        "drop", "patch", "replace", or "pass"

    match_action:
        Optional. Apply only if incoming report.last_command matches.

    once:
        Usually true. Delete rule after first matched report.

    patch:
        For mode="patch", deep-merge these fields into the real report.

    replacement:
        For mode="replace", replace whole report with this dict.

Typical usage
-------------
    python .\\testSM\\t9_set_A1_wrong_action_report.py
    python .\\testSM\\t3_c04_forward_020m_pre0_post0_real_robot.py
    python .\\testSM\\t9_show_intercept.py

No config.py edit is needed per test.
No NMS restart is needed per test.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


NMS_ROOT = Path(__file__).resolve().parents[1]
if str(NMS_ROOT) not in sys.path:
    sys.path.insert(0, str(NMS_ROOT))

import config  # noqa: E402


DEFAULT_SCANNER = "twin-scout-charlie"
DEFAULT_FORWARD_ACTION = "mobility.turn_move_turn.forward"

INTERCEPT_PREFIX = getattr(
    config,
    "MOBILITY_REPORT_INTERCEPT_KEY_PREFIX",
    f"{config.KEY_PREFIX}debug:mobility_report_intercept:",
)

ENABLE_KEY_BASE = getattr(
    config,
    "MOBILITY_REPORT_INTERCEPT_ENABLE_KEY",
    f"{config.KEY_PREFIX}debug:mobility_report_intercept:enabled",
)

EVENT_STREAM = getattr(
    config,
    "MOBILITY_REPORT_INTERCEPT_EVENT_STREAM",
    f"{config.KEY_PREFIX}debug:mobility_report_intercept:events",
)


def intercept_key(scanner: str) -> str:
    return f"{INTERCEPT_PREFIX}{scanner}"


def enable_key(scanner: Optional[str] = None) -> str:
    return ENABLE_KEY_BASE if not scanner else f"{ENABLE_KEY_BASE}:{scanner}"


def enable_intercept(scanner: str = DEFAULT_SCANNER) -> str:
    key = enable_key(scanner)
    config.r.set(key, "true")
    return key


def disable_intercept(scanner: str = DEFAULT_SCANNER) -> int:
    return int(config.r.delete(enable_key(scanner)) or 0)


def set_intercept_rule(scanner: str, rule: Dict[str, Any]) -> str:
    key = intercept_key(scanner)
    config.r.set(key, json.dumps(rule, ensure_ascii=False))
    return key


def clear_intercept_events() -> int:
    """
    Clear the debug event stream so each test shows only its own intercept events.
    """
    return int(config.r.delete(EVENT_STREAM) or 0)


def clear_intercept(scanner: str = DEFAULT_SCANNER, clear_events: bool = True) -> int:
    n = 0
    n += int(config.r.delete(intercept_key(scanner)) or 0)
    n += int(config.r.delete(enable_key(scanner)) or 0)

    if clear_events:
        n += clear_intercept_events()

    return n


def get_intercept_rule(scanner: str = DEFAULT_SCANNER) -> Optional[Dict[str, Any]]:
    raw = config.r.get(intercept_key(scanner))
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    return json.loads(raw)


def get_enable_value(scanner: str = DEFAULT_SCANNER) -> Any:
    return config.r.get(enable_key(scanner))


def read_recent_intercept_events(count: int = 20) -> List[Dict[str, Any]]:
    rows = config.r.xrevrange(EVENT_STREAM, "+", "-", count=count)
    out: List[Dict[str, Any]] = []

    for _xid, fields in rows:
        raw = fields.get("json") if isinstance(fields, dict) else None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if raw:
            try:
                out.append(json.loads(raw))
            except Exception:
                out.append({"raw": raw})

    return out


def arm_rule(scanner: str, rule: Dict[str, Any]) -> Dict[str, str]:
    clear_intercept(scanner)
    ekey = enable_intercept(scanner)
    rkey = set_intercept_rule(scanner, rule)
    return {
        "scanner": scanner,
        "enable_key": ekey,
        "rule_key": rkey,
    }


def rule_wrong_action_report(match_action: str = DEFAULT_FORWARD_ACTION) -> Dict[str, Any]:
    return {
        "mode": "replace",
        "match_action": match_action,
        "once": True,
        "replacement": {
            "last_command": "mobility.report.location",
            "last_command_args": {},
            "last_command_received_ts": 1770000000.0,
            "last_command_finished_ts": 1770000001.0,
            "last_exec_status": "completed",
            "last_error_code": "",
            "last_error_detail": "debug injected wrong-action report",
            "last_location_result": {
                "ok": True,
                "debug_injected": True,
            },
        },
    }


def rule_drop_report(match_action: str = DEFAULT_FORWARD_ACTION) -> Dict[str, Any]:
    return {
        "mode": "drop",
        "match_action": match_action,
        "once": True,
    }


def rule_robot_busy(match_action: str = DEFAULT_FORWARD_ACTION) -> Dict[str, Any]:
    return {
        "mode": "patch",
        "match_action": match_action,
        "once": True,
        "patch": {
            "last_exec_status": "failed",
            "last_error_code": "MOBILITY_BUSY",
            "last_error_detail": "debug injected robot busy",
        },
    }


def rule_collision_veto(match_action: str = DEFAULT_FORWARD_ACTION) -> Dict[str, Any]:
    return {
        "mode": "patch",
        "match_action": match_action,
        "once": True,
        "patch": {
            "last_exec_status": "failed",
            "last_error_code": "COLLISION_VETO",
            "last_error_detail": "debug injected local collision prevention veto",
            "last_location_result": {
                "ok": False,
                "error": "debug injected collision prevention veto",
            },
        },
    }


def rule_location_failed(match_action: str = "mobility.report.location") -> Dict[str, Any]:
    return {
        "mode": "patch",
        "match_action": match_action,
        "once": True,
        "patch": {
            "last_exec_status": "failed",
            "last_error_code": "LOCATION_SOLVE_FAILED",
            "last_error_detail": "debug injected location solve failure",
            "last_location_result": {
                "ok": False,
                "error": "debug injected no usable AprilTag pose",
                "apriltag": {
                    "ok": False,
                    "count": 0,
                    "tags": [],
                },
            },
        },
    }
