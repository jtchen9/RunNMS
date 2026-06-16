from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
import utility  # noqa: E402
from m8mobility_state_store import key_state, key_time, key_pose, key_report  # noqa: E402

SCANNER = "twin-scout-charlie"


def parse_json_maybe(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bytes):
        v = v.decode("utf-8", errors="replace")
    if not isinstance(v, str):
        return v
    try:
        return json.loads(v)
    except Exception:
        return v


def hgetall(k: str) -> Dict[str, Any]:
    d = config.r.hgetall(k)
    return d if isinstance(d, dict) else {}


def newest_cmds(limit: int = 10) -> List[Dict[str, Any]]:
    rows = config.r.xrevrange(config.key_cmd_stream(SCANNER), "+", "-", count=limit)
    out = []
    for xid, fields in rows:
        item = {"xid": xid, **fields}
        item["args_json"] = parse_json_maybe(item.get("args_json", ""))
        out.append(item)
    return out


def main() -> None:
    st = hgetall(key_state(SCANNER))
    tm = hgetall(key_time(SCANNER))
    pose = hgetall(key_pose(SCANNER))
    report = parse_json_maybe((hgetall(key_report(SCANNER)) or {}).get("last_mobility_report_json", ""))

    summary = {
        "scanner": SCANNER,
        "ts": utility.local_ts(),
        "state": st.get("state", ""),
        "state_detail": st.get("state_detail", ""),
        "robot_safety_state": st.get("robot_safety_state", ""),
        "stop_experiment": st.get("stop_experiment", ""),
        "stop_reason": st.get("stop_reason", ""),
        "need_location_recovery": st.get("need_location_recovery", ""),
        "location_recovery_context": st.get("location_recovery_context", ""),
        "location_recovery_phase": st.get("location_recovery_phase", ""),
        "visibility_turn_count": st.get("visibility_turn_count", ""),
        "last_visibility_turn_angle_deg": st.get("last_visibility_turn_angle_deg", ""),
        "legacy_need_location_retry": st.get("need_location_retry", ""),
        "legacy_retry_count": st.get("retry_count", ""),
        "busy_count": st.get("busy_count", ""),
        "collision_veto_count": st.get("collision_veto_count", ""),
        "exec_fail_count": st.get("exec_fail_count", ""),
        "correction_attempt_count": st.get("correction_attempt_count", ""),
        "outgoing_command_action": st.get("outgoing_command_action", ""),
        "outgoing_command_source": st.get("outgoing_command_source", ""),
        "outgoing_command_args_json": parse_json_maybe(st.get("outgoing_command_args_json", "")),
        "last_planned_command_action": pose.get("last_planned_command_action", ""),
        "last_planned_command_args_json": parse_json_maybe(pose.get("last_planned_command_args_json", "")),
        "cmd_stream_newest": newest_cmds(8),
        "last_report_action": "" if not isinstance(report, dict) else report.get("last_command", ""),
        "last_report_error": "" if not isinstance(report, dict) else report.get("last_error_code", ""),
        "last_report_status": "" if not isinstance(report, dict) else report.get("last_exec_status", ""),
        "last_mobility_report_at": tm.get("last_mobility_report_at", ""),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
