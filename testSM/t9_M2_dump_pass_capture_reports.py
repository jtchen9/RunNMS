from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(str(PROJECT_ROOT))

import config
import utility

from m8mobility_state_store import key_state, key_pose, key_report, key_time


SCANNER = os.environ.get("SCANNER", "twin-scout-charlie")
OUT_DIR = THIS_FILE.parent / "testSM"


def parse_json_maybe(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return x
    try:
        return json.loads(str(x))
    except Exception:
        return x


def hgetall(k: str) -> Dict[str, Any]:
    d = config.r.hgetall(k)
    return dict(d) if isinstance(d, dict) else {}


def get_event_stream() -> str:
    return getattr(
        config,
        "MOBILITY_REPORT_INTERCEPT_EVENT_STREAM",
        f"{config.KEY_PREFIX}debug:mobility_report_intercept:events",
    )


def read_events(count: int = 300) -> List[Dict[str, Any]]:
    key = get_event_stream()
    try:
        rows = config.r.xrevrange(key, count=count)
    except Exception:
        rows = []
    out: List[Dict[str, Any]] = []
    for xid, fields in reversed(rows):
        item: Dict[str, Any] = {"xid": xid}
        for k, v in dict(fields).items():
            if k in ("original_report_json", "final_report_json", "rule_json"):
                item[k] = parse_json_maybe(v)
            else:
                item[k] = v
        out.append(item)
    return out


def compact_report(report: Any) -> Dict[str, Any]:
    if not isinstance(report, dict):
        return {}
    loc = report.get("last_location_result") or {}

    compact = {
        "last_command": report.get("last_command"),
        "last_command_args": report.get("last_command_args"),
        "last_exec_status": report.get("last_exec_status"),
        "last_error_code": report.get("last_error_code"),
        "last_error_detail": report.get("last_error_detail"),
        "last_command_received_ts": report.get("last_command_received_ts"),
        "last_command_finished_ts": report.get("last_command_finished_ts"),
        "location_ok": loc.get("ok") if isinstance(loc, dict) else None,
        "capture_mode": loc.get("capture_mode") if isinstance(loc, dict) else None,
        "tag_count": None,
        "x_m": None,
        "y_m": None,
        "heading_deg": None,
    }

    sources = [loc]
    if isinstance(loc, dict):
        for key in ("pose", "location", "true_location", "result"):
            if isinstance(loc.get(key), dict):
                sources.append(loc.get(key))

    for src in sources:
        if isinstance(src, dict):
            if compact["tag_count"] is None:
                compact["tag_count"] = (
                    src.get("tag_count")
                    or src.get("count")
                    or src.get("unique_count")
                )
            for f in ("x_m", "y_m", "heading_deg"):
                if compact[f] is None and src.get(f) is not None:
                    compact[f] = src.get(f)

    return compact


def main() -> None:
    events = read_events(300)
    reports = []
    for e in events:
        if str(e.get("scanner", "")) != SCANNER:
            continue
        original = e.get("original_report_json")
        final = e.get("final_report_json")
        reports.append(
            {
                "xid": e.get("xid"),
                "ts": e.get("ts") or e.get("server_ts") or e.get("created_at"),
                "phase": e.get("phase"),
                "detail": e.get("detail"),
                "original": compact_report(original),
                "final": compact_report(final),
            }
        )

    state = hgetall(key_state(SCANNER))
    pose = hgetall(key_pose(SCANNER))
    report = hgetall(key_report(SCANNER))
    time = hgetall(key_time(SCANNER))

    evidence = {
        "test_id": "M2",
        "test_name": "csv_script_run_pass_capture",
        "scanner": SCANNER,
        "dumped_at": utility.local_ts(),
        "event_stream": get_event_stream(),
        "report_count": len(reports),
        "reports": reports,
        "final_state": {
            "state": state.get("state", ""),
            "state_detail": state.get("state_detail", ""),
            "robot_safety_state": state.get("robot_safety_state", ""),
            "stop_experiment": state.get("stop_experiment", ""),
            "stop_reason": state.get("stop_reason", ""),
            "correction_attempt_count": state.get("correction_attempt_count", ""),
            "retry_count": state.get("retry_count", ""),
            "busy_count": state.get("busy_count", ""),
            "collision_veto_count": state.get("collision_veto_count", ""),
            "exec_fail_count": state.get("exec_fail_count", ""),
            "outgoing_command_action": state.get("outgoing_command_action", ""),
            "outgoing_command_args_json": state.get("outgoing_command_args_json", ""),
        },
        "final_time": time,
        "final_pose": {
            "true_location_json": parse_json_maybe(pose.get("true_location_json", "")),
            "planned_location_json": parse_json_maybe(pose.get("planned_location_json", "")),
            "last_planned_command_action": pose.get("last_planned_command_action", ""),
            "last_planned_command_args_json": pose.get("last_planned_command_args_json", ""),
        },
        "last_report": parse_json_maybe(report.get("last_mobility_report_json", "")),
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = evidence["dumped_at"].replace("-", "").replace(":", "")
    out = OUT_DIR / f"M2_csv_script_run_pass_capture_{SCANNER}_{ts}.json"
    out.write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")

    print("M2 report capture dumped")
    print("scanner =", SCANNER)
    print("report_count =", len(reports))
    print("final_state =", evidence["final_state"])
    print("evidence =", out)
    print()
    print("COMPACT REPORTS:")
    for i, r in enumerate(reports, 1):
        c = r["final"] or r["original"]
        print(f"{i:02d}. phase={r.get('phase')} cmd={c.get('last_command')} status={c.get('last_exec_status')} err={c.get('last_error_code')}")
        print(f"    args={c.get('last_command_args')}")
        print(f"    loc_ok={c.get('location_ok')} tag_count={c.get('tag_count')} x={c.get('x_m')} y={c.get('y_m')} heading={c.get('heading_deg')}")


if __name__ == "__main__":
    main()
