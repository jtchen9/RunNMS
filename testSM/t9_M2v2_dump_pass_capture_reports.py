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
    if isinstance(x, (dict, list, int, float, bool)):
        return x
    s = str(x)
    try:
        return json.loads(s)
    except Exception:
        return s


def maybe_json_field(k: str, v: Any) -> Any:
    s = str(v)
    if k.endswith("_json") or k in ("rule", "rule_json", "original_report", "final_report", "original_report_json", "final_report_json"):
        return parse_json_maybe(s)
    if s.startswith("{") or s.startswith("["):
        return parse_json_maybe(s)
    return v


def hgetall(k: str) -> Dict[str, Any]:
    d = config.r.hgetall(k)
    return dict(d) if isinstance(d, dict) else {}


def get_event_stream() -> str:
    return getattr(
        config,
        "MOBILITY_REPORT_INTERCEPT_EVENT_STREAM",
        f"{config.KEY_PREFIX}debug:mobility_report_intercept:events",
    )


def read_raw_events(count: int = 500) -> List[Dict[str, Any]]:
    key = get_event_stream()
    try:
        rows = config.r.xrevrange(key, count=count)
    except Exception:
        rows = []
    out: List[Dict[str, Any]] = []
    for xid, fields in reversed(rows):
        item: Dict[str, Any] = {"xid": xid}
        for k, v in dict(fields).items():
            item[k] = maybe_json_field(k, v)
        out.append(item)
    return out


def pick_report(e: Dict[str, Any]) -> Any:
    # Robust against different production field names.
    for key in ("final_report_json", "final_report", "report_json", "report", "original_report_json", "original_report"):
        if isinstance(e.get(key), dict):
            return e.get(key)
    return None


def compact_report(report: Any) -> Dict[str, Any]:
    if not isinstance(report, dict):
        return {}

    loc = report.get("last_location_result") or {}
    apriltag = loc.get("apriltag") if isinstance(loc, dict) and isinstance(loc.get("apriltag"), dict) else {}

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
        "unique_tag_ids": None,
        "x_m": None,
        "y_m": None,
        "heading_deg": None,
    }

    if isinstance(apriltag, dict):
        compact["tag_count"] = apriltag.get("count") or apriltag.get("unique_count")
        compact["unique_tag_ids"] = apriltag.get("unique_tag_ids")

    sources = [loc]
    if isinstance(loc, dict):
        for key in ("pose", "location", "true_location", "result"):
            if isinstance(loc.get(key), dict):
                sources.append(loc.get(key))

    for src in sources:
        if isinstance(src, dict):
            if compact["tag_count"] is None:
                compact["tag_count"] = src.get("tag_count") or src.get("count") or src.get("unique_count")
            if compact["unique_tag_ids"] is None:
                compact["unique_tag_ids"] = src.get("unique_tag_ids")
            for f in ("x_m", "y_m", "heading_deg"):
                if compact[f] is None and src.get(f) is not None:
                    compact[f] = src.get(f)

    return compact


def main() -> None:
    raw_events = read_raw_events(500)

    compact_events = []
    for e in raw_events:
        report = pick_report(e)
        compact_events.append(
            {
                "xid": e.get("xid"),
                "scanner": e.get("scanner") or e.get("scanner_name") or e.get("robot") or e.get("device"),
                "phase": e.get("phase"),
                "detail": e.get("detail"),
                "mode": e.get("mode"),
                "action": e.get("action"),
                "raw_keys": sorted([str(k) for k in e.keys()]),
                "report": compact_report(report),
            }
        )

    state = hgetall(key_state(SCANNER))
    pose = hgetall(key_pose(SCANNER))
    report = hgetall(key_report(SCANNER))
    time = hgetall(key_time(SCANNER))

    evidence = {
        "test_id": "M2v2",
        "test_name": "csv_script_run_pass_capture_v2",
        "scanner": SCANNER,
        "dumped_at": utility.local_ts(),
        "event_stream": get_event_stream(),
        "raw_event_count": len(raw_events),
        "raw_events": raw_events,
        "compact_event_count": len(compact_events),
        "compact_events": compact_events,
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
    out = OUT_DIR / f"M2v2_csv_script_run_pass_capture_{SCANNER}_{ts}.json"
    out.write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")

    print("M2v2 report capture dumped")
    print("scanner =", SCANNER)
    print("raw_event_count =", len(raw_events))
    print("compact_event_count =", len(compact_events))
    print("final_state =", evidence["final_state"])
    print("evidence =", out)
    print()
    print("COMPACT EVENTS:")
    for i, e in enumerate(compact_events, 1):
        c = e["report"]
        print(f"{i:02d}. scanner={e.get('scanner')} phase={e.get('phase')} detail={e.get('detail')}")
        print(f"    cmd={c.get('last_command')} status={c.get('last_exec_status')} err={c.get('last_error_code')}")
        print(f"    args={c.get('last_command_args')}")
        print(f"    loc_ok={c.get('location_ok')} tag_count={c.get('tag_count')} x={c.get('x_m')} y={c.get('y_m')} heading={c.get('heading_deg')}")
        print(f"    raw_keys={e.get('raw_keys')}")


if __name__ == "__main__":
    main()
