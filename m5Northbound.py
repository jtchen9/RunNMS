from typing import Dict, Any, List, Optional, Tuple
import json
import asyncio
import requests
from fastapi import APIRouter
from datetime import timedelta

import config
import utility
from m8mobility_state_store import key_pose
import urllib3
if not getattr(config, "WEB_VERIFY_TLS", True):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
router = APIRouter()

# ==================
# 5) Northbound (NMS -> Web Server)
# ==================
def _web_headers() -> Dict[str, str]:
    h: Dict[str, str] = {}
    if config.WEB_API_KEY:
        h["X-API-Key"] = config.WEB_API_KEY
    return h


def _collect_iperf3_sessions_for_upload(
    budget: int,
    limit: int = 200,
) -> Tuple[List[Dict[str, Any]], List[str], int]:
    rows = config.r.xrange(config.KEY_TRAFFIC_RESULT_STREAM, count=limit)

    out: List[Dict[str, Any]] = []
    ids: List[str] = []
    bad_json_deleted = 0

    for xid, fields in rows:
        raw_json = fields.get("raw_json", "") or "{}"

        reverse_raw = fields.get("reverse", "")
        reverse = None
        if str(reverse_raw).strip() != "":
            reverse = str(reverse_raw).strip() in ("1", "true", "True")

        try:
            raw_obj = json.loads(raw_json)
        except Exception:
            try:
                config.r.xdel(config.KEY_TRAFFIC_RESULT_STREAM, xid)
            except Exception:
                pass
            bad_json_deleted += 1
            continue

        item = {
            "scanner": fields.get("scanner", ""),
            "session_id": fields.get("session_id", ""),
            "completion_time": fields.get("completion_time", ""),
            "time_format": config.TIME_FMT,
            "status": fields.get("status", ""),
            "detail": fields.get("detail", ""),
            "raw": raw_obj,
        }

        if reverse is not None:
            item["reverse"] = reverse

        item_bytes = utility._json_bytes(item)
        if item_bytes > budget:
            break

        out.append(item)
        ids.append(xid)
        budget -= item_bytes

        if budget <= 0:
            break

    return out, ids, bad_json_deleted


def _collect_ap_traffic_reports_for_upload(
    scanners: List[str],
    budget: int,
    limit_per_ap: int = 200,
) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]], int]:
    out: List[Dict[str, Any]] = []
    ids_by_ap: Dict[str, List[str]] = {}
    bad_json_deleted = 0

    for scanner in scanners:
        stream_key = config.key_ap_uplink_stream(scanner)
        rows = config.r.xrange(stream_key, count=limit_per_ap)

        for xid, fields in rows:
            payload_text = fields.get("payload_text", "") or ""

            try:
                raw_obj = json.loads(payload_text)
            except Exception:
                try:
                    config.r.xdel(stream_key, xid)
                except Exception:
                    pass
                bad_json_deleted += 1
                continue

            meta = config.r.hgetall(config.key_scanner_meta(scanner)) or {}
            ap_alias = (meta.get("ap_alias") or "").strip()
            ap_id = ap_alias or scanner

            item = {
                "ap_id": ap_id,
                "ap_real_id": scanner,
                "scanner": scanner,   # keep for backward compatibility/debug
                "time_format": config.TIME_FMT,
                "raw": raw_obj,
            }

            item_bytes = utility._json_bytes(item)
            if item_bytes > budget:
                return out, ids_by_ap, bad_json_deleted

            out.append(item)
            ids_by_ap.setdefault(scanner, []).append(xid)
            budget -= item_bytes

            if budget <= 0:
                return out, ids_by_ap, bad_json_deleted

    return out, ids_by_ap, bad_json_deleted


def _post_upload_scan_batch(
    items: List[Dict[str, Any]],
    iperf3_sessions: Optional[List[Dict[str, Any]]] = None,
    ap_traffic_reports: Optional[List[Dict[str, Any]]] = None,
    experiment: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str, int, int]:
    """
    Returns (ok, detail, accepted, rejected).
    We treat HTTP 2xx + JSON status=ok as ok.
    """
    payload = {
        "schema_version": 2,
        "report_type": "scan_batch",
        "nms_id": config.NMS_ID,
        "lab_id": _northbound_lab_id(),
        "time": utility.local_ts(),
        "time_format": config.TIME_FMT,
        "experiment": experiment or _build_experiment_snapshot(),
        "items": items,
        "iperf3_sessions": iperf3_sessions or [],
        "ap_traffic_reports": ap_traffic_reports or [],
    }

    try:
        config.r.set(config.KEY_NB_LAST_UPLOAD_PAYLOAD, json.dumps(payload, ensure_ascii=False))
        config.r.expire(config.KEY_NB_LAST_UPLOAD_PAYLOAD, config.NB_DEBUG_TTL_SEC)
    except Exception:
        pass

    try:
        resp = requests.post(
            config.WEB_NMS_UPLOAD_URL,
            json=payload,
            headers=_web_headers(),
            timeout=10,
            verify=getattr(config, "WEB_VERIFY_TLS", True),
        )
        resp.raise_for_status()
        j = resp.json()
        if isinstance(j, dict) and j.get("status") == "ok":
            return True, "ok", int(j.get("accepted") or 0), int(j.get("rejected") or 0)
        return False, f"web returned {j}", 0, 0
    except Exception as e:
        return False, f"post failed: {e}", 0, 0

    

def _northbound_upload_once() -> Dict[str, Any]:
    """
    One cycle: build one 1-minute northbound payload from passive producers.

    Ownership:
    - robot scan producers write to nms:uplink:{scanner}
    - iperf3 producer writes to KEY_TRAFFIC_RESULT_STREAM
    - AP producer may later write to its own holding area

    This function is the sole owner that:
    1) reads available data
    2) assembles the 1-minute payload
    3) POSTs /nms/upload_scan_batch
    4) deletes only what was successfully sent
    """
    robots = sorted(list(config.r.smembers(config.KEY_REGISTRY)))
    selected_items: List[Dict[str, Any]] = []
    selected_ids_by_robot: Dict[str, List[str]] = {}

    selected_iperf3_sessions: List[Dict[str, Any]] = []
    selected_iperf3_ids: List[str] = []

    ap_traffic_reports: List[Dict[str, Any]] = []
    selected_ap_ids_by_scanner: Dict[str, List[str]] = {}
    ap_bad_json_deleted = 0

    bad_json_deleted = 0
    oversize_deleted = 0
    iperf3_bad_json_deleted = 0

    experiment_snapshot = _build_experiment_snapshot()

    envelope_base = {
        "schema_version": 2,
        "report_type": "scan_batch",
        "nms_id": config.NMS_ID,
        "lab_id": _northbound_lab_id(),
        "time": utility.local_ts(),
        "time_format": config.TIME_FMT,
        "experiment": experiment_snapshot,
        "items": [],
        "iperf3_sessions": [],
        "ap_traffic_reports": [],
    }
    base_bytes = utility._json_bytes(envelope_base)
    budget = max(0, int(config.UPLOAD_BATCH_MAX_BYTES - base_bytes))

    # -------------------------
    # 1) Collect robot scan items
    # -------------------------
    for robot in robots:
        stream_key = config.key_uplink_stream(robot)
        if budget <= 0:
            break

        entries = config.r.xrange(stream_key, count=5000)
        for xid, fields in entries:
            if budget <= 0:
                break

            payload_text = fields.get("payload_text", "")
            received_at = fields.get("received_at", "") or utility.local_ts()

            lst = utility._safe_parse_entries_list(payload_text)
            if lst is None:
                try:
                    config.r.xdel(stream_key, xid)
                except Exception:
                    pass
                bad_json_deleted += 1
                continue

            item = {
                "scanner": robot,
                "time": received_at,
                "iface": "",
                "entries": lst,
                "time_format": config.TIME_FMT,
            }

            item_bytes = utility._json_bytes(item)
            if item_bytes > config.UPLOAD_BATCH_MAX_BYTES:
                try:
                    config.r.xdel(stream_key, xid)
                except Exception:
                    pass
                oversize_deleted += 1
                continue

            if item_bytes > budget:
                break

            selected_items.append(item)
            selected_ids_by_robot.setdefault(robot, []).append(xid)
            budget -= item_bytes

    # -------------------------
    # 2) Collect completed iperf3 session results
    # -------------------------
    if budget > 0:
        try:
            selected_iperf3_sessions, selected_iperf3_ids, iperf3_bad_json_deleted = _collect_iperf3_sessions_for_upload(
                budget=budget,
                limit=200,
            )
        except Exception:
            selected_iperf3_sessions, selected_iperf3_ids, iperf3_bad_json_deleted = [], [], 0

    # -------------------------
    # 3) Collect AP 1-minute traffic reports
    # -------------------------
    if budget > 0:
        try:
            ap_traffic_reports, selected_ap_ids_by_scanner, ap_bad_json_deleted = _collect_ap_traffic_reports_for_upload(
                scanners=robots,
                budget=budget,
                limit_per_ap=200,
            )
        except Exception:
            ap_traffic_reports, selected_ap_ids_by_scanner, ap_bad_json_deleted = [], {}, 0

    # -------------------------
    # 4) POST assembled 1-minute payload
    # -------------------------
    ok, detail, accepted, rejected = _post_upload_scan_batch(
        selected_items,
        iperf3_sessions=selected_iperf3_sessions,
        ap_traffic_reports=ap_traffic_reports,
        experiment=experiment_snapshot,
    )

    # -------------------------
    # 5) Delete only what was successfully sent
    # -------------------------
    if ok:
        deleted_total = 0

        for robot, ids in selected_ids_by_robot.items():
            if not ids:
                continue
            try:
                deleted_total += int(config.r.xdel(config.key_uplink_stream(robot), *ids))
                config.r.hset(config.KEY_NB_LAST_UPLOAD, robot, utility.local_ts())
                config.r.hset(config.KEY_NB_LAST_RESULT, robot, f"ok sent={len(ids)}")
            except Exception:
                pass

        if selected_iperf3_ids:
            try:
                deleted_total += int(config.r.xdel(config.KEY_TRAFFIC_RESULT_STREAM, *selected_iperf3_ids))
            except Exception:
                pass

        for scanner, ids in selected_ap_ids_by_scanner.items():
            if not ids:
                continue
            try:
                deleted_total += int(config.r.xdel(config.key_ap_uplink_stream(scanner), *ids))
            except Exception:
                pass

        return {
            "status": "ok",
            "sent_items": len(selected_items),
            "sent_iperf3_sessions": len(selected_iperf3_sessions),
            "sent_ap_traffic_reports": len(ap_traffic_reports),
            "deleted": deleted_total,
            "bad_json_deleted": bad_json_deleted,
            "iperf3_bad_json_deleted": iperf3_bad_json_deleted,
            "ap_bad_json_deleted": ap_bad_json_deleted,
            "oversize_deleted": oversize_deleted,
            "web_detail": detail,
            "accepted": accepted,
            "rejected": rejected,
            "time": utility.local_ts(),
        }

    return {
        "status": "fail",
        "sent_items": len(selected_items),
        "sent_iperf3_sessions": len(selected_iperf3_sessions),
        "sent_ap_traffic_reports": len(ap_traffic_reports),
        "bad_json_deleted": bad_json_deleted,
        "iperf3_bad_json_deleted": iperf3_bad_json_deleted,
        "ap_bad_json_deleted": ap_bad_json_deleted,
        "oversize_deleted": oversize_deleted,

        "error": detail,
        "time": utility.local_ts(),
    }


def _collect_traffic_events(limit: int = 200) -> Tuple[List[Dict[str, Any]], List[str]]:
    rows = config.r.xrange(config.KEY_TRAFFIC_EVENT_STREAM, count=limit)

    out: List[Dict[str, Any]] = []
    ids: List[str] = []

    for xid, fields in rows:
        duration_raw = fields.get("duration_sec", "")
        try:
            duration_sec = int(duration_raw) if str(duration_raw).strip() != "" else None
        except Exception:
            duration_sec = None

        reverse_raw = fields.get("reverse", "")
        reverse = None
        if str(reverse_raw).strip() != "":
            reverse = str(reverse_raw).strip() in ("1", "true", "True")        

        item = {
            "scanner": fields.get("scanner", ""),
            "session_id": fields.get("session_id", ""),
            "action": fields.get("action", ""),
            "status": fields.get("status", ""),
            "completion_time": fields.get("completion_time", ""),
            "detail": fields.get("detail", ""),
        }

        if duration_sec is not None:
            item["duration_sec"] = duration_sec

        if reverse is not None:
            item["reverse"] = reverse
            
        out.append(item)
        ids.append(xid)

    return out, ids


def _northbound_lab_id() -> str:
    return str(getattr(config, "NMS_NAME", config.NMS_ID) or config.NMS_ID)


def _int_or_default(v: Any, default: int = 0) -> int:
    try:
        s = str(v or "").strip()
        return int(s) if s != "" else int(default)
    except Exception:
        return int(default)


def _bool_from_redis(v: Any) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "yes", "on"}


def _empty_experiment_snapshot(
    *,
    state: str = "idle",
    next_scheduled_at: Optional[str] = None,
    idle_duration_sec: int = 0,
) -> Dict[str, Any]:
    return {
        "state": state,
        "experiment_id": None,
        "session_id": None,
        "lab_id": _northbound_lab_id(),
        "registered_at": None,
        "start_at": None,
        "end_at": None,
        "command_count": 0,
        "replace_existing": False,
        "elapsed_sec": 0,
        "remaining_sec": 0,
        "next_scheduled_at": next_scheduled_at,
        "idle_duration_sec": max(0, int(idle_duration_sec)),
    }


def _experiment_record_from_stream_row(stream_id: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    start_at_s = str(fields.get("start_at") or "").strip()
    end_at_s = str(fields.get("end_at") or "").strip()

    # Backward compatibility for old debug records only. New CSV registration
    # writes start_at/end_at.
    if not start_at_s:
        start_at_s = str(fields.get("t0") or "").strip()
    if not end_at_s:
        end_at_s = str(fields.get("last_execute_at") or "").strip()

    if not start_at_s or not end_at_s:
        return None

    try:
        start_dt = utility.parse_local_dt(start_at_s)
        end_dt = utility.parse_local_dt(end_at_s)
    except Exception:
        return None

    guard_sec = _int_or_default(fields.get("guard_sec"), 3600)
    wrapup_sec = _int_or_default(
        getattr(config, "NORTHBOUND_EXPERIMENT_WRAPUP_SEC", guard_sec),
        guard_sec,
    )
    wrapup_until_dt = end_dt + timedelta(seconds=wrapup_sec)

    experiment_id = str(fields.get("experiment_id") or fields.get("scenario_name") or "").strip() or None
    session_id = str(fields.get("session_id") or "").strip() or None
    lab_id = str(fields.get("lab_id") or _northbound_lab_id()).strip() or _northbound_lab_id()

    return {
        "state": str(fields.get("state") or "registered").strip() or "registered",
        "experiment_id": experiment_id,
        "session_id": session_id,
        "lab_id": lab_id,
        "registered_at": str(fields.get("registered_at") or "").strip() or None,
        "start_at": start_at_s,
        "end_at": end_at_s,
        # Internal only. Do not expose scheduling guard/wrap-up durations in
        # northbound payloads; NMS resolves attribution before sending.
        "_guard_sec": guard_sec,
        "_wrapup_sec": wrapup_sec,
        "_wrapup_until_s": wrapup_until_dt.strftime(config.TIME_FMT),
        "command_count": _int_or_default(fields.get("command_count") or fields.get("item_count"), 0),
        "replace_existing": _bool_from_redis(fields.get("replace_existing")),
        "_start_dt": start_dt,
        "_end_dt": end_dt,
        "_wrapup_until_dt": wrapup_until_dt,
    }


def _public_experiment_snapshot(
    record: Dict[str, Any],
    *,
    state: str,
    now_dt,
    next_scheduled_at: Optional[str] = None,
    idle_duration_sec: int = 0,
) -> Dict[str, Any]:
    start_dt = record["_start_dt"]
    end_dt = record["_end_dt"]
    wrapup_until_dt = record["_wrapup_until_dt"]

    if state == "scheduled":
        elapsed_sec = 0
        remaining_sec = max(0, int((start_dt - now_dt).total_seconds()))
    elif state == "running":
        elapsed_sec = max(0, int((now_dt - start_dt).total_seconds()))
        remaining_sec = max(0, int((end_dt - now_dt).total_seconds()))
    elif state == "wrapping_up":
        elapsed_sec = max(0, int((now_dt - start_dt).total_seconds()))
        remaining_sec = max(0, int((wrapup_until_dt - now_dt).total_seconds()))
    else:
        elapsed_sec = 0
        remaining_sec = 0

    return {
        "state": state,
        "experiment_id": record.get("experiment_id"),
        "session_id": record.get("session_id"),
        "lab_id": record.get("lab_id") or _northbound_lab_id(),
        "registered_at": record.get("registered_at"),
        "start_at": record.get("start_at"),
        "end_at": record.get("end_at"),
        "command_count": int(record.get("command_count") or 0),
        "replace_existing": bool(record.get("replace_existing")),
        "elapsed_sec": elapsed_sec,
        "remaining_sec": remaining_sec,
        "next_scheduled_at": next_scheduled_at,
        "idle_duration_sec": max(0, int(idle_duration_sec)),
    }


def _build_experiment_snapshot() -> Dict[str, Any]:
    """
    Build the experiment section used by both northbound reports.

    State rules:
    - running:     start_at <= now <= end_at
    - wrapping_up: end_at < now <= end_at + NORTHBOUND_EXPERIMENT_WRAPUP_SEC
                  NMS uses this internal window to keep reports associated with
                  the previous experiment long enough to flush late data. The
                  timing policy itself is not sent to the web server.
    - scheduled:   nearest future experiment
    - idle:        no current/guard/future experiment
    """
    try:
        rows = config.r.xrange(config.KEY_EXPERIMENT_REGISTRY, count=200)
    except Exception:
        return _empty_experiment_snapshot(state="unknown")

    if not rows:
        return _empty_experiment_snapshot(state="idle")

    now_dt = utility.parse_local_dt(utility.local_ts())

    running = None
    wrapping = None
    future = None
    past_latest = None

    for xid, fields in rows:
        rec = _experiment_record_from_stream_row(xid, fields)
        if rec is None:
            continue

        state = str(rec.get("state") or "").strip().lower()
        if state in {"cancelled", "canceled", "deleted"}:
            continue

        start_dt = rec["_start_dt"]
        end_dt = rec["_end_dt"]
        wrapup_until_dt = rec["_wrapup_until_dt"]

        if start_dt <= now_dt <= end_dt:
            if running is None or start_dt > running["_start_dt"]:
                running = rec
            continue

        if end_dt < now_dt <= wrapup_until_dt:
            if wrapping is None or end_dt > wrapping["_end_dt"]:
                wrapping = rec
            continue

        if now_dt < start_dt:
            if future is None or start_dt < future["_start_dt"]:
                future = rec
            continue

        if now_dt > wrapup_until_dt:
            if past_latest is None or wrapup_until_dt > past_latest["_wrapup_until_dt"]:
                past_latest = rec

    if running is not None:
        return _public_experiment_snapshot(running, state="running", now_dt=now_dt)

    if wrapping is not None:
        return _public_experiment_snapshot(wrapping, state="wrapping_up", now_dt=now_dt)

    if future is not None:
        return _public_experiment_snapshot(
            future,
            state="scheduled",
            now_dt=now_dt,
            next_scheduled_at=future.get("start_at"),
        )

    if past_latest is not None:
        idle_duration_sec = int((now_dt - past_latest["_wrapup_until_dt"]).total_seconds())
        return _empty_experiment_snapshot(state="idle", idle_duration_sec=idle_duration_sec)

    return _empty_experiment_snapshot(state="idle")



def _build_status_snapshot(traffic_events: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    scanners = sorted(list(config.r.smembers(config.KEY_REGISTRY)))
    robot_states: List[Dict[str, Any]] = []
    ap_states: List[Dict[str, Any]] = []

    for rid in scanners:
        meta = config.r.hgetall(config.key_scanner_meta(rid)) or {}
        last_seen = meta.get("last_seen", "")
        device_type = (meta.get("device_type") or "robot").strip().lower()

        if device_type == "ap":
            try:
                ssids = json.loads(meta.get("ssids_json", "[]") or "[]")
                if not isinstance(ssids, list):
                    ssids = []
            except Exception:
                ssids = []

            try:
                interfaces = json.loads(meta.get("interfaces_json", "[]") or "[]")
                if not isinstance(interfaces, list):
                    interfaces = []
            except Exception:
                interfaces = []

            try:
                associations = json.loads(meta.get("associations_json", "[]") or "[]")
                if not isinstance(associations, list):
                    associations = []
            except Exception:
                associations = []

            channel_val = meta.get("channel", "")
            antenna_val = meta.get("antenna_count", "")
            ap_state = _freshness_state(last_seen, config.AP_STALE_TIMEOUT_SEC)

            ap_alias = (meta.get("ap_alias") or "").strip()
            ap_location = _load_ap_location_from_meta(meta)
            
            ap_states.append({
                "ap_id": ap_alias or rid,
                "ap_real_id": rid,
                "alias": ap_alias or "",
                "location": ap_location,
                "state": ap_state,
                "last_seen": last_seen,
                "mac": meta.get("mac", ""),
                "ip": meta.get("ip", ""),
                "ssids": ssids,
                "band": meta.get("band", ""),
                "channel": int(channel_val) if str(channel_val).isdigit() else None,
                "antenna_count": int(antenna_val) if str(antenna_val).isdigit() else None,
                "interfaces": interfaces,
                "associations": associations,
                "device_name": meta.get("device_name", ""),
                "traffic_enabled": meta.get("traffic_enabled", ""),
                "detail": "",
            })

        else:
            av_streaming = meta.get("av_streaming", "")
            if av_streaming == "1":
                stream_state = "on"
            elif av_streaming == "0":
                stream_state = "off"
            else:
                stream_state = "unknown"

            wifi_status = _json_dict_or_empty(meta.get("wifi_status_json", ""))
            scan_status = _json_dict_or_empty(meta.get("scan_status_json", ""))
            voice_status = _json_dict_or_empty(meta.get("voice_status_json", ""))

            true_loc = _load_true_location(rid)

            if true_loc.get("location_ok"):
                robot_location = {
                    "mode": "estimated",
                    "x": float(true_loc.get("x_m", 0.0)),
                    "y": float(true_loc.get("y_m", 0.0)),
                    "heading_deg": float(true_loc.get("heading_deg", 0.0)),
                }
            else:
                robot_location = {
                    "mode": "unknown",
                    "x": None,
                    "y": None,
                    "heading_deg": None,
                }

            robot_states.append({
                "robot_id": rid,
                "last_seen": last_seen,
                "mode": "unknown",
                "stream_state": stream_state,
                "stream_path": rid,
                "location": robot_location,
                "wifi_status": wifi_status,
                "scan_status": scan_status,
                "voice_status": voice_status,
                "last_status_report": meta.get("last_status_report", ""),
                "detail": meta.get("av_detail", "")[:200],
            })

    if traffic_events is None:
        traffic_events = []

    return {
        "schema_version": 2,
        "report_type": "nms_status",
        "nms_id": config.NMS_ID,
        "lab_id": _northbound_lab_id(),
        "time_local": utility.local_ts(),
        "time_format": config.TIME_FMT,
        "experiment": _build_experiment_snapshot(),
        "nms_status": {
            "online": True,
            "detail": "",
            "uplink_queue_total": 0,
            "command_queue_total": 0,
            "last_uplink_ok": True,
            "last_uplink_time": None,
        },
        "aps": ap_states,
        "robots": robot_states,
        "traffic_events": traffic_events,
    }


def _post_report_status(snapshot: Dict[str, Any]) -> Tuple[bool, str, List[Dict[str, Any]]]:
    try:
        config.r.set(config.KEY_NB_LAST_STATUS_PAYLOAD, json.dumps(snapshot, ensure_ascii=False))
        config.r.expire(config.KEY_NB_LAST_STATUS_PAYLOAD, config.NB_DEBUG_TTL_SEC)
    except Exception:
        pass

    try:
        resp = requests.post(
            config.WEB_NMS_STATUS_URL,
            json=snapshot,
            headers=_web_headers(),
            timeout=10,
            verify=getattr(config, "WEB_VERIFY_TLS", True),
        )
        j = resp.json()
        if isinstance(j, dict) and j.get("status") == "ok":
            cmds = j.get("commands") or []
            if not isinstance(cmds, list):
                cmds = []
            return True, "ok", cmds
        return False, f"web returned {j}", []
    except Exception as e:
        return False, f"post failed: {e}", []


def _apply_web_cmds_as_intents(cmds: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    """
    Convert web cmds -> desired intents.
    Returns (intent_updates, enqueued_to_pi, skipped_as_noop)
    """
    intent_updates = 0
    enq = 0
    noop = 0
    now = utility.local_ts()

    for c in (cmds or []):
        target = str(c.get("target") or "").strip()
        action = str(c.get("action") or "").strip()
        category = str(c.get("category") or "").strip() or "av"

        if not target or not action:
            noop += 1
            continue

        if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, target):
            noop += 1
            continue

        if action == "av.stream.start":
            desired = "on"
        elif action == "av.stream.stop":
            desired = "off"
        else:
            noop += 1
            continue

        try:
            config.r.hset(config.KEY_INTENT_VIDEO, target, desired)
            config.r.hset(config.KEY_INTENT_VIDEO_TS, target, now)
            intent_updates += 1
        except Exception:
            pass

        applied = (config.r.hget(config.KEY_APPLIED_VIDEO, target) or "").strip()
        if not applied:
            meta = config.r.hgetall(config.key_scanner_meta(target)) or {}
            if meta.get("av_streaming") == "1":
                applied = "on"
            elif meta.get("av_streaming") == "0":
                applied = "off"

        if applied == desired:
            noop += 1
            continue

        cmd_fields = {
            "category": category,
            "action": action,
            "execute_at": now,
            "created_at": now,
            "args_json": json.dumps({"camera_role": "front"}, ensure_ascii=False),
            "web_cmd_id": str(c.get("cmd_id") or ""),
        }
        config.r.xadd(config.key_cmd_stream(target), cmd_fields, maxlen=5000, approximate=True)
        enq += 1

    return intent_updates, enq, noop


def _northbound_status_once() -> Dict[str, Any]:
    traffic_events: List[Dict[str, Any]] = []
    traffic_event_ids: List[str] = []

    try:
        traffic_events, traffic_event_ids = _collect_traffic_events()
    except Exception:
        traffic_events, traffic_event_ids = [], []

    snap = _build_status_snapshot(traffic_events=traffic_events)

    ok = False
    detail = ""
    cmds: List[Dict[str, Any]] = []

    try:
        ok, detail, cmds = _post_report_status(snap)
    except Exception as e:
        ok = False
        detail = f"post_status exception: {e}"
        cmds = []

    if ok and traffic_event_ids:
        try:
            config.r.xdel(config.KEY_TRAFFIC_EVENT_STREAM, *traffic_event_ids)
        except Exception:
            pass

    now = utility.local_ts()

    try:
        config.r.set(config.KEY_NB_LAST_CMDS, json.dumps(cmds, ensure_ascii=False))
    except Exception:
        pass

    intent_updates = 0
    enq = 0
    noop = 0
    err = 0

    try:
        intent_updates, enq, noop = _apply_web_cmds_as_intents(cmds)
    except Exception:
        err += 1

    try:
        config.r.set(
            config.KEY_NB_LAST_STATUS,
            f"{now} ok={ok} cmds={len(cmds)} intent={intent_updates} enq={enq} noop={noop} err={err} detail={(detail or '')[:120]}"
        )
    except Exception:
        pass

    return {
        "ok": ok,
        "detail": detail,
        "time": now,
        "commands_count": len(cmds),
        "intent_updates": intent_updates,
        "enq": enq,
        "noop": noop,
        "err": err,
    }


async def _northbound_loop():
    while True:
        try:
            _northbound_upload_once()
        except Exception:
            pass
        await asyncio.sleep(config.NORTHBOUND_UPLOAD_EVERY_SEC)


async def _status_loop():
    while True:
        try:
            _northbound_status_once()
        except Exception:
            pass
        await asyncio.sleep(config.STATUS_EVERY_SEC)


def _freshness_state(last_seen: str, timeout_sec: int) -> str:
    if not str(last_seen or "").strip():
        return "offline"

    try:
        last_dt = utility.parse_local_dt(last_seen)
        now_dt = utility.parse_local_dt(utility.local_ts())
    except Exception:
        return "offline"

    age = (now_dt - last_dt).total_seconds()
    if age <= timeout_sec:
        return "active"
    return "stale"


def _float_or_none(v: Any):
    try:
        s = str(v or "").strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None
    

def _json_dict_or_empty(text: str) -> Dict[str, Any]:
    try:
        obj = json.loads(text or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}
    

def _load_ap_location_from_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    loc = _json_dict_or_empty(meta.get("location_json", ""))

    try:
        x = loc.get("x")
        y = loc.get("y")

        if x is None or y is None:
            raise ValueError("missing x/y")

        return {
            "mode": loc.get("mode") or "fixed",
            "x": float(x),
            "y": float(y),
            "z": _float_or_none(loc.get("z")),
            "source": loc.get("source", ""),
            "updated_at": loc.get("updated_at", ""),
        }
    except Exception:
        return {
            "mode": "unknown",
            "x": None,
            "y": None,
            "z": None,
            "source": "",
            "updated_at": "",
        }
    

def _load_true_location(scanner: str) -> Dict[str, Any]:
    try:
        pose = config.r.hgetall(key_pose(scanner)) or {}
        raw = pose.get("true_location_json", "") or ""
        obj = json.loads(raw) if raw else {}
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}


@router.get("/northbound/_list_experiment", tags=["5 Northbound"])
def list_experiments(limit: int = 50) -> Dict[str, Any]:
    """
    List registered experiments from Redis stream.

    Returns newest first.
    """
    rows = config.r.xrevrange(config.KEY_EXPERIMENT_REGISTRY, count=limit)

    items: List[Dict[str, Any]] = []

    for xid, fields in rows:
        rec = _experiment_record_from_stream_row(xid, fields)
        if rec is None:
            item = {
                "raw": dict(fields),
                "time_format": config.TIME_FMT,
            }
        else:
            item = {
                "experiment_id": rec.get("experiment_id"),
                "session_id": rec.get("session_id"),
                "lab_id": rec.get("lab_id"),
                "registered_at": rec.get("registered_at"),
                "start_at": rec.get("start_at"),
                "end_at": rec.get("end_at"),
                "command_count": rec.get("command_count"),
                "replace_existing": rec.get("replace_existing"),
                "state": rec.get("state"),
                "time_format": config.TIME_FMT,
            }
        items.append(item)

    return {
        "status": "ok",
        "count": len(items),
        "experiments": items,
    }
