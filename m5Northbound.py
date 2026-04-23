from typing import Dict, Any, List, Optional, Tuple
import json
import asyncio
import requests
from fastapi import APIRouter

import config
import utility

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

            item = {
                "scanner": scanner,
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
) -> Tuple[bool, str, int, int]:
    """
    Returns (ok, detail, accepted, rejected).
    We treat HTTP 2xx + JSON status=ok as ok.
    """
    payload = {
        "nms_id": config.NMS_ID,
        "time": utility.local_ts(),
        "time_format": config.TIME_FMT,
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
        resp = requests.post(config.WEB_NMS_UPLOAD_URL, json=payload, headers=_web_headers(), timeout=10)
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

    envelope_base = {
        "nms_id": config.NMS_ID,
        "time": utility.local_ts(),
        "time_format": config.TIME_FMT,
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


def _build_experiment_snapshot() -> Dict[str, Any]:
    rows = config.r.xrange(config.KEY_EXPERIMENT_REGISTRY, count=200)

    if not rows:
        return {
            "state": "idle",
            "session_id": None,
            "scenario_name": None,
            "started_at": None,
            "elapsed_sec": 0,
            "next_scheduled_at": None,
            "idle_duration_sec": 0,
        }

    now_dt = utility.parse_local_dt(utility.local_ts())

    current = None
    future = None
    past_latest = None

    for _, fields in rows:
        try:
            t0_dt = utility.parse_local_dt(str(fields.get("t0") or "").strip())
            last_dt = utility.parse_local_dt(str(fields.get("last_execute_at") or "").strip())
        except Exception:
            continue

        if t0_dt <= now_dt <= last_dt:
            current = (fields, t0_dt, last_dt)
            break

        if now_dt < t0_dt:
            if future is None or t0_dt < future[1]:
                future = (fields, t0_dt)

        if now_dt > last_dt:
            if past_latest is None or last_dt > past_latest[1]:
                past_latest = (fields, last_dt)

    if current is not None:
        fields, t0_dt, _ = current
        return {
            "state": "running",
            "session_id": fields.get("session_id"),
            "scenario_name": fields.get("scenario_name") or None,
            "started_at": fields.get("t0"),
            "elapsed_sec": max(0, int((now_dt - t0_dt).total_seconds())),
            "next_scheduled_at": None,
            "idle_duration_sec": 0,
        }

    if future is not None:
        fields, _ = future
        return {
            "state": "scheduled",
            "session_id": fields.get("session_id"),
            "scenario_name": fields.get("scenario_name") or None,
            "started_at": None,
            "elapsed_sec": 0,
            "next_scheduled_at": fields.get("t0"),
            "idle_duration_sec": 0,
        }

    if past_latest is not None:
        fields, last_dt = past_latest
        return {
            "state": "idle",
            "session_id": fields.get("session_id"),
            "scenario_name": fields.get("scenario_name") or None,
            "started_at": None,
            "elapsed_sec": 0,
            "next_scheduled_at": None,
            "idle_duration_sec": max(0, int((now_dt - last_dt).total_seconds())),
        }

    return {
        "state": "idle",
        "session_id": None,
        "scenario_name": None,
        "started_at": None,
        "elapsed_sec": 0,
        "next_scheduled_at": None,
        "idle_duration_sec": 0,
    }


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

            ap_states.append({
                "ap_id": rid,
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

            robot_states.append({
                "robot_id": rid,
                "last_seen": last_seen,
                "mode": "unknown",
                "stream_state": stream_state,
                "stream_path": rid,
                "location": {"mode": "unknown", "x": 0.0, "y": 0.0},
                "detail": meta.get("av_detail", "")[:200],
            })

    if traffic_events is None:
        traffic_events = []

    return {
        "nms_id": config.NMS_ID,
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
        resp = requests.post(config.WEB_NMS_STATUS_URL, json=snapshot, headers=_web_headers(), timeout=10)
        resp.raise_for_status()
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
            "args_json": "{}",
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


@router.get("/northbound/_list_experiment", tags=["5 Northbound"])
def list_experiments(limit: int = 50) -> Dict[str, Any]:
    """
    List registered experiments from Redis stream.

    Returns newest first.
    """
    rows = config.r.xrevrange(config.KEY_EXPERIMENT_REGISTRY, count=limit)

    items: List[Dict[str, Any]] = []

    for xid, fields in rows:
        item = {
            "stream_id": xid,
            "session_id": fields.get("session_id"),
            "scenario_name": fields.get("scenario_name") or "",
            "registered_at": fields.get("registered_at"),
            "t0": fields.get("t0"),
            "last_execute_at": fields.get("last_execute_at"),
            "item_count": int(fields.get("item_count") or 0),
            "source": fields.get("source") or "",
            "time_format": fields.get("time_format") or config.TIME_FMT,
        }
        items.append(item)

    return {
        "status": "ok",
        "count": len(items),
        "experiments": items,
    }
