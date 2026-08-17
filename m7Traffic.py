# Ownership rule:
# - m7Traffic.py is a passive producer only.
# - It may enqueue traffic commands, execute iperf3, and write:
#     * traffic events  -> KEY_TRAFFIC_EVENT_STREAM
#     * iperf3 results  -> KEY_TRAFFIC_RESULT_STREAM
# - It must NOT build or post northbound payloads.
# - _northbound_loop() in m5Northbound.py is the sole owner of the 1-minute northbound report.
# 
from typing import Optional, Dict, Any, List
from datetime import timedelta
import json
import csv
import io
import subprocess
import threading
import os
import signal
import asyncio

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

import config
import utility
import m1Registry

router = APIRouter()


# =========================
# 7) iperf3 Traffic (NMS-side)
# =========================

IPERF3_BIN = "iperf3"

AC_TO_TOS = {
    "vo": 184,
    "vi": 136,
    "be": 0,
    "bk": 32,
}

UDP_DEFAULT_BITRATE = {
    "vo": "100K",
    "vi": "5M",
    "be": "100M",
    "bk": "100M",
}


def _get_scanner_wifi_ip(scanner: str) -> str:
    """
    Authoritative traffic IP comes from /registry/register only.
    m4 cmd_poll must not overwrite this field anymore.
    """
    meta = config.r.hgetall(config.key_scanner_meta(scanner)) or {}
    return (meta.get("ip") or "").strip()


def _get_scanner_wifi_snapshot(scanner: str) -> Dict[str, Any]:
    """Return the most recent robot Wi-Fi association known to the NMS.

    The snapshot is captured when iperf3 is started and again when it exits.
    Keeping both observations with the traffic record prevents a later
    5/6-GHz roam from being mistaken for the band used by an earlier session.
    """
    meta = config.r.hgetall(config.key_scanner_meta(scanner)) or {}

    try:
        wifi = json.loads(meta.get("wifi_status_json") or "{}")
        if not isinstance(wifi, dict):
            wifi = {}
    except Exception:
        wifi = {}

    freq_raw = wifi.get("freq_mhz")
    try:
        freq_mhz = int(freq_raw) if str(freq_raw or "").strip() else None
    except Exception:
        freq_mhz = None

    band = str(wifi.get("band") or "").strip().lower()
    if not band and freq_mhz is not None:
        if 2400 <= freq_mhz < 2500:
            band = "2.4g"
        elif 4900 <= freq_mhz < 5925:
            band = "5g"
        elif 5925 <= freq_mhz < 7125:
            band = "6g"

    snapshot: Dict[str, Any] = {
        "captured_at": utility.local_ts(),
        "status_reported_at": str(meta.get("last_status_report") or "").strip(),
        "scanner": scanner,
        "traffic_ip": (meta.get("ip") or "").strip(),
        "connected": bool(wifi.get("connected")),
        "iface": str(wifi.get("iface") or "").strip(),
        "iface_mac": str(wifi.get("iface_mac") or "").strip(),
        "ssid": str(wifi.get("ssid") or "").strip(),
        "assoc_bssid": str(wifi.get("assoc_bssid") or "").strip(),
        "freq_mhz": freq_mhz,
        "channel": wifi.get("channel"),
        "band": band,
    }
    return snapshot


def _alloc_port(scanner: str) -> str:
    """
    Allocate from the per-robot FIFO free-port queue.
    If queue is empty, initialize it once with TRAFFIC_PORT_START..TRAFFIC_PORT_END.
    """
    key = config.key_traffic_temp_ports(scanner)

    if int(config.r.llen(key)) == 0:
        pipe = config.r.pipeline()
        for p in range(config.TRAFFIC_PORT_START, config.TRAFFIC_PORT_END + 1):
            pipe.rpush(key, str(p))
        pipe.expire(key, config.TRAFFIC_TEMP_TTL_SEC)
        pipe.execute()

    port = config.r.lpop(key)
    if port is not None:
        config.r.expire(key, config.TRAFFIC_TEMP_TTL_SEC)

    return port or ""


def _release_port(scanner: str, port: str) -> None:
    """
    Release port back to the tail of the per-robot FIFO queue.
    """
    port_s = str(port or "").strip()
    if not port_s:
        return

    key = config.key_traffic_temp_ports(scanner)
    config.r.rpush(key, port_s)
    config.r.expire(key, config.TRAFFIC_TEMP_TTL_SEC)


def _push_event(
    scanner: str,
    session_id: str,
    action: str,
    status: str,
    detail: str = "",
    duration_sec: Optional[int] = None,
    reverse: Optional[bool] = None,
    protocol: Optional[str] = None,
    ac: Optional[str] = None,
    bitrate: Optional[str] = None,
    packet_size: Optional[int] = None,
    parallel: Optional[int] = None,
    wifi_start: Optional[Dict[str, Any]] = None,
) -> None:
    fields = {
        "scanner": scanner,
        "session_id": session_id,
        "action": action,
        "status": status,
        "completion_time": utility.local_ts(),
        "detail": detail or "",
    }

    if duration_sec is not None:
        fields["duration_sec"] = str(int(duration_sec))

    if reverse is not None:
        fields["reverse"] = "1" if reverse else "0"

    protocol_s = str(protocol or "").strip().lower()
    if protocol_s:
        fields["protocol"] = protocol_s

    ac_s = str(ac or "").strip().lower()
    if ac_s:
        fields["ac"] = ac_s

    bitrate_s = str(bitrate or "").strip()
    if bitrate_s:
        fields["bitrate"] = bitrate_s

    if packet_size is not None:
        fields["packet_size"] = str(int(packet_size))

    if parallel is not None:
        fields["parallel"] = str(int(parallel))

    if wifi_start is not None:
        fields["wifi_start_json"] = json.dumps(wifi_start, ensure_ascii=False)

    # Operational short-lived event stream (consumed by _status_loop)
    config.r.xadd(
        config.KEY_TRAFFIC_EVENT_STREAM,
        fields,
        maxlen=config.TRAFFIC_EVENT_MAXLEN,
        approximate=True,
    )

    # Debug/history mirror with longer TTL for RedisInsight / lab-map validation
    config.r.xadd(
        config.KEY_TRAFFIC_EVENT_TEMP_STREAM,
        fields,
        maxlen=config.TRAFFIC_EVENT_TEMP_MAXLEN,
        approximate=True,
    )
    config.r.expire(config.KEY_TRAFFIC_EVENT_TEMP_STREAM, config.TRAFFIC_TEMP_TTL_SEC)


def _push_result(
    scanner: str,
    session_id: str,
    status: str,
    raw: Any,
    detail: str = "",
    reverse: Optional[bool] = None,
    protocol: Optional[str] = None,
    ac: Optional[str] = None,
    bitrate: Optional[str] = None,
    packet_size: Optional[int] = None,
    parallel: Optional[int] = None,
    wifi_start: Optional[Dict[str, Any]] = None,
    wifi_end: Optional[Dict[str, Any]] = None,
) -> None:
    fields = {
        "scanner": scanner,
        "session_id": session_id,
        "completion_time": utility.local_ts(),
        "status": status,
        "detail": detail or "",
        "raw_json": json.dumps(raw or {}, ensure_ascii=False),
    }

    if reverse is not None:
        fields["reverse"] = "1" if reverse else "0"

    protocol_s = str(protocol or "").strip().lower()
    if protocol_s:
        fields["protocol"] = protocol_s

    ac_s = str(ac or "").strip().lower()
    if ac_s:
        fields["ac"] = ac_s

    bitrate_s = str(bitrate or "").strip()
    if bitrate_s:
        fields["bitrate"] = bitrate_s

    if packet_size is not None:
        fields["packet_size"] = str(int(packet_size))

    if parallel is not None:
        fields["parallel"] = str(int(parallel))

    if wifi_start is not None:
        fields["wifi_start_json"] = json.dumps(wifi_start, ensure_ascii=False)

    if wifi_end is not None:
        fields["wifi_end_json"] = json.dumps(wifi_end, ensure_ascii=False)

    config.r.xadd(
        config.KEY_TRAFFIC_RESULT_STREAM,
        fields,
        maxlen=config.TRAFFIC_RESULT_MAXLEN,
        approximate=True,
    )


class TrafficCmd(BaseModel):
    scanner: str
    action: str = Field(..., description="traffic.session.start or traffic.session.stop")
    execute_at: Optional[str] = None
    args: Dict[str, Any] = Field(default_factory=dict)
    args_json_text: Optional[str] = None


class TrafficScriptItem(BaseModel):
    scanner: str
    t_offset_sec: int
    action: str
    args: Dict[str, Any] = Field(default_factory=dict)


class TrafficScriptLoad(BaseModel):
    t0: str
    items: List[TrafficScriptItem]


class TrafficLoadCSVReq(BaseModel):
    t0: str = Field(..., description=f"Absolute local time, format: {config.TIME_FMT}")
    csv_text: str = Field(
        ...,
        description="CSV rows with columns: scanner,t_offset_sec,category,action,args_json",
    )


def _normalize_action(action: str) -> str:
    return (action or "").strip()


def _normalize_reverse(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _validate_execute_at(raw: Optional[str]) -> str:
    created_at = utility.local_ts()
    s = (raw or "").strip()
    if not s:
        return created_at
    try:
        return utility.parse_local_dt(s).strftime(config.TIME_FMT)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"execute_at must be like '{utility.local_ts()}' (format {config.TIME_FMT})",
        )


def _parse_args_json(args: Dict[str, Any], args_json_text: Optional[str]) -> str:
    if args_json_text is not None and args_json_text.strip() != "":
        raw = args_json_text.strip()
        try:
            j = json.loads(raw)
            if not isinstance(j, dict):
                raise ValueError("args_json_text must be a JSON object")
        except Exception:
            raise HTTPException(status_code=400, detail="args_json_text must be valid JSON object text")
        return raw

    return json.dumps(args or {}, ensure_ascii=False)


def _traffic_enqueue_core(
    *,
    scanner: str,
    action: str,
    execute_at: Optional[str],
    args_json: str,
    lab_id: str = "",
    experiment_id: str = "",
    session_id: str = "",
) -> Dict[str, Any]:
    m1Registry.require_whitelisted(scanner)

    if action not in ("traffic.session.start", "traffic.session.stop"):
        raise HTTPException(status_code=400, detail="action must be traffic.session.start or traffic.session.stop")

    created_at = utility.local_ts()
    execute_at_norm = _validate_execute_at(execute_at)

    xid = config.r.xadd(
        config.KEY_TRAFFIC_CMD_STREAM,
        {
            "scanner": scanner,
            "action": action,
            "execute_at": execute_at_norm,
            "created_at": created_at,
            "args_json": args_json,
            "lab_id": str(lab_id or ""),
            "experiment_id": str(experiment_id or ""),
            "session_id": str(session_id or ""),
        },
        maxlen=config.TRAFFIC_CMD_MAXLEN,
        approximate=True,
    )

    return {
        "status": "ok",
        "scanner": scanner,
        "action": action,
        "cmd_id": xid,
        "created_at": created_at,
        "execute_at": execute_at_norm,
        "time_format": config.TIME_FMT,
    }


@router.post("/traffic/_enqueue", tags=["7 iperf3 Traffic"])
def traffic_enqueue(cmd: TrafficCmd) -> Dict[str, Any]:
    scanner = (cmd.scanner or "").strip()
    if not scanner:
        raise HTTPException(status_code=400, detail="scanner required")

    action = _normalize_action(cmd.action)
    args_json = _parse_args_json(cmd.args or {}, cmd.args_json_text)

    return _traffic_enqueue_core(
        scanner=scanner,
        action=action,
        execute_at=cmd.execute_at,
        args_json=args_json,
    )


@router.post("/traffic/_stop", tags=["7 iperf3 Traffic"])
def traffic_stop(cmd: TrafficCmd) -> Dict[str, Any]:
    scanner = (cmd.scanner or "").strip()
    if not scanner:
        raise HTTPException(status_code=400, detail="scanner required")

    action = _normalize_action(cmd.action)
    if action != "traffic.session.stop":
        raise HTTPException(status_code=400, detail="action must be traffic.session.stop")

    args_json = _parse_args_json(cmd.args or {}, cmd.args_json_text)

    return _traffic_enqueue_core(
        scanner=scanner,
        action=action,
        execute_at=cmd.execute_at,
        args_json=args_json,
    )


@router.get("/traffic/_list", tags=["7 iperf3 Traffic"])
def traffic_list(limit: int = Query(200, ge=1, le=2000)) -> Dict[str, Any]:
    rows = config.r.xrange(config.KEY_TRAFFIC_CMD_STREAM, count=limit)

    items: List[Dict[str, Any]] = []
    for xid, fields in rows:
        item = dict(fields)
        item["cmd_id"] = xid
        items.append(item)

    return {
        "time": utility.local_ts(),
        "count": len(items),
        "items": items,
        "key": config.KEY_TRAFFIC_CMD_STREAM,
    }


@router.get("/traffic/_get/{cmd_id}", tags=["7 iperf3 Traffic"])
def traffic_get(cmd_id: str) -> Dict[str, Any]:
    rows = config.r.xrange(config.KEY_TRAFFIC_CMD_STREAM, min=cmd_id, max=cmd_id, count=1)
    if not rows:
        raise HTTPException(status_code=404, detail=f"traffic cmd_id not found: {cmd_id}")

    xid, fields = rows[0]
    return {
        "time": utility.local_ts(),
        "item": {
            "cmd_id": xid,
            **dict(fields),
        },
    }


@router.post("/traffic/_load_script", tags=["7 iperf3 Traffic"])
def traffic_load_script(script: TrafficScriptLoad) -> Dict[str, Any]:
    try:
        t0_dt = utility.parse_local_dt(script.t0)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid t0; expected like '{utility.local_ts()}' (format {config.TIME_FMT})",
        )

    added = 0
    skipped_not_whitelisted = 0
    bad_rows = 0

    for it in script.items:
        scanner = (it.scanner or "").strip()
        action = _normalize_action(it.action)

        if not scanner or action not in ("traffic.session.start", "traffic.session.stop"):
            bad_rows += 1
            continue

        if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, scanner):
            skipped_not_whitelisted += 1
            continue

        execute_at = (t0_dt + timedelta(seconds=int(it.t_offset_sec))).strftime(config.TIME_FMT)

        _traffic_enqueue_core(
            scanner=scanner,
            action=action,
            execute_at=execute_at,
            args_json=json.dumps(it.args or {}, ensure_ascii=False),
        )
        added += 1

    return {
        "status": "ok",
        "added": added,
        "skipped_not_whitelisted": skipped_not_whitelisted,
        "bad_rows": bad_rows,
        "t0": t0_dt.strftime(config.TIME_FMT),
        "time_format": config.TIME_FMT,
    }


@router.post("/traffic/_load_csv", tags=["7 iperf3 Traffic"])
def traffic_load_csv(req: TrafficLoadCSVReq) -> Dict[str, Any]:
    try:
        t0_dt = utility.parse_local_dt(req.t0)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid t0; expected like '{utility.local_ts()}' (format {config.TIME_FMT})",
        )

    f = io.StringIO(req.csv_text)
    reader = csv.DictReader(f)
    required_cols = {"scanner", "t_offset_sec", "category", "action", "args_json"}
    if not required_cols.issubset(set(reader.fieldnames or [])):
        raise HTTPException(status_code=400, detail=f"CSV must have columns: {sorted(list(required_cols))}")

    added = 0
    skipped_not_whitelisted = 0
    bad_rows = 0

    for row in reader:
        scanner = (row.get("scanner") or "").strip()
        if not scanner:
            bad_rows += 1
            continue

        if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, scanner):
            skipped_not_whitelisted += 1
            continue

        category = (row.get("category") or "").strip().lower()
        action = _normalize_action(row.get("action") or "")

        if category != "traffic" and action not in ("traffic.session.start", "traffic.session.stop"):
            continue

        try:
            offset = int((row.get("t_offset_sec") or "0").strip())
        except Exception:
            bad_rows += 1
            continue

        args_s = (row.get("args_json") or "").strip()
        if args_s:
            try:
                args = json.loads(args_s)
                if not isinstance(args, dict):
                    bad_rows += 1
                    continue
            except Exception:
                bad_rows += 1
                continue
        else:
            args = {}

        execute_at = (t0_dt + timedelta(seconds=offset)).strftime(config.TIME_FMT)

        _traffic_enqueue_core(
            scanner=scanner,
            action=action,
            execute_at=execute_at,
            args_json=json.dumps(args, ensure_ascii=False),
        )
        added += 1

    return {
        "status": "ok",
        "t0": t0_dt.strftime(config.TIME_FMT),
        "time_format": config.TIME_FMT,
        "added": added,
        "skipped_not_whitelisted": skipped_not_whitelisted,
        "bad_rows": bad_rows,
    }


def _collect_due_traffic_commands(server_now_str: str):
    server_now = utility.parse_local_dt(server_now_str)
    raw = config.r.xrange(config.KEY_TRAFFIC_CMD_STREAM, count=5000)

    due = []

    for xid, fields in raw:
        exec_at_s = fields.get("execute_at", "")
        if not exec_at_s:
            continue

        try:
            exec_at = utility.parse_local_dt(exec_at_s)
        except Exception:
            continue

        if exec_at > server_now:
            continue

        f2 = dict(fields)
        f2["cmd_id"] = xid
        due.append((xid, f2))

        if len(due) >= config.TRAFFIC_LOOP_BATCH_LIMIT:
            break

    return due


def _execute_start_real(scanner: str, args: Dict[str, Any]):
    session_id = str(args.get("session_id") or "").strip()
    if not session_id:
        return False, "session_id missing", None

    target_ip = _get_scanner_wifi_ip(scanner)
    if not target_ip:
        return False, "no valid Wi-Fi IP", _get_scanner_wifi_snapshot(scanner)

    port = _alloc_port(scanner)
    if not port:
        return False, "no free port", _get_scanner_wifi_snapshot(scanner)

    wifi_start = _get_scanner_wifi_snapshot(scanner)

    protocol = str(args.get("protocol") or "udp").lower()
    ac = str(args.get("ac") or "").lower()

    reverse = _normalize_reverse(args.get("reverse"))

    tos = AC_TO_TOS.get(ac, 0)

    duration = int(args.get("duration_sec") or 60)
    interval = int(args.get("report_interval_sec") or 60)

    bitrate: Optional[str] = None
    packet_size: Optional[int] = None
    parallel: Optional[int] = None

    cmd = [
        IPERF3_BIN,
        "-c", target_ip,
        "-p", port,
        "-t", str(duration),
        "-i", str(interval),
        "--tos", str(tos),
        "-J",
    ]

    if reverse:
        cmd.append("-R")

    if protocol == "udp":
        bitrate = str(args.get("bitrate") or UDP_DEFAULT_BITRATE.get(ac, "1M"))
        packet_size = int(args.get("packet_size") or 1500)

        cmd += ["-u", "-b", bitrate, "-l", str(packet_size)]

    else:
        parallel = int(args.get("parallel") or 1)
        cmd += ["-P", str(parallel)]

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
    except Exception as e:
        _release_port(scanner, port)
        return False, f"start failed: {e}", wifi_start

    # save runtime state (minimal)
    config.r.set(
        config.key_traffic_temp_running(scanner, session_id),
        json.dumps({
            "pid": proc.pid,
            "port": port,
            "wifi_start": wifi_start,
        }),
    )
    config.r.expire(
        config.key_traffic_temp_running(scanner, session_id),
        config.TRAFFIC_TEMP_TTL_SEC
    )

    def _watch():
        status = "ok"
        detail = ""
        raw: Any = {}

        try:
            out, _ = proc.communicate()
            text = (out or "").strip()

            if text:
                try:
                    raw = json.loads(text)
                    status = "ok"
                    detail = ""
                except Exception:
                    raw = {"raw_output": text}
                    status = "error"
                    detail = "iperf3 output is not valid JSON (likely interrupted)"
            else:
                raw = {}
                status = "error"
                detail = "iperf3 produced no output (likely interrupted)"
        except Exception as e:
            raw = {}
            status = "error"
            detail = f"watcher exception: {type(e).__name__}: {e}"
        finally:
            try:
                _push_result(
                    scanner,
                    session_id,
                    status,
                    raw,
                    detail,
                    reverse=reverse,
                    protocol=protocol,
                    ac=ac,
                    bitrate=bitrate,
                    packet_size=packet_size,
                    parallel=parallel,
                    wifi_start=wifi_start,
                    wifi_end=_get_scanner_wifi_snapshot(scanner),
                )
            finally:
                _release_port(scanner, port)
                config.r.delete(config.key_traffic_temp_running(scanner, session_id))

    threading.Thread(target=_watch, daemon=True).start()

    return True, f"started {session_id}", wifi_start


def _execute_stop_real(scanner: str, args: Dict[str, Any]):
    session_id = str(args.get("session_id") or "").strip()
    if not session_id:
        return False, "session_id missing"

    key = config.key_traffic_temp_running(scanner, session_id)
    s = config.r.get(key)

    if s:
        try:
            j = json.loads(s)
            pid = int(j.get("pid") or 0)
        except Exception:
            pid = 0

        if pid > 0:
            try:
                os.killpg(pid, signal.SIGTERM)
            except Exception:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    return False, "failed to kill process"

        return True, "stopped running session"

    # cancel queued future start
    raw = config.r.xrange(config.KEY_TRAFFIC_CMD_STREAM, count=5000)

    removed = 0
    for xid, fields in raw:
        if fields.get("action") != "traffic.session.start":
            continue

        if fields.get("scanner") != scanner:
            continue

        try:
            args2 = json.loads(fields.get("args_json") or "{}")
        except Exception:
            continue

        if str(args2.get("session_id") or "") != session_id:
            continue

        removed += int(config.r.xdel(config.KEY_TRAFFIC_CMD_STREAM, xid))

    if removed > 0:
        return True, f"cancelled {removed} queued session(s)"

    return False, "no running or queued session found"


def _event_duration_sec_from_args(args: Dict[str, Any]) -> int:
    try:
        d = int(args.get("duration_sec") or 60)
        return d if d > 0 else 60
    except Exception:
        return 60


def _execute_due_command(xid: str, fields: Dict[str, str]) -> None:
    scanner = (fields.get("scanner") or "").strip()
    action = (fields.get("action") or "").strip()

    try:
        args = json.loads(fields.get("args_json") or "{}")
        if not isinstance(args, dict):
            args = {}
    except Exception:
        args = {}

    reverse = _normalize_reverse(args.get("reverse"))
    session_id = str(args.get("session_id") or "").strip()
    protocol = str(args.get("protocol") or "").strip().lower()
    ac = str(args.get("ac") or "").strip().lower()
    bitrate: Optional[str] = None
    packet_size: Optional[int] = None
    parallel: Optional[int] = None

    if protocol == "udp":
        bitrate = str(args.get("bitrate") or UDP_DEFAULT_BITRATE.get(ac, "1M"))
        packet_size = int(args.get("packet_size") or 1500)
    elif protocol == "tcp":
        parallel = int(args.get("parallel") or 1)

    ok = False
    detail = ""
    duration_sec: Optional[int] = None
    wifi_start: Optional[Dict[str, Any]] = None

    if action == "traffic.session.start":
        duration_sec = _event_duration_sec_from_args(args)
        ok, detail, wifi_start = _execute_start_real(scanner, args)

    elif action == "traffic.session.stop":
        ok, detail = _execute_stop_real(scanner, args)

    _push_event(
        scanner=scanner,
        session_id=session_id,
        action=action,
        status="ok" if ok else "error",
        detail=detail,
        duration_sec=duration_sec,
        reverse=reverse if action == "traffic.session.start" else None,
        protocol=protocol if action == "traffic.session.start" else None,
        ac=ac if action == "traffic.session.start" else None,
        bitrate=bitrate if action == "traffic.session.start" else None,
        packet_size=packet_size if action == "traffic.session.start" else None,
        parallel=parallel if action == "traffic.session.start" else None,
        wifi_start=wifi_start if action == "traffic.session.start" else None,
    )

    try:
        config.r.xdel(config.KEY_TRAFFIC_CMD_STREAM, xid)
    except Exception:
        pass


async def _traffic_loop() -> None:
    while True:
        try:
            server_now_str = utility.local_ts()
            due = _collect_due_traffic_commands(server_now_str)

            for xid, fields in due:
                try:
                    _execute_due_command(xid, fields)
                except Exception as e:
                    # fail-safe: never crash loop
                    try:
                        scanner = fields.get("scanner", "")
                        args = json.loads(fields.get("args_json") or "{}")
                        session_id = str(args.get("session_id") or "")
                        protocol = str(args.get("protocol") or "").strip().lower()
                        ac = str(args.get("ac") or "").strip().lower()
                        bitrate = None
                        packet_size = None
                        parallel = None
                        if protocol == "udp":
                            bitrate = str(args.get("bitrate") or UDP_DEFAULT_BITRATE.get(ac, "1M"))
                            packet_size = int(args.get("packet_size") or 1500)
                        elif protocol == "tcp":
                            parallel = int(args.get("parallel") or 1)
                    except Exception:
                        scanner = ""
                        session_id = ""
                        protocol = ""
                        ac = ""
                        bitrate = None
                        packet_size = None
                        parallel = None

                    _push_event(
                        scanner=scanner,
                        session_id=session_id,
                        action=fields.get("action", ""),
                        status="error",
                        detail=f"loop exception: {type(e).__name__}: {e}",
                        protocol=protocol or None,
                        ac=ac or None,
                        bitrate=bitrate,
                        packet_size=packet_size,
                        parallel=parallel,
                    )

                    try:
                        config.r.xdel(config.KEY_TRAFFIC_CMD_STREAM, xid)
                    except Exception:
                        pass

        except Exception:
            pass

        await asyncio.sleep(config.TRAFFIC_LOOP_EVERY_SEC)
