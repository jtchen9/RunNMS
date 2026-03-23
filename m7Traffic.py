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


def _push_event(scanner: str, session_id: str, action: str, status: str, detail: str = "") -> None:
    config.r.xadd(
        config.KEY_TRAFFIC_EVENT_STREAM,
        {
            "scanner": scanner,
            "session_id": session_id,
            "action": action,
            "status": status,
            "completion_time": utility.local_ts(),
            "detail": detail or "",
        },
        maxlen=config.TRAFFIC_EVENT_MAXLEN,
        approximate=True,
    )


def _push_result(scanner: str, session_id: str, status: str, raw: Any, detail: str = "") -> None:
    config.r.xadd(
        config.KEY_TRAFFIC_RESULT_STREAM,
        {
            "scanner": scanner,
            "session_id": session_id,
            "completion_time": utility.local_ts(),
            "status": status,
            "detail": detail or "",
            "raw_json": json.dumps(raw or {}, ensure_ascii=False),
        },
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
        return False, "session_id missing"

    target_ip = _get_scanner_wifi_ip(scanner)
    if not target_ip:
        return False, "no valid Wi-Fi IP"

    port = _alloc_port(scanner)
    if not port:
        return False, "no free port"

    protocol = str(args.get("protocol") or "udp").lower()
    ac = str(args.get("ac") or "").lower()

    tos = AC_TO_TOS.get(ac, 0)

    duration = int(args.get("duration_sec") or 60)
    interval = int(args.get("report_interval_sec") or 60)

    cmd = [
        IPERF3_BIN,
        "-c", target_ip,
        "-p", port,
        "-t", str(duration),
        "-i", str(interval),
        "--tos", str(tos),
        "-J",
    ]

    if protocol == "udp":
        bitrate = str(args.get("bitrate") or UDP_DEFAULT_BITRATE.get(ac, "1M"))
        pkt = int(args.get("packet_size") or 1500)

        cmd += ["-u", "-b", bitrate, "-l", str(pkt)]

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
        return False, f"start failed: {e}"

    # save runtime state (minimal)
    config.r.set(
        config.key_traffic_temp_running(scanner, session_id),
        json.dumps({
            "pid": proc.pid,
            "port": port,
        }),
    )
    config.r.expire(
        config.key_traffic_temp_running(scanner, session_id),
        config.TRAFFIC_TEMP_TTL_SEC
    )

    def _watch():
        try:
            out, _ = proc.communicate()
            raw = json.loads(out) if out else {}
            _push_result(scanner, session_id, "ok", raw)
        except Exception as e:
            _push_result(scanner, session_id, "error", {}, str(e))
        finally:
            _release_port(scanner, port)
            config.r.delete(config.key_traffic_temp_running(scanner, session_id))

    threading.Thread(target=_watch, daemon=True).start()

    return True, f"started {session_id}"


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


def _execute_due_command(xid: str, fields: Dict[str, str]) -> None:
    scanner = (fields.get("scanner") or "").strip()
    action = (fields.get("action") or "").strip()

    try:
        args = json.loads(fields.get("args_json") or "{}")
        if not isinstance(args, dict):
            args = {}
    except Exception:
        args = {}

    session_id = str(args.get("session_id") or "").strip()

    ok = False
    detail = ""

    if action == "traffic.session.start":
        ok, detail = _execute_start_real(scanner, args)

    elif action == "traffic.session.stop":
        ok, detail = _execute_stop_real(scanner, args)

    _push_event(
        scanner=scanner,
        session_id=session_id,
        action=action,
        status="ok" if ok else "error",
        detail=detail,
    )

    # delete command after execution
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
                    except Exception:
                        scanner = ""
                        session_id = ""

                    _push_event(
                        scanner=scanner,
                        session_id=session_id,
                        action=fields.get("action", ""),
                        status="error",
                        detail=f"loop exception: {type(e).__name__}: {e}",
                    )

                    try:
                        config.r.xdel(config.KEY_TRAFFIC_CMD_STREAM, xid)
                    except Exception:
                        pass

        except Exception:
            pass

        await asyncio.sleep(config.TRAFFIC_LOOP_EVERY_SEC)
