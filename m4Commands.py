from typing import Optional, List, Dict, Any
from datetime import timedelta
import json
import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request, UploadFile, File, Form
from pydantic import BaseModel, Field

import config
import utility
import m1Registry
import m7Traffic
import m8mobility
from m8mobility_state_store import key_report, key_time

router = APIRouter()


# ==================
# 4) Commands
# ==================
class Cmd(BaseModel):
    category: str = "scan"
    action: str
    execute_at: Optional[str] = None
    args: Dict[str, Any] = Field(default_factory=dict)
    args_json_text: Optional[str] = None


class CmdAck(BaseModel):
    cmd_id: str
    status: str
    finished_at: Optional[str] = None
    detail: Optional[str] = None


class ScriptItem(BaseModel):
    scanner: str
    t_offset_sec: int
    category: str = "scan"
    action: str
    args: Dict[str, Any] = Field(default_factory=dict)


class ScriptLoad(BaseModel):
    t0: str
    items: List[ScriptItem]


class CmdLoadCSVReq(BaseModel):
    t0: str = Field(..., description=f"Absolute local time, format: {config.TIME_FMT}")
    csv_text: str = Field(..., description="CSV rows with columns: scanner,t_offset_sec,category,action,args_json")


class CmdPollReq(BaseModel):
    limit: int = 20
    av_streaming: Optional[int] = None
    av_detail: Optional[str] = None
    boot_id: Optional[str] = None
    status_report: Dict[str, Any] = Field(default_factory=dict)
    mobility_report: Optional[Dict[str, Any]] = None


@router.get("/cmd/_list_command_queues", tags=["4 Commands (Polling)"])
def cmd_list_command_queues() -> Dict[str, Any]:
    scanners = sorted(list(config.r.smembers(config.KEY_REGISTRY)))
    out: List[Dict[str, Any]] = []

    for s in scanners:
        key = config.key_cmd_stream(s)
        length = int(config.r.xlen(key))
        if length > 0:
            age_sec, oldest_id = utility._stream_oldest_age_sec(key)
        else:
            age_sec, oldest_id = 0, ""

        out.append({
            "queue_type": "command",
            "scanner": s,
            "key": key,
            "length": length,
            "maxlen": 5000,
            "oldest_age_sec": int(age_sec),
            "oldest_id": oldest_id,
        })

    return {
        "time": utility.local_ts(),
        "count": len(out),
        "items": out,
    }


@router.get("/cmd/_list_command_queue/{scanner}", tags=["4 Commands (Polling)"])
def cmd_list_command_queue(scanner: str) -> Dict[str, Any]:
    m1Registry.require_whitelisted(scanner)
    key = config.key_cmd_stream(scanner)

    length = int(config.r.xlen(key))
    if length > 0:
        age_sec, oldest_id = utility._stream_oldest_age_sec(key)
    else:
        age_sec, oldest_id = 0, ""

    return {
        "time": utility.local_ts(),
        "queue_type": "command",
        "scanner": scanner,
        "key": key,
        "length": length,
        "maxlen": 5000,
        "oldest_age_sec": int(age_sec),
        "oldest_id": oldest_id,
    }


def _cmd_enqueue_core(scanner: str, cmd: "Cmd") -> Dict[str, Any]:
    m1Registry.require_whitelisted(scanner)

    created_at = utility.local_ts()
    raw = (cmd.execute_at or "").strip()

    if raw == "":
        execute_at_norm = created_at
    else:
        try:
            execute_at_norm = utility.parse_local_dt(cmd.execute_at).strftime(config.TIME_FMT)
        except Exception:
            raise HTTPException(
                status_code=400,
                detail=f"execute_at must be like '{utility.local_ts()}' (format {config.TIME_FMT})"
            )
    if cmd.args_json_text is not None and cmd.args_json_text.strip() != "":
        raw = cmd.args_json_text.strip()
        try:
            json.loads(raw)
        except Exception:
            raise HTTPException(status_code=400, detail="args_json_text must be valid JSON text")
        args_json = raw
    else:
        args_json = json.dumps(cmd.args or {}, ensure_ascii=False)

    category_n = (cmd.category or "").strip().lower()
    action_n = (cmd.action or "").strip()
    args_obj = json.loads(args_json)

    if category_n == "mobility":
        if (cmd.execute_at or "").strip() not in ("", created_at):
            raise HTTPException(status_code=400, detail="Phase 1 mobility testing only supports immediate execution")
        return m8mobility.on_command_issued(scanner, action_n, args_obj)

    fields = {
        "category": cmd.category,
        "action": cmd.action,
        "execute_at": execute_at_norm,
        "created_at": created_at,
        "args_json": args_json,
    }

    xid = config.r.xadd(config.key_cmd_stream(scanner), fields, maxlen=5000, approximate=True)

    return {
        "status": "ok",
        "scanner": scanner,
        "cmd_id": xid,
        "created_at": created_at,
        "time_format": config.TIME_FMT,
    }


def _register_experiment_status(
    *,
    t0_str: str,
    last_execute_at_str: str,
    item_count: int,
    source: str,
    scenario_name: str = "",
) -> Dict[str, Any]:
    now = utility.local_ts()
    session_id = f"exp-{now}"

    fields = {
        "session_id": session_id,
        "scenario_name": str(scenario_name or ""),
        "registered_at": now,
        "t0": t0_str,
        "last_execute_at": last_execute_at_str,
        "item_count": str(int(item_count)),
        "source": str(source or ""),
        "time_format": config.TIME_FMT,
    }

    xid = config.r.xadd(
        config.KEY_EXPERIMENT_REGISTRY,
        fields,
        maxlen=200,
        approximate=True,
    )

    return {
        "stream_id": xid,
        **fields,
    }


def _enqueue_script_or_csv_item(
    *,
    scanner: str,
    category: str,
    action: str,
    execute_at: str,
    args: Dict[str, Any],
) -> None:
    """
    Dispatch one scheduled row to the correct internal queue.

    - traffic rows  -> NMS traffic command stream
    - mobility rows -> NMS mobility scheduled-command stream
    - everything else -> existing robot/AP command stream
    """
    category_n = (category or "").strip().lower()
    action_n = (action or "").strip()

    if category_n == "traffic" or action_n in ("traffic.session.start", "traffic.session.stop"):
        m7Traffic._traffic_enqueue_core(
            scanner=scanner,
            action=action_n,
            execute_at=execute_at,
            args_json=json.dumps(args or {}, ensure_ascii=False),
        )
        return

    if category_n == "mobility":
        _mobility_enqueue_core(
            scanner=scanner,
            action=action_n,
            execute_at=execute_at,
            args=args or {},
            source="script_or_csv",
        )
        return

    if category_n == "ap_meta" or action_n == "ap.location.update":
        _handle_ap_meta_row(
            scanner=scanner,
            action=action_n,
            args=args or {},
        )
        return
    
    config.r.xadd(
        config.key_cmd_stream(scanner),
        {
            "category": category,
            "action": action,
            "execute_at": execute_at,
            "created_at": utility.local_ts(),
            "args_json": json.dumps(args or {}, ensure_ascii=False),
        },
        maxlen=5000,
        approximate=True,
    )



def _mobility_enqueue_core(
    *,
    scanner: str,
    action: str,
    execute_at: str,
    args: Dict[str, Any],
    source: str,
) -> Dict[str, Any]:
    m1Registry.require_whitelisted(scanner)

    created_at = utility.local_ts()

    xid = config.r.xadd(
        config.KEY_MOBILITY_CMD_STREAM,
        {
            "scanner": scanner,
            "action": action,
            "execute_at": execute_at,
            "created_at": created_at,
            "args_json": json.dumps(args or {}, ensure_ascii=False),
            "source": source,
        },
        maxlen=20000,
        approximate=True,
    )

    return {
        "status": "ok",
        "scanner": scanner,
        "action": action,
        "cmd_id": xid,
        "created_at": created_at,
        "execute_at": execute_at,
        "time_format": config.TIME_FMT,
    }


def _handle_ap_meta_row(
    *,
    scanner: str,
    action: str,
    args: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Handle NMS-only AP metadata rows from experiment CSV.

    Current supported action:
    - ap.location.update

    This does NOT enqueue commands to AP.
    """
    m1Registry.require_whitelisted(scanner)

    action_n = (action or "").strip()
    if action_n != "ap.location.update":
        raise HTTPException(status_code=400, detail=f"unsupported ap_meta action: {action_n}")

    try:
        x_m = float(args.get("x_m"))
        y_m = float(args.get("y_m"))
    except Exception:
        raise HTTPException(status_code=400, detail="ap.location.update requires numeric x_m and y_m")

    z_raw = args.get("z_m", None)
    z_m = None
    if z_raw is not None and str(z_raw).strip() != "":
        try:
            z_m = float(z_raw)
        except Exception:
            raise HTTPException(status_code=400, detail="z_m must be numeric if provided")

    alias = str(args.get("alias") or "").strip()

    now = utility.local_ts()
    mapping = {
        "device_type": "ap",
        "location_mode": "fixed",
        "location_x_m": str(x_m),
        "location_y_m": str(y_m),
        "location_updated_at": now,
        "location_source": "experiment_csv",
    }

    if z_m is not None:
        mapping["location_z_m"] = str(z_m)

    if alias:
        mapping["ap_alias"] = alias

    config.r.hset(config.key_scanner_meta(scanner), mapping=mapping)
    config.r.sadd(config.KEY_REGISTRY, scanner)

    return {
        "status": "ok",
        "scanner": scanner,
        "action": action_n,
        "alias": alias,
        "location": {
            "mode": "fixed",
            "x_m": x_m,
            "y_m": y_m,
            "z_m": z_m,
        },
        "updated_at": now,
    }


@router.post("/cmd/_enqueue/{scanner}", tags=["4 Commands (Polling)"])
def cmd_enqueue(scanner: str, cmd: Cmd) -> Dict[str, Any]:
    return _cmd_enqueue_core(scanner, cmd)


def _collect_due_commands(scanner: str, limit: int, server_now_str: str) -> Dict[str, Any]:
    server_now = utility.parse_local_dt(server_now_str)
    raw = config.r.xrange(config.key_cmd_stream(scanner), count=5000)

    due_cmds = []
    skipped_not_due = 0
    skipped_expired = 0
    skipped_bad_time = 0

    for xid, fields in raw:
        exec_at_s = fields.get("execute_at", "")
        if not exec_at_s:
            skipped_bad_time += 1
            continue

        try:
            exec_at = utility.parse_local_dt(exec_at_s)
        except Exception:
            skipped_bad_time += 1
            continue

        if exec_at > server_now:
            skipped_not_due += 1
            continue

        age_sec = int((server_now - exec_at).total_seconds())
        if age_sec > config.CMD_EXPIRE_SEC:
            skipped_expired += 1
            continue

        f2 = dict(fields)
        f2["cmd_id"] = xid

        try:
            f2["execute_at"] = utility.parse_local_dt(f2.get("execute_at", "")).strftime(config.TIME_FMT)
        except Exception:
            pass

        try:
            f2["created_at"] = utility.parse_local_dt(f2.get("created_at", "")).strftime(config.TIME_FMT)
        except Exception:
            pass

        due_cmds.append((xid, f2))
        if len(due_cmds) >= limit:
            break

    return {
        "commands": due_cmds,
        "skipped": {
            "not_due": skipped_not_due,
            "expired": skipped_expired,
            "bad_time": skipped_bad_time,
        },
    }


@router.post("/cmd/poll/{scanner}", tags=["4 Commands (Polling)"])
def cmd_poll(
    scanner: str,
    req: CmdPollReq,
    request: Request,
) -> Dict[str, Any]:
    m1Registry.require_whitelisted(scanner)

    server_now_str = utility.local_ts()

    try:
        limit = int(req.limit or 20)
    except Exception:
        limit = 20
    limit = max(1, min(limit, 50))

    try:
        meta_updates: Dict[str, str] = {
            "device_type": "robot",
            "last_seen": server_now_str,
            "last_poll": server_now_str,
        }

        if request.client and request.client.host:
            meta_updates["last_poll_ip"] = request.client.host

        if req.av_streaming is not None:
            meta_updates["av_streaming"] = "1" if int(req.av_streaming) == 1 else "0"
            meta_updates["av_updated_at"] = server_now_str

        if req.av_detail is not None:
            meta_updates["av_detail"] = (req.av_detail or "")[:200]

        if req.boot_id is not None:
            meta_updates["boot_id"] = (req.boot_id or "")[:80]

        status_report = req.status_report or {}
        if isinstance(status_report, dict) and status_report:
            wifi_status = status_report.get("wifi_status") or {}
            scan_status = status_report.get("scan_status") or {}
            voice_status = status_report.get("voice_status") or {}

            if isinstance(wifi_status, dict):
                meta_updates["wifi_status_json"] = json.dumps(wifi_status, ensure_ascii=False)

            if isinstance(scan_status, dict):
                meta_updates["scan_status_json"] = json.dumps(scan_status, ensure_ascii=False)

            if isinstance(voice_status, dict):
                meta_updates["voice_status_json"] = json.dumps(voice_status, ensure_ascii=False)

            meta_updates["last_status_report"] = server_now_str

        mobility_report_ok = False
        if isinstance(req.mobility_report, dict) and req.mobility_report:
            try:
                config.r.hset(
                    key_report(scanner),
                    mapping={
                        "last_mobility_report_json": json.dumps(req.mobility_report, ensure_ascii=False),
                    },
                )
                config.r.hset(
                    key_time(scanner),
                    mapping={
                        "last_mobility_report_at": server_now_str,
                    },
                )
                mobility_report_ok = True
            except Exception:
                mobility_report_ok = False

        config.r.hset(config.key_scanner_meta(scanner), mapping=meta_updates)
        config.r.sadd(config.KEY_REGISTRY, scanner)

        if req.av_streaming is not None:
            applied = "on" if int(req.av_streaming) == 1 else "off"
            config.r.hset(config.KEY_APPLIED_VIDEO, scanner, applied)
            config.r.hset(config.KEY_APPLIED_VIDEO_TS, scanner, server_now_str)

        if mobility_report_ok:
            try:
                m8mobility.on_report_received(scanner)
            except Exception:
                pass

    except Exception:
        pass

    collected = _collect_due_commands(scanner=scanner, limit=limit, server_now_str=server_now_str)

    return {
        "scanner": scanner,
        "server_now": server_now_str,
        "cmd_expire_sec": config.CMD_EXPIRE_SEC,
        "time_format": config.TIME_FMT,
        "returned": len(collected["commands"]),
        "skipped": collected["skipped"],
        "commands": collected["commands"],
    }


@router.post("/cmd/ack/{scanner}", tags=["4 Commands (Polling)"])
def cmd_ack(scanner: str, ack: CmdAck) -> Dict[str, Any]:
    m1Registry.require_whitelisted(scanner)

    if ack.finished_at:
        try:
            finished_at = utility.parse_local_dt(ack.finished_at).strftime(config.TIME_FMT)
        except Exception:
            raise HTTPException(status_code=400, detail=f"finished_at must be like '{utility.local_ts()}' (format {config.TIME_FMT})")
    else:
        finished_at = utility.local_ts()

    config.r.xadd(
        config.key_cmdack_stream(scanner),
        {
            "cmd_id": ack.cmd_id,
            "status": ack.status,
            "finished_at": finished_at,
            "detail": ack.detail or "",
        },
        maxlen=20000,
        approximate=True,
    )

    deleted = int(config.r.xdel(config.key_cmd_stream(scanner), ack.cmd_id))

    return {
        "status": "ok",
        "scanner": scanner,
        "cmd_id": ack.cmd_id,
        "deleted": deleted,
        "finished_at": finished_at,
        "time_format": config.TIME_FMT,
    }


@router.post("/cmd/_load_script", tags=["4 Commands (Polling)"])
def cmd_load_script(script: ScriptLoad) -> Dict[str, Any]:
    try:
        t0_dt = utility.parse_local_dt(script.t0)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid t0; expected like '{utility.local_ts()}' (format {config.TIME_FMT})")

    added = 0
    skipped_not_whitelisted = 0

    for it in script.items:
        if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, (it.scanner or "").strip()):
            skipped_not_whitelisted += 1
            continue

        execute_at = (t0_dt + timedelta(seconds=int(it.t_offset_sec))).strftime(config.TIME_FMT)

        _enqueue_script_or_csv_item(
            scanner=it.scanner,
            category=it.category,
            action=it.action,
            execute_at=execute_at,
            args=it.args or {},
        )
        added += 1

    last_execute_at = t0_dt
    for it in script.items:
        try:
            cand = t0_dt + timedelta(seconds=int(it.t_offset_sec))
            if cand > last_execute_at:
                last_execute_at = cand
        except Exception:
            pass

    exp = _register_experiment_status(
        t0_str=t0_dt.strftime(config.TIME_FMT),
        last_execute_at_str=last_execute_at.strftime(config.TIME_FMT),
        item_count=added,
        source="load_script",
        scenario_name="",
    )

    return {
        "status": "ok",
        "added": added,
        "skipped_not_whitelisted": skipped_not_whitelisted,
        "t0": t0_dt.strftime(config.TIME_FMT),
        "time_format": config.TIME_FMT,
        "experiment": exp,
    }


@router.post("/cmd/_load_csv", tags=["4 Commands (Polling)"])
def cmd_load_csv(req: CmdLoadCSVReq) -> Dict[str, Any]:
    try:
        t0_dt = utility.parse_local_dt(req.t0)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid t0; expected like '{utility.local_ts()}' (format {config.TIME_FMT})")

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

        try:
            offset = int((row.get("t_offset_sec") or "0").strip())
        except Exception:
            bad_rows += 1
            continue

        category = (row.get("category") or "scan").strip() or "scan"
        action = (row.get("action") or "").strip()
        if not action:
            bad_rows += 1
            continue

        args_s = (row.get("args_json") or "").strip()
        if args_s:
            try:
                args = json.loads(args_s)
            except Exception:
                bad_rows += 1
                continue
        else:
            args = {}

        execute_at = (t0_dt + timedelta(seconds=offset)).strftime(config.TIME_FMT)

        _enqueue_script_or_csv_item(
            scanner=scanner,
            category=category,
            action=action,
            execute_at=execute_at,
            args=args,
        )
        added += 1

    last_execute_at = t0_dt
    f2 = io.StringIO(req.csv_text)
    reader2 = csv.DictReader(f2)

    for row in reader2:
        try:
            offset = int((row.get("t_offset_sec") or "0").strip())
            cand = t0_dt + timedelta(seconds=offset)
            if cand > last_execute_at:
                last_execute_at = cand
        except Exception:
            continue

    exp = _register_experiment_status(
        t0_str=t0_dt.strftime(config.TIME_FMT),
        last_execute_at_str=last_execute_at.strftime(config.TIME_FMT),
        item_count=added,
        source="load_csv",
        scenario_name="",
    )

    return {
        "status": "ok",
        "t0": t0_dt.strftime(config.TIME_FMT),
        "time_format": config.TIME_FMT,
        "added": added,
        "skipped_not_whitelisted": skipped_not_whitelisted,
        "bad_rows": bad_rows,
        "experiment": exp,
    }

@router.post("/cmd/_load_csv_file", tags=["4 Commands (Polling)"])
async def cmd_load_csv_file(
    t0: str = Form(..., description=f"Absolute local time, format: {config.TIME_FMT}"),
    csv_file: UploadFile = File(..., description="Upload a CSV file with columns: scanner,t_offset_sec,category,action,args_json"),
) -> Dict[str, Any]:
    try:
        t0_dt = utility.parse_local_dt(t0)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid t0; expected like '{utility.local_ts()}' (format {config.TIME_FMT})"
        )

    filename = (csv_file.filename or "").lower()
    if not filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Uploaded file must be a .csv file")

    try:
        raw_bytes = await csv_file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read uploaded CSV file: {e}")

    # Try UTF-8 first, then UTF-8 with BOM, then a common Windows fallback.
    text = None
    for enc in ("utf-8", "utf-8-sig", "cp950"):
        try:
            text = raw_bytes.decode(enc)
            break
        except Exception:
            continue

    if text is None:
        raise HTTPException(status_code=400, detail="Unable to decode CSV file as text")

    f = io.StringIO(text)
    reader = csv.DictReader(f)
    required_cols = {"scanner", "t_offset_sec", "category", "action", "args_json"}
    if not required_cols.issubset(set(reader.fieldnames or [])):
        raise HTTPException(
            status_code=400,
            detail=f"CSV must have columns: {sorted(list(required_cols))}"
        )

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

        try:
            offset = int((row.get("t_offset_sec") or "0").strip())
        except Exception:
            bad_rows += 1
            continue

        category = (row.get("category") or "scan").strip() or "scan"
        action = (row.get("action") or "").strip()
        if not action:
            bad_rows += 1
            continue

        args_s = (row.get("args_json") or "").strip()
        if args_s:
            try:
                args = json.loads(args_s)
            except Exception:
                bad_rows += 1
                continue
        else:
            args = {}

        execute_at = (t0_dt + timedelta(seconds=offset)).strftime(config.TIME_FMT)

        _enqueue_script_or_csv_item(
            scanner=scanner,
            category=category,
            action=action,
            execute_at=execute_at,
            args=args,
        )
        added += 1

    f2 = io.StringIO(text)
    reader2 = csv.DictReader(f2)

    last_execute_at = t0_dt
    for row in reader2:
        try:
            offset = int((row.get("t_offset_sec") or "0").strip())
            cand = t0_dt + timedelta(seconds=offset)
            if cand > last_execute_at:
                last_execute_at = cand
        except Exception:
            continue

    exp = _register_experiment_status(
        t0_str=t0_dt.strftime(config.TIME_FMT),
        last_execute_at_str=last_execute_at.strftime(config.TIME_FMT),
        item_count=added,
        source="load_csv_file",
        scenario_name=csv_file.filename or "",
    )

    return {
        "status": "ok",
        "t0": t0_dt.strftime(config.TIME_FMT),
        "time_format": config.TIME_FMT,
        "filename": csv_file.filename or "",
        "added": added,
        "skipped_not_whitelisted": skipped_not_whitelisted,
        "bad_rows": bad_rows,
        "experiment": exp,
    }
