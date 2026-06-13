from typing import Optional, List, Dict, Any
from datetime import timedelta
import json
import csv
import io
import copy

from fastapi import APIRouter, HTTPException, Query, Request, UploadFile, File, Form
from pydantic import BaseModel, Field

import config
import utility
import m1Registry
import m7Traffic
import m8mobility
from m8mobility_state_store import key_report, key_time, key_state, key_pose, _save_stop

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


# ============================================================
# Mobility report debug interception outlet
# ============================================================
def _mobility_report_intercept_key(scanner: str) -> str:
    return f"{config.MOBILITY_REPORT_INTERCEPT_KEY_PREFIX}{scanner}"


def _mobility_report_intercept_enable_key(scanner: str = "") -> str:
    base = getattr(
        config,
        "MOBILITY_REPORT_INTERCEPT_ENABLE_KEY",
        f"{config.KEY_PREFIX}debug:mobility_report_intercept:enabled",
    )
    return f"{base}:{scanner}" if scanner else base


def _redis_truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bytes):
        v = v.decode("utf-8", errors="replace")
    return str(v).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _mobility_report_intercept_enabled(scanner: str) -> bool:
    """
    Runtime gate for mobility report interception.

    Long-term usage:
    - config.MOBILITY_REPORT_INTERCEPT_AVAILABLE is a production kill switch.
    - testSM scripts enable the outlet by Redis key.
    - No config.py edit or NMS restart is needed for each test.

    Global enable key:
        nms:debug:mobility_report_intercept:enabled

    Scanner-specific enable key:
        nms:debug:mobility_report_intercept:enabled:<scanner>
    """
    try:
        available = bool(getattr(config, "MOBILITY_REPORT_INTERCEPT_AVAILABLE", False))
        if not available:
            return False

        return (
            _redis_truthy(config.r.get(_mobility_report_intercept_enable_key()))
            or _redis_truthy(config.r.get(_mobility_report_intercept_enable_key(scanner)))
        )
    except Exception:
        return False


def _deep_merge_dict(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursive dict merge used by intercept mode='patch'.

    This lets a test change only selected fields while preserving the real report.
    Example:
        patch last_exec_status, last_error_code, or nested last_location_result.ok
    """
    out = copy.deepcopy(base)

    for k, v in (patch or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge_dict(out[k], v)
        else:
            out[k] = copy.deepcopy(v)

    return out


def _log_mobility_report_intercept(
    scanner: str,
    *,
    phase: str,
    detail: str,
    original_report: Optional[Dict[str, Any]] = None,
    final_report: Optional[Dict[str, Any]] = None,
    rule_key: str = "",
) -> None:
    """
    Best-effort debug event stream.

    Inspect with:
        memurai-cli XREVRANGE nms:debug:mobility_report_intercept:events + - COUNT 20
    """
    try:
        payload = {
            "ts": utility.local_ts(),
            "scanner": scanner,
            "phase": phase,
            "detail": detail,
            "rule_key": rule_key,
            "original_action": "" if original_report is None else str(original_report.get("last_command") or ""),
            "original_status": "" if original_report is None else str(original_report.get("last_exec_status") or ""),
            "final_action": "" if final_report is None else str(final_report.get("last_command") or ""),
            "final_status": "" if final_report is None else str(final_report.get("last_exec_status") or ""),
        }

        config.r.xadd(
            config.MOBILITY_REPORT_INTERCEPT_EVENT_STREAM,
            {"json": json.dumps(payload, ensure_ascii=False)},
            maxlen=2000,
            approximate=True,
        )
    except Exception:
        pass


def _apply_mobility_report_intercept(
    scanner: str,
    report: Dict[str, Any],
) -> tuple[Optional[Dict[str, Any]], bool, str]:
    """
    Debug-only mobility report interception.

    This is called inside cmd_poll() before the report is written to Redis and
    before m8mobility.on_report_received(scanner) is called.

    Return:
        (report, False, detail)
            No interception applied. Continue normal path.

        (mutated_report, True, detail)
            Store mutated report, update last_mobility_report_at, call on_report_received().

        (None, True, detail)
            Drop report. Do NOT store report, do NOT update last_mobility_report_at,
            do NOT call on_report_received(). This simulates missing report / S1 timeout.

    Redis enable:
        nms:debug:mobility_report_intercept:enabled:<scanner> = true

    Redis rule key:
        nms:debug:mobility_report_intercept:<scanner>

    Rule JSON examples:

    1) Drop next matching report:
        {
          "mode": "drop",
          "match_action": "mobility.turn_move_turn.forward",
          "once": true
        }

    2) Patch real report:
        {
          "mode": "patch",
          "match_action": "mobility.turn_move_turn.forward",
          "once": true,
          "patch": {
            "last_exec_status": "failed",
            "last_error_code": "MOBILITY_BUSY",
            "last_error_detail": "debug injected robot busy"
          }
        }

    3) Replace whole report:
        {
          "mode": "replace",
          "match_action": "mobility.turn_move_turn.forward",
          "once": true,
          "replacement": {
            "last_command": "mobility.report.location",
            "last_exec_status": "completed",
            "last_error_code": "",
            "last_error_detail": "debug injected wrong action",
            "last_location_result": {"ok": true}
          }
        }

    match_action:
        Optional. If present, only apply when incoming report.last_command matches.

    once:
        Usually true. Deletes the rule after the first matched report.
    """
    if not _mobility_report_intercept_enabled(scanner):
        return report, False, "intercept_disabled"

    key = _mobility_report_intercept_key(scanner)
    raw = config.r.get(key)

    if not raw:
        return report, False, "no_intercept_rule"

    try:
        rule = json.loads(raw)
        if not isinstance(rule, dict):
            return report, False, "intercept_rule_not_dict"
    except Exception as e:
        _log_mobility_report_intercept(
            scanner,
            phase="bad_rule",
            detail=f"bad_intercept_json: {type(e).__name__}: {e}",
            original_report=report,
            final_report=report,
            rule_key=key,
        )
        return report, False, f"bad_intercept_json: {type(e).__name__}: {e}"

    mode = str(rule.get("mode", "pass") or "pass").strip().lower()
    match_action = str(rule.get("match_action", "") or "").strip()
    report_action = str((report or {}).get("last_command") or "").strip()

    if match_action and report_action != match_action:
        _log_mobility_report_intercept(
            scanner,
            phase="not_matched",
            detail=f"action={report_action} expected={match_action}",
            original_report=report,
            final_report=report,
            rule_key=key,
        )
        return report, False, f"intercept_not_matched action={report_action} expected={match_action}"

    if bool(rule.get("once", True)):
        try:
            config.r.delete(key)
        except Exception:
            pass

    if mode in {"pass", "passthrough", "pass-through"}:
        _log_mobility_report_intercept(
            scanner,
            phase="pass",
            detail="intercept_pass",
            original_report=report,
            final_report=report,
            rule_key=key,
        )
        return report, True, "intercept_pass"

    if mode == "drop":
        _log_mobility_report_intercept(
            scanner,
            phase="drop",
            detail="intercept_drop",
            original_report=report,
            final_report=None,
            rule_key=key,
        )
        return None, True, "intercept_drop"

    if mode == "replace":
        replacement = rule.get("replacement")
        if not isinstance(replacement, dict):
            _log_mobility_report_intercept(
                scanner,
                phase="replace_error",
                detail="intercept_replace_missing_replacement",
                original_report=report,
                final_report=report,
                rule_key=key,
            )
            return report, True, "intercept_replace_missing_replacement"

        final_report = copy.deepcopy(replacement)
        _log_mobility_report_intercept(
            scanner,
            phase="replace",
            detail="intercept_replace",
            original_report=report,
            final_report=final_report,
            rule_key=key,
        )
        return final_report, True, "intercept_replace"

    if mode == "patch":
        patch = rule.get("patch")
        if not isinstance(patch, dict):
            _log_mobility_report_intercept(
                scanner,
                phase="patch_error",
                detail="intercept_patch_missing_patch",
                original_report=report,
                final_report=report,
                rule_key=key,
            )
            return report, True, "intercept_patch_missing_patch"

        final_report = _deep_merge_dict(report, patch)
        _log_mobility_report_intercept(
            scanner,
            phase="patch",
            detail="intercept_patch",
            original_report=report,
            final_report=final_report,
            rule_key=key,
        )
        return final_report, True, "intercept_patch"

    _log_mobility_report_intercept(
        scanner,
        phase="unknown_mode",
        detail=f"intercept_unknown_mode={mode}",
        original_report=report,
        final_report=report,
        rule_key=key,
    )
    return report, True, f"intercept_unknown_mode={mode}"


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

    if category_n == "traffic" or action_n in ("traffic.session.start", "traffic.session.stop"):
        return m7Traffic._traffic_enqueue_core(
            scanner=scanner,
            action=action_n,
            execute_at=execute_at_norm,
            args_json=args_json,
        )

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



def _is_mobility_csv_row(row: Dict[str, Any]) -> bool:
    return (row.get("category") or "").strip().lower() == "mobility"


def _prepare_mobility_script_run_from_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Production preflight for script/CSV mobility runs.

    Invariants before accepting a new mobility script:
    - old global stop latch is cleared
    - old scheduled mobility rows are cleared
    - old per-robot command streams are cleared for scanners in this script
    - per-robot mobility state is reset to S0/NORMAL
    - stale planned pose and last-issued command mirrors are cleared
    - first mobility row for each scanner must be mobility.report.location

    Why first-row location is required:
    mobility.report.location is now a precondition command.  Its report refreshes
    true_location_json and S3 aligns planned_location_json=true_location_json.
    Script movement commands then start from a well-defined pose.
    """
    scanner_first: Dict[str, Dict[str, Any]] = {}
    mobility_scanners = set()

    for row in rows:
        scanner = (row.get("scanner") or "").strip()
        if not scanner:
            continue
        if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, scanner):
            continue
        if not _is_mobility_csv_row(row):
            continue

        action = (row.get("action") or "").strip()
        try:
            offset = int((row.get("t_offset_sec") or "0").strip())
        except Exception:
            offset = 0

        mobility_scanners.add(scanner)
        old = scanner_first.get(scanner)
        if old is None or offset < int(old.get("offset", 0)):
            scanner_first[scanner] = {"offset": offset, "action": action}

    if not mobility_scanners:
        return {
            "mobility_preflight": "skipped",
            "mobility_scanners": [],
            "detail": "no mobility rows",
        }

    bad_first = {
        scanner: info
        for scanner, info in scanner_first.items()
        if info.get("action") != "mobility.report.location"
    }
    if bad_first:
        raise HTTPException(
            status_code=400,
            detail=(
                "Mobility CSV script must start each scanner with "
                "mobility.report.location at the earliest mobility offset. "
                f"Bad first rows: {bad_first}"
            ),
        )

    _save_stop(False, "")

    old_mobility_len = 0
    try:
        old_mobility_len = int(config.r.xlen(config.KEY_MOBILITY_CMD_STREAM))
    except Exception:
        old_mobility_len = -1
    config.r.delete(config.KEY_MOBILITY_CMD_STREAM)

    reset_scanners = []
    for scanner in sorted(mobility_scanners):
        try:
            config.r.delete(config.key_cmd_stream(scanner))
        except Exception:
            pass

        utility._hset_many(
            key_state(scanner),
            {
                "state": "s0idle",
                "state_detail": "script-run preflight: reset before CSV",
                "mobility_ready": "true",
                "mobility_ready_reason": "script-run preflight",
                "robot_safety_state": "NORMAL",
                "stop_experiment": "false",
                "stop_reason": "",
                "need_location_retry": "false",
                "retry_count": "0",
                "busy_count": "0",
                "collision_veto_count": "0",
                "exec_fail_count": "0",
                "correction_attempt_count": "-1",
                "outgoing_command_action": "",
                "outgoing_command_args_json": "",
                "outgoing_command_source": "",
                "outgoing_command_updated_at": "",
                "pending_sequence_json": "",
                "pending_sequence_len": "0",
                "pending_sequence_reason": "",
                "last_error_code": "",
                "last_error_detail": "",
                "s2_entry_reason": "",
                "true_propagation_applied": "",
                "true_propagation_detail": "",
                "true_propagation_time": "",
            },
        )

        utility._hset_many(
            key_time(scanner),
            {
                "policy_updated_at": utility.local_ts(),
                "s1_timer_token": "",
                "s1_timer_started_at": "",
                "busy_retry_token": "",
                "busy_retry_started_at": "",
                "last_planned_command_issued_at": "",
                "planned_location_updated_at": "",
            },
        )

        try:
            config.r.hdel(
                key_pose(scanner),
                "planned_location_json",
                "last_planned_command_action",
                "last_planned_command_args_json",
                "last_planned_command_source",
                "last_planned_command_updated_at",
            )
        except Exception:
            pass

        reset_scanners.append(scanner)

    return {
        "mobility_preflight": "ok",
        "mobility_scanners": reset_scanners,
        "old_mobility_cmd_stream_len": old_mobility_len,
        "detail": "cleared stop latch, old mobility schedule, robot command streams, stale planned pose",
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
    location_obj = {
        "mode": "fixed",
        "x": x_m,
        "y": y_m,
        "z": z_m,
        "source": "experiment_csv",
        "updated_at": now,
    }

    mapping = {
        "device_type": "ap",
        "location_json": json.dumps(location_obj, ensure_ascii=False),
    }

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
    mobility_report_process_result = {}
    
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
            original_report = req.mobility_report

            final_report, _intercepted, _intercept_detail = _apply_mobility_report_intercept(
                scanner,
                original_report,
            )

            # final_report=None means the debug outlet intentionally dropped the report.
            # Important:
            # - do not save last_mobility_report_json
            # - do not update last_mobility_report_at
            # - do not call m8mobility.on_report_received()
            if final_report is not None:
                try:
                    config.r.hset(
                        key_report(scanner),
                        mapping={
                            "last_mobility_report_json": json.dumps(final_report, ensure_ascii=False),
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

        mobility_report_process_result = {}

        if mobility_report_ok:
            try:
                mobility_report_process_result = m8mobility.on_report_received(scanner)
            except Exception as e:
                mobility_report_process_result = {
                    "status": "error",
                    "scanner": scanner,
                    "detail": f"on_report_received exception: {type(e).__name__}: {e}",
                }

            try:
                utility._hset_many(
                    key_state(scanner),
                    {
                        "last_mobility_report_process_result_json": mobility_report_process_result,
                        "last_mobility_report_process_at": server_now_str,
                    },
                )
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
        "mobility_report_process_result": mobility_report_process_result,
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

    rows = list(reader)
    preflight = _prepare_mobility_script_run_from_rows(rows)

    added = 0
    skipped_not_whitelisted = 0
    bad_rows = 0

    for row in rows:
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
        "preflight": preflight,
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

    rows = list(reader)
    preflight = _prepare_mobility_script_run_from_rows(rows)

    added = 0
    skipped_not_whitelisted = 0
    bad_rows = 0

    for row in rows:
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
        "preflight": preflight,
        "experiment": exp,
    }
