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



def _cmd_redis_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v)


def _experiment_runtime_status() -> Dict[str, Any]:
    """
    Decide whether manual mobility commands should be blocked.

    Policy:
    - /cmd/_enqueue is the manual/direct command API.
    - Manual mobility commands are state-free when no experiment is currently
      running.
    - During the experiment runtime window, manual mobility commands are blocked
      so they cannot interfere with the script/state-machine path.

    Runtime window:
        start_at <= now <= end_at + NORTHBOUND_EXPERIMENT_WRAPUP_SEC

    Notes:
    - A future registered experiment does not block manual testing yet.
    - A past experiment does not block manual testing after its wrap-up window.
    - Cancelled/deleted/completed registry rows are ignored.
    """
    try:
        rows = config.r.xrange(config.KEY_EXPERIMENT_REGISTRY, min="-", max="+")
    except Exception as e:
        return {
            "active": True,
            "reason": f"unable to inspect experiment registry: {type(e).__name__}: {e}",
            "fail_closed": True,
        }

    if not rows:
        return {
            "active": False,
            "reason": "no registered experiment",
            "registered_count": 0,
        }

    now_dt = utility.parse_local_dt(utility.local_ts())
    wrapup_sec = int(getattr(config, "NORTHBOUND_EXPERIMENT_WRAPUP_SEC", 120) or 0)

    checked = 0
    future_count = 0
    past_count = 0

    for stream_id, raw_fields in rows:
        fields = {
            _cmd_redis_text(k): _cmd_redis_text(v)
            for k, v in dict(raw_fields).items()
        }

        state = fields.get("state", "").strip().lower()
        if state in {"cancelled", "canceled", "deleted", "completed"}:
            continue

        start_s = fields.get("start_at", "")
        end_s = fields.get("end_at", "")
        if not start_s or not end_s:
            # Registry exists but does not carry a usable runtime window.
            # Fail closed during experiment-mode uncertainty, because allowing
            # manual direct motion could interfere with queued script commands.
            return {
                "active": True,
                "reason": "experiment registry row missing start_at/end_at",
                "fail_closed": True,
                "stream_id": _cmd_redis_text(stream_id),
                "experiment_id": fields.get("experiment_id", ""),
            }

        try:
            start_dt = utility.parse_local_dt(start_s)
            end_dt = utility.parse_local_dt(end_s) + timedelta(seconds=wrapup_sec)
        except Exception as e:
            return {
                "active": True,
                "reason": f"experiment registry time parse failed: {type(e).__name__}: {e}",
                "fail_closed": True,
                "stream_id": _cmd_redis_text(stream_id),
                "experiment_id": fields.get("experiment_id", ""),
            }

        checked += 1

        if now_dt < start_dt:
            future_count += 1
            continue

        if now_dt > end_dt:
            past_count += 1
            continue

        return {
            "active": True,
            "reason": "experiment runtime window active",
            "fail_closed": False,
            "stream_id": _cmd_redis_text(stream_id),
            "experiment_id": fields.get("experiment_id", ""),
            "session_id": fields.get("session_id", ""),
            "lab_id": fields.get("lab_id", ""),
            "start_at": start_s,
            "end_at": fields.get("end_at", ""),
            "wrapup_sec": wrapup_sec,
            "state": state,
        }

    return {
        "active": False,
        "reason": "no experiment is currently inside its runtime window",
        "registered_count": len(rows),
        "checked_runtime_rows": checked,
        "future_experiment_count": future_count,
        "past_experiment_count": past_count,
        "wrapup_sec": wrapup_sec,
    }



def _cmd_redis_truthy(v: Any) -> bool:
    if v is None:
        return False
    text = _cmd_redis_text(v).strip().lower()
    return text in {"1", "true", "yes", "on", "enabled"}


def _mobility_state_machine_test_enabled() -> bool:
    """
    Persistent runtime debug flag.

    This flag is intentionally ignored during an active experiment runtime
    window. It exists only to make interactive state-machine testing convenient
    outside experiments.
    """
    try:
        key = getattr(
            config,
            "KEY_MOBILITY_STATE_MACHINE_TEST_ENABLED",
            f"{config.KEY_PREFIX}debug:mobility_state_machine_test_enabled",
        )
        return _cmd_redis_truthy(config.r.get(key))
    except Exception:
        return False


def _enqueue_manual_direct_command(
    *,
    scanner: str,
    cmd: "Cmd",
    action: str,
    execute_at: str,
    created_at: str,
    args_json: str,
    route_detail: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Original/manual direct robot polling queue path.

    This deliberately bypasses the mobility state machine. It is used when no
    experiment is currently running, so operators can test robot mobility
    commands without S0/S1/S5 state dependencies.
    """
    fields = {
        "category": cmd.category,
        "action": action,
        "execute_at": execute_at,
        "created_at": created_at,
        "args_json": args_json,
        "route": "manual_direct_no_active_experiment",
    }

    xid = config.r.xadd(
        config.key_cmd_stream(scanner),
        fields,
        maxlen=5000,
        approximate=True,
    )

    return {
        "status": "ok",
        "scanner": scanner,
        "cmd_id": xid,
        "created_at": created_at,
        "execute_at": execute_at,
        "time_format": config.TIME_FMT,
        "route": "manual_direct_no_active_experiment",
        "detail": "manual mobility command enqueued directly to robot polling queue",
        "experiment_runtime_status": route_detail,
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
        experiment_status = _experiment_runtime_status()

        if experiment_status.get("active"):
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "manual_mobility_blocked_during_experiment",
                    "message": (
                        "Manual /cmd/_enqueue mobility commands are blocked while "
                        "an experiment runtime window is active. Use the "
                        "script/scheduler/state-machine path for experiment motion."
                    ),
                    "experiment_runtime_status": experiment_status,
                },
            )

        if _mobility_state_machine_test_enabled():
            if (cmd.execute_at or "").strip() not in ("", created_at):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "mobility state-machine test mode only supports immediate "
                        "manual commands"
                    ),
                )

            result = m8mobility.on_command_issued(scanner, action_n, args_obj)
            if isinstance(result, dict):
                result.setdefault("route", "manual_state_machine_test")
                result.setdefault("experiment_runtime_status", experiment_status)
                result.setdefault(
                    "state_machine_test_key",
                    getattr(
                        config,
                        "KEY_MOBILITY_STATE_MACHINE_TEST_ENABLED",
                        f"{config.KEY_PREFIX}debug:mobility_state_machine_test_enabled",
                    ),
                )
            return result

        return _enqueue_manual_direct_command(
            scanner=scanner,
            cmd=cmd,
            action=action_n,
            execute_at=execute_at_norm,
            created_at=created_at,
            args_json=args_json,
            route_detail=experiment_status,
        )

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


def _redis_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v)


def _nms_lab_id() -> str:
    return str(getattr(config, "NMS_NAME", "DemoRoom") or "DemoRoom")


def _require_experiment_t0_future(t0_dt) -> Dict[str, Any]:
    """
    Experiment registration rule: t0 must be in the future.

    Offline CommonCheckers cannot validate this because script writers run it
    before the lab operator chooses an actual registration time. Therefore this
    check belongs to /cmd/_load_csv_file.
    """
    now_s = utility.local_ts()
    now_dt = utility.parse_local_dt(now_s)

    if t0_dt <= now_dt:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "experiment_t0_not_future",
                "message": (
                    "Experiment t0 must be in the future relative to NMS local time. "
                    "Use current NMS local time plus a preparation margin, such as 30-60 seconds."
                ),
                "t0": t0_dt.strftime(config.TIME_FMT),
                "server_now": now_dt.strftime(config.TIME_FMT),
                "time_format": config.TIME_FMT,
            },
        )

    return {
        "status": "ok",
        "rule": "t0_future",
        "t0": t0_dt.strftime(config.TIME_FMT),
        "server_now": now_dt.strftime(config.TIME_FMT),
        "lead_time_sec": int((t0_dt - now_dt).total_seconds()),
        "detail": "t0 is in the future relative to NMS local time",
    }


def _require_empty_experiment_registry() -> Dict[str, Any]:
    """
    Enforce the Auto-Lab rule that at most one experiment may be registered.

    A completed experiment remains in the registry until an operator explicitly
    deletes it. Therefore any existing registry row blocks new registration.
    """
    try:
        count = int(config.r.xlen(config.KEY_EXPERIMENT_REGISTRY))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to inspect experiment registry: {type(e).__name__}: {e}",
        )

    if count > 0:
        raise HTTPException(
            status_code=409,
            detail=(
                "An experiment is already registered. "
                "Delete the existing experiment before registering a new one."
            ),
        )

    return {
        "status": "ok",
        "registered_experiment_count": 0,
        "detail": "experiment registry empty; registration allowed",
    }


def _session_id_to_redis(session_id: Optional[str]) -> str:
    """Redis stream fields are strings; absent session_id is stored as empty string."""
    return (session_id or "").strip()


def _delete_stream_entries_for_experiment(
    *,
    stream_key: str,
    lab_id: str,
    experiment_id: str,
) -> int:
    """
    Delete entries from one Redis stream only when the stream row itself carries
    the same lab_id + experiment_id. This keeps replace_existing scoped.
    """
    target_lab_id = str(lab_id or "")
    target_experiment_id = str(experiment_id or "")
    delete_ids = []

    try:
        entries = config.r.xrange(stream_key, min="-", max="+")
    except Exception:
        return 0

    for xid, raw_fields in entries:
        try:
            fields = {
                _redis_text(k): _redis_text(v)
                for k, v in dict(raw_fields).items()
            }
        except Exception:
            continue

        if (
            fields.get("lab_id", "") == target_lab_id
            and fields.get("experiment_id", "") == target_experiment_id
        ):
            delete_ids.append(xid)

    if not delete_ids:
        return 0

    try:
        return int(config.r.xdel(stream_key, *delete_ids))
    except Exception:
        return 0


def _register_experiment_status(
    *,
    experiment_id: str,
    session_id: Optional[str],
    start_at_str: str,
    end_at_str: str,
    command_count: int,
    replace_existing: bool,
) -> Dict[str, Any]:
    """
    Write one coherent experiment-registration record.

    Final registry fields:
        experiment_id
        session_id
        lab_id
        registered_at
        start_at
        end_at
        guard_sec
        command_count
        replace_existing
        state
        time_format
    """
    registered_at = utility.local_ts()
    clean_session_id = (session_id or "").strip() or None

    fields = {
        "experiment_id": str(experiment_id or ""),
        # Redis streams store strings only. Use empty string internally for null.
        "session_id": "" if clean_session_id is None else clean_session_id,
        "lab_id": _nms_lab_id(),
        "registered_at": registered_at,
        "start_at": start_at_str,
        "end_at": end_at_str,
        "guard_sec": "3600",
        "command_count": str(int(command_count)),
        "replace_existing": "true" if replace_existing else "false",
        "state": "registered",
        "time_format": config.TIME_FMT,
    }

    xid = config.r.xadd(
        config.KEY_EXPERIMENT_REGISTRY,
        fields,
        maxlen=200,
        approximate=True,
    )

    result = {
        **fields,
    }
    # API-facing contract uses JSON null when the caller omitted session_id.
    result["session_id"] = clean_session_id
    result["replace_existing"] = bool(replace_existing)
    return result



def _is_mobility_csv_row(row: Dict[str, Any]) -> bool:
    return (row.get("category") or "").strip().lower() == "mobility"


def _validate_experiment_mobility_action(action: str) -> None:
    action_n = str(action or "").strip()

    blocked = set(getattr(config, "MOBILITY_SCRIPT_BLOCKED_ACTIONS", set()))
    if action_n in blocked:
        raise HTTPException(
            status_code=400,
            detail=(
                f"{action_n} is not allowed in experiment CSV scripts. "
                "Use mobility.move or site-level mobility macros; final robot "
                "orientation is controlled by the NMS mobility policy."
            ),
        )

    allowed = set(getattr(config, "MOBILITY_SCRIPT_ALLOWED_ACTIONS", set()))
    if allowed and action_n not in allowed:
        raise HTTPException(
            status_code=400,
            detail=(
                f"unsupported mobility action in experiment CSV: {action_n}. "
                f"Allowed mobility actions: {sorted(allowed)}"
            ),
        )


def _analyze_csv_rows_for_experiment(
    rows: List[Dict[str, Any]],
    *,
    t0_dt,
) -> Dict[str, Any]:
    """
    Dry-analyze CSV rows before any Redis mutation.

    This function is intentionally read-only except whitelist checks. It does not:
    - clear stop latches
    - clear command queues
    - reset mobility state
    - enqueue commands
    - write experiment registry records

    It validates rows, builds accepted enqueue items, computes end_at from accepted
    rows only, and enforces the mobility first-row precondition.
    """
    accepted_items: List[Dict[str, Any]] = []
    skipped_not_whitelisted = 0
    bad_rows = 0
    last_execute_at = t0_dt

    accepted_scanners = set()
    scanner_first: Dict[str, Dict[str, Any]] = {}
    mobility_scanners = set()

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

        execute_at_dt = t0_dt + timedelta(seconds=offset)
        execute_at = execute_at_dt.strftime(config.TIME_FMT)

        item = {
            "scanner": scanner,
            "category": category,
            "action": action,
            "execute_at": execute_at,
            "execute_at_dt": execute_at_dt,
            "args": args,
            "offset": offset,
        }
        accepted_items.append(item)
        accepted_scanners.add(scanner)

        if execute_at_dt > last_execute_at:
            last_execute_at = execute_at_dt

        if category.strip().lower() == "mobility":
            _validate_experiment_mobility_action(action)
            mobility_scanners.add(scanner)
            old = scanner_first.get(scanner)
            if old is None or offset < int(old.get("offset", 0)):
                scanner_first[scanner] = {"offset": offset, "action": action}

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

    if not accepted_items:
        raise HTTPException(
            status_code=400,
            detail=(
                "CSV contained no accepted commands; experiment was not registered. "
                f"bad_rows={bad_rows}, skipped_not_whitelisted={skipped_not_whitelisted}"
            ),
        )

    preflight = {
        "mobility_preflight": "ok" if mobility_scanners else "skipped",
        "scanners": sorted(accepted_scanners),
        "mobility_scanners": sorted(mobility_scanners),
        "detail": (
            "validated mobility first-row precondition; no Redis mutation performed"
            if mobility_scanners
            else "no mobility rows"
        ),
    }

    return {
        "accepted_items": accepted_items,
        "added": len(accepted_items),
        "skipped_not_whitelisted": skipped_not_whitelisted,
        "bad_rows": bad_rows,
        "last_execute_at": last_execute_at,
        "scanners": sorted(accepted_scanners),
        "mobility_scanners": sorted(mobility_scanners),
        "preflight": preflight,
    }


def _prepare_mobility_script_replacement_if_requested(
    *,
    mobility_scanners: List[str],
    replace_existing: bool,
) -> Dict[str, Any]:
    """
    Optional destructive cleanup for CSV-backed experiments.

    This must be called only after:
    - CSV dry analysis has passed
    - accepted command count is nonzero
    - same-lab time-conflict check has passed

    replace_existing=False preserves previously registered future experiments.
    replace_existing=True clears old mobility queues/state before loading this CSV.
    """
    if not mobility_scanners:
        return {
            "replacement_preflight": "skipped",
            "replace_existing": bool(replace_existing),
            "mobility_scanners": [],
            "detail": "no mobility rows",
        }

    if not replace_existing:
        return {
            "replacement_preflight": "preserved",
            "replace_existing": False,
            "mobility_scanners": list(mobility_scanners),
            "old_mobility_cmd_stream_len": None,
            "reset_scanners": [],
            "detail": "existing command queues and mobility state were preserved",
        }

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
                "need_location_recovery": "false",
                "location_recovery_context": "",
                "location_recovery_phase": "",
                "visibility_turn_count": "0",
                "last_visibility_turn_angle_deg": "",
                "last_location_recovery_action": "",
                "last_location_recovery_args_json": "",
                "last_location_recovery_detail": "",
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

        try:
            config.r.hdel(
                key_state(scanner),
                "need_location_retry",
                "retry_count",
            )
        except Exception:
            pass

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
        "replacement_preflight": "cleared",
        "replace_existing": True,
        "mobility_scanners": reset_scanners,
        "old_mobility_cmd_stream_len": old_mobility_len,
        "reset_scanners": reset_scanners,
        "detail": (
            "cleared stop latch, old mobility schedule, robot command streams, "
            "and stale planned pose because replace_existing=true"
        ),
    }


def _check_experiment_time_conflict_or_raise(
    *,
    lab_id: str,
    start_at_dt,
    end_at_dt,
    guard_sec: int = 3600,
) -> Dict[str, Any]:
    """
    Reject same-lab experiment registrations that violate the guard interval.

    Conflict rule:
        existing.start_at < new.end_at + guard_sec
        AND
        existing.end_at   > new.start_at - guard_sec

    Exactly one hour of separation is allowed because the inequalities are strict.
    replace_existing does not bypass this check.
    """
    new_guard_start = start_at_dt - timedelta(seconds=guard_sec)
    new_guard_end = end_at_dt + timedelta(seconds=guard_sec)

    try:
        entries = config.r.xrange(config.KEY_EXPERIMENT_REGISTRY, min="-", max="+")
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read experiment registry for conflict check: {e}",
        )

    checked = 0
    for stream_id, raw_fields in entries:
        fields = {
            _redis_text(k): _redis_text(v)
            for k, v in dict(raw_fields).items()
        }

        if fields.get("lab_id", "") != lab_id:
            continue

        state = fields.get("state", "").strip().lower()
        if state in {"cancelled", "canceled", "deleted"}:
            continue

        existing_start_s = fields.get("start_at", "")
        existing_end_s = fields.get("end_at", "")
        if not existing_start_s or not existing_end_s:
            # Old/deprecated registry records are ignored because they do not
            # contain the settled contract fields needed for conflict checking.
            continue

        try:
            existing_start = utility.parse_local_dt(existing_start_s)
            existing_end = utility.parse_local_dt(existing_end_s)
        except Exception:
            continue

        checked += 1
        if existing_start < new_guard_end and existing_end > new_guard_start:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "experiment_time_conflict",
                    "lab_id": lab_id,
                    "guard_sec": guard_sec,
                    "new_start_at": start_at_dt.strftime(config.TIME_FMT),
                    "new_end_at": end_at_dt.strftime(config.TIME_FMT),
                    "conflict_stream_id": _redis_text(stream_id),
                    "conflict_experiment_id": fields.get("experiment_id", ""),
                    "conflict_session_id": fields.get("session_id", ""),
                    "conflict_start_at": existing_start_s,
                    "conflict_end_at": existing_end_s,
                },
            )

    return {
        "status": "ok",
        "lab_id": lab_id,
        "guard_sec": guard_sec,
        "checked_same_lab_records": checked,
    }


def _prepare_mobility_script_run_from_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Deprecated compatibility wrapper.

    The production CSV experiment loader no longer uses this destructive helper.
    Use _analyze_csv_rows_for_experiment() before acceptance and
    _prepare_mobility_script_replacement_if_requested() only after the conflict
    check passes.
    """
    raise RuntimeError(
        "_prepare_mobility_script_run_from_rows is deprecated for experiment loading; "
        "use dry analysis + optional replacement cleanup instead"
    )


def _enqueue_script_or_csv_item(
    *,
    scanner: str,
    category: str,
    action: str,
    execute_at: str,
    args: Dict[str, Any],
    lab_id: str,
    experiment_id: str,
    session_id: Optional[str],
) -> None:
    """
    Dispatch one scheduled row to the correct internal queue.

    Every queued command carries experiment metadata:
        lab_id
        experiment_id
        session_id

    This is required so replace_existing=true can remove only old commands
    belonging to the same experiment_id, without touching other experiments.
    """
    category_n = (category or "").strip().lower()
    action_n = (action or "").strip()
    session_id_s = _session_id_to_redis(session_id)

    # Keep experiment metadata both as stream fields and inside args_json.
    # Stream fields are for Redis-side deletion/filtering.
    # args_json metadata is useful for downstream consumers/debugging.
    args2 = dict(args or {})
    args2.setdefault("_experiment", {})
    if isinstance(args2["_experiment"], dict):
        args2["_experiment"].update(
            {
                "lab_id": lab_id,
                "experiment_id": experiment_id,
                "session_id": session_id_s or None,
            }
        )

    if category_n == "traffic" or action_n in ("traffic.session.start", "traffic.session.stop"):
        # If m7Traffic._traffic_enqueue_core does not yet accept these metadata
        # fields, update that helper similarly so the resulting Redis stream row
        # carries lab_id, experiment_id, and session_id.
        m7Traffic._traffic_enqueue_core(
            scanner=scanner,
            action=action_n,
            execute_at=execute_at,
            args_json=json.dumps(args2, ensure_ascii=False),
            lab_id=lab_id,
            experiment_id=experiment_id,
            session_id=session_id_s,
        )
        return

    if category_n == "mobility":
        _mobility_enqueue_core(
            scanner=scanner,
            action=action_n,
            execute_at=execute_at,
            args=args2,
            source="script_or_csv",
            lab_id=lab_id,
            experiment_id=experiment_id,
            session_id=session_id_s,
        )
        return

    if category_n == "ap_meta" or action_n == "ap.location.update":
        _handle_ap_meta_row(
            scanner=scanner,
            action=action_n,
            args=args2,
        )
        return

    config.r.xadd(
        config.key_cmd_stream(scanner),
        {
            "category": category,
            "action": action,
            "execute_at": execute_at,
            "created_at": utility.local_ts(),
            "args_json": json.dumps(args2, ensure_ascii=False),
            "lab_id": lab_id,
            "experiment_id": experiment_id,
            "session_id": session_id_s,
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
    lab_id: str = "",
    experiment_id: str = "",
    session_id: str = "",
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
            "lab_id": str(lab_id or ""),
            "experiment_id": str(experiment_id or ""),
            "session_id": str(session_id or ""),
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


def _replace_existing_experiment_if_requested(
    *,
    replace_existing: bool,
    lab_id: str,
    experiment_id: str,
    scanners: List[str],
    mobility_scanners: List[str],
) -> Dict[str, Any]:
    """
    If replace_existing is true, delete only previous queued commands and
    registry records belonging to the same lab_id + experiment_id.

    Other experiments in the same lab must not be affected.
    """
    if not replace_existing:
        return {
            "replace_existing": False,
            "deleted_registry_entries": 0,
            "deleted_mobility_cmd_entries": 0,
            "deleted_robot_cmd_entries": {},
            "deleted_traffic_cmd_entries": 0,
            "reset_scanners": [],
            "detail": "replace_existing=false; existing commands and registry records preserved",
        }

    deleted_registry_entries = _delete_stream_entries_for_experiment(
        stream_key=config.KEY_EXPERIMENT_REGISTRY,
        lab_id=lab_id,
        experiment_id=experiment_id,
    )

    deleted_mobility_cmd_entries = _delete_stream_entries_for_experiment(
        stream_key=config.KEY_MOBILITY_CMD_STREAM,
        lab_id=lab_id,
        experiment_id=experiment_id,
    )

    deleted_traffic_cmd_entries = _delete_stream_entries_for_experiment(
        stream_key=config.KEY_TRAFFIC_CMD_STREAM,
        lab_id=lab_id,
        experiment_id=experiment_id,
    )

    deleted_robot_cmd_entries: Dict[str, int] = {}

    for scanner in sorted(set(scanners or [])):
        try:
            stream_key = config.key_cmd_stream(scanner)
        except Exception:
            continue

        deleted_robot_cmd_entries[scanner] = _delete_stream_entries_for_experiment(
            stream_key=stream_key,
            lab_id=lab_id,
            experiment_id=experiment_id,
        )

    # Mobility state is not experiment-scoped. Reset only mobility scanners
    # involved in the replacement CSV, and only after acceptance.
    _save_stop(False, "")

    reset_scanners = []
    for scanner in sorted(set(mobility_scanners or [])):
        utility._hset_many(
            key_state(scanner),
            {
                "state": "s0idle",
                "state_detail": "experiment replacement: reset before CSV",
                "mobility_ready": "true",
                "mobility_ready_reason": "experiment replacement",
                "robot_safety_state": "NORMAL",
                "stop_experiment": "false",
                "stop_reason": "",
                "need_location_recovery": "false",
                "location_recovery_context": "",
                "location_recovery_phase": "",
                "visibility_turn_count": "0",
                "last_visibility_turn_angle_deg": "",
                "last_location_recovery_action": "",
                "last_location_recovery_args_json": "",
                "last_location_recovery_detail": "",
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

        try:
            config.r.hdel(
                key_state(scanner),
                "need_location_retry",
                "retry_count",
            )
        except Exception:
            pass

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
        "replace_existing": True,
        "deleted_registry_entries": deleted_registry_entries,
        "deleted_mobility_cmd_entries": deleted_mobility_cmd_entries,
        "deleted_robot_cmd_entries": deleted_robot_cmd_entries,
        "deleted_traffic_cmd_entries": deleted_traffic_cmd_entries,
        "reset_scanners": reset_scanners,
        "detail": (
            "replaced only entries matching same lab_id + experiment_id; "
            "other experiments were preserved"
        ),
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


# @router.post("/cmd/_load_script", tags=["4 Commands (Polling)"])
# def cmd_load_script(script: ScriptLoad) -> Dict[str, Any]:
#     """
#     Deprecated. /cmd/_load_csv_file is the only supported experiment-registration
#     API. This endpoint is intentionally blocked to prevent inconsistent registry
#     records and queue behavior.
#     """
#     raise HTTPException(
#         status_code=410,
#         detail="/cmd/_load_script is deprecated. Use /cmd/_load_csv_file.",
#     )


# @router.post("/cmd/_load_csv", tags=["4 Commands (Polling)"])
# def cmd_load_csv(req: CmdLoadCSVReq) -> Dict[str, Any]:
#     """
#     Deprecated. /cmd/_load_csv_file is the only supported experiment-registration
#     API. This endpoint is intentionally blocked to prevent inconsistent registry
#     records and queue behavior.
#     """
#     raise HTTPException(
#         status_code=410,
#         detail="/cmd/_load_csv is deprecated. Use /cmd/_load_csv_file.",
#     )


@router.post("/cmd/_delete_experiment", tags=["4 Commands (Polling)"])
def cmd_delete_experiment() -> Dict[str, Any]:
    """
    Delete the currently registered experiment and clear all pending command queues.

    One-experiment lifecycle rule:
    - at most one experiment may be registered
    - completed experiment remains registered until explicitly deleted
    - deletion clears pending robot/mobility/traffic commands
    - collected results/history, scanner registry, whitelist, AP metadata,
      robot pose, and mobility runtime state are preserved
    """
    try:
        registered_count = int(config.r.xlen(config.KEY_EXPERIMENT_REGISTRY))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to inspect experiment registry: {type(e).__name__}: {e}",
        )

    try:
        deleted_registry_keys = int(config.r.delete(config.KEY_EXPERIMENT_REGISTRY))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to delete experiment registry: {type(e).__name__}: {e}",
        )

    queue_cleanup = m8mobility._clear_all_command_queues()

    return {
        "status": "ok",
        "detail": "registered experiment deleted and pending command queues cleared",
        "registered_experiment_count_before": registered_count,
        "deleted_registry_keys": deleted_registry_keys,
        "queue_cleanup": queue_cleanup,
        "results_history_preserved": True,
        "pose_preserved": True,
        "mobility_state_preserved": True,
        "time": utility.local_ts(),
    }


@router.post("/cmd/_load_csv_file", tags=["4 Commands (Polling)"])
async def cmd_load_csv_file(
    t0: str = Form(..., description=f"Absolute local time, format: {config.TIME_FMT}"),
    session_id: Optional[str] = Form(
        None,
        description="Optional session id under the experiment id. If omitted, stored as null.",
    ),
    replace_existing: bool = Form(
        False,
        description=(
            "If true, clear existing queued commands and reset mobility state after "
            "the same-lab time-conflict check passes. Default false preserves "
            "previously registered future experiments."
        ),
    ),
    csv_file: UploadFile = File(
        ...,
        description="Upload a CSV file with columns: scanner,t_offset_sec,category,action,args_json",
    ),
) -> Dict[str, Any]:
    """
    Register and load one CSV-backed experiment.

    Final settled experiment-registration contract:

    - Only this endpoint, /cmd/_load_csv_file, registers experiments.
      The older /cmd/_load_script and /cmd/_load_csv APIs are deprecated/removed
      to avoid multiple inconsistent registration paths.

    - experiment_id is derived from the uploaded CSV filename stem.
      Example:
          M2_test.csv -> experiment_id = "M2_test"

    - session_id is optional input below experiment_id.
      If omitted, session_id is stored as null in the API response and as an
      empty string in the Redis stream.
      The web server uses experiment_id, not session_id, to decide whether data
      belongs to a long-term experiment database.

    - lab_id is config.NMS_NAME.
      This identifies which NMS/lab instance registered the experiment, because
      multiple NMSs may report to the same web server.

    - registered_at is the NMS local time when this endpoint accepts the CSV.

    - start_at is the parsed t0 input.

    - end_at is derived as:
          start_at + maximum valid t_offset_sec in the accepted CSV rows

    - guard_sec is the required preparation gap between experiments in the same
      lab. Current value: 3600 seconds.

    - command_count is the number of accepted/enqueued CSV rows.
      If command_count would be zero, the endpoint rejects the request and does
      not register an experiment.

    - replace_existing controls queue cleanup only after conflict checking:
          false: preserve existing queued commands and append this experiment
          true:  after the time-conflict check passes, clear old queues/state
                 before enqueueing this experiment
      It does not bypass same-lab time-conflict checking.

    - state is initially "registered".
      Later states may be derived or updated as: "running", "completed",
      or "cancelled".

    - Same-lab experiment registration rejects time conflicts:
          existing.start_at < new.end_at + guard_sec
          AND
          existing.end_at   > new.start_at - guard_sec

    - The registry stream is:
          config.KEY_EXPERIMENT_REGISTRY = "nms:experiment:registry"

    Safe mutation order:
        1. parse t0 and require it to be in the future
        2. parse/decode CSV
        3. dry-analyze accepted rows and compute end_at
        4. reject if command_count == 0
        5. check single-experiment registration gate
        6. reset lab mobility state
        7. enqueue accepted commands
        8. write registry record

    Registry fields written by this endpoint:
        experiment_id
        session_id
        lab_id
        registered_at
        start_at
        end_at
        guard_sec
        command_count
        replace_existing
        state
        time_format
    """
    try:
        t0_dt = utility.parse_local_dt(t0)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid t0; expected like '{utility.local_ts()}' (format {config.TIME_FMT})"
        )

    t0_preflight = _require_experiment_t0_future(t0_dt)

    original_filename = (csv_file.filename or "").strip()
    filename_lower = original_filename.lower()

    if not filename_lower.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Uploaded file must be a .csv file")

    experiment_id = original_filename.rsplit(".", 1)[0].strip()
    if not experiment_id:
        raise HTTPException(status_code=400, detail="CSV filename must provide a non-empty experiment_id")

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

    analysis = _analyze_csv_rows_for_experiment(
        rows,
        t0_dt=t0_dt,
    )

    added = int(analysis["added"])
    skipped_not_whitelisted = int(analysis["skipped_not_whitelisted"])
    bad_rows = int(analysis["bad_rows"])
    last_execute_at = analysis["last_execute_at"]
    accepted_items = analysis["accepted_items"]

    single_experiment_gate = _require_empty_experiment_registry()

    # Registration occurs immediately before experiment execution.
    # Reset the whole lab once, after all CSV validation/gating succeeds and
    # before any accepted experiment command is enqueued.
    mobility_reset = m8mobility.mobility_init()

    for item in accepted_items:
        _enqueue_script_or_csv_item(
            scanner=item["scanner"],
            category=item["category"],
            action=item["action"],
            execute_at=item["execute_at"],
            args=item["args"],
            lab_id=_nms_lab_id(),
            experiment_id=experiment_id,
            session_id=session_id,
        )

    exp = _register_experiment_status(
        experiment_id=experiment_id,
        session_id=session_id,
        start_at_str=t0_dt.strftime(config.TIME_FMT),
        end_at_str=last_execute_at.strftime(config.TIME_FMT),
        command_count=added,
        replace_existing=replace_existing,
    )

    preflight = {
        **analysis["preflight"],
        "t0_gate": t0_preflight,
        "single_experiment_gate": single_experiment_gate,
        "mobility_reset": mobility_reset,
    }

    return {
        "status": "ok",
        "t0": t0_dt.strftime(config.TIME_FMT),
        "time_format": config.TIME_FMT,
        "filename": csv_file.filename or "",
        "experiment_id": experiment_id,
        "session_id": (session_id or "").strip() or None,
        "lab_id": _nms_lab_id(),
        "replace_existing": bool(replace_existing),
        "added": added,
        "skipped_not_whitelisted": skipped_not_whitelisted,
        "bad_rows": bad_rows,
        "preflight": preflight,
        "experiment": exp,
    }
