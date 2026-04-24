"""
Mobility subsystem façade.

Blocks:
- B1) initialization block
- B2) input hook block
- B3) output hook block
- B4) state control block
- B5) API endpoint block for visibility

Phase 1 rule:
- Structural cleanup only; no logic smoothing.
"""
from typing import Dict, Any
from fastapi import APIRouter
import json
import asyncio

import config
import utility

from m8mobility_state_store import (
    _clear_outgoing_command_preview, _clear_pending_sequence, _load_stop, 
    _reset_correction_counter, _set_state, key_state, key_time, key_report, key_pose
)
from m8mobility_state import S0_IDLE, enter_s0idle_on_command, process_s1_event, _get_state
from m8mobility_map import _ensure_mobility_assets_ready

router = APIRouter()

# ===== B1) initialization block =====

def mobility_init() -> Dict[str, Any]:
    """
    Initialize mobility subsystem (Phase 2 scope).

    Responsibilities:
    - validate static assets
    - reset mobility runtime state
    - set scanners to S0_IDLE

    Does NOT:
    - start experiment
    - enqueue commands
    - run state machine

    Expected to be called before experiment start.
    """
    assets = _ensure_mobility_assets_ready()

    scanners = sorted(list(config.r.smembers(config.KEY_REGISTRY)))
    initialized = []
    skipped_not_whitelisted = []

    for scanner in scanners:
        if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, scanner):
            skipped_not_whitelisted.append(scanner)
            continue

        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        _reset_correction_counter(scanner)
        _set_state(scanner, S0_IDLE, "mobility_init")

        utility._hset_many(
            key_time(scanner),
            {
                "s1_timer_token": "",
                "s1_timer_started_at": "",
                "busy_retry_token": "",
                "busy_retry_started_at": "",
                "last_planned_command_issued_at": "",
                "policy_updated_at": "",
                "last_mobility_report_at": "",
            },
        )

        utility._hset_many(
            key_state(scanner),
            {
                "retry_count": "0",
                "collision_veto_count": "0",
                "busy_count": "0",
                "exec_fail_count": "0",
                "s2_entry_reason": "",
                "true_propagation_applied": "",
                "true_propagation_detail": "",
                "true_propagation_time": "",
                "stop_experiment": "false",
                "stop_reason": "",
                "robot_safety_state": "NORMAL",
                "need_location_retry": "false",
            },
        )

        utility._hset_many(
            key_report(scanner),
            {
                "last_mobility_report_json": "",
                "last_10s_report_json": "",
                "last_10s_report_at": "",
            },
        )

        initialized.append(scanner)

    return {
        "status": "ok",
        "detail": "mobility subsystem initialized",
        "assets": assets,
        "initialized_count": len(initialized),
        "initialized_scanners": initialized,
        "skipped_not_whitelisted": skipped_not_whitelisted,
    }

def manual_resume(scanner: str) -> Dict[str, Any]:
    """
    Manual recovery from S7.

    Responsibilities:
    - clear stop-related state for this scanner
    - clear timers and retry markers
    - return scanner to S0_IDLE

    Note:
    - does not reconstruct pose
    - does not restart experiment
    """
    _clear_pending_sequence(scanner)
    _clear_outgoing_command_preview(scanner)
    _reset_correction_counter(scanner)

    utility._hset_many(
        key_time(scanner),
        {
            "s1_timer_token": "",
            "s1_timer_started_at": "",
            "busy_retry_token": "",
            "busy_retry_started_at": "",
        },
    )

    utility._hset_many(
        key_state(scanner),
        {
            "retry_count": "0",
            "collision_veto_count": "0",
            "busy_count": "0",
            "exec_fail_count": "0",
            "s2_entry_reason": "",
            "true_propagation_applied": "",
            "true_propagation_detail": "",
            "true_propagation_time": "",
            "stop_experiment": "false",
            "stop_reason": "",
            "need_location_retry": "false",
            "robot_safety_state": "NORMAL",
        },
    )

    _set_state(scanner, S0_IDLE, "manual resume")

    return {
        "status": "ok",
        "scanner": scanner,
        "state": S0_IDLE,
        "detail": "manual resume complete",
    }


# ===== B2) input hook block =====

def on_command_issued(scanner: str, action: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Entry point for mobility commands from NMS.

    Current scope:
    - supports immediate execution from Swagger /cmd/_enqueue
    - routes command into S0

    Future:
    - will also be called by script / CSV loaders
    """
    state = _get_state(scanner)
    if state != S0_IDLE:
        utility._hset_many(
            key_state(scanner),
            {
                "state_detail": f"blocked: not idle, ({state})",
                "state_updated_at": utility.local_ts(),
            },
        )
        return {
            "status": "blocked",
            "scanner": scanner,
            "state": state,
            "detail": f"mobility command blocked in {state}",
        }

    return enter_s0idle_on_command(scanner, action, args)

def on_report_received(scanner: str) -> Dict[str, Any]:
    """
    Entry point when a mobility report is received from robot.

    Called from:
    - cmd_poll() when mobility_report_json is present

    Behavior:
    - only runs when state == S1_WAITING_REPORT
    - otherwise ignored (non-blocking)

    Starts state machine from S1.
    """    
    return process_s1_event(scanner, source="report")

def _collect_due_mobility_commands(server_now_str: str):
    server_now = utility.parse_local_dt(server_now_str)
    raw = config.r.xrange(config.KEY_MOBILITY_CMD_STREAM, count=5000)

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

        if len(due) >= config.MOBILITY_LOOP_BATCH_LIMIT:
            break

    return due

def _dispatch_due_mobility_command(xid: str, fields: Dict[str, str]) -> None:
    scanner = (fields.get("scanner") or "").strip()
    action = (fields.get("action") or "").strip()

    try:
        args = json.loads(fields.get("args_json") or "{}")
        if not isinstance(args, dict):
            args = {}
    except Exception:
        args = {}

    result = on_command_issued(scanner, action, args)

    status = str(result.get("status") or "").strip().lower()

    # Keep only when scanner is temporarily not ready.
    # Any non-blocked result means the mobility subsystem consumed the row.
    if status != "blocked":
        try:
            config.r.xdel(config.KEY_MOBILITY_CMD_STREAM, xid)
        except Exception:
            pass

async def _mobility_loop() -> None:
    while True:
        try:
            server_now_str = utility.local_ts()
            due = _collect_due_mobility_commands(server_now_str)

            for xid, fields in due:
                try:
                    _dispatch_due_mobility_command(xid, fields)
                except Exception:
                    pass
        except Exception:
            pass

        await asyncio.sleep(config.MOBILITY_LOOP_EVERY_SEC)
        
                
# ===== B3) state control block =====

def should_block_command(category: str) -> bool:
    stop = _load_stop()
    if not stop.get("stop"):
        return False
    return (category or "").lower() != "av"


# ===== B4) API endpoint block for visibility =====

@router.post("/mobility/init", tags=["8 Mobility"])
def api_mobility_init() -> Dict[str, Any]:
    return mobility_init()


@router.post("/mobility/manual_resume/{scanner}", tags=["8 Mobility"])
def api_mobility_manual_resume(scanner: str) -> Dict[str, Any]:
    return manual_resume(scanner)


@router.get("/mobility/state/{scanner}", tags=["8 Mobility"])
def api_mobility_state(scanner: str) -> Dict[str, Any]:
    state = _get_state(scanner)
    stop = _load_stop()

    return {
        "scanner": scanner,
        "state": state,
        "state_detail": utility._hget(key_state(scanner), "state_detail", ""),
        "state_updated_at": utility._hget(key_state(scanner), "state_updated_at", ""),
        "stop": stop,
        "correction_attempt_count": utility._hget(key_state(scanner), "correction_attempt_count", ""),
    }


@router.get("/mobility/debug/{scanner}", tags=["8 Mobility"])
def api_mobility_debug(scanner: str) -> Dict[str, Any]:
    return {
        "scanner": scanner,
        "state": config.r.hgetall(key_state(scanner)),
        "time": config.r.hgetall(key_time(scanner)),
        "report": config.r.hgetall(key_report(scanner)),
        "pose": config.r.hgetall(key_pose(scanner)),
    }
