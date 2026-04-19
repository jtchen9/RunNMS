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

from config import r, KEY_REGISTRY, KEY_WHITELIST_SCANNER_META
from utility import _hset_many, local_ts

from m8mobility_state_store import (
    _clear_outgoing_command_preview, _clear_pending_sequence, _load_stop, 
    _reset_correction_counter, _set_state, key_state, key_time, key_report, key_pose
)
from m8mobility_state import (
    VALID_STATES, S0_IDLE, S1_WAITING_REPORT, S2_EVALUATING_POLICY, S3_SOLVING_TRUE_LOCATION,
    S4_WAITING_LOCATION_RETRY, S5_COMPUTING_CORRECTION, S6_ISSUING_CORRECTION, S7_STOPPED, 
    enter_s0idle_on_command, enter_s1waiting_report_on_report, process_s1_event, run_state_machine, s0idle, s1waiting_report,
    s2evaluating_policy, s3solving_true_location, s4waiting_location_retry, s5computing_correction,
    s6issuing_correction, s7stopped, _get_state
)
from m8mobility_map import _ensure_mobility_assets_ready

router = APIRouter()

# ===== B1) initialization block =====

def mobility_init() -> Dict[str, Any]:
    """
    Unfinished temporary function.

    Current intended boundary:
    - Keep mobility subsystem isolated from the rest of NMS.
    - Do not enqueue commands, poll robots, or run state transitions here.
    - Only prepare static assets / config and leave robots waiting in S0_IDLE.

    Planned responsibilities:
    0) rely on POST /admin/reset to clean up previous experiment data
    1) run _ensure_mobility_assets_ready()
    2) if needed, read site AprilTag map
    3) if needed, read static restriction map
    4) initialize mobility state to S0_IDLE for relevant scanners
    5) validate required mobility configuration if needed
    """
    assets = _ensure_mobility_assets_ready()

    scanners = sorted(list(r.smembers(KEY_REGISTRY)))
    initialized = []
    skipped_not_whitelisted = []

    for scanner in scanners:
        if not r.hexists(KEY_WHITELIST_SCANNER_META, scanner):
            skipped_not_whitelisted.append(scanner)
            continue

        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        _reset_correction_counter(scanner)
        _set_state(scanner, S0_IDLE, "mobility_init")

        _hset_many(
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

        _hset_many(
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

        _hset_many(
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
    Unfinished temporary function
    Manual recovery from s7stopped.
    Does not reconstruct pose; only clears stop-state residue for this scanner and returns to s0idle.
    """
    _clear_pending_sequence(scanner)
    _clear_outgoing_command_preview(scanner)
    _reset_correction_counter(scanner)

    _hset_many(
        key_time(scanner),
        {
            "s1_timer_token": "",
            "s1_timer_started_at": "",
            "busy_retry_token": "",
            "busy_retry_started_at": "",
        },
    )

    _hset_many(
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
    Unfinished temporary function.

    Future wiring target:
    - m4Commands interactive enqueue
    - m4Commands CSV load
    - m4Commands CSV file upload
    - if blocked try until success

    Current responsibility:
    - Only gate by current mobility state
    - Delegate all S0 command-entry logic to enter_s0idle_on_command(...)
    """
    state = _get_state(scanner)
    if state != S0_IDLE:
        _hset_many(
            key_state(scanner),
            {
                "state_detail": f"blocked: not idle, ({state})",
                "state_updated_at": local_ts(),
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
    Unfinished temporary function.

    ToDo: Link from outside to this entry point
    when robot sends mobility report.

    Report-arrival path for S1.
    The actual S1 resolution is serialized inside process_s1_event(...).
    """
    return process_s1_event(scanner, source="report")


# ===== B3) state control block =====

def should_block_command(category: str) -> bool:
    stop = _load_stop()
    if not stop.get("stop"):
        return False
    return (category or "").lower() != "av"


# ===== B4) API endpoint block for visibility =====

