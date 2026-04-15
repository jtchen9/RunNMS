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
from m8mobility_command_model import _build_command_from_true_to_planned, _normalize_mobility_command
from m8mobility_pose import _apply_mobility_command_to_pose, _is_loc_ok, _load_true, _save_planned
from m8mobility_state_store import _clear_outgoing_command_preview, _clear_pending_sequence, _get_state, _load_stop, _reset_correction_counter, _set_state, key_pose, key_state, key_time
from m8mobility_map import _ensure_mobility_assets_ready, _is_path_clear
from m8mobility_state import (
    VALID_STATES, S0_IDLE, S1_WAITING_REPORT, S2_EVALUATING_POLICY, S3_SOLVING_TRUE_LOCATION,
    S4_WAITING_LOCATION_RETRY, S5_COMPUTING_CORRECTION, S6_ISSUING_CORRECTION, S7_STOPPED, _s0_init_planned,
    enter_s0idle_on_command, enter_s1waiting_report_on_report, s0idle, s1waiting_report,
    s2evaluating_policy, s3solving_true_location, s4waiting_location_retry, s5computing_correction,
    s6issuing_correction, s7stopped, manual_resume_recover
)
from utility import _hset_many
from utility import local_ts

router = APIRouter()

# ===== B1) initialization block =====

def mobility_init():
    """
    Unfinished temporary function
    """
    pass

def manual_resume(scanner: str) -> Dict[str, Any]:
    """
    Unfinished temporary function
    Manual recovery from s7stopped.
    Does not reconstruct pose; only clears stop state for this scanner and returns to s0idle.
    """
    _clear_pending_sequence(scanner)
    _clear_outgoing_command_preview(scanner)
    _reset_correction_counter(scanner)
    _set_state(scanner, S0_IDLE, "manual resume")

    return {
        "status": "ok",
        "scanner": scanner,
        "state": S0_IDLE,
        "detail": "manual resume complete",
    }


# ===== B2) input hook block =====

def on_command_issued(scanner: str, action: str, args: Dict[str, Any]) -> Dict[str, Any]:
    state = _get_state(scanner)
    if state != S0_IDLE:
        _hset_many(
            key_state(scanner),
            {
                "state_detail": f"blocked: not idle ({state})",
                "state_updated_at": local_ts(),
            },
        )
        return {"status": "blocked", "state": state}

    stop = _load_stop()
    if stop.get("stop"):
        _set_state(scanner, S7_STOPPED, stop.get("reason", ""))
        return {"status": "blocked", "reason": "experiment stopped"}

    try:
        # ---------------------------------------------------------
        # Step 1: normalize script command
        # ---------------------------------------------------------
        action, args = _normalize_mobility_command(action, args)

        # ---------------------------------------------------------
        # Step 2: update planned location (script intent)
        # ---------------------------------------------------------
        planned = _s0_init_planned(scanner)
        new_planned = _apply_mobility_command_to_pose(planned, action, args)
        _save_planned(scanner, new_planned)

        # ---------------------------------------------------------
        # Step 3: build actual motion command (NEW CORE)
        # ---------------------------------------------------------
        issued_action, issued_args, build_debug = _build_command_from_true_to_planned(scanner)

        # ---------------------------------------------------------
        # Step 4: determine start pose (for path check)
        # ---------------------------------------------------------
        start = _load_true(scanner)
        if not _is_loc_ok(start):
            # fallback ONLY if true does not exist at all
            start = new_planned

        # ---------------------------------------------------------
        # Step 5: path safety check
        # ---------------------------------------------------------
        tx = float(start["x_m"])
        ty = float(start["y_m"])
        px = float(new_planned["x_m"])
        py = float(new_planned["y_m"])

        path_ok, blocked = _is_path_clear(tx, ty, px, py, exclude_scanner=scanner)

        if not path_ok:
            _set_state(scanner, S7_STOPPED, f"script command path unsafe, blocked={len(blocked)}")
            return {
                "status": "stop",
                "reason": "path unsafe",
                "blocked_cells": len(blocked),
                "debug": build_debug,
            }

        # ---------------------------------------------------------
        # Step 6: record issued command (IMPORTANT: modified command)
        # ---------------------------------------------------------
        ts = local_ts()

        _hset_many(
            key_time(scanner),
            {
                "last_planned_command_issued_at": ts,
            },
        )

        _hset_many(
            key_pose(scanner),
            {
                "last_planned_command_action": issued_action,
                "last_planned_command_args_json": issued_args,
            },
        )

        # ---------------------------------------------------------
        # Step 7: reset correction counter (new script command)
        # ---------------------------------------------------------
        _reset_correction_counter(scanner)

        # ---------------------------------------------------------
        # Step 8: debug info
        # ---------------------------------------------------------
        _hset_many(
            key_state(scanner),
            {
                "last_command_build_debug": build_debug,
                "outgoing_command_action": issued_action,
                "outgoing_command_args_json": issued_args,
                "outgoing_command_source": "script",
                "outgoing_command_updated_at": ts,
            },
        )

        # ---------------------------------------------------------
        # Step 9: transition
        # ---------------------------------------------------------
        _set_state(scanner, S1_WAITING_REPORT, f"issued {issued_action}")

        return {
            "status": "ok",
            "issued_action": issued_action,
            "issued_args": issued_args,
            "new_planned": new_planned,
            "state_after": S1_WAITING_REPORT,
            "debug": build_debug,
        }

    except Exception as e:
        _set_state(scanner, S0_IDLE, f"s0 error: {e}")
        return {"status": "error", "detail": str(e)}
    
def on_report_received(scanner: str) -> Dict[str, Any]:
    """
    Entry when robot sends mobility report.
    In Phase 3, just run the state machine from s1waiting_report.
    """
    return run_state_machine(scanner)


# ===== B3) output hook block =====


# ===== B4) state control block =====

def should_block_command(category: str) -> bool:
    stop = _load_stop()
    if not stop.get("stop"):
        return False
    return (category or "").lower() != "av"

def run_state_machine(scanner: str) -> Dict[str, Any]:
    state = _get_state(scanner)

    if state == S0_IDLE:
        return s0idle(scanner)
    if state == S1_WAITING_REPORT:
        return s1waiting_report(scanner)
    if state == S2_EVALUATING_POLICY:
        return s2evaluating_policy(scanner)
    if state == S3_SOLVING_TRUE_LOCATION:
        return s3solving_true_location(scanner)
    if state == S4_WAITING_LOCATION_RETRY:
        return s4waiting_location_retry(scanner)
    if state == S5_COMPUTING_CORRECTION:
        return s5computing_correction(scanner)
    if state == S6_ISSUING_CORRECTION:
        return s6issuing_correction(scanner)
    if state == S7_STOPPED:
        return s7stopped(scanner)

    _set_state(scanner, S0_IDLE, "invalid state reset")
    return s0idle(scanner)


# ===== B5) API endpoint block for visibility =====

