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
import m8mobility_link as link
import m8mobility_state as state_api
import m8mobility_s8_view as view_rules

from m8mobility_state_store import (
    _clear_outgoing_command_preview, _clear_pending_sequence,  _clear_pose, _load_stop, _save_stop,
    _reset_correction_counter, _set_state, key_state, key_time, key_report, key_pose
)
from m8mobility_state import (
    S0_IDLE, S7_STOPPED, S8_RECOVERY, _s8_episode, _s8_expired, _s8_stop, enter_s0idle_on_command, process_s1_event, _get_state, _cancel_runtime_timers,
)
from m8mobility_map import _ensure_mobility_assets_ready

router = APIRouter()


def _delete_stream_rows_for_scanner(stream_key: str, scanner: str) -> int:
    """Delete only rows owned by one scanner from a shared Redis stream."""
    delete_ids = []
    try:
        rows = config.r.xrange(stream_key, min="-", max="+")
    except Exception:
        return 0

    for xid, fields in rows:
        if str((fields or {}).get("scanner") or "").strip() == scanner:
            delete_ids.append(xid)

    if not delete_ids:
        return 0

    try:
        return int(config.r.xdel(stream_key, *delete_ids))
    except Exception:
        return 0


def _clear_command_queues_for_scanner(scanner: str) -> Dict[str, Any]:
    """Clear one scanner's robot queue and rows in shared mobility/traffic queues."""
    robot_key = config.key_cmd_stream(scanner)
    deleted_robot_queue = int(config.r.delete(robot_key))
    deleted_mobility_rows = _delete_stream_rows_for_scanner(
        config.KEY_MOBILITY_CMD_STREAM, scanner
    )
    deleted_traffic_rows = _delete_stream_rows_for_scanner(
        config.KEY_TRAFFIC_CMD_STREAM, scanner
    )

    return {
        "scanner": scanner,
        "deleted_robot_queue_keys": deleted_robot_queue,
        "deleted_mobility_rows": deleted_mobility_rows,
        "deleted_traffic_rows": deleted_traffic_rows,
    }


def _clear_all_command_queues() -> Dict[str, Any]:
    """Clear all current robot command queues plus shared mobility/traffic queues."""
    robot_keys = list(config.r.scan_iter(match=f"{config.KEY_PREFIX}cmd:*"))
    deleted_robot_queue_keys = int(config.r.delete(*robot_keys)) if robot_keys else 0
    deleted_mobility_queue_keys = int(config.r.delete(config.KEY_MOBILITY_CMD_STREAM))
    deleted_traffic_queue_keys = int(config.r.delete(config.KEY_TRAFFIC_CMD_STREAM))

    return {
        "deleted_robot_queue_keys": deleted_robot_queue_keys,
        "deleted_mobility_queue_keys": deleted_mobility_queue_keys,
        "deleted_traffic_queue_keys": deleted_traffic_queue_keys,
    }

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

    # Whole-lab reset semantics:
    # - cancel in-process mobility timers
    # - clear global stop latch
    # - clear pending command queues
    # - invalidate old pose before returning scanners to S0_IDLE
    for scanner in scanners:
        _cancel_runtime_timers(scanner)
 
    _save_stop(False, "")
    state_machine_test_key = getattr(
        config,
        "KEY_MOBILITY_STATE_MACHINE_TEST_ENABLED",
        f"{config.KEY_PREFIX}debug:mobility_state_machine_test_enabled",
    )
    config.r.set(state_machine_test_key, "false")
    queue_cleanup = _clear_all_command_queues()

    for scanner in scanners:
        if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, scanner):
            skipped_not_whitelisted.append(scanner)
            continue

        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        _clear_pose(scanner)
        _reset_correction_counter(scanner)
        config.r.hdel(key_state(scanner), "s8_episode_json", "s8_landing_issue_claim")
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
                "s5_runtime_safety_stop_json": "",
                "last_script_mobility_action": "",
                "last_script_mobility_args_json": "",
                "last_script_mobility_planned_json": "",
                "last_s0_command_action": "",
                "last_s0_command_args_json": "",
                "last_s0_command_at": "",
                "last_s0_rejected_action": "",
                "last_s0_rejected_args_json": "",
                "last_s0_rejected_reason": "",
                "last_s0_rejected_at": "",
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
        "queue_cleanup": queue_cleanup,
        "global_stop_cleared": True,
        "state_machine_test_enabled": False,
        "state_machine_test_key": state_machine_test_key,
    }

def manual_resume(scanner: str) -> Dict[str, Any]:
    """
    Manual reset/recovery for one scanner.

    Responsibilities:
    - cancel active mobility timers for this scanner
    - clear this scanner's pending command queues
    - invalidate old true/planned pose
    - clear stop-related state and retry markers
    - return scanner to S0_IDLE

    Note:
    - does not reconstruct pose
    - does not restart experiment
    - mobility.report.location is required before normal state-machine movement
    """
    _cancel_runtime_timers(scanner)
    queue_cleanup = _clear_command_queues_for_scanner(scanner)

    _clear_pending_sequence(scanner)
    _clear_outgoing_command_preview(scanner)
    _clear_pose(scanner)
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
            "s5_runtime_safety_stop_json": "",
            "last_script_mobility_action": "",
            "last_script_mobility_args_json": "",
            "last_script_mobility_planned_json": "",
            "last_s0_command_action": "",
            "last_s0_command_args_json": "",
            "last_s0_command_at": "",
            "last_s0_rejected_action": "",
            "last_s0_rejected_args_json": "",
            "last_s0_rejected_reason": "",
            "last_s0_rejected_at": "",
        },
    )

    config.r.hdel(key_state(scanner), "s8_episode_json", "s8_landing_issue_claim")
    _set_state(scanner, S0_IDLE, "manual resume")

    return {
        "status": "ok",
        "scanner": scanner,
        "state": S0_IDLE,
        "detail": "manual resume complete",
        "queue_cleanup": queue_cleanup,
        "pose_cleared": True,
        "timers_cancelled": True,
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
        # Holding a new row must not overwrite active-command/stop diagnostics.
        return {
            "status": "blocked",
            "scanner": scanner,
            "state": state,
            "detail": f"mobility command blocked in {state}",
        }

    return enter_s0idle_on_command(scanner, action, args)

def on_report_received(scanner: str, report=None) -> Dict[str, Any]:
    """
    Entry point when a mobility report is received from robot.

    Called from:
    - cmd_poll() when mobility_report_json is present

    Behavior:
    - only runs when state == S1_WAITING_REPORT
    - otherwise ignored (non-blocking)

    Starts state machine from S1.
    """    
    if (_get_state(scanner) == S8_RECOVERY and not _load_stop().get("stop")
            and getattr(config,"MOBILITY_S8_VIEW_ENABLED",False) is True
            and _s8_episode(scanner).get("kind") == "check_alternate_view" and report is not None):
        return state_api.s8recovery(scanner, report=report)
    if _load_stop().get("stop") or _get_state(scanner) in (S7_STOPPED, S8_RECOVERY):
        return {"status": "ignored", "detail": "report recorded only; no active S1 transaction"}
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

    if _get_state(scanner) == S0_IDLE and not _load_stop().get("stop"):
        gate = link.disposition(scanner, key_time(scanner), "communication_deadline")
        if gate == "wait":
            return  # Original row stays in the NMS stream; no robot command or retry.
        if gate == "expired":
            from m8mobility_state import S7_STOPPED, s7stopped
            _set_state(scanner, S7_STOPPED, "communication_timeout before dispatch")
            s7stopped(scanner)
            config.r.xdel(config.KEY_MOBILITY_CMD_STREAM, xid)
            return
    result = on_command_issued(scanner, action, args)

    status = str(result.get("status") or "").strip().lower()

    # Keep only when scanner is temporarily not ready.
    # Any non-blocked result means the mobility subsystem consumed the row.
    if status != "blocked":
        try:
            config.r.xdel(config.KEY_MOBILITY_CMD_STREAM, xid)
        except Exception:
            pass

def _check_s8_deadlines():
    # Runs without a robot poll, including after the experiment end time.
    for scanner in config.r.smembers(config.KEY_REGISTRY):
        state = _get_state(scanner)
        if state in (S0_IDLE, S7_STOPPED):
            continue
        episode = _s8_episode(scanner)
        if state == S8_RECOVERY or episode.get("landing"):
            if _load_stop().get("stop") or _s8_expired(episode):
                _s8_stop(scanner, episode, "stopped or recovery deadline expired")


def _check_participant_links(now):
    if _load_stop().get("stop"):
        return
    for _, record in config.r.xrange(config.KEY_EXPERIMENT_REGISTRY):
        if (record.get("state") != "registered" or
                not record["start_at"] <= now <= record["end_at"]):
            continue
        for scanner in json.loads(record.get("mobility_scanners_json", "[]")):
            key = key_time(scanner)
            status = link.disposition(scanner, key, "communication_deadline")
            if status == "expired":
                from m8mobility_state import S7_STOPPED, s7stopped
                _set_state(scanner, S7_STOPPED, "communication_timeout: robot polls absent")
                s7stopped(scanner)
                return


async def _mobility_loop() -> None:
    import time
    next_check = 0
    while True:
        try:
            server_now_str = utility.local_ts()
            if time.monotonic() >= next_check:
                next_check = time.monotonic() + config.MOBILITY_LINK_CHECK_SEC
                _check_s8_deadlines()
                _check_participant_links(server_now_str)
            due = _collect_due_mobility_commands(server_now_str)

            # Oldest due command per robot first. A reconnect halfway through
            # a batch must not release a later row ahead of the held row.
            seen = set()
            for xid, fields in due:
                scanner = fields.get("scanner")
                if scanner in seen:
                    continue
                seen.add(scanner)
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


def movement_delivery_blocked(scanner: str, fields: Dict[str, Any]) -> bool:
    """Final delivery gate; called only for physical/mobility rows."""
    if should_block_command("mobility"):
        return True
    state = _get_state(scanner)
    if state == S7_STOPPED:
        return True
    ep = _s8_episode(scanner)
    if ep.get("landing") and state not in (S0_IDLE, S8_RECOVERY):
        # S6 uses the normal S1-managed queue. Do not deliver leftover moves.
        try:
            args = json.loads(fields.get("args_json") or "{}")
        except (TypeError, ValueError):
            return True
        return not (state in ("s6issuing_correction", "s1waiting_report")
                    and utility._hget(key_state(scanner), "s8_landing_issue_claim", "") == ep.get("id")
                    and fields.get("mobility_s1_managed") == "1" and not _s8_expired(ep)
                    and fields.get("action") == "mobility.turn_move_turn.forward"
                    and args == ep["landing"].get("args"))
    child = fields.get("s8_episode_id")
    if state == S8_RECOVERY:
        episode = _s8_episode(scanner)
        if (getattr(config,"MOBILITY_S8_ENABLED",False) is not True or
                getattr(config,"MOBILITY_S8_VIEW_ENABLED",False) is not True or not child or
                child != episode.get("id") or episode.get("kind") != "check_alternate_view" or _s8_expired(episode)):
            return True
        try:
            return not view_rules.admit(state_api,scanner,episode,fields)
        except Exception as exc:
            _s8_stop(scanner,episode,f"alternate-view delivery veto: {type(exc).__name__}: {str(exc)[:160]}")
            return True
    return bool(child)  # a child from a closed episode cannot be released


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
