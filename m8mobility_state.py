"""
Mobility subsystem state logic.

Division rule:
- State orchestration lives here, in state order.
- Shared services are imported from mobility-specific service files.
- No wildcard imports.
"""
import json
from typing import Dict, Any, Optional
import math
import threading
import uuid
import config
import utility

from m8mobility_command_model import _angle_diff_deg, _build_command_from_true_to_planned, _circular_mean_deg
from m8mobility_state_store import ( 
    _reset_correction_counter, key_state, key_time, key_pose, _set_state, _load_stop, _save_stop, _is_anchor_fresh, 
    _load_report_json, _save_policy_time, _save_pending_sequence, _clear_pending_sequence, 
    _load_pending_sequence, _save_last_issued_command, _save_outgoing_command_preview, 
    _clear_outgoing_command_preview, _inc_correction_counter, 
    _get_correction_counter, _update_10s_report, _load_true, _load_planned, _save_planned, _is_loc_ok   
) 
from m8mobility_pose import ( 
    _apply_mobility_command_to_pose, _pose_error 
) 
from m8mobility_map import _is_path_clear, _is_path_clear_debug, _load_tag_map 
from m8mobility_command_model import ( 
    _normalize_mobility_command, 
    _build_turn_only_command, _build_turn_move_turn_forward_command, 
    _propagate_true_by_last_command, _should_propagate_true
)

# ===== state constants =====

S0_IDLE = "s0idle"
S1_WAITING_REPORT = "s1waiting_report"
S2_EVALUATING_POLICY = "s2evaluating_policy"
S3_SOLVING_TRUE_LOCATION = "s3solving_true_location"
S4_WAITING_LOCATION_RETRY = "s4waiting_location_retry"
S5_COMPUTING_CORRECTION = "s5computing_correction"
S6_ISSUING_CORRECTION = "s6issuing_correction"
S7_STOPPED = "s7stopped"

VALID_STATES = {
    S0_IDLE,
    S1_WAITING_REPORT,
    S2_EVALUATING_POLICY,
    S3_SOLVING_TRUE_LOCATION,
    S4_WAITING_LOCATION_RETRY,
    S5_COMPUTING_CORRECTION,
    S6_ISSUING_CORRECTION,
    S7_STOPPED,
}


# ===== policy thresholds =====

LOCATION_RETRY_LIMIT = 2
UNEXPECTED_EVENT_SUM_LIMIT = 2
IMMEDIATE_STOP_ERROR_CODES = {
    "TOF_SENSOR_FAIL",
    "BAD_COMMAND_ARGS",
    "UNEXPECTED_EXCEPTION",
}
RECOVERABLE_ERROR_CODES = {
    "MOVE_EXEC_FAIL",
    "TURN_EXEC_FAIL",
    "LOCATION_CAPTURE_FAIL",
}
NO_TAG_OK_CODES = {
    "NO_TAG_VISIBLE",
}
COLLISION_ERROR_CODES = {
    "COLLISION_BLOCKED_AT_START",
    "COLLISION_STOP_DURING_MOVE",
}

MULTI_TAG_POS_THRESH_M = 0.5
MULTI_TAG_HEADING_THRESH_DEG = 20.0

CORRECTION_ATTEMPT_LIMIT = 1

S1_REPORT_TIMEOUT_SEC = 90
S1_EVENT_LOCK_TTL_SEC = 10
_S1_TIMERS: Dict[str, threading.Timer] = {}

MOBILITY_BUSY_RETRY_WAIT_SEC = 5
_BUSY_RETRY_TIMERS: Dict[str, threading.Timer] = {}


# ===== helper =====

def _get_state(scanner: str) -> str:
    s = utility._hget(key_state(scanner), "state", S0_IDLE)
    return s if s in VALID_STATES else S0_IDLE


# ===== s0idle =====

def s0idle(scanner: str) -> Dict[str, Any]:
    return {"state": S0_IDLE, "status": "ok", "detail": "idle"}

def enter_s0idle_on_command(scanner: str, action: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Entry point for script command (mobility category).

    Responsibilities:
    1) initialize planned location from current true location
    2) reset correction counter
    3) enter S5 → S6 to construct and issue command

    Notes:
    - command emission is handled in S6 via normal command stream
    - no direct queue logic here
    """
    stop = _load_stop()
    if stop.get("stop"):
        _set_state(scanner, S7_STOPPED, stop.get("reason", ""))
        return s7stopped(scanner)

    try:
        # ---------------------------------------------------------
        # Step 1: normalize script command
        # ---------------------------------------------------------
        action, args = _normalize_mobility_command(action, args)

        # ---------------------------------------------------------
        # Step 2A: location precondition command
        # ---------------------------------------------------------
        # mobility.report.location is not a movement intent.  It must not
        # advance planned_location_json and it must not trigger S5 correction
        # from a stale planned pose.  It is used to refresh true_location_json;
        # S3 will align planned=true when the location report is solved.
        if action == "mobility.report.location":
            _clear_pending_sequence(scanner)
            _clear_outgoing_command_preview(scanner)
            _reset_correction_counter(scanner)

            _save_outgoing_command_preview(
                scanner,
                action=action,
                args=args,
                source="location_precondition",
            )
            issued_at = _save_last_issued_command(
                scanner,
                action=action,
                args=args,
            )

            _set_state(scanner, S1_WAITING_REPORT, "location precondition issued")
            _start_s1_timer(scanner)

            return {
                "state": S1_WAITING_REPORT,
                "status": "ok",
                "detail": "location precondition issued",
                "issued_command": {
                    "action": action,
                    "args": args,
                },
                "issued_at": issued_at,
            }

        # ---------------------------------------------------------
        # Step 2B: update planned location (script movement intent only)
        # ---------------------------------------------------------
        planned = _s0_init_planned(scanner)
        new_planned = _apply_mobility_command_to_pose(planned, action, args)
        _save_planned(scanner, new_planned)

        # ---------------------------------------------------------
        # Step 3: reset correction counter for this new script command
        # Counter semantics:
        #   -1 : no command issued yet
        #    0 : initial modified command already issued
        #   >=1: true correction attempts
        # ---------------------------------------------------------
        _reset_correction_counter(scanner)

        # ---------------------------------------------------------
        # Step 4: transition to S5
        # S0 does NOT issue commands anymore.
        # S5/S6 now own command computation + issuance.
        # ---------------------------------------------------------
        _set_state(scanner, S5_COMPUTING_CORRECTION, f"script accepted: {action}")
        return run_state_machine(scanner)
    
    except Exception as e:
        _set_state(scanner, S7_STOPPED, f"s0 stop: {e}")
        return s7stopped(scanner)
    
def _s0_init_planned(scanner: str) -> Dict[str, Any]:
    planned = _load_planned(scanner)
    if _is_loc_ok(planned):
        return planned

    true_loc = _load_true(scanner)
    if not _is_loc_ok(true_loc):
        raise ValueError("missing initial true location")

    ok, reason = _is_anchor_fresh(scanner)
    if not ok:
        raise ValueError(f"initial true location not usable: {reason}")

    planned = {
        "location_ok": True,
        "x_m": float(true_loc["x_m"]),
        "y_m": float(true_loc["y_m"]),
        "heading_deg": utility._deg_norm_360(float(true_loc["heading_deg"])),
    }

    _save_planned(scanner, planned)
    return planned


# ===== s1waiting_report =====

def s1waiting_report(scanner: str) -> Dict[str, Any]:
    return {
        "status": "waiting",
        "state": S1_WAITING_REPORT,
        "detail": "waiting for expected mobility report",
    }

def process_s1_event(scanner: str, source: str, timer_token: Optional[str] = None) -> Dict[str, Any]:
    """
    Unified S1 event resolver.

    source:
      - "report"  : triggered by robot API report arrival
      - "timeout" : triggered by S1 timer callback
    """
    owner = _acquire_s1_lock(scanner)
    if not owner:
        return {
            "status": "blocked",
            "scanner": scanner,
            "detail": "s1 event lock busy",
        }

    try:
        state = _get_state(scanner)
        if state != S1_WAITING_REPORT:
            return {
                "status": "blocked",
                "scanner": scanner,
                "state": state,
                "detail": f"s1 event ignored in {state}",
            }

        # ---------------------------------------------------------
        # Source A: report arrival
        # ---------------------------------------------------------
        if source == "report":
            ok, reason = _s1_has_fresh_report(scanner)
            if not ok:
                return {
                    "status": "waiting",
                    "scanner": scanner,
                    "state": S1_WAITING_REPORT,
                    "detail": reason,
                }

            match_ok, match_reason = _s1_report_matches_expected_command(scanner)
            if not match_ok:
                return {
                    "status": "waiting",
                    "scanner": scanner,
                    "state": S1_WAITING_REPORT,
                    "detail": match_reason,
                }

            _cancel_s1_timer(scanner)
            return enter_s1waiting_report_on_report(scanner)

        # ---------------------------------------------------------
        # Source B: timeout callback
        # ---------------------------------------------------------
        if source == "timeout":
            current_token = utility._hget(key_time(scanner), "s1_timer_token", "")
            if not current_token or str(current_token) != str(timer_token or ""):
                return {
                    "status": "ignored",
                    "scanner": scanner,
                    "detail": "stale s1 timeout token",
                }

            # If a valid report is already present, let normal path win.
            ok, _ = _s1_has_fresh_report(scanner)
            if ok:
                match_ok, _ = _s1_report_matches_expected_command(scanner)
                if match_ok:
                    _cancel_s1_timer(scanner)
                    return enter_s1waiting_report_on_report(scanner)

            # No acceptable report has arrived in time -> dangerous.
            state_hash = key_state(scanner)
            old_busy_count = int(utility._hget(state_hash, "busy_count", "0") or "0")
            new_busy_count = old_busy_count + 1

            utility._hset_many(
                state_hash,
                {
                    "busy_count": str(new_busy_count),
                    "need_location_retry": "true",
                    "robot_safety_state": "WAITING_LOCATION_RETRY",
                    "state_detail": f"s1 timeout after {S1_REPORT_TIMEOUT_SEC}s",
                    "state_updated_at": utility.local_ts(),
                },
            )

            _cancel_s1_timer(scanner)
            _set_state(scanner, S4_WAITING_LOCATION_RETRY, f"s1 timeout after {S1_REPORT_TIMEOUT_SEC}s")
            return run_state_machine(scanner)

        return {
            "status": "error",
            "scanner": scanner,
            "detail": f"unknown s1 event source: {source}",
        }

    finally:
        _release_s1_lock(scanner, owner)

def enter_s1waiting_report_on_report(scanner: str) -> Dict[str, Any]:
    """
    Handle mobility report in S1.

    Responsibilities:
    - validate report matches expected command
    - reject irrelevant or outdated reports
    - cancel S1 timer on valid report
    - transition to S2

    Note:
    - timeout path is handled separately by timer callback
    """
    report = _load_report_json(scanner)
    if not isinstance(report, dict) or not report:
        _set_state(scanner, S7_STOPPED, "missing mobility report at s1 entry")
        return s7stopped(scanner)

    _set_state(scanner, S2_EVALUATING_POLICY, "fresh report received")
    return run_state_machine(scanner)

def _s1_has_fresh_report(scanner: str) -> tuple[bool, str]:
    report_ts = utility._hget(key_time(scanner), "last_mobility_report_at", "")
    issued_ts = utility._hget(key_time(scanner), "last_planned_command_issued_at", "")

    if not issued_ts:
        return False, "missing last_planned_command_issued_at"

    if not report_ts:
        return False, "missing last_mobility_report_at"

    if report_ts < issued_ts:
        return False, "report not fresh yet"

    return True, ""

def _s1_has_timed_out(scanner: str) -> tuple[bool, str]:
    issued_ts = utility._hget(key_time(scanner), "last_planned_command_issued_at", "")
    if not issued_ts:
        return False, "missing last_planned_command_issued_at"

    try:
        issued_dt = utility.parse_local_dt(issued_ts)
        now_dt = utility.parse_local_dt(utility.local_ts())
    except Exception:
        return False, "time parse failed"

    elapsed = (now_dt - issued_dt).total_seconds()
    if elapsed >= S1_REPORT_TIMEOUT_SEC:
        return True, f"s1 report timeout after {int(elapsed)} sec"

    return False, ""

def _s1_report_matches_expected_command(scanner: str) -> tuple[bool, str]:
    report = _load_report_json(scanner)
    if not isinstance(report, dict) or not report:
        return False, "missing mobility report json"

    expected_action = utility._hget(key_pose(scanner), "last_planned_command_action", "")
    expected_args_raw =  utility._hget(key_pose(scanner), "last_planned_command_args_json", "")

    report_action = str(report.get("last_command") or "").strip()
    report_args = report.get("last_command_args") or {}

    try:
        expected_args = json.loads(expected_args_raw) if str(expected_args_raw).strip() else {}
        if not isinstance(expected_args, dict):
            return False, "expected command args is not a JSON object"
    except Exception as e:
        return False, f"expected command args decode failed: {e}"

    if not isinstance(report_args, dict):
        return False, "report command args is not a JSON object"

    if not expected_action:
        return False, "missing expected action"
    if report_action != expected_action:
        return False, f"report action mismatch: expected {expected_action}, got {report_action}"

    try:
        expected_action_n, expected_args_n = _normalize_mobility_command(expected_action, expected_args)
        report_action_n, report_args_n = _normalize_mobility_command(report_action, report_args)
    except Exception as e:
        return False, f"command normalization failed: {e}"

    if expected_action_n != report_action_n:
        return False, f"normalized action mismatch: expected {expected_action_n}, got {report_action_n}"

    if expected_args_n != report_args_n:
        return False, "normalized command args mismatch"

    return True, ""

def _s1_lock_key(scanner: str) -> str:
    return f"{config.KEY_PREFIX}scanner:{scanner}:mobility:s1_event_lock"

def _acquire_s1_lock(scanner: str) -> str:
    owner = uuid.uuid4().hex
    ok = config.r.set(_s1_lock_key(scanner), owner, nx=True, ex=S1_EVENT_LOCK_TTL_SEC)
    return owner if ok else ""

def _release_s1_lock(scanner: str, owner: str) -> None:
    key = _s1_lock_key(scanner)
    try:
        cur = config.r.get(key) or ""
        if isinstance(cur, bytes):
            cur = cur.decode("utf-8", errors="ignore")
        if str(cur) == str(owner):
            config.r.delete(key)
    except Exception:
        pass

def _start_s1_timer(scanner: str) -> None:
    _cancel_s1_timer(scanner)

    token = uuid.uuid4().hex
    utility._hset_many(
        key_time(scanner),
        {
            "s1_timer_token": token,
            "s1_timer_started_at": utility.local_ts(),
        },
    )

    t = threading.Timer(S1_REPORT_TIMEOUT_SEC, _s1_timeout_callback, args=(scanner, token))
    t.daemon = True
    _S1_TIMERS[scanner] = t
    t.start()

def _cancel_s1_timer(scanner: str) -> None:
    t = _S1_TIMERS.pop(scanner, None)
    if t is not None:
        try:
            t.cancel()
        except Exception:
            pass

    utility._hset_many(
        key_time(scanner),
        {
            "s1_timer_token": "",
            "s1_timer_started_at": "",
        },
    )

def _s1_timeout_callback(scanner: str, token: str) -> None:
    process_s1_event(scanner, source="timeout", timer_token=token)


# ===== s2evaluating_policy =====

def s2evaluating_policy(scanner: str) -> Dict[str, Any]:
    entry_reason = _get_s2_entry_reason(scanner)

    # ---------------------------------------------------------
    # Special entry: busy retry timer fired
    # ---------------------------------------------------------    
    if entry_reason == "busy_retry_timer":
        _clear_s2_entry_reason(scanner)

        last_action = utility._hget(key_pose(scanner), "last_planned_command_action", "")
        last_args_raw = utility._hget(key_pose(scanner), "last_planned_command_args_json", "")

        if not last_action or not last_args_raw:
            _cancel_busy_retry_timer(scanner)
            _set_state(scanner, S7_STOPPED, "MOBILITY_BUSY retry missing previous command")
            return s7stopped(scanner)

        try:
            last_args = json.loads(last_args_raw) if isinstance(last_args_raw, str) else last_args_raw
        except Exception as e:
            _cancel_busy_retry_timer(scanner)
            _set_state(
                scanner,
                S7_STOPPED,
                f"MOBILITY_BUSY retry bad previous args json: {type(e).__name__}: {e}",
            )
            return s7stopped(scanner)

        if not isinstance(last_args, dict):
            _cancel_busy_retry_timer(scanner)
            _set_state(
                scanner,
                S7_STOPPED,
                f"MOBILITY_BUSY retry previous args not dict: {type(last_args).__name__}",
            )
            return s7stopped(scanner)

        try:
            last_action, last_args = _normalize_mobility_command(last_action, last_args)
        except Exception as e:
            _cancel_busy_retry_timer(scanner)
            _set_state(
                scanner,
                S7_STOPPED,
                f"MOBILITY_BUSY retry bad previous command: {type(e).__name__}: {e}",
            )
            return s7stopped(scanner)

        _save_outgoing_command_preview(
            scanner,
            action=last_action,
            args=last_args,
            source="retry_busy",
        )

        _save_last_issued_command(
            scanner,
            action=last_action,
            args=last_args,
        )

        _cancel_busy_retry_timer(scanner)
        _set_state(scanner, S1_WAITING_REPORT, "busy retry command reissued")
        _start_s1_timer(scanner)

        return {
            "state": S1_WAITING_REPORT,
            "status": "retry",
            "detail": "busy retry command reissued",
        }

    # ---------------------------------------------------------
    # Normal entry: evaluate latest report policy
    # ---------------------------------------------------------
    result = _s2_evaluate_policy(scanner)

    transition_to = result["transition_to"]
    _set_state(scanner, transition_to, result["detail"])

    if transition_to in (S0_IDLE, S1_WAITING_REPORT, S2_EVALUATING_POLICY):
        return {
            "state": transition_to,
            "status": result["status"],
            "detail": result["detail"],
        }

    return run_state_machine(scanner)

def _s2_evaluate_policy(scanner: str) -> Dict[str, Any]:
    report = _load_report_json(scanner)

    last_exec_status = str(report.get("last_exec_status") or "").strip()
    last_error_code = str(report.get("last_error_code") or "").strip()
    last_error_detail = str(report.get("last_error_detail") or "").strip()

    state_hash = key_state(scanner)
    old_retry_count = int(utility._hget(state_hash, "retry_count", "0") or "0")
    old_collision_veto_count = int(utility._hget(state_hash, "collision_veto_count", "0") or "0")
    old_busy_count = int(utility._hget(state_hash, "busy_count", "0") or "0")
    old_exec_fail_count = int(utility._hget(state_hash, "exec_fail_count", "0") or "0")

    out = {
        "last_error_code": last_error_code,
        "last_error_detail": last_error_detail[:300],
        "need_location_retry": "false",
        "stop_experiment": "false",
        "stop_reason": "",
        "robot_safety_state": "NORMAL",
        "retry_count": str(old_retry_count),
        "collision_veto_count": str(old_collision_veto_count),
        "busy_count": str(old_busy_count),
        "exec_fail_count": str(old_exec_fail_count),
    }

    # Healthy path
    if not last_error_code and last_exec_status.lower() in ("completed", "accepted", "ok", ""):
        out.update({
            "need_location_retry": "false",
            "stop_experiment": "false",
            "stop_reason": "",
            "robot_safety_state": "NORMAL",
            "retry_count": "0",
            "collision_veto_count": "0",
            "busy_count": "0",
            "exec_fail_count": "0",
        })
        utility._hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "ok",
            "transition_to": S3_SOLVING_TRUE_LOCATION,
            "detail": "healthy mobility report",
        }

    # No-tag-visible path
    # This is treated as normal operation for sparse-tag deployment.
    # Allow S3 to decide whether propagation is safe.
    if last_error_code in NO_TAG_OK_CODES:
        out.update({
            "need_location_retry": "false",
            "stop_experiment": "false",
            "stop_reason": "",
            "robot_safety_state": "NORMAL_NO_TAG",
            "retry_count": "0",
        })
        utility._hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "ok",
            "transition_to": S3_SOLVING_TRUE_LOCATION,
            "detail": f"{last_error_code}: continue to s3 for propagation decision",
        }

    # Immediate stop
    if last_error_code in IMMEDIATE_STOP_ERROR_CODES:
        out.update({
            "need_location_retry": "false",
            "stop_experiment": "true",
            "stop_reason": last_error_code,
            "robot_safety_state": "UNSAFE_STOP",
        })
        utility._hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": f"immediate stop due to {last_error_code}",
        }

    # Busy
    if last_error_code == "MOBILITY_BUSY":
        busy_count = old_busy_count + 1
        unexpected_sum = _unexpected_event_sum(
            busy_count=busy_count,
            collision_veto_count=old_collision_veto_count,
            exec_fail_count=old_exec_fail_count,
        )

        if unexpected_sum >= UNEXPECTED_EVENT_SUM_LIMIT:
            out.update({
                "busy_count": str(busy_count),
                "need_location_retry": "false",
                "stop_experiment": "true",
                "stop_reason": "UNEXPECTED_EVENT_SUM_LIMIT",
                "robot_safety_state": "UNSAFE_STOP",
            })
            utility._hset_many(state_hash, out)
            _save_policy_time(scanner)
            _cancel_busy_retry_timer(scanner)
            _clear_s2_entry_reason(scanner)
            return {
                "status": "stop",
                "transition_to": S7_STOPPED,
                "detail": f"MOBILITY_BUSY sum={unexpected_sum}",
            }

        out.update({
            "busy_count": str(busy_count),
            "need_location_retry": "false",
            "stop_experiment": "false",
            "stop_reason": "",
            "robot_safety_state": "WAITING_PREVIOUS_MOTION",
        })
        utility._hset_many(state_hash, out)
        _save_policy_time(scanner)

        _set_s2_entry_reason(scanner, "busy_retry_timer")
        _start_busy_retry_timer(scanner)

        return {
            "status": "wait",
            "transition_to": S2_EVALUATING_POLICY,
            "detail": f"MOBILITY_BUSY sum={unexpected_sum}; busy retry timer started",
        }

    # Collision vetoes
    if last_error_code in COLLISION_ERROR_CODES:
        veto_count = old_collision_veto_count + 1
        unexpected_sum = _unexpected_event_sum(
            busy_count=old_busy_count,
            collision_veto_count=veto_count,
            exec_fail_count=old_exec_fail_count,
        )

        out.update({
            "collision_veto_count": str(veto_count),
            "need_location_retry": "true",
            "stop_experiment": "true" if unexpected_sum >= UNEXPECTED_EVENT_SUM_LIMIT else "false",
            "stop_reason": "UNEXPECTED_EVENT_SUM_LIMIT" if unexpected_sum >= UNEXPECTED_EVENT_SUM_LIMIT else "",
            "robot_safety_state": "UNSAFE_STOP" if unexpected_sum >= UNEXPECTED_EVENT_SUM_LIMIT else "COLLISION_BLOCKED",
        })
        utility._hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "stop" if unexpected_sum >= UNEXPECTED_EVENT_SUM_LIMIT else "retry",
            "transition_to": S7_STOPPED if unexpected_sum >= UNEXPECTED_EVENT_SUM_LIMIT else S4_WAITING_LOCATION_RETRY,
            "detail": f"{last_error_code} sum={unexpected_sum}",
        }

    # Recoverable failures
    if last_error_code in RECOVERABLE_ERROR_CODES:
        retry_count = old_retry_count + 1
        exec_fail_count = old_exec_fail_count

        if last_error_code in ("MOVE_EXEC_FAIL", "TURN_EXEC_FAIL"):
            exec_fail_count += 1

        unexpected_sum = _unexpected_event_sum(
            busy_count=old_busy_count,
            collision_veto_count=old_collision_veto_count,
            exec_fail_count=exec_fail_count,
        )

        stop = unexpected_sum >= UNEXPECTED_EVENT_SUM_LIMIT

        out.update({
            "retry_count": str(retry_count),
            "exec_fail_count": str(exec_fail_count),
            "need_location_retry": "true",
            "stop_experiment": "true" if stop else "false",
            "stop_reason": "UNEXPECTED_EVENT_SUM_LIMIT" if stop else "",
            "robot_safety_state": "UNSAFE_STOP" if stop else "LOCATION_RECOVERY_NEEDED",
        })
        utility._hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "stop" if stop else "retry",
            "transition_to": S7_STOPPED if stop else S4_WAITING_LOCATION_RETRY,
            "detail": f"{last_error_code} sum={unexpected_sum}",
        }
    
    # Unknown error code -> conservative stop
    if last_error_code:
        out.update({
            "need_location_retry": "false",
            "stop_experiment": "true",
            "stop_reason": f"UNKNOWN_ERROR_CODE:{last_error_code}",
            "robot_safety_state": "UNSAFE_STOP",
        })
        utility._hset_many(state_hash, out)
        _save_policy_time(scanner)
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": f"unknown error code {last_error_code}",
        }

    # Strange status but no error code -> retry once
    retry_count = old_retry_count + 1
    stop = retry_count >= LOCATION_RETRY_LIMIT

    out.update({
        "retry_count": str(retry_count),
        "need_location_retry": "true",
        "stop_experiment": "true" if stop else "false",
        "stop_reason": "STATUS_RETRY_EXCEEDED" if stop else "",
        "robot_safety_state": "UNSAFE_STOP" if stop else "LOCATION_RECOVERY_NEEDED",
    })
    utility._hset_many(state_hash, out)
    _save_policy_time(scanner)
    return {
        "status": "stop" if stop else "retry",
        "transition_to": S7_STOPPED if stop else S4_WAITING_LOCATION_RETRY,
        "detail": f"unexpected status '{last_exec_status}'",
    }

def _unexpected_event_sum(
    busy_count: int,
    collision_veto_count: int,
    exec_fail_count: int,
) -> int:
    return int(busy_count) + int(collision_veto_count) + int(exec_fail_count)

def _start_busy_retry_timer(scanner: str) -> None:
    _cancel_busy_retry_timer(scanner)

    token = uuid.uuid4().hex
    utility._hset_many(
        key_time(scanner),
        {
            "busy_retry_token": token,
            "busy_retry_started_at": utility.local_ts(),
        },
    )

    t = threading.Timer(MOBILITY_BUSY_RETRY_WAIT_SEC, _busy_retry_timeout_callback, args=(scanner, token))
    t.daemon = True
    _BUSY_RETRY_TIMERS[scanner] = t
    t.start()

def _cancel_busy_retry_timer(scanner: str) -> None:
    t = _BUSY_RETRY_TIMERS.pop(scanner, None)
    if t is not None:
        try:
            t.cancel()
        except Exception:
            pass

    utility._hset_many(
        key_time(scanner),
        {
            "busy_retry_token": "",
            "busy_retry_started_at": "",
        },
    )

def _busy_retry_timeout_callback(scanner: str, token: str) -> None:
    """
    Busy-retry timer callback.

    Important:
        Do NOT clear s2_entry_reason here.

    Reason:
        s2evaluating_policy() uses s2_entry_reason == "busy_retry_timer"
        to enter the special retry path that reissues the previous command.

        If this callback clears s2_entry_reason before run_state_machine(),
        S2 will wrongly re-evaluate the same stored MOBILITY_BUSY report,
        increment busy_count again, and may stop at sum=2.

    Ownership checks are done here.
    Cleanup is done inside s2evaluating_policy() after it consumes the marker.
    """
    current_state = _get_state(scanner)
    current_token = utility._hget(key_time(scanner), "busy_retry_token", "")
    entry_reason = _get_s2_entry_reason(scanner)

    # Validate ownership.
    if current_state != S2_EVALUATING_POLICY:
        return

    if str(current_token or "") != str(token):
        return

    if entry_reason != "busy_retry_timer":
        return

    # Let s2evaluating_policy() consume the busy_retry_timer marker.
    # It will:
    #   - clear s2_entry_reason
    #   - reissue the previous command
    #   - cancel busy retry timer
    #   - enter S1
    run_state_machine(scanner)

def _set_s2_entry_reason(scanner: str, reason: str) -> None:
    utility._hset_many(
        key_state(scanner),
        {
            "s2_entry_reason": str(reason or ""),
        },
    )

def _get_s2_entry_reason(scanner: str) -> str:
    return str(utility._hget(key_state(scanner), "s2_entry_reason", "") or "").strip()

def _clear_s2_entry_reason(scanner: str) -> None:
    utility._hset_many(
        key_state(scanner),
        {
            "s2_entry_reason": "",
        },
    )


def _last_issued_action(scanner: str) -> str:
    return str(utility._hget(key_pose(scanner), "last_planned_command_action", "") or "").strip()


# ===== s3solving_true_location =====

def s3solving_true_location(scanner: str) -> Dict[str, Any]:
    loc = _s3_solve_true_location(scanner)

    # ---------------------------------------------------------
    # Case A: AprilTag solve succeeds -> update true and continue to S5
    # ---------------------------------------------------------
    if loc.get("location_ok") is True:
        _s3_save_true_location(scanner, loc)
        _update_10s_report(scanner)

        # A pure mobility.report.location command is a script-run precondition.
        # After it solves the physical true pose, planned must be reset to
        # that same pose.  Do not continue to S5, otherwise S5 may build a
        # correction toward an old planned_location_json.
        if _last_issued_action(scanner) == "mobility.report.location":
            planned = {
                "location_ok": True,
                "x_m": float(loc["x_m"]),
                "y_m": float(loc["y_m"]),
                "heading_deg": utility._deg_norm_360(float(loc["heading_deg"])),
            }
            _save_planned(scanner, planned)
            _clear_pending_sequence(scanner)
            _clear_outgoing_command_preview(scanner)
            _reset_correction_counter(scanner)
            utility._hset_many(
                key_state(scanner),
                {
                    "need_location_retry": "false",
                    "retry_count": "0",
                    "collision_veto_count": "0",
                    "busy_count": "0",
                    "exec_fail_count": "0",
                    "stop_experiment": "false",
                    "stop_reason": "",
                    "robot_safety_state": "NORMAL",
                    "last_error_code": "",
                    "last_error_detail": "",
                    "s2_entry_reason": "",
                },
            )
            _set_state(scanner, S0_IDLE, "location precondition solved; planned=true")
            return s0idle(scanner)

        _set_state(scanner, S5_COMPUTING_CORRECTION, "true location solved")
        return run_state_machine(scanner)

    # ---------------------------------------------------------
    # Case B0: location precondition failed
    # ---------------------------------------------------------
    # A failed mobility.report.location must not propagate old true pose and
    # must not continue to S5.  It should request a location retry.
    if _last_issued_action(scanner) == "mobility.report.location":
        detail = loc.get("detail", "location precondition failed")
        utility._hset_many(
            key_state(scanner),
            {
                "need_location_retry": "true",
                "robot_safety_state": "LOCATION_RECOVERY_NEEDED",
                "true_propagation_applied": "false",
                "true_propagation_detail": detail,
                "true_propagation_time": utility.local_ts(),
            },
        )
        _set_state(scanner, S4_WAITING_LOCATION_RETRY, f"location precondition solve failed: {detail}")
        return run_state_machine(scanner)

    # ---------------------------------------------------------
    # Case B: no AprilTag solve -> propagate true by last issued command
    # If propagation succeeds, continue to S5 as well.
    # S3 no longer decides "done"; S5 owns that responsibility now.
    # ---------------------------------------------------------
    if _propagation_allowed(scanner) and _should_propagate_true(scanner):
        propagated = _propagate_true_by_last_command(scanner)
        if _is_loc_ok(propagated):
            utility._hset_many(
                key_state(scanner),
                {
                    "true_propagation_applied": "true",
                    "true_propagation_detail": loc.get("detail", ""),
                    "true_propagation_time": utility.local_ts(),
                },
            )
            _update_10s_report(scanner)
            _set_state(scanner, S5_COMPUTING_CORRECTION, "true propagated, continue to s5")
            return run_state_machine(scanner)

    # ---------------------------------------------------------
    # Case C: cannot solve and cannot propagate -> retry path
    # ---------------------------------------------------------
    propagation_reason = loc.get("detail", "")
    if not _propagation_allowed(scanner):
        propagation_reason = f"{propagation_reason}; propagation disabled by unexpected-event history"
    elif not _should_propagate_true(scanner):
        propagation_reason = f"{propagation_reason}; propagation precondition failed"

    utility._hset_many(
        key_state(scanner),
        {
            "true_propagation_applied": "false",
            "true_propagation_detail": propagation_reason,
            "true_propagation_time": utility.local_ts(),
        },
    )
    _set_state(scanner, S4_WAITING_LOCATION_RETRY, f"solve failed: {propagation_reason}")
    return run_state_machine(scanner)

def _s3_solve_true_location(scanner: str) -> Dict[str, Any]:
    tag_map = _load_tag_map()
    tags_map = tag_map.get("tags") or {}
    if not isinstance(tags_map, dict):
        return {
            "location_ok": False,
            "detail": "invalid tag_map_json",
            "tags_used": [],
            "tag_count": 0,
            "solver_stage": "single_tag",
            "source": "apriltag",
            "updated_at": utility.local_ts(),
        }

    visible = _s3_extract_visible_tags(scanner)
    if not visible:
        return {
            "location_ok": False,
            "detail": "no usable apriltag observation in latest report",
            "tags_used": [],
            "tag_count": 0,
            "solver_stage": "single_tag",
            "source": "apriltag",
            "updated_at": utility.local_ts(),
        }

    cands = []
    for obs in visible:
        tag_world = tags_map.get(str(obs["id"]))
        if not isinstance(tag_world, dict):
            continue
        try:
            cands.append(_s3_solve_single_tag(obs, tag_world))
        except Exception:
            continue

    if not cands:
        return {
            "location_ok": False,
            "detail": "visible tags not found in tag_map_json",
            "tags_used": [],
            "tag_count": 0,
            "solver_stage": "single_tag",
            "source": "apriltag",
            "updated_at": utility.local_ts(),
        }

    return _s3_fuse_candidates(cands)

def _s3_extract_visible_tags(scanner: str) -> list[Dict[str, Any]]:
    """
    Parse dual-camera observations from latest mobility report.

    AprilTag calibrated measurements are interpreted as measurements
    at the camera center.

    Mechanical camera offsets are carried forward for later conversion
    from camera position to robot-center position.
    """

    report = _load_report_json(scanner)

    loc = report.get("last_location_result") or {}
    if not isinstance(loc, dict) or not bool(loc.get("ok")):
        return []

    apr = loc.get("apriltag") or {}
    if not isinstance(apr, dict) or not bool(apr.get("ok")):
        return []

    tags = apr.get("tags") or []
    if not isinstance(tags, list):
        return []

    camera_cfg = {
        "front": {
            "heading_offset_deg": 0.0,
            "forward_offset_m": 0.055,
        },
        "rear": {
            "heading_offset_deg": 180.0,
            "forward_offset_m": -0.055,
        },
    }

    out = []

    for t in tags:
        if not isinstance(t, dict):
            continue

        try:
            camera_role = str(t.get("camera_role") or "").strip().lower()
            if camera_role not in camera_cfg:
                continue

            # KEEP calibrated_pose exactly as today
            pose = t.get("calibrated_pose") or {}
            if not isinstance(pose, dict):
                continue

            angle_deg_cw = float(pose["angle_deg"])

            out.append({
                "id": int(t["id"]),

                "camera_role": camera_role,

                "camera_offset_deg":
                    float(camera_cfg[camera_role]["heading_offset_deg"]),

                "camera_forward_offset_m":
                    float(camera_cfg[camera_role]["forward_offset_m"]),

                "distance_m":
                    float(pose["distance_m"]),

                "angle_deg_cw":
                    angle_deg_cw,

                "yaw_deg":
                    float(pose["yaw_deg"]),

                "bearing_robot_deg_ccw":
                    utility._wrap_angle_deg(
                        float(camera_cfg[camera_role]["heading_offset_deg"])
                        - angle_deg_cw
                    ),

                "snapshot_path":
                    str(t.get("snapshot_path") or ""),
            })

        except Exception:
            continue

    return out

def _s3_solve_single_tag(
    obs: Dict[str, Any],
    tag_world: Dict[str, Any]
) -> Dict[str, Any]:

    tag_x = float(tag_world["x_m"])
    tag_y = float(tag_world["y_m"])
    tag_yaw_world = float(tag_world["yaw_deg"])

    distance_m = float(obs["distance_m"])
    angle_deg_cw = float(obs["angle_deg_cw"])
    yaw_deg = float(obs["yaw_deg"])

    camera_role = str(obs.get("camera_role") or "")
    camera_offset_deg = float(obs.get("camera_offset_deg") or 0.0)

    camera_forward_offset_m = float(
        obs.get("camera_forward_offset_m") or 0.0
    )

    # ---------------------------------------------------------
    # Solve robot heading
    # ---------------------------------------------------------

    yaw_corrected_deg = yaw_deg - angle_deg_cw

    camera_heading_deg = utility._deg_norm_360(
        tag_yaw_world + 180.0 - yaw_corrected_deg
    )

    robot_heading_deg = utility._deg_norm_360(
        camera_heading_deg - camera_offset_deg
    )

    # ---------------------------------------------------------
    # Bearing from robot toward tag
    # ---------------------------------------------------------

    bearing_robot_to_tag_deg_ccw = utility._wrap_angle_deg(
        camera_offset_deg - angle_deg_cw
    )

    world_bearing_deg = utility._deg_norm_360(
        robot_heading_deg + bearing_robot_to_tag_deg_ccw
    )

    world_bearing_rad = utility._deg_to_rad(world_bearing_deg)

    # ---------------------------------------------------------
    # First solve CAMERA position
    # ---------------------------------------------------------

    camera_x = tag_x - distance_m * math.cos(world_bearing_rad)
    camera_y = tag_y - distance_m * math.sin(world_bearing_rad)

    # ---------------------------------------------------------
    # Convert CAMERA position -> ROBOT CENTER position
    # ---------------------------------------------------------

    robot_heading_rad = utility._deg_to_rad(robot_heading_deg)

    robot_x = (
        camera_x
        - camera_forward_offset_m * math.cos(robot_heading_rad)
    )

    robot_y = (
        camera_y
        - camera_forward_offset_m * math.sin(robot_heading_rad)
    )

    return {
        "tag_id": int(obs["id"]),

        "camera_role": camera_role,

        "camera_offset_deg": camera_offset_deg,

        "camera_forward_offset_m": camera_forward_offset_m,

        "camera_x_m": camera_x,
        "camera_y_m": camera_y,

        "x_m": robot_x,
        "y_m": robot_y,

        "heading_deg": robot_heading_deg,

        "distance_m": distance_m,

        "angle_deg_cw": angle_deg_cw,

        "yaw_deg": yaw_deg,

        "bearing_robot_deg_ccw":
            bearing_robot_to_tag_deg_ccw,

        "snapshot_path":
            str(obs.get("snapshot_path") or ""),

        "yaw_corrected_deg":
            yaw_corrected_deg,
    }

def _s3_candidate_pos_distance(c0: Dict[str, Any], c1: Dict[str, Any]) -> float:
    return math.hypot(float(c0["x_m"]) - float(c1["x_m"]), float(c0["y_m"]) - float(c1["y_m"]))

def _s3_candidate_heading_distance(c0: Dict[str, Any], c1: Dict[str, Any]) -> float:
    return abs(_angle_diff_deg(float(c0["heading_deg"]), float(c1["heading_deg"])))

def _s3_candidate_weight(c: Dict[str, Any]) -> float:
    """
    Observation quality weight.

    Higher confidence:
      - tag near camera center
      - shorter distance

    Lower confidence:
      - tag near FOV edge
      - longer distance

    Never returns zero for a valid candidate.
    """
    angle = abs(float(c.get("angle_deg_cw", 0.0)))
    dist = max(0.2, float(c.get("distance_m", 1.0)))

    angle_w = max(0.15, 1.0 - angle / 45.0)
    dist_w = 1.0 / math.sqrt(dist)

    return float(angle_w * dist_w)

def _s3_weighted_circular_mean_deg(vals: list[float], weights: list[float]) -> float:
    sx = 0.0
    sy = 0.0

    for v, w in zip(vals, weights):
        rad = math.radians(float(v))
        sx += float(w) * math.cos(rad)
        sy += float(w) * math.sin(rad)

    if abs(sx) < 1e-12 and abs(sy) < 1e-12:
        return utility._deg_norm_360(float(vals[0]))

    return utility._deg_norm_360(math.degrees(math.atan2(sy, sx)))

def _s3_weighted_mean(vals: list[float], weights: list[float]) -> float:
    wsum = sum(float(w) for w in weights)
    if wsum <= 0:
        return sum(float(v) for v in vals) / max(1, len(vals))
    return sum(float(v) * float(w) for v, w in zip(vals, weights)) / wsum

def _s3_candidate_residuals(cands: list[Dict[str, Any]], x: float, y: float, h: float) -> list[Dict[str, Any]]:
    out = []
    for c in cands:
        d = dict(c)
        d["position_residual_m"] = float(math.hypot(float(c["x_m"]) - x, float(c["y_m"]) - y))
        d["heading_residual_deg"] = float(abs(_angle_diff_deg(float(c["heading_deg"]), h)))
        out.append(d)
    return out

def _s3_fuse_candidates(cands: list[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Best-effort AprilTag candidate fusion.

    Calibration philosophy:
    - Return the best pose estimate whenever possible.
    - Do not fail just because candidates are noisy.
    - Remove only hard outliers that are fundamentally inconsistent.
    - Keep full diagnostics so calibration can reveal which tags/cameras are weak.
    """
    if not cands:
        return {
            "location_ok": False,
            "detail": "no candidates",
            "tags_used": [],
            "tag_count": 0,
            "solver_stage": "best_effort_fusion",
            "source": "apriltag",
            "updated_at": utility.local_ts(),
        }

    if len(cands) == 1:
        c = cands[0]
        return {
            "location_ok": True,
            "x_m": float(c["x_m"]),
            "y_m": float(c["y_m"]),
            "heading_deg": utility._deg_norm_360(float(c["heading_deg"])),
            "source": "apriltag",
            "tags_used": [c["tag_id"]],
            "tag_count": 1,
            "solver_stage": "single_tag_best_effort",
            "updated_at": utility.local_ts(),
            "detail": "single-tag estimate",
            "candidates": cands,
            "used_candidates": cands,
            "hard_outliers": [],
            "soft_outliers": [],
        }

    # ---------------------------------------------------------
    # Pass 1: initial weighted estimate from all candidates
    # ---------------------------------------------------------
    weights0 = [_s3_candidate_weight(c) for c in cands]

    x0 = _s3_weighted_mean([float(c["x_m"]) for c in cands], weights0)
    y0 = _s3_weighted_mean([float(c["y_m"]) for c in cands], weights0)
    h0 = _s3_weighted_circular_mean_deg([float(c["heading_deg"]) for c in cands], weights0)

    diag0 = _s3_candidate_residuals(cands, x0, y0, h0)

    # ---------------------------------------------------------
    # Pass 2: remove only hard outliers.
    #
    # These thresholds should be loose. They mean:
    # "This candidate is too far from the voting majority to be treated
    #  as noisy data."
    # ---------------------------------------------------------
    HARD_POS_OUTLIER_M = max(1.0, 3.0 * MULTI_TAG_POS_THRESH_M)
    HARD_HEADING_OUTLIER_DEG = max(45.0, 2.5 * MULTI_TAG_HEADING_THRESH_DEG)

    kept = []
    hard_outliers = []

    for c, d in zip(cands, diag0):
        pos_bad = float(d["position_residual_m"]) > HARD_POS_OUTLIER_M
        heading_bad = float(d["heading_residual_deg"]) > HARD_HEADING_OUTLIER_DEG

        if pos_bad and heading_bad:
            hard_outliers.append(d)
        else:
            kept.append(c)

    # Never allow hard-outlier filtering to remove everything.
    if not kept:
        kept = list(cands)
        hard_outliers = []

    # ---------------------------------------------------------
    # Pass 3: final weighted estimate from kept candidates
    # ---------------------------------------------------------
    weights = [_s3_candidate_weight(c) for c in kept]

    x = _s3_weighted_mean([float(c["x_m"]) for c in kept], weights)
    y = _s3_weighted_mean([float(c["y_m"]) for c in kept], weights)
    h = _s3_weighted_circular_mean_deg([float(c["heading_deg"]) for c in kept], weights)

    used_diag = _s3_candidate_residuals(kept, x, y, h)

    for d, w in zip(used_diag, weights):
        d["weight"] = float(w)

    soft_outliers = []
    for d in used_diag:
        if (
            float(d["position_residual_m"]) > MULTI_TAG_POS_THRESH_M
            or float(d["heading_residual_deg"]) > MULTI_TAG_HEADING_THRESH_DEG
        ):
            soft_outliers.append(d)

    detail_parts = []
    if hard_outliers:
        detail_parts.append(f"ignored {len(hard_outliers)} hard outlier(s)")
    if soft_outliers:
        detail_parts.append(f"{len(soft_outliers)} soft outlier(s) kept in fusion")
    if not detail_parts:
        detail_parts.append("best-effort fusion ok")

    return {
        "location_ok": True,
        "x_m": float(x),
        "y_m": float(y),
        "heading_deg": float(h),
        "source": "apriltag",
        "tags_used": [c["tag_id"] for c in kept],
        "tag_count": len(kept),
        "solver_stage": "best_effort_fusion",
        "updated_at": utility.local_ts(),
        "detail": "; ".join(detail_parts),
        "candidates": cands,
        "used_candidates": used_diag,
        "hard_outliers": hard_outliers,
        "soft_outliers": soft_outliers,
    }

def _s3_save_true_location(scanner: str, loc: Dict[str, Any]) -> None:
    utility._hset_many(
        key_pose(scanner),
        {
            "true_location_json": loc,
        },
    )
    utility._hset_many(
        key_time(scanner),
        {
            "true_location_updated_at": utility.local_ts(),
        },
    )

def _propagation_allowed(scanner: str) -> bool:
    state_hash = key_state(scanner)

    busy_count = int(utility._hget(state_hash, "busy_count", "0") or "0")
    collision_veto_count = int(utility._hget(state_hash, "collision_veto_count", "0") or "0")
    exec_fail_count = int(utility._hget(state_hash, "exec_fail_count", "0") or "0")

    return _unexpected_event_sum(
        busy_count=busy_count,
        collision_veto_count=collision_veto_count,
        exec_fail_count=exec_fail_count,
    ) == 0


# ===== s4waiting_location_retry =====

def s4waiting_location_retry(scanner: str) -> Dict[str, Any]:
    result = _s4_handle_location_retry(scanner)

    transition_to = result["transition_to"]
    _set_state(scanner, transition_to, result["detail"])

    if transition_to == S1_WAITING_REPORT:
        _start_s1_timer(scanner)
        out = {
            "state": transition_to,
            "status": result["status"],
            "detail": result["detail"],
        }
        if "last_retry_requested_at" in result:
            out["last_retry_requested_at"] = result["last_retry_requested_at"]
        return out

    if transition_to in (S0_IDLE, S4_WAITING_LOCATION_RETRY):
        return {
            "state": transition_to,
            "status": result["status"],
            "detail": result["detail"],
        }

    return run_state_machine(scanner)

def _s4_handle_location_retry(scanner: str) -> Dict[str, Any]:
    state_hash = key_state(scanner)
    time_hash = key_time(scanner)

    retry_count = utility._to_int(utility._hget(state_hash, "retry_count", "0"), 0)
    stop_experiment = (utility._hget(state_hash, "stop_experiment", "false").lower() == "true")
    stop_reason = utility._hget(state_hash, "stop_reason", "")
    need_location_retry = (utility._hget(state_hash, "need_location_retry", "false").lower() == "true")

    # If policy already says stop, go straight to stopped
    if stop_experiment:
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": stop_reason or "policy requested stop",
        }

    # If no retry is needed anymore, return to policy phase caller flow
    if not need_location_retry:
        return {
            "status": "ok",
            "transition_to": S0_IDLE,
            "detail": "no location retry needed",
        }

    # Retry still allowed: issue mobility.report.location and wait for its report in S1
    if retry_count < LOCATION_RETRY_LIMIT:
        now_ts = utility.local_ts()
        new_retry_count = retry_count + 1

        action = "mobility.report.location"
        args = {}

        _save_outgoing_command_preview(
            scanner,
            action=action,
            args=args,
            source="location_retry",
        )

        _save_last_issued_command(
            scanner,
            action=action,
            args=args,
        )

        utility._hset_many(
            state_hash,
            {
                "retry_count": str(new_retry_count),
                "need_location_retry": "false",
                "outgoing_command_action": action,
                "outgoing_command_args_json": args,
                "outgoing_command_source": "location_retry",
                "outgoing_command_updated_at": now_ts,
            },
        )

        utility._hset_many(
            time_hash,
            {
                "last_retry_requested_at": now_ts,
            },
        )

        return {
            "status": "retry",
            "transition_to": S1_WAITING_REPORT,
            "detail": f"location retry issued, retry_count={new_retry_count}",
            "last_retry_requested_at": now_ts,
        }

    # Defensive fallback: retry exhausted -> stop
    return {
        "status": "stop",
        "transition_to": S7_STOPPED,
        "detail": "location retry exhausted",
    }


# ===== s5computing_correction/ compute-next-command =====

def s5computing_correction(scanner: str) -> Dict[str, Any]:
    result = _s5_compute_correction(scanner)

    transition_to = result["transition_to"]
    _set_state(scanner, transition_to, result["detail"])

    if transition_to == S0_IDLE:
        return {
            "state": transition_to,
            "status": result["status"],
            "detail": result["detail"],
            "error": result["error"],
            "pending_sequence": result["pending_sequence"],
            **({"correction_detail": result["correction_detail"]} if "correction_detail" in result else {}),
        }

    return run_state_machine(scanner)

def _s5_compute_correction(scanner: str) -> Dict[str, Any]:
    true_loc = _load_true(scanner)
    planned_loc = _load_planned(scanner)

    if not _is_loc_ok(planned_loc):
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": "planned location invalid in s5",
            "error": {},
            "pending_sequence": [],
        }

    if not _is_loc_ok(true_loc):
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": "true location invalid in s5",
            "error": {},
            "pending_sequence": [],
        }

    err = _pose_error(true_loc, planned_loc)
    dpos = float(err["dpos_m"])
    dhead = abs(float(err["dhead_deg"]))

    # ---------------------------------------------------------
    # If already close enough, no command is needed.
    # ---------------------------------------------------------
    if dpos <= config.MOBILITY_POS_IGNORE_THRESH_M and dhead <= config.MOBILITY_ANGLE_IGNORE_THRESH_DEG:
        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        return {
            "status": "ok",
            "transition_to": S0_IDLE,
            "detail": "already close enough to planned location",
            "error": err,
            "pending_sequence": [],
        }

    # ---------------------------------------------------------
    # Correction / command issue limit
    # Counter semantics:
    #   -1 : no command issued yet
    #    0 : initial modified command already issued
    #   >=1: true correction attempts
    # ---------------------------------------------------------
    if _get_correction_counter(scanner) >= CORRECTION_ATTEMPT_LIMIT:
        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        return {
            "status": "ok",
            "transition_to": S0_IDLE,
            "detail": f"correction limit reached ({CORRECTION_ATTEMPT_LIMIT})",
            "error": err,
            "pending_sequence": [],
        }

    # ---------------------------------------------------------
    # Build next command from current true -> current planned
    # ---------------------------------------------------------
    action, args, correction_detail = _build_command_from_true_to_planned(scanner)

    if not action:
        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        return {
            "status": "ok",
            "transition_to": S0_IDLE,
            "detail": "no command needed",
            "error": err,
            "pending_sequence": [],
            "correction_detail": correction_detail,
        }

    # ---------------------------------------------------------
    # Path safety belongs to S5 now.
    # Only commands with translational movement need path check.
    # Turn-only and location-report commands do not sweep a path.
    # ---------------------------------------------------------
    action, args = _normalize_mobility_command(action, args)

    needs_path_check = (
        action in (
            "mobility.turn_move_turn.forward",
            "mobility.turn_move_turn.backward",
        )
        and abs(float(args.get("distance_m", 0.0) or 0.0)) > 1e-9
    )

    if needs_path_check:
        tx = float(true_loc["x_m"])
        ty = float(true_loc["y_m"])

        simulated_target = _apply_mobility_command_to_pose(true_loc, action, args)
        px = float(simulated_target["x_m"])
        py = float(simulated_target["y_m"])

        path_ok, blocked, path_debug = _is_path_clear_debug(
            tx,
            ty,
            px,
            py,
            exclude_scanner=scanner,
        )

        if not path_ok:
            _clear_pending_sequence(scanner)
            _clear_outgoing_command_preview(scanner)

            return {
                "status": "stop",
                "transition_to": S7_STOPPED,
                "detail": (
                    f"path unsafe in s5, blocked={len(blocked)}, "
                    f"start_grid={path_debug.get('start', {}).get('grid')}, "
                    f"target_grid={path_debug.get('target', {}).get('grid')}, "
                    f"blocked_cells={path_debug.get('blocked_cells', [])[:10]}"
                ),
                "error": {
                    **err,
                    "path_debug": path_debug,
                    "action": action,
                    "args": args,
                    "simulated_target": simulated_target,
                },
                "pending_sequence": [],
                "correction_detail": correction_detail,
            }

    seq = [{"action": action, "args": args}]

    _save_pending_sequence(scanner, seq, "computed_by_s5")

    return {
        "status": "ok",
        "transition_to": S6_ISSUING_CORRECTION,
        "detail": f"computed command {action}",
        "error": err,
        "pending_sequence": seq,
        "correction_detail": correction_detail,
    }


# ===== s6issuing_correction =====

def s6issuing_correction(scanner: str) -> Dict[str, Any]:
    result = _s6_issue_correction(scanner)

    transition_to = result["transition_to"]
    _set_state(scanner, transition_to, result["detail"])

    if transition_to == S1_WAITING_REPORT:
        _start_s1_timer(scanner)
        return {
            "state": transition_to,
            "status": result["status"],
            "detail": result["detail"],
            "issued_command": result["issued_command"],
            **({"remaining_count": result["remaining_count"]} if "remaining_count" in result else {}),
            **({"issued_at": result["issued_at"]} if "issued_at" in result else {}),
            **({"correction_count": result["correction_count"]} if "correction_count" in result else {}),
        }

    if transition_to == S0_IDLE:
        return {
            "state": transition_to,
            "status": result["status"],
            "detail": result["detail"],
            "issued_command": result["issued_command"],
            **({"remaining_count": result["remaining_count"]} if "remaining_count" in result else {}),
            **({"issued_at": result["issued_at"]} if "issued_at" in result else {}),
            **({"correction_count": result["correction_count"]} if "correction_count" in result else {}),
        }

    return run_state_machine(scanner)

def _s6_issue_correction(scanner: str) -> Dict[str, Any]:
    seq = _load_pending_sequence(scanner)

    if not seq:
        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        return {
            "status": "ok",
            "transition_to": S0_IDLE,
            "detail": "no pending command to issue",
            "issued_command": {},
        }

    cmd = seq[0]
    rest = seq[1:]

    if not isinstance(cmd, dict):
        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": "invalid pending command",
            "issued_command": {},
        }

    action = str(cmd.get("action") or "").strip()
    args = cmd.get("args") or {}

    try:
        action, args = _normalize_mobility_command(action, args)
    except Exception as e:
        _clear_pending_sequence(scanner)
        _clear_outgoing_command_preview(scanner)
        return {
            "status": "stop",
            "transition_to": S7_STOPPED,
            "detail": f"invalid command at s6: {e}",
            "issued_command": {},
        }

    # ---------------------------------------------------------
    # S6 is the sole command-issuance state.
    # It must NOT update planned_location_json or true_location_json.
    # ---------------------------------------------------------

    # ---------------------------------------------------------
    # Save outgoing preview
    # ---------------------------------------------------------
    _save_outgoing_command_preview(
        scanner,
        action=action,
        args=args,
        source="mobility",
    )

    # ---------------------------------------------------------
    # Record last issued command
    # This is the command that later S3 may propagate from
    # if AprilTag verification is unavailable.
    # ---------------------------------------------------------
    issued_at = _save_last_issued_command(
        scanner,
        action=action,
        args=args,
    )

    # ---------------------------------------------------------
    # Increment command/correction counter
    # Counter semantics:
    #   -1 : before any issued command
    #    0 : initial modified command issued
    #   >=1: true correction attempts
    # ---------------------------------------------------------
    count = _inc_correction_counter(scanner)

    utility._hset_many(
        key_state(scanner),
        {
            "correction_attempt_count": str(count),
            "last_correction_issued_at": issued_at,
        },
    )

    # ---------------------------------------------------------
    # Save remaining sequence
    # ---------------------------------------------------------
    _save_pending_sequence(scanner, rest, "remaining_after_issue")

    return {
        "status": "ok",
        "transition_to": S1_WAITING_REPORT,
        "detail": f"issued command {action}",
        "issued_command": {
            "action": action,
            "args": args,
        },
        "remaining_count": len(rest),
        "issued_at": issued_at,
        "correction_count": count,
    }


# ===== s7stopped =====

def s7stopped(scanner: str) -> Dict[str, Any]:
    """
    Stop state for the entire experiment.

    Mobility-subsystem responsibility:
    1) write global stop key
    2) mark scanner as stopped
    3) clear mobility-local transient state (timers, pending sequence, preview)

    Outside mobility (NOT implemented here):
    - stop command dispatch
    - stop reports
    - stop traffic sessions
    """
    reason = utility._hget(key_state(scanner), "state_detail", "")
    reason = str(reason or "").strip() or "manual reset required"

    _save_stop(True, reason)

    _clear_pending_sequence(scanner)
    _clear_outgoing_command_preview(scanner)

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
            "s2_entry_reason": "",
            "need_location_retry": "false",
            "stop_experiment": "true",
            "stop_reason": reason,
            "robot_safety_state": "UNSAFE_STOP",
        },
    )

    _set_state(scanner, S7_STOPPED, reason)

    return {
        "state": S7_STOPPED,
        "status": "stopped",
        "detail": reason,
    }


# ===== state machine =====

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

