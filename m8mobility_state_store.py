"""
Mobility subsystem Redis-backed state store.

Division rule:
- Mobility-project-specific persistence helpers live here.
- Redis key builders, stop state, timestamps, reports, counters, and preview persistence live here.
- No path math, no command-model math, and no state-policy logic here.
"""
from typing import Dict, Any
import json
import config

from utility import _hget, _hget_json, _hset_many, local_ts


# ===== redis key helpers =====

def key_state(scanner: str) -> str:
    return f"{config.KEY_PREFIX}scanner:{scanner}:mobility:state"

def key_time(scanner: str) -> str:
    return f"{config.KEY_PREFIX}scanner:{scanner}:mobility:time"

def key_report(scanner: str) -> str:
    return f"{config.KEY_PREFIX}scanner:{scanner}:mobility:report"

def key_pose(scanner: str) -> str:
    return f"{config.KEY_PREFIX}scanner:{scanner}:mobility:pose"

KEY_STOP = f"{config.KEY_PREFIX}mobility:experiment_stop_state_json"


# ===== pose validity check =====

def _is_loc_ok(loc: Dict[str, Any]) -> bool:
    return isinstance(loc, dict) and loc.get("location_ok") is True


# ===== load/save pose =====

def _load_true(scanner: str) -> Dict[str, Any]:
    return _hget_json(key_pose(scanner), "true_location_json")

def _load_planned(scanner: str) -> Dict[str, Any]:
    return _hget_json(key_pose(scanner), "planned_location_json")

def _save_true(scanner: str, loc: Dict[str, Any]) -> None:
    _hset_many(
        key_pose(scanner),
        {
            "true_location_json": loc,
        },
    )
    _hset_many(
        key_time(scanner),
        {
            "true_location_updated_at": local_ts(),
        },
    )

def _save_planned(scanner: str, loc: Dict[str, Any]) -> None:
    _hset_many(
        key_pose(scanner),
        {
            "planned_location_json": loc,
        },
    )
    _hset_many(
        key_time(scanner),
        {
            "planned_location_updated_at": local_ts(),
        },
    )

# ===== state / stop helpers =====

def _set_state(scanner: str, state: str, detail: str = "") -> None:
    _hset_many(
        key_state(scanner),
        {
            "state": state,
            "state_updated_at": local_ts(),
            "state_detail": detail[:300],
        },
    )

def _load_stop() -> Dict[str, Any]:
    raw = config.r.get(KEY_STOP) or ""
    if not raw.strip():
        return {"stop": False, "reason": ""}
    try:
        return json.loads(raw)
    except Exception:
        return {"stop": False, "reason": ""}

def _save_stop(stop: bool, reason: str = "") -> None:
    config.r.set(
        KEY_STOP,
        json.dumps(
            {
                "stop": bool(stop),
                "reason": str(reason or "")[:300],
                "updated_at": local_ts(),
            },
            ensure_ascii=False,
        ),
    )
    
# ===== report / time helpers =====

def _load_report_json(scanner: str) -> Dict[str, Any]:
    return _hget_json(key_report(scanner), "last_mobility_report_json")

def _save_policy_time(scanner: str) -> None:
    _hset_many(
        key_time(scanner),
        {
            "policy_updated_at": local_ts(),
        },
    )

def _is_anchor_fresh(scanner: str) -> tuple[bool, str]:
    report_ts = _hget(key_time(scanner), "last_mobility_report_at", "")
    issued_ts = _hget(key_time(scanner), "last_planned_command_issued_at", "")

    if not report_ts:
        return False, "missing last_mobility_report_at"

    if issued_ts and report_ts < issued_ts:
        return False, "stale true_location (report older than last command)"

    return True, ""


# ===== pending sequence / command preview =====

def _save_pending_sequence(scanner: str, seq: list[Dict[str, Any]], reason: str = "") -> None:
    _hset_many(
        key_state(scanner),
        {
            "pending_sequence_json": seq,
            "pending_sequence_len": str(len(seq)),
            "pending_sequence_reason": reason,
        },
    )

def _load_pending_sequence(scanner: str) -> list[Dict[str, Any]]:
    raw = _hget(key_state(scanner), "pending_sequence_json", "")
    if not raw.strip():
        return []
    try:
        seq = json.loads(raw)
        return seq if isinstance(seq, list) else []
    except Exception:
        return []

def _clear_pending_sequence(scanner: str) -> None:
    _hset_many(
        key_state(scanner),
        {
            "pending_sequence_json": "",
            "pending_sequence_len": "0",
            "pending_sequence_reason": "",
        },
    )


# ===== issued-command tracking helpers =====

def _save_last_issued_command(scanner: str, action: str, args: Dict[str, Any]) -> str:
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
            "last_planned_command_action": action,
            "last_planned_command_args_json": args,
        },
    )

    return ts


# ===== preview/output persistence helpers =====

def _save_outgoing_command_preview(scanner: str, action: str, args: Dict[str, Any], source: str) -> None:
    """
    Unfinished temporary function
    Temporary placeholder before full queue abstraction.
    For Phase 1, save preview AND enqueue the command to the normal robot command stream.
    """
    now_ts = local_ts()

    _hset_many(
        key_state(scanner),
        {
            "outgoing_command_action": action,
            "outgoing_command_args_json": args,
            "outgoing_command_source": source,
            "outgoing_command_updated_at": now_ts,
        },
    )

    config.r.xadd(
        config.key_cmd_stream(scanner),
        {
            "category": "mobility",
            "action": action,
            "execute_at": now_ts,
            "created_at": now_ts,
            "args_json": json.dumps(args or {}, ensure_ascii=False),
        },
        maxlen=5000,
        approximate=True,
    )
    
def _clear_outgoing_command_preview(scanner: str) -> None:
    """
    Unfinished temporary function
    """
    _hset_many(
        key_state(scanner),
        {
            "outgoing_command_action": "",
            "outgoing_command_args_json": "",
            "outgoing_command_source": "",
            "outgoing_command_updated_at": "",
        },
    )


# ===== correction counter helpers =====

def _reset_correction_counter(scanner: str) -> None:
    _hset_many(
        key_state(scanner),
        {
            "correction_attempt_count": "-1",
        },
    )

def _inc_correction_counter(scanner: str) -> int:
    val = _hget(key_state(scanner), "correction_attempt_count", "0")
    try:
        n = int(val)
    except Exception:
        n = 0

    n += 1

    _hset_many(
        key_state(scanner),
        {
            "correction_attempt_count": str(n),
        },
    )

    return n

def _get_correction_counter(scanner: str) -> int:
    val = _hget(key_state(scanner), "correction_attempt_count", "0")
    try:
        return int(val)
    except Exception:
        return 0


# ===== 10-second mobility visibility =====

def _update_10s_report(scanner: str) -> None:
    true_loc = _load_true(scanner)

    payload = {
        "scanner": scanner,
        "time": local_ts(),
        "true_location": true_loc if _is_loc_ok(true_loc) else {},
    }

    _hset_many(
        key_report(scanner),
        {
            "last_10s_report_json": payload,
            "last_10s_report_at": payload["time"],
        },
    )
