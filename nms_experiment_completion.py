"""One sleeping completion worker per registration; no robot-poll dependency.

The Redis registration is durable. Threads are not: startup reconstructs pending
work, and process shutdown stops it. All lifecycle writers share startup_lock.
"""
import json
import logging
import threading
from datetime import timedelta

import config
import utility

_LOG = logging.getLogger(__name__)
_TERMINAL = {"completed", "finalized", "deleted", "cancelled", "canceled", "failed", "stopped"}
_IDENTITY = ("lab_id", "experiment_id", "session_id", "registered_at", "start_at", "end_at")
_guard = threading.RLock()
_worker = None
_stopping = False


def _text(value):
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _fields(raw):
    return {_text(k): _text(v) for k, v in raw.items()}


def lifecycle_lock():
    # Same lock already used by CSV registration. HTTP finalize timeout is 10 s.
    return config.r.lock(config.KEY_EXPERIMENT_REGISTRY + ":startup_lock", timeout=300)


def _identity(fields):
    return tuple(fields.get(k, "") for k in _IDENTITY)


def _current(xid):
    rows = config.r.xrange(config.KEY_EXPERIMENT_REGISTRY, min=xid, max=xid)
    return _fields(rows[0][1]) if rows else None


# Compare-and-replace the exact row atomically. XADD precedes XDEL so a failed
# append cannot destroy the pending registration. No Python snapshot can restore
# a registration that was removed or changed while the HTTP request was running.
_REPLACE = r"""
local rows = redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1])
if #rows ~= 1 then return false end
local current = rows[1][2]
local expected = cjson.decode(ARGV[2])
local count = 0
for k,v in pairs(expected) do count = count + 1 end
if #current ~= count * 2 then return false end
for i=1,#current,2 do
    if expected[current[i]] ~= current[i+1] then return false end
end
local args = {'XADD', KEYS[1], 'MAXLEN', '~', '200', '*'}
for i=3,#ARGV do args[#args+1] = ARGV[i] end
local newid = redis.call(unpack(args))
redis.call('XDEL', KEYS[1], ARGV[1])
return newid
"""


def complete_once(xid, expected_identity, notify):
    """Return (finished, result). Only the owner of the lifecycle lock can notify."""
    lock = lifecycle_lock()
    if not lock.acquire(blocking=False):
        return False, {"status": "busy"}
    try:
        fields = _current(xid)
        if fields is None or _identity(fields) != expected_identity:
            return True, {"status": "registration_changed", "registry_id": xid}
        if fields.get("state", "").lower().strip() in _TERMINAL:
            return True, {"status": "terminal", "registry_id": xid}
        deadline = utility.parse_local_dt(fields["end_at"]) + timedelta(
            seconds=int(getattr(config, "NORTHBOUND_EXPERIMENT_WRAPUP_SEC", 120)))
        now = utility.parse_local_dt(utility.local_ts())
        if now < deadline:
            return False, {"status": "not_due", "registry_id": xid}

        result = notify(fields)
        # The old transport helper treats non-JSON HTTP responses as ok. Such a
        # response is not explicit backend acceptance and must remain pending.
        if not isinstance(result, dict) or str(result.get("status", "")).lower() != "ok" or "raw_response" in result:
            return False, {"status": "notify_not_accepted", "registry_id": xid,
                           "experiment_id": fields.get("experiment_id"),
                           "web_datalake_notify": result}
        # If our lock expired/lost ownership, never write from an old snapshot.
        if not lock.owned():
            return False, {"status": "lock_lost", "registry_id": xid}
        now_s = utility.local_ts()
        updated = dict(fields, state="completed", final_result="success",
                       final_reason="planned_experiment_finished_after_wrapup",
                       finalized_at=now_s, finalize_notified_at=now_s,
                       web_finalize_notify_json=json.dumps(result, ensure_ascii=False))
        flat = [item for pair in updated.items() for item in pair]
        new_id = config.r.eval(_REPLACE, 1, config.KEY_EXPERIMENT_REGISTRY,
                               xid, json.dumps(fields), *flat)
        if not new_id:
            return True, {"status": "registration_changed", "registry_id": xid}
        return True, {"status": "finalized", "registry_id": xid,
                      "new_registry_id": _text(new_id),
                      "experiment_id": fields.get("experiment_id"),
                      "web_datalake_notify": result}
    finally:
        if lock.owned():
            lock.release()


def _record(result):
    status = result.get("status")
    if status in {"not_due", "busy", "terminal", "registration_changed"}:
        return
    try:
        config.r.set(config.KEY_EXPERIMENT_REGISTRY + ":completion:last_result",
                     json.dumps(dict(result, checked_at=utility.local_ts()), ensure_ascii=False))
    except Exception:
        _LOG.exception("Could not save experiment completion diagnostics")
    level = logging.INFO if status == "finalized" else logging.WARNING
    _LOG.log(level, "Experiment completion: %s", json.dumps(result, ensure_ascii=False))


class _Worker:
    def __init__(self, xid, fields):
        self.xid = _text(xid)
        self.identity = _identity(fields)
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, name="experiment-completion", daemon=True)

    def run(self):
        global _worker
        try:
            from m4Commands import _notify_web_experiment_finalize
            interval = max(1.0, float(getattr(config, "EXPERIMENT_COMPLETION_CHECK_SEC", 10)))
            while not self.stop.is_set():
                try:
                    done, result = complete_once(self.xid, self.identity, _notify_web_experiment_finalize)
                    _record(result)
                    if done:
                        return
                except Exception as exc:
                    _record({"status": "error", "registry_id": self.xid,
                             "detail": f"{type(exc).__name__}: {exc}"})
                self.stop.wait(interval)
        finally:
            with _guard:
                if _worker is self:
                    _worker = None


def start_registration(xid, raw_fields):
    """Called while registration owns lifecycle_lock; no permanent idle loop."""
    global _worker
    fields = _fields(raw_fields)
    if fields.get("state", "").strip().lower() in _TERMINAL:
        return
    if not fields.get("experiment_id") or not fields.get("end_at"):
        raise ValueError("Cannot start completion task: incomplete registration")
    utility.parse_local_dt(fields["end_at"])
    with _guard:
        if _stopping:
            raise RuntimeError("NMS is stopping")
        if _worker and _worker.xid == _text(xid) and _worker.identity == _identity(fields):
            return
        if _worker:
            _worker.stop.set()
        _worker = _Worker(xid, fields)
        _worker.thread.start()


def cancel_registration():
    """Caller holds lifecycle_lock: no finalization HTTP call is in flight."""
    with _guard:
        if _worker:
            _worker.stop.set()


def restore_pending():
    """Startup only. Do not guess between multiple pending registrations."""
    global _stopping
    with _guard:
        _stopping = False
    lock = lifecycle_lock()
    if not lock.acquire(blocking=True, blocking_timeout=15):
        raise RuntimeError("Experiment lifecycle busy during NMS startup; retry startup")
    try:
        pending = [(xid, _fields(raw)) for xid, raw in config.r.xrange(config.KEY_EXPERIMENT_REGISTRY)
                   if _fields(raw).get("state", "").strip().lower() not in _TERMINAL]
        if len(pending) > 1:
            raise RuntimeError("Multiple pending experiment registrations; refusing ambiguous recovery")
        if pending:
            start_registration(*pending[0])
    finally:
        lock.release()


def stop():
    """NMS shutdown: stop sleeping/retries and let any bounded HTTP call finish."""
    global _stopping
    with _guard:
        _stopping = True
        worker = _worker
        if worker:
            worker.stop.set()
    if worker:
        worker.thread.join(timeout=15)
        if worker.thread.is_alive():
            _LOG.warning("Completion task still finishing an in-flight request during NMS shutdown")
