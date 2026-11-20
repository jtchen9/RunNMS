"""Shared preparation admission boundary. No new mobility state or recovery policy."""
import functools
import json
import threading
import uuid

OWNER = uuid.uuid4().hex
ACTIVITY = threading.RLock()


def prefix():
    import config
    return config.KEY_EXPERIMENT_REGISTRY + ":preparation"


def active():
    import config
    uid = config.r.get(prefix() + ":active")
    if not uid:
        return None
    if isinstance(uid, bytes): uid = uid.decode()
    raw = config.r.get(prefix() + ":record:" + uid)
    if not raw:
        raise RuntimeError("Preparation ownership exists without its record; reconcile before issuing commands")
    return json.loads(raw)


def held(doc=None):
    doc = active() if doc is None else doc
    return bool(doc and not doc["runtime"].get("released"))


def require_manual_admission():
    if held():
        from fastapi import HTTPException
        raise HTTPException(409, "Preparation owns command admission. Delete the experiment before issuing manual commands.")


def execution_allowed(scanner):
    import config
    doc = active()
    if not doc or doc["runtime"].get("released"):
        return True
    rt = doc["runtime"]
    lease = config.r.get(prefix() + ":worker_lock")
    if isinstance(lease, bytes): lease = lease.decode()
    return (rt.get("owner") == OWNER == lease and rt["phase"] == "preparing"
            and rt.get("active_robot") == scanner)


def row_allowed(scanner, fields):
    """Both robot and AP poll endpoints use this gate. Poll/report ingest continues."""
    doc = active()
    if not doc or doc["runtime"].get("released"):
        return True
    rt, uid = doc["runtime"], doc["snapshot"]["registration_uid"]
    if rt["phase"] == "health":
        return (fields.get("preparation_uid") == uid and fields.get("action") == "health.check.static")
    return fields.get("mobility_s1_managed") == "1" and execution_allowed(scanner)


def serialized(fn):
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        with ACTIVITY:
            return fn(*args, **kwargs)
    return wrapped


def guarded_mobility(fn):
    @functools.wraps(fn)
    def wrapped(scanner, *args, **kwargs):
        with ACTIVITY:
            if not execution_allowed(scanner):
                return {"status": "blocked", "scanner": scanner, "detail": "preparation admission held"}
            return fn(scanner, *args, **kwargs)
    return wrapped


def queued_row_current(key, xid, fields):
    """A scheduler snapshot fetched before deletion cannot revive a removed row."""
    import config
    rows=config.r.xrange(key,min=xid,max=xid,count=1)
    return bool(rows and rows[0][1] == {k:v for k,v in fields.items() if k!="cmd_id"})
