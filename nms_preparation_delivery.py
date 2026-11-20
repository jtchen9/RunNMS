"""Durable preparation web delivery. No robot commands, timers, or schedule release.

The lifecycle driver (Step 5) calls create/advance then flush_once on its timer.
Store identity, revisions and diagnostic occurrences BEFORE any HTTP request.
Redis durability uses the site's existing persistence policy; no keys expire.
"""
import base64
import copy
import json
import ssl
import urllib.error
import urllib.request
import uuid
from datetime import datetime

TIME_FMT = "%Y-%m-%d-%H:%M:%S"
PHASES = {"awaiting_attempt", "preparing", "retry_wait", "ready"}
STATES = {"preparing": 0, "scheduled": 1, "running": 2, "wrapping_up": 3}
EVENTS = {"attempt_started", "robot_ready", "attempt_failed", "retry_scheduled",
          "reset_started", "reset_completed", "all_robots_ready", "schedule_assigned", "cancelled"}
IDENTITY = ("nms_id", "lab_id", "experiment_id", "session_id", "registration_uid")


def packed(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def stamp(value):
    if not isinstance(value, str) or datetime.strptime(value, TIME_FMT).strftime(TIME_FMT) != value:
        raise ValueError("Expected Taiwan-local YYYY-MM-DD-HH:MM:SS")
    return value


def natural(value):
    if type(value) is not int or value < 0:
        raise ValueError("Expected nonnegative integer")


def validate_snapshot(s):
    for k in IDENTITY:
        if not isinstance(s.get(k), str) or not s[k].strip():
            raise ValueError("Missing " + k)
    natural(s["update_seq"])
    if s["state"] not in STATES:
        raise ValueError("Terminal transitions belong to finalize/delete, not preparation updates")
    p = s["preparation"]
    if p["phase"] not in PHASES:
        raise ValueError("Unsupported preparation phase")
    natural(p["attempt"])
    stamp(p["updated_at"])
    if "reset_required" in p and type(p["reset_required"]) is not bool:
        raise ValueError("reset_required must be boolean")
    if p["phase"] == "retry_wait":
        stamp(p["retry_at"])
    if s["state"] == "preparing":
        if s.get("start_at") is not None or s.get("end_at") is not None:
            raise ValueError("Pending times must be null")
    else:
        if p["phase"] != "ready":
            raise ValueError("A committed schedule requires ready preparation")
        if stamp(s["end_at"]) < stamp(s["start_at"]):
            raise ValueError("end_at precedes start_at")
    packed(s)


class Conflict(RuntimeError):
    pass


class Store:
    """Redis WATCH transactions serialize revisions and merge concurrent acknowledgements."""
    def __init__(self, redis, prefix):
        self.redis, self.prefix = redis, prefix

    def key(self, uid):
        return self.prefix + ":record:" + uid

    def load(self, uid):
        raw = self.redis.get(self.key(uid))
        return json.loads(raw) if raw else None

    def _edit(self, uid, edit):
        from redis.exceptions import WatchError
        for _ in range(16):
            with self.redis.pipeline() as pipe:
                try:
                    pipe.watch(self.key(uid))
                    raw = pipe.get(self.key(uid))
                    doc = json.loads(raw) if raw else None
                    result = edit(doc)
                    pipe.multi()
                    pipe.set(self.key(uid), packed(result))
                    pipe.sadd(self.prefix + ":registrations", uid)
                    pipe.execute()
                    return result
                except WatchError:
                    continue
        raise Conflict("Concurrent preparation writes; retry operation")

    def create(self, fields, files, preparation, registration_uid=None):
        """files: list of (field name, (filename, bytes, content_type)). Persist once.

        registration_uid must be reused by the driver on a registration retry.
        Recreating under that UID must use exactly the original package.
        """
        fields = copy.deepcopy(fields)
        uid = registration_uid or uuid.uuid4().hex
        fields.setdefault("lab_id", fields.get("nms_id"))
        fields["session_id"] = fields.get("session_id") or "default"
        fields.update(registration_uid=uid, update_seq=0, preparation_json=packed(preparation))
        fields.pop("start_at", None)
        fields.pop("end_at", None)
        stamp(fields["registered_at"])
        natural(fields["command_count"])
        snapshot = {k: fields[k] for k in IDENTITY}
        snapshot.update(update_seq=0, state="preparing", start_at=None, end_at=None,
                        preparation=copy.deepcopy(preparation))
        validate_snapshot(snapshot)
        encoded = [[name, [filename, base64.b64encode(data).decode("ascii"), mime]]
                   for name, (filename, data, mime) in files]
        if not any(name == "command_sheet_file" for name, _ in encoded):
            raise ValueError("command_sheet_file required")
        package = {"fields": fields, "files": encoded}
        def edit(old):
            if old:
                if old["package"] != package:
                    raise Conflict("Registration retry changed the original package")
                return old
            return dict(package=package, snapshot=snapshot, registered=False,
                        session_uid=None, state_queue=[], events={}, pending_events=[],
                        blocked={}, remote_inactive=False, locally_closed=False,
                        last_delivery=None, event_batch=None)
        self._edit(uid, edit)
        return uid

    def advance(self, uid, expected_seq, *, state, preparation, start_at=None, end_at=None,
                events=()):
        """Persist a new lifecycle revision and its event occurrences atomically.

        Schedule values MUST come from the committed runtime schedule. This layer
        validates them but does not create, enqueue or authorize that schedule.
        """
        def edit(doc):
            if doc is None:
                raise Conflict("Unknown registration")
            old = doc["snapshot"]
            if old["update_seq"] != expected_seq or doc["locally_closed"] or doc["remote_inactive"]:
                raise Conflict("Stale/closed registration")
            new = {k: old[k] for k in IDENTITY}
            new.update(update_seq=expected_seq + 1, state=state,
                       preparation=copy.deepcopy(preparation), start_at=start_at, end_at=end_at)
            validate_snapshot(new)
            if STATES[state] < STATES[old["state"]] or preparation["attempt"] < old["preparation"]["attempt"]:
                raise Conflict("Lifecycle cannot move backwards")
            if old["preparation"]["phase"] == "ready" and preparation["phase"] != "ready":
                raise Conflict("Ready preparation cannot revert")
            if old["start_at"] is not None and (start_at, end_at) != (old["start_at"], old["end_at"]):
                raise Conflict("Committed schedule is immutable")
            doc["snapshot"] = new
            doc["state_queue"].append(new)
            for event in events:
                self._add_event(doc, event, new["update_seq"])
            return doc
        return self._edit(uid, edit)["snapshot"]

    @staticmethod
    def _add_event(doc, event, seq):
        e = copy.deepcopy(event)
        allowed = {"event_id", "update_seq", "attempt", "time_local", "event_type", "robot", "reason", "details"}
        if set(e) - allowed:
            raise ValueError("Unexpected diagnostic fields; no script offsets allowed")
        e.setdefault("event_id", uuid.uuid4().hex)
        e.setdefault("update_seq", seq)
        if not isinstance(e["event_id"], str) or not e["event_id"].strip() or len(e["event_id"]) > 128:
            raise ValueError("Invalid event_id")
        natural(e["update_seq"])
        natural(e["attempt"])
        if e["update_seq"] > doc["snapshot"]["update_seq"] or e["attempt"] > doc["snapshot"]["preparation"]["attempt"]:
            raise ValueError("Event refers to an uncommitted revision/attempt")
        stamp(e["time_local"])
        if e["event_type"] not in EVENTS or ("details" in e and not isinstance(e["details"], dict)):
            raise ValueError("Invalid diagnostic event")
        if len(packed(e).encode("utf-8")) > 65536:
            raise ValueError("Event exceeds 64 KiB")
        previous = doc["events"].get(e["event_id"])
        if previous is not None and previous != e:
            raise Conflict("Same event ID with different contents")
        if previous is None:
            doc["events"][e["event_id"]] = e
            doc["pending_events"].append(e["event_id"])
        return e["event_id"]

    def event(self, uid, event):
        """Delayed diagnostics remain valid after deletion/finalization."""
        chosen = dict(event)
        chosen.setdefault("event_id", uuid.uuid4().hex)
        def edit(doc):
            if doc is None:
                raise Conflict("Unknown registration")
            self._add_event(doc, chosen, doc["snapshot"]["update_seq"])
            return doc
        self._edit(uid, edit)
        return chosen["event_id"]

    def close_local(self, uid):
        """Driver calls after terminal handling. This does NOT notify delete/finalize."""
        def edit(doc):
            doc["locally_closed"] = True
            doc["state_queue"] = []
            return doc
        self._edit(uid, edit)

    def snapshot(self, uid):
        doc = self.load(uid)
        if not doc or doc["locally_closed"] or doc["remote_inactive"]:
            return None
        return {k: v for k, v in doc["snapshot"].items() if k != "nms_id"}

    def retry_blocked(self, uid, channel):
        """Explicit retry after fixing key/config/server; never silently discard records."""
        def edit(doc):
            doc["blocked"].pop(channel, None)
            return doc
        self._edit(uid, edit)


class Web:
    def __init__(self, base_url, api_key, verify_tls=True):
        self.base = base_url.rstrip("/")
        self.api_key = api_key
        self.context = ssl.create_default_context() if verify_tls else ssl._create_unverified_context()

    @classmethod
    def configured(cls):
        import config
        return cls(getattr(config, "WEB_NORTHBOUND_BASE_URL", "") or
                   getattr(config, "NORTHBOUND_WEB_BASE_URL", "") or getattr(config, "WEB_BASE", ""),
                   getattr(config, "WEB_NORTHBOUND_API_KEY", "") or
                   getattr(config, "NORTHBOUND_API_KEY", "") or
                   getattr(config, "NMS_API_KEY", "") or getattr(config, "WEB_API_KEY", ""),
                   getattr(config, "WEB_VERIFY_TLS", True))

    def send(self, path, payload, files=None):
        if not self.base:
            raise RuntimeError("Webserver base URL is not configured")
        if files is None:
            data, content_type = packed(payload).encode("utf-8"), "application/json"
        else:
            # Reuse the installed multipart encoder; no registration handler runs.
            from m4Commands import _multipart_body
            data, content_type = _multipart_body(payload, files)
        req = urllib.request.Request(self.base + path, data=data, method="POST", headers={
            "Content-Type": content_type, "X-API-Key": self.api_key})
        try:
            with urllib.request.urlopen(req, timeout=60 if files is not None else 10,
                                        context=self.context) as response:
                code, raw = response.status, response.read()
        except urllib.error.HTTPError as exc:
            code, raw = exc.code, exc.read()
        try:
            result = json.loads(raw)
        except (ValueError, UnicodeError):
            result = {"status": "invalid_json_response"}
        return code, result


class Delivery:
    def __init__(self, store, web):
        self.store, self.web = store, web

    def flush_once(self, uid):
        """One bounded HTTP request. Caller retries transient results on its timer.

        Registration first, then state revisions, then <=100 diagnostic events.
        No external call is made with a Redis transaction held open.
        """
        doc = self.store.load(uid)
        if not doc:
            raise Conflict("Unknown registration")
        files = None
        if not doc["registered"]:
            channel, path = "registration", "/nms/experiment/register"
            payload = doc["package"]["fields"]
            files = [(name, (f[0], base64.b64decode(f[1]), f[2])) for name, f in doc["package"]["files"]]
        elif doc.get("delete_request") and not doc.get("delete_acknowledged"):
            channel, path = "delete", "/nms/experiment/delete"
            payload, files = doc["delete_request"], []
        elif doc["state_queue"] and not doc["remote_inactive"] and not doc["locally_closed"] and "state" not in doc["blocked"]:
            channel, path = "state", "/nms/experiment/preparation"
            payload = doc["state_queue"][0]
        elif doc["pending_events"]:
            channel, path = "events", "/nms/experiment/preparation_events"
            payload = {k: doc["snapshot"][k] for k in IDENTITY}
            def pin(current):
                if not current["event_batch"]:
                    current["event_batch"] = current["pending_events"][:100]
                return current
            doc = self.store._edit(uid, pin)
            payload["events"] = [doc["events"][eid] for eid in doc["event_batch"]]
        else:
            return {"status": "blocked" if doc["blocked"] else "idle", "blocked": doc["blocked"]}
        if channel in doc["blocked"]:
            return {"status": "blocked", "channel": channel, "detail": doc["blocked"][channel]}
        try:
            code, response = self.web.send(path, payload, files)
        except Exception as exc:
            code, response = 0, {"status": "transport_error", "error_type": type(exc).__name__}
        good = 200 <= code < 300 and isinstance(response, dict) and response.get("status") == "ok"
        inactive = False
        ack = set()
        if good and channel == "registration":
            remote = response.get("experiment") or {}
            good = remote.get("registration_uid") == uid and bool(remote.get("session_uid"))
            # A lost registration response may be replayed after remote state advanced.
            inactive = remote.get("state") in {"completed", "deleted", "cancelled", "failed", "stopped"}
        elif good and channel == "delete":
            # The existing multipart delete endpoint explicitly acknowledges status=ok.
            inactive = True
        elif good and channel == "state":
            decision = response.get("decision")
            inactive = decision in {"terminal", "superseded"}
            remote = response.get("experiment") or {}
            good = inactive or (decision in {"accepted", "duplicate", "stale"} and
                                remote.get("registration_uid") == uid and
                                type(remote.get("update_seq")) is int and
                                remote["update_seq"] >= payload["update_seq"])
        elif good and channel == "events":
            ids = response.get("acknowledged_event_ids")
            sent = {e["event_id"] for e in payload["events"]}
            good = (response.get("registration_uid") == uid and
                    response.get("session_uid") == doc["session_uid"] and isinstance(ids, list) and
                    all(isinstance(x, str) for x in ids) and set(ids).issubset(sent))
            if good:
                ack = set(ids)
        result = {"status": "acknowledged" if good else "not_acknowledged", "channel": channel,
                  "http_status": code, "response": response}
        def edit(current):
            current["last_delivery"] = result
            if not good:
                if code in {400, 401, 403, 409, 422}:
                    current["blocked"][channel] = result
                return current
            if channel == "registration":
                current["registered"] = True
                current["session_uid"] = remote["session_uid"]
            elif channel == "delete":
                current["delete_acknowledged"] = True
            elif channel == "state":
                seq = payload["update_seq"]
                current["state_queue"] = [s for s in current["state_queue"] if s["update_seq"] != seq]
            else:
                current["pending_events"] = [eid for eid in current["pending_events"] if eid not in ack]
                current["event_batch"] = [eid for eid in (current["event_batch"] or []) if eid not in ack] or None
            if inactive:
                current["remote_inactive"] = True
                current["state_queue"] = []
            return current
        self.store._edit(uid, edit)
        return result
