"""Offline transport/persistence tests. No installed NMS/robot/server is contacted."""
import ast
import copy
import json
from pathlib import Path
import sys
import threading
import types
import unittest
import tempfile
import importlib.util
import contextlib
import io
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
try:
    from redis.exceptions import WatchError
except ImportError:
    errors = types.ModuleType("redis.exceptions")
    errors.WatchError = type("WatchError", (Exception,), {})
    sys.modules["redis.exceptions"] = errors
    WatchError = errors.WatchError
from nms_preparation_delivery import Store, Delivery, Conflict, packed


class MemoryRedis:
    """Small WATCH transaction double; data survive Store/Delivery reconstruction."""
    def __init__(self):
        self.data, self.versions, self.sets = {}, {}, {}
        self.lock = threading.RLock()
    def get(self, key):
        with self.lock: return self.data.get(key)
    def pipeline(self): return Pipeline(self)


class Pipeline:
    def __init__(self, r): self.r, self.watched, self.ops = r, {}, []
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def watch(self, key):
        with self.r.lock: self.watched[key] = self.r.versions.get(key, 0)
    def get(self, key): return self.r.get(key)
    def multi(self): pass
    def set(self, key, value): self.ops.append(("set", key, value))
    def sadd(self, key, value): self.ops.append(("sadd", key, value))
    def execute(self):
        with self.r.lock:
            if any(self.r.versions.get(k, 0) != v for k, v in self.watched.items()): raise WatchError()
            for op, key, value in self.ops:
                if op == "set":
                    self.r.data[key] = value
                    self.r.versions[key] = self.r.versions.get(key, 0) + 1
                else: self.r.sets.setdefault(key, set()).add(value)


NOW = "2026-11-20-18:00:00"

def prep(phase="preparing", attempt=1):
    return dict(phase=phase, attempt=attempt, updated_at=NOW,
                **({"retry_at": "2026-11-20-18:03:00"} if phase == "retry_wait" else {}))


def event(name="attempt_started", eid="e1"):
    return dict(event_id=eid, event_type=name, attempt=1, time_local=NOW)


class FakeWeb:
    def __init__(self): self.calls, self.hook, self.custom = [], None, None
    def send(self, path, payload, files=None):
        self.calls.append(copy.deepcopy((path, payload, files)))
        if self.hook: self.hook()
        if self.custom:
            code, result = self.custom(path, payload)
            return code, copy.deepcopy(result)
        uid = payload["registration_uid"]
        if path.endswith("/register"):
            return 200, {"status": "ok", "experiment": dict(registration_uid=uid, session_uid="default__v000001", state="preparing", update_seq=0)}
        if path.endswith("/preparation"):
            return 200, {"status": "ok", "decision": "accepted", "experiment": dict(payload)}
        return 200, dict(status="ok", registration_uid=uid, session_uid="default__v000001",
                         acknowledged_event_ids=[e["event_id"] for e in payload["events"]])


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.redis = MemoryRedis()
        self.store = Store(self.redis, "test:delivery")
        self.web = FakeWeb()
        self.delivery = Delivery(self.store, self.web)
        self.fields = dict(nms_id="DemoRoom", experiment_id="Probe", registered_at=NOW, command_count=0)
        self.files = [("command_sheet_file", ("probe.csv", b"scanner,t_offset_sec\r\n", "text/csv"))]
        self.uid = self.store.create(self.fields, self.files, prep("awaiting_attempt", 0), "run1")

    def advance(self, state="preparing", preparation=None, **kw):
        seq = self.store.load(self.uid)["snapshot"]["update_seq"]
        return self.store.advance(self.uid, seq, state=state, preparation=preparation or prep(), **kw)

    def flush(self):
        for _ in range(10):
            result = self.delivery.flush_once(self.uid)
            if result["status"] == "idle": return
            self.assertEqual(result["status"], "acknowledged", result)
        self.fail("not drained")

    def test_registration_first_and_optional_poses(self):
        self.advance(events=[event()]); self.flush()
        paths = [x[0] for x in self.web.calls]
        self.assertEqual(paths, ["/nms/experiment/register", "/nms/experiment/preparation", "/nms/experiment/preparation_events"])
        fields = self.web.calls[0][1]
        self.assertNotIn("start_at", fields)
        self.assertEqual(fields["registration_uid"], "run1")
        self.assertEqual([f[0] for f in self.web.calls[0][2]], ["command_sheet_file"])

    def test_original_package_survives_later_state_and_restart(self):
        self.web.custom = lambda p, v: (0, {"status": "error"})
        self.delivery.flush_once(self.uid)
        original = self.web.calls[0]
        self.advance()
        self.store = Store(self.redis, "test:delivery")
        Delivery(self.store, self.web).flush_once(self.uid)
        self.assertEqual(original, self.web.calls[-1])

    def test_changed_package_rejected(self):
        with self.assertRaises(Conflict):
            self.store.create(dict(self.fields, command_count=1), self.files, prep("awaiting_attempt", 0), self.uid)

    def test_identical_create_keeps_later_state(self):
        self.advance()
        self.store.create(self.fields, self.files, prep("awaiting_attempt", 0), self.uid)
        self.assertEqual(self.store.snapshot(self.uid)["update_seq"], 1)

    def test_stale_writer_rejected(self):
        self.advance()
        with self.assertRaises(Conflict): self.store.advance(self.uid, 0, state="preparing", preparation=prep())

    def test_atomic_state_event_validation(self):
        bad = dict(event(), elapsed_sec=10)
        with self.assertRaises(ValueError): self.advance(events=[bad])
        self.assertEqual(self.store.snapshot(self.uid)["update_seq"], 0)
        self.assertEqual(self.store.load(self.uid)["pending_events"], [])

    def test_lost_event_ack_preserves_identical_batch_with_new_arrivals(self):
        self.advance(events=[event()])
        self.delivery.flush_once(self.uid); self.delivery.flush_once(self.uid)
        self.web.custom = lambda p, v: (0, {"status": "timeout"})
        self.delivery.flush_once(self.uid)
        sent = self.web.calls[-1]
        self.store.event(self.uid, event("robot_ready", "e2"))
        Delivery(Store(self.redis, "test:delivery"), self.web).flush_once(self.uid)
        self.assertEqual(sent, self.web.calls[-1])
        self.web.custom = None
        self.flush()
        self.assertEqual(self.store.load(self.uid)["pending_events"], [])
        self.assertEqual(len(self.store.load(self.uid)["events"]), 2)

    def test_partial_ack_only_removes_acknowledged_ids(self):
        self.advance(events=[event(), event("robot_ready", "e2")])
        self.delivery.flush_once(self.uid); self.delivery.flush_once(self.uid)
        self.web.custom = lambda p,v: (200, dict(status="ok", registration_uid=self.uid,
            session_uid="default__v000001", acknowledged_event_ids=["e1"]))
        self.delivery.flush_once(self.uid)
        self.assertEqual(self.store.load(self.uid)["pending_events"], ["e2"])

    def test_wrong_identity_or_unknown_ack_keeps_events(self):
        self.advance(events=[event()]); self.delivery.flush_once(self.uid); self.delivery.flush_once(self.uid)
        for uid, ids in [("wrong", ["e1"]), (self.uid, ["unrelated"]), (self.uid, None)]:
            self.web.custom = lambda p,v: (200, dict(status="ok", registration_uid=uid,
                session_uid="default__v000001", acknowledged_event_ids=ids))
            self.delivery.flush_once(self.uid)
            self.assertEqual(self.store.load(self.uid)["pending_events"], ["e1"])

    def test_event_duplicate_and_conflict(self):
        self.advance(events=[event()])
        self.store.event(self.uid, event())
        self.assertEqual(self.store.load(self.uid)["pending_events"], ["e1"])
        with self.assertRaises(Conflict): self.store.event(self.uid, dict(event(), reason="changed"))

    def test_events_older_than_current_revision_are_valid(self):
        self.advance(); self.advance(preparation=prep("retry_wait"))
        self.store.event(self.uid, dict(event(), update_seq=1))
        self.flush()
        self.assertEqual(self.web.calls[-1][1]["events"][0]["update_seq"], 1)

    def test_event_batch_limit(self):
        self.advance(events=[event(eid=str(i)) for i in range(101)])
        self.flush()
        batches = [c[1]["events"] for c in self.web.calls if c[0].endswith("_events")]
        self.assertEqual([len(x) for x in batches], [100, 1])

    def test_oversized_event_rejected(self):
        self.advance()
        with self.assertRaises(ValueError): self.store.event(self.uid, dict(event(), details={"x": "中" * 30000}))

    def test_terminal_ack_stops_state_but_allows_events(self):
        self.advance(events=[event()]); self.delivery.flush_once(self.uid)
        self.web.custom = lambda p,v: (200, {"status": "ok", "decision": "terminal", "experiment": None})
        self.delivery.flush_once(self.uid)
        self.assertIsNone(self.store.snapshot(self.uid))
        with self.assertRaises(Conflict): self.advance()
        self.web.custom = None; self.flush()
        self.assertTrue(self.web.calls[-1][0].endswith("_events"))

    def test_delete_keeps_late_diagnostics_no_reopening(self):
        self.advance(); self.flush(); self.store.close_local(self.uid)
        self.store.event(self.uid, event("cancelled"))
        self.flush()
        self.assertIsNone(self.store.snapshot(self.uid))
        self.assertEqual(sum(p.endswith("/register") for p,_,_ in self.web.calls), 1)

    def test_new_same_name_run_keeps_old_event_identity(self):
        self.advance(); self.flush(); self.store.close_local(self.uid)
        self.store.create(self.fields, self.files, prep("awaiting_attempt", 0), "run2")
        self.store.event(self.uid, event("cancelled")); self.flush()
        self.assertEqual(self.web.calls[-1][1]["registration_uid"], "run1")
        self.assertFalse(self.store.load("run2")["registered"])

    def test_http_conflicts_block_without_discarding(self):
        for status in [400, 401, 403, 409, 422]:
            self.store.retry_blocked(self.uid, "registration")
            self.web.custom = lambda p,v: (status, {"status": "error"})
            self.delivery.flush_once(self.uid)
            n = len(self.web.calls)
            self.assertEqual(self.delivery.flush_once(self.uid)["status"], "blocked")
            self.assertEqual(len(self.web.calls), n)
            self.assertFalse(self.store.load(self.uid)["registered"])

    def test_transient_errors_retried(self):
        self.web.custom = lambda p,v: (503, {"status": "error"})
        self.delivery.flush_once(self.uid); self.delivery.flush_once(self.uid)
        self.assertEqual(len(self.web.calls), 2)
        self.assertEqual(self.store.load(self.uid)["blocked"], {})

    def test_non_json_success_is_not_acceptance(self):
        self.web.custom = lambda p,v: (200, {"status": "invalid_json_response"})
        self.delivery.flush_once(self.uid)
        self.assertFalse(self.store.load(self.uid)["registered"])

    def test_wrong_registration_uid_not_accepted(self):
        self.web.custom = lambda p,v: (200, {"status": "ok", "experiment": {"registration_uid": "wrong", "session_uid": "x"}})
        self.delivery.flush_once(self.uid)
        self.assertFalse(self.store.load(self.uid)["registered"])

    def test_snapshot_same_as_state_payload_without_nms_id(self):
        self.advance(); self.flush()
        state = self.web.calls[1][1].copy(); state.pop("nms_id")
        self.assertEqual(state, self.store.snapshot(self.uid))

    def test_snapshot_reads_do_not_increment_sequence(self):
        a = self.store.snapshot(self.uid)
        self.assertEqual(a, self.store.snapshot(self.uid))

    def test_schedule_immutable(self):
        self.advance(state="scheduled", preparation=prep("ready"), start_at="2026-11-20-18:01:00", end_at="2026-11-20-18:02:00")
        with self.assertRaises(Conflict): self.advance(state="running", preparation=prep("ready"), start_at="2026-11-20-18:02:00", end_at="2026-11-20-18:03:00")

    def test_running_wrapup_keep_times(self):
        times = dict(start_at="2026-11-20-18:01:00", end_at="2026-11-20-18:02:00")
        for state in ["scheduled", "running", "wrapping_up"]: self.advance(state=state, preparation=prep("ready"), **times)
        self.flush()
        self.assertEqual([v[1]["state"] for v in self.web.calls if v[0].endswith("/preparation")], ["scheduled", "running", "wrapping_up"])

    def test_invalid_phase_and_terminal_state_rejected(self):
        for state, phase in [("completed", "ready"), ("preparing", "checking_health"), ("scheduled", "preparing")]:
            with self.assertRaises(ValueError): self.advance(state=state, preparation=prep(phase))

    def test_concurrent_append_survives_state_ack(self):
        self.advance(); self.delivery.flush_once(self.uid)
        self.web.hook = lambda: self.advance(preparation=prep("retry_wait"))
        self.delivery.flush_once(self.uid)
        self.assertEqual([s["update_seq"] for s in self.store.load(self.uid)["state_queue"]], [2])

    def test_concurrent_event_append_survives_ack(self):
        self.advance(events=[event()]); self.delivery.flush_once(self.uid); self.delivery.flush_once(self.uid)
        self.web.hook = lambda: self.store.event(self.uid, event("robot_ready", "e2"))
        self.delivery.flush_once(self.uid)
        self.assertEqual(self.store.load(self.uid)["pending_events"], ["e2"])

    def test_superseded_ack_does_not_restart(self):
        self.advance(); self.delivery.flush_once(self.uid)
        self.web.custom = lambda p,v: (200, {"status":"ok", "decision":"superseded", "experiment":None})
        self.delivery.flush_once(self.uid)
        self.assertIsNone(self.store.snapshot(self.uid))
        self.assertEqual(self.delivery.flush_once(self.uid)["status"], "idle")

    def test_stale_and_duplicate_state_ack(self):
        self.advance(); self.delivery.flush_once(self.uid)
        for decision in ["duplicate", "stale"]:
            if not self.store.load(self.uid)["state_queue"]: self.advance()
            self.web.custom = lambda p,v: (200, {"status":"ok", "decision":decision, "experiment":v})
            self.assertEqual(self.delivery.flush_once(self.uid)["status"], "acknowledged")
        self.assertEqual(self.store.load(self.uid)["state_queue"], [])

    def test_no_state_ack_from_unknown_identity(self):
        self.advance(); self.delivery.flush_once(self.uid)
        self.web.custom = lambda p,v: (200, {"status":"ok", "decision":"accepted",
            "experiment":dict(v, registration_uid="other")})
        self.delivery.flush_once(self.uid)
        self.assertEqual(len(self.store.load(self.uid)["state_queue"]), 1)

    def test_retry_wait_and_retry_keep_attempt_monotonic(self):
        self.advance(); self.advance(preparation=prep("retry_wait"))
        self.advance(preparation=prep("preparing",2))
        with self.assertRaises(Conflict): self.advance(preparation=prep("preparing",1))

    def test_no_robot_or_command_module_imported_by_store(self):
        tree=ast.parse((ROOT / "nms_preparation_delivery.py").read_text())
        top_imports=[n for n in tree.body if isinstance(n,(ast.Import,ast.ImportFrom))]
        for n in top_imports:
            text=ast.unparse(n)
            self.assertNotIn("m8mobility",text)
            self.assertNotIn("m4Commands",text)
            self.assertNotIn("config",text)

    def test_legacy_and_new_finalize_delete_uid(self):
        source = ast.parse((ROOT / "m4Commands.py").read_text(encoding="utf-8-sig"))
        names = {"_notify_web_experiment_finalize", "_notify_web_experiment_delete_from_rows"}
        selected = ast.Module(body=[n for n in source.body if isinstance(n, ast.FunctionDef) and n.name in names], type_ignores=[])
        calls=[]
        env=dict(Dict=dict, Any=object, List=list, utility=types.SimpleNamespace(local_ts=lambda: NOW),
                 config=types.SimpleNamespace(NORTHBOUND_EXPERIMENT_WRAPUP_SEC=120),
                 _nms_lab_id=lambda:"DemoRoom", _redis_text=str,
                 _post_multipart_to_webserver=lambda p,f,fs: calls.append((p, f.copy())) or {"status":"ok"})
        exec(compile(selected,"m4Commands.py","exec"),env)
        runtime = types.SimpleNamespace(before_finalize=lambda fields:True,
                    store=lambda:types.SimpleNamespace(load=lambda uid:None),wake=lambda:None)
        with patch.dict(sys.modules,{"nms_preparation_runtime":runtime}):
            for uid in [None,"uid-test"]:
                fields=dict(experiment_id="test", **({"registration_uid": uid} if uid else {}))
                env["_notify_web_experiment_finalize"](fields)
                env["_notify_web_experiment_delete_from_rows"]([("id",fields)])
                for _,payload in calls[-2:]:
                    self.assertEqual(payload.get("registration_uid"), uid)
                    if uid is None: self.assertNotIn("registration_uid",payload)


class ProbeRedis(MemoryRedis):
    rows=[]
    def set(self,k,v):
        with self.lock:
            self.data[k]=v
            self.versions[k]=self.versions.get(k,0)+1
    def xrange(self,*a,**kw): return self.rows


class ProbeWeb(FakeWeb):
    def __init__(self):
        super().__init__()
        self.seen=set()
    def send(self,path,payload,files=None):
        if path.endswith("/delete"):
            self.calls.append(copy.deepcopy((path,payload,files)))
            return 200, {"status":"ok"}
        code,reply=super().send(path,payload,files)
        if path.endswith("_events"):
            ids={e["event_id"] for e in payload["events"]}
            reply["duplicate_event_ids"]=sorted(ids & self.seen)
            self.seen.update(ids)
        return code,reply


class ProbeTests(unittest.TestCase):
    def setUp(self):
        spec=importlib.util.spec_from_file_location("probe", ROOT / "tools" / "preparation_web_probe.py")
        self.probe=importlib.util.module_from_spec(spec); spec.loader.exec_module(self.probe)
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.probe.ROOT=Path(self.temp.name)
        self.redis=ProbeRedis(); self.web=ProbeWeb()
        self.lock=types.SimpleNamespace(acquire=lambda **k:True,owned=lambda:True,release=lambda:None)
        modules={"config":types.SimpleNamespace(r=self.redis,KEY_EXPERIMENT_REGISTRY="experiments",NMS_ID="DemoRoom"),
                 "utility":types.SimpleNamespace(local_ts=lambda:NOW),
                 "nms_experiment_completion":types.SimpleNamespace(lifecycle_lock=lambda:self.lock)}
        p=patch.dict(sys.modules,modules); p.start(); self.addCleanup(p.stop)
        p=patch.object(self.probe.Web,"configured",return_value=self.web); p.start(); self.addCleanup(p.stop)

    def run_probe(self,mode):
        with contextlib.redirect_stdout(io.StringIO()): self.probe.run(mode)

    def test_live_cancel_and_evidence(self):
        self.run_probe("live"); self.run_probe("cancel")
        self.assertEqual(len(list(self.probe.ROOT.rglob("step4_*.json"))),2)
        self.assertEqual(len(self.web.seen),4)
        self.assertEqual(self.web.calls[-1][0],"/nms/experiment/preparation_events")
        self.assertEqual(self.web.calls[-2][0],"/nms/experiment/delete")

    def test_rerunning_probe_reuses_uid_and_package(self):
        self.run_probe("live"); self.run_probe("live")
        self.assertEqual(sum(c[0].endswith("/register") for c in self.web.calls),1)
        self.run_probe("cancel"); self.run_probe("cancel")
        self.assertEqual(sum(c[0].endswith("/delete") for c in self.web.calls),1)

    def test_existing_experiment_blocks_probe(self):
        self.redis.rows=[("x",{"state":"registered"})]
        with self.assertRaises(RuntimeError): self.run_probe("live")
        self.assertEqual(self.web.calls,[])

    def test_new_probe_uses_new_identity(self):
        self.run_probe("live"); self.run_probe("cancel"); self.run_probe("live")
        uids=[c[1]["registration_uid"] for c in self.web.calls if c[0].endswith("/register")]
        self.assertEqual(len(set(uids)),2)


if __name__ == "__main__": unittest.main(verbosity=2)
