"""Offline Step 5 integration tests. Never imports the installed config or contacts robots.
Requires fakeredis (test-only): python -m pip install fakeredis
Run from any directory: python testSM/testPreparation/step5_check.py
"""
import ast
import asyncio
import copy
import csv
import io
import json
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
try:
    import fakeredis
except ImportError:
    raise SystemExit("Offline test dependency missing. Run: python -m pip install fakeredis")
from nms_preparation_lifecycle import Lifecycle,Conflict,ceil_minute,FMT
from nms_preparation_delivery import Delivery
import nms_preparation_gate as gate
import nms_preparation_runtime as runtime
from nms_preparation_registration import compile_operations,planned_poses_csv

NOW="2026-11-20-18:00:00"
A,B="twin-scout-alpha","twin-scout-bravo"

def module(name,**kw):
    m=types.ModuleType(name);m.__dict__.update(kw);return m

def selected(filename,names,env):
    tree=ast.parse((ROOT/filename).read_text(encoding="utf-8-sig"))
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),filename,"exec"),env)
    return env

def manifest():
    return dict(initial_positions=[dict(scanner=s,x_m=5.,y_m=3.+i,heading_deg=270.,row_number=i+2) for i,s in enumerate((A,B))],
                commands=[dict(scanner=A,t_offset_sec=0,row_number=2,category="scan",action="scan.start",args={}),
                          dict(scanner=B,t_offset_sec=120,row_number=3,category="mobility",action="mobility.move",args={"x_m":5.,"y_m":4.2})],
                reentry_reminders=[])

class Base(unittest.TestCase):
    def setUp(self):
        self.r=fakeredis.FakeRedis(decode_responses=True)
        self.config=module("config",r=self.r,KEY_EXPERIMENT_REGISTRY="test:experiments",KEY_REGISTRY="test:robots",
            MOBILITY_ROBOT_RESTRICT_RADIUS_M=.5,TIME_FMT=FMT,KEY_MOBILITY_CMD_STREAM="test:mobility",KEY_TRAFFIC_CMD_STREAM="test:traffic",
            key_cmd_stream=lambda s:"cmd:"+s,key_scanner_meta=lambda s:"meta:"+s)
        self.utility=module("utility",local_ts=lambda:NOW,parse_local_dt=lambda s:datetime.strptime(s,FMT))
        self.mods=patch.dict(sys.modules,{"config":self.config,"utility":self.utility});self.mods.start();self.addCleanup(self.mods.stop)
        self.st=Lifecycle(self.r,gate.prefix());self.uid="run-one"
        self.st.create(dict(nms_id="DemoRoom",experiment_id="Test",session_id="default",registered_at=NOW,command_count=2),
                       [("command_sheet_file",("test.csv",b"csv","text/csv"))],
                       dict(phase="awaiting_attempt",attempt=0,updated_at=NOW),self.uid)
        self.st.activate(self.uid,manifest(),self.config.KEY_EXPERIMENT_REGISTRY,
            dict(registration_uid=self.uid,state="preparing",start_at="",end_at=""),NOW)
        self.r.set(gate.prefix()+":worker_lock",gate.OWNER)
        self.d=self.st.claim(self.uid,gate.OWNER,NOW)
        self.calls=[]
    def begin(self):
        self.d=self.st.begin(self.uid,gate.OWNER,NOW);self.token=self.d["runtime"]["token"];return self.d
    def stage(self,phase,**kw):
        self.d=self.st.stage(self.uid,gate.OWNER,self.token,phase,NOW,**kw);return self.d
    def ready(self):
        self.begin();self.stage("health");self.stage("preparing",ready=A);self.stage("preparing",ready=B);return self.stage("ready")
    def ops(self,m,t,i):return [("xadd","cmd:"+c["scanner"],dict(action=c["action"],execute_at=t.strftime(FMT))) for c in m["commands"]]
    def commit(self):return self.st.commit(self.uid,gate.OWNER,self.token,lambda:datetime(2026,11,20,18,2,40),self.ops)
    def mark_registered(self):
        def f(d):d["registered"]=True;d["session_uid"]="default__v000001";return d
        self.st._edit(self.uid,f)
    def poll(self,s,stamp="2026-11-20-18:00:01"):
        self.r.hset(self.config.key_scanner_meta(s),mapping=dict(last_poll=stamp,registration_status="ready"))

class LifecycleCases(Base):
    def test_no_commands_or_times_before_ready(self):
        self.begin();self.stage("health");self.stage("preparing",ready=A)
        with self.assertRaises(Conflict):self.commit()
        self.assertEqual(self.r.xlen("cmd:"+A),0);self.assertIsNone(self.d["snapshot"]["start_at"])
    def test_commit_assigns_ceil_time_and_all_commands_exactly_once(self):
        self.ready();d=self.commit();again=self.commit()
        self.assertEqual(d,again);self.assertEqual(d["snapshot"]["start_at"],"2026-11-20-18:03:00")
        self.assertEqual(d["snapshot"]["end_at"],"2026-11-20-18:05:00")
        self.assertEqual(d["snapshot"]["preparation"]["phase"],"ready")
        self.assertEqual([self.r.xlen("cmd:"+s) for s in (A,B)],[1,1])
        self.assertEqual(self.r.xlen(self.config.KEY_EXPERIMENT_REGISTRY),1)
        self.assertEqual(sum(e["event_type"]=="schedule_assigned" for e in d["events"].values()),1)
    def test_wrong_destination_type_cannot_partially_release(self):
        self.ready();self.r.set("cmd:"+B,"wrongtype")
        with self.assertRaises(ValueError):self.commit()
        self.assertEqual(self.r.xlen("cmd:"+A),0);self.assertFalse(self.st.load(self.uid)["runtime"]["released"])
    def test_ready_before_commit_can_fail_and_retry(self):
        self.ready();self.assertEqual(self.d["snapshot"]["preparation"]["phase"],"preparing")
        self.d=self.st.fail(self.uid,gate.OWNER,self.token,NOW,"commit failed")
        self.assertEqual(self.d["snapshot"]["preparation"]["phase"],"retry_wait")
    def test_60_second_adjustment_and_new_attempt_token(self):
        self.begin();old=self.token;self.st.fail(self.uid,gate.OWNER,old,NOW,"s7stopped",A)
        with self.assertRaises(Conflict):self.st.begin(self.uid,gate.OWNER,"2026-11-20-18:00:59")
        d=self.st.begin(self.uid,gate.OWNER,"2026-11-20-18:01:00")
        self.assertEqual(d["runtime"]["attempt"],2);self.assertNotEqual(old,d["runtime"]["token"])
        with self.assertRaises(Conflict):self.st.stage(self.uid,gate.OWNER,old,"ready",NOW)
    def test_restart_in_flight_requires_reset_same_registration(self):
        self.begin();self.stage("preparing",robot=A)
        d=Lifecycle(self.r,gate.prefix()).claim(self.uid,"new-owner",NOW)
        self.assertEqual(d["snapshot"]["registration_uid"],self.uid)
        self.assertEqual(d["runtime"]["phase"],"retry_wait")
        with self.assertRaises(Conflict):self.commit()
    def test_restart_after_commit_preserves_schedule_and_queue(self):
        self.ready();d=self.commit();new=Lifecycle(self.r,gate.prefix()).claim(self.uid,"new-owner",NOW)
        self.assertEqual(new["snapshot"],d["snapshot"]);self.assertTrue(new["runtime"]["released"])
        self.assertEqual(self.r.xlen("cmd:"+A),1)
    def test_cancel_revokes_token_and_prevents_release(self):
        self.ready();d=self.st.request_cancel(self.uid,NOW)
        with self.assertRaises(Conflict):self.commit()
        self.assertTrue(d["locally_closed"]);self.assertEqual(d["state_queue"],[])
        self.assertEqual(d["delete_request"]["registration_uid"],self.uid)
    def test_stale_worker_cannot_close_new_registration(self):
        self.r.set(self.st.active_key,"new-uid");self.st.finish(self.uid,gate.OWNER)
        self.assertEqual(self.r.get(self.st.active_key),"new-uid")
        with self.assertRaises(Conflict):self.st.fail(self.uid,gate.OWNER,self.d["runtime"]["token"],NOW,"old")
    def test_concurrent_watch_mutation_recompiles_without_duplicates(self):
        self.ready();n=[0]
        def ops(m,t,i):
            n[0]+=1
            if n[0]==1:self.r.set(self.st.key(self.uid),self.r.get(self.st.key(self.uid)))
            return self.ops(m,t,i)
        self.st.commit(self.uid,gate.OWNER,self.token,lambda:datetime(2026,11,20,18,2,40),ops)
        self.assertEqual(n[0],2);self.assertEqual(self.r.xlen("cmd:"+A),1)
    def test_minute_rounding(self):
        self.assertEqual(ceil_minute(datetime(2026,11,20,18,0)),datetime(2026,11,20,18,0))
        self.assertEqual(ceil_minute(datetime(2026,11,20,23,59,59)),datetime(2026,11,21))

class Gates(Base):
    def test_health_only_matching_uid_and_action(self):
        self.begin();self.stage("health")
        self.assertTrue(gate.row_allowed(A,dict(preparation_uid=self.uid,action="health.check.static")))
        for fields in (dict(action="scan.start"),dict(preparation_uid="old",action="health.check.static"),dict(preparation_uid=self.uid,action="ap.channel.set")):
            self.assertFalse(gate.row_allowed(A,fields))
    def test_only_active_preparation_robot_and_managed_commands(self):
        self.begin();self.stage("preparing",robot=A)
        self.assertTrue(gate.row_allowed(A,dict(mobility_s1_managed="1")))
        self.assertFalse(gate.row_allowed(B,dict(mobility_s1_managed="1")))
        self.assertFalse(gate.row_allowed(A,dict(action="scan.start")))
        self.r.delete(gate.prefix()+":worker_lock")
        self.assertFalse(gate.row_allowed(A,dict(mobility_s1_managed="1")))
    def test_manual_blocked_until_release_then_ordinary_commands_allowed(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):gate.require_manual_admission()
        self.ready();self.commit();gate.require_manual_admission()
        self.assertTrue(gate.row_allowed(A,dict(action="scan.start")))
    def test_retry_and_cancel_block_stale_callbacks(self):
        self.begin();self.stage("preparing",robot=A)
        fn=gate.guarded_mobility(lambda scanner:self.calls.append(scanner) or {"status":"ok"})
        self.assertEqual(fn(A)["status"],"ok")
        self.st.fail(self.uid,gate.OWNER,self.token,NOW,"s7",A)
        self.assertEqual(fn(A)["status"],"blocked")
        self.st.request_cancel(self.uid,NOW);self.assertEqual(fn(A)["status"],"blocked")
        self.assertEqual(self.calls,[A])
    def test_no_active_registration_preserves_normal_behaviour(self):
        self.r.delete(self.st.active_key)
        self.assertTrue(gate.execution_allowed(A));self.assertTrue(gate.row_allowed(A,{}))
    def test_quiescence_requires_fresh_post_freeze_poll_from_every_robot(self):
        self.assertFalse(runtime.quiescent(self.d));self.poll(A);self.poll(B,NOW)
        with patch.object(runtime,"now",return_value="2026-11-20-18:00:02"):
            self.assertFalse(runtime.quiescent(self.d));self.poll(B);self.assertTrue(runtime.quiescent(self.d))
        with patch.object(runtime,"now",return_value="2026-11-20-18:01:00"):
            self.assertFalse(runtime.quiescent(self.d))
    def test_current_status_uses_exact_lifecycle_snapshot(self):
        self.begin();self.st.fail(self.uid,gate.OWNER,self.token,NOW,"camera unavailable",A)
        d=self.st.load(self.uid);actual=runtime.current_snapshot()
        self.assertEqual({k:actual[k] for k in d["snapshot"] if k!="nms_id"},{k:v for k,v in d["snapshot"].items() if k!="nms_id"})
    def test_real_ap_poller_uses_same_collection_gate(self):
        # m6AP is unchanged; source integration is verified separately in the package report.
        text=(ROOT/"m4Commands.py").read_text()
        self.assertIn("if not row_allowed(scanner, fields):",text)
    def test_real_traffic_dispatch_holds_preparation(self):
        env=dict(Dict=dict,Any=object,serialized=gate.serialized,held=gate.held)
        selected("m7Traffic.py",{"_execute_due_command"},env)
        env["_execute_due_command"]("old-row",{})  # Would execute/load traffic without the gate.
    def test_real_mobility_dispatch_holds_preparation(self):
        env=dict(Dict=dict,Any=object,preparation_serialized=gate.serialized,preparation_held=gate.held)
        selected("m8mobility.py",{"_dispatch_due_mobility_command"},env)
        env["_dispatch_due_mobility_command"]("old-row",{})

class Worker(Base):
    def invoke(self,failure=None,cancel_at=None):
        self.begin();self.mark_registered()
        class Lock:
            def acquire(self,**kw):return True
            def owned(self):return True
            def release(self):pass
        async def health(*a,**kw):self.calls.append("health");return {A:"PASS",B:"PASS"}
        async def position(**kw):
            tag=(kw["scanner"],kw["measure_only"]);self.calls.append(tag)
            if tag==cancel_at:self.st.request_cancel(self.uid,NOW)
            if tag==failure:return dict(status="failed",reason="s7stopped")
            return dict(status="measured" if kw["measure_only"] else "ready")
        import contextlib
        modules={"nms_static_readiness":module("nms_static_readiness",check_targets=health),
                 "nms_preparation_positioning":module("nms_preparation_positioning",NmsPositioningPort=lambda:object(),position_once=position,PositioningError=ValueError),
                 "nms_experiment_completion":module("nms_experiment_completion",lifecycle_lock=Lock,start_registration=lambda *a:self.calls.append("timer")),
                 "m8mobility_trace":module("m8mobility_trace",experiment_trace_boundary=contextlib.nullcontext)}
        import nms_preparation_registration as registration
        with patch.dict(sys.modules,modules),patch.object(runtime,"reset",side_effect=lambda d:self.calls.append("reset")),patch.object(runtime,"freeze"),patch.object(runtime,"now",return_value=NOW),patch.object(registration,"compile_operations",side_effect=self.ops):
            asyncio.run(runtime._attempt(self.uid,self.d))
        return self.st.load(self.uid)
    def test_all_measurements_before_any_move_then_single_release(self):
        d=self.invoke()
        self.assertEqual(self.calls,["reset","health",(A,True),(B,True),(A,False),(B,False),"timer"])
        self.assertTrue(d["runtime"]["released"]);self.assertEqual(self.r.xlen("cmd:"+A),1)
    def test_s7_during_preparation_schedules_retry_without_releasing(self):
        d=self.invoke(failure=(A,False))
        self.assertEqual(d["runtime"]["phase"],"retry_wait");self.assertEqual(d["runtime"]["retry_at"],"2026-11-20-18:01:00")
        self.assertEqual(self.r.xlen("cmd:"+A),0);self.assertFalse(d["runtime"]["released"])
    def test_delete_during_measurement_cannot_issue_followup_or_release(self):
        d=self.invoke(cancel_at=(A,True))
        self.assertTrue(d["runtime"]["cancel_requested"]);self.assertEqual(self.r.xlen("cmd:"+A),0)
        self.assertNotIn((A,False),self.calls)
    def test_finalize_waits_for_schedule_upload_not_diagnostic_error(self):
        self.ready();self.commit();self.mark_registered()
        fields=dict(registration_uid=self.uid)
        self.assertFalse(runtime.before_finalize(fields))
        def advance(d):
            d["snapshot"]["state"]="wrapping_up";d["state_queue"]=[];d["blocked"]={"events":{"status":"blocked"}};return d
        self.st._edit(self.uid,advance);self.assertTrue(runtime.before_finalize(fields))
        self.assertTrue(runtime.before_finalize({}))

class Compilation(Base):
    def test_all_queue_types_sorted_and_experiment_metadata_retained(self):
        m=manifest();m["commands"] += [dict(row_number=4,scanner=A,t_offset_sec=120,category="traffic",action="traffic.start",args={}),dict(row_number=5,scanner="ap1",t_offset_sec=30,category="ap",action="ap.channel.set",args={})]
        helper=module("m4Commands",_resolve_ap_sta_alias=lambda a,b:b,_resolve_operator_args=lambda s,a,b:b)
        with patch.dict(sys.modules,{"m4Commands":helper}):ops=compile_operations(m,datetime(2026,11,20,18,3),self.d["snapshot"])
        self.assertEqual([o[1] for o in ops],["cmd:"+A,"cmd:ap1","test:mobility","test:traffic"])
        for op,key,fields in ops:
            self.assertEqual(json.loads(fields["args_json"])["_experiment"]["registration_uid"],self.uid)
        self.assertEqual(ops[2][2]["execute_at"],"2026-11-20-18:05:00")
    def test_pose_preview_is_derived_not_a_measured_pose(self):
        rows=list(csv.reader(io.StringIO(planned_poses_csv(manifest()).decode())))
        self.assertEqual(rows[0][1],"intended_x_m");self.assertEqual(len(rows),3)
        self.assertEqual(self.r.hgetall("meta:"+A),{})

class DeliveryCases(Base):
    def test_cancel_before_registration_ack_is_registered_then_deleted_then_events(self):
        self.st.request_cancel(self.uid,NOW);calls=[]
        class Web:
            def send(_,path,payload,files):
                calls.append((path,copy.deepcopy(payload)))
                if path.endswith("/register"):return 200,dict(status="ok",experiment=dict(registration_uid=self.uid,session_uid="default__v000001",state="preparing"))
                if path.endswith("/delete"):return 200,dict(status="ok")
                return 200,dict(status="ok",registration_uid=self.uid,session_uid="default__v000001",acknowledged_event_ids=[e["event_id"] for e in payload["events"]])
        delivery=Delivery(self.st,Web())
        for _ in range(3):self.assertEqual(delivery.flush_once(self.uid)["status"],"acknowledged")
        self.assertEqual([p for p,v in calls],["/nms/experiment/register","/nms/experiment/delete","/nms/experiment/preparation_events"])
        self.assertTrue(self.st.load(self.uid)["delete_acknowledged"])
    def test_delete_transport_failure_preserves_exact_request_for_restart(self):
        self.mark_registered();self.st.request_cancel(self.uid,NOW);calls=[]
        class Web:
            def send(_,path,payload,files):calls.append(copy.deepcopy(payload));return 0,dict(status="timeout")
        Delivery(self.st,Web()).flush_once(self.uid)
        Delivery(Lifecycle(self.r,gate.prefix()),Web()).flush_once(self.uid)
        self.assertEqual(calls[0],calls[1]);self.assertNotIn("delete_acknowledged",self.st.load(self.uid))

class Integration(Base):
    def test_lost_lease_prevents_schedule_commit(self):
        self.ready();self.r.set(gate.prefix()+":worker_lock","other-worker")
        with self.assertRaises(Conflict):self.commit()
        self.assertEqual(self.r.xlen("cmd:"+A),0)
    def test_committed_schedule_cannot_be_failed_back_into_preparation(self):
        self.ready();self.commit()
        with self.assertRaises(Conflict):self.st.fail(self.uid,gate.OWNER,self.token,NOW,"late error")
        self.assertTrue(self.st.load(self.uid)["runtime"]["released"])
    def test_deleted_scheduler_snapshot_cannot_revive_motion_or_traffic(self):
        f=dict(action="mobility.move",args_json="{}")
        xid=self.r.xadd("test:mobility",f)
        self.assertTrue(gate.queued_row_current("test:mobility",xid,dict(f,cmd_id=xid)))
        self.r.xdel("test:mobility",xid)
        self.assertFalse(gate.queued_row_current("test:mobility",xid,dict(f,cmd_id=xid)))
    def test_actual_poll_collection_keeps_held_rows_and_delivers_health_only(self):
        self.config.CMD_EXPIRE_SEC=3600;self.config.MOBILITY_POLL_STALE_SEC=30
        env=dict(Dict=dict,Any=object,config=self.config,utility=self.utility,row_allowed=gate.row_allowed,
                 m8mobility=types.SimpleNamespace(should_block_command=lambda c:False))
        selected("m4Commands.py",{"_collect_due_commands_registered"},env)
        key=self.config.key_cmd_stream(A)
        scan=self.r.xadd(key,dict(action="scan.start",execute_at=NOW,created_at=NOW))
        move=self.r.xadd(key,dict(action="mobility.report.location",execute_at=NOW,created_at=NOW,mobility_s1_managed="1"))
        fn=env["_collect_due_commands_registered"]
        self.assertEqual(fn(A,20,NOW)["commands"],[]);self.assertEqual(self.r.xlen(key),2)
        self.begin();self.stage("health")
        health=self.r.xadd(key,dict(action="health.check.static",execute_at=NOW,created_at=NOW,preparation_uid=self.uid))
        self.assertEqual([x[0] for x in fn(A,20,NOW)["commands"]],[health])
        self.stage("preparing",robot=A)
        self.assertEqual([x[0] for x in fn(A,20,NOW)["commands"]],[move])
        self.assertEqual([x[0] for x in self.r.xrange(key)],[scan,health])
    def test_ready_reset_checks_assets_then_resets_only_participants(self):
        calls=[];self.config.KEY_PREFIX="test:"
        self.poll(A);self.poll(B)
        mobility=module("m8mobility",_ensure_mobility_assets_ready=lambda:calls.append("assets"),
                        manual_resume=lambda robot:calls.append(robot))
        state_store=module("m8mobility_state_store",key_state=lambda s:"state:"+s,key_pose=lambda s:"pose:"+s,
                           _save_stop=lambda stop,why:calls.append((stop,why)))
        self.r.hset("pose:parked",mapping={"true_location_json":"retain"})
        with patch.dict(sys.modules,{"m8mobility":mobility,"m8mobility_state_store":state_store}),patch.object(runtime,"now",return_value="2026-11-20-18:00:02"):
            runtime.reset(self.d)
        self.assertEqual(calls,["assets",A,B,(False,"")])
        self.assertEqual(self.r.hget("pose:parked","true_location_json"),"retain")
        self.assertEqual(self.r.get("test:debug:mobility_state_machine_test_enabled"),"false")
    def test_pending_preparation_does_not_restore_completion_timer(self):
        env=dict(_TERMINAL={"completed"},_fields=lambda f:f,config=self.config,
                 _guard=__import__("threading").RLock(),_stopping=False,
                 start_registration=lambda *a:self.calls.append(a))
        class Lock:
            def acquire(self,**kw):return True
            def release(self):pass
        env["lifecycle_lock"]=Lock
        selected("nms_experiment_completion.py",{"restore_pending"},env)
        env["restore_pending"]();self.assertEqual(self.calls,[])
    def test_registration_validates_before_creating_any_pending_identity(self):
        import nms_preparation_registration as registration
        from fastapi import HTTPException
        class Upload:
            filename="bad.csv"
            async def read(self):return b"bad"
        m4=module("m4Commands",__file__=str(ROOT/"m4Commands.py"),_nms_lab_id=lambda:"DemoRoom")
        mods={"m4Commands":m4,"m8mobility":module("m8mobility"),"nms_experiment_completion":module("nms_experiment_completion")}
        old=self.r.get(self.st.active_key)
        with patch.dict(sys.modules,mods),patch.object(registration,"validate",side_effect=ValueError("invalid CSV")):
            with self.assertRaises(HTTPException) as cm:asyncio.run(registration.register(Upload(),None,None))
        self.assertEqual(cm.exception.status_code,422);self.assertEqual(self.r.get(self.st.active_key),old)
    def test_registration_creates_pending_record_without_releasing_commands(self):
        import nms_preparation_registration as registration
        self.r.delete(self.st.active_key,self.config.KEY_EXPERIMENT_REGISTRY)
        class Upload:
            filename="new.csv"
            async def read(self):return b"original bytes"
        class Lock:
            def acquire(self,**kw):return True
            def owned(self):return True
            def release(self):pass
        m4=module("m4Commands",__file__=str(ROOT/"m4Commands.py"),_nms_lab_id=lambda:"DemoRoom",
            _require_empty_experiment_registry=lambda:None,m1Registry=types.SimpleNamespace(require_whitelisted=lambda s:None),
            _registration_route_previews=lambda *a:([],{"status":"ok"}))
        mods={"m4Commands":m4,"m8mobility":module("m8mobility",_get_state=lambda s:"s0idle"),
              "nms_experiment_completion":module("nms_experiment_completion",lifecycle_lock=Lock)}
        with patch.dict(sys.modules,mods),patch.object(registration,"validate",return_value=(manifest(),{"ok":True})):
            result=asyncio.run(registration.register(Upload(),None,None))
        d=self.st.load(result["registration_uid"])
        self.assertEqual(result["commands_enqueued"],0);self.assertIsNone(result["t0"])
        self.assertFalse(d["registered"]);self.assertFalse(d["runtime"]["released"])
        self.assertEqual(self.r.xlen("cmd:"+A),0)
    def test_csv_registration_endpoint_has_no_t0_and_poses_optional(self):
        tree=ast.parse((ROOT/"m4Commands.py").read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=="cmd_load_csv_file")
        self.assertEqual([a.arg for a in fn.args.args],["csv_file","initial_poses_file","session_id"])
        self.assertIsNone(fn.args.defaults[1].args[0].value)
    def test_f_g_only_rows_do_not_break_route_preview_row_numbers(self):
        import tempfile
        sys.path.insert(0,str(ROOT/"sitemap/CommonCheckers"))
        from route_reconstruct import reconstruct
        f=io.StringIO(newline="");w=csv.writer(f)
        w.writerows([["scanner","t_offset_sec","category","action","args_json","initial_robot","initial_xy",""],
            ["","","","","",A,"(5,3)",""],
            [A,0,"mobility","mobility.report.location","{}","","",""],
            [A,60,"mobility","mobility.move",'{"x_m":5,"y_m":2.8}',"","",""]])
        with tempfile.TemporaryDirectory() as temp:
            commands=Path(temp)/"commands.csv";poses=Path(temp)/"poses.csv"
            commands.write_text(f.getvalue());poses.write_bytes(planned_poses_csv(manifest()))
            result=reconstruct(commands,poses,None)
        self.assertFalse([d for d in result["diagnostics"] if d["severity"]=="error"],result["diagnostics"])
        self.assertIn(4,[e["source_line"] for e in result["robots"][A]["points"]])

class WorkerLoop(Base):
    def setup_worker(self):
        outer=self
        class Lease:
            def __init__(self,key):self.key=key
            def acquire(self,token=None,**kw):self.token=token;return outer.r.set(self.key,token,nx=True)
            def owned(self):return outer.r.get(self.key)==self.token
            def extend(self,*a,**kw):
                if not self.owned():raise RuntimeError("lost lease")
            def release(self):
                if self.owned():outer.r.delete(self.key)
        self.r.delete(gate.prefix()+":worker_lock")
        self.r.lock=lambda key,**kw:Lease(key)
        mobility=module("m8mobility",_clear_all_command_queues=lambda:self.calls.append("clear"))
        return mobility
    def test_worker_drives_one_attempt_and_restores_completion_task(self):
        mobility=self.setup_worker();self.mark_registered()
        async def attempt(uid,d):
            token=d["runtime"]["token"]
            for robot in (A,B):self.st.stage(uid,gate.OWNER,token,"preparing",NOW,ready=robot)
            self.st.stage(uid,gate.OWNER,token,"ready",NOW)
            self.st.commit(uid,gate.OWNER,token,lambda:datetime(2026,11,20,18,0,1),self.ops)
        def timer(xid,fields):
            self.calls.append("timer");self.r.xadd(self.config.KEY_EXPERIMENT_REGISTRY,dict(fields,state="completed"));self.r.xdel(self.config.KEY_EXPERIMENT_REGISTRY,xid)
        real_sleep=asyncio.sleep
        async def fast_sleep(_):await real_sleep(0)
        mods={"m8mobility":mobility,"nms_experiment_completion":module("nms_experiment_completion",start_registration=timer)}
        with patch.dict(sys.modules,mods),patch.object(runtime,"freeze"),patch.object(runtime,"quiescent",return_value=True),patch.object(runtime,"now",return_value=NOW),patch.object(runtime,"_attempt",side_effect=attempt),patch.object(runtime.asyncio,"sleep",side_effect=fast_sleep):
            asyncio.run(runtime._manage_one(self.uid))
        self.assertEqual(self.calls,["clear","timer"])
        self.assertIsNone(self.r.get(self.st.active_key));self.assertEqual(self.r.xlen("cmd:"+A),1)
    def test_cancelled_worker_resets_after_quiescence_and_never_starts_attempt(self):
        mobility=self.setup_worker();self.st.request_cancel(self.uid,NOW)
        with patch.dict(sys.modules,{"m8mobility":mobility}),patch.object(runtime,"freeze"),patch.object(runtime,"quiescent",return_value=True),patch.object(runtime,"reset") as reset,patch.object(runtime,"_attempt") as attempt:
            asyncio.run(runtime._manage_one(self.uid))
        reset.assert_called_once();attempt.assert_not_called();self.assertIsNone(self.r.get(self.st.active_key))
    def test_missing_post_cancel_poll_retains_gate_without_reset_or_release(self):
        mobility=self.setup_worker();self.st.request_cancel(self.uid,NOW)
        async def stop_after_iteration(_):runtime._stopping=True
        try:
            with patch.dict(sys.modules,{"m8mobility":mobility}),patch.object(runtime,"freeze"),patch.object(runtime,"quiescent",return_value=False),patch.object(runtime,"reset") as reset,patch.object(runtime.asyncio,"sleep",side_effect=stop_after_iteration):
                asyncio.run(runtime._manage_one(self.uid))
            reset.assert_not_called();self.assertTrue(gate.held());self.assertEqual(self.r.xlen("cmd:"+A),0)
        finally:runtime._stopping=False

class PublicValidator(unittest.TestCase):
    def raw(self,action="mobility.move",args='{"x_m":5,"y_m":2.8}'):
        f=io.StringIO(newline="");csv.writer(f).writerows([
            ["scanner","t_offset_sec","category","action","args_json","initial_robot","initial_xy",""],
            ["","","","","",A,"(5,3)",""],
            [A,0,"mobility","mobility.report.location","{}","","",""],
            [A,240,"mobility",action,args,"","",""]])
        return f.getvalue().encode()
    def run_validation(self,raw):
        from nms_preparation_registration import validate
        helper=module("preferred_direction_helper",lookup_preferred_direction=lambda **kw:dict(ok=True,preferred_heading_deg=270))
        with patch.dict(sys.modules,{"preferred_direction_helper":helper}):
            return validate(raw,None,ROOT,"DemoRoom")
    def test_inline_roster_uses_real_safety_checker_and_preserves_row_number(self):
        m,r=self.run_validation(self.raw());self.assertTrue(r["ok"])
        self.assertEqual([c["row_number"] for c in m["commands"]],[3,4])
    def test_forbidden_primitive_is_still_rejected(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as cm:self.run_validation(self.raw("mobility.turn",'{"angle_deg":90}'))
        self.assertEqual(cm.exception.status_code,422)
    def test_path_into_restriction_still_rejected(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):self.run_validation(self.raw(args='{"x_m":0,"y_m":0}'))

if __name__=="__main__":unittest.main(verbosity=2)
