"""Offline Phase B replay. Original input from trace; post-stop reports and solver results are simulated."""
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch
import json
import sys
import unittest

import s8_step2_check as base
import s8_step3_check as step3
sys.path.insert(0,str(base.ROOT))
import m8mobility_s8_view as view


class Pipe:
    """In-memory Redis transaction boundary; run the actual production advance() logic."""
    def __init__(self, store): self.r=store; self.ops=[]
    def __enter__(self): return self
    def __exit__(self,*_): pass
    def watch(self,*_): pass
    def hget(self,*a): return self.r.hget(*a)
    def multi(self): pass
    def hset(self,*a): self.ops.append((self.r.hset,a))
    def xadd(self,*a): self.ops.append((self.r.xadd,a))
    def execute(self):
        for f,a in self.ops: f(*a)


class View(unittest.TestCase):
    call, loc = base.Step2.call, staticmethod(base.Step2.loc)
    poll = base.Step2.poll
    def setUp(self):
        step3.Landing.setUp(self)
        fx=json.loads(Path(__file__).with_name("phase_b_view_input.json").read_text())
        self.fx=fx; self.shot=fx["snapshot"]
        self.shot.update(s8_start_trusted=fx["start_evidence"]["confidence_level"]=="HIGH",
                         s8_start_report_finished_ts=fx["prior_finished"])
        self.target=self.shot["planned_pose"]; self.pose=self.fx["suspect"]
        self.pose.update(confidence={"level":self.pose["confidence_level"]})
        self.raw=fx["report"];self.macro={};self.h['state','last_s0_command_action']='mobility.move'
        self.config.MOBILITY_S8_VIEW_ENABLED=True
        self.config.mobility_site_dir=lambda:self.site; self.commands=[]
        self.store.hget=lambda k,f:self.h.get((k,f));self.store.hset=lambda k,f,v:self.h.update({(k,f):v})
        def add(k,row):
            i=str(len(self.commands)+1)
            self.commands.append(dict(row));self.q[k][i]=row
        self.store.xadd=add; self.store.pipeline=lambda:Pipe(self.store)
        self.util.local_ts=lambda:'2026-11-11T23:04:03'
        self.n.update(_trace_active_snapshot=lambda s:self.shot,_trace_active_id=lambda s:fx["trace_id"],
            view_rules=view,_propagation_allowed=lambda s:True, sys=sys, __name__="s8_fake_sm")
        base.load("m8mobility_state.py",["_s3_extract_visible_tags"],self.n)
        self.util._wrap_angle_deg=lambda x:(x+180)%360-180
        self.solved=self.loc(4.04,3.01,'HIGH'); self.solved.update(heading_deg=175.,tags_used=[41,42,43])
        self.solver_inputs=[]
        def solve(**kw):
            self.solver_inputs.append(kw["visible_tags"]);return self.solved
        self.n["solve_robot_location"]=solve
        self.sm=NS(**self.n);self.n["state_api"]=self.sm
        mods=patch.dict(sys.modules,{"s8_fake_sm":self.sm,"redis.exceptions":NS(WatchError=RuntimeError)})
        mods.start();self.addCleanup(mods.stop)
        self.target0=self.target.copy()
    def begin(self):
        with redirect_stdout(StringIO()):
            return self.call("run_state_machine")["state"]
    def take(self,action):
        rows=self.poll();self.assertEqual(len(rows),1); f=rows[0][1]
        self.assertEqual(f["action"],action)
        self.assertEqual(self.poll(),[])
        return f
    def notify(self,row):
        return self.call("on_report_received",row)
    def turned(self):
        self.assertEqual(self.begin(),"s8recovery")
        f=self.take(view.TURN)
        args=json.loads(f["args_json"])
        self.assertAlmostEqual(args["angle_deg"],-60.,places=2)
        t=self.raw["last_command_finished_ts"]
        self.turn_result=dict(last_command=view.TURN,last_command_args=args,last_exec_status="completed",
                              last_command_received_ts=t+10,last_command_finished_ts=t+19)
        self.notify(self.turn_result)
        self.notify(self.turn_result)
        self.take(view.REPORT)
    def fresh(self):
        t=self.turn_result["last_command_finished_ts"]+10
        paths={role:f"/tmp/location_{role}_{int((t+i)*1000)}.jpg" for role,i in [("front",1),("rear",3)]}
        tags=[dict(id=i,camera_role="front",snapshot_path=paths["front"],
                   calibrated_pose=dict(angle_deg=i-41,yaw_deg=5,distance_m=2.1)) for i in (41,42,43)]
        loc=dict(ok=True,capture_mode="dual_camera",snapshots=paths,apriltag=dict(ok=True,tags=tags))
        for role in paths:loc[role]=dict(snapshot=dict(ok=True,snapshot_path=paths[role]))
        return dict(last_command=view.REPORT,last_command_args={},last_exec_status="completed",
                    last_command_received_ts=t,last_command_finished_ts=t+6,last_location_result=loc)
    def test_recorded_input_to_fresh_view(self):
        self.turned();self.assertEqual(self.notify(self.fresh())["state"],"s0idle")
        self.assertEqual(self.target0,self.target);self.assertEqual(len(self.commands),2)
        self.assertEqual([v["id"] for v in self.solver_inputs[0]],[41,42,43])
        self.assertEqual(self.trace[-1],"accepted_without_correction")
        print("Phase B replay: trusted grid (4.1, 3.0), alternate 175 deg, estimated turn -60 deg; simulated HIGH report -> S5 -> S0.")
    def test_unchanged_reports_do_not_advance(self):
        self.begin();self.notify(self.raw);self.take(view.TURN); self.notify(self.raw)
        self.assertFalse(self.poll());self.now+=121;self.n["_check_s8_deadlines"]()
        self.assertEqual(self.call("_get_state"),"s7stopped")
    def test_block_at_delivery(self):
        self.begin();self.peer[30,40]=1
        self.assertEqual(self.poll(),[]);self.assertTrue(self.stop["stop"])
    def test_missing_alternate(self):
        with patch.object(view,"lookup",side_effect=ValueError("no alternate")):
            self.assertEqual(self.begin(),"s7stopped")
        self.assertFalse(self.commands)
    def test_untrusted_start(self):
        self.shot["s8_start_trusted"]=False;self.assertEqual(self.begin(),"s7stopped")
    def test_heading_disagrees(self):
        self.pose["heading_deg"]=100;self.assertEqual(self.begin(),"s7stopped")
    def test_forged_child(self):
        self.begin();ep=self.call("_s8_episode");row=self.commands[0].copy();row["s8_view_ticket"]="old"
        self.assertTrue(self.call("movement_delivery_blocked",row))
        row=self.commands[0].copy();row["args_json"]='{"angle_deg":90}'
        self.assertTrue(self.call("movement_delivery_blocked",row))
    def test_turn_fails(self):
        self.begin();f=self.take(view.TURN)
        t=self.raw["last_command_finished_ts"]+2
        self.notify(dict(last_command=view.TURN,last_command_args=json.loads(f["args_json"]),
            last_command_received_ts=t,last_command_finished_ts=t+1,last_exec_status="failed",last_error_code="TURN_EXEC_FAIL"))
        self.assertTrue(self.stop["stop"]);self.assertEqual(len(self.commands),1)
    def test_stale_camera(self):
        self.turned();rep=self.fresh();rep["last_location_result"]["snapshots"]["front"]="/tmp/location_front_1.jpg"
        self.assertEqual(self.notify(rep)["state"],"s7stopped");self.assertFalse(self.solver_inputs)
    def test_wrong_report_id(self):
        self.turned();r=self.fresh();r["last_command_id"]="foreign"
        self.assertEqual(self.notify(r)["state"],"s7stopped")

    def test_ordinary_correction_after_fresh_report(self):
        self.turned()
        self.solved["x_m"]=4.17
        out=self.notify(self.fresh())
        self.assertEqual(out["state"],"s1waiting_report")
        self.assertEqual(len(self.saved),1)
        self.assertEqual(self.saved[0][0],"mobility.turn_move_turn.forward")
        self.assertLessEqual(self.saved[0][1]["distance_m"],.30)

    def test_still_low(self):
        self.turned();self.solved["confidence"]["level"]="LOW"
        self.assertEqual(self.notify(self.fresh())["state"],"s7stopped")
    def test_no_new_tags(self):
        self.turned();self.solved["tags_used"]=[44,47,48]
        self.assertEqual(self.notify(self.fresh())["state"],"s7stopped")
    def test_impossible_pose(self):
        self.turned();self.solved["x_m"]+=1
        self.assertEqual(self.notify(self.fresh())["state"],"s7stopped")
    def test_next_row_held(self):
        self.begin(); row=dict(scanner=self.robot,action="mobility.move",args_json="{}");self.q["schedule"]["next"]=row
        self.n["_dispatch_due_mobility_command"]("next",row);self.assertIn("next",self.q["schedule"])
    def test_global_stop(self):
        self.begin();self.stop.update(stop=True,reason="operator");self.n["_check_s8_deadlines"]()
        self.assertEqual(self.poll(),[]);self.assertEqual(self.stop["reason"],"operator")


if __name__=="__main__":
    unittest.main(verbosity=2)
