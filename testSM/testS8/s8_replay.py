"""Offline design replay, not an NMS patch. Run this file in VS Code (Python 3.9+).
The embedded events come from the uploaded Phase A and Phase B Bravo traces.
Supply trace paths under __main__ to reproduce the input records from scratch.
Clearance, fresh retry results and scheduler cases in the tests are hypothetical.
No Redis, NMS, robot connection or third-party package is used.
"""
import json
import math
from pathlib import Path

LIMIT = 0.30             # normal first-correction maximum in m
ARRIVAL = 0.15           # position acceptance band in this replay
NEAR_MISS = 0.301        # proposed narrow Phase A exception
EPS = 1e-10              # for the calculated remaining-distance comparison only

def replay_inputs(path):
    history = {}
    for line in Path(path).read_text(encoding="utf-8-sig").splitlines():
        if not line.strip():
            continue
        r = json.loads(line)
        if r.get("scanner") != "twin-scout-bravo":
            continue
        ctx = history.setdefault(r["trace_id"], {})
        d = r["data"]
        ctx[r["event"]] = d
        if r["event"] == "s5_followup_decision" and d.get("outcome") == "stopped_before_first_correction":
            src = ctx["s6_command_issued"]["args"].get("move_profile","default")
            loc = d["solved_true_pose"]
            yield dict(time=r["logged_at"], confidence=loc["confidence_level"],
                       measured=loc.get("source")=="apriltag_componentwise" and loc["location_ok"],
                       completed=ctx["mobility_report_received"]["last_exec_status"]=="completed",
                       count=d["correction_attempt_count"], residual=d["pose_error_to_planned"]["dpos_m"],
                       y=loc["y_m"], profile=src)

def first_gate(row, *, alternate_view=False, turn_area_verified=False, path_verified=False,
               valid_landing=False, stopped=False, expired=False):
    """Scoped to the reviewed first-correction branch; no other stop is intercepted.
    A verified turn area must cover the plausible pose envelope, not just the LUT cell.
    valid_landing is external: the live macro's landing gate passed with fresh evidence.
    """
    if stopped or expired: return "STOP"
    d=row["residual"]
    if not math.isfinite(d) or d<0: return "STOP"
    if row["count"]!=0 or d<=LIMIT: return "NORMAL_POLICY"
    if not row["completed"] or not row["measured"]: return "STOP"
    if row["profile"] not in ("default","bump_crossing_up","bump_crossing_down"): return "STOP"
    if row["profile"]!="default" and not valid_landing: return "STOP"
    if row["confidence"]=="HIGH" and d<=NEAR_MISS:
        return "ONE_CHECKED_CORRECTION" if path_verified else "STOP"
    if row["confidence"]=="LOW" and row["profile"]=="default":
        return "ONE_IN_PLACE_TURN" if alternate_view and turn_area_verified else "STOP"
    return "STOP"

def after_turn(residual, *, high_confidence, fresh_correlated, credible_pose,
               path_verified, time_left, stopped=False):
    """Only for completion of the single S8 visibility turn. Does not loop."""
    if (stopped or not time_left or not high_confidence or not fresh_correlated
        or not credible_pose or not math.isfinite(residual) or residual<0): return "STOP"
    if residual<=ARRIVAL: return "RETURN_TO_S5"
    # One translation only. Planner must validate the actual segment and stopping point.
    if not path_verified or residual-min(residual,LIMIT)>ARRIVAL+EPS: return "STOP"
    return "ONE_CHECKED_CORRECTION"

def new_intent(state, *, stopped=False, reserved=False):
    """Abstract admission decision for NEW scripted mobility intents only.
    Must be atomic with reserving the idle robot when implemented; this is not a queue.
    """
    if stopped or state=="s7stopped": return "STOP"
    if state=="s0idle" and not reserved: return "ADMIT"
    return "HOLD"

FIXTURES = [{'time': '2026-11-10-08:04:18.883607', 'confidence': 'HIGH', 'measured': True, 'completed': True, 'count': 0, 'residual': 0.3005525475070023, 'y': 6.105320840443929, 'profile': 'bump_crossing_up'}, {'time': '2026-11-11-23:04:03.265768', 'confidence': 'LOW', 'measured': True, 'completed': True, 'count': 0, 'residual': 0.715825174065671, 'y': 2.902586652580927, 'profile': 'default'}]

def run(rows):
    a,b=rows
    assert a["profile"]=="bump_crossing_up" and a["y"]>6.0 and a["confidence"]=="HIGH"
    assert b["profile"]=="default" and b["confidence"]=="LOW"
    assert first_gate(a,valid_landing=True,path_verified=True)=="ONE_CHECKED_CORRECTION"
    assert first_gate(a,valid_landing=False,path_verified=True)=="STOP"
    assert first_gate(a,valid_landing=True,path_verified=False)=="STOP"
    assert first_gate(b,alternate_view=True,turn_area_verified=False)=="STOP"
    assert first_gate(b,alternate_view=False,turn_area_verified=True)=="STOP"
    assert first_gate(b,alternate_view=True,turn_area_verified=True)=="ONE_IN_PLACE_TURN"
    good=dict(high_confidence=True,fresh_correlated=True,credible_pose=True,path_verified=True,time_left=True)
    for d,want in [(0.099,"RETURN_TO_S5"),(.25,"ONE_CHECKED_CORRECTION"),(.36,"ONE_CHECKED_CORRECTION"),(b["residual"],"STOP")]:
        assert after_turn(d,**good)==want
    for k in good:
        assert after_turn(.36,**{**good,k:False})=="STOP"
    assert after_turn(.36,stopped=True,**good)=="STOP"
    assert new_intent("s8recovery")=="HOLD" and new_intent("s1waiting_report")=="HOLD"
    assert new_intent("s0idle",reserved=True)=="HOLD" and new_intent("s0idle")=="ADMIT"
    assert new_intent("s7stopped")=="STOP" and new_intent("s0idle",stopped=True)=="STOP"
    for row in rows: print(row["time"],row["confidence"],f'{row["residual"]:.6f} m',row["profile"])
    print("PASS: two recorded stop inputs; conditional recovery and new-intent admission cases. No commands issued.")

if __name__=="__main__":
    TRACE_PATHS=[]  # optional: [r"path/to/PhaseA.jsonl", r"path/to/PhaseB.jsonl"]
    run([r for p in TRACE_PATHS for r in replay_inputs(p)] if TRACE_PATHS else FIXTURES)
