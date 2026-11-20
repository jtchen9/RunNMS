"""Display-only preparation progress. Numbers 1-6; failure is not Step 7."""
from copy import deepcopy

STEPS = {
    "health_check": (1, "Checking robot health"),
    "measuring_location": (2, "Measuring current location"),
    "checking_path": (3, "Checking path to starting position"),
    "moving_to_start": (4, "Moving to starting position"),
    "confirming_location": (5, "Confirming arrival location"),
    "assigning_start_time": (6, "Assigning experiment start time"),
}

def entry(step, now, previous=None, status="active"):
    number, label = STEPS[step]
    old = previous or {}
    return dict(step=step, step_number=number, step_label=label, status=status,
                step_started_at=old.get("step_started_at", now) if old.get("step")==step else now)

def describe(rt, previous, now):
    robots = [p["scanner"] for p in rt["manifest"]["initial_positions"]]
    phase = rt["phase"]
    rows = deepcopy(rt.get("robot_progress", {}))
    if phase == "health":
        rows = {r:entry("health_check",now,(previous.get("robot_steps") or {}).get(r)) for r in robots}
        step = "health_check"
    elif phase == "preparing":
        for r in robots:
            rows.setdefault(r, dict(step=None,step_number=None,step_label="Waiting",status="waiting",step_started_at=now))
            if r in rt["ready"]:rows[r].update(status="ready",step=None,step_number=None,step_label="Ready")
        running = [v for r,v in rows.items() if r not in rt["ready"] and v.get("step_number")]
        step = min(running,key=lambda v:v["step_number"])["step"] if running else None
    elif phase == "ready":
        rows = {r:dict(step=None,step_number=None,step_label="Ready",status="ready",step_started_at=(rows.get(r) or {}).get("step_started_at",now)) for r in robots}
        step = "assigning_start_time"
    else:
        step = None
        for r in robots:
            rows.setdefault(r,dict(step=None,step_number=None,step_label="Waiting",step_started_at=now))
            rows[r]["status"] = ("failed" if r == rt.get("failed_robot") else "paused") if phase=="retry_wait" else "waiting"
    labels = {"awaiting_attempt":"Waiting for robot connections", "resetting":"Resetting preparation state",
              "retry_wait":"Preparation failed — waiting to retry", "cancelling":"Cancelling preparation"}
    number,label = STEPS[step] if step else (None,labels.get(phase,"Preparing robots"))
    same = previous.get("step")==step and previous.get("subphase")==phase and previous.get("attempt")==rt["attempt"]
    return dict(step=step,step_number=number,step_label=label,
                step_started_at=previous.get("step_started_at",now) if same else now,
                robot_steps=rows, active_robots=list(rt.get("active_robots") or ([rt["active_robot"]] if rt.get("active_robot") else [])))
