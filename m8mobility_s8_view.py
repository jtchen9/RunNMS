"""Step 5: one alternate view on a short, completed ordinary movement. Default off."""
import hashlib
import json
import math
import re
import uuid
from pathlib import Path

from m8mobility_s8_landing import measured, number, ACTION

TURN = "mobility.turn"
REPORT = "mobility.report.location"
FLAG = "MOBILITY_S8_VIEW_ENABLED"
FIELD = "s8_view_tx_json"
WAIT = {"status": "waiting"}


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def delta(a, b):
    return (a - b + 180) % 360 - 180


def xy_dist(a, b):
    return math.hypot(a["x_m"] - b["x_m"], a["y_m"] - b["y_m"])


def lookup(site, x, y):
    from tools.check_preferred_heading import check_preferred_heading
    import numpy as np
    folder = Path(site) / "preferred_direction_lut"
    with np.load(folder / "alternative_heading_v1.npz", allow_pickle=False) as f:
        fingerprint = json.loads(f["metadata"].item())["tag_map_sha256"]
    require(hashlib.sha256((Path(site)/"tag_location.txt").read_bytes()).hexdigest() == fingerprint,
            "site tag map changed since alternate LUT build")
    row = check_preferred_heading(folder / "preferred_heading_deg.npy", x, y)
    require(row["status"] == "ok", "no validated alternate at the trusted start cell")
    return row


def clear(sm, scanner, start, end):
    ok, blocked, debug = sm._is_path_clear_debug(start["x_m"], start["y_m"], end["x_m"], end["y_m"],
                                                 exclude_scanner=scanner)
    require(ok and not blocked and debug.get("sample_count", 0) > 0
            and not sm.ramp_restriction_movement_issues(start, end, sm._load_runtime_ramp_policy()),
            "runtime map, peer or ramp blocks recovery")


def make_plan(sm, scanner, e, report):
    shot = sm._trace_active_snapshot(scanner)
    start, end = shot["start_pose"], shot["simulated_endpoint"]
    suspect = e["suspect_or_landing_pose"]
    args = shot["args"]
    require(shot.get("s8_start_trusted") is True and start == e["pre_move_pose"]
            and shot["initial_script_execution"] is True and shot["command_kind"] == "s5_movement"
            and shot["action"] == report.get("last_command") == ACTION
            and set(args) == {"pre_angle", "distance_m", "post_angle"}
            and report.get("last_command_args") == args and report.get("last_exec_status") == "completed"
            and not report.get("last_error_code"), "ordinary completed parent move not verified")
    require(all(number(p.get(k)) for p in (start,end,suspect) for k in ("x_m","y_m","heading_deg"))
            and all(number(v) for v in args.values()) and 0 < args["distance_m"] <= .30
            and abs(xy_dist(start,end) - args["distance_m"]) < .001
            and xy_dist(end,e["original_target"]) < .001, "parent geometry or distance unsupported")
    received, finish = report.get("last_command_received_ts"), report.get("last_command_finished_ts")
    prev = shot.get("s8_start_report_finished_ts")
    require(all(number(v) for v in (prev,received,finish)) and 0 < prev < received <= finish
            and received-prev <= 300, "no recent trusted pre-move report")
    planned_heading = (start["heading_deg"] + args["pre_angle"] + args["post_angle"]) % 360
    require(abs(delta(planned_heading,end["heading_deg"])) < .01
            and abs(delta(planned_heading,suspect["heading_deg"])) <= 25,
            "post-move heading estimates disagree")
    # No translation. Check the prior short path and both possible position estimates.
    clear(sm, scanner, start, end)
    clear(sm, scanner, end, suspect)
    view = lookup(sm.config.mobility_site_dir(), start["x_m"], start["y_m"])
    heading = view["alternative_heading_deg"]
    turn = delta(heading, planned_heading)
    predicted = sorted(set(view["alternative_tags"]["front"] + view["alternative_tags"]["rear"]))
    old = suspect.get("tags_observed") or suspect.get("tags_used")
    require(isinstance(old,list) and old and len(set(predicted)-set(old)) >= 2
            and 30 <= abs(turn) <= 135, "alternate lacks a useful bounded turn or tag diversity")
    return dict(angle=float(turn), heading=heading, prior_finish=finish, start=start, end=end,
                old_ids=old, predicted_ids=predicted, grid=[view["grid_x_m"],view["grid_y_m"]])


def read_tx(sm, scanner, e):
    raw = sm.config.r.hget(sm.key_state(scanner), FIELD)
    t = json.loads(raw) if raw else {}
    return (raw, t if t.get("episode") == e["id"] else {})


def advance(sm, scanner, e, prior, new, action=None, args=None):
    """Compare-and-swap the recovery phase and enqueue its child in the same transaction."""
    from redis.exceptions import WatchError
    r, key = sm.config.r, sm.key_state(scanner)
    with r.pipeline() as p:
        try:
            p.watch(key)
            if (p.hget(key, FIELD) != prior or p.hget(key,"state") != sm.S8_RECOVERY
                or json.loads(p.hget(key,"s8_episode_json") or "{}").get("id") != e["id"]
                or sm._load_stop().get("stop") or sm._s8_expired(e)):
                return False
            new = dict(new,episode=e["id"])
            if action is not None:
                new.update(action=action,args=args,ticket=uuid.uuid4().hex)
            p.multi()
            p.hset(key,FIELD,json.dumps(new))
            if action is not None:
                stamp=sm.utility.local_ts()
                p.xadd(sm.config.key_cmd_stream(scanner),dict(category="mobility",action=action,
                    args_json=json.dumps(args),execute_at=stamp,created_at=stamp,mobility_s1_managed="1",
                    s8_episode_id=e["id"],s8_view_ticket=new["ticket"]))
            p.execute()
            try:
                sm.append_trace_event(event="s8_view_phase",scanner=scanner,trace_id=e["original_trace"],
                    data={"episode_id":e["id"], "phase":new["phase"], "grid":new["grid"],
                          "heading_deg":new["heading"], "turn_deg":new["angle"],
                          "predicted_tag_ids":new["predicted_ids"], "robot_cmd_id":new.get("cmd_id"),
                          "verified_pose":new.get("verified_pose")})
            except Exception:
                pass  # Passive trace cannot change the result of a committed transaction.
            return True
        except WatchError:
            return False  # deadline remains active; a child is never replayed


def admit(sm, scanner, e, fields):
    # Only called by the final robot-poll gate, after claiming the stream row with XDEL.
    raw,t = read_tx(sm,scanner,e)
    if (not getattr(sm.config,FLAG,False) or not t or t["phase"] not in ("turn_queued","report_queued")
            or fields.get("s8_episode_id") != e["id"] or fields.get("s8_view_ticket") != t.get("ticket")
            or fields.get("action") != t["action"] or fields.get("mobility_s1_managed") != "1"
            or json.loads(fields.get("args_json") or "{}") != t["args"]):
        return False
    if t["phase"] == "turn_queued":
        clear(sm,scanner,t["start"],t["end"])
        clear(sm,scanner,t["end"],e["suspect_or_landing_pose"])
    return advance(sm,scanner,e,raw,dict(t,phase=t["phase"].replace("_queued","_sent"),
                                        cmd_id=fields["cmd_id"]))


def capture(report, received, finished):
    loc = report.get("last_location_result") or {}
    require(loc.get("ok") is True and loc.get("capture_mode") == "dual_camera", "fresh dual capture missing")
    paths = loc.get("snapshots") or {}
    for role in ("front","rear"):
        camera=loc.get(role) or {}
        m=re.fullmatch(r".*/location_"+role+r"_(\d+)\.jpg", str(paths.get(role,"")))
        require(m and camera.get("snapshot",{}).get("ok") is True
                and camera["snapshot"].get("snapshot_path") == paths[role]
                and int(received*1000) <= int(m[1]) <= math.ceil(finished*1000),
                "snapshot is missing, stale, or outside the report command")
    tags=(loc.get("apriltag") or {}).get("tags") or []
    require(tags and all(t.get("camera_role") in paths and t.get("snapshot_path") == paths[t["camera_role"]]
                         for t in tags), "tag evidence not bound to this capture")
    return loc


def step(sm, scanner, e, report=None):
    require(e.get("kind") == "check_alternate_view" and e["original_action"] == "mobility.move"
            and e.get("original_trace") and e["original_trace"] == sm._trace_active_id(scanner)
            and not sm._load_site_macro_context(scanner) and not sm._load_s0_command_arg_overrides(scanner)
            and sm._get_correction_counter(scanner)==0 and sm._load_planned(scanner)==e["original_target"]
            and sm._load_true(scanner)==e["suspect_or_landing_pose"] and sm._propagation_allowed(scanner),
            "recovery parent changed or earlier mobility fault")
    raw,t=read_tx(sm,scanner,e)
    if not t:
        if report is not None:
            return WAIT  # only the S5 entry callback may plan the turn
        p=make_plan(sm,scanner,e,sm._load_report_json(scanner))
        advance(sm,scanner,e,raw,dict(p,phase="turn_queued"),TURN,{"angle_deg":p["angle"]})
        return WAIT
    if report is None or t["phase"] == "verified":
        return WAIT
    rec,fin=report.get("last_command_received_ts"), report.get("last_command_finished_ts")
    if not number(rec) or rec <= t["prior_finish"]:
        return WAIT  # previous command or heartbeat
    require(t["phase"] in ("turn_sent","report_sent") and report.get("last_command")==t["action"]
            and report.get("last_command_args")==t["args"], "unexpected newer mobility report")
    echoed = report.get("last_command_id") or report.get("cmd_id")
    require(not echoed or str(echoed) == str(t.get("cmd_id")), "robot echoed another command ID")
    require(not report.get("last_error_code") and report.get("last_exec_status") not in ("failed","error","rejected","busy"),
            "recovery child reported execution error")
    if report.get("last_exec_status") != "completed":
        return WAIT
    require(number(fin) and rec <= fin, "invalid child completion time")
    if t["phase"]=="turn_sent":
        advance(sm,scanner,e,raw,dict(t,phase="report_queued",prior_finish=fin),REPORT,{})
        return WAIT
    capture(report,rec,fin)
    pose=sm.solve_robot_location(visible_tags=sm._s3_extract_visible_tags(scanner,report=report),
                                 sample_uid=f"s8_{scanner}_{e['id']}_{t['ticket']}")
    require(measured(pose) and abs(delta(pose["heading_deg"],t["heading"])) <= 25
            and xy_dist(pose,t["end"]) <= .30 and xy_dist(pose,t["start"]) <= .45,
            "new solve is weak or inconsistent with the turn/short move")
    used=set(pose.get("tags_used") or [])
    require(len((used & set(t["predicted_ids"])) - set(t["old_ids"])) >= 2,
            "fresh solve lacks two newly predicted tags")
    clear(sm,scanner,pose,e["original_target"])
    if not advance(sm,scanner,e,raw,dict(t,phase="verified",
            verified_pose={k:pose[k] for k in ("x_m","y_m","heading_deg","tags_used")})):
        return WAIT
    return dict(status="observed",episode_id=e["id"],verified=True,pose=pose)
