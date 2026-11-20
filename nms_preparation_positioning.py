"""One preparation attempt using the existing mobility subsystem.

Used by the Step 5 lifecycle worker, which owns admission of all experiment
commands, persists registration/attempt identity, and reconciles outstanding
work on cancellation/restart before retrying.
No experiment queue, t0, global reset, shutdown, automatic retry or S8 here.
"""
import asyncio
from copy import deepcopy
from datetime import datetime
import math
import time
import uuid
from nms_preparation_gate import serialized as preparation_serialized

TIME_FMT = "%Y-%m-%d-%H:%M:%S"


class PositioningError(ValueError):
    pass


def _pose(value):
    if not isinstance(value, dict) or value.get("location_ok") is not True:
        raise PositioningError("No valid measured location")
    if value.get("source") not in {"apriltag", "apriltag_componentwise"}:
        raise PositioningError("Preparation requires a measured AprilTag location")
    try:
        xyz = tuple(float(value[k]) for k in ("x_m", "y_m", "heading_deg"))
    except (KeyError, TypeError, ValueError) as exc:
        raise PositioningError("Incomplete measured pose") from exc
    if not all(math.isfinite(v) for v in xyz):
        raise PositioningError("Nonfinite measured pose")
    return deepcopy(value)


def _captures(report):
    result = report.get("last_location_result") or {}
    snaps = result.get("snapshots") or {}
    return {str(k): str(v) for k, v in snaps.items() if v} if isinstance(snaps, dict) else {}


def preparation_report_fresh(report, issued_at, previous_report):
    """S1-only scoped freshness check, before S3/S5 can act on this report."""
    try:
        issued = datetime.strptime(issued_at, TIME_FMT)
        received = datetime.strptime(report["last_command_received_ts"], TIME_FMT)
        finished = datetime.strptime(report["last_command_finished_ts"], TIME_FMT)
        if not issued <= received <= finished:
            return False, "preparation report predates this issued command"
    except (KeyError, TypeError, ValueError):
        return False, "preparation report lacks valid execution timestamps"
    # Fresh errors still reach existing S2 error/collision handling.
    if report.get("last_exec_status") == "completed":
        captures = _captures(report)
        if not captures:
            return False, "preparation report has no snapshot identities"
        if set(captures.values()) & set(_captures(previous_report or {}).values()):
            return False, "preparation report reuses old snapshots"
    return True, ""


def measured_evidence(snapshot, *, scope, since, previous=None):
    """Require a new accepted S3 solve, fresh execution and camera captures.

    Timestamp receipt alone is insufficient: robots resend old reports in polls.
    Confidence policy remains the existing solver's; LOW alone is not a veto.
    """
    evidence = snapshot.get("evidence") or {}
    previous = previous or {}
    if evidence.get("scope") != scope or not evidence.get("solve_id"):
        raise PositioningError("Missing evidence for this preparation attempt")
    if evidence["solve_id"] == previous.get("solve_id"):
        raise PositioningError("No new accepted location solve")
    pose = _pose(snapshot.get("pose"))
    if evidence.get("pose") != snapshot.get("pose"):
        raise PositioningError("Measured evidence does not match the current pose")
    report = evidence.get("report") or {}
    if report.get("last_exec_status") != "completed":
        raise PositioningError("Location command did not complete successfully")
    result = report.get("last_location_result") or {}
    if result.get("ok") is not True:
        raise PositioningError("Location capture failed")
    try:
        issued = datetime.strptime(evidence["issued_at"], TIME_FMT)
        received = datetime.strptime(report["last_command_received_ts"], TIME_FMT)
        finished = datetime.strptime(report["last_command_finished_ts"], TIME_FMT)
        solved = datetime.strptime(evidence["solved_at"], TIME_FMT)
        earliest = datetime.strptime(since, TIME_FMT)
    except (KeyError, TypeError, ValueError) as exc:
        raise PositioningError("Missing or invalid command/capture timestamps") from exc
    if not earliest <= issued <= received <= finished <= solved:
        raise PositioningError("Stale or inconsistent location timestamps")
    captures = _captures(report)
    if not captures:
        raise PositioningError("Location evidence has no camera snapshot identities")
    old_captures = _captures(previous.get("report") or {})
    if set(captures.values()) & set(old_captures.values()):
        raise PositioningError("Camera snapshot reused from the preceding solve")
    return pose, deepcopy(evidence)


async def position_once(*, scanner, target, port, owns_attempt,
                        timeout_seconds=240, measure_only=False, monotonic=time.monotonic,
                        sleep=asyncio.sleep):
    """Report -> <=1m/path gate -> ordinary move -> fresh confirmation.

    The S0/S5/S6 machinery owns motion and its normal correction thresholds.
    A final independent report is used for preparation acceptance. No result
    overwrites measured pose with target coordinates. Failure returns to the
    caller; it never resets a robot, clears stop, or retries the movement.
    timeout_seconds bounds the whole attempt, not each phase separately.
    """
    try:
        xy = {k: float(target[k]) for k in ("x_m", "y_m")}
        if not all(math.isfinite(v) for v in xy.values()): raise ValueError()
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0: raise ValueError()
    except (KeyError, TypeError, ValueError):
        return {"status": "failed", "scanner": scanner, "reason": "Invalid target or time budget"}
    scope = uuid.uuid4().hex
    deadline = monotonic() + timeout_seconds
    opened = False
    phase = "admission"
    diagnostics = {}

    def guard():
        if not owns_attempt(): raise PositioningError("Preparation ownership changed or was cancelled")
        if monotonic() >= deadline: raise PositioningError("Preparation attempt timed out")

    def admission_current():
        return owns_attempt() and monotonic() < deadline

    async def issue_and_wait(action, args):
        guard()
        since = port.local_ts()
        result = port.issue(scanner, action, args, scope=scope, owns_attempt=admission_current)
        if result.get("status") not in {"ok", "waiting", "retry"}:
            raise PositioningError(result.get("detail") or "Mobility command was not admitted")
        while True:
            guard()
            snap = port.snapshot(scanner)
            if snap.get("stop") or snap.get("state") == "s7stopped":
                raise PositioningError(snap.get("reason") or "Mobility stopped")
            if snap.get("state") == "s0idle": return snap, since
            await sleep(0.25)

    try:
        guard()
        port.open(scanner, scope=scope, owns_attempt=admission_current)
        opened = True
        before = port.snapshot(scanner).get("evidence") or {}
        phase = "initial_location"
        snap, since = await issue_and_wait("mobility.report.location", {})
        start_pose, initial_evidence = measured_evidence(snap, scope=scope, since=since, previous=before)
        distance = math.hypot(start_pose["x_m"]-xy["x_m"], start_pose["y_m"]-xy["y_m"])
        diagnostics.update(initial_pose=start_pose, initial_distance_m=distance)
        if distance > 1.0 + 1e-9:
            raise PositioningError(f"Initial position is {distance:.3f}m away; limit is 1.000m")
        if measure_only:
            return {"status": "measured", "scanner": scanner, "evidence": initial_evidence, **diagnostics}
        phase = "path_check"
        guard()
        diagnostics["path_check"] = port.check_path(scanner, start_pose, xy)
        phase = "move"
        # Recheck ownership, measured pose and path under the per-robot admission
        # lock just before S0. Planned start is aligned to MEASURED start only.
        guard()
        result = port.move(scanner, xy, initial_evidence=initial_evidence,
                           scope=scope, owns_attempt=admission_current)
        if result.get("status") not in {"ok", "waiting", "retry"}:
            raise PositioningError(result.get("detail") or "Preparation move was not admitted")
        while True:
            guard()
            snap = port.snapshot(scanner)
            if snap.get("stop") or snap.get("state") == "s7stopped":
                raise PositioningError(snap.get("reason") or "Mobility stopped")
            if snap.get("state") == "s0idle": break
            await sleep(0.25)
        # Normal motion reports/corrections already ran. Confirm with a new
        # report even when S5 suppressed a negligible movement.
        before = snap.get("evidence") or initial_evidence
        phase = "confirmation"
        snap, since = await issue_and_wait("mobility.report.location", {})
        final_pose, evidence = measured_evidence(snap, scope=scope, since=since, previous=before)
        residual = math.hypot(final_pose["x_m"]-xy["x_m"], final_pose["y_m"]-xy["y_m"])
        diagnostics.update(final_pose=final_pose, final_distance_m=residual)
        if residual > port.final_tolerance_m + 1e-9:
            raise PositioningError(f"Final position error {residual:.3f}m exceeds {port.final_tolerance_m:.3f}m")
        guard()
        return {"status": "ready", "scanner": scanner, "scope": scope,
                "evidence": evidence, **diagnostics}
    except PositioningError as exc:
        return {"status": "failed", "scanner": scanner, "phase": phase,
                "reason": str(exc), **diagnostics}
    finally:
        if opened:
            port.close(scanner, scope=scope)


async def prepare_controller_robot(controller, scanner, port, **timing):
    """Connect one robot attempt to Step 2; driver must call robots serially.

    Full lab admission/persistence belongs to the future registration driver.
    If cancelled while awaiting work, an old callback cannot change controller
    state. A failure never resets the mobility subsystem or clears its queues.
    """
    if controller.phase != "preparing":
        raise PositioningError("Controller is not in a preparation attempt")
    token = controller.token
    owns = lambda: controller.token == token and controller.phase == "preparing"
    result = await position_once(scanner=scanner, target=controller.target_for(scanner),
                                 port=port, owns_attempt=owns, **timing)
    if owns():
        now = datetime.strptime(port.local_ts(), TIME_FMT)
        if result["status"] == "ready":
            controller.report_ready(scanner, token=token, now=now)
        else:
            snap = port.snapshot(scanner)
            # S7 is an attempt failure during preparation, not experiment death.
            # The lifecycle driver waits 180s, cancels old work/timers and resets
            # before invoking begin_attempt(reset_completed=True). It must then
            # obtain new reports; this callback never clears a stop on its own.
            controller.attempt_failed(token=token, now=now, reason=result["reason"], scanner=scanner,
                reset_required=bool(snap.get("stop") or snap.get("state") != "s0idle"))
    return result


class NmsPositioningPort:
    """Real NMS adapter. Instantiation does NOT issue commands.

    Only use once the caller owns exclusive lab-level preparation admission.
    The short per-robot lock below also preserves registration/poll processing.
    No lock is held while awaiting camera reports or physical movement.
    """
    def __init__(self):
        import config
        import utility
        import m8mobility as mobility
        import m8mobility_state as state
        import m8mobility_state_store as store
        self.config, self.utility = config, utility
        self.mobility, self.state, self.store = mobility, state, store
        self.final_tolerance_m = float(getattr(config, "MOBILITY_POST_CORRECTION_FAIL_THRESH_M", .3))
        if not math.isfinite(self.final_tolerance_m) or self.final_tolerance_m <= 0:
            raise PositioningError("Invalid final position tolerance")

    def local_ts(self): return self.utility.local_ts()

    def snapshot(self, scanner):
        stop = self.store._load_stop()
        st = self.config.r.hgetall(self.store.key_state(scanner))
        return {"state": st.get("state", "s0idle"), "stop": stop.get("stop", False),
                "reason": stop.get("reason") or st.get("state_detail", ""),
                "pose": self.store._load_true(scanner),
                "evidence": self.utility._hget_json(self.store.key_pose(scanner), "preparation_evidence_json")}

    def _admit(self, scanner, owns_attempt):
        if not owns_attempt(): raise PositioningError("Preparation ownership changed")
        if self.store._load_stop().get("stop"): raise PositioningError("Stop is latched")
        if self.mobility.registration_blocks_commands(scanner): raise PositioningError("Robot registration is incomplete")
        if self.state._get_state(scanner) != "s0idle": raise PositioningError("Robot is not idle")
        if not self.mobility.link.poll_fresh(scanner): raise PositioningError("Robot is not polling")

    @preparation_serialized
    def open(self, scanner, *, scope, owns_attempt):
        lock = self.mobility.registration_command_lock(scanner)
        if not lock.acquire(blocking=False): raise PositioningError("Robot registration/delivery is busy")
        try:
            self._admit(scanner, owns_attempt)
            if self.config.r.xlen(self.config.key_cmd_stream(scanner)):
                raise PositioningError("Robot has pending commands; preparation cannot overtake them")
            if not self.config.r.hsetnx(self.store.key_state(scanner), "preparation_scope", scope):
                raise PositioningError("Another preparation attempt requires reconciliation")
            try:
                self.utility._hset_many(self.store.key_state(scanner), {
                    "preparation_baseline_report_json": self.store._load_report_json(scanner),
                })
            except Exception:
                self.close(scanner, scope=scope)
                raise
        finally:
            lock.release()

    def close(self, scanner, *, scope):
        self.config.r.eval("if redis.call('HGET',KEYS[1],'preparation_scope') == ARGV[1] then "
                           "return redis.call('HDEL',KEYS[1],'preparation_scope') end return 0",
                           1, self.store.key_state(scanner), scope)

    @preparation_serialized
    def _with_lock(self, scanner, scope, owns_attempt, callback):
        lock = self.mobility.registration_command_lock(scanner)
        if not lock.acquire(blocking=False): raise PositioningError("Robot registration/delivery is busy")
        try:
            self._admit(scanner, owns_attempt)
            if self.config.r.hget(self.store.key_state(scanner), "preparation_scope") != scope:
                raise PositioningError("Preparation scope changed")
            return callback()
        finally:
            lock.release()

    def issue(self, scanner, action, args, *, scope, owns_attempt):
        return self._with_lock(scanner, scope, owns_attempt,
            lambda: self.mobility._on_command_issued_locked(scanner, action, args))

    def check_path(self, scanner, start, target):
        from m8mobility_map import _load_site_config, _is_path_clear_debug
        from preferred_direction_helper import lookup_preferred_direction
        from checker.static_safety_core import ramp_restriction_movement_issues
        site = _load_site_config()
        for pose in (start, target):
            if not (0 <= pose["x_m"] < site["world_width_m"] and 0 <= pose["y_m"] < site["world_height_m"]):
                raise PositioningError("Preparation path endpoint is outside the map")
        ok, blocked, detail = _is_path_clear_debug(start["x_m"], start["y_m"], target["x_m"], target["y_m"], exclude_scanner=scanner)
        policy = self.state._load_runtime_ramp_policy()
        issues = ramp_restriction_movement_issues(start, target, policy)
        if not ok or blocked or issues:
            raise PositioningError("Preparation path is blocked by the static/dynamic map or bump policy")
        heading = lookup_preferred_direction(x_m=target["x_m"], y_m=target["y_m"])
        if not heading.get("ok") or not math.isfinite(float(heading.get("preferred_heading_deg", float('nan')))):
            raise PositioningError("No valid preferred heading at the intended initial position")
        return {"path_ok": True, "preferred_heading_deg": heading["preferred_heading_deg"], "map": detail}

    def move(self, scanner, target, *, initial_evidence, scope, owns_attempt):
        def send():
            snap = self.snapshot(scanner)
            if snap["evidence"] != initial_evidence or snap["pose"] != initial_evidence["pose"]:
                raise PositioningError("Measured position changed before movement admission")
            self.check_path(scanner, snap["pose"], target)
            if not owns_attempt():
                raise PositioningError("Preparation ownership or time budget expired during path checking")
            # Align the path's planned start to the actual measurement; never
            # seed true_location_json with the operator's intended coordinates.
            self.store._save_planned(scanner, _pose(snap["pose"]))
            return self.mobility._on_command_issued_locked(scanner, "mobility.move", dict(target))
        return self._with_lock(scanner, scope, owns_attempt, send)
