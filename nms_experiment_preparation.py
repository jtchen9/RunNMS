"""Preparation lifecycle core -- not yet connected to live registration.

No Redis, network, robot commands, timers or background threads in this module.
The future NMS adapter owns persistence, safety checks, result correlation,
polling, publication and atomic schedule installation. This core holds the
script as offsets and produces a schedule candidate only after all robots pass.
It does not modify the robot mobility state machine.
"""
from copy import deepcopy
from datetime import datetime, timedelta
import math
import uuid

TIME_FMT = "%Y-%m-%d-%H:%M:%S"
RETRY_SECONDS = 180
TERMINAL = {"cancelled", "stopped"}


class PreparationError(ValueError):
    pass


def _local(value):
    if not isinstance(value, datetime) or value.tzinfo is not None:
        raise PreparationError("Use a naive NMS local datetime")
    return value


def ceil_minute(value):
    value = _local(value)
    floor = value.replace(second=0, microsecond=0)
    return floor if value == floor else floor + timedelta(minutes=1)


class Preparation:
    """One registration's held script and readiness state.

    Tokens contain a new registration UUID and an attempt UUID. A delayed result
    from a previous attempt or a deleted/re-registered experiment cannot advance
    this instance, even if the experiment name is reused.

    report_ready is called ONLY after the adapter verifies a fresh matching
    report, the current pose and the existing movement acceptance rules. It is
    not an API for accepting arbitrary robot-supplied readiness flags.
    """

    def __init__(self, *, lab_id, experiment_id, session_id, manifest, now):
        _local(now)
        if not lab_id or not experiment_id:
            raise PreparationError("lab_id and experiment_id are required")
        if manifest.get("schema_version") != "experiment_csv_v1":
            raise PreparationError("Expected the Step 1 parsed CSV manifest")
        self._manifest = deepcopy(manifest)
        commands = self._manifest.get("commands", [])
        if not commands:
            raise PreparationError("No commands to hold")
        for cmd in commands:
            offset = cmd.get("t_offset_sec")
            if type(offset) is not int or offset < 0:
                raise PreparationError("Offsets must be nonnegative integer seconds")
        self._robots = tuple(p["scanner"] for p in manifest["initial_positions"])
        if len(self._robots) != len(set(self._robots)):
            raise PreparationError("Duplicate initial robot")
        self.identity = (str(lab_id), str(experiment_id), str(session_id or ""), uuid.uuid4().hex)
        self.registered_at = now
        self.updated_at = now
        self.phase = "checking_health"
        self.attempt = 0
        self._attempt_id = ""
        self._ready = set()
        self.retry_at = None
        self.ready_at = None
        self.failed_robot = None
        self.reason = ""
        self.reset_required = False

    @property
    def token(self):
        return self.identity + (self._attempt_id,)

    def target_for(self, scanner):
        for position in self._manifest["initial_positions"]:
            if position["scanner"] == scanner:
                return {"x_m": position["x_m"], "y_m": position["y_m"]}
        raise PreparationError("Robot is not in the initial roster")

    def _live(self):
        if self.phase in TERMINAL:
            raise PreparationError("Preparation has ended")

    def health_passed(self, *, token, now):
        if token != self.token or self.phase != "checking_health":
            return False
        self.updated_at = _local(now)
        self.phase = "awaiting_attempt"
        return True

    def begin_attempt(self, *, now, all_robots_idle, stop_latched, reset_completed=False):
        """Adapter must observe actual idle state, not just an expired timeout."""
        self._live()
        _local(now)
        if self.phase not in {"awaiting_attempt", "retry_wait"}:
            raise PreparationError("Health checks or the previous attempt are unfinished")
        if self.retry_at is not None and now < self.retry_at:
            raise PreparationError("The 180-second operator adjustment period is unfinished")
        if self.reset_required and not reset_completed:
            raise PreparationError("Reset the failed preparation attempt before retrying")
        if stop_latched:
            raise PreparationError("Stop is latched; preparation cannot clear it")
        if not all_robots_idle:
            raise PreparationError("Wait for outstanding robot work to finish")
        self.attempt += 1
        self._attempt_id = uuid.uuid4().hex
        self._ready.clear()  # Reconfirm every robot; no implication of another move.
        self.phase = "preparing" if self._robots else "ready"
        self.updated_at = now
        self.ready_at = now if not self._robots else None
        self.retry_at = None
        self.failed_robot = None
        self.reason = ""
        self.reset_required = False
        return self.token

    def report_ready(self, scanner, *, token, now):
        if token != self.token or self.phase != "preparing":
            return False
        if scanner not in self._robots:
            raise PreparationError("Robot is not in the initial roster")
        self.updated_at = _local(now)
        self._ready.add(scanner)
        if len(self._ready) == len(self._robots):
            self.phase = "ready"
            self.ready_at = now
        return True

    def attempt_failed(self, *, token, now, reason, scanner=None, reset_required=False):
        if token != self.token or self.phase != "preparing":
            return False
        if not reason or (scanner is not None and scanner not in self._robots):
            raise PreparationError("Failure needs a reason and a known robot, if specified")
        self.updated_at = _local(now)
        self.phase = "retry_wait"
        self.retry_at = now + timedelta(seconds=RETRY_SECONDS)
        self.ready_at = None
        self._ready.clear()
        self.failed_robot = scanner
        self.reason = str(reason)
        self.reset_required = bool(reset_required)
        return True

    def cancel(self, *, now):
        self.phase = "cancelled"
        self.updated_at = _local(now)
        self.reason = "Experiment deleted by operator"
        self._ready.clear()

    def stop(self, *, now, reason):
        self._live()
        if not reason:
            raise PreparationError("Stop needs a reason")
        self.phase = "stopped"
        self.updated_at = _local(now)
        self.reason = str(reason)
        self._ready.clear()

    def schedule_candidate(self, *, now):
        """Compute, never publish or enqueue. Caller must recheck token at commit.

        Choose t0 using the current handoff time as well as readiness time: a
        slow publication/validation step must not install an already-past t0.
        If a candidate becomes stale, recompute it before the atomic handoff.
        The future adapter must install the queue and completion deadline from
        the SAME candidate. It must not independently recompute either.
        """
        if self.phase != "ready":
            raise PreparationError("Commands remain held until every robot is ready")
        start = ceil_minute(max(_local(now), self.ready_at))
        commands = deepcopy(self._manifest["commands"])
        # Stable time order, preserving CSV row order when offsets are equal.
        commands.sort(key=lambda c: c["t_offset_sec"])
        for cmd in commands:
            cmd["execute_at"] = (start + timedelta(seconds=cmd["t_offset_sec"])).strftime(TIME_FMT)
        end = start + timedelta(seconds=max(c["t_offset_sec"] for c in commands))
        return {
            "token": self.token,
            "start_at": start.strftime(TIME_FMT),
            "end_at": end.strftime(TIME_FMT),
            "commands": commands,
            "command_count": len(commands),
            "candidate_only": True,
            "commands_enqueued": 0,
        }

    def snapshot(self, *, now):
        _local(now)
        return {
            "schema": "experiment_preparation_v1",
            "lab_id": self.identity[0], "experiment_id": self.identity[1],
            "session_id": self.identity[2], "registration_version": self.identity[3],
            "phase": self.phase, "attempt": self.attempt,
            "registered_at": self.registered_at.strftime(TIME_FMT),
            "updated_at": self.updated_at.strftime(TIME_FMT),
            "ready_at": self.ready_at.strftime(TIME_FMT) if self.ready_at else None,
            "ready_robots": sorted(self._ready),
            "pending_robots": sorted(set(self._robots) - self._ready),
            "failed_robot": self.failed_robot, "reason": self.reason,
            "reset_required": self.reset_required,
            "retry_at": self.retry_at.strftime(TIME_FMT) if self.phase == "retry_wait" else None,
            "retry_in_sec": max(0, math.ceil((self.retry_at-now).total_seconds()))
                if self.phase == "retry_wait" else 0,
            # No final t0 exists until the live adapter commits a candidate.
            "start_at": None, "end_at": None,
            "commands_held": len(self._manifest["commands"]),
            "commands_enqueued": 0,
        }
