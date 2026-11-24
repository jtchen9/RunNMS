"""Durable preparation lifecycle and atomic schedule commit; no hardware imports.

Runtime transitions and diagnostics share the Step 4 record. Active identity is
watched together with the record, so deleted/replaced workers cannot write back.
"""
import copy
import json
import uuid
from datetime import datetime, timedelta
from nms_preparation_delivery import Store, Conflict, packed, validate_snapshot
from nms_preparation_progress import describe, entry

FMT = "%Y-%m-%d-%H:%M:%S"

def ceil_minute(now):
    floor = now.replace(second=0, microsecond=0)
    return floor if floor == now else floor + timedelta(minutes=1)


class Lifecycle(Store):
    @property
    def active_key(self): return self.prefix + ":active"

    def activate(self, uid, manifest, registry_key, registry_fields, now):
        """Caller owns the existing experiment lifecycle lock; create pending row atomically."""
        from redis.exceptions import WatchError
        key = self.key(uid)
        with self.redis.pipeline() as p:
            p.watch(key, self.active_key, registry_key)
            doc = json.loads(p.get(key))
            if p.get(self.active_key) or p.xlen(registry_key):
                raise Conflict("Another registration or preparation cleanup is active")
            doc["runtime"] = dict(manifest=copy.deepcopy(manifest), phase="awaiting_attempt", owner="",
                token=uuid.uuid4().hex, attempt=0, ready=[], active_robot=None,
                freeze_at=now, retry_at=None, released=False, cancel_requested=False,
                registry_key=registry_key, registry_fields=registry_fields, health={})
            p.multi(); p.set(key, packed(doc)); p.set(self.active_key, uid)
            p.xadd(registry_key, registry_fields)
            try: p.execute()
            except WatchError as e: raise Conflict("Registration changed during activation") from e

    def owned_edit(self, uid, edit, owner=None, token=None):
        from redis.exceptions import WatchError
        for _ in range(16):
            with self.redis.pipeline() as p:
                p.watch(self.key(uid), self.active_key)
                active = p.get(self.active_key)
                if isinstance(active,bytes): active=active.decode()
                if active != uid: raise Conflict("Registration is no longer active")
                doc = json.loads(p.get(self.key(uid)))
                rt = doc["runtime"]
                if owner is not None and rt["owner"] != owner: raise Conflict("Worker ownership changed")
                if token is not None and rt["token"] != token: raise Conflict("Attempt ownership changed")
                result = edit(doc)
                p.multi(); p.set(self.key(uid), packed(result))
                try: p.execute(); return result
                except WatchError: continue
        raise Conflict("Concurrent lifecycle writes; retry")

    def publish(self, doc, now, events=(), reason=""):
        rt=doc["runtime"]
        phase = "retry_wait" if rt["phase"] == "retry_wait" else "preparing" if rt["attempt"] else "awaiting_attempt"
        prep=dict(phase=phase, attempt=rt["attempt"], updated_at=now,
                  subphase=rt["phase"], ready_robots=list(rt["ready"]),
                  pending_robots=[x["scanner"] for x in rt["manifest"]["initial_positions"] if x["scanner"] not in rt["ready"]],
                  reason=reason, failed_robot=rt.get("failed_robot"), reset_required=phase=="retry_wait")
        prep.update(describe(rt,doc["snapshot"].get("preparation") or {},now))
        if phase == "retry_wait": prep["retry_at"]=rt["retry_at"]
        new=dict(doc["snapshot"], update_seq=doc["snapshot"]["update_seq"]+1, preparation=prep)
        validate_snapshot(new)
        doc["snapshot"]=new; doc["state_queue"].append(new)
        for kind,robot,details in events:
            self._add_event(doc,dict(event_type=kind,attempt=rt["attempt"],time_local=now,
                                    robot=robot,reason=reason or None,details=details),new["update_seq"])
        return doc

    def claim(self,uid,owner,now):
        def edit(doc):
            rt=doc["runtime"]; rt["owner"]=owner
            if rt["released"] or rt["cancel_requested"]: return doc
            rt["token"]=uuid.uuid4().hex; rt["active_robot"]=None
            if rt["phase"] not in {"awaiting_attempt","retry_wait"}:
                return self._fail(doc,now,"NMS restarted during preparation; obtain fresh reports after reset")
            # Require a new poll after startup, not an old Redis last_poll value.
            rt["freeze_at"]=now
            return doc
        return self.owned_edit(uid,edit)

    def _fail(self,doc,now,reason,scanner=None):
        rt=doc["runtime"]
        if rt["released"] or rt["cancel_requested"]:
            raise Conflict("Cannot retry an ended preparation")
        rt.update(phase="retry_wait",freeze_at=now,retry_at=(datetime.strptime(now,FMT)+timedelta(seconds=60)).strftime(FMT),
                  ready=[],active_robot=None,active_robots=[],failed_robot=scanner,token=uuid.uuid4().hex)
        return self.publish(doc,now,[("attempt_failed",scanner,{}),("retry_scheduled",None,{"retry_at":rt["retry_at"],"reset_required":True})],reason)

    def fail(self,uid,owner,token,now,reason,scanner=None):
        return self.owned_edit(uid,lambda d:self._fail(d,now,reason,scanner),owner,token)

    def begin(self,uid,owner,now):
        def edit(doc):
            rt=doc["runtime"]
            if rt["phase"] not in {"awaiting_attempt","retry_wait"} or rt["cancel_requested"]:
                raise Conflict("Cannot begin this attempt")
            if rt["retry_at"] and now<rt["retry_at"]:raise Conflict("60-second adjustment period unfinished")
            rt.update(phase="resetting",attempt=rt["attempt"]+1,token=uuid.uuid4().hex,ready=[],failed_robot=None,active_robot=None,active_robots=[],robot_progress={})
            return self.publish(doc,now,[("attempt_started",None,{}),("reset_started",None,{})])
        return self.owned_edit(uid,edit,owner)

    def stage(self,uid,owner,token,phase,now,robot=None,ready=None,health=None,robots=None):
        def edit(doc):
            rt=doc["runtime"]
            if rt["cancel_requested"] or rt["released"]:raise Conflict("Preparation ended")
            members = list(robots) if robots is not None else ([robot] if robot else [])
            roster = {p["scanner"] for p in rt["manifest"]["initial_positions"]}
            if not set(members) <= roster: raise Conflict("Unknown preparation robot")
            rt.update(phase=phase,active_robot=robot,active_robots=members)
            events=[]
            if phase=="health":events.append(("reset_completed",None,{}))
            if health is not None:rt["health"]=health
            if ready is not None and ready not in rt["ready"]:
                rt["ready"].append(ready);events.append(("robot_ready",ready,{}))
            if phase=="ready":events.append(("all_robots_ready",None,{}))
            return self.publish(doc,now,events)
        return self.owned_edit(uid,edit,owner,token)

    def progress(self,uid,owner,token,robot,step,now):
        def edit(doc):
            rt=doc["runtime"]
            members=rt.get("active_robots") or ([rt["active_robot"]] if rt.get("active_robot") else [])
            if rt["cancel_requested"] or rt["released"] or rt["phase"]!="preparing" or robot not in members:
                raise Conflict("Progress belongs to an inactive preparation robot")
            rows=rt.setdefault("robot_progress",{})
            old=rows.get(robot,{})
            events=[]
            if step=="ready":
                if robot in rt["ready"]:return doc
                rt["ready"].append(robot)
                rows[robot]=dict(step=None,step_number=None,step_label="Ready",status="ready",step_started_at=now)
                events=[("robot_ready",robot,{})]
            else:
                new=entry(step,now,old)
                if new==old:return doc
                rows[robot]=new
            return self.publish(doc,now,events)
        return self.owned_edit(uid,edit,owner,token)

    def request_cancel(self,uid,now):
        def edit(doc):
            rt=doc["runtime"]
            if rt["cancel_requested"]:return doc
            rt.update(cancel_requested=True,phase="cancelling",active_robot=None,active_robots=[],freeze_at=now,token=uuid.uuid4().hex)
            doc["locally_closed"]=True;doc["state_queue"]=[]
            doc["delete_request"] = dict({k:doc["snapshot"][k] for k in ("nms_id","lab_id","experiment_id","session_id","registration_uid")},
                                         deleted_at=now,reason="nms_delete_experiment")
            self._add_event(doc,dict(event_type="cancelled",time_local=now,attempt=rt["attempt"],reason="Operator deleted experiment"),doc["snapshot"]["update_seq"])
            return doc
        return self.owned_edit(uid,edit)

    def finish(self,uid,owner):
        from redis.exceptions import WatchError
        for _ in range(16):
            with self.redis.pipeline() as p:
                p.watch(self.key(uid),self.active_key)
                active=p.get(self.active_key)
                if isinstance(active,bytes):active=active.decode()
                if active!=uid:return
                d=json.loads(p.get(self.key(uid)))
                if d["runtime"]["owner"]!=owner:raise Conflict("Stale worker")
                d["locally_closed"]=True;d["runtime"]["phase"]="closed";d["state_queue"]=[]
                p.multi();p.set(self.key(uid),packed(d));p.delete(self.active_key)
                try:p.execute();return
                except WatchError:continue
        raise Conflict("Could not close active registration")

    def commit(self,uid,owner,token,clock,compile_rows):
        """All queue writes + registry + schedule revision + event in ONE transaction.

        Validate/watch every destination type before MULTI: Redis execution errors
        do not roll back earlier commands. No approximate stream trimming here.
        """
        from redis.exceptions import WatchError
        for _ in range(16):
            with self.redis.pipeline() as p:
                p.watch(self.key(uid),self.active_key,self.prefix+":worker_lock")
                lease=p.get(self.prefix+":worker_lock")
                if isinstance(lease,bytes):lease=lease.decode()
                if lease!=owner:raise Conflict("Worker lease lost before commit")
                active=p.get(self.active_key)
                if isinstance(active,bytes):active=active.decode()
                if active!=uid:raise Conflict("Registration replaced")
                d=json.loads(p.get(self.key(uid)));rt=d["runtime"]
                if rt["owner"]!=owner or rt["token"]!=token:raise Conflict("Stale worker/attempt")
                if rt["released"]:return d
                if rt["phase"]!="ready" or rt["cancel_requested"]:raise Conflict("Not ready to release")
                roster={x["scanner"] for x in rt["manifest"]["initial_positions"]}
                if set(rt["ready"])!=roster:raise Conflict("Not all robots ready")
                now=clock();start=ceil_minute(now)
                end=start+timedelta(seconds=max(x["t_offset_sec"] for x in rt["manifest"]["commands"]))
                operations=compile_rows(rt["manifest"],start,d["snapshot"])
                registry=rt["registry_key"]
                keys={registry:"stream"}
                for op,key,*args in operations:
                    kind={"xadd":"stream","hset":"hash","sadd":"set"}[op]
                    if key in keys and keys[key]!=kind:raise ValueError("Conflicting destination types")
                    keys[key]=kind
                p.watch(*keys)
                for key,kind in keys.items():
                    actual=p.type(key)
                    if isinstance(actual,bytes):actual=actual.decode()
                    if actual not in ("none",kind):raise ValueError("Wrong Redis destination type: "+key)
                rows=p.xrange(registry)
                if len(rows)!=1 or rows[0][1].get("registration_uid")!=uid:raise Conflict("Registry identity changed")
                stamp=now.strftime(FMT)
                snap=dict(d["snapshot"],update_seq=d["snapshot"]["update_seq"]+1,state="scheduled",
                          start_at=start.strftime(FMT),end_at=end.strftime(FMT))
                snap["preparation"]=dict(snap["preparation"],phase="ready",subphase="ready",updated_at=stamp,
                                         step="ready",step_number=None,step_label="Preparation complete",
                                         step_started_at=stamp,active_robots=[])
                validate_snapshot(snap);d["snapshot"]=snap;d["state_queue"].append(snap)
                self._add_event(d,dict(event_type="schedule_assigned",attempt=rt["attempt"],time_local=stamp,
                                      details={"start_at":snap["start_at"],"end_at":snap["end_at"]}),snap["update_seq"])
                rt.update(released=True,phase="scheduled",active_robot=None)
                fields=dict(rt["registry_fields"],state="registered",start_at=snap["start_at"],end_at=snap["end_at"])
                rt["registry_fields"]=fields
                p.multi()
                for op,key,*args in operations:
                    if op=="xadd":p.xadd(key,args[0])
                    elif op=="hset":p.hset(key,mapping=args[0])
                    else:p.sadd(key,*args)
                p.xadd(registry,fields);p.xdel(registry,rows[0][0]);p.set(self.key(uid),packed(d))
                try:p.execute();return d
                except WatchError:continue
        raise Conflict("Concurrent commit; retry without releasing any commands")
