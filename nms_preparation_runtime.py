"""NMS-owned preparation worker. Sleeps without polling when no work exists."""
import asyncio
import json
import logging
from datetime import datetime, timedelta

from nms_preparation_delivery import Delivery,Web,Conflict
from nms_preparation_lifecycle import Lifecycle
from nms_preparation_gate import ACTIVITY,OWNER,active,prefix

_log=logging.getLogger(__name__)
_loop=None
_tasks=[]
_work=None
_upload=None
_stopping=False


def store():
    import config
    return Lifecycle(config.r,prefix())


def now():
    import utility
    return utility.local_ts()


def wake():
    if _loop is not None and not _stopping:
        _loop.call_soon_threadsafe(_work.set)
        _loop.call_soon_threadsafe(_upload.set)


def owns(uid,token):
    import config
    d=active()
    lease=config.r.get(prefix()+":worker_lock")
    if isinstance(lease,bytes):lease=lease.decode()
    return bool(d and d["snapshot"]["registration_uid"]==uid and d["runtime"]["token"]==token
                and d["runtime"]["owner"]==OWNER==lease and not d["runtime"]["cancel_requested"] and not _stopping)


def quiescent(doc):
    """The agent polls synchronously between command batches. A poll newer than
    the freeze boundary is therefore needed before resetting an issued command.
    No timeout is treated as proof that physical movement has finished.
    """
    import config,utility
    for p in doc["runtime"]["manifest"]["initial_positions"]:
        meta=config.r.hgetall(config.key_scanner_meta(p["scanner"]))
        if meta.get("registration_status") in {"pending","failed"}:return False
        poll=meta.get("last_poll","")
        if poll<=doc["runtime"]["freeze_at"]:return False
        try:age=(utility.parse_local_dt(now())-utility.parse_local_dt(poll)).total_seconds()
        except (ValueError,TypeError):return False
        if not 0<=age<=30:return False
    return True


def freeze(doc):
    import m8mobility_state as state
    # Caller has already persisted retry/cancellation and revoked the token.
    # Serialization waits for current callback code to leave before timer cleanup.
    with ACTIVITY:
        for p in doc["runtime"]["manifest"]["initial_positions"]:
            state._cancel_runtime_timers(p["scanner"])


def reset(doc):
    import config,m8mobility
    from m8mobility_state_store import key_state,key_pose,_save_stop
    with ACTIVITY:
        if not quiescent(doc):raise Conflict("Wait for fresh post-freeze polls before reset")
        m8mobility._ensure_mobility_assets_ready()
        config.r.set(getattr(config,"KEY_MOBILITY_STATE_MACHINE_TEST_ENABLED",
                             config.KEY_PREFIX+"debug:mobility_state_machine_test_enabled"),"false")
        for p in doc["runtime"]["manifest"]["initial_positions"]:
            scanner=p["scanner"]
            m8mobility.manual_resume(scanner)
            config.r.hdel(key_state(scanner),"preparation_scope","preparation_baseline_report_json")
            config.r.hdel(key_pose(scanner),"preparation_evidence_json")
        _save_stop(False,"")


def cancel_active(registry_rows=()):
    """Called by delete while holding the existing lifecycle lock."""
    d=active()
    if d:
        d=store().request_cancel(d["snapshot"]["registration_uid"],now())
        freeze(d);wake()
    for _, fields in registry_rows:
        uid=fields.get("registration_uid")
        if uid and (not d or uid != d["snapshot"]["registration_uid"]) and store().load(uid):
            def request(old):
                old["locally_closed"]=True;old["state_queue"]=[]
                old.setdefault("delete_request",dict({k:old["snapshot"][k] for k in ("nms_id","lab_id","experiment_id","session_id","registration_uid")},
                                                  deleted_at=now(),reason="nms_delete_experiment"))
                return old
            store()._edit(uid,request);wake()
    return bool(d)


def current_snapshot(mobility_stops=None):
    import config
    d=active()
    if not d or d["locally_closed"]:return None
    result={k:v for k,v in d["snapshot"].items() if k!="nms_id"}
    fields=d["package"]["fields"]
    result.update(registered_at=fields["registered_at"],command_count=fields["command_count"],
                  replace_existing=False,elapsed_sec=0,remaining_sec=0,
                  next_scheduled_at=None,idle_duration_sec=0)
    if result["start_at"]:
        t=datetime.strptime(now(),"%Y-%m-%d-%H:%M:%S")
        start=datetime.strptime(result["start_at"],"%Y-%m-%d-%H:%M:%S")
        end=datetime.strptime(result["end_at"],"%Y-%m-%d-%H:%M:%S")
        if result["state"]=="scheduled":
            remaining=(start-t).total_seconds();result["next_scheduled_at"]=result["start_at"]
        elif result["state"]=="wrapping_up":
            remaining=(end+timedelta(seconds=int(getattr(config,"NORTHBOUND_EXPERIMENT_WRAPUP_SEC",120)))-t).total_seconds()
        else:remaining=(end-t).total_seconds()
        result.update(elapsed_sec=max(0,int((t-start).total_seconds())),remaining_sec=max(0,int(remaining)))
        if mobility_stops:
            result["mobility_stops"]=[dict(robot_id=robot,reason=reason,experiment_id=result["experiment_id"])
                for robot,stamp,reason in mobility_stops if stamp>=fields["registered_at"]]
    return result


def _terminal_registry(uid):
    import config
    rows=config.r.xrange(config.KEY_EXPERIMENT_REGISTRY)
    return any(f.get("registration_uid")==uid and f.get("state") in {"completed","finalized"} for _,f in rows)


def before_finalize(fields):
    """Do not finalize ahead of a pending registration/schedule state upload."""
    uid=fields.get("registration_uid")
    if not uid:return True
    d=store().load(uid)
    return bool(d and d["registered"] and not d["state_queue"] and not any(k in d["blocked"] for k in ("registration","state")) and d["snapshot"]["state"] == "wrapping_up")


async def _attempt(uid,d):
    import config
    from nms_static_readiness import check_targets
    from nms_preparation_positioning import NmsPositioningPort,position_once
    from nms_preparation_batch import run_batch,PathBarrier,BatchFailure
    from nms_preparation_registration import compile_operations
    import nms_experiment_completion as completion
    from m8mobility_trace import experiment_trace_boundary
    st=store();token=d["runtime"]["token"];robot=None
    try:
        if not owns(uid,token):return
        # One trace boundary for the whole registration, including retries.
        if d["runtime"]["attempt"]==1:
            with experiment_trace_boundary():reset(d)
        else:reset(d)
        d=st.stage(uid,OWNER,token,"health",now());wake()
        names=sorted({c["scanner"] for c in d["runtime"]["manifest"]["commands"]} | {p["scanner"] for p in d["runtime"]["manifest"]["initial_positions"]})
        health=await check_targets(names,preparation_uid=uid,owns_attempt=lambda:owns(uid,token))
        d=st.stage(uid,OWNER,token,"preparing",now(),health=health);wake()
        port=NmsPositioningPort()
        positions=d["runtime"]["manifest"]["initial_positions"]
        # Measure all robots before any move; both passes run concurrently.
        # Per-robot scope/admission locks remain in the positioning adapter.
        members=[p["scanner"] for p in positions]
        for measure_only in (True,False):
            d=st.stage(uid,OWNER,token,"preparing",now(),robots=members);wake()
            barrier=None if measure_only or not members else PathBarrier(
                members,port,config.MOBILITY_ROBOT_RESTRICT_RADIUS_M)
            async def job(target):
                scanner=target["scanner"]
                def progress(step):
                    st.progress(uid,OWNER,token,scanner,step,now());wake()
                try:
                    if not owns(uid,token):raise Conflict("Preparation ownership changed")
                    result=await position_once(scanner=scanner,target=target,port=port,
                        owns_attempt=lambda:owns(uid,token),measure_only=measure_only,
                        on_progress=progress,before_move=barrier.wait if barrier else None)
                    if result["status"] not in {"measured","ready"}:
                        raise BatchFailure(scanner,result.get("reason","Preparation failed"))
                    if not measure_only:progress("ready")
                    return result
                except asyncio.CancelledError:
                    raise
                except Conflict:
                    raise
                except BatchFailure:
                    raise
                except Exception as exc:
                    raise BatchFailure(scanner,str(exc)) from exc
            try:
                await run_batch(positions,job)
            except BatchFailure as exc:
                robot=exc.scanner
                raise
        d=st.stage(uid,OWNER,token,"ready",now());wake()
        lock=completion.lifecycle_lock()
        if not lock.acquire(blocking=False):raise RuntimeError("Lifecycle busy at schedule commit")
        try:
            with ACTIVITY:
                if not owns(uid,token):return
                d=st.commit(uid,OWNER,token,datetime.now,compile_operations)
                rows=config.r.xrange(config.KEY_EXPERIMENT_REGISTRY)
                for xid,fields in rows:
                    if fields.get("registration_uid")==uid:completion.start_registration(xid,fields)
        finally:
            if lock.owned():lock.release()
        wake()
    except asyncio.CancelledError:
        raise
    except Conflict:
        # A deleted/replaced attempt has no authority to publish a failure.
        if owns(uid,token) and not st.load(uid)["runtime"]["released"]:
            d=st.fail(uid,OWNER,token,now(),"Preparation ownership/commit conflict; retry after reset",robot)
            freeze(d);wake()
    except Exception as exc:
        if owns(uid,token) and not st.load(uid)["runtime"]["released"]:
            reason=str(getattr(exc,"detail",str(exc)))
            d=st.fail(uid,OWNER,token,now(),reason,robot);freeze(d);wake()


async def _heartbeat(lock,lost):
    while not _stopping:
        await asyncio.sleep(5)
        try:
            await asyncio.to_thread(lock.extend,30,replace_ttl=True)
        except Exception:
            lost.set();return


async def _manage_one(uid):
    import config
    st=store()
    lease=config.r.lock(prefix()+":worker_lock",timeout=30,thread_local=False)
    if not lease.acquire(blocking=False,token=OWNER):
        await asyncio.sleep(1);_work.set();return
    lost=asyncio.Event();heartbeat=asyncio.create_task(_heartbeat(lease,lost))
    try:
        with ACTIVITY:
            d=st.claim(uid,OWNER,now())
            if not d["runtime"]["released"]:
                import m8mobility
                freeze(d)
                m8mobility._clear_all_command_queues()
        wake()
        while not _stopping and not lost.is_set():
            d=active()
            if not d or d["snapshot"]["registration_uid"]!=uid:return
            rt=d["runtime"]
            if d["remote_inactive"] and not rt["cancel_requested"]:
                d=st.request_cancel(uid,now());freeze(d);rt=d["runtime"]
            if rt["cancel_requested"]:
                if rt["released"] or quiescent(d):
                    if not rt["released"]:reset(d)
                    st.finish(uid,OWNER);wake();return
            elif rt["released"]:
                if _terminal_registry(uid):st.finish(uid,OWNER);wake();return
                # Commit may have succeeded immediately before a process/error
                # interrupted timer creation. Reinstall idempotently from Redis.
                import nms_experiment_completion as completion
                for xid, fields in config.r.xrange(config.KEY_EXPERIMENT_REGISTRY):
                    if fields.get("registration_uid")==uid:
                        completion.start_registration(xid,fields)
                phase="wrapping_up" if now()>d["snapshot"]["end_at"] else "running" if now()>=d["snapshot"]["start_at"] else "scheduled"
                if phase!=d["snapshot"]["state"]:
                    st.advance(uid,d["snapshot"]["update_seq"],state=phase,preparation=d["snapshot"]["preparation"],
                               start_at=d["snapshot"]["start_at"],end_at=d["snapshot"]["end_at"]);wake()
            elif d["registered"] and not d["blocked"]:
                if rt["phase"] in {"awaiting_attempt","retry_wait"} and (not rt["retry_at"] or now()>=rt["retry_at"]):
                    if quiescent(d):
                        with ACTIVITY:d=st.begin(uid,OWNER,now())
                        await _attempt(uid,d)
            await asyncio.sleep(.5)
    except Conflict:
        return
    finally:
        heartbeat.cancel();await asyncio.gather(heartbeat,return_exceptions=True)
        if lease.owned():lease.release()


async def _manager():
    while not _stopping:
        await _work.wait();_work.clear()
        try:
            d=active()
            if d:await _manage_one(d["snapshot"]["registration_uid"])
        except asyncio.CancelledError:raise
        except Exception:
            _log.exception("Preparation worker error; held state retained")
            await asyncio.sleep(10);_work.set()


async def _pump():
    st=store();delivery=Delivery(st,Web.configured())
    while not _stopping:
        await _upload.wait();_upload.clear()
        pending=False
        for uid in st.redis.smembers(st.prefix+":registrations"):
            if isinstance(uid,bytes):uid=uid.decode()
            try:
                d=st.load(uid)
                if not d or "runtime" not in d:continue
                if not d["registered"] or d["state_queue"] or d["pending_events"] or (d.get("delete_request") and not d.get("delete_acknowledged")):
                    result=await asyncio.to_thread(delivery.flush_once,uid)
                    pending |= result["status"] not in {"idle","blocked"}
            except asyncio.CancelledError:raise
            except Exception:
                _log.exception("Preparation delivery failed; records retained");pending=True
        if pending:
            await asyncio.sleep(10);_upload.set()


async def start():
    global _loop,_tasks,_work,_upload,_stopping
    _loop=asyncio.get_running_loop();_stopping=False
    _work=asyncio.Event();_upload=asyncio.Event()
    _tasks=[asyncio.create_task(_manager()),asyncio.create_task(_pump())]
    wake()


async def stop():
    global _stopping
    _stopping=True
    for t in _tasks:t.cancel()
    await asyncio.gather(*_tasks,return_exceptions=True)
