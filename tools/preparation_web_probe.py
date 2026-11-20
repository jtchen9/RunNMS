"""Desk-only protocol evidence. Never submits CSV to NMS or issues robot commands.

--live: pending registration + simulated failed attempt + duplicate diagnostics.
--cancel: delete that same test session, then upload its cancellation diagnostic.
No flags: print instructions only. Use when no real experiment is registered.
"""
import argparse
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from nms_preparation_delivery import Store, Delivery, Web, TIME_FMT, IDENTITY, packed


def run(mode):
    import config
    import utility
    import nms_experiment_completion
    r = config.r
    prefix = config.KEY_EXPERIMENT_REGISTRY + ":preparation_web_probe"
    store, web = Store(r, prefix), Web.configured()
    transcript = []
    uid = r.get(prefix + ":current")
    if isinstance(uid, bytes): uid = uid.decode()
    lock = nms_experiment_completion.lifecycle_lock()
    if not lock.acquire(blocking=False):
        raise RuntimeError("Experiment lifecycle is busy; run the probe when idle")
    try:
        terminal = {"completed", "finalized", "deleted", "cancelled", "canceled", "failed", "stopped"}
        for _, row in r.xrange(config.KEY_EXPERIMENT_REGISTRY):
            row = {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k,v in row.items()}
            if row.get("state", "").lower() not in terminal:
                raise RuntimeError("A real experiment is registered. Finish/delete it before the protocol probe.")
        doc = store.load(uid) if uid else None
        if mode == "live" and (not doc or doc["locally_closed"]):
            uid = uuid.uuid4().hex
            now = utility.local_ts()
            fields = dict(nms_id=config.NMS_ID, lab_id=config.NMS_ID, session_id="default",
                          experiment_id="PreparationProtocol_" + datetime.strptime(now, TIME_FMT).strftime("%Y%m%d_%H%M%S") + "_" + uid[:6],
                          registered_at=now, command_count=0,
                          extra_metadata_json=packed({"protocol_test_only": True, "no_robot_commands": True}))
            files = [("command_sheet_file", ("ProtocolOnly.csv",
                      b"scanner,t_offset_sec,category,action,args_json,initial_robot,initial_xy\r\n", "text/csv"))]
            store.create(fields, files, dict(phase="awaiting_attempt", attempt=0, updated_at=now), uid)
            r.set(prefix + ":current", uid)
            doc = store.load(uid)
        if not doc:
            raise RuntimeError("No protocol session exists; run --live first")
        delivery = Delivery(store, web)

        def check_lock():
            if not lock.owned(): raise RuntimeError("Lifecycle lock expired; stop and rerun when idle")

        def drain():
            for _ in range(12):
                check_lock()
                result = delivery.flush_once(uid)
                transcript.append(result)
                if result["status"] == "idle": return
                if result["status"] != "acknowledged":
                    raise RuntimeError("Delivery not acknowledged; inspect evidence JSON. Pending data retained; rerun the same command after fixing the cause.")
            raise RuntimeError("Delivery did not drain within bounded probe limit")

        if mode == "live":
            drain()
            doc = store.load(uid)
            if doc["snapshot"]["update_seq"] == 0:
                now = utility.local_ts()
                store.advance(uid, 0, state="preparing",
                    preparation=dict(phase="preparing", attempt=1, updated_at=now),
                    events=[dict(event_id=uid+":attempt1", event_type="attempt_started", attempt=1, time_local=now,
                                 details={"protocol_test_only": True})])
            drain()
            if store.load(uid)["snapshot"]["update_seq"] == 1:
                now = utility.local_ts()
                store.advance(uid, 1, state="preparing",
                    preparation=dict(phase="retry_wait", attempt=1, updated_at=now, reset_required=False,
                                     reason="Protocol probe: simulated preparation failure; no robot commands issued",
                                     retry_at=(datetime.strptime(now,TIME_FMT)+timedelta(seconds=180)).strftime(TIME_FMT)),
                    events=[dict(event_id=uid+":"+kind, event_type=kind, attempt=1, time_local=now,
                                 details={"protocol_test_only": True}) for kind in ["attempt_failed", "retry_scheduled"]])
            drain()
            # Replay exactly stored occurrences to verify server idempotency.
            doc = store.load(uid)
            body = {k: doc["snapshot"][k] for k in IDENTITY}
            body["events"] = list(doc["events"].values())
            check_lock()
            code, reply = web.send("/nms/experiment/preparation_events", body)
            transcript.append(dict(probe="duplicate_events", http_status=code, response=reply))
            ids = {e["event_id"] for e in body["events"]}
            if not (code == 200 and reply.get("status") == "ok" and reply.get("registration_uid") == uid
                    and set(reply.get("duplicate_event_ids", [])) == ids):
                raise RuntimeError("Server did not acknowledge the repeated events as duplicates")
            print("PASS: pending registration, preparation revisions, events and duplicate acknowledgements.")
            print("Inspect LIVE and DataLake/Experiment/Future: preparation retry_wait, no assigned t0.")
            print("The retry countdown is a protocol display test; no robot retry worker is running.")
            print("Then run this tool with --cancel and download the Deleted Experiment ZIP.")
        else:
            # Persist the exact delete request for retries too.
            def prepare_delete(current):
                if "probe_delete_fields" not in current:
                    current["probe_delete_fields"] = {k: current["snapshot"][k] for k in IDENTITY}
                    current["probe_delete_fields"].update(deleted_at=utility.local_ts(), reason="protocol_probe_cleanup")
                return current
            doc = store._edit(uid, prepare_delete)
            # Register must have been accepted before deletion can target it.
            if not doc["registered"]:
                drain()
            if not doc.get("probe_deleted"):
                check_lock()
                code, reply = web.send("/nms/experiment/delete", doc["probe_delete_fields"], [])
                transcript.append(dict(probe="delete", http_status=code, response=reply))
                if not (200 <= code < 300 and isinstance(reply,dict) and reply.get("status") == "ok"):
                    raise RuntimeError("Deletion not acknowledged; retain this UID and rerun --cancel")
                def mark(current):
                    current["probe_deleted"] = True
                    return current
                store._edit(uid, mark)
            store.close_local(uid)
            doc = store.load(uid)
            eid = uid + ":cancelled"
            if eid not in doc["events"]:
                store.event(uid, dict(event_id=eid, event_type="cancelled", attempt=doc["snapshot"]["preparation"]["attempt"],
                    time_local=doc["probe_delete_fields"]["deleted_at"], details={"protocol_test_only": True}))
            drain()
            print("PASS: deletion acknowledged; late cancellation diagnostic acknowledged without reopening.")
            print("Download this session's Deleted Experiment ZIP and inspect preparation_events.jsonl.")
        print("Experiment:", store.load(uid)["package"]["fields"]["experiment_id"])
    finally:
        if uid:
            record = store.load(uid)
            if record:
                folder = ROOT / "testSM" / "testPreparation" / "evidence"
                folder.mkdir(parents=True, exist_ok=True)
                output = folder / ("step4_" + uid + "_" + mode + ".json")
                # No API key or HTTP authorization headers are included.
                output.write_text(json.dumps(dict(record=record, calls=transcript), ensure_ascii=False, indent=2), encoding="utf-8")
                print("Evidence:", output)
        if lock.owned(): lock.release()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--live", action="store_true")
    group.add_argument("--cancel", action="store_true")
    args = parser.parse_args()
    if not args.live and not args.cancel:
        parser.print_help()
        print("No network requests or robot commands issued.")
    else:
        run("live" if args.live else "cancel")
