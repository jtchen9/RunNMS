"""Registration adapter: validate before mutation; retain original CSV for upload."""
import csv
import io
import json
import math
import sys
import tempfile
import uuid
from pathlib import Path
from fastapi import HTTPException


def validate(raw, poses_raw, root, site_id):
    from experiment_csv import parse_registration_csv
    from preferred_direction_helper import lookup_preferred_direction
    manifest=parse_registration_csv(raw,poses_raw)
    for pose in manifest["initial_positions"]:
        result=lookup_preferred_direction(x_m=pose["x_m"],y_m=pose["y_m"])
        if not result.get("ok"):
            raise ValueError("No preferred heading at initial position of "+pose["scanner"])
        # A derived PLANNED heading for checker/preview, never a measured robot pose.
        pose["heading_deg"]=float(result["preferred_heading_deg"])
        if not math.isfinite(pose["heading_deg"]):
            raise ValueError("Invalid preferred heading at initial position of "+pose["scanner"])
    common=root / "sitemap" / "CommonCheckers"
    if str(common) not in sys.path:sys.path.insert(0,str(common))
    from checker.checker_runner import validate_script
    with tempfile.TemporaryDirectory(prefix="preparation_validate_") as folder:
        script=Path(folder)/"CommandSheet.csv";script.write_bytes(raw)
        poses=Path(folder)/"InitialPoses.csv"
        if poses_raw is not None:poses.write_bytes(poses_raw)
        report=validate_script(script_csv=script,initial_poses_csv=poses,
                               common_dir=common,site_dir=root/"sitemap"/site_id,prepared_manifest=manifest)
    if not report["ok"]:raise HTTPException(422,{"error":"public_validation_failed","public_validation":report})
    manifest["commands"]=report.pop("validated_commands")
    return manifest,report


def planned_poses_csv(manifest):
    f=io.StringIO(newline="");w=csv.writer(f)
    w.writerow(["scanner","intended_x_m","intended_y_m","intended_heading_deg","position_tolerance_m","heading_tolerance_deg"])
    for p in manifest["initial_positions"]:w.writerow([p["scanner"],p["x_m"],p["y_m"],p["heading_deg"],.3,10])
    return f.getvalue().encode("utf-8")


def compile_operations(manifest,start,identity):
    """Same three queue formats as existing enqueue helpers, compiled without writes."""
    import config,utility,m4Commands
    from datetime import timedelta
    ops=[]
    for c in sorted(manifest["commands"],key=lambda x:(x["t_offset_sec"],x["row_number"])):
        scanner,category,action=c["scanner"],c["category"].lower(),c["action"]
        args=dict(m4Commands._resolve_operator_args(scanner,action,m4Commands._resolve_ap_sta_alias(action,c["args"])))
        args["_experiment"]={k:identity[k] for k in ("lab_id","experiment_id","session_id","registration_uid")}
        meta={k:identity[k] for k in ("lab_id","experiment_id","session_id","registration_uid")}
        fields=dict(meta,execute_at=(start+timedelta(seconds=c["t_offset_sec"])).strftime(config.TIME_FMT),
                    created_at=utility.local_ts(),action=action,args_json=json.dumps(args,ensure_ascii=False))
        if category=="mobility":
            fields.update(scanner=scanner,source="script_or_csv")
            key=config.KEY_MOBILITY_CMD_STREAM
        elif category=="traffic":
            fields["scanner"]=scanner;key=config.KEY_TRAFFIC_CMD_STREAM
        elif category=="ap_meta" or action=="ap.location.update":
            loc=dict(mode="fixed",x=float(args["x_m"]),y=float(args["y_m"]),
                     z=float(args["z_m"]) if args.get("z_m") not in (None,"") else None,
                     source="experiment_csv",updated_at=utility.local_ts())
            mapping=dict(device_type="ap",location_json=json.dumps(loc))
            if args.get("alias"):mapping["ap_alias"]=str(args["alias"])
            ops.extend([("hset",config.key_scanner_meta(scanner),mapping),("sadd",config.KEY_REGISTRY,scanner)])
            continue
        else:
            fields["category"]=category;key=config.key_cmd_stream(scanner)
        ops.append(("xadd",key,fields))
    return ops


async def register(csv_file,initial_poses_file,session_id):
    import asyncio
    import config,utility,m4Commands,m8mobility
    import nms_experiment_completion as completion
    import nms_preparation_runtime as runtime
    from nms_preparation_gate import ACTIVITY,active
    from experiment_csv import CsvFormatError
    filename=(csv_file.filename or "").strip()
    if not filename.lower().endswith(".csv") or not Path(filename).stem:
        raise HTTPException(400,"A named command CSV is required")
    if initial_poses_file and not (initial_poses_file.filename or "").lower().endswith(".csv"):
        raise HTTPException(400,"Initial positions upload must be a CSV")
    raw=await csv_file.read();poses_raw=await initial_poses_file.read() if initial_poses_file else None
    root=Path(m4Commands.__file__).resolve().parent
    try:manifest,validation=await asyncio.to_thread(validate,raw,poses_raw,root,m4Commands._nms_lab_id())
    except CsvFormatError as e:raise HTTPException(422,e.as_dict()) from e
    except ValueError as e:raise HTTPException(422,str(e)) from e
    names=sorted({c["scanner"] for c in manifest["commands"]}|{p["scanner"] for p in manifest["initial_positions"]})
    for name in names:m4Commands.m1Registry.require_whitelisted(name)
    routes,preview=await asyncio.to_thread(m4Commands._registration_route_previews,raw,planned_poses_csv(manifest))
    lock=completion.lifecycle_lock()
    if not lock.acquire(blocking=False):raise HTTPException(409,"Experiment lifecycle is busy")
    try:
        with ACTIVITY:
            m4Commands._require_empty_experiment_registry()
            if active():raise HTTPException(409,"Previous preparation cleanup is still waiting for robot polls")
            for scanner in config.r.smembers(config.KEY_REGISTRY):
                if m8mobility._get_state(scanner) not in ("s0idle","s7stopped"):
                    raise HTTPException(409,"Robot still has active work: "+scanner)
            uid=uuid.uuid4().hex;now=utility.local_ts()
            fields=dict(nms_id=m4Commands._nms_lab_id(),lab_id=m4Commands._nms_lab_id(),experiment_id=Path(filename).stem,
                        session_id=session_id or "default",registered_at=now,command_count=len(manifest["commands"]),
                        public_validation_json=json.dumps(validation,ensure_ascii=False),
                        extra_metadata_json=json.dumps({"route_preview":preview,"preparation_required":True}))
            files=[("command_sheet_file",(filename,raw,"text/csv"))]+routes
            if poses_raw is not None:files.append(("initial_poses_file",(initial_poses_file.filename,poses_raw,"text/csv")))
            store=runtime.store()
            store.create(fields,files,dict(phase="awaiting_attempt",attempt=0,updated_at=now),uid)
            registry=dict(lab_id=fields["lab_id"],experiment_id=fields["experiment_id"],session_id=fields["session_id"],
                          registration_uid=uid,registered_at=now,start_at="",end_at="",state="preparing",
                          command_count=str(fields["command_count"]),time_format=config.TIME_FMT,
                          mobility_scanners_json=json.dumps([p["scanner"] for p in manifest["initial_positions"]]))
            store.activate(uid,manifest,config.KEY_EXPERIMENT_REGISTRY,registry,now)
            # The durable gate is now held. The worker clears old queues before
            # any attempt, including recovery from a crash just after activation.
            runtime.wake()
    finally:
        if lock.owned():lock.release()
    return dict(status="ok",registered=True,registration_uid=uid,experiment_id=fields["experiment_id"],
                session_id=fields["session_id"],commands_enqueued=0,commands_held=len(manifest["commands"]),
                t0=None,start_at=None,end_at=None,state="preparing",public_validation=validation,
                route_preview=preview,detail="Health checks and preparation run in the NMS; t0 is assigned after all robots are ready")
