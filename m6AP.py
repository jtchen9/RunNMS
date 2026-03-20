from typing import Optional, List, Dict, Any
import json

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

import config
import utility
import m1Registry
import m4Commands

router = APIRouter()


# ==================
# 6) AP
# ==================
class APPollStatus(BaseModel):
    mac: str
    ip: Optional[str] = None
    ssids: List[str] = Field(default_factory=list)
    band: Optional[str] = None
    channel: Optional[int] = None
    antenna_count: Optional[int] = None


class APPollReq(BaseModel):
    time: str
    status: APPollStatus


class APAssociationItem(BaseModel):
    sta_mac: str
    ssid: str
    mcs: int


class APTrafficRecord(BaseModel):
    sta_mac: str
    ac: str
    avg_frame_duration_us: float
    frame_count: int
    mcs_distribution: Dict[str, int] = Field(default_factory=dict)


class APTrafficReq(BaseModel):
    time_start: str
    time_end: str
    associations: List[APAssociationItem] = Field(default_factory=list)
    records: List[APTrafficRecord] = Field(default_factory=list)


@router.post("/ap/poll/{scanner}", tags=["6 AP Control (Polling)"])
def ap_poll(
    scanner: str,
    req: APPollReq,
    request: Request,
    limit: int = Query(5, ge=1, le=50),
) -> Dict[str, Any]:
    m1Registry.require_whitelisted(scanner)

    try:
        _ = utility.parse_local_dt(req.time)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"time must be like '{utility.local_ts()}' (format {config.TIME_FMT})"
        )

    server_now_str = utility.local_ts()

    ssids_json = json.dumps(req.status.ssids or [], ensure_ascii=False)

    meta_updates: Dict[str, str] = {
        "device_type": "ap",
        "last_seen": server_now_str,
        "last_ap_poll": server_now_str,
        "status_updated_at": server_now_str,
        "mac": utility.normalize_mac(req.status.mac),
        "ip": (req.status.ip or (request.client.host if request.client and request.client.host else "") or ""),
        "ssids_json": ssids_json,
        "band": str(req.status.band or ""),
        "channel": str(req.status.channel if req.status.channel is not None else ""),
        "antenna_count": str(req.status.antenna_count if req.status.antenna_count is not None else ""),
    }

    try:
        config.r.hset(config.key_scanner_meta(scanner), mapping=meta_updates)
        config.r.sadd(config.KEY_REGISTRY, scanner)
    except Exception:
        pass

    collected = m4Commands._collect_due_commands(scanner=scanner, limit=limit, server_now_str=server_now_str)

    return {
        "scanner": scanner,
        "server_now": server_now_str,
        "time_format": config.TIME_FMT,
        "returned": len(collected["commands"]),
        "skipped": collected["skipped"],
        "commands": collected["commands"],
    }


@router.post("/ap/traffic/{scanner}", tags=["7 AP Performance Upload"])
def ap_traffic(scanner: str, req: APTrafficReq) -> Dict[str, Any]:
    m1Registry.require_whitelisted(scanner)

    try:
        ts0 = utility.parse_local_dt(req.time_start).strftime(config.TIME_FMT)
        ts1 = utility.parse_local_dt(req.time_end).strftime(config.TIME_FMT)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"time_start/time_end must be like '{utility.local_ts()}' (format {config.TIME_FMT})"
        )

    payload = {
        "time_start": ts0,
        "time_end": ts1,
        "associations": [x.model_dump() for x in req.associations],
        "records": [x.model_dump() for x in req.records],
    }
    payload_text = json.dumps(payload, ensure_ascii=False)
    received_at = utility.local_ts()

    try:
        config.r.xadd(
            config.key_ap_uplink_stream(scanner),
            {
                "received_at": received_at,
                "time_start": ts0,
                "time_end": ts1,
                "assoc_count": str(len(req.associations)),
                "record_count": str(len(req.records)),
                "payload_text": payload_text,
            },
            maxlen=config.AP_UPLINK_MAXLEN,
            approximate=True,
        )

        config.r.hset(
            config.key_scanner_meta(scanner),
            mapping={
                "device_type": "ap",
                "last_seen": received_at,
                "last_ap_traffic": received_at,
                "last_ap_traffic_time_start": ts0,
                "last_ap_traffic_time_end": ts1,
                "last_ap_assoc_count": str(len(req.associations)),
                "last_ap_record_count": str(len(req.records)),
            }
        )
        config.r.sadd(config.KEY_REGISTRY, scanner)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed to store AP traffic: {e}")

    return {
        "status": "accepted",
        "scanner": scanner,
        "received_at": received_at,
        "time_start": ts0,
        "time_end": ts1,
        "association_count": len(req.associations),
        "record_count": len(req.records),
        "queued_in": config.key_ap_uplink_stream(scanner),
    }