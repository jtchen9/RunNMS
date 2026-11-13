from typing import Optional, List, Dict, Any
import json

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field, model_validator

import config
import utility
import m1Registry
import m4Commands

router = APIRouter()


# ==================
# 6) AP
# ==================
class APInterfaceItem(BaseModel):
    ifname: Optional[str] = None
    bssid: Optional[str] = None
    ssid: Optional[str] = None
    band: Optional[str] = None
    channel: Optional[int] = None
    freq_mhz: Optional[int] = None
    channel_width_mhz: Optional[int] = None


class APAssociationItem(BaseModel):
    sta_mac: str
    ssid: str
    band: str


class APPollStatus(BaseModel):
    # Preserve additions without making older APs supply them.
    model_config = {"extra": "allow"}
    backhaul_mac: Optional[str] = None
    wireless_macs: Optional[Dict[str, Any]] = None

    mac: str
    ip: Optional[str] = None
    ssids: List[str] = Field(default_factory=list)
    antenna_count: Optional[int] = None
    interfaces: List[APInterfaceItem] = Field(default_factory=list)
    associations: List[APAssociationItem] = Field(default_factory=list)
    device_name: Optional[str] = None


class APPollReq(BaseModel):
    time: str
    status: APPollStatus


class APTrafficRecord(BaseModel):
    model_config = {"extra": "allow"}

    sta_mac: str
    ac: str
    avg_frame_duration_us: float
    frame_count: int
    download_bytes: int = 0
    upload_bytes: int = 0
    mcs_distribution: Dict[str, int] = Field(default_factory=dict)
    mcs_distribution_source: Optional[str] = None


class APTrafficReq(BaseModel):
    # The AP owns the traffic schema; the NMS preserves its contents.
    model_config = {"extra": "allow"}
    time_start: str
    time_end: str
    device_name: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def validate_report_shape(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            raise ValueError("AP traffic report must be an object")
        version = data.get("format_version", 1)
        if type(version) is not int or version not in (1, 2):
            raise ValueError("unsupported AP traffic format_version")
        if version == 2:
            if not isinstance(data.get("radios"), list) or not isinstance(data.get("peers"), list):
                raise ValueError("version 2 requires radios and peers arrays")
            if any(not isinstance(x, dict) for x in data["radios"] + data["peers"]):
                raise ValueError("radios and peers entries must be objects")
            for peer in data["peers"]:
                records = peer.get("records", [])
                if not isinstance(records, list) or any(not isinstance(x, dict) for x in records):
                    raise ValueError("peer records must be an array of objects")
        else:
            # Keep original record validation while retaining AP-owned additions.
            records = data.get("records", [])
            if not isinstance(records, list):
                raise ValueError("records must be an array")
            for record in records:
                APTrafficRecord.model_validate(record)
        return data

    def report_payload(self) -> Dict[str, Any]:
        return self.model_dump(exclude_unset=True)


@router.post("/ap/poll/{scanner}", tags=["6 AP Control"])
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
    interfaces_json = json.dumps([x.model_dump() for x in req.status.interfaces], ensure_ascii=False)
    associations_json = json.dumps([x.model_dump() for x in req.status.associations], ensure_ascii=False)

    meta_updates: Dict[str, str] = {
        "device_type": "ap",
        "last_seen": server_now_str,
        "last_ap_poll": server_now_str,
        "mac": utility.normalize_mac(req.status.mac),
        "ip": (req.status.ip or (request.client.host if request.client and request.client.host else "") or ""),
        "ssids_json": ssids_json,
        "antenna_count": str(req.status.antenna_count if req.status.antenna_count is not None else ""),
        "interfaces_json": interfaces_json,
        "associations_json": associations_json,
        "device_name": str(req.status.device_name or ""),
    }

    # Absent optional fields do not erase metadata during a mixed-image rollout.
    if "backhaul_mac" in req.status.model_fields_set:
        meta_updates["backhaul_mac"] = req.status.backhaul_mac or ""
    if "wireless_macs" in req.status.model_fields_set:
        meta_updates["wireless_macs_json"] = json.dumps(req.status.wireless_macs, ensure_ascii=False)

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


@router.post("/ap/traffic/{scanner}", tags=["6 AP Control"])
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

    payload = req.report_payload()
    payload.update({"time_start": ts0, "time_end": ts1})
    if payload.get("format_version", 1) == 2:
        record_count = sum(len(peer.get("records", [])) for peer in payload["peers"])
    else:
        payload.setdefault("records", [])
        record_count = len(payload["records"])
    payload_text = json.dumps(payload, ensure_ascii=False)
    received_at = utility.local_ts()

    try:
        config.r.xadd(
            config.key_ap_uplink_stream(scanner),
            {
                "received_at": received_at,
                "time_start": ts0,
                "time_end": ts1,
                "record_count": str(record_count),
                "device_name": str(req.device_name or ""),
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
                "last_ap_record_count": str(record_count),
                "device_name": str(req.device_name or ""),
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
        "record_count": record_count,
        "queued_in": config.key_ap_uplink_stream(scanner),
    }
