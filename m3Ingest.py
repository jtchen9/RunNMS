from typing import Any, Dict
import hashlib

from fastapi import APIRouter, Body, HTTPException, Request

import config
import utility
import m1Registry

router = APIRouter()


# ===================
# 3) Ingest (opaque payload queued for uplink)
# ===================
@router.post("/ingest/{scanner}", tags=["3 Southbound Ingest (Pi→NMS)"])
async def ingest(
    scanner: str,
    payload: bytes = Body(
        ...,
        media_type="application/octet-stream",
        description="Opaque payload (JSON/CSV/binary). NMS will not parse.",
    ),
    request: Request = ...,
) -> Dict[str, Any]:
    m1Registry.require_whitelisted(scanner)

    body = payload
    received_at = utility.local_ts()
    size = len(body)
    sha256 = hashlib.sha256(body).hexdigest()

    try:
        payload_text = body.decode("utf-8")
    except Exception:
        raise HTTPException(status_code=400, detail="payload must be utf-8 JSON text")

    key = config.key_uplink_stream(scanner)
    config.r.xadd(
        key,
        {
            "received_at": received_at,
            "size": str(size),
            "sha256": sha256,
            "payload_text": payload_text,
        },
        maxlen=config.UPLINK_MAXLEN,
        approximate=True,
    )

    config.r.sadd(config.KEY_REGISTRY, scanner)
    config.r.hset(config.key_scanner_meta(scanner), mapping={"last_seen": received_at})

    return {
        "status": "accepted",
        "scanner": scanner,
        "queued_in": key,
        "bytes": size,
        "sha256": sha256,
        "received_at": received_at,
    }