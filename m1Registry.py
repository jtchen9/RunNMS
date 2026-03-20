from typing import Optional, List, Dict, Any
import json
import base64

from fastapi import APIRouter, HTTPException, File, UploadFile
from pydantic import BaseModel, Field

import config
import utility

router = APIRouter()


# ===================
# 1) Registry & Whitelist
# ===================
class WhitelistItem(BaseModel):
    scanner: str = Field(..., description="Assigned name, e.g. twin-scout-alpha")

    mac: Optional[str] = Field(
        default=None,
        description="MAC address, e.g. 2c:cf:67:d0:67:f3"
    )

    llm_weblink: Optional[str] = Field(
        default=None,
        description="ChatGPT conversation URL for this device"
    )

    tailscaled_state_b64: Optional[str] = Field(
        default=None,
        description="Base64 encoded tailscaled.state file"
    )

    comment: Optional[str] = Field(
        default=None,
        description="Optional note for humans"
    )


class WhitelistUpsertReq(BaseModel):
    items: List[WhitelistItem] = Field(default_factory=list)


class RegisterReq(BaseModel):
    mac: str
    ip: Optional[str] = None
    scanner_version: Optional[str] = None
    capabilities: Optional[str] = None


def require_whitelisted(scanner: str) -> None:
    """
    Whitelist check: uses KEY_WHITELIST_SCANNER_META.
    """
    if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, (scanner or "").strip()):
        raise HTTPException(status_code=403, detail=f"Scanner '{scanner}' not in whitelist")


def whitelist_meta_get(scanner: str) -> Dict[str, Any]:
    """
    Return parsed whitelist meta for a scanner.
    Expected stored value: JSON dict with at least {scanner, mac, llm_weblink}.
    """
    scanner = (scanner or "").strip()
    if not scanner:
        return {}

    s = config.r.hget(config.KEY_WHITELIST_SCANNER_META, scanner)
    if not s:
        return {}

    try:
        j = json.loads(s)
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}


def find_scanner_by_mac(mac: str) -> str:
    """
    Reverse lookup: scan whitelist meta objects and match by mac.
    """
    mac = utility.normalize_mac(mac)
    cursor = 0
    while True:
        cursor, pairs = config.r.hscan(config.KEY_WHITELIST_SCANNER_META, cursor=cursor, count=200)
        for scanner, meta_s in pairs.items():
            try:
                meta = json.loads(meta_s) if meta_s else {}
            except Exception:
                meta = {}
            if utility.normalize_mac(meta.get("mac", "")) == mac:
                return scanner
        if int(cursor) == 0:
            break
    return ""


@router.get("/registry/_list_whitelists", tags=["1 Registry & Whitelist"])
def registry_list_whitelists() -> Dict[str, Any]:
    raw = config.r.hgetall(config.KEY_WHITELIST_SCANNER_META) or {}
    items: List[Dict[str, Any]] = []

    for scanner, meta_s in raw.items():
        try:
            meta = json.loads(meta_s)
            if not isinstance(meta, dict):
                meta = {"scanner": scanner, "meta_raw": meta_s}
        except Exception:
            meta = {"scanner": scanner, "meta_raw": meta_s}

        meta["scanner"] = meta.get("scanner") or scanner

        tailscaled_state_b64 = (meta.get("tailscaled_state_b64") or "").strip()
        if tailscaled_state_b64:
            meta["tailscaled_state_present"] = True
            meta["tailscaled_state_b64_size"] = len(tailscaled_state_b64)
        else:
            meta["tailscaled_state_present"] = False
            meta["tailscaled_state_b64_size"] = 0

        meta.pop("tailscaled_state_b64", None)
        items.append(meta)

    items.sort(key=lambda x: (x.get("scanner") or ""))

    return {
        "time": utility.local_ts(),
        "count": len(items),
        "items": items,
        "key": config.KEY_WHITELIST_SCANNER_META,
        "schema_version": int(config.WHITELIST_SCHEMA_VERSION),
    }


@router.get("/registry/_list_whitelist_meta_reverse/{mac}", tags=["1 Registry & Whitelist"])
def registry_list_whitelist_meta_reverse(mac: str) -> Dict[str, Any]:
    mac_n = utility.normalize_mac(mac)
    scanner = find_scanner_by_mac(mac_n)
    found = bool(scanner)
    meta = whitelist_meta_get(scanner) if found else {}
    return {
        "time": utility.local_ts(),
        "mac": mac_n,
        "scanner": scanner,
        "found": found,
        "meta": meta,
    }


@router.post("/registry/_whitelist_upsert", tags=["1 Registry & Whitelist"])
def registry_whitelist_upsert(req: WhitelistUpsertReq) -> Dict[str, Any]:
    if not req.items:
        return {"status": "ok", "upserted": 0}

    now = utility.local_ts()
    upserted = 0

    for it in req.items:
        scanner = (it.scanner or "").strip()
        if not scanner:
            continue

        old = whitelist_meta_get(scanner)

        old_mac = utility.normalize_mac(old.get("mac", "")) if old else ""
        old_llm = (old.get("llm_weblink", "") or "").strip() if old else ""
        old_tailscaled_state_b64 = (old.get("tailscaled_state_b64", "") or "").strip() if old else ""
        old_comment = (old.get("comment", "") or "").strip() if old else ""

        if it.mac is None or str(it.mac).strip() == "":
            new_mac = old_mac
        else:
            new_mac = utility.normalize_mac(it.mac)

        if it.llm_weblink is None or str(it.llm_weblink).strip() == "":
            new_llm = old_llm
        else:
            new_llm = (it.llm_weblink or "").strip()

        if it.tailscaled_state_b64 is None or str(it.tailscaled_state_b64).strip() == "":
            new_tailscaled_state_b64 = old_tailscaled_state_b64
        else:
            new_tailscaled_state_b64 = (it.tailscaled_state_b64 or "").strip()

        if it.comment is None:
            new_comment = old_comment
        else:
            new_comment = (it.comment or "").strip()

        if not new_llm:
            new_llm = config.DEFAULT_LLM_WEBLINK

        meta = {
            "schema_version": int(config.WHITELIST_SCHEMA_VERSION),
            "scanner": scanner,
            "mac": new_mac,
            "llm_weblink": new_llm,
            "tailscaled_state_b64": new_tailscaled_state_b64,
            "comment": new_comment,
            "updated_at": now,
        }

        config.r.hset(config.KEY_WHITELIST_SCANNER_META, scanner, json.dumps(meta, ensure_ascii=False))
        upserted += 1

    return {
        "status": "ok",
        "time": now,
        "upserted": int(upserted),
        "key": config.KEY_WHITELIST_SCANNER_META,
        "schema_version": int(config.WHITELIST_SCHEMA_VERSION),
    }


@router.delete("/registry/_whitelist/{scanner}", tags=["1 Registry & Whitelist"])
def registry_whitelist_delete(scanner: str) -> Dict[str, Any]:
    scanner = (scanner or "").strip()
    if not scanner:
        raise HTTPException(status_code=400, detail="scanner required")

    removed = config.r.hdel(config.KEY_WHITELIST_SCANNER_META, scanner)
    return {
        "status": "ok",
        "time": utility.local_ts(),
        "scanner": scanner,
        "removed": int(removed),
        "key": config.KEY_WHITELIST_SCANNER_META,
    }


@router.post("/registry/register", tags=["1 Registry & Whitelist"])
def register(req: RegisterReq) -> Dict[str, Any]:
    mac = utility.normalize_mac(req.mac)
    scanner = find_scanner_by_mac(mac)
    if not scanner:
        raise HTTPException(status_code=403, detail=f"MAC '{mac}' not in whitelist")

    wmeta = whitelist_meta_get(scanner)
    llm = (wmeta.get("llm_weblink") or "").strip() or config.DEFAULT_LLM_WEBLINK
    tailscaled_state_b64 = (wmeta.get("tailscaled_state_b64") or "").strip()

    now = utility.local_ts()
    config.r.sadd(config.KEY_REGISTRY, scanner)

    updates: Dict[str, str] = {"last_seen": now, "mac": mac}

    if req.ip:
        updates["ip"] = req.ip
    if req.scanner_version:
        updates["scanner_version"] = req.scanner_version
    if req.capabilities:
        updates["capabilities"] = req.capabilities

    caps = (req.capabilities or "").lower()
    if "ap" in caps:
        updates["device_type"] = "ap"
    else:
        updates["device_type"] = "robot"

    config.r.hset(config.key_scanner_meta(scanner), mapping=updates)

    return {
        "scanner": scanner,
        "llm_weblink": llm,
        "tailscaled_state_b64": tailscaled_state_b64,
        "time": now,
        "time_format": config.TIME_FMT,
    }


@router.get("/registry/_list_scanners", tags=["1 Registry & Whitelist"])
def registry_list_scanners() -> Dict[str, Any]:
    scanners = sorted(list(config.r.smembers(config.KEY_REGISTRY)))
    out = [{"scanner": s, "meta": config.r.hgetall(config.key_scanner_meta(s))} for s in scanners]
    return {"time": utility.local_ts(), "count": len(out), "scanners": out}


@router.post("/registry/_tailscaled_state_upload/{scanner}", tags=["1 Registry & Whitelist"])
async def registry_tailscaled_state_upload(
    scanner: str,
    state_file: UploadFile = File(..., description="Binary tailscaled.state file"),
) -> Dict[str, Any]:
    """
    Operator-only:
    Upload a binary tailscaled.state file for one scanner/AP,
    store it in whitelist meta as base64 text.
    """
    scanner = (scanner or "").strip()
    if not scanner:
        raise HTTPException(status_code=400, detail="scanner required")

    if not config.r.hexists(config.KEY_WHITELIST_SCANNER_META, scanner):
        raise HTTPException(status_code=404, detail=f"scanner '{scanner}' not found in whitelist")

    data = await state_file.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty uploaded file")

    try:
        b64 = base64.b64encode(data).decode("ascii")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed to base64 encode file: {e}")

    old = whitelist_meta_get(scanner)
    if not old:
        raise HTTPException(status_code=500, detail=f"failed to load whitelist meta for '{scanner}'")

    now = utility.local_ts()

    meta = {
        "schema_version": int(old.get("schema_version") or config.WHITELIST_SCHEMA_VERSION),
        "scanner": scanner,
        "mac": utility.normalize_mac(old.get("mac", "")),
        "llm_weblink": (old.get("llm_weblink") or "").strip() or config.DEFAULT_LLM_WEBLINK,
        "tailscaled_state_b64": b64,
        "comment": (old.get("comment") or "").strip(),
        "updated_at": now,
    }

    config.r.hset(config.KEY_WHITELIST_SCANNER_META, scanner, json.dumps(meta, ensure_ascii=False))

    return {
        "status": "ok",
        "scanner": scanner,
        "filename": state_file.filename or "",
        "bytes": len(data),
        "tailscaled_state_present": True,
        "tailscaled_state_b64_size": len(b64),
        "time": now,
    }