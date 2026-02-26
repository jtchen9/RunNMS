from fastapi import Body, FastAPI, Request, HTTPException, Query, File, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import redis
from datetime import datetime, timedelta
import hashlib
import uuid
import json
import uvicorn
import requests
import csv
import io
import asyncio

# =============================================================================
# Constants / Config  (NO getenv; lab-only hardcoded)
# Place ALL constants + Redis key conventions here, before app = FastAPI(...)
# =============================================================================
WEB_BASE: str = "http://localhost:80"
# WEB_BASE: str = "https://6g-private.com:80"

REDIS_URL: str = "redis://localhost:6379/0"
KEY_PREFIX: str = "nms:"  # Redis keyspace prefix

# ---- Time format (ONE format everywhere) ----
TIME_FMT: str = "%Y-%m-%d-%H:%M:%S"  

# -----------------------------------------------------------------------------#
# 1) Registry & Whitelist
# -----------------------------------------------------------------------------#
KEY_REGISTRY: str = f"{KEY_PREFIX}registry:scanners"                      # SET(scanner_name)
def key_scanner_meta(scanner: str) -> str:
    return f"{KEY_PREFIX}scanner:{scanner}:meta"                          # HASH(...)

# -----------------------------------------------------------------------------#
# 2) Bootstrap (Bundles)
# -----------------------------------------------------------------------------#
BUNDLE_DIR: Path = Path(r"D:\Data\_Action\_RunNMS\bundles")
KEY_BUNDLE_INDEX: str = f"{KEY_PREFIX}bundle:index"                       # HASH(bundle_id -> json(meta))
BUNDLE_META_SCHEMA_VERSION = 1

# -----------------------------------------------------------------------------#
# 3) Southbound Ingest (Pi→NMS)
# -----------------------------------------------------------------------------#
UPLINK_MAXLEN: int = 50000
UPLOAD_ENABLED: bool = True

def key_uplink_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}uplink:{scanner}"                                # STREAM(opaque payloads)

# -----------------------------------------------------------------------------#
# 4) Commands (Polling)
# -----------------------------------------------------------------------------#
CMD_EXPIRE_SEC: int = 3600  # seconds

def key_cmd_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}cmd:{scanner}"                                   # STREAM(commands)

def key_cmdack_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}cmdack:{scanner}"                                # STREAM(acks)

# -----------------------------------------------------------------------------#
# 5) Northbound (NMS → Web Server)
# -----------------------------------------------------------------------------#
NMS_ID: str = "nms-lab-01"
WEB_API_KEY: str = ""  # optional in early dev

WEB_NMS_UPLOAD_URL: str = f"{WEB_BASE}/nms/upload_scan_batch"
WEB_NMS_STATUS_URL: str = f"{WEB_BASE}/nms/report_status"

NORTHBOUND_EVERY_SEC: int = 10
UPLOAD_BATCH_MAX_BYTES: int = 100_000   # hard cap per POST (≈100KB)

# optional operator visibility
KEY_NB_LAST_UPLOAD: str = f"{KEY_PREFIX}nb:last_upload"   # HASH(robot -> time)
KEY_NB_LAST_RESULT: str = f"{KEY_PREFIX}nb:last_result"   # HASH(robot -> result)
KEY_NB_LAST_STATUS: str = f"{KEY_PREFIX}nb:last_status"   # STRING (time/result summary)

# =============================================================================
# Runtime init
# =============================================================================
BUNDLE_DIR.mkdir(parents=True, exist_ok=True)
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# =============================================================================
# App
# =============================================================================
app = FastAPI(
    title="Wi-Fi NMS (Control-plane + Pass-through Relay)",
    description=(
        "This NMS is intentionally *not* a data processor.\n\n"
        "- Receives opaque scan payloads from scanners and queues them for uplink\n"
        "- Manages scanner whitelist + registry\n"
        "- Provides bootstrap bundles (ZIP) for initializing/updating scanners\n"
        "- Provides command enqueue/poll/ack (extendible: scan/robot/video/audio)\n"
    ),
)

# =============================================================================
# Utilities (time: ONE format everywhere)
# =============================================================================
def local_ts() -> str:
    """Current local time string in the ONE official format  (TIME_FMT)."""
    return datetime.now().strftime(TIME_FMT)

def parse_local_dt(s: str) -> datetime:
    """
    Parse local time string into naive datetime.
    REQUIRED format (after cleanup): TIME_FMT

    Cleanup accepted (to tolerate pasted strings):
      - 'T' replaced by space
      - trailing 'Z' removed
      - timezone suffix like '+08:00' removed
      - microseconds removed
      - spaces removed
    """
    s = (s or "").strip()
    if not s:
        raise ValueError("empty time string")

    s = s.replace("T", " ").strip()
    if s.endswith("Z"):
        s = s[:-1].strip()
    if "+" in s:
        s = s.split("+", 1)[0].strip()
    if "." in s:
        s = s.split(".", 1)[0].strip()

    # remove spaces to be strict about MM-DD-HH:MM:SS
    s = s.replace(" ", "")

    return datetime.strptime(s, TIME_FMT)

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _bundle_zip_path(bundle_id: str) -> Path:
    return BUNDLE_DIR / f"{bundle_id}.zip"

def _validate_bundle_id(bundle_id: str) -> str:
    if not bundle_id or not bundle_id.startswith("robotBundle"):
        raise HTTPException(status_code=400, detail="bundle_id must start with 'robotBundle'")
    for ch in bundle_id:
        if not (ch.isalnum() or ch in "._-"):
            raise HTTPException(status_code=400, detail="bundle_id contains invalid characters")
    return bundle_id

def _bundle_exists(bundle_id: str) -> bool:
    if r.hexists(KEY_BUNDLE_INDEX, bundle_id):
        return True
    return _bundle_zip_path(bundle_id).exists()

def _bundle_meta_get(bundle_id: str) -> Dict[str, Any]:
    s = r.hget(KEY_BUNDLE_INDEX, bundle_id)
    raw: Any = None

    if not s:
        # Return canonical "empty" meta (caller may treat as not-found)
        return {}

    try:
        raw = json.loads(s)
    except Exception:
        raw = None

    # If corrupted / not a dict, wrap it
    if not isinstance(raw, dict):
        raw = {"bundle_id": bundle_id, "meta_raw": s}

    # Normalize to canonical schema (always same keys)
    meta: Dict[str, Any] = {
        "schema_version": int(raw.get("schema_version") or BUNDLE_META_SCHEMA_VERSION),
        "bundle_id": str(raw.get("bundle_id") or bundle_id),
        "sha256": str(raw.get("sha256") or ""),
        "size_bytes": int(raw.get("size_bytes") or 0),
        "uploaded_at": str(raw.get("uploaded_at") or ""),
        "path": str(raw.get("path") or str(_bundle_zip_path(bundle_id))),
        "stored_as": str(raw.get("stored_as") or Path(str(raw.get("path") or _bundle_zip_path(bundle_id))).name),
        "comment": str(raw.get("comment") or ""),
    }

    # Optional: write back normalized meta to Redis to clean up old entries
    try:
        r.hset(KEY_BUNDLE_INDEX, bundle_id, json.dumps(meta, ensure_ascii=False))
    except Exception:
        pass

    return meta

def _web_headers() -> Dict[str, str]:
    h: Dict[str, str] = {}
    if WEB_API_KEY:
        h["X-API-Key"] = WEB_API_KEY
    return h

def _safe_parse_entries_list(payload_text: str) -> Optional[List[Any]]:
    """
    Minimal validation only: must be JSON list.
    No further inspection of list contents.
    """
    try:
        v = json.loads(payload_text)
    except Exception:
        return None
    if not isinstance(v, list):
        return None
    return v

def _json_bytes(obj: Any) -> int:
    # estimate size on wire (utf-8 json)
    return len(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))

# =============================================================================
# 0) Health
# =============================================================================
@app.get("/health", tags=["0 Health"])
def health() -> Dict[str, Any]:
    try:
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {
        "status": "ok",
        "redis_ok": redis_ok,
        "upload_enabled": UPLOAD_ENABLED,
        "web_post_url_configured": bool(WEB_NMS_UPLOAD_URL and WEB_NMS_STATUS_URL),
        "time": local_ts(),
        "time_format": TIME_FMT,
    }

# =============================================================================
# 1) Registry & Whitelist  (REVISED: whitelist now stores mac + llm_weblink)
# =============================================================================

# --- NEW whitelist storage (canonical) ---
KEY_WHITELIST_SCANNER_META: str = f"{KEY_PREFIX}registry:whitelist_scanner_meta"  # HASH(scanner -> json(meta))
WHITELIST_SCHEMA_VERSION: int = 1

# NOTE: KEY_REGISTRY and key_scanner_meta(scanner) remain unchanged.

class WhitelistItem(BaseModel):
    scanner: str = Field(..., description="Assigned name, e.g. twin-scout-alpha")
    mac: str = Field(..., description="MAC address, e.g. 2c:cf:67:d0:67:f3")
    llm_weblink: str = Field(default="", description="ChatGPT conversation URL for this Pi")
    comment: Optional[str] = Field(default="", description="Optional note for humans")

class WhitelistUpsertReq(BaseModel):
    items: List[WhitelistItem] = Field(default_factory=list)

class RegisterReq(BaseModel):
    mac: str
    ip: Optional[str] = None
    scanner_version: Optional[str] = None
    capabilities: Optional[str] = None

def normalize_mac(mac: str) -> str:
    return (mac or "").strip().lower()

def require_whitelisted(scanner: str) -> None:
    """
    Whitelist check (UPDATED): uses KEY_WHITELIST_SCANNER_META.
    """
    if not r.hexists(KEY_WHITELIST_SCANNER_META, (scanner or "").strip()):
        raise HTTPException(status_code=403, detail=f"Scanner '{scanner}' not in whitelist")

def whitelist_meta_get(scanner: str) -> Dict[str, Any]:
    """
    Return parsed whitelist meta for a scanner.
    Expected stored value: JSON dict with at least {scanner, mac, llm_weblink}.
    """
    scanner = (scanner or "").strip()
    if not scanner:
        return {}

    s = r.hget(KEY_WHITELIST_SCANNER_META, scanner)
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
    mac = normalize_mac(mac)
    cursor = 0
    while True:
        cursor, pairs = r.hscan(KEY_WHITELIST_SCANNER_META, cursor=cursor, count=200)
        for scanner, meta_s in pairs.items():
            try:
                meta = json.loads(meta_s) if meta_s else {}
            except Exception:
                meta = {}
            if normalize_mac(meta.get("mac", "")) == mac:
                return scanner
        if int(cursor) == 0:
            break
    return ""

# -----------------------------------------------------------------------------
# Operator-only: list all whitelist entries (FULL items)
# -----------------------------------------------------------------------------
@app.get("/registry/_list_whitelists", tags=["1 Registry & Whitelist"])
def registry_list_whitelists() -> Dict[str, Any]:
    raw = r.hgetall(KEY_WHITELIST_SCANNER_META) or {}
    items: List[Dict[str, Any]] = []

    for scanner, meta_s in raw.items():
        try:
            meta = json.loads(meta_s)
            if not isinstance(meta, dict):
                meta = {"scanner": scanner, "meta_raw": meta_s}
        except Exception:
            meta = {"scanner": scanner, "meta_raw": meta_s}

        # Ensure at least scanner field exists
        meta["scanner"] = meta.get("scanner") or scanner
        items.append(meta)

    items.sort(key=lambda x: (x.get("scanner") or ""))

    return {
        "time": local_ts(),
        "count": len(items),
        "items": items,
        "key": KEY_WHITELIST_SCANNER_META,
        "schema_version": int(WHITELIST_SCHEMA_VERSION),
    }

# -----------------------------------------------------------------------------
# Operator-only: whitelist meta for one scanner (metadata style)
# -----------------------------------------------------------------------------
@app.get("/registry/_list_whitelist_meta/{scanner}", tags=["1 Registry & Whitelist"])
def registry_list_whitelist_meta(scanner: str) -> Dict[str, Any]:
    scanner = (scanner or "").strip()
    exists = bool(r.hexists(KEY_WHITELIST_SCANNER_META, scanner))
    meta = whitelist_meta_get(scanner) if exists else {}

    return {
        "time": local_ts(),
        "whitelist_key": KEY_WHITELIST_SCANNER_META,
        "scanner": scanner,
        "hexists": exists,
        "meta": meta,
    }

# -----------------------------------------------------------------------------
# Operator-only: reverse lookup scanner by mac
# -----------------------------------------------------------------------------
@app.get("/registry/_list_whitelist_meta_reverse/{mac}", tags=["1 Registry & Whitelist"])
def registry_list_whitelist_meta_reverse(mac: str) -> Dict[str, Any]:
    mac_n = normalize_mac(mac)
    scanner = find_scanner_by_mac(mac_n)
    found = bool(scanner)
    meta = whitelist_meta_get(scanner) if found else {}
    return {
        "time": local_ts(),
        "mac": mac_n,
        "scanner": scanner,
        "found": found,
        "meta": meta,
    }

# -----------------------------------------------------------------------------
# Operator-only: upsert whitelist items (scanner -> json(meta))
# -----------------------------------------------------------------------------
DEFAULT_LLM_WEBLINK = "https://chatgpt.com/"

@app.post("/registry/_whitelist_upsert", tags=["1 Registry & Whitelist"])
def registry_whitelist_upsert(req: WhitelistUpsertReq) -> Dict[str, Any]:
    if not req.items:
        return {"status": "ok", "upserted": 0}

    now = local_ts()
    upserted = 0

    for it in req.items:
        scanner = (it.scanner or "").strip()
        mac = normalize_mac(it.mac)
        llm = (it.llm_weblink or "").strip()
        comment = (it.comment or "").strip() if it.comment else ""

        if not scanner:
            continue
        if not mac:
            continue
        if not llm:
            llm = DEFAULT_LLM_WEBLINK

        meta = {
            "schema_version": int(WHITELIST_SCHEMA_VERSION),
            "scanner": scanner,
            "mac": mac,
            "llm_weblink": llm,
            "comment": comment,
            "updated_at": now,
        }

        r.hset(KEY_WHITELIST_SCANNER_META, scanner, json.dumps(meta, ensure_ascii=False))
        upserted += 1

    return {
        "status": "ok",
        "time": now,
        "upserted": int(upserted),
        "key": KEY_WHITELIST_SCANNER_META,
        "schema_version": int(WHITELIST_SCHEMA_VERSION),
    }

# -----------------------------------------------------------------------------
# Operator-only: delete whitelist entry by scanner
# -----------------------------------------------------------------------------
@app.delete("/registry/_whitelist/{scanner}", tags=["1 Registry & Whitelist"])
def registry_whitelist_delete(scanner: str) -> Dict[str, Any]:
    scanner = (scanner or "").strip()
    if not scanner:
        raise HTTPException(status_code=400, detail="scanner required")

    removed = r.hdel(KEY_WHITELIST_SCANNER_META, scanner)
    return {
        "status": "ok",
        "time": local_ts(),
        "scanner": scanner,
        "removed": int(removed),
        "key": KEY_WHITELIST_SCANNER_META,
    }

# -----------------------------------------------------------------------------
# Pi-facing: register by mac -> returns {scanner, llm_weblink}
# -----------------------------------------------------------------------------
@app.post("/registry/register", tags=["1 Registry & Whitelist"])
def register(req: RegisterReq) -> Dict[str, Any]:
    mac = normalize_mac(req.mac)
    scanner = find_scanner_by_mac(mac)
    if not scanner:
        raise HTTPException(status_code=403, detail=f"MAC '{mac}' not in whitelist")

    wmeta = whitelist_meta_get(scanner)
    llm = (wmeta.get("llm_weblink") or "").strip() or DEFAULT_LLM_WEBLINK

    now = local_ts()
    r.sadd(KEY_REGISTRY, scanner)

    updates: Dict[str, str] = {"last_seen": now, "mac": mac}
    if req.ip:
        updates["ip"] = req.ip
    if req.scanner_version:
        updates["scanner_version"] = req.scanner_version
    if req.capabilities:
        updates["capabilities"] = req.capabilities

    r.hset(key_scanner_meta(scanner), mapping=updates)

    return {
        "scanner": scanner,
        "llm_weblink": llm,
        "time": now,
        "time_format": TIME_FMT,
    }

# -----------------------------------------------------------------------------
# Operator-only: list registered scanners (unchanged, but keep underscore)
# -----------------------------------------------------------------------------
@app.get("/registry/_list_scanners", tags=["1 Registry & Whitelist"])
def registry_list_scanners() -> Dict[str, Any]:
    scanners = sorted(list(r.smembers(KEY_REGISTRY)))
    out = [{"scanner": s, "meta": r.hgetall(key_scanner_meta(s))} for s in scanners]
    return {"time": local_ts(), "count": len(out), "scanners": out}

# =============================================================================
# 2) Bootstrap (Bundles)
# =============================================================================
class BootstrapReport(BaseModel):
    installed_version: str

@app.get("/bootstrap/_list_bundles", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_list_bundles() -> Dict[str, Any]:
    """
    Debug-only: list all bundles known to NMS.

    Source of truth is Redis KEY_BUNDLE_INDEX.
    We also do a disk existence check for each bundle path.
    """
    idx = r.hgetall(KEY_BUNDLE_INDEX) or {}

    items: List[Dict[str, Any]] = []
    for bundle_id, meta_s in idx.items():
        try:
            meta = json.loads(meta_s)
            if not isinstance(meta, dict):
                meta = {"bundle_id": bundle_id, "meta_raw": meta_s}
                
        except Exception:
            meta = {"bundle_id": bundle_id, "meta_raw": meta_s}

        # Disk check
        p = Path(meta.get("path") or str(_bundle_zip_path(bundle_id)))
        meta["bundle_id"] = meta.get("bundle_id") or bundle_id
        meta["exists_on_disk"] = p.exists()
        meta["disk_path_checked"] = str(p)

        if p.exists() and "size_bytes" not in meta:
            try:
                meta["size_bytes"] = int(p.stat().st_size)
            except Exception:
                pass

        items.append(meta)

    items.sort(key=lambda x: x.get("bundle_id", ""))

    return {
        "time": local_ts(),
        "count": len(items),
        "items": items,
        "bundle_dir": str(BUNDLE_DIR),
        "index_key": KEY_BUNDLE_INDEX,
    }

@app.get("/bootstrap/_list_bundle_meta/{scanner}", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_list_bundle_meta(scanner: str) -> Dict[str, Any]:
    """
    Debug-only: show bundle-related telemetry fields in scanner meta.
    - scanner_version: reported during /registry/register
    - installed_version: reported during /bootstrap/report/{scanner}
    """
    require_whitelisted(scanner)
    meta = r.hgetall(key_scanner_meta(scanner)) or {}

    scanner_version = meta.get("scanner_version", "")
    installed_version = meta.get("installed_version", "")

    return {
        "time": local_ts(),
        "scanner": scanner,
        "scanner_version": scanner_version,
        "installed_version": installed_version,
        "effective_bundle": (installed_version or scanner_version),
        "note": "Debug only. Upgrades are controlled by commands; these fields are telemetry.",
    }

@app.post("/bootstrap/_bundle", tags=["2 Bootstrap (Init/Update)"])
async def bootstrap_bundle_upload(
    bundle_id: str = Query(..., description="Bundle ID, e.g. robotBundle1.0 (no .zip)"),
    bundle: UploadFile = File(..., description="ZIP file (robot bundle)"),
) -> Dict[str, Any]:
    bundle_id = _validate_bundle_id(bundle_id)

    ctype = (bundle.content_type or "").lower()
    if ctype and ("zip" not in ctype) and ("application/octet-stream" not in ctype):
        raise HTTPException(status_code=415, detail="uploaded file must be a zip")

    data = await bundle.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty uploaded file")

    target_path = _bundle_zip_path(bundle_id)

    overwrite = False
    renamed_old_to = ""
    previous_meta: Dict[str, Any] = {}

    if target_path.exists() or r.hexists(KEY_BUNDLE_INDEX, bundle_id):
        overwrite = True
        previous_meta = _bundle_meta_get(bundle_id)

        if target_path.exists():
            ts_tag = local_ts().replace("-", "").replace(":", "")
            backup_name = f"{bundle_id}__old__{ts_tag}.zip"
            backup_path = BUNDLE_DIR / backup_name
            try:
                target_path.rename(backup_path)
                renamed_old_to = backup_name
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"failed to rename old bundle: {e}")

    try:
        target_path.write_bytes(data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed to store bundle: {e}")

    meta = {
        "schema_version": int(BUNDLE_META_SCHEMA_VERSION),
        "bundle_id": bundle_id,
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": int(len(data)),
        "uploaded_at": local_ts(),
        "path": str(target_path),
        "stored_as": target_path.name,
        "comment": "",
    }
    r.hset(KEY_BUNDLE_INDEX, bundle_id, json.dumps(meta, ensure_ascii=False))

    resp: Dict[str, Any] = {
        "status": "ok",
        "bundle_id": bundle_id,
        "size_bytes": int(len(data)),
        "stored_as": target_path.name,
    }
    if overwrite:
        resp["overwrite"] = True
        resp["previous"] = {"meta": previous_meta, "renamed_to": renamed_old_to}
    return resp

@app.delete("/bootstrap/_bundle/{bundle_id}", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_bundle_delete(bundle_id: str) -> Dict[str, Any]:
    bundle_id = _validate_bundle_id(bundle_id)

    meta_s = r.hget(KEY_BUNDLE_INDEX, bundle_id)
    catalog_found = bool(meta_s)

    meta: Dict[str, Any] = {}
    if meta_s:
        try:
            meta = json.loads(meta_s)
        except Exception:
            meta = {"bundle_id": bundle_id, "meta_raw": meta_s}

    path = Path(meta.get("path") or str(_bundle_zip_path(bundle_id)))
    file_found = path.exists()

    deleted_file = False
    file_error = ""

    if file_found:
        try:
            path.unlink()
            deleted_file = True
        except Exception as e:
            file_error = str(e)

    deleted_catalog = int(r.hdel(KEY_BUNDLE_INDEX, bundle_id)) if catalog_found else 0
    not_found = (not catalog_found) and (not file_found)

    resp: Dict[str, Any] = {
        "status": "ok",
        "bundle_id": bundle_id,
        "catalog_found": catalog_found,
        "file_found": file_found,
        "deleted_catalog": deleted_catalog,
        "deleted_file": deleted_file,
        "not_found": not_found,
    }
    if file_error:
        resp["file_error"] = file_error
    if meta:
        resp["previous_meta"] = meta
    return resp

@app.get("/bootstrap/bundle/{bundle_id}", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_bundle(bundle_id: str):
    bundle_id = _validate_bundle_id(bundle_id)

    meta_s = r.hget(KEY_BUNDLE_INDEX, bundle_id)
    if not meta_s:
        raise HTTPException(status_code=404, detail=f"bundle not found: {bundle_id}")

    try:
        meta = json.loads(meta_s)
    except Exception:
        raise HTTPException(status_code=500, detail=f"bundle catalog corrupted: {bundle_id}")

    path = Path(meta.get("path") or str(_bundle_zip_path(bundle_id)))
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"bundle missing on disk: {bundle_id}")

    return FileResponse(path=str(path), filename=path.name, media_type="application/zip")

@app.post("/bootstrap/report/{scanner}", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_report(scanner: str, report: BootstrapReport) -> Dict[str, Any]:
    require_whitelisted(scanner)
    meta_k = key_scanner_meta(scanner)
    now = local_ts()
    r.hset(meta_k, mapping={
        "installed_version": report.installed_version,
        "last_bootstrap": now,
        "last_seen": now,
    })
    return {"status": "ok", "scanner": scanner, "installed_version": report.installed_version, "time": now}

# =============================================================================
# 3) Ingest (opaque payload queued for uplink)
# =============================================================================
@app.post("/ingest/{scanner}", tags=["3 Southbound Ingest (Pi→NMS)"])
async def ingest(
    scanner: str,
    payload: bytes = Body(..., media_type="application/octet-stream",
                          description="Opaque payload (JSON/CSV/binary). NMS will not parse."),
    request: Request = ...,
):
    require_whitelisted(scanner)

    body = payload
    received_at = local_ts()
    size = len(body)
    sha256 = hashlib.sha256(body).hexdigest()

    # store raw UTF-8 text (Pi is expected to send JSON)
    try:
        payload_text = body.decode("utf-8")
    except Exception:
        # cannot decode -> skip and reject (better than poisoning queue)
        raise HTTPException(status_code=400, detail="payload must be utf-8 JSON text")

    key = key_uplink_stream(scanner)
    r.xadd(
        key,
        {
            "received_at": received_at,
            "size": str(size),
            "sha256": sha256,
            "payload_text": payload_text,
        },
        maxlen=UPLINK_MAXLEN,
        approximate=True,
    )

    r.sadd(KEY_REGISTRY, scanner)
    r.hset(key_scanner_meta(scanner), mapping={"last_seen": received_at})

    return {"status": "accepted", "scanner": scanner, "queued_in": key, "bytes": size, "sha256": sha256, "received_at": received_at}

# =============================================================================
# 4) Commands
# =============================================================================
class Cmd(BaseModel):
    category: str = "scan"
    action: str
    execute_at: str  # MUST be TIME_FMT
    args: Dict[str, Any] = Field(default_factory=dict)
    args_json_text: Optional[str] = None

class CmdAck(BaseModel):
    cmd_id: str
    status: str  # ok | error
    finished_at: Optional[str] = None  # MUST be TIME_FMT if provided
    detail: Optional[str] = None

@app.get("/cmd/_list_command_queues", tags=["4 Commands (Polling)"])
def cmd_list_command_queues() -> Dict[str, Any]:
    """
    Debug-only: show per-scanner command queue status (metadata only).
    This corresponds to Redis stream: nms:cmd:{scanner}

    Iteration source: KEY_REGISTRY (active/seen scanners), for symmetry with /relay/_list_payload_queues.
    """
    scanners = sorted(list(r.smembers(KEY_REGISTRY)))
    out: List[Dict[str, Any]] = []

    for s in scanners:
        key = key_cmd_stream(s)
        length = int(r.xlen(key))
        if length > 0:
            age_sec, oldest_id = _stream_oldest_age_sec(key)
        else:
            age_sec, oldest_id = 0, ""

        out.append({
            "queue_type": "command",
            "scanner": s,
            "key": key,
            "length": length,
            "maxlen": 5000,
            "oldest_age_sec": int(age_sec),
            "oldest_id": oldest_id,
        })

    return {
        "time": local_ts(),
        "count": len(out),
        "items": out,
    }

@app.get("/cmd/_list_command_queue/{scanner}", tags=["4 Commands (Polling)"])
def cmd_list_command_queue(scanner: str) -> Dict[str, Any]:
    """
    Debug-only: show current queued command stream for a scanner.
    This corresponds to Redis stream: nms:cmd:{scanner}
    """
    require_whitelisted(scanner)
    key = key_cmd_stream(scanner)

    length = int(r.xlen(key))
    if length > 0:
        age_sec, oldest_id = _stream_oldest_age_sec(key)
    else:
        age_sec, oldest_id = 0, ""

    return {
        "time": local_ts(),
        "queue_type": "command",
        "scanner": scanner,
        "key": key,
        "length": length,
        "maxlen": 5000,
        "oldest_age_sec": int(age_sec),
        "oldest_id": oldest_id,
    }

@app.post("/cmd/_enqueue/{scanner}", tags=["4 Commands (Polling)"])
def cmd_enqueue(scanner: str, cmd: Cmd) -> Dict[str, Any]:
    require_whitelisted(scanner)

    # Validate execute_at format (force one style across Pi/NMS/DB)
    try:
        _ = parse_local_dt(cmd.execute_at)
    except Exception:
        raise HTTPException(status_code=400, detail=f"execute_at must be like '{local_ts()}' (format {TIME_FMT})")

    created_at = local_ts()

    if cmd.args_json_text is not None and cmd.args_json_text.strip() != "":
        raw = cmd.args_json_text.strip()
        try:
            json.loads(raw)
        except Exception:
            raise HTTPException(status_code=400, detail="args_json_text must be valid JSON text")
        args_json = raw
    else:
        args_json = json.dumps(cmd.args or {}, ensure_ascii=False)

    # Normalize execute_at to TIME_FMT (also removes any pasted noise)
    execute_at_norm = parse_local_dt(cmd.execute_at).strftime(TIME_FMT)

    # IMPORTANT CHANGE:
    # - Do NOT store cmd_id in Redis at all.
    fields = {
        "category": cmd.category,
        "action": cmd.action,
        "execute_at": execute_at_norm,
        "created_at": created_at,
        "args_json": args_json,
    }

    # Redis stream id (XID) becomes the real id. We return it as cmd_id to keep API contract stable.
    xid = r.xadd(key_cmd_stream(scanner), fields, maxlen=5000, approximate=True)

    return {
        "status": "ok",
        "scanner": scanner,
        "cmd_id": xid,              # KEEP RESPONSE FIELD NAME
        "created_at": created_at,
        "time_format": TIME_FMT,
    }

@app.get("/cmd/poll/{scanner}", tags=["4 Commands (Polling)"])
def cmd_poll(
    scanner: str,
    now: Optional[str] = None,
    limit: int = Query(5, ge=1, le=50)
) -> Dict[str, Any]:
    require_whitelisted(scanner)

    server_now_str = local_ts()
    server_now = parse_local_dt(server_now_str)

    raw = r.xrange(key_cmd_stream(scanner), count=5000)

    due_cmds = []
    skipped_not_due = 0
    skipped_expired = 0
    skipped_bad_time = 0

    for xid, fields in raw:
        exec_at_s = fields.get("execute_at", "")
        if not exec_at_s:
            skipped_bad_time += 1
            continue

        try:
            exec_at = parse_local_dt(exec_at_s)
        except Exception:
            skipped_bad_time += 1
            continue

        if exec_at > server_now:
            skipped_not_due += 1
            continue

        age_sec = int((server_now - exec_at).total_seconds())
        if age_sec > CMD_EXPIRE_SEC:
            skipped_expired += 1
            continue

        # normalize for output
        f2 = dict(fields)

        # IMPORTANT CHANGE:
        # - Provide cmd_id to Pi (contract unchanged), but cmd_id == Redis XID.
        f2["cmd_id"] = xid

        try:
            f2["execute_at"] = parse_local_dt(f2.get("execute_at", "")).strftime(TIME_FMT)
        except Exception:
            pass
        try:
            f2["created_at"] = parse_local_dt(f2.get("created_at", "")).strftime(TIME_FMT)
        except Exception:
            pass

        due_cmds.append((xid, f2))
        if len(due_cmds) >= limit:
            break

    return {
        "scanner": scanner,
        "server_now": server_now_str,
        "client_now": now,
        "cmd_expire_sec": CMD_EXPIRE_SEC,
        "time_format": TIME_FMT,
        "returned": len(due_cmds),
        "skipped": {
            "not_due": skipped_not_due,
            "expired": skipped_expired,
            "bad_time": skipped_bad_time,
        },
        "commands": due_cmds,
    }

@app.post("/cmd/ack/{scanner}", tags=["4 Commands (Polling)"])
def cmd_ack(scanner: str, ack: CmdAck) -> Dict[str, Any]:
    require_whitelisted(scanner)

    if ack.finished_at:
        try:
            finished_at = parse_local_dt(ack.finished_at).strftime(TIME_FMT)
        except Exception:
            raise HTTPException(status_code=400, detail=f"finished_at must be like '{local_ts()}' (format {TIME_FMT})")
    else:
        finished_at = local_ts()

    # Record ack (unchanged idea; cmd_id now contains xid)
    r.xadd(
        key_cmdack_stream(scanner),
        {
            "cmd_id": ack.cmd_id,    # NOTE: now this is XID
            "status": ack.status,
            "finished_at": finished_at,
            "detail": ack.detail or "",
        },
        maxlen=20000,
        approximate=True,
    )

    # IMPORTANT CHANGE (Option A):
    # - Delete the command from the command stream using cmd_id as the Redis XID.
    deleted = int(r.xdel(key_cmd_stream(scanner), ack.cmd_id))

    return {
        "status": "ok",
        "scanner": scanner,
        "cmd_id": ack.cmd_id,       # KEEP CONTRACT
        "deleted": deleted,         # helpful debug signal
        "finished_at": finished_at,
        "time_format": TIME_FMT,
    }

# Script loader
class ScriptItem(BaseModel):
    scanner: str
    t_offset_sec: int
    category: str = "scan"
    action: str
    args: Dict[str, Any] = Field(default_factory=dict)

class ScriptLoad(BaseModel):
    t0: str  # MUST be TIME_FMT
    items: List[ScriptItem]

@app.post("/cmd/_load_script", tags=["4 Commands (Polling)"])
def cmd_load_script(script: ScriptLoad) -> Dict[str, Any]:
    try:
        t0_dt = parse_local_dt(script.t0)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid t0; expected like '{local_ts()}' (format {TIME_FMT})")

    added = 0
    skipped_not_whitelisted = 0

    for it in script.items:
        if not r.hexists(KEY_WHITELIST_SCANNER_META, (it.scanner or "").strip()):
            skipped_not_whitelisted += 1
            continue

        created_at = local_ts()
        execute_at = (t0_dt + timedelta(seconds=int(it.t_offset_sec))).strftime(TIME_FMT)

        r.xadd(
            key_cmd_stream(it.scanner),
            {
                "category": it.category,
                "action": it.action,
                "execute_at": execute_at,
                "created_at": created_at,
                "args_json": json.dumps(it.args or {}, ensure_ascii=False),
            },
            maxlen=5000,
            approximate=True,
        )
        added += 1

    return {
        "status": "ok",
        "added": added,
        "skipped_not_whitelisted": skipped_not_whitelisted,
        "t0": t0_dt.strftime(TIME_FMT),
        "time_format": TIME_FMT,
    }

class CmdLoadCSVReq(BaseModel):
    t0: str = Field(..., description=f"Absolute local time, format: {TIME_FMT}")
    csv_text: str = Field(..., description="CSV rows with columns: scanner,t_offset_sec,category,action,args_json")

@app.post("/cmd/_load_csv", tags=["4 Commands (Polling)"])
def cmd_load_csv(req: CmdLoadCSVReq) -> Dict[str, Any]:
    try:
        t0_dt = parse_local_dt(req.t0)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid t0; expected like '{local_ts()}' (format {TIME_FMT})")

    f = io.StringIO(req.csv_text)
    reader = csv.DictReader(f)
    required_cols = {"scanner", "t_offset_sec", "category", "action", "args_json"}
    if not required_cols.issubset(set(reader.fieldnames or [])):
        raise HTTPException(status_code=400, detail=f"CSV must have columns: {sorted(list(required_cols))}")

    added = 0
    skipped_not_whitelisted = 0
    bad_rows = 0

    for row in reader:
        scanner = (row.get("scanner") or "").strip()
        if not scanner:
            bad_rows += 1
            continue
        if not r.hexists(KEY_WHITELIST_SCANNER_META, scanner):
            skipped_not_whitelisted += 1
            continue

        try:
            offset = int((row.get("t_offset_sec") or "0").strip())
        except Exception:
            bad_rows += 1
            continue

        category = (row.get("category") or "scan").strip() or "scan"
        action = (row.get("action") or "").strip()
        if not action:
            bad_rows += 1
            continue

        args_s = (row.get("args_json") or "").strip()
        if args_s:
            try:
                args = json.loads(args_s)
            except Exception:
                bad_rows += 1
                continue
        else:
            args = {}

        execute_at = (t0_dt + timedelta(seconds=offset)).strftime(TIME_FMT)
        created_at = local_ts()

        r.xadd(
            key_cmd_stream(scanner),
            {
                "category": category,
                "action": action,
                "execute_at": execute_at,
                "created_at": created_at,
                "args_json": json.dumps(args, ensure_ascii=False),
            },
            maxlen=5000,
            approximate=True,
        )
        added += 1

    return {
        "status": "ok",
        "t0": t0_dt.strftime(TIME_FMT),
        "time_format": TIME_FMT,
        "added": added,
        "skipped_not_whitelisted": skipped_not_whitelisted,
        "bad_rows": bad_rows,
    }

# =============================================================================
# 5) Northbound (NMS -> Web Server)
# =============================================================================

def _post_upload_scan_batch(items: List[Dict[str, Any]]) -> Tuple[bool, str, int, int]:
    """
    Returns (ok, detail, accepted, rejected).
    We treat HTTP 2xx + JSON status=ok as ok.
    """
    payload = {
        "nms_id": NMS_ID,
        "time": local_ts(),
        "time_format": TIME_FMT,
        "items": items,
    }
    try:
        resp = requests.post(WEB_NMS_UPLOAD_URL, json=payload, headers=_web_headers(), timeout=10)
        resp.raise_for_status()
        j = resp.json()
        if isinstance(j, dict) and j.get("status") == "ok":
            return True, "ok", int(j.get("accepted") or 0), int(j.get("rejected") or 0)
        return False, f"web returned {j}", 0, 0
    except Exception as e:
        return False, f"post failed: {e}", 0, 0

def _northbound_upload_once() -> Dict[str, Any]:
    """
    One cycle: build a <=100KB batch from queued uplink streams.
    Delete only what was actually sent (and only if web accepted).
    Always returns quickly; never blocks future cycles.
    """
    robots = sorted(list(r.smembers(KEY_REGISTRY)))
    selected_items: List[Dict[str, Any]] = []
    selected_ids_by_robot: Dict[str, List[str]] = {}

    bad_json_deleted = 0
    oversize_deleted = 0

    # We'll build the final envelope size-aware.
    # Start with empty items envelope cost (approx).
    envelope_base = {"nms_id": NMS_ID, "time": local_ts(), "time_format": TIME_FMT, "items": []}
    base_bytes = _json_bytes(envelope_base)
    budget = max(0, int(UPLOAD_BATCH_MAX_BYTES - base_bytes))

    for robot in robots:
        stream_key = key_uplink_stream(robot)
        if budget <= 0:
            break

        entries = r.xrange(stream_key, count=5000)  # oldest first
        for xid, fields in entries:
            if budget <= 0:
                break

            payload_text = fields.get("payload_text", "")
            received_at = fields.get("received_at", "") or local_ts()

            lst = _safe_parse_entries_list(payload_text)
            if lst is None:
                # Bad payload: delete it so it won't clog forever
                try:
                    r.xdel(stream_key, xid)
                except Exception:
                    pass
                bad_json_deleted += 1
                continue

            item = {
                "scanner": robot,
                "time": received_at,     # injected by NMS
                "iface": "",             # intentionally blank
                "entries": lst,          # raw list (no processing)
                "time_format": TIME_FMT,
            }

            item_bytes = _json_bytes(item)
            if item_bytes > UPLOAD_BATCH_MAX_BYTES:
                # Single item can never fit -> drop it
                try:
                    r.xdel(stream_key, xid)
                except Exception:
                    pass
                oversize_deleted += 1
                continue

            if item_bytes > budget:
                # Not enough room this cycle; keep it queued for next cycle
                break

            selected_items.append(item)
            selected_ids_by_robot.setdefault(robot, []).append(xid)
            budget -= item_bytes

    ok, detail, accepted, rejected = _post_upload_scan_batch(selected_items)

    if ok:
        # delete only selected ids
        deleted_total = 0
        for robot, ids in selected_ids_by_robot.items():
            if not ids:
                continue
            try:
                deleted_total += int(r.xdel(key_uplink_stream(robot), *ids))
                r.hset(KEY_NB_LAST_UPLOAD, robot, local_ts())
                r.hset(KEY_NB_LAST_RESULT, robot, f"ok sent={len(ids)}")
            except Exception:
                pass
        return {
            "status": "ok",
            "sent_items": len(selected_items),
            "deleted": deleted_total,
            "bad_json_deleted": bad_json_deleted,
            "oversize_deleted": oversize_deleted,
            "web_detail": detail,
            "accepted": accepted,
            "rejected": rejected,
            "time": local_ts(),
        }

    # failed post -> do not delete queued items
    return {
        "status": "fail",
        "sent_items": len(selected_items),
        "bad_json_deleted": bad_json_deleted,
        "oversize_deleted": oversize_deleted,
        "error": detail,
        "time": local_ts(),
    }

def _build_status_snapshot() -> Dict[str, Any]:
    robots = sorted(list(r.smembers(KEY_REGISTRY)))
    robot_states: List[Dict[str, Any]] = []

    for rid in robots:
        meta = r.hgetall(key_scanner_meta(rid)) or {}
        last_seen = meta.get("last_seen", "")

        robot_states.append({
            "robot_id": rid,
            "last_seen": last_seen,
            "mode": "unknown",
            "stream_state": "unknown",
            "stream_path": rid,
            "location": {"mode": "unknown", "x": 0.0, "y": 0.0},
            "detail": "",
        })

    return {
        "nms_id": NMS_ID,
        "time_local": local_ts(),
        "time_format": TIME_FMT,
        "experiment": {
            "state": "idle",
            "session_id": None,
            "scenario_name": None,
            "started_at": None,
            "elapsed_sec": 0,
            "next_scheduled_at": None,
            "idle_duration_sec": 0,
        },
        "nms_status": {
            "online": True,
            "detail": "",
            "uplink_queue_total": 0,
            "command_queue_total": 0,
            "last_uplink_ok": True,
            "last_uplink_time": None,
        },
        "aps": [],
        "robots": robot_states,
    }

def _post_report_status(snapshot: Dict[str, Any]) -> Tuple[bool, str, List[Dict[str, Any]]]:
    try:
        resp = requests.post(WEB_NMS_STATUS_URL, json=snapshot, headers=_web_headers(), timeout=10)
        resp.raise_for_status()
        j = resp.json()
        if isinstance(j, dict) and j.get("status") == "ok":
            cmds = j.get("commands") or []
            if not isinstance(cmds, list):
                cmds = []
            return True, "ok", cmds
        return False, f"web returned {j}", []
    except Exception as e:
        return False, f"post failed: {e}", []

def _northbound_status_once() -> Dict[str, Any]:
    snap = _build_status_snapshot()
    ok, detail, cmds = _post_report_status(snap)
    # For now: log-only (no command execution yet)
    try:
        r.set(KEY_NB_LAST_STATUS, f"{local_ts()} ok={ok} cmds={len(cmds)} detail={detail[:120]}")
    except Exception:
        pass
    return {"ok": ok, "detail": detail, "commands_count": len(cmds), "time": local_ts()}

def _xid_to_ms(xid: str) -> int:
    return int(xid.split("-", 1)[0])

def _stream_oldest_age_sec(stream_key: str) -> Tuple[int, str]:
    first = r.xrange(stream_key, count=1)
    if not first:
        return 0, ""
    oldest_id, _ = first[0]
    now_ms = int(datetime.now().timestamp() * 1000)
    age_sec = max(0, (now_ms - _xid_to_ms(oldest_id)) // 1000)
    return int(age_sec), oldest_id

# =============================================================================
# 9) Admin
# =============================================================================
class ResetReq(BaseModel):
    confirm: str = Field(..., description="Must be EXACTLY 'RESET' to proceed.")
    keep_whitelist: bool = True
    keep_bundles: bool = True  # keep bundle metadata keys (and never touch bundle files on disk)

@app.post("/admin/_reset", tags=["9 Admin"])
def admin_reset(req: ResetReq) -> Dict[str, Any]:
    """
    Admin-only: delete Redis keys under KEY_PREFIX, with optional keeps.
    - Does NOT touch bundle ZIP files on disk.
    - By default keeps whitelist + bundle index
    """
    if req.confirm != "RESET":
        raise HTTPException(status_code=400, detail="confirm must be 'RESET'")

    keep = set()
    if req.keep_whitelist:
        keep.add(KEY_WHITELIST_SCANNER_META)
    if req.keep_bundles:
        keep.add(KEY_BUNDLE_INDEX)

    deleted = 0
    scanned = 0
    cursor = 0
    pattern = f"{KEY_PREFIX}*"

    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
        scanned += len(keys)
        to_del = [k for k in keys if k not in keep]
        if to_del:
            deleted += int(r.delete(*to_del))
        if int(cursor) == 0:
            break

    return {
        "status": "ok",
        "time": local_ts(),
        "prefix": KEY_PREFIX,
        "scanned_keys_count": int(scanned),
        "deleted_keys_count": int(deleted),
        "kept": sorted(list(keep)),
        "bundle_dir_untouched": str(BUNDLE_DIR),
        "note": "Redis keys removed; bundle ZIP files on disk are untouched.",
    }

# =============================================================================
# Auto-flush background task
# =============================================================================
async def _northbound_loop():
    while True:
        try:
            _northbound_upload_once()
        except Exception:
            pass
        await asyncio.sleep(NORTHBOUND_EVERY_SEC)

async def _status_loop():
    while True:
        try:
            _northbound_status_once()
        except Exception:
            pass
        await asyncio.sleep(NORTHBOUND_EVERY_SEC)

@app.on_event("startup")
async def _startup():
    asyncio.create_task(_northbound_loop())
    asyncio.create_task(_status_loop())
    

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
