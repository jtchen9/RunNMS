from fastapi import Body, FastAPI, Request, HTTPException, Query, File, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import redis
from datetime import datetime, timedelta
import hashlib
import json
import uvicorn
import requests
import csv
import io
import asyncio
import base64

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

KEY_NB_LAST_CMDS: str = f"{KEY_PREFIX}nb:last_cmds"          # STRING (json list)
KEY_NB_LAST_CMDS_TIME: str = f"{KEY_PREFIX}nb:last_cmds_time" # STRING (time)
KEY_NB_LAST_CMDS_ERR: str = f"{KEY_PREFIX}nb:last_cmds_err"   # STRING (error summary)

# Desired state (intent) from web
KEY_INTENT_VIDEO: str = f"{KEY_PREFIX}intent:video"          # HASH(robot -> "on"/"off")
KEY_INTENT_VIDEO_TS: str = f"{KEY_PREFIX}intent:video_ts"    # HASH(robot -> time)

# Last Pi-reported state observed by NMS (idempotency guard uses Pi truth)
KEY_APPLIED_VIDEO: str = f"{KEY_PREFIX}applied:video"        # HASH(robot -> "on"/"off"/"unknown")
KEY_APPLIED_VIDEO_TS: str = f"{KEY_PREFIX}applied:video_ts"  # HASH(robot -> time)

# -----------------------------------------------------------------------------#
# 6) AP performance upload
# -----------------------------------------------------------------------------#
AP_UPLINK_MAXLEN: int = 20000

def key_ap_uplink_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}ap_uplink:{scanner}"   # STREAM(AP performance payloads)

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
    if not bundle_id or not (bundle_id.startswith("apBundle") or bundle_id.startswith("robotBundle")):
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

        meta["scanner"] = meta.get("scanner") or scanner

        # Do not dump full base64 blob in listing API
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
        "time": local_ts(),
        "count": len(items),
        "items": items,
        "key": KEY_WHITELIST_SCANNER_META,
        "schema_version": int(WHITELIST_SCHEMA_VERSION),
    }

# -----------------------------------------------------------------------------
# Operator-only: whitelist meta for one scanner (metadata style)
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

        meta["scanner"] = meta.get("scanner") or scanner

        # Do not dump full base64 blob in listing API
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
        "time": local_ts(),
        "count": len(items),
        "items": items,
        "key": KEY_WHITELIST_SCANNER_META,
        "schema_version": int(WHITELIST_SCHEMA_VERSION),
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
# Supports partial update:
# - scanner is required
# - mac / llm_weblink / tailscaled_state_b64 / comment are optional
# - omitted fields keep previous values
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
        if not scanner:
            continue

        old = whitelist_meta_get(scanner)

        old_mac = normalize_mac(old.get("mac", "")) if old else ""
        old_llm = (old.get("llm_weblink", "") or "").strip() if old else ""
        old_tailscaled_state_b64 = (old.get("tailscaled_state_b64", "") or "").strip() if old else ""
        old_comment = (old.get("comment", "") or "").strip() if old else ""

        # Partial update rules:
        # - None => keep old value
        # - blank string => also keep old value (prevents accidental wipe)
        if it.mac is None or str(it.mac).strip() == "":
            new_mac = old_mac
        else:
            new_mac = normalize_mac(it.mac)

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

        # Only for brand-new entries with no old llm value
        if not new_llm:
            new_llm = DEFAULT_LLM_WEBLINK

        meta = {
            "schema_version": int(WHITELIST_SCHEMA_VERSION),
            "scanner": scanner,
            "mac": new_mac,
            "llm_weblink": new_llm,
            "tailscaled_state_b64": new_tailscaled_state_b64,
            "comment": new_comment,
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
# Pi/AP-facing: register by mac -> returns {scanner, llm_weblink, tailscaled_state_b64}
# -----------------------------------------------------------------------------
@app.post("/registry/register", tags=["1 Registry & Whitelist"])
def register(req: RegisterReq) -> Dict[str, Any]:
    mac = normalize_mac(req.mac)
    scanner = find_scanner_by_mac(mac)
    if not scanner:
        raise HTTPException(status_code=403, detail=f"MAC '{mac}' not in whitelist")

    wmeta = whitelist_meta_get(scanner)
    llm = (wmeta.get("llm_weblink") or "").strip() or DEFAULT_LLM_WEBLINK
    tailscaled_state_b64 = (wmeta.get("tailscaled_state_b64") or "").strip()

    now = local_ts()
    r.sadd(KEY_REGISTRY, scanner)

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

    r.hset(key_scanner_meta(scanner), mapping=updates)

    return {
        "scanner": scanner,
        "llm_weblink": llm,
        "tailscaled_state_b64": tailscaled_state_b64,
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

@app.post("/registry/_tailscaled_state_upload/{scanner}", tags=["1 Registry & Whitelist"])
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

    # must already exist in whitelist
    if not r.hexists(KEY_WHITELIST_SCANNER_META, scanner):
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

    now = local_ts()

    meta = {
        "schema_version": int(old.get("schema_version") or WHITELIST_SCHEMA_VERSION),
        "scanner": scanner,
        "mac": normalize_mac(old.get("mac", "")),
        "llm_weblink": (old.get("llm_weblink") or "").strip() or DEFAULT_LLM_WEBLINK,
        "tailscaled_state_b64": b64,
        "comment": (old.get("comment") or "").strip(),
        "updated_at": now,
    }

    r.hset(KEY_WHITELIST_SCANNER_META, scanner, json.dumps(meta, ensure_ascii=False))

    return {
        "status": "ok",
        "scanner": scanner,
        "filename": state_file.filename or "",
        "bytes": len(data),
        "tailscaled_state_present": True,
        "tailscaled_state_b64_size": len(b64),
        "time": now,
    }

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

def _cmd_enqueue_core(scanner: str, cmd: "Cmd") -> Dict[str, Any]:
    """
    Single source of truth for enqueuing commands to Redis.
    Used by:
      - /cmd/_enqueue/{scanner}
      - northbound bridge (web -> NMS command stream)
    """
    require_whitelisted(scanner)

    # Validate execute_at format (force one style across Pi/NMS/DB)
    try:
        _ = parse_local_dt(cmd.execute_at)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"execute_at must be like '{local_ts()}' (format {TIME_FMT})"
        )

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

    execute_at_norm = parse_local_dt(cmd.execute_at).strftime(TIME_FMT)

    fields = {
        "category": cmd.category,
        "action": cmd.action,
        "execute_at": execute_at_norm,
        "created_at": created_at,
        "args_json": args_json,
    }

    xid = r.xadd(key_cmd_stream(scanner), fields, maxlen=5000, approximate=True)

    return {
        "status": "ok",
        "scanner": scanner,
        "cmd_id": xid,              # KEEP RESPONSE FIELD NAME
        "created_at": created_at,
        "time_format": TIME_FMT,
    }

@app.post("/cmd/_enqueue/{scanner}", tags=["4 Commands (Polling)"])
def cmd_enqueue(scanner: str, cmd: Cmd) -> Dict[str, Any]:
    return _cmd_enqueue_core(scanner, cmd)

@app.get("/cmd/poll/{scanner}", tags=["4 Commands (Polling)"])
def cmd_poll(
    scanner: str,
    request: Request,
    now: Optional[str] = None,
    limit: int = Query(5, ge=1, le=50),

    # robot-side live status
    av_streaming: Optional[int] = Query(None, description="1 if AV stream currently running, else 0"),
    av_detail: Optional[str] = Query(None, description="Optional info like 'pid=1234'"),
    boot_id: Optional[str] = Query(None, description="Unique per boot; changes after reboot"),
) -> Dict[str, Any]:
    require_whitelisted(scanner)

    server_now_str = local_ts()
    try:
        meta_updates: Dict[str, str] = {
            "last_seen": server_now_str,
            "last_poll": server_now_str,
        }
        if request.client and request.client.host:
            meta_updates["ip"] = request.client.host

        if av_streaming is not None:
            meta_updates["av_streaming"] = "1" if int(av_streaming) == 1 else "0"
            meta_updates["av_updated_at"] = server_now_str

        if av_detail is not None:
            meta_updates["av_detail"] = (av_detail or "")[:200]

        if boot_id is not None:
            meta_updates["boot_id"] = (boot_id or "")[:80]

        r.hset(key_scanner_meta(scanner), mapping=meta_updates)
        r.sadd(KEY_REGISTRY, scanner)

        # robot AV truth comes from Pi report
        if av_streaming is not None:
            applied = "on" if int(av_streaming) == 1 else "off"
            r.hset(KEY_APPLIED_VIDEO, scanner, applied)
            r.hset(KEY_APPLIED_VIDEO_TS, scanner, server_now_str)

    except Exception:
        pass

    collected = _collect_due_commands(scanner=scanner, limit=limit, server_now_str=server_now_str)

    return {
        "scanner": scanner,
        "server_now": server_now_str,
        "client_now": now,
        "cmd_expire_sec": CMD_EXPIRE_SEC,
        "time_format": TIME_FMT,
        "returned": len(collected["commands"]),
        "skipped": collected["skipped"],
        "commands": collected["commands"],
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
    scanners = sorted(list(r.smembers(KEY_REGISTRY)))
    robot_states: List[Dict[str, Any]] = []
    ap_states: List[Dict[str, Any]] = []

    for rid in scanners:
        meta = r.hgetall(key_scanner_meta(rid)) or {}
        last_seen = meta.get("last_seen", "")
        device_type = (meta.get("device_type") or "robot").strip().lower()

        if device_type == "ap":
            try:
                ssids = json.loads(meta.get("ssids_json", "[]") or "[]")
                if not isinstance(ssids, list):
                    ssids = []
            except Exception:
                ssids = []

            channel_val = meta.get("channel", "")
            antenna_val = meta.get("antenna_count", "")

            ap_states.append({
                "ap_id": rid,
                "last_seen": last_seen,
                "mac": meta.get("mac", ""),
                "ip": meta.get("ip", ""),
                "ssids": ssids,
                "band": meta.get("band", ""),
                "channel": int(channel_val) if str(channel_val).isdigit() else None,
                "antenna_count": int(antenna_val) if str(antenna_val).isdigit() else None,
                "traffic_enabled": meta.get("traffic_enabled", ""),
                "detail": "",
            })

        else:
            av_streaming = meta.get("av_streaming", "")
            if av_streaming == "1":
                stream_state = "on"
            elif av_streaming == "0":
                stream_state = "off"
            else:
                stream_state = "unknown"

            robot_states.append({
                "robot_id": rid,
                "last_seen": last_seen,
                "mode": "unknown",
                "stream_state": stream_state,
                "stream_path": rid,
                "location": {"mode": "unknown", "x": 0.0, "y": 0.0},
                "detail": meta.get("av_detail", "")[:200],
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
        "aps": ap_states,
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

    ok = False
    detail = ""
    cmds: List[Dict[str, Any]] = []

    try:
        ok, detail, cmds = _post_report_status(snap)
    except Exception as e:
        ok = False
        detail = f"post_status exception: {e}"
        cmds = []

    now = local_ts()

    # Store last cmds from web for inspection/debug (your request)
    try:
        r.set(KEY_NB_LAST_CMDS, json.dumps(cmds, ensure_ascii=False))
    except Exception:
        pass

    intent_updates = 0
    enq = 0
    noop = 0
    err = 0

    # Apply web cmds as intents (idempotent)
    try:
        intent_updates, enq, noop = _apply_web_cmds_as_intents(cmds)
    except Exception:
        err += 1

    # Status line (easy to read in redis-cli)
    try:
        r.set(
            KEY_NB_LAST_STATUS,
            f"{now} ok={ok} cmds={len(cmds)} intent={intent_updates} enq={enq} noop={noop} err={err} detail={(detail or '')[:120]}"
        )
    except Exception:
        pass

    return {
        "ok": ok,
        "detail": detail,
        "time": now,
        "commands_count": len(cmds),
        "intent_updates": intent_updates,
        "enq": enq,
        "noop": noop,
        "err": err,
    }

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

def _apply_web_cmds_as_intents(cmds: List[Dict[str, Any]]) -> Tuple[int, int, int]:
    """
    Convert web cmds -> desired intents.
    Returns (intent_updates, enqueued_to_pi, skipped_as_noop)
    """
    intent_updates = 0
    enq = 0
    noop = 0
    now = local_ts()

    for c in (cmds or []):
        target = str(c.get("target") or "").strip()
        action = str(c.get("action") or "").strip()
        category = str(c.get("category") or "").strip() or "av"
       
        if not target or not action:
            noop += 1
            continue

        # target must be whitelisted
        if not r.hexists(KEY_WHITELIST_SCANNER_META, target):
            noop += 1
            continue

        # Only handle the intents we recognize (expand later)
        if action == "av.stream.start":
            desired = "on"
        elif action == "av.stream.stop":
            desired = "off"
        else:
            noop += 1
            continue

        # 1) Record intent (desired state) from web (idempotent and OK to overwrite)
        try:
            r.hset(KEY_INTENT_VIDEO, target, desired)
            r.hset(KEY_INTENT_VIDEO_TS, target, now)
            intent_updates += 1
        except Exception:
            # If Redis hiccups, still attempt noop/enq decisions best-effort
            pass

        # 2) Determine "applied" based on Pi truth
        # Prefer KEY_APPLIED_VIDEO (written by /cmd/poll), fallback to scanner meta.
        applied = (r.hget(KEY_APPLIED_VIDEO, target) or "").strip()
        if not applied:
            meta = r.hgetall(key_scanner_meta(target)) or {}
            if meta.get("av_streaming") == "1":
                applied = "on"
            elif meta.get("av_streaming") == "0":
                applied = "off"

        # 3) If Pi already in desired state -> noop
        if applied == desired:
            noop += 1
            continue

        # 4) Need to apply: enqueue ONE Pi command (execute now, args empty)
        cmd_fields = {
            "category": category,          # "av"
            "action": action,              # av.stream.start/stop
            "execute_at": now,
            "created_at": now,
            "args_json": "{}",
            "web_cmd_id": str(c.get("cmd_id") or ""),  # trace only
        }
        r.xadd(key_cmd_stream(target), cmd_fields, maxlen=5000, approximate=True)
        enq += 1

        # IMPORTANT:
        # Do NOT mark KEY_APPLIED_VIDEO here.
        # It must only reflect Pi truth (updated by /cmd/poll).
        # This fixes the reboot corner case automatically.

    return intent_updates, enq, noop

# =============================================================================
# 6) AP
# =============================================================================
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


def _collect_due_commands(scanner: str, limit: int, server_now_str: str) -> Dict[str, Any]:
    """
    Shared helper for robot /cmd/poll and AP /ap/poll.
    Returns due commands in the same envelope shape already used by robots.
    """
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

        f2 = dict(fields)
        f2["cmd_id"] = xid  # keep robot/AP contract: cmd_id == Redis XID

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
        "commands": due_cmds,
        "skipped": {
            "not_due": skipped_not_due,
            "expired": skipped_expired,
            "bad_time": skipped_bad_time,
        },
    }

@app.post("/ap/poll/{scanner}", tags=["6 AP Control (Polling)"])
def ap_poll(
    scanner: str,
    req: APPollReq,
    request: Request,
    limit: int = Query(5, ge=1, le=50),
) -> Dict[str, Any]:
    require_whitelisted(scanner)

    try:
        _ = parse_local_dt(req.time)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"time must be like '{local_ts()}' (format {TIME_FMT})"
        )

    server_now_str = local_ts()

    ssids_json = json.dumps(req.status.ssids or [], ensure_ascii=False)

    meta_updates: Dict[str, str] = {
        "device_type": "ap",
        "last_seen": server_now_str,
        "last_ap_poll": server_now_str,
        "status_updated_at": server_now_str,
        "mac": normalize_mac(req.status.mac),
        "ip": (req.status.ip or (request.client.host if request.client and request.client.host else "") or ""),
        "ssids_json": ssids_json,
        "band": str(req.status.band or ""),
        "channel": str(req.status.channel if req.status.channel is not None else ""),
        "antenna_count": str(req.status.antenna_count if req.status.antenna_count is not None else ""),
    }

    try:
        r.hset(key_scanner_meta(scanner), mapping=meta_updates)
        r.sadd(KEY_REGISTRY, scanner)
    except Exception:
        pass

    collected = _collect_due_commands(scanner=scanner, limit=limit, server_now_str=server_now_str)

    return {
        "scanner": scanner,
        "server_now": server_now_str,
        "time_format": TIME_FMT,
        "returned": len(collected["commands"]),
        "skipped": collected["skipped"],
        "commands": collected["commands"],
    }

@app.post("/ap/traffic/{scanner}", tags=["7 AP Performance Upload"])
def ap_traffic(scanner: str, req: APTrafficReq) -> Dict[str, Any]:
    require_whitelisted(scanner)

    try:
        ts0 = parse_local_dt(req.time_start).strftime(TIME_FMT)
        ts1 = parse_local_dt(req.time_end).strftime(TIME_FMT)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"time_start/time_end must be like '{local_ts()}' (format {TIME_FMT})"
        )

    payload = {
        "time_start": ts0,
        "time_end": ts1,
        "associations": [x.model_dump() for x in req.associations],
        "records": [x.model_dump() for x in req.records],
    }
    payload_text = json.dumps(payload, ensure_ascii=False)
    received_at = local_ts()

    try:
        r.xadd(
            key_ap_uplink_stream(scanner),
            {
                "received_at": received_at,
                "time_start": ts0,
                "time_end": ts1,
                "assoc_count": str(len(req.associations)),
                "record_count": str(len(req.records)),
                "payload_text": payload_text,
            },
            maxlen=AP_UPLINK_MAXLEN,
            approximate=True,
        )

        r.hset(
            key_scanner_meta(scanner),
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
        r.sadd(KEY_REGISTRY, scanner)

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
        "queued_in": key_ap_uplink_stream(scanner),
    }

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
