from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from pathlib import Path
from typing import Optional, List, Dict, Any
import os
import base64
import redis
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import hashlib
import uuid
import json
import uvicorn

load_dotenv()

# =============================================================================
# Config (.env)
# =============================================================================
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Web (cloud) uplink endpoint (provided by Web chatroom later)
WEB_POST_URL = os.getenv("WEB_POST_URL", "")  # e.g. https://your-web-host/ingest
WEB_API_KEY = os.getenv("WEB_API_KEY", "")    # optional

# Queue/retention controls
UPLINK_MAXLEN = int(os.getenv("UPLINK_MAXLEN", "50000"))   # per-scanner queue trim
UPLOAD_ENABLED = os.getenv("UPLOAD_ENABLED", "1") == "1"   # allow turning off uploader during dev

# Keyspace prefix
KEY_PREFIX = os.getenv("KEY_PREFIX", "nms:")

# Bundle directory (Windows-friendly)
BUNDLE_DIR = Path(os.getenv("BUNDLE_DIR", r"D:\_RunNMS\bundles"))
BUNDLE_DIR.mkdir(parents=True, exist_ok=True)

# Redis
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# App
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
# Redis key conventions (single source of truth)
# =============================================================================
KEY_WHITELIST = f"{KEY_PREFIX}registry:whitelist"      # SET(scanner)
KEY_REGISTRY  = f"{KEY_PREFIX}registry:scanners"       # SET(scanner)
KEY_BUNDLE_CURRENT = f"{KEY_PREFIX}bundle:current"     # HASH(name, version, path, sha256)

def key_scanner_meta(scanner: str) -> str:
    return f"{KEY_PREFIX}scanner:{scanner}:meta"       # HASH(...)

def key_uplink_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}uplink:{scanner}"             # STREAM(opaque payloads)

def key_cmd_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}cmd:{scanner}"                # STREAM(commands)

def key_cmdack_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}cmdack:{scanner}"             # STREAM(acks)

# =============================================================================
# Utilities
# =============================================================================
def utc_iso() -> str:
    """Return current UTC time as ISO-8601 string with timezone offset."""
    return datetime.now(timezone.utc).isoformat()

def sha256_file(p: Path) -> str:
    """Compute SHA256 hex digest of a file (streaming, low memory)."""
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def get_bundle_current() -> Dict[str, Any]:
    """
    Get the currently configured bootstrap bundle metadata from Redis.

    Redis HASH: nms:bundle:current
      - name:    filename, e.g. dummy_scanner_bundle.zip
      - version: semantic version string, e.g. v1
      - path:    absolute path to file (Windows), e.g. D:\\_RunNMS\\bundles\\dummy.zip
      - sha256:  cached digest for speed

    If missing, defaults to:
      name=dummy_scanner_bundle.zip, version=v1, path=BUNDLE_DIR/name

    Returns dict with:
      {name, version, path, sha256, exists}
    """
    meta = r.hgetall(KEY_BUNDLE_CURRENT) or {}

    name = meta.get("name", "dummy_scanner_bundle.zip")
    version = meta.get("version", "v1")
    path_str = meta.get("path", str(BUNDLE_DIR / name))

    path = Path(path_str)
    if not path.exists():
        return {"name": name, "version": version, "path": str(path), "exists": False}

    sha = meta.get("sha256")
    if not sha:
        sha = sha256_file(path)
        r.hset(KEY_BUNDLE_CURRENT, mapping={"name": name, "version": version, "path": str(path), "sha256": sha})

    return {"name": name, "version": version, "path": str(path), "sha256": sha, "exists": True}

def require_whitelisted(scanner: str) -> None:
    """403 if scanner not in whitelist."""
    if not r.sismember(KEY_WHITELIST, scanner):
        raise HTTPException(status_code=403, detail=f"Scanner '{scanner}' not in whitelist")

# =============================================================================
# 0) Health (first thing operator checks)
# =============================================================================
@app.get("/health", tags=["0 Health"])
def health() -> Dict[str, Any]:
    """
    Health check for NMS service and Redis connectivity.

    Returns:
      - redis_ok: whether Redis PING succeeded
      - upload_enabled: whether uplink forwarding is enabled (dev flag)
      - web_post_url_configured: whether WEB_POST_URL is configured
      - time_utc: current server time in UTC
    """
    try:
        r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {
        "status": "ok",
        "redis_ok": redis_ok,
        "upload_enabled": UPLOAD_ENABLED,
        "web_post_url_configured": bool(WEB_POST_URL),
        "time_utc": utc_iso(),
    }

# =============================================================================
# 1) Registry & Whitelist (new scanner must be whitelisted first)
# =============================================================================
class WhitelistAddReq(BaseModel):
    """Admin request to add scanners to whitelist."""
    scanners: List[str] = Field(default_factory=list)

@app.post("/registry/whitelist/add", tags=["1 Registry & Whitelist"])
def whitelist_add(req: WhitelistAddReq) -> Dict[str, Any]:
    """
    Add scanners to whitelist (admin helper).

    Redis:
      SADD nms:registry:whitelist <scanner...>
    """
    if not req.scanners:
        return {"status": "ok", "added": 0, "whitelist_size": r.scard(KEY_WHITELIST)}
    added = r.sadd(KEY_WHITELIST, *req.scanners)
    return {"status": "ok", "added": int(added), "whitelist_size": r.scard(KEY_WHITELIST)}

@app.get("/registry/whitelist", tags=["1 Registry & Whitelist"])
def whitelist_list() -> Dict[str, Any]:
    """
    List current whitelist members.

    Redis:
      SMEMBERS nms:registry:whitelist
    """
    members = sorted(list(r.smembers(KEY_WHITELIST)))
    return {"count": len(members), "scanners": members}

class RegisterReq(BaseModel):
    """
    Scanner self-registration payload.
    The Pi can call this at boot/login to report identity and capabilities.
    """
    scanner: str
    ip: Optional[str] = None
    scanner_version: Optional[str] = None
    capabilities: Optional[str] = None  # string for now (can be JSON later)

@app.post("/registry/register", tags=["1 Registry & Whitelist"])
def register(req: RegisterReq) -> Dict[str, Any]:
    """
    Register a scanner in NMS registry (whitelist required).

    Redis:
      - SADD nms:registry:scanners <scanner>
      - HSET nms:scanner:{scanner}:meta fields (ip, version, capabilities, last_seen_utc)
    """
    require_whitelisted(req.scanner)
    now = utc_iso()

    r.sadd(KEY_REGISTRY, req.scanner)

    updates: Dict[str, str] = {"last_seen_utc": now}
    if req.ip:
        updates["ip"] = req.ip
    if req.scanner_version:
        updates["scanner_version"] = req.scanner_version
    if req.capabilities:
        updates["capabilities"] = req.capabilities

    r.hset(key_scanner_meta(req.scanner), mapping=updates)
    return {"status": "ok", "scanner": req.scanner, "updated": updates}

@app.get("/registry/scanners", tags=["1 Registry & Whitelist"])
def list_scanners() -> Dict[str, Any]:
    """
    List all scanners that have registered with NMS.

    Redis:
      - SMEMBERS nms:registry:scanners
      - HGETALL nms:scanner:{scanner}:meta
    """
    scanners = sorted(list(r.smembers(KEY_REGISTRY)))
    out = [{"scanner": s, "meta": r.hgetall(key_scanner_meta(s))} for s in scanners]
    return {"count": len(out), "scanners": out}

# =============================================================================
# 2) Bootstrap (init/update bundle for new-comer Pi)
# =============================================================================
class BootstrapReport(BaseModel):
    """Scanner reports what bundle version it has installed."""
    installed_version: str

@app.get("/bootstrap/check/{scanner}", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_check(scanner: str) -> Dict[str, Any]:
    """
    Decide whether a scanner should init/update/none based on installed_version vs desired bundle version.

    Logic:
      - if installed_version missing -> action=init
      - if installed_version != desired -> action=update
      - else -> action=none

    Returns bundle metadata and download URL.
    """
    require_whitelisted(scanner)

    bundle = get_bundle_current()
    if not bundle.get("exists"):
        raise HTTPException(
            status_code=500,
            detail=f"Bundle file not found: {bundle.get('path')}. Put it under BUNDLE_DIR or update {KEY_BUNDLE_CURRENT}.",
        )

    meta_k = key_scanner_meta(scanner)
    installed_version = r.hget(meta_k, "installed_version") or ""

    desired_version = bundle["version"]
    if installed_version == "":
        action = "init"
    elif installed_version != desired_version:
        action = "update"
    else:
        action = "none"

    r.hset(meta_k, mapping={"last_seen_utc": utc_iso()})

    return {
        "allowed": True,
        "scanner": scanner,
        "action": action,
        "bundle_name": bundle["name"],
        "bundle_version": desired_version,
        "bundle_url": f"/bootstrap/bundle/{bundle['name']}",
        "sha256": bundle["sha256"],
        "installed_version": installed_version,
        "whitelist_key": KEY_WHITELIST,
    }

@app.get("/bootstrap/bundle/{bundle_name}", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_bundle(bundle_name: str):
    """
    Download the current bootstrap bundle file (ZIP).

    Security rule (simple and effective):
      Only allow downloading the *currently configured* bundle by exact name.
    """
    bundle = get_bundle_current()
    if not bundle.get("exists"):
        raise HTTPException(status_code=404, detail="bundle file missing on disk")

    if bundle_name != bundle["name"]:
        raise HTTPException(status_code=404, detail="bundle not found")

    path = Path(bundle["path"])
    return FileResponse(
        path=str(path),
        filename=path.name,
        media_type="application/zip",
    )

@app.post("/bootstrap/report/{scanner}", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_report(scanner: str, report: BootstrapReport) -> Dict[str, Any]:
    """
    Scanner reports it has installed a bundle version.

    Redis:
      HSET nms:scanner:{scanner}:meta installed_version, last_bootstrap_utc, last_seen_utc
    """
    require_whitelisted(scanner)
    meta_k = key_scanner_meta(scanner)
    r.hset(meta_k, mapping={
        "installed_version": report.installed_version,
        "last_bootstrap_utc": utc_iso(),
        "last_seen_utc": utc_iso(),
    })
    return {"status": "ok", "scanner": scanner, "installed_version": report.installed_version}

# =============================================================================
# 3) Ingest (pass-through relay; opaque payload queued for uplink)
# =============================================================================
@app.post("/ingest/{scanner}", tags=["3 Ingest (Pass-through Relay)"])
async def ingest(scanner: str, request: Request) -> Dict[str, Any]:
    """
    Pass-through ingest endpoint (opaque payload).

    This NMS does NOT parse the payload. It queues it for uplink to Web later.

    Redis STREAM: nms:uplink:{scanner}
      fields:
        - received_at_utc
        - content_type
        - size
        - sha256
        - payload_b64
    """
    require_whitelisted(scanner)

    body: bytes = await request.body()
    content_type = request.headers.get("content-type", "application/octet-stream")
    received_at = utc_iso()
    size = len(body)
    sha256 = hashlib.sha256(body).hexdigest()
    payload_b64 = base64.b64encode(body).decode("ascii")

    key = key_uplink_stream(scanner)
    r.xadd(
        key,
        {
            "received_at_utc": received_at,
            "content_type": content_type,
            "size": str(size),
            "sha256": sha256,
            "payload_b64": payload_b64,
        },
        maxlen=UPLINK_MAXLEN,
        approximate=True,
    )

    # Update registry and meta (management-plane only)
    r.sadd(KEY_REGISTRY, scanner)
    r.hset(key_scanner_meta(scanner), mapping={"last_seen_utc": received_at})

    return {"status": "accepted", "scanner": scanner, "queued_in": key, "bytes": size, "sha256": sha256}

# =============================================================================
# 4) Commands (polling; extendible categories; scan only for now)
# =============================================================================
class Cmd(BaseModel):
    """
    Command format sent to a scanner.

    Fields:
      - category: high-level domain (scan | robot | video | audio | ...)
      - action:   command action string (e.g. scan.start / scan.stop)
      - execute_at: absolute UTC time (ISO8601)
      - args:     free-form dict; Pi tarball defines interpretation
    """
    category: str = "scan"
    action: str
    execute_at: str
    args: Dict[str, Any] = Field(default_factory=dict)

class CmdAck(BaseModel):
    """Scanner acknowledgement for a command execution result."""
    cmd_id: str
    status: str  # ok | error
    finished_at: Optional[str] = None
    detail: Optional[str] = None

@app.post("/cmd/enqueue/{scanner}", tags=["4 Commands (Polling)"])
def cmd_enqueue(scanner: str, cmd: Cmd) -> Dict[str, Any]:
    """
    Enqueue a command for a specific scanner.

    Redis STREAM: nms:cmd:{scanner}
    """
    require_whitelisted(scanner)

    cmd_id = str(uuid.uuid4())
    created_at = utc_iso()

    fields = {
        "cmd_id": cmd_id,
        "category": cmd.category,
        "action": cmd.action,
        "execute_at": cmd.execute_at,
        "created_at_utc": created_at,
        "args_json": json.dumps(cmd.args, ensure_ascii=False),
    }
    r.xadd(key_cmd_stream(scanner), fields, maxlen=5000, approximate=True)
    return {"status": "ok", "scanner": scanner, "cmd_id": cmd_id, "stored_in": key_cmd_stream(scanner)}

@app.get("/cmd/poll/{scanner}", tags=["4 Commands (Polling)"])
def cmd_poll(
    scanner: str,
    now: Optional[str] = None,
    limit: int = Query(5, ge=1, le=50)
) -> Dict[str, Any]:
    """
    Scanner polls for commands.

    For now: returns the most recent `limit` commands and Pi decides due-ness.
    Later: we can add server-side filtering by execute_at.
    """
    require_whitelisted(scanner)
    data = r.xrevrange(key_cmd_stream(scanner), count=limit)
    return {"scanner": scanner, "server_now_utc": utc_iso(), "client_now": now, "commands": data}

@app.post("/cmd/ack/{scanner}", tags=["4 Commands (Polling)"])
def cmd_ack(scanner: str, ack: CmdAck) -> Dict[str, Any]:
    """
    Record an acknowledgement for a command execution.

    We do not delete commands yet (auditable). We store ack records separately.
    Redis STREAM: nms:cmdack:{scanner}
    """
    require_whitelisted(scanner)

    finished_at = ack.finished_at or utc_iso()
    r.xadd(
        key_cmdack_stream(scanner),
        {
            "cmd_id": ack.cmd_id,
            "status": ack.status,
            "finished_at_utc": finished_at,
            "detail": ack.detail or "",
        },
        maxlen=20000,
        approximate=True,
    )
    return {"status": "ok", "scanner": scanner, "cmd_id": ack.cmd_id, "finished_at_utc": finished_at}

# Script loader (dummy but useful for test)
class ScriptItem(BaseModel):
    """One script item: run `action` at t0 + offset seconds."""
    scanner: str
    t_offset_sec: int
    category: str = "scan"
    action: str
    args: Dict[str, Any] = Field(default_factory=dict)

class ScriptLoad(BaseModel):
    """Script load request: t0 absolute UTC + list of relative-time items."""
    t0: str
    items: List[ScriptItem]

@app.post("/cmd/load_script", tags=["4 Commands (Polling)"])
def cmd_load_script(script: ScriptLoad) -> Dict[str, Any]:
    """
    Load a simple time-based script.

    Input:
      - t0: absolute ISO8601 UTC string
      - items: each has t_offset_sec relative to t0

    Behavior:
      - Converts each item to an absolute execute_at time (UTC)
      - Enqueues it into the scanner command stream
    """
    # Parse t0 as ISO8601
    try:
        t0_dt = datetime.fromisoformat(script.t0.replace("Z", "+00:00"))
        if t0_dt.tzinfo is None:
            # assume UTC if no tz given
            t0_dt = t0_dt.replace(tzinfo=timezone.utc)
        else:
            # normalize to UTC
            t0_dt = t0_dt.astimezone(timezone.utc)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid t0; expected ISO8601 time, e.g. 2026-01-08T04:00:00+00:00")

    added = 0
    for it in script.items:
        if not r.sismember(KEY_WHITELIST, it.scanner):
            continue

        cmd_id = str(uuid.uuid4())
        created_at = utc_iso()
        execute_at_dt = t0_dt + timedelta(seconds=int(it.t_offset_sec))
        execute_at = execute_at_dt.isoformat()

        r.xadd(
            key_cmd_stream(it.scanner),
            {
                "cmd_id": cmd_id,
                "category": it.category,
                "action": it.action,
                "execute_at": execute_at,
                "created_at_utc": created_at,
                "args_json": json.dumps(it.args, ensure_ascii=False),
            },
            maxlen=5000,
            approximate=True,
        )
        added += 1

    return {"status": "ok", "added": added}

# =============================================================================
# 9) Debug helpers (operator-only)
# =============================================================================
@app.get("/debug/queue/{scanner}", tags=["9 Debug"])
def debug_queue(scanner: str) -> Dict[str, Any]:
    """
    Show current queued uplink length for a scanner (pass-through queue).
    """
    require_whitelisted(scanner)
    key = key_uplink_stream(scanner)
    return {"scanner": scanner, "key": key, "length": r.xlen(key), "maxlen": UPLINK_MAXLEN}

@app.get("/debug/whitelist/{scanner}", tags=["9 Debug"])
def debug_whitelist(scanner: str) -> Dict[str, Any]:
    """
    Debug whitelist membership and show whitelist preview.
    """
    members = sorted(list(r.smembers(KEY_WHITELIST)))
    return {
        "whitelist_key": KEY_WHITELIST,
        "scanner": scanner,
        "sismember": bool(r.sismember(KEY_WHITELIST, scanner)),
        "members_preview": members[:50],
        "count": len(members),
    }

# =============================================================================
# (COMMENTED OUT) Old data-processing APIs - MOVE TO WEB CHATROOM LATER
# =============================================================================
"""
# NMS should NOT parse or store per-BSSID scan items in production.
# Keep legacy endpoints here for copy/paste into the Web chatroom.

@app.get("/latest/{scanner}")
def latest_scan(scanner: str):
    ...

@app.get("/last/{scanner}/{n}")
def last_n_scans(scanner: str, n: int):
    ...
"""

if __name__ == "__main__":
    # Dev mode only. In deployment, use a proper process manager.
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
