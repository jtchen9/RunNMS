from fastapi import Body, FastAPI, Request, HTTPException, Query
from fastapi.responses import FileResponse, PlainTextResponse
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
from typing import Tuple
import requests
import csv
import io

load_dotenv()

# =============================================================================
# Config (.env)
# =============================================================================
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
KEY_PREFIX = os.getenv("KEY_PREFIX", "nms:")  # Keyspace prefix

# -----------------------------
# South Bound Config
# -----------------------------
# Queue/retention controls
UPLINK_MAXLEN = int(os.getenv("UPLINK_MAXLEN", "50000"))   # per-scanner queue trim
UPLOAD_ENABLED = os.getenv("UPLOAD_ENABLED", "1") == "1"   # allow turning off uploader during dev

# Bundle directory (Windows-friendly)
BUNDLE_DIR = Path(os.getenv("BUNDLE_DIR", r"D:\_RunNMS\bundles"))
BUNDLE_DIR.mkdir(parents=True, exist_ok=True)

# Comand polling
CMD_EXPIRE_SEC = int(os.getenv("CMD_EXPIRE_SEC", "3600"))  # 1 hour default

# -----------------------------
# North Bound Config
# -----------------------------
# Web (cloud) uplink endpoint (provided by Web chatroom later)
WEB_POST_URL = os.getenv("WEB_POST_URL", "")  # e.g. https://your-web-host/ingest
WEB_API_KEY = os.getenv("WEB_API_KEY", "")    # optional

NMS_ID = os.getenv("NMS_ID", "nms-192.168.137.3")
WEB_BASE = os.getenv("WEB_BASE", "https://6g-private.com")  # web host
WEB_POST_URL = os.getenv("WEB_POST_URL", f"{WEB_BASE}/dataScanned/batch")  # web contract
WEB_API_KEY = os.getenv("WEB_API_KEY", "")

AUTO_FLUSH = os.getenv("AUTO_FLUSH", "0") == "1"           # default OFF (safe)
AUTO_FLUSH_EVERY_SEC = int(os.getenv("AUTO_FLUSH_EVERY_SEC", "10"))
FLUSH_MIN_ITEMS = int(os.getenv("FLUSH_MIN_ITEMS", "20"))  # upload if queue >= this
FLUSH_MAX_WAIT_SEC = int(os.getenv("FLUSH_MAX_WAIT_SEC", "60"))  # upload if oldest waited >= this
FLUSH_BATCH_LIMIT = int(os.getenv("FLUSH_BATCH_LIMIT", "200"))   # max items per flush per scanner

KEY_LAST_UPLOAD = f"{KEY_PREFIX}uplink:last_upload"   # HASH scanner -> last_upload_utc
KEY_LAST_RESULT = f"{KEY_PREFIX}uplink:last_result"   # HASH scanner -> last_result (short string)
KEY_AUTO_FLUSH = f"{KEY_PREFIX}uplink:auto_flush"  # string "0"/"1"

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
KEY_WHITELIST_NAME2MAC = f"{KEY_PREFIX}registry:whitelist_name2mac"  # HASH(name -> mac)
KEY_REGISTRY  = f"{KEY_PREFIX}registry:scanners"                     # SET(scanner)
KEY_BUNDLE_CURRENT = f"{KEY_PREFIX}bundle:current"                   # HASH(name, version, path, sha256)

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

def parse_utc_iso(s: str) -> datetime:
    """
    Parse ISO8601 time string into aware UTC datetime.
    Accepts 'Z' suffix or '+00:00'.
    """
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)

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
    """403 if scanner name not in whitelist hash (name -> mac)."""
    if not r.hexists(KEY_WHITELIST_NAME2MAC, scanner):
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
class WhitelistItem(BaseModel):
    scanner: str = Field(..., description="Assigned name, e.g. scanner01")
    mac: str = Field(..., description="MAC address, e.g. 2c:cf:67:d0:67:f3")

class WhitelistUpsertReq(BaseModel):
    items: List[WhitelistItem] = Field(default_factory=list)
    
class RegisterReq(BaseModel):
    mac: str
    ip: Optional[str] = None
    scanner_version: Optional[str] = None
    capabilities: Optional[str] = None

def normalize_mac(mac: str) -> str:
    return (mac or "").strip().lower()

def find_name_by_mac(mac: str) -> str:
    """
    Reverse lookup using the single source-of-truth HASH(name -> mac).
    For ~10 Pis, scanning is fine and avoids dual-table sync problems.
    """
    mac = normalize_mac(mac)
    cursor = 0
    while True:
        cursor, pairs = r.hscan(KEY_WHITELIST_NAME2MAC, cursor=cursor, count=200)
        # pairs is dict: {name: mac}
        for name, v in pairs.items():
            if (v or "").strip().lower() == mac:
                return name
        if int(cursor) == 0:
            break
    return ""

@app.post("/registry/whitelist/upsert", tags=["1 Registry & Whitelist"])
def whitelist_upsert(req: WhitelistUpsertReq) -> Dict[str, Any]:
    """
    Add/update whitelist mappings (scanner name -> MAC).
    Single source of truth: Redis HASH nms:registry:whitelist_name2mac
    """
    if not req.items:
        return {"status": "ok", "upserted": 0}

    mapping: Dict[str, str] = {}
    for it in req.items:
        scanner = (it.scanner or "").strip()
        mac = (it.mac or "").strip().lower()
        if not scanner or not mac:
            continue
        mapping[scanner] = mac

    if not mapping:
        return {"status": "ok", "upserted": 0}

    r.hset(KEY_WHITELIST_NAME2MAC, mapping=mapping)
    return {"status": "ok", "upserted": len(mapping)}

@app.get("/registry/whitelist", tags=["1 Registry & Whitelist"])
def whitelist_list() -> Dict[str, Any]:
    m = r.hgetall(KEY_WHITELIST_NAME2MAC) or {}
    # sort for stable display in Swagger responses
    items = [{"scanner": k, "mac": v} for k, v in sorted(m.items(), key=lambda x: x[0])]
    return {"count": len(items), "items": items, "key": KEY_WHITELIST_NAME2MAC}

@app.delete("/registry/whitelist/{scanner}", tags=["1 Registry & Whitelist"])
def whitelist_delete(scanner: str) -> Dict[str, Any]:
    scanner = (scanner or "").strip()
    if not scanner:
        raise HTTPException(status_code=400, detail="scanner required")
    removed = r.hdel(KEY_WHITELIST_NAME2MAC, scanner)
    return {"status": "ok", "removed": int(removed), "scanner": scanner}

@app.get("/registry/whitelist/reverse/{mac}", tags=["1 Registry & Whitelist"])
def whitelist_reverse(mac: str) -> Dict[str, Any]:
    mac = (mac or "").strip().lower()
    name = find_name_by_mac(mac)
    return {"mac": mac, "scanner": name, "found": bool(name)}

@app.post("/registry/register", tags=["1 Registry & Whitelist"])
def register(req: RegisterReq):
    """
    Register a scanner by MAC (Pi is dumb, cloned SD).

    - Looks up scanner name by MAC in Redis HASH(name->mac)
    - Updates registry/meta keyed by *name*
    - Returns plain text scanner name (e.g., 'scanner01')
    """
    mac = normalize_mac(req.mac)
    scanner = find_name_by_mac(mac)
    if not scanner:
        raise HTTPException(status_code=403, detail=f"MAC '{mac}' not in whitelist")

    now = utc_iso()

    # Keep registry as NAMES (minimal disruption)
    r.sadd(KEY_REGISTRY, scanner)

    updates: Dict[str, str] = {"last_seen_utc": now, "mac": mac}
    if req.ip:
        updates["ip"] = req.ip
    if req.scanner_version:
        updates["scanner_version"] = req.scanner_version
    if req.capabilities:
        updates["capabilities"] = req.capabilities

    r.hset(key_scanner_meta(scanner), mapping=updates)

    # Plain text response for Pi
    return PlainTextResponse(content=scanner)

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
        "whitelist_key": KEY_WHITELIST_NAME2MAC,
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
@app.post("/ingest/{scanner}", tags=["3 Southbound Ingest (Pi→NMS)"])
async def ingest(
    scanner: str,
    payload: bytes = Body(..., media_type="application/octet-stream",
                          description="Opaque payload (JSON/CSV/binary). NMS will not parse."),
    request: Request = None,
):
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

    body = payload
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
    Scanner polls for commands that are due now, while filtering out expired ones.

    Policy:
      - due if execute_at <= server_now_utc
      - expired if server_now_utc - execute_at > CMD_EXPIRE_SEC
    """
    require_whitelisted(scanner)

    server_now = utc_now_dt()
    # Optional: accept client-provided now, but we control centrally using server_now.
    # (We still echo it back for debugging.)
    raw = r.xrange(key_cmd_stream(scanner), count=5000)  # oldest -> newest (stable ordering)

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
            exec_at = parse_utc_iso(exec_at_s)
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

        due_cmds.append((xid, fields))
        if len(due_cmds) >= limit:
            break

    return {
        "scanner": scanner,
        "server_now_utc": server_now.isoformat(),
        "client_now": now,
        "cmd_expire_sec": CMD_EXPIRE_SEC,
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
        if not r.sismember(KEY_WHITELIST_NAME2MAC, it.scanner):
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

class CmdLoadCSVReq(BaseModel):
    t0: str = Field(..., description="Absolute UTC ISO8601 time, e.g. 2026-01-08T04:00:00+00:00")
    csv_text: str = Field(..., description="CSV rows with columns: scanner,t_offset_sec,category,action,args_json")

@app.post("/cmd/load_csv", tags=["4 Commands (Polling)"])
def cmd_load_csv(req: CmdLoadCSVReq) -> Dict[str, Any]:
    """
    Load commands from a pasted CSV (Excel-friendly).

    CSV columns:
      scanner,t_offset_sec,category,action,args_json

    - execute_at = t0 + t_offset_sec seconds
    - skips non-whitelisted scanners
    """
    try:
        t0_dt = parse_utc_iso(req.t0)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid t0; expected ISO8601 UTC")

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
        if not r.sismember(KEY_WHITELIST_NAME2MAC, scanner):
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

        execute_at = (t0_dt + timedelta(seconds=offset)).isoformat()

        cmd_id = str(uuid.uuid4())
        r.xadd(
            key_cmd_stream(scanner),
            {
                "cmd_id": cmd_id,
                "category": category,
                "action": action,
                "execute_at": execute_at,
                "created_at_utc": utc_iso(),
                "args_json": json.dumps(args, ensure_ascii=False),
            },
            maxlen=5000,
            approximate=True,
        )
        added += 1

    return {
        "status": "ok",
        "added": added,
        "skipped_not_whitelisted": skipped_not_whitelisted,
        "bad_rows": bad_rows,
    }

# =============================================================================
# 5) Northbound Relay (NMS -> Web /dataScanned)
#     - This is the "uploader" side (northbound)
#     - It consumes Redis streams queued by /ingest/{scanner} (southbound)
# =============================================================================

def _xid_to_ms(xid: str) -> int:
    """Convert Redis stream id '<ms>-<seq>' to millisecond timestamp."""
    return int(xid.split("-", 1)[0])

def _stream_oldest_age_sec(stream_key: str) -> Tuple[int, str]:
    """
    Return (age_sec, oldest_id) for the oldest entry in a stream.
    If the stream is empty, returns (0, "").
    """
    first = r.xrange(stream_key, count=1)
    if not first:
        return 0, ""
    oldest_id, _fields = first[0]
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    age_sec = max(0, (now_ms - _xid_to_ms(oldest_id)) // 1000)
    return int(age_sec), oldest_id

def _should_flush(scanner: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Decide whether a scanner stream should flush based on policy:
      - queue length >= FLUSH_MIN_ITEMS OR
      - oldest item age >= FLUSH_MAX_WAIT_SEC
    """
    stream_key = key_uplink_stream(scanner)
    qlen = r.xlen(stream_key)

    if qlen <= 0:
        return False, {"scanner": scanner, "key": stream_key, "qlen": 0, "reason": "empty"}

    age_sec, oldest_id = _stream_oldest_age_sec(stream_key)

    if qlen >= FLUSH_MIN_ITEMS:
        return True, {"scanner": scanner, "key": stream_key, "qlen": qlen, "age_sec": age_sec, "oldest_id": oldest_id, "reason": "min_items"}

    if age_sec >= FLUSH_MAX_WAIT_SEC:
        return True, {"scanner": scanner, "key": stream_key, "qlen": qlen, "age_sec": age_sec, "oldest_id": oldest_id, "reason": "max_wait"}

    return False, {"scanner": scanner, "key": stream_key, "qlen": qlen, "age_sec": age_sec, "oldest_id": oldest_id, "reason": "not_due"}

def _post_to_web(scanner: str, items: list) -> Tuple[bool, str]:
    """
    POST a batch to the Web server.
    Returns (ok, detail). ok=True only if web returns JSON with {"ok": true}.
    """
    if not WEB_POST_URL:
        return False, "WEB_POST_URL not configured"

    headers: Dict[str, str] = {}
    if WEB_API_KEY:
        headers["X-API-Key"] = WEB_API_KEY

    payload = {
        "nms_id": NMS_ID,
        "scanner": scanner,
        "items": items,
    }

    try:
        resp = requests.post(WEB_POST_URL, json=payload, headers=headers, timeout=10)
        resp.raise_for_status()
        j = resp.json()
        if isinstance(j, dict) and j.get("ok") is True:
            return True, f"web ok accepted={j.get('accepted')}"
        return False, f"web returned {j}"
    except Exception as e:
        return False, f"post failed: {e}"

@app.get("/relay/status", tags=["5 Northbound Relay (NMS→Web /dataScanned)"])
def relay_status() -> Dict[str, Any]:
    """
    Show per-scanner uplink queue status.
    This is purely NMS-side validation/visibility.
    """
    scanners = sorted(list(r.smembers(KEY_REGISTRY)))
    out = []

    for s in scanners:
        stream_key = key_uplink_stream(s)
        qlen = r.xlen(stream_key)
        age_sec, oldest_id = _stream_oldest_age_sec(stream_key) if qlen else (0, "")
        out.append({
            "scanner": s,
            "queue_key": stream_key,
            "qlen": qlen,
            "oldest_age_sec": age_sec,
            "oldest_id": oldest_id,
            "last_upload_utc": r.hget(KEY_LAST_UPLOAD, s) or "",
            "last_result": r.hget(KEY_LAST_RESULT, s) or "",
        })

    return {
        "status": "ok",
        "web_post_url": WEB_POST_URL,
        "auto_flush": AUTO_FLUSH,
        "flush_every_sec": AUTO_FLUSH_EVERY_SEC,
        "policy": {
            "min_items": FLUSH_MIN_ITEMS,
            "max_wait_sec": FLUSH_MAX_WAIT_SEC,
            "batch_limit": FLUSH_BATCH_LIMIT,
        },
        "scanners": out,
    }

@app.post("/relay/flush", tags=["5 Northbound Relay (NMS→Web /dataScanned)"])
def relay_flush(
    scanner: Optional[str] = None,
    force: bool = False,
    limit: int = Query(FLUSH_BATCH_LIMIT, ge=1, le=5000),
) -> Dict[str, Any]:
    """
    Flush queued items to Web for one scanner or all scanners.
    - If force=false, only flush scanners meeting policy (min_items OR max_wait).
    - Deletes items from Redis only when Web replies ok=true.
    """
    scanners = [scanner] if scanner else sorted(list(r.smembers(KEY_REGISTRY)))
    results = []

    for s in scanners:
        stream_key = key_uplink_stream(s)
        qlen = r.xlen(stream_key)

        if qlen == 0:
            results.append({"scanner": s, "flushed": 0, "skipped": True, "reason": "empty"})
            continue

        due, info = _should_flush(s)
        if (not force) and (not due):
            results.append({"scanner": s, "flushed": 0, "skipped": True, **info})
            continue

        # Pull oldest entries up to limit (preserve order)
        entries = r.xrange(stream_key, count=limit)

        ids: List[str] = []
        items: List[Dict[str, Any]] = []

        for xid, fields in entries:
            ids.append(xid)
            items.append({
                "redis_id": xid,
                # IMPORTANT: field name must match what /ingest stored
                "received_at_utc": fields.get("received_at_utc", ""),
                "content_type": fields.get("content_type", ""),
                "size": int(fields.get("size", "0") or 0),
                "sha256": fields.get("sha256", ""),
                "payload_b64": fields.get("payload_b64", ""),
            })

        ok, detail = _post_to_web(s, items)

        if ok:
            if ids:
                r.xdel(stream_key, *ids)
            r.hset(KEY_LAST_UPLOAD, s, utc_iso())
            r.hset(KEY_LAST_RESULT, s, f"ok flushed={len(ids)}")
            results.append({"scanner": s, "flushed": len(ids), "deleted": len(ids), "web_detail": detail})
        else:
            r.hset(KEY_LAST_RESULT, s, f"fail {detail[:180]}")
            results.append({"scanner": s, "flushed": 0, "deleted": 0, "error": detail, "queued": qlen})

    return {"status": "ok", "results": results}

@app.get("/relay/autoflush/status", tags=["5 Northbound Relay (NMS→Web /dataScanned)"])
def relay_autoflush_status() -> Dict[str, Any]:
    val = r.get(KEY_AUTO_FLUSH)
    if val is None:
        val = "1" if AUTO_FLUSH else "0"
        r.set(KEY_AUTO_FLUSH, val)
    return {
        "enabled": (val == "1"),
        "key": KEY_AUTO_FLUSH,
        "every_sec": AUTO_FLUSH_EVERY_SEC,
        "policy": {
            "min_items": FLUSH_MIN_ITEMS,
            "max_wait_sec": FLUSH_MAX_WAIT_SEC,
            "batch_limit": FLUSH_BATCH_LIMIT,
        },
        "web_post_url": WEB_POST_URL,
    }

class AutoFlushSetReq(BaseModel):
    enabled: bool

@app.post("/relay/autoflush/set", tags=["5 Northbound Relay (NMS→Web /dataScanned)"])
def relay_autoflush_set(req: AutoFlushSetReq) -> Dict[str, Any]:
    r.set(KEY_AUTO_FLUSH, "1" if req.enabled else "0")
    return {"status": "ok", "enabled": req.enabled, "key": KEY_AUTO_FLUSH}

# =============================================================================
#8) Admin
# =============================================================================
class ResetReq(BaseModel):
    confirm: str = Field(..., description="Must be EXACTLY 'RESET' to proceed.")
    keep_whitelist: bool = True
    keep_bundle_current: bool = True

@app.post("/admin/reset", tags=["8 Admin"])
def admin_reset(req: ResetReq) -> Dict[str, Any]:
    """
    Reset experiment state in Redis.

    Deletes:
      - registry set
      - scanner metas
      - uplink queues
      - command streams and acks
      - relay bookkeeping (last upload/result)

    Keeps by default:
      - whitelist (recommended)
      - bundle current metadata (recommended)
    """
    if req.confirm != "RESET":
        raise HTTPException(status_code=400, detail="confirm must be 'RESET'")

    # Keys we must keep
    keep = set()
    if req.keep_whitelist:
        keep.add(KEY_WHITELIST_NAME2MAC)
    if req.keep_bundle_current:
        keep.add(KEY_BUNDLE_CURRENT)

    # Also keep any other config keys you may add later by whitelisting them here.

    # Scan and delete
    deleted = 0
    cursor = 0
    pattern = f"{KEY_PREFIX}*"
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
        to_del = [k for k in keys if k not in keep]
        if to_del:
            deleted += r.delete(*to_del)
        if cursor == 0:
            break

    return {
        "status": "ok",
        "deleted_keys_count": int(deleted),
        "kept": sorted(list(keep)),
        "prefix": KEY_PREFIX,
    }

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
    mac = r.hget(KEY_WHITELIST_NAME2MAC, scanner) or ""
    return {
        "whitelist_key": KEY_WHITELIST_NAME2MAC,
        "scanner": scanner,
        "hexists": bool(r.hexists(KEY_WHITELIST_NAME2MAC, scanner)),
        "mac": mac,
    }

import asyncio

async def _autoflush_loop():
    while True:
        try:
            enabled = (r.get(KEY_AUTO_FLUSH) == "1")
            if enabled:
                # Call the same logic as relay_flush(scanner=None,...)
                # Force=False so policy decides. Limit=FLUSH_BATCH_LIMIT.
                relay_flush(scanner=None, force=False, limit=FLUSH_BATCH_LIMIT)
        except Exception as e:
            # swallow errors to keep loop alive
            pass
        await asyncio.sleep(AUTO_FLUSH_EVERY_SEC)

@app.on_event("startup")
async def _startup():
    # Initialize flag if missing
    if r.get(KEY_AUTO_FLUSH) is None:
        r.set(KEY_AUTO_FLUSH, "1" if AUTO_FLUSH else "0")
    # Start background task
    asyncio.create_task(_autoflush_loop())

if __name__ == "__main__":
    # Dev mode only. In deployment, use a proper process manager.
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
