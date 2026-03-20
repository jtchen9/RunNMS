from pathlib import Path
import redis

# ======================
# Constants / Config  (NO getenv; lab-only hardcoded)
# ======================
WEB_BASE: str = "http://localhost:80"
# WEB_BASE: str = "https://6g-private.com:80"

REDIS_URL: str = "redis://localhost:6379/0"
KEY_PREFIX: str = "nms:"  # Redis keyspace prefix

# ---- Time format (ONE format everywhere) ----
TIME_FMT: str = "%Y-%m-%d-%H:%M:%S"

# -----------------
# 1) Registry & Whitelist
# -----------------
KEY_REGISTRY: str = f"{KEY_PREFIX}registry:scanners"  # SET(scanner_name)

def key_scanner_meta(scanner: str) -> str:
    return f"{KEY_PREFIX}scanner:{scanner}:meta"  # HASH(...)

KEY_WHITELIST_SCANNER_META: str = f"{KEY_PREFIX}registry:whitelist_scanner_meta"
WHITELIST_SCHEMA_VERSION: int = 1
DEFAULT_LLM_WEBLINK: str = "https://chatgpt.com/"

# -------------------
# 2) Bootstrap (Bundles)
# -------------------
BUNDLE_DIR: Path = Path(r"D:\Data\_Action\_RunNMS\bundles")
KEY_BUNDLE_INDEX: str = f"{KEY_PREFIX}bundle:index"
BUNDLE_META_SCHEMA_VERSION: int = 1

# -----------------
# 3) Southbound Ingest (Pi→NMS)
# -----------------
UPLINK_MAXLEN: int = 50000
UPLOAD_ENABLED: bool = True

def key_uplink_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}uplink:{scanner}"  # STREAM(opaque payloads)

# ---------------
# 4) Commands (Polling)
# ---------------
CMD_EXPIRE_SEC: int = 3600  # seconds

def key_cmd_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}cmd:{scanner}"  # STREAM(commands)

def key_cmdack_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}cmdack:{scanner}"  # STREAM(acks)

# ------------------
# 5) Northbound (NMS → Web Server)
# ------------------
NMS_ID: str = "nms-lab-01"
WEB_API_KEY: str = ""  # optional in early dev

WEB_NMS_UPLOAD_URL: str = f"{WEB_BASE}/nms/upload_scan_batch"
WEB_NMS_STATUS_URL: str = f"{WEB_BASE}/nms/report_status"

NORTHBOUND_EVERY_SEC: int = 10
UPLOAD_BATCH_MAX_BYTES: int = 100_000  # hard cap per POST (≈100KB)

KEY_NB_LAST_UPLOAD: str = f"{KEY_PREFIX}nb:last_upload"
KEY_NB_LAST_RESULT: str = f"{KEY_PREFIX}nb:last_result"
KEY_NB_LAST_STATUS: str = f"{KEY_PREFIX}nb:last_status"

KEY_NB_LAST_CMDS: str = f"{KEY_PREFIX}nb:last_cmds"
KEY_NB_LAST_CMDS_TIME: str = f"{KEY_PREFIX}nb:last_cmds_time"
KEY_NB_LAST_CMDS_ERR: str = f"{KEY_PREFIX}nb:last_cmds_err"

KEY_INTENT_VIDEO: str = f"{KEY_PREFIX}intent:video"
KEY_INTENT_VIDEO_TS: str = f"{KEY_PREFIX}intent:video_ts"

KEY_APPLIED_VIDEO: str = f"{KEY_PREFIX}applied:video"
KEY_APPLIED_VIDEO_TS: str = f"{KEY_PREFIX}applied:video_ts"

# -------------------
# 6) AP performance upload
# -------------------
AP_UPLINK_MAXLEN: int = 20000

def key_ap_uplink_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}ap_uplink:{scanner}"  # STREAM(AP performance payloads)

# ==================
# Runtime init
# ==================
BUNDLE_DIR.mkdir(parents=True, exist_ok=True)
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)