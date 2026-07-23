from pathlib import Path
import json
import redis

# Unique identity of this NMS/lab instance.
# Used as lab_id in experiment registration and northbound reports.
NMS_NAME = "DemoRoom"

# ======================
# Constants / Config  (NO getenv; lab-only hardcoded)
#
# NOTE:
# TLS verification is disabled for lab testing because backend runs HTTPS on port 80
# with incomplete certificate chain.
# MUST set WEB_VERIFY_TLS = True before production deployment.
# ======================
WEB_BASE: str = "http://localhost:80"
# WEB_BASE: str = "https://6g-private.com:80"
WEB_VERIFY_TLS = False

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

KEY_MOBILITY_CMD_STREAM: str = f"{KEY_PREFIX}mobility:cmd"
MOBILITY_LOOP_EVERY_SEC: int = 1
MOBILITY_LOOP_BATCH_LIMIT: int = 50

# ------------------
# 5) Northbound (NMS → Web Server)
# ------------------
NMS_ID: str = "DemoRoom"
WEB_API_KEY: str = ""  # optional in early dev

WEB_NMS_UPLOAD_URL: str = f"{WEB_BASE}/nms/upload_scan_batch"
WEB_NMS_STATUS_URL: str = f"{WEB_BASE}/nms/report_status"

STATUS_EVERY_SEC: int = 10
NORTHBOUND_UPLOAD_EVERY_SEC: int = 60
# Internal NMS-only attribution window after experiment end_at.
# This is not sent to the web server; it only controls which experiment identity
# NMS attaches to 10-second and 1-minute northbound reports.
NORTHBOUND_EXPERIMENT_WRAPUP_SEC: int = 120
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

KEY_NB_LAST_UPLOAD_PAYLOAD: str = f"{KEY_PREFIX}nb:last_upload_payload"
KEY_NB_LAST_STATUS_PAYLOAD: str = f"{KEY_PREFIX}nb:last_status_payload"
NB_DEBUG_TTL_SEC: int = 48 * 3600

KEY_EXPERIMENT_REGISTRY: str = f"{KEY_PREFIX}experiment:registry"

# Persistent runtime debug flag.
# When true and no experiment runtime window is active, manual /cmd/_enqueue
# mobility commands are routed through the mobility state machine for
# interactive state-machine testing. /mobility/init resets this flag to false.
KEY_MOBILITY_STATE_MACHINE_TEST_ENABLED: str = f"{KEY_PREFIX}debug:mobility_state_machine_test_enabled"

# -------------------
# 6) AP command poll, status and performance upload
# -------------------
AP_UPLINK_MAXLEN: int = 20000
AP_STALE_TIMEOUT_SEC: int = 30

def key_ap_uplink_stream(scanner: str) -> str:
    return f"{KEY_PREFIX}ap_uplink:{scanner}"  # STREAM(AP performance payloads)

# -------------------
# 7) iperf3 Traffic (NMS-side)
# -------------------
# Robot-side passive reflector ports:
# NMS allocates from the free-port FIFO queue of each robot.
TRAFFIC_PORT_START: int = 5201
TRAFFIC_PORT_END: int = 5212

# Internal NMS traffic command/event/result streams
TRAFFIC_CMD_MAXLEN: int = 20000
TRAFFIC_EVENT_MAXLEN: int = 20000
TRAFFIC_RESULT_MAXLEN: int = 20000

# Traffic worker loop
TRAFFIC_LOOP_EVERY_SEC: int = 1
TRAFFIC_LOOP_BATCH_LIMIT: int = 50

# Temporary debug/runtime retention
# Keep short-lived runtime state only when needed.
TRAFFIC_TEMP_TTL_SEC: int = 48 * 3600

KEY_TRAFFIC_CMD_STREAM: str = f"{KEY_PREFIX}traffic:cmd"        # STREAM(NMS-internal timed traffic commands)
KEY_TRAFFIC_EVENT_STREAM: str = f"{KEY_PREFIX}traffic:events"   # STREAM(10-second traffic on/off/error events)
KEY_TRAFFIC_RESULT_STREAM: str = f"{KEY_PREFIX}traffic:results" # STREAM(1-minute completed iperf3 reports)

def key_traffic_temp_running(scanner: str, session_id: str) -> str:
    return f"{KEY_PREFIX}traffic:_temp:running:{scanner}:{session_id}"

def key_traffic_temp_ports(scanner: str) -> str:
    return f"{KEY_PREFIX}traffic:_temp:ports:{scanner}"

KEY_TRAFFIC_EVENT_TEMP_STREAM: str = f"{KEY_PREFIX}traffic:events:temp"  # STREAM(debug mirror of traffic events)
TRAFFIC_EVENT_TEMP_MAXLEN: int = 5000

# -------------------
# 8) Mobility
# -------------------
MOBILITY_SITE_NAME = "DemoRoom"
MOBILITY_SITEMAP_ROOT = Path(r".\sitemap")

def _resolve_mobility_site_name(site_name: str | None = None) -> str:
    return str(site_name or MOBILITY_SITE_NAME or NMS_NAME)

def mobility_site_dir(site_name: str | None = None) -> Path:
    return MOBILITY_SITEMAP_ROOT / _resolve_mobility_site_name(site_name)

def mobility_site_json_path(site_name: str | None = None) -> Path:
    return mobility_site_dir(site_name) / "site.json"

def mobility_restriction_map_path(site_name: str | None = None) -> Path:
    return mobility_site_dir(site_name) / "restriction_map.npy"

def mobility_tag_location_path(site_name: str | None = None) -> Path:
    return mobility_site_dir(site_name) / "tag_location.txt"

def mobility_script_authoring_config_dir(site_name: str | None = None) -> Path:
    return mobility_site_dir(site_name) / "script_authoring" / "config"

def mobility_macro_policy_path(site_name: str | None = None) -> Path:
    return mobility_script_authoring_config_dir(site_name) / "macro_policy.json"

def mobility_safety_policy_path(site_name: str | None = None) -> Path:
    return mobility_script_authoring_config_dir(site_name) / "safety_policy.json"

def _load_json_policy(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def load_mobility_macro_policy(site_name: str | None = None) -> dict:
    return _load_json_policy(mobility_macro_policy_path(site_name))

def load_mobility_safety_policy(site_name: str | None = None) -> dict:
    return _load_json_policy(mobility_safety_policy_path(site_name))

def mobility_bump_crossing_macros(site_name: str | None = None) -> dict:
    return dict(load_mobility_macro_policy(site_name).get("macros", {}) or {})

def mobility_robot_safety_radius_m(site_name: str | None = None) -> float:
    policy = load_mobility_safety_policy(site_name)
    return float(policy["robot_safety_radius_m"])

# Kept only for old code references; new code should call mobility_restriction_map_path().
MOBILITY_STATIC_RESTRICTION_MAP_NPY = str(mobility_restriction_map_path())

# Runtime should read world size from site.json.
# Grid resolution fallback is kept for legacy/debug helpers that need a default.
MOBILITY_GRID_RESOLUTION_M = 0.1

# Site-specific robot-to-robot safety radius.
#
# The authoritative value lives in:
#   sitemap/<site_name>/script_authoring/config/safety_policy.json
#
# Keep this public constant name for existing runtime modules. The value is now
# loaded from the same site policy JSON that CommonCheckers uses, so callers do
# not need to change their interface while avoiding duplicate definitions.
MOBILITY_ROBOT_RESTRICT_RADIUS_M = mobility_robot_safety_radius_m()
# -------- Mobility correction thresholds --------

# Ignore very small residual position errors. Used by S5.
MOBILITY_POS_IGNORE_THRESH_M = 0.05

# Strict runtime gate used by S5 before issuing the first correction.
# If the main/scripted command ends more than this far from the planned target,
# do not try a large correction; stop the experiment for manual review.
MOBILITY_FIRST_CORRECTION_MAX_M = 0.60

# Strict runtime convergence gate used by S5 during AutoLab bring-up.
# After a correction command has already been attempted, residual position error
# above this value means the robot did not converge reliably; stop the experiment.
MOBILITY_POST_CORRECTION_FAIL_THRESH_M = 0.15


# -------- Site-specific mobility macros --------
# NMS-only macro actions. These are never sent directly to robots.
MOBILITY_MACRO_IN2OUT = "mobility.in2out"
MOBILITY_MACRO_OUT2IN = "mobility.out2in"
MOBILITY_SITE_MACRO_ACTIONS = {
    MOBILITY_MACRO_IN2OUT,
    MOBILITY_MACRO_OUT2IN,
}

# Site-specific bump-crossing macro geometry.
#
# The authoritative values live in:
#   sitemap/<site_name>/script_authoring/config/macro_policy.json
#
# Keep this public constant name for existing runtime modules. The value is now
# loaded from the same site policy JSON that CommonCheckers uses, so callers do
# not need to change their interface while avoiding duplicate definitions.
MOBILITY_BUMP_CROSSING_MACROS = mobility_bump_crossing_macros()

# Script-level policy. Low-level turn-move-turn commands may still be generated
# internally by NMS, but experiment CSVs should use semantic movement/macro actions.
MOBILITY_SCRIPT_BLOCKED_ACTIONS = {
    "mobility.turn",
    "mobility.turn_move_turn.forward",
    "mobility.turn_move_turn.backward",
}
MOBILITY_SCRIPT_ALLOWED_ACTIONS = {
    "mobility.report.location",
    "mobility.move",
    MOBILITY_MACRO_IN2OUT,
    MOBILITY_MACRO_OUT2IN,
}


# Dynamic obstacle freshness.
# A powered-off / put-away robot must not remain as a phantom blocker.
MOBILITY_DYNAMIC_OBSTACLE_TTL_SEC = 120

# -----------------------------
# Mobility report interception
# -----------------------------
# Production-code debug outlet for NMS state-machine tests.
#
# Important design:
# - The outlet exists in production code.
# - It is normally inactive.
# - testSM scripts enable it at runtime by Redis key.
# - No config.py edit or NMS restart is needed per test.
#
# Kill switch:
#   Set MOBILITY_REPORT_INTERCEPT_AVAILABLE = False to disable this feature entirely.
#
# Runtime enable keys:
#   nms:debug:mobility_report_intercept:enabled
#   nms:debug:mobility_report_intercept:enabled:<scanner>
#
# Rule key:
#   nms:debug:mobility_report_intercept:<scanner>
#
# Event stream:
#   nms:debug:mobility_report_intercept:events
MOBILITY_REPORT_INTERCEPT_AVAILABLE = True
MOBILITY_REPORT_INTERCEPT_KEY_PREFIX = f"{KEY_PREFIX}debug:mobility_report_intercept:"
MOBILITY_REPORT_INTERCEPT_ENABLE_KEY = f"{KEY_PREFIX}debug:mobility_report_intercept:enabled"
MOBILITY_REPORT_INTERCEPT_EVENT_STREAM = f"{KEY_PREFIX}debug:mobility_report_intercept:events"

# ==================
# Runtime init
# ==================
BUNDLE_DIR.mkdir(parents=True, exist_ok=True)
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
