from datetime import datetime
from typing import Any, List, Optional, Tuple
import json

import config


def local_ts() -> str:
    """Current local time string in the ONE official format."""
    return datetime.now().strftime(config.TIME_FMT)


def parse_local_dt(s: str) -> datetime:
    """
    Parse local time string into naive datetime.
    REQUIRED format: config.TIME_FMT

    Cleanup accepted:
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

    s = s.replace(" ", "")
    return datetime.strptime(s, config.TIME_FMT)


def normalize_mac(mac: str) -> str:
    return (mac or "").strip().lower()


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
    """Estimate size on wire (utf-8 json)."""
    return len(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _xid_to_ms(xid: str) -> int:
    return int(xid.split("-", 1)[0])


def _stream_oldest_age_sec(stream_key: str) -> Tuple[int, str]:
    first = config.r.xrange(stream_key, count=1)
    if not first:
        return 0, ""
    oldest_id, _ = first[0]
    now_ms = int(datetime.now().timestamp() * 1000)
    age_sec = max(0, (now_ms - _xid_to_ms(oldest_id)) // 1000)
    return int(age_sec), oldest_id

"""
Mobility subsystem generic utility helpers.

Division rule:
- Project-neutral helpers only.
- Keep this file small so it can be merged into utility.py later if desired.
- No state transitions and no mobility policy here.
"""
from typing import Dict, Any
import json
import math
import config

# ===== numeric / angle helpers =====

def _wrap_angle_deg(deg: float) -> float:
    d = (deg + 180.0) % 360.0 - 180.0
    return d

def _deg_norm_360(deg: float) -> float:
    x = deg % 360.0
    return x + 360.0 if x < 0 else x

def _deg_to_rad(deg: float) -> float:
    return math.radians(deg)

def _to_int(s: str, default: int = 0) -> int:
    try:
        return int(s)
    except Exception:
        return default


# ===== generic redis hash/json wrappers =====

def _hgetall(key: str) -> Dict[str, Any]:
    return config.r.hgetall(key) or {}

def _hget(key: str, field: str, default: str = "") -> str:
    data = _hgetall(key)
    return str(data.get(field) or default)

def _hset_many(key: str, mapping: Dict[str, Any]) -> None:
    out = {}
    for k, v in mapping.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        elif v is None:
            out[k] = ""
        else:
            out[k] = str(v)
    config.r.hset(key, mapping=out)

def _hget_json(key: str, field: str) -> Dict[str, Any]:
    raw = _hget(key, field, "")
    if not raw.strip():
        return {}
    try:
        j = json.loads(raw)
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}

def _hset_json(key: str, field: str, value: Dict[str, Any]) -> None:
    _hset_many(key, {field: value})

