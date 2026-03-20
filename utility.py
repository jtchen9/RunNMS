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