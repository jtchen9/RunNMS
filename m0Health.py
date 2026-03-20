from typing import Any, Dict
from fastapi import APIRouter
import config
import utility

router = APIRouter()


@router.get("/health", tags=["0 Health"])
def health() -> Dict[str, Any]:
    try:
        config.r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False

    return {
        "status": "ok",
        "redis_ok": redis_ok,
        "upload_enabled": config.UPLOAD_ENABLED,
        "web_post_url_configured": bool(config.WEB_NMS_UPLOAD_URL and config.WEB_NMS_STATUS_URL),
        "time": utility.local_ts(),
        "time_format": config.TIME_FMT,
    }