from typing import Dict, Any, Set
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException

import config
import utility

router = APIRouter()


# ====================
# 9) Admin
# ====================
class ResetReq(BaseModel):
    confirm: str = Field(..., description="Must be EXACTLY 'RESET' to proceed.")
    keep_whitelist: bool = True
    keep_bundles: bool = True  # keep bundle metadata keys (and never touch bundle files on disk)


@router.post("/admin/_reset", tags=["9 Admin"])
def admin_reset(req: ResetReq) -> Dict[str, Any]:
    """
    Admin-only: delete Redis keys under KEY_PREFIX, with optional keeps.
    - Does NOT touch bundle ZIP files on disk.
    - By default keeps whitelist + bundle index
    """
    if req.confirm != "RESET":
        raise HTTPException(status_code=400, detail="confirm must be 'RESET'")

    keep: Set[str] = set()
    if req.keep_whitelist:
        keep.add(config.KEY_WHITELIST_SCANNER_META)
    if req.keep_bundles:
        keep.add(config.KEY_BUNDLE_INDEX)

    deleted = 0
    scanned = 0
    cursor = 0
    pattern = f"{config.KEY_PREFIX}*"

    while True:
        cursor, keys = config.r.scan(cursor=cursor, match=pattern, count=1000)
        scanned += len(keys)
        to_del = [k for k in keys if k not in keep]
        if to_del:
            deleted += int(config.r.delete(*to_del))
        if int(cursor) == 0:
            break

    return {
        "status": "ok",
        "time": utility.local_ts(),
        "prefix": config.KEY_PREFIX,
        "scanned_keys_count": int(scanned),
        "deleted_keys_count": int(deleted),
        "kept": sorted(list(keep)),
        "bundle_dir_untouched": str(config.BUNDLE_DIR),
        "note": "Redis keys removed; bundle ZIP files on disk are untouched.",
    }