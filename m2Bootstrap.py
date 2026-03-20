from pathlib import Path
from typing import Dict, Any, List
import json
import hashlib

from fastapi import APIRouter, HTTPException, Query, File, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

import config
import utility
import m1Registry

router = APIRouter()


# ====================
# 2) Bootstrap (Bundles)
# ====================
class BootstrapReport(BaseModel):
    installed_version: str


def _bundle_zip_path(bundle_id: str) -> Path:
    return config.BUNDLE_DIR / f"{bundle_id}.zip"


def _validate_bundle_id(bundle_id: str) -> str:
    if not bundle_id or not (bundle_id.startswith("apBundle") or bundle_id.startswith("robotBundle")):
        raise HTTPException(status_code=400, detail="bundle_id must start with 'robotBundle'")
    for ch in bundle_id:
        if not (ch.isalnum() or ch in "._-"):
            raise HTTPException(status_code=400, detail="bundle_id contains invalid characters")
    return bundle_id


def _bundle_exists(bundle_id: str) -> bool:
    if config.r.hexists(config.KEY_BUNDLE_INDEX, bundle_id):
        return True
    return _bundle_zip_path(bundle_id).exists()


def _bundle_meta_get(bundle_id: str) -> Dict[str, Any]:
    s = config.r.hget(config.KEY_BUNDLE_INDEX, bundle_id)
    raw: Any = None

    if not s:
        return {}

    try:
        raw = json.loads(s)
    except Exception:
        raw = None

    if not isinstance(raw, dict):
        raw = {"bundle_id": bundle_id, "meta_raw": s}

    meta: Dict[str, Any] = {
        "schema_version": int(raw.get("schema_version") or config.BUNDLE_META_SCHEMA_VERSION),
        "bundle_id": str(raw.get("bundle_id") or bundle_id),
        "sha256": str(raw.get("sha256") or ""),
        "size_bytes": int(raw.get("size_bytes") or 0),
        "uploaded_at": str(raw.get("uploaded_at") or ""),
        "path": str(raw.get("path") or str(_bundle_zip_path(bundle_id))),
        "stored_as": str(raw.get("stored_as") or Path(str(raw.get("path") or _bundle_zip_path(bundle_id))).name),
        "comment": str(raw.get("comment") or ""),
    }

    try:
        config.r.hset(config.KEY_BUNDLE_INDEX, bundle_id, json.dumps(meta, ensure_ascii=False))
    except Exception:
        pass

    return meta


@router.get("/bootstrap/_list_bundles", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_list_bundles() -> Dict[str, Any]:
    idx = config.r.hgetall(config.KEY_BUNDLE_INDEX) or {}

    items: List[Dict[str, Any]] = []
    for bundle_id, meta_s in idx.items():
        try:
            meta = json.loads(meta_s)
            if not isinstance(meta, dict):
                meta = {"bundle_id": bundle_id, "meta_raw": meta_s}
        except Exception:
            meta = {"bundle_id": bundle_id, "meta_raw": meta_s}

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
        "time": utility.local_ts(),
        "count": len(items),
        "items": items,
        "bundle_dir": str(config.BUNDLE_DIR),
        "index_key": config.KEY_BUNDLE_INDEX,
    }


@router.get("/bootstrap/_list_bundle_meta/{scanner}", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_list_bundle_meta(scanner: str) -> Dict[str, Any]:
    m1Registry.require_whitelisted(scanner)
    meta = config.r.hgetall(config.key_scanner_meta(scanner)) or {}

    scanner_version = meta.get("scanner_version", "")
    installed_version = meta.get("installed_version", "")

    return {
        "time": utility.local_ts(),
        "scanner": scanner,
        "scanner_version": scanner_version,
        "installed_version": installed_version,
        "effective_bundle": (installed_version or scanner_version),
        "note": "Debug only. Upgrades are controlled by commands; these fields are telemetry.",
    }


@router.post("/bootstrap/_bundle", tags=["2 Bootstrap (Init/Update)"])
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

    if target_path.exists() or config.r.hexists(config.KEY_BUNDLE_INDEX, bundle_id):
        overwrite = True
        previous_meta = _bundle_meta_get(bundle_id)

        if target_path.exists():
            ts_tag = utility.local_ts().replace("-", "").replace(":", "")
            backup_name = f"{bundle_id}__old__{ts_tag}.zip"
            backup_path = config.BUNDLE_DIR / backup_name
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
        "schema_version": int(config.BUNDLE_META_SCHEMA_VERSION),
        "bundle_id": bundle_id,
        "sha256": hashlib.sha256(data).hexdigest(),
        "size_bytes": int(len(data)),
        "uploaded_at": utility.local_ts(),
        "path": str(target_path),
        "stored_as": target_path.name,
        "comment": "",
    }
    config.r.hset(config.KEY_BUNDLE_INDEX, bundle_id, json.dumps(meta, ensure_ascii=False))

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


@router.delete("/bootstrap/_bundle/{bundle_id}", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_bundle_delete(bundle_id: str) -> Dict[str, Any]:
    bundle_id = _validate_bundle_id(bundle_id)

    meta_s = config.r.hget(config.KEY_BUNDLE_INDEX, bundle_id)
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

    deleted_catalog = int(config.r.hdel(config.KEY_BUNDLE_INDEX, bundle_id)) if catalog_found else 0
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


@router.get("/bootstrap/bundle/{bundle_id}", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_bundle(bundle_id: str):
    bundle_id = _validate_bundle_id(bundle_id)

    meta_s = config.r.hget(config.KEY_BUNDLE_INDEX, bundle_id)
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


@router.post("/bootstrap/report/{scanner}", tags=["2 Bootstrap (Init/Update)"])
def bootstrap_report(scanner: str, report: BootstrapReport) -> Dict[str, Any]:
    m1Registry.require_whitelisted(scanner)
    meta_k = config.key_scanner_meta(scanner)
    now = utility.local_ts()
    config.r.hset(meta_k, mapping={
        "installed_version": report.installed_version,
        "last_bootstrap": now,
        "last_seen": now,
    })
    return {"status": "ok", "scanner": scanner, "installed_version": report.installed_version, "time": now}