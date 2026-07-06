from __future__ import annotations
import datetime as dt, json
from pathlib import Path
from typing import Any, Dict

def make_run_id(solver_name: str, dataset_name: str) -> str:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{ts}_{solver_name}_{dataset_name}"

def write_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
