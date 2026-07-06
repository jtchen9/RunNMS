from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

def load_prepared_dataset(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Prepared dataset not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if "samples" not in payload or not isinstance(payload["samples"], list):
        raise ValueError(f"Invalid prepared dataset: missing list field 'samples': {path}")
    return payload

def index_samples_by_uid(samples: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for sample in samples:
        uid = str(sample.get("sample_uid") or "").strip()
        if not uid:
            raise ValueError(f"Prepared sample missing sample_uid: {sample}")
        if uid in out:
            raise ValueError(f"Duplicate sample_uid in prepared dataset: {uid}")
        out[uid] = sample
    return out
