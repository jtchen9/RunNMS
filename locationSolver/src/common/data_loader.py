from __future__ import annotations
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

@dataclass(frozen=True)
class DatasetSpec:
    name: str
    csv_path: Path

def project_root() -> Path:
    return Path(__file__).resolve().parents[2]

def default_dataset_specs() -> Dict[str, DatasetSpec]:
    root = project_root()
    d = root / "data" / "input"
    return {
        "normal_diversity_v1": DatasetSpec("normal_diversity_v1", d / "normal_diversity_v1.csv"),
        "clean_core_v1": DatasetSpec("clean_core_v1", d / "clean_core_v1.csv"),
    }

def load_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def load_dataset(name: str) -> List[Dict[str, str]]:
    specs = default_dataset_specs()
    if name not in specs:
        raise KeyError(f"Unknown dataset {name!r}; known={sorted(specs)}")
    return load_csv_rows(specs[name].csv_path)

def group_rows_by_sample(
    rows: Iterable[Dict[str, str]],
    sample_field: str = "sample_uid",
) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        uid = str(row.get(sample_field) or "").strip()
        if not uid:
            raise ValueError(f"Missing {sample_field!r} in row: {row}")
        out.setdefault(uid, []).append(row)
    return out
