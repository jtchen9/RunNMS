from __future__ import annotations

import sys
from pathlib import Path
ROOT_DIR=Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path: sys.path.insert(0,str(ROOT_DIR))
from src.common.prepared_loader import load_prepared_dataset, index_samples_by_uid

def validate_prepared_dataset(prepared_subdir: str, json_file_name: str="samples.json") -> None:
    path=ROOT_DIR/"data"/"prepared"/prepared_subdir/json_file_name
    payload=load_prepared_dataset(path); samples=payload["samples"]; indexed=index_samples_by_uid(samples)
    obs=sum(int(s.get("observation_count",0)) for s in samples)
    print("\n"+"="*72); print("Prepared Dataset Validation"); print("="*72)
    print(f"JSON file         : {path}"); print(f"samples           : {len(samples)}"); print(f"unique sample_uid : {len(indexed)}"); print(f"observations      : {obs}"); print("="*72+"\n")

if __name__ == "__main__":
    # PREPARED_SUBDIR="normal_diversity"
    PREPARED_SUBDIR="clean_core"
    validate_prepared_dataset(PREPARED_SUBDIR)
