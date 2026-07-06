from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.data_loader import group_rows_by_sample, load_csv_rows


def inspect_dataset(csv_file_name: str) -> None:
    csv_path = ROOT_DIR / "data" / "input" / csv_file_name

    rows = load_csv_rows(csv_path)
    grouped = group_rows_by_sample(rows)
    counts = sorted(len(v) for v in grouped.values())

    print("")
    print("=" * 72)
    print("Location Solver Dataset Inspection")
    print("=" * 72)
    print(f"ROOT_DIR        : {ROOT_DIR}")
    print(f"CSV file        : {csv_path}")
    print(f"observations    : {len(rows)}")
    print(f"samples         : {len(grouped)}")
    print(f"min tags/sample : {counts[0] if counts else 0}")
    print(f"max tags/sample : {counts[-1] if counts else 0}")
    print(f"mean tags/sample: {(sum(counts) / len(counts)) if counts else 0:.3f}")
    print("=" * 72)
    print("")

    print("Per-sample observation counts:")
    print("-" * 72)
    for sample_uid in sorted(grouped):
        print(f"{sample_uid:24s} {len(grouped[sample_uid]):3d}")
    print("-" * 72)


if __name__ == "__main__":
    # INPUT_CSV_FILE = "normal_diversity_v1.csv"

    # Alternative:
    INPUT_CSV_FILE = "clean_core_v1.csv"

    inspect_dataset(INPUT_CSV_FILE)
