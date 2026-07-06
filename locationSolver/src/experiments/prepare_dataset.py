from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from src.common.data_loader import load_csv_rows
from src.common.dataset_builder import build_prepared_samples


def prepare_dataset(
    input_csv_file: str,
    output_subdir: str,
    output_json_file: str = "samples.json",
) -> None:
    input_path = ROOT_DIR / "data" / "input" / input_csv_file
    output_dir = ROOT_DIR / "data" / "prepared" / output_subdir
    output_path = output_dir / output_json_file

    rows = load_csv_rows(input_path)
    samples = build_prepared_samples(rows)

    output_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "input_csv": str(input_path),
        "sample_count": len(samples),
        "observation_count": sum(
            int(sample["observation_count"]) for sample in samples
        ),
        "samples": samples,
    }

    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("")
    print("=" * 72)
    print("Location Solver Dataset Preparation")
    print("=" * 72)
    print(f"ROOT_DIR          : {ROOT_DIR}")
    print(f"Input CSV         : {input_path}")
    print(f"Output JSON       : {output_path}")
    print(f"Samples prepared  : {payload['sample_count']}")
    print(f"Observations      : {payload['observation_count']}")
    print("=" * 72)
    print("")


if __name__ == "__main__":
    # ------------------------------------------------------------------
    # Change only these parameters, then press Run in VS Code.
    # ------------------------------------------------------------------

    # INPUT_CSV_FILE = "normal_diversity_v1.csv"
    # OUTPUT_SUBDIR = "normal_diversity"

    # Alternative:
    INPUT_CSV_FILE = "clean_core_v1.csv"
    OUTPUT_SUBDIR = "clean_core"

    prepare_dataset(
        input_csv_file=INPUT_CSV_FILE,
        output_subdir=OUTPUT_SUBDIR,
    )
