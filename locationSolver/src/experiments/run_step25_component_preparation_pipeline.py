from __future__ import annotations

import csv
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.common.component_preparation import (
    FirstScreenConfig,
    M2DistanceScreenConfig,
    YawAdmissionConfig,
    compute_pass1_component_rows,
    decide_distance_use_from_m2,
    first_screen_raw_rows,
    prepare_final_componentwise_sample,
)
from src.common.dataset_builder import build_prepared_samples
from src.common.run_manifest import make_run_id, write_manifest
from src.common.run_output import write_json
from src.common.tag_map_loader import (
    build_tag_pose_map,
    load_tag_xy_map,
    load_tag_yaw_json,
)
from src.solvers.solver_2_distance_angle import (
    DistanceAngleSolverConfig,
    solve_distance_angle,
)


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fields: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fields.append(key)

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=fields,
            extrasaction="ignore",
        )
        w.writeheader()
        w.writerows(rows)


def run_pipeline_to_final_input(
    raw_csv_path: Path,
    tag_map_path: Path,
    tag_yaw_map_path: Path,
) -> None:
    raw_rows = _read_csv(raw_csv_path)

    first_config = FirstScreenConfig()
    m2_config = M2DistanceScreenConfig()
    yaw_config = YawAdmissionConfig()

    # Exact original Solver #2 defaults.
    pass1_config = DistanceAngleSolverConfig()

    survivors, first_audit = first_screen_raw_rows(
        raw_rows=raw_rows,
        config=first_config,
    )

    # Converts survivors to the unchanged Solver #2 input structure.
    samples = build_prepared_samples(survivors)

    tag_xy_map = load_tag_xy_map(tag_map_path)
    tag_yaw_map = load_tag_yaw_json(tag_yaw_map_path)
    tag_pose_map = build_tag_pose_map(
        tag_xy_map,
        tag_yaw_map,
    )

    pass1_results = []
    component_rows_all: List[Dict[str, Any]] = []
    final_samples = []
    final_audit_all: List[Dict[str, Any]] = []

    for sample in samples:
        pass1 = solve_distance_angle(
            sample=sample,
            tag_xy_map=tag_xy_map,
            config=pass1_config,
        )
        pass1_results.append(pass1)

        if not pass1.success:
            continue

        component_rows = compute_pass1_component_rows(
            sample=sample,
            pass1=pass1,
            tag_xy_map=tag_xy_map,
            solver_config=pass1_config,
        )
        component_rows_all.extend(component_rows)

        distance_decisions = decide_distance_use_from_m2(
            component_rows=component_rows,
            config=m2_config,
        )

        final_sample, final_audit = (
            prepare_final_componentwise_sample(
                sample=sample,
                pass1=pass1,
                distance_decisions_by_uid=distance_decisions,
                tag_pose_map=tag_pose_map,
                yaw_config=yaw_config,
            )
        )

        final_samples.append(final_sample)
        final_audit_all.extend(final_audit)

    run_id = make_run_id(
        "step25_component_prepare",
        "raw_pipeline",
    )
    out_dir = ROOT_DIR / "output" / "diagnostics" / run_id
    out_dir.mkdir(parents=True, exist_ok=False)

    _write_csv(
        out_dir / "first_screen_audit.csv",
        first_audit,
    )
    _write_csv(
        out_dir / "pass1_component_metrics.csv",
        component_rows_all,
    )
    _write_csv(
        out_dir / "final_component_decisions.csv",
        final_audit_all,
    )

    # Exact JSON payload to be consumed next by the new final
    # component-wise D/A/Y solver.
    payload = {
        "pipeline_stage": "ready_for_final_componentwise_solver",
        "samples": final_samples,
    }
    (out_dir / "final_componentwise_samples.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    write_json(
        out_dir / "run_config.json",
        {
            "raw_csv_path": str(raw_csv_path),
            "tag_map_path": str(tag_map_path),
            "tag_yaw_map_path": str(tag_yaw_map_path),

            "first_screen_config": asdict(first_config),
            "pass1_solver_config": asdict(pass1_config),
            "m2_distance_screen_config": asdict(m2_config),
            "yaw_admission_config": asdict(yaw_config),

            "architecture": [
                "raw whole-tag first screen",
                "unchanged Solver #2 no-yaw Pass 1",
                "direct M2 distance rejection",
                "no second-stage angle screening",
                "Stage-5-style GT-free yaw keep/flip/reject",
                "prepare final component-wise D/A/Y input",
            ],

            "important": {
                "middle_holdout_solver": False,
                "gt_used_in_estimation": False,
                "old_truth_assisted_yaw_path_disabled": True,
                "legacy_nonpositive_DA_weights_repaired_after_new_first_screen": True,
            },
        },
    )

    write_manifest(
        out_dir / "run_manifest.json",
        {
            "run_id": run_id,
            "raw_observation_count": len(raw_rows),
            "first_screen_survivor_count": len(survivors),
            "prepared_sample_count": len(samples),
            "pass1_success_count": sum(
                1 for r in pass1_results if r.success
            ),
            "final_prepared_sample_count": len(final_samples),
            "distance_rejected_count": sum(
                1 for row in final_audit_all
                if not bool(row["distance_use"])
            ),
            "angle_rejected_count": sum(
                1 for row in final_audit_all
                if not bool(row["angle_use"])
            ),
            "yaw_admitted_count": sum(
                1 for row in final_audit_all
                if bool(row["yaw_use"])
            ),
            "output_dir": str(out_dir),
        },
    )

    print("")
    print("=" * 94)
    print("STEP 25: COMPONENT PREPARATION PIPELINE")
    print("=" * 94)
    print(f"Raw observations         : {len(raw_rows)}")
    print(f"First-screen survivors   : {len(survivors)}")
    print(f"Prepared samples         : {len(samples)}")
    print(
        f"Pass-1 success           : "
        f"{sum(1 for r in pass1_results if r.success)}/{len(samples)}"
    )
    print(
        f"Distance components cut  : "
        f"{sum(1 for r in final_audit_all if not bool(r['distance_use']))}"
    )
    print(
        f"Angle components cut     : "
        f"{sum(1 for r in final_audit_all if not bool(r['angle_use']))}"
    )
    print(
        f"Yaw components admitted  : "
        f"{sum(1 for r in final_audit_all if bool(r['yaw_use']))}"
    )
    print(f"Output directory         : {out_dir}")
    print("")
    print("Final-solver input:")
    print("  final_componentwise_samples.json")
    print("=" * 94)
    print("")


if __name__ == "__main__":
    RAW_CSV_PATH = (
        ROOT_DIR
        / "data"
        / "input"
        / "all_observations_long.csv"
    )

    TAG_MAP_PATH = (
        ROOT_DIR.parent
        / "sitemap"
        / "DemoRoom"
        / "tag_location.txt"
    )

    TAG_YAW_MAP_PATH = (
        ROOT_DIR
        / "config"
        / "datasets"
        / "demoroom_tag_yaw_v1.json"
    )

    run_pipeline_to_final_input(
        raw_csv_path=RAW_CSV_PATH,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
    )
