from __future__ import annotations

import sys
from dataclasses import asdict
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from src.common.evaluator import evaluate_solver_results
from src.common.prepared_loader import load_prepared_dataset
from src.common.run_manifest import make_run_id, write_manifest
from src.common.run_output import write_json, write_rows_csv
from src.common.tag_map_loader import (
    build_tag_pose_map,
    load_tag_xy_map,
    load_tag_yaw_json,
)
from src.solvers.solver_3_distance_angle_yaw import (
    DistanceAngleYawSolverConfig,
    solve_distance_angle_yaw,
)


def run_solver3_distance_angle_yaw(
    prepared_subdir: str,
    tag_map_path: Path,
    tag_yaw_map_path: Path,
) -> None:
    prepared_path = (
        ROOT_DIR
        / "data"
        / "prepared"
        / prepared_subdir
        / "samples.json"
    )

    payload = load_prepared_dataset(prepared_path)
    samples = payload["samples"]

    tag_xy_map = load_tag_xy_map(tag_map_path)
    tag_yaw_map = load_tag_yaw_json(tag_yaw_map_path)
    tag_pose_map = build_tag_pose_map(tag_xy_map, tag_yaw_map)

    config = DistanceAngleYawSolverConfig()

    results = [
        solve_distance_angle_yaw(
            sample=sample,
            tag_pose_map=tag_pose_map,
            config=config,
        )
        for sample in samples
    ]

    sample_rows, summary = evaluate_solver_results(samples, results)

    dataset_name = prepared_subdir
    solver_name = "solver3_distance_angle_yaw"

    run_id = make_run_id(solver_name, dataset_name)
    run_dir = ROOT_DIR / "output" / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    run_config = {
        "solver_name": solver_name,
        "dataset_name": dataset_name,
        "prepared_json": str(prepared_path),
        "tag_map_path": str(tag_map_path),
        "tag_yaw_map_path": str(tag_yaw_map_path),
        "solver_config": asdict(config),
        "important_note": (
            "Yaw acceptance/sign currently uses offline diagnostic labels "
            "from prepared data; this is not yet production-safe gating."
        ),
    }

    write_json(run_dir / "run_config.json", run_config)
    write_json(run_dir / "summary_metrics.json", summary)
    write_rows_csv(run_dir / "sample_results.csv", sample_rows)

    failures = [row for row in sample_rows if not bool(row["success"])]
    write_rows_csv(run_dir / "failures.csv", failures)

    manifest = {
        "run_id": run_id,
        "solver_name": solver_name,
        "dataset_name": dataset_name,
        "prepared_json": str(prepared_path),
        "tag_map_path": str(tag_map_path),
        "tag_yaw_map_path": str(tag_yaw_map_path),
        "output_dir": str(run_dir),
        "sample_count": len(samples),
        "result_count": len(results),
    }
    write_manifest(run_dir / "run_manifest.json", manifest)

    print("")
    print("=" * 72)
    print("Solver #3 - Distance + Angle + Conditional Yaw")
    print("=" * 72)
    print(f"Prepared dataset : {prepared_path}")
    print(f"Tag XY map       : {tag_map_path}")
    print(f"Tag yaw map      : {tag_yaw_map_path}")
    print(f"Run output       : {run_dir}")
    print(f"Samples          : {len(samples)}")
    print(f"Success rate     : {summary['solver_success_rate']:.3f}")
    print(f"Position median  : {summary['position_median_error_m']}")
    print(f"Position p90     : {summary['position_p90_error_m']}")
    print(f"Heading median   : {summary['heading_median_abs_error_deg']}")
    print(f"Heading p90      : {summary['heading_p90_abs_error_deg']}")
    print(
        f"Joint 10cm/10deg : "
        f"{summary['joint_within_10cm_10deg_rate']:.3f}"
    )
    print("=" * 72)
    print("")


if __name__ == "__main__":
    # PREPARED_SUBDIR = "normal_diversity"

    # Alternative:
    PREPARED_SUBDIR = "clean_core"

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

    run_solver3_distance_angle_yaw(
        prepared_subdir=PREPARED_SUBDIR,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
    )
