from __future__ import annotations

import csv
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List


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
)
from src.solvers.solver_4_robust_twopass import (
    RobustTwoPassConfig,
    solve_robust_twopass,
)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fields,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def run_candidate_v1(
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
    tag_pose_map = build_tag_pose_map(
        tag_xy_map,
        tag_yaw_map,
    )

    # Frozen candidate v1 configuration.
    base_config = DistanceAngleYawSolverConfig(
        distance_sigma_m=0.05,
        angle_sigma_deg=2.5,
        yaw_sigma_deg=3.0,

        distance_global_weight=1.0,
        angle_global_weight=1.0,
        yaw_global_weight=0.75,
    )

    robust_config = RobustTwoPassConfig(
        rejection_score_threshold=0.80,
        min_score_gap=0.50,
        max_rejections=1,
    )

    results = []
    residual_rows: List[Dict[str, Any]] = []

    for sample in samples:
        result, rows = solve_robust_twopass(
            sample=sample,
            tag_pose_map=tag_pose_map,
            base_config=base_config,
            robust_config=robust_config,
        )
        results.append(result)

        for row in rows:
            out = dict(row)
            out["sample_uid"] = sample["sample_uid"]
            residual_rows.append(out)

    sample_rows, summary = evaluate_solver_results(
        samples,
        results,
    )

    run_id = make_run_id(
        "solver4_candidate_v1",
        prepared_subdir,
    )
    out_dir = ROOT_DIR / "output" / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=False)

    rejected_sample_count = sum(
        1 for r in results
        if len(r.tags_rejected) > 0
    )
    total_rejected_tags = sum(
        len(r.tags_rejected)
        for r in results
    )

    write_json(
        out_dir / "run_config.json",
        {
            "candidate_name": "solver4_candidate_v1",
            "prepared_subdir": prepared_subdir,
            "prepared_json": str(prepared_path),
            "tag_map_path": str(tag_map_path),
            "tag_yaw_map_path": str(tag_yaw_map_path),
            "base_solver_config": asdict(base_config),
            "robust_config": asdict(robust_config),
            "important_note": (
                "Yaw acceptance/sign still depends on current offline "
                "diagnostic labels. This is not yet production-safe. "
                "Robust rejection itself uses pass-1 residuals, not GT."
            ),
        },
    )

    write_json(
        out_dir / "summary_metrics.json",
        summary,
    )

    write_rows_csv(
        out_dir / "sample_results.csv",
        sample_rows,
    )

    _write_csv(
        out_dir / "observation_residuals.csv",
        residual_rows,
    )

    rejected_rows = [
        row for row in residual_rows
        if bool(row.get("rejected"))
    ]
    _write_csv(
        out_dir / "rejected_observations.csv",
        rejected_rows,
    )

    write_manifest(
        out_dir / "run_manifest.json",
        {
            "run_id": run_id,
            "candidate_name": "solver4_candidate_v1",
            "prepared_subdir": prepared_subdir,
            "prepared_json": str(prepared_path),
            "output_dir": str(out_dir),
            "sample_count": len(samples),
            "result_count": len(results),
            "solver_success_count": sum(
                1 for r in results if r.success
            ),
            "rejected_sample_count": rejected_sample_count,
            "total_rejected_tags": total_rejected_tags,
        },
    )

    print("")
    print("=" * 84)
    print("Solver #4 Candidate v1 - Frozen Configuration Run")
    print("=" * 84)
    print(f"Prepared dataset : {prepared_path}")
    print(f"Output directory : {out_dir}")
    print("")
    print("Frozen measurement model:")
    print("  distance sigma  = 0.05 m")
    print("  angle sigma     = 2.5 deg")
    print("  yaw sigma       = 3.0 deg")
    print("  distance weight = 1.0")
    print("  angle weight    = 1.0")
    print("  yaw weight      = 0.75")
    print("")
    print("Frozen robust rule:")
    print("  threshold       = 0.80")
    print("  min score gap   = 0.50")
    print("  max rejections  = 1")
    print("")
    print(f"Success count     : {sum(1 for r in results if r.success)} / {len(results)}")
    print(f"Rejected samples  : {rejected_sample_count}")
    print(f"Rejected tags     : {total_rejected_tags}")
    print("")
    print("Metrics:")
    print(
        f"  position mean   = "
        f"{summary['position_mean_error_m']:.6f} m"
    )
    print(
        f"  position p90    = "
        f"{summary['position_p90_error_m']:.6f} m"
    )
    print(
        f"  position max    = "
        f"{summary['position_max_error_m']:.6f} m"
    )
    print(
        f"  heading p90     = "
        f"{summary['heading_p90_abs_error_deg']:.6f} deg"
    )
    print(
        f"  joint 10/10     = "
        f"{summary['joint_within_10cm_10deg_rate']:.3f}"
    )
    print("=" * 84)
    print("")


if __name__ == "__main__":
    PREPARED_SUBDIR = "normal_diversity"

    # Independent validation later:
    # PREPARED_SUBDIR = "validation_v1"

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

    run_candidate_v1(
        prepared_subdir=PREPARED_SUBDIR,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
    )
