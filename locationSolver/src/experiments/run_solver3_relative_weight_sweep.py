from __future__ import annotations

import csv
import sys
import time
from dataclasses import asdict
from itertools import product
from pathlib import Path
from typing import Any, Dict, List


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from src.common.evaluator import evaluate_solver_results
from src.common.prepared_loader import load_prepared_dataset
from src.common.run_manifest import make_run_id, write_manifest
from src.common.run_output import write_json
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
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _rank_key(row: Dict[str, Any]) -> tuple:
    """
    Preserve the same ranking rule used in prior sweeps.
    """
    return (
        -float(row["joint_within_10cm_10deg_rate"]),
        -float(row["solver_success_rate"]),
        float(row["position_p90_error_m"]),
        float(row["position_max_error_m"]),
        float(row["heading_p90_abs_error_deg"]),
        int(row["total_rejected_tags"]),
    )


def run_relative_weight_sweep(
    prepared_subdir: str,
    tag_map_path: Path,
    tag_yaw_map_path: Path,
    distance_weight_values: List[float],
    angle_weight_values: List[float],
    yaw_weight_values: List[float],
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

    # Stage 12B: freeze the selected, physically plausible scale point.
    fixed_scales = {
        "distance_sigma_m": 0.05,
        "angle_sigma_deg": 2.5,
        "yaw_sigma_deg": 3.0,
    }

    # Keep the intuitive robust rule frozen.
    robust_config = RobustTwoPassConfig(
        rejection_score_threshold=0.80,
        min_score_gap=0.50,
        max_rejections=1,
    )

    combinations = list(product(
        distance_weight_values,
        angle_weight_values,
        yaw_weight_values,
    ))

    run_id = make_run_id(
        "solver3_relative_weight_sweep_fixed_scales",
        prepared_subdir,
    )
    out_dir = ROOT_DIR / "output" / "sweeps" / run_id
    out_dir.mkdir(parents=True, exist_ok=False)

    rows: List[Dict[str, Any]] = []
    start_all = time.perf_counter()

    print("")
    print("=" * 92)
    print("Solver #3 Relative-Weight Sweep with Fixed Scales and Robust Rule")
    print("=" * 92)
    print(f"Prepared dataset : {prepared_path}")
    print(f"Combinations     : {len(combinations)}")
    print(f"Output dir       : {out_dir}")
    print("")
    print("Fixed scales:")
    print("  distance_sigma_m = 0.05")
    print("  angle_sigma_deg  = 2.5")
    print("  yaw_sigma_deg    = 3.0")
    print("")
    print("Fixed robust rule:")
    print("  threshold         = 0.80")
    print("  min_score_gap     = 0.50")
    print("  max_rejections    = 1")
    print("=" * 92)
    print("")

    for idx, (
        distance_weight,
        angle_weight,
        yaw_weight,
    ) in enumerate(combinations, start=1):

        base_config = DistanceAngleYawSolverConfig(
            distance_sigma_m=fixed_scales["distance_sigma_m"],
            angle_sigma_deg=fixed_scales["angle_sigma_deg"],
            yaw_sigma_deg=fixed_scales["yaw_sigma_deg"],

            distance_global_weight=float(distance_weight),
            angle_global_weight=float(angle_weight),
            yaw_global_weight=float(yaw_weight),
        )

        results = []
        combo_start = time.perf_counter()

        for sample in samples:
            result, _ = solve_robust_twopass(
                sample=sample,
                tag_pose_map=tag_pose_map,
                base_config=base_config,
                robust_config=robust_config,
            )
            results.append(result)

        combo_runtime_s = time.perf_counter() - combo_start

        _, summary = evaluate_solver_results(samples, results)

        rejected_samples = sum(
            1 for result in results
            if len(result.tags_rejected) > 0
        )
        rejected_tags = sum(
            len(result.tags_rejected)
            for result in results
        )

        row = {
            "combo_index": idx,

            "distance_sigma_m": fixed_scales["distance_sigma_m"],
            "angle_sigma_deg": fixed_scales["angle_sigma_deg"],
            "yaw_sigma_deg": fixed_scales["yaw_sigma_deg"],

            "distance_global_weight": float(distance_weight),
            "angle_global_weight": float(angle_weight),
            "yaw_global_weight": float(yaw_weight),

            "robust_rejection_score_threshold":
                robust_config.rejection_score_threshold,
            "robust_min_score_gap":
                robust_config.min_score_gap,
            "robust_max_rejections":
                robust_config.max_rejections,

            "solver_success_rate": summary["solver_success_rate"],

            "position_mean_error_m": summary["position_mean_error_m"],
            "position_median_error_m": summary["position_median_error_m"],
            "position_p90_error_m": summary["position_p90_error_m"],
            "position_p95_error_m": summary["position_p95_error_m"],
            "position_max_error_m": summary["position_max_error_m"],

            "heading_mean_abs_error_deg":
                summary["heading_mean_abs_error_deg"],
            "heading_median_abs_error_deg":
                summary["heading_median_abs_error_deg"],
            "heading_p90_abs_error_deg":
                summary["heading_p90_abs_error_deg"],
            "heading_p95_abs_error_deg":
                summary["heading_p95_abs_error_deg"],
            "heading_max_abs_error_deg":
                summary["heading_max_abs_error_deg"],

            "position_within_5cm_rate":
                summary["position_within_5cm_rate"],
            "position_within_10cm_rate":
                summary["position_within_10cm_rate"],
            "heading_within_5deg_rate":
                summary["heading_within_5deg_rate"],
            "heading_within_10deg_rate":
                summary["heading_within_10deg_rate"],

            "joint_within_10cm_10deg_rate":
                summary["joint_within_10cm_10deg_rate"],

            "samples_with_rejection": rejected_samples,
            "total_rejected_tags": rejected_tags,

            "combo_runtime_s": combo_runtime_s,
        }

        rows.append(row)

        print(
            f"[{idx:03d}/{len(combinations):03d}] "
            f"wd={distance_weight:4.2f} "
            f"wa={angle_weight:4.2f} "
            f"wy={yaw_weight:4.2f} | "
            f"joint={row['joint_within_10cm_10deg_rate']:.3f} "
            f"p90={row['position_p90_error_m']:.4f}m "
            f"max={row['position_max_error_m']:.4f}m "
            f"h90={row['heading_p90_abs_error_deg']:.3f}deg "
            f"rej={rejected_tags}"
        )

    total_runtime_s = time.perf_counter() - start_all

    ranked = sorted(rows, key=_rank_key)

    ranked_rows = []
    for rank, row in enumerate(ranked, start=1):
        out = {"rank": rank}
        out.update(row)
        ranked_rows.append(out)

    _write_csv(
        out_dir / "sweep_results_unsorted.csv",
        rows,
    )
    _write_csv(
        out_dir / "sweep_results_ranked.csv",
        ranked_rows,
    )
    _write_csv(
        out_dir / "top20_configs.csv",
        ranked_rows[:20],
    )

    best = ranked_rows[0]

    write_json(
        out_dir / "best_config.json",
        {
            "prepared_subdir": prepared_subdir,
            "fixed_measurement_scales": fixed_scales,
            "best_relative_weights": {
                "distance_global_weight":
                    best["distance_global_weight"],
                "angle_global_weight":
                    best["angle_global_weight"],
                "yaw_global_weight":
                    best["yaw_global_weight"],
            },
            "fixed_robust_rule": asdict(robust_config),
            "best_metrics": {
                "joint_within_10cm_10deg_rate":
                    best["joint_within_10cm_10deg_rate"],
                "solver_success_rate":
                    best["solver_success_rate"],
                "position_mean_error_m":
                    best["position_mean_error_m"],
                "position_median_error_m":
                    best["position_median_error_m"],
                "position_p90_error_m":
                    best["position_p90_error_m"],
                "position_p95_error_m":
                    best["position_p95_error_m"],
                "position_max_error_m":
                    best["position_max_error_m"],
                "heading_mean_abs_error_deg":
                    best["heading_mean_abs_error_deg"],
                "heading_p90_abs_error_deg":
                    best["heading_p90_abs_error_deg"],
                "heading_p95_abs_error_deg":
                    best["heading_p95_abs_error_deg"],
                "heading_max_abs_error_deg":
                    best["heading_max_abs_error_deg"],
                "samples_with_rejection":
                    best["samples_with_rejection"],
                "total_rejected_tags":
                    best["total_rejected_tags"],
            },
            "ranking_rule": [
                "maximize joint_within_10cm_10deg_rate",
                "maximize solver_success_rate",
                "minimize position_p90_error_m",
                "minimize position_max_error_m",
                "minimize heading_p90_abs_error_deg",
                "minimize total_rejected_tags",
            ],
        },
    )

    write_manifest(
        out_dir / "run_manifest.json",
        {
            "run_id": run_id,
            "sweep_name":
                "solver3_relative_weight_sweep_fixed_scales",
            "prepared_subdir": prepared_subdir,
            "prepared_json": str(prepared_path),
            "combination_count": len(combinations),
            "distance_weight_values": distance_weight_values,
            "angle_weight_values": angle_weight_values,
            "yaw_weight_values": yaw_weight_values,
            "fixed_measurement_scales": fixed_scales,
            "fixed_robust_rule": asdict(robust_config),
            "total_runtime_s": total_runtime_s,
            "output_dir": str(out_dir),
        },
    )

    print("")
    print("=" * 92)
    print("Relative-Weight Sweep Complete")
    print("=" * 92)
    print(f"Total runtime : {total_runtime_s:.2f} s")
    print(f"Output dir    : {out_dir}")
    print("")
    print("Best relative weights:")
    print(
        f"  distance = "
        f"{best['distance_global_weight']}"
    )
    print(
        f"  angle    = "
        f"{best['angle_global_weight']}"
    )
    print(
        f"  yaw      = "
        f"{best['yaw_global_weight']}"
    )
    print("")
    print("Best metrics:")
    print(
        f"  joint 10/10   = "
        f"{best['joint_within_10cm_10deg_rate']:.3f}"
    )
    print(
        f"  position p90  = "
        f"{best['position_p90_error_m']:.6f} m"
    )
    print(
        f"  position max  = "
        f"{best['position_max_error_m']:.6f} m"
    )
    print(
        f"  heading p90   = "
        f"{best['heading_p90_abs_error_deg']:.6f} deg"
    )
    print(
        f"  rejected tags = "
        f"{best['total_rejected_tags']}"
    )
    print("=" * 92)
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

    # Stage 12B: targeted local sweep, 3 x 4 x 4 = 48 combinations.
    DISTANCE_WEIGHT_VALUES = [
        0.75,
        1.00,
        1.25,
    ]

    ANGLE_WEIGHT_VALUES = [
        0.75,
        1.00,
        1.25,
        1.50,
    ]

    YAW_WEIGHT_VALUES = [
        0.25,
        0.50,
        0.75,
        1.00,
    ]

    run_relative_weight_sweep(
        prepared_subdir=PREPARED_SUBDIR,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
        distance_weight_values=DISTANCE_WEIGHT_VALUES,
        angle_weight_values=ANGLE_WEIGHT_VALUES,
        yaw_weight_values=YAW_WEIGHT_VALUES,
    )
