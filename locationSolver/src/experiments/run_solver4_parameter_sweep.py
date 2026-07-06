from __future__ import annotations

import csv
import json
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
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _rank_key(row: Dict[str, Any]) -> tuple:
    """
    Lexicographic ranking, as agreed:

    1. joint 10cm/10deg rate      maximize
    2. solver success rate        maximize
    3. position p90               minimize
    4. position max               minimize
    5. heading p90                minimize
    6. total rejected tags        minimize

    Python sorts ascending, so maximization terms are negated.
    """
    return (
        -float(row["joint_within_10cm_10deg_rate"]),
        -float(row["solver_success_rate"]),
        float(row["position_p90_error_m"]),
        float(row["position_max_error_m"]),
        float(row["heading_p90_abs_error_deg"]),
        int(row["total_rejected_tags"]),
    )


def run_solver4_parameter_sweep(
    prepared_subdir: str,
    tag_map_path: Path,
    tag_yaw_map_path: Path,
    rejection_score_threshold_values: List[float],
    min_score_gap_values: List[float],
    max_rejections_values: List[int],
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

    # Freeze Solver #3 baseline while sweeping only robust parameters.
    base_config = DistanceAngleYawSolverConfig()

    combinations = list(
        product(
            rejection_score_threshold_values,
            min_score_gap_values,
            max_rejections_values,
        )
    )

    sweep_name = "solver4_robust_parameter_sweep"
    run_id = make_run_id(sweep_name, prepared_subdir)
    sweep_dir = ROOT_DIR / "output" / "sweeps" / run_id
    sweep_dir.mkdir(parents=True, exist_ok=False)

    comparison_rows: List[Dict[str, Any]] = []
    all_start = time.perf_counter()

    print("")
    print("=" * 78)
    print("Solver #4 Robust Parameter Sweep")
    print("=" * 78)
    print(f"Prepared dataset : {prepared_path}")
    print(f"Combinations     : {len(combinations)}")
    print(f"Output dir       : {sweep_dir}")
    print("=" * 78)
    print("")

    for combo_index, (
        rejection_score_threshold,
        min_score_gap,
        max_rejections,
    ) in enumerate(combinations, start=1):

        robust_config = RobustTwoPassConfig(
            rejection_score_threshold=float(rejection_score_threshold),
            min_score_gap=float(min_score_gap),
            max_rejections=int(max_rejections),
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

        samples_with_rejection = sum(
            1 for result in results
            if len(result.tags_rejected) > 0
        )
        total_rejected_tags = sum(
            len(result.tags_rejected) for result in results
        )

        row = {
            "combo_index": combo_index,

            "rejection_score_threshold": float(rejection_score_threshold),
            "min_score_gap": float(min_score_gap),
            "max_rejections": int(max_rejections),

            "solver_success_rate": summary["solver_success_rate"],

            "position_mean_error_m": summary["position_mean_error_m"],
            "position_median_error_m": summary["position_median_error_m"],
            "position_p90_error_m": summary["position_p90_error_m"],
            "position_p95_error_m": summary["position_p95_error_m"],
            "position_max_error_m": summary["position_max_error_m"],

            "heading_mean_abs_error_deg": summary["heading_mean_abs_error_deg"],
            "heading_median_abs_error_deg": summary["heading_median_abs_error_deg"],
            "heading_p90_abs_error_deg": summary["heading_p90_abs_error_deg"],
            "heading_p95_abs_error_deg": summary["heading_p95_abs_error_deg"],
            "heading_max_abs_error_deg": summary["heading_max_abs_error_deg"],

            "position_within_5cm_rate": summary["position_within_5cm_rate"],
            "position_within_10cm_rate": summary["position_within_10cm_rate"],
            "heading_within_5deg_rate": summary["heading_within_5deg_rate"],
            "heading_within_10deg_rate": summary["heading_within_10deg_rate"],

            "joint_within_10cm_10deg_rate": (
                summary["joint_within_10cm_10deg_rate"]
            ),
            "joint_within_20cm_20deg_rate": (
                summary["joint_within_20cm_20deg_rate"]
            ),

            "samples_with_rejection": samples_with_rejection,
            "total_rejected_tags": total_rejected_tags,

            "combo_runtime_s": combo_runtime_s,
        }

        comparison_rows.append(row)

        print(
            f"[{combo_index:02d}/{len(combinations):02d}] "
            f"thr={rejection_score_threshold:4.2f} "
            f"gap={min_score_gap:4.2f} "
            f"maxrej={max_rejections} | "
            f"joint={row['joint_within_10cm_10deg_rate']:.3f} "
            f"p90={row['position_p90_error_m']:.4f}m "
            f"max={row['position_max_error_m']:.4f}m "
            f"h90={row['heading_p90_abs_error_deg']:.3f}deg "
            f"rej={total_rejected_tags}"
        )

    total_runtime_s = time.perf_counter() - all_start

    ranked_rows = sorted(comparison_rows, key=_rank_key)

    for rank, row in enumerate(ranked_rows, start=1):
        row["rank"] = rank

    # Put rank first in ranked CSV.
    ranked_rows_for_csv = []
    for row in ranked_rows:
        ordered = {"rank": row["rank"]}
        ordered.update({
            k: v for k, v in row.items()
            if k != "rank"
        })
        ranked_rows_for_csv.append(ordered)

    _write_csv(
        sweep_dir / "sweep_results_unsorted.csv",
        comparison_rows,
    )
    _write_csv(
        sweep_dir / "sweep_results_ranked.csv",
        ranked_rows_for_csv,
    )

    best = ranked_rows[0]

    best_config = {
        "prepared_subdir": prepared_subdir,
        "base_solver_config": asdict(base_config),
        "best_robust_parameters": {
            "rejection_score_threshold": best["rejection_score_threshold"],
            "min_score_gap": best["min_score_gap"],
            "max_rejections": best["max_rejections"],
        },
        "best_metrics": {
            "joint_within_10cm_10deg_rate": (
                best["joint_within_10cm_10deg_rate"]
            ),
            "solver_success_rate": best["solver_success_rate"],
            "position_p90_error_m": best["position_p90_error_m"],
            "position_max_error_m": best["position_max_error_m"],
            "heading_p90_abs_error_deg": best["heading_p90_abs_error_deg"],
            "total_rejected_tags": best["total_rejected_tags"],
        },
        "ranking_rule": [
            "maximize joint_within_10cm_10deg_rate",
            "maximize solver_success_rate",
            "minimize position_p90_error_m",
            "minimize position_max_error_m",
            "minimize heading_p90_abs_error_deg",
            "minimize total_rejected_tags",
        ],
    }

    write_json(
        sweep_dir / "best_config.json",
        best_config,
    )

    # Save top 10 separately for quick inspection.
    _write_csv(
        sweep_dir / "top10_configs.csv",
        ranked_rows_for_csv[:10],
    )

    sweep_manifest = {
        "run_id": run_id,
        "sweep_name": sweep_name,
        "prepared_subdir": prepared_subdir,
        "prepared_json": str(prepared_path),
        "tag_map_path": str(tag_map_path),
        "tag_yaw_map_path": str(tag_yaw_map_path),
        "combination_count": len(combinations),
        "rejection_score_threshold_values": rejection_score_threshold_values,
        "min_score_gap_values": min_score_gap_values,
        "max_rejections_values": max_rejections_values,
        "total_runtime_s": total_runtime_s,
        "output_dir": str(sweep_dir),
        "ranking_rule": best_config["ranking_rule"],
    }

    write_manifest(
        sweep_dir / "run_manifest.json",
        sweep_manifest,
    )

    print("")
    print("=" * 78)
    print("Sweep Complete")
    print("=" * 78)
    print(f"Total runtime     : {total_runtime_s:.2f} s")
    print(f"Ranked results    : {sweep_dir / 'sweep_results_ranked.csv'}")
    print(f"Top 10            : {sweep_dir / 'top10_configs.csv'}")
    print(f"Best config       : {sweep_dir / 'best_config.json'}")
    print("")
    print("Best parameters:")
    print(
        f"  rejection_score_threshold = "
        f"{best['rejection_score_threshold']}"
    )
    print(f"  min_score_gap             = {best['min_score_gap']}")
    print(f"  max_rejections            = {best['max_rejections']}")
    print("")
    print("Best metrics:")
    print(
        f"  joint 10cm/10deg = "
        f"{best['joint_within_10cm_10deg_rate']:.3f}"
    )
    print(
        f"  position p90     = "
        f"{best['position_p90_error_m']:.6f} m"
    )
    print(
        f"  position max     = "
        f"{best['position_max_error_m']:.6f} m"
    )
    print(
        f"  heading p90      = "
        f"{best['heading_p90_abs_error_deg']:.6f} deg"
    )
    print(
        f"  rejected tags    = "
        f"{best['total_rejected_tags']}"
    )
    print("=" * 78)
    print("")


if __name__ == "__main__":
    # ------------------------------------------------------------------
    # Change only these parameters, then press Run in VS Code.
    # ------------------------------------------------------------------

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

    # Brutal Stage-1 sweep: 5 x 5 x 2 = 50 combinations.
    REJECTION_SCORE_THRESHOLD_VALUES = [
        2.0,
        2.5,
        3.0,
        3.5,
        4.0,
    ]

    MIN_SCORE_GAP_VALUES = [
        0.25,
        0.50,
        0.75,
        1.00,
        1.50,
    ]

    MAX_REJECTIONS_VALUES = [
        0,
        1,
    ]

    run_solver4_parameter_sweep(
        prepared_subdir=PREPARED_SUBDIR,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
        rejection_score_threshold_values=REJECTION_SCORE_THRESHOLD_VALUES,
        min_score_gap_values=MIN_SCORE_GAP_VALUES,
        max_rejections_values=MAX_REJECTIONS_VALUES,
    )
