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
    Primary ranking for development comparison.

    1. maximize joint 10cm/10deg rate
    2. maximize solver success rate
    3. minimize position p90
    4. minimize position max
    5. minimize heading p90
    6. minimize total rejected tags
    """
    return (
        -float(row["joint_within_10cm_10deg_rate"]),
        -float(row["solver_success_rate"]),
        float(row["position_p90_error_m"]),
        float(row["position_max_error_m"]),
        float(row["heading_p90_abs_error_deg"]),
        int(row["total_rejected_tags"]),
    )


def run_refined_solver4_sweep(
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

    # Freeze Solver #3 baseline; sweep only robust rejection parameters.
    base_config = DistanceAngleYawSolverConfig()

    combinations = list(product(
        rejection_score_threshold_values,
        min_score_gap_values,
        max_rejections_values,
    ))

    sweep_name = "solver4_refined_robust_sweep"
    run_id = make_run_id(sweep_name, prepared_subdir)
    sweep_dir = ROOT_DIR / "output" / "sweeps" / run_id
    sweep_dir.mkdir(parents=True, exist_ok=False)

    comparison_rows: List[Dict[str, Any]] = []
    all_start = time.perf_counter()

    print("")
    print("=" * 82)
    print("Solver #4 Refined Robust Parameter Sweep")
    print("=" * 82)
    print(f"Prepared dataset : {prepared_path}")
    print(f"Combinations     : {len(combinations)}")
    print(f"Output dir       : {sweep_dir}")
    print("=" * 82)
    print("")

    for combo_index, (
        threshold,
        gap,
        max_rejections,
    ) in enumerate(combinations, start=1):

        robust_config = RobustTwoPassConfig(
            rejection_score_threshold=float(threshold),
            min_score_gap=float(gap),
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
            "rejection_score_threshold": float(threshold),
            "min_score_gap": float(gap),
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

            "samples_with_rejection": samples_with_rejection,
            "total_rejected_tags": total_rejected_tags,

            "combo_runtime_s": combo_runtime_s,
        }

        comparison_rows.append(row)

        print(
            f"[{combo_index:03d}/{len(combinations):03d}] "
            f"thr={threshold:4.2f} "
            f"gap={gap:4.2f} "
            f"maxrej={max_rejections} | "
            f"joint={row['joint_within_10cm_10deg_rate']:.3f} "
            f"p90={row['position_p90_error_m']:.4f}m "
            f"max={row['position_max_error_m']:.4f}m "
            f"h90={row['heading_p90_abs_error_deg']:.3f}deg "
            f"rej={total_rejected_tags}"
        )

    total_runtime_s = time.perf_counter() - all_start

    ranked = sorted(comparison_rows, key=_rank_key)

    ranked_rows = []
    for rank, row in enumerate(ranked, start=1):
        out = {"rank": rank}
        out.update(row)
        ranked_rows.append(out)

    _write_csv(
        sweep_dir / "sweep_results_unsorted.csv",
        comparison_rows,
    )
    _write_csv(
        sweep_dir / "sweep_results_ranked.csv",
        ranked_rows,
    )
    _write_csv(
        sweep_dir / "top20_configs.csv",
        ranked_rows[:20],
    )

    best = ranked_rows[0]

    best_payload = {
        "prepared_subdir": prepared_subdir,
        "base_solver_config": asdict(base_config),
        "best_robust_parameters": {
            "rejection_score_threshold": best["rejection_score_threshold"],
            "min_score_gap": best["min_score_gap"],
            "max_rejections": best["max_rejections"],
        },
        "best_metrics": {
            "joint_within_10cm_10deg_rate": best["joint_within_10cm_10deg_rate"],
            "solver_success_rate": best["solver_success_rate"],
            "position_mean_error_m": best["position_mean_error_m"],
            "position_median_error_m": best["position_median_error_m"],
            "position_p90_error_m": best["position_p90_error_m"],
            "position_p95_error_m": best["position_p95_error_m"],
            "position_max_error_m": best["position_max_error_m"],
            "heading_mean_abs_error_deg": best["heading_mean_abs_error_deg"],
            "heading_median_abs_error_deg": best["heading_median_abs_error_deg"],
            "heading_p90_abs_error_deg": best["heading_p90_abs_error_deg"],
            "heading_p95_abs_error_deg": best["heading_p95_abs_error_deg"],
            "heading_max_abs_error_deg": best["heading_max_abs_error_deg"],
            "samples_with_rejection": best["samples_with_rejection"],
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
        best_payload,
    )

    manifest = {
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
        "ranking_rule": best_payload["ranking_rule"],
    }

    write_manifest(
        sweep_dir / "run_manifest.json",
        manifest,
    )

    print("")
    print("=" * 82)
    print("Refined Sweep Complete")
    print("=" * 82)
    print(f"Total runtime   : {total_runtime_s:.2f} s")
    print(f"Ranked results  : {sweep_dir / 'sweep_results_ranked.csv'}")
    print(f"Top 20          : {sweep_dir / 'top20_configs.csv'}")
    print(f"Best config     : {sweep_dir / 'best_config.json'}")
    print("")
    print("Best parameters:")
    print(f"  threshold      = {best['rejection_score_threshold']}")
    print(f"  min_score_gap  = {best['min_score_gap']}")
    print(f"  max_rejections = {best['max_rejections']}")
    print("")
    print("Best metrics:")
    print(
        f"  joint 10/10    = "
        f"{best['joint_within_10cm_10deg_rate']:.3f}"
    )
    print(
        f"  position p90   = "
        f"{best['position_p90_error_m']:.6f} m"
    )
    print(
        f"  position max   = "
        f"{best['position_max_error_m']:.6f} m"
    )
    print(
        f"  heading p90    = "
        f"{best['heading_p90_abs_error_deg']:.6f} deg"
    )
    print(
        f"  rejected tags  = "
        f"{best['total_rejected_tags']}"
    )
    print("=" * 82)
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

    # Refined sweep based on the observed residual-score range.
    REJECTION_SCORE_THRESHOLD_VALUES = [
        0.80,
        1.00,
        1.20,
        1.40,
        1.60,
        1.80,
    ]

    MIN_SCORE_GAP_VALUES = [
        0.05,
        0.10,
        0.20,
        0.30,
        0.50,
    ]

    MAX_REJECTIONS_VALUES = [
        0,
        1,
    ]

    run_refined_solver4_sweep(
        prepared_subdir=PREPARED_SUBDIR,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
        rejection_score_threshold_values=REJECTION_SCORE_THRESHOLD_VALUES,
        min_score_gap_values=MIN_SCORE_GAP_VALUES,
        max_rejections_values=MAX_REJECTIONS_VALUES,
    )
