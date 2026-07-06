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
from src.solvers.solver_4_component_aware import (
    ComponentAwareRobustConfig,
    solve_component_aware_twopass,
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
    return (
        -float(row["joint_within_10cm_10deg_rate"]),
        -float(row["solver_success_rate"]),
        float(row["position_p90_error_m"]),
        float(row["position_max_error_m"]),
        float(row["heading_p90_abs_error_deg"]),
        int(row["total_rejected_tags"]),
    )


def run_component_aware_sweep(
    prepared_subdir: str,
    tag_map_path: Path,
    tag_yaw_map_path: Path,

    core_reject_threshold_values: List[float],
    core_support_threshold_values: List[float],
    yaw_support_threshold_values: List[float],
    min_decision_score_gap_values: List[float],
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

    base_config = DistanceAngleYawSolverConfig()

    combinations = list(product(
        core_reject_threshold_values,
        core_support_threshold_values,
        yaw_support_threshold_values,
        min_decision_score_gap_values,
    ))

    run_id = make_run_id(
        "solver4_component_aware_sweep",
        prepared_subdir,
    )
    out_dir = ROOT_DIR / "output" / "sweeps" / run_id
    out_dir.mkdir(parents=True, exist_ok=False)

    rows: List[Dict[str, Any]] = []
    start_all = time.perf_counter()

    print("")
    print("=" * 88)
    print("Solver #4 Component-Aware Rejection Sweep")
    print("=" * 88)
    print(f"Prepared dataset : {prepared_path}")
    print(f"Combinations     : {len(combinations)}")
    print(f"Output dir       : {out_dir}")
    print("=" * 88)
    print("")

    for idx, (
        core_reject_threshold,
        core_support_threshold,
        yaw_support_threshold,
        min_gap,
    ) in enumerate(combinations, start=1):

        robust_config = ComponentAwareRobustConfig(
            core_reject_threshold=float(core_reject_threshold),
            core_support_threshold=float(core_support_threshold),
            yaw_support_threshold=float(yaw_support_threshold),
            min_decision_score_gap=float(min_gap),
            max_rejections=1,
        )

        results = []
        combo_start = time.perf_counter()

        for sample in samples:
            result, _ = solve_component_aware_twopass(
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

            "core_reject_threshold": float(core_reject_threshold),
            "core_support_threshold": float(core_support_threshold),
            "yaw_support_threshold": float(yaw_support_threshold),
            "yaw_bonus_weight": robust_config.yaw_bonus_weight,
            "min_decision_score_gap": float(min_gap),

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
            f"core={core_reject_threshold:4.2f} "
            f"support={core_support_threshold:4.2f} "
            f"yaw={yaw_support_threshold:4.2f} "
            f"gap={min_gap:4.2f} | "
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
            "base_solver_config": asdict(base_config),
            "best_component_aware_parameters": {
                "core_reject_threshold":
                    best["core_reject_threshold"],
                "core_support_threshold":
                    best["core_support_threshold"],
                "yaw_support_threshold":
                    best["yaw_support_threshold"],
                "yaw_bonus_weight":
                    best["yaw_bonus_weight"],
                "min_decision_score_gap":
                    best["min_decision_score_gap"],
                "max_rejections": 1,
            },
            "best_metrics": {
                "joint_within_10cm_10deg_rate":
                    best["joint_within_10cm_10deg_rate"],
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
                "heading_p90_abs_error_deg":
                    best["heading_p90_abs_error_deg"],
                "samples_with_rejection":
                    best["samples_with_rejection"],
                "total_rejected_tags":
                    best["total_rejected_tags"],
            },
        },
    )

    write_manifest(
        out_dir / "run_manifest.json",
        {
            "run_id": run_id,
            "sweep_name": "solver4_component_aware_sweep",
            "prepared_subdir": prepared_subdir,
            "prepared_json": str(prepared_path),
            "combination_count": len(combinations),
            "total_runtime_s": total_runtime_s,
            "output_dir": str(out_dir),
            "core_reject_threshold_values":
                core_reject_threshold_values,
            "core_support_threshold_values":
                core_support_threshold_values,
            "yaw_support_threshold_values":
                yaw_support_threshold_values,
            "min_decision_score_gap_values":
                min_decision_score_gap_values,
        },
    )

    print("")
    print("=" * 88)
    print("Component-Aware Sweep Complete")
    print("=" * 88)
    print(f"Total runtime : {total_runtime_s:.2f} s")
    print(f"Output dir    : {out_dir}")
    print("")
    print("Best parameters:")
    print(
        f"  core_reject_threshold  = "
        f"{best['core_reject_threshold']}"
    )
    print(
        f"  core_support_threshold = "
        f"{best['core_support_threshold']}"
    )
    print(
        f"  yaw_support_threshold  = "
        f"{best['yaw_support_threshold']}"
    )
    print(
        f"  min_score_gap          = "
        f"{best['min_decision_score_gap']}"
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
    print("=" * 88)
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

    # 4 x 3 x 3 x 2 = 72 combinations.
    CORE_REJECT_THRESHOLD_VALUES = [
        0.80,
        1.00,
        1.20,
        1.40,
    ]

    CORE_SUPPORT_THRESHOLD_VALUES = [
        0.40,
        0.60,
        0.80,
    ]

    YAW_SUPPORT_THRESHOLD_VALUES = [
        1.50,
        2.00,
        2.50,
    ]

    MIN_DECISION_SCORE_GAP_VALUES = [
        0.30,
        0.50,
    ]

    run_component_aware_sweep(
        prepared_subdir=PREPARED_SUBDIR,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
        core_reject_threshold_values=
            CORE_REJECT_THRESHOLD_VALUES,
        core_support_threshold_values=
            CORE_SUPPORT_THRESHOLD_VALUES,
        yaw_support_threshold_values=
            YAW_SUPPORT_THRESHOLD_VALUES,
        min_decision_score_gap_values=
            MIN_DECISION_SCORE_GAP_VALUES,
    )
