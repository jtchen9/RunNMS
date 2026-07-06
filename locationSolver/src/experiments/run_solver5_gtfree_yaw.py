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
from src.solvers.solver_5_gtfree_yaw_twopass import (
    Stage5Config,
    solve_stage5_gtfree_yaw,
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


def run_stage5(
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

    # Intentionally no sweep here.
    # Use physically motivated gates inherited from the earlier yaw study,
    # replacing GT reference with Pass-1 predicted geometry.
    config = Stage5Config()

    final_results = []
    pass1_results = []
    da_rows_all: List[Dict[str, Any]] = []
    yaw_rows_all: List[Dict[str, Any]] = []

    for sample in samples:
        final_result, pass1_result, da_rows, yaw_rows = (
            solve_stage5_gtfree_yaw(
                sample=sample,
                tag_pose_map=tag_pose_map,
                config=config,
            )
        )

        final_results.append(final_result)
        pass1_results.append(pass1_result)
        da_rows_all.extend(da_rows)
        yaw_rows_all.extend(yaw_rows)

    pass1_sample_rows, pass1_summary = evaluate_solver_results(
        samples,
        pass1_results,
    )
    final_sample_rows, final_summary = evaluate_solver_results(
        samples,
        final_results,
    )

    run_id = make_run_id(
        "solver5_gtfree_yaw_twopass",
        prepared_subdir,
    )
    out_dir = ROOT_DIR / "output" / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=False)

    write_json(
        out_dir / "run_config.json",
        {
            "solver_name": "solver5_gtfree_yaw_twopass",
            "prepared_subdir": prepared_subdir,
            "prepared_json": str(prepared_path),
            "tag_map_path": str(tag_map_path),
            "tag_yaw_map_path": str(tag_yaw_map_path),
            "stage5_config": asdict(config),
            "gt_audit": {
                "pass1_uses_gt": False,
                "da_outlier_rejection_uses_gt": False,
                "yaw_sign_resolution_uses_gt": False,
                "pass2_uses_gt": False,
                "evaluator_uses_gt_after_estimation": True,
            },
            "important_note": (
                "Stage 5 reads raw measured yaw_deg only. "
                "It intentionally ignores yaw_use_offline_label and "
                "yaw_sign_corrected_deg."
            ),
        },
    )

    write_json(
        out_dir / "summary_before_after.json",
        {
            "pass1_distance_angle": pass1_summary,
            "final_stage5": final_summary,
            "rejected_sample_count": sum(
                1 for r in final_results
                if len(r.tags_rejected) > 0
            ),
            "total_rejected_observations": sum(
                len(r.tags_rejected)
                for r in final_results
            ),
            "accepted_yaw_total": sum(
                1 for r in yaw_rows_all
                if bool(r.get("accepted"))
            ),
            "accepted_yaw_keep_total": sum(
                1 for r in yaw_rows_all
                if bool(r.get("accepted"))
                and r.get("best_mode") == "keep"
            ),
            "accepted_yaw_flip_total": sum(
                1 for r in yaw_rows_all
                if bool(r.get("accepted"))
                and r.get("best_mode") == "flip"
            ),
        },
    )

    write_rows_csv(
        out_dir / "sample_results_pass1_distance_angle.csv",
        pass1_sample_rows,
    )
    write_rows_csv(
        out_dir / "sample_results_stage5_final.csv",
        final_sample_rows,
    )

    _write_csv(
        out_dir / "da_residuals_pass1.csv",
        da_rows_all,
    )
    _write_csv(
        out_dir / "yaw_gtfree_decisions.csv",
        yaw_rows_all,
    )
    _write_csv(
        out_dir / "yaw_gtfree_accepted.csv",
        [
            row for row in yaw_rows_all
            if bool(row.get("accepted"))
        ],
    )

    rejected_rows = [
        row for row in da_rows_all
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
            "solver_name": "solver5_gtfree_yaw_twopass",
            "prepared_subdir": prepared_subdir,
            "prepared_json": str(prepared_path),
            "output_dir": str(out_dir),

            "sample_count": len(samples),
            "final_success_count": sum(
                1 for r in final_results if r.success
            ),

            "rejected_sample_count": sum(
                1 for r in final_results
                if len(r.tags_rejected) > 0
            ),
            "total_rejected_observations": sum(
                len(r.tags_rejected)
                for r in final_results
            ),

            "accepted_yaw_total": sum(
                1 for r in yaw_rows_all
                if bool(r.get("accepted"))
            ),
            "accepted_yaw_keep_total": sum(
                1 for r in yaw_rows_all
                if bool(r.get("accepted"))
                and r.get("best_mode") == "keep"
            ),
            "accepted_yaw_flip_total": sum(
                1 for r in yaw_rows_all
                if bool(r.get("accepted"))
                and r.get("best_mode") == "flip"
            ),
        },
    )

    print("")
    print("=" * 86)
    print("Solver #5: GT-Free Yaw Two-Pass")
    print("=" * 86)
    print(f"Prepared dataset : {prepared_path}")
    print(f"Output directory : {out_dir}")
    print("")
    print("Pass 1:")
    print("  distance + angle only")
    print("  no yaw")
    print("  no GT")
    print("")
    print("Between passes:")
    print("  D/A-only outlier screening")
    print("  raw-yaw keep/flip from Pass-1 predicted geometry")
    print("")
    print("Pass 2:")
    print("  cleaned distance + angle")
    print("  qualified GT-free yaw")
    print("")
    print(
        f"Rejected obs      : "
        f"{sum(len(r.tags_rejected) for r in final_results)}"
    )
    print(
        f"Accepted yaw      : "
        f"{sum(1 for r in yaw_rows_all if bool(r.get('accepted')))}"
    )
    print(
        f"  keep            : "
        f"{sum(1 for r in yaw_rows_all if bool(r.get('accepted')) and r.get('best_mode') == 'keep')}"
    )
    print(
        f"  flip            : "
        f"{sum(1 for r in yaw_rows_all if bool(r.get('accepted')) and r.get('best_mode') == 'flip')}"
    )
    print("")
    print("Pass 1 -> Stage 5 final:")
    print(
        f"  position mean   : "
        f"{pass1_summary['position_mean_error_m']:.6f} "
        f"-> {final_summary['position_mean_error_m']:.6f} m"
    )
    print(
        f"  position p90    : "
        f"{pass1_summary['position_p90_error_m']:.6f} "
        f"-> {final_summary['position_p90_error_m']:.6f} m"
    )
    print(
        f"  position max    : "
        f"{pass1_summary['position_max_error_m']:.6f} "
        f"-> {final_summary['position_max_error_m']:.6f} m"
    )
    print(
        f"  heading p90     : "
        f"{pass1_summary['heading_p90_abs_error_deg']:.6f} "
        f"-> {final_summary['heading_p90_abs_error_deg']:.6f} deg"
    )
    print(
        f"  joint 10/10     : "
        f"{pass1_summary['joint_within_10cm_10deg_rate']:.3f} "
        f"-> {final_summary['joint_within_10cm_10deg_rate']:.3f}"
    )
    print("=" * 86)
    print("")


if __name__ == "__main__":
    PREPARED_SUBDIR = "normal_diversity"

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

    run_stage5(
        prepared_subdir=PREPARED_SUBDIR,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
    )
