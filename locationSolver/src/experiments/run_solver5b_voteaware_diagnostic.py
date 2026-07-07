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

from src.solvers.solver_5b_voteaware_gtfree_yaw_twopass import (
    Stage5bConfig,
    solve_stage5b_gtfree_yaw,
)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _position_error(row: Dict[str, Any]) -> float | None:
    v = row.get("position_error_m")
    return None if v in (None, "") else float(v)


def run_comparison(
    prepared_subdir: str,
    tag_map_path: Path,
    tag_yaw_map_path: Path,
) -> None:
    prepared_path = (
        ROOT_DIR / "data" / "prepared" / prepared_subdir / "samples.json"
    )
    payload = load_prepared_dataset(prepared_path)
    samples = payload["samples"]

    tag_xy_map = load_tag_xy_map(tag_map_path)
    tag_yaw_map = load_tag_yaw_json(tag_yaw_map_path)
    tag_pose_map = build_tag_pose_map(tag_xy_map, tag_yaw_map)

    stage5_cfg = Stage5Config(
        distance_sigma_m=0.05,
        angle_sigma_deg=2.5,
        yaw_sigma_deg=3.0,
        distance_global_weight=1.0,
        angle_global_weight=1.0,
        yaw_global_weight=0.75,
        rejection_score_threshold=0.80,
        min_score_gap=0.50,
        max_rejections=1,
        min_observations_after_rejection=3,
        min_abs_predicted_yaw_deg=8.0,
        yaw_accept_best_error_deg=10.0,
        yaw_accept_separation_deg=15.0,
    )

    stage5b_cfg = Stage5bConfig(
        distance_sigma_m=0.05,
        angle_sigma_deg=2.5,
        yaw_sigma_deg=3.0,
        distance_global_weight=1.0,
        angle_global_weight=1.0,
        yaw_global_weight=0.75,

        one_rejection_score_threshold=0.80,
        one_rejection_min_score_gap=0.50,
        min_observations_for_one_rejection=5,

        two_rejection_score_threshold=0.80,
        two_rejection_min_score_gap=0.50,
        min_observations_for_two_rejection=6,

        min_abs_predicted_yaw_deg=8.0,
        yaw_accept_best_error_deg=10.0,
        yaw_accept_separation_deg=15.0,
    )

    s5_results = []
    s5b_results = []
    s5b_da_rows_all = []
    s5b_yaw_rows_all = []

    for sample in samples:
        r5, _, _, _ = solve_stage5_gtfree_yaw(
            sample=sample,
            tag_pose_map=tag_pose_map,
            config=stage5_cfg,
        )

        r5b, _, da5b, yaw5b = solve_stage5b_gtfree_yaw(
            sample=sample,
            tag_pose_map=tag_pose_map,
            config=stage5b_cfg,
        )

        s5_results.append(r5)
        s5b_results.append(r5b)
        s5b_da_rows_all.extend(da5b)
        s5b_yaw_rows_all.extend(yaw5b)

    s5_rows, s5_summary = evaluate_solver_results(samples, s5_results)
    s5b_rows, s5b_summary = evaluate_solver_results(samples, s5b_results)

    s5_by_uid = {str(r["sample_uid"]): r for r in s5_rows}
    s5b_by_uid = {str(r["sample_uid"]): r for r in s5b_rows}

    result5_by_uid = {r.sample_uid: r for r in s5_results}
    result5b_by_uid = {r.sample_uid: r for r in s5b_results}

    affected = []
    for sample in samples:
        uid = str(sample["sample_uid"])
        r5 = result5_by_uid[uid]
        r5b = result5b_by_uid[uid]

        rej5 = list(r5.tags_rejected or [])
        rej5b = list(r5b.tags_rejected or [])

        if rej5 != rej5b:
            row5 = s5_by_uid[uid]
            row5b = s5b_by_uid[uid]

            e5 = _position_error(row5)
            e5b = _position_error(row5b)

            affected.append({
                "sample_uid": uid,
                "observation_count": len(sample.get("observations") or []),

                "stage5_rejected_tags": ",".join(map(str, rej5)),
                "stage5b_rejected_tags": ",".join(map(str, rej5b)),

                "stage5_position_error_m": e5,
                "stage5b_position_error_m": e5b,
                "position_error_change_m": (
                    None if e5 is None or e5b is None else e5b - e5
                ),

                "stage5_heading_abs_error_deg":
                    row5.get("heading_abs_error_deg"),
                "stage5b_heading_abs_error_deg":
                    row5b.get("heading_abs_error_deg"),

                "stage5_yaw_accepted_count":
                    (r5.extra or {}).get("stage5_yaw_accepted_count"),
                "stage5b_yaw_accepted_count":
                    (r5b.extra or {}).get("stage5b_yaw_accepted_count"),
            })

    run_id = make_run_id(
        "solver5b_voteaware_diagnostic",
        prepared_subdir,
    )
    out_dir = ROOT_DIR / "output" / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=False)

    write_json(
        out_dir / "run_config.json",
        {
            "solver_name": "solver5b_voteaware_diagnostic",
            "prepared_subdir": prepared_subdir,
            "stage5_frozen_config": asdict(stage5_cfg),
            "stage5b_temporary_config": asdict(stage5b_cfg),
            "stage5b_rule": {
                "priority_1": (
                    "Try reject two first when N>=6: "
                    "s2>=0.80 and s2-s3>=0.50"
                ),
                "priority_2": (
                    "If no pair rejected, try reject one when N>=5: "
                    "s1>=0.80 and s1-s2>=0.50"
                ),
                "settle": (
                    "After pair rejection, one rejection, or no rejection, stop."
                ),
            },
            "important_note": (
                "Temporary Stage-5b diagnostic only. "
                "All Stage-5 yaw logic and solver parameters are unchanged."
            ),
        },
    )

    write_json(
        out_dir / "summary_stage5_vs_stage5b.json",
        {
            "stage5_frozen": s5_summary,
            "stage5b_temporary": s5b_summary,
            "affected_sample_count": len(affected),
            "stage5_total_rejections": sum(
                len(r.tags_rejected or []) for r in s5_results
            ),
            "stage5b_total_rejections": sum(
                len(r.tags_rejected or []) for r in s5b_results
            ),
            "stage5b_reject_two_sample_count": sum(
                1 for r in s5b_results
                if len(r.tags_rejected or []) == 2
            ),
            "stage5b_reject_one_sample_count": sum(
                1 for r in s5b_results
                if len(r.tags_rejected or []) == 1
            ),
        },
    )

    write_rows_csv(out_dir / "sample_results_stage5.csv", s5_rows)
    write_rows_csv(out_dir / "sample_results_stage5b.csv", s5b_rows)
    _write_csv(out_dir / "affected_samples_only.csv", affected)
    _write_csv(out_dir / "stage5b_da_residuals.csv", s5b_da_rows_all)
    _write_csv(
        out_dir / "stage5b_rejected_observations.csv",
        [r for r in s5b_da_rows_all if bool(r.get("rejected"))],
    )
    _write_csv(out_dir / "stage5b_yaw_decisions.csv", s5b_yaw_rows_all)

    write_manifest(
        out_dir / "run_manifest.json",
        {
            "run_id": run_id,
            "solver_name": "solver5b_voteaware_diagnostic",
            "prepared_subdir": prepared_subdir,
            "sample_count": len(samples),
            "affected_sample_count": len(affected),
            "stage5_total_rejections": sum(
                len(r.tags_rejected or []) for r in s5_results
            ),
            "stage5b_total_rejections": sum(
                len(r.tags_rejected or []) for r in s5b_results
            ),
            "stage5b_reject_two_sample_count": sum(
                1 for r in s5b_results
                if len(r.tags_rejected or []) == 2
            ),
            "stage5b_reject_one_sample_count": sum(
                1 for r in s5b_results
                if len(r.tags_rejected or []) == 1
            ),
        },
    )

    print("")
    print("=" * 92)
    print("TEMPORARY STAGE-5b DIAGNOSTIC: FROZEN STAGE-5 vs VOTE-AWARE REJECTION")
    print("=" * 92)
    print(f"Prepared dataset     : {prepared_path}")
    print(f"Output directory     : {out_dir}")
    print("")
    print("Stage-5b rule:")
    print("  1) Try reject TWO first when N>=6")
    print("       s2 >= 0.80 and s2-s3 >= 0.50")
    print("  2) If not, try reject ONE when N>=5")
    print("       s1 >= 0.80 and s1-s2 >= 0.50")
    print("  3) Otherwise reject none")
    print("")
    print(f"Affected samples     : {len(affected)}")
    print(
        f"Total rejections     : "
        f"{sum(len(r.tags_rejected or []) for r in s5_results)} "
        f"-> {sum(len(r.tags_rejected or []) for r in s5b_results)}"
    )
    print(
        f"Reject-two samples   : "
        f"{sum(1 for r in s5b_results if len(r.tags_rejected or []) == 2)}"
    )
    print("")
    print("Global position:")
    print(
        f"  mean               : "
        f"{s5_summary['position_mean_error_m']:.6f} "
        f"-> {s5b_summary['position_mean_error_m']:.6f} m"
    )
    print(
        f"  p90                : "
        f"{s5_summary['position_p90_error_m']:.6f} "
        f"-> {s5b_summary['position_p90_error_m']:.6f} m"
    )
    print(
        f"  p95                : "
        f"{s5_summary['position_p95_error_m']:.6f} "
        f"-> {s5b_summary['position_p95_error_m']:.6f} m"
    )
    print(
        f"  max                : "
        f"{s5_summary['position_max_error_m']:.6f} "
        f"-> {s5b_summary['position_max_error_m']:.6f} m"
    )
    print("")
    print("Affected samples only:")
    if not affected:
        print("  none")
    else:
        for row in affected:
            print(
                f"  {row['sample_uid']}: "
                f"reject [{row['stage5_rejected_tags'] or '-'}] "
                f"-> [{row['stage5b_rejected_tags'] or '-'}], "
                f"pos "
                f"{100.0*float(row['stage5_position_error_m']):.2f} "
                f"-> {100.0*float(row['stage5b_position_error_m']):.2f} cm"
            )
    print("=" * 92)
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

    run_comparison(
        prepared_subdir=PREPARED_SUBDIR,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
    )
