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


from src.common.evaluator import evaluate_solver_results
from src.common.run_manifest import make_run_id, write_manifest
from src.common.run_output import write_json, write_rows_csv
from src.common.tag_map_loader import (
    build_tag_pose_map,
    load_tag_xy_map,
    load_tag_yaw_json,
)
from src.solvers.solver_6_componentwise_distance_angle_yaw import (
    ComponentwiseDistanceAngleYawSolverConfig,
    solve_componentwise_distance_angle_yaw,
)


def _load_final_samples(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))

    if payload.get("pipeline_stage") != (
        "ready_for_final_componentwise_solver"
    ):
        raise ValueError(
            "Unexpected pipeline_stage in final input: "
            f"{payload.get('pipeline_stage')!r}"
        )

    samples = payload.get("samples")
    if not isinstance(samples, list):
        raise ValueError("final input must contain list field 'samples'")

    return samples


def _write_csv(
    path: Path,
    rows: List[Dict[str, Any]],
) -> None:
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

    with path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fields,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def _component_pattern_audit(
    samples: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for sample in samples:
        sample_uid = str(sample["sample_uid"])

        for obs in sample.get("observations") or []:
            flags = obs.get("flags") or {}

            d = bool(flags.get("distance_use"))
            a = bool(flags.get("angle_use"))
            y = bool(flags.get("yaw_use"))

            pattern = (
                ("D" if d else "")
                + ("A" if a else "")
                + ("Y" if y else "")
            ) or "NONE"

            rows.append({
                "sample_uid": sample_uid,
                "observation_uid": str(
                    obs.get("observation_uid") or ""
                ),
                "tag_id": int(obs["tag_id"]),
                "camera_role": str(
                    obs.get("camera_role") or ""
                ),
                "component_pattern": pattern,

                "distance_use": d,
                "distance_use_reason": str(
                    flags.get("distance_use_reason") or ""
                ),

                "angle_use": a,
                "angle_use_reason": str(
                    flags.get("angle_use_reason") or ""
                ),

                "yaw_use": y,
                "yaw_use_reason": str(
                    flags.get("yaw_use_reason") or ""
                ),
            })

    return rows


def run_final_solver(
    final_input_path: Path,
    tag_map_path: Path,
    tag_yaw_map_path: Path,
) -> None:
    samples = _load_final_samples(final_input_path)

    tag_xy_map = load_tag_xy_map(tag_map_path)
    tag_yaw_map = load_tag_yaw_json(tag_yaw_map_path)
    tag_pose_map = build_tag_pose_map(
        tag_xy_map,
        tag_yaw_map,
    )

    config = ComponentwiseDistanceAngleYawSolverConfig()

    results = [
        solve_componentwise_distance_angle_yaw(
            sample=sample,
            tag_pose_map=tag_pose_map,
            config=config,
        )
        for sample in samples
    ]

    sample_rows, summary = evaluate_solver_results(
        samples,
        results,
    )

    run_id = make_run_id(
        "solver6_final_componentwise_day",
        "step26",
    )
    out_dir = ROOT_DIR / "output" / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=False)

    pattern_rows = _component_pattern_audit(samples)

    _write_csv(
        out_dir / "final_component_pattern_audit.csv",
        pattern_rows,
    )
    write_rows_csv(
        out_dir / "sample_results_final_componentwise.csv",
        sample_rows,
    )

    write_json(
        out_dir / "summary_final_componentwise.json",
        summary,
    )

    write_json(
        out_dir / "run_config.json",
        {
            "solver_name": (
                "solver6_final_componentwise_distance_angle_yaw"
            ),
            "final_input_path": str(final_input_path),
            "tag_map_path": str(tag_map_path),
            "tag_yaw_map_path": str(tag_yaw_map_path),
            "solver_config": asdict(config),

            "gt_audit": {
                "solver_uses_gt": False,
                "solver_reads_evaluation_fields": False,
                "solver_reads_old_truth_assisted_yaw": False,
                "evaluator_uses_gt_after_estimation": True,
            },

            "component_contract": {
                "distance": "flags.distance_use",
                "angle": "flags.angle_use",
                "yaw": "flags.yaw_use",
                "resolved_yaw": (
                    "measured.yaw_resolved_deg"
                ),
            },
        },
    )

    write_manifest(
        out_dir / "run_manifest.json",
        {
            "run_id": run_id,
            "solver_name": (
                "solver6_final_componentwise_distance_angle_yaw"
            ),
            "sample_count": len(samples),
            "success_count": sum(
                1 for result in results if result.success
            ),
            "failure_count": sum(
                1 for result in results if not result.success
            ),
            "active_distance_total": sum(
                int(result.extra.get(
                    "active_distance_count", 0
                ))
                for result in results
            ),
            "active_angle_total": sum(
                int(result.extra.get(
                    "active_angle_count", 0
                ))
                for result in results
            ),
            "active_yaw_total": sum(
                int(result.extra.get(
                    "active_yaw_count", 0
                ))
                for result in results
            ),
            "output_dir": str(out_dir),
        },
    )

    print("")
    print("=" * 94)
    print("SOLVER #6: FINAL COMPONENT-WISE D/A/Y")
    print("=" * 94)
    print(f"Final input       : {final_input_path}")
    print(f"Samples           : {len(samples)}")
    print(
        f"Success           : "
        f"{sum(1 for r in results if r.success)}/{len(results)}"
    )
    print(
        f"Active distance   : "
        f"{sum(int(r.extra.get('active_distance_count', 0)) for r in results)}"
    )
    print(
        f"Active angle      : "
        f"{sum(int(r.extra.get('active_angle_count', 0)) for r in results)}"
    )
    print(
        f"Active yaw        : "
        f"{sum(int(r.extra.get('active_yaw_count', 0)) for r in results)}"
    )
    print("")
    print("Evaluation:")
    print(
        f"  position mean   : "
        f"{summary['position_mean_error_m']:.6f} m"
    )
    print(
        f"  position p90    : "
        f"{summary['position_p90_error_m']:.6f} m"
    )
    print(
        f"  position p95    : "
        f"{summary['position_p95_error_m']:.6f} m"
    )
    print(
        f"  position max    : "
        f"{summary['position_max_error_m']:.6f} m"
    )
    print(
        f"  heading mean    : "
        f"{summary['heading_mean_abs_error_deg']:.6f} deg"
    )
    print(
        f"  heading p90     : "
        f"{summary['heading_p90_abs_error_deg']:.6f} deg"
    )
    print(
        f"  heading max     : "
        f"{summary['heading_max_abs_error_deg']:.6f} deg"
    )
    print(
        f"  joint 10/10     : "
        f"{summary['joint_within_10cm_10deg_rate']:.3f}"
    )
    print("")
    print(f"Output directory  : {out_dir}")
    print("=" * 94)
    print("")


if __name__ == "__main__":
    # VS Code direct-run parameters.
    #
    # Point this to Step-25 output:
    #   final_componentwise_samples.json
    FINAL_INPUT_PATH = (
        ROOT_DIR
        / "data"
        / "input"
        / "final_componentwise_samples.json"
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

    run_final_solver(
        final_input_path=FINAL_INPUT_PATH,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
    )
