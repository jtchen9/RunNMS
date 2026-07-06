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
from src.common.geometry import (
    predicted_tag_angle_deg,
    predicted_tag_distance_m,
    predicted_tag_yaw_deg,
    wrap_angle_deg,
)
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
from src.solvers.solver_4_robust_twopass import (
    RobustTwoPassConfig,
    solve_robust_twopass,
)


def _residual_row_for_pose(
    *,
    sample_uid: str,
    obs: Dict[str, Any],
    pose_x_m: float,
    pose_y_m: float,
    pose_heading_deg: float,
    tag_pose_map: Dict[int, tuple],
    base_config: DistanceAngleYawSolverConfig,
    stage: str,
) -> Dict[str, Any]:
    tag_id = int(obs["tag_id"])
    tag_x_m, tag_y_m, tag_yaw_deg = tag_pose_map[tag_id]

    measured = obs.get("measured") or {}
    weights = obs.get("weights") or {}
    flags = obs.get("flags") or {}

    meas_d = float(measured["distance_m"])
    meas_a = float(measured["angle_deg"])

    pred_d = predicted_tag_distance_m(
        pose_x_m,
        pose_y_m,
        pose_heading_deg,
        obs["camera_role"],
        tag_x_m,
        tag_y_m,
    )
    pred_a = predicted_tag_angle_deg(
        pose_x_m,
        pose_y_m,
        pose_heading_deg,
        obs["camera_role"],
        tag_x_m,
        tag_y_m,
    )

    raw_d = pred_d - meas_d
    raw_a = wrap_angle_deg(pred_a - meas_a)

    wd = float(weights.get("distance", 1.0))
    wa = float(weights.get("angle", 1.0))

    nd = (
        (base_config.distance_global_weight * wd) ** 0.5
        * raw_d
        / base_config.distance_sigma_m
    )
    na = (
        (base_config.angle_global_weight * wa) ** 0.5
        * raw_a
        / base_config.angle_sigma_deg
    )

    yaw_used = False
    meas_y = None
    pred_y = None
    raw_y = None
    ny = None

    if (
        bool(flags.get("yaw_use_offline_label"))
        and measured.get("yaw_sign_corrected_deg") is not None
        and tag_yaw_deg is not None
    ):
        wy = float(weights.get("yaw", 1.0))
        if wy > 0.0:
            yaw_used = True
            meas_y = float(measured["yaw_sign_corrected_deg"])
            pred_y = predicted_tag_yaw_deg(
                pose_x_m,
                pose_y_m,
                pose_heading_deg,
                obs["camera_role"],
                tag_x_m,
                tag_y_m,
                float(tag_yaw_deg),
            )
            raw_y = wrap_angle_deg(pred_y - meas_y)
            ny = (
                (base_config.yaw_global_weight * wy) ** 0.5
                * raw_y
                / base_config.yaw_sigma_deg
            )

    components = [nd, na]
    if ny is not None:
        components.append(ny)

    combined_score = (
        sum(v * v for v in components) / len(components)
    ) ** 0.5

    return {
        "stage": stage,
        "sample_uid": sample_uid,
        "observation_uid": str(obs.get("observation_uid") or ""),
        "tag_id": tag_id,
        "camera_role": str(obs.get("camera_role") or ""),

        "measured_distance_m": meas_d,
        "predicted_distance_m": pred_d,
        "distance_residual_m": raw_d,
        "normalized_distance_residual": nd,

        "measured_angle_deg": meas_a,
        "predicted_angle_deg": pred_a,
        "angle_residual_deg": raw_a,
        "normalized_angle_residual": na,

        "yaw_used": yaw_used,
        "measured_yaw_deg": meas_y,
        "predicted_yaw_deg": pred_y,
        "yaw_residual_deg": raw_y,
        "normalized_yaw_residual": ny,

        "combined_score": combined_score,
    }


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run_best_robust_diagnostics(
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

    base_config = DistanceAngleYawSolverConfig()

    # Best plateau point from refined sweep.
    robust_config = RobustTwoPassConfig(
        rejection_score_threshold=0.80,
        min_score_gap=0.50,
        max_rejections=1,
    )

    pass1_results = []
    final_results = []

    pass1_residual_rows: List[Dict[str, Any]] = []
    final_residual_rows: List[Dict[str, Any]] = []
    rejected_rows: List[Dict[str, Any]] = []

    sample_by_uid = {
        str(sample["sample_uid"]): sample
        for sample in samples
    }

    for sample in samples:
        uid = str(sample["sample_uid"])

        pass1 = solve_distance_angle_yaw(
            sample=sample,
            tag_pose_map=tag_pose_map,
            config=base_config,
        )
        pass1_results.append(pass1)

        final, solver4_pass1_rows = solve_robust_twopass(
            sample=sample,
            tag_pose_map=tag_pose_map,
            base_config=base_config,
            robust_config=robust_config,
        )
        final_results.append(final)

        if pass1.success:
            for obs in sample.get("observations") or []:
                tag_id = int(obs["tag_id"])
                if tag_id not in tag_pose_map:
                    continue

                pass1_residual_rows.append(
                    _residual_row_for_pose(
                        sample_uid=uid,
                        obs=obs,
                        pose_x_m=float(pass1.estimated_x_m),
                        pose_y_m=float(pass1.estimated_y_m),
                        pose_heading_deg=float(pass1.estimated_heading_deg),
                        tag_pose_map=tag_pose_map,
                        base_config=base_config,
                        stage="pass1",
                    )
                )

        rejected_uids = set(
            str(x)
            for x in (final.extra or {}).get(
                "rejected_observation_uids", []
            )
        )

        if final.success:
            for obs in sample.get("observations") or []:
                tag_id = int(obs["tag_id"])
                if tag_id not in tag_pose_map:
                    continue

                row = _residual_row_for_pose(
                    sample_uid=uid,
                    obs=obs,
                    pose_x_m=float(final.estimated_x_m),
                    pose_y_m=float(final.estimated_y_m),
                    pose_heading_deg=float(final.estimated_heading_deg),
                    tag_pose_map=tag_pose_map,
                    base_config=base_config,
                    stage="final",
                )
                row["rejected_from_pass2"] = (
                    str(obs.get("observation_uid") or "")
                    in rejected_uids
                )
                final_residual_rows.append(row)

        # Detailed rejected-observation rows from Solver #4 pass-1 diagnostics.
        for row in solver4_pass1_rows:
            if bool(row.get("rejected")):
                rejected_rows.append(dict(row))

    pass1_sample_rows, pass1_summary = evaluate_solver_results(
        samples,
        pass1_results,
    )
    final_sample_rows, final_summary = evaluate_solver_results(
        samples,
        final_results,
    )

    pass1_by_uid = {r["sample_uid"]: r for r in pass1_sample_rows}
    final_by_uid = {r["sample_uid"]: r for r in final_sample_rows}

    before_after_rows: List[Dict[str, Any]] = []

    for uid in sorted(sample_by_uid):
        before = pass1_by_uid[uid]
        after = final_by_uid[uid]
        final_result = next(
            r for r in final_results
            if r.sample_uid == uid
        )

        rejected_ids = list(final_result.tags_rejected)
        rejected_uids = list(
            (final_result.extra or {}).get(
                "rejected_observation_uids", []
            )
        )

        before_after_rows.append({
            "sample_uid": uid,

            "pass1_success": before["success"],
            "final_success": after["success"],

            "pass1_est_x_m": before["estimated_x_m"],
            "pass1_est_y_m": before["estimated_y_m"],
            "pass1_est_heading_deg": before["estimated_heading_deg"],

            "final_est_x_m": after["estimated_x_m"],
            "final_est_y_m": after["estimated_y_m"],
            "final_est_heading_deg": after["estimated_heading_deg"],

            "gt_x_m": before["gt_x_m"],
            "gt_y_m": before["gt_y_m"],
            "gt_heading_deg": before["gt_heading_deg"],

            "pass1_position_error_m": before["position_error_m"],
            "final_position_error_m": after["position_error_m"],
            "position_error_change_m": (
                None
                if (
                    before["position_error_m"] is None
                    or after["position_error_m"] is None
                )
                else float(after["position_error_m"])
                - float(before["position_error_m"])
            ),

            "pass1_abs_heading_error_deg": before["abs_heading_error_deg"],
            "final_abs_heading_error_deg": after["abs_heading_error_deg"],
            "heading_error_change_deg": (
                None
                if (
                    before["abs_heading_error_deg"] is None
                    or after["abs_heading_error_deg"] is None
                )
                else float(after["abs_heading_error_deg"])
                - float(before["abs_heading_error_deg"])
            ),

            "rejected_count": len(rejected_ids),
            "rejected_tag_ids": ";".join(str(x) for x in rejected_ids),
            "rejected_observation_uids": ";".join(
                str(x) for x in rejected_uids
            ),

            "robust_passes": (
                final_result.extra or {}
            ).get("robust_passes"),
        })

    run_id = make_run_id(
        "solver4_best_robust_diagnostics",
        prepared_subdir,
    )
    out_dir = ROOT_DIR / "output" / "diagnostics" / run_id
    out_dir.mkdir(parents=True, exist_ok=False)

    write_json(
        out_dir / "run_config.json",
        {
            "prepared_subdir": prepared_subdir,
            "prepared_json": str(prepared_path),
            "tag_map_path": str(tag_map_path),
            "tag_yaw_map_path": str(tag_yaw_map_path),
            "base_solver_config": asdict(base_config),
            "robust_config": asdict(robust_config),
            "important_note": (
                "Yaw acceptance/sign still uses offline diagnostic labels. "
                "Robust rejection uses pass-1 residuals, not ground truth."
            ),
        },
    )

    write_json(
        out_dir / "summary_before_after.json",
        {
            "pass1_solver3": pass1_summary,
            "final_solver4": final_summary,
            "rejected_sample_count": sum(
                1 for r in final_results
                if len(r.tags_rejected) > 0
            ),
            "total_rejected_observations": sum(
                len(r.tags_rejected)
                for r in final_results
            ),
        },
    )

    write_rows_csv(
        out_dir / "sample_results_pass1.csv",
        pass1_sample_rows,
    )
    write_rows_csv(
        out_dir / "sample_results_final.csv",
        final_sample_rows,
    )

    _write_csv(
        out_dir / "sample_before_after.csv",
        before_after_rows,
    )
    _write_csv(
        out_dir / "rejected_observations.csv",
        rejected_rows,
    )
    _write_csv(
        out_dir / "observation_residuals_pass1.csv",
        pass1_residual_rows,
    )
    _write_csv(
        out_dir / "observation_residuals_final.csv",
        final_residual_rows,
    )

    manifest = {
        "run_id": run_id,
        "prepared_subdir": prepared_subdir,
        "output_dir": str(out_dir),
        "sample_count": len(samples),
        "pass1_result_count": len(pass1_results),
        "final_result_count": len(final_results),
        "rejected_sample_count": sum(
            1 for r in final_results
            if len(r.tags_rejected) > 0
        ),
        "total_rejected_observations": sum(
            len(r.tags_rejected)
            for r in final_results
        ),
    }

    write_manifest(
        out_dir / "run_manifest.json",
        manifest,
    )

    print("")
    print("=" * 82)
    print("Solver #4 Best-Configuration Before/After Diagnostics")
    print("=" * 82)
    print(f"Prepared dataset   : {prepared_path}")
    print(f"Output directory   : {out_dir}")
    print("")
    print("Robust configuration:")
    print(
        f"  threshold        : "
        f"{robust_config.rejection_score_threshold}"
    )
    print(
        f"  min_score_gap    : "
        f"{robust_config.min_score_gap}"
    )
    print(
        f"  max_rejections   : "
        f"{robust_config.max_rejections}"
    )
    print("")
    print(
        f"Rejected samples   : "
        f"{manifest['rejected_sample_count']}"
    )
    print(
        f"Rejected obs       : "
        f"{manifest['total_rejected_observations']}"
    )
    print("")
    print("Before -> After:")
    print(
        f"  position mean    : "
        f"{pass1_summary['position_mean_error_m']:.6f} "
        f"-> {final_summary['position_mean_error_m']:.6f} m"
    )
    print(
        f"  position p90     : "
        f"{pass1_summary['position_p90_error_m']:.6f} "
        f"-> {final_summary['position_p90_error_m']:.6f} m"
    )
    print(
        f"  position max     : "
        f"{pass1_summary['position_max_error_m']:.6f} "
        f"-> {final_summary['position_max_error_m']:.6f} m"
    )
    print(
        f"  heading p90      : "
        f"{pass1_summary['heading_p90_abs_error_deg']:.6f} "
        f"-> {final_summary['heading_p90_abs_error_deg']:.6f} deg"
    )
    print(
        f"  joint 10/10      : "
        f"{pass1_summary['joint_within_10cm_10deg_rate']:.3f} "
        f"-> {final_summary['joint_within_10cm_10deg_rate']:.3f}"
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

    run_best_robust_diagnostics(
        prepared_subdir=PREPARED_SUBDIR,
        tag_map_path=TAG_MAP_PATH,
        tag_yaw_map_path=TAG_YAW_MAP_PATH,
    )
