from __future__ import annotations

from typing import Any, Dict, List

from src.common.data_loader import group_rows_by_sample


GT_FIELDS = ("gt_x_m", "gt_y_m", "gt_heading_deg")


def _to_float(value: Any, field_name: str) -> float:
    try:
        if value is None or str(value).strip() == "":
            raise ValueError
        return float(value)
    except Exception as exc:
        raise ValueError(
            f"Invalid numeric value for {field_name!r}: {value!r}"
        ) from exc


def _to_int(value: Any, field_name: str) -> int:
    try:
        if value is None or str(value).strip() == "":
            raise ValueError
        return int(float(value))
    except Exception as exc:
        raise ValueError(
            f"Invalid integer value for {field_name!r}: {value!r}"
        ) from exc


def _same_float(a: float, b: float, tol: float = 1e-9) -> bool:
    return abs(float(a) - float(b)) <= tol


def build_prepared_samples(
    rows: List[Dict[str, str]],
    sample_field: str = "sample_uid",
) -> List[Dict[str, Any]]:
    """
    Convert long-format CSV rows into one structured object per sample.

    Important:
    - preserves every observation row
    - does not perform new screening
    - does not call any location solver
    - validates GT pose consistency inside each sample
    """
    grouped = group_rows_by_sample(rows, sample_field=sample_field)

    prepared: List[Dict[str, Any]] = []

    for sample_uid in sorted(grouped):
        sample_rows = grouped[sample_uid]
        if not sample_rows:
            continue

        first = sample_rows[0]

        gt_x = _to_float(first.get("gt_x_m"), "gt_x_m")
        gt_y = _to_float(first.get("gt_y_m"), "gt_y_m")
        gt_h = _to_float(first.get("gt_heading_deg"), "gt_heading_deg")

        observations: List[Dict[str, Any]] = []

        for row_index, row in enumerate(sample_rows, start=1):
            row_gt_x = _to_float(row.get("gt_x_m"), "gt_x_m")
            row_gt_y = _to_float(row.get("gt_y_m"), "gt_y_m")
            row_gt_h = _to_float(row.get("gt_heading_deg"), "gt_heading_deg")

            if not (
                _same_float(row_gt_x, gt_x)
                and _same_float(row_gt_y, gt_y)
                and _same_float(row_gt_h, gt_h)
            ):
                raise ValueError(
                    f"Inconsistent GT pose inside sample {sample_uid!r} "
                    f"at row {row_index}: "
                    f"expected ({gt_x}, {gt_y}, {gt_h}), "
                    f"got ({row_gt_x}, {row_gt_y}, {row_gt_h})"
                )

            obs = {
                "observation_uid": str(
                    row.get("observation_uid")
                    or f"{sample_uid}_row{row_index:03d}"
                ),
                "tag_id": _to_int(row.get("tag_id"), "tag_id"),
                "camera_role": str(row.get("camera_role") or "").strip().lower(),

                "measured": {
                    "distance_m": _to_float(
                        row.get("meas_distance_m"), "meas_distance_m"
                    ),
                    "angle_deg": _to_float(
                        row.get("meas_angle_deg"), "meas_angle_deg"
                    ),
                    "yaw_deg": _to_float(
                        row.get("meas_yaw_deg"), "meas_yaw_deg"
                    ),
                    "yaw_sign_corrected_deg": (
                        None
                        if str(row.get("yaw_sign_corrected_deg") or "").strip() == ""
                        else _to_float(
                            row.get("yaw_sign_corrected_deg"),
                            "yaw_sign_corrected_deg",
                        )
                    ),
                },

                "weights": {
                    "distance": _to_float(
                        row.get("distance_weight_default"),
                        "distance_weight_default",
                    ),
                    "angle": _to_float(
                        row.get("angle_weight_default"),
                        "angle_weight_default",
                    ),
                    "yaw": _to_float(
                        row.get("yaw_weight_default"),
                        "yaw_weight_default",
                    ),
                },

                "flags": {
                    "geometry_gate_pass": (
                        str(row.get("geometry_gate_pass") or "").strip().upper() == "Y"
                    ),
                    "sample_quality_pass": (
                        str(row.get("sample_quality_pass") or "").strip().upper() == "Y"
                    ),
                    "yaw_use_offline_label": (
                        str(row.get("yaw_use_offline_label") or "").strip().upper() == "Y"
                    ),
                    "watch_tag40": (
                        str(row.get("watch_tag40") or "").strip().upper() == "Y"
                    ),
                    "clean_core_pass": (
                        str(row.get("clean_core_pass") or "").strip().upper() == "Y"
                    ),
                },

                "evaluation": {
                    "true_distance_m": _to_float(
                        row.get("true_distance_m"), "true_distance_m"
                    ),
                    "distance_error_m": _to_float(
                        row.get("distance_error_m"), "distance_error_m"
                    ),
                    "true_angle_deg": _to_float(
                        row.get("true_angle_deg"), "true_angle_deg"
                    ),
                    "angle_error_deg": _to_float(
                        row.get("angle_error_deg"), "angle_error_deg"
                    ),
                    "true_yaw_deg": _to_float(
                        row.get("true_yaw_deg"), "true_yaw_deg"
                    ),
                    "yaw_error_deg": _to_float(
                        row.get("yaw_error_deg"), "yaw_error_deg"
                    ),
                },

                "screening_notes": str(row.get("screening_notes") or ""),
            }

            observations.append(obs)

        prepared.append({
            "sample_uid": sample_uid,
            "dataset_id": str(first.get("dataset_id") or ""),
            "source_role": str(first.get("source_role") or ""),
            "ground_truth": {
                "x_m": gt_x,
                "y_m": gt_y,
                "heading_deg": gt_h,
            },
            "observation_count": len(observations),
            "observations": observations,
        })

    return prepared
