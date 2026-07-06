from src.common.dataset_builder import build_prepared_samples


def test_build_prepared_samples() -> None:
    rows = [
        {
            "sample_uid": "s1",
            "dataset_id": "main29",
            "source_role": "test",
            "gt_x_m": "1.0",
            "gt_y_m": "2.0",
            "gt_heading_deg": "45.0",
            "observation_uid": "o1",
            "tag_id": "31",
            "camera_role": "front",
            "meas_distance_m": "2.5",
            "meas_angle_deg": "10.0",
            "meas_yaw_deg": "20.0",
            "yaw_sign_corrected_deg": "20.0",
            "distance_weight_default": "1.0",
            "angle_weight_default": "1.0",
            "yaw_weight_default": "1.0",
            "geometry_gate_pass": "Y",
            "sample_quality_pass": "Y",
            "yaw_use_offline_label": "Y",
            "watch_tag40": "N",
            "clean_core_pass": "Y",
            "true_distance_m": "2.4",
            "distance_error_m": "0.1",
            "true_angle_deg": "9.0",
            "angle_error_deg": "1.0",
            "true_yaw_deg": "18.0",
            "yaw_error_deg": "2.0",
            "screening_notes": "pass",
        },
        {
            "sample_uid": "s1",
            "dataset_id": "main29",
            "source_role": "test",
            "gt_x_m": "1.0",
            "gt_y_m": "2.0",
            "gt_heading_deg": "45.0",
            "observation_uid": "o2",
            "tag_id": "32",
            "camera_role": "front",
            "meas_distance_m": "3.0",
            "meas_angle_deg": "-5.0",
            "meas_yaw_deg": "-15.0",
            "yaw_sign_corrected_deg": "-15.0",
            "distance_weight_default": "1.0",
            "angle_weight_default": "1.0",
            "yaw_weight_default": "1.0",
            "geometry_gate_pass": "Y",
            "sample_quality_pass": "Y",
            "yaw_use_offline_label": "Y",
            "watch_tag40": "N",
            "clean_core_pass": "Y",
            "true_distance_m": "3.1",
            "distance_error_m": "-0.1",
            "true_angle_deg": "-4.0",
            "angle_error_deg": "-1.0",
            "true_yaw_deg": "-14.0",
            "yaw_error_deg": "-1.0",
            "screening_notes": "pass",
        },
    ]

    samples = build_prepared_samples(rows)

    assert len(samples) == 1
    assert samples[0]["sample_uid"] == "s1"
    assert samples[0]["observation_count"] == 2
    assert samples[0]["observations"][0]["tag_id"] == 31
