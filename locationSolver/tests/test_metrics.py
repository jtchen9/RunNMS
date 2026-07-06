from src.common.metrics import fraction_le, heading_error_deg, median, percentile_nearest_rank, position_error_m, wrap_angle_deg

def test_metrics() -> None:
    assert wrap_angle_deg(190.0) == -170.0
    assert wrap_angle_deg(-190.0) == 170.0
    assert heading_error_deg(5.0,355.0) == 10.0
    assert position_error_m(3.0,4.0,0.0,0.0) == 5.0
    assert median([1.0,3.0,2.0]) == 2.0
    assert percentile_nearest_rank([1,2,3,4],90) == 4.0
    assert fraction_le([0.05,0.10,0.20],0.10) == 2/3
