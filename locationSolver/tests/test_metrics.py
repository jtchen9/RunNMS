from src.common.metrics import wrap_angle_deg, heading_error_deg, position_error_m

def test_metrics():
    assert wrap_angle_deg(190.0) == -170.0
    assert heading_error_deg(5.0, 355.0) == 10.0
    assert position_error_m(3.0, 4.0, 0.0, 0.0) == 5.0
