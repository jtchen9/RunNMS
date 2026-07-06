from src.common.evaluator import evaluate_solver_results
from src.common.solver_result import SolverResult

def test_evaluator_common_contract() -> None:
    samples=[{"sample_uid":"s1","ground_truth":{"x_m":1.0,"y_m":2.0,"heading_deg":350.0},"observation_count":3},{"sample_uid":"s2","ground_truth":{"x_m":0.0,"y_m":0.0,"heading_deg":90.0},"observation_count":4}]
    results=[SolverResult(sample_uid="s1",success=True,estimated_x_m=1.03,estimated_y_m=2.04,estimated_heading_deg=5.0,tags_input=[31,32,33],tags_used=[31,32,33]),SolverResult(sample_uid="s2",success=False,failure_reason="test_failure")]
    rows,summary=evaluate_solver_results(samples,results)
    assert len(rows)==2
    assert round(rows[0]["position_error_m"],6)==0.05
    assert rows[0]["abs_heading_error_deg"]==15.0
    assert summary["sample_count_total"]==2
    assert summary["solver_success_count"]==1
