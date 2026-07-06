from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple
from src.common.metrics import fraction_le, heading_error_deg, mean, median, percentile_nearest_rank, position_error_m
from src.common.solver_result import SolverResult

def evaluate_solver_results(prepared_samples: List[Dict[str, Any]], solver_results: Iterable[SolverResult]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    result_by_uid: Dict[str, SolverResult] = {}
    for r in solver_results:
        if r.sample_uid in result_by_uid:
            raise ValueError(f"Duplicate SolverResult for {r.sample_uid}")
        result_by_uid[r.sample_uid] = r

    rows: List[Dict[str, Any]] = []
    for sample in prepared_samples:
        uid=str(sample["sample_uid"]); gt=sample["ground_truth"]; r=result_by_uid.get(uid)
        row={"sample_uid":uid,"gt_x_m":float(gt["x_m"]),"gt_y_m":float(gt["y_m"]),"gt_heading_deg":float(gt["heading_deg"]),"observation_count":int(sample.get("observation_count",0))}
        if r is None:
            row.update({"success":False,"failure_reason":"missing_solver_result","estimated_x_m":None,"estimated_y_m":None,"estimated_heading_deg":None,"position_error_m":None,"signed_heading_error_deg":None,"abs_heading_error_deg":None,"within_5cm":False,"within_10cm":False,"within_20cm":False,"within_5deg":False,"within_10deg":False,"within_20deg":False,"within_10cm_10deg":False,"within_20cm_20deg":False,"iterations":None,"runtime_ms":None,"objective_value":None,"tag_count_input":0,"tag_count_used":0,"tag_count_rejected":0,"tags_input":"","tags_used":"","tags_rejected":""})
            rows.append(row); continue
        pos_ok=r.success and r.estimated_x_m is not None and r.estimated_y_m is not None
        head_ok=r.success and r.estimated_heading_deg is not None
        pe=position_error_m(r.estimated_x_m,r.estimated_y_m,row["gt_x_m"],row["gt_y_m"]) if pos_ok else None
        she=heading_error_deg(r.estimated_heading_deg,row["gt_heading_deg"]) if head_ok else None
        ahe=abs(she) if she is not None else None
        row.update({"success":bool(r.success),"failure_reason":str(r.failure_reason or ""),"estimated_x_m":r.estimated_x_m,"estimated_y_m":r.estimated_y_m,"estimated_heading_deg":r.estimated_heading_deg,"position_error_m":pe,"signed_heading_error_deg":she,"abs_heading_error_deg":ahe,"within_5cm":bool(pe is not None and pe<=0.05),"within_10cm":bool(pe is not None and pe<=0.10),"within_20cm":bool(pe is not None and pe<=0.20),"within_5deg":bool(ahe is not None and ahe<=5.0),"within_10deg":bool(ahe is not None and ahe<=10.0),"within_20deg":bool(ahe is not None and ahe<=20.0),"within_10cm_10deg":bool(pe is not None and ahe is not None and pe<=0.10 and ahe<=10.0),"within_20cm_20deg":bool(pe is not None and ahe is not None and pe<=0.20 and ahe<=20.0),"iterations":r.iterations,"runtime_ms":r.runtime_ms,"objective_value":r.objective_value,"tag_count_input":len(r.tags_input),"tag_count_used":len(r.tags_used),"tag_count_rejected":len(r.tags_rejected),"tags_input":";".join(str(x) for x in r.tags_input),"tags_used":";".join(str(x) for x in r.tags_used),"tags_rejected":";".join(str(x) for x in r.tags_rejected)})
        rows.append(row)
    return rows, summarize_sample_rows(rows)

def summarize_sample_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total=len(rows); succ=[r for r in rows if r["success"]]; pos=[float(r["position_error_m"]) for r in rows if r["position_error_m"] is not None]; ang=[float(r["abs_heading_error_deg"]) for r in rows if r["abs_heading_error_deg"] is not None]
    s={"sample_count_total":total,"solver_success_count":len(succ),"solver_failure_count":total-len(succ),"solver_success_rate":len(succ)/total if total else 0.0,"position_coverage_count":len(pos),"position_coverage_rate":len(pos)/total if total else 0.0,"heading_coverage_count":len(ang),"heading_coverage_rate":len(ang)/total if total else 0.0}
    if pos:
        s.update({"position_mean_error_m":mean(pos),"position_median_error_m":median(pos),"position_p90_error_m":percentile_nearest_rank(pos,90),"position_p95_error_m":percentile_nearest_rank(pos,95),"position_max_error_m":max(pos),"position_within_5cm_rate":fraction_le(pos,0.05),"position_within_10cm_rate":fraction_le(pos,0.10),"position_within_20cm_rate":fraction_le(pos,0.20)})
    else:
        s.update({k:None for k in ["position_mean_error_m","position_median_error_m","position_p90_error_m","position_p95_error_m","position_max_error_m","position_within_5cm_rate","position_within_10cm_rate","position_within_20cm_rate"]})
    if ang:
        s.update({"heading_mean_abs_error_deg":mean(ang),"heading_median_abs_error_deg":median(ang),"heading_p90_abs_error_deg":percentile_nearest_rank(ang,90),"heading_p95_abs_error_deg":percentile_nearest_rank(ang,95),"heading_max_abs_error_deg":max(ang),"heading_within_5deg_rate":fraction_le(ang,5),"heading_within_10deg_rate":fraction_le(ang,10),"heading_within_20deg_rate":fraction_le(ang,20)})
    else:
        s.update({k:None for k in ["heading_mean_abs_error_deg","heading_median_abs_error_deg","heading_p90_abs_error_deg","heading_p95_abs_error_deg","heading_max_abs_error_deg","heading_within_5deg_rate","heading_within_10deg_rate","heading_within_20deg_rate"]})
    s["joint_within_10cm_10deg_rate"]=sum(bool(r["within_10cm_10deg"]) for r in rows)/total if total else 0.0
    s["joint_within_20cm_20deg_rate"]=sum(bool(r["within_20cm_20deg"]) for r in rows)/total if total else 0.0
    return s
