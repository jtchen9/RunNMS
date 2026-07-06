from __future__ import annotations

import math
from typing import Iterable, Sequence

def wrap_angle_deg(angle_deg: float) -> float:
    return (float(angle_deg) + 180.0) % 360.0 - 180.0

def heading_error_deg(est_heading_deg: float, gt_heading_deg: float) -> float:
    return wrap_angle_deg(float(est_heading_deg) - float(gt_heading_deg))

def position_error_m(est_x_m: float, est_y_m: float, gt_x_m: float, gt_y_m: float) -> float:
    return math.hypot(float(est_x_m)-float(gt_x_m), float(est_y_m)-float(gt_y_m))

def mean(values: Sequence[float]) -> float:
    if not values: raise ValueError("mean() requires at least one value")
    return sum(float(v) for v in values) / len(values)

def median(values: Sequence[float]) -> float:
    if not values: raise ValueError("median() requires at least one value")
    xs=sorted(float(v) for v in values); n=len(xs); m=n//2
    return xs[m] if n%2 else 0.5*(xs[m-1]+xs[m])

def percentile_nearest_rank(values: Sequence[float], p: float) -> float:
    if not values: raise ValueError("percentile requires at least one value")
    if not 0.0 <= p <= 100.0: raise ValueError("p must be in [0, 100]")
    xs=sorted(float(v) for v in values)
    if p == 0.0: return xs[0]
    rank=math.ceil((p/100.0)*len(xs)); idx=min(len(xs)-1,max(0,rank-1))
    return xs[idx]

def fraction_le(values: Iterable[float], threshold: float) -> float:
    xs=[float(v) for v in values]
    if not xs: raise ValueError("fraction_le() requires at least one value")
    return sum(v <= float(threshold) for v in xs)/len(xs)
