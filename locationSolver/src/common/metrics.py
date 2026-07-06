from __future__ import annotations
import math

def wrap_angle_deg(angle_deg: float) -> float:
    return (float(angle_deg) + 180.0) % 360.0 - 180.0

def heading_error_deg(est_heading_deg: float, gt_heading_deg: float) -> float:
    return wrap_angle_deg(float(est_heading_deg) - float(gt_heading_deg))

def position_error_m(est_x_m: float, est_y_m: float, gt_x_m: float, gt_y_m: float) -> float:
    return math.hypot(float(est_x_m)-float(gt_x_m), float(est_y_m)-float(gt_y_m))
