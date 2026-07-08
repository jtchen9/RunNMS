from __future__ import annotations

"""
Frozen operational configuration for robot_location_v1_1.

Historical development pipeline:
    Layer-1 physical whole-tag screen
    unchanged Solver #2 D+A Pass-1
    direct M2 distance screen with minimum 3 active distances
    no second-stage angle screen
    GT-free yaw keep/flip/reject from Pass-1 pose
    final component-wise D/A/Y solver
"""

from .component_preparation import (
    M2DistanceScreenConfig,
    YawAdmissionConfig,
)
from .pass1_solver import DistanceAngleSolverConfig
from .final_solver import (
    ComponentwiseDistanceAngleYawSolverConfig,
)


SOLVER_VERSION = "robot_location_v1_1"

FRONT_MAX_DISTANCE_M = 5.5
FRONT_MAX_ABS_ANGLE_DEG = 30.0

PASS1_CONFIG = DistanceAngleSolverConfig()

M2_CONFIG = M2DistanceScreenConfig(
    abs_m2_threshold=0.95,
    min_active_distances_after_m2=3,
)

YAW_CONFIG = YawAdmissionConfig(
    min_abs_predicted_yaw_deg=8.0,
    yaw_accept_best_error_deg=10.0,
    yaw_accept_separation_deg=15.0,
)

FINAL_CONFIG = ComponentwiseDistanceAngleYawSolverConfig(
    distance_sigma_m=0.05,
    angle_sigma_deg=2.5,
    yaw_sigma_deg=3.0,
    distance_global_weight=1.0,
    angle_global_weight=1.0,
    yaw_global_weight=0.75,
)
