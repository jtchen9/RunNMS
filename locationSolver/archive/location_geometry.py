"""
location_geometry.py

Part 1 + Part 2 reusable geometry functions.

This module contains no CSV logic, no pandas, no output files, and no test data
paths. It is intended to be imported by both test scripts and future production
solver code.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Mapping


@dataclass(frozen=True)
class RobotPose:
    x_m: float
    y_m: float
    heading_deg: float


@dataclass(frozen=True)
class CameraPose:
    x_m: float
    y_m: float
    heading_deg: float


@dataclass(frozen=True)
class PredictedMeasurement:
    distance_m: float
    angle_deg: float
    yaw_deg: float
    bearing_world_deg: float
    camera_pose: CameraPose


@dataclass(frozen=True)
class MeasurementResidual:
    distance_residual_m: float
    angle_residual_deg: float
    yaw_residual_deg: float


def deg_norm_360(angle_deg: float) -> float:
    """Normalize angle to [0, 360)."""
    return float(angle_deg % 360.0)


def wrap_angle_deg(angle_deg: float) -> float:
    """Normalize angle to [-180, 180)."""
    return float((angle_deg + 180.0) % 360.0 - 180.0)


def abs_angle_error_deg(a_deg: float, b_deg: float) -> float:
    """Absolute wrapped angle error."""
    return abs(wrap_angle_deg(a_deg - b_deg))


def bearing_world_deg(x0_m: float, y0_m: float, x1_m: float, y1_m: float) -> float:
    """
    World bearing from point (x0_m, y0_m) to point (x1_m, y1_m).

    Returns angle in degrees using project convention:
        0 deg   = +x
        90 deg  = +y
        180 deg = -x
        270 deg = -y
    """
    import math
    return deg_norm_360(math.degrees(math.atan2(y1_m - y0_m, x1_m - x0_m)))


def camera_pose_from_robot_pose(
    robot_pose: RobotPose,
    camera_cfg: Mapping[str, float],
) -> CameraPose:
    """
    Convert robot-center pose to camera pose.

    camera_cfg fields:
        heading_offset_deg
        forward_offset_m

    The offset is along the robot forward direction, not along camera heading.
    """
    h_rad = math.radians(robot_pose.heading_deg)
    forward_offset_m = float(camera_cfg["forward_offset_m"])

    camera_x = robot_pose.x_m + forward_offset_m * math.cos(h_rad)
    camera_y = robot_pose.y_m + forward_offset_m * math.sin(h_rad)
    camera_h = deg_norm_360(robot_pose.heading_deg + float(camera_cfg["heading_offset_deg"]))

    return CameraPose(
        x_m=float(camera_x),
        y_m=float(camera_y),
        heading_deg=float(camera_h),
    )


def predict_tag_measurement(
    robot_pose: RobotPose,
    tag_world,
    camera_cfg: Mapping[str, float],
) -> PredictedMeasurement:
    """
    Predict what one camera should measure for one tag.

    View-angle convention:
        angle_deg = camera_heading_deg - bearing_camera_to_tag_deg

    Yaw convention:
        yaw_deg = tag_yaw_deg + 180 - camera_heading_deg + angle_deg

    Both angle_deg and yaw_deg are returned in [-180, 180).
    """
    cam = camera_pose_from_robot_pose(robot_pose, camera_cfg)

    dx = float(tag_world.x_m) - cam.x_m
    dy = float(tag_world.y_m) - cam.y_m
    distance_m = math.hypot(dx, dy)

    if distance_m < 1e-12:
        bearing_world_deg = 0.0
    else:
        bearing_world_deg = deg_norm_360(math.degrees(math.atan2(dy, dx)))

    angle_deg = wrap_angle_deg(cam.heading_deg - bearing_world_deg)

    yaw_deg = wrap_angle_deg(
        float(tag_world.yaw_deg) + 180.0 - cam.heading_deg + angle_deg
    )

    return PredictedMeasurement(
        distance_m=float(distance_m),
        angle_deg=float(angle_deg),
        yaw_deg=float(yaw_deg),
        bearing_world_deg=float(bearing_world_deg),
        camera_pose=cam,
    )


def compute_measurement_residual(
    measured_distance_m: float,
    measured_angle_deg: float,
    measured_yaw_deg: float,
    predicted: PredictedMeasurement,
) -> MeasurementResidual:
    """
    Compute residual = measured - predicted.

    Distance residual is in meters.
    Angle and yaw residuals use wrapped subtraction in degrees.
    """
    return MeasurementResidual(
        distance_residual_m=float(measured_distance_m - predicted.distance_m),
        angle_residual_deg=float(wrap_angle_deg(measured_angle_deg - predicted.angle_deg)),
        yaw_residual_deg=float(wrap_angle_deg(measured_yaw_deg - predicted.yaw_deg)),
    )
