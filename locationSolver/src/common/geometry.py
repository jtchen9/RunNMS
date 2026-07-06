from __future__ import annotations

import math
from typing import Tuple


FRONT_CAMERA_FORWARD_OFFSET_M = 0.055
REAR_CAMERA_FORWARD_OFFSET_M = -0.075


def wrap_angle_deg(angle_deg: float) -> float:
    return (float(angle_deg) + 180.0) % 360.0 - 180.0


def camera_forward_offset_m(camera_role: str) -> float:
    role = str(camera_role or "").strip().lower()
    if role == "front":
        return FRONT_CAMERA_FORWARD_OFFSET_M
    if role == "rear":
        return REAR_CAMERA_FORWARD_OFFSET_M
    raise ValueError(f"Unknown camera_role: {camera_role!r}")


def camera_heading_deg(
    robot_heading_deg: float,
    camera_role: str,
) -> float:
    role = str(camera_role or "").strip().lower()
    if role == "front":
        return float(robot_heading_deg) % 360.0
    if role == "rear":
        return (float(robot_heading_deg) + 180.0) % 360.0
    raise ValueError(f"Unknown camera_role: {camera_role!r}")


def camera_xy_from_robot_pose(
    robot_x_m: float,
    robot_y_m: float,
    robot_heading_deg: float,
    camera_role: str,
) -> Tuple[float, float]:
    offset_m = camera_forward_offset_m(camera_role)
    h_rad = math.radians(float(robot_heading_deg))

    cam_x = float(robot_x_m) + offset_m * math.cos(h_rad)
    cam_y = float(robot_y_m) + offset_m * math.sin(h_rad)

    return cam_x, cam_y


def predicted_tag_distance_m(
    robot_x_m: float,
    robot_y_m: float,
    robot_heading_deg: float,
    camera_role: str,
    tag_x_m: float,
    tag_y_m: float,
) -> float:
    cam_x, cam_y = camera_xy_from_robot_pose(
        robot_x_m,
        robot_y_m,
        robot_heading_deg,
        camera_role,
    )

    return math.hypot(
        float(tag_x_m) - cam_x,
        float(tag_y_m) - cam_y,
    )


def predicted_tag_angle_deg(
    robot_x_m: float,
    robot_y_m: float,
    robot_heading_deg: float,
    camera_role: str,
    tag_x_m: float,
    tag_y_m: float,
) -> float:
    """
    Match the established project convention:

        bearing_world = atan2(tag_y - cam_y, tag_x - cam_x)
        pred_angle = wrap(camera_heading - bearing_world)

    Therefore positive/negative angle follows the current T11 convention.
    """
    cam_x, cam_y = camera_xy_from_robot_pose(
        robot_x_m,
        robot_y_m,
        robot_heading_deg,
        camera_role,
    )

    bearing_world_deg = math.degrees(
        math.atan2(
            float(tag_y_m) - cam_y,
            float(tag_x_m) - cam_x,
        )
    )

    cam_heading = camera_heading_deg(
        robot_heading_deg,
        camera_role,
    )

    return wrap_angle_deg(cam_heading - bearing_world_deg)
