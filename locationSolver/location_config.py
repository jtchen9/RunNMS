"""
location_config.py

Reusable configuration for DemoRoom location geometry and solver.

This file should contain stable parameters that are shared by test scripts
and production-evolving solver code.
"""

CAMERA_CONFIG = {
    "front": {
        "heading_offset_deg": 0.0,
        "forward_offset_m": 0.055,
    },
    "rear": {
        "heading_offset_deg": 180.0,
        "forward_offset_m": -0.075,
    },
}

TAG_YAW_BY_FACING = {
    "right": 0.0,
    "up": 90.0,
    "left": 180.0,
    "down": 270.0,
}

ANGLE_CONVENTION = {
    "heading_zero_deg": "+x",
    "heading_90_deg": "+y",
    "view_angle_deg": "camera_heading_deg - bearing_camera_to_tag_deg",
    "yaw_deg": "tag_yaw_deg + 180 - camera_heading_deg + view_angle_deg",
}
