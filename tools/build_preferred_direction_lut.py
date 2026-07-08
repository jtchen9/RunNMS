#!/usr/bin/env python3
from __future__ import annotations

"""
Offline preferred-direction LUT generator.

Frozen operational rule:
    preferred_direction_v1
Historical origin:
    former experimental Rule C

Default placement:
    D:\Data\_Action\_RunNMS\tools\build_preferred_direction_lut.py

Reads:
    sitemap\DemoRoom\tag_location.txt

Writes:
    sitemap\DemoRoom\preferred_direction_lut\

This tool is offline only. Regenerate after tag-location updates.
"""

import argparse
import hashlib
import json
import math
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np


ROOT_DIR = Path(__file__).resolve().parents[1]

DEFAULT_TAG_FILE = (
    ROOT_DIR / "sitemap" / "DemoRoom" / "tag_location.txt"
)

DEFAULT_OUTPUT_DIR = (
    ROOT_DIR
    / "sitemap"
    / "DemoRoom"
    / "preferred_direction_lut"
)


# ---------------------------------------------------------------------------
# Frozen preferred_direction_v1 rule
# ---------------------------------------------------------------------------

RULE_ID = "preferred_direction_v1"
RULE_HISTORY = "former_rule_C"

GRID_STEP_M = 0.1
HEADING_STEP_DEG = 5.0

X_MIN_M = 0.0
X_MAX_M = 11.4
Y_MIN_M = 0.0
Y_MAX_M = 11.4

INNER_Y_MAX_M = 5.20
OUTER_Y_MIN_M = 5.40

DISTANCE_MIN_M = 0.4

FRONT_FORWARD_OFFSET_M = 0.055
REAR_FORWARD_OFFSET_M = -0.075

FRONT_HALF_FOV_DEG = 35.0
REAR_HALF_FOV_DEG = 15.0
TAG_INCIDENCE_LIMIT_DEG = 60.0

FRONT_DISTANCE_FULL_M = 4.0
FRONT_DISTANCE_SOFT1_M = 5.0
FRONT_DISTANCE_MAX_M = 6.0
FRONT_DISTANCE_SOFT1_WEIGHT = 0.7
FRONT_DISTANCE_SOFT2_WEIGHT = 0.4

REAR_DISTANCE_FULL_M = 2.5
REAR_DISTANCE_SOFT_M = 3.0
REAR_DISTANCE_MAX_M = 3.0
REAR_DISTANCE_SOFT_WEIGHT = 0.5

FRONT_CONFIDENCE = 1.0
REAR_CONFIDENCE = 0.6

MIN_GOOD_EFFECTIVE = 3.0
MIN_MARGINAL_EFFECTIVE = 2.0
MIN_GOOD_GEOMETRY_SPAN_DEG = 45.0

SCORE_GEOMETRY_BONUS_PER_DEG = 0.10


FACING_TO_YAW = {
    "right": 0.0,
    "up": 90.0,
    "left": 180.0,
    "down": 270.0,
}


@dataclass(frozen=True)
class Tag:
    tag_id: int
    x_m: float
    y_m: float
    yaw_deg: float
    facing: str
    room: str


@dataclass(frozen=True)
class CameraCfg:
    name: str
    heading_offset_deg: float
    forward_offset_m: float
    usable_half_fov_deg: float
    confidence: float


def deg_norm_360(a: float) -> float:
    return float(a % 360.0)


def wrap_angle_deg(a: float) -> float:
    return float((a + 180.0) % 360.0 - 180.0)


def angle_diff_abs(a: float, b: float) -> float:
    return abs(wrap_angle_deg(a - b))


def circular_span_deg(angles_deg: List[float]) -> float:
    if len(angles_deg) <= 1:
        return 0.0

    vals = sorted(deg_norm_360(a) for a in angles_deg)

    gaps = [
        vals[i + 1] - vals[i]
        for i in range(len(vals) - 1)
    ]
    gaps.append(vals[0] + 360.0 - vals[-1])

    return float(360.0 - max(gaps))


def bearing_world_deg(
    x0: float,
    y0: float,
    x1: float,
    y1: float,
) -> float:
    return deg_norm_360(
        math.degrees(math.atan2(y1 - y0, x1 - x0))
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            h.update(chunk)

    return h.hexdigest()


def parse_tag_location(path: Path) -> List[Tag]:
    lines = path.read_text(
        encoding="utf-8",
        errors="ignore",
    ).splitlines()

    room = None
    facing = None
    tags: List[Tag] = []

    room_re = re.compile(
        r"^\s*(Outer Room|Inner Room)\s*$",
        re.I,
    )
    facing_re = re.compile(
        r"^\s*===\s*Tags Facing\s+"
        r"(Down|Right|Up|Left)\s*$",
        re.I,
    )

    # Accept both:
    #   Tag#40: (1.200, 4.703)
    #   Tag#40: (1.200 4.703)
    tag_re = re.compile(
        r"Tag#\s*(\d+)\s*:\s*"
        r"\(\s*([-+]?\d+(?:\.\d+)?)"
        r"\s*,?\s+"
        r"([-+]?\d+(?:\.\d+)?)\s*\)"
    )

    for line in lines:
        m = room_re.search(line)
        if m:
            room = (
                "outer"
                if m.group(1).lower().startswith("outer")
                else "inner"
            )
            continue

        m = facing_re.search(line)
        if m:
            facing = m.group(1).lower()
            continue

        m = tag_re.search(line)
        if m and room and facing:
            tags.append(
                Tag(
                    tag_id=int(m.group(1)),
                    x_m=float(m.group(2)),
                    y_m=float(m.group(3)),
                    yaw_deg=FACING_TO_YAW[facing],
                    facing=facing,
                    room=room,
                )
            )

    tags.sort(key=lambda t: t.tag_id)

    if not tags:
        raise ValueError(f"No tags parsed from {path}")

    return tags


def classify_room(x: float, y: float) -> str:
    if y <= INNER_Y_MAX_M:
        return "inner"

    if y >= OUTER_Y_MIN_M:
        return "outer"

    return "blocked"


def eligible_tags_for_room(
    tags: List[Tag],
    room: str,
) -> List[Tag]:
    if room not in {"inner", "outer"}:
        return []

    return [
        t for t in tags
        if t.room == room
    ]


def camera_pose(
    robot_x: float,
    robot_y: float,
    robot_h_deg: float,
    cam: CameraCfg,
) -> Tuple[float, float, float]:
    hr = math.radians(robot_h_deg)

    cx = robot_x + cam.forward_offset_m * math.cos(hr)
    cy = robot_y + cam.forward_offset_m * math.sin(hr)
    ch = deg_norm_360(
        robot_h_deg + cam.heading_offset_deg
    )

    return cx, cy, ch


def distance_weight_for_camera(
    d: float,
    camera_name: str,
) -> float:
    if d < DISTANCE_MIN_M:
        return 0.0

    if camera_name == "rear":
        if d <= REAR_DISTANCE_FULL_M:
            return 1.0

        if d <= REAR_DISTANCE_SOFT_M:
            return REAR_DISTANCE_SOFT_WEIGHT

        return 0.0

    if d <= FRONT_DISTANCE_FULL_M:
        return 1.0

    if d <= FRONT_DISTANCE_SOFT1_M:
        return FRONT_DISTANCE_SOFT1_WEIGHT

    if d <= FRONT_DISTANCE_MAX_M:
        return FRONT_DISTANCE_SOFT2_WEIGHT

    return 0.0


def tag_is_good_for_camera(
    x: float,
    y: float,
    h: float,
    tag: Tag,
    cam: CameraCfg,
) -> Tuple[bool, Dict[str, float]]:
    cx, cy, ch = camera_pose(
        x,
        y,
        h,
        cam,
    )

    dx = tag.x_m - cx
    dy = tag.y_m - cy
    d = math.hypot(dx, dy)

    if d < 1e-9:
        return False, {
            "distance_m": d,
            "distance_weight": 0.0,
            "camera_view_angle_deg": 999.0,
            "tag_incidence_deg": 999.0,
            "bearing_world_deg": 0.0,
        }

    tag_bearing = deg_norm_360(
        math.degrees(math.atan2(dy, dx))
    )

    camera_view_angle = wrap_angle_deg(
        ch - tag_bearing
    )

    camera_from_tag_bearing = deg_norm_360(
        tag_bearing + 180.0
    )

    tag_incidence = angle_diff_abs(
        camera_from_tag_bearing,
        tag.yaw_deg,
    )

    distance_weight = distance_weight_for_camera(
        d,
        cam.name,
    )

    ok = (
        distance_weight > 0.0
        and abs(camera_view_angle) <= cam.usable_half_fov_deg
        and tag_incidence <= TAG_INCIDENCE_LIMIT_DEG
    )

    return ok, {
        "distance_m": float(d),
        "distance_weight": float(distance_weight),
        "camera_view_angle_deg": float(camera_view_angle),
        "tag_incidence_deg": float(tag_incidence),
        "bearing_world_deg": float(tag_bearing),
    }


def eval_pose_heading(
    x: float,
    y: float,
    h: float,
    tags: List[Tag],
    cameras: List[CameraCfg],
) -> Dict[str, float | int | str]:
    room = classify_room(x, y)

    eligible = eligible_tags_for_room(
        tags,
        room,
    )

    front_count = 0
    rear_count = 0

    front_weighted = 0.0
    rear_weighted = 0.0

    bearings: List[float] = []

    for cam in cameras:
        for tag in eligible:
            ok, metrics = tag_is_good_for_camera(
                x,
                y,
                h,
                tag,
                cam,
            )

            if not ok:
                continue

            contribution = (
                cam.confidence
                * float(metrics["distance_weight"])
            )

            if cam.name == "front":
                front_count += 1
                front_weighted += contribution
            else:
                rear_count += 1
                rear_weighted += contribution

            bearings.append(
                bearing_world_deg(
                    x,
                    y,
                    tag.x_m,
                    tag.y_m,
                )
            )

    effective = (
        front_weighted
        + rear_weighted
    )

    geometry_span = circular_span_deg(
        bearings
    )

    score = (
        10.0 * effective
        + SCORE_GEOMETRY_BONUS_PER_DEG
        * geometry_span
    )

    if room == "blocked":
        status = "BLOCKED"

    elif (
        effective >= MIN_GOOD_EFFECTIVE
        and geometry_span
        >= MIN_GOOD_GEOMETRY_SPAN_DEG
    ):
        status = "GOOD"

    elif effective >= MIN_MARGINAL_EFFECTIVE:
        status = "MARGINAL"

    else:
        status = "BAD"

    return {
        "heading_deg": float(h),
        "room": room,

        "front_good_count": int(front_count),
        "rear_good_count": int(rear_count),
        "good_total_count": int(
            front_count + rear_count
        ),

        "effective_good_count": float(effective),
        "geometry_span_deg": float(geometry_span),
        "score": float(score),
        "status": status,
    }


def choose_best_heading(
    results: List[Dict[str, float | int | str]],
) -> Dict[str, float | int | str]:
    status_rank = {
        "GOOD": 3,
        "MARGINAL": 2,
        "BAD": 1,
        "BLOCKED": 0,
    }

    # Exact operational tie-break order from the proven T2 implementation.
    # Do not change casually; even a "harmless" extra tie-break field can
    # alter headings at equal-score cells.
    return max(
        results,
        key=lambda r: (
            float(r["score"]),
            status_rank.get(
                str(r["status"]),
                0,
            ),
            int(r["good_total_count"]),
            float(r["geometry_span_deg"]),
        ),
    )


def grid_values(
    vmin: float,
    vmax: float,
    step: float,
) -> np.ndarray:
    n = int(
        math.floor(
            (vmax - vmin) / step
            + 1e-9
        )
    ) + 1

    return np.round(
        np.array(
            [
                vmin + i * step
                for i in range(n)
            ],
            dtype=np.float32,
        ),
        6,
    )


def heading_values(
    step_deg: float,
) -> np.ndarray:
    n = int(
        round(360.0 / step_deg)
    )

    return np.array(
        [
            i * step_deg
            for i in range(n)
        ],
        dtype=np.float32,
    )


def build_lut(
    tag_file: Path,
    output_dir: Path,
) -> None:
    tags = parse_tag_location(
        tag_file
    )

    cameras = [
        CameraCfg(
            name="front",
            heading_offset_deg=0.0,
            forward_offset_m=FRONT_FORWARD_OFFSET_M,
            usable_half_fov_deg=FRONT_HALF_FOV_DEG,
            confidence=FRONT_CONFIDENCE,
        ),
        CameraCfg(
            name="rear",
            heading_offset_deg=180.0,
            forward_offset_m=REAR_FORWARD_OFFSET_M,
            usable_half_fov_deg=REAR_HALF_FOV_DEG,
            confidence=REAR_CONFIDENCE,
        ),
    ]

    xs = grid_values(
        X_MIN_M,
        X_MAX_M,
        GRID_STEP_M,
    )

    ys = grid_values(
        Y_MIN_M,
        Y_MAX_M,
        GRID_STEP_M,
    )

    hs = heading_values(
        HEADING_STEP_DEG
    )

    ny = len(ys)
    nx = len(xs)

    preferred_heading = np.full(
        (ny, nx),
        np.nan,
        dtype=np.float32,
    )

    best_score = np.full(
        (ny, nx),
        np.nan,
        dtype=np.float32,
    )

    status_code = np.zeros(
        (ny, nx),
        dtype=np.uint8,
    )

    effective_count = np.zeros(
        (ny, nx),
        dtype=np.float32,
    )

    geometry_span = np.zeros(
        (ny, nx),
        dtype=np.float32,
    )

    status_to_code = {
        "BLOCKED": 0,
        "BAD": 1,
        "MARGINAL": 2,
        "GOOD": 3,
    }

    t0 = time.time()

    for iy, y in enumerate(ys):
        if iy % 10 == 0:
            print(
                f"row {iy + 1}/{ny}, "
                f"y={float(y):.2f}, "
                f"elapsed={time.time() - t0:.1f}s",
                flush=True,
            )

        for ix, x in enumerate(xs):
            room = classify_room(
                float(x),
                float(y),
            )

            if room == "blocked":
                continue

            results = [
                eval_pose_heading(
                    float(x),
                    float(y),
                    float(h),
                    tags,
                    cameras,
                )
                for h in hs
            ]

            best = choose_best_heading(
                results
            )

            preferred_heading[iy, ix] = float(
                best["heading_deg"]
            )

            best_score[iy, ix] = float(
                best["score"]
            )

            status_code[iy, ix] = (
                status_to_code[
                    str(best["status"])
                ]
            )

            effective_count[iy, ix] = float(
                best["effective_good_count"]
            )

            geometry_span[iy, ix] = float(
                best["geometry_span_deg"]
            )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    np.save(
        output_dir / "preferred_heading_deg.npy",
        preferred_heading,
    )

    np.save(
        output_dir / "preferred_heading_score.npy",
        best_score,
    )

    np.save(
        output_dir
        / "preferred_heading_status_code.npy",
        status_code,
    )

    np.save(
        output_dir
        / "preferred_heading_effective_count.npy",
        effective_count,
    )

    np.save(
        output_dir
        / "preferred_heading_geometry_span_deg.npy",
        geometry_span,
    )

    np.save(
        output_dir / "axis_x_m.npy",
        xs,
    )

    np.save(
        output_dir / "axis_y_m.npy",
        ys,
    )

    metadata = {
        "table_version": RULE_ID,
        "rule_history": RULE_HISTORY,

        "tag_map_path": str(tag_file),
        "tag_map_sha256": sha256_file(tag_file),

        "grid_step_m": GRID_STEP_M,
        "heading_step_deg": HEADING_STEP_DEG,

        "x_min_m": X_MIN_M,
        "x_max_m": X_MAX_M,
        "y_min_m": Y_MIN_M,
        "y_max_m": Y_MAX_M,

        "grid_shape_yx": [
            int(ny),
            int(nx),
        ],

        "heading_convention": (
            "0=+x,90=+y,deg_norm_360"
        ),

        "status_code": {
            "0": "BLOCKED",
            "1": "BAD",
            "2": "MARGINAL",
            "3": "GOOD",
        },

        "rule": {
            "front_forward_offset_m":
                FRONT_FORWARD_OFFSET_M,

            "rear_forward_offset_m":
                REAR_FORWARD_OFFSET_M,

            "front_half_fov_deg":
                FRONT_HALF_FOV_DEG,

            "rear_half_fov_deg":
                REAR_HALF_FOV_DEG,

            "tag_incidence_limit_deg":
                TAG_INCIDENCE_LIMIT_DEG,

            "front_distance_full_m":
                FRONT_DISTANCE_FULL_M,

            "front_distance_soft1_m":
                FRONT_DISTANCE_SOFT1_M,

            "front_distance_max_m":
                FRONT_DISTANCE_MAX_M,

            "front_distance_soft1_weight":
                FRONT_DISTANCE_SOFT1_WEIGHT,

            "front_distance_soft2_weight":
                FRONT_DISTANCE_SOFT2_WEIGHT,

            "rear_distance_full_m":
                REAR_DISTANCE_FULL_M,

            "rear_distance_soft_m":
                REAR_DISTANCE_SOFT_M,

            "rear_distance_max_m":
                REAR_DISTANCE_MAX_M,

            "rear_distance_soft_weight":
                REAR_DISTANCE_SOFT_WEIGHT,

            "front_confidence":
                FRONT_CONFIDENCE,

            "rear_confidence":
                REAR_CONFIDENCE,

            "min_good_effective":
                MIN_GOOD_EFFECTIVE,

            "min_marginal_effective":
                MIN_MARGINAL_EFFECTIVE,

            "min_good_geometry_span_deg":
                MIN_GOOD_GEOMETRY_SPAN_DEG,

            "score_geometry_bonus_per_deg":
                SCORE_GEOMETRY_BONUS_PER_DEG,

            "inner_y_max_m":
                INNER_Y_MAX_M,

            "outer_y_min_m":
                OUTER_Y_MIN_M,
        },

        "generated_unix_time":
            time.time(),
    }

    (
        output_dir
        / "preferred_direction_meta.json"
    ).write_text(
        json.dumps(
            metadata,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("")
    print("Preferred-direction LUT complete")
    print(f"Rule       : {RULE_ID}")
    print(f"Tag map    : {tag_file}")
    print(f"Output dir : {output_dir}")
    print(
        f"Grid       : {nx} x {ny}, "
        f"step={GRID_STEP_M} m"
    )


def main() -> None:
    p = argparse.ArgumentParser(
        description=(
            "Build DemoRoom preferred-direction LUT "
            "using frozen preferred_direction_v1."
        )
    )

    p.add_argument(
        "--tag-file",
        default=str(DEFAULT_TAG_FILE),
    )

    p.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
    )

    args = p.parse_args()

    tag_file = Path(args.tag_file)
    output_dir = Path(args.output_dir)

    if not tag_file.exists():
        raise FileNotFoundError(
            f"Tag map not found: {tag_file}"
        )

    build_lut(
        tag_file=tag_file,
        output_dir=output_dir,
    )


if __name__ == "__main__":
    main()
