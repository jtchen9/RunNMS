#!/usr/bin/env python3
from __future__ import annotations

r"""
Cross-check cleaned preferred_direction_v1 LUT against the proven T2 Rule-C
implementation already used in this project.

Default placement:
    D:\Data\_Action\_RunNMS\tools\crosscheck_preferred_direction_lut.py

Prerequisites:
    1. The LUT has already been generated.
    2. Proven T2 exists at project root:
           t2_build_preferred_heading_matrices.py

Default behavior:
    Compare a fixed set of representative 10-cm grid points.

Optional:
    --all-grid
        Recompute the proven T2 answer for every LUT grid cell.
        This is much slower and is intended only for a one-time deep check.

Pass criterion:
    preferred heading must match exactly at every compared non-blocked cell.
"""

import argparse
import math
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np


ROOT_DIR = Path(__file__).resolve().parents[1]

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import testLocation.t2_build_preferred_heading_matrices as t2


DEFAULT_TAG_FILE = (
    ROOT_DIR / "sitemap" / "DemoRoom" / "tag_location.txt"
)

DEFAULT_LUT_DIR = (
    ROOT_DIR
    / "sitemap"
    / "DemoRoom"
    / "preferred_direction_lut"
)


# Representative 0.1-m grid points across inner and outer areas.
DEFAULT_POINTS: List[Tuple[float, float]] = [
    (1.0, 1.0),
    (2.0, 2.0),
    (4.0, 1.5),
    (5.0, 1.0),
    (6.0, 3.5),
    (9.0, 1.5),
    (10.0, 4.0),
    (1.0, 6.0),
    (3.0, 8.0),
    (6.0, 8.0),
    (9.0, 8.0),
    (10.0, 10.0),
]


def build_proven_t2_cfg(
    tag_file: Path,
    output_dir: Path,
) -> t2.PlannerCfg:
    """
    Reconstruct the proven T2 Rule-C defaults exactly.
    """
    return t2.PlannerCfg(
        tag_file=str(tag_file),
        output_dir=str(output_dir),

        x_min=0.0,
        x_max=11.4,
        y_min=0.0,
        y_max=11.4,

        grid_step_m=0.1,
        heading_step_deg=5.0,

        inner_y_max=5.20,
        outer_y_min=5.40,

        distance_min_m=0.4,
        distance_max_m=6.0,

        front_distance_full_m=4.0,
        front_distance_soft1_m=5.0,
        front_distance_max_m=6.0,
        front_distance_soft1_weight=0.7,
        front_distance_soft2_weight=0.4,

        rear_distance_full_m=2.5,
        rear_distance_soft_m=3.0,
        rear_distance_max_m=3.0,
        rear_distance_soft_weight=0.5,

        tag_incidence_limit_deg=60.0,

        front_forward_offset_m=0.055,
        rear_forward_offset_m=-0.075,

        front_half_fov_deg=35.0,
        rear_half_fov_deg=15.0,

        front_confidence=1.0,
        rear_confidence=0.6,

        min_good_effective=3.0,
        min_marginal_effective=2.0,
        min_good_geometry_span_deg=45.0,

        score_geometry_bonus_per_deg=0.10,

        include_outer_tags=True,
        include_inner_tags=True,

        save_3d_matrices=False,
        make_png=False,
    )


def build_cameras(
    cfg: t2.PlannerCfg,
) -> List[t2.CameraCfg]:
    return [
        t2.CameraCfg(
            "front",
            0.0,
            cfg.front_forward_offset_m,
            cfg.front_half_fov_deg,
            cfg.front_confidence,
        ),
        t2.CameraCfg(
            "rear",
            180.0,
            cfg.rear_forward_offset_m,
            cfg.rear_half_fov_deg,
            cfg.rear_confidence,
        ),
    ]


def proven_heading_at(
    x: float,
    y: float,
    tags: List[t2.Tag],
    cameras: List[t2.CameraCfg],
    cfg: t2.PlannerCfg,
) -> Tuple[str, float]:
    room = t2.classify_room(x, y, cfg)

    if room == "blocked":
        return room, math.nan

    results = [
        t2.eval_pose_heading(
            x,
            y,
            float(h),
            tags,
            cameras,
            cfg,
        )
        for h in t2.heading_values(
            cfg.heading_step_deg
        )
    ]

    best, _ = t2.choose_best_heading(results)

    return room, float(best["heading_deg"]) % 360.0


def nearest_index(
    axis: np.ndarray,
    value: float,
) -> int:
    return int(
        np.abs(
            axis.astype(float)
            - float(value)
        ).argmin()
    )


def compare_point(
    x: float,
    y: float,
    heading_lut: np.ndarray,
    xs: np.ndarray,
    ys: np.ndarray,
    tags: List[t2.Tag],
    cameras: List[t2.CameraCfg],
    cfg: t2.PlannerCfg,
) -> Dict[str, object]:
    ix = nearest_index(xs, x)
    iy = nearest_index(ys, y)

    gx = float(xs[ix])
    gy = float(ys[iy])

    room, proven_h = proven_heading_at(
        gx,
        gy,
        tags,
        cameras,
        cfg,
    )

    lut_h = float(heading_lut[iy, ix])

    if room == "blocked":
        match = (
            not math.isfinite(proven_h)
            and not math.isfinite(lut_h)
        )
    else:
        match = (
            math.isfinite(lut_h)
            and t2.angle_diff_abs(
                lut_h,
                proven_h,
            ) < 1e-9
        )

    return {
        "requested_x_m": x,
        "requested_y_m": y,
        "grid_x_m": gx,
        "grid_y_m": gy,
        "room": room,
        "proven_t2_heading_deg": proven_h,
        "clean_lut_heading_deg": lut_h,
        "match": match,
    }


def main() -> None:
    p = argparse.ArgumentParser(
        description=(
            "Cross-check cleaned preferred_direction_v1 LUT "
            "against proven T2 Rule-C."
        )
    )

    p.add_argument(
        "--tag-file",
        default=str(DEFAULT_TAG_FILE),
    )

    p.add_argument(
        "--lut-dir",
        default=str(DEFAULT_LUT_DIR),
    )

    p.add_argument(
        "--all-grid",
        action="store_true",
        help=(
            "Compare every LUT grid cell. "
            "Slow; use for one-time deep validation."
        ),
    )

    args = p.parse_args()

    tag_file = Path(args.tag_file)
    lut_dir = Path(args.lut_dir)

    if not tag_file.exists():
        raise FileNotFoundError(
            f"Tag map not found: {tag_file}"
        )

    if not lut_dir.exists():
        raise FileNotFoundError(
            f"LUT directory not found: {lut_dir}"
        )

    heading_lut = np.load(
        lut_dir / "preferred_heading_deg.npy"
    )

    xs = np.load(
        lut_dir / "axis_x_m.npy"
    )

    ys = np.load(
        lut_dir / "axis_y_m.npy"
    )

    cfg = build_proven_t2_cfg(
        tag_file=tag_file,
        output_dir=lut_dir,
    )

    tags = t2.parse_tag_location(
        tag_file
    )

    cameras = build_cameras(cfg)

    if args.all_grid:
        points = [
            (float(x), float(y))
            for y in ys
            for x in xs
        ]
    else:
        points = list(DEFAULT_POINTS)

    rows = []

    for i, (x, y) in enumerate(points, start=1):
        if args.all_grid and i % 500 == 0:
            print(
                f"checked {i}/{len(points)}",
                flush=True,
            )

        rows.append(
            compare_point(
                x=x,
                y=y,
                heading_lut=heading_lut,
                xs=xs,
                ys=ys,
                tags=tags,
                cameras=cameras,
                cfg=cfg,
            )
        )

    mismatches = [
        row for row in rows
        if not bool(row["match"])
    ]

    print("")
    print("=" * 88)
    print("PREFERRED-DIRECTION LUT CROSS-CHECK")
    print("=" * 88)
    print(f"Compared cells : {len(rows)}")
    print(f"Mismatches     : {len(mismatches)}")
    print("")

    for row in rows:
        print(
            f"({row['grid_x_m']:.1f},{row['grid_y_m']:.1f}) "
            f"room={row['room']:<7} "
            f"T2={row['proven_t2_heading_deg']} "
            f"LUT={row['clean_lut_heading_deg']} "
            f"match={row['match']}"
        )

    print("")
    if mismatches:
        print("RESULT: FAIL")
        print("")
        print("Mismatch details:")
        for row in mismatches[:50]:
            print(row)
        raise SystemExit(1)

    print("RESULT: PASS")
    print("Clean LUT headings match proven T2 at all compared cells.")


if __name__ == "__main__":
    main()
