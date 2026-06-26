#!/usr/bin/env python3
"""
grid resolution: 0.1 m
heading resolution: 5¢X
front offset: +0.055 m
rear offset: -0.075 m
front usable half-FOV: ¡Ó35¢X
rear usable half-FOV: ¡Ó15¢X
distance range: 0.4¡V4.0 m
inner robot ¡÷ tags 31¡V58 only
outer robot ¡÷ tags 1¡V30 only
no doorway exceptions
==============
outputs:
preferred-heading lookup table
matrix files for later calculation
optional PNG heatmaps
==============
Quick Test commands:
 
python build_preferred_heading_matrices.py \
  --mode points \
  --tag-file "tag_location(3).txt" \
  --output-dir preferred_heading_test \
  --points "2,2;2,4;6,2;6,4;8,2;8,4;2,8;6,8;9,8" \
  --no-png
==============
Full matrix run:
ython build_preferred_heading_matrices.py \
  --mode full \
  --tag-file "tag_location(3).txt" \
  --output-dir preferred_heading_full
  
(ptional heatmaps are also generated unless you add "--no-png")
(save only 2D outputs, add "--no-3d-matrices"
==============
Important adjustable parameters
--rear-confidence 0.5
--rear-half-fov-deg 18
--front-half-fov-deg 35
--distance-max-m 4.0
--min-good-effective 3.0
--min-good-geometry-span-deg 45
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np


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
    gaps = [vals[i + 1] - vals[i] for i in range(len(vals) - 1)]
    gaps.append(vals[0] + 360.0 - vals[-1])
    return float(360.0 - max(gaps))


def bearing_world_deg(x0: float, y0: float, x1: float, y1: float) -> float:
    return deg_norm_360(math.degrees(math.atan2(y1 - y0, x1 - x0)))


@dataclass
class Tag:
    tag_id: int
    x_m: float
    y_m: float
    yaw_deg: float
    facing: str
    room: str


@dataclass
class CameraCfg:
    name: str
    heading_offset_deg: float
    forward_offset_m: float
    usable_half_fov_deg: float
    confidence: float


@dataclass
class PlannerCfg:
    tag_file: str
    output_dir: str
    x_min: float
    x_max: float
    y_min: float
    y_max: float
    grid_step_m: float
    heading_step_deg: float
    inner_y_max: float
    outer_y_min: float
    distance_min_m: float
    distance_max_m: float
    tag_incidence_limit_deg: float
    front_forward_offset_m: float
    rear_forward_offset_m: float
    front_half_fov_deg: float
    rear_half_fov_deg: float
    front_confidence: float
    rear_confidence: float
    min_good_effective: float
    min_marginal_effective: float
    min_good_geometry_span_deg: float
    score_geometry_bonus_per_deg: float
    include_outer_tags: bool
    include_inner_tags: bool
    save_3d_matrices: bool
    make_png: bool


FACING_TO_YAW = {"right": 0.0, "up": 90.0, "left": 180.0, "down": 270.0}


def parse_tag_location(path: Path) -> List[Tag]:
    text = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    room = None
    facing = None
    tags: List[Tag] = []
    room_re = re.compile(r"^\s*(Outer Room|Inner Room)\s*$", re.I)
    facing_re = re.compile(r"^\s*===\s*Tags Facing\s+(Down|Right|Up|Left)\s*$", re.I)
    tag_re = re.compile(r"Tag#\s*(\d+)\s*:\s*\(\s*([-+]?\d+(?:\.\d+)?)\s*,\s*([-+]?\d+(?:\.\d+)?)\s*\)")
    for line in text:
        m = room_re.search(line)
        if m:
            room = "outer" if m.group(1).lower().startswith("outer") else "inner"
            continue
        m = facing_re.search(line)
        if m:
            facing = m.group(1).lower()
            continue
        m = tag_re.search(line)
        if m and room and facing:
            tag_id = int(m.group(1))
            x = float(m.group(2))
            y = float(m.group(3))
            tags.append(Tag(tag_id=tag_id, x_m=x, y_m=y, yaw_deg=FACING_TO_YAW[facing], facing=facing, room=room))
    tags.sort(key=lambda t: t.tag_id)
    if not tags:
        raise ValueError(f"No tags parsed from {path}")
    return tags


def classify_room(x: float, y: float, cfg: PlannerCfg) -> str:
    if y <= cfg.inner_y_max:
        return "inner"
    if y >= cfg.outer_y_min:
        return "outer"
    return "blocked"


def eligible_tags_for_room(tags: List[Tag], room: str, cfg: PlannerCfg) -> List[Tag]:
    if room == "inner":
        return [t for t in tags if t.room == "inner" and cfg.include_inner_tags]
    if room == "outer":
        return [t for t in tags if t.room == "outer" and cfg.include_outer_tags]
    return []


def camera_pose(robot_x: float, robot_y: float, robot_h_deg: float, cam: CameraCfg) -> Tuple[float, float, float]:
    hr = math.radians(robot_h_deg)
    cx = robot_x + cam.forward_offset_m * math.cos(hr)
    cy = robot_y + cam.forward_offset_m * math.sin(hr)
    ch = deg_norm_360(robot_h_deg + cam.heading_offset_deg)
    return cx, cy, ch


def tag_is_good_for_camera(x: float, y: float, h: float, tag: Tag, cam: CameraCfg, cfg: PlannerCfg) -> Tuple[bool, Dict[str, float]]:
    cx, cy, ch = camera_pose(x, y, h, cam)
    dx = tag.x_m - cx
    dy = tag.y_m - cy
    d = math.hypot(dx, dy)
    if d < 1e-9:
        return False, {"distance_m": d, "camera_view_angle_deg": 999.0, "tag_incidence_deg": 999.0, "bearing_world_deg": 0.0}
    tag_bearing = deg_norm_360(math.degrees(math.atan2(dy, dx)))
    camera_view_angle = wrap_angle_deg(ch - tag_bearing)
    camera_from_tag_bearing = deg_norm_360(tag_bearing + 180.0)
    tag_incidence = angle_diff_abs(camera_from_tag_bearing, tag.yaw_deg)
    ok = (
        cfg.distance_min_m <= d <= cfg.distance_max_m
        and abs(camera_view_angle) <= cam.usable_half_fov_deg
        and tag_incidence <= cfg.tag_incidence_limit_deg
    )
    return ok, {
        "distance_m": float(d),
        "camera_view_angle_deg": float(camera_view_angle),
        "tag_incidence_deg": float(tag_incidence),
        "bearing_world_deg": float(tag_bearing),
    }


def eval_pose_heading(x: float, y: float, h: float, tags: List[Tag], cameras: List[CameraCfg], cfg: PlannerCfg) -> Dict:
    room = classify_room(x, y, cfg)
    eligible = eligible_tags_for_room(tags, room, cfg)
    front_ids: List[int] = []
    rear_ids: List[int] = []
    bearings: List[float] = []
    tag_debug: List[Dict] = []
    for cam in cameras:
        for tag in eligible:
            ok, m = tag_is_good_for_camera(x, y, h, tag, cam, cfg)
            if not ok:
                continue
            if cam.name == "front":
                front_ids.append(tag.tag_id)
            elif cam.name == "rear":
                rear_ids.append(tag.tag_id)
            bearings.append(bearing_world_deg(x, y, tag.x_m, tag.y_m))
            tag_debug.append({"tag_id": tag.tag_id, "camera": cam.name, **m})
    front_good = len(front_ids)
    rear_good = len(rear_ids)
    front_conf = next(c.confidence for c in cameras if c.name == "front")
    rear_conf = next(c.confidence for c in cameras if c.name == "rear")
    effective_good = front_conf * front_good + rear_conf * rear_good
    geometry_span = circular_span_deg(bearings)
    score = 10.0 * front_conf * front_good + 10.0 * rear_conf * rear_good + cfg.score_geometry_bonus_per_deg * geometry_span
    if room == "blocked":
        status = "BLOCKED"
    elif effective_good >= cfg.min_good_effective and geometry_span >= cfg.min_good_geometry_span_deg:
        status = "GOOD"
    elif effective_good >= cfg.min_marginal_effective:
        status = "MARGINAL"
    else:
        status = "BAD"
    return {
        "x_m": x,
        "y_m": y,
        "heading_deg": h,
        "room": room,
        "front_good_count": front_good,
        "rear_good_count": rear_good,
        "effective_good_count": effective_good,
        "good_total_count": front_good + rear_good,
        "geometry_span_deg": geometry_span,
        "score": score,
        "status": status,
        "front_good_tag_ids": front_ids,
        "rear_good_tag_ids": rear_ids,
        "all_good_tag_ids": front_ids + rear_ids,
        "tag_debug": tag_debug,
    }


def choose_best_heading(results: List[Dict]) -> Tuple[Dict, Optional[Dict]]:
    status_rank = {"GOOD": 3, "MARGINAL": 2, "BAD": 1, "BLOCKED": 0}
    ordered = sorted(results, key=lambda r: (r["score"], status_rank.get(r["status"], 0), r["good_total_count"], r["geometry_span_deg"]), reverse=True)
    best = ordered[0]
    backup = None
    for r in ordered[1:]:
        if angle_diff_abs(r["heading_deg"], best["heading_deg"]) >= 30.0:
            backup = r
            break
    if backup is None and len(ordered) > 1:
        backup = ordered[1]
    return best, backup


def heading_values(step_deg: float) -> np.ndarray:
    n = int(round(360.0 / step_deg))
    return np.array([i * step_deg for i in range(n)], dtype=np.float32)


def grid_values(vmin: float, vmax: float, step: float) -> np.ndarray:
    n = int(math.floor((vmax - vmin) / step + 1e-9)) + 1
    return np.round(np.array([vmin + i * step for i in range(n)], dtype=np.float32), 6)


def write_tag_table(tags: List[Tag], outdir: Path) -> None:
    with (outdir / "tag_table_parsed.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["tag_id", "x_m", "y_m", "yaw_deg", "facing", "room"])
        w.writeheader()
        for t in tags:
            w.writerow(asdict(t))


def write_config(cfg: PlannerCfg, outdir: Path) -> None:
    with (outdir / "planner_config.json").open("w", encoding="utf-8") as f:
        json.dump(asdict(cfg), f, indent=2)


def lookup_row_from_best(x: float, y: float, best: Dict, backup: Optional[Dict]) -> Dict:
    backup = backup or best
    return {
        "x_m": round(float(x), 3),
        "y_m": round(float(y), 3),
        "room": best["room"],
        "best_heading_deg": round(float(best["heading_deg"]), 3) if best["room"] != "blocked" else "",
        "backup_heading_deg": round(float(backup["heading_deg"]), 3) if backup["room"] != "blocked" else "",
        "best_status": best["status"],
        "backup_status": backup["status"],
        "best_score": round(float(best["score"]), 6) if best["room"] != "blocked" else "",
        "backup_score": round(float(backup["score"]), 6) if backup["room"] != "blocked" else "",
        "best_front_good_count": int(best["front_good_count"]),
        "best_rear_good_count": int(best["rear_good_count"]),
        "best_good_total_count": int(best["good_total_count"]),
        "best_effective_good_count": round(float(best["effective_good_count"]), 3),
        "best_geometry_span_deg": round(float(best["geometry_span_deg"]), 3),
        "best_front_good_tag_ids": " ".join(map(str, best["front_good_tag_ids"])),
        "best_rear_good_tag_ids": " ".join(map(str, best["rear_good_tag_ids"])),
        "best_all_good_tag_ids": " ".join(map(str, best["all_good_tag_ids"])),
        "backup_front_good_tag_ids": " ".join(map(str, backup["front_good_tag_ids"])),
        "backup_rear_good_tag_ids": " ".join(map(str, backup["rear_good_tag_ids"])),
        "backup_all_good_tag_ids": " ".join(map(str, backup["all_good_tag_ids"])),
    }


def write_lookup(rows: List[Dict], path: Path) -> None:
    fieldnames = [
        "x_m", "y_m", "room", "best_heading_deg", "backup_heading_deg", "best_status", "backup_status",
        "best_score", "backup_score", "best_front_good_count", "best_rear_good_count", "best_good_total_count",
        "best_effective_good_count", "best_geometry_span_deg", "best_front_good_tag_ids", "best_rear_good_tag_ids",
        "best_all_good_tag_ids", "backup_front_good_tag_ids", "backup_rear_good_tag_ids", "backup_all_good_tag_ids",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def make_heatmaps(outdir: Path, xs: np.ndarray, ys: np.ndarray, matrices: Dict[str, np.ndarray]) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception as e:
        print(f"[WARN] matplotlib unavailable; skip PNG heatmaps: {e}")
        return
    specs = [
        ("matrix_best_heading_deg", "Best heading deg"),
        ("matrix_best_score", "Best score"),
        ("matrix_best_good_total_count", "Good tag count at best heading"),
        ("matrix_best_geometry_span_deg", "Geometry span deg at best heading"),
        ("matrix_status_code", "Status code: 0 blocked, 1 bad, 2 marginal, 3 good"),
    ]
    extent = [float(xs[0]), float(xs[-1]), float(ys[0]), float(ys[-1])]
    for key, title in specs:
        arr = matrices.get(key)
        if arr is None:
            continue
        plt.figure(figsize=(8, 8))
        plt.imshow(arr, origin="lower", extent=extent, aspect="equal")
        plt.colorbar()
        plt.title(title)
        plt.xlabel("x_m")
        plt.ylabel("y_m")
        plt.tight_layout()
        plt.savefig(outdir / (key.replace("matrix_", "heatmap_") + ".png"), dpi=160)
        plt.close()


def parse_points(points_text: str) -> List[Tuple[float, float]]:
    pts = []
    for item in points_text.split(";"):
        item = item.strip()
        if not item:
            continue
        parts = [p.strip() for p in item.split(",")]
        if len(parts) != 2:
            raise ValueError(f"Bad point item {item!r}. Expected x,y")
        pts.append((float(parts[0]), float(parts[1])))
    return pts


def run_points_mode(tags: List[Tag], cfg: PlannerCfg, cameras: List[CameraCfg], points: List[Tuple[float, float]], outdir: Path) -> None:
    hs = heading_values(cfg.heading_step_deg)
    rows = []
    debug = {}
    for x, y in points:
        results = [eval_pose_heading(x, y, float(h), tags, cameras, cfg) for h in hs]
        best, backup = choose_best_heading(results)
        rows.append(lookup_row_from_best(x, y, best, backup))
        debug[f"{x:.3f},{y:.3f}"] = {"best": best, "backup": backup, "all_headings": results}
    write_lookup(rows, outdir / "preferred_heading_points.csv")
    with (outdir / "preferred_heading_points_debug.json").open("w", encoding="utf-8") as f:
        json.dump(debug, f, indent=2)
    print("\nPOINTS MODE RESULT")
    print("=" * 100)
    for r in rows:
        print(
            f"({r['x_m']:.2f},{r['y_m']:.2f}) room={r['room']:<7} "
            f"best_h={str(r['best_heading_deg']):>6} status={r['best_status']:<8} "
            f"score={str(r['best_score']):>8} front={r['best_front_good_count']} rear={r['best_rear_good_count']} "
            f"eff={r['best_effective_good_count']} span={r['best_geometry_span_deg']} tags=[{r['best_all_good_tag_ids']}]"
        )
    print(f"\nWrote: {outdir / 'preferred_heading_points.csv'}")
    print(f"Wrote: {outdir / 'preferred_heading_points_debug.json'}")


def run_full_mode(tags: List[Tag], cfg: PlannerCfg, cameras: List[CameraCfg], outdir: Path) -> None:
    xs = grid_values(cfg.x_min, cfg.x_max, cfg.grid_step_m)
    ys = grid_values(cfg.y_min, cfg.y_max, cfg.grid_step_m)
    hs = heading_values(cfg.heading_step_deg)
    nx, ny, nh = len(xs), len(ys), len(hs)
    print(f"Grid: nx={nx}, ny={ny}, headings={nh}, total cell/headings={nx*ny*nh:,}")

    good_front_count = np.zeros((ny, nx, nh), dtype=np.uint8)
    good_rear_count = np.zeros((ny, nx, nh), dtype=np.uint8)
    good_total_count = np.zeros((ny, nx, nh), dtype=np.uint8)
    effective_good_count = np.zeros((ny, nx, nh), dtype=np.float32)
    geometry_span_deg = np.zeros((ny, nx, nh), dtype=np.float32)
    score = np.zeros((ny, nx, nh), dtype=np.float32)

    best_heading_deg = np.full((ny, nx), np.nan, dtype=np.float32)
    backup_heading_deg = np.full((ny, nx), np.nan, dtype=np.float32)
    best_score = np.full((ny, nx), np.nan, dtype=np.float32)
    backup_score = np.full((ny, nx), np.nan, dtype=np.float32)
    best_good_total_count = np.zeros((ny, nx), dtype=np.uint8)
    best_front_good_count = np.zeros((ny, nx), dtype=np.uint8)
    best_rear_good_count = np.zeros((ny, nx), dtype=np.uint8)
    best_geometry_span_deg = np.zeros((ny, nx), dtype=np.float32)
    status_code = np.zeros((ny, nx), dtype=np.uint8)
    status_to_code = {"BLOCKED": 0, "BAD": 1, "MARGINAL": 2, "GOOD": 3}

    lookup_rows = []
    best_tag_debug = {}
    t0 = time.time()

    for iy, y in enumerate(ys):
        if iy % 10 == 0:
            print(f"row {iy+1}/{ny}, y={y:.2f}, elapsed={time.time()-t0:.1f}s", flush=True)
        for ix, x in enumerate(xs):
            room = classify_room(float(x), float(y), cfg)
            if room == "blocked":
                dummy = {
                    "x_m": round(float(x), 3), "y_m": round(float(y), 3), "room": "blocked",
                    "best_heading_deg": "", "backup_heading_deg": "", "best_status": "BLOCKED", "backup_status": "BLOCKED",
                    "best_score": "", "backup_score": "", "best_front_good_count": 0, "best_rear_good_count": 0,
                    "best_good_total_count": 0, "best_effective_good_count": 0, "best_geometry_span_deg": 0,
                    "best_front_good_tag_ids": "", "best_rear_good_tag_ids": "", "best_all_good_tag_ids": "",
                    "backup_front_good_tag_ids": "", "backup_rear_good_tag_ids": "", "backup_all_good_tag_ids": "",
                }
                lookup_rows.append(dummy)
                continue
            results = []
            for ih, h in enumerate(hs):
                r = eval_pose_heading(float(x), float(y), float(h), tags, cameras, cfg)
                results.append(r)
                good_front_count[iy, ix, ih] = r["front_good_count"]
                good_rear_count[iy, ix, ih] = r["rear_good_count"]
                good_total_count[iy, ix, ih] = r["good_total_count"]
                effective_good_count[iy, ix, ih] = r["effective_good_count"]
                geometry_span_deg[iy, ix, ih] = r["geometry_span_deg"]
                score[iy, ix, ih] = r["score"]
            best, backup = choose_best_heading(results)
            best_heading_deg[iy, ix] = best["heading_deg"]
            backup_heading_deg[iy, ix] = backup["heading_deg"] if backup else np.nan
            best_score[iy, ix] = best["score"]
            backup_score[iy, ix] = backup["score"] if backup else np.nan
            best_good_total_count[iy, ix] = best["good_total_count"]
            best_front_good_count[iy, ix] = best["front_good_count"]
            best_rear_good_count[iy, ix] = best["rear_good_count"]
            best_geometry_span_deg[iy, ix] = best["geometry_span_deg"]
            status_code[iy, ix] = status_to_code.get(best["status"], 0)
            lookup_rows.append(lookup_row_from_best(float(x), float(y), best, backup))
            best_tag_debug[f"{x:.3f},{y:.3f}"] = {
                "best_heading_deg": best["heading_deg"],
                "best_status": best["status"],
                "best_front_good_tag_ids": best["front_good_tag_ids"],
                "best_rear_good_tag_ids": best["rear_good_tag_ids"],
                "backup_heading_deg": backup["heading_deg"] if backup else None,
                "backup_status": backup["status"] if backup else None,
                "backup_front_good_tag_ids": backup["front_good_tag_ids"] if backup else [],
                "backup_rear_good_tag_ids": backup["rear_good_tag_ids"] if backup else [],
            }

    matrices = {
        "matrix_good_front_count": good_front_count,
        "matrix_good_rear_count": good_rear_count,
        "matrix_good_total_count": good_total_count,
        "matrix_effective_good_count": effective_good_count,
        "matrix_geometry_span_deg": geometry_span_deg,
        "matrix_score": score,
        "matrix_best_heading_deg": best_heading_deg,
        "matrix_backup_heading_deg": backup_heading_deg,
        "matrix_best_score": best_score,
        "matrix_backup_score": backup_score,
        "matrix_best_good_total_count": best_good_total_count,
        "matrix_best_front_good_count": best_front_good_count,
        "matrix_best_rear_good_count": best_rear_good_count,
        "matrix_best_geometry_span_deg": best_geometry_span_deg,
        "matrix_status_code": status_code,
    }
    save_names = list(matrices.keys()) if cfg.save_3d_matrices else [
        "matrix_best_heading_deg", "matrix_backup_heading_deg", "matrix_best_score", "matrix_backup_score",
        "matrix_best_good_total_count", "matrix_best_front_good_count", "matrix_best_rear_good_count",
        "matrix_best_geometry_span_deg", "matrix_status_code",
    ]
    for name in save_names:
        np.save(outdir / f"{name}.npy", matrices[name])
    np.save(outdir / "axis_x_m.npy", xs)
    np.save(outdir / "axis_y_m.npy", ys)
    np.save(outdir / "axis_heading_deg.npy", hs)
    write_lookup(lookup_rows, outdir / "preferred_heading_lookup.csv")
    with (outdir / "debug_best_tag_ids.json").open("w", encoding="utf-8") as f:
        json.dump(best_tag_debug, f, indent=2)
    if cfg.make_png:
        make_heatmaps(outdir, xs, ys, matrices)
    print(f"\nFull run complete in {time.time()-t0:.1f}s")
    print(f"Wrote output directory: {outdir}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="DemoRoom preferred-heading matrix generator")
    p.add_argument("--mode", choices=["points", "full"], default="points")
    p.add_argument("--tag-file", default="tag_location(3).txt")
    p.add_argument("--output-dir", default="preferred_heading_out")
    p.add_argument("--x-min", type=float, default=0.0)
    p.add_argument("--x-max", type=float, default=11.4)
    p.add_argument("--y-min", type=float, default=0.0)
    p.add_argument("--y-max", type=float, default=11.4)
    p.add_argument("--grid-step-m", type=float, default=0.1)
    p.add_argument("--heading-step-deg", type=float, default=5.0)
    p.add_argument("--inner-y-max", type=float, default=5.20)
    p.add_argument("--outer-y-min", type=float, default=5.40)
    p.add_argument("--distance-min-m", type=float, default=0.4)
    p.add_argument("--distance-max-m", type=float, default=4.0)
    p.add_argument("--tag-incidence-limit-deg", type=float, default=60.0)
    p.add_argument("--front-forward-offset-m", type=float, default=0.055)
    p.add_argument("--rear-forward-offset-m", type=float, default=-0.075)
    p.add_argument("--front-half-fov-deg", type=float, default=35.0)
    p.add_argument("--rear-half-fov-deg", type=float, default=15.0)
    p.add_argument("--front-confidence", type=float, default=1.0)
    p.add_argument("--rear-confidence", type=float, default=0.3)
    p.add_argument("--min-good-effective", type=float, default=3.0)
    p.add_argument("--min-marginal-effective", type=float, default=2.0)
    p.add_argument("--min-good-geometry-span-deg", type=float, default=45.0)
    p.add_argument("--score-geometry-bonus-per-deg", type=float, default=0.05)
    p.add_argument("--disable-outer-tags", action="store_true")
    p.add_argument("--disable-inner-tags", action="store_true")
    p.add_argument("--no-3d-matrices", action="store_true", help="Save only reduced 2D matrices, not full y/x/heading matrices.")
    p.add_argument("--no-png", action="store_true", help="Skip PNG heatmap generation.")
    p.add_argument("--points", default="2,2;2,4;6,2;6,4;8,2;8,4", help='Semicolon-separated x,y list, e.g. "2,2;2,4;6,2"')
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    tag_file = Path(args.tag_file)
    if not tag_file.exists():
        tag_file2 = Path(__file__).resolve().parent / args.tag_file
        if tag_file2.exists():
            tag_file = tag_file2
        else:
            raise FileNotFoundError(f"Tag file not found: {args.tag_file}")
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    cfg = PlannerCfg(
        tag_file=str(tag_file), output_dir=str(outdir),
        x_min=args.x_min, x_max=args.x_max, y_min=args.y_min, y_max=args.y_max,
        grid_step_m=args.grid_step_m, heading_step_deg=args.heading_step_deg,
        inner_y_max=args.inner_y_max, outer_y_min=args.outer_y_min,
        distance_min_m=args.distance_min_m, distance_max_m=args.distance_max_m,
        tag_incidence_limit_deg=args.tag_incidence_limit_deg,
        front_forward_offset_m=args.front_forward_offset_m, rear_forward_offset_m=args.rear_forward_offset_m,
        front_half_fov_deg=args.front_half_fov_deg, rear_half_fov_deg=args.rear_half_fov_deg,
        front_confidence=args.front_confidence, rear_confidence=args.rear_confidence,
        min_good_effective=args.min_good_effective, min_marginal_effective=args.min_marginal_effective,
        min_good_geometry_span_deg=args.min_good_geometry_span_deg,
        score_geometry_bonus_per_deg=args.score_geometry_bonus_per_deg,
        include_outer_tags=not args.disable_outer_tags,
        include_inner_tags=not args.disable_inner_tags,
        save_3d_matrices=not args.no_3d_matrices,
        make_png=not args.no_png,
    )
    tags = parse_tag_location(tag_file)
    cameras = [
        CameraCfg("front", 0.0, cfg.front_forward_offset_m, cfg.front_half_fov_deg, cfg.front_confidence),
        CameraCfg("rear", 180.0, cfg.rear_forward_offset_m, cfg.rear_half_fov_deg, cfg.rear_confidence),
    ]
    write_config(cfg, outdir)
    write_tag_table(tags, outdir)
    print("DemoRoom preferred-heading planner")
    print("=" * 80)
    print(f"tag_file: {tag_file}")
    print(f"tags parsed: {len(tags)}")
    print(f"output_dir: {outdir}")
    print(f"mode: {args.mode}")
    print(f"front offset={cfg.front_forward_offset_m}, rear offset={cfg.rear_forward_offset_m}")
    print(f"front half FOV={cfg.front_half_fov_deg}, rear half FOV={cfg.rear_half_fov_deg}")
    print(f"distance range={cfg.distance_min_m}..{cfg.distance_max_m} m")
    print(f"inner tags enabled={cfg.include_inner_tags}, outer tags enabled={cfg.include_outer_tags}")
    print("room gate: inner y<=%.2f, outer y>=%.2f, middle blocked" % (cfg.inner_y_max, cfg.outer_y_min))
    if args.mode == "points":
        run_points_mode(tags, cfg, cameras, parse_points(args.points), outdir)
    else:
        run_full_mode(tags, cfg, cameras, outdir)


if __name__ == "__main__":
    main()
