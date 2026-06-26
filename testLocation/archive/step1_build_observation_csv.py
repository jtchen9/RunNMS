#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

CAMERA_OFFSET_M = 0.055


def deg_norm_360(v: float) -> float:
    v = float(v) % 360.0
    if v < 0:
        v += 360.0
    return v


def wrap_angle_deg(v: float) -> float:
    v = (float(v) + 180.0) % 360.0 - 180.0
    # Keep +180 instead of -180 only for cleaner display; either is equivalent.
    if v <= -180.0:
        v += 360.0
    return v


def angle_error_deg(a: float, b: float) -> float:
    return abs(wrap_angle_deg(float(a) - float(b)))


def camera_pose_from_robot(robot_x: float, robot_y: float, robot_h: float, camera_role: str) -> Tuple[float, float, float]:
    h_rad = math.radians(robot_h)
    camera_role = str(camera_role).strip().lower()
    if camera_role == "front":
        cam_h = deg_norm_360(robot_h)
        off = CAMERA_OFFSET_M
    elif camera_role == "rear":
        cam_h = deg_norm_360(robot_h + 180.0)
        off = -CAMERA_OFFSET_M
    else:
        cam_h = deg_norm_360(robot_h)
        off = 0.0
    cam_x = robot_x + off * math.cos(h_rad)
    cam_y = robot_y + off * math.sin(h_rad)
    return cam_x, cam_y, cam_h


def tag_world_from_truth(gt_x: float, gt_y: float, gt_h: float, camera_role: str,
                         true_d: float, true_a: float, true_y: float) -> Dict[str, float]:
    """Reconstruct tag x/y/yaw from the truth columns printed by T7.

    This is only used for old measure11 rows that do not contain single-tag pose fields.
    It is algebraically the reverse of _expected_obs_from_truth().
    """
    cam_x, cam_y, cam_h = camera_pose_from_robot(gt_x, gt_y, gt_h, camera_role)

    # In T7 convention: true_angle = cam_heading - bearing_world.
    bearing_world = deg_norm_360(cam_h - true_a)
    br = math.radians(bearing_world)
    tag_x = cam_x + true_d * math.cos(br)
    tag_y = cam_y + true_d * math.sin(br)

    # true_yaw = tag_yaw + 180 - cam_heading + true_angle.
    tag_yaw = wrap_angle_deg(true_y + cam_h - true_a - 180.0)
    return {"x_m": tag_x, "y_m": tag_y, "yaw_deg": tag_yaw}


def estimate_robot_pose_from_one_tag(obs: Dict[str, Any], tag_world: Dict[str, Any],
                                     camera_offset_m: float = CAMERA_OFFSET_M) -> Dict[str, Any]:
    """Pure single-tag inversion copied from t7_probe_single_tag.py, without project imports."""
    try:
        cam = str(obs.get("camera_role") or "").strip().lower()
        if cam not in {"front", "rear"}:
            raise ValueError(f"bad camera_role={cam!r}")

        d_m = float(obs["distance_m"])
        angle_deg = float(obs["angle_deg_cw"])
        yaw_deg = float(obs["yaw_deg"])

        tag_x = float(tag_world["x_m"])
        tag_y = float(tag_world["y_m"])
        tag_yaw = float(tag_world["yaw_deg"])

        cam_h = deg_norm_360(tag_yaw + 180.0 + angle_deg - yaw_deg)
        bearing_world = deg_norm_360(cam_h - angle_deg)
        br = math.radians(bearing_world)

        cam_x = tag_x - d_m * math.cos(br)
        cam_y = tag_y - d_m * math.sin(br)

        if cam == "front":
            robot_h = deg_norm_360(cam_h)
            off = camera_offset_m
        else:
            robot_h = deg_norm_360(cam_h - 180.0)
            off = -camera_offset_m

        hr = math.radians(robot_h)
        robot_x = cam_x - off * math.cos(hr)
        robot_y = cam_y - off * math.sin(hr)

        return {
            "location_ok": True,
            "x_m": robot_x,
            "y_m": robot_y,
            "heading_deg": robot_h,
            "camera_x_m": cam_x,
            "camera_y_m": cam_y,
            "camera_heading_deg": cam_h,
            "source": "single_tag_from_obs",
        }
    except Exception as e:
        return {"location_ok": False, "detail": f"single-tag pose failed: {e}", "source": "single_tag_from_obs"}


def single_tag_pose_error(pose: Dict[str, Any], gt_x: float, gt_y: float, gt_h: float) -> Tuple[Optional[float], Optional[float]]:
    if not pose.get("location_ok"):
        return None, None
    px = float(pose["x_m"])
    py = float(pose["y_m"])
    ph = float(pose["heading_deg"])
    pos_cm = 100.0 * math.hypot(px - gt_x, py - gt_y)
    h_err = angle_error_deg(ph, gt_h)
    return pos_cm, h_err


ROW_RE = re.compile(
    r"^\s*(?P<tag>\d+)\s+(?P<cam>front|rear)\s+"
    r"(?P<true_d>-?\d+(?:\.\d+)?)\s+(?P<meas_d>-?\d+(?:\.\d+)?)\s+(?P<d_err>-?\d+(?:\.\d+)?)\s+"
    r"(?P<true_a>-?\d+(?:\.\d+)?)\s+(?P<meas_a>-?\d+(?:\.\d+)?)\s+(?P<a_err>-?\d+(?:\.\d+)?)\s+"
    r"(?P<true_y>-?\d+(?:\.\d+)?)\s+(?P<meas_y>-?\d+(?:\.\d+)?)\s+(?P<y_err>-?\d+(?:\.\d+)?)\s+"
    r"(?P<score>-?\d+(?:\.\d+)?)\s+(?P<flag>.*\S)?\s*$"
)

GT_RE = re.compile(r"GT\s*:\s*x=(?P<x>-?\d+(?:\.\d+)?)\s+y=(?P<y>-?\d+(?:\.\d+)?)\s+h=(?P<h>-?\d+(?:\.\d+)?)")

PER_TAG_HEADER = (
    "x,y,h,tag,cam,true_d,meas_d,d_err_cm,true_a,meas_a,a_err,true_y,meas_y,y_err,"
    "single_ok,single_x,single_y,single_h,single_pos_err_cm,single_heading_err_deg,score,flag"
)


def dataset_name(path: Path) -> str:
    name = path.name.lower()
    if "0624" in name:
        return "0624"
    if "0625" in name:
        return "0625"
    return path.stem


def build_row(dataset: str, source_file: str, gt_x: float, gt_y: float, gt_h: float,
              tag: int, cam: str, true_d: float, meas_d: float, d_err_cm: float,
              true_a: float, meas_a: float, a_err: float,
              true_y: float, meas_y: float, y_err: float,
              single_ok: Optional[bool] = None, single_x: Any = "", single_y: Any = "", single_h: Any = "",
              single_pos_err_cm: Any = "", single_heading_err_deg: Any = "",
              score: float = 0.0, flag: str = "") -> Dict[str, Any]:

    # Recompute missing single-tag information for old files.
    if single_ok is None or single_x == "" or single_y == "" or single_h == "":
        obs = {
            "id": int(tag),
            "camera_role": str(cam),
            "distance_m": float(meas_d),
            "angle_deg_cw": float(meas_a),
            "yaw_deg": float(meas_y),
        }
        tag_world = tag_world_from_truth(gt_x, gt_y, gt_h, cam, float(true_d), float(true_a), float(true_y))
        pose = estimate_robot_pose_from_one_tag(obs, tag_world)
        single_ok = bool(pose.get("location_ok"))
        if single_ok:
            single_x = float(pose["x_m"])
            single_y = float(pose["y_m"])
            single_h = float(pose["heading_deg"])
            single_pos_err_cm, single_heading_err_deg = single_tag_pose_error(pose, gt_x, gt_y, gt_h)
        else:
            single_x = single_y = single_h = single_pos_err_cm = single_heading_err_deg = ""

    return {
        "dataset": dataset,
        "source_file": source_file,
        "gt_x": f"{float(gt_x):.3f}",
        "gt_y": f"{float(gt_y):.3f}",
        "gt_h": f"{float(gt_h):.1f}",
        "tag_id": int(tag),
        "camera_role": str(cam),
        "true_distance_m": f"{float(true_d):.4f}",
        "meas_distance_m": f"{float(meas_d):.4f}",
        "distance_error_cm": f"{float(d_err_cm):.2f}",
        "true_angle_deg": f"{float(true_a):.3f}",
        "meas_angle_deg": f"{float(meas_a):.3f}",
        "angle_error_deg": f"{float(a_err):.3f}",
        "true_yaw_deg": f"{float(true_y):.3f}",
        "meas_yaw_deg": f"{float(meas_y):.3f}",
        "yaw_error_deg": f"{float(y_err):.3f}",
        "single_ok": str(bool(single_ok)),
        "single_x": "" if single_x in (None, "") else f"{float(single_x):.4f}",
        "single_y": "" if single_y in (None, "") else f"{float(single_y):.4f}",
        "single_h": "" if single_h in (None, "") else f"{float(single_h):.3f}",
        "single_pos_err_cm": "" if single_pos_err_cm in (None, "") else f"{float(single_pos_err_cm):.2f}",
        "single_heading_err_deg": "" if single_heading_err_deg in (None, "") else f"{float(single_heading_err_deg):.3f}",
        "score": f"{float(score):.3f}",
        "flag": str(flag or "OK").strip(),
    }


def parse_file(path: Path) -> List[Dict[str, Any]]:
    ds = dataset_name(path)
    text = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    rows: List[Dict[str, Any]] = []
    gt: Optional[Tuple[float, float, float]] = None

    i = 0
    while i < len(text):
        line = text[i].strip()
        mgt = GT_RE.search(line)
        if mgt:
            gt = (float(mgt.group("x")), float(mgt.group("y")), float(mgt.group("h")))
            i += 1
            continue

        # Newer format: parse PER_TAG_CSV block directly.
        if line == "PER_TAG_CSV":
            if i + 1 < len(text) and text[i + 1].strip() == PER_TAG_HEADER:
                j = i + 2
                while j < len(text):
                    raw = text[j].strip()
                    if not raw or raw.startswith("*") or raw.startswith("=") or raw.startswith("python ") or raw.startswith("T7-LITE"):
                        break
                    parts = next(csv.reader([raw]))
                    if len(parts) >= 22:
                        try:
                            rows.append(build_row(
                                ds, path.name,
                                float(parts[0]), float(parts[1]), float(parts[2]),
                                int(parts[3]), parts[4],
                                float(parts[5]), float(parts[6]), float(parts[7]),
                                float(parts[8]), float(parts[9]), float(parts[10]),
                                float(parts[11]), float(parts[12]), float(parts[13]),
                                single_ok=(str(parts[14]).strip().lower() == "true"),
                                single_x=parts[15], single_y=parts[16], single_h=parts[17],
                                single_pos_err_cm=parts[18], single_heading_err_deg=parts[19],
                                score=float(parts[20]), flag=parts[21],
                            ))
                        except Exception:
                            pass
                    j += 1
                i = j
                continue

        # Older format: parse printed observation table rows.
        # This will also find duplicates in newer files, but skip them when PER_TAG_CSV exists for the same row later.
        mr = ROW_RE.match(text[i])
        if mr and gt is not None:
            try:
                rows.append(build_row(
                    ds, path.name,
                    gt[0], gt[1], gt[2],
                    int(mr.group("tag")), mr.group("cam"),
                    float(mr.group("true_d")), float(mr.group("meas_d")), float(mr.group("d_err")),
                    float(mr.group("true_a")), float(mr.group("meas_a")), float(mr.group("a_err")),
                    float(mr.group("true_y")), float(mr.group("meas_y")), float(mr.group("y_err")),
                    score=float(mr.group("score")), flag=(mr.group("flag") or "OK"),
                ))
            except Exception:
                pass
        i += 1

    # Deduplicate: prefer PER_TAG_CSV rows, which have 4-decimal values; if duplicate from table exists, last wins.
    dedup: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for r in rows:
        key = (r["dataset"], r["gt_x"], r["gt_y"], r["gt_h"], r["tag_id"], r["camera_role"])
        dedup[key] = r
    return list(dedup.values())


FIELDNAMES = [
    "dataset", "source_file", "gt_x", "gt_y", "gt_h", "tag_id", "camera_role",
    "true_distance_m", "meas_distance_m", "distance_error_cm",
    "true_angle_deg", "meas_angle_deg", "angle_error_deg",
    "true_yaw_deg", "meas_yaw_deg", "yaw_error_deg",
    "single_ok", "single_x", "single_y", "single_h",
    "single_pos_err_cm", "single_heading_err_deg", "score", "flag",
]


def main() -> None:
    ap = argparse.ArgumentParser(description="Step 1: normalize T7 measurement logs into one per-tag observation CSV.")
    ap.add_argument("inputs", nargs="+", help="Measurement text files, e.g. measure11-0624.txt measure12-0625.txt")
    ap.add_argument("--out", default="step1_observations.csv", help="Output CSV path")
    args = ap.parse_args()

    all_rows: List[Dict[str, Any]] = []
    for p in args.inputs:
        all_rows.extend(parse_file(Path(p)))

    all_rows.sort(key=lambda r: (r["dataset"], float(r["gt_x"]), float(r["gt_y"]), float(r["gt_h"]), int(r["tag_id"]), r["camera_role"]))

    out = Path(args.out)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(all_rows)

    print(f"wrote {len(all_rows)} rows to {out}")
    by_ds: Dict[str, int] = {}
    by_pose: set[Tuple[str, str, str, str]] = set()
    for r in all_rows:
        by_ds[r["dataset"]] = by_ds.get(r["dataset"], 0) + 1
        by_pose.add((r["dataset"], r["gt_x"], r["gt_y"], r["gt_h"]))
    print("rows_by_dataset:", by_ds)
    print("unique_dataset_pose_count:", len(by_pose))


if __name__ == "__main__":
    main()
