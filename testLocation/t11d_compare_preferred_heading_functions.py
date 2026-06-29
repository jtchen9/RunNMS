
from __future__ import annotations

import csv
import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
os.chdir(ROOT_DIR)

import t2_build_preferred_heading_matrices as t2

# ---------------------------------------------------------------------------
# Controlled rule definitions
# ---------------------------------------------------------------------------

# A = original t2 rule. Do not modify.
# B = A physical gates + distance extension to 6 m + soft long-distance weight
#     + stronger geometry-span bonus.
# C = B + higher rear-camera confidence.

HEADING_STEP_DEG = 5.0

A_DISTANCE_MAX_M = 4.0
A_REAR_CONFIDENCE = 0.3
A_GEOMETRY_BONUS_PER_DEG = 0.05

B_DISTANCE_MAX_M = 6.0
B_REAR_CONFIDENCE = 0.3
B_GEOMETRY_BONUS_PER_DEG = 0.10

C_DISTANCE_MAX_M = 6.0
C_REAR_CONFIDENCE = 0.6
C_GEOMETRY_BONUS_PER_DEG = 0.10

# Camera-specific distance weights for B/C.
# Front camera: real data shows useful observations beyond 4 m, so keep soft extension.
# Rear camera: apply survivor-bias correction; predicted rear tags beyond 3 m are unreliable.
FRONT_DISTANCE_FULL_M = 4.0
FRONT_DISTANCE_SOFT1_M = 5.0
FRONT_DISTANCE_MAX_M = 6.0

REAR_DISTANCE_FULL_M = 2.5
REAR_DISTANCE_SOFT_M = 3.0
REAR_DISTANCE_MAX_M = 3.0

# Keep these as hard physical gates.
FRONT_HALF_FOV_DEG = 35.0
REAR_HALF_FOV_DEG = 15.0
TAG_INCIDENCE_LIMIT_DEG = 60.0

DISTANCE_MIN_M = 0.4
MIN_GOOD_EFFECTIVE = 3.0
MIN_MARGINAL_EFFECTIVE = 2.0
MIN_GOOD_GEOMETRY_SPAN_DEG = 45.0

LARGE_HEADING_DIFF_DEG = 45.0
LOW_TAG_THRESHOLD = 3

TAG_FILE = ROOT_DIR / "sitemap" / "DemoRoom" / "tag_location.txt"
TEST_DIR = ROOT_DIR / "testLocation"
CONDENSED_DIR = TEST_DIR / "output" / "t11_console_condensed"
SAMPLE_SUMMARY_CSV = CONDENSED_DIR / "sample_summary.csv"
LOW_TAG_POINTS_CSV = CONDENSED_DIR / "low_tag_points.csv"
OUT_DIR = TEST_DIR / "output" / "preferred_heading_rule_compare"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def build_cfg(distance_max_m: float, rear_confidence: float, geometry_bonus: float) -> t2.PlannerCfg:
    return t2.PlannerCfg(
        tag_file=str(TAG_FILE),
        output_dir=str(OUT_DIR),
        x_min=0.0,
        x_max=11.4,
        y_min=0.0,
        y_max=11.4,
        grid_step_m=0.1,
        heading_step_deg=HEADING_STEP_DEG,
        inner_y_max=5.20,
        outer_y_min=5.40,
        distance_min_m=DISTANCE_MIN_M,
        distance_max_m=distance_max_m,
        tag_incidence_limit_deg=TAG_INCIDENCE_LIMIT_DEG,
        front_forward_offset_m=0.055,
        rear_forward_offset_m=-0.075,
        front_half_fov_deg=FRONT_HALF_FOV_DEG,
        rear_half_fov_deg=REAR_HALF_FOV_DEG,
        front_confidence=1.0,
        rear_confidence=rear_confidence,
        min_good_effective=MIN_GOOD_EFFECTIVE,
        min_marginal_effective=MIN_MARGINAL_EFFECTIVE,
        min_good_geometry_span_deg=MIN_GOOD_GEOMETRY_SPAN_DEG,
        score_geometry_bonus_per_deg=geometry_bonus,
        include_outer_tags=True,
        include_inner_tags=True,
        save_3d_matrices=False,
        make_png=False,
    )


def build_cameras(cfg: t2.PlannerCfg) -> List[t2.CameraCfg]:
    return [
        t2.CameraCfg("front", 0.0, cfg.front_forward_offset_m, cfg.front_half_fov_deg, cfg.front_confidence),
        t2.CameraCfg("rear", 180.0, cfg.rear_forward_offset_m, cfg.rear_half_fov_deg, cfg.rear_confidence),
    ]


def camera_distance_soft_weight(camera_name: str, d: float) -> float:
    """
    Camera-specific distance weight for B/C.

    Front:
        0.4-4.0 m : 1.0
        4.0-5.0 m : 0.7
        5.0-6.0 m : 0.4
        >6.0 m    : 0.0

    Rear:
        0.4-2.5 m : 1.0
        2.5-3.0 m : 0.5
        >3.0 m    : 0.0

    Rear uses a shorter effective range to compensate survivor bias:
    a predicted rear tag beyond 3 m may be counted by geometry, but often
    does not survive in real detection.
    """
    if d < DISTANCE_MIN_M:
        return 0.0

    if camera_name == "rear":
        if d <= REAR_DISTANCE_FULL_M:
            return 1.0
        if d <= REAR_DISTANCE_SOFT_M:
            return 0.5
        return 0.0

    # front
    if d <= FRONT_DISTANCE_FULL_M:
        return 1.0
    if d <= FRONT_DISTANCE_SOFT1_M:
        return 0.7
    if d <= FRONT_DISTANCE_MAX_M:
        return 0.4
    return 0.0


def safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def first_key(row: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in row:
            return k
    return None


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        print(f"WARNING: missing input file: {path}")
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def get_xy(row: Dict[str, Any]) -> Tuple[float, float]:
    xk = first_key(row, ["x", "x_m", "gt_x_m", "ground_truth_x_m"])
    yk = first_key(row, ["y", "y_m", "gt_y_m", "ground_truth_y_m"])
    if xk is None or yk is None:
        raise ValueError(f"Cannot find x/y columns. Available columns: {list(row.keys())}")
    return float(row[xk]), float(row[yk])


def get_tags_seen(row: Dict[str, Any]) -> int:
    k = first_key(row, ["tags_seen", "tag_count", "tags", "num_tags"])
    return safe_int(row.get(k), 0) if k else 0


def get_pos_err(row: Dict[str, Any]) -> Optional[float]:
    k = first_key(row, ["position_error_m", "pos_err_m", "pos_error_m"])
    return safe_float(row.get(k)) if k else None


def get_heading_err(row: Dict[str, Any]) -> Optional[float]:
    k = first_key(row, ["heading_error_deg", "head_err_deg", "heading_err_deg"])
    return safe_float(row.get(k)) if k else None


def get_today_heading(row: Dict[str, Any]) -> Optional[float]:
    k = first_key(row, ["gt_heading_deg", "heading_deg", "h_deg", "ground_truth_heading_deg"])
    return safe_float(row.get(k)) if k else None


def load_low_tag_xy(path: Path) -> set[Tuple[float, float]]:
    out = set()
    for row in read_csv_rows(path):
        try:
            out.add(get_xy(row))
        except Exception:
            pass
    return out


def heading_diff_abs(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    return t2.angle_diff_abs(float(a), float(b))


def weighted_eval_pose_heading(
    x: float,
    y: float,
    h: float,
    tags: List[t2.Tag],
    cameras: List[t2.CameraCfg],
    cfg: t2.PlannerCfg,
) -> Dict[str, Any]:
    """
    Same physical gates as t2, except cfg.distance_max_m may be 6.0.
    Distance from 4.0 to 6.0 m contributes by soft weight.
    """
    room = t2.classify_room(x, y, cfg)
    eligible = t2.eligible_tags_for_room(tags, room, cfg)

    front_ids: List[int] = []
    rear_ids: List[int] = []
    front_weighted = 0.0
    rear_weighted = 0.0
    bearings: List[float] = []
    tag_debug: List[Dict[str, Any]] = []

    for cam in cameras:
        for tag in eligible:
            ok, m = t2.tag_is_good_for_camera(x, y, h, tag, cam, cfg)
            if not ok:
                continue

            dw = camera_distance_soft_weight(cam.name, float(m["distance_m"]))
            if dw <= 0.0:
                continue

            if cam.name == "front":
                front_ids.append(tag.tag_id)
                front_weighted += cam.confidence * dw
            elif cam.name == "rear":
                rear_ids.append(tag.tag_id)
                rear_weighted += cam.confidence * dw

            bearings.append(t2.bearing_world_deg(x, y, tag.x_m, tag.y_m))
            tag_debug.append({
                "tag_id": tag.tag_id,
                "camera": cam.name,
                "distance_weight": dw,
                **m,
            })

    front_count = len(front_ids)
    rear_count = len(rear_ids)
    total_count = front_count + rear_count
    effective_weighted = front_weighted + rear_weighted
    geometry_span = t2.circular_span_deg(bearings)

    score = 10.0 * effective_weighted + cfg.score_geometry_bonus_per_deg * geometry_span

    if room == "blocked":
        status = "BLOCKED"
    elif effective_weighted >= cfg.min_good_effective and geometry_span >= cfg.min_good_geometry_span_deg:
        status = "GOOD"
    elif effective_weighted >= cfg.min_marginal_effective:
        status = "MARGINAL"
    else:
        status = "BAD"

    return {
        "x_m": x,
        "y_m": y,
        "heading_deg": h,
        "room": room,
        "front_good_count": front_count,
        "rear_good_count": rear_count,
        "good_total_count": total_count,
        "front_weighted_count": front_weighted,
        "rear_weighted_count": rear_weighted,
        "effective_good_count": effective_weighted,
        "geometry_span_deg": geometry_span,
        "score": score,
        "status": status,
        "front_good_tag_ids": front_ids,
        "rear_good_tag_ids": rear_ids,
        "all_good_tag_ids": front_ids + rear_ids,
        "tag_debug": tag_debug,
    }


def choose_best_weighted(results: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    status_rank = {"GOOD": 3, "MARGINAL": 2, "BAD": 1, "BLOCKED": 0}
    ordered = sorted(
        results,
        key=lambda r: (
            r["score"],
            status_rank.get(r["status"], 0),
            r["effective_good_count"],
            r["good_total_count"],
            r["geometry_span_deg"],
        ),
        reverse=True,
    )
    best = ordered[0]
    backup = None
    for r in ordered[1:]:
        if t2.angle_diff_abs(r["heading_deg"], best["heading_deg"]) >= 30.0:
            backup = r
            break
    if backup is None and len(ordered) > 1:
        backup = ordered[1]
    return best, backup


def eval_function_A(x: float, y: float, tags: List[t2.Tag]) -> Dict[str, Any]:
    cfg = build_cfg(A_DISTANCE_MAX_M, A_REAR_CONFIDENCE, A_GEOMETRY_BONUS_PER_DEG)
    cameras = build_cameras(cfg)
    results = [t2.eval_pose_heading(x, y, float(h), tags, cameras, cfg) for h in t2.heading_values(cfg.heading_step_deg)]
    best, backup = t2.choose_best_heading(results)
    return best


def eval_function_B(x: float, y: float, tags: List[t2.Tag]) -> Dict[str, Any]:
    cfg = build_cfg(B_DISTANCE_MAX_M, B_REAR_CONFIDENCE, B_GEOMETRY_BONUS_PER_DEG)
    cameras = build_cameras(cfg)
    results = [weighted_eval_pose_heading(x, y, float(h), tags, cameras, cfg) for h in t2.heading_values(cfg.heading_step_deg)]
    best, backup = choose_best_weighted(results)
    return best


def eval_function_C(x: float, y: float, tags: List[t2.Tag]) -> Dict[str, Any]:
    cfg = build_cfg(C_DISTANCE_MAX_M, C_REAR_CONFIDENCE, C_GEOMETRY_BONUS_PER_DEG)
    cameras = build_cameras(cfg)
    results = [weighted_eval_pose_heading(x, y, float(h), tags, cameras, cfg) for h in t2.heading_values(cfg.heading_step_deg)]
    best, backup = choose_best_weighted(results)
    return best


def fmt_ids(ids: List[int]) -> str:
    return " ".join(str(i) for i in ids)


def priority_label(row: Dict[str, Any]) -> str:
    low_tag = bool(row["is_low_tag_point"])
    large_ab = bool(row["large_diff_A_B"])
    large_ac = bool(row["large_diff_A_C"])
    high_err = (safe_float(row.get("today_position_error_m"), 0.0) or 0.0) >= 0.25

    if low_tag and (large_ab or large_ac):
        return "P1_low_tag_and_heading_diff"
    if low_tag:
        return "P2_low_tag"
    if high_err and (large_ab or large_ac):
        return "P3_high_error_and_heading_diff"
    if large_ab or large_ac:
        return "P4_heading_diff"
    return "P9_low_priority"


def main() -> None:
    tags = t2.parse_tag_location(TAG_FILE)
    sample_rows = read_csv_rows(SAMPLE_SUMMARY_CSV)
    low_tag_xy = load_low_tag_xy(LOW_TAG_POINTS_CSV)

    print("=" * 72)
    print("T11D: controlled preferred-heading comparison")
    print("=" * 72)
    print(f"TAG_FILE       = {TAG_FILE}")
    print(f"SAMPLE_SUMMARY = {SAMPLE_SUMMARY_CSV}")
    print(f"LOW_TAG_POINTS = {LOW_TAG_POINTS_CSV}")
    print(f"OUT_DIR        = {OUT_DIR}")
    print(f"tags loaded    = {len(tags)}")
    print(f"sample rows    = {len(sample_rows)}")
    print("")
    print("Rules:")
    print(f"A: distance<=4.0, rear_conf={A_REAR_CONFIDENCE}, geom_bonus={A_GEOMETRY_BONUS_PER_DEG}")
    print(f"B: front<=6 soft, rear<=3 survivor-corrected, rear_conf={B_REAR_CONFIDENCE}, geom_bonus={B_GEOMETRY_BONUS_PER_DEG}")
    print(f"C: front<=6 soft, rear<=3 survivor-corrected, rear_conf={C_REAR_CONFIDENCE}, geom_bonus={C_GEOMETRY_BONUS_PER_DEG}")
    print("Hard physical gates for all: front FOV=35, rear FOV=15, tag incidence<=60, same-room tags only.")
    print("")

    out_rows = []

    for srow in sample_rows:
        try:
            x, y = get_xy(srow)
        except Exception as e:
            print(f"WARNING: skip row: {e}")
            continue

        A = eval_function_A(x, y, tags)
        B = eval_function_B(x, y, tags)
        C = eval_function_C(x, y, tags)

        ab = heading_diff_abs(A["heading_deg"], B["heading_deg"])
        ac = heading_diff_abs(A["heading_deg"], C["heading_deg"])
        bc = heading_diff_abs(B["heading_deg"], C["heading_deg"])

        tags_seen = get_tags_seen(srow)
        row = {
            "x": x,
            "y": y,
            "today_heading_deg": get_today_heading(srow),
            "today_tags_seen": tags_seen,
            "today_position_error_m": get_pos_err(srow),
            "today_heading_error_deg": get_heading_err(srow),
            "is_low_tag_point": (x, y) in low_tag_xy or tags_seen <= LOW_TAG_THRESHOLD,

            "A_current_heading_deg": A["heading_deg"],
            "A_score": round(A["score"], 4),
            "A_status": A["status"],
            "A_front_count": A["front_good_count"],
            "A_rear_count": A["rear_good_count"],
            "A_effective_count": round(A["effective_good_count"], 3),
            "A_geometry_span_deg": round(A["geometry_span_deg"], 1),
            "A_tag_ids": fmt_ids(A["all_good_tag_ids"]),

            "B_front6_rear3_heading_deg": B["heading_deg"],
            "B_score": round(B["score"], 4),
            "B_status": B["status"],
            "B_front_count": B["front_good_count"],
            "B_rear_count": B["rear_good_count"],
            "B_effective_weighted_count": round(B["effective_good_count"], 3),
            "B_geometry_span_deg": round(B["geometry_span_deg"], 1),
            "B_tag_ids": fmt_ids(B["all_good_tag_ids"]),

            "C_front6_rear3_rear06_heading_deg": C["heading_deg"],
            "C_score": round(C["score"], 4),
            "C_status": C["status"],
            "C_front_count": C["front_good_count"],
            "C_rear_count": C["rear_good_count"],
            "C_effective_weighted_count": round(C["effective_good_count"], 3),
            "C_geometry_span_deg": round(C["geometry_span_deg"], 1),
            "C_tag_ids": fmt_ids(C["all_good_tag_ids"]),

            "diff_A_B_deg": None if ab is None else round(ab, 1),
            "diff_A_C_deg": None if ac is None else round(ac, 1),
            "diff_B_C_deg": None if bc is None else round(bc, 1),
            "large_diff_A_B": ab is not None and ab >= LARGE_HEADING_DIFF_DEG,
            "large_diff_A_C": ac is not None and ac >= LARGE_HEADING_DIFF_DEG,
            "large_diff_B_C": bc is not None and bc >= LARGE_HEADING_DIFF_DEG,
        }
        row["priority"] = priority_label(row)
        out_rows.append(row)

    priority_order = {
        "P1_low_tag_and_heading_diff": 1,
        "P2_low_tag": 2,
        "P3_high_error_and_heading_diff": 3,
        "P4_heading_diff": 4,
        "P9_low_priority": 9,
    }
    out_rows.sort(key=lambda r: (
        priority_order.get(r["priority"], 99),
        safe_int(r.get("today_tags_seen"), 999),
        r["x"],
        r["y"],
    ))

    compare_csv = OUT_DIR / "heading_rule_compare_29points.csv"
    priority_csv = OUT_DIR / "remeasure_priority.csv"

    if out_rows:
        fieldnames = list(out_rows[0].keys())
        with compare_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(out_rows)

        priority_rows = [r for r in out_rows if r["priority"] != "P9_low_priority"]
        with priority_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(priority_rows)

    print("Output written:")
    print(f"  {compare_csv}")
    print(f"  {priority_csv}")
    print("")
    print("Top remeasurement candidates:")
    for r in out_rows:
        if r["priority"] == "P9_low_priority":
            continue
        print(
            f"({r['x']:.0f},{r['y']:.0f}) "
            f"today_tags={r['today_tags_seen']} "
            f"A={r['A_current_heading_deg']} "
            f"B={r['B_front6_rear3_heading_deg']} "
            f"C={r['C_front6_rear3_rear06_heading_deg']} "
            f"diffAB={r['diff_A_B_deg']} "
            f"diffAC={r['diff_A_C_deg']} "
            f"priority={r['priority']}"
        )


if __name__ == "__main__":
    main()
