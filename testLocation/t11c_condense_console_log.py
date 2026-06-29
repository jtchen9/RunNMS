"""
t11c_condense_console_log.py

Condense copied T11 console logs into concise CSV files.

Default input:
    D:\\Data\\_Action\\_RunNMS\\testLocation\\output\\t11_location_collection\\measure13-0629.txt

Practical use:
    1. Save copied console text as INPUT_TXT below.
    2. Run this script from VS Code or cmd.
    3. Review CSV files in OUTPUT_DIR.

This script does not talk to Redis, NMS, or the robot.
It only parses text logs.
"""

from __future__ import annotations

import ast
import csv
import re
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Adjustable settings
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[1]

INPUT_TXT = ROOT_DIR / "testLocation" / "output" / "t11_location_collection" / "measure13-0629.txt"

OUTPUT_DIR = ROOT_DIR / "testLocation" / "output" / "t11_console_condensed"

LOW_TAG_THRESHOLD = 3
HEADING_DIFF_THRESHOLD_DEG = 30.0


# ---------------------------------------------------------------------------
# Small parsing helpers
# ---------------------------------------------------------------------------

FLOAT = r"[-+]?\d+(?:\.\d+)?"

RE_POINT_HEADER = re.compile(r"^\s*x\s*=\s*(%s)\s+y\s*=\s*(%s)\s*$" % (FLOAT, FLOAT), re.IGNORECASE)
RE_PREF_HEADING = re.compile(r"Preferred heading at x=(%s), y=(%s):\s*(%s)\s*deg" % (FLOAT, FLOAT, FLOAT))
RE_GT = re.compile(r"GT:\s*\((%s),\s*(%s),\s*(%s)\)" % (FLOAT, FLOAT, FLOAT))
RE_EST = re.compile(r"Estimate:\s*\((%s),\s*(%s),\s*(%s)\)" % (FLOAT, FLOAT, FLOAT))
RE_POS_ERR = re.compile(r"Position Error\s*:\s*(%s)\s*m" % FLOAT)
RE_HEAD_ERR = re.compile(r"Heading Error\s*:\s*(%s)\s*deg" % FLOAT)
RE_TAGS_SEEN = re.compile(r"Tags Seen\s*:\s*(\d+)")
RE_FRONT_REAR = re.compile(r"Front\s*/\s*Rear\s*:\s*(\d+)\s*/\s*(\d+)")
RE_TAGS_USED = re.compile(r"Tags Used\s*:\s*(\[.*\])")
RE_SOLVER_STAGE = re.compile(r"Solver Stage\s*:\s*(.*)$")
RE_SOLVER_DETAIL = re.compile(r"Solver Detail\s*:\s*(.*)$")
RE_COMPACT_JSON = re.compile(r"Compact JSON\s*:\s*(.*)$")
RE_COMPACT_CSV = re.compile(r"Compact CSV\s*:\s*(.*)$")
RE_OBS_JSON = re.compile(r"Obs JSON\s*:\s*(.*)$")
RE_LATEST_OBS = re.compile(r"Latest Obs\s*:\s*(.*)$")
RE_UNMEASURABLE = re.compile(r"can\s+not\s+measure|cannot\s+measure", re.IGNORECASE)

# Example tag row:
#   43 front    Y   1.020   1.023  -0.003    33.3    34.8    -1.4    42.8    44.8    -2.0
RE_TAG_ROW = re.compile(
    r"^\s*(\d+)\s+(front|rear)\s+([YN])\s+"
    r"(%s)\s+(%s)\s+(%s)\s+"
    r"(%s)\s+(%s)\s+(%s)\s+"
    r"(%s)\s+(%s)\s+(%s)\s*$" % tuple([FLOAT] * 9),
    re.IGNORECASE,
)


def _to_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _to_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _angle_diff_deg(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    d = (float(a) - float(b) + 180.0) % 360.0 - 180.0
    return d


def _abs_or_none(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return abs(float(x))


def _parse_tags_used(text: str) -> str:
    text = text.strip()
    try:
        value = ast.literal_eval(text)
        if isinstance(value, list):
            return ";".join(str(int(v)) for v in value)
    except Exception:
        pass
    return text


def _new_sample() -> Dict[str, Any]:
    return {
        "sample_index": None,
        "declared_x_m": None,
        "declared_y_m": None,
        "preferred_heading_deg": None,
        "gt_x_m": None,
        "gt_y_m": None,
        "gt_heading_deg": None,
        "estimated_x_m": None,
        "estimated_y_m": None,
        "estimated_heading_deg": None,
        "position_error_m": None,
        "heading_error_deg": None,
        "tags_seen": None,
        "front_count": None,
        "rear_count": None,
        "tags_used": "",
        "used_tag_count": None,
        "solver_stage": "",
        "solver_detail": "",
        "compact_json": "",
        "compact_csv": "",
        "obs_json": "",
        "latest_obs": "",
    }


def _finalize_sample(sample: Dict[str, Any], tag_rows: List[Dict[str, Any]]) -> None:
    pref = sample.get("preferred_heading_deg")
    gt_h = sample.get("gt_heading_deg")
    sample["actual_minus_preferred_heading_deg"] = _angle_diff_deg(gt_h, pref)
    sample["abs_actual_minus_preferred_heading_deg"] = _abs_or_none(sample["actual_minus_preferred_heading_deg"])

    tags_seen = sample.get("tags_seen")
    if tags_seen is None:
        tags_seen = len([r for r in tag_rows if r.get("sample_index") == sample.get("sample_index")])
        sample["tags_seen"] = tags_seen

    sample_tags = [r for r in tag_rows if r.get("sample_index") == sample.get("sample_index")]
    used_tags = [r for r in sample_tags if r.get("used") == "Y"]
    rejected_tags = [r for r in sample_tags if r.get("used") == "N"]

    sample["table_tag_count"] = len(sample_tags)
    sample["table_used_tag_count"] = len(used_tags)
    sample["table_rejected_tag_count"] = len(rejected_tags)

    # Basic measurement-error summaries from compact tag table.
    for prefix, rows in (("all", sample_tags), ("used", used_tags), ("rejected", rejected_tags)):
        if rows:
            sample[f"{prefix}_mean_abs_distance_error_m"] = sum(abs(float(r["distance_error_m"])) for r in rows) / len(rows)
            sample[f"{prefix}_mean_abs_angle_error_deg"] = sum(abs(float(r["angle_error_deg"])) for r in rows) / len(rows)
            sample[f"{prefix}_mean_abs_yaw_error_deg"] = sum(abs(float(r["yaw_error_deg"])) for r in rows) / len(rows)
            sample[f"{prefix}_max_abs_distance_error_m"] = max(abs(float(r["distance_error_m"])) for r in rows)
            sample[f"{prefix}_max_abs_angle_error_deg"] = max(abs(float(r["angle_error_deg"])) for r in rows)
            sample[f"{prefix}_max_abs_yaw_error_deg"] = max(abs(float(r["yaw_error_deg"])) for r in rows)
        else:
            sample[f"{prefix}_mean_abs_distance_error_m"] = None
            sample[f"{prefix}_mean_abs_angle_error_deg"] = None
            sample[f"{prefix}_mean_abs_yaw_error_deg"] = None
            sample[f"{prefix}_max_abs_distance_error_m"] = None
            sample[f"{prefix}_max_abs_angle_error_deg"] = None
            sample[f"{prefix}_max_abs_yaw_error_deg"] = None

    sample["low_tag_flag"] = "Y" if (tags_seen is not None and int(tags_seen) <= LOW_TAG_THRESHOLD) else "N"
    sample["heading_override_flag"] = "Y" if (
        sample.get("abs_actual_minus_preferred_heading_deg") is not None
        and sample["abs_actual_minus_preferred_heading_deg"] >= HEADING_DIFF_THRESHOLD_DEG
    ) else "N"

    # A simple field priority for deciding what to re-measure.
    if sample["low_tag_flag"] == "Y":
        sample["remeasure_priority"] = "P1_LOW_TAG"
    elif sample["heading_override_flag"] == "Y":
        sample["remeasure_priority"] = "P2_HEADING_OVERRIDE"
    elif sample.get("position_error_m") is not None and float(sample["position_error_m"]) >= 0.25:
        sample["remeasure_priority"] = "P3_HIGH_ERROR"
    else:
        sample["remeasure_priority"] = "OK"


def parse_console_log(text: str) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    samples: List[Dict[str, Any]] = []
    tag_rows: List[Dict[str, Any]] = []
    unmeasurable: List[Dict[str, Any]] = []

    current_point_x = None
    current_point_y = None
    current_sample: Optional[Dict[str, Any]] = None
    current_sample_index = 0
    in_tag_table = False

    def start_sample() -> Dict[str, Any]:
        nonlocal current_sample_index
        current_sample_index += 1
        s = _new_sample()
        s["sample_index"] = current_sample_index
        s["declared_x_m"] = current_point_x
        s["declared_y_m"] = current_point_y
        return s

    for raw_line in text.splitlines():
        line = raw_line.rstrip("\n")
        stripped = line.strip()

        m = RE_POINT_HEADER.match(stripped)
        if m:
            current_point_x = _to_float(m.group(1))
            current_point_y = _to_float(m.group(2))
            in_tag_table = False
            continue

        if RE_UNMEASURABLE.search(stripped):
            unmeasurable.append({
                "declared_x_m": current_point_x,
                "declared_y_m": current_point_y,
                "note": stripped,
            })
            continue

        m = RE_PREF_HEADING.search(stripped)
        if m:
            if current_sample is None or current_sample.get("gt_x_m") is not None:
                current_sample = start_sample()
                samples.append(current_sample)
            current_sample["preferred_heading_deg"] = _to_float(m.group(3))
            continue

        m = RE_GT.search(stripped)
        if m:
            if current_sample is None or current_sample.get("gt_x_m") is not None:
                current_sample = start_sample()
                samples.append(current_sample)
            current_sample["gt_x_m"] = _to_float(m.group(1))
            current_sample["gt_y_m"] = _to_float(m.group(2))
            current_sample["gt_heading_deg"] = _to_float(m.group(3))
            if current_sample.get("declared_x_m") is None:
                current_sample["declared_x_m"] = current_sample["gt_x_m"]
            if current_sample.get("declared_y_m") is None:
                current_sample["declared_y_m"] = current_sample["gt_y_m"]
            continue

        if current_sample is None:
            continue

        m = RE_EST.search(stripped)
        if m:
            current_sample["estimated_x_m"] = _to_float(m.group(1))
            current_sample["estimated_y_m"] = _to_float(m.group(2))
            current_sample["estimated_heading_deg"] = _to_float(m.group(3))
            continue

        m = RE_POS_ERR.search(stripped)
        if m:
            current_sample["position_error_m"] = _to_float(m.group(1))
            continue

        m = RE_HEAD_ERR.search(stripped)
        if m:
            current_sample["heading_error_deg"] = _to_float(m.group(1))
            continue

        m = RE_TAGS_SEEN.search(stripped)
        if m:
            current_sample["tags_seen"] = _to_int(m.group(1))
            continue

        m = RE_FRONT_REAR.search(stripped)
        if m:
            current_sample["front_count"] = _to_int(m.group(1))
            current_sample["rear_count"] = _to_int(m.group(2))
            continue

        m = RE_TAGS_USED.search(stripped)
        if m:
            current_sample["tags_used"] = _parse_tags_used(m.group(1))
            if current_sample["tags_used"]:
                current_sample["used_tag_count"] = len(current_sample["tags_used"].split(";"))
            else:
                current_sample["used_tag_count"] = 0
            continue

        m = RE_SOLVER_STAGE.search(stripped)
        if m:
            current_sample["solver_stage"] = m.group(1).strip()
            continue

        m = RE_SOLVER_DETAIL.search(stripped)
        if m:
            current_sample["solver_detail"] = m.group(1).strip()
            continue

        m = RE_COMPACT_JSON.search(stripped)
        if m:
            current_sample["compact_json"] = m.group(1).strip()
            continue

        m = RE_COMPACT_CSV.search(stripped)
        if m:
            current_sample["compact_csv"] = m.group(1).strip()
            continue

        m = RE_OBS_JSON.search(stripped)
        if m:
            current_sample["obs_json"] = m.group(1).strip()
            continue

        m = RE_LATEST_OBS.search(stripped)
        if m:
            current_sample["latest_obs"] = m.group(1).strip()
            continue

        if stripped.startswith("Tag") and "TrueD" in stripped and "YErr" in stripped:
            in_tag_table = True
            continue

        if in_tag_table:
            m = RE_TAG_ROW.match(stripped)
            if m:
                tag_rows.append({
                    "sample_index": current_sample["sample_index"],
                    "gt_x_m": current_sample.get("gt_x_m"),
                    "gt_y_m": current_sample.get("gt_y_m"),
                    "gt_heading_deg": current_sample.get("gt_heading_deg"),
                    "preferred_heading_deg": current_sample.get("preferred_heading_deg"),
                    "tag_id": int(m.group(1)),
                    "camera_role": m.group(2).lower(),
                    "used": m.group(3).upper(),
                    "meas_distance_m": float(m.group(4)),
                    "true_distance_m": float(m.group(5)),
                    "distance_error_m": float(m.group(6)),
                    "meas_angle_deg": float(m.group(7)),
                    "true_angle_deg": float(m.group(8)),
                    "angle_error_deg": float(m.group(9)),
                    "meas_yaw_deg": float(m.group(10)),
                    "true_yaw_deg": float(m.group(11)),
                    "yaw_error_deg": float(m.group(12)),
                })
                continue
            if stripped.startswith("=") or not stripped:
                continue
            # Any non-table line ends the table.
            if "Compact Tag Measurement" not in stripped and not stripped.startswith("-"):
                in_tag_table = False

    for s in samples:
        _finalize_sample(s, tag_rows)

    return samples, tag_rows, unmeasurable


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: Optional[List[str]] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        keys = []
        for r in rows:
            for k in r.keys():
                if k not in keys:
                    keys.append(k)
        fieldnames = keys
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def build_low_tag_rows(samples: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [s for s in samples if s.get("low_tag_flag") == "Y"]


def build_remeasure_rows(samples: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    order = {"P1_LOW_TAG": 1, "P2_HEADING_OVERRIDE": 2, "P3_HIGH_ERROR": 3, "OK": 9}
    rows = [s for s in samples if s.get("remeasure_priority") != "OK"]
    return sorted(rows, key=lambda r: (order.get(r.get("remeasure_priority"), 9), r.get("gt_x_m") or 999, r.get("gt_y_m") or 999))


def build_tag_stats(tag_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for r in tag_rows:
        key = (r["tag_id"], r["camera_role"])
        groups.setdefault(key, []).append(r)

    out = []
    for (tag_id, camera_role), rows in sorted(groups.items()):
        used_rows = [r for r in rows if r["used"] == "Y"]
        rejected_rows = [r for r in rows if r["used"] == "N"]

        def mean_abs(field: str, selected: List[Dict[str, Any]]) -> Optional[float]:
            if not selected:
                return None
            return sum(abs(float(r[field])) for r in selected) / len(selected)

        out.append({
            "tag_id": tag_id,
            "camera_role": camera_role,
            "obs_count": len(rows),
            "used_count": len(used_rows),
            "rejected_count": len(rejected_rows),
            "reject_rate": len(rejected_rows) / len(rows) if rows else None,
            "mean_abs_distance_error_m": mean_abs("distance_error_m", rows),
            "mean_abs_angle_error_deg": mean_abs("angle_error_deg", rows),
            "mean_abs_yaw_error_deg": mean_abs("yaw_error_deg", rows),
            "used_mean_abs_distance_error_m": mean_abs("distance_error_m", used_rows),
            "used_mean_abs_angle_error_deg": mean_abs("angle_error_deg", used_rows),
            "used_mean_abs_yaw_error_deg": mean_abs("yaw_error_deg", used_rows),
        })
    return out


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not INPUT_TXT.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_TXT}\n"
            "Edit INPUT_TXT near the top of this script to point to your copied console log."
        )

    text = INPUT_TXT.read_text(encoding="utf-8", errors="replace")
    samples, tag_rows, unmeasurable = parse_console_log(text)

    sample_csv = OUTPUT_DIR / "sample_summary.csv"
    tag_csv = OUTPUT_DIR / "tag_measurements.csv"
    low_tag_csv = OUTPUT_DIR / "low_tag_points.csv"
    remeasure_csv = OUTPUT_DIR / "remeasure_candidates.csv"
    unmeasurable_csv = OUTPUT_DIR / "unmeasurable_points.csv"
    tag_stats_csv = OUTPUT_DIR / "tag_error_stats.csv"

    write_csv(sample_csv, samples)
    write_csv(tag_csv, tag_rows)
    write_csv(low_tag_csv, build_low_tag_rows(samples))
    write_csv(remeasure_csv, build_remeasure_rows(samples))
    write_csv(unmeasurable_csv, unmeasurable)
    write_csv(tag_stats_csv, build_tag_stats(tag_rows))

    print("T11 console log condensed.")
    print(f"Input              : {INPUT_TXT}")
    print(f"Samples parsed     : {len(samples)}")
    print(f"Tag rows parsed    : {len(tag_rows)}")
    print(f"Unmeasurable points: {len(unmeasurable)}")
    print("")
    print(f"Sample summary     : {sample_csv}")
    print(f"Tag measurements   : {tag_csv}")
    print(f"Low-tag points     : {low_tag_csv}")
    print(f"Remeasure candidates: {remeasure_csv}")
    print(f"Unmeasurable points: {unmeasurable_csv}")
    print(f"Tag error stats    : {tag_stats_csv}")


if __name__ == "__main__":
    main()
