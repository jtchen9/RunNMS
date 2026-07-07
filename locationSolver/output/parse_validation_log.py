from __future__ import annotations

"""
parse_validation_log.py

Parse the long text log produced by:
    t11_collect_validation_final_componentwise.py

Output:
    one LONG-FORM CSV row per observed tag

Sample-level fields are repeated on every tag row so the CSV is easy to:
    - filter by location
    - rank by final position error
    - inspect raw D/A/Y errors
    - inspect Layer-1 / M2 / yaw decisions
    - rebuild per-location samples later

No solver is rerun. This parser only extracts what is already printed
in the validation log.

Usage:
    1. Put this file anywhere.
    2. Edit INPUT_TXT / OUTPUT_CSV at the bottom.
    3. Run in VS Code.

Tested against the 2026-07-07 validation log format.
"""

import csv
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

FLOAT = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?"

RE_GT = re.compile(
    rf"^GT:\s*\(\s*({FLOAT})\s*,\s*({FLOAT})\s*,\s*({FLOAT})\s*\)\s*$"
)

RE_INPUT_GT_X = re.compile(rf"^Input ground-truth x_m:\s*({FLOAT})\s*$")
RE_INPUT_GT_Y = re.compile(rf"^Input ground-truth y_m:\s*({FLOAT})\s*$")

RE_PREF_HEADING = re.compile(
    rf"^Preferred heading at x=({FLOAT}), y=({FLOAT}):\s*({FLOAT})\s*deg\s*$"
)

RE_TAGS_OBSERVED = re.compile(r"^Tags Observed\s*:\s*(\d+)\s*$")
RE_FRONT_REAR = re.compile(r"^Front / Rear\s*:\s*(\d+)\s*/\s*(\d+)\s*$")
RE_TAG_IDS = re.compile(r"^Tag IDs\s*:\s*\[(.*?)\]\s*$")

RE_PASS1 = re.compile(
    rf"^Pass-1 pose\s*:\s*x=({FLOAT}),\s*y=({FLOAT}),\s*h=({FLOAT})"
)
RE_FINAL = re.compile(
    rf"^Final pose\s*:\s*x=({FLOAT}),\s*y=({FLOAT}),\s*h=({FLOAT})"
)
RE_HUMAN = re.compile(
    rf"^Human reference\s*:\s*position diff=({FLOAT})\s*cm,\s*"
    rf"heading diff=({FLOAT})"
)

RE_OBS_USABLE = re.compile(
    r"^Observed / Layer-1 usable\s*:\s*(\d+)\s*/\s*(\d+)\s*$"
)
RE_UNIQUE_USABLE = re.compile(r"^Unique usable tags\s*:\s*(\d+)\s*$")
RE_USABLE_FR = re.compile(
    r"^Usable front/rear\s*:\s*(\d+)\s*/\s*(\d+)\s*$"
)
RE_LAYER1_GATED = re.compile(r"^Layer-1 gated out\s*:\s*(.*?)\s*$")

RE_DISTANCE_CUT = re.compile(
    r"^Distance components cut\s*:\s*(\d+)(?:\s*\[(.*?)\])?\s*$"
)
RE_ANGLE_CUT = re.compile(r"^Angle components cut\s*:\s*(\d+)\s*$")
RE_YAW_ADMITTED = re.compile(
    r"^Yaw admitted\s*:\s*(\d+)\s*\(keep=(\d+),\s*flip=(\d+)\)\s*$"
)
RE_YAW_FLIPS = re.compile(r"^Yaw flips\s*:\s*(.*?)\s*$")
RE_COLLECTION = re.compile(r"^COLLECTION\s*:\s*(.*?)\s*$")
RE_NOTES = re.compile(r"^Notes\s*:\s*(.*?)\s*$")

RE_OBS_TABLE_ROW = re.compile(
    rf"^\s*(\d+)\s+"
    rf"(front|rear)\s+"
    rf"({FLOAT})\s+"   # Dist
    rf"({FLOAT})\s+"   # TrueD
    rf"({FLOAT})\s+"   # DErr
    rf"({FLOAT})\s+"   # Ang
    rf"({FLOAT})\s+"   # TrueA
    rf"({FLOAT})\s+"   # AErr
    rf"({FLOAT})\s+"   # Yaw
    rf"({FLOAT})\s+"   # TrueY
    rf"({FLOAT})\s*$"  # YErr
)

# Yaw diagnostic row:
# Tag Cam TrueA Yaw TrueY KeepE FlipE Best BestE Gain Sep QYaw Decision
# QYaw may be "-" and Decision may contain underscores.
RE_YAW_DIAG_ROW = re.compile(
    rf"^\s*(\d+)\s+"
    rf"(front|rear)\s+"
    rf"({FLOAT})\s+"   # TrueA
    rf"({FLOAT})\s+"   # Yaw
    rf"({FLOAT})\s+"   # TrueY
    rf"({FLOAT})\s+"   # KeepE
    rf"({FLOAT})\s+"   # FlipE
    rf"(keep|flip)\s+"
    rf"({FLOAT})\s+"   # BestE
    rf"({FLOAT})\s+"   # Gain
    rf"({FLOAT})\s+"   # Sep
    rf"(\S+)\s+"       # QYaw or -
    rf"(\S+)\s*$"      # Decision
)


CSV_FIELDS = [
    # sample identity / GT
    "sample_index",
    "gt_x_m",
    "gt_y_m",
    "gt_heading_deg",
    "preferred_heading_deg",

    # raw sample counts
    "tags_observed",
    "front_observed",
    "rear_observed",
    "tag_ids_observed",

    # current-pipeline sample summary
    "layer1_usable_count",
    "unique_usable_tags",
    "usable_front_count",
    "usable_rear_count",
    "layer1_gated_out",

    "pass1_x_m",
    "pass1_y_m",
    "pass1_heading_deg",

    "final_x_m",
    "final_y_m",
    "final_heading_deg",

    "final_position_error_cm",
    "final_heading_error_deg",

    "distance_components_cut_count",
    "distance_components_cut_tags",
    "angle_components_cut_count",

    "yaw_admitted_count",
    "yaw_keep_count",
    "yaw_flip_count",
    "yaw_flip_tags",

    "collection_conclusion",
    "collection_notes",

    # tag identity / raw measurement diagnostic
    "tag_id",
    "camera_role",

    "meas_distance_m",
    "true_distance_m",
    "distance_error_m",

    "meas_angle_deg",
    "true_angle_deg",
    "angle_error_deg",

    "meas_yaw_deg",
    "true_yaw_deg",
    "yaw_error_deg",

    # yaw diagnostic
    "yaw_keep_error_deg",
    "yaw_flip_error_deg",
    "yaw_best_mode",
    "yaw_best_error_deg",
    "yaw_gain_deg",
    "yaw_separation_deg",
    "yaw_quantized_deg",
    "yaw_diagnostic_decision",

    # useful derived pipeline labels
    "layer1_tag_usable_inferred",
    "distance_component_cut",
    "yaw_admitted_inferred",
    "yaw_flip_inferred",
]


def _clean_line(line: str) -> str:
    """
    Normalize harmless encoding damage in headings only.

    The log contains mojibake such as:
        '�X' / '¢X'
    where degree symbols were damaged.

    Numeric parsing does not depend on those symbols, so we simply strip
    a few common artifacts and normalize whitespace.
    """
    line = line.replace("\ufeff", "")
    line = line.replace("\x00", "")
    return line.rstrip("\r\n")


def _to_float(text: Optional[str]) -> Optional[float]:
    if text is None or text == "":
        return None
    try:
        x = float(text)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _parse_tag_list(text: Optional[str]) -> List[int]:
    if not text:
        return []
    return [int(x) for x in re.findall(r"\d+", text)]


def _fmt_tag_list(tags: List[int]) -> str:
    return "|".join(str(x) for x in tags)


def _new_sample(index: int) -> Dict[str, Any]:
    return {
        "sample_index": index,
        "gt_x_m": None,
        "gt_y_m": None,
        "gt_heading_deg": None,
        "preferred_heading_deg": None,

        "tags_observed": None,
        "front_observed": None,
        "rear_observed": None,
        "tag_ids_observed": [],

        "layer1_usable_count": None,
        "unique_usable_tags": None,
        "usable_front_count": None,
        "usable_rear_count": None,
        "layer1_gated_out": "",

        "pass1_x_m": None,
        "pass1_y_m": None,
        "pass1_heading_deg": None,

        "final_x_m": None,
        "final_y_m": None,
        "final_heading_deg": None,

        "final_position_error_cm": None,
        "final_heading_error_deg": None,

        "distance_components_cut_count": None,
        "distance_components_cut_tags": [],
        "angle_components_cut_count": None,

        "yaw_admitted_count": None,
        "yaw_keep_count": None,
        "yaw_flip_count": None,
        "yaw_flip_tags": [],

        "collection_conclusion": "",
        "collection_notes": "",

        "observations": [],
        "yaw_diag_by_tag_camera": {},
    }


def parse_validation_log(text: str) -> List[Dict[str, Any]]:
    lines = [_clean_line(x) for x in text.splitlines()]

    samples: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None

    in_obs_table = False
    in_yaw_diag = False

    # A new sample is anchored on the explicit "GT: (...)" line.
    # This is more reliable than decorative "(x=..., y=...)" separators.
    for line in lines:
        stripped = line.strip()

        # ---------------------------------------------------------------
        # New sample anchor
        # ---------------------------------------------------------------
        m = RE_GT.match(stripped)
        if m:
            if current is not None:
                samples.append(current)

            current = _new_sample(len(samples) + 1)
            current["gt_x_m"] = float(m.group(1))
            current["gt_y_m"] = float(m.group(2))
            current["gt_heading_deg"] = float(m.group(3))

            in_obs_table = False
            in_yaw_diag = False
            continue

        if current is None:
            continue

        # ---------------------------------------------------------------
        # Table mode transitions
        # ---------------------------------------------------------------
        if stripped == "Observation-Only Tag Measurement Check":
            in_obs_table = True
            in_yaw_diag = False
            continue

        if stripped == "Yaw Keep/Flip Quantization Diagnostic":
            in_obs_table = False
            in_yaw_diag = True
            continue

        if "VALIDATION FIELD CHECK" in stripped:
            in_obs_table = False
            in_yaw_diag = False
            continue

        # ---------------------------------------------------------------
        # Observation table
        # ---------------------------------------------------------------
        if in_obs_table:
            m = RE_OBS_TABLE_ROW.match(line)
            if m:
                current["observations"].append({
                    "tag_id": int(m.group(1)),
                    "camera_role": m.group(2),

                    "meas_distance_m": float(m.group(3)),
                    "true_distance_m": float(m.group(4)),
                    "distance_error_m": float(m.group(5)),

                    "meas_angle_deg": float(m.group(6)),
                    "true_angle_deg": float(m.group(7)),
                    "angle_error_deg": float(m.group(8)),

                    "meas_yaw_deg": float(m.group(9)),
                    "true_yaw_deg": float(m.group(10)),
                    "yaw_error_deg": float(m.group(11)),
                })
                continue

            # Leave mode once the next section starts.
            if stripped.startswith("Yaw Keep/Flip"):
                in_obs_table = False

        # ---------------------------------------------------------------
        # Yaw diagnostic table
        # ---------------------------------------------------------------
        if in_yaw_diag:
            m = RE_YAW_DIAG_ROW.match(line)
            if m:
                tag_id = int(m.group(1))
                camera_role = m.group(2)

                qyaw_text = m.group(12)
                qyaw = None if qyaw_text == "-" else _to_float(qyaw_text)

                current["yaw_diag_by_tag_camera"][(tag_id, camera_role)] = {
                    "yaw_keep_error_deg": float(m.group(6)),
                    "yaw_flip_error_deg": float(m.group(7)),
                    "yaw_best_mode": m.group(8),
                    "yaw_best_error_deg": float(m.group(9)),
                    "yaw_gain_deg": float(m.group(10)),
                    "yaw_separation_deg": float(m.group(11)),
                    "yaw_quantized_deg": qyaw,
                    "yaw_diagnostic_decision": m.group(13),
                }
                continue

            if "VALIDATION FIELD CHECK" in stripped:
                in_yaw_diag = False

        # ---------------------------------------------------------------
        # Sample-level fields
        # ---------------------------------------------------------------
        m = RE_PREF_HEADING.match(stripped)
        if m:
            current["preferred_heading_deg"] = float(m.group(3))
            continue

        m = RE_TAGS_OBSERVED.match(stripped)
        if m:
            current["tags_observed"] = int(m.group(1))
            continue

        m = RE_FRONT_REAR.match(stripped)
        if m:
            current["front_observed"] = int(m.group(1))
            current["rear_observed"] = int(m.group(2))
            continue

        m = RE_TAG_IDS.match(stripped)
        if m:
            current["tag_ids_observed"] = _parse_tag_list(m.group(1))
            continue

        m = RE_OBS_USABLE.match(stripped)
        if m:
            current["layer1_usable_count"] = int(m.group(2))
            continue

        m = RE_UNIQUE_USABLE.match(stripped)
        if m:
            current["unique_usable_tags"] = int(m.group(1))
            continue

        m = RE_USABLE_FR.match(stripped)
        if m:
            current["usable_front_count"] = int(m.group(1))
            current["usable_rear_count"] = int(m.group(2))
            continue

        m = RE_LAYER1_GATED.match(stripped)
        if m:
            current["layer1_gated_out"] = m.group(1)
            continue

        m = RE_PASS1.match(stripped)
        if m:
            current["pass1_x_m"] = float(m.group(1))
            current["pass1_y_m"] = float(m.group(2))
            current["pass1_heading_deg"] = float(m.group(3))
            continue

        m = RE_FINAL.match(stripped)
        if m:
            current["final_x_m"] = float(m.group(1))
            current["final_y_m"] = float(m.group(2))
            current["final_heading_deg"] = float(m.group(3))
            continue

        m = RE_HUMAN.match(stripped)
        if m:
            current["final_position_error_cm"] = float(m.group(1))
            current["final_heading_error_deg"] = float(m.group(2))
            continue

        m = RE_DISTANCE_CUT.match(stripped)
        if m:
            current["distance_components_cut_count"] = int(m.group(1))
            current["distance_components_cut_tags"] = _parse_tag_list(
                m.group(2)
            )
            continue

        m = RE_ANGLE_CUT.match(stripped)
        if m:
            current["angle_components_cut_count"] = int(m.group(1))
            continue

        m = RE_YAW_ADMITTED.match(stripped)
        if m:
            current["yaw_admitted_count"] = int(m.group(1))
            current["yaw_keep_count"] = int(m.group(2))
            current["yaw_flip_count"] = int(m.group(3))
            continue

        m = RE_YAW_FLIPS.match(stripped)
        if m:
            current["yaw_flip_tags"] = _parse_tag_list(m.group(1))
            continue

        m = RE_COLLECTION.match(stripped)
        if m:
            current["collection_conclusion"] = m.group(1)
            continue

        m = RE_NOTES.match(stripped)
        if m:
            current["collection_notes"] = m.group(1)
            continue

    if current is not None:
        samples.append(current)

    return samples


def samples_to_long_rows(
    samples: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for sample in samples:
        cut_tags = set(sample["distance_components_cut_tags"])
        flip_tags = set(sample["yaw_flip_tags"])

        for obs in sample["observations"]:
            tag_id = int(obs["tag_id"])
            role = str(obs["camera_role"])

            yaw_diag = sample["yaw_diag_by_tag_camera"].get(
                (tag_id, role),
                {},
            )

            # Infer Layer-1 usability from the current frozen rule.
            # This is a convenience field only; the printed sample summary
            # remains authoritative.
            d = float(obs["meas_distance_m"])
            a = float(obs["meas_angle_deg"])
            layer1_usable = not (
                role == "front"
                and (d > 5.5 or abs(a) > 30.0)
            )

            decision = str(
                yaw_diag.get("yaw_diagnostic_decision") or ""
            )
            # The diagnostic table is an older truth-aware diagnostic,
            # not the final solver admission source.  Therefore this field
            # is named "..._inferred" and should not be confused with the
            # final solver's exact internal yaw decision.
            yaw_admitted_inferred = decision.startswith("accept_")

            row = {
                "sample_index": sample["sample_index"],
                "gt_x_m": sample["gt_x_m"],
                "gt_y_m": sample["gt_y_m"],
                "gt_heading_deg": sample["gt_heading_deg"],
                "preferred_heading_deg": sample["preferred_heading_deg"],

                "tags_observed": sample["tags_observed"],
                "front_observed": sample["front_observed"],
                "rear_observed": sample["rear_observed"],
                "tag_ids_observed": _fmt_tag_list(
                    sample["tag_ids_observed"]
                ),

                "layer1_usable_count": sample["layer1_usable_count"],
                "unique_usable_tags": sample["unique_usable_tags"],
                "usable_front_count": sample["usable_front_count"],
                "usable_rear_count": sample["usable_rear_count"],
                "layer1_gated_out": sample["layer1_gated_out"],

                "pass1_x_m": sample["pass1_x_m"],
                "pass1_y_m": sample["pass1_y_m"],
                "pass1_heading_deg": sample["pass1_heading_deg"],

                "final_x_m": sample["final_x_m"],
                "final_y_m": sample["final_y_m"],
                "final_heading_deg": sample["final_heading_deg"],

                "final_position_error_cm": (
                    sample["final_position_error_cm"]
                ),
                "final_heading_error_deg": (
                    sample["final_heading_error_deg"]
                ),

                "distance_components_cut_count": (
                    sample["distance_components_cut_count"]
                ),
                "distance_components_cut_tags": _fmt_tag_list(
                    sample["distance_components_cut_tags"]
                ),
                "angle_components_cut_count": (
                    sample["angle_components_cut_count"]
                ),

                "yaw_admitted_count": sample["yaw_admitted_count"],
                "yaw_keep_count": sample["yaw_keep_count"],
                "yaw_flip_count": sample["yaw_flip_count"],
                "yaw_flip_tags": _fmt_tag_list(
                    sample["yaw_flip_tags"]
                ),

                "collection_conclusion": (
                    sample["collection_conclusion"]
                ),
                "collection_notes": sample["collection_notes"],

                **obs,

                "yaw_keep_error_deg": yaw_diag.get(
                    "yaw_keep_error_deg"
                ),
                "yaw_flip_error_deg": yaw_diag.get(
                    "yaw_flip_error_deg"
                ),
                "yaw_best_mode": yaw_diag.get("yaw_best_mode"),
                "yaw_best_error_deg": yaw_diag.get(
                    "yaw_best_error_deg"
                ),
                "yaw_gain_deg": yaw_diag.get("yaw_gain_deg"),
                "yaw_separation_deg": yaw_diag.get(
                    "yaw_separation_deg"
                ),
                "yaw_quantized_deg": yaw_diag.get(
                    "yaw_quantized_deg"
                ),
                "yaw_diagnostic_decision": decision,

                "layer1_tag_usable_inferred": layer1_usable,
                "distance_component_cut": tag_id in cut_tags,
                "yaw_admitted_inferred": yaw_admitted_inferred,
                "yaw_flip_inferred": tag_id in flip_tags,
            }

            rows.append(row)

    return rows


def write_csv(
    rows: List[Dict[str, Any]],
    output_csv: Path,
) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    with output_csv.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=CSV_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def validate_parse(
    samples: List[Dict[str, Any]],
    rows: List[Dict[str, Any]],
) -> None:
    print("")
    print("=" * 82)
    print("VALIDATION LOG PARSE CHECK")
    print("=" * 82)
    print(f"Samples parsed          : {len(samples)}")
    print(f"Tag rows parsed         : {len(rows)}")

    samples_missing_final = [
        s["sample_index"]
        for s in samples
        if s["final_x_m"] is None
        or s["final_y_m"] is None
        or s["final_position_error_cm"] is None
    ]

    samples_missing_obs = [
        s["sample_index"]
        for s in samples
        if len(s["observations"]) == 0
    ]

    count_mismatches = []
    for s in samples:
        expected = s["tags_observed"]
        actual = len(s["observations"])
        if expected is not None and expected != actual:
            count_mismatches.append(
                (s["sample_index"], expected, actual)
            )

    print(f"Samples missing final   : {samples_missing_final or 'none'}")
    print(f"Samples missing tag rows: {samples_missing_obs or 'none'}")
    print(f"Observed-count mismatch : {count_mismatches or 'none'}")

    if samples:
        worst = sorted(
            (
                s for s in samples
                if s["final_position_error_cm"] is not None
            ),
            key=lambda s: s["final_position_error_cm"],
            reverse=True,
        )[:5]

        print("")
        print("Top final position errors:")
        for s in worst:
            print(
                f"  sample {s['sample_index']:02d}: "
                f"GT=({s['gt_x_m']:.3f},{s['gt_y_m']:.3f},"
                f"{s['gt_heading_deg']:.1f}) "
                f"error={s['final_position_error_cm']:.1f} cm"
            )

    print("=" * 82)
    print("")


def main(
    input_txt: Path,
    output_csv: Path,
) -> None:
    text = input_txt.read_text(
        encoding="utf-8",
        errors="replace",
    )

    samples = parse_validation_log(text)
    rows = samples_to_long_rows(samples)
    write_csv(rows, output_csv)
    validate_parse(samples, rows)

    print(f"Input : {input_txt}")
    print(f"Output: {output_csv}")
    print("")


if __name__ == "__main__":
    # ---------------------------------------------------------------
    # VS Code direct-run parameters
    # ---------------------------------------------------------------

    INPUT_TXT = Path(
        r"D:\Data\_Action\_RunNMS\measure16-0707.txt"
    )

    OUTPUT_CSV = Path(
        r"D:\Data\_Action\_RunNMS\measure16-0707_parsed.csv"
    )

    main(
        input_txt=INPUT_TXT,
        output_csv=OUTPUT_CSV,
    )
