from __future__ import annotations

"""
rerun_validation_log_with_current_solver.py

Rerun all validation samples referenced by the long T11 text log through
the CURRENT final solver pipeline.

IMPORTANT:
- This does NOT reconstruct observations from printed tables.
- It loads each original saved "Obs JSON" file referenced in the log.
- Therefore the replay uses the actual collected observation payload,
  including the original per-observation fields and weights.
- It imports the same current validation companion and calls its existing
  pipeline function, avoiding a second implementation of the solver path.

Expected project layout:
    D:\Data\_Action\_RunNMS\
        t11_collect_validation_final_componentwise.py
        locationSolver\
        measure16-0707.txt
        testLocation\output\t11_observation_collection\obs_....json

Before running:
1. Install the Step-29 patched:
       locationSolver\src\common\component_preparation.py
2. Place this script under:
       D:\Data\_Action\_RunNMS\
   or edit ROOT_DIR below.
3. Open in VS Code and press Run.
"""

import csv
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

THIS_DIR = Path(__file__).resolve().parent
ROOT_DIR = THIS_DIR
LOCATION_SOLVER_DIR = ROOT_DIR / "locationSolver"

for p in (ROOT_DIR, LOCATION_SOLVER_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


# ---------------------------------------------------------------------------
# Reuse the exact current interactive pipeline implementation.
# This script does not duplicate solver logic.
# ---------------------------------------------------------------------------

import t11_collect_validation_final_componentwise as current_validation


FLOAT = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?"

RE_GT = re.compile(
    rf"^GT:\s*\(\s*({FLOAT})\s*,\s*({FLOAT})\s*,\s*({FLOAT})\s*\)\s*$"
)

RE_OBS_JSON = re.compile(
    r"^Obs JSON\s*:\s*(.+?\.json)\s*$",
    re.IGNORECASE,
)

RE_OLD_FINAL = re.compile(
    rf"^Final pose\s*:\s*x=({FLOAT}),\s*y=({FLOAT}),\s*h=({FLOAT})"
)

RE_OLD_HUMAN = re.compile(
    rf"^Human reference\s*:\s*position diff=({FLOAT})\s*cm,\s*"
    rf"heading diff=({FLOAT})"
)


def _wrap_angle_deg(a: float) -> float:
    return float((a + 180.0) % 360.0 - 180.0)


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _load_observations_from_obs_json(path: Path) -> List[Dict[str, Any]]:
    """
    Accept a few likely collector JSON shapes without modifying data.
    """
    with path.open("r", encoding="utf-8-sig") as f:
        payload = json.load(f)

    if isinstance(payload, dict):
        observations = payload.get("observations")
        if isinstance(observations, list):
            return observations

        # Conservative fallbacks for a wrapped payload.
        for key in ("payload", "report", "data"):
            inner = payload.get(key)
            if isinstance(inner, dict):
                observations = inner.get("observations")
                if isinstance(observations, list):
                    return observations

    raise ValueError(
        f"Cannot find observation list in JSON: {path}"
    )


def parse_validation_log(log_path: Path) -> List[Dict[str, Any]]:
    """
    Parse sample GT + original Obs JSON path + old printed final result.

    Sample boundary is anchored by GT lines.
    """
    text = log_path.read_text(
        encoding="utf-8",
        errors="replace",
    )

    samples: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None

    for raw_line in text.splitlines():
        line = raw_line.strip()

        m = RE_GT.match(line)
        if m:
            if current is not None:
                samples.append(current)

            current = {
                "sample_index": len(samples) + 1,
                "gt_x_m": float(m.group(1)),
                "gt_y_m": float(m.group(2)),
                "gt_heading_deg": float(m.group(3)),
                "obs_json_path": None,

                "old_final_x_m": None,
                "old_final_y_m": None,
                "old_final_heading_deg": None,
                "old_position_error_cm": None,
                "old_heading_error_deg": None,
            }
            continue

        if current is None:
            continue

        m = RE_OBS_JSON.match(line)
        if m:
            current["obs_json_path"] = m.group(1).strip()
            continue

        m = RE_OLD_FINAL.match(line)
        if m:
            current["old_final_x_m"] = float(m.group(1))
            current["old_final_y_m"] = float(m.group(2))
            current["old_final_heading_deg"] = float(m.group(3))
            continue

        m = RE_OLD_HUMAN.match(line)
        if m:
            current["old_position_error_cm"] = float(m.group(1))
            current["old_heading_error_deg"] = float(m.group(2))
            continue

    if current is not None:
        samples.append(current)

    return samples


def rerun_one(sample_meta: Dict[str, Any]) -> Dict[str, Any]:
    obs_path_text = sample_meta.get("obs_json_path")
    if not obs_path_text:
        raise ValueError(
            f"Sample {sample_meta['sample_index']} missing Obs JSON path"
        )

    obs_path = Path(obs_path_text)
    if not obs_path.exists():
        raise FileNotFoundError(
            f"Sample {sample_meta['sample_index']} Obs JSON not found: "
            f"{obs_path}"
        )

    observations = _load_observations_from_obs_json(obs_path)

    run = current_validation._run_current_final_pipeline(
        observations
    )

    pass1 = run["pass1"]
    final_result = run["final_result"]
    final_audit = run["final_audit"]

    distance_cut_tags = [
        int(row["tag_id"])
        for row in final_audit
        if not bool(row.get("distance_use"))
    ]

    distance_floor_kept_tags = [
        int(row["tag_id"])
        for row in final_audit
        if str(row.get("distance_use_reason") or "")
        == "kept_m2_min_active_distance_floor"
    ]

    yaw_admitted_tags = [
        int(row["tag_id"])
        for row in final_audit
        if bool(row.get("yaw_use"))
    ]

    out = dict(sample_meta)
    out.update({
        "rerun_success": False,
        "rerun_failure_reason": "",

        "observed_count": len(observations),
        "layer1_usable_count": len(
            run["sample"].get("observations") or []
        ),

        "new_pass1_x_m": None,
        "new_pass1_y_m": None,
        "new_pass1_heading_deg": None,

        "new_final_x_m": None,
        "new_final_y_m": None,
        "new_final_heading_deg": None,

        "new_position_error_cm": None,
        "new_heading_error_deg": None,

        "new_distance_cut_count": len(distance_cut_tags),
        "new_distance_cut_tags": "|".join(
            str(x) for x in distance_cut_tags
        ),

        "distance_floor_activated": bool(
            distance_floor_kept_tags
        ),
        "distance_floor_kept_tags": "|".join(
            str(x) for x in distance_floor_kept_tags
        ),

        "new_yaw_admitted_count": len(yaw_admitted_tags),
        "new_yaw_admitted_tags": "|".join(
            str(x) for x in yaw_admitted_tags
        ),
    })

    if pass1 is not None and pass1.success:
        out["new_pass1_x_m"] = float(pass1.estimated_x_m)
        out["new_pass1_y_m"] = float(pass1.estimated_y_m)
        out["new_pass1_heading_deg"] = (
            float(pass1.estimated_heading_deg) % 360.0
        )

    if final_result is None or not final_result.success:
        out["rerun_failure_reason"] = (
            "final_result_unavailable"
            if final_result is None
            else str(final_result.failure_reason)
        )
        return out

    est_x = float(final_result.estimated_x_m)
    est_y = float(final_result.estimated_y_m)
    est_h = float(final_result.estimated_heading_deg) % 360.0

    gt_x = float(sample_meta["gt_x_m"])
    gt_y = float(sample_meta["gt_y_m"])
    gt_h = float(sample_meta["gt_heading_deg"])

    pos_err_cm = 100.0 * math.hypot(
        est_x - gt_x,
        est_y - gt_y,
    )

    heading_err = abs(
        _wrap_angle_deg(est_h - gt_h)
    )

    out.update({
        "rerun_success": True,
        "new_final_x_m": est_x,
        "new_final_y_m": est_y,
        "new_final_heading_deg": est_h,
        "new_position_error_cm": pos_err_cm,
        "new_heading_error_deg": heading_err,
    })

    old_pos = _safe_float(sample_meta.get("old_position_error_cm"))
    if old_pos is not None:
        out["position_error_change_cm"] = pos_err_cm - old_pos
        out["position_error_improvement_cm"] = old_pos - pos_err_cm
    else:
        out["position_error_change_cm"] = None
        out["position_error_improvement_cm"] = None

    return out


CSV_FIELDS = [
    "sample_index",
    "gt_x_m",
    "gt_y_m",
    "gt_heading_deg",
    "obs_json_path",

    "old_final_x_m",
    "old_final_y_m",
    "old_final_heading_deg",
    "old_position_error_cm",
    "old_heading_error_deg",

    "rerun_success",
    "rerun_failure_reason",

    "observed_count",
    "layer1_usable_count",

    "new_pass1_x_m",
    "new_pass1_y_m",
    "new_pass1_heading_deg",

    "new_final_x_m",
    "new_final_y_m",
    "new_final_heading_deg",

    "new_position_error_cm",
    "new_heading_error_deg",

    "position_error_change_cm",
    "position_error_improvement_cm",

    "new_distance_cut_count",
    "new_distance_cut_tags",

    "distance_floor_activated",
    "distance_floor_kept_tags",

    "new_yaw_admitted_count",
    "new_yaw_admitted_tags",
]


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open(
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


def print_summary(rows: List[Dict[str, Any]]) -> None:
    ok = [
        r for r in rows
        if r.get("rerun_success")
        and r.get("new_position_error_cm") is not None
    ]

    print("")
    print("=" * 88)
    print("25-SAMPLE VALIDATION RERUN — CURRENT SOLVER WITH STEP-29 M2 FLOOR")
    print("=" * 88)
    print(f"Samples total        : {len(rows)}")
    print(f"Successful reruns    : {len(ok)}")
    print(
        "Floor activated      : "
        f"{sum(bool(r.get('distance_floor_activated')) for r in rows)}"
    )

    if ok:
        errors = [
            float(r["new_position_error_cm"])
            for r in ok
        ]
        print(f"Mean position error  : {sum(errors)/len(errors):.2f} cm")
        print(f"Max position error   : {max(errors):.2f} cm")

        print("")
        print("Top position errors after fix:")
        for r in sorted(
            ok,
            key=lambda x: float(x["new_position_error_cm"]),
            reverse=True,
        )[:10]:
            print(
                f"  sample {int(r['sample_index']):02d}: "
                f"{float(r['new_position_error_cm']):6.2f} cm "
                f"(old={r.get('old_position_error_cm')}, "
                f"floor={r.get('distance_floor_activated')}, "
                f"Dcut=[{r.get('new_distance_cut_tags','')}])"
            )

    print("")
    print("Samples where floor activated:")
    floor_rows = [
        r for r in rows
        if r.get("distance_floor_activated")
    ]
    if not floor_rows:
        print("  none")
    else:
        for r in floor_rows:
            print(
                f"  sample {int(r['sample_index']):02d}: "
                f"old={r.get('old_position_error_cm')} cm, "
                f"new={r.get('new_position_error_cm'):.2f} cm, "
                f"kept-by-floor=[{r.get('distance_floor_kept_tags','')}]"
            )

    print("=" * 88)
    print("")


def main(
    validation_log_path: Path,
    output_csv_path: Path,
) -> None:
    samples = parse_validation_log(validation_log_path)

    print(f"Parsed samples: {len(samples)}")
    if len(samples) != 25:
        print(
            "WARNING: expected 25 samples from this validation log; "
            f"found {len(samples)}"
        )

    rows: List[Dict[str, Any]] = []

    for sample in samples:
        index = sample["sample_index"]
        print(
            f"Rerunning sample {index:02d} "
            f"GT=({sample['gt_x_m']}, "
            f"{sample['gt_y_m']}, "
            f"{sample['gt_heading_deg']})"
        )

        try:
            row = rerun_one(sample)
        except Exception as exc:
            row = dict(sample)
            row.update({
                "rerun_success": False,
                "rerun_failure_reason": (
                    f"{type(exc).__name__}: {exc}"
                ),
            })
            print(f"  FAILED: {exc}")

        rows.append(row)

    write_csv(rows, output_csv_path)
    print_summary(rows)

    print(f"Output CSV: {output_csv_path}")


if __name__ == "__main__":
    # ------------------------------------------------------------------
    # VS Code direct-run parameters
    # ------------------------------------------------------------------

    VALIDATION_LOG_PATH = (
        ROOT_DIR / "measure16-0707.txt"
    )

    OUTPUT_CSV_PATH = (
        ROOT_DIR
        / "measure16-0707_rerun_step29.csv"
    )

    main(
        validation_log_path=VALIDATION_LOG_PATH,
        output_csv_path=OUTPUT_CSV_PATH,
    )
