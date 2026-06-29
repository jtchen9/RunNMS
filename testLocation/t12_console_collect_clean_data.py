r"""
t11_console_collect_clean_data.py

Field helper for clean DemoRoom localization data collection.

Normal workflow:
    1. Put latest robot observation in:
           testLocation\input\latest_location_observation.json
       or change DEFAULT_OBSERVATION_FILE below.
    2. Run this script.
    3. Enter ground-truth x,y.
    4. Script looks up preferred heading from t2 output.
    5. Rotate robot to that heading.
    6. Press Enter.
    7. Script reads DEFAULT_OBSERVATION_FILE automatically.
    8. Script prints a compact console report.
    9. Script saves detailed timestamped logs automatically.

No argparse is used.
"""

from __future__ import annotations

from pathlib import Path
import csv
import json
import math
import sys
from datetime import datetime

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from locationSolver.location_config import CAMERA_CONFIG
from locationSolver.location_items import DEFAULT_MEASUREMENT_WEIGHTS, make_tag_observation
from locationSolver.location_tagmap import load_tag_map
from locationSolver.location_geometry import abs_angle_error_deg
from locationSolver.location_joint_solver_yaw import JointYawSolverConfig, solve_distance_angle_yaw_pose
from locationSolver.location_item_filter import (
    ItemFilterConfig,
    build_filter_states_from_joint_yaw_residuals,
    count_filter_states,
)
from locationSolver.location_filtered_solver import solve_filtered_pose
from locationSolver.location_confidence import ConfidenceConfig, evaluate_pose_confidence


def prompt_float(label: str) -> float:
    while True:
        text = input(label).strip()
        try:
            return float(text)
        except ValueError:
            print("Please enter a number.")


def prompt_optional_float(label: str) -> float | None:
    text = input(label).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        print("Invalid number; ignored.")
        return None


def first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def first_existing_key(d: dict, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in d:
            return c
    return None


def float_or_nan(v) -> float:
    try:
        return float(v)
    except Exception:
        return float("nan")


def str_or_empty(v) -> str:
    try:
        return str(v)
    except Exception:
        return ""


def load_observations_from_csv(path: Path):
    df = pd.read_csv(path)

    col_tag = first_existing_col(df, ["tag_id", "tag", "tag_no", "id"])
    col_cam = first_existing_col(df, ["camera_role", "camera", "cam_role", "camera_name"])
    col_d = first_existing_col(df, ["meas_distance_m", "distance_m", "measured_distance_m", "distance"])
    col_a = first_existing_col(df, ["meas_angle_deg", "angle_deg", "measured_angle_deg", "angle_deg_cw", "angle"])
    col_yaw = first_existing_col(df, ["meas_yaw_deg", "yaw_deg", "measured_yaw_deg", "yaw"])

    missing = [
        name for name, col in [
            ("tag_id", col_tag),
            ("camera_role", col_cam),
            ("distance", col_d),
            ("angle", col_a),
            ("yaw", col_yaw),
        ]
        if col is None
    ]
    if missing:
        raise ValueError(f"CSV missing required columns {missing}. Columns={list(df.columns)}")

    out = []
    for idx, r in df.iterrows():
        out.append(
            make_tag_observation(
                tag_id=int(r[col_tag]),
                camera_role=str_or_empty(r[col_cam]),
                distance_m=float_or_nan(r[col_d]),
                angle_deg=float_or_nan(r[col_a]),
                yaw_deg=float_or_nan(r[col_yaw]),
                source=f"{path.name}:row={idx}",
            )
        )
    return out


def normalize_json_observation_record(d: dict, source: str):
    k_tag = first_existing_key(d, ["tag_id", "tag", "tag_no", "id"])
    k_cam = first_existing_key(d, ["camera_role", "camera", "cam_role", "camera_name"])
    k_d = first_existing_key(d, ["meas_distance_m", "distance_m", "measured_distance_m", "distance"])
    k_a = first_existing_key(d, ["meas_angle_deg", "angle_deg", "measured_angle_deg", "angle_deg_cw", "angle"])
    k_yaw = first_existing_key(d, ["meas_yaw_deg", "yaw_deg", "measured_yaw_deg", "yaw"])

    missing = [
        name for name, key in [
            ("tag_id", k_tag),
            ("camera_role", k_cam),
            ("distance", k_d),
            ("angle", k_a),
            ("yaw", k_yaw),
        ]
        if key is None
    ]
    if missing:
        raise ValueError(f"JSON observation missing required keys {missing}: {d}")

    return make_tag_observation(
        tag_id=int(d[k_tag]),
        camera_role=str_or_empty(d[k_cam]),
        distance_m=float_or_nan(d[k_d]),
        angle_deg=float_or_nan(d[k_a]),
        yaw_deg=float_or_nan(d[k_yaw]),
        source=source,
    )


def load_observations_from_json_text(json_text: str, source: str):
    data = json.loads(json_text)

    if isinstance(data, dict):
        if "observations" in data:
            records = data["observations"]
        elif "tags" in data:
            records = data["tags"]
        elif "detections" in data:
            records = data["detections"]
        else:
            records = [data]
    elif isinstance(data, list):
        records = data
    else:
        raise ValueError("JSON must be a list or dict.")

    out = []
    for i, rec in enumerate(records):
        if not isinstance(rec, dict):
            raise ValueError(f"Observation #{i} is not a dict: {rec}")
        out.append(normalize_json_observation_record(rec, source=f"{source}:item={i}"))
    return out


def load_observations_from_file(path: Path):
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()

    if not path.exists():
        raise FileNotFoundError(f"Observation file not found: {path}")

    if path.suffix.lower() == ".csv":
        return load_observations_from_csv(path), str(path)

    if path.suffix.lower() == ".json":
        return (
            load_observations_from_json_text(
                path.read_text(encoding="utf-8"),
                source=path.name,
            ),
            str(path),
        )

    raise ValueError(f"Unsupported observation file suffix: {path.suffix}")


def find_preferred_heading_files(preferred_dir: Path) -> list[Path]:
    if not preferred_dir.exists():
        return []

    files = []
    for suffix in ["*.csv", "*.json"]:
        files.extend(preferred_dir.rglob(suffix))

    def priority(p: Path):
        name = p.name.lower()
        if "preferred_heading_lookup" in name:
            return 0
        if "preferred" in name and "heading" in name:
            return 1
        if "heading" in name:
            return 2
        return 3

    return sorted(files, key=priority)


def lookup_heading_from_csv(path: Path, x_m: float, y_m: float):
    try:
        df = pd.read_csv(path)
    except Exception:
        return None

    x_col = first_existing_col(df, ["x_m", "x", "grid_x_m", "robot_x", "center_x"])
    y_col = first_existing_col(df, ["y_m", "y", "grid_y_m", "robot_y", "center_y"])
    h_col = first_existing_col(df, [
        "preferred_heading_deg",
        "best_heading_deg",
        "heading_deg",
        "preferred_heading",
        "best_heading",
        "heading",
    ])
    if x_col is None or y_col is None or h_col is None:
        return None

    tmp = df[[x_col, y_col, h_col]].copy()
    tmp[x_col] = pd.to_numeric(tmp[x_col], errors="coerce")
    tmp[y_col] = pd.to_numeric(tmp[y_col], errors="coerce")
    tmp[h_col] = pd.to_numeric(tmp[h_col], errors="coerce")
    tmp = tmp.dropna()
    if len(tmp) == 0:
        return None

    dist2 = (tmp[x_col] - x_m) ** 2 + (tmp[y_col] - y_m) ** 2
    idx = dist2.idxmin()
    return {
        "heading_deg": float(tmp.loc[idx, h_col]) % 360.0,
        "source_file": str(path),
        "nearest_x_m": float(tmp.loc[idx, x_col]),
        "nearest_y_m": float(tmp.loc[idx, y_col]),
        "nearest_dist_m": math.sqrt(float(dist2.loc[idx])),
    }


def lookup_heading_from_json(path: Path, x_m: float, y_m: float):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    if isinstance(data, dict):
        for key in ["points", "records", "lookup", "data"]:
            if key in data and isinstance(data[key], list):
                data = data[key]
                break
    if not isinstance(data, list):
        return None

    best = None
    best_d2 = None
    for rec in data:
        if not isinstance(rec, dict):
            continue
        kx = first_existing_key(rec, ["x_m", "x", "grid_x_m", "robot_x", "center_x"])
        ky = first_existing_key(rec, ["y_m", "y", "grid_y_m", "robot_y", "center_y"])
        kh = first_existing_key(rec, [
            "preferred_heading_deg",
            "best_heading_deg",
            "heading_deg",
            "preferred_heading",
            "best_heading",
            "heading",
        ])
        if kx is None or ky is None or kh is None:
            continue
        rx = float_or_nan(rec[kx])
        ry = float_or_nan(rec[ky])
        rh = float_or_nan(rec[kh])
        if not all(math.isfinite(v) for v in [rx, ry, rh]):
            continue

        d2 = (rx - x_m) ** 2 + (ry - y_m) ** 2
        if best is None or d2 < best_d2:
            best = {
                "heading_deg": rh % 360.0,
                "source_file": str(path),
                "nearest_x_m": rx,
                "nearest_y_m": ry,
                "nearest_dist_m": math.sqrt(d2),
            }
            best_d2 = d2
    return best


def lookup_preferred_heading(x_m: float, y_m: float, preferred_dir: Path):
    for p in find_preferred_heading_files(preferred_dir):
        if p.suffix.lower() == ".csv":
            result = lookup_heading_from_csv(p, x_m, y_m)
        elif p.suffix.lower() == ".json":
            result = lookup_heading_from_json(p, x_m, y_m)
        else:
            result = None

        if result is not None:
            return result
    return None


def run_pipeline(
    observations,
    tag_map,
    x_gt_m: float,
    y_gt_m: float,
    heading_gt_deg: float,
    camera_config,
    measurement_weights,
    solver_config,
    filter_config,
    confidence_config,
):
    pass1 = solve_distance_angle_yaw_pose(
        observations=observations,
        tag_map=tag_map,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
        initial_x_m=x_gt_m,
        initial_y_m=y_gt_m,
        initial_heading_deg=heading_gt_deg,
        solver_config=solver_config,
    )

    filter_states = build_filter_states_from_joint_yaw_residuals(
        residual_records=pass1.residuals,
        cfg=filter_config,
    )
    filter_counts = count_filter_states(filter_states.values())

    filtered = solve_filtered_pose(
        observations=observations,
        tag_map=tag_map,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
        filter_states=filter_states,
        initial_x_m=pass1.x_m,
        initial_y_m=pass1.y_m,
        initial_heading_deg=pass1.heading_deg,
        solver_config=solver_config,
    )

    filter_shift_pos_cm = 100.0 * math.hypot(filtered.x_m - pass1.x_m, filtered.y_m - pass1.y_m)
    filter_shift_heading_deg = abs_angle_error_deg(filtered.heading_deg, pass1.heading_deg)

    # Current installed interface uses 3 positional args.
    confidence = evaluate_pose_confidence(filtered, tag_map, confidence_config)

    return {
        "pass1": pass1,
        "filter_states": filter_states,
        "filter_counts": filter_counts,
        "filtered": filtered,
        "confidence": confidence,
        "filter_shift_pos_cm": filter_shift_pos_cm,
        "filter_shift_heading_deg": filter_shift_heading_deg,
    }


def get_conf_value(conf, names, default=0):
    """
    Read a confidence-report value using several possible field names.
    This keeps t11 tolerant while location_confidence.py is still evolving.
    """
    for name in names:
        if hasattr(conf, name):
            return getattr(conf, name)

    if hasattr(conf, "to_dict"):
        d = conf.to_dict()
        for name in names:
            if name in d:
                return d[name]

    return default


def print_compact_report(result_pack, x_gt_m: float, y_gt_m: float, heading_gt_deg: float, observation_count: int):
    filtered = result_pack["filtered"]
    conf = result_pack["confidence"]
    counts = result_pack["filter_counts"]

    pos_err_cm = 100.0 * math.hypot(filtered.x_m - x_gt_m, filtered.y_m - y_gt_m)
    heading_err_deg = abs_angle_error_deg(filtered.heading_deg, heading_gt_deg)

    print("")
    print("=" * 72)
    print("CLEAN DATA SAMPLE REPORT")
    print("=" * 72)
    print(f"Ground truth     : x={x_gt_m:.3f}, y={y_gt_m:.3f}, h={heading_gt_deg:.1f} deg")
    print(f"Estimated        : x={filtered.x_m:.3f}, y={filtered.y_m:.3f}, h={filtered.heading_deg:.1f} deg")
    print(f"Error            : position={pos_err_cm:.1f} cm, heading={heading_err_deg:.1f} deg")
    print(f"Confidence       : {conf.confidence} ({conf.reason})")
    print("")
    print(f"Observations     : {observation_count}")

    distance_used_count = get_conf_value(
        conf,
        ["distance_used_count", "distance_item_count", "distance_tag_count"],
        0,
    )
    angle_used_count = get_conf_value(
        conf,
        ["angle_used_count", "angle_item_count", "angle_tag_count"],
        0,
    )
    yaw_used_count = get_conf_value(
        conf,
        ["yaw_used_count", "yaw_item_count", "yaw_tag_count"],
        0,
    )
    tag_used_count = get_conf_value(
        conf,
        ["tag_used_count", "used_tag_count", "tag_count"],
        0,
    )

    distance_bearing_span_deg = get_conf_value(
        conf,
        ["distance_bearing_span_deg", "bearing_span_deg"],
        0.0,
    )
    angle_bearing_span_deg = get_conf_value(
        conf,
        ["angle_bearing_span_deg", "bearing_span_deg"],
        0.0,
    )

    distance_rms_cm = get_conf_value(
        conf,
        ["distance_rms_cm"],
        0.0,
    )
    angle_rms_deg = get_conf_value(
        conf,
        ["angle_rms_deg"],
        0.0,
    )
    yaw_rms_deg = get_conf_value(
        conf,
        ["yaw_rms_deg"],
        0.0,
    )

    distance_max_abs_cm = get_conf_value(
        conf,
        ["distance_max_abs_cm", "distance_max_cm"],
        0.0,
    )
    angle_max_abs_deg = get_conf_value(
        conf,
        ["angle_max_abs_deg", "angle_max_deg"],
        0.0,
    )
    yaw_max_abs_deg = get_conf_value(
        conf,
        ["yaw_max_abs_deg", "yaw_max_deg"],
        0.0,
    )

    print(f"Used items       : distance={distance_used_count}, angle={angle_used_count}, yaw={yaw_used_count}")
    print(f"Used tags        : {tag_used_count}")
    print(f"Bearing span     : distance={distance_bearing_span_deg:.1f} deg, angle={angle_bearing_span_deg:.1f} deg")
    print("")
    print(f"Residual RMS     : distance={distance_rms_cm:.1f} cm, angle={angle_rms_deg:.1f} deg, yaw={yaw_rms_deg:.1f} deg")
    print(f"Residual max     : distance={distance_max_abs_cm:.1f} cm, angle={angle_max_abs_deg:.1f} deg, yaw={yaw_max_abs_deg:.1f} deg")

    print("")
    print(f"Filter disabled  : total={counts['disabled']}, distance={counts['distance_disabled']}, angle={counts['angle_disabled']}, yaw={counts['yaw_disabled']}")
    print(f"Filter shift     : position={result_pack['filter_shift_pos_cm']:.1f} cm, heading={result_pack['filter_shift_heading_deg']:.1f} deg")
    print("")
    print("Quick judgement:")
    if conf.confidence == "GOOD" and pos_err_cm <= 10.0 and heading_err_deg <= 5.0:
        print("  Excellent. This sample meets the 10 cm / 5 deg target.")
    elif conf.confidence in {"GOOD", "MARGINAL"}:
        print("  Useful sample. Keep it for parameter tuning.")
    else:
        print("  Not trustworthy yet. Keep the log, but inspect residuals before using it for tuning.")
    print("=" * 72)


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        return

    keys = []
    for row in rows:
        for k in row.keys():
            if k not in keys:
                keys.append(k)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


def write_outputs(
    output_dir: Path,
    sample_id: str,
    x_gt_m: float,
    y_gt_m: float,
    heading_gt_deg: float,
    preferred,
    observation_source: str,
    observations,
    result_pack,
    config_dump: dict,
):
    output_dir.mkdir(parents=True, exist_ok=True)

    pass1 = result_pack["pass1"]
    filtered = result_pack["filtered"]
    conf = result_pack["confidence"]
    counts = result_pack["filter_counts"]

    pos_err_cm = 100.0 * math.hypot(filtered.x_m - x_gt_m, filtered.y_m - y_gt_m)
    heading_err_deg = abs_angle_error_deg(filtered.heading_deg, heading_gt_deg)

    summary = {
        "sample_id": sample_id,
        "ts_local": datetime.now().isoformat(timespec="seconds"),
        "ground_truth": {
            "x_m": x_gt_m,
            "y_m": y_gt_m,
            "heading_deg": heading_gt_deg,
        },
        "preferred_heading": preferred,
        "observation_source": observation_source,
        "observation_count": len(observations),
        "pass1": {
            "ok": pass1.ok,
            "x_m": pass1.x_m,
            "y_m": pass1.y_m,
            "heading_deg": pass1.heading_deg,
            "distance_rms_cm": pass1.distance_rms_cm,
            "angle_rms_deg": pass1.angle_rms_deg,
            "yaw_rms_deg": pass1.yaw_rms_deg,
        },
        "filtered": {
            "ok": filtered.ok,
            "x_m": filtered.x_m,
            "y_m": filtered.y_m,
            "heading_deg": filtered.heading_deg,
            "pos_err_cm": pos_err_cm,
            "heading_err_deg": heading_err_deg,
            "distance_rms_cm": filtered.distance_rms_cm,
            "angle_rms_deg": filtered.angle_rms_deg,
            "yaw_rms_deg": filtered.yaw_rms_deg,
        },
        "confidence": conf.to_dict(),
        "filter_counts": counts,
        "filter_shift_pos_cm": result_pack["filter_shift_pos_cm"],
        "filter_shift_heading_deg": result_pack["filter_shift_heading_deg"],
        "config": config_dump,
    }

    summary_path = output_dir / f"{sample_id}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    obs_rows = [
        {
            "tag_id": o.tag_id,
            "camera_role": o.camera_role,
            "distance_m": o.distance_m,
            "angle_deg": o.angle_deg,
            "yaw_deg": o.yaw_deg,
            "source": o.source,
        }
        for o in observations
    ]
    write_csv(obs_rows, output_dir / f"{sample_id}_observations.csv")

    state_rows = [s.to_dict() for s in result_pack["filter_states"].values()]
    write_csv(state_rows, output_dir / f"{sample_id}_item_states.csv")

    residual_rows = [
        {
            "tag_id": r.tag_id,
            "camera_role": r.camera_role,
            "distance_residual_cm": r.distance_residual_cm,
            "distance_weight": r.distance_weight,
            "angle_residual_deg": r.angle_residual_deg,
            "angle_weight": r.angle_weight,
            "yaw_residual_deg": r.yaw_residual_deg,
            "yaw_weight": r.yaw_weight,
        }
        for r in filtered.residuals
    ]
    write_csv(residual_rows, output_dir / f"{sample_id}_final_residuals.csv")

    return summary_path


if __name__ == "__main__":
    # -------------------------------------------------------------------------
    # Adjustable parameters
    # -------------------------------------------------------------------------
    TAG_FILE = ROOT_DIR / "sitemap" / "DemoRoom" / "tag_location.txt"

    PREFERRED_HEADING_DIR = (
        ROOT_DIR / "testLocation" / "output" / "preferred_heading_full"
    )

    DEFAULT_OBSERVATION_FILE = (
        ROOT_DIR / "testLocation" / "input" / "latest_location_observation.json"
    )

    OUTPUT_DIR = (
        ROOT_DIR / "testLocation" / "output" / "clean_data_collection"
    )

    camera_config = CAMERA_CONFIG
    measurement_weights = DEFAULT_MEASUREMENT_WEIGHTS

    solver_config = JointYawSolverConfig(
        sigma_distance_m=0.05,
        sigma_angle_deg=3.0,
        sigma_yaw_deg=10.0,
        search_initial_step_xy_m=0.20,
        search_initial_step_h_deg=10.0,
        search_min_step_xy_m=0.002,
        search_min_step_h_deg=0.10,
        max_iterations=250,
        robust_clip_sigma=3.0,
        x_min=-1.0,
        x_max=12.5,
        y_min=-1.0,
        y_max=12.5,
        local_heading_seed_offsets_deg=(-30.0, -15.0, 0.0, 15.0, 30.0),
        use_global_heading_seeds=True,
        global_heading_seed_step_deg=45.0,
    )

    filter_config = ItemFilterConfig(
        distance_good_m=0.08,
        distance_weak_m=0.20,
        distance_severe_m=0.35,
        angle_good_deg=5.0,
        angle_weak_deg=12.0,
        angle_severe_deg=20.0,
        yaw_good_deg=10.0,
        yaw_weak_deg=25.0,
        yaw_severe_deg=45.0,
        min_distance_items_to_keep=3,
        min_angle_items_to_keep=2,
        weak_multiplier=0.50,
        bad_but_needed_multiplier=0.25,
        yaw_weak_multiplier=0.25,
    )

    confidence_config = ConfidenceConfig()

    # -------------------------------------------------------------------------
    # Field collection flow
    # -------------------------------------------------------------------------
    print("")
    print("=" * 72)
    print("DemoRoom clean localization data collection")
    print("=" * 72)
    print(f"ROOT_DIR              = {ROOT_DIR}")
    print(f"TAG_FILE              = {TAG_FILE}")
    print(f"PREFERRED_HEADING_DIR = {PREFERRED_HEADING_DIR}")
    print(f"OBSERVATION_FILE      = {DEFAULT_OBSERVATION_FILE}")
    print(f"OUTPUT_DIR            = {OUTPUT_DIR}")
    print("")

    x_gt_m = prompt_float("Input ground-truth x_m: ")
    y_gt_m = prompt_float("Input ground-truth y_m: ")

    preferred = lookup_preferred_heading(x_gt_m, y_gt_m, PREFERRED_HEADING_DIR)

    if preferred is None:
        print("")
        print("Preferred-heading lookup failed.")
        preferred_heading_deg = prompt_float("Manually input target heading_deg: ")
        preferred = {
            "heading_deg": preferred_heading_deg,
            "source_file": "manual_input",
            "nearest_x_m": x_gt_m,
            "nearest_y_m": y_gt_m,
            "nearest_dist_m": 0.0,
        }
    else:
        preferred_heading_deg = preferred["heading_deg"]

    print("")
    print("-" * 72)
    print(f"Preferred heading at x={x_gt_m:.3f}, y={y_gt_m:.3f}: {preferred_heading_deg:.1f} deg")
    print(f"Lookup source: {preferred['source_file']}")
    print(f"Nearest lookup point: x={preferred['nearest_x_m']:.3f}, y={preferred['nearest_y_m']:.3f}, distance={preferred['nearest_dist_m']:.3f} m")
    print("-" * 72)
    print("")
    print(f"Please rotate robot to heading {preferred_heading_deg:.1f} deg.")
    print("After the robot is correctly oriented and stable, press Enter.")
    input("Press Enter to continue...")

    actual_heading = prompt_optional_float(
        f"Input actual heading_deg, or press Enter to use {preferred_heading_deg:.1f}: "
    )
    if actual_heading is None:
        actual_heading = preferred_heading_deg

    print("")
    print("Reading observation from:")
    print(f"  {DEFAULT_OBSERVATION_FILE}")

    if not DEFAULT_OBSERVATION_FILE.exists():
        DEFAULT_OBSERVATION_FILE.parent.mkdir(parents=True, exist_ok=True)

        example = {
            "observations": [
                {
                    "tag_id": 40,
                    "camera_role": "front",
                    "distance_m": 1.23,
                    "angle_deg": 4.5,
                    "yaw_deg": -2.1
                },
                {
                    "tag_id": 55,
                    "camera_role": "rear",
                    "distance_m": 1.85,
                    "angle_deg": -6.0,
                    "yaw_deg": 3.2
                }
            ]
        }

        DEFAULT_OBSERVATION_FILE.write_text(
            json.dumps(example, indent=2),
            encoding="utf-8",
        )

        print("")
        print("Observation file does not exist yet.")
        print("A template file has been created:")
        print(f"  {DEFAULT_OBSERVATION_FILE}")
        print("")
        print("Replace this template with the latest robot observation JSON, then run t11 again.")
        sys.exit(1)

    observations, observation_source = load_observations_from_file(DEFAULT_OBSERVATION_FILE)

    sample_id = datetime.now().strftime("clean_%Y%m%d_%H%M%S")

    print(f"Loaded observations: {len(observations)}")
    print("Running full location pipeline...")

    tag_map = load_tag_map(TAG_FILE)

    result_pack = run_pipeline(
        observations=observations,
        tag_map=tag_map,
        x_gt_m=x_gt_m,
        y_gt_m=y_gt_m,
        heading_gt_deg=actual_heading,
        camera_config=camera_config,
        measurement_weights=measurement_weights,
        solver_config=solver_config,
        filter_config=filter_config,
        confidence_config=confidence_config,
    )

    print_compact_report(
        result_pack=result_pack,
        x_gt_m=x_gt_m,
        y_gt_m=y_gt_m,
        heading_gt_deg=actual_heading,
        observation_count=len(observations),
    )

    config_dump = {
        "tag_file": str(TAG_FILE),
        "preferred_heading_dir": str(PREFERRED_HEADING_DIR),
        "default_observation_file": str(DEFAULT_OBSERVATION_FILE),
        "output_dir": str(OUTPUT_DIR),
        "camera_config": camera_config,
        "measurement_weights": measurement_weights,
        "solver_config": solver_config.__dict__,
        "filter_config": filter_config.__dict__,
        "confidence_config": getattr(confidence_config, "__dict__", {}),
    }

    summary_path = write_outputs(
        output_dir=OUTPUT_DIR,
        sample_id=sample_id,
        x_gt_m=x_gt_m,
        y_gt_m=y_gt_m,
        heading_gt_deg=actual_heading,
        preferred=preferred,
        observation_source=observation_source,
        observations=observations,
        result_pack=result_pack,
        config_dump=config_dump,
    )

    print("")
    print("Saved:")
    print(f"  {summary_path}")
    print(f"  {OUTPUT_DIR / (sample_id + '_observations.csv')}")
    print(f"  {OUTPUT_DIR / (sample_id + '_item_states.csv')}")
    print(f"  {OUTPUT_DIR / (sample_id + '_final_residuals.csv')}")
    print("")
