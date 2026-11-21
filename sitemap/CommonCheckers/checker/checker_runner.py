from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Support both execution styles:
#   1) package mode: python -m checker.checker_runner
#   2) direct VS Code file run: python sitemap/CommonCheckers/checker/checker_runner.py
#
# Direct file execution has no package context. In that case, add the
# CommonCheckers directory to sys.path and import through the checker package
# so sibling modules can keep their relative imports.
if __package__ in (None, ""):
    _COMMON_DIR_FOR_DIRECT_RUN = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(_COMMON_DIR_FOR_DIRECT_RUN))

    from checker.initialization_rules import check_first_mobility_command, check_initial_poses_exist
    from checker.script_model import load_initial_poses_csv, load_script_csv
    from checker.timeline_rules import check_first_move_after_report_location_spacing, check_global_moving_command_spacing
    from checker.movement_rules import check_max_single_mobility_move_distance
    from checker.validation_report import make_report, sha256_file, write_report
    from checker.vocabulary_rules import check_device_targets, check_vocabulary
    from checker.argument_rules import check_command_arguments
    from checker.macro_rules import check_macro_and_bump_rules
    from checker.path_rules import check_planned_path_rules
    from checker.traffic_rules import check_traffic_sessions
    from checker.reserved_location_rules import resolve_reserved_launch_locations, write_normalized_script_csv
else:
    from .initialization_rules import check_first_mobility_command, check_initial_poses_exist
    from .script_model import load_initial_poses_csv, load_script_csv
    from .timeline_rules import check_first_move_after_report_location_spacing, check_global_moving_command_spacing
    from .movement_rules import check_max_single_mobility_move_distance
    from .validation_report import make_report, sha256_file, write_report
    from .vocabulary_rules import check_device_targets, check_vocabulary
    from .argument_rules import check_command_arguments
    from .macro_rules import check_macro_and_bump_rules
    from .path_rules import check_planned_path_rules
    from .traffic_rules import check_traffic_sessions
    from .reserved_location_rules import resolve_reserved_launch_locations, write_normalized_script_csv


def _load_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _load_common_version(common_dir: Path) -> str:
    return str(
        _load_json(common_dir / "VERSION.json").get("common_checkers_version", "")
    )


def _load_site_version(site_dir: Path) -> tuple[str, str]:
    data = _load_json(site_dir / "script_authoring" / "VERSION.json")
    return (
        str(data.get("site_id", "")),
        str(data.get("site_authoring_version", "")),
    )


def _has_initial_pose_file_blocking_error(issues: list[Dict[str, Any]]) -> bool:
    """
    If the initial-pose file is missing or structurally invalid, avoid noisy
    follow-on per-robot errors such as MISSING_INITIAL_POSE. The user should fix
    the file/path first.
    """
    blocking_codes = {
        "INITIAL_POSES_CSV_MISSING",
        "INITIAL_POSES_CSV_MISSING_COLUMNS",
    }
    return any(
        issue.get("level") == "error" and issue.get("code") in blocking_codes
        for issue in issues
    )



def validate_script(
    *,
    script_csv: str | Path,
    initial_poses_csv: Optional[str | Path] = None,
    site_dir: str | Path,
    common_dir: str | Path,
    report_json: Optional[str | Path] = None,
    normalized_script_csv: Optional[str | Path] = None,
    prepared_manifest: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    script_csv = Path(script_csv)
    # I/J is the pose source even when the NMS supplies its internal adapter argument.
    initial_poses_csv = script_csv
    site_dir = Path(site_dir)
    common_dir = Path(common_dir)

    common_version = _load_common_version(common_dir)
    site_id, site_version = _load_site_version(site_dir)
    policy = _load_json(common_dir / "config" / "script_policy.json")
    macro_policy = _load_json(site_dir / "script_authoring" / "config" / "macro_policy.json")
    zone_policy = _load_json(site_dir / "script_authoring" / "config" / "zone_policy.json")
    path_policy = _load_json(site_dir / "script_authoring" / "config" / "path_policy.json")
    safety_policy = _load_json(site_dir / "script_authoring" / "config" / "safety_policy.json")
    ap_roster = _load_json(site_dir / "script_authoring" / "config" / "ap_roster.json")

    if prepared_manifest is None:
        from single_csv_authoring import checker_manifest
        prepared_manifest = checker_manifest(script_csv.read_bytes())
    from checker.script_model import ScriptRow, InitialPose
    source_rows = [ScriptRow(c["row_number"],c["scanner"],c["t_offset_sec"],c["category"].lower(),c["action"],c["args"])
                   for c in prepared_manifest["commands"]]
    initial_poses = {p["scanner"]: InitialPose(p["row_number"],p["scanner"],p["x_m"],p["y_m"],p["heading_deg"],.3,10)
                     for p in prepared_manifest["initial_positions"]}
    from single_csv_authoring import reminder_issues
    issues, pose_issues = reminder_issues(prepared_manifest, macro_policy), []
    issues.extend(pose_issues)

    rows, reserved_issues, resolved_constant_count = resolve_reserved_launch_locations(
        source_rows,
        macro_policy,
    )
    issues.extend(reserved_issues)

    issues.extend(check_vocabulary(rows, policy))
    issues.extend(check_command_arguments(rows, policy))
    ap_scanners = {
        str(item.get("scanner", "")).strip()
        for item in (ap_roster.get("aps", []) or [])
        if isinstance(item, dict) and str(item.get("scanner", "")).strip()
    }
    issues.extend(check_device_targets(rows, set(initial_poses), ap_scanners))
    issues.extend(check_traffic_sessions(rows, initial_poses))
    prepared_single_csv = prepared_manifest is not None and prepared_manifest.get("schema_version") == "experiment_csv_v2"
    if prepared_single_csv:
        from checker.initialization_rules import check_prepared_mobility_sequence
        issues.extend(check_prepared_mobility_sequence(rows, policy))
    else:
        issues.extend(check_first_mobility_command(rows, policy))

    # If the initial-pose file itself is missing or malformed, stop here for
    # pose-dependent checks. This avoids duplicate/confusing messages such as
    # "missing intended initial pose for robot X" when the real problem is a bad
    # file path or wrong CSV type.
    if not _has_initial_pose_file_blocking_error(pose_issues):
        issues.extend(check_initial_poses_exist(rows, initial_poses))
        issues.extend(check_max_single_mobility_move_distance(rows, initial_poses, policy, macro_policy))
        issues.extend(check_macro_and_bump_rules(rows, initial_poses, macro_policy))
        issues.extend(check_planned_path_rules(rows, initial_poses, macro_policy, zone_policy, path_policy, safety_policy, site_dir))

    if not prepared_single_csv:
        issues.extend(check_first_move_after_report_location_spacing(rows, policy))
    issues.extend(check_global_moving_command_spacing(rows, policy, initial_poses, macro_policy))

    ok = not any(x.get("level") == "error" for x in issues)

    report = make_report(
        ok=ok,
        issues=issues,
        script_path=script_csv,
        initial_poses_path=initial_poses_csv,
        site_id=site_id,
        site_version=site_version,
        common_checkers_version=common_version,
    )

    if prepared_manifest is not None:
        report["validated_commands"] = [dict(row_number=r.row_number,scanner=r.scanner,t_offset_sec=r.t_offset_sec,
                                             category=r.category,action=r.action,args=r.args) for r in rows]
    report["resolved_launch_constant_count"] = resolved_constant_count
    report["normalized_script_path"] = str(normalized_script_csv or "")
    report["csv_rewritten"] = False
    report["registration_format"] = "single_csv_ij_roster"

    if normalized_script_csv:
        normalized_path = Path(normalized_script_csv)
        if ok:
            report["source_script_path"] = report["script_path"]
            report["source_script_hash"] = report["script_hash"]
            # Preserve the registration contract and authored A-E. Resolution
            # of symbolic targets is returned in validated_commands only.
            if normalized_path.resolve() != script_csv.resolve():
                normalized_path.parent.mkdir(parents=True, exist_ok=True)
                normalized_path.write_bytes(script_csv.read_bytes())
            report["script_path"] = str(normalized_path)
            report["script_hash"] = sha256_file(normalized_path)
        elif normalized_path.resolve() != script_csv.resolve() and normalized_path.exists():
            normalized_path.unlink()

    if report_json:
        write_report(report, report_json)

    return report


def run_from_vscode() -> Dict[str, Any]:
    """
    VS Code entry point.

    Edit only the constants in this function, then press the VS Code Run button.
    No PowerShell arguments are required.
    """
    # ---------------------------------------------------------
    # Edit these paths for the script you want to check.
    #
    # SCRIPT_CSV is the experiment command script.
    # The same CSV carries I/J initial positions. No separate poses input.
    # ---------------------------------------------------------
    # Auto-detect the sitemap folder from this file location:
    #   <workspace>/sitemap/CommonCheckers/checker/checker_runner.py
    SITEMAP_DIR = Path(__file__).resolve().parents[2]

    SCRIPT_CSV = SITEMAP_DIR / "DemoRoom" / "script_authoring" / "examples" / "demo_invalid_move_too_long.csv"
    SITE_DIR = SITEMAP_DIR / "DemoRoom"
    COMMON_DIR = SITEMAP_DIR / "CommonCheckers"
    REPORT_JSON = SITEMAP_DIR / "validation_report.json"

    report = validate_script(
        script_csv=SCRIPT_CSV,
        site_dir=SITE_DIR,
        common_dir=COMMON_DIR,
        report_json=REPORT_JSON,
    )

    print(json.dumps(report, indent=2, ensure_ascii=False))

    if report.get("ok"):
        print("\nCHECK RESULT: PASS")
    else:
        print("\nCHECK RESULT: FAIL")
        print(f"Errors: {report.get('error_count', 0)}")
        print(f"Warnings: {report.get('warning_count', 0)}")

    print(f"Report written to: {REPORT_JSON}")
    return report


if __name__ == "__main__":
    run_from_vscode()
