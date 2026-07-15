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
    from checker.timeline_rules import check_global_moving_command_spacing
    from checker.validation_report import make_report, write_report
    from checker.vocabulary_rules import check_vocabulary
    from checker.macro_rules import check_macro_and_bump_rules
    from checker.path_rules import check_planned_path_rules
else:
    from .initialization_rules import check_first_mobility_command, check_initial_poses_exist
    from .script_model import load_initial_poses_csv, load_script_csv
    from .timeline_rules import check_global_moving_command_spacing
    from .validation_report import make_report, write_report
    from .vocabulary_rules import check_vocabulary
    from .macro_rules import check_macro_and_bump_rules
    from .path_rules import check_planned_path_rules


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
    initial_poses_csv: str | Path,
    site_dir: str | Path,
    common_dir: str | Path,
    report_json: Optional[str | Path] = None,
) -> Dict[str, Any]:
    script_csv = Path(script_csv)
    initial_poses_csv = Path(initial_poses_csv)
    site_dir = Path(site_dir)
    common_dir = Path(common_dir)

    common_version = _load_common_version(common_dir)
    site_id, site_version = _load_site_version(site_dir)
    policy = _load_json(common_dir / "config" / "script_policy.json")
    macro_policy = _load_json(site_dir / "script_authoring" / "config" / "macro_policy.json")
    bump_guard_zones = _load_json(site_dir / "script_authoring" / "config" / "bump_guard_zones.json")
    zone_policy = _load_json(site_dir / "script_authoring" / "config" / "zone_policy.json")
    path_policy = _load_json(site_dir / "script_authoring" / "config" / "path_policy.json")

    rows, issues = load_script_csv(script_csv)
    initial_poses, pose_issues = load_initial_poses_csv(initial_poses_csv)
    issues.extend(pose_issues)

    issues.extend(check_vocabulary(rows, policy))
    issues.extend(check_first_mobility_command(rows, policy))

    # If the initial-pose file itself is missing or malformed, stop here for
    # pose-dependent checks. This avoids duplicate/confusing messages such as
    # "missing intended initial pose for robot X" when the real problem is a bad
    # file path or wrong CSV type.
    if not _has_initial_pose_file_blocking_error(pose_issues):
        issues.extend(check_initial_poses_exist(rows, initial_poses))
        issues.extend(check_macro_and_bump_rules(rows, initial_poses, macro_policy, bump_guard_zones))
        issues.extend(check_planned_path_rules(rows, initial_poses, macro_policy, zone_policy, path_policy, site_dir))

    issues.extend(check_global_moving_command_spacing(rows, policy))

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
    # INITIAL_POSES_CSV is a different file: it must contain columns
    # scanner,intended_x_m,intended_y_m,intended_heading_deg,...
    #
    # Do not point INITIAL_POSES_CSV to a script CSV.
    # ---------------------------------------------------------
    # Auto-detect the sitemap folder from this file location:
    #   <workspace>/sitemap/CommonCheckers/checker/checker_runner.py
    SITEMAP_DIR = Path(__file__).resolve().parents[2]

    SCRIPT_CSV = SITEMAP_DIR / "DemoRoom" / "script_authoring" / "examples" / "demo_safe_script.csv"
    INITIAL_POSES_CSV = SITEMAP_DIR / "DemoRoom" / "script_authoring" / "examples" / "demo_initial_poses.csv"
    SITE_DIR = SITEMAP_DIR / "DemoRoom"
    COMMON_DIR = SITEMAP_DIR / "CommonCheckers"
    REPORT_JSON = SITEMAP_DIR / "validation_report.json"

    report = validate_script(
        script_csv=SCRIPT_CSV,
        initial_poses_csv=INITIAL_POSES_CSV,
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
