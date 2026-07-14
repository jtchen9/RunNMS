from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from .initialization_rules import check_first_mobility_command, check_initial_poses_exist
from .script_model import load_initial_poses_csv, load_script_csv
from .timeline_rules import check_global_moving_command_spacing
from .validation_report import make_report, write_report
from .vocabulary_rules import check_vocabulary


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

    rows, issues = load_script_csv(script_csv)
    initial_poses, pose_issues = load_initial_poses_csv(initial_poses_csv)
    issues.extend(pose_issues)

    issues.extend(check_vocabulary(rows, policy))
    issues.extend(check_first_mobility_command(rows, policy))
    issues.extend(check_initial_poses_exist(rows, initial_poses))
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
    # Paths may be absolute, or relative to the current VS Code workspace.
    # ---------------------------------------------------------
    SCRIPT_CSV = Path(r"sitemap\DemoRoom\script_authoring\examples\demo_safe_script.csv")
    INITIAL_POSES_CSV = Path(r"sitemap\DemoRoom\script_authoring\examples\demo_initial_poses.csv")
    SITE_DIR = Path(r"sitemap\DemoRoom")
    COMMON_DIR = Path(r"sitemap\CommonCheckers")
    REPORT_JSON = Path(r"validation_report.json")

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
