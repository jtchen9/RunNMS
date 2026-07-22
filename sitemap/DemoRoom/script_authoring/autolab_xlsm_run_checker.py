from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run AutoLab CommonCheckers from XLSM-generated CSV files.")
    p.add_argument("--script_csv", required=True)
    p.add_argument("--initial_poses_csv", required=True)
    p.add_argument("--site_dir", required=True)
    p.add_argument("--common_dir", required=True)
    p.add_argument("--report_json", required=True)
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    common_dir = Path(args.common_dir).resolve()
    if not common_dir.exists():
        raise FileNotFoundError(f"common_dir not found: {common_dir}")

    # CommonCheckers has package folder "checker" under common_dir.
    sys.path.insert(0, str(common_dir))

    from checker.checker_runner import validate_script  # type: ignore

    report = validate_script(
        script_csv=Path(args.script_csv),
        initial_poses_csv=Path(args.initial_poses_csv),
        site_dir=Path(args.site_dir),
        common_dir=common_dir,
        report_json=Path(args.report_json),
    )

    print(json.dumps(report, indent=2, ensure_ascii=False))

    if report.get("ok"):
        print("\nCHECK RESULT: PASS")
        return 0

    print("\nCHECK RESULT: FAIL")
    print(f"Errors: {report.get('error_count', 0)}")
    print(f"Warnings: {report.get('warning_count', 0)}")
    print(f"Report written to: {args.report_json}")
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"RUNNER ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        raise SystemExit(2)
