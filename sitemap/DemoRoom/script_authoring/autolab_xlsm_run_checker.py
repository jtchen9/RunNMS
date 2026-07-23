from __future__ import annotations

import argparse
import csv
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run AutoLab CommonCheckers from XLSM-generated CSV files.")
    p.add_argument("--script_csv", required=True)
    p.add_argument("--initial_poses_csv", required=True)
    p.add_argument("--site_dir", required=True)
    p.add_argument("--common_dir", required=True)
    p.add_argument("--report_json", required=True)
    p.add_argument("--feedback_csv", required=False)
    return p.parse_args()


def _write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


def _normalize_report(report: Any) -> dict:
    if isinstance(report, dict):
        return report
    return {
        "ok": False,
        "status": "fail",
        "error_count": 1,
        "warning_count": 0,
        "issues": [{
            "level": "error",
            "code": "CHECKER_RETURNED_NON_DICT",
            "row_number": None,
            "message": f"validate_script returned {type(report).__name__}, not dict.",
            "suggestion": "Check CommonCheckers checker_runner.validate_script return contract.",
        }],
        "raw_return_repr": repr(report),
    }


def _status_for_levels(levels: list[str]) -> str:
    norm = [str(x).lower() for x in levels]
    if any(x == "error" for x in norm):
        return "ERROR"
    if any(x == "warning" for x in norm):
        return "WARNING"
    return "OK"


def _join_unique(values: list[Any]) -> str:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = "" if value is None else str(value)
        if not text:
            continue
        if text not in seen:
            out.append(text)
            seen.add(text)
    return " | ".join(out)


def _write_feedback_csv(path: Path, report: dict) -> None:
    """
    Convert CommonCheckers JSON report into row feedback that Excel VBA can import.

    CSV row_number includes the header row, so:
        cmd_row_id = row_number - 1
    Global/non-row issues use cmd_row_id = 0 and remain visible in ValidationReport.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    grouped: dict[int, dict[str, Any]] = {}

    for issue in report.get("issues", []) or []:
        if not isinstance(issue, dict):
            continue

        row_number = issue.get("row_number")
        try:
            row_number_int = int(row_number)
        except (TypeError, ValueError):
            row_number_int = 1

        cmd_row_id = max(row_number_int - 1, 0)
        item = grouped.setdefault(cmd_row_id, {
            "cmd_row_id": cmd_row_id,
            "row_number": row_number_int if row_number_int > 1 else "",
            "levels": [],
            "codes": [],
            "messages": [],
            "suggestions": [],
            "scanner": issue.get("scanner", ""),
            "action": issue.get("action", ""),
            "other_scanner": issue.get("other_scanner", ""),
            "distance_m": issue.get("distance_m", ""),
        })

        item["levels"].append(issue.get("level", "error"))
        item["codes"].append(issue.get("code", ""))
        item["messages"].append(issue.get("message", ""))
        item["suggestions"].append(issue.get("suggestion", ""))

        if not item.get("scanner") and issue.get("scanner"):
            item["scanner"] = issue.get("scanner")
        if not item.get("action") and issue.get("action"):
            item["action"] = issue.get("action")
        if not item.get("other_scanner") and issue.get("other_scanner"):
            item["other_scanner"] = issue.get("other_scanner")
        if not item.get("distance_m") and issue.get("distance_m") is not None:
            item["distance_m"] = issue.get("distance_m")

    rows = []
    for cmd_row_id in sorted(grouped):
        item = grouped[cmd_row_id]
        rows.append({
            "cmd_row_id": item["cmd_row_id"],
            "row_number": item["row_number"],
            "status": _status_for_levels(item["levels"]),
            "level": _join_unique(item["levels"]),
            "issue_code": _join_unique(item["codes"]),
            "message": _join_unique(item["messages"]),
            "suggestion": _join_unique(item["suggestions"]),
            "scanner": item.get("scanner", ""),
            "action": item.get("action", ""),
            "other_scanner": item.get("other_scanner", ""),
            "distance_m": item.get("distance_m", ""),
        })

    with path.open("w", encoding="utf-8-sig", newline="") as f:
        fieldnames = [
            "cmd_row_id",
            "row_number",
            "status",
            "level",
            "issue_code",
            "message",
            "suggestion",
            "scanner",
            "action",
            "other_scanner",
            "distance_m",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _run_checker(args: argparse.Namespace) -> tuple[int, dict]:
    common_dir = Path(args.common_dir).resolve()
    if not common_dir.exists():
        raise FileNotFoundError(f"common_dir not found: {common_dir}")

    script_csv = Path(args.script_csv)
    initial_poses_csv = Path(args.initial_poses_csv)
    site_dir = Path(args.site_dir)
    report_path = Path(args.report_json)

    for label, path in [
        ("script_csv", script_csv),
        ("initial_poses_csv", initial_poses_csv),
        ("site_dir", site_dir),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} not found: {path}")

    sys.path.insert(0, str(common_dir))

    from checker.checker_runner import validate_script  # type: ignore

    raw_report = validate_script(
        script_csv=script_csv,
        initial_poses_csv=initial_poses_csv,
        site_dir=site_dir,
        common_dir=common_dir,
        report_json=report_path,
    )

    report = _normalize_report(raw_report)
    report.setdefault("generated_by", "autolab_xlsm_run_checker.py")
    report.setdefault("generated_at", datetime.now().isoformat(timespec="seconds"))

    _write_report(report_path, report)

    if args.feedback_csv:
        _write_feedback_csv(Path(args.feedback_csv), report)

    return (0 if report.get("ok") else 1), report


def main() -> int:
    args = _parse_args()

    try:
        exit_code, report = _run_checker(args)
        print(json.dumps(report, indent=2, ensure_ascii=False))

        if exit_code == 0:
            print("\nCHECK RESULT: PASS")
            print(f"Report written to: {args.report_json}")
            if args.feedback_csv:
                print(f"Feedback written to: {args.feedback_csv}")
            return 0

        print("\nCHECK RESULT: FAIL")
        print(f"Errors: {report.get('error_count', 0)}")
        print(f"Warnings: {report.get('warning_count', 0)}")
        print(f"Report written to: {args.report_json}")
        if args.feedback_csv:
            print(f"Feedback written to: {args.feedback_csv}")
        return 1

    except Exception as e:
        error_report = {
            "ok": False,
            "status": "fail",
            "error_count": 1,
            "warning_count": 0,
            "issues": [{
                "level": "error",
                "code": "XLSM_CHECKER_RUNNER_ERROR",
                "row_number": None,
                "message": f"{type(e).__name__}: {e}",
                "suggestion": "Open checker_stdout.txt and verify Python, NMS root, CommonCheckers path, site_dir, and generated CSV files.",
            }],
            "generated_by": "autolab_xlsm_run_checker.py",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "traceback": traceback.format_exc(),
        }

        try:
            _write_report(Path(args.report_json), error_report)
            if args.feedback_csv:
                _write_feedback_csv(Path(args.feedback_csv), error_report)
            print(json.dumps(error_report, indent=2, ensure_ascii=False))
        except Exception:
            print("Could not write error report:", file=sys.stderr)
            traceback.print_exc()

        print(f"RUNNER ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
