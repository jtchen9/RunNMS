from __future__ import annotations

import json
import sys
from pathlib import Path


# Auto-detect:
#   <workspace>/sitemap/DemoRoom/script_authoring/examples/run_p4_regression_examples.py
SITEMAP_DIR = Path(__file__).resolve().parents[3]
COMMON_DIR = SITEMAP_DIR / "CommonCheckers"
SITE_DIR = SITEMAP_DIR / "DemoRoom"

if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from checker.checker_runner import validate_script  # type: ignore # noqa: E402


TESTS = [
    {
        "name": "safe normal move",
        "script": SITE_DIR / "script_authoring" / "examples" / "demo_safe_script.csv",
        "initial_poses": SITE_DIR / "script_authoring" / "examples" / "demo_initial_poses.csv",
        "expect_ok": True,
        "expected_error_codes": [],
    },
    {
        "name": "invalid macro args",
        "script": SITE_DIR / "script_authoring" / "examples" / "demo_p4_invalid_macro_args.csv",
        "initial_poses": SITE_DIR / "script_authoring" / "examples" / "demo_p4_macro_start_initial_poses.csv",
        "expect_ok": False,
        "expected_error_codes": ["MACRO_ARGS_NOT_ALLOWED"],
    },
    {
        "name": "invalid macro wrong start",
        "script": SITE_DIR / "script_authoring" / "examples" / "demo_p4_invalid_macro_wrong_start.csv",
        "initial_poses": SITE_DIR / "script_authoring" / "examples" / "demo_initial_poses.csv",
        "expect_ok": False,
        "expected_error_codes": ["MACRO_START_POSE_MISMATCH"],
    },
    {
        "name": "invalid move crosses bump",
        "script": SITE_DIR / "script_authoring" / "examples" / "demo_p4_invalid_move_crosses_bump.csv",
        "initial_poses": SITE_DIR / "script_authoring" / "examples" / "demo_initial_poses.csv",
        "expect_ok": False,
        "expected_error_codes": ["MOVE_CROSSES_BUMP_GUARD_ZONE"],
    },
    {
        "name": "valid macro in2out",
        "script": SITE_DIR / "script_authoring" / "examples" / "demo_p4_valid_macro_in2out.csv",
        "initial_poses": SITE_DIR / "script_authoring" / "examples" / "demo_p4_macro_start_initial_poses.csv",
        "expect_ok": True,
        "expected_error_codes": [],
    },
]


def run_tests() -> None:
    all_passed = True

    for test in TESTS:
        report = validate_script(
            script_csv=test["script"],
            initial_poses_csv=test["initial_poses"],
            site_dir=SITE_DIR,
            common_dir=COMMON_DIR,
            report_json=SITE_DIR / "script_authoring" / "examples" / f"report_{test['name'].replace(' ', '_')}.json",
        )
        codes = [issue.get("code") for issue in report.get("issues", [])]
        ok_matches = report.get("ok") == test["expect_ok"]
        codes_match = all(code in codes for code in test["expected_error_codes"])
        passed = ok_matches and codes_match
        all_passed = all_passed and passed

        print("=" * 72)
        print(test["name"])
        print(f"expected ok={test['expect_ok']} actual ok={report.get('ok')}")
        print(f"error codes: {codes}")
        print("RESULT:", "PASS" if passed else "FAIL")

        if not passed:
            print(json.dumps(report, indent=2, ensure_ascii=False))

    print("=" * 72)
    print("ALL P4 REGRESSION EXAMPLES:", "PASS" if all_passed else "FAIL")


if __name__ == "__main__":
    run_tests()
