from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path


COMMON_DIR = Path(__file__).resolve().parents[1]
SITE_DIR = COMMON_DIR.parent / "DemoRoom"
if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from checker.checker_runner import validate_script
from checker.script_model import load_script_csv


class ScriptStructureRuleTests(unittest.TestCase):
    def _write(self, directory: Path, name: str, text: str) -> Path:
        path = directory / name
        path.write_text(text, encoding="utf-8")
        return path

    def _poses(self, directory: Path) -> Path:
        return self._write(
            directory,
            "initial_poses.csv",
            "scanner,intended_x_m,intended_y_m,intended_heading_deg,"
            "position_tolerance_m,heading_tolerance_deg\n",
        )

    def test_args_json_column_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            script = self._write(
                Path(temp_dir),
                "missing_args_json.csv",
                "scanner,t_offset_sec,category,action\n"
                "twin-scout-delta,0,scan,scan.once\n",
            )
            rows, issues = load_script_csv(script)

        self.assertEqual(rows, [])
        self.assertEqual([x["code"] for x in issues], ["SCRIPT_CSV_MISSING_COLUMNS"])
        self.assertEqual(issues[0]["missing_columns"], ["args_json"])
        self.assertIn("args_json", issues[0]["message"])

    def test_header_only_script_reports_empty_script(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            script = self._write(
                Path(temp_dir),
                "empty.csv",
                "scanner,t_offset_sec,category,action,args_json\n",
            )
            rows, issues = load_script_csv(script)

        self.assertEqual(rows, [])
        self.assertEqual([x["code"] for x in issues], ["EMPTY_SCRIPT"])

    def test_invalid_data_row_is_not_mislabeled_as_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            script = self._write(
                Path(temp_dir),
                "bad_row.csv",
                "scanner,t_offset_sec,category,action,args_json\n"
                'twin-scout-delta,not-an-int,scan,scan.once,"{}"\n',
            )
            rows, issues = load_script_csv(script)

        self.assertEqual(rows, [])
        self.assertEqual([x["code"] for x in issues], ["BAD_T_OFFSET_SEC"])

    def test_validate_script_rejects_empty_script_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            script = self._write(
                directory,
                "empty.csv",
                "scanner,t_offset_sec,category,action,args_json\n",
            )
            report = validate_script(
                script_csv=script,
                initial_poses_csv=self._poses(directory),
                common_dir=COMMON_DIR,
                site_dir=SITE_DIR,
            )

        self.assertFalse(report["ok"])
        self.assertIn("EMPTY_SCRIPT", [x["code"] for x in report["issues"]])

    def test_validate_script_rejects_missing_args_json_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            script = self._write(
                directory,
                "missing_args_json.csv",
                "scanner,t_offset_sec,category,action\n"
                "twin-scout-delta,0,scan,scan.once\n",
            )
            report = validate_script(
                script_csv=script,
                initial_poses_csv=self._poses(directory),
                common_dir=COMMON_DIR,
                site_dir=SITE_DIR,
            )

        self.assertFalse(report["ok"])
        self.assertIn(
            "SCRIPT_CSV_MISSING_COLUMNS",
            [x["code"] for x in report["issues"]],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
