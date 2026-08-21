from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path


COMMON_DIR = Path(__file__).resolve().parents[1]
SITE_DIR = COMMON_DIR.parent / "DemoRoom"
if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from checker.checker_runner import validate_script  # noqa: E402


class ReservedLaunchLocationTests(unittest.TestCase):
    def _poses(self, directory: Path, x: float = 9.06, y: float = 3.3) -> Path:
        path = directory / "initial_poses.csv"
        path.write_text(
            "scanner,intended_x_m,intended_y_m,intended_heading_deg,position_tolerance_m,heading_tolerance_deg\n"
            f"twin-scout-charlie,{x},{y},270,0.2,10\n",
            encoding="utf-8",
        )
        return path

    def _script(self, directory: Path, rows: list[tuple[int, str, str]]) -> Path:
        path = directory / "source.csv"
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["scanner", "t_offset_sec", "category", "action", "args_json"])
            for offset, action, args_json in rows:
                category = "scan" if action.startswith("scan.") else "mobility"
                writer.writerow(["twin-scout-charlie", offset, category, action, args_json])
        return path

    def _validate(self, directory: Path, rows: list[tuple[int, str, str]], *, normalized=False):
        output = directory / "experiment_script.csv" if normalized else None
        report = validate_script(
            script_csv=self._script(directory, rows),
            initial_poses_csv=self._poses(directory),
            common_dir=COMMON_DIR,
            site_dir=SITE_DIR,
            normalized_script_csv=output,
        )
        return report, output

    @staticmethod
    def _codes(report: dict) -> list[str]:
        return [str(issue.get("code")) for issue in report.get("issues", [])]

    def test_valid_constant_is_resolved_then_full_validation_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            report, output = self._validate(directory, [
                (0, "mobility.report.location", "{}"),
                (180, "mobility.move", '{"x_m":"IN2OUT","y_m":"IN2OUT"}'),
                (360, "mobility.in2out", "{}"),
            ], normalized=True)
            self.assertTrue(report["ok"], report)
            self.assertEqual(report["resolved_launch_constant_count"], 1)
            self.assertTrue(output and output.exists())
            with output.open(encoding="utf-8-sig") as stream:
                normalized_rows = list(csv.DictReader(stream))
            move_args = json.loads(normalized_rows[1]["args_json"])
            self.assertEqual(move_args, {"x_m": 9.06, "y_m": 4.299})

            second_report = validate_script(
                script_csv=output,
                initial_poses_csv=self._poses(directory),
                common_dir=COMMON_DIR,
                site_dir=SITE_DIR,
            )
            self.assertTrue(second_report["ok"], second_report)

    def test_macro_without_preceding_staging_move_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            report, _ = self._validate(Path(temp), [
                (0, "mobility.report.location", "{}"),
                (180, "mobility.in2out", "{}"),
            ])
        self.assertIn("MACRO_REQUIRES_PRECEDING_RESERVED_MOVE", self._codes(report))

    def test_intervening_mobility_command_breaks_pair(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            report, _ = self._validate(Path(temp), [
                (0, "mobility.report.location", "{}"),
                (180, "mobility.move", '{"x_m":"IN2OUT","y_m":"IN2OUT"}'),
                (360, "mobility.report.location", "{}"),
                (540, "mobility.in2out", "{}"),
            ])
        codes = self._codes(report)
        self.assertIn("RESERVED_LAUNCH_NOT_FOLLOWED_BY_MACRO", codes)
        self.assertIn("MACRO_REQUIRES_PRECEDING_RESERVED_MOVE", codes)

    def test_non_mobility_command_between_pair_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            report, _ = self._validate(Path(temp), [
                (0, "mobility.report.location", "{}"),
                (180, "mobility.move", '{"x_m":"IN2OUT","y_m":"IN2OUT"}'),
                (240, "scan.once", "{}"),
                (360, "mobility.in2out", "{}"),
            ])
        self.assertTrue(report["ok"], report)

    def test_mixed_reserved_pair_is_rejected_and_no_output_is_left(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            output = directory / "experiment_script.csv"
            output.write_text("stale", encoding="utf-8")
            report, _ = self._validate(directory, [
                (0, "mobility.report.location", "{}"),
                (180, "mobility.move", '{"x_m":"IN2OUT","y_m":4.3}'),
                (360, "mobility.in2out", "{}"),
            ], normalized=True)
            self.assertIn("RESERVED_LAUNCH_PAIR_REQUIRED", self._codes(report))
            self.assertFalse(output.exists())

    def test_failed_in_place_normalization_preserves_symbolic_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            script = self._script(directory, [
                (0, "mobility.report.location", "{}"),
                (180, "mobility.move", '{"x_m":"IN2OUT","y_m":4.3}'),
                (360, "mobility.in2out", "{}"),
            ])
            before = script.read_text(encoding="utf-8")
            report = validate_script(
                script_csv=script,
                initial_poses_csv=self._poses(directory),
                common_dir=COMMON_DIR,
                site_dir=SITE_DIR,
                normalized_script_csv=script,
            )
            self.assertFalse(report["ok"])
            self.assertEqual(script.read_text(encoding="utf-8"), before)

    def test_reserved_move_must_be_consumed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            report, _ = self._validate(Path(temp), [
                (0, "mobility.report.location", "{}"),
                (180, "mobility.move", '{"x_m":"OUT2IN","y_m":"OUT2IN"}'),
            ])
        self.assertIn("RESERVED_LAUNCH_NOT_FOLLOWED_BY_MACRO", self._codes(report))

    def test_resolved_staging_move_still_obeys_normal_distance_rule(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            self._poses(directory, x=9.06, y=0.3)
            script = self._script(directory, [
                (0, "mobility.report.location", "{}"),
                (180, "mobility.move", '{"x_m":"IN2OUT","y_m":"IN2OUT"}'),
                (360, "mobility.in2out", "{}"),
            ])
            report = validate_script(
                script_csv=script,
                initial_poses_csv=directory / "initial_poses.csv",
                common_dir=COMMON_DIR,
                site_dir=SITE_DIR,
            )
        self.assertIn("MOBILITY_MOVE_DISTANCE_TOO_LONG", self._codes(report))


if __name__ == "__main__":
    unittest.main(verbosity=2)
