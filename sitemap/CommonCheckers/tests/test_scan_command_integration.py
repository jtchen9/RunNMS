from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


COMMON_DIR = Path(__file__).resolve().parents[1]
SITE_DIR = COMMON_DIR.parent / "DemoRoom"

if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from checker.checker_runner import validate_script
from checker.initialization_rules import (
    check_first_mobility_command,
    check_initial_poses_exist,
)
from checker.movement_rules import check_max_single_mobility_move_distance
from checker.script_model import InitialPose, ScriptRow
from checker.timeline_rules import (
    check_first_move_after_report_location_spacing,
    check_global_moving_command_spacing,
)
from checker.vocabulary_rules import check_vocabulary


class ScanCommandIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = json.loads(
            (COMMON_DIR / "config" / "script_policy.json").read_text(
                encoding="utf-8"
            )
        )

    def test_checker_runner_accepts_all_three_scan_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            script_path = temp_path / "experiment_script.csv"
            poses_path = temp_path / "initial_poses.csv"

            script_path.write_text(
                "scanner,t_offset_sec,category,action,args_json\n"
                'twin-scout-delta,0,scan,scan.start,"{}"\n'
                'twin-scout-delta,10,scan,scan.once,"{}"\n'
                'twin-scout-delta,20,scan,scan.stop,"{}"\n',
                encoding="utf-8",
            )
            poses_path.write_text(
                "scanner,intended_x_m,intended_y_m,intended_heading_deg,"
                "position_tolerance_m,heading_tolerance_deg\n"
                "twin-scout-delta,3.9,4.4,270,0.2,10\n",
                encoding="utf-8",
            )

            report = validate_script(
                script_csv=script_path,
                initial_poses_csv=poses_path,
                common_dir=COMMON_DIR,
                site_dir=SITE_DIR,
            )

        self.assertTrue(report["ok"], report["issues"])
        self.assertEqual(report["error_count"], 0)
        self.assertEqual(report["warning_count"], 0)
        self.assertEqual(report["issues"], [])

    def test_checker_runner_rejects_scan_target_not_in_initial_poses(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            script_path = temp_path / "experiment_script.csv"
            poses_path = temp_path / "initial_poses.csv"

            script_path.write_text(
                "scanner,t_offset_sec,category,action,args_json\n"
                'unknown-robot,0,scan,scan.once,"{}"\n',
                encoding="utf-8",
            )
            poses_path.write_text(
                "scanner,intended_x_m,intended_y_m,intended_heading_deg,"
                "position_tolerance_m,heading_tolerance_deg\n"
                "twin-scout-delta,3.9,4.4,270,0.2,10\n",
                encoding="utf-8",
            )

            report = validate_script(
                script_csv=script_path,
                initial_poses_csv=poses_path,
                common_dir=COMMON_DIR,
                site_dir=SITE_DIR,
            )

        self.assertFalse(report["ok"])
        self.assertIn(
            "SCAN_COMMAND_BAD_TARGET",
            [str(issue.get("code")) for issue in report["issues"]],
        )

    def test_checker_runner_rejects_unknown_scan_action(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            script_path = temp_path / "experiment_script.csv"
            poses_path = temp_path / "initial_poses.csv"

            script_path.write_text(
                "scanner,t_offset_sec,category,action,args_json\n"
                'twin-scout-delta,0,scan,scan.invalid,"{}"\n',
                encoding="utf-8",
            )
            poses_path.write_text(
                "scanner,intended_x_m,intended_y_m,intended_heading_deg,"
                "position_tolerance_m,heading_tolerance_deg\n"
                "twin-scout-delta,3.9,4.4,270,0.2,10\n",
                encoding="utf-8",
            )

            report = validate_script(
                script_csv=script_path,
                initial_poses_csv=poses_path,
                common_dir=COMMON_DIR,
                site_dir=SITE_DIR,
            )

        self.assertFalse(report["ok"])
        self.assertIn(
            "UNKNOWN_ACTION",
            [str(issue.get("code")) for issue in report["issues"]],
        )

    def test_scan_rows_do_not_enter_mobility_rules(self) -> None:
        scanner = "twin-scout-delta"
        rows = [
            ScriptRow(2, scanner, 0, "scan", "scan.start", {}),
            ScriptRow(
                3,
                scanner,
                0,
                "mobility",
                "mobility.report.location",
                {},
            ),
            ScriptRow(4, scanner, 30, "scan", "scan.once", {}),
            ScriptRow(
                5,
                scanner,
                60,
                "mobility",
                "mobility.move",
                {"x_m": 1.0, "y_m": 0.0},
            ),
            ScriptRow(6, scanner, 120, "scan", "scan.stop", {}),
            ScriptRow(
                7,
                scanner,
                240,
                "mobility",
                "mobility.move",
                {"x_m": 2.0, "y_m": 0.0},
            ),
        ]
        poses = {
            scanner: InitialPose(
                row_number=2,
                scanner=scanner,
                x_m=0.0,
                y_m=0.0,
                heading_deg=0.0,
                position_tolerance_m=0.2,
                heading_tolerance_deg=10.0,
            )
        }

        self.assertEqual(check_vocabulary(rows, self.policy), [])
        self.assertEqual(check_first_mobility_command(rows, self.policy), [])
        self.assertEqual(check_initial_poses_exist(rows, poses), [])
        self.assertEqual(
            check_first_move_after_report_location_spacing(rows, self.policy),
            [],
        )
        self.assertEqual(
            check_global_moving_command_spacing(rows, self.policy),
            [],
        )
        self.assertEqual(
            check_max_single_mobility_move_distance(
                rows,
                poses,
                self.policy,
            ),
            [],
        )

    def test_missing_mobility_pose_points_to_first_affected_command(self) -> None:
        scanner = "twin-scout-delta"
        rows = [
            ScriptRow(
                2,
                scanner,
                60,
                "mobility",
                "mobility.move",
                {"x_m": 4.0, "y_m": 4.0},
            ),
            ScriptRow(
                3,
                scanner,
                0,
                "mobility",
                "mobility.report.location",
                {},
            ),
        ]

        issues = check_initial_poses_exist(rows, {})

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["code"], "MISSING_INITIAL_POSE")
        self.assertEqual(issues[0]["row_number"], 3)
        self.assertEqual(issues[0]["scanner"], scanner)
        self.assertEqual(issues[0]["action"], "mobility.report.location")


if __name__ == "__main__":
    unittest.main(verbosity=2)
