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

from checker.argument_rules import check_command_arguments
from checker.checker_runner import validate_script
from checker.script_model import ScriptRow


class CommandArgumentRuleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = json.loads(
            (COMMON_DIR / "config" / "script_policy.json").read_text(
                encoding="utf-8"
            )
        )

    def _row(
        self,
        action: str,
        args: dict,
        *,
        category: str = "mobility",
    ) -> ScriptRow:
        return ScriptRow(
            row_number=2,
            scanner="twin-scout-delta",
            t_offset_sec=0,
            category=category,
            action=action,
            args=args,
        )

    def _codes(self, row: ScriptRow) -> list[str]:
        return [
            str(issue["code"])
            for issue in check_command_arguments([row], self.policy)
        ]

    def test_every_admitted_command_has_one_argument_rule(self) -> None:
        allowed = {
            (category, action)
            for category, actions in self.policy[
                "allowed_actions_by_category"
            ].items()
            for action in actions
        }
        ruled = {
            (category, action)
            for category, actions in self.policy[
                "argument_rules_by_category"
            ].items()
            for action in actions
        }
        self.assertEqual(ruled, allowed)

    def test_all_no_argument_commands_accept_empty_object(self) -> None:
        commands = [
            ("mobility", "mobility.report.location"),
            ("mobility", "mobility.in2out"),
            ("mobility", "mobility.out2in"),
            ("scan", "scan.start"),
            ("scan", "scan.stop"),
            ("scan", "scan.once"),
        ]

        for category, action in commands:
            with self.subTest(action=action):
                row = self._row(action, {}, category=category)
                self.assertEqual(check_command_arguments([row], self.policy), [])

    def test_all_no_argument_commands_reject_keys(self) -> None:
        commands = [
            ("mobility", "mobility.report.location"),
            ("mobility", "mobility.in2out"),
            ("mobility", "mobility.out2in"),
            ("scan", "scan.start"),
            ("scan", "scan.stop"),
            ("scan", "scan.once"),
        ]

        for category, action in commands:
            with self.subTest(action=action):
                row = self._row(
                    action,
                    {"unexpected": 1},
                    category=category,
                )
                self.assertEqual(
                    self._codes(row),
                    ["COMMAND_ARGS_NOT_ALLOWED"],
                )

    def test_mobility_move_accepts_required_and_optional_fields(self) -> None:
        valid_args = [
            {"x_m": 1.4, "y_m": 0.3},
            {"x_m": 1.4, "y_m": 0.3, "heading_deg": 0},
            {"x_m": 10.099, "y_m": 11.099, "heading_deg": 359.999},
        ]

        for args in valid_args:
            with self.subTest(args=args):
                row = self._row("mobility.move", args)
                self.assertEqual(check_command_arguments([row], self.policy), [])

    def test_mobility_move_rejects_missing_required_fields(self) -> None:
        self.assertEqual(
            self._codes(self._row("mobility.move", {})),
            ["COMMAND_ARGS_MISSING_REQUIRED"],
        )
        self.assertEqual(
            self._codes(self._row("mobility.move", {"x_m": 2.0})),
            ["COMMAND_ARGS_MISSING_REQUIRED"],
        )
        self.assertEqual(
            self._codes(self._row("mobility.move", {"y_m": 2.0})),
            ["COMMAND_ARGS_MISSING_REQUIRED"],
        )

    def test_mobility_move_rejects_unknown_fields(self) -> None:
        row = self._row(
            "mobility.move",
            {"x_m": 2.0, "y_m": 2.0, "dx_m": 1.0},
        )
        self.assertEqual(
            self._codes(row),
            ["COMMAND_ARGS_UNKNOWN_FIELDS"],
        )

    def test_mobility_move_rejects_non_numeric_values(self) -> None:
        invalid_args = [
            {"x_m": "2.0", "y_m": 2.0},
            {"x_m": True, "y_m": 2.0},
            {"x_m": 2.0, "y_m": "2.0"},
            {"x_m": 2.0, "y_m": False},
            {"x_m": 2.0, "y_m": 2.0, "heading_deg": "90"},
            {"x_m": float("nan"), "y_m": 2.0},
            {"x_m": float("inf"), "y_m": 2.0},
        ]

        for args in invalid_args:
            with self.subTest(args=args):
                row = self._row("mobility.move", args)
                self.assertEqual(
                    self._codes(row),
                    ["COMMAND_ARG_BAD_TYPE"],
                )

    def test_mobility_move_enforces_half_open_ranges(self) -> None:
        invalid_args = [
            {"x_m": 1.399, "y_m": 1.0},
            {"x_m": 10.1, "y_m": 1.0},
            {"x_m": 2.0, "y_m": 0.299},
            {"x_m": 2.0, "y_m": 11.1},
            {"x_m": 2.0, "y_m": 1.0, "heading_deg": -0.001},
            {"x_m": 2.0, "y_m": 1.0, "heading_deg": 360.0},
        ]

        for args in invalid_args:
            with self.subTest(args=args):
                row = self._row("mobility.move", args)
                self.assertEqual(
                    self._codes(row),
                    ["COMMAND_ARG_OUT_OF_RANGE"],
                )

    def test_checker_runner_invokes_argument_rules(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            script_path = temp_path / "experiment_script.csv"
            poses_path = temp_path / "initial_poses.csv"

            script_path.write_text(
                "scanner,t_offset_sec,category,action,args_json\n"
                'twin-scout-delta,0,scan,scan.once,"{""force"":true}"\n',
                encoding="utf-8",
            )
            poses_path.write_text(
                "scanner,intended_x_m,intended_y_m,intended_heading_deg,"
                "position_tolerance_m,heading_tolerance_deg\n",
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
            "COMMAND_ARGS_NOT_ALLOWED",
            [str(issue.get("code")) for issue in report["issues"]],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
