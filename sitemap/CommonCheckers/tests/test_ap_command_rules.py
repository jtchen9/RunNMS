from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


COMMON_DIR = Path(__file__).resolve().parents[1]
if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from checker.argument_rules import check_command_arguments
from checker.script_model import ScriptRow
from checker.vocabulary_rules import check_device_targets


class APCommandRuleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = json.loads(
            (COMMON_DIR / "config" / "script_policy.json").read_text(encoding="utf-8")
        )

    def _row(self, action: str, args: dict, scanner: str = "AP1") -> ScriptRow:
        return ScriptRow(2, scanner, 0, "ap", action, args)

    def _codes(self, row: ScriptRow) -> list[str]:
        return [x["code"] for x in check_command_arguments([row], self.policy)]

    def test_no_argument_ap_commands(self) -> None:
        for action in (
            "ap.association.get",
            "ap.traffic.enable",
            "ap.traffic.disable",
        ):
            with self.subTest(action=action):
                self.assertEqual(self._codes(self._row(action, {})), [])
                self.assertEqual(
                    self._codes(self._row(action, {"unexpected": 1})),
                    ["COMMAND_ARGS_NOT_ALLOWED"],
                )

    def test_disassociate_contract(self) -> None:
        valid = {"sta_mac": "aa:BB:01:23:45:fF", "time_period": 10}
        self.assertEqual(self._codes(self._row("ap.sta.disassociate", valid)), [])

        invalid_cases = [
            ({"time_period": 10}, "COMMAND_ARGS_MISSING_REQUIRED"),
            ({"sta_mac": "aa:bb:cc:dd:ee:ff"}, "COMMAND_ARGS_MISSING_REQUIRED"),
            ({"sta_mac": "aa:bb:cc:dd:ee:ff", "time_period": 0}, "COMMAND_ARG_OUT_OF_RANGE"),
            ({"sta_mac": "aa:bb:cc:dd:ee:ff", "time_period": 300}, "COMMAND_ARG_OUT_OF_RANGE"),
            ({"sta_mac": "aa:bb:cc:dd:ee:ff", "time_period": 10.5}, "COMMAND_ARG_BAD_TYPE"),
            ({"sta_mac": "aa-bb-cc-dd-ee-ff", "time_period": 10}, "COMMAND_ARG_BAD_FORMAT"),
        ]
        for args, expected in invalid_cases:
            with self.subTest(args=args):
                self.assertIn(expected, self._codes(self._row("ap.sta.disassociate", args)))

    def test_txpower_contract(self) -> None:
        valid_cases = [
            {"txpower": 0},
            {"txpower": 30},
            {"txpower": 20, "sta_mac": "00:11:22:aa:BB:ff"},
        ]
        for args in valid_cases:
            with self.subTest(args=args):
                self.assertEqual(self._codes(self._row("ap.txpower.set", args)), [])

        invalid_cases = [
            ({}, "COMMAND_ARGS_MISSING_REQUIRED"),
            ({"txpower": -1}, "COMMAND_ARG_OUT_OF_RANGE"),
            ({"txpower": 31}, "COMMAND_ARG_OUT_OF_RANGE"),
            ({"txpower": 20.5}, "COMMAND_ARG_BAD_TYPE"),
            ({"txpower": "20"}, "COMMAND_ARG_BAD_TYPE"),
            ({"txpower": 20, "sta_mac": "not-a-mac"}, "COMMAND_ARG_BAD_FORMAT"),
        ]
        for args, expected in invalid_cases:
            with self.subTest(args=args):
                self.assertIn(expected, self._codes(self._row("ap.txpower.set", args)))

    def test_ap_targets_are_roster_bound(self) -> None:
        aps = {"AP1", "AP2", "AP3", "AP4", "AP5", "AP6"}
        robots = {"twin-scout-alpha"}
        self.assertEqual(
            check_device_targets(
                [self._row("ap.association.get", {})],
                robots,
                aps,
            ),
            [],
        )
        issues = check_device_targets(
            [self._row("ap.association.get", {}, scanner="twin-scout-alpha")],
            robots,
            aps,
        )
        self.assertEqual([x["code"] for x in issues], ["AP_COMMAND_BAD_TARGET"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
