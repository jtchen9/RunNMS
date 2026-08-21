from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


COMMON_DIR = Path(__file__).resolve().parents[1]
SITE_DIR = COMMON_DIR.parent / "DemoRoom"
if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from checker.static_safety_core import (  # noqa: E402
    macro_planned_pose,
    macro_planned_pose_from_current,
    macro_start_pose_issues,
    ramp_restriction_movement_issues,
)


class RampMacroPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = json.loads(
            (SITE_DIR / "script_authoring" / "config" / "macro_policy.json")
            .read_text(encoding="utf-8")
        )
        cls.in2out = cls.policy["macros"]["mobility.in2out"]
        cls.out2in = cls.policy["macros"]["mobility.out2in"]

    def test_authoritative_geometry_and_profiles(self) -> None:
        self.assertEqual((self.in2out["start_x_m"], self.in2out["start_y_m"]), (9.06, 4.3))
        self.assertEqual((self.out2in["start_x_m"], self.out2in["start_y_m"]), (9.06, 6.1))
        self.assertEqual(self.in2out["move_profile"], "bump_crossing_up")
        self.assertEqual(self.out2in["move_profile"], "bump_crossing_down")
        self.assertEqual(self.in2out["launch_constant"], "IN2OUT")
        self.assertEqual(self.out2in["launch_constant"], "OUT2IN")
        self.assertEqual(self.in2out["distance_m"], 2.0)
        self.assertEqual(self.out2in["distance_m"], 2.0)

    def test_preflight_requires_exact_launch_but_runtime_uses_axis_tolerances(self) -> None:
        exact = {"x_m": 9.06, "y_m": 4.3, "heading_deg": 0.0}
        offset = {"x_m": 9.16, "y_m": 4.49, "heading_deg": 0.0}
        outside_x = {"x_m": 9.211, "y_m": 4.3, "heading_deg": 0.0}
        self.assertEqual(macro_start_pose_issues(exact, self.in2out), [])
        self.assertTrue(macro_start_pose_issues(offset, self.in2out))
        self.assertEqual(macro_start_pose_issues(offset, self.in2out, runtime=True), [])
        self.assertTrue(macro_start_pose_issues(outside_x, self.in2out, runtime=True))

    def test_preflight_endpoint_is_fixed_and_runtime_endpoint_is_relative(self) -> None:
        fixed = macro_planned_pose(self.in2out)
        relative = macro_planned_pose_from_current(
            {"x_m": 9.15, "y_m": 4.11, "heading_deg": 180.0},
            self.in2out,
        )
        self.assertAlmostEqual(fixed["x_m"], 9.06)
        self.assertAlmostEqual(fixed["y_m"], 6.3)
        self.assertAlmostEqual(relative["x_m"], 9.15)
        self.assertAlmostEqual(relative["y_m"], 6.11)

    def test_normal_movement_cannot_enter_zone(self) -> None:
        issues = ramp_restriction_movement_issues(
            {"x_m": 8.0, "y_m": 4.8},
            {"x_m": 9.0, "y_m": 4.8},
            self.policy,
        )
        self.assertEqual(issues[0]["code"], "NORMAL_MOVE_ENTERS_RAMP_ZONE")

    def test_normal_movement_may_exit_landing_buffer_away_from_core(self) -> None:
        issues = ramp_restriction_movement_issues(
            {"x_m": 9.06, "y_m": 6.05},
            {"x_m": 10.0, "y_m": 6.4},
            self.policy,
        )
        self.assertEqual(issues, [])

    def test_normal_exit_may_not_cross_core(self) -> None:
        issues = ramp_restriction_movement_issues(
            {"x_m": 9.06, "y_m": 6.05},
            {"x_m": 9.06, "y_m": 4.0},
            self.policy,
        )
        self.assertEqual(issues[0]["code"], "NORMAL_MOVE_EXIT_CROSSES_BUMP_CORE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
