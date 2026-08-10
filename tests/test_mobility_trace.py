import json
import math
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import m8mobility_trace


class MobilityTraceTests(unittest.TestCase):
    def test_expected_endpoint_has_unit_completion_and_zero_cross_track(self):
        result = m8mobility_trace.endpoint_comparison(
            start_pose={"x_m": 8.5, "y_m": 1.5, "heading_deg": 90.0},
            expected_pose={"x_m": 8.5, "y_m": 3.0, "heading_deg": 270.0},
            actual_pose={"x_m": 8.5, "y_m": 3.0, "heading_deg": 270.0},
            commanded_distance_m=1.5,
        )
        self.assertAlmostEqual(result["expected_to_actual_position_error_m"], 0.0)
        self.assertAlmostEqual(result["along_track_displacement_m"], 1.5)
        self.assertAlmostEqual(result["cross_track_displacement_m"], 0.0)
        self.assertAlmostEqual(result["commanded_distance_completion_ratio"], 1.0)

    def test_undertravel_and_lateral_error_are_separated(self):
        result = m8mobility_trace.endpoint_comparison(
            start_pose={"x_m": 8.5, "y_m": 1.5, "heading_deg": 90.0},
            expected_pose={"x_m": 8.5, "y_m": 3.0, "heading_deg": 270.0},
            actual_pose={"x_m": 8.7, "y_m": 1.65, "heading_deg": 100.0},
            commanded_distance_m=1.5,
        )
        self.assertAlmostEqual(result["along_track_displacement_m"], 0.15)
        self.assertAlmostEqual(result["cross_track_displacement_m"], -0.2)
        self.assertAlmostEqual(result["commanded_distance_completion_ratio"], 0.1)
        self.assertAlmostEqual(
            result["expected_to_actual_position_error_m"],
            math.hypot(0.2, 1.35),
        )

    def test_append_writes_one_parseable_jsonl_record(self):
        with tempfile.TemporaryDirectory() as td:
            trace_path = Path(td) / "trace.jsonl"
            with patch.object(m8mobility_trace, "TRACE_PATH", trace_path):
                ok = m8mobility_trace.append_trace_event(
                    event="test_event",
                    scanner="twin-scout-charlie",
                    trace_id="trace-1",
                    data={"value": 3},
                )
            self.assertTrue(ok)
            rows = trace_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(rows), 1)
            record = json.loads(rows[0])
            self.assertEqual(record["event"], "test_event")
            self.assertEqual(record["trace_id"], "trace-1")
            self.assertEqual(record["data"]["value"], 3)

    def test_append_failure_is_swallowed(self):
        with tempfile.TemporaryDirectory() as td:
            # Opening a directory as a file fails, which must return False only.
            with patch.object(m8mobility_trace, "TRACE_PATH", Path(td)):
                ok = m8mobility_trace.append_trace_event(
                    event="test_failure",
                    scanner="twin-scout-charlie",
                )
            self.assertFalse(ok)

    def test_incomplete_pose_returns_diagnostic_instead_of_raising(self):
        result = m8mobility_trace.endpoint_comparison(
            start_pose={},
            expected_pose={},
            actual_pose={},
            commanded_distance_m=1.0,
        )
        self.assertFalse(result["comparison_ok"])
        self.assertIn("KeyError", result["detail"])


if __name__ == "__main__":
    unittest.main()
