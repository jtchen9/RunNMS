import unittest

from robot_location_helper.correction_decision import (
    decide_followup_correction,
)


def _location(x_m, *, confidence_level="HIGH"):
    return {
        "location_ok": True,
        "x_m": float(x_m),
        "y_m": 0.0,
        "confidence": {
            "level": confidence_level,
        },
    }


PLANNED = {
    "location_ok": True,
    "x_m": 0.0,
    "y_m": 0.0,
}


class CorrectionDecisionThresholdTests(unittest.TestCase):
    def test_below_15_cm_does_not_request_correction(self):
        result = decide_followup_correction(
            location_result=_location(0.149),
            planned_location=PLANNED,
        )

        self.assertFalse(result["go"])
        self.assertEqual(
            result["reason_code"],
            "POSITION_ERROR_WITHIN_15CM",
        )

    def test_exactly_15_cm_does_not_request_correction(self):
        result = decide_followup_correction(
            location_result=_location(0.15),
            planned_location=PLANNED,
        )

        self.assertFalse(result["go"])
        self.assertEqual(result["position_threshold_m"], 0.15)

    def test_above_15_cm_with_high_confidence_requests_correction(self):
        result = decide_followup_correction(
            location_result=_location(0.151),
            planned_location=PLANNED,
        )

        self.assertTrue(result["go"])
        self.assertEqual(
            result["reason_code"],
            "POSITION_ERROR_GT_15CM_AND_CONFIDENCE_SUFFICIENT",
        )

    def test_above_15_cm_with_medium_confidence_requests_correction(self):
        result = decide_followup_correction(
            location_result=_location(
                0.20,
                confidence_level="MEDIUM",
            ),
            planned_location=PLANNED,
        )

        self.assertTrue(result["go"])

    def test_above_15_cm_with_low_confidence_blocks_correction(self):
        result = decide_followup_correction(
            location_result=_location(
                0.20,
                confidence_level="LOW",
            ),
            planned_location=PLANNED,
        )

        self.assertFalse(result["go"])
        self.assertEqual(
            result["reason_code"],
            "LARGE_ERROR_BUT_LOW_LOCATION_CONFIDENCE",
        )


if __name__ == "__main__":
    unittest.main()
