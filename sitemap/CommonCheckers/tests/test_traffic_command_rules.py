from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


COMMON_DIR = Path(__file__).resolve().parents[1]
if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from checker.argument_rules import check_command_arguments
from checker.script_model import InitialPose, ScriptRow
from checker.traffic_rules import check_traffic_sessions


class TrafficCommandRuleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = json.loads(
            (COMMON_DIR / "config" / "script_policy.json").read_text(encoding="utf-8")
        )
        cls.poses = {
            "twin-scout-alpha": InitialPose(2, "twin-scout-alpha", 2, 2, 0, 0.2, 10),
            "twin-scout-bravo": InitialPose(3, "twin-scout-bravo", 3, 3, 0, 0.2, 10),
        }

    def _row(
        self,
        action: str,
        args: dict,
        *,
        scanner: str = "twin-scout-alpha",
        offset: int = 0,
        row_number: int = 2,
    ) -> ScriptRow:
        return ScriptRow(row_number, scanner, offset, "traffic", action, args)

    def _codes(self, row: ScriptRow) -> list[str]:
        return [x["code"] for x in check_command_arguments([row], self.policy)]

    def _tcp(self, **changes) -> dict:
        args = {
            "session_id": "S1",
            "ac": "bk",
            "protocol": "tcp",
            "duration_sec": 300,
            "report_interval_sec": 60,
            "reverse": True,
            "parallel": 1,
        }
        args.update(changes)
        return args

    def _udp(self, **changes) -> dict:
        args = {
            "session_id": "S2",
            "ac": "bk",
            "protocol": "udp",
            "duration_sec": 300,
            "report_interval_sec": 60,
            "reverse": True,
            "bitrate": "100M",
            "packet_size": 1200,
        }
        args.update(changes)
        return args

    def test_valid_tcp_udp_and_stop(self) -> None:
        self.assertEqual(self._codes(self._row("traffic.session.start", self._tcp())), [])
        self.assertEqual(self._codes(self._row("traffic.session.start", self._udp())), [])
        self.assertEqual(
            self._codes(self._row("traffic.session.stop", {"session_id": "S1"})), []
        )

    def test_session_id_format(self) -> None:
        for bad in ("s1", "S", "S1234567890", "S bad", "S.bad"):
            with self.subTest(value=bad):
                self.assertIn(
                    "COMMAND_ARG_BAD_FORMAT",
                    self._codes(self._row("traffic.session.start", self._tcp(session_id=bad))),
                )

    def test_duration_and_fixed_report_interval(self) -> None:
        for value in (60, 1201, 60.5, "300"):
            with self.subTest(duration=value):
                self.assertTrue(
                    set(self._codes(self._row("traffic.session.start", self._tcp(duration_sec=value))))
                    & {"COMMAND_ARG_OUT_OF_RANGE", "COMMAND_ARG_BAD_TYPE"}
                )
        for value in (61, 1200):
            self.assertEqual(
                self._codes(self._row("traffic.session.start", self._tcp(duration_sec=value))),
                [],
            )
        self.assertIn(
            "COMMAND_ARG_OUT_OF_RANGE",
            self._codes(self._row("traffic.session.start", self._tcp(report_interval_sec=30))),
        )

    def test_protocol_specific_contracts(self) -> None:
        tcp_missing = self._tcp()
        del tcp_missing["parallel"]
        self.assertIn(
            "COMMAND_ARGS_MISSING_REQUIRED",
            self._codes(self._row("traffic.session.start", tcp_missing)),
        )
        self.assertIn(
            "COMMAND_ARGS_FORBIDDEN_FIELDS",
            self._codes(
                self._row("traffic.session.start", self._tcp(bitrate="1M"))
            ),
        )
        udp_missing = self._udp()
        del udp_missing["packet_size"]
        self.assertIn(
            "COMMAND_ARGS_MISSING_REQUIRED",
            self._codes(self._row("traffic.session.start", udp_missing)),
        )
        self.assertIn(
            "COMMAND_ARGS_FORBIDDEN_FIELDS",
            self._codes(self._row("traffic.session.start", self._udp(parallel=1))),
        )

    def test_malformed_protocol_type_is_reported_without_crashing(self) -> None:
        codes = self._codes(
            self._row("traffic.session.start", self._tcp(protocol=["tcp"]))
        )
        self.assertIn("COMMAND_ARG_BAD_TYPE", codes)

    def test_enums_types_and_ranges(self) -> None:
        invalid = (
            (self._tcp(ac="xx"), "COMMAND_ARG_BAD_VALUE"),
            (self._tcp(protocol="sctp"), "COMMAND_ARG_BAD_VALUE"),
            (self._tcp(reverse="TRUE"), "COMMAND_ARG_BAD_TYPE"),
            (self._tcp(parallel=0), "COMMAND_ARG_OUT_OF_RANGE"),
            (self._tcp(parallel=17), "COMMAND_ARG_OUT_OF_RANGE"),
            (self._udp(bitrate="2M"), "COMMAND_ARG_BAD_VALUE"),
            (self._udp(packet_size=63), "COMMAND_ARG_OUT_OF_RANGE"),
            (self._udp(packet_size=1473), "COMMAND_ARG_OUT_OF_RANGE"),
        )
        for args, expected in invalid:
            with self.subTest(args=args):
                self.assertIn(
                    expected,
                    self._codes(self._row("traffic.session.start", args)),
                )

    def test_global_session_registry(self) -> None:
        rows = [
            self._row("traffic.session.start", self._tcp(), offset=10, row_number=2),
            self._row(
                "traffic.session.start",
                self._udp(session_id="S1"),
                scanner="twin-scout-bravo",
                offset=20,
                row_number=3,
            ),
            self._row(
                "traffic.session.stop",
                {"session_id": "S1"},
                scanner="twin-scout-bravo",
                offset=5,
                row_number=4,
            ),
            self._row(
                "traffic.session.stop",
                {"session_id": "Sunknown"},
                offset=30,
                row_number=5,
            ),
            self._row(
                "traffic.session.stop",
                {"session_id": "S1"},
                offset=40,
                row_number=6,
            ),
        ]
        issues = check_traffic_sessions(rows, self.poses)
        codes = [x["code"] for x in issues]
        self.assertIn("TRAFFIC_SESSION_ID_DUPLICATE", codes)
        self.assertIn("TRAFFIC_STOP_TARGET_MISMATCH", codes)
        self.assertIn("TRAFFIC_STOP_NOT_AFTER_START", codes)
        self.assertIn("TRAFFIC_STOP_UNKNOWN_SESSION", codes)
        self.assertIn("TRAFFIC_SESSION_MULTIPLE_STOPS", codes)

    def test_target_must_be_enabled_robot(self) -> None:
        issues = check_traffic_sessions(
            [self._row("traffic.session.start", self._tcp(), scanner="AP1")],
            self.poses,
        )
        self.assertEqual([x["code"] for x in issues], ["TRAFFIC_TARGET_NOT_ROBOT"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
