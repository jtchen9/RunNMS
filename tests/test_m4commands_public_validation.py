from __future__ import annotations

import asyncio
import importlib.util
import io
import sys
import tempfile
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


NMS_ROOT = Path(__file__).resolve().parents[1]
COMMON_DIR = NMS_ROOT / "sitemap" / "CommonCheckers"
SITE_DIR = NMS_ROOT / "sitemap" / "DemoRoom"


class HTTPException(Exception):
    def __init__(self, status_code: int, detail=None):
        super().__init__(str(detail))
        self.status_code = status_code
        self.detail = detail


class UploadFile:
    def __init__(self, *, filename: str, file: io.BytesIO):
        self.filename = filename
        self.file = file

    async def read(self) -> bytes:
        return self.file.read()


class _Router:
    def get(self, *args, **kwargs):
        return lambda func: func

    def post(self, *args, **kwargs):
        return lambda func: func


def _parameter_marker(default=None, *args, **kwargs):
    return default


def _install_import_stubs() -> None:
    fastapi = types.ModuleType("fastapi")
    fastapi.APIRouter = _Router
    fastapi.HTTPException = HTTPException
    fastapi.Query = _parameter_marker
    fastapi.Request = object
    fastapi.UploadFile = UploadFile
    fastapi.File = _parameter_marker
    fastapi.Form = _parameter_marker

    pydantic = types.ModuleType("pydantic")
    pydantic.BaseModel = object
    pydantic.Field = _parameter_marker

    config = types.ModuleType("config")
    config.TIME_FMT = "%Y-%m-%d %H:%M:%S"
    config.KEY_WHITELIST_SCANNER_META = "nms:whitelist:scanner_meta"
    config.r = object()

    utility = types.ModuleType("utility")
    utility.local_ts = lambda: "2026-07-27 12:00:00"
    utility.parse_local_dt = lambda value: value

    m1_registry = types.ModuleType("m1Registry")
    m7_traffic = types.ModuleType("m7Traffic")

    m8_mobility = types.ModuleType("m8mobility")
    m8_mobility._clear_all_command_queues = lambda: {}
    m8_mobility.mobility_init = lambda: {}

    state_store = types.ModuleType("m8mobility_state_store")
    state_store.key_report = lambda scanner: ""
    state_store.key_time = lambda scanner: ""
    state_store.key_state = lambda scanner: ""
    state_store.key_pose = lambda scanner: ""
    state_store._save_stop = lambda *args, **kwargs: None

    sys.modules.update(
        {
            "config": config,
            "utility": utility,
            "m1Registry": m1_registry,
            "m7Traffic": m7_traffic,
            "m8mobility": m8_mobility,
            "m8mobility_state_store": state_store,
            "fastapi": fastapi,
            "pydantic": pydantic,
        }
    )


def _load_m4commands():
    _install_import_stubs()
    spec = importlib.util.spec_from_file_location(
        "m4commands_phase2_under_test",
        NMS_ROOT / "m4Commands.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PublicRegistrationValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.m4 = _load_m4commands()
        common_text = str(COMMON_DIR.resolve())
        if common_text not in sys.path:
            sys.path.insert(0, common_text)
        from checker.checker_runner import validate_script

        cls.validate_script = staticmethod(validate_script)

    @staticmethod
    def _poses_bytes() -> bytes:
        return (
            "scanner,intended_x_m,intended_y_m,intended_heading_deg,"
            "position_tolerance_m,heading_tolerance_deg\n"
            "twin-scout-delta,3.9,4.4,270,0.2,10\n"
        ).encode("utf-8")

    def _direct_report(self, script_bytes: bytes) -> dict:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            script_path = directory / "experiment_script.csv"
            poses_path = directory / "initial_poses.csv"
            script_path.write_bytes(script_bytes)
            poses_path.write_bytes(self._poses_bytes())
            return self.validate_script(
                script_csv=script_path,
                initial_poses_csv=poses_path,
                common_dir=COMMON_DIR,
                site_dir=SITE_DIR,
            )

    @staticmethod
    def _public_result(report: dict) -> dict:
        return {
            "ok": report.get("ok"),
            "error_count": report.get("error_count"),
            "warning_count": report.get("warning_count"),
            "issue_codes": [x.get("code") for x in report.get("issues", [])],
            "common_checkers_version": report.get("common_checkers_version"),
            "site_authoring_version": report.get("site_authoring_version"),
        }

    def test_adapter_matches_direct_common_checker_for_valid_scan(self) -> None:
        script = (
            "scanner,t_offset_sec,category,action,args_json\n"
            'twin-scout-delta,0,scan,scan.once,"{}"\n'
        ).encode("utf-8")

        direct = self._direct_report(script)
        adapted = self.m4._run_public_script_validation(
            script_csv_bytes=script,
            initial_poses_csv_bytes=self._poses_bytes(),
            common_dir=COMMON_DIR,
            site_dir=SITE_DIR,
        )

        self.assertEqual(self._public_result(adapted), self._public_result(direct))
        self.assertTrue(adapted["ok"])

    def test_adapter_matches_direct_common_checker_for_invalid_scan_args(self) -> None:
        script = (
            "scanner,t_offset_sec,category,action,args_json\n"
            'twin-scout-delta,0,scan,scan.once,"{""force"":true}"\n'
        ).encode("utf-8")

        direct = self._direct_report(script)
        adapted = self.m4._run_public_script_validation(
            script_csv_bytes=script,
            initial_poses_csv_bytes=self._poses_bytes(),
            common_dir=COMMON_DIR,
            site_dir=SITE_DIR,
        )

        self.assertEqual(self._public_result(adapted), self._public_result(direct))
        self.assertFalse(adapted["ok"])
        self.assertIn(
            "COMMAND_ARGS_NOT_ALLOWED",
            [x.get("code") for x in adapted["issues"]],
        )

    def test_adapter_selects_site_folder_from_nms_lab_id(self) -> None:
        with patch.object(self.m4, "_nms_lab_id", return_value="AnotherTestSite"):
            with self.assertRaises(FileNotFoundError) as caught:
                self.m4._run_public_script_validation(
                    script_csv_bytes=b"",
                    initial_poses_csv_bytes=b"",
                    common_dir=COMMON_DIR,
                )

        expected = NMS_ROOT / "sitemap" / "AnotherTestSite"
        self.assertIn(str(expected), str(caught.exception))

    def test_validation_failure_stops_before_registration_operations(self) -> None:
        failure_report = {
            "ok": False,
            "error_count": 1,
            "warning_count": 0,
            "issues": [{"code": "EMPTY_SCRIPT", "level": "error"}],
        }
        script_upload = UploadFile(
            filename="empty.csv",
            file=io.BytesIO(
                b"scanner,t_offset_sec,category,action,args_json\n"
            ),
        )
        poses_upload = UploadFile(
            filename="initial_poses.csv",
            file=io.BytesIO(self._poses_bytes()),
        )

        with (
            patch.object(
                self.m4,
                "_run_public_script_validation",
                return_value=failure_report,
            ),
            patch.object(self.m4.utility, "parse_local_dt") as parse_t0,
            patch.object(self.m4, "_require_experiment_t0_future") as t0_gate,
            patch.object(self.m4, "_analyze_csv_rows_for_experiment") as analyze,
            patch.object(self.m4, "_require_empty_experiment_registry") as registry_gate,
            patch.object(self.m4.m8mobility, "mobility_init") as mobility_init,
            patch.object(self.m4, "_enqueue_script_or_csv_item") as enqueue,
            patch.object(self.m4, "_register_experiment_status") as register,
        ):
            with self.assertRaises(HTTPException) as caught:
                asyncio.run(
                    self.m4.cmd_load_csv_file(
                        t0="not-even-parsed",
                        session_id=None,
                        replace_existing=False,
                        csv_file=script_upload,
                        initial_poses_file=poses_upload,
                    )
                )

        self.assertEqual(caught.exception.status_code, 422)
        self.assertEqual(
            caught.exception.detail["error"],
            "public_validation_failed",
        )
        self.assertEqual(
            caught.exception.detail["public_validation"],
            failure_report,
        )
        for mocked in (
            parse_t0,
            t0_gate,
            analyze,
            registry_gate,
            mobility_init,
            enqueue,
            register,
        ):
            mocked.assert_not_called()

    def test_shared_first_mobility_rule_stops_before_api_analyzer(self) -> None:
        script_upload = UploadFile(
            filename="bad_first_mobility.csv",
            file=io.BytesIO(
                (
                    "scanner,t_offset_sec,category,action,args_json\n"
                    'twin-scout-delta,0,mobility,mobility.move,'
                    '"{""x_m"":4.0,""y_m"":4.0}"\n'
                ).encode("utf-8")
            ),
        )
        poses_upload = UploadFile(
            filename="initial_poses.csv",
            file=io.BytesIO(
                (
                    "scanner,intended_x_m,intended_y_m,intended_heading_deg,"
                    "position_tolerance_m,heading_tolerance_deg\n"
                    "twin-scout-delta,3.9,4.4,270,0.2,10\n"
                ).encode("utf-8")
            ),
        )

        with (
            patch.object(self.m4, "_analyze_csv_rows_for_experiment") as analyze,
            patch.object(self.m4, "_require_empty_experiment_registry") as registry_gate,
            patch.object(self.m4.m8mobility, "mobility_init") as mobility_init,
            patch.object(self.m4, "_enqueue_script_or_csv_item") as enqueue,
            patch.object(self.m4, "_register_experiment_status") as register,
        ):
            with self.assertRaises(HTTPException) as caught:
                asyncio.run(
                    self.m4.cmd_load_csv_file(
                        t0="not-even-parsed",
                        session_id=None,
                        replace_existing=False,
                        csv_file=script_upload,
                        initial_poses_file=poses_upload,
                    )
                )

        self.assertEqual(caught.exception.status_code, 422)
        issue_codes = [
            issue.get("code")
            for issue in caught.exception.detail["public_validation"]["issues"]
        ]
        self.assertIn("BAD_FIRST_MOBILITY_COMMAND", issue_codes)
        for mocked in (
            analyze,
            registry_gate,
            mobility_init,
            enqueue,
            register,
        ):
            mocked.assert_not_called()

    def test_registration_analysis_keeps_only_live_whitelist_rule(self) -> None:
        rows = [
            {
                "scanner": "twin-scout-delta",
                "t_offset_sec": "0",
                "category": "mobility",
                "action": "mobility.report.location",
                "args_json": "{}",
            }
        ]
        live_redis = types.SimpleNamespace(hexists=lambda key, scanner: True)

        with patch.object(self.m4.config, "r", live_redis):
            result = self.m4._analyze_csv_rows_for_experiment(
                rows,
                t0_dt=datetime(2026, 7, 27, 12, 0, 0),
            )

        self.assertEqual(result["added"], 1)
        self.assertEqual(result["bad_rows"], 0)
        self.assertEqual(result["skipped_not_whitelisted"], 0)
        self.assertEqual(
            result["preflight"]["public_validation"],
            "passed_before_registration_analysis",
        )
        self.assertEqual(result["preflight"]["live_whitelist"], "ok")

    def test_registration_rejects_when_no_target_is_live_whitelisted(self) -> None:
        rows = [
            {
                "scanner": "twin-scout-delta",
                "t_offset_sec": "0",
                "category": "scan",
                "action": "scan.once",
                "args_json": "{}",
            }
        ]
        live_redis = types.SimpleNamespace(hexists=lambda key, scanner: False)

        with (
            patch.object(self.m4.config, "r", live_redis),
            self.assertRaises(HTTPException) as caught,
        ):
            self.m4._analyze_csv_rows_for_experiment(
                rows,
                t0_dt=datetime(2026, 7, 27, 12, 0, 0),
            )

        self.assertEqual(caught.exception.status_code, 400)
        self.assertIn("no accepted commands", str(caught.exception.detail))
        self.assertIn("skipped_not_whitelisted=1", str(caught.exception.detail))


if __name__ == "__main__":
    unittest.main(verbosity=2)
