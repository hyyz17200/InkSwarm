from __future__ import annotations

from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from printfarm.spooler_service import (
    SERVICE_RUNNING,
    SERVICE_STOPPED,
    SpoolerMaintenance,
    SpoolerMaintenanceError,
    SpoolerServiceStatus,
    _elevated_helper_command,
    run_elevated_spooler_maintenance,
)


def _status(state: int, process_id: int) -> SpoolerServiceStatus:
    return SpoolerServiceStatus(state=state, process_id=process_id, checkpoint=0, wait_hint_ms=0)


class SpoolerMaintenanceLocalizationTests(TestCase):
    def test_non_windows_messages_default_to_english_and_can_localize(self) -> None:
        with patch("printfarm.spooler_service.sys.platform", "linux"):
            with self.assertRaisesRegex(SpoolerMaintenanceError, "only supported on Windows"):
                SpoolerMaintenance()
            with self.assertRaisesRegex(SpoolerMaintenanceError, "重启打印队列仅支持 Windows"):
                SpoolerMaintenance(language="zh-Hans")
            with self.assertRaisesRegex(SpoolerMaintenanceError, "重启打印队列仅支持 Windows"):
                run_elevated_spooler_maintenance(language="zh-Hans")

    def test_elevated_helper_command_passes_language(self) -> None:
        _file_path, parameters, _working_dir = _elevated_helper_command(
            Path("result.json"),
            Path("events.jsonl"),
            15.0,
            language="zh-Hans",
        )

        self.assertIn("--language zh-Hans", parameters)


class SpoolerRestartTests(TestCase):
    def test_lingering_stopped_pid_is_rejected_before_service_restart(self) -> None:
        maintenance = SpoolerMaintenance.__new__(SpoolerMaintenance)
        maintenance.language = "en"
        maintenance.is_process_elevated = lambda: True
        maintenance.query_status = lambda: _status(SERVICE_RUNNING, 100)
        maintenance.stop = lambda log=None: _status(SERVICE_STOPPED, 100)
        start_calls: list[bool] = []
        maintenance.start = lambda log=None: start_calls.append(True) or _status(SERVICE_RUNNING, 200)

        with self.assertRaisesRegex(SpoolerMaintenanceError, "PID is still 100 after stopping"):
            maintenance.restart()

        self.assertEqual(start_calls, [])


class ElevatedMaintenanceCleanupTests(TestCase):
    def test_temp_files_are_cleaned_when_helper_wait_fails(self) -> None:
        cleaned: list[Path] = []
        close_handle = Mock()
        with patch("printfarm.spooler_service._shell_execute_runas", return_value=123), patch(
            "printfarm.spooler_service._wait_for_helper", side_effect=SpoolerMaintenanceError("wait failed")
        ), patch("printfarm.spooler_service._cleanup_temp_file", side_effect=cleaned.append), patch(
            "printfarm.spooler_service.ctypes.windll.kernel32.CloseHandle", close_handle
        ):
            with self.assertRaisesRegex(SpoolerMaintenanceError, "wait failed"):
                run_elevated_spooler_maintenance()

        self.assertEqual(len(cleaned), 2)
        self.assertTrue(str(cleaned[0]).endswith(".json"))
        self.assertTrue(str(cleaned[1]).endswith(".events.jsonl"))
        close_handle.assert_called_once_with(123)

    def test_temp_files_are_cleaned_when_helper_result_is_missing(self) -> None:
        cleaned: list[Path] = []
        with patch("printfarm.spooler_service._shell_execute_runas", return_value=123), patch(
            "printfarm.spooler_service._wait_for_helper", return_value=2
        ), patch("printfarm.spooler_service._cleanup_temp_file", side_effect=cleaned.append), patch(
            "printfarm.spooler_service.ctypes.windll.kernel32.CloseHandle"
        ):
            with self.assertRaisesRegex(SpoolerMaintenanceError, "did not write a result"):
                run_elevated_spooler_maintenance()

        self.assertEqual(len(cleaned), 2)
