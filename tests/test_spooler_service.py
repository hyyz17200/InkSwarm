from __future__ import annotations

from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

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
