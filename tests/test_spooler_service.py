from __future__ import annotations

from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from printfarm.spooler_service import (
    SpoolerMaintenance,
    SpoolerMaintenanceError,
    _elevated_helper_command,
    run_elevated_spooler_maintenance,
)


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
