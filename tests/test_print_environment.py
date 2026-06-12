from __future__ import annotations

from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

from printfarm.controller import PrintController
from printfarm.models import TaskItem, WorkerConfig
from printfarm.spooler import PrinterSpooler


class PrintEnvironmentTests(TestCase):
    def test_non_windows_environment_is_rejected(self) -> None:
        with patch("printfarm.spooler.sys.platform", "linux"):
            with self.assertRaisesRegex(RuntimeError, "only supported on Windows"):
                PrinterSpooler.validate_environment()
            with self.assertRaisesRegex(RuntimeError, "InkSwarm 打印仅支持 Windows"):
                PrinterSpooler.validate_environment(language="zh-Hans")

    def test_controller_start_aborts_before_thread_when_environment_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            controller = PrintController(root / "cache", root / "statistics")
            task = TaskItem(file_path=Path("job.pdf"), copies=1, task_id="task-a")
            worker = WorkerConfig(
                name="WorkerA",
                directory=root / "worker",
                printer_name="PrinterA",
                enabled=True,
            )

            with patch(
                "printfarm.controller.PrinterSpooler.validate_environment",
                side_effect=RuntimeError("missing pywin32"),
            ):
                with self.assertRaisesRegex(RuntimeError, "missing pywin32"):
                    controller.start([task], [worker])

            self.assertFalse(controller.is_running())
            self.assertIsNone(controller._thread)
            self.assertIsNone(controller._statistics_run_id)
            self.assertFalse(any((root / "statistics" / "pending_runs").glob("*.json")))
