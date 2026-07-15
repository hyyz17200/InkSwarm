from __future__ import annotations

from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

from printfarm.config_store import ConfigStore
from printfarm.models import PresetConfig, TaskItem, WorkerConfig
from printfarm.run_service import RunService
from printfarm.worker_service import WorkerService, WorkerValidationError, validate_unique_worker_names


def worker(name: str, directory: str) -> WorkerConfig:
    return WorkerConfig(
        name=name,
        directory=Path(directory),
        printer_name=f"Printer {name or 'blank'}",
        enabled=True,
    )


class WorkerValidationTests(TestCase):
    def test_unique_worker_names_pass(self) -> None:
        validate_unique_worker_names([
            worker("P01", "workers/P01"),
            worker("P02", "workers/P02"),
        ])

    def test_duplicate_worker_names_fail_with_directories(self) -> None:
        with self.assertRaises(WorkerValidationError) as raised:
            validate_unique_worker_names([
                worker("P07", "workers/P07-a"),
                worker("P07", "workers/P07-b"),
            ])

        message = str(raised.exception)
        self.assertIn("Duplicate Worker name 'P07'", message)
        self.assertIn("workers\\P07-a", message)
        self.assertIn("workers\\P07-b", message)

    def test_duplicate_worker_names_are_case_insensitive(self) -> None:
        with self.assertRaisesRegex(WorkerValidationError, "Duplicate Worker name 'p07'"):
            validate_unique_worker_names([
                worker("P07", "workers/P07-a"),
                worker("p07", "workers/P07-b"),
            ])

    def test_empty_worker_name_fails(self) -> None:
        with self.assertRaisesRegex(WorkerValidationError, "Worker name cannot be empty"):
            validate_unique_worker_names([worker("   ", "workers/blank")])

    def test_worker_name_errors_can_localize_to_chinese(self) -> None:
        with self.assertRaises(WorkerValidationError) as raised:
            validate_unique_worker_names(
                [
                    worker("P07", "workers/P07-a"),
                    worker("P07", "workers/P07-b"),
                    worker("   ", "workers/blank"),
                ],
                language="zh-Hans",
            )

        message = str(raised.exception)
        self.assertIn("Worker 名称重复 'P07'", message)
        self.assertIn("Worker 名称不能为空", message)

    def test_run_service_blocks_duplicate_worker_names_before_snapshot(self) -> None:
        task = TaskItem(file_path=Path("job.pdf"), copies=1)

        with self.assertRaisesRegex(WorkerValidationError, "Duplicate Worker name 'P07'"):
            RunService().prepare_start(
                tasks=[task],
                workers=[
                    worker("P07", "workers/P07-a"),
                    worker("P07", "workers/P07-b"),
                ],
                settings={},
            )

    def test_run_service_worker_validation_uses_settings_language(self) -> None:
        task = TaskItem(file_path=Path("job.pdf"), copies=1)

        with self.assertRaisesRegex(WorkerValidationError, "Worker 名称重复 'P07'"):
            RunService().prepare_start(
                tasks=[task],
                workers=[
                    worker("P07", "workers/P07-a"),
                    worker("P07", "workers/P07-b"),
                ],
                settings={"language": "zh-Hans"},
            )


class WorkerServiceLocalizationTests(TestCase):
    def test_restore_preset_message_defaults_to_english_and_can_localize(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            preset = PresetConfig(name="Glossy", printui_restore_file="Glossy.dat")
            current_worker = WorkerConfig(
                name="WorkerA",
                directory=root,
                printer_name="PrinterA",
                active_preset=preset.name,
                presets={preset.name: preset},
            )
            (root / "Glossy.dat").write_bytes(b"snapshot")
            service = WorkerService(ConfigStore(root))

            with patch("printfarm.worker_service.restore_printer_settings"):
                self.assertEqual(
                    service.restore_preset_if_any(current_worker),
                    "Loaded driver snapshot for WorkerA/Glossy.",
                )
                self.assertEqual(
                    service.restore_preset_if_any(current_worker, language="zh-Hans"),
                    "已载入 WorkerA/Glossy 的驱动快照。",
                )
