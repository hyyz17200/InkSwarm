from __future__ import annotations

from pathlib import Path
from unittest import TestCase

from printfarm.models import TaskItem, WorkerConfig
from printfarm.run_service import RunService
from printfarm.worker_service import WorkerValidationError, validate_unique_worker_names


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
