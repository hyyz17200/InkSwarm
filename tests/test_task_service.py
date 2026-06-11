from __future__ import annotations

from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
import json
import tempfile

from printfarm.config_store import ConfigStore
from printfarm.models import TaskItem
from printfarm.task_inspector import TaskInspection, TaskInspectionError
from printfarm.task_service import TaskService


class TaskServiceAddFilesTests(TestCase):
    def make_service(self, root: Path) -> TaskService:
        return TaskService(ConfigStore(root))

    def test_missing_file_is_reported_as_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = self.make_service(Path(tmp))
            missing = Path(tmp) / "missing.pdf"

            result = service.add_files([], [missing])

            self.assertEqual(result.added_count, 0)
            self.assertEqual(len(result.skipped), 1)
            self.assertEqual(result.skipped[0].file_path, missing)
            self.assertEqual(result.skipped[0].reason, "文件不存在")

    def test_unsupported_file_type_is_reported_as_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = self.make_service(root)
            unsupported = root / "notes.txt"
            unsupported.write_text("not printable", encoding="utf-8")

            result = service.add_files([], [unsupported])

            self.assertEqual(result.added_count, 0)
            self.assertEqual(len(result.skipped), 1)
            self.assertEqual(result.skipped[0].file_path, unsupported)
            self.assertEqual(result.skipped[0].reason, "不支持的文件类型")

    def test_duplicate_file_is_reported_as_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = self.make_service(root)
            pdf = root / "job.pdf"
            pdf.write_bytes(b"%PDF")
            existing = [TaskItem(file_path=pdf.resolve())]

            result = service.add_files(existing, [pdf])

            self.assertEqual(result.added_count, 0)
            self.assertEqual(len(result.skipped), 1)
            self.assertEqual(result.skipped[0].file_path, pdf.resolve())
            self.assertEqual(result.skipped[0].reason, "已在任务列表中")

    def test_successful_and_failed_inspections_keep_existing_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = self.make_service(root)
            good = root / "good.pdf"
            bad = root / "bad.pdf"
            good.write_bytes(b"%PDF")
            bad.write_bytes(b"%PDF")

            def inspect(path: Path) -> TaskInspection:
                if path == bad.resolve():
                    raise TaskInspectionError("broken pdf")
                return TaskInspection(display_size_mm="10 x 10 mm", preview_bytes=b"preview")

            with patch("printfarm.task_service.inspect_task_input", side_effect=inspect):
                result = service.add_files([], [good, bad], default_copies=3)

            self.assertEqual(result.added_count, 1)
            self.assertEqual(len(result.skipped), 1)
            self.assertEqual(result.skipped[0].file_path, bad.resolve())
            self.assertEqual(result.skipped[0].reason, "broken pdf")


class TaskServiceRestoreTests(TestCase):
    def test_restore_reports_missing_session_file_as_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = ConfigStore(root)
            service = TaskService(store)
            missing = root / "missing.pdf"
            store.task_session_file.write_text(
                json.dumps(
                    [
                        {
                            "file_path": str(missing),
                            "enabled": True,
                            "copies": 5,
                        }
                    ]
                ),
                encoding="utf-8",
            )

            result = service.restore_saved_tasks([])

            self.assertEqual(result.requested_count, 1)
            self.assertEqual(result.add_result.added_count, 0)
            self.assertEqual(len(result.add_result.skipped), 1)
            self.assertEqual(result.add_result.skipped[0].file_path, missing)
            self.assertEqual(result.add_result.skipped[0].reason, "文件不存在")
