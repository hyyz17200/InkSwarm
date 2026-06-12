from __future__ import annotations

from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
import json
import tempfile

from PIL import Image

from printfarm.config_store import ConfigStore
from printfarm.models import TaskItem
from printfarm.task_inspector import TaskInspection, TaskInspectionError, inspect_task_input
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
            self.assertEqual(result.skipped[0].reason, "File does not exist")

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
            self.assertEqual(result.skipped[0].reason, "Unsupported file type")

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
            self.assertEqual(result.skipped[0].reason, "Already in task list")

    def test_known_skip_reasons_return_translation_keys_when_language_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = self.make_service(root)
            missing = root / "missing.pdf"
            unsupported = root / "notes.txt"
            unsupported.write_text("not printable", encoding="utf-8")
            pdf = root / "job.pdf"
            pdf.write_bytes(b"%PDF")
            existing = [TaskItem(file_path=pdf.resolve())]

            result = service.add_files(existing, [missing, unsupported, pdf], language="zh-Hans")

            self.assertEqual([item.reason for item in result.skipped], ["File does not exist", "Unsupported file type", "Already in task list"])
            self.assertEqual(
                [item.reason_key for item in result.skipped],
                ["task.skip.missing_file", "task.skip.unsupported", "task.skip.duplicate"],
            )

    def test_successful_and_failed_inspections_keep_existing_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = self.make_service(root)
            good = root / "good.pdf"
            bad = root / "bad.pdf"
            good.write_bytes(b"%PDF")
            bad.write_bytes(b"%PDF")

            def inspect(path: Path, *, language: str = "en") -> TaskInspection:
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
            self.assertEqual(result.add_result.skipped[0].reason, "File does not exist")


class TaskInspectionLocalizationTests(TestCase):
    def test_empty_pdf_error_localizes(self) -> None:
        class EmptyDocument:
            def __len__(self) -> int:
                return 0

            def close(self) -> None:
                pass

        with patch("printfarm.task_inspector.pdfium.PdfDocument", return_value=EmptyDocument()):
            with self.assertRaisesRegex(TaskInspectionError, "PDF has no pages"):
                inspect_task_input(Path("empty.pdf"))

        with patch("printfarm.task_inspector.pdfium.PdfDocument", return_value=EmptyDocument()):
            with self.assertRaisesRegex(TaskInspectionError, "PDF 没有页面"):
                inspect_task_input(Path("empty.pdf"), language="zh-Hans")

    def test_cmyk_without_icc_error_localizes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            image_path = Path(tmp) / "cmyk.jpg"
            Image.new("CMYK", (4, 4)).save(image_path)

            with self.assertRaisesRegex(TaskInspectionError, "CMYK file has no embedded ICC"):
                inspect_task_input(image_path)
            with self.assertRaisesRegex(TaskInspectionError, "CMYK 文件没有嵌入 ICC"):
                inspect_task_input(image_path, language="zh-Hans")
