from __future__ import annotations

from io import BytesIO
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

            def inspect(path: Path, *, language: str = "en", cmyk_fallback_icc: Path | None = None) -> TaskInspection:
                if path == bad.resolve():
                    raise TaskInspectionError("broken pdf")
                return TaskInspection(display_size_mm="10 x 10 mm", preview_bytes=b"preview")

            with patch("printfarm.task_service.inspect_task_input", side_effect=inspect):
                result = service.add_files([], [good, bad], default_copies=3)

            self.assertEqual(result.added_count, 1)
            self.assertEqual(len(result.skipped), 1)
            self.assertEqual(result.skipped[0].file_path, bad.resolve())
            self.assertEqual(result.skipped[0].reason, "broken pdf")

    def test_unreadable_pdf_is_reported_as_skipped_without_aborting_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = self.make_service(root)
            good = root / "good.png"
            Image.new("RGB", (4, 4)).save(good)
            bad = root / "bad.pdf"
            bad.write_bytes(b"%PDF-broken")

            # Simulate pdfium failing to open a corrupt/unreadable PDF. This used to
            # raise out of _inspect_pdf and abort the whole import; it must now be
            # caught and recorded as a skip while the good file is still added.
            with patch(
                "printfarm.task_inspector.pdfium.PdfDocument",
                side_effect=RuntimeError("cannot open pdf"),
            ):
                result = service.add_files([], [good, bad])

            self.assertEqual(result.added_count, 1)
            self.assertEqual(len(result.skipped), 1)
            self.assertEqual(result.skipped[0].file_path, bad.resolve())
            self.assertEqual(result.skipped[0].reason, "cannot open pdf")


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

    def test_unreadable_pdf_open_failure_raises_inspection_error(self) -> None:
        # A failure to even open the document must be wrapped as TaskInspectionError
        # rather than letting pdfium's raw RuntimeError propagate.
        with patch(
            "printfarm.task_inspector.pdfium.PdfDocument",
            side_effect=RuntimeError("cannot open pdf"),
        ):
            with self.assertRaisesRegex(TaskInspectionError, "cannot open pdf"):
                inspect_task_input(Path("broken.pdf"))

    def test_cmyk_without_icc_error_localizes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            image_path = Path(tmp) / "cmyk.jpg"
            Image.new("CMYK", (4, 4)).save(image_path)

            with self.assertRaisesRegex(TaskInspectionError, "CMYK file has no embedded ICC"):
                inspect_task_input(image_path)
            with self.assertRaisesRegex(TaskInspectionError, "CMYK 文件没有嵌入 ICC"):
                inspect_task_input(image_path, language="zh-Hans")

    def test_cmyk_without_embedded_icc_is_accepted_when_fallback_is_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            image_path = Path(tmp) / "cmyk.jpg"
            Image.new("CMYK", (4, 4)).save(image_path)
            fallback = Path(tmp) / "fallback.icc"
            fallback.write_bytes(b"exists")

            inspection = inspect_task_input(image_path, cmyk_fallback_icc=fallback)

            self.assertIsInstance(inspection, TaskInspection)

    def test_cmyk_without_embedded_icc_is_rejected_when_fallback_file_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            image_path = Path(tmp) / "cmyk.jpg"
            Image.new("CMYK", (4, 4)).save(image_path)
            missing_fallback = Path(tmp) / "does-not-exist.icc"

            with self.assertRaisesRegex(TaskInspectionError, "CMYK file has no embedded ICC"):
                inspect_task_input(image_path, cmyk_fallback_icc=missing_fallback)


class TaskInspectionExifOrientationTests(TestCase):
    def test_exif_orientation_affects_size_and_preview(self) -> None:
        # A camera-style JPEG stored rotated (Orientation=6) must be inspected
        # upright: the mm size and the preview both follow what viewers show.
        with tempfile.TemporaryDirectory() as tmp:
            image_path = Path(tmp) / "rotated.jpg"
            exif = Image.Exif()
            exif[0x0112] = 6
            Image.new("RGB", (40, 20), (200, 10, 10)).save(image_path, exif=exif.tobytes(), dpi=(100, 100))

            inspection = inspect_task_input(image_path)

            self.assertEqual(inspection.display_size_mm, "5 × 10 mm")
            with Image.open(BytesIO(inspection.preview_bytes)) as preview:
                self.assertEqual(preview.size, (20, 40))
