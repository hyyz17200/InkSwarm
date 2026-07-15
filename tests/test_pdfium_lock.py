from unittest import TestCase

from printfarm import renderer, task_inspector
from printfarm.pdfium_lock import PDFIUM_LOCK


class PdfiumLockTests(TestCase):
    def test_renderer_and_task_inspector_share_process_wide_lock(self) -> None:
        self.assertIs(renderer.PDFIUM_LOCK, PDFIUM_LOCK)
        self.assertIs(task_inspector.PDFIUM_LOCK, PDFIUM_LOCK)
