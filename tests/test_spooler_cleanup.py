from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from printfarm import spooler as spooler_module
from printfarm.spooler import PrinterSpooler


class _FailingDC:
    def __init__(self, failure_stage: str) -> None:
        self.failure_stage = failure_stage
        self.abort_calls = 0
        self.delete_calls = 0

    def CreatePrinterDC(self, printer_name: str) -> None:
        if self.failure_stage == "create":
            raise RuntimeError("printer unavailable")

    def StartDoc(self, job_name: str) -> int:
        raise RuntimeError("print failed")

    def AbortDoc(self) -> None:
        self.abort_calls += 1
        if self.failure_stage == "print":
            raise RuntimeError("abort failed")

    def DeleteDC(self) -> None:
        self.delete_calls += 1
        if self.failure_stage == "print":
            raise RuntimeError("delete failed")


class PrinterDCCleanupTests(TestCase):
    @staticmethod
    def _spooler() -> PrinterSpooler:
        instance = PrinterSpooler.__new__(PrinterSpooler)
        instance.language = "en"
        return instance

    def test_create_printer_failure_still_deletes_created_dc(self) -> None:
        dc = _FailingDC("create")
        with patch.object(spooler_module, "win32ui", SimpleNamespace(CreateDC=lambda: dc)):
            with self.assertRaisesRegex(RuntimeError, "printer unavailable"):
                self._spooler().print_single_copy("missing", [], [], "job")

        self.assertEqual(dc.abort_calls, 1)
        self.assertEqual(dc.delete_calls, 1)

    def test_cleanup_failures_do_not_mask_original_print_exception(self) -> None:
        dc = _FailingDC("print")
        with patch.object(spooler_module, "win32ui", SimpleNamespace(CreateDC=lambda: dc)):
            with self.assertRaisesRegex(RuntimeError, "print failed"):
                self._spooler().print_single_copy("printer", [], [], "job")

        self.assertEqual(dc.abort_calls, 1)
        self.assertEqual(dc.delete_calls, 1)
