from __future__ import annotations

from unittest import TestCase, mock

from printfarm import spooler as spooler_module
from printfarm.spooler import (
    PRINTER_ATTRIBUTE_WORK_OFFLINE,
    PRINTER_STATUS_ERROR,
    PRINTER_STATUS_OFFLINE,
    PRINTER_STATUS_PAUSED,
    list_printers_with_jobs,
    read_printer_queue_snapshot,
)


class _FakeWin32Print:
    """Just enough of win32print for the queue inspection helpers."""

    def __init__(self, printers: dict[str, dict], enum_failure: Exception | None = None) -> None:
        # printers: name -> {"status": int, "attributes": int, "jobs": int}
        self.printers = printers
        self.enum_failure = enum_failure
        self.open_handles: list[str] = []
        self.closed_handles: list[str] = []

    def OpenPrinter(self, name: str) -> str:
        if name not in self.printers:
            raise OSError(1801, "The printer name is invalid.")
        self.open_handles.append(name)
        return name

    def ClosePrinter(self, handle: str) -> None:
        self.closed_handles.append(handle)

    def GetPrinter(self, handle: str, level: int) -> dict:
        entry = self.printers[handle]
        if isinstance(entry.get("query_failure"), Exception):
            raise entry["query_failure"]
        return {"Status": entry.get("status", 0), "Attributes": entry.get("attributes", 0)}

    def EnumJobs(self, handle: str, first: int, count: int, level: int) -> list[dict]:
        return [{"JobId": index} for index in range(self.printers[handle].get("jobs", 0))]

    def EnumPrinters(self, flags: int, name, level: int) -> list[dict]:
        if self.enum_failure is not None:
            raise self.enum_failure
        return [{"pPrinterName": printer_name} for printer_name in self.printers]


class ReadPrinterQueueSnapshotTests(TestCase):
    def _patch(self, fake: _FakeWin32Print):
        patcher = mock.patch.object(spooler_module, "win32print", fake)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_reports_job_count_and_decodes_status_flags(self) -> None:
        fake = _FakeWin32Print(
            {
                "P1": {
                    "status": PRINTER_STATUS_OFFLINE | PRINTER_STATUS_PAUSED | PRINTER_STATUS_ERROR,
                    "jobs": 3,
                }
            }
        )
        self._patch(fake)

        snapshot = read_printer_queue_snapshot("P1")

        self.assertEqual(snapshot.printer_name, "P1")
        self.assertEqual(snapshot.job_count, 3)
        self.assertTrue(snapshot.offline)
        self.assertTrue(snapshot.paused)
        self.assertTrue(snapshot.error)
        self.assertIsNone(snapshot.unreachable)
        self.assertEqual(fake.closed_handles, ["P1"])

    def test_work_offline_attribute_counts_as_offline(self) -> None:
        self._patch(_FakeWin32Print({"P1": {"attributes": PRINTER_ATTRIBUTE_WORK_OFFLINE, "jobs": 0}}))

        snapshot = read_printer_queue_snapshot("P1")

        self.assertTrue(snapshot.offline)
        self.assertFalse(snapshot.paused)
        self.assertEqual(snapshot.job_count, 0)

    def test_unknown_printer_is_reported_unreachable(self) -> None:
        self._patch(_FakeWin32Print({}))

        snapshot = read_printer_queue_snapshot("NoSuch")

        self.assertEqual(snapshot.printer_name, "NoSuch")
        self.assertIsNotNone(snapshot.unreachable)
        self.assertEqual(snapshot.job_count, 0)

    def test_query_failure_still_closes_the_handle(self) -> None:
        fake = _FakeWin32Print({"P1": {"query_failure": OSError(5, "Access is denied."), "jobs": 9}})
        self._patch(fake)

        snapshot = read_printer_queue_snapshot("P1")

        self.assertIsNotNone(snapshot.unreachable)
        self.assertEqual(fake.closed_handles, ["P1"])


class ListPrintersWithJobsTests(TestCase):
    def _patch(self, fake: _FakeWin32Print):
        patcher = mock.patch.object(spooler_module, "win32print", fake)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_returns_only_non_excluded_printers_with_queued_jobs(self) -> None:
        self._patch(
            _FakeWin32Print(
                {
                    "Worker A": {"jobs": 4},
                    "Other Busy": {"jobs": 2},
                    "Other Idle": {"jobs": 0},
                }
            )
        )

        results = list_printers_with_jobs(exclude={"Worker A"})

        self.assertEqual([(s.printer_name, s.job_count) for s in results], [("Other Busy", 2)])

    def test_enumeration_failure_degrades_to_empty(self) -> None:
        self._patch(_FakeWin32Print({}, enum_failure=OSError(1722, "The RPC server is unavailable.")))

        self.assertEqual(list_printers_with_jobs(), [])
