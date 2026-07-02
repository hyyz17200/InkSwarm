from __future__ import annotations

import sys
import unittest

from printfarm.print_helper import PrintHelperClient


@unittest.skipUnless(sys.platform == "win32", "print helper requires Windows printing APIs")
class PrintHelperProcessTests(unittest.TestCase):
    """End-to-end tests against a real helper subprocess (no real printer used)."""

    def test_unknown_printer_reports_error_and_helper_stays_alive(self) -> None:
        client = PrintHelperClient(language="en")
        self.addCleanup(client.terminate)
        client.ensure_started()
        for job_name in ("test job 1", "test job 2"):
            with self.assertRaises(RuntimeError):
                client.print_copy(
                    printer_name="InkSwarm-NoSuchPrinter-__test__",
                    job_name=job_name,
                    page_paths=[],
                    page_specs=[],
                    ignore_margins=True,
                    fit_mode="actual",
                )
        # Two consecutive errors were both answered: the helper survived the
        # first failure instead of dying with the failed command.
        client.close()

    def test_terminate_surfaces_as_force_stopped_error(self) -> None:
        client = PrintHelperClient(language="en")
        client.ensure_started()
        client.terminate()
        with self.assertRaisesRegex(RuntimeError, "force-stopped"):
            client.print_copy(
                printer_name="InkSwarm-NoSuchPrinter-__test__",
                job_name="after terminate",
                page_paths=[],
                page_specs=[],
                ignore_margins=True,
                fit_mode="actual",
            )
