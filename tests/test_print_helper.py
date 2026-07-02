from __future__ import annotations

import sys
import threading
import time
import unittest
from unittest import mock

from printfarm import print_helper as print_helper_module
from printfarm import spooler as spooler_module
from printfarm.print_helper import PrintHelperClient

# Stand-in helper process: acknowledges startup, then on the first print
# command reports a job id and blocks forever — the shape of a GDI submit
# stuck inside a driver.
_STUCK_HELPER_STUB = (
    "import json, sys, time\n"
    "sys.stdout.write(json.dumps({'event': 'ready'}) + '\\n')\n"
    "sys.stdout.flush()\n"
    "for line in sys.stdin:\n"
    "    if json.loads(line).get('cmd') == 'exit':\n"
    "        break\n"
    "    sys.stdout.write(json.dumps({'event': 'job_started', 'job_id': 4242}) + '\\n')\n"
    "    sys.stdout.flush()\n"
    "    time.sleep(600)\n"
)


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

    def test_kill_mid_submit_deletes_the_interrupted_spooler_job(self) -> None:
        deleted: list[tuple[str, int]] = []
        logs: list[str] = []
        errors: list[BaseException] = []
        with mock.patch.object(
            print_helper_module,
            "_print_helper_command",
            lambda language: [sys.executable, "-c", _STUCK_HELPER_STUB],
        ), mock.patch.object(
            spooler_module,
            "delete_spooler_job",
            lambda printer_name, job_id: deleted.append((printer_name, job_id)),
        ):
            client = PrintHelperClient(language="en", log_callback=logs.append)
            self.addCleanup(client.terminate)

            def submit() -> None:
                try:
                    client.print_copy(
                        printer_name="Printer P1",
                        job_name="stuck job",
                        page_paths=[],
                        page_specs=[],
                        ignore_margins=True,
                        fit_mode="actual",
                    )
                except BaseException as exc:  # noqa: BLE001 - collected for asserts
                    errors.append(exc)

            worker = threading.Thread(target=submit, daemon=True)
            worker.start()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and client._inflight_job is None:
                time.sleep(0.02)
            self.assertEqual(client._inflight_job, ("Printer P1", 4242))

            client.terminate()
            worker.join(10)
            self.assertFalse(worker.is_alive())

        self.assertEqual(deleted, [("Printer P1", 4242)])
        self.assertEqual(len(errors), 1)
        self.assertIn("force-stopped", str(errors[0]))
        self.assertTrue(logs and "4242" in logs[0])
        self.assertIsNone(client._inflight_job)

    def test_kill_inflight_interrupts_submit_but_client_stays_usable(self) -> None:
        deleted: list[tuple[str, int]] = []
        errors: list[BaseException] = []
        with mock.patch.object(
            print_helper_module,
            "_print_helper_command",
            lambda language: [sys.executable, "-c", _STUCK_HELPER_STUB],
        ), mock.patch.object(
            spooler_module,
            "delete_spooler_job",
            lambda printer_name, job_id: deleted.append((printer_name, job_id)),
        ):
            client = PrintHelperClient(language="en")
            self.addCleanup(client.terminate)

            def submit() -> None:
                try:
                    client.print_copy(
                        printer_name="Printer P1",
                        job_name="stuck job",
                        page_paths=[],
                        page_specs=[],
                        ignore_margins=True,
                        fit_mode="actual",
                    )
                except BaseException as exc:  # noqa: BLE001 - collected for asserts
                    errors.append(exc)

            worker = threading.Thread(target=submit, daemon=True)
            worker.start()
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and client._inflight_job is None:
                time.sleep(0.02)
            self.assertEqual(client._inflight_job, ("Printer P1", 4242))
            killed_proc = client._proc

            client.kill_inflight()
            worker.join(10)
            self.assertFalse(worker.is_alive())

            # The interrupted submit was cleaned up and reported as force-stopped...
            self.assertEqual(deleted, [("Printer P1", 4242)])
            self.assertEqual(len(errors), 1)
            self.assertIn("force-stopped", str(errors[0]))

            # ...but unlike terminate(), the client can start a fresh helper.
            # (Compare process objects, not PIDs: Windows reuses PIDs quickly.)
            client.ensure_started()
            self.assertIsNotNone(killed_proc.poll())
            self.assertIsNot(client._proc, killed_proc)
            self.assertIsNone(client._proc.poll())

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
