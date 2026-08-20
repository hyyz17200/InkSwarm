from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock

from printfarm import print_helper as print_helper_module
from printfarm import spooler as spooler_module
from printfarm.print_helper import PrintHelperClient


_UNICODE_PROTOCOL_HELPER_STUB = r"""
import sys
import types

spooler_module = types.ModuleType("printfarm.spooler")

class PrinterSpooler:
    def __init__(self, language):
        sys.stderr.write("辅助进程错误流\n")
        sys.stderr.flush()

    def print_single_copy(self, **kwargs):
        raise RuntimeError(f"{kwargs['job_name']}|{kwargs['page_paths'][0]}")

spooler_module.PrinterSpooler = PrinterSpooler
sys.modules["printfarm.spooler"] = spooler_module

from printfarm.print_helper import main
raise SystemExit(main([]))
"""

_RELEASE_HELPER_STUB = r"""
import sys
import types

spooler_module = types.ModuleType("printfarm.spooler")

class PrinterSpooler:
    def __init__(self, language):
        self.prepare_count = 0

    def prepare_pages(self, page_paths, page_specs):
        self.prepare_count += 1
        return [self.prepare_count]

    def print_single_copy(self, **kwargs):
        if self.prepare_count >= 2:
            raise RuntimeError(f"prepare-count={self.prepare_count}")

spooler_module.PrinterSpooler = PrinterSpooler
sys.modules["printfarm.spooler"] = spooler_module

from printfarm.print_helper import main
raise SystemExit(main([]))
"""

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

# Stand-in helper that relays a debug line (the way the real helper forwards
# its debug_log output) before confirming the copy.
_DEBUG_EVENT_HELPER_STUB = (
    "import json, sys\n"
    "sys.stdout.write(json.dumps({'event': 'ready'}) + '\\n')\n"
    "sys.stdout.flush()\n"
    "for line in sys.stdin:\n"
    "    if json.loads(line).get('cmd') == 'exit':\n"
    "        break\n"
    "    sys.stdout.write(json.dumps({'event': 'debug', 'message': 'draw page rect=(0,0,1,1)'}) + '\\n')\n"
    "    sys.stdout.write(json.dumps({'event': 'done'}) + '\\n')\n"
    "    sys.stdout.flush()\n"
)


class PrintHelperEncodingTests(unittest.TestCase):
    def test_helper_protocol_forces_utf8_for_all_standard_streams(self) -> None:
        env = os.environ.copy()
        # Reproduce a Chinese-locale child even when the test host itself uses
        # UTF-8.  main() must override this for all three redirected streams.
        env["PYTHONIOENCODING"] = "cp936:replace"
        proc = subprocess.Popen(
            [sys.executable, "-c", _UNICODE_PROTOCOL_HELPER_STUB],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="strict",
            env=env,
        )
        self.addCleanup(lambda: proc.kill() if proc.poll() is None else None)
        assert proc.stdin is not None
        assert proc.stdout is not None
        assert proc.stderr is not None

        self.assertEqual(json.loads(proc.stdout.readline()), {"event": "ready"})
        page_path = Path("缓存目录") / "页面一.bmp"
        command = {
            "cmd": "print",
            "printer_name": "测试打印机",
            "job_name": "中文作业名",
            "page_paths": [str(page_path)],
            "page_specs": [{}],
            "ignore_margins": True,
            "fit_mode": "actual",
        }
        proc.stdin.write(json.dumps(command, ensure_ascii=False) + "\n")
        proc.stdin.write('{"cmd": "exit"}\n')
        proc.stdin.flush()

        event = json.loads(proc.stdout.readline())
        self.assertEqual(event["event"], "error")
        self.assertEqual(event["message"], f"中文作业名|{page_path}")
        proc.stdin.close()
        self.assertEqual(proc.wait(timeout=10), 0)
        self.assertEqual(proc.stderr.read(), "辅助进程错误流\n")


class PrintHelperReleaseTests(unittest.TestCase):
    def test_release_command_drops_predecoded_pages_before_next_batch(self) -> None:
        with mock.patch.object(
            print_helper_module,
            "_print_helper_command",
            lambda language: [sys.executable, "-c", _RELEASE_HELPER_STUB],
        ):
            client = PrintHelperClient(language="en")
            self.addCleanup(client.terminate)
            kwargs = {
                "printer_name": "Printer P1",
                "page_paths": [Path("page.bmp")],
                "page_specs": [{}],
                "ignore_margins": True,
                "fit_mode": "actual",
                "reuse_pages": True,
            }
            client.print_copy(job_name="batch one copy 1", **kwargs)
            client.print_copy(job_name="batch one copy 2", **kwargs)
            client.release_pages()

            with self.assertRaisesRegex(RuntimeError, "prepare-count=2"):
                client.print_copy(job_name="batch two copy 1", **kwargs)
            client.close()


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
            assert killed_proc is not None

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
            started = client._proc
            assert started is not None
            self.assertIsNotNone(killed_proc.poll())
            self.assertIsNot(started, killed_proc)
            self.assertIsNone(started.poll())

    def test_debug_events_are_relayed_and_not_treated_as_protocol_answers(self) -> None:
        captured: list[str] = []
        with mock.patch.object(
            print_helper_module,
            "_print_helper_command",
            lambda language: [sys.executable, "-c", _DEBUG_EVENT_HELPER_STUB],
        ), mock.patch.object(print_helper_module, "debug_log", captured.append):
            client = PrintHelperClient(language="en")
            self.addCleanup(client.terminate)
            client.print_copy(
                printer_name="Printer P1",
                job_name="debug job",
                page_paths=[],
                page_specs=[],
                ignore_margins=True,
                fit_mode="actual",
            )
            client.close()
        self.assertTrue(any("draw page rect=(0,0,1,1)" in line for line in captured))

    def test_real_helper_forwards_spooler_debug_lines(self) -> None:
        # The helper has no debug file of its own: its submit-path debug_log
        # lines must arrive in the parent as relayed "debug" events.
        captured: list[str] = []
        with mock.patch.object(print_helper_module, "debug_log", captured.append):
            client = PrintHelperClient(language="en")
            self.addCleanup(client.terminate)
            with self.assertRaises(RuntimeError):
                client.print_copy(
                    printer_name="InkSwarm-NoSuchPrinter-__test__",
                    job_name="debug relay probe",
                    page_paths=[],
                    page_specs=[],
                    ignore_margins=True,
                    fit_mode="actual",
                )
            client.close()
        self.assertTrue(any("spooler print start" in line for line in captured))

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
