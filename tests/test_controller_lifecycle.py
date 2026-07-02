from __future__ import annotations

from pathlib import Path
import tempfile
import threading
import time
from unittest import TestCase, mock

from PySide6.QtCore import Qt

from printfarm import controller as controller_module
from printfarm.controller import (
    RUN_STATE_IDLE,
    RUN_STATE_RUNNING,
    RUN_STATE_STOPPING,
    PrintController,
)
from printfarm.models import RenderArtifact, RunOptions, TaskItem, WorkerConfig


def _wait_until(predicate, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


class _FakeRenderer:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def ensure_render_cache(self, task, worker) -> RenderArtifact:
        return RenderArtifact(cache_dir=Path("fake-cache"), page_paths=[], metadata={"pages": []})


class _SpoolerControl:
    """Shared handle that lets a test observe and unblock the fake print client."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.send_started = threading.Event()
        self.copies_sent = 0


class _FakeSpooler:
    """WorkerRuntime only uses the spooler for env checks and queue polling."""

    def __init__(self, language: str = "en") -> None:
        pass

    @staticmethod
    def validate_environment(language: str = "en") -> None:
        return None


def _make_fake_print_client(control: _SpoolerControl, block: bool):
    class _FakePrintClient:
        def __init__(self, language: str = "en", log_callback=None) -> None:
            self._terminated = False

        def print_copy(
            self,
            printer_name,
            job_name,
            page_paths,
            page_specs,
            ignore_margins,
            fit_mode,
            reuse_pages=False,
        ) -> None:
            control.send_started.set()
            if block and not control.release.wait(timeout=30):
                raise RuntimeError("test print client was never released")
            if self._terminated:
                # Mirrors the real client: a killed helper surfaces as an error.
                raise RuntimeError("print helper terminated")
            control.copies_sent += 1

        def terminate(self) -> None:
            self._terminated = True
            control.release.set()

        def close(self) -> None:
            pass

    return _FakePrintClient


def _task(copies: int = 1) -> TaskItem:
    return TaskItem(file_path=Path("job.pdf"), copies=copies)


def _worker(name: str = "W1") -> WorkerConfig:
    return WorkerConfig(
        name=name,
        directory=Path(name),
        printer_name=f"Printer {name}",
        enabled=True,
        weight=1,
        active_preset="",
    )


class RunLifecycleTests(TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        self.controller = PrintController(root / "cache", root / "statistics")
        self.states: list[str] = []
        self.controller.signals.run_state.connect(self.states.append, Qt.ConnectionType.DirectConnection)

    def _run_with_fakes(self, control: _SpoolerControl, block: bool, run_options: RunOptions) -> None:
        self.addCleanup(control.release.set)
        for target, replacement in (
            ("Renderer", _FakeRenderer),
            ("PrinterSpooler", _FakeSpooler),
            ("PrintHelperClient", _make_fake_print_client(control, block)),
        ):
            patcher = mock.patch.object(controller_module, target, replacement)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.controller.start([_task()], [_worker()], run_options)

    def _assert_truthful_stop(self, control: _SpoolerControl) -> None:
        self.assertTrue(control.send_started.wait(10), "fake spooler send never started")
        self.controller.stop()
        self.assertIn(RUN_STATE_STOPPING, self.states)
        self.assertNotIn(RUN_STATE_IDLE, self.states)
        self.assertTrue(self.controller.is_running())
        # Wait past the controller's 2 s grace join: the old behavior reported
        # "not running" here while the worker thread was still inside a send.
        time.sleep(2.5)
        self.assertNotIn(RUN_STATE_IDLE, self.states)
        self.assertTrue(self.controller.is_running())
        control.release.set()
        self.assertTrue(_wait_until(lambda: RUN_STATE_IDLE in self.states))
        self.assertTrue(_wait_until(lambda: not self.controller.is_running()))
        self.assertEqual(self.states, [RUN_STATE_RUNNING, RUN_STATE_STOPPING, RUN_STATE_IDLE])

    def test_stop_with_blocked_send_static_mode_reports_stopping_until_worker_exits(self) -> None:
        control = _SpoolerControl()
        self._run_with_fakes(control, block=True, run_options=RunOptions())
        self._assert_truthful_stop(control)

    def test_stop_with_blocked_send_tail_balance_mode_reports_stopping_until_worker_exits(self) -> None:
        control = _SpoolerControl()
        self._run_with_fakes(
            control,
            block=True,
            run_options=RunOptions(tail_balance_enabled=True, tail_balance_idle_seconds=1),
        )
        self._assert_truthful_stop(control)

    def test_force_stop_kills_blocked_send_and_reaches_idle(self) -> None:
        control = _SpoolerControl()
        self._run_with_fakes(control, block=True, run_options=RunOptions())
        self.assertTrue(control.send_started.wait(10), "fake print client send never started")
        self.controller.stop()
        self.assertIn(RUN_STATE_STOPPING, self.states)
        self.assertTrue(self.controller.is_running())
        self.controller.force_stop()
        self.assertTrue(_wait_until(lambda: RUN_STATE_IDLE in self.states))
        self.assertTrue(_wait_until(lambda: not self.controller.is_running()))
        # The killed copy was never recorded as a success.
        self.assertEqual(control.copies_sent, 0)
        self.assertEqual(self.states, [RUN_STATE_RUNNING, RUN_STATE_STOPPING, RUN_STATE_IDLE])

    def test_normal_completion_goes_straight_to_idle(self) -> None:
        control = _SpoolerControl()
        self._run_with_fakes(control, block=False, run_options=RunOptions())
        self.assertTrue(_wait_until(lambda: RUN_STATE_IDLE in self.states))
        self.assertTrue(_wait_until(lambda: not self.controller.is_running()))
        self.assertEqual(self.states, [RUN_STATE_RUNNING, RUN_STATE_IDLE])
        self.assertEqual(control.copies_sent, 1)
        # A stop after the run ended must not resurrect a stale STOPPING state.
        self.controller.stop()
        self.assertEqual(self.states, [RUN_STATE_RUNNING, RUN_STATE_IDLE])
