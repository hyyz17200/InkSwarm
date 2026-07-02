from __future__ import annotations

from unittest import TestCase

from printfarm.maintenance_flow import (
    STAGE_DRAINING,
    STAGE_RESTARTING,
    STAGE_WAITING_SENDS,
    MaintenanceControl,
    SpoolerMaintenanceFlow,
)
from printfarm.spooler import PrinterQueueSnapshot
from printfarm.spooler_service import SpoolerMaintenanceCancelled


class _FakeController:
    """Scripted controller: idle_script yields wait_until_paused_idle results."""

    def __init__(self, idle_script: list[bool], pending_sends: int = 1) -> None:
        self.idle_script = list(idle_script)
        self.pending_sends = pending_sends
        self.kill_calls = 0

    def wait_until_paused_idle(self, timeout_seconds: float = 15.0, quiet_seconds: float = 0.3) -> bool:
        if self.idle_script:
            return self.idle_script.pop(0)
        return True

    def active_spool_send_count(self) -> int:
        return self.pending_sends

    def force_terminate_in_flight(self) -> int:
        self.kill_calls += 1
        self.pending_sends = 0
        return 2


class _QueueScript:
    """Scripted per-poll queue depths for one printer."""

    def __init__(self, name: str, depths: list[int], **flags) -> None:
        self.name = name
        self.depths = list(depths)
        self.flags = flags

    def next_snapshot(self) -> PrinterQueueSnapshot:
        depth = self.depths.pop(0) if len(self.depths) > 1 else self.depths[0]
        return PrinterQueueSnapshot(printer_name=self.name, job_count=depth, **self.flags)


class MaintenanceFlowTests(TestCase):
    def _make_flow(
        self,
        controller,
        queue_scripts: list[_QueueScript] | None = None,
        was_running: bool = True,
        restart=None,
        control: MaintenanceControl | None = None,
    ):
        scripts = {script.name: script for script in (queue_scripts or [])}
        self.statuses = []
        self.logs = []
        self.restart_calls = []
        self.control = control or MaintenanceControl()

        def default_restart(log) -> None:
            self.restart_calls.append(True)
            log("service restarted")

        flow = SpoolerMaintenanceFlow(
            controller=controller,
            printer_names=[script.name for script in (queue_scripts or [])],
            control=self.control,
            restart=restart or default_restart,
            on_status=self.statuses.append,
            on_log=self.logs.append,
            was_running=was_running,
            language="en",
            snapshot_reader=lambda name: scripts[name].next_snapshot(),
            sleep=lambda seconds: None,
            clock=self._clock,
        )
        return flow

    def _clock(self) -> float:
        self._ticks += 1.0
        return self._ticks

    def setUp(self) -> None:
        self._ticks = 0.0

    def test_happy_path_waits_sends_then_drains_then_restarts(self) -> None:
        controller = _FakeController(idle_script=[False, True])
        flow = self._make_flow(controller, [_QueueScript("P1", [2, 1, 0])])

        outcome = flow.run()

        self.assertTrue(outcome.ok)
        self.assertFalse(outcome.cancelled)
        self.assertEqual(outcome.killed_submits, 0)
        self.assertFalse(outcome.skipped_drain)
        self.assertEqual(self.restart_calls, [True])
        stages = [status.stage for status in self.statuses]
        self.assertIn(STAGE_WAITING_SENDS, stages)
        self.assertIn(STAGE_DRAINING, stages)
        self.assertEqual(stages[-1], STAGE_RESTARTING)

    def test_force_kill_request_kills_and_flow_proceeds(self) -> None:
        controller = _FakeController(idle_script=[False, False, True])
        flow = self._make_flow(controller, [_QueueScript("P1", [0])])
        self.control.force_kill.set()

        outcome = flow.run()

        self.assertTrue(outcome.ok)
        self.assertEqual(controller.kill_calls, 1)
        self.assertEqual(outcome.killed_submits, 2)
        self.assertFalse(self.control.force_kill.is_set())

    def test_cancel_during_send_wait_aborts_before_restart(self) -> None:
        controller = _FakeController(idle_script=[False, False])
        flow = self._make_flow(controller, [_QueueScript("P1", [0])])
        self.control.cancel.set()

        outcome = flow.run()

        self.assertFalse(outcome.ok)
        self.assertTrue(outcome.cancelled)
        self.assertEqual(self.restart_calls, [])

    def test_cancel_during_drain_aborts_before_restart(self) -> None:
        controller = _FakeController(idle_script=[True])
        script = _QueueScript("P1", [5])
        flow = self._make_flow(controller, [script])

        original_reader = flow.snapshot_reader

        def cancelling_reader(name: str) -> PrinterQueueSnapshot:
            self.control.cancel.set()
            return original_reader(name)

        flow.snapshot_reader = cancelling_reader

        outcome = flow.run()

        self.assertTrue(outcome.cancelled)
        self.assertEqual(self.restart_calls, [])

    def test_skip_drain_restarts_with_residual_jobs(self) -> None:
        controller = _FakeController(idle_script=[True])
        flow = self._make_flow(
            controller,
            [_QueueScript("P1", [3]), _QueueScript("P2", [1], offline=True)],
        )
        self.control.skip_drain.set()

        outcome = flow.run()

        self.assertTrue(outcome.ok)
        self.assertTrue(outcome.skipped_drain)
        self.assertEqual(outcome.residual_jobs, 4)
        self.assertEqual(self.restart_calls, [True])
        self.assertTrue(any("P1" in message and "P2" in message for message in self.logs))

    def test_not_running_skips_the_send_wait_stage(self) -> None:
        controller = _FakeController(idle_script=[False, False, False])
        flow = self._make_flow(controller, [_QueueScript("P1", [0])], was_running=False)

        outcome = flow.run()

        self.assertTrue(outcome.ok)
        # wait_until_paused_idle was never consulted.
        self.assertEqual(len(controller.idle_script), 3)
        self.assertNotIn(STAGE_WAITING_SENDS, [status.stage for status in self.statuses])

    def test_no_observed_printers_skips_the_drain_stage(self) -> None:
        controller = _FakeController(idle_script=[True])
        flow = self._make_flow(controller, [])

        outcome = flow.run()

        self.assertTrue(outcome.ok)
        self.assertNotIn(STAGE_DRAINING, [status.stage for status in self.statuses])

    def test_uac_denial_is_reported_as_cancelled(self) -> None:
        controller = _FakeController(idle_script=[True])

        def denied(log) -> None:
            raise SpoolerMaintenanceCancelled("denied by user")

        flow = self._make_flow(controller, [_QueueScript("P1", [0])], restart=denied)

        outcome = flow.run()

        self.assertFalse(outcome.ok)
        self.assertTrue(outcome.cancelled)
        self.assertEqual(outcome.error, "denied by user")

    def test_restart_failure_is_reported_as_error(self) -> None:
        controller = _FakeController(idle_script=[True])

        def broken(log) -> None:
            raise RuntimeError("service stuck in STOP_PENDING")

        flow = self._make_flow(controller, [_QueueScript("P1", [0])], restart=broken)

        outcome = flow.run()

        self.assertFalse(outcome.ok)
        self.assertFalse(outcome.cancelled)
        self.assertIn("STOP_PENDING", outcome.error or "")

    def test_drain_status_reports_printer_snapshots(self) -> None:
        controller = _FakeController(idle_script=[True])
        flow = self._make_flow(controller, [_QueueScript("P1", [2, 0], paused=True)])

        outcome = flow.run()

        self.assertTrue(outcome.ok)
        drain_statuses = [status for status in self.statuses if status.stage == STAGE_DRAINING]
        self.assertTrue(drain_statuses)
        first = drain_statuses[0].printers[0]
        self.assertEqual((first.printer_name, first.job_count, first.paused), ("P1", 2, True))
