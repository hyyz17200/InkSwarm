from __future__ import annotations

from dataclasses import dataclass, field
import threading
import time
from typing import Callable, Sequence

from .debug_logger import debug_exception, debug_log
from .i18n import normalize_language, translate
from .spooler import PrinterQueueSnapshot, read_printer_queue_snapshot
from .spooler_service import SpoolerMaintenanceCancelled


STAGE_WAITING_SENDS = "waiting_sends"
STAGE_DRAINING = "draining"
STAGE_RESTARTING = "restarting"


class MaintenanceControl:
    """User decisions for the staged queue restart, set by the dialog buttons.

    Every wait in the flow is indefinite by design: instead of hidden
    timeouts, the dialog shows what the flow is waiting for and the user
    decides — force-kill a stuck submission, skip the queue drain, or cancel.
    The flow thread only ever polls these events.
    """

    def __init__(self) -> None:
        self.cancel = threading.Event()
        self.force_kill = threading.Event()
        self.skip_drain = threading.Event()


@dataclass(frozen=True)
class MaintenanceStatus:
    """One progress tick shown by the maintenance dialog."""

    stage: str
    elapsed_seconds: float
    pending_sends: int = 0
    printers: tuple[PrinterQueueSnapshot, ...] = ()


@dataclass
class MaintenanceOutcome:
    ok: bool = False
    cancelled: bool = False
    error: str | None = None
    was_running: bool = False
    killed_submits: int = 0
    skipped_drain: bool = False
    residual_jobs: int = 0


class SpoolerMaintenanceFlow:
    """Stage engine for the print-queue restart.

    Stage 1 (runs only while a run is active): wait for InkSwarm's own
    in-flight submissions to finish; the user may force-kill a stuck one.
    Stage 2: wait for the workers' printers to consume the jobs already in
    the Windows queue, so the restart cannot replay a half-transferred job;
    the user may skip this and restart with residual jobs (the spooler
    resumes them after the restart — they are never deleted, keeping what
    reaches paper consistent with the statistics). Stage 3: restart the
    Spooler service; this is the only stage that cannot be cancelled.
    """

    def __init__(
        self,
        *,
        controller,
        printer_names: Sequence[str],
        control: MaintenanceControl,
        restart: Callable[[Callable[[str], None]], None],
        on_status: Callable[[MaintenanceStatus], None],
        on_log: Callable[[str], None],
        was_running: bool,
        language: str = "en",
        snapshot_reader: Callable[[str], PrinterQueueSnapshot] = read_printer_queue_snapshot,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.controller = controller
        self.printer_names = [name for name in printer_names if name.strip()]
        self.control = control
        self.restart = restart
        self.on_status = on_status
        self.on_log = on_log
        self.was_running = was_running
        self.language = normalize_language(language)
        self.snapshot_reader = snapshot_reader
        self.sleep = sleep
        self.clock = clock

    def _t(self, key: str, **kwargs: object) -> str:
        return translate(self.language, key, **kwargs)

    def run(self) -> MaintenanceOutcome:
        outcome = MaintenanceOutcome(was_running=self.was_running)
        try:
            if self.was_running and not self._wait_sends_idle(outcome):
                outcome.cancelled = True
                return outcome
            if not self._drain_queues(outcome):
                outcome.cancelled = True
                return outcome
            self.on_status(MaintenanceStatus(stage=STAGE_RESTARTING, elapsed_seconds=0.0))
            self.restart(self.on_log)
        except SpoolerMaintenanceCancelled as exc:
            outcome.cancelled = True
            outcome.error = str(exc)
            return outcome
        except Exception as exc:
            debug_exception("SpoolerMaintenanceFlow.run", exc)
            outcome.error = str(exc)
            return outcome
        outcome.ok = True
        return outcome

    def _wait_sends_idle(self, outcome: MaintenanceOutcome) -> bool:
        """Stage 1: returns False only when the user cancelled."""
        self.on_log(self._t("spool.pause_wait_log"))
        started = self.clock()
        while True:
            if self.control.cancel.is_set():
                return False
            if self.control.force_kill.is_set():
                self.control.force_kill.clear()
                killed = self.controller.force_terminate_in_flight()
                outcome.killed_submits += killed
                self.on_log(self._t("spool.force_kill_log", count=killed))
            self.on_status(
                MaintenanceStatus(
                    stage=STAGE_WAITING_SENDS,
                    elapsed_seconds=self.clock() - started,
                    pending_sends=self.controller.active_spool_send_count(),
                )
            )
            # Short slices instead of one long wait so the loop stays
            # responsive to the user's cancel/force-kill decisions.
            if self.controller.wait_until_paused_idle(timeout_seconds=0.5):
                self.on_log(self._t("spool.paused_log"))
                return True

    def _drain_queues(self, outcome: MaintenanceOutcome) -> bool:
        """Stage 2: returns False only when the user cancelled."""
        if not self.printer_names:
            return True
        self.on_log(self._t("spool.drain_wait_log", printers=", ".join(self.printer_names)))
        started = self.clock()
        while True:
            if self.control.cancel.is_set():
                return False
            snapshots = tuple(self.snapshot_reader(name) for name in self.printer_names)
            total_jobs = sum(snapshot.job_count for snapshot in snapshots)
            if total_jobs == 0:
                self.on_log(self._t("spool.drain_all_clear"))
                return True
            if self.control.skip_drain.is_set():
                self.control.skip_drain.clear()
                outcome.skipped_drain = True
                outcome.residual_jobs = total_jobs
                residual = ", ".join(
                    f"{snapshot.printer_name} ×{snapshot.job_count}"
                    for snapshot in snapshots
                    if snapshot.job_count
                )
                debug_log(f"maintenance drain skipped residual_jobs={total_jobs} ({residual})")
                self.on_log(self._t("spool.skip_drain_log", count=total_jobs, printers=residual))
                return True
            self.on_status(
                MaintenanceStatus(
                    stage=STAGE_DRAINING,
                    elapsed_seconds=self.clock() - started,
                    printers=snapshots,
                )
            )
            self.sleep(1.0)
