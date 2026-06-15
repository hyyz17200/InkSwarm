from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import queue
import threading
import time

from PySide6.QtCore import QObject, Signal

from .i18n import normalize_language, translate
from .models import LogMessage, RunOptions, TaskItem, TaskStatusMessage, WorkerConfig, WorkerStatusMessage, WorkerTaskBatch
from .debug_logger import debug_exception, debug_log
from .local_logger import QUEUE_LIMIT_LOG_KEY
from .printer_access import PrinterAccessCoordinator
from .printui import restore_printer_settings
from .renderer import Renderer
from .scheduler import WeightedScheduler
from .spooler import PrinterSpooler
from .statistics_writer import MonthlyStatisticsWriter, StatisticsTaskRecord
from .tail_balance import TailBalanceAssignment, TailBalanceCoordinator


class ControllerSignals(QObject):
    log = Signal(object)
    task_status = Signal(object)
    worker_status = Signal(object)
    run_state = Signal(bool)
    pause_state = Signal(bool)
    spool_progress = Signal(int, int)


class _PauseGate:
    def __init__(self) -> None:
        self._event = threading.Event()

    def pause(self) -> None:
        self._event.set()

    def resume(self) -> None:
        self._event.clear()

    def is_paused(self) -> bool:
        return self._event.is_set()

    def wait_if_paused(
        self,
        stop_event: threading.Event,
        poll_seconds: float = 0.2,
        on_pause=None,
    ) -> bool:
        if not self._event.is_set():
            return not stop_event.is_set()
        if on_pause is not None:
            on_pause()
        while self._event.is_set():
            if stop_event.is_set():
                return False
            time.sleep(max(0.05, poll_seconds))
        return not stop_event.is_set()


class WorkerRuntime(threading.Thread):
    def __init__(
        self,
        worker: WorkerConfig,
        job_queue: queue.Queue[WorkerTaskBatch | None] | None,
        signals: ControllerSignals,
        renderer: Renderer,
        printer_access: PrinterAccessCoordinator,
        progress_callback,
        spool_send_start_callback,
        spool_send_end_callback,
        stop_event: threading.Event,
        pause_gate: _PauseGate,
        run_options: RunOptions,
        tail_coordinator: TailBalanceCoordinator | None = None,
    ) -> None:
        super().__init__(daemon=True, name=f"WorkerRuntime-{worker.name}")
        self.worker = worker
        self.job_queue = job_queue
        self.signals = signals
        self.renderer = renderer
        self.printer_access = printer_access
        self.progress_callback = progress_callback
        self.spool_send_start_callback = spool_send_start_callback
        self.spool_send_end_callback = spool_send_end_callback
        self.stop_event = stop_event
        self.pause_gate = pause_gate
        self.run_options = run_options
        self.tail_coordinator = tail_coordinator
        self.spooler: PrinterSpooler | None = None

    def _t(self, key: str, **kwargs: object) -> str:
        return translate(self.run_options.language, key, **kwargs)

    def run(self) -> None:
        self.spooler = PrinterSpooler(language=self.run_options.language)
        self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, "Idle"))
        while True:
            assignment = self._next_assignment()
            if assignment is None:
                self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, "Stopped"))
                break
            batch = assignment.batch
            try:
                if self.stop_event.is_set():
                    self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, "Stopping"))
                    continue
                if not self._wait_for_resume("Paused"):
                    self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, "Stopping"))
                    continue
                if assignment.stolen_from:
                    self.signals.log.emit(
                        LogMessage(
                            "info",
                            self._t(
                                "controller.tail_balance",
                                worker_name=self.worker.name,
                                from_worker=assignment.stolen_from,
                                file_name=batch.task.file_name(),
                                copies=batch.copies,
                            ),
                        )
                    )
                self._process_batch(batch, assignment.active_id)
            except Exception as exc:
                debug_exception(f"WorkerRuntime.run[{self.worker.name}]", exc)
                self.signals.log.emit(LogMessage("error", f"{self.worker.name}: {exc}"))
                self.signals.task_status.emit(
                    TaskStatusMessage(
                        task_id=batch.task.task_id,
                        status="Error",
                        error_message=str(exc),
                    )
                )
                self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, "Error"))
            finally:
                self._complete_assignment(assignment)

    def _next_assignment(self) -> TailBalanceAssignment | None:
        if self.tail_coordinator is not None:
            return self.tail_coordinator.get_next(self.worker.name, self.stop_event)
        assert self.job_queue is not None
        batch = self.job_queue.get()
        if batch is None:
            self.job_queue.task_done()
            return None
        return TailBalanceAssignment(batch=batch, active_id=None)

    def _complete_assignment(self, assignment: TailBalanceAssignment) -> None:
        if self.tail_coordinator is not None and assignment.active_id is not None:
            self.tail_coordinator.complete_assignment(self.worker.name, assignment.active_id)
            return
        assert self.job_queue is not None
        self.job_queue.task_done()

    def _process_batch(self, batch: WorkerTaskBatch, active_id: int | None = None) -> None:
        preset = self.worker.get_active_preset()
        self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, f"Preparing {batch.task.file_name()} ×{batch.copies}"))
        artifact = self.renderer.ensure_render_cache(batch.task, self.worker)
        self.signals.log.emit(
            LogMessage(
                "info",
                self._t(
                    "controller.using_cache",
                    worker_name=self.worker.name,
                    cache_name=artifact.cache_dir.name,
                    file_name=batch.task.file_name(),
                    copies=batch.copies,
                ),
            )
        )
        self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, f"Printing {batch.task.file_name()} ×{batch.copies}"))
        debug_log(f"worker batch start worker={self.worker.name} printer={batch.printer_name} preset={preset.name} task={batch.task.file_name()} copies={batch.copies}")
        assert self.spooler is not None
        spooler = self.spooler
        restore_file = self.worker.resolve_path(preset.printui_restore_file) if preset.printui_restore_file else None

        def before_each_copy(current_copy: int, total_copies: int) -> bool | None:
            if self.stop_event.is_set():
                raise RuntimeError(self._t("controller.stopped"))
            if not self._wait_for_resume(f"Paused {batch.task.file_name()}"):
                raise RuntimeError(self._t("controller.stopped"))
            if self.run_options.worker_queue_limit_enabled and self.run_options.worker_queue_limit > 0:
                spooler.wait_until_queue_available(
                    printer_name=batch.printer_name,
                    max_queue_jobs=self.run_options.worker_queue_limit,
                    poll_seconds=self.run_options.queue_poll_seconds,
                    stop_event=self.stop_event,
                    status_callback=lambda status: self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, status)),
                    log_callback=lambda msg: self.signals.log.emit(
                        LogMessage("info", f"{self.worker.name}: {msg}", once_key=QUEUE_LIMIT_LOG_KEY)
                    ),
                    log_cooldown_seconds=60.0,
                    pause_callback=lambda: self._wait_for_resume(f"Paused {batch.task.file_name()}"),
                    language=self.run_options.language,
                )
            if not self._wait_for_resume(f"Paused {batch.task.file_name()}"):
                raise RuntimeError(self._t("controller.stopped"))
            if self.tail_coordinator is not None and active_id is not None:
                if not self.tail_coordinator.claim_active_copy(self.worker.name, active_id):
                    return False
            self.signals.worker_status.emit(
                WorkerStatusMessage(self.worker.name, f"Printing {batch.task.file_name()} {current_copy}/{total_copies}")
            )
            return None

        def after_each_copy(current_copy: int, total_copies: int) -> None:
            self.progress_callback(batch.task.task_id, 1)

        def on_printer_wait() -> None:
            self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, f"Waiting printer {batch.printer_name}"))
            self.signals.log.emit(
                LogMessage(
                    "info",
                    self._t(
                        "controller.printer_wait",
                        worker_name=self.worker.name,
                        printer_name=batch.printer_name,
                    ),
                )
            )

        with self.printer_access.hold(
            batch.printer_name,
            owner=self.worker.name,
            wait_callback=on_printer_wait,
            stop_event=self.stop_event,
        ):
            if not self._wait_for_resume(f"Paused {batch.task.file_name()}"):
                raise RuntimeError(self._t("controller.stopped"))
            if restore_file:
                self.signals.log.emit(
                    LogMessage(
                        "info",
                        self._t(
                            "controller.restore_preset",
                            worker_name=self.worker.name,
                            preset_name=restore_file.name,
                        ),
                    )
                )
                self._run_spool_send(
                    lambda: restore_printer_settings(
                        self.worker.printer_name,
                        restore_file,
                        initialize_defaults=self.run_options.printer_defaults_check_enabled,
                        language=self.run_options.language,
                    )
                )
            spooler.print_cached_pages(
                printer_name=batch.printer_name,
                page_paths=artifact.page_paths,
                page_specs=artifact.metadata.get("pages", []),
                job_name=batch.task.file_name(),
                copies=batch.copies,
                ignore_margins=self.run_options.ignore_margins,
                before_each_copy=before_each_copy,
                after_each_copy=after_each_copy,
                before_send=lambda: self._begin_spool_send(batch.task.file_name()),
                after_send=self.spool_send_end_callback,
            )
        debug_log(f"worker batch end worker={self.worker.name} task={batch.task.file_name()} copies={batch.copies}")
        self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, "Idle"))

    def _wait_for_resume(self, paused_status: str) -> bool:
        return self.pause_gate.wait_if_paused(
            self.stop_event,
            on_pause=lambda: self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, paused_status)),
        )

    def _begin_spool_send(self, paused_label: str) -> None:
        if not self._wait_for_resume(f"Paused {paused_label}"):
            raise RuntimeError(self._t("controller.stopped"))
        self.spool_send_start_callback()

    def _run_spool_send(self, callback) -> None:
        self.spool_send_start_callback()
        try:
            callback()
        finally:
            self.spool_send_end_callback()


class PrintController:
    def __init__(self, cache_root: Path, statistics_root: Path | None = None) -> None:
        self.signals = ControllerSignals()
        self._cache_root = cache_root
        self._statistics_writer = MonthlyStatisticsWriter(statistics_root or (cache_root.parent / "statistics"))
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._pause_gate = _PauseGate()
        self._lock = threading.Lock()
        self._spool_activity = threading.Condition()
        self._active_spool_sends = 0
        self._task_targets: dict[str, int] = {}
        self._task_progress: dict[str, int] = defaultdict(int)
        self._task_started_at: dict[str, float] = {}
        self._task_file_names: dict[str, str] = {}
        self._statistics_run_id: str | None = None
        self._language = "en"
        self._spool_target = 0
        self._spool_progress = 0
        self._flush_pending_statistics("startup", emit_log=False)

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def is_paused(self) -> bool:
        return self._pause_gate.is_paused()

    def _t(self, key: str, **kwargs: object) -> str:
        return translate(self._language, key, **kwargs)

    @staticmethod
    def validate_environment(language: str = "en") -> None:
        PrinterSpooler.validate_environment(language=language)

    def start(self, tasks: list[TaskItem], workers: list[WorkerConfig], run_options: RunOptions | None = None) -> None:
        if self.is_running():
            raise RuntimeError(self._t("controller.run_already_active"))
        options = run_options or RunOptions()
        options.language = normalize_language(options.language)
        self._language = options.language
        self.validate_environment(language=options.language)
        self._flush_pending_statistics("start")
        self._stop_event.clear()
        self._set_paused(False, emit=False)
        run_started_at = time.time()
        self._statistics_run_id = self._statistics_writer.new_run_id()
        self._task_targets = {task.task_id: task.copies for task in tasks}
        self._task_progress = defaultdict(int)
        self._task_started_at = {}
        self._task_file_names = {task.task_id: task.file_name() for task in tasks}
        self._spool_target = sum(max(0, int(task.copies)) for task in tasks)
        self._spool_progress = 0
        try:
            self._statistics_writer.begin_run(
                self._statistics_run_id,
                run_started_at,
                [
                    StatisticsTaskRecord(
                        task_id=task.task_id,
                        file_name=task.file_name(),
                        requested_copies=task.copies,
                    )
                    for task in tasks
                ],
            )
        except Exception as exc:
            debug_exception("PrintController.start.statistics_begin", exc)
            self.signals.log.emit(LogMessage("warning", self._t("controller.statistics_begin_failed", error=exc)))
        with self._spool_activity:
            self._active_spool_sends = 0
        self.signals.spool_progress.emit(0, self._spool_target)
        debug_log(f"controller start tasks={len(tasks)} workers={len(workers)} spool_target={self._spool_target} options={options}")
        self._thread = threading.Thread(target=self._run, args=(tasks, workers, options), daemon=True, name="PrintControllerMain")
        self._thread.start()
        self.signals.run_state.emit(True)

    def stop(self) -> None:
        debug_log("controller stop requested")
        self._stop_event.set()
        self._set_paused(False)
        self.signals.log.emit(LogMessage("warning", self._t("controller.stop_requested")))

    def pause(self) -> None:
        if not self.is_running() or self.is_paused():
            return
        debug_log("controller pause requested")
        self._set_paused(True)
        self.signals.log.emit(LogMessage("warning", self._t("controller.pause_requested")))

    def resume(self) -> None:
        if not self.is_paused():
            return
        debug_log("controller resume requested")
        self._set_paused(False)
        self.signals.log.emit(LogMessage("info", self._t("controller.resume_requested")))

    def toggle_pause(self) -> None:
        if self.is_paused():
            self.resume()
        else:
            self.pause()

    def _set_paused(self, paused: bool, emit: bool = True) -> None:
        was_paused = self._pause_gate.is_paused()
        if paused:
            self._pause_gate.pause()
        else:
            self._pause_gate.resume()
        if emit and was_paused != paused:
            self.signals.pause_state.emit(paused)

    def _run(self, tasks: list[TaskItem], workers: list[WorkerConfig], run_options: RunOptions) -> None:
        scheduler = WeightedScheduler()
        renderer = Renderer(
            self._cache_root,
            auto_orient_enabled=run_options.auto_orient_enabled,
            target_orientation=run_options.target_orientation,
            rip_limit_enabled=run_options.rip_limit_enabled,
            rip_limit_ppi=run_options.rip_limit_ppi,
            cmyk_fallback_icc=run_options.cmyk_fallback_icc,
            language=run_options.language,
        )
        queues: dict[str, queue.Queue[WorkerTaskBatch | None]] = {}
        runtimes: list[WorkerRuntime] = []
        printer_access = PrinterAccessCoordinator(language=run_options.language)
        active_workers = [worker for worker in workers if worker.enabled and worker.printer_name.strip()]
        tail_coordinator = (
            TailBalanceCoordinator(
                active_workers,
                idle_seconds=run_options.tail_balance_idle_seconds,
                enabled=True,
                language=run_options.language,
            )
            if run_options.tail_balance_enabled
            else None
        )

        try:
            for worker in active_workers:
                q: queue.Queue[WorkerTaskBatch | None] | None = None
                if tail_coordinator is None:
                    q = queue.Queue()
                    queues[worker.name] = q
                runtime = WorkerRuntime(
                    worker=worker,
                    job_queue=q,
                    signals=self.signals,
                    renderer=renderer,
                    printer_access=printer_access,
                    progress_callback=self._record_progress,
                    spool_send_start_callback=self._begin_spool_send,
                    spool_send_end_callback=self._end_spool_send,
                    stop_event=self._stop_event,
                    pause_gate=self._pause_gate,
                    run_options=run_options,
                    tail_coordinator=tail_coordinator,
                )
                runtime.start()
                runtimes.append(runtime)

            for task in tasks:
                if self._stop_event.is_set():
                    break
                if not self._pause_gate.wait_if_paused(self._stop_event):
                    break
                started_at = time.time()
                self._task_started_at.setdefault(task.task_id, started_at)
                self._mark_statistics_task_started(task, started_at)
                self.signals.task_status.emit(TaskStatusMessage(task.task_id, "Scheduling"))
                try:
                    batches = scheduler.allocate(task, workers, language=run_options.language)
                except Exception as exc:
                    debug_exception(f"PrintController._run.allocate[{task.file_name()}]", exc)
                    self.signals.task_status.emit(TaskStatusMessage(task.task_id, "Error", error_message=str(exc)))
                    self.signals.log.emit(LogMessage("error", str(exc)))
                    continue

                summary = ", ".join(f"{batch.worker_name}×{batch.copies}" for batch in batches)
                self.signals.task_status.emit(TaskStatusMessage(task.task_id, "Queued", assigned_summary=summary))
                for batch in batches:
                    if not self._pause_gate.wait_if_paused(self._stop_event):
                        break
                    if tail_coordinator is not None:
                        tail_coordinator.add_batch(batch)
                    else:
                        queues[batch.worker_name].put(batch)
                    self.signals.log.emit(
                        LogMessage(
                            "info",
                            self._t(
                                "controller.schedule",
                                file_name=task.file_name(),
                                worker_name=batch.worker_name,
                                copies=batch.copies,
                            ),
                        )
                    )
                if self._stop_event.is_set():
                    break

            if tail_coordinator is not None:
                tail_coordinator.close_dispatch()
                tail_coordinator.wait_until_done()
            else:
                for q in queues.values():
                    q.join()
        finally:
            debug_log("controller run finalizing")
            if tail_coordinator is not None:
                tail_coordinator.close_dispatch()
            else:
                for q in queues.values():
                    q.put(None)
            for runtime in runtimes:
                runtime.join(timeout=2)
            self._set_paused(False)
            self._emit_summary(tasks)
            self._finish_statistics_run()
            self.signals.run_state.emit(False)

    def _begin_spool_send(self) -> None:
        with self._spool_activity:
            self._active_spool_sends += 1
            debug_log(f"controller active spool sends={self._active_spool_sends}")

    def _end_spool_send(self) -> None:
        with self._spool_activity:
            self._active_spool_sends = max(0, self._active_spool_sends - 1)
            debug_log(f"controller active spool sends={self._active_spool_sends}")
            self._spool_activity.notify_all()

    def wait_until_paused_idle(self, timeout_seconds: float = 15.0, quiet_seconds: float = 0.3) -> bool:
        deadline = time.monotonic() + max(0.1, timeout_seconds)
        quiet_started: float | None = None
        while True:
            with self._spool_activity:
                active_sends = self._active_spool_sends
            now = time.monotonic()
            if active_sends <= 0:
                if quiet_started is None:
                    quiet_started = now
                if now - quiet_started >= max(0.0, quiet_seconds):
                    return True
            else:
                quiet_started = None
            if now >= deadline:
                return False
            time.sleep(0.05)

    def _record_progress(self, task_id: str, copies_done: int) -> None:
        total = self._task_targets.get(task_id, 0)
        file_name = self._task_file_names.get(task_id, task_id)
        run_id = self._statistics_run_id
        if run_id is not None:
            try:
                self._statistics_writer.record_success(
                    run_id=run_id,
                    task_id=task_id,
                    file_name=file_name,
                    requested_copies=total,
                    copies_done=copies_done,
                )
            except Exception as exc:
                debug_exception(f"PrintController._record_progress.statistics[{file_name}]", exc)
                self.signals.log.emit(
                    LogMessage("warning", self._t("controller.statistics_snapshot_failed", file_name=file_name, error=exc))
                )
        with self._lock:
            self._task_progress[task_id] += copies_done
            done = self._task_progress[task_id]
            self._spool_progress += copies_done
            spool_done = self._spool_progress
            spool_total = self._spool_target
        self.signals.spool_progress.emit(spool_done, spool_total)
        status = "Done" if done >= total else f"Printing {done}/{total}"
        self.signals.task_status.emit(TaskStatusMessage(task_id=task_id, status=status, completed_copies=done))

    def _mark_statistics_task_started(self, task: TaskItem, started_at: float) -> None:
        run_id = self._statistics_run_id
        if run_id is None:
            return
        try:
            self._statistics_writer.mark_task_started(
                run_id=run_id,
                task_id=task.task_id,
                file_name=task.file_name(),
                requested_copies=task.copies,
                started_at_ts=started_at,
            )
        except Exception as exc:
            debug_exception(f"PrintController._mark_statistics_task_started[{task.file_name()}]", exc)
            self.signals.log.emit(
                LogMessage(
                    "warning",
                    self._t("controller.statistics_task_start_failed", file_name=task.file_name(), error=exc),
                )
            )

    def _finish_statistics_run(self) -> None:
        run_id = self._statistics_run_id
        self._statistics_run_id = None
        if run_id is None:
            return
        try:
            result = self._statistics_writer.finish_run(run_id)
        except Exception as exc:
            debug_exception("PrintController._finish_statistics_run", exc)
            self.signals.log.emit(LogMessage("warning", self._t("controller.statistics_sync_failed", error=exc)))
            return
        if not result.ok:
            detail = "; ".join(result.errors) or self._t("controller.pending_runs", count=result.pending_runs)
            debug_log(f"statistics flush pending context=finish detail={detail}")
            self.signals.log.emit(LogMessage("warning", self._t("controller.statistics_csv_deferred", detail=detail)))
        elif result.flushed_runs:
            debug_log(f"statistics flush complete context=finish flushed_runs={result.flushed_runs}")

    def _flush_pending_statistics(self, context: str, emit_log: bool = True) -> None:
        try:
            result = self._statistics_writer.flush_pending_runs()
        except Exception as exc:
            debug_exception(f"PrintController._flush_pending_statistics[{context}]", exc)
            if emit_log:
                self.signals.log.emit(LogMessage("warning", self._t("controller.history_statistics_sync_failed", error=exc)))
            return
        if not result.ok:
            detail = "; ".join(result.errors) or self._t("controller.pending_runs", count=result.pending_runs)
            debug_log(f"statistics flush pending context={context} detail={detail}")
            if emit_log:
                self.signals.log.emit(LogMessage("warning", self._t("controller.history_statistics_csv_deferred", detail=detail)))
        elif result.flushed_runs:
            debug_log(f"statistics flush complete context={context} flushed_runs={result.flushed_runs}")

    def _emit_summary(self, tasks: list[TaskItem]) -> None:
        success_tasks = 0
        failed_tasks = 0
        success_copies = 0
        failed_copies = 0
        for task in tasks:
            total = self._task_targets.get(task.task_id, task.copies)
            done = min(self._task_progress.get(task.task_id, 0), total)
            success_copies += done
            failed_copies += max(0, total - done)
            if done >= total:
                success_tasks += 1
            else:
                failed_tasks += 1
        self.signals.log.emit(
            LogMessage(
                "info",
                self._t(
                    "controller.summary",
                    success_tasks=success_tasks,
                    failed_tasks=failed_tasks,
                    success_copies=success_copies,
                    failed_copies=failed_copies,
                ),
            )
        )
