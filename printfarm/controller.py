from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import queue
import threading
import time

from PySide6.QtCore import QObject, Signal

from .models import LogMessage, RunOptions, TaskItem, TaskStatusMessage, WorkerConfig, WorkerStatusMessage, WorkerTaskBatch
from .debug_logger import debug_exception, debug_log
from .printui import restore_printer_settings
from .renderer import Renderer
from .scheduler import WeightedScheduler
from .spooler import PrinterSpooler
from .statistics_writer import MonthlyStatisticsWriter


class ControllerSignals(QObject):
    log = Signal(object)
    task_status = Signal(object)
    worker_status = Signal(object)
    run_state = Signal(bool)
    spool_progress = Signal(int, int)


class WorkerRuntime(threading.Thread):
    def __init__(
        self,
        worker: WorkerConfig,
        job_queue: queue.Queue[WorkerTaskBatch | None],
        signals: ControllerSignals,
        renderer: Renderer,
        progress_callback,
        stop_event: threading.Event,
        run_options: RunOptions,
    ) -> None:
        super().__init__(daemon=True, name=f"WorkerRuntime-{worker.name}")
        self.worker = worker
        self.job_queue = job_queue
        self.signals = signals
        self.renderer = renderer
        self.progress_callback = progress_callback
        self.stop_event = stop_event
        self.run_options = run_options
        self.spooler: PrinterSpooler | None = None

    def run(self) -> None:
        self.spooler = PrinterSpooler()
        self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, "Idle"))
        while True:
            batch = self.job_queue.get()
            if batch is None:
                self.job_queue.task_done()
                self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, "Stopped"))
                break
            try:
                if self.stop_event.is_set():
                    self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, "Stopping"))
                    continue
                self._process_batch(batch)
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
                self.job_queue.task_done()

    def _process_batch(self, batch: WorkerTaskBatch) -> None:
        preset = self.worker.get_active_preset()
        self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, f"Preparing {batch.task.file_name()} ×{batch.copies}"))
        restore_file = self.worker.resolve_path(preset.printui_restore_file) if preset.printui_restore_file else None
        if restore_file:
            self.signals.log.emit(LogMessage("info", f"{self.worker.name}: 恢复驱动预设 {restore_file.name}"))
            restore_printer_settings(self.worker.printer_name, restore_file)

        artifact = self.renderer.ensure_render_cache(batch.task, self.worker)
        self.signals.log.emit(
            LogMessage(
                "info",
                f"{self.worker.name}: 使用缓存 {artifact.cache_dir.name}，打印 {batch.task.file_name()} ×{batch.copies}",
            )
        )
        self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, f"Printing {batch.task.file_name()} ×{batch.copies}"))
        debug_log(f"worker batch start worker={self.worker.name} printer={batch.printer_name} preset={preset.name} task={batch.task.file_name()} copies={batch.copies}")
        assert self.spooler is not None

        def before_each_copy(current_copy: int, total_copies: int) -> None:
            if self.stop_event.is_set():
                raise RuntimeError("已停止")
            if self.run_options.worker_queue_limit_enabled and self.run_options.worker_queue_limit > 0:
                self.spooler.wait_until_queue_available(
                    printer_name=batch.printer_name,
                    max_queue_jobs=self.run_options.worker_queue_limit,
                    poll_seconds=self.run_options.queue_poll_seconds,
                    stop_event=self.stop_event,
                    status_callback=lambda status: self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, status)),
                    log_callback=lambda msg: self.signals.log.emit(LogMessage("info", f"{self.worker.name}: {msg}")),
                    log_cooldown_seconds=60.0,
                )
            self.signals.worker_status.emit(
                WorkerStatusMessage(self.worker.name, f"Printing {batch.task.file_name()} {current_copy}/{total_copies}")
            )

        def after_each_copy(current_copy: int, total_copies: int) -> None:
            self.progress_callback(batch.task.task_id, 1)

        self.spooler.print_cached_pages(
            printer_name=batch.printer_name,
            page_paths=artifact.page_paths,
            page_specs=artifact.metadata.get("pages", []),
            job_name=batch.task.file_name(),
            copies=batch.copies,
            ignore_margins=self.run_options.ignore_margins,
            before_each_copy=before_each_copy,
            after_each_copy=after_each_copy,
        )
        debug_log(f"worker batch end worker={self.worker.name} task={batch.task.file_name()} copies={batch.copies}")
        self.signals.worker_status.emit(WorkerStatusMessage(self.worker.name, "Idle"))


class PrintController:
    def __init__(self, cache_root: Path, statistics_root: Path | None = None) -> None:
        self.signals = ControllerSignals()
        self._cache_root = cache_root
        self._statistics_writer = MonthlyStatisticsWriter(statistics_root or (cache_root.parent / "statistics"))
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._task_targets: dict[str, int] = {}
        self._task_progress: dict[str, int] = defaultdict(int)
        self._task_started_at: dict[str, float] = {}
        self._task_file_names: dict[str, str] = {}
        self._task_stats_recorded: set[str] = set()
        self._spool_target = 0
        self._spool_progress = 0

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self, tasks: list[TaskItem], workers: list[WorkerConfig], run_options: RunOptions | None = None) -> None:
        if self.is_running():
            raise RuntimeError("当前已有流程正在运行")
        options = run_options or RunOptions()
        self._stop_event.clear()
        self._task_targets = {task.task_id: task.copies for task in tasks}
        self._task_progress = defaultdict(int)
        self._task_started_at = {}
        self._task_file_names = {task.task_id: task.file_name() for task in tasks}
        self._task_stats_recorded = set()
        self._spool_target = sum(max(0, int(task.copies)) for task in tasks)
        self._spool_progress = 0
        self.signals.spool_progress.emit(0, self._spool_target)
        debug_log(f"controller start tasks={len(tasks)} workers={len(workers)} spool_target={self._spool_target} options={options}")
        self._thread = threading.Thread(target=self._run, args=(tasks, workers, options), daemon=True, name="PrintControllerMain")
        self._thread.start()
        self.signals.run_state.emit(True)

    def stop(self) -> None:
        debug_log("controller stop requested")
        self._stop_event.set()
        self.signals.log.emit(LogMessage("warning", "收到停止请求，当前页完成后会结束。"))

    def _run(self, tasks: list[TaskItem], workers: list[WorkerConfig], run_options: RunOptions) -> None:
        scheduler = WeightedScheduler()
        renderer = Renderer(
            self._cache_root,
            auto_orient_enabled=run_options.auto_orient_enabled,
            target_orientation=run_options.target_orientation,
            rip_limit_enabled=run_options.rip_limit_enabled,
            rip_limit_ppi=run_options.rip_limit_ppi,
        )
        queues: dict[str, queue.Queue[WorkerTaskBatch | None]] = {}
        runtimes: list[WorkerRuntime] = []

        try:
            for worker in workers:
                if not worker.enabled or not worker.printer_name.strip():
                    continue
                q: queue.Queue[WorkerTaskBatch | None] = queue.Queue()
                queues[worker.name] = q
                runtime = WorkerRuntime(
                    worker=worker,
                    job_queue=q,
                    signals=self.signals,
                    renderer=renderer,
                    progress_callback=self._record_progress,
                    stop_event=self._stop_event,
                    run_options=run_options,
                )
                runtime.start()
                runtimes.append(runtime)

            for task in tasks:
                if self._stop_event.is_set():
                    break
                self._task_started_at.setdefault(task.task_id, time.time())
                self.signals.task_status.emit(TaskStatusMessage(task.task_id, "Scheduling"))
                try:
                    batches = scheduler.allocate(task, workers)
                except Exception as exc:
                    debug_exception(f"PrintController._run.allocate[{task.file_name()}]", exc)
                    self.signals.task_status.emit(TaskStatusMessage(task.task_id, "Error", error_message=str(exc)))
                    self.signals.log.emit(LogMessage("error", str(exc)))
                    continue

                summary = ", ".join(f"{batch.worker_name}×{batch.copies}" for batch in batches)
                self.signals.task_status.emit(TaskStatusMessage(task.task_id, "Queued", assigned_summary=summary))
                for batch in batches:
                    queues[batch.worker_name].put(batch)
                    self.signals.log.emit(
                        LogMessage("info", f"调度 {task.file_name()} -> {batch.worker_name} ×{batch.copies}")
                    )

            for q in queues.values():
                q.join()
        finally:
            debug_log("controller run finalizing")
            for q in queues.values():
                q.put(None)
            for runtime in runtimes:
                runtime.join(timeout=2)
            self._emit_summary(tasks)
            self.signals.run_state.emit(False)

    def _record_progress(self, task_id: str, copies_done: int) -> None:
        with self._lock:
            self._task_progress[task_id] += copies_done
            done = self._task_progress[task_id]
            total = self._task_targets.get(task_id, 0)
            self._spool_progress += copies_done
            spool_done = self._spool_progress
            spool_total = self._spool_target
        self.signals.spool_progress.emit(spool_done, spool_total)
        status = "Done" if done >= total else f"Printing {done}/{total}"
        self.signals.task_status.emit(TaskStatusMessage(task_id=task_id, status=status, completed_copies=done))
        if done >= total and task_id not in self._task_stats_recorded:
            started_at = self._task_started_at.get(task_id, time.time())
            file_name = self._task_file_names.get(task_id, task_id)
            try:
                self._statistics_writer.append_success(started_at, file_name, total)
            except Exception as exc:
                debug_exception(f"PrintController._record_progress.statistics[{file_name}]", exc)
                self.signals.log.emit(LogMessage("warning", f"统计写入失败: {file_name}: {exc}"))
            else:
                self._task_stats_recorded.add(task_id)
                debug_log(f"statistics append file={file_name} quantity={total} started_at={started_at}")

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
                f"流程统计: 成功任务 {success_tasks}, 失败任务 {failed_tasks}, 成功张数 {success_copies}, 失败张数 {failed_copies}",
            )
        )
