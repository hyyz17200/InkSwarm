from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable
import copy

from .i18n import normalize_language
from .models import RunOptions, TaskItem, WorkerConfig
from .worker_service import validate_unique_worker_names


@dataclass
class PreparedRun:
    tasks: list[TaskItem]
    workers: list[WorkerConfig]
    run_options: RunOptions
    spool_total: int


class RunService:
    def prepare_start(self, tasks: list[TaskItem], workers: list[WorkerConfig], settings: dict[str, Any]) -> PreparedRun:
        language = normalize_language(settings.get("language", "en"))
        validate_unique_worker_names(workers, language=language)
        active_tasks = [task for task in tasks if task.enabled]
        copied_tasks = copy.deepcopy(active_tasks)
        copied_workers = copy.deepcopy(workers)
        return PreparedRun(
            tasks=copied_tasks,
            workers=copied_workers,
            run_options=self.build_run_options(settings),
            spool_total=self.spool_total(copied_tasks),
        )

    @staticmethod
    def build_run_options(settings: dict[str, Any]) -> RunOptions:
        return RunOptions(
            auto_orient_enabled=bool(settings.get("auto_orient_enabled", False)),
            target_orientation=str(settings.get("target_orientation", "portrait") or "portrait").lower(),
            ignore_margins=bool(settings.get("ignore_margins", True)),
            worker_queue_limit_enabled=bool(settings.get("worker_queue_limit_enabled", False)),
            worker_queue_limit=int(settings.get("worker_queue_limit", 3) or 3),
            tail_balance_enabled=bool(settings.get("tail_balance_enabled", False)),
            tail_balance_idle_seconds=max(1, int(settings.get("tail_balance_idle_seconds", 15) or 15)),
            rip_limit_enabled=bool(settings.get("rip_limit_enabled", True)),
            rip_limit_ppi=int(settings.get("rip_limit_ppi", 300) or 300),
            printer_defaults_check_enabled=bool(settings.get("printer_defaults_check_enabled", True)),
            language=normalize_language(settings.get("language", "en")),
        )

    @staticmethod
    def spool_total(tasks: Iterable[TaskItem]) -> int:
        return sum(max(0, int(task.copies)) for task in tasks)
