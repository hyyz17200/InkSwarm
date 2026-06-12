from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from .config_store import ConfigStore
from .i18n import normalize_language, translate
from .models import SUPPORTED_INPUT_SUFFIXES, TaskItem, TaskStatusMessage
from .task_inspector import TaskInspectionError, build_preview_file, inspect_task_input


@dataclass
class SkippedTaskInput:
    file_path: Path
    reason: str


@dataclass
class AddTasksResult:
    added_count: int = 0
    skipped: list[SkippedTaskInput] = field(default_factory=list)


@dataclass
class RestoreTasksResult:
    requested_count: int = 0
    add_result: AddTasksResult = field(default_factory=AddTasksResult)


class TaskService:
    def __init__(self, store: ConfigStore) -> None:
        self.store = store

    def add_files(
        self,
        tasks: list[TaskItem],
        files: Iterable[Path],
        default_copies: int = 1,
        language: str = "en",
    ) -> AddTasksResult:
        language = normalize_language(language)
        result = AddTasksResult()
        existing_paths = {task.file_path.resolve() for task in tasks}
        copies = max(1, int(default_copies))
        for path in files:
            if not path.exists():
                result.skipped.append(SkippedTaskInput(path, translate(language, "task.skip.missing_file")))
                continue
            if path.suffix.lower() not in SUPPORTED_INPUT_SUFFIXES:
                result.skipped.append(SkippedTaskInput(path, translate(language, "task.skip.unsupported")))
                continue
            resolved = path.resolve()
            if resolved in existing_paths:
                result.skipped.append(SkippedTaskInput(resolved, translate(language, "task.skip.duplicate")))
                continue
            try:
                inspection = inspect_task_input(resolved, language=language)
            except TaskInspectionError as exc:
                result.skipped.append(SkippedTaskInput(resolved, str(exc)))
                continue

            task = TaskItem(file_path=resolved, copies=copies, display_size_mm=inspection.display_size_mm)
            preview_path = build_preview_file(self.store.paths.preview_dir, task.task_id, inspection.preview_bytes)
            task.preview_path = str(preview_path)
            tasks.append(task)
            existing_paths.add(resolved)
            result.added_count += 1
        return result

    def restore_saved_tasks(self, tasks: list[TaskItem], language: str = "en") -> RestoreTasksResult:
        session_items = self.store.load_task_session()
        if not session_items:
            return RestoreTasksResult()

        files_to_add: list[Path] = []
        copies_map: dict[Path, int] = {}
        enabled_map: dict[Path, bool] = {}
        for item in session_items:
            raw_path = item.get("file_path")
            if not raw_path:
                continue
            path = Path(raw_path)
            resolved = path.resolve()
            files_to_add.append(path)
            copies_map[resolved] = max(1, int(item.get("copies", 1)))
            enabled_map[resolved] = bool(item.get("enabled", True))

        add_result = self.add_files(tasks, files_to_add, language=language)
        for task in tasks:
            resolved = task.file_path.resolve()
            task.copies = copies_map.get(resolved, task.copies)
            task.enabled = enabled_map.get(resolved, task.enabled)
            if not task.enabled:
                task.status = "Disabled"
        return RestoreTasksResult(requested_count=len(files_to_add), add_result=add_result)

    def save_session(self, tasks: list[TaskItem]) -> None:
        self.store.save_task_session(tasks)

    def clear_session(self) -> None:
        self.store.clear_task_session()

    @staticmethod
    def set_task_copies(tasks: list[TaskItem], task_id: str, value: int) -> None:
        task = next((item for item in tasks if item.task_id == task_id), None)
        if task is not None:
            task.copies = int(value)

    @staticmethod
    def set_task_enabled(tasks: list[TaskItem], task_id: str, enabled: bool) -> TaskItem | None:
        task = next((item for item in tasks if item.task_id == task_id), None)
        if task is None:
            return None
        task.enabled = bool(enabled)
        if task.enabled:
            if task.status == "Disabled":
                task.status = "Pending"
        else:
            task.status = "Disabled"
            task.assigned_summary = ""
            task.completed_copies = 0
            task.error_message = ""
        return task

    @staticmethod
    def remove_rows(tasks: list[TaskItem], rows: Iterable[int]) -> None:
        for row in sorted(set(rows), reverse=True):
            if 0 <= row < len(tasks):
                tasks.pop(row)

    @staticmethod
    def set_rows_copies(tasks: list[TaskItem], rows: Iterable[int], value: int) -> None:
        for row in sorted(set(rows)):
            if 0 <= row < len(tasks):
                tasks[row].copies = int(value)

    @staticmethod
    def clear(tasks: list[TaskItem]) -> None:
        tasks.clear()

    @staticmethod
    def reset_for_run(tasks: list[TaskItem]) -> None:
        for task in tasks:
            task.status = "Waiting" if task.enabled else "Disabled"
            task.completed_copies = 0
            task.assigned_summary = ""
            task.error_message = ""

    @staticmethod
    def apply_status(tasks: list[TaskItem], status: TaskStatusMessage) -> TaskItem | None:
        task = next((item for item in tasks if item.task_id == status.task_id), None)
        if task is None:
            return None
        task.status = status.status
        if status.completed_copies is not None:
            task.completed_copies = status.completed_copies
        if status.assigned_summary is not None:
            task.assigned_summary = status.assigned_summary
        if status.error_message is not None:
            task.error_message = status.error_message
        return task
