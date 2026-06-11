from __future__ import annotations

from pathlib import Path
from typing import Iterable

from .config_store import ConfigStore
from .models import WorkerConfig
from .printui import (
    open_printer_preferences,
    open_printer_properties,
    restore_printer_settings,
    save_printer_settings,
)


class WorkerValidationError(RuntimeError):
    pass


def validate_unique_worker_names(workers: Iterable[WorkerConfig]) -> None:
    seen: dict[str, WorkerConfig] = {}
    errors: list[str] = []
    for worker in workers:
        raw_name = str(worker.name or "")
        display_name = raw_name.strip()
        normalized_name = display_name.casefold()
        if not normalized_name:
            errors.append(f"Worker name cannot be empty: {worker.directory}")
            continue
        previous = seen.get(normalized_name)
        if previous is not None:
            errors.append(
                f"Duplicate Worker name '{display_name}': {previous.directory} and {worker.directory}"
            )
            continue
        seen[normalized_name] = worker

    if errors:
        raise WorkerValidationError("; ".join(errors))


class WorkerService:
    def __init__(self, store: ConfigStore) -> None:
        self.store = store

    def list_groups(self) -> list[str]:
        return self.store.list_worker_groups()

    def load_workers(self, group_name: str | None = None) -> list[WorkerConfig]:
        return self.store.load_workers(group_name)

    def save_workers(self, workers: Iterable[WorkerConfig]) -> None:
        self.store.save_workers(workers)

    @staticmethod
    def validate_workers(workers: Iterable[WorkerConfig]) -> None:
        validate_unique_worker_names(workers)

    def restore_preset_if_any(self, worker: WorkerConfig, initialize_defaults: bool = True) -> str | None:
        preset = worker.get_active_preset()
        snapshot_path = worker.resolve_path(preset.printui_restore_file) if preset.printui_restore_file else None
        if snapshot_path and snapshot_path.exists():
            restore_printer_settings(worker.printer_name, snapshot_path, initialize_defaults=initialize_defaults)
            return f"已载入 {worker.name}/{preset.name} 的驱动快照。"
        return None

    @staticmethod
    def open_preferences(worker: WorkerConfig) -> None:
        open_printer_preferences(worker.printer_name)

    @staticmethod
    def open_properties(worker: WorkerConfig) -> None:
        open_printer_properties(worker.printer_name)

    def capture_snapshot(self, worker: WorkerConfig, initialize_defaults: bool = True) -> Path:
        preset = worker.get_active_preset()
        snapshot_path = worker.resolve_path(preset.printui_restore_file) if preset.printui_restore_file else None
        if snapshot_path is None:
            snapshot_path = worker.directory / f"{preset.name}.dat"
            preset.printui_restore_file = snapshot_path.name
        save_printer_settings(worker.printer_name, snapshot_path, initialize_defaults=initialize_defaults)
        self.store.save_worker(worker)
        return snapshot_path
