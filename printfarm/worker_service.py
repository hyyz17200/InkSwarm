from __future__ import annotations

from pathlib import Path
from typing import Iterable

from .config_store import ConfigStore
from .i18n import normalize_language, translate
from .models import WorkerConfig
from .printui import (
    open_printer_preferences,
    open_printer_properties,
    restore_printer_settings,
    save_printer_settings,
)


class WorkerValidationError(RuntimeError):
    pass


def validate_unique_worker_names(workers: Iterable[WorkerConfig], language: str = "en") -> None:
    language = normalize_language(language)
    seen: dict[str, WorkerConfig] = {}
    errors: list[str] = []
    for worker in workers:
        raw_name = str(worker.name or "")
        display_name = raw_name.strip()
        normalized_name = display_name.casefold()
        if not normalized_name:
            errors.append(translate(language, "worker.validation.empty_name", directory=worker.directory))
            continue
        previous = seen.get(normalized_name)
        if previous is not None:
            errors.append(
                translate(
                    language,
                    "worker.validation.duplicate_name",
                    name=display_name,
                    first_directory=previous.directory,
                    second_directory=worker.directory,
                )
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

    @property
    def last_load_errors(self) -> tuple[tuple[Path, str], ...]:
        return tuple(self.store.last_worker_load_errors)

    def save_workers(self, workers: Iterable[WorkerConfig]) -> None:
        self.store.save_workers(workers)

    @staticmethod
    def validate_workers(workers: Iterable[WorkerConfig], language: str = "en") -> None:
        validate_unique_worker_names(workers, language=language)

    def restore_preset_if_any(
        self,
        worker: WorkerConfig,
        initialize_defaults: bool = True,
        language: str = "en",
    ) -> str | None:
        language = normalize_language(language)
        preset = worker.get_active_preset()
        snapshot_path = worker.resolve_path(preset.printui_restore_file) if preset.printui_restore_file else None
        if snapshot_path and snapshot_path.exists():
            restore_printer_settings(
                worker.printer_name,
                snapshot_path,
                initialize_defaults=initialize_defaults,
                language=language,
            )
            return translate(language, "worker.preset_restored", worker_name=worker.name, preset_name=preset.name)
        return None

    @staticmethod
    def open_preferences(worker: WorkerConfig, language: str = "en") -> None:
        open_printer_preferences(worker.printer_name, language=language)

    @staticmethod
    def open_properties(worker: WorkerConfig, language: str = "en") -> None:
        open_printer_properties(worker.printer_name, language=language)

    def capture_snapshot(
        self,
        worker: WorkerConfig,
        initialize_defaults: bool = True,
        language: str = "en",
    ) -> Path:
        preset = worker.get_active_preset()
        snapshot_path = worker.resolve_path(preset.printui_restore_file) if preset.printui_restore_file else None
        if snapshot_path is None:
            snapshot_path = worker.directory / f"{preset.name}.dat"
            preset.printui_restore_file = snapshot_path.name
        save_printer_settings(
            worker.printer_name,
            snapshot_path,
            initialize_defaults=initialize_defaults,
            language=language,
        )
        self.store.save_worker(worker)
        return snapshot_path
