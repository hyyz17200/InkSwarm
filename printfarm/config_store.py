from __future__ import annotations

from pathlib import Path
import json
from typing import Any, Iterable

from .models import AppPaths, PresetConfig, TaskItem, WorkerConfig


class ConfigStore:
    def __init__(self, root: Path):
        self.paths = AppPaths(
            root=root.resolve(),
            workers_dir=(root / "Workers").resolve(),
            cache_dir=(root / "cache").resolve(),
            logs_dir=(root / "logs").resolve(),
            statistics_dir=(root / "statistics").resolve(),
            preview_dir=(root / "cache" / "previews").resolve(),
            settings_file=(root / "app_settings.json").resolve(),
        )
        self.paths.cache_dir.mkdir(parents=True, exist_ok=True)
        self.paths.logs_dir.mkdir(parents=True, exist_ok=True)
        self.paths.statistics_dir.mkdir(parents=True, exist_ok=True)
        self.paths.preview_dir.mkdir(parents=True, exist_ok=True)
        self.task_session_file = (self.paths.root / "task_session.json").resolve()

    def default_group_dir(self) -> Path:
        return (self.paths.root / "Workers").resolve()

    def worker_group_dir(self, group_name: str | None) -> Path:
        if not group_name:
            return self.default_group_dir()
        return (self.paths.root / group_name).resolve()

    def list_worker_groups(self) -> list[str]:
        names: list[str] = []
        for entry in sorted(self.paths.root.iterdir()):
            if not entry.is_dir():
                continue
            if entry.name == "workers":
                names.append(entry.name)
            elif entry.name.startswith("Workers"):
                names.append(entry.name)
        if not names:
                names = [self.default_group_dir().name]
        return names

    def _worker_config_exists_anywhere(self) -> bool:
        for group_name in self.list_worker_groups():
            group_dir = self.worker_group_dir(group_name)
            for directory in group_dir.iterdir():
                if not directory.is_dir():
                    continue
                if (directory / "worker.json").exists():
                    return True
        return False

    def ensure_sample_worker(self, group_dir: Path | None = None) -> None:
        target_group = (group_dir or self.default_group_dir()).resolve()
        sample_dir = target_group / "SampleWorker"
        preset_dir = sample_dir / "presets"
        preset_dir.mkdir(parents=True, exist_ok=True)
        worker_json = sample_dir / "worker.json"
        preset_json = preset_dir / "default.json"
        if not worker_json.exists():
            worker_json.write_text(
                json.dumps(
                    {
                        "name": "SampleWorker",
                        "printer_name": "Microsoft Print to PDF",
                        "enabled": False,
                        "weight": 1,
                        "active_preset": "default",
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        if not preset_json.exists():
            preset_json.write_text(
                json.dumps(
                    {
                        "name": "default",
                        "dpi": 300,
                        "fit_mode": "actual",
                        "rendering_intent": "relative_colorimetric",
                        "input_icc": "",
                        "output_icc": "",
                        "printui_restore_file": "",
                        "black_point_compensation": False,
                        "notes": "示例预设",
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

    def _load_presets_for_worker(self, directory: Path) -> dict[str, PresetConfig]:
        presets_dir = directory / "presets"
        presets_dir.mkdir(parents=True, exist_ok=True)
        presets: dict[str, PresetConfig] = {}
        for preset_file in sorted(presets_dir.glob("*.json")):
            preset_raw = json.loads(preset_file.read_text(encoding="utf-8"))
            preset = PresetConfig.from_dict(preset_raw, file_path=preset_file)
            presets[preset.name] = preset
        return presets

    def load_workers(self, group_name: str | None = None) -> list[WorkerConfig]:
        group_dir = self.worker_group_dir(group_name)
        group_dir.mkdir(parents=True, exist_ok=True)
        workers: list[WorkerConfig] = []
        candidate_dirs = [p for p in group_dir.iterdir() if p.is_dir()]
        for directory in sorted(candidate_dirs):
            worker_file = directory / "worker.json"
            if not worker_file.exists():
                continue
            raw = json.loads(worker_file.read_text(encoding="utf-8"))
            presets = self._load_presets_for_worker(directory)
            workers.append(WorkerConfig.from_dict(raw, directory=directory, presets=presets))

        if not workers and not self._worker_config_exists_anywhere():
            self.ensure_sample_worker(group_dir)
            return self.load_workers(group_name)
        return workers

    def save_worker(self, worker: WorkerConfig) -> None:
        worker.directory.mkdir(parents=True, exist_ok=True)
        worker.preset_dir.mkdir(parents=True, exist_ok=True)
        worker.worker_file.write_text(json.dumps(worker.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
        existing_files = {p.resolve() for p in worker.preset_dir.glob("*.json")}
        written_files: set[Path] = set()
        for preset in worker.presets.values():
            target = preset.file_path or (worker.preset_dir / f"{preset.name}.json")
            preset.file_path = target
            target.write_text(json.dumps(preset.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
            written_files.add(target.resolve())
        for old_file in existing_files - written_files:
            try:
                old_file.unlink(missing_ok=True)
            except Exception:
                pass

    def save_workers(self, workers: Iterable[WorkerConfig]) -> None:
        for worker in workers:
            self.save_worker(worker)

    def load_app_settings(self) -> dict[str, Any]:
        defaults = {
            "ui_scale": 100,
            "auto_clear_cache_on_start": False,
            "font_family": "Segoe UI",
            "active_worker_group": self.default_group_dir().name,
            "save_tasks_on_exit": False,
            "show_debug_console": False,
            "auto_orient_enabled": False,
            "target_orientation": "portrait",
            "ignore_margins": True,
            "worker_queue_limit_enabled": False,
            "worker_queue_limit": 3,
            "rip_limit_enabled": True,
            "rip_limit_ppi": 300,
            "font_engine": "auto",
        }
        if not self.paths.settings_file.exists():
            return defaults
        try:
            data = json.loads(self.paths.settings_file.read_text(encoding="utf-8"))
            defaults.update(data)
            return defaults
        except Exception:
            return defaults

    def save_app_settings(self, data: dict[str, Any]) -> None:
        self.paths.settings_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    def save_task_session(self, tasks: list[TaskItem]) -> None:
        payload = []
        for task in tasks:
            payload.append({
                "file_path": str(task.file_path),
                "copies": int(task.copies),
            })
        self.task_session_file.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def load_task_session(self) -> list[dict[str, Any]]:
        if not self.task_session_file.exists():
            return []
        try:
            data = json.loads(self.task_session_file.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def clear_task_session(self) -> None:
        try:
            self.task_session_file.unlink(missing_ok=True)
        except Exception:
            pass
