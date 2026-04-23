from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import hashlib
import json
import os
import time
import uuid


SUPPORTED_INPUT_SUFFIXES = {".pdf", ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp"}

INTENT_NAME_TO_PIL = {
    "perceptual": 0,
    "relative_colorimetric": 1,
    "saturation": 2,
    "absolute_colorimetric": 3,
}

DEFAULT_RASTER_DPI = 300


@dataclass
class TaskItem:
    file_path: Path
    copies: int = 1
    display_size_mm: str = "读取中"
    task_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    status: str = "Pending"
    assigned_summary: str = ""
    completed_copies: int = 0
    error_message: str = ""
    preview_path: str = ""

    def file_name(self) -> str:
        return self.file_path.name

    def to_row(self) -> list[str]:
        return [
            self.file_name(),
            str(self.copies),
            self.display_size_mm,
            self.status,
            self.assigned_summary,
        ]

    def to_json(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "file_path": str(self.file_path),
            "copies": self.copies,
            "display_size_mm": self.display_size_mm,
            "status": self.status,
            "assigned_summary": self.assigned_summary,
            "completed_copies": self.completed_copies,
            "error_message": self.error_message,
            "preview_path": self.preview_path,
        }


@dataclass
class PresetConfig:
    name: str
    dpi: int = 300
    fit_mode: str = "actual"
    rendering_intent: str = "relative_colorimetric"
    input_icc: str = ""
    output_icc: str = ""
    printui_restore_file: str = ""
    black_point_compensation: bool = False
    notes: str = ""
    file_path: Path | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any], file_path: Path | None = None) -> "PresetConfig":
        file_stem = file_path.stem if file_path else "preset"
        return cls(
            name=file_stem,
            dpi=int(data.get("dpi", 300)),
            fit_mode=data.get("fit_mode", "actual"),
            rendering_intent=data.get("rendering_intent", "relative_colorimetric"),
            input_icc=data.get("input_icc", ""),
            output_icc=data.get("output_icc", ""),
            printui_restore_file=data.get("printui_restore_file", ""),
            black_point_compensation=bool(data.get("black_point_compensation", False)),
            notes=data.get("notes", ""),
            file_path=file_path,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "dpi": self.dpi,
            "fit_mode": self.fit_mode,
            "rendering_intent": self.rendering_intent,
            "input_icc": self.input_icc,
            "output_icc": self.output_icc,
            "printui_restore_file": self.printui_restore_file,
            "black_point_compensation": self.black_point_compensation,
            "notes": self.notes,
        }


@dataclass
class WorkerConfig:
    name: str
    directory: Path
    printer_name: str
    enabled: bool = True
    weight: int = 1
    active_preset: str = ""
    presets: dict[str, PresetConfig] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any], directory: Path, presets: dict[str, PresetConfig]) -> "WorkerConfig":
        name = data.get("name", directory.name)
        printer_name = data.get("printer_name", "")
        fallback_preset = next(iter(presets.keys()), "")
        active_preset = data.get("active_preset") or fallback_preset
        return cls(
            name=name,
            directory=directory,
            printer_name=printer_name,
            enabled=bool(data.get("enabled", True)),
            weight=max(1, int(data.get("weight", 1))),
            active_preset=active_preset,
            presets=presets,
        )

    @property
    def worker_file(self) -> Path:
        return self.directory / "worker.json"

    @property
    def preset_dir(self) -> Path:
        return self.directory / "presets"

    def get_active_preset(self) -> PresetConfig:
        if self.active_preset in self.presets:
            return self.presets[self.active_preset]
        if self.presets:
            self.active_preset = next(iter(self.presets.keys()))
            return self.presets[self.active_preset]
        default = PresetConfig(name="default")
        self.presets[default.name] = default
        self.active_preset = default.name
        return default

    def resolve_path(self, value: str) -> Path | None:
        if not value:
            return None
        raw = Path(value)
        if raw.is_absolute():
            return raw
        return (self.directory / raw).resolve()

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "printer_name": self.printer_name,
            "enabled": self.enabled,
            "weight": self.weight,
            "active_preset": self.active_preset,
        }


@dataclass
class WorkerTaskBatch:
    task: TaskItem
    worker_name: str
    printer_name: str
    preset_name: str
    copies: int


@dataclass
class RenderArtifact:
    cache_dir: Path
    page_paths: list[Path]
    metadata: dict[str, Any]


@dataclass
class RunOptions:
    auto_orient_enabled: bool = False
    target_orientation: str = "portrait"
    ignore_margins: bool = True
    worker_queue_limit_enabled: bool = False
    worker_queue_limit: int = 0
    queue_poll_seconds: float = 5.0
    rip_limit_enabled: bool = True
    rip_limit_ppi: int = DEFAULT_RASTER_DPI


@dataclass
class AppPaths:
    root: Path
    workers_dir: Path
    cache_dir: Path
    logs_dir: Path
    statistics_dir: Path
    preview_dir: Path
    settings_file: Path


@dataclass
class WorkerStatusMessage:
    worker_name: str
    status: str


@dataclass
class TaskStatusMessage:
    task_id: str
    status: str
    completed_copies: int | None = None
    assigned_summary: str | None = None
    error_message: str | None = None


@dataclass
class LogMessage:
    level: str
    message: str
    timestamp: float = field(default_factory=time.time)

    def format(self) -> str:
        ts = time.strftime("%H:%M:%S", time.localtime(self.timestamp))
        return f"[{ts}] {self.level.upper()}: {self.message}"


def file_signature(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def stable_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def normalize_path_text(value: str) -> str:
    return os.path.normpath(value)
