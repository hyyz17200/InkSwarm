from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import hashlib
import json
import os
import time
import uuid
import zlib


SUPPORTED_INPUT_SUFFIXES = {".pdf", ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp"}

INTENT_NAME_TO_PIL = {
    "perceptual": 0,
    "relative_colorimetric": 1,
    "saturation": 2,
    "absolute_colorimetric": 3,
}

DEFAULT_RASTER_DPI = 300

# Render-cache image formats. Chosen for fast I/O plus at least simple redundancy
# compression. TIFF deflate is the recommended balance (fast encode/decode, ~10x
# on real print content, never expands on incompressible data); TIFF none is the
# fastest I/O with no compression; PNG L1 is the lightweight PNG option.
CACHE_FORMAT_TIFF_DEFLATE = "TIFF_Deflate"
CACHE_FORMAT_TIFF_NONE = "TIFF_NoCompression"
CACHE_FORMAT_PNG_L1 = "PNG_L1"
CACHE_IMAGE_FORMATS = (CACHE_FORMAT_TIFF_DEFLATE, CACHE_FORMAT_TIFF_NONE, CACHE_FORMAT_PNG_L1)
DEFAULT_CACHE_IMAGE_FORMAT = CACHE_FORMAT_TIFF_DEFLATE


def normalize_cache_image_format(value: object) -> str:
    text = str(value or "").strip()
    return text if text in CACHE_IMAGE_FORMATS else DEFAULT_CACHE_IMAGE_FORMAT


def cache_image_format_spec(value: object) -> tuple[str, str, dict[str, Any]]:
    """Map a cache-format choice to (file extension, PIL format, save kwargs)."""
    fmt = normalize_cache_image_format(value)
    if fmt == CACHE_FORMAT_PNG_L1:
        return "png", "PNG", {"compress_level": 1, "optimize": False}
    if fmt == CACHE_FORMAT_TIFF_NONE:
        return "tif", "TIFF", {"compression": "none"}
    return "tif", "TIFF", {"compression": "tiff_deflate"}


# Page sizing / placement modes. This is a draw-time decision (it only changes the
# destination rectangle on the printer DC, never the rasterized bitmap), so it is a
# global run option rather than a render/cache parameter and does not invalidate the
# render cache. All modes keep the image centered on the physical page.
#   ACTUAL     : print at the file's true physical size (1:1); if it is larger than
#                the page, shrink it to fit while keeping the aspect ratio. Default.
#   ACTUAL_100 : always print at exact 100% physical size and never scale; if it is
#                larger than the page only the centered part prints and the outer
#                edge is cropped. Use when millimeter accuracy must be guaranteed.
#   FIT        : scale up or down to fit the page, keeping the aspect ratio; the whole
#                image stays visible, possibly with blank margins.
#   FILL       : scale to cover the whole page, keeping the aspect ratio; overflow is
#                cropped so no blank margin is left.
FIT_MODE_ACTUAL = "actual"
FIT_MODE_ACTUAL_100 = "actual_100"
FIT_MODE_FIT = "fit"
FIT_MODE_FILL = "fill"
FIT_MODES = (FIT_MODE_ACTUAL, FIT_MODE_ACTUAL_100, FIT_MODE_FIT, FIT_MODE_FILL)
DEFAULT_FIT_MODE = FIT_MODE_ACTUAL


def normalize_fit_mode(value: object) -> str:
    text = str(value or "").strip()
    return text if text in FIT_MODES else DEFAULT_FIT_MODE


@dataclass
class TaskItem:
    file_path: Path
    copies: int = 1
    enabled: bool = True
    display_size_mm: str = "Reading"
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
            "Enabled" if self.enabled else "Disabled",
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
            "enabled": self.enabled,
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
    rendering_intent: str = "relative_colorimetric"
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
            rendering_intent=data.get("rendering_intent", "relative_colorimetric"),
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
            "rendering_intent": self.rendering_intent,
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
    fit_mode: str = DEFAULT_FIT_MODE
    worker_queue_limit_enabled: bool = False
    worker_queue_limit: int = 0
    queue_poll_seconds: float = 5.0
    tail_balance_enabled: bool = False
    tail_balance_idle_seconds: int = 15
    rip_limit_enabled: bool = True
    rip_limit_ppi: int = DEFAULT_RASTER_DPI
    printer_defaults_check_enabled: bool = True
    cmyk_fallback_icc: str = ""
    cache_image_format: str = DEFAULT_CACHE_IMAGE_FORMAT
    language: str = "en"


@dataclass
class AppPaths:
    root: Path
    workers_dir: Path
    cache_dir: Path
    logs_dir: Path
    statistics_dir: Path
    preview_dir: Path
    settings_file: Path
    icc_dir: Path


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
    once_key: str | None = None

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


def file_content_hash(path: Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_content_crc32(path: Path, chunk_size: int = 1 << 20) -> int:
    crc = 0
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            crc = zlib.crc32(chunk, crc)
    return crc & 0xFFFFFFFF


def normalize_path_text(value: str) -> str:
    return os.path.normpath(value)


def resolve_icc_path(icc_dir: Path, value: str) -> Path | None:
    """Resolve a configured ICC file name to an absolute path under ``icc_dir``.

    A bare file name (the normal case, entered in Settings) resolves inside the
    program's ``icc`` directory; an absolute path is honored as-is. An empty
    value means "no profile configured" and maps to ``None``. Existence is not
    checked here — callers decide what an unavailable file means.
    """
    name = (value or "").strip()
    if not name:
        return None
    candidate = Path(name)
    if candidate.is_absolute():
        return candidate
    return icc_dir / candidate
