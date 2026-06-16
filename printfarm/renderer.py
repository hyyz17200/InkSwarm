from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, TypedDict, cast
import json
import os
import threading
import time

import pypdfium2 as pdfium
from PIL import Image, ImageCms

from .i18n import normalize_language, translate
from .models import (
    DEFAULT_CACHE_IMAGE_FORMAT,
    INTENT_NAME_TO_PIL,
    PresetConfig,
    RenderArtifact,
    TaskItem,
    WorkerConfig,
    cache_image_format_spec,
    file_content_crc32,
    file_content_hash,
    file_signature,
    normalize_cache_image_format,
    stable_hash,
)
from .debug_logger import debug_exception, debug_log
from .task_inspector import MM_PER_INCH, PDF_POINTS_PER_INCH, get_image_dpi

Image.MAX_IMAGE_PIXELS = None
# Do NOT enable ImageFile.LOAD_TRUNCATED_IMAGES: incomplete or corrupt bitmaps must
# raise during decode so they are rejected, never printed as partial output.


IMAGE_RIP_PRESHRINK_FACTOR = 2.0


def _icc_fingerprint(path: Path | None) -> str:
    """Content hash for an ICC profile, used as part of the render-cache key.

    Identity is based on ICC *content* rather than its path/filename, so workers
    with the same model/ink/paper/ICC combination share one render cache even
    when their preset files live in different directories. A missing or unset
    profile maps to "" to match the renderer's fallback (no input/output ICC).
    """
    if path is None:
        return ""
    try:
        return file_content_hash(path)
    except OSError:
        return ""


class PageRenderInfo(TypedDict):
    file: str
    width_mm: float
    height_mm: float


class Renderer:
    def __init__(
        self,
        cache_root: Path,
        auto_orient_enabled: bool = False,
        target_orientation: str = "portrait",
        rip_limit_enabled: bool = True,
        rip_limit_ppi: int = 300,
        cmyk_fallback_icc: str = "",
        cache_image_format: str = DEFAULT_CACHE_IMAGE_FORMAT,
        language: str = "en",
    ):
        self.cache_root = cache_root.resolve()
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.auto_orient_enabled = auto_orient_enabled
        self.target_orientation = (target_orientation or "portrait").lower()
        self.rip_limit_enabled = bool(rip_limit_enabled)
        self.rip_limit_ppi = max(36, int(rip_limit_ppi or 300))
        # Cached page bitmap format. The extension/format/kwargs are derived once
        # and reused for every page save; the normalized name participates in the
        # cache key so changing the format produces a fresh, separate cache.
        self.cache_image_format = normalize_cache_image_format(cache_image_format)
        self._cache_ext, self._cache_save_format, self._cache_save_kwargs = cache_image_format_spec(
            self.cache_image_format
        )
        # Global fallback ICC used only for CMYK input that has no usable embedded
        # profile. RGB input falls back to sRGB instead, so this never affects RGB.
        self.cmyk_fallback_icc_path = Path(cmyk_fallback_icc) if cmyk_fallback_icc else None
        self.language = normalize_language(language)
        # Per-run memo of source signatures. The controller builds a fresh
        # Renderer for each run, so the CRC of each source file is computed once
        # per run (source files do not change mid-run) and recomputed next run —
        # which is what catches a same-size/same-mtime edit between runs.
        self._source_signature_cache: dict[tuple[str, int, int], dict[str, Any]] = {}
        self._source_signature_lock = threading.Lock()
        # Render serialization, scoped to this Renderer instance. The controller
        # builds a fresh Renderer per run and runs never overlap, so these lock
        # maps coordinate only the workers of the current run and are freed with
        # the Renderer when the run ends, instead of growing for the life of the
        # process. _pdf_render_locks holds one lock per source PDF path (so a file
        # is not rendered by two workers at once); _render_key_locks holds one lock
        # per cache key (so identically configured workers that share a cache
        # directory render/write it once while the others wait and then read it).
        self._pdf_render_locks: dict[str, threading.Lock] = {}
        self._pdf_render_locks_guard = threading.Lock()
        self._render_key_locks: dict[str, threading.Lock] = {}
        self._render_key_locks_guard = threading.Lock()

    def ensure_render_cache(self, task: TaskItem, worker: WorkerConfig) -> RenderArtifact:
        preset = worker.get_active_preset()
        key_payload = self._cache_key_payload(task, worker, preset)
        key = stable_hash(key_payload)
        cache_dir = self.cache_root / key

        artifact = self._load_cache_artifact(cache_dir)
        if artifact is not None:
            debug_log(f"renderer cache hit worker={worker.name} preset={preset.name} task={task.file_name()} key={key}")
            return artifact

        # Identically configured workers resolve to the same key and therefore the
        # same cache directory. Serialize per key so only one thread renders/writes
        # it while the others wait and then read the finished result.
        with self._get_render_key_lock(key):
            artifact = self._load_cache_artifact(cache_dir)
            if artifact is not None:
                debug_log(f"renderer cache hit (shared) worker={worker.name} preset={preset.name} task={task.file_name()} key={key}")
                return artifact

            cache_dir.mkdir(parents=True, exist_ok=True)
            debug_log(f"renderer cache miss worker={worker.name} preset={preset.name} task={task.file_name()} key={key}")
            if task.file_path.suffix.lower() == ".pdf":
                page_info = self._render_pdf(task.file_path, cache_dir, worker, preset)
            else:
                page_info = self._render_image_file(task.file_path, cache_dir, worker, preset)

            metadata = {
                "source": str(task.file_path),
                "preset_name": preset.name,
                "worker_name": worker.name,
                "dpi": preset.dpi,
                "rendering_intent": preset.rendering_intent,
                "black_point_compensation": bool(preset.black_point_compensation),
                "cmyk_fallback_icc_sha256": key_payload["cmyk_fallback_icc"],
                "output_icc_sha256": key_payload["output_icc"],
                "cache_image_format": self.cache_image_format,
                "rip_limit_enabled": self.rip_limit_enabled,
                "rip_limit_ppi": self.rip_limit_ppi,
                "auto_orient_enabled": self.auto_orient_enabled,
                "target_orientation": self.target_orientation,
                "image_rip_preshrink_factor": IMAGE_RIP_PRESHRINK_FACTOR,
                "cache_schema": key_payload["cache_schema"],
                "pages": page_info,
            }
            self._write_metadata_atomic(cache_dir / "metadata.json", metadata)
            return RenderArtifact(
                cache_dir=cache_dir,
                page_paths=[cache_dir / p["file"] for p in page_info],
                metadata=metadata,
            )

    def _cache_key_payload(self, task: TaskItem, worker: WorkerConfig, preset: PresetConfig) -> dict[str, Any]:
        """Build the render-cache identity.

        Only inputs that change the cached bitmap participate: the source file
        signature, the color pipeline (the global CMYK fallback ICC and the
        preset output ICC by content hash, rendering intent, black point
        compensation), the raster resolution, and the RIP/orientation options.
        Worker name, printer name, preset name/notes and ICC *paths* are
        deliberately excluded so identically configured workers share one cache.

        The CMYK fallback ICC only affects CMYK input without an embedded
        profile, but its content hash participates for every input so that
        changing the fallback in Settings is always detected. RGB and
        embedded-profile inputs are then re-rendered unnecessarily on a fallback
        change; that is a rare event and the simpler, always-correct trade-off.
        """
        output_icc_path = worker.resolve_path(preset.output_icc) if preset.output_icc else None
        return {
            "cache_schema": 4,
            "cache_image_format": self.cache_image_format,
            "source": self._source_signature(task.file_path),
            "dpi": int(preset.dpi),
            "rendering_intent": preset.rendering_intent,
            "black_point_compensation": bool(preset.black_point_compensation),
            "cmyk_fallback_icc": _icc_fingerprint(self.cmyk_fallback_icc_path),
            "output_icc": _icc_fingerprint(output_icc_path),
            "auto_orient_enabled": self.auto_orient_enabled,
            "target_orientation": self.target_orientation,
            "rip_limit_enabled": self.rip_limit_enabled,
            "rip_limit_ppi": self.rip_limit_ppi,
            "image_rip_preshrink_factor": IMAGE_RIP_PRESHRINK_FACTOR,
        }

    def _source_signature(self, path: Path) -> dict[str, Any]:
        """Source-file identity: path + size + mtime + a CRC32 of the content.

        Size and mtime alone miss a content change that preserves both (e.g. a
        timestamp-preserving copy/restore, or coarse filesystem mtime). Adding a
        fast CRC32 closes that gap; combined with size and mtime an accidental
        collision is effectively impossible. The CRC is memoized per run (see
        __init__) so the file is read once per run rather than once per worker.
        """
        signature = file_signature(path)
        memo_key = (str(signature["path"]), int(signature["size"]), int(signature["mtime_ns"]))
        with self._source_signature_lock:
            cached = self._source_signature_cache.get(memo_key)
        if cached is not None:
            return cached
        # Computed outside the lock so a large source does not block other lookups.
        signature["crc32"] = file_content_crc32(path)
        with self._source_signature_lock:
            existing = self._source_signature_cache.get(memo_key)
            if existing is not None:
                return existing
            self._source_signature_cache[memo_key] = signature
            return signature

    def _load_cache_artifact(self, cache_dir: Path) -> RenderArtifact | None:
        meta_file = cache_dir / "metadata.json"
        if not meta_file.exists():
            return None
        try:
            metadata = json.loads(meta_file.read_text(encoding="utf-8"))
            page_paths = [cache_dir / page["file"] for page in metadata["pages"]]
        except (OSError, ValueError, KeyError, TypeError):
            return None
        if not page_paths or not all(path.exists() for path in page_paths):
            return None
        return RenderArtifact(cache_dir=cache_dir, page_paths=page_paths, metadata=metadata)

    def _get_render_key_lock(self, key: str) -> threading.Lock:
        with self._render_key_locks_guard:
            lock = self._render_key_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._render_key_locks[key] = lock
            return lock

    @staticmethod
    def _write_metadata_atomic(meta_file: Path, metadata: dict[str, Any]) -> None:
        tmp_file = meta_file.with_name(f"{meta_file.name}.tmp")
        tmp_file.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp_file, meta_file)

    def _render_pdf(self, pdf_path: Path, cache_dir: Path, worker: WorkerConfig, preset: PresetConfig) -> list[PageRenderInfo]:
        page_info: list[PageRenderInfo] = []
        effective_dpi = self._effective_rip_dpi(preset.dpi)
        scale = effective_dpi / PDF_POINTS_PER_INCH
        lock = self._get_pdf_render_lock(pdf_path)
        with lock:
            document = pdfium.PdfDocument(str(pdf_path))
            try:
                for index in range(len(document)):
                    page = self._load_pdf_page_with_retry(document, index, pdf_path, worker)
                    width_pt = float(page.get_width())
                    height_pt = float(page.get_height())
                    width_mm = width_pt * MM_PER_INCH / PDF_POINTS_PER_INCH
                    height_mm = height_pt * MM_PER_INCH / PDF_POINTS_PER_INCH
                    debug_log(
                        f"render pdf worker={worker.name} preset={preset.name} page={index + 1} effective_dpi={effective_dpi} "
                        f"size_mm={width_mm:.3f}x{height_mm:.3f}"
                    )
                    bitmap = page.render(scale=cast(Any, scale), optimize_mode="print")
                    image = bitmap.to_pil()
                    image = self._apply_color_transform(image, worker, preset)
                    image, width_mm, height_mm = self._apply_orientation(image, width_mm, height_mm)
                    debug_log(
                        f"render pdf final worker={worker.name} preset={preset.name} page={index + 1} "
                        f"final_px={image.width}x{image.height} size_mm={width_mm:.3f}x{height_mm:.3f}"
                    )
                    out_path = cache_dir / f"page_{index + 1:04d}.{self._cache_ext}"
                    self._save_cache_image(image, out_path)
                    page_info.append({
                        "file": out_path.name,
                        "width_mm": round(width_mm, 3),
                        "height_mm": round(height_mm, 3),
                    })
            finally:
                document.close()
        return page_info

    def _render_image_file(self, image_path: Path, cache_dir: Path, worker: WorkerConfig, preset: PresetConfig) -> list[PageRenderInfo]:
        with Image.open(image_path) as image:
            dpi_x, dpi_y = get_image_dpi(image)
            width_mm = image.width / dpi_x * MM_PER_INCH
            height_mm = image.height / dpi_y * MM_PER_INCH
            image = self._pre_shrink_for_rip_limit(image, width_mm, height_mm, image_path, worker)
            rendered = self._apply_color_transform(image, worker, preset)
            rendered, width_mm, height_mm = self._apply_orientation(rendered, width_mm, height_mm)
            rendered = self._apply_rip_limit_to_image(rendered, width_mm, height_mm, image_path, worker)
            debug_log(
                f"render image final worker={worker.name} preset={preset.name} source={image_path.name} "
                f"final_px={rendered.width}x{rendered.height} size_mm={width_mm:.3f}x{height_mm:.3f}"
            )
            out_path = cache_dir / f"page_0001.{self._cache_ext}"
            self._save_cache_image(rendered, out_path)
        return [{"file": out_path.name, "width_mm": round(width_mm, 3), "height_mm": round(height_mm, 3)}]


    def _effective_rip_dpi(self, requested_dpi: int) -> int:
        requested = max(36, int(requested_dpi or 300))
        if not self.rip_limit_enabled:
            return requested
        return max(36, min(requested, self.rip_limit_ppi))

    def _rip_limit_size(self, width_mm: float, height_mm: float) -> tuple[int, int]:
        max_w = max(1, round(width_mm / MM_PER_INCH * self.rip_limit_ppi))
        max_h = max(1, round(height_mm / MM_PER_INCH * self.rip_limit_ppi))
        return max_w, max_h

    def _get_pdf_render_lock(self, pdf_path: Path) -> threading.Lock:
        key = str(pdf_path.resolve()).lower()
        with self._pdf_render_locks_guard:
            lock = self._pdf_render_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._pdf_render_locks[key] = lock
            return lock

    def _load_pdf_page_with_retry(self, document: pdfium.PdfDocument, index: int, pdf_path: Path, worker: WorkerConfig):
        attempts = 3
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return document[index]
            except Exception as exc:
                last_exc = exc
                debug_exception(f"Renderer._load_pdf_page_with_retry[{worker.name}:{pdf_path.name}:page{index + 1}:attempt{attempt}]", exc)
                if attempt < attempts:
                    time.sleep(0.15 * attempt)
        assert last_exc is not None
        raise last_exc

    def _apply_rip_limit_to_image(self, image: Image.Image, width_mm: float, height_mm: float, source_path: Path, worker: WorkerConfig) -> Image.Image:
        if not self.rip_limit_enabled:
            return image
        max_w, max_h = self._rip_limit_size(width_mm, height_mm)
        if image.width <= max_w and image.height <= max_h:
            return image
        debug_log(
            f"rip downscale worker={worker.name} source={source_path.name} from={image.width}x{image.height} to={max_w}x{max_h} limit_ppi={self.rip_limit_ppi}"
        )
        return image.resize((max_w, max_h), Image.Resampling.BICUBIC)

    def _pre_shrink_for_rip_limit(self, image: Image.Image, width_mm: float, height_mm: float, source_path: Path, worker: WorkerConfig) -> Image.Image:
        if not self.rip_limit_enabled:
            return image
        max_w, max_h = self._rip_limit_size(width_mm, height_mm)
        preshrink_w = max(1, round(max_w * IMAGE_RIP_PRESHRINK_FACTOR))
        preshrink_h = max(1, round(max_h * IMAGE_RIP_PRESHRINK_FACTOR))
        if image.width <= preshrink_w and image.height <= preshrink_h:
            return image
        scale = min(
            preshrink_w / max(1, image.width),
            preshrink_h / max(1, image.height),
            1.0,
        )
        if scale >= 1.0:
            return image
        new_size = (
            max(1, round(image.width * scale)),
            max(1, round(image.height * scale)),
        )
        debug_log(
            f"rip preshrink worker={worker.name} source={source_path.name} from={image.width}x{image.height} "
            f"to={new_size[0]}x{new_size[1]} factor={IMAGE_RIP_PRESHRINK_FACTOR:g} limit_ppi={self.rip_limit_ppi}"
        )
        shrunk = image.resize(new_size, Image.Resampling.BICUBIC)
        shrunk.info.update(image.info)
        return shrunk

    def _apply_orientation(self, image: Image.Image, width_mm: float, height_mm: float) -> tuple[Image.Image, float, float]:
        if not self.auto_orient_enabled:
            return image, width_mm, height_mm
        if abs(width_mm - height_mm) < 0.01:
            return image, width_mm, height_mm
        is_landscape = width_mm > height_mm
        target_landscape = self.target_orientation == "landscape"
        if is_landscape == target_landscape:
            return image, width_mm, height_mm
        rotated = image.rotate(90, expand=True)
        return rotated, height_mm, width_mm

    def _save_cache_image(self, image: Image.Image, out_path: Path) -> None:
        if "icc_profile" in image.info:
            image.info.pop("icc_profile", None)
        debug_log(
            f"cache save path={out_path.name} px={image.width}x{image.height} mode={image.mode} "
            f"format={self._cache_save_format} opts={self._cache_save_kwargs}"
        )
        image.save(out_path, format=self._cache_save_format, **self._cache_save_kwargs)

    def _open_embedded_profile(self, embedded_profile_bytes: bytes | None, context: str):
        if not embedded_profile_bytes:
            return None
        try:
            return ImageCms.getOpenProfile(BytesIO(embedded_profile_bytes))
        except Exception as exc:
            # An unreadable/corrupt embedded profile is treated as "no usable
            # embedded profile" so the caller can fall back (sRGB for RGB, the
            # configured CMYK fallback ICC for CMYK).
            debug_exception(f"Renderer._open_embedded_profile[{context}]", exc)
            return None

    def _rgb_source_profile(self, embedded_profile_bytes: bytes | None, worker: WorkerConfig):
        profile = self._open_embedded_profile(embedded_profile_bytes, f"{worker.name}:rgb-embedded")
        if profile is not None:
            return profile
        return ImageCms.createProfile("sRGB")

    def _cmyk_source_profile(self, embedded_profile_bytes: bytes | None, worker: WorkerConfig):
        profile = self._open_embedded_profile(embedded_profile_bytes, f"{worker.name}:cmyk-embedded")
        if profile is not None:
            return profile
        fallback = self.cmyk_fallback_icc_path
        if fallback is not None and fallback.exists():
            try:
                return ImageCms.getOpenProfile(str(fallback))
            except Exception as exc:
                debug_exception(f"Renderer._cmyk_source_profile[{worker.name}:fallback]", exc)
                raise RuntimeError(
                    translate(self.language, "renderer.cmyk_fallback_icc_unreadable", path=fallback.name)
                ) from exc
        raise RuntimeError(translate(self.language, "renderer.cmyk_missing_icc", worker_name=worker.name))

    def _open_rgb_output_profile(self, output_profile_path: Path, worker: WorkerConfig):
        if not output_profile_path.exists():
            raise RuntimeError(
                translate(
                    self.language,
                    "renderer.output_icc_missing",
                    worker_name=worker.name,
                    path=output_profile_path,
                )
            )
        try:
            profile = ImageCms.getOpenProfile(str(output_profile_path))
        except Exception as exc:
            debug_exception(f"Renderer._open_rgb_output_profile[{worker.name}]", exc)
            raise RuntimeError(
                translate(
                    self.language,
                    "renderer.output_icc_unreadable",
                    worker_name=worker.name,
                    path=output_profile_path.name,
                )
            ) from exc
        color_space = self._profile_color_space(profile)
        if color_space != "RGB":
            raise RuntimeError(
                translate(
                    self.language,
                    "renderer.output_icc_not_rgb",
                    worker_name=worker.name,
                    path=output_profile_path.name,
                    color_space=color_space or "?",
                )
            )
        return profile

    @staticmethod
    def _profile_color_space(profile: Any) -> str:
        cms_profile = getattr(profile, "profile", profile)
        raw = getattr(cms_profile, "xcolor_space", "") or ""
        return str(raw).strip().upper()

    def _apply_color_transform(self, image: Image.Image, worker: WorkerConfig, preset: PresetConfig) -> Image.Image:
        source_mode = image.mode
        embedded_profile_bytes = image.info.get("icc_profile")

        if source_mode == "RGBA":
            background = Image.new("RGB", image.size, (255, 255, 255))
            background.paste(image, mask=image.getchannel("A"))
            image = background
            source_mode = "RGB"
        elif source_mode not in {"RGB", "CMYK"}:
            image = image.convert("RGB")
            source_mode = "RGB"

        output_profile_path = worker.resolve_path(preset.output_icc) if preset.output_icc else None

        debug_log(f"color transform start worker={worker.name} preset={preset.name} mode={source_mode} cmyk_fallback_icc={'yes' if self.cmyk_fallback_icc_path else 'no'} output_icc={'yes' if output_profile_path else 'no'}")

        if source_mode == "CMYK":
            src_profile = self._cmyk_source_profile(embedded_profile_bytes, worker)
        elif source_mode == "RGB":
            src_profile = self._rgb_source_profile(embedded_profile_bytes, worker)
        else:
            src_profile = ImageCms.createProfile("sRGB")

        intent_type = getattr(ImageCms, "Intent", int)
        intent = cast(Any, intent_type(INTENT_NAME_TO_PIL.get(preset.rendering_intent, 1)))
        flags_type = getattr(ImageCms, "Flags", int)
        flags = cast(Any, flags_type(0))
        bpc_flag = getattr(flags_type, "BLACKPOINTCOMPENSATION", 0)
        if preset.black_point_compensation and bpc_flag:
            flags |= cast(Any, bpc_flag)

        working_image = image.convert("CMYK" if source_mode == "CMYK" else "RGB")
        # The output is always rasterized to an RGB DIB and handed to the Windows
        # GDI printer DC, so the output (printer) ICC must describe an RGB device
        # space. A CMYK/Gray/Lab output profile would silently produce wrong color,
        # so reject it up front with a clear message instead.
        if not output_profile_path:
            dst_profile = ImageCms.createProfile("sRGB")
        else:
            dst_profile = self._open_rgb_output_profile(output_profile_path, worker)
        output_mode = "RGB"

        rendered = ImageCms.profileToProfile(
            working_image,
            src_profile,
            dst_profile,
            renderingIntent=intent,
            outputMode=output_mode,
            flags=flags,
        )
        if rendered is None:
            raise RuntimeError(translate(self.language, "renderer.icc_transform_failed"))
        rendered.info.pop("icc_profile", None)
        debug_log(f"color transform end worker={worker.name} preset={preset.name} output_mode=RGB")
        return rendered.convert("RGB")
