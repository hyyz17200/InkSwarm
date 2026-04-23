from __future__ import annotations

from io import BytesIO
from pathlib import Path
import json
import threading
import time

import pypdfium2 as pdfium
from PIL import Image, ImageCms, ImageFile

from .models import INTENT_NAME_TO_PIL, PresetConfig, RenderArtifact, TaskItem, WorkerConfig, file_signature, stable_hash
from .debug_logger import debug_exception, debug_log
from .task_inspector import MM_PER_INCH, PDF_POINTS_PER_INCH, get_image_dpi

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True


PDF_RENDER_LOCKS: dict[str, threading.Lock] = {}
PDF_RENDER_LOCKS_GUARD = threading.Lock()


class Renderer:
    def __init__(self, cache_root: Path, auto_orient_enabled: bool = False, target_orientation: str = "portrait", rip_limit_enabled: bool = True, rip_limit_ppi: int = 300):
        self.cache_root = cache_root.resolve()
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.auto_orient_enabled = auto_orient_enabled
        self.target_orientation = (target_orientation or "portrait").lower()
        self.rip_limit_enabled = bool(rip_limit_enabled)
        self.rip_limit_ppi = max(36, int(rip_limit_ppi or 300))

    def ensure_render_cache(self, task: TaskItem, worker: WorkerConfig) -> RenderArtifact:
        preset = worker.get_active_preset()
        key_payload = {
            "source": file_signature(task.file_path),
            "preset": preset.to_dict(),
            "worker": worker.name,
            "auto_orient_enabled": self.auto_orient_enabled,
            "target_orientation": self.target_orientation,
            "rip_limit_enabled": self.rip_limit_enabled,
            "rip_limit_ppi": self.rip_limit_ppi,
        }
        key = stable_hash(key_payload)
        cache_dir = self.cache_root / key
        meta_file = cache_dir / "metadata.json"
        if meta_file.exists():
            metadata = json.loads(meta_file.read_text(encoding="utf-8"))
            page_paths = [cache_dir / page["file"] for page in metadata["pages"]]
            if all(path.exists() for path in page_paths):
                debug_log(f"renderer cache hit worker={worker.name} preset={preset.name} task={task.file_name()} key={key}")
                return RenderArtifact(cache_dir=cache_dir, page_paths=page_paths, metadata=metadata)

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
            "rip_limit_enabled": self.rip_limit_enabled,
            "rip_limit_ppi": self.rip_limit_ppi,
            "auto_orient_enabled": self.auto_orient_enabled,
            "target_orientation": self.target_orientation,
            "pages": page_info,
        }
        meta_file.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
        return RenderArtifact(cache_dir=cache_dir, page_paths=[cache_dir / p["file"] for p in page_info], metadata=metadata)

    def _render_pdf(self, pdf_path: Path, cache_dir: Path, worker: WorkerConfig, preset: PresetConfig) -> list[dict[str, float | str]]:
        page_info: list[dict[str, float | str]] = []
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
                    bitmap = page.render(scale=scale, optimize_mode="print")
                    image = bitmap.to_pil()
                    image = self._apply_color_transform(image, worker, preset)
                    image, width_mm, height_mm = self._apply_orientation(image, width_mm, height_mm)
                    debug_log(
                        f"render pdf final worker={worker.name} preset={preset.name} page={index + 1} "
                        f"final_px={image.width}x{image.height} size_mm={width_mm:.3f}x{height_mm:.3f}"
                    )
                    out_path = cache_dir / f"page_{index + 1:04d}.png"
                    self._save_cache_image(image, out_path)
                    page_info.append({
                        "file": out_path.name,
                        "width_mm": round(width_mm, 3),
                        "height_mm": round(height_mm, 3),
                    })
            finally:
                document.close()
        return page_info

    def _render_image_file(self, image_path: Path, cache_dir: Path, worker: WorkerConfig, preset: PresetConfig) -> list[dict[str, float | str]]:
        with Image.open(image_path) as image:
            dpi_x, dpi_y = get_image_dpi(image)
            width_mm = image.width / dpi_x * MM_PER_INCH
            height_mm = image.height / dpi_y * MM_PER_INCH
            rendered = self._apply_color_transform(image, worker, preset)
            rendered, width_mm, height_mm = self._apply_orientation(rendered, width_mm, height_mm)
            rendered = self._apply_rip_limit_to_image(rendered, width_mm, height_mm, image_path, worker)
            debug_log(
                f"render image final worker={worker.name} preset={preset.name} source={image_path.name} "
                f"final_px={rendered.width}x{rendered.height} size_mm={width_mm:.3f}x{height_mm:.3f}"
            )
            out_path = cache_dir / "page_0001.png"
            self._save_cache_image(rendered, out_path)
        return [{"file": out_path.name, "width_mm": round(width_mm, 3), "height_mm": round(height_mm, 3)}]


    def _effective_rip_dpi(self, requested_dpi: int) -> int:
        requested = max(36, int(requested_dpi or 300))
        if not self.rip_limit_enabled:
            return requested
        return max(36, min(requested, self.rip_limit_ppi))

    def _get_pdf_render_lock(self, pdf_path: Path) -> threading.Lock:
        key = str(pdf_path.resolve()).lower()
        with PDF_RENDER_LOCKS_GUARD:
            lock = PDF_RENDER_LOCKS.get(key)
            if lock is None:
                lock = threading.Lock()
                PDF_RENDER_LOCKS[key] = lock
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
        max_w = max(1, round(width_mm / 25.4 * self.rip_limit_ppi))
        max_h = max(1, round(height_mm / 25.4 * self.rip_limit_ppi))
        if image.width <= max_w and image.height <= max_h:
            return image
        debug_log(
            f"rip downscale worker={worker.name} source={source_path.name} from={image.width}x{image.height} to={max_w}x{max_h} limit_ppi={self.rip_limit_ppi}"
        )
        return image.resize((max_w, max_h), Image.Resampling.BICUBIC)

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
        debug_log(f"cache save path={out_path.name} px={image.width}x{image.height} mode={image.mode} format=PNG compress_level=0")
        image.save(out_path, format="PNG", compress_level=0, optimize=False)

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

        input_profile_path = worker.resolve_path(preset.input_icc) if preset.input_icc else None
        output_profile_path = worker.resolve_path(preset.output_icc) if preset.output_icc else None

        debug_log(f"color transform start worker={worker.name} preset={preset.name} mode={source_mode} input_icc={'yes' if input_profile_path else 'no'} output_icc={'yes' if output_profile_path else 'no'}")

        if source_mode == "RGB":
            if input_profile_path and input_profile_path.exists():
                src_profile = ImageCms.getOpenProfile(str(input_profile_path))
            elif embedded_profile_bytes:
                src_profile = ImageCms.getOpenProfile(BytesIO(embedded_profile_bytes))
            else:
                src_profile = ImageCms.createProfile("sRGB")
        elif source_mode == "CMYK":
            if input_profile_path and input_profile_path.exists():
                src_profile = ImageCms.getOpenProfile(str(input_profile_path))
            elif embedded_profile_bytes:
                src_profile = ImageCms.getOpenProfile(BytesIO(embedded_profile_bytes))
            else:
                raise RuntimeError(f"{worker.name}: CMYK 输入没有可用 ICC，已拒绝处理")
        else:
            src_profile = ImageCms.createProfile("sRGB")

        intent = INTENT_NAME_TO_PIL.get(preset.rendering_intent, 1)
        flags = 0
        bpc_flag = getattr(getattr(ImageCms, "Flags", object()), "BLACKPOINTCOMPENSATION", 0)
        if preset.black_point_compensation and bpc_flag:
            flags |= int(bpc_flag)

        working_image = image.convert("CMYK" if source_mode == "CMYK" else "RGB")
        if not output_profile_path:
            dst_profile = ImageCms.createProfile("sRGB")
            output_mode = "RGB"
        else:
            dst_profile = ImageCms.getOpenProfile(str(output_profile_path))
            output_mode = "RGB"

        rendered = ImageCms.profileToProfile(
            working_image,
            src_profile,
            dst_profile,
            renderingIntent=intent,
            outputMode=output_mode,
            flags=flags,
        )
        rendered.info.pop("icc_profile", None)
        debug_log(f"color transform end worker={worker.name} preset={preset.name} output_mode=RGB")
        return rendered.convert("RGB")
