from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Iterable

import pypdfium2 as pdfium
from PIL import Image, ImageCms, ImageFile

from .models import DEFAULT_RASTER_DPI

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True

MM_PER_INCH = 25.4
PDF_POINTS_PER_INCH = 72.0


@dataclass
class TaskInspection:
    display_size_mm: str
    preview_bytes: bytes
    page_count: int = 1


class TaskInspectionError(RuntimeError):
    pass


def inspect_task_input(file_path: Path, preview_max_size: tuple[int, int] = (320, 320)) -> TaskInspection:
    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        return _inspect_pdf(file_path, preview_max_size)
    return _inspect_image(file_path, preview_max_size)


def _inspect_pdf(file_path: Path, preview_max_size: tuple[int, int]) -> TaskInspection:
    document = pdfium.PdfDocument(str(file_path))
    try:
        if len(document) == 0:
            raise TaskInspectionError("PDF 没有页面")
        first_page = document[0]
        width_pt = float(first_page.get_width())
        height_pt = float(first_page.get_height())
        width_mm = width_pt * MM_PER_INCH / PDF_POINTS_PER_INCH
        height_mm = height_pt * MM_PER_INCH / PDF_POINTS_PER_INCH
        display = _format_mm(width_mm, height_mm, len(document))
        bitmap = first_page.render(scale=min(preview_max_size) / max(width_pt, height_pt), optimize_mode="lcd")
        image = bitmap.to_pil().convert("RGB")
        preview = _image_to_png_bytes(image, preview_max_size)
        return TaskInspection(display_size_mm=display, preview_bytes=preview, page_count=len(document))
    except TaskInspectionError:
        raise
    except Exception as exc:
        raise TaskInspectionError(str(exc)) from exc
    finally:
        document.close()


def _inspect_image(file_path: Path, preview_max_size: tuple[int, int]) -> TaskInspection:
    try:
        with Image.open(file_path) as image:
            mode = image.mode
            embedded_profile = image.info.get("icc_profile")
            if mode == "CMYK" and not embedded_profile:
                raise TaskInspectionError("CMYK 文件没有嵌入 ICC，已跳过")

            dpi_x, dpi_y = get_image_dpi(image)
            width_mm = image.width / dpi_x * MM_PER_INCH
            height_mm = image.height / dpi_y * MM_PER_INCH
            display = _format_mm(width_mm, height_mm, 1)

            preview_image = image.convert("RGB") if image.mode != "RGB" else image.copy()
            preview = _image_to_png_bytes(preview_image, preview_max_size)
            return TaskInspection(display_size_mm=display, preview_bytes=preview, page_count=1)
    except TaskInspectionError:
        raise
    except Exception as exc:
        raise TaskInspectionError(str(exc)) from exc


def get_image_dpi(image: Image.Image) -> tuple[float, float]:
    dpi = image.info.get("dpi")
    if isinstance(dpi, tuple) and len(dpi) >= 2 and dpi[0] and dpi[1]:
        return float(dpi[0]), float(dpi[1])
    if "resolution" in image.info and image.info["resolution"]:
        resolution = image.info["resolution"]
        if isinstance(resolution, tuple) and len(resolution) >= 2 and resolution[0] and resolution[1]:
            return float(resolution[0]), float(resolution[1])
    return float(DEFAULT_RASTER_DPI), float(DEFAULT_RASTER_DPI)


def build_preview_file(preview_dir: Path, task_id: str, preview_bytes: bytes) -> Path:
    preview_dir.mkdir(parents=True, exist_ok=True)
    target = preview_dir / f"{task_id}.png"
    target.write_bytes(preview_bytes)
    return target


def _image_to_png_bytes(image: Image.Image, preview_max_size: tuple[int, int]) -> bytes:
    preview = image.copy()
    preview.thumbnail(preview_max_size, Image.Resampling.LANCZOS)
    buffer = BytesIO()
    preview.save(buffer, format="PNG", compress_level=1)
    return buffer.getvalue()


def _format_mm(width_mm: float, height_mm: float, page_count: int) -> str:
    base = f"{round(width_mm)} × {round(height_mm)} mm"
    if page_count > 1:
        return f"{base} · {page_count}页"
    return base
