from __future__ import annotations

from pathlib import Path
import sys
import threading
import time
from typing import Any, Callable

from PIL import Image, ImageFile, ImageWin

from .debug_logger import debug_exception, debug_log
from .i18n import normalize_language, translate

Image.MAX_IMAGE_PIXELS = None
ImageFile.LOAD_TRUNCATED_IMAGES = True

win32ui: Any = None
win32con: Any = None
win32print: Any = None


class PrinterSpooler:
    def __init__(self, language: str = "en") -> None:
        self.language = normalize_language(language)
        self.validate_environment(language=self.language)
        self._queue_waiting_states: dict[str, bool] = {}
        self._queue_pause_last_log_ts: dict[str, float] = {}

    @staticmethod
    def validate_environment(language: str = "en") -> None:
        language = normalize_language(language)
        if sys.platform != "win32":
            raise RuntimeError(translate(language, "spooler.windows_only"))
        global win32ui, win32con, win32print
        try:
            import win32ui as _win32ui  # type: ignore
            import win32con as _win32con  # type: ignore
            import win32print as _win32print  # type: ignore
        except ModuleNotFoundError as exc:
            missing = exc.name or str(exc)
            raise RuntimeError(translate(language, "spooler.dependency_missing", missing=missing)) from exc
        except ImportError as exc:
            raise RuntimeError(translate(language, "spooler.dependency_import_failed", error=exc)) from exc

        win32ui = _win32ui
        win32con = _win32con
        win32print = _win32print

    def get_queue_depth(self, printer_name: str) -> int:
        handle = win32print.OpenPrinter(printer_name)
        try:
            jobs = win32print.EnumJobs(handle, 0, 999, 1)
            return len(jobs or [])
        finally:
            win32print.ClosePrinter(handle)

    def wait_until_queue_available(
        self,
        printer_name: str,
        max_queue_jobs: int,
        poll_seconds: float = 5.0,
        stop_event: threading.Event | None = None,
        status_callback: Callable[[str], None] | None = None,
        log_callback: Callable[[str], None] | None = None,
        log_cooldown_seconds: float = 60.0,
        pause_callback: Callable[[], bool] | None = None,
        language: str | None = None,
    ) -> None:
        active_language = normalize_language(language or self.language)
        if max_queue_jobs <= 0:
            self._queue_waiting_states.pop(printer_name, None)
            return
        while True:
            if stop_event is not None and stop_event.is_set():
                raise RuntimeError(translate(active_language, "runtime.stopped"))
            if pause_callback is not None and not pause_callback():
                raise RuntimeError(translate(active_language, "runtime.stopped"))
            depth = self.get_queue_depth(printer_name)
            if depth < max_queue_jobs:
                self._queue_waiting_states[printer_name] = False
                return
            if status_callback is not None:
                status_callback(translate(active_language, "spooler.queue_status", depth=depth, limit=max_queue_jobs))
            already_waiting = self._queue_waiting_states.get(printer_name, False)
            last_log_ts = self._queue_pause_last_log_ts.get(printer_name, 0.0)
            now = time.time()
            should_log = (not already_waiting) and (now - last_log_ts >= max(1.0, log_cooldown_seconds))
            if log_callback is not None and should_log:
                log_callback(translate(active_language, "spooler.queue_limit_log", depth=depth, limit=max_queue_jobs))
                self._queue_pause_last_log_ts[printer_name] = now
            self._queue_waiting_states[printer_name] = True
            time.sleep(max(0.2, poll_seconds))

    def print_cached_pages(
        self,
        printer_name: str,
        page_paths: list[Path],
        page_specs: list[dict],
        job_name: str,
        copies: int,
        ignore_margins: bool = True,
        before_each_copy: Callable[[int, int], bool | None] | None = None,
        after_each_copy: Callable[[int, int], None] | None = None,
        before_send: Callable[[], None] | None = None,
        after_send: Callable[[], None] | None = None,
    ) -> None:
        for copy_index in range(copies):
            if before_each_copy is not None:
                if before_each_copy(copy_index + 1, copies) is False:
                    break
            effective_name = f"{job_name} [copy {copy_index + 1}/{copies}]"
            debug_log(f"spooler print start printer={printer_name} job={effective_name} pages={len(page_paths)} ignore_margins={ignore_margins}")
            if before_send is not None:
                before_send()
            try:
                self._print_single_job(printer_name, page_paths, page_specs, effective_name, ignore_margins=ignore_margins)
            finally:
                if after_send is not None:
                    after_send()
            debug_log(f"spooler print end printer={printer_name} job={effective_name}")
            if after_each_copy is not None:
                after_each_copy(copy_index + 1, copies)

    def _print_single_job(self, printer_name: str, page_paths: list[Path], page_specs: list[dict], job_name: str, ignore_margins: bool = True) -> None:
        dc = win32ui.CreateDC()
        dc.CreatePrinterDC(printer_name)
        try:
            dc.StartDoc(job_name)
            for page_path, page_spec in zip(page_paths, page_specs):
                with Image.open(page_path) as opened:
                    image = opened.convert("RGB")
                    image.load()
                    dc.StartPage()
                    self._draw_image_actual_size(dc, image, page_spec, ignore_margins=ignore_margins)
                    dc.EndPage()
            dc.EndDoc()
        except Exception as exc:
            debug_exception(f"PrinterSpooler._print_single_job[{printer_name}:{job_name}]", exc)
            try:
                dc.AbortDoc()
            except Exception:
                pass
            raise
        finally:
            dc.DeleteDC()

    def _draw_image_actual_size(self, dc, image: Image.Image, page_spec: dict, ignore_margins: bool = True) -> None:
        physical_width = dc.GetDeviceCaps(win32con.PHYSICALWIDTH)
        physical_height = dc.GetDeviceCaps(win32con.PHYSICALHEIGHT)
        printable_width = dc.GetDeviceCaps(win32con.HORZRES)
        printable_height = dc.GetDeviceCaps(win32con.VERTRES)
        physical_offset_x = dc.GetDeviceCaps(win32con.PHYSICALOFFSETX)
        physical_offset_y = dc.GetDeviceCaps(win32con.PHYSICALOFFSETY)
        dpi_x = dc.GetDeviceCaps(win32con.LOGPIXELSX)
        dpi_y = dc.GetDeviceCaps(win32con.LOGPIXELSY)

        width_mm = float(page_spec.get("width_mm", 0))
        height_mm = float(page_spec.get("height_mm", 0))
        if width_mm <= 0 or height_mm <= 0:
            raise RuntimeError(translate(self.language, "spooler.page_size_missing"))

        dst_w = max(1, round(width_mm / 25.4 * dpi_x))
        dst_h = max(1, round(height_mm / 25.4 * dpi_y))
        max_w = physical_width if ignore_margins else printable_width
        max_h = physical_height if ignore_margins else printable_height
        if dst_w > max_w or dst_h > max_h:
            scale = min(max_w / max(dst_w, 1), max_h / max(dst_h, 1))
            clamped_w = max(1, int(dst_w * scale))
            clamped_h = max(1, int(dst_h * scale))
            debug_log(
                f"draw clamp ignore_margins={ignore_margins} from={dst_w}x{dst_h} to={clamped_w}x{clamped_h} "
                f"physical={physical_width}x{physical_height} printable={printable_width}x{printable_height}"
            )
            dst_w, dst_h = clamped_w, clamped_h
        if ignore_margins:
            left = round((physical_width - dst_w) / 2) - physical_offset_x
            top = round((physical_height - dst_h) / 2) - physical_offset_y
        else:
            left = max(0, round((printable_width - dst_w) / 2))
            top = max(0, round((printable_height - dst_h) / 2))
        right = left + dst_w
        bottom = top + dst_h

        debug_log(
            "draw page "
            f"ignore_margins={ignore_margins} physical={physical_width}x{physical_height} "
            f"printable={printable_width}x{printable_height} offset={physical_offset_x},{physical_offset_y} "
            f"dst={dst_w}x{dst_h} rect=({left},{top},{right},{bottom})"
        )

        dib = ImageWin.Dib(image)
        dib.draw(dc.GetHandleOutput(), (left, top, right, bottom))
