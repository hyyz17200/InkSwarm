from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
import threading
import time
from typing import Any, Callable

from PIL import Image, ImageWin

from .debug_logger import debug_exception, debug_log
from .i18n import normalize_language, translate
from .models import (
    DEFAULT_FIT_MODE,
    FIT_MODE_ACTUAL_100,
    FIT_MODE_FILL,
    FIT_MODE_FIT,
    normalize_fit_mode,
)

Image.MAX_IMAGE_PIXELS = None
# Do NOT enable ImageFile.LOAD_TRUNCATED_IMAGES: incomplete or corrupt bitmaps must
# raise during decode so they are rejected, never printed as partial output.

# When a job prints more than one copy, the page bitmaps are decoded once and the
# resulting Windows DIBs are reused for every copy instead of re-decoding the
# cache file each time. Holding all pages of a document in memory is the trade-off
# for that; above this estimated budget we fall back to per-copy streaming so a
# large multi-page document cannot exhaust memory. Sized in decoded bytes (~4 B/px).
PREDECODE_MAX_BYTES = 1024 * 1024 * 1024

win32ui: Any = None
win32con: Any = None
win32print: Any = None

PRINTER_STATUS_PAUSED = 0x00000001
PRINTER_STATUS_ERROR = 0x00000002
PRINTER_STATUS_OFFLINE = 0x00000080
PRINTER_ATTRIBUTE_WORK_OFFLINE = 0x00000400
PRINTER_ENUM_LOCAL = 0x00000002
PRINTER_ENUM_CONNECTIONS = 0x00000004


@dataclass
class _PreparedPage:
    dib: Any
    page_spec: dict


@dataclass(frozen=True)
class PrinterQueueSnapshot:
    """Best-effort view of one printer's queue for maintenance drain waits.

    When `unreachable` is set the query itself failed and the other fields
    are meaningless; the caller decides whether to keep waiting.
    """

    printer_name: str
    job_count: int = 0
    offline: bool = False
    paused: bool = False
    error: bool = False
    unreachable: str | None = None


def read_printer_queue_snapshot(printer_name: str) -> PrinterQueueSnapshot:
    if win32print is None:
        PrinterSpooler.validate_environment()
    try:
        handle = win32print.OpenPrinter(printer_name)
    except Exception as exc:
        return PrinterQueueSnapshot(printer_name=printer_name, unreachable=str(exc))
    try:
        info = win32print.GetPrinter(handle, 2)
        jobs = win32print.EnumJobs(handle, 0, 999, 1)
    except Exception as exc:
        return PrinterQueueSnapshot(printer_name=printer_name, unreachable=str(exc))
    finally:
        win32print.ClosePrinter(handle)
    status = int(info.get("Status", 0))
    attributes = int(info.get("Attributes", 0))
    return PrinterQueueSnapshot(
        printer_name=printer_name,
        job_count=len(jobs or []),
        offline=bool(status & PRINTER_STATUS_OFFLINE) or bool(attributes & PRINTER_ATTRIBUTE_WORK_OFFLINE),
        paused=bool(status & PRINTER_STATUS_PAUSED),
        error=bool(status & PRINTER_STATUS_ERROR),
    )


def list_printers_with_jobs(exclude: set[str] | None = None) -> list[PrinterQueueSnapshot]:
    """Best-effort scan for queued jobs on printers outside the given set.

    A Print Spooler restart is machine-wide, so queues InkSwarm does not
    manage are affected too; the caller warns the user about these. Any
    enumeration or per-printer failure degrades to "no warning" rather than
    blocking the maintenance.
    """
    if win32print is None:
        PrinterSpooler.validate_environment()
    excluded = set(exclude or ())
    try:
        printers = win32print.EnumPrinters(PRINTER_ENUM_LOCAL | PRINTER_ENUM_CONNECTIONS, None, 4)
    except Exception as exc:
        debug_exception("list_printers_with_jobs.enum", exc)
        return []
    results: list[PrinterQueueSnapshot] = []
    for entry in printers or ():
        name = str(entry.get("pPrinterName", "")) if isinstance(entry, dict) else ""
        if not name or name in excluded:
            continue
        snapshot = read_printer_queue_snapshot(name)
        if snapshot.job_count > 0:
            results.append(snapshot)
    return results


def delete_spooler_job(printer_name: str, job_id: int) -> None:
    """Remove one job from a printer's queue (cleanup after a killed submit).

    Used when the print helper is force-killed mid-submit: the half-submitted
    job was never reported as sent (it counts as failed), so removing it keeps
    what actually reaches paper consistent with the statistics.
    """
    if win32print is None:
        PrinterSpooler.validate_environment()
    handle = win32print.OpenPrinter(printer_name)
    try:
        win32print.SetJob(handle, int(job_id), 0, None, win32print.JOB_CONTROL_DELETE)
    finally:
        win32print.ClosePrinter(handle)


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
                status_callback(f"Queue {depth}/{max_queue_jobs}")
            already_waiting = self._queue_waiting_states.get(printer_name, False)
            last_log_ts = self._queue_pause_last_log_ts.get(printer_name, 0.0)
            now = time.time()
            should_log = (not already_waiting) and (now - last_log_ts >= max(1.0, log_cooldown_seconds))
            if log_callback is not None and should_log:
                log_callback(translate(active_language, "spooler.queue_limit_log", depth=depth, limit=max_queue_jobs))
                self._queue_pause_last_log_ts[printer_name] = now
            self._queue_waiting_states[printer_name] = True
            time.sleep(max(0.2, poll_seconds))

    def prepare_pages(self, page_paths: list[Path], page_specs: list[dict]) -> list[_PreparedPage] | None:
        """Decode every page once into reusable DIBs, or None if too large to hold.

        Returning None makes the caller fall back to per-copy streaming, keeping
        peak memory at one page regardless of document size.
        """
        prepared: list[_PreparedPage] = []
        total_bytes = 0
        for page_path, page_spec in zip(page_paths, page_specs):
            with Image.open(page_path) as opened:
                image = opened.convert("RGB")
                image.load()
            total_bytes += image.width * image.height * 4
            if total_bytes > PREDECODE_MAX_BYTES:
                debug_log(f"spooler predecode skipped pages>{len(prepared)} est_bytes={total_bytes} cap={PREDECODE_MAX_BYTES}; streaming per copy")
                return None
            prepared.append(_PreparedPage(dib=ImageWin.Dib(image), page_spec=page_spec))
        debug_log(f"spooler predecode pages={len(prepared)} est_bytes={total_bytes}")
        return prepared

    def print_single_copy(
        self,
        printer_name: str,
        page_paths: list[Path],
        page_specs: list[dict],
        job_name: str,
        ignore_margins: bool = True,
        fit_mode: str = DEFAULT_FIT_MODE,
        prepared: list[_PreparedPage] | None = None,
        on_job_started: Callable[[int], None] | None = None,
    ) -> None:
        """Submit one print job (one copy of the document) to the Windows spooler.

        ``prepared`` reuses DIBs decoded by prepare_pages(); otherwise pages are
        decoded on demand, keeping peak memory at one page. ``on_job_started``
        receives the spooler job id right after StartDoc, before any page is
        drawn, so a supervisor can clean up the job if this call is killed
        mid-submit.
        """
        debug_log(
            f"spooler print start printer={printer_name} job={job_name} pages={len(page_paths)} "
            f"ignore_margins={ignore_margins} predecoded={prepared is not None}"
        )
        dc = win32ui.CreateDC()
        try:
            dc.CreatePrinterDC(printer_name)
            job_id = dc.StartDoc(job_name)
            if on_job_started is not None and isinstance(job_id, int) and job_id > 0:
                on_job_started(job_id)
            if prepared is not None:
                for page in prepared:
                    dc.StartPage()
                    rect = self._compute_draw_rect(dc, page.page_spec, ignore_margins=ignore_margins, fit_mode=fit_mode)
                    page.dib.draw(dc.GetHandleOutput(), rect)
                    dc.EndPage()
            else:
                for page_path, page_spec in zip(page_paths, page_specs):
                    with Image.open(page_path) as opened:
                        image = opened.convert("RGB")
                        image.load()
                        dc.StartPage()
                        rect = self._compute_draw_rect(dc, page_spec, ignore_margins=ignore_margins, fit_mode=fit_mode)
                        ImageWin.Dib(image).draw(dc.GetHandleOutput(), rect)
                        dc.EndPage()
            dc.EndDoc()
        except Exception as exc:
            debug_exception(f"PrinterSpooler.print_single_copy[{printer_name}:{job_name}]", exc)
            try:
                dc.AbortDoc()
            except Exception:
                pass
            raise
        finally:
            try:
                dc.DeleteDC()
            except Exception:
                pass
        debug_log(f"spooler print end printer={printer_name} job={job_name}")

    def _compute_draw_rect(self, dc, page_spec: dict, ignore_margins: bool = True, fit_mode: str = DEFAULT_FIT_MODE) -> tuple[int, int, int, int]:
        fit_mode = normalize_fit_mode(fit_mode)
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

        # The image's true 1:1 size on this device, in device pixels.
        natural_w = max(1, round(width_mm / 25.4 * dpi_x))
        natural_h = max(1, round(height_mm / 25.4 * dpi_y))
        # Target area the image is sized against: the full physical sheet when margins
        # are ignored, otherwise the driver-reported printable area.
        max_w = physical_width if ignore_margins else printable_width
        max_h = physical_height if ignore_margins else printable_height

        dst_w, dst_h = self._fit_destination_size(fit_mode, natural_w, natural_h, max_w, max_h)

        # Center on the chosen area. When dst is larger than the area (ACTUAL_100 or
        # FILL overflow) the offsets go negative, so the image is centered and the
        # overflowing edge is cropped by the device; the centering is never clamped to
        # zero, otherwise an overflowing image would shift instead of staying centered.
        if ignore_margins:
            left = round((physical_width - dst_w) / 2) - physical_offset_x
            top = round((physical_height - dst_h) / 2) - physical_offset_y
        else:
            left = round((printable_width - dst_w) / 2)
            top = round((printable_height - dst_h) / 2)
        right = left + dst_w
        bottom = top + dst_h

        debug_log(
            "draw page "
            f"fit_mode={fit_mode} ignore_margins={ignore_margins} physical={physical_width}x{physical_height} "
            f"printable={printable_width}x{printable_height} offset={physical_offset_x},{physical_offset_y} "
            f"natural={natural_w}x{natural_h} dst={dst_w}x{dst_h} rect=({left},{top},{right},{bottom})"
        )

        return left, top, right, bottom

    @staticmethod
    def _fit_destination_size(fit_mode: str, natural_w: int, natural_h: int, max_w: int, max_h: int) -> tuple[int, int]:
        """Destination size in device pixels for the chosen fit mode.

        ``natural_*`` is the image's true 1:1 size on the device; ``max_*`` is the
        target area (physical sheet or printable area). The caller centers the result,
        so a size larger than the page is cropped and a smaller size leaves a centered
        margin. Aspect ratio is always preserved (a single uniform scale factor).
        """
        natural_w = max(1, natural_w)
        natural_h = max(1, natural_h)
        if fit_mode == FIT_MODE_ACTUAL_100:
            # Exact 1:1, never scaled; overflow is cropped by centering.
            return natural_w, natural_h
        if fit_mode == FIT_MODE_FIT:
            # Largest size that still fits inside the page (may scale up or down).
            scale = min(max_w / natural_w, max_h / natural_h)
        elif fit_mode == FIT_MODE_FILL:
            # Smallest size that fully covers the page (may scale up or down); cropped.
            scale = max(max_w / natural_w, max_h / natural_h)
        else:
            # FIT_MODE_ACTUAL: 1:1, but shrink to fit if it would overflow the page.
            scale = min(1.0, max_w / natural_w, max_h / natural_h)
        return max(1, round(natural_w * scale)), max(1, round(natural_h * scale))
