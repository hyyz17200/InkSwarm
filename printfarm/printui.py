from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import threading
from typing import Any

from .debug_logger import debug_exception, debug_log
from .i18n import normalize_language, translate

# rundll32/printui processes launched on the print path (driver-settings restore
# before a batch). Tracked so a force stop can kill a hung driver restore the
# same way it kills a hung print helper. Interactive dialogs (preferences /
# properties opened by the user) are deliberately not tracked.
_active_processes: set[subprocess.Popen] = set()
_active_processes_lock = threading.Lock()


def terminate_active_printui() -> int:
    """Kill printui processes currently running on the print path (force stop)."""
    with _active_processes_lock:
        processes = list(_active_processes)
    for process in processes:
        try:
            if process.poll() is None:
                debug_log(f"printui terminating pid={process.pid}")
                process.kill()
        except Exception as exc:
            debug_exception("terminate_active_printui", exc)
    return len(processes)


def _run_printui(
    args: list[str],
    check: bool = True,
    language: str = "en",
    track: bool = False,
) -> subprocess.CompletedProcess[str]:
    language = normalize_language(language)
    command = [
        "rundll32.exe",
        "printui.dll,PrintUIEntry",
        *args,
    ]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=False,
    )
    if track:
        with _active_processes_lock:
            _active_processes.add(process)
    try:
        stdout, stderr = process.communicate()
    finally:
        if track:
            with _active_processes_lock:
                _active_processes.discard(process)
    result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    if check and result.returncode != 0:
        stderr_text = result.stderr.strip() or result.stdout.strip() or translate(language, "printui.unknown_error")
        raise RuntimeError(stderr_text)
    return result


def _devmode_driver_data_size(devmode: Any) -> int:
    if devmode is None:
        return 0
    try:
        data = getattr(devmode, "DriverData", None)
    except Exception:
        return 0
    return len(data or b"")


def _printer_devmode(win32print: Any, handle: Any, level: int) -> Any:
    try:
        info = win32print.GetPrinter(handle, level)
    except Exception:
        return None
    if not isinstance(info, dict):
        return None
    return info.get("pDevMode")


def _driver_default_devmode(
    win32print: Any,
    win32con: Any,
    pywintypes: Any,
    handle: Any,
    printer_name: str,
    language: str = "en",
) -> Any:
    size = win32print.DocumentProperties(0, handle, printer_name, None, None, 0)
    fixed_size = pywintypes.DEVMODEType().Size
    driver_extra = max(0, int(size) - int(fixed_size))
    devmode = pywintypes.DEVMODEType(driver_extra)
    result = win32print.DocumentProperties(0, handle, printer_name, devmode, None, win32con.DM_OUT_BUFFER)
    if result != getattr(win32con, "IDOK", 1):
        raise RuntimeError(translate(language, "printui.document_properties_failed", result=result))
    return devmode


def ensure_printer_defaults_initialized(printer_name: str, language: str = "en") -> bool:
    language = normalize_language(language)
    if sys.platform != "win32":
        raise RuntimeError(translate(language, "printui.windows_only"))
    try:
        import pywintypes  # type: ignore
        import win32con  # type: ignore
        import win32print  # type: ignore
    except ModuleNotFoundError as exc:
        missing = exc.name or str(exc)
        raise RuntimeError(translate(language, "printui.dependency_missing", missing=missing)) from exc
    except ImportError as exc:
        raise RuntimeError(translate(language, "printui.dependency_import_failed", error=exc)) from exc

    handle = win32print.OpenPrinter(printer_name)
    try:
        existing = _printer_devmode(win32print, handle, 8)
        if _devmode_driver_data_size(existing) > 0:
            return False
        candidate = _printer_devmode(win32print, handle, 2)
        if _devmode_driver_data_size(candidate) <= 0:
            candidate = _driver_default_devmode(win32print, win32con, pywintypes, handle, printer_name, language=language)
    finally:
        win32print.ClosePrinter(handle)

    if _devmode_driver_data_size(candidate) <= 0:
        raise RuntimeError(translate(language, "printui.devmode_incomplete"))

    access = getattr(win32print, "PRINTER_ACCESS_ADMINISTER", 4) | getattr(win32print, "PRINTER_ACCESS_USE", 8)
    try:
        handle = win32print.OpenPrinter(printer_name, {"DesiredAccess": access})
        try:
            result = win32print.DocumentProperties(
                0,
                handle,
                printer_name,
                candidate,
                candidate,
                win32con.DM_IN_BUFFER | win32con.DM_OUT_BUFFER,
            )
            if result != getattr(win32con, "IDOK", 1):
                raise RuntimeError(translate(language, "printui.document_properties_failed", result=result))
            win32print.SetPrinter(handle, 8, {"pDevMode": candidate}, 0)
        finally:
            win32print.ClosePrinter(handle)
    except Exception as exc:
        raise RuntimeError(translate(language, "printui.defaults_init_failed")) from exc
    return True


def restore_printer_settings(
    printer_name: str,
    data_file: Path,
    initialize_defaults: bool = True,
    language: str = "en",
) -> None:
    language = normalize_language(language)
    if sys.platform != "win32":
        raise RuntimeError(translate(language, "printui.windows_only"))
    if not data_file.exists():
        raise FileNotFoundError(data_file)
    try:
        if initialize_defaults:
            ensure_printer_defaults_initialized(printer_name, language=language)
        _run_printui([
            "/Sr",
            f"/n{printer_name}",
            f"/a{str(data_file)}",
            "d",
            "g",
            "u",
            "r",
            "p",
            "h",
        ], language=language, track=True)
    except Exception as exc:
        raise RuntimeError(translate(language, "printui.restore_failed", error=exc)) from exc


def save_printer_settings(
    printer_name: str,
    data_file: Path,
    initialize_defaults: bool = True,
    language: str = "en",
) -> None:
    language = normalize_language(language)
    if sys.platform != "win32":
        raise RuntimeError(translate(language, "printui.windows_only"))
    data_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        if initialize_defaults:
            ensure_printer_defaults_initialized(printer_name, language=language)
        _run_printui([
            "/Ss",
            f"/n{printer_name}",
            f"/a{str(data_file)}",
            "d",
            "g",
            "u",
            "c",
        ], language=language)
    except Exception as exc:
        raise RuntimeError(translate(language, "printui.save_failed", error=exc)) from exc


def open_printer_preferences(printer_name: str, language: str = "en") -> None:
    language = normalize_language(language)
    if sys.platform != "win32":
        raise RuntimeError(translate(language, "printui.windows_only"))
    _run_printui(["/e", f"/n{printer_name}"], check=False, language=language)


def open_printer_properties(printer_name: str, language: str = "en") -> None:
    language = normalize_language(language)
    if sys.platform != "win32":
        raise RuntimeError(translate(language, "printui.windows_only"))
    _run_printui(["/p", f"/n{printer_name}"], check=False, language=language)
