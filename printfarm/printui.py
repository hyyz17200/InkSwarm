from __future__ import annotations

from pathlib import Path
import subprocess
import sys
from typing import Any


def _run_printui(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    command = [
        "rundll32.exe",
        "printui.dll,PrintUIEntry",
        *args,
    ]
    result = subprocess.run(command, capture_output=True, text=True, shell=False)
    if check and result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip() or "未知错误"
        raise RuntimeError(stderr)
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


def _driver_default_devmode(win32print: Any, win32con: Any, pywintypes: Any, handle: Any, printer_name: str) -> Any:
    size = win32print.DocumentProperties(0, handle, printer_name, None, None, 0)
    fixed_size = pywintypes.DEVMODEType().Size
    driver_extra = max(0, int(size) - int(fixed_size))
    devmode = pywintypes.DEVMODEType(driver_extra)
    result = win32print.DocumentProperties(0, handle, printer_name, devmode, None, win32con.DM_OUT_BUFFER)
    if result != getattr(win32con, "IDOK", 1):
        raise RuntimeError(f"DocumentProperties returned {result}")
    return devmode


def ensure_printer_defaults_initialized(printer_name: str) -> bool:
    if sys.platform != "win32":
        raise RuntimeError("PrintUI 仅支持 Windows")
    try:
        import pywintypes  # type: ignore
        import win32con  # type: ignore
        import win32print  # type: ignore
    except ModuleNotFoundError as exc:
        missing = exc.name or str(exc)
        raise RuntimeError(f"Windows printing dependency is missing: {missing}.") from exc
    except ImportError as exc:
        raise RuntimeError(f"Windows printing dependencies failed to import: {exc}") from exc

    handle = win32print.OpenPrinter(printer_name)
    try:
        existing = _printer_devmode(win32print, handle, 8)
        if _devmode_driver_data_size(existing) > 0:
            return False
        candidate = _printer_devmode(win32print, handle, 2)
        if _devmode_driver_data_size(candidate) <= 0:
            candidate = _driver_default_devmode(win32print, win32con, pywintypes, handle, printer_name)
    finally:
        win32print.ClosePrinter(handle)

    if _devmode_driver_data_size(candidate) <= 0:
        raise RuntimeError("Could not read a complete printer default DEVMODE.")

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
                raise RuntimeError(f"DocumentProperties returned {result}")
            win32print.SetPrinter(handle, 8, {"pDevMode": candidate}, 0)
        finally:
            win32print.ClosePrinter(handle)
    except Exception as exc:
        raise RuntimeError(
            "初始化打印默认值失败。请以管理员权限设置一次该打印机的 Printing Defaults，"
            "或在设置中关闭“初始化打印默认值检查”。"
        ) from exc
    return True


def restore_printer_settings(printer_name: str, data_file: Path, initialize_defaults: bool = True) -> None:
    if sys.platform != "win32":
        raise RuntimeError("PrintUI 仅支持 Windows")
    if not data_file.exists():
        raise FileNotFoundError(data_file)
    try:
        if initialize_defaults:
            ensure_printer_defaults_initialized(printer_name)
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
        ])
    except Exception as exc:
        raise RuntimeError(f"恢复打印机预设失败: {exc}") from exc


def save_printer_settings(printer_name: str, data_file: Path, initialize_defaults: bool = True) -> None:
    if sys.platform != "win32":
        raise RuntimeError("PrintUI 仅支持 Windows")
    data_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        if initialize_defaults:
            ensure_printer_defaults_initialized(printer_name)
        _run_printui([
            "/Ss",
            f"/n{printer_name}",
            f"/a{str(data_file)}",
            "d",
            "g",
            "u",
            "c",
        ])
    except Exception as exc:
        raise RuntimeError(f"保存打印机预设失败: {exc}") from exc


def open_printer_preferences(printer_name: str) -> None:
    if sys.platform != "win32":
        raise RuntimeError("PrintUI 仅支持 Windows")
    _run_printui(["/e", f"/n{printer_name}"], check=False)


def open_printer_properties(printer_name: str) -> None:
    if sys.platform != "win32":
        raise RuntimeError("PrintUI 仅支持 Windows")
    _run_printui(["/p", f"/n{printer_name}"], check=False)
