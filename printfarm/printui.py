from __future__ import annotations

from pathlib import Path
import subprocess
import sys


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


def restore_printer_settings(printer_name: str, data_file: Path) -> None:
    if sys.platform != "win32":
        raise RuntimeError("PrintUI 仅支持 Windows")
    if not data_file.exists():
        raise FileNotFoundError(data_file)
    try:
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


def save_printer_settings(printer_name: str, data_file: Path) -> None:
    if sys.platform != "win32":
        raise RuntimeError("PrintUI 仅支持 Windows")
    data_file.parent.mkdir(parents=True, exist_ok=True)
    try:
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
