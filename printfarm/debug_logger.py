from __future__ import annotations

from pathlib import Path
import atexit
import faulthandler
import sys
import threading
import time
import traceback
from typing import Callable, TextIO

_debug_lock = threading.Lock()
_debug_file_handle: TextIO | None = None
_debug_file_path: Path | None = None

# Alternative sink for processes that own no debug file (helper subprocesses):
# when set, debug_log() hands the raw message to this callable instead of
# writing to disk, so the parent process can merge it into the main debug.log.
_debug_forwarder: Callable[[str], None] | None = None


def set_debug_forwarder(forwarder: Callable[[str], None] | None) -> None:
    global _debug_forwarder
    _debug_forwarder = forwarder

# Block-buffered writes: instead of flushing every line (a syscall per line held
# under _debug_lock, which couples worker threads to disk latency), we let the OS
# batch writes and only flush after a burst of lines or a short idle interval.
_DEBUG_FLUSH_BUFFER_BYTES = 64 * 1024
_DEBUG_FLUSH_EVERY_LINES = 64
_DEBUG_FLUSH_INTERVAL_SECONDS = 0.5
_lines_since_flush = 0
_last_flush_monotonic = 0.0


def initialize_debug_logging(logs_dir: Path) -> Path:
    global _debug_file_handle, _debug_file_path, _lines_since_flush, _last_flush_monotonic
    logs_dir.mkdir(parents=True, exist_ok=True)
    debug_path = (logs_dir / "debug.log").resolve()
    try:
        debug_path.unlink(missing_ok=True)
    except Exception:
        pass
    _debug_file_path = debug_path
    _debug_file_handle = debug_path.open("a", encoding="utf-8", buffering=_DEBUG_FLUSH_BUFFER_BYTES)
    _lines_since_flush = 0
    _last_flush_monotonic = time.monotonic()
    _install_hooks()
    debug_log("debug logger initialized")
    return debug_path


def debug_log(message: str) -> None:
    forwarder = _debug_forwarder
    if forwarder is not None:
        try:
            forwarder(message.rstrip())
        except Exception:
            pass
        return
    handle = _debug_file_handle
    if handle is None:
        return
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"[{ts}] {message.rstrip()}\n"
    global _lines_since_flush, _last_flush_monotonic
    with _debug_lock:
        try:
            handle.write(line)
            _lines_since_flush += 1
            now = time.monotonic()
            if (
                _lines_since_flush >= _DEBUG_FLUSH_EVERY_LINES
                or (now - _last_flush_monotonic) >= _DEBUG_FLUSH_INTERVAL_SECONDS
            ):
                handle.flush()
                _lines_since_flush = 0
                _last_flush_monotonic = now
        except Exception:
            pass


def flush_debug_log() -> None:
    handle = _debug_file_handle
    if handle is None:
        return
    global _lines_since_flush, _last_flush_monotonic
    with _debug_lock:
        try:
            handle.flush()
            _lines_since_flush = 0
            _last_flush_monotonic = time.monotonic()
        except Exception:
            pass


def debug_exception(context: str, exc: BaseException) -> None:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    debug_log(f"EXCEPTION in {context}:\n{tb}")
    flush_debug_log()


def install_qt_message_handler() -> None:
    try:
        from PySide6.QtCore import qInstallMessageHandler
    except Exception:
        return

    def _handler(msg_type, context, message):
        file_name = getattr(context, "file", "") or ""
        line_no = getattr(context, "line", 0) or 0
        function_name = getattr(context, "function", "") or ""
        debug_log(f"QT[{int(msg_type)}] {message} ({file_name}:{line_no} {function_name})")

    try:
        qInstallMessageHandler(_handler)
        debug_log("Qt message handler installed")
    except Exception as exc:
        debug_exception("install_qt_message_handler", exc)


def _install_hooks() -> None:
    _install_fault_handler()
    _install_python_exception_hooks()
    atexit.register(_close_debug_file)


def _install_fault_handler() -> None:
    handle = _debug_file_handle
    if handle is None:
        return
    try:
        faulthandler.enable(handle, all_threads=True)
        debug_log("faulthandler enabled")
    except Exception as exc:
        debug_exception("faulthandler.enable", exc)


def _install_python_exception_hooks() -> None:
    def _sys_excepthook(exc_type, exc_value, exc_traceback):
        tb = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        debug_log(f"UNHANDLED EXCEPTION:\n{tb}")
        flush_debug_log()
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    sys.excepthook = _sys_excepthook

    if hasattr(threading, "excepthook"):
        def _threading_excepthook(args):
            tb = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
            thread_name = getattr(args.thread, "name", "<unknown thread>")
            debug_log(f"UNHANDLED THREAD EXCEPTION in {thread_name}:\n{tb}")
            flush_debug_log()
            if getattr(threading, "__excepthook__", None):
                threading.__excepthook__(args)
        threading.excepthook = _threading_excepthook  # type: ignore[assignment]


def _close_debug_file() -> None:
    global _debug_file_handle
    if _debug_file_handle is None:
        return
    try:
        debug_log("debug logger shutting down")
    finally:
        try:
            _debug_file_handle.close()
        except Exception:
            pass
        _debug_file_handle = None
