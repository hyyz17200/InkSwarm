from __future__ import annotations

from contextlib import contextmanager
import threading
from typing import Callable, Iterator

from .debug_logger import debug_log
from .i18n import normalize_language, translate


class PrinterAccessCoordinator:
    def __init__(self, language: str = "en") -> None:
        self.language = normalize_language(language)
        self._guard = threading.Lock()
        self._locks: dict[str, threading.RLock] = {}

    @contextmanager
    def hold(
        self,
        printer_name: str,
        owner: str = "",
        wait_callback: Callable[[], None] | None = None,
        stop_event: threading.Event | None = None,
        poll_seconds: float = 0.2,
    ) -> Iterator[None]:
        key = self._normalize_printer_name(printer_name)
        lock = self._lock_for(key)
        label = owner or "<unknown>"
        if not lock.acquire(blocking=False):
            debug_log(f"printer access wait owner={label} printer={printer_name} key={key}")
            if wait_callback is not None:
                wait_callback()
            while True:
                if stop_event is not None and stop_event.is_set():
                    raise RuntimeError(translate(self.language, "printer_access.stopped"))
                if lock.acquire(timeout=max(0.05, poll_seconds)):
                    break
        try:
            debug_log(f"printer access acquired owner={label} printer={printer_name} key={key}")
            yield
        finally:
            lock.release()
            debug_log(f"printer access released owner={label} printer={printer_name} key={key}")

    def _lock_for(self, key: str) -> threading.RLock:
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.RLock()
                self._locks[key] = lock
            return lock

    @staticmethod
    def _normalize_printer_name(printer_name: str) -> str:
        return (printer_name or "").strip().casefold()
