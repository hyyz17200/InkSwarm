from __future__ import annotations

from pathlib import Path
import threading
import time


QUEUE_LIMIT_LOG_KEY = "worker-queue-limit-paused"


def regular_log_once_key(_text: str, once_key: str | None = None) -> str | None:
    if once_key:
        return once_key
    return None


class RegularLogFilter:
    def __init__(self) -> None:
        self._seen_once_keys: set[str] = set()

    def reset(self) -> None:
        self._seen_once_keys.clear()

    def should_write(self, text: str, once_key: str | None = None) -> bool:
        key = regular_log_once_key(text, once_key=once_key)
        if key is None:
            return True
        if key in self._seen_once_keys:
            return False
        self._seen_once_keys.add(key)
        return True


class LocalLogWriter:
    def __init__(self, logs_dir: Path) -> None:
        self.logs_dir = logs_dir
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def append_line(self, text: str) -> None:
        day = time.strftime("%Y-%m-%d", time.localtime())
        target = self.logs_dir / f"{day}.log"
        with self._lock:
            with target.open("a", encoding="utf-8") as fh:
                fh.write(text.rstrip("\n") + "\n")
