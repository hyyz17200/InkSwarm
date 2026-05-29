from __future__ import annotations

from pathlib import Path
import threading
import time


QUEUE_LIMIT_LOG_KEY = "worker-queue-limit-paused"
QUEUE_LIMIT_LOG_MARKERS = ("队列等待任务数 ", "已达到上限", "暂停该 Worker 发送。")


def regular_log_once_key(text: str) -> str | None:
    if all(marker in text for marker in QUEUE_LIMIT_LOG_MARKERS):
        return QUEUE_LIMIT_LOG_KEY
    return None


class RegularLogFilter:
    def __init__(self) -> None:
        self._seen_once_keys: set[str] = set()

    def should_write(self, text: str) -> bool:
        key = regular_log_once_key(text)
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
