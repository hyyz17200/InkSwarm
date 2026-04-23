from __future__ import annotations

from pathlib import Path
import threading
import time


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
