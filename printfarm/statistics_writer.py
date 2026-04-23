from __future__ import annotations

from pathlib import Path
import csv
import os
import tempfile
import threading
import time


class MonthlyStatisticsWriter:
    def __init__(self, statistics_dir: Path) -> None:
        self.statistics_dir = statistics_dir
        self.statistics_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def append_success(self, started_at_ts: float, file_name: str, quantity: int) -> None:
        month = time.strftime("%Y-%m", time.localtime(started_at_ts))
        target = self.statistics_dir / f"{month}.csv"
        started_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(started_at_ts))
        row = [started_at, file_name, str(int(quantity))]
        with self._lock:
            rows: list[list[str]] = []
            if target.exists():
                with target.open("r", encoding="utf-8-sig", newline="") as fh:
                    reader = csv.reader(fh)
                    rows = [r for r in reader]
            if not rows or rows[0] != ["任务启动时刻", "文件名", "文件数量"]:
                rows = [["任务启动时刻", "文件名", "文件数量"]] + [r for r in rows if r]
            rows.append(row)

            tmp_fd, tmp_name = tempfile.mkstemp(prefix=target.stem + ".", suffix=".tmp", dir=str(self.statistics_dir))
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8-sig", newline="") as fh:
                    writer = csv.writer(fh)
                    writer.writerows(rows)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_name, target)
                try:
                    dir_fd = os.open(str(self.statistics_dir), os.O_RDONLY)
                except OSError:
                    dir_fd = None
                if dir_fd is not None:
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
            finally:
                try:
                    if os.path.exists(tmp_name):
                        os.unlink(tmp_name)
                except OSError:
                    pass
