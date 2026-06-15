from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, cast
import csv
import json
import os
import re
import tempfile
import threading
import time
import uuid


CSV_HEADER = [
    "Task Started At",
    "Last Success At",
    "File Name",
    "Requested Copies",
    "Successful Copies",
    "Unfinished Copies",
    "Completion Status",
    "Run ID",
    "Task ID",
]
CHINESE_CSV_HEADER = [
    "任务启动时刻",
    "最后成功时刻",
    "文件名",
    "请求张数",
    "成功张数",
    "未完成张数",
    "完成状态",
    "运行ID",
    "任务ID",
]
LEGACY_CSV_HEADER = ["任务启动时刻", "文件名", "文件数量"]


@dataclass(frozen=True)
class StatisticsTaskRecord:
    task_id: str
    file_name: str
    requested_copies: int


@dataclass(frozen=True)
class StatisticsFlushResult:
    flushed_runs: int
    pending_runs: int
    errors: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors and self.pending_runs == 0


@dataclass(frozen=True)
class MonthlyStatisticsReport:
    month: str
    csv_path: Path
    exists: bool
    header: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]

    @property
    def total_success_copies(self) -> int:
        try:
            success_index = self.header.index("Successful Copies")
        except ValueError:
            success_index = 4
        total = 0
        for row in self.rows:
            if len(row) <= success_index:
                continue
            try:
                total += max(0, int(row[success_index]))
            except (TypeError, ValueError):
                continue
        return total


class MonthlyStatisticsWriter:
    def __init__(self, statistics_dir: Path) -> None:
        self.statistics_dir = statistics_dir
        self.pending_dir = self.statistics_dir / "pending_runs"
        self.statistics_dir.mkdir(parents=True, exist_ok=True)
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def new_run_id(self) -> str:
        return uuid.uuid4().hex

    def begin_run(self, run_id: str, started_at_ts: float, tasks: Iterable[StatisticsTaskRecord]) -> None:
        safe_run_id = self._safe_run_id(run_id)
        payload = self._empty_payload(safe_run_id, started_at_ts)
        for task in tasks:
            payload["tasks"][str(task.task_id)] = {
                "task_id": str(task.task_id),
                "file_name": str(task.file_name),
                "requested_copies": max(0, int(task.requested_copies)),
                "success_copies": 0,
                "started_at_ts": float(started_at_ts),
                "last_success_at_ts": None,
            }
        with self._lock:
            self._write_payload(payload)

    def mark_task_started(
        self,
        run_id: str,
        task_id: str,
        file_name: str,
        requested_copies: int,
        started_at_ts: float,
    ) -> None:
        safe_run_id = self._safe_run_id(run_id)
        with self._lock:
            payload = self._load_or_create_payload(safe_run_id, started_at_ts)
            task = self._ensure_task(payload, task_id, file_name, requested_copies, started_at_ts)
            task["started_at_ts"] = float(started_at_ts)
            self._write_payload(payload)

    # Design note (kept synchronous on purpose): record_success runs once per printed
    # copy on each worker thread and rewrites the whole pending-run JSON with an fsync
    # under the single self._lock, so all workers serialize here (one shared file == one
    # writer at a time, ~one fsync per copy). Measured cost is negligible on SSD (~1-3%
    # lock utilization even at 20 workers x 1 page/s) and only bites on slow HDD/network/
    # USB drives. We deliberately do NOT batch these writes or move them to a background
    # thread: durably recording every spooled copy before continuing is worth the tiny
    # overhead, and an async writer would risk losing the last events on a crash.
    def record_success(
        self,
        run_id: str,
        task_id: str,
        file_name: str,
        requested_copies: int,
        copies_done: int = 1,
        success_at_ts: float | None = None,
    ) -> None:
        copies = int(copies_done)
        if copies <= 0:
            return
        safe_run_id = self._safe_run_id(run_id)
        now = time.time() if success_at_ts is None else float(success_at_ts)
        with self._lock:
            payload = self._load_or_create_payload(safe_run_id, now)
            task = self._ensure_task(payload, task_id, file_name, requested_copies, now)
            task["success_copies"] = max(0, int(task.get("success_copies", 0))) + copies
            task["last_success_at_ts"] = now
            self._write_payload(payload)

    def finish_run(self, run_id: str, ended_at_ts: float | None = None) -> StatisticsFlushResult:
        safe_run_id = self._safe_run_id(run_id)
        now = time.time() if ended_at_ts is None else float(ended_at_ts)
        with self._lock:
            payload = self._load_payload_path(self._pending_path(safe_run_id))
            if payload is not None:
                payload["finalized"] = True
                payload["ended_at_ts"] = now
                self._write_payload(payload)
        return self.flush_pending_runs(run_ids={safe_run_id})

    def flush_pending_runs(self, run_ids: set[str] | None = None) -> StatisticsFlushResult:
        selected = {self._safe_run_id(run_id) for run_id in run_ids} if run_ids else None
        flushed = 0
        errors: list[str] = []
        with self._lock:
            paths = sorted(self.pending_dir.glob("*.json"))
            for path in paths:
                run_id = path.stem
                if selected is not None and run_id not in selected:
                    continue
                try:
                    payload = self._load_payload_path(path)
                    if payload is None:
                        continue
                    rows = self._rows_from_payload(payload)
                    if rows:
                        self._upsert_csv_rows(payload, rows)
                    path.unlink(missing_ok=True)
                    flushed += 1
                except Exception as exc:
                    errors.append(f"{path.name}: {exc}")
        pending = len(list(self.pending_dir.glob("*.json")))
        if selected is not None:
            pending = len([p for p in self.pending_dir.glob("*.json") if p.stem in selected])
        return StatisticsFlushResult(flushed_runs=flushed, pending_runs=pending, errors=tuple(errors))

    def append_success(self, started_at_ts: float, file_name: str, quantity: int) -> None:
        run_id = self.new_run_id()
        task_id = uuid.uuid4().hex
        task = StatisticsTaskRecord(task_id=task_id, file_name=file_name, requested_copies=int(quantity))
        self.begin_run(run_id, started_at_ts, [task])
        self.record_success(run_id, task_id, file_name, int(quantity), int(quantity), started_at_ts)
        result = self.finish_run(run_id, started_at_ts)
        if not result.ok:
            message = "; ".join(result.errors) or f"{result.pending_runs} pending runs"
            raise PermissionError(message)

    def read_monthly_report(self, month: str | None = None) -> MonthlyStatisticsReport:
        selected_month = self._safe_month(month)
        target = self.statistics_dir / f"{selected_month}.csv"
        with self._lock:
            raw_rows = self._read_csv_rows(target)
            rows = self._normalize_csv_rows(raw_rows)
            exists = target.exists()
        return MonthlyStatisticsReport(
            month=selected_month,
            csv_path=target,
            exists=exists,
            header=tuple(CSV_HEADER),
            rows=tuple(tuple(row) for row in rows),
        )

    def read_daily_report(self, day: str | None = None) -> MonthlyStatisticsReport:
        selected_day = self._safe_day(day)
        report = self.read_monthly_report(selected_day[:7])
        return MonthlyStatisticsReport(
            month=report.month,
            csv_path=report.csv_path,
            exists=report.exists,
            header=report.header,
            rows=tuple(row for row in report.rows if row and row[0].startswith(selected_day)),
        )

    def _safe_month(self, month: str | None = None) -> str:
        if month is None:
            return time.strftime("%Y-%m", time.localtime())
        value = str(month).strip()
        if not re.fullmatch(r"\d{4}-\d{2}", value):
            raise ValueError(f"invalid statistics month: {month!r}")
        return value

    def _safe_day(self, day: str | None = None) -> str:
        if day is None:
            return time.strftime("%Y-%m-%d", time.localtime())
        value = str(day).strip()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError(f"invalid statistics day: {day!r}")
        return value

    def _safe_run_id(self, run_id: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(run_id)).strip("._")
        return safe or uuid.uuid4().hex

    def _pending_path(self, run_id: str) -> Path:
        return self.pending_dir / f"{run_id}.json"

    def _empty_payload(self, run_id: str, started_at_ts: float) -> dict:
        return {
            "schema_version": 1,
            "run_id": run_id,
            "started_at_ts": float(started_at_ts),
            "ended_at_ts": None,
            "finalized": False,
            "tasks": {},
        }

    def _load_or_create_payload(self, run_id: str, started_at_ts: float) -> dict:
        payload = self._load_payload_path(self._pending_path(run_id))
        return payload if payload is not None else self._empty_payload(run_id, started_at_ts)

    def _load_payload_path(self, path: Path) -> dict | None:
        if not path.exists():
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("pending statistics payload is not an object")
        raw.setdefault("tasks", {})
        raw.setdefault("schema_version", 1)
        raw.setdefault("run_id", path.stem)
        raw.setdefault("started_at_ts", time.time())
        raw.setdefault("ended_at_ts", None)
        raw.setdefault("finalized", False)
        if not isinstance(raw["tasks"], dict):
            raw["tasks"] = {}
        return raw

    def _ensure_task(
        self,
        payload: dict,
        task_id: str,
        file_name: str,
        requested_copies: int,
        fallback_started_at_ts: float,
    ) -> dict:
        tasks = payload.setdefault("tasks", {})
        task_key = str(task_id)
        task = tasks.setdefault(
            task_key,
            {
                "task_id": task_key,
                "file_name": str(file_name),
                "requested_copies": max(0, int(requested_copies)),
                "success_copies": 0,
                "started_at_ts": float(fallback_started_at_ts),
                "last_success_at_ts": None,
            },
        )
        task["task_id"] = task_key
        task["file_name"] = str(file_name or task.get("file_name") or task_key)
        task["requested_copies"] = max(int(task.get("requested_copies", 0)), max(0, int(requested_copies)))
        task.setdefault("success_copies", 0)
        task.setdefault("started_at_ts", float(fallback_started_at_ts))
        task.setdefault("last_success_at_ts", None)
        return task

    def _write_payload(self, payload: dict) -> None:
        run_id = self._safe_run_id(str(payload.get("run_id", "")))
        payload["run_id"] = run_id
        target = self._pending_path(run_id)
        tmp_fd, tmp_name = tempfile.mkstemp(prefix=target.stem + ".", suffix=".tmp", dir=str(self.pending_dir))
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8", newline="") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
                fh.write("\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, target)
            self._fsync_dir(self.pending_dir)
        finally:
            self._cleanup_tmp(tmp_name)

    def _rows_from_payload(self, payload: dict) -> list[list[str]]:
        finalized = bool(payload.get("finalized", False))
        default_started_at = float(payload.get("started_at_ts", time.time()))
        run_id = str(payload.get("run_id", ""))
        rows: list[list[str]] = []
        tasks = payload.get("tasks", {})
        if not isinstance(tasks, dict):
            return rows
        for task_id in sorted(tasks):
            task = tasks[task_id]
            if not isinstance(task, dict):
                continue
            requested = max(0, int(task.get("requested_copies", 0)))
            success = max(0, int(task.get("success_copies", 0)))
            if success <= 0 and not finalized:
                continue
            started_at_ts = float(task.get("started_at_ts", default_started_at) or default_started_at)
            last_success_ts = task.get("last_success_at_ts")
            unfinished = max(0, requested - success)
            status = "Complete" if requested > 0 and success >= requested else "Incomplete"
            rows.append(
                [
                    self._format_ts(started_at_ts),
                    self._format_ts(float(last_success_ts)) if last_success_ts is not None else "",
                    str(task.get("file_name") or task_id),
                    str(requested),
                    str(success),
                    str(unfinished),
                    status,
                    run_id,
                    str(task.get("task_id") or task_id),
                ]
            )
        return rows

    def _upsert_csv_rows(self, payload: dict, new_rows: list[list[str]]) -> None:
        rows_by_target: dict[Path, list[list[str]]] = {}
        for row in new_rows:
            row = self._pad_csv_row(row)
            target = self._target_for_row(payload, row)
            rows_by_target.setdefault(target, []).append(row)

        for target in sorted(rows_by_target, key=lambda item: str(item)):
            self._upsert_target_csv_rows(target, rows_by_target[target])

    def _upsert_target_csv_rows(self, target: Path, new_rows: list[list[str]]) -> None:
        current_rows = self._read_csv_rows(target)
        data_rows = self._normalize_csv_rows(current_rows)
        index: dict[tuple[str, str], int] = {}
        deduped: list[list[str]] = []
        for row in data_rows:
            key = (row[7], row[8])
            if key[0] and key[1]:
                if key in index:
                    deduped[index[key]] = row
                else:
                    index[key] = len(deduped)
                    deduped.append(row)
            else:
                deduped.append(row)
        for row in new_rows:
            key = (row[7], row[8])
            if key in index:
                deduped[index[key]] = row
            else:
                index[key] = len(deduped)
                deduped.append(row)
        self._write_csv_rows(target, [CSV_HEADER] + deduped)

    def _target_for_row(self, payload: dict, row: list[str]) -> Path:
        month = self._month_from_row(row)
        if month is None:
            return self._target_for_payload(payload)
        return self.statistics_dir / f"{month}.csv"

    @staticmethod
    def _month_from_row(row: list[str]) -> str | None:
        if not row:
            return None
        match = re.match(r"^(\d{4}-\d{2})-\d{2}(?:\s|$)", str(row[0]).strip())
        return match.group(1) if match else None

    def _target_for_payload(self, payload: dict) -> Path:
        started_at_ts = float(payload.get("started_at_ts", time.time()))
        month = time.strftime("%Y-%m", time.localtime(started_at_ts))
        return self.statistics_dir / f"{month}.csv"

    def _format_ts(self, ts: float) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

    def _read_csv_rows(self, target: Path) -> list[list[str]]:
        if not target.exists():
            return []
        with target.open("r", encoding="utf-8-sig", newline="") as fh:
            return [row for row in csv.reader(fh)]

    def _normalize_csv_rows(self, rows: list[list[str]]) -> list[list[str]]:
        if not rows:
            return []
        header = rows[0]
        if header == CSV_HEADER:
            return [self._normalize_csv_row_values(row) for row in rows[1:] if any(row)]
        if header == CHINESE_CSV_HEADER:
            return [self._normalize_csv_row_values(row) for row in rows[1:] if any(row)]
        if header == LEGACY_CSV_HEADER:
            return self._migrate_legacy_rows(rows[1:])
        if len(header) == len(LEGACY_CSV_HEADER):
            return self._migrate_legacy_rows(rows)
        return [self._normalize_csv_row_values(row) for row in rows[1:] if any(row)]

    def _migrate_legacy_rows(self, rows: list[list[str]]) -> list[list[str]]:
        migrated: list[list[str]] = []
        for index, row in enumerate(rows, start=1):
            if len(row) < 3 or not any(row):
                continue
            quantity = max(0, self._safe_int(row[2]))
            migrated.append(
                [
                    row[0],
                    row[0],
                    row[1],
                    str(quantity),
                    str(quantity),
                    "0",
                    "Complete",
                    f"legacy-{index:06d}",
                    f"legacy-{index:06d}",
                ]
            )
        return migrated

    def _normalize_csv_row_values(self, row: list[str]) -> list[str]:
        normalized = self._pad_csv_row(row)
        normalized[6] = self._normalize_completion_status(normalized[6])
        return normalized

    @staticmethod
    def _normalize_completion_status(value: str) -> str:
        text = str(value).strip()
        if text == "完成":
            return "Complete"
        if text == "未完成":
            return "Incomplete"
        return text

    def _pad_csv_row(self, row: list[str]) -> list[str]:
        padded = [str(value) for value in row[: len(CSV_HEADER)]]
        padded.extend([""] * (len(CSV_HEADER) - len(padded)))
        return padded

    def _write_csv_rows(self, target: Path, rows: list[list[str]]) -> None:
        tmp_fd, tmp_name = tempfile.mkstemp(prefix=target.stem + ".", suffix=".tmp", dir=str(self.statistics_dir))
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8-sig", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerows(rows)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, target)
            self._fsync_dir(self.statistics_dir)
        finally:
            self._cleanup_tmp(tmp_name)

    def _fsync_dir(self, directory: Path) -> None:
        try:
            dir_fd = os.open(str(directory), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def _cleanup_tmp(self, tmp_name: str) -> None:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass

    def _safe_int(self, value: object) -> int:
        try:
            return int(cast(Any, value))
        except (TypeError, ValueError):
            return 0
