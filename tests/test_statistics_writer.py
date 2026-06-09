from __future__ import annotations

from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
import csv
import tempfile
import time

from printfarm.statistics_writer import CSV_HEADER, LEGACY_CSV_HEADER, StatisticsTaskRecord, MonthlyStatisticsWriter


def ts(year: int, month: int, day: int, hour: int = 10, minute: int = 0, second: int = 0) -> float:
    return time.mktime((year, month, day, hour, minute, second, 0, 0, -1))


def fmt(value: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def read_dict_rows(root: Path, month: str = "2026-05") -> list[dict[str, str]]:
    with (root / f"{month}.csv").open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


class MonthlyStatisticsWriterTests(TestCase):
    def test_finalized_partial_run_records_successful_copies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            writer = MonthlyStatisticsWriter(root)
            started_at = ts(2026, 5, 1)
            task_started_at = started_at + 10
            last_success_at = started_at + 25

            writer.begin_run("run-partial", started_at, [StatisticsTaskRecord("task-a", "job.pdf", 9)])
            writer.mark_task_started("run-partial", "task-a", "job.pdf", 9, task_started_at)
            for offset in range(6):
                writer.record_success("run-partial", "task-a", "job.pdf", 9, success_at_ts=started_at + 20 + offset)
            result = writer.finish_run("run-partial", started_at + 100)

            self.assertTrue(result.ok)
            self.assertFalse(any((root / "pending_runs").glob("*.json")))
            rows = read_dict_rows(root)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["任务启动时刻"], fmt(task_started_at))
            self.assertEqual(rows[0]["最后成功时刻"], fmt(last_success_at))
            self.assertEqual(rows[0]["文件名"], "job.pdf")
            self.assertEqual(rows[0]["请求张数"], "9")
            self.assertEqual(rows[0]["成功张数"], "6")
            self.assertEqual(rows[0]["未完成张数"], "3")
            self.assertEqual(rows[0]["完成状态"], "未完成")

    def test_unfinalized_pending_run_recovers_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            started_at = ts(2026, 5, 2)
            writer = MonthlyStatisticsWriter(root)
            writer.begin_run("run-crash", started_at, [StatisticsTaskRecord("task-a", "crash.pdf", 5)])
            writer.record_success("run-crash", "task-a", "crash.pdf", 5, copies_done=2, success_at_ts=started_at + 30)

            restarted_writer = MonthlyStatisticsWriter(root)
            result = restarted_writer.flush_pending_runs()

            self.assertTrue(result.ok)
            self.assertFalse(any((root / "pending_runs").glob("*.json")))
            rows = read_dict_rows(root)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["文件名"], "crash.pdf")
            self.assertEqual(rows[0]["请求张数"], "5")
            self.assertEqual(rows[0]["成功张数"], "2")
            self.assertEqual(rows[0]["未完成张数"], "3")
            self.assertEqual(rows[0]["完成状态"], "未完成")

    def test_locked_csv_keeps_pending_and_later_upserts_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            started_at = ts(2026, 5, 3)
            writer = MonthlyStatisticsWriter(root)
            writer.begin_run("run-locked", started_at, [StatisticsTaskRecord("task-a", "locked.pdf", 4)])
            writer.record_success("run-locked", "task-a", "locked.pdf", 4, copies_done=4, success_at_ts=started_at + 40)

            with patch.object(writer, "_write_csv_rows", side_effect=PermissionError("locked by Excel")):
                result = writer.finish_run("run-locked", started_at + 100)

            self.assertFalse(result.ok)
            self.assertEqual(result.pending_runs, 1)
            self.assertTrue(any((root / "pending_runs").glob("*.json")))
            self.assertFalse((root / "2026-05.csv").exists())

            retry = writer.flush_pending_runs()
            retry_again = writer.flush_pending_runs()

            self.assertTrue(retry.ok)
            self.assertTrue(retry_again.ok)
            rows = read_dict_rows(root)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["运行ID"], "run-locked")
            self.assertEqual(rows[0]["任务ID"], "task-a")
            self.assertEqual(rows[0]["成功张数"], "4")

    def test_legacy_three_column_csv_is_migrated_when_new_row_is_written(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_path = root / "2026-05.csv"
            with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
                writer_csv = csv.writer(fh)
                writer_csv.writerow(["任务启动时刻", "文件名", "文件数量"])
                writer_csv.writerow(["2026-05-01 09:00:00", "legacy.pdf", "7"])

            writer = MonthlyStatisticsWriter(root)
            started_at = ts(2026, 5, 4)
            writer.begin_run("run-new", started_at, [StatisticsTaskRecord("task-new", "new.pdf", 2)])
            writer.record_success("run-new", "task-new", "new.pdf", 2, copies_done=2, success_at_ts=started_at + 20)
            result = writer.finish_run("run-new", started_at + 30)

            self.assertTrue(result.ok)
            with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
                raw_rows = list(csv.reader(fh))
            self.assertEqual(raw_rows[0], CSV_HEADER)

            rows = read_dict_rows(root)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["文件名"], "legacy.pdf")
            self.assertEqual(rows[0]["请求张数"], "7")
            self.assertEqual(rows[0]["成功张数"], "7")
            self.assertEqual(rows[0]["运行ID"], "legacy-000001")
            self.assertEqual(rows[1]["文件名"], "new.pdf")
            self.assertEqual(rows[1]["成功张数"], "2")

    def test_read_monthly_report_normalizes_legacy_csv_without_rewriting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_path = root / "2026-05.csv"
            with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
                writer_csv = csv.writer(fh)
                writer_csv.writerow(LEGACY_CSV_HEADER)
                writer_csv.writerow(["2026-05-01 09:00:00", "legacy.pdf", "7"])

            writer = MonthlyStatisticsWriter(root)
            report = writer.read_monthly_report("2026-05")

            self.assertTrue(report.exists)
            self.assertEqual(report.month, "2026-05")
            self.assertEqual(report.header, tuple(CSV_HEADER))
            self.assertEqual(len(report.rows), 1)
            self.assertEqual(report.rows[0][2], "legacy.pdf")
            self.assertEqual(report.rows[0][3], "7")
            self.assertEqual(report.rows[0][4], "7")
            self.assertEqual(report.rows[0][7], "legacy-000001")
            self.assertEqual(report.total_success_copies, 7)
            with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
                raw_rows = list(csv.reader(fh))
            self.assertEqual(raw_rows[0], LEGACY_CSV_HEADER)

    def test_read_daily_report_filters_current_month_rows_by_start_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_path = root / "2026-05.csv"
            with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
                writer_csv = csv.writer(fh)
                writer_csv.writerow(CSV_HEADER)
                writer_csv.writerow([
                    "2026-05-04 09:00:00",
                    "2026-05-04 09:10:00",
                    "today.pdf",
                    "3",
                    "3",
                    "0",
                    "完成",
                    "run-today",
                    "task-today",
                ])
                writer_csv.writerow([
                    "2026-05-05 09:00:00",
                    "2026-05-05 09:10:00",
                    "other-day.pdf",
                    "2",
                    "2",
                    "0",
                    "完成",
                    "run-other-day",
                    "task-other-day",
                ])

            writer = MonthlyStatisticsWriter(root)
            report = writer.read_daily_report("2026-05-04")

            self.assertTrue(report.exists)
            self.assertEqual(report.month, "2026-05")
            self.assertEqual(report.csv_path, csv_path)
            self.assertEqual(len(report.rows), 1)
            self.assertEqual(report.rows[0][2], "today.pdf")
            self.assertEqual(report.total_success_copies, 3)
