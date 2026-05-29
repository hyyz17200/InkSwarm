from __future__ import annotations

from unittest import TestCase

from printfarm.local_logger import RegularLogFilter, regular_log_once_key


class RegularLogFilterTests(TestCase):
    def test_queue_limit_message_is_written_once(self) -> None:
        log_filter = RegularLogFilter()
        message = "[12:00:00] INFO: WorkerA: 队列等待任务数 3 已达到上限 3，暂停该 Worker 发送。"

        self.assertTrue(log_filter.should_write(message))
        self.assertFalse(log_filter.should_write(message))
        self.assertFalse(
            log_filter.should_write("[12:01:00] INFO: WorkerB: 队列等待任务数 4 已达到上限 4，暂停该 Worker 发送。")
        )

    def test_other_messages_are_not_suppressed(self) -> None:
        log_filter = RegularLogFilter()
        message = "[12:00:00] INFO: WorkerA: 使用缓存 abc，打印 job.pdf ×1"

        self.assertIsNone(regular_log_once_key(message))
        self.assertTrue(log_filter.should_write(message))
        self.assertTrue(log_filter.should_write(message))

