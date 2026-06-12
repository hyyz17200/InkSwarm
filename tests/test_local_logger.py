from __future__ import annotations

from unittest import TestCase

from printfarm.local_logger import QUEUE_LIMIT_LOG_KEY, RegularLogFilter, regular_log_once_key


class RegularLogFilterTests(TestCase):
    def test_explicit_once_key_suppresses_cross_language_queue_limit_logs(self) -> None:
        log_filter = RegularLogFilter()

        self.assertEqual(regular_log_once_key("anything", once_key=QUEUE_LIMIT_LOG_KEY), QUEUE_LIMIT_LOG_KEY)
        self.assertTrue(
            log_filter.should_write(
                "[12:00:00] INFO: WorkerA: Queue waiting jobs 3 reached limit 3; pausing this worker.",
                once_key=QUEUE_LIMIT_LOG_KEY,
            )
        )
        self.assertFalse(
            log_filter.should_write(
                "[12:01:00] INFO: WorkerB: 队列等待任务数 4 已达到上限 4，暂停该 Worker 发送。",
                once_key=QUEUE_LIMIT_LOG_KEY,
            )
        )

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

