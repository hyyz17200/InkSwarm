from __future__ import annotations

from unittest import TestCase

from printfarm.local_logger import QUEUE_LIMIT_LOG_KEY, RegularLogFilter, regular_log_once_key


class RegularLogFilterTests(TestCase):
    def test_explicit_once_key_suppresses_queue_limit_logs(self) -> None:
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
                "[12:01:00] INFO: WorkerB: Queue waiting jobs 4 reached limit 4; pausing this worker.",
                once_key=QUEUE_LIMIT_LOG_KEY,
            )
        )

    def test_reset_allows_once_key_again(self) -> None:
        log_filter = RegularLogFilter()

        self.assertTrue(log_filter.should_write("first", once_key=QUEUE_LIMIT_LOG_KEY))
        self.assertFalse(log_filter.should_write("second", once_key=QUEUE_LIMIT_LOG_KEY))

        log_filter.reset()

        self.assertTrue(log_filter.should_write("next run", once_key=QUEUE_LIMIT_LOG_KEY))
        self.assertFalse(log_filter.should_write("same run repeat", once_key=QUEUE_LIMIT_LOG_KEY))

    def test_queue_limit_text_without_once_key_is_not_special_cased(self) -> None:
        log_filter = RegularLogFilter()
        message = "[12:00:00] INFO: WorkerA: Queue waiting jobs 3 reached limit 3; pausing this worker."

        self.assertIsNone(regular_log_once_key(message))
        self.assertTrue(log_filter.should_write(message))
        self.assertTrue(log_filter.should_write(message))

    def test_other_messages_are_not_suppressed(self) -> None:
        log_filter = RegularLogFilter()
        message = "[12:00:00] INFO: WorkerA: using cache abc, printing job.pdf x1"

        self.assertIsNone(regular_log_once_key(message))
        self.assertTrue(log_filter.should_write(message))
        self.assertTrue(log_filter.should_write(message))
