from __future__ import annotations

from pathlib import Path
import threading
from unittest import TestCase

from printfarm.models import TaskItem, WorkerConfig, WorkerTaskBatch
from printfarm.scheduler import WeightedScheduler
from printfarm.tail_balance import TailBalanceCoordinator


def worker(name: str, weight: int = 1) -> WorkerConfig:
    return WorkerConfig(
        name=name,
        directory=Path(name),
        printer_name=f"Printer {name}",
        enabled=True,
        weight=weight,
        active_preset="default",
    )


def task(copies: int = 1) -> TaskItem:
    return TaskItem(file_path=Path("job.pdf"), copies=copies, task_id="task-a")


def batch(item: TaskItem, owner: WorkerConfig, copies: int) -> WorkerTaskBatch:
    return WorkerTaskBatch(
        task=item,
        worker_name=owner.name,
        printer_name=owner.printer_name,
        preset_name=owner.active_preset,
        copies=copies,
    )


class StaticSchedulerRegressionTests(TestCase):
    def test_weighted_scheduler_initial_allocation_is_unchanged(self) -> None:
        item = task(copies=30)
        workers = [worker("A", weight=25), worker("B", weight=5)]

        result = WeightedScheduler().allocate(item, workers)

        self.assertEqual([(entry.worker_name, entry.copies) for entry in result], [("A", 25), ("B", 5)])


class TailBalanceCoordinatorTests(TestCase):
    def test_disabled_coordinator_does_not_steal(self) -> None:
        item = task(copies=3)
        workers = [worker("A"), worker("B")]
        coordinator = TailBalanceCoordinator(workers, idle_seconds=1, enabled=False)
        coordinator.add_batch(batch(item, workers[0], copies=3))
        coordinator.close_dispatch()

        self.assertIsNone(coordinator.try_steal_now("B", idle_elapsed_seconds=30))
        self.assertEqual(coordinator.remaining_for_worker("A"), 3)

    def test_does_not_steal_before_static_dispatch_closes(self) -> None:
        item = task(copies=3)
        workers = [worker("A"), worker("B")]
        coordinator = TailBalanceCoordinator(workers, idle_seconds=1)
        coordinator.add_batch(batch(item, workers[0], copies=3))

        self.assertIsNone(coordinator.try_steal_now("B", idle_elapsed_seconds=30))
        self.assertEqual(coordinator.remaining_for_worker("A"), 3)

    def test_idle_threshold_must_be_reached_before_stealing(self) -> None:
        item = task(copies=3)
        workers = [worker("A"), worker("B")]
        coordinator = TailBalanceCoordinator(workers, idle_seconds=15)
        coordinator.add_batch(batch(item, workers[0], copies=3))
        coordinator.close_dispatch()

        self.assertIsNone(coordinator.try_steal_now("B", idle_elapsed_seconds=14.9))
        self.assertEqual(coordinator.remaining_for_worker("A"), 3)

    def test_steals_one_pending_copy_and_leaves_donor_work(self) -> None:
        item = task(copies=3)
        workers = [worker("A"), worker("B")]
        coordinator = TailBalanceCoordinator(workers, idle_seconds=1)
        coordinator.add_batch(batch(item, workers[0], copies=3))
        coordinator.close_dispatch()

        assignment = coordinator.try_steal_now("B", idle_elapsed_seconds=1)

        self.assertIsNotNone(assignment)
        assert assignment is not None
        self.assertEqual(assignment.stolen_from, "A")
        self.assertEqual(assignment.batch.worker_name, "B")
        self.assertEqual(assignment.batch.printer_name, "Printer B")
        self.assertEqual(assignment.batch.copies, 1)
        self.assertEqual(coordinator.remaining_for_worker("A"), 2)

    def test_donor_single_remaining_copy_is_not_stolen(self) -> None:
        item = task(copies=1)
        workers = [worker("A"), worker("B")]
        coordinator = TailBalanceCoordinator(workers, idle_seconds=1)
        coordinator.add_batch(batch(item, workers[0], copies=1))
        coordinator.close_dispatch()

        self.assertIsNone(coordinator.try_steal_now("B", idle_elapsed_seconds=1))
        self.assertEqual(coordinator.remaining_for_worker("A"), 1)

    def test_active_batch_only_lends_unclaimed_remaining_copies(self) -> None:
        item = task(copies=3)
        workers = [worker("A"), worker("B")]
        coordinator = TailBalanceCoordinator(workers, idle_seconds=1)
        coordinator.add_batch(batch(item, workers[0], copies=3))
        coordinator.close_dispatch()
        donor_assignment = coordinator.get_next("A", threading.Event())
        assert donor_assignment is not None
        assert donor_assignment.active_id is not None

        self.assertTrue(coordinator.claim_active_copy("A", donor_assignment.active_id))
        stolen = coordinator.try_steal_now("B", idle_elapsed_seconds=1)

        self.assertIsNotNone(stolen)
        assert stolen is not None
        self.assertEqual(stolen.batch.copies, 1)
        self.assertEqual(coordinator.remaining_for_worker("A"), 1)
        self.assertIsNone(coordinator.try_steal_now("B", idle_elapsed_seconds=1))
