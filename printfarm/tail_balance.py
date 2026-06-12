from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import threading
import time

from .i18n import normalize_language, translate
from .models import WorkerConfig, WorkerTaskBatch


@dataclass
class TailBalanceAssignment:
    batch: WorkerTaskBatch
    active_id: int | None
    stolen_from: str | None = None


@dataclass
class _ActiveBatch:
    worker_name: str
    batch: WorkerTaskBatch
    remaining: int


@dataclass
class _StolenBatch:
    donor_name: str
    batch: WorkerTaskBatch
    adds_outstanding_batch: bool


class TailBalanceCoordinator:
    def __init__(
        self,
        workers: list[WorkerConfig],
        idle_seconds: int = 15,
        enabled: bool = True,
        language: str = "en",
    ) -> None:
        self.language = normalize_language(language)
        self.enabled = bool(enabled)
        self.idle_seconds = max(1, int(idle_seconds))
        self._workers = {worker.name: worker for worker in workers}
        self._worker_order = [worker.name for worker in workers]
        self._pending: dict[str, deque[WorkerTaskBatch]] = {worker.name: deque() for worker in workers}
        self._active: dict[int, _ActiveBatch] = {}
        self._worker_active: dict[str, int] = {}
        self._idle_since: dict[str, float] = {}
        self._tail_ready: set[str] = set()
        self._dispatch_closed = False
        self._outstanding_batches = 0
        self._next_active_id = 1
        self._condition = threading.Condition()

    def add_batch(self, batch: WorkerTaskBatch) -> None:
        if batch.copies <= 0:
            return
        with self._condition:
            if self._dispatch_closed:
                raise RuntimeError(translate(self.language, "tail_balance.dispatch_closed"))
            self._pending.setdefault(batch.worker_name, deque()).append(batch)
            self._outstanding_batches += 1
            self._condition.notify_all()

    def close_dispatch(self) -> None:
        with self._condition:
            self._dispatch_closed = True
            self._condition.notify_all()

    def wait_until_done(self) -> None:
        with self._condition:
            while self._outstanding_batches > 0:
                self._condition.wait(0.1)

    def get_next(self, worker_name: str, stop_event: threading.Event) -> TailBalanceAssignment | None:
        while True:
            with self._condition:
                own_queue = self._pending.setdefault(worker_name, deque())
                if own_queue:
                    if worker_name not in self._tail_ready:
                        self._idle_since.pop(worker_name, None)
                    return self._start_assignment_locked(worker_name, own_queue.popleft())

                if self._dispatch_closed and self._outstanding_batches <= 0:
                    return None

                if not self._dispatch_closed or stop_event.is_set():
                    self._condition.wait(0.1)
                    continue

                if worker_name in self._tail_ready:
                    assignment = self._try_steal_locked(worker_name)
                    if assignment is not None:
                        return assignment
                    self._condition.wait(0.1)
                    continue

                now = time.monotonic()
                idle_since = self._idle_since.setdefault(worker_name, now)
                idle_elapsed = now - idle_since
                if idle_elapsed >= self.idle_seconds:
                    self._tail_ready.add(worker_name)
                    assignment = self._try_steal_locked(worker_name)
                    if assignment is not None:
                        return assignment

                wait_seconds = max(0.05, min(0.5, self.idle_seconds - idle_elapsed))
                self._condition.wait(wait_seconds)

    def try_steal_now(self, worker_name: str, idle_elapsed_seconds: float) -> TailBalanceAssignment | None:
        with self._condition:
            if worker_name not in self._tail_ready and idle_elapsed_seconds < self.idle_seconds:
                return None
            if not self._dispatch_closed:
                return None
            self._tail_ready.add(worker_name)
            return self._try_steal_locked(worker_name)

    def claim_active_copy(self, worker_name: str, active_id: int) -> bool:
        with self._condition:
            active = self._active.get(active_id)
            if active is None or active.worker_name != worker_name:
                return False
            if active.remaining <= 0:
                return False
            active.remaining -= 1
            self._condition.notify_all()
            return True

    def complete_assignment(self, worker_name: str, active_id: int) -> None:
        with self._condition:
            active = self._active.pop(active_id, None)
            if active is None:
                return
            if active.worker_name == worker_name:
                self._worker_active.pop(worker_name, None)
            self._outstanding_batches = max(0, self._outstanding_batches - 1)
            self._condition.notify_all()

    def remaining_for_worker(self, worker_name: str) -> int:
        with self._condition:
            return self._remaining_for_worker_locked(worker_name)

    def outstanding_batches(self) -> int:
        with self._condition:
            return self._outstanding_batches

    def _start_assignment_locked(
        self,
        worker_name: str,
        batch: WorkerTaskBatch,
        stolen_from: str | None = None,
    ) -> TailBalanceAssignment:
        active_id = self._next_active_id
        self._next_active_id += 1
        self._active[active_id] = _ActiveBatch(
            worker_name=worker_name,
            batch=batch,
            remaining=max(0, int(batch.copies)),
        )
        self._worker_active[worker_name] = active_id
        return TailBalanceAssignment(batch=batch, active_id=active_id, stolen_from=stolen_from)

    def _try_steal_locked(self, worker_name: str) -> TailBalanceAssignment | None:
        if not self.enabled:
            return None
        if worker_name not in self._workers:
            return None

        donor_names = [
            donor_name
            for donor_name in self._worker_order
            if donor_name != worker_name and self._remaining_for_worker_locked(donor_name) > 1
        ]
        donor_names.sort(key=lambda donor_name: self._remaining_for_worker_locked(donor_name), reverse=True)

        for donor_name in donor_names:
            stolen = self._steal_pending_copy_locked(donor_name, worker_name)
            if stolen is None:
                stolen = self._steal_active_copy_locked(donor_name, worker_name)
            if stolen is None:
                continue
            if stolen.adds_outstanding_batch:
                self._outstanding_batches += 1
            assignment = self._start_assignment_locked(worker_name, stolen.batch, stolen_from=stolen.donor_name)
            self._condition.notify_all()
            return assignment
        return None

    def _steal_pending_copy_locked(self, donor_name: str, thief_name: str) -> _StolenBatch | None:
        donor_queue = self._pending.setdefault(donor_name, deque())
        if not donor_queue:
            return None

        for index in range(len(donor_queue) - 1, -1, -1):
            source = donor_queue[index]
            if source.copies <= 0:
                self._remove_pending_at(donor_queue, index)
                continue
            if self._remaining_for_worker_locked(donor_name) <= 1:
                return None
            stolen_batch = self._clone_for_worker(source, thief_name, copies=1)
            if source.copies > 1:
                source.copies -= 1
                return _StolenBatch(donor_name, stolen_batch, adds_outstanding_batch=True)
            self._remove_pending_at(donor_queue, index)
            return _StolenBatch(donor_name, stolen_batch, adds_outstanding_batch=False)
        return None

    def _steal_active_copy_locked(self, donor_name: str, thief_name: str) -> _StolenBatch | None:
        for active_id in sorted(self._active):
            active = self._active[active_id]
            if active.worker_name != donor_name:
                continue
            if active.remaining <= 1:
                continue
            active.remaining -= 1
            return _StolenBatch(donor_name, self._clone_for_worker(active.batch, thief_name, copies=1), adds_outstanding_batch=True)
        return None

    def _clone_for_worker(self, source: WorkerTaskBatch, worker_name: str, copies: int) -> WorkerTaskBatch:
        worker = self._workers[worker_name]
        return WorkerTaskBatch(
            task=source.task,
            worker_name=worker.name,
            printer_name=worker.printer_name,
            preset_name=worker.active_preset,
            copies=copies,
        )

    def _remaining_for_worker_locked(self, worker_name: str) -> int:
        pending = sum(max(0, int(batch.copies)) for batch in self._pending.setdefault(worker_name, deque()))
        active = sum(
            max(0, int(active_batch.remaining))
            for active_batch in self._active.values()
            if active_batch.worker_name == worker_name
        )
        return pending + active

    @staticmethod
    def _remove_pending_at(items: deque[WorkerTaskBatch], index: int) -> WorkerTaskBatch:
        items.rotate(-index)
        removed = items.popleft()
        items.rotate(index)
        return removed
