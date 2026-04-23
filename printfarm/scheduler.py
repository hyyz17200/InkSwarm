from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from math import floor
from typing import Iterable

from .models import TaskItem, WorkerConfig, WorkerTaskBatch


@dataclass
class SchedulerState:
    ring_index: int = 0


class WeightedScheduler:
    def __init__(self) -> None:
        self.state = SchedulerState()

    def allocate(self, task: TaskItem, workers: Iterable[WorkerConfig]) -> list[WorkerTaskBatch]:
        eligible = [w for w in workers if self._worker_accepts_task(w)]
        if not eligible:
            raise RuntimeError(f"没有可用 Worker 能处理任务 {task.file_name()}")

        ordered = self._rotate_workers(eligible)
        assignments = self._allocate_proportional_copies(task.copies, ordered)
        if not assignments:
            raise RuntimeError("没有可调度的 Worker")

        # 轮询顺序保持为当前轮转顺序，但每台机器只发送一批，避免重复 RIP。
        result: list[WorkerTaskBatch] = []
        for worker in ordered:
            copies = assignments.get(worker.name, 0)
            if copies <= 0:
                continue
            result.append(
                WorkerTaskBatch(
                    task=task,
                    worker_name=worker.name,
                    printer_name=worker.printer_name,
                    preset_name=worker.active_preset,
                    copies=copies,
                )
            )

        self.state.ring_index = (self.state.ring_index + task.copies) % len(ordered)
        return result

    def _rotate_workers(self, eligible: list[WorkerConfig]) -> list[WorkerConfig]:
        if not eligible:
            return []
        start = self.state.ring_index % len(eligible)
        return eligible[start:] + eligible[:start]

    def _allocate_proportional_copies(self, total_copies: int, workers: list[WorkerConfig]) -> dict[str, int]:
        if total_copies <= 0 or not workers:
            return {}

        total_speed = sum(max(1, int(worker.weight)) for worker in workers)
        if total_speed <= 0:
            raise RuntimeError("可用 Worker 的速度总和无效")

        assignments: dict[str, int] = defaultdict(int)
        remainders: list[tuple[float, int, WorkerConfig]] = []
        allocated = 0
        for index, worker in enumerate(workers):
            speed = max(1, int(worker.weight))
            exact = (total_copies * speed) / total_speed
            whole = floor(exact)
            assignments[worker.name] = whole
            allocated += whole
            remainders.append((exact - whole, index, worker))

        remaining = total_copies - allocated
        if remaining > 0:
            remainders.sort(key=lambda item: (-item[0], item[1]))
            for _, _, worker in remainders[:remaining]:
                assignments[worker.name] += 1

        return assignments

    @staticmethod
    def _worker_accepts_task(worker: WorkerConfig) -> bool:
        return worker.enabled and bool(worker.printer_name.strip())
