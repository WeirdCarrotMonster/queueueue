from collections import defaultdict
from typing import DefaultDict, Iterable, Tuple


class StatCollector:

    def __init__(self) -> None:
        self.tasks_received_total: int = 0
        self.tasks_received: DefaultDict[str, int] = defaultdict(int)

        self.tasks_completed_total: int = 0
        self.tasks_completed: DefaultDict[str, int] = defaultdict(int)

        self.tasks_processing_total: int = 0
        self.tasks_processing: DefaultDict[str, int] = defaultdict(int)

    def push_task_received(self, pool: str) -> None:
        self.tasks_received_total += 1
        self.tasks_received[pool] += 1

    def push_task_completed(self, pool: str) -> None:
        self.tasks_completed_total += 1
        self.tasks_completed[pool] += 1

    def push_task_processing(self, pool: str, seconds: int) -> None:
        self.tasks_processing_total += seconds
        self.tasks_processing[pool] += seconds

    def stat_iter(self) -> Iterable[Tuple[str, int]]:
        yield ("tasks_received.total", self.tasks_received_total)

        for pool_name, task_counter in self.tasks_received.items():
            pool_name = pool_name.replace(".", "_")
            yield (f"tasks_received.pool.{pool_name}", task_counter)

        yield ("tasks_completed.total", self.tasks_completed_total)

        for pool_name, task_counter in self.tasks_completed.items():
            pool_name = pool_name.replace(".", "_")
            yield (f"tasks_completed.pool.{pool_name}", task_counter)

        yield ("task_processing.total", self.tasks_processing_total)

        for pool_name, task_counter in self.tasks_processing.items():
            pool_name = pool_name.replace(".", "_")
            yield (f"task_processing.pool.{pool_name}", task_counter)
