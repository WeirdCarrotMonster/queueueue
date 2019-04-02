from collections import defaultdict
from typing import DefaultDict, Iterable, Tuple


class StatCollector:

    def __init__(self) -> None:
        self.tasks_received_total: int = 0
        self.tasks_received: DefaultDict[str, int] = defaultdict(int)

    def push_task_received(self, pool: str) -> None:
        self.tasks_received_total += 1
        self.tasks_received[pool] += 1

    def stat_iter(self) -> Iterable[Tuple[str, int]]:
        yield ("tasks_received.total", self.tasks_received_total)

        for pool_name, task_counter in self.tasks_received.items():
            pool_name = pool_name.replace(".", "_")
            yield (f"tasks_received.pool.{pool_name}", task_counter)
