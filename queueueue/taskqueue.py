import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple, Iterator


class Task(object):

    def __init__(self,
                 name: str,
                 locks: List[str],
                 pool: str,
                 args: List[Any],
                 kwargs: Dict[str, Any],
                 status: str = "pending",
                 **kw) -> None:
        if "id" in kw:
            self.id = uuid.UUID(kw.pop("id"))
        else:
            self.id = uuid.uuid4()
        self.name = name
        self.locks = frozenset(locks)
        self.pool = pool
        self.args = args or []
        self.kwargs = kwargs or {}
        self.status = status

        self.stdout = None
        self.stderr = None
        self.result = None
        self.traceback = None

        self.created = datetime.now(timezone.utc)
        self.finished = None  # type: Optional[datetime]
        self.taken = None  # type: Optional[datetime]

        self.completed = asyncio.Event()

    def __repr__(self) -> str:
        return "<{0} [{1}][{2}]>".format(self.name, self.id, ",".join(str(_) for _ in self.locks))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Task):  # pragma: no cover
            return False

        return (
            self.name == other.name and
            self.locks == other.locks and
            self.args == other.args and
            self.kwargs == other.kwargs
        )

    def is_equal_to(self, other: "Task", ignore_kwargs: Optional[Set[str]] = None):
        if ignore_kwargs is None:
            ignore_kwargs = frozenset()

        self_kwargs = set(self.kwargs.keys())
        other_kwargs = set(other.kwargs.keys())

        s_diff = self_kwargs.symmetric_difference(other_kwargs)
        if not s_diff.issubset(ignore_kwargs):
            return False

        s_keys = self_kwargs.intersection(other_kwargs)
        s_keys.difference_update(ignore_kwargs)

        for key in s_keys:
            if self.kwargs[key] != other.kwargs[key]:
                return False

        return (
                self.name == other.name and
                self.locks == other.locks and
                self.args == other.args
        )

    def complete(self, **data):
        for attr in ["stdout", "stderr", "result", "status", "traceback"]:
            if attr in data:
                setattr(self, attr, data[attr])

        self.completed.data = {
            "status": data.get("status"),
            "result": data.get("result")
        }
        self.finished = datetime.now(timezone.utc)
        self.completed.set()

    def for_json(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "name": self.name,
            "locks": list(self.locks),
            "pool": self.pool,
            "args": self.args,
            "kwargs": self.kwargs,
            "created": self.created.isoformat(),
            "taken": self.taken.isoformat() if self.taken else None
        }

    @property
    def worker_info(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "name": self.name,
            "args": self.args,
            "kwargs": self.kwargs
        }

    @property
    def full_info(self) -> Dict[str, Any]:
        return {
            "id": str(self.id),
            "name": self.name,
            "args": self.args,
            "kwargs": self.kwargs,
            "locks": list(self.locks),
            "pool": self.pool,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "result": self.result,
            "status": self.status,
            "traceback": self.traceback
        }

    @property
    def processing_duration(self) -> int:
        if not self.finished:  # pragma: no cover
            return 0

        return int((self.finished - self.created).total_seconds())


class MultiLockPriorityPoolQueue(object):

    def __init__(self):
        self._locks = {}  # type: Dict[str, Tuple[Task, datetime]]
        self._tasks = []  # type: List[Task]
        self._active_tasks = {}  # type: Dict[uuid.UUID, Task]
        self._logger = logging.getLogger("Queue")

    @property
    def task_count(self) -> int:
        return len(self._tasks)

    @property
    def tasks(self) -> Tuple[Task, ...]:
        return tuple(self._tasks)

    @property
    def tasks_taken(self) -> Tuple[Task, ...]:
        return tuple(self._active_tasks.values())

    @property
    def locks(self) -> FrozenSet[str]:
        return frozenset(self._locks)

    @property
    def iter_locks(self) -> Iterator[Tuple[str, Task, datetime]]:
        for key, value in self._locks.items():
            yield (key, value[0], value[1])

    @property
    def tasks_pending(self) -> Tuple[uuid.UUID, ...]:
        return tuple(task.id for task in self._tasks)

    @property
    def tasks_active(self) -> Tuple[uuid.UUID, ...]:
        return tuple(self._active_tasks.keys())

    def __len__(self) -> int:
        return len(self._tasks)

    def put(self,
            task: Task,
            unique: bool = False,
            unique_ignore_kwargs: Optional[Set[str]] = None) -> bool:
        if unique:
            for e_task in self._tasks:
                if e_task.is_equal_to(task, unique_ignore_kwargs):
                    self._logger.info("Task %s not unique", repr(task))
                    return False

        self._logger.info("Queued task %s", repr(task))
        self._tasks.append(task)
        self._logger.debug("Queue length: %s", len(self._tasks))

        return True

    def get(self, pool: str) -> Optional[Task]:
        for task in self._tasks:
            if task.pool != pool:
                continue
            if self.locks & task.locks:
                continue

            task.taken = datetime.now(timezone.utc)

            self._tasks.remove(task)
            self._active_tasks[task.id] = task

            for lock in task.locks:
                self._locks[lock] = (task, task.taken)

            self._logger.info("Sending task %s", repr(task))
            self._logger.debug("Active locks: %s", self.locks)
            return task
        return None

    def complete(self, task_id: str, data: Dict[str, Any]) -> Task:
        _task_id = uuid.UUID(task_id)
        task = self._active_tasks.pop(_task_id, None)

        if not task:
            raise LookupError

        self._logger.info("Completed task %s", repr(task))
        task.complete(**data)

        for lock in task.locks:
            self._locks.pop(lock, None)

        self._logger.debug("Queue length: %s", len(self._tasks))
        self._logger.debug("Active locks: %s", self.locks)

        return task

    def safe_remove(self, task_id: str):
        _task_id = uuid.UUID(task_id)

        if _task_id in self._active_tasks:
            task = self._active_tasks.pop(_task_id)

            for lock in task.locks:
                self._locks.pop(lock, None)

            return

        try:
            task = next(
                (t for t in self._tasks if t.id == _task_id)
            )
            self._tasks.remove(task)
        except StopIteration:
            raise LookupError
