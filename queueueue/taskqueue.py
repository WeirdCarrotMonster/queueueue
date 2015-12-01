import logging
import uuid
from collections import defaultdict
from threading import Lock

import asyncio


class Task(object):

    def __init__(self, name, locks, pool, args, kwargs, id=None, status="pending"):
        self.id = uuid.UUID(id) if id else uuid.uuid4()
        self.name = name
        self.locks = locks
        self.pool = pool
        self.args = args
        self.kwargs = kwargs
        self.status = status

        self.stdout = None
        self.stderr = None
        self.result = None
        self.traceback = None

        self.completed = asyncio.Event()

    def __repr__(self):
        return "<{} [{}][{}]>".format(self.name, self.id, ",".join(str(_) for _ in self.locks))

    def complete(self, **data):
        for attr in ["stdout", "stderr", "result", "status", "traceback"]:
            if attr in data:
                setattr(self, attr, data[attr])

        self.completed.data = {
            "status": data.get("status"),
            "result": data.get("result")
        }
        self.completed.set()

    def for_json(self):
        return {
            "id": str(self.id),
            "name": self.name,
            "locks": self.locks,
            "pool": self.pool,
            "args": self.args,
            "kwargs": self.kwargs
        }

    @property
    def worker_info(self):
        return {
            "id": str(self.id),
            "name": self.name,
            "args": self.args,
            "kwargs": self.kwargs
        }

    @property
    def full_info(self):
        return {
            "id": str(self.id),
            "name": self.name,
            "args": self.args,
            "kwargs": self.kwargs,
            "locks": self.locks,
            "pool": self.pool,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "result": self.result,
            "status": self.status,
            "traceback": self.traceback
        }


class MultiLockPriorityPoolQueue(object):

    def __init__(self):
        self._locks = defaultdict(Lock)
        self._tasks = []
        self._active_tasks = {}
        self._logger = logging.getLogger("Queue")

    @property
    def task_count(self):
        return len(self._tasks)

    @property
    def tasks(self):
        return self._tasks

    @property
    def locks_free(self):
        return list(
            data[0] for data in filter(
                lambda data: not data[1].locked(),
                self._locks.items()
            )
        )

    @property
    def locks_taken(self):
        return list(
            data[0] for data in filter(
                lambda data: data[1].locked(),
                self._locks.items()
            )
        )

    @property
    def tasks_pending(self):
        return [task.id for task in self._tasks]

    @property
    def tasks_active(self):
        return list(self._active_tasks.keys())

    def put(self, task):
        self._logger.info("Queued task {}".format(repr(task)))
        self._tasks.append(task)
        self._logger.debug("Queue length: {}".format(len(self._tasks)))

    def get(self, pool):
        for task in self._tasks:
            if task.pool != pool:
                continue
            if not all(not self._locks[_].locked() for _ in task.locks):
                continue

            self._tasks.remove(task)
            self._active_tasks[task.id] = task

            for lock in task.locks:
                self._locks[lock].acquire()

            self._logger.info("Sending task {}".format(repr(task)))
            self._logger.debug("Active locks: {}".format([key for key, value in self._locks.items() if value.locked()]))
            return task
        return None

    def complete(self, task_id, data):
        task_id = uuid.UUID(task_id)
        task = self._active_tasks.pop(task_id, None)

        if not task:
            raise LookupError

        self._logger.info("Completed task {}".format(repr(task)))
        task.complete(**data)

        for lock in task.locks:
            self._locks[lock].release()

        self._logger.debug("Queue length: {}".format(len(self._tasks)))
        self._logger.debug("Active locks: {}".format([key for key, value in self._locks.items() if value.locked()]))

        return task

    def safe_remove(self, task_id):
        task_id = uuid.UUID(task_id)

        if task_id in self._active_tasks:
            task = self._active_tasks.pop(task_id, None)

            for lock in task.locks:
                self._locks[lock].release()

            return

        for task in self._tasks:
            if task.id == task_id:
                self._tasks.remove(task)
                return

        raise LookupError
