import logging
import uuid

import asyncio


class Task(object):

    def __init__(self, name, locks, pool, args, kwargs, id=None, status="pending"):
        self.id = uuid.UUID(id) if id else uuid.uuid4()
        self.name = name
        self.locks = set(locks)
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
        return "<{0} [{1}][{2}]>".format(self.name, self.id, ",".join(str(_) for _ in self.locks))

    def __eq__(self, other):
        """Compares two task objects by their initial params."""
        return (
            self.name == other.name and
            self.locks == other.locks and
            self.args == other.args and
            self.kwargs == other.kwargs
        )

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
            "locks": list(self.locks),
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
            "locks": list(self.locks),
            "pool": self.pool,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "result": self.result,
            "status": self.status,
            "traceback": self.traceback
        }


class MultiLockPriorityPoolQueue(object):

    def __init__(self):
        self._locks = set()
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
    def locks(self):
        return self._locks

    @property
    def tasks_pending(self):
        return [task.id for task in self._tasks]

    @property
    def tasks_active(self):
        return list(self._active_tasks.keys())

    def put(self, task, unique=False):
        if unique and task in self._tasks:
            self._logger.info("Task {0} not unique".format(repr(task)))
            return

        self._logger.info("Queued task {0}".format(repr(task)))
        self._tasks.append(task)
        self._logger.debug("Queue length: {0}".format(len(self._tasks)))

    def get(self, pool):
        for task in self._tasks:
            if task.pool != pool:
                continue
            if self._locks & task.locks:
                continue

            self._tasks.remove(task)
            self._active_tasks[task.id] = task

            self._locks |= task.locks

            self._logger.info("Sending task {0}".format(repr(task)))
            self._logger.debug("Active locks: {0}".format(self.locks))
            return task
        return None

    def complete(self, task_id, data):
        task_id = uuid.UUID(task_id)
        task = self._active_tasks.pop(task_id, None)

        if not task:
            raise LookupError

        self._logger.info("Completed task {0}".format(repr(task)))
        task.complete(**data)

        self._locks -= task.locks

        self._logger.debug("Queue length: {0}".format(len(self._tasks)))
        self._logger.debug("Active locks: {0}".format(self.locks))

        return task

    def safe_remove(self, task_id):
        task_id = uuid.UUID(task_id)

        if task_id in self._active_tasks:
            task = self._active_tasks.pop(task_id, None)

            self._locks -= task.locks

            return

        for task in self._tasks:
            if task.id == task_id:
                self._tasks.remove(task)
                return

        raise LookupError
