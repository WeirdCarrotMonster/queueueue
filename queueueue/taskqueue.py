import logging
import uuid
from collections import defaultdict
from threading import Lock


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

    def update(self, **data):
        for attr in ["stdout", "stderr", "result", "status", "traceback"]:
            if attr in data:
                setattr(self, attr, data[attr])

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


class MultiLockPriorityPoolQueue(object):

    def __init__(self):
        self._locks = defaultdict(Lock)
        self._tasks = []
        self._active_tasks = {}

    @property
    def task_count(self):
        return len(self._tasks)

    @property
    def tasks(self):
        return self._tasks

    @property
    def locks_free(self):
        return len(list(filter(lambda l: not l.locked(), self._locks.values())))

    @property
    def locks_taken(self):
        return len(list(filter(lambda l: l.locked(), self._locks.values())))

    def put(self, task):
        logging.info("Queued task {}[{}]".format(task.name, task.id))
        logging.debug("Queue length: {}".format(len(self._tasks)))
        self._tasks.append(task)

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

            logging.info("Sending task {}[{}]".format(task.name, task.id))
            logging.debug("Active locks: {}".format([key for key, value in self._locks.items() if value.locked()]))
            return task
        return None

    def complete(self, task_id, data):
        task_id = uuid.UUID(task_id)
        task = self._active_tasks.pop(task_id, None)

        if not task:
            raise LookupError

        logging.info("Removing task {}[{}]".format(task.name, task.id))
        task.update(**data)

        for lock in task.locks:
            self._locks[lock].release()

        logging.debug("Queue length: {}".format(len(self._tasks)))
        logging.debug("Active locks: {}".format([key for key, value in self._locks.items() if value.locked()]))

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
