import unittest
import uuid

import asyncio
import pytest
from queueueue.taskqueue import MultiLockPriorityPoolQueue, Task


class TestTaskQueue(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        pass
        # self.loop.close()

    def test_queue_add(self):
        q = MultiLockPriorityPoolQueue()
        t = Task("test_task", [], "pool", [], {})

        q.put(t)

        assert len(q._tasks) == 1
        assert q.task_count == 1
        assert len(q._locks) == 0

    def test_queue_add_unique(self):
        q = MultiLockPriorityPoolQueue()
        t = Task("test_task", [], "pool", [], {})
        t2 = Task("test_task", [], "pool", [], {})

        q.put(t)
        q.put(t2, unique=True)

        assert len(q._tasks) == 1
        assert q._tasks[0].id == t.id
        assert q.task_count == 1
        assert len(q._locks) == 0

    def test_queue_add_unique_not_eq(self):
        q = MultiLockPriorityPoolQueue()
        t = Task("test_task", [], "pool", [], {"test": 2})
        t2 = Task("test_task", [], "pool", [], {"test": 1})

        q.put(t)
        q.put(t2, unique=True)

        assert len(q._tasks) == 2
        assert q._tasks[0].id == t.id
        assert q._tasks[1].id == t2.id
        assert q.task_count == 2
        assert len(q._locks) == 0

    def test_queue_add_unique_ignore_kwargs(self):
        q = MultiLockPriorityPoolQueue()
        t = Task("test_task", [], "pool", [], {})
        t2 = Task("test_task", [], "pool", [], {"test": 1})

        q.put(t)
        q.put(t2, unique=True, unique_ignore_kwargs={"test"})

        assert len(q._tasks) == 1
        assert q._tasks[0].id == t.id
        assert q.task_count == 1
        assert len(q._locks) == 0

    def test_queue_add_unique_ignore_kwargs_not_eq(self):
        q = MultiLockPriorityPoolQueue()
        t = Task("test_task", [], "pool", [], {"test": 2, "asd": 1})
        t2 = Task("test_task", [], "pool", [], {"test": 1})

        q.put(t)
        q.put(t2, unique=True, unique_ignore_kwargs={"test"})

        assert len(q._tasks) == 2
        assert q._tasks[0].id == t.id
        assert q._tasks[1].id == t2.id
        assert q.task_count == 2
        assert len(q._locks) == 0

    def test_queue_task_list_access(self):
        q = MultiLockPriorityPoolQueue()
        t = Task("test_task", [], "pool", [], {})

        q.put(t)

        assert q.tasks is not q._tasks

    def test_queue_use_locks(self):
        q = MultiLockPriorityPoolQueue()
        t = Task("test_task", [1, 2, 3], "pool", [], {})

        q.put(t)
        q.get("pool")

        assert len(q._locks) == 3
        assert 1 in q._locks
        assert 2 in q._locks
        assert 3 in q._locks

    def test_queue_blocking_tasks(self):
        q = MultiLockPriorityPoolQueue()
        t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
        t2 = Task("test_task", [1, 2, 3], "pool", [2], {})

        q.put(t1)
        q.put(t2)
        task1 = q.get("pool")
        assert task1.args[0] == 1
        assert len(q._locks) == 3

        task2 = q.get("pool")
        assert task2 is None

    def test_queue_not_blocking_tasks(self):
        q = MultiLockPriorityPoolQueue()
        t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
        t2 = Task("test_task", [4, 5, 6], "pool", [2], {})

        q.put(t1)
        q.put(t2)

        task1 = q.get("pool")
        assert task1.args[0] == 1
        assert len(q._locks) == 3

        task2 = q.get("pool")
        assert task2.args[0] == 2
        assert len(q._locks) == 6

    def test_queue_pools(self):
        q = MultiLockPriorityPoolQueue()
        t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
        t2 = Task("test_task", [1, 2, 3], "pool", [2], {})

        q.put(t1)
        q.put(t2)
        task1 = q.get("pool_2")
        task2 = q.get("pool")

        assert task1 is None
        assert task2.args[0] == 1

    def test_queue_blocking_pools(self):
        q = MultiLockPriorityPoolQueue()
        t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
        t2 = Task("test_task", [1, 2, 3], "pool_2", [2], {})

        q.put(t1)
        q.put(t2)
        task1 = q.get("pool")
        task2 = q.get("pool_2")

        assert task1.args[0] == 1
        assert task2 is None

    def test_queue_complete_missing_task(self):
        q = MultiLockPriorityPoolQueue()

        with pytest.raises(LookupError):
            q.complete(str(uuid.uuid4()), {})

    def test_queue_complete_task(self):
        q = MultiLockPriorityPoolQueue()

        t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
        q.put(t1)

        task = q.get("pool")
        assert len(q._locks) == 3
        q.complete(str(task.id), {"stdout": "", "stderr": "", "result": "", "status": "success"})

        assert len(q._locks) == 0

    def test_queue_safe_remove_active(self):
        q = MultiLockPriorityPoolQueue()
        t = Task("test_task", [1, 2, 3], "pool", [], {})

        q.put(t)
        q.get("pool")

        assert len(q._locks) == 3
        assert 1 in q._locks
        assert 2 in q._locks
        assert 3 in q._locks

        q.safe_remove(str(t.id))

        assert len(q._locks) == 0

    def test_queue_safe_remove_pending(self):
        q = MultiLockPriorityPoolQueue()
        t = Task("test_task", [1, 2, 3], "pool", [], {})

        q.put(t)

        assert len(q._locks) == 0

        q.safe_remove(str(t.id))

        assert len(q._locks) == 0

    def test_queue_safe_remove_not_existing(self):
        q = MultiLockPriorityPoolQueue()
        t = Task("test_task", [1, 2, 3], "pool", [], {})

        assert len(q._locks) == 0

        with pytest.raises(LookupError):
            q.safe_remove(str(t.id))
