import pytest
import uuid
import sys, os

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')


from overseer.queue import MultiLockPriorityPoolQueue, Task


def test_queue_add():
    q = MultiLockPriorityPoolQueue()
    t = Task("test_task", [], "pool", [], {})

    q.put(t)

    assert len(q._tasks) == 1
    assert len(q._locks.items()) == 0


def test_queue_use_locks():
    q = MultiLockPriorityPoolQueue()
    t = Task("test_task", [1, 2, 3], "pool", [], {})

    q.put(t)
    q.get("pool")

    assert len(q._locks.items()) == 3
    assert 1 in q._locks
    assert 2 in q._locks
    assert 3 in q._locks


def test_queue_blocking_tasks():
    q = MultiLockPriorityPoolQueue()
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
    t2 = Task("test_task", [1, 2, 3], "pool", [2], {})

    q.put(t1)
    q.put(t2)
    task1 = q.get("pool")
    assert task1.args[0] == 1
    assert len(q._locks.items()) == 3

    task2 = q.get("pool")
    assert task2 is None


def test_queue_not_blocking_tasks():
    q = MultiLockPriorityPoolQueue()
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
    t2 = Task("test_task", [4, 5, 6], "pool", [2], {})

    q.put(t1)
    q.put(t2)

    task1 = q.get("pool")
    assert task1.args[0] == 1
    assert len(q._locks.items()) == 3

    task2 = q.get("pool")
    assert task2.args[0] == 2
    assert len(q._locks.items()) == 6


def test_queue_pools():
    q = MultiLockPriorityPoolQueue()
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
    t2 = Task("test_task", [1, 2, 3], "pool", [2], {})

    q.put(t1)
    q.put(t2)
    task1 = q.get("pool_2")
    task2 = q.get("pool")

    assert task1 is None
    assert task2.args[0] == 1


def test_queue_blocking_pools():
    q = MultiLockPriorityPoolQueue()
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
    t2 = Task("test_task", [1, 2, 3], "pool_2", [2], {})

    q.put(t1)
    q.put(t2)
    task1 = q.get("pool")
    task2 = q.get("pool_2")

    assert task1.args[0] == 1
    assert task2 is None


def test_queue_complete_missing_task():
    q = MultiLockPriorityPoolQueue()

    with pytest.raises(LookupError):
        q.complete(str(uuid.uuid4()), {})


def test_queue_complete_task():
    q = MultiLockPriorityPoolQueue()

    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
    q.put(t1)

    task = q.get("pool")
    assert len(q._locks.items()) == 3
    q.complete(str(task.id), {"stdout": "", "stderr": "", "result": "", "status": "success"})

    assert all(not lock.locked() for lock in q._locks.values())
