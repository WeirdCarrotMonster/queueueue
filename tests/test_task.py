import pytest
import sys, os

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')


from overseer.queue import Task


def test_task_update():
    t = Task("test_task", [1, 2, 3], "pool", [1], {})

    update_dict = {
        "stdout": 1,
        "stderr": 2,
        "result": 3,
        "traceback": 4,
        "status": 5
    }

    t.update(**update_dict)

    for key, value in update_dict.items():
        assert getattr(t, key) == value


def test_task_update_only_required():
    t = Task("test_task", [1, 2, 3], "pool", [1], {})

    update_dict = {
        "wrong": 1,
        "error": 2,
    }

    t.update(**update_dict)

    for key, value in update_dict.items():
        with pytest.raises(AttributeError):
            assert getattr(t, key) == value


def test_task_worker_info():
    t = Task("test_task", [1, 2, 3], "pool", [1], {})

    w_i = t.worker_info

    assert len(w_i.items()) == 4
    assert "id" in w_i
    assert "name" in w_i
    assert "args" in w_i
    assert "kwargs" in w_i
