import pytest
from queueueue.taskqueue import Task


def test_task_update():
    t = Task("test_task", [1, 2, 3], "pool", [1], {})

    update_dict = {
        "stdout": 1,
        "stderr": 2,
        "result": 3,
        "traceback": 4,
        "status": 5
    }

    t.complete(**update_dict)

    for key, value in update_dict.items():
        assert getattr(t, key) == value


def test_task_update_only_required():
    t = Task("test_task", [1, 2, 3], "pool", [1], {})

    update_dict = {
        "wrong": 1,
        "error": 2,
    }

    t.complete(**update_dict)

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


def test_task_full_info():
    t = Task("test_task", [1, 2, 3], "pool", [1], {})

    f_i = t.full_info

    assert len(f_i.items()) == 11
    assert "id" in f_i
    assert "name" in f_i
    assert "args" in f_i
    assert "kwargs" in f_i
    assert "locks" in f_i
    assert "pool" in f_i
    assert "stdout" in f_i
    assert "stderr" in f_i
    assert "result" in f_i
    assert "status" in f_i
    assert "traceback" in f_i


def test_task_for_json():
    t = Task("test_task", [1, 2, 3], "pool", [1], {})

    assert type(t.for_json()) == dict
