import json
import socket
import unittest.mock

import asyncio
import pytest
import aiohttp
from queueueue.queueueue import Manager, safe_int_conversion
from queueueue.taskqueue import Task


def test_manager_create():
    Manager()


def test_manager_create_auth():
    Manager(auth=("username", "password"))


def test_manager_add_handler():
    m = Manager()

    @m.result_handler
    @asyncio.coroutine
    def sample(result):
        pass

    with pytest.raises(AssertionError):
        @m.result_handler
        def not_a_handler(result):
            pass


def test_manager_create_invalid_auth():
    with pytest.raises(ValueError):
        Manager(auth=("username",))
    with pytest.raises(TypeError):
        Manager(auth=["username"])


def test_save_int_conversion():
    assert safe_int_conversion(None, 10) == 10
    assert safe_int_conversion("10", 0) == 10
    assert safe_int_conversion(10, 10) == 10
    assert safe_int_conversion(10, 10, max_val=5) == 5
    assert safe_int_conversion(10, 10, min_val=15) == 15


def coroutine(f):
    def wrapper(self, *args, **kwargs):
        return self.loop.run_until_complete(asyncio.coroutine(f)(self, *args, **kwargs))

    return wrapper


@pytest.fixture
def cli(loop, aiohttp_client):
    manager = Manager(loop=loop)
    return loop.run_until_complete(aiohttp_client(manager._app))


async def test_handle_request(cli):
    response = await cli.options("/")
    data = await response.json()

    assert data["tasks"]["pending"] == 0
    assert data["tasks"]["active"] == 0
    assert data["locks"] == 0

    response = await cli.get("/")
    data = await response.json()

    assert len(data["tasks"]["pending"]) == 0
    assert len(data["tasks"]["active"]) == 0
    assert len(data["locks"]) == 0


async def test_queue_add(cli):
    task = Task("test_task", [1, 2, 3], "pool", [1], {})
    response = await cli.post("/task", json=task.for_json())
    assert response.status == 200
    data = await response.json()
    assert data["result"] == "success"

    response = await cli.options("/")
    assert response.status == 200
    data = await response.json()
    assert data["tasks"]["pending"] == 1

    response = await cli.get("/")
    assert response.status == 200
    data = await response.json()
    assert len(data["tasks"]["pending"]) == 1


async def test_queue_add_unique(cli):
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
    t2 = Task("test_task", [1, 2, 3], "pool", [1], {})

    response = await cli.post("/task", json=t1.for_json())
    assert response.status == 200
    data = await response.json()
    assert data["result"] == "success"

    response = await cli.post(
        "/task", json=t2.for_json(), params={"unique": "true"})
    assert response.status == 200
    data = await response.json()
    assert data["result"] == "success"

    response = await cli.get("/task")
    assert response.status == 200
    data = await response.json()
    assert len(data) == 1


async def test_queue_add_equal_not_unique(cli):
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
    t2 = Task("test_task", [1, 2, 3], "pool", [1], {})

    response = await cli.post("/task", json=t1.for_json())
    assert response.status == 200
    data = await response.json()
    assert data["result"] == "success"

    response = await cli.post("/task", json=t2.for_json())
    assert response.status == 200
    data = await response.json()
    assert data["result"] == "success"

    response = await cli.get("/task")
    assert response.status == 200
    data = await response.json()
    assert len(data) == 2


async def test_queue_add_unique_not_equal(cli):
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
    t2 = Task("test_task", [1, 2, 3, 4], "pool", [1], {})

    response = await cli.post("/task", json=t1.for_json())
    assert response.status == 200
    data = await response.json()
    assert data["result"] == "success"

    response = await cli.post(
        "/task", json=t2.for_json(), params={"unique": "true"})
    assert response.status == 200
    data = await response.json()
    assert data["result"] == "success"

    response = await cli.get("/task")
    assert response.status == 200
    data = await response.json()
    assert len(data) == 2


async def test_queue_pool_required(cli):
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
    await cli.post("/task", json=t1.for_json())

    response = await cli.patch("/task/pending")
    assert response.status == 200
    data = await response.json()
    assert data is None


async def test_queue_view_tasks(cli):
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
    t2 = Task("test_task", [1, 2, 3], "pool", [1], {})

    await cli.post("/task", json=t1.for_json())
    await cli.post("/task", json=t2.for_json())

    response = await cli.get("/task")
    assert response.status == 200
    data = await response.json()
    assert len(data) == 2

    response = await cli.get("/task", params={"limit": 1})
    assert response.status == 200
    data = await response.json()
    assert len(data) == 1
    assert data[0]["id"] == str(t1.id)

    response = await cli.get("/task", params={"offset": 1})
    assert response.status == 200
    data = await response.json()
    assert len(data) == 1
    assert data[0]["id"] == str(t2.id)


async def test_complete_unknown_task(cli):
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})

    response = await cli.patch(
        "/task/{}".format(str(t1.id)),
        json={"stdout": "", "stderr": "", "result": "", "status": "success"}
    )
    assert response.status == 404
    data = await response.json()
    assert data["error"] == "Unknown task"


async def test_delete_unknown_task(cli):
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})

    response = await cli.delete("/task/{}".format(str(t1.id)))
    assert response.status == 404
    data = await(response.json())
    assert data["error"] == "Unknown task"


async def test_delete_task(cli):
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})

    await cli.post("/task", json=t1.for_json())

    response = await cli.get("/task")
    assert response.status == 200
    data = await response.json()
    assert len(data) == 1

    response = await cli.delete("/task/{}".format(str(t1.id)))
    assert response.status == 200

    response = await cli.get("/task")
    assert response.status == 200
    data = await response.json()
    assert len(data) == 0


async def test_queue_task_work_process(cli):
    t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
    await cli.post("/task", json=t1.for_json())

    response = await cli.get("/")
    assert response.status == 200
    data = await response.json()
    assert len(data["tasks"]["pending"]) == 1
    assert len(data["tasks"]["active"]) == 0

    response = await cli.patch(
        "/task/pending",
        params={"pool": "pool"}
    )
    assert response.status == 200
    data = await response.json()
    assert data["id"] == str(t1.id)

    response = await cli.get("/")
    data = await response.json()
    assert len(data["tasks"]["pending"]) == 0
    assert len(data["tasks"]["active"]) == 1
    assert len(data["locks"]) == 3

    response = await cli.patch(
        "/task/{}".format(str(t1.id)),
        json={"stdout": "", "stderr": "", "result": "", "status": "success"}
    )
    assert response.status == 200

    response = await cli.get("/")
    data = await response.json()
    assert len(data["tasks"]["pending"]) == 0
    assert len(data["tasks"]["active"]) == 0
    assert len(data["locks"]) == 0
