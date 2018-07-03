import asyncio

import pytest

from queueueue.app import (build_app, get_encoded_auth, setup_basic_auth,
                           setup_bearer_auth)
from queueueue.routes import setup_routes
from queueueue.taskqueue import Task


@pytest.fixture
def cli(loop, aiohttp_client):
    app = build_app()
    setup_routes(app)
    return loop.run_until_complete(aiohttp_client(app))


async def test_handle_request(cli):
    response = await cli.get("/task")
    data = await response.json()

    assert len(data) == 0


async def test_queue_add(cli):
    task = Task("test_task", [1, 2, 3], "pool", [1], {})
    response = await cli.post("/task", json=task.for_json())
    assert response.status == 200
    data = await response.json()
    assert data["result"] == "success"

    response = await cli.get("/task")
    assert response.status == 200
    data = await response.json()
    assert len(data) == 1
    assert "created" in data[0]
    assert "taken" in data[0]
    assert data[0]["taken"] is None


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
    t1 = Task("test_task", ["1", "2", "3"], "pool", [1], {})
    await cli.post("/task", json=t1.for_json())

    response = await cli.get("/task")
    assert response.status == 200
    data = await response.json()
    assert t1.id in cli.server.app["queue"].tasks_pending
    assert len(data) == 1

    response = await cli.patch(
        "/task/pending",
        params={"pool": "pool"}
    )
    assert response.status == 200
    data = await response.json()
    assert data["id"] == str(t1.id)

    response = await cli.get("/task")
    data = await response.json()
    assert t1.id not in cli.server.app["queue"].tasks_pending
    assert t1.id in cli.server.app["queue"].tasks_active
    assert len(data) == 0

    response = await cli.get("/task/taken")
    assert response.status == 200
    data = await response.json()
    assert len(data) == 1
    taken_date = data[0]["taken"]

    response = await cli.patch(
        "/task/{}".format(str(t1.id)),
        json={"stdout": "", "stderr": "", "result": "", "status": "success"}
    )
    assert response.status == 200

    response = await cli.get("/task")
    data = await response.json()
    assert len(data) == 0


async def test_queue_add_wait_complete(cli):
    task = Task("test_task", ["1"], "pool", [1], {})

    long_response = cli.post("/task", json=task.for_json(), params={"wait": "true"})
    long_response = asyncio.ensure_future(long_response)

    async def task_getter():
        for _ in range(3):
            response = await cli.patch(
                "/task/pending",
                params={"pool": "pool"}
            )
            data = await response.json()

            if data:
                return data
        else:
            raise Exception("Failed to long-poll task")

    get_task = asyncio.ensure_future(task_getter())

    done, pending = await asyncio.wait(
        [
            long_response,
            get_task
        ],
        return_when=asyncio.FIRST_COMPLETED
    )

    assert get_task in done

    response = await cli.patch(
        "/task/{}".format(str(task.id)),
        json={"stdout": "", "stderr": "", "result": "test_result", "status": "success"}
    )
    assert response.status == 200

    response = await long_response
    assert response.status == 200

    data = await response.json()
    assert isinstance(data, dict)
    assert data["result"] == "test_result"


async def test_basic_auth_forbidden(cli):
    app = cli.server.app

    setup_basic_auth(app, ["username:password"])

    response = await cli.get("/task")
    assert response.status == 403


async def test_basic_auth_passing(cli):
    app = cli.server.app

    setup_basic_auth(app, ["username:password"])

    auth = "Basic " + get_encoded_auth("username", "password")
    response = await cli.get("/task", headers={"Authorization": auth})
    assert response.status == 200


async def test_bearer_auth_forbidden(cli):
    app = cli.server.app

    setup_bearer_auth(app, ["really_long_token"])

    response = await cli.get("/task")
    assert response.status == 403


async def test_bearer_auth_passing(cli):
    app = cli.server.app

    setup_bearer_auth(app, ["really_long_token"])

    auth = "Bearer really_long_token"
    response = await cli.get("/task", headers={"Authorization": auth})
    assert response.status == 200


async def test_mixed_auth_passing(cli):
    app = cli.server.app

    setup_bearer_auth(app, ["really_long_token"])
    setup_basic_auth(app, ["username:password"])

    auth = "Basic " + get_encoded_auth("username", "password")
    response = await cli.get("/task", headers={"Authorization": auth})
    assert response.status == 200

    auth = "Bearer really_long_token"
    response = await cli.get("/task", headers={"Authorization": auth})
    assert response.status == 200
