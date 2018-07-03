import pytest

from queueueue.app import build_app
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

    response = await cli.get("/task")
    assert response.status == 200
    data = await response.json()
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
    assert len(data) == 0

    response = await cli.patch(
        "/task/{}".format(str(t1.id)),
        json={"stdout": "", "stderr": "", "result": "", "status": "success"}
    )
    assert response.status == 200

    response = await cli.get("/task")
    data = await response.json()
    assert len(data) == 0
