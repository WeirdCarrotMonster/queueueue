import socket
import unittest
import unittest.mock

import json
import aiohttp
import asyncio
import pytest
from aiohttp import client
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
    with pytest.raises(AssertionError):
        Manager(auth=("username",))
    with pytest.raises(AssertionError):
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


class TestManagerTaskProcessing(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        pass

    def find_unused_port(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
        s.close()
        return port

    @asyncio.coroutine
    def create_server(self, *args, **kwargs):
        port = self.find_unused_port()
        manager = Manager(loop=self.loop, port=port, *args, **kwargs)
        srv = yield from manager.create_server()
        self.addCleanup(srv.close)
        return "http://127.0.0.1:{}".format(port), manager

    @coroutine
    def test_handle_request(self):
        url, _ = yield from self.create_server()
        r = yield from client.options(
            "{}/".format(url), loop=self.loop)
        data = yield from r.json()
        assert data["tasks"] == 0
        assert data["locks"]["taken"] == 0
        assert data["locks"]["free"] == 0

    @coroutine
    def test_handle_auth(self):
        url, _ = yield from self.create_server(auth=("username", "password"))
        r = yield from client.options(
            "{}/".format(url), loop=self.loop, auth=("username", "password"))
        yield from r.release()
        assert r.status == 200

    @coroutine
    def test_invalid_auth(self):
        url, _ = yield from self.create_server(auth=("username", "password"))
        r = yield from client.options(
            "{}/".format(url), loop=self.loop)
        yield from r.release()
        assert r.status == 403

        r = yield from client.options(
            "{}/".format(url), loop=self.loop, auth=("username", "wrong_password"))
        yield from r.release()
        assert r.status == 403

    @coroutine
    def test_queue_add(self):
        url, _ = yield from self.create_server()
        t = Task("test_task", [1, 2, 3], "pool", [1], {})
        r = yield from client.post(
            "{}/task".format(url), data=json.dumps(t.for_json()),
            loop=self.loop)
        data = yield from r.json()
        assert r.status == 200
        assert data["result"] == "success"

        r = yield from client.options(
            "{}/".format(url), data=json.dumps(t.for_json()),
            loop=self.loop)
        data = yield from r.json()
        assert r.status == 200
        assert data["tasks"] == 1

    @coroutine
    def test_queue_pool_required(self):
        url, _ = yield from self.create_server()
        t = Task("test_task", [1, 2, 3], "pool", [1], {})
        r = yield from client.post(
            "{}/task".format(url), data=json.dumps(t.for_json()),
            loop=self.loop)
        data = yield from r.json()

        r = yield from client.patch(
            "{}/task/pending".format(url),
            loop=self.loop)
        assert r.status == 200
        data = yield from r.json()
        assert data is None

    @coroutine
    def test_queue_view_tasks(self):
        url, _ = yield from self.create_server()
        t1 = Task("test_task", [1, 2, 3], "pool", [1], {})
        t2 = Task("test_task", [1, 2, 3], "pool", [1], {})

        r = yield from client.post(
            "{}/task".format(url), data=json.dumps(t1.for_json()),
            loop=self.loop)
        data = yield from r.json()
        assert r.status == 200
        assert data["result"] == "success"

        r = yield from client.post(
            "{}/task".format(url), data=json.dumps(t2.for_json()),
            loop=self.loop)
        data = yield from r.json()
        assert r.status == 200
        assert data["result"] == "success"

        r = yield from client.get(
            "{}/task".format(url),
            loop=self.loop)
        assert r.status == 200
        data = yield from r.json()
        assert len(data) == 2

        r = yield from client.get(
            "{}/task".format(url),
            params={"limit": 1},
            loop=self.loop)
        assert r.status == 200
        data = yield from r.json()
        assert len(data) == 1
        assert data[0]["id"] == str(t1.id)

        r = yield from client.get(
            "{}/task".format(url),
            params={"offset": 1},
            loop=self.loop)
        assert r.status == 200
        data = yield from r.json()
        assert len(data) == 1
        assert data[0]["id"] == str(t2.id)

    @coroutine
    def test_complete_unknown_task(self):
        url, _ = yield from self.create_server()
        t = Task("test_task", [1, 2, 3], "pool", [1], {})

        r = yield from client.patch(
            "{}/task/{}".format(url, str(t.id)),
            data=json.dumps({"stdout": "", "stderr": "", "result": "", "status": "success"}),
            loop=self.loop)
        assert r.status == 404
        data = yield from r.json()
        assert data["error"] == "Unknown task"

    @coroutine
    def test_delete_unknown_task(self):
        url, _ = yield from self.create_server()
        t = Task("test_task", [1, 2, 3], "pool", [1], {})

        r = yield from client.delete(
            "{}/task/{}".format(url, str(t.id)),
            loop=self.loop)
        assert r.status == 404
        data = yield from r.json()
        assert data["error"] == "Unknown task"

    @coroutine
    def test_delete_task(self):
        url, _ = yield from self.create_server()
        t = Task("test_task", [1, 2, 3], "pool", [1], {})

        r = yield from client.post(
            "{}/task".format(url), data=json.dumps(t.for_json()),
            loop=self.loop)
        yield from r.release()

        r = yield from client.options(
            "{}/".format(url),
            loop=self.loop)
        data = yield from r.json()
        assert r.status == 200
        assert data["tasks"] == 1

        r = yield from client.delete(
            "{}/task/{}".format(url, str(t.id)),
            loop=self.loop)
        yield from r.release()
        assert r.status == 200

        r = yield from client.options(
            "{}/".format(url),
            loop=self.loop)
        data = yield from r.json()
        assert r.status == 200
        assert data["tasks"] == 0

    @coroutine
    def test_queue_task_work(self):
        url, manager = yield from self.create_server()

        result_storage = []

        @manager.result_handler
        @asyncio.coroutine
        def store_result(result):
            result_storage.append(result)

        @manager.result_handler
        @asyncio.coroutine
        def bad_function(result):
            raise Exception()

        t = Task("test_task", [1, 2, 3], "pool", [1], {})

        r = yield from client.post(
            "{}/task".format(url), data=json.dumps(t.for_json()),
            loop=self.loop)
        assert r.status == 200
        data = yield from r.json()
        assert data["result"] == "success"

        r = yield from client.options(
            "{}/".format(url),
            loop=self.loop)
        assert r.status == 200
        data = yield from r.json()
        assert data["tasks"] == 1

        r = yield from client.patch(
            "{}/task/pending".format(url), params={"pool": "pool"},
            loop=self.loop)
        assert r.status == 200
        data = yield from r.json()
        assert data["id"] == str(t.id)

        r = yield from client.options(
            "{}/".format(url),
            loop=self.loop)
        assert r.status == 200
        data = yield from r.json()
        assert data["tasks"] == 0
        assert data["locks"]["taken"] == 3
        assert data["locks"]["free"] == 0

        r = yield from client.patch(
            "{}/task/{}".format(url, str(t.id)),
            data=json.dumps({"stdout": "", "stderr": "", "result": "", "status": "success"}),
            loop=self.loop)
        assert r.status == 200
        yield from r.release()

        assert len(result_storage) == 1

        r = yield from client.options(
            "{}/".format(url),
            loop=self.loop)
        assert r.status == 200
        data = yield from r.json()
        assert data["tasks"] == 0
        assert data["locks"]["taken"] == 0
        assert data["locks"]["free"] == 3
