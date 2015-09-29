#!/usr/bin/env python3.4

import logging
from email.base64mime import b64encode

import simplejson as json

import asyncio
from aiohttp import web

from .taskqueue import MultiLockPriorityPoolQueue, Task


def safe_int_conversion(value, default, min_val=None, max_val=None):
    try:
        result = int(value)

        if max_val:
            result = min(result, max_val)
        elif min_val:
            result = max(result, min_val)
    except (TypeError, ValueError):
        result = default

    return result


class Manager(object):
    def __init__(self, loop=None, host="127.0.0.1", port=8080, auth=None):
        self._queue = MultiLockPriorityPoolQueue()
        self._loop = loop
        self._result_handlers = []
        self._host = host
        self._port = port
        self._logger = logging.getLogger("queueueue")

        if auth:
            assert type(auth) == tuple, "Auth credentials must be a tuple, {} provided".format(type(auth))
            assert len(auth) == 2, "Auth credentials must be a tuple of two strings, {} provided".format(len(auth))
            self._auth = "Basic {}".format(
                b64encode(bytes("{}:{}".format(auth[0], auth[1]), "utf-8")).decode()
            )
        else:
            self._auth = None

        self._app = web.Application(loop=self._loop)
        self._setup_routes()

        self._srv = None

    @asyncio.coroutine
    def create_server(self):
        self._srv = yield from self._loop.create_server(
            self._app.make_handler(),
            self._host, self._port
        )
        return self._srv

    def _setup_routes(self):
        self._add_route('OPTIONS', '/', self.short_info)

        self._add_route('GET', '/task', self.list_tasks)
        self._add_route('POST', '/task', self.add_task)

        self._add_route('PATCH', '/task/pending', self.get_task)

        self._add_route('DELETE', '/task/{task_id}', self.delete_task)
        self._add_route('PATCH', '/task/{task_id}', self.complete_task)

    def _add_route(self, method, route, func):
        self._app.router.add_route(method, route, self._authenticate(func))

    def _authenticate(self, f):
        if not self._auth:
            return f

        @asyncio.coroutine
        def wrapper(request, *args, **kwargs):
            if not request.headers.get("AUTHORIZATION") == self._auth:
                return web.Response(text=json.dumps({"error": "Not authorized"}), status=403)
            return f(request, *args, **kwargs)

        return wrapper

    def result_handler(self, f):
        assert asyncio.iscoroutinefunction(f), "Result handler must be coroutine"
        self._result_handlers.append(f)

    @asyncio.coroutine
    def handle_result(self, task):
        for handler in self._result_handlers:
            try:
                yield from handler(task)
            except:
                pass

    @asyncio.coroutine
    def short_info(self, request):
        return web.Response(text=json.dumps({
            "tasks": self._queue.task_count,
            "locks": {
                "taken": self._queue.locks_taken,
                "free": self._queue.locks_free
            }
        }))

    @asyncio.coroutine
    def list_tasks(self, request):
        offset = safe_int_conversion(
            request.GET.get("offset"), 0,
            min_val=0, max_val=self._queue.task_count
        )
        limit = safe_int_conversion(
            request.GET.get("limit"), 50,
            min_val=1, max_val=50
        )

        return web.Response(text=json.dumps([
            task.for_json() for task in self._queue.tasks[offset:offset + limit]
        ]))

    @asyncio.coroutine
    def add_task(self, request):
        data = yield from request.json(loader=json.loads)
        self._queue.put(Task(**data))
        return web.Response(text=json.dumps({"result": "success"}))

    @asyncio.coroutine
    def get_task(self, request):
        pool = request.GET.get("pool")
        if not pool:
            return web.Response(text=json.dumps(None))

        task = self._queue.get(pool=pool)
        data = task.worker_info if task else None
        return web.Response(text=json.dumps(data))

    @asyncio.coroutine
    def complete_task(self, request):
        _id = request.match_info.get('task_id')
        data = yield from request.json(loader=json.loads)

        try:
            task = self._queue.complete(_id, data)
            yield from self.handle_result(task)
            return web.Response(text=json.dumps({"result": "Success"}))
        except LookupError:
            return web.Response(text=json.dumps({"error": "Unknown task"}), status=404)

    @asyncio.coroutine
    def delete_task(self, request):
        _id = request.match_info.get('task_id')
        try:
            self._queue.safe_remove(_id)
            return web.Response(text=json.dumps({"result": "Success"}))
        except LookupError:
            return web.Response(text=json.dumps({"error": "Unknown task"}), status=404)
