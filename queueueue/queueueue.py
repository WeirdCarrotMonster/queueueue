#!/usr/bin/env python3.4

import json
import logging
from email.base64mime import b64encode

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


class JSONResponse(web.Response):

    def __init__(self, data, **kwargs):
        kwargs["text"] = json.dumps(data, indent="  ")
        kwargs["content_type"] = "application/json"
        super().__init__(**kwargs)


class Manager(object):
    def __init__(self, loop=None, host="127.0.0.1", port=8080, auth=None):
        self._queue = MultiLockPriorityPoolQueue()
        self._loop = loop
        self._result_handlers = []
        self._host = host
        self._port = port
        self.logger = logging.getLogger("queueueue")

        if auth:
            if not isinstance(auth, tuple):
                raise TypeError("Auth credentials must be a tuple, got {}", type(auth))
            if len(auth) != 2:
                raise ValueError("Auth credentials must be a tuple of two, got {}", len(auth))

            self._set_auth(auth)
        else:
            self._auth = None

        self._app = web.Application(loop=self._loop)
        self._setup_routes()

        self._srv = None

    async def create_server(self):
        self._srv = await self._loop.create_server(
            self._app.make_handler(),
            self._host, self._port
        )
        self.logger.info("Created server on %s:%s", self._host, self._port)
        return self._srv

    def _setup_routes(self):
        self._add_route('OPTIONS', '/', self.short_info)
        self._add_route('GET', '/', self.extended_info)

        self._add_route('GET', '/task', self.list_tasks)
        self._add_route('POST', '/task', self.add_task)

        self._add_route('PATCH', '/task/pending', self.get_task)

        self._add_route('DELETE', '/task/{task_id}', self.delete_task)
        self._add_route('PATCH', '/task/{task_id}', self.complete_task)

    def _add_route(self, method, route, func):
        self._app.router.add_route(method, route, self._authenticate(func))

    def _set_auth(self, credentials):
        self._auth = "Basic {}".format(
            b64encode(bytes("{}:{}".format(credentials[0], credentials[1]), "utf-8")).decode()
        )

    def _authenticate(self, f):
        if not self._auth:
            return f

        async def wrapper(request, *args, **kwargs):
            if not request.headers.get("AUTHORIZATION") == self._auth:
                self.logger.warning(
                    "Request with invalid auth credentials blocked: %s %s",
                    request.method, request.path_qs)
                return JSONResponse({
                    "error": "Not authorized"
                }, status=403)
            return f(request, *args, **kwargs)

        return wrapper

    def result_handler(self, f):
        assert asyncio.iscoroutinefunction(f), "Result handler must be coroutine"
        self._result_handlers.append(f)

    async def handle_result(self, task):
        for handler in self._result_handlers:
            try:
                await handler(task)
            except:
                pass

    async def short_info(self, request):
        return JSONResponse({
            "tasks": {
                "pending": len(self._queue.tasks_pending),
                "active": len(self._queue.tasks_active)
            },
            "locks": len(self._queue.locks)
        })

    async def extended_info(self, request):
        return JSONResponse({
            "tasks": {
                "pending": [str(task) for task in self._queue.tasks_pending],
                "active": [str(task) for task in self._queue.tasks_active]
            },
            "locks": list(self._queue.locks)
        })

    async def list_tasks(self, request):
        offset = safe_int_conversion(
            request.query.get("offset"), 0,
            min_val=0, max_val=self._queue.task_count
        )
        limit = safe_int_conversion(
            request.query.get("limit"), 50,
            min_val=1, max_val=50
        )

        return JSONResponse([
            task.for_json()
            for task in self._queue.tasks[offset:offset + limit]
        ])

    async def add_task(self, request):
        data = await request.json()
        task = Task(**data)

        unique = request.query.get("unique", "").lower() == "true"
        wait = request.query.get("wait", "").lower() == "true"

        self._queue.put(task, unique=unique)

        if wait:
            await task.completed.wait()
            result = task.completed.data
        else:
            result = {"result": "success"}

        return JSONResponse(result)

    async def get_task(self, request):
        pool = request.query.get("pool")
        if not pool:
            return JSONResponse(None)

        task = self._queue.get(pool=pool)
        data = task.worker_info if task else None
        return JSONResponse(data)

    async def complete_task(self, request):
        _id = request.match_info.get('task_id')
        data = await request.json()

        try:
            task = self._queue.complete(_id, data)
            await self.handle_result(task)
            return JSONResponse({"result": "Success"})
        except LookupError:
            return JSONResponse({"error": "Unknown task"}, status=404)

    async def delete_task(self, request):
        _id = request.match_info.get('task_id')
        try:
            self._queue.safe_remove(_id)
            return JSONResponse({"result": "Success"})
        except LookupError:
            return JSONResponse({"error": "Unknown task"}, status=404)
