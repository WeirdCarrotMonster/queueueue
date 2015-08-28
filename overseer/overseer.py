#!/usr/bin/env python3.4

import logging
import uuid
from collections import defaultdict
from email.base64mime import b64encode
from threading import Lock

import simplejson as json

import asyncio
from aiohttp import web


class Task(object):

    def __init__(self, name, locks, pool, args, kwargs, id=None, status="pending"):
        self.id = uuid.UUID(id) if id else uuid.uuid4()
        self.name = name
        self.locks = locks
        self.pool = pool
        self.args = args
        self.kwargs = kwargs
        self.status = status

        self.stdout = None
        self.stderr = None
        self.result = None
        self.traceback = None

    def update(self, **data):
        for attr in ["stdout", "stderr", "result", "status", "traceback"]:
            if attr in data:
                setattr(self, attr, data[attr])

    @property
    def worker_info(self):
        return {
            "id": str(self.id),
            "name": self.name,
            "args": self.args,
            "kwargs": self.kwargs
        }


class MultiLockPriorityPoolQueue(object):

    def __init__(self):
        self._locks = defaultdict(Lock)
        self._tasks = []
        self._active_tasks = {}

    def put(self, task):
        logging.info("Queued task {}[{}]".format(task.name, task.id))
        logging.debug("Queue length: {}".format(len(self._tasks)))
        self._tasks.append(task)

    def get(self, pool):
        for task in self._tasks:
            if task.pool != pool:
                continue
            if not all(not self._locks[_].locked() for _ in task.locks):
                continue

            self._tasks.remove(task)
            self._active_tasks[task.id] = task

            for lock in task.locks:
                self._locks[lock].acquire()

            logging.info("Sending task {}[{}]".format(task.name, task.id))
            logging.debug("Active locks: {}".format([key for key, value in self._locks.items() if value.locked()]))
            return task
        return None

    def complete(self, task_id, data):
        task_id = uuid.UUID(task_id)
        task = self._active_tasks.pop(task_id)

        if not task:
            raise LookupError

        logging.info("Removing task {}[{}]".format(task.name, task.id))
        logging.debug("Queue length: {}".format(len(self._tasks)))
        logging.debug("Active locks: {}".format([key for key, value in self._locks.items() if value.locked()]))
        task.update(**data)

        for lock in task.locks:
            self._locks[lock].release()

        return task


class Manager(object):
    def __init__(self, host="127.0.0.1", port=8080, auth=None):
        self._queue = MultiLockPriorityPoolQueue()
        self._loop = asyncio.get_event_loop()
        self._result_handlers = []
        self._host = host
        self._port = port

        if auth:
            assert type(auth) == tuple, "Auth credentials must be a tuple"
            assert len(auth) == 2, "Auth credentials must be a tuple of two strings"
            self._auth = "Basic {}".format(
                b64encode(bytes("{}:{}".format(auth[0], auth[1]), "utf-8")).decode()
            )
        else:
            self._auth = None

        self._app = web.Application(loop=self._loop)
        self._app.router.add_route(
            'PUT', '/task',
            self._authenticate(self.add_task))
        self._app.router.add_route(
            'PATCH', '/task/pending',
            self._authenticate(self.get_task))
        self._app.router.add_route(
            'PATCH', '/task/{task_id}',
            self._authenticate(self.complete_task))

        self._srv = self._loop.create_server(
            self._app.make_handler(),
            self._host, self._port
        )

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
        assert asyncio.iscoroutine(f), "Result handler must be coroutine"
        self._result_handlers.append(f)

    @asyncio.coroutine
    def handle_result(self, task):
        for handler in self._result_handlers:
            try:
                yield from handler(task)
            except:
                pass

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

    def run(self):
        print("Server started on http://{}:{}".format(self._host, self._port))
        self._loop.run_until_complete(self._srv)
        try:
            self._loop.run_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(prog='overseer')
    parser.add_argument("--host", help="overseer listen address")
    parser.add_argument("--port", help="overseer listen port")
    parser.add_argument("--auth", help="authentication credentials")
    parser.add_argument("--loglevel", help="logging verbosity level")

    args = parser.parse_args()

    config = {}
    if args.host:
        config["host"] = args.host
    if args.port:
        config["port"] = args.port
    if args.auth:
        config["auth"] = tuple(_ for _ in args.auth.split(":"))
    if args.loglevel:
        logging.basicConfig(level=args.loglevel)

    m = Manager(**config)
    m.run()
