#!/usr/bin/env python3.4

import logging
from email.base64mime import b64encode

import simplejson as json

import asyncio
from aiohttp import web

from .queue import MultiLockPriorityPoolQueue, Task


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


def main():
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


if __name__ == "__main__":
    main()
