from base64 import b64encode
from typing import List

from aiohttp import web

from .taskqueue import MultiLockPriorityPoolQueue


def build_app() -> web.Application:
    app = web.Application()
    app["queue"] = MultiLockPriorityPoolQueue()
    app["auth"] = set()
    return app


def setup_basic_auth(app: web.Application, credentials: List[str]):
    for entry in credentials:
        try:
            username, password = entry.split(",")
        except ValueError as error:
            raise ValueError("Invalid basic auth format {entry}: {error}".format(
                entry=entry,
                error=error
            ))

        encoded = b64encode("{}:{}".format(username, password).encode()).decode()
        app["auth"].add("Basic " + encoded)


def setup_bearer_auth(app: web.Application, credentials: List[str]):
    for entry in credentials:
        app["auth"].add("Bearer " + entry)
