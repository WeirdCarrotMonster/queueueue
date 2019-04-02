from base64 import b64encode
from typing import List

from aiohttp import web

from .stats.collector import StatCollector
from .taskqueue import MultiLockPriorityPoolQueue


def build_app() -> web.Application:
    app = web.Application()
    app["queue"] = MultiLockPriorityPoolQueue()
    app["auth"] = set()
    app["stats"] = StatCollector()
    return app


def get_encoded_auth(username: str, password: str) -> str:
    return b64encode("{}:{}".format(username, password).encode()).decode()


def setup_basic_auth(app: web.Application, credentials: List[str]):
    for entry in credentials:
        try:
            username, password = entry.split(":")
        except ValueError as error:
            raise ValueError("Invalid basic auth format {entry}: {error}".format(
                entry=entry,
                error=error
            ))

        app["auth"].add("Basic " + get_encoded_auth(username, password))


def setup_bearer_auth(app: web.Application, credentials: List[str]):
    for entry in credentials:
        app["auth"].add("Bearer " + entry)
