from typing import TYPE_CHECKING

from . import views

if TYPE_CHECKING:  # pragma: no cover
    from aiohttp import web


def setup_routes(app: "web.Application"):
    app.router.add_route('GET', '/task', views.list_tasks)
    app.router.add_route('POST', '/task', views.add_task)

    app.router.add_route('PATCH', '/task/pending', views.get_task)

    app.router.add_route('DELETE', '/task/{task_id}', views.delete_task)
    app.router.add_route('PATCH', '/task/{task_id}', views.complete_task)
