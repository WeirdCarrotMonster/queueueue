from logging import getLogger

from aiohttp.web_response import json_response

from queueueue.utils import safe_int_conversion

from .taskqueue import Task


def authenticate(func):

    async def wrapper(request, *args, **kwargs):
        provided_auth = request.headers.get("AUTHORIZATION")
        if request.app["auth"] and provided_auth not in request.app["auth"]:
            getLogger("aiohttp.access").warning(
                "Request with invalid auth credentials blocked: %s %s",
                request.method, request.path_qs)
            return json_response({
                "error": "Not authorized"
            }, status=403)
        return await func(request, *args, **kwargs)

    return wrapper


@authenticate
async def list_tasks(request):
    offset = safe_int_conversion(
        request.query.get("offset"), 0,
        min_val=0, max_val=request.app["queue"].task_count
    )
    limit = safe_int_conversion(
        request.query.get("limit"), 50,
        min_val=1, max_val=50
    )

    return json_response([
        task.for_json()
        for task in request.app["queue"].tasks[offset:offset + limit]
    ])


@authenticate
async def list_taken_tasks(request):
    offset = safe_int_conversion(
        request.query.get("offset"), 0,
        min_val=0, max_val=request.app["queue"].task_count
    )
    limit = safe_int_conversion(
        request.query.get("limit"), 50,
        min_val=1, max_val=50
    )

    return json_response([
        task.for_json()
        for task in request.app["queue"].tasks_taken[offset:offset + limit]
    ])


@authenticate
async def add_task(request):
    data = await request.json()
    task = Task(**data)

    unique = request.query.get("unique", "").lower() == "true"
    wait = request.query.get("wait", "").lower() == "true"

    request.app["queue"].put(task, unique=unique)

    if wait:
        await task.completed.wait()
        result = task.completed.data
    else:
        result = {"result": "success"}

    return json_response(result)


@authenticate
async def get_task(request):
    pool = request.query.get("pool")
    if not pool:
        return json_response(None)

    task = request.app["queue"].get(pool=pool)
    data = task.worker_info if task else None
    return json_response(data)


@authenticate
async def complete_task(request):
    _id = request.match_info.get('task_id')
    data = await request.json()

    try:
        request.app["queue"].complete(_id, data)
        return json_response({"result": "Success"})
    except LookupError:
        return json_response({"error": "Unknown task"}, status=404)


@authenticate
async def delete_task(request):
    _id = request.match_info.get('task_id')
    try:
        request.app["queue"].safe_remove(_id)
        return json_response({"result": "Success"})
    except LookupError:
        return json_response({"error": "Unknown task"}, status=404)
