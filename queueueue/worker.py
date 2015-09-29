# coding=utf-8

import logging
from collections import namedtuple
from multiprocessing import Process, Queue
from time import sleep

import requests
import simplejson as json


class TaskResult(object):

    def __init__(self, status, result=None, stdout=None, stderr=None, traceback=None):
        self.status = status
        self.result = result
        self.stdout = stdout
        self.stderr = stderr
        self.traceback = traceback

    @property
    def data(self):
        return {
            "status": self.status,
            "result": self.result,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "traceback": self.traceback
        }


class WrappedProcess(Process):
    def __init__(self, queue, *args, **kwargs):
        self._queue = queue
        super(WrappedProcess, self).__init__(*args, **kwargs)

    def run(self):
        from io import StringIO
        import sys
        stdout = StringIO()
        stderr = StringIO()
        sys.stdout = stdout
        sys.stderr = stderr
        try:
            result = self._target(*self._args, **self._kwargs)
        except:
            result = None
            import traceback
            trace = traceback.format_exc()
        else:
            trace = None
        self._queue.put(TaskResult(
            result=result,
            stdout=stdout.getvalue(),
            stderr=stderr.getvalue(),
            traceback=trace,
            status="failure" if trace else "success"
        ))


class Worker(object):

    def __init__(self, api, pool, auth=None, sleep_time=10):
        self._api = api
        self._pool = pool
        self._auth = auth
        self._sleep_time = sleep_time

        self._tasks = {}

        self._current_task = None

    def task(self, arg):
        if not getattr(arg, "__call__", None):
            # Декоратор с параметрами - на вход передали имя функции
            def wrapper(f):
                self._register_task(f, arg)
                return f
        else:
            # Декоратор без параметров - оборачивает непосредственно функцию
            self._register_task(arg, arg.__name__)
            return arg

    def register_task(self, func, name=None):
        func_name = name or getattr(func, "func_name", None)

        self._register_task(func, func_name)

    def _register_task(self, func, name):
        logging.debug("Registered function {} for task {}".format(func, name))
        self._tasks[name] = func

    def _api_entry(self, point):
        return "{}/{}".format(self._api, point)

    def _request_task(self):
        r = requests.patch(
            self._api_entry("task/pending"),
            params={"pool": self._pool},
            auth=self._auth
        )
        data = r.json()

        if r.status_code != 200:
            raise requests.ConnectionError(data)
        if not data:
            raise StopIteration
        return data

    def _report_task(self, task_id, task_result):
        r = requests.patch(
            self._api_entry("task/{}".format(task_id)),
            data=json.dumps(task_result.data),
            auth=self._auth
        )

    def _wait_task(self):
        sleep(self._sleep_time)

    def _check_handler(self, task_name):
        if task_name not in self._tasks:
            raise LookupError

    def _run_task(self, task_name, args, kwargs):
        logging.info("Starting task {}".format(task_name))
        task = self._tasks.get(task_name)
        q = Queue()
        p = WrappedProcess(target=task, args=args, kwargs=kwargs, queue=q)
        p.start()
        p.join()

        result = q.get()

        if result.status == "success":
            logging.info("Task {} finished successfully".format(task_name))
            logging.debug("stdout:\n{}".format(result.stdout))
            logging.debug("stderr:\n{}".format(result.stderr))
            logging.debug("result:\n{}".format(result.result))
        else:
            logging.warning("Task {} finished with status {}".format(task_name, result.status))
            logging.debug("stdout:\n{}".format(result.stdout))
            logging.debug("stderr:\n{}".format(result.stderr))
            logging.debug("traceback:\n{}".format(result.traceback))

        return result

    def run(self):
        while True:
            try:
                task = self._request_task()
                self._check_handler(task.get("name"))
                task_result = self._run_task(
                    task.get("name"), task.get("args", []), task.get("kwargs", {})
                )
                self._report_task(task.get("id"), task_result)
            except requests.ConnectionError as e:
                logging.warning("Connection error: {}".format(e))
                self._wait_task()
            except StopIteration:
                logging.debug("No task received, sleeping")
                self._wait_task()
            except LookupError:
                logging.critical("Unknown task type received {}".format(task.get("name")))
                self._report_task(task.get("id"), TaskResult(
                    "failure", traceback="unknown task"
                ))
            except KeyboardInterrupt:
                break
