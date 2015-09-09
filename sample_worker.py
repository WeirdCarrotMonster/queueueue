from overseer.worker import Worker
from time import sleep
import logging


logging.basicConfig(level="DEBUG")


w = Worker("http://10.10.0.45:8080", "default")


@w.task
def baton(size):
    sleep(size)
    return size * 2


if __name__ == "__main__":
    w.run()
