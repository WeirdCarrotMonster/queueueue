import argparse
import logging

import asyncio

from .queueueue import Manager


def build_handlers(manager, handlers):
    for handler in handlers:
        if handler.startswith("stdout://"):
            stdout_format = handler[9:]

            @asyncio.coroutine
            def log_stdout(message):
                print(stdout_format.format(**message.full_info))

            manager.result_handler(log_stdout)
        else:
            raise ValueError("Unknown handler type {0}".format(handler))


def main():
    parser = argparse.ArgumentParser(prog='queueueue')
    parser.add_argument("--host", help="queueueue listen address")
    parser.add_argument("--port", help="queueueue listen port")
    parser.add_argument("--auth", help="authentication credentials")
    parser.add_argument("--loglevel", help="logging verbosity level")
    parser.add_argument("--reshandlers", help="list of result handlers", nargs="*", type=str)

    args = parser.parse_args()

    config = {}
    if args.host:
        config["host"] = args.host
    if args.port:
        config["port"] = args.port
    if args.auth:
        config["auth"] = tuple(_ for _ in args.auth.split(":"))
    if args.loglevel:
        logging.basicConfig(
            level=args.loglevel,
            format="%(asctime)-15s %(levelname)-8s %(message)s"
        )

    loop = asyncio.get_event_loop()

    m = Manager(loop, **config)

    if args.reshandlers:
        build_handlers(m, args.reshandlers)

    try:
        loop.run_until_complete(m.create_server())
        loop.run_forever()
    except KeyboardInterrupt:
        m.logger.info("Stopping server")
