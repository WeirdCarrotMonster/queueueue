import argparse
import logging

import asyncio

from .queueueue import Manager


def main():
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
        logging.basicConfig(
            level=args.loglevel,
            format="%(asctime)-15s %(levelname)-8s %(message)s"
        )

    loop = asyncio.get_event_loop()

    m = Manager(loop, **config)

    try:
        loop.run_until_complete(m.create_server())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
