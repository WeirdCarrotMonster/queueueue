import argparse
import logging
import os

from aiohttp import web

from .app import build_app, setup_basic_auth, setup_bearer_auth
from .routes import setup_routes


def main():
    parser = argparse.ArgumentParser(prog='queueueue')
    parser.add_argument("--host", help="queueueue listen address")
    parser.add_argument("--port", help="queueueue listen port")
    parser.add_argument("--auth-basic", help="authentication credentials", action="append")
    parser.add_argument("--auth-bearer", help="authentication credentials", action="append")
    parser.add_argument(
        "--loglevel",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"],
        default="INFO"
    )
    parser.add_argument(
        "--graphite",
        default=os.environ.get("QUEUE_GRAPHITE", None),
        help="Graphite stats server host")
    parser.add_argument(
        "--graphite-stats-root",
        default=os.environ.get("QUEUE_GRAPHITE_ROOT", "queue"),
        help="Graphite stats root key")
    parser.add_argument(
        "--graphite-freq",
        help="Graphite metric collection frequency",
        type=int, default=10
    )

    args = parser.parse_args()

    app = build_app()
    setup_routes(app)

    if args.auth_basic:
        setup_basic_auth(app, args.auth_basic)

    if args.auth_bearer:
        setup_bearer_auth(app, args.auth_bearer)

    if args.graphite:
        from .stats.pusher_graphite import GraphiteStatPusher
        pusher = GraphiteStatPusher(
            app["stats"],
            args.graphite,
            stats_root=args.graphite_stats_root,
            frequency=args.graphite_freq
        )

        pusher.start()

    logging.basicConfig(level=args.loglevel)

    web.run_app(
        app,
        host=args.host,
        port=args.port
    )


if __name__ == "__main__":
    main()
