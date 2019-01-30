import argparse

from aiohttp import web

from queueueue.app import build_app, setup_basic_auth, setup_bearer_auth
from queueueue.routes import setup_routes


def main():
    parser = argparse.ArgumentParser(prog='queueueue')
    parser.add_argument("--host", help="queueueue listen address")
    parser.add_argument("--port", help="queueueue listen port")
    parser.add_argument("--auth-basic", help="authentication credentials", action="append")
    parser.add_argument("--auth-bearer", help="authentication credentials", action="append")

    args = parser.parse_args()

    app = build_app()
    setup_routes(app)

    if args.auth_basic:
        setup_basic_auth(app, args.auth_basic)

    if args.auth_bearer:
        setup_bearer_auth(app, args.auth_bearer)

    web.run_app(
        app,
        host=args.host,
        port=args.port
    )
