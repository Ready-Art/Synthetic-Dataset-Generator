# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""``python -m webapp`` -- start the web front end.

This is the only place that starts uvicorn. Importing ``webapp.app`` does not, so a test
can build the app without binding a port.

The web app **owns the engine**: it constructs the ``HeadlessController`` and starts the
control socket. That is what keeps ``cli.py`` working against it and what makes the socket the
singleton guard -- a second ``python -m webapp`` is refused by
``ControlServer._prepare_socket_path`` with no new code. The engine seam wires that up; the
HTTP server starts first.

Usage::

    python -m webapp                     # 127.0.0.1:8765, no TLS, no nginx
    python -m webapp --port 9000
    python -m webapp --trust-proxy       # behind nginx terminating HTTPS
"""

import argparse
import sys

from .config import DEFAULT_HOST, DEFAULT_PORT, WebConfig


def build_parser():
    parser = argparse.ArgumentParser(
        prog='python -m webapp',
        description='Serve the ReadyArt Synthetic Dataset Generator web front end.',
    )
    parser.add_argument('--host', default=DEFAULT_HOST,
                        help=f'Bind address (default {DEFAULT_HOST}, loopback only).')
    parser.add_argument('--port', type=int, default=DEFAULT_PORT,
                        help=f'Bind port (default {DEFAULT_PORT}).')
    parser.add_argument('--config', default=None,
                        help='Path to the config file the engine should run from.')
    parser.add_argument('--socket', default=None,
                        help='Control socket path (default: the config\'s control.socket_path).')
    parser.add_argument('--trust-proxy', action='store_true',
                        help='Believe X-Forwarded-For / X-Forwarded-Proto from a reverse '
                             'proxy. Required behind nginx; never set it on a direct bind.')
    parser.add_argument('--allow-insecure-http', action='store_true',
                        help='Permit a non-loopback bind without TLS. Off by default: it '
                             'sends a password in the clear.')
    parser.add_argument('--auth-file', default=None,
                        help='Where to keep the password hash and session secret '
                             '(default: webapp/state/auth.json, mode 0600).')
    parser.add_argument('--open-browser', action='store_true',
                        help='Open the default browser at the served URL on startup.')
    parser.add_argument('--log-level', default='info',
                        choices=('critical', 'error', 'warning', 'info', 'debug', 'trace'),
                        help='uvicorn log level (default info).')
    return parser


def main(argv=None):
    """Parse ``argv``, build the app and serve it. Returns a process exit code."""
    args = build_parser().parse_args(argv)
    config = WebConfig(
        host=args.host,
        port=args.port,
        config_path=args.config,
        socket_path=args.socket,
        trust_proxy=args.trust_proxy,
        allow_insecure_http=args.allow_insecure_http,
        open_browser=args.open_browser,
        log_level=args.log_level,
        auth_file=args.auth_file,
    )
    try:
        config.check()
    except ValueError as exc:
        print(f'webapp: {exc}', file=sys.stderr)
        return 2

    import uvicorn

    from .app import create_app

    app = create_app(config)

    # First-run setup: print the one-time token to the console, never over HTTP.
    from .security.routes import announce_setup_token
    announce_setup_token(app)

    # Take the control socket BEFORE uvicorn binds. A second `python -m webapp`
    # must fail here, at boot, rather than start and then fail on its first page load.
    from .app import start_engine
    from .engine import EngineError
    try:
        start_engine(app)
    except EngineError as exc:
        print(f'webapp: {exc.message}', file=sys.stderr)
        return 4 if exc.code == 'already_running' else 1

    if config.open_browser:
        _open_later(config.url())

    print(f'webapp: serving on {config.url()} ({config.mode} mode)', flush=True)
    try:
        uvicorn.run(app, host=config.host, port=config.port, log_level=config.log_level,
                    access_log=False)
    finally:
        # Release the socket so the next start is not refused by a stale file.
        app.state.engine.stop() if app.state.engine is not None else None
    return 0


def _open_later(url):
    """Open ``url`` in the default browser a moment after the server binds."""
    import threading
    import webbrowser

    def _open():
        webbrowser.open(url)

    timer = threading.Timer(1.2, _open)
    timer.daemon = True
    timer.start()


if __name__ == '__main__':
    sys.exit(main())
