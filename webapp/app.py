# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""The FastAPI application -- the route table and nothing else.

``create_app()`` builds the app and returns it. **Importing this module must not start a
server, open a socket or touch the network**, so a headless test can import it and drive
the routes through ``TestClient``. Starting uvicorn is ``webapp.__main__``'s job.

The engine seam is ``control.HeadlessController``, the same object ``cli.py`` talks to over
a socket. This module never imports ``generate_tk.py`` -- that builds the whole Tk window at
import time -- and ``tests/test_web_engine.py`` asserts the import graph stays clean.

Phase 1 serves only the shell: the stylesheet, the static bundle and a health probe. The
auth gate lands in Phase 2 and the engine routes in Phase 3; both plug into the hooks left
here (``_install_auth`` / ``_install_api``) rather than being bolted on beside them.
"""

import asyncio
import json
import os

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from . import theme
from .config import WebConfig
from .engine import Engine, EngineError, build_engine

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')

# How often `/ws/stats` pushes. The engine's own stats are cheap (they read app_state),
# so a 1 s tick keeps the UI live without measurable cost.
WS_TICK_SECONDS = 1.0

# The single page. Everything else is an ES module under /static.
INDEX_HTML = os.path.join(STATIC_DIR, 'index.html')


def create_app(config=None, controller_factory=None, serve_socket=True, start_metrics=True):
    """Build and return the FastAPI app.

    ``config`` is a :class:`webapp.config.WebConfig`; it is optional so a test can build an
    app with the defaults and never bind anything.

    ``controller_factory``, ``serve_socket`` and ``start_metrics`` are for tests: a fake
    controller lets ``tests/test_web_engine.py`` drive every verb with no config file, no
    database and no network, ``serve_socket=False`` skips the control socket so a test process
    cannot collide with a real one, and ``start_metrics=False`` keeps the vLLM metrics poller
    from making HTTP requests during a test.
    """
    config = config or WebConfig()
    app = FastAPI(
        title='ReadyArt Synthetic Dataset Generator',
        version=_version(),
        docs_url=None,          # no interactive API docs on a password-protected tool
        redoc_url=None,
        openapi_url=None,
    )
    app.state.web_config = config

    # --- static ---------------------------------------------------------

    # The stylesheet is served from theme.py rather than a .css file so there is exactly one
    # definition of every colour. A second copy on disk would drift.
    @app.get('/static/theme.css', include_in_schema=False)
    def theme_css():
        return PlainTextResponse(theme.STYLESHEET, media_type='text/css')

    if os.path.isdir(STATIC_DIR):
        app.mount('/static', StaticFiles(directory=STATIC_DIR), name='static')

    # --- health ---------------------------------------------------------

    @app.get('/api/health')
    def health():
        """Unauthenticated liveness probe. Deliberately says nothing about the run.

        This is the one route that answers without a session, because a reverse proxy or a
        systemd unit needs a cheap liveness check. It must never leak whether a password is
        set, what the config path is, or any engine state.
        """
        return {'ok': True, 'app': 'webapp', 'version': _version()}

    # --- page -----------------------------------------------------------

    @app.get('/', response_class=HTMLResponse)
    def index():
        return HTMLResponse(_read_index())

    # Auth first: the gate is middleware, so it wraps every route registered after it. The
    # engine routes below are therefore gated without declaring anything themselves.
    from .security import install_auth
    install_auth(app, config)

    # The engine. Built lazily on first use so `import webapp.app` stays free of the engine's
    # dependency graph and of any socket.
    app.state.engine = None
    app.state.engine_factory = (lambda: build_engine(
        config, controller_factory=controller_factory, serve_socket=serve_socket,
        start_metrics=start_metrics))

    _install_api(app)

    # The Review tab. Separate module because it needs verbs the control socket does
    # not have (list/read/drop/dismiss flagged conversations); see
    # webapp/review_routes.py for why they are not added to control.py.
    from .review_routes import install_review
    install_review(app)

    # The six data tabs: APIs, Quality, Regex Watch, Metrics, Prompts, Queue. Read-only
    # views over data the engine already keeps, plus the three Queue actions. Separate module
    # so this file stays the shell and the auth gate rather than a route dump.
    from .tabs_routes import install_tabs
    install_tabs(app)

    # The Config editor. Phase 6b: one generic renderer over the whole config tree, so every
    # one of the 1,291 leaves is reachable and new ones can be added. Separate
    # module because it is the only route that *writes* the config, and that deserves to be
    # read on its own.
    from .config_routes import install_config
    install_config(app)

    # The Datasets panel. Phase 6d-a: the Tk panel's 696 lines become routes over
    # `dataset_settings`, which is already GUI-free. Separate module because it is the only
    # place that performs source I/O (reading column names) and the only writer of the
    # `datasets` subtree, and both deserve to be read on their own.
    from .datasets_routes import install_datasets
    install_datasets(app)

    # The Profiles panel. Phase 6d-b: the last bespoke widget. Separate module because it is
    # the only route that **replaces the whole config** in one operation, and because it has
    # to close two holes `config_loader.py` leaves open (a `yaml.dump` over `config.yml`, and
    # an unsanitized profile name that is a path component).
    from .profiles_routes import install_profiles
    install_profiles(app)

    # The dataset itself: download it, clear it, list the backups. Separate module because
    # it is the only place that streams a file back to the browser, and the only route that
    # deletes stored conversations. Both deserve to be read on their own.
    from .dataset_routes import install_dataset
    install_dataset(app)

    return app


def _get_engine(app):
    """The started engine, building it on first use.

    Lazy on purpose: importing ``webapp.app`` must not touch the network or open a socket, and a
    test that only exercises the auth gate must not need a config file.
    """
    engine = app.state.engine
    if engine is None:
        engine = app.state.engine_factory()
        app.state.engine = engine
    return engine


def start_engine(app):
    """Build the engine now, at process start.

    ``python -m webapp`` calls this **before** uvicorn binds, so the control socket is taken at
    boot rather than on the first request. That is what makes a second ``python -m webapp`` fail
    immediately with ``already_running`` instead of appearing to start and then failing on its first
    page load -- a lazy engine would leave the second process listening on a port it cannot serve.

    Raises :class:`webapp.engine.EngineError` with code ``already_running`` when another
    controller owns the socket. That is the singleton guard, not a bug.
    """
    return _get_engine(app)


def _install_api(app):
    """``GET /api/stats``, ``POST /api/cmd`` and ``WS /ws/stats``.

    All three are behind the auth gate (registered above), so none of them declares its own
    check. The payload is ``control.build_stats_payload``'s own document, unreshaped.
    """

    @app.get('/api/stats')
    def stats(include_recent: bool = True):
        try:
            return _get_engine(app).stats(include_recent=include_recent)
        except EngineError as exc:
            return error_response(exc.code, exc.message, status=exc.status)

    @app.post('/api/cmd')
    async def cmd(request: Request):
        try:
            body = await request.json()
        except (ValueError, json.JSONDecodeError):
            return error_response('bad_request', 'The body must be JSON.', status=400)
        if not isinstance(body, dict):
            return error_response('bad_request', 'The body must be a JSON object.',
                                 status=400)
        command = body.get('cmd')
        if not command:
            return error_response('bad_request', 'A request needs a "cmd".', status=400)
        args = body.get('args') or {}
        try:
            engine = _get_engine(app)
        except EngineError as exc:
            return error_response(exc.code, exc.message, status=exc.status)
        try:
            result = engine.command(command, args)
        except EngineError as exc:
            return error_response(exc.code, exc.message, status=exc.status)
        # `handle()` returns {ok, id, data|error}; drop the id, which the browser does
        # not use, and keep the rest exactly as the engine wrote it.
        result.pop('id', None)
        status = 200 if result.get('ok') else _status_for(result)
        return JSONResponse(result, status_code=status)

    @app.websocket('/ws/stats')
    async def stats_socket(websocket: WebSocket):
        """Push the same payload on the existing tick.

        A run outlives the browser tab, so the page re-renders from this stream and never from
        JS memory. The socket closes when the client goes away; the next page load reconnects.
        """
        await websocket.accept()
        try:
            engine = _get_engine(app)
        except EngineError as exc:
            await websocket.send_json({'ok': False,
                                       'error': {'code': exc.code,
                                                 'message': exc.message}})
            await websocket.close()
            return
        try:
            while True:
                try:
                    payload = engine.stats(include_recent=True)
                except EngineError as exc:
                    payload = {'ok': False,
                               'error': {'code': exc.code, 'message': exc.message}}
                await websocket.send_json(payload)
                await asyncio.sleep(WS_TICK_SECONDS)
        except (WebSocketDisconnect, RuntimeError):
            return


def _status_for(result):
    """The HTTP status for an engine error object, from its ``code``."""
    code = (result.get('error') or {}).get('code')
    if code in ('already_running',):
        return 409
    if code in ('not_running', 'no_daemon', 'empty'):
        return 404
    if code in ('refused',):
        return 403
    if code in ('timeout',):
        return 504
    if code in ('unknown_command', 'bad_request', 'protocol_mismatch'):
        return 400
    return 500


def _read_index():
    """The shell page, or a readable error page if the bundle is missing."""
    try:
        with open(INDEX_HTML, 'r', encoding='utf-8') as handle:
            return handle.read()
    except OSError as exc:
        return (
            '<!doctype html><meta charset="utf-8">'
            '<title>webapp</title>'
            f'<body style="background:{theme.BG};color:{theme.TEXT};'
            f'font-family:{theme.FONT_STACK};padding:32px">'
            '<h1>Static bundle missing</h1>'
            f'<p>Could not read <code>{INDEX_HTML}</code>: {exc}</p>'
            '</body>'
        )


def _version():
    from . import __version__
    return __version__


def error_response(code, message, status=400, extra=None):
    """The one error shape every JSON route uses: ``{ok: false, error: {code, message}}``.

    Matches ``control.HeadlessController.handle`` so the browser has a single error contract
    whether a failure came from the engine or from the web layer.

    ``extra`` is merged into the ``error`` object. The Config editor uses it to report every
    rejected field at once (``error.fields``), so a save with three bad numbers highlights all
    three rather than one per attempt.
    """
    error = {'code': code, 'message': message}
    if extra:
        error.update(extra)
    return JSONResponse({'ok': False, 'error': error}, status_code=status)
