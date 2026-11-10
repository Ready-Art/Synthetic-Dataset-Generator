# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Headless tests for the engine seam and the routes.

The gate has four parts, and all four are here:

1. **Every verb is driven through a fake controller headless.** No config file, no database,
   no network, no display.
2. **The CLI still works against a web-owned controller.** ``cli status``, ``cli stats`` and
   ``cli save`` are run as real subprocesses against a real socket this test owns, which is the
   regression test for "the CLI remains".
3. **A second controller is refused** with ``already_running`` -- the socket is the singleton
   guard, and there is no lock file.
4. **No GUI toolkit is imported.** Asserted by inspecting ``sys.modules`` after importing
   and exercising the whole web package, and by grepping the source for the import.
"""

import json
import os
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient

import control
from webapp.app import create_app
from webapp.config import WebConfig
from webapp.engine import BROWSER_COMMANDS, Engine, EngineError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOOD_PASSWORD = 'correct horse battery staple'


# --- a fake controller ---------------------------------------------------

class FakeController:
    """Stands in for ``HeadlessController``: same verbs, same response shape.

    Deliberately not a mock of ``handle``: it implements the verbs the way the real controller
    does, so a route that calls the wrong one fails here rather than in production.
    """

    COMMANDS = ('version', 'status', 'start', 'stop', 'pause', 'resume',
                'stats', 'export', 'save', 'clear', 'wait', 'shutdown')

    def __init__(self):
        self.calls = []
        self.phase = 'idle'
        self.active = False

    def handle(self, request):
        request_id = request.get('id')
        command = request.get('cmd')
        if command not in self.COMMANDS:
            return {'ok': False, 'id': request_id,
                    'error': {'code': 'unknown_command',
                              'message': f'Unknown command {command!r}.'}}
        args = request.get('args') or {}
        try:
            data = getattr(self, f'cmd_{command}')(**args)
        except control.CommandError as exc:
            return {'ok': False, 'id': request_id,
                    'error': {'code': exc.code, 'message': exc.message}}
        except TypeError as exc:
            return {'ok': False, 'id': request_id,
                    'error': {'code': 'bad_request', 'message': str(exc)}}
        self.calls.append(command)
        return {'ok': True, 'id': request_id, 'data': data}

    # --- the verbs ------------------------------------------------------

    def cmd_version(self):
        return {'version': control.GENERATOR_VERSION, 'protocol': control.PROTOCOL_VERSION}

    def cmd_status(self):
        return {'running': self.active, 'generating': self.active,
                'phase': self.phase, 'pid': 4242, 'uptime_s': 1.0,
                'version': control.GENERATOR_VERSION, 'threads': 4,
                'alive_threads': 4 if self.active else 0,
                'tasks': {'pending': 0, 'completed': 0, 'failed': 0},
                'progress': {'processed': 0, 'total': 0, 'percent': 0.0, 'queued': 0}}

    def cmd_start(self, dry_run=False, turns=None, chunks=None, threads=None,
                  force_fresh=False, resume=None):
        if self.active:
            raise control.CommandError('already_running',
                                      'A generation run is already in progress.')
        if dry_run:
            return {'dry_run': True, 'started': False, 'threads': 4, 'turns': 2}
        self.active = True
        self.phase = 'running'
        return {'started': True, 'threads': 4, 'turns': 2, 'tasks_queued': 8}

    def cmd_stop(self, clear=False, timeout=30.0):
        self.active = False
        self.phase = 'idle'
        return {'stopped': True, 'cleared': bool(clear)}

    def cmd_pause(self):
        self.phase = 'paused'
        return {'paused': True}

    def cmd_resume(self):
        self.phase = 'running'
        return {'resumed': True}

    def cmd_stats(self, include_recent=True, filters_only=False):
        payload = {
            'ok': True,
            'generated_at': '2026-11-09 00:00:00',
            'generator': {'version': control.GENERATOR_VERSION, 'pid': 4242, 'uptime_s': 1.0,
                          'phase': self.phase, 'config_path': 'config/config.yml',
                          'protocol': control.PROTOCOL_VERSION},
            'run': {'active': self.active, 'generating': self.active,
                    'paused': self.phase == 'paused', 'stopping': False,
                    'threads': 4, 'alive_threads': 4 if self.active else 0,
                    'duplication_mode': False,
                    'tasks': {'pending': 0, 'completed': 0, 'failed': 0},
                    'progress': {'processed': 0, 'total': 0, 'percent': 0.0,
                                  'queued': 0},
                    'run_started_at': None, 'run_elapsed_s': 0.0},
            'output': {'sink': 'jsonl', 'jsonl_files': [], 'jsonl_rows': 0,
                        'database': {'enabled': False, 'connected': False,
                                      'rows': None}},
            'tokens': {'input': 0, 'output': 0, 'estimated_cost': 0.0},
            'attempts': {'total': 0, 'per_api': {}},
            'detected': {},
            'filtered': {},
            'quality': {'enabled': False, 'scored': 0},
            'detection_config': {},
        }
        if include_recent:
            payload['regex_watch_log'] = []
            payload['cast_events_log'] = []
        return payload

    def cmd_export(self, output=None, source='auto', append=False):
        return {'ok': True, 'source': 'jsonl', 'path': output or '/tmp/out.jsonl',
                'rows': 0, 'message': 'exported'}

    def cmd_save(self, output=None, source='auto', append=False):
        return self.cmd_export(output=output, source=source, append=append)

    def cmd_clear(self, confirm=False, target='both', backup=True):
        if not confirm:
            raise control.CommandError('refused',
                                      'clear is destructive and needs confirm=true.')
        return {'target': target, 'backup_path': None, 'db_cleared': True,
                'jsonl_removed': 0, 'state_removed': False}

    def cmd_wait(self, timeout=0.0):
        return {'waited': False, 'note': 'No run is active.', 'running': False}

    def cmd_shutdown(self, force=False, timeout=30.0):
        return {'shutting_down': True}


def make_client(tmpdir, serve_socket=False, controller_factory=None):
    """An authenticated client over an app whose engine is a fake controller."""
    config = WebConfig(auth_file=os.path.join(tmpdir, 'auth.json'))
    app = create_app(config,
                     controller_factory=controller_factory or FakeController,
                     serve_socket=serve_socket)
    app.state.auth.set_password(GOOD_PASSWORD)
    client = TestClient(app, follow_redirects=False)
    client.post('/login', data={'password': GOOD_PASSWORD})
    return app, client


# --- 1. every verb, headless -------------------------------------------

def test_every_browser_verb_round_trips():
    """Drive every verb in BROWSER_COMMANDS through POST /api/cmd."""
    with tempfile.TemporaryDirectory() as tmpdir:
        app, client = make_client(tmpdir)
        args = {
            'start': {'dry_run': True},
            'clear': {'confirm': True},
            'wait': {'timeout': 0.0},
            'stats': {},
        }
        for command in BROWSER_COMMANDS:
            response = client.post('/api/cmd',
                                   json={'cmd': command, 'args': args.get(command, {})})
            assert response.status_code == 200, (
                f'{command} answered {response.status_code}: {response.text}')
            body = response.json()
            assert body.get('ok') is True, f'{command} failed: {body}'
            assert 'data' in body, f'{command} returned no data'
            assert 'id' not in body, 'the response leaked the request id'


def test_shutdown_is_not_reachable_from_the_browser():
    """A tab must not be able to stop the process that serves it."""
    with tempfile.TemporaryDirectory() as tmpdir:
        app, client = make_client(tmpdir)
        response = client.post('/api/cmd', json={'cmd': 'shutdown'})
        assert response.status_code == 400
        assert response.json()['error']['code'] == 'unknown_command'
        assert 'shutdown' not in BROWSER_COMMANDS


def test_an_unknown_verb_is_a_400():
    with tempfile.TemporaryDirectory() as tmpdir:
        app, client = make_client(tmpdir)
        response = client.post('/api/cmd', json={'cmd': 'nonsense'})
        assert response.status_code == 400
        assert response.json()['error']['code'] == 'unknown_command'


def test_a_bad_body_is_a_400():
    with tempfile.TemporaryDirectory() as tmpdir:
        app, client = make_client(tmpdir)
        assert client.post('/api/cmd', content=b'not json').status_code == 400
        assert client.post('/api/cmd', json=[1, 2, 3]).status_code == 400
        assert client.post('/api/cmd', json={}).status_code == 400


def test_a_bad_args_shape_is_a_400():
    with tempfile.TemporaryDirectory() as tmpdir:
        app, client = make_client(tmpdir)
        response = client.post('/api/cmd', json={'cmd': 'stats', 'args': 'nope'})
        assert response.status_code == 400
        assert response.json()['error']['code'] == 'bad_request'


def test_an_engine_error_keeps_its_code_and_status():
    """A refused clear is 403, not a generic 500, and keeps the engine's code."""
    with tempfile.TemporaryDirectory() as tmpdir:
        app, client = make_client(tmpdir)
        response = client.post('/api/cmd', json={'cmd': 'clear', 'args': {}})
        assert response.status_code == 403
        assert response.json()['error']['code'] == 'refused'


def test_already_running_is_a_409():
    with tempfile.TemporaryDirectory() as tmpdir:
        app, client = make_client(tmpdir)
        assert client.post('/api/cmd', json={'cmd': 'start', 'args': {}}).status_code == 200
        second = client.post('/api/cmd', json={'cmd': 'start', 'args': {}})
        assert second.status_code == 409
        assert second.json()['error']['code'] == 'already_running'


def test_stats_is_the_engine_payload_unreshaped():
    """The browser must read exactly what `cli stats` reads."""
    with tempfile.TemporaryDirectory() as tmpdir:
        app, client = make_client(tmpdir)
        response = client.get('/api/stats')
        assert response.status_code == 200
        body = response.json()
        expected = app.state.engine.controller.cmd_stats(include_recent=True)
        assert body == expected, 'the route reshaped the engine payload'
        for key in ('generator', 'run', 'output', 'tokens', 'attempts',
                    'detected', 'filtered', 'quality', 'detection_config'):
            assert key in body, f'the payload lost {key!r}'


def test_stats_include_recent_can_be_turned_off():
    with tempfile.TemporaryDirectory() as tmpdir:
        app, client = make_client(tmpdir)
        body = client.get('/api/stats?include_recent=false').json()
        assert 'regex_watch_log' not in body
        assert 'cast_events_log' not in body


# --- 2. the CLI still works against a web-owned controller --------------

def _run_cli(args, socket_path, timeout=30):
    """Run the real CLI as a subprocess against ``socket_path``."""
    command = [sys.executable, '-B', '-m', 'cli', '--socket', socket_path] + args
    return subprocess.run(command, cwd=ROOT, capture_output=True, text=True,
                          timeout=timeout)


def test_the_cli_works_against_a_web_owned_controller():
    """The regression test for "the CLI remains".

    A real ``ControlServer`` is started over a real socket by an ``Engine``, then the
    real ``cli`` is run as a subprocess against it. This is the one test that proves the
    web app did not quietly break the terminal front end.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        socket_path = os.path.join(tmpdir, 'control.sock')
        engine = Engine(WebConfig(socket_path=socket_path),
                        controller_factory=FakeController, serve_socket=True)
        engine.start()
        try:
            status = _run_cli(['status', '--text'], socket_path)
            assert status.returncode == 0, (
                f'cli status exited {status.returncode}: {status.stderr}')
            assert 'idle' in status.stdout, status.stdout

            stats = _run_cli(['stats', '--filters-only', '--text'], socket_path)
            assert stats.returncode == 0, (
                f'cli stats exited {stats.returncode}: {stats.stderr}')

            save = _run_cli(['save', '--output',
                             os.path.join(tmpdir, 'out.jsonl')], socket_path)
            assert save.returncode == 0, (
                f'cli save exited {save.returncode}: {save.stderr}')

            version = _run_cli(['version', '--json'], socket_path)
            assert version.returncode == 0
            assert json.loads(version.stdout)['version'] == control.GENERATOR_VERSION
        finally:
            engine.stop()


# --- 3. the socket is the singleton guard -------------------------------

def test_a_second_controller_is_refused():
    """The socket refuses a second controller, with no lock file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        socket_path = os.path.join(tmpdir, 'control.sock')
        first = Engine(WebConfig(socket_path=socket_path),
                       controller_factory=FakeController, serve_socket=True)
        first.start()
        try:
            second = Engine(WebConfig(socket_path=socket_path),
                            controller_factory=FakeController, serve_socket=True)
            try:
                second.start()
            except EngineError as exc:
                assert exc.code == 'already_running', exc.code
                assert 'already listening' in exc.message, exc.message
            else:
                raise AssertionError('a second controller was allowed to start')
        finally:
            first.stop()


def test_start_engine_refuses_at_boot_not_on_first_request():
    """The socket must be taken at process start, not lazily.

    A lazy engine would let a second ``python -m webapp`` bind its port, appear to start,
    and only fail on its first page load -- which reads as a broken app rather than as
    "already running". ``start_engine`` is what ``__main__`` calls before uvicorn binds.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        socket_path = os.path.join(tmpdir, 'control.sock')
        first = Engine(WebConfig(socket_path=socket_path),
                       controller_factory=FakeController, serve_socket=True)
        first.start()
        try:
            config = WebConfig(auth_file=os.path.join(tmpdir, 'auth.json'),
                               socket_path=socket_path)
            app = create_app(config, controller_factory=FakeController,
                             serve_socket=True)
            from webapp.app import start_engine
            try:
                start_engine(app)
            except EngineError as exc:
                assert exc.code == 'already_running', exc.code
                assert exc.status == 409, exc.status
            else:
                raise AssertionError('start_engine accepted a second controller')
        finally:
            first.stop()


def test_the_socket_is_freed_after_a_stop():
    with tempfile.TemporaryDirectory() as tmpdir:
        socket_path = os.path.join(tmpdir, 'control.sock')
        first = Engine(WebConfig(socket_path=socket_path),
                        controller_factory=FakeController, serve_socket=True)
        first.start()
        assert os.path.exists(socket_path)
        first.stop()
        assert not os.path.exists(socket_path), 'the socket file outlived the server'
        second = Engine(WebConfig(socket_path=socket_path),
                         controller_factory=FakeController, serve_socket=True)
        second.start()
        second.stop()


def test_cli_launch_is_refused_while_the_web_app_owns_the_socket():
    """`cli launch` must be refused with already_running, not start a second run."""
    with tempfile.TemporaryDirectory() as tmpdir:
        socket_path = os.path.join(tmpdir, 'control.sock')
        engine = Engine(WebConfig(socket_path=socket_path),
                         controller_factory=FakeController, serve_socket=True)
        engine.start()
        try:
            result = _run_cli(['launch'], socket_path)
            assert result.returncode == control.EXIT_ALREADY_RUNNING, (
                f'cli launch exited {result.returncode}, expected '
                f'{control.EXIT_ALREADY_RUNNING}: {result.stdout} {result.stderr}')
        finally:
            engine.stop()


def test_engine_start_is_idempotent():
    with tempfile.TemporaryDirectory() as tmpdir:
        socket_path = os.path.join(tmpdir, 'control.sock')
        engine = Engine(WebConfig(socket_path=socket_path),
                         controller_factory=FakeController, serve_socket=True)
        try:
            engine.start()
            controller = engine.controller
            engine.start()
            assert engine.controller is controller, 'start() built a second controller'
        finally:
            engine.stop()


# --- 4. no GUI toolkit is imported ----------------------------------------

GUI_TOOLKITS = ('tkinter', 'ttkbootstrap', 'matplotlib', 'PyQt5', 'PyQt6',
                'PySide2', 'PySide6', 'wx')


def test_no_gui_toolkit_is_imported():
    """Importing and exercising the web package must not import a GUI toolkit.

    A toolkit imported here would make every headless path need a display, and the web UI
    is the GUI -- it must never pull one in.
    """
    loaded = sorted(name for name in sys.modules
                    if name.split('.')[0] in GUI_TOOLKITS)
    assert not loaded, f'a GUI toolkit was imported: {loaded}'


def test_no_webapp_module_imports_a_gui_toolkit():
    """Grep the source: a static import would fail before the runtime check above."""
    import ast
    offenders = []
    for dirpath, dirnames, filenames in os.walk(os.path.join(ROOT, 'webapp')):
        dirnames[:] = [d for d in dirnames if d != '__pycache__']
        for name in filenames:
            if not name.endswith('.py'):
                continue
            path = os.path.join(dirpath, name)
            with open(path, 'r', encoding='utf-8') as handle:
                tree = ast.parse(handle.read())
            for node in ast.walk(tree):
                targets = []
                if isinstance(node, ast.Import):
                    targets = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    targets = [node.module or '']
                for target in targets:
                    if target.split('.')[0] in GUI_TOOLKITS:
                        offenders.append(
                            f'{os.path.relpath(path, ROOT)}: import {target}')
    assert not offenders, f'webapp imports a GUI toolkit: {offenders}'


def test_the_engine_does_not_import_a_gui_toolkit():
    """The engine seam must not reach a GUI toolkit through any module."""
    import webapp.engine as engine_module
    source = open(engine_module.__file__, 'r', encoding='utf-8').read()
    for forbidden in GUI_TOOLKITS:
        assert f'import {forbidden}' not in source, (
            f'webapp/engine.py imports {forbidden!r}')


# --- the websocket ------------------------------------------------------

def test_the_stats_socket_pushes_the_same_payload():
    """`/ws/stats` pushes what `/api/stats` returns, on the same tick."""
    with tempfile.TemporaryDirectory() as tmpdir:
        app, client = make_client(tmpdir)
        with client.websocket_connect('/ws/stats') as socket:
            payload = socket.receive_json()
        expected = app.state.engine.controller.cmd_stats(include_recent=True)
        assert payload == expected, 'the socket payload differs from /api/stats'


def test_the_stats_socket_requires_a_session():
    """The socket is behind the gate too: no cookie, no stream."""
    with tempfile.TemporaryDirectory() as tmpdir:
        app, client = make_client(tmpdir)
        client.cookies.clear()
        try:
            with client.websocket_connect('/ws/stats') as socket:
                socket.receive_json()
        except Exception:  # noqa: BLE001 - a rejected handshake is the point
            pass
        else:
            raise AssertionError('an unauthenticated client opened /ws/stats')


# --- 5. the version is declared once ------------------------------------

def test_the_webapp_version_matches_the_engine():
    """``webapp.__version__`` is a copy, not an import, so it can drift.

    ``webapp/__init__.py`` says in a comment that this test keeps the two in step. Until
    Phase 9 nothing did: the copy was a literal, and a bump to ``control.GENERATOR_VERSION``
    alone would have left the web app reporting the previous release. The copy exists on
    purpose -- importing ``control`` would drag ``generation.py`` and ``requests`` into a
    package that must stay importable headless -- so the agreement has to be asserted here.
    """
    import webapp

    assert webapp.__version__ == control.GENERATOR_VERSION, (
        f'webapp.__version__ is {webapp.__version__!r} but control.GENERATOR_VERSION is '
        f'{control.GENERATOR_VERSION!r}; bump both')


def test_the_health_route_reports_the_same_version():
    """The one unauthenticated route names a version, so it must not name a stale one."""
    app = create_app(WebConfig(auth_file=os.path.join(tempfile.mkdtemp(), 'auth.json')),
                     controller_factory=FakeController, serve_socket=False)
    client = TestClient(app)
    body = client.get('/api/health').json()
    assert body['version'] == control.GENERATOR_VERSION, body


if __name__ == '__main__':
    failures = 0
    for name, function in sorted(globals().items()):
        if name.startswith('test_') and callable(function):
            try:
                function()
            except AssertionError as exc:
                failures += 1
                print(f'FAIL {name}: {exc}')
            except Exception as exc:  # noqa: BLE001 - a harness reports, never raises
                failures += 1
                print(f'FAIL {name}: {type(exc).__name__}: {exc}')
            else:
                print(f'ok   {name}')
    print(f"\n{'OK' if not failures else str(failures) + ' FAILED'}")
    sys.exit(1 if failures else 0)
