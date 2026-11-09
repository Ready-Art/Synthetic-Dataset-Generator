# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""The engine seam -- the web app's one connection to the generator.

This module is a thin wrapper over ``control.HeadlessController``, which already runs a whole
generation job with no GUI. The web app is a **second client of the same engine**, not a rewrite
of it: every verb here is the controller's own verb, and ``build_stats_payload`` is the contract
the browser reads. Nothing is reshaped on the way through.

Two rules govern this module:

**The web app owns the controller AND serves the control socket.** ``Engine.start()``
constructs the controller and starts a ``ControlServer`` on the existing socket path, exactly as
``generate_tk.py`` does when ``control.enabled`` is true. That is what:

* keeps ``cli.py`` working -- ``cli status`` / ``stats`` / ``save`` attach to this socket;
* makes the socket the singleton guard -- a second ``python -m webapp`` is refused by
  ``ControlServer._prepare_socket_path`` with ``already_running``, and so is ``cli launch``.
  **No lock file**: it would be a second, weaker copy of a guard that already exists and would
  go stale after a crash where the socket does not.

**Never import ``generate_tk.py``.** It builds the entire Tk window at import time, so
importing it here would make the whole web app need a display. ``tests/test_web_engine.py``
asserts the import graph stays clean.

The controller is injectable so a test can drive a fake through every verb with no config file, no
database and no network.
"""

import threading

import control
import vllm_metrics

# The verbs the browser may invoke through ``POST /api/cmd``. A subset of
# ``HeadlessController.COMMANDS``: ``shutdown`` is deliberately absent, because a browser tab
# must not be able to stop the process that serves it. The CLI keeps ``shutdown``.
BROWSER_COMMANDS = (
    'version', 'status', 'start', 'stop', 'pause', 'resume',
    'stats', 'export', 'save', 'clear', 'wait',
)


class EngineError(RuntimeError):
    """A failure that the route layer turns into a JSON error response."""

    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


class Engine:
    """Owns the one ``HeadlessController`` this process is allowed to have.

    ``start()`` is idempotent and thread-safe: uvicorn may serve the first request from any
    thread, and two controllers would corrupt a run.
    """

    def __init__(self, config=None, controller_factory=None, serve_socket=True,
                 start_metrics=True):
        self.config = config
        self._controller_factory = controller_factory or self._default_factory
        self._serve_socket = serve_socket
        # The vLLM metrics poller. `generate_tk.py` starts it when the Tk window is built;
        # the web app has no window, so without this the Metrics tab's table fills in (it is
        # written by the generation threads) while every graph stays empty, because the ring
        # the graphs draw is only appended to by the poller. A test passes False.
        self._start_metrics = start_metrics
        self._controller = None
        self._server = None
        self._lock = threading.Lock()
        self._started = False
        self._error = None

    # --- construction ----------------------------------------------------

    def _default_factory(self):
        """Build the real controller from the web config.

        ``serve_socket=False`` because ``Engine`` starts the ``ControlServer`` itself: the
        controller's own ``start_server`` would build a second server over the same socket path and
        refuse to start, since the first one is already listening.
        """
        config = self.config
        return control.HeadlessController(
            config_path=getattr(config, 'config_path', None),
            serve_socket=False,
            socket_path=getattr(config, 'socket_path', None),
        )

    # --- lifecycle ------------------------------------------------------

    def start(self):
        """Build the controller and start the control socket. Idempotent.

        Raises :class:`EngineError` with code ``already_running`` when another controller owns
        the socket -- that is the singleton guard, not a bug.
        """
        with self._lock:
            if self._started:
                return self
            controller = self._controller_factory()
            self._controller = controller

            if self._serve_socket:
                socket_path = (getattr(self.config, 'socket_path', None)
                               or control.default_socket_path())
                server = control.ControlServer(controller, socket_path=socket_path)
                try:
                    server.start()
                except control.CommandError as exc:
                    # already_running / refused / failed: the socket is the guard, so this is
                    # the correct answer to a second `python -m webapp`.
                    self._error = exc
                    raise EngineError(exc.code, exc.message,
                                      status=409 if exc.code == 'already_running' else 500)
                self._server = server

            if self._start_metrics:
                # Best-effort telemetry: a poller that cannot start must not stop the app
                # from serving. `start_polling` is idempotent and returns the live thread.
                try:
                    vllm_metrics.start_polling()
                except Exception:  # noqa: BLE001 - telemetry is never fatal
                    pass

            self._started = True
            return self

    def stop(self):
        """Stop the control socket and the metrics poller. Safe to call more than once."""
        with self._lock:
            server, self._server = self._server, None
            self._started = False
        if server is not None:
            try:
                server.stop()
            except Exception:  # noqa: BLE001 - a shutdown must never raise
                pass
        if self._start_metrics:
            try:
                vllm_metrics.stop_polling()
            except Exception:  # noqa: BLE001 - a shutdown must never raise
                pass

    # --- accessors ------------------------------------------------------

    @property
    def controller(self):
        if self._controller is None:
            raise EngineError('not_started', 'The engine has not been started.', status=503)
        return self._controller

    @property
    def started(self):
        return self._started

    def socket_path(self):
        if self._server is not None:
            return self._server.socket_path
        return (getattr(self.config, 'socket_path', None)
                or control.default_socket_path())

    # --- the two verbs the routes need ----------------------------------

    def stats(self, include_recent=True):
        """The full stats document -- ``build_stats_payload``, unreshaped.

        The browser reads exactly what ``cli stats`` reads, so the two front ends cannot drift.
        """
        try:
            return self.controller.cmd_stats(include_recent=bool(include_recent))
        except control.CommandError as exc:
            raise EngineError(exc.code, exc.message, status=400)
        except Exception as exc:  # noqa: BLE001 - a route must answer, not crash
            raise EngineError('failed', str(exc), status=500)

    def command(self, cmd, args=None):
        """Dispatch one control verb and return ``{ok, data|error}``.

        The response shape is ``HeadlessController.handle``'s own, so the browser has one error
        contract whether a failure came from the engine or from the web layer.
        """
        if cmd not in BROWSER_COMMANDS:
            raise EngineError(
                'unknown_command',
                f'Unknown command {cmd!r}. Known: {", ".join(BROWSER_COMMANDS)}.',
                status=400)
        if args is not None and not isinstance(args, dict):
            raise EngineError('bad_request', '"args" must be an object.', status=400)
        request = {'cmd': cmd, 'args': args or {}}
        try:
            return self.controller.handle(request)
        except Exception as exc:  # noqa: BLE001 - handle() should not raise, but be safe
            raise EngineError('failed', str(exc), status=500)


def build_engine(config, controller_factory=None, serve_socket=True, start_metrics=True):
    """Construct and start an :class:`Engine`. Used by ``create_app``."""
    engine = Engine(config, controller_factory=controller_factory,
                    serve_socket=serve_socket, start_metrics=start_metrics)
    engine.start()
    return engine
