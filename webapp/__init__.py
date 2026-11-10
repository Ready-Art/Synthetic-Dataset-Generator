# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""The web front end for the ReadyArt Synthetic Dataset Generator.

This package is a **second client of the same engine** ``cli.py`` already drives. It does
not reimplement generation, detection or scoring: it owns a ``control.HeadlessController``
in-process and serves it to a browser.

Layout::

    webapp/
        __main__.py     python -m webapp  ->  uvicorn on 127.0.0.1:8765
        app.py          create_app() -> FastAPI; the route table
        config.py       bind address, TLS/proxy mode, CLI flags
        theme.py        the Storm palette and the one stylesheet (pure data)
        engine.py       the HeadlessController wrapper
        security/       password policy, session cookie, rate limiter
        static/         index.html, ES modules, no build step

Two rules that are easy to break and are asserted by tests:

* **Importing this package must not start a server, open a socket or touch the network.**
  ``import webapp.app`` is safe from a headless test.
* **This package is the only GUI.** It depends on no GUI toolkit and needs no display; the
  browser is the front end.
"""

__all__ = ['__version__']

# Kept in step with ``control.GENERATOR_VERSION`` by tests/test_web_engine.py rather than
# imported, so importing this package stays free of the engine's dependency graph.
__version__ = '16.0.1'
