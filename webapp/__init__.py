# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

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
        engine.py       the HeadlessController wrapper (Phase 3)
        security/       password policy, session cookie, rate limiter (Phase 2)
        static/         index.html, ES modules, no build step

Two rules that are easy to break and are asserted by tests:

* **Importing this package must not start a server, open a socket or touch the network.**
  ``import webapp.app`` is safe from a headless test.
* **Never import ``generate_tk.py``.** It builds the entire Tk window at import time.
"""

__all__ = ['__version__']

# Kept in step with ``control.GENERATOR_VERSION`` by tests/test_web_engine.py rather than
# imported, so importing this package stays free of the engine's dependency graph.
__version__ = '15.0.0'
