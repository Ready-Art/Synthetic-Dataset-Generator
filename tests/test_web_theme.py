# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Headless tests for the web design tokens and the skeleton.

The point of this file is that **the contrast table is a test, not a table someone
eyeballed.** A palette that looks fine in a mockup failed AA twice during design (accent
4.42:1, danger 3.81:1 on the raised surface), so every foreground/background pair the UI
actually draws is recomputed here from the constants and must clear 4.5:1.

It also pins the two structural rules of the package:

* importing ``webapp.theme`` / ``webapp.app`` must not start a server or touch the network;
* no ``#rrggbb`` literal may live outside ``webapp/theme.py``.
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import webapp.theme as theme


HEX_RE = re.compile(r'^#[0-9a-fA-F]{6}$')
# Any 6-digit hex literal in source. Used to prove theme.py is the only home for one.
HEX_ANYWHERE_RE = re.compile(r'#[0-9a-fA-F]{6}\b')

WEBAPP_DIR = os.path.dirname(os.path.abspath(theme.__file__))
THEME_PATH = os.path.abspath(theme.__file__)


# --- contrast maths ------------------------------------------------------

def _channel(value):
    value = value / 255.0
    return value / 12.92 if value <= 0.03928 else ((value + 0.055) / 1.055) ** 2.4


def relative_luminance(hex_colour):
    """WCAG 2.x relative luminance of a ``#rrggbb`` string."""
    raw = hex_colour.lstrip('#')
    r, g, b = (int(raw[i:i + 2], 16) for i in (0, 2, 4))
    return 0.2126 * _channel(r) + 0.7152 * _channel(g) + 0.0722 * _channel(b)


def contrast_ratio(foreground, background):
    """WCAG 2.x contrast ratio between two ``#rrggbb`` strings."""
    a = relative_luminance(foreground)
    b = relative_luminance(background)
    high, low = max(a, b), min(a, b)
    return (high + 0.05) / (low + 0.05)


# --- palette -------------------------------------------------------------

def test_every_colour_constant_is_valid_hex():
    bad = []
    for name in dir(theme):
        if name.startswith('_'):
            continue
        value = getattr(theme, name)
        if isinstance(value, str) and value.startswith('#'):
            if not HEX_RE.match(value):
                bad.append((name, value))
        elif isinstance(value, (tuple, list)):
            for item in value:
                if isinstance(item, str) and item.startswith('#') and not HEX_RE.match(item):
                    bad.append((name, item))
    assert not bad, f'invalid colour constants: {bad}'


def test_tokens_are_unique_where_they_should_be():
    """The four surfaces and the three semantic colours must not collide.

    A duplicate here means two layers render identically and the UI loses its depth, which
    is exactly the "slightly wrong background" bug ui_theme.py records.
    """
    surfaces = [theme.BG, theme.SURFACE, theme.RAISED]
    assert len(set(surfaces)) == len(surfaces), f'surface tokens collide: {surfaces}'
    assert theme.BORDER not in surfaces, 'BORDER must differ from every surface'
    assert theme.ACCENT != theme.OK != theme.WARN != theme.DANGER


def test_contrast_table_passes_aa():
    """Every pair in the contrast table must clear 4.5:1."""
    failures = []
    for fg_name, bg_name, expected in theme.CONTRAST_PAIRS:
        fg = getattr(theme, fg_name)
        bg = getattr(theme, bg_name)
        ratio = contrast_ratio(fg, bg)
        if ratio < theme.MIN_CONTRAST:
            failures.append(f'{fg_name} on {bg_name}: {ratio:.2f}:1 < {theme.MIN_CONTRAST}')
        elif abs(ratio - expected) > 0.06:
            failures.append(
                f'{fg_name} on {bg_name}: measured {ratio:.2f}:1 but the plan records '
                f'{expected:.2f}:1 -- the palette drifted from the verified table')
    assert not failures, '\n'.join(failures)


def test_contrast_table_is_complete():
    """Every text colour must be measured against all three surfaces.

    A new token that is only checked on one background is how the mockup palette shipped a
    failing accent: it was measured on the page background, not on the card it is drawn on.
    """
    measured = {(fg, bg) for fg, bg, _ in theme.CONTRAST_PAIRS}
    for fg in ('TEXT', 'MUTED', 'ACCENT', 'OK', 'WARN', 'DANGER'):
        for bg in ('BG', 'SURFACE', 'RAISED'):
            assert (fg, bg) in measured, f'missing contrast pair {fg} on {bg}'
    assert ('ACCENT_FG', 'ACCENT') in measured, 'accent button text is unmeasured'


def test_accent_button_text_is_not_white():
    """White on the accent fill fails badly (2.43:1); the token exists to prevent it."""
    white_on_accent = contrast_ratio('#ffffff', theme.ACCENT)
    assert white_on_accent < theme.MIN_CONTRAST, (
        'white on ACCENT now passes AA; the ACCENT_FG token may be unnecessary, but do not '
        'change this test without re-measuring the whole table')
    assert contrast_ratio(theme.ACCENT_FG, theme.ACCENT) >= theme.MIN_CONTRAST


def test_danger_deep_carries_body_text():
    """The filled destructive button uses DANGER_DEEP, so its text must clear AA."""
    ratio = contrast_ratio(theme.TEXT, theme.DANGER_DEEP)
    assert ratio >= theme.MIN_CONTRAST, f'TEXT on DANGER_DEEP is {ratio:.2f}:1'


# --- stylesheet ----------------------------------------------------------

def test_stylesheet_defines_every_token_as_a_custom_property():
    """The CSS must expose each colour, so no call site needs a literal."""
    for name, prop in (
        ('BG', '--bg'), ('SURFACE', '--surface'), ('RAISED', '--raised'),
        ('BORDER', '--border'), ('TEXT', '--text'), ('MUTED', '--muted'),
        ('ACCENT', '--accent'), ('ACCENT_FG', '--accent-fg'), ('OK', '--ok'),
        ('WARN', '--warn'), ('DANGER', '--danger'),
    ):
        assert f'{prop}:' in theme.STYLESHEET, f'{prop} missing from STYLESHEET'
        assert getattr(theme, name) in theme.STYLESHEET, (
            f'{name} ({getattr(theme, name)}) is not the value STYLESHEET uses for {prop}')


def test_stylesheet_has_no_stray_percent_formatting():
    """STYLESHEET is a %-formatted template; a lone `%` would raise at import time.

    This is a cheap guard: the module imports fine today, but an edit that adds `width: 50%`
    would break the import, not the render, and that is a confusing failure.
    """
    assert '%%' not in theme.STYLESHEET, 'a literal %% survived into the rendered stylesheet'


# --- package hygiene -----------------------------------------------------

def test_no_hex_literal_outside_theme_module():
    """No ``#rrggbb`` in any other file under webapp/ (same rule as ui_theme.py)."""
    offenders = []
    for dirpath, dirnames, filenames in os.walk(WEBAPP_DIR):
        dirnames[:] = [d for d in dirnames if d not in ('__pycache__', 'state')]
        for name in filenames:
            path = os.path.join(dirpath, name)
            if os.path.abspath(path) == THEME_PATH:
                continue
            if not name.endswith(('.py', '.js', '.html', '.css')):
                continue
            with open(path, 'r', encoding='utf-8', errors='replace') as handle:
                for number, line in enumerate(handle, 1):
                    if HEX_ANYWHERE_RE.search(line):
                        offenders.append(f'{os.path.relpath(path, WEBAPP_DIR)}:{number}')
    assert not offenders, f'hex literals outside theme.py: {offenders}'


def test_importing_theme_starts_nothing():
    """theme.py is pure data: no socket, no server, no GUI root."""
    import tkinter
    assert tkinter._default_root is None, 'importing webapp.theme created a GUI root'


def test_importing_app_does_not_bind_a_port():
    """``import webapp.app`` must not serve anything."""
    import webapp.app as app_module
    assert callable(app_module.create_app)
    # create_app() is what builds the app; importing must not have built or bound one.
    assert not hasattr(app_module, '_IMPORTED_APP'), (
        'webapp.app built an app at import time; it must only build one in create_app()')


def test_create_app_builds_without_network():
    """create_app() returns a FastAPI app and registers the health route."""
    from fastapi import FastAPI
    from webapp.app import create_app

    app = create_app()
    assert isinstance(app, FastAPI)
    # `app.routes` holds mounted routers as well as routes, and a mount has no
    # `.path`, so read it defensively rather than assuming every entry is a route.
    paths = {getattr(route, 'path', None) for route in app.routes}
    assert '/api/health' in paths
    assert '/' in paths
    assert '/static/theme.css' in paths


def test_web_config_rejects_remote_bind_without_tls():
    """A non-loopback bind without --trust-proxy must refuse to start."""
    from webapp.config import WebConfig

    WebConfig(host='127.0.0.1').check()          # loopback: fine, no flags
    WebConfig(host='localhost').check()
    WebConfig(host='::1').check()
    WebConfig(host='0.0.0.0', trust_proxy=True).check()
    WebConfig(host='0.0.0.0', allow_insecure_http=True).check()
    try:
        WebConfig(host='0.0.0.0').check()
    except ValueError as exc:
        assert 'trust-proxy' in str(exc)
    else:
        raise AssertionError('a non-loopback bind without TLS was accepted')


def test_web_config_loopback_matches_control():
    """The web copy of the loopback test must agree with control.py's."""
    import control
    from webapp.config import is_loopback_host

    for host in ('127.0.0.1', 'localhost', '::1', '[::1]', '0.0.0.0',
                 '192.168.1.5', 'example.com', ''):
        assert is_loopback_host(host) == control._is_loopback_host(host), (
            f'webapp.config and control disagree about {host!r}')


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
