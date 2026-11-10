# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""The auth routes and the gate in front of every other route.

Four routes are public, and **nothing else is**:

``GET /login``, ``POST /login``
    The password form. ``POST`` sets the session cookie on success.
``GET /setup``, ``POST /setup``
    First-run only, and only with the console token.
``POST /logout``
    Clears the cookie.
``GET /api/health``
    Liveness probe. Says nothing about the run or about whether a password is set.
``/static/*``
    The stylesheet and the ES modules. No secrets, and the login page needs them.

Everything else -- including every ``/api/*`` route -- is gated by
:class:`SessionGate`, which is a middleware rather than a per-route dependency so a route added
later cannot forget to declare it. ``tests/test_web_auth.py`` enumerates ``app.routes`` and asserts
the public set is exactly the list above.

First-run setup
---------------
With no ``auth.json``, the server generates a one-time token and **prints it to the console**. It is
never served over HTTP, so an unauthenticated visitor cannot claim the instance: they would have to read
the server's terminal. The token is invalidated after use, and setup cannot be re-run without deleting
``auth.json`` on the host.
"""

import secrets
import sys

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.requests import HTTPConnection

from ..config import AUTH_FILE
from . import policy, session
from .ratelimit import RateLimiter

# The exact set of paths reachable without a session. Anything not here needs one.
PUBLIC_PATHS = frozenset({
    '/login',
    '/setup',
    '/logout',
    '/api/health',
})

# Static assets are public by prefix: the login page cannot render without the stylesheet.
PUBLIC_PREFIXES = ('/static/',)

# The generic message a failed login gets. It must not distinguish "wrong password" from
# "no password set".
GENERIC_LOGIN_ERROR = 'Incorrect password.'


class AuthState:
    """Everything the auth layer needs, built once per app.

    Held on ``app.state.auth`` so the middleware and the routes share one instance without a
    module-level global (which would leak between two apps in one test process).
    """

    def __init__(self, auth_file=None, limiter=None, clock=None):
        # Resolved at construction time, not at import time, so a test can point it at a
        # temporary directory and ``--auth-file`` can override it.
        self.auth_file = auth_file or AUTH_FILE
        self.limiter = limiter or RateLimiter()
        self.clock = clock
        self._setup_token = None

    # --- the stored secret ----------------------------------------------

    def load(self):
        """The auth file contents, or ``None`` when no password is set yet."""
        return session.load_auth_file(self.auth_file)

    def has_password(self):
        try:
            return self.load() is not None
        except session.SessionError:
            # A corrupt file must NOT read as "no password set": that would re-open setup.
            return True

    def secret(self):
        data = self.load()
        return data['secret'] if data else None

    def set_password(self, password):
        """Validate against the policy, then store the hash. Raises ``PolicyError``."""
        policy.validate(password)
        existing = None
        try:
            existing = self.load()
        except session.SessionError:
            existing = None
        secret = existing['secret'] if existing else None
        return session.save_auth_file(self.auth_file, session.hash_password(password),
                                      secret=secret)

    # --- the one-time setup token ---------------------------------------

    def setup_token(self):
        """The token printed at startup, generating it on first call."""
        if self._setup_token is None:
            self._setup_token = secrets.token_urlsafe(24)
        return self._setup_token

    def check_setup_token(self, candidate):
        """Constant-time compare against the console token."""
        expected = self._setup_token
        if not expected or not candidate:
            return False
        return secrets.compare_digest(str(candidate), expected)

    def consume_setup_token(self):
        self._setup_token = None

    # --- session --------------------------------------------------------

    def issue_token(self):
        secret = self.secret()
        if not secret:
            raise session.SessionError('No password is set.')
        return session.make_token(secret)

    def read_session(self, request):
        """The session payload, or ``None`` when the request carries no valid session."""
        raw = request.cookies.get(session.COOKIE_NAME)
        if not raw:
            return None
        secret = self.secret()
        if not secret:
            return None
        try:
            return session.verify_token(raw, secret)
        except session.SessionError:
            return None


def client_ip(request, trust_proxy):
    """The address the rate limiter keys on.

    ``X-Forwarded-For`` is client-spoofable, so it is believed **only** behind
    ``--trust-proxy``. Behind nginx every request arrives from ``127.0.0.1``, which
    would make one lockout for everyone; without a proxy, believing the header would let a client
    pick its own bucket and dodge the limiter.
    """
    if trust_proxy:
        forwarded = request.headers.get('x-forwarded-for', '')
        if forwarded:
            return forwarded.split(',')[0].strip()
        real = request.headers.get('x-real-ip', '')
        if real:
            return real.strip()
    return request.client.host if request.client else 'unknown'


class SessionGateMiddleware:
    """The gate, as a pure ASGI middleware so it covers HTTP **and** WebSocket.

    ``@app.middleware('http')`` only sees HTTP requests, so a WebSocket handshake would
    bypass it and the live stats stream would be readable by anyone who can reach the port.
    A pure ASGI middleware sees both scopes, so a route added later -- on either protocol --
    cannot quietly become public.

    Public paths are ``PUBLIC_PATHS`` plus ``PUBLIC_PREFIXES``; everything else needs a
    valid session cookie. An unauthenticated ``/api/*`` request gets a JSON 401, a browser
    navigation gets a 303 to ``/login``, and a WebSocket handshake is closed with 1008.
    """

    def __init__(self, app, state):
        self.app = app
        self.state = state

    async def __call__(self, scope, receive, send):
        if scope['type'] not in ('http', 'websocket'):
            await self.app(scope, receive, send)
            return
        path = scope.get('path', '')
        if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES):
            await self.app(scope, receive, send)
            return

        # HTTPConnection, not Request: Request asserts scope['type'] == 'http', and this
        # middleware also sees WebSocket handshakes.
        connection = HTTPConnection(scope, receive=receive)
        if self.state.read_session(connection) is not None:
            await self.app(scope, receive, send)
            return

        if scope['type'] == 'websocket':
            # Close before accepting: the browser sees a refused handshake, which it
            # treats as "not signed in" rather than as a stream that broke.
            await send({'type': 'websocket.close', 'code': 1008})
            return

        if path.startswith('/api/'):
            response = JSONResponse(
                {'ok': False,
                 'error': {'code': 'unauthenticated',
                           'message': 'A session is required.'}},
                status_code=401)
        else:
            response = RedirectResponse('/login', status_code=303)
        await response(scope, receive, send)


def install_auth(app, config):
    """Attach the auth state, the gate middleware and the four public routes."""
    state = AuthState(auth_file=getattr(config, 'auth_file', None))
    app.state.auth = state

    trust_proxy = bool(getattr(config, 'trust_proxy', False))

    # --- the gate -------------------------------------------------------

    # A pure ASGI middleware, not `@app.middleware('http')`: the decorator form only
    # sees HTTP requests, so a WebSocket handshake would bypass the gate entirely and the
    # live stats stream would be readable by anyone who can reach the port. One class
    # covers both scopes, which is the only way to be sure a second protocol is not a
    # second hole.
    app.add_middleware(SessionGateMiddleware, state=state)

    # --- login ----------------------------------------------------------

    @app.get('/login', response_class=HTMLResponse, include_in_schema=False)
    def login_page(request: Request):
        if not state.has_password():
            return RedirectResponse('/setup', status_code=303)
        return HTMLResponse(_login_html(error='', status=200))

    @app.post('/login', include_in_schema=False)
    async def login_submit(request: Request):
        if not state.has_password():
            return RedirectResponse('/setup', status_code=303)
        ip = client_ip(request, trust_proxy)
        if state.limiter.is_locked(ip):
            seconds = state.limiter.retry_after(ip)
            return HTMLResponse(
                _login_html(error=f'Too many failed attempts. Try again in '
                                  f'{_humanise(seconds)}.',
                            status=429),
                status_code=429)
        form = await request.form()
        password = str(form.get('password', ''))
        data = state.load()
        if data and session.verify_password(password, data['password_hash']):
            state.limiter.record_success(ip)
            response = RedirectResponse('/', status_code=303)
            return session.set_session_cookie(response, state.issue_token(), request)
        state.limiter.record_failure(ip)
        remaining = max(0, state.limiter.max_failures - _failures(state, ip))
        message = GENERIC_LOGIN_ERROR
        if state.limiter.is_locked(ip):
            message = (f'Too many failed attempts. Try again in '
                       f'{_humanise(state.limiter.retry_after(ip))}.')
        elif remaining:
            message = f'{GENERIC_LOGIN_ERROR} {remaining} attempt(s) left.'
        return HTMLResponse(_login_html(error=message, status=401), status_code=401)

    # --- setup ----------------------------------------------------------

    @app.get('/setup', response_class=HTMLResponse, include_in_schema=False)
    def setup_page(request: Request):
        if state.has_password():
            return RedirectResponse('/login', status_code=303)
        return HTMLResponse(_setup_html(error='', status=200))

    @app.post('/setup', include_in_schema=False)
    async def setup_submit(request: Request):
        if state.has_password():
            return RedirectResponse('/login', status_code=303)
        form = await request.form()
        token = str(form.get('token', ''))
        password = str(form.get('password', ''))
        confirm = str(form.get('confirm', ''))
        if not state.check_setup_token(token):
            return HTMLResponse(
                _setup_html(error='That setup token is not correct. It was printed in the '
                                  'terminal that started the server.', status=403),
                status_code=403)
        if password != confirm:
            return HTMLResponse(_setup_html(error='The two passwords do not match.'),
                                status_code=400)
        try:
            policy.validate(password)
        except policy.PolicyError as exc:
            return HTMLResponse(_setup_html(error=str(exc)), status_code=400)
        state.set_password(password)
        state.consume_setup_token()
        response = RedirectResponse('/', status_code=303)
        return session.set_session_cookie(response, state.issue_token(), request)

    # --- logout ---------------------------------------------------------

    @app.post('/logout', include_in_schema=False)
    def logout():
        response = RedirectResponse('/login', status_code=303)
        return session.clear_session_cookie(response)

    return app


def _failures(state, ip):
    entry = state.limiter.snapshot().get(ip) or {}
    return int(entry.get('failures', 0))


def _humanise(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f'{seconds} second{"s" if seconds != 1 else ""}'
    minutes = (seconds + 59) // 60
    return f'{minutes} minute{"s" if minutes != 1 else ""}'


# --- pages ---------------------------------------------------------------
# Inline HTML, not templates: there are two pages, they share the stylesheet, and a template
# engine would be a dependency for no benefit. Every colour comes from the stylesheet's custom
# properties, so no hex literal appears here.

def _page(title, body, status):
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} &middot; ReadyArt</title>
<link rel="stylesheet" href="/static/theme.css">
</head>
<body>
<div class="login-wrap">
  <div class="login-card">
    <h1>{title}</h1>
{body}
  </div>
</div>
</body>
</html>
"""


def _login_html(error, status=200):
    return _page('Sign in', f"""    <p class="muted" style="margin:0 0 16px">Enter the password for this instance.</p>
    <p class="login-error" id="login-error">{error}</p>
    <form method="post" action="/login">
      <label class="card-caption" for="password">Password</label>
      <input type="password" id="password" name="password" autocomplete="current-password"
             autofocus required>
      <button class="btn-primary" type="submit"
              style="width:100%;margin-top:14px">Sign in</button>
    </form>
    <p class="login-hint">5 failed attempts lock this address for 5 minutes.</p>""", status)


def _setup_html(error, status=200):
    rules = ''.join(f'<li>{rule}</li>' for rule in policy.describe_rules())
    return _page('First-run setup', f"""    <p class="muted" style="margin:0 0 16px">
      No password is set yet. The setup token was printed in the terminal that
      started the server.</p>
    <p class="login-error">{error}</p>
    <form method="post" action="/setup">
      <label class="card-caption" for="token">Setup token</label>
      <input type="text" id="token" name="token" autocomplete="off" required>
      <label class="card-caption" for="password"
             style="display:block;margin-top:12px">Password</label>
      <input type="password" id="password" name="password"
             autocomplete="new-password" required>
      <label class="card-caption" for="confirm"
             style="display:block;margin-top:12px">Confirm password</label>
      <input type="password" id="confirm" name="confirm"
             autocomplete="new-password" required>
      <button class="btn-primary" type="submit"
              style="width:100%;margin-top:16px">Set password</button>
    </form>
    <ul class="login-hint">{rules}</ul>""", status)


def announce_setup_token(app):
    """Print the one-time setup token to the console.

    Called from ``webapp.__main__`` after the app is built and before uvicorn starts. It is
    **never** served over HTTP: that is what stops an unauthenticated visitor claiming the
    instance.
    """
    state = getattr(app.state, 'auth', None)
    if state is None or state.has_password():
        return None
    token = state.setup_token()
    banner = (
        '\n' + '=' * 72 + '\n'
        '  FIRST-RUN SETUP\n'
        '  No password is set. Open the web UI and enter this token:\n\n'
        f'      {token}\n\n'
        '  This token is shown once and is invalidated after use.\n'
        + '=' * 72 + '\n'
    )
    print(banner, file=sys.stderr, flush=True)
    return token
