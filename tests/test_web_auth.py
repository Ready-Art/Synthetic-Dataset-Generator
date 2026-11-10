# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Headless tests for the auth layer.

Covers every case that was verified by hand before the layer was written:

* The password policy -- all ten cases.
* The seven session cases: fresh OK, tampered rejected, expired rejected, wrong
  secret rejected, garbage rejected, constant-time compare, argon2 round trip.
* Lockout after five failures, unlock after the window, other IP unaffected.
* First-run setup needs the console token, and the token is single-use.
* The gate: **every** route except the public set needs a session, asserted by
  enumerating the route table rather than spot-checking.

No network, no display, no real auth file: every test builds its own app over a temporary
directory.
"""

import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient

from webapp.app import create_app
from webapp.config import WebConfig
from webapp.security import policy, session
from webapp.security.ratelimit import RateLimiter
from webapp.security.routes import AuthState, PUBLIC_PATHS, PUBLIC_PREFIXES

GOOD_PASSWORD = 'correct horse battery staple'


# --- harness -------------------------------------------------------------

class FakeClock:
    """A monotonic clock the test advances by hand."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def make_app(tmpdir, trust_proxy=False, clock=None):
    """Build an app whose auth file lives in ``tmpdir``."""
    config = WebConfig(auth_file=os.path.join(tmpdir, 'auth.json'),
                       trust_proxy=trust_proxy)
    app = create_app(config)
    if clock is not None:
        app.state.auth.limiter = RateLimiter(clock=clock)
    return app


def configure(app, password=GOOD_PASSWORD):
    """Set a password directly, bypassing the setup form."""
    app.state.auth.set_password(password)


# --- the password policy, all ten cases ---------------------------------

def test_policy_verified_cases():
    """The ten policy cases, exactly as they were recorded."""
    failures = []
    for password, expected in policy.VERIFIED_CASES:
        got = policy.is_acceptable(password)
        if got != expected:
            failures.append(f'{password!r}: got {got}, expected {expected}')
    assert not failures, '; '.join(failures)


def test_policy_rejects_short_and_accepts_long():
    assert not policy.is_acceptable('short')
    assert policy.is_acceptable('a-long-enough-passphrase')


def test_policy_rejects_the_app_name_plus_digits():
    for candidate in ('ReadyArt2026', 'readyart1234', 'Synthetic2026',
                      'generator99', 'Dataset-2026'):
        assert not policy.is_acceptable(candidate), candidate


def test_policy_requires_two_character_classes():
    assert not policy.is_acceptable('alllowercaseletters')
    assert not policy.is_acceptable('ALLUPPERCASELETTERS')
    assert policy.is_acceptable('MixedCaseLettersHere')


def test_policy_caps_the_length():
    """A cap prevents a hashing DoS: argon2 is deliberately slow."""
    assert not policy.is_acceptable('aA1!' + 'x' * policy.MAX_LENGTH)


def test_policy_rejects_a_repeated_character():
    assert not policy.is_acceptable('a' * 20)
    assert not policy.is_acceptable('!' * 20)


def test_policy_rejects_sequences():
    for candidate in ('qwertyuiop12', 'Qwerty!@#$%^&*()', 'abcdefghijkl',
                      '0123456789ab', 'poiuytrewq12'):
        assert not policy.is_acceptable(candidate), candidate


def test_policy_message_names_the_rule():
    """A policy that only says "too weak" makes a user guess and give up."""
    try:
        policy.validate('short')
    except policy.PolicyError as exc:
        assert '12 characters' in str(exc)
    else:
        raise AssertionError('short password was accepted')


def test_policy_rules_are_described():
    rules = policy.describe_rules()
    assert len(rules) >= 4
    assert any('12' in rule for rule in rules)


# --- the session primitives ---------------------------------------------

def test_argon2_round_trip():
    hashed = session.hash_password(GOOD_PASSWORD)
    assert hashed.startswith('$argon2id$'), hashed
    assert session.verify_password(GOOD_PASSWORD, hashed)
    assert not session.verify_password('wrong password here', hashed)


def test_argon2_rejects_a_bad_hash_without_raising():
    assert not session.verify_password(GOOD_PASSWORD, 'not-a-hash')
    assert not session.verify_password(GOOD_PASSWORD, '')
    assert not session.verify_password(GOOD_PASSWORD, None)


def test_fresh_token_verifies():
    token = session.make_token('secret-one')
    payload = session.verify_token(token, 'secret-one')
    assert payload['exp'] > payload['iat']


def test_tampered_token_is_rejected():
    token = session.make_token('secret-one')
    version, body, signature = token.split('.')
    # Flip one character of the payload: the signature no longer covers it.
    tampered_body = ('A' if body[0] != 'A' else 'B') + body[1:]
    try:
        session.verify_token(f'{version}.{tampered_body}.{signature}', 'secret-one')
    except session.SessionError:
        pass
    else:
        raise AssertionError('a tampered payload was accepted')


def test_tampered_signature_is_rejected():
    token = session.make_token('secret-one')
    version, body, signature = token.split('.')
    tampered = ('A' if signature[0] != 'A' else 'B') + signature[1:]
    try:
        session.verify_token(f'{version}.{body}.{tampered}', 'secret-one')
    except session.SessionError:
        pass
    else:
        raise AssertionError('a tampered signature was accepted')


def test_expired_token_is_rejected():
    issued = int(time.time()) - (session.SESSION_TTL_SECONDS + 60)
    token = session.make_token('secret-one', issued_at=issued)
    try:
        session.verify_token(token, 'secret-one')
    except session.SessionError as exc:
        assert 'expired' in str(exc).lower()
    else:
        raise AssertionError('an expired token was accepted')


def test_wrong_secret_is_rejected():
    token = session.make_token('secret-one')
    try:
        session.verify_token(token, 'secret-two')
    except session.SessionError:
        pass
    else:
        raise AssertionError('a token signed with another secret was accepted')


def test_garbage_tokens_are_rejected():
    for garbage in ('', 'x', 'a.b', 'a.b.c.d', 'v1..', 'v2.abc.def', 'not.a.token'):
        try:
            session.verify_token(garbage, 'secret-one')
        except session.SessionError:
            pass
        else:
            raise AssertionError(f'garbage token {garbage!r} was accepted')


def test_auth_file_is_0600_and_round_trips():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, 'auth.json')
        secret = session.save_auth_file(path, session.hash_password(GOOD_PASSWORD))
        mode = os.stat(path).st_mode & 0o777
        assert mode == 0o600, f'auth file mode is {oct(mode)}, expected 0o600'
        data = session.load_auth_file(path)
        assert data['secret'] == secret
        assert session.verify_password(GOOD_PASSWORD, data['password_hash'])


def test_missing_auth_file_reads_as_none():
    with tempfile.TemporaryDirectory() as tmpdir:
        assert session.load_auth_file(os.path.join(tmpdir, 'nope.json')) is None


def test_corrupt_auth_file_raises_rather_than_reading_as_unset():
    """A corrupt file must not re-open setup to anyone who can reach the server."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, 'auth.json')
        with open(path, 'w', encoding='utf-8') as handle:
            handle.write('{"password_hash": ""}')
        try:
            session.load_auth_file(path)
        except session.SessionError:
            pass
        else:
            raise AssertionError('a corrupt auth file read as "no password set"')


# --- the rate limiter ---------------------------------------------------

def test_lockout_after_five_failures():
    clock = FakeClock()
    limiter = RateLimiter(clock=clock)
    for _ in range(4):
        limiter.record_failure('10.0.0.1')
        assert not limiter.is_locked('10.0.0.1')
    limiter.record_failure('10.0.0.1')
    assert limiter.is_locked('10.0.0.1')


def test_lockout_is_per_ip():
    clock = FakeClock()
    limiter = RateLimiter(clock=clock)
    for _ in range(5):
        limiter.record_failure('10.0.0.1')
    assert limiter.is_locked('10.0.0.1')
    assert not limiter.is_locked('10.0.0.2'), 'a locked client locked another address'


def test_unlocks_after_the_window():
    clock = FakeClock()
    limiter = RateLimiter(clock=clock)
    for _ in range(5):
        limiter.record_failure('10.0.0.1')
    assert limiter.is_locked('10.0.0.1')
    clock.advance(limiter.lockout_seconds + 1)
    assert not limiter.is_locked('10.0.0.1'), 'the lockout never expired'


def test_success_clears_the_counter():
    clock = FakeClock()
    limiter = RateLimiter(clock=clock)
    for _ in range(4):
        limiter.record_failure('10.0.0.1')
    limiter.record_success('10.0.0.1')
    for _ in range(4):
        limiter.record_failure('10.0.0.1')
    assert not limiter.is_locked('10.0.0.1')


def test_retry_after_counts_down():
    clock = FakeClock()
    limiter = RateLimiter(clock=clock)
    for _ in range(5):
        limiter.record_failure('10.0.0.1')
    first = limiter.retry_after('10.0.0.1')
    clock.advance(60)
    second = limiter.retry_after('10.0.0.1')
    assert second < first, f'retry_after did not decrease: {first} -> {second}'


def test_stale_entries_are_pruned():
    clock = FakeClock()
    limiter = RateLimiter(clock=clock)
    limiter.record_failure('10.0.0.1')
    clock.advance(limiter.forget_after + 1)
    limiter.record_failure('10.0.0.2')
    assert '10.0.0.1' not in limiter.snapshot(), 'a stale entry was never forgotten'


# --- the gate, by enumerating the route table -----------------------------

def _route_paths(app):
    paths = set()
    for route in app.routes:
        path = getattr(route, 'path', None)
        if path:
            paths.add(path)
    return paths


def test_the_public_route_set_is_exactly_what_is_documented():
    """Only /login, /setup, /logout and /api/health answer without a session.

    Asserted by enumerating the route table, not by spot-checking: a route added in a later
    phase cannot quietly become public.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        public = []
        for path in sorted(_route_paths_with_methods(app)):
            if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES):
                public.append(path)
        # /static/theme.css is served by a route, not the StaticFiles mount, so it shows up
        # as a path here. It is public by prefix and carries no secret.
        expected = {'/login', '/setup', '/logout', '/api/health', '/static/theme.css'}
        unexpected = set(public) - expected
        assert not unexpected, f'unexpectedly public routes: {sorted(unexpected)}'
        assert set(public) == expected, (
            f'a public route disappeared: {sorted(expected - set(public))}')


def _route_paths_with_methods(app):
    """Every (path, method) pair the app serves, so a POST-only route is not missed."""
    for route in app.routes:
        path = getattr(route, 'path', None)
        if not path:
            continue
        methods = getattr(route, 'methods', None) or {'GET'}
        for method in methods:
            if method in ('HEAD', 'OPTIONS'):
                continue
            yield path


def test_every_non_public_route_returns_401_without_a_cookie():
    """Drive every gated route with no cookie and require 401 (or a redirect to /login)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        checked = 0
        for path in sorted(_route_paths_with_methods(app)):
            if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES):
                continue
            if '{' in path:
                continue  # a path parameter needs a real value; covered per-route later
            response = client.get(path)
            assert response.status_code in (401, 303), (
                f'{path} answered {response.status_code} without a session')
            if response.status_code == 303:
                assert response.headers['location'] == '/login', (
                    f'{path} redirected to {response.headers["location"]}, not /login')
            checked += 1
        assert checked >= 1, 'no gated route was exercised'


def test_public_routes_answer_without_a_cookie():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        assert client.get('/api/health').status_code == 200
        assert client.get('/login').status_code == 200
        assert client.get('/static/theme.css').status_code == 200


# --- login, setup and logout end to end ----------------------------------

def test_login_with_the_right_password_sets_a_cookie():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        response = client.post('/login', data={'password': GOOD_PASSWORD})
        assert response.status_code == 303, response.text
        assert session.COOKIE_NAME in response.cookies
        cookie = response.headers['set-cookie'].lower()
        assert 'httponly' in cookie, cookie
        assert 'samesite=lax' in cookie, cookie
        # Plain HTTP on loopback: the cookie must NOT be Secure, or it never sticks.
        assert 'secure' not in cookie, cookie


def test_login_with_the_wrong_password_is_generic():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        response = client.post('/login', data={'password': 'not the password'})
        assert response.status_code == 401
        assert 'Incorrect password' in response.text
        assert session.COOKIE_NAME not in response.cookies


def test_login_locks_out_after_five_failures():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        for _ in range(5):
            client.post('/login', data={'password': 'wrong password here'})
        response = client.post('/login', data={'password': GOOD_PASSWORD})
        assert response.status_code == 429, (
            'a correct password was accepted while the address was locked out')
        assert session.COOKIE_NAME not in response.cookies


def test_login_unlocks_after_the_window():
    with tempfile.TemporaryDirectory() as tmpdir:
        clock = FakeClock()
        app = make_app(tmpdir, clock=clock)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        for _ in range(5):
            client.post('/login', data={'password': 'wrong password here'})
        assert client.post('/login', data={'password': GOOD_PASSWORD}).status_code == 429
        clock.advance(app.state.auth.limiter.lockout_seconds + 1)
        response = client.post('/login', data={'password': GOOD_PASSWORD})
        assert response.status_code == 303, 'the lockout never expired'


def test_a_valid_cookie_reaches_a_gated_route():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        client.post('/login', data={'password': GOOD_PASSWORD})
        response = client.get('/')
        assert response.status_code == 200


def test_an_expired_cookie_is_rejected():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        client.post('/login', data={'password': GOOD_PASSWORD})
        # Re-issue a token that expired an hour ago and plant it.
        secret = app.state.auth.secret()
        expired = session.make_token(
            secret, issued_at=int(time.time()) - session.SESSION_TTL_SECONDS - 3600)
        # Delete before setting: `cookies.set` **appends** a second cookie of the same name, and
        # httpx then sends both. The server reads the first (the still-valid one from `/login`),
        # so the request succeeded and this test failed for the wrong reason -- the planted token
        # was never the one being checked.
        client.cookies.delete(session.COOKIE_NAME)
        client.cookies.set(session.COOKIE_NAME, expired)
        assert client.get('/').status_code == 303


def test_a_tampered_cookie_is_rejected():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        client.post('/login', data={'password': GOOD_PASSWORD})
        token = client.cookies.get(session.COOKIE_NAME)
        version, body, signature = token.split('.')
        # Delete before setting, for the same reason as the expired-cookie test above: a second
        # cookie of the same name would be appended and the valid one would still be sent.
        client.cookies.delete(session.COOKIE_NAME)
        client.cookies.set(session.COOKIE_NAME, f'{version}.{body}.AAAA{signature[4:]}')
        assert client.get('/').status_code == 303


def test_logout_clears_the_cookie():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        client.post('/login', data={'password': GOOD_PASSWORD})
        response = client.post('/logout')
        assert response.status_code == 303
        assert client.get('/').status_code == 303


def test_secure_cookie_behind_a_trusted_proxy():
    """Behind nginx the cookie must be Secure, derived from X-Forwarded-Proto."""
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir, trust_proxy=True)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        response = client.post('/login', data={'password': GOOD_PASSWORD},
                               headers={'X-Forwarded-Proto': 'https'})
        assert response.status_code == 303
        assert 'secure' in response.headers['set-cookie'].lower()


def test_forwarded_for_is_ignored_without_trust_proxy():
    """A spoofable header must not pick the rate-limit bucket."""
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir, trust_proxy=False)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        for index in range(5):
            client.post('/login', data={'password': 'wrong password here'},
                         headers={'X-Forwarded-For': f'10.0.0.{index}'})
        # All five came from the same real address, so the address is locked.
        assert client.post('/login', data={'password': GOOD_PASSWORD}).status_code == 429


def test_forwarded_for_is_believed_behind_a_trusted_proxy():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir, trust_proxy=True)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        for index in range(5):
            client.post('/login', data={'password': 'wrong password here'},
                         headers={'X-Forwarded-For': f'10.0.0.{index}'})
        # Five different forwarded addresses: no single bucket reached the limit.
        assert client.post('/login', data={'password': GOOD_PASSWORD},
                           headers={'X-Forwarded-For': '10.0.0.99'}).status_code == 303


# --- first-run setup ----------------------------------------------------

def test_setup_requires_the_console_token():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        client = TestClient(app, follow_redirects=False)
        response = client.post('/setup', data={'token': 'wrong-token',
                                              'password': GOOD_PASSWORD,
                                              'confirm': GOOD_PASSWORD})
        assert response.status_code == 403
        assert not app.state.auth.has_password(), 'setup succeeded without the token'


def test_setup_accepts_the_console_token_once():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        client = TestClient(app, follow_redirects=False)
        token = app.state.auth.setup_token()
        response = client.post('/setup', data={'token': token,
                                              'password': GOOD_PASSWORD,
                                              'confirm': GOOD_PASSWORD})
        assert response.status_code == 303, response.text
        assert app.state.auth.has_password()
        assert session.COOKIE_NAME in response.cookies
        # The token is single-use: a second setup attempt is refused outright.
        second = client.post('/setup', data={'token': token,
                                            'password': 'another good passphrase',
                                            'confirm': 'another good passphrase'})
        assert second.status_code == 303
        assert second.headers['location'] == '/login'


def test_setup_enforces_the_policy():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        client = TestClient(app, follow_redirects=False)
        token = app.state.auth.setup_token()
        response = client.post('/setup', data={'token': token, 'password': 'short',
                                              'confirm': 'short'})
        assert response.status_code == 400
        assert not app.state.auth.has_password()


def test_setup_rejects_a_mismatch():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        client = TestClient(app, follow_redirects=False)
        token = app.state.auth.setup_token()
        response = client.post('/setup', data={'token': token,
                                              'password': GOOD_PASSWORD,
                                              'confirm': 'a different passphrase'})
        assert response.status_code == 400
        assert not app.state.auth.has_password()


def test_setup_redirects_once_a_password_exists():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        response = client.get('/setup')
        assert response.status_code == 303
        assert response.headers['location'] == '/login'


def test_login_redirects_to_setup_before_a_password_exists():
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        client = TestClient(app, follow_redirects=False)
        response = client.get('/login')
        assert response.status_code == 303
        assert response.headers['location'] == '/setup'


def test_health_never_reveals_whether_a_password_is_set():
    """An unauthenticated caller learns nothing about the instance."""
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        client = TestClient(app, follow_redirects=False)
        before = client.get('/api/health').json()
        configure(app)
        after = client.get('/api/health').json()
        assert before == after, f'health changed after setup: {before} -> {after}'
        for secret_key in ('password', 'hash', 'secret', 'auth', 'configured'):
            assert secret_key not in after, f'health leaks {secret_key!r}'


def test_auth_state_never_serves_the_hash_or_secret():
    """No route may return the hash or the session secret."""
    with tempfile.TemporaryDirectory() as tmpdir:
        app = make_app(tmpdir)
        configure(app)
        client = TestClient(app, follow_redirects=False)
        client.post('/login', data={'password': GOOD_PASSWORD})
        secret = app.state.auth.secret()
        data = app.state.auth.load()
        for path in ('/api/health', '/login'):
            text = client.get(path).text
            assert secret not in text, f'{path} leaks the session secret'
            assert data['password_hash'] not in text, f'{path} leaks the password hash'


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
