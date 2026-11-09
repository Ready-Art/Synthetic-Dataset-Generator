# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Password hashing and the signed session cookie.

**Argon2id** for the password, **HMAC-SHA256** for the cookie. No JWT: for a single user a
signed cookie is sufficient, and it has no algorithm-confusion surface to get wrong. No
``passlib`` (unmaintained), no ``python-jose`` (CVE history), no ``pyjwt``.

What lives where
----------------
``webapp/state/auth.json`` (mode 0600, gitignored) holds exactly two secrets::

    {"password_hash": "$argon2id$...", "secret": "<64 hex chars>", "created_at": "..."}

The file is the only copy. It is never served, never logged, and never in a tracked file.

The cookie
----------
``v1.<base64url(payload)>.<base64url(hmac)>`` where the payload is ``{"iat": ..., "exp": ...}``.
``v1`` is a version tag so the format can change without accepting an old token. The HMAC covers
the version tag and the payload, so a tampered payload fails, and the comparison is
``hmac.compare_digest`` so it does not leak timing.

The cookie is ``HttpOnly``, ``SameSite=Lax``, ``Path=/`` and ``Secure`` **only when the request
actually arrived over HTTPS**. A ``Secure`` cookie on plain-HTTP loopback never sticks and the user
sees a login loop, so the flag is derived per request, never from a constant.
"""

import base64
import hashlib
import hmac
import json
import os
import secrets
import time

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError

# 12 hours. Long enough that a run can be watched all day without a re-login, short enough that
# a stolen cookie expires within a shift.
SESSION_TTL_SECONDS = 12 * 60 * 60

COOKIE_NAME = 'readyart_session'
COOKIE_VERSION = 'v1'

# argon2id with the library's defaults (m=64 MiB, t=3, p=4). Deliberately slow: ~50-100 ms per
# verification, which is fine on a login path and is the reason the password has a length cap.
_hasher = PasswordHasher()


class SessionError(ValueError):
    """A cookie that is missing, malformed, tampered with or expired."""


# --- password hashing ----------------------------------------------------

def hash_password(password):
    """Return an argon2id hash string for ``password``.

    The policy must have run first; this function does not re-check it, so the one place that
    decides what a weak password is stays ``policy.validate``.
    """
    return _hasher.hash(password)


def verify_password(password, stored_hash):
    """``True`` when ``password`` matches ``stored_hash``. Never raises on a bad hash."""
    if not stored_hash:
        return False
    try:
        return _hasher.verify(stored_hash, password)
    except (VerifyMismatchError, InvalidHashError, ValueError):
        return False


def needs_rehash(stored_hash):
    """True when the stored hash uses outdated argon2 parameters."""
    try:
        return _hasher.check_needs_rehash(stored_hash)
    except (InvalidHashError, ValueError):
        return True


# --- the auth file -------------------------------------------------------

def _b64encode(raw):
    return base64.urlsafe_b64encode(raw).decode('ascii').rstrip('=')


def _b64decode(text):
    padding = '=' * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def load_auth_file(path):
    """Read ``auth.json``. Returns ``None`` when it does not exist.

    A file that exists but cannot be parsed raises: silently treating a corrupt file as "no
    password set" would re-open first-run setup to anyone who can reach the server.
    """
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return None
    if not isinstance(data, dict) or not data.get('password_hash') or not data.get('secret'):
        raise SessionError(f'{path} is not a valid auth file.')
    return data


def save_auth_file(path, password_hash, secret=None):
    """Write ``auth.json`` atomically with mode 0600. Returns the secret used.

    Written to a temporary file in the same directory and then renamed, so a crash mid-write cannot
    leave a half-written file that the next start would read as corrupt.
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    secret = secret or secrets.token_hex(32)
    payload = {
        'password_hash': password_hash,
        'secret': secret,
        'created_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
    }
    temporary = f'{path}.tmp'
    # Create with 0600 from the start rather than chmod after: there is a window between
    # open() and chmod() in which the file is world-readable.
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
            json.dump(payload, handle, indent=2)
            handle.write('\n')
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return secret


def delete_auth_file(path):
    """Remove ``auth.json``, re-opening first-run setup. Requires filesystem access."""
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


# --- the signed cookie ---------------------------------------------------

def _sign(secret, message):
    return hmac.new(secret.encode('utf-8'), message.encode('utf-8'),
                    hashlib.sha256).digest()


def make_token(secret, issued_at=None, ttl=SESSION_TTL_SECONDS):
    """Build a signed session token.

    ``iat`` and ``exp`` are unix seconds. The payload is JSON so a reader can see why a token
    was rejected, and the signature covers the version tag so a downgrade cannot be forged.
    """
    issued = int(issued_at if issued_at is not None else time.time())
    payload = {'iat': issued, 'exp': issued + int(ttl)}
    body = _b64encode(json.dumps(payload, separators=(',', ':')).encode('utf-8'))
    message = f'{COOKIE_VERSION}.{body}'
    signature = _b64encode(_sign(secret, message))
    return f'{message}.{signature}'


def verify_token(token, secret, now=None):
    """Return the payload dict, or raise :class:`SessionError`.

    Order matters: the signature is checked **before** the payload is trusted for anything, and
    the comparison is constant-time.
    """
    if not token or not isinstance(token, str):
        raise SessionError('No session token.')
    parts = token.split('.')
    if len(parts) != 3:
        raise SessionError('Malformed session token.')
    version, body, signature = parts
    if version != COOKIE_VERSION:
        raise SessionError('Unsupported session token version.')
    expected = _b64encode(_sign(secret, f'{version}.{body}'))
    if not hmac.compare_digest(signature, expected):
        raise SessionError('Session token signature does not match.')
    try:
        payload = json.loads(_b64decode(body).decode('utf-8'))
    except (ValueError, UnicodeDecodeError):
        raise SessionError('Unreadable session token payload.')
    if not isinstance(payload, dict) or 'exp' not in payload:
        raise SessionError('Session token payload is incomplete.')
    current = now if now is not None else time.time()
    if int(payload['exp']) <= int(current):
        raise SessionError('Session token has expired.')
    return payload


def cookie_secure(request):
    """Whether the session cookie should carry ``Secure`` for THIS request.

    Derived from the actual scheme, never from a constant:

    * direct HTTPS -> ``Secure``;
    * behind a trusted proxy that says ``X-Forwarded-Proto: https`` -> ``Secure``;
    * plain HTTP on loopback -> **not** ``Secure``, because a ``Secure`` cookie over
      ``http://127.0.0.1`` is never stored and the user sees a login loop.

    ``X-Forwarded-Proto`` is only believed when ``--trust-proxy`` is set, because any client can
    send that header.
    """
    config = getattr(request.app.state, 'web_config', None)
    trust_proxy = bool(getattr(config, 'trust_proxy', False))
    if request.url.scheme == 'https':
        return True
    if trust_proxy:
        forwarded = request.headers.get('x-forwarded-proto', '')
        if forwarded.split(',')[0].strip().lower() == 'https':
            return True
    return False


def set_session_cookie(response, token, request):
    """Attach the session cookie with the flags the cookie policy requires."""
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=SESSION_TTL_SECONDS,
        httponly=True,
        samesite='lax',      # Strict breaks a reload after an external link
        secure=cookie_secure(request),
        path='/',
    )
    return response


def clear_session_cookie(response):
    """Clear the cookie server-side."""
    response.delete_cookie(COOKIE_NAME, path='/')
    return response
