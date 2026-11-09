# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Authentication: one password, no username, no multi-user.

Four small modules, each testable on its own with no server:

``policy``
    The password policy. Runs when a password is **set**, so a weak one can never be
    stored -- not only when someone tries to log in with one.

``session``
    Argon2id hashing plus an HMAC-SHA256 signed cookie. No JWT: for a single user a signed
    cookie is sufficient and has no algorithm-confusion surface.

``ratelimit``
    Per-IP failed-attempt lockout, in memory (one process, one user).

``routes``
    ``/login``, ``/setup``, ``/logout`` and the middleware that gates every other route.

No new dependency is needed: ``argon2-cffi`` is already installed and is the only import.
Deliberately **not** used: ``passlib`` (unmaintained), ``python-jose`` (CVE history),
``pyjwt`` (unnecessary).
"""

from .routes import install_auth

__all__ = ['install_auth']
