# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Runtime configuration for the web front end.

Pure data plus a couple of pure helpers: importing this module must not bind a socket,
start a server or read a secret off disk. ``webapp.__main__`` turns a :class:`WebConfig`
into a uvicorn process.

Two supported deployment shapes, and **neither is a degraded
mode**:

``local``
    ``python -m webapp`` -> ``http://127.0.0.1:8765``. No TLS, no nginx, no proxy
    headers, no warnings. This is the zero-config default.

``proxied``
    Bind loopback, let nginx terminate TLS and forward. ``--trust-proxy`` is required
    before ``X-Forwarded-For`` / ``X-Forwarded-Proto`` are believed, because both are
    client-spoofable headers.

The bind address defaults to loopback for the same reason ``control.py`` refuses a
non-loopback control listener: this process can clear a database, so it must not be
reachable from the network by accident.
"""

import ipaddress
import os

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 8765

# Where the session secret and the password hash live. Gitignored, mode 0600.
STATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'state')
AUTH_FILE = os.path.join(STATE_DIR, 'auth.json')

# The control socket the web app owns. Same default as ``control.default_socket_path()``;
# resolved lazily there so a ``control.socket_path`` override in the config still wins.
SOCKET_NAME = 'generator-control.sock'


def is_loopback_host(host):
    """True when ``host`` names this machine's loopback interface only.

    Deliberately a copy of ``control._is_loopback_host`` rather than an import: the web
    package must stay importable without pulling in the engine's dependency graph (which
    reaches ``generation.py`` and ``requests``). The rule is four lines and is pinned by a
    test that compares the two implementations.
    """
    if not host:
        return False
    name = str(host).strip().lower()
    if name in ('localhost', '::1', '[::1]'):
        return True
    try:
        return ipaddress.ip_address(name.strip('[]')).is_loopback
    except ValueError:
        return False


class WebConfig:
    """Everything ``python -m webapp`` needs to start, and nothing else.

    A plain object rather than a dataclass so it can be built from an ``argparse``
    namespace with one call and printed in a log line without importing ``dataclasses``.
    """

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, config_path=None,
                 socket_path=None, trust_proxy=False, allow_insecure_http=False,
                 open_browser=False, log_level='info', auth_file=None):
        self.host = host or DEFAULT_HOST
        self.port = int(port or DEFAULT_PORT)
        self.config_path = config_path
        self.socket_path = socket_path
        # Overridable so a test can point at a temporary directory and so a deployment can
        # keep the secret outside the checkout. Defaults to webapp/state/auth.json.
        self.auth_file = auth_file or AUTH_FILE
        self.trust_proxy = bool(trust_proxy)
        self.allow_insecure_http = bool(allow_insecure_http)
        self.open_browser = bool(open_browser)
        self.log_level = log_level or 'info'

    # --- derived ---------------------------------------------------------

    @property
    def loopback_only(self):
        return is_loopback_host(self.host)

    @property
    def mode(self):
        """``'local'`` or ``'proxied'`` -- for a log line and the UI's own footer."""
        return 'proxied' if self.trust_proxy else 'local'

    def check(self):
        """Validate the combination, raising ``ValueError`` with a human message.

        The one rule that matters: a password must never cross a plain-HTTP connection to a
        non-loopback address. A remote bind without ``--trust-proxy`` is almost certainly a
        mistake, and without TLS the session cookie would be sniffable, so refuse unless the
        operator says ``--allow-insecure-http`` out loud.
        """
        if self.port < 1 or self.port > 65535:
            raise ValueError(f'Port {self.port} is out of range (1-65535).')
        if not self.loopback_only and not self.trust_proxy and not self.allow_insecure_http:
            raise ValueError(
                f'Refusing to bind {self.host}:{self.port}. A non-loopback bind serves a '
                f'password over the network, so it needs TLS in front of it: pass '
                f'--trust-proxy when nginx terminates HTTPS, or --allow-insecure-http to '
                f'accept plain HTTP on purpose.')
        return self

    def url(self):
        scheme = 'https' if self.trust_proxy else 'http'
        host = self.host
        if ':' in host and not host.startswith('['):
            host = f'[{host}]'
        return f'{scheme}://{host}:{self.port}/'

    def __repr__(self):
        return (f'WebConfig(host={self.host!r}, port={self.port}, mode={self.mode!r}, '
                f'trust_proxy={self.trust_proxy})')
