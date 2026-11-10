# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Per-IP failed-login lockout.

**5 failed attempts per IP -> 5-minute lockout**, in memory. One process, one user, so a dict is
the whole data structure; there is no reason to reach for Redis.

Why per-IP and not global: a global counter lets anyone lock the real user out by failing five
times from anywhere. Per-IP means a locked client only locks itself.

The clock is injectable so the test can prove the window expires without sleeping for five minutes.
"""

import threading
import time

MAX_FAILURES = 5
LOCKOUT_SECONDS = 5 * 60

# Forget an IP entirely once its last failure is this old. Without this the dict grows forever on a
# public deployment, which is a slow memory leak on a long-running process.
FORGET_AFTER_SECONDS = 60 * 60


class RateLimiter:
    """A per-IP failed-attempt counter with a lockout window.

    Thread-safe: uvicorn serves requests on a thread pool, so two failed logins can arrive at once.
    """

    def __init__(self, max_failures=MAX_FAILURES, lockout_seconds=LOCKOUT_SECONDS,
                 forget_after=FORGET_AFTER_SECONDS, clock=None):
        self.max_failures = int(max_failures)
        self.lockout_seconds = float(lockout_seconds)
        self.forget_after = float(forget_after)
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        # ip -> {'failures': int, 'last': monotonic, 'locked_until': monotonic | None}
        self._entries = {}

    # --- internals -------------------------------------------------------

    def _entry(self, ip, now):
        entry = self._entries.get(ip)
        if entry is None:
            entry = {'failures': 0, 'last': now, 'locked_until': None}
            self._entries[ip] = entry
        return entry

    def _prune(self, now):
        """Drop entries that are old enough to be irrelevant."""
        stale = [ip for ip, entry in self._entries.items()
                 if not entry.get('locked_until')
                 and now - entry.get('last', now) > self.forget_after]
        for ip in stale:
            del self._entries[ip]

    # --- public ----------------------------------------------------------

    def is_locked(self, ip):
        """True when ``ip`` is inside its lockout window."""
        now = self._clock()
        with self._lock:
            entry = self._entries.get(ip)
            if entry is None:
                return False
            locked_until = entry.get('locked_until')
            if locked_until is None:
                return False
            if now >= locked_until:
                # The window has passed: clear it so the next attempt counts from zero.
                entry['locked_until'] = None
                entry['failures'] = 0
                entry['last'] = now
                return False
            return True

    def retry_after(self, ip):
        """Seconds until ``ip`` may try again, or 0 when it is not locked."""
        now = self._clock()
        with self._lock:
            entry = self._entries.get(ip)
            if entry is None or entry.get('locked_until') is None:
                return 0
            remaining = entry['locked_until'] - now
            return max(0, int(remaining + 0.999))

    def record_failure(self, ip):
        """Count one failed attempt. Returns the number of failures recorded."""
        now = self._clock()
        with self._lock:
            self._prune(now)
            entry = self._entry(ip, now)
            entry['failures'] += 1
            entry['last'] = now
            if entry['failures'] >= self.max_failures:
                entry['locked_until'] = now + self.lockout_seconds
            return entry['failures']

    def record_success(self, ip):
        """Clear the counter for ``ip`` after a successful login."""
        with self._lock:
            self._entries.pop(ip, None)

    def reset(self):
        """Forget every entry. Used by the test harness and by a password change."""
        with self._lock:
            self._entries.clear()

    def snapshot(self):
        """A copy of the current state, for a test or a diagnostics view."""
        with self._lock:
            return {ip: dict(entry) for ip, entry in self._entries.items()}
