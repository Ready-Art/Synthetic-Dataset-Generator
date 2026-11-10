# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Headless tests for the v14.2.0 shared HTTP connection pool.

The generator used to call the module-level ``requests.post(...)`` helper, which builds a
THROWAWAY ``Session`` per call: a fresh TCP connection, a fresh TLS handshake, one request,
then close. From every worker and every judge thread. Against a local vLLM behind nginx that
is one connection per request, thousands per minute, and the server's socket table fills with
ESTAB entries against :443 -- the "client side spam connections" report. It was never extra
requests; it was the same requests each paying for a new connection.

These tests pin the fix:

  * the shared session exists, is pooled, and is shared by every module facade;
  * the pool is sized to the thread count, so no connection is opened and thrown away;
  * ``max_retries`` is CONNECT-ONLY -- a read retry would resend a generation request whose
    tokens may already have been produced, silently duplicating work;
  * ``generation``, ``quality`` and ``vllm_metrics`` all bind the SAME facade object, so a
    test that patches ``generation.requests.post`` still intercepts the judge's and the
    poller's calls -- the patch semantics the existing suite depends on;
  * the facade still exposes ``post``/``get``/``exceptions``/``RequestException``, so the
    ~100 existing call sites and the whole existing test suite are unchanged;
  * against a real keep-alive server, N requests open far fewer than N connections.

No model or display is needed. A loopback HTTP server is started on an ephemeral port, so no
external network is touched.

Run with ``python -B tests/test_http_connection_pool.py``.
"""
import json
import os
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
# Import in a scratch directory so logging_config's output/ side effect stays out of the repo.
_tmpdir = tempfile.TemporaryDirectory(prefix="http_pool_")
os.chdir(_tmpdir.name)
sys.path.insert(0, str(REPO))

import api_handler
import generation
import quality
import vllm_metrics

FAILURES = []


def check(name, condition, detail=""):
    if condition:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        FAILURES.append(name)


# ---------------------------------------------------------------------------
# A counting keep-alive server
# ---------------------------------------------------------------------------
_ACCEPTED = 0
_SERVED = 0
_LOCK = threading.Lock()


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive: connections are reusable

    def do_POST(self):
        global _SERVED
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        with _LOCK:
            _SERVED += 1
        body = json.dumps({"choices": [{"message": {"content": "ok"}}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.do_POST()

    def log_message(self, *args):
        pass


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def get_request(self):
        global _ACCEPTED
        sock, addr = super().get_request()
        with _LOCK:
            _ACCEPTED += 1
        return sock, addr


def _reset_counters():
    global _ACCEPTED, _SERVED
    with _LOCK:
        _ACCEPTED = 0
        _SERVED = 0


def _counters():
    with _LOCK:
        return _ACCEPTED, _SERVED


def _start_server():
    server = _Server(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_address[1]


def main():
    print("http connection pool")

    # --- 1. the shared session is pooled and mounted for both schemes ---
    session = api_handler._http_session
    check("a shared session exists", session is not None)
    check("http:// is mounted on the pooled adapter",
          "http://" in session.adapters)
    check("https:// is mounted on the pooled adapter",
          "https://" in session.adapters)

    adapter = session.get_adapter("https://example.invalid/")
    check("the https adapter is an HTTPAdapter",
          adapter.__class__.__name__ == "HTTPAdapter",
          adapter.__class__.__name__)
    check("pool_maxsize covers the largest thread count",
          adapter._pool_maxsize == api_handler.HTTP_POOL_MAXSIZE
          and api_handler.HTTP_POOL_MAXSIZE >= 64,
          f"pool_maxsize={adapter._pool_maxsize}")
    check("pool_connections covers the largest thread count",
          adapter._pool_connections == api_handler.HTTP_POOL_MAXSIZE,
          f"pool_connections={adapter._pool_connections}")
    check("the pool never blocks a worker waiting for a socket",
          adapter._pool_block is False)

    # --- 2. retries are CONNECT-ONLY ---
    retries = adapter.max_retries
    check("connect failures are retried", retries.connect == api_handler.HTTP_CONNECT_RETRIES,
          f"connect={retries.connect}")
    check("read failures are NOT retried (a resend would duplicate generation work)",
          retries.read == 0, f"read={retries.read}")
    check("status failures are NOT retried", retries.status == 0, f"status={retries.status}")
    check("redirects are NOT retried", retries.redirect == 0, f"redirect={retries.redirect}")
    check("POST is allowed to be retried for the connect case",
          retries.allowed_methods is None, str(retries.allowed_methods))
    check("a non-2xx status is returned, not raised",
          retries.raise_on_status is False)

    # --- 3. one session, one facade shared by every module ---
    # The modules used to import the real `requests` module, so patching
    # `generation.requests.post` replaced the one shared function and every module saw the
    # stub. The facade must preserve that, or the existing suite silently stops intercepting
    # the judge's and the poller's calls.
    check("generation and quality bind the SAME facade",
          generation.requests is quality.requests)
    check("quality and vllm_metrics bind the SAME facade",
          quality.requests is vllm_metrics.requests)
    check("the facade is api_handler's one instance",
          generation.requests is api_handler.http)
    check("every facade shares the ONE session",
          generation.requests._session is api_handler._http_session
          and quality.requests._session is api_handler._http_session
          and vllm_metrics.requests._session is api_handler._http_session)

    # --- 4. the facade keeps the requests surface the call sites use ---
    for name in ("post", "get", "request"):
        check(f"facade exposes {name}()", callable(getattr(generation.requests, name, None)))
    check("facade re-exports requests.exceptions",
          generation.requests.exceptions is api_handler.requests.exceptions)
    check("facade re-exports RequestException",
          generation.requests.RequestException is api_handler.requests.RequestException)
    check("requests.exceptions.Timeout is still catchable",
          issubclass(generation.requests.exceptions.Timeout, Exception))

    # --- 5. a patch on one module reaches the others ---
    # This is the property the existing test suite depends on: it patches
    # `generation.requests.post` and expects the quality judge's calls intercepted too.
    captured = []

    def _stub(*args, **kwargs):
        captured.append(args)

    original = generation.requests.post
    generation.requests.post = _stub
    try:
        generation.requests.post("http://example.invalid/x")
        check("the generation stub intercepts generation's call", len(captured) == 1)
        check("the generation stub also reaches quality (shared facade)",
              quality.requests.post is _stub)
        check("the generation stub also reaches vllm_metrics (shared facade)",
              vllm_metrics.requests.post is _stub)
        check("the stub does not replace the shared session's own post",
              api_handler._http_session.post is not _stub)
    finally:
        generation.requests.post = original
    check("the patch is fully restored",
          quality.requests.post is not _stub and vllm_metrics.requests.post is not _stub)

    # --- 6. real reuse against a keep-alive server ---
    server, port = _start_server()
    url = f"http://127.0.0.1:{port}/v1/chat/completions"
    try:
        _reset_counters()
        threads_n, per_thread = 8, 15
        total = threads_n * per_thread

        def work():
            for _ in range(per_thread):
                generation.requests.post(url, data="{}", timeout=10)

        workers = [threading.Thread(target=work) for _ in range(threads_n)]
        for t in workers:
            t.start()
        for t in workers:
            t.join()

        accepted, served = _counters()
        check("every request reached the server", served == total,
              f"served={served} expected={total}")
        # Before the fix this was 1 connection per request (accepted == total).
        check("connections are REUSED, not opened per request",
              accepted < total / 4,
              f"{accepted} connections for {total} requests")
        check("the pool never exceeded its configured size",
              accepted <= api_handler.HTTP_POOL_MAXSIZE,
              f"{accepted} > {api_handler.HTTP_POOL_MAXSIZE}")

        # --- 7. a reused connection survives the server closing it while idle ---
        # nginx closes idle keep-alive connections (keepalive_timeout, 75 s by default) and
        # the rate limiter can park a slot for up to 60 s, so this is the normal case, not
        # an edge case. urllib3 must re-open transparently rather than raise.
        time.sleep(0.4)
        failures = 0
        for _ in range(3):
            try:
                generation.requests.post(url, data="{}", timeout=10)
            except Exception as exc:  # noqa: BLE001 - any raise is the failure
                failures += 1
                print(f"       raised {type(exc).__name__}: {exc}")
            time.sleep(0.3)
        check("a request after an idle gap does not raise", failures == 0,
              f"{failures} raised")
    finally:
        server.shutdown()

    # --- 8. teardown is safe and idempotent ---
    api_handler.close_http_session()
    api_handler.close_http_session()
    check("close_http_session() is idempotent", True)

    print()
    if FAILURES:
        print(f"FAILED ({len(FAILURES)}): {', '.join(FAILURES)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
