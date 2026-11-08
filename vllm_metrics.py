# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""vLLM server metrics for the dashboard's 📊 Metrics tab (v13.1.0).

The generator's four main API slots, the two fixers and the quality judge may all point at a
vLLM server. vLLM exposes Prometheus metrics on ``/metrics`` on the SAME port as its OpenAI
API, and that endpoint is the only place the server's real prefill/decode speeds and its
saturation live. This module reads it and turns it into the three numbers the Metrics tab shows:

``Prompt processing speed``
    Prompt tokens computed per second of prefill time. Taken from the delta of
    ``vllm:request_prefill_kv_computed_tokens_sum`` (tokens actually computed, i.e. prefix
    cache hits excluded) over the delta of ``vllm:request_prefill_time_seconds_sum``. When the
    kv-computed histogram is absent (older vLLM), it falls back to
    ``vllm:prompt_tokens_by_source_total{source="local_compute"}`` and then to
    ``vllm:prompt_tokens_total`` minus ``vllm:prompt_tokens_cached_total``.

``Token generation speed``
    Generation tokens per second of decode time, from the deltas of
    ``vllm:generation_tokens_total`` and ``vllm:request_decode_time_seconds_sum``. This is
    per-request stream speed (the sum of decode time across the batch), NOT wall-clock
    throughput, so it stays meaningful while many requests are in flight.

``Endpoint load``
    ``vllm:kv_cache_usage_perc`` as a percentage -- vLLM's own saturation gauge (the
    fraction of KV cache blocks in use). 1.0 is full. ``num_requests_running`` and
    ``num_requests_waiting`` are reported next to it because they explain WHY it is high
    (many streams, or a queue building).

Two important rules
-------------------
1. **Nothing here is aggregated across endpoints.** Each generator API slot gets its own row,
   and each row carries the figures of the vLLM server it points at. Two slots on one server
   therefore show the same server figures, which is correct: the server's KV cache and decode
   time are shared, and pretending otherwise would be an invented number.
2. **The per-slot figures are OUR measurements, not the server's.** ``slot_usage()`` counts the
   prompt/completion tokens each slot's own responses reported (``usage`` in the completion body)
   against the wall-clock time those requests took. That is the only per-slot number that exists:
   vLLM's own metrics have no per-caller label.

The poller is a daemon thread started by ``start_polling()`` from ``generate.py`` once the main
window exists. It never runs in a test, the CLI or a headless run unless something calls it, and it
only issues a GET against ``/metrics`` -- it sends no inference request and changes no config.

**It polls whether or not the Metrics tab is open** (v13.5.0). The tab is a *view* of the history,
not the thing that collects it: gating the poll on the visible tab meant the graphs were empty for
the first 10 s after every tab switch and the history had holes wherever the user was looking at
something else. The history is also written to ``output/metrics_history.json`` (every ``SAVE_INTERVAL_SECONDS``,
plus once on shutdown), so it survives the window closing.
"""
import json
import os
import threading
import time
from urllib.parse import urlsplit

import requests

from api_handler import http

# v14.2.0: the /metrics poller shares the pooled session too. It runs every 2 s against
# every endpoint, so it was a steady drip of fresh connections on top of the workers'.
# The name stays ``requests`` for the call site and for the tests that stub it.
requests = http

# --- tuning ------------------------------------------------------------------
# v13.5.0: 2 s, down from 10 s.  At 10 s the tab was visibly stale and the graphs were
# coarse: a 40-thread run finishes several requests between two polls, so a burst of bans
# landed as one tall spike and a short stall vanished between samples entirely.  /metrics is a
# cheap local GET (a few KB of text), so 2 s costs nothing measurable and makes the rates
# trustworthy -- a rate is a delta over the interval, and a shorter interval means less
# averaging over work that happened at a different moment.
POLL_INTERVAL_SECONDS = 2.0
# /metrics is a cheap local endpoint; a short timeout keeps a dead host from stalling a poll.
# It must stay BELOW the poll interval, or one dead host would push every other endpoint's
# samples apart and make their rates wrong too.
REQUEST_TIMEOUT_SECONDS = 1.5
# 300 samples at 2 s = 10 minutes of graph history.  The old 120 samples was 20 minutes at
# 10 s, so the window is shorter but far denser: the graph is now smooth enough to read a
# trend from instead of joining a handful of points with straight lines.
HISTORY_LENGTH = 300
# How often the history is written to disk.  Deliberately SLOWER than the poll interval: the
# document is a few hundred KB, and rewriting it every 2 s is pointless churn on an SD card for
# telemetry that only needs to survive a restart.  A crash loses at most this much history.
SAVE_INTERVAL_SECONDS = 30.0

# --- per-slot own measurement -------------------------------------------------
# {slot_idx: {"prompt_tokens": int, "completion_tokens": int, "requests": int,
#             "seconds": float, "first_at": float|None, "last_at": float|None}}
_slot_usage = {}
_slot_usage_lock = threading.Lock()

# --- per-slot throughput history (v13.4.0) ------------------------------------
# {slot_idx: [{"time": float, "tokens_per_second": float|None}, ...]}
# Kept apart from the per-URL history because two slots can share one URL.
_slot_history = {}
_slot_history_lock = threading.Lock()

# --- endpoint state ----------------------------------------------------------
# {metrics_url: {"counters": {...}|None, "sample_at": float|None,
#                "model": str, "error": str|None, "history": [(t, dict), ...],
#                "last": dict}}
_endpoints = {}
_endpoints_lock = threading.Lock()

_poller_thread = None
_poller_stop = threading.Event()

# Slot labels. Index counts from 0 (see the API Slots Index in AGENTS.md); the label counts
# from 1 the way the GUI does.
SLOT_LABELS = {
    0: "API 1",
    1: "API 2",
    2: "API 3",
    3: "API 4",
    4: "API 5 (Slop Fixer)",
    5: "API 6 (Anti-Slop Fixer)",
    6: "Judge (Quality)",
}

METRICS_PATH = "/metrics"


# ---------------------------------------------------------------------------
# URL derivation
# ---------------------------------------------------------------------------

def metrics_url_for_api_url(api_url):
    """The ``/metrics`` URL of the vLLM server behind an OpenAI-style API URL.

    vLLM serves Prometheus metrics on the same host and port as the OpenAI API, at the
    root path, so the path of the configured URL is discarded::

        http://host:8000/v1/chat/completions -> http://host:8000/metrics
        http://host:8000/v1                -> http://host:8000/metrics
        http://host:8000/                   -> http://host:8000/metrics

    Returns ``None`` for an empty or unparseable URL, so a slot that is not configured
    simply produces no row.
    """
    if not api_url:
        return None
    text = str(api_url).strip()
    if not text:
        return None
    if "://" not in text:
        # A bare host:port is common in a hand-edited config; assume http, like requests.
        text = "http://" + text
    try:
        parts = urlsplit(text)
    except ValueError:
        return None
    if not parts.scheme or not parts.netloc:
        return None
    # A hand-edited config can hold a host with a space in it; that is not an endpoint.
    if any(char.isspace() for char in parts.netloc):
        return None
    return "{}://{}{}".format(parts.scheme, parts.netloc, METRICS_PATH)


# ---------------------------------------------------------------------------
# Prometheus text parsing
# ---------------------------------------------------------------------------

def _parse_labels(blob):
    """Parse a Prometheus label block (``a="1",b="2"``) into a dict.

    Values are unquoted with the backslash escapes Prometheus allows, which is all the
    parser needs: this module only ever looks labels up by exact name.
    """
    labels = {}
    i = 0
    length = len(blob)
    while i < length:
        equals = blob.find("=", i)
        if equals < 0:
            break
        name = blob[i:equals].strip()
        # Skip the opening quote; find the matching unescaped one.
        cursor = equals + 1
        if cursor < length and blob[cursor] == '"':
            cursor += 1
            chars = []
            while cursor < length:
                char = blob[cursor]
                if char == "\\" and cursor + 1 < length:
                    chars.append(blob[cursor + 1])
                    cursor += 2
                    continue
                if char == '"':
                    cursor += 1
                    break
                chars.append(char)
                cursor += 1
            labels[name] = "".join(chars)
        else:
            comma = blob.find(",", cursor)
            if comma < 0:
                comma = length
            labels[name] = blob[cursor:comma].strip()
            cursor = comma
        i = cursor + 1
    return labels


def parse_prometheus_text(text):
    """Parse a Prometheus exposition body into ``{metric_name: [(labels, value), ...]}``.

    Comment lines (``# HELP`` / ``# TYPE``) are ignored, so the result only holds samples.
    A sample that does not parse as a float is skipped rather than raised on: one odd line
    from a newer vLLM must not blank the whole tab.
    """
    samples = {}
    if not text:
        return samples
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        brace = line.find("{")
        space = line.rfind(" ")
        if space <= 0:
            continue
        if 0 <= brace < space:
            name = line[:brace]
            labels = _parse_labels(line[brace + 1:line.rfind("}")])
            value_text = line[space + 1:]
        else:
            name = line[:space]
            labels = {}
            value_text = line[space + 1:]
        try:
            value = float(value_text)
        except (TypeError, ValueError):
            continue
        samples.setdefault(name.strip(), []).append((labels, value))
    return samples


def _sum_series(samples, name, label_filter=None):
    """Sum every sample of one metric, optionally only those matching ``label_filter``.

    Returns ``None`` when the metric is absent so callers can tell "no such metric" from a
    genuine zero (a counter that has not moved yet).
    """
    entries = samples.get(name)
    if not entries:
        return None
    total = 0.0
    matched = False
    for labels, value in entries:
        if label_filter is not None:
            if any(labels.get(key) != expected for key, expected in label_filter.items()):
                continue
        matched = True
        total += value
    return total if matched else None


def _first_value(samples, name, label_filter=None, default=None):
    """First matching sample's value for a gauge (vLLM reports one per engine)."""
    entries = samples.get(name)
    if not entries:
        return default
    for labels, value in entries:
        if label_filter is not None:
            if any(labels.get(key) != expected for key, expected in label_filter.items()):
                continue
        return value
    return default


def _series_by_label(samples, name, label_name):
    """``{label_value: summed_value}`` for one metric, grouped by a single label.

    Used for the proxy's per-status request counts and its per-validator ban counts, which are
    naturally several series of one metric rather than several metrics.  An empty dict means the metric
    is absent, so a caller can tell "no such metric" from "every series is zero".
    """
    entries = samples.get(name)
    if not entries:
        return {}
    grouped = {}
    for labels, value in entries:
        key = labels.get(label_name) or ""
        grouped[key] = grouped.get(key, 0.0) + value
    return grouped


def _bans_by_validator(samples):
    """``{validator: {"total": n, "fixed": n}}`` from ``antislop:ban_events_total``.

    The pair is what a ban-list tuner needs: a validator with a large ``total`` and a small ``fixed``
    is firing on things the model cannot avoid, so the list (not the model) is what to change.
    """
    entries = samples.get("antislop:ban_events_total")
    if not entries:
        return {}
    grouped = {}
    for labels, value in entries:
        validator = labels.get("validator") or "unknown"
        bucket = grouped.setdefault(validator, {"total": 0.0, "fixed": 0.0})
        bucket["total"] += value
        if labels.get("fixed") == "true":
            bucket["fixed"] += value
    return grouped


def _model_name(samples):
    """The model a vLLM exposes, read from any sample's ``model_name`` label."""
    for entries in samples.values():
        for labels, _value in entries:
            name = labels.get("model_name")
            if name:
                return name
    return ""


def is_vllm_metrics(samples):
    """True when a parsed body really came from vLLM (any ``vllm:`` metric present)."""
    return any(name.startswith("vllm:") for name in samples)


def is_antislop_metrics(samples):
    """True when the body carries the antislop-vllm-fork proxy's own ``antislop:`` metrics (v13.3.0).

    The fork's ``GET /metrics`` concatenates the upstream vLLM body with its own counters, so a slot
    pointed at the fork usually has BOTH prefixes.  A body with only ``antislop:`` is a fork whose
    upstream vLLM is down: it is still a REACHABLE endpoint, which is why this is a separate test
    from :func:`is_vllm_metrics` rather than folded into it.
    """
    return any(name.startswith("antislop:") for name in samples)


def summarise_antislop(samples):
    """Reduce the fork's ``antislop:`` metrics to the figures the tab can show (v13.3.0).

    Every value is ``None`` when the metric is absent, so a plain vLLM body (which has no
    ``antislop:`` metric at all) yields an all-``None`` summary rather than a page of zeroes that
    would read as "this proxy did nothing".

    ``ban_fixed`` is the subset of ``ban_events`` whose back-track succeeded, which is the number to
    watch when tuning a ban list: a large ``ban_events`` with a small ``ban_fixed`` means the list is
    firing on things the model cannot avoid.
    """
    ban_total = _sum_series(samples, "antislop:ban_events_total")
    ban_fixed = _sum_series(samples, "antislop:ban_events_total", {"fixed": "true"})
    by_status = _series_by_label(samples, "antislop:requests_total", "status")
    by_finish = _series_by_label(samples, "antislop:generations_total", "finish_reason")
    by_stream = _series_by_label(samples, "antislop:streaming_requests_total", "stream")
    duration_sum = _sum_series(samples, "antislop:request_duration_seconds_sum")
    duration_count = _sum_series(samples, "antislop:request_duration_seconds_count")
    return {
        "requests": _sum_series(samples, "antislop:requests_total"),
        "requests_by_status": by_status or None,
        "requests_failed": (sum(value for status, value in by_status.items()
                                if not status.startswith("2")) if by_status else None),
        "in_flight": _first_value(samples, "antislop:requests_in_flight"),
        "duration_seconds": duration_sum,
        "duration_count": duration_count,
        "streaming": _sum_series(samples, "antislop:streaming_requests_total"),
        "streaming_by_stream": by_stream or None,
        "generations": _sum_series(samples, "antislop:generations_total"),
        "generations_by_finish": by_finish or None,
        "generation_tokens": _sum_series(samples, "antislop:generation_tokens_total"),
        "prompt_tokens": _sum_series(samples, "antislop:prompt_tokens_total"),
        "ban_events": ban_total,
        "ban_fixed": ban_fixed,
        "bans_by_validator": _bans_by_validator(samples) or None,
        "backtracks": _sum_series(samples, "antislop:backtracks_total"),
        "upstream_errors": _sum_series(samples, "antislop:upstream_errors_total"),
        "scrape_errors": _sum_series(
            samples, "antislop:upstream_metrics_scrape_total", {"result": "error"}),
        "scrape_ok": _sum_series(
            samples, "antislop:upstream_metrics_scrape_total", {"result": "ok"}),
        "last_scrape_at": _first_value(
            samples, "antislop:upstream_metrics_last_scrape_timestamp_seconds"),
    }


def compute_antislop_rates(current, previous, elapsed_seconds):
    """The proxy's own token-generation speed between two ``summarise_antislop()`` snapshots.

    This is deliberately NOT the same figure as :func:`compute_rates`' ``generation_tps``.  vLLM's
    figure divides by the server's own decode timer, so it measures the GPU.  This one divides by the
    **wall clock** between two polls, so it measures what the client actually received end to end --
    the gap between the two is the proxy's own overhead, which is the whole reason to show both.

    A single snapshot reports ``None`` rather than a guess, and a negative delta (a restarted proxy
    zeroed its counters) drops the sample instead of drawing a spike.
    """
    rates = {"generation_tps": None, "ban_rate": None, "backtrack_rate": None,
             "avg_request_seconds": None}
    if previous is None or elapsed_seconds is None or elapsed_seconds <= 0:
        return rates
    tokens = _delta(current.get("generation_tokens"), previous.get("generation_tokens"))
    if tokens is not None and tokens > 0:
        rates["generation_tps"] = tokens / elapsed_seconds
    bans = _delta(current.get("ban_events"), previous.get("ban_events"))
    if bans is not None and bans > 0:
        rates["ban_rate"] = bans / elapsed_seconds
    backtracks = _delta(current.get("backtracks"), previous.get("backtracks"))
    if backtracks is not None and backtracks > 0:
        rates["backtrack_rate"] = backtracks / elapsed_seconds
    # The mean time one request spent inside the proxy, from the summary's own sum/count pair.
    # A DELTA of both keeps it a window figure rather than a lifetime average.
    duration_sum = _delta(current.get("duration_seconds"), previous.get("duration_seconds"))
    duration_count = _delta(current.get("duration_count"), previous.get("duration_count"))
    if duration_sum is not None and duration_count is not None and duration_count > 0:
        rates["avg_request_seconds"] = duration_sum / duration_count
    return rates


def antislop_columns_enabled(config=None):
    """Whether the 📊 Metrics tab shows the proxy's own columns (``metrics.antislop_columns``).

    **Off by default.**  A plain vLLM serves no ``antislop:`` metrics, so a default run's tab must be
    exactly what it was before v13.3.0 (see the "features opt-in by default" rule in AGENTS.md).

    ``config=None`` reads ``app_state.global_config``, the same fallback :func:`_slot_urls` uses, so the
    UI can call this with no argument and a test can pass a plain dict.
    """
    if config is None:
        import app_state
        config = app_state.global_config
    return bool(_config_get(config, "metrics.antislop_columns", False))


# ---------------------------------------------------------------------------
# One endpoint snapshot
# ---------------------------------------------------------------------------

def summarise(samples):
    """Reduce a parsed ``/metrics`` body to the counters and gauges this tab uses.

    Every value is a CUMULATIVE counter or an instantaneous gauge; the speeds are derived by
    ``compute_rates()`` from two snapshots, never from a single one.
    """
    kv_computed = _sum_series(samples, "vllm:request_prefill_kv_computed_tokens_sum")
    local_compute = _sum_series(
        samples, "vllm:prompt_tokens_by_source_total", {"source": "local_compute"})
    prompt_total = _sum_series(samples, "vllm:prompt_tokens_total")
    prompt_cached = _sum_series(samples, "vllm:prompt_tokens_cached_total")
    if kv_computed is not None:
        # Tokens actually computed: prefix-cache hits are excluded, so this is the work the
        # GPU really did and it pairs with the prefill timer.
        prompt_compute_tokens = kv_computed
    elif local_compute is not None:
        prompt_compute_tokens = local_compute
    elif prompt_total is not None:
        prompt_compute_tokens = prompt_total - (prompt_cached or 0.0)
    else:
        prompt_compute_tokens = None

    load = _first_value(samples, "vllm:kv_cache_usage_perc")
    return {
        "prompt_compute_tokens": prompt_compute_tokens,
        "generation_tokens": _sum_series(samples, "vllm:generation_tokens_total"),
        "prefill_seconds": _sum_series(samples, "vllm:request_prefill_time_seconds_sum"),
        "decode_seconds": _sum_series(samples, "vllm:request_decode_time_seconds_sum"),
        "requests_ok": _sum_series(samples, "vllm:request_success_total"),
        "kv_cache_usage": load,
        "load_percent": None if load is None else load * 100.0,
        "running": _first_value(samples, "vllm:num_requests_running"),
        "waiting": _first_value(samples, "vllm:num_requests_waiting"),
        "model": _model_name(samples),
    }


def _delta(current, previous):
    """Non-negative delta of one cumulative counter, or ``None`` if it cannot be trusted.

    A negative delta means the server restarted (counters went back to zero), so the pair is not
    a rate: the caller drops the sample and the next poll re-baselines instead of drawing a
    nonsense spike into the graph.
    """
    if current is None or previous is None:
        return None
    value = current - previous
    return value if value >= 0 else None


def compute_rates(current, previous, elapsed_seconds):
    """Prompt/decode speeds in tokens per second between two ``summarise()`` snapshots.

    Both speeds are tokens divided by the server's OWN time counter, not by the wall clock, so a
    burst of concurrency cannot make the numbers look faster than the hardware. Returns a dict with
    ``prompt_tps`` and ``generation_tps``; either is ``None`` when that work did not happen in
    this interval (no tokens, or no time recorded).
    """
    rates = {"prompt_tps": None, "generation_tps": None}
    if previous is None or elapsed_seconds is None or elapsed_seconds <= 0:
        return rates

    prompt_tokens = _delta(current.get("prompt_compute_tokens"), previous.get("prompt_compute_tokens"))
    prompt_seconds = _delta(current.get("prefill_seconds"), previous.get("prefill_seconds"))
    if prompt_tokens is not None and prompt_seconds is not None and prompt_seconds > 0.001:
        rates["prompt_tps"] = prompt_tokens / prompt_seconds

    generation_tokens = _delta(current.get("generation_tokens"), previous.get("generation_tokens"))
    decode_seconds = _delta(current.get("decode_seconds"), previous.get("decode_seconds"))
    if generation_tokens is not None and decode_seconds is not None and decode_seconds > 0.001:
        rates["generation_tps"] = generation_tokens / decode_seconds
    return rates


# ---------------------------------------------------------------------------
# Per-slot own measurement
# ---------------------------------------------------------------------------

def reset_slot_usage():
    """Clear the per-slot token accounting (a new run starts from zero)."""
    with _slot_usage_lock:
        _slot_usage.clear()


def note_slot_usage(slot_idx, usage, elapsed_seconds):
    """Record one finished request's reported tokens against the slot that sent it.

    ``usage`` is the ``usage`` object of the completion body (``prompt_tokens`` /
    ``completion_tokens``) and ``elapsed_seconds`` is that request's wall-clock duration.
    A body without ``usage`` (a server that does not report it) records the request and its
    time but no tokens, so the slot's rate stays honest instead of being invented.

    This is the only per-slot number that exists: vLLM's ``/metrics`` has no caller label, so a
    slot sharing a server cannot be separated from it there. Here each slot's own responses are
    counted, which is exactly "this API's" throughput.
    """
    try:
        slot_idx = int(slot_idx)
    except (TypeError, ValueError):
        return
    usage = usage or {}
    try:
        prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
        completion_tokens = int(usage.get("completion_tokens", 0) or 0)
    except (TypeError, ValueError):
        prompt_tokens = completion_tokens = 0
    try:
        elapsed = max(0.0, float(elapsed_seconds or 0.0))
    except (TypeError, ValueError):
        elapsed = 0.0

    now = time.time()
    with _slot_usage_lock:
        entry = _slot_usage.setdefault(slot_idx, {
            "prompt_tokens": 0, "completion_tokens": 0, "requests": 0,
            "seconds": 0.0, "first_at": None, "last_at": None,
        })
        entry["prompt_tokens"] += prompt_tokens
        entry["completion_tokens"] += completion_tokens
        entry["requests"] += 1
        entry["seconds"] += elapsed
        if entry["first_at"] is None:
            entry["first_at"] = now
        entry["last_at"] = now


def slot_usage(slot_idx):
    """This slot's own totals and its achieved rate, or ``None`` before its first request.

    ``tokens_per_second`` is all tokens the slot received (prompt + completion) divided by the
    wall-clock time its requests took, which is the end-to-end speed a user experiences for that
    API rather than the server's per-stream decode speed.
    """
    with _slot_usage_lock:
        entry = _slot_usage.get(int(slot_idx))
        if entry is None:
            return None
        data = dict(entry)
    tokens = data["prompt_tokens"] + data["completion_tokens"]
    data["total_tokens"] = tokens
    # A server that reports no token usage gives no throughput: report "-" rather than 0, which
    # would read as "this slot is doing nothing" when the truth is "we cannot tell".
    data["tokens_per_second"] = (tokens / data["seconds"]) if (tokens and data["seconds"] > 0.001) else None
    return data


def slot_usage_rows():
    """Every slot with recorded usage, as ``[(slot_idx, usage_dict), ...]`` sorted by slot."""
    with _slot_usage_lock:
        indices = sorted(_slot_usage)
    return [(idx, slot_usage(idx)) for idx in indices]


def record_slot_history(config=None):
    """Append one throughput sample per configured slot, for the per-slot graph (v13.4.0).

    Called once per poll cycle.  A slot's own throughput is measured from its responses, not from
    the server, so it needs its own ring: the per-URL history cannot hold it, because two slots may
    share one URL and would then overwrite each other's line.
    """
    now = time.time()
    for slot_idx, _api_url in _slot_urls(config):
        usage = slot_usage(slot_idx) or {}
        with _slot_history_lock:
            ring = _slot_history.setdefault(slot_idx, [])
            ring.append({"time": now,
                         "tokens_per_second": usage.get("tokens_per_second")})
            overflow = len(ring) - HISTORY_LENGTH
            if overflow > 0:
                del ring[:overflow]


def slot_history(slot_idx):
    """A copy of one slot's throughput ring, oldest first (empty before its first poll)."""
    with _slot_history_lock:
        return list(_slot_history.get(int(slot_idx), ()))


def reset_slot_history():
    """Forget every slot's throughput ring (a new run starts from a clean baseline)."""
    with _slot_history_lock:
        _slot_history.clear()


# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------

def _config_get(config, path, default=None):
    """Read a dot-separated config path from a ConfigLoader OR a plain nested dict.

    ``app_state.global_config`` resolves ``'api.apis.0.url'`` itself, but a plain dict (what the
    tests pass, and what a caller building a throwaway config has) does not. Supporting both here
    keeps the row builder usable without a live ConfigLoader.
    """
    if hasattr(config, "get") and not isinstance(config, dict):
        return config.get(path, default)
    value = config
    for key in path.split("."):
        if isinstance(value, dict) and key in value:
            value = value[key]
        elif isinstance(value, list) and key.isdigit() and int(key) < len(value):
            value = value[int(key)]
        else:
            return default
    return value


def _slot_urls(config=None):
    """Resolve ``[(slot_idx, api_url), ...]`` for every slot that may be a vLLM server.

    Slots 0-3 are the main generation APIs, 4 the Slop Fixer, 5 the Anti-Slop Fixer and 6 the
    quality judge (which has no ``api.apis`` entry; it is configured under
    ``quality.scoring_api``). A slot without a URL is skipped.
    """
    if config is None:
        import app_state
        config = app_state.global_config
    urls = []
    apis = _config_get(config, "api.apis", []) or []
    for idx in range(6):
        entry = apis[idx] if idx < len(apis) and isinstance(apis[idx], dict) else {}
        url = (entry.get("url") or "").strip()
        if url:
            urls.append((idx, url))
    judge_url = (_config_get(config, "quality.scoring_api.url", "") or "").strip()
    if judge_url:
        urls.append((6, judge_url))
    return urls


def _fetch_metrics(metrics_url):
    """GET one ``/metrics`` body. Returns ``(text, error_message)``; text is ``None`` on error.

    Every exception is caught: a dead host, a DNS failure and a refused connection all mean the
    same thing here (no figures for this endpoint), and one bad endpoint must never stop the poll of
    the others.
    """
    try:
        response = requests.get(metrics_url, timeout=REQUEST_TIMEOUT_SECONDS)
    except Exception as exc:  # requests.RequestException, and anything a stub raises
        return None, str(exc)[:200] or exc.__class__.__name__
    if response.status_code != 200:
        return None, "HTTP {}".format(response.status_code)
    return response.text, None


def poll_endpoint(metrics_url, now=None):
    """Fetch one endpoint, compute its speeds and append a history sample.

    Returns the endpoint's state dict (never raises). The first successful poll only records a
    baseline: a rate needs two snapshots, so it reports no speeds yet rather than inventing one.
    """
    now = time.time() if now is None else now
    text, error = _fetch_metrics(metrics_url)
    with _endpoints_lock:
        state = _endpoints.setdefault(metrics_url, {
            "counters": None, "antislop_counters": None, "sample_at": None, "model": "",
            "error": None, "history": [], "last": {},
        })

    if error is not None:
        with _endpoints_lock:
            state["error"] = error
        return state

    samples = parse_prometheus_text(text)
    # v13.3.0: the antislop-vllm-fork's /metrics carries its own antislop: metrics alongside the
    # upstream vllm: ones, and a fork whose upstream vLLM is DOWN still answers with only the
    # antislop: half.  That is a REACHABLE endpoint, not an error, so either prefix is enough.
    has_vllm = is_vllm_metrics(samples)
    has_antislop = is_antislop_metrics(samples)
    if not (has_vllm or has_antislop):
        with _endpoints_lock:
            state["error"] = "not a vLLM endpoint (no vllm: metrics)"
        return state

    current = summarise(samples)
    antislop_current = summarise_antislop(samples)
    previous = state["counters"]
    antislop_previous = state.get("antislop_counters")
    elapsed = None if state["sample_at"] is None else now - state["sample_at"]
    rates = compute_rates(current, previous, elapsed)
    antislop_rates = compute_antislop_rates(antislop_current, antislop_previous, elapsed)

    sample = {
        "time": now,
        "prompt_tps": rates["prompt_tps"],
        "generation_tps": rates["generation_tps"],
        "load_percent": current["load_percent"],
        "running": current["running"],
        "waiting": current["waiting"],
        "antislop_generation_tps": antislop_rates["generation_tps"],
        "antislop_ban_events": antislop_current["ban_events"],
        # v13.4.0: the rest of the proxy's figures, so the new graph panels have a history to
        # draw.  ``None`` for a plain vLLM, which draws a gap rather than a zero line.
        "antislop_ban_rate": antislop_rates["ban_rate"],
        "antislop_backtrack_rate": antislop_rates["backtrack_rate"],
        "antislop_in_flight": antislop_current["in_flight"],
        "antislop_avg_request_seconds": antislop_rates["avg_request_seconds"],
        "antislop_upstream_errors": antislop_current["upstream_errors"],
        "antislop_requests": antislop_current["requests"],
    }
    # ``last`` is what the table reads: the gauges from this snapshot plus the rates derived
    # from it, so one dict holds everything one row needs.
    latest = dict(current)
    latest.update(rates)
    latest["sample_at"] = now
    latest["antislop"] = antislop_current
    latest["antislop_rates"] = antislop_rates
    latest["is_antislop"] = has_antislop

    with _endpoints_lock:
        state["counters"] = current
        state["antislop_counters"] = antislop_current
        state["sample_at"] = now
        state["model"] = current["model"] or state["model"]
        state["error"] = None
        state["last"] = latest
        history = state["history"]
        history.append(sample)
        overflow = len(history) - HISTORY_LENGTH
        if overflow > 0:
            del history[:overflow]
    return state


def poll_once(config=None):
    """Poll every distinct vLLM endpoint one slot points at, once.

    The same URL is fetched ONCE per cycle however many slots use it (two slots on one server
    must not double the request rate), then every slot builds its own row from that one result.

    v13.5.0: the endpoints are fetched **concurrently**, one short-lived thread each.  Serially, a
    single dead or slow host cost every later endpoint its sample for that cycle -- with a 1.5 s
    timeout and four endpoints that is up to 6 s of skew, which is exactly what made the figures
    look "missing" and the rates unreliable.  Now the cycle takes as long as the SLOWEST endpoint,
    not the sum of all of them, so one bad host cannot starve the others.
    """
    metrics_urls = []
    for _slot_idx, api_url in _slot_urls(config):
        metrics_url = metrics_url_for_api_url(api_url)
        if metrics_url and metrics_url not in metrics_urls:
            metrics_urls.append(metrics_url)
    if not metrics_urls:
        return metrics_urls

    # One thread per endpoint, joined before returning so a caller still sees a complete cycle.
    # ``poll_endpoint()`` never raises, so a thread cannot leak an exception.
    threads = []
    for metrics_url in metrics_urls:
        thread = threading.Thread(target=poll_endpoint, args=(metrics_url,),
                                  name="vllm-metrics-fetch", daemon=True)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join(timeout=REQUEST_TIMEOUT_SECONDS + 1.0)

    # v13.4.0: one throughput sample per slot for the per-slot graph.  Done here rather than in
    # poll_endpoint() because that function is per-URL, and a slot is not a URL.
    record_slot_history(config)
    # v13.5.0: persist the cycle, so the history survives the window closing.
    save_snapshot(config)
    return metrics_urls


def _poller_loop(interval):
    """Poll until ``stop_polling()`` is called; never lets an exception kill the thread."""
    while not _poller_stop.is_set():
        try:
            poll_once()
        except Exception:
            # A poll is best-effort telemetry: a bad config or a dead host must not take the
            # thread (and with it the tab's refresh) down for the rest of the run.
            pass
        if _poller_stop.wait(interval):
            return


def start_polling(interval=POLL_INTERVAL_SECONDS):
    """Start the background poller (idempotent). Returns the thread.

    Called from ``generate.py`` once the main window exists. It is deliberately NOT started by
    import: a test, the CLI and a headless run must not poll anything.

    v13.5.0: the previous run's saved history is loaded first, so the graphs open with real samples
    instead of an empty canvas.  A missing or unreadable snapshot is ignored.
    """
    global _poller_thread
    if _poller_thread is not None and _poller_thread.is_alive():
        return _poller_thread
    load_snapshot()
    _poller_stop.clear()
    _poller_thread = threading.Thread(
        target=_poller_loop, args=(interval,), name="vllm-metrics-poller", daemon=True)
    _poller_thread.start()
    return _poller_thread


def stop_polling():
    """Ask the background poller to exit (used on window close and by the tests).

    v13.5.0: writes a FINAL snapshot before returning, so the last interval before the window
    closed is not lost to the save throttle.  ``force=True`` because the throttle would otherwise
    skip it whenever the window is closed within ``SAVE_INTERVAL_SECONDS`` of the last write.
    """
    global _poller_thread
    _poller_stop.set()
    thread = _poller_thread
    if thread is not None and thread.is_alive():
        thread.join(timeout=REQUEST_TIMEOUT_SECONDS + 1)
    _poller_thread = None
    save_snapshot(force=True)


def reset_endpoints():
    """Forget every endpoint and its history (a new run starts from a clean baseline)."""
    with _endpoints_lock:
        _endpoints.clear()


# ---------------------------------------------------------------------------
# Persistence (v13.5.0)
#
# The poller runs whether or not the Metrics tab is open, and the history is written to
# ``output/metrics_history.json`` every cycle.  Two reasons:
#
# 1. **The figures outlive the window.**  Before this, closing the generator threw away every
#    sample, so a user who wanted to know why a run slowed down had to have had the tab open at
#    the time.  The file is a plain JSON document, readable with ``jq`` or a notebook.
# 2. **A crash loses at most one interval.**  The write is atomic (temp file + ``os.replace``),
#    so a kill mid-write cannot leave a truncated file that fails to parse on the next start.
#
# It is deliberately NOT a growing log: the same bounded ring the graphs draw is what is saved, so
# the file stays a few hundred KB however long the run is.  ``metrics.save_history: false`` turns
# it off for a user who does not want the writes.
# ---------------------------------------------------------------------------

# ``output/`` is the same directory the dataset, the log and the run state use (app_state.OUTPUT_DIR).
METRICS_HISTORY_FILENAME = "metrics_history.json"

_save_lock = threading.Lock()
_last_save_at = 0.0


def _round_floats(value, digits=3, drop_none=False):
    """Round every float in a nested document, so the JSON is not 17 digits per sample.

    A ``repr(float)`` of a rate is ~18 characters; three decimals is far more precision than a
    graph can show.  ``drop_none`` also omits ``null`` values, which is what makes a plain-vLLM
    sample half the size: it carries seven ``antislop_*`` keys that are all ``None``, and a missing
    key already reads back as ``None`` (see ``load_snapshot()``).  Nothing is turned into ``0.0``, so
    "this endpoint does not report that figure" stays distinguishable from "it reported zero".
    """
    if isinstance(value, float):
        return round(value, digits)
    if isinstance(value, dict):
        return {key: _round_floats(item, digits, drop_none)
                for key, item in value.items()
                if not (drop_none and item is None)}
    if isinstance(value, list):
        return [_round_floats(item, digits, drop_none) for item in value]
    return value


def history_path(config=None):
    """Where the metrics history is written: ``<output>/metrics_history.json``.

    Read from ``app_state.OUTPUT_DIR`` so it follows the same directory as every other output
    file.  Falls back to ``output/`` when ``app_state`` is not importable (a bare script), so a
    caller never has to care which it is.
    """
    try:
        import app_state
        output_dir = getattr(app_state, "OUTPUT_DIR", "output")
    except Exception:
        output_dir = "output"
    return os.path.join(output_dir, METRICS_HISTORY_FILENAME)


def save_history_enabled(config=None):
    """Whether the poller writes ``output/metrics_history.json`` (``metrics.save_history``).

    **On by default** (v13.5.0).  Unlike the antislop COLUMNS, this writes a file in ``output/``,
    which is gitignored local run data -- it changes no request, no prompt and no output row, so
    the "a default run stays byte-identical" rule is not in play.  The key exists so a user on a
    read-only or network filesystem can turn it off.
    """
    if config is None:
        try:
            import app_state
            config = app_state.global_config
        except Exception:
            return True
    return bool(_config_get(config, "metrics.save_history", True))


def _generator_version():
    """The generator's version string, or ``""`` when it cannot be read.

    Read lazily from ``control.GENERATOR_VERSION`` (the one place the version is declared) rather
    than imported at module scope: ``control`` pulls in the control socket, and this module must
    stay importable headlessly with no side effects.
    """
    try:
        import control
        return str(getattr(control, "GENERATOR_VERSION", "") or "")
    except Exception:
        return ""


def _history_document(config=None):
    """The serialisable snapshot of every endpoint and every slot.

    Built under the locks so a concurrent poll cannot produce a half-updated document.  ``last`` is
    dropped: it is the derived per-row view, and re-deriving it from ``counters`` on load is both
    smaller and impossible to get out of step with the history.
    """
    now = time.time()
    with _endpoints_lock:
        endpoints = {}
        for url, state in _endpoints.items():
            endpoints[url] = {
                "model": state.get("model") or "",
                "error": state.get("error"),
                "sample_at": state.get("sample_at"),
                "counters": state.get("counters"),
                "antislop_counters": state.get("antislop_counters"),
                "history": list(state.get("history") or []),
            }
    with _slot_history_lock:
        slots = {str(idx): list(ring) for idx, ring in _slot_history.items()}
    with _slot_usage_lock:
        usage = {str(idx): dict(entry) for idx, entry in _slot_usage.items()}
    return {
        "version": 1,
        "generator_version": _generator_version(),
        "saved_at": now,
        "poll_interval_seconds": POLL_INTERVAL_SECONDS,
        "history_length": HISTORY_LENGTH,
        "endpoints": endpoints,
        "slots": slots,
        "slot_usage": usage,
    }


def save_snapshot(config=None, force=False):
    """Write the metrics history to ``output/metrics_history.json``.  Never raises.

    Throttled to ``SAVE_INTERVAL_SECONDS`` unless ``force`` is set: the document is a few hundred
    KB and rewriting it on every 2 s poll is pointless churn on an SD card.  ``force=True`` is what
    shutdown uses, so the last interval is never lost.

    Atomic: the document goes to a temp file in the same directory, then ``os.replace()`` swaps it
    in, so a crash or a kill mid-write leaves the previous good file rather than a truncated one.
    A failure is swallowed -- losing a telemetry snapshot must never fail a generation request or
    kill the poller thread.
    """
    global _last_save_at
    if not save_history_enabled(config):
        return None
    now = time.time()
    if not force and (now - _last_save_at) < SAVE_INTERVAL_SECONDS:
        return None
    path = history_path(config)
    try:
        document = _round_floats(_history_document(config), drop_none=True)
        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        with _save_lock:
            temp_path = path + ".tmp"
            with open(temp_path, "w", encoding="utf-8") as handle:
                json.dump(document, handle, separators=(",", ":"))
            os.replace(temp_path, path)
        _last_save_at = now
        return path
    except Exception:
        return None


def load_snapshot(config=None):
    """Restore a saved history document, returning how many endpoints were loaded.

    Called once at startup so the graphs open with the previous run's samples rather than an empty
    canvas.  A missing, unreadable or newer-versioned file is ignored (returns 0) -- a stale
    snapshot must never stop the app from starting.
    """
    path = history_path(config)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            document = json.load(handle)
    except Exception:
        return 0
    if not isinstance(document, dict) or document.get("version") != 1:
        return 0

    loaded = 0
    with _endpoints_lock:
        for url, saved in (document.get("endpoints") or {}).items():
            if not isinstance(saved, dict):
                continue
            history = [sample for sample in (saved.get("history") or [])
                       if isinstance(sample, dict) and "time" in sample]
            _endpoints[url] = {
                "counters": saved.get("counters"),
                "antislop_counters": saved.get("antislop_counters"),
                "sample_at": saved.get("sample_at"),
                "model": saved.get("model") or "",
                "error": saved.get("error"),
                "history": history[-HISTORY_LENGTH:],
                "last": {},
            }
            loaded += 1
    with _slot_history_lock:
        for key, ring in (document.get("slots") or {}).items():
            try:
                idx = int(key)
            except (TypeError, ValueError):
                continue
            if isinstance(ring, list):
                _slot_history[idx] = [
                    sample for sample in ring
                    if isinstance(sample, dict) and "time" in sample][-HISTORY_LENGTH:]
    with _slot_usage_lock:
        for key, entry in (document.get("slot_usage") or {}).items():
            try:
                idx = int(key)
            except (TypeError, ValueError):
                continue
            if isinstance(entry, dict):
                _slot_usage[idx] = dict(entry)
    return loaded


# ---------------------------------------------------------------------------
# Rows for the UI
# ---------------------------------------------------------------------------

def endpoint_rows(config=None):
    """One row per API slot whose backend is a vLLM server, for the Metrics tab table.

    Each row is a dict::

        {'slot_idx', 'label', 'api_url', 'metrics_url', 'model', 'error',
         'prompt_tps', 'generation_tps', 'load_percent', 'running', 'waiting',
         'prompt_tokens', 'generation_tokens', 'requests',
         'slot_tokens_per_second', 'history'}

    The server figures are shared by every slot on the same server (that is the truth:
    vLLM's KV cache and decode time are server-wide). The ``slot_*`` figures are this
    slot's own measured usage, so two slots on one server still differ.
    """
    rows = []
    with _endpoints_lock:
        state_snapshot = {url: {
            "error": state.get("error"),
            "model": state.get("model", ""),
            "last": dict(state.get("last") or {}),
            "history": list(state.get("history") or []),
        } for url, state in _endpoints.items()}

    seen_slots = set()
    for slot_idx, api_url in _slot_urls(config):
        if slot_idx in seen_slots:
            continue
        seen_slots.add(slot_idx)
        metrics_url = metrics_url_for_api_url(api_url)
        if not metrics_url:
            continue
        state = state_snapshot.get(metrics_url)
        own = slot_usage(slot_idx) or {}
        last = (state or {}).get("last") or {}
        antislop = last.get("antislop") or {}
        antislop_rates = last.get("antislop_rates") or {}
        rows.append({
            "slot_idx": slot_idx,
            "label": SLOT_LABELS.get(slot_idx, "API {}".format(slot_idx + 1)),
            "api_url": api_url,
            "metrics_url": metrics_url,
            "model": (state or {}).get("model", "") or last.get("model", ""),
            "error": (state or {}).get("error"),
            "prompt_tps": last.get("prompt_tps"),
            "generation_tps": last.get("generation_tps"),
            "load_percent": last.get("load_percent"),
            "running": last.get("running"),
            "waiting": last.get("waiting"),
            "prompt_tokens": own.get("prompt_tokens"),
            "generation_tokens": own.get("completion_tokens"),
            "requests": own.get("requests"),
            "slot_tokens_per_second": own.get("tokens_per_second"),
            # v13.3.0: the antislop-vllm-fork proxy's own figures.  ``None`` for a plain vLLM
            # (which serves no antislop: metrics), so the columns render "-" and nothing is invented.
            "is_antislop": bool(last.get("is_antislop")),
            "antislop_requests": antislop.get("requests"),
            "antislop_ban_events": antislop.get("ban_events"),
            "antislop_ban_fixed": antislop.get("ban_fixed"),
            "antislop_upstream_errors": antislop.get("upstream_errors"),
            "antislop_scrape_errors": antislop.get("scrape_errors"),
            "antislop_in_flight": antislop.get("in_flight"),
            "antislop_generation_tps": antislop_rates.get("generation_tps"),
            # v13.4.0: the rest of the proxy's figures, so the tab can show all twelve metrics
            # the fork emits rather than four of them.
            "antislop_prompt_tokens": antislop.get("prompt_tokens"),
            "antislop_generation_tokens": antislop.get("generation_tokens"),
            "antislop_generations": antislop.get("generations"),
            "antislop_streaming": antislop.get("streaming"),
            "antislop_backtracks": antislop.get("backtracks"),
            "antislop_requests_failed": antislop.get("requests_failed"),
            "antislop_scrape_ok": antislop.get("scrape_ok"),
            "antislop_last_scrape_at": antislop.get("last_scrape_at"),
            "antislop_avg_request_seconds": antislop_rates.get("avg_request_seconds"),
            "antislop_ban_rate": antislop_rates.get("ban_rate"),
            "antislop_backtrack_rate": antislop_rates.get("backtrack_rate"),
            "antislop_bans_by_validator": antislop.get("bans_by_validator"),
            "antislop_generations_by_finish": antislop.get("generations_by_finish"),
            "antislop_requests_by_status": antislop.get("requests_by_status"),
            "slot_history": slot_history(slot_idx),
            "history": (state or {}).get("history") or [],
        })
    return rows


def polled_endpoint_count():
    """How many distinct endpoints the last poll touched (0 before the first poll)."""
    with _endpoints_lock:
        return len(_endpoints)


# ---------------------------------------------------------------------------
# Human-readable formatting
# ---------------------------------------------------------------------------

def format_speed(tokens_per_second):
    """A tokens/second value the way the table shows it: ``1,234 tok/s`` or ``-``."""
    if tokens_per_second is None:
        return "-"
    try:
        value = float(tokens_per_second)
    except (TypeError, ValueError):
        return "-"
    if value >= 1000:
        return "{:,.0f} tok/s".format(value)
    if value >= 100:
        return "{:.0f} tok/s".format(value)
    return "{:.1f} tok/s".format(value)


def format_percent(percent):
    """A 0-100 percentage: ``63%``, or ``-`` when the server reports no gauge."""
    if percent is None:
        return "-"
    try:
        value = float(percent)
    except (TypeError, ValueError):
        return "-"
    return "{:.0f}%".format(value)


def format_count(count):
    """A request count: ``38``, or ``-`` when unknown."""
    if count is None:
        return "-"
    try:
        return "{:,.0f}".format(float(count))
    except (TypeError, ValueError):
        return "-"


def format_tokens(tokens):
    """A token total: ``1,234,567``, or ``-`` when unknown."""
    if tokens is None:
        return "-"
    try:
        return "{:,}".format(int(tokens))
    except (TypeError, ValueError):
        return "-"


def format_duration(seconds):
    """A duration the way the table shows it: ``820 ms``, ``2.4 s``, ``1m 12s``, or ``-``."""
    if seconds is None:
        return "-"
    try:
        value = float(seconds)
    except (TypeError, ValueError):
        return "-"
    if value < 0:
        return "-"
    if value < 1:
        return "{:.0f} ms".format(value * 1000.0)
    if value < 60:
        return "{:.1f} s".format(value)
    minutes, rest = divmod(value, 60.0)
    return "{:.0f}m {:.0f}s".format(minutes, rest)


def format_rate_per_minute(per_second):
    """A per-second rate shown per minute: ``18.0/min``, or ``-``.

    Bans and back-tracks are rare enough that a per-second figure is always a small fraction;
    per minute is the unit a user can actually read.
    """
    if per_second is None:
        return "-"
    try:
        value = float(per_second) * 60.0
    except (TypeError, ValueError):
        return "-"
    if value >= 100:
        return "{:,.0f}/min".format(value)
    return "{:.1f}/min".format(value)


def format_ratio(numerator, denominator):
    """``numerator / denominator`` as a percentage: ``62%``, or ``-`` when it cannot be told."""
    if numerator is None or denominator is None:
        return "-"
    try:
        top, bottom = float(numerator), float(denominator)
    except (TypeError, ValueError):
        return "-"
    if bottom <= 0:
        return "-"
    return "{:.0f}%".format(100.0 * top / bottom)


def describe_load(load_percent):
    """A one-word reading of a KV-cache load percentage, for the table's colour tag."""
    if load_percent is None:
        return "unknown"
    try:
        value = float(load_percent)
    except (TypeError, ValueError):
        return "unknown"
    if value >= 90:
        return "saturated"
    if value >= 60:
        return "busy"
    if value > 0:
        return "light"
    return "idle"
