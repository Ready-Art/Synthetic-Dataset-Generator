# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless regression test for the 📊 Metrics tab's vLLM reader (v13.1.0).

Run with ``python -B tests/test_vllm_metrics.py``. Headless: no display, no configuration,
no network, no model and no running server. Every HTTP call is stubbed, so this test never
touches a real vLLM.

What this file pins
------------------

1. **The /metrics URL derivation.** vLLM serves Prometheus metrics on the same host and port as
   its OpenAI API at the root path, so the path of a configured API URL is discarded. A slot with
   no URL produces no row at all.
2. **The Prometheus parser.** Labels (including escaped quotes), comment lines, unparseable
   samples, and the absence of a metric (``None``) versus a genuine zero (``0.0``).
3. **The speed maths.** Prompt processing speed is computed prompt tokens over the server's own
   prefill time, and token generation speed is generation tokens over decode time. Both come from
   the DELTA of two snapshots, so a single snapshot reports no speed rather than a made-up one.
   A negative delta (a server restart) drops the sample instead of drawing a spike.
4. **Endpoint load** is ``vllm:kv_cache_usage_perc`` as a percentage, and it is NOT aggregated
   with anything else.
5. **One row per slot, never one aggregate row.** Two slots on the same server each get their own
   row, and their server figures agree (that is the truth: the KV cache is server-wide) while their
   own measured throughputs differ.
6. **Only vLLM counts.** A body with no ``vllm:`` metric is reported as "not a vLLM endpoint"
   and contributes no figures.
7. **The poller is not started by importing the module**, and one poll cycle fetches a shared URL
   once however many slots point at it.
"""

import io
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import vllm_metrics as vm

# --- fixtures ----------------------------------------------------------------

# A body shaped like the real one this feature was written against (see metrics.txt): the
# prefill/decode sums, the kv-computed token counter, the KV-cache gauge and the request counts.
FIRST_POLL = """\
# HELP python_gc_objects_collected_total Objects collected during gc
# TYPE python_gc_objects_collected_total counter
python_gc_objects_collected_total{generation="0"} 7.805968e+06
# HELP vllm:num_requests_running Number of requests in model execution batches.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{engine="0",model_name="acme/model"} 38.0
# HELP vllm:num_requests_waiting Number of requests waiting to be processed.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{engine="0",model_name="acme/model"} 2.0
# HELP vllm:kv_cache_usage_perc KV-cache usage. 1 means 100 percent usage.
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{engine="0",model_name="acme/model"} 0.5
# TYPE vllm:prompt_tokens_total counter
vllm:prompt_tokens_total{engine="0",model_name="acme/model"} 100000.0
# TYPE vllm:prompt_tokens_cached_total counter
vllm:prompt_tokens_cached_total{engine="0",model_name="acme/model"} 40000.0
# TYPE vllm:prompt_tokens_by_source_total counter
vllm:prompt_tokens_by_source_total{engine="0",model_name="acme/model",source="local_compute"} 60000.0
vllm:prompt_tokens_by_source_total{engine="0",model_name="acme/model",source="local_cache_hit"} 40000.0
# TYPE vllm:request_prefill_kv_computed_tokens_sum counter
vllm:request_prefill_kv_computed_tokens_sum{engine="0",model_name="acme/model"} 60000.0
# TYPE vllm:generation_tokens_total counter
vllm:generation_tokens_total{engine="0",model_name="acme/model"} 10000.0
# TYPE vllm:request_prefill_time_seconds_sum counter
vllm:request_prefill_time_seconds_sum{engine="0",model_name="acme/model"} 200.0
# TYPE vllm:request_decode_time_seconds_sum counter
vllm:request_decode_time_seconds_sum{engine="0",model_name="acme/model"} 100.0
# TYPE vllm:request_success_total counter
vllm:request_success_total{engine="0",finished_reason="stop",model_name="acme/model"} 500.0
vllm:request_success_total{engine="0",finished_reason="length",model_name="acme/model"} 5.0
"""

# The same server 10 seconds later: 30,000 more computed prompt tokens in 100 more prefill
# seconds (300 tok/s) and 2,000 more generation tokens in 50 more decode seconds (40 tok/s).
SECOND_POLL = """\
vllm:num_requests_running{engine="0",model_name="acme/model"} 12.0
vllm:num_requests_waiting{engine="0",model_name="acme/model"} 0.0
vllm:kv_cache_usage_perc{engine="0",model_name="acme/model"} 0.93
vllm:prompt_tokens_total{engine="0",model_name="acme/model"} 130000.0
vllm:prompt_tokens_cached_total{engine="0",model_name="acme/model"} 40000.0
vllm:request_prefill_kv_computed_tokens_sum{engine="0",model_name="acme/model"} 90000.0
vllm:generation_tokens_total{engine="0",model_name="acme/model"} 12000.0
vllm:request_prefill_time_seconds_sum{engine="0",model_name="acme/model"} 300.0
vllm:request_decode_time_seconds_sum{engine="0",model_name="acme/model"} 150.0
vllm:request_success_total{engine="0",finished_reason="stop",model_name="acme/model"} 520.0
"""

NOT_VLLM = """\
# HELP http_requests_total Total number of requests by method, status and handler.
# TYPE http_requests_total counter
http_requests_total{handler="/v1/models",method="GET",status="2xx"} 26.0
"""

# --- antislop-vllm-fork bodies (v13.3.0) -------------------------------------
# The fork's GET /metrics concatenates the upstream vLLM body with its own antislop: metrics, so a
# slot pointed at the fork produces BOTH prefixes in one body.  This fixture is the two halves of
# FIRST_POLL/SECOND_POLL plus the proxy counters, i.e. what the fork actually serves.
ANTISLOP_FIRST = FIRST_POLL + """\
# HELP antislop:requests_total Chat completion requests handled by this proxy, by endpoint and HTTP status.
# TYPE antislop:requests_total counter
antislop:requests_total{endpoint="/v1/chat/completions",status="200"} 40.0
# TYPE antislop:requests_in_flight gauge
antislop:requests_in_flight 2.0
# TYPE antislop:generations_total counter
antislop:generations_total{finish_reason="stop"} 38.0
antislop:generations_total{finish_reason="length"} 2.0
# TYPE antislop:generation_tokens_total counter
antislop:generation_tokens_total 9000.0
# TYPE antislop:prompt_tokens_total counter
antislop:prompt_tokens_total 95000.0
# TYPE antislop:ban_events_total counter
antislop:ban_events_total{validator="slop_phrase",fixed="true"} 30.0
antislop:ban_events_total{validator="regex",fixed="false"} 5.0
# TYPE antislop:upstream_errors_total counter
antislop:upstream_errors_total 1.0
# TYPE antislop:upstream_metrics_scrape_total counter
antislop:upstream_metrics_scrape_total{result="ok"} 1.0
"""

ANTISLOP_SECOND = SECOND_POLL + """\
# TYPE antislop:requests_total counter
antislop:requests_total{endpoint="/v1/chat/completions",status="200"} 60.0
# TYPE antislop:requests_in_flight gauge
antislop:requests_in_flight 1.0
# TYPE antislop:generations_total counter
antislop:generations_total{finish_reason="stop"} 58.0
antislop:generations_total{finish_reason="length"} 2.0
# TYPE antislop:generation_tokens_total counter
antislop:generation_tokens_total 11000.0
# TYPE antislop:prompt_tokens_total counter
antislop:prompt_tokens_total 125000.0
# TYPE antislop:ban_events_total counter
antislop:ban_events_total{validator="slop_phrase",fixed="true"} 50.0
antislop:ban_events_total{validator="regex",fixed="false"} 7.0
# TYPE antislop:upstream_errors_total counter
antislop:upstream_errors_total 2.0
"""

# A fork whose upstream vLLM is DOWN: only the antislop: half is served, and the scrape counter
# says so.  This is a REACHABLE endpoint, not an error.
ANTISLOP_ONLY = """\
# TYPE antislop:requests_total counter
antislop:requests_total{endpoint="/v1/chat/completions",status="200"} 12.0
# TYPE antislop:requests_in_flight gauge
antislop:requests_in_flight 0.0
# TYPE antislop:generation_tokens_total counter
antislop:generation_tokens_total 3000.0
# TYPE antislop:ban_events_total counter
antislop:ban_events_total{validator="slop_phrase",fixed="true"} 9.0
# TYPE antislop:upstream_errors_total counter
antislop:upstream_errors_total 4.0
# TYPE antislop:upstream_metrics_scrape_total counter
antislop:upstream_metrics_scrape_total{result="error"} 12.0
"""


class _StubResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code


class _StubRequests:
    """Stand-in for the ``requests`` module: records calls, serves canned bodies."""

    def __init__(self, bodies=None, error=None, status_code=200):
        # A body may be a single string (served for every call) or a list of strings served in
        # order, which is how a test simulates two successive snapshots of one server.
        self.bodies = dict(bodies or {})
        self.error = error
        self.status_code = status_code
        self.calls = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        if self.error is not None:
            raise self.error
        body = self.bodies.get(url, "")
        if isinstance(body, list):
            # The last entry repeats once the sequence is exhausted.
            index = min(self.calls.count(url) - 1, len(body) - 1)
            body = body[index] if body else ""
        return _StubResponse(body, self.status_code)


def _with_stub(stub):
    """Install ``stub`` as ``vllm_metrics.requests`` for the duration of a ``with`` block."""
    class _Ctx:
        def __enter__(self):
            self._saved = vm.requests
            vm.requests = stub
            return stub

        def __exit__(self, *exc):
            vm.requests = self._saved
            return False
    return _Ctx()


def _reset():
    vm.reset_endpoints()
    vm.reset_slot_usage()


# --- 1. URL derivation -------------------------------------------------------

def test_metrics_url_discards_the_api_path():
    assert vm.metrics_url_for_api_url("http://10.0.0.5:8000/v1/chat/completions") == \
        "http://10.0.0.5:8000/metrics"
    assert vm.metrics_url_for_api_url("https://host.example/v1") == \
        "https://host.example/metrics"
    assert vm.metrics_url_for_api_url("http://host:8000") == "http://host:8000/metrics"
    # A bare host:port from a hand-edited config is assumed to be http.
    assert vm.metrics_url_for_api_url("localhost:8000") == "http://localhost:8000/metrics"


def test_metrics_url_is_none_for_an_unconfigured_slot():
    assert vm.metrics_url_for_api_url("") is None
    assert vm.metrics_url_for_api_url(None) is None
    assert vm.metrics_url_for_api_url("   ") is None
    assert vm.metrics_url_for_api_url("not a url") is None


# --- 2. Prometheus parsing ---------------------------------------------------

def test_parser_ignores_comments_and_keeps_labels():
    samples = vm.parse_prometheus_text(FIRST_POLL)
    assert not any(name.startswith("#") for name in samples)
    entries = samples["vllm:request_success_total"]
    reasons = sorted(labels["finished_reason"] for labels, _value in entries)
    assert reasons == ["length", "stop"], reasons


def test_parser_handles_escaped_label_values():
    body = 'vllm:thing_total{note="a \\"quoted\\" word",engine="0"} 3.0\n'
    samples = vm.parse_prometheus_text(body)
    labels, value = samples["vllm:thing_total"][0]
    assert labels["note"] == 'a "quoted" word', labels
    assert labels["engine"] == "0"
    assert value == 3.0


def test_parser_skips_an_unparseable_sample_instead_of_raising():
    body = "vllm:good_total 1.0\nvllm:bad_total not_a_number\n"
    samples = vm.parse_prometheus_text(body)
    assert samples["vllm:good_total"][0][1] == 1.0
    assert "vllm:bad_total" not in samples


def test_missing_metric_is_none_but_a_zero_is_zero():
    samples = vm.parse_prometheus_text("vllm:kv_cache_usage_perc 0.0\n")
    assert vm._sum_series(samples, "vllm:absent_total") is None
    assert vm._sum_series(samples, "vllm:kv_cache_usage_perc") == 0.0


def test_only_a_body_with_a_vllm_metric_counts():
    assert vm.is_vllm_metrics(vm.parse_prometheus_text(FIRST_POLL)) is True
    assert vm.is_vllm_metrics(vm.parse_prometheus_text(NOT_VLLM)) is False


# --- 3. Speed maths ---------------------------------------------------------

def test_a_single_snapshot_reports_no_speed():
    """One snapshot has no delta, so there is no rate to report -- never a guess."""
    current = vm.summarise(vm.parse_prometheus_text(FIRST_POLL))
    rates = vm.compute_rates(current, None, None)
    assert rates["prompt_tps"] is None, rates
    assert rates["generation_tps"] is None, rates


def test_prompt_and_generation_speeds_come_from_the_server_timers():
    first = vm.summarise(vm.parse_prometheus_text(FIRST_POLL))
    second = vm.summarise(vm.parse_prometheus_text(SECOND_POLL))
    rates = vm.compute_rates(second, first, 10.0)
    # 30000 computed prompt tokens over 100 s of prefill time.
    assert abs(rates["prompt_tps"] - 300.0) < 1e-6, rates
    # 2000 generation tokens over 50 s of decode time.
    assert abs(rates["generation_tps"] - 40.0) < 1e-6, rates


def test_prompt_speed_excludes_prefix_cache_hits():
    """The prompt figure must be GPU work, not tokens served from the prefix cache."""
    first = vm.summarise(vm.parse_prometheus_text(FIRST_POLL))
    # prompt_tokens_total - prompt_tokens_cached_total would be 30000 here too, but the
    # kv-computed counter is what is used when it exists, so the value must match IT.
    assert first["prompt_compute_tokens"] == 60000.0, first


def test_prompt_speed_falls_back_when_the_kv_counter_is_absent():
    body = FIRST_POLL.replace(
        "vllm:request_prefill_kv_computed_tokens_sum", "vllm:unrelated_counter_total")
    summary = vm.summarise(vm.parse_prometheus_text(body))
    # Falls back to prompt_tokens_by_source_total{source="local_compute"}.
    assert summary["prompt_compute_tokens"] == 60000.0, summary


def test_a_negative_delta_drops_the_sample():
    """A restarted server zeroes its counters; that pair is not a rate."""
    first = vm.summarise(vm.parse_prometheus_text(FIRST_POLL))
    restarted = dict(first)
    restarted["generation_tokens"] = 5.0
    rates = vm.compute_rates(restarted, first, 10.0)
    assert rates["generation_tps"] is None, rates


def test_no_work_in_the_interval_reports_no_speed():
    first = vm.summarise(vm.parse_prometheus_text(FIRST_POLL))
    rates = vm.compute_rates(first, first, 10.0)
    assert rates["prompt_tps"] is None, rates
    assert rates["generation_tps"] is None, rates


# --- 4/5. Polling, per-slot rows ------------------------------------------

def _config(urls, judge_url=""):
    """A minimal stand-in for ``global_config`` with the given slot URLs."""
    apis = [{"url": urls.get(i, ""), "model": "m%d" % i} for i in range(6)]
    return {
        "api": {"apis": apis},
        "quality": {"scoring_api": {"url": judge_url}},
    }


def test_poll_records_a_baseline_then_a_rate():
    _reset()
    stub = _StubRequests({
        "http://h:8000/metrics": [FIRST_POLL, SECOND_POLL],
    })
    with _with_stub(stub):
        vm.poll_endpoint("http://h:8000/metrics", now=1000.0)
        state = vm.poll_endpoint("http://h:8000/metrics", now=1010.0)
    assert state["error"] is None, state
    assert state["model"] == "acme/model"
    history = state["history"]
    assert len(history) == 2, history
    # The first poll only establishes a baseline; the second one has a delta to report.
    assert history[0]["generation_tps"] is None, history
    assert abs(history[1]["generation_tps"] - 40.0) < 1e-6, history
    assert abs(history[1]["prompt_tps"] - 300.0) < 1e-6, history
    assert history[1]["load_percent"] == 93.0, history


def test_a_non_vllm_endpoint_is_reported_as_such():
    _reset()
    stub = _StubRequests({"http://h:8000/metrics": NOT_VLLM})
    with _with_stub(stub):
        state = vm.poll_endpoint("http://h:8000/metrics", now=1000.0)
    assert "not a vLLM" in (state["error"] or ""), state


def test_an_unreachable_endpoint_records_the_error_without_raising():
    _reset()
    stub = _StubRequests(error=IOError("connection refused"))
    with _with_stub(stub):
        state = vm.poll_endpoint("http://h:8000/metrics", now=1000.0)
    assert "connection refused" in (state["error"] or ""), state


def test_one_poll_fetches_a_shared_url_once():
    """Two slots on one server must not double the request rate."""
    _reset()
    config = _config({0: "http://h:8000/v1", 1: "http://h:8000/v1/chat/completions"})
    stub = _StubRequests({"http://h:8000/metrics": FIRST_POLL})
    with _with_stub(stub):
        urls = vm.poll_once(config)
    assert urls == ["http://h:8000/metrics"], urls
    assert stub.calls == ["http://h:8000/metrics"], stub.calls


def test_every_slot_gets_its_own_row_never_one_aggregate():
    _reset()
    config = _config({
        0: "http://h:8000/v1", 1: "http://h:8000/v1",
        4: "http://other:9000/v1",
    })
    stub = _StubRequests({
        "http://h:8000/metrics": SECOND_POLL,
        "http://other:9000/metrics": SECOND_POLL,
    })
    with _with_stub(stub):
        vm.poll_once(config)
        rows = vm.endpoint_rows(config)
    labels = [row["label"] for row in rows]
    assert labels == ["API 1", "API 2", "API 5 (Slop Fixer)"], labels
    # Same server -> identical server figures (the KV cache is server-wide) ...
    assert rows[0]["load_percent"] == rows[1]["load_percent"]
    assert rows[0]["generation_tps"] == rows[1]["generation_tps"]
    # ... but the slots are still separate rows, not one summed row.
    assert len(rows) == 3, rows


def test_slot_throughput_is_measured_per_slot():
    """The per-slot figure comes from that slot's OWN responses, not the server."""
    _reset()
    config = _config({0: "http://h:8000/v1", 1: "http://h:8000/v1"})
    stub = _StubRequests({"http://h:8000/metrics": FIRST_POLL})
    with _with_stub(stub):
        vm.poll_once(config)
        vm.note_slot_usage(0, {"prompt_tokens": 100, "completion_tokens": 100}, 2.0)
        vm.note_slot_usage(1, {"prompt_tokens": 1000, "completion_tokens": 1000}, 4.0)
        rows = vm.endpoint_rows(config)
    assert rows[0]["slot_tokens_per_second"] == 100.0, rows[0]
    assert rows[1]["slot_tokens_per_second"] == 500.0, rows[1]
    # The SERVER figures are still shared, which is why the two differ only here.
    assert rows[0]["generation_tps"] == rows[1]["generation_tps"]


def test_usage_without_tokens_records_the_request_but_invents_no_rate():
    _reset()
    vm.note_slot_usage(0, {}, 2.0)
    usage = vm.slot_usage(0)
    assert usage["requests"] == 1, usage
    assert usage["total_tokens"] == 0, usage
    assert usage["tokens_per_second"] is None, usage


def test_an_unconfigured_slot_produces_no_row():
    _reset()
    config = _config({0: "", 1: "http://h:8000/v1"})
    stub = _StubRequests({"http://h:8000/metrics": FIRST_POLL})
    with _with_stub(stub):
        vm.poll_once(config)
        rows = vm.endpoint_rows(config)
    assert [row["slot_idx"] for row in rows] == [1], rows


def test_the_judge_slot_is_its_own_row():
    _reset()
    config = _config({0: "http://main:8000/v1"}, judge_url="http://judge:8001/v1")
    stub = _StubRequests({
        "http://main:8000/metrics": FIRST_POLL,
        "http://judge:8001/metrics": FIRST_POLL,
    })
    with _with_stub(stub):
        vm.poll_once(config)
        rows = vm.endpoint_rows(config)
    assert [row["slot_idx"] for row in rows] == [0, 6], rows
    assert rows[1]["label"] == "Judge (Quality)", rows[1]


# --- 6. The antislop-vllm-fork's own metrics (v13.3.0) ----------------------
# The fork's GET /metrics concatenates the upstream vLLM body with its own antislop: metrics.
# These checks pin the OPTIONAL half: a plain vLLM body must behave exactly as before.

def test_an_antislop_body_is_recognised():
    assert vm.is_antislop_metrics(vm.parse_prometheus_text(ANTISLOP_FIRST)) is True
    assert vm.is_antislop_metrics(vm.parse_prometheus_text(ANTISLOP_ONLY)) is True
    # A plain vLLM body has no antislop: metric, and must not be mistaken for one.
    assert vm.is_antislop_metrics(vm.parse_prometheus_text(FIRST_POLL)) is False
    assert vm.is_antislop_metrics(vm.parse_prometheus_text(NOT_VLLM)) is False


def test_the_antislop_summary_reads_the_proxy_counters():
    summary = vm.summarise_antislop(vm.parse_prometheus_text(ANTISLOP_FIRST))
    assert summary["requests"] == 40.0, summary
    assert summary["in_flight"] == 2.0, summary
    assert summary["generations"] == 40.0, summary
    assert summary["generation_tokens"] == 9000.0, summary
    assert summary["prompt_tokens"] == 95000.0, summary
    # Bans are split by outcome, so a list that fires but never fixes is visible.
    assert summary["ban_events"] == 35.0, summary
    assert summary["ban_fixed"] == 30.0, summary
    assert summary["upstream_errors"] == 1.0, summary
    # No result="error" sample in this body, so the figure is ABSENT, not zero: a zero would
    # read as "scrapes are failing zero times", which is a different claim.
    assert summary["scrape_errors"] is None, summary


def test_the_antislop_summary_is_none_for_a_plain_vllm_body():
    """Backwards compatibility: a vLLM-only body must yield no antislop figures at all."""
    summary = vm.summarise_antislop(vm.parse_prometheus_text(FIRST_POLL))
    assert summary["requests"] is None, summary
    assert summary["ban_events"] is None, summary
    assert summary["generation_tokens"] is None, summary


def test_the_antislop_scrape_errors_are_counted():
    summary = vm.summarise_antislop(vm.parse_prometheus_text(ANTISLOP_ONLY))
    assert summary["scrape_errors"] == 12.0, summary
    assert summary["upstream_errors"] == 4.0, summary


def test_the_proxy_generation_speed_comes_from_two_snapshots():
    first = vm.summarise_antislop(vm.parse_prometheus_text(ANTISLOP_FIRST))
    second = vm.summarise_antislop(vm.parse_prometheus_text(ANTISLOP_SECOND))
    rates = vm.compute_antislop_rates(second, first, 10.0)
    # 2000 more generation tokens over 10 s of wall clock.
    assert abs(rates["generation_tps"] - 200.0) < 1e-6, rates


def test_a_single_antislop_snapshot_reports_no_speed():
    current = vm.summarise_antislop(vm.parse_prometheus_text(ANTISLOP_FIRST))
    rates = vm.compute_antislop_rates(current, None, None)
    assert rates["generation_tps"] is None, rates


def test_a_restarted_proxy_drops_the_antislop_sample():
    first = vm.summarise_antislop(vm.parse_prometheus_text(ANTISLOP_FIRST))
    restarted = dict(first)
    restarted["generation_tokens"] = 5.0
    rates = vm.compute_antislop_rates(restarted, first, 10.0)
    assert rates["generation_tps"] is None, rates


def test_a_fork_with_a_dead_upstream_is_reachable_not_an_error():
    """The whole point: the proxy answers, so the row must not read "unreachable"."""
    _reset()
    stub = _StubRequests({"http://h:8000/metrics": ANTISLOP_ONLY})
    with _with_stub(stub):
        state = vm.poll_endpoint("http://h:8000/metrics", now=1000.0)
    assert state["error"] is None, state
    assert state["last"]["antislop"]["requests"] == 12.0, state["last"]
    # The vLLM figures are absent, not invented.
    assert state["last"]["generation_tps"] is None, state["last"]


def test_a_body_with_neither_prefix_is_still_an_error():
    _reset()
    stub = _StubRequests({"http://h:8000/metrics": NOT_VLLM})
    with _with_stub(stub):
        state = vm.poll_endpoint("http://h:8000/metrics", now=1000.0)
    assert "not a vLLM" in (state["error"] or ""), state


def test_a_fork_row_carries_both_sets_of_figures():
    _reset()
    config = _config({0: "http://h:8000/v1"})
    stub = _StubRequests({"http://h:8000/metrics": [ANTISLOP_FIRST, ANTISLOP_SECOND]})
    with _with_stub(stub):
        vm.poll_once(config)
        vm.poll_once(config)
        rows = vm.endpoint_rows(config)
    row = rows[0]
    assert row["is_antislop"] is True, row
    # The vLLM figures still arrive, unchanged (backwards compatibility).
    assert row["generation_tps"] is not None, row
    assert row["load_percent"] is not None, row
    # ... and so do the proxy's own.
    assert row["antislop_requests"] == 60.0, row
    assert row["antislop_ban_events"] == 57.0, row
    assert row["antislop_ban_fixed"] == 50.0, row
    assert row["antislop_upstream_errors"] == 2.0, row
    assert row["antislop_generation_tps"] is not None, row


def test_a_plain_vllm_row_has_no_antislop_figures():
    """Backwards compatibility: a vLLM-only server's row is exactly what it was."""
    _reset()
    config = _config({0: "http://h:8000/v1"})
    stub = _StubRequests({"http://h:8000/metrics": FIRST_POLL})
    with _with_stub(stub):
        vm.poll_once(config)
        rows = vm.endpoint_rows(config)
    row = rows[0]
    assert row["is_antislop"] is False, row
    assert row["antislop_requests"] is None, row
    assert row["antislop_ban_events"] is None, row
    assert row["antislop_generation_tps"] is None, row


def test_the_antislop_columns_are_off_by_default():
    """A default run must stay byte-identical, so the columns ship hidden."""
    assert vm.antislop_columns_enabled({}) is False
    assert vm.antislop_columns_enabled({"metrics": {}}) is False
    assert vm.antislop_columns_enabled({"metrics": {"antislop_columns": False}}) is False


def test_the_antislop_columns_can_be_switched_on():
    assert vm.antislop_columns_enabled({"metrics": {"antislop_columns": True}}) is True


# --- 7. Formatting ---------------------------------------------------------

def test_formatting_is_human_readable():
    assert vm.format_speed(1234.5) == "1,234 tok/s"
    assert vm.format_speed(203.44) == "203 tok/s"
    assert vm.format_speed(40.0) == "40.0 tok/s"
    assert vm.format_speed(None) == "-"
    assert vm.format_percent(63.5) == "64%"
    assert vm.format_percent(None) == "-"
    assert vm.format_count(38.0) == "38"
    assert vm.format_tokens(1234567) == "1,234,567"
    assert vm.format_tokens(None) == "-"


def test_load_reading_reflects_saturation():
    assert vm.describe_load(0.0) == "idle"
    assert vm.describe_load(12.0) == "light"
    assert vm.describe_load(63.5) == "busy"
    assert vm.describe_load(93.0) == "saturated"
    assert vm.describe_load(None) == "unknown"


# --- 7. The poller is opt-in ------------------------------------------------

def test_importing_the_module_does_not_start_the_poller():
    """A test, the CLI and a headless run must never poll a server."""
    assert vm._poller_thread is None, vm._poller_thread


def test_start_and_stop_polling_are_safe():
    vm.stop_polling()  # stop with nothing running
    thread = vm.start_polling(interval=0.05)
    assert thread.is_alive()
    assert vm.start_polling(interval=0.05) is thread, "start_polling must be idempotent"
    vm.stop_polling()
    assert vm._poller_thread is None


# --- the module is in the undefined-globals sweep -----------------------------

def test_module_is_checked_by_the_undefined_globals_test():
    with io.open(os.path.join(ROOT, "tests", "test_undefined_globals.py"),
                  encoding="utf-8") as handle:
        source = handle.read()
    assert '"vllm_metrics.py"' in source, \
        "add vllm_metrics.py to test_undefined_globals.CHECKED_MODULES"


def main():
    failures = []
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith("test_") and callable(obj)]
    for name, test in tests:
        try:
            test()
        except AssertionError as exc:
            failures.append("%s: %s" % (name, exc))
        except Exception as exc:  # noqa: BLE001 - report any failure, keep going
            failures.append("%s: %r" % (name, exc))
    if failures:
        print("FAILED ({} of {}):".format(len(failures), len(tests)))
        for failure in failures:
            print("  " + failure)
        return 1
    print("OK: {} checks passed".format(len(tests)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
