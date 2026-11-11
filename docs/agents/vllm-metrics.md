<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# vLLM metrics (`vllm_metrics.py`) — the 📊 Metrics tab

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching `vllm_metrics.py`, the 📊 Metrics tab, the per-endpoint vLLM graph, or the per-slot token accounting that feeds it.

**Related:** [`docs/agents/webapp.md`](webapp.md) (the routes and the browser modules), [`docs/agents/core-modules.md`](core-modules.md)

## Contents

- `vllm_metrics.py`
- The `note_slot_usage()` call sites
- `tests/test_vllm_metrics.py`
- `tests/test_web_tabs.py` and `tests/test_web_v151.py`

---

### `vllm_metrics.py` [v13.1.0]
**Purpose:** Read vLLM's Prometheus `/metrics` endpoint and turn it into the three figures the 📊 Metrics tab shows — per API endpoint, never aggregated
**Key:** `metrics_url_for_api_url()`, `parse_prometheus_text()`, `summarise()`, `compute_rates()`, `note_slot_usage()`, `endpoint_rows()`, `start_polling()` / `stop_polling()`, `format_speed()` / `format_percent()` / `format_count()` / `format_tokens()` / `describe_load()`
**Imports:** stdlib + `requests` only. **It must NOT import `generation.py`, `quality.py` or any GUI toolkit** — `generation.py` and `quality.py` import IT, and the module has to stay importable headlessly so the tests need no display.

**Edit for:**

- **A new metric on the tab** → add it to `summarise()` (the counter/gauge), then to `endpoint_rows()` (the row dict), then to the browser's row builder in `webapp/static/js/tabs.js` (`endpointTable()` for a column, `vllmServerPanels()` for a graph). **Never aggregate across slots**: the tab exists so a user can see WHICH endpoint is slow, so a new figure is another per-slot column, not a total row.
- **A new endpoint source** → `_slot_urls()`. It reads `api.apis[0..5]` plus `quality.scoring_api.url` (slot 6, the judge, which has no `api.apis` entry). `_config_get()` accepts BOTH a `ConfigLoader` and a plain nested dict, so a caller can build a throwaway config without a live loader — do not replace it with a bare `config.get('a.b.c')`, which only works for the former.
- **How a speed is computed** → `compute_rates(current, previous, elapsed)`. Both speeds are tokens over the server's OWN timer counters (`request_prefill_time_seconds_sum`, `request_decode_time_seconds_sum`), never over wall-clock time, so concurrency cannot make them read faster than the hardware. Both come from the DELTA of two `summarise()` snapshots; a single snapshot reports `None` rather than a guess. `_delta()` returns `None` for a **negative** delta (a restarted server zeroed its counters), so that sample is dropped instead of drawn as a spike. **Keep that rule**: a restart otherwise produces a nonsense spike in the graph.
- **Which prompt counter is used** → `request_prefill_kv_computed_tokens_sum` first (tokens actually computed, prefix-cache hits excluded), then `prompt_tokens_by_source_total{source="local_compute"}`, then `prompt_tokens_total - prompt_tokens_cached_total`. The first is the honest "GPU work" figure and pairs with the prefill timer; the fallbacks exist only for older vLLM builds.
- **Endpoint load** → `vllm:kv_cache_usage_perc` as a percentage. It is vLLM's own saturation gauge (fraction of KV-cache blocks in use, 1.0 = full) and is what vLLM's scheduler is actually constrained by, which is why it is the load figure rather than a request count. `describe_load()` gives the colour tag (`idle`/`light`/`busy`/`saturated`).
- **Polling** → `poll_once()` fetches every DISTINCT URL once per cycle (`poll_endpoint()` per URL), so two slots on one server do not double the request rate. `start_polling()` is a daemon thread and is called from `webapp/engine.py`'s `Engine.start()`, next to the socket start; `stop_polling()` is in `Engine.stop()`. **Importing this module must never start it** — a test and a bare CLI run import it and must issue no request. `_fetch_metrics()` catches every exception (a dead host, DNS failure and refused connection all mean "no figures here"), so one bad endpoint never stops the others.
- **The per-slot figure** → `note_slot_usage(slot_idx, usage, elapsed)`. vLLM's `/metrics` carries **no caller label**, so the server figures are identical for two slots on one server; a slot's OWN throughput can only come from the `usage` object of its own responses. That is why `endpoint_rows()` returns both: the server figures (shared, truthful) and `slot_tokens_per_second` (per slot). A body with no `usage` records the request and its time but **no tokens**, so the rate stays `-` rather than reading as 0.
- **Formatting** → the unit strings come from `format_speed()` / `format_percent()` / `format_count()` / `format_tokens()`, not from inline f-strings at a call site. Add a new unit here. The browser's table cells are formatted by `num()` in `webapp/static/js/dom.js`; a figure that needs a unit string is formatted on the server.
- **History** → a bounded `HISTORY_LENGTH` ring per endpoint, one sample per poll. `reset_endpoints()` / `reset_slot_usage()` / `reset_slot_history()` clear the rings; nothing calls them automatically today, so a fresh run has to call them or a speed is computed across the gap between two runs.
- **The antislop-vllm-fork's own metrics (v13.3.0)** → the fork's `GET /metrics` concatenates the upstream vLLM body with its own `antislop:` counters, so one URL yields both. `is_antislop_metrics()` is a SEPARATE test from `is_vllm_metrics()`, and `poll_endpoint()` accepts a body with **either** prefix: a fork whose upstream vLLM is down still answers, so that row is reachable rather than `unreachable`. Only a body with **neither** prefix is "not a vLLM endpoint". `summarise_antislop()` returns `None` for every figure when the metric is absent, so a plain vLLM row carries no invented zeroes. `compute_antislop_rates()` divides the proxy's `generation_tokens_total` delta by the **wall clock** between polls, deliberately unlike `compute_rates()` (which divides by the server's decode timer): the gap between the two figures IS the proxy's overhead. **The proxy's figures are opt-in** (`metrics.antislop_columns`, default `false`): `/api/metrics` reports `antislop_columns` from `antislop_columns_enabled()` and the tab offers the second toggle regardless, so a default run's tab is unchanged. `antislop_columns_enabled(config=None)` falls back to `app_state.global_config` like `_slot_urls()`.

**v13.4.0 — the full proxy set and the extra panels.**

- **Read all twelve, not four.** `summarise_antislop()` also reads `requests_in_flight`, `request_duration_seconds` (sum + count), `streaming_requests_total`, `generations_total`, `prompt_tokens_total`, `backtracks_total`, `upstream_metrics_scrape_total` (both results) and the last-scrape timestamp, and keeps the **per-label series** (`requests_by_status`, `bans_by_validator`, `generations_by_finish`, `streaming_by_stream`) via `_series_by_label()` / `_bans_by_validator()`. Those per-label series are what a user tunes against — a validator with a large `total` and a small `fixed` is firing on things the model cannot avoid. `compute_antislop_rates()` adds `ban_rate`, `backtrack_rate` and `avg_request_seconds`, all from `_delta()` so a restarted proxy drops the sample instead of spiking.
- **A proxy-wide total is the ONE exception to "never aggregate across slots".** The per-slot rule exists so a user can see *which* endpoint is slow; a proxy-wide total answers a different question, and it must be **deduplicated by `metrics_url`** because two slots on one proxy report the same counters — summing per row would double them. Never aggregate the **vLLM server** figures this way.
- **The per-slot throughput ring is separate from the per-URL history.** `record_slot_history()` / `slot_history()` / `reset_slot_history()` keep one ring per slot, because two slots can share one URL and would otherwise overwrite each other's line in the slot graph. `poll_once()` calls `record_slot_history()` after polling (it is per-slot, and `poll_endpoint()` is per-URL).
- **The graph panels are in the browser, not in this module.** `webapp/static/js/tabs.js` builds them (`slotThroughputPanels()`, `vllmServerPanels()`, `antislopPanels()`) and `webapp/static/js/chart.js`'s `lineChart()` draws them into a host element it measures itself. The chart reads the palette from the CSS custom properties, so it holds no hex literal and cannot drift from `webapp/theme.py`. Two rules are load-bearing: the viewBox width **is** the host's width in CSS pixels with `preserveAspectRatio="xMinYMin meet`, so one user unit is one pixel and axis text is never scaled horizontally; and a `ResizeObserver` redraws on a width change, because a fixed viewBox squashed every glyph and ran the axis labels together (v15.1.0). `lineChart()` fills a host that is already in the document — a detached node has no computed style, so the lines would all take the fallback colour.
- **`rateHistory()` turns a cumulative counter into a per-minute rate.** Used for the proxy's request counter. A negative delta yields `null` for that sample, so a restarted proxy draws a gap rather than a spike.
- **Poll cadence and concurrency (v13.5.0).** `POLL_INTERVAL_SECONDS` is **2 s** and
  `REQUEST_TIMEOUT_SECONDS` is **1.5 s** — the timeout MUST stay below the interval, or one dead host
  delays the next cycle as well as this one. `HISTORY_LENGTH` is **300** (10 minutes at 2 s).
  `poll_once()` fetches every endpoint **concurrently**, one short-lived thread each, then joins them:
  serially, a single slow host cost every later endpoint its sample for that cycle, and that skew is what
  made the rates wrong (a rate is a delta over an interval). **Do not make this serial again** —
  `test_one_slow_endpoint_does_not_delay_the_others` fails on a serial implementation, deliberately.
- **The history is saved (v13.5.0).** `save_snapshot()` writes `output/metrics_history.json` (path from
  `history_path()`, which reads `app_state.OUTPUT_DIR`), `load_snapshot()` restores it, and
  `start_polling()` calls `load_snapshot()` so the graphs open with the previous run's samples.
  **On by default** (`metrics.save_history`, `save_history_enabled()`): it writes gitignored local run
  data and changes no request, prompt or output row, so the "a default run stays byte-identical" rule is
  not in play. The write is **atomic** (temp file + `os.replace`) and **throttled** to
  `SAVE_INTERVAL_SECONDS` (30 s) — a few hundred KB every 2 s is pointless churn on an SD card — and
  `stop_polling()` forces a final one so stopping the app loses nothing. `_round_floats(...)` rounds
  every float to 3 decimals and drops `null` keys, which halves the file; a dropped key reads back as
  `None`, so "this endpoint does not report that figure" stays distinguishable from "it reported zero".
  `load_snapshot()` returns 0 for a missing, corrupt or wrong-version file — a stale snapshot must never
  stop the app from starting.
- **The tab's redraw is gated on being open; the COLLECTION is not.** `vllm_metrics` polls and saves on
  its own daemon thread whatever screen the browser is on. `webapp/static/js/tabs.js` fetches `/api/metrics`
  when the tab mounts, so switching to it shows the history that accumulated while you were elsewhere. Do not
  move the gate onto the poll.
- **The tab POLLS while it is open (v16.1.0).** `metricsTab` passes `interval: 2000` to `makeTab`, which
  re-fetches `/api/metrics` on that tick and puts an **Auto refresh** switch in the toolbar. Before that the
  tab only re-fetched on a click, so with the page open the figures froze: the per-slot ring is appended by
  the generation threads and the server/proxy rings by this module's poller, both on their own schedule, so a
  click-only tab showed a snapshot of a run that had already moved on. **The tab's interval must stay equal to
  `POLL_INTERVAL_SECONDS`** -- `tests/test_web_v161.py` asserts it against the constant, because polling
  slower than the poller means the page is permanently behind what it could already show. The repaint goes
  through `makeTab`'s scroll-preserving path, so a poll cannot throw the reader to the top of a graph stack.
- **Formatting units.** `format_duration()`, `format_rate_per_minute()` and `format_ratio()` live in `vllm_metrics.py` with the other `format_*()` helpers. `format_ratio()` returns `-` for a zero denominator: "nothing has fired yet" is not "0%".

### The `note_slot_usage()` call sites

One per request builder, in the branch where `response.status_code == 200` and the `usage` object has already been read. **A new builder that sends `messages` must call it too**, or that slot shows no throughput. They are:

1. `generation.py` — the question request (and the answer request, the continuation request, `rewrite_regex_watch_text()`, `call_slop_fixer_llm()`, `call_anti_slop_llm()`);
2. `quality.py` — `_call_logic_judge_once()`, which passes `scoring_api_slot` (index 6 unless `quality.scoring_api.slot_idx` points elsewhere).

The `api_response_time` passed to each call is the SAME duration the response-time tracker already records, so the two features cannot disagree on how long a request took. It is captured once into a local before the tracker is updated — do not recompute it, and do not move the call above the `status_code == 200` check (an error body has no `usage`).

---

### `tests/test_vllm_metrics.py`
**Purpose:** Headless regression test for the vLLM reader (no display, no config, no network: every HTTP call is stubbed)
**Key:** URL derivation (the API path is discarded, an unconfigured slot yields no row); the parser (escaped labels, comment lines, unparseable samples, absent-vs-zero); the speed maths (a single snapshot reports no speed, the delta matches the server timers, prefix-cache hits are excluded, the fallback ladder, a negative delta and an idle interval both report nothing); one row per slot with no aggregate row; per-slot throughput differing while the server figures agree; the judge as its own row; a shared URL fetched once per cycle; and that importing the module does not start the poller; **plus the v13.3.0 antislop half**: an antislop body is recognised, the proxy counters are summarised, a plain-vLLM body yields `None` antislop figures (backwards compatibility), a fork with a dead upstream is reachable rather than an error, a body with neither prefix is still an error, a fork row carries both sets of figures, and `metrics.antislop_columns` defaults off
**Run:** `python -B tests/test_vllm_metrics.py`
**Edit for:** Extend the fixtures (`FIRST_POLL` / `SECOND_POLL` / `ANTISLOP_FIRST` / `ANTISLOP_SECOND` / `ANTISLOP_ONLY`) when a new vLLM or antislop metric is read; keep it free of source data, generation runs, network and model APIs

---

### `tests/test_web_tabs.py` and `tests/test_web_v151.py`
**Purpose:** Headless regression tests for the 📊 Metrics tab's route and its browser module. Both drive `/api/metrics` through a fake `vllm_metrics`, so they need no display and no server.
**Key:** the route reports each slot with its own history and the server rows with both availability flags (`vllm_available`, `antislop_available`); it reports `antislop_available` when the fork answers and `antislop_columns` from the config; it never hands out the poller's own lists, and it copies the endpoint history so a tick cannot mutate what the page holds; the tab has **two independent toggles** (per-slot throughput on, server/proxy metrics off) and renders the server panels when switched on; the chart's viewBox is measured from its host, its gutter is sized to its labels, its axis ticks are whole numbers, and it redraws on a resize; and no module under `webapp/` carries a hex literal
**Run:** `python -B tests/test_web_tabs.py` and `python -B tests/test_web_v151.py`
**Edit for:** Add tab-behaviour cases; keep it free of source data, generation runs, network and model APIs

**v16.1.0 addition:** `tests/test_web_v161.py` pins the tab's own refresh -- that `metricsTab` carries an
`interval`, that the interval equals `vllm_metrics.POLL_INTERVAL_SECONDS * 1000`, that the toolbar switch
starts and stops the timer, and that a repaint preserves every keyed scroller.

**v13.5.0 additions:** the poll interval is short enough to be usable and the timeout stays below
it; the history window is dense enough to read a trend; **one slow endpoint does not delay the
others** (fails on a serial `poll_once()`); saving is on by default and can be switched off; a poll
cycle writes the file; the history restores after a restart; a missing figure stays missing rather
than becoming zero; saving is throttled but can be forced; a corrupt or wrong-version file does not
stop startup; the document is rounded and drops nulls; `stop_polling()` writes a final snapshot; the
path follows `output/`; and `start_polling()` loads the saved history.

**v13.4.0 additions:** the proxy-wide total does **not** double-count two slots on one proxy; the
antislop panels show themselves for a proxied slot even with the key off, and stay empty for a plain
vLLM; and the slot / proxy / queue panels each draw the right number of lines.

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Add a metric to the 📊 Metrics tab | `vllm_metrics.py` (`summarise()`, `endpoint_rows()`, the `format_*()` helpers), `webapp/tabs_routes.py` (the `/api/metrics` payload), `webapp/static/js/tabs.js` (`endpointTable()` — a heading and a value — or `vllmServerPanels()` for a graph), `docs/agents/vllm-metrics.md`, `tests/test_vllm_metrics.py` + `tests/test_web_tabs.py` |
| Add a new API endpoint source to the metrics poller | `vllm_metrics.py` (`_slot_urls()`, `SLOT_LABELS`), `tests/test_vllm_metrics.py` |
| Change how a vLLM speed or load is computed | `vllm_metrics.py` (`summarise()`, `compute_rates()`, `_delta()`), `tests/test_vllm_metrics.py` |
| Record a new slot's own token usage | `vllm_metrics.py` (`note_slot_usage()`), the request builder that owns it (`generation.py` or `quality.py`), `tests/test_vllm_metrics.py` |
| Add a metric from the antislop-vllm-fork proxy | the fork's `api_server/metrics.py` + `GET /metrics` first, then `vllm_metrics.py` (`summarise_antislop()`, `endpoint_rows()`), `webapp/tabs_routes.py` (the row keys the page reads), `webapp/static/js/tabs.js` (`antislopPanels()`), `webapp/config_schema.py` + `config/config.yml` (`metrics.antislop_columns`), `tests/test_vllm_metrics.py` + `tests/test_web_tabs.py` |
| Add a graph panel to the 📊 Metrics tab | `webapp/static/js/tabs.js` (a new panel builder), `webapp/static/js/chart.js` (`lineChart()` options), `webapp/tabs_routes.py` if it needs a new figure, `tests/test_web_tabs.py` |
| Add a figure to the proxy-wide total | `webapp/static/js/tabs.js` (the deduplicated sum), `vllm_metrics.py` (`summarise_antislop()` + a `format_*()` helper), `tests/test_vllm_metrics.py` + `tests/test_web_tabs.py` |
| Change the poll cadence, the timeout or the history length | `vllm_metrics.py` (`POLL_INTERVAL_SECONDS`, `REQUEST_TIMEOUT_SECONDS`, `HISTORY_LENGTH`, `SAVE_INTERVAL_SECONDS`), `tests/test_vllm_metrics.py` |
| Change what the saved history file contains | `vllm_metrics.py` (`_history_document()`, `_round_floats()`, `save_snapshot()`, `load_snapshot()`, and bump the document's `version`), `tests/test_vllm_metrics.py` |
