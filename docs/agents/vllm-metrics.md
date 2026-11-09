<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# vLLM metrics (`vllm_metrics.py`) — the 📊 Metrics tab

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching `vllm_metrics.py`, the 📊 Metrics dashboard tab, the per-endpoint vLLM graph, or the per-slot token accounting that feeds it.

**Related:** [`docs/agents/generate-ui.md`](generate-ui.md) (the tab's widgets live there), [`docs/agents/dashboard.md`](dashboard.md) (the graph drawing helpers), [`docs/agents/core-modules.md`](core-modules.md)

## Contents

- `vllm_metrics.py`
- The four places that call `note_slot_usage()`
- `tests/test_vllm_metrics.py`
- `tests/test_metrics_tab_ui.py`

---

### `vllm_metrics.py` [v13.1.0]
**Purpose:** Read vLLM's Prometheus `/metrics` endpoint and turn it into the three figures the 📊 Metrics tab shows — per API endpoint, never aggregated
**Key:** `metrics_url_for_api_url()`, `parse_prometheus_text()`, `summarise()`, `compute_rates()`, `note_slot_usage()`, `endpoint_rows()`, `start_polling()` / `stop_polling()`, `format_speed()` / `format_percent()` / `format_count()` / `format_tokens()` / `describe_load()`
**Imports:** stdlib + `requests` only. **It must NOT import `generate_tk.py`, `generation.py`, `quality.py` or Tk** — `generation.py` and `quality.py` import IT, and the module has to stay importable headlessly so the tests need no display.

**Edit for:**

- **A new metric on the tab** → add it to `summarise()` (the counter/gauge), then to `endpoint_rows()` (the row dict) and to the table columns in `generate_tk.py`'s `elif tab_name == "📊 Metrics":` block. **Never aggregate across slots**: the tab exists so a user can see WHICH endpoint is slow, so a new figure is another per-slot column, not a total row.
- **A new endpoint source** → `_slot_urls()`. It reads `api.apis[0..5]` plus `quality.scoring_api.url` (slot 6, the judge, which has no `api.apis` entry). `_config_get()` accepts BOTH a `ConfigLoader` and a plain nested dict, so a caller can build a throwaway config without a live loader — do not replace it with a bare `config.get('a.b.c')`, which only works for the former.
- **How a speed is computed** → `compute_rates(current, previous, elapsed)`. Both speeds are tokens over the server's OWN timer counters (`request_prefill_time_seconds_sum`, `request_decode_time_seconds_sum`), never over wall-clock time, so concurrency cannot make them read faster than the hardware. Both come from the DELTA of two `summarise()` snapshots; a single snapshot reports `None` rather than a guess. `_delta()` returns `None` for a **negative** delta (a restarted server zeroed its counters), so that sample is dropped instead of drawn as a spike. **Keep that rule**: a restart otherwise produces a nonsense spike in the graph.
- **Which prompt counter is used** → `request_prefill_kv_computed_tokens_sum` first (tokens actually computed, prefix-cache hits excluded), then `prompt_tokens_by_source_total{source="local_compute"}`, then `prompt_tokens_total - prompt_tokens_cached_total`. The first is the honest "GPU work" figure and pairs with the prefill timer; the fallbacks exist only for older vLLM builds.
- **Endpoint load** → `vllm:kv_cache_usage_perc` as a percentage. It is vLLM's own saturation gauge (fraction of KV-cache blocks in use, 1.0 = full) and is what vLLM's scheduler is actually constrained by, which is why it is the load figure rather than a request count. `describe_load()` gives the table's colour tag (`idle`/`light`/`busy`/`saturated`).
- **Polling** → `poll_once()` fetches every DISTINCT URL once per cycle (`poll_endpoint()` per URL), so two slots on one server do not double the request rate. `start_polling()` is a daemon thread and is called from `generate_tk.py` next to `init_database_pool()`; `stop_polling()` is in the `__main__` `finally:`. **Importing this module must never start it** — a test, the CLI and a headless run import or build `generate_tk.py` and must issue no request. `_fetch_metrics()` catches every exception (a dead host, DNS failure and refused connection all mean "no figures here"), so one bad endpoint never stops the others.
- **The per-slot figure** → `note_slot_usage(slot_idx, usage, elapsed)`. vLLM's `/metrics` carries **no caller label**, so the server figures are identical for two slots on one server; a slot's OWN throughput can only come from the `usage` object of its own responses. That is why `endpoint_rows()` returns both: the server figures (shared, truthful) and `slot_tokens_per_second` (per slot). A body with no `usage` records the request and its time but **no tokens**, so the rate stays `-` rather than reading as 0.
- **Formatting** → the tab's strings come from `format_speed()` / `format_percent()` / `format_count()` / `format_tokens()`, not from inline f-strings at the call site. Add a new unit here.
- **History** → a bounded `HISTORY_LENGTH` ring per endpoint, one sample per poll. `reset_endpoints()` / `reset_slot_usage()` are called by `generate.reset_all_stats_and_history()`, so a new run starts from a clean baseline and no speed is computed across the gap between two runs.
- **The antislop-vllm-fork's own metrics (v13.3.0)** → the fork's `GET /metrics` concatenates the upstream vLLM body with its own `antislop:` counters, so one URL yields both. `is_antislop_metrics()` is a SEPARATE test from `is_vllm_metrics()`, and `poll_endpoint()` accepts a body with **either** prefix: a fork whose upstream vLLM is down still answers, so that row is reachable rather than `unreachable`. Only a body with **neither** prefix is "not a vLLM endpoint". `summarise_antislop()` returns `None` for every figure when the metric is absent, so a plain vLLM row carries no invented zeroes. `compute_antislop_rates()` divides the proxy's `generation_tokens_total` delta by the **wall clock** between polls, deliberately unlike `compute_rates()` (which divides by the server's decode timer): the gap between the two figures IS the proxy's overhead. **The four UI columns are opt-in** (`metrics.antislop_columns`, default `false`) — they are always created and hidden with `displaycolumns` by `generate._apply_metrics_columns()`, so a default run's tab is byte-identical to v13.2.1. `antislop_columns_enabled(config=None)` falls back to `app_state.global_config` like `_slot_urls()`.

**v13.4.0 — the full proxy set, the KPI strip and the extra panels.**

- **Read all twelve, not four.** `summarise_antislop()` also reads `requests_in_flight`, `request_duration_seconds` (sum + count), `streaming_requests_total`, `generations_total`, `prompt_tokens_total`, `backtracks_total`, `upstream_metrics_scrape_total` (both results) and the last-scrape timestamp, and keeps the **per-label series** (`requests_by_status`, `bans_by_validator`, `generations_by_finish`, `streaming_by_stream`) via `_series_by_label()` / `_bans_by_validator()`. Those per-label series are what a user tunes against — a validator with a large `total` and a small `fixed` is firing on things the model cannot avoid. `compute_antislop_rates()` adds `ban_rate`, `backtrack_rate` and `avg_request_seconds`, all from `_delta()` so a restarted proxy drops the sample instead of spiking.
- **The columns now show themselves.** `_apply_metrics_columns(tree, rows=None)` shows the antislop columns when **any** row reports `is_antislop`, whether or not the key is set. The key still decides for a plain vLLM, so a default run is unchanged. `rows=None` means "apply the config only" (the build path, before the first poll). **Do not remove this**: a shipped `config.yml` does not gain a new key automatically (`config_loader` does not merge defaults into an existing file), so without it a user who points a slot at the fork sees an empty-looking tab and concludes the feature is broken.
- **The KPI strip aggregates, and only the proxy.** `generate._metrics_totals()` sums the proxy's counters across rows **deduplicated by `metrics_url`** — two slots on one proxy report the same counters, so summing per row would double them. This is the ONE exception to "never aggregate across slots": the per-slot rule exists so a user can see *which* endpoint is slow, and a proxy-wide total answers a different question. Never aggregate the **vLLM server** figures this way.
- **The per-slot throughput ring is separate from the per-URL history.** `record_slot_history()` / `slot_history()` / `reset_slot_history()` keep one ring per slot, because two slots can share one URL and would otherwise overwrite each other's line in the slot graph. `poll_once()` calls `record_slot_history()` after polling (it is per-slot, and `poll_endpoint()` is per-URL). `generate.reset_all_stats_and_history()` calls `reset_slot_history()` with the other two resets.
- **The graph panels are in `dashboard.py`.** `update_vllm_slot_graph()`, `update_vllm_proxy_graph()` and `update_vllm_queue_graph()` each build their figure lazily via `_metric_panel()` and cache it on the canvas widget (`<attr>_fig` / `<attr>_axes` / `<attr>_canvas`), so a refresh reuses it instead of allocating a new matplotlib figure every poll — that churn is what drops the X connection over a long run. They place their axes with explicit rectangles, so they must **not** call `_fit_issue_graph()` (which runs `tight_layout()` and warns on explicitly-placed axes). Each keeps its own throttle dict, like `_vllm_graph_last_draw`.
- **`_rate_history()` turns a cumulative counter into a per-minute rate.** Used for the proxy's request counter. A negative delta yields `None` for that sample, so a restarted proxy draws a gap rather than a spike.
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
  `stop_polling()` forces a final one so closing the window loses nothing. `_round_floats(...)` rounds
  every float to 3 decimals and drops `null` keys, which halves the file; a dropped key reads back as
  `None`, so "this endpoint does not report that figure" stays distinguishable from "it reported zero".
  `load_snapshot()` returns 0 for a missing, corrupt or wrong-version file — a stale snapshot must never
  stop the app from starting.
- **The tab's refresh is gated on visibility; the COLLECTION is not.** `vllm_metrics` polls and saves on
  its own daemon thread whatever tab is on screen. Only the redraw is skipped while hidden (it is the
  expensive part) and `_on_dashboard_tab_changed()` forces one on the way in, so switching to the tab
  shows the history that accumulated while you were elsewhere. Do not move the gate onto the poll.
- **The graph redraw floor must stay below the poll interval.** `dashboard._VLLM_GRAPH_REDRAW_MIN_INTERVAL`
  is 1.0 s; at 5 s against a 2 s poll the graphs would drop two samples in three.
- **Formatting units.** `format_duration()`, `format_rate_per_minute()` and `format_ratio()` live in `vllm_metrics.py` with the other `format_*()` helpers. `format_ratio()` returns `-` for a zero denominator: "nothing has fired yet" is not "0%".

### The four `note_slot_usage()` call sites

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

### `tests/test_metrics_tab_ui.py`
**Purpose:** Display-dependent regression test for the 📊 Metrics dashboard tab (builds the REAL `generate_tk.py` UI; skips cleanly without a display)
**Key:** the tab is registered in `tabs_widgets` with `metrics_page` / `metrics_tree` / `metrics_graph_canvas` / `metrics_status_label`; it has no issue panels and did not swallow another tab's; its table uses `REGEX_TREE_STYLE` with a row height that fits the font and no column narrower than its heading; the three required figures each have a column; **one row per configured slot, never one aggregate row**, with two slots on one server still separate and a different load per server; an unreachable endpoint keeps its own row marked `unreachable`; the no-backend empty state; the status line names the poll interval; the graph draws **one line per endpoint** (two in the speed panel, one in the load panel); **the four antislop columns exist but are hidden by default, are shown when `metrics.antislop_columns` is on, and every row carries a value for every column** (a short row would silently shift every cell after it)
**Run:** `xvfb-run python -B tests/test_metrics_tab_ui.py` (or `DISPLAY=:99` with `Xvnc :99`)
**Edit for:** Add tab-behaviour cases; keep it free of source data, generation runs, network and model APIs

**v13.5.0 additions:** the poll interval is short enough to be usable and the timeout stays below
it; the history window is dense enough to read a trend; **one slow endpoint does not delay the
others** (fails on a serial `poll_once()`); saving is on by default and can be switched off; a poll
cycle writes the file; the history restores after a restart; a missing figure stays missing rather
than becoming zero; saving is throttled but can be forced; a corrupt or wrong-version file does not
stop startup; the document is rounded and drops nulls; `stop_polling()` writes a final snapshot; the
path follows `output/`; and `start_polling()` loads the saved history. The UI test adds: the poller
collects while the tab is closed, the status line names the save file and shows the real interval,
and the graph redraw floor is shorter than the poll interval.

**v13.4.0 additions:** the four KPI cards exist and are populated from the polled figures; they do **not** double-count two slots on one proxy; the antislop columns show themselves for a proxied slot even with the key off, and stay hidden for a plain vLLM; the table's two scrollbars are at least 16px wide (the theme's ~6px thumb is unusable); the graph stack's scrollbar is a real `tk.Scrollbar` at least 16px wide and the stack actually overflows with every panel laid out inside it; all four graph panels exist; and the slot / proxy / queue panels each draw the right number of lines.

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Add a metric to the 📊 Metrics tab | `vllm_metrics.py` (`summarise()`, `endpoint_rows()`, the `format_*()` helpers), `generate_tk.py` (`_update_metrics_tab()` — a column heading, a value, and the signature tuple), `docs/agents/vllm-metrics.md`, `tests/test_vllm_metrics.py` + `tests/test_metrics_tab_ui.py` |
| Add a new API endpoint source to the metrics poller | `vllm_metrics.py` (`_slot_urls()`, `SLOT_LABELS`), `tests/test_vllm_metrics.py` |
| Change how a vLLM speed or load is computed | `vllm_metrics.py` (`summarise()`, `compute_rates()`, `_delta()`), `tests/test_vllm_metrics.py` |
| Record a new slot's own token usage | `vllm_metrics.py` (`note_slot_usage()`), the request builder that owns it (`generation.py` or `quality.py`), `tests/test_vllm_metrics.py` |
| Add a metric from the antislop-vllm-fork proxy | the fork's `api_server/metrics.py` + `GET /metrics` first, then `vllm_metrics.py` (`summarise_antislop()`, `endpoint_rows()`), `generate_tk.py` (`METRICS_ANTISLOP_COLUMNS`, `_apply_metrics_columns()`, the `values=` tuples), `config/config.yml` (`metrics.antislop_columns`), `tests/test_vllm_metrics.py` + `tests/test_metrics_tab_ui.py` |
| Add a graph panel to the 📊 Metrics tab | `dashboard.py` (`_metric_panel()`, a new `update_vllm_*_graph()` with its own throttle dict), `generate_tk.py` (`_refresh_metrics_graphs()`, `_add_graph_panel()` in the tab build), `tests/test_metrics_tab_ui.py` |
| Add a figure to the KPI strip | `generate_tk.py` (`_metrics_totals()`, `_update_metrics_summary()`, `kpi_specs`), `vllm_metrics.py` (`summarise_antislop()` + a `format_*()` helper), `tests/test_vllm_metrics.py` + `tests/test_metrics_tab_ui.py` |
| Change the poll cadence, the timeout or the history length | `vllm_metrics.py` (`POLL_INTERVAL_SECONDS`, `REQUEST_TIMEOUT_SECONDS`, `HISTORY_LENGTH`, `SAVE_INTERVAL_SECONDS`), `dashboard.py` (`_VLLM_GRAPH_REDRAW_MIN_INTERVAL` must stay below the interval), `tests/test_vllm_metrics.py` + `tests/test_metrics_tab_ui.py` |
| Change what the saved history file contains | `vllm_metrics.py` (`_history_document()`, `_round_floats()`, `save_snapshot()`, `load_snapshot()`, and bump the document's `version`), `tests/test_vllm_metrics.py` |
