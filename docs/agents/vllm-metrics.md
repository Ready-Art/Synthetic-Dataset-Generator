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
**Imports:** stdlib + `requests` only. **It must NOT import `generate.py`, `generation.py`, `quality.py` or Tk** — `generation.py` and `quality.py` import IT, and the module has to stay importable headlessly so the tests need no display.

**Edit for:**

- **A new metric on the tab** → add it to `summarise()` (the counter/gauge), then to `endpoint_rows()` (the row dict) and to the table columns in `generate.py`'s `elif tab_name == "📊 Metrics":` block. **Never aggregate across slots**: the tab exists so a user can see WHICH endpoint is slow, so a new figure is another per-slot column, not a total row.
- **A new endpoint source** → `_slot_urls()`. It reads `api.apis[0..5]` plus `quality.scoring_api.url` (slot 6, the judge, which has no `api.apis` entry). `_config_get()` accepts BOTH a `ConfigLoader` and a plain nested dict, so a caller can build a throwaway config without a live loader — do not replace it with a bare `config.get('a.b.c')`, which only works for the former.
- **How a speed is computed** → `compute_rates(current, previous, elapsed)`. Both speeds are tokens over the server's OWN timer counters (`request_prefill_time_seconds_sum`, `request_decode_time_seconds_sum`), never over wall-clock time, so concurrency cannot make them read faster than the hardware. Both come from the DELTA of two `summarise()` snapshots; a single snapshot reports `None` rather than a guess. `_delta()` returns `None` for a **negative** delta (a restarted server zeroed its counters), so that sample is dropped instead of drawn as a spike. **Keep that rule**: a restart otherwise produces a nonsense spike in the graph.
- **Which prompt counter is used** → `request_prefill_kv_computed_tokens_sum` first (tokens actually computed, prefix-cache hits excluded), then `prompt_tokens_by_source_total{source="local_compute"}`, then `prompt_tokens_total - prompt_tokens_cached_total`. The first is the honest "GPU work" figure and pairs with the prefill timer; the fallbacks exist only for older vLLM builds.
- **Endpoint load** → `vllm:kv_cache_usage_perc` as a percentage. It is vLLM's own saturation gauge (fraction of KV-cache blocks in use, 1.0 = full) and is what vLLM's scheduler is actually constrained by, which is why it is the load figure rather than a request count. `describe_load()` gives the table's colour tag (`idle`/`light`/`busy`/`saturated`).
- **Polling** → `poll_once()` fetches every DISTINCT URL once per cycle (`poll_endpoint()` per URL), so two slots on one server do not double the request rate. `start_polling()` is a daemon thread and is called from `generate.py` next to `init_database_pool()`; `stop_polling()` is in the `__main__` `finally:`. **Importing this module must never start it** — a test, the CLI and a headless run import or build `generate.py` and must issue no request. `_fetch_metrics()` catches every exception (a dead host, DNS failure and refused connection all mean "no figures here"), so one bad endpoint never stops the others.
- **The per-slot figure** → `note_slot_usage(slot_idx, usage, elapsed)`. vLLM's `/metrics` carries **no caller label**, so the server figures are identical for two slots on one server; a slot's OWN throughput can only come from the `usage` object of its own responses. That is why `endpoint_rows()` returns both: the server figures (shared, truthful) and `slot_tokens_per_second` (per slot). A body with no `usage` records the request and its time but **no tokens**, so the rate stays `-` rather than reading as 0.
- **Formatting** → the tab's strings come from `format_speed()` / `format_percent()` / `format_count()` / `format_tokens()`, not from inline f-strings at the call site. Add a new unit here.
- **History** → a bounded `HISTORY_LENGTH` ring per endpoint, one sample per poll. `reset_endpoints()` / `reset_slot_usage()` are called by `generate.reset_all_stats_and_history()`, so a new run starts from a clean baseline and no speed is computed across the gap between two runs.
- **The antislop-vllm-fork's own metrics (v13.3.0)** → the fork's `GET /metrics` concatenates the upstream vLLM body with its own `antislop:` counters, so one URL yields both. `is_antislop_metrics()` is a SEPARATE test from `is_vllm_metrics()`, and `poll_endpoint()` accepts a body with **either** prefix: a fork whose upstream vLLM is down still answers, so that row is reachable rather than `unreachable`. Only a body with **neither** prefix is "not a vLLM endpoint". `summarise_antislop()` returns `None` for every figure when the metric is absent, so a plain vLLM row carries no invented zeroes. `compute_antislop_rates()` divides the proxy's `generation_tokens_total` delta by the **wall clock** between polls, deliberately unlike `compute_rates()` (which divides by the server's decode timer): the gap between the two figures IS the proxy's overhead. **The four UI columns are opt-in** (`metrics.antislop_columns`, default `false`) — they are always created and hidden with `displaycolumns` by `generate._apply_metrics_columns()`, so a default run's tab is byte-identical to v13.2.1. `antislop_columns_enabled(config=None)` falls back to `app_state.global_config` like `_slot_urls()`.

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
**Purpose:** Display-dependent regression test for the 📊 Metrics dashboard tab (builds the REAL `generate.py` UI; skips cleanly without a display)
**Key:** the tab is registered in `tabs_widgets` with `metrics_page` / `metrics_tree` / `metrics_graph_canvas` / `metrics_status_label`; it has no issue panels and did not swallow another tab's; its table uses `REGEX_TREE_STYLE` with a row height that fits the font and no column narrower than its heading; the three required figures each have a column; **one row per configured slot, never one aggregate row**, with two slots on one server still separate and a different load per server; an unreachable endpoint keeps its own row marked `unreachable`; the no-backend empty state; the status line names the poll interval; the graph draws **one line per endpoint** (two in the speed panel, one in the load panel); **the four antislop columns exist but are hidden by default, are shown when `metrics.antislop_columns` is on, and every row carries a value for every column** (a short row would silently shift every cell after it)
**Run:** `xvfb-run python -B tests/test_metrics_tab_ui.py` (or `DISPLAY=:99` with `Xvnc :99`)
**Edit for:** Add tab-behaviour cases; keep it free of source data, generation runs, network and model APIs

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Add a metric to the 📊 Metrics tab | `vllm_metrics.py` (`summarise()`, `endpoint_rows()`, the `format_*()` helpers), `generate.py` (`_update_metrics_tab()` — a column heading, a value, and the signature tuple), `docs/agents/vllm-metrics.md`, `tests/test_vllm_metrics.py` + `tests/test_metrics_tab_ui.py` |
| Add a new API endpoint source to the metrics poller | `vllm_metrics.py` (`_slot_urls()`, `SLOT_LABELS`), `tests/test_vllm_metrics.py` |
| Change how a vLLM speed or load is computed | `vllm_metrics.py` (`summarise()`, `compute_rates()`, `_delta()`), `tests/test_vllm_metrics.py` |
| Record a new slot's own token usage | `vllm_metrics.py` (`note_slot_usage()`), the request builder that owns it (`generation.py` or `quality.py`), `tests/test_vllm_metrics.py` |
| Add a metric from the antislop-vllm-fork proxy | the fork's `api_server/metrics.py` + `GET /metrics` first, then `vllm_metrics.py` (`summarise_antislop()`, `endpoint_rows()`), `generate.py` (`METRICS_ANTISLOP_COLUMNS`, `_apply_metrics_columns()`, the `values=` tuples), `config/config.yml` (`metrics.antislop_columns`), `tests/test_vllm_metrics.py` + `tests/test_metrics_tab_ui.py` |
