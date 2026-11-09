<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 📊 Performance Tips

- **Increase threads per API** for higher throughput (watch rate limits — the rate limit status indicators show real-time usage)
- **Enable Valkey caching** to avoid redundant API calls for identical prompts
- **Use PostgreSQL** for large datasets — it's more efficient than appending to JSONL
- **Disable unused detection** features to reduce API calls (e.g., `no_user_impersonation: true`)
- **Lower `max_attempts`** if you prefer faster generation over quality
- **Use collaborative mode** with multiple APIs for maximum throughput
- **Use duplication mode** when you need diverse responses from different models for the same prompts
- **Set a budget limit** to prevent unexpected API costs during long runs
- **Test API connections** in the Config screen before starting a generation run
- **Test Valkey connectivity** from the Overview screen to ensure caching is working
- **Use the Prompts screen** to verify your prompt templates are working correctly before committing to a full run
- **Pause and adjust** rate limits mid-run — configuration is reloaded when you resume
- **Monitor circuit breaker** — if an API slot's circuit opens frequently, the endpoint may be experiencing issues
- **Use Force Recovery** if you need to resume with intentionally changed settings, but be aware of potential incompatibilities
- **Configure anti-slop** — Set up API Slot 6 and populate `anti_slop.phrases` for secondary quality filtering

> **⚠️ Read this first:** the built-in slop/anti-slop path is the *expensive* option — every detected phrase
> costs an extra full LLM round trip through the fixers, and it is **off by default since v12.7.6**
> (`detection.slop.enabled` / `detection.anti_slop.enabled` ship `false`). Moving anti-slop to
> [AntiSlop-vLLM](https://github.com/Ready-Art/antislop-vllm-fork/tree/dev) — the **`dev` branch, not
> `main`** — removes the fixer round trips entirely, because it bans the token during generation instead of
> rewriting the text afterwards. See [Slop & Anti-Slop](slop-and-antislop.md).

- **Use multi-character mode** — Set `num_characters` > 1 for richer, multi-party dialogues
- **Use emotional states** — Enable `emotional_states` to add tonal variety to conversations
- **Use emotional arcs** — Replace flat states with progressions (`calm -> wary -> tense`) so the emotional register shifts across the conversation instead of being one static mood; pair with `generation.num_turns` ≥ the number of stages so every stage has a turn
- **Lean on the persona/personality split** — Keep `persona` (the private self, shown only after `persona_breakthrough_turn`) distinct from `personality` (the outward mask, always shown) so early turns read as surface behavior and later turns reveal the interior
- **Add character traits** — Use the `traits` field in character entries to add more depth to character personas
- **Enable slop → anti-slop fallback** — Set `slop_to_anti_slop_fallback: true` to use the Anti-Slop Fixer as a final attempt when the Slop Fixer fails
- **Toggle conversation history for fixers** — Set `generation.use_conversation_history_for_fixers: false` to disable passing conversation context to slop/anti-slop fixer LLMs (reduces token usage but may affect coherence)
- **Adjust malformed response thresholds** — Tune `max_newlines_malformed` and `max_text_length_malformed` to catch poorly formatted LLM output without rejecting valid responses
- **Turn thinking off for the fixers and the judge — this is usually the single biggest win on a reasoning model** — Set **Thinking Mode** in the Config screen's `samplers` section to `"disable"`. Since v13.0.4 that reaches the Slop Fixer, the Anti-Slop Fixer and the Quality Judge too (they used to send no `chat_template_kwargs` at all, so the server default applied and they thought on every request). Those are short, structured jobs — one rewritten passage, one small JSON verdict — so the chain of thought is pure cost, and on a local vLLM it dominated their wall-clock time; users have reported roughly a **5x** overall generation speedup from switching it off. If you want the main replies to keep thinking, leave the main setting alone and set the three per-request overrides instead: **Thinking Mode (Slop Fixer)** and **Thinking Mode (Anti-Slop)** under `samplers`, and **Judge Thinking Mode** under `quality`
- **Watch the 📊 Metrics screen when a run feels slow** — it reads every vLLM backend's own `/metrics` endpoint and shows **prompt-processing speed**, **token-generation speed** and **endpoint load** per API endpoint, so you can tell *which* slot is the bottleneck and whether it is prefill (a low prompt speed), decode (a low generation speed) or saturation (endpoint load near 100%, with requests waiting). Nothing is aggregated, so a slow slot cannot hide behind fast ones. If load is high and requests are waiting, lower that slot's threads or its rate limit rather than adding more; if load is low and the speeds are low, the model itself is the limit
- **The metrics history is kept even when the tab is closed** — since v13.5.0 the poller runs every
  **2 s** whether or not you are looking at the 📊 Metrics screen, and writes the history to
  `output/metrics_history.json` every 30 s. So when a run slows down you can open the tab (or read the
  file afterwards) and see what the endpoints were doing at the time, instead of having had to be
  watching. Endpoints are fetched **concurrently**, so one dead host no longer delays every other
  endpoint's sample and skews its rate
- **Read the KPI strip before the table** — since v13.4.0 the 📊 Metrics screen opens with four cards: **⚡ Proxy Throughput**, **🔥 Busiest Endpoint** (the worst KV-cache load, colour-coded), **🛡️ Bans Fired** with the percentage the back-track fixed, and **⚠️ Errors** (upstream failures + non-2xx requests + unreachable endpoints). If **Errors** is climbing, stop tuning speed and read the failing slot's row; a proxy that is returning 500s is not slow, it is broken
- **Use the queue panel to tell "slow" from "queueing"** — the **Requests Running vs Waiting** graph draws both counts per server. A `waiting` line that climbs while `running` stays flat means the server is queueing: the bottleneck is admission, so lower that slot's threads or its rate limit. A flat `waiting` line with a low `running` count means the GPU is simply not being fed, which is a different fix
- **Compare the two generation speeds** — the table has the vLLM **Generation Speed** (tokens ÷ the server's own decode timer, i.e. the GPU) and the proxy's **Proxy Gen Speed** (tokens ÷ the wall clock between two polls, i.e. what the client received). The gap between them is the proxy's own overhead. **Avg Proxy Time** is the mean wall-clock time one request spent inside the proxy: a large average with a healthy generation speed means the proxy, not the GPU, is the bottleneck
- **Running the AntiSlop-vLLM proxy? Turn on `metrics.antislop_columns`** — the proxy's `GET /metrics` serves its own `antislop:` counters alongside the upstream vLLM body, so setting `metrics.antislop_columns: true` adds **Proxy Requests**, **Bans (fixed/total)**, **Proxy Errors** and **Proxy Gen Speed** to the same tab with no second URL. **Bans (fixed/total)** is the tuning figure: a large total with a small fixed count means the ban list is firing on things the model cannot avoid, and every one of those costs a back-track and a restart for an answer that rarely improves. **Proxy Gen Speed** is measured over the wall clock rather than the server's decode timer, so the gap between it and the vLLM **Generation Speed** column is the proxy's own overhead
- **Use the Queue screen** — Monitor task progress in real-time; use **Purge Queue** to abandon bad batches without stopping the entire job, or **Retry Failed** to recover from transient issues
- **Export queue state** — Use **📤 Export Queue** to get a CSV snapshot of task status for external analysis
- **Enable quality scoring** — Set `quality.enabled: true` to get per-conversation quality metrics; use heuristic-only mode (default) for zero overhead, or enable LLM scoring for deeper analysis
- **Tune the quality threshold** — Start with `min_score_threshold: 50` and adjust based on the distribution of scores you see in the Quality screen; too low flags nothing, too high flags everything
- **Use a dedicated cheap model for scoring** — Point `quality.scoring_api` at a fast, inexpensive model to minimize the cost impact of LLM-based scoring
- **Match `quality.judge_concurrency` to the judge server, not to your thread count** — the judge is a gate: a worker waits for its verdict before it may write, so judge throughput caps the whole run. `judge_concurrency` defaults to `1`, which caps the judge at one request at a time; with `T` generation threads and `L` seconds of judge latency the last waiter waits about `(T / 1) × L`, and once that passes `quality.result_timeout` (600 s) healthy conversations are dropped as `judge_failed`. The target is `concurrency >= threads * judge_latency / seconds_per_conversation`, capped at what the judge server can really serve in parallel. **A local vLLM that slows down past 2-3 concurrent sequences wants `2` or `3`, not `8`** — more only moves the queue into vLLM's scheduler, where it is harder to see and costs the same latency, and a judge request is large (conversation + character profile) so wide concurrency can exhaust KV cache and trigger preemption. Lower `judge_max_attempts` and `judge_json_repair_attempts` before raising concurrency: at the defaults one flaky conversation can occupy the judge for ~723 s, longer than `result_timeout` itself.
- **Export flagged conversations** — Use **📤 Export Flagged → JSONL** in the Review screen to get a clean file of low-quality outputs for manual inspection or re-processing
- **Dismiss reviewed items** — After manually handling flagged conversations, use **✅ Dismiss All** to clear the review queue and keep the screen focused on new items
- **Test GUI layouts** — Use `tools/check_gui_layout.py` to validate the **deprecated Tk window** on smaller display sizes. The web UI is checked differently: every screen must fit 1440x900 and 900x600 with no page scroll, and `tests/test_web_overview.py` asserts it
- **Clear the dataset without stopping the run** — **Clear dataset** on the Overview action bar deletes the stored conversations and takes a backup zip first. Use it instead of **Stop & Clear** when the run is already finished and you only want a fresh dataset; Stop & Clear also stops a run, so using it on an idle app is a needless stop
- **Download the dataset from the browser** — **Download dataset** streams the whole dataset back as one JSONL file, consolidated from the per-API files. It is a GET that consolidates into a temporary file, so it is safe to click twice and it never doubles the next export. Use it instead of `cli export` when you are already in the browser and the file is on the same machine
- **Point at a dataset instead of `input/*.txt` when the prompts come from a table** — a bound dataset source gives one frozen row per task, so a run cannot drift when the file changes underneath it. Import the columns once, then map the ones you need to bare aliases with `{{ds:alias}}`. The Datasets screen never shows a cell value, so the config stays safe to commit. See [Dataset bindings](dataset-bindings.md)
- **Prefer `csv`/`jsonl` over `parquet` for a large source** — `parquet` and the two Hugging Face kinds need `requirements-datasets.txt` and pull in `pyarrow`/`datasets`. A CSV reads with the standard library and no extra install, so reach for Parquet only when the file is already Parquet
- **Narrow `visible_columns` on a wide source** — a 40-column table makes the column picker unusable. `visible_columns: []` means none and `null` means all, so set an explicit list of the few columns the prompts actually use
- **The web UI costs nothing extra** — it is a second client of the same engine, so a run driven from the browser sends and writes exactly what the CLI does. There is no build step and no Node toolchain, so nothing about it changes throughput
- **Watch the run from any device on the same host** — the web UI is served over HTTP, so a run started in the browser keeps going when the tab closes: the state lives on the server, and the page re-primes itself on reload. A long run therefore does not need a tab left open
