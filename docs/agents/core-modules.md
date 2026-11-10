<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# Core modules (API calls, state, config loading, text, style metrics)

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching `api_handler.py`, `api_profiles.py`, `app_state.py`, `config_loader.py`, `logging_config.py`, `text_utils.py` or `style_metrics.py`.

**Related:** [`docs/agents/generation.md`](generation.md), [`docs/agents/quality.md`](quality.md)

## Contents

- `api_handler.py`
- The HTTP connection pool (v14.2.0)
- `api_profiles.py`
- `app_state.py`
- `config_loader.py`
- `logging_config.py`
- `tests/test_api_profiles.py`
- `text_utils.py`
- `tests/test_repeated_words.py`
- `style_metrics.py`
- `tests/test_style_metrics.py`
- `tests/test_http_connection_pool.py`
- `Key State Variables (`app_state.py`)`
- `Thread Safety Pattern`
- `Config Paths (`config_loader.py`)`
- Tasks that touch these files

---

### `api_handler.py` [4]
**Purpose:** API calls, rate limiting, Valkey caching, the ONE definition of the thinking-mode rule, and the ONE shared HTTP connection pool
**Key:** `RateLimiter`, `global_rate_limiter`, `get/set_cached_response`,
`resolve_thinking_mode()`, `thinking_payload_fragment()`, `apply_thinking_mode()`, `THINKING_MODES`,
`http` (the shared `HttpClient`), `HttpClient`, `_http_session`, `close_http_session()`,
`HTTP_POOL_MAXSIZE`, `HTTP_CONNECT_RETRIES`
**Edit for:**
- Rate limit logic → `RateLimiter.wait_if_needed()`
- Cache TTL → `set_cached_response()` ttl param
- Response time tracking → `api_response_times_per_slot`
- **Anything about `chat_template_kwargs` / `enable_thinking`** → `resolve_thinking_mode()`,
  `thinking_payload_fragment()`, `apply_thinking_mode()` (v13.0.4). They live here because both
  `generation.py` and `quality.py` import this module and `quality.py` must not import
  `generation.py`. **Any new request builder that sends `messages` must call
  `apply_thinking_mode()`** — a builder that reads `enable_thinking` itself silently ignores
  `inherit`. See the v13.0.4 task row below for the full list of files.
- **Anything about HTTP connections / pooling / retries** → `_build_session()`,
  `HTTP_POOL_MAXSIZE`, `HTTP_CONNECT_RETRIES` (v14.2.0). See the connection-reuse rule below

---

### The HTTP connection pool (v14.2.0) — read before touching any request call site

Every outbound request goes through **one shared, pooled `requests.Session`**, exposed as the
`HttpClient` facade `api_handler.http`. `generation`, `quality` and `vllm_metrics` each bind their
module-level name `requests` to that ONE object (`requests = http`), so the ~100 call sites still
read `requests.post(url, headers=..., data=..., timeout=...)` and
`requests.exceptions.Timeout` still works.

**Why it exists.** The old code called the module-level `requests.post(...)` helper, which builds a
throwaway `Session` per call: a new TCP connection, a new TLS handshake, one request, close — from
every worker and every judge thread. Against a local vLLM behind nginx that is one connection per
request, and the server's socket table fills with `ESTAB` entries against `:443` (the "client side
spam connections" report). It was never extra requests; it was the same requests each paying for a
new connection. Measured: 120 requests / 8 threads went from **120 connections, 1.04 s** to
**8 connections, 0.67 s**.

Four rules that are easy to break:

- **`requests` must stay bound to the SHARED facade, not a per-module instance.** The suite patches
  `generation.requests.post` and expects the quality judge's and the poller's calls intercepted too,
  because before v14.2.0 all three imported the same real module. A per-module `HttpClient()` looks
  tidier and silently breaks that: `tests/test_dataset_compatibility.py` fails with
  `len(judges) == 0`. `tests/test_http_connection_pool.py` pins the shared identity.
- **`max_retries` is CONNECT-ONLY.** `read=0`/`status=0` on purpose: a read timeout on a generation
  request means the model may already have produced tokens, so resending it would silently duplicate
  work and bill the slot twice. `allowed_methods=None` is required for the connect case, because
  urllib3 otherwise refuses to retry POST.
- **`pool_maxsize` must stay >= the largest `api.threads` the GUI offers** (64). A pool smaller than
  the thread count makes urllib3 open extra connections and discard them instead of pooling them,
  which brings the churn straight back. `pool_block=False` keeps the old never-block-a-worker
  behaviour.
- **A reused connection may be dead.** nginx closes idle keep-alive connections
  (`keepalive_timeout`, 75 s default) and the rate limiter can park a slot for 60 s. urllib3 2.x
  detects this and re-opens transparently (verified: 0/6 requests failed across idle gaps longer
  than the server's timeout), so do not "fix" it by disabling pooling or by raising the server's
  timeout.

`close_http_session()` is registered with `atexit`; it is idempotent and never raises.

---
**Purpose:** Filter request payloads per API endpoint compatibility
**Key:** `apply_profile_for_slot()`, `detect_profile()`, `set_slot_profile()`
**Edit for:**
- Add new API profile → `config/api_profiles.yml` (not code)
- Param rename logic → `_RENAME_TRANSFORMS`
- Always-allowed keys → `_ALWAYS_ALLOWED`

---

### `app_state.py` [6]
**Purpose:** Shared runtime state, counters, locks, run flags
**Key:** All globals (counters, locks, flags)
**Edit for:**
- Add new counter → Add to module level + init in `control._reset_stats()`
- Add new lock → `threading.Lock()` at module level
- `duplication_mode` → a plain `bool`, set by `control.cmd_start()`, read by `generation.py` on every write path. Never a `.get()`/`.set()` wrapper
- `live_prompt_preview_hook` → the one front-end seam; `webapp/live_prompts.py` installs it, `generation.update_live_prompt_preview()` calls it, `None` means no consumer
- Random-name deck (`random_name_deck`, `random_name_lock`, `random_names_pool_key`) → the pool dealt by `generation.deal_random_names()`. It is deliberately module-level run state, NOT per-worker state: every worker must deal from the same deck or the pool would be spread per worker and repeat across them. `random_names_pool_key` is the fingerprint of the pool the deck was built from, so editing the list or toggling the feature starts a fresh deck instead of dealing from a pool the user has replaced. `reset_all_stats_and_history()` clears it so a new run starts a fresh pass

---

### `config_loader.py` [8]
**Purpose:** Load/save config.yml and profiles
**Key:** `ConfigLoader` class, `get()`, `set()`, `save_profile()`
**Edit for:**
- Config path → `__init__(path=...)`
- Profile operations → `save_profile()`, `load_profile_to_main_config()`, `delete_profile()`

---

### `logging_config.py` [13]
**Purpose:** Shared logging utility, and the ONE gate for the per-request prompt traces
**Key:** `log_message()`, `LOG_FILE_PATH`, `DEBUG_LOGGING_ENABLED`, `DEBUG_PROMPT_LOGGING_ENABLED`, `debug_prompt_logging_enabled()`, `write_debug_prompt()`
**Edit for:**
- Log path → `LOG_FILE_PATH`
- Enable debug → `DEBUG_LOGGING_ENABLED = True` or UI checkbox
- Prompt traces (`output/debug_prompt*.jsonl`) → `generation.debug_prompt_log: true` in the config, or the "Prompt Trace" checkbox in the Tools panel. **Opt-in since v13.1.1**: the traces used to be written UNCONDITIONALLY, one append per request from every worker and every judge thread. Every write site must go through `debug_prompt_logging_enabled()` / `write_debug_prompt()` — never a bare `open(..., 'a')` on a debug path, which is what `tests/test_prompt_trace_gate.py` fails on. Check the flag at CALL time, so a toggle takes effect without a restart

---

### `tests/test_api_profiles.py` [15]
**Purpose:** Unit tests for `api_profiles.py`
**Edit for:** Add new profile tests in `main()`

---

### `text_utils.py` [17]
**Purpose:** Text processing utilities
**Key:** `normalize_quotes()`, `repair_straight_quotes()`, `remove_markdown()`, `remove_reasoning_text()`, `collapse_repeated_words()`, `strip_style_instruction_leak()`, `strip_scene_cast_leak()`
**Edit for:**
- **`{style}` instruction leak (v13.2.1)** → `strip_style_instruction_leak()` and `STYLE_LEAD_VERBS` / `_STYLE_INSTRUCTION_LINE_PATTERN`. The shipped prompts interpolate a style as `Begin this message as follows: {style}`, and some models echo that clause back as the **first line of the reply**; because the styles are imperatives (`open on a sound…`) the echo reads as a stage direction above the prose. **It is called from `worker()` on the accepted answer, NOT from the `write_conversation()` loop** — the worker is where `task_style` is in scope, and the guard must be gated on `if task_style:` so a style-free run is byte-identical. Matching the configured strings cannot work: the leak is never an exact pool entry (the model paraphrases `someone entering or leaving` into `someone entering the shop`), so the pattern matches the **imperative verb** the style always opens with. It is deliberately conservative — it only drops an instruction that is a standalone first line with real prose after it, and leaves a message that is ONLY an instruction, an instruction that is not first, a first line that runs on into prose, and ordinary prose containing the word `open` untouched. Deleting a real opening sentence would be a worse failure than leaving one ugly line. Guarded by `tests/test_style_leak_guard.py`
- **`SCENE CAST` note leak (v13.5.1)** → `strip_scene_cast_leak()` and `_SCENE_CAST_HEADER_PATTERN` / `_SCENE_CAST_ENTRY_PATTERN`. `prompts.scene_cast` appends a per-reply cast note to the reply's final user message (or its system prompt, on `placement: system`), and some models echo it back as the **first line of the reply**. The echo is also **stale** — it is the note for the reply being written, so a row that kept it repeats `NAME: LEAVES` on every later turn, long after that character left. Same class of bug as the `{style}` leak above, and guarded the same conservative way: only the **first non-empty line** is considered, it must be the `SCENE CAST` header itself, the block is consumed for as long as the following lines still read as cast entries (either shipped shape — `NAME: PRESENT` one per line, or `PRESENT: a, b` / `NOT IN THE SCENE: c` / `LEAVES:` / `ARRIVES:`), a blank line ends it, and a message that is ONLY the block is returned unchanged because that is a failed generation, not a leak with prose after it. **It is called from `worker()` on the man's two message paths** — the opening question reply and the user continuation reply — each gated on `if scene_cast_plan["members"] and <reply>:` so a cast-free run is byte-identical. Guarded by `tests/test_scene_cast_leak_guard.py`
- Add text cleanup → New function + call in `write_conversation()` processing loop. The loop runs `remove_reasoning_text` → `remove_em_dash` → `remove_excessive_asterisks` → `remove_asterisk_space_asterisk` → `remove_all_asterisks` → `ensure_space_after_line_break` → `remove_markdown` (each behind its own config flag) → `collapse_repeated_words` (**always on**) → `repair_straight_quotes` → `normalize_quotes` (both always on). Insert a flag-gated pass before the always-on tail and an always-on pass in the tail, never after `normalize_quotes` — the quote passes stay last so nothing reintroduces an unbalanced glyph after they have balanced it
- Quote handling → `normalize_quotes()` (curly vs straight balance)
- **Repeated-word stalls (v12.7.3)** → `collapse_repeated_words()` is the NEWEST of the module's three always-on passes (`repair_straight_quotes()` and `normalize_quotes()` are the others), and it is justified against the rule in `AGENTS.md` ("keep features opt-in by default"): a stalled word is a property of the *reply*, not of a prompt or a setting, so a deterministic pass can remove it without a retry and without a config key. It is idempotent, safe on empty input and returns text with no repetition unchanged
- **The exclusion list IS the safety story — do not invert it.** `_REPEAT_EXEMPT_WORDS` names ordinary English words that are *never* collapsed (`no no no`, `yes yes yes`, `very very good`, `ha ha ha`, `wait wait wait`, `hmm`, `okay` are deliberate emphasis), and every other word is collapsed. Writing it as an exclusion list is what makes the pass generalise: a word the model stalls on is caught without having been enumerated, and the module carries no inclusion list of "bad" words that would be inappropriate to publish. Do not add a word to the list because a run happened to repeat it, and do not swap the design for an inclusion list
- **Four regexes, one per way a stall is written** — `_REPEATED_SENTENCE_WORD_PATTERN` (`Word. Word.`), `_REPEATED_COMMA_RUN_END_PATTERN` (`word, word, word.`), `_REPEATED_COMMA_RUN_PATTERN` (`word, word, word text`) and `_REPEATED_BARE_WORD_PATTERN` (`word word word`). All four are case-insensitive and keep the FIRST occurrence's casing, so a capitalised sentence opener stays capitalised. The comma-run-end pattern must be applied BEFORE the comma-run pattern: it consumes the trailing terminator, and reversing the order would leave `word.` then `text` with the punctuation already eaten
- **Guarded by `tests/test_repeated_words.py`** — headless; it pins the four shapes, the exempt words (including that an exempt word is left alone even when it really is a stall), casing, the two-word scope, empty/`None` input, idempotency, and the `write_conversation()` call order (strictly increasing, `normalize_quotes()` LAST, no config flag on the collapse line). Run it before changing a regex or `_REPEAT_EXEMPT_WORDS`
- **Known scope, do not "fix" it silently** — a repetition is only collapsed when the repeated token is a single word (`She said. She said.` is left alone, because the run is two words), and a repeated word that happens to be exempt is left alone even when it is genuinely a stall. Both are deliberate: the pass is a low-risk deterministic cleanup, not a detector, and a false collapse rewrites valid prose

---

### `style_metrics.py`
**Purpose:** (v11.7) Pure-stdlib measurement layer ported from the scotoma analysis scripts (comma habits, tail-end positivity lift, narration/dialogue segmentation, interiority density, long-range grounding, coherence defects, narration-only negation density/runs, denial-then-correction couplets, comma-appended absolute trailers, question handback). Imports NOTHING project-local and nothing third-party — it is a leaf in the dependency graph (the `app_state.py` rule), so it unit-tests headless. `response_metrics()` = one flat dict of numbers per assistant turn; `aggregate()` = means across rows, excluding `UNMEASURED` (-1.0); `UNMEASURED` is the gap token. **`TRAILER_PATTERN` is the ONE definition of the comma-appended absolute-trailer construction** — `config/config.yml` and `detection.DEFAULT_REGEX_WATCH_PATTERNS` mirror that exact string and `tests/test_regex_watch.py` asserts all three are identical, so the metric and the rewrite pass cannot drift apart. The trailer rule (from `scotoma/trailer_rate.py`) is: a comma-appended segment that carries **no finite verb** and **no speech verb** and **does** carry a predicate signal (body noun / `-ing` participle / `-ly` adverb). A comma that directly follows a speech verb, or that closes a line of dialogue, never opens a trailer. Dropping the verbless test is what made ordinary clauses like `"...the man said"` and `"...the boat is leaving"` read as absolutes. Design decisions that fix scotoma defects and must not be "simplified" away: tail lift is clamped at 10.0 (an unbounded ratio destroys means); `question_handback` uses `min_words=1` (a 5-word floor silently drops "What do you think?"); `negation_profile` is narration-only via `segments()` (asterisks invert the dialogue convention — asserted in the tests); `denial_correction_hits` rejects a negated correction tail (a negation RUN is not a denial-correction). Consumed by `quality._score_tic_density()` (the `tic_density` dimension) and `generation._style_metrics_for_record()` (the per-record `style_*` metadata block)
**Edit for:** New measurements go here first (keep it dependency-free and Python 3.11-clean — no 3.12+ syntax such as PEP 695 `type` aliases), then wire them into `_score_tic_density` / `response_metrics` / the metadata block; covered by `tests/test_style_metrics.py`

---

### `tests/test_style_metrics.py`
**Purpose:** Headless guard for `style_metrics.py` — 58 checks over every metric, including the clamped-lift, UNMEASURED-exclusion, min_words=1 handback, segments() convention inversion and negation-run-is-not-a-couplet cases

---

## Key State Variables (`app_state.py`)

**Counters:** `total_attempts_global`, `refusal_count_total`, `slop_count_total`, `error_count_total`, `anti_slop_count_total`, `ooc_count_total`, `logic_issue_count_total`, `regex_watch_count_total`, `non_latin_count_total`, `scene_continuity_count_total`, `total_input_tokens`, `total_output_tokens`

**Per-API:** `*_per_api` dicts for indices 0-5

**Recent issues:** `recent_*_total`, `recent_*_per_api` (MAX_RECENT = 10); includes `recent_ooc_total` / `recent_ooc_per_api`, `recent_logic_total` / `recent_logic_per_api`, `recent_regex_total` / `recent_regex_per_api` `recent_non_latin_total` / `recent_non_latin_per_api` and `recent_scene_continuity_total` / `recent_scene_continuity_per_api`

**Regex Watch log (v11.6):** `regex_watch_log` — a bounded ring buffer (`MAX_REGEX_WATCH_LOG`) of `(pattern, matched_text, api_slot_idx, status)` entries, guarded by `regex_watch_log_lock`; the `🔎 Regex Watch` tab reads it. Status is `rewritten` or `unresolved`

**Cast event log (v12.0):** `cast_events_log` — a bounded ring buffer (`MAX_CAST_EVENTS_LOG` = 500) of `(task_id, event, name, api_slot_idx)` entries, guarded by `cast_events_log_lock`, filled by `app_state.record_cast_events()` and cleared by `control.cmd_reset_stats()`. `event` is `arrived` / `exited` / `arrival_deferred` / `exit_deferred` / `arrival_forced` / `exit_cancelled`. **`exit_cancelled` is the important one**: it means the model never narrated a scheduled departure, so the character legitimately stayed — count those to measure whether the steering lands

**Issue timestamp keys:** `refusals`, `user_speaking`, `slop`, `errors`, `anti_slop`, `incomplete_quotes`, `out_of_character`, `logic_issues`, `regex_watch`, `non_latin`, `scene_continuity`

**First-reply opener tally:** `first_reply_first_word_counts` / `first_reply_frame_counts` / `first_reply_total`, guarded by `first_reply_opener_lock` and touched only through `record_first_reply_opener()`, `first_reply_opener_snapshot()` and `reset_first_reply_openers()`. Like the random-name deck it is RUN state shared by every worker (the collapse it measures is across conversations), cleared by `reset_all_stats_and_history()` and not saved in `generation_state.json`

**Locks:** `stats_lock`, `issue_timestamps_lock`, `quality_lock`, `task_queue_ui_lock`, `api_circuit_breaker_lock`, `first_reply_opener_lock`

**Flags:** `stop_processing`, `pause_processing`, `processing_active`, `quality_enabled`, `quality_output_filter`, `use_conversation_history_for_fixers`

**Judge drops (v11.4):** `quality_dropped` ({task_id: reason}) and `dropped_task_ids` (set), both guarded by `quality_lock`; populated by `quality._record_quality_result()`, cleared by `reset_all_stats_and_history()`, persisted as `quality_dropped` in `generation_state.json`.

**Manual drops (v12.8.2):** the same two containers also carry the operator's drops, with the reason `quality.DROP_REASON_MANUAL` (`"manual"`), written by `generate._drop_review_conversation()`. Nothing in `quality.py`'s scoring path ever produces that reason, so the two causes stay distinguishable; a manual drop also removes the id from `quality_scores` and `quality_review_ids` under their own locks.

**`output_data_lock`:** the lock a worker holds while it appends a finished conversation to the dataset (file or database). It lives in `app_state`, not as a local in `generate.start_processing()`, so the Review tab's drop rewrite takes the SAME lock as a write and the two cannot interleave (v12.8.2). `start_processing()` binds its local `output_data_lock` to `app_state.output_data_lock`; do not give it a fresh `Lock()` again.

---

## Thread Safety Pattern
```python
with app_state.some_lock:
    app_state.counter += 1
# OR for timeouts:
lock_acquired = app_state.some_lock.acquire(timeout=0.50)
if lock_acquired:
    try:
        # modify state
    finally:
        app_state.some_lock.release()
```

---

## Config Paths (`config_loader.py`)
- Main config: `config/config.yml`
- API Profiles: `config/api_profiles.yml`
- Profiles: `config/profiles/*.yml`
- Environment: API keys via `os.getenv()` or inline in config

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Fix quote balancing | `text_utils.py` (`normalize_quotes()`, `repair_straight_quotes()`) |
| Add/modify an always-on deterministic text cleanup (v12.7.3: repeated-word collapse) | `text_utils.py` (the helper + its pre-compiled patterns at the top of the module), `generation.py` (`write_conversation()` processing loop — it goes in the always-on tail BEFORE `repair_straight_quotes`/`normalize_quotes`), `docs/features.md` (Text Post-Processing list), `docs/how-it-works.md` (step 7), `tests/test_repeated_words.py`. Keep the rule an EXCLUSION list of ordinary words (`_REPEAT_EXEMPT_WORDS`) so valid emphasis such as `no no no` survives, keep the helper idempotent and empty-input safe, and keep the comma-run-end pattern ahead of the comma-run pattern. There is no config key on purpose — do not add one |
| Add UI metric | `app_state.py` (the counter + its per-API dict), `control.build_stats_payload` (the wire field), `webapp/tabs_routes.py` (the route), `webapp/static/js/` (the card) |
| Add a new outbound HTTP call, or change pooling / retries / timeouts (v14.2.0) | `api_handler.py` (`_build_session()`, `HTTP_POOL_MAXSIZE`, `HTTP_CONNECT_RETRIES`). **Bind the module's `requests` name to the SHARED `api_handler.http` facade** — never `import requests` and never a per-module `HttpClient()`, or the suite's `generation.requests.post` patches stop reaching the judge and the poller. Keep `max_retries` connect-only (`read=0`/`status=0`) so a read timeout is never silently resent. `tests/test_http_connection_pool.py` |
| Change which requests send `chat_template_kwargs` / add a per-request `enable_thinking` override (v13.0.4) | `api_handler.py` (`resolve_thinking_mode()` / `thinking_payload_fragment()` / `apply_thinking_mode()` / `THINKING_MODES` / the `*_THINKING_KEY` constants — the ONE definition, here because both `generation.py` and `quality.py` import it and `quality.py` must not import `generation.py`), the four call sites (`generation.call_slop_fixer_llm`, `call_anti_slop_llm`, `rewrite_regex_watch_text`, `quality._call_logic_judge_once`), `config/config.yml`, `webapp/config_schema.py` (three dropdowns; the choices are asserted equal to `api_handler.THINKING_MODES`), `tests/test_thinking_mode.py`. **The keys are SIBLINGS of `slop_fixer_params` / `anti_slop_params` / `sampler_settings`, never entries in them** — those dicts are read with an all-or-nothing `params or main_sampler_settings` fallback, so a lone extra key makes one truthy and stops it inheriting the main defaults (top_p 0.9 -> 0.95). **`inherit` is the default for the auxiliary three and must stay so.** Mechanism: `docs/samplers-and-infrastructure.md` |
| Add a style measurement / style metadata column | `style_metrics.py` (keep it dependency-free, Python 3.11-clean — no 3.12+ syntax such as PEP 695 `type` aliases), then `quality._score_tic_density()` (if it feeds the dimension) and/or `generation._style_metrics_for_record()` (it flows into the `style_*` block automatically via `response_metrics()`/`aggregate()`); `tests/test_style_metrics.py` |
