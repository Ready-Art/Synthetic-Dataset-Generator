<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Core modules (API calls, state, config loading, text, layout, style metrics)

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching `api_handler.py`, `api_profiles.py`, `app_state.py`, `config_loader.py`, `logging_config.py`, `text_utils.py`, `ui_layout.py` or `style_metrics.py`.

**Related:** [`docs/agents/generation.md`](generation.md), [`docs/agents/quality.md`](quality.md)

## Contents

- `api_handler.py`
- `api_profiles.py`
- `app_state.py`
- `config_loader.py`
- `logging_config.py`
- `tests/test_api_profiles.py`
- `text_utils.py`
- `tests/test_repeated_words.py`
- `ui_layout.py`
- `style_metrics.py`
- `tests/test_style_metrics.py`
- `Key State Variables (`app_state.py`)`
- `Thread Safety Pattern`
- `Config Paths (`config_loader.py`)`
- Tasks that touch these files

---

### `api_handler.py` [4]
**Purpose:** API calls, rate limiting, Valkey caching
**Key:** `RateLimiter`, `global_rate_limiter`, `get/set_cached_response`
**Edit for:**
- Rate limit logic → `RateLimiter.wait_if_needed()`
- Cache TTL → `set_cached_response()` ttl param
- Response time tracking → `api_response_times_per_slot`

---

### `api_profiles.py` [5]
**Purpose:** Filter request payloads per API endpoint compatibility
**Key:** `apply_profile_for_slot()`, `detect_profile()`, `set_slot_profile()`
**Edit for:**
- Add new API profile → `config/api_profiles.yml` (not code)
- Param rename logic → `_RENAME_TRANSFORMS`
- Always-allowed keys → `_ALWAYS_ALLOWED`

---

### `app_state.py` [6]
**Purpose:** Shared runtime state, counters, locks, GUI handles
**Key:** All globals (counters, locks, widget refs)
**Edit for:**
- Add new counter → Add to module level + init in `reset_all_stats_and_history()`
- Add new lock → `threading.Lock()` at module level
- GUI widget refs → Assign in `generate.py` after widget creation
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
**Purpose:** Shared logging utility
**Key:** `log_message()`, `LOG_FILE_PATH`, `DEBUG_LOGGING_ENABLED`
**Edit for:**
- Log path → `LOG_FILE_PATH`
- Enable debug → `DEBUG_LOGGING_ENABLED = True` or UI checkbox

---

### `tests/test_api_profiles.py` [15]
**Purpose:** Unit tests for `api_profiles.py`
**Edit for:** Add new profile tests in `main()`

---

### `text_utils.py` [17]
**Purpose:** Text processing utilities
**Key:** `normalize_quotes()`, `repair_straight_quotes()`, `remove_markdown()`, `remove_reasoning_text()`, `collapse_repeated_words()`
**Edit for:**
- Add text cleanup → New function + call in `write_conversation()` processing loop. The loop runs `remove_reasoning_text` → `remove_em_dash` → `remove_excessive_asterisks` → `remove_asterisk_space_asterisk` → `remove_all_asterisks` → `ensure_space_after_line_break` → `remove_markdown` (each behind its own config flag) → `collapse_repeated_words` (**always on**) → `repair_straight_quotes` → `normalize_quotes` (both always on). Insert a flag-gated pass before the always-on tail and an always-on pass in the tail, never after `normalize_quotes` — the quote passes stay last so nothing reintroduces an unbalanced glyph after they have balanced it
- Quote handling → `normalize_quotes()` (curly vs straight balance)
- **Repeated-word stalls (v12.7.3)** → `collapse_repeated_words()` is the NEWEST of the module's three always-on passes (`repair_straight_quotes()` and `normalize_quotes()` are the others), and it is justified against the rule in `AGENTS.md` ("keep features opt-in by default"): a stalled word is a property of the *reply*, not of a prompt or a setting, so a deterministic pass can remove it without a retry and without a config key. It is idempotent, safe on empty input and returns text with no repetition unchanged
- **The exclusion list IS the safety story — do not invert it.** `_REPEAT_EXEMPT_WORDS` names ordinary English words that are *never* collapsed (`no no no`, `yes yes yes`, `very very good`, `ha ha ha`, `wait wait wait`, `hmm`, `okay` are deliberate emphasis), and every other word is collapsed. Writing it as an exclusion list is what makes the pass generalise: a word the model stalls on is caught without having been enumerated, and the module carries no inclusion list of "bad" words that would be inappropriate to publish. Do not add a word to the list because a run happened to repeat it, and do not swap the design for an inclusion list
- **Four regexes, one per way a stall is written** — `_REPEATED_SENTENCE_WORD_PATTERN` (`Word. Word.`), `_REPEATED_COMMA_RUN_END_PATTERN` (`word, word, word.`), `_REPEATED_COMMA_RUN_PATTERN` (`word, word, word text`) and `_REPEATED_BARE_WORD_PATTERN` (`word word word`). All four are case-insensitive and keep the FIRST occurrence's casing, so a capitalised sentence opener stays capitalised. The comma-run-end pattern must be applied BEFORE the comma-run pattern: it consumes the trailing terminator, and reversing the order would leave `word.` then `text` with the punctuation already eaten
- **Guarded by `tests/test_repeated_words.py`** — headless; it pins the four shapes, the exempt words (including that an exempt word is left alone even when it really is a stall), casing, the two-word scope, empty/`None` input, idempotency, and the `write_conversation()` call order (strictly increasing, `normalize_quotes()` LAST, no config flag on the collapse line). Run it before changing a regex or `_REPEAT_EXEMPT_WORDS`
- **Known scope, do not "fix" it silently** — a repetition is only collapsed when the repeated token is a single word (`She said. She said.` is left alone, because the run is two words), and a repeated word that happens to be exempt is left alone even when it is genuinely a stall. Both are deliberate: the pass is a low-risk deterministic cleanup, not a detector, and a false collapse rewrites valid prose

---

### `ui_layout.py` [18]
**Purpose:** Tkinter layout helpers
**Key:** `ScrollableFrame`, `ResponsiveGrid`, `NotebookNavigator`, `wrap_label()`
**Edit for:**
- Custom layout → Use `ResponsiveGrid` for reflowing widgets
- Scrollable area → Wrap in `ScrollableFrame`

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

**Cast event log (v12.0):** `cast_events_log` — a bounded ring buffer (`MAX_CAST_EVENTS_LOG` = 500) of `(task_id, event, name, api_slot_idx)` entries, guarded by `cast_events_log_lock`, filled by `app_state.record_cast_events()` and cleared by `dashboard.clear_dashboard()`. `event` is `arrived` / `exited` / `arrival_deferred` / `exit_deferred` / `arrival_forced` / `exit_cancelled`. **`exit_cancelled` is the important one**: it means the model never narrated a scheduled departure, so the character legitimately stayed — count those to measure whether the steering lands

**Issue timestamp keys:** `refusals`, `user_speaking`, `slop`, `errors`, `anti_slop`, `incomplete_quotes`, `out_of_character`, `logic_issues`, `regex_watch`, `non_latin`, `scene_continuity`

**Locks:** `stats_lock`, `issue_timestamps_lock`, `quality_lock`, `task_queue_ui_lock`, `api_circuit_breaker_lock`

**GUI handles:** `root`, `dashboard_notebook`, `slot_widgets`, `rate_limit_labels`, `quality_*_label`

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
| Add UI metric | `app_state.py` (widget ref), `generate.py` (create widget), `dashboard.py` (update) |
| Add a style measurement / style metadata column | `style_metrics.py` (keep it dependency-free, Python 3.11-clean — no 3.12+ syntax such as PEP 695 `type` aliases), then `quality._score_tic_density()` (if it feeds the dimension) and/or `generation._style_metrics_for_record()` (it flows into the `style_*` block automatically via `response_metrics()`/`aggregate()`); `tests/test_style_metrics.py` |
