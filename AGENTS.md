# AGENTS.md - Codebase Cheat Sheet

## Setup and Tests

The application targets Python 3.9 (`README.md`, Requirements), so code and tests must not use anything newer, such
as `match` or `X | Y` type unions (both 3.10).
All test scripts live in `tests/` (run them from the repository root, e.g. `python -B tests/test_api_profiles.py`).
Tests that import `generation.py` (`tests/test_opening_mode.py`, `tests/test_persona_arc.py`,
`tests/test_dataset_compatibility.py`) or the Datasets panel need Tk; `tests/test_datasets_panel.py` and
`tests/test_dataset_column_menu.py` also need a display; `tests/test_review_tab_ui.py` and
`tests/test_regex_watch_tab_ui.py` need a display too (they build the real `generate.py` UI) and skip
cleanly without one. `tests/test_quality_judge_settings_ui.py` also
needs a display (it builds the real `ConfigEditor`) and skips cleanly without one, as does
`tests/test_editor_readability_ui.py` (it also needs the 'superhero' theme, so run it under `xvfb-run`).

```sh
python -m pip install -r requirements.txt            # the application
python -m pip install -r requirements-datasets.txt   # the application + Hugging Face, Parquet and saved-HF sources
```

Run tests from the repository root. Every test file runs on its own and exits non-zero on a failure; none needs a
model, a network connection or a running server. While working, run the test for the module you change, where the
File Reference below names one. Before finishing, and before opening a pull request, run all of them and say in
the pull request which ones you ran and whether they passed:

```sh
for t in tests/test_*.py; do python -B "$t" || echo "FAILED: $t"; done
```

- `tests/test_dataset_backends.py` skips its checks unless `requirements-datasets.txt` is installed. A skip does not count
  as a pass.
- `tests/test_dataset_compatibility.py` runs the current worker twice per scenario, once with values typed in and once
  with the same values bound from CSV/JSONL sources, and requires the two runs to send identical requests. A failure
  means dataset binding changed what the worker sends.
- Importing `logging_config` creates `output/` in the working directory, and `logging_config.LOG_FILE_PATH` is
  relative to it, so `tests/test_api_profiles.py` appends to the repository's `output/log.txt`. `tests/test_opening_mode.py` and
  `tests/test_persona_arc.py` change into a temporary directory first.
- `tools/check_gui_layout.py` and `tools/check_dataset_hub.py` are not part of the test run (see Tools).
- `tests/test_quality_judge_policy.py` is headless: it stubs `requests.post` and never touches the network.
- `tests/test_quality_json_robustness.py` is headless too: it stubs `requests.post` and covers the judge's JSON extraction/repair ladder, the format-repair round, truncation retry and the `response_format` fallback.

## Boundaries

- `.gitignore` covers local output: `output/` (generated rows, `log.txt`, run state), `__pycache__/`,
  `.pytest_cache/` and `layout-check/` (once the layout tool has run). Stage files by name; never `git add -A`
  or `git add .`.
- **Never commit an API key.** `config/config.yml` is tracked and ships with every `key: ''` empty.
- **Ask before changing the wording of a shipped prompt** in `config/config.yml` (`prompts.*`). The prompts are
  short and general on purpose, and every output row depends on them.
- **New worker options go in the `kwargs=` dict** of the worker launch in `generate.py`, never in `args=` (see
  `generate.py` below).
- **Dataset settings hold names only.** Source selectors, column names and aliases go in `datasets.sources[]`;
  record values and credentials never do, and the Datasets panel never shows cell values (`dataset_settings.py`,
  `datasets_panel.py` docstrings).

## Core Architecture
```
generate.py (UI entry) → generation.py (worker logic) → api_handler.py (API calls)
                    ↓                              ↓
              app_state.py (shared state) ← detection.py (issue checks)
                    ↓
              dashboard.py (display updates)
```

## File Reference

### `config/config.yml` [1]
**Purpose:** Main configuration file for API settings, generation parameters, prompts, detection, and samplers
**Key:** `api.apis[]`, `generation.*`, `prompts.*`, `detection.*`, `samplers.*`
**Edit for:**
- API endpoints → `api.apis[].url`, `api.apis[].model`, `api.apis[].key`
- Rate limits → `api.apis[].rate_limit_rpm`, `api.apis[].threads`
- Generation settings → `generation.max_character_cards`, `generation.num_turns`, `generation.output_format`
- Prompts → `prompts.system.base`, `prompts.question`, `prompts.answer`, `prompts.greeting`, `prompts.first_line`
- Opening mode → `prompts.opening_mode`, `prompts.greeting_context`, `prompts.first_line_styles`
- Detection phrases → `detection.refusal.phrases`, `detection.slop.phrases`, `detection.user_speaking.*.phrases`, `detection.out_of_character.phrases` (+ `enabled`, `fixes`)
- Regex Watch (v11.6) → `detection.regex_watch.enabled` (off by default), `detection.regex_watch.max_rounds` (1-5, default 3), `detection.regex_watch.patterns[]` (regular expressions, user-editable; shipped defaults are mirrored by `detection.DEFAULT_REGEX_WATCH_PATTERNS`). A hit is rewritten by the SAME main API slot, not a fixer slot; there is no fixes list
- Sampler params → `samplers.temperature`, `samplers.top_p`, `samplers.repetition_penalty`
- Bad words (v11.1) → `samplers.use_bad_words` (bool, off by default), `samplers.bad_words` (list of strings; leading whitespace on every entry is preserved exactly as typed, matching vLLM semantics). The single toggle drives every main-API request (question/answer/user-continuation/opening-mode greeting+first-line) plus the Slop Fixer (slot 5) and Anti-Slop Fixer (slot 6), even when those fixers carry their own `slop_fixer_params` / `anti_slop_params` sampler overrides — `generation.apply_bad_words_to_payload()` reads from the TOP-LEVEL `samplers.*` keys so it works regardless of per-slot override state
- Character profiles → `prompts.character.characters[]` (name, race, job, hair, hair_color, clothing, appearance, persona, personality, traits, backstory, setting) — `persona` = inner personality (private self), `personality` = outward behavior (observed manner); `persona` is withheld from the profile and re-injected as a breakthrough block from `prompts.character.persona_breakthrough_turn` onward
- Emotional states/arcs → `prompts.emotional_states.states[]` — each entry is a flat state or a `'->'`-joined progression (`'neutral -> tense -> angry'`) expanded across the conversation's assistant turns
- User-side emotional states/arcs (v10.6.0, optional) → `prompts.user_emotional_states.{enabled,states[]}` — same flat/'->' arc format as `emotional_states` but applied to the SIMULATED USER's messages (generated opening question/first line + each user continuation); off by default; note is ephemeral (user prompt only, never history/system); placement does not apply. **Editor pitfall (fixed v10.7.1):** the editor's text field for this key is created `disabled`, and this Tk build drops `delete`/`insert` on a disabled Text widget *silently* (no exception) — any handler that writes into a config-editor text field must ensure its state is `normal` first (see `load_config_handler()` order)
- Random names → `prompts.random_names.enabled`, `prompts.random_names.names[]` (optional list of names to randomly assign to characters, overrides character Name field when enabled)
- Random settings pool → `prompts.random_settings.enabled`, `prompts.random_settings.settings[]` (analogous to `random_names`). When `setting_enabled` is true and a selected character's own `setting` is empty, a setting is sampled from this pool (when enabled) instead of the hardcoded default. For a multi-character ensemble the per-character settings are unified to ONE shared setting picked from the ensemble, so the whole scene happens in a single location
- Conversation history for fixers → `generation.use_conversation_history_for_fixers` (whether to pass conversation context to slop/anti-slop fixer LLMs)
- Output metadata → `generation.include_metadata` (flat, schema-stable provenance block per output.jsonl record; trainer-safe; never modifies `conversations[]`; false = legacy two-field records)
- Quality scoring → `quality.enabled`, `quality.use_llm_scoring`, `quality.output_filter`, `quality.roleplay_dimensions` (opt-in roleplay dimensions: voice_distinctiveness / grounding / persona_alignment), `quality.min_score_threshold`, `quality.max_chars_for_scoring`, `quality.max_profile_chars_for_scoring`, `quality.judge_max_attempts` / `quality.judge_retry_delay` / `quality.result_timeout` (v11.4 judge retry-and-drop policy), `quality.judge_json_repair_attempts` / `quality.judge_max_tokens` / `quality.judge_request_timeout` / `quality.judge_structured_output` (v11.5 judge JSON robustness: format-repair rounds, token budget, request timeout, optional `response_format`), `quality.scoring_api.{url,model,key}`
- Datasets → `datasets.sources[]` (default `{sources: []}`) — names-only source definitions (kind, location, config/split/revision selectors, imported column names, aliases, visibility). Dataset *records* and credentials are never stored here; see `docs/dataset-bindings.md`

### `config/api_profiles.yml` [2]
**Purpose:** API compatibility profiles for request payload filtering per endpoint
**Key:** Profile definitions with `allow`, `rename`, `detect_hosts`, `label`
**Edit for:**
- Add new profile → Add new top-level key with `label`, `allow`, `rename`, `detect_hosts`
- Modify allowed params → Change `allow` list (use `all` or `from_config` for special cases)
- Key renames → `rename` dict (e.g., `repetition_penalty: frequency_penalty`)
- Auto-detection hosts → `detect_hosts` list (exact or `.suffix` for subdomains)
**Profiles:** `openai_compatible`, `openai`, `mistral`, `xai_grok`, `gemini_openai`, `anthropic_openai`, `custom`

### `tools/check_gui_layout.py` [3]
**Purpose:** GUI layout testing tool for checking UI at various window sizes without running generation
**Key:** `run()`, `check_gui_layout()`, `widget_info()`, `check_navigation()`, `check_character_profiles()`, `check_opening_mode_round_trip()`, `check_prompt_interactions()`
**Edit for:**
- Add new layout check → New `check_*()` function + call in `run()`
- Modify test sizes → `DEFAULT_SIZES` list or `--size` CLI arg
- Add screenshot capture → `--screenshots` flag + `take_screenshot()` calls
- Test character profiles → `--character-count` flag + `check_character_profiles()`
- Test opening mode → `check_opening_mode_round_trip()` validates greeting/prompt persistence
- Test prompt interactions → `check_prompt_interactions()` validates caret placement, scrolling, keyboard nav
**CLI:** `--check` (exit nonzero on fail), `--config-editor`, `--screenshots`, `--scale`, `--allow-below-minimum`

### `api_handler.py` [4]
**Purpose:** API calls, rate limiting, Valkey caching
**Key:** `RateLimiter`, `global_rate_limiter`, `get/set_cached_response`
**Edit for:**
- Rate limit logic → `RateLimiter.wait_if_needed()`
- Cache TTL → `set_cached_response()` ttl param
- Response time tracking → `api_response_times_per_slot`

### `api_profiles.py` [5]
**Purpose:** Filter request payloads per API endpoint compatibility
**Key:** `apply_profile_for_slot()`, `detect_profile()`, `set_slot_profile()`
**Edit for:**
- Add new API profile → `config/api_profiles.yml` (not code)
- Param rename logic → `_RENAME_TRANSFORMS`
- Always-allowed keys → `_ALWAYS_ALLOWED`

### `app_state.py` [6]
**Purpose:** Shared runtime state, counters, locks, GUI handles
**Key:** All globals (counters, locks, widget refs)
**Edit for:**
- Add new counter → Add to module level + init in `reset_all_stats_and_history()`
- Add new lock → `threading.Lock()` at module level
- GUI widget refs → Assign in `generate.py` after widget creation

### `config_editor.py` [7]
**Purpose:** Configuration Editor UI window
**Key:** `ConfigEditor` class, `save_config_handler()`, `load_config_handler()`, `DatasetsPanel` tab, `configure_spinbox_style()`, `color_contrast()`, `wrapped_checkbutton()`
**Edit for:**
- Add new config field → Add UI widget + bind to `save_config_handler()` + `load_config_handler()` (Quality Scoring section holds the `quality.roleplay_dimensions` toggle: `quality_roleplay_dims_var_editor`, saved/loaded with the rest of `quality.*`)
- **Wrapping labels (v11.6.2)** → a `ttk.Label` created with `wraplength=1` is the "not wrapped yet" sentinel; it MUST be handed to `wrap_label(label, container, ...)` or it renders one character per line forever. **The container must own a real allocated width** — never a frame that is itself sized by the label (a `ttk.Frame` gridded `sticky="w"`, or a `.pack()`ed child). `wrap_label` binds `<Configure>` on that container and sets `wraplength` from its width, so wrapping against a label-sized frame closes a feedback loop: label narrows → frame narrows → label narrows again, collapsing to a ~20px-wide, several-hundred-pixel-tall column (the Detection "Rewrite rounds per reply" caption and the Samplers "Enable Bad Words" description both hit this). `wrapped_checkbutton()` therefore wraps its description against *parent*, not against its own inner frame. Guarded by `tests/test_editor_readability_ui.py`
- **Spinboxes (v11.6.2)** → never create a bare `ttk.Spinbox`. The 'superhero' theme sets spinbox text white but leaves `fieldbackground` unset, so the field falls back to the native white entry background: white-on-white, an apparently empty box. Use `style=self.spinbox_style` (from `configure_spinbox_style()`, called once in `__init__` before any tab is built). It takes the colours from the theme's own `TEntry`, verifies them with `color_contrast()` (falls back to a known-good pair below 4.5:1), and `map`s a readable disabled state — the Regex Watch rounds box starts disabled. Guarded by `tests/test_editor_readability_ui.py`
- New tab → Add `ttk.Frame` + `notebook.add()` in `__init__()`
- Regex Watch panel (v11.6) → Detection tab `col1_frame`, a `LabelFrame` with `regex_watch_enabled_var_editor` + `_toggle_regex_watch_fields()`, a rounds `Spinbox` (`regex_watch_max_rounds_var_editor`), a Listbox backed by the plain list `self.regex_watch_patterns` (`_render_regex_patterns()`), an Entry plus Add/Update, Remove, Test and Restore buttons, and a status label. Handlers live in `ConfigEditor`; the save/validate helpers are MODULE-LEVEL (`regex_watch_rounds_value(editor)`, `regex_watch_enabled_value(editor)`, `regex_watch_pattern_values(editor)`) because the headless save-path tests drive a stub whose unknown attributes resolve to a Tk var — a method would be shadowed by that stub. **Patterns are saved VERBATIM**: `sanitize_input()` strips backslashes and quotes and would corrupt every expression
- Validation → `validate_config_handler()`
- Datasets tab → `DatasetsPanel(self.notebook)` added in `__init__()`; config gathered in `save_config_handler()` (`config_data['datasets'] = self.datasets_panel.get_config()`), loaded in `load_config_handler()` (`self.datasets_panel.load_config(...)`), validated in `validate_config_handler()`; `DatasetSettingsError` surfaces on the status bar

### `config_loader.py` [8]
**Purpose:** Load/save config.yml and profiles
**Key:** `ConfigLoader` class, `get()`, `set()`, `save_profile()`
**Edit for:**
- Config path → `__init__(path=...)`
- Profile operations → `save_profile()`, `load_profile_to_main_config()`, `delete_profile()`

### `dashboard.py` [9]
**Purpose:** Dashboard display, issue panels, progress bars, status labels
**Key:** `update_dashboard()`, `update_issue_graph()`, `create_modern_issue_panel()`
**Edit for:**
- New metric label → Add to `app_state.py` + update in `update_dashboard()`
- Issue graph series → `draw_issue_graph()` and `update_issue_graph()` each have parallel `counts` / `colors` / `labels` dicts that must contain the same keys (a missing key = KeyError; an uncounted series = IndexError). `offsets` is computed from `len(counts)`, so adding a series only requires extending the three dicts in BOTH functions
- Issue panel → `create_modern_issue_panel()` + `update_modern_issue_panel()`
- Progress bar style → `configure_animated_progress_styles()`, `get_progress_style_name()`

### `detection.py` [10]
**Purpose:** Issue detection (refusals, user speaking, slop, anti-slop, out-of-character drift, Regex Watch)
**Key:** `is_refusal()`, `is_user_speaking()`, `is_slop()`, `is_anti_slop()`, `is_out_of_character()`, `is_incomplete_quote()`, `is_regex_watch()`, `find_regex_matches()`, `validate_regex_patterns()`, `DEFAULT_REGEX_WATCH_PATTERNS`, `RegexWatchError`
**Regex Watch (v11.6):** `is_regex_watch(text, patterns)` mirrors the other `is_*()` shape (`(bool, [(pattern, sentence), ...])`, timestamp into `issue_timestamps['regex_watch']`, 1hr cutoff) but matches WHOLE-REPLY regular expressions instead of word-boundary phrases. `_compile_regex()` is `lru_cache`d and raises `RegexWatchError`; `validate_regex_patterns()` returns `(valid, [(pattern, message), ...])` and drops blanks/duplicates, so an editor save and a config load share one definition of "usable". An invalid pattern is skipped at run time (never raises), a zero-width match is ignored (it has nothing to rewrite and would loop). `find_regex_matches()` reports `(pattern, sentence, matched_text)`; the sentence is the one containing the match, capped at 300 chars
**Edit for:**
- Add detection type → New `is_*()` function + add key to `app_state.issue_timestamps`
- Regex Watch → `detection.py` (`is_regex_watch()` / `find_regex_matches()` / `validate_regex_patterns()` / `DEFAULT_REGEX_WATCH_PATTERNS`), `config/config.yml` (`detection.regex_watch.*`), `generate.py` (config load + validation, worker kwargs, metric card, `🔎 Regex Watch` tab + `_update_regex_watch_tab()`), `generation.py` (`apply_regex_watch()` / `rewrite_regex_watch_text()` + the four call sites), `config_editor.py` (Detection tab panel), `dashboard.py` (graph series + `clear_dashboard()`), `app_state.py` (counters, recent lists, `regex_watch_log`), `tests/test_regex_watch.py` / `tests/test_regex_watch_ui.py`
- Out-of-character drift → `is_out_of_character(answer, ooc_phrases)` mirrors the other `is_*()` shape (phrase-driven, word-boundary matching via `_candidate_patterns`, timestamp into `issue_timestamps['out_of_character']` on a 1hr cutoff) **but matches narration only**: `_strip_quoted_dialogue()` blanks every quoted span (straight and curly double/single quotes) before matching, so a listed phrase spoken by a character *inside* dialogue does NOT flag/regenerate the turn. Catches helpful-bot narration breaks ("As a helpful AI…", "Is there anything else I can help you with today?"), god-viewing the user, and fourth-wall drops that the LLM-judge consistency dimension then scores against the character card
- Phrase lists → Passed from config via `generate.py` → `worker()` (`current_ooc_phrases` / `current_ooc_fixes` / `current_ooc_enabled`)
- Regex Watch patterns → `generate.py` validates `detection.regex_watch.patterns` at run start (invalid entries are logged and dropped) and passes `current_regex_patterns` / `current_regex_watch_enabled` / `current_regex_watch_max_rounds` to `worker()` as kwargs
- GUI config → Config Editor Detection tab has an "Out-of-Character (OOC) Detection" LabelFrame (`col2_frame`) with an enable checkbox (`ooc_enabled_var_editor` + `_toggle_ooc_fields()`), phrases/fixes text areas (`ooc_phrases_text` / `ooc_fixes_text`), saved to / loaded from `detection.out_of_character.{enabled,phrases,fixes}`, validated in `validate_config_handler()`
- Timestamp tracking → `issue_timestamps[key].append(time.time())` with 1hr cutoff

### `generate.py` [11]
**Purpose:** Main UI, Tkinter app, worker thread management, queue setup
**Key:** `start_processing()`, `worker()` launch, UI widgets, `ConfigEditor`
**Edit for:**
- Queue dataset wiring → `configure_queue_datasets(app_state.task_queue, global_config.get('datasets', {}))` in `start_processing()` attaches an in-memory `DatasetTaskAttachments` service to the run's task queue; attachments die with the queue and are NOT persisted across app restarts
- Worker launch → `threading.Thread(target=worker, args=(...), kwargs={...})` in `start_processing()`. **New worker options MUST be added to the `kwargs=` dict, not `args=`** — the positional list length is fixed and the OOC/opening-mode params (`current_ooc_*`, `opening_mode_local`, etc.) are kwarg-sourced; adding a positional arg silently shifts every later slot and `tests/test_worker_launch.py` fails on the mismatch
- New UI tab → Add to `dashboard_notebook` + populate widgets; custom tabs (Quality / 🔍 Review / 🔎 Regex Watch) need their own gated per-tick refresher (`update_quality_tab()` / `_update_review_table()` / `_update_regex_watch_tab()`), called from the periodic GUI update loop and from `_on_dashboard_tab_changed()` so the incoming tab isn't stale — `update_dashboard()` itself only handles Totals/API issue panels and the graph
- New Treeview table → Give it a dedicated style with an explicit `font` and `rowheight` (see "Table row heights" above); a bare `ttk.Treeview` inherits the theme's cramped 20px row and clips its own text
- Quality tab → `update_quality_tab()` fills the dimension summary labels (from `quality.get_quality_stats()['dimension_averages']`) and the latest-50 scores Treeview from `app_state.quality_scores`
- **🔍 Review tab (v11.4)** → a fixed, non-scrolling split view (`page_scroll.grid_remove()` + a `review_page` frame) because nesting a scrolling table and a scrolling Text in the page's `ScrollableFrame` made both unusable. Widget registry keys: `review_page`, `review_tree`, `review_filter_var`, `review_count_label`, `detail_text`, `review_signature`, `review_detail_task`. Helpers: `_record_belongs_to_task()` (id matching, `_api` suffix for duplication rows), `_review_output_files()` / `_iter_review_records()` (main output + per-API files + database), `_find_review_record()`, `_format_review_conversation()` (ShareGPT `conversations[]` or `messages[]`), `_review_signature()` / `_update_review_table(force=False)` (rebuild only on change so a tick never clears the selection), `_show_review_detail(review_tree, force=False)` (skips re-render of the same selection so the scroll position survives), `_export_flagged_review()`. **The Treeview item id is the FULL task id** — never reintroduce a truncated display id in the item id or in the detail lookup. `<<TreeviewSelect>>` (single click) opens the transcript; the detail pane must keep its own scrollbar and stay out of the page canvas
- **🔎 Regex Watch tab (v11.6)** → a fixed, non-scrolling diagnostics area (`page_scroll.grid_remove()` + `regex_page`), not an issue panel: it is added to `tab_names` but skipped by the `tab_name not in (...)` issue-panel block. Widget registry keys: `regex_page`, `regex_pattern_tree`, `regex_hit_tree`, `regex_signature`. Refresher `_update_regex_watch_tab(force=False)` is called from the periodic GUI tick and from `_on_dashboard_tab_changed()`; like the Review tab it is gated to the visible tab and rebuilds only when its signature changes, so a tick never clears the selection. It reads the configured patterns from `global_config` and the hits from `app_state.regex_watch_log`. The metric card is `app_state.regex_watch_label` (`_create_ttkbs_metric(metrics_frame, "Regex Watch", "🔎")`)
- **Table row heights (v11.6.1)** → a `ttk.Treeview` with no explicit style inherits the theme's default `Treeview`, whose `rowheight` Tk falls back to a flat 20px for a 10pt font (17px linespace). That 3px of slack clips glyph ascenders/descenders and makes a table read as scrunched, overlapping text — the Review, Queue and Regex Watch tables each hit this. Every table needs its own style with an explicit `font` and a `rowheight` derived from `font.metrics('linespace') + padding`. Regex Watch uses `REGEX_TREE_STYLE` (`'RegexWatch.Treeview'`) plus the module-level `REGEX_TREE_BODY_FONT` / `REGEX_TREE_HEADING_FONT` constants, configured once by `_configure_regex_tree_style()` (called just above the `for tab_name in tab_names:` loop, so it runs before any tab is built) and applied to both trees via `style=REGEX_TREE_STYLE`. Columns are sized by `_configure_regex_tree_columns(tree, columns, widths=..., stretch_columns=...)`, which never lets a column fall below the width its own heading text needs. `tests/test_regex_watch_tab_ui.py` guards all of this
- **Dropped conversations (v11.4)** → `generation._quality_dropped(verdict)` gates both write paths (non-duplication final write, duplication per-turn write). A drop must never write a row, never `completed_task_ids.add()`, and never enter `quality_review_ids`
- Queue logic → Task tuple structure + `enqueue_opening_tasks()`
- DB export → `export_db_to_jsonl()`, `clear_database()`
- Quality tab layout → `_rebuild_quality_roleplay_layout()` builds the summary labels + score-table columns from `quality.active_scoring_dimensions()` (the toggle is read at build time and re-checked each tick, so a change in the Config Editor mid-run re-lays-out the Quality tab automatically; `update_quality_tab()` fills rows in that column order)

### `generation.py` [12]
**Purpose:** Worker loop, LLM calls, conversation generation, output writing
**Key:** `worker()`, `generate_answer_with_retries()`, `write_conversation()`, `call_slop_fixer_llm()`, `call_anti_slop_llm()`, `rewrite_regex_watch_text()`, `apply_regex_watch()`
**Edit for:**
- API call flow → `generate_answer_with_retries()` main loop
- Slop fixing → `call_slop_fixer_llm()` (API slot 5, index 4) / `call_anti_slop_llm()` (API slot 6, index 5)
- Regex Watch (v11.6) → `apply_regex_watch()` wraps `rewrite_regex_watch_text()`. A hit is rewritten by the SAME main API slot that produced the text (no fixer slot), up to `max_rounds` times, re-checking the whole reply after each round. A content rejection (empty, under half the original length, still matches its own pattern, unbalanced quotes) is FINAL for that attempt and returns `None`; only transport failures (network/timeout/non-200) are retried. It runs on answers (in `generate_answer_with_retries()`, AFTER every phrase-driven check has passed so it never races another fix), generated questions (`generate_question()`), user continuations (`generate_user_continuation()`) and opening-mode greetings/first lines (`_request_opening_message()`). `REGEX_WATCH_MAX_ROUNDS` is module scope because the `worker()` signature uses it as a default
- Output format → `write_conversation()` ShareGPT structure
- Opening mode → `generate_greeting()`, `generate_first_line()`, `enqueue_opening_tasks()`
- Quality scoring → `quality.score_conversation()` call (via `_score_and_store_quality()`) after conversation complete, passing `character_profile_text=character_injection` and `scene_characters=scene_character_names`
- **Judge gatekeeper (v11.4)** → `_score_and_store_quality()` returns the judge verdict (never `None`) and `_quality_dropped(verdict)` tells the write paths to skip the row. `quality_dropped_this_task` (set per task) suppresses the non-duplication final write, the duplication per-turn write and `completed_task_ids.add()`; the row is dropped, not requeued and not flagged. `quality_dropped` is persisted in `save_generation_state()`
- Turn completion check → `conversation_is_complete()`
- Conversation context for fixers → `call_slop_fixer_llm()` and `call_anti_slop_llm()` now accept `conversation_history` parameter to provide full conversation context for coherent rephrasing
- Character prompt injection → character profile block built in `worker()` (personality/traits are conditionally injected lines in the profile; `persona` = inner personality is deliberately EXCLUDED from the profile and re-injected later as an INNER PERSONA BREAKTHROUGH block once the scene reaches the configured slot). Setting resolution → a `settings_for_selected[]` array is computed per task before the profile loop: each selected character keeps its own non-empty `setting`, else samples from the `random_settings` pool (if enabled) or the `A standard indoor environment.` default; when >1 character is selected the ensemble is unified to one shared setting picked from the distinct candidate settings (order-preserving dedupe), which becomes both the injected `Setting:` line for every character and `meta_setting`
- Persona/emotional-state split → `_split_emotional_arc_entry()`, `_expand_emotional_arc()`, `_emotional_state_injection()`, `_user_emotional_state_injection()` (v10.6.0, user-side twin of `_emotional_state_injection`), `_persona_block_for_characters()` (all above `worker()`). User-side states (optional) are selected once per task when `user_emotional_states_enabled_local`, expanded across the USER turns (`_user_emotional_slot_state(user_turn_no)`: turn 1 = opening question/first line, continuation after answer k = turn k+1), and appended via `user_turn_note=` to the ephemeral user prompt of `generate_question()`, `generate_first_line()` and `generate_user_continuation()` — never stored in history, never in the system prompt. Recorded on metadata as `user_emotional_state`. The worker builds `base_scene_prompt` (system + lore + persona-free profile) once per task. It is the system message of every greeting, answer and user-continuation request in the task (a retry still appends its jailbreak or fix instruction to it); the generated question gets it plus `QUESTION_USER_SIDE`, and the first-line and fixer requests have their own system prompts. The per-reply notes come from `slot_notes(slot_no)`, which returns `(system note, depth-0 note)`: the INNER PERSONA BREAKTHROUGH block from `persona_breakthrough_turn` onward, then the slot's emotional state, which is a flat name or an arc stage (`'neutral -> anxious'` expanded across the conversation's assistant turns), each in the part its placement names (`prompts.character.persona_breakthrough_placement`, `prompts.emotional_states.placement`; `depth0` or `system`, default `depth0`, parsed by `note_placement()`). The depth-0 note is appended to the slot's final user message, the answer prompt or the opening-mode greeting prompt, after the history and never stored in it; the system note is appended to that slot's answer or greeting system prompt, before any retry fix, and the user continuation after the answer gets the same system prompt (without the fix), as in v10: if the continuation used the plain scene prompt instead, answers and continuations would alternate between two system prompts and a server that keeps one cached prompt per slot (llama-server) would recompute the history on every request. The generated question gets neither. With both on `depth0`, from one answer request to the next the system prompt and the history are the same text, which lets a server's prefix cache reuse them; keep the default that way. `placement_cache_warning()` estimates the history share a `system` placement makes the server compute again; `start_processing` logs it and shows a toast, and the config editor shows it under the placement dropdowns. Opening-mode greeting = slot 1, so conversation turn N lives at slot N+1.
- Generated first user message → when characters are profiled, its system prompt is the scene prompt plus `QUESTION_USER_SIDE`, which tells the model it writes only the user's side and that the user is not one of the characters
- Output metadata → `_build_record_metadata()` (20 flat keys, `meta_version` 3, same shape on every record, null-filled; `_safe_float()` keeps numeric types stable; v3 added `quality_voice_distinctiveness`/`quality_grounding`/`quality_persona_alignment`, `-1.0` whenever the roleplay toggle is off or unscored) + `_score_and_store_quality()` (quality scoring deduped by task_id; called BEFORE both write paths so metadata can carry scores; accepts `scene_characters=` for the voice-distinctiveness heuristic)
- Dataset binding → `bind_task_datasets(queue, task, ...)` near the top of the `worker()` task loop freezes one row per enabled source into an immutable `task_dataset_values` mapping for the life of the task (every turn, fix and retry); `validate_task_templates()` pre-checks the question, continuation, greeting and first-line templates up front. The static authored inputs in `dataset_authored_inputs` (system prompts, answer prompt, jailbreaks and fixes, lore, character fields, emotional states, name/setting pools, first-line styles) are rendered once per task with `render_task_inputs()`. The question, continuation, greeting and first-line templates are rendered where they are formatted, with `render_with_native_placeholders()`: `generate_question()`, `generate_user_continuation()`, `fill_greeting_prompt()` (called by `generate_greeting()`) and `generate_first_line()` take a `dataset_values=None` kwarg, and `None` skips rendering entirely, so generation without datasets behaves byte-identically to the pre-dataset engine. `docs/dataset-bindings.md` (Placeholder syntax) lists the fields

### `logging_config.py` [13]
**Purpose:** Shared logging utility
**Key:** `log_message()`, `LOG_FILE_PATH`, `DEBUG_LOGGING_ENABLED`
**Edit for:**
- Log path → `LOG_FILE_PATH`
- Enable debug → `DEBUG_LOGGING_ENABLED = True` or UI checkbox

### `quality.py` [14]
**Purpose:** Quality scoring (heuristic + LLM-based)
**Key:** `score_conversation()`, `_score_diversity()`, `_score_technical()`, `_score_with_llm()`, `_call_logic_judge()`, `_call_logic_judge_once()`, `_extract_json_object()`, `_repair_json()`, `_repair_truncated_json()`, `_lenient_json()`, `_salvage_scores_from_text()`, `_coerce_score()`, `judge_json_repair_attempts()`, `judge_max_tokens()`, `judge_request_timeout()`, `judge_structured_output()`, `_judge_response_format()`, `_heuristic_fallback_for_llm_dim()`, `active_scoring_dimensions()`, `_score_voice_distinctiveness()`, `_score_grounding()`, `_score_persona_alignment()`, `enqueue_quality_scoring()`, `_record_quality_result()`
**Edit for:**
- Scoring dimensions → `SCORING_DIMENSIONS` dict (weights) + `ROLEPLAY_SCORING_DIMENSIONS` (opt-in via `quality.roleplay_dimensions`; active set comes from `active_scoring_dimensions()`, which every score/stats/UI path must use instead of `SCORING_DIMENSIONS` directly)
- **Judge retry/drop policy (v11.4)** → `_call_logic_judge()` is a retry wrapper around `_call_logic_judge_once()`. Transient failures (network, timeout, non-200, unreadable envelope) retry `quality.judge_max_attempts` times (default 3) with `quality.judge_retry_delay` linear back-off; **invalid JSON is final and never retried**. `_judge_drop_reason()` decides the verdict: `invalid_json` / `logic_zero` (a judge `logic` score of 0, also final) / `judge_failed` (every retry exhausted). A drop returns `{"dropped": True, "drop_reason": ..., "method": "dropped"}` — `score_conversation()` never falls back to heuristics for a failed judge. A judge that is not configured is a SKIP, not a drop (`JUDGE_SKIP_FLAGS`). Adding a final-failure flag means adding it to `JUDGE_FINAL_FAILURE_FLAGS`; adding a skip flag means adding it to `JUDGE_SKIP_FLAGS`
- **Verdict delivery** → `enqueue_quality_scoring()` queues the job and waits on its `threading.Event` (the processor still runs one judge at a time) because the worker must know the verdict BEFORE it writes. `_record_quality_result()` stores scores in `app_state.quality_scores` and drops in `app_state.quality_dropped` / `app_state.dropped_task_ids`; a drop is never stored as a score and never flagged for review
- **Regex Watch stats (v11.6)** → `apply_regex_watch()` bumps `regex_watch_count_total` / `regex_watch_counts_per_api`, fills `recent_regex_total` / `recent_regex_per_api` (same shape as the other recent lists) and appends to `app_state.regex_watch_log` (bounded `MAX_REGEX_WATCH_LOG`, guarded by `regex_watch_log_lock`), which the Regex Watch dashboard area reads. `save_generation_state()` persists both counters
- **Judge reply parsing (v11.5)** → never call `json.loads` on a judge reply directly; go through `_extract_json_object(content, preferred_keys=dims)`, which returns `(dict|None, how)` and tries direct parse → balanced-object scan (`_json_objects_in`, survives thinking blocks/prose/fences, prefers a candidate carrying the score keys) → `_repair_json` → `_lenient_json` → `_repair_truncated_json` → `_salvage_scores_from_text` (all-or-nothing per dimension). `_coerce_score()` turns `"85"`, `"85/100"` and prose numbers into scores. `_call_logic_judge()` owns the policy: a token-limited reply (`finish_reason: length`) is TRANSIENT and retried with a doubled `max_tokens`; an unparsable reply gets `quality.judge_json_repair_attempts` format-repair rounds (the model's own text is echoed back via `repair_of=`) before it becomes the final `llm_invalid_json` drop. `llm_bad_envelope` / `llm_empty_reply` / `llm_truncated` are transient (never final, never skip). Raw replies, `finish_reason` and the recovery rung are appended to the per-slot debug log. `quality.judge_structured_output` (`auto` default) sends `response_format`; a server that rejects it is remembered in `_JUDGE_NO_RESPONSE_FORMAT` and the request repeated without it. Adding a new final-failure flag still means adding it to `JUDGE_FINAL_FAILURE_FLAGS`
- Heuristic logic → `_score_diversity()`, `_score_technical()`, `_heuristic_fallback_for_llm_dim()`, plus the roleplay heuristics (voice = bigram overlap between characters' attributed spoken lines via `_spoken_lines_by_character()`; grounding = profile-keyword coverage via `_profile_keywords()`; persona = Persona-section keyword timing around `prompts.character.persona_breakthrough_turn` via `_persona_sections()`)
- LLM scoring → `_score_with_llm()` (uses API slot from `quality.scoring_api` config); when the toggle is on it appends the three roleplay definitions + JSON fields to the judge prompt, raises the `max_tokens` default by 64/dim, and refines (replaces) the heuristic roleplay scores with the judge's; `_score_and_store_quality()` in `generation.py` forwards `scene_characters=scene_character_names`
- Tests → `tests/test_quality_roleplay.py` (toggle on/off, all three heuristics, judge prompt/refinement via stubbed `requests.post`); `tests/test_quality_json_robustness.py` (extraction ladder, salvage, coercion, format-repair rounds, truncation retry, `response_format` fallback, envelope errors)
- Persona-fidelity consistency → `score_conversation()` / `_score_with_llm()` accept `character_profile_text`; the actual character card (the `character_injection` block from `worker()`) is injected into the judge prompt so the `consistency` dimension measures persona/personality/traits fidelity, not just self-consistency. Capped by `quality.max_profile_chars_for_scoring` (default 4000). Without a profile the judge falls back to generic self-consistency language
- Consistency heuristic fallback → `_heuristic_fallback_for_llm_dim('consistency')` no longer returns a flat `70`; it scores down on well-known OOC markers (helpful-bot narration, "as <name>, I would" breaks, fourth-wall drops)
- Flags → Add to `flags` list in scoring functions

### `tests/test_api_profiles.py` [15]
**Purpose:** Unit tests for `api_profiles.py`
**Edit for:** Add new profile tests in `main()`

### `tests/test_opening_mode.py` [16]
**Purpose:** Tests for opening mode functionality
**Edit for:** Add new opening mode tests (greeting, first-line, worker integration). It also checks where the per-reply note goes, that the requests of a task share one system prompt, and the generated question's `QUESTION_USER_SIDE` line

### `text_utils.py` [17]
**Purpose:** Text processing utilities
**Key:** `normalize_quotes()`, `repair_straight_quotes()`, `remove_markdown()`, `remove_reasoning_text()`
**Edit for:**
- Add text cleanup → New function + call in `write_conversation()` processing loop
- Quote handling → `normalize_quotes()` (curly vs straight balance)

### `ui_layout.py` [18]
**Purpose:** Tkinter layout helpers
**Key:** `ScrollableFrame`, `ResponsiveGrid`, `NotebookNavigator`, `wrap_label()`
**Edit for:**
- Custom layout → Use `ResponsiveGrid` for reflowing widgets
- Scrollable area → Wrap in `ScrollableFrame`

### `tests/test_user_emotional_states_ui.py`
**Purpose:** Display-dependent regression test for the v10.6.0 user-side emotional-states save/load round trip in the REAL `ConfigEditor` (enable + type → save → `config.yml` holds states; reopen → states visible in the field; unmodified reopen+save → no clobber to `[]`). Catches the disabled-Text silent-drop bug class (edits dropped while the field is still `disabled`)
**Run:** `python -B tests/test_user_emotional_states_ui.py` (needs a display, e.g. under `xvfb-run`; skips cleanly without one)
**Edit for:** Add editor round-trip cases; keep it free of source data, generation runs, network and model APIs

### `tests/test_persona_arc.py` [19]
**Purpose:** Unit tests for the persona/emotional-arc helpers in `generation.py`
**Key:** `main()` covers `_split_emotional_arc_entry`, `_expand_emotional_arc`, `_emotional_state_injection`, `_persona_block_for_characters`
**Edit for:** Add arc-parsing/expansion/injection cases (arrows, stage distribution, header text, single vs multi-char labels)

### `tests/test_random_settings.py` [20]
**Purpose:** Standalone validation of the setting resolution/unification algorithm added to `worker()`
**Key:** `resolve_settings()` mirrors the per-task block; `run()` covers keep-own-setting, pool-sample-on-empty, multi-character unification, selection-off default, pool-disabled default, and ensemble-bounded unification across a seed range
**Edit for:** Add settings-resolution edge cases (blank/whitespace own setting, pool with duplicates, 3+ char ensembles)

### `tests/test_quality_roleplay.py`
**Purpose:** Headless unit tests for the opt-in roleplay quality dimensions in `quality.py` (no model/network/display; the LLM-judge path is exercised through a stubbed `requests.post`)
**Key:** covers the `quality.roleplay_dimensions` toggle (off by default; active dimension set + weight normalization), the three heuristics (`_score_voice_distinctiveness`, `_score_grounding`, `_score_persona_alignment` incl. the early-leak / never-surfaces / no-persona cases) and judge prompt construction + refinement when on
**Edit for:** Add roleplay-dimension heuristic or judge-prompt cases

### `tests/test_quality_judge_policy.py`
**Purpose:** Headless tests for the v11.4 judge retry/drop policy in `quality.py` and the worker's drop gate (no model/network/display; the judge is exercised through a stubbed `requests.post`)
**Key:** transient failures retry `quality.judge_max_attempts` times then drop; `judge_max_attempts` overrides the count; a good reply after a failed attempt is kept; invalid JSON and a `logic` score of 0 drop with no transient retry (v11.5 adds one format-repair round, and `judge_json_repair_attempts: 0` restores the immediate drop); a low non-zero logic score is kept; an unconfigured judge is a skip, not a drop; a drop is stored in `app_state.quality_dropped` and never in `quality_scores` / `quality_review_ids`; `generation._quality_dropped()` recognises a drop
**Edit for:** Add retry/drop-policy cases (new final-failure or skip flags, timeout handling, state bookkeeping)

### `tests/test_quality_json_robustness.py`
**Purpose:** Headless tests for the v11.5 judge JSON-robustness layer in `quality.py` (no model/network/display; the judge is exercised through a stubbed `requests.post`)
**Key:** `_extract_json_object()` recovers a reply from a thinking block, surrounding prose, ``` fences anywhere, trailing commas, smart quotes, Python literals and a cut-off tail; a candidate object carrying the score keys beats a decoy `{...}` in the prose; numeric salvage requires EVERY expected dimension; `_coerce_score()` handles `"85"` / `"85/100"` / prose; a format-repair round recovers the verdict and is bounded by `quality.judge_json_repair_attempts`; a token-limited reply is salvaged or retried with a doubled budget; `response_format` is sent and a rejecting endpoint is retried once without it and remembered; an unreadable envelope and an empty reply are transient
**Edit for:** Add extraction/repair cases whenever a new malformed-reply shape shows up in the logs; keep it free of source data, generation runs, network and model APIs

### `tests/test_regex_watch.py`
**Purpose:** Headless tests for Regex Watch (v11.6) — the pattern engine in `detection.py` and the rewrite pass in `generation.py` (no model/network/display; `requests.post` is stubbed)
**Key:** validation keeps order/drops blanks/duplicates/reports syntax errors and oversized patterns; every shipped default compiles; the six positive sentences match and clean prose does not; the reported sentence is the one holding the match (neighbours excluded) and is capped; zero-width and invalid patterns never hit and never raise; matching is case-insensitive; the timestamp is appended once and `record_timestamp=False` does not; a clean rewrite replaces the text; a rewrite that still matches, is empty, is much shorter than the original or unbalances quotes is discarded and the original kept; a non-200 is retried then given up; an empty pattern list and an unconfigured API are no-ops; a second shape introduced by the first rewrite is caught in the same pass; the loop is bounded by `max_rounds` and a leftover hit is reported `unresolved`; the shipped `config.yml` block is off by default, has 1-5 rounds and compiles
**Run:** `python -B tests/test_regex_watch.py`
**Edit for:** Add pattern-engine or rewrite-policy cases

### `tests/test_regex_watch_ui.py`
**Purpose:** Display-dependent regression test for the v11.6 Regex Watch editor panel in the REAL `ConfigEditor` (skips cleanly without a display)
**Key:** patterns with backslashes and quote classes survive save -> `config.yml` -> reopen byte for byte; a duplicate is refused; Remove deletes the selected pattern and the removal is saved; an invalid pattern is refused by Add and blocks the save; the toggle disables/enables the entry and Add button; Restore Defaults uses `detection.DEFAULT_REGEX_WATCH_PATTERNS`
**Run:** `python -B tests/test_regex_watch_ui.py` (needs a display, e.g. under `xvfb-run`)
**Edit for:** Add editor round-trip cases; keep it free of source data, generation runs, network and model APIs

### `tests/test_regex_watch_tab_ui.py`
**Purpose:** Display-dependent regression test for the `🔎 Regex Watch` dashboard tab layout (builds the REAL `generate.py` UI; skips cleanly without a display)
**Key:** both tables use `REGEX_TREE_STYLE` (not the theme's default `Treeview`); the style's `rowheight` fits the body font's linespace plus `MIN_ROW_SLACK`; the rendered rows really are that tall; no column is narrower than its own heading text needs; both stacked tables share one row height
**Run:** `python -B tests/test_regex_watch_tab_ui.py` (needs a display, e.g. under `xvfb-run`)
**Edit for:** Add tab-layout cases; keep it free of source data, generation runs, network and model APIs

### `tests/test_editor_readability_ui.py`
**Purpose:** Display-dependent regression test for the v11.6.2 Configuration Editor readability fixes (builds the REAL `ConfigEditor` under the production 'superhero' theme; skips cleanly without a display)
**Key:** no mapped label in any tab is taller than twice its width (a "skyscraper"), checked at a normal size AND at the 760x520 minimum; the Regex Watch patterns caption has a real `wraplength`; the Bad Words description is not a skyscraper; both spinboxes keep >=4.5:1 field/text contrast and >=3:1 while disabled; the rounds spinbox accepts typed input once enabled; every `TSpinbox` in the editor carries `SPINBOX_FIELD_STYLE`
**Run:** `python -B tests/test_editor_readability_ui.py` (needs a display, e.g. under `xvfb-run`)
**Edit for:** Add editor layout/contrast cases; keep it free of source data, generation runs, network and model APIs

### `tests/test_review_tab_ui.py`
**Purpose:** Display-dependent regression test for the v11.4 Review tab (builds the REAL `generate.py` UI; skips cleanly without a display)
**Key:** full-task-id Treeview items + single-click selection; transcript rendered from the output JSONL; the detail pane is read-only and scrollable; a refresh tick keeps the selection and the scroll position; Export Flagged → JSONL writes every row with `_quality_score`; the filter narrows and restores the list
**Run:** `python -B tests/test_review_tab_ui.py` (needs a display, e.g. under `xvfb-run`)
**Edit for:** Add Review-tab behaviour cases; keep it free of source data, generation runs, network and model APIs

### `tests/test_worker_launch.py` [21]
**Purpose:** AST-level regression check that the worker launch in `generate.py` matches the `worker()` signature in `generation.py`
**Key:** Rebuilds the `worker()` signature from its AST, extracts the actual `threading.Thread(target=worker, args=..., kwargs=...)` call from `start_processing()`, and binds them via `inspect.Signature.bind()`; then asserts the OOC kwargs (`current_ooc_phrases`/`current_ooc_fixes`/`current_ooc_enabled`) are supplied by keyword from same-named locals (never positionally) — no app config, GUI, or network code is evaluated
**Run:** `python -B tests/test_worker_launch.py`
**Edit for:** Extend per-kwarg source assertions when new params are moved into / added to the `kwargs=` dict at the launch site

### `dataset_sources.py` [22]
**Purpose:** Dataset readers for the optional dataset-binding foundation (CSV/TSV/JSON/JSONL stdlib; Parquet via optional `pyarrow`; Hugging Face Hub / saved-`DatasetDict` via optional `datasets` 4.x). No Tk/app/config imports; importing makes no network requests
**Key:** `SourceSpec`, `DatasetSchema`, `DatasetRow`, `DatasetSource` (ABC), `LocalFileSource`, `HuggingFaceSource`, `SavedDatasetSource`, `open_source()`, `DatasetDependencyError`
**Edit for:**
- Add new source kind → New `DatasetSource` subclass + register in `open_source()` + `dataset_settings._SELECTOR_DEFAULTS` + `datasets_panel._KINDS`
- Schema discovery → `schema()` returns column names + opaque fingerprint, never record values (JSON reads records internally to find keys, discards them)
- Optional deps → Guarded by `_optional_module()` / `_datasets_backend()`; absent dependency → `DatasetDependencyError`

### `dataset_settings.py` [23]
**Purpose:** Names-only settings for the `datasets` config subtree (source selectors, cached column names, aliases, visibility). No GUI, file, or network I/O
**Key:** `normalize_config()`, `validate_config()`, `source_selector()`, `source_spec()`, `apply_schema()`, `visible_column_names()`, `with_visible_columns()`, `DatasetSettingsError`
**Edit for:**
- Add new source selector field → `_SELECTOR_DEFAULTS` + `source_selector()` + `validate_config()`
- Readiness rule → An *enabled* source with aliases must be complete; disabled/alias-free sources may stay drafts. Hiding a column is presentation-only, never changes alias mapping

### `dataset_bindings.py` [24]
**Purpose:** Freeze dataset values for one logical generation task, independently of workers (no GUI/worker/app-state imports). One `DatasetBindingManager` per run
**Key:** `DatasetBindingManager` (`bind(task_id)`, `snapshot()`), `TaskBinding` (`to_dict()`/`from_dict()` JSON snapshots), `SourceBinding`, `SourceProvenance`, `cell_to_text()`, `DatasetExhaustedError`
**Edit for:**
- Row advancement → Each source advances sequentially once per newly bound task; rows stage until EVERY source succeeds — exhaustion or invalid data never cycles, skips, or partially publishes
- Lock ordering → Allocation lock released before the caller renders prompts or calls model APIs
- Snapshot format → Serializes actual values (foundation for later save/resume; does not restore cursors/pending jobs/conversation progress)

### `dataset_templates.py` [25]
**Purpose:** Dataset placeholders (`{{ds:alias}}`) and explicit boundaries for authored-prompt rendering. No app/GUI/network/RNG imports
**Key:** `render_template()` (single-pass), `render_with_native_placeholders()` (preserves the generator's ordered native `str.replace` cascade), `validate_alias()`, `dataset_placeholders()`, `DatasetTemplateError`
**Edit for:**
- Add placeholder syntax → `_TOKEN_START` regex + `render_template()`; keep single-pass guarantee (inserted values are never re-interpreted)
- Reserved namespace → Whitespace-tainted `{{ ds:... }}` variants raise instead of silently passing to the model; the compatibility renderer uses literal native replacements that can match `{context}` inside `{{context}}`

### `dataset_tasks.py` [26]
**Purpose:** Attach optional dataset values to the generator's existing task identities. One attachment service per existing queue; original task tuples/IDs/retry rules untouched; in-memory only
**Key:** `configure_queue_datasets()`, `bind_task_datasets()`, `render_task_inputs()`, `validate_task_templates()`, `DatasetTaskAttachments`
**Edit for:**
- Queue wiring → `configure_queue_datasets()` called from `generate.py` at processing start; readers opened lazily by the first worker that needs a binding; `_OwnedReader` closes each reader once at queue lifetime end (an old worker finishing its queue is never cut off by a replacement run)
- Task metadata entry → A *reference* to the immutable binding, not a mutable global "current row"

### `datasets_panel.py` [27]
**Purpose:** Names-only dataset configuration panel (the Configuration Editor **Datasets** tab), independent of generation workers
**Key:** `DatasetsPanel`, `load_config()` / `get_config()` / `validate()`, `_metadata_worker()` (max 2 daemon threads)
**Edit for:**
- Add source kind UI → `_KINDS` tuple + kind/label maps; Hub sources expose config/split/revision, `hf_disk` exposes its split selector
- Metadata inspection → Runs in background threads; Tk widgets touched only by the UI thread; column import calls the schema interface and never previews cell values

### `dataset_column_menu.py` [28]
**Purpose:** Bounded, searchable column-visibility picker for the Datasets panel — one Treeview shows all column names regardless of count
**Key:** `ColumnVisibilityMenu` (`set_columns()`, `on_change(None | tuple_of_names)`) — `None` = all columns (incl. imported later); explicit empty = none
**Edit for:** Selection semantics in the docstring: `set_columns` never fires `on_change`; canonicalization to `None` when every current column is selected; no application-wide event bindings

### `tests/test_dataset_sources.py` [29]
**Purpose:** Unit tests for `dataset_sources.py` (local CSV/TSV/JSON/JSONL readers, schema discovery, fingerprints, error paths)
**Edit for:** Add reader/format cases with temporary fixture files

### `tests/test_dataset_settings.py` [30]
**Purpose:** Unit tests for `dataset_settings.py` normalization/validation (selectors, aliases, visibility, readiness rules)
**Edit for:** Add setting-shape and validation-failure cases

### `tests/test_dataset_bindings.py` [31]
**Purpose:** Unit tests for `dataset_bindings.py` (sequential allocation, repeat-bind idempotency, exhaustion, snapshot round trip, thread safety)
**Edit for:** Add binding-lifecycle cases (use `ThreadPoolExecutor` for concurrency)

### `tests/test_dataset_templates.py` [32]
**Purpose:** Unit tests for `dataset_templates.py` rendering (single-pass, native-placeholder compatibility, alias validation, reserved-namespace typos)
**Edit for:** Add render/template edge cases

### `tests/test_dataset_tasks.py` [33]
**Purpose:** Unit tests for `dataset_tasks.py` (queue attachment lifecycle, lazy reader open/close, same-run reuse, GC behavior of weak references)
**Edit for:** Add task-attachment lifecycle cases

### `tests/test_dataset_backends.py` [34]
**Purpose:** Backend-coverage tests for optional readers (Parquet via `pyarrow`, Hub / saved-`DatasetDict` via `datasets` 4.x) incl. resource lifecycle on rename/delete; never touches the network or a model API
**Edit for:** Extend optional-backend cases; hard-dep backends (csv/json/jsonl) always run, optional ones skip when the package is missing

### `tests/test_dataset_column_menu.py` [35]
**Purpose:** Unit tests for `dataset_column_menu.py` (selection semantics, large-column boundedness, grab/focus handling); skips cleanly without a display
**Edit for:** Add picker behavior cases (run on a display for zero skips)

### `tests/test_datasets_panel.py` [36]
**Purpose:** Unit tests for `datasets_panel.py` (add/edit/import/alias/save-load flows, background metadata workers); synthetic column names only, optional screenshots via `READYART_GUI_SCREENSHOTS`
**Edit for:** Add panel-flow cases

### `tests/test_dataset_pipeline.py` [37]
**Purpose:** Pipeline-level simulation of dataset integration into worker-shaped request stages (question/answer/continuation/cleanup); all model replies are fixtures, expected request bodies assembled independently of the renderer
**Edit for:** Add stage-level pipeline scenarios (scheduler/transport/retry/cache are small simulations, not production impls)

### `tests/test_dataset_compatibility.py` [38]
**Purpose:** Bounded equivalence check: the real worker/request builders/fixers/state saver/output writer run twice — baseline with explicitly populated authored inputs vs candidate fed the same values through real CSV/JSONL sources (fake HTTP, no cache hits/waits/display)
**Edit for:** Extend side-by-side scenarios; this proves byte-level compatibility, not exhaustive equivalence

### `docs/dataset-bindings.md` [39]
**Purpose:** User/design doc for the Datasets tab (workflow, placeholders, what is/isn't persisted; notes lineage from upstream `5993702`)
**Edit for:** Update when Datasets UX or persistence semantics change

### `requirements-datasets.txt` [40]
**Purpose:** Optional extras for Hugging Face Hub, local Parquet, and saved-HF dataset support (includes base `requirements.txt` + `datasets==4.4.2`; requires Python 3.9+). Base `requirements.txt` only pins `requests>=2.32.4,<3`
**Edit for:** Bump optional backend pins here, never in `requirements.txt`

### `tools/check_dataset_hub.py` [41]
**Purpose:** Opt-in live Hub reader check — prints schema/provenance and verifies live-row binding, same-job reuse, snapshot round trip, and literal rendering; never prints dataset cells, calls a model, or mutates app config
**Usage:** `python -B tools/check_dataset_hub.py --repository owner/dataset --split train --column text [--config X] [--revision Y] [--rows N]` (install `requirements-datasets.txt` first; unit tests never invoke this)

## Common Tasks Quick Reference

| Task | Files to Edit |
|------|---------------|
| Add new API slot | `config/config.yml` (`api.apis[]`), `config_editor.py` (UI), `app_state.py` (counters), `generation.py` (worker logic) |
| Add detection type | `config/config.yml` (phrases/fixes/enabled), `detection.py` (`is_*()`), `app_state.py` (timestamps/counters), `generation.py` (worker kwargs + check call), `dashboard.py` (display), `config_editor.py` (Detection tab panel + save/load/validate) — see e.g. OOC Drift |
| Add config option | `config/config.yml` (default value), `config_editor.py` (UI + save/load), `config_loader.py` (if new structure), `generation.py` (pass to worker) |
| Add quality dimension | `quality.py` (`SCORING_DIMENSIONS` + `HEURISTIC_DIMENSIONS`/`LLM_DIMENSIONS` sets, scoring logic; opt-in sets like `ROLEPLAY_SCORING_DIMENSIONS` go through `active_scoring_dimensions()` + `is_roleplay_scoring_enabled()`), `generation.py` (`_build_record_metadata()` schema, bump `_METADATA_SCHEMA_VERSION`), `generate.py` (Quality tab via `_rebuild_quality_roleplay_layout()`, already dynamic), `config_editor.py` (only if user-togglable), `config/config.yml` (default), `tests/test_opening_mode.py` (metadata contract) |
| Modify output format | `generation.py` (`write_conversation()`) |
| Add slop/anti-slop phrase | `config/config.yml` (`detection.*.phrases`) → `detection.py` phrase lists |
| Add/modify a Regex Watch pattern | `config/config.yml` (`detection.regex_watch.patterns[]`) and `detection.py` (`DEFAULT_REGEX_WATCH_PATTERNS` — the Restore Defaults source; keep the two in sync), `config_editor.py` (Detection tab panel, `_restore_default_regex_patterns()`), `tests/test_regex_watch.py` |
| Change how Regex Watch rewrites text | `generation.py` (`rewrite_regex_watch_text()` / `apply_regex_watch()`), `config/config.yml` (`detection.regex_watch.max_rounds`), `generate.py` (`current_regex_watch_max_rounds`), `tests/test_regex_watch.py` |
| Fix quote balancing | `text_utils.py` (`normalize_quotes()`, `repair_straight_quotes()`) |
| Add UI metric | `app_state.py` (widget ref), `generate.py` (create widget), `dashboard.py` (update) |
| Change rate limit | `config/config.yml` (`api.apis[].rate_limit_rpm`), `config_editor.py` (UI field), `api_handler.py` (`set_rate_limit()`) |
| Modify retry logic | `generation.py` (`generate_answer_with_retries()`, `MAX_TASK_REQUEUES`) |
| Change judge retry/drop policy (v11.4) | `quality.py` (`QUALITY_JUDGE_MAX_ATTEMPTS`, `_call_logic_judge()`, `_judge_drop_reason()`, `JUDGE_FINAL_FAILURE_FLAGS` / `JUDGE_SKIP_FLAGS`), `generation.py` (`_score_and_store_quality()`, `_quality_dropped()`, both write paths), `config/config.yml` (`quality.judge_max_attempts` / `judge_retry_delay` / `result_timeout`), `config_editor.py` (three fields), `tests/test_quality_judge_policy.py` |
| Change how the judge's JSON is parsed (v11.5) | `quality.py` (`_extract_json_object()` ladder plus `_repair_json()` / `_lenient_json()` / `_repair_truncated_json()` / `_salvage_scores_from_text()` / `_coerce_score()`), `config/config.yml` (`quality.judge_json_repair_attempts` / `judge_max_tokens` / `judge_request_timeout` / `judge_structured_output`), `config_editor.py` (four fields), `tests/test_quality_json_robustness.py` / `tests/test_quality_judge_settings_ui.py` — keep the rule that only a genuinely unparsable reply drops a row |
| Change the Review tab | `generate.py` (`_update_review_table()`, `_show_review_detail()`, `_export_flagged_review()`, `_iter_review_records()`, the `elif tab_name == "🔍 Review"` layout block), `tests/test_review_tab_ui.py` |
| Fix a cramped / clipped Treeview table | `generate.py` — give the table its own style with an explicit `font` + `rowheight` from `font.metrics('linespace')`, and size columns from the heading text. Regex Watch does this via `REGEX_TREE_STYLE`, `_configure_regex_tree_style()`, `_configure_regex_tree_columns()`; guarded by `tests/test_regex_watch_tab_ui.py` |
| Fix a label rendering one character per line (a "skyscraper") | `config_editor.py` — every `wraplength=1` label needs a `wrap_label(label, container, ...)` call, and the container must NOT be sized by that label (never a `ttk.Frame` gridded `sticky="w"`, never a frame packed around it). `wrapped_checkbutton()` wraps against `parent` for this reason. Guarded by `tests/test_editor_readability_ui.py` |
| Fix an unreadable / white-on-white spinbox | `config_editor.py` — `configure_spinbox_style()` + `SPINBOX_FIELD_STYLE`; pass `style=self.spinbox_style` to every `ttk.Spinbox`. The theme sets white text but no `fieldbackground`. Guarded by `tests/test_editor_readability_ui.py` |
| Add API profile | `config/api_profiles.yml` (new profile definition), no code change needed |
| Modify API profile params | `config/api_profiles.yml` (`allow`, `rename` lists), `api_profiles.py` (if transform logic changes) |
| Add character profile | `config/config.yml` (`prompts.character.characters[]`), `config_editor.py` (UI fields) |
| Add character field | `config/config.yml` (default value per character), `config_editor.py` (`_add_character_row()` long-field loop + `_get_current_editor_config_data()` gather + keep-list + `_sync_character_text_values()` + legacy fallback in `load` path), `generate.py` (legacy fallback), `generation.py` (prompt injection in profile block) |
| Modify opening mode prompts | `config/config.yml` (`prompts.greeting`, `prompts.first_line`, `prompts.greeting_context`, `prompts.first_line_styles`) |
| Add random names feature | `config/config.yml` (`prompts.random_names.enabled`, `prompts.random_names.names[]`), `config_editor.py` (UI toggle + text area), `generate.py` (load config + pass to worker), `generation.py` (use random names in character profile) |
| Add random settings pool | `config/config.yml` (`prompts.random_settings.enabled`, `prompts.random_settings.settings[]`), `config_editor.py` (UI toggle + text area + save/load/toggle/validation), `generate.py` (load config + pass to worker), `generation.py` (sample empty character settings + unify multi-character ensembles; see setting resolution above) |
| Add/modify persona breakthrough or emotional arc | `config/config.yml` (`prompts.character.persona_breakthrough_turn` and `persona_breakthrough_placement`, `prompts.emotional_states.states[]` — a state may be `'->'`-joined stages — and `placement`), `config_editor.py` (spinbox under Character Engine + states label + the two placement dropdowns and the cache-warning label), `generate.py` (load + pass to worker kwargs), `generation.py` (`_split_emotional_arc_entry`/`_expand_emotional_arc`/`_emotional_state_injection`/`_persona_block_for_characters` + `note_placement`/`placement_cache_warning` + `slot_notes()` in `worker()`), `tests/test_opening_mode.py` and `tests/test_persona_arc.py` |
| Add/modify user-side emotional states/arcs (v10.6.0) | `config/config.yml` (`prompts.user_emotional_states.{enabled,states[]}`, off by default), `config_editor.py` ("Enable User Emotional States" checkbox + text area in the Character Engine tab + save/load/toggle/validation; **the field starts disabled — enable it before any `delete`/`insert`, or this Tk build drops the edits silently**), `generate.py` (load + pass `user_emotional_states_enabled_local`/`user_emotional_states_list_local` to worker kwargs), `generation.py` (`_user_emotional_state_injection`, the per-task user-turn plan in `worker()`, `user_turn_note=` on `generate_question`/`generate_first_line`/`generate_user_continuation`, `dataset_authored_inputs` entry, `user_emotional_state` metadata field + `_METADATA_SCHEMA_VERSION` bump), `tests/test_opening_mode.py` (`test_user_emotional_arc_requests` + wiring kwarg set + meta_version), `tests/test_persona_arc.py` (injection text) and `tests/test_user_emotional_states_ui.py` (editor save/load round trip) |
| Toggle conversation history for fixers | `config/config.yml` (`generation.use_conversation_history_for_fixers`), `config_editor.py` (UI checkbox), `app_state.py` (flag), `generation.py` (conditional pass to fixers) |
| Add/modify output metadata field | `generation.py` (`_build_record_metadata()` schema + `_safe_float()`), keep same-keys/same-types/null-filled invariant; config flag `generation.include_metadata` |
| Add dataset source type | `dataset_sources.py` (new `DatasetSource` subclass + register in `open_source()`), `dataset_settings.py` (`_SELECTOR_DEFAULTS`, `source_selector`), `datasets_panel.py` (`_KINDS` list), `tests/test_dataset_sources.py` (or `tests/test_dataset_backends.py` for an optional package), `requirements-datasets.txt` (if it needs a new package) |
| Add dataset column→alias mapping | `config_editor.py` (Datasets tab) → `datasets_panel.py` (Apply/Copy placeholder) → `dataset_settings.py` (validation) — placeholders `{{ds:alias}}` are then usable in any authored prompt field |
| Modify dataset placeholder rendering | `dataset_templates.py` (`render_template()` / `render_with_native_placeholders()` / `validate_alias()`); worker-side call sites in `generation.py` (`render_task_inputs()`) |
| Change per-task row allocation | `dataset_bindings.py` (`DatasetBindingManager.bind()`), `dataset_tasks.py` (`bind_task_datasets()`) — keep exhaustion/invalid-data rules: no silent cycling, skipping, or partial publish |
| Run dataset tests | `for t in tests/test_dataset_sources.py tests/test_dataset_settings.py tests/test_dataset_bindings.py tests/test_dataset_templates.py tests/test_dataset_tasks.py tests/test_dataset_backends.py tests/test_dataset_column_menu.py tests/test_datasets_panel.py tests/test_dataset_pipeline.py tests/test_dataset_compatibility.py; do python -B "$t"; done` (live Hub check: `tools/check_dataset_hub.py`, opt-in only) |
| Run GUI layout tests | `tools/check_gui_layout.py --check --config-editor --screenshots` |
| Change what a request contains (prompt text, message order or role) | `generation.py`, `tests/test_opening_mode.py` |

## Key State Variables (`app_state.py`)

**Counters:** `total_attempts_global`, `refusal_count_total`, `slop_count_total`, `error_count_total`, `anti_slop_count_total`, `ooc_count_total`, `logic_issue_count_total`, `regex_watch_count_total`, `total_input_tokens`, `total_output_tokens`

**Per-API:** `*_per_api` dicts for indices 0-5

**Recent issues:** `recent_*_total`, `recent_*_per_api` (MAX_RECENT = 10); includes `recent_ooc_total` / `recent_ooc_per_api`, `recent_logic_total` / `recent_logic_per_api` and `recent_regex_total` / `recent_regex_per_api`

**Regex Watch log (v11.6):** `regex_watch_log` — a bounded ring buffer (`MAX_REGEX_WATCH_LOG`) of `(pattern, matched_text, api_slot_idx, status)` entries, guarded by `regex_watch_log_lock`; the `🔎 Regex Watch` tab reads it. Status is `rewritten` or `unresolved`

**Issue timestamp keys:** `refusals`, `user_speaking`, `slop`, `errors`, `anti_slop`, `incomplete_quotes`, `out_of_character`, `logic_issues`, `regex_watch`

**Locks:** `stats_lock`, `issue_timestamps_lock`, `quality_lock`, `task_queue_ui_lock`, `api_circuit_breaker_lock`

**GUI handles:** `root`, `dashboard_notebook`, `slot_widgets`, `rate_limit_labels`, `quality_*_label`

**Flags:** `stop_processing`, `pause_processing`, `processing_active`, `quality_enabled`, `quality_output_filter`, `use_conversation_history_for_fixers`

**Judge drops (v11.4):** `quality_dropped` ({task_id: reason}) and `dropped_task_ids` (set), both guarded by `quality_lock`; populated by `quality._record_quality_result()`, cleared by `reset_all_stats_and_history()`, persisted as `quality_dropped` in `generation_state.json`.

## API Slots Index
Indexes count from 0 in code (`api_slot_idx`, `*_per_api` keys). The GUI, the README and most log messages count from 1, so index 4 is "API Slot 5"; debug-log file names (`_api_slot_<index>`) use the index.
- **0-3:** Main generation APIs (duplication mode) — API Slots 1-4
- **4:** Slop Fixer LLM — API Slot 5
- **5:** Anti-Slop Fixer LLM (independent) — API Slot 6
- **6:** Quality scoring LLM (if configured separately; `quality.scoring_api.slot_idx`) — API Slot 7

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

## Config Paths (`config_loader.py`)
- Main config: `config/config.yml`
- API Profiles: `config/api_profiles.yml`
- Profiles: `config/profiles/*.yml`
- Environment: API keys via `os.getenv()` or inline in config

## Tools

### `tools/check_dataset_hub.py`
**Purpose:** Opt-in live Hugging Face Hub reader verification (schema/provenance printout + binding invariants); no cells, no model, no config mutation
**Usage:** `python -B tools/check_dataset_hub.py --repository owner/dataset --split train --column text --rows 2`
**Flags:** `--config`, `--revision`, `--rows N` (≥1), repeatable distinct `--column`; exits 1 on any invariant failure

### `tools/check_gui_layout.py`
**Purpose:** Isolated GUI layout testing without running generation or saving production state
**Usage:** `python tools/check_gui_layout.py --check --config-editor --screenshots`
**Flags:**
- `--check`: Exit nonzero if any layout assertion fails
- `--config-editor`: Include config editor tab and character profile tests
- `--screenshots`: Capture PNG screenshots of each resize scenario
- `--size WIDTHxHEIGHT`: Custom window size (repeatable, default: 900x600, 1024x700, 1366x700, 1916x1126)
- `--scale FLOAT`: Tk pixels/point scale (diagnostic, not Windows DPI emulation)
- `--character-count N`: Number of fixture character profiles (1-100, default: 10)
- `--allow-below-minimum`: Override production window minima for diagnostics
**Output:** `./layout-check/layout_measurements.json` with widget visibility, overlaps, scroll region, and check results
