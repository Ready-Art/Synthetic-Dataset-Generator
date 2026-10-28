AGENTS.md - Codebase Cheat Sheet

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
- Sampler params → `samplers.temperature`, `samplers.top_p`, `samplers.repetition_penalty`
- Character profiles → `prompts.character.characters[]` (name, race, job, hair, hair_color, clothing, appearance, persona, personality, traits, backstory, setting) — `persona` = inner personality (private self), `personality` = outward behavior (observed manner); `persona` is withheld from the profile and re-injected as a breakthrough block from `prompts.character.persona_breakthrough_turn` onward
- Emotional states/arcs → `prompts.emotional_states.states[]` — each entry is a flat state or a `'->'`-joined progression (`'neutral -> tense -> angry'`) expanded across the conversation's assistant turns
- Random names → `prompts.random_names.enabled`, `prompts.random_names.names[]` (optional list of names to randomly assign to characters, overrides character Name field when enabled)
- Random settings pool → `prompts.random_settings.enabled`, `prompts.random_settings.settings[]` (analogous to `random_names`). When `setting_enabled` is true and a selected character's own `setting` is empty, a setting is sampled from this pool (when enabled) instead of the hardcoded default. For a multi-character ensemble the per-character settings are unified to ONE shared setting picked from the ensemble, so the whole scene happens in a single location
- Conversation history for fixers → `generation.use_conversation_history_for_fixers` (whether to pass conversation context to slop/anti-slop fixer LLMs)
- Output metadata → `generation.include_metadata` (flat, schema-stable provenance block per output.jsonl record; trainer-safe; never modifies `conversations[]`; false = legacy two-field records)
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
- Rate limit logic → `wait_if_needed()` (line 33-56)
- Cache TTL → `set_cached_response()` ttl param (line 82)
- Response time tracking → `api_response_times_per_slot` (line 13)

### `api_profiles.py` [5]
**Purpose:** Filter request payloads per API endpoint compatibility
**Key:** `apply_profile_for_slot()`, `detect_profile()`, `set_slot_profile()`
**Edit for:**
- Add new API profile → `config/api_profiles.yml` (not code)
- Param rename logic → `_RENAME_TRANSFORMS` (line 34)
- Always-allowed keys → `_ALWAYS_ALLOWED` (line 23)

### `app_state.py` [6]
**Purpose:** Shared runtime state, counters, locks, GUI handles
**Key:** All globals (counters, locks, widget refs)
**Edit for:**
- Add new counter → Add to module level + init in `reset_all_stats_and_history()`
- Add new lock → `threading.Lock()` at module level
- GUI widget refs → Assign in `generate.py` after widget creation

### `config_editor.py` [7]
**Purpose:** Configuration Editor UI window
**Key:** `ConfigEditor` class, `save_config_handler()`, `load_config_handler()`, `DatasetsPanel` tab
**Edit for:**
- Add new config field → Add UI widget + bind to `save_config_handler()` + `load_config_handler()`
- New tab → Add `ttk.Frame` + `notebook.add()` in `__init__()`
- Validation → `validate_config_handler()`
- Datasets tab → `DatasetsPanel(self.notebook)` added in `__init__()` (~line 959); config gathered in `save_config_handler()` (`config_data['datasets'] = self.datasets_panel.get_config()`), loaded in `load_config_handler()` (`self.datasets_panel.load_config(...)`), validated in `validate_config_handler()`; `DatasetSettingsError` surfaces on the status bar

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
**Purpose:** Issue detection (refusals, user speaking, slop, anti-slop, out-of-character drift)
**Key:** `is_refusal()`, `is_user_speaking()`, `is_slop()`, `is_anti_slop()`, `is_out_of_character()`, `is_incomplete_quote()`
**Edit for:**
- Add detection type → New `is_*()` function + add key to `app_state.issue_timestamps`
- Out-of-character drift → `is_out_of_character(answer, ooc_phrases)` mirrors the other `is_*()` shape (phrase-driven, word-boundary matching via `_candidate_patterns`, timestamp into `issue_timestamps['out_of_character']` on a 1hr cutoff) **but matches narration only**: `_strip_quoted_dialogue()` blanks every quoted span (straight and curly double/single quotes) before matching, so a listed phrase spoken by a character *inside* dialogue does NOT flag/regenerate the turn. Catches helpful-bot narration breaks ("As a helpful AI…", "Is there anything else I can help you with today?"), god-viewing the user, and fourth-wall drops that the LLM-judge consistency dimension then scores against the character card
- Phrase lists → Passed from config via `generate.py` → `worker()` (`current_ooc_phrases` / `current_ooc_fixes` / `current_ooc_enabled`)
- GUI config → Config Editor Detection tab has an "Out-of-Character (OOC) Detection" LabelFrame (`col2_frame`) with an enable checkbox (`ooc_enabled_var_editor` + `_toggle_ooc_fields()`), phrases/fixes text areas (`ooc_phrases_text` / `ooc_fixes_text`), saved to / loaded from `detection.out_of_character.{enabled,phrases,fixes}`, validated in `validate_config_handler()`
- Timestamp tracking → `issue_timestamps[key].append(time.time())` with 1hr cutoff

### `generate.py` [11]
**Purpose:** Main UI, Tkinter app, worker thread management, queue setup
**Key:** `start_processing()`, `worker()` launch, UI widgets, `ConfigEditor`
**Edit for:**
- Queue dataset wiring → `configure_queue_datasets(app_state.task_queue, global_config.get('datasets', {}))` at processing start (~line 1216) attaches an in-memory `DatasetTaskAttachments` service to the run's task queue; attachments die with the queue and are NOT persisted across app restarts
- Worker launch → `threading.Thread(target=worker, args=(...), kwargs={...})` in `start_processing()` (~line 1480). **New worker options MUST be added to the `kwargs=` dict, not `args=`** — the positional list length is fixed and the OOC/opening-mode params (`current_ooc_*`, `opening_mode_local`, etc.) are kwarg-sourced; adding a positional arg silently shifts every later slot and `test_worker_launch.py` fails on the mismatch
- New UI tab → Add to `dashboard_notebook` + populate widgets
- Queue logic → Task tuple structure + `enqueue_opening_tasks()`
- DB export → `export_db_to_jsonl()`, `clear_database()`

### `generation.py` [12]
**Purpose:** Worker loop, LLM calls, conversation generation, output writing
**Key:** `worker()`, `generate_answer_with_retries()`, `write_conversation()`, `call_slop_fixer_llm()`, `call_anti_slop_llm()`
**Edit for:**
- API call flow → `generate_answer_with_retries()` main loop
- Slop fixing → `call_slop_fixer_llm()` (API slot 5) / `call_anti_slop_llm()` (API slot 6)
- Output format → `write_conversation()` ShareGPT structure
- Opening mode → `generate_greeting()`, `generate_first_line()`, `enqueue_opening_tasks()`
- Quality scoring → `quality.score_conversation()` call after conversation complete
- Turn completion check → `conversation_is_complete()` (line 200+)
- Conversation context for fixers → `call_slop_fixer_llm()` and `call_anti_slop_llm()` now accept `conversation_history` parameter to provide full conversation context for coherent rephrasing
- Character prompt injection → character profile block built in `worker()` (personality/traits are conditionally injected lines in the profile; `persona` = inner personality is deliberately EXCLUDED from the profile and re-injected later as an INNER PERSONA BREAKTHROUGH block once the scene reaches the configured slot). Setting resolution → a `settings_for_selected[]` array is computed per task before the profile loop: each selected character keeps its own non-empty `setting`, else samples from the `random_settings` pool (if enabled) or the `A standard indoor environment.` default; when >1 character is selected the ensemble is unified to one shared setting picked from the distinct candidate settings (order-preserving dedupe), which becomes both the injected `Setting:` line for every character and `meta_setting`
- Persona/emotional-state split → `_split_emotional_arc_entry()`, `_expand_emotional_arc()`, `_emotional_state_injection()`, `_persona_block_for_characters()` (all above `worker()`). The worker builds a constant `base_scene_prompt` (system + lore + persona-free profile) then recomposes `current_task_scene_prompt(slot_no)` at the top of each turn: persona surfaces from `persona_breakthrough_turn` onward, and emotional states may be a flat name or an arc (`'neutral -> anxious'`) expanded across the conversation's assistant turns. Opening-mode greeting = slot 1, so conversation turn N lives at slot N+1.
- Output metadata → `_build_record_metadata()` (17 flat keys, same shape on every record, null-filled; `_safe_float()` keeps numeric types stable) + `_score_and_store_quality()` (quality scoring deduped by task_id; called BEFORE both write paths so metadata can carry scores)
- Dataset binding → `bind_task_datasets(queue, task, ...)` (per task, ~line 653) freezes one row per enabled source into an immutable `task_dataset_values` mapping for the life of the task; `validate_task_templates()` pre-checks all authored templates up front. Authored fields (question/answer/continuation prompts, greeting context, first-line, character long fields via `dataset_authored_inputs`) are rendered with `render_task_inputs()` / `render_with_native_placeholders()`; prompt-fillers (`fill_question_prompt()`, continuation, `fill_greeting_prompt()`, `fill_first_line_prompt()`) take a `dataset_values=None` kwarg — `None` skips rendering entirely, so generation without datasets behaves byte-identically to the pre-dataset engine

### `logging_config.py` [13]
**Purpose:** Shared logging utility
**Key:** `log_message()`, `LOG_FILE_PATH`, `DEBUG_LOGGING_ENABLED`
**Edit for:**
- Log path → `LOG_FILE_PATH`
- Enable debug → `DEBUG_LOGGING_ENABLED = True` or UI checkbox

### `quality.py` [14]
**Purpose:** Quality scoring (heuristic + LLM-based)
**Key:** `score_conversation()`, `_score_diversity()`, `_score_technical()`, `_score_with_llm()`, `_heuristic_fallback_for_llm_dim()`
**Edit for:**
- Scoring dimensions → `SCORING_DIMENSIONS` dict (weights)
- Heuristic logic → `_score_diversity()`, `_score_technical()`, `_heuristic_fallback_for_llm_dim()`
- LLM scoring → `_score_with_llm()` (uses API slot from `quality.scoring_api` config)
- Persona-fidelity consistency → `score_conversation()` / `_score_with_llm()` accept `character_profile_text`; the actual character card (the `character_injection` block from `worker()`) is injected into the judge prompt so the `consistency` dimension measures persona/personality/traits fidelity, not just self-consistency. Capped by `quality.max_profile_chars_for_scoring` (default 4000). Without a profile the judge falls back to generic self-consistency language
- Consistency heuristic fallback → `_heuristic_fallback_for_llm_dim('consistency')` no longer returns a flat `70`; it scores down on well-known OOC markers (helpful-bot narration, "as <name>, I would" breaks, fourth-wall drops)
- Flags → Add to `flags` list in scoring functions

### `test_api_profiles.py` [15]
**Purpose:** Unit tests for `api_profiles.py`
**Edit for:** Add new profile tests in `main()`

### `test_opening_mode.py` [16]
**Purpose:** Tests for opening mode functionality
**Edit for:** Add new opening mode tests (greeting, first-line, worker integration)

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

### `test_persona_arc.py` [19]
**Purpose:** Unit tests for the persona/emotional-arc helpers in `generation.py`
**Key:** `main()` covers `_split_emotional_arc_entry`, `_expand_emotional_arc`, `_emotional_state_injection`, `_persona_block_for_characters`
**Edit for:** Add arc-parsing/expansion/injection cases (arrows, stage distribution, header text, single vs multi-char labels)

### `test_random_settings.py` [20]
**Purpose:** Standalone validation of the setting resolution/unification algorithm added to `worker()`
**Key:** `resolve_settings()` mirrors the per-task block; `run()` covers keep-own-setting, pool-sample-on-empty, multi-character unification, selection-off default, pool-disabled default, and ensemble-bounded unification across a seed range
**Edit for:** Add settings-resolution edge cases (blank/whitespace own setting, pool with duplicates, 3+ char ensembles)

### `test_worker_launch.py` [21]
**Purpose:** AST-level regression check that the worker launch in `generate.py` matches the `worker()` signature in `generation.py`
**Key:** Rebuilds the `worker()` signature from its AST, extracts the actual `threading.Thread(target=worker, args=..., kwargs=...)` call from `start_processing()`, and binds them via `inspect.Signature.bind()`; then asserts the OOC kwargs (`current_ooc_phrases`/`current_ooc_fixes`/`current_ooc_enabled`) are supplied by keyword from same-named locals (never positionally) — no app config, GUI, or network code is evaluated
**Run:** `python -B -m unittest test_worker_launch`
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

### `test_dataset_sources.py` [29]
**Purpose:** Unit tests for `dataset_sources.py` (local CSV/TSV/JSON/JSONL readers, schema discovery, fingerprints, error paths)
**Edit for:** Add reader/format cases with temporary fixture files

### `test_dataset_settings.py` [30]
**Purpose:** Unit tests for `dataset_settings.py` normalization/validation (selectors, aliases, visibility, readiness rules)
**Edit for:** Add setting-shape and validation-failure cases

### `test_dataset_bindings.py` [31]
**Purpose:** Unit tests for `dataset_bindings.py` (sequential allocation, repeat-bind idempotency, exhaustion, snapshot round trip, thread safety)
**Edit for:** Add binding-lifecycle cases (use `ThreadPoolExecutor` for concurrency)

### `test_dataset_templates.py` [32]
**Purpose:** Unit tests for `dataset_templates.py` rendering (single-pass, native-placeholder compatibility, alias validation, reserved-namespace typos)
**Edit for:** Add render/template edge cases

### `test_dataset_tasks.py` [33]
**Purpose:** Unit tests for `dataset_tasks.py` (queue attachment lifecycle, lazy reader open/close, same-run reuse, GC behavior of weak references)
**Edit for:** Add task-attachment lifecycle cases

### `test_dataset_backends.py` [34]
**Purpose:** Backend-coverage tests for optional readers (Parquet via `pyarrow`, Hub / saved-`DatasetDict` via `datasets` 4.x) incl. resource lifecycle on rename/delete; never touches the network or a model API
**Edit for:** Extend optional-backend cases; hard-dep backends (csv/json/jsonl) always run, optional ones skip when the package is missing

### `test_dataset_column_menu.py` [35]
**Purpose:** Unit tests for `dataset_column_menu.py` (selection semantics, large-column boundedness, grab/focus handling); skips cleanly without a display
**Edit for:** Add picker behavior cases (run on a display for zero skips)

### `test_datasets_panel.py` [36]
**Purpose:** Unit tests for `datasets_panel.py` (add/edit/import/alias/save-load flows, background metadata workers); synthetic column names only, optional screenshots via `READYART_GUI_SCREENSHOTS`
**Edit for:** Add panel-flow cases

### `test_dataset_pipeline.py` [37]
**Purpose:** Pipeline-level simulation of dataset integration into worker-shaped request stages (question/answer/continuation/cleanup); all model replies are fixtures, expected request bodies assembled independently of the renderer
**Edit for:** Add stage-level pipeline scenarios (scheduler/transport/retry/cache are small simulations, not production impls)

### `test_dataset_compatibility.py` [38]
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
| Add quality dimension | `quality.py` (`SCORING_DIMENSIONS`, scoring logic), `dashboard.py` (display) |
| Modify output format | `generation.py` (`write_conversation()`) |
| Add slop/anti-slop phrase | `config/config.yml` (`detection.*.phrases`) → `detection.py` phrase lists |
| Fix quote balancing | `text_utils.py` (`normalize_quotes()`, `repair_straight_quotes()`) |
| Add UI metric | `app_state.py` (widget ref), `generate.py` (create widget), `dashboard.py` (update) |
| Change rate limit | `config/config.yml` (`api.apis[].rate_limit_rpm`), `config_editor.py` (UI field), `api_handler.py` (`set_rate_limit()`) |
| Modify retry logic | `generation.py` (`generate_answer_with_retries()`, `MAX_TASK_REQUEUES`) |
| Add API profile | `config/api_profiles.yml` (new profile definition), no code change needed |
| Modify API profile params | `config/api_profiles.yml` (`allow`, `rename` lists), `api_profiles.py` (if transform logic changes) |
| Add character profile | `config/config.yml` (`prompts.character.characters[]`), `config_editor.py` (UI fields) |
| Add character field | `config/config.yml` (default value per character), `config_editor.py` (`_add_character_row()` long-field loop + `_get_current_editor_config_data()` gather + keep-list + `_sync_character_text_values()` + legacy fallback in `load` path), `generate.py` (legacy fallback), `generation.py` (prompt injection in profile block) |
| Modify opening mode prompts | `config/config.yml` (`prompts.greeting`, `prompts.first_line`, `prompts.greeting_context`, `prompts.first_line_styles`) |
| Add random names feature | `config/config.yml` (`prompts.random_names.enabled`, `prompts.random_names.names[]`), `config_editor.py` (UI toggle + text area), `generate.py` (load config + pass to worker), `generation.py` (use random names in character profile) |
| Add random settings pool | `config/config.yml` (`prompts.random_settings.enabled`, `prompts.random_settings.settings[]`), `config_editor.py` (UI toggle + text area + save/load/toggle/validation), `generate.py` (load config + pass to worker), `generation.py` (sample empty character settings + unify multi-character ensembles; see setting resolution above) |
| Add/modify persona breakthrough or emotional arc | `config/config.yml` (`prompts.character.persona_breakthrough_turn`, `prompts.emotional_states.states[]` — a state may be `'->'`-joined stages), `config_editor.py` (spinbox under Character Engine + states label), `generate.py` (load + pass to worker kwargs), `generation.py` (`_split_emotional_arc_entry`/`_expand_emotional_arc`/`_emotional_state_injection`/`_persona_block_for_characters` + per-turn `current_task_scene_prompt()` in `worker()`) |
| Toggle conversation history for fixers | `config/config.yml` (`generation.use_conversation_history_for_fixers`), `config_editor.py` (UI checkbox), `app_state.py` (flag), `generation.py` (conditional pass to fixers) |
| Add/modify output metadata field | `generation.py` (`_build_record_metadata()` schema + `_safe_float()`), keep same-keys/same-types/null-filled invariant; config flag `generation.include_metadata` |
| Add dataset source type | `dataset_sources.py` (new `DatasetSource` subclass + register in `open_source()`), `dataset_settings.py` (`_SELECTOR_DEFAULTS`, `source_selector`), `datasets_panel.py` (`_KINDS` list) |
| Add dataset column→alias mapping | `config_editor.py` (Datasets tab) → `datasets_panel.py` (Apply/Copy placeholder) → `dataset_settings.py` (validation) — placeholders `{{ds:alias}}` are then usable in any authored prompt field |
| Modify dataset placeholder rendering | `dataset_templates.py` (`render_template()` / `render_with_native_placeholders()` / `validate_alias()`); worker-side call sites in `generation.py` (`render_task_inputs()`) |
| Change per-task row allocation | `dataset_bindings.py` (`DatasetBindingManager.bind()`), `dataset_tasks.py` (`bind_task_datasets()`) — keep exhaustion/invalid-data rules: no silent cycling, skipping, or partial publish |
| Run dataset tests | `python -m unittest test_dataset_sources test_dataset_settings test_dataset_bindings test_dataset_templates test_dataset_tasks test_dataset_backends test_dataset_column_menu test_datasets_panel test_dataset_pipeline test_dataset_compatibility` (live Hub check: `tools/check_dataset_hub.py`, opt-in only) |
| Run GUI layout tests | `tools/check_gui_layout.py --check --config-editor --screenshots` |

## Key State Variables (`app_state.py`)

**Counters:** `total_attempts_global`, `refusal_count_total`, `slop_count_total`, `error_count_total`, `anti_slop_count_total`, `ooc_count_total`, `total_input_tokens`, `total_output_tokens`

**Per-API:** `*_per_api` dicts for indices 0-5

**Recent issues:** `recent_*_total`, `recent_*_per_api` (MAX_RECENT = 10); includes `recent_ooc_total` / `recent_ooc_per_api`

**Issue timestamp keys:** `refusals`, `user_speaking`, `slop`, `errors`, `anti_slop`, `incomplete_quotes`, `out_of_character`

**Locks:** `stats_lock`, `issue_timestamps_lock`, `quality_lock`, `task_queue_ui_lock`, `api_circuit_breaker_lock`

**GUI handles:** `root`, `dashboard_notebook`, `slot_widgets`, `rate_limit_labels`, `quality_*_label`

**Flags:** `stop_processing`, `pause_processing`, `processing_active`, `quality_enabled`, `quality_output_filter`, `use_conversation_history_for_fixers`

## API Slots Index
- **0-3:** Main generation APIs (duplication mode)
- **4:** Slop Fixer LLM
- **5:** Anti-Slop Fixer LLM (independent)
- **6:** Quality scoring LLM (if configured separately)

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
