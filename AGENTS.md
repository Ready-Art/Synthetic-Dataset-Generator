# AGENTS.md - Codebase Cheat Sheet

## Setup and Tests

The application targets Python 3.8 (`README.md`, Requirements), so code and tests must not use anything newer, such
as `list[str]` annotations, `match` or `str.removesuffix()`. The optional dataset backends need Python 3.9 or later.
Tests that import `generation.py` (`test_opening_mode.py`, `test_persona_arc.py`, `test_dataset_compatibility.py`)
or the Datasets panel need Tk; `test_datasets_panel.py` and `test_dataset_column_menu.py` also need a display.

```sh
python -m pip install -r requirements.txt            # the application
python -m pip install -r requirements-datasets.txt   # the application + Hugging Face, Parquet and saved-HF sources
```

Run tests from the repository root. Every test file runs on its own and exits non-zero on a failure; none needs a
model, a network connection or a running server. While working, run the test for the module you change, where the
File Reference below names one. Before finishing, run all of them:

```sh
for t in test_*.py; do python -B "$t" || echo "FAILED: $t"; done
```

- `test_dataset_backends.py` skips its checks unless `requirements-datasets.txt` is installed. A skip does not count
  as a pass.
- `test_dataset_compatibility.py` compares the requests the worker sends with those of the engine at its
  `BASELINE_COMMIT`, which it extracts with `git archive`, so it needs `git` and a clone that holds that commit (not
  a shallow clone or a zip download). A change that alters the requests on purpose (a prompt, a message's order or
  role) makes it fail by design. Move `BASELINE_COMMIT` to a commit with the same change.
  `DATASET_COMPATIBILITY_BASELINE_DIR=<dir>` runs it against an extracted tree instead.
- Importing `logging_config` creates `output/` in the working directory, and `logging_config.LOG_FILE_PATH` is
  relative to it, so `test_api_profiles.py` appends to the repository's `output/log.txt`. `test_opening_mode.py` and
  `test_persona_arc.py` change into a temporary directory first.
- `tools/check_gui_layout.py` and `tools/check_dataset_hub.py` are not part of the test run (see Tools).

## Boundaries

- **The repository has no `.gitignore`.** `output/` (generated rows, `log.txt`, run state), `__pycache__/` and
  `layout-check/` (once the layout tool has run) are untracked local output. Stage files by name; never `git add -A`
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
- Sampler params → `samplers.temperature`, `samplers.top_p`, `samplers.repetition_penalty`
- Character profiles → `prompts.character.characters[]` (name, race, job, hair, hair_color, clothing, appearance, persona, personality, traits, backstory, setting) — `persona` = inner personality (private self), `personality` = outward behavior (observed manner); `persona` is withheld from the profile and re-injected as a breakthrough block from `prompts.character.persona_breakthrough_turn` onward
- Emotional states/arcs → `prompts.emotional_states.states[]` — each entry is a flat state or a `'->'`-joined progression (`'neutral -> tense -> angry'`) expanded across the conversation's assistant turns
- Random names → `prompts.random_names.enabled`, `prompts.random_names.names[]` (optional list of names to randomly assign to characters, overrides character Name field when enabled)
- Random settings pool → `prompts.random_settings.enabled`, `prompts.random_settings.settings[]` (analogous to `random_names`). When `setting_enabled` is true and a selected character's own `setting` is empty, a setting is sampled from this pool (when enabled) instead of the hardcoded default. For a multi-character ensemble the per-character settings are unified to ONE shared setting picked from the ensemble, so the whole scene happens in a single location
- Conversation history for fixers → `generation.use_conversation_history_for_fixers` (whether to pass conversation context to slop/anti-slop fixer LLMs)
- Output metadata → `generation.include_metadata` (flat, schema-stable provenance block per output.jsonl record; trainer-safe; never modifies `conversations[]`; false = legacy two-field records)
- Dataset sources → `datasets.sources[]` (source selectors, column names and aliases; edited in the Config Editor's Datasets tab, normalized by `dataset_settings.py`)

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
**Key:** `ConfigEditor` class, `save_config_handler()`, `load_config_handler()`
**Edit for:**
- Add new config field → Add UI widget + bind to `save_config_handler()` + `load_config_handler()`
- New tab → Add `ttk.Frame` + `notebook.add()` in `__init__()`
- Validation → `validate_config_handler()`

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
- Worker launch → `threading.Thread(target=worker, args=(...), kwargs={...})` in `start_processing()`. **New worker options MUST be added to the `kwargs=` dict, not `args=`** — the positional list length is fixed and the OOC/opening-mode params (`current_ooc_*`, `opening_mode_local`, etc.) are kwarg-sourced; adding a positional arg silently shifts every later slot and `test_worker_launch.py` fails on the mismatch
- New UI tab → Add to `dashboard_notebook` + populate widgets
- Queue logic → Task tuple structure + `enqueue_opening_tasks()`
- DB export → `export_db_to_jsonl()`, `clear_database()`

### `generation.py` [12]
**Purpose:** Worker loop, LLM calls, conversation generation, output writing
**Key:** `worker()`, `generate_answer_with_retries()`, `write_conversation()`, `call_slop_fixer_llm()`, `call_anti_slop_llm()`
**Edit for:**
- API call flow → `generate_answer_with_retries()` main loop
- Slop fixing → `call_slop_fixer_llm()` (API slot 5, index 4) / `call_anti_slop_llm()` (API slot 6, index 5)
- Output format → `write_conversation()` ShareGPT structure
- Dataset values → `bind_task_datasets()` near the top of the `worker()` task loop freezes one row per source for the task, across turns, fixes and retries. `render_task_inputs()` expands `{{ds:alias}}` in the static authored inputs; the question, continuation, greeting and first-line templates are expanded where they are formatted, with `render_with_native_placeholders()`. `docs/dataset-bindings.md` (Placeholder syntax) lists the fields
- Opening mode → `generate_greeting()`, `generate_first_line()`, `enqueue_opening_tasks()`
- Quality scoring → `quality.score_conversation()` call after conversation complete
- Turn completion check → `conversation_is_complete()`
- Conversation context for fixers → `call_slop_fixer_llm()` and `call_anti_slop_llm()` now accept `conversation_history` parameter to provide full conversation context for coherent rephrasing
- Character prompt injection → character profile block built in `worker()` (personality/traits are conditionally injected lines in the profile; `persona` = inner personality is deliberately EXCLUDED from the profile and re-injected later as an INNER PERSONA BREAKTHROUGH block once the scene reaches the configured slot). Setting resolution → a `settings_for_selected[]` array is computed per task before the profile loop: each selected character keeps its own non-empty `setting`, else samples from the `random_settings` pool (if enabled) or the `A standard indoor environment.` default; when >1 character is selected the ensemble is unified to one shared setting picked from the distinct candidate settings (order-preserving dedupe), which becomes both the injected `Setting:` line for every character and `meta_setting`
- Persona/emotional-state split → `_split_emotional_arc_entry()`, `_expand_emotional_arc()`, `_emotional_state_injection()`, `_persona_block_for_characters()` (all above `worker()`). The worker builds `base_scene_prompt` (system + lore + persona-free profile) once per task. It is the system message of every greeting, answer and user-continuation request in the task (a retry still appends its jailbreak or fix instruction to it); the generated question gets it plus `QUESTION_USER_SIDE`, and the first-line and fixer requests have their own system prompts. The per-reply note comes from `current_turn_note(slot_no)`: the INNER PERSONA BREAKTHROUGH block from `persona_breakthrough_turn` onward, then the slot's emotional state, which is a flat name or an arc stage (`'neutral -> anxious'` expanded across the conversation's assistant turns). The note is appended to the slot's final user message, the answer prompt or the opening-mode greeting prompt, after the history and never stored in it. The question and user-continuation requests do not get it. So from one answer request to the next the system prompt and the history are the same text, which lets a server's prefix cache reuse them; keep it that way. Opening-mode greeting = slot 1, so conversation turn N lives at slot N+1.
- Generated first user message → when characters are profiled, its system prompt is the scene prompt plus `QUESTION_USER_SIDE`, which tells the model it writes only the user's side and that the user is not one of the characters
- Output metadata → `_build_record_metadata()` (17 flat keys, same shape on every record, null-filled; `_safe_float()` keeps numeric types stable) + `_score_and_store_quality()` (quality scoring deduped by task_id; called BEFORE both write paths so metadata can carry scores)

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

### Dataset modules [22]
**Purpose:** Optional dataset rows as prompt variables (`{{ds:alias}}`). Setup, placeholder rules, limits and verification are in `docs/dataset-bindings.md`; the README's "Dataset Sources and Prompt Variables" is the summary
**Flow:** Config Editor Datasets tab (`datasets_panel.py`) → `datasets.sources[]` (`dataset_settings.py`) → `configure_queue_datasets()` on the task queue in `generate.py` `start_processing()` → `bind_task_datasets()` per task in `worker()` (`dataset_tasks.py`, `dataset_bindings.py`) → readers in `dataset_sources.py` → `render_task_inputs()` / `render_with_native_placeholders()` (`dataset_templates.py`)
- `dataset_settings.py` — normalizes the `datasets` config subtree; names only, no GUI, file or network I/O
- `datasets_panel.py` / `dataset_column_menu.py` — the Datasets tab and its searchable column picker; reads schemas only, at most two daemon threads, Tk touched only on the UI thread
- `dataset_sources.py` — readers: CSV/TSV/JSON/JSONL with the standard library; Parquet, saved-HF (`save_to_disk`) and Hub sources need `requirements-datasets.txt`
- `dataset_bindings.py` — `DatasetBindingManager`, one per run: each source advances one row per newly bound task, and a task keeps its `TaskBinding` for every stage and retry
- `dataset_tasks.py` — attaches bindings to the queue's existing task identities; in memory only, so a restart or a new queue does not keep row assignments
- `dataset_templates.py` — `{{ds:alias}}` parsing and rendering; `render_with_native_placeholders()` keeps the native `str.replace` order of the `{context}`/`{subject}` variables
**Keep:** `dataset_settings.py`, `dataset_sources.py`, `dataset_bindings.py` and `dataset_templates.py` import no GUI, worker or `app_state` code
**Tests:** `test_dataset_<module>.py` for each module, `test_datasets_panel.py` (real Tk, with the Config Editor), `test_dataset_pipeline.py` (orchestration), `test_dataset_backends.py` (needs `requirements-datasets.txt`), `test_dataset_compatibility.py` (requests compared with `BASELINE_COMMIT`; see Setup and Tests)
**Edit for:**
- New source format → reader in `dataset_sources.py` + `test_dataset_sources.py` (and `test_dataset_backends.py` for an optional package)
- New field that accepts placeholders → the worker's `dataset_authored_inputs` in `generation.py` + `test_dataset_compatibility.py` scenarios + the field list in `docs/dataset-bindings.md`

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
| Add/modify persona breakthrough or emotional arc | `config/config.yml` (`prompts.character.persona_breakthrough_turn`, `prompts.emotional_states.states[]` — a state may be `'->'`-joined stages), `config_editor.py` (spinbox under Character Engine + states label), `generate.py` (load + pass to worker kwargs), `generation.py` (`_split_emotional_arc_entry`/`_expand_emotional_arc`/`_emotional_state_injection`/`_persona_block_for_characters` + `current_turn_note()` in `worker()`), `test_opening_mode.py` and `test_persona_arc.py` |
| Toggle conversation history for fixers | `config/config.yml` (`generation.use_conversation_history_for_fixers`), `config_editor.py` (UI checkbox), `app_state.py` (flag), `generation.py` (conditional pass to fixers) |
| Add/modify output metadata field | `generation.py` (`_build_record_metadata()` schema + `_safe_float()`), keep same-keys/same-types/null-filled invariant; config flag `generation.include_metadata` |
| Run GUI layout tests | `tools/check_gui_layout.py --check --config-editor --screenshots` |
| Add a dataset source format | `dataset_sources.py` (reader), `test_dataset_sources.py`, `test_dataset_backends.py` (optional package), `requirements-datasets.txt` (if a new package) |
| Change what a request contains (prompt text, message order or role) | `generation.py`, `test_opening_mode.py`, then move `BASELINE_COMMIT` in `test_dataset_compatibility.py` to a commit with the same change |

## Key State Variables (`app_state.py`)

**Counters:** `total_attempts_global`, `refusal_count_total`, `slop_count_total`, `error_count_total`, `anti_slop_count_total`, `ooc_count_total`, `total_input_tokens`, `total_output_tokens`

**Per-API:** `*_per_api` dicts for indices 0-5

**Recent issues:** `recent_*_total`, `recent_*_per_api` (MAX_RECENT = 10); includes `recent_ooc_total` / `recent_ooc_per_api`

**Issue timestamp keys:** `refusals`, `user_speaking`, `slop`, `errors`, `anti_slop`, `incomplete_quotes`, `out_of_character`

**Locks:** `stats_lock`, `issue_timestamps_lock`, `quality_lock`, `task_queue_ui_lock`, `api_circuit_breaker_lock`

**GUI handles:** `root`, `dashboard_notebook`, `slot_widgets`, `rate_limit_labels`, `quality_*_label`

**Flags:** `stop_processing`, `pause_processing`, `processing_active`, `quality_enabled`, `quality_output_filter`, `use_conversation_history_for_fixers`

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

### `tools/check_dataset_hub.py`
**Purpose:** Opt-in live check of a Hugging Face Hub dataset reader; prints the schema and provenance, never cell values. Needs network access and `requirements-datasets.txt`; not part of the test run
**Usage:** `python -B tools/check_dataset_hub.py --repository owner/dataset --config default --split train --column text --rows 2`
