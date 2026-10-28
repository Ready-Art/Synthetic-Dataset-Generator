# codebase.md - Codebase Cheat Sheet

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
- Detection phrases → `detection.refusal.phrases`, `detection.slop.phrases`, `detection.user_speaking.*.phrases`
- Sampler params → `samplers.temperature`, `samplers.top_p`, `samplers.repetition_penalty`
- Character profiles → `prompts.character.characters[]` (name, race, job, hair, hair_color, clothing, appearance, persona, personality, traits, backstory, setting) — `persona` = inner personality (private self), `personality` = outward behavior (observed manner); `persona` is withheld from the profile and re-injected as a breakthrough block from `prompts.character.persona_breakthrough_turn` onward
- Emotional states/arcs → `prompts.emotional_states.states[]` — each entry is a flat state or a `'->'`-joined progression (`'neutral -> tense -> angry'`) expanded across the conversation's assistant turns
- Random names → `prompts.random_names.enabled`, `prompts.random_names.names[]` (optional list of names to randomly assign to characters, overrides character Name field when enabled)
- Conversation history for fixers → `generation.use_conversation_history_for_fixers` (whether to pass conversation context to slop/anti-slop fixer LLMs)
- Output metadata → `generation.include_metadata` (flat, schema-stable provenance block per output.jsonl record; trainer-safe; never modifies `conversations[]`; false = legacy two-field records)

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
- Issue panel → `create_modern_issue_panel()` + `update_modern_issue_panel()`
- Progress bar style → `configure_animated_progress_styles()`, `get_progress_style_name()`

### `detection.py` [10]
**Purpose:** Issue detection (refusals, user speaking, slop, anti-slop)
**Key:** `is_refusal()`, `is_user_speaking()`, `is_slop()`, `is_anti_slop()`, `is_incomplete_quote()`
**Edit for:**
- Add detection type → New `is_*()` function + add to `app_state.issue_timestamps`
- Phrase lists → Passed from config via `generate.py` → `worker()`
- Timestamp tracking → `issue_timestamps[key].append(time.time())` with 1hr cutoff

### `generate.py` [11]
**Purpose:** Main UI, Tkinter app, worker thread management, queue setup
**Key:** `start_processing()`, `worker()` launch, UI widgets, `ConfigEditor`
**Edit for:**
- Worker kwargs → `start_processing()` thread launch (~line 800+)
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
- Character prompt injection → character profile block built in `worker()` (personality/traits are conditionally injected lines in the profile; `persona` = inner personality is deliberately EXCLUDED from the profile and re-injected later as an INNER PERSONA BREAKTHROUGH block once the scene reaches the configured slot)
- Persona/emotional-state split → `_split_emotional_arc_entry()`, `_expand_emotional_arc()`, `_emotional_state_injection()`, `_persona_block_for_characters()` (all above `worker()`). The worker builds a constant `base_scene_prompt` (system + lore + persona-free profile) then recomposes `current_task_scene_prompt(slot_no)` at the top of each turn: persona surfaces from `persona_breakthrough_turn` onward, and emotional states may be a flat name or an arc (`'neutral -> anxious'`) expanded across the conversation's assistant turns. Opening-mode greeting = slot 1, so conversation turn N lives at slot N+1.
- Output metadata → `_build_record_metadata()` (17 flat keys, same shape on every record, null-filled; `_safe_float()` keeps numeric types stable) + `_score_and_store_quality()` (quality scoring deduped by task_id; called BEFORE both write paths so metadata can carry scores)

### `logging_config.py` [13]
**Purpose:** Shared logging utility
**Key:** `log_message()`, `LOG_FILE_PATH`, `DEBUG_LOGGING_ENABLED`
**Edit for:**
- Log path → `LOG_FILE_PATH`
- Enable debug → `DEBUG_LOGGING_ENABLED = True` or UI checkbox

### `quality.py` [14]
**Purpose:** Quality scoring (heuristic + LLM-based)
**Key:** `score_conversation()`, `_score_diversity()`, `_score_technical()`, `_score_with_llm()`
**Edit for:**
- Scoring dimensions → `SCORING_DIMENSIONS` dict (weights)
- Heuristic logic → `_score_diversity()`, `_score_technical()`
- LLM scoring → `_score_with_llm()` (uses API slot from `quality.scoring_api` config)
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

## Common Tasks Quick Reference

| Task | Files to Edit |
|------|---------------|
| Add new API slot | `config/config.yml` (`api.apis[]`), `config_editor.py` (UI), `app_state.py` (counters), `generation.py` (worker logic) |
| Add detection type | `detection.py` (function), `app_state.py` (timestamps/counters), `dashboard.py` (display) |
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
| Add/modify persona breakthrough or emotional arc | `config/config.yml` (`prompts.character.persona_breakthrough_turn`, `prompts.emotional_states.states[]` — a state may be `'->'`-joined stages), `config_editor.py` (spinbox under Character Engine + states label), `generate.py` (load + pass to worker kwargs), `generation.py` (`_split_emotional_arc_entry`/`_expand_emotional_arc`/`_emotional_state_injection`/`_persona_block_for_characters` + per-turn `current_task_scene_prompt()` in `worker()`) |
| Toggle conversation history for fixers | `config/config.yml` (`generation.use_conversation_history_for_fixers`), `config_editor.py` (UI checkbox), `app_state.py` (flag), `generation.py` (conditional pass to fixers) |
| Add/modify output metadata field | `generation.py` (`_build_record_metadata()` schema + `_safe_float()`), keep same-keys/same-types/null-filled invariant; config flag `generation.include_metadata` |
| Run GUI layout tests | `tools/check_gui_layout.py --check --config-editor --screenshots` |

## Key State Variables (`app_state.py`)

**Counters:** `total_attempts_global`, `refusal_count_total`, `slop_count_total`, `error_count_total`, `anti_slop_count_total`, `total_input_tokens`, `total_output_tokens`

**Per-API:** `*_per_api` dicts for indices 0-5

**Recent issues:** `recent_*_total`, `recent_*_per_api` (MAX_RECENT = 10)

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
