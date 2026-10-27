# Plan.md - Codebase Cheat Sheet

## Core Architecture
```
generate.py (UI entry) → generation.py (worker logic) → api_handler.py (API calls)
                    ↓                              ↓
              app_state.py (shared state) ← detection.py (issue checks)
                    ↓
              dashboard.py (display updates)
```

## File Reference

### `api_handler.py` [1]
**Purpose:** API calls, rate limiting, Valkey caching
**Key:** `RateLimiter`, `global_rate_limiter`, `get/set_cached_response`
**Edit for:**
- Rate limit logic → `wait_if_needed()` (line 33-56)
- Cache TTL → `set_cached_response()` ttl param (line 82)
- Response time tracking → `api_response_times_per_slot` (line 13)

### `api_profiles.py` [2]
**Purpose:** Filter request payloads per API endpoint compatibility
**Key:** `apply_profile_for_slot()`, `detect_profile()`, `set_slot_profile()`
**Edit for:**
- Add new API profile → `config/api_profiles.yml` (not code)
- Param rename logic → `_RENAME_TRANSFORMS` (line 34)
- Always-allowed keys → `_ALWAYS_ALLOWED` (line 23)

### `app_state.py` [3]
**Purpose:** Shared runtime state, counters, locks, GUI handles
**Key:** All globals (counters, locks, widget refs)
**Edit for:**
- Add new counter → Add to module level + init in `reset_all_stats_and_history()`
- Add new lock → `threading.Lock()` at module level
- GUI widget refs → Assign in `generate.py` after widget creation

### `config_editor.py` [4]
**Purpose:** Configuration Editor UI window
**Key:** `ConfigEditor` class, `save_config_handler()`, `load_config_handler()`
**Edit for:**
- Add new config field → Add UI widget + bind to `save_config_handler()` + `load_config_handler()`
- New tab → Add `ttk.Frame` + `notebook.add()` in `__init__()`
- Validation → `validate_config_handler()`

### `config_loader.py` [5]
**Purpose:** Load/save config.yml and profiles
**Key:** `ConfigLoader` class, `get()`, `set()`, `save_profile()`
**Edit for:**
- Config path → `__init__(path=...)`
- Profile operations → `save_profile()`, `load_profile_to_main_config()`, `delete_profile()`

### `dashboard.py` [6]
**Purpose:** Dashboard display, issue panels, progress bars, status labels
**Key:** `update_dashboard()`, `update_issue_graph()`, `create_modern_issue_panel()`
**Edit for:**
- New metric label → Add to `app_state.py` + update in `update_dashboard()`
- Issue panel → `create_modern_issue_panel()` + `update_modern_issue_panel()`
- Progress bar style → `configure_animated_progress_styles()`, `get_progress_style_name()`

### `detection.py` [7]
**Purpose:** Issue detection (refusals, user speaking, slop, anti-slop)
**Key:** `is_refusal()`, `is_user_speaking()`, `is_slop()`, `is_anti_slop()`, `is_incomplete_quote()`
**Edit for:**
- Add detection type → New `is_*()` function + add to `app_state.issue_timestamps`
- Phrase lists → Passed from config via `generate.py` → `worker()`
- Timestamp tracking → `issue_timestamps[key].append(time.time())` with 1hr cutoff

### `generate.py` [8]
**Purpose:** Main UI, Tkinter app, worker thread management, queue setup
**Key:** `start_processing()`, `worker()` launch, UI widgets, `ConfigEditor`
**Edit for:**
- Worker kwargs → `start_processing()` thread launch (~line 800+)
- New UI tab → Add to `dashboard_notebook` + populate widgets
- Queue logic → Task tuple structure + `enqueue_opening_tasks()`
- DB export → `export_db_to_jsonl()`, `clear_database()`

### `generation.py` [9]
**Purpose:** Worker loop, LLM calls, conversation generation, output writing
**Key:** `worker()`, `generate_answer_with_retries()`, `write_conversation()`, `call_slop_fixer_llm()`, `call_anti_slop_llm()`
**Edit for:**
- API call flow → `generate_answer_with_retries()` main loop
- Slop fixing → `call_slop_fixer_llm()` (API slot 5) / `call_anti_slop_llm()` (API slot 6)
- Output format → `write_conversation()` ShareGPT structure
- Opening mode → `generate_greeting()`, `generate_first_line()`, `enqueue_opening_tasks()`
- Quality scoring → `quality.score_conversation()` call after conversation complete
- Turn completion check → `conversation_is_complete()` (line 200+)

### `logging_config.py` [10]
**Purpose:** Shared logging utility
**Key:** `log_message()`, `LOG_FILE_PATH`, `DEBUG_LOGGING_ENABLED`
**Edit for:**
- Log path → `LOG_FILE_PATH`
- Enable debug → `DEBUG_LOGGING_ENABLED = True` or UI checkbox

### `quality.py` [11]
**Purpose:** Quality scoring (heuristic + LLM-based)
**Key:** `score_conversation()`, `_score_diversity()`, `_score_technical()`, `_score_with_llm()`
**Edit for:**
- Scoring dimensions → `SCORING_DIMENSIONS` dict (weights)
- Heuristic logic → `_score_diversity()`, `_score_technical()`
- LLM scoring → `_score_with_llm()` (uses API slot from `quality.scoring_api` config)
- Flags → Add to `flags` list in scoring functions

### `test_api_profiles.py` [12]
**Purpose:** Unit tests for `api_profiles.py`
**Edit for:** Add new profile tests in `main()`

### `test_opening_mode.py` [13]
**Purpose:** Tests for opening mode functionality
**Edit for:** Add new opening mode tests (greeting, first-line, worker integration)

### `text_utils.py` [14]
**Purpose:** Text processing utilities
**Key:** `normalize_quotes()`, `repair_straight_quotes()`, `remove_markdown()`, `remove_reasoning_text()`
**Edit for:**
- Add text cleanup → New function + call in `write_conversation()` processing loop
- Quote handling → `normalize_quotes()` (curly vs straight balance)

### `ui_layout.py` [15]
**Purpose:** Tkinter layout helpers
**Key:** `ScrollableFrame`, `ResponsiveGrid`, `NotebookNavigator`, `wrap_label()`
**Edit for:**
- Custom layout → Use `ResponsiveGrid` for reflowing widgets
- Scrollable area → Wrap in `ScrollableFrame`

## Common Tasks Quick Reference

| Task | Files to Edit |
|------|---------------|
| Add new API slot | `config_editor.py` (UI), `app_state.py` (counters), `generation.py` (worker logic) |
| Add detection type | `detection.py` (function), `app_state.py` (timestamps/counters), `dashboard.py` (display) |
| Add config option | `config_editor.py` (UI + save/load), `config_loader.py` (if new structure), `generation.py` (pass to worker) |
| Add quality dimension | `quality.py` (`SCORING_DIMENSIONS`, scoring logic), `dashboard.py` (display) |
| Modify output format | `generation.py` (`write_conversation()`) |
| Add slop/anti-slop phrase | Config UI → `detection.py` phrase lists |
| Fix quote balancing | `text_utils.py` (`normalize_quotes()`, `repair_straight_quotes()`) |
| Add UI metric | `app_state.py` (widget ref), `generate.py` (create widget), `dashboard.py` (update) |
| Change rate limit | `config_editor.py` (UI field), `api_handler.py` (`set_rate_limit()`) |
| Modify retry logic | `generation.py` (`generate_answer_with_retries()`, `MAX_TASK_REQUEUES`) |

## Key State Variables (`app_state.py`)

**Counters:** `total_attempts_global`, `refusal_count_total`, `slop_count_total`, `error_count_total`, `anti_slop_count_total`, `total_input_tokens`, `total_output_tokens`

**Per-API:** `*_per_api` dicts for indices 0-5

**Recent issues:** `recent_*_total`, `recent_*_per_api` (MAX_RECENT = 10)

**Locks:** `stats_lock`, `issue_timestamps_lock`, `quality_lock`, `task_queue_ui_lock`, `api_circuit_breaker_lock`

**GUI handles:** `root`, `dashboard_notebook`, `slot_widgets`, `rate_limit_labels`, `quality_*_label`

**Flags:** `stop_processing`, `pause_processing`, `processing_active`, `quality_enabled`, `quality_output_filter`

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
- Profiles: `config/profiles/*.yml`
- Environment: API keys via `os.getenv()` or inline in config
