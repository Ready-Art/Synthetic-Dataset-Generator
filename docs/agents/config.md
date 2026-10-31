<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Configuration files

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are changing `config/config.yml`, `config/api_profiles.yml` or the dependency pins.

**Related:** [`docs/agents/config-editor.md`](config-editor.md), [`docs/agents/task-index.md`](task-index.md)

## Contents

- `config/config.yml`
- `config/api_profiles.yml`
- `requirements-datasets.txt`
- Tasks that touch these files

---

### `config/config.yml` [1]
**Purpose:** Main configuration file for API settings, generation parameters, prompts, detection, and samplers
**Key:** `api.apis[]`, `generation.*`, `prompts.*`, `detection.*`, `samplers.*`
**Edit for:**
- API endpoints → `api.apis[].url`, `api.apis[].model`, `api.apis[].key`
- Rate limits → `api.apis[].rate_limit_rpm`, `api.apis[].threads`
- Generation settings → `generation.max_character_cards`, `generation.num_turns`, `generation.output_format`
- Prompts → `prompts.system.base`, `prompts.question`, `prompts.answer`, `prompts.greeting`, `prompts.first_line`
- Questions-file dataset variables (v11.9.0) → `prompts.questions_dataset.append_alias` (ONE bare alias whose value is appended to every `input/questions.txt` line; `''` = placeholders only, append nothing) and `append_separator` (text between the line and the appended value, real characters, default `"\n\n"`; the editor stores it escaped because a Tk Entry cannot hold a newline). A questions-file line is authored text, so it may also contain `{{ds:alias}}` placeholders. Chunk-mode source `.txt` is deliberately NOT rendered: a chunk is cut at a random character offset and could split a placeholder in half
- Opening mode → `prompts.opening_mode`, `prompts.greeting_context`, `prompts.first_line_styles`
- Detection phrases → `detection.refusal.phrases`, `detection.slop.phrases`, `detection.user_speaking.*.phrases`, `detection.out_of_character.phrases` (+ `enabled`, `fixes`)
- Non-Latin script check (v11.10.0) → `detection.non_latin.enabled` (off by default) and `detection.non_latin.fixes[]` (one instruction per retry round; empty falls back to `generation.DEFAULT_NON_LATIN_FIX`, so switching the check on needs no fix list). A Unicode range allow-list in `detection.is_non_latin()`: no pattern to compile, no phrase list to sanitize, no LLM call
- Regex Watch (v11.6) → `detection.regex_watch.enabled` (off by default), `detection.regex_watch.max_rounds` (1-5, default 3), `detection.regex_watch.patterns[]` (regular expressions, user-editable; shipped defaults are mirrored by `detection.DEFAULT_REGEX_WATCH_PATTERNS`). A hit is rewritten by the SAME main API slot, not a fixer slot; there is no fixes list
- Sampler params → `samplers.temperature`, `samplers.top_p`, `samplers.repetition_penalty`
- Bad words (v11.1) → `samplers.use_bad_words` (bool, off by default), `samplers.bad_words` (list of strings; leading whitespace on every entry is preserved exactly as typed, matching vLLM semantics). The single toggle drives every main-API request (question/answer/user-continuation/opening-mode greeting+first-line) plus the Slop Fixer (slot 5) and Anti-Slop Fixer (slot 6), even when those fixers carry their own `slop_fixer_params` / `anti_slop_params` sampler overrides — `generation.apply_bad_words_to_payload()` reads from the TOP-LEVEL `samplers.*` keys so it works regardless of per-slot override state
- Character profiles → `prompts.character.characters[]` (name, race, job, hair, hair_color, clothing, appearance, persona, personality, traits, backstory, setting) — `persona` = inner personality (private self), `personality` = outward behavior (observed manner); `persona` is withheld from the profile and re-injected as a breakthrough block from `prompts.character.persona_breakthrough_turn` onward
- Emotional states/arcs → `prompts.emotional_states.states[]` — each entry is a flat state or a `'->'`-joined progression (`'neutral -> tense -> angry'`) expanded across the conversation's assistant turns
- User-side emotional states/arcs (v10.6.0, optional) → `prompts.user_emotional_states.{enabled,states[]}` — same flat/'->' arc format as `emotional_states` but applied to the SIMULATED USER's messages (generated opening question/first line + each user continuation); off by default; note is ephemeral (user prompt only, never history/system); placement does not apply. **Editor pitfall (fixed v10.7.1):** the editor's text field for this key is created `disabled`, and this Tk build drops `delete`/`insert` on a disabled Text widget *silently* (no exception) — any handler that writes into a config-editor text field must ensure its state is `normal` first (see `load_config_handler()` order)
- Scene cast / join-leave (v12.0) → `prompts.scene_cast.{enabled,shape,min_present,max_churn,max_deferrals,placement,exit_guidance,arrival_guidance,allow_cutaway,cutaway_note}` (off by default; acts ONLY with 2+ selected characters). **The generator owns the cast**: `generation._build_scene_cast_plan()` computes who is present at each assistant slot BEFORE any request is sent, the model is told the state in the per-turn note and only NARRATES the transitions. `shape` is `minimal` (at most one join + one leave), `churn` (up to `max_churn` of each) or `none`. **A join AND a leave each cost one present character**, so 2 characters with `min_present: 2` can never change cast (the worker logs a WARNING and runs a static cast — do not "fix" this into a silent no-op). `placement` is `depth0` (default) or `system`, parsed by `note_placement()`
- Scene continuity check (v12.0) → `detection.scene_continuity.{enabled,fixes,max_rounds}` (enabled by default but INERT unless `prompts.scene_cast.enabled` is also on). A character who `exited` or is still `absent` may not speak or act. `detection.check_scene_continuity()` is **code-only** (attributed quoted lines + a curated narration action-verb list; no LLM, no phrase list to sanitize), so an empty `fixes` list falls back to `generation.DEFAULT_SCENE_CONTINUITY_FIX`
- Random names → `prompts.random_names.enabled`, `prompts.random_names.names[]` (optional list of names to randomly assign to characters, overrides character Name field when enabled)
- Random settings pool → `prompts.random_settings.enabled`, `prompts.random_settings.settings[]` (analogous to `random_names`). When `setting_enabled` is true and a selected character's own `setting` is empty, a setting is sampled from this pool (when enabled) instead of the hardcoded default. For a multi-character ensemble the per-character settings are unified to ONE shared setting picked from the ensemble, so the whole scene happens in a single location
- Conversation history for fixers → `generation.use_conversation_history_for_fixers` (whether to pass conversation context to slop/anti-slop fixer LLMs)
- Output metadata → `generation.include_metadata` (flat, schema-stable provenance block per output.jsonl record; trainer-safe; never modifies `conversations[]`; false = legacy two-field records)
- Retry missing quotations (v11.7.1) → `generation.retry_missing_quotations` (off by default; GUI label **"Experiment: Retry missing quotations"** in the Config Editor Generation tab, var `retry_missing_quotations_var_editor`). When on, a generated answer whose spoken line has NO quotation marks at all (e.g. `Its funny she says`) is detected by `detection.is_missing_quotation()` inside `generate_answer_with_retries()` (runs AFTER every phrase check + the incomplete-quote check, BEFORE Regex Watch) and sent back to the SAME main API slot for one retry (no fixer slot, nothing appended to the system prompt so prefix caches survive; if it comes back mark-less again the best-effort answer is kept, never dropped). Distinct from `is_incomplete_quote`, which only sees UNBALANCED quotes. Threaded as worker kwarg `retry_missing_quotations_local` (never positional) into `_per_turn_logic_gate` → its rewrite call. Guarded by `tests/test_missing_quotations.py`
- Style metrics (v11.7) → `generation.include_style_metrics` (off by default; when on AND `include_metadata` is on, each record's metadata gains a flat `style_*` block: `style_metrics.aggregate()` over the record's assistant turns, flattened with a `style_` prefix; duplication rows measure only the turn they carry)
- Quality scoring → `quality.enabled`, `quality.use_llm_scoring`, `quality.output_filter`, `quality.roleplay_dimensions` (opt-in roleplay dimensions: voice_distinctiveness / grounding / persona_alignment), `quality.cast_continuity` (v12.0 opt-in `cast_continuity` dimension: **LLM-ONLY, never in HEURISTIC_DIMENSIONS** — there is no measurable proxy for "was that exit earned?", and the objective half of the question is already answered for free by `detection.check_scene_continuity()`; registered in `CAST_LLM_DIMENSIONS` only, and it joins the composite through `active_scoring_dimensions()`), `quality.min_score_threshold`, `quality.max_chars_for_scoring`, `quality.max_profile_chars_for_scoring`, `quality.judge_max_attempts` / `quality.judge_retry_delay` / `quality.result_timeout` (v11.4 judge retry-and-drop policy), `quality.judge_json_repair_attempts` / `quality.judge_max_tokens` / `quality.judge_request_timeout` / `quality.judge_structured_output` (v11.5 judge JSON robustness: format-repair rounds, token budget, request timeout, optional `response_format`), `quality.judge_priority_enabled` / `quality.judge_priority` (v11.8.x vLLM priority scheduling: the toggle decides whether the `priority` field is sent at all — off by default, because backends without priority support reject the unknown field and vLLM rejects a non-zero priority when the served model is not priority-scheduled — and the value is clamped -10..10 with a default of 0, which every vLLM server accepts; -10 is the HIGHEST priority and must stay reachable, never flattened up to 0), `quality.tic_dimension` (v11.7 opt-in heuristic-only `tic_density` dimension, weight 0.10; measured from `style_metrics.py`, off by default like the roleplay dimensions), `quality.scoring_api.{url,model,key}`
- Datasets → `datasets.sources[]` (default `{sources: []}`) — names-only source definitions (kind, location, config/split/revision selectors, imported column names, aliases, visibility). Dataset *records* and credentials are never stored here; see `docs/dataset-bindings.md`

---

### `config/api_profiles.yml` [2]
**Purpose:** API compatibility profiles for request payload filtering per endpoint
**Key:** Profile definitions with `allow`, `rename`, `detect_hosts`, `label`
**Edit for:**
- Add new profile → Add new top-level key with `label`, `allow`, `rename`, `detect_hosts`
- Modify allowed params → Change `allow` list (use `all` or `from_config` for special cases)
- Key renames → `rename` dict (e.g., `repetition_penalty: frequency_penalty`)
- Auto-detection hosts → `detect_hosts` list (exact or `.suffix` for subdomains)
**Profiles:** `openai_compatible`, `openai`, `mistral`, `xai_grok`, `gemini_openai`, `anthropic_openai`, `custom`

---

### `requirements-datasets.txt` [40]
**Purpose:** Optional extras for Hugging Face Hub, local Parquet, and saved-HF dataset support (includes base `requirements.txt` + `datasets==4.4.2`; requires Python 3.9+). Base `requirements.txt` only pins `requests>=2.32.4,<3`
**Edit for:** Bump optional backend pins here, never in `requirements.txt`

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Add new API slot | `config/config.yml` (`api.apis[]`), `config_editor.py` (UI), `app_state.py` (counters), `generation.py` (worker logic) |
| Add detection type | `config/config.yml` (phrases/fixes/enabled), `detection.py` (`is_*()`), `app_state.py` (timestamps/counters), `generation.py` (worker kwargs + check call), `dashboard.py` (display), `config_editor.py` (Detection tab panel + save/load/validate) — see e.g. OOC Drift |
| Add config option | `config/config.yml` (default value), `config_editor.py` (UI + save/load), `config_loader.py` (if new structure), `generation.py` (pass to worker) |
| Add slop/anti-slop phrase | `config/config.yml` (`detection.*.phrases`) → `detection.py` phrase lists |
| Add/modify a Regex Watch pattern | `config/config.yml` (`detection.regex_watch.patterns[]`) and `detection.py` (`DEFAULT_REGEX_WATCH_PATTERNS` — the Restore Defaults source; keep the two in sync), `config_editor.py` (Detection tab panel, `_restore_default_regex_patterns()`), `tests/test_regex_watch.py` |
| Change rate limit | `config/config.yml` (`api.apis[].rate_limit_rpm`), `config_editor.py` (UI field), `api_handler.py` (`set_rate_limit()`) |
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
