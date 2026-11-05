<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Generation pipeline (`generation.py`)

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching the worker loop, retries, prompts, cast/arc helpers, metadata or output writing.

**Related:** [`docs/agents/detection.md`](detection.md), [`docs/agents/quality.md`](quality.md), [`docs/agents/generate-ui.md`](generate-ui.md)

## Contents

- `generation.py`
- `tests/test_opening_mode.py`
- `tests/test_persona_arc.py`
- `tests/test_random_settings.py`
- `tests/test_progress_accounting.py`
- `tests/test_scene_cast.py`
- Tasks that touch these files

**Retry checks** live in [`docs/agents/generation-retries.md`](generation-retries.md).

---

### `generation.py` [12]
**Purpose:** Worker loop, LLM calls, conversation generation, output writing
**Key:** `worker()`, `generate_answer_with_retries()`, `write_conversation()`, `call_slop_fixer_llm()`, `call_anti_slop_llm()`, `rewrite_regex_watch_text()`, `apply_regex_watch()`, `DEFAULT_NON_LATIN_FIX`, `_build_scene_cast_plan()`, `_advance_cast_state()`, `_cast_presence_note()`, `_cast_names_for_slot()`, `DEFAULT_SCENE_CONTINUITY_FIX`, `SCENE_CONTINUITY_MAX_ROUNDS`
**Edit for:**
- API call flow → `generate_answer_with_retries()` main loop
- Slop fixing → `call_slop_fixer_llm()` (API slot 5, index 4) / `call_anti_slop_llm()` (API slot 6, index 5)
- **The retry checks live in [`docs/agents/generation-retries.md`](generation-retries.md)** — read that file before adding, moving or fixing anything that can send an answer back to the model (the phrase branches, Regex Watch, the non-Latin check, scene continuity, the missing-quotation retry, the repeated-opener guard). It also records the RETRY ORDER, the thing most easily broken by inserting a check in the wrong place
- Scene cast (v12.0) → the plan/state helpers (`_scene_cast_feasible()`, `_build_scene_cast_plan()`, `_cast_state_from_plan()`, `_cast_slot_transitions()`, `_cast_names_for_slot()`, `_advance_cast_state()`, `_cast_presence_note()`) are PURE module-scope functions (no Tk, no network, no `app_state` mutation), so they unit-test headlessly. In `worker()` the plan is computed AFTER the profile loop (`scene_character_names` completes there) and BEFORE `base_scene_prompt`, then wired into `slot_notes()` LAST so the final thing the model reads is who may speak. **`_advance_cast_state()` ASYMMETRY IS DELIBERATE AND MUST NOT BE "SIMPLIFIED":** an un-narrated ARRIVAL is FORCED to `present` (being in the scene is the default and cannot corrupt the transcript), but an un-narrated EXIT is CANCELLED back to `present`. Forcing an exit would make that character's very next legitimate line a continuity violation the generator invented itself. Cancelling is the only direction that cannot produce an incoherent dataset. Deferral counts live on the STATE (`state['_deferrals']`), never on the plan, because the plan is immutable and shared by every slot. **`_cast_presence_note()` carries NO per-reply counter:** it goes on `placement: system`, so a slot number would make every reply differ and destroy the prefix cache; it must depend only on the present/absent SETS and this reply's transitions. In duplication mode the state advances from the PRIMARY answer only (`primary_api_answer_for_conv_flow`), or the duplicated rows diverge
- Answer-prompt native variables (v12.5, widened v12.7) → `fill_answer_prompt()` is the answer prompt's formatting boundary, the one it was missing; every other authored prompt fills its variables where it formats, so a prompt that wrote `{subject}` sent the model the literal token. Fills `{subject}`/`{context}` from `task[3]`/`task[4]` plus (v12.7) `{character_name}` = `scene_character_names[0]`, `{character_names}` = `", ".join(...)`, `{character_profile}` = `character_injection`; empty → `N/A`, and it goes through `render_with_native_placeholders()` when `task_dataset_values` is set so `{{ds:alias}}` keeps its single-pass guarantee. **`{subject}` is the SOURCE PASSAGE, made safe inline in v12.7:** `_clean_chunk_for_inline()` collapses whitespace and strips a trailing terminator (the reported prompt rendered `guts.,`). **v12.7 REVERSES the v12.5 rule that the name is not a variable, and that reversal IS the release:** `{subject}` in a name slot got the passage, which the model reproduced every turn. `generate_question()`/`generate_user_continuation()` fill the name variables too; `worker()` calls it once per turn so the answer request and the gate share the text. Shipped prompts name none of the five, so a default run is byte-identical. The continuation trim and the repeated-reply guard are in [`docs/agents/generation-retries.md`](generation-retries.md)
- **Repeated-opener guard (v12.5)** → the branch lives in `generate_answer_with_retries()` AFTER Regex Watch. The kwarg pair `repeat_opener_enabled_local` / `repeat_opener_fixes_local` / `repeat_opener_max_retries` / `previous_assistant_replies_local` is threaded through `worker()` and `_per_turn_logic_gate()`, and `previous_assistant_replies` is read from `current_llm_conversation_context`. See [`docs/agents/generation-retries.md`](generation-retries.md) — the cache-safety rule (fix on the ANSWER PROMPT, never the system prompt) is there and must not be moved
- Output format → `write_conversation()` ShareGPT structure
- Opening mode → `generate_greeting()`, `generate_first_line()`, `enqueue_opening_tasks()`
- Quality scoring → `quality.score_conversation()` call (via `_score_and_store_quality()`) after conversation complete, passing `character_profile_text=character_injection` and `scene_characters=scene_character_names`
- **Judge gatekeeper (v11.4)** → `_score_and_store_quality()` returns the judge verdict (never `None`) and `_quality_dropped(verdict)` tells the write paths to skip the row. `quality_dropped_this_task` (set per task) suppresses the non-duplication final write, the duplication per-turn write and `completed_task_ids.add()`; the row is dropped, not requeued and not flagged. `quality_dropped` is persisted in `save_generation_state()`
- **Progress accounting (v13.0.5) → `_credit_task_progress(task_id, turns=None, queue=None)`** — `processed_tasks` is **task-level, not per-turn**, and this is its ONLY writer: one task's turns, capped, **idempotent** via `app_state.progress_credited_task_ids` (persisted in the state file), **never refunded**. The old per-turn credit + epilogue refund netted zero for every async task, so the bar sat at turns in flight and oscillated. Call sites: the sync write path, `_mark_task_written()`, both judge-drop branches. `turns` defaults to `app_state.current_num_turns_for_progress`, set by `worker()`. **Never re-add a per-turn credit or a refund**; the ETA's `start_times_list` append stays where the increment block was
- Turn completion check → `conversation_is_complete()`
- Conversation context for fixers → `call_slop_fixer_llm()` and `call_anti_slop_llm()` now accept `conversation_history` parameter to provide full conversation context for coherent rephrasing
- Character prompt injection → character profile block built in `worker()` (personality/traits are conditionally injected lines in the profile; `persona` = inner personality is deliberately EXCLUDED from the profile and re-injected later as an INNER PERSONA BREAKTHROUGH block once the scene reaches the configured slot). Setting resolution → a `settings_for_selected[]` array is computed per task before the profile loop: each selected character keeps its own non-empty `setting`, else samples from the `random_settings` pool (if enabled) or the `A standard indoor environment.` default; when >1 character is selected the ensemble is unified to one shared setting picked from the distinct candidate settings (order-preserving dedupe), which becomes both the injected `Setting:` line for every character and `meta_setting`
- Persona/emotional-state split → `_split_emotional_arc_entry()`, `_expand_emotional_arc()`, `_emotional_state_injection()`, `_user_emotional_state_injection()` (v10.6.0, user-side twin of `_emotional_state_injection`), `_persona_block_for_characters()` (all above `worker()`). User-side states (optional) are selected once per task when `user_emotional_states_enabled_local`, expanded across the USER turns (`_user_emotional_slot_state(user_turn_no)`: turn 1 = opening question/first line, continuation after answer k = turn k+1), and appended via `user_turn_note=` to the ephemeral user prompt of `generate_question()`, `generate_first_line()` and `generate_user_continuation()` — never stored in history, never in the system prompt. Recorded on metadata as `user_emotional_state`. The worker builds `base_scene_prompt` (system + lore + persona-free profile) once per task. It is the system message of every greeting, answer and user-continuation request in the task (a retry still appends its jailbreak or fix instruction to it); the generated question gets it plus `QUESTION_USER_SIDE`, and the first-line and fixer requests have their own system prompts. The per-reply notes come from `slot_notes(slot_no)`, which returns `(system note, depth-0 note)`: the INNER PERSONA BREAKTHROUGH block from `persona_breakthrough_turn` onward, then the slot's emotional state, which is a flat name or an arc stage (`'neutral -> anxious'` expanded across the conversation's assistant turns), each in the part its placement names (`prompts.character.persona_breakthrough_placement`, `prompts.emotional_states.placement`; `depth0` or `system`, default `depth0`, parsed by `note_placement()`). The depth-0 note is appended to the slot's final user message, the answer prompt or the opening-mode greeting prompt, after the history and never stored in it; the system note is appended to that slot's answer or greeting system prompt, before any retry fix, and the user continuation after the answer gets the same system prompt (without the fix), as in v10: if the continuation used the plain scene prompt instead, answers and continuations would alternate between two system prompts and a server that keeps one cached prompt per slot (llama-server) would recompute the history on every request. The generated question gets neither. With both on `depth0`, from one answer request to the next the system prompt and the history are the same text, which lets a server's prefix cache reuse them; keep the default that way. `placement_cache_warning()` estimates the history share a `system` placement makes the server compute again; `start_processing` logs it and shows a toast, and the config editor shows it under the placement dropdowns. Opening-mode greeting = slot 1, so conversation turn N lives at slot N+1.
- Generated first user message → when characters are profiled, its system prompt is the scene prompt plus `QUESTION_USER_SIDE`, which tells the model it writes only the user's side and that the user is not one of the characters
- Output metadata → `_build_record_metadata()` (26 flat keys at `meta_version` 7, same shape on every record, gap-filled — never `None`; `_safe_float()` keeps numeric types stable; v3 added the three roleplay dimension columns, v5 added `quality_logic`, v6 (v11.7) added `quality_tic_density` (`-1.0` whenever `quality.tic_dimension` is off or unscored) plus, when `include_style_metrics=` is passed, the flat `style_*` block built by `_style_metrics_for_record()` from `style_metrics.response_metrics()`/`aggregate()`; **v7 (v12.0) added `cast_schedule` / `cast_events` (the schedule string and the applied transitions, `""` when the feature is off) and `quality_cast_continuity` (`-1.0` when off/unscored)**) + `_score_and_store_quality()` (quality scoring deduped by task_id; called BEFORE both write paths so metadata can carry scores; accepts `scene_characters=` for the voice-distinctiveness heuristic)
- Dataset binding → `bind_task_datasets(queue, task, ...)` near the top of the `worker()` task loop freezes one row per enabled source into an immutable `task_dataset_values` mapping for the life of the task (every turn, fix and retry); `validate_task_templates()` pre-checks the question, continuation, greeting and first-line templates up front. The static authored inputs in `dataset_authored_inputs` (system prompts, answer prompt, jailbreaks and fixes, lore, character fields, emotional states, name/setting pools, first-line styles) are rendered once per task with `render_task_inputs()`. The question, continuation, greeting and first-line templates are rendered where they are formatted, with `render_with_native_placeholders()`: `generate_question()`, `generate_user_continuation()`, `fill_greeting_prompt()` (called by `generate_greeting()`) and `generate_first_line()` take a `dataset_values=None` kwarg, and `None` skips rendering entirely, so generation without datasets behaves byte-identically to the pre-dataset engine. **Questions-file lines (v11.9.0):** the `use_questions_file_local` branch does NOT use `initial_user_question = task[3]` directly; it calls `render_question_task(task, task_dataset_values, append_alias=questions_append_alias_local, separator=questions_append_separator_local)[3]`, and the binding block validates `task[3]` up front with `validate_task_templates` so an unmapped alias fails the task instead of reaching the model. Both kwargs are keyword-only (`questions_append_alias_local` / `questions_append_separator_local`) `docs/dataset-bindings.md` (Placeholder syntax) lists the fields

---

### `tests/test_opening_mode.py` [16]
**Purpose:** Tests for opening mode functionality
**Edit for:** Add new opening mode tests (greeting, first-line, worker integration). It also checks where the per-reply note goes, that the requests of a task share one system prompt, and the generated question's `QUESTION_USER_SIDE` line

---

### `tests/test_persona_arc.py` [19]
**Purpose:** Unit tests for the persona/emotional-arc helpers in `generation.py`
**Key:** `main()` covers `_split_emotional_arc_entry`, `_expand_emotional_arc`, `_emotional_state_injection`, `_persona_block_for_characters`
**Edit for:** Add arc-parsing/expansion/injection cases (arrows, stage distribution, header text, single vs multi-char labels)

---

### `tests/test_random_settings.py` [20]
**Purpose:** Standalone validation of the setting resolution/unification algorithm added to `worker()`
**Key:** `resolve_settings()` mirrors the per-task block; `run()` covers keep-own-setting, pool-sample-on-empty, multi-character unification, selection-off default, pool-disabled default, and ensemble-bounded unification across a seed range
**Edit for:** Add settings-resolution edge cases (blank/whitespace own setting, pool with duplicates, 3+ char ensembles)

---

### `tests/test_progress_accounting.py` [45]
**Purpose:** Headless regression test for the v13.0.5 progress accounting fix

### `tests/test_scene_cast.py` [44]
**Purpose:** Headless guard for the v12.0 Scene Cast feature — schedule invariants (no exit below `min_present`, every character exactly once, no character both joins and leaves, slots in range, exit after enter, minimal = at most one of each), the state machine (defer/force/cancel, the deliberate asymmetry asserted explicitly, no illegal state ever produced, deferrals live on the state not the plan), cast-note cache stability (two static slots are BYTE-IDENTICAL, no `None`, no per-reply counter), the detector (speaker prefix, name-adjacent quotes, `"Where is Melody?"` does not flag, narration actions, possessives, names ending in punctuation, accents, `expected_missing` is soft, timestamps only on hard hits), metadata v7 (`cast_schedule` / `cast_events` / `quality_cast_continuity`, identical key sets), the dimension gating (LLM-only, never heuristic, driven through the real `global_config` ConfigLoader) and the shipped config being inert
**Edit for:** Add cases when the schedule, the state machine, the note or the detector changes

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Progress accounting: the bar stalls or oscillates (v13.0.5) | `generation.py` (`_credit_task_progress()`, both completion sites, both drop branches, the skip-completed branch, `save_generation_state()`), `app_state.py` (credited set + lock), `generate.py` + `control.py` (resume seeding), `tests/test_progress_accounting.py` |
| Add/modify scene cast (v12.0) | `generation.py` (`_scene_cast_feasible()` / `_build_scene_cast_plan()` / `_cast_state_from_plan()` / `_cast_slot_transitions()` / `_cast_names_for_slot()` / `_advance_cast_state()` / `_cast_presence_note()` — keep them PURE and keep the exit-cancelled / arrival-forced asymmetry), `detection.py` (`check_scene_continuity()` + the attribution helpers), `generation.py` (`generate_answer_with_retries()` branch after OOC, `slot_notes()`, `write_conversation()` cast args, `_build_record_metadata()` + `_METADATA_SCHEMA_VERSION`, `placement_cache_warning()`), `generate.py` (config read + the five worker kwargs + metric card + issue panel + color_map), `config/config.yml` (`prompts.scene_cast.*`, `detection.scene_continuity.*`, `quality.cast_continuity`), `config_editor.py` (three panels + save/load/toggle/validate), `quality.py` (`CAST_SCORING_DIMENSIONS` / `CAST_LLM_DIMENSIONS` / `is_cast_scoring_enabled()` + the judge block + the `item[:10]` queue unpack), `app_state.py` (counters, recent lists, `cast_events_log`, `record_cast_events()`, the `scene_continuity` timestamp key), `dashboard.py` (four dicts + signature + panels + clear), `tests/test_scene_cast.py`, `docs/scene-cast.md` — keep the feature OFF by default so the default run stays byte-identical to v11.10.0 |
| Modify output format | `generation.py` (`write_conversation()`); its per-turn text pipeline order is load-bearing — see [`core-modules.md`](core-modules.md) |
| Modify retry logic, Regex Watch rewriting, the non-Latin / scene-continuity / missing-quotation / repeated-opener branches | **see [`generation-retries.md`](generation-retries.md)** — one row per check, with the retry ORDER |
| Add/modify output metadata field | `generation.py` (`_build_record_metadata()` schema + `_safe_float()`), keep same-keys/same-types/gap-filled (-1.0/"") invariant, bump `_METADATA_SCHEMA_VERSION` and the meta_version check in `tests/test_opening_mode.py`; config flags `generation.include_metadata` / `generation.include_style_metrics` (the latter adds the flat `style_*` block via `_style_metrics_for_record()`) |
| Change what a request contains (prompt text, message order or role) | `generation.py`, `tests/test_opening_mode.py` |
| Add/modify the answer prompt's native variables (v12.5, widened v12.7) | `generation.py` (`fill_answer_prompt()` + `_clean_chunk_for_inline()` + `ANSWER_PROMPT_NATIVE_VARIABLES` + the per-turn call in `worker()` + the `character_names=` kwarg on `generate_question()`/`generate_user_continuation()`), `docs/prompts-and-characters.md` (Template Variables), `tests/test_repeat_opener_and_answer_vars.py` — keep a variable-free prompt byte-identical. **Since v12.7 the name IS a variable** (`{character_name}`); it is filled from `scene_character_names`, the same list the profile's `Name:` line is built from, so the two cannot disagree |
