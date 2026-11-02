<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Retry checks in `generate_answer_with_retries()`

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are adding, moving or fixing one of the checks that can send an answer back to the model — the phrase-driven branches, Regex Watch, the non-Latin check, the scene-continuity branch, the missing-quotation retry, the repeated-opener guard or the repeated-reply guard.

**Related:** [`docs/agents/generation.md`](generation.md) (worker loop, prompts, metadata, output), [`docs/agents/detection.md`](detection.md) (the detectors themselves), [`docs/agents/quality.md`](quality.md)

## Contents

- The retry order (read this first)
- Per-check notes
- Tasks that touch this file

---

## The retry order (read this first)

`generate_answer_with_retries()` is one `while True` loop per attempt. A branch that `continue`s
re-requests the same slot; a branch that `break`s abandons the attempt and a fresh one starts.

The order is deliberate and is the thing most easily broken by inserting a check in the wrong place:

1. **Non-Latin script** (v11.10.0) — FIRST among the checks. A half-foreign reply is wrong at the
   character level, so every later check (quotes, slop, regex) would be inspecting text about to be
   thrown away.
2. **Refusal / user speaking / slop / anti-slop** — the phrase-driven branches.
3. **OOC drift** — a character-level wrongness, but only in narration.
4. **Scene continuity** (v12.0) — the OOC branch's sibling.
5. **Incomplete quote** — a shape pass.
6. **Missing quotations** (v11.7.1) — a shape pass.
7. **Regex Watch** (v11.6) — runs only once every phrase-driven check has passed, so a rewrite never
   races another fix.
8. **Repeated opener** (v12.5) — LAST, so it judges the text that is actually about to be written.
9. **Repeated reply** (v12.7) — the opener guard's sibling, immediately AFTER it. Both compare the
   reply to the conversation rather than to a rule, and the whole-reply check must see the text the opener
   check finally settled on, so the two run together at the end.

The rule behind 5–9: the shape passes (quotes, regex) must run on text that is about to be KEPT, so they
belong after every check that can still replace the text.

---

## Per-check notes

### Regex Watch (v11.6)
`apply_regex_watch()` wraps `rewrite_regex_watch_text()`. A hit is rewritten by the SAME main API slot that
produced the text (no fixer slot), up to `max_rounds` times, re-checking the whole reply after each round. A
content rejection (empty, under half the original length, still matches its own pattern, unbalanced quotes) is
FINAL for that attempt and returns `None`; only transport failures (network/timeout/non-200) are retried. It
runs on answers, generated questions (`generate_question()`), user continuations
(`generate_user_continuation()`) and opening-mode greetings/first lines (`_request_opening_message()`).
`REGEX_WATCH_MAX_ROUNDS` is module scope because the `worker()` signature uses it as a default.

### Non-Latin script check (v11.10.0)
`DEFAULT_NON_LATIN_FIX` is module scope (the fallback instruction when `detection.non_latin.fixes` is
empty) and the branch lives in `generate_answer_with_retries()`, plus the re-roll in `generate_question()` /
`generate_user_continuation()` / `_request_opening_message()`. The kwarg is `non_latin_enabled_local` (+
`non_latin_fixes_local` on the answer path) and is threaded through `worker()` and `_per_turn_logic_gate()`.
On exhaustion the answer branch does NOT keep the turn: it breaks to a fresh attempt, so a permanently stray
reply is never written. Guarded by `tests/test_non_latin.py`.

### Scene continuity retry branch (v12.0)
In `generate_answer_with_retries()` AFTER the OOC branch and BEFORE Regex Watch / the missing-quotation retry
(it is a character-level wrongness like OOC, while those two are shape passes that must run on text about to be
kept). Kwargs `scene_continuity_enabled_local` / `scene_continuity_fixes_local` /
`scene_continuity_max_rounds` / `cast_blocked_names_local` / `cast_expected_names_local`. On exhaustion the
best-effort answer is KEPT, never dropped.

### Repeated-opener guard (v12.5)
`detection.is_repeated_opener()` plus the branch in `generate_answer_with_retries()` AFTER Regex Watch and
BEFORE the success return.

**This is the only check that compares a reply to the CONVERSATION rather than to a phrase list or a rule**,
which is the whole point: `generation.history_size` prunes `app_state.question_history` (recent opening
QUESTIONS), never the assistant turns, so a model that opened every reply the same way was invisible to every
check. `previous_assistant_replies_local` is read straight from `current_llm_conversation_context` in `worker()`
so it cannot drift from the history the request sends.

**The fix goes on the ANSWER PROMPT (`answer_prompt_iter`), never the system prompt** — the answer prompt is
the final user message, so the system prompt and the whole history stay byte-identical and a prefix-caching server
still serves them; appending to the system prompt would invalidate the cache on every retry, the cost the `depth0`
placements exist to avoid. `answer_prompt_iter` is reset from the original template per outer attempt and each retry
REBUILDS it (`template + "\n\n" + fix`) so two retries never stack two copies.

Kwargs `repeat_opener_enabled_local` / `repeat_opener_fixes_local` / `repeat_opener_max_retries` /
`previous_assistant_replies_local`, threaded through `worker()` and `_per_turn_logic_gate()`. On exhaustion the
best-effort answer is KEPT, never dropped. Guarded by `tests/test_repeat_opener_and_answer_vars.py` /
`tests/test_repeat_opener_ui.py`.

### Repeated-reply guard (v12.7)
`detection.is_repeated_reply()` plus the branch in `generate_answer_with_retries()` immediately AFTER the
repeated-opener branch and BEFORE the success return.

**It is the opener guard's sibling and catches what that guard cannot.** `is_repeated_opener()` compares the first
few CONTENT words, so it catches a reused sentence frame (`She lets out a breath` / `She lets out a laugh`). A
reply repeated WHOLE shares no frame with anything — it *is* the earlier reply — so the opener check sees nothing.
The reported defect was three byte-identical 884-character replies in one conversation, produced by an answer prompt
that was constant across turns (it embedded the same `{subject}` chunk every time). `is_repeated_reply()` compares
the whole reply: whitespace collapsed, case folded, then a `difflib` similarity ratio at or above
`REPEATED_REPLY_SIMILARITY` (0.92). Replies under `REPEATED_REPLY_MIN_CHARS` (120) never hit, because two
short replies can coincide innocently ("She nods.") and staying quiet only suppresses a retry. It records NO
timestamp, the same reasoning `is_repeated_opener()` documents.

**Same cache contract as the opener guard:** the fix goes on the ANSWER PROMPT (`answer_prompt_iter`), never the
system prompt, and each retry REBUILDS it from the original template so two retries never stack two copies.
`DEFAULT_REPEAT_REPLY_FIX` points at the likely CAUSE rather than only the symptom — it tells the model this is
a NEW turn and that its earlier reply is already written — so the guard is useful even against a prompt the
operator has not fixed yet.

Kwargs `repeat_reply_enabled_local` / `repeat_reply_fixes_local` / `repeat_reply_max_retries`, threaded through
`worker()` and `_per_turn_logic_gate()`, reusing the same `previous_assistant_replies_local` list. On exhaustion
the best-effort answer is KEPT, never dropped. Guarded by `tests/test_repeat_opener_and_answer_vars.py` /
`tests/test_repeat_reply_ui.py`.

### Continuation history trim (v12.7, not a retry)
`generate_user_continuation()` sent `[system] + conversation_history_for_llm + [user]`, and the worker has ALREADY
appended her reply to that history, so the final user message's `{last_assistant_message}` embedded the same reply a
second time. When the template names the variable the trailing assistant message is dropped from the history sent with
the request; a template that does not name it keeps the full history, so an existing config's requests are unchanged.
The variable's VALUE is unchanged, so the prompt still reads naturally. The shipped continuation prompt names no
variables, so a default run is byte-identical.

---

## Tasks that touch this file

| Task | Files to edit |
|------|---------------|
| Modify retry logic | `generation.py` (`generate_answer_with_retries()`, `MAX_TASK_REQUEUES`) |
| Change how Regex Watch rewrites text | `generation.py` (`rewrite_regex_watch_text()` / `apply_regex_watch()`), `config/config.yml` (`detection.regex_watch.max_rounds`), `generate.py` (`current_regex_watch_max_rounds`), `tests/test_regex_watch.py` |
| Add/modify the repeated-reply guard (v12.7) | `detection.py` (`is_repeated_reply()` / `REPEATED_REPLY_MIN_CHARS` / `REPEATED_REPLY_SIMILARITY` / `_normalize_for_repeat()`), `generation.py` (`DEFAULT_REPEAT_REPLY_FIX` + `REPEAT_REPLY_MAX_RETRIES` + the branch after the repeated-opener branch + the `answer_prompt_iter` handling + the `repeat_reply_*` kwargs on worker/`_per_turn_logic_gate`), `generate.py` (config read + the three worker kwargs), `config/config.yml` (`generation.repeat_reply_*`, off by default), `config_editor.py` (checkbox + Spinbox + fixes Text + save/load/validate; **the Text is created ENABLED and never disabled**), `tests/test_repeat_opener_and_answer_vars.py` / `tests/test_repeat_reply_ui.py` — keep the fix on the ANSWER PROMPT (not the system prompt) or the prefix cache dies, and keep it AFTER the opener guard so it judges the settled text |
| Add/modify the repeated-opener guard (v12.5) | `detection.py` (`is_repeated_opener()` / `opener_tokens()` / `REPEATED_OPENER_WORDS`), `generation.py` (`DEFAULT_REPEAT_OPENER_FIX` + `REPEAT_OPENER_MAX_RETRIES` + the branch after Regex Watch + the `answer_prompt_iter` handling + `previous_assistant_replies` in `worker()`), `generate.py` (config read + the three worker kwargs), `config/config.yml` (`generation.repeat_opener_*`, off by default), `config_editor.py` (checkbox + Spinbox + fixes Text + save/load/validate; **the Text is created ENABLED and never disabled**), `tests/test_repeat_opener_and_answer_vars.py` / `tests/test_repeat_opener_ui.py` — keep the fix on the ANSWER PROMPT (not the system prompt) or the prefix cache dies |
| Add/modify the non-Latin script check (v11.10.0) | `detection.py` (`is_non_latin()` / `find_non_latin_characters()` / `LATIN_ALLOWED_PATTERN` — keep it an ALLOW-list, never a block-list of known scripts), `generation.py` (`DEFAULT_NON_LATIN_FIX` + the answer branch + the question/continuation/opening-mode re-roll), `generate.py` (config read + `current_non_latin_enabled` / `current_non_latin_fixes` worker kwargs), `config/config.yml` (`detection.non_latin.*`, off by default), `config_editor.py` (Detection tab panel + `_toggle_non_latin_fields()` + save/load/validate), `dashboard.py` (metric label + panels + graph series), `app_state.py` (`non_latin_count_total` / `non_latin_counts_per_api` / `recent_non_latin_*` / the `non_latin` timestamp key), `tests/test_non_latin.py` — keep the check LLM-free (it is a character-class test, not a judge call) |
| Add/modify missing-quotations retry (v11.7.1, apostrophe guard v11.7.2) | `detection.py` (`is_missing_quotation()` + `has_dialogue_quote_marks()`; keep the word-internal-apostrophe strip BEFORE the quote-glyph test or the check goes blind on contractions again), `generation.py` (`generate_answer_with_retries()` check + `retry_missing_quotations_local` kwarg on worker/`_per_turn_logic_gate`/call sites), `generate.py` (read `generation.retry_missing_quotations` + worker kwargs), `config/config.yml` (default), `config_editor.py` (Generation tab checkbox + save/load), `tests/test_missing_quotations.py` |
