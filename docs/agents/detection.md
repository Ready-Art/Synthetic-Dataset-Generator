<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Detection checks (`detection.py`)

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are adding or fixing a refusal/slop/OOC/Regex Watch/non-Latin/scene-continuity check.

**Related:** [`docs/agents/generation.md`](generation.md), [`docs/agents/config.md`](config.md)

## Contents

- `detection.py`
- `tests/test_regex_watch.py`
- `tests/test_non_latin.py`
- `tests/test_missing_quotations.py`
- Tasks that touch these files

---

### `detection.py` [10]
**Purpose:** Issue detection (refusals, user speaking, slop, anti-slop, out-of-character drift, Regex Watch)
**Key:** `is_refusal()`, `is_user_speaking()`, `is_slop()`, `is_anti_slop()`, `is_out_of_character()`, `is_incomplete_quote()`, `is_missing_quotation()`, `is_non_latin()`, `find_non_latin_characters()`, `LATIN_ALLOWED_PATTERN`, `NON_LATIN_SCRIPT_RANGES`, `is_regex_watch()`, `check_scene_continuity()`, `find_attributed_speakers()`, `find_character_actions()`, `find_regex_matches()`, `validate_regex_patterns()`, `DEFAULT_REGEX_WATCH_PATTERNS`, `RegexWatchError`
`is_missing_quotation()` (v11.7.1, apostrophe guard fixed in v11.7.2): mirrors the other `is_*()` shape `(bool, [(label, snippet), ...])` but takes only `text` (no phrase list) and records NO timestamp (it is an opt-in retry trigger, not a dashboard-tracked issue). It fires only when the reply has words to check, contains NO quote glyph (see `has_dialogue_quote_marks()` below), is longer than 3 words, and carries a speech-attribution verb (a curated list: says/said/whispered/told/asked/sighs/…). A quoted reply — balanced or unbalanced — never matches: the balanced case is `normalize_quotes`, the unbalanced one is `is_incomplete_quote`
**`has_dialogue_quote_marks()` (v11.7.2):** the single definition of "did the model mark its speech at all?", used by `is_missing_quotation()`. It strips word-internal apostrophes FIRST (`_APOSTROPHE_IN_WORD_RE` = `(?<=\w)['’](?=\w)`) and only then looks for `"`/`'`/`‘`/`’`/`“`/`”`. **This ordering is the point:** v11.7.1 bailed out on any apostrophe at all, so ordinary prose (`It's not funny she says`, `Melody's hand trembles as she speaks`) silently disabled the whole check and mark-less spoken lines reached the dataset — the experiment appeared on but never fired on realistic text. A **plural possessive** (`dogs'`, `the clocks' hands` — apostrophe after the final word character) is deliberately NOT stripped, because it is genuinely ambiguous with a closing single quote; leaving it counted only suppresses the check (no pointless retry), which is the safe direction. Regression cases for both sides live in `tests/test_missing_quotations.py` (`test_detector()` apostrophe positives + `test_has_dialogue_quote_marks()`)
**Non-Latin script check (v11.10.0):** `is_non_latin(text)` mirrors the other `is_*()` shape (`(bool, [(label, sentence), ...])`, timestamp into `issue_timestamps['non_latin']`, 1hr cutoff) but takes only `text` and is **purely character-class based** -- no LLM, no phrase list, no network. It is an **allow-list**, not a block-list: `LATIN_ALLOWED_PATTERN` accepts Basic Latin through Latin Extended-D, the ligatures, General Punctuation, super/subscripts, currency, letterlike, arrows, math operators, box drawing, geometric shapes and miscellaneous symbols/dingbats; anything outside it is flagged (Chinese, Japanese, Korean, Cyrillic, Greek, Arabic, Hebrew, Devanagari, Thai, the emoji planes). The allow-list is the point: a block-list of known scripts silently misses every script nobody listed, while anything outside Latin is by definition not Latin. `NON_LATIN_SCRIPT_RANGES` only supplies the readable script NAME in the label (plus `'Other non-Latin'` / `'Emoji / symbols'`); the decision is the allow-list. `find_non_latin_characters()` reports one entry per DISTINCT script (a CJK sentence would otherwise produce hundreds of rows), with the sentence holding the first offending character. `text.isascii()` is the fast path, so a pure-ASCII reply costs one method call. Accented Latin (`Café`, `naïve`, `straße`), curly quotes, guillemets and em dashes all pass -- the check must not fight ordinary English prose

**Regex Watch (v11.6):** `is_regex_watch(text, patterns)` mirrors the other `is_*()` shape (`(bool, [(pattern, sentence), ...])`, timestamp into `issue_timestamps['regex_watch']`, 1hr cutoff) but matches WHOLE-REPLY regular expressions instead of word-boundary phrases. `_compile_regex()` is `lru_cache`d and raises `RegexWatchError`; `validate_regex_patterns()` returns `(valid, [(pattern, message), ...])` and drops blanks/duplicates, so an editor save and a config load share one definition of "usable". An invalid pattern is skipped at run time (never raises), a zero-width match is ignored (it has nothing to rewrite and would loop). `find_regex_matches()` reports `(pattern, sentence, matched_text)`; the sentence is the one containing the match, capped at 300 chars
**Edit for:**
- Add detection type → New `is_*()` function + add key to `app_state.issue_timestamps`
- Non-Latin script check → `detection.py` (`is_non_latin()` / `find_non_latin_characters()` / `LATIN_ALLOWED_PATTERN` / `NON_LATIN_SCRIPT_RANGES`), `generation.py` (`DEFAULT_NON_LATIN_FIX` + the answer branch + the question/continuation/opening-mode re-roll), `generate.py` (config read + worker kwargs + metric card + issue panel), `config/config.yml` (`detection.non_latin.*`), `config_editor.py` (Detection tab panel + `_toggle_non_latin_fields()`), `dashboard.py` (metric label + panels + graph series), `app_state.py` (`non_latin_*`), `tests/test_non_latin.py`
- Scene continuity (v12.0) → `check_scene_continuity(text, blocked_names, expected_names=())` mirrors the `is_*()` shape `(bool, [(kind, name, sentence), ...])` and a timestamp into `issue_timestamps['scene_continuity']`, but takes a LIST OF BLOCKED NAMES instead of a phrase list: a character in state `absent`/`exited` may not speak or act. **Code-only, no LLM.** Attribution = an explicit speaker prefix (`Melody:` — script style, needs no verb), or the character's name within ~60 chars before / ~40 after a quoted span WITH a speech verb nearby; the span's own text is never searched, so a character merely *mentioned* in someone else's line (`"Where is Melody?"`) never flags. `find_character_actions()` blanks quoted dialogue FIRST (narration-only, the same rule `is_out_of_character` uses) and requires the name to PRECEDE the action verb in the same sentence, so `Cole nodded at Melody` blames Cole. `_scene_name_pattern()` uses word-char lookarounds, NOT `\b`, because `\b` never matches a name ending in punctuation (`A.B.`, `Dr. Vance`). Only `exited_speaks`/`exited_acts` are HARD (regenerate); `expected_missing` is SOFT (reported, never rewrites — the state machine settles a forgotten beat for free). **Attribution deliberately re-implements the closest-name heuristic from `quality._spoken_lines_by_character()` rather than importing it**: `quality.py` imports `app_state` and the judge machinery while `detection.py` is a leaf, so importing quality here would invert the dependency graph for ~30 lines of regex. Do not "deduplicate" it
- Regex Watch → `detection.py` (`is_regex_watch()` / `find_regex_matches()` / `validate_regex_patterns()` / `DEFAULT_REGEX_WATCH_PATTERNS`), `config/config.yml` (`detection.regex_watch.*`), `generate.py` (config load + validation, worker kwargs, metric card, `🔎 Regex Watch` tab + `_update_regex_watch_tab()`), `generation.py` (`apply_regex_watch()` / `rewrite_regex_watch_text()` + the four call sites), `config_editor.py` (Detection tab panel), `dashboard.py` (graph series + `clear_dashboard()`), `app_state.py` (counters, recent lists, `regex_watch_log`), `tests/test_regex_watch.py` / `tests/test_regex_watch_ui.py`
- Out-of-character drift → `is_out_of_character(answer, ooc_phrases)` mirrors the other `is_*()` shape (phrase-driven, word-boundary matching via `_candidate_patterns`, timestamp into `issue_timestamps['out_of_character']` on a 1hr cutoff) **but matches narration only**: `_strip_quoted_dialogue()` blanks every quoted span (straight and curly double/single quotes) before matching, so a listed phrase spoken by a character *inside* dialogue does NOT flag/regenerate the turn. Catches helpful-bot narration breaks ("As a helpful AI…", "Is there anything else I can help you with today?"), god-viewing the user, and fourth-wall drops that the LLM-judge consistency dimension then scores against the character card
- Phrase lists → Passed from config via `generate.py` → `worker()` (`current_ooc_phrases` / `current_ooc_fixes` / `current_ooc_enabled`)
- Regex Watch patterns → `generate.py` validates `detection.regex_watch.patterns` at run start (invalid entries are logged and dropped) and passes `current_regex_patterns` / `current_regex_watch_enabled` / `current_regex_watch_max_rounds` to `worker()` as kwargs
- GUI config → Config Editor Detection tab has an "Out-of-Character (OOC) Detection" LabelFrame (`col2_frame`) with an enable checkbox (`ooc_enabled_var_editor` + `_toggle_ooc_fields()`), phrases/fixes text areas (`ooc_phrases_text` / `ooc_fixes_text`), saved to / loaded from `detection.out_of_character.{enabled,phrases,fixes}`, validated in `validate_config_handler()`
- Timestamp tracking → `issue_timestamps[key].append(time.time())` with 1hr cutoff

---

### `tests/test_regex_watch.py`
**Purpose:** Headless tests for Regex Watch (v11.6) — the pattern engine in `detection.py` and the rewrite pass in `generation.py` (no model/network/display; `requests.post` is stubbed)
**Key:** validation keeps order/drops blanks/duplicates/reports syntax errors and oversized patterns; every shipped default compiles; the positive sentences match and clean prose does not (v11.7: eight shipped defaults incl. the scotoma denial-correction and absolute-trailer tics; `test_scotoma_tic_patterns()` covers the two new shapes); the reported sentence is the one holding the match (neighbours excluded) and is capped; zero-width and invalid patterns never hit and never raise; matching is case-insensitive; the timestamp is appended once and `record_timestamp=False` does not; a clean rewrite replaces the text; a rewrite that still matches, is empty, is much shorter than the original or unbalances quotes is discarded and the original kept; a non-200 is retried then given up; an empty pattern list and an unconfigured API are no-ops; a second shape introduced by the first rewrite is caught in the same pass; the loop is bounded by `max_rounds` and a leftover hit is reported `unresolved`; the shipped `config.yml` block is off by default, has 1-5 rounds and compiles
**Run:** `python -B tests/test_regex_watch.py`
**Edit for:** Add pattern-engine or rewrite-policy cases

---

### `tests/test_non_latin.py`
**Purpose:** Headless tests for the v11.10.0 non-Latin script check -- the Unicode allow-list detector in `detection.py` and the retry/re-roll paths in `generation.py` (no model/network/display; `requests.post` is stubbed)
**Key:** every reported script is flagged with its label (Chinese, Japanese, Korean, Cyrillic, Greek, Arabic, Hebrew, Devanagari, Thai, emoji); accented Latin (`Café`, `naïve`, `straße`), curly quotes, guillemets, an em dash, a star and a heart all pass; one row per DISTINCT script (a CJK sentence reports one, two scripts report two); `find_non_latin_characters()` returns `(script, character, sentence)`; a hit records one timestamp and a clean reply none; the shipped config block is off with a `fixes` list; an assistant answer is regenerated with the built-in `DEFAULT_NON_LATIN_FIX` (or the configured fix), a permanently stray reply is never written, and the generated question / user continuation / greeting / first line are re-rolled (off keeps them as-is)
**Run:** `python -B tests/test_non_latin.py`
**Edit for:** Add detector or retry-policy cases

---

### `tests/test_missing_quotations.py`
**Purpose:** Headless tests for the v11.7.1 "Experiment: Retry missing quotations" pipeline (no model/network/display; `requests.post` is stubbed)
**Key:** the detector flags a spoken line with NO quotation marks and an attribution verb (straight/curly-quoted replies, pure narration and short fragments are all non-hits); the retry pass — off → mark-less reply written as-is (exactly one main-API request); on → the answer is sent back to the SAME main API slot, nothing appended to the system prompt (prefix-cache friendly), the quoted re-reply is what gets written; a still-broken retry is kept (bounded to one retry per attempt, never dropped); a properly quoted reply needs no extra request
**Run:** `python -B tests/test_missing_quotations.py`

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Add/modify the non-Latin script check (v11.10.0) | `detection.py` (`is_non_latin()` / `find_non_latin_characters()` / `LATIN_ALLOWED_PATTERN` — keep it an ALLOW-list, never a block-list of known scripts), `generation.py` (`DEFAULT_NON_LATIN_FIX` + the answer branch in `generate_answer_with_retries()` + the question/continuation/opening-mode re-roll), `generate.py` (read `detection.non_latin.*` at Start + `current_non_latin_enabled` / `current_non_latin_fixes` worker kwargs), `config/config.yml` (`detection.non_latin.*`, off by default), `config_editor.py` (Detection tab panel + `_toggle_non_latin_fields()` + save/load/validate), `dashboard.py` (metric label + panels + graph series), `app_state.py` (`non_latin_count_total` / `non_latin_counts_per_api` / `recent_non_latin_*` / the `non_latin` timestamp key), `tests/test_non_latin.py` — keep the check LLM-free (it is a character-class test, not a judge call) |
| Add/modify missing-quotations retry (v11.7.1, apostrophe guard v11.7.2) | `detection.py` (`is_missing_quotation()` + `has_dialogue_quote_marks()`; keep the word-internal-apostrophe strip BEFORE the quote-glyph test or the check goes blind on contractions again), `generation.py` (`generate_answer_with_retries()` check + `retry_missing_quotations_local` kwarg on worker/`_per_turn_logic_gate`/call sites), `generate.py` (read `generation.retry_missing_quotations` + worker kwargs), `config/config.yml` (default), `config_editor.py` (Generation tab checkbox + save/load), `tests/test_missing_quotations.py` |
