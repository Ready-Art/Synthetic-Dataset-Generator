<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 🔧 How It Works

## Generation Pipeline

1. **Task Creation** — Random chunks are extracted from input files (subject + surrounding context), or questions are read from `questions.txt`
2. **Question Generation** — An LLM generates an initial question based on the subject/context, avoiding recent question history. Responses are checked for malformed content (excessive newlines/length)
3. **Answer Generation** — The assistant generates an answer, with automatic detection and retry for:
   - Refusals → Apply jailbreak prompts and retry
   - User speaking → Apply speaking fix prompts and retry
   - Slop → Attempt sentence-level rewriting via Slop Fixer LLM, then fallback to system prompt fixes
   - Anti-slop → Attempt sentence-level rewriting via Anti-Slop Fixer LLM
   - Incomplete quotes → Retry with fix instruction, then programmatic auto-fix
4. **User Continuation** — If multi-turn, an LLM generates the user's next message. Responses are also checked for malformed content
5. **Per-Reply Note (NEW in v10.0)** — The scene prompt (system prompt + lore + persona-free character profiles) is the system message of every request in a task (a retry still appends its fix instruction to it, as in step 3). Each assistant reply gets a note: the INNER PERSONA BREAKTHROUGH (from `persona_breakthrough_turn` onward) and the emotional-state injection (a flat state, or the current stage of an arc advanced one step per reply). Each block goes where its placement says. On `depth0` (the default) it follows the reply's final instruction (the `answer` prompt, or the `greeting` prompt in opening mode), after the conversation history, and is not stored in it, so the system prompt never changes and the server's prefix cache keeps the history. On `system` it is appended to that reply's system prompt, which changes whenever the block does; the user continuation after that reply shares its system prompt. Questions get the scene prompt without either block, and on `depth0` so do user continuations
6. **Repeat** — Steps 3–5 repeat for the configured number of turns
7. **Post-Processing** — Text cleaning (reasoning removal, asterisk handling, markdown stripping, quote normalization, straight quote repair, etc.)
8. **Quality Scoring** — The completed conversation is scored across the seven core dimensions plus, when `quality.roleplay_dimensions` is on, the three roleplay dimensions, and, when `quality.tic_dimension` is on, the tic-density dimension (all opt-in sets are heuristic-based; only the core set is LLM-refined) *before* the write, so the score can be attached to the record's metadata. When LLM scoring is on the judge is the gatekeeper: a conversation it drops (invalid JSON, a `logic` score of 0, or every retry failed) is **not written at all**. Otherwise, if the composite score is below the threshold, the task is flagged for review
9. **Output** — Write to JSONL file or PostgreSQL database, appending the flat `metadata` provenance/quality block when `generation.include_metadata` is true (see [Record Metadata Block](output-formats.md#record-metadata-block-new-in-v990))
10. **State Save** — Update crash recovery state (including quality scores, judge drops and review flags)

## Quality Scoring Pipeline

```
Conversation Completed (≥ 2 messages)
    │
    ▼
Heuristic Scoring (always runs, no API call)
    ├── Diversity: TTR, sentence variance, trigram repetition, paragraph structure
    └── Technical: quote balance, asterisks, em dashes, whitespace, lowercase check
    │
    ▼
Roleplay Dimensions (optional, if roleplay_dimensions: true)
    ├── Voice distinctiveness: bigram overlap between characters' spoken lines
    ├── Grounding: profile-keyword coverage in the scene
    └── Persona alignment: outward vs inner-persona timing around the breakthrough slot
    │  (each also refined by the LLM judge below when one runs)
    │
    ▼
LLM Scoring (optional, if use_llm_scoring: true)
    ├── Format conversation (truncate to max_chars_for_scoring)
    ├── Send to scoring LLM with structured prompt
    ├── Parse JSON response → coherence, naturalness, engagement, consistency (+ the
    │   three roleplay dimensions when enabled)
    └── On failure → heuristic fallback estimates
    │
    ▼
Dropped by the judge?
    ├── Yes → DROP: no row written, no score stored, no review flag
    │   (invalid JSON and logic 0 are final; transient failures are retried
    │    judge_max_attempts times first)
    └── No  → continue
    │
    ▼
Composite Score = Σ (active dimension_score × weight) / Σ active weights
    │
    ▼
Below min_score_threshold?
    ├── Yes → Flag task_id in quality_review_ids, log WARNING
    └── No  → Store score only
    │
    ▼
Store in app_state.quality_scores[task_id]
Persist in generation_state.json
```

## Multi-Character Conversation Mode

When `num_characters` is set to a value greater than 1, the character engine selects multiple random character profiles and injects them all into the system prompt. Each character receives a distinct profile block with name, race, age, job, clothing, jewelry, appearance, backstory, personality (outward behavior), traits, setting, and optional class. As in single-character mode, each selected character's inner `persona` is withheld from the static profile and re-injected — as a numbered `CHARACTER N` line inside the same INNER PERSONA BREAKTHROUGH block — from `persona_breakthrough_turn` onward. The system prompt instructs the LLM to maintain all character personas throughout the conversation with distinct voices and personalities.

Characters with empty or whitespace-only names are automatically skipped. Ages outside the 18–60 range are auto-corrected. Character selection uses round-robin distribution across tasks for even coverage.

## Slop Fixing Flow

```
Slop Detected
    │
    ▼
Extract paragraph context around slop phrase
    │
    ▼
Call Slop Fixer LLM (API Slot 5) to rewrite paragraph
    │
    ├── Includes last 6 messages of conversation history for context
    │   (helps LLM understand topic and maintain coherence)
    │
    ├── Success → Check for incomplete quotes
    │       │
    │       ├── Quotes balanced → Replace in text
    │       └── Quotes broken → Skip replacement (preserve original)
    │
    └── Failure → Try next iteration with rotating fix instructions
            │
            ├── Max iterations reached → Fallback to system prompt fix
            └── All fixes exhausted → Accept with slop remaining
```

## Anti-Slop Fixing Flow

```
Anti-Slop Detected
    │
    ▼
Extract paragraph context around anti-slop phrase
    │
    ▼
Call Anti-Slop Fixer LLM (API Slot 6) to rewrite paragraph
    │
    ├── Includes last 6 messages of conversation history for context
    │   (helps LLM understand topic and maintain coherence)
    │
    ├── Success → Check for incomplete quotes
    │       │
    │       ├── Quotes balanced → Replace in text
    │       └── Quotes broken → Skip replacement (preserve original)
    │
    └── Failure → Try next iteration with rotating fix instructions
            │
            ├── Max iterations reached → Log warning
            └── All fixes exhausted → Accept with anti-slop remaining
```

## Slop → Anti-Slop Fallback

When `slop_to_anti_slop_fallback` is enabled and the Slop Fixer fails to fully resolve slop, the system makes a final attempt using the Anti-Slop Fixer LLM (API Slot 6):

```
Slop Fixer Failed to Fully Resolve Slop
    │
    ▼
Check for remaining slop phrases
    │
    ├── Remaining slop found
    │       │
    │       ▼
    │   Call Anti-Slop Fixer LLM (1 attempt)
    │       │
    │       ├── Success & quotes balanced → Replace in text
    │       ├── Success & quotes broken → Skip replacement
    │       └── Failure → Accept with slop remaining
    │
    └── No remaining slop → Slop was resolved during iterations
```

## Incomplete Quote Detection & Auto-Fix

When the LLM returns a response with unbalanced quotation marks and retries are exhausted, the text processing pipeline applies programmatic fixes:

1. **`repair_straight_quotes()`** — Applies structural heuristics for unbalanced straight quotes (odd count):
   - If text ends with sentence punctuation and doesn't start with a quote → prepend opening quote
   - If text starts with a capital word and doesn't end with a quote → append closing quote
   - Fallback: append at end
2. **`normalize_quotes()`** — Final normalization pass:
   - **Collapse runs** — Multiple consecutive quotes of the same type are collapsed to a single mark
   - **Curly quotes (`\u201c`, `\u201d`)** — If left and right counts don't match, the missing openers are prepended or closers are appended to balance them
   - **Straight quotes (`"`)** — Because the same glyph is used for both opening and closing, an odd count is ambiguous. The function deliberately **does not** guess where the missing quote belongs — this is handled by the detection and retry system and `repair_straight_quotes()` instead

This approach prevents the common bug where a trailing quote is force-appended to dialogue that intentionally lacks one (e.g., inch marks like `6"`).

## Circuit Breaker Flow

```
API Call Fails (HTTP error, timeout, or exception)
    │
    ▼
Increment failure counter for this API slot
    │
    ▼
Failure count >= max_consecutive_failures (5)?
    │
    ├── Yes → Open circuit (skip this API slot)
    │         Calculate exponential backoff: 60s → 120s → 240s → 480s → 600s (max)
    │         Log: "API Slot X circuit OPEN after 5 consecutive failures. Backoff: Xs"
    │         Wait for cooldown period
    │         Then: Close circuit, reset failure counter
    │         Log: "API Slot X circuit closed. Resuming requests with base cooldown."
    │
    └── No  → Continue using this API slot

On Success:
    Reset failure counter to 0
    Reset cooldown to base_cooldown_seconds (60s)
```

## Task Requeue on Host Failure

When an API host is down (circuit breaker open), tasks assigned to that slot are requeued rather than discarded:

```
Worker detects circuit is open for assigned API slot
    │
    ▼
Requeue task (up to MAX_TASK_REQUEUES = 50 times)
    │
    ├── Requeue successful → Back off, wait for circuit to close
    └── Max requeues exceeded → Discard task, log error
```

This ensures that temporary outages don't result in lost work — tasks are automatically retried when the host recovers.

## Budget Enforcement

```
After each task completes:
    │
    ▼
Calculate current cost = (input_tokens + output_tokens) × (cost_per_1k / 1000)
    │
    ▼
Is current_cost >= budget_limit?
    │
    ├── Yes → Log warning, set stop_processing = True, all workers exit
    └── No  → Continue generation
```

Budget is checked at the start of each worker loop iteration with thread-safe access to token counters, ensuring spending doesn't significantly exceed the limit.

## Malformed Response Detection

The application checks for malformed LLM responses using two configurable thresholds:

- **`max_newlines_malformed`** (32 in the shipped `config.yml`) — If a response contains more newlines than this threshold, it's considered malformed and the attempt is retried
- **`max_text_length_malformed`** (default: 5000) — If a response exceeds this character length, it's considered malformed and the attempt is retried

This catches cases where the LLM generates excessively long or poorly formatted output, such as dumping entire documents or generating repetitive text.
