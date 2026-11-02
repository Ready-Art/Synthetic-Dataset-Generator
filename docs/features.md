<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# ✨ Features

## Core Generation
- **Multi-API Orchestration** — Configure up to 6 API slots (4 generation, 1 slop fixer, 1 anti-slop fixer) with independent endpoints, models, keys, and rate limits
- **Per-API Thread Configuration** — Each API slot has its own configurable thread count for fine-grained throughput control
- **Per-API Rate Limiting** — Configurable requests-per-minute (RPM) for each API slot with automatic wait and real-time color-coded status indicators (green/orange/red)
- **Master Duplication Mode** — Generate the same conversation across multiple APIs simultaneously for dataset diversity
- **Collaborative Mode** — Distribute tasks across enabled APIs for higher throughput
- **Multi-Turn Conversations** — Generate conversations with configurable turn counts (Q/A pairs)
- **Randomized Chunking** — Automatically extracts random subject/context chunks from input text files
- **Questions File Mode** — Use a predefined list of questions instead of LLM-generated ones

## Quality Scoring (NEW in v9.4.0)
- **Hybrid Scoring Engine** — Scores every completed conversation across seven core dimensions (coherence, naturalness, engagement, diversity, consistency, logic, technical) using a fast heuristic pass plus optional LLM-based evaluation, and optionally two further opt-in dimension groups behind GUI toggles: three roleplay-specific dimensions (voice distinctiveness, grounding, persona alignment) and the v11.7 tic-density dimension
- **Composite Score (0–100)** — Weighted combination of all active dimension scores; weights configurable per dimension
- **Heuristic Dimensions** — Diversity (type-token ratio, sentence length variance, trigram repetition, paragraph structure) and Technical (quote balance, stray asterisks, em dashes, whitespace, all-lowercase detection) run without any API call
- **Optional LLM Scoring** — Coherence, naturalness, engagement, and consistency can be scored by a dedicated LLM (separate API endpoint) for deeper evaluation; falls back to heuristic estimates if the LLM call fails
- **Threshold Flagging** — Conversations scoring below a configurable minimum threshold are automatically flagged for human review
- **Quality Review Tab** — Dedicated dashboard tab listing all flagged conversations with score, lowest dimension, flags, source file, and timestamp; supports filtering, single-click transcript view, export to JSONL, and bulk dismiss
- **Quality Metrics Bar** — Real-time average composite score (color-coded green/amber/red), count of below-threshold conversations, and most frequent flag displayed in the main metrics area
- **Quality Score Table** — Dashboard tab showing the latest 50 scored conversations with per-dimension breakdown and method used
- **Output Filtering** — Optional flag to mark below-threshold conversations for post-processing (score metadata attached to exported JSONL)
- **vLLM Judge Priority** (v11.8) — Optional `priority` field (**-10 to 10**, lower = earlier; **-10 is highest**, 0 the default, **10 lowest**) on every judge request for vLLM's priority scheduling, so the judge can be parked behind the generation traffic or pushed ahead of it. Off by default, because backends without priority support reject the unknown field and vLLM rejects a non-zero priority when the served model is not priority-scheduled
- **Quality State Persistence** — Scores and review flags are saved in the generation state file for crash recovery

## Quality Control & Detection
- **Refusal Detection** — Automatically detects and retries LLM refusals with configurable jailbreak prompts
- **User Speaking Detection** — Detects when the assistant impersonates the user, with gender-specific phrase lists
- **Slop Detection** — Identifies undesirable phrases/patterns in generated text
- **Anti-Slop Detection** — Independent secondary detection layer with its own API slot (6), dedicated sampler settings, and LLM rewriting
- **Incomplete Quote Detection** — Catches unbalanced quotation marks (both straight `"` and curly `""` quotes) with programmatic auto-fix fallback including structural heuristic repair
- **Sentence-Level Slop Fixing** — Dedicated LLM (API Slot 5) rewrites problematic sentences while preserving paragraph context and balanced quotes
- **Anti-Slop Fixing** — Dedicated LLM (API Slot 6) for anti-slop phrase rewriting with paragraph-level context awareness and rotating fix instructions
- **Slop → Anti-Slop Fallback** — When the Slop Fixer fails to fully resolve slop, the Anti-Slop Fixer LLM is used as a final attempt before accepting the output
- **Conversation History for Fixers** — Slop and anti-slop fixer LLMs can optionally receive the full conversation context (last 6 messages) for more coherent rephrasing (configurable via `generation.use_conversation_history_for_fixers`)
- **Rotating Fix Instructions** — Cycle through multiple fix strategies for stubborn issues
- **Malformed Response Detection** — Automatically rejects responses with excessive newlines or length beyond configurable thresholds (`max_newlines_malformed`, `max_text_length_malformed`)
- **Non-Latin Script Check** (v11.10.0) — Flags and regenerates replies containing stray characters from another script (Chinese, Japanese, Korean, Cyrillic, Greek, Arabic, Hebrew, Devanagari, Thai, emoji). The check is a **Unicode range allow-list**, so it needs no LLM call, no phrase list and no network request

## Character & Persona Engine
- **Multi-Character Conversations** — Inject multiple character profiles into a single conversation for rich, multi-party dialogues (1–10 characters per conversation)
- **Scene Cast (v12.0)** — Characters can leave and join a multi-character scene. The *generator* owns the schedule (who is present at each reply) and the model only narrates the transitions, so the cast never drifts: a scheduled exit the model never writes is **cancelled** (the character stays legitimately present) rather than assumed, and a character who has left **cannot speak again** (a code-only continuity check regenerates the reply). Off by default, and only active with 2+ characters
- **Character Profiles** — Randomly inject character names, ages (validated 18–60), races, jobs, clothing, jewelry, appearance, backstories, personalities, traits, and settings into system prompts
- **Card-Based Character Editor** — Visual card layout in the Configuration Editor with individual add/remove per character
- **Name Validation** — Characters with empty or whitespace-only names are automatically skipped with a warning
- **Class Selection** — Optionally assign fantasy classes (mage, warlock, rogue, etc.) to characters
- **Setting Selection** — Optionally assign custom locations/environments to each character
- **Random Settings Pool (NEW)** — A configurable pool of locations (`prompts.random_settings.settings[]`, analogous to `random_names`). When `setting_enabled` is true, a character whose own `setting` is empty is given a setting sampled from the pool instead of the default indoor environment; multi-character scenes are additionally unified to one shared location pulled from the ensemble, so all characters inhabit the same place (feeds consistency, prevents cross-talk)
- **Emotional States** — Assign random emotional states (happy, sad, angry, etc.) that influence response tone
- **Emotional Arcs (NEW in v10.0)** — A state can be a *progression* (`calm -> wary -> tense`) instead of a single mood. Arcs are expanded across the conversation's assistant turns, so the emotional register shifts over time (start composed, surface anger near the end) rather than staying flat — much richer training signal for multi-turn scenes
- **User-Side Emotional States & Arcs (NEW in v10.6.0, optional)** — Give the *simulated user* their own emotional register. The same flat-state / `->`-arc format is steered onto the user's side of the conversation (the generated opening question/first line and each user continuation) instead of the character's replies. Off by default; enable with `prompts.user_emotional_states.enabled`. The note is ephemeral (appended to the user prompt only, never stored in history or the system prompt), and the assigned label is recorded in the record metadata as `user_emotional_state`

- **Persona Breakthrough (NEW in v10.0)** — The inner `persona` is deliberately kept out of early turns and re-injected as an *INNER PERSONA BREAKTHROUGH* block from a configurable slot (`prompts.character.persona_breakthrough_turn`, default 2). Slot 1 stays outward-behaviour-only, mirroring how real character cards hide the private self until the scene breaks through
- **Variable System Prompts** — Randomly select from a list of system prompt variations per conversation
- **Top-Level System Prompt** — Prepend a universal instruction to all system prompts across all conversations
- **Lore Injection** — Inject world lore/background information into every system prompt

## Text Post-Processing
- Remove thinking blocks from reasoning models
- Remove em dashes (—)
- Remove excessive asterisks (`**`, `****`, etc.)
- Remove `* *` patterns
- Remove all asterisks
- Ensure spaces after line breaks
- Strip markdown formatting to plain text
- **Collapse a word the model repeated back-to-back** (v12.7.3, always on) — a stalled reply can emit the same word two or more times in a row: `Serenity. Serenity. Then she left.`, `faster, faster, faster.`, or `word word word`. No phrase list can express "the same word twice", so this is deterministic cleanup rather than another retry: the extra copies are removed and the first one is kept with its original casing. Ordinary repeated words are deliberately **left alone**, because `no no no`, `yes yes yes`, `very very good`, `ha ha ha` and `wait wait wait` are correct English emphasis — the rule is written as an exclusion list of ordinary words rather than an inclusion list of words to rewrite, so an unusual word the model stalls on is still caught without having been enumerated in advance. There is no setting: a stalled word is a property of the reply, not of a prompt, so a default run gains the cleanup without any config change
- **Experiment: Retry missing quotations** (v11.7.1, contraction-safe since v11.7.2) — when on, a generated reply whose spoken line came back with NO quotation marks at all (`Its funny she says` instead of `"Its funny," she says`) is sent back to the SAME main API for one retry before the turn is kept. Apostrophes inside words (`don't`, `Melody's`) are contractions, not quotation marks, so they no longer disable the check
- Normalize and balance quotation marks (handles both straight `"` and curly `""` quotes, with special handling to avoid breaking inch marks and intentional unquoted dialogue)
- **Structural Quote Repair** — `repair_straight_quotes()` applies heuristic-based fixes for unbalanced straight quotes (prepends opener or appends closer based on text structure) before the final normalization pass

## Output Metadata (NEW in v9.9.0)
- **Provenance per Record** — Every output row can carry a flat, schema-stable `metadata` block recording which character(s), emotional state, and setting produced it, plus its quality score, so datasets are self-describing and filterable after generation
- **Trainer-Safe by Design** — The block lives at the record level (a sibling of `id` and `conversations`), never inside `conversations[]`, and uses only flat scalars (`str`/`int`/`float`/`bool`, never `null`) with identical keys and types on every row, so Hugging Face `load_dataset` and Axolotl's template mapping are unaffected
- **Quality Scores Inlined** — Quality scoring runs before the write, so each record's `quality_composite` / `quality_*` dimension fields describe that exact conversation (per-turn rows in Duplication Mode carry `duplication_turn` for identification)
- **Filterable Post-Generation** — Use the metadata columns to filter datasets in the training repo without regenerating (e.g. keep only rows with `quality_composite >= 80`)
- **Toggleable & Back-Compatible** — `generation.include_metadata` (default `true`); set to `false` to restore the legacy two-field records exactly, byte-for-byte

## Infrastructure & UI
- **Budget & Cost Control** — Set a spending limit per run; generation automatically stops when the budget is reached, with real-time cost tracking
- **Circuit Breaker** — Automatically disables API slots after consecutive failures with exponential backoff cooldown (60s → 120s → 240s → 480s → 600s max) and re-enables after cooldown
- **Task Requeue on Host Failure** — Tasks assigned to a downed API host are automatically requeued (up to 50 times) rather than discarded, ensuring no work is lost during outages
- **Valkey/Redis Caching** — Cache LLM responses (1-hour TTL, MD5-keyed per API slot) to avoid redundant API calls
- **PostgreSQL Storage** — Optional database backend with connection pooling, automatic initialization at startup, and JSONL export
- **Crash Recovery** — Automatic state saving/loading with configuration change detection and incompatibility warnings (now includes quality scores)
- **Configuration Profiles** — Save, load, and delete named configuration profiles
- **Configuration Editor Search** — Search bar for quickly finding tabs and LabelFrame sections by name
- **API Compatibility Profiles** — Per-slot payload profiles (OpenAI, Mistral, xAI, Gemini, Anthropic-compat, or user-defined) that trim the request body to what each endpoint accepts, so hosted APIs don't 400/422 on sampler params they don't support; profiles are editable data in `config/api_profiles.yml`, with URL-based auto-detection
- **API Connection Testing** — Test API connectivity directly from the configuration editor
- **Valkey Connection Testing** — Test Valkey/Redis connectivity from the main UI with detailed success/failure feedback
- **Real-Time Dashboard** — Monitor refusals, slop, errors, quality scores, and API response times with time-series graphs, search, and copy functionality
- **Queue Management** — View pending, completed, and failed tasks; purge pending tasks, retry failed tasks, and export queue state to CSV
- **Live Prompt Preview** — View prompts being sent to APIs in real-time with role-based color coding, search & highlight, section copy, and auto-scroll toggle
- **Toast Notifications** — Non-blocking, auto-dismissing notification popups for status updates (success, error, warning, info)
- **Token & Cost Tracking** — Track input/output tokens and estimate API costs with budget enforcement
- **Debug Logging Toggle** — Enable/disable verbose debug logging from the UI with a single checkbox
- **Adaptive GUI Updates** — Dashboard refreshes faster (500ms) during active generation and slower (2s) when idle
- **Per-API Debug Logs** — Separate debug log files per API slot in duplication mode for easier troubleshooting (now includes quality scoring requests)
- **Animated Progress Bars** — Color-coded progress bars that change style based on completion percentage (blue → cyan → green → amber → bright green) with pulse animations at milestones
- **Stop & Clear Job** — Stop the current generation job, clear all progress, and reset for a fresh start
- **Force Recovery** — Bypass configuration compatibility checks and force-load a previous generation state
- **Clear Database** — Clear the PostgreSQL `generated_conversations` table from the UI with confirmation dialog
- **Export DB → JSONL** — Export stored conversations from PostgreSQL to a JSONL file (runs in background thread)
- **Rate Limit Status Display** — Real-time per-API rate limit usage indicators with color-coded status (green/orange/red)
- **API Response Times** — Average, min, max response times and sample count per API slot displayed in the UI
- **Thread Status Display** — Shows spawned and active thread counts in the metrics bar
- **Dashboard Search** — Case-insensitive search across all issue panels within a dashboard tab with auto-scroll to first match
- **Dashboard Copy All** — Copy all issue text from a dashboard tab to clipboard
- **Clear Dashboard** — Reset all recent issue lists and graph data with a single button
- **ttkbootstrap "superhero" Theme** — Modern dark-themed UI with custom styling for all widgets
