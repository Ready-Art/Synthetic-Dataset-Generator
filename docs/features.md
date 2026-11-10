<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
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
- **Hybrid Scoring Engine** — Scores every completed conversation across seven core dimensions (coherence, naturalness, engagement, diversity, consistency, logic, technical) using a fast heuristic pass plus optional LLM-based evaluation, and optionally three further opt-in dimension groups behind GUI toggles: three roleplay-specific dimensions (voice distinctiveness, grounding, persona alignment), the v11.7 tic-density dimension, and the v12.0 cast-continuity dimension
- **Composite Score (0–100)** — Weighted combination of all active dimension scores; weights configurable per dimension
- **Heuristic Dimensions** — Diversity (type-token ratio, sentence length variance, trigram repetition, paragraph structure) and Technical (quote balance, stray asterisks, em dashes, whitespace, all-lowercase detection) run without any API call
- **Optional LLM Scoring** — Coherence, naturalness, engagement, and consistency can be scored by a dedicated LLM (separate API endpoint) for deeper evaluation; falls back to heuristic estimates if the LLM call fails
- **Threshold Flagging** — Conversations scoring below a configurable minimum threshold are automatically flagged for human review
- **Quality Review screen** — Dedicated screen listing all flagged conversations with score, lowest dimension, flags, source file, and timestamp; supports filtering, single-click transcript view, export to JSONL, bulk dismiss, and (v12.8.2) **dropping the selected conversation from the dataset** — the action for flagged output that is simply bad data. A drop removes only the selected task's rows (output file, per-API duplication rows and database row) after a confirmation, and never falls back to the whole set
- **Quality Metrics Bar** — Real-time average composite score (color-coded green/amber/red), count of below-threshold conversations, and most frequent flag displayed in the main metrics area
- **Quality Score Table** — The Quality screen shows the latest 50 scored conversations with per-dimension breakdown and method used
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

> **Note (v12.7.6):** Slop Detection, Anti-Slop Detection, Sentence-Level Slop Fixing, Anti-Slop Fixing and the Slop → Anti-Slop Fallback are all **opt-in and disabled by default** (`detection.slop.enabled` / `detection.anti_slop.enabled` ship `false`). They still work when you turn them on, but the recommended tool for slop/anti-slop work is [AntiSlop-vLLM](https://github.com/Ready-Art/antislop-vllm-fork/tree/dev) — the `dev` branch, not `main` — which bans the token before it is emitted instead of rewriting afterwards. See [Slop & Anti-Slop](slop-and-antislop.md).

- **Conversation History for Fixers** — Slop and anti-slop fixer LLMs can optionally receive the full conversation context (last 6 messages) for more coherent rephrasing (configurable via `generation.use_conversation_history_for_fixers`)
- **Rotating Fix Instructions** — Cycle through multiple fix strategies for stubborn issues
- **Malformed Response Detection** — Automatically rejects responses with excessive newlines or length beyond configurable thresholds (`max_newlines_malformed`, `max_text_length_malformed`)
- **Non-Latin Script Check** (v11.10.0) — Flags and regenerates replies containing stray characters from another script (Chinese, Japanese, Korean, Cyrillic, Greek, Arabic, Hebrew, Devanagari, Thai, emoji). The check is a **Unicode range allow-list**, so it needs no LLM call, no phrase list and no network request

## Character & Persona Engine
- **Multi-Character Conversations** — Inject multiple character profiles into a single conversation for rich, multi-party dialogues (1–10 characters per conversation)
- **Scene Cast (v12.0)** — Characters can leave and join a multi-character scene. The *generator* owns the schedule (who is present at each reply) and the model only narrates the transitions, so the cast never drifts: a scheduled exit the model never writes is **cancelled** (the character stays legitimately present) rather than assumed, and a character who has left **cannot speak again** (a code-only continuity check regenerates the reply). Off by default, and only active with 2+ characters. **v13.5.1 makes the scene coherent end to end:** the cast note reaches the user's own messages too (his opening message gets the slot-1 note, so he cannot touch a character who has not arrived yet, and his later messages get the note for the next reply, so he cannot address someone who has left); a model that echoes the note back as the first line of a reply has it stripped, because the echo is stale and would repeat the same `NAME: LEAVES` on every later turn; the `{style}` opener now applies to the first assistant reply only; and the exit and arrival beats are varied per task instead of being identical in every row
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

## Optional Cleaner Pipeline (NEW in v12.8.0)
- **A separate, standalone post-processor** — `tools/cleaner_pipeline/` is a nine-stage JSONL cleaner you run by hand on a *finished* `output/*.jsonl`: quote guard, strip chat-template sentinels, strip control/invisible characters, strip double-encoded text and markup, normalise whitespace, drop foreign-script records, an independent verifier, repair truncated turns, and drop records with unbalanced quotes. It writes `build/09_final.jsonl` plus a report for every record it changed or dropped
- **Not part of the application** — nothing imports it, no config key controls it, no test runs it, and it adds no dependency. The per-turn cleanup above and the detection layer are unchanged and independent of it
- **It is a starting point, not a product** — its rules were reverse-engineered from one corpus's defects and step 01 assumes every turn contains quoted dialogue, so it can drop an entire prose dataset. Read [Cleaner Pipeline](cleaner-pipeline.md) before you run it

## Dataset Sources (NEW in v14.4.0)
- **Bind a dataset instead of `input/*.txt`** — A run can draw its prompt variables from a real dataset rather than from text files. Seven source kinds: `csv`, `tsv`, `json`, `jsonl`, `parquet` (via optional `pyarrow`), `huggingface` (Hub) and `hf_disk` (a saved `DatasetDict`). Install `requirements-datasets.txt` for the last three
- **`{{ds:alias}}` placeholders** — Map a column to a bare alias and write it into any prompt. The alias is what the prompt sees, so renaming a column upstream is one edit in the Datasets screen, not a prompt rewrite
- **Names only, never values** — `datasets.sources[]` holds selectors, column names and aliases. Record values and credentials never go into config, and the Datasets screen never shows a cell value. That is what makes the config safe to commit
- **One row frozen per task** — Each task gets its own immutable binding, so a run cannot drift mid-flight when the source file changes underneath it. Exhaustion and invalid data are reported, never silently cycled or skipped
- **Column visibility** — `visible_columns` narrows which columns the picker offers: `null` means all (including columns imported later), `[]` means none, and a list is honoured in schema order
- **Import columns** — Read a source's column names in a background thread and cache them, with a schema fingerprint so a changed file is detected rather than trusted
- **Datasets screen** — A two-pane list and detail, the alias editor, column visibility and import, all in the web UI. See [Dataset bindings](dataset-bindings.md)

## Output Metadata (NEW in v9.9.0)
- **Provenance per Record** — Every output row can carry a flat, schema-stable `metadata` block recording which character(s), emotional state, and setting produced it, plus its quality score, so datasets are self-describing and filterable after generation
- **Trainer-Safe by Design** — The block lives at the record level (a sibling of `id` and `conversations`), never inside `conversations[]`, and uses only flat scalars (`str`/`int`/`float`/`bool`, never `null`) with identical keys and types on every row, so Hugging Face `load_dataset` and Axolotl's template mapping are unaffected
- **Quality Scores Inlined** — Quality scoring runs before the write, so each record's `quality_composite` / `quality_*` dimension fields describe that exact conversation (per-turn rows in Duplication Mode carry `duplication_turn` for identification)
- **Filterable Post-Generation** — Use the metadata columns to filter datasets in the training repo without regenerating (e.g. keep only rows with `quality_composite >= 80`)
- **Toggleable & Back-Compatible** — `generation.include_metadata` (default `true`); set to `false` to restore the legacy two-field records exactly, byte-for-byte

See [Infrastructure & UI](features-ui.md) for the dashboard, the queue, the
cost controls and the web front end's screens.
