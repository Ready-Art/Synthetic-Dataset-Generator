<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 🔑 Key Configuration Details

## Character Engine Injection Format

When characters are injected into the system prompt, they follow this format:

```
--- CHARACTER 1 PROFILE ---
Name: Melody
Gender: Female
Race: Human
Age: 59
Job: Horologist and Pawn Shop Manager
Clothing: Heavyweight flannel button-down, faded rust color...
Jewelry: Silver locket with faded photograph, simple copper wedding band
Appearance: Broad-shouldered and sturdy with a soft midsection...
Backstory: Estranged from religious parents, lives alone above the shop...
Personality (outward behavior): Stoic and introverted on the surface; curt with strangers, loyal with friends...
Traits: Meticulous, cynical, resilient, stubborn...
Setting: Cluttered repair counter in dimly lit pawn shop...
--- END CHARACTER 1 PROFILE ---
```

The `Name` field is conditionally included based on the `include_names_in_prompt` setting. The `Setting` field is only included when `setting_enabled` is true — and its value is resolved per scene: a character keeps its own non-empty `setting`, otherwise an empty one is filled from the `random_settings` pool (when enabled) or the default indoor environment, and multi-character ensembles are unified to a single shared setting (see `random_settings`). The `Class` field is only included when `class_enabled` is true. The `Personality` field is only included when it's not empty or "Unknown" (it describes outward behavior). **The `Persona` (inner personality) line is deliberately absent from this static block** — it is re-injected later as a separate breakthrough block (below) so early turns stay outward-only.

## Persona Breakthrough Injection (NEW in v10.0)

The inner `persona` is withheld on early assistant slots and only surfaces once the scene has run long enough to "break through." From `persona_breakthrough_turn` (default 2) onward, an INNER PERSONA BREAKTHROUGH block is appended to the scene prompt (and the profile's outward behaviour still governs how things are delivered):

```
--- INNER PERSONA BREAKTHROUGH ---
The conversation has been going long enough that the outward mask starts to slip. From this point on, let the character's private inner persona surface underneath their usual manner: their hidden hopes, fears, and vulnerabilities can now shape their words, but the outward behaviour described in the profile still governs how things are delivered. Do not announce the shift; let it show through in tone and word choice.
CHARACTER 1's inner persona (private self, surfacing now): Deep down she is a tender idealist... her sarcasm is armor over loneliness...
--- END INNER PERSONA BREAKTHROUGH ---
```

The block is sent with every reply from `persona_breakthrough_turn` onward (turn 2+ by default), where `persona_breakthrough_placement` says, and the character's outward profile stays constant throughout. With a single character the line reads "the character's inner persona…"; with several, each gets a numbered `CHARACTER N` line.

## Emotional State Injection

When emotional states are enabled, a state is sent with each reply, where `emotional_states.placement` says (by default after the history, at the end of the last user message). A **flat** state is constant across the whole conversation:

```
EMOTIONAL STATE: ANXIOUS
Express this emotional state throughout your responses. Use appropriate tone, word choice, and emotional expression that reflects ANXIOUS feelings.
```

An **arc** (an entry written as `state -> state -> ...`) is instead expanded across the conversation's assistant turns, one stage per turn, and injected with a progression header that tells the model the current stage and that earlier/later replies use the other stages:

```
EMOTIONAL STATE (PROGRESSION): CALM -> WARY -> TENSE
This conversation is a short emotional arc. In your current reply you are in the wary state; earlier replies showed the earlier states, later replies will show the later ones. Let the shift play out naturally through tone, word choice, and what you choose to hide or reveal rather than announcing it.
```

For a 3-stage arc over a 4-turn conversation the plan resolves to `[calm, wary, wary, tense]` (always starting on the first stage and ending on the last); outside opening mode a 1-turn task holds the start stage. In opening mode the greeting is one of these replies, so a 1-turn task gives the greeting the start stage and the answer the end stage, and a 2-turn task spreads the arc over three replies (greeting, then two answers): `[calm, wary, tense]`. A greeting taken from the profile's `greeting` field is used as written, so no request receives its stage; the answers still get the later stages. The metadata `emotional_state` field records the full configured entry (e.g. `"calm -> wary -> tense"` or `"anxious"`).

## Lore Injection

When lore is configured, it's injected after the system prompt with clear delimiters:

```
--- WORLD LORE ---
[Your lore text here]
--- END WORLD LORE ---
```

## Top-Level System Prompt

The top-level system prompt is prepended to all system prompts, separated by a double newline. This allows you to add universal instructions that apply to every conversation regardless of the base system prompt or variations.

## Question Generation Cache

When Valkey caching is enabled, question generation uses an MD5 hash of the full messages array (sorted by key) as the cache key. This means identical prompts will return cached responses, saving API calls. The cache key includes the API slot index, so different models can produce different cached responses for the same prompt.

## Per-API Debug Logs

> **Off by default since v13.1.1.** These traces are one file append per request, from every
> worker and every judge thread. That is pure diagnostic overhead on the request path, and nothing
> reads the files during a run, so they are written only when you ask for them: set
> `generation.debug_prompt_log: true` in `config.yml`, or tick **📝 Prompt Trace** in the Tools
> panel. Turn it on while debugging a prompt, not for a long run.

In duplication mode, each API slot writes to its own debug log file:
- `output/debug_prompt_api_slot_0.jsonl`
- `output/debug_prompt_api_slot_1.jsonl`
- etc.

In collaborative mode, a single debug log is used:
- `output/debug_prompt.jsonl`

Each debug log entry is a JSON object containing:
- `timestamp` — When the request was made
- `thread_id` — Which worker thread made the request
- `type` — Request type (`question_request`, `answer_request`, `user_continuation_request`, `slop_fix_request`, `anti_slop_request`, `quality_scoring_request`)
- `api_slot_idx` — Which API slot was used
- `attempt` — Retry attempt number
- `source_file` — Input file that generated this task
- `api_url` — The endpoint called
- `model` — The model name used
- `messages` — The full messages array sent to the API
- `payload_dict` — The complete API payload including sampler settings

## Quality Score Storage

Quality scores are stored in-memory in `app_state.quality_scores` (a dict keyed by task_id) and persisted in `generation_state.json` for crash recovery. Each entry contains:

```json
{
  "composite": 87.3,
  "dimensions": {
    "coherence": 90,
    "naturalness": 85,
    "engagement": 82,
    "diversity": 95,
    "consistency": 88,
    "technical": 92,
    "voice_distinctiveness": 85,
    "grounding": 90,
    "persona_alignment": 60
  },
  "flags": [],
  "scored_at": "2025-06-15 14:32:00",
  "method": "hybrid"
}
```

The `method` field indicates which scoring path was used: `"heuristic_only"`, `"hybrid"`, or `"none"`. The three roleplay dimensions appear only when `quality.roleplay_dimensions` is on; stored entries predate the toggle and simply lack those keys (the dashboard shows `N/A` for them).
