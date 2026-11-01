<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 🧑 Prompts & Character Engine

```yaml
prompts:
  use_questions_file: false       # Use input/questions.txt instead of LLM-generated questions
  question: "Write the user's first message in a roleplay drawn from the passage below. Write only the message itself, as the user, not as any other character.\n\n{context}"
  answer: "Write your next message in the roleplay. Write only the message itself."
  user_continuation_prompt: "Write the user's next message. Write only as the user, not as any other character."
  opening_mode: false             # Start each sample with the character's greeting (see Opening Mode below)
  greeting: "Write the opening message of this roleplay as {speaker}, ...\n{context}"  # Opening mode's greeting prompt
  greeting_context: "Source material for the setting. Take from it only the kind of place, the objects, the weather and the mood.\n---\n{chunk}\n---\nNone of the source's people, names, ships, places or events exist in this scene: ..."  # Opening mode: the greeting's {context} when there is a book chunk
  first_line: "You are the user in a roleplay chat, not any of the characters in it. ... Write only your own next message: {style}. ..."  # Opening mode's first-line prompt
  first_line_styles:              # Opening mode: one is picked at random for the first-line prompt's {style}
    - "an action"
    - "a line of dialogue"
    - "an action with a line of dialogue"
    - "a short reply"
    - "a question to the character"
  question: "Generate a question based on the provided text. Recent questions to avoid: {recent_questions}\n\nSubject: {subject}\n\nContext: {context}"
  answer: "Provide an answer to the last question."
  user_continuation_prompt: "Continue the conversation naturally based on the assistant's last response: {last_assistant_message}"

  system:
    top_level_system_prompt: ""   # Prepended to ALL system prompts
    base: "You are an excellent role-player."
    variable: false               # Enable random system prompt selection
    variations: []                # One per line; used when variable: true

  lore: ""                        # World lore injected into every system prompt

  character:
    enabled: true
    include_names_in_prompt: true  # Include character names in the prompt
    class_enabled: false           # Enable fantasy class selection
    setting_enabled: false         # Enable custom setting/location per character
    num_characters: 1              # Number of characters per conversation (1-10)
    # Persona Breakthrough (NEW in v10.0): which assistant slot (1 = first reply) the inner persona
    # starts being injected from. Slot 1 is always outward-behaviour-only; the persona 'breaks
    # through' from this slot onward. Set 1 to expose the persona from the very first reply.
    persona_breakthrough_turn: 2
    # Where the breakthrough block is sent: depth0 (end of the reply's last user message, after the
    # history; keeps the prefix cache) or system (end of that reply's system prompt, as in v10).
    persona_breakthrough_placement: depth0
    characters:                    # List-of-dicts format (recommended)
      - name: "Melody"
        age: "59"
        gender: "Female"
        race: "Human"
        job: "Horologist and Pawn Shop Manager"
        clothing: "Heavyweight flannel button-down, faded rust color, straight-leg cargo pants dark olive green"
        jewelry: "Silver locket with faded photograph, simple copper wedding band"
        appearance: "Broad-shouldered and sturdy with a soft midsection, weathered olive skin, steel-gray and dark brown hair in choppy jaw-length bob"
        backstory: "Estranged from religious parents, lives alone above the shop, drives a 2004 Subaru Outback, goal is to restore an 18th-century marine chronometer"
        persona: "Deep down she is a tender idealist who fell in love with timepieces as a child and still believes anything truly broken can be made to run again - clocks and people alike. Her sarcasm is armor over a lonely, vulnerable core; the longer a scene stays tense the more that mask slips, and under real pressure she snaps before she catches it"
        personality: "Stoic and introverted on the surface, curt and defensively sarcastic with strangers but patient once trust is earned; keeps her real feelings tightly inside and copes by disassembling cheap watches into tiny glass jars"
        traits: "Meticulous, cynical, resilient, stubborn; taps thumb against index finger rhythmically when thinking, refuses hot beverages without lids"
        setting: "Cluttered repair counter in dimly lit pawn shop, 6:45 PM Tuesday, heavy rain lashing windows, smells of brass polish and damp wool"
        class: ""
        greeting: ""                 # Optional: opening mode uses it as the first message
      - name: "Bob"
        age: "150"
        gender: "Male"
        race: "Elf"
        job: "Teacher"
        clothing: "Business suit"
        appearance: "Short with glasses"
        backstory: "Traveled the world"
        persona: "Beneath the calm exterior he is restless and afraid of being forgotten, and his kindness is a way to earn one more chance at belonging"
        personality: "Patient and wise"
        traits: "Diplomatic, well-read"
        setting: "A bustling marketplace"
        class: "rogue"

  emotional_states:
    enabled: true
    # Each entry is either a flat state (constant across the whole conversation) or an arc using
    # '->' between stages. Arcs are expanded across the conversation's assistant turns, so the
    # emotional register shifts over time rather than being one static injection.
    placement: depth0              # depth0 or system, as for persona_breakthrough_placement
    states:
      - "happy"
      - "sad"
      - "anxious"
      # Arcs (NEW in v10.0): a start -> ... -> end progression, one stage per assistant turn
      - "calm -> wary -> tense"
      - "composed -> guarded -> snapped"

  user_emotional_states:         # (NEW in v10.6.0) Emotional state / arc for the SIMULATED USER.
    enabled: false               # Optional and off by default. Same flat-state / '->' arc format as
                                 # emotional_states, but applied to the user's messages (the generated
                                 # opening question/first line and each user continuation), not to the
                                 # character's replies. The note is appended to the ephemeral user prompt
                                 # only -- never stored in the conversation history or the system prompt.
    states: []
      # Examples (uncomment / edit to enable):
      # - "curious"
      # - "defensive -> softening -> open"

  random_names:
    enabled: false              # Enable random name assignment (overrides character Name field)
    names:                      # List of names to randomly select from when enabled
      - "Alice"
      - "Bob"
      - "Charlie"
      - "Diana"
      - "Eve"

  random_settings:              # Pool of settings, analogous to random_names
    enabled: false              # When setting_enabled is true and a character's own setting is empty,
                                # sample one from this list instead of the default indoor environment
    settings:                   # One per line; also used as the candidate pool when unifying multi-character scenes
      - "A cozy coffee shop in the old town"
      - "A bustling city street at rush hour"
      - "A late-night diner by the train station"
      - "A quiet park bench in autumn"
```

> **Note:** `random_settings` only takes effect when `character.setting_enabled` is `true`. It fills *empty* per-character `setting` fields and is the candidate pool used to unify a multi-character ensemble to a single shared location. A character that already has its own non-empty `setting` keeps it.

**Character Configuration Notes:**

- **New format (recommended):** Use the `characters` list with dicts containing `name`, `age`, `gender`, `race`, `job`, `clothing`, `jewelry`, `appearance`, `backstory`, `persona`, `personality`, `traits`, `setting`, and `class` fields. This allows full control over each character's attributes.
- **Legacy format (backward compatible):** Separate lists (`name: [...]`, `race: [...]`, etc.) are automatically converted to the new format at runtime.
- **`num_characters`:** Controls how many characters are randomly selected and injected into each conversation's system prompt. Set to 1 for single-character conversations, or higher (up to 10) for multi-character dialogues.
- **`max_character_cards`:** Limits the number of character profile cards displayed in the Configuration Editor UI (default: 10, max: 100).
- **`user_emotional_states`:** (NEW in v10.6.0) An **optional** emotional system for the *simulated user's side* of the conversation. It reuses the same flat-state / `->`-arc format as `emotional_states`, but one entry is randomly assigned per task and expanded across the user's turns instead of the character's replies: user turn 1 is the generated opening question (or first line in opening mode), and each later user continuation advances the arc one stage. The note is appended to that request's ephemeral user prompt only — it is never stored in the conversation history and never sent in the system prompt, so it does not affect prefix-cache reuse. `enabled` defaults to `false` (the feature is off and no user note is ever added); the Configuration Editor's Character Engine tab has its own "Enable User Emotional States" checkbox and text area. The assigned label is recorded on the record metadata as `user_emotional_state`. There is no placement option: because the note rides on the user prompt, `depth0`/`system` do not apply.
- **`persona_breakthrough_placement` and `emotional_states.placement`:** Where each reply's INNER PERSONA BREAKTHROUGH block and emotional state are sent, set separately. `depth0` (the default, and the value when the key is missing) appends the block to the end of the reply's last user message, after the conversation history: the system prompt never changes, so the server reads the history from its prefix cache. `system` appends it to the end of that reply's system prompt, as v10 did, and the user continuation after the reply shares that system prompt, so the simulated user sees the block too: whenever the block changes (the persona appearing, an arc moving to its next stage) the system prompt changes and the server computes the whole history again. A flat state, or a persona shown from the first reply, keeps the system prompt fixed and costs no cache. The generated question gets neither block in either placement. When a `system` placement changes the system prompt between replies, the editor and the start of a run show a warning with the estimated share of history computed again. The estimate counts the replies where the system prompt changes, weighted by how much history each one carries (a reply k carries about k−1 exchanges), averaged over the configured states. It is a model of reuse: on a llama-server replay (Gemma 4 E2B, `-np 2 --cache-ram 0`, 4 turns, persona from reply 2 and a 3-stage arc) it estimates 67% and the server computed 67% of the history again. Measured on llama-server with Gemma 4 26B-A4B at 4 turns, both blocks on `system` read 35% of the answer calls' prompt tokens from the cache against 77% on `depth0`, and spent about 1.6× as long on prefill. The same runs found output differences: with `depth0`, fewer replies were judged at their planned arc stage (99 of 160 against 114 of 160), mostly reply 1 reading later than planned on one arc.- **`persona`:** Optional field describing the character's **inner personality** — the private self underneath the surface: hidden fears, motivations, private grief, contradictions, and what happens under real pressure. The persona line is **not** part of the static profile block: it is withheld on early turns and re-injected as an INNER PERSONA BREAKTHROUGH block from `persona_breakthrough_turn` onward, so the model portrays an authentic interior life only once the scene "breaks through".
- **`personality`:** Optional field describing the character's **outward behavior** — the personality people actually observe: how they speak, act, cope, and treat others. Always injected into the profile block as `Personality (outward behavior):`. Keep it distinct from `persona` (the inner self); together the two give the LLM both the mask and the face beneath it, with the mask shown first and the face revealed later.
- **`traits`:** Optional field that adds character traits (e.g., "quick-witted, detail-oriented") to the character's system prompt injection.
- **`age`:** Validated to be between 18 and 60 in the configuration editor. Invalid ages are auto-corrected at runtime.
- **`name`:** Characters with empty or whitespace-only names are automatically skipped with a warning.
- **`greeting`:** Optional. In opening mode, when a sample has this character alone, the greeting is used as-is as the first message: no greeting request, no checks, and no book chunk. Quotes and markup in it are kept. With random names on it is not used, since it names the profile's own character; the greeting is generated under the random name instead.
- **`random_names`:** Optional feature that overrides character names with randomly selected names from a configured list. When `prompts.random_names.enabled` is true, each character's name in the prompt is replaced with a random name from `prompts.random_names.names[]`. Names are selected independently for each character (not in order), allowing for varied name assignments across conversations. This is useful for generating diverse datasets without manually creating many character profiles.
- **`random_settings`:** Optional feature (analogous to `random_names`) that adds location diversity to roleplay data. It only applies when `character.setting_enabled` is `true`. For each selected character whose own `setting` field is empty, a setting is sampled from `prompts.random_settings.settings[]` (when `enabled`) instead of falling back to the default `"A standard indoor environment."`. A character with its own non-empty `setting` always keeps it. Additionally, when multiple characters are selected for one scene, their settings are **unified to a single shared setting** drawn from the ensemble (the distinct settings they would otherwise carry) so every character inhabits the same location — this feeds the consistency dimension and prevents cross-talk between characters set in different places.

**Template Variables** (each is optional; the shipped question prompt uses only `{context}`):
- `{recent_questions}` — Recent question history to avoid repetition
- `{subject}` — The subject text chunk
- `{context}` — The broader context surrounding the subject (in the greeting prompt: `greeting_context` with the book chunk in its `{chunk}`, or empty when the task has no book; the shipped `greeting_context` tells the model to take only the kind of place, objects, weather and mood from the chunk and none of its people, names or events)
- `{last_assistant_message}` — The assistant's previous response (for continuations)
- `{speaker}` — Greeting prompt: who the greeting is written as (see below)
- `{chunk}` — Greeting context: the book chunk
- `{style}` — First-line prompt: one of `first_line_styles`, picked at random

**Opening Mode:** With `opening_mode: true`, each sample opens the way a SillyTavern chat does, with the character's greeting:

1. **Greeting.** The scene prompt (system prompt, lore, character profiles) is sent with the `greeting` prompt, which gets the book chunk in `{context}` when the run has input files and is followed by the first reply's note (its emotional state, and the inner persona when `persona_breakthrough_turn` is 1); a block on `system` placement goes at the end of the greeting's system prompt instead. It uses `max_tokens_answer` and is retried on refusal, slop, the configured user-speaking phrases (unless `no_user_impersonation` is on), a short reply or a malformed one. `{speaker}` is the character's name with one character (or "the character described above" when `include_names_in_prompt` is false), "the narrator, bringing ... into the scene" with several, and "the character you are playing" when no profile is in the prompt.
2. **First user line.** The `first_line` prompt is the system message and the greeting is sent as the other party's turn, without the scene prompt, so the line is written from the user's seat. It uses `max_tokens_user_reply` and is retried on refusal, slop, a short reply or a malformed one.
3. **Replies and later turns.** The existing answer and continuation path, with every check and fixer, starting from the greeting and first line. Later user turns still come from `user_continuation_prompt`. `num_turns` counts replies; the greeting does not count.

Neither new request uses the Valkey response cache, so a retry gets a new reply. A sample costs one request more than in chunk mode. Without input files, a run queues `num_random_chunks` opening tasks (12000 by default); with input files, opening tasks are book chunks, with IDs starting `opening_` so finished chunk-mode tasks do not suppress them. A greeting or first line that fails every attempt, or a conversation that ends short, is requeued, and marked failed once `MAX_TASK_REQUEUES` is reached. Quality scoring scores the conversation from the first user line. Opening mode cannot run with `use_questions_file` or `master_duplication_mode` on.

The editor saves the `greeting`, `greeting_context` and `first_line` prompts, the first-line styles, every character profile field and the emotional states with their quotes, apostrophes, angle brackets and backslashes, which dialogue, markup and the `->` in an emotional arc need. The system, question, answer, continuation and lore prompts still have those characters removed when saved.
