<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# 🧑 Prompts & Character Engine

```yaml
prompts:
  use_questions_file: false       # Use input/questions.txt instead of LLM-generated questions
  question: "Write the user's first message in a roleplay drawn from the passage below. ... Recent openings to avoid repeating: {recent_questions}\n\n{context}"
  answer: "Write your next message in the roleplay. Write only the message itself. ... CHARACTER INFORMATION: if a CHARACTER PROFILE block is present ... EMOTIONAL STATE: ... SCENE CAST: ..."
  user_continuation_prompt: "Write the user's next message. Write only as the user, not as any other character, and do not write any character's words, actions, thoughts or feelings. ... USER EMOTIONAL STATE: ... SCENE CAST: ..."
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
  style:                          # {style} for EVERY prompt (see "{style}: one style per conversation")
    enabled: true                 # false = the pre-v13.2.0 prompts, with {style} resolving to nothing
    styles:                       # Dealt WITHOUT replacement across the run; empty = the built-in list
      - "open on a sound or a noise before anything else"
      - "open mid-action, with something already in motion"
      - "open on a small physical detail of the setting"
      # ... 16 entries ship in config/config.yml
  # The prompts above are the ones shipped in config/config.yml. Each names the blocks
  # the worker actually attaches (see "What the shipped prompts refer to" below).

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
- **`random_names`:** Optional feature that gives **each conversation its own name** from a configured list. When `prompts.random_names.enabled` is true, every selected character's `Name:` line in the prompt is replaced with a name dealt from `prompts.random_names.names[]`. This is useful for generating diverse datasets without manually creating many character profiles.

  The name is **scoped to the conversation**: it is chosen for that one scene and is never written back to the character card, so the card keeps its own name and the next conversation starts from it again. This is what lets the feature vary a dataset while reusing a handful of cards.

  The pool is dealt **without replacement** across the run: it is shuffled into a deck and consumed in order, and a new pass is shuffled only once the deck runs out. A 1,000-name pool therefore reaches ~1,000 distinct names over a 1,410-scene run, instead of the ~740 that independent per-character `random.choice` draws would produce (where each used name repeats 2-6 times and ~260 names never appear at all). Within one conversation the dealt names are always **distinct**, so two characters in a multi-character scene are never handed the same name, and a card's own name is never dealt back as a "random" name. If the pool is smaller than the scene, the scene gets as many distinct names as the pool holds rather than duplicates or blanks.

  Because the pool is spread rather than redrawn, a long run **exhausts** a short list: a 50-name pool over 2,000 scenes reuses each name about 40 times. Size the list to the number of conversations you intend to generate.
- **`random_settings`:** Optional feature (analogous to `random_names`) that adds location diversity to roleplay data. It only applies when `character.setting_enabled` is `true`. For each selected character whose own `setting` field is empty, a setting is sampled from `prompts.random_settings.settings[]` (when `enabled`) instead of falling back to the default `"A standard indoor environment."`. A character with its own non-empty `setting` always keeps it. Additionally, when multiple characters are selected for one scene, their settings are **unified to a single shared setting** drawn from the ensemble (the distinct settings they would otherwise carry) so every character inhabits the same location — this feeds the consistency dimension and prevents cross-talk between characters set in different places.

**Template Variables** (each is optional; an unused variable is left alone, so a prompt that names none of them is sent byte-identically. The shipped question prompt uses `{recent_questions}` and `{context}`, the shipped answer prompt uses none, and the shipped continuation prompt uses none):
- `{recent_questions}` — Recent opening questions, as a bulleted list, to avoid repeating them (question prompt only). It is pruned to `generation.history_size` entries and is the only list of recent questions; it never holds assistant replies
- `{subject}` — **The SOURCE PASSAGE**, never a character and never her name: the task's subject chunk, up to `generation.subject_size` characters of the source book. Available in the question prompt and the answer prompt (v12.5); before v12.5 the answer prompt sent a literal `{subject}` to the model. Since v12.7 it is **made safe to embed mid-sentence**: whitespace is collapsed (a multi-paragraph chunk cannot break the instruction's line structure) and a trailing sentence terminator is stripped, so `... into her guts.` followed by the prompt's own comma no longer renders as `guts.,`
- `{context}` — The broader context surrounding the subject (the same kind of passage, not a character), also available in the answer prompt (v12.5) and normalised the same way since v12.7. In the greeting prompt it is `greeting_context` with the book chunk in its `{chunk}`, or empty when the task has no book; the shipped `greeting_context` tells the model to take only the kind of place, objects, weather and mood from the chunk and none of its people, names or events
- `{character_name}` — **Her name**, as the `CHARACTER PROFILE` block's `Name:` line gives it (the first selected character's, or its random name when `random_names` is on). Available in the question, answer and continuation prompts since v12.7. This is the variable an answer prompt should name when it wants to say **who** she is: `the woman, {character_name},` renders as `the woman, Marlowe,`
- `{character_names}` — Every selected character's name, joined by `, `, for a multi-character scene. Same availability as `{character_name}`
- `{character_profile}` — The whole `CHARACTER N PROFILE` block the system prompt carries, for a prompt that wants the profile inline in the request instead of only in the system prompt. Answer prompt only
- `{last_assistant_message}` — The assistant's previous response (for continuations). **Since v12.7 her reply is sent to that request exactly ONCE:** the history already ends with it, so the trailing assistant message is dropped from the history when the template names this variable, and kept when it does not (so an existing config's requests are unchanged)
- `{speaker}` — Greeting prompt: who the greeting is written as (see below)
- `{chunk}` — Greeting context: the book chunk
- `{style}` — **The conversation's style**, available in the question, answer, user continuation, greeting and first-line prompts (v13.2.0). One value is dealt per **conversation** and every prompt in it fills the same value, so a conversation keeps one style from the greeting to the last reply. With `prompts.style.enabled: false` it resolves to nothing and the prompts read as they did before v13.2.0; the first-line prompt then falls back to `first_line_styles`, drawn at random per first line as it always was

An empty `{subject}` or `{context}` in the answer prompt fills as `N/A`, the same as in the question prompt, so a prompt reading "... of the woman, {subject}" never ends on a dangling comma. The three character variables fall back to `N/A` the same way, so `her, {character_name}.` never reads `her, .`. In the question prompt, `{subject}` and `{context}` are the task's two chunks from the source file; both are also available in the answer prompt since v12.5.

> **Note:** `{subject}` is the **source passage**. Writing it where the sentence wants a **name** — `You are writing the reply of the woman, {subject}, in an intense roleplay` — is what produced the **duplicated assistant turn** fixed in v12.7: the model was handed a ~1000-character slab of finished prose in a name slot, and the most probable continuation of that instruction is to reproduce the slab. Every turn injected the *same* chunk, so every turn reproduced the *same* text, and three byte-identical 884-character replies came out of one conversation. Use `{character_name}` when the prompt wants her name and `{subject}` only when it wants the passage.

> **Note:** `{character_name}` is filled from the same per-task name the `CHARACTER PROFILE` block's `Name:` line carries, so a prompt can name her **without** `include_names_in_prompt` having to be on — the token is filled either way. Before v12.5 the name was not a variable at all (a literal `{subject}` reached the model, so she was referred to only by pronoun or job title: "the archivist", "the captain").

**What the shipped prompts refer to.** The shipped prompts name the blocks the worker actually attaches, so the injected material is used rather than half-ignored:

| Prompt | Blocks it names |
|--------|-----------------|
| `question` | the passage in `{context}`; `USER EMOTIONAL STATE` (when `prompts.user_emotional_states` is on, appended to that request only); the user's seat (the user is someone who meets the characters in their world, not one of the characters — the worker also appends its own `QUESTION_USER_SIDE` line to the question's system prompt); `{recent_questions}`; `{style}` |
| `answer` | `CHARACTER PROFILE` (every field, with `Gender` called out as authoritative, and distinct voices in a multi-character scene); `EMOTIONAL STATE` including the `A -> B -> C` progression and the per-character scope of a per-character run; `INNER PERSONA BREAKTHROUGH`; `SCENE CAST` (`PRESENT` / `NOT IN THE SCENE`, and narrating a scheduled entrance or exit); `{style}` |
| `user_continuation_prompt` | the character profile so the user replies to who the characters actually are; `USER EMOTIONAL STATE`; `SCENE CAST` so the user does not address a character who is not in the scene; `{style}` |
| `greeting` | `{speaker}`, `{context}` and `{style}`; the user is addressed directly as "you" and is already present when the message begins |
| `first_line` | `{style}`; the message it receives is the character's opening greeting, not a running scene |

`SCENE CAST` only appears when `prompts.scene_cast.enabled` is on and the scene has 2+ characters, and the `EMOTIONAL STATE` / `INNER PERSONA BREAKTHROUGH` blocks only when their features are on, so a default run sees none of them. The prompts say "if a ... block is present" rather than assuming it, which keeps one shipped prompt valid for every feature combination. **The same rule governs gender and character count:** the shipped prompts say "every character" / "them" rather than "her", because `prompts.character.num_characters` supports 1–10 and a profile's `Gender` field is authoritative, so a prompt written around one female character would misdescribe every other configuration.

**`{style}`: one style per conversation (v13.2.0).** A generated dataset used to open on the same three or four words in about one row in five. The cause is not a bad prompt but a **constant** one: every request carried the same instruction, so the model's most probable opening was the same one every time — and the repeated-opener and repeated-reply guards could not see it, because both compare a reply with **its own conversation**, never with the run.

`{style}` fixes that by naming a style per conversation. Three rules make it work, and all three matter:

1. **One style per conversation, never per turn.** The style is dealt once when a task starts and every prompt in that task fills the same value. A style that changed between turns would read as two scenes stitched together.
2. **Dealt WITHOUT replacement across the run.** The list is shuffled into a deck and consumed in order, and a fresh pass is shuffled only when it runs out. Independent random draws **redraw with replacement**, so over a few thousand rows a 12-entry list leaves some entries dealt four times more often than others — and those are exactly the rows that still share an opening. Walking the deck gives every entry the same share: 1,200 deals over a 12-entry list give each entry **exactly 100**.
3. **The entries describe how a message BEGINS, not a mood or a genre.** Two rows can both be "tense" and still open on the same words; one opening on a sound and one mid-action cannot. That is why the shipped list is written as "open on …" rather than "a tense scene".

```yaml
prompts:
  style:
    enabled: true        # false = the pre-v13.2.0 prompts, {style} resolves to nothing
    styles:              # one per line; empty = generation.DEFAULT_STYLE_VARIANTS
      - "open on a sound or a noise before anything else"
      - "open mid-action, with something already in motion"
```

Size the list to the number of conversations you intend to generate, the same way `random_names` is sized: the deck spreads the list rather than redrawing it, so a short list is walked round many times over a long run. A **larger** list is what gives a long run more distinct openings — 16 entries over 1,000 conversations means each style is used about 63 times, so the entries should differ from each other in a way that matters to the opening, not just in wording.

The variable is available in **all five authored prompts**, so a config can name it wherever it wants the opening shaped. `{style}` is filled **last** in every formatter, so a style entry that happens to contain another token is inserted literally instead of being substituted again. An empty or absent style resolves to **nothing**, never the literal `{style}`, so a config written for this feature and run with it off sends a prompt with the slot blank rather than an instruction to write "{style}".

`prompts.first_line_styles` is unchanged and still applies when the style block is off. With the block on, the dealt style **wins** in the first-line prompt, so the greeting, the first line and every reply share one style.

**Opening Mode:** With `opening_mode: true`, each sample opens the way a SillyTavern chat does, with the character's greeting:

1. **Greeting.** The scene prompt (system prompt, lore, character profiles) is sent with the `greeting` prompt, which gets the book chunk in `{context}` when the run has input files and is followed by the first reply's note (its emotional state, and the inner persona when `persona_breakthrough_turn` is 1); a block on `system` placement goes at the end of the greeting's system prompt instead. It uses `max_tokens_answer` and is retried on refusal, slop, the configured user-speaking phrases (unless `no_user_impersonation` is on), a short reply or a malformed one. `{speaker}` is the character's name with one character (or "the character described above" when `include_names_in_prompt` is false), "the narrator, bringing ... into the scene" with several, and "the character you are playing" when no profile is in the prompt.
2. **First user line.** The `first_line` prompt is the system message and the greeting is sent as the other party's turn, without the scene prompt, so the line is written from the user's seat. It uses `max_tokens_user_reply` and is retried on refusal, slop, a short reply or a malformed one.
3. **Replies and later turns.** The existing answer and continuation path, with every check and fixer, starting from the greeting and first line. Later user turns still come from `user_continuation_prompt`. `num_turns` counts replies; the greeting does not count.

Neither new request uses the Valkey response cache, so a retry gets a new reply. A sample costs one request more than in chunk mode. Without input files, a run queues `num_random_chunks` opening tasks (12000 by default); with input files, opening tasks are book chunks, with IDs starting `opening_` so finished chunk-mode tasks do not suppress them. A greeting or first line that fails every attempt, or a conversation that ends short, is requeued, and marked failed once `MAX_TASK_REQUEUES` is reached. Quality scoring scores the conversation from the first user line. Opening mode cannot run with `use_questions_file` or `master_duplication_mode` on.

The editor saves the `greeting`, `greeting_context` and `first_line` prompts, the first-line styles, every character profile field, the emotional states, the fixes lists and the question, answer and continuation prompts with their quotes, apostrophes and backslashes intact. Since v12.4 `sanitize_input()` removes only `<` and `>` (a shape guard, not an HTML escaper), and it holds an authored `->` aside so an emotional arc written `A -> B -> C` survives a save instead of collapsing to `A - B - C`.
