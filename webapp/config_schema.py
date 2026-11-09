# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""The Config editor's schema -- Phase 6c, decoration and validation.

**This is not the storage model.** The tree in :mod:`webapp.config_tree` is the source of truth
and every leaf in it renders whether or not it has an entry here.
What this module adds is the two things the generic renderer cannot know:

* **Decoration** -- a human label, a help line, and for a fixed-choice field the list of choices,
  so the editor stops showing ``judge_structured_output`` and starts showing "Judge Structured
  Output" with a dropdown.
* **Validation** -- the 60-odd range, enum, URL, regex and cross-field checks that
  ``config_editor.py:validate_config_handler`` runs before ``save_config_handler`` writes. That
  method is the only thing standing between a typo and a run that fails an hour in, so it has to
  survive the move to the browser. **The validators are the real work here, not the
  labels.**

A missing entry is never a missing field
---------------------------------------
:func:`describe` returns the schema's decoration *merged over* the generic answer, so a path with
no entry keeps the label and kind 6b derived from its own name and value. Adding a leaf to
``config.yml`` therefore cannot make it unreachable, and 6c cannot break what 6b proved. That is
the whole reason the schema is a decoration layer rather than a form definition.

Why the enum tuples are literals
--------------------------------
``NOTE_PLACEMENTS``, ``CAST_TRANSITION_MODES`` and ``THINKING_MODES`` live in ``generation`` and
``api_handler``, which are large modules the editor has no other reason to import. They are
written out here and **pinned by a test** against the engine's own constants
(``tests/test_web_config_schema.py``), so a drift is a red test rather than a dropdown that
offers a value the worker silently rewrites.

Why a leaf is never moved between sections
------------------------------------------
A leaf could in principle be moved between sections for display, but the nav is built from the
top-level keys of the tree and 6b's tests assert that the section list *is* ``tree.keys()``.
Relocating a leaf for display would either duplicate it or break that guarantee, and the tree is
the storage model. So a leaf stays in the section it is stored in; the schema improves the
label and the help text instead.
"""

import re

# --- the fixed choice lists, pinned against the engine by a test ----------

# `generation.NOTE_PLACEMENTS`. Where a per-reply block is injected.
NOTE_PLACEMENTS = ('depth0', 'system')

# `generation.CAST_TRANSITION_MODES`. How a scene-cast entrance/exit is detected.
CAST_TRANSITION_MODES = ('evidence', 'name', 'always')

# `api_handler.THINKING_MODES`, via `config_editor.THINKING_MODE_CHOICES`.
THINKING_MODES = ('inherit', 'default', 'enable', 'disable')

# `config_editor.py`'s scene-cast shape dropdown.
SCENE_CAST_SHAPES = ('minimal', 'churn', 'none')

# The judge's structured-output modes (`config_editor.py:validate_config_handler`).
JUDGE_STRUCTURED_OUTPUT = ('auto', 'off', 'json_object', 'json_schema')

# `run_config.py` reads `gender` to pick the user-speaking phrase set.
GENDERS = ('female', 'male', 'neutral')

# `generation.py` writes sharegpt only; the Tk editor shows it as a fixed label.
OUTPUT_FORMATS = ('sharegpt',)


class Field:
    """One schema entry: how a leaf is labelled, drawn and checked.

    Every attribute is optional. An empty ``kind`` means "use the kind the value's own type
    implies", which is what keeps a schema entry purely additive.
    """

    __slots__ = ('label', 'help', 'kind', 'choices', 'minimum', 'maximum', 'validator', 'codec')

    def __init__(self, label, help='', kind='', choices=(), minimum=None, maximum=None,
                 validator='', codec=''):
        self.label = label
        self.help = help
        self.kind = kind
        self.choices = tuple(choices)
        self.minimum = minimum
        self.maximum = maximum
        self.validator = validator
        self.codec = codec

    def __repr__(self):
        return f'Field({self.label!r})'


def _text(label, help='', validator=''):
    return Field(label, help, 'text', validator=validator)


def _long(label, help=''):
    return Field(label, help, 'textarea')


def _num(label, help='', minimum=None, maximum=None, validator=''):
    return Field(label, help, 'number', minimum=minimum, maximum=maximum, validator=validator)


def _bool(label, help=''):
    return Field(label, help, 'bool')


def _enum(label, choices, help='', validator=''):
    return Field(label, help, 'enum', choices=choices, validator=validator)


def _list(label, help=''):
    return Field(label, help, 'list')


# --- the schema ----------------------------------------------------------
#
# Keyed by exact dot path. Indexed paths (`api.apis.0.url`, `prompts.character.characters.3.name`)
# are matched by the patterns in `_PATTERNS` below, so a seventh API slot or a tenth character
# card is decorated without a new entry.

SCHEMA = {
    # --- api -------------------------------------------------------------
    'api.master_duplication_mode': _bool(
        'Master duplication mode',
        'Duplicates each generated conversation across enabled API slots 1-4.'),
    'api.apis': Field('API slots', 'One entry per API endpoint. Slot 5 is the Slop Fixer and '
                                   'slot 6 the Anti-Slop Fixer; both are configured under '
                                   'Samplers.'),
    'api.threads': _num('Threads', 'Default worker count for a slot that does not set its own.',
                        minimum=1),
    'api.pricing': Field('Pricing', 'Used for the cost and budget metrics on the Overview tab.'),
    'api.pricing.cost_per_1k_tokens': _num(
        'Cost per 1k tokens ($)',
        'Enter 0 if unknown. Drives the Cost metric.', minimum=0),
    'api.pricing.budget_limit': _num(
        'API budget limit ($)',
        'Set to 0 to disable. A run stops when the estimated spend reaches this.', minimum=0),

    # --- valkey ----------------------------------------------------------
    'valkey': Field('Valkey cache', 'Optional Redis-compatible cache for deduplication.'),
    'valkey.enabled': _bool('Enable Valkey caching', 'Off by default; the run works without it.'),
    'valkey.host': _text('Host', 'Hostname of the Valkey/Redis server.'),
    'valkey.port': _num('Port', '1-65535.', minimum=1, maximum=65535),
    'valkey.db': _num('Database', 'Database number. Must be 0 or more.', minimum=0),
    'valkey.password': Field('Password', 'Optional. Stored in config.yml, never sent to the '
                                         'browser.', 'secret'),

    # --- database --------------------------------------------------------
    'database': Field('PostgreSQL database', 'Optional. Off by default.'),
    'database.enabled': _bool('Enable PostgreSQL database', 'Off by default.'),
    'database.host': _text('Host'),
    'database.port': _num('Port', '1-65535.', minimum=1, maximum=65535),
    'database.dbname': _text('Database name'),
    'database.user': _text('User'),
    'database.password': Field('Password', 'Stored in config.yml, never sent to the browser.',
                              'secret'),
    'database.pool_size': _num('Connection pool size', 'Must be at least 1.', minimum=1),

    # --- generation ------------------------------------------------------
    'generation.max_character_cards': _num(
        'Max character cards', 'Maximum character profiles in the Character Engine. 1-100.',
        minimum=1, maximum=100),
    'generation.subject_size': _num(
        'Subject size (chars)',
        'Size of the subject text for question generation when not using questions.txt.',
        minimum=0),
    'generation.context_size': _num(
        'Context size (chars)',
        'Total size of the text (subject plus surrounding) for Q/A generation when not using '
        'questions.txt.', minimum=0),
    'generation.max_attempts': _num(
        'Max attempts (per Q/A turn)',
        'Main retries for a valid answer per turn per API; also used for question, '
        'user-continuation and Slop Fixer calls.', minimum=1),
    'generation.num_turns': _num(
        'Number of turns (per conversation)',
        'Total Q/A pairs, e.g. 1 for a single Q/A. Must be greater than 0.', minimum=1),
    'generation.placeholder_share': _num(
        'Placeholder share (0 to 1)',
        'Opening mode: the share of rows that start with a [Start a new chat] user turn, as '
        "SillyTavern's Strict mode sends.", minimum=0, maximum=1),
    'generation.history_size': _num(
        'History size (questions)',
        'How many recent initial questions to avoid repeating in question generation.',
        minimum=0),
    'generation.num_random_chunks': _num(
        'Number of random chunks', 'Total tasks to generate per run.', minimum=0),
    'generation.sanitize_input_max_length': _num(
        'Max input length',
        'Cap for sanitize_input. A longer input is rejected rather than truncated.',
        minimum=1),
    'generation.remove_reasoning': _bool(
        'Remove reasoning', 'Strips ... blocks from the LLM output.'),
    'generation.remove_em_dash': _bool(
        'Remove em dash', 'Experimental: removes the em dash character from output.'),
    'generation.remove_asterisks': _bool(
        'Remove asterisks', 'Experimental: removes asterisks from output.'),
    'generation.remove_asterisk_space_asterisk': _bool(
        'Remove "* *" pattern', 'Experimental: removes the asterisk-space-asterisk pattern.'),
    'generation.remove_all_asterisks': _bool(
        'Remove all asterisks', 'Experimental: removes every asterisk, including single ones.'),
    'generation.ensure_space_after_line_break': _bool(
        'Ensure space after line break',
        'Experimental: prevents words running together across a line break.'),
    'generation.remove_markdown': _bool(
        'Remove markdown', 'Experimental: converts markdown formatting to plain text.'),
    'generation.retry_missing_quotations': _bool(
        'Retry missing quotations',
        'If a generated reply has a spoken line with no quotation marks at all, retry it.'),
    'generation.repeat_opener_guard': _bool(
        'Repeated opener guard',
        'Sends a reply back when it opens the same way as earlier replies.'),
    'generation.repeat_opener_fixes': _list(
        'Repeated opener fixes',
        'One per line; appended to the answer prompt and cycled. Empty means the built-in '
        'default.'),
    'generation.repeat_opener_max_retries': _num(
        'Repeated opener max retries', 'Must be 0 or more.', minimum=0),
    'generation.repeat_reply_guard': _bool(
        'Repeated reply guard', 'Sends a reply back when it repeats an earlier reply.'),
    'generation.repeat_reply_fixes': _list(
        'Repeated reply fixes',
        'One per line; appended to the answer prompt and cycled. Empty means the built-in '
        'default.'),
    'generation.repeat_reply_max_retries': _num(
        'Repeated reply max retries', 'Must be 0 or more.', minimum=0),
    'generation.first_reply_first_word_cap': _num(
        'First-reply first-word cap (0 to 1)',
        "Experiment: the largest share of this run's first replies that may open with one word, "
        'e.g. 0.3. Judged once 20 first replies are recorded. 0 is off, the default.',
        minimum=0, maximum=1),
    'generation.first_reply_opener_cap': _num(
        'First-reply opening cap (0 to 1)',
        'Experiment: the same for the first two content words, so "A sharp intake of breath" '
        "counts as 'sharp intake' whatever article starts it, e.g. 0.05. 0 is off.",
        minimum=0, maximum=1),
    'generation.first_reply_opener_max_retries': _num(
        'First-reply opener max retries', 'Must be 0 or more.', minimum=0),
    'generation.max_slop_sentence_fix_iterations': _num(
        'Max slop sentence fix iterations',
        'Iterations of sentence-level slop fixing by the Slop Fixer LLM.', minimum=0),
    'generation.max_anti_slop_fix_iterations': _num(
        'Max anti-slop fix iterations',
        'Iterations of the Anti-Slop Fixer.', minimum=0),
    'generation.output_format': _enum(
        'Output format', OUTPUT_FORMATS, 'Format for the output.jsonl files.'),
    'generation.api_request_timeout': _num(
        'API request timeout (seconds)',
        'Connect and read timeout, e.g. 300.', minimum=1),
    'generation.max_newlines_malformed': _num(
        'Max newlines (malformed)',
        'A reply with more newlines than this is treated as malformed.', minimum=0),
    'generation.max_text_length_malformed': _num(
        'Max text length (malformed)',
        'A reply longer than this many characters is treated as malformed.', minimum=0),
    'generation.slop_to_anti_slop_fallback': _bool(
        'Slop to anti-slop fallback',
        'Use the Anti-Slop API as a final attempt when the Slop Fixer fails.'),
    'generation.discard_refused_conversations': _bool(
        'Discard refused conversations',
        'Do not save a conversation if any reply needed a jailbreak.'),
    'generation.use_conversation_history_for_fixers': _bool(
        'Use conversation history for fixers',
        'Pass conversation context to the slop and anti-slop fixer LLMs for coherent '
        'rephrasing.'),
    'generation.include_metadata': _bool(
        'Include metadata in output',
        'A flat, schema-stable provenance block per record. Trainer-safe; never modifies '
        'conversations[].'),
    'generation.include_style_metrics': _bool(
        'Include style metrics in metadata',
        'Flat style_* columns from the scotoma measurements. Off by default so the default '
        'record stays small.'),
    'generation.debug_prompt_log': _bool(
        'Write prompt traces',
        'Dumps every request\'s full message list and payload to output/debug_prompt*.jsonl, '
        'plus the judge\'s raw reply. One file append per request per worker, so leave it off '
        'for long runs.'),

    # --- quality ---------------------------------------------------------
    'quality': Field('Quality scoring', 'Scores each conversation and can flag it for review.'),
    'quality.enabled': _bool('Enable quality scoring'),
    'quality.use_llm_scoring': _bool(
        'Use LLM for scoring',
        'Coherence, naturalness, engagement and consistency. Uses extra API calls.'),
    'quality.output_filter': _bool(
        'Flag conversations below threshold',
        'Marks a conversation for post-filtering when it scores below the threshold.'),
    'quality.roleplay_dimensions': _bool(
        'Roleplay dimensions',
        'Score the roleplay-specific dimensions as well as the base ones.'),
    'quality.cast_continuity': _bool(
        'Cast continuity dimension',
        'Judges whether entrances and exits read naturally. LLM only, no heuristic.'),
    'quality.tic_dimension': _bool(
        'Tic dimension', 'Score repeated verbal tics as their own dimension.'),
    'quality.per_turn_logic_check': _bool(
        'Per-turn logic check',
        "Rewrites failed turns via the same main API, using the judge's findings as context."),
    'quality.logic_threshold': _num(
        'Logic check threshold', '0-100. Checked when the per-turn logic check is on.',
        minimum=0, maximum=100),
    'quality.logic_max_rewrites': _num(
        'Logic max rewrites', 'Must be 0 or more.', minimum=0),
    'quality.logic_max_chars_for_check': _num(
        'Max chars for logic check', 'Must be greater than 100.', minimum=101),
    'quality.min_score_threshold': _num(
        'Minimum score threshold', '0-100. Below this a conversation is flagged.',
        minimum=0, maximum=100),
    'quality.max_chars_for_scoring': _num(
        'Max chars for scoring', 'Must be greater than 100.', minimum=101),
    'quality.max_profile_chars_for_scoring': _num(
        'Max profile chars for scoring', 'Must be greater than 0.', minimum=1),
    'quality.weights': Field('Judge settings', 'Retries, timeouts and concurrency for the '
                                               'quality judge.'),
    'quality.judge_max_attempts': _num(
        'Judge max attempts', '1-10.', minimum=1, maximum=10),
    'quality.judge_retry_delay': _num(
        'Judge retry delay (seconds)', '0-30.', minimum=0, maximum=30),
    'quality.result_timeout': _num(
        'Judge result timeout (seconds)', 'At least 30.', minimum=30),
    'quality.judge_concurrency': _num(
        'Judge concurrency', '1-32.', minimum=1, maximum=32),
    'quality.async_scoring': _bool(
        'Async scoring', 'Score in the background so generation is not blocked.'),
    'quality.judge_json_repair_attempts': _num(
        'Judge JSON repair rounds', '0-3.', minimum=0, maximum=3),
    'quality.judge_max_tokens': _num(
        'Judge max tokens', '0 means automatic. 0-8192.', minimum=0, maximum=8192),
    'quality.judge_request_timeout': _num(
        'Judge request timeout (seconds)', '5-1800.', minimum=5, maximum=1800),
    'quality.judge_structured_output': _enum(
        'Judge structured output', JUDGE_STRUCTURED_OUTPUT,
        'auto = ask the server for JSON and fall back if it refuses; off = the older '
        'behaviour.'),
    'quality.judge_priority_enabled': _bool(
        'Judge priority enabled',
        'A server without priority scheduling only accepts a priority of 0, so a non-zero '
        'value needs this on to be sent at all.'),
    'quality.judge_priority': _num(
        'Judge priority', '-10 to 10. Lower is handled earlier; -10 is highest.',
        minimum=-10, maximum=10),
    'quality.scoring_api': Field('Quality scoring API',
                                 'A dedicated endpoint for the judge. Leave the URL empty to '
                                 'reuse a main slot.'),
    'quality.scoring_api.url': _text('Quality scoring API URL', 'Must be an http(s) URL.',
                                     validator='url'),
    'quality.scoring_api.model': _text('Quality scoring model'),
    'quality.scoring_api.key': Field('Quality scoring API key',
                                     'Stored in config.yml, never sent to the browser.',
                                     'secret'),
    'quality.scoring_api.slot_idx': _num(
        'Judge API slot', '0-6. 6 means the seventh slot.', minimum=0, maximum=6),
    'quality.scoring_api.enable_thinking': _enum(
        'Judge thinking mode', THINKING_MODES,
        "inherit = follow the Samplers tab's main Thinking Mode. disable = no chain of thought "
        'for score and judge requests, which is much faster on a reasoning model.'),

    # --- prompts ---------------------------------------------------------
    'prompts': Field('Prompts', 'The prompt bodies. Almost all of this file.'),
    'prompts.system': Field('System prompt', 'The system message sent with every request.'),
    'prompts.system.base': _long(
        'Base system prompt', 'Used when variable system prompts are off.'),
    'prompts.system.variable': _bool(
        'Use variable system prompts', 'Randomly chosen from the list below.'),
    'prompts.system.variations': _list(
        'System prompt variations', 'One per line. Chosen at random per conversation.'),
    'prompts.system.top_level_system_prompt': _long(
        'Top-level system prompt',
        'An extra system message placed above the base prompt. Empty by default.'),
    'prompts.question': _long(
        'Question prompt', "Writes the user's first message."),
    'prompts.answer': _long(
        'Answer prompt', 'Writes the character\'s reply.'),
    'prompts.user_continuation_prompt': _long(
        'User continuation prompt', "Writes the user's next message."),
    'prompts.use_questions_file': _bool(
        'Use questions file',
        'Read the questions from input/questions.txt instead of generating them.'),
    'prompts.opening_mode': _bool(
        'Opening mode',
        'The first turn is a greeting from the character rather than a question from the user.'),
    'prompts.greeting': _long(
        'Greeting prompt', 'Writes the opening message in opening mode.'),
    'prompts.greeting_context': _long(
        'Greeting context prompt',
        'Source material for the setting. Take from it only the kind of place, time and mood.'),
    'prompts.first_line': _long(
        'First line prompt', 'Writes the user\'s first line in a roleplay chat.'),
    'prompts.first_line_styles': _list(
        'First line styles',
        'One per line. A style is picked at random and named in the first-line prompt.'),
    'prompts.style': Field('Style', 'Random opening styles appended to the answer prompt.'),
    'prompts.style.enabled': _bool('Enable style prompts'),
    'prompts.style.styles': _list(
        'Style list', 'One per line. A style is picked at random per conversation.'),
    'prompts.questions_dataset': Field(
        'Questions file dataset variables',
        'Dataset aliases usable in input/questions.txt.'),
    'prompts.questions_dataset.append_alias': _text(
        'Append alias',
        'A letter or underscore first, then letters, digits or underscores. Enter it without '
        "'ds:' or braces. Only applies when the questions file is enabled.",
        validator='alias'),
    'prompts.questions_dataset.append_separator': Field(
        'Separator',
        'Shown escaped: \\n is a newline, \\t a tab, \\r a carriage return, \\\\ a backslash.',
        'text', codec='escape'),
    'prompts.lore': _long(
        'World lore and background', 'Injected into the system prompt when non-empty.'),
    'prompts.character': Field('Character engine',
                               'Random character profiles, and the notes built from them.'),
    'prompts.character.enabled': _bool(
        'Enable character engine', 'Random character profiles in conversations.'),
    'prompts.character.include_names_in_prompt': _bool(
        'Include character names in prompt'),
    'prompts.character.class_enabled': _bool(
        'Enable class selection', 'Fantasy classes such as mage, warlock or rogue.'),
    'prompts.character.setting_enabled': _bool(
        'Enable setting selection', 'A custom location or environment for each character.'),
    'prompts.character.num_characters': _num(
        'Number of characters per conversation',
        '1-10. Requires enough character cards.', minimum=1, maximum=10),
    'prompts.character.persona_breakthrough_turn': _num(
        'Persona breakthrough turn',
        '1 = expose the inner persona from the very first reply; 2 = hold back until the '
        'second reply onward. 1-99.', minimum=1, maximum=99),
    'prompts.character.persona_breakthrough_placement': _enum(
        'Persona breakthrough placement', NOTE_PLACEMENTS,
        'depth0 = end of the last user message, after the history: the server reuses its '
        'prefix cache. system = end of the system prompt, as in v10: the history is computed '
        'again whenever the block changes.'),
    'prompts.character.characters': Field('Character profiles',
                                          'Each card is one complete character.'),
    'prompts.scene_cast': Field('Scene cast',
                                'Characters may leave and join during a multi-character scene.'),
    'prompts.scene_cast.enabled': _bool(
        'Enable scene cast',
        'Applies only when 2 or more characters are selected. Off by default.'),
    'prompts.scene_cast.shape': _enum(
        'Cast shape', SCENE_CAST_SHAPES,
        'minimal = one entrance and one exit; churn = up to the max joins and leaves; '
        'none = nothing is ever scheduled.'),
    'prompts.scene_cast.min_present': _num(
        'Minimum characters present', '1-10.', minimum=1, maximum=10),
    'prompts.scene_cast.max_churn': _num(
        'Max joins and leaves', 'Churn shape only. 0-10.', minimum=0, maximum=10),
    'prompts.scene_cast.max_deferrals': _num(
        'Max deferrals',
        'How many replies an un-narrated exit may slip before it is cancelled. 0-5.',
        minimum=0, maximum=5),
    'prompts.scene_cast.placement': _enum(
        'Cast note placement', NOTE_PLACEMENTS,
        'depth0 = end of the last user message, after the history: the server reuses its '
        'prefix cache. system = end of the system prompt.'),
    'prompts.scene_cast.exit_detection': _enum(
        'Transition detection', CAST_TRANSITION_MODES,
        'evidence = a departure or arrival verb in a sentence about the character, with a '
        'pronoun fallback. name = the older name-anywhere test. always = every scheduled '
        'transition lands, narrated or not.'),
    'prompts.scene_cast.join_slot': _num(
        'Join slot', '0 means automatic. Reply numbers count the greeting in opening mode.',
        minimum=0),
    'prompts.scene_cast.exit_slot': _num(
        'Exit slot', '0 means automatic. A departure needs a reply after it to be visible.',
        minimum=0),
    'prompts.scene_cast.allow_cutaway': _bool(
        'Allow cut-away', 'A short moment elsewhere, rejoining the main thread.'),
    'prompts.scene_cast.exit_guidance': _list(
        'Exit guidance', 'One per line. Empty means the built-in list.'),
    'prompts.scene_cast.arrival_guidance': _list(
        'Arrival guidance', 'One per line. Empty means the built-in list.'),
    'prompts.scene_cast.cutaway_note': _long(
        'Cut-away note', 'Empty means the built-in note.'),
    'prompts.emotional_states': Field(
        'Emotional states', 'A random emotional state or arc assigned to the character.'),
    'prompts.emotional_states.enabled': _bool(
        'Enable emotional states', 'Randomly assigned to conversations.'),
    'prompts.emotional_states.states': _list(
        'Emotional states and arcs',
        "One per line. Use '->' for a progression, e.g. 'neutral -> tense'."),
    'prompts.emotional_states.placement': _enum(
        'Emotional state placement', NOTE_PLACEMENTS,
        'depth0 = end of the last user message, after the history. system = end of the system '
        'prompt.'),
    'prompts.emotional_states.per_character': _bool(
        'Per-character emotions',
        'Multi-character scenes: give each character its own random state or arc. Applies only '
        'when 2 or more characters are selected.'),
    'prompts.user_emotional_states': Field(
        'User emotional states', "An emotional state or arc for the simulated user's side."),
    'prompts.user_emotional_states.enabled': _bool(
        'Enable user emotional states',
        "Assign an emotional state or arc to the simulated user. Applies to the user's side "
        'of the conversation only.'),
    'prompts.user_emotional_states.states': _list(
        'User emotional states and arcs',
        "One per line. Use '->' for a progression, e.g. 'curious -> defensive'."),
    'prompts.random_names': Field(
        'Random names', 'Each conversation gets its own name from the list.'),
    'prompts.random_names.enabled': _bool(
        'Enable random names',
        'Each conversation gets its own name from the list below; the character card itself is '
        'unchanged.'),
    'prompts.random_names.names': _list(
        'Random names',
        'One per line. Dealt without replacement, so a long run spreads the whole list.'),
    'prompts.random_settings': Field(
        'Random settings', 'Fill empty character settings from a pool.'),
    'prompts.random_settings.enabled': _bool(
        'Enable random settings pool',
        'Fill empty character settings; unifies multi-character scenes.'),
    'prompts.random_settings.settings': _list(
        'Random settings pool',
        'One per line, sampled into empty character settings.'),

    # --- detection -------------------------------------------------------
    'detection': Field('Detection', 'Checks that flag or repair a reply.'),
    'detection.no_user_impersonation': _bool(
        'Disable user impersonation detection',
        'Globally disables the check that the model is writing as the user.'),
    'detection.out_of_character': Field(
        'Out-of-character detection',
        'Catches helpful-bot narration breaks, god-viewing and fourth-wall drops.'),
    'detection.out_of_character.enabled': _bool('Enable OOC detection'),
    'detection.out_of_character.phrases': _list(
        'Detection phrases', 'One per line. A reply containing one is flagged.'),
    'detection.out_of_character.fixes': _list(
        'Fixes', 'Appended to the system prompt, or used for fixer rotation. One per line.'),
    'detection.non_latin': Field(
        'Non-Latin script check', 'Flags stray Chinese, Japanese, Korean or Cyrillic text.'),
    'detection.non_latin.enabled': _bool('Enable non-Latin script check'),
    'detection.non_latin.fixes': _list(
        'Fixes',
        'One per line; appended to the system prompt on a retry. Empty means the built-in '
        'default.'),
    'detection.foreign_word': Field(
        'Foreign word check', 'Flags a word that is not in the expected language.'),
    'detection.foreign_word.enabled': _bool('Enable foreign word check'),
    'detection.foreign_word.fixes': _list(
        'Fixes', 'One per line. Empty means the built-in default.'),
    'detection.scene_continuity': Field(
        'Scene continuity check',
        'Catches a character who has left the scene, or has not arrived, still being written.'),
    'detection.scene_continuity.enabled': _bool('Enable scene continuity check'),
    'detection.scene_continuity.max_rounds': _num(
        'Max rewrite rounds', '1-5.', minimum=1, maximum=5),
    'detection.scene_continuity.fixes': _list(
        'Fixes', 'One per line. Empty means the built-in default.'),
    'detection.refusal': Field('Refusal detection',
                               'Catches a model that refuses the prompt.'),
    'detection.refusal.phrases': _list(
        'Detection phrases', 'One per line. Empty means the built-in list.'),
    'detection.refusal.fixes': _list(
        'Fixes', 'One per line. Empty means the built-in default.'),
    'detection.anti_slop': Field(
        'Anti-slop detection',
        'An optional, independent detection layer. Every detected phrase costs an extra call.'),
    'detection.anti_slop.enabled': _bool(
        'Enable anti-slop detection', 'Off by default since v12.7.6.'),
    'detection.anti_slop.phrases': _list(
        'Detection phrases', 'One per line. Empty means the built-in list.'),
    'detection.anti_slop.fixes': _list(
        'Fixes', 'One per line. Empty means the built-in default.'),
    'detection.slop': Field('Slop detection',
                            'Off by default since v12.7.6; every detected phrase costs an '
                            'extra call.'),
    'detection.slop.enabled': _bool('Enable slop detection'),
    'detection.slop.phrases': _list(
        'Detection phrases', 'One per line. Empty means the built-in list.'),
    'detection.slop.fixes': _list(
        'Fixes', 'One per line. Empty means the built-in default.'),
    'detection.user_speaking': Field(
        'User speaking detection',
        'One phrase set per assistant persona gender. Chosen by the top-level `gender` key.'),
    'detection.regex_watch': Field(
        'Regex Watch', 'Catches rhetorical shapes such as "not just X, but Y".'),
    'detection.regex_watch.enabled': _bool('Enable Regex Watch'),
    'detection.regex_watch.max_rounds': _num(
        'Rewrite rounds per reply', '1-5.', minimum=1, maximum=5),
    'detection.regex_watch.patterns': _list(
        'Patterns',
        'One regular expression per entry. A pattern that does not compile is rejected on '
        'save, because it would otherwise be skipped silently at run time.'),

    # --- samplers --------------------------------------------------------
    'samplers': Field('Samplers', 'The request parameters sent to the main API.'),
    'samplers.priority': _list(
        'Sampler priority',
        'The order of keys in the API payload, one per line, e.g. temperature, top_p.'),
    'samplers.temperature': _num('Temperature', 'Must be 0 or more.', minimum=0),
    'samplers.top_p': _num('Top P', '0 to 1.', minimum=0, maximum=1),
    'samplers.min_p': _num('Min P', '0 to 1.', minimum=0, maximum=1),
    'samplers.top_k': _num('Top K', '0 or more. 0 disables the cut.', minimum=0),
    'samplers.repetition_penalty': _num(
        'Repetition penalty', 'Must be greater than 0. 1.0 is neutral.', minimum=0.000001),
    'samplers.max_tokens_question': _num(
        'Max tokens (question)', 'At least 1.', minimum=1),
    'samplers.max_tokens_answer': _num('Max tokens (answer)', 'At least 1.', minimum=1),
    'samplers.max_tokens_user_reply': _num(
        'Max tokens (user reply)', 'At least 1.', minimum=1),
    'samplers.enable_thinking': _enum(
        'Thinking mode', THINKING_MODES,
        'Controls chat_template_kwargs enable_thinking for question, answer, '
        'user-continuation and opening-mode requests. The Slop Fixer, Anti-Slop Fixer and '
        'Quality Judge follow it unless their own override is set.'),
    'samplers.logit_bias': _text(
        'Logit bias', 'JSON, e.g. {"50256": -100}. Must parse as JSON when non-empty.',
        validator='json'),
    'samplers.use_bad_words': _bool(
        'Enable bad words',
        'Sends a vLLM-style `bad_words` list to every main-API and slop/anti-slop request.'),
    'samplers.bad_words': _list(
        'Bad words',
        'One per line. Leading whitespace is significant: ` For Example` forbids the word with '
        'its leading space, `For Example` forbids it anywhere.'),
    'samplers.slop_fixer_enable_thinking': _enum(
        'Thinking mode (Slop Fixer)', THINKING_MODES,
        'inherit = follow the main Thinking Mode; disable = no chain of thought for this '
        'rewrite.'),
    'samplers.anti_slop_fixer_enable_thinking': _enum(
        'Thinking mode (Anti-Slop)', THINKING_MODES,
        'inherit = follow the main Thinking Mode; disable = no chain of thought for this '
        'rewrite.'),
    'samplers.slop_fixer_params': Field(
        'Slop Fixer sampler overrides',
        'API slot 5. Empty means the main sampler settings are used.'),
    'samplers.anti_slop_params': Field(
        'Anti-Slop Fixer sampler overrides',
        'API slot 6. Independent of the Slop Fixer. Empty means the main settings are used.'),

    # --- top level -------------------------------------------------------
    'gender': _enum(
        'Assistant persona gender', GENDERS,
        'Selects which user-speaking phrase set is used for detection.'),
    'metrics': Field('Metrics', 'What the Metrics tab records.'),
    'metrics.antislop_columns': _bool(
        'Anti-slop columns', 'Show the anti-slop columns in the metrics table.'),
    'metrics.save_history': _bool(
        'Save history', 'Keep a per-run history for the metrics graphs.'),
    'datasets': Field('Datasets', 'Dataset sources bound to prompt aliases.'),
    'datasets.sources': _list(
        'Sources', 'Dataset sources. Edited from the Datasets panel.'),
    'control': Field('Control interface (CLI)',
                     'Lets `cli.py` reach this process over a socket.'),
    'control.enabled': _bool(
        'Serve control commands',
        'Lets `generator stats` and `generator stop` reach this process. The web app always '
        'serves the socket, because it is what stops two controllers owning one run.'),
    'control.socket_path': _text(
        'Socket path', 'Empty means output/generator-control.sock.'),
    'control.tcp_host': _text(
        'TCP host (optional)', 'Empty means a unix socket only; 127.0.0.1 adds TCP.'),
    'control.tcp_port': _num(
        'TCP port', '1-65535.', minimum=1, maximum=65535),
    'control.allow_remote': _bool(
        'Allow a non-loopback TCP bind',
        'Exposes the control channel to the network. Off by default.'),
}


# --- indexed paths -------------------------------------------------------
#
# A seventh API slot or a tenth character card must be decorated without a new entry, so the
# repeated shapes are matched by pattern. The first match wins, so the more specific patterns
# come first.

_PATTERNS = (
    (re.compile(r'^api\.apis\.\d+$'),
     Field('API slot', 'One endpoint: URL, model, key, and how many workers to run against it.')),

    (re.compile(r'^api\.apis\.\d+\.url$'),
     _text('API URL', 'The chat-completions endpoint, e.g. '
                      'http://127.0.0.1:8000/v1/chat/completions.', validator='url')),
    (re.compile(r'^api\.apis\.\d+\.model$'),
     _text('Model name', 'The model identifier the endpoint expects.')),
    (re.compile(r'^api\.apis\.\d+\.key$'),
     Field('API key', 'Stored in config.yml and pushed to the environment on save. Never sent '
                      'to the browser.', 'secret')),
    (re.compile(r'^api\.apis\.\d+\.enabled$'),
     _bool('Enabled for generation and duplication')),
    (re.compile(r'^api\.apis\.\d+\.threads$'),
     _num('Number of threads', 'At least 1.', minimum=1)),
    (re.compile(r'^api\.apis\.\d+\.rate_limit_rpm$'),
     _num('Rate limit (RPM)', 'Requests per minute. 0 disables the limit.', minimum=0)),
    (re.compile(r'^api\.apis\.\d+\.api_profile$'),
     _enum('API compatibility', (), 'Trims the request body to what the chosen endpoint '
                                    'accepts, so a request does not fail on an unknown field.',
           validator='api_profile')),

    (re.compile(r'^prompts\.character\.characters\.\d+$'),
     Field('Character card', 'One complete character.')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.name$'),
     _text('Name')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.age$'),
     _text('Age', 'Between 18 and 60 when set.', validator='age')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.gender$'),
     _text('Gender')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.race$'),
     _text('Race')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.job$'),
     _text('Job')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.eye_color$'),
     _text('Eye colour')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.hair$'),
     _text('Hair')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.hair_color$'),
     _text('Hair colour')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.clothing$'),
     _long('Clothing')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.jewelry$'),
     _long('Jewellery')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.appearance$'),
     _long('Appearance')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.traits$'),
     _long('Traits')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.backstory$'),
     _long('Backstory')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.persona$'),
     _long('Persona (inner personality)')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.personality$'),
     _long('Personality (outward behaviour)')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.setting$'),
     _long('Setting')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.class$'),
     _text('Class')),
    (re.compile(r'^prompts\.character\.characters\.\d+\.greeting$'),
     _long('Greeting', 'Optional. Used in opening mode.')),

    (re.compile(r'^detection\.user_speaking\.(female|male|neutral)$'),
     Field('User speaking phrases', 'Phrases that identify the assistant writing as the user.')),
    (re.compile(r'^detection\.user_speaking\.(female|male|neutral)\.phrases$'),
     _list('Detection phrases', 'One per line. Empty means the built-in list.')),
    (re.compile(r'^detection\.user_speaking\.(female|male|neutral)\.fixes$'),
     _list('Fixes', 'One per line. Empty means the built-in default.')),

    (re.compile(r'^detection\.regex_watch\.patterns\.\d+$'),
     _text('Pattern', 'One regular expression. Must compile, or the save is rejected.',
           validator='regex')),
    (re.compile(r'^prompts\.random_names\.names\.\d+$'),
     _text('Name')),
    (re.compile(r'^prompts\.random_settings\.settings\.\d+$'),
     _text('Setting')),
    (re.compile(r'^prompts\.emotional_states\.states\.\d+$'),
     _text('State or arc')),
    (re.compile(r'^prompts\.user_emotional_states\.states\.\d+$'),
     _text('State or arc')),
    (re.compile(r'^prompts\.scene_cast\.exit_guidance\.\d+$'),
     _text('Exit guidance')),
    (re.compile(r'^prompts\.scene_cast\.arrival_guidance\.\d+$'),
     _text('Arrival guidance')),
    (re.compile(r'^prompts\.first_line_styles\.\d+$'),
     _text('Style')),
    (re.compile(r'^prompts\.style\.styles\.\d+$'),
     _text('Style')),
    (re.compile(r'^prompts\.system\.variations\.\d+$'),
     _long('Variation')),
    (re.compile(r'^generation\.repeat_opener_fixes\.\d+$'),
     _text('Fix')),
    (re.compile(r'^generation\.repeat_reply_fixes\.\d+$'),
     _text('Fix')),
    (re.compile(r'^samplers\.priority\.\d+$'),
     _text('Sampler key', 'A payload key name, e.g. temperature.')),
    (re.compile(r'^samplers\.bad_words\.\d+$'),
     _text('Bad word', 'Leading whitespace is significant.')),
    (re.compile(r'^detection\.(refusal|slop|anti_slop|out_of_character)\.(phrases|fixes)\.\d+$'),
     _text('Phrase')),
    (re.compile(r'^detection\.(non_latin|foreign_word|scene_continuity)\.fixes\.\d+$'),
     _text('Fix')),
)


def lookup(path):
    """The :class:`Field` for `path`, or ``None`` when it has no schema entry."""
    field = SCHEMA.get(path)
    if field is not None:
        return field
    for pattern, candidate in _PATTERNS:
        if pattern.match(path):
            return candidate
    return None


def label_for(path, fallback=''):
    """The schema's label for `path`, or `fallback` when there is no entry."""
    field = lookup(path)
    if field is not None and field.label:
        return field.label
    return fallback


def encode_escape_sequence(text):
    """Show control characters as their two-character escapes, for a single-line input.

    Ported from `config_editor.encode_escape_sequence`. A browser `<input>` cannot hold a real
    newline, so the separator is shown as ``\\n`` and decoded on save -- the same rule the Tk
    editor applies, so a value means the same thing in both.
    """
    if not isinstance(text, str):
        return ''
    result = text
    for real, shown in (('\\', '\\\\'), ('\n', '\\n'), ('\t', '\\t'), ('\r', '\\r')):
        result = result.replace(real, shown)
    return result


def decode_escape_sequence(text):
    """Turn typed ``\\n`` / ``\\t`` / ``\\r`` / ``\\\\`` back into real characters.

    Ported from `config_editor.decode_escape_sequence`. Only those four escapes are recognised;
    anything else, including a lone trailing backslash, is kept exactly as typed, so an unusual
    separator is never silently mangled.
    """
    if not isinstance(text, str):
        return ''
    out = []
    index = 0
    while index < len(text):
        char = text[index]
        if char == '\\' and index + 1 < len(text):
            replacement = {'n': '\n', 't': '\t', 'r': '\r', '\\': '\\'}.get(text[index + 1])
            if replacement is not None:
                out.append(replacement)
                index += 2
                continue
        out.append(char)
        index += 1
    return ''.join(out)


CODECS = {
    'escape': (encode_escape_sequence, decode_escape_sequence),
}


def encode(path, value):
    """The form of `value` a browser input should show."""
    field = lookup(path)
    if field is None or not field.codec:
        return value
    return CODECS[field.codec][0](value)


def decode(path, value):
    """The stored form of a value typed into a browser input."""
    field = lookup(path)
    if field is None or not field.codec:
        return value
    return CODECS[field.codec][1](value)


def _api_profile_choices():
    """The compatibility profile names, read from the registry rather than copied.

    A profile is data (`config/api_profiles.yml`), so a hard-coded list here would go stale the
    moment someone adds one. This is the one choice list that cannot be a literal.
    """
    try:
        import api_profiles
        return tuple(name for name, _label in api_profiles.list_profiles())
    except Exception:  # noqa: BLE001 - a missing registry must not break the whole editor
        return ()


def describe(path, kind):
    """The decoration for one node: label, help, kind, choices and bounds.

    `kind` is the kind :func:`webapp.config_tree.kind_of` already decided. The schema may
    override it, but **only for a scalar leaf** -- a group, a flat list and a secret keep the
    shape the tree gave them, because that shape is what the renderer and the save path agree
    on. This is what keeps a schema entry additive.
    """
    field = lookup(path)
    if field is None:
        return {}

    out = {}
    if field.label:
        out['label'] = field.label
    if field.help:
        out['help'] = field.help
    if field.choices or field.validator == 'api_profile':
        choices = _api_profile_choices() if field.validator == 'api_profile' else field.choices
        out['choices'] = list(choices)
    if field.minimum is not None:
        out['min'] = field.minimum
    if field.maximum is not None:
        out['max'] = field.maximum

    # A schema kind may only refine a scalar. `group`, `list` and `secret` come from the tree.
    if field.kind and kind not in ('group', 'list', 'secret'):
        out['kind'] = field.kind
    # An enum with no choices would render an empty dropdown, which is worse than a text field.
    if out.get('kind') == 'enum' and not out.get('choices'):
        out.pop('kind', None)
    return out


# --- validation ----------------------------------------------------------
#
# These are the checks `config_editor.py:validate_config_handler` performs, ported so they run
# **server-side before a save** exactly as they gate `save_config_handler` today. A
# rejected save writes nothing, and every bad field is reported at once.

def _is_blank(value):
    return value is None or (isinstance(value, str) and not value.strip())


def _check_url(value, _tree):
    """`config_editor.validate_url`: an http(s) scheme and a host."""
    if _is_blank(value):
        return None
    from urllib.parse import urlparse
    try:
        result = urlparse(str(value))
    except ValueError:
        return 'Not a valid URL.'
    if result.scheme not in ('http', 'https') or not result.netloc:
        return 'Must be an http:// or https:// URL with a host.'
    return None


def _check_alias(value, _tree):
    """`dataset_templates.validate_alias`, without importing it on every save."""
    if _is_blank(value):
        return None
    from dataset_templates import DatasetTemplateError, validate_alias
    try:
        validate_alias(str(value))
    except DatasetTemplateError as exc:
        return str(exc)
    return None


def _check_json(value, _tree):
    """`json.loads` on a non-empty value, as the Tk editor does for logit bias."""
    if _is_blank(value):
        return None
    import json
    try:
        json.loads(str(value))
    except (ValueError, TypeError) as exc:
        return f'Must be valid JSON ({exc}).'
    return None


def _check_regex(value, _tree):
    """One pattern must compile. `detection._compile_regex` is the authority."""
    if _is_blank(value):
        return None
    from detection import RegexWatchError, _compile_regex
    try:
        _compile_regex(str(value))
    except RegexWatchError as exc:
        return f'Not a valid regular expression ({exc}).'
    return None


def _check_age(value, _tree):
    """A character card age, when set, must be a number between 18 and 60."""
    if _is_blank(value):
        return None
    try:
        age = int(str(value).strip())
    except ValueError:
        return f'Must be a whole number (got {value!r}).'
    if age < 18 or age > 60:
        return f'Must be between 18 and 60 (got {age}).'
    return None


def _check_api_profile(value, _tree):
    """A slot's compatibility profile must be one the registry knows."""
    if _is_blank(value):
        return None
    import api_profiles
    known = {name for name, _label in api_profiles.list_profiles()}
    if str(value) not in known:
        return f'{value!r} is not a known profile ({", ".join(sorted(known))}).'
    return None


VALIDATORS = {
    'url': _check_url,
    'alias': _check_alias,
    'json': _check_json,
    'regex': _check_regex,
    'age': _check_age,
    'api_profile': _check_api_profile,
}


def _check_choices(path, value, field):
    """A fixed-choice field must hold one of its choices.

    This is the check that makes an `enum` worth having: without it the dropdown is decoration and
    a hand-written request could store a value the worker silently rewrites -- which is exactly
    the failure `config_editor.py` guards against for the scene-cast shape and the judge's
    structured-output mode.
    """
    if _is_blank(value):
        return None
    choices = _api_profile_choices() if field.validator == 'api_profile' else field.choices
    if not choices:
        return None
    if str(value) not in choices:
        return f'Must be one of {", ".join(choices)} (got {value!r}).'
    return None


def _check_bounds(path, value, field):
    """The `minimum` / `maximum` on a Field, for a value that is already the right type."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if field.minimum is not None and value < field.minimum:
        return f'Must be at least {field.minimum} (got {value}).'
    if field.maximum is not None and value > field.maximum:
        return f'Must be at most {field.maximum} (got {value}).'
    return None


# --- cross-field rules ---------------------------------------------------
#
# A rule that reads more than one leaf. Each returns `(path, message)` or None. They run after
# the per-field checks, against the tree with the patch already applied, so a rule sees the
# state the user is asking for rather than the state on disk.

def _rule_append_alias(tree):
    """`config_editor.py`: an append alias only applies while the questions file is enabled."""
    alias = _get(tree, 'prompts.questions_dataset.append_alias')
    if _is_blank(alias):
        return None
    if not _get(tree, 'prompts.use_questions_file'):
        return ('prompts.questions_dataset.append_alias',
                "Append alias only applies when the questions file is enabled; turn on 'Use "
                "questions file' or clear the field.")
    return None


def _rule_bad_words(tree):
    """Enabling bad words with an empty list would send an empty `bad_words` array."""
    if not _get(tree, 'samplers.use_bad_words'):
        return None
    words = _get(tree, 'samplers.bad_words') or []
    if not any(str(word).strip() for word in words):
        return ('samplers.bad_words',
                'Enabling bad words requires at least one non-empty line.')
    return None


def _rule_scene_cast_shape(tree):
    """A shape the worker does not understand silently falls back to 'minimal'."""
    shape = _get(tree, 'prompts.scene_cast.shape')
    if _is_blank(shape):
        return None
    if str(shape) not in SCENE_CAST_SHAPES:
        return ('prompts.scene_cast.shape',
                f'Must be one of {", ".join(SCENE_CAST_SHAPES)} (got {shape!r}).')
    return None


def _rule_logic_threshold(tree):
    """The logic threshold is only meaningful while the per-turn logic check is on."""
    if not _get(tree, 'quality.per_turn_logic_check'):
        return None
    value = _get(tree, 'quality.logic_threshold')
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    if value < 0 or value > 100:
        return ('quality.logic_threshold', f'Must be 0-100 (got {value}).')
    return None


RULES = (
    _rule_append_alias,
    _rule_bad_words,
    _rule_scene_cast_shape,
    _rule_logic_threshold,
)


def _get(tree, path, default=None):
    """Read a dot path out of a plain mapping, tolerating a missing branch."""
    value = tree
    for part in path.split('.'):
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            return default
    return value


def validate(tree, values):
    """Check a coerced patch against `tree` (which already has the patch applied).

    Returns ``{path: message}`` naming every rejected field, or an empty dict when the patch is
    acceptable. **Every** bad field is reported, not just the first, so the editor can highlight
    all of them at once -- the same reason `config_routes.coerce` collects its errors.
    """
    errors = {}

    for path, value in values.items():
        field = lookup(path)
        if field is None:
            continue
        if field.validator:
            message = VALIDATORS[field.validator](value, tree)
            if message:
                errors[path] = message
                continue
        message = _check_choices(path, value, field)
        if message:
            errors[path] = message
            continue
        message = _check_bounds(path, value, field)
        if message:
            errors[path] = message

    for rule in RULES:
        result = rule(tree)
        if result is not None:
            path, message = result
            errors.setdefault(path, message)

    return errors
