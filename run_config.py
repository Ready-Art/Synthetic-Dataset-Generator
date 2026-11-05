# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Turn ``config/config.yml`` into everything a generation run needs (v13.0).

This module is the ONE definition of what the configuration means for a run. The GUI's
``generate.start_processing()`` and the headless controller (``control.py``, driven by
``cli.py``) both build their run through here, so a config key cannot be honoured by one
path and silently ignored by the other -- a CLI run and a GUI run of the same config must
send identical requests and write identical rows.

Two rules make that possible, and both are load-bearing:

1. **Nothing here touches Tk.** No widget, no ``toast``, no ``messagebox``, no
   ``app_state.root``. Every failure is returned as ``(False, "reason")`` (or raised as
   ``RunConfigError``) and the caller decides how to tell the user. That is what lets the
   module be imported and exercised headlessly, which is what makes it testable.
2. **Nothing here imports ``generate.py``.** That module builds the whole Tk window at
   *import* time, so importing it from an engine-side module would make every headless path
   need a display. The dependency is one-way, exactly as ``generation.py`` already does it:
   ``generate.py -> run_config.py -> generation.py``.

The config reads are a straight lift of the block that used to live inline in
``start_processing()``, including the v12.7.6 slop/anti-slop default-off gate and every
``try/except (TypeError, ValueError)`` clamp, because those clamps are behaviour: a typo in
``config.yml`` must degrade the same way whichever path reads it.
"""

import os
import zipfile

import api_profiles
import app_state
import detection
from config_loader import sanitize_input
from dataset_templates import DEFAULT_QUESTION_SEPARATOR, DatasetTemplateError, validate_alias
from logging_config import log_message
from generation import (
    DEFAULT_FIRST_LINE_PROMPT,
    DEFAULT_FIRST_LINE_STYLES,
    DEFAULT_GREETING_CONTEXT,
    DEFAULT_GREETING_PROMPT,
    FIRST_REPLY_OPENER_MAX_RETRIES,
    DEFAULT_NOTE_PLACEMENT,
    enqueue_opening_tasks,
    note_placement,
)

INPUT_DIR = app_state.INPUT_DIR
OUTPUT_DIR = app_state.OUTPUT_DIR
BASE_OUTPUT_FILE_PATH = app_state.BASE_OUTPUT_FILE_PATH
STATE_FILE_PATH = app_state.STATE_FILE_PATH
QUESTIONS_FILE_PATH = os.path.join(INPUT_DIR, 'questions.txt')

DEFAULT_QUESTION_PROMPT = (
    "Generate a question based on the provided text. Recent questions to avoid: "
    "{recent_questions}\n\nSubject: {subject}\n\nContext: {context}"
)
DEFAULT_ANSWER_PROMPT = "Provide an answer to the last question."
DEFAULT_USER_CONTINUATION_PROMPT = (
    "Continue the conversation naturally based on the assistant's last response: "
    "{last_assistant_message}"
)

# The database table every write/export/clear goes through (see
# generation.write_conversation and generate.clear_database).
DB_TABLE = 'generated_conversations'


class RunConfigError(Exception):
    """A configuration problem that must stop a run before it starts.

    Carries a human-readable message that is already safe to show; the GUI turns it into a
    toast, the CLI turns it into a non-zero exit plus a JSON error object.
    """


# ---------------------------------------------------------------------------------------
# Value readers
#
# Each of these mirrors a read that used to be inline in start_processing(). They exist as
# named helpers so the clamping rule is written once and a test can pin it.
# ---------------------------------------------------------------------------------------

def _clamped_share(config, key, default=0.0):
    """A 0..1 share from config; a non-number degrades to `default` with a log line."""
    try:
        return min(1.0, max(0.0, float(config.get(key, default) or default)))
    except (TypeError, ValueError):
        log_message(f"{key} is not a number. Using {default}.", "WARNING")
        return default


def _int_with_default(config, key, default, minimum=None, maximum=None):
    """An int from config, clamped into [minimum, maximum] when those are given."""
    try:
        value = int(config.get(key, default))
    except (TypeError, ValueError):
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _clean_fix_list(raw):
    """A fix list from config: a list of non-empty stripped strings, never a bare string."""
    if not isinstance(raw, list):
        return []
    return [str(fix).strip() for fix in raw if str(fix).strip()]


def _clean_str_list(raw):
    """A list of non-empty stripped strings (used for prompt/name/setting pools)."""
    if isinstance(raw, str):
        raw = [raw]
    return [str(item).strip() for item in (raw or []) if str(item).strip()]


# ---------------------------------------------------------------------------------------
# RunSettings
# ---------------------------------------------------------------------------------------

class RunSettings:
    """Every value a generation run is configured with, resolved from one config.

    The field names match the ``current_*`` / ``*_local`` locals the worker launch already
    uses, so the GUI's ``start_processing()`` can unpack them straight back into the names
    its (unchanged) launch block reads. Adding a worker option means adding a field here and
    a read in ``build_run_settings()`` -- nowhere else.
    """

    # Keys ``start_processing()`` unpacks positionally in the same order it always has, so a
    # future reader can compare this tuple against the launch block and see they agree.
    WORKER_POSITIONAL_FIELDS = (
        'use_questions_file', 'use_variable_system', 'question_prompt', 'answer_prompt',
        'user_continuation_prompt', 'num_turns', 'refusal_phrases',
        'user_speaking_phrases', 'slop_phrases', 'anti_slop_phrases', 'anti_slop_fixes',
        'jailbreaks', 'speaking_fixes', 'slop_fixes_fallback', 'max_attempts',
        'history_size', 'remove_reasoning', 'remove_em_dash', 'remove_asterisks',
        'remove_asterisk_space_asterisk', 'remove_all_asterisks',
        'ensure_space_after_line_break', 'remove_markdown', 'output_format',
        'slop_fixes_for_rotation', 'top_level_system_prompt', 'character_engine_enabled',
        'class_selection_enabled', 'setting_selection_enabled', 'characters',
        'emotional_states_enabled', 'emotional_states', 'num_characters',
        'no_user_impersonation', 'api_request_timeout', 'slop_to_anti_slop_fallback',
        'lore', 'random_names_enabled', 'random_names', 'include_metadata',
        'random_settings_enabled', 'random_settings',
    )

    WORKER_KEYWORD_FIELDS = (
        'opening_mode', 'greeting_prompt', 'first_line_prompt', 'placeholder_share',
        'greeting_context', 'first_line_styles', 'persona_breakthrough_turn',
        'persona_placement', 'emotion_placement', 'ooc_phrases', 'ooc_fixes',
        'ooc_enabled', 'per_turn_logic_check', 'per_turn_logic_max_rewrites',
        'user_emotional_states_enabled', 'user_emotional_states',
        'per_char_random_emotions', 'regex_patterns', 'regex_watch_enabled',
        'regex_watch_max_rounds', 'include_style_metrics', 'retry_missing_quotations',
        'non_latin_enabled', 'non_latin_fixes', 'questions_append_alias',
        'questions_append_separator', 'scene_cast_enabled', 'scene_cast_config',
        'scene_continuity_enabled', 'scene_continuity_fixes',
        'scene_continuity_max_rounds', 'repeat_opener_guard', 'repeat_opener_fixes',
        'repeat_opener_max_retries', 'repeat_reply_guard', 'repeat_reply_fixes',
        'repeat_reply_max_retries', 'first_reply_first_word_cap',
        'first_reply_opener_cap', 'first_reply_opener_max_retries',
    )

    def __init__(self):
        # --- run shape ---
        self.master_duplication_enabled = False
        self.num_threads = 10
        self.num_random_chunks = 12000
        self.subject_size = 1000
        self.context_size = 3000

        # --- prompts ---
        self.question_prompt = DEFAULT_QUESTION_PROMPT
        self.answer_prompt = DEFAULT_ANSWER_PROMPT
        self.user_continuation_prompt = DEFAULT_USER_CONTINUATION_PROMPT
        self.top_level_system_prompt = ''
        self.lore = ''
        self.use_questions_file = False
        self.use_variable_system = False
        self.system_prompts = []
        self.questions_append_alias = ''
        self.questions_append_separator = DEFAULT_QUESTION_SEPARATOR

        # --- opening mode ---
        self.opening_mode = False
        self.greeting_prompt = DEFAULT_GREETING_PROMPT
        self.first_line_prompt = DEFAULT_FIRST_LINE_PROMPT
        self.greeting_context = DEFAULT_GREETING_CONTEXT
        self.first_line_styles = list(DEFAULT_FIRST_LINE_STYLES)
        self.placeholder_share = 0.0

        # --- generation ---
        self.num_turns = 1
        self.max_attempts = 5
        self.history_size = 10
        self.api_request_timeout = 300
        self.output_format = 'sharegpt'
        self.remove_reasoning = False
        self.remove_em_dash = False
        self.remove_asterisks = False
        self.remove_asterisk_space_asterisk = False
        self.remove_all_asterisks = False
        self.ensure_space_after_line_break = False
        self.remove_markdown = False
        self.include_metadata = True
        self.include_style_metrics = False
        self.slop_to_anti_slop_fallback = False
        self.use_conversation_history_for_fixers = True

        # --- character engine ---
        self.character_engine_enabled = True
        self.class_selection_enabled = False
        self.setting_selection_enabled = False
        self.characters = []
        self.num_characters = 1
        self.persona_breakthrough_turn = 2
        self.persona_placement = DEFAULT_NOTE_PLACEMENT
        self.emotional_states_enabled = False
        self.emotional_states = []
        self.per_char_random_emotions = False
        self.emotion_placement = DEFAULT_NOTE_PLACEMENT
        self.user_emotional_states_enabled = False
        self.user_emotional_states = []
        self.random_names_enabled = False
        self.random_names = []
        self.random_settings_enabled = False
        self.random_settings = []

        # --- detection ---
        self.refusal_phrases = []
        self.jailbreaks = []
        self.user_speaking_phrases = []
        self.speaking_fixes = []
        self.slop_enabled = False
        self.slop_phrases = []
        self.slop_fixes_fallback = []
        self.slop_fixes_for_rotation = []
        self.anti_slop_enabled = False
        self.anti_slop_phrases = []
        self.anti_slop_fixes = []
        self.ooc_enabled = True
        self.ooc_phrases = []
        self.ooc_fixes = []
        self.regex_patterns = []
        self.regex_watch_enabled = False
        self.regex_watch_max_rounds = 3
        self.non_latin_enabled = False
        self.non_latin_fixes = []
        self.scene_cast_enabled = False
        self.scene_cast_config = {}
        self.scene_continuity_enabled = True
        self.scene_continuity_fixes = []
        self.scene_continuity_max_rounds = 2
        self.retry_missing_quotations = False
        self.repeat_opener_guard = False
        self.repeat_opener_fixes = []
        self.repeat_opener_max_retries = 1
        self.repeat_reply_guard = False
        self.repeat_reply_fixes = []
        self.repeat_reply_max_retries = 1
        self.first_reply_first_word_cap = 0.0
        self.first_reply_opener_cap = 0.0
        self.first_reply_opener_max_retries = FIRST_REPLY_OPENER_MAX_RETRIES
        self.no_user_impersonation = False
        self.gender = 'female'

        # --- quality ---
        self.quality_enabled = True
        self.quality_output_filter = False
        self.per_turn_logic_check = False
        self.per_turn_logic_max_rewrites = 1

        # --- resolved per-slot API configuration ---
        self.api_configs = []            # all_api_configs_runtime (6 slots)
        self.active_api_configs = []     # [{'config': ..., 'original_slot_idx': i}, ...]
        self.slop_fixer_config = {}
        self.anti_slop_fixer_config = {}

    def as_dict(self):
        """Every setting as a plain dict (for the stats payload and for tests)."""
        return {key: value for key, value in vars(self).items()}


# ---------------------------------------------------------------------------------------
# build_run_settings
# ---------------------------------------------------------------------------------------

def build_run_settings(config=None, master_duplication_enabled=None):
    """Resolve `config` into a :class:`RunSettings`.

    ``master_duplication_enabled`` overrides the config value when given; the GUI passes the
    live checkbox value there (it owns a Tk variable, and the checkbox is the source of truth
    for the current window), while the CLI lets it come from the config file.
    """
    cfg = app_state.global_config if config is None else config
    settings = RunSettings()

    settings.master_duplication_enabled = (
        bool(cfg.get('api.master_duplication_mode', False))
        if master_duplication_enabled is None
        else bool(master_duplication_enabled)
    )

    # --- run shape -------------------------------------------------------------------
    settings.subject_size = cfg.get('generation.subject_size', 1000)
    settings.context_size = cfg.get('generation.context_size', 3000)
    settings.num_random_chunks = cfg.get('generation.num_random_chunks', 12000)

    config_threads = cfg.get('api.threads')
    if config_threads is not None:
        try:
            settings.num_threads = int(config_threads)
        except (TypeError, ValueError):
            raise RunConfigError(
                f"api.threads is not a whole number ({config_threads!r}).")
    else:
        settings.num_threads = app_state.num_threads or 10
    if settings.num_threads <= 0:
        raise RunConfigError("Number of threads must be positive.")

    # --- prompts ---------------------------------------------------------------------
    settings.use_questions_file = cfg.get('prompts.use_questions_file', False)
    settings.use_variable_system = cfg.get('prompts.system.variable', False)
    settings.question_prompt = cfg.get('prompts.question', DEFAULT_QUESTION_PROMPT)
    if not settings.question_prompt or not settings.question_prompt.strip():
        settings.question_prompt = DEFAULT_QUESTION_PROMPT
        log_message("Question prompt was empty in config. Using default prompt.", "WARNING")
    settings.answer_prompt = cfg.get('prompts.answer', DEFAULT_ANSWER_PROMPT)
    settings.user_continuation_prompt = cfg.get(
        'prompts.user_continuation_prompt', DEFAULT_USER_CONTINUATION_PROMPT)
    settings.top_level_system_prompt = cfg.get('prompts.system.top_level_system_prompt', '')
    settings.lore = cfg.get('prompts.lore', '')
    settings.api_request_timeout = cfg.get('generation.api_request_timeout', 300)

    base_sys_prompt = cfg.get('prompts.system.base', "You are a helpful assistant.")
    variations = []
    if settings.use_variable_system:
        variations = cfg.get('prompts.system.variations', [])
    if not variations:
        # Only a genuine misconfiguration when the feature is ON: with it off, an empty
        # variation list is the normal case and the base prompt is the intended one. The old
        # inline block logged this warning unconditionally, which made every default run look
        # broken. Log text only -- the prompt actually sent is unchanged either way.
        if settings.use_variable_system:
            log_message("Warning: Use variable system prompts ON, but no variations in config. "
                        "Using base system prompt.", "WARNING")
        variations = [base_sys_prompt]
    if not any(str(p).strip() for p in variations):
        log_message("Warning: No valid system prompts loaded (all empty). Using a default.",
                    "WARNING")
        variations = ["You are a helpful assistant."]
    settings.system_prompts = list(variations)

    # --- opening mode ----------------------------------------------------------------
    settings.opening_mode = bool(cfg.get('prompts.opening_mode', False))
    settings.greeting_prompt = cfg.get('prompts.greeting', DEFAULT_GREETING_PROMPT)
    if not settings.greeting_prompt or not settings.greeting_prompt.strip():
        settings.greeting_prompt = DEFAULT_GREETING_PROMPT
        if settings.opening_mode:
            log_message("Greeting prompt was empty in config. Using default prompt.", "WARNING")
    settings.first_line_prompt = cfg.get('prompts.first_line', DEFAULT_FIRST_LINE_PROMPT)
    if not settings.first_line_prompt or not settings.first_line_prompt.strip():
        settings.first_line_prompt = DEFAULT_FIRST_LINE_PROMPT
        if settings.opening_mode:
            log_message("First line prompt was empty in config. Using default prompt.",
                        "WARNING")
    settings.greeting_context = cfg.get('prompts.greeting_context', DEFAULT_GREETING_CONTEXT)
    if not isinstance(settings.greeting_context, str) or not settings.greeting_context.strip():
        settings.greeting_context = DEFAULT_GREETING_CONTEXT
        if settings.opening_mode:
            log_message("Greeting context was empty in config. Using default.", "WARNING")
    styles = _clean_str_list(
        cfg.get('prompts.first_line_styles', list(DEFAULT_FIRST_LINE_STYLES)))
    if not styles:
        styles = list(DEFAULT_FIRST_LINE_STYLES)
        if settings.opening_mode:
            log_message("First line styles were empty in config. Using defaults.", "WARNING")
    settings.first_line_styles = styles
    settings.placeholder_share = _clamped_share(
        cfg, 'generation.placeholder_share', 0.0)

    # --- generation ------------------------------------------------------------------
    settings.num_turns = cfg.get('generation.num_turns', 1)
    if settings.num_turns <= 0:
        settings.num_turns = 1
    settings.max_attempts = cfg.get(
        'generation.max_attempts', cfg.get('samplers.max_attempts', 5))
    settings.history_size = cfg.get(
        'generation.history_size', cfg.get('samplers.history_size', 10))
    settings.remove_reasoning = cfg.get('generation.remove_reasoning', False)
    settings.remove_em_dash = cfg.get('generation.remove_em_dash', False)
    settings.remove_asterisks = cfg.get('generation.remove_asterisks', False)
    settings.remove_asterisk_space_asterisk = cfg.get(
        'generation.remove_asterisk_space_asterisk', False)
    settings.remove_all_asterisks = cfg.get('generation.remove_all_asterisks', False)
    settings.ensure_space_after_line_break = cfg.get(
        'generation.ensure_space_after_line_break', False)
    settings.remove_markdown = cfg.get('generation.remove_markdown', False)
    # The output format is fixed: write_conversation() only emits ShareGPT rows.
    settings.output_format = 'sharegpt'
    settings.include_metadata = cfg.get('generation.include_metadata', True)
    settings.include_style_metrics = bool(
        settings.include_metadata and cfg.get('generation.include_style_metrics', False))
    settings.use_conversation_history_for_fixers = cfg.get(
        'generation.use_conversation_history_for_fixers', True)
    settings.gender = cfg.get('gender', 'female')

    # --- quality ---------------------------------------------------------------------
    settings.quality_enabled = cfg.get('quality.enabled', True)
    settings.quality_output_filter = cfg.get('quality.output_filter', False)
    settings.per_turn_logic_check = bool(
        cfg.get('quality.enabled', True)
        and cfg.get('quality.per_turn_logic_check', False))
    settings.per_turn_logic_max_rewrites = _int_with_default(
        cfg, 'quality.logic_max_rewrites', 1, minimum=0)

    # --- character engine ------------------------------------------------------------
    character_config = cfg.get('prompts.character', {}) or {}
    settings.character_engine_enabled = character_config.get('enabled', True)
    settings.class_selection_enabled = character_config.get('class_enabled', False)
    settings.setting_selection_enabled = character_config.get('setting_enabled', False)
    settings.persona_breakthrough_turn = _int_with_default(
        character_config, 'persona_breakthrough_turn', 2, minimum=1)
    settings.num_characters = character_config.get('num_characters', 1)
    if settings.num_characters < 1:
        settings.num_characters = 1
    settings.characters = _characters_from_config(character_config)

    emotional_config = cfg.get('prompts.emotional_states', {}) or {}
    settings.emotional_states_enabled = emotional_config.get('enabled', False)
    settings.emotional_states = emotional_config.get('states', []) or []
    settings.per_char_random_emotions = bool(emotional_config.get('per_character', False))
    if settings.emotional_states_enabled and not settings.emotional_states:
        log_message("Warning: Emotional states enabled but no states defined. "
                    "Disabling emotional states.", "WARNING")
        settings.emotional_states_enabled = False
        settings.emotional_states = []

    user_emotional_config = cfg.get('prompts.user_emotional_states', {}) or {}
    settings.user_emotional_states_enabled = user_emotional_config.get('enabled', False)
    settings.user_emotional_states = user_emotional_config.get('states', []) or []
    if settings.user_emotional_states_enabled and not settings.user_emotional_states:
        log_message("Warning: User emotional states enabled but no states defined. "
                    "Disabling user emotional states.", "WARNING")
        settings.user_emotional_states_enabled = False
        settings.user_emotional_states = []

    settings.persona_placement = _placement_or_default(
        character_config.get('persona_breakthrough_placement'),
        'prompts.character.persona_breakthrough_placement')
    settings.emotion_placement = _placement_or_default(
        emotional_config.get('placement'), 'prompts.emotional_states.placement')

    random_names_config = cfg.get('prompts.random_names', {}) or {}
    settings.random_names_enabled = random_names_config.get('enabled', False)
    settings.random_names = random_names_config.get('names', []) or []
    if settings.random_names_enabled and not settings.random_names:
        log_message("Warning: Random names enabled but no names defined. "
                    "Disabling random names.", "WARNING")
        settings.random_names_enabled = False
        settings.random_names = []

    random_settings_config = cfg.get('prompts.random_settings', {}) or {}
    settings.random_settings_enabled = random_settings_config.get('enabled', False)
    settings.random_settings = random_settings_config.get('settings', []) or []
    if settings.random_settings_enabled and not settings.random_settings:
        log_message("Warning: Random settings enabled but no settings defined. "
                    "Disabling random settings.", "WARNING")
        settings.random_settings_enabled = False
        settings.random_settings = []

    # --- detection -------------------------------------------------------------------
    _build_detection_settings(settings, cfg)

    # --- dataset aliases in input/questions.txt --------------------------------------
    settings.questions_append_alias = str(
        cfg.get('prompts.questions_dataset.append_alias', '') or '').strip()
    separator = cfg.get('prompts.questions_dataset.append_separator',
                        DEFAULT_QUESTION_SEPARATOR)
    settings.questions_append_separator = (
        separator if isinstance(separator, str) else DEFAULT_QUESTION_SEPARATOR)
    if settings.questions_append_alias:
        try:
            validate_alias(settings.questions_append_alias)
        except DatasetTemplateError as exc:
            raise RunConfigError(f"prompts.questions_dataset.append_alias: {exc}")
        if not settings.use_questions_file:
            log_message("prompts.questions_dataset.append_alias is set but the questions "
                        "file is off; it has no effect.", "WARNING")

    # --- API slots -------------------------------------------------------------------
    settings.api_configs, settings.active_api_configs = build_api_configs(cfg)
    if len(settings.api_configs) > 4:
        settings.slop_fixer_config = settings.api_configs[4]
    if len(settings.api_configs) > 5:
        settings.anti_slop_fixer_config = settings.api_configs[5]

    return settings


def _placement_or_default(value, key):
    placement = note_placement(value)
    if placement is None:
        log_message(f"{key} is not 'depth0' or 'system'. Using {DEFAULT_NOTE_PLACEMENT}.",
                    "WARNING")
        return DEFAULT_NOTE_PLACEMENT
    return placement


def _characters_from_config(character_config):
    """The character list, converting the pre-list-of-dicts format when needed."""
    characters = character_config.get('characters', [])
    if characters:
        return characters

    old_names = character_config.get('name', [])
    old_ages = character_config.get('age', [])
    old_genders = character_config.get('gender', [])
    old_races = character_config.get('race', [])
    old_jobs = character_config.get('job', [])
    old_clothing = character_config.get('clothing', [])
    old_jewelry = character_config.get('jewelry', [])
    old_appearance = character_config.get('appearance', [])
    old_backstory = character_config.get('backstory', [])
    old_persona = character_config.get('persona', [])
    old_personality = character_config.get('personality', [])
    old_setting = character_config.get('setting', [])
    old_class = character_config.get('class', [])

    lists = [old_names, old_races, old_jobs, old_clothing, old_jewelry, old_appearance,
             old_backstory, old_persona, old_personality, old_setting, old_class,
             old_genders, old_ages]
    if not any(lists):
        return []

    max_len = max(len(entry) for entry in lists)
    converted = []
    for i in range(max_len):
        def pick(seq):
            return seq[i] if i < len(seq) else ''
        converted.append({
            'name': pick(old_names), 'age': pick(old_ages), 'gender': pick(old_genders),
            'race': pick(old_races), 'job': pick(old_jobs), 'clothing': pick(old_clothing),
            'jewelry': pick(old_jewelry), 'appearance': pick(old_appearance),
            'backstory': pick(old_backstory), 'persona': pick(old_persona),
            'personality': pick(old_personality), 'setting': pick(old_setting),
            'class': pick(old_class),
        })
    log_message(f"Converted old character config format ({max_len} characters) to new "
                f"format.", "INFO")
    return converted


def _build_detection_settings(settings, cfg):
    """Every detection read, including the v12.7.6 slop/anti-slop default-off gate.

    That gate is deliberately duplicated here rather than delegated, because it is a
    *behaviour* rule: a disabled layer contributes an EMPTY phrase list so the worker's
    existing "no phrases = no detection" path does the rest, and so the dashboard counters
    stay at zero instead of counting detections nothing acts on. Keeping it in one place
    (here) is what stops the CLI and the GUI disagreeing about it.
    """
    settings.refusal_phrases = cfg.get('detection.refusal.phrases', [])
    settings.jailbreaks = cfg.get('detection.refusal.fixes', [])

    speaking_config = cfg.get(f'detection.user_speaking.{settings.gender}', {}) or {}
    settings.user_speaking_phrases = speaking_config.get('phrases', [])
    settings.speaking_fixes = speaking_config.get('fixes', [])

    settings.slop_enabled = bool(cfg.get('detection.slop.enabled', False))
    settings.anti_slop_enabled = bool(cfg.get('detection.anti_slop.enabled', False))
    if not settings.slop_enabled:
        log_message("Slop Detection is disabled (detection.slop.enabled: false). "
                    "See docs/slop-and-antislop.md for the recommended alternative.", "INFO")
    if not settings.anti_slop_enabled:
        log_message("Anti-Slop Detection is disabled (detection.anti_slop.enabled: false). "
                    "See docs/slop-and-antislop.md for the recommended alternative.", "INFO")

    settings.slop_phrases = (cfg.get('detection.slop.phrases', [])
                             if settings.slop_enabled else [])
    settings.slop_fixes_fallback = (cfg.get('detection.slop.fixes', [])
                                    if settings.slop_enabled else [])
    settings.slop_fixes_for_rotation = (cfg.get('detection.slop.fixes', [])
                                        if settings.slop_enabled else [])
    settings.anti_slop_phrases = (cfg.get('detection.anti_slop.phrases', [])
                                  if settings.anti_slop_enabled else [])
    settings.anti_slop_fixes = (cfg.get('detection.anti_slop.fixes', [])
                                if settings.anti_slop_enabled else [])
    settings.slop_to_anti_slop_fallback = bool(
        cfg.get('generation.slop_to_anti_slop_fallback', False)
        and settings.slop_enabled and settings.anti_slop_enabled)

    settings.ooc_phrases = cfg.get('detection.out_of_character.phrases', [])
    settings.ooc_fixes = cfg.get('detection.out_of_character.fixes', [])
    settings.ooc_enabled = bool(cfg.get('detection.out_of_character.enabled', True))

    settings.regex_patterns, pattern_errors = detection.validate_regex_patterns(
        cfg.get('detection.regex_watch.patterns', []) or [])
    for bad_pattern, bad_reason in pattern_errors:
        log_message(f"Regex Watch: ignoring invalid pattern '{str(bad_pattern)[:60]}': "
                    f"{bad_reason}", "WARNING")
    settings.regex_watch_enabled = bool(cfg.get('detection.regex_watch.enabled', False))
    settings.regex_watch_max_rounds = _int_with_default(
        cfg, 'detection.regex_watch.max_rounds', 3, minimum=1, maximum=5)

    settings.retry_missing_quotations = bool(
        cfg.get('generation.retry_missing_quotations', False))

    settings.repeat_opener_guard = bool(cfg.get('generation.repeat_opener_guard', False))
    settings.repeat_opener_fixes = _clean_fix_list(
        cfg.get('generation.repeat_opener_fixes', []))
    settings.repeat_opener_max_retries = _int_with_default(
        cfg, 'generation.repeat_opener_max_retries', 1, minimum=0)

    settings.repeat_reply_guard = bool(cfg.get('generation.repeat_reply_guard', False))
    settings.repeat_reply_fixes = _clean_fix_list(
        cfg.get('generation.repeat_reply_fixes', []))
    settings.repeat_reply_max_retries = _int_with_default(
        cfg, 'generation.repeat_reply_max_retries', 1, minimum=0)

    settings.first_reply_first_word_cap = _clamped_share(
        cfg, 'generation.first_reply_first_word_cap', 0.0)
    settings.first_reply_opener_cap = _clamped_share(
        cfg, 'generation.first_reply_opener_cap', 0.0)
    settings.first_reply_opener_max_retries = _int_with_default(
        cfg, 'generation.first_reply_opener_max_retries',
        FIRST_REPLY_OPENER_MAX_RETRIES, minimum=0)

    settings.non_latin_enabled = bool(cfg.get('detection.non_latin.enabled', False))
    settings.non_latin_fixes = _clean_fix_list(cfg.get('detection.non_latin.fixes', []))

    cast_config = cfg.get('prompts.scene_cast', {}) or {}
    if not isinstance(cast_config, dict):
        cast_config = {}
    settings.scene_cast_config = cast_config
    settings.scene_cast_enabled = bool(cast_config.get('enabled', False))

    settings.scene_continuity_enabled = bool(
        cfg.get('detection.scene_continuity.enabled', True))
    settings.scene_continuity_fixes = _clean_fix_list(
        cfg.get('detection.scene_continuity.fixes', []))
    settings.scene_continuity_max_rounds = _int_with_default(
        cfg, 'detection.scene_continuity.max_rounds', 2, minimum=1, maximum=5)

    settings.no_user_impersonation = bool(cfg.get('detection.no_user_impersonation', False))


# ---------------------------------------------------------------------------------------
# API slot configuration
# ---------------------------------------------------------------------------------------

def build_api_configs(config=None):
    """Resolve the six API slots into runtime dicts.

    Returns ``(all_api_configs_runtime, active_enabled_api_configs_for_worker)``. The
    environment override (``API_URL_1``, ``MODEL_NAME_1``, ``API_KEY_1``, ...) wins over the
    YAML value, exactly as it did inline.
    """
    cfg = app_state.global_config if config is None else config
    yml_apis = cfg.get('api.apis', [])
    if not yml_apis or not isinstance(yml_apis, list):
        raise RunConfigError("API configuration missing or malformed in config.yml.")

    all_configs = []
    active_for_worker = []

    for i in range(len(yml_apis)):
        if i >= 6:
            break
        api_conf_yml = yml_apis[i] if isinstance(yml_apis[i], dict) else {}
        api_runtime = {
            'url': os.getenv(f'API_URL_{i+1}', api_conf_yml.get('url', '')),
            'model': os.getenv(f'MODEL_NAME_{i+1}', api_conf_yml.get('model', '')),
            'key': os.getenv(f'API_KEY_{i+1}', api_conf_yml.get('key', '')),
            'sampler_settings': cfg.get('samplers', {}),
            'threads': api_conf_yml.get('threads', 10),
            'rate_limit_rpm': api_conf_yml.get('rate_limit_rpm', 60),
            'api_profile': os.getenv(
                f'API_PROFILE_{i+1}', api_conf_yml.get('api_profile', api_profiles.DEFAULT_PROFILE)),
            'custom_allowed_params': api_conf_yml.get('custom_allowed_params', []),
        }
        enabled_in_config = api_conf_yml.get('enabled', (i == 0))
        api_runtime['enabled'] = enabled_in_config

        if i < 4:
            if enabled_in_config and api_runtime['url']:
                active_for_worker.append(
                    {'config': api_runtime, 'original_slot_idx': i})
        elif i == 4:
            api_runtime['sampler_settings'] = cfg.get(
                'samplers.slop_fixer_params', cfg.get('samplers', {}))
        elif i == 5:
            api_runtime['sampler_settings'] = cfg.get(
                'samplers.anti_slop_params',
                cfg.get('samplers.slop_fixer_params', cfg.get('samplers', {})))

        all_configs.append(api_runtime)

    while len(all_configs) < 6:
        all_configs.append({'enabled': False, 'url': '', 'model': '', 'key': '',
                            'sampler_settings': {}})

    return all_configs, active_for_worker


def register_api_profiles(all_configs):
    """Register each slot's payload-compatibility profile with ``api_profiles``."""
    api_profiles.load_profiles()
    api_profiles.reset_slot_profiles()
    for slot_idx, conf in enumerate(all_configs[:6]):
        resolved = api_profiles.set_slot_profile(
            slot_idx, conf.get('api_profile'), conf.get('custom_allowed_params'),
            conf.get('url'))
        if conf.get('url'):
            log_message(f"API Slot {slot_idx+1} compatibility profile: {resolved}", "INFO")


def apply_rate_limits(all_configs):
    """Push every slot's ``rate_limit_rpm`` into the shared rate limiter."""
    from api_handler import global_rate_limiter
    for i in range(6):
        if i < len(all_configs):
            rpm = all_configs[i].get('rate_limit_rpm', 60)
            global_rate_limiter.set_rate_limit(i, rpm)
            log_message(f"API Slot {i+1} rate limit set to {rpm} RPM from config", "INFO")


def validate_run_shape(settings):
    """Reject a run that cannot produce anything, with the reason.

    Raises :class:`RunConfigError`. Kept separate from ``build_run_settings`` so the CLI can
    run ``--dry-run`` (build + validate, queue nothing, send nothing) and report what WOULD
    happen.
    """
    from generation import opening_mode_conflict

    if settings.master_duplication_enabled:
        configured = sum(
            1 for idx, conf in enumerate(settings.api_configs)
            if idx < 4 and conf.get('enabled') and conf.get('url'))
        if configured == 0:
            raise RunConfigError("Duplication mode on, but no APIs 1-4 are configured.")
    elif not settings.active_api_configs:
        raise RunConfigError("No APIs enabled/configured for non-duplication mode.")

    conflict = opening_mode_conflict(
        settings.opening_mode, settings.use_questions_file,
        settings.master_duplication_enabled)
    if conflict:
        raise RunConfigError(conflict)

    if settings.slop_fixer_config.get('url') and not settings.slop_fixer_config.get('model'):
        log_message("Warning: Slop Fixer API URL (API Slot 5) is set, but Model is missing. "
                    "Sentence-level slop fixing will be disabled.", "WARNING")
        settings.slop_fixer_config['url'] = None


def total_threads_for_run(settings):
    """How many worker threads this run starts (sum over the APIs that will do work)."""
    total = 0
    if settings.master_duplication_enabled:
        for api_idx, api_conf in enumerate(settings.api_configs):
            if api_idx < 4 and api_conf.get('enabled', False) and api_conf.get('url'):
                total += api_conf.get('threads', 10)
    else:
        for api_config in settings.active_api_configs:
            total += api_config['config'].get('threads', 10)
    if total <= 0:
        log_message(f"Warning: No valid thread count found in API configs. Using default of "
                    f"10.", "WARNING")
        total = 10
    return total


# ---------------------------------------------------------------------------------------
# Valkey
# ---------------------------------------------------------------------------------------

def init_valkey(config=None):
    """Create and test the Valkey/Redis client, or disable caching. Never raises."""
    import redis
    import api_handler

    cfg = app_state.global_config if config is None else config
    if not cfg.get('valkey.enabled', True):
        api_handler.valkey_client = None
        log_message("Valkey caching is disabled in config.", "INFO")
        return False

    try:
        host = cfg.get('valkey.host', 'localhost')
        port = cfg.get('valkey.port', 6379)
        db = cfg.get('valkey.db', 0)
        password = cfg.get('valkey.password')
        log_message(f"Valkey config - Host: {host}, Port: {port}, DB: {db}", "DEBUG")
        api_handler.valkey_client = redis.Redis(
            host=host, port=port, db=db, password=password, decode_responses=True,
            socket_timeout=5, socket_connect_timeout=5, retry_on_timeout=True)
    except Exception as exc:
        log_message(f"Failed to initialize Valkey client: {exc}. Caching will be disabled.",
                    "WARNING")
        api_handler.valkey_client = None
        return False

    try:
        if api_handler.valkey_client.ping():
            log_message("Valkey connected successfully during initialization.", "INFO")
            return True
        log_message("Valkey PING returned False", "WARNING")
        api_handler.valkey_client = None
        return False
    except redis.ConnectionError as exc:
        log_message(f"Valkey connection error during init: {exc}", "ERROR")
    except redis.TimeoutError as exc:
        log_message(f"Valkey timeout error during init: {exc}", "ERROR")
    except Exception as exc:
        log_message(f"Valkey unexpected error during init: {exc}", "ERROR")
    api_handler.valkey_client = None
    return False


# ---------------------------------------------------------------------------------------
# Input reading
# ---------------------------------------------------------------------------------------

def read_book(file_path, config=None):
    """A whole input file, sanitized and length-capped."""
    cfg = app_state.global_config if config is None else config
    with open(file_path, 'r', encoding='utf-8') as handle:
        content = handle.read()
    max_len = cfg.get('generation.sanitize_input_max_length', 100000000)
    return sanitize_input(content, max_length=max_len)


def read_txt(file_path, config=None):
    """An input file as a list of non-empty sanitized lines."""
    if not os.path.exists(file_path):
        return None
    cfg = app_state.global_config if config is None else config
    max_len = cfg.get('generation.sanitize_input_max_length', 100000000)
    with open(file_path, 'r', encoding='utf-8') as handle:
        return [sanitize_input(line.strip(), max_length=max_len)
                for line in handle if line.strip()]


def input_files():
    """Every ``.txt`` in ``input/`` except the questions file."""
    if not os.path.isdir(INPUT_DIR):
        return []
    questions_name = os.path.basename(QUESTIONS_FILE_PATH)
    return [name for name in os.listdir(INPUT_DIR)
            if name.endswith('.txt') and name != questions_name]


# ---------------------------------------------------------------------------------------
# Task queue population
# ---------------------------------------------------------------------------------------

def populate_task_queue(settings, task_queue=None, config=None):
    """Queue this run's tasks. Returns ``(total_tasks_to_queue, total_for_progress)``.

    Mirrors the GUI's three branches exactly -- questions file, opening mode without input,
    random chunking -- including the ``completed_task_ids`` skip that still counts toward
    the progress total. ``task_queue`` defaults to ``app_state.task_queue``.
    """
    import random

    cfg = app_state.global_config if config is None else config
    queue = app_state.task_queue if task_queue is None else task_queue
    total = 0

    if settings.use_questions_file:
        app_state.questions_list = read_txt(QUESTIONS_FILE_PATH, cfg)
        if not app_state.questions_list:
            raise RunConfigError(
                f"{QUESTIONS_FILE_PATH} enabled but empty or not found.")
        for index, question_text in enumerate(app_state.questions_list):
            task_id = f"q_{index}"
            if task_id not in app_state.completed_task_ids:
                task_tuple = (task_id, os.path.basename(QUESTIONS_FILE_PATH), index,
                              question_text)
                queue.put(task_tuple)
                total += 1
                with app_state.task_queue_ui_lock:
                    app_state.pending_task_ids.add(task_id)
                    app_state.task_metadata[task_id] = {
                        'file': os.path.basename(QUESTIONS_FILE_PATH), 'retries': 0,
                        'task_data': task_tuple}
        return total, total + len(app_state.completed_task_ids)

    app_state.questions_list = []
    files = input_files()

    if settings.opening_mode and not files:
        total = enqueue_opening_tasks(settings.num_random_chunks)
        log_message(f"Opening mode without input files: {total} tasks for this run.", "INFO")
        return total, total

    if not settings.opening_mode and not files:
        raise RunConfigError("No input .txt files found for chunking.")

    subject_size = settings.subject_size
    context_size = settings.context_size
    if context_size < subject_size:
        log_message(f"Warning: context_size ({context_size}) is less than subject_size "
                    f"({subject_size}). Setting context_size = subject_size.", "WARNING")
        context_size = subject_size

    queued = 0
    attempt = 0
    max_attempts = settings.num_random_chunks * 5

    while queued < settings.num_random_chunks and attempt < max_attempts:
        attempt += 1
        if not files:
            break
        file_name = random.choice(files)
        file_path = os.path.join(INPUT_DIR, file_name)
        try:
            full_content = read_book(file_path, cfg)
            content_len = len(full_content)
            max_start = max(0, content_len - context_size)
            if max_start <= 0:
                log_message(f"File {file_name} is too small for the requested context size. "
                            f"Skipping.", "DEBUG")
                continue
            start = random.randint(0, max_start)
            task_id = ("opening_" if settings.opening_mode else "") + \
                      f"{file_name}_chunk_at_{start}"
            if task_id in app_state.completed_task_ids:
                total += 1
                continue

            subject_end = min(start + subject_size, content_len)
            subject_content = full_content[start:subject_end]
            if not subject_content.strip() or len(subject_content) < subject_size / 2:
                log_message(f"Random chunk at {start} in {file_name} is too short. "
                            f"Skipping.", "DEBUG")
                continue

            subject_len = len(subject_content)
            context_needed = context_size - subject_len
            before = context_needed // 2
            after = context_needed - before
            context_start = max(0, start - before)
            context_end = min(content_len, subject_end + after)
            if context_end - context_start < context_size:
                if context_start == 0 and context_end < content_len:
                    context_end = min(content_len, context_start + context_size)
                elif context_end == content_len and context_start > 0:
                    context_start = max(0, context_end - context_size)
            context_text = full_content[context_start:context_end]

            task_tuple = (task_id, file_name, start, subject_content, context_text)
            queue.put(task_tuple)
            queued += 1
            total += 1
            with app_state.task_queue_ui_lock:
                app_state.pending_task_ids.add(task_id)
                app_state.task_metadata[task_id] = {
                    'file': file_name, 'retries': 0, 'task_data': task_tuple}
        except Exception as exc:
            log_message(f"Error processing random chunk from file {file_name}: {exc}",
                        "ERROR")
            continue

    log_message(f"Attempted to queue {settings.num_random_chunks} random tasks. "
                f"Successfully queued {queued} new unique tasks.", "INFO")
    return total, total + len(app_state.completed_task_ids)


# ---------------------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------------------

def init_db_pool(config=None):
    """Open the PostgreSQL pool when ``database.enabled`` is on. Never raises."""
    cfg = app_state.global_config if config is None else config
    if not cfg.get('database.enabled', False):
        app_state.db_pool = None
        log_message("PostgreSQL database disabled in config.", "INFO")
        return None
    try:
        from psycopg2 import pool as pg_pool
        app_state.db_pool = pg_pool.ThreadedConnectionPool(
            minconn=2, maxconn=cfg.get('database.pool_size', 10), connect_timeout=5,
            host=cfg.get('database.host'), port=cfg.get('database.port'),
            dbname=cfg.get('database.dbname'), user=cfg.get('database.user'),
            password=cfg.get('database.password'))
        log_message("PostgreSQL connection pool initialized.", "INFO")
        return app_state.db_pool
    except Exception as exc:
        log_message(f"Failed to init DB pool: {exc}", "ERROR")
        app_state.db_pool = None
        return None


def close_db_pool():
    """Close every pooled connection (idempotent)."""
    if not app_state.db_pool:
        return
    try:
        app_state.db_pool.closeall()
        log_message("PostgreSQL pool closed.", "INFO")
    except Exception as exc:
        log_message(f"Error closing PostgreSQL pool: {exc}", "WARNING")
    finally:
        app_state.db_pool = None


def db_row_count(pool=None):
    """Rows in the conversations table, or None when the database is unreachable."""
    pool = app_state.db_pool if pool is None else pool
    if not pool:
        return None
    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {DB_TABLE}")
            return int(cur.fetchone()[0])
    except Exception as exc:
        log_message(f"Could not count database rows: {exc}", "WARNING")
        return None
    finally:
        if conn is not None:
            pool.putconn(conn)


def clear_database(pool=None):
    """TRUNCATE the conversations table. Returns ``(ok, message)``.

    No confirmation and no dialog: the caller owns the "are you sure" (the GUI's
    messagebox, the CLI's ``--yes``). That split is what makes this callable from a
    script and testable without a display.
    """
    pool = app_state.db_pool if pool is None else pool
    if not pool:
        return False, "Database pool is not initialized."
    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {DB_TABLE} RESTART IDENTITY;")
            conn.commit()
        log_message("Database cleared successfully.", "INFO")
        return True, "Database cleared."
    except Exception as exc:
        log_message(f"Failed to clear database: {exc}", "ERROR")
        return False, f"Failed to clear database: {exc}"
    finally:
        if conn is not None:
            pool.putconn(conn)


def export_db_to_jsonl(output_path, pool=None):
    """Stream the conversations table out to `output_path`. Returns ``(ok, message, rows)``."""
    pool = app_state.db_pool if pool is None else pool
    if not pool:
        return False, "Database pool is not initialized.", 0
    conn = None
    rows = 0
    try:
        conn = pool.getconn()
        with conn.cursor(name='export_cursor') as cur:
            cur.itersize = 1000
            cur.execute(f"SELECT conversation_data FROM {DB_TABLE} ORDER BY created_at")
            parent = os.path.dirname(os.path.abspath(output_path))
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as handle:
                for row in cur:
                    handle.write(_json_dumps(row[0]) + '\n')
                    rows += 1
        conn.commit()
        log_message(f"Exported {rows} conversation(s) to {output_path}.", "INFO")
        return True, f"Exported {rows} conversation(s) to {output_path}.", rows
    except Exception as exc:
        log_message(f"Export failed: {exc}", "ERROR")
        return False, f"Export failed: {exc}", rows
    finally:
        if conn is not None:
            pool.putconn(conn)


def _json_dumps(value):
    import json
    return json.dumps(value)


# ---------------------------------------------------------------------------------------
# JSONL output files
# ---------------------------------------------------------------------------------------

# Prefixes of files in output/ that are DERIVED from the dataset rather than part of it.
#
# A save must not read its own previous export back in (that doubles the row count on
# every save), and a clear must not delete an export the operator asked for. The prompt
# traces are not dataset rows at all. One list, used by both, so the two cannot disagree.
DERIVED_OUTPUT_PREFIXES = ('debug_prompt', 'dataset_export', 'preclear_backup')


def jsonl_output_files(directory=None):
    """Every conversation JSONL the generator itself writes, sorted.

    Excludes everything in ``DERIVED_OUTPUT_PREFIXES``: the prompt traces are not dataset
    rows, and the exports and backups are copies OF the dataset. Sweeping those in would make
    ``save`` read its own previous output back in (doubling the row count on every save) and
    make ``clear`` delete the very export the operator just asked for.
    """
    directory = OUTPUT_DIR if directory is None else directory
    if not os.path.isdir(directory):
        return []
    found = []
    for name in sorted(os.listdir(directory)):
        if not name.endswith('.jsonl'):
            continue
        if name.startswith(DERIVED_OUTPUT_PREFIXES):
            continue
        found.append(os.path.join(directory, name))
    return found


def count_jsonl_rows(path):
    """Non-blank line count, or 0 when the file is missing/unreadable."""
    if not path or not os.path.exists(path):
        return 0
    rows = 0
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            for line in handle:
                if line.strip():
                    rows += 1
    except OSError:
        return 0
    return rows


def jsonl_row_counts(directory=None):
    """``[{path, rows}]`` for every output JSONL."""
    return [{'path': path, 'rows': count_jsonl_rows(path)}
            for path in jsonl_output_files(directory)]


def consolidate_jsonl(output_path, sources=None, append=False):
    """Write every conversation row from `sources` into one JSONL. ``(ok, message, rows)``.

    Used by ``generator save`` when the file is the sink and the rows are spread over
    per-API files. A line that is not valid JSON is copied through verbatim rather than
    dropped -- it cannot be attributed to a task, and silently discarding a row from a
    dataset is worse than carrying one odd line.
    """
    import json

    sources = jsonl_output_files() if sources is None else list(sources)
    if not sources:
        return False, "No output JSONL files found.", 0

    # Drop the destination from the source list rather than refusing to write it.
    #
    # Re-saving to the same path is the normal case (`generator save -o out.jsonl`
    # twice), so refusing would break it; and reading the destination while writing
    # it would append the dataset to itself. Skipping it in the loop is what makes
    # both work. The list is still checked first so an all-target source list is
    # reported as "nothing to do" instead of silently writing an empty file.
    destination = os.path.abspath(output_path)
    sources = [src for src in sources if os.path.abspath(src) != destination]
    if not sources:
        return False, "No output JSONL files to read (the destination is the only " \
                       "candidate).", 0

    mode = 'a' if append else 'w'
    rows = 0
    try:
        parent = os.path.dirname(destination)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(output_path, mode, encoding='utf-8') as out:
            for source in sources:
                try:
                    with open(source, 'r', encoding='utf-8') as handle:
                        for line in handle:
                            if not line.strip():
                                continue
                            try:
                                json.loads(line)
                            except ValueError:
                                log_message(f"Copying a non-JSON line from {source} verbatim.",
                                            "WARNING")
                            out.write(line if line.endswith('\n') else line + '\n')
                            rows += 1
                except OSError as exc:
                    log_message(f"Could not read {source}: {exc}", "WARNING")
    except Exception as exc:
        return False, f"Save failed: {exc}", rows
    log_message(f"Wrote {rows} conversation(s) to {output_path}.", "INFO")
    return True, f"Wrote {rows} conversation(s) to {output_path}.", rows


def backup_output_zip(prefix='preclear_backup', directory=None):
    """Zip every output JSONL + the state file. Returns ``(ok, message, path)``.

    Taken before a destructive ``clear`` so a mistake is recoverable. Returns a failure
    rather than raising when there is nothing to back up.
    """
    import time

    directory = OUTPUT_DIR if directory is None else directory
    if not os.path.isdir(directory):
        return False, "No output directory.", None
    targets = jsonl_output_files(directory)
    if os.path.exists(STATE_FILE_PATH):
        targets.append(STATE_FILE_PATH)
    if not targets:
        return False, "Nothing to back up.", None
    stamp = time.strftime('%Y%m%d_%H%M%S')
    path = os.path.join(directory, f"{prefix}_{stamp}.zip")
    try:
        with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as archive:
            for target in targets:
                if os.path.exists(target):
                    archive.write(target, os.path.basename(target))
    except Exception as exc:
        return False, f"Backup failed: {exc}", None
    return True, f"Backed up {len(targets)} file(s) to {path}.", path


def clear_jsonl(directory=None):
    """Delete every output conversation JSONL. Returns ``(ok, message, removed_count)``."""
    directory = OUTPUT_DIR if directory is None else directory
    removed = 0
    for path in jsonl_output_files(directory):
        try:
            os.remove(path)
            removed += 1
        except OSError as exc:
            log_message(f"Could not remove {path}: {exc}", "WARNING")
    if removed:
        log_message(f"Removed {removed} output JSONL file(s).", "INFO")
    return True, f"Removed {removed} output JSONL file(s).", removed


def clear_state_file():
    """Remove the saved generation state so the next run starts fresh."""
    if os.path.exists(STATE_FILE_PATH):
        try:
            os.remove(STATE_FILE_PATH)
            log_message(f"Removed state file: {STATE_FILE_PATH}", "INFO")
            return True
        except OSError as exc:
            log_message(f"Could not remove {STATE_FILE_PATH}: {exc}", "WARNING")
            return False
    return True
