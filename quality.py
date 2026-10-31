# quality.py
"""Quality scoring layer for the Synthetic Dataset Generator.

Scores completed conversations across the core dimensions (coherence, naturalness,
engagement, diversity, consistency, technical) using a hybrid approach:
  - Heuristic (fast, no API call): repetition, diversity, punctuation, length
  - LLM (optional, uses a dedicated API slot): coherence, naturalness, engagement,
    consistency (persona fidelity against the character card) and
    logic (objective internal contradictions: sexed anatomy vs. stated gender,
    profile-contradicting body parts/injuries/clothing, impossible timeline statements)

An optional third set of roleplay-specific dimensions -- voice_distinctiveness,
grounding and persona_alignment -- is added behind the ``quality.roleplay_dimensions``
toggle (GUI checkbox in the Configuration Editor). They carry heuristic base scores
(n-gram overlap between characters' spoken lines, profile-keyword coverage, outward vs
inner persona signal placement) and are refined by the LLM judge when available.

The composite score is a weighted 0-100 value over the active dimensions.
Reads shared state through app_state; never imports generate.py (one-way dependency).
"""

import json
import re
import time
import random
import requests
import hashlib
import threading
from queue import Queue, Empty
from threading import Lock

import app_state
import text_utils
import style_metrics
from logging_config import log_message
from api_handler import (
    global_rate_limiter,
    api_response_times_per_slot,
    api_response_times_lock,
    MAX_RESPONSE_TIMES_TO_TRACK,
)
from app_state import (
    global_config,
    BASE_DEBUG_LOG_PATH,
)

# --- Scoring dimensions and their default weights ---
# ``logic`` is the internal-logic / factual-coherence check: it hunts objective
# contradictions in the output (wrong sexed anatomy vs. a character's stated gender,
# profile-contradicting body parts/injuries/clothing, impossible timeline or quantity
# statements). It is distinct from ``coherence`` (does the exchange flow?) and
# ``consistency`` (does the voice stay in character?): those two ask *is it natural*
# and *is it on-character*, while ``logic`` asks *is it even possible*. Weight is added
# on top of the existing set; the composite is normalized by total active weight, so
# existing numbers only shift by a small, well-defined amount.
SCORING_DIMENSIONS = {
    "coherence": 0.25,
    "naturalness": 0.20,
    "engagement": 0.15,
    "diversity": 0.15,
    "consistency": 0.15,
    "logic": 0.15,
    "technical": 0.10,
}

# --- Roleplay-specific dimensions (opt-in via quality.roleplay_dimensions; off by default so
# existing composite scores stay comparable). When active their weights are taken from
# ROLEPLAY_SCORING_DIMENSIONS and folded into the composite; when inactive they never appear in
# scored results, statistics or output metadata. ---
ROLEPLAY_SCORING_DIMENSIONS = {
    "voice_distinctiveness": 0.10,
    "grounding": 0.10,
    "persona_alignment": 0.10,
}

# --- Tic-density dimension (v11.7, opt-in via quality.tic_dimension; off by default so
# existing composite scores stay comparable, exactly like the roleplay dimensions). It has a
# REAL heuristic (style_metrics measurements over the assistant turns), so it is registered in
# HEURISTIC_DIMENSIONS and deliberately NOT in LLM_DIMENSIONS -- the judge-refined path would
# fabricate a number for something we can measure directly. When active its weight is taken
# from TIC_SCORING_DIMENSIONS and folded into the composite; when inactive it never appears in
# scored results, statistics or output metadata. ---
TIC_SCORING_DIMENSIONS = {
    "tic_density": 0.10,
}

# --- Heuristic-only dimensions (no LLM needed) ---
HEURISTIC_DIMENSIONS = {"diversity", "technical", "tic_density"}

# --- LLM-based dimensions (require an API call) ---
LLM_DIMENSIONS = {"coherence", "naturalness", "engagement", "consistency", "logic"}

# --- LLM-based roleplay dimensions (also have a heuristic fallback so they stay
# meaningful when LLM scoring is off or the judge call fails) ---
ROLEPLAY_LLM_DIMENSIONS = set(ROLEPLAY_SCORING_DIMENSIONS)

# --- Judge retry / drop policy (v11.4) ------------------------------------------------
# The quality judge is retried on TRANSIENT failures only (network error, timeout,
# non-200 status, unreadable response envelope). Two verdicts are final and are never
# retried: a reply that is not valid JSON, and a conversation-level logic score of 0.
# A final failure drops the conversation: it is not written to the dataset and is not
# stored as a score (see ``score_conversation`` -> ``drop`` and
# ``generation._score_and_store_quality``).
QUALITY_JUDGE_MAX_ATTEMPTS = 3
QUALITY_JUDGE_RETRY_DELAY = 1.0  # seconds, multiplied by the attempt number

# Flags that make a judge failure final (no retry, drop the conversation).
JUDGE_FINAL_FAILURE_FLAGS = frozenset({"llm_invalid_json"})

# Flags that mean "no judge ran at all" (e.g. no scoring API configured). These are not
# failures: the conversation keeps its heuristic scores instead of being dropped.
JUDGE_SKIP_FLAGS = frozenset({"llm_not_configured"})

# Drop reasons recorded on the returned result / in app_state.quality_dropped.
DROP_REASON_INVALID_JSON = "invalid_json"
DROP_REASON_LOGIC_ZERO = "logic_zero"
DROP_REASON_JUDGE_FAILED = "judge_failed"


def judge_max_attempts(config=None):
    """How many times the quality judge is tried before the row is dropped.

    ``quality.judge_max_attempts`` (default 3, clamped to 1-10). Set to 1 to disable
    retries entirely.
    """
    cfg = global_config if config is None else config
    try:
        attempts = int(cfg.get('quality.judge_max_attempts', QUALITY_JUDGE_MAX_ATTEMPTS))
    except (TypeError, ValueError):
        attempts = QUALITY_JUDGE_MAX_ATTEMPTS
    return max(1, min(10, attempts))


def judge_retry_delay(config=None):
    """Seconds to wait before the next judge attempt (grows linearly per attempt)."""
    cfg = global_config if config is None else config
    try:
        delay = float(cfg.get('quality.judge_retry_delay', QUALITY_JUDGE_RETRY_DELAY))
    except (TypeError, ValueError):
        delay = QUALITY_JUDGE_RETRY_DELAY
    return max(0.0, min(30.0, delay))


def quality_result_timeout(config=None):
    """Seconds a worker waits for its queued quality verdict before dropping the row.

    Covers the whole judge conversation including retries, so the default is generous
    (``quality.result_timeout``, 600s). Only a judge that is wedged this long is dropped
    on time-out -- normal retries finish far sooner.
    """
    cfg = global_config if config is None else config
    try:
        timeout = float(cfg.get('quality.result_timeout', 600))
    except (TypeError, ValueError):
        timeout = 600.0
    return max(30.0, timeout)


def _judge_drop_reason(scores, flags, success):
    """Why this judge result must drop the conversation, or None to keep it.

    Dropped (never saved to the dataset):
      * ``invalid_json``  -- the reply was not parseable JSON (final, never retried)
      * ``logic_zero``    -- the judge scored the conversation's ``logic`` at 0 (final)
      * ``judge_failed``  -- transient failures survived every retry

    A judge that never ran (no scoring API configured) is not a failure: the caller keeps
    its heuristic scores.
    """
    flags = list(flags or [])
    if JUDGE_SKIP_FLAGS.intersection(flags) and not success:
        return None
    if DROP_REASON_INVALID_JSON in flags or "llm_invalid_json" in flags:
        return DROP_REASON_INVALID_JSON
    if success:
        try:
            logic_score = scores.get("logic")
        except AttributeError:
            logic_score = None
        if logic_score is not None and int(logic_score) <= 0:
            return DROP_REASON_LOGIC_ZERO
        return None
    return DROP_REASON_JUDGE_FAILED


def is_tic_scoring_enabled(config=None):
    """Whether the tic_density dimension is included in this run's scoring.

    Driven by ``quality.tic_dimension`` in config.yml (GUI toggle in the Configuration
    Editor's Quality Scoring section), default off so existing composite scores stay
    comparable (plan.md open question 6.1, decided: opt-in).
    """
    cfg = global_config if config is None else config
    return bool(cfg.get('quality.tic_dimension', False))


def is_roleplay_scoring_enabled(config=None):
    """Whether the roleplay-specific dimensions are included in this run's scoring.

    Driven by ``quality.roleplay_dimensions`` in config.yml (GUI toggle in the
    Configuration Editor's Quality Scoring section), default off.
    """
    cfg = global_config if config is None else config
    return bool(cfg.get('quality.roleplay_dimensions', False))


def active_scoring_dimensions(config=None):
    """The ordered dimension names + weights that apply right now.

    The generic dimensions always count; the roleplay dimensions are appended when
    the toggle is on. Order is stable (insertion order) so the dashboard can lay out
    labels and table columns from it without special-casing.
    """
    dims = dict(SCORING_DIMENSIONS)
    if is_tic_scoring_enabled(config):
        dims.update(TIC_SCORING_DIMENSIONS)
    if is_roleplay_scoring_enabled(config):
        dims.update(ROLEPLAY_SCORING_DIMENSIONS)
    return dims


def score_conversation(conversation_history, task_id, thread_id, api_slot_idx=None, file_name="",
                       character_profile_text="", scene_characters=None, logic_findings=None):
    """
    Scores a completed conversation over the active dimensions (see
    ``active_scoring_dimensions()``). Returns a dict:
    {
        "composite": 87.3,
        "dimensions": {
            "coherence": 90,
            "naturalness": 85,
            "engagement": 82,
            "diversity": 95,
            "consistency": 88,
            "logic": 90,
            "technical": 92,
            "voice_distinctiveness": 85,   # only when quality.roleplay_dimensions is on
            "grounding": 90,               # only when quality.roleplay_dimensions is on
            "persona_alignment": 60        # only when quality.roleplay_dimensions is on
        },
        "flags": ["short_response", "low_diversity"],
        "scored_at": "2025-01-15 14:32:00",
        "method": "hybrid"  # or "heuristic_only" or "llm_only" or "dropped"
    }

    When the LLM judge is enabled and rejects the conversation -- invalid JSON, a ``logic``
    score of 0, or a transient failure that survived every retry -- the result is a DROP:
    ``{"dropped": True, "drop_reason": "invalid_json"|"logic_zero"|"judge_failed", ...}``
    with ``method`` ``"dropped"``. Callers must not write a dropped conversation to the
    dataset (``generation._score_and_store_quality`` / the worker write path enforce this).

    ``scene_characters`` (the scene's character names) feeds the opt-in
    voice-distinctiveness heuristic; it is ignored when the toggle is off.
    """
    if not conversation_history or len(conversation_history) < 2:
        return {
            "composite": 0,
            "dimensions": {k: 0 for k in active_scoring_dimensions()},
            "flags": ["empty_conversation"],
            "scored_at": time.strftime('%Y-%m-%d %H:%M:%S'),
            "method": "none"
        }

    flags = []
    dimension_scores = {}

    # --- Heuristic scoring (always runs) ---
    diversity_score, diversity_flags = _score_diversity(conversation_history)
    technical_score, technical_flags = _score_technical(conversation_history)
    dimension_scores["diversity"] = diversity_score
    dimension_scores["technical"] = technical_score
    flags.extend(diversity_flags)
    flags.extend(technical_flags)

    # Tic density (v11.7, opt-in via quality.tic_dimension): scotoma style tics measured
    # per reply. Heuristic-only (a real measurement, not a judge guess), so no LLM fallback
    # is ever applied to it below.
    tic_active = is_tic_scoring_enabled()
    if tic_active:
        tic_score, tic_flags = _score_tic_density(conversation_history)
        dimension_scores["tic_density"] = tic_score
        flags.extend(tic_flags)

    # --- Roleplay dimensions (always heuristic-scored when the toggle is on; the LLM
    # judge refines them below when available) ---
    roleplay_active = is_roleplay_scoring_enabled()
    if roleplay_active:
        characters = scene_characters or []
        voice_score, voice_flags = _score_voice_distinctiveness(conversation_history, characters)
        grounding_score, grounding_flags = _score_grounding(conversation_history, character_profile_text)
        persona_score, persona_flags = _score_persona_alignment(
            conversation_history, character_profile_text, characters)
        dimension_scores["voice_distinctiveness"] = voice_score
        dimension_scores["grounding"] = grounding_score
        dimension_scores["persona_alignment"] = persona_score
        flags.extend(voice_flags)
        flags.extend(grounding_flags)
        flags.extend(persona_flags)

    # --- LLM scoring (optional, if configured) ---
    use_llm_scoring = global_config.get('quality.use_llm_scoring', False)
    method = "heuristic_only"

    if use_llm_scoring:
        llm_scores, llm_flags, llm_success, _llm_extras = _score_with_llm(
            conversation_history, task_id, thread_id, api_slot_idx, file_name,
            character_profile_text=character_profile_text,
            logic_findings=logic_findings
        )
        drop_reason = _judge_drop_reason(llm_scores, llm_flags, llm_success)
        if drop_reason:
            # v11.4: the judge is the gatekeeper for the dataset. A judge that is still
            # failing after its retries, a reply that is not valid JSON, and a logic score
            # of 0 all DROP the conversation: no score is stored, no review flag is set,
            # and the caller does not write the row. There is no heuristic fallback for a
            # failed judge any more -- a fallback score would silently let an unjudged
            # conversation into the dataset.
            log_message(
                f"Thread {thread_id}: Quality judge dropped {task_id} ({drop_reason}); "
                f"the conversation is not saved to the dataset.", "WARNING")
            return {
                "dropped": True,
                "drop_reason": drop_reason,
                "composite": 0,
                "dimensions": {k: 0 for k in active_scoring_dimensions()},
                "flags": list(llm_flags),
                "scored_at": time.strftime('%Y-%m-%d %H:%M:%S'),
                "method": "dropped",
            }
        if llm_success:
            method = "hybrid"
            for dim in LLM_DIMENSIONS:
                if dim in llm_scores:
                    dimension_scores[dim] = llm_scores[dim]
            # The roleplay judge dimensions refine the heuristic base only when present;
            # the judge may omit them (older models), in which case the heuristic stands.
            if roleplay_active:
                for dim in ROLEPLAY_LLM_DIMENSIONS:
                    if dim in llm_scores:
                        dimension_scores[dim] = llm_scores[dim]
            flags.extend(llm_flags)
        else:
            # No judge ran (not configured): keep the heuristic estimates.
            log_message(f"Thread {thread_id}: LLM quality scoring unavailable for {task_id}. Using heuristic estimates.", "WARNING")
            for dim in LLM_DIMENSIONS:
                dimension_scores[dim] = _heuristic_fallback_for_llm_dim(dim, conversation_history)
            flags.extend(llm_flags)
            flags.append("llm_scoring_unavailable")
    else:
        # No LLM: use heuristic estimates for all dimensions
        for dim in LLM_DIMENSIONS:
            dimension_scores[dim] = _heuristic_fallback_for_llm_dim(dim, conversation_history)
        method = "heuristic_only"

    # --- Compute composite score over the ACTIVE dimensions (generic + roleplay when
    # toggled on). Custom ``quality.weights`` entries scale the per-dimension defaults;
    # every composite is normalized by the total active weight, so partial or unbalanced
    # user configs still produce a meaningful 0-100 score.
    active_dims = active_scoring_dimensions()
    default_weights = dict(SCORING_DIMENSIONS)
    if tic_active:
        default_weights.update(TIC_SCORING_DIMENSIONS)
    if roleplay_active:
        default_weights.update(ROLEPLAY_SCORING_DIMENSIONS)
    custom_weights = global_config.get('quality.weights', {}) or {}

    def _weight(dim):
        try:
            return max(0.0, float(custom_weights.get(dim, default_weights[dim])))
        except (TypeError, ValueError):
            return default_weights[dim]

    total_weight = sum(_weight(d) for d in active_dims)
    if total_weight == 0:
        total_weight = 1.0

    composite = sum(
        dimension_scores.get(dim, 0) * _weight(dim) / total_weight
        for dim in active_dims
    )
    composite = round(composite, 1)

    # --- Threshold flag ---
    min_threshold = global_config.get('quality.min_score_threshold', 50)
    if composite < min_threshold:
        flags.append(f"below_threshold_{min_threshold}")

    return {
        "composite": composite,
        "dimensions": {dim: round(dimension_scores.get(dim, 0), 1) for dim in active_dims},
        "flags": list(set(flags)),
        "scored_at": time.strftime('%Y-%m-%d %H:%M:%S'),
        "method": method
    }


def _score_tic_density(conversation_history):
    """Scores the density of scotoma style tics across the assistant turns (v11.7).

    Heuristic-only; every number comes from ``style_metrics.response_metrics()`` measured
    per assistant turn (no LLM judge, no network). Returns (score 0-100, flags).

    The cut points are DENSITY judgements ported from scotoma's teacher-vs-human arm
    comparisons, not hard rules (plan.md 6.2: ship them, log the raw values, re-calibrate
    from real runs). A clean conversation scores 100; each band that trips subtracts and
    carries its flag so the Review tab can explain the deduction.
    """
    flags = []
    turns = [turn.get("content", "") for turn in conversation_history
             if turn.get("role") == "assistant"]
    rows = [style_metrics.response_metrics(t) for t in turns if (t or "").strip()]
    if not rows:
        return 0, ["no_assistant_text"]

    def turn_fraction(predicate):
        return sum(1 for r in rows if predicate(r)) / len(rows)

    score = 100.0
    # Denial-then-correction couplets: the tic the whole scotoma project was built around.
    if turn_fraction(lambda r: r["denial_correction"] > 0) > 0.20:
        score -= 20
        flags.append("denial_correction_tic")
    # Consecutive negation runs in narration ("She didn't move. She didn't speak.").
    if turn_fraction(lambda r: r["narr_neg_run_ge2"] > 0) > 0.30:
        score -= 15
        flags.append("negation_runs")
    # Comma clause stacking: a mean share of sentences carrying 3+ commas.
    mean_commas_3plus = sum(r["sents_3plus_commas_pct"] for r in rows) / len(rows)
    if mean_commas_3plus > 20:
        score -= 10
        flags.append("comma_clause_stacking")
    # Stacked qualifiers (two comma-appended absolutes in one sentence).
    if turn_fraction(lambda r: r["qualification_stack"] > 0) > 0.15:
        score -= 15
        flags.append("qualification_stack")
    # Formulaic warm/significance closers at the end of turns.
    mean_tail_lift = sum(max(r["tail_affect_lift"], r["tail_sig_lift"]) for r in rows) / len(rows)
    if mean_tail_lift >= 3.0:
        score -= 10
        flags.append("formulaic_warm_closer")
    # Shallow narration: heavy asterisked narration with almost no interiority signal.
    mean_narr_pct = sum(r["narration_pct"] for r in rows) / len(rows)
    mean_interior = sum(r["interior_per_100w"] for r in rows) / len(rows)
    if mean_narr_pct > 40 and mean_interior < 1.0:
        score -= 10
        flags.append("shallow_narration")
    return max(0, min(100, score)), flags


def _score_diversity(conversation_history):
    """Scores vocabulary and structural diversity. Returns (score 0-100, flags)."""
    flags = []
    score = 100.0

    all_text = " ".join(
        turn.get("content", "") for turn in conversation_history
        if turn.get("role") == "assistant"
    )

    if not all_text.strip():
        return 0, ["no_assistant_text"]

    words = re.findall(r'\b\w+\b', all_text.lower())
    if len(words) < 10:
        return 20, ["very_short_response"]

    # Type-token ratio (TTR): unique words / total words
    unique_words = set(words)
    ttr = len(unique_words) / len(words)
    if ttr < 0.3:
        score -= 30
        flags.append("low_vocabulary_diversity")
    elif ttr < 0.45:
        score -= 15
        flags.append("moderate_vocabulary_diversity")

    # Sentence length variance
    sentences = re.split(r'[.!?]+\s*', all_text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 3]
    if len(sentences) >= 3:
        lengths = [len(s.split()) for s in sentences]
        mean_len = sum(lengths) / len(lengths)
        variance = sum((l - mean_len) ** 2 for l in lengths) / len(lengths)
        cv = (variance ** 0.5) / mean_len if mean_len > 0 else 0
        if cv < 0.15:
            score -= 20
            flags.append("uniform_sentence_length")
    else:
        score -= 25
        flags.append("too_few_sentences")

    # Repetition: check for repeated n-grams (3-grams)
    trigrams = set()
    repeated_trigrams = 0
    for i in range(len(words) - 2):
        tri = tuple(words[i:i+3])
        if tri in trigrams:
            repeated_trigrams += 1
        trigrams.add(tri)
    if len(trigrams) > 0 and repeated_trigrams / len(trigrams) > 0.15:
        score -= 20
        flags.append("repetitive_phrases")

    # Paragraph structure
    paragraph_count = all_text.count('\n\n') + 1
    if paragraph_count == 1 and len(words) > 100:
        score -= 10
        flags.append("single_block_paragraph")

    return max(0, min(100, score)), flags


def _score_technical(conversation_history):
    """Scores technical correctness (quotes, formatting, punctuation). Returns (score 0-100, flags)."""
    flags = []
    score = 100.0

    all_text = " ".join(
        turn.get("content", "") for turn in conversation_history
    )

    if not all_text.strip():
        return 0, ["empty_text"]

    # Quote balance (straight + curly)
    straight = all_text.count('"')
    curly_open = all_text.count('\u201c')
    curly_close = all_text.count('\u201d')

    if straight % 2 != 0:
        score -= 30
        flags.append("unbalanced_straight_quotes")
    if curly_open != curly_close:
        score -= 25
        flags.append("unbalanced_curly_quotes")

    # Check for stray asterisks (markdown remnants)
    asterisk_count = all_text.count('*')
    if asterisk_count > 0:
        score -= 15
        flags.append("stray_asterisks")

    # Em dashes (if configured to remove)
    if global_config.get('generation.remove_em_dash', False):
        em_dash_count = all_text.count('\u2014')
        if em_dash_count > 0:
            score -= 10
            flags.append("em_dashes_present")

    # Check for excessive whitespace
    if re.search(r'\t', all_text):
        score -= 10
        flags.append("tab_characters")
    if re.search(r' {3,}', all_text):
        score -= 5
        flags.append("excessive_spaces")

    # Trailing whitespace on lines
    lines = all_text.split('\n')
    trailing_ws = sum(1 for line in lines if line != line.rstrip())
    if trailing_ws > len(lines) * 0.1:
        score -= 5
        flags.append("trailing_whitespace")

    # Check for all-lowercase responses (possible formatting issue)
    assistant_texts = [
        turn.get("content", "") for turn in conversation_history
        if turn.get("role") == "assistant"
    ]
    for text in assistant_texts:
        if text and text.islower() and not any(c in text for c in '.!?'):
            score -= 20
            flags.append("all_lowercase_no_punctuation")
            break

    return max(0, min(100, score)), flags


def _heuristic_fallback_for_llm_dim(dim, conversation_history):
    """
    When LLM scoring is unavailable, provide a rough heuristic estimate
    for the LLM-based dimensions so the composite score is still meaningful.
    """
    all_text = " ".join(
        turn.get("content", "") for turn in conversation_history
        if turn.get("role") == "assistant"
    )
    words = re.findall(r'\b\w+\b', all_text.lower())
    word_count = len(words)

    if dim == "coherence":
        # Proxy: longer, multi-sentence responses tend to be more coherent
        if word_count > 200:
            return 75
        elif word_count > 100:
            return 65
        elif word_count > 50:
            return 55
        return 40

    elif dim == "naturalness":
        # Proxy: check for overly formal or robotic patterns
        robotic_patterns = [r'\bI (am|was|will) (going to|able to|willing to)\b',
                           r'\bIn (order to|the event that|the case where)\b']
        hits = sum(1 for p in robotic_patterns if re.search(p, all_text, re.IGNORECASE))
        return max(30, 85 - hits * 15)

    elif dim == "engagement":
        # Proxy: question marks, exclamation, varied punctuation
        questions = all_text.count('?')
        exclamations = all_text.count('!')
        if questions + exclamations > 3:
            return 80
        elif questions + exclamations > 0:
            return 65
        return 50

    elif dim == "logic":
        # Internal-logic proxy: without a profile the judge's job (contradicting the
        # character card's gender/anatomy/etc.) can't be done, so fall back to a mild
        # positive tilt rather than guessing. The real check is LLM-only -- when the LLM
        # is off, ``score_conversation`` still records a heuristic number for the
        # composite, and the per-turn rewrite gate (``check_logic``) simply does not fire.
        # A few generic impossibility markers get flagged so the heuristic is not flat:
        impossible_markers = [
            r'\bher penis\b', r'\bhis vagina\b', r'\bhis breast\b', r'\bher testicle',
            r'\bpenis.{0,25}\bvagina\b|\bvagina.{0,25}\bpenis\b',
            r'\bthree legs?\b|\bfour arms?\b|\btwo heads?\b',
        ]
        hits = sum(1 for p in impossible_markers if re.search(p, all_text.lower()))
        return max(30, 72 - hits * 10)

    elif dim == "consistency":
        # Persona-fidelity proxy: flag well-known OOC drift markers (helpful-bot
        # narration, breaking the fourth wall) instead of returning a flat 70.
        # Real character-card comparison requires the LLM judge; this keeps the
        # heuristic fallback from being pure noise.
        ooc_markers = [
            r'\bas a helpful ai\b',
            r'\bas an? (ai|assistant)\b',
            r"\bi'm an? ai\b",
            r"\bi am an? ai\b",
            r'\blet me know if there.{0,4}s anything else\b',
            r'\bis there anything else i can help\b',
            r"\byou're welcome\b",
            r'\bi hope this helps\b',
            r'\bas \w+, i would\b',
        ]
        hits = sum(1 for p in ooc_markers if re.search(p, all_text.lower()))
        if hits >= 3:
            return max(20, 55 - hits * 10)
        elif hits == 2:
            return 45
        elif hits == 1:
            return 60
        # No OOC markers: a mild positive tilt vs. the old flat 70.
        return 72

    return 60  # Default fallback


# --- Roleplay-dimension heuristics (opt-in; see ROLEPLAY_SCORING_DIMENSIONS) ---

_QUOTED_SPANS_RE = re.compile(u'\u201c(.+?)\u201d|\u2018(.+?)\u2019|"(.+?)"|\'(.+?)\'', re.DOTALL)
_STOPWORDS = frozenset((
    'the', 'and', 'a', 'an', 'of', 'in', 'on', 'at', 'to', 'for', 'with', 'by', 'from',
    'is', 'are', 'was', 'were', 'be', 'been', 'being', 'it', 'its', 'his', 'her', 'their',
    'our', 'your', 'my', 'he', 'she', 'they', 'we', 'you', 'i', 'this', 'that', 'these',
    'those', 'not', 'no', 'but', 'or', 'as', 'if', 'then', 'than', 'so', 'just', 'really',
    'said', 'says', 'say', 'asked', 'asks', 'ask', 'replied', 'replies', 'reply',
    'told', 'tell', 'whispered', 'whispers', 'murmured', 'murmurs', 'called', 'calls',
    'shouted', 'shouts', 'laughed', 'laughes', 'smiled', 'smiles', 'nodded', 'nods',
    'head', 'face', 'eyes', 'eye', 'voice', 'looked', 'looks', 'look', 'turned', 'turns',
    'into', 'out', 'up', 'down', 'over', 'under', 'about', 'there', 'here', 'now', 'did',
    'do', 'does', 'have', 'has', 'had', 'will', 'would', 'could', 'should', 'may', 'might',
))


def _spoken_lines_by_character(conversation_history, scene_characters):
    """Attributes quoted lines to scene characters for the voice-distinctiveness heuristic.

    A line counts for a character when the narration around it names them:
      * an explicit speaker prefix before the quote (``Alice:"..."``), or
      * the character's name inside the ~60 chars of narration immediately before or after
        the quoted span (``Alice said, "..."`` / ``"...", she whispered`` style proximity).
    Lines that match no character stay unattributed (excluded from the overlap math).
    Returns (lines_by_char, total_attribution_ratio).
    """
    names = [str(n).strip() for n in (scene_characters or []) if str(n).strip()]
    lower_names = [n.lower() for n in names]
    lines_by_char = {n: [] for n in names}
    attributed = 0
    total = 0
    for turn in conversation_history:
        if turn.get("role") != "assistant":
            continue
        content = turn.get("content", "") or ""
        for m in _QUOTED_SPANS_RE.finditer(content):
            span = next((g for g in m.groups() if g is not None), "").strip()
            if not span:
                continue
            total += 1
            matched_name = None
            # An explicit speaker prefix sits in the narration just before the opening
            # quote (``Alice: "..."`` / ``Alice's voice rang out, "..."``).
            before = content[max(0, m.start() - 30):m.start()]
            prefix = re.match(r'^(?:([A-Z][\w\'\-]{1,30})\s*(?:\u2019|\')?\s*[:\u2019\'\"]\s*)$', before)
            if prefix and prefix.group(1).lower() in lower_names:
                matched_name = next(n for n in names if n.lower() == prefix.group(1).lower())
            else:
                # Otherwise attribute by the character whose name sits CLOSEST to the quote
                # in the surrounding narration (~60 chars before / ~40 chars after), so a
                # line like 'Alice said, "..." Bob replied, "..."' splits correctly.
                before = content[max(0, m.start() - 60):m.start()].lower()
                after = content[m.end():min(len(content), m.end() + 40)].lower()
                best_dist = None
                for n, ln in zip(names, lower_names):
                    pat = re.compile(r'\b' + re.escape(ln) + r'\b')
                    cand = None
                    mb = [mm for mm in pat.finditer(before)]
                    if mb:
                        cand = m.start() - (mb[-1].start() + max(0, m.start() - 60))
                    ma = [mm for mm in pat.finditer(after)]
                    if ma:
                        d = ma[0].end() + 40
                        if cand is None or d < cand:
                            cand = d
                    if cand is not None and (best_dist is None or cand < best_dist):
                        best_dist = cand
                        matched_name = n
            if matched_name:
                lines_by_char[matched_name].append(span)
                attributed += 1
    ratio = (attributed / total) if total else 0.0
    return lines_by_char, ratio


def _bigram_set(tokens):
    return {tuple(tokens[i:i + 2]) for i in range(len(tokens) - 1)}


def _jaccard(a, b):
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _score_voice_distinctiveness(conversation_history, scene_characters):
    """Scores whether multiple characters sound different from each other.

    Heuristic: 2-gram (bigram) overlap between the distinct characters' attributed
    spoken lines. High pairwise Jaccard overlap means interchangeable voices. Only
    meaningful for multi-character scenes -- with fewer than two attributed characters
    it returns a neutral score and a flag instead of guessing.
    Returns (score 0-100, flags).
    """
    lines_by_char, ratio = _spoken_lines_by_character(conversation_history, scene_characters)
    voiced = {n: _bigram_set(re.findall(r'\b\w+\b', " ".join(ls).lower())) for n, ls in lines_by_char.items()}
    voiced = {n: g for n, g in voiced.items() if len(g) >= 10}

    if len(voiced) < 2:
        if ratio < 0.5 and any(lines_by_char.values()):
            return 55, ["undifferentiated_multi_character"]
        return 70, ["insufficient_multi_character_dialogue"]

    grams = list(voiced.values())
    pair_overlaps = []
    for i in range(len(grams)):
        for j in range(i + 1, len(grams)):
            pair_overlaps.append(_jaccard(grams[i], grams[j]))
    avg_overlap = sum(pair_overlaps) / len(pair_overlaps)

    score = 100.0
    flags = []
    if avg_overlap > 0.5:
        score -= 45
        flags.append("interchangeable_character_voices")
    elif avg_overlap > 0.3:
        score -= 20
        flags.append("similar_character_voices")
    if ratio < 0.5:
        flags.append("partial_line_attribution")
    return max(0, min(100, score)), flags


def _profile_keywords(profile_text):
    """Extracts concrete setting/backstory keywords from the character card block(s).

    Keeps the frequent content words (length >= 5, non-stopword); those are the details
    a grounded response should echo rather than generic filler.
    """
    text = (profile_text or "").lower()
    words = re.findall(r"\b\w{5,}\b", text)
    freq = {}
    for w in words:
        if w in _STOPWORDS or not any(c.isalpha() for c in w):
            continue
        freq[w] = freq.get(w, 0) + 1
    ranked = sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))
    return [w for w, _ in ranked[:40]]


def _score_grounding(conversation_history, character_profile_text):
    """Scores whether the response references setting/backstory details rather than filler.

    Heuristic proxy: how many distinct profile keywords (names, places, objects, era terms)
    appear anywhere in the conversation. The LLM judge refines this against the profile
    when available; the heuristic keeps it meaningful in heuristic-only mode.
    Returns (score 0-100, flags).
    """
    profile = (character_profile_text or "").strip()
    if len(profile) < 80:
        # No card to ground against (e.g. lore-only task): neutral, do not punish.
        return 70, ["no_profile_for_grounding"]
    keywords = _profile_keywords(profile)
    if not keywords:
        return 70, ["no_profile_for_grounding"]
    all_text = (" ".join(turn.get("content", "") or "" for turn in conversation_history)).lower()
    hits = sum(1 for kw in keywords if kw in all_text)
    fraction = hits / len(keywords)
    if fraction >= 0.20:
        score, flags = 90, []
    elif fraction >= 0.10:
        score, flags = 75, []
    elif fraction >= 0.04:
        score, flags = 60, ["weak_grounding"]
    elif fraction >= 0.01:
        score, flags = 45, ["generic_filler_response"]
    else:
        score, flags = 30, ["generic_filler_response", "ungrounded_scene"]
    return max(0, min(100, score)), flags


def _persona_sections(profile_text):
    """Splits the character card into the Persona and Personality (outward behavior)
    section texts, as lowercased keyword lists (frequent content words, len >= 5)."""
    profile = (profile_text or "").upper()
    if not profile:
        return [], []
    persona_kws = []
    personality_kws = []
    chunks = re.split(r'---\s*CHARACTER\s+\d+\s+PROFILE\s*---', profile)
    for chunk in chunks[1:]:
        p_end = chunk.find('--- END')
        body = chunk if p_end == -1 else chunk[:p_end]
        def _section(label):
            m = re.search(re.escape(label) + r':\s*(.*?)(?:\n[A-Z][A-Z /\(\)]*:|\n---)', body, re.DOTALL)
            return m.group(1) if m else ""
        for kw in _profile_keywords(_section('PERSONA')):
            if kw not in persona_kws:
                persona_kws.append(kw)
        for kw in _profile_keywords(_section('PERSONALITY (OUTWARD BEHAVIOR)')):
            if kw not in personality_kws:
                personality_kws.append(kw)
    return persona_kws, personality_kws


def _score_persona_alignment(conversation_history, character_profile_text, scene_characters):
    """Scores whether the outward personality vs. inner persona split was handled correctly.

    The engine deliberately withholds the inner ``persona`` until the configured
    breakthrough slot (``prompts.character.persona_breakthrough_turn``), so a faithful
    conversation shows outward behaviour early and inner-persona signals only AFTER the
    breakthrough turn -- curt on the surface, tender under pressure. Signals are the
    profile's persona keywords: appearing late-only is the ideal (surface first, mask
    slipping later); appearing EARLY (before the breakthrough slot) means the model
    leaked the withheld profile; never appearing means the persona was configured but
    left unexplored. With no persona fields configured it degrades to a mild
    emotional-range check instead of guessing. Returns (score 0-100, flags).
    """
    persona_kws, _ = _persona_sections(character_profile_text)
    if not persona_kws:
        text = " ".join(t.get("content", "") or "" for t in conversation_history if t.get("role") == "assistant")
        dynamic = text.count('!') + text.count('?')
        return (68 if dynamic > 2 else 55), ["no_inner_persona_to_align"]

    try:
        cutoff = max(1, int(global_config.get('prompts.character.persona_breakthrough_turn', 2)))
    except (TypeError, ValueError):
        cutoff = 2
    assistant_turns = [t for t in conversation_history if t.get("role") == "assistant"]
    # Outward phase: everything strictly BEFORE the breakthrough slot (the breakthrough
    # slot itself is where the persona is allowed to start surfacing).
    early = " ".join(t.get("content", "") or "" for t in assistant_turns[:cutoff - 1]).lower()
    late = " ".join(t.get("content", "") or "" for t in assistant_turns[cutoff - 1:]).lower()
    early_hits = {kw for kw in persona_kws if kw in early}
    late_hits = {kw for kw in persona_kws if kw in late}
    late_only = late_hits - early_hits

    if not early_hits and not late_hits:
        # Persona was configured but never surfaced, even after breakthrough.
        return 55, ["no_inner_persona_signals"]

    score = 75.0
    flags = []
    if len(late_only) >= 2:
        score = 92
    elif len(late_only) == 1:
        score = 80
    if early_hits and not late_hits:
        # Inner-persona talk only in the outward-behaviour phase: the model leaked the
        # withheld profile before its breakthrough slot and never followed through.
        score = min(score, 60)
        flags.append("early_inner_persona_leak")
    elif early_hits:
        # Persona surfaced both early and late: partial leak, the split muddied.
        score -= 10
        flags.append("early_inner_persona_leak")
    return max(0, min(100, score)), flags


def _score_with_llm(conversation_history, task_id, thread_id, api_slot_idx, file_name="",
                    character_profile_text="", logic_findings=None):
    """
    Calls a dedicated LLM to score the conversation on LLM-based dimensions.
    Uses API Slot 7 (index 6) if configured, otherwise the primary API.

    ``character_profile_text`` is the scene's character card (the ``character_injection``
    block: name, personality, traits, appearance, setting, etc.). When provided, the judge
    scores the conversation's consistency against that actual profile -- persona, voice,
    tone, and stated traits -- rather than a generic "is it consistent?" check. Without it,
    the consistency dimension cannot measure character fidelity at all, so passing it in is
    the single biggest quality win for roleplay data.

    ``logic_findings`` (optional) is the list of per-turn findings recorded by
    ``check_logic`` during this conversation; when non-empty they are appended to the
    judge prompt as a FINDINGS block so the conversation-level score takes them into
    account without having to re-derive them from scratch.

    Returns (scores_dict, flags, success_bool, extras_dict); extras carries the
    non-score fields (``ooc_evidence``, ``logic_evidence``, ``rewrite_instruction``,
    ``notes``) parsed out of the judge's JSON reply -- empty dict on failure.
    """
    # Determine which API to use for scoring
    scoring_api_config = global_config.get('quality.scoring_api', {})
    api_url = scoring_api_config.get('url') or global_config.get('api.apis.0.url', '')
    model_name = scoring_api_config.get('model') or global_config.get('api.apis.0.model', '')
    api_key = scoring_api_config.get('key') or global_config.get('api.apis.0.key', '')
    scoring_api_slot = scoring_api_config.get('slot_idx', 6)  # Default to slot 7
    sampler_settings = scoring_api_config.get('sampler_settings', {}) or {}

    if not api_url or not model_name:
        log_message(f"Thread {thread_id}: Quality LLM not configured. Skipping LLM scoring.", "WARNING")
        return {}, ["llm_not_configured"], False, {}

    # Build the scoring prompt
    conversation_text = _format_conversation_for_scoring(conversation_history)
    max_chars = global_config.get('quality.max_chars_for_scoring', 8000)
    conversation_text = _truncate_head_tail(conversation_text, max_chars)

    # The character card, when available, is what makes "consistency" measurable as
    # persona fidelity. Truncate defensively so a very long multi-character scene cannot
    # blow out the judge's context.
    profile_block = (character_profile_text or "").strip()
    max_profile_chars = global_config.get('quality.max_profile_chars_for_scoring', 4000)
    if len(profile_block) > max_profile_chars:
        profile_block = profile_block[:max_profile_chars] + "\n[...profile truncated...]"

    dimensions_text = (
        "DIMENSIONS:\n"
        "- coherence: Does the conversation flow logically? Do responses follow naturally from prompts?\n"
        "- naturalness: Does the dialogue sound like real human conversation? Avoid robotic/formulaic language.\n"
        "- engagement: Is the content interesting, vivid, and compelling? Does it hold attention?\n"
        "- consistency: Persona fidelity AND internal consistency. Check the FULL conversation turn by turn -- "
        "drift usually creeps in on later turns, so weigh late responses at least as heavily as early ones. "
        "Score HOW WELL the assistant stays in character -- voice, tone, phrasing, stated personality and traits, "
        "and world details -- and whether anything contradicts the character profile(s) below (e.g. a stoic "
        "character becoming exuberant, a character suddenly knowing what the user is thinking, narration breaks "
        "like \"As a helpful AI, I would...\", customer-service closers such as \"Is there anything else I can help "
        "you with?\", or dropping into second-person advice). Quoted dialogue (\"...\") is the character speaking "
        "and is judged on fidelity to the profile; unquoted narrator text must also stay in-scene -- god-viewing "
        "the user's thoughts/actions and fourth-wall drops are out of character. Penalise hard for out-of-character "
        "drift: each distinct OOC moment should visibly pull the score down.\n"
        "- logic: Internal-logic / factual coherence of the ASSISTANT's output. Hunt concrete contradictions, "
        "not style: sexed anatomy that does not match a character's stated gender (e.g. a female character "
        "described with a penis, or a male character with a vagina), body parts, scars, injuries, clothing or "
        "appearance that contradict the profile(s) below or the character's own earlier lines, and impossible "
        "or self-contradicting statements within the conversation (an object both present and absent, two "
        "mutually-exclusive times or locations, a dead/absent person acting, quantities that change between "
        "sentences without cause). Quoted dialogue counts too. Do NOT penalise style, pacing, or in-scene "
        "character behaviour -- only objective impossibilities and self-/profile-contradictions. Score 100 when "
        "the output holds together factually; each distinct contradiction should pull it down sharply. If the "
        "judge flagged specific per-turn logic issues in the FINDINGS block below, incorporate them: the turn(s) "
        "named there count as confirmed findings unless the surrounding text clearly contradicts the finding.\n"
    )
    if logic_findings:
        findings_lines = []
        for f in logic_findings[:8]:
            findings_lines.append(f"  * turn {f.get('turn','?')}: {f.get('evidence','(no description)')}")
        dimensions_text += (
            "\nFINDINGS (per-turn logic gate already run on this conversation; treat these as confirmed "
            "unless the text obviously refutes them):\n" + "\n".join(findings_lines) + "\n"
        )
    if profile_block:
        consistency_context = (
            "\n\nSCORE consistency AGAINST THE CHARACTER PROFILE(S) BELOW. If the assistant's voice, "
            "tone, stated traits, or described world details contradict the profile, lower the "
            "consistency score accordingly.\n\n"
            f"CHARACTER PROFILE(S):\n{profile_block}\n"
        )
    else:
        consistency_context = (
            "\n\n(No character profile was supplied. Judge consistency only on internal "
            "self-consistency of the conversation.)\n"
        )

    scoring_prompt = (
        "You are a quality evaluator for synthetic roleplay dialogue data. "
        "Score the following conversation on each dimension from 0 to 100.\n\n"
        + dimensions_text + consistency_context +
        "Respond ONLY with a JSON object, no other text. Format:\n"
        '{"coherence": <0-100>, "naturalness": <0-100>, "engagement": <0-100>, "consistency": <0-100>, '
        '"logic": <0-100>, '
        '"ooc_evidence": ["<very short description of each distinct out-of-character moment, or [] if there are none; avoid quotation marks>"], '
        '"logic_evidence": ["<very short description of each distinct logic/factual contradiction found in the assistant output, or [] if there are none; avoid quotation marks>"], '
    )

    if is_roleplay_scoring_enabled():
        scoring_prompt += (
            "- voice_distinctiveness: Do multiple characters sound DIFFERENT from each other -- "
            "distinct vocabulary, sentence rhythm, catchphrases and speech habits? If voices are "
            "interchangeable (swap a line and nobody would notice), score low. Single-character "
            "scenes: score on whether the voice is at least vividly individual.\n"
            "- grounding: Does the dialogue and narration reference the SPECIFIC setting, people, "
            "objects and backstory details from the profile(s) below, rather than generic filler "
            "that could happen anywhere? Score how concretely anchored the scene is to its world.\n"
            "- persona_alignment: The profiles may carry an OUTWARD personality and a WITHHELD inner "
            "persona (revealed later under pressure). Does the character's behaviour follow that split -- "
            "e.g. curt on the surface but tender once the mask slips -- without the inner self leaking "
            "in too early or never appearing at all?\n"
            '"voice_distinctiveness": <0-100>, "grounding": <0-100>, "persona_alignment": <0-100>, \n'
        )

    scoring_prompt += (
        '"notes": "<one sentence summary>"}\n\n'
        f"CONVERSATION:\n{conversation_text}"
    )
    _judge_dims = list(LLM_DIMENSIONS) + (
        list(ROLEPLAY_LLM_DIMENSIONS) if is_roleplay_scoring_enabled() else [])
    scores, flags, success, extras = _call_logic_judge(
        api_url, model_name, api_key, scoring_api_slot, thread_id, task_id, file_name,
        sampler_settings, scoring_prompt, expected_dims=_judge_dims,
    )
    return scores, flags, success, extras


# --- Judge reply parsing (v11.5) -------------------------------------------------------
# Small local judges (Qwen 3.5 4B and friends) usually produce the right *evaluation* but
# wrap it in prose, a thinking block, a code fence, a truncated tail, or "85/100"-style
# strings. Dropping those conversations throws away good data, so parsing is now a ladder:
#
#   1. extract the JSON object (direct -> balanced scan -> repair -> lenient -> salvage)
#   2. if nothing parses, ask the SAME judge once more to re-emit the evaluation as JSON
#      (quality.judge_json_repair_attempts, default 1) -- never a silent drop first
#   3. only then is the reply "invalid JSON" and the conversation dropped
#
# A truncated reply (finish_reason == "length") is treated as TRANSIENT and retried with a
# doubled token budget, because a cut-off tail is the most common cause of the
# "Unterminated string" JSON errors. ``quality.judge_structured_output`` can additionally
# ask the server for grammar-constrained JSON (with automatic fallback for servers that do
# not support response_format).

_REASONING_BLOCK_RE = re.compile(
    r'<(think|thinking|reasoning|analysis|scratchpad)>[\s\S]*?</\1\s*>', re.IGNORECASE)
_UNCLOSED_REASONING_RE = re.compile(
    r'<(think|thinking|reasoning|analysis|scratchpad)>[\s\S]*$', re.IGNORECASE)
_CODE_FENCE_RE = re.compile(r'```[A-Za-z0-9_+-]*')

# Servers that rejected ``response_format`` once; remembered for the rest of the run so the
# fallback request is not repeated on every conversation.
_JUDGE_NO_RESPONSE_FORMAT = set()

JUDGE_SYSTEM_PROMPT = (
    "You are a strict JSON-only API. Respond with ONLY a single valid JSON object. "
    "No markdown, no code fences, no explanations, no extra text -- just raw JSON. "
    "Rules: every key and every string uses double quotes; every string is closed before the "
    "line ends; numbers are bare numbers (85, not \"85\"); booleans are true/false; no trailing "
    "commas. Never put double quotes inside a string value -- paraphrase quoted dialogue instead."
)


def judge_max_tokens(dims=None, sampler_settings=None):
    """Token budget for one judge request.

    A reply cut off mid-string is the most common cause of "Unterminated string" JSON errors,
    so the budget is configurable (``quality.judge_max_tokens``, 0 = automatic) and a
    truncated reply doubles it for the next attempt. The automatic value keeps the historical
    ``512 + 64``-per-extra-dimension formula so default behaviour is unchanged.
    """
    sampler = sampler_settings or {}
    try:
        explicit = int(sampler.get('max_tokens', 0) or 0)
    except (TypeError, ValueError):
        explicit = 0
    if explicit > 0:
        return explicit
    try:
        configured = int(global_config.get('quality.judge_max_tokens', 0) or 0)
    except (TypeError, ValueError):
        configured = 0
    if configured > 0:
        return max(64, min(8192, configured))
    dims = list(dims or LLM_DIMENSIONS)
    return 512 + max(0, len(dims) - len(LLM_DIMENSIONS)) * 64


def judge_request_timeout(config=None):
    """HTTP timeout for one judge request.

    A local 4B judge writing a full evaluation plus evidence lists regularly needs more than the
    old hard-coded 60 s, and a timeout used to burn a whole transient-retry cycle. ``quality.
    judge_request_timeout`` (default 120 s) wins over the legacy ``quality.scoring_timeout``
    (still honoured when set), and the value is clamped to 5-1800 s.
    """
    cfg = global_config if config is None else config
    for key in ('quality.judge_request_timeout', 'quality.scoring_timeout'):
        try:
            value = cfg.get(key)
        except AttributeError:
            value = None
        if value in (None, ''):
            continue
        try:
            return max(5.0, min(1800.0, float(value)))
        except (TypeError, ValueError):
            continue
    return 120.0


def judge_json_repair_attempts(config=None):
    """How many times an unparsable judge reply is re-asked for valid JSON.

    ``quality.judge_json_repair_attempts`` (default 1, clamped to 0-3). This is NOT the
    transient-failure retry count: it is a dedicated "format repair" round that sends the
    model's own broken reply back and asks for the same evaluation as JSON. 0 disables it,
    restoring the v11.4 behaviour where invalid JSON drops immediately.
    """
    cfg = global_config if config is None else config
    try:
        attempts = int(cfg.get('quality.judge_json_repair_attempts', 1))
    except (TypeError, ValueError):
        attempts = 1
    return max(0, min(3, attempts))


def judge_structured_output(config=None):
    """How (whether) the judge request asks the server for constrained JSON.

    ``quality.judge_structured_output``: ``auto`` (default -- send ``response_format``
    ``json_object`` and fall back for servers that reject it), ``json_object``,
    ``json_schema``, or ``off`` (v11.4 behaviour: no ``response_format`` at all).
    """
    cfg = global_config if config is None else config
    mode = str(cfg.get('quality.judge_structured_output', 'auto') or 'auto').strip().lower()
    return mode if mode in ('off', 'json_object', 'json_schema', 'auto') else 'auto'


# vLLM priority scheduling for the judge request (v11.8).
#
# vLLM's OpenAI-compatible server accepts a top-level ``priority`` integer: lower values are
# handled earlier, so **-10 is the highest priority** (it jumps the queue) and **10 is the
# lowest**. 0 is the server's own default. The range exposed here is deliberately narrow
# (-10..10) rather than vLLM's full int64 span -- a handful of steps either side of the default
# is all a judge needs, and a tight bounded field cannot be mis-typed into a runaway value.
# Any priority OTHER THAN 0 makes a server that does not use priority scheduling reject the
# request outright, and other backends (OpenAI, Anthropic, llama.cpp) reject the unknown field
# entirely. So the field is opt-in twice over: a toggle decides whether it is sent at all, and the
# value defaults to 0 (the vLLM default, accepted everywhere, including by servers without
# priority scheduling).
JUDGE_PRIORITY_MIN = -10
JUDGE_PRIORITY_MAX = 10


def judge_priority_enabled(config=None):
    """Whether the judge request should carry a vLLM ``priority`` field.

    ``quality.judge_priority_enabled`` (default false). Off means the field is NEVER sent, so
    backends without priority support see a payload identical to v11.7.
    """
    cfg = global_config if config is None else config
    try:
        value = cfg.get('quality.judge_priority_enabled', False)
    except AttributeError:
        return False
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes', 'on')
    return bool(value)


def judge_priority(config=None):
    """The vLLM ``priority`` value to send (-10 to 10, lower = earlier handling).

    ``quality.judge_priority`` (default 0) is clamped to -10..10 rather than raising: a bad
    config value must not break a run. **-10 is the highest priority** (handled ahead of
    everything else), 0 is the vLLM default, 10 the lowest. Only meaningful when
    ``judge_priority_enabled()`` is true -- the caller must check that before adding the field.
    """
    cfg = global_config if config is None else config
    try:
        value = cfg.get('quality.judge_priority', 0)
    except AttributeError:
        value = 0
    try:
        priority = int(value)
    except (TypeError, ValueError):
        priority = 0
    return max(JUDGE_PRIORITY_MIN, min(JUDGE_PRIORITY_MAX, priority))


def _judge_priority_field():
    """The payload fragment for the judge request, or ``{}`` when the toggle is off."""
    if not judge_priority_enabled():
        return {}
    return {"priority": judge_priority()}


def _judge_response_format(mode, dims):
    """Build the ``response_format`` payload for a judge request (or None)."""
    if mode == 'off':
        return None
    if mode == 'json_schema':
        properties = {}
        for dim in dims:
            properties[dim] = {"type": "integer", "minimum": 0, "maximum": 100}
        properties["ooc_evidence"] = {"type": "array", "items": {"type": "string"}}
        properties["logic_evidence"] = {"type": "array", "items": {"type": "string"}}
        properties["rewrite_instruction"] = {"type": "string"}
        properties["notes"] = {"type": "string"}
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "quality_scores",
                "strict": False,
                "schema": {
                    "type": "object",
                    "properties": properties,
                    "required": list(dims),
                    "additionalProperties": True,
                },
            },
        }
    # json_object, and the "auto" default: the widest-supported form.
    return {"type": "json_object"}


def _looks_like_response_format_rejection(response):
    """True when a non-200 reply looks like the server refusing ``response_format``."""
    try:
        if response is None or response.status_code == 200:
            return False
        body = str(getattr(response, 'text', '') or '')[:4000].lower()
    except Exception:
        return False
    return ('response_format' in body or 'json_schema' in body or 'json_object' in body)


def _record_judge_response_time(slot, seconds):
    """Track judge latency without disturbing the per-slot bookkeeping."""
    try:
        with api_response_times_lock:
            if 0 <= slot < len(api_response_times_per_slot):
                times = api_response_times_per_slot[slot]
                times.append(seconds)
                if len(times) > MAX_RESPONSE_TIMES_TO_TRACK:
                    del times[:-MAX_RESPONSE_TIMES_TO_TRACK]
    except Exception:
        pass


def _strip_judge_wrapping(content):
    """Remove thinking blocks, code fences and prose padding around a JSON reply."""
    text = content if isinstance(content, str) else str(content or "")
    text = _REASONING_BLOCK_RE.sub(' ', text)
    text = _UNCLOSED_REASONING_RE.sub(' ', text)
    text = _CODE_FENCE_RE.sub(' ', text)
    return text.strip()


def _json_objects_in(text):
    """Yield every complete JSON value that starts at a ``{`` or ``[`` in ``text``."""
    decoder = json.JSONDecoder()
    index = 0
    length = len(text)
    while index < length:
        if text[index] not in '{[':
            index += 1
            continue
        try:
            value, end = decoder.raw_decode(text, index)
        except ValueError:
            index += 1
            continue
        yield value
        index = end if end > index else index + 1


def _lenient_json(content):
    """Normalize near-JSON: smart quotes, trailing commas, Python literals."""
    text = content
    for src, dst in (('\u201c', '"'), ('\u201d', '"'), ('\u2018', "'"),
                     ('\u2019', "'"), ('\u00a0', ' ')):
        text = text.replace(src, dst)
    text = re.sub(r',\s*([}\]])', r'\1', text)
    text = re.sub(r'(?<![\w"])(True|False|None)(?![\w"])',
                  lambda m: {'True': 'true', 'False': 'false', 'None': 'null'}[m.group(1)],
                  text)
    return text


def _repair_truncated_json(content, max_steps=800):
    """Close a reply that was cut off mid-member (token limit, dropped connection).

    The tail is trimmed back to the last complete member and the enclosing object/array is
    closed, so ``{"coherence": 80, "naturalness": 78, "engag`` still yields the scores the
    model had already committed to instead of being thrown away. Returns the repaired text or
    None when no valid object can be built.
    """
    body = (content or '').strip()
    if not body:
        return None
    stop = max(0, len(body) - max_steps)
    for end in range(len(body), stop, -1):
        chunk = body[:end].rstrip()
        if not chunk or chunk[-1] in '{[,:':
            continue
        chunk = re.sub(r',\s*$', '', chunk)
        chunk += ']' * max(0, chunk.count('[') - chunk.count(']'))
        chunk += '}' * max(0, chunk.count('{') - chunk.count('}'))
        try:
            value = json.loads(chunk)
        except ValueError:
            continue
        if isinstance(value, dict) and value:
            return chunk
    return None


def _coerce_score(value, default=50.0):
    """Coerce a judge value into a float, tolerating ``"85"``, ``"85/100"`` and prose."""
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r'-?\d+(?:\.\d+)?', str(value if value is not None else ''))
    if not match:
        return default
    try:
        return float(match.group(0))
    except ValueError:
        return default


def _extract_json_object(content, preferred_keys=None):
    """Find the judge's JSON object in a messy reply.

    Returns ``(dict_or_None, how)`` where ``how`` is one of ``object``, ``direct``,
    ``scan``, ``repaired``, ``lenient`` or ``unparseable``. ``preferred_keys`` (the expected
    score dimensions) lets a candidate object that actually carries scores win over an
    unrelated ``{...}`` that happened to appear earlier in the prose.
    """
    if isinstance(content, dict):
        return content, "object"
    if not isinstance(content, str) or not content.strip():
        return None, "unparseable"

    text = _strip_judge_wrapping(content)
    preferred = set(preferred_keys or ())

    try:
        direct = json.loads(text)
        if isinstance(direct, dict) and direct:
            return direct, "direct"
    except ValueError:
        pass

    def _pick(candidates, how):
        fallback = None
        for candidate in candidates:
            if not isinstance(candidate, dict) or not candidate:
                continue
            if preferred and preferred.intersection(candidate.keys()):
                return candidate, how
            if fallback is None:
                fallback = candidate
        return (fallback, how) if fallback is not None else (None, None)

    found, how = _pick(_json_objects_in(text), "scan")
    if found is not None:
        return found, how

    for label, candidate_text in (("repaired", _repair_json(text)),
                                  ("lenient", _lenient_json(text)),
                                  ("truncated", _repair_truncated_json(text))):
        if not candidate_text or candidate_text == text:
            continue
        try:
            parsed = json.loads(candidate_text)
            if isinstance(parsed, dict) and parsed:
                return parsed, label
        except ValueError:
            pass
        found, how = _pick(_json_objects_in(candidate_text), label)
        if found is not None:
            return found, how

    return None, "unparseable"


def _salvage_scores_from_text(text, dims):
    """Last-resort numeric recovery from a reply that is not parseable JSON at all.

    Every requested dimension must be present as ``"dim": <number>`` for the salvage to
    count, so a partial or hallucinated fragment can never be mistaken for a full
    evaluation. Callers treat a successful salvage as a normal (kept) verdict.
    """
    dims = list(dims or [])
    if not text or not dims:
        return {}
    cleaned = _strip_judge_wrapping(text)
    found = {}
    for dim in dims:
        match = re.search(r'["\']?%s["\']?\s*[:=]\s*["\']?\s*(-?\d+(?:\.\d+)?)'
                          % re.escape(dim), cleaned, re.IGNORECASE)
        if not match:
            return {}
        try:
            found[dim] = float(match.group(1))
        except ValueError:
            return {}
    return found


def _repair_json(content):
    """Attempts to repair common JSON issues from LLM output.

    LLMs often produce malformed JSON with:
    - Unescaped quotes within string values
    - Trailing commas
    - Missing closing braces/brackets
    - Newlines within strings

    This function applies targeted repairs while preserving the actual content. It is one
    rung of the ladder in ``_extract_json_object``: ``_json_objects_in`` already recovers
    well-formed objects from surrounding prose, so this only handles genuinely broken text.
    """
    if not content:
        return content

    content = _strip_judge_wrapping(content)

    # Remove trailing commas before } or ]
    content = re.sub(r',\s*([\]}])', r'\1', content)

    # Try to find the JSON object boundaries
    json_match = re.search(r'\{[\s\S]*\}', content)
    if json_match:
        content = json_match.group(0)

    # Fix unescaped quotes within string values
    # Strategy: parse character by character, tracking whether we're in a string
    result = []
    in_string = False
    escaped = False
    i = 0

    while i < len(content):
        char = content[i]

        if escaped:
            result.append(char)
            escaped = False
            i += 1
            continue

        if char == '\\':
            result.append(char)
            escaped = True
            i += 1
            continue

        if char == '"' and not in_string:
            in_string = True
            result.append(char)
            i += 1
            continue

        if char == '"' and in_string:
            # Check if this quote should be escaped
            # Look ahead: if next non-space char is : or , or } or ], this quote ends the string
            # Otherwise, it might be an unescaped quote within the string
            look_ahead = i + 1
            while look_ahead < len(content) and content[look_ahead] in ' \t\n\r':
                look_ahead += 1

            if look_ahead < len(content):
                next_char = content[look_ahead]
                if next_char not in ':,}]':
                    # This quote is likely within the string - escape it
                    result.append('\\"')
                    i += 1
                    continue

            in_string = False
            result.append(char)
            i += 1
            continue

        # Handle newlines within strings - escape them
        if char == '\n' and in_string:
            result.append('\\n')
            i += 1
            continue

        result.append(char)
        i += 1

    repaired = ''.join(result)

    # Close any unclosed strings, arrays, or objects
    # Count braces and brackets to determine what needs closing
    open_braces = repaired.count('{') - repaired.count('}')
    open_brackets = repaired.count('[') - repaired.count(']')

    # If we're still in a string (odd number of quotes), close it
    quote_count = repaired.count('"') - repaired.count('\\"')
    if quote_count % 2 != 0:
        repaired += '"'

    # Close any open brackets
    repaired += ']' * max(0, open_brackets)

    # Close any open braces
    repaired += '}' * max(0, open_braces)

    return repaired


def _call_logic_judge(api_url, model_name, api_key, scoring_api_slot, thread_id, task_id,
                      file_name, sampler_settings, prompt, expected_dims=None, max_attempts=None):
    """Sends ``prompt`` to the quality-scoring LLM and parses the JSON reply.

    Shared by the conversation-level judge (``_score_with_llm``, which expects the
    score dimensions in ``expected_dims`` -- defaults to every LLM dimension) and the
    per-turn logic gate (``check_logic``). Returns
    ``(scores_dict, flags, success_bool, extras_dict)``; non-score fields
    (``logic_evidence``, ``rewrite_instruction``, ``notes``) are parsed into the
    caller-facing extras but stripped from ``scores_dict`` so score consumers only ever
    see 0-100 numbers.

    Retry policy (v11.4, hardened in v11.5): a TRANSIENT failure -- a network error, a
    timeout, a non-200 status, an unreadable response envelope, an empty reply, or a reply
    cut off by the token limit -- is retried up to ``max_attempts`` times
    (``quality.judge_max_attempts``, default 3; ``1`` disables retries) with a linear
    back-off of ``quality.judge_retry_delay`` seconds. A reply cut off at the token limit
    also doubles the request's token budget for the next attempt.

    An unparsable reply is not dropped on the first sight: ``quality.judge_json_repair_attempts``
    (default 1) format-repair rounds send the model's own broken text back and ask for the
    same evaluation as JSON. Only when that also fails does the reply become
    ``llm_invalid_json`` (final, the conversation is dropped).
    """
    attempts = judge_max_attempts() if max_attempts is None else max(1, int(max_attempts))
    delay = judge_retry_delay()
    repair_rounds = judge_json_repair_attempts()
    dims = list(expected_dims) if expected_dims is not None else (
        list(LLM_DIMENSIONS) + (list(ROLEPLAY_LLM_DIMENSIONS) if is_roleplay_scoring_enabled() else []))
    max_tokens = judge_max_tokens(dims, sampler_settings)

    last = ({}, ["llm_exception"], False, {})
    for attempt in range(1, attempts + 1):
        scores, flags, success, extras = _call_logic_judge_once(
            api_url, model_name, api_key, scoring_api_slot, thread_id, task_id,
            file_name, sampler_settings, prompt, expected_dims=dims, max_tokens=max_tokens,
        )
        last = (scores, flags, success, extras)
        if success:
            if attempt > 1:
                log_message(
                    f"Thread {thread_id}: Quality judge succeeded for {task_id} on attempt "
                    f"{attempt}/{attempts}.", "INFO")
            return last
        if JUDGE_FINAL_FAILURE_FLAGS.intersection(flags):
            # The evaluation may well be fine -- only its packaging is broken. Ask the same
            # model to re-emit it as JSON before giving up on the conversation.
            broken = extras.get("_raw_content") or ""
            for repair_round in range(1, repair_rounds + 1):
                log_message(
                    f"Thread {thread_id}: Quality judge reply for {task_id} was not valid JSON; "
                    f"asking for a JSON-only re-emission ({repair_round}/{repair_rounds}).",
                    "WARNING")
                scores, flags, success, extras = _call_logic_judge_once(
                    api_url, model_name, api_key, scoring_api_slot, thread_id, task_id,
                    file_name, sampler_settings, prompt, expected_dims=dims,
                    max_tokens=max_tokens, repair_of=broken,
                )
                last = (scores, flags, success, extras)
                if success:
                    log_message(
                        f"Thread {thread_id}: Quality judge recovered valid JSON for {task_id} "
                        f"after a format-repair round.", "INFO")
                    return last
                broken = extras.get("_raw_content") or broken
                if not JUDGE_FINAL_FAILURE_FLAGS.intersection(flags):
                    break
            if JUDGE_FINAL_FAILURE_FLAGS.intersection(flags):
                log_message(
                    f"Thread {thread_id}: Quality judge returned invalid JSON for {task_id}; "
                    f"not retrying (attempt {attempt}/{attempts}).", "WARNING")
                return last
        if "llm_truncated" in flags:
            grown = min(max(64, max_tokens) * 2, 8192)
            if grown > max_tokens:
                log_message(
                    f"Thread {thread_id}: Quality judge reply for {task_id} hit the token "
                    f"limit; retrying with max_tokens={grown}.", "WARNING")
                max_tokens = grown
        if attempt < attempts:
            wait = delay * attempt
            log_message(
                f"Thread {thread_id}: Quality judge attempt {attempt}/{attempts} failed for "
                f"{task_id} ({', '.join(flags)}); retrying in {wait:.1f}s.", "WARNING")
            if wait > 0:
                time.sleep(wait)
    log_message(
        f"Thread {thread_id}: Quality judge failed for {task_id} after {attempts} attempt(s) "
        f"({', '.join(last[1])}); the conversation will be dropped.", "ERROR")
    return last


def _call_logic_judge_once(api_url, model_name, api_key, scoring_api_slot, thread_id, task_id,
                           file_name, sampler_settings, prompt, expected_dims=None,
                           max_tokens=None, repair_of=None):
    """One judge request. See ``_call_logic_judge`` for the retry wrapper around it.

    ``repair_of`` is the model's own previous, unparsable reply: when set, the request asks
    for the SAME evaluation again as a bare JSON object.
    """
    dims = list(expected_dims) if expected_dims is not None else (
        list(LLM_DIMENSIONS) + (list(ROLEPLAY_LLM_DIMENSIONS) if is_roleplay_scoring_enabled() else []))
    if max_tokens is None:
        max_tokens = judge_max_tokens(dims, sampler_settings)

    messages = [
        {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]
    if repair_of:
        messages.append({"role": "assistant", "content": str(repair_of)[:6000]})
        messages.append({"role": "user", "content": (
            "That reply could not be parsed as JSON. Reply again with ONLY the JSON object for "
            "the same evaluation -- same keys, same values, double-quoted keys, every string "
            "closed, no markdown and no commentary.")})

    payload_dict = {
        "model": model_name,
        "messages": messages,
        "temperature": sampler_settings.get('temperature', 0.1),
        "top_p": sampler_settings.get('top_p', 0.9),
        # evidence lists and the per-turn rewrite instruction take space; a reply cut off by
        # the token limit is the most common source of "unterminated string" JSON errors
        "max_tokens": max_tokens,
        "stream": False,
    }
    # vLLM priority scheduling (opt-in, see judge_priority_enabled): sent on EVERY attempt,
    # including the format-repair and response_format-fallback requests, so a retried judge
    # request keeps its place in the queue. Absent entirely when the toggle is off.
    payload_dict.update(_judge_priority_field())

    headers = {'Content-Type': 'application/json'}
    if api_key:
        headers['Authorization'] = f"Bearer {api_key}"

    # Optional grammar-constrained JSON. Servers that reject response_format are remembered
    # and the request is repeated without it (see the loop below).
    response_format = _judge_response_format(judge_structured_output(), dims)
    format_cache_key = (api_url or "", model_name or "")
    if response_format and format_cache_key in _JUDGE_NO_RESPONSE_FORMAT:
        response_format = None

    # Debug log (request)
    try:
        current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{scoring_api_slot}.jsonl"
        with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
            debug_log.write(json.dumps({
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                "thread_id": thread_id,
                "type": "quality_scoring_request",
                "api_slot_idx": scoring_api_slot,
                "task_id": task_id,
                "source_file": file_name,
                "messages": messages,
            }) + '\n')
    except Exception:
        pass

    # Rate limit
    try:
        global_rate_limiter.wait_if_needed(scoring_api_slot)
    except Exception:
        pass

    # Make the API call
    api_call_start = time.time()
    try:
        timeout = judge_request_timeout()

        response = None
        for _format_try in range(2):
            body = dict(payload_dict)
            if response_format:
                body['response_format'] = response_format
            response = requests.post(api_url, headers=headers, data=json.dumps(body),
                                     timeout=timeout)
            if response.status_code == 200:
                break
            if response_format and _looks_like_response_format_rejection(response):
                _JUDGE_NO_RESPONSE_FORMAT.add(format_cache_key)
                log_message(
                    f"Thread {thread_id}: Quality judge endpoint rejected response_format for "
                    f"{task_id}; retrying without it.", "WARNING")
                response_format = None
                continue
            break

        api_response_time = time.time() - api_call_start
        _record_judge_response_time(scoring_api_slot, api_response_time)

        if response is None or response.status_code != 200:
            status = getattr(response, 'status_code', 'no response')
            log_message(f"Thread {thread_id}: Quality scoring API returned {status} for {task_id}.", "WARNING")
            return {}, [f"llm_error_{status}"], False, {}

        try:
            response_data = response.json()
        except ValueError:
            log_message(
                f"Thread {thread_id}: Quality scoring API returned an unreadable envelope for "
                f"{task_id}.", "WARNING")
            return {}, ["llm_bad_envelope"], False, {}

        try:
            choices = response_data.get('choices') or []
            choice = choices[0] or {}
            message = choice.get('message') or {}
        except (AttributeError, IndexError, TypeError):
            log_message(
                f"Thread {thread_id}: Quality scoring API response had no usable choices for "
                f"{task_id}.", "WARNING")
            return {}, ["llm_bad_envelope"], False, {}

        finish_reason = str(choice.get('finish_reason') or '').lower()
        content = message.get('content')
        if isinstance(content, list):
            # Some servers return content parts instead of a single string.
            parts = []
            for part in content:
                if isinstance(part, dict):
                    parts.append(str(part.get('text') or part.get('content') or ''))
                else:
                    parts.append(str(part))
            content = "".join(parts)
        content = (content or "").strip()

        # Debug log (response) -- the raw judge reply is what makes a JSON failure diagnosable
        try:
            current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{scoring_api_slot}.jsonl"
            with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
                debug_log.write(json.dumps({
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "thread_id": thread_id,
                    "type": "quality_scoring_response",
                    "api_slot_idx": scoring_api_slot,
                    "task_id": task_id,
                    "source_file": file_name,
                    "finish_reason": finish_reason,
                    "content": content,
                }) + '\n')
        except Exception:
            pass

        if not content:
            if finish_reason == 'length':
                log_message(
                    f"Thread {thread_id}: Quality scoring LLM ran out of tokens before writing "
                    f"anything for {task_id}.", "WARNING")
                return {}, ["llm_truncated"], False, {"_raw_content": ""}
            log_message(
                f"Thread {thread_id}: Quality scoring LLM returned an empty reply for {task_id}.",
                "WARNING")
            return {}, ["llm_empty_reply"], False, {"_raw_content": ""}

        # Parse the JSON response through the extraction ladder.
        parsed, how = _extract_json_object(content, preferred_keys=dims)
        if parsed is None:
            salvaged = _salvage_scores_from_text(content, dims)
            if salvaged:
                parsed, how = salvaged, "salvaged"
        if parsed is None:
            if finish_reason == 'length':
                log_message(
                    f"Thread {thread_id}: Quality scoring LLM reply for {task_id} was cut off by "
                    f"the token limit before it formed valid JSON.", "WARNING")
                return {}, ["llm_truncated"], False, {"_raw_content": content}
            log_message(
                f"Thread {thread_id}: Quality scoring LLM returned invalid JSON for {task_id}: "
                f"{content[:500]}", "WARNING")
            return {}, ["llm_invalid_json"], False, {"_raw_content": content}
        if how != "direct":
            log_message(
                f"Thread {thread_id}: Quality scoring LLM JSON recovered for {task_id} "
                f"({how}).", "DEBUG")

        flags = []
        scores = {}
        for dim in dims:
            numeric = _coerce_score(parsed.get(dim, 50), 50.0)
            scores[dim] = max(0, min(100, int(numeric)))
            if numeric < 40:
                flags.append(f"low_{dim}")

        # Keep non-score fields out of the score dict so callers never have to know
        # which extra keys the judge was asked to return.
        for extra in ("ooc_evidence", "logic_evidence", "rewrite_instruction", "notes"):
            scores.pop(extra, None)

        # LLM-judged OOC evidence: concrete out-of-character moments the judge spotted.
        # An explicit, non-empty list flags the row even when consistency stayed >= 40,
        # so the dashboard can show persona drift without relying on phrase matching alone.
        ooc_evidence = parsed.get('ooc_evidence')
        if isinstance(ooc_evidence, str):
            ooc_evidence = [ooc_evidence]
        if ooc_evidence is not None and not isinstance(ooc_evidence, list):
            ooc_evidence = []
        ooc_evidence = [str(e).strip() for e in (ooc_evidence or []) if str(e).strip()]
        if ooc_evidence:
            flags.append("ooc_drift")

        # LLM-judged logic evidence: concrete contradictions found in the output.
        logic_evidence = parsed.get('logic_evidence')
        if isinstance(logic_evidence, str):
            logic_evidence = [logic_evidence]
        if logic_evidence is not None and not isinstance(logic_evidence, list):
            logic_evidence = []
        logic_evidence = [str(e).strip() for e in (logic_evidence or []) if str(e).strip()]
        if logic_evidence:
            flags.append("logic_issues")

        rewrite_instruction = parsed.get('rewrite_instruction')
        if isinstance(rewrite_instruction, list):
            rewrite_instruction = "; ".join(str(x) for x in rewrite_instruction)
        rewrite_instruction = str(rewrite_instruction or "").strip()

        # Non-score extras travel in a parallel dict so callers that only want scores
        # (the conversation-level path) can ignore it; the per-turn logic gate reads it.
        extras = {
            "ooc_evidence": ooc_evidence,
            "logic_evidence": logic_evidence,
            "rewrite_instruction": rewrite_instruction,
            "notes": str(parsed.get('notes') or ""),
        }
        return scores, flags, True, extras

    except requests.exceptions.Timeout:
        log_message(f"Thread {thread_id}: Quality scoring LLM timed out for {task_id}.", "WARNING")
        return {}, ["llm_timeout"], False, {}
    except Exception as e:
        log_message(f"Thread {thread_id}: Quality scoring LLM error for {task_id}: {e}", "ERROR")
        return {}, ["llm_exception"], False, {}


# --- Per-turn logic gate ---------------------------------------------------------------
# The gate the user asked for: run after every assistant turn, hunt objective logic
# problems (wrong sexed anatomy vs. the character's stated gender, profile-contradicting
# body parts/injuries/clothing, impossible timeline or quantity statements) using the
# SAME quality-scoring LLM slot. When it fails, the caller re-prompts the main
# generation API with the original system prompt, full conversation history and the
# judge's specific findings -- enough context that the model knows exactly what to fix
# instead of guessing from a bare sentence.

def check_logic(answer, conversation_history, character_profile_text="", turn_number=None,
                task_id="", thread_id="", file_name=""):
    """Per-turn internal-logic check against the character profile(s).

    Uses the same API slot as conversation-level quality scoring (``quality.scoring_api``,
    falling back to the primary API when the dedicated one is not configured). The
    request body mirrors the judge format so the caller gets back a concrete, per-issue
    ``rewrite_instruction`` in addition to the ``logic_evidence`` list, and those strings
    are what the worker appends to the next generation prompt -- enough context that the
    model does not have to guess at what went wrong.

    Returns:
        {"failed": bool, "score": int, "evidence": [str, ...],
         "rewrite_instruction": str, "success": bool, "flags": [str, ...]}

        ``success=False`` means the LLM could not be reached / returned unparsable
        output; in that case ``failed`` stays ``False`` so the answer is not blocked on a
        judge outage. ``failed=True`` means the check ran and found issues below the
        configured ``quality.logic_threshold`` (default 70).
    """
    result = {"failed": False, "score": None, "evidence": [],
              "rewrite_instruction": "", "success": False, "flags": []}

    threshold = global_config.get('quality.logic_threshold', 70)
    try:
        threshold = int(threshold)
    except (TypeError, ValueError):
        threshold = 70

    scoring_api_config = global_config.get('quality.scoring_api', {})
    api_url = scoring_api_config.get('url') or global_config.get('api.apis.0.url', '')
    model_name = scoring_api_config.get('model') or global_config.get('api.apis.0.model', '')
    api_key = scoring_api_config.get('key') or global_config.get('api.apis.0.key', '')
    scoring_api_slot = scoring_api_config.get('slot_idx', 6)
    sampler_settings = scoring_api_config.get('sampler_settings', {}) or {}

    if not api_url or not model_name:
        log_message(f"Thread {thread_id}: Logic check skipped -- quality LLM not configured "
                    f"(task {task_id}).", "DEBUG")
        result["flags"].append("logic_llm_not_configured")
        return result

    profile_block = (character_profile_text or "").strip()
    max_profile_chars = global_config.get('quality.max_profile_chars_for_scoring', 4000)
    if len(profile_block) > max_profile_chars:
        profile_block = profile_block[:max_profile_chars] + "\n[...profile truncated...]"

    # The check needs to see the last few turns for temporal continuity and enough of the
    # current turn to catch self-contradictions in longer replies. Cap both ends so a
    # very long conversation cannot blow out the judge's context.
    context_turns = conversation_history[-3:] if conversation_history else []
    conversation_text = _format_conversation_for_scoring(
        context_turns + [{"role": "assistant", "content": answer}])
    max_chars = global_config.get('quality.logic_max_chars_for_check', 4000)
    conversation_text = _truncate_head_tail(conversation_text, max_chars)

    prompt = (
        "You are a strict quality evaluator for synthetic roleplay dialogue. "
        "Your ONLY job on this turn is to check for objective INTERNAL LOGIC ERRORS in the "
        "assistant's LATEST reply (labelled 'ASSISTANT' below, the final message in the "
        "CONVERSATION block). Do NOT evaluate style, pacing, tone, or in-character choices -- "
        "only facts that contradict the character profile(s) or the reply's own earlier claims.\n\n"
        "WHAT TO LOOK FOR (all of these count as failures):\n"
        "1. Sexed anatomy that contradicts a character's stated gender or earlier descriptions "
        "(a female character described with male-only organs, a male character with female-only "
        "organs, or either one described with BOTH simultaneously).\n"
        "2. Body parts, scars, injuries, clothing, hair, eye colour, or appearance details that "
        "contradict the CHARACTER PROFILE(S) block below, or that contradict a description the "
        "same character used in an EARLIER turn of this conversation.\n"
        "3. Impossible or self-contradicting statements within this reply: an object both present "
        "and absent, two mutually-exclusive times/places/distances, quantities that change between "
        "sentences without cause, a character who was left in one room acting in another, a person "
        "who is clearly dead or absent acting.\n"
        "4. A character referring to a name, place, or object as their own that belongs to a "
        "DIFFERENT character in the scene (identity mix-ups).\n\n"
        "DO NOT PENALISE: ordinary roleplay licence (weather, background extras, generic objects), "
        "a character changing clothes/mood over the course of the conversation, quoted dialogue "
        "(a character may say anything), or any stylistic issue.\n\n"
        "Score 100 when the reply holds together factually. Each distinct contradiction should "
        "pull the score down sharply (roughly 15-25 points per distinct finding); multiple "
        "findings compound. A score below " + str(threshold) + " is a FAIL.\n\n"
        "Respond ONLY with a JSON object, no other text. Format:\n"
        '{"logic": <0-100>, '
        '"logic_evidence": ["<very short description of each distinct logic error found; avoid quotation marks; [] if none>"], '
        '"rewrite_instruction": "<one or two imperative sentences the author should follow to fix the specific errors above, referencing the exact profile fields or earlier-turn lines the errors contradict; empty string if the score is at or above the fail threshold>"}\n'
    )
    if profile_block:
        prompt += ("\nCHARACTER PROFILE(S):\n" + profile_block + "\n\n")
    prompt += "\nCONVERSATION (final message is the one to check):\n" + conversation_text

    expected_dims = ["logic"]
    scores, flags, success, extras = _call_logic_judge(
        api_url, model_name, api_key, scoring_api_slot, thread_id, task_id, file_name,
        sampler_settings, prompt, expected_dims=expected_dims,
    )
    result["flags"] = flags
    if not success:
        return result

    result["success"] = True
    result["score"] = scores.get("logic")
    result["evidence"] = list(extras.get("logic_evidence") or [])
    result["rewrite_instruction"] = (extras.get("rewrite_instruction") or "").strip()
    if result["score"] is not None and result["score"] < threshold:
        result["failed"] = True
    return result


def build_logic_rewrite_instruction(result, answer, character_profile_text="",
                                     conversation_history=None, turn_number=None):
    """Assembles the context-rich instruction appended to the NEXT generation prompt after
    a failed logic check.

    The point of this helper -- and the reason the whole rewrite loop exists -- is to send
    back to the main API EXACTLY enough context that the model does not have to guess what
    to change: the specific findings from the judge, the offending profile fields, the
    original system prompt (already part of the caller's message list) and the full
    conversation history up to (but not including) the flawed reply. A bare one-line hint
    like "fix the anatomy" would leave the model to improvise, which is the failure mode
    this feature is meant to eliminate.

    Returns a multi-line instruction block suitable for appending to the current system
    prompt (or prepending to the next user message) for a single retry.
    """
    score = result.get("score")
    evidence = result.get("evidence") or []
    rewrite = (result.get("rewrite_instruction") or "").strip()

    parts = []
    parts.append(
        "LOGIC-CORRECTION RETRY: your previous reply failed the internal-logic check"
        + (f" (score {score}/100)" if score is not None else "")
        + ". The reviewer found the following concrete problems -- fix EACH one specifically, "
        "do not just rephrase the surrounding sentences:\n"
    )
    if evidence:
        for i, e in enumerate(evidence, start=1):
            parts.append(f"  {i}. {e}")
    elif rewrite:
        parts.append(f"  1. {rewrite}")
    else:
        parts.append(
            "  1. (No specific findings were recorded -- re-check the reply against the "
            "character profiles and the earlier turns and correct any factual contradiction "
            "you find: sexed anatomy vs. stated gender, profile-contradicting body parts or "
            "clothing, or impossible timeline/quantity statements.)"
        )
    # The judge's own fix instruction carries context the bare evidence list may omit
    # (exactly which profile fields / earlier lines to match). Send it whenever present,
    # on top of the enumerated findings, so the model never has to guess.
    if rewrite:
        parts.append("\nReviewer's fix instruction: " + rewrite)
    parts.append(
        "Keep everything else about the reply intact: same scene, same tone, same characters, "
        "same plot beat. Only change the words needed to fix the listed problems, and make "
        "sure the corrected version matches the CHARACTER PROFILE(S) and the earlier turns "
        "already in the conversation. Reply with the full corrected message only."
    )
    return "\n".join(parts)


def _truncate_head_tail(text, max_chars):
    """Truncate ``text`` to ``max_chars`` keeping BOTH ends.

    Head-only truncation loses the end of long conversations, which is exactly where
    persona drift and OOC creep accumulate; keeping the last exchanges lets the judge
    see how the scene resolves. The start is kept so the opening voice is a reference
    point for comparison.
    """
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    head = max_chars * 3 // 5
    tail = max_chars - head - 40  # reserve room for the marker
    tail = max(tail, 1)
    marker = "\n\n[...middle of conversation truncated for scoring...]\n\n"
    return text[:head] + marker + text[-tail:]


def _format_conversation_for_scoring(conversation_history):
    """Formats conversation history into a readable string for the scoring LLM."""
    parts = []
    for turn in conversation_history:
        role = turn.get("role", "unknown")
        content = turn.get("content", "")
        role_label = "USER" if role == "user" else "ASSISTANT" if role == "assistant" else role.upper()
        parts.append(f"[{role_label}]: {content}")
    return "\n\n".join(parts)


def get_quality_stats():
    """Returns aggregated quality statistics for the dashboard."""
    with app_state.quality_lock:
        scores = app_state.quality_scores
        active_dims = list(active_scoring_dimensions())
        if not scores:
            return {
                "count": 0,
                "avg_composite": 0,
                "min_composite": 0,
                "max_composite": 0,
                "dimension_averages": {dim: 0 for dim in active_dims},
                "flag_counts": {},
                "threshold_failures": 0
            }

        composites = [s["composite"] for s in scores.values()]
        avg = sum(composites) / len(composites)
        dim_avgs = {}
        for dim in active_dims:
            dim_scores = [s["dimensions"].get(dim, 0) for s in scores.values()]
            dim_avgs[dim] = round(sum(dim_scores) / len(dim_scores), 1)

        flag_counts = {}
        for s in scores.values():
            for flag in s.get("flags", []):
                flag_counts[flag] = flag_counts.get(flag, 0) + 1

        threshold = global_config.get('quality.min_score_threshold', 50)
        threshold_failures = sum(1 for c in composites if c < threshold)

        return {
            "count": len(scores),
            "avg_composite": round(avg, 1),
            "min_composite": round(min(composites), 1),
            "max_composite": round(max(composites), 1),
            "dimension_averages": dim_avgs,
            "flag_counts": flag_counts,
            "threshold_failures": threshold_failures
        }


# =============================================================================
# Quality Scoring Queue System (v11.2)
# =============================================================================
# The LLM quality judge can only process one request at a time. This queue
# system ensures that quality scoring requests from multiple worker threads
# are processed sequentially without blocking the workers or losing requests.
# =============================================================================

# Quality scoring queue: holds tuples of (conversation_history, task_id, thread_id,
# api_slot_idx, file_name, character_profile_text, scene_characters, logic_findings)
_quality_scoring_queue = Queue()
_quality_queue_lock = Lock()
_quality_processor_thread = None
_quality_processor_running = False


def initialize_quality_queue():
    """Initialize the quality scoring queue system. Called once at app startup."""
    global _quality_processor_thread, _quality_processor_running
    with _quality_queue_lock:
        if _quality_processor_running:
            return  # Already initialized
        _quality_processor_running = True
        _quality_processor_thread = threading.Thread(
            target=_quality_queue_processor,
            daemon=True,
            name="QualityScoringProcessor"
        )
        _quality_processor_thread.start()
        log_message("Quality scoring queue processor started.", "INFO")


def shutdown_quality_queue():
    """Shutdown the quality scoring queue processor. Called on app exit."""
    global _quality_processor_running
    with _quality_queue_lock:
        _quality_processor_running = False
    if _quality_processor_thread:
        _quality_processor_thread.join(timeout=2.0)
        log_message("Quality scoring queue processor stopped.", "INFO")


def enqueue_quality_scoring(conversation_history, task_id, thread_id, api_slot_idx,
                            file_name, character_profile_text, scene_characters,
                            logic_findings, timeout=None):
    """Scores one conversation and returns the verdict (never ``None``).

    The judge is a gatekeeper for the dataset: the worker needs the verdict BEFORE it writes
    the row, because a dropped conversation must never reach output.jsonl (v11.4). So this
    call queues the request (the processor still runs one judge at a time) and waits for its
    result. When the processor is not running (tests, or scoring disabled) it scores inline.

    Returns the ``score_conversation`` dict. A dropped conversation carries
    ``{"dropped": True, "drop_reason": ...}``; a queue that could not accept the job (full,
    or the wait timed out) is reported as a dropped row with reason ``judge_failed`` so an
    unjudged conversation never slips into the dataset.
    """
    if not _quality_processor_running:
        # Fallback to synchronous scoring when processor isn't running (e.g., tests)
        log_message(f"Thread {thread_id}: Quality scoring queue not running, using synchronous scoring for {task_id}.", "DEBUG")
        try:
            qr = score_conversation(
                conversation_history,
                task_id,
                thread_id,
                api_slot_idx=api_slot_idx,
                file_name=file_name,
                character_profile_text=character_profile_text,
                scene_characters=scene_characters,
                logic_findings=logic_findings
            )
        except Exception as e:
            log_message(f"Thread {thread_id}: Quality scoring error for {task_id}: {e}", "ERROR")
            qr = _dropped_result(DROP_REASON_JUDGE_FAILED, ["llm_exception"])
        return _record_quality_result(qr, task_id, thread_id, "Quality score")

    job = {"event": threading.Event(), "result": None, "abandoned": False}
    try:
        _quality_scoring_queue.put((
            conversation_history,
            task_id,
            thread_id,
            api_slot_idx,
            file_name,
            character_profile_text,
            scene_characters,
            logic_findings,
            job,
        ), block=False)
        log_message(f"Thread {thread_id}: Quality scoring queued for {task_id} (queue size: {_quality_scoring_queue.qsize()}).", "DEBUG")
    except Full:
        log_message(f"Thread {thread_id}: Quality scoring queue full for {task_id}; the conversation is dropped.", "WARNING")
        return _record_quality_result(_dropped_result(DROP_REASON_JUDGE_FAILED, ["llm_queue_full"]),
                                      task_id, thread_id, "Quality processor")

    wait_seconds = quality_result_timeout() if timeout is None else max(1.0, float(timeout))
    deadline = time.time() + wait_seconds
    while not job["event"].wait(0.25):
        # A Stop must not hang on a queued judge: abandon the wait and drop the row.
        if app_state.stop_processing or time.time() >= deadline:
            break
    if not job["event"].is_set():
        job["abandoned"] = True
        log_message(
            f"Thread {thread_id}: Timed out after {wait_seconds:.0f}s waiting for the quality "
            f"verdict on {task_id}; the conversation is dropped.", "ERROR")
        return _record_quality_result(_dropped_result(DROP_REASON_JUDGE_FAILED, ["llm_timeout"]),
                                      task_id, thread_id, "Quality processor")
    return job["result"] or _dropped_result(DROP_REASON_JUDGE_FAILED, ["llm_exception"])


def _dropped_result(reason, flags=None):
    """A score_conversation-shaped result for a conversation the judge rejected."""
    return {
        "dropped": True,
        "drop_reason": reason,
        "composite": 0,
        "dimensions": {k: 0 for k in active_scoring_dimensions()},
        "flags": list(flags or []),
        "scored_at": time.strftime('%Y-%m-%d %H:%M:%S'),
        "method": "dropped",
    }


def _record_quality_result(qr, task_id, thread_id, prefix="Quality score"):
    """Stores a verdict: scores in ``quality_scores``, drops in ``quality_dropped``.

    A dropped conversation is never stored as a score and never flagged for review -- it is
    recorded only as a drop, so nothing downstream can pick it up as a scored row.
    """
    if qr.get("dropped"):
        with app_state.quality_lock:
            app_state.quality_dropped[task_id] = qr.get("drop_reason", DROP_REASON_JUDGE_FAILED)
            app_state.dropped_task_ids.add(task_id)
        log_message(
            f"Thread {thread_id}: {prefix} for {task_id}: DROPPED "
            f"({qr.get('drop_reason')}; flags: {qr.get('flags')}). Not saved to the dataset.",
            "WARNING")
        return qr

    with app_state.quality_lock:
        if task_id not in app_state.quality_scores:
            app_state.quality_scores[task_id] = qr
        # A later successful verdict supersedes an earlier drop for the same task (a dropped
        # task stays retryable, so this happens on the retry).
        app_state.quality_dropped.pop(task_id, None)
        app_state.dropped_task_ids.discard(task_id)
    min_threshold = global_config.get('quality.min_score_threshold', 50)
    if qr.get('composite', 0) < min_threshold:
        with app_state.quality_review_lock:
            app_state.quality_review_ids.add(task_id)
    log_message(
        f"Thread {thread_id}: {prefix} for {task_id}: "
        f"{qr['composite']}/100 (method: {qr['method']}, flags: {qr['flags']})",
        "INFO"
    )
    return qr


def _quality_queue_processor():
    """
    Background thread that processes quality scoring requests one at a time.
    This ensures the LLM judge is never overwhelmed with concurrent requests.
    """
    log_message("Quality scoring processor thread started.", "DEBUG")
    
    while _quality_processor_running:
        try:
            # Wait for a scoring request with a timeout to allow checking _quality_processor_running
            item = _quality_scoring_queue.get(timeout=1.0)
        except Empty:
            continue
        
        (conversation_history, task_id, thread_id, api_slot_idx, file_name,
         character_profile_text, scene_characters, logic_findings) = item[:8]
        job = item[8] if len(item) > 8 else None
        
        log_message(f"Quality processor: Starting score for {task_id} (from thread {thread_id}).", "DEBUG")
        
        try:
            # Perform the actual scoring
            qr = score_conversation(
                conversation_history,
                task_id,
                thread_id,
                api_slot_idx=api_slot_idx,
                file_name=file_name,
                character_profile_text=character_profile_text,
                scene_characters=scene_characters,
                logic_findings=logic_findings
            )
            # The waiting worker gave up (stop/timeout) and already recorded the drop; storing
            # the late score too would resurrect a row that was dropped.
            if job is not None and job.get("abandoned"):
                log_message(f"Quality processor: {task_id} was abandoned by its worker; "
                            f"discarding the late verdict.", "WARNING")
            else:
                _record_quality_result(qr, task_id, thread_id, "Quality processor")
        except Exception as e:
            log_message(f"Quality processor: Error scoring {task_id}: {e}", "ERROR")
            qr = _dropped_result(DROP_REASON_JUDGE_FAILED, ["llm_exception"])
        finally:
            if job is not None:
                job["result"] = qr
                job["event"].set()
            _quality_scoring_queue.task_done()
    
    log_message("Quality scoring processor thread exiting.", "DEBUG")
