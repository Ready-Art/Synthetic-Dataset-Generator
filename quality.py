# quality.py
"""Quality scoring layer for the Synthetic Dataset Generator.

Scores completed conversations across the core dimensions (coherence, naturalness,
engagement, diversity, consistency, technical) using a hybrid approach:
  - Heuristic (fast, no API call): repetition, diversity, punctuation, length
  - LLM (optional, uses a dedicated API slot): coherence, naturalness, engagement,
    consistency (persona fidelity against the character card)

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

import app_state
import text_utils
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
SCORING_DIMENSIONS = {
    "coherence": 0.25,
    "naturalness": 0.20,
    "engagement": 0.15,
    "diversity": 0.15,
    "consistency": 0.15,
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

# --- Heuristic-only dimensions (no LLM needed) ---
HEURISTIC_DIMENSIONS = {"diversity", "technical"}

# --- LLM-based dimensions (require an API call) ---
LLM_DIMENSIONS = {"coherence", "naturalness", "engagement", "consistency"}

# --- LLM-based roleplay dimensions (also have a heuristic fallback so they stay
# meaningful when LLM scoring is off or the judge call fails) ---
ROLEPLAY_LLM_DIMENSIONS = set(ROLEPLAY_SCORING_DIMENSIONS)


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
    if is_roleplay_scoring_enabled(config):
        dims.update(ROLEPLAY_SCORING_DIMENSIONS)
    return dims


def score_conversation(conversation_history, task_id, thread_id, api_slot_idx=None, file_name="",
                       character_profile_text="", scene_characters=None):
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
            "technical": 92,
            "voice_distinctiveness": 85,   # only when quality.roleplay_dimensions is on
            "grounding": 90,               # only when quality.roleplay_dimensions is on
            "persona_alignment": 60        # only when quality.roleplay_dimensions is on
        },
        "flags": ["short_response", "low_diversity"],
        "scored_at": "2025-01-15 14:32:00",
        "method": "hybrid"  # or "heuristic_only" or "llm_only"
    }

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
        llm_scores, llm_flags, llm_success = _score_with_llm(
            conversation_history, task_id, thread_id, api_slot_idx, file_name,
            character_profile_text=character_profile_text
        )
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
            # Fallback: estimate LLM dimensions heuristically
            log_message(f"Thread {thread_id}: LLM quality scoring failed for {task_id}. Using heuristic fallback.", "WARNING")
            for dim in LLM_DIMENSIONS:
                dimension_scores[dim] = _heuristic_fallback_for_llm_dim(dim, conversation_history)
            flags.append("llm_scoring_fallback")
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
                    character_profile_text=""):
    """
    Calls a dedicated LLM to score the conversation on LLM-based dimensions.
    Uses API Slot 7 (index 6) if configured, otherwise the primary API.

    ``character_profile_text`` is the scene's character card (the ``character_injection``
    block: name, personality, traits, appearance, setting, etc.). When provided, the judge
    scores the conversation's consistency against that actual profile -- persona, voice,
    tone, and stated traits -- rather than a generic "is it consistent?" check. Without it,
    the consistency dimension cannot measure character fidelity at all, so passing it in is
    the single biggest quality win for roleplay data.

    Returns (scores_dict, flags, success_bool).
    """
    # Determine which API to use for scoring
    scoring_api_config = global_config.get('quality.scoring_api', {})
    api_url = scoring_api_config.get('url') or global_config.get('api.apis.0.url', '')
    model_name = scoring_api_config.get('model') or global_config.get('api.apis.0.model', '')
    api_key = scoring_api_config.get('key') or global_config.get('api.apis.0.key', '')
    scoring_api_slot = scoring_api_config.get('slot_idx', 6)  # Default to slot 7

    if not api_url or not model_name:
        log_message(f"Thread {thread_id}: Quality LLM not configured. Skipping LLM scoring.", "WARNING")
        return {}, ["llm_not_configured"], False

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
        '"ooc_evidence": ["<very short description of each distinct out-of-character moment, or [] if there are none>"], '
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

    messages = [
        {"role": "system", "content": "You are a strict but fair quality evaluator. Respond only with valid JSON."},
        {"role": "user", "content": scoring_prompt}
    ]

    # Build payload
    sampler_settings = scoring_api_config.get('sampler_settings', {})
    # The three roleplay dimensions add a score line each; extend the default headroom
    # so the judge is not cut off mid-JSON when they are active.
    extra_dims = len(ROLEPLAY_LLM_DIMENSIONS) if is_roleplay_scoring_enabled() else 0
    payload_dict = {
        "model": model_name,
        "messages": messages,
        "temperature": sampler_settings.get('temperature', 0.1),
        "top_p": sampler_settings.get('top_p', 0.9),
        # ooc_evidence entries take space; give the judge headroom beyond bare scores
        "max_tokens": sampler_settings.get('max_tokens', 384 + extra_dims * 64),
        "stream": False
    }

    payload = json.dumps(payload_dict)
    headers = {'Content-Type': 'application/json'}
    if api_key:
        headers['Authorization'] = f"Bearer {api_key}"

    # Debug log
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
        timeout = global_config.get('quality.scoring_timeout', 60)
        response = requests.post(api_url, headers=headers, data=payload, timeout=timeout)
        api_response_time = time.time() - api_call_start

        with api_response_times_lock:
            if scoring_api_slot < 6:
                api_response_times_per_slot[scoring_api_slot].append(api_response_time)
                if len(api_response_times_per_slot[scoring_api_slot]) > MAX_RESPONSE_TIMES_TO_TRACK:
                    api_response_times_per_slot[scoring_api_slot] = api_response_times_per_slot[scoring_api_slot][-MAX_RESPONSE_TIMES_TO_TRACK:]

        if response.status_code != 200:
            log_message(f"Thread {thread_id}: Quality scoring API returned {response.status_code} for {task_id}.", "WARNING")
            return {}, [f"llm_error_{response.status_code}"], False

        response_data = response.json()
        content = response_data['choices'][0]['message'].get('content', '')

        # Parse the JSON response
        content = content.strip()
        # Handle cases where the LLM wraps in markdown code blocks
        if content.startswith('```'):
            content = re.sub(r'^```(?:json)?\s*', '', content)
            content = re.sub(r'\s*```$', '', content)

        parsed = json.loads(content)
        flags = []

        scores = {}
        judged_dims = list(LLM_DIMENSIONS) + (list(ROLEPLAY_LLM_DIMENSIONS) if is_roleplay_scoring_enabled() else [])
        for dim in judged_dims:
            val = parsed.get(dim, 50)
            scores[dim] = max(0, min(100, int(val)))
            if val < 40:
                flags.append(f"low_{dim}")

        # Keep non-score fields (ooc_evidence, notes) out of the score dict so callers
        # never have to know which extra keys the judge was asked to return.
        scores.pop("ooc_evidence", None)
        scores.pop("notes", None)

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

        if parsed.get('notes'):
            # Store notes for debugging (not in composite score)
            pass

        return scores, flags, True

    except json.JSONDecodeError:
        log_message(f"Thread {thread_id}: Quality scoring LLM returned invalid JSON for {task_id}.", "WARNING")
        return {}, ["llm_invalid_json"], False
    except requests.exceptions.Timeout:
        log_message(f"Thread {thread_id}: Quality scoring LLM timed out for {task_id}.", "WARNING")
        return {}, ["llm_timeout"], False
    except Exception as e:
        log_message(f"Thread {thread_id}: Quality scoring LLM error for {task_id}: {e}", "ERROR")
        return {}, [f"llm_exception"], False


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
