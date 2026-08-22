# quality.py
"""Quality scoring layer for the Synthetic Dataset Generator.

Scores completed conversations across six dimensions using a hybrid approach:
  - Heuristic (fast, no API call): repetition, diversity, punctuation, length
  - LLM (optional, uses a dedicated API slot): coherence, naturalness, engagement

The composite score is a weighted 0-100 value. Each dimension gets its own sub-score.
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

# --- Heuristic-only dimensions (no LLM needed) ---
HEURISTIC_DIMENSIONS = {"diversity", "technical"}

# --- LLM-based dimensions (require an API call) ---
LLM_DIMENSIONS = {"coherence", "naturalness", "engagement", "consistency"}


def score_conversation(conversation_history, task_id, thread_id, api_slot_idx=None, file_name=""):
    """
    Scores a completed conversation. Returns a dict:
    {
        "composite": 87.3,
        "dimensions": {
            "coherence": 90,
            "naturalness": 85,
            "engagement": 82,
            "diversity": 95,
            "consistency": 88,
            "technical": 92
        },
        "flags": ["short_response", "low_diversity"],
        "scored_at": "2025-01-15 14:32:00",
        "method": "hybrid"  # or "heuristic_only" or "llm_only"
    }
    """
    if not conversation_history or len(conversation_history) < 2:
        return {
            "composite": 0,
            "dimensions": {k: 0 for k in SCORING_DIMENSIONS},
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

    # --- LLM scoring (optional, if configured) ---
    use_llm_scoring = global_config.get('quality.use_llm_scoring', False)
    method = "heuristic_only"

    if use_llm_scoring:
        llm_scores, llm_flags, llm_success = _score_with_llm(
            conversation_history, task_id, thread_id, api_slot_idx, file_name
        )
        if llm_success:
            method = "hybrid"
            for dim in LLM_DIMENSIONS:
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

    # --- Compute composite score ---
    weights = global_config.get('quality.weights', SCORING_DIMENSIONS)
    # Normalize weights in case user configured non-standard ones
    total_weight = sum(weights.get(d, 0) for d in SCORING_DIMENSIONS)
    if total_weight == 0:
        total_weight = 1

    composite = sum(
        dimension_scores.get(dim, 0) * weights.get(dim, 0) / total_weight
        for dim in SCORING_DIMENSIONS
    )
    composite = round(composite, 1)

    # --- Threshold flag ---
    min_threshold = global_config.get('quality.min_score_threshold', 50)
    if composite < min_threshold:
        flags.append(f"below_threshold_{min_threshold}")

    return {
        "composite": composite,
        "dimensions": {dim: round(dimension_scores.get(dim, 0), 1) for dim in SCORING_DIMENSIONS},
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
        # Proxy: check if character names (if any) are used consistently
        # For now, default to a moderate score
        return 70

    return 60  # Default fallback


def _score_with_llm(conversation_history, task_id, thread_id, api_slot_idx, file_name=""):
    """
    Calls a dedicated LLM to score the conversation on LLM-based dimensions.
    Uses API Slot 7 (index 6) if configured, otherwise the primary API.
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
    if len(conversation_text) > max_chars:
        conversation_text = conversation_text[:max_chars] + "\n[...truncated...]"

    scoring_prompt = (
        "You are a quality evaluator for synthetic dialogue data. "
        "Score the following conversation on each dimension from 0 to 100.\n\n"
        "DIMENSIONS:\n"
        "- coherence: Does the conversation flow logically? Do responses follow naturally from prompts?\n"
        "- naturalness: Does the dialogue sound like real human conversation? Avoid robotic/formulaic language.\n"
        "- engagement: Is the content interesting, vivid, and compelling? Does it hold attention?\n"
        "- consistency: Are characters, tones, and settings consistent throughout? No contradictions?\n\n"
        "Respond ONLY with a JSON object, no other text. Format:\n"
        '{"coherence": <0-100>, "naturalness": <0-100>, "engagement": <0-100>, "consistency": <0-100>, "notes": "<one sentence summary>"}\n\n'
        f"CONVERSATION:\n{conversation_text}"
    )

    messages = [
        {"role": "system", "content": "You are a strict but fair quality evaluator. Respond only with valid JSON."},
        {"role": "user", "content": scoring_prompt}
    ]

    # Build payload
    sampler_settings = scoring_api_config.get('sampler_settings', {})
    payload_dict = {
        "model": model_name,
        "messages": messages,
        "temperature": sampler_settings.get('temperature', 0.1),
        "top_p": sampler_settings.get('top_p', 0.9),
        "max_tokens": sampler_settings.get('max_tokens', 256),
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
        for dim in LLM_DIMENSIONS:
            val = parsed.get(dim, 50)
            scores[dim] = max(0, min(100, int(val)))
            if val < 40:
                flags.append(f"low_{dim}")

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
        if not scores:
            return {
                "count": 0,
                "avg_composite": 0,
                "min_composite": 0,
                "max_composite": 0,
                "dimension_averages": {dim: 0 for dim in SCORING_DIMENSIONS},
                "flag_counts": {},
                "threshold_failures": 0
            }

        composites = [s["composite"] for s in scores.values()]
        avg = sum(composites) / len(composites)
        dim_avgs = {}
        for dim in SCORING_DIMENSIONS:
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
