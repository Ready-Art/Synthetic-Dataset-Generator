# detection.py
import re
import time

# These will be set by generate.py after import
issue_timestamps = None
issue_timestamps_lock = None

def is_refusal(answer, refusal_phrases_list):
    """Detects if the LLM's answer contains refusal phrases. Returns (bool, list_of_detected_info)."""
    PUNCTUATION = '.,!?\"\'*()[]{};:' # Characters to strip for matching
    def clean_sentence_for_match(sentence):
        # Normalize: lowercase, strip punctuation from words, join back
        return ' '.join(word.strip(PUNCTUATION) for word in sentence.split()).lower()

    sentences_with_delimiters = re.split(r'([.!?]["\']?\s*|[\n]+)', answer)
    original_sentences = []
    current_s = ""
    if sentences_with_delimiters:
        for part in sentences_with_delimiters:
            if part is None: continue
            current_s += part
            if re.search(r'[.!?]["\']?\s*$', part.strip()) or '\n' in part:
                if current_s.strip():
                    original_sentences.append(current_s.strip())
                current_s = ""
    if current_s.strip(): # Add any remaining part
        original_sentences.append(current_s.strip())

    if not original_sentences and answer.strip(): # Fallback if split fails but answer exists
        original_sentences = [answer.strip()]

    # OPTIMIZATION 1: Pre-process phrases once to avoid repeated work
    # Filter out empty phrases and create a list of lowercase phrases
    processed_phrases = []
    for phrase in refusal_phrases_list:
        phrase_lower = phrase.lower().strip()
        if phrase_lower:  # Skip empty phrases
            processed_phrases.append(phrase_lower)

    # OPTIMIZATION 2: Pre-clean all sentences once, not for each phrase
    cleaned_sentences = []
    for sentence in original_sentences:
        cleaned_sentences.append(clean_sentence_for_match(sentence))

    # OPTIMIZATION 3: Single pass through phrases with cached cleaned sentences
    detected_info = [] # List to store (phrase, original_sentence_text)
    for phrase_lower in processed_phrases:
        for i, cleaned_sentence in enumerate(cleaned_sentences):
            if re.search(r'\b' + re.escape(phrase_lower) + r'\b', cleaned_sentence, re.IGNORECASE):
                detected_info.append((phrase_lower, original_sentences[i]))
                break # Found this phrase, no need to check other sentences for the same phrase

    # Track timestamp when refusal is detected
    if detected_info:
        with issue_timestamps_lock:
            issue_timestamps['refusals'].append(time.time())
            # Keep only last 60 minutes of data
            cutoff = time.time() - 3600
            issue_timestamps['refusals'] = [t for t in issue_timestamps['refusals'] if t > cutoff]

    return bool(detected_info), detected_info

def is_user_speaking(answer, user_speaking_phrases_list):
    """Detects if the assistant's answer impersonates the user. Returns (bool, list_of_detected_info)."""
    PUNCTUATION = '.,!?\"\'*()[]{};:'
    def clean_sentence_for_match(sentence):
        return ' '.join(word.strip(PUNCTUATION) for word in sentence.split()).lower()

    sentences_with_delimiters = re.split(r'([.!?]["\']?\s*|[\n]+)', answer)
    original_sentences = []
    current_s = ""
    if sentences_with_delimiters:
        for part in sentences_with_delimiters:
            if part is None: continue
            current_s += part
            if re.search(r'[.!?]["\']?\s*$', part.strip()) or '\n' in part:
                if current_s.strip(): original_sentences.append(current_s.strip())
                current_s = ""
    if current_s.strip(): original_sentences.append(current_s.strip())
    if not original_sentences and answer.strip(): original_sentences = [answer.strip()]

    # OPTIMIZATION 1: Pre-process phrases once to avoid repeated work
    processed_phrases = []
    for phrase in user_speaking_phrases_list:
        phrase_lower = phrase.lower().strip()
        if phrase_lower:
            processed_phrases.append(phrase_lower)

    # OPTIMIZATION 2: Pre-clean all sentences once, not for each phrase
    cleaned_sentences = []
    for sentence in original_sentences:
        cleaned_sentences.append(clean_sentence_for_match(sentence))

    # FIX: Collect ALL detected issues instead of returning after first match
    detected_info = []
    for phrase_lower in processed_phrases:
        for i, cleaned_sentence in enumerate(cleaned_sentences):
            if re.search(r'\b' + re.escape(phrase_lower) + r'\b', cleaned_sentence, re.IGNORECASE):
                detected_info.append((phrase_lower, original_sentences[i]))
                # Removed the "break" here to continue checking other sentences for the same phrase
                # Removed the "if detected_info: break" to continue checking other phrases

    # Track timestamp only if issues were detected (once per call, not per issue)
    if detected_info:
        with issue_timestamps_lock:
            issue_timestamps['user_speaking'].append(time.time())
            cutoff = time.time() - 3600
            issue_timestamps['user_speaking'] = [t for t in issue_timestamps['user_speaking'] if t > cutoff]

    return bool(detected_info), detected_info

def is_slop(answer, slop_phrases_list):
    """Detects if the answer contains "slop" (undesirable phrases). Returns (bool, list_of_detected_info)."""
    PUNCTUATION = '.,!?\"\'*()[]{};:'
    def clean_sentence_for_match(sentence):
        return ' '.join(word.strip(PUNCTUATION) for word in sentence.split()).lower()

    sentences_with_delimiters = re.split(r'([.!?]["\']?\s*|[\n]+)', answer)
    processed_original_sentences = []
    current_s = ""
    if sentences_with_delimiters:
        for part in sentences_with_delimiters:
            if part is None: continue
            current_s += part
            if re.search(r'[.!?]["\']?\s*$', part.strip()) or '\n' in part:
                if current_s.strip():
                    processed_original_sentences.append(current_s.strip())
                current_s = ""
    if current_s.strip():
        processed_original_sentences.append(current_s.strip())

    if not processed_original_sentences and answer.strip():
        processed_original_sentences = [answer.strip()]

    # OPTIMIZATION 1: Pre-process phrases once to avoid repeated work
    # Filter out empty phrases and create a list of lowercase phrases
    processed_phrases = []
    for phrase in slop_phrases_list:
        phrase_lower = phrase.lower().strip()
        if phrase_lower:  # Skip empty phrases
            processed_phrases.append(phrase_lower)

    # OPTIMIZATION 2: Pre-clean all sentences once, not for each phrase
    cleaned_sentences = []
    for sentence in processed_original_sentences:
        cleaned_sentences.append(clean_sentence_for_match(sentence))

    detected_info = []

    for phrase_lower in processed_phrases:
        for i, cleaned_sentence in enumerate(cleaned_sentences):
            if re.search(r'\b' + re.escape(phrase_lower) + r'\b', cleaned_sentence, re.IGNORECASE):
                original_sentence = processed_original_sentences[i]
                detected_info.append((phrase_lower, original_sentence))
                # Don't break - continue checking for more issues

    # Track timestamp only if issues were detected
    if detected_info:
        with issue_timestamps_lock:
            issue_timestamps['slop'].append(time.time())
            cutoff = time.time() - 3600
            issue_timestamps['slop'] = [t for t in issue_timestamps['slop'] if t > cutoff]

    return bool(detected_info), detected_info

def is_anti_slop(answer, anti_slop_phrases_list):
    """Detects if the answer contains anti-slop phrases (sentence-level issues). Returns (bool, list_of_detected_info)."""
    PUNCTUATION = '.,!?\"\'*()[]{};:'
    def clean_sentence_for_match(sentence):
        return ' '.join(word.strip(PUNCTUATION) for word in sentence.split()).lower()

    sentences_with_delimiters = re.split(r'([.!?]["\']?\s*|[\n]+)', answer)
    processed_original_sentences = []
    current_s = ""
    if sentences_with_delimiters:
        for part in sentences_with_delimiters:
            if part is None: continue
            current_s += part
            if re.search(r'[.!?]["\']?\s*$', part.strip()) or '\n' in part:
                if current_s.strip():
                    processed_original_sentences.append(current_s.strip())
                current_s = ""
    if current_s.strip():
        processed_original_sentences.append(current_s.strip())

    if not processed_original_sentences and answer.strip():
        processed_original_sentences = [answer.strip()]

    # OPTIMIZATION 1: Pre-process phrases once to avoid repeated work
    processed_phrases = []
    for phrase in anti_slop_phrases_list:
        phrase_lower = phrase.lower().strip()
        if phrase_lower:  # Skip empty phrases
            processed_phrases.append(phrase_lower)

    # OPTIMIZATION 2: Pre-clean all sentences once, not for each phrase
    cleaned_sentences = []
    for sentence in processed_original_sentences:
        cleaned_sentences.append(clean_sentence_for_match(sentence))

    # FIX: Collect ALL detected issues instead of returning after first match
    detected_info = []  # List to store (phrase, original_sentence_text)

    for phrase_lower in processed_phrases:
        for i, cleaned_sentence in enumerate(cleaned_sentences):
            if re.search(r'\b' + re.escape(phrase_lower) + r'\b', cleaned_sentence, re.IGNORECASE):
                original_sentence = processed_original_sentences[i]
                detected_info.append((phrase_lower, original_sentence))
                # Don't break - continue checking for more issues in this message

    # Track timestamp only if issues were detected (once per call, not per issue)
    if detected_info:
        with issue_timestamps_lock:
            issue_timestamps['anti_slop'].append(time.time())
            # Keep only last 60 minutes of data
            cutoff = time.time() - 3600
            issue_timestamps['anti_slop'] = [t for t in issue_timestamps['anti_slop'] if t > cutoff]

    return bool(detected_info), detected_info

def is_incomplete_quote(text):
    """Detects if the text contains an unbalanced number of quotation marks.
    Returns (bool, list_of_detected_info)"""
    if not text:
        return False, []

    quote_count = text.count('"')
    if quote_count % 2 != 0:
        snippet = text[:150] + ("..." if len(text) > 150 else "")
        detected_info = [("Incomplete quote (unbalanced quotation marks)", snippet)]

        if issue_timestamps is not None and issue_timestamps_lock is not None:
            with issue_timestamps_lock:
                issue_timestamps['incomplete_quotes'].append(time.time())
                cutoff = time.time() - 3600
                issue_timestamps['incomplete_quotes'] = [t for t in issue_timestamps['incomplete_quotes'] if t > cutoff]

        return True, detected_info
    return False, []
