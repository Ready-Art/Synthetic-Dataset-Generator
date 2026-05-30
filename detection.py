# detection.py
import re
import time

# These will be set by generate.py after import
issue_timestamps = None
issue_timestamps_lock = None

def is_refusal(answer, refusal_phrases_list):
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

    processed_phrases = [p.lower().strip() for p in refusal_phrases_list if p.strip()]
    cleaned_sentences = [clean_sentence_for_match(s) for s in original_sentences]

    # Pre-compile regex patterns ONCE per call
    compiled_patterns = [(p, re.compile(r'\b' + re.escape(p) + r'\b', re.IGNORECASE)) for p in processed_phrases]

    detected_info = []
    for phrase_lower, pattern in compiled_patterns:
        for i, cleaned_sentence in enumerate(cleaned_sentences):
            if pattern.search(cleaned_sentence):
                detected_info.append((phrase_lower, original_sentences[i]))
                break

    if detected_info:
        with issue_timestamps_lock:
            issue_timestamps['refusals'].append(time.time())
            cutoff = time.time() - 3600
            issue_timestamps['refusals'] = [t for t in issue_timestamps['refusals'] if t > cutoff]

    return bool(detected_info), detected_info

def is_user_speaking(answer, user_speaking_phrases_list):
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

    processed_phrases = [p.lower().strip() for p in user_speaking_phrases_list if p.strip()]
    cleaned_sentences = [clean_sentence_for_match(s) for s in original_sentences]

    # Pre-compile regex patterns ONCE per call
    compiled_patterns = [(p, re.compile(r'\b' + re.escape(p) + r'\b', re.IGNORECASE)) for p in processed_phrases]

    detected_info = []
    for phrase_lower, pattern in compiled_patterns:
        for i, cleaned_sentence in enumerate(cleaned_sentences):
            if pattern.search(cleaned_sentence):
                detected_info.append((phrase_lower, original_sentences[i]))

    if detected_info:
        with issue_timestamps_lock:
            issue_timestamps['user_speaking'].append(time.time())
            cutoff = time.time() - 3600
            issue_timestamps['user_speaking'] = [t for t in issue_timestamps['user_speaking'] if t > cutoff]

    return bool(detected_info), detected_info

def is_slop(answer, slop_phrases_list):
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
                if current_s.strip(): processed_original_sentences.append(current_s.strip())
                current_s = ""
    if current_s.strip(): processed_original_sentences.append(current_s.strip())
    if not processed_original_sentences and answer.strip(): processed_original_sentences = [answer.strip()]

    processed_phrases = [p.lower().strip() for p in slop_phrases_list if p.strip()]
    cleaned_sentences = [clean_sentence_for_match(s) for s in processed_original_sentences]

    # Pre-compile regex patterns ONCE per call
    compiled_patterns = [(p, re.compile(r'\b' + re.escape(p) + r'\b', re.IGNORECASE)) for p in processed_phrases]

    detected_info = []
    for phrase_lower, pattern in compiled_patterns:
        for i, cleaned_sentence in enumerate(cleaned_sentences):
            if pattern.search(cleaned_sentence):
                # FIXED: Changed from original_sentences to processed_original_sentences
                detected_info.append((phrase_lower, processed_original_sentences[i]))

    if detected_info:
        with issue_timestamps_lock:
            issue_timestamps['slop'].append(time.time())
            cutoff = time.time() - 3600
            issue_timestamps['slop'] = [t for t in issue_timestamps['slop'] if t > cutoff]

    return bool(detected_info), detected_info

def is_anti_slop(answer, anti_slop_phrases_list):
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
                if current_s.strip(): processed_original_sentences.append(current_s.strip())
                current_s = ""
    if current_s.strip(): processed_original_sentences.append(current_s.strip())
    if not processed_original_sentences and answer.strip(): processed_original_sentences = [answer.strip()]

    processed_phrases = [p.lower().strip() for p in anti_slop_phrases_list if p.strip()]
    cleaned_sentences = [clean_sentence_for_match(s) for s in processed_original_sentences]

    # Pre-compile regex patterns ONCE per call
    compiled_patterns = [(p, re.compile(r'\b' + re.escape(p) + r'\b', re.IGNORECASE)) for p in processed_phrases]

    detected_info = []
    for phrase_lower, pattern in compiled_patterns:
        for i, cleaned_sentence in enumerate(cleaned_sentences):
            if pattern.search(cleaned_sentence):
                detected_info.append((phrase_lower, processed_original_sentences[i]))

    if detected_info:
        with issue_timestamps_lock:
            issue_timestamps['anti_slop'].append(time.time())
            cutoff = time.time() - 3600
            issue_timestamps['anti_slop'] = [t for t in issue_timestamps['anti_slop'] if t > cutoff]

    return bool(detected_info), detected_info

def is_incomplete_quote(text):
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
