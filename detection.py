# detection.py
import collections
import functools
import re
import time

# These will be set by generate.py after import
issue_timestamps = None
issue_timestamps_lock = None

@functools.lru_cache(maxsize=32)
def _compile_phrases(phrases):
    """Compiles a phrase list once, and indexes each phrase by its rarest word in the list."""
    compiled = [(p, re.compile(r'\b' + re.escape(p) + r'\b', re.IGNORECASE)) for p in phrases]
    phrase_words = [set(re.findall(r'\w+', p)) for p in phrases]
    word_counts = collections.Counter(w for words in phrase_words for w in words)
    by_word, no_words = {}, []
    for idx, words in enumerate(phrase_words):
        # Non-ASCII words are always searched: IGNORECASE folds some letters (ſ/s, ı/i, ς/σ) that lower() keeps apart
        if words and all(w.isascii() for w in words):
            by_word.setdefault(min(words, key=word_counts.__getitem__), []).append(idx)
        else:
            no_words.append(idx)
    return compiled, by_word, no_words

def _candidate_patterns(phrases, cleaned_sentences):
    """Returns, in list order, the compiled phrases whose index word appears in a sentence.
    A phrase only matches on word boundaries, so each of its words must appear whole."""
    compiled, by_word, no_words = _compile_phrases(tuple(phrases))
    candidates = set(no_words)
    for sentence in cleaned_sentences:
        for word in set(re.findall(r'\w+', sentence)):
            if not word.isascii():
                return compiled  # see _compile_phrases: search every phrase
            candidates.update(by_word.get(word, ()))
    return [compiled[i] for i in sorted(candidates)]

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

    # Compiled once per phrase list; only phrases that could match are searched
    compiled_patterns = _candidate_patterns(processed_phrases, cleaned_sentences)

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

    # Compiled once per phrase list; only phrases that could match are searched
    compiled_patterns = _candidate_patterns(processed_phrases, cleaned_sentences)

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

    # Compiled once per phrase list; only phrases that could match are searched
    compiled_patterns = _candidate_patterns(processed_phrases, cleaned_sentences)

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

    # Compiled once per phrase list; only phrases that could match are searched
    compiled_patterns = _candidate_patterns(processed_phrases, cleaned_sentences)

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

    straight_count = text.count('"')
    curly_left = text.count('\u201c')   # Explicit — no ambiguity
    curly_right = text.count('\u201d')  # Explicit — no ambiguity

    if straight_count % 2 != 0:
        snippet = text[:150] + ("..." if len(text) > 150 else "")
        return True, [("Incomplete quote (unbalanced straight quotes)", snippet)]

    if curly_left != curly_right:
        snippet = text[:150] + ("..." if len(text) > 150 else "")
        return True, [("Incomplete quote (unbalanced curly quotes)", snippet)]

    return False, []
