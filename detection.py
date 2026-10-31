# detection.py
import collections
import functools
import re
import time

import style_metrics

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

# Spans of quoted dialogue ("...", '...', curly ‘...’ / “...”). OOC phrases live in
# the *narrator's* voice, so these are blanked before matching: the same words spoken
# by a character inside quotation marks are legitimate in-character speech, not drift.
_QUOTED_DIALOGUE_RE = re.compile(
    r'"[^"]*"|'
    r"'[^']*'" + '|' +
    r'\u2018[^\u2019]*\u2019|'
    r'\u201c[^\u201d]*\u201d'
)

def _strip_quoted_dialogue(sentence):
    """Blank every quoted-dialogue span in *sentence* down to a single space.

    Used only by :func:`is_out_of_character`. The result is a parallel *narration*
    view of the sentence — NOT an index-aligned edit (lengths differ) — so callers
    must keep the original sentence alongside it for reporting.
    """
    return _QUOTED_DIALOGUE_RE.sub(' ', sentence)

def is_out_of_character(answer, ooc_phrases_list):
    """Detects out-of-character drift: assistant answering like a helpful bot,
    narration breaks ("As <name>, I would..."), god-viewing the user, or otherwise
    dropping the roleplay voice. Phrase-driven like is_refusal/is_slop/is_user_speaking.

    Unlike those checks, phrases are matched only against *narration*: quoted dialogue
    is stripped first, so an OOC phrase spoken by a character inside "..." / '...' does
    NOT flag the turn. This avoids discarding+regenerating good answers just because a
    character happened to say a listed phrase in a line of speech.

    Mirrors the existing is_*() shape: returns (bool, [(phrase, sentence), ...]) and,
    on a hit, appends a timestamp to ``issue_timestamps['out_of_character']`` (1-hour
    rolling window) so the dashboard picks it up automatically.
    """
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

    processed_phrases = [p.lower().strip() for p in ooc_phrases_list if p.strip()]
    # Match only against the narration view (quoted dialogue blanked out); keep the
    # original sentence for reporting so the dashboard shows the real line.
    cleaned_sentences = [clean_sentence_for_match(_strip_quoted_dialogue(s)) for s in processed_original_sentences]

    # Compiled once per phrase list; only phrases that could match are searched
    compiled_patterns = _candidate_patterns(processed_phrases, cleaned_sentences)

    detected_info = []
    for phrase_lower, pattern in compiled_patterns:
        for i, cleaned_sentence in enumerate(cleaned_sentences):
            if pattern.search(cleaned_sentence):
                detected_info.append((phrase_lower, processed_original_sentences[i]))

    if detected_info:
        with issue_timestamps_lock:
            issue_timestamps['out_of_character'].append(time.time())
            cutoff = time.time() - 3600
            issue_timestamps['out_of_character'] = [t for t in issue_timestamps['out_of_character'] if t > cutoff]

    return bool(detected_info), detected_info

# --- Regex Watch (v11.6) ----------------------------------------------------------------
# A phrase list cannot express a *shape* of prose ("it is not just X, it is Y"). Regex Watch
# lets the end user keep a small, editable set of regular expressions that describe those
# shapes. It mirrors the slop checks: same (bool, [(label, sentence), ...]) return shape, same
# 1-hour timestamp bookkeeping, so the dashboard picks hits up automatically.
#
# Patterns are matched against the WHOLE answer (a rewrite target can span a clause boundary),
# not sentence by sentence, so the reporting sentence is the one that contains the match.


class RegexWatchError(ValueError):
    """Raised for an unusable pattern (syntax error or an empty/oversized entry)."""


MAX_REGEX_PATTERN_LENGTH = 2000

# The patterns shipped in config/config.yml (detection.regex_watch.patterns). They describe
# rhetorical SHAPES, not words, and are matched case-insensitively against the whole reply.
# Written from scratch for this project: the point of each is the "X, but Y" pivot a model
# falls into when it wants to sound emphatic, so every pattern is anchored on that pivot and
# bounded (``{1,60}?``) so a match cannot run across a sentence break.
DEFAULT_REGEX_WATCH_PATTERNS = (
    # "not just/only/merely X, but (also) Y"
    r"\bnot\s+(?:just|only|merely|simply)\s+[^.!?\n]{1,60}?\s*(?:[,;:\u2014\u2013-])?\s*but\s+(?:also\s+)?\w",
    # "not only X but also Y" (the pivot needs no comma)
    r"\bnot\s+only\s+[^.!?\n]{1,60}?\bbut\s+also\s+\w",
    # "it is not just X, it is Y" -- the same subject restated
    r"\bit(?:'s|\u2019s|\s+is)\s+not\s+(?:just|only|merely|simply)\s+[^.!?\n]{1,60}?[,;:\u2014\u2013-]\s*it(?:'s|\u2019s|\s+is)\b",
    # "X is not (just/about) Y, it is (about) Z"
    r"\b\w+\s+is\s+not\s+(?:just\s+|only\s+|merely\s+)?(?:about\s+)?[^.!?\n]{1,60}?[,;:\u2014\u2013-]\s*it(?:'s|\u2019s|\s+is)\s+(?:about\s+)?\w",
    # "more than just a Z"
    r"\bmore\s+than\s+just\s+(?:a|an|the)?\s*\w+",
    # "less about X and more about Y"
    r"\bless\s+about\s+[^.!?\n]{1,40}?\s+and\s+(?:more|so\s+much\s+more)\s+about\s+\w",
    # v11.7 scotoma tic: denial-then-correction (scotoma/denial_correction.py -- the shape the
    # analysis project was built around). Negate a predicate, then assert the replacement.
    # The metric-side counterpart (style_metrics.denial_correction_hits) rejects a negated
    # correction tail; the rewrite-side pattern deliberately does not, so an LLM pass gets to
    # judge each candidate instead of the regex deciding.
    r"\b(?:is|are|was|were|does|do|did|has|have|had|can|could|will|would|should)n['\u2019]t\b[^.;!?\n]{0,80}[;.\u2014\u2013-]\s*(?:it|he|she|they|this|that|there|instead|rather)\b",
    # v11.7 scotoma tic: comma-appended absolute trailer (scotoma/two_tics.py +
    # trailer_rate.py; measured teacher 11.8% of sentences vs human 2.5%, rewrite fraction 0.79).
    # The pattern is DEFINED ONCE, in style_metrics.TRAILER_PATTERN, so the measurement and the
    # rewrite can never drift apart; config/config.yml mirrors that exact string and
    # tests/test_regex_watch.py asserts all three are identical. It requires the segment to be
    # verbless (no finite verb, no speech attribution) and to carry a predicate signal, which is
    # what keeps ordinary clauses -- "the man said", "the boat is leaving", "the door is open" --
    # and the comma that closes a line of dialogue out of the match.
    style_metrics.TRAILER_PATTERN,
)


@functools.lru_cache(maxsize=64)
def _compile_regex(pattern):
    """Compile one user pattern, or raise :class:`RegexWatchError`.

    ``IGNORECASE`` matches the phrase-driven checks. ``re.MULTILINE`` is deliberately NOT set
    so ``^``/``$`` mean start/end of the reply, which is what a user writing an anchored
    pattern expects.
    """
    if not isinstance(pattern, str):
        raise RegexWatchError("pattern must be text")
    text = pattern.strip()
    if not text:
        raise RegexWatchError("pattern is empty")
    if len(text) > MAX_REGEX_PATTERN_LENGTH:
        raise RegexWatchError(
            "pattern is longer than %d characters" % MAX_REGEX_PATTERN_LENGTH)
    try:
        return re.compile(text, re.IGNORECASE)
    except re.error as exc:
        raise RegexWatchError(str(exc))


def validate_regex_patterns(patterns):
    """Return ``(valid_patterns, errors)`` for an editable pattern list.

    ``errors`` is a list of ``(pattern, message)``; ``valid_patterns`` keeps the input order and
    drops blanks and duplicates so a saved config never grows a dead or repeated entry.
    """
    valid, errors, seen = [], [], set()
    for raw in patterns or ():
        text = (raw or "").strip()
        if not text:
            continue
        try:
            _compile_regex(text)
        except RegexWatchError as exc:
            errors.append((text, str(exc)))
            continue
        if text in seen:
            continue
        seen.add(text)
        valid.append(text)
    return valid, errors


def _sentence_around(text, start, end):
    """The sentence of *text* that contains ``text[start:end]`` (trimmed, capped for display)."""
    left = 0
    for marker in re.finditer(r'[.!?]\s|[\n]', text[:start]):
        left = marker.end()
    right_match = re.search(r'[.!?](\s|$)|\n', text[end:])
    right = end + right_match.end() if right_match else len(text)
    sentence = ' '.join(text[left:right].split())
    if len(sentence) > 300:
        sentence = sentence[:297] + "..."
    return sentence


def find_regex_matches(text, patterns):
    """Return ``[(pattern, sentence, matched_text), ...]`` for every pattern that matches *text*.

    Invalid patterns are skipped silently: the config editor validates on save, and a pattern
    that breaks at run time must never stop generation. The first match per pattern is
    reported, which is enough to drive one rewrite round; the caller re-runs the check after
    each rewrite.
    """
    if not text:
        return []
    found = []
    for pattern in patterns or ():
        try:
            compiled = _compile_regex(pattern)
        except RegexWatchError:
            continue
        match = compiled.search(text)
        if match is None or match.end() == match.start():
            # A zero-width match has nothing to rewrite; it would loop forever.
            continue
        found.append((pattern.strip(),
                      _sentence_around(text, match.start(), match.end()),
                      match.group(0)))
    return found


def is_regex_watch(text, patterns, record_timestamp=True):
    """Detects user-supplied regex shapes ("not just X, but Y") in *text*.

    Same contract as the other ``is_*()`` checks: ``(bool, [(pattern, sentence), ...])``, plus a
    timestamp in ``issue_timestamps['regex_watch']`` (1-hour rolling window) so the dashboard's
    Regex Watch area and issue graph update on their own. ``record_timestamp=False`` is for
    callers that only want to probe the text (e.g. a re-check after a rewrite).
    """
    matches = find_regex_matches(text, patterns)
    if matches and record_timestamp and issue_timestamps is not None \
            and issue_timestamps_lock is not None:
        with issue_timestamps_lock:
            issue_timestamps.setdefault('regex_watch', []).append(time.time())
            cutoff = time.time() - 3600
            issue_timestamps['regex_watch'] = [t for t in issue_timestamps['regex_watch'] if t > cutoff]
    return bool(matches), [(pattern, sentence) for pattern, sentence, _matched in matches]


# --- Non-Latin script check (v11.10.0) --------------------------------------------------
# A model that has been trained on more than one language occasionally drops a stray glyph into an
# otherwise English reply -- a Chinese character, a Cyrillic word, a Greek letter used as decoration.
# Those strays poison a dataset whose target language is Latin-script only, and a phrase list cannot
# catch them (there is no fixed phrase to list). This check is PURELY CHARACTER-CLASS BASED: it needs
# no LLM call, no phrase list and no network round trip, so it costs nothing per reply.
#
# The rule is an ALLOW-LIST, not a block-list: a character is accepted when it belongs to the Latin
# script (plus digits, whitespace, punctuation and the handful of symbol blocks a roleplay reply
# legitimately uses), and flagged otherwise. A block-list of known scripts would silently miss every
# script nobody thought to list; an allow-list catches them all -- Chinese, Japanese, Korean, Cyrillic,
# Greek, Arabic, Hebrew, Devanagari, Thai, emoji -- because anything outside Latin is, by definition,
# not Latin. The script NAMES below only make the report readable; the decision is the allow-list.

# Script ranges reported by name. These are for the human-readable label ONLY; a character outside
# every one of them but also outside the Latin allow-list is still flagged (as "Other non-Latin").
NON_LATIN_SCRIPT_RANGES = (
    ('Chinese (CJK)', '\u2e80-\u2eff\u3000-\u303f\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uff00-\uffef'),
    ('Japanese (Kana)', '\u3040-\u30ff'),
    ('Korean (Hangul)', '\u1100-\u11ff\u3130-\u318f\ua960-\ua97f\uac00-\ud7af'),
    ('Cyrillic', '\u0400-\u052f\u2de0-\u2dff\ua640-\ua69f'),
    ('Greek', '\u0370-\u03ff\u1f00-\u1fff'),
    ('Arabic', '\u0600-\u06ff\u0750-\u077f\u08a0-\u08ff\ufb50-\ufdff\ufe70-\ufeff'),
    ('Hebrew', '\u0590-\u05ff'),
    ('Devanagari', '\u0900-\u097f'),
    ('Thai', '\u0e00-\u0e7f'),
    ('Emoji / symbols', '\u2300-\u23ff\u2b00-\u2bff\U0001f000-\U0001faff'),
)

# The allow-list. A reply may use anything in these ranges:
#   \u0000-\u024f  Basic Latin, Latin-1 Supplement, Latin Extended-A and -B
#   \u0250-\u02af  IPA extensions (Latin-script phonetics)
#   \u1d00-\u1d7f  Phonetic extensions (Latin letters)
#   \u1e00-\u1eff  Latin Extended Additional / Extended-C
#   \u2c60-\u2c7f  Latin Extended-C
#   \ua720-\ua7ff  Latin Extended-D
#   \u2000-\u206f  General punctuation (curly quotes, dashes, ellipsis, narrow spaces)
#   \u2070-\u209f  Super/subscripts
#   \u20a0-\u20cf  Currency symbols
#   \u2100-\u214f  Letterlike symbols (e.g. the numero sign, the degree sign)
#   \u2190-\u21ff  Arrows
#   \u2200-\u22ff  Mathematical operators
#   \u2500-\u257f  Box drawing (scene separators)
#   \u25a0-\u25ff  Geometric shapes (bullets, decorative separators)
#   \u2600-\u27bf  Miscellaneous symbols + dingbats (a reply may use a star or a heart)
#   \ufb00-\ufb4f  Latin ligatures (ff, fi, fl, ...)
#   \ufffd          The Unicode replacement character (a decode artefact, not a script)
# Everything else -- including the emoji planes, which sit above the BMP -- is flagged.
LATIN_ALLOWED_PATTERN = (
    r'[\u0000-\u024f\u0250-\u02af\u1d00-\u1d7f\u1e00-\u1eff'
    r'\u2c60-\u2c7f\ua720-\ua7ff'
    r'\u2000-\u206f\u2070-\u209f\u20a0-\u20cf\u2100-\u214f'
    r'\u2190-\u21ff\u2200-\u22ff\u2500-\u257f\u25a0-\u25ff\u2600-\u27bf'
    r'\ufb00-\ufb4f\ufffd]'
)

_NON_LATIN_ALLOWED_RE = re.compile(LATIN_ALLOWED_PATTERN)
_NON_LATIN_SCRIPT_RES = tuple(
    (name, re.compile('[%s]' % ranges)) for name, ranges in NON_LATIN_SCRIPT_RANGES)


def _non_latin_script_name(character):
    """Name the script *character* belongs to, or ``"Other non-Latin"``.

    Only used to make the report readable; the flag itself comes from the allow-list.
    """
    for name, pattern in _NON_LATIN_SCRIPT_RES:
        if pattern.match(character):
            return name
    return "Other non-Latin"


def find_non_latin_characters(text):
    """Return ``[(script_name, character, sentence), ...]`` for every non-Latin character.

    One entry per DISTINCT script (not per character): a Chinese sentence would otherwise
    produce hundreds of identical rows, and one entry is all the fix instruction needs. The
    reported sentence is the one that holds the first offending character, trimmed and capped.
    """
    if not text:
        return []
    text = str(text)
    found, seen = [], set()
    for match in re.finditer(r'[^\x00-\x7f]', text):
        character = match.group(0)
        if _NON_LATIN_ALLOWED_RE.match(character):
            continue
        script = _non_latin_script_name(character)
        if script in seen:
            continue
        seen.add(script)
        found.append((script, character, _sentence_around(text, match.start(), match.end())))
    return found


def is_non_latin(text):
    """Detect characters outside the Latin script (Chinese, Cyrillic, Greek, emoji, ...).

    The check is an allow-list over Unicode ranges, so it needs no LLM call and no phrase list:
    a character is accepted when it is Latin script (any accent or extension), a digit, a space,
    or one of the punctuation/symbol blocks a roleplay reply legitimately uses, and flagged
    otherwise. ASCII replies are the fast path -- ``text.isascii()`` returns immediately.

    Same contract as the other ``is_*()`` checks: ``(bool, [(label, sentence), ...])`` plus a
    timestamp in ``issue_timestamps['non_latin']`` (1-hour rolling window) so the dashboard's
    metric card, issue panel and graph update on their own.
    """
    if not text or not str(text).strip():
        return False, []
    text = str(text)
    if text.isascii():
        return False, []  # fast path: pure ASCII is pure Latin
    hits = find_non_latin_characters(text)
    if not hits:
        return False, []
    if issue_timestamps is not None and issue_timestamps_lock is not None:
        with issue_timestamps_lock:
            issue_timestamps.setdefault('non_latin', []).append(time.time())
            cutoff = time.time() - 3600
            issue_timestamps['non_latin'] = [t for t in issue_timestamps['non_latin'] if t > cutoff]
    return True, [("Non-Latin script (%s)" % script, sentence)
                  for script, _character, sentence in hits]


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

# --- Missing quotations (v11.7.2) -----------------------------------------------------------
# Complements is_incomplete_quote(): that check catches a reply whose quote marks are
# UNBALANCED (one missing), while is_missing_quotation() catches a reply whose spoken
# line is missing its quote marks ENTIRELY -- "Its funny she says" instead of
# "\"Its funny,\" she says." The generated question/continuation prompts ask the model to
# put speech in quotation marks, so a reply with narration + a clear speech-attribution
# verb but no " / ' / ‘ / “ quote glyph at all is treated as a missing-quotations defect.
# It is opt-in (Experiment: Retry missing quotations) and, when on, sends the answer back
# to the SAME main API for one retry before the turn is kept.
_MISSING_QUOTATION_ATTRIBUTION_RE = re.compile(
    r'\b(?:says?|said|shouted|whispered|whispers?|murmured|muttered|called|cried|asked|' 
    r'replied|answered|retorted|exclaimed|gasped|laughed|sighs?|groaned|yawned|hissed|' 
    r'snarled|growled|told|telling|asks?|speaks?|spoke|replies?)\b', re.IGNORECASE)

# A word-internal apostrophe is a contraction or a possessive (don't, Melody's), never a
# quotation mark. Stripping those BEFORE looking for quote glyphs is what keeps a reply like
# "It's not funny she says" detectable: the previous guard bailed out on ANY apostrophe, so
# ordinary prose -- which is full of contractions -- silently disabled the whole check and let
# mark-less spoken lines through to the dataset. Only apostrophes BETWEEN word characters are
# removed, so genuine single-quoted dialogue ('Its funny,' she says) still counts as quoted.
# A plural possessive (dogs') is left alone on purpose: treating it as a quotation mark only
# suppresses the check, which is the safe direction (no pointless retry).
_APOSTROPHE_IN_WORD_RE = re.compile(r"(?<=\w)['\u2019](?=\w)")
_DIALOGUE_QUOTE_MARKS = ('"', "'", '\u2018', '\u2019', '\u201c', '\u201d')


def has_dialogue_quote_marks(text):
    """True when ``text`` carries a real quotation mark (contractions do not count).

    ``is_missing_quotation()`` uses this to answer "did the model mark its speech at all?".
    Apostrophes inside a word are contractions/possessives, so they are stripped first;
    what survives is checked for any straight or curly single/double quote glyph.
    """
    if not text:
        return False
    return any(mark in _APOSTROPHE_IN_WORD_RE.sub("", str(text))
               for mark in _DIALOGUE_QUOTE_MARKS)


def is_missing_quotation(text):
    """Detect a spoken line that is missing its quotation marks entirely.

    Returns ``(True, [(label, snippet), ...])`` only when ALL of the following hold:
    the text has words to check, it contains NO quotation mark of any kind once
    contractions/possessives are discounted (see ``has_dialogue_quote_marks()``), it
    narrates (more than a handful of words), and it carries a speech-attribution verb
    (\"...she says\", \"he said\", ...).

    Deliberate non-hits: any reply containing at least one real quote glyph (the balanced
    case is handled by ``normalize_quotes`` and unbalanced ones by ``is_incomplete_quote``),
    pure narration with no speech attribution, single-word fragments, and empty input.
    Mirrors the other ``is_*()`` shape; it records no timestamp because it is an
    experimental, opt-in retry trigger rather than a dashboard-tracked issue type.
    """
    if not text or not str(text).strip():
        return False, []
    text = str(text)
    if has_dialogue_quote_marks(text):
        return False, []
    words = text.split()
    if len(words) < 4:
        return False, []
    if not _MISSING_QUOTATION_ATTRIBUTION_RE.search(text):
        return False, []
    snippet = text.strip()[:150]
    label = "Missing quotation (spoken line with no quotation marks)"
    return True, [(label, snippet + ("..." if len(text.strip()) > 150 else ""))]
