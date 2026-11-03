# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

# detection.py
import collections
import difflib
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

# --- Scene Continuity (v12.0) --------------------------------------------------------------
# The Scene Cast feature lets characters join and leave a multi-character scene. The GENERATOR
# owns that schedule (generation._build_scene_cast_plan / _advance_cast_state); this section is
# the hard, LLM-free half of enforcing it: a character who has LEFT the scene (state 'exited')
# or has not ARRIVED yet (state 'absent') may not speak or act.
#
# Why code and not a prompt: an exited character with an attributed spoken line is an objective,
# mechanically detectable fact. Asking a model to notice it costs a request per turn and can be
# argued with; a regex cannot. This mirrors the OOC drift check, and like it the matching runs on
# NARRATION (quoted dialogue blanked) for the action half, so a character merely being *named* in
# someone else's line ("Where is Melody?") never flags.
#
# Attribution deliberately re-implements the closest-name heuristic from
# quality._spoken_lines_by_character() instead of importing it: quality.py imports app_state and
# the judge machinery, while detection.py is a leaf. Importing quality here would invert the
# dependency graph for ~30 lines of regex. Do not "deduplicate" this into quality.py.

# Dialogue spans, positions needed. Unlike _QUOTED_DIALOGUE_RE this variant refuses to treat a
# word-internal apostrophe as a quote, so "Melody's and Cole's hands" cannot produce a phantom
# span. Double quotes (straight + curly) and non-word-internal single quotes only.
_SCENE_DIALOGUE_SPAN_RE = re.compile(
    r'"[^"]*"'
    r'|\u201c[^\u201d]*\u201d'
    r"|(?<!\w)'[^']{1,400}'(?!\w)"
)

# Speech-attribution verbs: a quoted span near one of these is spoken, not just mentioned.
_SCENE_SPEECH_VERBS = (
    'say', 'says', 'said', 'saying', 'asks', 'asked', 'asking', 'replies', 'replied',
    'answers', 'answered', 'whispers', 'whispered', 'murmurs', 'murmured', 'mutters',
    'muttered', 'calls', 'called', 'cries', 'cried', 'shouts', 'shouted', 'snaps', 'snapped',
    'hisses', 'hissed', 'growls', 'growled', 'breathes', 'breathed', 'tells', 'told',
    'exclaims', 'exclaimed', 'gasps', 'gasped', 'sighs', 'sighed', 'laughs', 'laughed',
    'adds', 'added', 'begins', 'began', 'continues', 'continued', 'offers', 'offered',
    'warns', 'warned', 'insists', 'insisted', 'admits', 'admitted', 'retorts', 'retorted',
)
_SCENE_SPEECH_RE = re.compile(
    r'\b(?:' + '|'.join(_SCENE_SPEECH_VERBS) + r')\b', re.IGNORECASE)

# Narration action verbs: a blocked name immediately BEFORE one of these is doing something in
# the scene. Only used on the narration view (dialogue blanked).
_SCENE_ACTION_VERBS = (
    'nods', 'nodded', 'smiles', 'smiled', 'grins', 'grinned', 'laughs', 'laughed',
    'frowns', 'frowned', 'shrugs', 'shrugged', 'sighs', 'sighed', 'turns', 'turned',
    'steps', 'stepped', 'walks', 'walked', 'strides', 'strode', 'leans', 'leaned',
    'stands', 'stood', 'sits', 'sat', 'reaches', 'reached', 'grabs', 'grabbed',
    'takes', 'took', 'picks', 'picked', 'sets', 'set', 'places', 'placed',
    'looks', 'looked', 'stares', 'stared', 'glances', 'glanced', 'watches', 'watched',
    'crosses', 'crossed', 'opens', 'opened', 'closes', 'closed', 'lifts', 'lifted',
    'pours', 'poured', 'drinks', 'drank', 'eats', 'ate', 'touches', 'touched',
    'holds', 'held', 'pulls', 'pulled', 'pushes', 'pushed', 'kneels', 'knelt',
    'rises', 'rose', 'waves', 'waved', 'blinks', 'blinked', 'breathes', 'exhales',
)
_SCENE_ACTION_RE = re.compile(
    r'\b(?:' + '|'.join(_SCENE_ACTION_VERBS) + r')\b', re.IGNORECASE)

# How close narration has to be to a quote for the name in it to claim the line.
_ATTRIBUTION_BEFORE_CHARS = 60
_ATTRIBUTION_AFTER_CHARS = 40

# A bare speaker prefix: `Melody:` / `Melody -` immediately before the quote, with no verb at
# all. Script-style roleplay uses this constantly, so it must count as attribution on its own --
# requiring a speech verb here would let an exited character talk freely in that format.
_SPEAKER_PREFIX_RE = re.compile(r'[:\u2014\u2013-]\s*$')


def _scene_name_pattern(name):
    """Compile a whole-name matcher that survives names ending in punctuation.

    ``\b`` is wrong for a name like ``A.B.`` or ``Dr. Vance``: the trailing boundary needs a
    word character after a dot, so ``A.B. said`` would never match. Word-char lookarounds give
    the intended "whole name, not part of a longer word" semantics instead.
    """
    return re.compile(r'(?<!\w)' + re.escape(name) + r'(?!\w)', re.IGNORECASE)

# Violation kinds. Only the first two are HARD (they regenerate the reply); 'expected_missing'
# is reported but never rewrites, because a beat the model forgot is handled deterministically
# by the cast state machine (it defers, then cancels the exit / forces the arrival).
SCENE_CONTINUITY_HARD_KINDS = ('exited_speaks', 'exited_acts')
SCENE_CONTINUITY_SOFT_KINDS = ('expected_missing',)


def _scene_sentence_bounds(text, start, end):
    """(start, end) of the sentence in *text* containing the span [start, end)."""
    left = max(text.rfind('.', 0, start), text.rfind('!', 0, start),
               text.rfind('?', 0, start), text.rfind('\n', 0, start))
    right_candidates = [i for i in (text.find('.', end), text.find('!', end),
                                    text.find('?', end), text.find('\n', end)) if i != -1]
    right = min(right_candidates) + 1 if right_candidates else len(text)
    return left + 1, right


def _scene_snippet(text, start, end, limit=300):
    """The sentence containing [start, end), capped so a runaway reply cannot flood the log."""
    s, e = _scene_sentence_bounds(text, start, end)
    sentence = text[s:e].strip()
    if len(sentence) > limit:
        sentence = sentence[:limit] + '...'
    return sentence


def _closest_name(haystack, names, anchor):
    """The name whose occurrence in *haystack* sits closest to *anchor* (a position in the
    full text), or None. *haystack* is a (window_text, window_offset) pair."""
    window_text, window_offset = haystack
    if not window_text:
        return None
    best_name, best_distance = None, None
    for name in names:
        pattern = _scene_name_pattern(name)
        for match in pattern.finditer(window_text):
            distance = abs((window_offset + match.start()) - anchor)
            if best_distance is None or distance < best_distance:
                best_distance, best_name = distance, name
    return best_name


def find_attributed_speakers(text, names):
    """[(name, sentence), ...] for each quoted span attributable to one of *names*.

    A quoted span is attributed when the narration immediately around it (60 chars before,
    40 after) names the character AND carries a speech verb, with the CLOSEST name winning a
    tie. The span's own text is never searched, so a character merely being discussed
    ("Where is Melody?") is not attributed as the speaker.

    *names* are the scene's character names; a name is matched whole-word and
    case-insensitively, so accented and metacharacter-bearing names are safe.
    """
    if not text or not names:
        return []
    names = [str(n).strip() for n in names if str(n).strip()]
    if not names:
        return []
    text = str(text)
    out = []
    for span in _SCENE_DIALOGUE_SPAN_RE.finditer(text):
        before_start = max(0, span.start() - _ATTRIBUTION_BEFORE_CHARS)
        after_end = min(len(text), span.end() + _ATTRIBUTION_AFTER_CHARS)
        before = (text[before_start:span.start()], before_start)
        after = (text[span.end():after_end], span.end())
        narration = before[0] + ' ' + after[0]
        # Attribution needs EITHER a speech verb near the span OR a bare `Name:` speaker prefix
        # right before it (script-style dialogue, which carries no verb at all).
        if not (_SCENE_SPEECH_RE.search(narration)
                or _SPEAKER_PREFIX_RE.search(before[0])):
            continue
        name = _closest_name(before, names, span.start())
        if name is None:
            name = _closest_name(after, names, span.end())
        if name is not None:
            out.append((name, _scene_snippet(text, span.start(), span.end())))
    return out


def find_character_actions(text, names):
    """[(name, sentence), ...] for a name in NARRATION performing an action.

    Quoted dialogue is blanked FIRST, so a character speaking about someone else, or being
    spoken to, is never mistaken for acting. Within a sentence the name must PRECEDE the
    action verb, which keeps "Cole shrugged at Melody" from blaming Melody for the shrug.
    """
    if not text or not names:
        return []
    names = [str(n).strip() for n in names if str(n).strip()]
    if not names:
        return []
    text = str(text)
    narration = _strip_quoted_dialogue(text)
    out = []
    for name in names:
        name_re = _scene_name_pattern(name)
        for name_match in name_re.finditer(narration):
            tail = narration[name_match.end():]
            verb_match = _SCENE_ACTION_RE.search(tail)
            if not verb_match:
                continue
            # Reject a verb that belongs to a DIFFERENT sentence.
            between = tail[:verb_match.start()]
            if re.search(r'[.!?\n]', between):
                continue
            out.append((name, _scene_snippet(narration, name_match.start(), name_match.end())))
            break  # one report per character is enough to trigger a rewrite
    return out


def check_scene_continuity(text, blocked_names, expected_names=()):
    """Detect a cast continuity violation in one assistant reply.

    *blocked_names* are the characters who may NOT appear (cast states 'absent' / 'exited').
    *expected_names* are the characters whose entrance/exit this reply was supposed to
    narrate; a missing one is reported as a SOFT finding and never regenerates the turn.

    Returns ``(violation, [(kind, name, sentence), ...])`` where ``kind`` is one of
    ``exited_speaks`` / ``exited_acts`` (hard: the caller regenerates) or
    ``expected_missing`` (soft: the cast state machine handles it for free). On a hard hit a
    timestamp is appended to ``issue_timestamps['scene_continuity']`` (1-hour rolling window)
    so the dashboard picks it up like every other issue type.
    """
    findings = []
    blocked = [str(n).strip() for n in (blocked_names or []) if str(n).strip()]
    if text and blocked:
        for name, sentence in find_attributed_speakers(text, blocked):
            findings.append(('exited_speaks', name, sentence))
        for name, sentence in find_character_actions(text, blocked):
            findings.append(('exited_acts', name, sentence))
    for name in (expected_names or []):
        name = str(name).strip()
        if not name:
            continue
        if not _scene_name_pattern(name).search(str(text or '')):
            findings.append(('expected_missing', name, ''))

    hard = [f for f in findings if f[0] in SCENE_CONTINUITY_HARD_KINDS]
    if hard and issue_timestamps is not None and issue_timestamps_lock is not None:
        with issue_timestamps_lock:
            issue_timestamps['scene_continuity'].append(time.time())
            cutoff = time.time() - 3600
            issue_timestamps['scene_continuity'] = [
                t for t in issue_timestamps['scene_continuity'] if t > cutoff]
    return bool(hard), findings


# --- Repeated opener (v12.5) ---------------------------------------------------------------
# The failure this catches: a model that opens EVERY reply in a conversation the same way
# ("She lets out a breath.", "She lets out a laugh.", ...). It is not a banned phrase and not a
# wrong word -- each reply is individually fine -- so no phrase list can express it. What makes
# it a defect is the REPETITION across the conversation's assistant turns, which is why this
# check takes the earlier replies and not just the new one.
#
# Only the OPENING of a reply is compared, and only its first few words, because the opener is
# the position where the collapse is audible and where the model has the most freedom. A model
# that repeats a phrase mid-sentence is doing something else (a verbal tic), which the
# regex_watch / tic_density machinery already measures.
REPEATED_OPENER_WORDS = 4

# Opener tokens that carry no information: they open half the sentences in English, so two
# replies agreeing on them is not evidence of repetition. Comparing the CONTENT words is what
# makes "She lets out a breath" collide with "She lets out a laugh" while "She sets the cup
# down" does not collide with "She lets out a breath".
_OPENER_STOPWORDS = frozenset("""
a an the and but or so then now as if of to in on at for with from by
she he they it her his their its them him you i we
is was are were be been being has have had do does did
this that these those there here
""".split())

_OPENER_TOKEN_RE = re.compile(r"[^\W\d_]+", re.UNICODE)


def opener_tokens(text, limit=REPEATED_OPENER_WORDS):
    """Return the first *limit* informative words of *text*, lowercased.

    Stopwords and one-character words are dropped so the comparison is over content words:
    the opener of "She lets out a breath and turns away" is ``['lets', 'out', 'breath',
    'turns']``. Returns a tuple, so it can be compared and hashed directly.
    """
    if not text:
        return ()
    tokens = []
    for word in _OPENER_TOKEN_RE.findall(str(text)):
        lowered = word.lower()
        if len(lowered) < 2 or lowered in _OPENER_STOPWORDS:
            continue
        tokens.append(lowered)
        if len(tokens) >= limit:
            break
    return tuple(tokens)


def is_repeated_opener(text, previous_texts, min_shared_words=3):
    """Detect a reply whose opener repeats an earlier reply's opener.

    Returns ``(True, [(label, snippet), ...])`` in the same shape as the other ``is_*()``
    checks. *previous_texts* are the conversation's EARLIER assistant replies; the check is
    inherently a comparison, so an empty list can never hit and a first reply is always clean.

    The comparison is a PREFIX of the opening content words, not a set overlap, and it hits on
    either of two shapes:

    * the first *min_shared_words* words are identical, or
    * the first ``min_shared_words - 1`` are identical and the two openers then diverge.

    The second shape is not a loosening for its own sake -- it is the dominant real-world form.
    The reported tic is a fixed frame with a swapped object: "She lets out a breath", "She lets
    out a laugh", "She lets out a sigh" share only ``lets out`` yet are the same sentence, and a
    strict three-word match would miss every one of them while catching almost nothing else.
    A set overlap would instead fire on two replies that merely happen to reuse a common word
    later in the opener, which is not repetition of a frame.

    Openers shorter than *min_shared_words* content words never hit: too little text to call
    repetition, and staying quiet only suppresses a retry, which is the safe direction.

    Deliberately records NO timestamp: like ``is_missing_quotation()`` this is an opt-in
    retry trigger, not a dashboard-tracked issue type. The operator's own sample shows the tic
    compounding down a conversation (28.6% of first replies, 69.9% of fourth replies), so
    counting it as a per-conversation defect would swamp the issue graph with the normal case.
    """
    if not text or not previous_texts:
        return False, []
    try:
        wanted = max(2, int(min_shared_words))
    except (TypeError, ValueError):
        wanted = 3
    current = opener_tokens(text)
    if len(current) < wanted:
        return False, []
    for previous in previous_texts:
        earlier = opener_tokens(previous)
        if len(earlier) < wanted:
            continue
        shared = 0
        for left, right in zip(current, earlier):
            if left != right:
                break
            shared += 1
        if shared >= wanted - 1:
            label = "Repeated opener (matches an earlier reply)"
            snippet = " ".join(current[:REPEATED_OPENER_WORDS])
            return True, [(label, snippet)]
    return False, []


# A whole reply shorter than this many characters is never called a repeat: two one-line replies can
# coincide for innocent reasons ("She nods."), and staying quiet only suppresses a retry.
REPEATED_REPLY_MIN_CHARS = 120

# How much of the shorter reply two texts must share to count as a near-repeat. 0.92 catches the
# duplicated turn with a trailing space or a swapped name while leaving a genuinely rewritten reply
# (which differs in most of its sentences) alone.
REPEATED_REPLY_SIMILARITY = 0.92

_REPEATED_REPLY_WS_RE = re.compile(r"\s+", re.UNICODE)


def _normalize_for_repeat(text):
    """Whitespace-collapsed, case-folded form of *text* for whole-reply comparison."""
    return _REPEATED_REPLY_WS_RE.sub(" ", str(text or "")).strip().casefold()


def _similarity(left, right):
    """Similarity ratio of two ALREADY normalised strings, 0.0-1.0.

    ``difflib`` is used rather than a token overlap because the failure this detects is a verbatim
    repeat: a byte-identical pair must score exactly 1.0, and a pair that differs only in a trailing
    phrase should still score near 1.0. Both hold for ``SequenceMatcher`` and neither holds for a
    bag-of-words ratio on a long text with a small edit.
    """
    if left == right:
        return 1.0
    if not left or not right:
        return 0.0
    return difflib.SequenceMatcher(None, left, right, autojunk=False).ratio()


def is_repeated_reply(text, previous_texts, min_chars=REPEATED_REPLY_MIN_CHARS,
                      similarity=REPEATED_REPLY_SIMILARITY):
    """Detect a reply that repeats an EARLIER reply of the same conversation.

    Returns ``(True, [(label, snippet), ...])`` in the same shape as the other ``is_*()`` checks.

    This is the sibling of ``is_repeated_opener()``, not a replacement for it, and the two catch
    different failures. The opener check compares the first few CONTENT words and catches a model that
    reuses one sentence frame ("She lets out a breath" / "She lets out a laugh"). A verbatim repeat shares
    no frame with anything -- it *is* the earlier reply -- so comparing openers sees nothing. The reported
    defect was three byte-identical 884-character replies in one conversation, which only a whole-text
    comparison can see.

    The comparison is over the WHOLE reply: whitespace collapsed, case folded, then a similarity ratio.
    ``similarity=1.0`` is therefore an exact (modulo whitespace/case) match, and the default 0.92 also
    catches a repeat whose tail was slightly rewritten. Replies shorter than *min_chars* never hit: two short
    replies can coincide innocently and a false positive costs a wasted retry.

    Deliberately records NO timestamp: like ``is_missing_quotation()`` and ``is_repeated_opener()`` this is
    an opt-in retry trigger, not a dashboard-tracked issue type. It is also a consequence rather than a
    defect of the text -- a constant answer prompt produces it -- so counting it per conversation would
    report the operator's prompt rather than the dataset.
    """
    if not text or not previous_texts:
        return False, []
    try:
        floor = max(0, int(min_chars))
    except (TypeError, ValueError):
        floor = REPEATED_REPLY_MIN_CHARS
    current = _normalize_for_repeat(text)
    if len(current) < floor:
        return False, []
    try:
        threshold = float(similarity)
    except (TypeError, ValueError):
        threshold = REPEATED_REPLY_SIMILARITY
    threshold = min(1.0, max(0.5, threshold))
    for previous in previous_texts:
        earlier = _normalize_for_repeat(previous)
        if len(earlier) < floor:
            continue
        score = _similarity(current, earlier)
        if score >= threshold:
            label = "Repeated reply (matches an earlier reply verbatim)"
            snippet = " ".join(current.split()[:REPEATED_OPENER_WORDS])
            return True, [(label, snippet)]
    return False, []


# --- Overused first-reply opener ----------------------------------------------------
# The failure this catches: across a whole RUN, the first reply of most conversations opens the
# same way (a reported dataset had 65% of first replies opening "A sharp intake of" and 87% opening
# with "A"/"An"). Each conversation is fine on its own, so the per-conversation checks above cannot
# see it -- and the first reply has no earlier reply to compare against at all. Only a tally over
# the run's first replies shows the collapse, so this check takes the tally, not the conversation.
#
# Two keys, because the collapse happens at two grains:
#   * the FIRST WORD, stopwords included ("a"), which catches one sentence shape ("A <noun> of ...");
#   * the FRAME, the first two CONTENT words from opener_tokens() ("sharp intake"), which catches one
#     reaction beat whatever article or pronoun it starts with.
# A first word is naturally far more common than a frame ("She" can open a large share of honest
# replies), so each key has its own cap.

# No reply is judged until the run has recorded this many first replies: a share computed over a
# handful of replies is noise, and staying quiet only skips a retry.
OPENER_CAP_MIN_SAMPLES = 20

# How many words of the rejected reply are quoted back to the model as "the opening to avoid".
OPENER_CAP_SNIPPET_WORDS = 5

_OPENER_SNIPPET_RE = re.compile(r"\S+", re.UNICODE)


def opener_keys(text):
    """Return ``(first_word, frame)`` for the run-wide first-reply tally.

    ``first_word`` is the first word of *text*, lowercased, with stopwords kept ("A sharp intake of
    breath" gives ``"a"``); leading markup and quote marks are skipped because only letters count as a
    word. ``frame`` is the first two content words joined by a space (``"sharp intake"``). Either is
    ``""`` when the text has no such word.
    """
    words = _OPENER_TOKEN_RE.findall(str(text or ""))
    first_word = words[0].lower() if words else ""
    frame = " ".join(opener_tokens(text, limit=2))
    return first_word, frame


def opener_snippet(text, limit=OPENER_CAP_SNIPPET_WORDS):
    """The first *limit* whitespace-separated words of *text*, as written (for a fix instruction)."""
    return " ".join(_OPENER_SNIPPET_RE.findall(str(text or ""))[:limit])


def is_overused_opener(text, first_word_counts, frame_counts, total,
                       first_word_cap=0.0, frame_cap=0.0, min_samples=OPENER_CAP_MIN_SAMPLES):
    """Detect a first reply whose opener the run has already used more than its share.

    *first_word_counts* and *frame_counts* map a key from ``opener_keys()`` to how many of the run's
    *total* recorded first replies had it. A cap is the largest share (0-1) one key may hold; 0 or
    less turns that key's check off. A reply hits when its key ALREADY holds more than the cap, so a
    run converges on the cap rather than stopping one reply short of it, and nothing hits until
    *total* reaches *min_samples*.

    Returns ``(True, [(label, snippet), ...])`` in the same shape as the other ``is_*()`` checks; the
    snippet is the reply's opening words as written, for the fix instruction. Records NO timestamp,
    like ``is_repeated_opener()``: it is an opt-in retry trigger, not a dashboard-tracked issue type.
    """
    try:
        total = int(total)
        floor = max(1, int(min_samples))
    except (TypeError, ValueError):
        return False, []
    if total < floor or not text:
        return False, []
    first_word, frame = opener_keys(text)
    hits = []
    for name, key, counts, cap in (("first word", first_word, first_word_counts, first_word_cap),
                                   ("opening", frame, frame_counts, frame_cap)):
        try:
            cap = float(cap or 0.0)
        except (TypeError, ValueError):
            cap = 0.0
        if cap <= 0 or not key:
            continue
        share = (counts or {}).get(key, 0) / total
        if share > cap:
            hits.append(f"{name} '{key}' opens {share:.0%} of {total} first replies (cap {cap:.0%})")
    if not hits:
        return False, []
    label = "Overused first-reply opener (" + "; ".join(hits) + ")"
    return True, [(label, opener_snippet(text))]
