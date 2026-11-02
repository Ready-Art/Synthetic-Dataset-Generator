# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

# text_utils.py
import re

# --- Pre-compiled regex patterns for better performance ---
_EM_DASH_PATTERN = re.compile(r'[ \t]*—+[ \t]*')
# NEW: Pattern to match two or more asterisks in a row
_EXCESSIVE_ASTERISKS_PATTERN = re.compile(r'\*{2,}')
# NEW: A word repeated back-to-back in the same slot.
#
# A model can stall on a word and emit it two or more times in a row -- "Word. Word.",
# "word, word, word", "word word word". No phrase list can express "the same word
# twice", so this is handled as deterministic text cleanup rather than another retry.
#
# The rule is deliberately written as an EXCLUSION list of ordinary words rather than
# an inclusion list of the words to rewrite. A repeated ordinary word is frequently
# correct English -- "no no no", "yes yes yes", "very very good" are all deliberate
# emphasis -- so those are left alone, while a word that is not ordinary English is
# collapsed. Writing it this way keeps the module free of any word list that would be
# inappropriate to publish, and it generalises: an unusual word the model stalls on is
# caught without having been enumerated in advance.
#
# Four shapes are collapsed, because a stall is written more than one way:
#
#   * the word as consecutive one-word sentences -- "Word. Word." -> "Word."
#   * a comma-separated run ending the sentence -- "word, word, word." -> "word."
#   * a comma-separated run that continues -- "word, word, word text" -> "word text"
#   * a bare run with no separator -- "word word word" -> "word"
#
# Matching is case-insensitive and the replacement keeps the FIRST occurrence's casing,
# so a capitalised sentence opener stays capitalised.
_REPEAT_EXEMPT_WORDS = frozenset("""
a an the and but or if so then than that this these those it its
i you he she we they me my your his her our their
is was are were be been am do does did done have has had
not no nor yes yeah yep nah nope
to of in on at for with from by as
very really quite too also still even just only much many
all some any every each
oh ah ahh ohh ha haha heh hey hi hello bye
hmm mm uh um er erm well now right okay ok
sure fine good bad better best wow yay
please wait stop more again go come back
what why how when where who which
don t can cant dont wont isn t wasn t
""".split())

# The same word as consecutive one-word sentences: "Word. Word."
_REPEATED_SENTENCE_WORD_PATTERN = re.compile(
    r'\b([A-Za-z][A-Za-z\'-]*)([.!?])(?:\s+\1\2)+', re.IGNORECASE)
# A comma-separated run that ends the sentence: "word, word, word."
_REPEATED_COMMA_RUN_END_PATTERN = re.compile(
    r'\b([A-Za-z][A-Za-z\'-]*)(?:[,;]\s*\1\b)+\s*([.!?])', re.IGNORECASE)
# A comma-separated run that continues: "word, word, word text"
_REPEATED_COMMA_RUN_PATTERN = re.compile(
    r'\b([A-Za-z][A-Za-z\'-]*)(?:[,;]\s*\1\b)+', re.IGNORECASE)
# A bare space-separated run: "word word word"
_REPEATED_BARE_WORD_PATTERN = re.compile(
    r'\b([A-Za-z][A-Za-z\'-]*)(?:\s+\1\b)+', re.IGNORECASE)
# --- End of pre-compiled patterns ---

def remove_reasoning_text(text):
    """Removes <think>...</think> style reasoning blocks from text if configured."""
    text = re.sub(r'<think>.*?</think>\s*', '', text, flags=re.DOTALL | re.IGNORECASE)
    # A </think> with no opening tag: everything before it is reasoning
    text = re.split(r'</think>', text, flags=re.IGNORECASE)[-1]
    # An unclosed <think> (e.g. cut off at max tokens) is reasoning to the end
    return re.split(r'<think>', text, maxsplit=1, flags=re.IGNORECASE)[0].strip()

def remove_em_dash(text):
    """Removes em dash (—) characters from text."""
    return _EM_DASH_PATTERN.sub(' ', text)

def ensure_space_after_line_break(text):
    """Ensures there is a space after line breaks."""
    pattern1 = re.compile(r'(\r?\n)(\S)')
    text = pattern1.sub(r'\1 \2', text)
    pattern2 = re.compile(r'\n\s*\n')
    text = pattern2.sub('\n\n', text)
    return text

def remove_excessive_asterisks(text):
    """Removes groups of two or more asterisks from text."""
    return _EXCESSIVE_ASTERISKS_PATTERN.sub('', text)

def remove_asterisk_space_asterisk(text):
    """Removes '* *' patterns from text."""
    pattern = re.compile(r'\s*\*\s+\*\s*')
    return pattern.sub(' ', text)

def remove_all_asterisks(text):
    """Removes all asterisk characters from text."""
    return re.sub(r'\*', '', text)

def remove_markdown(text):
    """Removes markdown formatting, converting it to plain text while preserving content."""
    if not text:
        return text

    # Remove image markdown: ![alt](url) -> alt
    text = re.sub(r'!\[([^\]]*)\]\([^)]*\)', r'\1', text)

    # Remove link markdown: [text](url) -> text
    text = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', text)

    # Remove code block backticks
    text = text.replace('```', '').replace('`', '')

    # Remove heading markers (# at start of line)
    text = re.sub(r'^\s*#{1,6}\s+', '', text, flags=re.MULTILINE)

    # Remove bold/italic/strikethrough markers
    text = re.sub(r'(\*{1,3}|_{1,3}|~{2})(.*?)(\1)', r'\2', text)

    # Remove blockquote markers
    text = re.sub(r'^\s*>\s*', '', text, flags=re.MULTILINE)

    # Remove horizontal rules
    text = re.sub(r'^\s*[-*_]{3,}\s*$', '', text, flags=re.MULTILINE)

    # Remove unordered list markers (-, *, +)
    text = re.sub(r'^\s*[-*+]\s+', '', text, flags=re.MULTILINE)

    # Remove ordered list markers (1., 2., etc.)
    text = re.sub(r'^\s*\d+\.\s+', '', text, flags=re.MULTILINE)

    return text.strip()

def normalize_quotes(text):
    if not text:
        return text

    # 1. Collapse runs of each quote type into a single mark.
    #    These three patterns target DIFFERENT characters: straight ("),
    #    curly-open (U+201C) and curly-close (U+201D). They are written with
    #    explicit \u escapes ON PURPOSE so that an editor, paste, or
    #    "smart-quote normalizer" can't silently flatten the curly literals
    #    back to straight quotes -- which is exactly what previously broke this
    #    function (all three subs and the if/elif below had collapsed to the
    #    same straight-quote glyph, so curly quotes were never balanced and the
    #    straight branch force-appended stray quotes).
    text = re.sub(r'"{2,}', '"', text)              # straight  "
    text = re.sub('\u201c{2,}', '\u201c', text)     # curly open  U+201C
    text = re.sub('\u201d{2,}', '\u201d', text)     # curly close U+201D

    # 2. Balance CURLY quotes by direction. Curly quotes carry their own
    #    open/close identity, so an imbalance is unambiguous: append the missing
    #    closers at the end, or prepend the missing openers at the start.
    n_open = text.count('\u201c')
    n_close = text.count('\u201d')
    if n_open > n_close:
        text += '\u201d' * (n_open - n_close)
    elif n_close > n_open:
        text = '\u201c' * (n_close - n_open) + text

    # 3. Straight quotes use the SAME glyph for open and close, so an odd count
    #    is ambiguous -- we cannot know where the missing quote belongs. The old
    #    code force-appended a closing " on odd parity, which produced stray
    #    trailing quotes whenever a passage mixed straight and curly quotes
    #    (e.g. an inch mark like 6", or a lone straight quote inside curly
    #    dialogue). We deliberately do NOT guess here; is_incomplete_quote() plus
    #    the regeneration retry loop are the right place to handle a genuinely
    #    unbalanced straight quote.
    return text

def collapse_repeated_words(text):
    """Collapse a word the model repeated back-to-back in the same slot.

    Deterministic post-processing for the stall where a reply emits one word two or
    more times in a row: "Word. Word. The rest." becomes "Word. The rest.", and
    "word, word, word text" becomes "word text".

    Words in ``_REPEAT_EXEMPT_WORDS`` are never collapsed. That exclusion is the
    whole safety story: a repeated ordinary word is frequently correct English --
    "no no no", "yes yes yes", "very very good" are all deliberate emphasis -- so
    a general "collapse any repeated word" rule would rewrite valid prose. Every word
    in that set is ordinary English, so the module carries no word list that would be
    inappropriate to publish, and an unusual word the model stalls on is still caught
    without having been enumerated in advance.

    A stall is written more than one way, so each of these is collapsed the same
    way: as consecutive one-word sentences ("Word. Word."), as a comma-separated run
    with or without a full stop after it, and with no separator at all. Matching is
    case-insensitive and the first occurrence's casing is kept.

    Safe on empty input and on text with no repetition: returns it unchanged.
    """
    if not text:
        return text

    def _keep_first_sentence(match):
        word, punct = match.group(1), match.group(2)
        if word.lower() in _REPEAT_EXEMPT_WORDS:
            return match.group(0)
        return word + punct

    def _keep_first_comma_end(match):
        word, punct = match.group(1), match.group(2)
        if word.lower() in _REPEAT_EXEMPT_WORDS:
            return match.group(0)
        return word + punct

    def _keep_first_word(match):
        word = match.group(1)
        if word.lower() in _REPEAT_EXEMPT_WORDS:
            return match.group(0)
        return word

    # "Word. Word." -> "Word."
    text = _REPEATED_SENTENCE_WORD_PATTERN.sub(_keep_first_sentence, text)
    # "word, word, word." -> "word."
    text = _REPEATED_COMMA_RUN_END_PATTERN.sub(_keep_first_comma_end, text)
    # "word, word, word text" -> "word text"
    text = _REPEATED_COMMA_RUN_PATTERN.sub(_keep_first_word, text)
    # "word word word" -> "word"
    text = _REPEATED_BARE_WORD_PATTERN.sub(_keep_first_word, text)
    return text

def repair_straight_quotes(text):
    """
    Attempts to repair unbalanced straight quotes using structural heuristics.
    Only applies when the count is odd (one missing quote).
    """
    if text.count('"') % 2 == 0:
        return text  # Already balanced

    # Strategy: find the most likely position for the missing quote.
    # Heuristic 1: If the text starts with a word that's commonly dialogue
    #   (e.g., a capital letter followed by lowercase), prepend an opening quote.
    # Heuristic 2: If the last word ends with punctuation inside what looks
    #   like a sentence, append a closing quote.
    # Heuristic 3: Look for the longest span between existing quotes and
    #   check if it looks like a complete sentence (ends with ., !, ?).

    stripped = text.strip()

    # If the text ends with punctuation + closing context, likely missing an opener
    if re.search(r'[.!?]\s*$', stripped) and not stripped.startswith('"'):
        return '"' + stripped

    # If the text starts with a capital and looks like dialogue, likely missing a closer
    if re.match(r'^[A-Z][a-z]+', stripped) and not stripped.endswith('"'):
        return stripped + '"'

    # Fallback: append at end (least bad option)
    return stripped + '"'
