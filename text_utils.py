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

# --- {style} instruction leak (v13.2.1) ------------------------------------
#
# `prompts.style` hands the model one style per conversation and every prompt that names it
# interpolates it as "Begin this message as follows: {style}". Some models echo that clause back
# as the first line of the reply instead of writing prose, and because the style strings are
# imperatives ("open on a sound...") the echo reads like a stage direction at the top of the
# message:
#
#     open on someone entering the shop, the bell's chime cutting through the silence.
#
#     "You actually remembered," she whispers...
#
# The leak is never an EXACT copy of the pool entry -- the model paraphrases it ("someone
# entering the shop" rather than "someone entering or leaving"). Matching the configured strings
# therefore cannot work, so this matches the imperative VERB the style always starts with: a short
# first line whose first word is one of those verbs and whose first clause ends in a period.
# Ordinary prose is unaffected because "open" is not used as a sentence-initial bare imperative
# in the middle of a roleplay reply.
STYLE_LEAD_VERBS = (
    "open", "begin", "start", "frame", "kick", "launch", "commence", "lead",
)
# A leaked instruction line: the verb, then at most ~14 words, then the end of its clause.
_STYLE_INSTRUCTION_LINE_PATTERN = re.compile(
    r"^\s*(?:" + "|".join(STYLE_LEAD_VERBS) + r")\b[^.\n]{0,120}\.\s*$",
    re.IGNORECASE,
)


def strip_style_instruction_leak(text):
    """Remove a leaked ``{style}`` instruction from the start of a generated message.

    Only the leading instruction is touched: the first non-empty line is dropped when it reads as an
    imperative directive, together with the blank lines that followed it. Everything after the first
    real line of prose is left exactly as generated, so a character who legitimately says "Open the
    door." later in the message is untouched.

    A message whose FIRST line is the only line, or whose first line does not match, is returned
    unchanged -- the guard is deliberately conservative, because deleting a real opening sentence
    would be a worse failure than leaving one ugly instruction in place.
    """
    if not text:
        return text

    lines = text.split("\n")

    # Find the first non-empty line; anything above it is leading whitespace we keep as-is.
    first = next((index for index, line in enumerate(lines) if line.strip()), None)
    if first is None:
        return text

    # The first line must be the ONLY content of its paragraph -- a leaked instruction sits on its own
    # line, so a line that continues into prose is not one.
    if not _STYLE_INSTRUCTION_LINE_PATTERN.match(lines[first]):
        return text

    # Refuse to strip when doing so would leave nothing behind: a message that is nothing but the
    # instruction is a failed generation, not a leak with prose after it.
    remaining = [line for line in lines[first + 1:] if line.strip()]
    if not remaining:
        return text

    # Drop the instruction and the blank lines that separated it from the prose, keeping any
    # leading whitespace that preceded it.
    keep_from = first + 1
    while keep_from < len(lines) and not lines[keep_from].strip():
        keep_from += 1
    return "\n".join(lines[:first] + lines[keep_from:])


# `prompts.scene_cast` appends a per-reply SCENE CAST note to the reply's final user message (or
# its system prompt, on 'system' placement). Some models echo that note back as the first line of
# the reply instead of writing prose, so the saved message opens with the schedule itself:
#
#     SCENE CAST
#     Samara: PRESENT
#     Ashlyn: LEAVES
#     Sage: PRESENT
#
#     I watch Ashlyn stride away ...
#
# The echo is also STALE -- it is the note for the reply being written, so a row that kept it
# repeats "Ashlyn: LEAVES" on every later turn, long after she left. This is the same class of bug
# as the v13.2.1 `{style}` instruction leak above, and is guarded the same conservative way.
#
# Two shapes are shipped (`prompts.scene_cast.shape`), and both must be recognised:
#   - 'minimal':  NAME: PRESENT / NAME: LEAVES / NAME: ARRIVES, one per line
#   - 'full':     PRESENT: a, b / NOT IN THE SCENE: c / LEAVES: c / ARRIVES: c
_SCENE_CAST_HEADER_PATTERN = re.compile(r"^\s*scene\s+cast\s*:?\s*$", re.IGNORECASE)
# One line of the block, in either shape. Deliberately narrow: a name (or the PRESENT /
# NOT IN THE SCENE / LEAVES / ARRIVES keyword), a colon, and the rest of the line. A prose line
# that happens to follow the header would have to look exactly like a cast entry to be eaten.
_SCENE_CAST_ENTRY_PATTERN = re.compile(
    r"^\s*(?:"
    r"(?:present|not\s+in\s+the\s+scene|leaves|arrives)\s*:"
    r"|[^:\n]{1,60}:\s*(?:present|leaves|arrives)\b"
    r")",
    re.IGNORECASE,
)


def strip_scene_cast_leak(text):
    """Remove a leaked ``SCENE CAST`` note from the start of a generated message.

    Only the leading block is touched: the first non-empty line must be the ``SCENE CAST`` header
    itself, and the lines that follow it are consumed for as long as they still read as cast entries.
    The blank lines that separated the block from the prose go with it, and any leading whitespace
    above the block is preserved. Everything from the first real line of prose onwards is left
    exactly as generated.

    A message whose first line is not the header, or whose block is the ONLY content, is returned
    unchanged -- the guard is deliberately conservative. A message that is nothing but the note is a
    failed generation, not a leak with prose after it, and deleting a real opening sentence would be
    a worse failure than leaving one ugly note in place.
    """
    if not text:
        return text

    lines = text.split("\n")

    # Find the first non-empty line; anything above it is leading whitespace we keep as-is.
    first = next((index for index, line in enumerate(lines) if line.strip()), None)
    if first is None:
        return text

    if not _SCENE_CAST_HEADER_PATTERN.match(lines[first]):
        return text

    # Consume the block: every following line that still looks like a cast entry. A blank line ends
    # it, so a note followed by prose is stripped and prose that merely mentions a name is not.
    end = first + 1
    while end < len(lines) and lines[end].strip() and _SCENE_CAST_ENTRY_PATTERN.match(lines[end]):
        end += 1

    # Refuse to strip when doing so would leave nothing behind: a message that is nothing but the
    # note is a failed generation, not a leak with prose after it.
    remaining = [line for line in lines[end:] if line.strip()]
    if not remaining:
        return text

    # Drop the block and the blank lines that separated it from the prose, keeping any leading
    # whitespace that preceded it.
    keep_from = end
    while keep_from < len(lines) and not lines[keep_from].strip():
        keep_from += 1
    return "\n".join(lines[:first] + lines[keep_from:])



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
