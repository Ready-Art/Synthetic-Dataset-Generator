# text_utils.py
import re

# --- Pre-compiled regex patterns for better performance ---
_EM_DASH_PATTERN = re.compile(r'\\—')
# NEW: Pattern to match two or more asterisks in a row
_EXCESSIVE_ASTERISKS_PATTERN = re.compile(r'\*{2,}')
# --- End of pre-compiled patterns ---

def remove_reasoning_text(text):
    """Removes ... style reasoning blocks from text if configured."""
    if re.search(r'', text, re.IGNORECASE) and not re.search(r'', text, re.IGNORECASE):
        return None
    match = re.search(r'\s*(.*)', text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text

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
