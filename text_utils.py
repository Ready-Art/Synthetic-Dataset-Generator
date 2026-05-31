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
    if not text: return text
    # 1. Keep your existing excessive quote collapsing...
    text = re.sub(r'"{2,}', '"', text)
    text = re.sub(r'"{2,}', '"', text)
    text = re.sub(r'"{2,}', '"', text)

    # 2. Replace parity logic with structural balancing
    result = []
    straight_open = False
    curly_open = False

    for char in text:
        if char == '"':
            if not straight_open:
                result.append('"')
                straight_open = True
            else:
                result.append('"')
                straight_open = False
        elif char == '"':
            if not curly_open:
                result.append('"')
                curly_open = True
            else:
                result.append('"')
                curly_open = False
        else:
            result.append(char)

    # Force-close any unclosed quotes at the end
    if straight_open: result.append('"')
    if curly_open: result.append('"')

    return "".join(result)
