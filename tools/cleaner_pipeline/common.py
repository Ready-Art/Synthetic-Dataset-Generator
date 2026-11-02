#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""
Shared helpers for the JSONL cleanup pipeline.

Every step script is a *pure filter*: it removes characters and writes a new
JSONL. No step reformats text. Whitespace normalisation is isolated in step 04
so that the pipeline is composable and each stage can be verified on its own.
"""

import json
import os
import re
import sys
from collections import Counter

# ---------------------------------------------------------------------------
# Chat-template special tokens.
# Matched as EXACT strings, longest-first, so no partial residue is left.
# Never use character-class stripping - that is what turned "<turn|>" into
# "turn|" and "<|channel>" into "|channel" in the original broken cleaner.
# ---------------------------------------------------------------------------
SPECIAL_TOKENS = [
    # present in this corpus
    "<turn|>", "<|channel>", "<channel|>",
    # ChatML / Qwen
    "<|im_start|>", "<|im_end|>", "<|endoftext|>",
    "<|object_ref_start|>", "<|object_ref_end|>",
    "<|fim_prefix|>", "<|fim_middle|>", "<|fim_suffix|>",
    # Llama 3
    "<|begin_of_text|>", "<|end_of_text|>", "<|start_header_id|>",
    "<|end_header_id|>", "<|eot_id|>",
    # Phi / generic role tokens
    "<|user|>", "<|assistant|>", "<|system|>", "<|end|>", "<|sep|>",
    "<|message|>", "<|constrain|>", "<|channel|>",
    # Llama 2 / Mistral
    "[INST]", "[/INST]", "<<SYS>>", "<</SYS>>",
    # Gemma
    "<start_of_turn>", "<end_of_turn>", "<bos>", "<eos>",
    # padding / specials
    "<pad>", "<unk>", "<mask>", "<cls>", "<s>", "</s>",
    # other sentinel styles
    "<|turn>", "</turn|>", "<turn>",
]
SPECIAL_TOKENS = sorted(set(SPECIAL_TOKENS), key=len, reverse=True)


# ---------------------------------------------------------------------------
# Whitespace-tolerant matching.
#
# Exact-string removal is not enough: the generator sometimes emits the same
# sentinel with spaces injected between its characters, e.g.
#
#     <turn|>      <turn |>      <turn | >      < turn |  >
#
# A literal str.replace("<turn|>") misses all of those and leaves the token in
# the corpus. Every token is therefore compiled to a regex that allows optional
# spaces/tabs between its characters, and tokens are tried longest-first so no
# shorter pattern can eat part of a longer one.
# ---------------------------------------------------------------------------
def flex_token(tok, ignorecase=False):
    """Compile a token so internal spaces/tabs are tolerated."""
    body = r"[ \t]*".join(re.escape(ch) for ch in tok)
    return re.compile(body, re.IGNORECASE if ignorecase else 0)


# Case-insensitive: the generator occasionally upper-cases a sentinel
# ("<TURN | >"). No English prose legitimately contains these shapes, so
# folding case cannot cause a false positive.
TOKEN_REGEXES = [(tok, flex_token(tok, ignorecase=True)) for tok in SPECIAL_TOKENS]

# Whole chat-template leak block, removed as one unit BEFORE any single-token
# pass, because "thought" is otherwise a legitimate English word (948 hits).
# Tolerant of spaces between characters and of the newline that the template
# inserts inside the block.
CHANNEL_BLOCK_RE = re.compile(
    r"<[ \t]*turn[ \t]*\|?[ \t]*>[ \t\n]*"
    r"<[ \t]*\|[ \t]*channel[ \t]*>[ \t\n]*thought[ \t\n]*"
    r"<[ \t]*channel[ \t]*\|[ \t]*>",
    re.IGNORECASE,
)
CHANNEL_ONLY_RE = re.compile(
    r"<[ \t]*\|[ \t]*channel[ \t]*>[ \t\n]*thought[ \t\n]*"
    r"<[ \t]*channel[ \t]*\|[ \t]*>",
    re.IGNORECASE,
)

def sub_guarded(pattern, text):
    """Remove every match, but never glue two words together.

    A plain ``pattern.sub("", text)`` can concatenate the characters on either
    side of a removed token. This corpus has a live case:

        'Thud.<turn|>\\n <|channel>thought\\n <channel|>The next morning'

    Removing the whole channel block leaves ``'Thud.The next morning'`` -
    a sentence welded to the next one. Where a removal would join two
    non-whitespace characters, a single space is inserted instead, so the
    boundary survives as a word break.

    Returns (new_text, n_removed).
    """
    out = []
    pos = 0
    n = 0
    for m in pattern.finditer(text):
        n += 1
        out.append(text[pos:m.start()])
        before = text[m.start() - 1] if m.start() > 0 else ""
        after = text[m.end()] if m.end() < len(text) else ""
        if before and after and not before.isspace() and not after.isspace():
            out.append(" ")
        pos = m.end()
    if not n:
        return text, 0
    out.append(text[pos:])
    return "".join(out), n


# Characters that are never legitimate prose. Newline (U+000A) is preserved -
# it carries the two-paragraph shape of the assistant targets.
CONTROL_CHARS = frozenset(
    set(range(0x00, 0x09)) | set(range(0x0b, 0x20)) | {0x7f}
    | {0xad, 0x200b, 0x200c, 0x200d, 0x200e, 0x200f,
       0x2028, 0x2029, 0x2060, 0xfeff, 0xfffd}
)
TAB_RE = re.compile(r"[\t\v\f]")

# Whitespace rules. Newlines are never collapsed, and the "\n\n " paragraph
# convention is never touched.
#
# The `leading spaces` rule carries a negative lookbehind on purpose. Without
# it, `\n[ \t]+` matches the single space that follows the blank line in this
# corpus's paragraph break, turning
#
#     '...the door closed.\n\n "I can feel it..."'
#
# into
#
#     '...the door closed.\n\n"I can feel it..."'
#
# and silently destroying the paragraph indent on all 5,896 turns that use it.
# `(?<!\n)` makes the rule fire only on a newline that is NOT the second of a
# blank-line pair, i.e. only on genuine wrapped-line indents.
WHITESPACE_RULES = [
    ("run of spaces",      re.compile(r"[ ]{2,}"),           " "),
    ("space before punct", re.compile(r" +([,.;:!?])"),      r"\1"),
    ("trailing spaces",    re.compile(r"[ \t]+\n"),          "\n"),
    ("leading spaces",     re.compile(r"(?<!\n)\n[ \t]+"),  "\n"),
    ("excess blank lines", re.compile(r"\n{3,}"),           "\n\n"),
]


# ---------------------------------------------------------------------------
# Source dataset resolution.
#
# The pristine input is `input.jsonl`. The old name `unclean.jsonl` is only kept
# as a last-resort fallback so existing checkouts keep working; nothing in the
# pipeline writes to or depends on that name any more.
#
# Resolution order:
#   1. explicit argument (e.g. 07's --input)
#   2. $SOURCE
#   3. ./input.jsonl, then ../input.jsonl
#   4. ./unclean.jsonl, then ../unclean.jsonl   (legacy, warned about)
# ---------------------------------------------------------------------------
SOURCE_NAME = "input.jsonl"
LEGACY_SOURCE_NAME = "unclean.jsonl"
_SOURCE_CANDIDATES = [SOURCE_NAME, os.path.join("..", SOURCE_NAME),
                      LEGACY_SOURCE_NAME, os.path.join("..", LEGACY_SOURCE_NAME)]


def resolve_source(explicit=None):
    """Return the path of the pristine source dataset, or exit with a message."""
    if explicit:
        if not os.path.isfile(explicit):
            raise SystemExit(f"ERROR: source not found: {explicit}")
        return explicit

    env = os.environ.get("SOURCE")
    if env:
        if not os.path.isfile(env):
            raise SystemExit(f"ERROR: $SOURCE not found: {env}")
        return env

    for path in _SOURCE_CANDIDATES:
        if os.path.isfile(path):
            if os.path.basename(path) == LEGACY_SOURCE_NAME:
                print(f"WARNING: using legacy source name {path}; "
                      f"rename it to {SOURCE_NAME}", file=sys.stderr)
            return path

    raise SystemExit("ERROR: no source dataset found; tried "
                     + ", ".join(_SOURCE_CANDIDATES)
                     + " (override with SOURCE=/path/to/dataset.jsonl)")


def _bump(counts, key, n):
    if counts is not None and n:
        counts[key] += n


def strip_special_tokens(text, counts=None):
    """Remove every special token, tolerant of injected spaces.

    Removal is guarded (see sub_guarded): a token that sat between two words
    without surrounding whitespace becomes a space, not a weld.
    """
    text, n = sub_guarded(CHANNEL_BLOCK_RE, text)
    _bump(counts, "channel_block", n)
    text, n = sub_guarded(CHANNEL_ONLY_RE, text)
    _bump(counts, "channel_only", n)
    for tok, rx in TOKEN_REGEXES:
        text, n = sub_guarded(rx, text)
        _bump(counts, f"token {tok}", n)
    return text


def strip_control_chars(text, counts=None):
    """Tabs become a space; other control/invisible chars are deleted."""
    text, n = TAB_RE.subn(" ", text)
    _bump(counts, "tab->space", n)
    text, n = re.subn(
        "[" + "".join(re.escape(chr(c)) for c in sorted(CONTROL_CHARS)) + "]",
        "", text)
    _bump(counts, "control/invisible", n)
    return text


def normalise_whitespace(text, counts=None):
    """Tidy whitespace left behind by removals. Never collapses newlines."""
    for label, pattern, repl in WHITESPACE_RULES:
        text, n = pattern.subn(repl, text)
        _bump(counts, label, n)
    return text.strip()


def load(path):
    """Read a JSONL file into a list of records."""
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def save(records, path):
    """Write records back out as JSONL, preserving unicode."""
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def iter_turns(records):
    """Yield (record, turn_index, turn) for every turn in the dataset."""
    for rec in records:
        for idx, turn in enumerate(rec["conversations"]):
            yield rec, idx, turn


def integrity_check(before, after, label=""):
    """
    Verify structural invariants survived a filtering step.
    Returns True if all checks pass.
    """
    problems = []

    if len(before) != len(after):
        problems.append(f"record count changed: {len(before)} -> {len(after)}")

    for a, b in zip(before, after):
        if a["id"] != b["id"]:
            problems.append(f"id mismatch: {a['id']} != {b['id']}")
            break
        if len(a["conversations"]) != len(b["conversations"]):
            problems.append(f"turn count changed in {a['id']}")
            break
        ra = [t["from"] for t in a["conversations"]]
        rb = [t["from"] for t in b["conversations"]]
        if ra != rb:
            problems.append(f"role sequence changed in {a['id']}")
            break

    empty = [t for r in after for t in r["conversations"] if not t["value"].strip()]
    if empty:
        problems.append(f"{len(empty)} turn(s) became empty")

    print(f"\n--- integrity check {label} ---")
    if problems:
        for p in problems:
            print(f"  FAIL: {p}")
        return False
    print(f"  records      : {len(after)} (unchanged)")
    print(f"  turns/record : 8 (unchanged)")
    print(f"  roles        : preserved")
    print(f"  empty turns  : none")
    print("  PASS")
    return True


def report_counts(counts, title="removals"):
    """Pretty-print a Counter of removals."""
    print(f"\n--- {title} ---")
    if not counts:
        print("  nothing removed")
        return
    for key, val in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {key:30s} {val}")
    print(f"  {'TOTAL':30s} {sum(counts.values())}")


# ===========================================================================
# Truncation detection and repair (step 07).
#
# Measured on this corpus, truncation splits cleanly by role and needs a
# different repair on each side:
#
#   gpt   (8.74%, 495 turns)  multi-sentence prose. Almost every one has an
#                            internal sentence boundary and keeps most of its text,
#                            so trimming back to that boundary loses little.
#
#   human (3.11%, 176 turns)  single template
#                            `<action>, "<speech>" I <verb> [as|while <action>]`.
#                            All of them have an even number of quote marks, so the
#                            dialogue is closed; what is cut is the trailing
#                            attribution clause (`I grunt as`, `I grunt as the
#                            smell of hot`). Most contain no internal sentence
#                            boundary, so "trim to last sentence" is a no-op on
#                            this side. Fix: drop the severed clause and close
#                            the sentence.
#
# Both rules are *trims*: they only ever delete a suffix, never add prose. No
# invented text can enter the corpus.
# ===========================================================================

# A cut may only be made where the text before it already closes a clause: terminal
# punctuation, or a separator such as a comma. Used to validate a `quote` cut.
CLAUSE_CLOSE_RE = re.compile(r"[.,;:!?\u2026]$")

# How structurally clean each kind of cut is, best first. A repair picks the best
# available kind and only then the longest text, because a longer cut is not a
# better cut if it leaves a severed clause behind.
#
# Measured on this corpus: ranking purely by length produced 14 repairs that ended
# on a dangling `I <verb> while ...` (`...," I say while the smell of pine needles.`);
# ranking by kind first reduces that to 2, both of which are complete clauses.
CUT_RANK = {"sentence": 4, "attribution": 3, "clause": 2, "quote": 1}

# A turn is complete if it ends with terminal punctuation, optionally followed by
# closing quotes/brackets. A closing quote or bracket alone is not enough -
# `...on your skin,"` is still a severed sentence.
CLOSERS = '"\'\u201d\u2019)]}'
TERMINAL_RE = re.compile(r"[.!?\u2026][" + re.escape(CLOSERS) + r"]*$")

# Sentence boundary: terminal punctuation + optional closers, followed by
# whitespace or end of text. Lookahead (not a consuming group) so consecutive
# boundaries are all found.
SENTENCE_END_RE = re.compile(
    r"[.!?\u2026][" + re.escape(CLOSERS) + r"]*(?=\s|$)")

# Abbreviations whose trailing period must not be mistaken for a boundary.
ABBREVIATIONS = frozenset({
    "mr", "mrs", "ms", "dr", "st", "jr", "sr", "vs", "etc", "no",
    "e.g", "i.e", "a.m", "p.m", "mt", "ft", "prof", "rev", "gen",
})

# Attribution verbs of the human template: `... ," I <verb> as ...`.
# Derived from the clean turns at run time, with this list as a floor so the
# repair still works if the input is a different slice of the corpus.
ATTRIBUTION_VERBS = frozenset({
    "grunt", "mutter", "moan", "say", "sneer", "growl", "hiss", "breathe",
    "whisper", "murmur", "purr", "gasp", "pant", "groan", "snarl", "bark",
    "shout", "howl", "command", "sigh", "rasp", "coo", "drawl",
    "snap", "spit", "chuckle", "laugh", "add", "reply", "answer",
})

# The corpus's attribution template:  `...," I grunt`  /  `...," I grunt as ...`
ATTRIBUTION_RE = re.compile(r'["\u201d]\s*,?\s*I\s+([a-z]+)\b')

# Subordinators that open a trailing clause in this corpus's templates:
#     `I <action> while <action>`      `...," I <verb> as <action>`
# When a turn is cut mid-clause the severed clause almost always begins at the
# last of these, so cutting there leaves a complete main clause behind.
SUBORDINATORS = (" while ", " as ", " and ", " when ", " where ",
                 " because ", " so ", " but ", " that ", " with ", " which ",
                 " until ", " till ", " before ", " after ")

# Words that cannot end a complete clause. A candidate cut that leaves one of these
# as its final word is not a usable turn - it is still a mid-phrase cut.
DANGLE_WORDS = frozenset({
    "a", "an", "the", "my", "your", "his", "her", "its", "our", "their",
    "this", "that", "these", "those", "some", "any", "each", "every",
    "of", "to", "in", "on", "at", "with", "from", "into", "onto", "by",
    "about", "after", "before", "until", "since", "through", "against",
    "over", "under", "between", "than", "then", "like", "for", "upon",
    "and", "or", "but", "as", "while", "when", "if", "because", "so",
    "is", "are", "was", "were", "be", "been", "being", "am", "has",
    "have", "had", "do", "does", "did", "will", "would", "can", "could",
    "may", "might", "must", "not", "very", "just", "still", "too", "also",
    "up", "down", "out", "off", "more", "most", "least", "only",
})


def is_truncated(text):
    """True if the turn does not end on a complete sentence."""
    return not TERMINAL_RE.search(text.rstrip())


def sentence_boundaries(text, require_even_quotes=False):
    """End offsets of every complete sentence, abbreviations excluded."""
    out = []
    for m in SENTENCE_END_RE.finditer(text):
        before = text[:m.start()]
        word = re.findall(r"([A-Za-z.]+)$", before)
        if word and word[0].lower().strip(".") in ABBREVIATIONS:
            continue
        if require_even_quotes and text[:m.end()].count('"') % 2:
            continue
        out.append(m.end())
    return out


def _strip_trailing_separator(text):
    """Drop a dangling `,` / `;` / `:` left where a clause was cut."""
    return re.sub(r"[,;:]\s*$", "", text)


# Whole words used by NON-truncated turns of this corpus. Populated by
# learn_vocabulary() before any repair runs, and used to reject mid-word cuts: a
# cut leaving `aub`, `gutt` or `hes` behind is a fragment, not a word.
VOCABULARY = set()


def learn_vocabulary(records):
    """Collect every whole word used by a complete (non-truncated) turn."""
    vocab = set()
    for rec in records:
        for turn in rec["conversations"]:
            if is_truncated(turn["value"]):
                continue
            vocab.update(w.lower() for w in re.findall(r"[A-Za-z']+", turn["value"]))
    VOCABULARY.clear()
    VOCABULARY.update(vocab)
    return VOCABULARY


def final_word_known(text):
    """True if the last word is a whole word of this corpus, not a fragment."""
    words = re.findall(r"[A-Za-z']+", text)
    if not words:
        return False
    last = words[-1].lower()
    if not VOCABULARY:
        return len(last) > 2          # nothing learned: fall back to a length test
    # The word must actually occur as a standalone word somewhere in the corpus.
    # `aub` appears inside "auburn" but never alone, so it is a fragment and is
    # rejected. This is deliberately strict: a rare real word that happens to be
    # absent from the vocabulary only costs a shorter (still valid) cut, whereas
    # accepting a fragment would put a mangled word into the training data.
    return last in VOCABULARY


def ends_cleanly(text):
    """True if the text ends on a word that can legitimately close a clause."""
    words = re.findall(r"[A-Za-z']+", text)
    if not words:
        return False
    last = words[-1].lower()
    if last in DANGLE_WORDS:
        return False
    # A cut mid-word leaves a fragment; treat a very short final token as suspect
    # only when the text does not end on punctuation.
    if not TERMINAL_RE.search(text) and len(last) < 2:
        return False
    return True


def candidate_cuts(text):
    """
    Every offset at which `text` could be cut to leave a complete turn.

    Returns [(offset, kind)], ascending. The kind records WHY the cut is defensible,
    and that justification is what makes a repair trustworthy:

      sentence     end of a complete sentence      `... makes her skin crawl.`
      clause       start of a severed subclause    `... her walls tight` | `while he ...`
      attribution  end of a complete attribution  `..., \"I grunt` | `as the smell`
      quote        last closing double quote      `..., \"`

    A `quote` cut is the weakest of the four: it proves the dialogue closed, but says
    nothing about the words before it. It is therefore admitted only when the text
    inside the quote already ends on clause punctuation (CLAUSE_CLOSE_RE).
    """
    value = text.rstrip()
    cuts = set()

    # 1. sentence boundaries (abbreviations excluded, quotes balanced)
    for end in sentence_boundaries(value, require_even_quotes=True):
        cuts.add((end, "sentence"))

    # 2. immediately before a subordinator that opens a severed clause.
    #
    #    LEFT-MOST, not right-most. The severed tail is the trailing clause, so the
    #    earliest subordinator still inside the text is the one that begins it.
    #    Using rfind() here cut at the LAST subordinator, which can sit inside the
    #    severed clause itself and leave its head behind:
    #
    #      '... before I set the tray down on the counter," I say while the smell of
    #       pine needles and dampened'
    #       -> rfind(' while ') gives '..." I say while the smell of pine needles.'
    #          (still severed) while find() gives '..." I say.' (complete)
    for sep in SUBORDINATORS:
        idx = value.find(sep)
        if idx > 0:
            head = _strip_trailing_separator(value[:idx].rstrip())
            if head and ends_cleanly(head):
                cuts.add((len(head), "clause"))

    # 3. end of a complete trailing attribution:  `...," I grunt` | `as the smell`
    #    Cutting after the verb keeps the corpus's `"<speech>," I <verb>` shape,
    #    which is strictly better than dropping the attribution altogether.
    for m in ATTRIBUTION_RE.finditer(value):
        if m.group(1).lower() in ATTRIBUTION_VERBS:
            cuts.add((m.end(), "attribution"))

    # 4. the last closing double quote, for turns whose dialogue is closed but whose
    #    trailing action clause was severed before any subordinator appeared.
    #
    #    GUARD: the closing quote only proves the quotation is closed. If the words
    #    inside it stop mid-phrase, cutting here would bolt a period onto a severed
    #    clause and ship it as a sentence:
    #
    #        '...until you're screaming for me to finish, while the smell"'
    #         -> '...until you're screaming for me to finish, while the smell."'
    #
    #    So the cut is admitted only when the text just inside the quote already
    #    ends on clause punctuation. A quotation that genuinely finished reads
    #    '... right now."' or '... like this,"'; a severed one reads '... the smell"'
    #    or '... the dim blue"' and is rejected.
    quotes = [i for i, ch in enumerate(value) if ch == '"']
    closing = [i for i in quotes if value[:i + 1].count('"') % 2 == 0]
    if closing:
        last = closing[-1]
        if CLAUSE_CLOSE_RE.search(value[:last].rstrip()):
            cuts.add((last + 1, "quote"))

    return sorted(cuts)


def _close_sentence(text):
    """Make a trimmed prefix read as a finished sentence, adding no new words."""
    out = _strip_trailing_separator(text.rstrip())
    if not out:
        return out
    if out.endswith('"'):
        inner = _strip_trailing_separator(out[:-1].rstrip())
        if not re.search(r"[.!?\u2026]$", inner):
            inner += "."
        return inner + '"'
    if not re.search(r"[.!?\u2026]$", out):
        out += "."
    return out


def _close_open_quote(text):
    """Close a quotation left open by a cut, adding no new words.

    Some truncated human turns are cut *inside* the spoken dialogue, after the
    closing quote has already been lost:

        '... while I finish the last of the coffee,\" I grow'
          -> clause cut at 'while' leaves the dialogue unclosed

    The only characters this adds are a terminal period and one closing quote, and
    only at the cut point; every word still comes from the source. Step 07 verifies
    that, so the no-invented-prose guarantee holds.
    """
    core = _strip_trailing_separator(text.rstrip())
    if not re.search(r"[.!?\u2026]$", core):
        core += "."
    return core + '"'


def repair_turn(text, role, min_words=None):
    """
    Repair one truncated turn by keeping the longest valid prefix.

    The repair only ever deletes a suffix (plus, at the cut point, turns a dangling
    `,` into a `.` so the kept text reads as a finished sentence). No words are
    invented, so nothing that was not in the source can enter the corpus.

    Returns (new_text, reason). new_text is None when nothing usable survives.
    """
    value = text.rstrip()
    if not is_truncated(value):
        return value, "clean"

    if min_words is None:
        min_words = MIN_HUMAN_WORDS if role == "human" else MIN_GPT_WORDS

    best = None
    for cut, kind in candidate_cuts(value):
        head = value[:cut].rstrip()
        if not head or not ends_cleanly(head):
            continue
        # A cut may land inside an open quotation only when it is justified by a
        # clause/attribution boundary; a bare `quote` cut can never leave one open.
        open_quote = head.count('"') % 2 == 1
        if open_quote and kind not in ("clause", "attribution"):
            continue
        # Reject ANY cut that would leave a mid-word fragment, whatever its kind.
        # This is the check that stops `... her hair aub`, `... loud gutt`,
        # `... heart race with hes` and `... I grow` (from "growl") from being
        # closed off with a period and passed into the training set. Applied
        # uniformly because a sentence boundary is no guarantee either: the source
        # text itself contains sentences that were cut mid-word before the period
        # was added.
        if not final_word_known(head):
            continue
        # A `quote` cut may remove the severed trailing clause, but never a
        # complete sentence - that would be deleting content, not repairing a cut.
        tail = value[cut:]
        if kind == "quote" and re.search(r"[.!?\u2026]", tail):
            continue

        fixed = _close_open_quote(head) if open_quote else _close_sentence(head)
        if len(fixed.split()) < min_words:
            continue
        if len(fixed) < MIN_GPT_KEEP * len(value):
            continue

        # Prefer a cut that needs no invented closing quote, then the
        # structurally cleanest kind of cut, then the longest text.
        score = (not open_quote, CUT_RANK[kind], len(fixed))
        if best is None or score > best[0]:
            best = (score, fixed, kind)

    if best is None:
        return None, "no-usable-prefix"
    return best[1], f"trimmed-{best[2]}"


# Floor lengths for a repaired turn. Clean human turns in this corpus never fall
# below 16 words (p01 = 25, median = 39), so 12 is a safe floor: it only rejects
# repairs that genuinely lost the turn. Assistant turns are longer, so the floor
# is set just above the shortest clean one.
MIN_HUMAN_WORDS = 12
MIN_GPT_WORDS = 20
# ...and a repaired turn must keep at least this share of its original text,
# otherwise the truncation had already destroyed most of the turn.
MIN_GPT_KEEP = 0.25


def derive_attribution_verbs(records):
    """Attribution verbs actually used by the clean human turns."""
    pattern = re.compile(r'["\u201d]\s*,?\s*I\s+([a-z]+)\b')
    found = set()
    for rec in records:
        for turn in rec["conversations"]:
            if turn["from"] != "human" or is_truncated(turn["value"]):
                continue
            found.update(m.lower() for m in pattern.findall(turn["value"]))
    return found


def diff_turns(before, after):
    """Return list of (record_id, turn_index, role, old, new) that changed."""
    out = []
    for a, b in zip(before, after):
        for idx, (x, y) in enumerate(zip(a["conversations"], b["conversations"])):
            if x["value"] != y["value"]:
                out.append((a["id"], idx, x["from"], x["value"], y["value"]))
    return out


# ===========================================================================
# Dialogue-quote integrity (step 01).
#
# Shared by the pipeline and any standalone checker so that there is ONE
# implementation: a check that the pipeline runs can never disagree with a check
# run by hand. The three tests below are the whole of the checker's logic,
# unchanged in behaviour:
#
#   1. CURLY quotes (U+201C / U+201D) - a left-to-right positional WALK, not a
#      bare count. A close-quote with nothing open before it is `missing_open`;
#      an open-quote that appears mid-text before the previous one closed, or is
#      left unclosed at the end of the turn, is `missing_close`.
#      Multi-paragraph dialogue is NOT flagged: a re-open that sits at a
#      paragraph boundary (preceded only by whitespace after a newline) is a
#      continuation of the same quotation, not a new unclosed one.
#
#   2. STRAIGHT quotes (" ) - inch/feet marks (6", 5'9") are stripped FIRST,
#      because `6"` is a measurement, not a quotation mark. An ODD number of
#      straight quotes left over means an unpaired dialogue quote.
#
#   3. MINIMUM DIALOGUE - every checked reply must contain at least
#      MIN_QUOTES dialogue marks (default 2 = one open/close pair), because this
#      corpus's replies are meant to contain a quoted line of speech.
#
# Apostrophes (' and U+2019) are never counted: contractions and possessives are
# not quotation marks.
# ===========================================================================

CURLY_OPEN, CURLY_CLOSE = chr(0x201C), chr(0x201D)
STRAIGHT_QUOTE = chr(0x22)
QUOTE_MARKS = (CURLY_OPEN, CURLY_CLOSE, STRAIGHT_QUOTE)

# Inch / feet-inch marks that are NOT dialogue quotes:
#     6"     12 "     5'9"     5' 9 "
INCH_MARKS_RE = re.compile(r"\d+\s*'\s*\d+\s*\"|\d+\s*\"")

# Role labels seen across ShareGPT / OpenAI-messages / flat exports.
ROLE_ALIASES = {
    "human": "human", "user": "human", "prompter": "human",
    "gpt": "gpt", "assistant": "gpt", "model": "gpt", "bot": "gpt",
    "system": "system",
}

# Each checked reply is expected to hold a quoted line of speech, i.e. at least
# one open/close pair.
MIN_QUOTES = 2
MIN_QUOTES_ROLES = frozenset({"human", "gpt"})


def norm_role(raw):
    """Map an arbitrary role/speaker label onto human / gpt / system."""
    s = str(raw).strip().lower()
    return ROLE_ALIASES.get(s, s)


def extract_turns(obj):
    """Return (schema_name, [(role, text), ...]) - schema-aware, generic fallback.

    Supports ShareGPT (``conversations``), OpenAI chat (``messages``), flat
    ``{"human": ..., "gpt": ...}`` records, and - as a last resort - every string
    leaf of an unrecognised structure.
    """
    if isinstance(obj, dict):
        if isinstance(obj.get("conversations"), list):              # ShareGPT
            return "conversations", [
                (norm_role(t.get("from", "?")), t["value"])
                for t in obj["conversations"]
                if isinstance(t, dict) and "value" in t
            ]
        if isinstance(obj.get("messages"), list):                   # chat/messages
            return "messages", [
                (norm_role(t.get("role", "?")), t["content"])
                for t in obj["messages"]
                if isinstance(t, dict) and "content" in t
            ]
        flat = [(norm_role(k), v) for k, v in obj.items()
                if str(k).strip().lower() in ROLE_ALIASES and isinstance(v, str)]
        if flat:
            return "human_gpt", flat
    out = []                                                        # fallback

    def leaves(o):
        if isinstance(o, str):
            out.append(("?", o))
        elif isinstance(o, dict):
            for v in o.values():
                leaves(v)
        elif isinstance(o, list):
            for v in o:
                leaves(v)
    leaves(obj)
    return "fallback", out


def curly_quote_errors(text):
    """Positional walk over curly quotes. Returns a list of error tags."""
    errs = []
    depth = 0                       # 0 = outside a quote, 1 = inside an open quote
    for i, ch in enumerate(text):
        if ch == CURLY_OPEN:
            if depth == 0:
                depth = 1
            else:                   # already inside an open quote
                j = i - 1
                while j >= 0 and text[j] in " \t":
                    j -= 1
                at_paragraph_boundary = (j < 0) or (text[j] == "\n")
                if not at_paragraph_boundary:
                    errs.append("missing_close")
                # stay depth=1 either way (still inside a quote)
        elif ch == CURLY_CLOSE:
            if depth >= 1:
                depth -= 1
            else:
                errs.append("missing_open")        # close with nothing open
    if depth != 0:
        errs.append("missing_close")               # unclosed at end of turn
    return errs


def count_quote_marks(text):
    """Return (curly_marks, straight_marks) after stripping inch/feet marks."""
    stripped = INCH_MARKS_RE.sub("", text)
    curly = stripped.count(CURLY_OPEN) + stripped.count(CURLY_CLOSE)
    straight = stripped.count(STRAIGHT_QUOTE)
    return curly, straight


def straight_quote_unpaired(text):
    """True if an odd number of straight quotes remain after inch-mark removal."""
    return INCH_MARKS_RE.sub("", text).count(STRAIGHT_QUOTE) % 2 == 1


def check_turn_quotes(text, min_quotes=0, straight_warn_only=False):
    """Run every dialogue-quote test on one turn.

    Returns (reasons, warns, n_marks): reasons are fatal (the record is dropped),
    warns are informational only.
    """
    reasons, warns = [], []

    curly_errs = curly_quote_errors(text)
    if curly_errs:
        reasons.append("curly " + ",".join(sorted(set(curly_errs))))

    n_curly, n_straight = count_quote_marks(text)
    n_total = n_curly + n_straight

    if min_quotes and n_total < min_quotes:
        reasons.append(f"only {n_total} quote(s) - needs >= {min_quotes} "
                       f"for a quoted line of speech")

    if n_straight % 2 == 1:
        msg = "unpaired straight-quote (after inch-mark strip)"
        (warns if straight_warn_only else reasons).append(msg)

    return reasons, warns, n_total


def quote_errors_in_record(rec, check_roles=None, min_quotes_roles=None,
                           min_quotes=MIN_QUOTES, straight_warn_only=False):
    """Check every turn of one record.

    Returns (reasons, warns) as flat "<role>: <message>" strings, so a report line
    says which side of the conversation broke.
    """
    if min_quotes_roles is None:
        min_quotes_roles = MIN_QUOTES_ROLES
    _schema, turns = extract_turns(rec)
    reasons, warns = [], []
    for who, text in turns:
        if not isinstance(text, str):
            continue
        if check_roles is not None and who not in check_roles:
            continue
        minq = min_quotes if who in min_quotes_roles else 0
        r, w, _ = check_turn_quotes(text, minq, straight_warn_only)
        reasons.extend(f"{who}: {m}" for m in r)
        warns.extend(f"{who}: {m}" for m in w)
    return reasons, warns
