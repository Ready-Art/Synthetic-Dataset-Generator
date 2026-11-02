# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the v12.7.3 repeated-word collapse in ``text_utils``.

``collapse_repeated_words()`` removes a word the model stalled on and emitted two or more
times in a row ("Serenity. Serenity. Then she left."). It is ALWAYS ON and has no config
key, so this file is the only thing standing between a bad edit to the regexes or to
``_REPEAT_EXEMPT_WORDS`` and every generated row.

Two properties matter more than any single example:

  * an EXEMPT word is never collapsed -- "no no no" and "very very good" are correct
    English emphasis, and rewriting them would corrupt valid prose;
  * the pass is IDEMPOTENT and leaves text with no repetition untouched, so it cannot
    creep into the output of an ordinary reply.

No model, network or display is needed. Run with
``python -B tests/test_repeated_words.py``.
"""
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import text_utils


FAILURES = []


def check(name, condition):
    if not condition:
        FAILURES.append(name)
        print("FAIL: %s" % name)


def eq(name, actual, expected):
    check("%s (got %r, expected %r)" % (name, actual, expected), actual == expected)


# --------------------------------------------------------------------------------------
# The four shapes a stall is written in
# --------------------------------------------------------------------------------------

def test_shapes():
    c = text_utils.collapse_repeated_words

    # 1. consecutive one-word sentences
    eq("sentence run collapses",
       c("Serenity. Serenity. Then she left."), "Serenity. Then she left.")
    eq("sentence run of three collapses",
       c("Marlowe. Marlowe. Marlowe."), "Marlowe.")
    eq("sentence run with exclamation collapses", c("Run! Run! Go."), "Run! Go.")
    eq("sentence run with question mark collapses",
       c("Honestly? Honestly? Because."), "Honestly? Because.")

    # 2. comma-separated run that ends the sentence
    eq("comma run at end collapses", c("Faster, faster, faster."), "Faster.")
    eq("comma run at end keeps its terminator", c("Louder, louder!"), "Louder!")
    eq("semicolon run at end collapses", c("Quiet; quiet; quiet."), "Quiet.")

    # 3. comma-separated run that continues
    eq("comma run mid-sentence collapses",
       c("word, word, word text here."), "word text here.")

    # 4. bare space-separated run
    eq("bare run collapses", c("word word word end"), "word end")
    eq("bare run of two collapses", c("Mia Mia"), "Mia")


# --------------------------------------------------------------------------------------
# The exclusion list is the safety story: ordinary repeated words are emphasis
# --------------------------------------------------------------------------------------

def test_exempt_words():
    c = text_utils.collapse_repeated_words
    untouched = [
        "No no no.",
        "Yes. Yes. Yes.",
        "very very good",
        "ha ha ha",
        "Wait wait wait.",
        "hmm hmm hmm",
        "okay okay okay",
        "Oh oh oh",
        "Well well well.",
        "He ran and ran.",
        "The the end.",
        "and then, then, then",
    ]
    for text in untouched:
        eq("exempt emphasis untouched: %r" % text, c(text), text)

    # Documented limitation, asserted on purpose: an exempt word is left alone even when
    # it really is a stall. A false collapse would rewrite valid prose, so this is the
    # safe direction to fail in.
    eq("exempt word is never collapsed even as a stall",
       c("Why? Why? Because."), "Why? Why? Because.")
    eq("exempt word in a comma run is never collapsed",
       c("Again, again!"), "Again, again!")

    # Spot-check the set itself, so removing an entry is caught here and not in a run.
    for word in ("no", "yes", "very", "ha", "wait", "hmm", "okay", "oh", "well"):
        check("_REPEAT_EXEMPT_WORDS contains %r" % word,
              word in text_utils._REPEAT_EXEMPT_WORDS)


# --------------------------------------------------------------------------------------
# Casing, idempotency, unaffected text, empty input
# --------------------------------------------------------------------------------------

def test_casing_and_safety():
    c = text_utils.collapse_repeated_words
    eq("first occurrence casing is kept", c("Serenity. Serenity."), "Serenity.")
    eq("lowercase first occurrence stays lowercase", c("serenity. Serenity."), "serenity.")
    eq("bare run keeps first casing", c("Mia mia"), "Mia")

    # A two-word run is NOT a stall and must survive (documented scope).
    eq("two-word run untouched", c("She said. She said."), "She said. She said.")

    # No repetition -> byte identical.
    plain = "Rain drums on the ferry's tin roof. Ines looks up. \"Last crossing.\""
    eq("text without repetition is unchanged", c(plain), plain)

    # Empty / falsy input.
    eq("empty string safe", c(""), "")
    eq("None passthrough", c(None), None)

    # Idempotent.
    for text in ("Serenity. Serenity. Then she left.", "word, word, word text", "Mia Mia"):
        once = c(text)
        eq("idempotent: %r" % text, c(once), once)

    # Tokens with no letters are not words and must be left alone.
    eq("digits untouched", c('6" 6"'), '6" 6"')
    eq("apostrophe word collapses", c("Melody's Melody's"), "Melody's")


# --------------------------------------------------------------------------------------
# The wiring in write_conversation() keeps the quote passes LAST
# --------------------------------------------------------------------------------------

def test_pipeline_order():
    src = (REPO / "generation.py").read_text(encoding="utf-8")
    start = src.index("processed_content = text_utils.remove_reasoning_text")
    end = src.index("sg_role =", start)
    block = src[start:end]

    order = [
        "text_utils.remove_reasoning_text",
        "text_utils.remove_markdown",
        "text_utils.collapse_repeated_words",
        "text_utils.repair_straight_quotes",
        "text_utils.normalize_quotes",
    ]
    offsets = []
    for name in order:
        check("write_conversation calls %s" % name, name in block)
        if name in block:
            offsets.append(block.index(name))
    check("pipeline offsets are strictly increasing",
          all(offsets[i] < offsets[i + 1] for i in range(len(offsets) - 1)))

    # normalize_quotes must be the LAST text_utils call in the block.
    last = max(i for i in range(len(block)) if block.startswith("text_utils.", i))
    check("normalize_quotes is the last text_utils call",
          block[last:].startswith("text_utils.normalize_quotes"))

    # collapse_repeated_words is unconditional (no config flag on its line).
    line = [l for l in block.splitlines() if "collapse_repeated_words" in l][0]
    check("collapse_repeated_words is unconditional",
          "_flag" not in line and " if " not in line)


def main():
    test_shapes()
    test_exempt_words()
    test_casing_and_safety()
    test_pipeline_order()
    if FAILURES:
        print("\n%d FAILED check(s):" % len(FAILURES))
        for name in FAILURES:
            print("  - %s" % name)
        return 1
    print("\nAll repeated-word checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
