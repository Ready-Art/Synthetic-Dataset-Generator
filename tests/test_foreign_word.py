#!/usr/bin/env python
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Headless tests for the v13.5.2 foreign-word corruption check.

No network, no model, no display. Covers the detector itself, the conservative
false-positive policy, the retry-ladder wiring in ``generation.py`` and the
counters in ``app_state.py``.

Run from the repository root:

    python -B tests/test_foreign_word.py
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app_state
import detection
import generation

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_failures = []


def check(label, condition, detail=None):
    if condition:
        print("  ok   %s" % label)
    else:
        print("  FAIL %s" % label)
        if detail is not None:
            print("       %r" % (detail,))
        _failures.append(label)


# --------------------------------------------------------------------------------------
# 1. Foreign words in another language, written in Latin letters
# --------------------------------------------------------------------------------------

def test_foreign_words_are_caught():
    print("\nForeign words (Latin letters, another language)")
    cases = [
        ("her green olhos watching him", "Portuguese"),
        ("her brown ojos widened", "Spanish"),
        ("green ogen stared back", "Dutch"),
        ("her yeux were wide", "French"),
        ("die augen were wide", "German"),
        ("i suoi occhi were dark", "Italian"),
    ]
    for text, language in cases:
        detected, info = detection.is_foreign_word(text)
        check("%r is caught" % text, detected, info)
        if detected:
            check("%r names %s" % (text, language),
                  any(language in label for label, _sentence in info), info)


# --------------------------------------------------------------------------------------
# 2. Glued words (a capital in the middle of a word)
# --------------------------------------------------------------------------------------

def test_glued_words_are_caught():
    print("\nGlued words")
    for text in ("visiblyTrembling hands", "blueEyes looked up",
                 "her skinFlushed pink", "the doorCreaked open"):
        detected, info = detection.is_foreign_word(text)
        check("%r is caught" % text, detected, info)


def test_camel_case_names_and_brands_pass():
    print("\nCamel-case names and brands are NOT flagged")
    for text in ("He opened his iPhone and checked macOS.",
                 "I use JavaScript and GitHub daily.",
                 "She played on her PlayStation.",
                 "Mr McDonald arrived at the BlackBerry stand.",
                 "The PowerPoint deck was on WordPress."):
        detected, info = detection.is_foreign_word(text)
        check("%r passes" % text, not detected, info)


def test_capitalised_glued_word_needs_an_english_tail():
    print("\nA capitalised glued word is only flagged on an English tail")
    # A real name whose tail is not an ordinary English word.
    for text in ("DeAngelo smiled at LaToya.", "Mr McDonald arrived."):
        detected, info = detection.is_foreign_word(text)
        check("%r passes" % text, not detected, info)
    # A capitalised fused pair whose tail is ordinary English IS caught, even with no cast:
    # the capital is the signal, so the name does not have to be known.
    detected, info = detection.is_foreign_word("BlueEyes looked up")
    check("'BlueEyes' is caught", detected, info)
    # "Jessieshifted" has NO capital at all, so it is a different case entirely -- it can only be
    # caught when the cast is supplied (see test_glued_name_words_need_the_cast).


# --------------------------------------------------------------------------------------
# 3. A name fused to the next word with no capital at all
# --------------------------------------------------------------------------------------

def test_glued_name_words_need_the_cast():
    print("\nName fused with no capital (needs the cast)")
    names = ["Jessie", "Raven", "Elyse"]
    detected, info = detection.is_foreign_word("Jessieshifted on the bed", names)
    check("'Jessieshifted' is caught when the cast is given", detected, info)
    detected, info = detection.is_foreign_word("Raventurned away", names)
    check("'Raventurned' is caught when the cast is given", detected, info)
    # Without the cast it cannot guess, and must not.
    detected, info = detection.is_foreign_word("Jessieshifted on the bed")
    check("it does not guess without the cast", not detected, info)
    # A name followed by a normal word is not a glued word.
    for text in ("Jessie smiled at him.", "Raven turned and left.", "Elyse looked up."):
        detected, info = detection.is_foreign_word(text, names)
        check("%r passes" % text, not detected, info)


# --------------------------------------------------------------------------------------
# 4. Ordinary English must never be flagged (the false-positive policy)
# --------------------------------------------------------------------------------------

def test_english_words_that_look_foreign_pass():
    print("\nEnglish words that resemble a foreign word")
    # These are the words deliberately NOT listed, because they are also English.
    for text in ("He ate a pie and a sole fillet.",
                 "the sea was calm and the hay was dry",
                 "She wore a black dress and red shoes.",
                 "Come here and roll the dice, son.",
                 "The contract expires tomorrow.",
                 "Her eyes were green and her hair was red.",
                 "He had a large hand and a small nose."):
        detected, info = detection.is_foreign_word(text)
        check("%r passes" % text, not detected, info)


def test_plain_english_passes():
    print("\nPlain English")
    for text in ("She looked at him with green eyes.",
                 "He touched her cheek and she shivered.",
                 "The room smelled of rain and old paper.",
                 ""):
        detected, info = detection.is_foreign_word(text)
        check("%r passes" % text, not detected, info)


# --------------------------------------------------------------------------------------
# 5. The return contract
# --------------------------------------------------------------------------------------

def test_contract():
    print("\nReturn contract")
    detected, info = detection.is_foreign_word("her green olhos and brown ojos")
    check("a hit returns True", detected is True)
    check("the payload is a list of (label, sentence) pairs",
          isinstance(info, list) and all(
              isinstance(pair, tuple) and len(pair) == 2 for pair in info), info)
    check("each distinct word is reported once", len(info) == 2, info)
    detected, info = detection.is_foreign_word("her green olhos and green olhos again")
    check("a repeated word is reported once", len(info) == 1, info)
    detected, info = detection.is_foreign_word(None)
    check("None returns (False, [])", (detected, info) == (False, []))
    detected, info = detection.is_foreign_word("   ")
    check("blank returns (False, [])", (detected, info) == (False, []))


# --------------------------------------------------------------------------------------
# 6. The retry-ladder wiring in generation.py
# --------------------------------------------------------------------------------------

def test_generation_wiring():
    print("\nWiring in generation.py")
    with open(os.path.join(REPO, "generation.py"), encoding="utf-8") as handle:
        source = handle.read()

    check("DEFAULT_FOREIGN_WORD_FIX exists",
          hasattr(generation, "DEFAULT_FOREIGN_WORD_FIX"))
    check("the default fix names English",
          "English" in generation.DEFAULT_FOREIGN_WORD_FIX)
    check("the default fix forbids joining words",
          "space" in generation.DEFAULT_FOREIGN_WORD_FIX.lower())

    check("the ladder calls detection.is_foreign_word",
          "detection.is_foreign_word(" in source)
    check("the ladder is gated on foreign_word_enabled_local",
          re.search(r"if foreign_word_enabled_local:", source) is not None)
    check("the ladder retries with a fix",
          "foreign_word_fix_list" in source)
    check("the ladder gives up rather than looping",
          "fixes \"\n" in source or "exhausted" in source)
    check("the check runs after the non-Latin one",
          source.index("detection.is_non_latin(answer)")
          < source.index("detection.is_foreign_word("))
    check("the cast is passed to the detector",
          "character_names_local" in source)


def test_counters_exist():
    print("\nCounters in app_state.py")
    check("foreign_word_count_total", hasattr(app_state, "foreign_word_count_total"))
    check("foreign_word_counts_per_api",
          hasattr(app_state, "foreign_word_counts_per_api"))
    check("recent_foreign_word_total", hasattr(app_state, "recent_foreign_word_total"))
    check("recent_foreign_word_per_api",
          hasattr(app_state, "recent_foreign_word_per_api"))
    check("per-api counters cover every slot",
          len(app_state.foreign_word_counts_per_api) == 7,
          app_state.foreign_word_counts_per_api)


def test_config_is_opt_in():
    print("\nThe check is opt-in")
    import run_config
    settings = run_config.RunSettings()
    check("RunSettings defaults foreign_word_enabled to False",
          settings.foreign_word_enabled is False)
    check("RunSettings defaults foreign_word_fixes to empty",
          settings.foreign_word_fixes == [])


# --------------------------------------------------------------------------------------

def main():
    test_foreign_words_are_caught()
    test_glued_words_are_caught()
    test_camel_case_names_and_brands_pass()
    test_capitalised_glued_word_needs_an_english_tail()
    test_glued_name_words_need_the_cast()
    test_english_words_that_look_foreign_pass()
    test_plain_english_passes()
    test_contract()
    test_generation_wiring()
    test_counters_exist()
    test_config_is_opt_in()

    print()
    if _failures:
        print("%d check(s) FAILED:" % len(_failures))
        for label in _failures:
            print("  - %s" % label)
        return 1
    print("all foreign-word checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
