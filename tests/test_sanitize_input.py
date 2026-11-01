# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""v12.4 regression guard: sanitize_input() must not corrupt authored prompts.

Before v12.4 `sanitize_input()` stripped double quotes, apostrophes and backslashes on
every editor save. That silently rewrote real prose: "don't is right" was stored as
"dont is right", so a prompt written to forbid broken contractions modelled the exact
error it banned, and any prompt quoting dialogue lost its quotation marks on save.

These checks pin the behaviour that matters: the shipped prompts survive a save round
trip byte for byte, and an authored emotional arc keeps its `->`.
"""

import os
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_loader import sanitize_input

CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "config.yml"
)


def test_apostrophes_survive():
    text = "apostrophes for contractions (dont is wrong, don't is right)"
    assert sanitize_input(text) == text


def test_double_quotes_survive():
    text = 'use double quotes for speech, "like this"'
    assert sanitize_input(text) == text


def test_backslashes_survive():
    text = r"a backslash \ and a path C:\temp"
    assert sanitize_input(text) == text


def test_angle_brackets_are_still_removed():
    assert sanitize_input("<b>bold</b>") == "bbold/b"


def test_arc_arrow_survives():
    text = "calm -> wary -> tense"
    assert sanitize_input(text) == text


def test_arc_arrow_survives_beside_removed_brackets():
    assert sanitize_input("<x> calm -> wary") == "x calm -> wary"


def test_empty_and_none_pass_through():
    assert sanitize_input("") == ""
    assert sanitize_input(None) is None


def test_max_length_still_enforced():
    try:
        sanitize_input("abcdef", max_length=3)
    except ValueError:
        return
    raise AssertionError("max_length was not enforced")


def test_shipped_prompts_survive_a_save_round_trip():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        prompts = yaml.safe_load(f)["prompts"]

    for key in ("question", "answer", "user_continuation_prompt"):
        original = prompts[key].strip()
        assert sanitize_input(original) == original, f"{key} was altered by sanitize_input()"


def test_shipped_prompts_keep_contractions_and_arc_notation():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        answer = yaml.safe_load(f)["prompts"]["answer"]

    assert "A -> B -> C" in answer, "the arc notation was lost"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"\n{len(tests)} checks passed")
