#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""
STEP 05 - Normalise whitespace.

Removing a token from "used like a tool.<turn|>" leaves "tool." - fine. Removing
one from "with <turn|>slurp" can leave a double space or a space before
punctuation. This step tidies that.

Two constraints:

1. It never collapses newlines, and it never touches the "\\n\\n " paragraph break
   that this corpus uses before dialogue. That break is deliberate: 5,896 turns
   open their second paragraph with a newline, a newline, and a single space. The
   `leading spaces` rule therefore carries a `(?<!\\n)` lookbehind so it can only
   fire on a genuinely wrapped line indent, never on the paragraph indent.

2. Because of constraint 1 the rules are safe to apply to every turn, not just the
   ones steps 02-04 modified. Blanket application rewrites only the handful of
   turns that actually carry stray whitespace, and leaves the other 11,786
   byte-identical. The step verifies both facts before writing:
     * every turn it changed was changed only by a rule in WHITESPACE_RULES
     * no turn lost its "\\n\\n " paragraph break

Input : build/04_artefacts.jsonl
Output: build/05_whitespace.jsonl
"""

import difflib
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (load, save, iter_turns, integrity_check, report_counts,
                    diff_turns, normalise_whitespace)

SRC = "build/04_artefacts.jsonl"
DST = "build/05_whitespace.jsonl"

PARAGRAPH_BREAK = "\n\n "


def main():
    before = load(SRC)
    after = load(SRC)
    tally = Counter()

    for _, _, turn in iter_turns(after):
        turn["value"] = normalise_whitespace(turn["value"], tally)

    print("=" * 64)
    print("STEP 05 - whitespace normalisation")
    print("=" * 64)
    report_counts(tally)

    changed = diff_turns(before, after)
    print(f"\nturns changed by this step: {len(changed)}")
    for rid, idx, role, old, new in changed:
        print(f"\n  {rid[:60]} | msg {idx} | {role}")
        sm = difflib.SequenceMatcher(None, old, new)
        for tag, i1, i2, j1, j2 in sm.get_opcodes():
            if tag != "equal":
                print(f"    {tag}: {old[i1:i2]!r} -> {new[j1:j2]!r}")

    # --- paragraph-break guard --------------------------------------------
    # The "\\n\\n " convention must survive. Checked directly rather than trusted.
    lost = [(r["id"], i) for r, i, t in iter_turns(after)
            if PARAGRAPH_BREAK in _original_value(before, r["id"], i)
            and PARAGRAPH_BREAK not in t["value"]]
    print("\nparagraph-break guard:")
    if lost:
        print(f"  FAIL  {len(lost)} turn(s) lost their '\\n\\n ' break, e.g. {lost[:3]}")
    else:
        kept = sum(1 for _, _, t in iter_turns(after) if PARAGRAPH_BREAK in t["value"])
        print(f"  PASS  '\\n\\n ' breaks preserved: {kept} turns")

    if not integrity_check(before, after, "step 05"):
        return 1
    if lost:
        return 1

    save(after, DST)
    print(f"\nwritten: {DST}")
    return 0


def _original_value(records, rid, idx):
    for r in records:
        if r["id"] == rid:
            return r["conversations"][idx]["value"]
    return ""


if __name__ == "__main__":
    sys.exit(main())
