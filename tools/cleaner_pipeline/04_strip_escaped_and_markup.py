#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""
STEP 04 - Remove escaped-unicode text and HTML artefacts.

Some pipelines double-encode, leaving literal text that was meant to be a
character or markup:
  * literal "\\uXXXX" escape sequences
  * literal "\\xNN" hex escapes
  * HTML entities such as &nbsp; &amp; &#39;
  * stray markup tags such as <br> <p> </i>

Input : build/03_control.jsonl
Output: build/04_artefacts.jsonl
"""

import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import load, save, iter_turns, integrity_check, report_counts, diff_turns

SRC = "build/03_control.jsonl"
DST = "build/04_artefacts.jsonl"

ESC_U = re.compile(r"\\u[0-9a-fA-F]{4}")
ESC_X = re.compile(r"\\x[0-9a-fA-F]{2}")
HTML_ENTITY = re.compile(r"&(?:nbsp|amp|quot|apos|lt|gt|#\d{2,5}|#x[0-9a-fA-F]{2,4});")
MARKUP_TAG = re.compile(
    r"</?(?:br|p|i|b|em|strong|span|div|html|body)\s*/?>", re.IGNORECASE
)


def strip_artefacts(text, counts):
    def bump(key, n):
        if n:
            counts[key] += n

    text, n = ESC_U.subn("", text)
    bump("escaped \\uXXXX", n)
    text, n = ESC_X.subn("", text)
    bump("escaped \\xNN", n)
    text, n = HTML_ENTITY.subn("", text)
    bump("html entity", n)
    text, n = MARKUP_TAG.subn("", text)
    bump("markup tag", n)
    return text


def main():
    before = load(SRC)
    after = load(SRC)
    tally = Counter()

    for _, _, turn in iter_turns(after):
        turn["value"] = strip_artefacts(turn["value"], tally)

    print("=" * 64)
    print("STEP 04 - escaped-unicode / HTML artefact removal")
    print("=" * 64)
    report_counts(tally)

    changed = diff_turns(before, after)
    print(f"\nturns changed: {len(changed)}")
    for rid, idx, role, old, new in changed[:10]:
        print(f"\n  {rid[:60]} | msg {idx} | {role}")
        print(f"    before: {old[:130]!r}")
        print(f"    after : {new[:130]!r}")

    if not integrity_check(before, after, "step 04"):
        return 1

    save(after, DST)
    print(f"\nwritten: {DST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
