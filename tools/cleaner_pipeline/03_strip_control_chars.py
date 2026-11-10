#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""
STEP 03 - Remove control and invisible characters.

Removes characters that are never legitimate prose:
  * C0 controls other than newline (tab, vertical tab, form feed, NUL, ...)
  * soft hyphen (U+00AD)
  * zero-width space / joiners / directional marks (U+200B-U+200F)
  * line and paragraph separators (U+2028, U+2029)
  * word joiner (U+2060)
  * BOM / zero-width no-break space (U+FEFF)
  * replacement character (U+FFFD)

Newline (U+000A) is preserved - it carries the two-paragraph shape
of the assistant targets.

Input : build/02_tokens.jsonl
Output: build/03_control.jsonl
"""

import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (load, save, iter_turns, integrity_check, report_counts,
                    diff_turns, strip_control_chars)

SRC = "build/02_tokens.jsonl"
DST = "build/03_control.jsonl"


def main():
    before = load(SRC)
    after = load(SRC)
    tally = Counter()

    for _, _, turn in iter_turns(after):
        turn["value"] = strip_control_chars(turn["value"], tally)

    print("=" * 64)
    print("STEP 03 - control / invisible character removal")
    print("=" * 64)
    report_counts(tally)

    changed = diff_turns(before, after)
    print(f"\nturns changed: {len(changed)}")
    for rid, idx, role, old, new in changed:
        print(f"\n  {rid[:60]} | msg {idx} | {role}")
        print(f"    before: {old[:130]!r}")
        print(f"    after : {new[:130]!r}")

    if not integrity_check(before, after, "step 03"):
        return 1

    save(after, DST)
    print(f"\nwritten: {DST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
