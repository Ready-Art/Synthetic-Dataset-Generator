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
STEP 02 - Remove chat-template special tokens.

Removes every known chat-template sentinel. Matching is tolerant of spaces or
tabs injected between the token's characters, so all of these are caught:

    <turn|>      <turn |>      <turn | >      < turn |  >

The <|channel>thought<channel|> block is removed as a whole unit first,
because "thought" is otherwise a legitimate English word that appears
frequently in ordinary prose and must never be stripped on its own.

Input : input.jsonl  (or $SOURCE / SOURCE=... ./run_all.sh)
Output: build/02_tokens.jsonl
"""

import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (load, save, iter_turns, integrity_check, report_counts,
                    diff_turns, strip_special_tokens, resolve_source)

# Step 01's output, not the pristine source: records that step 01 dropped for
# unpaired dialogue quotes must not be resurrected here.
DEFAULT_INPUT = "build/01_quotes.jsonl"
DST = "build/02_tokens.jsonl"


def pick_input():
    """Prefer step 01's output; fall back to the pristine source.

    The fallback keeps this step runnable on its own (e.g. on input.jsonl), but in
    the pipeline it always consumes build/01_quotes.jsonl so that step 01's drops
    stay dropped.
    """
    if os.path.isfile(DEFAULT_INPUT):
        return DEFAULT_INPUT
    return resolve_source()


def main():
    src = pick_input()
    print(f"source: {src}")
    before = load(src)
    after = load(src)          # independent copy
    tally = Counter()

    for _, _, turn in iter_turns(after):
        turn["value"] = strip_special_tokens(turn["value"], tally)

    # report
    print("=" * 64)
    print("STEP 02 - special-token removal")
    print("=" * 64)
    report_counts(tally)

    changed = diff_turns(before, after)
    print(f"\nturns changed: {len(changed)}")
    for rid, idx, role, old, new in changed:
        print(f"\n  {rid[:60]} | msg {idx} | {role}")
        print(f"    before: ...{old[-110:]!r}")
        print(f"    after : ...{new[-110:]!r}")

    if not integrity_check(before, after, "step 02"):
        return 1

    os.makedirs("build", exist_ok=True)
    save(after, DST)
    print(f"\nwritten: {DST}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
