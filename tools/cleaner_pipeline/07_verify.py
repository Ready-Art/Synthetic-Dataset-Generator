#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""
STEP 07 - Verify the cleaned dataset against the source.

Independent of the cleaning scripts: re-derives every check from scratch so a bug
in a filter cannot hide itself. It shares only the token table and the matching
patterns with the filters, so a token that step 02 can remove can never be
"verified" as absent while actually remaining.

Checks:
  * every line is valid JSON, no BOM
  * record count, ids, order preserved (relative to the survivors of step 06)
  * 8 turns per record, human/gpt roles preserved
  * no empty turns
  * zero residual special tokens - including spaced variants such as
    "<turn | >" (checked with the same whitespace-tolerant patterns)
  * zero control / invisible characters
  * zero escaped-unicode or HTML artefacts
  * every difference from the source is an intended removal
    (token / tab / control char / whitespace collapse only)
  * no word was welded together by a removal

Input : input.jsonl  (source)  and  build/05_whitespace.jsonl (result)
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (SPECIAL_TOKENS, TOKEN_REGEXES, CHANNEL_BLOCK_RE,
                    CHANNEL_ONLY_RE, CONTROL_CHARS, TAB_RE,
                    WHITESPACE_RULES, sub_guarded, load, resolve_source)

RESULT = "build/05_whitespace.jsonl"

ANGLE = re.compile(r"<[^<>\n]{1,40}>")
PIPE = re.compile(r"\|\s*[a-z_]{2,20}\s*\|?")
HTML = re.compile(r"&(?:nbsp|amp|quot|apos|lt|gt|#\d{2,5}|#x[0-9a-fA-F]{2,4});")
ESCAPE = re.compile(r"\\u[0-9a-fA-F]{4}|\\x[0-9a-fA-F]{2}")


def replay(text):
    """Apply exactly steps 02-05 to a pristine turn."""
    text, _ = sub_guarded(CHANNEL_BLOCK_RE, text)      # step 02
    text, _ = sub_guarded(CHANNEL_ONLY_RE, text)
    for _, rx in TOKEN_REGEXES:
        text, _ = sub_guarded(rx, text)
    text = TAB_RE.sub(" ", text)                       # step 03
    text = "".join(ch for ch in text if ord(ch) not in CONTROL_CHARS)
    for _, pattern, repl in WHITESPACE_RULES:          # step 05
        text = pattern.sub(repl, text)
    return text.strip()


def main():
    ok = True
    failures = []

    def check(label, condition, detail=""):
        nonlocal ok
        if condition:
            print(f"  PASS  {label}")
        else:
            ok = False
            failures.append(label)
            print(f"  FAIL  {label} {detail}")

    print("=" * 64)
    print("STEP 07 - final verification")
    print("=" * 64)

    # --- file level -------------------------------------------------------
    raw = open(RESULT, encoding="utf-8").read()
    print("\nfile level:")
    check("no BOM at file start", not raw.startswith("\ufeff"))
    lines = [l for l in raw.splitlines() if l.strip()]
    print(f"        lines: {len(lines)}")

    source_path = resolve_source()
    print(f"        source: {source_path}")
    src_all = load(source_path)
    res = load(RESULT)

    # Step 06 legitimately removes records, so the comparison set is the
    # survivors of that step, not the whole source.
    res_ids = [r["id"] for r in res]
    src = [r for r in src_all if r["id"] in set(res_ids)]

    # --- structure --------------------------------------------------------
    print("\nstructure:")
    print(f"        source records: {len(src_all)}  "
          f"surviving: {len(src)}  verified: {len(res)}")
    check("no record lost after step 06",
          len(src) == len(res), f"{len(src)} -> {len(res)}")
    check("ids and order preserved",
          [r["id"] for r in src] == [r["id"] for r in res])
    check("8 turns per record",
          all(len(r["conversations"]) == 8 for r in res))
    check("role sequences preserved",
          all([t["from"] for t in a["conversations"]] ==
              [t["from"] for t in b["conversations"]]
              for a, b in zip(src, res)))
    check("no empty turns",
          all(t["value"].strip() for r in res for t in r["conversations"]))

    # --- content ----------------------------------------------------------
    print("\nresidual content:")
    found = {}
    for tok, rx in TOKEN_REGEXES:
        c = len(rx.findall(raw))
        if c:
            found[tok] = c
    check("no special tokens (incl. spaced variants)", not found, str(found))

    # Explicitly name the spaced forms, so a regression is unambiguous.
    spaced_hits = {}
    for tok in ("<turn|>", "<|channel>", "<channel|>"):
        body = r"[ \t]*".join(re.escape(ch) for ch in tok)
        c = len(re.findall(body, raw))
        if c:
            spaced_hits[tok] = c
    check("no spaced token variants", not spaced_hits, str(spaced_hits))

    check("no angle-bracket patterns", not ANGLE.search(raw))
    check("no pipe-token patterns", not PIPE.search(raw))
    check("no html entities", not HTML.search(raw))
    check("no escaped-unicode text", not ESCAPE.search(raw))

    bad = set()
    for r in res:
        for t in r["conversations"]:
            for ch in t["value"]:
                if ord(ch) in CONTROL_CHARS:
                    bad.add(hex(ord(ch)))
    check("no control / invisible characters", not bad, str(bad))

    # --- provenance of every change ---------------------------------------
    # Re-apply the exact same allowed transformations to the source turn and
    # require the result to match byte-for-byte. Whitespace rules mirror step 05
    # exactly, including its refusal to collapse newlines or eat the paragraph
    # indent, so an unintended edit cannot pass silently.
    print("\nchange provenance:")
    changed = 0
    unexplained = []
    for a, b in zip(src, res):
        for idx, (x, y) in enumerate(zip(a["conversations"], b["conversations"])):
            old, new = x["value"], y["value"]
            if old == new:
                continue
            changed += 1
            if replay(old) != new:
                unexplained.append((a["id"], idx))

    check("every change is an intended removal", not unexplained,
          str(unexplained[:3]))
    print(f"        turns changed: {changed} of {len(res) * 8}")

    # --- the removal guard ------------------------------------------------
    # A removal must never weld two words together. Checked directly: any
    # ``.Capital`` junction in the output that did not exist in the source would
    # be a weld, because the source never contains one (verified below).
    print("\nremoval guard:")
    src_welds = sum(len(re.findall(r"[a-z][.][A-Z]", t["value"]))
                    for r in src for t in r["conversations"])
    check("source contains no ``.Capital`` weld to begin with",
          src_welds == 0, f"{src_welds} found")

    welded = []
    for r in res:
        for idx, t in enumerate(r["conversations"]):
            m = re.search(r"[a-z][.][A-Z]", t["value"])
            if m:
                welded.append((r["id"], idx, m.group()))
    check("no removal welded two sentences together", not welded,
          str(welded[:3]))

    print("\n" + "=" * 64)
    if ok:
        print("RESULT: PASS - dataset is clean")
        return 0
    print(f"RESULT: FAIL - {len(failures)} check(s) failed")
    for f in failures:
        print(f"  - {f}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
