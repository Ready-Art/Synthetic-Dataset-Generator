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
STEP 09 - Drop records with an unbalanced double quote.

Step 08 repairs truncation but deliberately leaves one defect alone, because it
is not truncation and cannot be repaired by trimming: a turn may carry a STRAY
MID-TURN CLOSING QUOTE in the source, so the dialogue markers no longer pair up.
Such turns typically sit in msg 0 (the user's opening prompt):

    'I slide my hand down the grey wool sleeve of your jacket to feel the
     grain of the cloth beneath my fingers, "You're always so clinical and
     composed at the morgue," so tell me, does this make you feel a little less
     isolated?"'

Read the quote characters: the narration is closed with `"` before the dialogue
even opens, and the dialogue's own closing quote is then consumed by the final
`?"`. Three quote characters where there should be two (or four).

Why the record is dropped instead of repaired:

  * There is no way to know WHICH quote is the stray one without inventing a
    reading of the source. Deleting the first closes the narration and leaves
    the dialogue as unmarked prose; deleting the last leaves the dialogue
    unclosed. Either choice silently rewrites the scene.
  * This pipeline's rule is that a repair only ever DELETES a suffix and must
    leave a trimmed prefix of the source (see step 08). A mid-string edit
    violates that rule, so it does not belong in step 08.
  * The defect is structural, not cosmetic: the turn is the model's INPUT
    prompt, and unbalanced dialogue markers in a prompt teach the model to
    mis-close quotes. That is the same class of damage as truncation, which
    this pipeline treats as fatal.
  * Dropping the whole line is the only honest option - cheaper than shipping
    prompts that contradict themselves.

Detection is parity, not pattern matching: a turn is unbalanced when its
double-quote characters (`"`, `\u201c`, `\u201d`) number odd. Parity is a
property of the defect itself, so it cannot be fooled by how the stray quote is
spaced, and it needs no per-case exception list. Apostrophes are NOT counted -
`don't` and `'` are ordinary text in this corpus.

Measured on build/08_repaired.jsonl for this dataset: zero turns are
unbalanced, so this step drops nothing. It stays in the pipeline because the
defect is one generator glitch away and the check is cheap.

Input : build/08_repaired.jsonl   (or input.jsonl with --input)
Output: build/09_final.jsonl
        build/unbalanced_quote_report.txt
"""

import argparse
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import load, save, iter_turns, resolve_source

DEFAULT_INPUT = "build/08_repaired.jsonl"
DST = "build/09_final.jsonl"
REPORT = "build/unbalanced_quote_report.txt"

# Double-quote characters. Smart quotes are counted alongside the ASCII form
# because both appear in this corpus; `\u201c`/`\u201d` are distinct characters,
# so counting them together keeps the parity arithmetic correct.
QUOTE_CHARS = '"\u201c\u201d'


def count_quotes(text):
    return sum(text.count(ch) for ch in QUOTE_CHARS)


def quote_balanced(text):
    return count_quotes(text) % 2 == 0


def pick_input(explicit):
    """Prefer the pipeline's own output; fall back to the pristine source."""
    if explicit:
        if not os.path.isfile(explicit):
            raise SystemExit(f"ERROR: input not found: {explicit}")
        return explicit
    if os.path.isfile(DEFAULT_INPUT):
        return DEFAULT_INPUT
    return resolve_source()


def snippet(text, limit=300):
    """Show the turn around its first unbalanced-looking quote pair."""
    if len(text) <= limit:
        return text
    return text[:limit] + " [...]"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=None)
    args = ap.parse_args()

    src_path = pick_input(args.input)
    records = load(src_path)

    print("=" * 70)
    print("STEP 09 - drop records with unbalanced quotes")
    print("=" * 70)
    print(f"input: {src_path}")

    # --- baseline ---------------------------------------------------------
    total_turns = sum(len(r["conversations"]) for r in records)
    flagged = []            # (id, msg, role, quote_count, value)
    flagged_records = set()
    for rec, idx, turn in iter_turns(records):
        if quote_balanced(turn["value"]):
            continue
        flagged.append((rec["id"], idx, turn["from"],
                        count_quotes(turn["value"]), turn["value"]))
        flagged_records.add(rec["id"])

    print(f"\nbefore:")
    print(f"  records        : {len(records)}")
    print(f"  turns          : {total_turns}")
    print(f"  unbalanced turns: {len(flagged)} in {len(flagged_records)} record(s)")

    # --- drop -------------------------------------------------------------
    out = []
    dropped = []
    for rec in records:
        if rec["id"] in flagged_records:
            dropped.append(rec)
            continue
        out.append(rec)

    # --- verification -----------------------------------------------------
    problems = []

    # 1. The defect is gone from the output.
    still = [(r["id"], i, t["from"]) for r, i, t in iter_turns(out)
             if not quote_balanced(t["value"])]
    if still:
        problems.append(f"{len(still)} unbalanced turn(s) remain, e.g. {still[:3]}")

    # 2. Exactly the flagged records were dropped - no more, no fewer. A filter
    #    that quietly eats an extra record is the failure mode that matters
    #    most here, so the sets must match exactly, not just in size.
    dropped_ids = {r["id"] for r in dropped}
    if dropped_ids != flagged_records:
        problems.append("dropped set != flagged set "
                        f"(extra: {sorted(dropped_ids - flagged_records)[:3]}, "
                        f"missing: {sorted(flagged_records - dropped_ids)[:3]})")

    # 3. Every surviving record is byte-identical to its input. This step only
    #    deletes whole lines; it must not edit a single character of what stays.
    originals = {r["id"]: r for r in records}
    mutated = []
    for rec in out:
        old = originals[rec["id"]]
        if json.dumps(old, ensure_ascii=False) != json.dumps(rec, ensure_ascii=False):
            mutated.append(rec["id"])
    if mutated:
        problems.append(f"{len(mutated)} surviving record(s) were modified, "
                        f"e.g. {mutated[:3]}")

    # 4. Order and shape of the survivors are intact.
    expected_ids = [r["id"] for r in records if r["id"] not in flagged_records]
    if [r["id"] for r in out] != expected_ids:
        problems.append("record order changed")

    bad_shape = [r["id"] for r in out
                 if len(r["conversations"]) != 8
                 or [t["from"] for t in r["conversations"]]
                 != ["human", "gpt"] * 4]
    if bad_shape:
        problems.append(f"{len(bad_shape)} record(s) lost 8-turn human/gpt shape")

    if len({r["id"] for r in out}) != len(out):
        problems.append("duplicate record ids")

    empty = [(r["id"], i) for r, i, t in iter_turns(out) if not t["value"].strip()]
    if empty:
        problems.append(f"{len(empty)} empty turn(s)")

    # 5. Informational only: adjacent quote pairs that are NOT unbalanced. These
    #    are a different defect (`""` / `\u201c\u201c`) and are left untouched.
    adjacent = []
    for r, i, t in iter_turns(out):
        v = t["value"]
        for a, b in zip(v, v[1:]):
            if a in QUOTE_CHARS and b in QUOTE_CHARS:
                adjacent.append((r["id"], i, a + b))
                break

    # --- summary ----------------------------------------------------------
    print(f"\ndropped records: {len(dropped)}")
    for (rid, idx, role, n, value) in flagged:
        print(f"  {rid[:64]}")
        print(f"      msg {idx} {role}: {n} double quotes (odd)")

    after_turns = sum(len(r["conversations"]) for r in out)
    print(f"\nafter:")
    print(f"  records        : {len(out)} "
          f"(dropped {len(dropped)}, {100 * len(dropped) / len(records):.2f}%)")
    print(f"  turns          : {after_turns} "
          f"(removed {total_turns - after_turns})")
    print(f"  unbalanced turns: 0")

    print("\nverification:")
    if problems:
        for p in problems:
            print(f"  FAIL  {p}")
    else:
        print("  PASS  no unbalanced quotes remain")
        print("  PASS  exactly the flagged records were dropped")
        print("  PASS  every surviving record is byte-identical to its input")
        print("  PASS  record order preserved")
        print("  PASS  every record keeps 8 human/gpt turns")
        print("  PASS  ids unique")
        print("  PASS  no empty turns")
    if adjacent:
        print(f"  NOTE  {len(adjacent)} surviving turn(s) contain an adjacent "
              f"quote pair (a different defect); not dropped by this step:")
        for rid, idx, pair in adjacent:
            print(f"          {rid[:56]} | msg {idx} | {pair!r}")

    save(out, DST)
    print(f"\nwritten: {DST}")

    # --- report -----------------------------------------------------------
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("Unbalanced-quote report\n")
        f.write("=" * 74 + "\n\n")
        f.write(f"input : {src_path}\n")
        f.write(f"output: {DST}\n\n")
        f.write("DEFECT\n")
        f.write("  A turn whose double-quote characters number odd: a stray\n")
        f.write("  mid-turn closing quote, so the dialogue markers do not pair.\n")
        f.write("  Not repairable - deleting either quote silently rewrites the\n")
        f.write("  scene - so the whole record is dropped.\n\n")
        f.write("BEFORE\n")
        f.write(f"  records          : {len(records)}\n")
        f.write(f"  turns            : {total_turns}\n")
        f.write(f"  unbalanced turns : {len(flagged)} in {len(flagged_records)} record(s)\n\n")

        f.write("AFTER\n")
        f.write(f"  records          : {len(out)}\n")
        f.write(f"  turns            : {after_turns}\n")
        f.write(f"  unbalanced turns : 0\n")
        f.write(f"  records dropped  : {len(dropped)} "
                f"({100 * len(dropped) / len(records):.2f}%)\n")
        f.write(f"  turns removed    : {total_turns - after_turns}\n\n")

        by_pos = Counter((idx, role) for _, idx, role, _, _ in flagged)
        f.write("  unbalanced turns by position:\n")
        for (idx, role), n in sorted(by_pos.items()):
            f.write(f"    msg {idx} {role:6s}: {n}\n")
        f.write("\n")

        f.write("-" * 74 + "\nDROPPED RECORDS\n" + "-" * 74 + "\n")
        for (rid, idx, role, n, value) in flagged:
            f.write(f"{rid}\n")
            f.write(f"    msg {idx} {role}: {n} double quotes (odd)\n")
            f.write(f"    text: {snippet(value)}\n\n")

        f.write("-" * 74 + "\nADJACENT QUOTE PAIRS (different defect, NOT dropped)\n"
                + "-" * 74 + "\n")
        f.write(f"{len(adjacent)} surviving turn(s) contain two quote characters\n")
        f.write("with nothing between them. Reported only; left untouched.\n\n")
        for rid, idx, pair in adjacent:
            f.write(f"{rid} | msg {idx} | {pair!r}\n")

    print(f"report : {REPORT}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
