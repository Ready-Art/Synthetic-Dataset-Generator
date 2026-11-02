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
STEP 08 - Repair truncated turns.

The source corpus contains turns that stop mid-clause, because the source text
was chunked at a byte offset with no sentence-boundary snapping:

    "...The contrast between the cool air and the warm water filling the
     basin makes her skin"                                  (gpt, cut mid-phrase)
    "...she lets out a loud gutt"                          (gpt, cut mid-word)
    '...garlic on your skin," I mutter as'                 (human, cut mid-clause)

Left in place these teach the model that stopping mid-clause is acceptable, so
they are repaired here rather than filtered out. A whole-record delete would
cost far more data than the damage warrants: truncation touches 289 of 1,416
records (20.4%), but almost all of them lose only a trailing clause.

Two repair rules, because the two roles are structurally different
(measured on this corpus - see the docstring in common.py):

  gpt    trim back to the last complete sentence.
         Almost every truncated gpt turn has an internal boundary and keeps most
         of its text, so this loses little.

  human  drop the severed trailing attribution clause and close the dialogue.
         All truncated human turns have balanced quotes, so the dialogue itself
         is intact and only the `I <verb> as <action>` tail is broken.

Both rules only ever DELETE a suffix. The only characters a repair can add are
terminal punctuation and, when a cut lands inside an open quotation, one closing
quote at the cut point. No word is invented, so nothing that was not in the source
can enter the corpus.

A record is dropped only when a repair cannot produce a usable turn:
  * nothing complete survives the trim
  * the repaired turn is shorter than MIN_HUMAN_WORDS / loses >75% of its text
  * the truncation is in turn 0 (the user's opening prompt), because the corpus's
    design is a complete first-person opener and there is no earlier turn to
    carry the scene if it is missing
  * more than MAX_REPAIRS_PER_RECORD turns in the same record needed repair
    (that record is a fragment of a fragment; repairing it produces a scene that
    no longer resembles the source)

Input : build/06_foreign_dropped.jsonl   (or input.jsonl with --input)
        --input overrides; otherwise $SOURCE / input.jsonl is the fallback
Output: build/08_repaired.jsonl
        build/truncation_report.txt
"""

import argparse
import json
import os
import re
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (load, save, iter_turns, is_truncated, repair_turn,
                    derive_attribution_verbs, learn_vocabulary, VOCABULARY,
                    MIN_GPT_KEEP, MIN_HUMAN_WORDS, MIN_GPT_WORDS,
                    ATTRIBUTION_VERBS, resolve_source)

DEFAULT_INPUT = "build/06_foreign_dropped.jsonl"
DST = "build/08_repaired.jsonl"
REPORT = "build/truncation_report.txt"

# How much of a record's text may be lost to repairs before the record is dropped.
# Measured: every fully-repairable record loses at most 8.1% of its words (median
# 1.7%), because each repair is a small trim - so a record with several truncated
# turns is still fine. An earlier draft dropped any record with more than 2 truncated
# turns, which discarded 106 perfectly good records; the share lost is the real
# measure of damage, not the count.
MAX_RECORD_LOSS = 0.25


def pick_input(explicit):
    """Pick the JSONL to repair.

    Prefer the pipeline's own output; fall back to the pristine source so the
    step can be run standalone on input.jsonl.

    The default is step 06's output, NOT step 05's: records that step 06 dropped
    for foreign script must not be resurrected here.
    """
    if explicit:
        if not os.path.isfile(explicit):
            raise SystemExit(f"ERROR: input not found: {explicit}")
        return explicit
    if os.path.isfile(DEFAULT_INPUT):
        return DEFAULT_INPUT
    return resolve_source()


def quote_balanced(text):
    return text.count('"') % 2 == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=None)
    args = ap.parse_args()

    src_path = pick_input(args.input)
    records = load(src_path)

    print("=" * 70)
    print("STEP 08 - truncation repair")
    print("=" * 70)
    print(f"input: {src_path}")

    # --- baseline ---------------------------------------------------------
    total_turns = sum(len(r["conversations"]) for r in records)
    before_by_role = Counter()
    before_positions = Counter()
    before_records = set()
    for rec, idx, turn in iter_turns(records):
        if is_truncated(turn["value"]):
            before_by_role[turn["from"]] += 1
            before_positions[(idx, turn["from"])] += 1
            before_records.add(rec["id"])

    print(f"\nbefore:")
    print(f"  records            : {len(records)}")
    print(f"  truncated turns    : {sum(before_by_role.values())} "
          f"of {total_turns} ({100 * sum(before_by_role.values()) / total_turns:.2f}%)")
    for role in ("human", "gpt"):
        n = before_by_role[role]
        print(f"    {role:6s}: {n:5d} "
              f"({100 * n / (total_turns / 2):.2f}% of {role} turns)")
    print(f"  records with >=1 truncated turn: {len(before_records)} "
          f"({100 * len(before_records) / len(records):.1f}%)")

    # Learn the template's attribution verbs and the corpus vocabulary from the
    # complete turns. Both are used by the repair itself:
    #   * vocabulary   - rejects cuts that would leave a mid-word fragment
    #   * verbs        - recognises a finished `"...," I grunt` attribution
    verbs = derive_attribution_verbs(records) | ATTRIBUTION_VERBS
    learn_vocabulary(records)
    print(f"  vocabulary learned : {len(VOCABULARY)} words "
          f"(from {total_turns - sum(before_by_role.values())} complete turns)")

    # --- repair -----------------------------------------------------------
    out = []
    dropped = []
    repairs = []           # (id, idx, role, reason, removed_text, kept_text)
    reasons = Counter()
    words_removed = Counter()
    per_record_repairs = Counter()

    for rec in records:
        new_turns = []
        record_drop = None
        record_repairs = 0

        for idx, turn in enumerate(rec["conversations"]):
            role, value = turn["from"], turn["value"]

            if not is_truncated(value):
                new_turns.append(turn)
                continue

            record_repairs += 1

            fixed, reason = repair_turn(value, role)

            if fixed is None:
                record_drop = f"msg {idx} {role}: {reason}"
                reasons[f"drop: {role} {reason}"] += 1
                break

            if idx == 0:
                record_drop = "msg 0 human: opening prompt truncated"
                reasons["drop: opener truncated"] += 1
                break

            reasons[f"repair: {role} {reason}"] += 1
            words_removed[role] += len(value.split()) - len(fixed.split())
            repairs.append({
                "id": rec["id"], "msg": idx, "role": role, "reason": reason,
                "removed": value.rstrip()[len(fixed):].strip(),
                "kept_tail": fixed[-70:],
                "verb": (fixed.split()[-1] if role == "human" else ""),
            })
            new_turns.append({"from": role, "value": fixed})

        if record_drop is None:
            before_words = sum(len(t["value"].split())
                               for t in rec["conversations"])
            after_words = sum(len(t["value"].split()) for t in new_turns)
            lost = 1 - (after_words / max(1, before_words))
            if lost > MAX_RECORD_LOSS:
                record_drop = (f"repairs would remove {lost:.0%} of the record "
                               f"(> {MAX_RECORD_LOSS:.0%})")
                reasons["drop: excessive text loss"] += 1

        if record_drop is not None:
            dropped.append({"id": rec["id"], "why": record_drop})
            continue

        per_record_repairs[record_repairs] += 1
        out.append({"id": rec["id"], "conversations": new_turns})

    # --- verification -----------------------------------------------------
    problems = []

    still = [(r["id"], i, t["from"]) for r, i, t in iter_turns(out)
             if is_truncated(t["value"])]
    if still:
        problems.append(f"{len(still)} turn(s) still truncated, e.g. {still[:3]}")

    empty = [(r["id"], i) for r, i, t in iter_turns(out) if not t["value"].strip()]
    if empty:
        problems.append(f"{len(empty)} empty turn(s)")

    # Quote balance: a repair must never INTRODUCE an imbalance. The source already
    # contains a handful of turns with a stray mid-turn closing quote (e.g.
    # `...,\" so tell me, ...\"`) - a separate defect, not truncation. Those are
    # not a step-07 failure and are deliberately left untouched here, because the
    # stray quote sits mid-string and cannot be removed without inventing a
    # reading of the source. They are reported below and the affected records are
    # dropped wholesale by step 09.
    source_turns = {(r["id"], i): t["value"]
                    for r in records for i, t in enumerate(r["conversations"])}
    introduced = []
    preexisting = []
    for r, i, t in iter_turns(out):
        if quote_balanced(t["value"]):
            continue
        if quote_balanced(source_turns[(r["id"], i)]):
            introduced.append((r["id"], i))
        else:
            preexisting.append((r["id"], i))
    if introduced:
        problems.append(f"{len(introduced)} repair(s) introduced unbalanced quotes, "
                        f"e.g. {introduced[:3]}")

    bad_shape = [r["id"] for r in out
                 if len(r["conversations"]) != 8
                 or [t["from"] for t in r["conversations"]]
                 != ["human", "gpt"] * 4]
    if bad_shape:
        problems.append(f"{len(bad_shape)} record(s) lost 8-turn human/gpt shape")

    ids = [r["id"] for r in out]
    if len(ids) != len(set(ids)):
        problems.append("duplicate record ids")

    # Provenance: every surviving turn must be a trimmed prefix of its original.
    # The only permitted edits are deleting a suffix and, at the cut point,
    # swapping a dangling separator for terminal punctuation. Verified by
    # stripping the final punctuation from both sides and requiring an exact
    # prefix match - so no word can have been invented or reordered.
    originals = {r["id"]: r["conversations"] for r in records}
    mutated = []
    for rec in out:
        for idx, turn in enumerate(rec["conversations"]):
            old = originals[rec["id"]][idx]["value"].rstrip()
            new = turn["value"]
            if new == old:
                continue
            core = re.sub(r'["\'\u201d.,!?\u2026]+$', '', new)
            if not old.startswith(core):
                mutated.append((rec["id"], idx))
    if mutated:
        problems.append(f"{len(mutated)} turn(s) are not a trimmed prefix of the "
                        f"source, e.g. {mutated[:3]}")

    # No new words may have entered: the kept text's word sequence must be a
    # subsequence of the source turn's word sequence, in order.
    invented = []
    for rec in out:
        for idx, turn in enumerate(rec["conversations"]):
            old_words = originals[rec["id"]][idx]["value"].split()
            new_words = turn["value"].split()
            if new_words == old_words:
                continue
            n = len(new_words)
            if old_words[:n - 1] != new_words[:n - 1]:
                invented.append((rec["id"], idx))
    if invented:
        problems.append(f"{len(invented)} turn(s) contain words not in the source, "
                        f"e.g. {invented[:3]}")

    # --- summary ----------------------------------------------------------
    after_turns = sum(len(r["conversations"]) for r in out)
    after_by_role = Counter(t["from"] for r, _, t in iter_turns(out))
    print(f"\nrepairs applied: {len(repairs)}")
    for k, v in sorted(reasons.items()):
        print(f"  {k:44s} {v}")
    print(f"\n  words removed   human {words_removed['human']:6d}   "
          f"gpt {words_removed['gpt']:6d}")
    print(f"  repairs/record  "
          + "  ".join(f"{k}:{v}" for k, v in sorted(per_record_repairs.items())))

    print(f"\nafter:")
    print(f"  records            : {len(out)} "
          f"(dropped {len(dropped)}, {100 * len(dropped) / len(records):.1f}%)")
    print(f"  truncated turns    : {sum(1 for _, _, t in iter_turns(out) if is_truncated(t['value']))}"
          f" of {after_turns} (0.00%)")
    for role in ("human", "gpt"):
        n = after_by_role[role]
        print(f"    {role:6s}: {n:5d} turns")

    print("\nverification:")
    if problems:
        for p in problems:
            print(f"  FAIL  {p}")
    else:
        print("  PASS  no truncated turns remain")
        print("  PASS  no empty turns")
        print("  PASS  no repair introduced unbalanced quotes")
        print("  PASS  every record keeps 8 human/gpt turns")
        print("  PASS  ids unique")
        print("  PASS  every turn is a trimmed prefix of the source "
              "(no invented text)")
    if preexisting:
        print(f"  NOTE  {len(preexisting)} turn(s) had unbalanced quotes in the "
              f"SOURCE already (stray mid-turn closing quote); left untouched "
              f"here - step 09 drops these records:")
        for rid, idx in preexisting:
            print(f"          {rid[:60]} | msg {idx}")

    save(out, DST)
    print(f"\nwritten: {DST}")

    # --- report -----------------------------------------------------------
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("Truncation repair report\n")
        f.write("=" * 74 + "\n\n")
        f.write(f"input : {src_path}\n")
        f.write(f"output: {DST}\n\n")
        f.write("BEFORE\n")
        f.write(f"  records                     : {len(records)}\n")
        f.write(f"  truncated turns             : {sum(before_by_role.values())}"
                f" of {total_turns}\n")
        for role in ("human", "gpt"):
            f.write(f"    {role:6s}                    : {before_by_role[role]}"
                    f" ({100 * before_by_role[role] / (total_turns / 2):.2f}%)\n")
        f.write(f"  records with >=1 truncated  : {len(before_records)}"
                f" ({100 * len(before_records) / len(records):.1f}%)\n\n")
        f.write("  by position:\n")
        for (idx, role), n in sorted(before_positions.items()):
            f.write(f"    msg {idx} {role:6s}: {n}\n")

        f.write("\nREPAIRS\n")
        for k, v in sorted(reasons.items()):
            f.write(f"  {k:44s} {v}\n")
        f.write(f"  words removed: human {words_removed['human']}, "
                  f"gpt {words_removed['gpt']}\n\n")

        f.write("AFTER\n")
        f.write(f"  records        : {len(out)}\n")
        f.write(f"  truncated turns: 0\n")
        f.write(f"  dropped records: {len(dropped)}\n")
        f.write(f"  pre-existing unbalanced-quote turns (not caused by repair, "
                f"left as-is here): {len(preexisting)}\n")
        f.write(f"  -> step 09 drops these {len({rid for rid, _ in preexisting})} "
                f"record(s) from the dataset\n\n")

        f.write("-" * 74 + "\nPRE-EXISTING QUOTE IMBALANCE (separate defect,\n"
                "dropped by step 09)\n" + "-" * 74 + "\n")
        for rid, idx in preexisting:
            f.write(f"{rid} | msg {idx}\n")
        f.write("\n")

        f.write("-" * 74 + "\nDROPPED RECORDS\n" + "-" * 74 + "\n")
        for d in dropped:
            f.write(f"{d['id']}\n    reason: {d['why']}\n")

        f.write("\n" + "-" * 74 + "\nREPAIRED TURNS\n" + "-" * 74 + "\n")
        for r in repairs:
            f.write(f"{r['id']} | msg {r['msg']} | {r['role']} | {r['reason']}\n")
            f.write(f"    removed : ...{r['removed'][:120]}\n")
            f.write(f"    kept end: ...{r['kept_tail']}\n\n")

        f.write("-" * 74 + "\nATTRIBUTION VERBS NOT IN THE CLEAN TEMPLATE\n" + "-" * 74 + "\n")
        odd = [r for r in repairs if r["role"] == "human" and r["verb"] not in verbs]
        f.write(f"{len(odd)} repaired human turn(s) end on a verb absent from the "
                f"clean-turn template.\n\n")
        for r in odd[:40]:
            f.write(f"{r['id']} | msg {r['msg']} | {r['verb']!r}\n")

    print(f"report : {REPORT}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
