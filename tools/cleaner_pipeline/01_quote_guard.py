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
STEP 01 - Dialogue-quote integrity guard.

This is the FIRST step of the pipeline, before any token stripping, whitespace
normalisation, truncation repair or record filtering. It exists because the three
tests it runs were previously only available in a separate script that had to be
remembered and run by hand. A hand-run check is not a check: if nobody runs it, a
corpus full of unpaired dialogue quotes ships. Those three tests now live in
`common.py` and are enforced here, so the pipeline cannot produce an output that
they would have rejected.

WHAT IT CHECKS

  * CURLY quotes (U+201C / U+201D) - a left-to-right positional WALK, not a bare
    count.
      - missing_open  : a close-quote with nothing open before it.
      - missing_close : a new open-quote appears mid-text before the previous one
                        closed, or an open-quote is left unclosed at the end of
                        the turn.
    Legit multi-paragraph dialogue is NOT flagged: a re-open that sits at a
    paragraph boundary (preceded only by whitespace after a newline) is a
    continuation of the same quotation, not a new unclosed one.

  * STRAIGHT quotes ( " ) - inch/feet marks (6", 5'9") are stripped FIRST, so a
    measurement is never mistaken for a quotation mark. An ODD number of straight
    quotes left over means an unpaired dialogue quote. `--straight-warn-only`
    downgrades this to a warning (the record is kept).

  * MINIMUM DIALOGUE - every reply is supposed to contain a quoted line of speech,
    so each checked reply must hold at least --min-quotes quotation marks
    (default 2 = one open/close pair).
        "It's great," she said.    -> OK       (2 marks)
        It's great, she said.      -> DROPPED  (0 marks)
        "It's great, she said.     -> DROPPED  (1 mark, also missing_close)

    Apostrophes (' and U+2019) are never counted: contractions and possessives are
    not quotation marks.

WHY IT RUNS FIRST

  Every later step edits text. Steps 02-05 delete tokens and characters; step 08
  repairs truncation by cutting a suffix and closing the sentence. Once text has
  been rewritten, a quote imbalance can no longer be attributed to the SOURCE: a
  repair that legitimately closes an open quotation would look like a new defect,
  and a genuine source defect could be masked by an edit. Measuring the defect on
  the pristine input keeps the diagnosis honest and makes this step's report the
  single authoritative record of how many records the corpus loses to bad quotes.

WHY A RECORD IS DROPPED, NOT REPAIRED

  There is no way to know WHICH quote is the stray one without inventing a reading
  of the source. Deleting the first closes the narration and leaves the dialogue as
  unmarked prose; deleting the last leaves the dialogue unclosed. Either choice
  silently rewrites the scene, so the whole record goes. The broken turn is also
  usually the model's INPUT prompt, and shipping a prompt that mis-closes its own
  quotes teaches the model to do the same.

MEASURED ON THIS DATASET

  The corpus uses straight quotes only (36,450 marks, zero curly marks), so the
  guard flags nothing:  FIRST QUOTE GUARD: 0 flagged, 0 dropped. It stays in the
  pipeline because the defect is one generator glitch away and the check is cheap.

Survivors are preserved BYTE FOR BYTE: this step only ever deletes whole lines.
It verifies that before writing (dropped set == flagged set, survivors identical to
input, order preserved, 8-turn shape intact, ids unique).

Input : input.jsonl  (or $SOURCE / SOURCE=... ./run_all.sh)
Output: build/01_quotes.jsonl
        build/quote_guard_report.txt
"""

import argparse
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (extract_turns, norm_role, quote_errors_in_record,
                    resolve_source, MIN_QUOTES, MIN_QUOTES_ROLES)

DST = "build/01_quotes.jsonl"
REPORT = "build/quote_guard_report.txt"


def snippet(text, limit=300):
    """Show the turn, truncated to a readable length."""
    flat = text.replace("\n", "\\n")
    return flat if len(flat) <= limit else flat[:limit] + " [...]"


def record_id(obj, lineno):
    """The record's id, or a line-based stand-in when the schema has none."""
    if isinstance(obj, dict):
        for key in ("id", "uid", "name", "conversation_id"):
            val = obj.get(key)
            if isinstance(val, str) and val.strip():
                return val
    return f"line-{lineno}"


def main():
    ap = argparse.ArgumentParser(
        description="Step 01: drop records whose dialogue quotes do not pair up.")
    ap.add_argument("--input", default=None,
                    help="override the source path (default: resolve_source())")
    ap.add_argument("--straight-warn-only", action="store_true",
                    help="treat an odd straight-quote count as a warning "
                         "(record kept), not a removal")
    ap.add_argument("--min-quotes", type=int, default=MIN_QUOTES,
                    help=f"minimum quotation marks each reply must contain "
                         f"(default {MIN_QUOTES} = one open/close pair; "
                         f"0 disables the check)")
    ap.add_argument("--min-quotes-roles", default="human,gpt",
                    help="comma list of roles that must satisfy --min-quotes "
                         "(default: human,gpt)")
    ap.add_argument("--roles", default="all",
                    help="comma list of roles to scan for quote errors, "
                         "or 'all' (default: all)")
    args = ap.parse_args()

    src_path = resolve_source(args.input)

    check_roles = (None if args.roles.strip().lower() == "all"
                   else {norm_role(r) for r in args.roles.split(",") if r.strip()})
    minq_roles = {norm_role(r) for r in args.min_quotes_roles.split(",") if r.strip()}

    print("=" * 70)
    print("STEP 01 - dialogue-quote integrity guard")
    print("=" * 70)
    print(f"input: {src_path}")
    print(f"min-quotes = {args.min_quotes} for roles: {sorted(minq_roles)}")
    print(f"straight-quote handling: "
          f"{'warn only' if args.straight_warn_only else 'drop record'}")

    # --- scan -------------------------------------------------------------
    # Read raw lines so a surviving record can be written back byte for byte. A
    # record is never re-serialised here.
    total_lines = 0
    total_turns = 0
    schema_counts = Counter()
    role_counts = Counter()
    flagged = []          # (lineno, id, [reasons], [warns], [(role, text), ...])
    flagged_lines = set()
    bad_json = []         # (lineno, error)
    out = []              # surviving raw lines, byte for byte

    with open(src_path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            if not raw.strip():
                continue
            total_lines += 1
            try:
                obj = json.loads(raw)
            except Exception as exc:
                bad_json.append((lineno, str(exc)))
                flagged_lines.add(lineno)
                continue

            schema, turns = extract_turns(obj)
            schema_counts[schema] += 1
            total_turns += len(turns)

            reasons, warns = quote_errors_in_record(
                obj, check_roles, minq_roles, args.min_quotes,
                args.straight_warn_only)

            for who, text in turns:
                if isinstance(text, str) and (check_roles is None or who in check_roles):
                    role_counts[who] += 1

            if reasons:
                flagged.append((lineno, record_id(obj, lineno), reasons, warns, turns))
                flagged_lines.add(lineno)
            else:
                out.append(raw)

    flagged_records = len(flagged) + len(bad_json)

    print(f"\nbefore:")
    print(f"  records            : {total_lines}")
    print(f"  turns              : {total_turns}")
    print(f"  schema             : {dict(schema_counts)}")
    print(f"  turns scanned/role : {dict(role_counts)}")
    print(f"  flagged records    : {flagged_records} "
          f"({len(flagged)} quote error, {len(bad_json)} invalid json)")

    # A schema that never matches means nothing was actually checked. Say so loudly
    # rather than reporting a clean run.
    only_fallback = (schema_counts.get("fallback")
                     and not (schema_counts.get("conversations")
                              or schema_counts.get("messages")
                              or schema_counts.get("human_gpt")))
    if only_fallback:
        print("  WARNING: every record hit the generic fallback - the schema is "
              "not recognised, so dialogue may not be getting checked at all")

    # --- verification -----------------------------------------------------
    problems = []

    # 1. The defect is gone from the output.
    still = []
    for lineno, raw in enumerate(out, 1):
        try:
            obj = json.loads(raw)
        except Exception:
            still.append((lineno, "invalid json"))
            continue
        reasons, _ = quote_errors_in_record(
            obj, check_roles, minq_roles, args.min_quotes, args.straight_warn_only)
        if reasons:
            still.append((lineno, reasons[:2]))
    if still:
        problems.append(f"{len(still)} flagged record(s) survived, e.g. {still[:3]}")

    # 2. Exactly the flagged lines were dropped - no more, no fewer. A filter that
    #    quietly eats an extra record is the failure mode that matters most, so the
    #    counts must match exactly.
    expected_kept = total_lines - flagged_records
    if len(out) != expected_kept:
        problems.append(f"kept {len(out)} line(s), expected {expected_kept}")

    # 3. Every survivor is byte-identical to its input line. `out` is built by
    #    copying the original raw line verbatim, so this is a structural guarantee
    #    rather than a re-serialisation; the check proves nothing reformatted it.
    with open(src_path, encoding="utf-8") as f:
        src_lines = [l for l in f if l.strip()]
    expected_lines = [l for i, l in enumerate(src_lines, 1)
                      if i not in flagged_lines]
    if out != expected_lines:
        problems.append("surviving lines are not byte-identical to the input")

    # 4. Order is preserved. Byte identity (check 3) implies it; asserted
    #    separately so a future refactor that re-serialises cannot hide a reorder.
    src_ids = [json.loads(l).get("id") for l in expected_lines]
    if [json.loads(l).get("id") for l in out] != src_ids:
        problems.append("record order changed")

    # 5. Shape of the survivors is intact - each record still has exactly the turns
    #    it had on the way in, in the same order. This is deliberately compared
    #    against the INPUT record rather than against a hard-coded 8-turn shape: the
    #    guard is schema-agnostic (it accepts ShareGPT, messages and flat records),
    #    so asserting one fixed shape would make it reject every other corpus. The
    #    8-turn convention is reported below, informationally, when the input is
    #    uniform.
    parsed_out = [json.loads(l) for l in out]
    parsed_src = [json.loads(l) for l in expected_lines]
    bad_shape = []
    for before, after in zip(parsed_src, parsed_out):
        b_turns = before.get("conversations")
        a_turns = after.get("conversations")
        rid = after.get("id", "?")
        if b_turns is None or a_turns is None:
            # Non-ShareGPT schema: compare the extracted turn count instead.
            if len(extract_turns(before)[1]) != len(extract_turns(after)[1]):
                bad_shape.append(rid)
            continue
        if len(b_turns) != len(a_turns):
            bad_shape.append(rid)
        elif [t.get("from") for t in b_turns] != [t.get("from") for t in a_turns]:
            bad_shape.append(rid)
    if bad_shape:
        problems.append(f"{len(bad_shape)} record(s) changed turn count or role "
                         f"sequence, e.g. {bad_shape[:3]}")

    # 6. Ids stay unique.
    ids = [r.get("id") for r in parsed_out]
    if len(ids) != len(set(ids)):
        problems.append("duplicate record ids")

    # 7. No empty turns. Uses the schema-aware extractor so this holds for every
    #    supported shape, not just ShareGPT.
    empty = []
    for r in parsed_out:
        for i, (who, text) in enumerate(extract_turns(r)[1]):
            if not str(text).strip():
                empty.append((r.get("id", "?"), i, who))
    if empty:
        problems.append(f"{len(empty)} empty turn(s), e.g. {empty[:3]}")

    # 8. Informational: the corpus's 8-turn human/gpt convention. Not a failure -
    #    the guard does not impose a shape, it only preserves the one it was given.
    eight_turn = all(
        len(r.get("conversations") or []) == 8
        and [t.get("from") for t in r.get("conversations") or []]
        == ["human", "gpt"] * 4
        for r in parsed_out)

    # --- summary ----------------------------------------------------------
    after_turns = sum(len(extract_turns(r)[1]) for r in parsed_out)
    pct = (100 * flagged_records / total_lines) if total_lines else 0.0
    print(f"\ndropped records: {flagged_records} ({pct:.2f}%)")
    for lineno, rid, reasons, warns, turns in flagged:
        print(f"  {str(rid)[:64]}  (line {lineno})")
        for why in reasons:
            print(f"      {why}")
        broken = [t for t in turns
                  if isinstance(t[1], str) and
                  any(t[0] in r for r in reasons)]
        if broken:
            print(f"      text: {snippet(broken[0][1])}")
    for lineno, err in bad_json:
        print(f"  line {lineno}: invalid json - {err}")

    print(f"\nafter:")
    print(f"  records            : {len(out)}")
    print(f"  turns              : {after_turns} "
          f"(removed {total_turns - after_turns})")

    print("\nverification:")
    if problems:
        for p in problems:
            print(f"  FAIL  {p}")
    else:
        print("  PASS  no flagged record survived")
        print("  PASS  exactly the flagged records were dropped")
        print("  PASS  every surviving record is byte-identical to its input")
        print("  PASS  record order preserved")
        print("  PASS  every record keeps its turn count and role sequence")
        print("  PASS  ids unique")
        print("  PASS  no empty turns")
        if eight_turn and parsed_out:
            print("  INFO  every record keeps 8 human/gpt turns")

    os.makedirs("build", exist_ok=True)
    with open(DST, "w", encoding="utf-8") as f:
        f.writelines(out)
    print(f"\nwritten: {DST}")

    # --- report -----------------------------------------------------------
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("Dialogue-quote guard report\n")
        f.write("=" * 74 + "\n\n")
        f.write(f"input : {src_path}\n")
        f.write(f"output: {DST}\n\n")
        f.write("WHY THIS STEP RUNS FIRST\n")
        f.write("  Every later step edits text: tokens and control characters are\n")
        f.write("  deleted, and truncation repair cuts a suffix and closes the\n")
        f.write("  sentence. Once text has been rewritten, a quote imbalance can no\n")
        f.write("  longer be attributed to the source. Measuring on the pristine\n")
        f.write("  input keeps the diagnosis honest.\n\n")
        f.write("TESTS\n")
        f.write("  curly   positional walk for U+201C / U+201D:\n")
        f.write("          missing_open  - close with nothing open before it\n")
        f.write("          missing_close - open not closed, or re-opened mid-text\n")
        f.write("                          (a re-open at a paragraph boundary is a\n")
        f.write("                          legitimate continuation, not a defect)\n")
        f.write("  straight odd count of \" after inch/feet marks are stripped\n")
        f.write("  min-quotes each checked reply needs a quoted line of speech\n")
        f.write("          apostrophes are never counted\n\n")
        f.write(f"SETTINGS\n")
        f.write(f"  min-quotes        : {args.min_quotes}\n")
        f.write(f"  min-quotes roles  : {sorted(minq_roles)}\n")
        f.write(f"  roles scanned     : "
                f"{'all' if check_roles is None else sorted(check_roles)}\n")
        f.write(f"  straight quotes   : "
                f"{'warn only' if args.straight_warn_only else 'drop record'}\n\n")
        f.write("BEFORE\n")
        f.write(f"  records           : {total_lines}\n")
        f.write(f"  turns             : {total_turns}\n")
        f.write(f"  schema matched    : {dict(schema_counts)}\n")
        f.write(f"  turns by role     : {dict(role_counts)}\n")
        f.write(f"  flagged records   : {flagged_records} "
                f"({len(flagged)} quote error, {len(bad_json)} invalid json)\n\n")
        f.write("AFTER\n")
        f.write(f"  records           : {len(out)}\n")
        f.write(f"  turns             : {after_turns}\n")
        f.write(f"  turns removed     : {total_turns - after_turns}\n")
        f.write(f"  records dropped   : {flagged_records} ({pct:.2f}%)\n\n")
        f.write("-" * 74 + "\nDROPPED RECORDS\n" + "-" * 74 + "\n")
        if not flagged and not bad_json:
            f.write("none - no turn in this dataset has an unpaired dialogue quote\n")
        for lineno, rid, reasons, warns, turns in flagged:
            f.write(f"{rid}  (line {lineno})\n")
            for why in reasons:
                f.write(f"    {why}\n")
            for who, text in turns:
                if isinstance(text, str) and any(who in r for r in reasons):
                    f.write(f"    {who} text: {snippet(text)}\n")
            f.write("\n")
        for lineno, err in bad_json:
            f.write(f"line {lineno}: invalid json - {err}\n")

    print(f"report : {REPORT}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
