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
STEP 06 - Drop records containing foreign-script intrusions.

The corpus contains fragments of Thai, Bengali, CJK, Hangul, Cyrillic, Arabic,
Devanagari, Kannada, Telugu and Japanese spliced into English sentences, plus
accented Latin lookalikes that are not English words:

    "she шеpresents in an airy voice"       Cyrillic
    "for the very نخست time"                Arabic
    "her blue眼睛 dart up toward him"        CJK
    "with a heavy الثud that vibrates"       Arabic
    "for the very först time"               Latin (not an English word)
    "with clinical précision"                Latin (not an English word)
    "in a low and 떨리는 voice"               Hangul

These are vocabulary corruption from the generator, not chat-template leaks, and
they are unrecoverable: the intrusions are spliced *inside* English words and
phrases, so deleting the characters does not leave the sentence intact
("her blue眼睛 dart up" -> "her blue dart up" is a rewrite, not a repair).
Deleting the characters would silently corrupt the text; keeping them would train
the model to emit foreign script. The whole record is therefore dropped.

DETECTION
---------
Script is decided per word, and the test is positive rather than negative - a
word is flagged only when it is proven not to be English:

  1. every character is ASCII                        -> English
  2. the character is one of ALLOWED_PUNCT           -> English
     (curly quotes, dashes, ellipsis, bullet, nbsp)
  3. otherwise the whole word, accents included, is
     a word of the system English dictionary         -> English  ("café" passes)
  4. anything else                                   -> FOREIGN

Rule 3 is what makes this safe. A character-range test alone cannot separate
"précision" (corruption of "precision") from "café" (a real English loanword):
both are Latin script with a diacritic. Only a dictionary lookup distinguishes
them. Without it, "gözler", "först" and "så" would survive because they look
Latin.

The dictionary is /usr/share/dict/american-english. If it cannot be read, the
step falls back to a bundled allowlist of English loanwords and says so.

Input : build/05_whitespace.jsonl
Output: build/06_foreign_dropped.jsonl
        build/foreign_script_report.txt
"""

import json
import os
import re
import sys
import unicodedata
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import load, save, iter_turns

SRC = "build/05_whitespace.jsonl"
DST = "build/06_foreign_dropped.jsonl"
REPORT = "build/foreign_script_report.txt"

DICT_PATH = "/usr/share/dict/american-english"

# Characters that are not English letters but are ordinary English typography.
# These are never treated as foreign script.
ALLOWED_PUNCT = frozenset(
    "\u2018\u2019\u201c\u201d"        # curly single / double quotes
    "\u2013\u2014\u2026"              # en dash, em dash, ellipsis
    "\u00b7\u2022\u2011\u00a0"         # middot, bullet, non-breaking hyphen, nbsp
)

# Fallback only, used when the system dictionary cannot be read: English
# loanwords whose conventional spelling carries a diacritic.
FALLBACK_ALLOW = frozenset({
    "cafe", "cafes", "café", "cafés", "naive", "naïve", "naivety", "naïvety",
    "resume", "resumes", "résumé", "résumés", "cliche", "cliches", "cliché",
    "clichés", "decor", "décor", "fiance", "fiancé", "fiancee", "fiancée",
    "protege", "protégé", "protegee", "protégée", "expose", "exposé",
    "soiree", "soirée", "facade", "façade", "elite", "élite", "puree",
    "purée", "saute", "sauté", "creme", "crème", "flambe", "flambé",
    "papier", "mache", "jalapeno", "jalapeño", "pina", "piña", "senor",
    "señor", "senora", "señora", "senorita", "señorita", "manana", "mañana",
    "nino", "niño", "uber", "über", "doppelganger", "doppelgänger",
    "schadenfreude", "gestalt", "poltergeist", "zeitgeist", "angstrom",
    "smorgasbord", "fjord", "role", "rôle", "coup", "reverie", "rêverie",
    "boutique", "debris", "débris", "precis", "precise", "precision",
    "chateau", "château", "gateau", "gâteau", "tete", "tête", "fete", "fête",
    "betise", "bêtise", "cote", "côte", "pate", "pâté", "coupe", "coquette",
})

WORD_RE = re.compile(r"\w+", re.UNICODE)


def load_dictionary():
    """Return (set of lowercase words, label describing the source)."""
    try:
        with open(DICT_PATH, encoding="utf-8", errors="ignore") as f:
            words = {w.strip().lower() for w in f if w.strip()}
        if len(words) > 1000:
            return words, DICT_PATH
    except OSError:
        pass
    return set(FALLBACK_ALLOW), f"fallback allowlist ({len(FALLBACK_ALLOW)} words)"


def script_of(ch):
    """Readable script name for a character, from its Unicode name."""
    name = unicodedata.name(ch, "")
    if not name:
        return f"U+{ord(ch):04X}"
    head = name.split()[0]
    if head in ("CJK", "IDEOGRAPHIC", "HIRAGANA", "KATAKANA"):
        return "CJK/Japanese"
    if head == "HANGUL":
        return "Hangul"
    return head.title()


def foreign_words(text, dictionary):
    """Return [(word, script, offset)] for every non-English word in `text`.

    A word is a maximal run of word characters. It is foreign when it contains a
    character that is neither ASCII nor allowed punctuation, and the word as
    written is absent from the English dictionary.
    """
    out = []
    for m in WORD_RE.finditer(text):
        word = m.group()
        exotic = [c for c in word if ord(c) > 0x7F and c not in ALLOWED_PUNCT]
        if not exotic:
            continue
        if word.lower() in dictionary:
            continue
        scripts = sorted({script_of(c) for c in exotic})
        out.append((word, "+".join(scripts), m.start()))
    return out


def stray_nonascii(text, spans):
    """Non-ASCII characters that fall outside every word run (lone symbols)."""
    out = []
    for i, ch in enumerate(text):
        if ord(ch) < 0x80 or ch in ALLOWED_PUNCT:
            continue
        if any(a <= i < b for a, b in spans):
            continue
        out.append((ch, script_of(ch), i))
    return out


def scan(text, dictionary):
    """Every foreign finding in one turn."""
    spans = [(m.start(), m.end()) for m in WORD_RE.finditer(text)]
    return foreign_words(text, dictionary) + stray_nonascii(text, spans)


def main():
    dictionary, dict_label = load_dictionary()

    records = load(SRC)
    print("=" * 70)
    print("STEP 06 - drop records containing foreign script")
    print("=" * 70)
    print(f"input      : {SRC}")
    print(f"dictionary : {dict_label}")
    if dict_label.startswith("fallback"):
        print("  WARNING: system dictionary unavailable; accented Latin words are")
        print("           checked against a small bundled allowlist only.")

    hits = []                # one entry per intrusion
    flagged_records = set()
    by_script = Counter()
    by_role = Counter()

    for rec, idx, turn in iter_turns(records):
        value = turn["value"]
        found = scan(value, dictionary)
        if not found:
            continue
        flagged_records.add(rec["id"])
        for word, script, pos in found:
            s = max(0, pos - 45)
            e = min(len(value), pos + len(word) + 45)
            hits.append({
                "id": rec["id"], "msg": idx, "role": turn["from"],
                "word": word, "script": script,
                "context": value[s:e].replace("\n", "\\n"),
            })
            by_script[script] += 1
            by_role[turn["from"]] += 1

    print(f"\nintrusions      : {len(hits)}")
    print(f"affected records: {len(flagged_records)}")
    print(f"by role         : {dict(by_role)}")
    print("\nby script:")
    for k, v in by_script.most_common():
        print(f"  {k:18s} {v}")

    # --- drop -------------------------------------------------------------
    out = [r for r in records if r["id"] not in flagged_records]
    dropped = [r for r in records if r["id"] in flagged_records]

    # --- verification -----------------------------------------------------
    problems = []

    # 1. The defect is gone from the output.
    residual = [(rec["id"], idx)
                for rec, idx, turn in iter_turns(out)
                if scan(turn["value"], dictionary)]
    if residual:
        problems.append(f"{len(residual)} turn(s) still contain foreign script, "
                        f"e.g. {residual[:3]}")

    # 2. Exactly the flagged records were dropped - no more, no fewer.
    dropped_ids = {r["id"] for r in dropped}
    if dropped_ids != flagged_records:
        problems.append("dropped set != flagged set "
                        f"(extra: {sorted(dropped_ids - flagged_records)[:3]}, "
                        f"missing: {sorted(flagged_records - dropped_ids)[:3]})")

    # 3. Every surviving record is byte-identical to its input. This step only
    #    deletes whole lines; it must not edit a character of what stays.
    originals = {r["id"]: r for r in records}
    mutated = [r["id"] for r in out
               if json.dumps(originals[r["id"]], ensure_ascii=False)
               != json.dumps(r, ensure_ascii=False)]
    if mutated:
        problems.append(f"{len(mutated)} surviving record(s) were modified, "
                        f"e.g. {mutated[:3]}")

    # 4. Order and shape of the survivors are intact.
    expected = [r["id"] for r in records if r["id"] not in flagged_records]
    if [r["id"] for r in out] != expected:
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

    print(f"\ndropped records : {len(dropped)}")
    for d in dropped:
        print(f"  {d['id']}")

    after_turns = sum(len(r["conversations"]) for r in out)
    print(f"\nafter:")
    print(f"  records : {len(out)} "
          f"(dropped {len(dropped)}, {100 * len(dropped) / len(records):.2f}%)")
    print(f"  turns   : {after_turns} "
          f"(removed {sum(len(r['conversations']) for r in dropped)})")

    print("\nverification:")
    if problems:
        for p in problems:
            print(f"  FAIL  {p}")
    else:
        print("  PASS  no foreign script remains")
        print("  PASS  exactly the flagged records were dropped")
        print("  PASS  every surviving record is byte-identical to its input")
        print("  PASS  record order preserved")
        print("  PASS  every record keeps 8 human/gpt turns")
        print("  PASS  ids unique")
        print("  PASS  no empty turns")

    save(out, DST)
    print(f"\nwritten: {DST}")

    # --- report -----------------------------------------------------------
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("Foreign-script report\n")
        f.write("=" * 74 + "\n\n")
        f.write(f"input      : {SRC}\n")
        f.write(f"output     : {DST}\n")
        f.write(f"dictionary : {dict_label}\n\n")
        f.write("POLICY\n")
        f.write("  Records containing foreign script are DROPPED, not flagged.\n")
        f.write("  The intrusions are spliced inside English words, so deleting\n")
        f.write("  the characters would corrupt the text, while keeping them\n")
        f.write("  would teach the model to emit foreign script. Neither is\n")
        f.write("  acceptable, so the whole record is removed.\n\n")
        f.write("BEFORE\n")
        f.write(f"  records     : {len(records)}\n")
        f.write(f"  intrusions  : {len(hits)}\n")
        f.write(f"  records hit : {len(flagged_records)}\n")
        f.write(f"  by role     : {dict(by_role)}\n\n")
        f.write("  by script:\n")
        for k, v in by_script.most_common():
            f.write(f"    {k:18s} {v}\n")
        f.write("\nAFTER\n")
        f.write(f"  records      : {len(out)}\n")
        f.write(f"  dropped      : {len(dropped)} "
                f"({100 * len(dropped) / len(records):.2f}%)\n")
        f.write(f"  turns removed: "
                f"{sum(len(r['conversations']) for r in dropped)}\n\n")
        f.write("-" * 74 + "\nDROPPED RECORDS\n" + "-" * 74 + "\n")
        for h in hits:
            f.write(f"{h['id']} | msg {h['msg']} | {h['role']} | "
                    f"{h['script']} | {h['word']!r}\n")
            f.write(f"    context: ...{h['context']}...\n\n")

    print(f"report : {REPORT}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
