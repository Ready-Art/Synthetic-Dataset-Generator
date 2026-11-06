# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the v13.2.1 ``{style}`` instruction-leak guard.

The defect these pin: every prompt that names ``{style}`` interpolates it as
``Begin this message as follows: {style}``, and some models echo that clause back as the
first line of the reply instead of writing prose. Because the shipped styles are
imperatives ("open on a sound..."), the echo reads as a stage direction pasted on top of
the message:

    open on someone entering the shop, the bell's chime cutting through the silence.

    "You actually remembered," she whispers.

Measured on a 3-conversation cast-continuity run against a vLLM endpoint, this hit
18 of 72 messages (25%), and it clustered in ONE conversation -- the model that had been
given the style echoed it for all nine later turns of that row.

The important property is that the leak is NEVER an exact copy of a pool entry: the model
paraphrases it ("someone entering the shop" for "someone entering or leaving"). Matching the
configured strings therefore cannot work, which is why the guard matches the imperative verb
the style always opens with.

Covered here:

1. A leaked instruction on its own line before the prose is removed, and the prose survives.
2. The blank lines that separated it are removed too, so the message does not start with a
   gap.
3. Leading whitespace above the instruction is preserved.
4. Ordinary prose that happens to contain the word "open" is untouched -- as a dialogue
   opener, mid-sentence, and capitalised at the start of a real sentence.
5. A message that is ONLY an instruction is left alone rather than emptied: that is a failed
   generation, not a leak with prose after it.
6. An instruction that is not the first line is left alone.
7. Empty/None input is returned unchanged.
8. ``generation.py`` calls the guard, and only when a style is actually in play, so a
   style-free run stays byte-identical to before the guard existed.

Run from the repository root: ``python -B tests/test_style_leak_guard.py``
"""

import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

import text_utils

FAILURES = []


def check(label, condition):
    if condition:
        print(f"  ok   {label}")
    else:
        print(f"  FAIL {label}")
        FAILURES.append(label)


PROSE = '"You actually remembered," she whispers, her voice cracking.'

# --- 1/2/3: the leak is removed, the prose survives, blank lines go with it -------------
print("stripping a leaked style instruction:")

leaked = ("open on someone entering the shop, the bell's chime cutting through the "
          "heavy silence like a blade.\n\n" + PROSE)
check("removes the instruction line", text_utils.strip_style_instruction_leak(leaked) == PROSE)

check("removes the blank line that followed it",
      not text_utils.strip_style_instruction_leak(leaked).startswith("\n"))

check("removes a multi-blank-line gap",
      text_utils.strip_style_instruction_leak("open on a sound.\n\n\n\n" + PROSE) == PROSE)

check("removes the 'Begin this message as follows:' wrapper form",
      text_utils.strip_style_instruction_leak(
          "Begin this message as follows: open on a sound.\n\n" + PROSE) == PROSE)

check("removes every shipped style verb",
      all(text_utils.strip_style_instruction_leak(f"{verb} on a sound.\n\n{PROSE}") == PROSE
          for verb in text_utils.STYLE_LEAD_VERBS))

check("preserves leading whitespace above the instruction",
      text_utils.strip_style_instruction_leak("\n\nopen on a sound.\n\n" + PROSE) == "\n\n" + PROSE)

# --- 4: real prose containing "open" is never touched -----------------------------------
print("leaving ordinary prose alone:")

untouched = [
    '"Open the door, would you?" she said.',
    'She pushed the door open. The bell rang.',
    'Open on a Monday, the shop was closed.',          # capitalised real sentence
    'He watched the open ledger on the counter.',
    '*She leans on the counter.* "Open it."',
]
for text in untouched:
    check(f"keeps {text[:38]!r}", text_utils.strip_style_instruction_leak(text) == text)

# --- 5/6/7: the guard refuses to make things worse -------------------------------------
print("refusing to over-strip:")

only_instruction = "open on someone entering the shop, the bell ringing."
check("keeps a message that is only the instruction",
      text_utils.strip_style_instruction_leak(only_instruction) == only_instruction)

later = "The shop was quiet.\n\nopen on a sound."
check("keeps an instruction that is not the first line",
      text_utils.strip_style_instruction_leak(later) == later)

check("returns empty input unchanged", text_utils.strip_style_instruction_leak("") == "")
check("returns None unchanged", text_utils.strip_style_instruction_leak(None) is None)

# A line that runs on into prose is not a standalone instruction.
run_on = "open on a sound and the bell rang as she came in through the door."
check("keeps a first line that is not a standalone clause",
      text_utils.strip_style_instruction_leak(run_on) == run_on)

# --- 8: the worker calls the guard, gated on a style being in play -----------------------
print("wiring in the worker:")

with open(os.path.join(REPO, "generation.py"), encoding="utf-8") as handle:
    source = handle.read()

check("generation.py calls strip_style_instruction_leak",
      "strip_style_instruction_leak(" in source)

# The call must sit inside an `if task_style:` guard, so a style-free run is unaffected.
guard_pattern = re.compile(
    r"if task_style:\s*\n\s*cleaned_answer = text_utils\.strip_style_instruction_leak\(")
check("the call is gated on `if task_style:`", bool(guard_pattern.search(source)))

print()
if FAILURES:
    print(f"{len(FAILURES)} FAILURE(S): {FAILURES}")
    sys.exit(1)
print("all style-leak-guard checks passed")
