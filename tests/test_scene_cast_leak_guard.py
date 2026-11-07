# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the v13.5.1 ``SCENE CAST`` note-leak guard.

The defect these pin: ``prompts.scene_cast`` appends a per-reply cast note to the reply's
final user message (or its system prompt, on ``placement: system``), and some models echo
that note back as the first line of the reply instead of writing prose:

    SCENE CAST
    Samara: PRESENT
    Ashlyn: LEAVES
    Sage: PRESENT

    I watch Ashlyn stride away in those pointed toe pumps and I don't stop her.

The echo is also STALE: it is the note for the reply being written, so a row that kept it
repeats "Ashlyn: LEAVES" on every later turn, long after she left. This is the same class
of bug as the v13.2.1 ``{style}`` instruction leak, and is guarded the same conservative
way -- see ``tests/test_style_leak_guard.py``.

Covered here:

1. Both shipped block shapes are stripped: ``NAME: PRESENT`` / ``NAME: LEAVES`` one per
   line (``shape: minimal``), and ``PRESENT: a, b`` / ``NOT IN THE SCENE: c`` /
   ``LEAVES: c`` / ``ARRIVES: c`` (``shape: full``).
2. The blank lines that separated the block from the prose go with it.
3. Leading whitespace above the block is preserved.
4. A message that is ONLY the block is left alone rather than emptied: that is a failed
   generation, not a leak with prose after it.
5. A block that is not the first line is left alone.
6. Ordinary prose that contains the words "scene cast" is left alone.
7. Text without a block is returned byte-identical.
8. ``generation.py`` calls the guard on the man's messages, and only when a cast is
   actually in play, so a cast-free run stays byte-identical to before the guard existed.

Run from the repository root: ``python -B tests/test_scene_cast_leak_guard.py``
"""

import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

import text_utils

FAILURES = []


def check(label, condition, detail=None):
    if condition:
        print(f"  ok   {label}")
    else:
        print(f"  FAIL {label}" + (f"  -- {detail!r}" if detail is not None else ""))
        FAILURES.append(label)


PROSE = "I watch Ashlyn stride away in those pointed toe pumps and I don't stop her."

# --- 1/2/3: both shapes are stripped, the prose survives --------------------------------
print("stripping a leaked SCENE CAST note:")

minimal = "SCENE CAST\nSamara: PRESENT\nAshlyn: LEAVES\nSage: PRESENT\n\n" + PROSE
check("removes a 'NAME: PRESENT' block", text_utils.strip_scene_cast_leak(minimal) == PROSE)

full = "SCENE CAST\nPRESENT: Haisley, Eloira\nLEAVES: Camilla\n\n" + PROSE
check("removes a 'PRESENT: a, b' block", text_utils.strip_scene_cast_leak(full) == PROSE)

not_in_scene = "SCENE CAST\nPRESENT: A, B\nNOT IN THE SCENE: C\n\n" + PROSE
check("removes a 'NOT IN THE SCENE' block",
      text_utils.strip_scene_cast_leak(not_in_scene) == PROSE)

arrives = "SCENE CAST\nPRESENT: A\nARRIVES: B\n\n" + PROSE
check("removes an 'ARRIVES' block", text_utils.strip_scene_cast_leak(arrives) == PROSE)

check("removes the blank line that followed the block",
      not text_utils.strip_scene_cast_leak(minimal).startswith("\n"))

check("removes a multi-blank-line gap",
      text_utils.strip_scene_cast_leak("SCENE CAST\nA: PRESENT\n\n\n\n" + PROSE) == PROSE)

check("accepts a lowercase header", 
      text_utils.strip_scene_cast_leak("scene cast\nA: PRESENT\n\n" + PROSE) == PROSE)

check("accepts a trailing colon on the header",
      text_utils.strip_scene_cast_leak("SCENE CAST:\nA: PRESENT\n\n" + PROSE) == PROSE)

check("preserves leading whitespace above the block",
      text_utils.strip_scene_cast_leak("\n\nSCENE CAST\nA: PRESENT\n\n" + PROSE) == "\n\n" + PROSE)

# --- 4/5/6/7: the guard refuses to make things worse ------------------------------------
print("refusing to over-strip:")

block_only = "SCENE CAST\nA: PRESENT\nB: LEAVES\n"
check("keeps a message that is only the block",
      text_utils.strip_scene_cast_leak(block_only) == block_only)

later = "The room was quiet.\n\nSCENE CAST\nA: PRESENT\n\n" + PROSE
check("keeps a block that is not the first line",
      text_utils.strip_scene_cast_leak(later) == later)

prose_mention = "The scene cast was small that night, just the two of them."
check("keeps prose that merely mentions the scene cast",
      text_utils.strip_scene_cast_leak(prose_mention) == prose_mention)

check("returns text without a block byte-identical",
      text_utils.strip_scene_cast_leak(PROSE) == PROSE)
check("returns empty input unchanged", text_utils.strip_scene_cast_leak("") == "")
check("returns None unchanged", text_utils.strip_scene_cast_leak(None) is None)

# A bare header line with prose under it is still a leak: the header goes, the prose stays.
header_then_prose = "SCENE CAST\n" + PROSE
check("strips a bare header with no entries under it",
      text_utils.strip_scene_cast_leak(header_then_prose) == PROSE)

# --- 8: the worker calls the guard, gated on a cast being in play ------------------------
print("wiring in the worker:")

with open(os.path.join(REPO, "generation.py"), encoding="utf-8") as handle:
    source = handle.read()

check("generation.py calls strip_scene_cast_leak",
      "strip_scene_cast_leak(" in source)

# Both call sites must sit inside an `if scene_cast_plan["members"]` guard, so a cast-free
# run is unaffected.
guard_pattern = re.compile(
    r'if scene_cast_plan\["members"\] and (?:initial_user_question|user_continuation_reply):\s*\n'
    r'\s*cleaned_\w+ = text_utils\.strip_scene_cast_leak\(')
check("both calls are gated on a cast being in play",
      len(guard_pattern.findall(source)) == 2,
      guard_pattern.findall(source))

# --- 9: the per-task guidance offset (D5) ----------------------------------------------
print("per-task guidance offset:")

import generation as g  # noqa: E402  (needs REPO on sys.path first)

cfg = {"enabled": True, "shape": "minimal", "min_present": 2, "total_slots": 8,
       "exit_guidance": ["EXIT-0", "EXIT-1", "EXIT-2", "EXIT-3"]}
plan = g._build_scene_cast_plan(["A", "B", "C"], 8, cfg)
state = g._cast_state_from_plan(plan)
leaver = next((m for m in plan["members"] if m["exit_slot"] is not None), None)
if leaver is None:
    check("the test plan has an exit to index", False, plan)
else:
    notes = []
    for offset in range(4):
        offset_cfg = dict(cfg)
        offset_cfg["guidance_offset"] = offset
        notes.append(g._cast_presence_note(plan, state, leaver["exit_slot"], offset_cfg))
    check("a different offset selects a different exit beat", len(set(notes)) == 4, notes)
    # The offset must not disturb cache stability: two static slots still match.
    static = [s for s in range(1, 9)
              if s not in {m["enter_slot"] for m in plan["members"]}
              and s not in {m["exit_slot"] for m in plan["members"]}]
    if len(static) >= 2:
        offset_cfg = dict(cfg)
        offset_cfg["guidance_offset"] = 3
        check("the offset keeps two static slots byte-identical",
              g._cast_presence_note(plan, state, static[0], offset_cfg)
              == g._cast_presence_note(plan, state, static[1], offset_cfg))

print()
if FAILURES:
    print(f"{len(FAILURES)} FAILURE(S): {FAILURES}")
    sys.exit(1)
print("all scene-cast-leak-guard checks passed")
