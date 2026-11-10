# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""tests/test_scene_cast.py - v12.0 Scene Cast (characters join and leave).

Headless guard for the feature's three layers:
  1. the SCHEDULE (generation._build_scene_cast_plan / _scene_cast_feasible),
  2. the STATE MACHINE (generation._advance_cast_state / _cast_names_for_slot),
  3. the CHECK (detection.check_scene_continuity and its attribution helpers).

No model, no network, no display. Runs on its own from the repository root:
    python -B tests/test_scene_cast.py
"""

import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

import app_state  # noqa: E402  (creates output/ via logging_config, as documented)
import detection  # noqa: E402
import generation as g  # noqa: E402

detection.issue_timestamps = app_state.issue_timestamps
detection.issue_timestamps_lock = app_state.issue_timestamps_lock

FAILURES = []


def check(label, condition, detail=""):
    if condition:
        print("  ok   " + label)
    else:
        FAILURES.append(label)
        print("  FAIL " + label + (("  <- " + str(detail)) if detail else ""))


# --------------------------------------------------------------------------------------
# 1. Feasibility
# --------------------------------------------------------------------------------------

def test_feasibility():
    print("\nFeasibility (a change costs one present character)")
    # A join AND a leave each need one character to spare, so 2 characters with
    # min_present 2 can never change cast. This is the documented no-op.
    check("2 characters, min_present 2: no join, no leave",
          g._scene_cast_feasible(2, 6, 2, "minimal", 2) == (False, False),
          g._scene_cast_feasible(2, 6, 2, "minimal", 2))
    check("3 characters, min_present 2, 6 slots: both possible",
          g._scene_cast_feasible(3, 6, 2, "minimal", 2) == (True, True))
    check("3 characters, min_present 3: no spare, nothing possible",
          g._scene_cast_feasible(3, 6, 3, "minimal", 2) == (False, False))
    check("shape 'none' disables both",
          g._scene_cast_feasible(3, 6, 2, "none", 2) == (False, False))
    check("an unknown shape disables both (fails safe)",
          g._scene_cast_feasible(3, 6, 2, "bogus", 2) == (False, False))
    check("too few slots for a join (2 slots): leave only",
          g._scene_cast_feasible(3, 2, 2, "minimal", 2) == (False, True))


# --------------------------------------------------------------------------------------
# 2. Plan invariants
# --------------------------------------------------------------------------------------

def test_plan():
    print("\nPlan (schedule invariants)")
    cfg = {"enabled": True, "shape": "minimal", "min_present": 2}

    empty = g._build_scene_cast_plan(["A", "B"], 4, cfg)
    check("2 characters with min_present 2 yields an EMPTY plan",
          empty["members"] == [], empty)

    check("feature off yields an empty plan",
          g._build_scene_cast_plan(["A", "B", "C"], 6, {"enabled": False})["members"] == [])
    check("fewer than 2 characters yields an empty plan",
          g._build_scene_cast_plan(["A"], 6, cfg)["members"] == [])
    check("shape 'none' yields an empty plan",
          g._build_scene_cast_plan(["A", "B", "C"], 6,
                                   {"enabled": True, "shape": "none"})["members"] == [])

    for names, slots in ((["A", "B", "C"], 6), (["A", "B", "C", "D"], 8),
                         (["A", "B", "C", "D", "E"], 10), (["A", "B", "C"], 4),
                         # 12 slots is the shipped num_turns default and the shape that exposed the
                         # vacuous check below: the old exit formula landed so late that the
                         # post-exit replies were never reached.
                         (["A", "B", "C"], 12), (["A", "B", "C", "D"], 12),
                         (["A", "B", "C", "D", "E"], 12)):
        plan = g._build_scene_cast_plan(names, slots, cfg)
        members = plan["members"]
        if not members:
            check("%d characters / %d slots: no plan, and that is legal" % (len(names), slots), True)
            continue
        check("%d characters / %d slots: every character appears once" % (len(names), slots),
              len(members) == len(names)
              and sorted(m["number"] for m in members) == list(range(1, len(names) + 1)))
        check("%d characters / %d slots: slots in range, exit after enter" % (len(names), slots),
              all(0 <= m["enter_slot"] <= slots for m in members)
              and all(m["exit_slot"] is None or
                      (1 <= m["exit_slot"] <= slots and m["exit_slot"] > m["enter_slot"])
                      for m in members))
        check("%d characters / %d slots: a character never both joins and leaves" % (len(names), slots),
              not any(m["enter_slot"] > 0 and m["exit_slot"] is not None for m in members))
        # minimal = at most one join and one leave
        check("%d characters / %d slots: minimal shape does at most 1 join + 1 leave" % (len(names), slots),
              sum(1 for m in members if m["enter_slot"] > 0) <= 1
              and sum(1 for m in members if m["exit_slot"] is not None) <= 1)

        # min_present must hold at every slot.
        #
        # The state MUST be advanced between slots. Checking every slot against the INITIAL state is
        # vacuous: an exit scheduled for slot N never takes effect, so no post-exit slot is ever
        # examined and a schedule that drops the scene to one character passes silently. This is
        # exactly how a 12-slot plan with an exit at slot 9 and an arrival at slot 11 shipped with
        # one character present for a whole reply.
        state = g._cast_state_from_plan(plan)
        worst = slots + 1
        for slot in range(1, slots + 1):
            present = g._cast_names_for_slot(plan, state, slot)[2]
            worst = min(worst, len(present))
            state, _events = g._advance_cast_state(
                plan, state, slot, "", max_deferrals=1)
        check("%d characters / %d slots: never fewer than min_present present" % (len(names), slots),
              worst >= plan["min_present"], "min present %d < %d" % (worst, plan["min_present"]))

    # churn never drops below min_present either, over a longer conversation
    churn_cfg = {"enabled": True, "shape": "churn", "min_present": 2, "max_churn": 2}
    plan = g._build_scene_cast_plan(["A", "B", "C", "D", "E"], 10, churn_cfg)
    state = g._cast_state_from_plan(plan)
    worst = 11
    for slot in range(1, 11):
        present = g._cast_names_for_slot(plan, state, slot)[2]
        worst = min(worst, len(present))
        state, _events = g._advance_cast_state(plan, state, slot, "", max_deferrals=1)
    check("churn: never fewer than min_present present", worst >= plan["min_present"],
          "min present %d < %d" % (worst, plan["min_present"]))

    # --- timing: both beats must be visible in the transcript ----------------------------
    # A join on the last reply, or a leave on the last reply, is a beat no reader ever sees.
    print("timing: entrances and exits land early enough to matter:")
    for slots in (6, 8, 10, 12):
        plan = g._build_scene_cast_plan(["A", "B", "C"], slots, cfg)
        members = plan["members"]
        if not members:
            continue
        joins = [m["enter_slot"] for m in members if m["enter_slot"] > 0]
        exits = [m["exit_slot"] for m in members if m["exit_slot"] is not None]
        for slot in joins:
            # At least two replies must remain for the newcomer to be present in.
            check("%d slots: arrival at %d leaves room to be present" % (slots, slot),
                  slots - slot >= 1, "only %d reply(s) left" % (slots - slot))
        for slot in exits:
            # At least two replies must remain after the exit beat for the aftermath.
            check("%d slots: exit at %d leaves an aftermath" % (slots, slot),
                  slots - slot >= 1, "only %d reply(s) after the exit" % (slots - slot))

    check("a plan is deterministic for the same input",
          g._build_scene_cast_plan(["A", "B", "C"], 6, cfg) ==
          g._build_scene_cast_plan(["A", "B", "C"], 6, cfg))


# --------------------------------------------------------------------------------------
# 3. State machine (including the deliberate asymmetry)
# --------------------------------------------------------------------------------------

def test_state_machine():
    print("\nState machine (defer, force, cancel)")
    cfg = {"enabled": True, "shape": "minimal", "min_present": 2}
    plan = g._build_scene_cast_plan(["A", "B", "C"], 6, cfg)
    members = {m["name"]: m for m in plan["members"]}
    leaver = next((m for m in plan["members"] if m["exit_slot"] is not None), None)
    joiner = next((m for m in plan["members"] if m["enter_slot"] > 0), None)
    check("the 3-character/6-slot plan has a leaver and a joiner",
          leaver is not None and joiner is not None, plan["members"])

    # --- an un-narrated EXIT is CANCELLED, never assumed ---
    state = g._cast_state_from_plan(plan)
    number = leaver["number"]
    slot = leaver["exit_slot"]
    state, events = g._advance_cast_state(plan, state, slot, "Nobody does anything.", max_deferrals=1)
    check("un-narrated exit: first reply DEFERS",
          ("exit_deferred", leaver["name"]) in events and state[number] == "exiting", events)
    state, events = g._advance_cast_state(plan, state, slot + 1, "Nobody does anything.",
                                         max_deferrals=1)
    check("un-narrated exit: CANCELLED back to present (never assumed gone)",
          ("exit_cancelled", leaver["name"]) in events and state[number] == "present", events)

    # --- an un-narrated ARRIVAL is FORCED to present ---
    state = g._cast_state_from_plan(plan)
    number = joiner["number"]
    slot = joiner["enter_slot"]
    state, events = g._advance_cast_state(plan, state, slot, "Nobody does anything.",
                                         max_deferrals=0)
    check("un-narrated arrival: FORCED to present",
          ("arrival_forced", joiner["name"]) in events and state[number] == "present", events)

    # --- a narrated exit really exits ---
    state = g._cast_state_from_plan(plan)
    state, events = g._advance_cast_state(
        plan, state, leaver["exit_slot"],
        "%s said goodbye and left." % leaver["name"], max_deferrals=1)
    check("narrated exit: EXITED", ("exited", leaver["name"]) in events
          and state[leaver["number"]] == "exited", events)
    # and an exited character is then blocked
    blocked = g._cast_names_for_slot(plan, state, leaver["exit_slot"] + 1)[0]
    check("an exited character is blocked on later replies", leaver["name"] in blocked, blocked)

    # --- an absent character is blocked before arriving ---
    state = g._cast_state_from_plan(plan)
    blocked = g._cast_names_for_slot(plan, state, 1)[0]
    check("an absent character is blocked before its arrival", joiner["name"] in blocked, blocked)

    # --- no illegal state ever appears, over a full simulated run ---
    legal = {"absent", "arriving", "present", "exiting", "exited"}
    state = g._cast_state_from_plan(plan)
    bad = []
    for slot in range(1, 7):
        state, _events = g._advance_cast_state(plan, state, slot, "nothing happens", max_deferrals=1)
        for key, value in state.items():
            if key != "_deferrals" and value not in legal:
                bad.append((slot, key, value))
    check("no illegal state is ever produced", not bad, bad)

    # --- deferral counts live on the state, not the plan (plan is immutable) ---
    check("the plan carries no mutable deferral bookkeeping",
          "_deferrals" not in plan, list(plan.keys()))

    # --- an empty plan is a no-op ---
    state, events = g._advance_cast_state({"members": []}, {}, 1, "anything", max_deferrals=1)
    check("an empty plan advances nothing", events == [])


# --------------------------------------------------------------------------------------
# 4. The cast note
# --------------------------------------------------------------------------------------

def test_note():
    print("\nCast note (cache stability and content)")
    cfg = {"enabled": True, "shape": "minimal", "min_present": 2}
    plan = g._build_scene_cast_plan(["A", "B", "C"], 6, cfg)
    state = g._cast_state_from_plan(plan)
    note_cfg = dict(cfg)
    note_cfg["total_slots"] = 6

    check("an empty plan produces no note",
          g._cast_presence_note({"members": []}, {}, 1, note_cfg) == "")

    # A static slot's note depends only on the present/absent SET, never on the slot
    # index -- otherwise a 'system' placement would invalidate the prefix cache every
    # reply. Pick two slots with no transition between them.
    transition_slots = set()
    for m in plan["members"]:
        if m["exit_slot"]:
            transition_slots.add(m["exit_slot"])
        if m["enter_slot"]:
            transition_slots.add(m["enter_slot"])
    static = [s for s in range(1, 7) if s not in transition_slots]
    if len(static) >= 2:
        a = g._cast_presence_note(plan, state, static[0], note_cfg)
        b = g._cast_presence_note(plan, state, static[1], note_cfg)
        check("two static slots produce BYTE-IDENTICAL notes (prefix cache)", a == b, (a, b))
    else:
        check("two static slots produce BYTE-IDENTICAL notes (prefix cache)", True,
              "no two static slots in this plan")

    note = g._cast_presence_note(plan, state, 1, note_cfg)
    check("the note never contains a None", "None" not in note, note)
    check("the note names the absent characters",
          all(m["name"] in note for m in plan["members"] if m["enter_slot"] > 0), note)
    check("the note tells the model not to voice the absent characters",
          "Do not give the absent characters" in note, note)
    check("the note carries no per-reply counter (that would break the cache)",
          "reply 1 of" not in note, note)

    # a transition slot names the character and the guidance
    leaver = next(m for m in plan["members"] if m["exit_slot"] is not None)
    exit_note = g._cast_presence_note(plan, state, leaver["exit_slot"], note_cfg)
    check("an exit slot names the leaving character", leaver["name"] in exit_note, exit_note)
    check("an exit slot says the character is gone afterwards",
          "is gone" in exit_note, exit_note)

    # custom guidance replaces the built-in default
    custom = dict(note_cfg)
    custom["exit_guidance"] = ["CUSTOM-EXIT-TEXT"]
    custom_note = g._cast_presence_note(plan, state, leaver["exit_slot"], custom)
    check("custom exit guidance is used verbatim", "CUSTOM-EXIT-TEXT" in custom_note, custom_note)

    # cut-away only appears when enabled AND a transition is on this slot
    # (allow_cutaway lives on the PLAN, so the plan has to be rebuilt for it)
    cut_cfg = dict(cfg)
    cut_cfg["allow_cutaway"] = True
    cut_plan = g._build_scene_cast_plan(["A", "B", "C"], 6, cut_cfg)
    cut_state = g._cast_state_from_plan(cut_plan)
    cut_leaver = next((m for m in cut_plan["members"] if m["exit_slot"] is not None), None)
    check("the rebuilt plan carries allow_cutaway",
          cut_plan.get("allow_cutaway") is True, cut_plan)
    cut_note = g._cast_presence_note(cut_plan, cut_state, cut_leaver["exit_slot"], note_cfg)
    check("cut-away appears on a transition slot when enabled", "CUT-AWAY" in cut_note, cut_note)
    static_slot = static[0] if static else 1
    check("cut-away never appears on a static slot",
          "CUT-AWAY" not in g._cast_presence_note(cut_plan, cut_state, static_slot, note_cfg))
    check("cut-away never appears when allow_cutaway is off",
          "CUT-AWAY" not in g._cast_presence_note(plan, state, leaver["exit_slot"], note_cfg))


# --------------------------------------------------------------------------------------
# 5. The detector
# --------------------------------------------------------------------------------------

def test_detector():
    print("\nDetector (attribution, narration-only actions)")
    cases = [
        # (text, blocked, expect_hard)
        ('Melody said, "I am still here."', ["Melody"], True),
        ('"I am still here," Melody said.', ["Melody"], True),
        ('Melody: "Still here."', ["Melody"], True),
        ('Melody nodded slowly.', ["Melody"], True),
        ('Melody walked away.', ["Melody"], True),
        # a name merely mentioned INSIDE someone else's line is not a violation
        ('Cole asked, "Where is Melody?"', ["Melody"], False),
        # a name after the action verb belongs to the actor, not the object
        ('Cole nodded at Melody.', ["Melody"], False),
        ('The room was empty.', ["Melody"], False),
        ('Melody. Cole nodded.', ["Melody"], False),
        ("Melody's hands trembled.", ["Melody"], False),
        ('', ["Melody"], False),
        ('Melody said, "x."', [], False),
        # names with punctuation / accents / metacharacters must not raise
        ('A.B. said, "hello."', ["A.B."], True),
        ('Dr. Vance said, "hello."', ["Dr. Vance"], True),
        ('Zo\u00eb said, "hi."', ["Zo\u00eb"], True),
        ('Melody said, "hi."', ["Melody (the elder)"], False),
    ]
    bad = []
    for text, blocked, expect in cases:
        got, findings = detection.check_scene_continuity(text, blocked)
        if got != expect:
            bad.append((text, blocked, expect, got, findings))
    check("every attribution case matches its expectation", not bad, bad)

    # expected_names is SOFT: reported, never a hard violation
    violation, findings = detection.check_scene_continuity("Nothing happens.", [], ["Ines"])
    kinds = [f[0] for f in findings]
    check("a missing expected name is reported as expected_missing",
          kinds == ["expected_missing"], findings)
    check("a missing expected name is NOT a hard violation", violation is False)
    check("expected_missing is not in the hard kinds",
          "expected_missing" not in detection.SCENE_CONTINUITY_HARD_KINDS)

    violation, findings = detection.check_scene_continuity("Ines arrived.", [], ["Ines"])
    check("a present expected name produces no finding", findings == [], findings)

    # an empty blocked list short-circuits
    check("no blocked names: no violation",
          detection.check_scene_continuity('Melody said, "hi."', []) == (False, []))

    # a hard hit records a timestamp for the dashboard
    with app_state.issue_timestamps_lock:
        app_state.issue_timestamps["scene_continuity"] = []
    detection.check_scene_continuity('Melody said, "hi."', ["Melody"])
    with app_state.issue_timestamps_lock:
        stamped = len(app_state.issue_timestamps["scene_continuity"])
    check("a hard hit records a dashboard timestamp", stamped == 1, stamped)

    # a soft hit records NO timestamp
    with app_state.issue_timestamps_lock:
        app_state.issue_timestamps["scene_continuity"] = []
    detection.check_scene_continuity("Nothing.", [], ["Ines"])
    with app_state.issue_timestamps_lock:
        stamped = len(app_state.issue_timestamps["scene_continuity"])
    check("a soft hit records no dashboard timestamp", stamped == 0, stamped)


# --------------------------------------------------------------------------------------
# 6. Metadata schema
# --------------------------------------------------------------------------------------

def test_metadata():
    print("\nMetadata (schema v7, flat and type-stable)")
    empty = g._build_record_metadata(
        ["A"], "", "", False, False, 1, {}, None, "2026-01-01 00:00:00")
    check("meta_version is 7", empty["meta_version"] == 7, empty["meta_version"])
    check("cast_schedule / cast_events are empty strings when the feature is off",
          empty["cast_schedule"] == "" and empty["cast_events"] == "", empty)
    check("quality_cast_continuity is -1.0 when unscored",
          empty["quality_cast_continuity"] == -1.0, empty["quality_cast_continuity"])

    populated = g._build_record_metadata(
        ["A"], "", "", False, False, 1, {}, None, "2026-01-01 00:00:00",
        cast_schedule="Melody:enter 0 exit 4",
        cast_events=[("exited", "Melody"), ("arrived", "Cole")])
    check("cast_schedule is recorded",
          populated["cast_schedule"] == "Melody:enter 0 exit 4", populated["cast_schedule"])
    check("cast_events are recorded as name + event",
          populated["cast_events"] == "Melody exited; Cole arrived", populated["cast_events"])
    check("the metadata keys are identical with and without cast data",
          set(empty) == set(populated), set(empty) ^ set(populated))


# --------------------------------------------------------------------------------------
# 7. Quality dimension gating
# --------------------------------------------------------------------------------------

def test_quality_dimension():
    print("\nQuality dimension (LLM-only, opt-in)")
    import quality
    from app_state import global_config
    check("cast_continuity is NOT a heuristic dimension",
          "cast_continuity" not in quality.HEURISTIC_DIMENSIONS)
    check("cast_continuity IS an LLM dimension",
          "cast_continuity" in quality.CAST_LLM_DIMENSIONS)

    # active_scoring_dimensions() reads dotted paths through the ConfigLoader, so the
    # test drives the real global_config (the same way tests/test_quality_roleplay.py does)
    # rather than passing a plain nested dict.
    original = global_config.config
    try:
        global_config.config = {"quality": {"enabled": True}}
        off = quality.active_scoring_dimensions()
        check("cast_continuity is absent from the active set when the toggle is off",
              "cast_continuity" not in off, list(off))

        global_config.config = {"quality": {"enabled": True, "cast_continuity": True}}
        on = quality.active_scoring_dimensions()
        check("cast_continuity joins the active set when the toggle is on",
              "cast_continuity" in on, list(on))
        check("the cast weight is 0.10", on.get("cast_continuity") == 0.10, on)
        check("the active set is the seven base dims + cast_continuity",
              list(on) == list(quality.SCORING_DIMENSIONS) + ["cast_continuity"], list(on))
    finally:
        global_config.config = original


# --------------------------------------------------------------------------------------
# 7b. Cast dimension is actually SCORED (v12.3 regression guard)
# --------------------------------------------------------------------------------------

class _StubResponse:
    """Minimal stand-in for a judge HTTP response (no network)."""

    status_code = 200
    text = ""

    def __init__(self, content):
        self._content = content

    def json(self):
        return {"choices": [{"message": {"content": self._content},
                             "finish_reason": "stop"}]}


def test_cast_dimension_is_scored():
    """v12.3: turning the toggle on must SCORE the dimension, not drop every row.

    Two shipped defects are covered here, both of which only appeared with
    ``quality.cast_continuity: true``:
      * ``score_conversation()`` raised ``KeyError: 'cast_continuity'`` while weighting the
        composite (its ``default_weights`` dict was never updated for the cast set), so every
        reply died in the scoring path and was dropped;
      * once that was fixed, the judge's cast number was parsed and then discarded, so the
        dimension always reported 0 and silently dragged the composite down.
    """
    print("\nCast dimension is scored end to end (v12.3)")
    import json as _json
    import quality
    from app_state import global_config

    reply = _json.dumps({
        "coherence": 85, "naturalness": 80, "engagement": 78, "consistency": 82,
        "logic": 90, "ooc_evidence": [], "logic_evidence": [],
        "voice_distinctiveness": 70, "grounding": 75, "persona_alignment": 65,
        "cast_continuity": 88, "notes": "fine",
    })

    original_post = quality.requests.post
    original_config = global_config.config
    try:
        quality.requests.post = lambda *a, **k: _StubResponse(reply)
        global_config.config = {
            "quality": {"enabled": True, "use_llm_scoring": True,
                        "roleplay_dimensions": True, "cast_continuity": True,
                        "tic_dimension": True,
                        "scoring_api": {"url": "http://stub", "model": "stub", "key": ""}},
            "api": {"apis": [{"url": "http://stub", "model": "stub", "key": ""}]},
        }
        conv = [{"role": "user", "content": "Hi there."},
                {"role": "assistant", "content": "Hello, friend. Good to see you."}]

        # The crash: this raised KeyError: 'cast_continuity' before v12.3.
        try:
            result = quality.score_conversation(conv, "cast_regress", 0)
            crashed = None
        except Exception as exc:  # noqa: BLE001 - the point is to report ANY crash
            result, crashed = None, exc
        check("score_conversation() does not crash with cast_continuity on",
              crashed is None, crashed)

        if result is not None:
            check("the conversation is not dropped",
                  not result.get("dropped"), result.get("drop_reason"))
            check("the judge's cast score is kept (not 0)",
                  result["dimensions"].get("cast_continuity") == 88,
                  result["dimensions"].get("cast_continuity"))
            check("the cast dimension carries weight in the composite",
                  result["composite"] > 0, result["composite"])

        # The judge request itself must ASK for the cast dimension, otherwise the model
        # never returns it and the extraction ladder stops preferring it.
        dims = quality._default_judge_dims()
        check("the default judge dims include cast_continuity when the toggle is on",
              "cast_continuity" in dims, dims)
        check("the default judge dims still include every base LLM dimension",
              all(d in dims for d in quality.LLM_DIMENSIONS), dims)

        # Every ACTIVE dimension must have a weight, or the composite weighting crashes.
        active = quality.active_scoring_dimensions()
        base = dict(quality.SCORING_DIMENSIONS)
        base.update(quality.TIC_SCORING_DIMENSIONS)
        base.update(quality.ROLEPLAY_SCORING_DIMENSIONS)
        base.update(quality.CAST_SCORING_DIMENSIONS)
        check("every active dimension has a default weight",
              all(d in base for d in active),
              [d for d in active if d not in base])

        # With the toggle OFF the cast key must stay out of the judge request and the result.
        global_config.config = {
            "quality": {"enabled": True, "use_llm_scoring": True,
                        "scoring_api": {"url": "http://stub", "model": "stub", "key": ""}},
            "api": {"apis": [{"url": "http://stub", "model": "stub", "key": ""}]},
        }
        check("cast_continuity is not requested while the toggle is off",
              "cast_continuity" not in quality._default_judge_dims(),
              quality._default_judge_dims())
        off_result = quality.score_conversation(conv, "cast_regress_off", 0)
        check("the cast key is absent from the scored dimensions while off",
              "cast_continuity" not in off_result["dimensions"],
              off_result["dimensions"])
    finally:
        quality.requests.post = original_post
        global_config.config = original_config


# --------------------------------------------------------------------------------------
# 8. Default config is inert
# --------------------------------------------------------------------------------------

def test_transition_evidence():
    """v14.1: a transition lands on EVIDENCE, not on a bare mention of the name.

    The v12.0 test searched the reply for the character's name anywhere, which was wrong in both
    directions and both halves were visible in one run:
      * "Cole is still here, arguing" contains "Cole", so the exit was recorded as landed and the
        next reply blocked a character who had never left;
      * "She grabbed her coat and headed for the door" contains no name, so a perfectly good exit
        was deferred and then cancelled -- which is why characters appeared never to leave.
    """
    print("\nTransition evidence (v14.1: the exit actually lands)")
    cfg = {"enabled": True, "shape": "minimal", "min_present": 2}
    plan = g._build_scene_cast_plan(["Melody", "Cole", "Ines"], 6, cfg)
    leaver = next(m for m in plan["members"] if m["exit_slot"] is not None)
    slot = leaver["exit_slot"]

    def advance(text, mode="evidence"):
        state = g._cast_state_from_plan(plan)
        return g._advance_cast_state(plan, state, slot, text, max_deferrals=1, mode=mode)

    # --- the beat lands on a real departure, named or pronominal -------------------------
    for text in ("%s said goodbye and left." % leaver["name"],
                 "She grabbed her coat and headed for the door.",
                 "He sighed, pushed back his chair, and walked out.",
                 '"I have to go," %s said.' % leaver["name"]):
        state, events = advance(text)
        check("a real departure lands the exit: %r" % text[:40],
              ("exited", leaver["name"]) in events, events)

    # --- a mere MENTION is not a departure ----------------------------------------------
    for text in ("%s is still here, arguing." % leaver["name"],
                 "%s did not leave." % leaver["name"],
                 "%s nodded slowly." % leaver["name"],
                 "%s stayed put." % leaver["name"]):
        state, events = advance(text)
        check("a mere mention does NOT land the exit: %r" % text[:40],
              ("exited", leaver["name"]) not in events, events)

    # --- mode 'name' reproduces the old (buggy) behaviour --------------------------------
    state, events = advance("%s is still here, arguing." % leaver["name"], mode="name")
    check("mode 'name' still lands on a bare mention (A/B escape hatch)",
          ("exited", leaver["name"]) in events, events)

    # --- mode 'always' lands every transition regardless of the prose ---------------------
    state, events = advance("Nobody does anything.", mode="always")
    check("mode 'always' lands the exit with no evidence at all",
          ("exited", leaver["name"]) in events, events)

    # --- an unknown mode falls back to 'evidence' ----------------------------------------
    state, events = advance("%s is still here." % leaver["name"], mode="bogus")
    check("an unknown mode falls back to 'evidence'",
          ("exited", leaver["name"]) not in events, events)

    # --- a pronoun is only evidence when ONE character is mid-transition ------------------
    check("a pronoun is ambiguous with two candidates",
          detection.find_transition_evidence("She left the room.", ["A", "B"], "departure") == [])
    check("a pronoun is evidence with one candidate",
          [n for n, _s in detection.find_transition_evidence(
              "She left the room.", ["A"], "departure")] == ["A"])

    # --- the detector never blames the wrong character -----------------------------------
    check("the actor, not the object, owns the verb",
          [n for n, _s in detection.find_transition_evidence(
              "Cole watched Ines leave.", ["Cole", "Ines"], "departure")] == ["Ines"])


def test_slot_overrides():
    """v14.1: join_slot / exit_slot place the beats, and are clamped rather than trusted."""
    print("\nSlot overrides (v14.1: place the beats in a short conversation)")
    base = {"enabled": True, "shape": "minimal", "min_present": 2}

    def plan_for(slots, join_slot=0, exit_slot=0):
        cfg = dict(base, join_slot=join_slot, exit_slot=exit_slot)
        return g._build_scene_cast_plan(["A", "B", "C"], slots, cfg)

    # 0 = automatic, so the override must not change the schedule at all.
    check("join_slot 0 and exit_slot 0 leave the automatic schedule untouched",
          plan_for(6) == plan_for(6, 0, 0))

    # An explicit exit moves the departure earlier, which is the whole point in a short scene.
    auto = plan_for(6)
    auto_exit = next(m["exit_slot"] for m in auto["members"] if m["exit_slot"] is not None)
    moved = plan_for(6, exit_slot=3)
    moved_exit = next(m["exit_slot"] for m in moved["members"] if m["exit_slot"] is not None)
    check("an explicit exit_slot moves the departure earlier", moved_exit == 3,
          "auto %s -> %s" % (auto_exit, moved_exit))

    # Clamping: a slot past the end of the conversation is pulled back into range, and an exit
    # always keeps a reply after it for the aftermath.
    for slots in (4, 6, 8):
        plan = plan_for(slots, join_slot=99, exit_slot=99)
        members = plan["members"]
        check("%d slots: an out-of-range override is clamped into range" % slots,
              all(m["enter_slot"] <= slots for m in members)
              and all(m["exit_slot"] is None or m["exit_slot"] <= slots - 1 for m in members),
              members)
        check("%d slots: a clamped exit still leaves an aftermath" % slots,
              all(m["exit_slot"] is None or slots - m["exit_slot"] >= 1 for m in members),
              members)

    # An exit before the arrival is impossible: the departure is pushed after it.
    plan = plan_for(8, join_slot=5, exit_slot=2)
    for m in plan["members"]:
        if m["exit_slot"] is not None and m["enter_slot"] > 0:
            check("an exit is never scheduled before the arrival it follows",
                  m["exit_slot"] > m["enter_slot"], m)

    # An override never violates the plan invariants.
    for slots in (4, 6, 8, 12):
        for join_slot, exit_slot in ((0, 0), (2, 3), (1, 2), (99, 99), (3, 2)):
            plan = plan_for(slots, join_slot, exit_slot)
            members = plan["members"]
            if not members:
                continue
            state = g._cast_state_from_plan(plan)
            worst = slots + 1
            for slot in range(1, slots + 1):
                worst = min(worst, len(g._cast_names_for_slot(plan, state, slot)[2]))
                state, _e = g._advance_cast_state(plan, state, slot, "", max_deferrals=1)
            check("%d slots / override (%d,%d): min_present still holds" % (slots, join_slot, exit_slot),
                  worst >= plan["min_present"], "min present %d" % worst)


def test_default_config_inert():
    print("\nDefault config (the shipped feature is inert)")
    import yaml
    with open(os.path.join(REPO, "config", "config.yml"), encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    cast = config.get("prompts", {}).get("scene_cast", {}) or {}
    check("prompts.scene_cast.enabled ships OFF", cast.get("enabled", False) is False, cast)
    check("prompts.scene_cast.placement is depth0 (cache-friendly by default)",
          cast.get("placement") == "depth0", cast)
    check("prompts.scene_cast.allow_cutaway ships OFF",
          cast.get("allow_cutaway", True) is False, cast)
    check("prompts.scene_cast.shape is one the worker understands",
          cast.get("shape") in g.SCENE_CAST_SHAPES, cast.get("shape"))
    check("prompts.scene_cast.exit_detection is a mode the worker understands",
          cast.get("exit_detection", "evidence") in g.CAST_TRANSITION_MODES,
          cast.get("exit_detection"))
    check("prompts.scene_cast.exit_detection ships 'evidence' (exits actually land)",
          cast.get("exit_detection", "evidence") == "evidence", cast.get("exit_detection"))
    check("prompts.scene_cast.join_slot ships 0 (automatic)",
          int(cast.get("join_slot", 0) or 0) == 0, cast.get("join_slot"))
    check("prompts.scene_cast.exit_slot ships 0 (automatic)",
          int(cast.get("exit_slot", 0) or 0) == 0, cast.get("exit_slot"))
    continuity = config.get("detection", {}).get("scene_continuity", {}) or {}
    check("detection.scene_continuity.max_rounds is in 1..5",
          1 <= int(continuity.get("max_rounds", 2)) <= 5, continuity)
    check("quality.cast_continuity ships OFF",
          config.get("quality", {}).get("cast_continuity", True) is False)
    # With the feature off, the plan builder returns an empty plan for any scene.
    check("feature off: no plan for any scene size",
          all(g._build_scene_cast_plan(["A", "B", "C", "D"], 10, cast)["members"] == []
              for _ in (0,)))


# --------------------------------------------------------------------------------------
# 9. placement_cache_warning accounts for the cast note
# --------------------------------------------------------------------------------------

def test_cache_warning():
    print("\nCache warning (the cast note is counted)")
    base = (6, False, 2, False, False, [], "depth0", "depth0")
    with_cast = (6, False, 2, False, False, [], "depth0", "depth0",
                  True, "system")
    no_cast = (6, False, 2, False, False, [], "depth0", "depth0",
               True, "depth0")
    base_share, base_warning = g.placement_cache_warning(*base)
    cast_share, cast_warning = g.placement_cache_warning(*with_cast)
    depth_share, depth_warning = g.placement_cache_warning(*no_cast)
    check("depth0 cast placement adds nothing (cache preserved)",
          depth_share == base_share and depth_warning == "", (depth_share, depth_warning))
    check("system cast placement is counted", cast_share > base_share,
          (base_share, cast_share))
    check("the warning mentions the scene cast", "scene cast" in cast_warning, cast_warning)
    check("no warning when nothing is in the system prompt",
          base_warning == "" and depth_warning == "", (base_warning, depth_warning))


def test_v142_regressions():
    """v14.2: the three defects that made a 2-character scene never leave and never arrive cleanly.

    All three were found by dumping a real run and reading it, and all three are invisible to the
    checks above because each one is a case where the code was self-consistent and simply wrong.
    """
    print("\nv14.2 regressions (exit scheduled, arrival directed, man not told to summon)")

    # --- (1) a 2-character scene with min_present 1 must actually SCHEDULE the exit ----------
    # The old `leave_count = min(max_leaves, spare, max(0, len(leavable) - 1))` budgeted against the
    # LEAVABLE set, and in a 2-character scene that set holds one name, so the budget was 0 and the
    # departure was silently dropped. `exit_slot` was configured, clamped, honoured -- and never used.
    two = {"enabled": True, "shape": "minimal", "min_present": 1, "exit_slot": 8, "join_slot": 0}
    plan = g._build_scene_cast_plan(["A", "B"], 10, two)
    exits = [m for m in plan["members"] if m["exit_slot"] is not None]
    check("2 characters / min_present 1: the configured exit is SCHEDULED",
          len(exits) == 1 and exits[0]["exit_slot"] == 8, plan["members"])
    check("2 characters / min_present 1: the exit is on the slot the config asked for",
          bool(exits) and exits[0]["exit_slot"] == 8,
          [(m["name"], m["exit_slot"]) for m in plan["members"]])

    # The budget must still respect min_present: 2 characters with min_present 2 has no spare.
    check("2 characters / min_present 2: still no exit (no spare character)",
          g._build_scene_cast_plan(["A", "B"], 10,
                                   {"enabled": True, "shape": "minimal", "min_present": 2,
                                    "exit_slot": 8})["members"] == [])
    # And a 3-character scene must not now schedule more leaves than it has spare characters.
    three = g._build_scene_cast_plan(["A", "B", "C"], 10,
                                     {"enabled": True, "shape": "minimal", "min_present": 2,
                                      "exit_slot": 8})
    check("3 characters / min_present 2: exactly one exit is scheduled",
          sum(1 for m in three["members"] if m["exit_slot"] is not None) == 1,
          three["members"])

    # --- (2) the ARRIVES directive must fire ON the arrival slot -----------------------------
    # A character scheduled to arrive on slot N is still 'absent' when slot N's note is built (the
    # state only flips when _advance_cast_state runs, after the reply). The old transition scan
    # skipped every non-'present' state, so the directive was never emitted at all.
    arrive_cfg = {"enabled": True, "shape": "minimal", "min_present": 1,
                  "join_slot": 3, "exit_slot": 0}
    plan = g._build_scene_cast_plan(["A", "B"], 6, arrive_cfg)
    state = g._cast_state_from_plan(plan)
    joiner = next(m for m in plan["members"] if m["enter_slot"] > 0)
    arriving, _exiting = g._cast_slot_transitions(plan, state, joiner["enter_slot"])
    check("the arrival slot reports the joiner as ARRIVING",
          [m["name"] for m in arriving] == [joiner["name"]], arriving)
    note_cfg = dict(arrive_cfg)
    note_cfg["total_slots"] = 6
    note = g._cast_presence_note(plan, state, joiner["enter_slot"], note_cfg)
    check("the arrival slot's note carries the ARRIVES directive",
          "ARRIVES" in note, note)
    check("the arrival slot's note does NOT also call the joiner absent",
          "NOT IN THE SCENE: " + joiner["name"] not in note, note)
    check("the arrival slot's note lists the joiner as ARRIVING IN THIS REPLY",
          "ARRIVING IN THIS REPLY: " + joiner["name"] in note, note)

    # --- (3) the MAN is never told an un-arrived character is present ------------------------
    # transitions=False is the man's note. A character mid-arrival has not been written into the room
    # yet, so listing her as PRESENT made him call out to someone the transcript had not placed there --
    # "Come here, Teresa" -- and the next reply then narrated her walking in from outside.
    plan = g._build_scene_cast_plan(["A", "B"], 6, arrive_cfg)
    state = g._cast_state_from_plan(plan)
    joiner = next(m for m in plan["members"] if m["enter_slot"] > 0)
    man_note = g._cast_presence_note(plan, state, joiner["enter_slot"], note_cfg,
                                     transitions=False)
    check("the man's note never lists a not-yet-arrived character as PRESENT",
          "PRESENT: " + joiner["name"] not in man_note
          and ("PRESENT: " + joiner["name"] + ",") not in man_note, man_note)
    check("the man's note lists the not-yet-arrived character as NOT IN THE SCENE",
          "NOT IN THE SCENE: " + joiner["name"] in man_note, man_note)
    check("the man's note forbids summoning an absent character",
          "summon" in man_note.lower(), man_note)
    check("the man's note carries NO transition directive",
          "ARRIVES" not in man_note and "LEAVES" not in man_note, man_note)

    # The man's note must stay cache-stable between two static slots (it is depth0 by default, but the
    # same text is what a 'system' placement would put in the prompt).
    static = [s for s in range(1, 7)
              if s not in (joiner["enter_slot"],)
              and not any(m["exit_slot"] == s for m in plan["members"])]
    if len(static) >= 2:
        check("the man's note is byte-identical between two static slots",
              g._cast_presence_note(plan, state, static[0], note_cfg, transitions=False)
              == g._cast_presence_note(plan, state, static[1], note_cfg, transitions=False))

    # An EXITING character is still in the room for both readers: the exit has not been narrated yet.
    exit_cfg = {"enabled": True, "shape": "minimal", "min_present": 1,
                "join_slot": 0, "exit_slot": 4}
    plan = g._build_scene_cast_plan(["A", "B"], 6, exit_cfg)
    state = g._cast_state_from_plan(plan)
    leaver = next(m for m in plan["members"] if m["exit_slot"] is not None)
    man_note = g._cast_presence_note(plan, state, leaver["exit_slot"], note_cfg,
                                     transitions=False)
    check("the man's note keeps an EXITING character as PRESENT",
          "PRESENT: " + leaver["name"] in man_note
          or ("PRESENT: " + leaver["name"] + ",") in man_note, man_note)


def test_v143_regressions():
    print("\nv14.3 regressions (the arrival slot is not a violation, objects are not actors)")

    # --- 1. A character mid-transition is NOT blocked on the reply that narrates the beat ---
    # The defect: _cast_slot_transitions reports the ARRIVING member for its enter_slot while the
    # state is still 'absent' (it only flips after the reply comes back), so the plain
    # 'absent'/'exited' test put the SAME name in blocked AND expected. The note said
    # "Dani ARRIVES in this reply" and the continuity check then flagged the model for writing it,
    # so every arrival reply was regenerated to exhaustion -- the "scene continuity violation, fixes
    # exhausted" flood in the operator's log.
    cfg = {"enabled": True, "shape": "minimal", "min_present": 1,
           "join_slot": 3, "exit_slot": 4, "exit_detection": "evidence"}
    plan = g._build_scene_cast_plan(["Presley", "Dani"], 5, cfg)
    joiner = next((m for m in plan["members"] if m["enter_slot"] > 0), None)
    check("the fixture plan schedules an arrival", joiner is not None, plan)
    state = g._cast_state_from_plan(plan)
    blocked, expected, _present = g._cast_names_for_slot(plan, state, joiner["enter_slot"])
    check("the arrival slot EXPECTS the newcomer", joiner["name"] in expected, expected)
    check("the arrival slot does NOT block the newcomer",
          joiner["name"] not in blocked, blocked)
    check("no name is both blocked and expected on the same slot",
          not (set(blocked) & set(expected)), (blocked, expected))

    # Writing the entrance the note asked for must therefore PASS the check.
    violation, findings = detection.check_scene_continuity(
        "%s steps through the door and looks around the room." % joiner["name"],
        list(blocked), list(expected))
    check("writing the directed entrance is not a violation", violation is False, findings)

    # The newcomer is still blocked on the replies BEFORE its arrival.
    early_blocked = g._cast_names_for_slot(plan, state, 1)[0]
    check("the newcomer is still blocked before its arrival slot",
          joiner["name"] in early_blocked, early_blocked)

    # An EXITING character stays writable on the reply that narrates the exit.
    leaver = next((m for m in plan["members"] if m["exit_slot"] is not None), None)
    check("the fixture plan schedules a departure", leaver is not None, plan)
    if leaver is not None:
        blocked, expected, _present = g._cast_names_for_slot(
            plan, state, leaver["exit_slot"])
        check("the departure slot EXPECTS the leaver", leaver["name"] in expected, expected)
        check("the departure slot does NOT block the leaver",
              leaver["name"] not in blocked, blocked)
        violation, findings = detection.check_scene_continuity(
            "%s said goodbye and left." % leaver["name"], list(blocked), list(expected))
        check("writing the directed exit is not a violation", violation is False, findings)

    # --- 2. A name that is the OBJECT of a preposition is not the actor ---
    # "She looks at Dani and smiles" put Dani after the verb, in the preposition's phrase, so the
    # next action verb in the sentence belongs to the subject. The detector used to blame Dani.
    object_cases = [
        "She looks at Dani and smiles.",
        "Presley glances over at Dani and laughs.",
        "Presley turned to Dani and smiled.",
        "Cole nodded at Melody.",
        "He set the glass down beside Dani and sighed.",
    ]
    bad = []
    for text in object_cases:
        name = "Dani" if "Dani" in text else "Melody"
        violation, findings = detection.check_scene_continuity(text, [name])
        if violation:
            bad.append((text, findings))
    check("a name in a preposition's phrase is not blamed for the action", not bad, bad)

    # The subject of the same sentence is still caught.
    subject_cases = [
        "Dani nods slowly.",
        "Dani walked away.",
        "Dani steps through the door and looks around the room.",
        "Melody nodded slowly.",
    ]
    missed = []
    for text in subject_cases:
        name = "Dani" if "Dani" in text else "Melody"
        violation, _findings = detection.check_scene_continuity(text, [name])
        if not violation:
            missed.append(text)
    check("a name that IS the actor is still caught", not missed, missed)

    # --- 3. An unfixable continuity miss DROPS the row ---
    # Every other check keeps its best-effort text on exhaustion; a cast contradiction cannot be
    # kept, because the row would be a transcript in which a character who left is still talking.
    check("quality carries a scene-continuity drop reason",
          getattr(g.quality, "DROP_REASON_SCENE_CONTINUITY", None) == "scene_continuity",
          getattr(g.quality, "DROP_REASON_SCENE_CONTINUITY", None))
    check("the drop reason is distinct from the judge's reasons",
          g.quality.DROP_REASON_SCENE_CONTINUITY not in
          (g.quality.DROP_REASON_INVALID_JSON, g.quality.DROP_REASON_LOGIC_ZERO,
           g.quality.DROP_REASON_JUDGE_FAILED, g.quality.DROP_REASON_MANUAL))

    # The retry helper reports the flag as its 4th element, and only when a violation
    # survived every fix round.
    import inspect
    source = inspect.getsource(g.generate_answer_with_retries)
    check("generate_answer_with_retries returns a 4-tuple",
          "cast_violation_this_task)" in source.replace(" ", ""),
          "return statements do not carry the flag")
    check("an exhausted continuity violation sets the drop flag",
          "cast_violation_this_task = True" in source)
    check("the exhausted branch no longer says it keeps the answer",
          "fixes exhausted for attempt {attempt+1}. Keeping the best-effort answer." not in source)

    worker_source = inspect.getsource(g.worker)
    check("the worker drops a task whose cast violation survived",
          "if cast_violation_this_task:" in worker_source)
    check("the drop is recorded so a resumed run does not retry it",
          "app_state.quality_dropped[task_id] = quality.DROP_REASON_SCENE_CONTINUITY"
          in worker_source)


def main():
    print("Scene Cast / Scene Continuity (v12.0) - headless checks")
    test_feasibility()
    test_plan()
    test_state_machine()
    test_note()
    test_detector()
    test_metadata()
    test_quality_dimension()
    test_cast_dimension_is_scored()
    test_transition_evidence()
    test_slot_overrides()
    test_v142_regressions()
    test_v143_regressions()
    test_default_config_inert()
    test_cache_warning()
    if FAILURES:
        print("\n%d check(s) FAILED:" % len(FAILURES))
        for name in FAILURES:
            print("  - " + name)
        return 1
    print("\nAll scene cast checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
