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
                         (["A", "B", "C", "D", "E"], 10), (["A", "B", "C"], 4)):
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

        # min_present must hold at every slot
        state = g._cast_state_from_plan(plan)
        worst = slots + 1
        for slot in range(1, slots + 1):
            present = g._cast_names_for_slot(plan, state, slot)[2]
            worst = min(worst, len(present))
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
    check("churn: never fewer than min_present present", worst >= plan["min_present"],
          "min present %d < %d" % (worst, plan["min_present"]))

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
# 8. Default config is inert
# --------------------------------------------------------------------------------------

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


def main():
    print("Scene Cast / Scene Continuity (v12.0) - headless checks")
    test_feasibility()
    test_plan()
    test_state_machine()
    test_note()
    test_detector()
    test_metadata()
    test_quality_dimension()
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
