# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Headless tests for the first-reply opener caps and the repeat-guard wiring fix.

1. A run's FIRST replies can collapse onto one opening (a reported dataset had 65% of first replies
   opening "A sharp intake of" and 87% opening "A"/"An"), and no check could see it: the repeated-opener
   and repeated-reply guards compare a reply with THIS conversation's earlier replies, and a first reply
   has none. ``generation.first_reply_first_word_cap`` / ``first_reply_opener_cap`` cap the share of the
   run's first replies one first word / one opening frame may hold, and retry a first reply over the
   cap with the opening it used named on the answer prompt.

2. The single-API worker path (the default, non-duplication mode) never passed the repeated-opener or
   repeated-reply guard settings to ``generate_answer_with_retries()``, so both guards were silently off
   there whatever the config said; the duplication path's logic-gate rewrite dropped them the same way.

The real worker runs against the fake server from ``test_opening_mode.py``. No model, network or display
is needed.

Run with ``python -B tests/test_first_reply_opener.py``.
"""
import ast
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_opening_mode as harness  # noqa: E402  (changes into a scratch copy of config/)

g = harness.g
app_state = harness.app_state
detection = harness.detection
check = harness.check
ROUTER = harness.ROUTER

COLLAPSED = "A sharp intake of breath. \"You're late,\" Ines says."
FRESH = "Ines folds her arms against the wind. \"You're late.\""
VARIED = ["Rain needles the deck.", "Ines laughs once.", "\"Get in,\" she says.", "The rope creaks.",
          "Lamplight swings over the water.", "Her hand finds the rail.", "Somewhere a bell rings."]


def seed_tally(collapsed=13, varied=7):
    """Record a run that has already written *collapsed* "A sharp intake" first replies."""
    app_state.reset_first_reply_openers()
    for _ in range(collapsed):
        app_state.record_first_reply_opener(*detection.opener_keys(COLLAPSED))
    for i in range(varied):
        app_state.record_first_reply_opener(*detection.opener_keys(VARIED[i % len(VARIED)]))


def answer_bodies():
    return ROUTER.bodies("answer")


# --- Detector -----------------------------------------------------------------------------------

def test_opener_keys():
    check("keys: article kept as first word, content words as frame",
          detection.opener_keys(COLLAPSED) == ("a", "sharp intake"))
    check("keys: leading markup and quote marks are skipped",
          detection.opener_keys("*An audible sigh.* \"Fine.\"") == ("an", "audible sigh"))
    check("keys: empty text gives empty keys", detection.opener_keys("") == ("", ""))
    check("snippet: the opening words as written",
          detection.opener_snippet(COLLAPSED) == "A sharp intake of breath.")


def test_is_overused_opener():
    words, frames = {"a": 13, "ines": 2}, {"sharp intake": 13}
    hit, info = detection.is_overused_opener(COLLAPSED, words, frames, 20, first_word_cap=0.3, frame_cap=0.05)
    check("detector: an opening over both caps hits", hit and "sharp intake" in info[0][0] and "'a'" in info[0][0])
    check("detector: the snippet is the reply's opening", info and info[0][1] == "A sharp intake of breath.")
    hit, _ = detection.is_overused_opener(COLLAPSED, words, frames, 19, first_word_cap=0.3, frame_cap=0.05)
    check("detector: nothing hits before the minimum sample", not hit)
    hit, _ = detection.is_overused_opener(COLLAPSED, words, frames, 20)
    check("detector: both caps 0 means off", not hit)
    hit, info = detection.is_overused_opener(COLLAPSED, words, frames, 20, frame_cap=0.05)
    check("detector: the frame cap alone hits", hit and "'a'" not in info[0][0])
    hit, _ = detection.is_overused_opener(FRESH, words, frames, 20, first_word_cap=0.3, frame_cap=0.05)
    check("detector: a fresh opening does not hit", not hit)
    hit, _ = detection.is_overused_opener(COLLAPSED, {"a": 6}, {"sharp intake": 1}, 20, first_word_cap=0.3)
    check("detector: a share exactly AT the cap does not hit (6/20 = 0.30)", not hit)


# --- Through the real worker ----------------------------------------------------------------------

CAPS = dict(first_reply_first_word_cap_local=0.3, first_reply_opener_cap_local=0.05)


def test_cap_retries_a_collapsed_first_reply():
    harness.reset_state()
    seed_tally()
    ROUTER.script["answer"] = [COLLAPSED, FRESH]
    harness.run_worker(harness.chunk_task(), **CAPS)
    bodies = answer_bodies()
    check("cap: the collapsed first reply is sent back once", len(bodies) == 2)
    if len(bodies) == 2:
        first, retry = bodies
        check("cap: the system prompt is byte-identical on the retry (prefix cache survives)",
              first["messages"][0] == retry["messages"][0])
        check("cap: the history is byte-identical on the retry",
              first["messages"][1:-1] == retry["messages"][1:-1])
        check("cap: the instruction rides on the answer prompt and names the opening",
              retry["messages"][-1]["content"].startswith(first["messages"][-1]["content"])
              and "\"A sharp intake of breath.\"" in retry["messages"][-1]["content"])
    rows = harness.read_rows()
    replies = [m["value"] for m in rows[0]["conversations"] if m["from"] == "gpt"] if rows else []
    check("cap: the fresh reply is what gets written", replies == [FRESH])
    words, frames, total = app_state.first_reply_opener_snapshot()
    check("cap: the kept reply is recorded, the rejected one is not",
          total == 21 and frames.get("sharp intake") == 13 and frames.get("ines folds") == 1)


def test_cap_keeps_the_best_effort_reply():
    harness.reset_state()
    seed_tally()
    ROUTER.script["answer"] = [COLLAPSED]
    harness.run_worker(harness.chunk_task(), **CAPS)
    check("exhausted: one request plus the default two retries", len(answer_bodies()) == 3)
    rows = harness.read_rows()
    check("exhausted: the row is written with the best-effort reply, never dropped",
          rows and [m["value"] for m in rows[0]["conversations"] if m["from"] == "gpt"] == [COLLAPSED])
    check("exhausted: the kept reply is recorded",
          app_state.first_reply_opener_snapshot()[1].get("sharp intake") == 14)

    harness.reset_state()
    seed_tally()
    harness.run_worker(harness.chunk_task(), first_reply_opener_max_retries=0, **CAPS)
    check("max_retries 0: detection without a retry", len(answer_bodies()) == 1)


def test_second_retry_names_every_rejected_opening():
    """Naming only the latest opening let Gemma 4 31B swing back to the first one on retry 2."""
    harness.reset_state()
    seed_tally()
    ROUTER.script["answer"] = [COLLAPSED, "A finger twitches against the strap.", FRESH]
    harness.run_worker(harness.chunk_task(), **CAPS)
    bodies = answer_bodies()
    check("accumulate: two retries were needed", len(bodies) == 3)
    if len(bodies) == 3:
        last = bodies[2]["messages"][-1]["content"]
        check("accumulate: retry 2 names both rejected openings",
              '"A sharp intake of breath."' in last and '"A finger twitches against the"' in last)
        check("accumulate: retry 2 names the first word to avoid", 'do not begin with "A"' in last)
    check("accumulate: a repeated opening is named once",
          g.first_reply_opener_fix(["A b c.", "A b c."]).count('"A b c."') == 1)


def test_cap_is_off_by_default():
    harness.reset_state()
    seed_tally()
    ROUTER.script["answer"] = [COLLAPSED, FRESH]
    harness.run_worker(harness.chunk_task())
    check("default: no retry", len(answer_bodies()) == 1)
    check("default: the tally is not touched", app_state.first_reply_opener_snapshot()[2] == 20)


def test_cap_judges_only_the_first_reply():
    harness.reset_state()
    seed_tally()
    ROUTER.script["answer"] = [FRESH, COLLAPSED]
    harness.run_worker(harness.chunk_task(), current_num_turns=2, **CAPS)
    check("turn 2: a collapsed later reply is not judged by the run-wide cap", len(answer_bodies()) == 2)
    check("turn 2: only the first reply is recorded", app_state.first_reply_opener_snapshot()[2] == 21)


def test_logic_gate_rewrite_is_recorded_once():
    """A first reply the per-turn logic gate rewrites counts once, as the rewrite."""
    harness.reset_state()
    app_state.reset_first_reply_openers()
    rewrite = "Rain needles the deck as Ines unhooks the lantern. \"You're late.\""
    ROUTER.script["answer"] = [COLLAPSED, rewrite]
    verdicts = [{"success": True, "failed": True, "score": 1, "evidence": ["contradiction"]},
                {"success": True, "failed": False, "score": 9}]
    real_check, real_build = g.quality.check_logic, g.quality.build_logic_rewrite_instruction
    g.quality.check_logic = lambda *a, **k: verdicts.pop(0) if verdicts else {"success": True, "failed": False}
    g.quality.build_logic_rewrite_instruction = lambda *a, **k: "Fix the contradiction."
    try:
        harness.run_worker(harness.chunk_task(), per_turn_logic_check_local=True,
                           per_turn_logic_max_rewrites_local=1, **CAPS)
    finally:
        g.quality.check_logic, g.quality.build_logic_rewrite_instruction = real_check, real_build
    check("gate: the turn-0 reply was regenerated once", len(answer_bodies()) == 2)
    _, frames, total = app_state.first_reply_opener_snapshot()
    check("gate: the first reply is recorded once, as the rewrite",
          total == 1 and "sharp intake" not in frames and frames.get("rain needles") == 1)


def test_single_api_path_runs_the_repeat_guard():
    """Regression: the default (non-duplication) path used to drop the guard settings."""
    harness.reset_state()
    app_state.reset_first_reply_openers()
    ROUTER.script["answer"] = ["She lets out a breath.", "She lets out a laugh.", "The window rattles."]
    harness.run_worker(harness.chunk_task(), current_num_turns=2, repeat_opener_enabled_local=True)
    check("single API: the repeated opener on turn 2 is retried", len(answer_bodies()) == 3)
    rows = harness.read_rows()
    replies = [m["value"] for m in rows[0]["conversations"] if m["from"] == "gpt"] if rows else []
    check("single API: the fresh opener is written", replies == ["She lets out a breath.", "The window rattles."])


def test_every_answer_call_passes_the_guards():
    """Each worker call that can produce an answer must carry every guard's settings."""
    tree = ast.parse(open(os.path.join(harness.REPO, "generation.py"), encoding="utf-8").read())
    worker = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "worker")
    wanted = {"repeat_opener_enabled_local", "repeat_opener_fixes_local", "repeat_opener_max_retries",
              "repeat_reply_enabled_local", "repeat_reply_fixes_local", "repeat_reply_max_retries",
              "previous_assistant_replies_local", "first_reply_first_word_cap_local",
              "first_reply_opener_cap_local", "first_reply_opener_max_retries"}
    calls = [node for node in ast.walk(worker) if isinstance(node, ast.Call)
             and getattr(node.func, "id", "") in ("generate_answer_with_retries", "_per_turn_logic_gate")]
    check("wiring: the worker has four answer call sites", len(calls) == 4)
    for call in calls:
        missing = wanted - {kw.arg for kw in call.keywords}
        check(f"wiring: {call.func.id} at line {call.lineno} passes every guard setting"
              + (f" (missing {sorted(missing)})" if missing else ""), not missing)


def main():
    test_opener_keys()
    test_is_overused_opener()
    test_cap_retries_a_collapsed_first_reply()
    test_cap_keeps_the_best_effort_reply()
    test_second_retry_names_every_rejected_opening()
    test_cap_is_off_by_default()
    test_cap_judges_only_the_first_reply()
    test_logic_gate_rewrite_is_recorded_once()
    test_single_api_path_runs_the_repeat_guard()
    test_every_answer_call_passes_the_guards()
    if harness.FAILURES:
        print(f"\n{len(harness.FAILURES)} first-reply opener check(s) failed")
        return 1
    print("\nAll first-reply opener checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
