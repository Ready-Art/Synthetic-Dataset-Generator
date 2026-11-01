# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the two v12.5 defects.

1. ``{subject}`` in the ANSWER prompt was never substituted. Every other authored prompt
   (question, continuation, greeting, first line) fills its native variables at its formatting
   boundary; the answer prompt had no such boundary, so a prompt that wrote ``{subject}`` sent
   the model the literal five characters. ``generation.fill_answer_prompt()`` is that boundary.

2. ``generation.history_size`` only prunes ``app_state.question_history`` -- the recent OPENING
   QUESTIONS fed into the question prompt. Nothing compared an assistant reply to the earlier
   assistant replies in the same conversation, so a model that opens every reply the same way was
   invisible to every check and the tic compounded down the conversation. The opt-in
   ``repeat_opener_guard`` compares the reply against the conversation and retries the opener.

No model, network or display is needed: ``requests.post`` is stubbed.

Run with ``python -B tests/test_repeat_opener_and_answer_vars.py``.
"""
import json
import os
import sys
import tempfile
import types
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
# Import generation in a scratch directory so its debug-log / config side effects stay out of
# the repository (the real app runs from the repo root).
_tmpdir = tempfile.TemporaryDirectory(prefix="repeat_opener_")
os.chdir(_tmpdir.name)
sys.path.insert(0, str(REPO))

import detection
import app_state
import generation


FAILURES = []


def check(name, condition):
    if condition:
        print("PASS: %s" % name)
    else:
        print("FAIL: %s" % name)
        FAILURES.append(name)


# --- Bug 1: the answer prompt's native variables --------------------------------------------

def test_fill_answer_prompt():
    # The exact shape from the operator's custom prompts: the name never reached the model.
    filled = generation.fill_answer_prompt(
        "You are writing the reply of the woman, {subject}, in an intense roleplay with a man.",
        "Melody", "The bridge was closed for repairs.")
    check("{subject} is substituted with the task's subject chunk",
          filled == "You are writing the reply of the woman, Melody, in an intense roleplay with a man.")
    check("no literal {subject} survives",
          "{subject}" not in filled)

    # {context} is the sibling variable of the same passage and is filled too.
    both = generation.fill_answer_prompt(
        "Subject: {subject}\nContext: {context}", "A", "B")
    check("{subject} and {context} are both substituted", both == "Subject: A\nContext: B")

    # An empty chunk becomes N/A rather than leaving a dangling clause (same as the question path).
    empty = generation.fill_answer_prompt("... of the woman, {subject}.", "", "")
    check("an empty chunk fills as N/A, not as nothing",
          empty == "... of the woman, N/A.")

    # THE DEFAULT-RUN GUARANTEE: the shipped answer prompt has no variables, so it must come back
    # byte-identical. Without this, the fix would change every existing dataset.
    shipped = ("Write your next message in the roleplay. Write only the message itself.\n\n"
               "CHARACTER INFORMATION: the CHARACTER PROFILE block gives her Gender, Age, Job, "
               "Appearance, Clothing, Jewelry, Personality, Traits and Backstory.")
    check("a prompt with no variables is returned byte-identical",
          generation.fill_answer_prompt(shipped, "Melody", "chunk") == shipped)
    check("an empty template stays empty",
          generation.fill_answer_prompt("", "Melody", "chunk") == "")

    # Dataset values keep their single-pass guarantee: an authored {{ds:alias}} is still expanded
    # and a native token inside the inserted data stays literal.
    with_ds = generation.fill_answer_prompt(
        "Name {{ds:name}} about {subject}", "A", "B", {"name": "Melody"})
    check("a dataset alias is rendered alongside the native variables",
          with_ds == "Name Melody about A")


# --- Bug 2: the repeated-opener detector -----------------------------------------------------

def test_opener_tokens():
    check("stopwords and one-letter words are dropped from the opener",
          detection.opener_tokens("She lets out a breath and turns away")
          == ("lets", "out", "breath", "turns"))
    check("an empty string yields no tokens", detection.opener_tokens("") == ())
    check("None yields no tokens", detection.opener_tokens(None) == ())


def test_repeated_opener_detector():
    earlier = ["She lets out a breath she did not know she was holding."]

    # THE REPORTED SHAPE: the same sentence frame with only the object swapped. A strict
    # three-word match misses every one of these, which is why the rule accepts two shared words
    # when the openers then diverge.
    for text in ["She lets out a laugh, short and sharp.",
                 "She lets out a breath, steadier this time.",
                 "She lets out a sigh and turns away."]:
        found, info = detection.is_repeated_opener(text, earlier)
        check("same frame with a swapped object is caught: %r" % text, found and len(info) == 1)
        check("the hit is labelled: %r" % text,
              found and info[0][0].startswith("Repeated opener"))

    # An identical opener is obviously caught too.
    found, _ = detection.is_repeated_opener("She lets out a breath, slowly.", earlier)
    check("an identical opener is caught", found)

    # Replies that open differently must NOT be touched: this is the guard's whole cost.
    for text in ["She sets the cup down and turns to the window.",
                 "Her hands find the edge of the table.",
                 "She lets the silence sit there.",
                 "The window rattles in its frame.",
                 "\"You are late,\" she says.",
                 ""]:
        found, info = detection.is_repeated_opener(text, earlier)
        check("a different opener is left alone: %r" % text, not found and info == [])

    # A first assistant reply can never hit: there is nothing to repeat yet.
    check("no earlier replies means no hit",
          detection.is_repeated_opener("She lets out a breath.", []) == (False, []))
    check("None earlier replies means no hit",
          detection.is_repeated_opener("She lets out a breath.", None) == (False, []))
    check("an empty reply never hits",
          detection.is_repeated_opener("", earlier) == (False, []))
    # Too few content words to call anything repetition: staying quiet only suppresses a retry.
    check("a very short opener never hits",
          detection.is_repeated_opener("She lets out.", earlier) == (False, []))
    # A hit against ANY earlier reply, not just the most recent one.
    check("an earlier-but-not-immediately-previous match still hits",
          detection.is_repeated_opener("She lets out a laugh.",
                                       ["She lets out a breath.", "She crosses the room."])[0])


# --- Bug 2: the retry pipeline ---------------------------------------------------------------

def _load_generation_with_stubs():
    """Import-time stubs so the answer loop can run without GUI, Valkey or a network."""
    app_state.stop_processing = False
    app_state.pause_processing = False
    app_state.total_input_tokens = 0
    app_state.total_output_tokens = 0
    app_state.total_attempts_global = 0
    for key in ("user_speaking_counts_per_api", "refusal_counts_per_api", "slop_counts_per_api",
                "anti_slop_counts_per_api", "ooc_counts_per_api", "error_counts_per_api"):
        setattr(app_state, key, {i: 0 for i in range(6)})
    for key in ("recent_user_speaking_total", "recent_refusals_total", "recent_slop_total",
                "recent_anti_slop_total", "recent_ooc_total", "recent_errors_total"):
        setattr(app_state, key, [])
    generation.check_circuit_breaker = lambda _slot: True
    generation.record_api_success = lambda _slot: None
    generation.record_api_failure = lambda _slot: None
    generation.update_live_prompt_preview = lambda *a, **k: None
    generation.apply_bad_words_to_payload = lambda payload, _t: payload
    generation.sanitize_payload_for_endpoint = lambda payload, _u, _s: payload
    generation.get_cached_response = lambda _hash, _slot: (None, False)
    generation.set_cached_response = lambda *a, **k: None
    generation.global_rate_limiter = types.SimpleNamespace(wait_if_needed=lambda _slot: None)
    os.makedirs(os.path.join(_tmpdir.name, "output"), exist_ok=True)
    app_state.master_duplication_enabled_var = types.SimpleNamespace(get=lambda: False)
    return generation


class _Response:
    def __init__(self, status_code, content):
        self.status_code = status_code
        self._content = content
        self.text = content

    def json(self):
        return {"choices": [{"message": {"content": self._content}}],
                "usage": {"prompt_tokens": 5, "completion_tokens": 5}}


EARLIER = "She lets out a breath she did not know she was holding."


def _call_generate(generation, replies, enabled=True, earlier=(EARLIER,), max_retries=1,
                   fixes=(), answer_prompt="Continue the scene."):
    """Drive generate_answer_with_retries with a scripted list of fixture replies."""
    calls = []

    def post(url, headers=None, data=None, timeout=None):
        calls.append(json.loads(data))
        return _Response(200, replies[min(len(calls) - 1, len(replies) - 1)])

    generation.requests.post = post
    history = []
    for text in earlier:
        history.append({"role": "user", "content": "She waits."})
        history.append({"role": "assistant", "content": text})
    history.append({"role": "user", "content": "What now?"})
    result = generation.generate_answer_with_retries(
        base_system_prompt="You are a helpful roleplay assistant.",
        conversation_history_for_llm=history,
        answer_prompt_template=answer_prompt,
        thread_id=0, q=None,
        sampler_settings_local={},
        api_url_local="http://fixture/v1/chat/completions",
        model_name_local="model-x", api_key_local="",
        refusal_phrases_local=[], user_speaking_phrases_local=[], slop_phrases_local=[],
        current_anti_slop_phrases_param=[],
        jailbreaks_local=[], speaking_fixes_local=[], slop_fixes_fallback_local=[],
        max_attempts_local=3,
        slop_fixer_api_config_param={"url": ""},
        current_slop_fixes_for_rotation_param=[],
        api_slot_idx=0,
        current_max_attempts_for_slop_fixer_call=2,
        anti_slop_fixer_api_config_param={"url": ""},
        master_duplication_enabled_local=False,
        no_user_impersonation_local=True,
        api_request_timeout_param=5,
        file_name="test.txt",
        repeat_opener_enabled_local=enabled,
        repeat_opener_fixes_local=fixes,
        repeat_opener_max_retries=max_retries,
        previous_assistant_replies_local=earlier,
    )
    return result, calls


def test_repeat_opener_pipeline():
    generation = _load_generation_with_stubs()

    # OFF (default): a repeated opener is written as-is, exactly one main-API call.
    result, calls = _call_generate(generation, ["She lets out a laugh, sharp and short."], enabled=False)
    check("off: the repeated opener is written as-is (no retry)",
          result[0] == "She lets out a laugh, sharp and short.")
    check("off: exactly one main-API request", len(calls) == 1)

    # ON: the repeated opener is sent back to the SAME main API; the fresh opener is kept.
    result, calls = _call_generate(
        generation, ["She lets out a laugh, sharp and short.",
                     "The window rattles in its frame."], enabled=True)
    check("on: the retried (fresh-opener) reply is what gets written",
          result[0] == "The window rattles in its frame.")
    check("on: the answer was sent back to the main API for a retry", len(calls) == 2)
    check("on: the retry went to the same model/slot",
          calls[1]["model"] == calls[0]["model"])

    # THE CACHE CONTRACT: the instruction goes on the ANSWER PROMPT (the last user message), so the
    # system prompt AND the whole history stay byte-identical and a prefix-caching server still
    # serves them. Appending to the system prompt would invalidate the cache on every retry.
    check("on: the system prompt is unchanged between the two requests",
          calls[0]["messages"][0]["content"] == calls[1]["messages"][0]["content"]
          == "You are a helpful roleplay assistant.")
    check("on: the conversation history is unchanged between the two requests",
          calls[0]["messages"][1:-1] == calls[1]["messages"][1:-1])
    check("on: the instruction is appended to the answer prompt (the final user message)",
          calls[0]["messages"][-1]["content"] == "Continue the scene."
          and calls[1]["messages"][-1]["content"].startswith("Continue the scene.")
          and calls[1]["messages"][-1]["content"] != "Continue the scene.")
    check("on: the default fix instruction is used when no list is configured",
          generation.DEFAULT_REPEAT_OPENER_FIX in calls[1]["messages"][-1]["content"])

    # A configured fix list is used instead of the default.
    result, calls = _call_generate(
        generation, ["She lets out a laugh.", "The window rattles."],
        enabled=True, fixes=("Open on something else entirely.",))
    check("on: a configured fix is used",
          "Open on something else entirely." in calls[1]["messages"][-1]["content"])

    # ON but the retry repeats again: bounded, best effort kept, never dropped.
    result, calls = _call_generate(
        generation, ["She lets out a laugh.", "She lets out a sigh."], enabled=True)
    check("on: a still-repeating retry is kept rather than dropped",
          result[0] == "She lets out a sigh.")
    check("on: the retry is bounded (no loop)", len(calls) == 2)

    # max_retries = 0 keeps the detection but never retries.
    result, calls = _call_generate(
        generation, ["She lets out a laugh."], enabled=True, max_retries=0)
    check("max_retries 0: no retry at all",
          result[0] == "She lets out a laugh." and len(calls) == 1)

    # A fresh opener needs no extra request, and a first reply can never trigger the guard.
    result, calls = _call_generate(generation, ["The window rattles in its frame."], enabled=True)
    check("on: a fresh opener needs no retry",
          result[0] == "The window rattles in its frame." and len(calls) == 1)
    result, calls = _call_generate(
        generation, ["She lets out a laugh."], enabled=True, earlier=())
    check("on: the first assistant reply never triggers the guard",
          result[0] == "She lets out a laugh." and len(calls) == 1)


def test_answer_prompt_reaches_the_model_filled():
    """End to end: the literal {subject} must never reach the wire."""
    generation = _load_generation_with_stubs()
    # The worker fills the prompt before calling this function, so drive the helper the same way
    # and confirm the request body carries the name rather than the token.
    prompt = generation.fill_answer_prompt(
        "You are writing the reply of the woman, {subject}, in an intense roleplay.",
        "Melody", "The bridge was closed.")
    result, calls = _call_generate(
        generation, ["She sets the cup down."], enabled=False, answer_prompt=prompt)
    sent = calls[0]["messages"][-1]["content"]
    check("the answer request carries the name, not the token",
          "Melody" in sent and "{subject}" not in sent)


def main():
    test_fill_answer_prompt()
    test_opener_tokens()
    test_repeated_opener_detector()
    test_repeat_opener_pipeline()
    test_answer_prompt_reaches_the_model_filled()
    if FAILURES:
        print("\n%d FAILED check(s):" % len(FAILURES))
        for name in FAILURES:
            print("  - %s" % name)
        return 1
    print("\nAll repeat-opener / answer-variable checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
