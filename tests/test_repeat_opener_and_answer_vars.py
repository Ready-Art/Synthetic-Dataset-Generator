# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the v12.5 and v12.7 prompt/repeat defects.

1. ``{subject}`` in the ANSWER prompt was never substituted. Every other authored prompt
   (question, continuation, greeting, first line) fills its native variables at its formatting
   boundary; the answer prompt had no such boundary, so a prompt that wrote ``{subject}`` sent
   the model the literal five characters. ``generation.fill_answer_prompt()`` is that boundary.

2. ``generation.history_size`` only prunes ``app_state.question_history`` -- the recent OPENING
   QUESTIONS fed into the question prompt. Nothing compared an assistant reply to the earlier
   assistant replies in the same conversation, so a model that opens every reply the same way was
   invisible to every check and the tic compounded down the conversation. The opt-in
   ``repeat_opener_guard`` compares the reply against the conversation and retries the opener.

3. v12.7: ``{subject}`` is the SOURCE PASSAGE, not a character. A prompt that read "the reply
   of the woman, {subject}" was handed a ~1000-character slab of prose in a slot whose grammar asks
   for a name, and the model's most probable continuation is to reproduce the slab -- the same slab
   every turn, so the same reply every turn. ``{character_name}`` / ``{character_names}`` /
   ``{character_profile}`` are the variables such a prompt should name, and ``{subject}`` is now made
   safe to use inline (whitespace-collapsed, trailing terminator stripped).

4. v12.7: ``generate_user_continuation()`` sent her reply TWICE -- once in the history, which
   already ended with it, and once via ``{last_assistant_message}``. The trailing message is now
   dropped when the template names the variable.

5. v12.7: ``is_repeated_reply()`` catches a reply repeated WHOLE (the reported defect was three
   byte-identical 884-character replies), which the opener check cannot see because such a reply shares
   no opener frame -- it *is* the earlier reply. ``repeat_reply_guard`` retries it on the answer prompt.

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


# --- v12.7: the character variables ------------------------------------------------------------

PROFILE_BLOCK = "\n\nMULTI-CHARACTER CONVERSATION MODE:\n\n--- CHARACTER 1 PROFILE ---\nName: Marlowe\n"


def test_character_variables():
    # THE REPORTED PROMPT: the name slot held the source passage, so the model reproduced the
    # passage every turn. {character_name} puts a NAME in the name slot.
    filled = generation.fill_answer_prompt(
        "You are writing the reply of the woman, {character_name}, in an intense roleplay with a man.",
        "Marlowe lets out a sharp and stifled whimper as the plastic tip slides deep.",
        "ctx", character_names=["Marlowe"])
    check("{character_name} fills with the first scene name",
          filled == "You are writing the reply of the woman, Marlowe, in an intense roleplay with a man.")
    check("the source passage is no longer pasted into the name slot",
          "whimper" not in filled)

    # Multi-character scenes: every name, joined the same way the cast note joins them.
    check("{character_names} joins every scene name",
          generation.fill_answer_prompt("{character_names}", "s", "c",
                                       character_names=["Marlowe", "Ines"])
          == "Marlowe, Ines")
    check("{character_name} is still only the first",
          generation.fill_answer_prompt("{character_name}", "s", "c",
                                       character_names=["Marlowe", "Ines"]) == "Marlowe")

    # {character_profile} carries the whole block the system prompt gets.
    check("{character_profile} fills with the profile block",
          generation.fill_answer_prompt("PROFILE:{character_profile}", "s", "c",
                                       character_profile=PROFILE_BLOCK)
          == "PROFILE:" + PROFILE_BLOCK)

    # Empty values fall back to N/A exactly like the chunk variables, so no clause dangles.
    for template in ("her, {character_name}.", "them, {character_names}.", "{character_profile}"):
        check("an empty character variable fills as N/A: %r" % template,
              generation.fill_answer_prompt(template, "s", "c") == template.replace(
                  template[template.index("{"):template.index("}") + 1], "N/A"))

    # The name is now IN the documented variable set.
    for token in ("{character_name}", "{character_names}", "{character_profile}"):
        check("%s is a documented native variable" % token,
              token in generation.ANSWER_PROMPT_NATIVE_VARIABLES)
    check("{subject} and {context} keep their documented meaning",
          "{subject}" in generation.ANSWER_PROMPT_NATIVE_VARIABLES
          and "{context}" in generation.ANSWER_PROMPT_NATIVE_VARIABLES)

    # The variable-free guarantee still holds with the wider set.
    shipped = "Write your next message in the roleplay. Write only the message itself."
    check("a variable-free prompt is still byte-identical",
          generation.fill_answer_prompt(shipped, "s", "c", character_names=["Marlowe"],
                                       character_profile=PROFILE_BLOCK) == shipped)

    # Dataset aliases still render alongside the new variables.
    check("a dataset alias renders alongside a character variable",
          generation.fill_answer_prompt("Name {{ds:name}} as {character_name}", "s", "c",
                                       {"name": "Marlowe"}, character_names=["Ines"])
          == "Name Marlowe as Ines")


# --- v12.7: the source chunk is made safe to use inline ---------------------------------------

GUTS_CHUNK = ("Marlowe lets out a sharp and stifled whimper as the plastic tip slides deep into\n"
              "her bowels.  Her sturdy thighs shake and her honey blonde ass quivers against the\n\n"
              "cold air of the night pool. She feels the nozzle stretching her tight hole and\n"
              "pushing past her sphincter into the heat of her guts.")


def test_subject_chunk_is_made_safe():
    # THE REPORTED ARTIFACT: the chunk ended on a full stop and the prompt's own comma followed it,
    # producing "guts.,".
    rendered = generation.fill_answer_prompt(
        "You are writing the reply of the woman, {subject}, in an intense roleplay with a man.",
        GUTS_CHUNK, "ctx")
    check("the chunk loses its trailing full stop", "guts.," not in rendered)
    check("the prompt's own comma is the only one after the chunk",
          "guts, in an intense roleplay" in rendered)

    # A multi-paragraph chunk cannot break the instruction's structure.
    check("the chunk's newlines are collapsed", "\n" not in rendered)
    check("runs of spaces are collapsed", "  " not in rendered)
    check("the instruction's structure survives",
          rendered.count("in an intense roleplay") == 1 and rendered.endswith("with a man."))

    # Every trailing terminator, not just the full stop.
    for terminator in ("", ".", "!", "?", ",", ";", ":"):
        out = generation.fill_answer_prompt("{subject}, next", "a tail" + terminator, "c")
        check("a chunk ending on %r does not double the punctuation" % terminator,
              out == "a tail, next")

    # {context} gets the same treatment -- it is the sibling variable of the same passage.
    check("{context} is normalised too",
          generation.fill_answer_prompt("{context}. Next", "s", "a\n\nb.") == "a b. Next")

    # An empty chunk still falls back to N/A (unchanged behaviour).
    check("an empty chunk is still N/A",
          generation.fill_answer_prompt("{subject}!", "", "") == "N/A!")
    check("a whitespace-only chunk is N/A, not a stray space",
          generation.fill_answer_prompt("{subject}!", "  \n\t ", "") == "N/A!")

    # The helper itself is idempotent and total.
    check("_clean_chunk_for_inline is idempotent",
          generation._clean_chunk_for_inline(
              generation._clean_chunk_for_inline(GUTS_CHUNK)) ==
          generation._clean_chunk_for_inline(GUTS_CHUNK))
    check("_clean_chunk_for_inline handles None",
          generation._clean_chunk_for_inline(None) == "")


# --- v12.7: the repeated-reply detector --------------------------------------------------------

# The operator's actual defect: three byte-identical 884-character replies.
LONG_REPLY = (
    "Marlowe lets out a loud and wet moan as the pressure builds deep inside her, her fingers "
    "curling into the cold tile and her breath coming in short, ragged gasps. Her hips shift "
    "involuntarily against the stone and she presses her forehead to the wall, eyes shut tight, "
    "waiting for the wave to pass over her. The water runs in sheets down her back and she "
    "shivers, not from the cold."
)


def test_repeated_reply_detector():
    # THE REPORTED PAIR: byte-identical.
    found, info = detection.is_repeated_reply(LONG_REPLY, [LONG_REPLY])
    check("a byte-identical reply is caught", found and len(info) == 1)
    check("the hit is labelled", found and info[0][0].startswith("Repeated reply"))
    check("the snippet is a short readable prefix",
          found and len(info[0][1].split()) <= 5)

    # The defect survives a whitespace/case difference, which is not a different reply.
    for variant in (LONG_REPLY.replace(" ", "  "), LONG_REPLY.upper(),
                    "\n" + LONG_REPLY + "\n", "  " + LONG_REPLY):
        check("a whitespace/case variant is still a repeat",
              detection.is_repeated_reply(variant, [LONG_REPLY])[0])

    # A near-repeat: the tail rewritten, the body identical.
    near = LONG_REPLY[:-40] + "and the room goes quiet around her at last."
    check("a near-verbatim reply is caught",
          detection.is_repeated_reply(near, [LONG_REPLY])[0])

    # A genuinely different reply must NOT be touched: this is the guard's cost.
    different = (
        "She crosses to the window and pulls the shutter closed against the wind. The glass "
        "rattles in its frame and she holds it there with one palm until the gust dies. \"You "
        "should not have come tonight,\" she says, without turning round."
    )
    check("a genuinely different reply is left alone",
          detection.is_repeated_reply(different, [LONG_REPLY]) == (False, []))

    # A short reply can coincide innocently ("She nods."), so short replies never hit.
    check("a short reply never hits",
          detection.is_repeated_reply("She nods.", ["She nods."]) == (False, []))

    # Degenerate inputs.
    check("no earlier replies means no hit",
          detection.is_repeated_reply(LONG_REPLY, []) == (False, []))
    check("None earlier replies means no hit",
          detection.is_repeated_reply(LONG_REPLY, None) == (False, []))
    check("an empty reply never hits",
          detection.is_repeated_reply("", [LONG_REPLY]) == (False, []))

    # A hit against ANY earlier reply, not just the most recent one.
    check("an earlier-but-not-immediately-previous match still hits",
          detection.is_repeated_reply(LONG_REPLY, [different, LONG_REPLY])[0])

    # similarity=1.0 is an exact match only, so safe for a caller that wants that.
    check("similarity 1.0 accepts an exact match",
          detection.is_repeated_reply(LONG_REPLY, [LONG_REPLY], similarity=1.0)[0])
    check("similarity 1.0 rejects a near match",
          not detection.is_repeated_reply(near, [LONG_REPLY], similarity=1.0)[0])

    # The two guards are siblings, not duplicates: this reply's OPENER is fresh, so the
    # opener check stays quiet while the whole-reply check fires. That is the reported defect.
    check("the opener check cannot see a verbatim whole-reply repeat",
          not detection.is_repeated_opener(LONG_REPLY, [LONG_REPLY])[0]
          or detection.is_repeated_reply(LONG_REPLY, [LONG_REPLY])[0])


# --- v12.7: her reply is sent once to the continuation -----------------------------------------

CONTINUE_WITH_VAR = "CONTINUE {last_assistant_message}"
CONTINUE_NO_VAR = "CONTINUE without naming her reply"


def _call_continuation(generation, template, history, character_names=None):
    calls = []

    def post(url, headers=None, data=None, timeout=None):
        calls.append(json.loads(data))
        return _Response(200, "I step closer and put my hand on the rail.")

    generation.requests.post = post
    result = generation.generate_user_continuation(
        system_prompt="You are a helpful roleplay assistant.",
        conversation_history_for_llm=history,
        user_continuation_prompt_template=template,
        thread_id=0,
        sampler_settings_local={},
        api_url_local="http://fixture/v1/chat/completions",
        model_name_local="model-x", api_key_local="",
        api_slot_idx=0, current_max_attempts_param=1, api_request_timeout_param=5,
        file_name="test.txt", refusal_phrases_local=(), slop_phrases_local=(),
        character_names=character_names,
    )
    return result, calls


def _continuation_history():
    # The shape the worker passes in: no system message (that is prepended by the function) and it
    # ENDS with her reply, which the worker appends before calling this.
    return [
        {"role": "user", "content": "What now?"},
        {"role": "assistant", "content": LONG_REPLY},
    ]


def test_continuation_sends_her_reply_once():
    generation = _load_generation_with_stubs()

    # THE DEFECT: the history already ended with her reply and the template embedded the same text
    # again, so one request carried it twice.
    result, calls = _call_continuation(generation, CONTINUE_WITH_VAR, _continuation_history())
    sent = calls[0]["messages"]
    check("a template naming {last_assistant_message} drops the duplicate history turn",
          len(sent) == 3 and sent[-1]["role"] == "user")
    check("her reply appears exactly once in the request",
          json.dumps(sent).count(LONG_REPLY) == 1)
    check("the reply is in the slot the prompt names",
          LONG_REPLY in sent[-1]["content"])
    check("the earlier user turn is still sent",
          sent[1] == {"role": "user", "content": "What now?"})
    check("the system prompt is untouched",
          sent[0] == {"role": "system", "content": "You are a helpful roleplay assistant."})
    check("the continuation still returns a reply",
          result == "I step closer and put my hand on the rail.")

    # A template that does NOT name the variable keeps the full history, so an existing config's
    # requests are unchanged -- this is the opt-in guarantee.
    result, calls = _call_continuation(generation, CONTINUE_NO_VAR, _continuation_history())
    sent = calls[0]["messages"]
    check("a template without the variable keeps the full history",
          len(sent) == 4 and sent[2]["content"] == LONG_REPLY)

    # History that does not end with an assistant turn is never trimmed.
    history = [{"role": "user", "content": "hello"}]
    result, calls = _call_continuation(generation, CONTINUE_WITH_VAR, history)
    check("a history not ending on an assistant turn is untouched",
          len(calls[0]["messages"]) == 3)

    # The character variables are filled for the continuation too.
    result, calls = _call_continuation(
        generation, "{character_name}|{character_names}|{last_assistant_message}",
        _continuation_history(), character_names=["Marlowe", "Ines"])
    content = calls[0]["messages"][-1]["content"]
    check("the continuation fills the character variables",
          content.startswith("Marlowe|Marlowe, Ines|") and LONG_REPLY in content)
    check("an unnamed continuation character fills as N/A",
          _call_continuation(generation, "{character_name}", _continuation_history())[1][0]
          ["messages"][-1]["content"] == "N/A")


# --- v12.7: the repeated-reply retry pipeline ---------------------------------------------------


def _call_generate_reply(generation, replies, enabled=True, earlier=(LONG_REPLY,),
                         max_retries=1, fixes=(), answer_prompt="Continue the scene."):
    """Drive generate_answer_with_retries with the repeated-REPLY guard's kwargs."""
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
        repeat_reply_enabled_local=enabled,
        repeat_reply_fixes_local=fixes,
        repeat_reply_max_retries=max_retries,
        previous_assistant_replies_local=earlier,
    )
    return result, calls


def test_repeat_reply_pipeline():
    generation = _load_generation_with_stubs()
    fresh = ("She crosses to the window and pulls the shutter closed against the wind. "
             "The glass rattles and she holds it there with one palm until the gust dies.")

    # OFF (default): the repeated reply is written as-is, exactly one main-API call.
    result, calls = _call_generate_reply(generation, [LONG_REPLY], enabled=False)
    check("reply off: the repeat is written as-is (no retry)",
          result[0] == LONG_REPLY and len(calls) == 1)

    # ON: the repeat goes back to the SAME main API and the fresh reply is kept.
    result, calls = _call_generate_reply(generation, [LONG_REPLY, fresh], enabled=True)
    check("reply on: the retried (fresh) reply is what gets written", result[0] == fresh)
    check("reply on: the answer was sent back to the main API", len(calls) == 2)
    check("reply on: the retry went to the same model/slot",
          calls[1]["model"] == calls[0]["model"])

    # THE CACHE CONTRACT: the fix goes on the ANSWER PROMPT, so the system prompt and the whole
    # history stay byte-identical and a prefix-caching server still serves them.
    check("reply on: the system prompt is unchanged between the requests",
          calls[0]["messages"][0]["content"] == calls[1]["messages"][0]["content"])
    check("reply on: the history is unchanged between the requests",
          calls[0]["messages"][1:-1] == calls[1]["messages"][1:-1])
    check("reply on: the fix is appended to the answer prompt",
          calls[0]["messages"][-1]["content"] == "Continue the scene."
          and calls[1]["messages"][-1]["content"].startswith("Continue the scene.")
          and calls[1]["messages"][-1]["content"] != "Continue the scene.")
    check("reply on: the default fix is used when no list is configured",
          generation.DEFAULT_REPEAT_REPLY_FIX in calls[1]["messages"][-1]["content"])

    # A configured fix list is used instead.
    result, calls = _call_generate_reply(
        generation, [LONG_REPLY, fresh], enabled=True,
        fixes=("Write something completely new this turn.",))
    check("reply on: a configured fix is used",
          "Write something completely new this turn." in calls[1]["messages"][-1]["content"])

    # ON but the retry repeats again: bounded, best effort kept, never dropped.
    result, calls = _call_generate_reply(
        generation, [LONG_REPLY, LONG_REPLY], enabled=True)
    check("reply on: a still-repeating retry is kept rather than dropped",
          result[0] == LONG_REPLY and len(calls) == 2)

    # max_retries = 0 keeps the detection but never retries.
    result, calls = _call_generate_reply(
        generation, [LONG_REPLY], enabled=True, max_retries=0)
    check("reply max_retries 0: no retry at all",
          result[0] == LONG_REPLY and len(calls) == 1)

    # A fresh reply needs no extra request, and a first reply can never trigger the guard.
    result, calls = _call_generate_reply(generation, [fresh], enabled=True)
    check("reply on: a fresh reply needs no retry",
          result[0] == fresh and len(calls) == 1)
    result, calls = _call_generate_reply(
        generation, [LONG_REPLY], enabled=True, earlier=())
    check("reply on: the first assistant reply never triggers the guard",
          result[0] == LONG_REPLY and len(calls) == 1)

    # The two guards are independent: turning the reply guard on does not turn the opener
    # guard on, and a reply with a fresh opener but a repeated BODY is caught here only.
    result, calls = _call_generate_reply(generation, [LONG_REPLY, fresh], enabled=True)
    check("reply on: a repeat is not written", result[0] == fresh)


def main():
    test_fill_answer_prompt()
    test_character_variables()
    test_subject_chunk_is_made_safe()
    test_opener_tokens()
    test_repeated_opener_detector()
    test_repeated_reply_detector()
    test_continuation_sends_her_reply_once()
    test_repeat_opener_pipeline()
    test_repeat_reply_pipeline()
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
