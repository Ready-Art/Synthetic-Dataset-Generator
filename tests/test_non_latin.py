# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the v11.10.0 non-Latin script check.

Covers the detector in ``detection.py`` (a Unicode range allow-list -- no LLM call, no phrase
list, no network) and the retry pass in ``generation.generate_answer_with_retries`` (stubbed
``requests.post``; no model, network or display needed).

Run with ``python -B tests/test_non_latin.py``.
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
_tmpdir = tempfile.TemporaryDirectory(prefix="non_latin_")
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


# --- Detector -----------------------------------------------------------------------------

def test_detector_flags_non_latin():
    """Stray characters from another script must be flagged, with a readable script label."""
    positives = [
        ("She smiles. \u4f60\u597d there.", "Chinese (CJK)"),
        ("\u3053\u3093\u306b\u3061\u306f, he says.", "Japanese (Kana)"),
        ("\uc548\ub155, she says.", "Korean (Hangul)"),
        ("\u041f\u0440\u0438\u0432\u0435\u0442, he says.", "Cyrillic"),
        ("\u0393\u03b5\u03b9\u03b1 \u03c3\u03bf\u03c5, she says.", "Greek"),
        ("\u0645\u0631\u062d\u0628\u0627, he says.", "Arabic"),
        ("\u05e9\u05dc\u05d5\u05dd, she says.", "Hebrew"),
        ("\u0928\u092e\u0938\u094d\u0924\u0947, he says.", "Devanagari"),
        ("\u0e2a\u0e27\u0e31\u0e2a\u0e14\u0e35, she says.", "Thai"),
        ("Smiling \U0001F600 widely.", "Emoji / symbols"),
    ]
    for text, script in positives:
        found, info = detection.is_non_latin(text)
        check("flags %s: %r" % (script, text), found and len(info) >= 1)
        check("labels the script %s" % script,
              found and any(script in label for label, _sentence in info))
        check("the reported sentence holds the stray: %r" % text,
              found and any(script not in sentence for _label, sentence in info))


def test_detector_accepts_latin():
    """Everything a Latin-script roleplay reply legitimately uses must pass."""
    negatives = [
        "Plain ASCII prose, with punctuation!",
        "Caf\u00e9 au lait, na\u00efve, r\u00e9sum\u00e9, \u00c6ther, stra\u00dfe.",
        "It\u2019s a \u2018quoted\u2019 line\u2026 and an \u2014 em dash.",
        "He whispers: \u00abbonjour\u00bb and shrugs.",
        "A star \u2605 and a heart \u2764.",
        "\u00a1Hola! \u00bfQu\u00e9 tal? \u2014 Se\u00f1or.",
        "",
        "   ",
    ]
    for text in negatives:
        found, info = detection.is_non_latin(text)
        check("does not flag: %r" % text, not found and info == [])
    check("None is not flagged", detection.is_non_latin(None) == (False, []))


def test_one_entry_per_script():
    """A whole sentence in another script reports ONE row per script, not one per character."""
    found, info = detection.is_non_latin("\u4f60\u597d\u4e16\u754c\uff0c\u4f60\u597d")
    check("a CJK sentence reports a single row", found and len(info) == 1)
    found, info = detection.is_non_latin("\u4f60\u597d and \u041f\u0440\u0438\u0432\u0435\u0442")
    check("two distinct scripts report two rows", found and len(info) == 2)


def test_find_non_latin_characters():
    hits = detection.find_non_latin_characters("Hi \u4f60 there.")
    check("find_non_latin_characters returns (script, character, sentence)",
          len(hits) == 1 and hits[0][0] == "Chinese (CJK)" and hits[0][1] == "\u4f60")
    check("clean text returns no hits",
          detection.find_non_latin_characters("Plain prose.") == [])
    check("empty text returns no hits", detection.find_non_latin_characters("") == [])


def test_timestamp():
    """A hit records one timestamp in issue_timestamps['non_latin'] (1-hour window)."""
    with app_state.issue_timestamps_lock:
        app_state.issue_timestamps['non_latin'] = []
    detection.issue_timestamps = app_state.issue_timestamps
    detection.issue_timestamps_lock = app_state.issue_timestamps_lock
    detection.is_non_latin("\u4f60\u597d")
    check("a hit appends exactly one timestamp",
          len(app_state.issue_timestamps['non_latin']) == 1)
    detection.is_non_latin("plain latin")
    check("a clean reply appends no timestamp",
          len(app_state.issue_timestamps['non_latin']) == 1)


def test_config_default():
    """The shipped config block is off by default and carries a fix list key."""
    import yaml
    with open(REPO / "config" / "config.yml", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    block = config.get("detection", {}).get("non_latin", {})
    check("detection.non_latin exists", isinstance(block, dict))
    check("the check is off by default", block.get("enabled") is False)
    check("fixes is a list", isinstance(block.get("fixes"), list))
    check("a built-in fix instruction exists",
          isinstance(generation.DEFAULT_NON_LATIN_FIX, str)
          and "Latin alphabet" in generation.DEFAULT_NON_LATIN_FIX)


# --- Retry pipeline ------------------------------------------------------------------------

def _load_generation_with_stubs():
    """Import-time stubs so the answer loop can run without GUI, Valkey or a network."""
    app_state.stop_processing = False
    app_state.pause_processing = False
    app_state.total_input_tokens = 0
    app_state.total_output_tokens = 0
    app_state.total_attempts_global = 0
    app_state.non_latin_count_total = 0
    app_state.non_latin_counts_per_api = {i: 0 for i in range(6)}
    app_state.recent_non_latin_total = []
    app_state.recent_non_latin_per_api = {i: [] for i in range(6)}
    with app_state.issue_timestamps_lock:
        app_state.issue_timestamps['non_latin'] = []
    detection.issue_timestamps = app_state.issue_timestamps
    detection.issue_timestamps_lock = app_state.issue_timestamps_lock
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


def _call_generate(generation, posts, enabled, fixes=()):
    """Drive generate_answer_with_retries with a scripted list of fixture replies."""
    calls = []

    def post(url, headers=None, data=None, timeout=None):
        calls.append(json.loads(data))
        return _Response(200, posts[min(len(calls) - 1, len(posts) - 1)])

    generation.requests.post = post
    result = generation.generate_answer_with_retries(
        base_system_prompt="You are a helpful roleplay assistant.",
        conversation_history_for_llm=[],
        answer_prompt_template="Continue the scene.",
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
        non_latin_enabled_local=enabled,
        non_latin_fixes_local=fixes,
    )
    return result, calls


def test_retry_pipeline():
    generation = _load_generation_with_stubs()
    stray = "She smiles. \u4f60\u597d, he says."
    clean = "She smiles. Hello, he says."

    # OFF (default): the stray reply is accepted as-is, exactly one main-API call.
    result, calls = _call_generate(generation, [stray], enabled=False)
    check("off: the stray reply is written as-is (no retry)", result[0] == stray)
    check("off: exactly one main-API request", len(calls) == 1)

    # ON: the stray reply is regenerated; the clean re-reply is what gets written.
    app_state.non_latin_count_total = 0
    result, calls = _call_generate(generation, [stray, clean], enabled=True)
    check("on: the retried (clean) reply is what gets written", result[0] == clean)
    check("on: the answer was regenerated", len(calls) == 2)
    check("on: the retry went to the same slot and model",
          calls[1]["model"] == calls[0]["model"])
    check("on: the built-in fix is appended to the system prompt",
          calls[0]["messages"][0]["content"] == "You are a helpful roleplay assistant."
          and "Latin alphabet" in calls[1]["messages"][0]["content"])
    check("on: the hit is counted", app_state.non_latin_count_total == 1)
    # v15.0.1: the ring holds `{time, phrase, context, api}` dicts, not bare tuples.
    # A tuple had no timestamp, which is why the Web UI's Time column was blank.
    check("on: the hit lands in recent_non_latin_total",
          len(app_state.recent_non_latin_total) == 1
          and app_state.recent_non_latin_total[0]["phrase"].startswith("Non-Latin script")
          and app_state.recent_non_latin_total[0]["time"])
    check("on: a timestamp is recorded",
          len(app_state.issue_timestamps['non_latin']) == 1)

    # ON with a configured fix list: the configured fix is used, not the built-in one.
    result, calls = _call_generate(
        generation, [stray, clean], enabled=True, fixes=("Only English letters, please.",))
    check("on: a configured fix is appended instead of the default",
          "Only English letters, please." in calls[1]["messages"][0]["content"])

    # ON and every reply is still stray: once the fixes are exhausted the turn fails for that
    # attempt and a FRESH attempt starts (the same policy as an unfixable refusal or OOC drift),
    # so the loop is bounded by max_attempts_local and no stray row reaches the dataset.
    result, calls = _call_generate(generation, [stray], enabled=True)
    check("on: a permanently stray reply is never written",
          result is not None and result[0] is None)
    check("on: the retry is bounded by max_attempts_local (no infinite loop)",
          len(calls) <= 3 * 3)

    # ON with a clean reply from the first call: no extra request at all.
    result, calls = _call_generate(generation, [clean], enabled=True)
    check("on: a clean reply needs no retry", result[0] == clean and len(calls) == 1)


def test_other_generation_paths():
    """The question, user-continuation and opening-mode paths reject a stray reply too."""
    generation = _load_generation_with_stubs()
    stray = "She smiles. \u4f60\u597d, he says."
    clean = "She smiles. Hello, he says."

    def scripted(posts):
        calls = []

        def post(url, headers=None, data=None, timeout=None):
            calls.append(json.loads(data))
            return _Response(200, posts[min(len(calls) - 1, len(posts) - 1)])

        generation.requests.post = post
        return calls

    # generate_question
    calls = scripted([stray, clean])
    text = generation.generate_question(
        "system", "question {context}", "subject", "context", 0, {},
        "http://fixture/v1/chat/completions", "model-x", "", 10,
        "raw subj", "raw ctx", 0, 3, 5, file_name="q.txt",
        non_latin_enabled_local=True)
    check("question: the stray reply is re-rolled", text == clean and len(calls) == 2)

    # generate_question with the check off
    calls = scripted([stray])
    text = generation.generate_question(
        "system", "question {context}", "subject", "context", 0, {},
        "http://fixture/v1/chat/completions", "model-x", "", 10,
        "raw subj", "raw ctx", 0, 3, 5, file_name="q.txt",
        non_latin_enabled_local=False)
    check("question: off keeps the stray reply", text == stray and len(calls) == 1)

    # generate_user_continuation
    calls = scripted([stray, clean])
    text = generation.generate_user_continuation(
        "system", [{"role": "assistant", "content": "hi"}], "next {last_assistant_message}",
        0, {}, "http://fixture/v1/chat/completions", "model-x", "", 0, 3, 5,
        file_name="u.txt", non_latin_enabled_local=True)
    check("continuation: the stray reply is re-rolled", text == clean and len(calls) == 2)

    # generate_greeting
    calls = scripted([stray, clean])
    text = generation.generate_greeting(
        "scene", "greet {speaker}{context}", "Ines", "", 0, {},
        "http://fixture/v1/chat/completions", "model-x", "", 0, 3, 5,
        file_name="g.txt", non_latin_enabled_local=True)
    check("greeting: the stray reply is re-rolled", text == clean and len(calls) == 2)

    # generate_first_line
    calls = scripted([stray, clean])
    text = generation.generate_first_line(
        "greeting", "write {style}", 0, {}, "http://fixture/v1/chat/completions",
        "model-x", "", 0, 3, 5, file_name="f.txt", styles=("an action",),
        non_latin_enabled_local=True)
    check("first line: the stray reply is re-rolled", text == clean and len(calls) == 2)


def main():
    test_detector_flags_non_latin()
    test_detector_accepts_latin()
    test_one_entry_per_script()
    test_find_non_latin_characters()
    test_timestamp()
    test_config_default()
    test_retry_pipeline()
    test_other_generation_paths()
    if FAILURES:
        print("\n%d FAILED check(s):" % len(FAILURES))
        for name in FAILURES:
            print("  - %s" % name)
        return 1
    print("\nAll non-Latin checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
