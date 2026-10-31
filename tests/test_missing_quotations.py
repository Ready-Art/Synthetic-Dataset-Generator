"""Headless tests for the v11.7.1 "Experiment: Retry missing quotations" pipeline.

Covers the detector in ``detection.py`` (a spoken line that arrived with NO quotation marks
at all -- "Its funny she says") and the retry pass in ``generation.generate_answer_with_retries``
(stubbed ``requests.post``; no model, network or display needed).

Run with ``python -B tests/test_missing_quotations.py``.
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
_tmpdir = tempfile.TemporaryDirectory(prefix="missing_quotes_")
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

def test_detector():
    # The reported bug shape: a spoken line with zero quotation marks and an attribution verb.
    positives = [
        "Its funny she says",
        'He says that it is funny, really',
        'The joke lands flat, she sighs, staring at the door',
    ]
    for text in positives:
        found, info = detection.is_missing_quotation(text)
        check("flags missing-quotations reply: %r" % text, found and len(info) == 1)
        check("labels the hit: %r" % text,
              found and info[0][0].startswith("Missing quotation"))

    # v11.7.2 regression: a CONTRACTION or POSSESSIVE must not switch the detector off. The
    # old guard bailed out on any apostrophe, so ordinary prose -- which is full of "don't" and
    # "Melody's" -- never got checked and mark-less spoken lines reached the dataset. These are
    # the same defect shapes as above with an apostrophe added; each must still flag.
    apostrophe_positives = [
        "It's not funny she says",                 # contraction, straight apostrophe
        'She doesn\u2019t care, he said',              # contraction, curly apostrophe
        "Don't do that, she whispered",            # contraction at the start of the reply
        "Melody's hand trembles as she speaks",    # possessive
        "It's the shop's only clock, she says",    # contraction + possessive in one reply
    ]
    for text in apostrophe_positives:
        found, info = detection.is_missing_quotation(text)
        check("apostrophe does not mask the defect: %r" % text,
              found and len(info) == 1)

    # Anything carrying a real quote glyph is NOT missing quotations: the balanced case belongs
    # to normalize_quotes, the unbalanced case to is_incomplete_quote. Single-quoted dialogue
    # counts as quoted -- only apostrophes BETWEEN word characters are discounted.
    negatives = [
        '"Its funny," she says.',          # proper straight-quote dialogue
        '\u201cIts funny,\u201d she says.',  # proper curly dialogue
        "'Its funny,' she says.",          # single-quoted dialogue (straight)
        '\u2018Its funny,\u2019 she says.',    # single-quoted dialogue (curly)
        'She whispered "no" into the dark.',  # embedded quoted word
        "The ship sailed at dawn.",            # narration, no speech attribution
        "It was quiet.",                       # too short
        "",                                    # empty
    ]
    for text in negatives:
        found, info = detection.is_missing_quotation(text)
        check("does not flag: %r" % text, not found and info == [])


def test_has_dialogue_quote_marks():
    """The contraction-discounting helper behind the detector (v11.7.2)."""
    # Apostrophes inside a word are contractions/possessives, not quotation marks.
    for text in ["don't", "Melody's watch", "it\u2019s late",
                 "doesn't", "the shop's only clock"]:
        check("apostrophe is not a quote mark: %r" % text,
              not detection.has_dialogue_quote_marks(text))
    # A plural possessive (apostrophe AFTER the final word character) is genuinely
    # ambiguous with a closing single quote, so it is deliberately left alone: treating it
    # as a quote mark only suppresses the check, which is the safe direction (no retry).
    for text in ["the clocks' hands", "dogs'"]:
        check("plural possessive is left ambiguous (suppresses, safe): %r" % text,
              detection.has_dialogue_quote_marks(text))
    # Real quote glyphs are.
    for text in ['"hi"', "'hi'", '\u201chi\u201d', '\u2018hi\u2019',
                 "she said 'go' and left", 'a lone " opener']:
        check("quote glyph detected: %r" % text,
              detection.has_dialogue_quote_marks(text))
    check("empty text carries no quote mark",
          not detection.has_dialogue_quote_marks(""))
    check("None carries no quote mark",
          not detection.has_dialogue_quote_marks(None))


# --- Retry pipeline ------------------------------------------------------------------------

def _load_generation_with_stubs():
    """Import-time stubs so the answer loop can run without GUI, Valkey or a network."""
    app_state.stop_processing = False
    app_state.pause_processing = False
    app_state.total_input_tokens = 0
    app_state.total_output_tokens = 0
    app_state.total_attempts_global = 0
    for key in ("user_speaking_counts_per_api", "refusal_counts_per_api", "slop_counts_per_api",
                "anti_slop_counts_per_api", "ooc_counts_per_api", "error_counts_per_api",
                "recent_user_speaking_total", "recent_refusals_total", "recent_slop_total",
                "recent_anti_slop_total", "recent_ooc_total", "recent_errors_total"):
        setattr(app_state, key, getattr(app_state, key, {i: [] for i in range(6)}
                                        if "_per_api" in key else []))
    for key in ("user_speaking_counts_per_api", "refusal_counts_per_api", "slop_counts_per_api",
                "anti_slop_counts_per_api", "ooc_counts_per_api", "error_counts_per_api"):
        setattr(app_state, key, {i: 0 for i in range(6)})
    generation.check_circuit_breaker = lambda _slot: True
    generation.record_api_success = lambda _slot: None
    generation.record_api_failure = lambda _slot: None
    generation.update_live_prompt_preview = lambda *a, **k: None
    generation.apply_bad_words_to_payload = lambda payload, _t: payload
    generation.sanitize_payload_for_endpoint = lambda payload, _u, _s: payload
    generation.get_cached_response = lambda _hash, _slot: (None, False)
    generation.set_cached_response = lambda *a, **k: None
    generation.global_rate_limiter = types.SimpleNamespace(wait_if_needed=lambda _slot: None)
    # The answer loop logs every request to output/debug_prompt*.jsonl and branches on the
    # duplication flag; provide both headlessly.
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


def _call_generate(generation, posts, flag):
    """Drive generate_answer_with_retries with a scripted list of fixture replies."""
    calls = []

    def post(url, headers=None, data=None, timeout=None):
        calls.append(json.loads(data))
        return _Response(200, posts[len(calls) - 1])

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
        retry_missing_quotations_local=flag,
    )
    return result, calls


def test_retry_pipeline():
    generation = _load_generation_with_stubs()

    # OFF (default): the mark-less reply is accepted as-is, exactly one main-API call.
    result, calls = _call_generate(
        generation, ["Its funny she says"], flag=False)
    check("off: the reply is written as-is (no retry)", result[0] == "Its funny she says")
    check("off: exactly one main-API request", len(calls) == 1)

    # ON: the mark-less reply is sent back to the SAME main API; the quoted re-reply is kept.
    result, calls = _call_generate(
        generation, ["Its funny she says", '"Its funny," she says.'], flag=True)
    check("on: the retried (quoted) reply is what gets written",
          result[0] == '"Its funny," she says.')
    check("on: the answer was sent back to the main API for a retry", len(calls) == 2)
    check("on: the retry went to the same slot",
          calls[0]["messages"][1]["role"] == "user"
          and calls[1]["model"] == calls[0]["model"])
    # Deliberately NOT appended to the system prompt (prefix-cache friendly): both requests
    # carry the identical, unmodified system message.
    check("on: nothing is appended to the system prompt",
          calls[0]["messages"][0]["content"] == calls[1]["messages"][0]["content"]
          == "You are a helpful roleplay assistant.")

    # ON but the retry comes back mark-less again: bounded to one retry, best effort kept.
    result, calls = _call_generate(
        generation, ["Its funny she says", "Its still funny he says"], flag=True)
    check("on: a still-broken retry is kept rather than dropped",
          result[0] == "Its still funny he says")
    check("on: at most one missing-quotation retry per attempt (no infinite loop)",
          len(calls) <= 6)

    # ON with a properly quoted reply from the first call: no extra request at all.
    result, calls = _call_generate(generation, ['"Its funny," she says.'], flag=True)
    check("on: a quoted reply needs no retry",
          result[0] == '"Its funny," she says.' and len(calls) == 1)


def main():
    test_detector()
    test_has_dialogue_quote_marks()
    test_retry_pipeline()
    if FAILURES:
        print("\n%d FAILED check(s):" % len(FAILURES))
        for name in FAILURES:
            print("  - %s" % name)
        return 1
    print("\nAll missing-quotations checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
