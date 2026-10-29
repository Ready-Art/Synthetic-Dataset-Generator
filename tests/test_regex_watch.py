"""Headless tests for Regex Watch (v11.6).

Covers the pattern engine in ``detection.py`` and the rewrite pass in ``generation.py``. No
model, network or display is needed: ``requests.post`` is stubbed with fixtures.

Run with ``python -B tests/test_regex_watch.py``.
"""
import json
import os
import sys
import types
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import detection


FAILURES = []


def check(name, condition):
    if condition:
        print("PASS: %s" % name)
    else:
        print("FAIL: %s" % name)
        FAILURES.append(name)


# --- Pattern engine ----------------------------------------------------------------------

def test_validation():
    valid, errors = detection.validate_regex_patterns(
        ["\\bnot\\s+just\\b", "   ", "\\bnot\\s+just\\b", "[unclosed"])
    check("valid patterns keep order and drop blanks", valid == ["\\bnot\\s+just\\b"])
    check("duplicates are collapsed", len(valid) == 1)
    check("an uncompilable pattern is reported", len(errors) == 1 and errors[0][0] == "[unclosed")
    check("the error names the reason", "unterminated" in errors[0][1].lower()
          or "missing" in errors[0][1].lower() or errors[0][1])

    valid, errors = detection.validate_regex_patterns([])
    check("an empty list validates to nothing", valid == [] and errors == [])
    valid, errors = detection.validate_regex_patterns(None)
    check("None validates to nothing", valid == [] and errors == [])

    long_pattern = "a" * (detection.MAX_REGEX_PATTERN_LENGTH + 1)
    valid, errors = detection.validate_regex_patterns([long_pattern])
    check("an oversized pattern is rejected", valid == [] and len(errors) == 1)


def test_default_patterns_compile():
    valid, errors = detection.validate_regex_patterns(detection.DEFAULT_REGEX_WATCH_PATTERNS)
    check("every shipped default pattern compiles", errors == [])
    check("the shipped defaults are all kept", len(valid) == len(detection.DEFAULT_REGEX_WATCH_PATTERNS))


def test_matching():
    patterns = list(detection.DEFAULT_REGEX_WATCH_PATTERNS)
    positives = [
        "It's not just a job, it's a calling.",
        "This is not about winning, it is about heart.",
        "She was not just a friend, but also a mentor.",
        "He is not only brave but also kind.",
        "It's more than just a game.",
        "It was less about the money and more about respect.",
    ]
    for text in positives:
        found, info = detection.is_regex_watch(text, patterns, record_timestamp=False)
        check("matches: %r" % text, found and info)

    negatives = [
        "The ship sailed at dawn.",
        "Nothing happened that night.",
        "She nodded once and left the room.",
    ]
    for text in negatives:
        found, info = detection.is_regex_watch(text, patterns, record_timestamp=False)
        check("no match: %r" % text, not found and info == [])


def test_match_reporting():
    matches = detection.find_regex_matches(
        "The ship sailed. It is not just a boat, it is a promise. Then silence.",
        [r"\bit\s+is\s+not\s+just\b.{1,60}?\bit\s+is\b"])
    check("a match is reported once", len(matches) == 1)
    pattern, sentence, matched = matches[0]
    check("the reported sentence is the one holding the match",
          sentence.startswith("It is not just") and "promise" in sentence)
    check("the matched text is returned", "not just a boat" in matched)
    check("the neighbouring sentences are excluded",
          "The ship sailed" not in sentence and "silence" not in sentence)


def test_zero_width_and_invalid_are_skipped():
    matches = detection.find_regex_matches("anything at all", [r"\b", r"(?=a)", "[unclosed"])
    check("zero-width and invalid patterns never produce a hit", matches == [])
    check("an invalid pattern does not raise", detection.is_regex_watch(
        "anything at all", ["[unclosed"], record_timestamp=False) == (False, []))


def test_case_insensitive():
    found, _ = detection.is_regex_watch("IT IS NOT JUST A BOAT, IT IS A PROMISE.",
                                       list(detection.DEFAULT_REGEX_WATCH_PATTERNS),
                                       record_timestamp=False)
    check("matching is case-insensitive", found)


def test_sentence_cap():
    long_text = ("It is not just " + ("x " * 400) + "it is something else.")
    matches = detection.find_regex_matches(long_text, [r"\bit\s+is\s+not\s+just\b"])
    check("a reported sentence is capped for display", matches and len(matches[0][1]) <= 300)


def test_timestamp_bookkeeping():
    lock = __import__("threading").Lock()
    detection.issue_timestamps = {'regex_watch': []}
    detection.issue_timestamps_lock = lock
    detection.is_regex_watch("It's not just a game.", [r"\bnot\s+just\s+a\s+game\b"])
    check("a hit appends a timestamp", len(detection.issue_timestamps['regex_watch']) == 1)
    detection.is_regex_watch("It's not just a game.", [r"\bnot\s+just\s+a\s+game\b"],
                             record_timestamp=False)
    check("record_timestamp=False does not append", len(detection.issue_timestamps['regex_watch']) == 1)
    detection.issue_timestamps = None
    detection.issue_timestamps_lock = None


# --- Rewrite pass ------------------------------------------------------------------------

def _load_generation_with_stubs():
    """Import generation.py with the modules it needs stubbed out (no GUI, no network)."""
    import app_state
    if 'generation' in sys.modules:
        return sys.modules['generation'], app_state

    # Keep the rewrite loop's bookkeeping harmless.
    app_state.stop_processing = False
    app_state.pause_processing = False
    app_state.total_input_tokens = 0
    app_state.total_output_tokens = 0
    app_state.regex_watch_count_total = 0
    app_state.regex_watch_counts_per_api = {i: 0 for i in range(6)}
    app_state.recent_regex_total = []
    app_state.recent_regex_per_api = {i: [] for i in range(6)}
    app_state.regex_watch_log = []

    import generation
    return generation, app_state


class _Response:
    def __init__(self, status_code, content):
        self.status_code = status_code
        self._content = content
        self.text = content

    def json(self):
        return {"choices": [{"message": {"content": self._content}}],
                "usage": {"prompt_tokens": 5, "completion_tokens": 5}}


def test_rewrite_pass():
    generation, app_state = _load_generation_with_stubs()
    generation.check_circuit_breaker = lambda _slot: True
    generation.record_api_success = lambda _slot: None
    generation.record_api_failure = lambda _slot: None
    generation.update_live_prompt_preview = lambda *a, **k: None
    generation.apply_bad_words_to_payload = lambda payload, _t: payload
    generation.sanitize_payload_for_endpoint = lambda payload, _u, _s: payload
    generation.global_rate_limiter = types.SimpleNamespace(wait_if_needed=lambda _slot: None)

    pattern = r"\bnot\s+just\s+a\s+\w+"
    calls = []

    def post_ok(url, headers=None, data=None, timeout=None):
        body = json.loads(data)
        calls.append(body)
        return _Response(200, "The blade was never merely a weapon.")

    generation.requests.post = post_ok
    rewritten, hits = generation.apply_regex_watch(
        "It was not just a blade.", [pattern],
        {"url": "http://fixture/model", "model": "m", "key": ""},
        {"temperature": 0.5}, 0, max_rounds=3)
    check("a clean rewrite replaces the text", rewritten == "The blade was never merely a weapon.")
    check("the rewrite is reported as rewritten", hits and hits[0][2] == "rewritten")
    check("the rewrite request carries the pattern", pattern in calls[0]["messages"][1]["content"])
    check("the rewrite request carries the passage",
          "It was not just a blade." in calls[0]["messages"][1]["content"])
    check("the counter records one hit", app_state.regex_watch_count_total == 1)
    check("the log records the hit", len(app_state.regex_watch_log) == 1)

    # A rewrite that still matches its own pattern is rejected.
    generation.requests.post = lambda *a, **k: _Response(200, "It was not just a blade.")
    rewritten, hits = generation.apply_regex_watch(
        "It was not just a blade.", [pattern],
        {"url": "http://fixture/model", "model": "m", "key": ""},
        {}, 0, max_rounds=1)
    check("a rewrite that still matches is discarded",
          rewritten == "It was not just a blade." and hits[0][2] == "unresolved")

    # An empty reply is rejected.
    generation.requests.post = lambda *a, **k: _Response(200, "  ")
    rewritten, hits = generation.apply_regex_watch(
        "It was not just a blade.", [pattern],
        {"url": "http://fixture/model", "model": "m", "key": ""},
        {}, 0, max_rounds=1)
    check("an empty rewrite keeps the original", rewritten == "It was not just a blade.")

    # A reply that unbalances quotes is rejected.
    generation.requests.post = lambda *a, **k: _Response(200, 'She said "the blade was never a weapon.')
    rewritten, _hits = generation.apply_regex_watch(
        "It was not just a blade.", [pattern],
        {"url": "http://fixture/model", "model": "m", "key": ""},
        {}, 0, max_rounds=1)
    check("a rewrite with unbalanced quotes is discarded",
          rewritten == "It was not just a blade.")

    # A non-200 reply is retried and then given up on.
    attempts = []

    def post_fail(*a, **k):
        attempts.append(1)
        return _Response(500, "server error")

    generation.requests.post = post_fail
    rewritten, hits = generation.apply_regex_watch(
        "It was not just a blade.", [pattern],
        {"url": "http://fixture/model", "model": "m", "key": ""},
        {}, 0, max_rounds=1, max_attempts_param=2)
    check("a failing endpoint keeps the original", rewritten == "It was not just a blade.")
    check("a failing endpoint is retried", len(attempts) == 2)
    check("the unresolved hit is logged", hits and hits[0][2] == "unresolved")

    # No patterns, no work.
    rewritten, hits = generation.apply_regex_watch(
        "It was not just a blade.", [],
        {"url": "http://fixture/model", "model": "m", "key": ""}, {}, 0)
    check("an empty pattern list is a no-op",
          rewritten == "It was not just a blade." and hits == [])

    # Unconfigured API is a no-op (the turn is kept, never dropped).
    rewritten, hits = generation.apply_regex_watch(
        "It was not just a blade.", [pattern], {"url": "", "model": "", "key": ""}, {}, 0)
    check("an unconfigured API keeps the text", rewritten == "It was not just a blade.")


def test_rounds_are_bounded():
    generation, app_state = _load_generation_with_stubs()
    generation.check_circuit_breaker = lambda _slot: True
    generation.record_api_success = lambda _slot: None
    generation.record_api_failure = lambda _slot: None
    generation.update_live_prompt_preview = lambda *a, **k: None
    generation.apply_bad_words_to_payload = lambda payload, _t: payload
    generation.sanitize_payload_for_endpoint = lambda payload, _u, _s: payload
    generation.global_rate_limiter = types.SimpleNamespace(wait_if_needed=lambda _slot: None)

    # Two shapes in one passage: each round fixes the first remaining hit, so two rounds are
    # needed to clean it and one round leaves the second hit unresolved.
    patterns = [r"\bnot\s+just\s+a\s+\w+", r"\bmore\s+than\s+just\s+a\s+\w+"]
    text = "It was not just a blade, and it was more than just a sword."
    calls = []

    def post_second_shape(url, headers=None, data=None, timeout=None):
        calls.append(1)
        # Round 1 removes the first shape and leaves the second one; round 2 clears it.
        return _Response(200, "It was more than just a sword." if len(calls) == 1
                         else "It was simply a blade.")

    generation.requests.post = post_second_shape
    rewritten, hits = generation.apply_regex_watch(
        text, patterns, {"url": "http://fixture/model", "model": "m", "key": ""},
        {}, 0, max_rounds=2)
    check("a second shape introduced by the first rewrite is caught in the same pass",
          rewritten == "It was simply a blade." and len(calls) == 2)
    check("both hits are reported as rewritten",
          len(hits) == 2 and all(h[2] == "rewritten" for h in hits))

    calls = []
    generation.requests.post = post_second_shape
    rewritten, hits = generation.apply_regex_watch(
        text, patterns, {"url": "http://fixture/model", "model": "m", "key": ""},
        {}, 0, max_rounds=1)
    check("the rewrite loop is bounded by max_rounds", len(calls) == 1)
    check("a hit left over when the rounds run out is reported unresolved",
          any(h[2] == "unresolved" for h in hits))


def test_scotoma_tic_patterns():
    """v11.7: the two scotoma tic shapes (denial-then-correction, absolute trailer)
    shipped as defaults -- they compile through the real validator, fire on the corpus
    shapes, and stay quiet on clean prose."""
    import yaml
    with open(REPO / "config" / "config.yml", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    shipped = list(config["detection"]["regex_watch"]["patterns"])
    check("defaults now ship eight patterns (six v11.6 + two scotoma tics)",
          len(detection.DEFAULT_REGEX_WATCH_PATTERNS) == 8 and len(shipped) == 8)
    denial = shipped[6]
    trailer = shipped[7]
    # The rewrite-side pattern and the config entry are the same string in both copies.
    check("config and DEFAULT_REGEX_WATCH_PATTERNS hold the same two scotoma entries",
          denial in detection.DEFAULT_REGEX_WATCH_PATTERNS
          and trailer in detection.DEFAULT_REGEX_WATCH_PATTERNS)

    for text in ["It wasn't anger. It was fear.",
                 "She didn't run. She walked, slow and steady."]:
        found, _info = detection.is_regex_watch(text, [denial], record_timestamp=False)
        check("denial pattern matches: %r" % text, found)
    # The pattern targets the contracted 'n't form; an uncontracted "would not" is out of
    # scope by construction (recall-first regex approximation, per the pattern comment).
    found, _info = detection.is_regex_watch(
        "He would not leave. Instead he turned back to the door.", [denial], record_timestamp=False)
    check("an uncontracted negation is not a denial-pattern match", not found)
    for text in ['She turned toward the door, her lips parted.',
                 'The street went quiet, the neon bleeding red.']:
        found, _info = detection.is_regex_watch(text, [trailer], record_timestamp=False)
        check("trailer pattern matches: %r" % text, found)
    # Note: the rewrite-side pattern is deliberately recall-first -- rejecting a negated
    # correction tail ("She didn't move. She didn't speak." is a RUN, not a couplet) happens
    # in style_metrics.denial_correction_hits, which tests/test_style_metrics.py asserts;
    # here the LLM rewrite pass gets to judge each regex candidate instead.
    # And the whole shipped default set still stays quiet on clean prose.
    for text in ["The ship sailed at dawn.", "Nothing happened that night.",
                 "She nodded once and left the room."]:
        found, info = detection.is_regex_watch(text, shipped, record_timestamp=False)
        check("shipped defaults all stay quiet: %r" % text, not found and info == [])


def test_config_defaults():
    import yaml
    with open(REPO / "config" / "config.yml", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    block = config["detection"]["regex_watch"]
    check("regex_watch is off by default", block["enabled"] is False)
    check("regex_watch has a rounds setting", 1 <= int(block["max_rounds"]) <= 5)
    check("regex_watch ships patterns", len(block["patterns"]) >= 4)
    valid, errors = detection.validate_regex_patterns(block["patterns"])
    check("every shipped config pattern compiles", errors == [])
    check("the shipped config patterns are kept", len(valid) == len(block["patterns"]))


def main():
    test_validation()
    test_default_patterns_compile()
    test_matching()
    test_match_reporting()
    test_zero_width_and_invalid_are_skipped()
    test_case_insensitive()
    test_sentence_cap()
    test_timestamp_bookkeeping()
    test_rewrite_pass()
    test_rounds_are_bounded()
    test_scotoma_tic_patterns()
    test_config_defaults()
    if FAILURES:
        print("\n%d FAILED check(s):" % len(FAILURES))
        for name in FAILURES:
            print("  - %s" % name)
        return 1
    print("\nAll Regex Watch checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
