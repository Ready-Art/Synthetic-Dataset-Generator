"""Tests for the v11.5 judge JSON-robustness layer in quality.py.

Run separately: ``python -B tests/test_quality_json_robustness.py``. No model, network or
display is used: the judge is exercised through a stubbed ``requests.post``.

The point of the layer is that a small local judge (Qwen 3.5 4B and friends) usually gets the
*evaluation* right and only the *packaging* wrong. A wrongly packaged reply must be recovered
instead of dropping the conversation, and only genuinely unparsable output may drop a row.

Covers:
  * ``_extract_json_object`` on the packaging failures seen in practice: a thinking block
    before the object, prose before/after, ``` fences anywhere, an object inside a nested
    code fence, trailing commas, single/smart quotes, Python literals, ``"85/100"`` strings,
    and a cut-off tail
  * a candidate object that actually carries scores wins over unrelated ``{...}`` prose
  * numeric salvage requires EVERY expected dimension (a fragment is not an evaluation)
  * the format-repair round recovers a real verdict (row kept, no drop)
  * the format-repair round is bounded and configurable (``quality.judge_json_repair_attempts``)
  * a truncated reply (finish_reason == "length") is transient and retried with a doubled
    token budget
  * ``quality.judge_structured_output`` sends response_format, and a server that rejects it
    is retried once without it and remembered for the rest of the run
  * an unreadable envelope / empty reply is transient, not a JSON failure
  * the judge request timeout defaults to 120s and honours both the new and legacy config keys
  * v11.8: the vLLM ``priority`` field is opt-in (``quality.judge_priority_enabled``), is
    never sent when the toggle is off, defaults to 0, is clamped to -10..10 (-10 is the HIGHEST
    priority, 10 the lowest), and rides on every attempt (format-repair and response_format
    fallback included)
"""

import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.mkdtemp(prefix="quality_json_robustness_")
os.chdir(_TMP)
os.makedirs("config", exist_ok=True)
with open("config/config.yml", "w", encoding="utf-8") as f:
    f.write("api:\n  apis: []\n")

import app_state            # noqa: E402
import quality              # noqa: E402
from app_state import global_config  # noqa: E402

FAILURES = []

CONV = [
    {"role": "user", "content": "Tell me about the chronometer."},
    {"role": "assistant", "content": "Melody sets her loupe down. \u201cThat one is not for sale.\u201d"},
]

DIMS = list(quality.LLM_DIMENSIONS)

GOOD = {"coherence": 80, "naturalness": 78, "engagement": 70,
        "consistency": 75, "logic": 88, "notes": "fine"}


def check(label, ok):
    print(("  ok   " if ok else "  FAIL ") + label)
    if not ok:
        FAILURES.append(label)


def set_config(cfg):
    global_config.config = cfg


def judge_config(**extra):
    cfg = {
        "quality": {
            "use_llm_scoring": True,
            "judge_retry_delay": 0,
            "judge_structured_output": "off",
            "scoring_api": {"url": "http://judge.invalid/v1/chat/completions",
                            "model": "judge-model"},
        }
    }
    cfg["quality"].update(extra)
    return cfg


class _Resp:
    def __init__(self, content, status_code=200, finish_reason="stop", text=None, raw_json=None):
        self.status_code = status_code
        self._content = content
        self._finish_reason = finish_reason
        self.text = text if text is not None else (content or "")
        self._raw_json = raw_json

    def json(self):
        if self._raw_json is not None:
            return self._raw_json
        return {"choices": [{"message": {"content": self._content},
                             "finish_reason": self._finish_reason}]}


def _stub(handler):
    original = quality.requests.post
    quality.requests.post = handler
    return original


def test_extraction_ladder():
    print("(1) _extract_json_object recovers the reply from real packaging failures")
    body = json.dumps(GOOD)

    cases = [
        ("bare object", body, 80),
        ("thinking block before the object",
         "<" + "think>Let me weigh the turns... {maybe}<" + "/think>\n" + body, 80),
        ("prose before and after",
         "Sure, here is my evaluation:\n" + body + "\nHope that helps!", 80),
        ("fenced anywhere", "Here you go:\n```json\n" + body + "\n```\nDone.", 80),
        ("trailing comma", body.replace('"logic": 88', '"logic": 88,'), 80),
        ("smart quotes", body.replace('"', '\u201d'), 80),
        ("string scores", json.dumps(dict(GOOD, coherence="85/100", logic="ninety")), 85),
        ("quoted number string", json.dumps(dict(GOOD, coherence="85")), 85),
        ("python literals", json.dumps(dict(GOOD)).replace("true", "True"), 80),
        ("cut-off tail", body[:-60], 80),
        ("cut off mid-key", '{"coherence": 80, "naturalness": 78, "engag', 80),
    ]
    for label, text, expected_coherence in cases:
        parsed, how = quality._extract_json_object(text, preferred_keys=DIMS)
        check("%s -> parsed (%s)" % (label, how), parsed is not None)
        if parsed:
            check("%s -> scores coerced" % label,
                  quality._coerce_score(parsed.get("coherence", 0)) == expected_coherence)

    # An unrelated {...} in the prose must not beat the object that carries the scores.
    decoy = 'The rubric {"not": "scores"} is below.\n' + body
    parsed, _how = quality._extract_json_object(decoy, preferred_keys=DIMS)
    check("candidate carrying scores wins over a decoy object",
          parsed is not None and parsed.get("logic") == 88)

    parsed, how = quality._extract_json_object("no json here at all", preferred_keys=DIMS)
    check("genuinely unparsable text stays unparsable", parsed is None and how == "unparseable")

    parsed, how = quality._extract_json_object(GOOD, preferred_keys=DIMS)
    check("an already-parsed dict is accepted", parsed == GOOD and how == "object")


def test_salvage_requires_every_dimension():
    print("(2) numeric salvage needs EVERY expected dimension")
    complete = "Scores: " + ", ".join('"%s": %d' % (d, 70) for d in DIMS)
    salvaged = quality._salvage_scores_from_text(complete, DIMS)
    check("all dimensions present -> salvage succeeds", set(salvaged) == set(DIMS))

    partial = 'Scores: "coherence": 70, "naturalness": 70'
    check("a fragment is not mistaken for an evaluation",
          quality._salvage_scores_from_text(partial, DIMS) == {})


def test_coercion():
    print("(3) score coercion tolerates prose and strings")
    check("int passes through", quality._coerce_score(88) == 88.0)
    check("numeric string", quality._coerce_score("85") == 85.0)
    check("'85/100' style", quality._coerce_score("85/100") == 85.0)
    check("prose around a number", quality._coerce_score("about 72 out of 100") == 72.0)
    check("unusable value falls back", quality._coerce_score("ninety") == 50.0)
    check("None falls back", quality._coerce_score(None) == 50.0)
    check("bool is not a score", quality._coerce_score(True) == 50.0)


def test_messy_but_recoverable_reply_is_kept():
    print("(4) a messy-but-recoverable reply keeps the conversation (no drop)")
    calls = []

    def handler(url, headers=None, data=None, timeout=None):
        calls.append(1)
        return _Resp("<think>weighing...</think>\nHere you go:\n```json\n"
                     + json.dumps(GOOD) + "\n```")

    real = _stub(handler)
    try:
        set_config(judge_config())
        result = quality.score_conversation(CONV, "t-messy", 1)
        check("conversation is kept", not result.get("dropped"))
        check("hybrid method", result.get("method") == "hybrid")
        check("judge values used", result["dimensions"]["logic"] == 88)
        check("only one judge call needed", len(calls) == 1)
    finally:
        quality.requests.post = real


def test_format_repair_round_recovers():
    print("(5) format-repair round recovers the evaluation instead of dropping the row")
    calls = []

    def handler(url, headers=None, data=None, timeout=None):
        calls.append(json.loads(data))
        if len(calls) == 1:
            return _Resp("I would say the writing is strong overall.")
        return _Resp(json.dumps(GOOD))

    real = _stub(handler)
    try:
        set_config(judge_config())
        result = quality.score_conversation(CONV, "t-repair", 1)
        check("two judge calls (original + repair)", len(calls) == 2)
        check("repair request carries the model's own broken text",
              any(m.get("role") == "assistant" for m in calls[1]["messages"]))
        check("repair request asks for JSON only",
              "ONLY the JSON object" in calls[1]["messages"][-1]["content"])
        check("conversation is kept", not result.get("dropped"))
        check("recovered judge values are used", result["dimensions"]["logic"] == 88)
    finally:
        quality.requests.post = real


def test_format_repair_is_bounded():
    print("(6) format repair is bounded and configurable")
    for rounds, expected_calls in ((0, 1), (1, 2), (2, 3), (9, 4)):
        calls = []

        def handler(url, headers=None, data=None, timeout=None):
            calls.append(1)
            return _Resp("still not json")

        real = _stub(handler)
        try:
            set_config(judge_config(judge_json_repair_attempts=rounds))
            result = quality.score_conversation(CONV, "t-bounded-%d" % rounds, 1)
            check("judge_json_repair_attempts=%d -> %d call(s)" % (rounds, expected_calls),
                  len(calls) == expected_calls)
            check("judge_json_repair_attempts=%d still drops" % rounds,
                  result.get("drop_reason") == "invalid_json")
        finally:
            quality.requests.post = real


def test_truncated_reply_retries_with_bigger_budget():
    print("(7) token-limited replies: salvage what was written, else retry with a bigger budget")

    # (a) Cut off mid-key, but the scores were already written: salvage, no extra API call.
    calls = []

    def salvageable(url, headers=None, data=None, timeout=None):
        calls.append(json.loads(data))
        return _Resp('{"coherence": 80, "naturalness": 78, "engag', finish_reason="length")

    real = _stub(salvageable)
    try:
        set_config(judge_config(judge_max_attempts=2))
        result = quality.score_conversation(CONV, "t-trunc-salvage", 1)
        check("a cut-off reply is salvaged without another API call", len(calls) == 1)
        check("salvaged conversation is kept", not result.get("dropped"))
        check("salvaged scores are used", result["dimensions"]["coherence"] == 80)
    finally:
        quality.requests.post = real

    # (b) Nothing usable was written before the cut-off: transient, retried with more tokens.
    calls = []

    def unusable(url, headers=None, data=None, timeout=None):
        calls.append(json.loads(data))
        if len(calls) == 1:
            return _Resp('{"coh', finish_reason="length")
        return _Resp(json.dumps(GOOD))

    real = _stub(unusable)
    try:
        set_config(judge_config(judge_max_attempts=2))
        result = quality.score_conversation(CONV, "t-trunc", 1)
        check("an unusable cut-off reply is retried", len(calls) == 2)
        check("retry doubles max_tokens",
              calls[1]["max_tokens"] >= 2 * calls[0]["max_tokens"])
        check("conversation is kept once the retry parses", not result.get("dropped"))
    finally:
        quality.requests.post = real


def test_structured_output_and_fallback():
    print("(8) response_format is requested, and a rejecting server falls back cleanly")
    quality._JUDGE_NO_RESPONSE_FORMAT.clear()
    calls = []

    def rejecting(url, headers=None, data=None, timeout=None):
        calls.append(json.loads(data))
        if "response_format" in json.loads(data):
            return _Resp("", status_code=400,
                         text='{"error": "response_format is not supported"}')
        return _Resp(json.dumps(GOOD))

    real = _stub(rejecting)
    try:
        set_config(judge_config(judge_structured_output="auto"))
        result = quality.score_conversation(CONV, "t-fmt", 1)
        check("first request asked for JSON mode",
              calls and calls[0].get("response_format") == {"type": "json_object"})
        check("rejecting server is retried without response_format",
              len(calls) == 2 and "response_format" not in calls[1])
        check("conversation is kept", not result.get("dropped"))
        check("rejecting endpoint is remembered",
              ("http://judge.invalid/v1/chat/completions", "judge-model")
              in quality._JUDGE_NO_RESPONSE_FORMAT)

        calls[:] = []
        quality.score_conversation(CONV, "t-fmt2", 1)
        check("later requests skip response_format for that endpoint",
              calls and "response_format" not in calls[0])

        # json_schema mode builds a real schema.
        set_config(judge_config(judge_structured_output="json_schema"))
        schema = quality._judge_response_format("json_schema", DIMS)
        check("json_schema mode builds a schema",
              schema and schema["type"] == "json_schema"
              and set(DIMS).issubset(schema["json_schema"]["schema"]["properties"]))

        # off mode sends nothing at all.
        check("off mode sends no response_format",
              quality._judge_response_format("off", DIMS) is None)
    finally:
        quality.requests.post = real
        quality._JUDGE_NO_RESPONSE_FORMAT.clear()


def test_request_timeout_is_configurable():
    print("(9b) the judge request timeout is configurable and sane by default")
    captured = {}

    def handler(url, headers=None, data=None, timeout=None):
        captured["timeout"] = timeout
        return _Resp(json.dumps(GOOD))

    real = _stub(handler)
    try:
        set_config(judge_config())
        quality.score_conversation(CONV, "t-timeout-default", 1)
        check("default judge timeout is 120s, not the old hard-coded 60s",
              captured.get("timeout") == 120)

        set_config(judge_config(judge_request_timeout=240))
        quality.score_conversation(CONV, "t-timeout-set", 1)
        check("quality.judge_request_timeout is honoured", captured.get("timeout") == 240)

        # The legacy key still works, and the new key wins when both are present.
        set_config(judge_config(scoring_timeout=90))
        quality.score_conversation(CONV, "t-timeout-legacy", 1)
        check("legacy quality.scoring_timeout is still honoured", captured.get("timeout") == 90)

        set_config(judge_config(scoring_timeout=90, judge_request_timeout=300))
        quality.score_conversation(CONV, "t-timeout-both", 1)
        check("judge_request_timeout wins over the legacy key", captured.get("timeout") == 300)

        set_config(judge_config(judge_request_timeout=0))
        quality.score_conversation(CONV, "t-timeout-clamp", 1)
        check("out-of-range timeout is clamped, not sent as 0", captured.get("timeout") == 5)
    finally:
        quality.requests.post = real


def test_judge_priority_field():
    print("(10) vLLM judge priority is opt-in, defaults to 0 and is clamped to -10..10")
    captured = []

    def handler(url, headers=None, data=None, timeout=None):
        captured.append(json.loads(data))
        return _Resp(json.dumps(GOOD))

    real = _stub(handler)
    try:
        # Off by default: the field must not be sent at all, so backends that do not know it
        # (OpenAI, Anthropic, llama.cpp) see a payload identical to v11.7.
        set_config(judge_config())
        quality.score_conversation(CONV, "t-prio-off", 1)
        check("priority is absent when the toggle is off",
              captured and "priority" not in captured[0])

        # Enabled with the default value: 0 is accepted by every vLLM server.
        captured[:] = []
        set_config(judge_config(judge_priority_enabled=True))
        quality.score_conversation(CONV, "t-prio-default", 1)
        check("priority defaults to 0", captured and captured[0].get("priority") == 0)

        # A real value is forwarded as an integer.
        captured[:] = []
        set_config(judge_config(judge_priority_enabled=True, judge_priority=3))
        quality.score_conversation(CONV, "t-prio-set", 1)
        check("configured priority is sent", captured and captured[0].get("priority") == 3)

        # -10 is the HIGHEST priority (vLLM: lower is handled earlier), so it must pass through
        # unmodified -- clamping it up to 0 would silently demote the user's top-priority judge.
        captured[:] = []
        set_config(judge_config(judge_priority_enabled=True, judge_priority=-10))
        quality.score_conversation(CONV, "t-prio-highest", 1)
        check("-10 (highest priority) is sent as-is",
              captured and captured[0].get("priority") == -10)

        # +10 is the LOWEST priority, and must survive just as cleanly.
        captured[:] = []
        set_config(judge_config(judge_priority_enabled=True, judge_priority=10))
        quality.score_conversation(CONV, "t-prio-lowest", 1)
        check("10 (lowest priority) is sent as-is",
              captured and captured[0].get("priority") == 10)

        # Both directions of the range are usable, not just the positive half.
        for negative in (-10, -7, -3, -1):
            captured[:] = []
            set_config(judge_config(judge_priority_enabled=True, judge_priority=negative))
            quality.score_conversation(CONV, "t-prio-neg-%d" % negative, 1)
            check("negative priority %d is forwarded unmodified" % negative,
                  captured and captured[0].get("priority") == negative)

        # Out-of-range values are clamped, never sent raw (a server rejects them outright).
        for raw, expected in ((-99, -10), (-10, -10), (-1, -1), (0, 0), (1, 1), (10, 10),
                              (11, 10), (99, 10), ("-4", -4), ("7", 7), ("nonsense", 0)):
            captured[:] = []
            set_config(judge_config(judge_priority_enabled=True, judge_priority=raw))
            quality.score_conversation(CONV, "t-prio-clamp-%s" % raw, 1)
            check("priority %r is clamped to %d" % (raw, expected),
                  captured and captured[0].get("priority") == expected)

        # The toggle accepts string forms so a hand-edited config.yml works.
        for raw, expected in (("true", True), ("yes", True), ("0", False), (False, False)):
            set_config(judge_config(judge_priority_enabled=raw, judge_priority=5))
            check("judge_priority_enabled=%r -> %r" % (raw, expected),
                  quality.judge_priority_enabled() is expected)
    finally:
        quality.requests.post = real

    # The field rides on the retry and format-repair attempts too, so a retried judge
    # request keeps its place in the vLLM queue.
    calls = []

    def flaky(url, headers=None, data=None, timeout=None):
        calls.append(json.loads(data))
        if len(calls) == 1:
            return _Resp("not json at all")
        return _Resp(json.dumps(GOOD))

    real = _stub(flaky)
    try:
        set_config(judge_config(judge_priority_enabled=True, judge_priority=7))
        result = quality.score_conversation(CONV, "t-prio-repair", 1)
        check("two judge calls", len(calls) == 2)
        check("the format-repair request keeps the priority",
              all(c.get("priority") == 7 for c in calls))
        check("conversation is kept", not result.get("dropped"))
    finally:
        quality.requests.post = real

    # The response_format fallback rebuilds the body from payload_dict, so priority survives it.
    quality._JUDGE_NO_RESPONSE_FORMAT.clear()
    calls = []

    def rejecting(url, headers=None, data=None, timeout=None):
        body = json.loads(data)
        calls.append(body)
        if "response_format" in body:
            return _Resp("", status_code=400,
                         text='{"error": "response_format is not supported"}')
        return _Resp(json.dumps(GOOD))

    real = _stub(rejecting)
    try:
        set_config(judge_config(judge_structured_output="auto",
                                judge_priority_enabled=True, judge_priority=3))
        quality.score_conversation(CONV, "t-prio-fmt", 1)
        check("priority survives the response_format fallback",
              len(calls) == 2 and all(c.get("priority") == 3 for c in calls)
              and "response_format" not in calls[1])
    finally:
        quality.requests.post = real
        quality._JUDGE_NO_RESPONSE_FORMAT.clear()


def test_bad_envelope_is_transient():
    print("(9) an unreadable envelope / empty reply is transient, not invalid JSON")
    calls = []

    def handler(url, headers=None, data=None, timeout=None):
        calls.append(1)
        if len(calls) == 1:
            return _Resp(None, raw_json={"unexpected": True})
        return _Resp(json.dumps(GOOD))

    real = _stub(handler)
    try:
        set_config(judge_config(judge_max_attempts=2))
        result = quality.score_conversation(CONV, "t-envelope", 1)
        check("unreadable envelope is retried", len(calls) == 2)
        check("conversation is kept", not result.get("dropped"))
    finally:
        quality.requests.post = real

    calls[:] = []

    def empty_handler(url, headers=None, data=None, timeout=None):
        calls.append(1)
        if len(calls) == 1:
            return _Resp("   ")
        return _Resp(json.dumps(GOOD))

    real = _stub(empty_handler)
    try:
        set_config(judge_config(judge_max_attempts=2))
        result = quality.score_conversation(CONV, "t-empty", 1)
        check("empty reply is retried, not treated as invalid JSON", len(calls) == 2)
        check("conversation is kept", not result.get("dropped"))
    finally:
        quality.requests.post = real


def main():
    test_extraction_ladder()
    test_salvage_requires_every_dimension()
    test_coercion()
    test_messy_but_recoverable_reply_is_kept()
    test_format_repair_round_recovers()
    test_format_repair_is_bounded()
    test_truncated_reply_retries_with_bigger_budget()
    test_structured_output_and_fallback()
    test_request_timeout_is_configurable()
    test_judge_priority_field()
    test_bad_envelope_is_transient()
    print()
    if FAILURES:
        print("{} judge JSON robustness test(s) failed".format(len(FAILURES)))
        sys.exit(1)
    print("all judge JSON robustness tests passed")


if __name__ == "__main__":
    try:
        main()
    finally:
        os.chdir("/")
        shutil.rmtree(_TMP, ignore_errors=True)
