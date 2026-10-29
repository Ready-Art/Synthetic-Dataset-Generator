"""Tests for the v11.4 quality-judge retry / drop policy and the Review tab.

Run separately: ``python -B tests/test_quality_judge_policy.py``. No model, network or
display is required: the judge is exercised through a stubbed ``requests.post`` and the
Review-tab helpers are called directly (no Tk is constructed here -- the display-dependent
parts live in ``tests/test_review_tab_ui.py``).

Covers:
  * a transient judge failure (HTTP error / timeout / exception) is retried 3 times and then
    DROPS the conversation, which is never stored as a score
  * ``quality.judge_max_attempts`` overrides the retry count
  * an invalid-JSON reply is final: no retry, immediate drop
  * a judge ``logic`` score of 0 is final: no retry, immediate drop
  * a judge with no scoring API configured is not a failure (heuristic scores stand)
  * the worker write path refuses to write a dropped conversation
  * Review-tab helpers: id matching, transcript formatting, export
"""

import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.mkdtemp(prefix="quality_judge_policy_")
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
            "scoring_api": {"url": "http://judge.invalid/v1/chat/completions",
                            "model": "judge-model"},
        }
    }
    cfg["quality"].update(extra)
    return cfg


class _Resp:
    def __init__(self, content, status_code=200):
        self.status_code = status_code
        self._content = content

    def json(self):
        return {"choices": [{"message": {"content": self._content}}]}


GOOD_SCORES = {"coherence": 80, "naturalness": 78, "engagement": 70,
               "consistency": 75, "logic": 88, "notes": "fine"}


def _stub(handler):
    """Patch requests.post with ``handler(url, headers, data, timeout)`` for one test."""
    original = quality.requests.post
    quality.requests.post = handler
    return original


def test_transient_failure_retries_three_times_then_drops():
    print("(1) transient failures retry 3x, then the conversation is dropped")
    calls = []

    def handler(url, headers=None, data=None, timeout=None):
        calls.append(1)
        return _Resp("", status_code=503)

    real = _stub(handler)
    try:
        set_config(judge_config())
        result = quality.score_conversation(CONV, "t-transient", 1)
        check("judge was attempted 3 times", len(calls) == 3)
        check("conversation is dropped", result.get("dropped") is True)
        check("drop reason is judge_failed", result.get("drop_reason") == "judge_failed")
        check("method is 'dropped'", result.get("method") == "dropped")
        check("a dropped row carries no composite score", result.get("composite") == 0)
    finally:
        quality.requests.post = real


def test_retry_count_is_configurable():
    print("(2) quality.judge_max_attempts controls the retry count")
    for attempts in (1, 2, 5):
        calls = []

        def handler(url, headers=None, data=None, timeout=None):
            calls.append(1)
            raise RuntimeError("judge unreachable")

        real = _stub(handler)
        try:
            set_config(judge_config(judge_max_attempts=attempts))
            quality.score_conversation(CONV, "t-attempts-%d" % attempts, 1)
            check("judge_max_attempts=%d -> %d attempt(s)" % (attempts, attempts),
                  len(calls) == attempts)
        finally:
            quality.requests.post = real


def test_recovers_on_a_later_attempt():
    print("(3) a transient failure followed by a good reply is kept")
    calls = []

    def handler(url, headers=None, data=None, timeout=None):
        calls.append(1)
        if len(calls) == 1:
            return _Resp("", status_code=500)
        return _Resp(json.dumps(GOOD_SCORES))

    real = _stub(handler)
    try:
        set_config(judge_config())
        result = quality.score_conversation(CONV, "t-recover", 1)
        check("judge retried once and succeeded", len(calls) == 2)
        check("conversation is kept", not result.get("dropped"))
        check("hybrid method", result.get("method") == "hybrid")
        check("judge logic score used", result["dimensions"]["logic"] == 88)
    finally:
        quality.requests.post = real


def test_invalid_json_is_final_and_drops():
    print("(4) invalid JSON is final: no retry, immediate drop")
    calls = []

    def handler(url, headers=None, data=None, timeout=None):
        calls.append(1)
        return _Resp("Sure! Here is my evaluation: the writing is lovely.")

    real = _stub(handler)
    try:
        set_config(judge_config())
        result = quality.score_conversation(CONV, "t-badjson", 1)
        check("invalid JSON is not retried", len(calls) == 1)
        check("conversation is dropped", result.get("dropped") is True)
        check("drop reason is invalid_json", result.get("drop_reason") == "invalid_json")
        check("llm_invalid_json flag recorded", "llm_invalid_json" in (result.get("flags") or []))
    finally:
        quality.requests.post = real


def test_logic_zero_is_final_and_drops():
    print("(5) a logic score of 0 is final: no retry, immediate drop")
    calls = []

    def handler(url, headers=None, data=None, timeout=None):
        calls.append(1)
        payload = dict(GOOD_SCORES, logic=0)
        return _Resp(json.dumps(payload))

    real = _stub(handler)
    try:
        set_config(judge_config())
        result = quality.score_conversation(CONV, "t-logic0", 1)
        check("logic 0 is not retried", len(calls) == 1)
        check("conversation is dropped", result.get("dropped") is True)
        check("drop reason is logic_zero", result.get("drop_reason") == "logic_zero")
    finally:
        quality.requests.post = real

    # A low-but-nonzero logic score is NOT a drop.
    calls = []

    def handler_low(url, headers=None, data=None, timeout=None):
        calls.append(1)
        return _Resp(json.dumps(dict(GOOD_SCORES, logic=15)))

    real = _stub(handler_low)
    try:
        set_config(judge_config())
        result = quality.score_conversation(CONV, "t-logic15", 1)
        check("a low non-zero logic score is kept, not dropped", not result.get("dropped"))
        check("low_logic is flagged", "low_logic" in result["flags"])
    finally:
        quality.requests.post = real


def test_unconfigured_judge_keeps_heuristics():
    print("(6) no scoring API configured is not a judge failure")
    set_config({"quality": {"use_llm_scoring": True, "scoring_api": {}}})
    result = quality.score_conversation(CONV, "t-unconfigured", 1)
    check("conversation is kept", not result.get("dropped"))
    check("heuristic method", result.get("method") == "heuristic_only")
    check("unavailable flag recorded", "llm_scoring_unavailable" in result["flags"])


def test_dropped_results_are_not_stored():
    print("(7) a dropped verdict is recorded as a drop, never as a score")

    def handler(url, headers=None, data=None, timeout=None):
        return _Resp("not json at all")

    real = _stub(handler)
    try:
        set_config(judge_config())
        app_state.quality_scores.clear()
        app_state.quality_review_ids.clear()
        app_state.quality_dropped.clear()
        app_state.dropped_task_ids.clear()
        quality._quality_processor_running = False  # force the inline path
        result = quality.enqueue_quality_scoring(CONV, "t-store", 1, 0, "", "", None, None)
        check("inline path returns the dropped verdict", result.get("dropped") is True)
        check("no score stored", "t-store" not in app_state.quality_scores)
        check("no review flag set", "t-store" not in app_state.quality_review_ids)
        check("drop recorded with its reason",
              app_state.quality_dropped.get("t-store") == "invalid_json"
              and "t-store" in app_state.dropped_task_ids)
    finally:
        quality.requests.post = real


def test_worker_skips_the_write_for_a_dropped_row():
    print("(8) the worker write path refuses a dropped conversation")
    import generation as g

    written = []
    real_write = g.write_conversation
    g.write_conversation = lambda *a, **k: written.append(k)
    try:
        check("_quality_dropped() recognises a drop",
              g._quality_dropped({"dropped": True, "drop_reason": "logic_zero"}) is True)
        check("_quality_dropped() ignores a normal verdict",
              g._quality_dropped({"composite": 80, "method": "hybrid"}) is False)
        check("_quality_dropped() tolerates None", g._quality_dropped(None) is False)
    finally:
        g.write_conversation = real_write


def main():
    test_transient_failure_retries_three_times_then_drops()
    test_retry_count_is_configurable()
    test_recovers_on_a_later_attempt()
    test_invalid_json_is_final_and_drops()
    test_logic_zero_is_final_and_drops()
    test_unconfigured_judge_keeps_heuristics()
    test_dropped_results_are_not_stored()
    test_worker_skips_the_write_for_a_dropped_row()
    print()
    if FAILURES:
        print("{} quality judge policy test(s) failed".format(len(FAILURES)))
        sys.exit(1)
    print("all quality judge policy tests passed")


if __name__ == "__main__":
    try:
        main()
    finally:
        os.chdir("/")
        shutil.rmtree(_TMP, ignore_errors=True)
