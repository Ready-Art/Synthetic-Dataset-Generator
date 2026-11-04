# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

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
    print("(4) invalid JSON: one format-repair round, then a final drop (never a transient retry)")
    calls = []

    def handler(url, headers=None, data=None, timeout=None):
        calls.append(1)
        return _Resp("Sure! Here is my evaluation: the writing is lovely.")

    real = _stub(handler)
    try:
        # v11.5: a format-repair round re-asks the model once for JSON-only output; the
        # evaluation itself is still not retried as a transient failure.
        set_config(judge_config())
        result = quality.score_conversation(CONV, "t-badjson", 1)
        check("invalid JSON gets exactly one format-repair round", len(calls) == 2)
        check("conversation is dropped", result.get("dropped") is True)
        check("drop reason is invalid_json", result.get("drop_reason") == "invalid_json")
        check("llm_invalid_json flag recorded", "llm_invalid_json" in (result.get("flags") or []))

        # Repair disabled -> v11.4 behaviour: a single attempt, immediate drop.
        calls[:] = []
        set_config(judge_config(judge_json_repair_attempts=0))
        result = quality.score_conversation(CONV, "t-badjson0", 1)
        check("judge_json_repair_attempts=0 restores the immediate drop", len(calls) == 1)
        check("still dropped as invalid_json", result.get("drop_reason") == "invalid_json")
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


def test_judge_concurrency_defaults_and_clamping():
    print("(9) quality.judge_concurrency is clamped and defaults to 1")
    set_config(judge_config())
    check("absent key -> 1 (a default run is unchanged)", quality.judge_concurrency() == 1)
    set_config(judge_config(judge_concurrency=3))
    check("configured value is used", quality.judge_concurrency() == 3)
    set_config(judge_config(judge_concurrency=0))
    check("below the floor -> 1", quality.judge_concurrency() == 1)
    set_config(judge_config(judge_concurrency=-5))
    check("negative -> 1", quality.judge_concurrency() == 1)
    set_config(judge_config(judge_concurrency=999))
    check("above the ceiling -> 32", quality.judge_concurrency() == 32)
    set_config(judge_config(judge_concurrency="garbage"))
    check("unparsable -> 1 (a bad config value never breaks a run)",
          quality.judge_concurrency() == 1)
    set_config(judge_config(judge_concurrency="4"))
    check("string form is accepted", quality.judge_concurrency() == 4)


def test_concurrent_processors_serve_jobs_in_parallel():
    print("(10) judge_concurrency N runs N judge requests at once")
    import threading as _threading

    # The judge is stubbed so that each request blocks until every expected request has
    # arrived. A single-threaded pool can never satisfy that: the first request would wait
    # forever. So this both proves the pool is parallel and proves the queue hands each job
    # to a different thread.
    expected = 3
    arrived = _threading.Semaphore(0)
    all_arrived = _threading.Event()
    counter = {"n": 0}
    counter_lock = _threading.Lock()

    def handler(url, headers=None, data=None, timeout=None):
        with counter_lock:
            counter["n"] += 1
            if counter["n"] >= expected:
                all_arrived.set()
        arrived.release()
        all_arrived.wait(timeout=10)
        return _Resp(json.dumps(GOOD_SCORES))

    set_config(judge_config(judge_concurrency=expected))
    app_state.quality_scores.clear()
    app_state.quality_dropped.clear()
    app_state.dropped_task_ids.clear()
    quality._quality_processor_running = False
    quality._quality_processor_threads = []

    real = _stub(handler)
    quality.initialize_quality_queue()
    try:
        check("pool size follows the config",
              len(quality._quality_processor_threads) == expected)
        results = {}

        def enqueue(task_id):
            results[task_id] = quality.enqueue_quality_scoring(
                CONV, task_id, 1, 0, "", "", None, None, timeout=30)

        waiters = [_threading.Thread(target=enqueue, args=(f"t-conc-{i}",))
                   for i in range(expected)]
        for w in waiters:
            w.start()
        for w in waiters:
            w.join(timeout=30)

        check("every waiter got a verdict", len(results) == expected)
        check("no waiter timed out",
              all(not r.get("dropped") for r in results.values()))
        check("all requests reached the judge at the same time (truly parallel)",
              all_arrived.is_set())
        check("every job was scored",
              all(f"t-conc-{i}" in app_state.quality_scores for i in range(expected)))
    finally:
        quality.shutdown_quality_queue()
        quality.requests.post = real


def test_single_processor_pool_is_still_serial():
    print("(11) judge_concurrency 1 keeps the old one-at-a-time behaviour")
    peak = {"now": 0, "max": 0}
    peak_lock = __import__("threading").Lock()

    def handler(url, headers=None, data=None, timeout=None):
        import threading as _t
        with peak_lock:
            peak["now"] += 1
            peak["max"] = max(peak["max"], peak["now"])
        try:
            _t.Event().wait(0.05)
            return _Resp(json.dumps(GOOD_SCORES))
        finally:
            with peak_lock:
                peak["now"] -= 1

    set_config(judge_config())  # no judge_concurrency -> 1
    app_state.quality_scores.clear()
    app_state.quality_dropped.clear()
    app_state.dropped_task_ids.clear()
    quality._quality_processor_running = False
    quality._quality_processor_threads = []

    real = _stub(handler)
    quality.initialize_quality_queue()
    try:
        import threading as _t
        check("one processor thread", len(quality._quality_processor_threads) == 1)
        waiters = [_t.Thread(target=quality.enqueue_quality_scoring,
                             args=(CONV, f"t-serial-{i}", 1, 0, "", "", None, None),
                             kwargs={"timeout": 30})
                   for i in range(3)]
        for w in waiters:
            w.start()
        for w in waiters:
            w.join(timeout=30)
        check("only one judge request was ever in flight", peak["max"] == 1)
    finally:
        quality.shutdown_quality_queue()
        quality.requests.post = real


def test_dedup_keeps_the_newest_verdict():
    print("(12) a repeated task_id keeps the NEWEST verdict, not the first (v12.9)")
    app_state.quality_scores.clear()
    app_state.quality_dropped.clear()
    app_state.dropped_task_ids.clear()

    first = {"composite": 10, "dimensions": {}, "flags": [], "method": "hybrid",
             "scored_at": "t1"}
    second = {"composite": 90, "dimensions": {}, "flags": [], "method": "hybrid",
              "scored_at": "t2"}

    quality._record_quality_result(first, "t-dedup", 1, "test")
    quality._record_quality_result(second, "t-dedup", 1, "test")
    check("the newest verdict wins",
          app_state.quality_scores["t-dedup"]["composite"] == 90)

    # And the other way round: a better score must not survive a later, worse one,
    # because in duplication mode each row carries the verdict for ITS turn.
    quality._record_quality_result(second, "t-dedup2", 1, "test")
    quality._record_quality_result(first, "t-dedup2", 1, "test")
    check("a later verdict replaces an earlier one (not max)",
          app_state.quality_scores["t-dedup2"]["composite"] == 10)


def test_check_logic_goes_through_the_queue():
    print("(13) check_logic uses the shared queue, not the judge directly (v12.9)")
    set_config(judge_config())
    app_state.quality_scores.clear()
    app_state.quality_dropped.clear()
    app_state.dropped_task_ids.clear()
    quality._quality_processor_running = False
    quality._quality_processor_threads = []

    # Record WHICH THREAD ran the judge. The processor must run it; the calling
    # worker must not, because a direct call from the worker escapes the limit.
    import threading as _t
    call_threads = []
    real_direct = quality._call_logic_judge

    def spy_direct(*args, **kwargs):
        call_threads.append(_t.current_thread().name)
        return real_direct(*args, **kwargs)

    def handler(url, headers=None, data=None, timeout=None):
        return _Resp(json.dumps({"logic": 95, "logic_evidence": [],
                                 "rewrite_instruction": ""}))

    real = _stub(handler)
    quality.initialize_quality_queue()
    try:
        # While the queue runs, check_logic must NOT call the judge directly from
        # the worker thread -- it must hand the request to the queue.
        quality._call_logic_judge = spy_direct
        result = quality.check_logic(
            "She sets the loupe down.",
            [{"role": "user", "content": "Show me the chronometer."},
             {"role": "assistant", "content": "Not for sale."}],
            character_profile_text="", turn_number=2,
            task_id="t-logic-q", thread_id=1, file_name="f.txt")
        check("check_logic succeeded through the queue", result.get("success") is True)
        check("check_logic read the score", result.get("score") == 95)
        check("the judge ran", call_threads)
        check("the judge ran on a processor thread, not the worker",
              all(n.startswith("QualityScoringProcessor") for n in call_threads))
        check("the queue is empty again", quality._quality_scoring_queue.qsize() == 0)
    finally:
        quality._call_logic_judge = real_direct
        quality.shutdown_quality_queue()
        quality.requests.post = real
        quality._quality_processor_running = False
        quality._quality_processor_threads = []


def main():
    test_transient_failure_retries_three_times_then_drops()
    test_retry_count_is_configurable()
    test_recovers_on_a_later_attempt()
    test_invalid_json_is_final_and_drops()
    test_logic_zero_is_final_and_drops()
    test_unconfigured_judge_keeps_heuristics()
    test_dropped_results_are_not_stored()
    test_worker_skips_the_write_for_a_dropped_row()
    test_judge_concurrency_defaults_and_clamping()
    test_concurrent_processors_serve_jobs_in_parallel()
    test_single_processor_pool_is_still_serial()
    test_dedup_keeps_the_newest_verdict()
    test_check_logic_goes_through_the_queue()
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
