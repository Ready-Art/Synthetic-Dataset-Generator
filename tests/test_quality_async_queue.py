# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Tests for the v12.9 async judge queue (`quality.async_scoring`).

Run separately: ``python -B tests/test_quality_async_queue.py``. No model, network or
display is required: the judge is exercised through a stubbed ``requests.post``.

The guarantee this file exists to protect:

  * generation NEVER waits for a verdict in async mode -- ``enqueue_quality_scoring()``
    returns ``None`` immediately even while the judge is still working, so there is no wait
    to time out of and no work can be lost to ``quality.result_timeout``;
  * a row is written ONLY after its verdict, so a written row is always scored;
  * a conversation the judge REJECTS is never written (it is a deliberate removal, and it is
    recorded with its reason rather than silently vanishing);
  * ``wait_for_quality_queue()`` does not return until every queued job is scored and
    written -- this is what makes "the judge continues until it is finished" true;
  * a job queued while the judge is slow still completes, even though the timeout would
    long since have dropped it in gate mode;
  * a job that cannot be written is COUNTED, not silently lost;
  * a default run (async off) is unchanged: the worker still blocks for its verdict.
"""

import json
import os
import shutil
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.mkdtemp(prefix="quality_async_queue_")
os.chdir(_TMP)
os.makedirs("config", exist_ok=True)
with open("config/config.yml", "w", encoding="utf-8") as f:
    f.write("api:\n  apis: []\n")

import app_state            # noqa: E402
import quality               # noqa: E402
from app_state import global_config  # noqa: E402

FAILURES = []

CONV = [
    {"role": "user", "content": "Tell me about the chronometer."},
    {"role": "assistant", "content": "Melody sets her loupe down. \u201cThat one is not for sale.\u201d"},
]

GOOD_SCORES = {"coherence": 80, "naturalness": 78, "engagement": 70,
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


def _stub(handler):
    original = quality.requests.post
    quality.requests.post = handler
    return original


def _reset_queue():
    """Put the queue back to a known-stopped state between cases."""
    try:
        quality.shutdown_quality_queue()
    except Exception:
        pass
    quality._quality_processor_running = False
    quality._quality_processor_threads = []
    while True:
        try:
            quality._quality_scoring_queue.get_nowait()
        except Exception:
            break
        else:
            quality._quality_scoring_queue.task_done()
    app_state.quality_scores.clear()
    app_state.quality_dropped.clear()
    app_state.dropped_task_ids.clear()
    app_state.quality_write_failures.clear()
    # Mirror generation._install_quality_write_handler(): the writer takes the job's
    # callable and the verdict. generation.py injects this because quality cannot
    # import generation (circular).
    quality.set_quality_write_handler(lambda job, verdict: job(verdict))


def test_async_flag_defaults_off():
    print("(1) async_scoring defaults to off, so a default run is unchanged")
    set_config(judge_config())
    check("absent key -> off", quality.judge_async_enabled() is False)
    set_config(judge_config(async_scoring=True))
    check("configured on", quality.judge_async_enabled() is True)


def test_generation_does_not_wait_for_a_slow_judge():
    print("(2) generation returns immediately while the judge is still working")
    _reset_queue()
    started = threading.Event()
    release = threading.Event()
    written = []
    written_lock = threading.Lock()

    def handler(url, headers=None, data=None, timeout=None):
        started.set()
        release.wait(timeout=20)
        return _Resp(json.dumps(GOOD_SCORES))

    def write_job(verdict):
        with written_lock:
            written.append(verdict)

    set_config(judge_config(async_scoring=True, judge_concurrency=1))
    real = _stub(handler)
    quality.initialize_quality_queue()
    try:
        t0 = time.time()
        result = quality.enqueue_quality_scoring(
            CONV, "t-async-1", 1, 0, "", "", None, None, write_job=write_job)
        elapsed = time.time() - t0
        check("enqueue returned None (the worker must not write)", result is None)
        check("enqueue did not block on the judge (< 2s, took %.2fs)" % elapsed,
              elapsed < 2.0)
        check("the judge did start on the job", started.wait(timeout=10))
        check("nothing was written before the verdict", not written)

        release.set()
        drained = quality.wait_for_quality_queue()
        check("the drain waited for the verdict", drained is True)
        check("the row was written after the verdict", len(written) == 1)
        check("the written row carries its verdict",
              written and written[0].get("composite", 0) > 0)
        check("the verdict is recorded in app_state",
              "t-async-1" in app_state.quality_scores)
    finally:
        quality.requests.post = real
        release.set()
        _reset_queue()


def test_nothing_is_lost_when_the_timeout_would_have_dropped_it():
    print("(3) a judge slower than result_timeout loses nothing in async mode")
    _reset_queue()
    written = []

    def slow_handler(url, headers=None, data=None, timeout=None):
        # Far longer than the 1s timeout below: gate mode would have dropped this
        # conversation as judge_failed. Async mode must still write it.
        time.sleep(2.0)
        return _Resp(json.dumps(GOOD_SCORES))

    set_config(judge_config(async_scoring=True, result_timeout=30))
    real = _stub(slow_handler)
    quality.initialize_quality_queue()
    try:
        result = quality.enqueue_quality_scoring(
            CONV, "t-slow", 1, 0, "", "", None, None,
            timeout=1,  # the gate's timeout, deliberately far too short
            write_job=lambda verdict: written.append(verdict))
        check("the short timeout did not drop the row", result is None)
        check("nothing was written yet", not written)
        quality.wait_for_quality_queue()
        check("the slow verdict was still written", len(written) == 1)
        check("it was not recorded as a drop",
              "t-slow" not in app_state.quality_dropped)
        check("it was not recorded as a write failure",
              "t-slow" not in app_state.quality_write_failures)
    finally:
        quality.requests.post = real
        _reset_queue()


def test_rejected_conversation_is_never_written():
    print("(4) a conversation the judge rejects is never written, only recorded")
    _reset_queue()
    written = []

    def handler(url, headers=None, data=None, timeout=None):
        return _Resp("not json at all")

    set_config(judge_config(async_scoring=True))
    real = _stub(handler)
    quality.initialize_quality_queue()
    try:
        quality.enqueue_quality_scoring(
            CONV, "t-reject", 1, 0, "", "", None, None,
            write_job=lambda verdict: written.append(verdict))
        quality.wait_for_quality_queue()
        check("the rejected row was not written", not written)
        check("the rejection is recorded with its reason",
              app_state.quality_dropped.get("t-reject") == "invalid_json")
        check("the rejection is not a write failure",
              "t-reject" not in app_state.quality_write_failures)
        check("no score was stored for it",
              "t-reject" not in app_state.quality_scores)
    finally:
        quality.requests.post = real
        _reset_queue()


def test_write_failure_is_counted_not_lost():
    print("(5) a scored row that cannot be written is counted, not silently lost")
    _reset_queue()

    def handler(url, headers=None, data=None, timeout=None):
        return _Resp(json.dumps(GOOD_SCORES))

    def failing_write_job(verdict):
        raise OSError("disk full")

    set_config(judge_config(async_scoring=True))
    real = _stub(handler)
    quality.initialize_quality_queue()
    try:
        quality.enqueue_quality_scoring(
            CONV, "t-writefail", 1, 0, "", "", None, None,
            write_job=failing_write_job)
        quality.wait_for_quality_queue()
        check("the failure is recorded, not swallowed",
              "t-writefail" in app_state.quality_write_failures)
        check("the verdict was still kept", "t-writefail" in app_state.quality_scores)
    finally:
        quality.requests.post = real
        _reset_queue()


def test_queue_drains_every_job():
    print("(6) the drain does not return until every queued job is written")
    _reset_queue()
    written = []
    written_lock = threading.Lock()

    def handler(url, headers=None, data=None, timeout=None):
        time.sleep(0.05)
        return _Resp(json.dumps(GOOD_SCORES))

    def write_job(verdict):
        with written_lock:
            written.append(verdict)

    count = 12
    set_config(judge_config(async_scoring=True, judge_concurrency=3))
    real = _stub(handler)
    quality.initialize_quality_queue()
    try:
        for i in range(count):
            quality.enqueue_quality_scoring(
                CONV, f"t-drain-{i}", 1, 0, "", "", None, None,
                write_job=write_job)
        drained = quality.wait_for_quality_queue()
        check("the drain reported success", drained is True)
        check("every one of %d rows was written (got %d)" % (count, len(written)),
              len(written) == count)
        check("the queue is empty after the drain",
              quality._quality_scoring_queue.qsize() == 0)
        check("no write failures", not app_state.quality_write_failures)
    finally:
        quality.requests.post = real
        _reset_queue()


def test_gate_mode_still_blocks_and_returns_a_verdict():
    print("(7) async off keeps the blocking gate: the worker still gets a verdict")
    _reset_queue()
    written = []

    def handler(url, headers=None, data=None, timeout=None):
        return _Resp(json.dumps(GOOD_SCORES))

    set_config(judge_config())  # async_scoring absent -> off
    real = _stub(handler)
    quality.initialize_quality_queue()
    try:
        result = quality.enqueue_quality_scoring(
            CONV, "t-gate", 1, 0, "", "", None, None,
            write_job=lambda verdict: written.append(verdict))
        check("gate mode returns the verdict (not None)", isinstance(result, dict))
        check("the verdict carries a composite", result.get("composite", 0) > 0)
        check("gate mode does not call the injected writer", not written)
    finally:
        quality.requests.post = real
        _reset_queue()


def test_per_turn_gate_is_not_queued_behind_conversation_scoring():
    """v12.9.2 regression: the per-turn logic gate must never freeze generation.

    ``check_logic`` runs mid-conversation and BLOCKS its worker for the verdict, so it used
    to be put on the same 1-wide FIFO as conversation scoring. A worker finishing task A
    queued A's conversation score, and every worker's next turn then waited behind that bulk
    job -- the generator stopped sending main-API requests until the judge caught up, in BOTH
    async and gate mode. This pins the fix: a gate request completes while a bulk score is
    still in flight, and the bulk scorer's processor thread is not involved.
    """
    print("(8) the per-turn logic gate does not queue behind conversation scoring (v12.9.2)")
    _reset_queue()
    bulk_started = threading.Event()
    bulk_release = threading.Event()
    gate_done = threading.Event()

    def handler(url, headers=None, data=None, timeout=None):
        body = json.loads(data)
        prompt = body["messages"][-1]["content"]
        if "INTERNAL LOGIC ERRORS" in prompt:
            # The per-turn gate request.
            gate_done.set()
            return _Resp(json.dumps({"logic": 95, "logic_evidence": [],
                                     "rewrite_instruction": ""}))
        # The conversation score: hold it open so the gate MUST overtake it.
        bulk_started.set()
        bulk_release.wait(timeout=20)
        return _Resp(json.dumps(GOOD_SCORES))

    set_config(judge_config(async_scoring=True, judge_concurrency=1))
    real = _stub(handler)
    quality.initialize_quality_queue()
    try:
        # 1) A bulk conversation score is now occupying the single processor.
        quality.enqueue_quality_scoring(
            CONV, "t-bulk", 1, 0, "", "", None, None,
            write_job=lambda verdict: None)
        check("the bulk score reached the judge", bulk_started.wait(timeout=10))

        # 2) A per-turn gate request must still complete, with the bulk job stuck.
        gate = threading.Thread(
            target=lambda: quality.check_logic(
                "She nods.", CONV, task_id="t-gate", thread_id=2),
            daemon=True)
        gate.start()
        check("the per-turn gate completed while the bulk score was still in flight",
              gate_done.wait(timeout=5))
        gate.join(timeout=10)
        check("the gate thread is not stuck", not gate.is_alive())
    finally:
        bulk_release.set()
        quality.requests.post = real
        try:
            quality.wait_for_quality_queue(quiet=True)
        except Exception:
            pass
        _reset_queue()


def test_gate_semaphore_is_separate_from_the_queue():
    print("(9) the gate has its own concurrency cap, sized to judge_concurrency")
    _reset_queue()
    set_config(judge_config(async_scoring=True, judge_concurrency=2))
    try:
        sem = quality._judge_gate_semaphore()
        check("the gate semaphore exists", sem is not None)
        check("it is not the queue", sem is not quality._quality_scoring_queue)
        check("it admits judge_concurrency requests at once",
              sem.acquire(timeout=0) and sem.acquire(timeout=0))
        check("and no more than that", sem.acquire(timeout=0) is False)
        sem.release()
        sem.release()
    finally:
        set_config(judge_config())
        quality._judge_gate_semaphore_obj = None
        quality._judge_gate_semaphore_size = 0


def main():
    test_async_flag_defaults_off()
    test_generation_does_not_wait_for_a_slow_judge()
    test_nothing_is_lost_when_the_timeout_would_have_dropped_it()
    test_rejected_conversation_is_never_written()
    test_write_failure_is_counted_not_lost()
    test_queue_drains_every_job()
    test_gate_mode_still_blocks_and_returns_a_verdict()
    test_per_turn_gate_is_not_queued_behind_conversation_scoring()
    test_gate_semaphore_is_separate_from_the_queue()
    print()
    if FAILURES:
        print("{} async queue test(s) failed".format(len(FAILURES)))
        sys.exit(1)
    print("all async queue tests passed")


if __name__ == "__main__":
    try:
        main()
    finally:
        os.chdir("/")
        shutil.rmtree(_TMP, ignore_errors=True)
