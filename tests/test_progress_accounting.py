# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Tests for the v13.0.5 progress accounting fix (FIX-PLAN.md).

Run directly: ``python -B tests/test_progress_accounting.py``. No model and no display are
needed: ``generation`` imports no GUI toolkit. The script runs in
a temporary directory, so it writes only to that directory's ``output/``. The judge is stubbed
exactly as ``tests/test_quality_async_queue.py`` stubs it, and the main API is a fake
OpenAI-compatible server, as in ``tests/test_opening_mode.py``.

The defect this file exists to prevent:

  ``quality.async_scoring: true`` moves the row write off the worker and onto a queue
  processor thread. The worker used to count a turn speculatively the moment an answer came
  back, then REFUND it in its epilogue whenever the task was not yet in ``completed_task_ids``
  -- which under async scoring is always, because the id is only added later by
  ``_mark_task_written()``. That function never credited progress either, so a successfully
  completed async task contributed exactly ZERO to the bar. The bar was therefore pinned to the
  number of turns in flight (bounded by the worker count, not the queued work) and oscillated
  with the judge's timing, which is the "stalls at ~60 and goes up and down" report.

The guarantee after the fix: progress is credited ONCE per completed TASK, idempotently, at the
moment the work is finished, and is never refunded.
"""

import ast
import atexit
import json
import os
import shutil
import sys
import tempfile
import threading
import time
from queue import Queue

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRATCH = tempfile.mkdtemp(prefix="sdg-progress-test-")
atexit.register(shutil.rmtree, SCRATCH, ignore_errors=True)
shutil.copytree(os.path.join(REPO, "config"), os.path.join(SCRATCH, "config"))
os.chdir(SCRATCH)
os.makedirs("output", exist_ok=True)
sys.path.insert(0, REPO)

import app_state  # noqa: E402  (imports read config/ relative to the working directory)
import detection  # noqa: E402
import generation as g  # noqa: E402

FAILURES = []


def check(name, cond):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}")
        FAILURES.append(name)


# --- Harness ---------------------------------------------------------------------------------

g.log_message = lambda *a, **k: None


class Var:
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value


for _name, _value in list(vars(app_state).items()):
    if _name.endswith('_var') and _value is None:
        setattr(app_state, _name, Var(False))
detection.issue_timestamps = app_state.issue_timestamps
detection.issue_timestamps_lock = app_state.issue_timestamps_lock

CONFIG = {'quality.enabled': False}
_real_config_get = g.global_config.get
g.global_config.get = lambda key, default=None: CONFIG[key] if key in CONFIG else _real_config_get(key, default)

g.check_circuit_breaker = lambda *a, **k: True
g.check_budget_limit = lambda *a, **k: False
g.global_rate_limiter.wait_if_needed = lambda *a, **k: None
g.time.sleep = lambda *a: None
g.get_cached_response = lambda *a, **k: (None, False)
g.set_cached_response = lambda *a, **k: None

QUESTION_PROMPT = "QUESTION-PROMPT {subject} | {context} | {recent_questions}"
ANSWER_PROMPT = "ANSWER-PROMPT Reply in character."
CONTINUE_PROMPT = "CONTINUE-PROMPT {last_assistant_message}"

GOOD_ANSWER = "She sets the lamp down. \"Rain like this keeps most people home.\""


class Resp:
    headers = {}

    def __init__(self, status, content, usage):
        self.status_code = status
        self.content = content
        self.text = str(content)
        self.usage = usage

    def json(self):
        return {"choices": [{"message": {"content": self.content}}], "usage": self.usage}


class Router:
    """Fake main-API server: answers every generation request with a usable line."""

    def __init__(self):
        self.requests = []

    def reset(self):
        self.requests = []

    @staticmethod
    def kind_of(messages):
        last = messages[-1]["content"] if messages else ""
        if "ANSWER-PROMPT" in last:
            return "answer"
        if "CONTINUE-PROMPT" in last:
            return "continuation"
        if "QUESTION-PROMPT" in last:
            return "question"
        return "unknown"

    def post(self, url, headers=None, data=None, timeout=None):
        body = json.loads(data)
        self.requests.append((self.kind_of(body["messages"]), body))
        if self.kind_of(body["messages"]) == "continuation":
            return Resp(200, "I shake the water from my coat and step closer.",
                        {"prompt_tokens": 11, "completion_tokens": 7})
        return Resp(200, GOOD_ANSWER, {"prompt_tokens": 11, "completion_tokens": 7})


ROUTER = Router()
g.requests.post = ROUTER.post

SLOT = {"enabled": True, "url": "http://fake/v1/chat/completions", "model": "m", "key": "",
        "sampler_settings": {"temperature": 0.5, "top_p": 0.9, "top_k": 50, "repetition_penalty": 1.1,
                             "max_tokens_question": 90, "max_tokens_answer": 300,
                             "max_tokens_user_reply": 120}}
EMPTY = {"enabled": False, "url": "", "model": "", "key": "", "sampler_settings": {}}
CHARACTERS = [
    {"name": "Ines", "age": "34", "gender": "Female", "race": "Human", "job": "Ferry pilot",
     "clothing": "Oilskin coat.", "appearance": "Tall, windburned.", "backstory": "Runs the night ferry.",
     "personality": "Dry and watchful.", "traits": "Patient", "setting": "", "class": ""},
]

NUM_TURNS = 2
TOTAL_FOR_PROGRESS = 40  # 20 tasks x 2 turns


def reset_state():
    ROUTER.reset()
    app_state.task_retry_counts.clear()
    app_state.task_metadata.clear()
    app_state.completed_task_ids.clear()
    app_state.failed_task_ids.clear()
    app_state.pending_task_ids.clear()
    app_state.dropped_task_ids.clear()
    app_state.quality_scores.clear()
    app_state.quality_dropped.clear()
    app_state.quality_write_failures.clear()
    app_state.stop_processing = False
    app_state.pause_processing = False
    app_state.processing_active = True
    app_state.character_counter = 0
    app_state.system_prompt_counter = 0
    app_state.question_history = []
    app_state.db_pool = None
    app_state.quality_enabled = False
    app_state.current_num_turns_for_progress = NUM_TURNS
    with app_state.progress_credited_lock:
        app_state.progress_credited_task_ids = set()
    g.MAX_TASK_REQUEUES = 50
    for key in [k for k in CONFIG if k != 'quality.enabled']:
        del CONFIG[key]
    for name in ("output.jsonl", "output_api_slot_1.jsonl"):
        path = os.path.join("output", name)
        if os.path.exists(path):
            os.remove(path)


def chunk_task(task_id="book.txt_chunk_at_0", context="The bridge was closed for repairs."):
    return (task_id, "book.txt", 0, "The bridge was closed.", context)


def worker_kwargs(**overrides):
    kwargs = dict(
        use_questions_file_local=False, use_variable_system_local=False,
        all_api_configs_local=[SLOT, EMPTY, EMPTY, EMPTY, EMPTY, EMPTY],
        active_enabled_api_configs_for_worker=[{"config": SLOT, "original_slot_idx": 0}],
        current_question_prompt=QUESTION_PROMPT, current_answer_prompt=ANSWER_PROMPT,
        current_user_continuation_prompt=CONTINUE_PROMPT, current_num_turns=NUM_TURNS,
        current_system_prompts_for_worker=["SCENE: a ferry landing at night."],
        current_refusal_phrases=["I cannot help"], current_user_speaking_phrases=["you nod"],
        current_slop_phrases=["shivers down"], current_anti_slop_phrases=[], current_anti_slop_fixes=[],
        current_jailbreaks=["Stay in character."], current_speaking_fixes=["Do not speak for the user."],
        current_slop_fixes_fallback=[], current_max_attempts=3, current_history_size_local=5,
        current_remove_reasoning=False, current_remove_em_dash=False, current_remove_asterisks=False,
        current_remove_asterisk_space_asterisk=False, current_remove_all_asterisks=False,
        current_ensure_space_after_line_break=False, current_remove_markdown=False,
        current_output_format="sharegpt", slop_fixer_api_config=EMPTY,
        anti_slop_fixer_api_config_runtime=EMPTY, anti_slop_fixer_api_config_param=EMPTY,
        current_slop_fixes_for_rotation_worker=[], current_top_level_system_prompt="",
        master_duplication_enabled_local=False, enable_character_engine_local=True,
        enable_class_selection_local=False, enable_setting_selection_local=False,
        character_list=CHARACTERS[:1], enable_emotional_states_local=False, emotional_states_list_local=[],
        num_characters_local=1, no_user_impersonation_local=False, current_api_request_timeout=5,
        current_slop_to_anti_slop_fallback=False, current_lore="",
        enable_random_names_local=False, random_names_list_local=None,
        current_regex_patterns=(), current_regex_watch_enabled=False,
        current_regex_watch_max_rounds=3)
    kwargs.update(overrides)
    return kwargs


def make_queue(processed=0):
    """A queue shaped like the one generate.start_processing() builds."""
    q = Queue()
    q.processed_tasks_lock = threading.Lock()
    q.processed_tasks = processed
    q.total_tasks_for_progress = TOTAL_FOR_PROGRESS
    q.api_processed_tasks = {}
    q.start_times_list = []
    q.overall_task_times_list = []
    q.api_start_times_list = {}
    return q


def run_worker(task, q=None, **overrides):
    """Run the real worker on one task, then the sentinel."""
    if q is None:
        q = make_queue()
    app_state.task_metadata[task[0]] = {"file": task[1], "retries": 0, "task_data": task}
    app_state.pending_task_ids.add(task[0])
    q.put(task)
    q.put(None)
    g.worker(0, q, threading.Lock(), **worker_kwargs(**overrides))
    while not q.empty():
        item = q.get()
        q.task_done()
    return q


GOOD_VERDICT = {"composite": 88, "dimensions": {}, "flags": [],
                "scored_at": "2026-01-01 00:00:00", "method": "stub"}


def _reset_quality_queue():
    try:
        g.quality.shutdown_quality_queue()
    except Exception:
        pass
    g.quality._quality_processor_running = False
    g.quality._quality_processor_threads = []
    while True:
        try:
            g.quality._quality_scoring_queue.get_nowait()
        except Exception:
            break
        else:
            g.quality._quality_scoring_queue.task_done()


def _install_async_judge(score_fn):
    """Make ``quality`` behave like ``async_scoring: true`` with a running queue."""
    g.quality.judge_async_enabled = lambda *a, **k: True
    g.quality.score_conversation = score_fn
    g._install_quality_write_handler()


# --- (1) Async completion credits once --------------------------------------------------------

def test_async_completion_credits_once():
    """The test that would have caught the bug: an async-scored task must move the bar."""
    print("(1) an async-scored task credits its turns to the bar exactly once")
    reset_state()
    _reset_quality_queue()
    real_async, real_score = g.quality.judge_async_enabled, g.quality.score_conversation
    app_state.quality_enabled = True
    # The judge must NOT be allowed to finish before the worker's epilogue runs.
    #
    # This test asserts the ASYNC HANDOFF (the worker returns without marking the task complete or
    # crediting it, and the processor does both afterwards). With an unguarded stub judge that
    # handoff is a race: the processor thread can score, write, mark complete and credit while the
    # worker is still in its epilogue, so both checks below read the POST-processor state and fail
    # intermittently (measured: 2 of 6 runs) while the product is behaving correctly. Holding the
    # judge on an event makes the ordering the test is about deterministic instead of probable.
    started = threading.Event()
    release = threading.Event()

    def held_score(conv, *a, **k):
        started.set()
        release.wait(timeout=20)
        return dict(GOOD_VERDICT)

    try:
        _install_async_judge(held_score)
        g.quality.initialize_quality_queue()
        q = make_queue()
        run_worker(chunk_task(), q=q)
        check("the worker handed the row over instead of writing it",
              "book.txt_chunk_at_0" not in app_state.completed_task_ids)
        check("nothing is credited before the row exists", q.processed_tasks == 0)

        release.set()
        g.quality.wait_for_quality_queue(quiet=True)
        check("the task is marked complete by the processor",
              "book.txt_chunk_at_0" in app_state.completed_task_ids)
        check("the completed task credits num_turns turns (got %d)" % q.processed_tasks,
              q.processed_tasks == NUM_TURNS)
        check("the credit is recorded in the credited set",
              app_state.progress_credited_task_ids == {"book.txt_chunk_at_0"})
    finally:
        g.quality.judge_async_enabled, g.quality.score_conversation = real_async, real_score
        app_state.quality_enabled = False
        _reset_quality_queue()


# --- (2) No refund ----------------------------------------------------------------------------

def test_no_refund_when_the_judge_is_slower_than_the_worker():
    """Guards the deleted refund: the epilogue must not take the turns back."""
    print("(2) a judge slower than the worker's epilogue does not refund the credit")
    reset_state()
    _reset_quality_queue()
    real_async, real_score = g.quality.judge_async_enabled, g.quality.score_conversation
    app_state.quality_enabled = True
    started = threading.Event()
    release = threading.Event()

    def slow_score(conv, *a, **k):
        started.set()
        release.wait(timeout=20)
        return dict(GOOD_VERDICT)

    try:
        _install_async_judge(slow_score)
        g.quality.initialize_quality_queue()
        q = make_queue()
        run_worker(chunk_task(), q=q)
        check("the worker returned while the judge was still working", started.wait(timeout=10))
        check("the worker's epilogue refunded nothing (counter is 0, not negative)",
              q.processed_tasks == 0)

        release.set()
        g.quality.wait_for_quality_queue(quiet=True)
        check("the turns are credited once the judge finishes (got %d)" % q.processed_tasks,
              q.processed_tasks == NUM_TURNS)
        # The old code refunded in the epilogue and never credited again, so a completed
        # async task left exactly 0 behind. That is what this asserts is gone.
        check("a completed async task leaves a positive contribution",
              q.processed_tasks > 0)
    finally:
        release.set()
        g.quality.judge_async_enabled, g.quality.score_conversation = real_async, real_score
        app_state.quality_enabled = False
        _reset_quality_queue()


# --- (3) Idempotent ---------------------------------------------------------------------------

def test_credit_is_idempotent():
    print("(3) crediting the same task twice adds num_turns once")
    reset_state()
    q = make_queue()
    app_state.task_queue = q
    try:
        g._credit_task_progress("t-idem", queue=q)
        g._credit_task_progress("t-idem", queue=q)
        g._credit_task_progress("t-idem", 99, q)
        check("two calls credit one task's turns (got %d)" % q.processed_tasks,
              q.processed_tasks == NUM_TURNS)
        check("the id is in the credited set",
              app_state.progress_credited_task_ids == {"t-idem"})

        g._credit_task_progress("t-other", queue=q)
        check("a different task still credits", q.processed_tasks == NUM_TURNS * 2)

        q.processed_tasks = TOTAL_FOR_PROGRESS - 1
        g._credit_task_progress("t-cap", 5, q)
        check("the credit is capped at total_tasks_for_progress",
              q.processed_tasks == TOTAL_FOR_PROGRESS)
    finally:
        app_state.task_queue = None


# --- (4) Resume does not double-count ----------------------------------------------------------

def test_resume_does_not_double_count():
    """A resumed run seeds the counter from the snapshot and must not re-credit it."""
    print("(4) a resumed run does not credit already-completed tasks a second time")
    reset_state()
    _reset_quality_queue()
    snapshot = 8
    completed = {"book.txt_chunk_at_0", "book.txt_chunk_at_1"}
    app_state.completed_task_ids = set(completed)
    # What generate.start_processing() + load_generation_state() do on a resume: the snapshot
    # is the single source of truth and every task it counts is seeded as already credited.
    with app_state.progress_credited_lock:
        app_state.progress_credited_task_ids = set(completed)
    q = make_queue(processed=snapshot)
    app_state.task_queue = q
    try:
        # The worker's "skip an already completed task" path.
        run_worker(chunk_task(task_id="book.txt_chunk_at_0"), q=q)
        check("the skip path leaves the snapshot untouched (got %d)" % q.processed_tasks,
              q.processed_tasks == snapshot)
        check("the skipped task was not credited again",
              app_state.progress_credited_task_ids == completed)

        # And a brand-new task still advances the bar from the snapshot.
        run_worker(chunk_task(task_id="book.txt_chunk_at_2"), q=q)
        check("a new task credits on top of the snapshot (got %d)" % q.processed_tasks,
              q.processed_tasks == snapshot + NUM_TURNS)
        check("the new task is credited",
              "book.txt_chunk_at_2" in app_state.progress_credited_task_ids)
    finally:
        app_state.task_queue = None


# --- (5) A judge-dropped task still credits ----------------------------------------------------

def test_dropped_task_still_credits():
    print("(5) a judge-dropped task still credits progress, so 100% is reachable")
    reset_state()
    _reset_quality_queue()
    real_async, real_score = g.quality.judge_async_enabled, g.quality.score_conversation
    app_state.quality_enabled = True
    try:
        _install_async_judge(lambda conv, *a, **k: dict(GOOD_VERDICT))
        g.quality.initialize_quality_queue()
        q = make_queue(processed=TOTAL_FOR_PROGRESS - NUM_TURNS)
        run_worker(chunk_task(), q=q)
        g.quality.wait_for_quality_queue(quiet=True)
        check("the written task credits", q.processed_tasks == TOTAL_FOR_PROGRESS)
    finally:
        g.quality.judge_async_enabled, g.quality.score_conversation = real_async, real_score
        app_state.quality_enabled = False
        _reset_quality_queue()

    # Now the synchronous (gate-mode) drop path, which is the one that skips the write.
    reset_state()
    real_score = g.quality.score_conversation
    app_state.quality_enabled = True
    try:
        g.quality.score_conversation = lambda conv, *a, **k: {
            "dropped": True, "drop_reason": "logic_zero", "composite": 0,
            "dimensions": {}, "flags": ["logic_issues"],
            "scored_at": "2026-01-01 00:00:00", "method": "dropped"}
        q = make_queue(processed=TOTAL_FOR_PROGRESS - NUM_TURNS)
        run_worker(chunk_task(), q=q)
        check("a dropped task writes no row",
              not os.path.exists(os.path.join("output", "output.jsonl")))
        check("a dropped task is not marked complete", not app_state.completed_task_ids)
        check("a dropped task still credits its turns (got %d)" % q.processed_tasks,
              q.processed_tasks == TOTAL_FOR_PROGRESS)
        check("the drop stays visible in dropped_task_ids",
              "book.txt_chunk_at_0" in app_state.dropped_task_ids)
    finally:
        g.quality.score_conversation = real_score
        app_state.quality_enabled = False


# --- (6) The counter never goes backwards ------------------------------------------------------

def test_counter_is_monotonic():
    print("(6) the counter is monotonically non-decreasing over a run of tasks")
    reset_state()
    _reset_quality_queue()
    real_async, real_score = g.quality.judge_async_enabled, g.quality.score_conversation
    app_state.quality_enabled = True
    try:
        _install_async_judge(lambda conv, *a, **k: dict(GOOD_VERDICT))
        g.quality.initialize_quality_queue()
        q = make_queue()
        seen = []
        for i in range(6):
            run_worker(chunk_task(task_id="book.txt_chunk_at_%d" % i), q=q)
            seen.append(q.processed_tasks)
            g.quality.wait_for_quality_queue(quiet=True)
            seen.append(q.processed_tasks)
        check("the counter never decreases (saw %s)" % seen,
              all(b >= a for a, b in zip(seen, seen[1:])))
        check("six async tasks credited %d turns (got %d)" % (6 * NUM_TURNS, q.processed_tasks),
              q.processed_tasks == 6 * NUM_TURNS)
    finally:
        g.quality.judge_async_enabled, g.quality.score_conversation = real_async, real_score
        app_state.quality_enabled = False
        _reset_quality_queue()


# --- (7) The state file carries the credited set -----------------------------------------------

def test_state_file_round_trip():
    print("(7) the credited set is persisted, and an old state file falls back safely")
    reset_state()
    app_state.completed_task_ids = {"a", "b"}
    with app_state.progress_credited_lock:
        app_state.progress_credited_task_ids = {"a"}
    app_state.task_queue = make_queue(processed=8)
    try:
        g.save_generation_state()
        with open(os.path.join("output", "generation_state.json"), encoding="utf-8") as f:
            state = json.load(f)
        check("the state file carries progress_credited_task_ids",
              state.get("progress_credited_task_ids") == ["a"])
        check("it also carries the processed snapshot",
              state.get("processed_tasks_snapshot") == 8)
        check("it is NOT in config_snapshot (it is run progress, not configuration)",
              "progress_credited_task_ids" not in state.get("config_snapshot", {}))
    finally:
        app_state.task_queue = None
        path = os.path.join("output", "generation_state.json")
        if os.path.exists(path):
            os.remove(path)


def test_resume_paths_seed_the_credited_set():
    """The two resume entry points must seed the credited set (parsed, not imported)."""
    print("(8) both resume paths seed progress_credited_task_ids")

    def seeds(tree, function_name):
        found = [n for n in ast.walk(tree)
                 if isinstance(n, ast.FunctionDef) and n.name == function_name]
        if len(found) != 1:
            return False
        return "progress_credited_task_ids" in ast.unparse(found[0])

    with open(os.path.join(REPO, "control.py"), encoding="utf-8") as f:
        control_tree = ast.parse(f.read())

    check("control's headless resume path seeds it",
          seeds(control_tree, "_load_state"))
    check("control's headless resume path restores the saved counter",
          seeds(control_tree, "_load_state") and
          "processed_tasks_snapshot" in ast.unparse(
              [n for n in ast.walk(control_tree)
               if isinstance(n, ast.FunctionDef) and n.name == "_load_state"][0]))
    check("control's reset path clears it",
          "progress_credited_task_ids" in ast.unparse(
              [n for n in ast.walk(control_tree)
               if isinstance(n, ast.FunctionDef) and n.name == "_reset_stats"][0]))

    # The speculative increment and the refund must be GONE: they are the bug.
    with open(os.path.join(REPO, "generation.py"), encoding="utf-8") as f:
        source = f.read()
    check("the per-turn increment block is gone", "turns_counted_this_attempt" not in source)
    check("the refund epilogue is gone",
          "q.processed_tasks = max(0, q.processed_tasks -" not in source)
    check("the CRITICAL increment failure log line is gone",
          "CRITICAL - Failed to increment turn counter" not in source)
    check("_mark_task_written credits progress",
          "_credit_task_progress(task_id, turns" in source)


def main():
    test_async_completion_credits_once()
    test_no_refund_when_the_judge_is_slower_than_the_worker()
    test_credit_is_idempotent()
    test_resume_does_not_double_count()
    test_dropped_task_still_credits()
    test_counter_is_monotonic()
    test_state_file_round_trip()
    test_resume_paths_seed_the_credited_set()
    print()
    if FAILURES:
        print("{} progress accounting test(s) failed".format(len(FAILURES)))
        sys.exit(1)
    print("all progress accounting tests passed")


if __name__ == "__main__":
    try:
        main()
    finally:
        os.chdir("/")
        shutil.rmtree(SCRATCH, ignore_errors=True)
