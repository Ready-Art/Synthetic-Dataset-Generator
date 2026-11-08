# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""tests/test_scene_continuity_drop.py - v14.3: an unfixable cast violation DROPS the row.

Headless, and it runs the REAL worker against a fake main-API server, the same way
``tests/test_progress_accounting.py`` does. No model, no network, no display:

    python -B tests/test_scene_continuity_drop.py

The defect this file exists to prevent:

  ``generate_answer_with_retries()`` treated a scene-continuity violation like every other check:
  it retried the reply with a fix appended to the system prompt, and when the fix rounds ran out it
  logged "Keeping the best-effort answer" and RETURNED the text. The worker then wrote the row.

  That is wrong for this one check and right for all the others. A slop phrase or a repeated opener
  is a style miss -- the transcript still makes sense, so losing the row would be the bigger loss. A
  continuity miss is a cast CONTRADICTION: the text has a character who has left the scene still
  speaking or acting, which is the exact corruption Scene Cast exists to prevent. Keeping it means
  shipping a dataset row that argues with itself.

  The fix is asymmetric on purpose: the flag travels back as the 4th element of the result tuple, and
  ``worker()`` drops the task (recorded in ``app_state.quality_dropped`` so a resumed run does not
  retry it, and credited for progress so the bar can still reach 100%).
"""

import atexit
import json
import os
import shutil
import sys
import tempfile
import threading
from queue import Queue

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRATCH = tempfile.mkdtemp(prefix="sdg-cast-drop-test-")
atexit.register(shutil.rmtree, SCRATCH, ignore_errors=True)
shutil.copytree(os.path.join(REPO, "config"), os.path.join(SCRATCH, "config"))
os.chdir(SCRATCH)
os.makedirs("output", exist_ok=True)
sys.path.insert(0, REPO)

import app_state  # noqa: E402  (imports read config/ relative to the working directory)
import detection  # noqa: E402
import generation as g  # noqa: E402

FAILURES = []


def check(name, cond, detail=""):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}" + (f"  <- {detail}" if detail else ""))
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

# The two characters of the scene. "Ines" leaves on slot 2 (see CAST_CONFIG below); "Cole" stays.
LEAVER = "Ines"
STAYER = "Cole"

# A reply in which the character who has ALREADY LEFT is still speaking and acting. This is the
# hard violation: an attributed quoted line plus a narration action, both belonging to Ines.
VIOLATING_ANSWER = (
    'Ines leaned in the doorway. "Still here, then," Ines said.'
)
# A reply that names NO character, so it is legal on every slot: the cast check only ever objects
# to a character who is absent/exited being written, and a missing scheduled beat is a SOFT finding
# that never rewrites or drops. (Naming the stayer would be a violation on the slots before his
# arrival slot, which is exactly the trap the fixture must not fall into.)
CLEAN_ANSWER = 'The lamp is set down on the counter. Rain like this keeps most people home.'


class Resp:
    headers = {}

    def __init__(self, status, content):
        self.status_code = status
        self.content = content
        self.text = str(content)
        self.usage = {"prompt_tokens": 11, "completion_tokens": 7}

    def json(self):
        return {"choices": [{"message": {"content": self.content}}], "usage": self.usage}


class Router:
    """Fake main-API server. Every ANSWER request returns the violating text, so the fix rounds
    can never succeed and the row must be dropped. Continuations stay clean."""

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
        kind = self.kind_of(body["messages"])
        self.requests.append((kind, body))
        if kind == "continuation":
            return Resp(200, "I shake the water from my coat and step closer.")
        if kind == "question":
            return Resp(200, "You are late.")
        return Resp(200, VIOLATING_ANSWER)


ROUTER = Router()
g.requests.post = ROUTER.post

SLOT = {"enabled": True, "url": "http://fake/v1/chat/completions", "model": "m", "key": "",
        "sampler_settings": {"temperature": 0.5, "top_p": 0.9, "top_k": 50, "repetition_penalty": 1.1,
                             "max_tokens_question": 90, "max_tokens_answer": 300,
                             "max_tokens_user_reply": 120}}
EMPTY = {"enabled": False, "url": "", "model": "", "key": "", "sampler_settings": {}}

# Two characters, so a cast is possible at all; min_present 1 leaves one spare, so the schedule
# really does contain a departure. The exit is placed on slot 2, which the violating answer then
# fails to narrate -- so Ines stays 'present' for slot 2 and is 'exited' from slot 3 on, where the
# check catches her still speaking.
CHARACTERS = [
    {"name": LEAVER, "age": "34", "gender": "Female", "race": "Human", "job": "Ferry pilot",
     "clothing": "Oilskin coat.", "appearance": "Tall, windburned.", "backstory": "Runs the night ferry.",
     "personality": "Dry and watchful.", "traits": "Patient", "setting": "", "class": ""},
    {"name": STAYER, "age": "41", "gender": "Male", "race": "Human", "job": "Harbourmaster",
     "clothing": "Wool coat.", "appearance": "Broad, grey.", "backstory": "Keeps the log.",
     "personality": "Slow and careful.", "traits": "Methodical", "setting": "", "class": ""},
]

CAST_CONFIG = {"enabled": True, "shape": "minimal", "min_present": 1,
               "max_deferrals": 1, "exit_detection": "always",
               "join_slot": 0, "exit_slot": 2, "placement": "depth0",
               "allow_cutaway": False, "exit_guidance": [], "arrival_guidance": [],
               "cutaway_note": ""}

NUM_TURNS = 4


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
    for key in list(CONFIG):
        if key != 'quality.enabled':
            del CONFIG[key]
    for name in ("output.jsonl",):
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
        current_slop_fixes_fallback=[], current_max_attempts=2, current_history_size_local=5,
        current_remove_reasoning=False, current_remove_em_dash=False, current_remove_asterisks=False,
        current_remove_asterisk_space_asterisk=False, current_remove_all_asterisks=False,
        current_ensure_space_after_line_break=False, current_remove_markdown=False,
        current_output_format="sharegpt", slop_fixer_api_config=EMPTY,
        anti_slop_fixer_api_config_runtime=EMPTY, anti_slop_fixer_api_config_param=EMPTY,
        current_slop_fixes_for_rotation_worker=[], current_top_level_system_prompt="",
        master_duplication_enabled_local=False, enable_character_engine_local=True,
        enable_class_selection_local=False, enable_setting_selection_local=False,
        character_list=CHARACTERS, enable_emotional_states_local=False, emotional_states_list_local=[],
        num_characters_local=2, no_user_impersonation_local=False, current_api_request_timeout=5,
        current_slop_to_anti_slop_fallback=False, current_lore="",
        enable_random_names_local=False, random_names_list_local=None,
        current_regex_patterns=(), current_regex_watch_enabled=False,
        current_regex_watch_max_rounds=3,
        # --- the feature under test ---
        scene_cast_enabled_local=True, scene_cast_config_local=dict(CAST_CONFIG),
        scene_continuity_enabled_local=True, scene_continuity_fixes_local=(),
        scene_continuity_max_rounds=1,
    )
    kwargs.update(overrides)
    return kwargs


def make_queue():
    q = Queue()
    q.processed_tasks_lock = threading.Lock()
    q.processed_tasks = 0
    q.total_tasks_for_progress = 1
    q.api_processed_tasks = {}
    q.start_times_list = []
    q.overall_task_times_list = {}
    q.api_start_times_list = {}
    return q


def run_worker(task, **overrides):
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


def read_output():
    path = os.path.join("output", "output.jsonl")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


# --- The checks ------------------------------------------------------------------------------

def test_the_fixture_really_violates():
    """Guard the guard: the fixture must be a violation, or the rest proves nothing."""
    print("\nThe fixture is a real violation")
    plan = g._build_scene_cast_plan([LEAVER, STAYER], NUM_TURNS, CAST_CONFIG)
    check("the fixture plan schedules a departure", any(
        m["exit_slot"] is not None for m in plan["members"]), plan)
    state = g._cast_state_from_plan(plan)
    # Walk to the first slot where the leaver is gone.
    slot = 1
    blocked = ()
    while slot <= NUM_TURNS:
        blocked, expected, _present = g._cast_names_for_slot(plan, state, slot)
        if LEAVER in blocked:
            break
        state, _events = g._advance_cast_state(
            plan, state, slot, CLEAN_ANSWER, max_deferrals=1, mode="always")
        slot += 1
    check("the leaver is blocked on a later slot", LEAVER in blocked, blocked)
    violation, findings = detection.check_scene_continuity(
        VIOLATING_ANSWER, list(blocked), list(expected))
    check("the fixture answer IS a hard violation", violation is True, findings)


def test_violating_conversation_is_dropped():
    print("\nAn unfixable violation drops the conversation")
    reset_state()
    task = chunk_task()
    run_worker(task)

    rows = read_output()
    check("nothing was written to output.jsonl", rows == [], rows)
    check("the task is recorded as dropped", task[0] in app_state.dropped_task_ids,
          sorted(app_state.dropped_task_ids))
    check("the drop reason is scene_continuity",
          app_state.quality_dropped.get(task[0]) == "scene_continuity",
          app_state.quality_dropped.get(task[0]))
    check("the task is NOT marked complete", task[0] not in app_state.completed_task_ids)
    check("the task is not left pending", task[0] not in app_state.pending_task_ids)
    # The fix rounds must actually have been tried before giving up.
    answers = [body for kind, body in ROUTER.requests if kind == "answer"]
    check("the reply was retried with a fix before the drop", len(answers) > 1, len(answers))


def test_clean_conversation_is_written():
    """The drop must be specific to the violation: a clean run still writes its row."""
    print("\nA clean conversation is still written")
    reset_state()
    task = chunk_task()

    original_post = ROUTER.post

    def clean_post(url, headers=None, data=None, timeout=None):
        body = json.loads(data)
        kind = Router.kind_of(body["messages"])
        ROUTER.requests.append((kind, body))
        if kind == "continuation":
            return Resp(200, "I shake the water from my coat and step closer.")
        if kind == "question":
            return Resp(200, "You are late.")
        return Resp(200, CLEAN_ANSWER)

    g.requests.post = clean_post
    try:
        run_worker(task)
    finally:
        g.requests.post = original_post

    rows = read_output()
    check("a clean conversation IS written", len(rows) == 1, len(rows))
    check("the clean task is not dropped", task[0] not in app_state.dropped_task_ids,
          sorted(app_state.dropped_task_ids))
    check("the clean task is marked complete", task[0] in app_state.completed_task_ids)


def test_result_tuple_contract():
    """The 4th element is what carries the verdict; the first three keep their meaning."""
    print("\nThe result tuple contract")
    import inspect
    source = inspect.getsource(g.generate_answer_with_retries)
    check("the docstring documents the 4-tuple",
          "4-TUPLE" in source and "cast_violation_flag" in source)
    check("the success return carries the flag",
          "refusal_ever_detected_this_task,\n                        cast_violation_this_task)" in source)
    check("the exhausted return carries the flag",
          "refusal_ever_detected_this_task,\n            cast_violation_this_task)" in source)
    # A caller that only reads [0..2] must keep working: the worker guards with len().
    worker_source = inspect.getsource(g.worker)
    check("the worker reads the flag defensively",
          "len(answer_result) > 3 and answer_result[3]" in worker_source)


def main():
    print("Scene continuity drop (v14.3) - headless worker checks")
    test_the_fixture_really_violates()
    test_violating_conversation_is_dropped()
    test_clean_conversation_is_written()
    test_result_tuple_contract()
    if FAILURES:
        print("\n%d check(s) FAILED:" % len(FAILURES))
        for name in FAILURES:
            print("  - " + name)
        return 1
    print("\nAll scene continuity drop checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
