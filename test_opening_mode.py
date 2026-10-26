"""Tests for opening mode -- run directly: `python test_opening_mode.py`.

No test framework, network or display. `import generation` still needs Tk installed (it imports
tkinter). The script copies config/ into a temporary directory and runs there, so it writes only
to that directory's output/. The real worker and request functions run against a fake
OpenAI-compatible server (a replacement for requests.post) that records every request and picks
each reply from the request's content, so retries cannot shift which reply goes where.
Exits non-zero if any check fails.
"""
import atexit
import json
import os
import shutil
import sys
import tempfile
import threading
from queue import Queue

REPO = os.path.dirname(os.path.abspath(__file__))
SCRATCH = tempfile.mkdtemp(prefix="sdg-opening-test-")
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

CACHE_CALLS = []
g.get_cached_response = lambda *a, **k: (CACHE_CALLS.append(("get", a)), (None, False))[1]
g.set_cached_response = lambda *a, **k: CACHE_CALLS.append(("set", a))

SANITIZE_CALLS = []
_real_sanitize = g.sanitize_payload_for_endpoint


def _recording_sanitize(payload, url, slot):
    SANITIZE_CALLS.append(slot)
    return _real_sanitize(payload, url, slot)


g.sanitize_payload_for_endpoint = _recording_sanitize

# Prompt markers the router uses to tell requests apart.
QUESTION_PROMPT = "QUESTION-PROMPT {subject} | {context} | {recent_questions}"
ANSWER_PROMPT = "ANSWER-PROMPT Reply in character."
CONTINUE_PROMPT = "CONTINUE-PROMPT {last_assistant_message}"


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
    """Fake server. `script[kind]` is a list of replies used in order (the last repeats); a reply
    is text (HTTP 200) or (status, text). `on_request(kind, body)` runs before replying."""

    DEFAULTS = {
        "question": ["What brings you out in this weather?"],
        "answer": ["She sets the lamp down. \"Rain like this keeps most people home.\""],
        "continuation": ["I shake the water from my coat and step closer."],
        "greeting": ["Rain drums on the tin roof as Ines checks the mooring lines. She glances up. \"Last crossing leaves in ten minutes. You coming?\""],
        "first_line": ["I hurry down the ramp. \"Wait for me, please!\""],
    }

    def __init__(self):
        self.reset()

    def reset(self):
        self.requests = []          # (kind, body)
        self.script = {}
        self.used = {}
        self.on_request = None

    @staticmethod
    def kind_of(messages):
        system = messages[0]["content"] if messages and messages[0]["role"] == "system" else ""
        last = messages[-1]["content"] if messages else ""
        if "FIRSTLINE-PROMPT" in system:
            return "first_line"
        for marker, kind in (("GREETING-PROMPT", "greeting"), ("ANSWER-PROMPT", "answer"),
                             ("CONTINUE-PROMPT", "continuation"), ("QUESTION-PROMPT", "question")):
            if marker in last:
                return kind
        return "unknown"

    def kinds(self):
        return [kind for kind, _ in self.requests]

    def bodies(self, kind):
        return [body for k, body in self.requests if k == kind]

    def post(self, url, headers=None, data=None, timeout=None):
        body = json.loads(data)
        kind = self.kind_of(body["messages"])
        self.requests.append((kind, body))
        if self.on_request:
            self.on_request(kind, body)
        replies = self.script.get(kind, self.DEFAULTS.get(kind, ["Unrouted request text."]))
        n = self.used.get(kind, 0)
        self.used[kind] = n + 1
        reply = replies[min(n, len(replies) - 1)]
        status, text = reply if isinstance(reply, tuple) else (200, reply)
        return Resp(status, text, {"prompt_tokens": 11, "completion_tokens": 7})


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
    {"name": "Oren", "age": "41", "gender": "Male", "race": "Human", "job": "Archivist",
     "clothing": "Wool vest.", "appearance": "Stooped, ink-stained.", "backstory": "Keeps the town records.",
     "personality": "Fussy, kind.", "traits": "", "setting": "", "class": ""},
]


def reset_state():
    ROUTER.reset()
    del CACHE_CALLS[:]
    del SANITIZE_CALLS[:]
    app_state.task_retry_counts.clear()
    app_state.task_metadata.clear()
    app_state.completed_task_ids.clear()
    app_state.failed_task_ids.clear()
    app_state.pending_task_ids.clear()
    app_state.stop_processing = False
    app_state.pause_processing = False
    app_state.processing_active = True
    app_state.character_counter = 0
    app_state.system_prompt_counter = 0
    app_state.question_history = []
    app_state.db_pool = None
    g.MAX_TASK_REQUEUES = 50
    for key in [k for k in CONFIG if k != 'quality.enabled']:
        del CONFIG[key]
    out = os.path.join("output", "output.jsonl")
    if os.path.exists(out):
        os.remove(out)


def read_rows():
    path = os.path.join("output", "output.jsonl")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def chunk_task(task_id="book.txt_chunk_at_0", context="The bridge was closed for repairs."):
    return (task_id, "book.txt", 0, "The bridge was closed.", context)


def worker_kwargs(**overrides):
    kwargs = dict(
        use_questions_file_local=False, use_variable_system_local=False,
        all_api_configs_local=[SLOT, EMPTY, EMPTY, EMPTY, EMPTY, EMPTY],
        active_enabled_api_configs_for_worker=[{"config": SLOT, "original_slot_idx": 0}],
        current_question_prompt=QUESTION_PROMPT, current_answer_prompt=ANSWER_PROMPT,
        current_user_continuation_prompt=CONTINUE_PROMPT, current_num_turns=1,
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
        current_slop_to_anti_slop_fallback=False, current_lore="")
    kwargs.update(overrides)
    return kwargs


def run_worker(task, **overrides):
    """Run the real worker on one task, then the sentinel. Returns the task ids left in the queue
    (a requeued task lands after the sentinel) and the queue's unfinished-task count."""
    q = Queue()
    q.processed_tasks_lock = threading.Lock()
    q.processed_tasks = 0
    q.total_tasks_for_progress = 100
    q.api_processed_tasks = {}
    q.start_times_list = []
    q.overall_task_times_list = []
    q.api_start_times_list = {}
    app_state.task_metadata[task[0]] = {"file": task[1], "retries": 0, "task_data": task}
    app_state.pending_task_ids.add(task[0])
    q.put(task)
    q.put(None)
    g.worker(0, q, threading.Lock(), **worker_kwargs(**overrides))
    left = []
    while not q.empty():
        item = q.get()
        q.task_done()
        left.append(item[0] if item else item)
    return left, q.unfinished_tasks


def write(history, task_id="t", **kwargs):
    g.write_conversation(None, history, False, False, False, False, False, False, False, "sharegpt",
                         task_id, api_slot_idx_for_output_file=None, **kwargs)


def u(text):
    return {"role": "user", "content": text}


def a(text):
    return {"role": "assistant", "content": text}


# --- (1) Completion check ---------------------------------------------------------------------

def test_conversation_is_complete():
    done = g.conversation_is_complete
    check("ordinary complete at num_turns 1", done([u("q"), a("r")], 1))
    check("ordinary complete at num_turns 2", done([u("q"), a("r"), u("c"), a("r2")], 2))
    check("ordinary incomplete at num_turns 2", not done([u("q"), a("r")], 2))
    check("ending on a user turn is incomplete", not done([u("q"), a("r"), u("c")], 2))
    check("question only is incomplete", not done([u("q")], 1))
    check("empty history is incomplete", not done([], 1))
    check("opening, failed last answer at num_turns 1: incomplete",
          not done([a("g"), u("f")], 1, has_greeting=True))
    check("opening, failed last answer at num_turns 2: incomplete",
          not done([a("g"), u("f"), a("r"), u("c")], 2, has_greeting=True))
    check("opening, one reply short at num_turns 2: incomplete",
          not done([a("g"), u("f"), a("r")], 2, has_greeting=True))
    check("opening complete at num_turns 1", done([a("g"), u("f"), a("r")], 1, has_greeting=True))
    check("opening complete at num_turns 2",
          done([a("g"), u("f"), a("r"), u("c"), a("r2")], 2, has_greeting=True))
    check("greeting only is incomplete", not done([a("g")], 1, has_greeting=True))
    # The message count this replaces accepted both failed opening histories.
    check("old message count would accept [greeting, user] at num_turns 1", len([a("g"), u("f")]) >= 2)


def test_ordinary_worker_uses_completion_check():
    reset_state()
    ROUTER.script["answer"] = ["She nods. \"First reply.\"", (500, "server error")]
    g.MAX_TASK_REQUEUES = 0
    left, unfinished = run_worker(chunk_task(), current_num_turns=2)
    check("ordinary task failing on its last answer writes no row", read_rows() == [])
    check("ordinary task failing on its last answer is not completed", not app_state.completed_task_ids)
    check("ordinary worker calls task_done once per task", unfinished == 0)
    reset_state()
    left, unfinished = run_worker(chunk_task(), current_num_turns=2)
    rows = read_rows()
    check("ordinary task at num_turns 2 writes human, gpt, human, gpt",
          len(rows) == 1 and [m["from"] for m in rows[0]["conversations"]] == ["human", "gpt", "human", "gpt"])
    check("ordinary rows carry no train key",
          all("train" not in m for row in rows for m in row["conversations"]))


# --- (1) Row writer ---------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params):
        self.log.append((sql, params))


class FakeConn:
    def __init__(self, log):
        self.log = log
        self.commits = 0

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        self.commits += 1


class FakePool:
    def __init__(self):
        self.log = []
        self.conn = FakeConn(self.log)
        self.returned = 0

    def getconn(self):
        return self.conn

    def putconn(self, conn):
        self.returned += 1


OPENING_HISTORY = [a("The lamp gutters. \"Close the door,\" she says."), u("I shut it behind me."),
                   a("She pours two cups.")]


def test_write_conversation():
    reset_state()
    plain = [u("Hello there."), a("Hello back.")]
    write(plain, task_id="plain")
    with open(os.path.join("output", "output.jsonl"), encoding="utf-8") as f:
        text = f.read()
    expected = json.dumps({"id": "plain", "conversations": [{"from": "human", "value": "Hello there."},
                                                            {"from": "gpt", "value": "Hello back."}]}) + "\n"
    check("both options off: row is byte-identical to the existing format", text == expected)

    reset_state()
    history = json.loads(json.dumps(OPENING_HISTORY))
    write(history, task_id="masked", mask_greeting=True)
    write(history, task_id="placeholder", mask_greeting=True, leading_placeholder="[Start a new chat]")
    rows = read_rows()
    masked, placed = rows[0]["conversations"], rows[1]["conversations"]
    check("mask_greeting marks the first gpt record with train false, after from and value",
          masked[0]["from"] == "gpt" and list(masked[0]) == ["from", "value", "train"]
          and masked[0]["train"] is False)
    check("mask_greeting leaves later records without a train key",
          all("train" not in m for m in masked[1:]))
    check("leading_placeholder prepends a human record",
          placed[0] == {"from": "human", "value": "[Start a new chat]"})
    check("with a placeholder the greeting is still the masked record",
          placed[1].get("train") is False and placed[1]["from"] == "gpt"
          and all("train" not in m for m in placed[2:]))
    check("train is written as JSON false", '"train": false' in json.dumps(rows[0]))
    check("history passed in is unchanged", history == OPENING_HISTORY)

    reset_state()
    CONFIG['database.enabled'] = True
    app_state.db_pool = FakePool()
    write(history, task_id="placeholder", mask_greeting=True, leading_placeholder="[Start a new chat]")
    pool = app_state.db_pool
    check("DB path inserts once and returns the connection",
          len(pool.log) == 1 and pool.conn.commits == 1 and pool.returned == 1)
    check("DB path writes no file", read_rows() == [])
    inserted = json.loads(pool.log[0][1][1])
    check("DB row matches the file row", inserted == rows[1])
    check("DB path leaves the history unchanged", history == OPENING_HISTORY)


def main():
    print("(1) completion check")
    test_conversation_is_complete()
    test_ordinary_worker_uses_completion_check()
    print("(1) row writer")
    test_write_conversation()
    if FAILURES:
        print(f"\n{len(FAILURES)} opening-mode test(s) failed")
        sys.exit(1)
    print("\nall opening-mode tests passed")


if __name__ == "__main__":
    main()
