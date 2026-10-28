"""Compare dataset-enabled request traces with the exact upstream engine.

Run separately: ``python -B test_dataset_compatibility.py``. The upstream commit
below must exist locally (it is the parent of this feature's source/UI commits).
Each side runs in its own process and temporary directory. The real worker,
request builders, paragraph fixers, state saver and output writer execute;
the existing opening harness supplies fake HTTP, no cache hits, no waits, and
GUI-variable stand-ins. No model, network connection or display is used.

This is a bounded compatibility check, not a claim of exhaustive equivalence.
The baseline receives explicitly populated authored inputs; the candidate gets
the same values from real CSV/JSONL sources. Dynamic native formatting remains
the upstream engine's responsibility on both sides.
"""

import ast
import csv
import io
import json
import os
from pathlib import Path
import random
import re
import subprocess
import sys
import tarfile
import tempfile
import threading
import unittest
from queue import Queue


REPO = Path(__file__).resolve().parent
BASELINE_COMMIT = "f04ed7bccea81e33dd79ebd1275e6c46c0c8e267"
RAW_LITERAL = "{{ds:unmapped_source}}"
MODEL_LITERAL = "{{ds:unmapped_model}} {last_assistant_message}"
CELL_LITERAL = "Literal <tag> Unicode \u03a9 and \\path"


def _values(index, scenario):
    return {
        "row": "R{}".format(index),
        "name": ("Ada", "Bo")[index],
        "literal": CELL_LITERAL,
        "setting": "" if scenario.startswith("settings_") else "Depot {}".format(index),
        "fixed_setting": "Fixed wing {}".format(index),
        "arc": "calm -> alert -> relieved",
        "continuation": "",
    }


def _materialize(value, values):
    """Independent fixture preparation, not the candidate's renderer."""
    if isinstance(value, str):
        for alias, text in values.items():
            value = value.replace("{{ds:" + alias + "}}", text)
        return value
    if isinstance(value, list):
        return [_materialize(item, values) for item in value]
    if isinstance(value, tuple):
        return tuple(_materialize(item, values) for item in value)
    if isinstance(value, dict):
        return {key: _materialize(item, values) for key, item in value.items()}
    return value


class TraceHTTP:
    def __init__(self, harness, scenario):
        self.harness = harness
        self.scenario = scenario
        self.calls = []
        self.counts = {}

    def post(self, url, headers=None, data=None, timeout=None):
        body = json.loads(data)
        messages = body["messages"]
        kind = ("slop" if url.endswith("/slop") else
                "anti" if url.endswith("/anti") else self.harness.Router.kind_of(messages))
        joined = "\n".join(message["content"] for message in messages)
        match = re.search(r"ROW=(R\d+)", joined)
        row = match.group(1) if match else "missing-row"
        key = kind, row, url
        count = self.counts.get(key, 0)
        self.counts[key] = count + 1
        self.calls.append({"kind": kind, "url": url, "body": body})
        if kind == "question":
            text = "Question ROW={} preserves {}.".format(row, MODEL_LITERAL)
        elif kind == "answer":
            if self.scenario == "requeue" and count == 0:
                return self.harness.Resp(500, "Fixture transport failure", {})
            if self.scenario == "refinement" and count == 0:
                text = "I cannot help with that request. ROW={}.".format(row)
            else:
                phrase = "shivers down and secretbad" if self.scenario == "refinement" else "steady movement"
                model = "B" if url.endswith("model-b") else "A"
                text = "Answer ROW={} MODEL={} NUMBER={} uses {} and {}.".format(
                    row, model, count + 1, phrase, MODEL_LITERAL)
        elif kind in ("slop", "anti"):
            original = messages[-1]["content"].split("<original_text>\n", 1)[1].rsplit("\n</original_text>", 1)[0]
            if kind == "slop":
                text = (original + " Revision {}.".format(count) if count % 3 < 2
                        else original.replace("shivers down", "steady movement"))
            else:
                text = (original + " Editing pass." if count % 2 == 0
                        else original.replace("secretbad", "clear wording"))
        elif kind == "continuation":
            text = "Continuation ROW={} preserves {}.".format(row, MODEL_LITERAL)
        elif kind == "greeting":
            text = "Greeting ROW={} gives context and preserves {}.".format(row, MODEL_LITERAL)
        elif kind == "first_line":
            text = "First reply ROW={} preserves {}.".format(row, MODEL_LITERAL)
        else:
            raise AssertionError("Unrouted request: {}".format(body))
        return self.harness.Resp(200, text, {"prompt_tokens": 11, "completion_tokens": 7})


def _worker_inputs(harness, scenario):
    primary = dict(harness.SLOT, url="http://fixture/model-a", model="model-a")
    secondary = dict(primary, url="http://fixture/model-b", model="model-b")
    slop = dict(primary, url="http://fixture/slop", model="slop-editor")
    anti = dict(primary, url="http://fixture/anti", model="anti-editor")
    result = harness.worker_kwargs(
        all_api_configs_local=[primary] + [harness.EMPTY] * 5,
        active_enabled_api_configs_for_worker=[{"config": primary, "original_slot_idx": 0}],
        current_num_turns=2, current_max_attempts=3,
        enable_character_engine_local=False,
        current_system_prompts_for_worker=["SCENE ROW={{ds:row}} NAME={{ds:name}} CELL={{ds:literal}}"],
        current_top_level_system_prompt="TOP ROW={{ds:row}}",
        current_lore="LORE {{ds:setting}}",
        current_question_prompt=("QUESTION-PROMPT ROW={{ds:row}} NAME={{ds:name}} "
                                 "SUBJECT={subject} CONTEXT={context} RECENT={recent_questions}"),
        current_answer_prompt="ANSWER-PROMPT ROW={{ds:row}} NAME={{ds:name}} CELL={{ds:literal}}",
        current_user_continuation_prompt="CONTINUE-PROMPT ROW={{ds:row}} LAST={last_assistant_message}",
        current_jailbreaks=["REFUSAL-FIX ROW={{ds:row}} NAME={{ds:name}}"],
        current_speaking_fixes=["SPEAKING-FIX ROW={{ds:row}} NAME={{ds:name}}"],
        current_slop_fixes_fallback=["FALLBACK-FIX ROW={{ds:row}}"],
        current_slop_fixes_for_rotation_worker=["ROTATE ROW={{ds:row}} NAME={{ds:name}}"],
        include_metadata_local=True,
    )
    if scenario == "refinement":
        result.update(slop_fixer_api_config=slop, anti_slop_fixer_api_config_runtime=anti,
                      anti_slop_fixer_api_config_param=anti, current_anti_slop_phrases=["secretbad"])
    elif scenario == "duplication":
        result.update(master_duplication_enabled_local=True,
                      all_api_configs_local=[primary, secondary] + [harness.EMPTY] * 4)
    elif scenario.startswith("opening"):
        character = dict(harness.CHARACTERS[0], name="Profile {{ds:name}}",
                         setting="{{ds:setting}}", persona="INNER {{ds:name}}",
                         backstory="PROFILE {{ds:literal}}", greeting="A fixed profile greeting.")
        result.update(
            opening_mode_local=True, enable_character_engine_local=True,
            enable_setting_selection_local=True, character_list=[character],
            enable_emotional_states_local=True, emotional_states_list_local=["{{ds:arc}}"],
            enable_random_names_local=True, random_names_list_local=["{{ds:name}}"],
            greeting_prompt_local="GREETING-PROMPT ROW={{ds:row}} SPEAKER={speaker} CONTEXT={context}",
            greeting_context_local="GCTX CELL={{ds:literal}} RAW={chunk}",
            first_line_prompt_local="FIRSTLINE-PROMPT ROW={{ds:row}} STYLE={style}",
            first_line_styles_local=["BRIEF {{ds:name}}"], placeholder_share_local=1.0,
            persona_breakthrough_turn_local=2,
        )
        if scenario == "opening_empty_context":
            result["greeting_context_local"] = "{{ds:continuation}}"
    elif scenario.startswith("settings_"):
        characters = [dict(harness.CHARACTERS[0], name="First {{ds:name}}", setting="{{ds:setting}}")]
        if scenario == "settings_ensemble":
            characters.extend([
                dict(harness.CHARACTERS[1], name="Second {{ds:name}}", setting="{{ds:fixed_setting}}"),
                dict(harness.CHARACTERS[1], name="Third {{ds:name}}", setting="{{ds:fixed_setting}}"),
            ])
        result.update(enable_character_engine_local=True, enable_setting_selection_local=True,
                      character_list=characters, num_characters_local=len(characters),
                      enable_random_settings_local=True,
                      random_settings_list_local=["Pool East {{ds:row}}", "Pool West {{ds:row}}"])
    elif scenario == "requeue":
        result.update(current_num_turns=1, current_max_attempts=1)
    elif scenario == "empty_continuation":
        result.update(current_user_continuation_prompt="{{ds:continuation}}")
    return result


def _dataset_config(rows):
    from dataset_settings import apply_schema, new_source
    from dataset_sources import SourceSpec, open_source
    settings = {"sources": []}
    path = Path("people.csv").resolve()
    columns = ["row", "name", "literal", "arc", "continuation"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    other = Path("places.jsonl").resolve()
    other.write_text("".join(json.dumps({key: row[key] for key in ("setting", "fixed_setting")}) + "\n"
                             for row in rows), encoding="utf-8")
    for kind, location, mapped in (("csv", path, columns), ("jsonl", other, ["setting", "fixed_setting"])):
        source = new_source(settings["sources"])
        source.update(kind=kind, location=str(location), column_aliases={key: key for key in mapped})
        reader = open_source(SourceSpec(source["source_id"], kind, str(location)))
        try:
            settings["sources"].append(apply_schema(source, reader.schema()))
        finally:
            reader.close()
    return settings


def _trace(repo, candidate, scenario, destination):
    # The harness changes cwd and monkeypatches module globals; always run this
    # entry point in a separate process, with the chosen source tree first.
    sys.path.insert(0, str(repo))
    import test_opening_mode as harness
    g, state = harness.g, harness.app_state
    harness.reset_state()
    state.purged_task_ids.clear()
    state.quality_scores.clear()
    state.quality_review_ids.clear()
    state.quality_enabled = False
    state.quality_output_filter = False
    state.use_conversation_history_for_fixers = True
    state.master_duplication_enabled_var = harness.Var(scenario == "duplication")
    g.MAX_TASK_REQUEUES = 2
    harness.CONFIG.update({
        "generation.max_slop_sentence_fix_iterations": 5,
        "generation.max_anti_slop_fix_iterations": 5,
        "generation.discard_refused_conversations": False,
        "generation.max_text_length_malformed": 20000,
        "generation.max_newlines_malformed": 100,
        "prompts.character.include_names_in_prompt": True,
        "api.master_duplication_mode": scenario == "duplication",
    })
    server = TraceHTTP(harness, scenario)
    g.requests.post = server.post
    inputs = _worker_inputs(harness, scenario)
    rows = [_values(index, scenario) for index in range(2 if scenario == "same_worker" else 1)]
    tasks = [("original-{}".format(index), "fixture.txt", index, "SUBJECT {context}",
              "CONTEXT_{} {}".format(index, RAW_LITERAL)) for index in range(len(rows))]
    q = Queue()
    q.processed_tasks_lock = threading.Lock()
    q.processed_tasks = 0
    q.total_tasks_for_progress = len(tasks) * inputs["current_num_turns"]
    q.api_processed_tasks = {}
    q.start_times_list = []
    q.overall_task_times_list = []
    q.api_start_times_list = {}
    q.all_tasks_queued = True
    state.task_queue = q
    for task in tasks:
        state.pending_task_ids.add(task[0])
        state.task_metadata[task[0]] = {"file": task[1], "retries": 0, "task_data": task}
    service = None
    if candidate:
        from dataset_tasks import configure_queue_datasets
        service = configure_queue_datasets(q, {"sources": []} if scenario == "disabled" else _dataset_config(rows))
    random.seed(8127)
    output_lock = threading.Lock()
    phases = []

    def snapshot():
        with q.mutex:
            remaining = [item[0] if item is not None else None for item in q.queue]
        return {
            "completed": sorted(state.completed_task_ids), "failed": sorted(state.failed_task_ids),
            "pending": sorted(state.pending_task_ids), "retries": dict(state.task_retry_counts),
            "processed": q.processed_tasks, "api_processed": dict(q.api_processed_tasks),
            "queued": remaining, "unfinished": q.unfinished_tasks,
        }

    if candidate and scenario != "disabled":
        for task in tasks:
            q.put(task)
        q.put(None)
        g.worker(0, q, output_lock, **inputs)
    else:
        # Upstream has no per-task aliases. Feeding its worker explicitly
        # populated inputs for each task gives the reference trace. Reuse the
        # queue/global state so recent questions and selection counters advance.
        for task, values in zip(tasks, rows):
            q.put(task)
            q.put(None)
            g.worker(0, q, output_lock, **_materialize(inputs, values))
    if scenario == "requeue":
        phases.append(snapshot())
        q.put(None)
        g.worker(0, q, output_lock, **(inputs if candidate else _materialize(inputs, rows[0])))
    phases.append(snapshot())

    output = {}
    for path in sorted(Path("output").glob("output*.jsonl")):
        records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        for record in records:
            if "metadata" in record:
                record["metadata"].pop("generated_at", None)
        output[path.name] = records
    bindings = {}
    for task in tasks:
        binding = state.task_metadata[task[0]].get("dataset_binding")
        if binding is not None:
            bindings[task[0]] = {"values": dict(binding.values),
                                 "row_ids": [source.row_id for source in binding.sources]}
    if service is not None:
        service.close()
    result = {
        "trace": {"calls": server.calls, "output": output, "phases": phases,
                  "cache": harness.CACHE_CALLS, "question_history": state.question_history,
                  "character_counter": state.character_counter, "system_prompt_counter": state.system_prompt_counter},
        "bindings": bindings,
    }
    Path(destination).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")


class DatasetCompatibilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scratch = tempfile.TemporaryDirectory(prefix="sdg-differential-")
        cls.directory = Path(cls.scratch.name)
        override = os.environ.get("DATASET_COMPATIBILITY_BASELINE_DIR")
        if override:
            cls.baseline = Path(override).resolve()
        else:
            cls.baseline = cls.directory / "upstream"
            cls.baseline.mkdir()
            archive = subprocess.run(["git", "archive", BASELINE_COMMIT], cwd=REPO,
                                     check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
            with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as handle:
                # Only ordinary files/directories are needed. Explicit extraction
                # keeps this check usable on Python versions before tar filters.
                for member in handle:
                    target = (cls.baseline / member.name).resolve()
                    target.relative_to(cls.baseline.resolve())
                    if member.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                    elif member.isfile():
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with handle.extractfile(member) as source:
                            target.write_bytes(source.read())
                    else:
                        raise ValueError("Unexpected archive member: {}".format(member.name))

    @classmethod
    def tearDownClass(cls):
        cls.scratch.cleanup()

    def _run(self, scenario, candidate):
        target = self.directory / (scenario + ("-candidate.json" if candidate else "-baseline.json"))
        repo = REPO if candidate else self.baseline
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONIOENCODING="utf-8",
                   HF_HUB_OFFLINE="1", HF_DATASETS_OFFLINE="1")
        result = subprocess.run(
            [sys.executable, "-B", str(Path(__file__).resolve()), "--trace", str(repo),
             "candidate" if candidate else "baseline", scenario, str(target)],
            cwd=self.directory, env=env, text=True, encoding="utf-8",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=35,
        )
        self.assertEqual(result.returncode, 0, result.stdout[-3000:] + result.stderr[-5000:])
        return json.loads(target.read_text(encoding="utf-8"))

    def compare(self, scenario):
        baseline = self._run(scenario, False)
        candidate = self._run(scenario, True)
        self.maxDiff = 15000
        self.assertEqual(candidate["trace"], baseline["trace"])
        return candidate

    def test_refinement_and_corrective_regeneration_match_upstream(self):
        result = self.compare("refinement")
        calls = result["trace"]["calls"]
        self.assertEqual(sum(call["kind"] == "answer" for call in calls), 3)
        self.assertEqual(sum(call["kind"] == "slop" for call in calls), 6)
        self.assertEqual(sum(call["kind"] == "anti" for call in calls), 4)
        self.assertTrue(any("REFUSAL-FIX ROW=R0" in call["body"]["messages"][0]["content"] for call in calls))
        self.assertTrue(any(call["kind"] == "slop" and "ROTATE ROW=R0" in call["body"]["messages"][-1]["content"]
                            for call in calls))
        continuation = next(call for call in calls if call["kind"] == "continuation")
        self.assertNotIn("shivers down", continuation["body"]["messages"][-1]["content"])
        self.assertNotIn("secretbad", continuation["body"]["messages"][-1]["content"])

    def test_duplication_shared_history_and_prefix_writes_match_upstream(self):
        result = self.compare("duplication")
        rows = result["trace"]["output"]["output_api_slot_1.jsonl"]
        self.assertEqual([len(row["conversations"]) for row in rows], [2, 4])
        self.assertIn("MODEL=A", rows[1]["conversations"][1]["value"])
        self.assertIn("MODEL=B", rows[1]["conversations"][-1]["value"])

    def test_opening_perspective_persona_arc_and_metadata_match_upstream(self):
        result = self.compare("opening")
        calls = result["trace"]["calls"]
        self.assertEqual([call["kind"] for call in calls], ["greeting", "first_line", "answer", "continuation", "answer"])
        self.assertNotIn("INNER PERSONA", calls[0]["body"]["messages"][0]["content"])
        self.assertIn("INNER PERSONA", calls[2]["body"]["messages"][0]["content"])
        self.assertNotIn("SCENE", calls[1]["body"]["messages"][0]["content"])
        row = result["trace"]["output"]["output.jsonl"][0]
        self.assertEqual(row["conversations"][0]["value"], "[Start a new chat]")
        self.assertEqual(row["metadata"]["characters"], ["Ada"])

    def test_two_jobs_on_one_worker_match_manually_populated_upstream(self):
        result = self.compare("same_worker")
        self.assertEqual(result["bindings"]["original-0"]["values"]["name"], "Ada")
        self.assertEqual(result["bindings"]["original-1"]["values"]["name"], "Bo")
        self.assertEqual([result["bindings"][task]["row_ids"] for task in ("original-0", "original-1")],
                         [["0", "0"], ["1", "1"]])

    def test_empty_dataset_setting_uses_upstream_pool_fallback(self):
        result = self.compare("settings_single")
        setting = result["trace"]["output"]["output.jsonl"][0]["metadata"]["setting"]
        self.assertIn(setting, ("Pool East R0", "Pool West R0"))

    def test_ensemble_unification_and_effective_metadata_match_upstream(self):
        result = self.compare("settings_ensemble")
        row = result["trace"]["output"]["output.jsonl"][0]
        setting = row["metadata"]["setting"]
        answer = next(call for call in result["trace"]["calls"] if call["kind"] == "answer")
        settings = re.findall(r"^Setting: (.*)$", answer["body"]["messages"][0]["content"], re.MULTILINE)
        self.assertEqual(settings, [setting] * 3)

    def test_whole_job_requeue_keeps_original_decisions_and_binding(self):
        result = self.compare("requeue")
        phases = result["trace"]["phases"]
        self.assertEqual(phases[0]["queued"], ["original-0"])
        self.assertEqual(phases[0]["retries"], {"original-0": 1})
        self.assertEqual(phases[1]["completed"], ["original-0"])
        self.assertEqual(result["bindings"]["original-0"]["row_ids"], ["0", "0"])

    def test_disabled_datasets_retain_original_native_cascade(self):
        result = self.compare("disabled")
        self.assertEqual(result["bindings"], {})
        question = result["trace"]["calls"][0]["body"]["messages"][-1]["content"]
        self.assertIn("SUBJECT=SUBJECT CONTEXT_0 " + RAW_LITERAL, question)

    def test_empty_dataset_continuation_preserves_original_stop_and_requeue(self):
        result = self.compare("empty_continuation")
        self.assertEqual([call["kind"] for call in result["trace"]["calls"]], ["question", "answer"])
        self.assertEqual(result["trace"]["output"], {})
        phase = result["trace"]["phases"][-1]
        self.assertEqual(phase["completed"], [])
        self.assertEqual(phase["queued"], ["original-0"])
        self.assertEqual(phase["retries"], {"original-0": 1})

    def test_empty_dataset_greeting_context_preserves_upstream_default(self):
        result = self.compare("opening_empty_context")
        greeting = next(call for call in result["trace"]["calls"] if call["kind"] == "greeting")
        self.assertIn("CONTEXT_0 " + RAW_LITERAL, greeting["body"]["messages"][-1]["content"])

    def test_original_methods_are_structurally_preserved(self):
        allowed_changes = {
            "generation.py": {
                "worker", "generate_question", "generate_user_continuation",
                "fill_greeting_prompt", "generate_greeting", "generate_first_line",
            },
            "generate.py": {"start_processing"},
        }
        for filename, permitted in allowed_changes.items():
            def functions(path):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                return {node.name: node for node in tree.body
                        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}

            original = functions(self.baseline / filename)
            candidate = functions(REPO / filename)
            with self.subTest(file=filename, check="original functions retained"):
                self.assertTrue(set(original).issubset(candidate),
                                "Original functions removed: {}".format(sorted(set(original) - set(candidate))))
            for name, node in original.items():
                if name in permitted or name not in candidate:
                    continue
                with self.subTest(file=filename, function=name):
                    self.assertEqual(
                        ast.dump(node, include_attributes=False),
                        ast.dump(candidate[name], include_attributes=False),
                        "Original method changed: {}:{}".format(filename, name),
                    )
        for filename in ("app_state.py", "api_handler.py", "detection.py", "quality.py", "text_utils.py"):
            with self.subTest(file=filename, check="byte identity"):
                self.assertEqual((REPO / filename).read_bytes(), (self.baseline / filename).read_bytes(),
                                 "Original module changed: {}".format(filename))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--trace":
        _trace(Path(sys.argv[2]), sys.argv[3] == "candidate", sys.argv[4], sys.argv[5])
    else:
        unittest.main()
