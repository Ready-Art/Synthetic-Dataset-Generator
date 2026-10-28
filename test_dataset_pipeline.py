"""Orchestration simulation for dataset bindings: python -B -m unittest test_dataset_pipeline.

These tests exercise real local readers, the binding manager, and the renderer
through a test-only multi-request pipeline. They DO NOT import or test the
application's generation.worker: dataset integration into that worker is a later
milestone. Request/history shapes follow its question, answer, continuation, and
cleanup stages, but the scheduler, transport, retry loop, and cache below are
deliberately small simulations, not new production implementations.

All model replies and failures are fixtures. No GUI, network, credentials,
optional dependencies, or real model requests are involved. Expected request
bodies are assembled independently from fixture rows, never with the renderer
under test or by copying values out of an actual request.
"""

import csv
import hashlib
import json
import re
import tempfile
import threading
import unittest
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from queue import Queue
from unittest.mock import patch

from dataset_bindings import DatasetBindingManager, SourceBinding, TaskBinding
from dataset_sources import DatasetSource, SourceSpec, open_source
from dataset_templates import render_template


STAGES = ("question", "answer", "cleanup", "continuation", "answer_2", "cleanup_2")
SHARED_TEMPLATES = {
    "system": "Record {{ds:record_id}}; label={{ds:label}}; policy={{ds:policy}}.\nContext: {context}",
    "question": "Question for {{ds:label}} / {{ds:label}}; subject={subject}; note={{ds:note}}.",
    "answer": "Answer {{ds:record_id}}; repeat={{ds:record_id}}; policy={{ds:policy}}.",
    "continuation": "Continue {{ds:label}}; last={last_assistant_message}",
    "cleanup": "Clean {{ds:record_id}}; rules={{ds:requirements}}; note={{ds:note}}.\nOriginal: {original_text}",
}
EDITOR_SYSTEM = "You edit records. Preserve literal data."


def serialized(payload):
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class SimulatedJob:
    task_id: str
    subject: str = "input {context} {{ds:label}}"
    context: str = "context {subject} {{ds:record_id}}"


@dataclass(frozen=True)
class RequestObservation:
    run_id: str
    task_id: str
    worker: str
    thread_id: int
    stage: str
    attempt: int
    body: str
    cache_key: str
    cache_hit: bool


@dataclass(frozen=True)
class PipelineResult:
    binding: TaskBinding
    history: tuple


class TransientFailure(Exception):
    """A simulated 503 response; never an actual network failure."""


class RetryExhausted(Exception):
    """The simulated scheduler must requeue the original logical job."""


class CountingSource(DatasetSource):
    """Wrap a real reader; count rows yielded for allocation, not schema reads."""

    def __init__(self, reader):
        super().__init__(reader.spec)
        self.reader = reader
        self.row_ids = []
        self._lock = threading.Lock()

    def schema(self):
        return self.reader.schema()

    def iter_rows(self):
        iterator = self.reader.iter_rows()
        try:
            for row in iterator:
                with self._lock:
                    self.row_ids.append(row.row_id)
                yield row
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()

    def close(self):
        self.reader.close()


class RecordingTransport:
    """Return fixtures selected from actual request content; record failed calls too."""

    def __init__(self, replies, failures=None, question_barrier=None):
        self.replies = replies
        self.failures = dict(failures or {})
        self.question_barrier = question_barrier
        self.calls = []
        self._lock = threading.Lock()

    def post(self, observation):
        key = (observation.run_id, observation.task_id, observation.stage)
        with self._lock:
            remaining = self.failures.get(key, 0)
            if remaining:
                self.failures[key] = remaining - 1
            self.calls.append((observation, 503 if remaining else 200))
        # A barrier here demonstrates that row-allocation coordination does not
        # prevent twenty already-bound jobs from reaching the API concurrently.
        if self.question_barrier is not None and observation.stage == "question" and observation.attempt == 1:
            self.question_barrier.wait(timeout=10)
        if remaining:
            raise TransientFailure("simulated HTTP 503")
        messages = json.loads(observation.body)["messages"]
        text = "\n".join(message["content"] for message in messages)
        record = re.search(r"\brecord-\d+\b", text)
        if record is None:
            raise AssertionError("A rendered request has no record identifier.")
        # The response depends on the payload's record, not its scheduler ID.
        # A stale request or bad cache key therefore also corrupts later history.
        return self.replies[(record.group(), observation.stage)]


class SimulatedPipeline:
    """Test-only caller that keeps bindings separate from worker/request identity."""

    def __init__(self, manager, transport, max_attempts=3, use_cache=False):
        self.manager = manager
        self.transport = transport
        self.max_attempts = max_attempts
        self.use_cache = use_cache
        self.templates = SHARED_TEMPLATES
        self.requests = []
        self.cache = {}
        self._lock = threading.Lock()

    def _call(self, binding, worker, stage, messages):
        body = serialized({"model": "simulated-model", "temperature": 0, "messages": messages})
        cache_key = hashlib.sha256(body.encode("utf-8")).hexdigest()
        for attempt in range(1, self.max_attempts + 1):
            with self._lock:
                hit = self.use_cache and cache_key in self.cache
                observation = RequestObservation(
                    binding.run_id, binding.task_id, worker, threading.get_ident(),
                    stage, attempt, body, cache_key, hit,
                )
                self.requests.append(observation)
                if hit:
                    return self.cache[cache_key]
            try:
                reply = self.transport.post(observation)
            except TransientFailure as exc:
                if attempt == self.max_attempts:
                    raise RetryExhausted("{} exhausted its retries".format(stage)) from exc
                continue
            if self.use_cache:
                with self._lock:
                    self.cache[cache_key] = reply
            return reply
        raise AssertionError("Request loop did not return or fail.")

    def execute(self, job, worker, binding=None):
        if binding is None:
            binding = self.manager.bind(job.task_id)
        if binding.task_id != job.task_id:
            raise ValueError("A saved binding must belong to this logical job.")
        if self.manager is not None and binding.run_id != self.manager.run_id:
            raise ValueError("A saved binding must belong to this run.")
        values = binding.values
        native = {"subject": job.subject, "context": job.context}
        system = render_template(self.templates["system"], values, native)
        history = []

        def request(stage, prompt):
            messages = [{"role": "system", "content": system}] + history + [{"role": "user", "content": prompt}]
            return self._call(binding, worker, stage, messages)

        question = request("question", render_template(self.templates["question"], values, native))
        history.append({"role": "user", "content": question})
        for number in (1, 2):
            suffix = "" if number == 1 else "_2"
            draft = request("answer" + suffix, render_template(self.templates["answer"], values))
            edit = render_template(self.templates["cleanup"], values, {"original_text": draft})
            clean = self._call(binding, worker, "cleanup" + suffix, [
                {"role": "system", "content": EDITOR_SYSTEM},
                *history,
                {"role": "user", "content": edit},
            ])
            history.append({"role": "assistant", "content": clean})
            if number == 1:
                continuation = request("continuation", render_template(
                    self.templates["continuation"], values, {"last_assistant_message": clean}
                ))
                history.append({"role": "user", "content": continuation})
        return PipelineResult(binding, tuple((message["role"], message["content"]) for message in history))


class DatasetPipelineSimulationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.original_templates = dict(SHARED_TEMPLATES)

    def tearDown(self):
        self.assertEqual(SHARED_TEMPLATES, self.original_templates, "A worker mutated shared prompt templates.")

    def fixture(self, run_id, count, start=0):
        directory = self.directory / run_id
        directory.mkdir()
        records = [{
            "record_id": "record-{:03d}".format(start + index),
            "label": "Label {:03d}".format(start + index),
            "note": 'literal {{ds:label}} {context} / "é"\nC:\\records\\' + str(start + index),
        } for index in range(count)]
        policies = [{
            "policy": "policy-{:03d}".format(100 + start + index),
            "requirements": {"tag": "{{ds:record_id}}", "flag": False},
        } for index in range(count)]
        records_path = directory / "records.csv"
        policies_path = directory / "policies.jsonl"
        with records_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=("record_id", "label", "note"))
            writer.writeheader()
            writer.writerows(records)
        policies_path.write_text("".join(json.dumps(row) + "\n" for row in policies), encoding="utf-8")
        sources = {
            "records": CountingSource(open_source(SourceSpec("records", "csv", str(records_path)))),
            "policies": CountingSource(open_source(SourceSpec("policies", "jsonl", str(policies_path)))),
        }
        manager = DatasetBindingManager(run_id, [
            SourceBinding(sources["records"], {"record_id": "record_id", "label": "label", "note": "note"}),
            SourceBinding(sources["policies"], {"policy": "policy", "requirements": "requirements"}),
        ])
        self.addCleanup(manager.close)
        replies = {}
        for record in records:
            for stage in STAGES:
                replies[(record["record_id"], stage)] = (
                    stage + " reply for " + record["record_id"] + ": literal {{ds:label}} {context}."
                )
        return {"manager": manager, "sources": sources, "records": records,
                "policies": policies, "replies": replies, "paths": (records_path, policies_path)}

    def expected_payloads(self, fixture, binding, job):
        # Source row IDs select independently authored fixture records. We do not
        # derive expected prompts from binding.values, SHARED_TEMPLATES, or render_template.
        row_ids = {source.spec.source_id: int(source.row_id) for source in binding.sources}
        record = fixture["records"][row_ids["records"]]
        policy = fixture["policies"][row_ids["policies"]]
        record_id, label, note = record["record_id"], record["label"], record["note"]
        replies = {stage: fixture["replies"][(record_id, stage)] for stage in STAGES}
        system = "Record " + record_id + "; label=" + label + "; policy=" + policy["policy"] + ".\nContext: " + job.context
        question = "Question for " + label + " / " + label + "; subject=" + job.subject + "; note=" + note + "."
        answer = "Answer " + record_id + "; repeat=" + record_id + "; policy=" + policy["policy"] + "."
        cleanup_prefix = "Clean " + record_id + '; rules={"flag":false,"tag":"{{ds:record_id}}"}; note=' + note + ".\nOriginal: "
        q = {"role": "user", "content": replies["question"]}
        a = {"role": "assistant", "content": replies["cleanup"]}
        c = {"role": "user", "content": replies["continuation"]}
        sys = {"role": "system", "content": system}
        editor = {"role": "system", "content": "You edit records. Preserve literal data."}
        messages = {
            "question": [sys, {"role": "user", "content": question}],
            "answer": [sys, q, {"role": "user", "content": answer}],
            "cleanup": [editor, q, {"role": "user", "content": cleanup_prefix + replies["answer"]}],
            "continuation": [sys, q, a, {"role": "user", "content": "Continue " + label + "; last=" + replies["cleanup"]}],
            "answer_2": [sys, q, a, c, {"role": "user", "content": answer}],
            "cleanup_2": [editor, q, a, c, {"role": "user", "content": cleanup_prefix + replies["answer_2"]}],
        }
        return {stage: {"model": "simulated-model", "temperature": 0, "messages": body}
                for stage, body in messages.items()}

    def assert_requests(self, fixture, pipeline, job, binding):
        expected = self.expected_payloads(fixture, binding, job)
        requests = [item for item in pipeline.requests
                    if (item.run_id, item.task_id) == (binding.run_id, job.task_id)]
        self.assertTrue(requests)
        for item in requests:
            with self.subTest(job=job.task_id, stage=item.stage, attempt=item.attempt):
                self.assertEqual(json.loads(item.body), expected[item.stage])
        return requests

    def assert_allocations(self, fixture, count):
        for source in fixture["sources"].values():
            self.assertEqual(source.row_ids, [str(index) for index in range(count)])

    def test_all_stages_and_reused_worker_keep_source_values_and_generated_text_literal(self):
        fixture = self.fixture("run", 2)
        transport = RecordingTransport(fixture["replies"])
        pipeline = SimulatedPipeline(fixture["manager"], transport)
        for index in range(2):
            job = SimulatedJob("job-{}".format(index))
            result = pipeline.execute(job, "same-worker")
            requests = self.assert_requests(fixture, pipeline, job, result.binding)
            self.assertEqual([request.stage for request in requests], list(STAGES))
            expected_record = fixture["records"][index]["record_id"]
            self.assertEqual(result.history[-1], ("assistant", fixture["replies"][(expected_record, "cleanup_2")]))
            self.assertIn("{{ds:label}} {context}", result.history[-1][1])
        self.assert_allocations(fixture, 2)
        self.assertEqual(len(transport.calls), 12)

    def test_transient_retries_repeat_exact_requests_without_allocating_more_rows(self):
        fixture = self.fixture("run", 2)
        failures = {("run", "job", "question"): 1, ("run", "job", "answer"): 1, ("run", "job", "cleanup"): 2}
        transport = RecordingTransport(fixture["replies"], failures)
        pipeline = SimulatedPipeline(fixture["manager"], transport)
        job = SimulatedJob("job")
        result = pipeline.execute(job, "worker")
        requests = self.assert_requests(fixture, pipeline, job, result.binding)
        counts = Counter(request.stage for request in requests)
        self.assertEqual(counts, {"question": 2, "answer": 2, "cleanup": 3, "continuation": 1, "answer_2": 1, "cleanup_2": 1})
        for stage in STAGES:
            attempts = [request for request in requests if request.stage == stage]
            self.assertEqual(len({request.body for request in attempts}), 1)
            self.assertEqual(len({request.cache_key for request in attempts}), 1)
        self.assertEqual(Counter(status for _, status in transport.calls), {503: 4, 200: 6})
        self.assert_allocations(fixture, 1)

    def test_whole_job_requeue_moves_to_another_thread_without_rebinding(self):
        fixture = self.fixture("run", 2)
        transport = RecordingTransport(fixture["replies"], {("run", "retry-job", "answer"): 2})
        pipeline = SimulatedPipeline(fixture["manager"], transport, max_attempts=2)
        original_job, other_job = SimulatedJob("retry-job"), SimulatedJob("other-job")
        queue = Queue()
        queue.put(original_job)
        requeued, replacement_finished = threading.Event(), threading.Event()

        def first_worker():
            job = queue.get(timeout=10)
            try:
                with self.assertRaises(RetryExhausted):
                    pipeline.execute(job, "worker-a")
                queue.put(job)
                requeued.set()
                if not replacement_finished.wait(timeout=10):
                    raise AssertionError("Replacement worker did not finish.")
            finally:
                queue.task_done()

        def replacement_worker():
            try:
                if not requeued.wait(timeout=10):
                    raise AssertionError("Original job was not requeued.")
                other = pipeline.execute(other_job, "worker-b")
                job = queue.get(timeout=10)
                try:
                    return job, pipeline.execute(job, "worker-b"), other
                finally:
                    queue.task_done()
            finally:
                replacement_finished.set()

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(first_worker)
            second = pool.submit(replacement_worker)
            handed_job, retried, other = second.result(timeout=15)
            first.result(timeout=15)
        self.assertIs(handed_job, original_job)
        self.assertEqual(queue.unfinished_tasks, 0)
        requests = self.assert_requests(fixture, pipeline, original_job, retried.binding)
        self.assert_requests(fixture, pipeline, other_job, other.binding)
        self.assertEqual({request.worker for request in requests}, {"worker-a", "worker-b"})
        self.assertEqual(len({request.thread_id for request in requests}), 2)
        self.assertEqual({source.row_id for source in retried.binding.sources}, {"0"})
        self.assertEqual({source.row_id for source in other.binding.sources}, {"1"})
        self.assertEqual(len(transport.calls), 15)
        self.assert_allocations(fixture, 2)

    def run_concurrently(self, pipeline, jobs):
        start = threading.Barrier(len(jobs))

        def execute(item):
            index, job = item
            start.wait(timeout=10)
            return pipeline.execute(job, "worker-{}".format(index))

        with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
            futures = [pool.submit(execute, item) for item in enumerate(jobs)]
            return [future.result(timeout=15) for future in futures]

    def test_twenty_jobs_reach_transport_concurrently_with_independent_histories(self):
        fixture = self.fixture("run", 20)
        transport = RecordingTransport(fixture["replies"], question_barrier=threading.Barrier(20))
        pipeline = SimulatedPipeline(fixture["manager"], transport)
        jobs = [SimulatedJob("job-{}".format(index)) for index in range(20)]
        results = self.run_concurrently(pipeline, jobs)
        self.assertEqual(len({id(result.binding) for result in results}), 20)
        self.assertEqual(len({request.thread_id for request, _ in transport.calls}), 20)
        for job, result in zip(jobs, results):
            self.assertEqual(len(self.assert_requests(fixture, pipeline, job, result.binding)), 6)
        self.assertEqual(len(transport.calls), 120)
        self.assert_allocations(fixture, 20)

    def test_same_task_race_allocates_once_even_when_multiple_callers_make_requests(self):
        fixture = self.fixture("run", 2)
        transport = RecordingTransport(fixture["replies"], question_barrier=threading.Barrier(20))
        pipeline = SimulatedPipeline(fixture["manager"], transport)
        job = SimulatedJob("same-job")
        results = self.run_concurrently(pipeline, [job] * 20)
        self.assertTrue(all(result.binding is results[0].binding for result in results))
        self.assertTrue(all(result.history == results[0].history for result in results))
        self.assert_requests(fixture, pipeline, job, results[0].binding)
        # Binding deduplication does not claim to deduplicate task scheduling.
        self.assertEqual(len(transport.calls), 120)
        self.assert_allocations(fixture, 1)

    def test_snapshot_and_original_job_replay_without_source_access_or_run_confusion(self):
        fixture = self.fixture("original-run", 1)
        job = SimulatedJob("same-task-id")
        first = SimulatedPipeline(fixture["manager"], RecordingTransport(fixture["replies"]))
        result = first.execute(job, "original-worker")
        saved = json.dumps({"job": asdict(job), "binding": result.binding.to_dict()}, ensure_ascii=False)
        fixture["manager"].close()
        for path in fixture["paths"]:
            path.unlink()
        replay = SimulatedPipeline(None, RecordingTransport(fixture["replies"]))
        with patch("dataset_sources.open_source", side_effect=AssertionError("Replay reopened a source")), \
             patch("dataset_sources.LocalFileSource.iter_rows", side_effect=AssertionError("Replay sampled a row")):
            decoded = json.loads(saved)
            restored = TaskBinding.from_dict(decoded["binding"])
            replayed = replay.execute(SimulatedJob(**decoded["job"]), "new-worker", restored)
        self.assertEqual(replayed, result)
        self.assertEqual([request.body for request in replay.requests], [request.body for request in first.requests])
        self.assert_allocations(fixture, 1)

        different = self.fixture("different-run", 1, start=100)
        next_pipeline = SimulatedPipeline(different["manager"], RecordingTransport(different["replies"]))
        next_result = next_pipeline.execute(job, "new-run-worker")
        self.assertNotEqual(next_result.binding.run_id, restored.run_id)
        self.assertNotEqual(next_result.binding.values, restored.values)
        self.assert_requests(different, next_pipeline, job, next_result.binding)
        self.assert_allocations(different, 1)

    def test_cache_keys_use_rendered_requests_and_cannot_mix_different_job_values(self):
        fixture = self.fixture("run", 2)
        transport = RecordingTransport(fixture["replies"])
        pipeline = SimulatedPipeline(fixture["manager"], transport, use_cache=True)
        first_job, second_job = SimulatedJob("one"), SimulatedJob("two")
        first = pipeline.execute(first_job, "worker-a")
        second = pipeline.execute(second_job, "worker-a")
        replayed = pipeline.execute(first_job, "worker-b")
        self.assertEqual(replayed, first)
        self.assertNotEqual(first.history, second.history)
        self.assertEqual(len(transport.calls), 12)
        self.assertEqual(len(pipeline.cache), 12)
        self.assertTrue(all(request.cache_hit for request in pipeline.requests[-6:]))
        first_requests = self.assert_requests(fixture, pipeline, first_job, first.binding)
        second_requests = self.assert_requests(fixture, pipeline, second_job, second.binding)
        for stage in STAGES:
            keys_a = {request.cache_key for request in first_requests if request.stage == stage}
            keys_b = {request.cache_key for request in second_requests if request.stage == stage}
            self.assertTrue(keys_a.isdisjoint(keys_b))
        self.assert_allocations(fixture, 2)


if __name__ == "__main__":
    unittest.main()
