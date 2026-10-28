"""In-memory task attachments: real files, concurrency, and queue ownership.

Run with ``python -B -m unittest test_dataset_tasks``. No model, GUI, network,
checkpoint, or changes to the generator's task/retry methods are involved.
"""

import csv
import gc
import tempfile
import threading
import unittest
import weakref
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue

from dataset_bindings import DatasetBindingError, DatasetExhaustedError
from dataset_settings import DatasetSettingsError, apply_schema
from dataset_sources import DatasetRow, DatasetSchema, SourceSpec, open_source
from dataset_tasks import (
    DatasetTaskAttachments, bind_task_datasets, configure_queue_datasets,
    render_task_inputs, validate_task_templates,
)
from dataset_templates import DatasetTemplateError


def configured_source(source_id, columns, aliases, location=None, enabled=True):
    return apply_schema({
        "source_id": source_id,
        "kind": "csv",
        "location": str(location or (source_id + ".csv")),
        "enabled": enabled,
        "column_aliases": dict(aliases),
    }, DatasetSchema(tuple(columns), "imported-" + source_id))


class ObservedReader:
    def __init__(self, spec, rows, columns=("value",), schema_error=False, close_error=False):
        self.spec = spec
        self.rows = rows
        self.columns = tuple(columns)
        self.schema_error = schema_error
        self.close_error = close_error
        self.schema_count = 0
        self.read_count = 0
        self.close_count = 0
        self.iterator_close_count = 0

    def schema(self):
        self.schema_count += 1
        if self.schema_error:
            raise ValueError("schema unavailable")
        return DatasetSchema(self.columns, "observed-" + self.spec.source_id)

    def iter_rows(self):
        try:
            for i, row in enumerate(self.rows):
                self.read_count += 1
                yield DatasetRow(str(i), row)
        finally:
            self.iterator_close_count += 1

    def close(self):
        self.close_count += 1
        if self.close_error:
            raise OSError("reader close failed")


class TaskAttachmentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="dataset-task-tests-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def csv_file(self, name, columns, rows):
        path = self.root / name
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            writer.writerows(rows)
        return path

    def service(self, settings, factory=open_source):
        service = DatasetTaskAttachments(settings, source_factory=factory)
        self.addCleanup(service.close)
        return service

    def test_unused_configuration_is_an_exact_task_passthrough(self):
        calls = []
        q = Queue()
        settings = {"sources": [
            configured_source("disabled", ("v",), {"v": "ignored"}, enabled=False),
            {"source_id": "draft", "location": "", "column_aliases": {}},
        ]}
        self.assertIsNone(configure_queue_datasets(q, settings, lambda spec: calls.append(spec)))
        self.assertFalse(hasattr(q, "dataset_attachments"))
        metadata = {"old": {"file": "source", "retries": 2}}
        self.assertIsNone(bind_task_datasets(q, object(), metadata))
        self.assertEqual(metadata, {"old": {"file": "source", "retries": 2}})
        self.assertEqual(calls, [])

    def test_setup_and_lookup_do_not_open_even_a_missing_file(self):
        calls = []
        settings = {"sources": [configured_source("missing", ("v",), {"v": "value"}, self.root / "missing.csv")]}

        def factory(spec):
            calls.append(spec)
            return open_source(spec)

        q = Queue()
        service = configure_queue_datasets(q, settings, factory)
        self.addCleanup(service.close)
        self.assertTrue(service.active)
        self.assertIsNone(service.get("book.txt_chunk_at_7"))
        self.assertEqual(calls, [])
        with self.assertRaises(DatasetBindingError):
            service.bind("book.txt_chunk_at_7")
        with self.assertRaises(DatasetBindingError):
            service.bind("another-task")
        self.assertEqual(len(calls), 1, "A failed source must not be reopened or advanced on retry")

    def test_two_real_sources_keep_correlated_values_and_original_ids(self):
        people = self.csv_file("people.csv", ("first", "last", "unused"), [
            ("Ada", "Lovelace", "not mapped"), ("Grace", "Hopper", "not mapped either"),
        ])
        items = self.csv_file("items.csv", ("category", "description"), [
            ("tool", 'Literal {{ds:first}} and {context}\n"quoted"'), ("part", "second"),
        ])
        service = self.service({"sources": [
            configured_source("people", ("first", "last", "unused"), {"first": "first", "last": "last"}, people),
            configured_source("items", ("category", "description"), {"category": "category", "description": "text"}, items),
        ]})
        first = service.bind("book.txt_chunk_at_7")
        second = service.bind("q_1")
        self.assertEqual(first.task_id, "book.txt_chunk_at_7")
        self.assertEqual(dict(first.values), {
            "first": "Ada", "last": "Lovelace", "category": "tool",
            "text": 'Literal {{ds:first}} and {context}\n"quoted"',
        })
        self.assertEqual(second.values["first"], "Grace")
        self.assertEqual(second.values["last"], "Hopper")
        self.assertEqual([source.row_id for source in first.sources], ["0", "0"])
        self.assertIs(service.bind("book.txt_chunk_at_7"), first)
        with self.assertRaises(TypeError):
            first.values["first"] = "changed"

    def test_configuration_is_detached_before_any_source_is_opened(self):
        path = self.csv_file("rows.csv", ("value",), [("original",), ("second",)])
        source = configured_source("rows", ("value",), {"value": "v"}, path)
        settings = {"sources": [source]}
        service = self.service(settings)
        source["location"] = "later-edit.csv"
        source["column_aliases"]["value"] = "other"
        source["columns"].clear()
        settings["sources"].clear()
        binding = service.bind("q_0")
        self.assertEqual(dict(binding.values), {"v": "original"})
        self.assertEqual(binding.sources[0].spec.location, str(path))

    def test_twenty_concurrent_jobs_and_twenty_retries_allocate_once(self):
        path = self.csv_file("rows.csv", ("n", "partner"), [(str(i), "pair-" + str(i)) for i in range(21)])
        service = self.service({"sources": [configured_source("rows", ("n", "partner"), {"n": "n", "partner": "partner"}, path)]})
        barrier = threading.Barrier(20)

        def bind(index):
            barrier.wait(timeout=10)
            return service.bind("task-{}".format(index))

        with ThreadPoolExecutor(max_workers=20) as pool:
            bindings = list(pool.map(bind, range(20)))
        self.assertEqual({b.values["n"] for b in bindings}, {str(i) for i in range(20)})
        for binding in bindings:
            self.assertEqual(binding.values["partner"], "pair-" + binding.values["n"])
        with ThreadPoolExecutor(max_workers=20) as pool:
            repeated = list(pool.map(lambda unused: service.bind("same-task"), range(20)))
        self.assertTrue(all(binding is repeated[0] for binding in repeated))
        self.assertEqual(repeated[0].values["n"], "20")

    def test_existing_task_and_metadata_survive_requeue_without_replacement(self):
        path = self.csv_file("rows.csv", ("value",), [("first",), ("second",)])
        q = Queue()
        service = configure_queue_datasets(q, {"sources": [configured_source("rows", ("value",), {"value": "v"}, path)]})
        self.addCleanup(service.close)
        task = ("q_0", "questions.txt", 0, "An original question")
        entry = {"file": "questions.txt", "retries": 0, "task_data": task}
        metadata = {"q_0": entry}
        lock = threading.Lock()
        q.put(task)
        original = q.get_nowait()
        first = bind_task_datasets(q, original, metadata, lock)
        q.put(original)  # The original requeue operation keeps this exact object.
        q.task_done()
        retry = q.get_nowait()
        second = bind_task_datasets(q, retry, metadata, lock)
        q.task_done()
        self.assertIs(retry, task)
        self.assertIs(second, first)
        self.assertIs(metadata["q_0"], entry)
        self.assertIs(entry["task_data"], task)
        self.assertIs(entry["dataset_binding"], first)
        self.assertEqual(entry["retries"], 0)
        self.assertEqual(service.bind("q_1").values["v"], "second")

    def test_old_queue_cannot_overwrite_newer_task_metadata(self):
        path = self.csv_file("rows.csv", ("value",), [("bound",)])
        q = Queue()
        service = configure_queue_datasets(q, {"sources": [configured_source("rows", ("value",), {"value": "v"}, path)]})
        self.addCleanup(service.close)
        old_task = ("q_0", "questions.txt", 0, "same payload")
        new_task = tuple(list(old_task))
        self.assertEqual(old_task, new_task)
        self.assertIsNot(old_task, new_task)
        newer_binding = object()
        metadata = {"q_0": {"task_data": new_task, "dataset_binding": newer_binding}}
        old_binding = bind_task_datasets(q, old_task, metadata, threading.Lock())
        self.assertEqual(old_binding.values["v"], "bound")
        self.assertIs(metadata["q_0"]["dataset_binding"], newer_binding)
        self.assertEqual(metadata["q_0"]["task_data"], old_task)

    def test_exhaustion_latches_new_allocations_but_keeps_bound_values(self):
        readers = []
        settings = {"sources": [
            configured_source("long", ("value",), {"value": "left"}),
            configured_source("short", ("value",), {"value": "right"}),
        ]}

        def factory(spec):
            rows = [{"value": "first"}, {"value": "second"}, {"value": "third"}]
            reader = ObservedReader(spec, rows if spec.source_id == "long" else rows[:1])
            readers.append(reader)
            return reader

        service = self.service(settings, factory)
        bound = service.bind("task-a")
        with self.assertRaises(DatasetExhaustedError):
            service.bind("task-b")
        with self.assertRaises(DatasetExhaustedError):
            service.bind("task-c")
        self.assertEqual([r.read_count for r in readers], [2, 1])
        self.assertIsNone(service.get("task-b"))
        self.assertIs(service.bind("task-a"), bound)

    def test_partial_factory_failure_closes_prior_readers_once(self):
        readers = []
        factory_calls = []
        settings = {"sources": [configured_source(name, ("value",), {"value": name}) for name in ("first", "second")]}

        def factory(spec):
            factory_calls.append(spec.source_id)
            if spec.source_id == "second":
                raise OSError("source unavailable")
            reader = ObservedReader(spec, [{"value": "first"}])
            readers.append(reader)
            return reader

        service = self.service(settings, factory)
        for task_id in ("a", "a", "b"):
            with self.assertRaisesRegex(DatasetBindingError, "source unavailable"):
                service.bind(task_id)
        self.assertEqual(factory_calls, ["first", "second"])
        self.assertEqual(readers[0].close_count, 1)
        self.assertEqual(readers[0].read_count, 0)

    def test_schema_failure_closes_inspected_and_uninspected_readers_once(self):
        readers = []
        settings = {"sources": [configured_source(name, ("value",), {"value": name}) for name in ("bad", "later")]}

        def factory(spec):
            reader = ObservedReader(spec, [{"value": "row"}], schema_error=(spec.source_id == "bad"))
            readers.append(reader)
            return reader

        service = self.service(settings, factory)
        with self.assertRaisesRegex(DatasetBindingError, "schema unavailable"):
            service.bind("task")
        self.assertEqual([reader.schema_count for reader in readers], [1, 0])
        self.assertEqual([reader.close_count for reader in readers], [1, 1])

    def test_releasing_queue_closes_readers_without_losing_existing_binding(self):
        readers = []

        def factory(spec):
            reader = ObservedReader(spec, [{"value": "one"}, {"value": "two"}])
            readers.append(reader)
            return reader

        q = Queue()
        service = configure_queue_datasets(q, {"sources": [configured_source("one", ("value",), {"value": "v"})]}, factory)
        binding = service.bind("q_0")
        q_ref = weakref.ref(q)
        del q
        gc.collect()
        self.assertIsNone(q_ref(), "The finalizer must not retain its queue")
        self.assertEqual(readers[0].close_count, 1)
        self.assertEqual(readers[0].iterator_close_count, 1)
        self.assertIs(service.get("q_0"), binding)
        self.assertIs(service.bind("q_0"), binding)
        with self.assertRaisesRegex(DatasetBindingError, "closed"):
            service.bind("q_1")
        service.close()
        self.assertEqual(readers[0].close_count, 1)

    def test_close_before_first_binding_does_no_io(self):
        calls = []
        service = self.service({"sources": [configured_source("one", ("value",), {"value": "v"})]}, lambda spec: calls.append(spec))
        service.close()
        self.assertIsNone(service.get("q_0"))
        with self.assertRaisesRegex(DatasetBindingError, "closed"):
            service.bind("q_0")
        self.assertEqual(calls, [])

    def test_close_attempts_every_reader_even_if_one_close_fails(self):
        readers = []

        def factory(spec):
            reader = ObservedReader(spec, [{"value": "row"}], close_error=(spec.source_id == "bad"))
            readers.append(reader)
            return reader

        service = DatasetTaskAttachments({"sources": [configured_source(name, ("value",), {"value": name}) for name in ("bad", "good")]}, factory)
        binding = service.bind("q_0")
        with self.assertRaisesRegex(DatasetBindingError, "reader close failed"):
            service.close()
        self.assertEqual([reader.close_count for reader in readers], [1, 1])
        self.assertIs(service.bind("q_0"), binding)
        service.close()

    def test_invalid_settings_and_task_ids_do_not_read_sources(self):
        calls = []
        invalid = configured_source("one", ("value",), {"value": "has spaces"})
        with self.assertRaises(DatasetSettingsError):
            DatasetTaskAttachments({"sources": [invalid]}, lambda spec: calls.append(spec))
        q = Queue()
        service = configure_queue_datasets(q, {"sources": [configured_source("one", ("value",), {"value": "v"})]}, lambda spec: calls.append(spec))
        self.addCleanup(service.close)
        for invalid_id in (None, "", " ", 42):
            with self.assertRaises(DatasetBindingError):
                service.bind(invalid_id)
        with self.assertRaises(DatasetBindingError):
            bind_task_datasets(q, ("too-short",))
        self.assertEqual(calls, [])

    def test_existing_queue_cannot_be_reconfigured(self):
        q = Queue()
        settings = {"sources": [configured_source("one", ("value",), {"value": "v"})]}
        first = configure_queue_datasets(q, settings)
        self.addCleanup(first.close)
        with self.assertRaisesRegex(DatasetBindingError, "already has"):
            configure_queue_datasets(q, {"sources": []})
        self.assertIs(q.dataset_attachments, first)

    def test_original_authored_inputs_can_be_rendered_for_successive_tasks(self):
        originals = (
            "Name {{ds:name}}", ["Fix {{ds:rule}}"],
            [{"name": "{{ds:name}}", "age": 30, "enabled": True, "missing": None}],
        )
        first = render_task_inputs(originals, {"name": "Ada {context}", "rule": "Keep {{ds:name}} literal"})
        second = render_task_inputs(originals, {"name": "Grace", "rule": "Use clear wording"})
        self.assertIsInstance(first, tuple)
        self.assertIsInstance(first[1], list)
        self.assertEqual(first[0], "Name Ada {context}")
        self.assertEqual(first[1], ["Fix Keep {{ds:name}} literal"])
        self.assertEqual(second[0], "Name Grace")
        self.assertEqual(originals[0], "Name {{ds:name}}")
        first[2][0]["name"] = "local edit"
        self.assertEqual(originals[2][0]["name"], "{{ds:name}}")
        self.assertEqual(second[2][0]["name"], "Grace")
        self.assertEqual(first[2][0]["age"], 30)
        self.assertIs(first[2][0]["enabled"], True)
        self.assertIsNone(first[2][0]["missing"])

    def test_validation_checks_authored_templates_without_rendering_values(self):
        values = {"name": "Literal {{ds:undefined}} {context}"}
        templates = {"question": "{{ds:name}} {recent_questions}", "fixes": ("Keep {{ds:name}}",)}
        self.assertIsNone(validate_task_templates(templates, values))
        self.assertEqual(templates["question"], "{{ds:name}} {recent_questions}")
        with self.assertRaisesRegex(DatasetTemplateError, "undefined"):
            validate_task_templates(["{{ds:undefined}}"], values)
        with self.assertRaises(DatasetTemplateError):
            validate_task_templates("{{ ds:name }}", values)


if __name__ == "__main__":
    unittest.main()
