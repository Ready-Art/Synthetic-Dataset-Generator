"""Focused binding tests: ``python -m unittest test_dataset_bindings``.

No GUI, network, application configuration, or model API is used. Full generation
worker/retry integration tests belong to the later wiring milestone.
"""

import json
import math
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError

from dataset_bindings import (
    DatasetBindingError, DatasetBindingManager, DatasetExhaustedError,
    SourceBinding, TaskBinding, cell_to_text,
)
from dataset_sources import DatasetRow, DatasetSchema, SourceSpec


class MemorySource:
    """Small reader with observable row allocation and metadata-only schema."""

    def __init__(self, source_id, columns, rows):
        self.spec = SourceSpec(source_id, "csv", source_id + ".csv")
        self.columns = tuple(columns)
        self.rows = rows
        self.read_count = 0
        self.schema_count = 0

    def schema(self):
        self.schema_count += 1
        return DatasetSchema(self.columns, "fixture-" + self.spec.source_id)

    def iter_rows(self):
        for index, values in enumerate(self.rows):
            self.read_count += 1
            yield DatasetRow(str(index), values)


class BindingTests(unittest.TestCase):
    def make_manager(self):
        people = MemorySource("people", ("first name", "family"), [
            {"first name": "Ada", "family": "Lovelace"},
            {"first name": "Grace", "family": "Hopper"},
        ])
        places = MemorySource("places", ("setting",), [
            {"setting": "Library"}, {"setting": "Workshop"},
        ])
        manager = DatasetBindingManager("run-1", [
            SourceBinding(people, {"first name": "first", "family": "last"}),
            SourceBinding(places, {"setting": "place"}),
        ])
        return manager, people, places

    def test_one_row_per_source_and_rebinding_is_stable(self):
        manager, people, places = self.make_manager()
        self.assertEqual((people.read_count, places.read_count), (0, 0))
        first = manager.bind("task-a")
        self.assertEqual(dict(first.values), {"first": "Ada", "last": "Lovelace", "place": "Library"})
        self.assertIs(manager.bind("task-a"), first)
        self.assertEqual((people.read_count, places.read_count), (1, 1))
        second = manager.bind("task-b")
        self.assertEqual(dict(second.values), {"first": "Grace", "last": "Hopper", "place": "Workshop"})
        self.assertEqual([source.row_id for source in second.sources], ["1", "1"])
        self.assertIs(manager.get("task-a"), first)
        self.assertIsNone(manager.get("unknown"))
        self.assertEqual(manager.snapshot(), (first, second))

    def test_mappings_values_and_provenance_are_detached_and_immutable(self):
        source = MemorySource("records", ("payload",), [{"payload": {"b": ["é", None], "a": True}}])
        aliases = {"payload": "data"}
        selection = SourceBinding(source, aliases)
        manager = DatasetBindingManager("run", [selection])
        aliases["payload"] = "changed"
        first = manager.bind("a")
        source.rows[0]["payload"]["b"].append("later edit")
        source.spec = SourceSpec("records", "csv", "changed.csv")
        self.assertEqual(first.values["data"], '{"a":true,"b":["é",null]}')
        self.assertEqual(first.sources[0].spec.location, "records.csv")
        with self.assertRaises(TypeError):
            first.values["data"] = "changed"
        with self.assertRaises(TypeError):
            first.sources[0].column_aliases["payload"] = "changed"
        with self.assertRaises(FrozenInstanceError):
            first.task_id = "changed"

    def test_snapshot_round_trip_retains_strings_without_resampling(self):
        source = MemorySource("text", ("body",), [{"body": 'C:\\work\\notes "hello"\n{{ds:other}} {context}'}])
        manager = DatasetBindingManager("run", [SourceBinding(source, {"body": "body"})])
        original = manager.bind("a")
        serialized = json.dumps(original.to_dict(), ensure_ascii=False)
        restored = TaskBinding.from_dict(json.loads(serialized))
        self.assertEqual(restored, original)
        self.assertEqual(source.read_count, 1)
        exported = original.to_dict()
        exported["values"]["body"] = "edited"
        exported["sources"][0]["spec"]["location"] = "elsewhere"
        self.assertEqual(restored, original)
        self.assertNotEqual(original.values["body"], "edited")

    def test_provenance_records_resolved_hub_commit_config_and_split(self):
        commit = "1" * 40

        class ResolvedSource(MemorySource):
            def schema(self):
                return DatasetSchema(
                    self.columns, "resolved-fingerprint",
                    SourceSpec("hub", "huggingface", "owner/data", "config-a", "train", commit),
                )

        source = ResolvedSource("hub", ("v",), [{"v": "fixed"}])
        source.spec = SourceSpec("hub", "huggingface", "owner/data", revision="main")
        manager = DatasetBindingManager("run", [SourceBinding(source, {"v": "value"})])
        binding = manager.bind("a")
        resolved = binding.sources[0].spec
        self.assertEqual((resolved.revision, resolved.config, resolved.split), (commit, "config-a", "train"))
        self.assertEqual(source.spec.revision, "main")
        self.assertEqual(TaskBinding.from_dict(binding.to_dict()), binding)

    def test_resolved_source_cannot_change_source_identity(self):
        class IncorrectSource(MemorySource):
            def schema(self):
                return DatasetSchema(self.columns, "fingerprint", SourceSpec("different", "csv", "other.csv"))

        source = IncorrectSource("original", ("v",), [{"v": "fixed"}])
        with self.assertRaisesRegex(DatasetBindingError, "retain.*source ID"):
            DatasetBindingManager("run", [SourceBinding(source, {"v": "value"})])
        self.assertEqual(source.read_count, 0)

    def test_columns_are_exact_and_aliases_unique(self):
        source = MemorySource("one", ('quoted"<column>\\',), [{'quoted"<column>\\': "kept"}])
        manager = DatasetBindingManager("run", [SourceBinding(source, {'quoted"<column>\\': "kept"})])
        self.assertEqual(manager.bind("a").values["kept"], "kept")
        with self.assertRaisesRegex(DatasetBindingError, "no mapped column"):
            DatasetBindingManager("run", [SourceBinding(source, {"stripped_column": "kept"})])
        other = MemorySource("two", ("v",), [{"v": 1}])
        with self.assertRaisesRegex(DatasetBindingError, "Duplicate dataset alias"):
            DatasetBindingManager("run", [
                SourceBinding(source, {'quoted"<column>\\': "same"}),
                SourceBinding(other, {"v": "same"}),
            ])
        with self.assertRaisesRegex(DatasetBindingError, "Duplicate source ID"):
            DatasetBindingManager("run", [
                SourceBinding(source, {'quoted"<column>\\': "a"}),
                SourceBinding(source, {'quoted"<column>\\': "b"}),
            ])
        self.assertEqual(other.read_count, 0)

    def test_invalid_mapping_and_schema_fail_before_rows(self):
        source = MemorySource("one", ("v",), [{"v": 1}])
        for aliases in ({}, {"v": "bad-name"}, {"v": "0starts_with_number"}, {"v": 1}):
            with self.subTest(aliases=aliases), self.assertRaises(DatasetBindingError):
                SourceBinding(source, aliases)
        with self.assertRaises(DatasetBindingError):
            SourceBinding(source, {"a": "same", "b": "same"})
        source.columns = ("v", "v")
        with self.assertRaisesRegex(DatasetBindingError, "distinct"):
            DatasetBindingManager("run", [SourceBinding(source, {"v": "v"})])
        self.assertEqual(source.read_count, 0)

    def test_exhaustion_does_not_discard_other_sources_staged_row(self):
        longer = MemorySource("longer", ("v",), [{"v": i} for i in range(3)])
        shorter = MemorySource("shorter", ("v",), [{"v": "only"}])
        manager = DatasetBindingManager("run", [
            SourceBinding(longer, {"v": "left"}), SourceBinding(shorter, {"v": "right"}),
        ])
        original = manager.bind("first")
        with self.assertRaisesRegex(DatasetExhaustedError, "shorter"):
            manager.bind("second")
        with self.assertRaises(DatasetExhaustedError):
            manager.bind("third")
        self.assertEqual(longer.read_count, 2)  # Second row stays staged, never skipped.
        self.assertEqual(manager.snapshot(), (original,))
        self.assertIs(manager.bind("first"), original)
        self.assertIsNone(manager.get("second"))

    def test_bad_rows_never_silently_skip(self):
        source = MemorySource("missing", ("required",), [{}, {"required": "would skip"}])
        manager = DatasetBindingManager("run", [SourceBinding(source, {"required": "value"})])
        for task in ("first", "second"):
            with self.assertRaisesRegex(DatasetBindingError, "missing mapped column"):
                manager.bind(task)
        self.assertEqual(source.read_count, 1)
        self.assertEqual(manager.snapshot(), ())

    def test_read_error_is_not_reinterpreted_as_end_of_source(self):
        class BrokenSource(MemorySource):
            def iter_rows(self):
                self.read_count += 1
                raise OSError("source unavailable")

        source = BrokenSource("broken", ("v",), [])
        manager = DatasetBindingManager("run", [SourceBinding(source, {"v": "v"})])
        for task in ("a", "b"):
            with self.assertRaisesRegex(DatasetBindingError, "source unavailable") as raised:
                manager.bind(task)
            self.assertNotIsInstance(raised.exception, DatasetExhaustedError)
        self.assertEqual(source.read_count, 1)

    def test_twenty_concurrent_tasks_remain_separate(self):
        source = MemorySource("rows", ("id", "paired"), [
            {"id": i, "paired": "pair-{}".format(i)} for i in range(20)
        ])
        manager = DatasetBindingManager("run", [SourceBinding(source, {"id": "id", "paired": "paired"})])
        barrier = threading.Barrier(20)

        def bind(index):
            barrier.wait(timeout=10)
            result = manager.bind("task-{}".format(index))
            self.assertIs(result, manager.bind(result.task_id))
            return result

        with ThreadPoolExecutor(max_workers=20) as pool:
            bindings = list(pool.map(bind, range(20)))
        self.assertEqual({item.values["id"] for item in bindings}, {str(i) for i in range(20)})
        self.assertEqual(len({item.task_id for item in bindings}), 20)
        for item in bindings:
            self.assertEqual(item.values["paired"], "pair-" + item.values["id"])
        self.assertEqual(source.read_count, 20)

    def test_concurrent_calls_for_one_task_allocate_once(self):
        source = MemorySource("rows", ("value",), [{"value": "one"}])
        manager = DatasetBindingManager("run", [SourceBinding(source, {"value": "value"})])
        barrier = threading.Barrier(20)

        def bind(_):
            barrier.wait(timeout=10)
            return manager.bind("same-task")

        with ThreadPoolExecutor(max_workers=20) as pool:
            bindings = list(pool.map(bind, range(20)))
        self.assertTrue(all(binding is bindings[0] for binding in bindings))
        self.assertEqual(source.read_count, 1)

    def test_run_scope_and_empty_sources(self):
        one = DatasetBindingManager("one", [])
        two = DatasetBindingManager("two", [])
        self.assertEqual(dict(one.bind("same-id").values), {})
        self.assertNotEqual(one.bind("same-id"), two.bind("same-id"))
        for invalid in ("", "  ", None, 4):
            with self.subTest(invalid=invalid), self.assertRaises(DatasetBindingError):
                DatasetBindingManager(invalid, [])
            with self.subTest(task=invalid), self.assertRaises(DatasetBindingError):
                one.bind(invalid)

    def test_close_releases_iterator_and_retains_completed_bindings(self):
        class ClosableSource(MemorySource):
            closed = False

            def iter_rows(self):
                try:
                    yield from super().iter_rows()
                finally:
                    self.closed = True

        source = ClosableSource("rows", ("v",), [{"v": "first"}, {"v": "second"}])
        with DatasetBindingManager("run", [SourceBinding(source, {"v": "v"})]) as manager:
            original = manager.bind("a")
            self.assertFalse(source.closed)
        self.assertTrue(source.closed)
        self.assertIs(manager.bind("a"), original)
        self.assertIs(manager.get("a"), original)
        with self.assertRaisesRegex(DatasetBindingError, "closed"):
            manager.bind("b")
        manager.close()  # Idempotent.

    def test_close_attempts_every_iterator_and_source_despite_errors(self):
        events = []

        class ManagedSource(MemorySource):
            def iter_rows(self):
                source_id = self.spec.source_id

                class RowIterator:
                    def __iter__(self):
                        return self

                    def __next__(self):
                        return DatasetRow("0", {"v": source_id})

                    def close(self):
                        events.append(source_id + ":iterator")
                        if source_id == "first":
                            raise OSError("iterator close failed")

                return RowIterator()

            def close(self):
                events.append(self.spec.source_id + ":source")
                if self.spec.source_id == "first":
                    raise OSError("source close failed")

        first = ManagedSource("first", ("v",), [])
        second = ManagedSource("second", ("v",), [])
        manager = DatasetBindingManager("run", [
            SourceBinding(first, {"v": "first"}), SourceBinding(second, {"v": "second"}),
        ])
        binding = manager.bind("a")
        with self.assertRaisesRegex(DatasetBindingError, "iterator close failed.*source close failed"):
            manager.close()
        self.assertEqual(events, ["first:iterator", "first:source", "second:iterator", "second:source"])
        self.assertIs(manager.bind("a"), binding)
        manager.close()
        self.assertEqual(len(events), 4)

    def test_close_releases_schema_reader_even_without_row_allocation(self):
        class ManagedSource(MemorySource):
            closed = False

            def close(self):
                self.closed = True

        source = ManagedSource("schema-only", ("v",), [{"v": 1}])
        manager = DatasetBindingManager("run", [SourceBinding(source, {"v": "value"})])
        manager.close()
        self.assertTrue(source.closed)
        self.assertEqual(source.read_count, 0)

    def test_constructor_failure_closes_inspected_readers_and_keeps_original_error(self):
        for failure_stage in ("mapping", "schema"):
            with self.subTest(failure_stage=failure_stage):
                events = []
                original_error = OSError("original schema failure")

                class ManagedSource(MemorySource):
                    closed = False

                    def schema(self):
                        schema = super().schema()
                        if self.spec.source_id == "failing" and failure_stage == "schema":
                            raise original_error
                        return schema

                    def close(self):
                        self.closed = True
                        events.append(self.spec.source_id)
                        if self.spec.source_id == "first":
                            raise OSError("secondary cleanup failure")

                first = ManagedSource("first", ("v",), [])
                failing = ManagedSource("failing", ("v",), [])
                later = ManagedSource("later", ("v",), [])
                selected_column = "missing" if failure_stage == "mapping" else "v"
                expected_type = DatasetBindingError if failure_stage == "mapping" else OSError
                with self.assertRaises(expected_type) as raised:
                    DatasetBindingManager("run", [
                        SourceBinding(first, {"v": "first"}),
                        SourceBinding(failing, {selected_column: "second"}),
                        SourceBinding(later, {"v": "third"}),
                    ])
                if failure_stage == "schema":
                    self.assertIs(raised.exception, original_error)
                else:
                    self.assertIn("no mapped column", str(raised.exception))
                self.assertEqual(events, ["failing", "first"])
                self.assertTrue(first.closed)
                self.assertTrue(failing.closed)
                self.assertFalse(later.closed)
                self.assertEqual(later.schema_count, 0)

    def test_snapshot_validation_rejects_mismatched_or_corrupt_state(self):
        manager, _, _ = self.make_manager()
        original = manager.bind("a")
        for mutate in (
            lambda data: data.update(version=2),
            lambda data: data.update(version=True),
            lambda data: data["values"].pop("first"),
            lambda data: data["values"].update(first=17),
            lambda data: data["sources"].append(data["sources"][0]),
            lambda data: data["sources"][0]["column_aliases"].update(family="bad-alias"),
        ):
            data = original.to_dict()
            mutate(data)
            with self.assertRaises(DatasetBindingError):
                TaskBinding.from_dict(data)


class CellTextTests(unittest.TestCase):
    def test_supported_values(self):
        self.assertEqual(cell_to_text(None), "")
        self.assertEqual(cell_to_text(False), "false")
        self.assertEqual(cell_to_text(17), "17")
        self.assertEqual(cell_to_text(1.5), "1.5")
        self.assertEqual(cell_to_text('é\n{{ds:x}} {context} \\"'), 'é\n{{ds:x}} {context} \\"')
        self.assertEqual(cell_to_text({"z": None, "a": [False, "é"]}), '{"a":[false,"é"],"z":null}')

    def test_unsupported_values_fail_explicitly(self):
        cyclic = []
        cyclic.append(cyclic)
        for value in (object(), b"bytes", (1, 2), {1: "numeric key"}, {"set"}, math.nan, math.inf, cyclic):
            with self.subTest(type=type(value).__name__), self.assertRaises(DatasetBindingError):
                cell_to_text(value)


if __name__ == "__main__":
    unittest.main()
