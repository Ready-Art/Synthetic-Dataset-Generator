"""Real optional-backend checks, with only temporary local fixtures.

Run with ``python -B tests/test_dataset_backends.py``. Parquet checks skip
when PyArrow is absent; saved-dataset checks skip unless datasets 4.x is installed.
The suite never reads the Hub or calls a model API. Rename/delete assertions check
resource lifecycle on the host running the suite; they do not constitute a
Windows run when executed on another operating system.
"""

# Repo root (app modules) onto sys.path so the suite runs from the repository root.
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))

from contextlib import contextmanager
from dataclasses import asdict
import gc
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import weakref

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None

try:
    import datasets as hf
except ImportError:
    hf = None

from dataset_bindings import (
    DatasetBindingError, DatasetBindingManager, DatasetExhaustedError,
    SourceBinding, TaskBinding,
)
from dataset_sources import DatasetSourceError, SourceSpec, open_source
from dataset_templates import render_template


_HF_VERSION = getattr(hf, "__version__", "not installed")
_HF_SUPPORTED = hf is not None and _HF_VERSION.startswith("4.")
_LITERAL = 'Zoë: literal {{ds:name}} / {context} / "quotes" / \\path\nsecond line'
_ROWS = [
    {
        "row_number": 7,
        "first name": "FIRST_ROW_VALUE",
        "nested": {"flags": [True, None, False], "note": "café"},
        "nullable": None,
        "literal": _LITERAL,
    },
    {
        "row_number": 8,
        "first name": "SECOND_ROW_VALUE",
        "nested": {"flags": [False], "note": "二"},
        "nullable": "present",
        "literal": "second record",
    },
]
_ALIASES = {
    "row_number": "number", "first name": "name", "nested": "details",
    "nullable": "optional", "literal": "text",
}


class _LocalBackendCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="sdg-backend-test-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def source(self, kind, path, split=None):
        source = open_source(SourceSpec("fixture", kind, str(path), split=split))
        self.addCleanup(source.close)
        return source

    def assert_schema_contains_only_metadata(self, schema, columns):
        self.assertEqual(schema.columns, tuple(columns))
        self.assertEqual(set(asdict(schema)), {"columns", "fingerprint", "resolved_spec"})
        self.assertIsInstance(schema.fingerprint, str)
        self.assertTrue(schema.fingerprint)
        serialized = json.dumps(asdict(schema), ensure_ascii=False)
        for private_value in ("FIRST_ROW_VALUE", "SECOND_ROW_VALUE", _LITERAL, "café"):
            self.assertNotIn(private_value, serialized)

    def assert_first_binding(self, binding):
        self.assertEqual(binding.values["number"], "7")
        self.assertEqual(binding.values["name"], "FIRST_ROW_VALUE")
        self.assertEqual(binding.values["details"], '{"flags":[true,null,false],"note":"café"}')
        self.assertEqual(binding.values["optional"], "")
        self.assertEqual(binding.values["text"], _LITERAL)
        self.assertEqual(
            render_template("{{ds:name}}: {{ds:text}} / {context}", binding.values,
                            {"context": "native passage"}),
            "FIRST_ROW_VALUE: " + _LITERAL + " / native passage",
        )


@unittest.skipUnless(pa is not None and pq is not None, "Optional PyArrow backend is not installed.")
class ParquetBackendTests(_LocalBackendCase):
    def write_parquet(self, rows, name="records.parquet"):
        path = self.directory / name
        pq.write_table(pa.Table.from_pylist(rows), path)
        return path

    def test_names_nested_nulls_and_literal_text_with_real_parquet(self):
        source = self.source("parquet", self.write_parquet(_ROWS))
        self.assert_schema_contains_only_metadata(source.schema(), _ROWS[0])
        rows = list(source.iter_rows())
        self.assertEqual([row.row_id for row in rows], ["0", "1"])
        self.assertEqual([row.values for row in rows], _ROWS)
        with DatasetBindingManager("parquet-run", [SourceBinding(source, _ALIASES)]) as manager:
            first = manager.bind("first")
            second = manager.bind("second")
            self.assert_first_binding(first)
            self.assertEqual(second.values["number"], "8")
            self.assertEqual(second.values["name"], "SECOND_ROW_VALUE")
            self.assertEqual(second.values["optional"], "present")
            self.assertEqual(json.loads(second.values["details"]), _ROWS[1]["nested"])
            self.assertIs(manager.bind("first"), first)

    def test_row_identity_and_cell_correlation_cross_parquet_batch_boundary(self):
        # The reader batches 256 rows. Distinct values on either side catch
        # resets of its row index and accidental reuse of a preceding batch.
        rows = [{"id": index, "paired": "record-{}".format(index), "offset": index + 1000}
                for index in range(259)]
        source = self.source("parquet", self.write_parquet(rows))
        with DatasetBindingManager("batches", [SourceBinding(
            source, {"id": "id", "paired": "paired", "offset": "offset"}
        )]) as manager:
            for index in range(len(rows)):
                binding = manager.bind("job-{}".format(index))
                self.assertEqual(binding.sources[0].row_id, str(index))
                self.assertEqual(dict(binding.values), {
                    "id": str(index), "paired": "record-{}".format(index),
                    "offset": str(index + 1000),
                })
            with self.assertRaises(DatasetExhaustedError):
                manager.bind("past-the-end")
            self.assertIsNone(manager.get("past-the-end"))

    def test_closing_partial_parquet_iterator_exits_real_file_context(self):
        path = self.write_parquet(_ROWS)
        source = self.source("parquet", path)
        constructor = pq.ParquetFile
        lifecycle = []

        @contextmanager
        def tracked_file(*args, **kwargs):
            try:
                with constructor(*args, **kwargs) as actual_file:
                    lifecycle.append("opened")
                    try:
                        yield actual_file
                    finally:
                        lifecycle.append("exiting")
            finally:
                lifecycle.append("closed")

        with patch.object(pq, "ParquetFile", tracked_file):
            iterator = source.iter_rows()
            self.addCleanup(iterator.close)
            self.assertEqual(next(iterator).values["first name"], "FIRST_ROW_VALUE")
            self.assertEqual(lifecycle, ["opened"])
            iterator.close()
            self.assertEqual(lifecycle, ["opened", "exiting", "closed"])
        source.close()
        moved = path.rename(self.directory / "renamed.parquet")
        self.assertEqual(pq.read_table(moved).to_pylist(), _ROWS)
        moved.unlink()
        self.assertFalse(moved.exists())

    def test_snapshot_survives_parquet_removal_without_reopening_source(self):
        path = self.write_parquet(_ROWS)
        source = self.source("parquet", path)
        with DatasetBindingManager("snapshot", [SourceBinding(source, _ALIASES)]) as manager:
            original = manager.bind("first")
            snapshot = json.loads(json.dumps(original.to_dict(), ensure_ascii=False))
        path.unlink()
        restored = TaskBinding.from_dict(snapshot)
        self.assertEqual(restored.to_dict(), original.to_dict())
        self.assert_first_binding(restored)

    def test_empty_parquet_retains_columns_and_reports_exhaustion(self):
        path = self.directory / "empty.parquet"
        pq.write_table(pa.table({"body": pa.array([], type=pa.string())}), path)
        source = self.source("parquet", path)
        self.assert_schema_contains_only_metadata(source.schema(), ("body",))
        with DatasetBindingManager("empty", [SourceBinding(source, {"body": "body"})]) as manager:
            with self.assertRaises(DatasetExhaustedError):
                manager.bind("first")

    def test_parquet_python_only_cell_type_fails_instead_of_using_repr(self):
        # Parquet can contain non-JSON Python values. The binding contract
        # explicitly rejects these until a deliberate text conversion exists.
        from datetime import date
        path = self.write_parquet([{"date": date(2025, 1, 2)}])
        source = self.source("parquet", path)
        with DatasetBindingManager("dates", [SourceBinding(source, {"date": "date"})]) as manager:
            with self.assertRaisesRegex(DatasetBindingError, "Unsupported cell type date"):
                manager.bind("first")
            self.assertIsNone(manager.get("first"))


@unittest.skipUnless(_HF_SUPPORTED, "Saved HF fixtures need datasets 4.x; found {}.".format(_HF_VERSION))
class SavedDatasetBackendTests(_LocalBackendCase):
    def setUp(self):
        super().setUp()
        if not hf.are_progress_bars_disabled():
            hf.disable_progress_bars()
            self.addCleanup(hf.enable_progress_bars)

    def save_dataset(self, dataset=None, name="saved"):
        path = self.directory / name
        if dataset is None:
            dataset = hf.Dataset.from_list(_ROWS)
        dataset.save_to_disk(str(path))
        return path

    def test_saved_dataset_names_raw_nested_values_and_snapshot(self):
        path = self.save_dataset()
        source = self.source("hf_disk", path)
        schema = source.schema()
        self.assert_schema_contains_only_metadata(schema, _ROWS[0])
        self.assertIsNone(schema.resolved_spec.split)
        rows = list(source.iter_rows())
        self.assertEqual([row.values for row in rows], _ROWS)
        self.assertEqual([row.row_id for row in rows], ["0", "1"])
        with DatasetBindingManager("saved-run", [SourceBinding(source, _ALIASES)]) as manager:
            first = manager.bind("first")
            second = manager.bind("second")
            self.assert_first_binding(first)
            self.assertEqual(second.values["name"], "SECOND_ROW_VALUE")
            self.assertEqual(second.values["number"], "8")
            self.assertIs(manager.bind("first"), first)
            snapshot = json.loads(json.dumps(first.to_dict(), ensure_ascii=False))
        restored = TaskBinding.from_dict(snapshot)
        self.assertEqual(restored.to_dict(), first.to_dict())
        self.assert_first_binding(restored)

    def test_persisted_numpy_subset_format_is_reset_to_all_raw_columns(self):
        dataset = hf.Dataset.from_list(_ROWS)
        dataset.set_format("numpy", columns=["row_number"], output_all_columns=False)
        path = self.save_dataset(dataset)
        # Verify the saved fixture actually carries the problematic format.
        probe = hf.load_from_disk(str(path))
        self.assertEqual(probe.format["type"], "numpy")
        self.assertEqual(set(probe[0]), {"row_number"})
        self.assertTrue(hasattr(probe[0]["row_number"], "dtype"))
        del probe

        source = self.source("hf_disk", path)
        self.assert_schema_contains_only_metadata(source.schema(), _ROWS[0])
        iterator = source.iter_rows()
        try:
            row = next(iterator)
            self.assertEqual(row.values, _ROWS[0])
            self.assertIs(type(row.values["row_number"]), int)
            self.assertIs(type(row.values["nested"]), dict)
        finally:
            iterator.close()
        with DatasetBindingManager("raw-format", [SourceBinding(source, _ALIASES)]) as manager:
            self.assert_first_binding(manager.bind("first"))

    def test_datasetdict_requires_unambiguous_split_and_preserves_selection(self):
        dataset = hf.DatasetDict({
            "train": hf.Dataset.from_dict({"value": ["training-only"]}),
            "validation": hf.Dataset.from_dict({"value": ["validation-only", "validation-next"]}),
        })
        path = self.save_dataset(dataset)
        with self.assertRaisesRegex(DatasetSourceError, "Choose a saved dataset split"):
            self.source("hf_disk", path).schema()
        with self.assertRaisesRegex(DatasetSourceError, "chosen split does not exist"):
            self.source("hf_disk", path, split="absent").schema()
        source = self.source("hf_disk", path, split="validation")
        schema = source.schema()
        self.assertEqual(schema.columns, ("value",))
        self.assertEqual(schema.resolved_spec.split, "validation")
        with DatasetBindingManager("selected", [SourceBinding(source, {"value": "value"})]) as manager:
            first = manager.bind("first")
            second = manager.bind("second")
            self.assertEqual(first.values["value"], "validation-only")
            self.assertEqual(second.values["value"], "validation-next")
            self.assertEqual(first.sources[0].spec.split, "validation")
            self.assertEqual(first.sources[0].row_id, "0")
            self.assertEqual(second.sources[0].row_id, "1")

    def test_single_split_is_recorded_and_single_dataset_rejects_split(self):
        dictionary_path = self.save_dataset(
            hf.DatasetDict({"only_split": hf.Dataset.from_list(_ROWS)}), name="dictionary"
        )
        source = self.source("hf_disk", dictionary_path)
        self.assertEqual(source.schema().resolved_spec.split, "only_split")
        with DatasetBindingManager("one-split", [SourceBinding(source, {"literal": "text"})]) as manager:
            self.assertEqual(manager.bind("first").sources[0].spec.split, "only_split")
        dataset_path = self.save_dataset(name="single")
        with self.assertRaisesRegex(DatasetSourceError, "one Dataset; do not set a split"):
            self.source("hf_disk", dataset_path, split="train").schema()

    def test_manager_close_releases_saved_reader_and_snapshot_survives_removal(self):
        path = self.save_dataset()
        source = self.source("hf_disk", path)
        with DatasetBindingManager("release", [SourceBinding(source, _ALIASES)]) as manager:
            first = manager.bind("first")  # Leave the iterator partially read.
            loaded_dataset = weakref.ref(source._dataset)
            self.assertIsNotNone(loaded_dataset())
            snapshot = json.loads(json.dumps(first.to_dict(), ensure_ascii=False))
        gc.collect()
        self.assertIsNone(source._dataset)
        self.assertIsNone(loaded_dataset())
        moved = path.rename(self.directory / "moved-saved")
        for file_path in moved.rglob("*"):
            if file_path.is_file():
                file_path.unlink()
        moved.rmdir()
        self.assertFalse(moved.exists())
        restored = TaskBinding.from_dict(snapshot)
        self.assert_first_binding(restored)
        self.assertIs(manager.bind("first"), first)

    def test_saved_metadata_change_stops_new_binding_but_keeps_original(self):
        path = self.save_dataset()
        source = self.source("hf_disk", path)
        with DatasetBindingManager("changed", [SourceBinding(source, _ALIASES)]) as manager:
            first = manager.bind("first")
            metadata = path / "state.json"
            with metadata.open("a", encoding="utf-8") as handle:
                handle.write("\n")
            self.assertIs(manager.bind("first"), first)
            self.assert_first_binding(first)
            with self.assertRaisesRegex(DatasetBindingError, "changed"):
                manager.bind("second")
            self.assertIsNone(manager.get("second"))

    def test_empty_saved_dataset_keeps_declared_columns(self):
        dataset = hf.Dataset.from_dict({"body": []}, features=hf.Features({"body": hf.Value("string")}))
        source = self.source("hf_disk", self.save_dataset(dataset))
        self.assert_schema_contains_only_metadata(source.schema(), ("body",))
        with DatasetBindingManager("empty-saved", [SourceBinding(source, {"body": "body"})]) as manager:
            with self.assertRaises(DatasetExhaustedError):
                manager.bind("first")


if __name__ == "__main__":
    unittest.main()
