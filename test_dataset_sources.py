"""Source boundary tests: python -B -m unittest test_dataset_sources.

Local files are real temporary fixtures. Optional Hub/Arrow backends are mocked;
these checks intentionally do not claim live Hub or installed-Arrow validation.
"""

import csv
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from dataset_bindings import DatasetBindingManager, SourceBinding, TaskBinding
from dataset_sources import (
    DatasetDependencyError, DatasetSourceError, HuggingFaceSource, SourceSpec,
    open_source,
)
from dataset_templates import render_template


class LocalSourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)

    def source(self, kind, text):
        path = self.directory / ("data." + kind)
        path.write_text(text, encoding="utf-8")
        return open_source(SourceSpec("local", kind, str(path)))

    def test_csv_preserves_correlated_cells_quotes_bom_and_newlines(self):
        path = self.directory / "records.csv"
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(['first name', 'body "quoted"'])
            writer.writerow(['Zoë', 'line one\n{{ds:literal}} {context} \\path'])
            writer.writerow(['Sam', 'other row'])
        source = open_source(SourceSpec("people", "csv", str(path)))
        schema = source.schema()
        self.assertEqual(schema.columns, ('first name', 'body "quoted"'))
        self.assertNotIn("Zoë", repr(schema))
        self.assertNotIn("line one", repr(schema))
        rows = list(source.iter_rows())
        self.assertEqual(rows[0].values['first name'], 'Zoë')
        self.assertEqual(rows[0].values['body "quoted"'], 'line one\n{{ds:literal}} {context} \\path')
        self.assertEqual(rows[1].values['first name'], 'Sam')

    def test_tsv_and_empty_cells(self):
        source = self.source("tsv", "name\tnote\nRin\t\n")
        self.assertEqual(source.schema().columns, ("name", "note"))
        self.assertEqual(list(source.iter_rows())[0].values, {"name": "Rin", "note": ""})

    def test_csv_long_text_is_not_limited_by_default_parser_cell_size(self):
        previous_limit = csv.field_size_limit()
        self.addCleanup(csv.field_size_limit, previous_limit)
        csv.field_size_limit(131072)
        text = "x" * 200000
        source = self.source("csv", "body\n" + text + "\n")
        self.assertEqual(list(source.iter_rows())[0].values["body"], text)

    def test_jsonl_schema_includes_late_columns_but_no_values(self):
        source = self.source("jsonl", '\n{"name":"FIRST","payload":{"x":[1,true]}}\n\n{"name":"SECOND","later":null}\n')
        schema = source.schema()
        self.assertEqual(schema.columns, ("name", "payload", "later"))
        self.assertNotIn("FIRST", repr(schema))
        rows = list(source.iter_rows())
        self.assertEqual([row.row_id for row in rows], ["0", "1"])
        self.assertEqual(rows[0].values["payload"], {"x": [1, True]})

    def test_json_array_and_blank_dataset(self):
        source = self.source("json", '[{"a":1},{"a":2,"b":"é"}]')
        self.assertEqual(source.schema().columns, ("a", "b"))
        self.assertEqual([row.values["a"] for row in source.iter_rows()], [1, 2])
        empty = self.source("jsonl", "\n")
        self.assertEqual(empty.schema().columns, ())
        self.assertEqual(list(empty.iter_rows()), [])

    def test_ambiguous_or_malformed_data_is_rejected(self):
        for kind, text in (
            ("csv", "x,x\n1,2\n"),
            ("csv", "x,\n1,2\n"),
            ("json", '{"x":1}'),
            ("json", '[1,2]'),
            ("jsonl", '{"x":1,"x":2}\n'),
            ("jsonl", '{"x":NaN}\n'),
            ("jsonl", 'not json\n'),
        ):
            with self.subTest(kind=kind, text=text):
                source = self.source(kind, text)
                with self.assertRaises(DatasetSourceError):
                    source.schema()
        broken = self.source("csv", "x,y\n1\n")
        broken.schema()  # Header inspection does not read/preview records.
        with self.assertRaisesRegex(DatasetSourceError, "number of fields"):
            list(broken.iter_rows())

    def test_local_changes_fail_without_rebinding_existing_job(self):
        source = self.source("jsonl", '{"v":"original"}\n{"v":"next"}\n')
        with DatasetBindingManager("run", [SourceBinding(source, {"v": "value"})]) as manager:
            original = manager.bind("first")
            Path(source.spec.location).write_text('{"v":"changed and longer"}\n', encoding="utf-8")
            self.assertIs(manager.bind("first"), original)
            self.assertEqual(original.values["value"], "original")
            with self.assertRaisesRegex(ValueError, "changed"):
                manager.bind("second")

    def test_literal_render_and_snapshot_round_trip_from_real_sources(self):
        names_path = self.directory / "names.csv"
        names_path.write_text("first,last\nJo,Quinn\nPat,Reed\n", encoding="utf-8")
        texts_path = self.directory / "texts.jsonl"
        texts_path.write_text(json.dumps({"body": 'raw {context} {{ds:first}} \\ text'}) + "\n", encoding="utf-8")
        with DatasetBindingManager("run", [
            SourceBinding(open_source(SourceSpec("names", "csv", str(names_path))), {"first": "first", "last": "last"}),
            SourceBinding(open_source(SourceSpec("texts", "jsonl", str(texts_path))), {"body": "text"}),
        ]) as manager:
            binding = manager.bind("job")
            restored = TaskBinding.from_dict(json.loads(json.dumps(binding.to_dict())))
            prompt = "{{ds:first}} {{ds:last}}: {{ds:text}} / {context}"
            self.assertEqual(render_template(prompt, restored.values, {"context": "native"}),
                             'Jo Quinn: raw {context} {{ds:first}} \\ text / native')

    def test_source_creation_is_lazy_and_missing_dependencies_are_optional(self):
        missing = open_source(SourceSpec("missing", "csv", str(self.directory / "absent.csv")))
        with self.assertRaises(DatasetSourceError):
            missing.schema()
        parquet = self.directory / "data.parquet"
        parquet.write_bytes(b"fixture")
        with patch("dataset_sources.importlib.import_module", side_effect=ImportError("absent")):
            source = open_source(SourceSpec("parquet", "parquet", str(parquet)))
            with self.assertRaises(DatasetDependencyError):
                source.schema()
            local = self.source("csv", "a\n1\n")
            self.assertEqual(local.schema().columns, ("a",))
            self.assertEqual(list(local.iter_rows())[0].values, {"a": "1"})

    def test_source_settings_preserve_windows_path_and_reject_wrong_selectors(self):
        path = r'C:\Data Files\people.jsonl'
        self.assertEqual(SourceSpec("win", "jsonl", path).location, path)
        with self.assertRaises(DatasetSourceError):
            SourceSpec("bad", "jsonl", path, split="train")
        with self.assertRaises(DatasetSourceError):
            SourceSpec("bad", "huggingface", "https://huggingface.co/datasets/owner/name")


class HubSourceTests(unittest.TestCase):
    def setUp(self):
        self.backend = SimpleNamespace(
            __version__="4.4.2",
            get_dataset_config_names=Mock(return_value=["default"]),
            load_dataset_builder=Mock(return_value=SimpleNamespace(info=SimpleNamespace(
                features={"first": "string", "last": "string"}, splits={"train": object()}
            ))),
            get_dataset_split_names=Mock(return_value=["train"]),
            load_dataset=Mock(return_value=[{"first": "Jo", "last": "Quinn"}]),
        )
        self.api = SimpleNamespace(dataset_info=Mock(return_value=SimpleNamespace(sha="a" * 40)))
        self.hub = SimpleNamespace(HfApi=Mock(return_value=self.api))
        self.addCleanup(patch.stopall)
        patch("dataset_sources._optional_module", side_effect=lambda name: {
            "datasets": self.backend, "huggingface_hub": self.hub,
        }[name]).start()
        self.source = HuggingFaceSource(SourceSpec("hub", "huggingface", "fixture-owner/fixture-dataset", revision="release"))

    def test_schema_has_names_only_and_stream_uses_same_pinned_revision(self):
        schema = self.source.schema()
        self.assertEqual(schema.columns, ("first", "last"))
        self.backend.load_dataset.assert_not_called()
        self.api.dataset_info.assert_called_once_with(repo_id="fixture-owner/fixture-dataset", revision="release", timeout=30)
        self.assertEqual(schema.resolved_spec.revision, "a" * 40)
        self.assertEqual(schema.resolved_spec.config, "default")
        self.assertEqual(schema.resolved_spec.split, "train")
        rows = list(self.source.iter_rows())
        self.assertEqual(rows[0].values, {"first": "Jo", "last": "Quinn"})
        self.api.dataset_info.assert_called_once()
        self.backend.load_dataset.assert_called_once_with("fixture-owner/fixture-dataset", name="default", split="train", revision="a" * 40, streaming=True)
        self.backend.load_dataset_builder.assert_called_once_with("fixture-owner/fixture-dataset", name="default", revision="a" * 40)

    def test_multiple_configs_and_splits_need_explicit_choices(self):
        self.backend.get_dataset_config_names.return_value = ["small", "large"]
        with self.assertRaisesRegex(DatasetSourceError, "Choose a dataset configuration"):
            self.source.schema()
        self.backend.get_dataset_config_names.return_value = ["default"]
        self.backend.load_dataset_builder.return_value.info.splits = {"train": object(), "test": object()}
        with self.assertRaisesRegex(DatasetSourceError, "Choose a dataset split"):
            self.source.schema()
        chosen = HuggingFaceSource(SourceSpec("hub", "huggingface", "fixture-owner/fixture-dataset", config="default", split="test"))
        self.assertEqual(chosen.schema().resolved_spec.split, "test")

    def test_missing_schema_never_silently_samples_rows(self):
        self.backend.load_dataset_builder.return_value.info.features = None
        with self.assertRaisesRegex(DatasetSourceError, "no declared column schema"):
            self.source.schema()
        self.backend.load_dataset.assert_not_called()

    def test_local_folder_cannot_be_read_under_hub_identity(self):
        with patch("dataset_sources.Path.exists", return_value=True):
            with self.assertRaisesRegex(DatasetSourceError, "shadows"):
                self.source.schema()
        self.backend.load_dataset_builder.assert_not_called()
        self.backend.load_dataset.assert_not_called()

    def test_old_backend_rejected_before_remote_code_can_load(self):
        self.backend.__version__ = "2.19.0"
        with self.assertRaises(DatasetDependencyError):
            self.source.schema()
        self.backend.load_dataset_builder.assert_not_called()


class SavedSourceTests(unittest.TestCase):
    def test_saved_schema_split_and_raw_rows_without_preview(self):
        with tempfile.TemporaryDirectory() as directory:
            dataset = Mock()
            dataset.column_names = ["body"]
            dataset.with_format.return_value = dataset
            dataset.__iter__ = Mock(return_value=iter([{"body": "text"}]))
            backend = SimpleNamespace(__version__="4.4.2", load_from_disk=Mock(return_value={"train": dataset}))
            source = open_source(SourceSpec("saved", "hf_disk", directory))
            with patch("dataset_sources._datasets_backend", return_value=backend):
                self.assertEqual(source.schema().columns, ("body",))
                self.assertEqual(source.schema().resolved_spec.split, "train")
                dataset.__iter__.assert_not_called()
                self.assertEqual(list(source.iter_rows())[0].values, {"body": "text"})
                source.close()
                self.assertIsNone(source._dataset)


if __name__ == "__main__":
    unittest.main()
