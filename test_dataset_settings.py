"""Pure settings checks: python -B -m unittest test_dataset_settings."""

import copy
import json
import unittest

from dataset_settings import (
    DatasetSettingsError,
    apply_schema,
    new_source,
    normalize_config,
    source_selector,
    source_spec,
    validate_config,
    visible_column_names,
    with_visible_columns,
)
from dataset_sources import DatasetSchema, SourceSpec


def configured_source(columns=("first_name", "age"), aliases=None):
    source = new_source([])
    source.update({"name": "People", "location": r"C:\data\people.csv"})
    source = apply_schema(source, DatasetSchema(tuple(columns), "schema-v1"))
    if aliases is not None:
        source["column_aliases"] = dict(aliases)
    return source


class DatasetSettingsTests(unittest.TestCase):
    def test_new_sources_are_variable_and_keep_stable_unique_ids(self):
        sources = []
        for number in range(1, 35):
            source = new_source(sources)
            self.assertEqual(source["name"], "Dataset {}".format(number))
            self.assertTrue(source["enabled"])
            self.assertEqual(source["kind"], "csv")
            self.assertEqual(source["location"], "")
            self.assertEqual(source["columns"], [])
            self.assertEqual(source["column_aliases"], {})
            self.assertIsNone(source["visible_columns"])
            self.assertIsNone(source["schema_selector"])
            self.assertIsNone(source["schema_fingerprint"])
            sources.append(source)
        self.assertEqual(len({source["source_id"] for source in sources}), 34)
        normalized = normalize_config({"sources": sources})
        self.assertEqual([source["source_id"] for source in sources],
                         [source["source_id"] for source in normalized["sources"]])
        validate_config(normalized)  # Blank, unmapped drafts are saveable.

    def test_absent_dataset_configuration_is_empty(self):
        self.assertEqual(normalize_config(None), {"sources": []})
        self.assertEqual(normalize_config({}), {"sources": []})
        validate_config(None)

    def test_settings_round_trip_is_detached_and_names_only(self):
        source = configured_source(aliases={"first_name": "person"})
        source["visible_columns"] = ["first_name"]
        source["rows"] = [{"first_name": "unwanted record value"}]
        source["token"] = "unwanted credential"
        source["schema_selector"]["records"] = ["unwanted nested value"]
        original = {"sources": [source], "rows": ["unwanted wrapper value"]}
        normalized = normalize_config(original)
        self.assertNotIn("unwanted", json.dumps(normalized))
        self.assertEqual(normalized, normalize_config(json.loads(json.dumps(normalized))))
        normalized["sources"][0]["columns"].append("new")
        normalized["sources"][0]["column_aliases"]["first_name"] = "changed"
        normalized["sources"][0]["schema_selector"]["location"] = "changed"
        normalized["sources"][0]["visible_columns"].append("age")
        self.assertEqual(source["columns"], ["first_name", "age"])
        self.assertEqual(source["column_aliases"], {"first_name": "person"})
        self.assertEqual(source["schema_selector"]["location"], r"C:\data\people.csv")
        self.assertEqual(source["visible_columns"], ["first_name"])

    def test_windows_paths_and_exact_column_names_survive(self):
        column = 'first<name> "quoted" \\ path'
        source = configured_source((column,), {column: "first_name"})
        source["location"] = r"D:\datasets\People's profiles\set <one>.csv"
        source = apply_schema(source, DatasetSchema((column,), "schema-v2"))
        normalized = normalize_config({"sources": [source]})
        self.assertEqual(normalized["sources"][0]["location"], source["location"])
        self.assertEqual(normalized["sources"][0]["columns"], [column])
        self.assertEqual(source_spec(source).location, source["location"])
        validate_config(normalized)  # No file existence or network check.

    def test_optional_blank_selectors_are_comparable_without_trimming_paths(self):
        source = configured_source(aliases={"first_name": "person"})
        source.update({"config": "", "split": " \t", "revision": "\n"})
        selected = source_selector(source)
        self.assertEqual(set(selected), {"kind", "location", "config", "split", "revision"})
        self.assertIsNone(selected["config"])
        self.assertIsNone(selected["split"])
        self.assertIsNone(selected["revision"])
        self.assertNotIn("source_id", selected)
        self.assertEqual(source_spec(source).location, r"C:\data\people.csv")
        validate_config({"sources": [source]})
        self.assertEqual(source["split"], " \t")
        source["location"] = "  exact path.csv  "
        self.assertEqual(source_selector(source)["location"], "  exact path.csv  ")

    def test_disabled_invalid_draft_is_preserved_but_ignored_for_active_validation(self):
        active = configured_source(aliases={"first_name": "person"})
        disabled = new_source([active])
        disabled.update({
            "enabled": False, "kind": "unfinished", "location": "",
            "column_aliases": {"missing column": "invalid alias", "other": "person"},
        })
        normalized = normalize_config({"sources": [active, disabled]})
        validate_config(normalized)
        self.assertEqual(normalized["sources"][1]["column_aliases"], disabled["column_aliases"])
        self.assertEqual(normalized["sources"][1]["kind"], "unfinished")
        normalized["sources"][1]["enabled"] = True
        with self.assertRaises(DatasetSettingsError):
            validate_config(normalized)

    def test_unmapped_drafts_can_be_saved_without_source_or_schema(self):
        source = new_source([])
        source["kind"] = "unfinished"
        source["column_aliases"] = {"draft column": ""}
        normalized = normalize_config({"sources": [source]})
        self.assertEqual(normalized["sources"][0]["column_aliases"], {})
        validate_config(normalized)
        with self.assertRaises(DatasetSettingsError):
            source_spec(source)

    def test_active_aliases_are_validated_without_rewriting_them(self):
        for alias in ("has space", "has-hyphen", "2name", " name ", " "):
            with self.subTest(alias=alias):
                source = configured_source(aliases={"first_name": alias})
                normalized = normalize_config({"sources": [source]})
                self.assertEqual(normalized["sources"][0]["column_aliases"]["first_name"], alias)
                with self.assertRaises(DatasetSettingsError):
                    validate_config(normalized)

    def test_aliases_are_globally_unique_even_when_the_column_is_hidden(self):
        first = configured_source(aliases={"first_name": "person"})
        second = configured_source(aliases={"age": "person"})
        second = with_visible_columns(second, [])
        with self.assertRaisesRegex(DatasetSettingsError, "person.*more than once"):
            validate_config({"sources": [first, second]})
        second["enabled"] = False
        validate_config({"sources": [first, second]})

    def test_aliases_cannot_collide_within_one_source(self):
        source = configured_source(aliases={"first_name": "same", "age": "same"})
        with self.assertRaisesRegex(DatasetSettingsError, "same.*more than once"):
            validate_config({"sources": [source]})

    def test_changing_source_selector_invalidates_active_cached_mappings(self):
        source = configured_source(aliases={"first_name": "person"})
        source["location"] = r"C:\data\different.csv"
        with self.assertRaisesRegex(DatasetSettingsError, "import columns again"):
            validate_config({"sources": [source]})
        refreshed = apply_schema(source, DatasetSchema(("first_name", "age"), "different-file"))
        validate_config({"sources": [refreshed]})
        self.assertEqual(refreshed["column_aliases"], {"first_name": "person"})

    def test_mappings_need_imported_columns_and_a_schema_fingerprint(self):
        source = configured_source(aliases={"first_name": "person"})
        for key in ("schema_selector", "schema_fingerprint"):
            candidate = copy.deepcopy(source)
            candidate[key] = None
            with self.subTest(key=key):
                with self.assertRaisesRegex(DatasetSettingsError, "import columns again"):
                    validate_config({"sources": [candidate]})
        for fingerprint in ("", " \t"):
            candidate = dict(source, schema_fingerprint=fingerprint)
            with self.assertRaisesRegex(DatasetSettingsError, "import columns again"):
                validate_config({"sources": [candidate]})
        source["column_aliases"]["absent"] = "another_alias"
        with self.assertRaisesRegex(DatasetSettingsError, "absent"):
            validate_config({"sources": [source]})

    def test_schema_refresh_refuses_to_silently_drop_a_mapped_column(self):
        source = configured_source(aliases={"first_name": "person", "age": "years"})
        original = copy.deepcopy(source)
        with self.assertRaisesRegex(DatasetSettingsError, "age.*Clear their aliases"):
            apply_schema(source, DatasetSchema(("first_name", "new"), "schema-v2"))
        self.assertEqual(source, original)
        source["column_aliases"]["age"] = ""
        refreshed = apply_schema(source, DatasetSchema(("first_name", "new"), "schema-v2"))
        self.assertEqual(refreshed["columns"], ["first_name", "new"])
        self.assertEqual(refreshed["column_aliases"], {"first_name": "person"})

    def test_resolved_hub_revision_does_not_replace_the_users_branch_selector(self):
        source = new_source([])
        source.update({"kind": "huggingface", "location": "owner/dataset", "revision": "main"})
        resolved = SourceSpec(source["source_id"], "huggingface", "owner/dataset",
                              config="default", split="train", revision="a" * 40)
        refreshed = apply_schema(source, DatasetSchema(("text",), "pinned-fingerprint", resolved))
        self.assertEqual(refreshed["revision"], "main")
        self.assertIsNone(refreshed["config"])
        self.assertIsNone(refreshed["split"])
        self.assertEqual(refreshed["schema_selector"]["revision"], "main")
        self.assertEqual(refreshed["schema_fingerprint"], "pinned-fingerprint")
        self.assertNotIn("resolved_spec", refreshed)

    def test_thousand_column_visibility_does_not_change_aliases(self):
        columns = tuple("column_{}".format(index) for index in range(1000))
        aliases = {"column_0": "first", "column_999": "last"}
        source = configured_source(columns, aliases)
        self.assertEqual(visible_column_names(source), columns)
        partial = with_visible_columns(source, ["column_501", "column_2"])
        self.assertEqual(visible_column_names(partial), ("column_2", "column_501"))
        self.assertEqual(partial["column_aliases"], aliases)
        self.assertEqual(visible_column_names(source), columns)
        none = with_visible_columns(partial, [])
        self.assertEqual(visible_column_names(none), ())
        self.assertEqual(none["column_aliases"], aliases)
        validate_config({"sources": [none]})
        all_again = with_visible_columns(none, None)
        self.assertEqual(visible_column_names(all_again), columns)
        all_again["column_aliases"]["column_0"] = "changed"
        self.assertEqual(none["column_aliases"], aliases)

    def test_all_selection_includes_future_schema_columns(self):
        source = configured_source()
        explicitly_all = with_visible_columns(source, ["age", "first_name"])
        self.assertIsNone(explicitly_all["visible_columns"])
        for current in (source, explicitly_all):
            refreshed = apply_schema(current, DatasetSchema(("first_name", "age", "city"), "schema-v2"))
            self.assertIsNone(refreshed["visible_columns"])
            self.assertEqual(visible_column_names(refreshed), ("first_name", "age", "city"))
        source["visible_columns"] = ["age", "first_name"]
        self.assertIsNone(normalize_config({"sources": [source]})["sources"][0]["visible_columns"])

    def test_partial_visibility_intersects_refreshed_schema(self):
        source = configured_source(("a", "b", "c"), {"a": "keep"})
        source = with_visible_columns(source, ["a", "b"])
        refreshed = apply_schema(source, DatasetSchema(("a", "c", "d"), "schema-v2"))
        self.assertEqual(refreshed["visible_columns"], ["a"])
        self.assertEqual(refreshed["column_aliases"], {"a": "keep"})
        self.assertEqual(source["visible_columns"], ["a", "b"])

    def test_none_and_all_are_distinct_even_for_an_empty_schema(self):
        source = new_source([])
        hidden = with_visible_columns(source, [])
        self.assertEqual(hidden["visible_columns"], [])
        self.assertIsNone(source["visible_columns"])
        self.assertEqual(normalize_config({"sources": [hidden]})["sources"][0]["visible_columns"], [])
        self.assertEqual(apply_schema(hidden, DatasetSchema(("new",), "v1"))["visible_columns"], [])
        self.assertIsNone(apply_schema(source, DatasetSchema(("new",), "v1"))["visible_columns"])

    def test_visibility_rejects_unknown_or_duplicate_names_without_mutation(self):
        source = configured_source(aliases={"first_name": "person"})
        original = copy.deepcopy(source)
        for selection in (["absent"], ["age", "age"], "age", {"age": True}):
            with self.subTest(selection=selection):
                with self.assertRaises(DatasetSettingsError):
                    with_visible_columns(source, selection)
        self.assertEqual(source, original)

    def test_structural_errors_and_duplicate_source_ids_are_not_silently_repaired(self):
        source = new_source([])
        malformed = (
            [], {"sources": {}}, {"sources": [None]}, {"sources": [{}]},
            {"sources": [dict(source, enabled="false")]},
            {"sources": [dict(source, columns="name")]},
            {"sources": [dict(source, columns=["name", "name"])]},
            {"sources": [dict(source, column_aliases={"name": 5})]},
            {"sources": [dict(source, visible_columns="name")]},
            {"sources": [dict(source, schema_selector=[])]},
            {"sources": [dict(source, location=None)]},
            {"sources": [source, dict(source, enabled=False)]},
        )
        for data in malformed:
            with self.subTest(data=data):
                with self.assertRaises(DatasetSettingsError):
                    normalize_config(data)

    def test_schema_input_must_contain_distinct_names_and_metadata(self):
        source = new_source([])
        for schema in (None, DatasetSchema(("same", "same"), "v1"),
                       DatasetSchema(("text",), ""), DatasetSchema(("",), "v1")):
            with self.subTest(schema=schema):
                with self.assertRaises(DatasetSettingsError):
                    apply_schema(source, schema)


if __name__ == "__main__":
    unittest.main()
