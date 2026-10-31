"""Focused, dependency-free tests: python -B tests/test_dataset_templates.py."""

# Repo root (app modules) onto sys.path so the suite runs from the repository root.
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))

import unittest
import random

from dataset_templates import (
    DEFAULT_QUESTION_SEPARATOR,
    DatasetTemplateError,
    dataset_placeholders,
    render_question_line,
    render_template,
    render_with_native_placeholders,
    validate_alias,
)


class DatasetTemplateTests(unittest.TestCase):
    def test_dataset_and_native_aliases_are_distinct(self):
        self.assertEqual(
            render_template(
                "{{ds:context}} / {context} / {{context}} / {unknown}",
                {"context": "dataset"},
                {"context": "passage"},
            ),
            "dataset / passage / {{context}} / {unknown}",
        )

    def test_inserted_values_remain_opaque_in_both_directions(self):
        dataset = '{context} {{ds:other}} "quoted" \\path\nnext'
        native = '{{ds:name}} {context} \\other\nline'
        self.assertEqual(
            render_template(
                "{{ds:name}}\n{context}\n{{ds:other}}",
                {"name": dataset, "other": "resolved only here"},
                {"context": native},
            ),
            dataset + "\n" + native + "\nresolved only here",
        )

    def test_native_values_do_not_expand_each_other(self):
        self.assertEqual(
            render_template("{subject} {context}", {}, {"subject": "{context}", "context": "text"}),
            "{context} text",
        )

    def test_repeated_aliases_and_empty_strings(self):
        self.assertEqual(
            render_template("{{ds:a}}{{ds:empty}}/{{ds:a}}", {"a": "same", "empty": ""}),
            "same/same",
        )

    def test_unrelated_json_code_tags_pipes_and_braces_survive(self):
        template = '{"name": "{{ds:name}}", "options": {"enabled": true}}\n{a: 1}\n<tag>|pipe|{{other}}'
        self.assertEqual(
            render_template(template, {"name": "Ari"}),
            '{"name": "Ari", "options": {"enabled": true}}\n{a: 1}\n<tag>|pipe|{{other}}',
        )

    def test_empty_mapping_preserves_plain_and_native_templates(self):
        template = "Plain {unmapped}; {{context}}; {}."
        self.assertEqual(render_template(template, {}), template)
        self.assertEqual(render_template("", {}), "")
        self.assertEqual(render_template("{context}", {}, {"context": ""}), "")

    def test_unknown_alias_fails_without_exposing_values(self):
        with self.assertRaisesRegex(DatasetTemplateError, "missing") as caught:
            render_template("{{ds:missing}}", {"other": "sensitive row content"})
        self.assertNotIn("sensitive row content", str(caught.exception))

    def test_malformed_reserved_syntax_fails(self):
        malformed = (
            "{{ds:}}",
            "{{ds:has-hyphen}}",
            "{{ds:1first}}",
            "{{ds: has_space}}",
            "{{ ds:name}}",
            "{{ds :name}}",
            "{{ds:name }}",
            "{{ds:name}",
            "{{ds:name",
            "{{ds:{context}}}",
        )
        for template in malformed:
            with self.subTest(template=template):
                with self.assertRaises(DatasetTemplateError):
                    render_template(template, {"name": "Ari"})
                with self.assertRaises(DatasetTemplateError):
                    dataset_placeholders(template)

    def test_placeholder_inventory_preserves_first_occurrence_order(self):
        self.assertEqual(
            dataset_placeholders("{{ds:b}} {context} {{ds:a}} {{ds:b}} {{other}}"),
            ("b", "a"),
        )
        self.assertEqual(dataset_placeholders("{context} plain {{other}}"), ())

    def test_alias_validation_does_not_rewrite_input(self):
        for alias in ("name", "Name", "_name", "name_2", "context"):
            self.assertEqual(validate_alias(alias), alias)
        for alias in ("", "has space", "has-hyphen", "2name", " name", "name ", "ds:name", "{{ds:name}}", "é", None):
            with self.subTest(alias=alias):
                with self.assertRaises(DatasetTemplateError):
                    validate_alias(alias)

    def test_values_must_have_been_formatted_by_the_binding_layer(self):
        for value in (None, 3, ["a"], {"key": "value"}):
            with self.subTest(value=value):
                with self.assertRaises(DatasetTemplateError):
                    render_template("{{ds:value}}", {"value": value})
                with self.assertRaises(DatasetTemplateError):
                    render_template("{context}", {}, {"context": value})

    def test_template_must_be_a_string(self):
        with self.assertRaises(TypeError):
            render_template(None, {})
        with self.assertRaises(TypeError):
            dataset_placeholders(None)


class LegacyNativeRenderingTests(unittest.TestCase):
    def test_no_dataset_aliases_match_the_original_ordered_replacements(self):
        cases = (
            ("{subject} / {context}", [("{subject}", "nested {context}"), ("{context}", "passage")]),
            ("{{context}} / {{{context}}}", [("{context}", "passage")]),
            ("{subject} / {context}", [("{context}", "passage"), ("{subject}", "nested {context}")]),
            ("{subject}", [("{subject}", "{context}"), ("{context}", "{subject}")]),
            ("{unknown} {} <tag>|pipe|", [("{context}", "unused")]),
            ("", [("{context}", "unused")]),
        )
        for template, replacements in cases:
            with self.subTest(template=template, replacements=replacements):
                expected = template
                for token, value in replacements:
                    expected = expected.replace(token, value)
                self.assertEqual(render_with_native_placeholders(template, {}, replacements), expected)

    def test_native_values_cascade_while_dataset_values_remain_literal(self):
        dataset = "Imported {context}; {{ds:other}}; {{context}}"
        native = "Native {{ds:other}}; {context}"
        self.assertEqual(
            render_with_native_placeholders(
                "{{ds:text}} | {subject} | {context} | {{ds:other}}",
                {"text": dataset, "other": "expanded only where authored"},
                [("{subject}", native), ("{context}", "resolved context")],
            ),
            dataset + " | Native {{ds:other}}; resolved context | resolved context | expanded only where authored",
        )

    def test_double_braces_retain_native_substring_behavior_with_dataset_aliases(self):
        self.assertEqual(
            render_with_native_placeholders(
                "{{context}} {{{context}}} {{ds:context}}",
                {"context": "{{context}}"},
                [("{context}", "native")],
            ),
            "{native} {{native}} {{context}}",
        )

    def test_direct_dataset_name_is_literal_but_native_speaker_keeps_legacy_cascade(self):
        name = "Ada {context}"
        self.assertEqual(
            render_with_native_placeholders(
                "Direct={{ds:name}}; Speaker={speaker}; Context={context}",
                {"name": name},
                [("{speaker}", name), ("{context}", "the hall")],
            ),
            "Direct=Ada {context}; Speaker=Ada the hall; Context=the hall",
        )

    def test_raw_native_text_is_not_parsed_as_dataset_syntax(self):
        raw = "{{ds:missing}} / {{ds:unfinished / {{ ds:invalid }}"
        self.assertEqual(
            render_with_native_placeholders(
                "{{ds:label}} {context}", {"label": "label"}, [("{context}", raw)]
            ),
            "label " + raw,
        )

    def test_original_dataset_placeholders_validate_before_native_operations(self):
        for template in ("{{ds:missing}}", "{{ds:bad-name}}", "{{ ds:name}}", "{{ds:name"):
            with self.subTest(template=template):
                with self.assertRaises(DatasetTemplateError):
                    render_with_native_placeholders(template, {"name": "valid"}, [(template, "removed")])

    def test_repeated_adjacent_empty_and_unicode_values(self):
        self.assertEqual(
            render_with_native_placeholders(
                "{{ds:a}}{{ds:empty}}{{ds:a}}/{context}/{{ds:b}}",
                {"a": "Élodie 雨", "empty": "", "b": "{context}\n\\path"},
                [("{context}", "native")],
            ),
            "Élodie 雨Élodie 雨/native/{context}\n\\path",
        )

    def test_markers_cannot_collide_with_any_input(self):
        dataset = "\ue000\ue001\U000f0000 {context} {{ds:b}}"
        native = "\ue002\ue004"
        self.assertEqual(
            render_with_native_placeholders(
                "\ue003{{ds:a}}/{context}/{{ds:b}}",
                {"a": dataset, "b": "\ue005"},
                [("{context}", native)],
            ),
            "\ue003" + dataset + "/" + native + "/\ue005",
        )

    def test_native_literals_cannot_corrupt_placeholder_markers(self):
        self.assertEqual(
            render_with_native_placeholders(
                "{{ds:first}} 123 \ue000 {{ds:second}}",
                {"first": "123", "second": "\ue001 123"},
                [("1", "x"), ("\ue000", "native"), ("x", "y")],
            ),
            "123 y23 native \ue001 123",
        )

    def test_original_alias_can_be_repeated_but_not_created_by_native_text(self):
        self.assertEqual(
            render_with_native_placeholders(
                "{{ds:a}}/{subject}/{{ds:a}}",
                {"a": "value"},
                [("{subject}", "{{ds:"), ("{{ds:", "{{ds:a}}")],
            ),
            "value/{{ds:a}}/value",
        )

    def test_literal_tokens_may_target_text_inside_original_dataset_syntax(self):
        self.assertEqual(
            render_with_native_placeholders(
                "{{ds:alias}} alias",
                {"alias": "alias"},
                [("alias", "changed")],
            ),
            "alias changed",
        )

    def test_no_native_replacements_matches_static_dataset_rendering(self):
        template = "Name: {{ds:name}}; {context}; {{context}}; {{ds:name}}"
        values = {"name": "Ari {context} {{ds:name}}"}
        self.assertEqual(
            render_with_native_placeholders(template, values, []),
            render_template(template, values),
        )

    def test_native_replacements_accept_an_ordered_generator(self):
        replacements = ((key, value) for key, value in [("{a}", "{b}"), ("{b}", "done")])
        self.assertEqual(
            render_with_native_placeholders("{a} {{ds:x}}", {"x": "{a}"}, replacements),
            "done {a}",
        )

    def test_invalid_values_fail_without_exposing_row_contents(self):
        with self.assertRaises(DatasetTemplateError) as caught:
            render_with_native_placeholders("{{ds:missing}}", {"other": "sensitive row contents"}, [])
        self.assertNotIn("sensitive row contents", str(caught.exception))
        for value in (None, 3, ["a"], {"key": "value"}):
            with self.subTest(value=value):
                with self.assertRaises(DatasetTemplateError):
                    render_with_native_placeholders("{{ds:value}}", {"value": value}, [])
                with self.assertRaises(DatasetTemplateError):
                    render_with_native_placeholders("{context}", {}, [("{context}", value)])
        for token in (None, "", 3):
            with self.subTest(token=token):
                with self.assertRaises(DatasetTemplateError):
                    render_with_native_placeholders("text", {}, [(token, "value")])
        with self.assertRaises(TypeError):
            render_with_native_placeholders(None, {}, [])

    def test_rendering_does_not_advance_scene_randomness(self):
        state = random.getstate()
        render_with_native_placeholders("{{ds:name}} {context}", {"name": "Ada"}, [("{context}", "text")])
        self.assertEqual(random.getstate(), state)


class QuestionLineTests(unittest.TestCase):
    """v11.9.0: input/questions.txt lines are authored text and may use dataset aliases."""

    def test_no_dataset_values_returns_the_line_unchanged(self):
        for line in ("Where does the ferry go?", "", "{{ds:name}}"):
            with self.subTest(line=line):
                self.assertEqual(render_question_line(line, None), line)

    def test_placeholders_written_into_the_line_are_expanded(self):
        self.assertEqual(
            render_question_line("Ask {{ds:name}} about {{ds:topic}}.",
                                 {"name": "Ada", "topic": "the tide"}),
            "Ask Ada about the tide.")

    def test_expansion_is_single_pass(self):
        # A cell that itself looks like a placeholder is inserted literally.
        self.assertEqual(
            render_question_line("{{ds:a}}", {"a": "{{ds:b}}", "b": "leaked"}),
            "{{ds:b}}")

    def test_append_alias_adds_the_value_after_the_separator(self):
        values = {"body": "Chapter one.", "name": "Ada"}
        self.assertEqual(render_question_line("Who are you?", values, append_alias="body"),
                         "Who are you?" + DEFAULT_QUESTION_SEPARATOR + "Chapter one.")
        self.assertEqual(
            render_question_line("Who are you?", values, append_alias="body", separator=" | "),
            "Who are you? | Chapter one.")

    def test_append_and_placeholder_can_be_combined(self):
        values = {"name": "Ada", "body": "Chapter one."}
        self.assertEqual(
            render_question_line("Ask {{ds:name}}.", values, append_alias="body", separator="\n"),
            "Ask Ada.\nChapter one.")

    def test_appended_value_is_not_scanned_for_placeholders(self):
        values = {"body": "Use {{ds:name}} here", "name": "Ada"}
        self.assertEqual(render_question_line("Question?", values, append_alias="body"),
                         "Question?" + DEFAULT_QUESTION_SEPARATOR + "Use {{ds:name}} here")

    def test_empty_appended_value_adds_no_separator(self):
        self.assertEqual(render_question_line("Question?", {"body": ""}, append_alias="body"),
                         "Question?")

    def test_empty_alias_or_none_appends_nothing(self):
        values = {"body": "Chapter one."}
        for append_alias in (None, ""):
            with self.subTest(append_alias=append_alias):
                self.assertEqual(
                    render_question_line("Question?", values, append_alias=append_alias), "Question?")

    def test_an_empty_line_becomes_the_appended_value_alone(self):
        self.assertEqual(render_question_line("", {"body": "Chapter one."}, append_alias="body"),
                         "Chapter one.")

    def test_unusable_append_alias_raises(self):
        with self.assertRaises(DatasetTemplateError):
            render_question_line("Question?", {"other": "x"}, append_alias="missing")
        with self.assertRaises(DatasetTemplateError):
            render_question_line("Question?", None, append_alias="body")
        with self.assertRaises(DatasetTemplateError):
            render_question_line("Question?", {"ds:body": "x"}, append_alias="ds:body")
        with self.assertRaises(DatasetTemplateError):
            render_question_line("Question?", {"body": "x"}, append_alias="body", separator=None)

    def test_unmapped_placeholder_in_the_line_raises(self):
        with self.assertRaises(DatasetTemplateError):
            render_question_line("Ask {{ds:missing}}.", {"name": "Ada"})
        with self.assertRaises(DatasetTemplateError):
            render_question_line("Ask {{ ds:name }}.", {"name": "Ada"})

    def test_a_non_string_line_raises(self):
        with self.assertRaises(TypeError):
            render_question_line(None, {})

    def test_rendering_does_not_mutate_the_supplied_values(self):
        values = {"name": "Ada"}
        render_question_line("Ask {{ds:name}}.", values)
        self.assertEqual(values, {"name": "Ada"})


if __name__ == "__main__":
    unittest.main()
