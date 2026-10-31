"""Regression test for the v11.9.0 "Questions File Dataset Variables" Configuration Editor
fields -- run directly: ``python -B tests/test_questions_dataset_ui.py`` (needs a display,
e.g. under ``xvfb-run``; skips cleanly without one).

Drives the REAL ConfigEditor and checks the full save/load round trip for the
``prompts.questions_dataset`` settings:

  1. type an alias and a separator -> save -> config.yml holds both, with the separator
     stored as REAL characters (``\\n\\n`` typed in the Entry becomes two newlines)
  2. reopen the editor -> both fields show the stored values again
  3. reopen -> save WITHOUT editing -> both values survive (no clobber to the defaults)
  4. an invalid alias is refused by validation, and so is an alias while the questions
     file is off

A missing Tk installation or unavailable display produces an explicit unittest skip, not a
passing check. No source data, application generation run, network, or model API is touched.
"""

# Repo root (app modules) onto sys.path so the suite runs from the repository root.
import atexit as _atexit
import os as _os
import shutil as _shutil
import sys as _sys
import tempfile as _tempfile
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))

import unittest

SCRATCH = _tempfile.mkdtemp(prefix="sdg-questions-dataset-ui-")
_atexit.register(_shutil.rmtree, SCRATCH, ignore_errors=True)
_os.chdir(SCRATCH)
_shutil.copytree(_os.path.join(_sys.path[0], "config"), _os.path.join(SCRATCH, "config"))

try:
    import tkinter as tk
except ImportError as exc:
    tk = None
    TK_UNAVAILABLE = str(exc)
else:
    TK_UNAVAILABLE = None
    import yaml
    from config_loader import ConfigLoader
    from config_editor import (
        ConfigEditor, decode_escape_sequence, encode_escape_sequence,
        questions_append_alias_value, questions_append_separator_value,
    )
    from dataset_templates import DEFAULT_QUESTION_SEPARATOR

    class _Var(object):
        def __init__(self, value):
            self.value = value

        def get(self):
            return self.value

    class _StubEditor(object):
        """Bare editor stand-in: the module-level helpers must work on it."""

        def __init__(self, separator, alias=""):
            self.questions_append_alias_var_editor = _Var(alias)
            self.questions_append_separator_var_editor = _Var(separator)


class QuestionsDatasetUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        cls.root.title("Questions file dataset variables save/load checks")
        cls.root.withdraw()
        cls.loader = ConfigLoader(path="config/config.yml")
        cls.dup_var = tk.BooleanVar(value=False)
        cls.no_impersonation_var = tk.BooleanVar(value=False)

    @classmethod
    def tearDownClass(cls):
        cls.root.destroy()

    def _open_editor(self):
        # Mirrors generate.open_config_editor(): reload the file, then construct the Toplevel.
        self.loader.load()
        return ConfigEditor(
            self.root, self.loader, self.dup_var, self.no_impersonation_var,
            on_config_saved=lambda: None)

    def _read_saved_block(self):
        with open("config/config.yml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return cfg.get("prompts", {}).get("questions_dataset", {})

    def test_escape_helpers(self):
        self.assertEqual(encode_escape_sequence("\n\n"), "\\n\\n")
        self.assertEqual(encode_escape_sequence("\t\\"), "\\t\\\\")
        self.assertEqual(decode_escape_sequence("\\n\\n"), "\n\n")
        self.assertEqual(decode_escape_sequence("\\t\\\\"), "\t\\")
        # An unknown escape and a lone trailing backslash are kept as typed.
        self.assertEqual(decode_escape_sequence("a\\qb"), "a\\qb")
        self.assertEqual(decode_escape_sequence("tail\\"), "tail\\")
        # decode itself is literal; the default fallback lives in the value helper.
        self.assertEqual(decode_escape_sequence(""), "")
        self.assertEqual(questions_append_separator_value(_StubEditor("")), DEFAULT_QUESTION_SEPARATOR)
        self.assertEqual(decode_escape_sequence(None), DEFAULT_QUESTION_SEPARATOR)

    def test_round_trip(self):
        # The shipped defaults: no alias, two-newline separator.
        editor = self._open_editor()
        try:
            self.assertEqual(editor.questions_append_alias_var_editor.get(), "",
                             "append alias should load empty by default")
            self.assertEqual(editor.questions_append_separator_var_editor.get(), "\\n\\n",
                             "the default separator should be shown escaped")
            editor.use_questions_file_var_editor.set(True)
            editor.questions_append_alias_var_editor.set("chapter_body")
            editor.questions_append_separator_var_editor.set("\\n---\\n")
            self.assertTrue(editor.save_config_handler(silent=True), "save reported failure")
        finally:
            editor.destroy()

        saved = self._read_saved_block()
        self.assertEqual(saved.get("append_alias"), "chapter_body")
        self.assertEqual(saved.get("append_separator"), "\n---\n",
                         "the separator must be stored as real characters")

        # Reopen: both fields must show the stored values again.
        editor2 = self._open_editor()
        try:
            self.assertEqual(editor2.questions_append_alias_var_editor.get(), "chapter_body")
            self.assertEqual(editor2.questions_append_separator_var_editor.get(), "\\n---\\n")
        finally:
            editor2.destroy()

        # Reopen and save WITHOUT editing: an untouched field must not clobber the values.
        editor3 = self._open_editor()
        try:
            self.assertTrue(editor3.save_config_handler(silent=True), "second save reported failure")
        finally:
            editor3.destroy()
        saved_again = self._read_saved_block()
        self.assertEqual(saved_again.get("append_alias"), "chapter_body")
        self.assertEqual(saved_again.get("append_separator"), "\n---\n")

    def test_validation_rejects_an_unusable_alias(self):
        editor = self._open_editor()
        try:
            editor.use_questions_file_var_editor.set(True)
            editor.questions_append_alias_var_editor.set("ds:body")
            self.assertFalse(editor.validate_config_handler(show_success_message=False),
                             "an alias with ds: must be refused")
            editor.questions_append_alias_var_editor.set("")
            self.assertTrue(editor.validate_config_handler(show_success_message=False),
                            "clearing the alias should validate")
        finally:
            editor.destroy()

    def test_validation_rejects_an_alias_without_the_questions_file(self):
        editor = self._open_editor()
        try:
            editor.use_questions_file_var_editor.set(False)
            editor.questions_append_alias_var_editor.set("chapter_body")
            self.assertFalse(editor.validate_config_handler(show_success_message=False),
                             "an append alias needs the questions file on")
        finally:
            editor.destroy()


if __name__ == "__main__":
    unittest.main(verbosity=2)
