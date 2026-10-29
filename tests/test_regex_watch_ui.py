"""Regression test for the v11.6 Regex Watch editor UI -- run directly:
`python -B tests/test_regex_watch_ui.py` (needs a display, e.g. under `xvfb-run`).

The Regex Watch panel lets the end user add, edit and remove their own regular expressions. This
test drives the REAL ConfigEditor under a display and checks the whole round trip, because the two
failure modes that matter here are invisible to a pure-Python test:

  1. a pattern must survive save -> config.yml -> reopen byte for byte (no backslash or quote
     mangling, which a naive sanitize step would cause);
  2. a pattern that does not compile must be REJECTED at save time, not silently skipped at run
     time;
  3. an unmodified reopen+save must not clobber the stored list.

A missing Tk installation or unavailable display produces an explicit unittest skip, not a
passing check. No source data, application generation run, network, or model API is touched.
"""

import atexit as _atexit
import os as _os
import shutil as _shutil
import sys as _sys
import tempfile as _tempfile
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))

import unittest

SCRATCH = _tempfile.mkdtemp(prefix="sdg-regex-watch-ui-")
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
    from config_editor import ConfigEditor


class RegexWatchUiTests(unittest.TestCase):
    # A pattern with backslashes, a quote class and a curly-quote escape: everything a naive
    # sanitize step would corrupt.
    PATTERN = r"\bnot\s+(?:just|only)\s+[^.!?\n]{1,40}?[,;:]\s*but\s+(?:also\s+)?\w"
    SECOND = r"it(?:'s|\u2019s|\s+is)\s+not\s+just"

    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        cls.root.title("Regex Watch save/load checks")
        cls.root.withdraw()
        cls.loader = ConfigLoader(path="config/config.yml")
        cls.dup_var = tk.BooleanVar(value=True)
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
        with open("config/config.yml", "r", encoding="utf-8") as handle:
            cfg = yaml.safe_load(handle)
        return cfg.get("detection", {}).get("regex_watch", {})

    def test_add_remove_and_round_trip(self):
        editor = self._open_editor()
        try:
            # The panel starts empty-or-shipped; start from a known state.
            editor.regex_watch_patterns = []
            editor._render_regex_patterns()
            editor.regex_watch_enabled_var_editor.set(True)
            editor._toggle_regex_watch_fields()
            self.assertEqual(str(editor.regex_watch_entry.cget("state")), "normal")

            # Add both patterns through the real Add button path.
            for pattern in (self.PATTERN, self.SECOND):
                editor.regex_watch_entry_var.set(pattern)
                editor._add_regex_pattern()
            self.assertEqual(editor.regex_watch_patterns, [self.PATTERN, self.SECOND])
            self.assertEqual(editor.regex_watch_listbox.size(), 2,
                             "the Listbox does not mirror the pattern list")

            # A duplicate is refused.
            editor.regex_watch_entry_var.set(self.PATTERN)
            editor._add_regex_pattern()
            self.assertEqual(len(editor.regex_watch_patterns), 2, "a duplicate pattern was added")

            editor.regex_watch_max_rounds_var_editor.set("4")
            self.assertTrue(editor.save_config_handler(silent=True), "save reported failure")
            saved = self._read_saved_block()
            self.assertTrue(saved.get("enabled"), "enabled flag not saved")
            self.assertEqual(saved.get("max_rounds"), 4, "rounds not saved")
            self.assertEqual(saved.get("patterns"), [self.PATTERN, self.SECOND],
                             "patterns were mangled on save")
        finally:
            editor.destroy()

        # Reopen: the patterns must be visible again, unchanged.
        editor2 = self._open_editor()
        try:
            self.assertTrue(editor2.regex_watch_enabled_var_editor.get(),
                            "enabled flag not loaded on reopen")
            self.assertEqual(editor2.regex_watch_patterns, [self.PATTERN, self.SECOND],
                             "patterns not loaded into the reopened editor")
            self.assertEqual(editor2.regex_watch_max_rounds_var_editor.get(), "4")
        finally:
            editor2.destroy()

        # Reopen and save WITHOUT editing: nothing may be clobbered.
        editor3 = self._open_editor()
        try:
            self.assertTrue(editor3.save_config_handler(silent=True), "second save failed")
            self.assertEqual(self._read_saved_block().get("patterns"),
                             [self.PATTERN, self.SECOND],
                             "an unmodified save clobbered the patterns")
        finally:
            editor3.destroy()

    def test_remove_pattern(self):
        editor = self._open_editor()
        try:
            editor.regex_watch_patterns = [self.PATTERN, self.SECOND]
            editor._render_regex_patterns()
            editor.regex_watch_enabled_var_editor.set(True)
            editor._toggle_regex_watch_fields()
            editor.regex_watch_listbox.selection_set(0)
            editor._remove_regex_pattern()
            self.assertEqual(editor.regex_watch_patterns, [self.SECOND],
                             "the selected pattern was not removed")
            self.assertEqual(editor.regex_watch_listbox.size(), 1)
            self.assertTrue(editor.save_config_handler(silent=True))
            self.assertEqual(self._read_saved_block().get("patterns"), [self.SECOND],
                             "the removal was not saved")
        finally:
            editor.destroy()

    def test_invalid_pattern_is_rejected(self):
        editor = self._open_editor()
        try:
            editor.regex_watch_patterns = [self.PATTERN]
            editor._render_regex_patterns()
            editor.regex_watch_enabled_var_editor.set(True)
            editor._toggle_regex_watch_fields()

            # The Add path refuses it and says why.
            editor.regex_watch_entry_var.set("[unclosed")
            editor._add_regex_pattern()
            self.assertEqual(editor.regex_watch_patterns, [self.PATTERN],
                             "an invalid pattern was added")
            self.assertIn("Invalid", editor.regex_watch_status_label.cget("text"))

            # A pattern injected past the Add path (as a hand-edited config could) blocks the
            # save: validate_config_handler() reports it and returns False.
            editor.regex_watch_patterns = [self.PATTERN, "[unclosed"]
            self.assertFalse(editor.validate_config_handler(show_success_message=False),
                             "validation accepted an invalid pattern")
            self.assertIn("Regex Watch", editor.status.cget("text"))
        finally:
            editor.destroy()

    def test_toggle_disables_the_entry(self):
        editor = self._open_editor()
        try:
            editor.regex_watch_enabled_var_editor.set(False)
            editor._toggle_regex_watch_fields()
            self.assertEqual(str(editor.regex_watch_entry.cget("state")), "disabled")
            self.assertEqual(str(editor.regex_watch_add_button.cget("state")), "disabled")
            editor.regex_watch_enabled_var_editor.set(True)
            editor._toggle_regex_watch_fields()
            self.assertEqual(str(editor.regex_watch_entry.cget("state")), "normal")
        finally:
            editor.destroy()

    def test_restore_defaults(self):
        editor = self._open_editor()
        try:
            import detection
            editor.regex_watch_patterns = []
            editor.regex_watch_enabled_var_editor.set(True)
            editor._toggle_regex_watch_fields()
            editor._restore_default_regex_patterns()
            self.assertTrue(editor.regex_watch_patterns, "restore produced an empty list")
            self.assertEqual(editor.regex_watch_patterns,
                             list(detection.DEFAULT_REGEX_WATCH_PATTERNS),
                             "restore did not use the shipped defaults")
        finally:
            editor.destroy()


if __name__ == "__main__":
    unittest.main()
