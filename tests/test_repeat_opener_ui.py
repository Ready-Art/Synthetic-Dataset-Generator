# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Regression test for the v12.5 repeated-opener guard's Configuration Editor controls.

Run directly: ``python -B tests/test_repeat_opener_ui.py`` (needs a display, e.g. under
``xvfb-run``; skips cleanly without one).

Drives the REAL ConfigEditor and checks the full save/load round trip for the three new
``generation.repeat_opener_*`` settings:

  1. tick the checkbox, set the retry count and type two fixes -> save -> config.yml holds all
     three, and the fix list is stored VERBATIM (quotes, apostrophes and `->` survive)
  2. reopen the editor -> every control shows the stored value again
  3. reopen -> save WITHOUT editing -> nothing is clobbered back to the default

The fixes list is the interesting case: the editor's ``sanitize_input()`` path once stripped
quotes and apostrophes from prose, which would silently corrupt an authored instruction. This test
pins that the list survives a save round trip unchanged.

A missing Tk installation or unavailable display produces an explicit unittest skip, not a passing
check. No source data, application generation run, network, or model API is touched.
"""

# Repo root (app modules) onto sys.path so the suite runs from the repository root.
import atexit as _atexit
import os as _os
import shutil as _shutil
import sys as _sys
import tempfile as _tempfile
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))

import unittest

SCRATCH = _tempfile.mkdtemp(prefix="sdg-repeat-opener-ui-")
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


# Quotes, an apostrophe and an arrow: exactly the characters sanitize_input() used to eat.
FIXES = ["Open on a sound, not on \"her breath\".",
         "Start with what she doesn't say, then let her mood go calm -> sharp."]


class RepeatOpenerUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        cls.root.title("Repeated opener save/load checks")
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

    def _read_saved(self):
        with open("config/config.yml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return cfg.get("generation", {})

    def test_round_trip(self):
        # The shipped defaults: off, 1 retry, no authored fixes.
        editor = self._open_editor()
        try:
            self.assertFalse(editor.repeat_opener_guard_var_editor.get(),
                             "guard should load off by default")
            self.assertEqual(editor.repeat_opener_max_retries_var.get(), "1",
                             "retry count should load its default of 1")
            self.assertEqual(editor.repeat_opener_fixes_text.get("1.0", "end-1c"), "",
                             "fixes should load empty by default")
            # The widget must be writable: this Tk build drops delete/insert on a disabled Text.
            self.assertNotEqual(str(editor.repeat_opener_fixes_text.cget("state")), "disabled",
                                "the fixes widget must not be disabled")

            editor.repeat_opener_guard_var_editor.set(True)
            editor.repeat_opener_max_retries_var.set("2")
            editor.repeat_opener_fixes_text.delete("1.0", tk.END)
            editor.repeat_opener_fixes_text.insert("1.0", "\n".join(FIXES))

            self.assertTrue(editor.save_config_handler(silent=True), "save reported failure")
        finally:
            editor.destroy()

        saved = self._read_saved()
        self.assertTrue(saved.get("repeat_opener_guard"),
                        "repeat_opener_guard not saved to config.yml")
        self.assertEqual(int(saved.get("repeat_opener_max_retries")), 2,
                         "repeat_opener_max_retries not saved")
        self.assertEqual(saved.get("repeat_opener_fixes"), FIXES,
                         "the fix list must be saved verbatim (quotes, apostrophes and -> kept)")

        # Reopen: every control must show the stored value again.
        editor2 = self._open_editor()
        try:
            self.assertTrue(editor2.repeat_opener_guard_var_editor.get(),
                            "guard flag not loaded on reopen")
            self.assertEqual(editor2.repeat_opener_max_retries_var.get(), "2",
                             "retry count not loaded on reopen")
            self.assertEqual(
                [line for line in editor2.repeat_opener_fixes_text.get("1.0", "end-1c").split("\n")
                 if line.strip()],
                FIXES, "fix list not loaded on reopen")
        finally:
            editor2.destroy()

        # Reopen and save WITHOUT editing: an untouched field must not clobber anything.
        editor3 = self._open_editor()
        try:
            self.assertTrue(editor3.save_config_handler(silent=True),
                            "second save reported failure")
        finally:
            editor3.destroy()

        again = self._read_saved()
        self.assertTrue(again.get("repeat_opener_guard"),
                        "unmodified save clobbered the guard flag")
        self.assertEqual(int(again.get("repeat_opener_max_retries")), 2,
                         "unmodified save clobbered the retry count")
        self.assertEqual(again.get("repeat_opener_fixes"), FIXES,
                         "unmodified save clobbered the fix list")

    def test_validation_rejects_a_negative_retry_count(self):
        editor = self._open_editor()
        try:
            editor.repeat_opener_max_retries_var.set("-1")
            self.assertFalse(editor.save_config_handler(silent=True),
                             "a negative retry count must be rejected")
        finally:
            editor.destroy()


if __name__ == "__main__":
    unittest.main(verbosity=2)
