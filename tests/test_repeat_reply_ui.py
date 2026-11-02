# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Regression test for the v12.7 repeated-reply guard's Configuration Editor controls.

Run directly: ``python -B tests/test_repeat_reply_ui.py`` (needs a display, e.g. under
``xvfb-run``; skips cleanly without one).

Drives the REAL ConfigEditor and checks the full save/load round trip for the three new
``generation.repeat_reply_*`` settings:

  1. tick the checkbox, set the retry count and type two fixes -> save -> config.yml holds all
     three, and the fix list is stored VERBATIM (quotes, apostrophes and `->` survive)
  2. reopen the editor -> every control shows the stored value again
  3. reopen -> save WITHOUT editing -> nothing is clobbered back to the default

It also pins the two properties that make this guard safe to ship off by default: the three keys
exist in the SHIPPED ``config/config.yml`` with their defaults, and the repeated-reply controls are
INDEPENDENT of the repeated-opener ones (ticking one must not tick the other).

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

SCRATCH = _tempfile.mkdtemp(prefix="sdg-repeat-reply-ui-")
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
FIXES = ["Do not repeat the previous turn; write \"something new\".",
         "Move the scene on, don't restate it -> new action."]


class RepeatReplyUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        cls.root.title("Repeated reply save/load checks")
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

    def test_shipped_defaults(self):
        """The three keys ship off/empty/1, so a default run is unchanged.

        Read from the REPOSITORY's config, not the scratch copy: another test in this file saves
        through the editor and would leave the scratch file mutated.
        """
        with open(_os.path.join(_sys.path[0], "config", "config.yml"), "r", encoding="utf-8") as f:
            shipped = yaml.safe_load(f).get("generation", {})
        self.assertIs(shipped.get("repeat_reply_guard"), False,
                      "repeat_reply_guard must ship off")
        self.assertEqual(shipped.get("repeat_reply_fixes"), [],
                         "repeat_reply_fixes must ship empty (the built-in default is used)")
        self.assertEqual(int(shipped.get("repeat_reply_max_retries")), 1,
                         "repeat_reply_max_retries must ship as 1")

    def test_round_trip(self):
        # The shipped defaults: off, 1 retry, no authored fixes.
        editor = self._open_editor()
        try:
            self.assertFalse(editor.repeat_reply_guard_var_editor.get(),
                             "guard should load off by default")
            self.assertEqual(editor.repeat_reply_max_retries_var.get(), "1",
                             "retry count should load its default of 1")
            self.assertEqual(editor.repeat_reply_fixes_text.get("1.0", "end-1c"), "",
                             "fixes should load empty by default")
            # The widget must be writable: this Tk build drops delete/insert on a disabled Text.
            self.assertNotEqual(str(editor.repeat_reply_fixes_text.cget("state")), "disabled",
                                "the fixes widget must not be disabled")

            editor.repeat_reply_guard_var_editor.set(True)
            editor.repeat_reply_max_retries_var.set("2")
            editor.repeat_reply_fixes_text.delete("1.0", tk.END)
            editor.repeat_reply_fixes_text.insert("1.0", "\n".join(FIXES))

            self.assertTrue(editor.save_config_handler(silent=True), "save reported failure")
        finally:
            editor.destroy()

        saved = self._read_saved()
        self.assertTrue(saved.get("repeat_reply_guard"),
                        "repeat_reply_guard not saved to config.yml")
        self.assertEqual(int(saved.get("repeat_reply_max_retries")), 2,
                         "repeat_reply_max_retries not saved")
        self.assertEqual(saved.get("repeat_reply_fixes"), FIXES,
                         "the fix list must be saved verbatim (quotes, apostrophes and -> kept)")

        # Reopen: every control must show the stored value again.
        editor2 = self._open_editor()
        try:
            self.assertTrue(editor2.repeat_reply_guard_var_editor.get(),
                            "guard flag not loaded on reopen")
            self.assertEqual(editor2.repeat_reply_max_retries_var.get(), "2",
                             "retry count not loaded on reopen")
            self.assertEqual(
                [line for line in editor2.repeat_reply_fixes_text.get("1.0", "end-1c").split("\n")
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
        self.assertTrue(again.get("repeat_reply_guard"),
                        "unmodified save clobbered the guard flag")
        self.assertEqual(int(again.get("repeat_reply_max_retries")), 2,
                         "unmodified save clobbered the retry count")
        self.assertEqual(again.get("repeat_reply_fixes"), FIXES,
                         "unmodified save clobbered the fix list")

    def test_guards_are_independent(self):
        """Ticking one guard must not tick the other: they catch different defects."""
        editor = self._open_editor()
        try:
            self.assertFalse(editor.repeat_opener_guard_var_editor.get(),
                             "the opener guard must stay off by default")
            editor.repeat_reply_guard_var_editor.set(True)
            self.assertFalse(editor.repeat_opener_guard_var_editor.get(),
                             "ticking the reply guard must not tick the opener guard")
            editor.repeat_reply_guard_var_editor.set(False)
            editor.repeat_opener_guard_var_editor.set(True)
            self.assertFalse(editor.repeat_reply_guard_var_editor.get(),
                             "ticking the opener guard must not tick the reply guard")
            # Restore, so a later test is not affected by this one.
        finally:
            editor.destroy()

    def test_validation_rejects_a_negative_retry_count(self):
        editor = self._open_editor()
        try:
            editor.repeat_reply_max_retries_var.set("-1")
            self.assertFalse(editor.save_config_handler(silent=True),
                             "a negative retry count must be rejected")
        finally:
            editor.destroy()


if __name__ == "__main__":
    unittest.main(verbosity=2)
