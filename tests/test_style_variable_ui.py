# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Save/load checks for the v13.2.0 ``prompts.style`` editor controls.

A field that is written but never read (or the reverse) is the classic editor defect: the
config looks right on disk and the next time the editor opens it shows something else. These
tests drive the real ``ConfigEditor`` against a scratch copy of the shipped config, so they
pin the round trip rather than the widget names.

The editor itself is a Tk window, so this skips cleanly without a display:

    xvfb-run -a python -B tests/test_style_variable_ui.py
"""

import os
import shutil
import sys
import tempfile
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

try:
    import tkinter as tk
except Exception as exc:  # pragma: no cover - depends on the build
    tk = None
    TK_UNAVAILABLE = str(exc)
else:
    TK_UNAVAILABLE = ""

import yaml  # noqa: E402

from config_editor import ConfigEditor  # noqa: E402
from config_loader import ConfigLoader  # noqa: E402
import generation as g  # noqa: E402


class StyleEditorRoundTrip(unittest.TestCase):
    """The style checkbox and variants Text must survive a save/load cycle."""

    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        cls.root.title("style editor save/load checks")
        cls.root.withdraw()
        cls.loader = ConfigLoader(path="config/config.yml")
        cls.dup_var = tk.BooleanVar(value=True)
        cls.no_impersonation_var = tk.BooleanVar(value=False)

    @classmethod
    def tearDownClass(cls):
        cls.root.destroy()

    def setUp(self):
        # Every test starts from the SHIPPED config, so one test's save cannot leak into the
        # next one's load (unittest runs them in name order, not definition order).
        shutil.copyfile(os.path.join(REPO, "config", "config.yml"),
                        os.path.join(os.getcwd(), "config", "config.yml"))

    def _open_editor(self):
        # Mirrors generate.open_config_editor(): reload the file, then construct the Toplevel.
        self.loader.load()
        return ConfigEditor(
            self.root, self.loader, self.dup_var, self.no_impersonation_var,
            on_config_saved=lambda: None)

    def _read_saved_style_block(self):
        with open("config/config.yml", "r", encoding="utf-8") as handle:
            cfg = yaml.safe_load(handle)
        return cfg.get("prompts", {}).get("style", {})

    def test_shipped_defaults_load_on(self):
        """The shipped config ships the block ON with the built-in list."""
        editor = self._open_editor()
        try:
            self.assertTrue(editor.style_enabled_var_editor.get(),
                            "the shipped style block should load enabled")
            loaded = editor.style_list_text.get("1.0", "end-1c").strip().split("\n")
            self.assertEqual(loaded, list(g.DEFAULT_STYLE_VARIANTS),
                             "the shipped style list did not load into the editor")
        finally:
            editor.destroy()

    def test_round_trip(self):
        """Toggle the checkbox, edit the list, save, and read both back."""
        editor = self._open_editor()
        try:
            editor.style_enabled_var_editor.set(False)
            editor.style_list_text.delete("1.0", tk.END)
            editor.style_list_text.insert(tk.END, "alpha style\nbeta style\n")
            self.assertTrue(editor.save_config_handler(silent=True),
                            "save reported failure")
        finally:
            editor.destroy()

        block = self._read_saved_style_block()
        self.assertIs(block.get("enabled"), False,
                      "prompts.style.enabled was not saved as false")
        self.assertEqual(block.get("styles"), ["alpha style", "beta style"],
                         "prompts.style.styles was not saved as a list")

    def test_reload_shows_the_saved_values(self):
        """What was saved must be what the next open shows -- the actual round trip."""
        # Self-contained: write the values through the editor first, then reopen, so this
        # does not depend on unittest's name ordering.
        editor = self._open_editor()
        try:
            editor.style_enabled_var_editor.set(False)
            editor.style_list_text.delete("1.0", tk.END)
            editor.style_list_text.insert(tk.END, "alpha style\nbeta style\n")
            self.assertTrue(editor.save_config_handler(silent=True),
                            "save reported failure")
        finally:
            editor.destroy()

        editor = self._open_editor()
        try:
            self.assertIs(editor.style_enabled_var_editor.get(), False,
                          "the saved enabled: false did not load back")
            loaded = editor.style_list_text.get("1.0", "end-1c").strip().split("\n")
            self.assertEqual(loaded, ["alpha style", "beta style"],
                             "the saved style list did not load back")
        finally:
            editor.destroy()

    def test_an_empty_list_loads_the_builtins(self):
        """An empty list would read as "no styles configured"; show the built-ins."""
        editor = self._open_editor()
        try:
            editor.style_list_text.delete("1.0", tk.END)
            self.assertTrue(editor.save_config_handler(silent=True),
                            "save reported failure")
        finally:
            editor.destroy()

        editor = self._open_editor()
        try:
            loaded = editor.style_list_text.get("1.0", "end-1c").strip().split("\n")
            self.assertEqual(loaded, list(g.DEFAULT_STYLE_VARIANTS),
                             "an empty saved list did not fall back to the built-ins")
        finally:
            editor.destroy()


if __name__ == "__main__":
    # These tests write config/config.yml, so run them against a scratch copy.
    with tempfile.TemporaryDirectory() as tmp:
        os.makedirs(os.path.join(tmp, "config"), exist_ok=True)
        shutil.copyfile(os.path.join(REPO, "config", "config.yml"),
                        os.path.join(tmp, "config", "config.yml"))
        original_cwd = os.getcwd()
        os.chdir(tmp)
        try:
            unittest.main(verbosity=2)
        finally:
            os.chdir(original_cwd)
