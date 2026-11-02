# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""v12.7.6: Slop and Anti-Slop ship OFF BY DEFAULT.

Three things are pinned here:

1. ``config/config.yml`` carries ``detection.slop.enabled: false`` and
   ``detection.anti_slop.enabled: false`` while still keeping the phrase/fix lists, so a
   layer switched back on restores the saved list instead of losing it.
2. ``generate.py`` gates the phrase lists on those flags, and requires BOTH layers before
   ``slop_to_anti_slop_fallback`` can be true. This is read from source, not imported:
   importing ``generate`` builds the Tk app.
3. The Config Editor's two checkboxes load ``false`` and round-trip through a save
   (skipped cleanly without a display).

The gate must be a GATE, not a deletion: a populated list with ``enabled: false`` has to come
out empty, and the SAME list with ``enabled: true`` has to come back intact. A test that only
checked the first half would pass just as well if the feature had been ripped out.

See ``docs/slop-and-antislop.md`` for why the layers are off, and
``docs/agents/slop-antislop-default-off.md`` for the reference notes.
"""

import os
import re
import sys
import unittest

import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(REPO_ROOT, "config", "config.yml")
GENERATE_PATH = os.path.join(REPO_ROOT, "generate.py")

# The editor half imports the app modules and reads config/config.yml by RELATIVE path (the
# same way generate.py does), so run from the repository root no matter where this file was
# invoked from. Done at import time, before unittest builds the test cases.
if os.getcwd() != REPO_ROOT:
    os.chdir(REPO_ROOT)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

try:
    import tkinter as tk
    TK_UNAVAILABLE = ""
except Exception as exc:  # pragma: no cover - environment dependent
    tk = None
    TK_UNAVAILABLE = str(exc)


def _read(path):
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def _shipped_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


class ShippedConfigTests(unittest.TestCase):
    """The keys exist, are false, and did not take the phrase lists with them."""

    def setUp(self):
        self.cfg = _shipped_config()

    def test_slop_enabled_is_false(self):
        slop = self.cfg["detection"]["slop"]
        self.assertIn("enabled", slop, "detection.slop.enabled must exist in the shipped config")
        self.assertIs(slop["enabled"], False)

    def test_anti_slop_enabled_is_false(self):
        anti = self.cfg["detection"]["anti_slop"]
        self.assertIn("enabled", anti, "detection.anti_slop.enabled must exist in the shipped config")
        self.assertIs(anti["enabled"], False)

    def test_phrase_and_fix_lists_are_preserved(self):
        # A default-off layer must keep its lists: switching it back on has to restore them.
        for layer in ("slop", "anti_slop"):
            block = self.cfg["detection"][layer]
            self.assertIn("phrases", block, "%s.phrases must still exist" % layer)
            self.assertIn("fixes", block, "%s.fixes must still exist" % layer)
            self.assertIsInstance(block["phrases"], list)
            self.assertIsInstance(block["fixes"], list)

    def test_fixer_settings_are_untouched(self):
        # v12.7.6 disabled the layers; it did not remove or re-tune the fixer machinery.
        generation = self.cfg["generation"]
        self.assertEqual(generation["max_slop_sentence_fix_iterations"], 5)
        self.assertIs(generation["slop_to_anti_slop_fallback"], False)
        self.assertIs(generation["use_conversation_history_for_fixers"], True)

    def test_anti_slop_iteration_budget_is_still_ten(self):
        # max_anti_slop_fix_iterations is NOT a key in the shipped config -- the worker falls
        # back to the code default in generation.py. Pin that default, because it is the number
        # the docs quote as the anti-slop fixer's worst-case cost per reply.
        generation_source = _read(os.path.join(REPO_ROOT, "generation.py"))
        self.assertIn("global_config.get('generation.max_anti_slop_fix_iterations', 10)",
                      generation_source)


class GenerateGateTests(unittest.TestCase):
    """`generate.py` is read from disk: importing it would build the Tk app."""

    @classmethod
    def setUpClass(cls):
        cls.source = _read(GENERATE_PATH)

    def test_enabled_flags_are_read(self):
        self.assertIn("detection.slop.enabled", self.source)
        self.assertIn("detection.anti_slop.enabled", self.source)

    def test_slop_phrases_are_gated_on_the_flag(self):
        # current_slop_phrases = (global_config.get('detection.slop.phrases', []) if current_slop_enabled else [])
        pattern = re.compile(
            r"current_slop_phrases\s*=\s*\(\s*global_config\.get\(\s*'detection\.slop\.phrases'",
            re.S)
        self.assertRegex(self.source, pattern,
                         "current_slop_phrases must be gated on current_slop_enabled")
        self.assertIn("if current_slop_enabled else []", self.source)

    def test_anti_slop_phrases_are_gated_on_the_flag(self):
        pattern = re.compile(
            r"current_anti_slop_phrases\s*=\s*\(\s*global_config\.get\(\s*'detection\.anti_slop\.phrases'",
            re.S)
        self.assertRegex(self.source, pattern,
                         "current_anti_slop_phrases must be gated on current_anti_slop_enabled")
        self.assertIn("if current_anti_slop_enabled else []", self.source)

    def test_fallback_requires_both_layers(self):
        # slop_to_anti_slop_fallback needs an anti-slop fixer to fall back TO, so it must be
        # false whenever either layer is off -- otherwise the worker takes the fallback branch
        # with no anti-slop phrases to look for.
        self.assertIn("and current_slop_enabled and current_anti_slop_enabled", self.source)

    def test_no_default_true_creep(self):
        # The whole point of v12.7.6 is that these ship false. A default of True anywhere in
        # the load path would silently undo the change.
        for key in ("detection.slop.enabled", "detection.anti_slop.enabled"):
            self.assertNotIn("get('%s', True)" % key, self.source)


class GateBehaviourTests(unittest.TestCase):
    """The gate's actual effect, exercised against a stub config object."""

    def _gate(self, slop_enabled, anti_slop_enabled, slop_phrases, anti_phrases):
        """Mirror the load path in generate.py so the arithmetic is testable without Tk."""
        current_slop_enabled = bool(slop_enabled)
        current_anti_slop_enabled = bool(anti_slop_enabled)
        current_slop_phrases = slop_phrases if current_slop_enabled else []
        current_anti_slop_phrases = anti_phrases if current_anti_slop_enabled else []
        current_slop_to_anti_slop_fallback = (
            True and current_slop_enabled and current_anti_slop_enabled)
        return (current_slop_phrases, current_anti_slop_phrases,
                current_slop_to_anti_slop_fallback)

    def test_disabled_forces_empty_lists(self):
        phrases, anti, fallback = self._gate(False, False, ["deep breath"], ["barely a whisper"])
        self.assertEqual(phrases, [])
        self.assertEqual(anti, [])
        self.assertIs(fallback, False)

    def test_enabled_restores_the_lists(self):
        # The other half of the contract: this is a gate, not a deletion.
        phrases, anti, fallback = self._gate(True, True, ["deep breath"], ["barely a whisper"])
        self.assertEqual(phrases, ["deep breath"])
        self.assertEqual(anti, ["barely a whisper"])
        self.assertIs(fallback, True)

    def test_fallback_needs_both(self):
        _, _, fallback = self._gate(True, False, ["a"], ["b"])
        self.assertIs(fallback, False)
        _, _, fallback = self._gate(False, True, ["a"], ["b"])
        self.assertIs(fallback, False)


class EditorCheckboxTests(unittest.TestCase):
    """The Config Editor round trip. Needs a display; skips cleanly without one."""

    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: %s" % exc) from exc
        cls.root.title("Slop default-off checks")
        cls.root.withdraw()
        cls.dup_var = tk.BooleanVar(value=True)
        cls.no_impersonation_var = tk.BooleanVar(value=False)

    @classmethod
    def tearDownClass(cls):
        cls.root.destroy()

    def _open_editor(self):
        from config_editor import ConfigEditor
        from config_loader import ConfigLoader
        loader = ConfigLoader(path="config/config.yml")
        loader.load()
        return ConfigEditor(self.root, loader, self.dup_var, self.no_impersonation_var,
                            on_config_saved=lambda: None)

    def test_checkboxes_default_unchecked(self):
        editor = self._open_editor()
        try:
            self.assertIs(bool(editor.slop_enabled_var_editor.get()), False)
            self.assertIs(bool(editor.anti_slop_enabled_var_editor.get()), False)
        finally:
            editor.destroy()

    def test_save_writes_the_flags(self):
        editor = self._open_editor()
        try:
            editor.slop_enabled_var_editor.set(True)
            editor.anti_slop_enabled_var_editor.set(True)
            data = editor._get_current_editor_config_data()
            self.assertIs(data["detection"]["slop"]["enabled"], True)
            self.assertIs(data["detection"]["anti_slop"]["enabled"], True)
        finally:
            editor.destroy()

    def test_save_preserves_phrases_while_off(self):
        editor = self._open_editor()
        try:
            editor.slop_enabled_var_editor.set(False)
            editor.slop_phrases_text.insert("1.0", "a saved phrase")
            data = editor._get_current_editor_config_data()
            self.assertIs(data["detection"]["slop"]["enabled"], False)
            self.assertIn("a saved phrase", data["detection"]["slop"]["phrases"])
        finally:
            editor.destroy()


if __name__ == "__main__":
    unittest.main(verbosity=2)
