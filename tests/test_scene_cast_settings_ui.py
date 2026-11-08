# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Regression test for the v14.1 Scene Cast transition settings in the Configuration Editor.

Run directly: ``python -B tests/test_scene_cast_settings_ui.py`` (needs a display, e.g. under
``xvfb-run``; skips cleanly without one).

v14.1 added three controls to the Scene Cast panel that decide WHEN and HOW a scheduled
entrance/exit lands:

  * ``exit_detection`` -- ``evidence`` (default) / ``name`` / ``always``
  * ``join_slot``      -- 0 = automatic, else the reply number the arrival lands on
  * ``exit_slot``      -- 0 = automatic, else the reply number the departure lands on

They must survive a save/reopen round trip through the REAL ``ConfigEditor``:

  1. type values -> save -> ``config.yml`` holds them under ``prompts.scene_cast.*``
  2. reopen the editor -> the values are visible again
  3. reopen -> save without editing -> the values survive (no clobber back to defaults)
  4. an out-of-range or unrecognised value is clamped on load, not written through
  5. the spinboxes are disabled until the Scene Cast toggle is on

No source data, generation run, network or model API is touched.
"""

# Repo root (app modules) onto sys.path so the suite runs from the repository root.
import atexit as _atexit
import os as _os
import shutil as _shutil
import sys as _sys
import tempfile as _tempfile
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))

import unittest

SCRATCH = _tempfile.mkdtemp(prefix="sdg-scene-cast-settings-ui-")
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


class SceneCastSettingsUiTests(unittest.TestCase):
    DETECTION = "always"
    JOIN_SLOT = "2"
    EXIT_SLOT = "4"

    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        cls.root.title("Scene Cast transition settings checks")
        cls.root.withdraw()
        cls.loader = ConfigLoader(path="config/config.yml")
        cls.dup_var = tk.BooleanVar(value=True)
        cls.no_impersonation_var = tk.BooleanVar(value=False)

    @classmethod
    def tearDownClass(cls):
        cls.root.destroy()

    def _open_editor(self):
        self.loader.load()
        return ConfigEditor(
            self.root, self.loader, self.dup_var, self.no_impersonation_var,
            on_config_saved=lambda: None)

    def _read_saved_cast(self):
        with open("config/config.yml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return cfg.get("prompts", {}).get("scene_cast", {}) or {}

    def _write_cast(self, cast):
        """Rewrite prompts.scene_cast in config.yml, preserving everything else."""
        with open("config/config.yml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        cfg.setdefault("prompts", {})["scene_cast"] = cast
        with open("config/config.yml", "w", encoding="utf-8") as f:
            yaml.safe_dump(cfg, f, sort_keys=False, allow_unicode=True)

    def test_round_trip(self):
        editor = self._open_editor()
        try:
            editor.enable_scene_cast_var_editor.set(True)
            editor._toggle_scene_cast_fields()
            editor.scene_cast_exit_detection_var_editor.set(self.DETECTION)
            editor.scene_cast_join_slot_var_editor.set(self.JOIN_SLOT)
            editor.scene_cast_exit_slot_var_editor.set(self.EXIT_SLOT)
            self.assertTrue(editor.save_config_handler(silent=True), "save reported failure")
            saved = self._read_saved_cast()
            self.assertEqual(saved.get("exit_detection"), self.DETECTION,
                             "exit_detection not saved")
            self.assertEqual(saved.get("join_slot"), 2, "join_slot not saved")
            self.assertEqual(saved.get("exit_slot"), 4, "exit_slot not saved")
            self.assertIs(saved.get("enabled"), True, "enabled not saved")
        finally:
            editor.destroy()

        editor2 = self._open_editor()
        try:
            self.assertEqual(editor2.scene_cast_exit_detection_var_editor.get(), self.DETECTION)
            self.assertEqual(editor2.scene_cast_join_slot_var_editor.get(), self.JOIN_SLOT)
            self.assertEqual(editor2.scene_cast_exit_slot_var_editor.get(), self.EXIT_SLOT)
            # A saved-on toggle reopens enabled, so the spinboxes must be editable again.
            self.assertEqual(str(editor2.scene_cast_join_slot_spinbox.cget("state")), "normal")
            self.assertEqual(str(editor2.scene_cast_exit_slot_spinbox.cget("state")), "normal")
        finally:
            editor2.destroy()

        editor3 = self._open_editor()
        try:
            self.assertTrue(editor3.save_config_handler(silent=True),
                            "second save reported failure")
            saved = self._read_saved_cast()
            self.assertEqual(saved.get("exit_detection"), self.DETECTION,
                             "unmodified save clobbered exit_detection")
            self.assertEqual(saved.get("join_slot"), 2, "unmodified save clobbered join_slot")
            self.assertEqual(saved.get("exit_slot"), 4, "unmodified save clobbered exit_slot")
        finally:
            editor3.destroy()

    def test_invalid_values_are_clamped_on_load(self):
        """An unknown mode or an out-of-range slot must not reach the worker as typed."""
        self._write_cast({
            "enabled": True, "shape": "minimal", "min_present": 2, "max_churn": 2,
            "max_deferrals": 1, "placement": "depth0",
            "exit_detection": "nonsense", "join_slot": 999, "exit_slot": -5,
        })
        editor = self._open_editor()
        try:
            self.assertEqual(editor.scene_cast_exit_detection_var_editor.get(), "evidence",
                             "an unknown exit_detection must fall back to 'evidence'")
            self.assertEqual(editor.scene_cast_join_slot_var_editor.get(), "20",
                             "join_slot must clamp to its spinbox maximum")
            self.assertEqual(editor.scene_cast_exit_slot_var_editor.get(), "0",
                             "a negative exit_slot must clamp to 0 (automatic)")
        finally:
            editor.destroy()

    def test_fields_disabled_while_off(self):
        self._write_cast({"enabled": False, "shape": "minimal", "min_present": 2,
                          "max_churn": 2, "max_deferrals": 1, "placement": "depth0",
                          "exit_detection": "evidence", "join_slot": 0, "exit_slot": 0})
        editor = self._open_editor()
        try:
            for name in ("scene_cast_join_slot_spinbox", "scene_cast_exit_slot_spinbox",
                         "scene_cast_min_present_spinbox"):
                widget = getattr(editor, name)
                self.assertEqual(str(widget.cget("state")), "disabled",
                                 name + " should be disabled while Scene Cast is off")
            editor.enable_scene_cast_var_editor.set(True)
            editor._toggle_scene_cast_fields()
            for name in ("scene_cast_join_slot_spinbox", "scene_cast_exit_slot_spinbox"):
                widget = getattr(editor, name)
                self.assertEqual(str(widget.cget("state")), "normal",
                                 name + " should be editable once Scene Cast is on")
        finally:
            editor.destroy()


if __name__ == "__main__":
    unittest.main(verbosity=2)
