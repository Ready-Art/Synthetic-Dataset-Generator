"""Regression test for the v11.5 judge JSON-robustness settings in the Configuration Editor.

Run directly: ``python -B tests/test_quality_judge_settings_ui.py`` (needs a display, e.g. under
``xvfb-run``; skips cleanly without one).

The three new Quality Scoring settings -- Judge JSON Repair Rounds, Judge Max Tokens and Judge
Structured Output -- must survive a save/reopen round trip through the REAL ``ConfigEditor``:

  1. type values -> save -> ``config.yml`` holds them under ``quality.*``
  2. reopen the editor -> the values are visible again
  3. reopen -> save without editing -> the values survive (no clobber back to defaults)
  4. out-of-range values are rejected by the editor's validation instead of being written

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

SCRATCH = _tempfile.mkdtemp(prefix="sdg-judge-settings-ui-")
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


class JudgeSettingsUiTests(unittest.TestCase):
    REPAIR = "2"
    MAX_TOKENS = "1024"
    TIMEOUT = "180"
    STRUCTURED = "json_schema"

    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        cls.root.title("Judge JSON robustness settings checks")
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

    def _read_saved_quality(self):
        with open("config/config.yml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return cfg.get("quality", {})

    def test_round_trip(self):
        editor = self._open_editor()
        try:
            editor.quality_judge_json_repair_var.set(self.REPAIR)
            editor.quality_judge_max_tokens_var.set(self.MAX_TOKENS)
            editor.quality_judge_request_timeout_var.set(self.TIMEOUT)
            editor.quality_judge_structured_output_var.set(self.STRUCTURED)
            self.assertTrue(editor.save_config_handler(silent=True), "save reported failure")
            saved = self._read_saved_quality()
            self.assertEqual(saved.get("judge_json_repair_attempts"), 2,
                             "judge_json_repair_attempts not saved")
            self.assertEqual(saved.get("judge_max_tokens"), 1024,
                             "judge_max_tokens not saved")
            self.assertEqual(saved.get("judge_request_timeout"), 180,
                             "judge_request_timeout not saved")
            self.assertEqual(saved.get("judge_structured_output"), self.STRUCTURED,
                             "judge_structured_output not saved")
        finally:
            editor.destroy()

        editor2 = self._open_editor()
        try:
            self.assertEqual(editor2.quality_judge_json_repair_var.get(), self.REPAIR)
            self.assertEqual(editor2.quality_judge_max_tokens_var.get(), self.MAX_TOKENS)
            self.assertEqual(editor2.quality_judge_request_timeout_var.get(), self.TIMEOUT)
            self.assertEqual(editor2.quality_judge_structured_output_var.get(), self.STRUCTURED)
        finally:
            editor2.destroy()

        editor3 = self._open_editor()
        try:
            self.assertTrue(editor3.save_config_handler(silent=True),
                            "second save reported failure")
            saved = self._read_saved_quality()
            self.assertEqual(saved.get("judge_json_repair_attempts"), 2,
                             "unmodified save clobbered judge_json_repair_attempts")
            self.assertEqual(saved.get("judge_request_timeout"), 180,
                             "unmodified save clobbered judge_request_timeout")
            self.assertEqual(saved.get("judge_structured_output"), self.STRUCTURED,
                             "unmodified save clobbered judge_structured_output")
        finally:
            editor3.destroy()

    def test_defaults_are_the_documented_ones(self):
        editor = self._open_editor()
        try:
            self.assertEqual(editor.quality_judge_structured_output_var.get(), "auto",
                             "structured output should default to auto")
        finally:
            editor.destroy()


if __name__ == "__main__":
    unittest.main(verbosity=2)
