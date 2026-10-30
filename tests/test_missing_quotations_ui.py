"""Regression test for the v11.7.1 "Experiment: Retry missing quotations" Configuration Editor
checkbox -- run directly: ``python -B tests/test_missing_quotations_ui.py`` (needs a display,
e.g. under ``xvfb-run``; skips cleanly without one).

Drives the REAL ConfigEditor and checks the full save/load round trip for the new
``generation.retry_missing_quotations`` flag:

  1. tick the checkbox -> save -> config.yml holds ``retry_missing_quotations: true``
  2. reopen the editor -> the checkbox is ticked again (the flag loaded)
  3. reopen -> save WITHOUT editing -> the flag survives (no clobber back to the default)

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

SCRATCH = _tempfile.mkdtemp(prefix="sdg-missing-quotes-ui-")
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


class MissingQuotationsUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        cls.root.title("Retry missing quotations save/load checks")
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

    def _read_saved_flag(self):
        with open("config/config.yml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return cfg.get("generation", {}).get("retry_missing_quotations")

    def test_round_trip(self):
        # The shipped default is off, so the freshly copied config starts un-ticked.
        editor = self._open_editor()
        try:
            self.assertFalse(editor.retry_missing_quotations_var_editor.get(),
                             "flag should load off by default")
            editor.retry_missing_quotations_var_editor.set(True)
            self.assertTrue(editor.save_config_handler(silent=True),
                            "save reported failure")
            self.assertTrue(self._read_saved_flag(),
                            "retry_missing_quotations not saved to config.yml")
        finally:
            editor.destroy()

        # Reopen: the ticked flag must load back into the checkbox.
        editor2 = self._open_editor()
        try:
            self.assertTrue(editor2.retry_missing_quotations_var_editor.get(),
                            "flag not loaded on reopen")
        finally:
            editor2.destroy()

        # Reopen and save WITHOUT editing: an untouched field must not clobber the flag.
        editor3 = self._open_editor()
        try:
            self.assertTrue(editor3.save_config_handler(silent=True),
                            "second save reported failure")
            self.assertTrue(self._read_saved_flag(),
                            "unmodified save clobbered the stored flag")
        finally:
            editor3.destroy()


if __name__ == "__main__":
    unittest.main(verbosity=2)
