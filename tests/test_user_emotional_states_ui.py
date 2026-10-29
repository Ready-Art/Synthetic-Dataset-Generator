"""Regression test for the v10.6.0 user-side emotional-states save/load UI -- run directly:
`python -B tests/test_user_emotional_states_ui.py`.

The Configuration Editor's "User Emotional States / Arcs" text field is created in the 'disabled'
state (the feature is off by default), and on this Tk build text edits on a disabled widget are
dropped silently. When load_config_handler() wrote into the field before re-enabling it, saved
states were lost on every reopen of the editor and an empty field clobbered them back to [] on
save. This test drives the REAL ConfigEditor under a display and checks the full round trip:

  1. enable + type states -> save -> config.yml holds the states
  2. reopen the editor (global_config.load() + new instance) -> the states are visible again
  3. reopen -> save without editing -> the states survive (no clobber)

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

SCRATCH = _tempfile.mkdtemp(prefix="sdg-user-emotions-ui-")
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


class UserEmotionalStatesUiTests(unittest.TestCase):
    STATES = ["curious -> defensive", "angry"]

    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        cls.root.title("User emotional states save/load checks")
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

    def _read_saved_states(self):
        with open("config/config.yml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return cfg.get("prompts", {}).get("user_emotional_states", {})

    def test_round_trip(self):
        # 1. Enable the feature, type states, save.
        editor = self._open_editor()
        try:
            editor.enable_user_emotional_states_var_editor.set(True)
            editor._toggle_user_emotional_states_fields()
            self.assertEqual(editor.user_emotional_states_text.cget("state"), "normal")
            editor.user_emotional_states_text.delete("1.0", tk.END)
            editor.user_emotional_states_text.insert(tk.END, "\n".join(self.STATES))
            self.assertTrue(editor.save_config_handler(silent=True),
                            "save reported failure")
            saved = self._read_saved_states()
            self.assertTrue(saved.get("enabled"), "enabled flag not saved")
            self.assertEqual(saved.get("states"), self.STATES, "states not saved to config.yml")
        finally:
            editor.destroy()

        # 2. Reopen the editor: the typed states must be visible again (previously lost because
        #    load_config_handler wrote into the still-disabled field).
        editor2 = self._open_editor()
        try:
            self.assertTrue(editor2.enable_user_emotional_states_var_editor.get(),
                            "enabled flag not loaded on reopen")
            text = [line.strip() for line in
                    editor2.user_emotional_states_text.get("1.0", tk.END).split("\n") if line.strip()]
            self.assertEqual(text, self.STATES,
                             "states not loaded into the reopened editor")
        finally:
            editor2.destroy()

        # 3. Reopen and save WITHOUT editing: a stale/empty field must not clobber the states.
        editor3 = self._open_editor()
        try:
            self.assertTrue(editor3.save_config_handler(silent=True),
                            "second save reported failure")
            saved = self._read_saved_states()
            self.assertEqual(saved.get("states"), self.STATES,
                             "unmodified save clobbered the stored states")
        finally:
            editor3.destroy()


if __name__ == "__main__":
    unittest.main(verbosity=2)
