"""Regression test for the v11.6.2 Configuration Editor label/spinbox readability fixes.

Run directly: ``python -B tests/test_editor_readability_ui.py`` (needs a display, e.g. under
``xvfb-run``; skips cleanly without one).

Two layout defects are invisible to a headless test, because they only appear once Tk has
allocated real geometry:

  1. **Skyscraper labels.** A wrapping ``ttk.Label`` whose ``wraplength`` is derived from its own
     parent frame closes a feedback loop when that frame is sized BY the label (``sticky="w"``):
     the label narrows, the frame narrows with it, the next ``<Configure>`` narrows the label
     again, and the text ends up one character per line -- a ~20px-wide, several-hundred-pixel-tall
     column. The Detection tab's "Rewrite rounds per reply" caption and the Samplers tab's
     "Enable Bad Words" description both hit this. The fix wraps against the *container* that owns
     a real allocated width, never against a frame sized by the label.

  2. **Invisible spinbox.** The ``superhero`` theme sets ``ttk.Spinbox`` text to white but leaves
     ``fieldbackground`` unset, so the field falls back to the native white entry background:
     white text on white, i.e. an apparently empty box. The Regex Watch rounds spinbox was hit
     hardest because it also starts disabled. Both spinboxes now use ``Readable.TSpinbox``.

Every check below is measured from the REAL ``ConfigEditor`` under a live display. A missing Tk
installation or unavailable display is an explicit unittest skip, never a pass. No source data,
generation run, network or model API is touched.
"""

# Repo root (app modules) onto sys.path so the suite runs from the repository root.
import atexit as _atexit
import os as _os
import shutil as _shutil
import sys as _sys
import tempfile as _tempfile
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))

import unittest

SCRATCH = _tempfile.mkdtemp(prefix="sdg-editor-readability-ui-")
_atexit.register(_shutil.rmtree, SCRATCH, ignore_errors=True)
_os.chdir(SCRATCH)
_shutil.copytree(_os.path.join(_sys.path[0], "config"), _os.path.join(SCRATCH, "config"))

try:
    import tkinter as tk
    from tkinter import ttk
except ImportError as exc:
    tk = None
    ttk = None
    TK_UNAVAILABLE = str(exc)
else:
    TK_UNAVAILABLE = None
    from config_loader import ConfigLoader
    import config_editor

# The white-on-white spinbox only reproduces under the production 'superhero' theme, where the
# Spinbox text is white and the field background is left to the platform. Testing under Tk's
# default theme would hide the bug entirely, so the root is built with ttkbootstrap when it is
# installed and the theme is applied explicitly either way.
try:
    import ttkbootstrap as _ttkbootstrap
except ImportError:
    _ttkbootstrap = None

PRODUCTION_THEME = "superhero"


# A label is a "skyscraper" when it is much taller than it is wide. Normal wrapped prose is
# wider than tall; one-character-per-line text is the opposite by a wide margin.
MAX_HEIGHT_TO_WIDTH = 2.0

# WCAG AA for large text is 3.0:1; body text wants 4.5:1. The broken spinbox was 1.0:1.
MIN_SPINBOX_CONTRAST = 4.5


def _contrast(widget, foreground, background):
    """WCAG contrast ratio between two Tk colours (names like 'white' are resolved by Tk)."""
    def luminance(color):
        red, green, blue = widget.winfo_rgb(color)
        channels = []
        for value in (red, green, blue):
            value = value / 257.0 / 255.0
            channels.append(value / 12.92 if value <= 0.03928
                            else ((value + 0.055) / 1.055) ** 2.4)
        return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2]
    high, low = sorted((luminance(foreground), luminance(background)), reverse=True)
    return (high + 0.05) / (low + 0.05)


def _descendants(widget):
    for child in widget.winfo_children():
        yield child
        yield from _descendants(child)


class EditorReadabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            if _ttkbootstrap is not None:
                cls.root = _ttkbootstrap.Window(themename=PRODUCTION_THEME)
            else:
                cls.root = tk.Tk()
                ttk.Style(cls.root).theme_use(PRODUCTION_THEME)
        except tk.TclError as exc:
            raise unittest.SkipTest(
                "A working Tk display with the {!r} theme is required: {}".format(
                    PRODUCTION_THEME, exc)) from exc
        cls.root.title("Editor readability checks")
        cls.root.withdraw()
        cls.loader = ConfigLoader(path="config/config.yml")
        cls.dup_var = tk.BooleanVar(value=True)
        cls.no_impersonation_var = tk.BooleanVar(value=False)

    @classmethod
    def tearDownClass(cls):
        cls.root.destroy()

    def setUp(self):
        # A real geometry pass needs a mapped window; withdraw() keeps it off-screen.
        self.editor = self._open_editor()
        self.editor.geometry("1280x900")
        self.editor.deiconify()
        self.editor.update_idletasks()
        self.editor.update()

    def tearDown(self):
        try:
            self.editor.destroy()
        except tk.TclError:
            pass

    def _open_editor(self):
        self.loader.load()
        return config_editor.ConfigEditor(
            self.root, self.loader, self.dup_var, self.no_impersonation_var,
            on_config_saved=lambda: None)

    def _select(self, tab_name):
        for tab in self.editor.notebook.tabs():
            if self.editor.notebook.tab(tab, "text") == tab_name:
                self.editor.notebook.select(tab)
                break
        else:
            self.fail("missing tab: %s" % tab_name)
        self._settle()

    def _settle(self, rounds=6):
        for _ in range(rounds):
            self.editor.update_idletasks()
            self.editor.update()

    def _skyscrapers(self):
        """Every mapped, non-empty label in the selected tab that is taller than it is wide."""
        tab_id = self.editor.notebook.select()
        frame = self.editor.nametowidget(tab_id)
        offenders = []
        for widget in _descendants(frame):
            if widget.winfo_class() != "TLabel":
                continue
            try:
                text = str(widget.cget("text"))
            except tk.TclError:
                continue
            if not text.strip() or not widget.winfo_ismapped():
                continue
            width, height = widget.winfo_width(), widget.winfo_height()
            if width <= 1:
                continue
            if height > MAX_HEIGHT_TO_WIDTH * width:
                offenders.append("%r rendered %sx%s" % (text[:60], width, height))
        return offenders

    # --- 1. no label collapses to a one-character column -------------------------------------

    def test_no_skyscraper_labels_in_any_tab(self):
        """The Detection and Samplers regressions, plus every other tab, at a normal window size."""
        failures = []
        for tab in self.editor.notebook.tabs():
            name = self.editor.notebook.tab(tab, "text")
            self._select(name)
            failures.extend("[%s] %s" % (name, item) for item in self._skyscrapers())
        self.assertEqual(failures, [], "labels collapsed to one character per line:\n"
                         + "\n".join(failures))

    def test_no_skyscraper_labels_at_minimum_window_size(self):
        """The wrap must survive the smallest supported window (760x520), where space is tightest."""
        self.editor.geometry("760x520")
        self._settle()
        failures = []
        for tab in self.editor.notebook.tabs():
            name = self.editor.notebook.tab(tab, "text")
            self._select(name)
            failures.extend("[%s] %s" % (name, item) for item in self._skyscrapers())
        self.assertEqual(failures, [], "labels collapsed at the minimum window size:\n"
                         + "\n".join(failures))

    def test_regex_watch_caption_wraps(self):
        """The caption under "Rewrite rounds per reply" is the specific v11.6.2 report."""
        self._select("Detection")
        caption = None
        for widget in _descendants(self.editor.nametowidget(self.editor.notebook.select())):
            if widget.winfo_class() != "TLabel":
                continue
            try:
                text = str(widget.cget("text"))
            except tk.TclError:
                continue
            if text.startswith("Patterns (one regular expression"):
                caption = widget
                break
        self.assertIsNotNone(caption, "the Regex Watch patterns caption is missing")
        # It must have been handed a real wrap width, not left at the 1px sentinel.
        self.assertGreater(int(caption.cget("wraplength") or 0), 1,
                           "the patterns caption kept wraplength=1 (never wrapped)")

    def test_bad_words_checkbox_description_wraps(self):
        """The Samplers 'Enable Bad Words' description was the tallest skyscraper (~735px)."""
        self._select("Samplers")
        description = None
        for widget in _descendants(self.editor.nametowidget(self.editor.notebook.select())):
            if widget.winfo_class() != "TLabel":
                continue
            try:
                text = str(widget.cget("text"))
            except tk.TclError:
                continue
            if text.startswith("Enable Bad Words"):
                description = widget
                break
        self.assertIsNotNone(description, "the Bad Words checkbox description is missing")
        width, height = description.winfo_width(), description.winfo_height()
        self.assertLessEqual(
            height, MAX_HEIGHT_TO_WIDTH * width,
            "the Bad Words description is still a skyscraper (%sx%s)" % (width, height))

    # --- 2. spinboxes are readable -----------------------------------------------------------

    def _spinbox_contrast(self, widget):
        style = ttk.Style(self.editor)
        style_name = widget.cget("style") or "TSpinbox"
        field = style.lookup(style_name, "fieldbackground")
        text = style.lookup(style_name, "foreground")
        self.assertTrue(field, "%s has no fieldbackground -- the field falls back to native white"
                        % widget)
        self.assertTrue(text, "%s has no foreground" % widget)
        return _contrast(widget, text, field), field, text

    def test_regex_watch_rounds_spinbox_is_readable(self):
        self._select("Detection")
        spinbox = self.editor.regex_watch_rounds_spinbox
        ratio, field, text = self._spinbox_contrast(spinbox)
        self.assertGreaterEqual(ratio, MIN_SPINBOX_CONTRAST,
                                "the rounds spinbox is unreadable (field %s on text %s, %.2f:1)"
                                % (field, text, ratio))

    def test_regex_watch_rounds_spinbox_is_readable_while_disabled(self):
        """Regex Watch is off by default, so the box the user first sees is the DISABLED one."""
        self._select("Detection")
        spinbox = self.editor.regex_watch_rounds_spinbox
        self.editor.regex_watch_enabled_var_editor.set(False)
        self.editor._toggle_regex_watch_fields()
        self._settle()
        self.assertEqual(str(spinbox.cget("state")), "disabled")
        style = ttk.Style(self.editor)
        style_name = spinbox.cget("style") or "TSpinbox"
        field = dict(style.map(style_name, "fieldbackground")).get("disabled") or \
            style.lookup(style_name, "fieldbackground")
        text = dict(style.map(style_name, "foreground")).get("disabled") or \
            style.lookup(style_name, "foreground")
        ratio = _contrast(spinbox, text, field)
        self.assertGreaterEqual(ratio, 3.0,
                                "the disabled rounds spinbox is unreadable (%s on %s, %.2f:1)"
                                % (field, text, ratio))

    def test_regex_watch_rounds_spinbox_accepts_input_when_enabled(self):
        """The reported box "cant be used" -- it must accept a typed value once the feature is on."""
        self._select("Detection")
        spinbox = self.editor.regex_watch_rounds_spinbox
        self.editor.regex_watch_enabled_var_editor.set(True)
        self.editor._toggle_regex_watch_fields()
        self._settle()
        self.assertEqual(str(spinbox.cget("state")), "normal")
        spinbox.delete(0, tk.END)
        spinbox.insert(0, "4")
        self._settle()
        self.assertEqual(self.editor.regex_watch_max_rounds_var_editor.get(), "4")
        self.assertEqual(config_editor.regex_watch_rounds_value(self.editor), 4)

    def test_persona_breakthrough_spinbox_is_readable(self):
        self._select("Character Engine")
        ratio, field, text = self._spinbox_contrast(self.editor.persona_breakthrough_spinbox)
        self.assertGreaterEqual(ratio, MIN_SPINBOX_CONTRAST,
                                "the persona breakthrough spinbox is unreadable "
                                "(field %s on text %s, %.2f:1)" % (field, text, ratio))

    def test_every_spinbox_uses_the_readable_style(self):
        """A new spinbox added later must not silently reintroduce the white-on-white field."""
        found = []
        for tab in self.editor.notebook.tabs():
            self.editor.notebook.select(tab)
            self._settle(2)
            for widget in _descendants(self.editor.nametowidget(tab)):
                if widget.winfo_class() == "TSpinbox":
                    found.append(widget)
        self.assertTrue(found, "no spinbox found to check")
        offenders = ["%s style=%r" % (widget, widget.cget("style"))
                     for widget in found if widget.cget("style") != config_editor.SPINBOX_FIELD_STYLE]
        self.assertEqual(offenders, [], "spinboxes without the readable style:\n"
                         + "\n".join(offenders))


if __name__ == "__main__":
    unittest.main()
