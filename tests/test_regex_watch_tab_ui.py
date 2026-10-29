"""Display-dependent regression test for the Regex Watch dashboard tab layout.

Run directly: ``python -B tests/test_regex_watch_tab_ui.py`` (needs a display; skips cleanly
without one, e.g. ``xvfb-run python -B tests/test_regex_watch_tab_ui.py``).

The ``🔎 Regex Watch`` dashboard tab builds two Treeview tables. Both were created without uncertain
style, so they inherited the theme's default ``Treeview`` metrics: a 10pt font (17px linespace)
packed into the flat 20px row height Tk falls back to when the theme sets none. Every row therefore
had 3px of slack, which clipped ascenders/descenders and made the whole tab read as scrunched,
overlapping text. The Review and Queue tables had already been fixed with a dedicated style;
Regex Watch was missed.

This test builds the REAL ``generate.py`` UI and asserts the layout invariants that keep the tab
readable: both tables use the dedicated style, the row height leaves room for the font's linespace
plus padding, and no column is narrower than its own heading text.

No source data, generation run, network, or model API is touched.
"""

import atexit as _atexit
import os as _os
import shutil as _shutil
import sys as _sys
import tempfile as _tempfile
import unittest

_REPO = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_sys.path.insert(0, _REPO)

SCRATCH = _tempfile.mkdtemp(prefix="sdg-regex-tab-ui-")
_atexit.register(_shutil.rmtree, SCRATCH, ignore_errors=True)
_os.chdir(SCRATCH)
_shutil.copytree(_os.path.join(_REPO, "config"), _os.path.join(SCRATCH, "config"))

try:
    import tkinter as tk
    from tkinter import font, ttk
except ImportError as exc:
    tk = None
    TK_UNAVAILABLE = str(exc)
else:
    TK_UNAVAILABLE = None

# A row must leave at least this much slack around the font's linespace, otherwise glyph
# ascenders/descenders are clipped and the text looks compressed.
MIN_ROW_SLACK = 6


class RegexWatchTabLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            import generate  # noqa: F401  (constructs the whole UI)
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        except Exception as exc:  # pragma: no cover - environment problem
            raise unittest.SkipTest("Could not construct the UI: {}".format(exc)) from exc

        import app_state
        import generate as g
        import detection

        cls.g = g
        cls.app_state = app_state
        cls.detection = detection
        cls.root = app_state.root
        cls.notebook = app_state.dashboard_notebook
        cls.tab = "🔎 Regex Watch"
        for tab_id in cls.notebook.tabs():
            if "Regex Watch" in cls.notebook.tab(tab_id, "text"):
                cls.notebook.select(tab_id)
                break
        cls.settle()

        _os.makedirs("output", exist_ok=True)
        cls.widgets = cls.notebook.tabs_widgets[cls.tab]

    @classmethod
    def tearDownClass(cls):
        if tk is not None:
            try:
                cls.root.destroy()
            except tk.TclError:
                pass

    @classmethod
    def settle(cls):
        cls.root.update_idletasks()
        cls.root.update()

    def _trees(self):
        return (("pattern", self.widgets["regex_pattern_tree"]),
                ("hit", self.widgets["regex_hit_tree"]))

    def test_trees_use_the_dedicated_style(self):
        """Both tables must opt into RegexWatch.Treeview, not the compressed theme default."""
        for name, tree in self._trees():
            self.assertEqual(tree.cget("style"), self.g.REGEX_TREE_STYLE,
                             "%s table is not using %s" % (name, self.g.REGEX_TREE_STYLE))

    def test_row_height_leaves_room_for_the_font(self):
        """The row must fit the font's linespace plus readable padding."""
        style = ttk.Style()
        linespace = font.Font(root=self.root, font=self.g.REGEX_TREE_BODY_FONT).metrics("linespace")
        rowheight = int(style.lookup(self.g.REGEX_TREE_STYLE, "rowheight") or 0)
        self.assertGreaterEqual(
            rowheight, linespace + MIN_ROW_SLACK,
            "rowheight %d cannot fit a %dpx font with padding" % (rowheight, linespace))

        # And the rendered rows really are that tall (the style is actually in effect).
        with self.app_state.regex_watch_log_lock:
            self.app_state.regex_watch_log = [
                (pattern, "Not just a whisper, but a promise.", 0, "rewritten")
                for pattern in self.detection.DEFAULT_REGEX_WATCH_PATTERNS[:4]
            ]
        self.g._update_regex_watch_tab(force=True)
        self.settle()
        for name, tree in self._trees():
            heights = [tree.bbox(item)[3] for item in tree.get_children() if tree.bbox(item)]
            self.assertTrue(heights, "%s table rendered no rows" % name)
            self.assertGreaterEqual(min(heights), linespace + MIN_ROW_SLACK,
                                    "%s rows are clipped" % name)

    def test_columns_are_wide_enough_for_their_headings(self):
        """No heading may be truncated by its own column width."""
        scale = self.root.winfo_fpixels("1i") / 96.0
        heading_font = font.Font(root=self.root, font=self.g.REGEX_TREE_HEADING_FONT)
        for name, tree in self._trees():
            for col in tree.cget("columns"):
                width = int(tree.column(col, "width"))
                needed = heading_font.measure(col) + round(24 * scale)
                self.assertGreaterEqual(
                    width, needed,
                    "%s table column %r is %dpx but its heading needs %dpx"
                    % (name, col, width, needed))

    def test_tables_share_one_row_height(self):
        """Both tables must agree, so the two stacked panes look like one surface."""
        with self.app_state.regex_watch_log_lock:
            self.app_state.regex_watch_log = [
                (self.detection.DEFAULT_REGEX_WATCH_PATTERNS[0],
                 "Not just a whisper, but a promise.", 0, "rewritten")]
        self.g._update_regex_watch_tab(force=True)
        self.settle()
        heights = {}
        for name, tree in self._trees():
            for item in tree.get_children():
                box = tree.bbox(item)
                if box:
                    heights[name] = box[3]
                    break
        self.assertEqual(set(heights), {"pattern", "hit"})
        self.assertEqual(len(set(heights.values())), 1,
                         "tables use different row heights: %r" % (heights,))


if __name__ == "__main__":
    unittest.main(verbosity=2)
