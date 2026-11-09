# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Display-dependent test for the dashboard tab strip's information architecture.

The strip was consolidated from twelve tabs to eight: `Totals` folded into
`Overview`, and `API 1`..`API 4` became one `APIs` tab with a slot selector.
This test pins the resulting count and order so a future tab addition is a
deliberate decision rather than an accident, and pins that the consolidation lost
no panel: the old Totals panels and graph live on Overview, the old per-API panels
live on APIs.
"""
import os as _os
import sys as _sys
import unittest

_REPO = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_sys.path.insert(0, _REPO)

try:
    import tkinter as tk
    from tkinter import ttk
except ImportError as exc:
    tk = None
    TK_UNAVAILABLE = str(exc)
else:
    TK_UNAVAILABLE = None

import ui_theme

EXPECTED_TAB_COUNT = 8
EXPECTED_TAB_ORDER = (
    ui_theme.TAB_OVERVIEW, ui_theme.TAB_APIS, ui_theme.TAB_QUALITY,
    ui_theme.TAB_REVIEW, ui_theme.TAB_REGEX_WATCH, ui_theme.TAB_METRICS,
    ui_theme.TAB_PROMPTS, ui_theme.TAB_QUEUE,
)

# The old Totals tab showed seven panels and skipped Errors.
TOTALS_PANEL_KEYS = ("refusals", "user_speak", "slop", "anti_slop",
                     "out_of_character", "non_latin", "scene_continuity")
# The old per-API tabs showed eight panels, Errors included.
PER_API_PANEL_KEYS = TOTALS_PANEL_KEYS + ("errors",)


@unittest.skipIf(tk is None, "Tk and the normal application GUI dependencies are required.")
class DashboardTabStripTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A desktop/virtual display is required: {}".format(exc))
        cls.root.withdraw()
        import generate_tk as g
        cls.g = g
        cls.notebook = g.app_state.dashboard_notebook
        cls.widgets = cls.notebook.tabs_widgets

    @classmethod
    def tearDownClass(cls):
        cls.root.destroy()

    def _labels(self):
        return [self.notebook.tab(tab, "text") for tab in self.notebook.tabs()]

    def test_tab_count_is_eight(self):
        self.assertEqual(len(self.notebook.tabs()), EXPECTED_TAB_COUNT,
                         "the strip is %d tabs, expected %d: %r"
                         % (len(self.notebook.tabs()), EXPECTED_TAB_COUNT, self._labels()))

    def test_tab_order_is_deliberate(self):
        self.assertEqual(tuple(self._labels()), EXPECTED_TAB_ORDER)

    def test_no_tab_label_carries_an_emoji(self):
        """Emoji tab icons render as tofu where no colour-emoji font is installed."""
        for label in self._labels():
            self.assertTrue(label.isascii(), "tab label %r is not plain ASCII" % label)

    def test_overview_keeps_the_totals_panels(self):
        overview = self.widgets[ui_theme.TAB_OVERVIEW]
        for key in TOTALS_PANEL_KEYS:
            self.assertIn(key, overview, "Overview lost the %r panel" % key)
        self.assertNotIn("errors", overview,
                         "Overview must keep the old Totals set, which excluded Errors")
        self.assertIn("graph_canvas", overview, "Overview lost the issue graph")

    def test_apis_keeps_the_per_api_panels(self):
        apis = self.widgets[ui_theme.TAB_APIS]
        for key in PER_API_PANEL_KEYS:
            self.assertIn(key, apis, "APIs lost the %r panel" % key)
        self.assertIn("slot_selector", apis, "APIs has no slot selector")

    def test_no_old_tab_keys_remain(self):
        """Nothing may still index the pre-merge registry keys."""
        for old in ("Totals", "API 1", "API 2", "API 3", "API 4"):
            self.assertNotIn(old, self.widgets, "stale registry key %r" % old)

    def test_overview_watched_content_precedes_diagnostics(self):
        """Progress, cards, panels and graph must all render above the diagnostics.

        Overview used to interleave three diagnostics blocks between the metric
        cards and the issue panels, which pushed the panels below the fold on a
        short window. Pack order is what decides this, so assert on it directly.
        """
        content = self.g.overview_content
        order = [child for child in content.pack_slaves()]
        diagnostics = self.g.overview_diagnostics
        self.assertIn(diagnostics, order, "the diagnostics section is not packed")
        self.assertIs(order[-1], diagnostics,
                      "diagnostics must be the last packed block on Overview")
        for watched in (self.g.progress_frame, self.g.metrics_frame,
                        self.g.metrics_row2, self.g.metrics_row3):
            self.assertIn(watched, order)
            self.assertLess(order.index(watched), order.index(diagnostics),
                            "%r renders below the diagnostics" % watched)

    def test_diagnostics_section_starts_collapsed(self):
        """The diagnostics body is hidden until the header is clicked."""
        diagnostics = self.g.overview_diagnostics
        self.assertFalse(diagnostics.expanded,
                         "diagnostics should start collapsed")
        self.assertFalse(diagnostics.content.winfo_manager(),
                         "a collapsed section must not be geometry-managed")

    def test_diagnostics_section_expands_and_holds_every_frame(self):
        """Expanding must reveal the same three frames that used to be inline."""
        diagnostics = self.g.overview_diagnostics
        self.assertEqual(diagnostics.toggle(), True)
        try:
            self.assertTrue(diagnostics.content.winfo_manager(),
                            "expanded section is still not managed")
            for frame in (self.g.db_status_frame, self.g.api_response_times_frame,
                          self.g.maintenance_frame):
                self.assertIs(frame.master, diagnostics.content,
                              "%r is not inside the diagnostics section" % frame)
        finally:
            diagnostics.expand(False)

    def test_every_issue_panel_receives_rows(self):
        """The failure mode of a bad merge is a silently frozen table.

        Every panel on Overview and APIs is fed a non-empty list and must end up
        with a child row. A merge that forgot to retarget a registry lookup leaves
        the table empty without raising, which a layout check cannot see.
        """
        import dashboard
        overview = self.widgets[ui_theme.TAB_OVERVIEW]
        for key in TOTALS_PANEL_KEYS:
            tree = overview[key]
            for item in tree.get_children():
                tree.delete(item)
            dashboard.update_modern_issue_panel(
                tree, [("12:00:00", "a phrase", "a context", 0)], is_total_tab=True)
            self.assertTrue(tree.get_children(),
                            "Overview/%s did not receive a row" % key)

        apis = self.widgets[ui_theme.TAB_APIS]
        self.g.app_state.api_slot_var.set(ui_theme.api_tab_name(1))
        for key in PER_API_PANEL_KEYS:
            tree = apis[key]
            for item in tree.get_children():
                tree.delete(item)
            dashboard.update_modern_issue_panel(
                tree, [("12:00:00", "a phrase", "a context")])
            self.assertTrue(tree.get_children(),
                            "APIs/%s did not receive a row" % key)


if __name__ == "__main__":
    unittest.main(verbosity=2)
