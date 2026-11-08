# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Display-dependent regression test for the v11.4 Review tab.

Run directly: ``python -B tests/test_review_tab_ui.py`` (needs a display; skips cleanly
without one, e.g. ``xvfb-run python -B tests/test_review_tab_ui.py``).

The Review tab used to be unusable cloth:
  * rows could not be selected into a conversation (the table stored a truncated Task ID and
    the detail lookup matched it with ``startswith``)
  * the flagged list was rebuilt on every 500 ms dashboard tick, which cleared the selection
  * the table and the detail Text were nested inside the page's ScrollableFrame, so the wheel
    scrolled the page instead of the conversation and the panes were squeezed
  * "Export Flagged → JSONL" read only ``output/output.jsonl`` and silently wrote nothing

This test drives the REAL ``generate.py`` Review tab (constructed by importing the module)
and checks the fixes end to end: single-click selection, a readable multi-line transcript, a
scrollable detail pane, a stable selection across refresh ticks, and a real export file.
"""

import atexit as _atexit
import json as _json
import os as _os
import shutil as _shutil
import sys as _sys
import tempfile as _tempfile
import time as _time
import unittest

_REPO = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_sys.path.insert(0, _REPO)

SCRATCH = _tempfile.mkdtemp(prefix="sdg-review-ui-")
_atexit.register(_shutil.rmtree, SCRATCH, ignore_errors=True)
_os.chdir(SCRATCH)
_shutil.copytree(_os.path.join(_REPO, "config"), _os.path.join(SCRATCH, "config"))

try:
    import tkinter as tk
except ImportError as exc:
    tk = None
    TK_UNAVAILABLE = str(exc)
else:
    TK_UNAVAILABLE = None

import ui_theme


TASK_ID = "opening_ferry_notes_chunk_at_1042"
LONG_ID = "opening_ferry_notes_chunk_at_1042_and_a_much_longer_suffix_for_truncation"
# A DIFFERENT task whose id is a strict PREFIX of TASK_ID. A drop must never take its row with
# it, so this is the row that catches an id match that is too loose.
PREFIX_ID = "opening_ferry_notes_chunk_at_10"
# A per-API duplication row belonging to TASK_ID (same convention as
# generate._record_belongs_to_task()), which a drop of TASK_ID must remove.
DUP_ID = TASK_ID + "_api0_turn1"
TRANSCRIPT = [
    {"from": "human", "value": "I know. I wanted to ask about the chronometer."},
    {"from": "gpt", "value": "Melody sets down her loupe. \u201cThat one is not for sale.\u201d"},
    {"from": "human", "value": "Not even to look at it?"},
    {"from": "gpt", "value": "A long pause, then she unlocks the case. \u201cFive minutes.\u201d"},
]


class ReviewTabTests(unittest.TestCase):
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
        cls.g = g
        cls.app_state = app_state
        cls.root = app_state.root
        cls.notebook = app_state.dashboard_notebook
        cls.tab = ui_theme.TAB_REVIEW
        for tab_id in cls.notebook.tabs():
            if "Review" in cls.notebook.tab(tab_id, "text"):
                cls.notebook.select(tab_id)
                break
        cls.settle()

        _os.makedirs("output", exist_ok=True)
        with open(_os.path.join("output", "output.jsonl"), "w", encoding="utf-8") as f:
            f.write(_json.dumps({"id": TASK_ID, "conversations": TRANSCRIPT}) + "\n")
            f.write(_json.dumps({"id": LONG_ID, "conversations": TRANSCRIPT}) + "\n")
            f.write(_json.dumps({"id": PREFIX_ID, "conversations": TRANSCRIPT}) + "\n")
            f.write(_json.dumps({"id": DUP_ID, "conversations": TRANSCRIPT}) + "\n")
            # A corrupt line. A drop rewrites the file, and it must copy this through
            # rather than silently delete data it cannot attribute to a task.
            f.write("{not json at all\n")

        now = _time.strftime("%Y-%m-%d %H:%M:%S")
        with app_state.quality_lock:
            for task_id, score in ((TASK_ID, 22.0), (LONG_ID, 44.0)):
                app_state.quality_scores[task_id] = {
                    "composite": score,
                    "dimensions": {"coherence": 40, "naturalness": 35, "engagement": 30,
                                   "diversity": 50, "consistency": 25, "logic": 0,
                                   "technical": 60},
                    "flags": ["below_threshold_50", "logic_issues"],
                    "scored_at": now,
                    "method": "hybrid",
                }
        with app_state.quality_review_lock:
            app_state.quality_review_ids.update({TASK_ID, LONG_ID})

    @classmethod
    def tearDownClass(cls):
        try:
            cls.root.destroy()
        except Exception:
            pass

    @classmethod
    def settle(cls, rounds=4):
        for _ in range(rounds):
            cls.root.update_idletasks()
            cls.root.update()

    def widgets(self):
        return self.notebook.tabs_widgets[self.tab]

    def test_01_rows_are_selectable_and_detail_shows_transcript(self):
        g = self.g
        g._update_review_table(force=True)
        self.settle()

        tree = self.widgets()["review_tree"]
        rows = tree.get_children()
        self.assertEqual(len(rows), 2, "both flagged conversations must be listed")
        self.assertIn(TASK_ID, rows, "the Treeview item id must be the FULL task id")

        # Selecting a row must open its conversation -- the old double-click-only binding and
        # the truncated-id lookup made this impossible.
        tree.selection_set(TASK_ID)
        self.settle()
        g._show_review_detail(tree)
        self.settle()

        detail = self.widgets()["detail_text"]
        text = detail.get("1.0", "end")
        self.assertIn(TASK_ID, text, "the detail pane must name the selected task")
        for turn in TRANSCRIPT:
            self.assertIn(turn["value"], text,
                          "the detail pane must show the conversation text")
        self.assertNotIn("NOTE: Full conversation text is in the output JSONL file",
                         text, "the old placeholder note must be gone")

    def test_02_detail_pane_scrolls(self):
        g = self.g
        tree = self.widgets()["review_tree"]
        tree.selection_set(TASK_ID)
        self.settle()
        g._show_review_detail(tree, force=True)
        self.settle()

        detail = self.widgets()["detail_text"]
        detail.update_idletasks()
        self.assertEqual(str(detail.cget("state")), "disabled",
                         "the detail pane is read-only")
        # The Text must have its own scrollbar and be able to scroll its content.
        self.assertTrue(detail.yview()[1] <= 1.0)
        detail.yview_moveto(1.0)
        self.settle()
        self.assertGreaterEqual(detail.yview()[0], 0.0)
        detail.yview_moveto(0.0)

    def test_03_refresh_tick_keeps_selection_and_scroll(self):
        g = self.g
        tree = self.widgets()["review_tree"]
        tree.selection_set(TASK_ID)
        self.settle()
        g._show_review_detail(tree, force=True)
        self.settle()

        detail = self.widgets()["detail_text"]
        detail.yview_moveto(0.5)
        self.settle()
        before = detail.yview()

        # A dashboard refresh tick must not rebuild the table or reset the reader.
        g._update_review_table()
        self.settle()
        self.assertEqual(tree.selection(), (TASK_ID,),
                         "a refresh tick must not clear the selection")
        self.assertEqual(detail.yview(), before,
                         "a refresh tick must not reset the detail scroll position")

    def test_04_export_writes_every_flagged_conversation(self):
        g = self.g
        g._update_review_table(force=True)
        self.settle()
        g._export_flagged_review()

        path = _os.path.join("output", "quality_review_flagged.jsonl")
        self.assertTrue(_os.path.exists(path), "Export Flagged must create the JSONL file")
        with open(path, "r", encoding="utf-8") as f:
            rows = [_json.loads(line) for line in f if line.strip()]
        # Both flagged conversations, plus the per-API duplication row that belongs to
        # TASK_ID (an export has always carried a task's duplication rows with it).
        self.assertEqual(len(rows), 3, "every flagged conversation must be exported")
        self.assertEqual({row["id"] for row in rows}, {TASK_ID, LONG_ID, DUP_ID})
        for row in rows:
            self.assertIn("_quality_score", row, "each export row carries its quality score")
            self.assertEqual(row["conversations"], TRANSCRIPT,
                             "the exported conversation must be unmodified")

    def test_05_filter_narrows_the_list(self):
        g = self.g
        widgets = self.widgets()
        widgets["review_filter_var"].set("1042_and_a_much_longer")
        g._update_review_table(force=True)
        self.settle()
        tree = widgets["review_tree"]
        self.assertEqual(tree.get_children(), (LONG_ID,),
                         "the filter must narrow the list by task id")
        widgets["review_filter_var"].set("")
        g._update_review_table(force=True)
        self.settle()
        self.assertEqual(len(tree.get_children()), 2, "clearing the filter restores the list")

    # --- v12.8.2: drop ONE selected conversation from the dataset -------------------------

    def output_ids(self):
        """Every ``id`` in output/output.jsonl, plus whether the corrupt line survived."""
        with open(_os.path.join("output", "output.jsonl"), "r", encoding="utf-8") as f:
            lines = [line for line in f.read().splitlines() if line.strip()]
        ids = []
        corrupt = 0
        for line in lines:
            try:
                ids.append(_json.loads(line)["id"])
            except (_json.JSONDecodeError, KeyError, TypeError):
                corrupt += 1
        return ids, corrupt

    def answer_yes(self, yes):
        """Stub the confirmation dialog and return a restore callable."""
        original = self.g.messagebox.askyesno
        self.g.messagebox.askyesno = lambda *a, **k: yes

        def restore():
            self.g.messagebox.askyesno = original
        return restore

    def test_06_review_tab_offers_a_drop_action(self):
        labels = [str(button.cget("text")) for button
                  in self.widgets()["action_layout"].widgets]
        self.assertTrue(any("Drop Selected" in label for label in labels),
                        "the Review tab must offer a Drop Selected action")
        # Drop must be its own action, not a rename of an existing one: Dismiss and Export
        # both leave the dataset untouched, and dropping is the only destructive action.
        self.assertTrue(any("Dismiss All" in label for label in labels))
        self.assertTrue(any("Export Flagged" in label for label in labels))

    def test_07_drop_removes_only_the_selected_conversation(self):
        g = self.g
        g._update_review_table(force=True)
        self.settle()
        tree = self.widgets()["review_tree"]
        self.assertIn(TASK_ID, tree.get_children())
        before_ids, before_corrupt = self.output_ids()
        self.assertEqual(before_corrupt, 1, "the fixture carries one corrupt line")

        restore = self.answer_yes(True)
        try:
            tree.selection_set(TASK_ID)
            self.settle()
            g._drop_review_conversation()
            self.settle()
        finally:
            restore()

        ids, corrupt = self.output_ids()
        self.assertNotIn(TASK_ID, ids, "the selected conversation must be gone")
        self.assertNotIn(DUP_ID, ids,
                         "the dropped conversation's per-API duplication rows go with it")
        self.assertIn(LONG_ID, ids, "every other conversation must survive the drop")
        self.assertIn(PREFIX_ID, ids,
                      "a task whose id is a PREFIX of the dropped one must survive")
        self.assertEqual(len(ids), len(before_ids) - 2,
                         "exactly the selected conversation's rows are removed")
        self.assertEqual(corrupt, 1, "a corrupt line is never discarded by a drop")

        self.assertNotIn(TASK_ID, tree.get_children(),
                         "the dropped row must leave the Review list")
        self.assertIn(LONG_ID, tree.get_children(), "the other flagged rows must stay")
        # The detail pane must not be left showing the conversation that was just deleted.
        detail = self.widgets()["detail_text"].get("1.0", "end")
        self.assertNotEqual(detail.splitlines()[0], "TASK: " + TASK_ID,
                            "the detail pane must not keep rendering a dropped conversation")

        with self.app_state.quality_lock:
            self.assertEqual(self.app_state.quality_dropped.get(TASK_ID),
                             g.quality.DROP_REASON_MANUAL,
                             "a manual drop is recorded as such")
            self.assertIn(TASK_ID, self.app_state.dropped_task_ids)
            self.assertNotIn(TASK_ID, self.app_state.quality_scores,
                             "a dropped row is no longer a scored row")
            self.assertIn(LONG_ID, self.app_state.quality_scores,
                          "other conversations keep their scores")
        with self.app_state.quality_review_lock:
            self.assertNotIn(TASK_ID, self.app_state.quality_review_ids)
            self.assertIn(LONG_ID, self.app_state.quality_review_ids)

        # The count line must report a manual drop separately from a judge drop.
        self.assertIn("dropped by you", self.widgets()["review_count_label"].cget("text"))

    def test_08_cancel_or_no_selection_drops_nothing(self):
        g = self.g
        g._update_review_table(force=True)
        self.settle()
        tree = self.widgets()["review_tree"]
        before_ids, before_corrupt = self.output_ids()

        # (a) declining the confirmation must not touch the dataset.
        restore = self.answer_yes(False)
        try:
            tree.selection_set(LONG_ID)
            self.settle()
            g._drop_review_conversation()
        finally:
            restore()
        self.assertEqual(self.output_ids(), (before_ids, before_corrupt),
                         "declining the confirmation must not remove anything")
        with self.app_state.quality_review_lock:
            self.assertIn(LONG_ID, self.app_state.quality_review_ids)

        # (b) no selection at all must not fall back to "drop everything".
        tree.selection_remove(*tree.selection())
        self.settle()
        restore = self.answer_yes(True)
        try:
            g._drop_review_conversation()
        finally:
            restore()
        self.assertEqual(self.output_ids(), (before_ids, before_corrupt),
                         "an empty selection must never drop the whole dataset")


if __name__ == "__main__":
    unittest.main(verbosity=2)
