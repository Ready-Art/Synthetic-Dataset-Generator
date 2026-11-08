<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Review tab and tab badges

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching the Review tab, its count badge, the flagged-id set, or any code that identifies a dashboard tab by its label.

**Related:** [`docs/agents/generate-ui.md`](generate-ui.md) (the rest of the Tk window), [`docs/agents/quality.md`](quality.md) (what produces a flag), [`docs/agents/dashboard.md`](dashboard.md)

## Contents

- Tab labels are mutable — compare the BASE name
- The Review tab
- Drop Selected
- Dropped conversations
- Review flags persist
- `tests/test_review_tab_ui.py`
- Tasks that touch these files

---

## Tab labels are mutable — compare the BASE name (v14.2.1)

The Review tab's label carries a live count: `"Review"` at zero, `"Review (5)"` once five
conversations are flagged. **Any code that identifies a dashboard tab by comparing
`notebook.tab(id, "text")` to a `ui_theme.TAB_*` constant must go through
`ui_theme.tab_base_name()`.** The badge string itself comes from
`ui_theme.tab_badge(name, count)` / `TAB_BADGE_TEMPLATE`; never build it inline.

The four sites that must use the helper:

1. `generate._active_dashboard_tab_text()` — feeds every gated per-tick refresher
   (`_update_review_table()`, `update_quality_tab()`, `_update_regex_watch_tab()`,
   `_update_metrics_tab()`).
2. `dashboard._active_dashboard_tab_text()` — feeds the issue panels and the graph.
3. The tab-strip reorder loop in `generate.py`.
4. The badge writer in `update_quality_display()` — which also froze at its last value,
   because it looked its own tab up by the label it had just changed.

**This is the v14.2.1 bug.** `_active_dashboard_tab_text()` returned the raw label, so the
moment the badge appeared the Review table decided it was not the visible tab and stopped
rebuilding. The tab showed `Review (5)` above an empty list, while Export (which reads
`quality_review_ids` directly, not the table) still wrote all five rows. Nothing was lost —
the list simply stopped being drawn.

Guarded by `tests/test_review_tab_ui.py::test_05a_badge_does_not_hide_the_list` (the exact
refresh → badge → tick sequence) and `test_05c_tab_badge_helpers_round_trip` (the helpers,
including that a real bracketed tab name such as `"Metrics (vLLM)"` is left alone).

---

## The Review tab (v11.4)

A fixed, non-scrolling split view (`page_scroll.grid_remove()` + a `review_page` frame),
because nesting a scrolling table and a scrolling Text in the page's `ScrollableFrame` made
both unusable.

Widget registry keys: `review_page`, `review_tree`, `review_filter_var`,
`review_count_label`, `detail_text`, `review_signature`, `review_detail_task`.

Helpers: `_record_belongs_to_task()` (id matching, `_api` suffix for duplication rows),
`_review_output_files()` / `_iter_review_records()` (main output + per-API files + database),
`_find_review_record()`, `_format_review_conversation()` (ShareGPT `conversations[]` or
`messages[]`), `_review_signature()` / `_update_review_table(force=False)` (rebuild only on
change so a tick never clears the selection), `_show_review_detail(review_tree, force=False)`
(skips re-render of the same selection so the scroll position survives),
`_export_flagged_review()`, `_drop_review_conversation()`.

**The Treeview item id is the FULL task id** — never reintroduce a truncated display id in
the item id or in the detail lookup. `<<TreeviewSelect>>` (single click) opens the
transcript; the detail pane must keep its own scrollbar and stay out of the page canvas.

---

## 🗑 Drop Selected (v12.8.2)

The only Review action that CHANGES the dataset (Export copies elsewhere, Dismiss All only
clears flags). `_drop_review_conversation()` acts on the Treeview selection and nothing
else — an empty selection is a no-op message, never a dataset-wide delete — behind a
`messagebox.askyesno` confirmation.

Helpers:

- `_drop_task_output_rows(task_id)` rewrites each output JSONL to a `.droptmp` neighbour and
  `os.replace()`s it into place. A corrupt/undecodable line is copied through, never
  discarded, because it cannot be attributed to the task.
- `_drop_task_db_rows(task_id)` deletes
  `task_id = %s OR substring(task_id from 1 for len+4) = '<task_id>_api'` — **deliberately
  no `LIKE`**, since a task id comes from a source file name and an unescaped `%`/`_` pattern
  could match another task.

Both use the `_record_belongs_to_task()` rule, so a task whose id is a PREFIX of the dropped
one survives. The whole rewrite runs under **`app_state.output_data_lock`** (shared state, not
the per-run local `start_processing()` used to create — the drop must take the same lock as a
worker's append), then the task is recorded in `quality_dropped` as
`quality.DROP_REASON_MANUAL`, added to `dropped_task_ids`, removed from `quality_scores` and
`quality_review_ids`, and `save_generation_state()` is called. The count line reports manual
drops separately (`dropped by you`) from judge drops.

`tests/test_review_tab_ui.py` guards the scope: prefix-id row survives, duplication rows go,
corrupt line survives, cancel/empty selection drop nothing.

---

## Dropped conversations (v11.4)

`generation._quality_dropped(verdict)` gates both write paths (non-duplication final write,
duplication per-turn write). A drop must never write a row, never `completed_task_ids.add()`,
and never enter `quality_review_ids`.

---

## Review flags persist (v14.2.1)

`quality_review_ids` is saved as a sorted list via `generation._review_ids_snapshot()` (which
takes `quality_review_lock`; iterating the live set from a judge thread raises
`RuntimeError: Set changed size during iteration`) and restored by both loaders
(`generate.load_generation_state()` and `control.py`'s resume path).

A state file written before this release has no such key, so both fall back to
`generation._derive_review_ids(scores, dropped)` — below `quality.min_score_threshold` and not
dropped, the same rule the scoring path applies.

Without this a resumed run restored every score but listed nothing on the Review tab, and
Export found nothing to write.

---

### `tests/test_review_tab_ui.py`
**Purpose:** Display-dependent regression test for the v11.4 Review tab (builds the REAL `generate.py` UI; skips cleanly without a display)
**Key:** full-task-id Treeview items + single-click selection; transcript rendered from the output JSONL; the detail pane is read-only and scrollable; a refresh tick keeps the selection and the scroll position; Export Flagged → JSONL writes every row with `_quality_score`; the filter narrows and restores the list; (v12.8.2) the tab offers a **Drop Selected** action, and a drop removes ONLY the selected task's rows — a task whose id is a strict PREFIX of the dropped one survives, the task's `_api` duplication rows go with it, a corrupt (non-JSON) line is copied through, and cancelling the confirmation or dropping with an empty selection changes nothing; (v14.2.1) the count badge does not hide the list, `_active_dashboard_tab_text()` returns the base name in BOTH modules, the badge helpers round-trip, and the flags survive a state round-trip (including the derived fallback for a pre-v14.2.1 state file)
**Run:** `python -B tests/test_review_tab_ui.py` (needs a display, e.g. under `xvfb-run`)
**Edit for:** Add Review-tab behaviour cases; keep it free of source data, generation runs, network and model APIs

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Change the Review tab | `generate.py` (`_update_review_table()`, `_show_review_detail()`, `_export_flagged_review()`, `_drop_review_conversation()` + `_drop_task_output_rows()` / `_drop_task_db_rows()`, `_iter_review_records()`, the `elif tab_name == ui_theme.TAB_REVIEW` layout block), `tests/test_review_tab_ui.py` |
| Add or change a tab count badge, or identify a tab by its label | `ui_theme.py` (`tab_badge()`, `tab_base_name()`, `TAB_BADGE_TEMPLATE`) + every label comparison listed above; `tests/test_review_tab_ui.py::test_05a` / `test_05c` |
| Persist or restore a per-task quality set across a resume | `generation.py` (`_build_generation_state_data()`, `_review_ids_snapshot()`, `_derive_review_ids()`), `generate.py` (`load_generation_state()`), `control.py` (resume path), `tests/test_review_tab_ui.py::test_05d` |
