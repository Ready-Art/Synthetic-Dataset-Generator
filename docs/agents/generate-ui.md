<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Main window (`generate.py`)

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching the main Tk window: tabs, metric cards, issue panels, worker launch, table styles, the Review tab or the Regex Watch tab.

**Related:** [`docs/agents/generation.md`](generation.md), [`docs/agents/dashboard.md`](dashboard.md), [`docs/agents/config-editor.md`](config-editor.md)

## Contents

- `generate.py`
- `tests/test_regex_watch_tab_ui.py`
- `tests/test_review_tab_ui.py`
- `tests/test_worker_launch.py`
- Tasks that touch these files

---

### `generate.py` [11]
**Purpose:** Main UI, Tkinter app, worker thread management, queue setup
**Key:** `start_processing()`, `worker()` launch, UI widgets, `ConfigEditor`
**Edit for:**
- Queue dataset wiring → `configure_queue_datasets(app_state.task_queue, global_config.get('datasets', {}))` in `start_processing()` attaches an in-memory `DatasetTaskAttachments` service to the run's task queue; attachments die with the queue and are NOT persisted across app restarts
- Worker launch → `threading.Thread(target=worker, args=(...), kwargs={...})` in `start_processing()`. **New worker options MUST be added to the `kwargs=` dict, not `args=`** — the v12.0 scene-cast options (`scene_cast_enabled_local`, `scene_cast_config_local`, `scene_continuity_enabled_local`, `scene_continuity_fixes_local`, `scene_continuity_max_rounds`) are the most recent additions, and `tests/test_opening_mode.py` asserts the EXACT kwarg key set, so that set literal must be extended with them — the positional list length is fixed and the OOC/opening-mode params (`current_ooc_*`, `opening_mode_local`, etc.) are kwarg-sourced; adding a positional arg silently shifts every later slot and `tests/test_worker_launch.py` fails on the mismatch
- New UI tab → Add to `dashboard_notebook` + populate widgets; custom tabs (Quality / 🔍 Review / 🔎 Regex Watch) need their own gated per-tick refresher (`update_quality_tab()` / `_update_review_table()` / `_update_regex_watch_tab()`), called from the periodic GUI update loop and from `_on_dashboard_tab_changed()` so the incoming tab isn't stale — `update_dashboard()` itself only handles Totals/API issue panels and the graph
- New Treeview table → Give it a dedicated style with an explicit `font` and `rowheight` (see "Table row heights" above); a bare `ttk.Treeview` inherits the theme's cramped 20px row and clips its own text
- Quality tab → `update_quality_tab()` fills the dimension summary labels (from `quality.get_quality_stats()['dimension_averages']`) and the latest-50 scores Treeview from `app_state.quality_scores`
- **🔍 Review tab (v11.4)** → a fixed, non-scrolling split view (`page_scroll.grid_remove()` + a `review_page` frame) because nesting a scrolling table and a scrolling Text in the page's `ScrollableFrame` made both unusable. Widget registry keys: `review_page`, `review_tree`, `review_filter_var`, `review_count_label`, `detail_text`, `review_signature`, `review_detail_task`. Helpers: `_record_belongs_to_task()` (id matching, `_api` suffix for duplication rows), `_review_output_files()` / `_iter_review_records()` (main output + per-API files + database), `_find_review_record()`, `_format_review_conversation()` (ShareGPT `conversations[]` or `messages[]`), `_review_signature()` / `_update_review_table(force=False)` (rebuild only on change so a tick never clears the selection), `_show_review_detail(review_tree, force=False)` (skips re-render of the same selection so the scroll position survives), `_export_flagged_review()`, `_drop_review_conversation()`. **The Treeview item id is the FULL task id** — never reintroduce a truncated display id in the item id or in the detail lookup. `<<TreeviewSelect>>` (single click) opens the transcript; the detail pane must keep its own scrollbar and stay out of the page canvas
- **🗑 Drop Selected (v12.8.2)** → the only Review action that CHANGES the dataset (Export copies elsewhere, Dismiss All only clears flags). `_drop_review_conversation()` acts on the Treeview selection and nothing else — an empty selection is a no-op message, never a dataset-wide delete — behind a `messagebox.askyesno` confirmation. Helpers: `_drop_task_output_rows(task_id)` rewrites each output JSONL to a `.droptmp` neighbour and `os.replace()`s it into place (a corrupt/undecodable line is copied through, never discarded, because it cannot be attributed to the task); `_drop_task_db_rows(task_id)` deletes `task_id = %s OR substring(task_id from 1 for len+4) = '<task_id>_api'` — **deliberately no `LIKE`**, since a task id comes from a source file name and an unescaped `%`/`_` pattern could match another task. Both use the `_record_belongs_to_task()` rule, so a task whose id is a PREFIX of the dropped one survives. The whole rewrite runs under **`app_state.output_data_lock`** (shared state, not the per-run local `start_processing()` used to create — the drop must take the same lock as a worker's append), then the task is recorded in `quality_dropped` as `quality.DROP_REASON_MANUAL`, added to `dropped_task_ids`, removed from `quality_scores` and `quality_review_ids`, and `save_generation_state()` is called. The count line reports manual drops separately (`dropped by you`) from judge drops. `tests/test_review_tab_ui.py` guards the scope: prefix-id row survives, duplication rows go, corrupt line survives, cancel/empty selection drop nothing
- **🔎 Regex Watch tab (v11.6)** → a fixed, non-scrolling diagnostics area (`page_scroll.grid_remove()` + `regex_page`), not an issue panel: it is added to `tab_names` but skipped by the `tab_name not in (...)` issue-panel block. Widget registry keys: `regex_page`, `regex_pattern_tree`, `regex_hit_tree`, `regex_signature`. Refresher `_update_regex_watch_tab(force=False)` is called from the periodic GUI tick and from `_on_dashboard_tab_changed()`; like the Review tab it is gated to the visible tab and rebuilds only when its signature changes, so a tick never clears the selection. It reads the configured patterns from `global_config` and the hits from `app_state.regex_watch_log`. The metric card is `app_state.regex_watch_label` (`_create_ttkbs_metric(metrics_frame, "Regex Watch", "🔎")`)
- **Non-Latin metric card + panels (v11.10.0)** → `app_state.non_latin_label` (`_create_ttkbs_metric(metrics_frame, "Non-Latin", "🔤")`) plus a `"Non-Latin"` entry in BOTH `issue_types` and `issue_keys` (key `non_latin`) and a `color_map` entry, so the Totals and per-API tabs get a `Recent Non-Latin` panel automatically. The panel is fed by `dashboard.update_dashboard()` (`app_state.recent_non_latin_total` / `recent_non_latin_per_api`), and `tests/test_regex_watch_tab_ui.py`-style tab gating does not apply (it is a normal issue panel)
- **Scene cast metric card + panels (v12.0)** → `app_state.scene_continuity_label` (`_create_ttkbs_metric(metrics_frame, "Cast Drift", "🎭")`) plus a `"Cast Drift"` entry in BOTH `issue_types` and `issue_keys` (key `scene_continuity`) and a `color_map` entry, so the Totals and per-API tabs get a `Recent Cast Drift` panel automatically. Fed by `dashboard.update_dashboard()` from `app_state.recent_scene_continuity_total` / `_per_api`, and by the `scene_continuity` series in the graph
- **Table row heights (v11.6.1)** → a `ttk.Treeview` with no explicit style inherits the theme's default `Treeview`, whose `rowheight` Tk falls back to a flat 20px for a 10pt font (17px linespace). That 3px of slack clips glyph ascenders/descenders and makes a table read as scrunched, overlapping text — the Review, Queue and Regex Watch tables each hit this. Every table needs its own style with an explicit `font` and a `rowheight` derived from `font.metrics('linespace') + padding`. Regex Watch uses `REGEX_TREE_STYLE` (`'RegexWatch.Treeview'`) plus the module-level `REGEX_TREE_BODY_FONT` / `REGEX_TREE_HEADING_FONT` constants, configured once by `_configure_regex_tree_style()` (called just above the `for tab_name in tab_names:` loop, so it runs before any tab is built) and applied to both trees via `style=REGEX_TREE_STYLE`. Columns are sized by `_configure_regex_tree_columns(tree, columns, widths=..., stretch_columns=...)`, which never lets a column fall below the width its own heading text needs. `tests/test_regex_watch_tab_ui.py` guards all of this
- **Dropped conversations (v11.4)** → `generation._quality_dropped(verdict)` gates both write paths (non-duplication final write, duplication per-turn write). A drop must never write a row, never `completed_task_ids.add()`, and never enter `quality_review_ids`
- Queue logic → Task tuple structure + `enqueue_opening_tasks()`
- DB export → `export_db_to_jsonl()`, `clear_database()`
- Quality tab layout → `_rebuild_quality_roleplay_layout()` builds the summary labels + score-table columns from `quality.active_scoring_dimensions()` (the toggle is read at build time and re-checked each tick, so a change in the Config Editor mid-run re-lays-out the Quality tab automatically; `update_quality_tab()` fills rows in that column order)

---

### `tests/test_regex_watch_tab_ui.py`
**Purpose:** Display-dependent regression test for the `🔎 Regex Watch` dashboard tab layout (builds the REAL `generate.py` UI; skips cleanly without a display)
**Key:** both tables use `REGEX_TREE_STYLE` (not the theme's default `Treeview`); the style's `rowheight` fits the body font's linespace plus `MIN_ROW_SLACK`; the rendered rows really are that tall; no column is narrower than its own heading text needs; both stacked tables share one row height
**Run:** `python -B tests/test_regex_watch_tab_ui.py` (needs a display, e.g. under `xvfb-run`)
**Edit for:** Add tab-layout cases; keep it free of source data, generation runs, network and model APIs

---

### `tests/test_review_tab_ui.py`
**Purpose:** Display-dependent regression test for the v11.4 Review tab (builds the REAL `generate.py` UI; skips cleanly without a display)
**Key:** full-task-id Treeview items + single-click selection; transcript rendered from the output JSONL; the detail pane is read-only and scrollable; a refresh tick keeps the selection and the scroll position; Export Flagged → JSONL writes every row with `_quality_score`; the filter narrows and restores the list; (v12.8.2) the tab offers a **Drop Selected** action, and a drop removes ONLY the selected task's rows — a task whose id is a strict PREFIX of the dropped one survives, the task's `_api` duplication rows go with it, a corrupt (non-JSON) line is copied through, and cancelling the confirmation or dropping with an empty selection changes nothing
**Run:** `python -B tests/test_review_tab_ui.py` (needs a display, e.g. under `xvfb-run`)
**Edit for:** Add Review-tab behaviour cases; keep it free of source data, generation runs, network and model APIs

---

### `tests/test_worker_launch.py` [21]
**Purpose:** AST-level regression check that the worker launch in `generate.py` matches the `worker()` signature in `generation.py`
**Key:** Rebuilds the `worker()` signature from its AST, extracts the actual `threading.Thread(target=worker, args=..., kwargs=...)` call from `start_processing()`, and binds them via `inspect.Signature.bind()`; then asserts the OOC kwargs (`current_ooc_phrases`/`current_ooc_fixes`/`current_ooc_enabled`) are supplied by keyword from same-named locals (never positionally) — no app config, GUI, or network code is evaluated
**Run:** `python -B tests/test_worker_launch.py`
**Edit for:** Extend per-kwarg source assertions when new params are moved into / added to the `kwargs=` dict at the launch site

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Change the Review tab | `generate.py` (`_update_review_table()`, `_show_review_detail()`, `_export_flagged_review()`, `_drop_review_conversation()` + `_drop_task_output_rows()` / `_drop_task_db_rows()`, `_iter_review_records()`, the `elif tab_name == "🔍 Review"` layout block), `tests/test_review_tab_ui.py` |
| Fix a cramped / clipped Treeview table | `generate.py` — give the table its own style with an explicit `font` + `rowheight` from `font.metrics('linespace')`, and size columns from the heading text. Regex Watch does this via `REGEX_TREE_STYLE`, `_configure_regex_tree_style()`, `_configure_regex_tree_columns()`; guarded by `tests/test_regex_watch_tab_ui.py` |
