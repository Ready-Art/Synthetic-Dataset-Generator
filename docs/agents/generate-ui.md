<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Main window (`generate_tk.py`)

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching the main Tk window: tabs, metric cards, issue panels, worker launch, table styles or the Regex Watch tab. **The Review tab and tab badges have their own file: [`review-tab.md`](review-tab.md).**

**Related:** [`docs/agents/review-tab.md`](review-tab.md), [`docs/agents/generation.md`](generation.md), [`docs/agents/dashboard.md`](dashboard.md), [`docs/agents/config-editor.md`](config-editor.md)

## Contents

- `generate_tk.py`
- `tests/test_regex_watch_tab_ui.py`
- `tests/test_worker_launch.py`
- Tasks that touch these files

---

### `generate_tk.py` [11]
**Purpose:** Main UI, Tkinter app, worker thread management, queue setup
**Key:** `start_processing()`, `worker()` launch, UI widgets, `ConfigEditor`
**Edit for:**
- Queue dataset wiring → `configure_queue_datasets(app_state.task_queue, global_config.get('datasets', {}))` in `start_processing()` attaches an in-memory `DatasetTaskAttachments` service to the run's task queue; attachments die with the queue and are NOT persisted across app restarts
- **Worker total / the `Start processing:` line** → the total is resolved ONCE, before that line, with `run_config.count_worker_threads(all_api_configs_runtime, active_enabled_api_configs_for_worker_list, master_duplication_enabled)`, stored in the local `total_threads`, and the launch loop counts to that same variable. **Never report `app_state.num_threads` here** — it is the legacy `api.threads` scalar, not the run total, and reporting it produced the v13.0.2 bug (30 threads started, "12 threads" logged). `app_state.num_threads` is assigned the resolved total so the control server's status payloads agree. `tests/test_worker_launch.py::test_reported_thread_count_is_the_launched_thread_count` enforces this
- Worker launch → `threading.Thread(target=worker, args=(...), kwargs={...})` in `start_processing()`. **New worker options MUST be added to the `kwargs=` dict, not `args=`** — the v12.0 scene-cast options (`scene_cast_enabled_local`, `scene_cast_config_local`, `scene_continuity_enabled_local`, `scene_continuity_fixes_local`, `scene_continuity_max_rounds`) are the most recent additions, and `tests/test_opening_mode.py` asserts the EXACT kwarg key set, so that set literal must be extended with them — the positional list length is fixed and the OOC/opening-mode params (`current_ooc_*`, `opening_mode_local`, etc.) are kwarg-sourced; adding a positional arg silently shifts every later slot and `tests/test_worker_launch.py` fails on the mismatch
- New UI tab → Add to `dashboard_notebook` + populate widgets; custom tabs (Quality / Review / Regex Watch) need their own gated per-tick refresher (`update_quality_tab()` / `_update_review_table()` / `_update_regex_watch_tab()`), called from the periodic GUI update loop and from `_on_dashboard_tab_changed()` so the incoming tab isn't stale — `update_dashboard()` itself only handles Totals/API issue panels and the graph
- New Treeview table → Give it a dedicated style with an explicit `font` and `rowheight` (see "Table row heights" above); a bare `ttk.Treeview` inherits the theme's cramped 20px row and clips its own text
- Quality tab → `update_quality_tab()` fills the dimension summary labels (from `quality.get_quality_stats()['dimension_averages']`) and the latest-50 scores Treeview from `app_state.quality_scores`
- **Regex Watch tab (v11.6)** → a fixed, non-scrolling diagnostics area (`page_scroll.grid_remove()` + `regex_page`), not an issue panel: it is added to `tab_names` but skipped by the `tab_name not in (...)` issue-panel block. Widget registry keys: `regex_page`, `regex_pattern_tree`, `regex_hit_tree`, `regex_signature`. Refresher `_update_regex_watch_tab(force=False)` is called from the periodic GUI tick and from `_on_dashboard_tab_changed()`; like the Review tab it is gated to the visible tab and rebuilds only when its signature changes, so a tick never clears the selection. It reads the configured patterns from `global_config` and the hits from `app_state.regex_watch_log`. The metric card is `app_state.regex_watch_label` (`_create_ttkbs_metric(metrics_frame, "Regex Watch", "🔎")`)
- **Non-Latin metric card + panels (v11.10.0)** → `app_state.non_latin_label` (`_create_ttkbs_metric(metrics_frame, "Non-Latin", "🔤")`) plus a `"Non-Latin"` entry in BOTH `issue_types` and `issue_keys` (key `non_latin`) and a `color_map` entry, so the Totals and per-API tabs get a `Recent Non-Latin` panel automatically. The panel is fed by `dashboard.update_dashboard()` (`app_state.recent_non_latin_total` / `recent_non_latin_per_api`), and `tests/test_regex_watch_tab_ui.py`-style tab gating does not apply (it is a normal issue panel)
- **Scene cast metric card + panels (v12.0)** → `app_state.scene_continuity_label` (`_create_ttkbs_metric(metrics_frame, "Cast Drift", "🎭")`) plus a `"Cast Drift"` entry in BOTH `issue_types` and `issue_keys` (key `scene_continuity`) and a `color_map` entry, so the Totals and per-API tabs get a `Recent Cast Drift` panel automatically. Fed by `dashboard.update_dashboard()` from `app_state.recent_scene_continuity_total` / `_per_api`, and by the `scene_continuity` series in the graph
- **Metrics tab (v13.1.0)** → a fixed, non-scrolling diagnostics area (like Regex Watch: `page_scroll.grid_remove()` + a `metrics_page` frame), added to `tab_names` and skipped by the `tab_name not in (...)` issue-panel block. Widget registry keys: `metrics_page`, `metrics_tree`, `metrics_graph_canvas`, `metrics_status_label`, `metrics_signature`. Its table uses the shared `REGEX_TREE_STYLE` / `_configure_regex_tree_columns()`, and `draw_vllm_metrics_graph()` builds the two-panel graph. The refresher `_update_metrics_tab(force=False)` is tab-gated and rebuilt only on a signature change, from the GUI tick and `_on_dashboard_tab_changed()`. **One row per API slot, never one aggregate row** — that is the point of the tab. `vllm_metrics.start_polling()` sits next to `init_database_pool()` and `stop_polling()` in the `__main__` `finally:`; **it must never run at import time**, because a test, the CLI and a headless run import or build this module. `reset_all_stats_and_history()` also calls `vllm_metrics.reset_slot_usage()` / `reset_endpoints()`. **The reader itself is documented in [`docs/agents/vllm-metrics.md`](vllm-metrics.md)** — read that before touching it
- **Table row heights (v11.6.1)** → a `ttk.Treeview` with no explicit style inherits the theme's default `Treeview`, whose `rowheight` Tk falls back to a flat 20px for a 10pt font (17px linespace). That 3px of slack clips glyph ascenders/descenders and makes a table read as scrunched, overlapping text — the Review, Queue and Regex Watch tables each hit this. Every table needs its own style with an explicit `font` and a `rowheight` derived from `font.metrics('linespace') + padding`. Regex Watch uses `REGEX_TREE_STYLE` (`'RegexWatch.Treeview'`) plus the module-level `REGEX_TREE_BODY_FONT` / `REGEX_TREE_HEADING_FONT` constants, configured once by `_configure_regex_tree_style()` (called just above the `for tab_name in tab_names:` loop, so it runs before any tab is built) and applied to both trees via `style=REGEX_TREE_STYLE`. Columns are sized by `_configure_regex_tree_columns(tree, columns, widths=..., stretch_columns=...)`, which never lets a column fall below the width its own heading text needs. `tests/test_regex_watch_tab_ui.py` guards all of this
- Queue logic → Task tuple structure + `enqueue_opening_tasks()`
- **Version label (v12.9)** → the `version_label` in the header frame is ONE of the FOUR sites a release bump must touch: the GUI label here, the `README.md` title, the `docs/changelog-v12.md` heading and the `docs/version-history.md` index row (`(**current: vX.Y**)`). Bumping only the changelog leaves the other three stale — v12.8.3 did exactly that, and v12.8.3's own entry had already missed two of them. **Do not bump it for a code change that ships no release**, and do not retag the historical `(v12.8.2)` markers, which annotate when a feature landed.
- **Async-judge wiring in `start_processing()` (v12.9.1)** → `generation._install_quality_write_handler()` must be called through the **module object**, so `generate_tk.py` carries a plain `import generation` alongside its `from generation import ...` lines. `from generation import worker` binds `worker` and **not** `generation`; v12.9 shipped without the module import and every async-scoring run died on the first Start click with `NameError: name 'generation' is not defined`, inside the Tk callback. The branch is gated on `quality.async_scoring` **and** `quality.use_llm_scoring`, so no default run, import-time check or test reached it. `tests/test_undefined_globals.py` now fails on any global a function references but the module never binds — add it to your mental checklist whenever you introduce a new module-object call (`<module>.<attr>()`) in this file
- DB export → `export_db_to_jsonl()`, `clear_database()`
- Quality tab layout → `_rebuild_quality_roleplay_layout()` builds the summary labels + score-table columns from `quality.active_scoring_dimensions()` (the toggle is read at build time and re-checked each tick, so a change in the Config Editor mid-run re-lays-out the Quality tab automatically; `update_quality_tab()` fills rows in that column order)

---

### `tests/test_regex_watch_tab_ui.py`
**Purpose:** Display-dependent regression test for the `Regex Watch` dashboard tab layout (builds the REAL `generate_tk.py` UI; skips cleanly without a display)
**Key:** both tables use `REGEX_TREE_STYLE` (not the theme's default `Treeview`); the style's `rowheight` fits the body font's linespace plus `MIN_ROW_SLACK`; the rendered rows really are that tall; no column is narrower than its own heading text needs; both stacked tables share one row height
**Run:** `python -B tests/test_regex_watch_tab_ui.py` (needs a display, e.g. under `xvfb-run`)
**Edit for:** Add tab-layout cases; keep it free of source data, generation runs, network and model APIs

---

### `tests/test_worker_launch.py` [21]
**Purpose:** AST-level regression check that the worker launch in `generate_tk.py` matches the `worker()` signature in `generation.py`
**Key:** Rebuilds the `worker()` signature from its AST, extracts the actual `threading.Thread(target=worker, args=..., kwargs=...)` call from `start_processing()`, and binds them via `inspect.Signature.bind()`; then asserts the OOC kwargs (`current_ooc_phrases`/`current_ooc_fixes`/`current_ooc_enabled`) are supplied by keyword from same-named locals (never positionally) — no app config, GUI, or network code is evaluated. Also pins the reported thread total: the `Start processing:` line must name `total_threads`, must NOT name `num_threads`, must be logged after the total is resolved through `run_config.count_worker_threads()`, and the launch loop must be `range(total_threads)`
**Run:** `python -B tests/test_worker_launch.py`
**Edit for:** Extend per-kwarg source assertions when new params are moved into / added to the `kwargs=` dict at the launch site

---

### `tests/test_undefined_globals.py`
**Purpose:** Headless guard against a global name a function USES but the module never BINDS — the v12.9.1 `NameError: name 'generation' is not defined` class of bug
**Key:** walks the `symtable` of every application module and fails on any global that is referenced but never bound at module level (assignment, import, `def`/`class`, `global` declaration or builtin); then pins the async-judge wiring specifically — the `generation._install_quality_write_handler()` call in `start_processing()` resolves to a module-level name bound to the module object, that object really defines the attribute, and `generate_tk.py` imports the module rather than only names from it
**Run:** `python -B tests/test_undefined_globals.py` (headless: the modules are parsed from source, never imported, so it needs no display, config, network or model)
**Edit for:** add a module to `CHECKED_MODULES` when you add a new top-level module; extend the wiring cases when a new module-object call is added to a gated branch

---

## Design tokens and styles (v14.0)

Every colour, font, spacing value, tab name and style builder lives in `ui_theme.py`.
**The full token reference, the style-builder table, the emoji fallbacks and the empty
states are in [`ui-theme.md`](ui-theme.md)** — read that before changing any visual value.

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Change a colour, font, spacing value, tab name, emoji icon or empty state | [`ui-theme.md`](ui-theme.md) — `ui_theme.py` only, never a raw `#rrggbb` / bare font family / tab-name string |
| Call a module object (`<module>.<attr>()`) from `generate_tk.py` | `generate_tk.py` — add the plain `import <module>`, not only `from <module> import x`; a `from ... import` never binds the module name and the call fails with `NameError` on whichever branch reaches it. `tests/test_undefined_globals.py` guards it |
| Fix a cramped / clipped Treeview table | `generate_tk.py` — give the table its own style with an explicit `font` + `rowheight` from `font.metrics('linespace')`, and size columns from the heading text. Regex Watch does this via `REGEX_TREE_STYLE`, `_configure_regex_tree_style()`, `_configure_regex_tree_columns()`; guarded by `tests/test_regex_watch_tab_ui.py` |
