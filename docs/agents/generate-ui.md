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
- **Worker total / the `Start processing:` line** → the total is resolved ONCE, before that line, with `run_config.count_worker_threads(all_api_configs_runtime, active_enabled_api_configs_for_worker_list, master_duplication_enabled)`, stored in the local `total_threads`, and the launch loop counts to that same variable. **Never report `app_state.num_threads` here** — it is the legacy `api.threads` scalar, not the run total, and reporting it produced the v13.0.2 bug (30 threads started, "12 threads" logged). `app_state.num_threads` is assigned the resolved total so the control server's status payloads agree. `tests/test_worker_launch.py::test_reported_thread_count_is_the_launched_thread_count` enforces this
- Worker launch → `threading.Thread(target=worker, args=(...), kwargs={...})` in `start_processing()`. **New worker options MUST be added to the `kwargs=` dict, not `args=`** — the v12.0 scene-cast options (`scene_cast_enabled_local`, `scene_cast_config_local`, `scene_continuity_enabled_local`, `scene_continuity_fixes_local`, `scene_continuity_max_rounds`) are the most recent additions, and `tests/test_opening_mode.py` asserts the EXACT kwarg key set, so that set literal must be extended with them — the positional list length is fixed and the OOC/opening-mode params (`current_ooc_*`, `opening_mode_local`, etc.) are kwarg-sourced; adding a positional arg silently shifts every later slot and `tests/test_worker_launch.py` fails on the mismatch
- New UI tab → Add to `dashboard_notebook` + populate widgets; custom tabs (Quality / Review / Regex Watch) need their own gated per-tick refresher (`update_quality_tab()` / `_update_review_table()` / `_update_regex_watch_tab()`), called from the periodic GUI update loop and from `_on_dashboard_tab_changed()` so the incoming tab isn't stale — `update_dashboard()` itself only handles Totals/API issue panels and the graph
- New Treeview table → Give it a dedicated style with an explicit `font` and `rowheight` (see "Table row heights" above); a bare `ttk.Treeview` inherits the theme's cramped 20px row and clips its own text
- Quality tab → `update_quality_tab()` fills the dimension summary labels (from `quality.get_quality_stats()['dimension_averages']`) and the latest-50 scores Treeview from `app_state.quality_scores`
- **Review tab (v11.4)** → a fixed, non-scrolling split view (`page_scroll.grid_remove()` + a `review_page` frame) because nesting a scrolling table and a scrolling Text in the page's `ScrollableFrame` made both unusable. Widget registry keys: `review_page`, `review_tree`, `review_filter_var`, `review_count_label`, `detail_text`, `review_signature`, `review_detail_task`. Helpers: `_record_belongs_to_task()` (id matching, `_api` suffix for duplication rows), `_review_output_files()` / `_iter_review_records()` (main output + per-API files + database), `_find_review_record()`, `_format_review_conversation()` (ShareGPT `conversations[]` or `messages[]`), `_review_signature()` / `_update_review_table(force=False)` (rebuild only on change so a tick never clears the selection), `_show_review_detail(review_tree, force=False)` (skips re-render of the same selection so the scroll position survives), `_export_flagged_review()`, `_drop_review_conversation()`. **The Treeview item id is the FULL task id** — never reintroduce a truncated display id in the item id or in the detail lookup. `<<TreeviewSelect>>` (single click) opens the transcript; the detail pane must keep its own scrollbar and stay out of the page canvas
- **🗑 Drop Selected (v12.8.2)** → the only Review action that CHANGES the dataset (Export copies elsewhere, Dismiss All only clears flags). `_drop_review_conversation()` acts on the Treeview selection and nothing else — an empty selection is a no-op message, never a dataset-wide delete — behind a `messagebox.askyesno` confirmation. Helpers: `_drop_task_output_rows(task_id)` rewrites each output JSONL to a `.droptmp` neighbour and `os.replace()`s it into place (a corrupt/undecodable line is copied through, never discarded, because it cannot be attributed to the task); `_drop_task_db_rows(task_id)` deletes `task_id = %s OR substring(task_id from 1 for len+4) = '<task_id>_api'` — **deliberately no `LIKE`**, since a task id comes from a source file name and an unescaped `%`/`_` pattern could match another task. Both use the `_record_belongs_to_task()` rule, so a task whose id is a PREFIX of the dropped one survives. The whole rewrite runs under **`app_state.output_data_lock`** (shared state, not the per-run local `start_processing()` used to create — the drop must take the same lock as a worker's append), then the task is recorded in `quality_dropped` as `quality.DROP_REASON_MANUAL`, added to `dropped_task_ids`, removed from `quality_scores` and `quality_review_ids`, and `save_generation_state()` is called. The count line reports manual drops separately (`dropped by you`) from judge drops. `tests/test_review_tab_ui.py` guards the scope: prefix-id row survives, duplication rows go, corrupt line survives, cancel/empty selection drop nothing
- **Regex Watch tab (v11.6)** → a fixed, non-scrolling diagnostics area (`page_scroll.grid_remove()` + `regex_page`), not an issue panel: it is added to `tab_names` but skipped by the `tab_name not in (...)` issue-panel block. Widget registry keys: `regex_page`, `regex_pattern_tree`, `regex_hit_tree`, `regex_signature`. Refresher `_update_regex_watch_tab(force=False)` is called from the periodic GUI tick and from `_on_dashboard_tab_changed()`; like the Review tab it is gated to the visible tab and rebuilds only when its signature changes, so a tick never clears the selection. It reads the configured patterns from `global_config` and the hits from `app_state.regex_watch_log`. The metric card is `app_state.regex_watch_label` (`_create_ttkbs_metric(metrics_frame, "Regex Watch", "🔎")`)
- **Non-Latin metric card + panels (v11.10.0)** → `app_state.non_latin_label` (`_create_ttkbs_metric(metrics_frame, "Non-Latin", "🔤")`) plus a `"Non-Latin"` entry in BOTH `issue_types` and `issue_keys` (key `non_latin`) and a `color_map` entry, so the Totals and per-API tabs get a `Recent Non-Latin` panel automatically. The panel is fed by `dashboard.update_dashboard()` (`app_state.recent_non_latin_total` / `recent_non_latin_per_api`), and `tests/test_regex_watch_tab_ui.py`-style tab gating does not apply (it is a normal issue panel)
- **Scene cast metric card + panels (v12.0)** → `app_state.scene_continuity_label` (`_create_ttkbs_metric(metrics_frame, "Cast Drift", "🎭")`) plus a `"Cast Drift"` entry in BOTH `issue_types` and `issue_keys` (key `scene_continuity`) and a `color_map` entry, so the Totals and per-API tabs get a `Recent Cast Drift` panel automatically. Fed by `dashboard.update_dashboard()` from `app_state.recent_scene_continuity_total` / `_per_api`, and by the `scene_continuity` series in the graph
- **Metrics tab (v13.1.0)** → a fixed, non-scrolling diagnostics area (like Regex Watch: `page_scroll.grid_remove()` + a `metrics_page` frame), added to `tab_names` and skipped by the `tab_name not in (...)` issue-panel block. Widget registry keys: `metrics_page`, `metrics_tree`, `metrics_graph_canvas`, `metrics_status_label`, `metrics_signature`. Its table uses the shared `REGEX_TREE_STYLE` / `_configure_regex_tree_columns()`, and `draw_vllm_metrics_graph()` builds the two-panel graph. The refresher `_update_metrics_tab(force=False)` is tab-gated and rebuilt only on a signature change, from the GUI tick and `_on_dashboard_tab_changed()`. **One row per API slot, never one aggregate row** — that is the point of the tab. `vllm_metrics.start_polling()` sits next to `init_database_pool()` and `stop_polling()` in the `__main__` `finally:`; **it must never run at import time**, because a test, the CLI and a headless run import or build this module. `reset_all_stats_and_history()` also calls `vllm_metrics.reset_slot_usage()` / `reset_endpoints()`. **The reader itself is documented in [`docs/agents/vllm-metrics.md`](vllm-metrics.md)** — read that before touching it
- **Table row heights (v11.6.1)** → a `ttk.Treeview` with no explicit style inherits the theme's default `Treeview`, whose `rowheight` Tk falls back to a flat 20px for a 10pt font (17px linespace). That 3px of slack clips glyph ascenders/descenders and makes a table read as scrunched, overlapping text — the Review, Queue and Regex Watch tables each hit this. Every table needs its own style with an explicit `font` and a `rowheight` derived from `font.metrics('linespace') + padding`. Regex Watch uses `REGEX_TREE_STYLE` (`'RegexWatch.Treeview'`) plus the module-level `REGEX_TREE_BODY_FONT` / `REGEX_TREE_HEADING_FONT` constants, configured once by `_configure_regex_tree_style()` (called just above the `for tab_name in tab_names:` loop, so it runs before any tab is built) and applied to both trees via `style=REGEX_TREE_STYLE`. Columns are sized by `_configure_regex_tree_columns(tree, columns, widths=..., stretch_columns=...)`, which never lets a column fall below the width its own heading text needs. `tests/test_regex_watch_tab_ui.py` guards all of this
- **Dropped conversations (v11.4)** → `generation._quality_dropped(verdict)` gates both write paths (non-duplication final write, duplication per-turn write). A drop must never write a row, never `completed_task_ids.add()`, and never enter `quality_review_ids`
- Queue logic → Task tuple structure + `enqueue_opening_tasks()`
- **Version label (v12.9)** → the `version_label` in the header frame is ONE of the FOUR sites a release bump must touch: the GUI label here, the `README.md` title, the `docs/changelog-v12.md` heading and the `docs/version-history.md` index row (`(**current: vX.Y**)`). Bumping only the changelog leaves the other three stale — v12.8.3 did exactly that, and v12.8.3's own entry had already missed two of them. **Do not bump it for a code change that ships no release**, and do not retag the historical `(v12.8.2)` markers, which annotate when a feature landed.
- **Async-judge wiring in `start_processing()` (v12.9.1)** → `generation._install_quality_write_handler()` must be called through the **module object**, so `generate.py` carries a plain `import generation` alongside its `from generation import ...` lines. `from generation import worker` binds `worker` and **not** `generation`; v12.9 shipped without the module import and every async-scoring run died on the first Start click with `NameError: name 'generation' is not defined`, inside the Tk callback. The branch is gated on `quality.async_scoring` **and** `quality.use_llm_scoring`, so no default run, import-time check or test reached it. `tests/test_undefined_globals.py` now fails on any global a function references but the module never binds — add it to your mental checklist whenever you introduce a new module-object call (`<module>.<attr>()`) in this file
- DB export → `export_db_to_jsonl()`, `clear_database()`
- Quality tab layout → `_rebuild_quality_roleplay_layout()` builds the summary labels + score-table columns from `quality.active_scoring_dimensions()` (the toggle is read at build time and re-checked each tick, so a change in the Config Editor mid-run re-lays-out the Quality tab automatically; `update_quality_tab()` fills rows in that column order)

---

### `tests/test_regex_watch_tab_ui.py`
**Purpose:** Display-dependent regression test for the `Regex Watch` dashboard tab layout (builds the REAL `generate.py` UI; skips cleanly without a display)
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
**Key:** Rebuilds the `worker()` signature from its AST, extracts the actual `threading.Thread(target=worker, args=..., kwargs=...)` call from `start_processing()`, and binds them via `inspect.Signature.bind()`; then asserts the OOC kwargs (`current_ooc_phrases`/`current_ooc_fixes`/`current_ooc_enabled`) are supplied by keyword from same-named locals (never positionally) — no app config, GUI, or network code is evaluated. Also pins the reported thread total: the `Start processing:` line must name `total_threads`, must NOT name `num_threads`, must be logged after the total is resolved through `run_config.count_worker_threads()`, and the launch loop must be `range(total_threads)`
**Run:** `python -B tests/test_worker_launch.py`
**Edit for:** Extend per-kwarg source assertions when new params are moved into / added to the `kwargs=` dict at the launch site

---

### `tests/test_undefined_globals.py`
**Purpose:** Headless guard against a global name a function USES but the module never BINDS — the v12.9.1 `NameError: name 'generation' is not defined` class of bug
**Key:** walks the `symtable` of every application module and fails on any global that is referenced but never bound at module level (assignment, import, `def`/`class`, `global` declaration or builtin); then pins the async-judge wiring specifically — the `generation._install_quality_write_handler()` call in `start_processing()` resolves to a module-level name bound to the module object, that object really defines the attribute, and `generate.py` imports the module rather than only names from it
**Run:** `python -B tests/test_undefined_globals.py` (headless: the modules are parsed from source, never imported, so it needs no display, config, network or model)
**Edit for:** add a module to `CHECKED_MODULES` when you add a new top-level module; extend the wiring cases when a new module-object call is added to a gated branch

---

## Design tokens and styles (v14.0)

**Every colour, font, spacing value and tab name lives in `ui_theme.py`.** It is pure data plus
pure helpers: importing it must never create a Tk object, so it is safe from a headless test, the CLI
and the layout harness. `grep -c "#[0-9a-fA-F]\{6\}" *.py` must be **0** outside `ui_theme.py`.

| Token group | What it owns |
|-------------|--------------|
| Palette | `SURFACE*`, `BORDER*`, `TEXT*`, `ACCENT` (`#3a63c0`, 5.64:1 behind white — **not** the theme's `primary #6693f5`, which is 2.98:1 and fails AA), `OK`/`WARN`/`DANGER`/`INFO`, `ISSUE_*`, `SYNTAX_*`, `SCROLL_*`, `CHART_*`, `PROGRESS_*` |
| Typography | `UI_FONT_STACK`, `MONO_FONT_STACK`, `EMOJI_FONT_STACK` + `SIZE_BODY`/`SIZE_SMALL`/`SIZE_HEADING`/`SIZE_TITLE`/`SIZE_METRIC` |
| Spacing | `SPACE_XS`/`SPACE_S`/`SPACE_M`/`SPACE_L`, `ROW_SLACK` |
| Theme | `THEME_NAME = 'one-dark'`. **A code constant, never a config key** — a config switch would let a default run differ from the previous release |
| Window | `MINIMUM_WINDOW_SIZE`, `PREFERRED_WINDOW_SIZE` — one owner; `tools/check_gui_layout.py` rebinds its own `SUPPORTED_MINIMUM_AT_96_DPI` from it |
| Tab names | `TAB_OVERVIEW`, `TAB_APIS`, `TAB_QUALITY`, `TAB_REVIEW`, `TAB_REGEX_WATCH`, `TAB_METRICS`, `TAB_PROMPTS`, `TAB_QUEUE` — import these, never write the string. There used to be five hard-coded copies of `"🔍 Review"` |

**Fonts are resolved, never named.** `resolve_font(root, stack, size, weight=None)` walks the stack and
returns the first family Tk actually has, so the same code produces the intended family on Windows, Linux and
macOS instead of silently substituting one. Never write `('Segoe UI', 10)` or `('Consolas', 9)` — a bare family
name is the bug this replaced.

**Table row heights come from `row_height(root, font_tuple, slack=ROW_SLACK)`**, which returns
`font.metrics('linespace') + slack`. ttk leaves `rowheight` empty on every theme, so a `Treeview` with no
explicit style is a flat 20px for a 10pt font whose linespace is 17px — that clips ascenders and descenders.

### The style builders (all in `ui_theme.py`, all called once from `generate.py`)

| Builder | Configures | Notes |
|---------|-----------|-------|
| `configure_global_styles(style, root)` | `.`, `Header.TLabel`, `Small.TLabel`, `Code.TLabel`, `MetricValue.TLabel` | `MetricValue.TLabel` was **referenced but never defined** before v14.0, so the Overview numbers rendered at the same 10pt as their captions |
| `configure_button_styles(style, root)` | `Primary`/`Secondary`/`Ghost`/`Danger.TButton` | **Hand-built with explicit `configure()` + `map()`.** ttkbootstrap's variant styles are inert: `TButton`, `Accent.TButton` and `primary`/`secondary`/`success`/`danger`/`info`/`outline-primary.TButton` all resolve to one identical colour pair on both `superhero` and `one-dark` |
| `configure_interaction_styles(style, root)` | `TButton`, `TCheckbutton`, `TRadiobutton` | v14.0. The 47 bare `ttk.Button`s had **no `focus` entry** and a `focuscolor` of `#ffffff` (white on a near-white face). `TCheckbutton`/`TRadiobutton` had a **completely empty `map()`** and no `indicatorcolor`. `TCombobox`/`TEntry` are left alone — the theme maps focus correctly |
| `configure_scrollbar_styles(style, root)` | `TScrollbar`, `Vertical.TScrollbar`, `Horizontal.TScrollbar` | v14.0. On `one-dark` the thumb was **invisible**: `background == troughcolor == #282c34`. The thumb is painted from **`darkcolor`/`lightcolor`** on the `Vertical.TScrollbar.thumb` element, not from `background`, so a `background`-only fix changes nothing. Verify by screenshot, never by `style.lookup` |
| `configure_table_style(root, style, name, ...)` | one `Treeview` + its `.Heading` | Sets `rowheight=row_height(...)`. Returns the name. Backs `Issue`, `Review`, `Queue`, `RegexWatch` and `Data.Treeview` |
| `configure_notebook_styles(style, root)` | `TNotebook`, `TNotebook.Tab` | Adds a 2px accent underline on the selected tab only. **`font` and `padding` must stay readable through `style.lookup`** — `NotebookNavigator` measures them to decide when the strip collapses |

### Emoji iconography (v14.0)

~130 emoji act as icons. Where no colour-emoji family is installed they render as tofu, which is most
bare Linux boxes. `emoji_font_available(root)` probes `EMOJI_FONT_STACK[:-1]` (the last entry is a plain text
font and always resolves, so it must not count as evidence) and caches the answer **on `root.winfo_toplevel()`** so
the main window and the Config Editor share one answer. `icon(root, emoji)` and `icon_text(root, text)` are the
two entry points; `icon_text()` returns its input **unchanged** when the font is present, so Windows appearance is
untouched. `EMOJI_FALLBACKS` needs **both the VS16 and the bare form** of every sequence (`'⚠️'` *and* `'⚠'`),
because replacement runs on the raw string and a bare entry alone leaves a stray `U+FE0F` behind.
`tests/test_ui_theme.py` scans the sources and fails on any emoji in **code** (comments excluded) with no map entry.

### Empty states (v14.0)

`ui_layout.empty_state(tree, message)` inserts one placeholder row, tagged `EMPTY_STATE_TAG` and drawn in
`TEXT_MUTED`, **only when the table has no rows**. Call it at the end of a populate function, after the real rows
are in. A `Treeview` with no rows is a blank rectangle that reads as a rendering failure. **The trap:** a refresher
that counts `get_children()` to decide "is this table empty" would now count the placeholder — grep for it first.

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Change a colour, font, spacing value or tab name | `ui_theme.py` only. Never write a raw `#rrggbb`, a bare font family or a tab-name string anywhere else |
| Restyle a widget class | `ui_theme.py` — add or extend a `configure_*_styles()` builder, then call it from `generate.py` beside the other four |
| Add an emoji as an icon | `ui_theme.EMOJI_FALLBACKS` (both the VS16 and bare form) + wrap the caption in `icon_text()` / `icon()` |
| Call a module object (`<module>.<attr>()`) from `generate.py` | `generate.py` — add the plain `import <module>`, not only `from <module> import x`; a `from ... import` never binds the module name and the call fails with `NameError` on whichever branch reaches it. `tests/test_undefined_globals.py` guards it |
| Change the Review tab | `generate.py` (`_update_review_table()`, `_show_review_detail()`, `_export_flagged_review()`, `_drop_review_conversation()` + `_drop_task_output_rows()` / `_drop_task_db_rows()`, `_iter_review_records()`, the `elif tab_name == ui_theme.TAB_REVIEW` layout block), `tests/test_review_tab_ui.py` |
| Fix a cramped / clipped Treeview table | `generate.py` — give the table its own style with an explicit `font` + `rowheight` from `font.metrics('linespace')`, and size columns from the heading text. Regex Watch does this via `REGEX_TREE_STYLE`, `_configure_regex_tree_style()`, `_configure_regex_tree_columns()`; guarded by `tests/test_regex_watch_tab_ui.py` |
