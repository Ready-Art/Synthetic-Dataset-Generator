<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Dashboard (`dashboard.py`)

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are adding a metric card, issue panel or issue-graph series.

**Related:** [`docs/agents/generate-ui.md`](generate-ui.md), [`docs/agents/core-modules.md`](core-modules.md)

## Contents

- `dashboard.py`

---

### `dashboard.py` [9]
**Purpose:** Dashboard display, issue panels, progress bars, status labels
**Key:** `update_dashboard()`, `update_issue_graph()`, `create_modern_issue_panel()`
**Edit for:**
- New metric label → Add to `app_state.py` + update in `update_dashboard()`
- Issue graph series → `draw_issue_graph()` and `update_issue_graph()` each have parallel `counts` / `colors` / `labels` dicts that must contain the same keys (a missing key = KeyError; an uncounted series = IndexError). `offsets` is computed from `len(counts)`, so adding a series only requires extending the three dicts in BOTH functions. The v11.10.0 `non_latin` series was added this way (plus the `_graph_last_draw` signature key list, which must also list every series or the graph will not redraw when only that series changes); **the v12.0 `scene_continuity` series (label `Cast Drift`, colour `#e599f7`) was added exactly the same way — four dicts plus the signature tuple**
- Issue panel → `create_modern_issue_panel()` + `update_modern_issue_panel()`
- Progress bar style → `configure_animated_progress_styles()`, `get_progress_style_name()`
- **📊 Metrics graph (v13.1.0)** → `draw_vllm_metrics_graph(canvas_widget)` creates the two stacked panels (speeds on top, KV-cache load below) and `update_vllm_metrics_graph(canvas_widget, rows, force=False)` refreshes them. Both take `vllm_metrics.endpoint_rows()` directly, so the graph never reads config or HTTP itself. **One line per endpoint, never a combined line**: each row keeps its own colour from `_VLLM_SERIES_COLORS` (indexed by `slot_idx`), so two slots on one server draw two lines with the same shape. The refresh is throttled on BOTH a 5 s interval and `_vllm_graph_signature(rows)`, because each redraw rasterises through Tk and a 500 ms tick would accumulate X server pixmaps. The empty state draws a hint rather than a blank pair of axes, and `draw_vllm_metrics_graph()` calls the updater once at the end so a freshly built tab is never blank. Its `graph_speed_ax` / `graph_load_ax` attributes are what `tests/test_metrics_tab_ui.py` asserts on
- **Metric formatting (v13.1.0)** → the tab's strings come from `vllm_metrics.format_speed()` / `format_percent()` / `format_count()` / `format_tokens()` / `describe_load()`, not from inline `f"{value:.1f}"` at the call site. Add a new unit there, not in `generate.py`
