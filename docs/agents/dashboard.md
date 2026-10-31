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
