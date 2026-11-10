<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# Review tab and its count line

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching the Review tab, its count line, the flagged-id set, or `review_data.py`.

**Related:** [`docs/agents/webapp.md`](webapp.md) (the routes, the tab registry and the theme), [`docs/agents/quality.md`](quality.md) (what produces a flag)

## Contents

- The count is a separate element, never part of the tab label
- The Review tab
- Drop selected
- Dropped conversations
- Review flags persist
- `tests/test_web_review.py`
- Tasks that touch these files

---

## The count is a separate element, never part of the tab label (v14.2.1)

The Review screen shows a live count — `0 flagged`, `5 flagged` — in a `<span class="review-count">`
in its toolbar. **It is its own element, and the tab's label stays the constant string `Review`.**

That separation is the fix for the v14.2.1 bug. When the count was folded into the tab label, the
moment it appeared the screen decided it was no longer the visible one and stopped rebuilding: the
tab read `Review (5)` above an empty list, while Export — which reads `quality_review_ids` directly,
not the rendered list — still wrote all five rows. Nothing was lost; the list simply stopped being
drawn.

The same rule holds in the web UI for a different reason: `webapp/static/js/main.js` keys its
screens by `id` (`review`), and `webapp/static/js/tabs.js`'s `makeTab()` re-fetches on mount. A label
is display text; an identifier is an id. Never derive one from the other.

Guarded by `tests/test_web_review.py::test_the_counts_report_the_two_drop_reasons_apart` (the two
drop reasons stay apart) and `test_the_selection_survives_a_stats_tick` (the screen refreshes on
mount, not on every tick).

---

## The Review tab (v11.4)

A fixed, non-scrolling two-pane split at `1fr / 1.15fr` — the flagged list on the left, the
conversation detail on the right, the detail slightly wider because it holds prose. Both panes carry
`min-height: 0` so each scrolls on its own; without it a long transcript pushes the footer strip off
the screen. The split collapses to one column on a narrow screen.

`webapp/static/js/review.js` holds the screen: `renderReview()` / `unmountReview()` (the tab registry
calls them), `refresh()`, `loadDetail()`, `select()`, `act()`, and the pane builders. The selection and
the loaded detail live at **module scope**, so a stats tick repaints the panes instead of resetting the
reader's place in a conversation. A slower detail response for a row the reader has already left is
dropped (`if (state.selected !== taskId) return`), so it cannot overwrite the newer one.

`review_data.py` is the data layer and is deliberately GUI-free: it reads and writes `app_state` and
returns plain dicts, so it can be exercised with no display and no running server. It is a separate
module rather than a `control.py` verb because `control.py`'s verb list is a contract the CLI depends
on, and adding four browser-only verbs would change that contract and the socket protocol.

Helpers: `record_belongs_to_task()` (id matching, `_api` suffix for duplication rows),
`output_files()` / `iter_records()` (main output + per-API files + database), `find_record()`,
`format_conversation()` (ShareGPT `conversations[]` or `messages[]`), `signature()` / `list_flagged()`
(the list, filtered by task id), `counts()`, `detail()`, `export_flagged()`, `dismiss_all()`, `drop()`.

**The row's key is the FULL task id** — never reintroduce a truncated display id in the row key or in
the detail lookup. Selecting a row only re-renders the detail pane; it never navigates away, and the
detail pane keeps its own scrollbar.

The score breakdown is a **footer strip** pinned below the transcript (Coherence / Character / Prose /
Overall, plus the method), colour-coded by threshold: below 50 is danger, below 70 warn, otherwise ok,
and an unknown value is muted. It only appears when there is a score.

---

## 🗑 Drop selected (v12.8.2)

The only Review action that CHANGES the dataset (Export copies elsewhere, Dismiss All only clears
flags). `review_data.drop(task_id)` acts on the selected task id and nothing else — an empty selection
is a message in the browser, never a dataset-wide delete — behind a `window.confirm()` in `review.js`.
The route does not rely on that confirmation: **drop is a POST with the task id in the path**
(`POST /api/review/{task_id}/drop`), so a link, a prefetch or a stale tab cannot fire it.

Helpers:

- `_drop_task_output_rows(task_id)` rewrites each output JSONL to a `.droptmp` neighbour and
  `os.replace()`s it into place. A corrupt/undecodable line is copied through, never discarded,
  because it cannot be attributed to the task.
- `_drop_task_db_rows(task_id)` deletes
  `task_id = %s OR substring(task_id from 1 for len+4) = '<task_id>_api'` — **deliberately
  no `LIKE`**, since a task id comes from a source file name and an unescaped `%`/`_` pattern
  could match another task.

Both use the `record_belongs_to_task()` rule, so a task whose id is a PREFIX of the dropped one
survives. The whole rewrite runs under **`app_state.output_data_lock`** (shared state, not the per-run
local `start_processing()` used to create — the drop must take the same lock as a worker's append),
then the task is recorded in `quality_dropped` as `quality.DROP_REASON_MANUAL`, added to
`dropped_task_ids`, removed from `quality_scores` and `quality_review_ids`, and
`save_generation_state()` is called. `counts()` reports manual drops separately (`dropped by you`)
from judge drops, because mixing them made the line read as if the judge had rejected a row the user
removed by hand.

`tests/test_web_review.py` guards the scope: prefix-id row survives, duplication rows go, corrupt line
survives, cancel/empty selection drop nothing.

---

## Dropped conversations (v11.4)

`generation._quality_dropped(verdict)` gates both write paths (non-duplication final write,
duplication per-turn write). A drop must never write a row, never `completed_task_ids.add()`,
and never enter `quality_review_ids`.

---

## Review flags persist (v14.2.1)

`quality_review_ids` is saved as a sorted list via `generation._review_ids_snapshot()` (which
takes `quality_review_lock`; iterating the live set from a judge thread raises
`RuntimeError: Set changed size during iteration`) and restored by `control.py`'s `_load_state()`.

A state file written before this release has no such key, so the loader falls back to
`generation._derive_review_ids(scores, dropped)` — below `quality.min_score_threshold` and not
dropped, the same rule the scoring path applies.

Without this a resumed run restored every score but listed nothing on the Review tab, and
Export found nothing to write.

---

### `tests/test_web_review.py`
**Purpose:** Headless regression test for the Review tab: the route table, `review_data.py` and the browser module's source. No display, no server, no real `output/`.
**Key:** the two-pane split is `1fr / 1.15fr` and both panes can shrink so each scrolls on its own; selecting a row only reloads the detail pane and never changes the URL; a repaint replaces the panes rather than the page, so the filter input's focus and the list's scroll survive; the selection survives a stats tick; a slow detail response cannot overwrite a newer selection; the footer strip has the four dimensions plus the method, is colour-coded by threshold, is pinned below the transcript, and only appears when there is a score; **drop is a POST with the task id in the path**, acts on the selection only, asks before destroying data and refuses to guess a target; dismiss and export are separate routes; the export button downloads in the browser and the download route answers an attachment; the counts report the two drop reasons apart; `record_belongs_to_task()` does not widen `abc` into `abcdef`; a non-JSON line is never discarded by a drop; `drop()` records `quality_dropped[task_id] == 'manual'` and clears the score; `dismiss_all()` clears the flags and deletes no data; `export_flagged()` always rewrites the file and reports the ids it could not find
**Run:** `python -B tests/test_web_review.py`
**Edit for:** Add Review-tab behaviour cases; keep it free of source data, generation runs, network and model APIs

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Change the Review tab | `webapp/static/js/review.js` (the panes, the toolbar, the actions), `webapp/review_routes.py` (the six routes), `review_data.py` (`list_flagged()`, `detail()`, `drop()` + `_drop_task_output_rows()` / `_drop_task_db_rows()`, `dismiss_all()`, `export_flagged()`), `tests/test_web_review.py` |
| Change the count line or the flagged list's filter | `review_data.py` (`counts()`, `list_flagged()`), `webapp/static/js/review.js` (`countLine()`), `tests/test_web_review.py` |
| Persist or restore a per-task quality set across a resume | `generation.py` (`_build_generation_state_data()`, `_review_ids_snapshot()`, `_derive_review_ids()`), `control.py` (`_load_state()`), `tests/test_web_review.py` |
