<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Dataset binding

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching `dataset_*.py`, `datasets_panel.py`, `docs/dataset-bindings.md` or `docs/input-folder.md`.

**Related:** [`docs/agents/generate-ui.md`](generate-ui.md), [`docs/agents/config-editor.md`](config-editor.md), [`docs/agents/generation.md`](generation.md)

## Contents

- `dataset_sources.py`
- `dataset_settings.py`
- `dataset_bindings.py`
- `dataset_templates.py`
- `dataset_tasks.py`
- `datasets_panel.py`
- `dataset_column_menu.py`
- `tests/test_dataset_sources.py`
- `tests/test_dataset_settings.py`
- `tests/test_dataset_bindings.py`
- `tests/test_dataset_templates.py`
- `tests/test_dataset_tasks.py`
- `tests/test_dataset_backends.py`
- `tests/test_dataset_column_menu.py`
- `tests/test_datasets_panel.py`
- `tests/test_dataset_pipeline.py`
- `tests/test_dataset_compatibility.py`
- `docs/dataset-bindings.md`
- `docs/input-folder.md`
- Tasks that touch these files

---

### `dataset_sources.py` [22]
**Purpose:** Dataset readers for the optional dataset-binding foundation (CSV/TSV/JSON/JSONL stdlib; Parquet via optional `pyarrow`; Hugging Face Hub / saved-`DatasetDict` via optional `datasets` 4.x). No Tk/app/config imports; importing makes no network requests
**Key:** `SourceSpec`, `DatasetSchema`, `DatasetRow`, `DatasetSource` (ABC), `LocalFileSource`, `HuggingFaceSource`, `SavedDatasetSource`, `open_source()`, `DatasetDependencyError`
**Edit for:**
- Add new source kind → New `DatasetSource` subclass + register in `open_source()` + `dataset_settings._SELECTOR_DEFAULTS` + `datasets_panel._KINDS`
- Schema discovery → `schema()` returns column names + opaque fingerprint, never record values (JSON reads records internally to find keys, discards them)
- Optional deps → Guarded by `_optional_module()` / `_datasets_backend()`; absent dependency → `DatasetDependencyError`

---

### `dataset_settings.py` [23]
**Purpose:** Names-only settings for the `datasets` config subtree (source selectors, cached column names, aliases, visibility). No GUI, file, or network I/O
**Key:** `normalize_config()`, `validate_config()`, `source_selector()`, `source_spec()`, `apply_schema()`, `visible_column_names()`, `with_visible_columns()`, `DatasetSettingsError`
**Edit for:**
- Add new source selector field → `_SELECTOR_DEFAULTS` + `source_selector()` + `validate_config()`
- Readiness rule → An *enabled* source with aliases must be complete; disabled/alias-free sources may stay drafts. Hiding a column is presentation-only, never changes alias mapping

---

### `dataset_bindings.py` [24]
**Purpose:** Freeze dataset values for one logical generation task, independently of workers (no GUI/worker/app-state imports). One `DatasetBindingManager` per run
**Key:** `DatasetBindingManager` (`bind(task_id)`, `snapshot()`), `TaskBinding` (`to_dict()`/`from_dict()` JSON snapshots), `SourceBinding`, `SourceProvenance`, `cell_to_text()`, `DatasetExhaustedError`
**Edit for:**
- Row advancement → Each source advances sequentially once per newly bound task; rows stage until EVERY source succeeds — exhaustion or invalid data never cycles, skips, or partially publishes
- Lock ordering → Allocation lock released before the caller renders prompts or calls model APIs
- Snapshot format → Serializes actual values (foundation for later save/resume; does not restore cursors/pending jobs/conversation progress)

---

### `dataset_templates.py` [25]
**Purpose:** Dataset placeholders (`{{ds:alias}}`) and explicit boundaries for authored-prompt rendering. No app/GUI/network/RNG imports
**Key:** `render_template()` (single-pass), `render_with_native_placeholders()` (preserves the generator's ordered native `str.replace` cascade), `render_question_line()` (v11.9.0: expands `{{ds:alias}}` in one `input/questions.txt` line and appends one alias value after a separator; `dataset_values=None` returns the line unchanged), `DEFAULT_QUESTION_SEPARATOR`, `validate_alias()`, `dataset_placeholders()`, `DatasetTemplateError`
**Edit for:**
- Add placeholder syntax → `_TOKEN_START` regex + `render_template()`; keep single-pass guarantee (inserted values are never re-interpreted)
- Reserved namespace → Whitespace-tainted `{{ ds:... }}` variants raise instead of silently passing to the model; the compatibility renderer uses literal native replacements that can match `{context}` inside `{{context}}`

---

### `dataset_tasks.py` [26]
**Purpose:** Attach optional dataset values to the generator's existing task identities. One attachment service per existing queue; original task tuples/IDs/retry rules untouched; in-memory only
**Key:** `configure_queue_datasets()`, `bind_task_datasets()`, `render_task_inputs()`, `validate_task_templates()`, `render_question_task()` (v11.9.0: expands a questions-file task's line, returning `(task_id, file_name, index, line) + task[4:]` so identity/retries/resume are untouched), `DatasetTaskAttachments`
**Edit for:**
- Queue wiring → `configure_queue_datasets()` called from `generate_tk.py` at processing start; readers opened lazily by the first worker that needs a binding; `_OwnedReader` closes each reader once at queue lifetime end (an old worker finishing its queue is never cut off by a replacement run)
- Task metadata entry → A *reference* to the immutable binding, not a mutable global "current row"

---

### `datasets_panel.py` [27]
**Purpose:** Names-only dataset configuration panel (the Configuration Editor **Datasets** tab), independent of generation workers
**Key:** `DatasetsPanel`, `load_config()` / `get_config()` / `validate()`, `_metadata_worker()` (max 2 daemon threads)
**Edit for:**
- Add source kind UI → `_KINDS` tuple + kind/label maps; Hub sources expose config/split/revision, `hf_disk` exposes its split selector
- Metadata inspection → Runs in background threads; Tk widgets touched only by the UI thread; column import calls the schema interface and never previews cell values

---

### `dataset_column_menu.py` [28]
**Purpose:** Bounded, searchable column-visibility picker for the Datasets panel — one Treeview shows all column names regardless of count
**Key:** `ColumnVisibilityMenu` (`set_columns()`, `on_change(None | tuple_of_names)`) — `None` = all columns (incl. imported later); explicit empty = none
**Edit for:** Selection semantics in the docstring: `set_columns` never fires `on_change`; canonicalization to `None` when every current column is selected; no application-wide event bindings

---

### `tests/test_dataset_sources.py` [29]
**Purpose:** Unit tests for `dataset_sources.py` (local CSV/TSV/JSON/JSONL readers, schema discovery, fingerprints, error paths)
**Edit for:** Add reader/format cases with temporary fixture files

---

### `tests/test_dataset_settings.py` [30]
**Purpose:** Unit tests for `dataset_settings.py` normalization/validation (selectors, aliases, visibility, readiness rules)
**Edit for:** Add setting-shape and validation-failure cases

---

### `tests/test_dataset_bindings.py` [31]
**Purpose:** Unit tests for `dataset_bindings.py` (sequential allocation, repeat-bind idempotency, exhaustion, snapshot round trip, thread safety)
**Edit for:** Add binding-lifecycle cases (use `ThreadPoolExecutor` for concurrency)

---

### `tests/test_dataset_templates.py` [32]
**Purpose:** Unit tests for `dataset_templates.py` rendering (single-pass, native-placeholder compatibility, alias validation, reserved-namespace typos)
**Edit for:** Add render/template edge cases

---

### `tests/test_dataset_tasks.py` [33]
**Purpose:** Unit tests for `dataset_tasks.py` (queue attachment lifecycle, lazy reader open/close, same-run reuse, GC behavior of weak references)
**Edit for:** Add task-attachment lifecycle cases

---

### `tests/test_dataset_backends.py` [34]
**Purpose:** Backend-coverage tests for optional readers (Parquet via `pyarrow`, Hub / saved-`DatasetDict` via `datasets` 4.x) incl. resource lifecycle on rename/delete; never touches the network or a model API
**Edit for:** Extend optional-backend cases; hard-dep backends (csv/json/jsonl) always run, optional ones skip when the package is missing

---

### `tests/test_dataset_column_menu.py` [35]
**Purpose:** Unit tests for `dataset_column_menu.py` (selection semantics, large-column boundedness, grab/focus handling); skips cleanly without a display
**Edit for:** Add picker behavior cases (run on a display for zero skips)

---

### `tests/test_datasets_panel.py` [36]
**Purpose:** Unit tests for `datasets_panel.py` (add/edit/import/alias/save-load flows, background metadata workers); synthetic column names only, optional screenshots via `READYART_GUI_SCREENSHOTS`
**Edit for:** Add panel-flow cases

---

### `tests/test_dataset_pipeline.py` [37]
**Purpose:** Pipeline-level simulation of dataset integration into worker-shaped request stages (question/answer/continuation/cleanup); all model replies are fixtures, expected request bodies assembled independently of the renderer
**Edit for:** Add stage-level pipeline scenarios (scheduler/transport/retry/cache are small simulations, not production impls)

---

### `tests/test_dataset_compatibility.py` [38]
**Purpose:** Bounded equivalence check: the real worker/request builders/fixers/state saver/output writer run twice — baseline with explicitly populated authored inputs vs candidate fed the same values through real CSV/JSONL sources (fake HTTP, no cache hits/waits/display)
**Edit for:** Extend side-by-side scenarios; this proves byte-level compatibility, not exhaustive equivalence

---

### `docs/dataset-bindings.md` [39]
**Purpose:** User/design doc for the Datasets tab (workflow, placeholders, what is/isn't persisted; notes lineage from upstream `5993702`)
**Edit for:** Update when Datasets UX or persistence semantics change

---

### `docs/input-folder.md` [42]
**Purpose:** User guide to the `input/` folder: which files chunk mode picks up, encoding/BOM rules, the `sanitize_input()` character strip, the `len > context_size` minimum-size rule, how a chunk is cut (random character offsets, not sentence boundaries), `questions.txt` line format, dataset variables in a questions-file line (v11.9.0: `{{ds:alias}}` placeholders and the single appended alias, and why chunk-mode `.txt` is deliberately not rendered), original worked examples, a pre-flight check snippet and a troubleshooting table
**Edit for:** Update when the `input/` discovery rule in `generate_tk.py` (`os.listdir` + `.endswith('.txt')` + the `questions.txt` exclusion), `read_book()`/`read_txt()`, `sanitize_input()`, `render_question_line()` / `render_question_task()` or the `prompts.questions_dataset.*` settings, or the `generation.subject_size` / `context_size` / `num_random_chunks` / `sanitize_input_max_length` defaults change

---

## Deeper reference

The data model and the module-by-module detail are split across three pages, so each
stays readable on its own:

| Page | Covers |
|------|--------|
| [`datasets-model.md`](datasets-model.md) | the names-only data model, `dataset_settings.py` (the pure model layer), `dataset_column_menu.py` |
| [`datasets-bindings.md`](datasets-bindings.md) | `dataset_templates.py`, `dataset_bindings.py`, `dataset_tasks.py` |
| [`datasets-panel-reference.md`](datasets-panel-reference.md) | `datasets_panel.py`, the direct answers, the FastAPI surface |

Read the one page that owns the code you are editing. The port has already landed
in `webapp/datasets_routes.py`, so these are a reference for changing it, not a work
order.

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Add dataset source type | `dataset_sources.py` (new `DatasetSource` subclass + register in `open_source()`), `dataset_settings.py` (`_SELECTOR_DEFAULTS`, `source_selector`), `datasets_panel.py` (`_KINDS` list), `tests/test_dataset_sources.py` (or `tests/test_dataset_backends.py` for an optional package), `requirements-datasets.txt` (if it needs a new package) |
| Modify dataset placeholder rendering | `dataset_templates.py` (`render_template()` / `render_with_native_placeholders()` / `validate_alias()`); worker-side call sites in `generation.py` (`render_task_inputs()`) |
| Add/modify dataset variables in `input/questions.txt` (v11.9.0) | `dataset_templates.py` (`render_question_line()`, `DEFAULT_QUESTION_SEPARATOR`), `dataset_tasks.py` (`render_question_task()`), `generation.py` (the `use_questions_file_local` branch + `validate_task_templates(task[3], ...)` in the binding block), `generate_tk.py` (read `prompts.questions_dataset.*` at Start + the two worker kwargs), `config/config.yml` (defaults), `config_editor.py` (Prompts tab panel + module-level escape/value helpers + save/load/validate), `docs/input-folder.md`, `tests/test_dataset_templates.py` / `tests/test_dataset_tasks.py` / `tests/test_questions_dataset_ui.py` / `tests/test_dataset_compatibility.py` — keep chunk-mode source `.txt` unrendered (a chunk is cut at a random offset and could split a placeholder) |
| Change per-task row allocation | `dataset_bindings.py` (`DatasetBindingManager.bind()`), `dataset_tasks.py` (`bind_task_datasets()`) — keep exhaustion/invalid-data rules: no silent cycling, skipping, or partial publish |
