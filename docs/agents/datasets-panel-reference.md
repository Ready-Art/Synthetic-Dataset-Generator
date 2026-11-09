<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Datasets subsystem — the Tk panel and the FastAPI surface

> **The port has landed.** `webapp/datasets_routes.py` and
> `tests/test_web_datasets.py` implement this subsystem today, so this is a
> **reference for changing it**, not a work order. Read the section that owns the
> code you are editing; do not re-port a section that already exists.


Covers sections 6-8: `datasets_panel.py` (the Tk panel that was replaced),
the direct answers to the specific questions, and the suggested FastAPI surface.

Part of the [Datasets subsystem reference](datasets-model.md). Sibling pages:

---

## 6. `datasets_panel.py` — the Tk panel (the thing being replaced)

### `_KINDS` — `datasets_panel.py:35`
```python
(("csv", "CSV"), ("tsv", "TSV"), ("json", "JSON array"), ("jsonl", "JSONL"),
 ("parquet", "Parquet"), ("huggingface", "Hugging Face Hub"),
 ("hf_disk", "Saved HF dataset"))
```
`_KIND_LABELS = dict(_KINDS)` (line 44), `_LABEL_KINDS = {label: kind for kind, label in _KINDS}` (line 45).
The combo shows **labels**; the model stores **kinds**. `_kind()` (line 253) maps back with
`_LABEL_KINDS.get(self.kind_var.get(), self.kind_var.get())`.

### `class DatasetsPanel(ttk.Frame)` — `datasets_panel.py:79`

`__init__(self, master, **kwargs)` — `datasets_panel.py:88`. Sets `padding=8` by default and
calls `self.load_config({})` at the end (line 126), so a fresh panel is an empty, valid config.

**Public API (the four methods a FastAPI layer must mirror):**

| Method | Line | Contract |
|---|---|---|
| `load_config(self, data)` | 659 | `normalize_config(data)`; cancels all pending imports; closes the column menu; replaces `self._sources`; selects source 0 (or `None`); re-renders. **No I/O.** |
| `get_config(self)` | 668 | Flushes the visible fields + alias editor into the model, then returns `normalize_config({"sources": self._sources})`. **No I/O.** |
| `validate(self)` | 674 | `validate_config(self.get_config())`. Raises `DatasetSettingsError`. Never fetches metadata. |
| `import_columns(self)` | 592 | The **only** entry point that performs source I/O. Starts a names-only schema read on a background worker. |

Other public-ish members used by tests and the editor:

| Member | Line | Behaviour |
|---|---|---|
| `select_source(self, source_id)` | 450 | `ValueError("Unknown dataset source ID.")` if absent; saves the outgoing source's fields and alias, cancels its pending import, closes the menu, loads the new one. |
| `add_source(self)` | 462 | Appends `new_source(self._sources)`, selects it, focuses + selects the name entry, **returns `source["source_id"]`**. No metadata import, no records read. |
| `remove_source(self)` | 476 | Pops the selected source; selects `min(index, len-1)` or `None`. Mutates only the editor's unsaved list. |
| `apply_alias(self)` | 511 | Commits the alias editor, then validates. Invalid → `_set_status(str(exc), error=True)` and refocuses the entry. Valid → `"Alias saved in this editor."` / `"Alias mapping cleared."` |
| `clear_alias(self)` | 526 | Sets the alias var to `""` then `apply_alias()`. |
| `copy_placeholder(self)` | 531 | Validates, then copies `{{ds:alias}}`. Invalid → `"Choose a valid alias before copying. {exc}"`. Success → `"Placeholder copied."` |

**Public attributes** (all `tk.StringVar`/`BooleanVar`, created at lines 107-117):
`name_var`, `enabled_var` (default `True`), `kind_var` (default `"CSV"`), `location_var`,
`config_var`, `split_var`, `revision_var`, `selected_column_var`, `alias_var`, `count_var`, `status_var`.
Widgets: `source_combo`, `add_button`, `remove_button`, `name_entry`, `enabled_check`,
`kind_combo`, `location_entry`, `browse_button`, `config_entry`, `split_entry`, `revision_entry`,
`import_button`, `column_menu`, `columns_tree`, `selected_column_entry`, `alias_entry`,
`apply_alias_button`, `clear_alias_button`, `copy_button`, `status_label`.

### `_save_source_fields(self)` — `datasets_panel.py:307` (the field→model write)
This is the exact mutation a web UI must reproduce on every field edit:

```python
source.update({
    "name": self.name_var.get(),
    "enabled": bool(self.enabled_var.get()),
    "kind": kind,
    "location": self.location_var.get(),
    "config":   (self.config_var.get()   or None) if kind == "huggingface" else None,
    "split":    (self.split_var.get()    or None) if kind in ("huggingface", "hf_disk") else None,
    "revision": (self.revision_var.get() or None) if kind == "huggingface" else None,
})
```
Then, **if `source_selector(source)` changed**, it cancels the pending import for that source
and resets the status. This is the "source settings changed → import again" rule.

### `_commit_alias_editor(self)` — `datasets_panel.py:332`
```python
if alias: source["column_aliases"][self._alias_column] = alias
else:     source["column_aliases"].pop(self._alias_column, None)
```
Empty string **removes** the mapping (matching `normalize_config`'s `""`-dropping rule).

### `_placeholder(alias)` — `datasets_panel.py:348` (staticmethod)
- `""` → `""`
- invalid alias → `"Invalid alias"`
- else → `"{{ds:" + alias + "}}"`

### `_update_count(self)` — `datasets_panel.py:367`
`"{} shown of {} · {} mapped".format(len(visible_column_names(source)), len(source["columns"]), len(source["column_aliases"]))`
(the separator is U+00B7 MIDDLE DOT surrounded by spaces). Empty when no source.

### Status strings (`_show_source_status`, `datasets_panel.py:428`) — reproduce verbatim
| Condition | Message |
|---|---|
| no source | `"Add a dataset, import its column names, then choose aliases for the columns you need."` |
| import in flight | `"Reading column names. Dataset records are not displayed."` |
| `schema_selector` set and != `source_selector(source)` | `"Source settings changed. Import columns again before using these mappings."` |
| has columns | `"Select a column to edit its alias. Hiding a column keeps its alias mapping."` |
| `schema_fingerprint` set, no columns | `"This source has no columns."` |
| kind == `huggingface` | `"Enter owner/dataset as the location. Set a config or split when the source has multiple choices."` |
| otherwise | `"Import column names to create mappings. Dataset records are not displayed."` |

`_set_status` (line 256) collapses `\r`/`\n` to spaces and truncates to 260 chars
(`message[:257] + "..."`). Errors use `ui_theme.DANGER_DEEP` as the foreground.

### `import_columns(self)` — `datasets_panel.py:592` (the async contract)
1. `_save_source_fields()`, `_commit_alias_editor()`.
2. Skip if this source already has an in-flight request.
3. `spec = source_spec(source)`, `selector = source_selector(source)`; a `ValueError` is
   shown as the status and aborts.
4. Increment `self._serial` → `token`; build `(token, source_id, selector, spec, cancelled)`.
5. `self._request_queue.put_nowait(...)` — the queue is `Queue(maxsize=2)` (line 101). `Full` →
   `"Earlier imports are finishing. Try importing again shortly."`
6. Record `self._requests[source_id] = (token, cancelled)`; start at most **2** daemon threads
   (line 566); poll every 80 ms (lines 621 and 657).

Result acceptance (`_poll_results`, `datasets_panel.py:623`) — **this is the ordering rule to port**:
- Drop the result unless `request[0] == token` (same request) and the source still exists.
- Drop unless `source_selector(source) == selector` **and** `source_id == self._selected_source_id`.
  (Test `test_out_of_order_import_result_cannot_replace_changed_source` covers this.)
- On success `apply_schema(source, schema)`; a `ValueError` becomes `"Import failed: {exc}"`.
- Success status: `"Imported {n} column names. Select a column to assign an alias."`

Worker (`_metadata_worker`, `datasets_panel.py:48`): reads `(token, source_id, selector, spec, cancelled)`,
calls `open_source(spec).schema()`, always `close()`s the reader, and puts
`(token, source_id, selector, schema, error)` where `error = str(exc) or type(exc).__name__`.
Close failure becomes `"Could not close the dataset reader: {}"` only when there was no prior error.

### `_browse(self)` — `datasets_panel.py:550` (file picker behaviour to reproduce)
- `huggingface` → no picker at all.
- `hf_disk` → directory chooser, title `"Choose a saved Hugging Face dataset"`.
- else file chooser, title `"Choose a dataset file"`, patterns:
  `csv: *.csv`, `tsv: *.tsv`, `json: *.json`, `jsonl: *.jsonl *.ndjson`, `parquet: *.parquet`,
  plus an `All files` filter.

### Kind-dependent field enablement (`_update_kind_controls`, `datasets_panel.py:284`)
| kind | config | split | revision | browse |
|---|---|---|---|---|
| `huggingface` | enabled | enabled | enabled | **disabled** |
| `hf_disk` | disabled | enabled | disabled | enabled (directory) |
| local file kinds | disabled | disabled | disabled | enabled (file) |

The selector row is hidden entirely for local kinds; `split` expands across the row
(`columnspan=5`) when the source is not a Hub source.

### Tk-only, do NOT port
`_build_widgets` (128) — every `ttk.*` construction, `grid`, `columnconfigure`;
`_on_resize` (263); `_refresh_source_choices` (267, `Combobox` labels
`"{index+1}. {name}"`, name sanitized `\r`/`\n`→space and truncated to 65 chars as
`name[:62] + "..."`); `_render_columns` (376, `Treeview` iids `"column-{index}"`);
`_load_selected` (402, `StringVar.set` + widget `state`); `_source_selected` (445);
`_column_selected` (488); `_focus_alias` (497); `_apply_alias_event` (503);
`_alias_focus_out` (507); `_start_workers` (566); `_cancel_request` (576);
`_cancel_all` (581); `_update_import_button` (586); `_on_destroy` (678).

The *semantics* of `_cancel_request`/`_cancel_all` DO need porting: cancelling sets the
per-request `threading.Event` so a stale result is discarded.

---
## 7. Direct answers to the specific questions

### 7.1 How does a dataset "source" get added, edited, removed, and what fields does it have?

**Add** — `DatasetsPanel.add_source()` (`datasets_panel.py:462`) → `new_source(self._sources)`
(`dataset_settings.py:167`). Appends a draft with a fresh `uuid4().hex` `source_id` and the lowest
unused `"Dataset {n}"` name. Returns the new `source_id`. **No metadata import, no records read.**
A web equivalent is `POST /api/datasets/sources` returning the new source object.

**Edit** — there is no `update_source` function. The panel mutates the source dict in place:
`_save_source_fields()` (`datasets_panel.py:307`) writes exactly `name`, `enabled`, `kind`,
`location`, `config`, `split`, `revision` — with the kind gating shown above. Aliases are written by
`_commit_alias_editor()` (`datasets_panel.py:332`). Visibility is written by
`with_visible_columns()` (`dataset_settings.py:283`). Columns/fingerprint are written only by
`apply_schema()` (`dataset_settings.py:239`).

**Remove** — `DatasetsPanel.remove_source()` (`datasets_panel.py:476`): `self._sources.pop(index)`,
then select `min(index, len-1)` or `None`. Only the editor's unsaved list changes; the file changes
when the whole config is saved.

**Fields of a source** (13, exact keys, order from `_source`, `dataset_settings.py:100`):

| Key | Type | Notes |
|---|---|---|
| `source_id` | `str` | `uuid4().hex`; required, unique, never regenerated on round-trip |
| `name` | `str` | default `"Dataset {index+1}"` **only when the key is absent**; `""` is allowed and preserved; `None` raises `"Source name must be a string."` |
| `enabled` | `bool` | strict `type(...) is bool`; default `True` |
| `kind` | `str` | one of the 7 kinds; default `"csv"` |
| `location` | `str` | default `""`; **not** stripped; Hub needs `owner/dataset` |
| `config` | `str \| None` | Hub only; whitespace-only → `None` in `source_selector` |
| `split` | `str \| None` | Hub and `hf_disk` only |
| `revision` | `str \| None` | Hub only |
| `columns` | `list[str]` | cached; unique non-empty names |
| `column_aliases` | `dict[str, str]` | column → bare alias; `""` values dropped |
| `visible_columns` | `list[str] \| None` | `None` = All; `[]` = none |
| `schema_selector` | `dict \| None` | the 5 selector keys as of the last import |
| `schema_fingerprint` | `str \| None` | opaque sha256 from the reader |

### 7.2 How does column mapping / `dataset_column_menu` work — what does the user pick and what does it write?

Two separate things share the word "column":

**(a) Visibility** — `ColumnVisibilityMenu` (`dataset_column_menu.py:24`). The user picks **which
column names are shown in the mapping table**. It writes `source["visible_columns"]` via
`with_visible_columns()` (`datasets_panel.py:543` → `dataset_settings.py:283`). It **never** touches
aliases. `None` = All (including columns imported later); `[]` = none; a full selection is
canonicalized back to `None`.

**(b) Alias mapping** — the selected-column editor in the panel. The user picks **one column name**
and types **one alias** (a bare identifier, no `ds:`, no braces). `_commit_alias_editor()`
(`datasets_panel.py:332`) writes `source["column_aliases"][column] = alias`, or pops the key when the
alias is empty. `apply_alias()` (`datasets_panel.py:511`) then validates with `validate_alias` and reports
`"Alias saved in this editor."` / `"Alias mapping cleared."` / the validation error.
`copy_placeholder()` (`datasets_panel.py:531`) puts `{{ds:alias}}` on the clipboard.

So: visibility = presentation only; alias = the actual mapping that reaches the prompt.

### 7.3 What are "bindings" (`dataset_bindings.py`) — what binds to what, and what is the data shape?

A **binding** freezes, per logical generation task, one row from **each** enabled+mapped source, and
exposes the row's mapped cells as `alias -> str`.

- `SourceBinding` binds a **reader** (`DatasetSource`) to its `column_aliases` mapping.
- `SourceProvenance` binds a **frozen `SourceSpec`** + `fingerprint` + `row_id` + `column_aliases` —
  the audit record, without unused cells.
- `TaskBinding` binds `run_id` + `task_id` + `values` (alias→str) + `sources` (tuple of provenance).
- `DatasetBindingManager` owns the cursors; `bind(task_id)` advances each source **once per newly bound
  task**, sequentially, and commits only when **every** source succeeded.

Data shape (`TaskBinding.to_dict()`, verified):
```json
{"version": 1, "run_id": "...", "task_id": "...",
 "values": {"alias": "text"},
 "sources": [{"spec": {"source_id","kind","location","config","split","revision"},
               "fingerprint": "...", "row_id": "0",
               "column_aliases": {"column": "alias"}}]}
```
`values` keys are exactly the union of every source's alias values (enforced in `__post_init__`).

### 7.4 What are "templates" (`dataset_templates.py`)?

The placeholder language and its renderers — **not** a stored template library. There is no
`templates` config key. Specifically:
- `{{ds:alias}}` syntax + `validate_alias` (bare identifier rules).
- `render_template` — single-pass, for authored prompts.
- `render_question_line` — the `input/questions.txt` boundary, with one optional appended alias.
- `render_with_native_placeholders` — preserves the generator's ordered native `str.replace`
  cascade while protecting dataset placeholders.
- `DEFAULT_QUESTION_SEPARATOR = "\n\n"`.

The only config keys involved are `prompts.questions_dataset.append_alias` and
`prompts.questions_dataset.append_separator` (`webapp/config_schema.py:405-416`;
written at `config_editor.py:2422-2425`; read at `config_editor.py:3241-3244`;
validated at `config_editor.py:3657` and `webapp/config_schema.py:1036`).
`append_separator` uses the `escape` codec (`webapp/config_schema.py:844-855`): stored as real
characters, shown as `\n`/`\t`/`\r`/`\\`; only those four escapes are decoded.

### 7.5 What are "tasks" (`dataset_tasks.py`)?

The bridge between the config subtree and the existing task queue. It does **not** define a task type.
It attaches an immutable `TaskBinding` to the generator's **existing** task IDs.
- `configure_queue_datasets(queue, settings)` — called once per run, from `generate_tk.py:2533` and
  `control.py:716`. Returns `None` when nothing is enabled+mapped.
- `bind_task_datasets(queue, task, task_metadata, metadata_lock)` — called per task
  (`generation.py:2063`); returns `None` or the `TaskBinding`.
- `render_task_inputs` / `validate_task_templates` / `render_question_task` — used at
  `generation.py:2074-2090` and `generation.py:2643`.
- `DatasetTaskAttachments` — the per-queue service holding the lazy manager.

**Nothing is persisted.** Attachments are in-memory only, for the queue's lifetime.

### 7.6 What is in `dataset_settings.py`?

The whole names-only model layer, and nothing else: `DatasetSettingsError`, `_SELECTOR_DEFAULTS`,
`source_selector`, `normalize_config`, `new_source`, `source_spec`, `validate_config`,
`apply_schema`, `visible_column_names`, `with_visible_columns`. No GUI, no file I/O, no network I/O.
It is the module a FastAPI backend should import directly.

---
## 8. Suggested FastAPI surface (derived from the above)

| Route | Backed by | Notes |
|---|---|---|
| `GET /api/datasets` | `normalize_config(load(config)["datasets"])` | returns `{"sources": [...]}` |
| `POST /api/datasets/sources` | `new_source(existing)` | returns the new source; no I/O |
| `PATCH /api/datasets/sources/{source_id}` | mutate the 7 field keys, then `normalize_config` | must re-check `source_selector` to invalidate `schema_selector` |
| `DELETE /api/datasets/sources/{source_id}` | pop, reselect `min(index, len-1)` | |
| `PUT /api/datasets/sources/{id}/aliases/{column}` | `validate_alias` + set/pop | `""` deletes |
| `PUT /api/datasets/sources/{id}/visibility` | `with_visible_columns(source, selection)` | `null` = All |
| `POST /api/datasets/sources/{id}/import` | `source_spec` → `open_source(spec).schema()` → `apply_schema` | the only route that does I/O; must return the `selector` it used so the client can drop stale results |
| `POST /api/datasets/validate` | `validate_config` | 422 with the exact `DatasetSettingsError` message |
| `POST /api/config/save` with `{"values": {"datasets.sources": [...]}}` | existing generic save | `coerce` accepts a list; no schema validator on that path |

Two invariants a web UI must not break:
1. **Never regenerate `source_id`** on a round trip (`normalize_config` docstring, line 142).
2. **A field edit that changes `source_selector` must invalidate the import** — otherwise
   `validate_config` rejects the save with the "import columns again" message.
