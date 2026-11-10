<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# Datasets subsystem — templates, bindings and tasks

> **The port has landed.** `webapp/datasets_routes.py` and
> `tests/test_web_datasets.py` implement this subsystem today, so this is a
> **reference for changing it**, not a work order. Read the section that owns the
> code you are editing; do not re-port a section that already exists.


Covers sections 3-5: `dataset_templates.py` (aliases and placeholder
rendering), `dataset_bindings.py` (freezing one row per task) and
`dataset_tasks.py` (attaching bindings to existing tasks).

Part of the [Datasets subsystem reference](datasets-model.md). Sibling page:
- [Datasets subsystem — the data model and the pure model layer](datasets-model.md)

---

## 3. `dataset_templates.py` — aliases and placeholder rendering

### `class DatasetTemplateError(ValueError)` — `dataset_templates.py:35`

### `DEFAULT_QUESTION_SEPARATOR = "\n\n"` — `dataset_templates.py:52`

### `validate_alias(alias: str) -> str` — `dataset_templates.py:55`
Returns `alias` unchanged or raises. Regex `_ALIAS = [A-Za-z_][A-Za-z0-9_]*\Z` (line 39) —
case-sensitive ASCII identifier, no `ds:`, no braces. Exact message:

> `Dataset aliases must start with a letter or underscore and contain only letters, digits, or underscores; enter the alias without 'ds:' or braces.`

### `dataset_placeholders(template: str) -> tuple[str, ...]` — `dataset_templates.py:102`
Unique `{{ds:alias}}` aliases in template order, deduplicated, no data read.
Verified: `'a {{ds:x}} b {{ds:y}} {{ds:x}} {context}'` → `('x', 'y')`.
Malformed reserved placeholders raise (same as rendering). Non-str → `TypeError("A prompt template must be a string.")`

### `render_template(template: str, dataset_values: Mapping[str, str], native_values: Mapping[str, str] | None = None) -> str` — `dataset_templates.py:118`
Single-pass substitution. Neither inserted dataset values nor inserted native values are re-scanned.
- Every `{{ds:alias}}` must be in `dataset_values` →
  `"No dataset column is mapped to alias 'x' ({{ds:x}})."`
- Native `{name}` replaced only if `name in native_values`; unknown native placeholders and
  unrelated braces (incl. `{{context}}`, JSON) are left alone.
- Values must already be `str` (empty string is valid) →
  `"The value for {kind} placeholder 'x' must already be a string."`
- Verified: `render_template('N {{ds:x}} {context}', {'x':'A {context}'}, {'context':'C'})` → `'N A {context} C'`

Token grammar (`_TOKEN_START`, line 46):
```
(?P<dataset>\{\{\s*ds\s*:) | (?P<native>(?<!\{)\{[A-Za-z_][A-Za-z0-9_]*\}(?!\}))
```
`_DATASET_PLACEHOLDER` (line 40) is `\{\{ds:([A-Za-z_][A-Za-z0-9_]*)\}\}\Z` — **no spaces allowed**
inside a real placeholder, but whitespace after `{{`/`ds` is *detected* so a typo raises instead of
reaching the model:
- unclosed → `"Unclosed dataset placeholder at character {n}. Use {{ds:alias}}."`
- malformed → `"Malformed dataset placeholder at character {n}. Use {{ds:alias}} without spaces, with a valid alias."`

### `render_question_line(line: str, dataset_values: Mapping[str, str] | None, append_alias: str | None = None, separator: str = DEFAULT_QUESTION_SEPARATOR) -> str` — `dataset_templates.py:171`
This is the `input/questions.txt` boundary.
- `dataset_values is None` → returns `line` unchanged and **never appends** (dataset-free runs stay
  byte-identical).
- `append_alias` `None` or `""` → returns the rendered line.
- `validate_alias(append_alias)` first.
- `dataset_values is None` but `append_alias` set →
  `"Cannot append dataset alias 'x' to a question line: this run has no dataset attachments."`
- alias not in `dataset_values` →
  `"No dataset column is mapped to alias 'x' ({{ds:x}}), so it cannot be appended to a question line."`
- `separator` not a str → `"The question append separator must be a string."`
- appended value not a str → `"The value for appended dataset alias 'x' must already be a string."`
- empty appended value → returns the rendered line (no dangling separator)
- empty rendered line → returns the appended value (no leading separator blunt)
- else `rendered + separator + appended`
- non-str `line` → `TypeError("A question line must be a string.")`

### `render_with_native_placeholders(template: str, dataset_values: Mapping[str, str], native_replacements: Iterable[tuple[str, str]]) -> str` — `dataset_templates.py:229`
Preserves the generator's **ordered `str.replace` cascade**. `native_replacements` is an ordered
iterable of `(literal_token, value)`.
- token not a non-empty str → `"Native replacement tokens must be nonempty strings."`
- value not a str → `"Native replacement values must already be strings."`
- Only dataset placeholders present in the **original** template are resolved; they are protected with
  one-character private-use markers (U+E000–U+F8FF, then planes 15/16) chosen to be absent from every
  input, then all native `str.replace` operations run in order, then a single `str.translate` inserts the
  dataset values. So a dataset value containing `{context}` or `{{ds:other}}` stays literal.
- No dataset placeholders → plain ordered `str.replace` loop.
- Cannot reserve a marker → `"Cannot reserve an unambiguous dataset placeholder marker."`
- Do **not** run further native substitutions over the result.

---
## 4. `dataset_bindings.py` — freezing one row per task

No GUI, no worker, no app-state imports. One `DatasetBindingManager` per run.

### `class DatasetBindingError(ValueError)` — `dataset_bindings.py:39`
### `class DatasetExhaustedError(DatasetBindingError)` — `dataset_bindings.py:43`
Raised when a source has no next row; **no binding is committed** for that task.

### `cell_to_text(value: Any) -> str` — `dataset_bindings.py:119`
- `str` → returned as-is.
- `None` → `""`.
- anything else → compact JSON: `json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)`.
- Verified: `[1, True, None]` → `"[1,true,null]"`; `{'b':1,'a':2}` → `'{"a":2,"b":1}'`; `3.5` → `'3.5'`.
- Rejected: non-finite floats, non-`str` object keys, circular structures, Python-only types.
  Messages: `"Unsupported cell type {name}."`, `"Dataset object keys must be strings."`,
  `"A dataset cell contains a circular structure."`,
  `"Dataset cell cannot be encoded as JSON: Out of range float values are not JSON compliant: nan"`.

### `@dataclass(frozen=True) class SourceBinding` — `dataset_bindings.py:141`
Fields: `source: DatasetSource`, `column_aliases: Mapping`.
`__post_init__` (147) copies aliases into a `MappingProxyType`.
`_copy_aliases` (53) rules: must be a non-empty Mapping →
`"Each source must have at least one column-to-alias mapping."`; column keys must be `str` →
`"Column names must be strings."`; alias validated →
`"Invalid alias for column 'a': {exc}"`; duplicates →
`"Duplicate dataset alias 'x'."`

### `@dataclass(frozen=True) class SourceProvenance` — `dataset_bindings.py:152`
Fields: `spec: SourceSpec`, `fingerprint: str`, `row_id: str`, `column_aliases: Mapping`.
`__post_init__` (160) copies the spec, requires non-empty `fingerprint`/`row_id`.

`to_dict(self) -> dict[str, Any]` — `dataset_bindings.py:166`:
```json
{"spec": {"source_id": "...", "kind": "...", "location": "...",
          "config": null, "split": null, "revision": null},
 "fingerprint": "...", "row_id": "0", "column_aliases": {"a": "x"}}
```
`from_dict(cls, data: Mapping) -> "SourceProvenance"` — `dataset_bindings.py:173`:
`cls(SourceSpec(**dict(data["spec"])), data["fingerprint"], data["row_id"], data["column_aliases"])`.
Errors: `"Persisted source provenance must be an object."`,
`"Persisted source specification must be an object."`,
`"Invalid persisted source provenance: {exc}"`.

### `@dataclass(frozen=True) class TaskBinding` — `dataset_bindings.py:189`
Fields: `run_id: str`, `task_id: str`, `values: Mapping`, `sources: tuple[SourceProvenance, ...]`.
`values` is `alias -> str` (a `MappingProxyType` after construction).

`__post_init__` (197) invariants, in order:
1. `run_id`, `task_id` non-empty strings.
2. `values` must be a Mapping → `"Bound values must be an alias-to-string mapping."`
3. each alias valid → `"Invalid bound alias: {exc}"`; each value a `str` →
   `"Bound value for 'x' must be a string."`
4. each source a `SourceProvenance` → `"Task sources must contain source provenance snapshots."`
5. duplicate `spec.source_id` → `"Duplicate source ID 's1'."`
6. duplicate alias across sources → `"Duplicate dataset alias 'x'."`
7. `set(values) != expected_aliases` →
   `"Bound aliases must exactly match the recorded source mappings."`

`to_dict(self) -> dict[str, Any]` — `dataset_bindings.py:228` (verified output):
```json
{"version": 1, "run_id": "run1", "task_id": "t1",
 "values": {"x": "v"},
 "sources": [{"spec": {...}, "fingerprint": "fp", "row_id": "0",
              "column_aliases": {"a": "x"}}]}
```
`from_dict(cls, data: Mapping) -> "TaskBinding"` — `dataset_bindings.py:237`:
- `type(data.get("version")) is not int or data["version"] != 1` →
  `"Unsupported dataset binding snapshot version."`
- `data["sources"]` not a list → `"Persisted sources must be a list."`
- other failures → `"Invalid persisted task binding: {exc}"`
- never reopens or samples a dataset.

### `class DatasetBindingManager` — `dataset_bindings.py:322`

`__init__(self, run_id: str, sources: Iterable[SourceBinding])` — `dataset_bindings.py:334`
- Validates every supplied source, calls `source.schema()` on each (this **does** perform I/O:
  it opens readers / memory-maps parquet).
- Uses `schema.resolved_spec` when present, requiring the same `source_id` →
  `"Resolved source settings must retain the configured source ID."`
- Requires distinct string column names → `"Dataset schema must contain distinct string column names."`
- Mapped columns must exist → `"Source 's1' has no mapped column(s): 'a'."`
- On any `BaseException`, closes every reader already inspected, then re-raises.
- Zero sources is valid and yields empty task bindings.
- `self.run_id` is a public attribute.

`bind(self, task_id: str) -> TaskBinding` — `dataset_bindings.py:390`
- Idempotent per `task_id`: a repeat returns the **same object**.
- Under `self._lock`, peeks every cursor, builds `values` by `update()` in source order, builds
  provenance, constructs `TaskBinding`, then commits every cursor. **All-or-nothing**: an exhausted or
  invalid source leaves every cursor uncommitted (rows are staged in `_SourceCursor.pending`).
- After `close()` → `"Dataset binding manager is closed; no new tasks can be bound."`
- `_text_identifier` message: `"Task ID must be a non-empty string."`

`get(self, task_id: str) -> TaskBinding | None` — `dataset_bindings.py:412` — no row selection, no new task.
`snapshot(self) -> tuple[TaskBinding, ...]` — `dataset_bindings.py:418` — committed bindings in allocation order.
`close(self) -> None` — `dataset_bindings.py:423` — releases iterators **and** readers; retains committed
bindings; idempotent; raises `"Could not close dataset reader(s): ..."` if any close failed.
`__enter__`/`__exit__` — `dataset_bindings.py:449`/`452`.

Row-level errors (`_SourceCursor.peek`, `dataset_bindings.py:264`):
- exhaustion → `DatasetExhaustedError("Dataset source 's1' is exhausted; no row was assigned to this task.")`
- missing mapped column →
  `"Source 's1', row '0' is missing mapped column 'a'."`
- unencodable cell → `"Source 's1', row '0', column 'a': {exc}"`
- read failure → `"Could not read source 's1': {exc}"`
- **A failed iterator is frozen**: retries never silently skip a malformed row.

---
## 5. `dataset_tasks.py` — attaching bindings to existing tasks

In-memory only. Nothing here is persisted; attachments do not survive a restart.

### `class DatasetTaskAttachments` — `dataset_tasks.py:77`

`__init__(self, settings, source_factory=open_source)` — `dataset_tasks.py:86`
- `normalize_config(settings)` then `validate_config(normalized)` — so an invalid `datasets` subtree
  raises at construction (this is what `control.py:749` catches as `ValueError` when it configures the
  queue at run start).
- Keeps only sources where `source["enabled"] and source["column_aliases"]`, as
  `(source_spec(source), MappingProxyType(dict(source["column_aliases"])))`.
- `self.run_id = uuid4().hex`.
- No source I/O at construction.

`active` (property) — `dataset_tasks.py:101` — `bool(self._sources)`.
`bind(self, task_id)` — `dataset_tasks.py:122` — returns the same `TaskBinding` for a repeated id;
lazily creates the manager on first use; caches the first failure so retries raise the same error.
- closed → `"Dataset task attachments are closed; no new rows can be assigned."`
- other errors → `"Could not attach dataset values: {exc}"`
`get(self, task_id)` — `dataset_tasks.py:147` — no source opened, no row selected.
`close(self)` — `dataset_tasks.py:153` — releases readers, keeps assigned values.

### `configure_queue_datasets(queue, settings, source_factory=open_source)` — `dataset_tasks.py:163`
Returns the service, or **`None`** when no source is enabled+mapped.
- If `getattr(queue, "dataset_attachments", None) is not None` →
  `"This queue already has dataset attachments; configure a new queue for a new run."`
- Registers `weakref.finalize(queue, service.close)` and sets `queue.dataset_attachments = service`.
- Configuration only: does not iterate or replace queued tasks.

### `bind_task_datasets(queue, task, task_metadata=None, metadata_lock=None)` — `dataset_tasks.py:182`
Returns the `TaskBinding` or `None` (queue has no attachments).
- `task` must be a `tuple` of length 4 or 5 →
  `"Dataset attachment needs an original four- or five-field task tuple."`
- Binds `task[0]` (the task id).
- Metadata entry is updated **only** while `entry.get("task_data") is task` (identity check), setting
  `entry["dataset_binding"] = binding`. This is a reference to the immutable binding, not a
  mutable "current row".

### `render_task_inputs(inputs, values)` — `dataset_tasks.py:211`
Returns **fresh** containers; mutates neither `inputs` nor `values`.
`str` → `render_template(inputs, values)`; `Mapping` → dict of the same keys;
`tuple` → tuple; `list` → list; anything else returned unchanged.

### `validate_task_templates(templates, values)` — `dataset_tasks.py:229`
Walks `str`/`Mapping`/`tuple`/`list` and raises
`"No dataset column is mapped to alias 'x' ({{ds:x}})."` for any unmapped alias. Does not expand.

### `render_question_task(task, values, append_alias="", separator=DEFAULT_QUESTION_SEPARATOR)` — `dataset_tasks.py:243`
- `task` must be a `tuple` of length 4 or 5 →
  `"A questions-file task must be a four- or five-field tuple; got 'tuple'."`
- `task[3]` must be a `str` → `"The question line of task 'id' must be a string."`
- Returns `(task[0], task[1], task[2], line) + tuple(task[4:])` — identity, file name and index
  are untouched so task IDs, retries and recovery keep working.

---
