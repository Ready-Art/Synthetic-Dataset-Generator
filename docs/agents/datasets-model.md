<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Datasets subsystem — the data model and the pure model layer

> **The port has landed.** `webapp/datasets_routes.py` and
> `tests/test_web_datasets.py` implement this subsystem today, so this is a
> **reference for changing it**, not a work order. Read the section that owns the
> code you are editing; do not re-port a section that already exists.


Covers sections 0-2: the names-only data model, `dataset_settings.py`
(the pure model layer) and `dataset_column_menu.py`.

Part of the [Datasets subsystem reference](datasets-model.md). Sibling pages:
- [Datasets subsystem — templates, bindings and tasks](datasets-bindings.md)

---

## 0. The one thing to get right first: the data model

The whole feature is a **names-only** configuration subtree. No cell values are ever stored
in config. The persisted unit is:

```yaml
# config/config.yml  (also config/profiles/<name>.yml)
datasets:
  sources:
    - source_id: "74f43879af914c67a7e8b8e5c5b945a4"   # uuid4().hex, stable, never regenerated
      name: "Dataset 1"
      enabled: true
      kind: "csv"                # csv|tsv|json|jsonl|parquet|huggingface|hf_disk
      location: "/data/names.csv"
      config: null               # Hub config name; only for kind == "huggingface"
      split: null                # "huggingface" or "hf_disk" only
      revision: null             # "huggingface" only
      columns: ["name", "category"]        # cached, from Import columns
      column_aliases: {"name": "nvd_f_name"}  # column name -> bare alias
      visible_columns: null      # null = All; [] = none; else list in schema order
      schema_selector: {kind: ..., location: ..., config: ..., split: ..., revision: ...}
      schema_fingerprint: "9f2c..."   # opaque sha256 from the reader
```

Verified by running `new_source([])` + `apply_schema(...)`:

```json
{"source_id": "...", "name": "Dataset 1", "enabled": true, "kind": "csv",
 "location": "", "config": null, "split": null, "revision": null,
 "columns": [], "column_aliases": {}, "visible_columns": null,
 "schema_selector": null, "schema_fingerprint": null}
```

**Exactly 13 keys per source.** `normalize_config` drops anything else.

### Persistence points

| What | Where |
|---|---|
| The whole subtree | `config/config.yml` key `datasets` → `datasets.sources` (a list). Default file value is `datasets: {sources: []}` (confirmed in `config/config.yml`). |
| Named profiles | `config/profiles/<safe_name>.yml`, same `datasets` key. `ConfigLoader.profiles_dir` = `os.path.join(os.path.dirname(path), 'profiles')` (`config_loader.py:52`). |
| Tk editor write | `config_editor.py:2601` — `config_data['datasets'] = self.datasets_panel.get_config()` |
| Tk editor read | `config_editor.py:3056` — `self.datasets_panel.load_config(config.get('datasets', {}))` |
| Tk editor validate | `config_editor.py:3569` — `self.datasets_panel.validate()` (first check in `validate_config_handler`) |
| Engine consume (GUI) | `generate_tk.py:2533` — `configure_queue_datasets(app_state.task_queue, global_config.get('datasets', {}))` |
| Engine consume (CLI/control) | `control.py:716` — same call, `cfg.get('datasets', {})` |
| Web schema entry | `webapp/config_schema.py:649-651` — `'datasets'` = `Field('Datasets', ...)`, `'datasets.sources'` = `_list('Sources', 'Dataset sources. Edited from the Datasets panel.')` |
| Web config path | `webapp/config_routes.py:401` `_config_path()` → `app_state.global_config.path` |

The web schema declares `datasets.sources` with `_list(...)` (`webapp/config_schema.py:650`), but that
only supplies the **label and help text**. The actual rendering is decided by `build_node`
(`webapp/config_routes.py:226`), which branches on the *value*: a list whose items are all dicts
becomes `kind: 'group'` with `count` and one child per index. Verified:
`build_node([{'source_id':'a'}], 'datasets.sources', 'sources')` →
`{'kind': 'group', 'count': 1, 'children': [{'path': 'datasets.sources.0', ...}]}`.
So the generic editor already shows each source as a repeated group at
`datasets.sources.<i>.<field>` — usable but flat and unvalidated. A dedicated panel is still the
right call, and `POST /api/config/save` accepts `{"values": {"datasets.sources": [...]}}` because
`coerce()` (`webapp/config_routes.py:329`) for a `list` current value only requires
`isinstance(raw, list)`, and `config_schema.validate` has no field or `RULES` entry for that path
(`RULES` at `webapp/config_schema.py:1082` is only `_rule_append_alias`, `_rule_bad_words`,
`_rule_scene_cast_shape`, `_rule_logic_threshold`). **Consequence: a save through the generic
route does not run `validate_config`, so an invalid datasets subtree can be written to
`config.yml` and will only fail later, at `configure_queue_datasets`.** A datasets panel should
call `validate_config` server-side before writing.

---
## 1. `dataset_settings.py` — the pure model layer (MUST port verbatim)

No GUI, no file I/O, no network I/O. This is the module a FastAPI backend should call directly.

### `class DatasetSettingsError(ValueError)` — `dataset_settings.py:31`
Raised for structurally invalid settings or a stale active mapping. Subclass of `ValueError`,
so `except ValueError` catches it (both `generate_tk.py:2533` and `control.py:716` rely on that).

### `_SELECTOR_DEFAULTS` — `dataset_settings.py:35`
```python
_SELECTOR_DEFAULTS = {"kind": "csv", "location": "", "config": None, "split": None, "revision": None}
```
The canonical selector field set and order. Add a new selector field here, in
`source_selector()`, and in `validate_config()`.

### `source_selector(source: Mapping) -> dict[str, Any]` — `dataset_settings.py:71`
Returns a **new** dict with exactly these keys, in this order:
`{"kind", "location", "config", "split", "revision"}`.

- `source_id` is **excluded** — this is the comparable identity of the source settings.
- Accepts unfinished selectors; does **not** construct a `SourceSpec`.
- `config`/`split`/`revision` are optional: `None` stays `None`; a **whitespace-only**
  string becomes `None`; any other string is preserved **exactly** (no `.strip()`, so paths keep
  trailing spaces). Verified: `{'kind':'csv','location':'  ','config':' '}` →
  `{'kind':'csv','location':'  ','config':None,'split':None,'revision':None}`.
- `kind` and `location` are **not** optional: `None` raises
  `DatasetSettingsError("Source kind must be a string.")` /
  `"Source location must be a string."`.
- Mutates nothing.

### `normalize_config(data: Mapping | None) -> dict[str, list]` — `dataset_settings.py:142`
Returns `{"sources": [source, ...]}`. This is the **only** function that defines the wire shape.

- `None` → `{"sources": []}`. `{}` → `{"sources": []}`.
- Non-mapping → `DatasetSettingsError("Datasets configuration must be an object.")`
- `data["sources"]` not list/tuple → `"Dataset sources must be a list."`
- Per source (`_source`, `dataset_settings.py:100`), output keys in this exact order:
  `source_id, name, enabled, kind, location, config, split, revision, columns,
   column_aliases, visible_columns, schema_selector, schema_fingerprint`
- **Drops** every unrecognized key. Deep-copies; never aliases the input.
- Does **not** validate drafts (no alias syntax, no location checks). Only structure.

Validation inside `normalize_config`:
| Rule | Exact message |
|---|---|
| `source_id` must be a str | `"Source ID must be a string."` |
| `source_id.strip()` non-empty | `"Source ID must be a nonempty string."` |
| duplicate `source_id` | `"Duplicate source ID 'a'."` (repr of the id) |
| `enabled` must be `type(...) is bool` (so `1`/`"yes"` rejected) | `"Source enabled must be true or false."` |
| `name` missing → default `"Dataset {index+1}"`; `name` must be str (empty string allowed) | `"Source name must be a string."` |
| `columns` must be list/tuple of non-empty str, no duplicates | `"Cached columns must be a list of column names."` / `"Cached columns must contain nonempty string names."` / `"Cached columns contains duplicate column 'x'."` |
| `column_aliases` must be a Mapping; keys non-empty str; values str | `"Column aliases must be an object."` / `"Alias mapping columns must be nonempty strings."` / `"An alias for 'a' must be a string."` |
| alias value `""` | **dropped** — a cleared GUI entry is an unmapped column |
| `visible_columns` | see `_visibility` below |
| `schema_selector` | if not `None`, re-normalized through `source_selector()` |
| `schema_fingerprint` | str or `None` |

`_visibility(value, columns)` — `dataset_settings.py:89`:
- `None` → `None` (All)
- otherwise must be a list of unique non-empty names
- if `columns` is non-empty **and** the selection equals the whole set → `None` (All,
  including columns imported later)
- an explicit `[]` on an **empty** schema stays `[]` (verified: `[]`), i.e. "none shown"

### `new_source(existing_sources: Iterable[Mapping]) -> dict[str, Any]` — `dataset_settings.py:167`
Creates one blank source. Returns the full 13-key dict (verified above).

- `existing_sources` being a `str`, `bytes`, or `Mapping` →
  `DatasetSettingsError("Existing sources must be a collection of source objects.")`
  (a `TypeError` from `list()` is re-raised as the same message).
- `source_id = uuid4().hex` (32 lowercase hex chars), retried until unique.
- `name` = the lowest unused `"Dataset {n}"`, starting at `n = 1`.
- Everything else from `_SELECTOR_DEFAULTS` + empty `columns`, `{}` aliases,
  `visible_columns=None`, `schema_selector=None`, `schema_fingerprint=None`.
- **Does not** mutate `existing_sources`.

### `source_spec(source: Mapping) -> SourceSpec` — `dataset_settings.py:186`
Builds a `dataset_sources.SourceSpec(source_id=source.get("source_id"), **source_selector(source))`.
- Does **not** open the source, does not touch the filesystem.
- Re-raises `DatasetSourceError`/`TypeError`/`ValueError` as `DatasetSettingsError`.
- Validation is `SourceSpec.__post_init__` (`dataset_sources.py:64`):
  - `source_id`, `kind`, `location` non-empty strings → `"Source {key} must be a nonempty string."`
  - `kind` in `{csv,tsv,json,jsonl,parquet,huggingface,hf_disk}` → `"Unsupported dataset kind: {kind}."`
  - `config`/`split`/`revision` `None` or non-empty str → `"Source {key} must be a nonempty string or None."`
  - `kind == "huggingface"`: `location` must match
    `[A-Za-z0-9_][A-Za-z0-9_.-]*/[A-Za-z0-9_][A-Za-z0-9_.-]*` →
    `"Use a Hugging Face repository ID such as owner/dataset, not a URL or file path."`
  - non-Hub kinds: `config`/`revision` must be `None` →
    `"Only Hub sources accept config or revision settings."`
  - non-Hub, non-`hf_disk`: `split` must be `None` →
    `"A local file is one table; split applies only to Hub or saved DatasetDict sources."`

### `validate_config(data: Mapping | None) -> None` — `dataset_settings.py:197`
Raises `DatasetSettingsError`; returns `None` on success. **Never** fetches metadata,
never checks file existence, never checks credentials.

Order of checks (first failure wins):
1. `normalize_config(data)` — structural.
2. For each source, **skip entirely** if `not source["enabled"] or not source["column_aliases"]`
   (disabled and alias-free sources may stay unfinished drafts).
3. `label = source["name"] or source["source_id"]`.
4. `source_spec(source)` → `"Source {label!r}: {exc}"`.
5. For each `(column, alias)` in `column_aliases` (dict order):
   - `validate_alias(alias)` → `"Source {label!r}, column {column!r}: {exc}"`
   - alias already used by an earlier enabled source →
     `"Dataset alias {alias!r} is used more than once among enabled sources ({first_label!r} and {label!r})."`
6. Mapped columns absent from `columns` →
   `"Source {label!r} has mapped columns absent from its imported schema: 'a', 'b'."`
7. Staleness — if `schema_selector is None` **or** `schema_fingerprint` is blank **or**
   `source_selector(source) != schema_selector` →
   `"Source {label!r}: import columns again for the current source settings before saving active mappings."`

**Ordering constraint:** aliases are globally unique across *enabled* sources only. Two
disabled sources may reuse an alias. Hidden columns keep their alias active (visibility is
presentation-only).

### `apply_schema(source: Mapping, schema: DatasetSchema) -> dict[str, Any]` — `dataset_settings.py:239`
Returns a **new** 13-key source dict; mutates nothing.

- `schema` must be a `DatasetSchema` → `"Imported schema must be a DatasetSchema."`
- `schema.columns` validated like `columns` above (`"Imported columns ..."` messages).
- `schema.fingerprint` must be a non-blank str → `"Imported schema must include a nonempty fingerprint."`
- If a currently mapped column is missing from the new schema →
  `"The new schema is missing mapped columns: 'a'. Clear their aliases in the current table before importing again."`
- `visible_columns`: filtered to the new schema in schema order, then re-canonicalized
  through `_visibility` (so a full selection collapses to `None`).
- Sets `schema_selector = source_selector(result)` (the *user's* selectors, not the resolved
  ones) and `schema_fingerprint = schema.fingerprint`.
- **Does not** overwrite `kind`/`location`/`config`/`split`/`revision` with resolved values
  from `schema.resolved_spec` — a branch name the user typed stays as typed.

### `visible_column_names(source: Mapping) -> tuple[str, ...]` — `dataset_settings.py:272`
Returns visible names **in schema order**. `visible_columns is None` → all `columns`.
Never changes aliases. Verified: `('a','b')` for `None`, `('b',)` for `['b']`.

### `with_visible_columns(source: Mapping, selection: Iterable[str] | None) -> dict[str, Any]` — `dataset_settings.py:283`
Returns a new source dict with `visible_columns` replaced; aliases untouched.

- `selection is None` → `visible_columns = None` (All, including future columns).
- `selection` being `str`/`bytes`/`Mapping` →
  `"Visible columns must be a collection of column names or None."`
- names not in `columns` → `"Unknown visible columns: 'zzz'."`
- stored in **schema order**, then canonicalized (all current columns → `None`).
- `[]` on a non-empty schema → `[]`; `[]` on an empty schema → `[]`.

---
## 2. `dataset_column_menu.py` — the column-visibility picker

### `class ColumnVisibilityMenu(ttk.Button)` — `dataset_column_menu.py:24`

**Port the selection algebra; do NOT port the widget.**

Pure logic that MUST be ported:

| Member | Line | Behaviour |
|---|---|---|
| `__init__(self, master, on_change, **kwargs)` | 38 | `on_change` must be callable or `None` → `TypeError("on_change must be callable or None.")`. Forces `text="Columns: All ▾"` and `command=self._toggle_popup`; pops caller-supplied `text`/`textvariable`. |
| `set_columns(self, columns, selection=None)` | 62 | Replaces schema + selection **silently** (never calls `on_change`). |
| `get_selection(self)` | 84 | `None` for All, else `tuple` of names **in original schema order**. |
| `toggle_column(self, name)` | 90 | Toggles one name, even if filtered out by search. |
| `select_all(self)` | 101 | `_change_selection(None)` — the whole dataset, ignoring the search query. |
| `clear_selection(self)` | 105 | `_change_selection(set())` — hide all, ignoring the search query. |
| `_canonicalize(self, selected)` | 109 | `selected & column_set`; `None` if `columns` non-empty and the result equals the whole set. |
| `_change_selection(self, selected)` | 115 | Canonicalizes; **returns early if unchanged**; else updates caption, refreshes checks, then calls `on_change(self.get_selection())`. |

`set_columns` validation (exact messages):
- `columns` a `str` → `TypeError("Column names must be a sequence, not one string.")`
- any non-str name → `TypeError("Column names must be strings.")`
- duplicates → `ValueError("Column names must be unique.")`
- `selection` a `str` → `TypeError("Selection must be a sequence of names or None.")`
- non-str in selection → `TypeError("Selected column names must be strings.")`

`toggle_column` unknown name → `ValueError("Unknown column name: 'x'.")`

Caption text (`_update_caption`, line 125): `"Columns: All ▾"` when `None`, else
`"Columns: {len(selected)} of {len(columns)} ▾"`.

Row indicator (`_indicator`, line 297): `"[x]"` / `"[ ]"` — plain ASCII on purpose.

**Tk-only, do NOT port:** `_pixels` (137), `_work_area` (140, Win32 `MonitorFromWindow`),
`_place_popup` (167), `open` (197, `Toplevel` + `grab_set` + `overrideredirect`),
`_populate_tree` (264, `ttk.Treeview` + `tkfont.measure`), `_refresh_checks` (303),
`_all_changed` (311), `_search_changed` (317), `_row_clicked` (320), `_row_space` (330),
`_focus_tree` (336), `_escape` (340), `_popup_clicked` (344), `_inside` (355),
`_focus_out` (362), `_check_focus` (366), `close` (376), `_popup_destroyed` (417),
`_destroyed` (421), `state` (426), `configure`/`config` (432/440), `destroy` (442).

The popup's *behaviour* to reimplement in JS: bounded size (440×380 CSS px at 96 dpi,
margin 8), case-insensitive substring search over names, `All` checkbox with an `alternate`
state when partially selected, status text `"{n} columns"` or `"{n} matches / {total}"`,
click/Space toggles, Escape / click-outside / focus-out closes, and the search field is
focused on open (repeated `open()` calls just refocus it).

---
