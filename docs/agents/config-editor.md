<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Configuration Editor (`config_editor.py`)

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are adding a config field to the editor, or fixing editor layout/readability.

**Related:** [`docs/agents/config.md`](config.md), [`docs/agents/generate-ui.md`](generate-ui.md)

## Contents

- `config_editor.py`
- `tests/test_user_emotional_states_ui.py`
- `tests/test_regex_watch_ui.py`
- `tests/test_editor_readability_ui.py`
- `tests/test_missing_quotations_ui.py`
- `tests/test_repeat_opener_ui.py`
- `tests/test_repeat_reply_ui.py`
- `tests/test_questions_dataset_ui.py`
- Tasks that touch these files

---

### `config_editor.py` [7]
**Purpose:** Configuration Editor UI window
**Key:** `ConfigEditor` class, `save_config_handler()`, `load_config_handler()`, `DatasetsPanel` tab, `configure_spinbox_style()`, `color_contrast()`, `wrapped_checkbutton()`
**Edit for:**
- Add new config field → Add UI widget + bind to `save_config_handler()` + `load_config_handler()` (Quality Scoring section holds the `quality.roleplay_dimensions` toggle: `quality_roleplay_dims_var_editor`, saved/loaded with the rest of `quality.*`)
- **Wrapping labels (v11.6.2)** → a `ttk.Label` created with `wraplength=1` is the "not wrapped yet" sentinel; it MUST be handed to `wrap_label(label, container, ...)` or it renders one character per line forever. **The container must own a real allocated width** — never a frame that is itself sized by the label (a `ttk.Frame` gridded `sticky="w"`, or a `.pack()`ed child). `wrap_label` binds `<Configure>` on that container and sets `wraplength` from its width, so wrapping against a label-sized frame closes a feedback loop: label narrows → frame narrows → label narrows again, collapsing to a ~20px-wide, several-hundred-pixel-tall column (the Detection "Rewrite rounds per reply" caption and the Samplers "Enable Bad Words" description both hit this). `wrapped_checkbutton()` therefore wraps its description against *parent*, not against its own inner frame. Guarded by `tests/test_editor_readability_ui.py`
- **Spinboxes (v11.6.2)** → never create a bare `ttk.Spinbox`. The 'superhero' theme sets spinbox text white but leaves `fieldbackground` unset, so the field falls back to the native white entry background: white-on-white, an apparently empty box. Use `style=self.spinbox_style` (from `configure_spinbox_style()`, called once in `__init__` before any tab is built). It takes the colours from the theme's own `TEntry`, verifies them with `color_contrast()` (falls back to a known-good pair below 4.5:1), and `map`s a readable disabled state — the Regex Watch rounds box starts disabled. Guarded by `tests/test_editor_readability_ui.py`
- **Questions File Dataset Variables (v11.9.0)** → Prompts tab `questions_ds_lf` LabelFrame (after the first-line styles area): `questions_append_alias_var_editor` + `questions_append_separator_var_editor` Entries, plus a `wrap_label`ed description. The separator is shown ESCAPED (`\n`, `\t`, `\\`) because a Tk Entry cannot hold a real newline. Module-level helpers do the conversion and work on a stub editor too (the save path is driven headless by tests): `encode_escape_sequence()`, `decode_escape_sequence()` (only those four escapes; a lone trailing backslash is kept as typed), `questions_append_alias_value()`, `questions_append_separator_value()` (empty field falls back to `DEFAULT_QUESTION_SEPARATOR`). Saved as `prompts.questions_dataset.{append_alias,append_separator}`; validated (invalid alias, or an alias while the questions file is off). Guarded by `tests/test_questions_dataset_ui.py`
- **Non-Latin Script Check (v11.10.0)** → Detection tab `col2_frame`, a `LabelFrame` with `non_latin_enabled_var_editor` + `_toggle_non_latin_fields()` and a `non_latin_fixes_text` ScrolledText (the field starts ENABLED and is only disabled by the toggle, so the load path can write into it before the state is applied — this Tk build drops `delete`/`insert` on a disabled Text widget silently). Saved as `detection.non_latin.{enabled,fixes}` (fixes saved verbatim, not through `sanitize_input`); validated by a `get_text_as_list` touch. There is no pattern list to validate — the check is a Unicode range allow-list in `detection.py`
- **Scene Cast + Scene Continuity + cast dimension (v12.0)** → THREE new places. (1) Character Engine tab: a `ttk.LabelFrame` "Scene Cast — Characters Join and Leave" with `enable_scene_cast_var_editor` + `_toggle_scene_cast_fields()`, a `scene_cast_shape_var_editor` Combobox, three `ttk.Spinbox`es (`scene_cast_min_present_spinbox` / `_max_churn_spinbox` / `_max_deferrals_spinbox`, each `style=self.spinbox_style`), `scene_cast_placement_var_editor` via `_add_placement_combobox`, an allow-cutaway checkbox, and three `ScrolledText` guidance fields. (2) Detection tab `col2_frame`: a `scene_continuity_lf` LabelFrame with `scene_continuity_enabled_var_editor` + `_toggle_scene_continuity_fields()`, a rounds Spinbox and a fixes `ScrolledText` (created ENABLED, only disabled by the toggle, so the load path can write into it). (3) Quality Scoring: `quality_cast_continuity_var_editor`. **Every guidance/fix list is saved VERBATIM** (no `sanitize_input`) like the regex patterns and the opening-mode prompts — it strips backslashes and quotes and would corrupt authored text. `_toggle_scene_cast_fields()` is called at the END of the load path so a config saved with the toggle on reopens editable
- New tab → Add `ttk.Frame` + `notebook.add()` in `__init__()`
- Regex Watch panel (v11.6) → Detection tab `col1_frame`, a `LabelFrame` with `regex_watch_enabled_var_editor` + `_toggle_regex_watch_fields()`, a rounds `Spinbox` (`regex_watch_max_rounds_var_editor`), a Listbox backed by the plain list `self.regex_watch_patterns` (`_render_regex_patterns()`), an Entry plus Add/Update, Remove, Test and Restore buttons, and a status label. Handlers live in `ConfigEditor`; the save/validate helpers are MODULE-LEVEL (`regex_watch_rounds_value(editor)`, `regex_watch_enabled_value(editor)`, `regex_watch_pattern_values(editor)`) because the headless save-path tests drive a stub whose unknown attributes resolve to a Tk var — a method would be shadowed by that stub. **Patterns are saved VERBATIM**: `sanitize_input()` strips backslashes and quotes and would corrupt every expression
- **Judge priority (v11.8.x)** → Quality Scoring section: `quality_judge_priority_enabled_var` checkbox (`command=self._toggle_judge_priority_field`) + `quality_judge_priority_spinbox` (`from_=-10, to=10` — -10 is the HIGHEST priority, so a 0 floor would make the top of the range unreachable from the GUI, and a positive-only ceiling would lose the bottom; created `state='disabled'`, uses `style=self.spinbox_style`). `_toggle_judge_priority_field()` flips it between `normal`/`disabled` and is ALSO called at the end of the load path, so a config saved with the toggle on reopens editable. The value is kept (never reset) while the toggle is off. Saved as `quality.judge_priority_enabled` / `judge_priority`, validated to -10..10
- Validation → `validate_config_handler()`
- Datasets tab → `DatasetsPanel(self.notebook)` added in `__init__()`; config gathered in `save_config_handler()` (`config_data['datasets'] = self.datasets_panel.get_config()`), loaded in `load_config_handler()` (`self.datasets_panel.load_config(...)`), validated in `validate_config_handler()`; `DatasetSettingsError` surfaces on the status bar

---

### `tests/test_user_emotional_states_ui.py`
**Purpose:** Display-dependent regression test for the v10.6.0 user-side emotional-states save/load round trip in the REAL `ConfigEditor` (enable + type → save → `config.yml` holds states; reopen → states visible in the field; unmodified reopen+save → no clobber to `[]`). Catches the disabled-Text silent-drop bug class (edits dropped while the field is still `disabled`)
**Run:** `python -B tests/test_user_emotional_states_ui.py` (needs a display, e.g. under `xvfb-run`; skips cleanly without one)
**Edit for:** Add editor round-trip cases; keep it free of source data, generation runs, network and model APIs

---

### `tests/test_regex_watch_ui.py`
**Purpose:** Display-dependent regression test for the v11.6 Regex Watch editor panel in the REAL `ConfigEditor` (skips cleanly without a display)
**Key:** patterns with backslashes and quote classes survive save -> `config.yml` -> reopen byte for byte; a duplicate is refused; Remove deletes the selected pattern and the removal is saved; an invalid pattern is refused by Add and blocks the save; the toggle disables/enables the entry and Add button; Restore Defaults uses `detection.DEFAULT_REGEX_WATCH_PATTERNS`
**Run:** `python -B tests/test_regex_watch_ui.py` (needs a display, e.g. under `xvfb-run`)
**Edit for:** Add editor round-trip cases; keep it free of source data, generation runs, network and model APIs

---

### `tests/test_editor_readability_ui.py`
**Purpose:** Display-dependent regression test for the v11.6.2 Configuration Editor readability fixes (builds the REAL `ConfigEditor` under the production 'superhero' theme; skips cleanly without a display)
**Key:** no mapped label in any tab is taller than twice its width (a "skyscraper"), checked at a normal size AND at the 760x520 minimum; the Regex Watch patterns caption has a real `wraplength`; the Bad Words description is not a skyscraper; both spinboxes keep >=4.5:1 field/text contrast and >=3:1 while disabled; the rounds spinbox accepts typed input once enabled; every `TSpinbox` in the editor carries `SPINBOX_FIELD_STYLE`
**Run:** `python -B tests/test_editor_readability_ui.py` (needs a display, e.g. under `xvfb-run`)
**Edit for:** Add editor layout/contrast cases; keep it free of source data, generation runs, network and model APIs

---

### `tests/test_missing_quotations_ui.py`
**Purpose:** Display-dependent regression test for the v11.7.1 "Experiment: Retry missing quotations" checkbox in the REAL `ConfigEditor` (skips cleanly without a display)
**Key:** save -> `config.yml` holds `retry_missing_quotations: true`; reopen -> checkbox ticked again; unmodified reopen+save -> flag survives (no clobber to the off default). Mirrors `tests/test_user_emotional_states_ui.py`
**Run:** `python -B tests/test_missing_quotations_ui.py` (needs a display, e.g. under `xvfb-run`; skips cleanly without one)

---

### `tests/test_repeat_opener_ui.py`
**Purpose:** Display-dependent regression test for the v12.5 repeated-opener controls (`repeat_opener_guard` checkbox, `repeat_opener_max_retries` Spinbox, `repeat_opener_fixes` Text) in the REAL `ConfigEditor` (skips cleanly without a display)
**Key:** save -> all three land in `config.yml`; reopen -> every control shows the stored value; unmodified reopen+save -> nothing clobbers to the default; **the fix list is stored VERBATIM** (quotes, apostrophes and `->` survive, which `sanitize_input()` used to eat); a negative retry count is rejected by validation. **The fixes Text is created ENABLED and never disabled** — this Tk build drops `delete`/`insert` on a disabled Text widget silently
**Run:** `python -B tests/test_repeat_opener_ui.py` (needs a display, e.g. under `xvfb-run`; skips cleanly without one)
---

### `tests/test_repeat_reply_ui.py`
**Purpose:** Display-dependent regression test for the v12.7 repeated-REPLY controls (`repeat_reply_guard` checkbox, `repeat_reply_max_retries` Spinbox, `repeat_reply_fixes` Text) in the REAL `ConfigEditor` (skips cleanly without a display)
**Key:** the three keys ship off/empty/1; save -> all three land in `config.yml`; reopen -> every control shows the stored value; unmodified reopen+save -> nothing clobbers to the default; **the fix list is stored VERBATIM**; a negative retry count is rejected by validation; **the two guards are INDEPENDENT** (ticking one must not tick the other, because they catch different defects). **The fixes Text is created ENABLED and never disabled** — this Tk build drops `delete`/`insert` on a disabled Text widget silently
**Run:** `python -B tests/test_repeat_reply_ui.py` (needs a display, e.g. under `xvfb-run`; skips cleanly without one)

---

### `tests/test_questions_dataset_ui.py`
**Purpose:** Display-dependent regression test for the v11.9.0 "Questions File Dataset Variables" fields in the REAL `ConfigEditor` (skips cleanly without a display)
**Key:** the escaped separator round-trips (`\n---\n` typed in the Entry is stored in `config.yml` as real characters and shown escaped again on reopen); an unmodified reopen+save does not clobber either value; the escape helpers only decode the four known escapes and keep an unknown escape / lone trailing backslash as typed; validation refuses an invalid alias and an alias while the questions file is off
**Run:** `python -B tests/test_questions_dataset_ui.py` (needs a display, e.g. under `xvfb-run`; skips cleanly without one)

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Fix a label rendering one character per line (a "skyscraper") | `config_editor.py` — every `wraplength=1` label needs a `wrap_label(label, container, ...)` call, and the container must NOT be sized by that label (never a `ttk.Frame` gridded `sticky="w"`, never a frame packed around it). `wrapped_checkbutton()` wraps against `parent` for this reason. Guarded by `tests/test_editor_readability_ui.py` |
| Fix an unreadable / white-on-white spinbox | `config_editor.py` — `configure_spinbox_style()` + `SPINBOX_FIELD_STYLE`; pass `style=self.spinbox_style` to every `ttk.Spinbox`. The theme sets white text but no `fieldbackground`. Guarded by `tests/test_editor_readability_ui.py` |
| Add dataset column→alias mapping | `config_editor.py` (Datasets tab) → `datasets_panel.py` (Apply/Copy placeholder) → `dataset_settings.py` (validation) — placeholders `{{ds:alias}}` are then usable in any authored prompt field |
