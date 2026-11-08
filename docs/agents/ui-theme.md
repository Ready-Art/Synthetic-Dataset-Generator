<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Design tokens and styles (`ui_theme.py`)

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are changing any colour, font, spacing value, tab name, emoji icon, empty state or widget style. Every one of them lives in `ui_theme.py`.

**Related:** [`docs/agents/generate-ui.md`](generate-ui.md) (the window these tokens dress), [`docs/agents/review-tab.md`](review-tab.md) (the Review tab's count badge)

## Contents

- Token groups
- The style builders
- Emoji iconography
- Empty states
- Tasks that touch these files

---


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
| Add or change a tab count badge | `ui_theme.tab_badge()` / `tab_base_name()` — see [`review-tab.md`](review-tab.md) |
| Fix a cramped / clipped Treeview table | give the table its own style with an explicit `font` + `rowheight` from `font.metrics('linespace')`, and size columns from the heading text. Regex Watch does this via `REGEX_TREE_STYLE`, `_configure_regex_tree_style()`, `_configure_regex_tree_columns()`; guarded by `tests/test_regex_watch_tab_ui.py` |
