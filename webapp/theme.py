# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Design tokens for the web front end -- the **Storm** palette.

Pure data. Importing this module must never start a server, open a socket or touch the
network, so it is safe to import from a headless test, from ``webapp.app`` and from the
theme test.

This is the web successor to ``ui_theme.py``. The rule is the same one that file
records: **no raw ``#rrggbb`` literal lives outside this module.** Call sites reference
the semantic names below, or the CSS custom properties in :data:`STYLESHEET`, which are
generated from them.

Why the values are what they are
--------------------------------
Every foreground/background pair in :data:`CONTRAST_PAIRS` was measured, not chosen by
eye. The mockup palette failed WCAG AA in two places -- ``--accent #6b9bf5`` was 4.42:1
and ``--danger #e06c75`` was 3.81:1 against the raised card surface, which is where both
are actually drawn. They became ``#7aa6f7`` (5.00:1) and ``#e8838a`` (4.66:1). This is
the same class of bug ``ui_theme.py`` already documents for the Tk accent.

``tests/test_web_theme.py`` recomputes :data:`CONTRAST_PAIRS` from the constants below
and fails on any pair under 4.5:1, so the table cannot drift from the palette.

Accent buttons use :data:`ACCENT_FG`, never white. White on ``--accent`` is 2.43:1 and
fails badly; ``ACCENT_FG`` on ``--accent`` is 7.56:1.
"""

# --- Palette -------------------------------------------------------------
# Semantic names only. Never reference a raw hex outside this module.

BG = '#232830'          # page background
SURFACE = '#282e38'     # panels, inputs
RAISED = '#2f3641'      # cards, headers, table bodies
BORDER = '#3d4552'      # 1px edges

TEXT = '#eef1f6'        # body text
MUTED = '#98a2b3'       # captions, secondary text

ACCENT = '#7aa6f7'      # primary action, active nav item
ACCENT_FG = '#0f1420'   # text ON an accent fill -- never white (2.43:1)

OK = '#5fbf7f'          # success
WARN = '#e0a458'        # warning
DANGER = '#e8838a'      # danger text and pills

# A filled destructive button needs white text, and white on DANGER is only 2.2:1.
# This darker red carries white at 5.4:1 and is used for the fill only, never for text.
DANGER_DEEP = '#8f2f38'

# Chart series colours. Deliberately drawn from the palette above so a graph never
# introduces a hue the rest of the UI does not have.
CHART_SERIES = (
    ACCENT,     # refusals
    DANGER,     # out of character
    WARN,       # slop
    OK,         # quality
    MUTED,      # everything else
)

# --- Type scale ----------------------------------------------------------
# The "massive text" bug in the Tk GUI was structural, not a font size: a 22px value
# inside a wraplength-220 label inside a 180px grid cell. In CSS a card sizes to its
# content and a long value ellipsizes with a `title` tooltip, so these sizes are safe.

FONT_STACK = ("-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, "
              "'Helvetica Neue', Arial, sans-serif")
MONO_STACK = ("ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, "
              "'Liberation Mono', monospace")

SIZE_BRAND = '15px'
SIZE_CARD_VALUE = '28px'
SIZE_SECTION = '11px'
SIZE_BODY = '13px'
SIZE_CAPTION = '10px'
SIZE_TABLE_HEAD = '10.5px'

TRACK_CARD_VALUE = '-0.5px'
TRACK_SECTION = '1.1px'
TRACK_CAPTION = '1.2px'

# --- Geometry ------------------------------------------------------------

RADIUS_CARD = '12px'
RADIUS_CONTROL = '8px'
RADIUS_PILL = '999px'

# --- Contrast contract ---------------------------------------------------
# (foreground name, background name, expected ratio from the plan's verified table).
# The test recomputes each ratio and asserts it is >= 4.5:1 AND within 0.05 of the
# expected value, so a palette edit that keeps AA but silently changes the look is
# still caught.

CONTRAST_PAIRS = (
    ('TEXT', 'BG', 13.08),
    ('TEXT', 'SURFACE', 12.06),
    ('TEXT', 'RAISED', 10.75),
    ('MUTED', 'BG', 5.75),
    ('MUTED', 'SURFACE', 5.30),
    ('MUTED', 'RAISED', 4.73),
    ('ACCENT', 'BG', 6.08),
    ('ACCENT', 'SURFACE', 5.61),
    ('ACCENT', 'RAISED', 5.00),
    ('OK', 'BG', 6.52),
    ('OK', 'SURFACE', 6.01),
    ('OK', 'RAISED', 5.36),
    ('WARN', 'BG', 6.79),
    ('WARN', 'SURFACE', 6.25),
    ('WARN', 'RAISED', 5.58),
    ('DANGER', 'BG', 5.67),
    ('DANGER', 'SURFACE', 5.23),
    ('DANGER', 'RAISED', 4.66),
    ('ACCENT_FG', 'ACCENT', 7.56),
)

# The minimum WCAG AA ratio for normal-size text. Every pair above must clear it.
MIN_CONTRAST = 4.5


# --- CSS -----------------------------------------------------------------

def _css_variables():
    """The ``:root`` custom-property block, generated from the constants above."""
    tokens = (
        ('--bg', BG),
        ('--surface', SURFACE),
        ('--raised', RAISED),
        ('--border', BORDER),
        ('--text', TEXT),
        ('--muted', MUTED),
        ('--accent', ACCENT),
        ('--accent-fg', ACCENT_FG),
        ('--ok', OK),
        ('--warn', WARN),
        ('--danger', DANGER),
        ('--danger-deep', DANGER_DEEP),
        ('--radius-card', RADIUS_CARD),
        ('--radius-control', RADIUS_CONTROL),
        ('--radius-pill', RADIUS_PILL),
        ('--font', FONT_STACK),
        ('--mono', MONO_STACK),
        ('--size-brand', SIZE_BRAND),
        ('--size-card-value', SIZE_CARD_VALUE),
        ('--size-section', SIZE_SECTION),
        ('--size-body', SIZE_BODY),
        ('--size-caption', SIZE_CAPTION),
        ('--size-table-head', SIZE_TABLE_HEAD),
        ('--track-card-value', TRACK_CARD_VALUE),
        ('--track-section', TRACK_SECTION),
        ('--track-caption', TRACK_CAPTION),
    )
    return '\n'.join(f'  {name}: {value};' for name, value in tokens)


STYLESHEET = """
:root {
%(variables)s
}

*, *::before, *::after { box-sizing: border-box; }

html, body {
  margin: 0;
  padding: 0;
  height: 100%%;
}

body {
  background: var(--bg);
  color: var(--text);
  font-family: var(--font);
  font-size: var(--size-body);
  line-height: 1.45;
  -webkit-font-smoothing: antialiased;
}

/* --- Typography ------------------------------------------------------- */

.brand {
  font-size: var(--size-brand);
  font-weight: 700;
  letter-spacing: 0;
}

.section-header {
  font-size: var(--size-section);
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: var(--track-section);
  color: var(--muted);
  margin: 0 0 8px;
}

.muted { color: var(--muted); }
.mono { font-family: var(--mono); }

/* --- Shell ------------------------------------------------------------ */

.app {
  display: grid;
  grid-template-rows: auto 1fr;
  height: 100vh;
  overflow: hidden;
}

.topbar {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 10px 18px;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
}

.topbar .brand { color: var(--text); }

.topbar .spacer { flex: 1 1 auto; }

.tabs {
  display: flex;
  gap: 2px;
  align-items: stretch;
  overflow-x: auto;
  scrollbar-width: thin;
}

.tab {
  appearance: none;
  border: 0;
  background: transparent;
  color: var(--muted);
  font: inherit;
  font-size: 12px;
  padding: 7px 12px;
  border-radius: var(--radius-control);
  cursor: pointer;
  white-space: nowrap;
}

.tab:hover { background: var(--raised); color: var(--text); }

.tab[aria-selected="true"] {
  background: var(--raised);
  color: var(--accent);
  font-weight: 600;
}

.main {
  overflow: hidden;
  padding: 16px 18px;
  min-height: 0;
}

/* The screen fills the main area exactly. `min-height: 0` at every level is what
   lets an inner panel scroll instead of the page: a magic `calc(100vh - Npx)` would
   drift the moment the header height changes. */
.screen {
  display: none;
  height: 100%%;
  min-height: 0;
  overflow: hidden;
}

.screen.active {
  display: flex;
  flex-direction: column;
  min-height: 0;
}

/* The default view must fit 1440x900 with no scroll. Only an expanded
   disclosure may scroll, because the extra cards genuinely need the room. */
.screen.active { overflow: hidden; }
.main.expanded .screen.active { overflow-y: auto; }

/* --- Cards ------------------------------------------------------------ */

.card {
  background: var(--raised);
  border: 1px solid var(--border);
  border-radius: var(--radius-card);
  padding: 14px 16px;
  min-width: 0;
}

.card-caption {
  font-size: var(--size-caption);
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: var(--track-caption);
  color: var(--muted);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.card-value {
  font-size: var(--size-card-value);
  font-weight: 700;
  letter-spacing: var(--track-card-value);
  color: var(--text);
  line-height: 1.1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.card-sub {
  font-size: 11px;
  color: var(--muted);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.metric-row {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 12px;
}

/* The disclosure row holds 10 cards, so it gets its own column count rather than
   being squeezed into the primary row's six. */
.metric-row.secondary {
  grid-template-columns: repeat(5, minmax(0, 1fr));
}

/* Below 1100px the six-across row would clip a 28px value, so it drops to three
   and the disclosure to two. This is the responsive rule the Tk ResponsiveGrid was
   trying to be, expressed once instead of per widget. */
@media (max-width: 1100px) {
  .metric-row { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .metric-row.secondary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .overview-grid { grid-template-columns: 1fr; }
  .overview-side { grid-template-rows: auto auto; }
}

/* --- Overview layout -------------------------------------------------- */

/* The body fills the height the header and metric row leave. `min-height: 0` on every
   level is what lets the inner panels scroll instead of the page. */
.overview-grid {
  display: grid;
  grid-template-columns: 1.55fr 1fr;
  gap: 12px;
  margin-top: 12px;
  flex: 1 1 auto;
  min-height: 0;
}

/* `auto` for progress (it sizes to its own rows) and `1fr` for the graph, which
   takes whatever is left. Both get `min-height: 0` so the graph shrinks rather than
   pushing the progress panel off the bottom. */
.overview-side {
  display: grid;
  grid-template-rows: auto minmax(0, 1fr);
  gap: 12px;
  min-height: 0;
  min-width: 0;
}

.action-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
}

.action-bar .spacer { flex: 1 1 auto; }

.disclosure-toggle {
  background: transparent;
  border: 0;
  color: var(--accent);
  font-size: 11px;
  font-weight: 600;
  padding: 2px 0;
  cursor: pointer;
  text-align: left;
}

.disclosure-body:empty { display: none; }

.disclosure-toggle:hover { text-decoration: underline; }

.issue-group { margin-bottom: 14px; }
.issue-group:last-child { margin-bottom: 0; }

.issue-group-head {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 4px;
}

.count-row {
  display: flex;
  gap: 14px;
  margin-top: 12px;
  padding-top: 10px;
  border-top: 1px solid var(--border);
}

.count-chip {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 11px;
}

.progress-row {
  grid-template-columns: 62px 1fr 38px;
}

.progress-detail {
  grid-column: 2 / 4;
  font-size: 10px;
  color: var(--muted);
  margin-top: -2px;
  padding-bottom: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.diag-table td { white-space: normal; }

/* --- Review ----------------------------------------------------------- */

/* 1fr / 1.15fr: the detail pane is slightly wider because it holds prose.
   Both panes get `min-height: 0` so each scrolls on its own rather than the page. */
.review-grid {
  display: grid;
  grid-template-columns: 1fr 1.15fr;
  gap: 12px;
  flex: 1 1 auto;
  min-height: 0;
}

.review-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
  flex-wrap: wrap;
}

.review-toolbar input[type="search"] {
  width: 220px;
  flex: 0 0 auto;
}

.review-toolbar .spacer { flex: 1 1 auto; }

.review-count {
  font-size: 11px;
  font-weight: 600;
  color: var(--danger);
  white-space: nowrap;
}

.review-table tbody tr { cursor: pointer; }
.review-table tbody tr:focus-visible {
  outline: 2px solid var(--accent);
  outline-offset: -2px;
}

/* The score column is colour-coded by severity, the same two tags the Tk table
   uses: critical below 40, warning otherwise. */
.review-table td.score { font-weight: 700; font-variant-numeric: tabular-nums; }
.review-table td.score.critical { color: var(--danger); }
.review-table td.score.warning { color: var(--warn); }

/* The detail pane is a column so the footer strip stays pinned at the bottom
   while the transcript scrolls. */
.review-detail {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.detail-head {
  font-size: 11px;
  color: var(--muted);
  border-bottom: 1px solid var(--border);
  padding-bottom: 6px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.detail-note {
  font-size: 11px;
  color: var(--warn);
  background: var(--raised);
  border-left: 3px solid var(--warn);
  border-radius: var(--radius-control);
  padding: 8px 10px;
}

.turn { border-left: 3px solid var(--border); padding-left: 10px; }
.turn-user { border-left-color: var(--accent); }
.turn-assistant { border-left-color: var(--ok); }
.turn-system { border-left-color: var(--warn); }

.turn-role {
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: var(--track-caption);
  color: var(--muted);
  margin-bottom: 3px;
}

.turn-text {
  font-family: var(--mono);
  font-size: 11.5px;
  white-space: pre-wrap;
  word-break: break-word;
}

/* The score breakdown footer strip: four colour-coded cells across the bottom of
   the detail pane. This is what makes "why was this flagged" answerable at a
   glance instead of by reading a wall of numbers. */
.score-strip {
  display: grid;
  grid-auto-flow: column;
  grid-auto-columns: minmax(0, 1fr);
  gap: 1px;
  background: var(--border);
  border-top: 1px solid var(--border);
  flex: 0 0 auto;
}

.score-cell {
  background: var(--raised);
  padding: 8px 12px;
  min-width: 0;
}

.score-cell .card-caption {
  color: var(--muted);
  overflow: hidden;
  text-overflow: ellipsis;
}

.score-value {
  font-size: 16px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  color: var(--text);
}

.score-value.small { font-size: 12px; font-weight: 600; }
.score-cell.ok .score-value { color: var(--ok); }
.score-cell.warn .score-value { color: var(--warn); }
.score-cell.danger .score-value { color: var(--danger); }
.score-cell.muted .score-value { color: var(--muted); }

@media (max-width: 1100px) {
  .review-grid { grid-template-columns: 1fr; }
}

/* --- Data tabs -------------------------------------------------------- */

/* The six data tabs share one shell: a toolbar, then a body that scrolls on its own.
   `min-height: 0` is what lets the body shrink; without it a long table pushes the
   toolbar off the top. */
.tab-body {
  display: flex;
  flex-direction: column;
  gap: 12px;
  flex: 1 1 auto;
  min-height: 0;
  overflow-y: auto;
}

.tab-toolbar {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
  flex-wrap: wrap;
}

.tab-toolbar .spacer { flex: 1 1 auto; }

.button-row {
  display: inline-flex;
  align-items: center;
  gap: 8px;
}

/* A stack of panels. `minmax(0, 1fr)` on the column keeps a wide table from
   forcing the grid wider than its container. */
.tab-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 12px;
  align-content: start;
}

.tab-grid.two {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

/* The Quality tab's six cards reuse `.metric-row` but are not the Overview's six, so
   they wrap rather than forcing a fixed six-column grid. */
.tab-grid .metric-row {
  grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
}

/* A score in a table cell, colour-coded by threshold. Same two tags the Review table
   uses, plus `ok` for a dimension average above the warn band. */
table.grid td .score { font-weight: 700; font-variant-numeric: tabular-nums; }
table.grid td .score.critical { color: var(--danger); }
table.grid td .score.danger { color: var(--danger); }
table.grid td .score.warning { color: var(--warn); }
table.grid td .score.warn { color: var(--warn); }
table.grid td .score.ok { color: var(--ok); }

/* --- Prompts ---------------------------------------------------------- */

/* List left, preview right. The preview is wider because it holds prose, and both
   panes scroll on their own so a 2,000-character prompt does not move the list. */
.prompt-grid {
  display: grid;
  grid-template-columns: 300px minmax(0, 1fr);
  gap: 12px;
  flex: 1 1 auto;
  min-height: 0;
}

.prompt-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
  overflow-y: auto;
  min-height: 0;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-panel);
  padding: 6px;
}

.prompt-row {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 8px;
  /* Doubled: STYLESHEET is a printf-style template, so a literal percent must be
     escaped here or the template raises on import. */
  width: 100%%;
  text-align: left;
  background: transparent;
  border: 1px solid transparent;
  border-radius: var(--radius-control);
  padding: 6px 8px;
  color: var(--text);
  font: inherit;
  font-size: 12px;
  cursor: pointer;
}

.prompt-row:hover { background: var(--raised); }
.prompt-row.selected {
  background: var(--raised);
  border-color: var(--accent);
}

.prompt-preview {
  display: flex;
  flex-direction: column;
  gap: 10px;
  min-height: 0;
  overflow-y: auto;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-panel);
  padding: 12px;
}

.prompt-preview .detail-head {
  display: flex;
  align-items: baseline;
  gap: 8px;
}

.prompt-preview .detail-head .spacer { flex: 1 1 auto; }

/* `pre-wrap` rather than `pre`: a prompt is authored prose, so a long line must wrap
   instead of forcing a horizontal scrollbar across the whole pane. */
.prompt-text {
  margin: 0;
  font-family: var(--mono);
  font-size: 11.5px;
  line-height: 1.55;
  white-space: pre-wrap;
  word-break: break-word;
  color: var(--text);
}

@media (max-width: 1100px) {
  .prompt-grid { grid-template-columns: minmax(0, 1fr); }
  .tab-grid.two { grid-template-columns: minmax(0, 1fr); }
}

/* --- Charts ----------------------------------------------------------- */

/* A fixed height, not `1fr`. Stretched to fill the panel the SVG's viewBox is
   scaled non-uniformly and the lines look like they were drawn with a ruler. */
.chart-host {
  width: 100%%;
  flex: 0 0 auto;
  display: block;
}

.chart {
  display: block;
  width: 100%%;
  height: 132px;
}

.chart-legend {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 6px;
  font-size: 10px;
  line-height: 1.2;
  color: var(--muted);
  flex: 0 0 auto;
}

/* The graph panel's body is a column so the chart takes the leftover height and
   the legend keeps its own row. As a scrolling block the legend was clipped. */
.panel-body.chart-body {
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.chart-legend-item {
  display: inline-flex;
  align-items: center;
  gap: 5px;
}

.chart-swatch {
  width: 9px;
  height: 3px;
  border-radius: 2px;
  display: inline-block;
}

/* --- Panels ----------------------------------------------------------- */

.panel {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-card);
  display: flex;
  flex-direction: column;
  min-height: 0;
  min-width: 0;
  overflow: hidden;
}

.panel-head {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 14px;
  border-bottom: 1px solid var(--border);
  background: var(--raised);
}

.panel-body {
  padding: 10px 14px;
  overflow: auto;
  min-height: 0;
  flex: 1 1 auto;
}

/* --- Tables ----------------------------------------------------------- */

table.grid {
  width: 100%%;
  border-collapse: collapse;
  font-size: 12px;
  table-layout: fixed;
}

table.grid th {
  position: sticky;
  top: 0;
  text-align: left;
  font-size: var(--size-table-head);
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.6px;
  color: var(--muted);
  background: var(--surface);
  padding: 6px 8px;
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
}

table.grid td {
  padding: 6px 8px;
  border-bottom: 1px solid var(--border);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

/* The issue table's Context column is the one that carries real text, so it gets
   the remaining width and the phrase column a fixed share. Without this the two
   columns fight and the context is clipped to a few characters. */
table.grid td:last-child, table.grid th:last-child {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

table.grid tbody tr:hover { background: var(--raised); }
table.grid tbody tr.selected { background: var(--raised); }

/* --- Pills ------------------------------------------------------------ */

.pill {
  display: inline-block;
  padding: 1px 8px;
  border-radius: var(--radius-pill);
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.6px;
  border: 1px solid currentColor;
}

.pill.ok { color: var(--ok); }
.pill.warn { color: var(--warn); }
.pill.danger { color: var(--danger); }
.pill.muted { color: var(--muted); }

/* --- Controls --------------------------------------------------------- */

button, .btn {
  appearance: none;
  font: inherit;
  font-size: 12px;
  font-weight: 600;
  padding: 7px 14px;
  border-radius: var(--radius-control);
  border: 1px solid var(--border);
  background: var(--raised);
  color: var(--text);
  cursor: pointer;
}

button:hover:not(:disabled), .btn:hover:not(:disabled) {
  border-color: var(--muted);
}

button:disabled, .btn:disabled {
  opacity: 0.45;
  cursor: not-allowed;
}

/* Keyboard navigation is a Phase 8 requirement, and a focus ring that only the browser
   draws is invisible on the raised surface in some engines. `:focus-visible` keeps the ring
   off a mouse click but shows it for Tab, which is the whole point of the pseudo-class. */
button:focus-visible,
.btn:focus-visible,
.tab:focus-visible,
.toast-close:focus-visible,
.disclosure-toggle:focus-visible,
table.grid tbody tr:focus-visible {
  outline: 2px solid var(--accent);
  outline-offset: 2px;
}

/* A tab is a control in a roving-tabindex tablist: only the selected one is in the tab
   order, so the arrow keys move between them. The ring has to be visible on the raised
   background the selected tab already sits on. */
.tab[aria-selected="true"]:focus-visible {
  outline-offset: -2px;
}

.btn-primary {
  background: var(--accent);
  border-color: var(--accent);
  color: var(--accent-fg);
}

.btn-danger {
  background: var(--danger-deep);
  border-color: var(--danger-deep);
  color: var(--text);
}

input[type="text"], input[type="password"], input[type="number"],
input[type="search"], select, textarea {
  font: inherit;
  font-size: 12px;
  color: var(--text);
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-control);
  padding: 6px 9px;
  width: 100%%;
}

input:focus, select:focus, textarea:focus {
  outline: 2px solid var(--accent);
  outline-offset: -1px;
}

/* --- Progress --------------------------------------------------------- */

.progress-row {
  display: grid;
  grid-template-columns: 84px 1fr 46px;
  align-items: center;
  gap: 10px;
  padding: 4px 0;
  font-size: 12px;
}

.progress-track {
  height: 7px;
  border-radius: var(--radius-pill);
  background: var(--bg);
  overflow: hidden;
}

.progress-fill {
  height: 100%%;
  border-radius: var(--radius-pill);
  background: var(--accent);
  transition: width 240ms ease;
}

.progress-pct {
  text-align: right;
  color: var(--muted);
  font-variant-numeric: tabular-nums;
}

/* --- Login ------------------------------------------------------------ */

.login-wrap {
  display: grid;
  place-items: center;
  height: 100vh;
  background: var(--bg);
}

.login-card {
  width: 340px;
  background: var(--raised);
  border: 1px solid var(--border);
  border-radius: var(--radius-card);
  padding: 24px;
}

.login-card h1 {
  font-size: var(--size-brand);
  margin: 0 0 4px;
}

.login-error {
  color: var(--danger);
  font-size: 12px;
  margin: 0 0 10px;
  min-height: 16px;
}

.login-hint {
  color: var(--muted);
  font-size: 11px;
  margin: 12px 0 0;
}

/* --- Toasts ----------------------------------------------------------- */

.toasts {
  position: fixed;
  right: 16px;
  bottom: 16px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  z-index: 50;
}

.toast {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  background: var(--raised);
  border: 1px solid var(--border);
  border-left: 3px solid var(--accent);
  border-radius: var(--radius-control);
  padding: 9px 14px;
  font-size: 12px;
  max-width: 340px;
  box-shadow: 0 6px 20px rgba(0, 0, 0, 0.35);
  animation: toast-in 120ms ease-out;
}

/* A toast that appears instantly is easy to miss beside a table that just changed. The
   slide is short on purpose: motion is a cue, not an event. */
@keyframes toast-in {
  from { opacity: 0; transform: translateY(6px); }
  to { opacity: 1; transform: translateY(0); }
}

.toast-message { flex: 1 1 auto; min-width: 0; overflow-wrap: anywhere; }

/* The dismiss button is a real button so it is reachable by Tab, and it is quiet until the
   toast is hovered or focused -- a row of loud crosses over a table is worse than no toasts. */
.toast-close {
  appearance: none;
  border: 0;
  background: transparent;
  color: var(--muted);
  font: inherit;
  font-size: 15px;
  line-height: 1;
  padding: 0 2px;
  cursor: pointer;
  flex: 0 0 auto;
}

.toast-close:hover { color: var(--text); }

.toast.ok { border-left-color: var(--ok); }
.toast.warn { border-left-color: var(--warn); }
.toast.danger { border-left-color: var(--danger); }

/* --- Empty states ----------------------------------------------------- */

.empty {
  color: var(--muted);
  font-size: 12px;
  padding: 18px 4px;
  text-align: center;
}

/* An empty state says what is empty and what to do about it. A single grey line does not,
   and a blank area reads as a bug. */
.empty-title {
  color: var(--text);
  font-size: 13px;
  font-weight: 600;
  margin-bottom: 4px;
}

.empty-hint {
  color: var(--muted);
  font-size: 11.5px;
  line-height: 1.5;
  max-width: 46ch;
  margin: 0 auto;
}

.empty-action { margin-top: 12px; }

/* --- Config editor ---------------------------------------------------- */

/* Left nav, right fields. The nav is a fixed 250px because a section name is short and
   a field list is not: giving the fields every remaining pixel is what keeps a 1,000-line
   list editor readable. Both panes scroll on their own, so the toolbar never moves. */
.config-body {
  display: grid;
  grid-template-columns: 250px minmax(0, 1fr);
  gap: 12px;
  flex: 1 1 auto;
  min-height: 0;
}

.config-nav-pane {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 0;
  overflow-y: auto;
  padding-right: 2px;
}

.config-nav {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

/* A nav item is a row, not a tab: the label takes the room and the leaf count sits
   right-aligned in a tabular figure so the column reads as a column. */
.config-nav-item {
  display: flex;
  align-items: center;
  gap: 8px;
  width: 100%%;
  padding: 7px 10px;
  border: 1px solid transparent;
  border-radius: var(--radius-control);
  background: transparent;
  color: var(--muted);
  font-size: 12px;
  font-weight: 500;
  text-align: left;
}

.config-nav-item:hover {
  background: var(--surface);
  border-color: var(--border);
  color: var(--text);
}

.config-nav-item[aria-selected="true"] {
  background: var(--raised);
  border-color: var(--border);
  color: var(--accent);
  font-weight: 600;
}

.config-nav-label {
  flex: 1 1 auto;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.config-nav-count {
  font-size: 10.5px;
  font-variant-numeric: tabular-nums;
  color: var(--muted);
}

/* The add form. It is not a field, so it is visually separated from the nav list. */
.config-add {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px;
  border: 1px dashed var(--border);
  border-radius: var(--radius-card);
  background: var(--surface);
}

.config-add input {
  font-size: 11px;
}

.config-add .config-help {
  margin: 0;
}

/* The field list. It scrolls; the toolbar above it does not. */
.config-fields {
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow-y: auto;
  padding-right: 4px;
}

.config-fields-head {
  display: flex;
  align-items: baseline;
  gap: 10px;
  padding-bottom: 8px;
  margin-bottom: 4px;
  border-bottom: 1px solid var(--border);
  position: sticky;
  top: 0;
  background: var(--bg);
  z-index: 1;
}

.config-fields-head .spacer { flex: 1 1 auto; }

.config-grid {
  display: flex;
  flex-direction: column;
}

/* A group heading is a rule with a name, not a box: nesting three levels of boxes inside a
   pane that already scrolls is how a form becomes unreadable. */
.config-group-head {
  display: flex;
  align-items: baseline;
  gap: 10px;
  padding: 12px 0 6px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 2px;
}

.config-group-label {
  font-size: var(--size-section);
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: var(--track-section);
  color: var(--muted);
}

.config-group-head .mono {
  font-size: 10.5px;
  color: var(--muted);
  opacity: 0.75;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.config-group-head.depth-1 { padding-left: 14px; }
.config-group-head.depth-2 { padding-left: 28px; }
.config-group-head.depth-3 { padding-left: 42px; }

/* Two columns: label left, control right. `minmax(0, 1fr)` on the control column is what
   lets a long value ellipsize instead of widening the grid -- the same rule that fixed the
   Overview cards. */
.config-row {
  display: grid;
  grid-template-columns: 240px minmax(0, 1fr);
  gap: 14px;
  align-items: start;
  padding: 7px 0;
  border-bottom: 1px solid var(--border);
}

.config-row.depth-1 { padding-left: 14px; }
.config-row.depth-2 { padding-left: 28px; }
.config-row.depth-3 { padding-left: 42px; }

.config-row.changed {
  background: var(--surface);
  box-shadow: inset 2px 0 0 var(--accent);
}

.config-row.invalid {
  background: var(--surface);
  box-shadow: inset 2px 0 0 var(--danger);
}

.config-label {
  display: flex;
  flex-direction: column;
  gap: 1px;
  min-width: 0;
  padding-top: 2px;
}

.config-label-text {
  font-size: 12px;
  font-weight: 600;
  color: var(--text);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

/* The raw path is the one thing that always identifies a field, so it is shown rather than
   hidden behind a tooltip. It ellipsizes, and the label carries the full path as a title. */
.config-path {
  font-size: 10px;
  color: var(--muted);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.config-control {
  display: flex;
  flex-direction: column;
  gap: 3px;
  min-width: 0;
}

.config-control input[type="text"],
.config-control input[type="number"],
.config-control input[type="password"],
.config-control select,
.config-control textarea {
  font-size: 12px;
}

/* An enum is a native <select>: the browser's own list is keyboard-navigable and accessible for
   free, and a fixed choice list is exactly what a select is for. */
.config-control select {
  width: 100%%;
  max-width: 320px;
}

/* A flat list is one textarea. Monospace because it is one entry per line and the line
   count is the point. */
.config-control textarea.config-list {
  font-family: var(--mono);
  font-size: 11.5px;
  line-height: 1.5;
  resize: vertical;
}

.config-check {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
  color: var(--muted);
  cursor: pointer;
}

.config-check input {
  width: auto;
  accent-color: var(--accent);
  cursor: pointer;
}

.config-secret {
  display: flex;
  align-items: center;
  gap: 8px;
}

.config-secret input { flex: 1 1 auto; }

/* Help text sits under the control, never beside it: beside it would steal the width the
   value needs, which is the mistake that produced the Tk "massive text" bug. */
.config-help {
  margin: 0;
  font-size: 10.5px;
  color: var(--muted);
  line-height: 1.35;
}

.config-error {
  margin: 0;
  font-size: 10.5px;
  font-weight: 600;
  color: var(--danger);
}

/* Below 1100px the nav goes above the fields rather than squeezing both. */
@media (max-width: 1100px) {
  .config-body {
    grid-template-columns: minmax(0, 1fr);
    grid-template-rows: auto minmax(0, 1fr);
  }

  .config-nav-pane {
    max-height: 180px;
  }

  .config-row {
    grid-template-columns: minmax(0, 1fr);
    gap: 4px;
  }
}
/* --- Datasets panel (6d-a) ------------------------------------------- */

/* Source list on the left, the selected source on the right. `minmax(0, 1fr)` on
   the detail column is what lets a long column name ellipsize instead of pushing
   the pane wider than the screen. */
.datasets-body {
  display: grid;
  grid-template-columns: 260px minmax(0, 1fr);
  gap: 14px;
  min-height: 0;
  flex: 1 1 auto;
  overflow: hidden;
}

.datasets-list-pane {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 0;
  overflow: hidden;
}

.datasets-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-height: 0;
  overflow-y: auto;
  flex: 1 1 auto;
}

.datasets-item {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 8px 10px;
  text-align: left;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-control);
  color: var(--text);
  cursor: pointer;
  font: inherit;
}

.datasets-item:hover { background: var(--raised); }

.datasets-item[aria-selected="true"] {
  border-color: var(--accent);
  background: var(--raised);
}

.datasets-item-name {
  font-size: 12.5px;
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.datasets-item-meta {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
  font-size: 10.5px;
}

.datasets-add { flex: 0 0 auto; }
.datasets-add button { width: 100%%; }

.datasets-detail {
  display: flex;
  flex-direction: column;
  gap: 10px;
  min-height: 0;
  overflow-y: auto;
  padding-right: 4px;
}

.datasets-detail-head {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.datasets-detail-head .spacer { flex: 1 1 auto; }

/* The status line is the model's own verdict, so it gets its own row rather than
   being folded into the header where it would be easy to miss. */
.datasets-status {
  margin: 0;
  font-size: 11.5px;
  line-height: 1.4;
  color: var(--muted);
}

.datasets-status.warn { color: var(--warn); }
.datasets-status.danger { color: var(--danger); }

.datasets-settings {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.datasets-row {
  display: grid;
  grid-template-columns: 130px minmax(0, 1fr);
  gap: 10px;
  align-items: start;
}

.datasets-label {
  font-size: 11px;
  font-weight: 600;
  color: var(--muted);
  padding-top: 6px;
}

.datasets-control { display: flex; flex-direction: column; gap: 3px; }
.datasets-control input[type="text"],
.datasets-control select { width: 100%%; }

.datasets-columns {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 0;
}

.datasets-columns-head {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.datasets-columns-head .spacer { flex: 1 1 auto; }

/* Phase 7: the column-name filter. It sits above the table rather than in the
   header row so a long query does not squeeze the Import button. */
.datasets-column-filter {
  display: flex;
  align-items: center;
  gap: 8px;
}

.datasets-column-filter input {
  width: 100%%;
}

/* An empty filter result is a state, not a blank table: a blank table reads as
   "this source has no columns", which is a different and alarming thing. */
.datasets-columns-table tbody tr.datasets-no-match td {
  padding: 10px 8px;
  color: var(--muted);
  font-style: italic;
  cursor: default;
}

/* The column table scrolls inside its own pane, so a 40-column source cannot push
   the alias editor off the bottom of the screen. */
.datasets-columns-table {
  max-height: 260px;
  overflow-y: auto;
  display: block;
}

.datasets-columns-table tbody tr { cursor: pointer; }
.datasets-columns-table tbody tr:focus-visible {
  outline: 2px solid var(--accent);
  outline-offset: -2px;
}

.datasets-alias-editor {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.datasets-alias-editor input { flex: 1 1 160px; }

.datasets-visibility {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  font-size: 11px;
}

.datasets-visibility .spacer { flex: 1 1 auto; }

/* Below 1100px the list goes above the detail rather than squeezing both. */
@media (max-width: 1100px) {
  .datasets-body {
    grid-template-columns: minmax(0, 1fr);
    grid-template-rows: auto minmax(0, 1fr);
  }

  .datasets-list { max-height: 160px; }

  .datasets-row {
    grid-template-columns: minmax(0, 1fr);
    gap: 4px;
  }

  .datasets-label { padding-top: 0; }
}
/* --- Profiles panel (6d-b) ------------------------------------------- */

/* Saved profiles on the left, the selected profile's preview on the right. The same
   two-pane shape as the Datasets panel, for the same reason: a list and a detail that
   both need to scroll independently. */
.profiles-body {
  display: grid;
  grid-template-columns: 260px minmax(0, 1fr);
  gap: 14px;
  min-height: 0;
  flex: 1 1 auto;
  overflow: hidden;
}

.profiles-list-pane {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-height: 0;
  overflow: hidden;
}

.profiles-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-height: 0;
  overflow-y: auto;
  flex: 1 1 auto;
}

.profiles-item {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 8px 10px;
  text-align: left;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-control);
  color: var(--text);
  cursor: pointer;
  font: inherit;
}

.profiles-item:hover { background: var(--raised); }

.profiles-item[aria-selected="true"] {
  border-color: var(--accent);
  background: var(--raised);
}

.profiles-item-name {
  font-size: 12.5px;
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.profiles-item-meta {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
  font-size: 10.5px;
}

/* The save form is pinned under the list rather than scrolling with it, so a long
   profile list cannot push the only way to save off the bottom. */
.profiles-save {
  display: flex;
  flex-direction: column;
  gap: 6px;
  flex: 0 0 auto;
  padding-top: 8px;
  border-top: 1px solid var(--border);
}

.profiles-save input { width: 100%%; }
.profiles-save button { width: 100%%; }

.profiles-detail {
  display: flex;
  flex-direction: column;
  gap: 10px;
  min-height: 0;
  overflow-y: auto;
  padding-right: 4px;
}

.profiles-detail-head {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.profiles-detail-head .spacer { flex: 1 1 auto; }

/* The verdict above the diff: whether a load is possible at all, and how big it is. */
.profiles-status {
  margin: 0;
  font-size: 11.5px;
  line-height: 1.4;
  color: var(--muted);
}

.profiles-status.warn { color: var(--warn); }
.profiles-status.danger { color: var(--danger); }

.profiles-error-block {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 8px 10px;
  border: 1px solid var(--danger);
  border-radius: var(--radius-control);
  background: var(--surface);
}

.profiles-errors {
  margin: 0;
  padding-left: 18px;
  font-size: 11.5px;
  line-height: 1.5;
  color: var(--danger);
}

.profiles-diff {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-height: 0;
}

/* The diff scrolls inside its own pane, so a profile that changes 300 values cannot push
   the Load button off the screen. */
.profiles-diff-table {
  max-height: 340px;
  overflow-y: auto;
  display: block;
}

.profiles-diff-table td {
  max-width: 260px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

/* Below 1100px the list goes above the detail rather than squeezing both. */
@media (max-width: 1100px) {
  .profiles-body {
    grid-template-columns: minmax(0, 1fr);
    grid-template-rows: auto minmax(0, 1fr);
  }

  .profiles-list { max-height: 140px; }
}
""" % {'variables': _css_variables()}
