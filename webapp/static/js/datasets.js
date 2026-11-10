// SPDX-License-Identifier: AGPL-3.0-or-later
//
// ReadyArt Synthetic Dataset Generator
// Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
//
// All code here is AGPLv3, and has been for some time. Very early versions were MIT by
// Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
// it is unlikely any of that code survives. Unless you have the original MIT-era code to
// compare against, treat this file as AGPLv3-only. See docs/licensing.md.

/**
 * The Datasets panel -- Phase 6d-a.
 *
 * A two-pane screen: the source list on the left, the selected source's settings and column
 * mapping on the right.
 *
 * **No model logic lives here.** `webapp/datasets_routes.py` sends the sources already
 * normalized by `dataset_settings.normalize_config`, each with its own status line, and every
 * mutation goes back through a route that calls the model layer. That is deliberate: the shape of a
 * source is defined in one place, and a second copy in JS would drift from the one
 * `configure_queue_datasets` reads at run start -- the failure would be silent, a source that looks
 * right in the editor and binds nothing at run time.
 *
 * **The status line comes from the server.** Whether an import is stale is
 * `source_selector(source) != schema_selector`, which is the model's own rule; computing it here
 * would be a second implementation of the one rule that decides whether a mapping can be saved.
 *
 * **An import can take seconds.** The route returns the `selector` it read with, and a result is
 * dropped when the source's selector has moved on since -- the `token` + `selector` check. Without
 * it, editing a location while a slow read runs would
 * attach the old file's columns to the new location.
 *
 * **Phase 7 -- the column picker.** `dataset_column_menu.py` (445 lines) was a bounded, searchable
 * popup for choosing which columns are visible. Most of it is already here (the alias editor, show
 * all / show none, a chosen subset); what was missing is the part that makes a 40-column source
 * usable rather than a scroll: a **name filter**, and **invert** plus the two filter-scoped
 * selections. Those are client-side only -- the visibility route already takes an arbitrary column
 * list, so nothing about the model changed.
 *
 * **The filter never changes what a selection means.** "Show all" and "Show none" act on the whole
 * schema, exactly as `ColumnVisibilityMenu.select_all` / `clear_selection` do, because a control that
 * silently acted on the filtered subset would hide columns the user cannot see. The two filter-scoped
 * buttons are named for what they do ("Only matching", "Hide matching") and only exist while a
 * filter is typed.
 */

import { el, emptyState, num, toast } from './dom.js';

// --- state ---------------------------------------------------------------

const state = {
  data: null,          // the last GET /api/datasets payload
  selected: null,      // the selected source_id
  draft: null,         // unsaved field edits for the selected source
  importing: new Set(),// source_ids with a read in flight
  aliasColumn: null,   // the column whose alias is being edited
  aliasValue: '',
  columnFilter: '',    // the column-name filter (Phase 7)
  loading: false,
  mounted: false,
};

/** Fetch JSON, sending an expired session to the login page. */
async function getJSON(path) {
  const response = await fetch(path, { credentials: 'same-origin' });
  if (response.status === 401) {
    window.location.href = '/login';
    throw new Error('Session expired.');
  }
  const body = await response.json();
  if (!body.ok) throw new Error(body.error?.message || 'The request failed.');
  return body;
}

async function sendJSON(path, method, payload) {
  const response = await fetch(path, {
    method,
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {}),
  });
  if (response.status === 401) {
    window.location.href = '/login';
    throw new Error('Session expired.');
  }
  const body = await response.json();
  if (!body.ok) throw new Error(body.error?.message || 'The request failed.');
  return body;
}

// --- loading -------------------------------------------------------------

async function refresh() {
  state.loading = true;
  try {
    state.data = await getJSON('/api/datasets');
    const ids = (state.data.sources || []).map((source) => source.source_id);
    const previous = state.selected;
    if (!state.selected || !ids.includes(state.selected)) {
      state.selected = ids[0] || null;
    }
    // The filter is per source: carrying it across a source change would leave a
    // query that matches nothing, which reads as "this source has no columns".
    // A refresh of the *same* source keeps it, because every mutation ends in a
    // refresh and clearing the filter on each alias edit would be hostile.
    if (state.selected !== previous) state.columnFilter = '';
    state.draft = null;
    state.aliasColumn = null;
    state.aliasValue = '';
  } catch (error) {
    state.data = { error: String(error.message || error), sources: [], kinds: [] };
  } finally {
    state.loading = false;
    repaint();
  }
}

function currentSource() {
  if (!state.data) return null;
  return (state.data.sources || []).find((s) => s.source_id === state.selected) || null;
}

function statusFor(source) {
  if (!source) return { text: '', level: 'muted' };
  const list = state.data?.status || [];
  const index = (state.data.sources || []).findIndex(
    (s) => s.source_id === source.source_id);
  return list[index] || { text: '', level: 'muted' };
}

/** The value a field shows: the pending draft edit if there is one, else the server's. */
function fieldValue(source, key) {
  if (state.draft && key in state.draft) return state.draft[key];
  const value = source[key];
  return value === null || value === undefined ? '' : value;
}

function isDirty() {
  return Boolean(state.draft && Object.keys(state.draft).length);
}

// --- rendering -----------------------------------------------------------

function repaint() {
  const main = document.getElementById('main');
  if (!main) return;
  const current = main.querySelector('#screen-datasets');
  if (!current) return;
  current.replaceChildren(...screen().childNodes);
}

function screen() {
  const body = el('div', { class: 'datasets-body' });

  if (!state.data) {
    body.append(el('div', { class: 'empty' }, 'Loading…'));
  } else if (state.data.error) {
    body.append(emptyState({ title: 'Could not load the datasets', hint: state.data.error }));
  } else {
    body.append(listPane(), detailPane());
  }

  return el('div', { class: 'screen active', id: 'screen-datasets' },
    toolbar(),
    body,
  );
}

function toolbar() {
  const counts = state.data?.counts || {};
  return el('div', { class: 'tab-toolbar' },
    el('span', { class: 'section-header', style: { margin: '0' } }, 'Datasets'),
    el('span', { class: 'muted', id: 'datasets-count' },
      state.data?.counts
        ? `${num(counts.sources)} source(s) · ${num(counts.enabled)} enabled · `
          + `${num(counts.mapped)} alias(es) · ${num(counts.columns)} column(s)`
        : ''),
    el('span', { class: 'spacer' }),
    el('button', {
      type: 'button',
      class: 'btn-primary',
      id: 'datasets-save',
      disabled: isDirty() ? null : 'disabled',
      onclick: () => saveDraft(currentSource()),
    }, isDirty() ? `Save (${Object.keys(state.draft).length})` : 'Save'),
    el('button', {
      type: 'button',
      disabled: isDirty() ? null : 'disabled',
      onclick: () => { state.draft = null; repaint(); },
    }, 'Discard'),
    el('button', {
      type: 'button',
      onclick: () => validateAll(),
    }, 'Validate'),
    el('button', { type: 'button', onclick: () => refresh() }, 'Reload'),
  );
}

function listPane() {
  const list = el('div', { class: 'datasets-list' });
  const sources = state.data?.sources || [];

  if (!sources.length) {
    list.append(emptyState({
      title: 'No datasets yet',
      hint: 'A dataset binds a CSV, JSONL, Parquet or Hub source to the prompt aliases '
        + 'that read it. Add one to begin.',
    }));
  }

  sources.forEach((source, index) => {
    const status = statusFor(source);
    const button = el('button', {
      type: 'button',
      class: 'datasets-item',
      'aria-selected': String(source.source_id === state.selected),
      onclick: () => selectSource(source.source_id),
    },
      el('span', { class: 'datasets-item-name' },
        `${index + 1}. ${source.name || '(unnamed)'}`),
      el('span', { class: 'datasets-item-meta' },
        el('span', { class: `pill ${source.enabled ? 'ok' : 'muted'}` },
          source.enabled ? 'on' : 'off'),
        el('span', { class: 'muted mono' }, source.kind),
        el('span', { class: 'muted' },
          `${num((source.column_aliases || {}) ? Object.keys(source.column_aliases).length : 0)} mapped`),
        status.level === 'warn'
          ? el('span', { class: 'pill warn' }, 'stale')
          : null,
      ),
    );
    list.append(button);
  });

  return el('div', { class: 'datasets-list-pane' },
    el('div', { class: 'section-header' }, 'Sources'),
    list,
    el('div', { class: 'datasets-add' },
      el('button', { type: 'button', onclick: () => addSource() }, 'Add dataset'),
    ),
  );
}

function detailPane() {
  const source = currentSource();
  const pane = el('div', { class: 'datasets-detail' });

  if (!source) {
    pane.append(emptyState({
      title: 'No dataset selected',
      hint: 'Add a dataset, import its column names, then choose aliases for the columns '
        + 'you need.',
    }));
    return pane;
  }

  const status = statusFor(source);
  pane.append(
    el('div', { class: 'datasets-detail-head' },
      el('span', { class: 'section-header', style: { margin: '0' } },
        source.name || '(unnamed)'),
      el('span', { class: 'muted mono' }, source.source_id),
      el('span', { class: 'spacer' }),
      el('button', {
        type: 'button',
        class: 'btn-danger',
        onclick: () => removeSource(source),
      }, 'Remove'),
    ),
    el('p', { class: `datasets-status ${status.level}` }, status.text),
    settingsGrid(source),
    columnSection(source),
  );
  return pane;
}

/** The seven editable fields, with the kind's own enablement rules. */
function settingsGrid(source) {
  const grid = el('div', { class: 'datasets-settings' });
  const kind = fieldValue(source, 'kind');
  const allowed = kind === 'huggingface'
    ? ['config', 'split', 'revision']
    : kind === 'hf_disk' ? ['split'] : [];

  grid.append(fieldRow(source, 'name', 'Name', 'text'));
  grid.append(fieldRow(source, 'enabled', 'Enabled', 'bool'));
  grid.append(kindRow(source));
  grid.append(fieldRow(source, 'location', 'Location', 'text',
    kind === 'huggingface'
      ? 'owner/dataset — a Hub repository ID, not a URL or a file path.'
      : kind === 'hf_disk'
        ? 'A directory saved by Hugging Face `save_to_disk`.'
        : 'A path to the file on this machine.'));

  // A selector field the kind does not accept is not shown at all, rather than
  // shown disabled: a disabled input invites the question "why can I not type
  // here", and the answer is "this kind has no such setting".
  for (const key of ['config', 'split', 'revision']) {
    if (!allowed.includes(key)) continue;
    grid.append(fieldRow(source, key, labelFor(key), 'text', helpFor(key)));
  }
  return grid;
}

function labelFor(key) {
  return { config: 'Hub config', split: 'Split', revision: 'Revision' }[key] || key;
}

function helpFor(key) {
  return {
    config: 'Only for a Hub source that has more than one configuration.',
    split: 'Which split to read. Applies to Hub and saved-dataset sources.',
    revision: 'A branch, tag or commit. Empty means the default branch.',
  }[key] || '';
}

function kindRow(source) {
  const kinds = state.data?.kinds || [];
  const current = String(fieldValue(source, 'kind') || 'csv');
  const options = kinds.map((entry) => el('option', {
    value: entry.kind,
    selected: entry.kind === current ? 'selected' : null,
  }, entry.label));
  if (current && !kinds.some((entry) => entry.kind === current)) {
    options.push(el('option', { value: current, selected: 'selected' },
      `${current} (not a known kind)`));
  }
  return el('div', { class: 'datasets-row' },
    el('label', { class: 'datasets-label' }, 'Kind'),
    el('div', { class: 'datasets-control' },
      el('select', {
        onchange: (event) => setDraft('kind', event.target.value, true),
      }, ...options),
      el('p', { class: 'config-help' },
        'The reader used for this source. Changing it clears the imported columns.'),
    ),
  );
}

function fieldRow(source, key, label, kind, help) {
  const value = fieldValue(source, key);
  let control;

  if (kind === 'bool') {
    control = el('label', { class: 'config-check' },
      el('input', {
        type: 'checkbox',
        checked: value ? 'checked' : null,
        onchange: (event) => setDraft(key, event.target.checked, true),
      }),
      el('span', {}, value ? 'on' : 'off'));
  } else {
    control = el('input', {
      type: 'text',
      value: value === null || value === undefined ? '' : String(value),
      oninput: (event) => setDraft(key, event.target.value),
    });
  }

  return el('div', { class: 'datasets-row' },
    el('label', { class: 'datasets-label' }, label),
    el('div', { class: 'datasets-control' },
      control,
      help ? el('p', { class: 'config-help' }, help) : null,
    ),
  );
}

/** The columns a filter query matches, in schema order. Case-insensitive substring. */
function matchesFilter(name, query) {
  if (!query) return true;
  return name.toLowerCase().includes(query.toLowerCase());
}

/** The set of currently visible names, or `null` for "all". */
function visibleSet(source) {
  const visible = source.visible_columns;
  if (visible === null || visible === undefined) return null;
  return new Set(visible);
}

/** The column table: filter, visibility, alias, and the import button. */
function columnSection(source) {
  const section = el('div', { class: 'datasets-columns' });
  const columns = source.columns || [];
  const aliases = source.column_aliases || {};
  const visible = source.visible_columns;
  const shown = visible === null || visible === undefined
    ? columns
    : columns.filter((name) => visible.includes(name));
  const query = state.columnFilter.trim();
  const matching = shown.filter((name) => matchesFilter(name, query));

  const importing = state.importing.has(source.source_id);
  section.append(el('div', { class: 'datasets-columns-head' },
    el('span', { class: 'section-header', style: { margin: '0' } }, 'Columns'),
    el('span', { class: 'muted', id: 'datasets-column-count' },
      columns.length
        ? `${num(shown.length)} shown of ${num(columns.length)} · `
          + `${num(Object.keys(aliases).length)} mapped`
          + (query ? ` · ${num(matching.length)} match` : '')
        : 'not imported'),
    el('span', { class: 'spacer' }),
    el('button', {
      type: 'button',
      disabled: importing ? 'disabled' : null,
      onclick: () => importColumns(source),
    }, importing ? 'Reading…' : 'Import columns'),
  ));

  if (!columns.length) {
    section.append(el('p', { class: 'config-help' },
      'Import column names to create mappings. Dataset records are never '
      + 'displayed, and nothing is read until you press Import.'));
    return section;
  }

  // The filter. It is client-side only: the visibility route already accepts an
  // arbitrary column list, so a 40-column source needs no new route to be
  // filterable. `type="search"` gives the browser's own clear button for free.
  section.append(el('div', { class: 'datasets-column-filter' },
    el('input', {
      type: 'search',
      id: 'datasets-column-search',
      value: state.columnFilter,
      placeholder: 'Filter column names…',
      'aria-label': 'Filter column names',
      oninput: (event) => {
        state.columnFilter = event.target.value;
        repaintKeepingFilter();
      },
      onkeydown: (event) => {
        if (event.key === 'Escape') {
          state.columnFilter = '';
          repaintKeepingFilter();
        }
      },
    }),
  ));

  const table = el('table', { class: 'grid datasets-columns-table' },
    el('thead', {},
      el('tr', {},
        el('th', {}, 'Column'),
        el('th', {}, 'Alias'),
        el('th', {}, 'Placeholder'),
      ),
    ),
  );
  const body = el('tbody', { id: 'datasets-column-rows' });
  if (!matching.length) {
    // An empty filter result is a state, not a blank table: a blank table reads
    // as "this source has no columns", which is a different and alarming thing.
    body.append(el('tr', { class: 'datasets-no-match' },
      el('td', { colspan: '3' },
        `No column matches “${query}”. `
        + `${num(shown.length)} column(s) are shown.`)));
  }
  for (const name of matching) {
    const alias = aliases[name] || '';
    const row = el('tr', {
      class: state.aliasColumn === name ? 'selected' : null,
      onclick: () => selectColumn(name, alias),
    },
      el('td', { class: 'mono' }, name),
      el('td', { class: 'mono' }, alias || '—'),
      el('td', { class: 'mono muted' },
        alias ? `{{ds:${alias}}}` : '—'),
    );
    body.append(row);
  }
  table.append(body);
  section.append(table);

  // The alias editor. It is separate from the table because a mapping is applied
  // one column at a time, and an empty alias *removes* the mapping rather than
  // storing an empty string.
  if (state.aliasColumn) {
    const input = el('input', {
      type: 'text',
      value: state.aliasValue,
      placeholder: 'alias (letters, digits, underscore)',
      oninput: (event) => { state.aliasValue = event.target.value; },
      onkeydown: (event) => {
        if (event.key === 'Enter') applyAlias(source);
        if (event.key === 'Escape') { state.aliasColumn = null; repaint(); }
      },
    });
    section.append(el('div', { class: 'datasets-alias-editor' },
      el('span', { class: 'muted' }, `Alias for ${state.aliasColumn}`),
      input,
      el('button', { type: 'button', onclick: () => applyAlias(source) }, 'Apply'),
      el('button', {
        type: 'button',
        onclick: () => clearAlias(source),
      }, 'Clear'),
      el('button', {
        type: 'button',
        onclick: () => copyPlaceholder(source),
      }, 'Copy placeholder'),
    ));
  }

  section.append(visibilityRow(source, columns, visible, query, matching));
  return section;
}

/**
 * Show all / none / invert, plus the two filter-scoped selections.
 *
 * **Show all and Show none act on the whole schema, never on the filtered
 * subset.** That is `ColumnVisibilityMenu.select_all` / `clear_selection`'s own
 * rule, and the reason is that a control which silently acted on a subset would
 * hide columns the user cannot see. The filter-scoped buttons say so in their
 * labels and only appear while a filter is typed.
 *
 * Never touches an alias: visibility is presentation only, and a hidden column
 * keeps its mapping active.
 */
function visibilityRow(source, columns, visible, query, matching) {
  const all = visible === null || visible === undefined;
  const current = visibleSet(source);
  const everyShown = all || (current && current.size === columns.length);
  const noneShown = current !== null && current.size === 0;

  // Invert: every column not currently visible becomes visible, and vice versa.
  // Computed against the whole schema, in schema order, so the result is stable.
  const inverted = columns.filter((name) => (current ? !current.has(name) : false));

  // "Only matching" keeps exactly the filtered columns; "Hide matching" removes
  // them from the current selection. Both are no-ops with no filter, so they are
  // not rendered at all until one is typed.
  const onlyMatching = () => setVisibility(source, matching);
  const hideMatching = () => {
    const keep = columns.filter((name) => !matchesFilter(name, query));
    return setVisibility(source, keep);
  };

  return el('div', { class: 'datasets-visibility' },
    el('span', { class: 'muted' },
      all ? 'All columns are shown.' : `${num(visible.length)} of `
        + `${num(columns.length)} shown. Hiding a column keeps its alias.`),
    el('span', { class: 'spacer' }),
    query ? el('button', {
      type: 'button',
      title: `Show only the ${num(matching.length)} column(s) matching “${query}”.`,
      disabled: matching.length ? null : 'disabled',
      onclick: onlyMatching,
    }, `Only matching (${num(matching.length)})`) : null,
    query ? el('button', {
      type: 'button',
      title: `Hide the ${num(matching.length)} column(s) matching “${query}”.`,
      disabled: matching.length ? null : 'disabled',
      onclick: hideMatching,
    }, 'Hide matching') : null,
    el('button', {
      type: 'button',
      title: 'Show every column, including ones imported later.',
      disabled: everyShown ? 'disabled' : null,
      onclick: () => setVisibility(source, null),
    }, 'Show all'),
    el('button', {
      type: 'button',
      title: 'Hide every column. Aliases are kept.',
      disabled: noneShown ? 'disabled' : null,
      onclick: () => setVisibility(source, []),
    }, 'Show none'),
    el('button', {
      type: 'button',
      title: 'Swap shown and hidden across every column.',
      disabled: noneShown ? 'disabled' : null,
      onclick: () => setVisibility(source, inverted),
    }, 'Invert'),
  );
}

// --- actions -------------------------------------------------------------

function selectSource(sourceId) {
  if (sourceId === state.selected) return;
  if (isDirty()) {
    toast('Save or discard the pending change first.', 'warn');
    return;
  }
  state.selected = sourceId;
  state.draft = null;
  state.aliasColumn = null;
  state.aliasValue = '';
  // The filter is per source: a query that matched nothing in the old source
  // would read as "this source has no columns" in the new one.
  state.columnFilter = '';
  repaint();
}

function setDraft(key, value, fullRepaint = false) {
  if (!state.draft) state.draft = {};
  state.draft[key] = value;
  // Only the toolbar is repainted by default, never the field the user is typing
  // in: a full repaint replaces the input node, so the caret would jump to the
  // end of the field on every keystroke. A control with no caret to lose (a
  // checkbox, or the kind select, which changes which fields exist) asks for the
  // full repaint instead.
  if (fullRepaint) repaint();
  else updateToolbar();
}

/**
 * Repaint the screen and put the caret back in the filter box.
 *
 * A full repaint replaces the input node, so without this the caret jumps out of
 * the field on the first keystroke and the filter is unusable. The value is
 * restored from `state.columnFilter`, so the text itself survives the repaint.
 */
function repaintKeepingFilter() {
  repaint();
  const input = document.getElementById('datasets-column-search');
  if (!input) return;
  input.focus();
  const end = input.value.length;
  try {
    input.setSelectionRange(end, end);
  } catch (error) {
    // `setSelectionRange` throws on some input types in some browsers; the
    // focus is what matters, and the caret lands at the end by default.
  }
}

function updateToolbar() {
  const current = document.getElementById('screen-datasets');
  if (!current) return;
  const existing = current.querySelector('.tab-toolbar');
  if (existing) existing.replaceWith(toolbar());
}

async function addSource() {
  try {
    const result = await sendJSON('/api/datasets/sources', 'POST', {});
    const source = result.data?.source;
    state.selected = source?.source_id || state.selected;
    state.draft = null;
    toast('Dataset added. Set its location, then import its columns.', 'ok');
    await refresh();
  } catch (error) {
    toast(String(error.message || error), 'danger');
  }
}

async function removeSource(source) {
  if (!window.confirm(
    `Remove "${source.name || source.source_id}"? `
    + 'Rows already generated from it are not deleted.')) return;
  try {
    await sendJSON(`/api/datasets/sources/${encodeURIComponent(source.source_id)}`,
      'DELETE', {});
    state.draft = null;
    toast('Dataset removed.', 'ok');
    await refresh();
  } catch (error) {
    toast(String(error.message || error), 'danger');
  }
}

/** Save the pending field edits, then re-read the source's status. */
async function saveDraft(source) {
  if (!isDirty()) return;
  const fields = { ...state.draft };
  try {
    await sendJSON(`/api/datasets/sources/${encodeURIComponent(source.source_id)}`,
      'PATCH', { fields });
    state.draft = null;
    toast('Dataset settings saved.', 'ok');
    await refresh();
  } catch (error) {
    toast(String(error.message || error), 'danger');
  }
}

function selectColumn(name, alias) {
  state.aliasColumn = name;
  state.aliasValue = alias || '';
  repaint();
}

async function applyAlias(source) {
  const column = state.aliasColumn;
  if (!column) return;
  // A pending field edit has to be written first: an alias is validated against
  // the source's *current* settings, so mapping a column on a location that is
  // not saved yet would validate against the old location.
  if (isDirty()) await saveDraft(source);
  try {
    await sendJSON(
      `/api/datasets/sources/${encodeURIComponent(source.source_id)}`
      + `/aliases/${encodeURIComponent(column)}`,
      'PUT', { alias: state.aliasValue });
    toast(state.aliasValue.trim() ? 'Alias saved.' : 'Alias mapping cleared.', 'ok');
    state.aliasColumn = null;
    state.aliasValue = '';
    await refresh();
  } catch (error) {
    toast(String(error.message || error), 'danger');
  }
}

async function clearAlias(source) {
  state.aliasValue = '';
  await applyAlias(source);
}

async function copyPlaceholder(source) {
  const alias = state.aliasValue.trim();
  if (!alias) {
    toast('Type an alias first.', 'warn');
    return;
  }
  const text = `{{ds:${alias}}}`;
  try {
    await navigator.clipboard.writeText(text);
    toast(`Copied ${text}`, 'ok');
  } catch (error) {
    toast(`Could not copy: ${text}`, 'danger');
  }
}

async function setVisibility(source, selection) {
  if (isDirty()) await saveDraft(source);
  try {
    await sendJSON(
      `/api/datasets/sources/${encodeURIComponent(source.source_id)}/visibility`,
      'PUT', { columns: selection });
    // The message names what happened, because the same route now serves five
    // different intentions and "All columns shown" after Invert would be a lie.
    const total = (source.columns || []).length;
    let message;
    if (selection === null) message = 'All columns shown.';
    else if (!selection.length) message = 'No columns shown.';
    else if (selection.length === total) message = 'All columns shown.';
    else message = `${num(selection.length)} of ${num(total)} columns shown.`;
    toast(message, 'ok');
    await refresh();
  } catch (error) {
    toast(String(error.message || error), 'danger');
  }
}

async function importColumns(source) {
  if (isDirty()) await saveDraft(source);
  state.importing.add(source.source_id);
  repaint();
  try {
    const result = await sendJSON(
      `/api/datasets/sources/${encodeURIComponent(source.source_id)}/import`,
      'POST', {});
    const columns = result.data?.columns || [];
    toast(`Imported ${num(columns.length)} column name(s).`, 'ok');
  } catch (error) {
    toast(String(error.message || error), 'danger');
  } finally {
    state.importing.delete(source.source_id);
    await refresh();
  }
}

async function validateAll() {
  try {
    await sendJSON('/api/datasets/validate', 'POST', {});
    toast('Datasets are valid. A run will bind these sources.', 'ok');
  } catch (error) {
    toast(String(error.message || error), 'danger');
  }
}

// --- the tab -------------------------------------------------------------

export function renderDatasets() {
  if (!state.mounted) {
    state.mounted = true;
    refresh();
  }
  return screen();
}

export function unmountDatasets() {
  // A pending field edit is kept, so switching tabs and coming back does not lose
  // typing. A reload clears it, which is honest: the file is the truth.
  state.mounted = false;
}

export const DATASETS_TAB = {
  id: 'datasets',
  label: 'Datasets',
  render: renderDatasets,
  unmount: unmountDatasets,
};
