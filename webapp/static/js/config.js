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
 * The Config editor -- one generic renderer over the whole config tree (Phase 6b).
 *
 * **This file contains no type logic.** The server pre-walks the tree and sends each node with
 * its own `kind` (`webapp/config_routes.py`), so the renderer only has to switch on a string it
 * was handed. That is deliberate: `config_tree.kind_of` is the one implementation of "what is
 * this value", it is tested, and a second copy here would drift from it.
 *
 * **Every leaf renders, schema or not.** A leaf with no schema entry still arrives with a kind
 * inferred from its value, so it still gets a field. This is what makes "all options can be
 * modified" true rather than aspirational. The schema (`webapp/config_schema.py`)
 * only *decorates*: a label, a help line, a choice list and a range. It never adds or removes a
 * field, so a path with no entry degrades to the generic render.
 *
 * **The tree is the storage model.** A save sends only the paths that changed, and the server
 * applies them to a freshly loaded tree and writes the whole tree back, so a key the editor does
 * not know about is carried through untouched, so no key is ever dropped by a save.
 *
 * **A secret is write-only.** `api.apis.*.key` and friends arrive as `***` with a `has_value`
 * flag. Leaving a masked field alone leaves the stored key alone, because the server puts the
 * real value back wherever it still sees `***`.
 */

import { el, emptyState, num, toast } from './dom.js';

// --- state ---------------------------------------------------------------

const state = {
  data: null,          // the last GET /api/config payload
  section: null,       // the selected section key
  edits: new Map(),    // path -> value, only what the user actually changed
  errors: new Map(),   // path -> message, from a rejected save
  loading: false,
  mounted: false,
  saving: false,
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

async function postJSON(path, payload) {
  const response = await fetch(path, {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {}),
  });
  if (response.status === 401) {
    window.location.href = '/login';
    throw new Error('Session expired.');
  }
  const body = await response.json();
  if (!body.ok) {
    // A rejected save names every bad field, so all of them can be highlighted at once.
    const error = new Error(body.error?.message || 'The request failed.');
    error.fields = body.error?.fields || {};
    throw error;
  }
  return body;
}

// --- loading -------------------------------------------------------------

async function refresh() {
  state.loading = true;
  try {
    state.data = await getJSON('/api/config');
    state.errors.clear();
    // Keep the current section if it still exists, else open the first one.
    const keys = (state.data.sections || []).map((entry) => entry.key);
    if (!state.section || !keys.includes(state.section)) {
      state.section = keys[0] || null;
    }
  } catch (error) {
    state.data = { error: String(error.message || error), sections: [], nodes: [] };
  } finally {
    state.loading = false;
    repaint();
  }
}

/** The node for the selected section, or null. */
function currentSection() {
  if (!state.data) return null;
  return (state.data.nodes || []).find((node) => node.path === state.section) || null;
}

// --- value helpers -------------------------------------------------------

/** The value a field should show: the pending edit if there is one, else the server's. */
function valueOf(node) {
  if (state.edits.has(node.path)) return state.edits.get(node.path);
  // A `list` node carries `items`, not `value` -- the server collapses a flat list into one
  // control, so its entries arrive as an array rather than as a scalar. Reading `value` here
  // would render every list empty, which is exactly what happened before this branch existed.
  if (node.kind === 'list') return node.items || [];
  return node.value;
}

function setEdit(path, value) {
  state.edits.set(path, value);
  state.errors.delete(path);
  updateToolbar();
}

function isDirty() {
  return state.edits.size > 0;
}

/** Count the leaves under a node, so the nav can show a size. */
function countLeaves(node) {
  if (!node) return 0;
  if (node.kind === 'group') {
    return (node.children || []).reduce((total, child) => total + countLeaves(child), 0);
  }
  if (node.kind === 'list') return (node.items || []).length || 1;
  return 1;
}

// --- rendering -----------------------------------------------------------

function repaint() {
  const main = document.getElementById('main');
  if (!main) return;
  const current = main.querySelector('#screen-config');
  if (!current) return;
  current.replaceChildren(...screen().childNodes);
}

function screen() {
  const body = el('div', { class: 'config-body' });

  if (!state.data) {
    body.append(el('div', { class: 'empty' }, 'Loading…'));
  } else if (state.data.error) {
    body.append(emptyState({ title: 'Could not load the config', hint: state.data.error }));
  } else {
    body.append(
      navPane(),
      fieldsPane(),
    );
  }

  return el('div', { class: 'screen active', id: 'screen-config' },
    toolbar(),
    body,
  );
}

function toolbar() {
  const dirty = isDirty();
  const saveButton = el('button', {
    class: 'btn-primary',
    type: 'button',
    id: 'config-save',
    disabled: !dirty || state.saving,
    onclick: () => save(),
  }, state.saving ? 'Saving…' : `Save${dirty ? ` (${state.edits.size})` : ''}`);

  const discard = el('button', {
    type: 'button',
    disabled: !dirty || state.saving,
    onclick: () => {
      state.edits.clear();
      state.errors.clear();
      repaint();
    },
  }, 'Discard');

  const total = state.data?.leaf_count;
  const secrets = state.data?.secret_count;

  return el('div', { class: 'tab-toolbar' },
    el('span', { class: 'section-header', style: { margin: '0' } }, 'Config'),
    el('span', { class: 'muted', id: 'config-count' },
      total === undefined ? '' :
        `${num(total)} option(s) · ${num(secrets)} secret(s) · ${num(state.edits.size)} changed`),
    el('span', { class: 'spacer' }),
    discard,
    saveButton,
    el('button', { type: 'button', onclick: () => refresh() }, 'Reload'),
  );
}

function updateToolbar() {
  const current = document.getElementById('screen-config');
  if (!current) return;
  const replacement = toolbar();
  const existing = current.querySelector('.tab-toolbar');
  if (existing) existing.replaceWith(replacement);
}

function navPane() {
  const list = el('div', { class: 'config-nav' });
  for (const section of state.data.sections || []) {
    const button = el('button', {
      type: 'button',
      class: 'config-nav-item',
      'aria-selected': String(section.key === state.section),
      onclick: () => {
        state.section = section.key;
        repaint();
      },
    },
      el('span', { class: 'config-nav-label' }, section.label),
      el('span', { class: 'config-nav-count' }, num(section.leaves)),
    );
    list.append(button);
  }
  return el('div', { class: 'config-nav-pane' },
    el('div', { class: 'section-header' }, 'Sections'),
    list,
    addForm(),
  );
}

/**
 * The "add an option" form.
 *
 * Adding is the same operation as editing: the server creates the intermediate mappings
 * a path needs, so a path that does not exist yet is a valid target. It lives under the nav
 * rather than in the field list because it is not a field -- it is how a new one comes to exist.
 */
function addForm() {
  const pathInput = el('input', {
    type: 'text',
    placeholder: 'section.key.subkey',
    'aria-label': 'New option path',
  });
  const valueInput = el('input', {
    type: 'text',
    placeholder: 'value',
    'aria-label': 'New option value',
  });

  async function add() {
    const path = pathInput.value.trim();
    if (!path) return;
    if (!path.includes('.')) {
      toast('A new option needs a section: section.key', 'danger');
      return;
    }
    let value = valueInput.value;
    // A typed value that reads as a number or a boolean is stored as one, so a new numeric
    // option is not silently a string. The server coerces against the existing type when the
    // path is known; for a new path this is the only chance to get it right.
    if (/^-?\d+$/.test(value)) value = parseInt(value, 10);
    else if (/^-?\d*\.\d+$/.test(value)) value = parseFloat(value);
    else if (value === 'true') value = true;
    else if (value === 'false') value = false;

    state.edits.set(path, value);
    pathInput.value = '';
    valueInput.value = '';
    toast(`Added ${path} to the pending changes. Press Save to write it.`, 'info');
    updateToolbar();
    // Show the section the new key belongs to, so the user can see it land.
    const section = path.split('.')[0];
    if ((state.data.sections || []).some((entry) => entry.key === section)) {
      state.section = section;
    }
    repaint();
  }

  return el('div', { class: 'config-add' },
    el('div', { class: 'section-header' }, 'Add an option'),
    pathInput,
    valueInput,
    el('button', { type: 'button', onclick: add }, 'Add'),
    el('p', { class: 'config-help' },
      'A path that does not exist yet is created. Press Save to write it to config.yml.'),
  );
}

function fieldsPane() {
  const node = currentSection();
  const pane = el('div', { class: 'config-fields' });
  if (!node) {
    pane.append(emptyState({
      title: 'No section selected',
      hint: 'Pick a section on the left to edit its options.',
    }));
    return pane;
  }
  pane.append(el('div', { class: 'config-fields-head' },
    el('span', { class: 'section-header', style: { margin: '0' } }, node.label),
    el('span', { class: 'muted mono' }, node.path),
    el('span', { class: 'spacer' }),
    el('span', { class: 'muted' }, `${num(countLeaves(node))} option(s)`),
  ));
  const grid = el('div', { class: 'config-grid' });
  if (node.kind === 'group') {
    for (const child of node.children || []) {
      grid.append(...fieldRows(child, 0));
    }
  } else {
    // A section that is a bare scalar, not a mapping -- `gender` is one in the shipped file.
    // Without this branch it would render an empty pane and the option would be unreachable.
    grid.append(...fieldRows(node, 0));
  }
  pane.append(grid);
  return pane;
}

/**
 * One node as one or more rows: a label column and a control column.
 *
 * A group becomes a sub-heading plus its children indented under it, which is what makes
 * `api.apis.0.url` readable as "API slots › #1 › URL" without a breadcrumb per field.
 */
function fieldRows(node, depth) {
  if (node.kind === 'group') {
    const rows = [el('div', {
      class: `config-group-head depth-${Math.min(depth, 3)}`,
      title: node.path,
    },
      el('span', { class: 'config-group-label' }, node.label),
      el('span', { class: 'muted mono' }, node.path),
    )];
    for (const child of node.children || []) {
      rows.push(...fieldRows(child, depth + 1));
    }
    return rows;
  }
  return [fieldRow(node, depth)];
}

function fieldRow(node, depth) {
  const control = controlFor(node);
  const error = state.errors.get(node.path);
  const changed = state.edits.has(node.path);
  const row = el('div', {
    class: `config-row depth-${Math.min(depth, 3)}`
      + (changed ? ' changed' : '') + (error ? ' invalid' : ''),
    dataset: { path: node.path },
  },
    el('label', { class: 'config-label', title: node.path },
      el('span', { class: 'config-label-text' }, node.label),
      el('span', { class: 'config-path mono' }, node.path),
    ),
    el('div', { class: 'config-control' },
      control,
      error ? el('p', { class: 'config-error' }, error) : null,
      el('p', { class: 'config-help' }, helpFor(node)),
    ),
  );
  return row;
}

/** The control for one leaf, chosen by the `kind` the server sent. */
function controlFor(node) {
  const value = valueOf(node);

  if (node.kind === 'bool') {
    const input = el('input', {
      type: 'checkbox',
      checked: value ? 'checked' : null,
      onchange: (event) => setEdit(node.path, event.target.checked),
    });
    return el('label', { class: 'config-check' }, input,
      el('span', {}, value ? 'on' : 'off'));
  }

  if (node.kind === 'number') {
    // The schema's bounds become the input's own, so the browser refuses an out-of-range value
    // before it is ever sent. The server still checks it: `min`/`max` on an <input> is a
    // convenience, not a guarantee, and a request can be made without a browser.
    return el('input', {
      type: 'number',
      step: 'any',
      min: node.min === undefined ? null : String(node.min),
      max: node.max === undefined ? null : String(node.max),
      value: value === null || value === undefined ? '' : String(value),
      oninput: (event) => {
        const raw = event.target.value;
        // An empty number field is a pending edit of '', which the server rejects with a
        // message naming the field -- better than silently storing 0.
        setEdit(node.path, raw === '' ? '' : Number(raw));
      },
    });
  }

  if (node.kind === 'enum') {
    // A fixed choice list from the schema. A stored value that is not in the list is kept as an
    // extra option rather than silently rewritten to the first one, so a hand-edited config is
    // shown honestly and the server's validator can reject it on save.
    const choices = node.choices || [];
    const current = value === null || value === undefined ? '' : String(value);
    const options = choices.map((choice) => el('option', {
      value: choice,
      selected: choice === current ? 'selected' : null,
    }, choice));
    if (current && !choices.includes(current)) {
      options.push(el('option', { value: current, selected: 'selected' },
        `${current} (not a known choice)`));
    }
    return el('select', {
      onchange: (event) => setEdit(node.path, event.target.value),
    }, ...options);
  }

  if (node.kind === 'secret') {
    // Masked and write-only. `has_value` is all the browser is told; typing replaces it.
    const input = el('input', {
      type: 'password',
      value: '',
      placeholder: node.has_value ? '•••••••• (set)' : 'not set',
      autocomplete: 'new-password',
      oninput: (event) => setEdit(node.path, event.target.value),
    });
    return el('div', { class: 'config-secret' }, input,
      el('span', { class: `pill ${node.has_value ? 'ok' : 'muted'}` },
        node.has_value ? 'set' : 'not set'));
  }

  if (node.kind === 'list') {
    // A flat list is one textarea, one item per line. For `prompts.random_names.names` that is
    // 1,000 lines -- which is genuinely the usable shape for a seed list, and it makes bulk
    // paste and export trivial. A paged row editor is 6d.
    const text = (value || []).join('\n');
    return el('textarea', {
      class: 'config-list',
      rows: String(Math.min(14, Math.max(4, (value || []).length + 1))),
      spellcheck: 'false',
      oninput: (event) => setEdit(node.path, event.target.value.split('\n')),
    }, text);
  }

  if (node.kind === 'textarea') {
    return el('textarea', {
      rows: '6',
      spellcheck: 'false',
      oninput: (event) => setEdit(node.path, event.target.value),
    }, String(value ?? ''));
  }

  return el('input', {
    type: 'text',
    value: value === null || value === undefined ? '' : String(value),
    oninput: (event) => setEdit(node.path, event.target.value),
  });
}

/**
 * The help line under a field.
 *
 * The schema's own `help` text when it has one, else the generic answer: the value's type and,
 * for a list, how many entries it holds. A list always appends its count, because the count is
 * the one thing that changes as the user edits and the schema cannot know it.
 */
function helpFor(node) {
  if (node.kind === 'list') {
    const count = (valueOf(node) || []).length;
    const counted = `One entry per line · ${num(count)} entr${count === 1 ? 'y' : 'ies'}`;
    return node.help ? `${node.help} · ${counted}` : counted;
  }
  if (node.help) return node.help;
  if (node.kind === 'secret') {
    return 'Write-only. Leave blank to keep the stored value.';
  }
  if (node.kind === 'bool') return 'Yes or no.';
  if (node.kind === 'number') return 'A number.';
  if (node.kind === 'enum') return 'Pick one.';
  if (node.kind === 'textarea') return 'Multi-line text.';
  return 'Text.';
}

// --- saving --------------------------------------------------------------

async function save() {
  if (!isDirty() || state.saving) return;
  state.saving = true;
  state.errors.clear();
  updateToolbar();

  const values = {};
  for (const [path, value] of state.edits) values[path] = value;

  try {
    const result = await postJSON('/api/config/save', { values });
    const data = result.data || {};
    state.edits.clear();
    toast(
      `Saved ${num((data.changed || []).length)} change(s)`
      + ((data.created || []).length ? ` · ${num(data.created.length)} new` : '')
      + (data.reloaded ? '' : ' · restart to apply'),
      'ok');
    state.saving = false;
    await refresh();
  } catch (error) {
    state.saving = false;
    const fields = error.fields || {};
    for (const [path, message] of Object.entries(fields)) {
      state.errors.set(path, message);
    }
    if (!Object.keys(fields).length) {
      toast(String(error.message || error), 'danger');
    } else {
      toast(`${Object.keys(fields).length} field(s) rejected. Nothing was saved.`, 'danger');
    }
    repaint();
  }
}

// --- the tab -------------------------------------------------------------

export function renderConfig() {
  if (!state.mounted) {
    state.mounted = true;
    refresh();
  }
  return screen();
}

export function unmountConfig() {
  // A pending edit is kept, so switching tabs and coming back does not lose typing. A reload
  // clears it, which is the honest behaviour: the file is the truth.
  state.mounted = false;
}

export const CONFIG_TAB = { id: 'config', label: 'Config', render: renderConfig, unmount: unmountConfig };
