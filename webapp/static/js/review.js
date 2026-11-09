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
 * The Review tab.
 *
 * Layout decisions, and they are not decoration:
 *
 * - **Two-pane split at `1fr / 1.15fr`**: the flagged list on the left, the conversation
 *   detail on the right. The detail is slightly wider because it holds prose.
 * - **Selecting a row must not navigate away.** The list and the detail are both on screen
 *   at once, and a click only re-renders the detail pane.
 * - **The score breakdown is a footer strip** on the detail pane (Coherence /
 *   Character / Prose / Overall), colour-coded by threshold. This is what makes "why was
 *   this flagged" answerable at a glance instead of by reading a wall of numbers.
 *
 * The three actions are deliberately distinct:
 *
 * - **Export** copies the flagged rows elsewhere. It never changes the dataset.
 * - **Dismiss** clears the flags. It never changes the dataset.
 * - **Drop** removes ONE conversation from the dataset. It is the only destructive one, so
 *   it acts on the selection only and asks first.
 */

import { el, emptyState, num, toast } from './dom.js';

// The four dimensions shown in the footer strip, in the Tk order. The payload's
// `dimensions` is keyed by the scoring dimension names; a missing one shows as a dash.
const FOOTER_DIMENSIONS = [
  { key: 'coherence', label: 'Coherence' },
  { key: 'character', label: 'Character' },
  { key: 'prose', label: 'Prose' },
  { key: 'overall', label: 'Overall' },
];

const state = {
  rows: [],
  counts: { flagged: 0, dropped_by_judge: 0, dropped_by_user: 0 },
  filter: '',
  selected: null,
  detail: null,
  loading: false,
  error: null,
};

let mounted = false;

export function renderReview(appState) {
  // The tab is re-rendered on every stats tick, so the selection and the loaded detail
  // must live at module scope: rebuilding them on each tick would reset the reader's place
  // in a conversation, which is the v11.4 bug the Tk tab records.
  if (!mounted) {
    mounted = true;
    refresh();
  }
  return screen();
}

/** Re-fetch the flagged list. Called on mount, on Refresh, and after an action. */
async function refresh() {
  state.loading = true;
  try {
    const url = state.filter
      ? `/api/review?filter=${encodeURIComponent(state.filter)}`
      : '/api/review';
    const response = await fetch(url, { credentials: 'same-origin' });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    const body = await response.json();
    if (!body.ok) throw new Error(body.error?.message || 'Could not load the list.');
    state.rows = body.rows || [];
    state.counts = body.counts || state.counts;
    state.error = null;
    // Keep the selection if the row is still there; otherwise open the first row so
    // the detail pane is never an empty box on arrival.
    if (!state.rows.some((row) => row.task_id === state.selected)) {
      state.selected = state.rows.length ? state.rows[0].task_id : null;
      state.detail = null;
      if (state.selected) await loadDetail(state.selected);
    }
  } catch (error) {
    state.error = String(error.message || error);
  } finally {
    state.loading = false;
    repaint();
  }
}

async function loadDetail(taskId) {
  state.detail = null;
  repaint();
  try {
    const response = await fetch(`/api/review/${encodeURIComponent(taskId)}`,
      { credentials: 'same-origin' });
    const body = await response.json();
    if (!body.ok) throw new Error(body.error?.message || 'Could not load it.');
    // A slower response for a row the reader has already left must not overwrite the
    // detail they are now reading.
    if (state.selected !== taskId) return;
    state.detail = body.detail;
  } catch (error) {
    if (state.selected !== taskId) return;
    state.detail = { task_id: taskId, turns: [], note: String(error.message || error) };
  }
  repaint();
}

async function select(taskId) {
  if (state.selected === taskId) return;
  state.selected = taskId;
  await loadDetail(taskId);
}

async function act(path, body, confirmText) {
  if (confirmText && !window.confirm(confirmText)) return;
  try {
    const response = await fetch(path, {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body || {}),
    });
    const payload = await response.json();
    if (!payload.ok) throw new Error(payload.error?.message || 'The action failed.');
    return payload.data;
  } catch (error) {
    toast(String(error.message || error), 'danger');
    return null;
  }
}

async function onExport() {
  const data = await act('/api/review/export', {});
  if (!data) return;
  toast(data.message, data.exported ? 'ok' : 'warn', 6000);
}

async function onDismiss() {
  const data = await act('/api/review/dismiss', {},
    'Clear every review flag? This does NOT delete any output data.');
  if (!data) return;
  toast(`Dismissed ${num(data.dismissed)} flag(s).`, 'ok');
  state.selected = null;
  state.detail = null;
  await refresh();
}

async function onDrop() {
  if (!state.selected) {
    toast('Select a flagged conversation first, then drop it.', 'info');
    return;
  }
  const taskId = state.selected;
  const data = await act(`/api/review/${encodeURIComponent(taskId)}/drop`, {},
    `Permanently remove this ONE conversation from the dataset?\n\n` +
    `Task: ${taskId}\n\n` +
    `Its output row (and its duplication rows) will be deleted from the output ` +
    `file or the database. Every other conversation is left untouched. ` +
    `This cannot be undone -- export first if you want a copy.`);
  if (!data) return;
  if (data.total_removed) {
    toast(`Dropped ${taskId} (${num(data.total_removed)} row(s) removed).`, 'ok');
  } else {
    toast('No output row found for that task (it may never have been written); ' +
      'the flag was cleared anyway.', 'warn', 6000);
  }
  state.selected = null;
  state.detail = null;
  await refresh();
}

// --- rendering -----------------------------------------------------------

function screen() {
  return el('div', { class: 'screen active', id: 'screen-review' },
    toolbar(),
    el('div', { class: 'review-grid' },
      listPane(),
      detailPane(),
    ),
  );
}

function toolbar() {
  const input = el('input', {
    type: 'search',
    placeholder: 'Filter by task id…',
    value: state.filter,
    'aria-label': 'Filter flagged conversations',
  });
  const apply = () => {
    state.filter = input.value;
    refresh();
  };
  input.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') apply();
  });

  return el('div', { class: 'review-toolbar' },
    input,
    el('button', { type: 'button', onclick: apply }, 'Filter'),
    el('button', { type: 'button', onclick: onExport }, 'Export flagged → JSONL'),
    el('button', { type: 'button', class: 'btn-danger', onclick: onDrop },
      'Drop selected'),
    el('button', { type: 'button', onclick: onDismiss }, 'Dismiss all'),
    el('button', { type: 'button', onclick: () => refresh() }, 'Refresh'),
    el('span', { class: 'spacer' }),
    countLine(),
  );
}

function countLine() {
  const { flagged, dropped_by_judge, dropped_by_user } = state.counts;
  const parts = [`${num(flagged)} flagged`];
  if (dropped_by_judge) {
    parts.push(`${num(dropped_by_judge)} dropped by the judge (not in the dataset)`);
  }
  if (dropped_by_user) parts.push(`${num(dropped_by_user)} dropped by you`);
  return el('span', { class: 'review-count' }, parts.join('  ·  '));
}

function listPane() {
  const body = el('div', { class: 'panel-body' });

  if (state.error) {
    body.append(emptyState({ title: 'Could not load the flagged list', hint: state.error }));
  } else if (!state.rows.length) {
    body.append(state.filter
      ? emptyState({
        title: 'No match',
        hint: `No flagged conversation matches “${state.filter}”. Clear the filter to see `
          + 'them all.',
      })
      : emptyState({
        title: 'Nothing is flagged',
        hint: 'Conversations the quality judge scores below the threshold appear here.',
      }));
  } else {
    body.append(listTable());
  }

  return el('section', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } },
        'Flagged conversations (below threshold)'),
      el('span', { class: 'spacer' }),
      el('span', { class: 'card-sub' },
        state.loading ? 'loading…' : `${num(state.rows.length)} shown`),
    ),
    body,
  );
}

function listTable() {
  const table = el('table', { class: 'grid review-table' },
    el('colgroup', {},
      el('col', { style: { width: '30%' } }),
      el('col', { style: { width: '54px' } }),
      el('col', { style: { width: '26%' } }),
      el('col', {}),
    ),
    el('thead', {},
      el('tr', {},
        el('th', {}, 'Task ID'),
        el('th', {}, 'Score'),
        el('th', {}, 'Lowest dim'),
        el('th', {}, 'Flags'),
      ),
    ),
  );

  const tbody = el('tbody', {});
  for (const row of state.rows) {
    const tr = el('tr', {
      class: row.task_id === state.selected ? 'selected' : '',
      tabindex: '0',
      role: 'button',
      'aria-selected': String(row.task_id === state.selected),
    },
      el('td', { title: row.task_id }, row.display_id),
      el('td', { class: `score ${row.severity}` }, num(row.composite)),
      el('td', { title: `${row.lowest_dim} (${row.lowest_value})` },
        `${row.lowest_dim} (${row.lowest_value})`),
      el('td', { title: (row.flags || []).join(', ') },
        (row.flags || []).slice(0, 3).join(', ') || '—'),
    );
    // A single click opens the conversation, not only a double click: the old
    // binding made rows feel dead until the user guessed they had to double-click.
    tr.addEventListener('click', () => select(row.task_id));
    tr.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        select(row.task_id);
      }
    });
    tbody.append(tr);
  }
  table.append(tbody);
  return table;
}

function detailPane() {
  const body = el('div', { class: 'panel-body review-detail' });

  if (!state.selected) {
    body.append(emptyState({
      title: 'No conversation selected',
      hint: 'Select a flagged conversation on the left to read it here.',
    }));
  } else if (!state.detail) {
    body.append(el('div', { class: 'empty' }, 'Loading…'));
  } else {
    const detail = state.detail;
    body.append(el('div', { class: 'detail-head' },
      el('span', { class: 'mono', title: detail.task_id }, detail.task_id)));
    if (detail.note) {
      body.append(el('div', { class: 'detail-note' }, detail.note));
    }
    for (const turn of detail.turns || []) {
      body.append(el('div', { class: `turn turn-${String(turn.role).toLowerCase()}` },
        el('div', { class: 'turn-role' }, turn.role),
        el('div', { class: 'turn-text' }, turn.text),
      ));
    }
  }

  const panel = el('section', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } },
        'Conversation detail')),
    body,
  );

  // The footer strip: the score breakdown, colour-coded by threshold. This is what
  // makes "why was this flagged" answerable at a glance.
  if (state.detail && state.detail.score !== null && state.detail.score !== undefined) {
    panel.append(scoreFooter(state.detail));
  }
  return panel;
}

function scoreFooter(detail) {
  const dimensions = detail.dimensions || {};
  const threshold = 50;
  const strip = el('div', { class: 'score-strip' });

  for (const { key, label } of FOOTER_DIMENSIONS) {
    let value;
    if (key === 'overall') value = detail.score;
    else value = dimensions[key];
    const known = value !== null && value !== undefined && value !== '';
    const level = !known ? 'muted'
      : Number(value) < threshold ? 'danger'
        : Number(value) < 70 ? 'warn' : 'ok';
    strip.append(el('div', { class: `score-cell ${level}` },
      el('div', { class: 'card-caption' }, label),
      el('div', { class: 'score-value' }, known ? num(value, 1) : '—'),
    ));
  }

  if (detail.method) {
    strip.append(el('div', { class: 'score-cell muted' },
      el('div', { class: 'card-caption' }, 'Method'),
      el('div', { class: 'score-value small' }, detail.method),
    ));
  }
  return strip;
}

// --- repaint -------------------------------------------------------------

function repaint() {
  const main = document.getElementById('main');
  if (!main) return;
  const current = main.querySelector('#screen-review');
  if (!current) return;
  // Replace the panes only, so the filter input keeps focus and the list keeps
  // its scroll position while the detail pane re-renders.
  const next = screen();
  current.replaceChildren(...next.childNodes);
}

/** Called when the tab is left, so a later return re-fetches rather than showing stale rows. */
export function unmountReview() {
  mounted = false;
}
