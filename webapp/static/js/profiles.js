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
 * The Profiles panel -- Phase 6d-b. The last bespoke widget.
 *
 * Replaces the Tk editor's Profiles tab: a list of saved configs on the left, the selected
 * profile's preview on the right, and a save form underneath.
 *
 * **No model logic lives here.** `webapp/profiles_routes.py` does the name validation, the
 * secret masking, the candidate merge and the validation; this module only draws what it is
 * sent. That is deliberate for the same reason the Datasets panel is: a second copy of the
 * name rule or the merge rule in JS would drift from the one that decides whether a load is
 * safe.
 *
 * **A load is destructive and is never one click.** Selecting a profile fetches a *preview* --
 * the leaf diff and any validation error -- and the Load button is disabled while that preview
 * is missing, broken or invalid. The user sees what would change before anything is written.
 *
 * **A preview is a GET; the load is a POST.** A link, a prefetch or a stale tab must not be
 * able to replace the running config.
 */

import { el, emptyState, num, toast } from './dom.js';

// --- state ---------------------------------------------------------------

const state = {
  data: null,        // the last GET /api/profiles payload
  selected: null,    // the selected profile name
  preview: null,     // the last GET /api/profiles/{name} payload
  previewFor: null,  // the name that preview describes
  loading: false,
  saving: false,
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
    state.data = await getJSON('/api/profiles');
    const names = (state.data.profiles || []).map((entry) => entry.name);
    if (!state.selected || !names.includes(state.selected)) {
      state.selected = names[0] || null;
    }
  } catch (error) {
    state.data = { error: String(error.message || error), profiles: [], counts: {} };
  } finally {
    state.loading = false;
    repaint();
  }
  if (state.selected) await loadPreview(state.selected);
}

/** Fetch the diff a load would make. Never writes anything. */
async function loadPreview(name) {
  if (!name) {
    state.preview = null;
    state.previewFor = null;
    repaint();
    return;
  }
  state.preview = null;
  state.previewFor = name;
  repaint();
  try {
    const body = await getJSON(`/api/profiles/${encodeURIComponent(name)}`);
    // A slow preview for a profile the user has already navigated away from must not
    // overwrite the one on screen.
    if (state.previewFor !== name) return;
    state.preview = body.data;
  } catch (error) {
    if (state.previewFor !== name) return;
    state.preview = { error: String(error.message || error), diff: {}, errors: {} };
  }
  repaint();
}

function currentProfile() {
  if (!state.data) return null;
  return (state.data.profiles || []).find((entry) => entry.name === state.selected) || null;
}

function previewReady() {
  return Boolean(state.preview && state.previewFor === state.selected
    && !state.preview.error && state.preview.valid);
}

function diffCount() {
  return Object.keys(state.preview?.diff || {}).length;
}

// --- rendering -----------------------------------------------------------

function repaint() {
  const main = document.getElementById('main');
  if (!main) return;
  const current = main.querySelector('#screen-profiles');
  if (!current) return;
  current.replaceChildren(...screen().childNodes);
}

function screen() {
  const body = el('div', { class: 'profiles-body' });

  if (!state.data) {
    body.append(el('div', { class: 'empty' }, 'Loading…'));
  } else if (state.data.error) {
    body.append(emptyState({ title: 'Could not load the profiles', hint: state.data.error }));
  } else {
    body.append(listPane(), detailPane());
  }

  return el('div', { class: 'screen active', id: 'screen-profiles' },
    toolbar(),
    body,
  );
}

function toolbar() {
  const counts = state.data?.counts || {};
  return el('div', { class: 'tab-toolbar' },
    el('span', { class: 'section-header', style: { margin: '0' } }, 'Profiles'),
    el('span', { class: 'muted', id: 'profiles-count' },
      state.data?.profiles
        ? `${num(counts.profiles)} profile(s)`
          + (counts.broken ? ` · ${num(counts.broken)} unusable` : '')
        : ''),
    el('span', { class: 'spacer' }),
    el('button', { type: 'button', onclick: () => refresh() }, 'Reload'),
  );
}

function listPane() {
  const list = el('div', { class: 'profiles-list' });
  const profiles = state.data?.profiles || [];

  if (!profiles.length) {
    list.append(emptyState({
      title: 'No profiles yet',
      hint: 'A profile is a saved copy of the whole config, so you can switch between '
        + 'setups without re-entering every value.',
    }));
  }

  for (const entry of profiles) {
    const button = el('button', {
      type: 'button',
      class: 'profiles-item',
      'aria-selected': String(entry.name === state.selected),
      onclick: () => selectProfile(entry.name),
    },
      el('span', { class: 'profiles-item-name', title: entry.name }, entry.name),
      el('span', { class: 'profiles-item-meta' },
        entry.broken
          ? el('span', { class: 'pill danger' }, 'unusable')
          : el('span', { class: 'muted' }, `${num(entry.leaves)} values`),
        entry.modified ? el('span', { class: 'muted mono' }, entry.modified) : null,
      ),
    );
    list.append(button);
  }

  return el('div', { class: 'profiles-list-pane' },
    el('div', { class: 'section-header' }, 'Saved profiles'),
    list,
    saveForm(),
  );
}

/** The save form. A name and a button -- the config it saves is the live one. */
function saveForm() {
  const input = el('input', {
    type: 'text',
    id: 'profiles-new-name',
    placeholder: 'new profile name',
    maxlength: '64',
    onkeydown: (event) => {
      if (event.key === 'Enter') saveProfile();
    },
  });
  return el('div', { class: 'profiles-save' },
    el('div', { class: 'section-header' }, 'Save the current config'),
    input,
    el('button', {
      type: 'button',
      class: 'btn-primary',
      disabled: state.saving ? 'disabled' : null,
      onclick: () => saveProfile(),
    }, state.saving ? 'Saving…' : 'Save as profile'),
    el('p', { class: 'config-help' },
      'Saves every value in config.yml, including the dataset sources. API keys are '
      + 'stored masked, so a profile never holds a key.'),
  );
}

function detailPane() {
  const profile = currentProfile();
  const pane = el('div', { class: 'profiles-detail' });

  if (!profile) {
    pane.append(emptyState({
      title: 'No profile selected',
      hint: 'Select a profile to see what loading it would change.',
    }));
    return pane;
  }

  pane.append(el('div', { class: 'profiles-detail-head' },
    el('span', { class: 'section-header', style: { margin: '0' } }, profile.name),
    el('span', { class: 'muted mono' }, `${num(profile.leaves)} values`),
    el('span', { class: 'spacer' }),
    el('button', {
      type: 'button',
      class: 'btn-danger',
      onclick: () => removeProfile(profile),
    }, 'Delete'),
    el('button', {
      type: 'button',
      class: 'btn-primary',
      id: 'profiles-load',
      disabled: previewReady() ? null : 'disabled',
      onclick: () => applyProfile(profile),
    }, 'Load into config.yml'),
  ));

  if (profile.broken) {
    pane.append(el('p', { class: 'profiles-status danger' },
      `This file cannot be read: ${profile.error}`));
    return pane;
  }

  if (state.previewFor !== profile.name) {
    pane.append(el('div', { class: 'empty' }, 'Reading the profile…'));
    return pane;
  }

  if (!state.preview) {
    pane.append(el('div', { class: 'empty' }, 'Reading the profile…'));
    return pane;
  }

  if (state.preview.error) {
    pane.append(el('p', { class: 'profiles-status danger' }, state.preview.error));
    return pane;
  }

  pane.append(previewSummary());
  pane.append(errorList());
  pane.append(diffTable());
  return pane;
}

/** The one-line verdict above the diff. */
function previewSummary() {
  const errors = Object.keys(state.preview?.errors || {}).length;
  const changes = diffCount();

  if (errors) {
    return el('p', { class: 'profiles-status danger' },
      `${num(errors)} problem(s) — this profile cannot be loaded until they are fixed.`);
  }
  if (!changes) {
    return el('p', { class: 'profiles-status muted' },
      'This profile matches the current config. Loading it would change nothing.');
  }
  return el('p', { class: 'profiles-status warn' },
    `Loading this profile would change ${num(changes)} value(s). `
    + 'Everything else stays as it is.');
}

function errorList() {
  const errors = state.preview?.errors || {};
  const paths = Object.keys(errors);
  if (!paths.length) return el('span', {});
  const list = el('ul', { class: 'profiles-errors' });
  for (const path of paths.sort()) {
    list.append(el('li', {},
      el('span', { class: 'mono' }, path),
      ' — ',
      errors[path],
    ));
  }
  return el('div', { class: 'profiles-error-block' },
    el('div', { class: 'section-header' }, 'Problems'),
    list,
  );
}

/** The leaf diff: what a load would actually change. */
function diffTable() {
  const diff = state.preview?.diff || {};
  const paths = Object.keys(diff).sort();

  const section = el('div', { class: 'profiles-diff' },
    el('div', { class: 'section-header' },
      paths.length ? `Changes (${num(paths.length)})` : 'Changes'),
  );

  if (!paths.length) {
    section.append(el('p', { class: 'config-help' },
      'No value would change.'));
    return section;
  }

  const table = el('table', { class: 'grid profiles-diff-table' },
    el('thead', {},
      el('tr', {},
        el('th', {}, 'Setting'),
        el('th', {}, 'Now'),
        el('th', {}, 'After loading'),
      ),
    ),
  );
  const body = el('tbody');
  for (const path of paths) {
    const entry = diff[path];
    body.append(el('tr', {},
      el('td', { class: 'mono', title: path }, path),
      el('td', { class: entry.from === null ? 'muted' : 'mono' },
        entry.from === null ? '(not set)' : entry.from),
      el('td', { class: entry.to === null ? 'muted' : 'mono' },
        entry.to === null ? '(not set)' : entry.to),
    ));
  }
  table.append(body);
  section.append(table);
  return section;
}

// --- actions -------------------------------------------------------------

function selectProfile(name) {
  if (name === state.selected) return;
  state.selected = name;
  state.preview = null;
  state.previewFor = null;
  repaint();
  loadPreview(name);
}

async function saveProfile() {
  const input = document.getElementById('profiles-new-name');
  const name = (input?.value || '').trim();
  if (!name) {
    toast('Type a name for the profile first.', 'warn');
    return;
  }
  // Overwriting is a second, explicit decision: a save that silently replaced a profile the
  // user had tuned is the one mistake that makes profiles untrustworthy.
  const existing = (state.data?.profiles || []).some((entry) => entry.name === name);
  if (existing && !window.confirm(
    `A profile named "${name}" already exists. Overwrite it?`)) return;

  state.saving = true;
  repaint();
  try {
    await sendJSON('/api/profiles', 'POST', { name, overwrite: existing });
    toast(`Saved profile "${name}".`, 'ok');
    state.selected = name;
    state.preview = null;
    state.previewFor = null;
    await refresh();
  } catch (error) {
    toast(String(error.message || error), 'danger');
  } finally {
    state.saving = false;
    repaint();
  }
}

async function applyProfile(profile) {
  if (!previewReady()) return;
  const changes = diffCount();
  const message = changes
    ? `Load "${profile.name}" into config.yml? ${num(changes)} value(s) would change. `
      + 'This replaces the current config.'
    : `Load "${profile.name}"? It matches the current config, so nothing would change.`;
  if (!window.confirm(message)) return;

  try {
    const body = await sendJSON(
      `/api/profiles/${encodeURIComponent(profile.name)}/load`, 'POST', {});
    const applied = Object.keys(body.data?.diff || {}).length;
    toast(applied
      ? `Loaded "${profile.name}" — ${num(applied)} value(s) changed.`
      : `"${profile.name}" was already the current config.`, 'ok');
    state.preview = null;
    state.previewFor = null;
    await refresh();
  } catch (error) {
    toast(String(error.message || error), 'danger');
  }
}

async function removeProfile(profile) {
  if (!window.confirm(
    `Delete the profile "${profile.name}"? This cannot be undone. `
    + 'The current config.yml is not affected.')) return;
  try {
    await sendJSON(`/api/profiles/${encodeURIComponent(profile.name)}`, 'DELETE', {});
    toast(`Deleted profile "${profile.name}".`, 'ok');
    if (state.selected === profile.name) {
      state.selected = null;
      state.preview = null;
      state.previewFor = null;
    }
    await refresh();
  } catch (error) {
    toast(String(error.message || error), 'danger');
  }
}

// --- the tab -------------------------------------------------------------

export function renderProfiles() {
  if (!state.mounted) {
    state.mounted = true;
    refresh();
  }
  return screen();
}

export function unmountProfiles() {
  // The preview is dropped on leave: it describes the config as it was when the tab was open,
  // and the config may have changed from the Config editor since.
  state.mounted = false;
  state.preview = null;
  state.previewFor = null;
}

export const PROFILES_TAB = {
  id: 'profiles',
  label: 'Profiles',
  render: renderProfiles,
  unmount: unmountProfiles,
};
