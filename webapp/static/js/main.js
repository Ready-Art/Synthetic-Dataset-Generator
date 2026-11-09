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
 * Entry point. No build step, no framework -- plain ES modules.
 *
 * Phase 1 boots the shell and proves the bundle loads. Phase 3 adds the stats socket,
 * Phase 4 the Overview screen, Phase 5 the data tabs. Phase 8 adds the polish: a real
 * tablist with arrow-key navigation, a remembered tab, and a first paint that comes from
 * the server rather than from an empty page.
 */

import { el, setStatus } from './dom.js';
import { connectStats } from './stats.js';
import { renderOverview } from './overview.js';
import { renderReview, unmountReview } from './review.js';
import { DATA_TABS } from './tabs.js';
import { CONFIG_TAB } from './config.js';
import { DATASETS_TAB } from './datasets.js';
import { PROFILES_TAB } from './profiles.js';

// The tab order follows the Tk notebook, with Review second because it is the
// screen the maintainer named. `unmount` is called when a tab is left so a return
// re-fetches rather than showing a stale list.
const TABS = [
  { id: 'overview', label: 'Overview', render: renderOverview },
  { id: 'review', label: 'Review', render: renderReview, unmount: unmountReview },
  // The six data tabs share one module because they share one shape: fetch, render,
  // re-fetch on Refresh. Each still has its own unmount so a return re-fetches.
  ...DATA_TABS.map((entry) => ({
    id: entry.id,
    label: entry.label,
    render: () => entry.tab.render(),
    unmount: () => entry.tab.unmount(),
  })),
  // The Config editor is the only editor, so it is last but never optional.
  { id: CONFIG_TAB.id, label: CONFIG_TAB.label,
    render: CONFIG_TAB.render, unmount: CONFIG_TAB.unmount },
  // The Datasets panel (6d-a) is a dedicated screen rather than a raw group in
  // the Config editor: a source is 13 fields, an import and a mapping, which a
  // generic field renderer cannot express. It writes the same `datasets` subtree.
  { id: DATASETS_TAB.id, label: DATASETS_TAB.label,
    render: DATASETS_TAB.render, unmount: DATASETS_TAB.unmount },
  // The Profiles panel (6d-b) is the last bespoke widget: a saved copy of the whole
  // config, with a preview of the diff before a destructive load. It writes config.yml
  // through the same splicing writer as the Config editor.
  { id: PROFILES_TAB.id, label: PROFILES_TAB.label,
    render: PROFILES_TAB.render, unmount: PROFILES_TAB.unmount },
];

// --- window state ---------------------------------------------------------

// **The selected tab is remembered across a reload, and nothing else is.** A reload after a
// deploy or a laptop wake-up should put the reader back where they were. The tab id is not a
// secret and is not a session token -- a token must never go in `localStorage`, but a UI
// preference may -- so this is the one thing that is persisted client-side. Everything else is
// re-fetched from the server, because a run outlives the browser tab.
const TAB_KEY = 'readyart.tab';

function readStoredTab() {
  try {
    const value = window.localStorage.getItem(TAB_KEY);
    return TABS.some((tab) => tab.id === value) ? value : null;
  } catch (error) {
    // A browser with storage disabled (private mode, a locked-down profile) throws on
    // access. A remembered tab is a convenience, never a requirement.
    return null;
  }
}

function storeTab(id) {
  try {
    window.localStorage.setItem(TAB_KEY, id);
  } catch (error) {
    // Ignored on purpose -- see `readStoredTab`.
  }
}

const state = {
  tab: readStoredTab() || 'overview',
  stats: null,
};

// --- the tablist ----------------------------------------------------------

function buildTabs() {
  const nav = document.getElementById('tabs');
  nav.replaceChildren();
  for (const tab of TABS) {
    const selected = tab.id === state.tab;
    const button = el('button', {
      class: 'tab',
      role: 'tab',
      id: `tab-${tab.id}`,
      type: 'button',
      'aria-selected': String(selected),
      'aria-controls': `screen-${tab.id}`,
      // A roving tabindex: the selected tab is the one Tab lands on, and the arrow keys move
      // between the rest. Without this, Tab walks through eleven tabs before reaching the
      // content, which is the classic tablist accessibility failure.
      tabindex: selected ? '0' : '-1',
    }, tab.label);
    button.addEventListener('click', () => selectTab(tab.id));
    nav.append(button);
  }
  nav.addEventListener('keydown', onTabKeydown);
}

/** Arrow keys, Home and End move between tabs; the list wraps at both ends. */
function onTabKeydown(event) {
  const keys = ['ArrowLeft', 'ArrowRight', 'Home', 'End'];
  if (!keys.includes(event.key)) return;
  const index = TABS.findIndex((tab) => tab.id === state.tab);
  if (index < 0) return;
  event.preventDefault();

  let next = index;
  if (event.key === 'ArrowLeft') next = (index - 1 + TABS.length) % TABS.length;
  else if (event.key === 'ArrowRight') next = (index + 1) % TABS.length;
  else if (event.key === 'Home') next = 0;
  else if (event.key === 'End') next = TABS.length - 1;

  const id = TABS[next].id;
  selectTab(id);
  // Focus follows selection, so a second arrow key press works without tabbing back.
  document.getElementById(`tab-${id}`)?.focus();
}

function selectTab(id) {
  const previous = TABS.find((entry) => entry.id === state.tab);
  if (previous && previous.unmount) previous.unmount();
  state.tab = id;
  storeTab(id);
  for (const tab of TABS) {
    const button = document.getElementById(`tab-${tab.id}`);
    if (!button) continue;
    const selected = tab.id === id;
    button.setAttribute('aria-selected', String(selected));
    button.setAttribute('tabindex', selected ? '0' : '-1');
  }
  render();
}

function render() {
  const tab = TABS.find((entry) => entry.id === state.tab) || TABS[0];
  const main = document.getElementById('main');
  const screen = tab.render(state);
  // The tablist points at this node with `aria-controls`, so it has to be the matching
  // tabpanel or the relationship is one-sided and a screen reader cannot announce it.
  screen.setAttribute('role', 'tabpanel');
  screen.setAttribute('aria-labelledby', `tab-${tab.id}`);
  screen.setAttribute('tabindex', '-1');
  main.replaceChildren(screen);
}

// --- connection state -----------------------------------------------------

// The status line is the only place a reader learns that the UI has stopped updating. It must
// therefore say *connected* as well as *disconnected*: a line that only ever changes on a
// failure looks identical to a line that never updates at all.
function setConnected(connected) {
  document.body.classList.toggle('offline', !connected);
}

function onStats(payload) {
  state.stats = payload;
  // The overview module re-renders from this, so it must see the same object the
  // tab switch does. One source of truth, not two copies of the payload.
  window.__webappState = state;
  const generator = payload?.generator || {};
  const phase = generator.phase || 'idle';
  const version = generator.version ? `v${generator.version}` : '';
  setStatus(`${phase}${version ? ' · ' + version : ''}`);
  setConnected(true);
  // Only the Overview re-renders on a stats tick. A tick must never repaint the Config
  // editor: it would throw away a half-typed field on every second.
  if (state.tab === 'overview') render();
}

function onDisconnected() {
  setStatus('reconnecting…');
  setConnected(false);
}

/**
 * Prime the first paint from the server.
 *
 * A reload must not show an empty Overview until the first socket tick arrives -- the run is
 * still going, so the page should look like it does mid-run the instant it loads. `/api/stats`
 * is the same document the socket pushes, so this is a fetch, not a second source of truth.
 */
async function primeStats() {
  try {
    const response = await fetch('/api/stats', { credentials: 'same-origin' });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (response.ok) onStats(await response.json());
  } catch (error) {
    // The socket's own reconnect path will report the failure; a failed prime is not fatal.
  }
}

function main() {
  window.__webappState = state;
  buildTabs();
  render();
  primeStats();
  connectStats(onStats, onDisconnected);

  // A laptop that sleeps, or a proxy that drops an idle connection, leaves the socket dead
  // until its backoff fires. Coming back online or back to the tab is a strong signal that the
  // connection is worth retrying immediately rather than waiting out the backoff.
  window.addEventListener('online', () => { primeStats(); });
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') primeStats();
  });
}

main();
