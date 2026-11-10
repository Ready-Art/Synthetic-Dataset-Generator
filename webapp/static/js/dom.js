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
 * Tiny DOM helpers. No framework: `el()` is a 12-line createElement wrapper and that is
 * the whole abstraction.
 */

/**
 * Create an element.
 *
 * @param {string} tag
 * @param {Object} [attrs] - `class`, `text`, `title`, `dataset`, or any attribute.
 * @param {...(Node|string)} children
 * @returns {HTMLElement}
 */
export function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value === null || value === undefined || value === false) continue;
    if (key === 'class') node.className = value;
    else if (key === 'text') node.textContent = String(value);
    else if (key === 'dataset') Object.assign(node.dataset, value);
    else if (key === 'style' && typeof value === 'object') Object.assign(node.style, value);
    else if (key.startsWith('on') && typeof value === 'function') {
      node.addEventListener(key.slice(2).toLowerCase(), value);
    } else node.setAttribute(key, String(value));
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

/** Replace an element's children in one call. */
export function fill(node, ...children) {
  node.replaceChildren(...children.flat().filter((c) => c !== null && c !== undefined));
  return node;
}

/** A metric card: caption, value, optional sub-line. Long values ellipsize with a tooltip. */
export function metricCard(caption, value, sub) {
  const valueNode = el('div', { class: 'card-value', title: String(value) }, String(value));
  return el('div', { class: 'card' },
    el('div', { class: 'card-caption', title: caption }, caption),
    valueNode,
    sub === undefined || sub === null ? null : el('div', { class: 'card-sub' }, sub),
  );
}

/** Set the top-bar status text. */
export function setStatus(text) {
  const node = document.getElementById('status-line');
  if (node) node.textContent = text;
}

/** Format a number with thousands separators; `null`/`undefined` become an em dash. */
export function num(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

/** Format a percentage from a 0-100 number. */
export function pct(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return `${Number(value).toFixed(digits)}%`;
}

/**
 * An empty state: a title, an optional hint, and an optional action.
 *
 * A bare "Nothing to show" line is the failure mode Phase 7 named -- a blank area reads as
 * "this thing has no data", which is alarming and usually wrong. An empty state says what is
 * empty, why, and what to do about it. The `empty` class stays on the wrapper so every
 * existing caller and test keeps working; the title and hint are additive.
 *
 * @param {Object} options
 * @param {string} options.title - what is empty ("No profiles yet")
 * @param {string} [options.hint] - why, or what to do next
 * @param {Node} [options.action] - an optional button
 * @returns {HTMLElement}
 */
export function emptyState({ title, hint, action } = {}) {
  return el('div', { class: 'empty' },
    el('div', { class: 'empty-title' }, title || 'Nothing to show yet.'),
    hint ? el('div', { class: 'empty-hint' }, hint) : null,
    action ? el('div', { class: 'empty-action' }, action) : null,
  );
}

// --- scroll preservation --------------------------------------------------

/**
 * Record the scroll offset of every keyed scroller under `root`.
 *
 * **Why this exists.** A stats tick re-renders the whole screen by replacing its children, which
 * throws away every scroll position with it. The Overview's Recent issues panel is the visible
 * symptom: a reader scrolls down to read a row, the next tick arrives a second later, and the panel
 * snaps back to the top. That is not a cosmetic annoyance -- it makes the list unreadable during a
 * run, which is the only time it has anything in it.
 *
 * A scroller opts in by carrying `data-scroll-key`, which must be **stable across renders** (an id
 * like `overview-issues`, not an index). Anything without a key is not touched, so this cannot
 * resurrect a scroll position for a panel that has genuinely changed.
 *
 * @param {HTMLElement} root
 * @returns {Map<string, number>}
 */
export function captureScroll(root) {
  const offsets = new Map();
  if (!root) return offsets;
  for (const node of root.querySelectorAll('[data-scroll-key]')) {
    if (node.scrollTop) offsets.set(node.dataset.scrollKey, node.scrollTop);
  }
  return offsets;
}

/**
 * Put back the offsets :func:`captureScroll` recorded.
 *
 * Deferred to the next frame on purpose: a scroll offset can only be set once the new content has a
 * height, and immediately after `replaceChildren` it does not, so the assignment would be clamped to
 * zero. The frame is the first point at which the browser has laid the list out.
 *
 * @param {HTMLElement} root
 * @param {Map<string, number>} offsets
 */
export function restoreScroll(root, offsets) {
  if (!root || !offsets || !offsets.size) return;
  const apply = () => {
    for (const node of root.querySelectorAll('[data-scroll-key]')) {
      const top = offsets.get(node.dataset.scrollKey);
      if (top) node.scrollTop = top;
    }
  };
  requestAnimationFrame(apply);
  // The frame is the fast path. A throttled renderer may not produce one for a while, and a
  // timeout that fires first is harmless because the assignment is idempotent.
  window.setTimeout(apply, 120);
}

// --- toasts --------------------------------------------------------------

// The older callers used `success` / `error` / `warning` / `info`; the web callers use
// `ok` / `warn` / `danger` / `info`. Both spellings are accepted and mapped to the four CSS
// classes that actually exist, so a caller written against the older vocabulary gets a styled
// toast instead of an unstyled one -- an unstyled toast is the silent-failure class this plan
// keeps finding.
const TOAST_LEVELS = {
  info: 'info',
  ok: 'ok', success: 'ok',
  warn: 'warn', warning: 'warn',
  danger: 'danger', error: 'danger',
};

// A stack of twenty toasts runs off the top of the viewport and the oldest is unreadable
// anyway. Oldest goes first.
const TOAST_MAX = 4;
const TOAST_DEFAULT_MS = 5000;

/**
 * Show a toast. Auto-dismisses, but **pauses while hovered or focused** so a long error
 * can be read, and carries a dismiss button so it can be closed early.
 *
 * The container is `#toasts` in index.html. `role` is `alert` for a warning or a danger
 * and `status` otherwise, so a screen reader interrupts for a failure and waits for a
 * success.
 *
 * @param {string} message
 * @param {string} [level] - info | ok | success | warn | warning | danger | error
 * @param {number} [ms] - auto-dismiss delay; `0` keeps it until dismissed
 * @returns {HTMLElement|null} the toast node
 */
export function toast(message, level = 'info', ms = TOAST_DEFAULT_MS) {
  const host = document.getElementById('toasts');
  if (!host) return null;

  const kind = TOAST_LEVELS[level] || 'info';
  const urgent = kind === 'danger' || kind === 'warn';

  const node = el('div', {
    class: `toast ${kind}`,
    role: urgent ? 'alert' : 'status',
  });

  const close = el('button', {
    type: 'button',
    class: 'toast-close',
    'aria-label': 'Dismiss notification',
  }, '×');

  node.append(el('span', { class: 'toast-message' }, message), close);

  let timer = null;
  const dismiss = () => {
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
    node.remove();
  };
  const start = () => {
    if (!ms) return;
    timer = setTimeout(dismiss, ms);
  };
  const pause = () => {
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
  };

  close.addEventListener('click', dismiss);
  node.addEventListener('mouseenter', pause);
  node.addEventListener('mouseleave', start);
  node.addEventListener('focusin', pause);
  node.addEventListener('focusout', start);

  host.append(node);

  // Cap the stack. `querySelectorAll` rather than a module array, so a toast removed by
  // some other path cannot leave a stale reference behind.
  const live = host.querySelectorAll('.toast');
  for (let i = 0; i < live.length - TOAST_MAX; i += 1) live[i].remove();

  start();
  return node;
}
