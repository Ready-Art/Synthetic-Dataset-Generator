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
 * The Overview screen.
 *
 * The layout decisions here are deliberate and are NOT decoration:
 *
 * - **Six metric cards in one row**, not sixteen in three rows. The rest go behind a
 *   "more metrics" disclosure. This is what fixes "the top half is a wall of text".
 * - The issue table and the progress panel sit **side by side** at `1.55fr / 1fr`, so the
 *   page fits 1440x900 with no scrolling.
 * - Progress is **one row per slot** with a label, a track and a percentage -- not a stack of
 *   full-width bars.
 * - Long values **ellipsize with a `title` tooltip** and the card grows. The Tk bug was a
 *   22px value inside `wraplength=220` inside a 180px grid cell; there is no wraplength here.
 *
 * Every screen must fit 1440x900 with no page scroll. If it needs one, the layout is wrong.
 */

import { el, emptyState, metricCard, num, pct, toast } from './dom.js';
import { lineChart, legend } from './chart.js';
import { command } from './stats.js';

// The six cards on the first row. Chosen because they are the ones a reader checks
// first: what was caught, what was dropped, and what broke.
const PRIMARY_METRICS = [
  { key: 'refusals', caption: 'Refusals' },
  { key: 'out_of_character', caption: 'Out of character' },
  { key: 'slop', caption: 'Slop' },
  { key: 'logic_issues', caption: 'Logic issues' },
  { key: 'regex_watch', caption: 'Regex watch' },
  { key: 'errors', caption: 'Errors' },
];

// The remaining counters, behind the disclosure. Same data, one click away.
const SECONDARY_METRICS = [
  { key: 'user_speaking', caption: 'User speak' },
  { key: 'anti_slop', caption: 'Anti-slop' },
  { key: 'non_latin', caption: 'Non-Latin' },
  { key: 'scene_continuity', caption: 'Cast drift' },
];

// The seven Overview issue panels, in the Tk order, with Errors excluded (the old
// "Totals" set). The key matches `detected.<key>` in the stats payload.
const ISSUE_PANELS = [
  { key: 'refusals', title: 'Refusals', colour: 'danger' },
  { key: 'user_speaking', title: 'User speaking', colour: 'warn' },
  { key: 'slop', title: 'Slop', colour: 'warn' },
  { key: 'anti_slop', title: 'Anti-slop', colour: 'warn' },
  { key: 'out_of_character', title: 'Out of character', colour: 'danger' },
  { key: 'logic_issues', title: 'Logic issues', colour: 'warn' },
  { key: 'regex_watch', title: 'Regex watch', colour: 'accent' },
];

// The graph series, in the Tk order. Colours come from the palette tokens.
const GRAPH_SERIES = [
  { key: 'refusals', name: 'Refusals', token: '--danger' },
  { key: 'out_of_character', name: 'OOC', token: '--warn' },
  { key: 'slop', name: 'Slop', token: '--accent' },
  { key: 'errors', name: 'Errors', token: '--ok' },
];

// Which disclosures are open, keyed by id. Module-level so a stats-driven
// re-render keeps them open.
const openState = { 'more-metrics': false, 'diagnostics': false };
let busy = false;

export function renderOverview(state) {
  const stats = state?.stats || {};
  const detected = stats.detected || {};
  const run = stats.run || {};
  const tokens = stats.tokens || {};
  const output = stats.output || {};
  const quality = stats.quality || {};

  const screen = el('div', { class: 'screen active', id: 'screen-overview' },
    actionBar(run),
    metricRow(detected),
    disclosure(detected, tokens, output, quality),
    mainGrid(stats, detected),
    diagnostics(stats),
  );
  return screen;
}

// --- action bar ----------------------------------------------------------

function actionBar(run) {
  const phase = run.active ? (run.paused ? 'paused' : 'running') : 'idle';
  const pillClass = phase === 'running' ? 'ok'
    : phase === 'paused' ? 'warn' : 'muted';

  const start = el('button', { class: 'btn-primary', type: 'button' }, 'Start');
  start.addEventListener('click', () => send(start, 'start'));

  const pause = el('button', { type: 'button' }, run.paused ? 'Resume' : 'Pause');
  pause.addEventListener('click', () => send(pause, run.paused ? 'resume' : 'pause'));

  const stop = el('button', { type: 'button' }, 'Stop');
  stop.addEventListener('click', () => send(stop, 'stop'));

  const stopClear = el('button', { class: 'btn-danger', type: 'button' },
    'Stop & Clear');
  stopClear.addEventListener('click', () => {
    if (!window.confirm('Stop the run and clear the dataset? This cannot be undone.')) {
      return;
    }
    send(stopClear, 'stop', { clear: true });
  });

  // Download the dataset, and start fresh. These two are about the dataset on disk
  // rather than about the run, so they are never disabled by the run's phase: a
  // finished dataset is exactly when a reader wants to take it away or clear it.
  const download = el('button', { type: 'button' }, 'Download dataset');
  download.addEventListener('click', () => downloadDataset(download));

  const clearDataset = el('button', { class: 'btn-danger', type: 'button' },
    'Clear dataset');
  clearDataset.addEventListener('click', () => clearStoredDataset(clearDataset));

  // Disable what cannot apply, rather than letting a click fail with a 409.
  start.disabled = Boolean(run.active);
  pause.disabled = !run.active;
  stop.disabled = !run.active;
  stopClear.disabled = !run.active;

  return el('div', { class: 'action-bar' },
    el('span', { class: `pill ${pillClass}` }, phase),
    start, pause, stop, stopClear,
    el('span', { class: 'spacer' }),
    download, clearDataset,
    el('span', { class: 'card-sub' },
      run.run_started_at ? `started ${run.run_started_at}` : 'no run yet'),
  );
}

/**
 * Download the dataset as one JSONL file.
 *
 * A plain navigation, not a fetch: the route answers with an attachment, so the
 * browser's own download handling is what names and saves the file. Reading it into
 * JS first would hold the whole dataset in memory for no reason.
 *
 * **The preflight is the point.** `/api/dataset/download` answers 404 with a JSON
 * error when there is nothing to send, and a plain navigation would replace the whole
 * app with that raw JSON -- which is exactly what "Download dataset is a 404 page"
 * looked like. `GET /api/dataset` is a cheap row count, so the button can say "there
 * is no dataset yet" in a toast and stay on the page instead of navigating into an
 * error document.
 */
async function downloadDataset(button) {
  button.disabled = true;
  try {
    const response = await fetch('/api/dataset', { credentials: 'same-origin' });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    if (!response.ok) {
      toast('Could not check the dataset. Try again.', 'danger');
      return;
    }
    const summary = await response.json();
    const rows = Number(summary.total_rows || 0);
    if (!rows) {
      toast('There is no dataset to download yet. Start a run first.', 'warn', 6000);
      return;
    }
    // The route is a GET, so a hidden link is enough and the browser keeps its own
    // download UI. The anchor is removed on the next tick.
    const link = el('a', { href: '/api/dataset/download', download: '' });
    document.body.appendChild(link);
    link.click();
    link.remove();
    toast(`Preparing ${num(rows)} row(s) for download…`, 'info');
  } catch (error) {
    toast(error.message || 'Could not start the download.', 'danger');
  } finally {
    // Re-enable on a short delay: the browser needs a moment to pick up the
    // attachment, and a second click before that would start a duplicate download.
    window.setTimeout(() => { button.disabled = false; }, 1500);
  }
}

/**
 * Clear the stored dataset, after an explicit confirmation.
 *
 * The confirmation names what is deleted and says a backup is taken, because "clear"
 * alone does not tell a reader whether their export survives.
 */
async function clearStoredDataset(button) {
  const agreed = window.confirm(
    'Delete the stored dataset? A backup zip is taken first, so this can be undone.');
  if (!agreed) return;
  button.disabled = true;
  try {
    const response = await fetch('/api/dataset/clear', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ confirm: true, target: 'both' }),
    });
    if (response.status === 401) {
      window.location.href = '/login';
      return;
    }
    const body = await response.json();
    if (!body.ok) {
      toast(body.error?.message || 'Could not clear the dataset.', 'error');
      return;
    }
    const data = body.data || {};
    const removed = data.jsonl_removed ?? 0;
    toast(`Cleared ${removed} file(s). Backup: ${data.backup_path || 'none'}.`, 'ok');
  } catch (error) {
    toast(error.message || 'Could not clear the dataset.', 'error');
  } finally {
    button.disabled = false;
  }
}

async function send(button, cmd, args = {}) {
  if (busy) return;
  busy = true;
  button.disabled = true;
  try {
    const result = await command(cmd, args);
    if (!result.ok) {
      const { toast } = await import('./dom.js');
      toast(result.error?.message || 'Command failed.', 'danger');
    }
  } finally {
    busy = false;
    button.disabled = false;
  }
}

// --- metric cards -------------------------------------------------------

function metricRow(detected) {
  return el('div', { class: 'metric-row' },
    PRIMARY_METRICS.map(({ key, caption }) => {
      const entry = detected[key] || {};
      const total = entry.total ?? 0;
      const recent = Array.isArray(entry.recent) ? entry.recent.length : 0;
      return metricCard(caption, num(total), recent ? `${num(recent)} recent` : null);
    }),
  );
}

function disclosure(detected, tokens, output, quality) {
  const body = el('div', { class: 'metric-row secondary', style: { marginTop: '10px' } },
    SECONDARY_METRICS.map(({ key, caption }) => {
      const entry = detected[key] || {};
      return metricCard(caption, num(entry.total ?? 0), null);
    }),
    metricCard('Tokens in', num(tokens.input), null),
    metricCard('Tokens out', num(tokens.output), null),
    metricCard('Est. cost', num(tokens.estimated_cost, 4), null),
    metricCard('Quality', quality.enabled ? num(quality.avg_composite, 1) : 'off',
      quality.enabled ? `${num(quality.scored)} scored` : null),
    metricCard('Below threshold', num(quality.threshold_failures),
      `threshold ${num(quality.threshold)}`),
    metricCard('Rows written', num(output.jsonl_rows),
      output.sink === 'postgres' ? 'postgres' : 'jsonl'),
  );

  return disclosureBlock('more-metrics', 'More metrics', 'Fewer metrics', body);
}

/**
 * A disclosure that toggles **locally** rather than re-rendering the screen.
 *
 * A full re-render would replace the very button that was clicked, so a second
 * click would land on a detached node and do nothing -- and it would throw away
 * the scroll position of the panels beside it. The open state is remembered in a
 * module flag so it survives the stats-driven re-render.
 */
function disclosureBlock(id, closedLabel, openLabel, body) {
  const isOpen = () => Boolean(openState[id]);
  const toggle = el('button', { type: 'button', class: 'disclosure-toggle' },
    isOpen() ? `▾ ${openLabel}` : `▸ ${closedLabel}`);
  const region = el('div', { class: 'disclosure-body' }, isOpen() ? body : null);

  toggle.addEventListener('click', () => {
    openState[id] = !openState[id];
    toggle.textContent = isOpen() ? `▾ ${openLabel}` : `▸ ${closedLabel}`;
    region.replaceChildren(...(isOpen() ? [body] : []));
    // Only an expanded screen is allowed to scroll: the default view must fit
    // 1440x900 exactly, which is the layout rule.
    document.getElementById('main')?.classList.toggle('expanded',
      Object.values(openState).some(Boolean));
  });

  return el('div', { style: { marginTop: '10px' } }, toggle, region);
}

// --- the two-column body ------------------------------------------------

function mainGrid(stats, detected) {
  // 1.55fr / 1fr: the issue table is the wider column because its rows are
  // longer, and the right column stacks progress over the graph so both fit the
  // height the issue table already needs. No page scroll at 1440x900.
  return el('div', { class: 'overview-grid' },
    issuePanel(detected),
    el('div', { class: 'overview-side' },
      progressPanel(stats),
      graphPanel(stats),
    ),
  );
}

function issuePanel(detected) {
  const body = el('div', { class: 'panel-body' });

  // One table per issue type, all inside one scrolling panel. A table per panel
  // would need seven scrollbars in a column that is already the taller one.
  let any = false;
  for (const { key, title, colour } of ISSUE_PANELS) {
    const recent = (detected[key] || {}).recent || [];
    if (!recent.length) continue;
    any = true;
    body.append(el('div', { class: 'issue-group' },
      el('div', { class: 'issue-group-head' },
        el('span', { class: 'section-header', style: { margin: '0' } }, title),
        el('span', { class: `pill ${colour}` }, num(recent.length)),
      ),
      issueTable(recent),
    ));
  }

  if (!any) {
    body.append(emptyState({
      title: 'No issues recorded yet',
      hint: 'Start a run and anything caught will appear here.',
    }));
  }

  return el('section', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } },
        'Recent issues')),
    body,
  );
}

function issueTable(rows) {
  // Fixed widths for Time and API (they are always short), a share for Phrase,
  // and the rest for Context -- which is the column that carries real text and is
  // the one a reader actually scans. Without a width on Phrase it steals Context's
  // space and the context is clipped to a few characters.
  const table = el('table', { class: 'grid issue-table' },
    el('colgroup', {},
      el('col', { style: { width: '72px' } }),
      el('col', { style: { width: '46px' } }),
      el('col', { style: { width: '30%' } }),
      el('col', {}),
    ),
    el('thead', {},
      el('tr', {},
        el('th', {}, 'Time'),
        el('th', {}, 'API'),
        el('th', {}, 'Phrase'),
        el('th', {}, 'Context'),
      ),
    ),
  );
  const tbody = el('tbody', {});
  // Newest first: the most recent issue is the one a reader wants.
  for (const row of [...rows].reverse()) {
    const entry = normaliseIssue(row);
    tbody.append(el('tr', {},
      el('td', { title: entry.time }, entry.time || '—'),
      el('td', {}, entry.api >= 0 ? `#${entry.api + 1}` : '—'),
      el('td', { title: entry.phrase }, entry.phrase || '—'),
      el('td', { title: entry.context }, entry.context || '—'),
    ));
  }
  table.append(tbody);
  return table;
}

function progressPanel(stats) {
  const run = stats.run || {};
  const progress = run.progress || {};
  const tasks = run.tasks || {};
  const output = stats.output || {};

  const rows = el('div', {},
    progressRow('Overall', progress.percent ?? 0,
      `${num(progress.processed)} / ${num(progress.total)}`),
    progressRow('Queue', progress.total
      ? ((progress.queued ?? 0) / progress.total) * 100
      : 0, `${num(progress.queued)} queued`),
    progressRow('Threads', run.threads
      ? ((run.alive_threads ?? 0) / run.threads) * 100
      : 0, `${num(run.alive_threads)} / ${num(run.threads)} alive`),
  );

  const counts = el('div', { class: 'count-row' },
    countChip('Pending', tasks.pending, 'muted'),
    countChip('Completed', tasks.completed, 'ok'),
    countChip('Failed', tasks.failed, 'danger'),
  );

  return el('section', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } }, 'Progress')),
    el('div', { class: 'panel-body' },
      rows,
      counts,
      el('div', { class: 'card-sub', style: { marginTop: '10px' } },
        `Sink: ${output.sink || 'jsonl'} · ` +
        `${num(output.jsonl_rows)} rows · ` +
        `${num((output.jsonl_files || []).length)} file(s)`),
    ),
  );
}

function progressRow(label, percent, detail) {
  const clamped = Math.max(0, Math.min(100, Number(percent) || 0));
  return el('div', { class: 'progress-row' },
    el('span', { class: 'muted', title: label }, label),
    el('div', { class: 'progress-track' },
      el('div', { class: 'progress-fill', style: { width: `${clamped}%` } })),
    el('span', { class: 'progress-pct' }, pct(clamped, 0)),
    detail ? el('span', { class: 'progress-detail' }, detail) : null,
  );
}

function countChip(label, value, colour) {
  return el('span', { class: 'count-chip' },
    el('span', { class: 'muted' }, label),
    el('span', { class: `pill ${colour}` }, num(value ?? 0)),
  );
}

// --- the issue graph ----------------------------------------------------

function graphPanel(stats) {
  const log = stats.regex_watch_log || [];
  const series = GRAPH_SERIES.map(({ key, name, token }) => ({
    name,
    colour: getComputedStyle(document.documentElement)
      .getPropertyValue(token).trim(),
    values: cumulativeSeries(stats, key, log.length),
  }));

  const host = el('div', { class: 'chart-host' });

  const panel = el('section', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } },
        'Issues over time')),
    el('div', { class: 'panel-body chart-body' },
      host,
      legend(series),
    ),
  );

  // Draw only once the panel is in the document. `lineChart` reads the palette
  // from `getComputedStyle`, which returns empty strings for a detached node, so a
  // draw before insertion produces an invisible chart rather than an error.
  //
  // **A frame is not guaranteed.** A background tab, a throttled renderer or a
  // headless capture may not produce one for seconds, and the old code drew *only*
  // from `requestAnimationFrame`, so the graph stayed blank until something else
  // forced a paint. The timeout is the fallback: whichever fires first draws, and the
  // second call is a no-op because the host already has content.
  drawWhenReady(panel, host, () => lineChart(host, {
    labels: log.map((entry) => String(entry.time ?? '')),
    series,
    empty: 'The issue graph fills in as a run produces issues.',
  }));

  return panel;
}

/**
 * A cumulative series for one counter.
 *
 * The payload's `recent` list is the last 50 issues, not a time series, so the graph
 * is built by walking the per-issue timestamps in order and counting up. That is what
 * the Tk graph shows: how the count grew over the run, not an instantaneous rate.
 */
/**
 * Run `draw` once `panel` is in the document, on the next frame or shortly after.
 *
 * `lineChart` needs the node connected (it reads the palette from `getComputedStyle`),
 * and it needs a layout pass. `requestAnimationFrame` gives both -- but only if the
 * browser produces a frame, which a background tab or a throttled renderer may not do
 * for a long time. The timeout is the safety net, and `host.childElementCount` makes
 * the second caller a no-op so the chart is never drawn twice.
 */
function drawWhenReady(panel, host, draw) {
  let done = false;
  const attempt = () => {
    if (done || !panel.isConnected || host.childElementCount) return;
    done = true;
    draw();
  };
  requestAnimationFrame(attempt);
  window.setTimeout(attempt, 250);
}

function normaliseIssue(raw) {
  // The engine sends `{time, phrase, context, api}`. This still accepts a bare
  // `[phrase, context, api]` array, because a payload from a process started before the
  // engine switched to dicts is a real possibility and a blank table is the worst outcome.
  if (Array.isArray(raw)) {
    return {
      time: '',
      phrase: String(raw[0] ?? ''),
      context: String(raw[1] ?? ''),
      api: Number.isInteger(raw[2]) ? raw[2] : -1,
    };
  }
  return {
    time: String(raw?.time ?? ''),
    phrase: String(raw?.phrase ?? ''),
    context: String(raw?.context ?? ''),
    api: Number.isInteger(raw?.api) ? raw.api : -1,
  };
}

function cumulativeSeries(stats, key, length) {
  const entry = (stats.detected || {})[key] || {};
  const recent = entry.recent || [];
  const total = Number(entry.total ?? 0);
  const seen = recent.length;
  const before = Math.max(0, total - seen);
  const values = [];
  let running = before;
  for (let index = 0; index < Math.max(length, seen); index += 1) {
    if (index < seen) running += 1;
    values.push(running);
  }
  return values;
}

// --- diagnostics --------------------------------------------------------

function diagnostics(stats) {
  const generator = stats.generator || {};
  const output = stats.output || {};
  const database = output.database || {};
  const detection = stats.detection_config || {};
  const filtered = stats.filtered || {};

  const rows = [
    ['Version', generator.version || '—'],
    ['Phase', generator.phase || '—'],
    ['PID', generator.pid ?? '—'],
    ['Uptime', `${num(generator.uptime_s, 1)} s`],
    ['Config', generator.config_path || '—'],
    ['Sink', output.sink || '—'],
    ['Database', database.enabled
      ? (database.connected ? `connected, ${num(database.rows)} rows` : 'enabled, not connected')
      : 'disabled'],
    ['Write failures', num(filtered.write_failures)],
    ['Review queue', num(filtered.review_queue)],
    ['Dropped', num(filtered.total)],
  ];

  const armed = Object.entries(detection)
    .filter(([, value]) => value && value.enabled)
    .map(([key]) => key);

  const body = el('div', {},
    el('table', { class: 'grid diag-table' },
      el('tbody', {}, rows.map(([label, value]) => el('tr', {},
        el('td', { class: 'muted', style: { width: '38%' } }, label),
        el('td', { title: String(value) }, String(value)),
      ))),
    ),
    el('div', { class: 'card-sub', style: { marginTop: '8px' } },
      armed.length ? `Checks armed: ${armed.join(', ')}` : 'No checks armed.'),
  );

  return disclosureBlock('diagnostics', 'Database & diagnostics',
    'Hide diagnostics', body);
}


