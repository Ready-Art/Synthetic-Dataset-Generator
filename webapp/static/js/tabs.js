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
 * The six data tabs: APIs, Quality, Regex Watch, Metrics, Prompts, Queue.
 *
 * One module rather than six, because they share a shape: fetch a JSON document, render a
 * table or a set of panels, re-fetch on demand. Six files would be six copies of the same
 * `fetch -> render -> error` dance.
 *
 * **Every tab re-fetches on mount and on Refresh, never from a cached copy.** A run outlives the
 * browser tab, so the server is the only source of truth and a stale list is worse than a
 * loading state.
 *
 * **The tabs are read-only except the Queue.** Editing a prompt happens in the Config tab, which is
 * the only editor; the Prompts tab previews and copies. The Queue's three buttons are the
 * only mutations here, and each says what it does before it does it.
 */

import { el, emptyState, num, toast } from './dom.js';
import { lineChart } from './chart.js';

// --- a tiny fetch helper -------------------------------------------------

/**
 * Fetch JSON from an API route.
 *
 * A 401 means the session expired, so the page goes to the login screen rather than showing a
 * broken tab. Any other failure becomes a thrown Error the caller renders as an empty state.
 */
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
  if (!body.ok) throw new Error(body.error?.message || 'The request failed.');
  return body;
}

/**
 * A tab that fetches on mount and on Refresh.
 *
 * `build` receives the payload and returns a screen element. `error` is rendered in place of the
 * body, so a failed fetch never leaves a blank screen with no explanation.
 */
function makeTab({ id, title, path, build, actions }) {
  const state = { data: null, error: null, loading: false, mounted: false };

  async function refresh() {
    state.loading = true;
    try {
      state.data = await getJSON(path);
      state.error = null;
    } catch (error) {
      state.error = String(error.message || error);
    } finally {
      state.loading = false;
      repaint();
    }
  }

  function repaint() {
    const main = document.getElementById('main');
    if (!main) return;
    const current = main.querySelector(`#screen-${id}`);
    if (!current) return;
    current.replaceChildren(...screen().childNodes);
  }

  function screen() {
    const body = el('div', { class: 'tab-body' });
    if (state.error) {
      body.append(emptyState({ title: 'Could not load this tab', hint: state.error }));
    } else if (!state.data) {
      body.append(el('div', { class: 'empty' }, 'Loading…'));
    } else {
      body.append(build(state.data, refresh));
    }
    return el('div', { class: 'screen active', id: `screen-${id}` },
      el('div', { class: 'tab-toolbar' },
        el('span', { class: 'section-header', style: { margin: '0' } }, title),
        el('span', { class: 'spacer' }),
        actions ? actions(state.data, refresh) : null,
        el('button', { type: 'button', onclick: () => refresh() }, 'Refresh'),
      ),
      body,
    );
  }

  return {
    render() {
      if (!state.mounted) {
        state.mounted = true;
        refresh();
      }
      return screen();
    },
    unmount() {
      state.mounted = false;
    },
  };
}

/** A panel with a header and a body. */
function panel(title, ...children) {
  return el('section', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } }, title)),
    el('div', { class: 'panel-body' }, ...children),
  );
}

/** A table from a header row and a list of row arrays. `cell` may be a string or a node. */
function table(headers, rows, { empty = 'Nothing to show yet.', widths } = {}) {
  if (!rows.length) return emptyState({ title: empty });
  const head = el('tr', {}, ...headers.map((text) => el('th', {}, text)));
  const body = el('tbody', {});
  for (const row of rows) {
    body.append(el('tr', {}, ...row.map((cell) => el('td', {}, cell))));
  }
  return el('table', { class: 'grid' },
    widths ? el('colgroup', {}, ...widths.map((w) => el('col', { style: { width: w } }))) : null,
    el('thead', {}, head),
    body,
  );
}

// --- APIs ----------------------------------------------------------------

const apisTab = makeTab({
  id: 'apis',
  title: 'APIs',
  path: '/api/apis',
  build(data) {
    const slots = data.slots || [];
    const rows = slots.map((slot) => [
      el('span', { class: 'mono' }, slot.label),
      el('span', { class: 'pill ' + (slot.enabled ? 'ok' : 'muted') },
        slot.enabled ? 'enabled' : 'disabled'),
      slot.url || el('span', { class: 'muted' }, 'not set'),
      slot.model || el('span', { class: 'muted' }, '—'),
      // The key itself never reaches the browser; only whether one is set.
      el('span', { class: 'pill ' + (slot.has_key ? 'ok' : 'danger') },
        slot.has_key ? 'key set' : 'no key'),
      num(slot.threads),
      num(slot.rate_limit_rpm),
      num(slot.attempts),
    ]);

    const detected = data.detected || {};
    const counterKeys = Object.keys(detected);
    const detectedRows = slots.map((slot) => {
      const cells = [el('span', { class: 'mono' }, slot.label)];
      for (const key of counterKeys) {
        cells.push(num((detected[key] || {})[String(slot.slot)] || 0));
      }
      return cells;
    });

    return el('div', { class: 'tab-grid' },
      panel('API slots',
        table(['Slot', 'State', 'URL', 'Model', 'Key', 'Threads', 'RPM', 'Attempts'],
          rows, { empty: 'No API slots are configured.', widths: ['70px', '90px', '28%', '20%', '90px', '80px', '70px', '80px'] })),
      panel('Detected per slot',
        counterKeys.length
          ? table(['Slot', ...counterKeys.map((key) => key.replace(/_/g, ' '))],
            detectedRows, { empty: 'No detections recorded yet.' })
          : emptyState({
            title: 'No detections yet',
            hint: 'Detections appear here as the run finds them.',
          })),
      panel('Pricing',
        el('div', { class: 'count-row' },
          el('span', {}, 'Cost per 1k tokens'),
          el('span', { class: 'mono' },
            num((data.pricing || {}).cost_per_1k_tokens, 4))),
        el('div', { class: 'count-row' },
          el('span', {}, 'Budget limit'),
          el('span', { class: 'mono' }, num((data.pricing || {}).budget_limit, 2))),
        el('div', { class: 'count-row' },
          el('span', {}, 'Master duplication mode'),
          el('span', { class: 'mono' },
            data.master_duplication_mode ? 'on' : 'off'))),
    );
  },
});

// --- Quality -------------------------------------------------------------

const qualityTab = makeTab({
  id: 'quality',
  title: 'Quality',
  path: '/api/quality',
  build(data) {
    const summary = data.summary || {};
    const threshold = data.threshold ?? 50;
    const dimensions = summary.dimension_averages || {};

    const dimensionRows = Object.entries(dimensions).map(([name, value]) => {
      const level = Number(value) < threshold ? 'danger'
        : Number(value) < 70 ? 'warn' : 'ok';
      return [
        el('span', {}, name),
        el('span', { class: `score ${level}` }, num(value, 1)),
        el('span', { class: 'muted' }, Number(value) < threshold ? 'below threshold' : ''),
      ];
    });

    const recent = data.recent || [];
    const recentRows = recent.map((row) => {
      const level = Number(row.composite) < 40 ? 'critical' : 'warning';
      return [
        el('span', { class: 'mono', title: row.task_id }, row.task_id),
        el('span', { class: `score ${level}` }, num(row.composite)),
        el('span', { class: 'muted' }, (row.flags || []).join(', ') || '—'),
        el('span', { class: 'muted' }, row.method || '—'),
        el('span', { class: 'muted' }, row.scored_at || '—'),
      ];
    });

    const cards = el('div', { class: 'metric-row' },
      metric('Scored', num(summary.count)),
      metric('Average', num(summary.avg_composite, 1)),
      metric('Lowest', num(summary.min_composite, 1)),
      metric('Highest', num(summary.max_composite, 1)),
      metric('Below threshold', num(summary.threshold_failures)),
      metric('Threshold', num(threshold)),
    );

    return el('div', { class: 'tab-grid' },
      cards,
      data.error ? el('div', { class: 'detail-note' }, data.error) : null,
      panel('Dimension averages',
        table(['Dimension', 'Average', 'Note'], dimensionRows,
          { empty: 'No scores recorded yet.' })),
      panel('Latest scores',
        table(['Task', 'Score', 'Flags', 'Method', 'Scored at'], recentRows,
          { empty: 'No scores recorded yet.',
            widths: ['26%', '70px', '24%', '70px', '20%'] })),
    );
  },
});

function metric(caption, value) {
  return el('div', { class: 'card' },
    el('div', { class: 'card-caption' }, caption),
    el('div', { class: 'card-value', title: String(value) }, String(value)),
  );
}

/**
 * A palette token as a colour string.
 *
 * The chart draws into an SVG, which cannot use `var(--accent)` in every attribute, so the
 * computed value is read once. No hex literal lives in this file: the colour comes
 * from `theme.py` through the stylesheet.
 */
function tokenColour(name, fallback = 'currentColor') {
  const value = getComputedStyle(document.documentElement)
    .getPropertyValue(name).trim();
  return value || fallback;
}

// --- Regex Watch ---------------------------------------------------------

const regexTab = makeTab({
  id: 'regex',
  title: 'Regex Watch',
  path: '/api/regex',
  build(data) {
    const patterns = data.patterns || [];
    const rows = patterns.map((row) => [
      el('span', { class: 'mono' }, row.pattern),
      el('span', { class: 'mono muted' }, row.replacement || '—'),
      el('span', { class: 'pill ' + (row.enabled ? 'ok' : 'muted') },
        row.enabled ? 'on' : 'off'),
      num(row.hits),
    ]);

    const hits = data.hits || [];
    const hitRows = hits.slice().reverse().map((hit) => [
      el('span', { class: 'muted' }, hit.time || hit.timestamp || '—'),
      el('span', { class: 'mono' }, hit.pattern || '—'),
      el('span', { class: 'muted', title: hit.context || '' },
        (hit.context || '—').slice(0, 120)),
    ]);

    return el('div', { class: 'tab-grid' },
      el('div', { class: 'count-row' },
        el('span', { class: 'pill ' + (data.enabled ? 'ok' : 'muted') },
          data.enabled ? 'watching' : 'off'),
        el('span', { class: 'muted' },
          `${num(data.total_hits)} hit(s) · max ${num(data.max_rounds)} round(s)`)),
      panel('Patterns', table(['Pattern', 'Replacement', 'State', 'Hits'], rows,
        { empty: 'No patterns configured. Add them in the Config tab.',
          widths: ['34%', '30%', '70px', '70px'] })),
      panel('Recent hits', table(['Time', 'Pattern', 'Context'], hitRows,
        { empty: 'No hits recorded yet.', widths: ['110px', '22%', ''] })),
    );
  },
});

// --- Metrics -------------------------------------------------------------

const metricsTab = makeTab({
  id: 'metrics',
  title: 'Metrics',
  path: '/api/metrics',
  build(data) {
    const slots = data.slots || [];
    if (!slots.length) {
      return emptyState({
        title: 'No vLLM metrics yet',
        hint: 'They fill in as the poller reaches each endpoint during a run.',
      });
    }

    const columns = ['Slot'];
    const seen = new Set();
    for (const slot of slots) {
      for (const key of Object.keys(slot.usage || {})) seen.add(key);
    }
    columns.push(...[...seen].sort());

    const rows = slots.map((slot) => {
      const cells = [el('span', { class: 'mono' }, slot.label)];
      for (const key of columns.slice(1)) {
        const value = (slot.usage || {})[key];
        cells.push(typeof value === 'number' ? num(value, 2) : (value ?? '—'));
      }
      return cells;
    });

    const graphs = slots.map((slot) => {
      const host = el('div', { class: 'chart-host' });
      const history = slot.history || [];
      // `lineChart(host, ...)` fills the host rather than returning a node, and it reads
      // the palette via getComputedStyle -- which returns empty strings for a detached
      // node. So the draw waits for insertion, or the lines all take the fallback colour.
      requestAnimationFrame(() => {
        if (!host.isConnected) return;
        lineChart(host, {
          labels: history.map((point) => String(point.time ?? '')),
          series: [{
            name: slot.label,
            colour: tokenColour('--accent'),
            values: history.map((point) => point.tokens_per_second),
          }],
          empty: 'Throughput fills in as the poller reaches this endpoint.',
        });
      });
      return el('div', { class: 'panel' },
        el('div', { class: 'panel-head' },
          el('span', { class: 'section-header', style: { margin: '0' } },
            `${slot.label} throughput`)),
        el('div', { class: 'panel-body chart-body' }, host));
    });

    return el('div', { class: 'tab-grid' },
      panel('Per-slot usage',
        table(columns, rows, { empty: 'No usage recorded yet.' })),
      el('div', { class: 'tab-grid two' }, ...graphs),
    );
  },
});

// --- Prompts -------------------------------------------------------------

const promptsTab = makeTab({
  id: 'prompts',
  title: 'Prompts',
  path: '/api/prompts',
  build(data) {
    const entries = data.entries || [];
    if (!entries.length) {
      return emptyState({
        title: 'No prompts in the config',
        hint: 'Add a prompt in the Config editor, then return here to preview it.',
      });
    }

    const list = el('div', { class: 'prompt-list' });
    const preview = el('div', { class: 'prompt-preview' },
      emptyState({ title: 'No prompt selected', hint: 'Select a prompt to preview it here.' }));

    function show(entry) {
      preview.replaceChildren(
        el('div', { class: 'detail-head' },
          el('span', { class: 'mono' }, entry.path),
          el('span', { class: 'spacer' }),
          el('span', { class: 'muted' },
            `${num(entry.length)} chars · ${num(entry.lines)} line(s)`)),
        el('pre', { class: 'prompt-text' }, entry.text || '(empty)'),
        el('div', { class: 'tab-toolbar' },
          el('button', {
            type: 'button',
            onclick: async () => {
              try {
                await navigator.clipboard.writeText(entry.text || '');
                toast(`Copied ${entry.path}`, 'ok');
              } catch (error) {
                toast('Could not copy to the clipboard.', 'danger');
              }
            },
          }, 'Copy'),
          el('span', { class: 'muted' },
            'Edit this prompt in the Config tab.')),
      );
    }

    for (const entry of entries) {
      const row = el('button', {
        class: 'prompt-row',
        type: 'button',
        title: entry.path,
      },
        el('span', { class: 'mono' }, entry.label),
        el('span', { class: 'muted' }, `${num(entry.length)} chars`));
      row.addEventListener('click', () => {
        for (const other of list.children) other.classList.remove('selected');
        row.classList.add('selected');
        show(entry);
      });
      list.append(row);
    }

    return el('div', { class: 'prompt-grid' }, list, preview);
  },
});

// --- Queue ---------------------------------------------------------------

const queueTab = makeTab({
  id: 'queue',
  title: 'Queue',
  path: '/api/queue',
  actions(data, refresh) {
    const purge = el('button', {
      class: 'btn-danger',
      type: 'button',
      onclick: async () => {
        if (!window.confirm(
          'Remove all pending tasks? They will be skipped by workers. ' +
          'Nothing is deleted from the dataset.')) return;
        try {
          const body = await postJSON('/api/queue/purge', {});
          toast(`Purged ${num(body.data.purged)} pending task(s).`, 'ok');
          refresh();
        } catch (error) {
          toast(String(error.message || error), 'danger');
        }
      },
    }, 'Purge pending');

    const retry = el('button', {
      type: 'button',
      onclick: async () => {
        const count = (data.counts || {}).failed || 0;
        if (!count) {
          toast('No failed tasks to retry.', 'info');
          return;
        }
        if (!window.confirm(`Retry ${count} failed task(s)?`)) return;
        try {
          const body = await postJSON('/api/queue/retry', {});
          toast(`Re-queued ${num(body.data.retried)} task(s).`, 'ok');
          refresh();
        } catch (error) {
          toast(String(error.message || error), 'danger');
        }
      },
    }, 'Retry failed');

    const exportButton = el('button', {
      type: 'button',
      onclick: async () => {
        try {
          const body = await postJSON('/api/queue/export', {});
          toast(`Wrote ${num(body.data.rows)} row(s) to ${body.data.path}`, 'ok', 6000);
        } catch (error) {
          toast(String(error.message || error), 'danger');
        }
      },
    }, 'Export CSV');

    return el('span', { class: 'button-row' }, purge, retry, exportButton);
  },
  build(data) {
    const tables = data.tables || {};
    const panels = [];
    const messages = {
      pending: 'No pending tasks. Press Start to queue a run.',
      completed: 'No completed tasks yet.',
      failed: 'No failed tasks. Nothing to retry.',
    };
    for (const state of ['pending', 'completed', 'failed']) {
      const rows = (tables[state] || []).map((row) => [
        el('span', { class: 'mono', title: row.task_id }, row.task_id),
        el('span', { class: 'muted' }, row.file || 'N/A'),
        num(row.retries),
      ]);
      panels.push(panel(
        `${state[0].toUpperCase()}${state.slice(1)} (${num((data.counts || {})[state] || 0)})`,
        table(['Task ID', 'Source file', 'Retries'], rows,
          { empty: messages[state], widths: ['40%', '44%', '70px'] })));
    }
    return el('div', { class: 'tab-grid' }, ...panels);
  },
});

// --- the tab registry ----------------------------------------------------

export const DATA_TABS = [
  { id: 'apis', label: 'APIs', tab: apisTab },
  { id: 'quality', label: 'Quality', tab: qualityTab },
  { id: 'regex', label: 'Regex Watch', tab: regexTab },
  { id: 'metrics', label: 'Metrics', tab: metricsTab },
  { id: 'prompts', label: 'Prompts', tab: promptsTab },
  { id: 'queue', label: 'Queue', tab: queueTab },
];
