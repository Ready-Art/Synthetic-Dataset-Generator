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
 * The data tabs: APIs, Quality, Regex Watch, Metrics, Prompts, Live Prompts, Queue.
 *
 * One module rather than seven, because they share a shape: fetch a JSON document, render a
 * table or a set of panels, re-fetch on demand. Seven files would be seven copies of the same
 * `fetch -> render -> error` dance.
 *
 * **Every tab re-fetches on mount and on Refresh, never from a cached copy.** A run outlives the
 * browser tab, so the server is the only source of truth and a stale list is worse than a
 * loading state.
 *
 * **The tabs are read-only except the Queue.** Editing a prompt happens in the Config tab, which is
 * the only editor; the Prompts tab previews and copies. The Queue's four buttons are the
 * only mutations here, and each says what it does before it does it.
 */

import { el, emptyState, num, toast } from './dom.js';
import { lineChart, legend, rateHistory } from './chart.js';

// --- a tiny fetch helper -------------------------------------------------

/**
 * Read a response body as JSON, **without letting a non-JSON body become a JSON error**.
 *
 * A route that raises an unhandled exception answers `500` with the plain text
 * `Internal Server Error`, not JSON. Calling `response.json()` on that throws
 * `SyntaxError: Unexpected token 'I', "Internal S..." is not valid JSON`, which is what a
 * reader saw as "JSON parse unexpected character at line 1 column 1" -- a message about the
 * *parser* for a bug that was a `500` from the *server*. The real cause was invisible.
 *
 * So a body is only parsed when the response actually claims to be JSON. Anything else becomes
 * an Error that names the status and the body, which is the thing the reader needs to see.
 *
 * @param {Response} response
 * @returns {Promise<Object>}
 */
async function readJSON(response) {
  const type = (response.headers.get('content-type') || '').toLowerCase();
  if (!type.includes('json')) {
    const text = (await response.text().catch(() => '')).trim();
    const detail = text.slice(0, 200) || response.statusText || 'no body';
    throw new Error(
      `The server answered ${response.status} with a non-JSON body: ${detail}`);
  }
  try {
    return await response.json();
  } catch (error) {
    throw new Error(
      `The server answered ${response.status} with a body that is not valid JSON.`);
  }
}

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
  const body = await readJSON(response);
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
  const body = await readJSON(response);
  if (!body.ok) throw new Error(body.error?.message || 'The request failed.');
  return body;
}

/**
 * A tab that fetches on mount and on Refresh.
 *
 * `build` receives the payload and returns a screen element. `error` is rendered in place of the
 * body, so a failed fetch never leaves a blank screen with no explanation.
 *
 * `interval` (ms) makes the tab poll while it is mounted. Only the Live Prompts tab uses it: a
 * debugging view of what is being sent right now is worthless if it only updates on a click, and
 * every other tab's data changes slowly enough that a Refresh button is honest.
 */
function makeTab({ id, title, path, build, actions, interval }) {
  const state = { data: null, error: null, loading: false, mounted: false, timer: null };

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
        if (interval) {
          state.timer = window.setInterval(() => {
            // A poll must not fight a repaint the reader triggered, and must not run after
            // the tab is gone.
            if (!state.loading && document.getElementById(`screen-${id}`)) refresh();
          }, interval);
        }
      }
      return screen();
    },
    unmount() {
      state.mounted = false;
      if (state.timer !== null) {
        window.clearInterval(state.timer);
        state.timer = null;
      }
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

/**
 * Run `draw` once `panel` is in the document, on the next frame or shortly after.
 *
 * `lineChart` needs a connected node and a layout pass, and `requestAnimationFrame` alone is not
 * a guarantee -- a throttled renderer may not produce a frame for seconds, which left the graph
 * blank. `host.childElementCount` makes the second caller a no-op, so the chart is never drawn
 * twice.
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

// Which of the two Metrics panels the reader has switched on. Module scope so a Refresh (or a
// tab switch and return) keeps the choice, and so the two toggles are independent -- the brief
// asked for one toggle each, not one combined switch.
const metricsView = { vllm: true, antislop: false };

/** A labelled switch. `onchange` receives the new boolean. */
function toggle(label, checked, onchange, hint) {
  const input = el('input', {
    type: 'checkbox',
    class: 'switch-input',
    checked: checked ? 'checked' : null,
  });
  input.checked = Boolean(checked);
  input.addEventListener('change', () => onchange(input.checked));
  return el('label', { class: 'switch', title: hint || label },
    input,
    el('span', { class: 'switch-track' }, el('span', { class: 'switch-thumb' })),
    el('span', { class: 'switch-label' }, label),
  );
}

/** The per-slot own-throughput panel. Always available: the generation threads write it. */
function slotThroughputPanels(data) {
  const slots = data.slots || [];
  if (!slots.length) {
    return [emptyState({
      title: 'No per-slot throughput yet',
      hint: 'A slot records its own tokens once it has completed a request.',
    })];
  }
  const graphs = slots.map((slot) => {
    const host = el('div', { class: 'chart-host' });
    const history = slot.history || [];
    const card = el('div', { class: 'panel' },
      el('div', { class: 'panel-head' },
        el('span', { class: 'section-header', style: { margin: '0' } },
          `${slot.label} throughput`)),
      el('div', { class: 'panel-body chart-body' }, host));
    // `lineChart(host, ...)` fills the host rather than returning a node, and it reads the
    // palette via getComputedStyle -- which returns empty strings for a detached node. So the
    // draw waits for insertion, or the lines all take the fallback colour.
    drawWhenReady(card, host, () => lineChart(host, {
      labels: history.map((point) => String(point.time ?? '')),
      series: [{
        name: slot.label,
        colour: tokenColour('--accent'),
        values: history.map((point) => point.tokens_per_second),
      }],
      empty: 'Throughput fills in as the poller reaches this endpoint.',
      label: `${slot.label} tokens per second`,
    }));
    return card;
  });
  return graphs;
}

/**
 * The vLLM server panels: prompt and generation speed, KV cache load, and the queue depth.
 *
 * These read the **server's** `/metrics`, so two slots on one server share a line. That is the
 * truth rather than a bug: vLLM's figures are server-wide, and the per-slot panel above is the
 * one that separates them.
 */
function vllmServerPanels(data) {
  const endpoints = data.endpoints || [];
  if (!endpoints.length) {
    return [emptyState({
      title: 'No vLLM endpoint configured',
      hint: 'Point an API slot at a vLLM server and the poller will read its /metrics.',
    })];
  }
  if (!data.vllm_available) {
    const errors = endpoints
      .filter((row) => row.error)
      .map((row) => `${row.label}: ${row.error}`);
    return [emptyState({
      title: 'No vLLM server answered',
      hint: errors.length
        ? errors.join(' · ')
        : 'The poller has not reached an endpoint yet.',
    })];
  }

  const colours = ['--accent', '--ok', '--warn', '--danger', '--muted'];
  const seriesFor = (key, suffix) => endpoints
    .filter((row) => !row.error)
    .map((row, index) => ({
      name: `${row.label}${suffix ? ` ${suffix}` : ''}`,
      colour: tokenColour(colours[index % colours.length]),
      values: (row.history || []).map((sample) => sample[key]),
    }));

  const labels = (endpoints.find((row) => (row.history || []).length)?.history || [])
    .map((sample) => {
      // A relative label ("seconds ago") is the only one
      // that stays readable when the ring is a rolling window.
      const newest = sample.time;
      return String(newest ?? '');
    });

  const panels = [];

  const speedHost = el('div', { class: 'chart-host' });
  const speedSeries = [
    ...seriesFor('prompt_tps', 'prompt'),
    ...seriesFor('generation_tps', 'gen'),
  ];
  const speedPanel = el('div', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } },
        'Server throughput (tok/s)')),
    el('div', { class: 'panel-body chart-body' }, speedHost, legend(speedSeries)));
  drawWhenReady(speedPanel, speedHost, () => lineChart(speedHost, {
    labels,
    series: speedSeries,
    empty: 'Server speeds appear after the poller has two samples.',
    label: 'vLLM prompt and generation tokens per second',
  }));
  panels.push(speedPanel);

  const loadHost = el('div', { class: 'chart-host' });
  const loadSeries = seriesFor('load_percent');
  const loadPanel = el('div', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } },
        'KV cache load (%)')),
    el('div', { class: 'panel-body chart-body' }, loadHost, legend(loadSeries)));
  drawWhenReady(loadPanel, loadHost, () => lineChart(loadHost, {
    labels,
    series: loadSeries,
    empty: 'Load appears after the poller has two samples.',
    label: 'vLLM KV cache load percent',
  }));
  panels.push(loadPanel);

  const queueHost = el('div', { class: 'chart-host' });
  const queueSeries = [
    ...seriesFor('running', 'running'),
    ...seriesFor('waiting', 'waiting'),
  ];
  const queuePanel = el('div', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } },
        'Requests running vs waiting')),
    el('div', { class: 'panel-body chart-body' }, queueHost, legend(queueSeries)));
  drawWhenReady(queuePanel, queueHost, () => lineChart(queueHost, {
    labels,
    series: queueSeries,
    empty: 'A vLLM server reports running and waiting counts on its /metrics.',
    label: 'vLLM running and waiting requests',
  }));
  panels.push(queuePanel);

  return panels;
}

/**
 * The antislop-vllm fork's own panels: request rate, bans and back-tracks, and generation speed.
 *
 * Only rows that actually served `antislop:` metrics contribute. A plain vLLM therefore shows the
 * panel's own explanation rather than three empty axes, which is the honest reading: there is no
 * proxy here. A proxied slot with no bans in the window is *good news*, and says so instead of
 * drawing a flat zero line that reads as broken.
 */
function antislopPanels(data) {
  const rows = (data.endpoints || []).filter((row) => row.is_antislop);
  if (!rows.length) {
    return [emptyState({
      title: 'No antislop-vllm-fork proxy metrics',
      hint: 'Point an API slot at the fork to see its request, ban and back-track rates. '
        + 'A plain vLLM serves no antislop: metrics.',
    })];
  }

  const colours = ['--accent', '--ok', '--warn', '--danger', '--muted'];
  const seriesFor = (key, suffix, transform) => rows.map((row, index) => {
    const history = row.history || [];
    const values = transform
      ? transform(history, key).map((sample) => sample.rate)
      : history.map((sample) => sample[key]);
    return {
      name: `${row.label}${suffix ? ` ${suffix}` : ''}`,
      colour: tokenColour(colours[index % colours.length]),
      values,
    };
  });

  const labels = (rows.find((row) => (row.history || []).length)?.history || [])
    .map((sample) => String(sample.time ?? ''));

  const panels = [];

  const requestHost = el('div', { class: 'chart-host' });
  const requestSeries = seriesFor('antislop_requests', 'requests/min', rateHistory);
  const requestPanel = el('div', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } },
        'Proxy requests per minute')),
    el('div', { class: 'panel-body chart-body' }, requestHost, legend(requestSeries)));
  drawWhenReady(requestPanel, requestHost, () => lineChart(requestHost, {
    labels,
    series: requestSeries,
    empty: 'The proxy request rate appears after two samples.',
    label: 'antislop proxy requests per minute',
  }));
  panels.push(requestPanel);

  const banHost = el('div', { class: 'chart-host' });
  const banSeries = [
    ...seriesFor('antislop_ban_rate', 'bans/min'),
    ...seriesFor('antislop_backtrack_rate', 'back-tracks/min'),
  ];
  const banPanel = el('div', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } },
        'Bans and back-tracks per minute')),
    el('div', { class: 'panel-body chart-body' }, banHost, legend(banSeries)));
  drawWhenReady(banPanel, banHost, () => lineChart(banHost, {
    labels,
    series: banSeries,
    empty: 'No bans or back-tracks in this window. That is the healthy case.',
    label: 'antislop bans and back-tracks per minute',
  }));
  panels.push(banPanel);

  const speedHost = el('div', { class: 'chart-host' });
  const speedSeries = seriesFor('antislop_generation_tps', 'tok/s');
  const speedPanel = el('div', { class: 'panel' },
    el('div', { class: 'panel-head' },
      el('span', { class: 'section-header', style: { margin: '0' } },
        'Proxy generation speed (tok/s)')),
    el('div', { class: 'panel-body chart-body' }, speedHost, legend(speedSeries)));
  drawWhenReady(speedPanel, speedHost, () => lineChart(speedHost, {
    labels,
    series: speedSeries,
    empty: 'The proxy reports its generation speed after two samples.',
    label: 'antislop proxy generation tokens per second',
  }));
  panels.push(speedPanel);

  return panels;
}

/** The per-endpoint table: one row per server, with the figures the graphs plot. */
function endpointTable(data) {
  const endpoints = data.endpoints || [];
  const rows = endpoints.map((row) => [
    el('span', { class: 'mono' }, row.label || `API ${row.slot_idx + 1}`),
    el('span', { class: 'mono muted', title: row.metrics_url || '' },
      row.model || (row.error ? 'error' : '—')),
    row.error
      ? el('span', { class: 'pill danger', title: row.error }, 'unreachable')
      : el('span', { class: 'pill ok' }, 'polled'),
    num(row.prompt_tps, 1),
    num(row.generation_tps, 1),
    num(row.load_percent, 1),
    num(row.running),
    num(row.waiting),
  ]);
  return table(
    ['Slot', 'Model', 'State', 'Prompt tok/s', 'Gen tok/s', 'Load %', 'Running', 'Waiting'],
    rows,
    { empty: 'No vLLM endpoint has been polled yet.',
      widths: ['80px', '22%', '90px', '90px', '80px', '70px', '70px', '70px'] });
}

const metricsTab = makeTab({
  id: 'metrics',
  title: 'Metrics',
  path: '/api/metrics',
  build(data, refresh) {
    // Two independent switches, in the toolbar. The brief asked for one each: the per-slot
    // throughput the generator measures itself, and the server/proxy figures the poller reads.
    // Both default to what a reader expects -- own throughput on, proxy off, because a plain
    // vLLM serves no antislop: metrics and an empty panel is worse than no panel.
    const toolbar = el('div', { class: 'metrics-switches' },
      toggle('Per-slot throughput', metricsView.vllm, (value) => {
        metricsView.vllm = value;
        refresh();
      }, 'Each API slot\'s own measured tokens per second'),
      toggle('vLLM & antislop-vLLM server metrics', metricsView.antislop, (value) => {
        metricsView.antislop = value;
        refresh();
      }, 'The server\'s /metrics: speeds, KV cache load, queue depth and the proxy\'s counters'),
    );

    const sections = [toolbar];

    if (metricsView.vllm) {
      sections.push(el('div', { class: 'section-heading' }, 'Per-slot throughput'));
      sections.push(el('div', { class: 'tab-grid two' }, ...slotThroughputPanels(data)));
    }

    if (metricsView.antislop) {
      sections.push(el('div', { class: 'section-heading' }, 'vLLM server metrics'));
      sections.push(panel('Endpoints', endpointTable(data)));
      sections.push(el('div', { class: 'tab-grid two' }, ...vllmServerPanels(data)));
      sections.push(el('div', { class: 'section-heading' }, 'antislop-vllm proxy'));
      sections.push(el('div', { class: 'tab-grid two' }, ...antislopPanels(data)));
    }

    return el('div', { class: 'tab-grid' }, ...sections);
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

// --- Live Prompts --------------------------------------------------------

// Which entry the reader has open, by `seq`. Module scope so the 2s poll does not close it.
let liveSelected = null;
// The list's scroll offset, also at module scope. A repaint replaces the whole screen, so a
// reader who has scrolled back through a retry sequence would otherwise be thrown to the top
// every two seconds by the poll -- the same complaint the Overview's Recent issues had.
let liveScrollTop = 0;

const livePromptsTab = makeTab({
  id: 'live',
  title: 'Live Prompts',
  path: '/api/live-prompts',
  // A debugging view of what is being sent right now is worthless if it only updates on a
  // click. Two seconds is the same tick the stats socket uses.
  interval: 2000,
  actions(data, refresh) {
    return el('button', {
      type: 'button',
      onclick: async () => {
        if (!window.confirm('Clear the prompt history? The run is not affected.')) return;
        try {
          const body = await postJSON('/api/live-prompts/clear', {});
          toast(`Cleared ${num(body.data.cleared)} prompt(s).`, 'ok');
          liveSelected = null;
          refresh();
        } catch (error) {
          toast(String(error.message || error), 'danger');
        }
      },
    }, 'Clear history');
  },
  build(data, refresh) {
    const entries = data.entries || [];

    if (!data.hooked) {
      return emptyState({
        title: 'The prompt preview is not hooked up',
        hint: 'The engine installs the preview hook when it starts. Restart the web app '
          + '(python -m webapp) and the viewer will fill in as requests are sent.',
      });
    }

    if (!entries.length) {
      return emptyState({
        title: 'No prompts sent yet',
        hint: 'Every request the generator sends is captured here, newest first. '
          + 'Start a run and this fills in within a couple of seconds.',
      });
    }

    // Keep the selection if it still exists; otherwise open the newest, which is the one a
    // reader opening the tab wants.
    if (!entries.some((entry) => entry.seq === liveSelected)) {
      liveSelected = entries[0].seq;
    }
    const selected = entries.find((entry) => entry.seq === liveSelected) || entries[0];

    const list = el('div', { class: 'prompt-list live-list' });
    for (const entry of entries) {
      const row = el('button', {
        class: `prompt-row${entry.seq === selected.seq ? ' selected' : ''}`,
        type: 'button',
        title: `${entry.message_type} · thread ${entry.thread_id ?? '—'}`,
      },
        el('span', { class: 'mono' }, `${entry.time} · ${entry.message_type}`),
        el('span', { class: 'muted' },
          `${num(entry.turns.length)} msg · ${num(entry.chars)} chars`));
      row.addEventListener('click', () => {
        liveSelected = entry.seq;
        // Repaint through the tab's own path, so the click and the poll produce the same DOM.
        // `liveScrollTop` is already up to date from the scroll listener, so the repaint
        // restores it.
        refresh();
      });
      list.append(row);
    }

    const scroller = el('div', { class: 'live-list-scroll' }, list);
    // Restore the offset after the browser has laid the list out. Without the frame the
    // assignment happens before the content has a height and is clamped to 0.
    requestAnimationFrame(() => { scroller.scrollTop = liveScrollTop; });
    scroller.addEventListener('scroll', () => {
      liveScrollTop = scroller.scrollTop;
    });

    const meta = el('div', { class: 'detail-head' },
      el('span', { class: 'mono' },
        `Thread ${selected.thread_id ?? '—'} · API slot ${num(selected.api_slot)}`),
      el('span', { class: 'spacer' }),
      el('span', { class: 'muted' },
        `${selected.message_type} · attempt ${selected.attempt ?? '—'} · ${selected.time}`),
    );

    const stats = el('div', { class: 'count-row' },
      el('span', { class: 'muted' },
        `${num(selected.turns.length)} message(s) · ${num(selected.chars)} chars · `
        + `~${num(selected.approx_tokens)} tokens`),
      selected.truncated
        ? el('span', { class: 'pill warn' }, 'truncated for display')
        : null,
    );

    const body = el('div', { class: 'live-turns' });
    for (const turn of selected.turns) {
      body.append(el('div', { class: `turn turn-${String(turn.role).toLowerCase()}` },
        el('div', { class: 'turn-role' }, String(turn.role).toUpperCase()),
        el('div', { class: 'turn-text' }, turn.content || '(empty)'),
      ));
    }

    const preview = el('div', { class: 'prompt-preview live-preview' }, meta, stats, body);

    return el('div', { class: 'prompt-grid live-grid' }, scroller, preview);
  },
});

// --- Queue ---------------------------------------------------------------

const queueTab = makeTab({
  id: 'queue',
  title: 'Queue',
  path: '/api/queue',
  actions(data, refresh) {
    const clear = el('button', {
      class: 'btn-danger',
      type: 'button',
      title: 'Empty pending, completed and failed. Works whether or not a run is going.',
      onclick: async () => {
        const counts = data.counts || {};
        const total = (counts.pending || 0) + (counts.completed || 0) + (counts.failed || 0);
        if (!total) {
          toast('The queue is already empty.', 'info');
          return;
        }
        if (!window.confirm(
          `Clear the whole queue?\n\n`
          + `${num(counts.pending || 0)} pending, ${num(counts.completed || 0)} completed, `
          + `${num(counts.failed || 0)} failed.\n\n`
          + `Nothing is deleted from the dataset, and a running job is not stopped -- only the `
          + `queued work is discarded.`)) return;
        try {
          const body = await postJSON('/api/queue/clear', {});
          toast(`Cleared ${num(body.data.total)} task(s).`, 'ok');
          refresh();
        } catch (error) {
          toast(String(error.message || error), 'danger');
        }
      },
    }, 'Clear queue');

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

    return el('span', { class: 'button-row' }, clear, purge, retry, exportButton);
  },
  build(data) {
    const tables = data.tables || {};
    const counts = data.counts || {};
    const messages = {
      pending: 'No pending tasks. Press Start to queue a run.',
      completed: 'No completed tasks yet.',
      failed: 'No failed tasks. Nothing to retry.',
    };

    // **Failed first, then pending, then completed.** The old order put pending first, and a
    // pending list can be thousands of rows, so it pushed the two short tables off the screen
    // and squashed every column of them into a sliver. Failed is the table a reader acts on,
    // pending is the one they scroll, completed is the one they rarely open -- so that is the
    // order, and each table scrolls inside its own panel rather than growing the page.
    const order = ['failed', 'pending', 'completed'];
    const panels = order.map((state) => {
      const rows = (tables[state] || []).map((row) => [
        el('span', { class: 'mono', title: row.task_id }, row.task_id),
        el('span', { class: 'muted' }, row.file || 'N/A'),
        num(row.retries),
      ]);
      return panel(
        `${state[0].toUpperCase()}${state.slice(1)} (${num(counts[state] || 0)})`,
        table(['Task ID', 'Source file', 'Retries'], rows,
          { empty: messages[state], widths: ['46%', '38%', '70px'] }));
    });

    return el('div', { class: 'tab-grid queue-grid' }, ...panels);
  },
});

// --- the tab registry ----------------------------------------------------

export const DATA_TABS = [
  { id: 'apis', label: 'APIs', tab: apisTab },
  { id: 'quality', label: 'Quality', tab: qualityTab },
  { id: 'regex', label: 'Regex Watch', tab: regexTab },
  { id: 'metrics', label: 'Metrics', tab: metricsTab },
  { id: 'prompts', label: 'Prompts', tab: promptsTab },
  { id: 'live', label: 'Live Prompts', tab: livePromptsTab },
  { id: 'queue', label: 'Queue', tab: queueTab },
];
