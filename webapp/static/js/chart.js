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
 * A small hand-rolled SVG line chart.
 *
 * **Why not Chart.js**: the whole point of this
 * stack is no build step and no vendored bundle, and the graphs here are a polyline, a
 * grid and a legend. A 200 KB charting library for that would be the largest file in the repo.
 *
 * Colours come from the CSS custom properties, so there is no hex literal here and the chart
 * cannot drift from the palette.
 *
 * **The viewBox is measured, not fixed (v15.1.0).** The first version drew into a hard-coded
 * `640x160` viewBox with `preserveAspectRatio="none"`, so the browser stretched the whole drawing
 * to the panel's width. A panel narrower than 640px therefore squashed every glyph horizontally and
 * the y-axis numbers ran into each other -- the "squeezed together" axis labels. The chart now
 * measures its host and uses a viewBox whose width **is** the host's width in CSS pixels, with
 * `preserveAspectRatio="xMinYMin meet"`, so one user unit is one pixel and text is never scaled.
 * A `ResizeObserver` redraws when the panel changes width.
 */

const SVG_NS = 'http://www.w3.org/2000/svg';

// Used only before the host has a layout (a hidden tab, a headless capture). The observer
// replaces it with the real width on the first resize.
const FALLBACK_WIDTH = 640;
const MIN_WIDTH = 220;

// How wide a character is relative to the font size, for the axis gutter. A monospace-ish
// estimate is fine here: it only decides how much room the labels get, and the gutter is
// rounded up.
const CHAR_RATIO = 0.62;

function svg(tag, attrs = {}) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value === null || value === undefined) continue;
    node.setAttribute(key, String(value));
  }
  return node;
}

/** Read a CSS custom property, so the chart uses the palette rather than its own colours. */
function token(name, fallback) {
  const value = getComputedStyle(document.documentElement)
    .getPropertyValue(name).trim();
  return value || fallback;
}

/** The host's width in CSS pixels, or the fallback when it has no layout yet. */
function hostWidth(host) {
  const width = Math.round(host.clientWidth || 0);
  return width >= MIN_WIDTH ? width : FALLBACK_WIDTH;
}

function textWidth(text, fontSize) {
  return String(text).length * fontSize * CHAR_RATIO;
}

/** A whole-number tick label. A fractional top would print `0.5` and `1.5` on a count axis. */
function formatTick(value) {
  const rounded = Math.round(value * 10) / 10;
  return Number.isInteger(rounded) ? String(rounded) : rounded.toFixed(1);
}

/**
 * A round top for the axis, in whole steps.
 *
 * `steps` gridlines means `steps + 1` labels including zero, so the step is the top divided by
 * the number of steps and is rounded UP to a whole number. That is what keeps every label an
 * integer on a token or request count, which is the only kind of value these charts carry.
 */
function axisTop(maxValue, steps) {
  const safe = Math.max(1, Number(maxValue) || 1);
  const step = Math.max(1, Math.ceil(safe / steps));
  return step * steps;
}

/**
 * Draw a multi-series line chart into `host`, replacing its contents.
 *
 * @param {HTMLElement} host
 * @param {Object} options
 * @param {string[]} options.labels - x-axis labels, oldest first
 * @param {Array<{name: string, colour: string, values: number[]}>} options.series
 * @param {number} [options.height] - height in CSS pixels
 * @param {string} [options.empty] - text to show when there is no data
 * @param {string} [options.label] - the accessible name for the image
 */
export function lineChart(host, options) {
  const {
    labels = [],
    series = [],
    height = 132,
    empty = 'No data yet.',
    label = 'Metric graph',
  } = options || {};

  host.replaceChildren();

  const hasData = series.some((entry) => (entry.values || []).some((v) => v > 0));
  if (!hasData) {
    host.append(Object.assign(document.createElement('div'), {
      className: 'empty',
      textContent: empty,
    }));
    watchResize(host, options);
    return;
  }

  const width = hostWidth(host);
  const fontSize = 10;

  // The gutter is sized to the widest label, so a four-digit token count cannot overlap the
  // plot. This is the fix for the squeezed axis: the labels get the room they need.
  const steps = height < 110 ? 3 : 4;
  const maxValue = Math.max(
    1,
    ...series.flatMap((entry) => (entry.values || []).map((v) => Number(v) || 0)),
  );
  const top = axisTop(maxValue, steps);
  const tickLabels = [];
  for (let index = 0; index <= steps; index += 1) {
    tickLabels.push(formatTick(top * (1 - index / steps)));
  }
  const gutter = Math.ceil(Math.max(...tickLabels.map((t) => textWidth(t, fontSize)))) + 10;

  const padding = { top: 10, right: 10, bottom: 18, left: gutter };
  const plotWidth = Math.max(1, width - padding.left - padding.right);
  const plotHeight = Math.max(1, height - padding.top - padding.bottom);

  const root = svg('svg', {
    viewBox: `0 0 ${width} ${height}`,
    // Never `none`: a non-uniform scale is what squashed the axis text. With the viewBox
    // matching the host's own width there is no scaling at all, and `meet` keeps it that way
    // if the panel is resized before the observer fires.
    preserveAspectRatio: 'xMinYMin meet',
    role: 'img',
    'aria-label': label,
    class: 'chart',
  });

  const grid = token('--border', 'currentColor');
  const muted = token('--muted', 'currentColor');

  // --- horizontal gridlines and y labels ---------------------------------
  for (let index = 0; index <= steps; index += 1) {
    const y = padding.top + (plotHeight * index) / steps;
    root.append(svg('line', {
      x1: padding.left, y1: y, x2: width - padding.right, y2: y,
      stroke: grid, 'stroke-width': 1, 'shape-rendering': 'crispEdges',
    }));
    const text = svg('text', {
      x: padding.left - 6, y: y + fontSize / 3,
      'text-anchor': 'end', 'font-size': fontSize, fill: muted,
    });
    text.textContent = tickLabels[index];
    root.append(text);
  }

  // --- one polyline per series ------------------------------------------
  const count = Math.max(1, labels.length - 1);
  for (const entry of series) {
    const values = entry.values || [];
    if (!values.length) continue;
    const points = values.map((raw, index) => {
      const x = padding.left + (plotWidth * index) / count;
      const value = Math.max(0, Math.min(top, Number(raw) || 0));
      const y = padding.top + plotHeight * (1 - value / top);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
    root.append(svg('polyline', {
      points,
      fill: 'none',
      stroke: entry.colour || token('--accent', 'currentColor'),
      'stroke-width': 1.6,
      'stroke-linejoin': 'round',
      'stroke-linecap': 'round',
    }));
  }

  // --- x labels, first and last only ------------------------------------
  if (labels.length) {
    const first = svg('text', {
      x: padding.left, y: height - 5, 'font-size': fontSize, fill: muted,
    });
    first.textContent = labels[0];
    root.append(first);
    if (labels.length > 1) {
      const last = svg('text', {
        x: width - padding.right, y: height - 5,
        'text-anchor': 'end', 'font-size': fontSize, fill: muted,
      });
      last.textContent = labels[labels.length - 1];
      root.append(last);
    }
  }

  host.append(root);
  watchResize(host, options);
}

// --- resize ---------------------------------------------------------------

// One observer per host. The options are kept so a resize can redraw with the same
// data rather than needing the caller to notice.
const watchers = new WeakMap();

function watchResize(host, options) {
  if (typeof ResizeObserver === 'undefined') return;
  let entry = watchers.get(host);
  if (!entry) {
    entry = { width: hostWidth(host), options };
    entry.observer = new ResizeObserver(() => {
      // A host that has been replaced is finished with; stop observing it so the
      // observer cannot keep a detached screen alive.
      if (!host.isConnected) {
        entry.observer.disconnect();
        watchers.delete(host);
        return;
      }
      const next = hostWidth(host);
      // The threshold matters: a redraw changes the SVG's size, which fires the
      // observer again. Under 8px is the same chart, so this cannot loop.
      if (Math.abs(next - entry.width) < 8) return;
      entry.width = next;
      lineChart(host, entry.options);
    });
    watchers.set(host, entry);
    entry.observer.observe(host);
  }
  entry.options = options;
}

/**
 * The legend for a chart. Rendered as HTML rather than inside the SVG so it wraps and
 * stays readable at a narrow width.
 */
export function legend(series) {
  const host = document.createElement('div');
  host.className = 'chart-legend';
  for (const entry of series) {
    const item = document.createElement('span');
    item.className = 'chart-legend-item';
    const swatch = document.createElement('span');
    swatch.className = 'chart-swatch';
    swatch.style.background = entry.colour;
    item.append(swatch, document.createTextNode(entry.name));
    host.append(item);
  }
  return host;
}

/**
 * Turn a cumulative counter's history into a per-minute rate history.
 *
 * `delta / seconds * 60`, and `None` for a sample where the counter went backwards (a
 * restarted server zeroed it) so the graph shows a gap rather than a spike. This is the same
 * rule `vllm_metrics._delta()` and the Tk panel apply.
 *
 * @param {Array<Object>} history - samples carrying `time` and `key`
 * @param {string} key - the cumulative counter to differentiate
 * @returns {Array<{time: number, rate: number|null}>}
 */
export function rateHistory(history, key) {
  const out = [];
  let previous = null;
  let previousTime = null;
  for (const sample of history || []) {
    const value = sample[key];
    let rate = null;
    if (value !== null && value !== undefined && previous !== null
        && previousTime !== null) {
      const elapsed = sample.time - previousTime;
      const delta = value - previous;
      if (elapsed > 0 && delta >= 0) rate = (delta / elapsed) * 60;
    }
    out.push({ time: sample.time, rate });
    previous = value === undefined ? null : value;
    previousTime = sample.time;
  }
  return out;
}
