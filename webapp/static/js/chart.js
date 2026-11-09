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
 * stack is no build step and no vendored bundle, and the three graphs here are a polyline, a
 * grid and a legend. A 200 KB charting library for that would be the largest file in the repo.
 *
 * Colours come from the CSS custom properties, so there is no hex literal here and the chart
 * cannot drift from the palette.
 */

const SVG_NS = 'http://www.w3.org/2000/svg';

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

/**
 * Draw a multi-series line chart into `host`, replacing its contents.
 *
 * @param {HTMLElement} host
 * @param {Object} options
 * @param {string[]} options.labels - x-axis labels, oldest first
 * @param {Array<{name: string, colour: string, values: number[]}>} options.series
 * @param {number} [options.height] - viewBox height in user units
 * @param {string} [options.empty] - text to show when there is no data
 */
export function lineChart(host, options) {
  const {
    labels = [],
    series = [],
    height = 160,
    empty = 'No data yet.',
  } = options || {};

  host.replaceChildren();

  const hasData = series.some((entry) => (entry.values || []).some((v) => v > 0));
  if (!hasData) {
    host.append(Object.assign(document.createElement('div'), {
      className: 'empty',
      textContent: empty,
    }));
    return;
  }

  const width = 640;
  const padding = { top: 10, right: 8, bottom: 18, left: 34 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;

  const maxValue = Math.max(
    1,
    ...series.flatMap((entry) => (entry.values || []).map((v) => Number(v) || 0)),
  );
  // Round the top of the axis up so the gridlines land on whole numbers.
  const top = Math.max(1, Math.ceil(maxValue));

  const root = svg('svg', {
    viewBox: `0 0 ${width} ${height}`,
    preserveAspectRatio: 'none',
    role: 'img',
    'aria-label': 'Issue graph',
    class: 'chart',
  });

  const grid = token('--border', 'currentColor');
  const muted = token('--muted', 'currentColor');

  // --- horizontal gridlines and y labels ---------------------------------
  const steps = 4;
  for (let index = 0; index <= steps; index += 1) {
    const y = padding.top + (plotHeight * index) / steps;
    root.append(svg('line', {
      x1: padding.left, y1: y, x2: width - padding.right, y2: y,
      stroke: grid, 'stroke-width': 1, 'shape-rendering': 'crispEdges',
    }));
    const label = svg('text', {
      x: padding.left - 6, y: y + 3,
      'text-anchor': 'end', 'font-size': 9, fill: muted,
    });
    label.textContent = String(Math.round(top * (1 - index / steps)));
    root.append(label);
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
      'vector-effect': 'non-scaling-stroke',
    }));
  }

  // --- x labels, first and last only ------------------------------------
  if (labels.length) {
    const first = svg('text', {
      x: padding.left, y: height - 5, 'font-size': 9, fill: muted,
    });
    first.textContent = labels[0];
    root.append(first);
    if (labels.length > 1) {
      const last = svg('text', {
        x: width - padding.right, y: height - 5,
        'text-anchor': 'end', 'font-size': 9, fill: muted,
      });
      last.textContent = labels[labels.length - 1];
      root.append(last);
    }
  }

  host.append(root);
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
