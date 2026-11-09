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
 * The live stats connection.
 *
 * State lives server-side and the page re-renders from `/ws/stats`; nothing is kept in JS
 * memory across a reload, because a run outlives the browser tab.
 *
 * The WebSocket is the primary path. If it drops (server restart, a proxy that buffers the
 * upgrade, a laptop waking up) the module falls back to polling `/api/stats` and retries the
 * socket with a backoff, so the UI never silently stops updating.
 */

const WS_PATH = '/ws/stats';
const POLL_MS = 2000;
const RECONNECT_MIN_MS = 1000;
const RECONNECT_MAX_MS = 15000;

let socket = null;
let pollTimer = null;
let reconnectTimer = null;
let reconnectDelay = RECONNECT_MIN_MS;
let stopped = true;
let last = null;
let onStatsHandler = null;
let onDisconnectHandler = null;

/**
 * Start receiving stats.
 *
 * @param {(payload: Object) => void} onStats
 * @param {() => void} [onDisconnect] - called when the socket drops and the UI is on the
 *   polling fallback, so the status line can say so instead of silently freezing.
 * @returns {() => void} a stop function
 */
export function connectStats(onStats, onDisconnect) {
  onStatsHandler = onStats;
  onDisconnectHandler = onDisconnect || null;
  stopped = false;
  openSocket();
  return stop;
}

function openSocket() {
  if (stopped) return;
  const scheme = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = `${scheme}//${window.location.host}${WS_PATH}`;
  try {
    socket = new WebSocket(url);
  } catch (error) {
    scheduleReconnect();
    return;
  }

  socket.addEventListener('open', () => {
    reconnectDelay = RECONNECT_MIN_MS;
    stopPolling();
  });

  socket.addEventListener('message', (event) => {
    let payload;
    try {
      payload = JSON.parse(event.data);
    } catch (error) {
      return;
    }
    deliver(payload);
  });

  socket.addEventListener('close', () => {
    socket = null;
    if (stopped) return;
    // Keep the UI live while the socket is down, then retry the socket. The status line is
    // told, because a UI that keeps polling silently looks identical to one that is live.
    if (onDisconnectHandler) onDisconnectHandler();
    startPolling();
    scheduleReconnect();
  });

  socket.addEventListener('error', () => {
    // `close` follows an error, so the reconnect is handled there.
  });
}

function scheduleReconnect() {
  if (stopped || reconnectTimer !== null) return;
  const delay = reconnectDelay;
  reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    openSocket();
  }, delay);
}

function startPolling() {
  if (pollTimer !== null) return;
  const tick = async () => {
    try {
      const response = await fetch('/api/stats', { credentials: 'same-origin' });
      if (response.ok) deliver(await response.json());
    } catch (error) {
      // A dropped connection is normal while the server restarts.
    }
  };
  tick();
  pollTimer = setInterval(tick, POLL_MS);
}

function stopPolling() {
  if (pollTimer !== null) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

function deliver(payload) {
  last = payload;
  if (onStatsHandler) onStatsHandler(payload);
}

export function stop() {
  stopped = true;
  onDisconnectHandler = null;
  stopPolling();
  if (reconnectTimer !== null) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (socket) {
    socket.close();
    socket = null;
  }
}

/** The most recent payload, for a render triggered by a tab switch. */
export function lastStats() {
  return last;
}

/**
 * POST a control verb. The server answers with the same `{ok, data|error}` shape the
 * control socket uses, so callers branch on `ok` and read `error.code`.
 */
export async function command(cmd, args = {}) {
  const response = await fetch('/api/cmd', {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ cmd, args }),
  });
  if (response.status === 401) {
    window.location.href = '/login';
    return { ok: false, error: { code: 'unauthenticated', message: 'Session expired.' } };
  }
  try {
    return await response.json();
  } catch (error) {
    return { ok: false, error: { code: 'bad_response', message: String(error) } };
  }
}
