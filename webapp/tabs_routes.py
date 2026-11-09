# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""The data tabs' routes: APIs, Quality, Regex Watch, Metrics, Prompts, Live Prompts, Queue.

Seven read routes and four queue actions. Everything here is a **view over data the engine
already keeps** -- `control.build_stats_payload()`, `quality.get_quality_stats()`,
`vllm_metrics.slot_usage_rows()`, and the config. Nothing recomputes a statistic, because a
second implementation of a number is a second number that can disagree.

**Why this is a separate module from `app.py`.** `app.py` owns the shell, the auth gate and the
engine seam. These six tabs are a large, self-contained block of read-only shaping, and putting
them in `app.py` would triple its size for no benefit. `install_tabs(app)` is called from
`create_app` exactly like `install_review`.

**Why the queue actions live here and not in `control.py`.** Purge, Retry and Export are the three
Queue-tab buttons. `control.py`'s verb list is a contract the CLI depends on, so adding
verbs there would change the socket protocol for features only the browser uses. The same reasoning
that put the Review verbs in `review_data.py` applies here.

What this module must never do
-----------------------------
* **Never import `generate_tk.py`.** It builds the whole Tk window at import time.
* **Never send an API key to the browser.** `api.apis.*.key` is reduced to a `has_key` boolean.
* **Never mutate the engine's dicts in place.** Every payload is a copy, so a route cannot corrupt
  a run by holding a reference.
"""

import csv
import os

import app_state
import quality
import run_config
import vllm_metrics
from fastapi import APIRouter

from . import live_prompts
from .app import error_response

# How many recent quality scores the Quality tab shows. The Tk tab shows the latest 50; the
# number is a display limit, not a statistic, so it lives here rather than in `quality.py`.
QUALITY_RECENT_LIMIT = 50

# The Queue tab's three tables, in the Tk order.
QUEUE_STATES = ('pending', 'completed', 'failed')


def install_tabs(app):
    """Attach the six data-tab routes to `app`. Called from `create_app`."""
    router = APIRouter(prefix='/api')

    # --- APIs ------------------------------------------------------------

    @router.get('/apis')
    def apis():
        """Per-slot configuration plus what each slot has actually done.

        The key is **never** included: `has_key` is all the browser needs to show "set" or
        "not set", and shipping the value would put an API key in a page.
        """
        cfg = app_state.global_config
        slots = []
        attempts_per_api = getattr(app_state, 'total_attempts_per_api', {}) or {}
        for index, entry in enumerate(cfg.get('api.apis', []) or []):
            if not isinstance(entry, dict):
                continue
            key = entry.get('key') or ''
            slots.append({
                'slot': index,
                'label': f'API {index + 1}',
                'url': entry.get('url', '') or '',
                'model': entry.get('model', '') or '',
                'enabled': bool(entry.get('enabled', False)),
                'threads': entry.get('threads', 0),
                'rate_limit_rpm': entry.get('rate_limit_rpm', 0),
                'api_profile': entry.get('api_profile', '') or '',
                'has_key': bool(key),
                'attempts': attempts_per_api.get(index, 0),
            })
        return {
            'ok': True,
            'slots': slots,
            'master_duplication_mode': bool(cfg.get('api.master_duplication_mode', False)),
            'threads': cfg.get('api.threads', 0),
            'pricing': cfg.get('api.pricing', {}) or {},
            'detected': _per_api_detected(),
        }

    # --- Quality ---------------------------------------------------------

    @router.get('/quality')
    def quality_tab():
        """The dimension summary plus the most recent individual scores.

        `get_quality_stats()` gives the aggregates; the recent list is the raw scores sorted by
        `scored_at`, which is what makes a low average answerable ("which one dragged it down").
        """
        try:
            summary = quality.get_quality_stats()
        except Exception as exc:  # noqa: BLE001 - a read must not break the tab
            summary = {'count': 0, 'avg_composite': 0, 'min_composite': 0,
                       'max_composite': 0, 'dimension_averages': {},
                       'flag_counts': {}, 'threshold_failures': 0}
            message = str(exc)
        else:
            message = None

        with app_state.quality_lock:
            scores = {task_id: dict(data or {})
                      for task_id, data in app_state.quality_scores.items()}

        recent = []
        for task_id, data in scores.items():
            recent.append({
                'task_id': task_id,
                'composite': data.get('composite'),
                'dimensions': data.get('dimensions') or {},
                'flags': list(data.get('flags') or []),
                'method': data.get('method'),
                'scored_at': data.get('scored_at', ''),
            })
        # Newest first, then the display limit. A stable tiebreak on task id keeps the order
        # from flickering between ticks when two rows share a timestamp.
        recent.sort(key=lambda row: (str(row['scored_at']), row['task_id']), reverse=True)
        recent = recent[:QUALITY_RECENT_LIMIT]

        return {
            'ok': True,
            'summary': summary,
            'recent': recent,
            'threshold': app_state.global_config.get('quality.min_score_threshold', 50),
            'enabled': bool(app_state.quality_enabled),
            'error': message,
        }

    # --- Regex Watch -----------------------------------------------------

    @router.get('/regex')
    def regex_watch():
        """The configured patterns and the hits recorded against them.

        Patterns come from the config; hits come from `app_state.regex_watch_log`. A pattern with
        no hits is still listed, because "this pattern has never fired" is information.
        """
        cfg = app_state.global_config
        raw_patterns = cfg.get('detection.regex_watch.patterns', []) or []
        patterns = []
        for entry in raw_patterns:
            if isinstance(entry, dict):
                patterns.append({
                    'pattern': entry.get('pattern', '') or '',
                    'replacement': entry.get('replacement', '') or '',
                    'enabled': bool(entry.get('enabled', True)),
                })
            elif isinstance(entry, str):
                patterns.append({'pattern': entry, 'replacement': '',
                                 'enabled': True})

        with app_state.regex_watch_log_lock:
            hits = [dict(entry) for entry in app_state.regex_watch_log]

        # Count hits per pattern so the table can show which ones actually fire.
        counts = {}
        for hit in hits:
            key = str(hit.get('pattern', ''))
            counts[key] = counts.get(key, 0) + 1
        for row in patterns:
            row['hits'] = counts.get(row['pattern'], 0)

        return {
            'ok': True,
            'patterns': patterns,
            'hits': hits[-50:],
            'enabled': bool(cfg.get('detection.regex_watch.enabled', False)),
            'max_rounds': cfg.get('detection.regex_watch.max_rounds', 3),
            'total_hits': len(hits),
        }

    # --- Metrics ---------------------------------------------------------

    @router.get('/metrics')
    def metrics():
        """Per-slot vLLM usage, the server-side endpoint rows, and the proxy's figures.

        Three sources, all copies so a tick cannot mutate what the page is holding:

        * `slot_usage_rows()` / `slot_history()` -- this slot's **own** measured throughput. The
          only figure that can tell two slots on one server apart, because vLLM's `/metrics` has
          no caller label.
        * `endpoint_rows()` -- what each vLLM **server** reports: prompt and generation tok/s, KV
          cache load, running/waiting requests, and the history ring behind the graphs.
        * the same rows' `antislop_*` keys -- the antislop-vllm fork's own counters. `None` for a
          plain vLLM, which is why the tab has a separate toggle rather than drawing a zero line.

        The two toggles in the browser read `vllm_available` and `antislop_available` to decide
        whether to offer themselves at all, and the whole document is one fetch so a toggle flip
        never needs a second round trip.
        """
        rows = []
        for slot, usage in vllm_metrics.slot_usage_rows():
            rows.append({
                'slot': slot,
                'label': f'API {slot + 1}',
                'usage': dict(usage or {}),
                'history': vllm_metrics.slot_history(slot),
            })

        endpoints = []
        for row in vllm_metrics.endpoint_rows():
            # A shallow copy per row: the antislop sub-dicts are rebuilt so nothing the page
            # holds aliases the poller's own state.
            entry = dict(row)
            entry['history'] = [dict(sample) for sample in row.get('history') or []]
            entry['slot_history'] = [dict(sample) for sample in row.get('slot_history') or []]
            endpoints.append(entry)

        return {
            'ok': True,
            'slots': rows,
            'endpoints': endpoints,
            # Whether any endpoint answered with `vllm:` metrics at all. A tab with no reachable
            # server says so instead of drawing empty axes.
            'vllm_available': any(not row.get('error') for row in endpoints),
            # The proxy's own columns are opt-in (`metrics.antislop_columns`), so the tab asks
            # rather than assuming. Off by default keeps a default run's tab unchanged.
            'antislop_columns': vllm_metrics.antislop_columns_enabled(),
            # True when at least one endpoint actually served `antislop:` metrics. The toggle is
            # still shown when this is false (a run can start before the fork is up) but the
            # panel says what is missing.
            'antislop_available': any(row.get('is_antislop') for row in endpoints),
        }

    # --- Prompts ---------------------------------------------------------

    @router.get('/prompts')
    def prompts():
        """Every prompt body, so the tab can preview and copy one.

        This is the config's own text, unmodified. It is **not** the config editor: this tab is
        read-only by design, and editing a prompt happens in the Config tab, which is the only
        editor.
        """
        cfg = app_state.global_config
        entries = []

        def add(path, label, text):
            if text is None:
                return
            text = str(text)
            entries.append({
                'path': path,
                'label': label,
                'text': text,
                'length': len(text),
                'lines': text.count('\n') + 1 if text else 0,
            })

        for key in ('question', 'answer', 'user_continuation_prompt', 'greeting',
                    'greeting_context', 'first_line', 'lore'):
            add(f'prompts.{key}', key.replace('_', ' ').title(),
                cfg.get(f'prompts.{key}', ''))

        for key in ('system', 'style', 'character', 'scene_cast',
                    'emotional_states', 'user_emotional_states',
                    'random_names', 'random_settings', 'questions_dataset'):
            block = cfg.get(f'prompts.{key}', {}) or {}
            if not isinstance(block, dict):
                continue
            for sub_key, value in block.items():
                if isinstance(value, str):
                    add(f'prompts.{key}.{sub_key}',
                        f'{key.replace("_", " ").title()} · {sub_key}'.title(), value)

        entries.sort(key=lambda row: row['path'])
        return {'ok': True, 'entries': entries}

    # --- Live prompts ----------------------------------------------------

    @router.get('/live-prompts')
    def live_prompts_route(limit: int = 0):
        """The prompts the engine has most recently sent, newest first.

        **The function is `live_prompts_route`, not `live_prompts`.** A route function named
        `live_prompts` shadows the imported `live_prompts` module for the whole body, so
        `live_prompts.snapshot(...)` called the function itself and raised
        `AttributeError: 'function' object has no attribute 'snapshot'`. FastAPI turned that into a
        500 with a plain-text body, and the browser's `response.json()` reported it as
        "Unexpected character at line 1 column 1" -- a JSON error for a bug that was never about
        JSON. `tests/test_web_v151.py` now scans every route module for a function that shadows a
        module it imports, so the class cannot come back.

        The engine calls ``update_live_prompt_preview`` before every request; the web app's hook
        (`webapp/live_prompts.py`) copies each one into a capped ring. This route hands out a
        copy, so a poll cannot mutate the ring a worker is appending to.

        ``limit=0`` means the whole ring. The tab asks for all of it and pages client-side, so a
        reader scrolling back through a retry sequence does not need a second request.
        """
        entries = live_prompts.snapshot(limit=limit or None)
        return {
            'ok': True,
            'entries': entries,
            'count': len(entries),
            'capacity': live_prompts.RING_LENGTH,
            # A viewer with no hook installed is a viewer that will never fill in, and that is
            # worth saying out loud rather than showing an empty list forever.
            'hooked': live_prompts.installed(),
        }

    @router.post('/live-prompts/clear')
    def clear_live_prompts():
        """Empty the ring. Never touches the dataset or the run."""
        return {'ok': True, 'data': {'cleared': live_prompts.clear()}}

    # --- Queue -----------------------------------------------------------

    @router.get('/queue')
    def queue():
        """The three queue tables, plus the counts the buttons need."""
        tables = {}
        with app_state.task_queue_ui_lock:
            for state in QUEUE_STATES:
                ids = sorted(getattr(app_state, f'{state}_task_ids', set()) or ())
                rows = []
                for task_id in ids:
                    meta = app_state.task_metadata.get(task_id, {}) or {}
                    rows.append({
                        'task_id': task_id,
                        'file': meta.get('file', 'N/A'),
                        'retries': meta.get('retries', 0),
                    })
                tables[state] = rows
            purged = len(getattr(app_state, 'purged_task_ids', set()) or ())

        return {
            'ok': True,
            'tables': tables,
            'counts': {state: len(tables[state]) for state in QUEUE_STATES},
            'purged': purged,
        }

    @router.post('/queue/purge')
    def purge():
        """Drop every pending task. Workers skip them; nothing is deleted from the dataset."""
        with app_state.task_queue_ui_lock:
            pending = list(app_state.pending_task_ids)
            app_state.purged_task_ids.update(pending)
            app_state.pending_task_ids.clear()
        return {'ok': True, 'data': {'purged': len(pending)}}

    @router.post('/queue/clear')
    def clear_queue():
        """Empty the whole queue -- pending, completed and failed -- with no run required.

        **Why this exists next to Purge.** Purge only touches `pending_task_ids`, so a queue left
        behind by a finished or crashed run keeps its completed and failed rows forever, and the
        only other way to clear it was Stop & Clear, which needs a live run (`cmd_stop` answers
        `not_running` when there is none). This route is the "start from a clean queue" button:
        it works whether or not a run is in progress, and it never deletes a dataset row.

        It also empties the in-memory `task_queue` when one exists, because a stale item left in
        the queue object would be picked up by the next run's first worker even though its id is
        no longer pending. `purged_task_ids` is filled first so a worker already holding a task
        still skips it.

        A live run is **not** stopped: the workers keep going on what they already hold, and only
        the queued work is discarded. That is the same contract as Purge, extended to the two
        finished sets.
        """
        with app_state.task_queue_ui_lock:
            pending = list(app_state.pending_task_ids)
            completed = list(app_state.completed_task_ids)
            failed = list(app_state.failed_task_ids)
            app_state.purged_task_ids.update(pending)
            app_state.pending_task_ids.clear()
            app_state.completed_task_ids.clear()
            app_state.failed_task_ids.clear()
            app_state.task_metadata.clear()

        drained = 0
        queue = getattr(app_state, 'task_queue', None)
        if queue is not None:
            try:
                while True:
                    queue.get_nowait()
                    drained += 1
            except Exception:  # noqa: BLE001 - Empty, or a queue that is not a Queue
                pass

        return {'ok': True, 'data': {
            'pending': len(pending),
            'completed': len(completed),
            'failed': len(failed),
            'drained': drained,
            'total': len(pending) + len(completed) + len(failed),
        }}

    @router.post('/queue/retry')
    def retry():
        """Re-queue every failed task with its retry counter reset."""
        with app_state.task_queue_ui_lock:
            to_retry = list(app_state.failed_task_ids)
            app_state.failed_task_ids.clear()
            requeued = 0
            for task_id in to_retry:
                app_state.pending_task_ids.add(task_id)
                meta = app_state.task_metadata.get(task_id, {}) or {}
                meta['retries'] = 0
                app_state.task_metadata[task_id] = meta
                if 'task_data' in meta and app_state.task_queue is not None:
                    app_state.task_queue.put(meta['task_data'])
                    requeued += 1
        return {'ok': True, 'data': {'retried': len(to_retry), 'requeued': requeued}}

    @router.post('/queue/export')
    def export_queue():
        """Write the three tables to a CSV beside the output files."""
        path = os.path.join(app_state.OUTPUT_DIR, 'queue_export.csv')
        rows = 0
        try:
            os.makedirs(app_state.OUTPUT_DIR, exist_ok=True)
            with open(path, 'w', newline='', encoding='utf-8') as handle:
                writer = csv.writer(handle)
                writer.writerow(['Status', 'Task ID', 'Source File', 'Retries'])
                with app_state.task_queue_ui_lock:
                    for state in QUEUE_STATES:
                        for task_id in sorted(
                                getattr(app_state, f'{state}_task_ids', set()) or ()):
                            meta = app_state.task_metadata.get(task_id, {}) or {}
                            writer.writerow([state, task_id, meta.get('file', 'N/A'),
                                             meta.get('retries', 0)])
                            rows += 1
        except OSError as exc:
            return error_response('failed', f'Could not write {path}: {exc}', status=500)
        return {'ok': True, 'data': {'path': path, 'rows': rows}}

    app.include_router(router)
    return app


def _per_api_detected():
    """`detected.<key>.per_api` for every counter, keyed by slot.

    The APIs tab shows each slot's own catches, which is the one thing the Overview tab's
    totals cannot answer.
    """
    from control import DETECTION_COUNTERS

    out = {}
    for key, _label, _total, per_api_attr, _recent, _recent_per_api in DETECTION_COUNTERS:
        per_api = getattr(app_state, per_api_attr, {}) or {}
        out[key] = {str(slot): value for slot, value in sorted(per_api.items())}
    return out
