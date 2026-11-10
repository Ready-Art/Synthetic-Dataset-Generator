# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Control surface for the generator: the engine both front ends drive.

Runs a generation job with no display, and exposes it over a local socket so a human at a
terminal -- or an LLM driving one -- can start it, stop it, save it, clear it and read its
statistics. ``cli.py`` is one client; the web app (``webapp/engine.py``) is the other, and it
serves the same socket. This module is the server and the backend.

**Everything a run needs is a plain module.** ``run_config`` reads the config, ``generation``
runs the worker, ``quality`` runs the judge, ``app_state`` holds the shared counters. Nothing
here imports a GUI toolkit, so the whole control surface is testable with no display:

    cli.py  ->  control.py  ->  run_config.py  ->  generation.py / quality.py / app_state.py
    webapp/engine.py  ->  control.py            # the second client of the same engine

**Why a socket instead of a one-shot command.** ``start`` has to keep running after the
command that started it returns, and a second terminal must be able to ask what happened. So
one process owns the run and a local socket is how you talk to it. The wire format is one JSON
object per line each way -- trivial to emit from a shell, trivial to parse from a script, and
trivial for an LLM to read.

**What is deliberately NOT here.** No widget, no ``toast``, no ``messagebox``, no window
handle of any kind. Every command returns a plain dict; the caller decides how to present it.
That is what lets ``tests/test_cli_control.py`` exercise every verb with no display, no network
and no database server.
"""

import json
import os
import socket
import socketserver
import stat
import threading
import time
import traceback
from queue import Empty, Queue

import app_state
import detection
import generation
import quality
import run_config
import logging_config
from logging_config import log_message

# v13.5.1: share the issue-timestamp registry with detection.py. `detection.issue_timestamps`
# and `detection.issue_timestamps_lock` are module globals initialised to None and wired up by
# the entry point that owns the run, so a run that hit a refusal phrase died inside
# `detection.is_refusal()` with "'NoneType' object does not support the context manager
# protocol". The controller owns the run, so it wires
# them up the same way.
detection.issue_timestamps = app_state.issue_timestamps
detection.issue_timestamps_lock = app_state.issue_timestamps_lock

PROTOCOL_VERSION = 1

# The control socket lives inside output/ because that directory is already gitignored and
# already the run's own scratch space, so a stale socket file is cleaned up with the rest.
DEFAULT_SOCKET_NAME = "generator-control.sock"

# Exit codes. Shared with cli.py so a script can branch on the number alone; documented in
# docs/cli.md because they are part of the contract, not an implementation detail.
EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_NOT_RUNNING = 3
EXIT_ALREADY_RUNNING = 4
EXIT_REFUSED = 5
EXIT_TIMEOUT = 6

# Error codes carried in a JSON error object, mapped to the exit code above. A CLI maps
# `error.code` through this table so a script never has to match on prose.
ERROR_EXIT_CODES = {
    "unknown_command": EXIT_USAGE,
    "bad_request": EXIT_USAGE,
    "protocol_mismatch": EXIT_USAGE,
    "not_running": EXIT_NOT_RUNNING,
    "no_daemon": EXIT_NOT_RUNNING,
    "already_running": EXIT_ALREADY_RUNNING,
    "refused": EXIT_REFUSED,
    "config": EXIT_FAILURE,
    "failed": EXIT_FAILURE,
    "timeout": EXIT_TIMEOUT,
}


class CommandError(Exception):
    """A command that failed for a reason the caller can act on.

    ``code`` is the machine-readable string (see ``ERROR_EXIT_CODES``); ``message`` is safe to
    show a human.
    """

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def workers_alive():
    """True while any worker thread of the current run is alive.

    ``generation`` has no equivalent helper: the run's liveness is the thread list, and that is
    the only thing this needs to look at.
    """
    return bool(app_state.threads) and any(
        thread.is_alive() for thread in app_state.threads if thread)


def reported_thread_count(config=None):
    """The worker count to report for the current (or next) run.

    ``app_state.num_threads`` alone is not a reliable answer: it is the legacy
    ``api.threads`` scalar, which is a fallback, not the run total (the workers are split
    per API slot). Report what is actually spawned while a run is live, and otherwise the
    total the next run would start, so a status call before and after ``start`` agree.
    """
    spawned = len([thread for thread in app_state.threads if thread])
    if spawned:
        return spawned
    cfg = app_state.global_config if config is None else config
    try:
        settings = run_config.build_run_settings(cfg)
        return run_config.total_threads_for_run(settings)
    except Exception:
        # A config that cannot be resolved is not this payload's problem -- fall back to
        # the scalar rather than failing the whole status call.
        return app_state.num_threads


def default_socket_path(config=None):
    """Where the control socket lives, honouring ``control.socket_path``."""
    cfg = app_state.global_config if config is None else config
    configured = str(cfg.get('control.socket_path', '') or '').strip()
    if configured:
        return configured
    return os.path.join(run_config.OUTPUT_DIR, DEFAULT_SOCKET_NAME)


def _json_default(value):
    """Make a payload JSON-safe for the odd non-primitive (sets, tuples, paths)."""
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, os.PathLike):
        return os.fspath(value)
    return str(value)


def dumps(payload):
    """Serialize a payload as one line of JSON."""
    return json.dumps(payload, default=_json_default, sort_keys=False)


# =======================================================================================
# Stats
# =======================================================================================

# Every detection counter, with its label and the EXACT app_state attribute names.
#
# The names are spelled out rather than derived from a stem because they are NOT
# consistent in app_state.py: `refusal_count_total` is singular, `ooc_count_total`
# abbreviates, and `logic_issue_count_total` / `regex_watch_count_total` sit next to
# `recent_logic_total` / `recent_regex_total`. Deriving a name from a stem silently
# produced an empty `per_api: {}` for four of the ten counters, which reads as "nothing
# detected" instead of "wrong attribute". One table, so a new counter cannot be added to
# app_state and forgotten here.
#
# (key, label, total attribute, per-api attribute, recent-total attribute,
#  recent-per-api attribute)
DETECTION_COUNTERS = (
    ('refusals', 'Refusals', 'refusal_count_total', 'refusal_counts_per_api',
     'recent_refusals_total', 'recent_refusals_per_api'),
    ('user_speaking', 'User speaking', 'user_speaking_count_total',
     'user_speaking_counts_per_api', 'recent_user_speaking_total',
     'recent_user_speaking_per_api'),
    ('slop', 'Slop', 'slop_count_total', 'slop_counts_per_api',
     'recent_slop_total', 'recent_slop_per_api'),
    ('anti_slop', 'Anti-Slop', 'anti_slop_count_total', 'anti_slop_counts_per_api',
     'recent_anti_slop_total', 'recent_anti_slop_per_api'),
    ('errors', 'Errors', 'error_count_total', 'error_counts_per_api',
     'recent_errors_total', 'recent_errors_per_api'),
    ('out_of_character', 'Out of character', 'ooc_count_total',
     'ooc_counts_per_api', 'recent_ooc_total', 'recent_ooc_per_api'),
    ('logic_issues', 'Logic issues', 'logic_issue_count_total',
     'logic_issue_counts_per_api', 'recent_logic_total', 'recent_logic_per_api'),
    ('regex_watch', 'Regex Watch', 'regex_watch_count_total',
     'regex_watch_counts_per_api', 'recent_regex_total', 'recent_regex_per_api'),
    ('non_latin', 'Non-Latin', 'non_latin_count_total', 'non_latin_counts_per_api',
     'recent_non_latin_total', 'recent_non_latin_per_api'),
    ('scene_continuity', 'Scene continuity', 'scene_continuity_count_total',
     'scene_continuity_counts_per_api', 'recent_scene_continuity_total',
     'recent_scene_continuity_per_api'),
)


def _recent_entry(raw):
    """One ``recent`` item as ``{time, phrase, context, api}``, whatever shape it arrived in.

    The engine now writes dicts (``app_state.record_recent``), but a process that started before
    that change still holds the old tuples, and a caller may hand-build a payload. Normalising
    here means the browser has exactly one shape to read, and the Time column is never blank.

    The old shapes were ``(phrase, context, api_slot)`` for a totals ring and
    ``(phrase, context)`` for a per-API ring, so a 2-tuple has no slot of its own -- it belongs
    to the slot it was filed under, which the caller supplies.
    """
    if isinstance(raw, dict):
        return {
            'time': str(raw.get('time') or ''),
            'phrase': str(raw.get('phrase') or ''),
            'context': str(raw.get('context') or ''),
            'api': int(raw.get('api', -1)) if raw.get('api') is not None else -1,
        }
    if isinstance(raw, (list, tuple)):
        parts = list(raw) + [None, None, None]
        phrase, context, api = parts[0], parts[1], parts[2]
        return {
            'time': '',
            'phrase': str(phrase if phrase is not None else ''),
            'context': str(context if context is not None else ''),
            'api': int(api) if isinstance(api, int) else -1,
        }
    return {'time': '', 'phrase': str(raw), 'context': '', 'api': -1}


def _counter_payload(key, with_recent=True):
    """One detection counter as ``{total, per_api, recent, recent_per_api}``."""
    entry = _counter_entry(key)
    total = getattr(app_state, entry[2], 0)
    per_api = getattr(app_state, entry[3], {}) or {}
    payload = {
        'total': total,
        'per_api': {str(slot): value for slot, value in sorted(per_api.items())},
    }
    if with_recent:
        recent = list(getattr(app_state, entry[4], []) or [])
        recent_per_api = getattr(app_state, entry[5], {}) or {}
        payload['recent'] = [_recent_entry(item)
                             for item in recent[-app_state.MAX_RECENT:]]
        payload['recent_per_api'] = {
            str(slot): [_recent_entry(item)
                        for item in list(value)[-app_state.MAX_RECENT:]]
            for slot, value in sorted(recent_per_api.items())}
    return payload


def _counter_entry(key):
    """The DETECTION_COUNTERS row for `key`."""
    for entry in DETECTION_COUNTERS:
        if entry[0] == key:
            return entry
    raise KeyError(key)


def _detection_config_summary(config=None):
    """The detection SETTINGS, i.e. "what is being filtered" as configured.

    Distinct from the runtime counters: this answers "which checks are armed, and how many patterns
    does each hold". A user asking "what's being filtered" usually means both, so ``stats``
    reports both and names them differently.
    """
    cfg = app_state.global_config if config is None else config

    def phrase_count(key):
        value = cfg.get(key, []) or []
        return len(value) if isinstance(value, list) else 0

    regex_patterns, _errors = detection.validate_regex_patterns(
        cfg.get('detection.regex_watch.patterns', []) or [])
    gender = cfg.get('gender', 'female')
    return {
        'slop_enabled': bool(cfg.get('detection.slop.enabled', False)),
        'slop_phrases': phrase_count('detection.slop.phrases'),
        'anti_slop_enabled': bool(cfg.get('detection.anti_slop.enabled', False)),
        'anti_slop_phrases': phrase_count('detection.anti_slop.phrases'),
        'refusal_enabled': phrase_count('detection.refusal.phrases') > 0,
        'refusal_phrases': phrase_count('detection.refusal.phrases'),
        'user_speaking_enabled': phrase_count(
            f'detection.user_speaking.{gender}.phrases') > 0,
        'user_speaking_phrases': phrase_count(
            f'detection.user_speaking.{gender}.phrases'),
        'out_of_character_enabled': bool(
            cfg.get('detection.out_of_character.enabled', True)),
        'out_of_character_phrases': phrase_count(
            'detection.out_of_character.phrases'),
        'regex_watch_enabled': bool(cfg.get('detection.regex_watch.enabled', False)),
        'regex_watch_patterns': len(regex_patterns),
        'regex_watch_max_rounds': cfg.get('detection.regex_watch.max_rounds', 3),
        'non_latin_enabled': bool(cfg.get('detection.non_latin.enabled', False)),
        'scene_continuity_enabled': bool(
            cfg.get('detection.scene_continuity.enabled', True)),
        'retry_missing_quotations': bool(
            cfg.get('generation.retry_missing_quotations', False)),
        'repeat_opener_guard': bool(cfg.get('generation.repeat_opener_guard', False)),
        'repeat_reply_guard': bool(cfg.get('generation.repeat_reply_guard', False)),
        'first_reply_first_word_cap': cfg.get(
            'generation.first_reply_first_word_cap', 0.0),
        'first_reply_opener_cap': cfg.get('generation.first_reply_opener_cap', 0.0),
        'quality_enabled': bool(cfg.get('quality.enabled', True)),
        'quality_use_llm_scoring': bool(cfg.get('quality.use_llm_scoring', False)),
        'quality_output_filter': bool(cfg.get('quality.output_filter', False)),
        'quality_min_score_threshold': cfg.get('quality.min_score_threshold', 50),
        'quality_per_turn_logic_check': bool(
            cfg.get('quality.per_turn_logic_check', False)),
        'discard_refused_conversations': bool(
            cfg.get('generation.discard_refused_conversations', False)),
    }


def _dropped_summary():
    """What was dropped and never written, grouped by reason.

    ``quality_dropped`` is the authoritative record: a rejected conversation is recorded there
    with its reason and never reaches the dataset. The reasons come from ``quality.DROP_REASON_*``;
    ``manual`` is the operator's own Drop Selected action, kept distinguishable so a reader can
    tell a judge rejection from a human decision.
    """
    with app_state.quality_lock:
        dropped = dict(app_state.quality_dropped)
        write_failures = sorted(app_state.quality_write_failures)
    with app_state.quality_review_lock:
        review_queue = len(app_state.quality_review_ids)

    by_reason = {}
    task_ids = {}
    for task_id, reason in sorted(dropped.items()):
        key = str(reason or 'unknown')
        by_reason[key] = by_reason.get(key, 0) + 1
        task_ids.setdefault(key, []).append(task_id)

    return {
        'total': len(dropped),
        'by_reason': by_reason,
        'task_ids': task_ids,
        'review_queue': review_queue,
        'write_failures': len(write_failures),
        'write_failure_ids': write_failures,
    }


def _task_progress():
    """Queue/progress numbers, from the queue object the engine is already updating."""
    queue = app_state.task_queue
    total = getattr(queue, 'total_tasks_for_progress', 0) if queue else 0
    processed = getattr(queue, 'processed_tasks', 0) if queue else 0
    percent = (processed / total * 100.0) if total else 0.0
    return {
        'processed': processed,
        'total': total,
        'percent': round(percent, 2),
        'queued': queue.qsize() if queue is not None else 0,
    }


def _estimated_cost(cfg):
    """The run's estimated spend, from the tokens actually counted and the configured price.

    ``(input + output) * cost_per_1k_tokens / 1000`` -- byte for byte the expression in
    ``generation.check_budget_limit()``, so the Overview card and the budget stop agree. A
    price of 0 (the shipped default) gives 0.0, which is the honest answer: the operator has
    not told the generator what a token costs, so there is nothing to estimate from.

    Read from the config on every call rather than cached, so a price edited in the Config tab
    takes effect on the next tick instead of at the next restart.
    """
    try:
        price_per_1k = float(cfg.get('api.pricing.cost_per_1k_tokens', 0) or 0)
    except (TypeError, ValueError):
        price_per_1k = 0.0
    if price_per_1k <= 0:
        return 0.0
    tokens = (int(getattr(app_state, 'total_input_tokens', 0) or 0)
              + int(getattr(app_state, 'total_output_tokens', 0) or 0))
    return round(tokens * (price_per_1k / 1000.0), 6)


def _quality_summary():
    """``quality.get_quality_stats()`` plus the flags a reader needs to interpret it."""
    try:
        stats = quality.get_quality_stats()
    except Exception as exc:  # a half-built state must not break the whole stats call
        log_message(f"Could not read quality stats: {exc}", "WARNING")
        stats = {'count': 0, 'avg_composite': 0, 'min_composite': 0, 'max_composite': 0,
                  'dimension_averages': {}, 'flag_counts': {}, 'threshold_failures': 0}
    return {
        'enabled': bool(app_state.quality_enabled),
        'scored': stats.get('count', 0),
        'avg_composite': stats.get('avg_composite', 0),
        'min_composite': stats.get('min_composite', 0),
        'max_composite': stats.get('max_composite', 0),
        'dimension_averages': stats.get('dimension_averages', {}),
        'flag_counts': stats.get('flag_counts', {}),
        'threshold_failures': stats.get('threshold_failures', 0),
        'threshold': app_state.global_config.get('quality.min_score_threshold', 50),
    }


def build_stats_payload(config=None, include_recent=True, generator=None):
    """The full statistics document -- the one an LLM reads.

    Deliberately flat and explicitly named: every value a reader needs to answer "how is it going"
    and "what is being filtered" is present, and nothing requires a second call.
    """
    cfg = app_state.global_config if config is None else config
    generator = generator or {}

    detected = {}
    for key, label, _total, _per_api, _recent, _recent_per_api in DETECTION_COUNTERS:
        payload = _counter_payload(key, with_recent=include_recent)
        payload['label'] = label
        detected[key] = payload

    attempts_per_api = getattr(app_state, 'total_attempts_per_api', {}) or {}
    output_files = run_config.jsonl_row_counts()

    payload = {
        'ok': True,
        'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'generator': {
            'version': generator.get('version', ''),
            'pid': os.getpid(),
            'uptime_s': generator.get('uptime_s', 0.0),
            'phase': generator.get('phase', 'idle'),
            'config_path': cfg.path,
            'protocol': PROTOCOL_VERSION,
        },
        'run': {
            'active': bool(app_state.processing_active),
            'generating': bool(app_state.processing_active),
            'paused': bool(app_state.pause_processing),
            'stopping': bool(app_state.stop_processing),
            'threads': reported_thread_count(cfg),
            'alive_threads': sum(1 for thread in app_state.threads
                                 if thread and thread.is_alive()),
            'duplication_mode': bool(app_state.duplication_mode),
            'tasks': {
                'pending': len(app_state.pending_task_ids),
                'completed': len(app_state.completed_task_ids),
                'failed': len(app_state.failed_task_ids),
            },
            'progress': _task_progress(),
            'run_started_at': generator.get('run_started_at'),
            'run_elapsed_s': generator.get('run_elapsed_s', 0.0),
        },
        'output': {
            'sink': 'postgres' if cfg.get('database.enabled', False) else 'jsonl',
            'jsonl_files': output_files,
            'jsonl_rows': sum(entry['rows'] for entry in output_files),
            'database': {
                'enabled': bool(cfg.get('database.enabled', False)),
                'connected': bool(app_state.db_pool),
                'rows': run_config.db_row_count() if app_state.db_pool else None,
            },
        },
        'tokens': {
            'input': app_state.total_input_tokens,
            'output': app_state.total_output_tokens,
            # v15.1.0: COMPUTED here, not read from `app_state.estimated_cost`.
            #
            # `app_state.estimated_cost` is declared and reset but **never written by a run**,
            # so reading it would leave the Overview's "Est. cost" card at 0.0000 forever. The
            # formula here is the same one `generation.check_budget_limit()` uses -- total
            # tokens times the price per 1k -- so the number the browser shows is the number
            # the budget check acts on, rather than a second opinion.
            'estimated_cost': _estimated_cost(cfg),
        },
        'attempts': {
            'total': app_state.total_attempts_global,
            'per_api': {str(key): value
                        for key, value in sorted(attempts_per_api.items())},
        },
        'detected': detected,
        'filtered': _dropped_summary(),
        'quality': _quality_summary(),
        'detection_config': _detection_config_summary(cfg),
    }

    if include_recent:
        with app_state.regex_watch_log_lock:
            payload['regex_watch_log'] = list(app_state.regex_watch_log)[-50:]
        with app_state.cast_events_log_lock:
            payload['cast_events_log'] = list(app_state.cast_events_log)[-50:]

    return payload


# =======================================================================================
# HeadlessController
# =======================================================================================

class HeadlessController:
    """Owns one generation run and executes the control verbs against it.

    One instance per process. Commands are serialised by ``_command_lock`` because they mutate
    shared engine state (the task queue, the worker list, the DB pool); the run itself is
    multi-threaded inside that, which is the engine's business.
    """

    def __init__(self, config_path=None, serve_socket=True, socket_path=None,
                 tcp_host=None, tcp_port=None):
        self.config_path = config_path
        self._started_at = time.time()
        self._run_started_at = None
        self._run_started_monotonic = None
        self._command_lock = threading.RLock()
        self._phase = 'idle'
        self._completion_thread = None
        self._completion_done = threading.Event()
        self._completion_done.set()
        self._settings = None
        # v13.0.5: the saved progress counter a resume continues from (see _load_state).
        self._loaded_processed_tasks = None
        self._server = None
        self._shutdown_requested = False
        self._socket_path = socket_path or default_socket_path()
        self._serve_socket = serve_socket
        self._tcp_host = tcp_host
        self._tcp_port = tcp_port

        # Load the config this controller will run from, so a bad path fails at launch
        # rather than on the first `start`.
        if config_path:
            app_state.global_config.path = config_path
        app_state.global_config.load()

        # The engine reads this on every write path to pick the output file name. Seed it from
        # the config so a `start` that never goes through cmd_start() still has an answer.
        app_state.duplication_mode = bool(
            app_state.global_config.get('api.master_duplication_mode', False))

        log_message(f"Headless controller ready (config: {app_state.global_config.path}).",
                     "INFO")

    # --- lifecycle -------------------------------------------------------------------

    @property
    def version(self):
        return _version_string()

    def uptime(self):
        return round(time.time() - self._started_at, 3)

    def run_elapsed(self):
        if self._run_started_monotonic is None:
            return 0.0
        return round(time.monotonic() - self._run_started_monotonic, 3)

    def phase(self):
        """'idle' | 'running' | 'paused' | 'stopping' | 'draining'.

        A worker still finishing an in-flight request counts as ``stopping``, not
        ``idle``: it is still writing rows, so a reader told "idle" would reasonably
        assume the dataset is final and the process safe to kill. That mismatch is what
        a slow HTTP call produces (the queue drains, `processing_active` clears, and two
        workers stay alive for another 300 s).
        """
        # Live workers win over the remembered phase. `_phase` is set to
        # 'stopping' by a stop and only cleared by the code path that performed
        # the stop, so a stop whose workers then exited in the background would
        # otherwise report 'stopping' forever -- a reader would wait for a finish
        # that already happened. `workers_alive()` is the ground truth.
        if app_state.processing_active:
            return 'paused' if app_state.pause_processing else 'running'
        if workers_alive():
            return 'stopping'
        if self._phase == 'draining':
            return 'draining'
        return 'idle'

    def _generator_info(self):
        return {
            'version': self.version,
            'uptime_s': self.uptime(),
            'phase': self.phase(),
            'run_started_at': self._run_started_at,
            'run_elapsed_s': self.run_elapsed(),
        }

    def start_server(self):
        """Start the control socket (and TCP listener when configured)."""
        if not self._serve_socket:
            return None
        self._server = ControlServer(self, socket_path=self._socket_path,
                                     tcp_host=self._tcp_host, tcp_port=self._tcp_port)
        self._server.start()
        return self._server

    def serve_forever(self):
        """Block, serving commands, until ``shutdown`` is called."""
        server = self.start_server()
        if server is None:
            raise CommandError('config', 'Control server is disabled.')
        server.serve_forever()

    # --- dispatch --------------------------------------------------------------------

    COMMANDS = (
        'version', 'status', 'start', 'stop', 'pause', 'resume',
        'stats', 'export', 'save', 'clear', 'wait', 'shutdown',
    )

    def handle(self, request):
        """Execute one request dict and return the response dict. Never raises."""
        request_id = request.get('id') if isinstance(request, dict) else None
        try:
            if not isinstance(request, dict):
                raise CommandError('bad_request', 'A request must be a JSON object.')
            protocol = request.get('protocol', PROTOCOL_VERSION)
            if protocol != PROTOCOL_VERSION:
                raise CommandError(
                    'protocol_mismatch',
                    f"Client speaks protocol {protocol}, this server speaks "
                    f"{PROTOCOL_VERSION}.")
            command = request.get('cmd')
            if not command:
                raise CommandError('bad_request', 'A request needs a "cmd".')
            handler = getattr(self, f"cmd_{command}", None)
            if command not in self.COMMANDS or handler is None:
                raise CommandError(
                    'unknown_command',
                    f"Unknown command {command!r}. Known: {', '.join(self.COMMANDS)}.")
            args = request.get('args') or {}
            if not isinstance(args, dict):
                raise CommandError('bad_request', '"args" must be an object.')
            with self._command_lock:
                data = handler(**args)
            return {'ok': True, 'id': request_id, 'data': data}
        except CommandError as exc:
            return {'ok': False, 'id': request_id,
                    'error': {'code': exc.code, 'message': exc.message}}
        except TypeError as exc:
            # A wrong keyword from the caller, not a server fault.
            return {'ok': False, 'id': request_id,
                    'error': {'code': 'bad_request', 'message': str(exc)}}
        except run_config.RunConfigError as exc:
            return {'ok': False, 'id': request_id,
                    'error': {'code': 'config', 'message': str(exc)}}
        except Exception as exc:  # noqa: BLE001 - a command must never kill the server
            log_message(f"Control command {request.get('cmd')!r} failed: {exc}", "ERROR")
            log_message(traceback.format_exc(), "ERROR")
            return {'ok': False, 'id': request_id,
                    'error': {'code': 'failed', 'message': str(exc)}}

    # --- verbs -----------------------------------------------------------------------

    def cmd_version(self):
        return {'version': self.version, 'protocol': PROTOCOL_VERSION}

    def cmd_status(self):
        # `running` means "work is still happening", which includes a worker
        # finishing an in-flight request after a stop. `generating` is the narrower
        # "the queue is still feeding workers" -- both are reported so a caller can
        # tell the two apart instead of guessing from one boolean.
        alive = sum(1 for thread in app_state.threads if thread and thread.is_alive())
        return {
            'running': bool(app_state.processing_active) or alive > 0,
            'generating': bool(app_state.processing_active),
            'phase': self.phase(),
            'pid': os.getpid(),
            'uptime_s': self.uptime(),
            'version': self.version,
            'config_path': app_state.global_config.path,
            'threads': reported_thread_count(),
            'alive_threads': alive,
            'tasks': {
                'pending': len(app_state.pending_task_ids),
                'completed': len(app_state.completed_task_ids),
                'failed': len(app_state.failed_task_ids),
            },
            'progress': _task_progress(),
            'run_started_at': self._run_started_at,
            'run_elapsed_s': self.run_elapsed(),
        }

    def cmd_start(self, dry_run=False, turns=None, chunks=None, threads=None,
                  force_fresh=False, resume=None):
        """Start a generation run. Returns the resolved plan (and starts it)."""
        if app_state.processing_active or workers_alive():
            raise CommandError('already_running',
                               'A generation run is already in progress.')

        app_state.global_config.load()
        cfg = app_state.global_config

        # Explicit overrides from the command line win over the config file, so a one-off
        # `--turns 2` does not require editing config.yml. They are applied to the loaded
        # in-memory config only -- nothing is written back to disk.
        if turns is not None:
            cfg.set('generation.num_turns', int(turns))
        if chunks is not None:
            cfg.set('generation.num_random_chunks', int(chunks))
        if threads is not None:
            cfg.set('api.threads', int(threads))

        settings = run_config.build_run_settings(cfg)
        run_config.validate_run_shape(settings)

        plan = {
            'threads': run_config.total_threads_for_run(settings),
            'turns': settings.num_turns,
            'num_random_chunks': settings.num_random_chunks,
            'duplication_mode': settings.master_duplication_enabled,
            'opening_mode': settings.opening_mode,
            'use_questions_file': settings.use_questions_file,
            'active_apis': len(settings.active_api_configs),
            'output_sink': ('postgres' if cfg.get('database.enabled', False) else 'jsonl'),
        }

        if dry_run:
            plan['dry_run'] = True
            plan['started'] = False
            plan['tasks_queued'] = 0
            return plan

        # --- real start ---------------------------------------------------------------
        # v13.1.1: prompt traces are opt-in (generation.debug_prompt_log), and a headless
        # run must honour the same key the GUI does.
        logging_config.DEBUG_PROMPT_LOGGING_ENABLED = bool(
            cfg.get('generation.debug_prompt_log', False))
        log_message(f"Prompt trace logging: {'enabled' if logging_config.DEBUG_PROMPT_LOGGING_ENABLED else 'disabled'}", "INFO")

        run_config.init_valkey(cfg)
        run_config.register_api_profiles(settings.api_configs)
        run_config.apply_rate_limits(settings.api_configs)
        run_config.init_db_pool(cfg)

        resumed = self._load_or_reset_state(cfg, force_fresh=force_fresh)
        plan['resumed'] = resumed

        app_state.stop_processing = False
        app_state.pause_processing = False
        app_state.processing_active = True
        app_state.duplication_mode = bool(settings.master_duplication_enabled)

        # A fresh queue per run.
        app_state.task_queue = Queue()
        app_state.task_queue.api_widgets = {}
        try:
            from dataset_tasks import configure_queue_datasets
            configure_queue_datasets(app_state.task_queue, cfg.get('datasets', {}))
        except ValueError as exc:
            app_state.processing_active = False
            raise CommandError('config', f"Dataset configuration: {exc}")

        try:
            queued, total_tasks = run_config.populate_task_queue(settings, config=cfg)
        except run_config.RunConfigError as exc:
            app_state.processing_active = False
            raise CommandError('config', str(exc))

        if total_tasks == 0 and not app_state.completed_task_ids:
            app_state.processing_active = False
            raise CommandError('config', 'No tasks to process.')
        if queued == 0 and app_state.completed_task_ids:
            app_state.processing_active = False
            raise CommandError('config',
                                'All tasks already completed. Nothing new to queue.')

        app_state.task_queue.all_tasks_queued = True
        app_state.task_queue.total_tasks_for_progress = total_tasks * settings.num_turns
        app_state.task_queue.processed_tasks_lock = threading.Lock()
        # v13.0.5: a resume continues from the saved counter, exactly as the GUI does.
        app_state.task_queue.processed_tasks = 0
        loaded_processed = getattr(self, '_loaded_processed_tasks', None)
        if resumed and loaded_processed is not None:
            app_state.task_queue.processed_tasks = loaded_processed
        elif resumed:
            app_state.task_queue.processed_tasks = len(app_state.completed_task_ids) * settings.num_turns
        # v13.0.5: progress is credited per completed task, so the credit helper needs the
        # run's turn count, and a resume must not re-credit tasks the state file already counts.
        app_state.current_num_turns_for_progress = settings.num_turns
        with app_state.progress_credited_lock:
            if resumed:
                app_state.progress_credited_task_ids |= set(app_state.completed_task_ids)
            else:
                app_state.progress_credited_task_ids = set()
        app_state.task_queue.start_times_list = []
        if settings.master_duplication_enabled:
            app_state.task_queue.api_processed_tasks = {i: 0 for i in range(4)}
            app_state.task_queue.api_start_times_list = {i: [] for i in range(4)}

        # The quality queue must be running before any worker hands it a job.
        if app_state.quality_enabled and cfg.get('quality.use_llm_scoring', False):
            if quality.judge_async_enabled():
                generation._install_quality_write_handler()
            quality.initialize_quality_queue()

        total_threads = run_config.total_threads_for_run(settings)
        self._settings = settings
        self._run_started_at = time.strftime('%Y-%m-%d %H:%M:%S')
        self._run_started_monotonic = time.monotonic()
        self._phase = 'running'
        self._completion_done.clear()

        app_state.threads = []
        for index in range(total_threads):
            thread = threading.Thread(
                target=generation.worker,
                args=self._worker_args(index, settings),
                kwargs=self._worker_kwargs(settings),
                name=f"Worker-{index}",
            )
            app_state.threads.append(thread)
            thread.start()
        log_message(f"Started {total_threads} worker threads (headless).", "INFO")

        self._completion_thread = threading.Thread(
            target=self._wait_for_completion, name="CompletionWaiter", daemon=True)
        self._completion_thread.start()

        plan['started'] = True
        plan['tasks_queued'] = queued
        plan['tasks_total'] = total_tasks
        plan['turns_total'] = app_state.task_queue.total_tasks_for_progress
        return plan

    def _worker_args(self, thread_id, settings):
        """The positional worker arguments, in ``generation.worker()``'s own order.

        Kept in one place next to ``_worker_kwargs`` so the two can be read together and
        compared against the signature. ``tests/test_cli_control.py`` binds them through
        ``inspect.Signature`` so a signature change fails the test rather than the run.
        """
        return (
            thread_id,
            app_state.task_queue,
            app_state.output_data_lock,
            settings.use_questions_file,
            settings.use_variable_system,
            settings.api_configs,
            settings.active_api_configs,
            settings.question_prompt,
            settings.answer_prompt,
            settings.user_continuation_prompt,
            settings.num_turns,
            settings.system_prompts,
            settings.refusal_phrases,
            settings.user_speaking_phrases,
            settings.slop_phrases,
            settings.anti_slop_phrases,
            settings.anti_slop_fixes,
            settings.jailbreaks,
            settings.speaking_fixes,
            settings.slop_fixes_fallback,
            settings.max_attempts,
            settings.history_size,
            settings.remove_reasoning,
            settings.remove_em_dash,
            settings.remove_asterisks,
            settings.remove_asterisk_space_asterisk,
            settings.remove_all_asterisks,
            settings.ensure_space_after_line_break,
            settings.remove_markdown,
            settings.output_format,
            settings.slop_fixer_config,
            settings.anti_slop_fixer_config,
            settings.anti_slop_fixer_config,
            settings.slop_fixes_for_rotation,
            settings.top_level_system_prompt,
            settings.master_duplication_enabled,
            settings.character_engine_enabled,
            settings.class_selection_enabled,
            settings.setting_selection_enabled,
            settings.characters,
            settings.emotional_states_enabled,
            settings.emotional_states,
            settings.num_characters,
            settings.no_user_impersonation,
            settings.api_request_timeout,
            settings.slop_to_anti_slop_fallback,
            settings.lore,
            settings.random_names_enabled,
            settings.random_names,
            settings.include_metadata,
            settings.random_settings_enabled,
            settings.random_settings,
        )

    def _worker_kwargs(self, settings):
        """The keyword-only worker tail, keyed exactly as ``generation.worker()`` names it."""
        return {
            'opening_mode_local': settings.opening_mode,
            'greeting_prompt_local': settings.greeting_prompt,
            'first_line_prompt_local': settings.first_line_prompt,
            'placeholder_share_local': settings.placeholder_share,
            'greeting_context_local': settings.greeting_context,
            'first_line_styles_local': settings.first_line_styles,
            'style_enabled_local': settings.style_enabled,
            'style_list_local': settings.style_list,
            'persona_breakthrough_turn_local': settings.persona_breakthrough_turn,
            'persona_placement_local': settings.persona_placement,
            'emotion_placement_local': settings.emotion_placement,
            'current_ooc_phrases': settings.ooc_phrases,
            'current_ooc_fixes': settings.ooc_fixes,
            'current_ooc_enabled': settings.ooc_enabled,
            'per_turn_logic_check_local': settings.per_turn_logic_check,
            'per_turn_logic_max_rewrites_local': settings.per_turn_logic_max_rewrites,
            'user_emotional_states_enabled_local': settings.user_emotional_states_enabled,
            'user_emotional_states_list_local': settings.user_emotional_states,
            'per_char_random_emotions_local': settings.per_char_random_emotions,
            'current_regex_patterns': settings.regex_patterns,
            'current_regex_watch_enabled': settings.regex_watch_enabled,
            'current_regex_watch_max_rounds': settings.regex_watch_max_rounds,
            'include_style_metrics_local': settings.include_style_metrics,
            'retry_missing_quotations_local': settings.retry_missing_quotations,
            'current_non_latin_enabled': settings.non_latin_enabled,
            'current_non_latin_fixes': settings.non_latin_fixes,
            'current_foreign_word_enabled': settings.foreign_word_enabled,
            'current_foreign_word_fixes': settings.foreign_word_fixes,
            'questions_append_alias_local': settings.questions_append_alias,
            'questions_append_separator_local': settings.questions_append_separator,
            'scene_cast_enabled_local': settings.scene_cast_enabled,
            'scene_cast_config_local': settings.scene_cast_config,
            'scene_continuity_enabled_local': settings.scene_continuity_enabled,
            'scene_continuity_fixes_local': settings.scene_continuity_fixes,
            'scene_continuity_max_rounds': settings.scene_continuity_max_rounds,
            'repeat_opener_enabled_local': settings.repeat_opener_guard,
            'repeat_opener_fixes_local': settings.repeat_opener_fixes,
            'repeat_opener_max_retries': settings.repeat_opener_max_retries,
            'repeat_reply_enabled_local': settings.repeat_reply_guard,
            'repeat_reply_fixes_local': settings.repeat_reply_fixes,
            'repeat_reply_max_retries': settings.repeat_reply_max_retries,
            'first_reply_first_word_cap_local': settings.first_reply_first_word_cap,
            'first_reply_opener_cap_local': settings.first_reply_opener_cap,
            'first_reply_opener_max_retries': settings.first_reply_opener_max_retries,
        }

    def _load_or_reset_state(self, cfg, force_fresh=False):
        """Load the saved state, or start fresh. Returns True when resuming."""
        if force_fresh:
            run_config.clear_state_file()
            self._reset_stats()
            return False
        if os.path.exists(run_config.STATE_FILE_PATH):
            if self._load_state(cfg):
                log_message("Resuming previous generation.", "INFO")
                return True
            log_message("State incompatible or unreadable. Starting fresh.", "WARNING")
            run_config.clear_state_file()
            self._reset_stats()
            return False
        self._reset_stats()
        return False

    def _load_state(self, cfg):
        """Load ``generation_state.json`` when its critical settings still match.

        A mismatch is NOT a question here (there is nobody to ask): it is treated as "start
        fresh", which is the safe direction -- resuming with different critical settings would
        reprocess or skip tasks against the wrong shape.
        """
        try:
            with open(run_config.STATE_FILE_PATH, 'r', encoding='utf-8') as handle:
                state = json.load(handle)
        except (OSError, ValueError) as exc:
            log_message(f"Could not read generation state: {exc}", "WARNING")
            return False

        snapshot = state.get('config_snapshot', {})
        current = {
            'prompts.use_questions_file': cfg.get('prompts.use_questions_file'),
            'generation.num_turns': cfg.get('generation.num_turns', 1),
            'api.master_duplication_mode': cfg.get('api.master_duplication_mode', False),
            'prompts.opening_mode': bool(cfg.get('prompts.opening_mode', False)),
        }
        for key, value in current.items():
            if snapshot.get(key) != value:
                log_message(f"Saved state differs on {key} "
                             f"(saved {snapshot.get(key)!r}, now {value!r}).", "WARNING")
                return False

        app_state.completed_task_ids = set(state.get('completed_task_ids', []))
        # v13.0.5: the saved progress counter, restored when the queue is rebuilt in
        # cmd_start(). Before this the headless path always restarted the bar at 0 on a resume.
        # Deliberately a per-run attribute rather than an app_state global: it belongs to this
        # controller's run, and a second controller would have its own.
        if not cfg.get('api.master_duplication_mode', False):
            self._loaded_processed_tasks = state.get('processed_tasks_snapshot')
        # v13.0.5: which tasks' turns the snapshot already counts (older state files have no
        # such key; there completed_task_ids is the correct fallback).
        with app_state.progress_credited_lock:
            if 'progress_credited_task_ids' in state:
                app_state.progress_credited_task_ids = set(
                    state.get('progress_credited_task_ids') or [])
            else:
                app_state.progress_credited_task_ids = set(app_state.completed_task_ids)
        app_state.system_prompt_counter = state.get('system_prompt_counter', 0)
        app_state.character_counter = state.get('character_counter', 0)
        app_state.question_history = state.get('question_history', [])
        with app_state.quality_lock:
            app_state.quality_scores = state.get('quality_scores', {})
            app_state.quality_dropped = state.get('quality_dropped', {})
            app_state.dropped_task_ids = set(app_state.quality_dropped)
        # v14.2.1: restore the Review flags, or derive them from the restored scores
        # for a state file written before the key existed.
        with app_state.quality_review_lock:
            if 'quality_review_ids' in state:
                app_state.quality_review_ids = set(state.get('quality_review_ids') or [])
            else:
                app_state.quality_review_ids = generation._derive_review_ids(
                    app_state.quality_scores, app_state.quality_dropped)
        app_state.total_attempts_global = state.get('total_attempts_global', 0)
        app_state.refusal_count_total = state.get('refusal_count_total', 0)
        app_state.user_speaking_count_total = state.get('user_speaking_count_total', 0)
        app_state.slop_count_total = state.get('slop_count_total', 0)
        app_state.error_count_total = state.get('error_count_total', 0)
        app_state.ooc_count_total = state.get('ooc_count_total', 0)
        app_state.total_input_tokens = state.get('total_input_tokens', 0)
        app_state.total_output_tokens = state.get('total_output_tokens', 0)
        log_message(f"State loaded: {len(app_state.completed_task_ids)} task(s) already "
                     f"completed.", "INFO")
        return True

    def _reset_stats(self):
        """Zero every counter and history list a new run starts from."""
        app_state.completed_task_ids = set()
        # v13.0.5: nothing is credited in a fresh run, and no saved counter is restored.
        self._loaded_processed_tasks = None
        with app_state.progress_credited_lock:
            app_state.progress_credited_task_ids = set()
        app_state.system_prompt_counter = 0
        app_state.character_counter = 0
        app_state.question_history = []
        app_state.random_name_deck = []
        app_state.random_names_pool_key = None
        # v13.2.0: a new run deals the style pool from the top again.
        app_state.style_deck = []
        app_state.styles_pool_key = None
        app_state.reset_first_reply_openers()
        app_state.total_attempts_global = 0
        app_state.refusal_count_total = 0
        app_state.user_speaking_count_total = 0
        app_state.slop_count_total = 0
        app_state.anti_slop_count_total = 0
        app_state.error_count_total = 0
        app_state.ooc_count_total = 0
        app_state.logic_issue_count_total = 0
        app_state.regex_watch_count_total = 0
        app_state.non_latin_count_total = 0
        app_state.scene_continuity_count_total = 0
        app_state.total_input_tokens = 0
        app_state.total_output_tokens = 0
        app_state.estimated_cost = 0.0
        for entry in DETECTION_COUNTERS:
            # Zero the per-API counter dict (in place: it is shared by reference).
            per_api = getattr(app_state, entry[3], None)
            if isinstance(per_api, dict):
                for slot in per_api:
                    per_api[slot] = 0
            # Clear the recent ring buffers by REPLACING them: rebinding the attribute is what
            # the stats payload re-reads, so no stale entry survives.
            setattr(app_state, entry[4], [])
            recent_per_api = getattr(app_state, entry[5], None)
            if isinstance(recent_per_api, dict):
                for slot in recent_per_api:
                    recent_per_api[slot] = []
        for name in ('total_attempts_per_api',):
            per_api = getattr(app_state, name, None)
            if isinstance(per_api, dict):
                for slot in per_api:
                    per_api[slot] = 0
        with app_state.regex_watch_log_lock:
            app_state.regex_watch_log = []
        with app_state.cast_events_log_lock:
            app_state.cast_events_log = []
        with app_state.quality_lock:
            app_state.quality_scores = {}
            app_state.quality_dropped = {}
            app_state.dropped_task_ids = set()
            app_state.quality_write_failures = set()
        with app_state.quality_review_lock:
            app_state.quality_review_ids = set()
        app_state.pending_task_ids = set()
        app_state.failed_task_ids = set()
        app_state.purged_task_ids = set()
        with app_state.task_queue_ui_lock:
            app_state.task_metadata = {}
        with app_state.issue_timestamps_lock:
            for key in app_state.issue_timestamps:
                app_state.issue_timestamps[key] = []
        log_message("All counters, history and progress trackers reset.", "INFO")

    def _wait_for_completion(self):
        """Wait for the queue to drain, stop the workers, then drain the judge.

        The ORDER is the whole point. ``generation.worker()`` loops until
        ``app_state.processing_active`` goes False, so a waiter that joined the workers
        first would deadlock: the workers idle in ``q.get(timeout=0.05)`` forever and the
        join never returns. (The GUI avoids this because its progress tick flips
        ``processing_active`` once the queue empties; headless has no tick, so the waiter
        does it here.) So: wait for the QUEUE to drain, then clear the flag, then join.
        """
        queue = app_state.task_queue
        try:
            self._phase = 'draining'
            while not app_state.stop_processing:
                if queue is None:
                    break
                unfinished = getattr(queue, 'unfinished_tasks', 0)
                if unfinished <= 0:
                    break
                time.sleep(0.1)

            # Every queued task is done (or a stop was requested). Now clear the flag so
            # the idle workers leave their loop, then join them.
            app_state.processing_active = False
            self._sentinel_workers()
            self._join_workers(timeout=10.0)

            try:
                if quality.judge_async_enabled() and app_state.quality_enabled:
                    log_message("Workers finished; draining the quality queue.", "INFO")
                    quality.wait_for_quality_queue()
            except Exception as exc:  # noqa: BLE001
                log_message(f"Error draining the quality queue: {exc}", "ERROR")
            log_message("All tasks completed or processing stopped. All threads joined.",
                         "INFO")
        finally:
            app_state.processing_active = False
            self._phase = 'idle'
            self._completion_done.set()

    def cmd_stop(self, clear=False, timeout=30.0):
        """Stop generation. ``clear=True`` also wipes progress, like Stop & Clear Job.

        ``timeout`` bounds the WHOLE stop, not each thread. A per-thread timeout made
        the real budget `timeout x threads` (10 workers x 30 s = 300 s), so the caller's
        own 30 s socket timeout fired first and reported a timeout for a stop that was
        working fine.

        A worker blocked inside an HTTP request cannot be interrupted, so a stop that runs
        out of time is NOT an error: it is reported as ``still_stopping`` with the run left
        winding down in the background. The caller asked for the run to stop, and it is
        stopping; failing the command because one in-flight request is slow would invite the
        caller to retry a stop that is already happening.
        """
        if not app_state.processing_active and not workers_alive():
            if clear:
                result = self._clear_progress()
                result['stopped'] = False
                result['note'] = 'No run was active; progress was cleared anyway.'
                return result
            raise CommandError('not_running', 'No generation run is in progress.')

        self._phase = 'stopping'
        app_state.stop_processing = True
        app_state.pause_processing = False

        self._sentinel_workers()
        still_running = self._join_workers(timeout=timeout)
        self._shutdown_quality_queue()

        result = {
            'stopped': True,
            'still_stopping': bool(still_running),
            'cleared': False,
            'completed_tasks': len(app_state.completed_task_ids),
            'pending_tasks': len(app_state.pending_task_ids),
        }
        if still_running:
            result['note'] = (
                f"{len(still_running)} worker(s) are still finishing an in-flight "
                f"request and will exit as soon as it returns. The run is stopping; "
                f"poll `status` to watch it finish.")
            # Leave processing_active True so `status` reports the run as still
            # winding down instead of claiming it is idle while workers are alive.
            log_message(f"Stop requested; {len(still_running)} worker(s) still "
                        f"finishing. Run is winding down.", "WARNING")
            return result

        app_state.processing_active = False
        generation.flush_generation_state()
        if clear:
            result.update(self._clear_progress())
            result['cleared'] = True
        self._phase = 'idle'
        self._completion_done.set()
        log_message("Generation stopped (headless).", "INFO")
        return result

    def _sentinel_workers(self):
        if app_state.task_queue is None or not app_state.threads:
            return
        alive = [thread for thread in app_state.threads if thread.is_alive()]
        count = len(alive) or len(app_state.threads)
        for _ in range(count):
            try:
                app_state.task_queue.put(None, block=False)
            except Exception:  # noqa: BLE001 - best effort, the join below is the guard
                break

    def _join_workers(self, timeout=30.0):
        """Join every worker within ONE shared deadline. Returns the still-alive ones.

        The deadline is shared so the total wait is bounded by ``timeout``, not by
        ``timeout x threads``. Workers that outlive it are RETURNED rather than dropped:
        they are still writing, so forgetting them would let a later run start on top of
        them, and the caller reports the run as winding down instead.
        """
        deadline = time.monotonic() + max(0.0, float(timeout))
        still_running = []
        for thread in list(app_state.threads):
            if not thread.is_alive():
                continue
            remaining = deadline - time.monotonic()
            if remaining > 0:
                thread.join(timeout=remaining)
            if thread.is_alive():
                still_running.append(thread)
        if not still_running:
            app_state.threads = []
        else:
            # Keep only the live ones: the dead threads are finished with.
            app_state.threads = list(still_running)
            log_message(f"{len(still_running)} worker(s) did not stop within "
                         f"{timeout}s.", "WARNING")
        return still_running

    def _shutdown_quality_queue(self):
        try:
            quality.shutdown_quality_queue()
        except Exception as exc:  # noqa: BLE001
            log_message(f"Error shutting down the quality queue: {exc}", "WARNING")

    def _clear_progress(self):
        """Remove the run's progress: state file, queue and counters (not the dataset)."""
        run_config.clear_state_file()
        if app_state.task_queue is not None:
            try:
                while not app_state.task_queue.empty():
                    app_state.task_queue.get_nowait()
            except Empty:
                pass
        app_state.task_queue = Queue()
        self._reset_stats()
        return {'state_file_removed': True}

    def cmd_pause(self):
        if not app_state.processing_active:
            raise CommandError('not_running', 'No generation run is in progress.')
        app_state.pause_processing = True
        log_message("Processing paused (headless).", "INFO")
        return {'paused': True}

    def cmd_resume(self):
        if not app_state.processing_active:
            raise CommandError('not_running', 'No generation run is in progress.')
        app_state.pause_processing = False
        # Reload the config so a mid-run edit takes effect, exactly as the GUI's Resume does.
        app_state.global_config.load()
        run_config.apply_rate_limits(
            run_config.build_api_configs(app_state.global_config)[0])
        log_message("Processing resumed; config reloaded (headless).", "INFO")
        return {'paused': False}

    def cmd_stats(self, include_recent=True, filters_only=False):
        payload = build_stats_payload(
            include_recent=bool(include_recent), generator=self._generator_info())
        if filters_only:
            return {
                'ok': True,
                'generated_at': payload['generated_at'],
                'detected': payload['detected'],
                'filtered': payload['filtered'],
                'detection_config': payload['detection_config'],
            }
        return payload

    def cmd_export(self, output=None, source='auto', append=False):
        """Write the dataset to one JSONL file. Returns ``(ok, message, rows)`` as a dict."""
        cfg = app_state.global_config
        if source not in ('auto', 'db', 'jsonl'):
            raise CommandError('bad_request',
                                "source must be 'auto', 'db' or 'jsonl'.")
        db_enabled = bool(cfg.get('database.enabled', False))
        if app_state.db_pool is None and db_enabled:
            run_config.init_db_pool(cfg)

        if source == 'auto':
            source = 'db' if (db_enabled and app_state.db_pool) else 'jsonl'

        if output is None:
            stamp = time.strftime('%Y%m%d_%H%M%S')
            output = os.path.join(run_config.OUTPUT_DIR, f"dataset_export_{stamp}.jsonl")

        if source == 'db':
            ok, message, rows = run_config.export_db_to_jsonl(output)
            if not ok:
                raise CommandError('failed', message)
            return {'ok': True, 'source': 'db', 'path': output, 'rows': rows,
                    'message': message}

        ok, message, rows = run_config.consolidate_jsonl(output, append=append)
        if not ok:
            raise CommandError('failed', message)
        return {'ok': True, 'source': 'jsonl', 'path': output, 'rows': rows,
                'message': message}

    def cmd_save(self, output=None, source='auto', append=False):
        """Alias of ``export`` -- the request calls this "save DB/JSONL"."""
        return self.cmd_export(output=output, source=source, append=append)

    def cmd_clear(self, confirm=False, target='both', backup=True):
        """Delete stored conversations. Destructive, so it needs ``confirm``.

        ``target``: ``db`` (truncate the table), ``jsonl`` (delete output files), ``state``
        (forget run progress) or ``both`` (db + jsonl, the default). A backup zip is taken first
        unless ``backup`` is false, so a mistake stays recoverable.
        """
        if not confirm:
            raise CommandError(
                'refused',
                'clear is destructive and needs confirm=true (CLI: --yes).')
        if target not in ('db', 'jsonl', 'both', 'state'):
            raise CommandError(
                'bad_request',
                "target must be one of db, jsonl, both, state.")

        cfg = app_state.global_config
        result = {'target': target, 'backup_path': None,
                  'db_cleared': False, 'jsonl_removed': 0, 'state_removed': False}

        if backup and target in ('db', 'jsonl', 'both'):
            ok, message, path = run_config.backup_output_zip()
            if ok:
                result['backup_path'] = path
            else:
                log_message(f"Pre-clear backup skipped: {message}", "WARNING")

        if target in ('db', 'both'):
            if app_state.db_pool is None and cfg.get('database.enabled', False):
                run_config.init_db_pool(cfg)
            ok, message = run_config.clear_database()
            if not ok:
                raise CommandError('failed', message)
            result['db_cleared'] = True
            result['db_message'] = message

        if target in ('jsonl', 'both'):
            ok, message, removed = run_config.clear_jsonl()
            if not ok:
                raise CommandError('failed', message)
            result['jsonl_removed'] = removed
            result['jsonl_message'] = message

        if target == 'state':
            result['state_removed'] = run_config.clear_state_file()

        return result

    def cmd_wait(self, timeout=0.0):
        """Block until the current run finishes. ``timeout=0`` waits forever."""
        if not app_state.processing_active and self._completion_done.is_set():
            return {'waited': False, 'note': 'No run is active.', 'running': False}
        finished = self._completion_done.wait(timeout=timeout or None)
        if not finished:
            raise CommandError('timeout', f"The run did not finish within {timeout}s.")
        return {'waited': True, 'running': bool(app_state.processing_active),
                'completed_tasks': len(app_state.completed_task_ids),
                'failed_tasks': len(app_state.failed_task_ids),
                'progress': _task_progress()}

    def cmd_shutdown(self, force=False, timeout=30.0):
        """Stop generation (if any), close everything, stop the server.

        Returns the response FIRST and shuts down on a short delay, because the caller is
        waiting on this same socket -- tearing it down before the reply is written would turn a
        successful shutdown into a connection reset on the client.
        """
        if self._shutdown_requested:
            return {'shutting_down': True, 'note': 'Shutdown already in progress.'}
        self._shutdown_requested = True

        stopped = False
        if app_state.processing_active or workers_alive():
            try:
                self.cmd_stop(clear=False, timeout=timeout)
                stopped = True
            except CommandError:
                if not force:
                    raise
                log_message("Forced shutdown past a stop failure.", "WARNING")
                app_state.stop_processing = True

        def _finish():
            time.sleep(0.2)  # let the reply reach the client
            try:
                self._shutdown_quality_queue()
                generation.flush_generation_state()
            except Exception as exc:  # noqa: BLE001
                log_message(f"Shutdown save failed: {exc}", "WARNING")
            run_config.close_db_pool()
            if self._server is not None:
                self._server.stop()
            log_message("Headless controller shut down.", "INFO")

        threading.Thread(target=_finish, name="ControlShutdown", daemon=True).start()
        return {'shutting_down': True, 'stopped_run': stopped}


# =======================================================================================
# Socket server
# =======================================================================================

class _ThreadedUnixServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True


class _ThreadedTCPServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


class _ControlHandler(socketserver.StreamRequestHandler):
    """One JSON request per line in, one JSON response per line out."""

    def handle(self):
        controller = self.server.controller
        try:
            self.connection.settimeout(3600)
        except OSError:
            pass
        while True:
            try:
                line = self.rfile.readline()
            except (OSError, socket.timeout):
                return
            if not line:
                return
            text = line.decode('utf-8', 'replace').strip()
            if not text:
                continue
            try:
                request = json.loads(text)
            except ValueError as exc:
                response = {'ok': False, 'id': None,
                            'error': {'code': 'bad_request',
                                      'message': f"Invalid JSON: {exc}"}}
            else:
                response = controller.handle(request)
            try:
                self.wfile.write((dumps(response) + '\n').encode('utf-8'))
                self.wfile.flush()
            except OSError:
                return
            if response.get('ok') and request_is_shutdown(request):
                return


def request_is_shutdown(request):
    """True when the request was the shutdown verb (the handler then closes)."""
    return isinstance(request, dict) and request.get('cmd') == 'shutdown'


def _is_loopback_host(host):
    """True when `host` names this machine's loopback interface only.

    Accepts the literal loopback addresses plus ``localhost``. Anything else --
    including ``0.0.0.0``, a LAN address, or a hostname that resolves off-box --
    is treated as remote, because the safe default for a control channel that can
    clear a database is "not reachable from the network".
    """
    if not host:
        return False
    name = str(host).strip().lower()
    if name in ('localhost', '::1', '[::1]'):
        return True
    try:
        import ipaddress
        return ipaddress.ip_address(name.strip('[]')).is_loopback
    except ValueError:
        return False


class ControlServer:
    """Listens on a Unix socket (and optionally loopback TCP) and serves commands."""

    def __init__(self, controller, socket_path=None, tcp_host=None, tcp_port=None,
                 allow_remote=False):
        self.controller = controller
        self.socket_path = socket_path or default_socket_path()
        self.tcp_host = tcp_host or None
        self.tcp_port = tcp_port
        # `allow_remote` is a SAFETY limit, not a feature: with it false the TCP
        # listener refuses to bind anything but a loopback address, so enabling TCP
        # by accident cannot expose the generator to the network. The unix socket is
        # unaffected -- it is already unreachable off-host via filesystem permissions.
        self.allow_remote = bool(allow_remote)
        self._unix_server = None
        self._tcp_server = None
        self._threads = []
        self._stopped = threading.Event()
        # Set when stop() has finished tearing everything down, so serve_forever() can
        # block until the teardown is COMPLETE rather than merely begun.
        self._stopped_done = threading.Event()

    def _check_tcp_host(self, host):
        """Reject a non-loopback TCP bind unless the caller explicitly allowed it."""
        if self.allow_remote:
            return
        if _is_loopback_host(host):
            return
        raise CommandError(
            'refused',
            f"Refusing to listen on non-loopback address {host!r}: set "
            f"control.allow_remote (or --tcp-allow-remote) to permit it.")

    def _prepare_socket_path(self):
        parent = os.path.dirname(os.path.abspath(self.socket_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        # A leftover file from a crashed daemon would make bind() fail; only remove it
        # when nothing is listening on it, so a live daemon is never clobbered.
        if os.path.exists(self.socket_path):
            if is_server_running(self.socket_path):
                raise CommandError(
                    'already_running',
                    f"Another controller is already listening on {self.socket_path}.")
            try:
                os.unlink(self.socket_path)
            except OSError as exc:
                raise CommandError(
                    'failed', f"Could not remove stale socket {self.socket_path}: {exc}")

    def start(self):
        """Bind and start serving in background threads."""
        self._prepare_socket_path()
        self._unix_server = _ThreadedUnixServer(self.socket_path, _ControlHandler)
        self._unix_server.controller = self.controller
        try:
            os.chmod(self.socket_path, stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            pass
        thread = threading.Thread(target=self._unix_server.serve_forever,
                                  name="ControlUnix", daemon=True)
        thread.start()
        self._threads.append(thread)
        log_message(f"Control socket listening on {self.socket_path}.", "INFO")

        if self.tcp_host:
            try:
                self._check_tcp_host(self.tcp_host)
                self._tcp_server = _ThreadedTCPServer(
                    (self.tcp_host, int(self.tcp_port or 8765)), _ControlHandler)
                self._tcp_server.controller = self.controller
                tcp_thread = threading.Thread(target=self._tcp_server.serve_forever,
                                              name="ControlTCP", daemon=True)
                tcp_thread.start()
                self._threads.append(tcp_thread)
                log_message(f"Control TCP listening on {self.tcp_host}:{self.tcp_port}.",
                             "INFO")
            except OSError as exc:
                log_message(f"Could not listen on TCP {self.tcp_host}:"
                             f"{self.tcp_port}: {exc}", "WARNING")
                self._tcp_server = None
        return self

    def serve_forever(self):
        """Serve in the calling thread until ``stop`` is called.

        Reuses a server ``start()`` already bound: calling ``_prepare_socket_path()`` again
        would see this process's own socket file, decide a controller was already listening
        and refuse to start. Only a server that was never started is bound here.

        ``start()`` has already put a ``serve_forever`` loop on its own thread, so this
        method's whole job is to park the calling thread until ``stop()`` finishes. It must
        NOT start a second loop: two ``serve_forever`` loops over one ``BaseServer`` share
        the single ``__shutdown_request`` / ``__is_shut_down`` pair, so whichever loop exits
        first resets the flag, and the other is left accepting on a closed socket -- an
        EINVAL spin that never ends and keeps the process alive at ~90% CPU.
        """
        if self._unix_server is None:
            self.start()
            if self._unix_server is None:
                raise CommandError('failed', 'The control server could not be started.')
        self._stopped_done.wait()

    def stop(self):
        """Stop serving and remove the socket file."""
        if self._stopped.is_set():
            return
        self._stopped.set()
        for server in (self._unix_server, self._tcp_server):
            if server is not None:
                try:
                    server.shutdown()
                    server.server_close()
                except Exception:  # noqa: BLE001
                    pass
        self._unix_server = None
        self._tcp_server = None
        try:
            if os.path.exists(self.socket_path):
                os.unlink(self.socket_path)
        except OSError:
            pass
        # Released LAST: this is what lets serve_forever() return, so the process can
        # actually exit instead of leaving a busy controller behind.
        self._stopped_done.set()
        log_message("Control server stopped.", "INFO")


# =======================================================================================
# Client helpers
# =======================================================================================

def send_request(request, socket_path=None, timeout=30.0):
    """Send one request to a running controller and return its response dict.

    Raises :class:`CommandError` with code ``no_daemon`` when nothing is listening, so the CLI
    can turn "the generator is not running" into its own exit code instead of a traceback.
    """
    path = socket_path or default_socket_path()
    if not os.path.exists(path):
        raise CommandError('no_daemon',
                            f"No controller is listening on {path}.")
    payload = dict(request)
    payload.setdefault('protocol', PROTOCOL_VERSION)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(path)
    except (FileNotFoundError, ConnectionRefusedError):
        sock.close()
        raise CommandError('no_daemon', f"No controller is listening on {path}.")
    except OSError as exc:
        sock.close()
        raise CommandError('no_daemon', f"Could not reach {path}: {exc}")
    try:
        sock.sendall((dumps(payload) + '\n').encode('utf-8'))
        chunks = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
            if chunks and chunks[-1].endswith(b'\n'):
                break
    except socket.timeout:
        raise CommandError('timeout', f"No reply from {path} within {timeout}s.")
    except OSError as exc:
        raise CommandError('no_daemon', f"Lost connection to {path}: {exc}")
    finally:
        sock.close()

    raw = b''.join(chunks).decode('utf-8', 'replace').strip()
    if not raw:
        raise CommandError('failed', "The controller closed the connection without a reply.")
    try:
        return json.loads(raw.split('\n')[0])
    except ValueError as exc:
        raise CommandError('failed', f"Unreadable reply from the controller: {exc}")


def is_server_running(socket_path=None):
    """True when something answers a ``status`` request on the socket."""
    try:
        response = send_request({'cmd': 'status'}, socket_path=socket_path, timeout=2.0)
    except CommandError:
        return False
    return bool(response.get('ok'))


def _version_string():
    """The generator's version string, read from the one place that owns it."""
    return GENERATOR_VERSION


# The one place the version string lives; the web UI and the CLI both read it from here.
GENERATOR_VERSION = "16.0.0"
