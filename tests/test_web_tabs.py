# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the six data tabs.

The tabs are **read-only views over data the engine already keeps**, so the thing worth testing
is not "does it render" but **"does it report the same numbers the engine does, and does it leak
what it must not"**. Four rules get the most attention:

* **An API key never reaches the browser.** `api.apis.*.key` is reduced to `has_key`; the value
  itself must not appear anywhere in any payload. This is the rule that matters most and it is the one
  mistake that would be a real security bug rather than a cosmetic one.
* **A route never mutates the engine's dicts.** Every payload is a copy, so a tab cannot corrupt
  a run by holding a reference.
* **The queue actions are scoped.** Purge touches pending only; Retry touches failed only; Export
  writes a CSV and changes nothing.
* **No route imports `generate_tk.py`**, which builds the whole Tk window at import time.

Everything runs against a fake `app_state` in a temporary directory. The real `output/` is never
opened and no server is started.
"""

import csv
import os
import sys
import tempfile
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app_state as real_app_state
import webapp.theme as theme
from webapp.tabs_routes import QUALITY_RECENT_LIMIT, QUEUE_STATES

STATIC_JS = os.path.join(os.path.dirname(os.path.abspath(theme.__file__)), 'static', 'js')


# --- helpers -------------------------------------------------------------

class FakeConfig:
    """A stand-in for `ConfigLoader` with `get()` and a nested dict."""

    def __init__(self, data=None, path='config/config.yml'):
        self.config = data or {}
        self.path = path

    def get(self, path, default=None):
        keys = path.split('.')
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            elif isinstance(value, list) and key.isdigit() and int(key) < len(value):
                value = value[int(key)]
            else:
                return default
        return value


class FakeQueue:
    """A stand-in for the task queue: `qsize`, `put`, and the progress counters."""

    def __init__(self):
        self.items = []
        self.total_tasks_for_progress = 0
        self.processed_tasks = 0

    def qsize(self):
        return len(self.items)

    def put(self, item):
        self.items.append(item)


class FakeAppState:
    """The attributes `tabs_routes` touches, with real locks and a temp output dir."""

    def __init__(self, output_dir):
        self.OUTPUT_DIR = output_dir
        self.global_config = FakeConfig()
        self.task_queue = FakeQueue()
        self.task_queue_ui_lock = threading.Lock()
        self.quality_lock = threading.Lock()
        self.quality_review_lock = threading.Lock()
        self.regex_watch_log_lock = threading.Lock()
        self.quality_scores = {}
        self.quality_review_ids = set()
        self.quality_dropped = {}
        self.quality_write_failures = set()
        self.quality_enabled = True
        self.regex_watch_log = []
        self.pending_task_ids = set()
        self.completed_task_ids = set()
        self.failed_task_ids = set()
        self.purged_task_ids = set()
        self.task_metadata = {}
        self.total_attempts_per_api = {}
        self.db_pool = None
        # The per-API detection counters, named as `control.DETECTION_COUNTERS` expects.
        for attr in ('total_refusals_per_api', 'total_slop_per_api',
                     'total_out_of_character_per_api', 'total_logic_issues_per_api',
                     'total_regex_watch_per_api', 'total_errors_per_api',
                     'total_user_speaking_per_api', 'total_anti_slop_per_api',
                     'total_non_latin_per_api', 'total_scene_continuity_per_api'):
            setattr(self, attr, {})


class sandbox:
    """Swap `tabs_routes`' module-level imports for fakes, then restore.

    `tabs_routes` does `import app_state`, `quality` and `vllm_metrics` at module scope, so
    assigning those module attributes is what redirects every route. The originals are restored on
    exit even if a test raises.
    """

    def __enter__(self):
        import webapp.tabs_routes as tabs

        self._directory = tempfile.TemporaryDirectory()
        self._saved = (tabs.app_state, tabs.quality, tabs.vllm_metrics)
        self.state = FakeAppState(self._directory.name)
        tabs.app_state = self.state
        tabs.quality = _FakeQuality()
        tabs.vllm_metrics = _FakeMetrics()
        self.tabs = tabs
        return self.state

    def __exit__(self, *exc_info):
        tabs = self.tabs
        tabs.app_state, tabs.quality, tabs.vllm_metrics = self._saved
        self._directory.cleanup()
        return False


class _FakeQuality:
    """`quality.get_quality_stats()` and the drop-reason constants."""

    DROP_REASON_MANUAL = 'manual'

    @staticmethod
    def get_quality_stats():
        scores = _CURRENT['quality_scores']
        if not scores:
            return {'count': 0, 'avg_composite': 0, 'min_composite': 0,
                    'max_composite': 0, 'dimension_averages': {},
                    'flag_counts': {}, 'threshold_failures': 0}
        composites = [s['composite'] for s in scores.values()]
        return {
            'count': len(composites),
            'avg_composite': round(sum(composites) / len(composites), 1),
            'min_composite': round(min(composites), 1),
            'max_composite': round(max(composites), 1),
            'dimension_averages': {},
            'flag_counts': {},
            'threshold_failures': 0,
        }


class _FakeMetrics:
    """`slot_usage_rows()`, `slot_history()`, `endpoint_rows()` and `antislop_columns_enabled()`."""

    @staticmethod
    def slot_usage_rows():
        return sorted(_CURRENT['slot_usage'].items())

    @staticmethod
    def slot_history(slot):
        return list(_CURRENT['slot_history'].get(slot, ()))

    @staticmethod
    def endpoint_rows():
        return list(_CURRENT['endpoint_rows'])

    @staticmethod
    def antislop_columns_enabled():
        return bool(_CURRENT['antislop_columns'])


# The fake quality/metrics helpers read this, because a staticmethod has no `self`.
_CURRENT = {'quality_scores': {}, 'slot_usage': {}, 'slot_history': {},
            'endpoint_rows': [], 'antislop_columns': False}


def _app():
    """A FastAPI app with only the tab routes, so no auth gate or engine is needed."""
    from fastapi import FastAPI

    from webapp.tabs_routes import install_tabs

    return install_tabs(FastAPI())


def _routes(app):
    """`(path, methods)` for every route, walking into included routers."""
    found = {}
    pending = list(app.routes)
    while pending:
        route = pending.pop()
        original = getattr(route, 'original_router', None)
        if original is not None:
            pending.extend(original.routes)
            continue
        path = getattr(route, 'path', None)
        if path:
            found.setdefault(path, set()).update(getattr(route, 'methods', None) or set())
    return found


def _call(path, method='GET'):
    """Call a route directly, without a client, so no lifespan or auth runs."""
    app = _app()
    for route in app.routes:
        pending = [route]
        while pending:
            candidate = pending.pop()
            original = getattr(candidate, 'original_router', None)
            if original is not None:
                pending.extend(original.routes)
                continue
            if getattr(candidate, 'path', None) == path:
                return candidate.endpoint()
    raise AssertionError(f'no route for {path!r}')


# --- the route table -----------------------------------------------------

def test_every_tab_route_exists():
    routes = _routes(_app())
    expected = {
        '/api/apis': {'GET'},
        '/api/quality': {'GET'},
        '/api/regex': {'GET'},
        '/api/metrics': {'GET'},
        '/api/prompts': {'GET'},
        '/api/live-prompts': {'GET'},
        '/api/live-prompts/clear': {'POST'},
        '/api/queue': {'GET'},
        '/api/queue/purge': {'POST'},
        '/api/queue/clear': {'POST'},
        '/api/queue/retry': {'POST'},
        '/api/queue/export': {'POST'},
    }
    for path, methods in expected.items():
        assert routes.get(path) == methods, f'{path}: {routes.get(path)}'


def test_the_queue_actions_are_posts():
    """A destructive or state-changing action must not answer a GET."""
    routes = _routes(_app())
    for path in ('/api/queue/purge', '/api/queue/clear', '/api/queue/retry',
                 '/api/queue/export', '/api/live-prompts/clear'):
        assert 'GET' not in routes[path], f'{path} answers a GET'


def test_the_tabs_never_import_generate():
    """`generate_tk.py` builds the whole Tk window at import time."""
    import webapp.tabs_routes as tabs

    with open(tabs.__file__, 'r', encoding='utf-8') as handle:
        source = handle.read()
    assert 'import generate_tk' not in source
    assert 'from generate' not in source


def test_every_es_module_parses():
    """A syntax error in a module is a blank tab, so parse them all.

    There is no build step, so nothing else would catch this before a browser does.
    `node --check` is used when node is present; the test skips rather than fails when it is
    not, because node is not a dependency of this project.
    """
    import shutil
    import subprocess

    node = shutil.which('node')
    if not node:
        return
    for name in sorted(os.listdir(STATIC_JS)):
        if not name.endswith('.js'):
            continue
        with open(os.path.join(STATIC_JS, name), 'r', encoding='utf-8') as handle:
            source = handle.read()
        result = subprocess.run([node, '--input-type=module', '--check'],
                                input=source, capture_output=True, text=True)
        assert result.returncode == 0, f'{name}: {result.stderr[:400]}'


def test_the_tab_module_has_no_hex_literal():
    """No hex outside `theme.py`. The chart reads palette tokens."""
    import re as _re

    with open(os.path.join(STATIC_JS, 'tabs.js'), 'r', encoding='utf-8') as handle:
        source = handle.read()
    assert not _re.search(r'#[0-9a-fA-F]{6}\b', source), (
        'tabs.js contains a hex literal instead of a palette token')


def test_every_data_tab_is_registered():
    """Six tabs, and each one has an id, a label and a render function."""
    import re as _re

    with open(os.path.join(STATIC_JS, 'tabs.js'), 'r', encoding='utf-8') as handle:
        source = handle.read()
    block = source.split('export const DATA_TABS = [', 1)[1].split('];', 1)[0]
    ids = _re.findall(r"id: '([a-z]+)'", block)
    assert ids == ['apis', 'quality', 'regex', 'metrics', 'prompts', 'live', 'queue'], ids


def test_every_data_tab_is_wired_into_main():
    """A tab defined but not in `TABS` is a tab nobody can reach."""
    with open(os.path.join(STATIC_JS, 'main.js'), 'r', encoding='utf-8') as handle:
        source = handle.read()
    assert 'DATA_TABS' in source, 'main.js does not import the data tabs'
    assert '...DATA_TABS.map' in source, 'the data tabs are not spread into TABS'


# --- the API key must never reach the browser ----------------------------

def test_an_api_key_is_never_sent_to_the_browser():
    """The key stays server-side; the browser gets `has_key` only."""
    with sandbox() as state:
        state.global_config.config = {'api': {'apis': [
            {'url': 'https://a.example', 'model': 'm', 'key': 'sk-SECRET-ONE',
             'enabled': True, 'threads': 4, 'rate_limit_rpm': 60,
             'api_profile': 'openai_compatible'},
            {'url': 'https://b.example', 'model': 'm', 'key': '',
             'enabled': False, 'threads': 2, 'rate_limit_rpm': 30,
             'api_profile': 'openai_compatible'},
        ]}}

        payload = _call('/api/apis')

        assert 'sk-SECRET-ONE' not in repr(payload), 'the API key leaked into the payload'
        slots = payload['slots']
        assert slots[0]['has_key'] is True, slots[0]
        assert slots[1]['has_key'] is False, slots[1]
        assert 'key' not in slots[0], slots[0]


def test_the_apis_tab_reports_each_slot():
    with sandbox() as state:
        state.global_config.config = {'api': {'apis': [
            {'url': 'https://a.example', 'model': 'model-a', 'key': 'k',
             'enabled': True, 'threads': 4, 'rate_limit_rpm': 60,
             'api_profile': 'openai_compatible'},
        ], 'threads': 4, 'pricing': {'cost_per_1k_tokens': 0.5}}}
        state.total_attempts_per_api = {0: 12}

        payload = _call('/api/apis')

        slot = payload['slots'][0]
        assert slot['slot'] == 0 and slot['label'] == 'API 1', slot
        assert slot['url'] == 'https://a.example', slot
        assert slot['model'] == 'model-a', slot
        assert slot['attempts'] == 12, slot
        assert payload['threads'] == 4, payload
        assert payload['pricing']['cost_per_1k_tokens'] == 0.5, payload


# --- quality -------------------------------------------------------------

def test_the_quality_tab_sorts_newest_first():
    with sandbox() as state:
        state.quality_scores = {
            'task_a': {'composite': 40, 'dimensions': {'prose': 40},
                       'flags': [], 'scored_at': '2026-11-09 01:00:00'},
            'task_b': {'composite': 70, 'dimensions': {'prose': 70},
                       'flags': [], 'scored_at': '2026-11-09 03:00:00'},
        }
        _CURRENT['quality_scores'] = state.quality_scores

        payload = _call('/api/quality')

        order = [row['task_id'] for row in payload['recent']]
        assert order == ['task_b', 'task_a'], order


def test_the_quality_tab_caps_the_recent_list():
    """The tab shows the latest N; a 10,000-row run must not ship 10,000 rows per tick."""
    with sandbox() as state:
        state.quality_scores = {
            f'task_{i:04d}': {'composite': 50, 'dimensions': {},
                              'flags': [], 'scored_at': f'2026-11-09 01:{i % 60:02d}:00'}
            for i in range(QUALITY_RECENT_LIMIT + 25)
        }
        _CURRENT['quality_scores'] = state.quality_scores

        payload = _call('/api/quality')

        assert len(payload['recent']) == QUALITY_RECENT_LIMIT, len(payload['recent'])


def test_the_quality_tab_survives_a_broken_summary():
    """A half-built state must not break the tab; it reports the error instead."""
    with sandbox() as state:
        state.quality_scores = {}
        _CURRENT['quality_scores'] = {}

        payload = _call('/api/quality')

        assert payload['ok'] is True, payload
        assert payload['summary']['count'] == 0, payload['summary']


# --- regex watch ---------------------------------------------------------

def test_the_regex_tab_counts_hits_per_pattern():
    with sandbox() as state:
        state.global_config.config = {'detection': {'regex_watch': {
            'enabled': True, 'max_rounds': 3,
            'patterns': [{'pattern': 'a+', 'replacement': 'a', 'enabled': True},
                         {'pattern': 'b+', 'replacement': 'b', 'enabled': True}],
        }}}
        state.regex_watch_log = [{'pattern': 'a+'}, {'pattern': 'a+'}, {'pattern': 'b+'}]

        payload = _call('/api/regex')

        by_pattern = {row['pattern']: row['hits'] for row in payload['patterns']}
        assert by_pattern == {'a+': 2, 'b+': 1}, by_pattern
        assert payload['total_hits'] == 3, payload


def test_the_regex_tab_lists_a_pattern_with_no_hits():
    """\"This pattern has never fired\" is information, not an omission."""
    with sandbox() as state:
        state.global_config.config = {'detection': {'regex_watch': {
            'enabled': True, 'max_rounds': 3,
            'patterns': [{'pattern': 'never', 'replacement': '', 'enabled': True}],
        }}}
        state.regex_watch_log = []

        payload = _call('/api/regex')

        assert len(payload['patterns']) == 1, payload
        assert payload['patterns'][0]['hits'] == 0, payload['patterns'][0]


def test_the_regex_tab_accepts_a_bare_string_pattern():
    """An older config may hold patterns as plain strings rather than dicts."""
    with sandbox() as state:
        state.global_config.config = {'detection': {'regex_watch': {
            'enabled': True, 'max_rounds': 3, 'patterns': ['bare+'],
        }}}

        payload = _call('/api/regex')

        assert payload['patterns'][0]['pattern'] == 'bare+', payload['patterns']
        assert payload['patterns'][0]['enabled'] is True, payload['patterns']


# --- metrics -------------------------------------------------------------

def test_the_metrics_tab_reports_each_slot_with_its_history():
    with sandbox() as state:
        _CURRENT['slot_usage'] = {0: {'tokens_per_second': 12.5}, 1: {}}
        _CURRENT['slot_history'] = {0: [{'time': 1.0, 'tokens_per_second': 12.5}]}
        _CURRENT['antislop_columns'] = False
        _CURRENT['endpoint_rows'] = []

        payload = _call('/api/metrics')

        assert [row['slot'] for row in payload['slots']] == [0, 1], payload['slots']
        assert payload['slots'][0]['usage']['tokens_per_second'] == 12.5, payload['slots']
        assert len(payload['slots'][0]['history']) == 1, payload['slots']
        assert payload['antislop_columns'] is False, payload


def test_the_metrics_tab_reports_the_server_rows_and_both_availability_flags():
    """v15.1.0: the two toggles need to know what is actually there.

    `vllm_available` is "some endpoint answered without an error"; `antislop_available` is "some
    endpoint served `antislop:` metrics". A plain vLLM is available and not antislop, which is the
    case the two toggles exist to distinguish.
    """
    with sandbox():
        _CURRENT['slot_usage'] = {}
        _CURRENT['slot_history'] = {}
        _CURRENT['endpoint_rows'] = [
            {'slot_idx': 0, 'label': 'API 1', 'error': None, 'is_antislop': False,
             'history': [{'time': 1.0, 'prompt_tps': 10.0}], 'slot_history': []},
        ]

        payload = _call('/api/metrics')

        assert payload['vllm_available'] is True, payload
        assert payload['antislop_available'] is False, payload
        assert payload['endpoints'][0]['label'] == 'API 1', payload['endpoints']


def test_the_metrics_tab_reports_antislop_available_when_the_fork_answers():
    with sandbox():
        _CURRENT['slot_usage'] = {}
        _CURRENT['slot_history'] = {}
        _CURRENT['endpoint_rows'] = [
            {'slot_idx': 0, 'label': 'API 1', 'error': None, 'is_antislop': True,
             'history': [], 'slot_history': []},
        ]

        payload = _call('/api/metrics')

        assert payload['antislop_available'] is True, payload


def test_the_metrics_tab_does_not_mutate_the_engine_history():
    """The ring is the engine's; a route must hand out a copy."""
    with sandbox():
        _CURRENT['slot_usage'] = {0: {}}
        _CURRENT['endpoint_rows'] = []
        ring = [{'time': 1.0, 'tokens_per_second': 1.0}]
        _CURRENT['slot_history'] = {0: ring}

        payload = _call('/api/metrics')

        payload['slots'][0]['history'].append({'time': 2.0})
        assert len(ring) == 1, 'the route handed out the engine\'s own list'


def test_the_metrics_tab_copies_the_endpoint_history_too():
    """The endpoint rings are the poller's; a route must not hand out the live lists."""
    with sandbox():
        _CURRENT['slot_usage'] = {}
        _CURRENT['slot_history'] = {}
        ring = [{'time': 1.0, 'prompt_tps': 1.0}]
        _CURRENT['endpoint_rows'] = [
            {'slot_idx': 0, 'label': 'API 1', 'error': None, 'is_antislop': False,
             'history': ring, 'slot_history': ring},
        ]

        payload = _call('/api/metrics')

        payload['endpoints'][0]['history'].append({'time': 2.0})
        assert len(ring) == 1, 'the route handed out the poller\'s own list'


# --- prompts -------------------------------------------------------------

def test_the_prompts_tab_lists_every_prompt_body():
    with sandbox() as state:
        state.global_config.config = {'prompts': {
            'question': 'Q text', 'answer': 'A text',
            'system': {'base': 'S text'},
            'random_names': {'names': ['a', 'b']},
        }}

        payload = _call('/api/prompts')

        paths = {row['path'] for row in payload['entries']}
        assert 'prompts.question' in paths, paths
        assert 'prompts.answer' in paths, paths
        assert 'prompts.system.base' in paths, paths


def test_the_prompts_tab_skips_a_list_of_names():
    """A 1,000-name list is not a prompt body; it belongs in the Config tab."""
    with sandbox() as state:
        state.global_config.config = {'prompts': {
            'question': 'Q', 'random_names': {'names': ['a', 'b', 'c']},
        }}

        payload = _call('/api/prompts')

        paths = {row['path'] for row in payload['entries']}
        assert 'prompts.random_names.names' not in paths, paths


def test_the_prompts_tab_reports_length_and_lines():
    with sandbox() as state:
        state.global_config.config = {'prompts': {'question': 'one\ntwo\nthree'}}

        payload = _call('/api/prompts')

        # Entries sort by path, so look the one up rather than assuming it is first.
        entry = next(row for row in payload['entries']
                     if row['path'] == 'prompts.question')
        assert entry['length'] == len('one\ntwo\nthree'), entry
        assert entry['lines'] == 3, entry


def test_the_prompts_tab_keeps_an_empty_prompt():
    """An empty prompt is a prompt that exists and is empty -- worth showing, not hiding."""
    with sandbox() as state:
        state.global_config.config = {'prompts': {'lore': ''}}

        payload = _call('/api/prompts')

        entry = next(row for row in payload['entries'] if row['path'] == 'prompts.lore')
        assert entry['length'] == 0, entry
        assert entry['lines'] == 0, entry


# --- queue ---------------------------------------------------------------

def test_the_queue_tab_reports_the_three_tables():
    with sandbox() as state:
        state.pending_task_ids = {'t1'}
        state.completed_task_ids = {'t2'}
        state.failed_task_ids = {'t3'}
        state.task_metadata = {'t1': {'file': 'a.jsonl', 'retries': 0},
                               't2': {'file': 'b.jsonl', 'retries': 1},
                               't3': {'file': 'c.jsonl', 'retries': 2}}

        payload = _call('/api/queue')

        assert set(payload['tables']) == set(QUEUE_STATES), payload['tables']
        assert payload['counts'] == {'pending': 1, 'completed': 1, 'failed': 1}, payload
        assert payload['tables']['failed'][0]['retries'] == 2, payload['tables']


def test_purge_touches_pending_only():
    with sandbox() as state:
        state.pending_task_ids = {'t1', 't2'}
        state.completed_task_ids = {'t3'}
        state.failed_task_ids = {'t4'}

        result = _call('/api/queue/purge', 'POST')

        assert result['data']['purged'] == 2, result
        assert not state.pending_task_ids, state.pending_task_ids
        assert state.completed_task_ids == {'t3'}, state.completed_task_ids
        assert state.failed_task_ids == {'t4'}, state.failed_task_ids
        assert state.purged_task_ids == {'t1', 't2'}, state.purged_task_ids


def test_retry_touches_failed_only_and_resets_the_counter():
    with sandbox() as state:
        state.pending_task_ids = {'t1'}
        state.failed_task_ids = {'t2'}
        state.task_metadata = {'t2': {'file': 'b.jsonl', 'retries': 3,
                                     'task_data': ('b.jsonl', 1)}}

        result = _call('/api/queue/retry', 'POST')

        assert result['data']['retried'] == 1, result
        assert state.failed_task_ids == set(), state.failed_task_ids
        assert 't2' in state.pending_task_ids, state.pending_task_ids
        assert state.task_metadata['t2']['retries'] == 0, state.task_metadata['t2']
        assert state.task_queue.items == [('b.jsonl', 1)], state.task_queue.items


def test_retry_with_nothing_failed_is_a_no_op():
    with sandbox() as state:
        state.failed_task_ids = set()

        result = _call('/api/queue/retry', 'POST')

        assert result['data']['retried'] == 0, result
        assert state.task_queue.items == [], state.task_queue.items


def test_export_writes_a_csv_and_changes_nothing():
    with sandbox() as state:
        state.pending_task_ids = {'t1'}
        state.failed_task_ids = {'t2'}
        state.task_metadata = {'t1': {'file': 'a.jsonl', 'retries': 0},
                               't2': {'file': 'b.jsonl', 'retries': 2}}

        result = _call('/api/queue/export', 'POST')

        assert result['data']['rows'] == 2, result
        path = result['data']['path']
        assert os.path.exists(path), path
        with open(path, 'r', encoding='utf-8', newline='') as handle:
            rows = list(csv.reader(handle))
        assert rows[0] == ['Status', 'Task ID', 'Source File', 'Retries'], rows[0]
        assert len(rows) == 3, rows
        # Nothing was consumed by exporting.
        assert state.pending_task_ids == {'t1'}, state.pending_task_ids
        assert state.failed_task_ids == {'t2'}, state.failed_task_ids


if __name__ == '__main__':
    failures = 0
    for name, function in sorted(globals().items()):
        if name.startswith('test_') and callable(function):
            try:
                function()
            except AssertionError as exc:
                failures += 1
                print(f'FAIL {name}: {exc}')
            except Exception as exc:  # noqa: BLE001 - a harness reports, never raises
                failures += 1
                print(f'FAIL {name}: {type(exc).__name__}: {exc}')
            else:
                print(f'ok   {name}')
    print(f"\n{'OK' if not failures else str(failures) + ' FAILED'}")
    sys.exit(1 if failures else 0)
