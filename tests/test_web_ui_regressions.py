# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Regression tests for the four Web UI bugs found after v15.0.0.

Each one was a *silent* failure -- the screen rendered, the numbers were just wrong or
missing -- so each test asserts the thing that was actually broken, not a proxy for it:

1. **Recent issues were blank.** The engine stored `recent` entries as bare tuples
   `(phrase, context, api_slot)` with no timestamp at all, and the browser read
   `row.time` / `row.phrase` / `row.context` off them, so every cell was `undefined`
   and rendered as an em dash. The table had the right number of rows and no data.
2. **The Metrics graphs were empty while the table had data.** The table is written by
   the generation threads (`note_slot_usage`), but the ring the graphs draw is only
   appended to by the vLLM poller -- and the web app never started it, because
   `start_polling()` used to be started by the front end itself.
3. **Download dataset was a 404 page.** The route is correct; the button navigated the
   whole tab to it, so a run with no rows yet replaced the app with a raw JSON error.
4. **Two panels were clipped.** The graph panel and the Review detail header were both
   flex/grid children with no `flex: 0 0 auto` / `min-height`, so a short column
   squeezed them and `.panel { overflow: hidden }` cut the bottom off.

The CSS assertions read `theme.STYLESHEET`; the payload assertions call the real
`control.build_stats_payload`; the poller assertion starts the real thread against an
unreachable host and checks the ring grows. No display, no browser, no network.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app_state
import control
import vllm_metrics
import webapp.theme as theme

STATIC_JS = os.path.join(os.path.dirname(os.path.abspath(theme.__file__)), 'static', 'js')


def _read(name):
    with open(os.path.join(STATIC_JS, name), 'r', encoding='utf-8') as handle:
        return handle.read()


def _strip_comments(source):
    """Drop `/* ... */` and `//` comments, so a rule about code is not satisfied by prose."""
    import re
    source = re.sub(r'/\*.*?\*/', '', source, flags=re.DOTALL)
    source = re.sub(r'^\s*//.*$', '', source, flags=re.MULTILINE)
    return source


def _css():
    return _strip_comments(theme.STYLESHEET)


def _css_block(selector):
    """The body of the TOP-LEVEL rule for `selector`, ignoring @media blocks."""
    css = _css()
    index = 0
    depth = 0
    while index < len(css):
        char = css[index]
        if char == '{':
            depth += 1
        elif char == '}':
            depth -= 1
        elif depth == 0 and css.startswith(selector, index):
            start = css.index('{', index)
            end = css.index('}', start)
            return css[start + 1:end]
        index += 1
    raise AssertionError(f'no top-level rule for {selector!r}')


# --- 1. recent issues carry a real time and named fields ------------------

def test_record_recent_writes_a_dict_with_a_time():
    """The engine's own recorder must produce `{time, phrase, context, api}`."""
    bucket = []
    app_state.record_recent(bucket, 'a phrase', 'some context', 2)
    assert len(bucket) == 1, bucket
    entry = bucket[0]
    assert set(entry) == {'time', 'phrase', 'context', 'api'}, entry
    assert entry['phrase'] == 'a phrase', entry
    assert entry['context'] == 'some context', entry
    assert entry['api'] == 2, entry
    # HH:MM:SS, which is what the Time column shows. A blank time was the visible bug.
    assert len(entry['time']) == 8 and entry['time'].count(':') == 2, entry


def test_record_recent_caps_the_ring():
    """The ring is trimmed in one place, so no call site can grow it without bound."""
    bucket = []
    for index in range(app_state.MAX_RECENT + 5):
        app_state.record_recent(bucket, f'p{index}', 'c', 0)
    assert len(bucket) == app_state.MAX_RECENT, len(bucket)
    # The OLDEST entries are the ones dropped.
    assert bucket[-1]['phrase'] == f'p{app_state.MAX_RECENT + 4}', bucket[-1]


def test_record_recent_keeps_the_task_level_sentinel():
    """`-1` means "not tied to a slot", which the web UI already used."""
    bucket = []
    app_state.record_recent(bucket, 'task error', '', -1)
    assert bucket[0]['api'] == -1, bucket[0]


def test_the_stats_payload_sends_recent_as_objects():
    """The wire contract is dicts, so the browser never has to guess an index."""
    saved = app_state.recent_refusals_total
    try:
        app_state.recent_refusals_total = []
        app_state.record_recent(app_state.recent_refusals_total, 'I cannot', 'the reply', 1)
        payload = control.build_stats_payload(include_recent=True)
        recent = payload['detected']['refusals']['recent']
        assert len(recent) == 1, recent
        entry = recent[0]
        assert isinstance(entry, dict), f'recent is still {type(entry).__name__}: {entry!r}'
        assert entry['phrase'] == 'I cannot', entry
        assert entry['context'] == 'the reply', entry
        assert entry['api'] == 1, entry
        assert entry['time'], 'the Time column would be blank'
    finally:
        app_state.recent_refusals_total = saved


def test_the_stats_payload_normalises_a_legacy_tuple():
    """A process started before the change still holds tuples; they must not blank the table.

    This is the exact shape that caused the bug, so it is the one case that has to keep
    working: `(phrase, context, api_slot)` with no time.
    """
    saved = app_state.recent_refusals_total
    try:
        app_state.recent_refusals_total = [('old phrase', 'old context', 3)]
        payload = control.build_stats_payload(include_recent=True)
        entry = payload['detected']['refusals']['recent'][0]
        assert isinstance(entry, dict), entry
        assert entry['phrase'] == 'old phrase', entry
        assert entry['context'] == 'old context', entry
        assert entry['api'] == 3, entry
        # A tuple never carried a time, so an empty string is correct here -- but the
        # field must EXIST, so the browser reads a string rather than `undefined`.
        assert entry['time'] == '', entry
    finally:
        app_state.recent_refusals_total = saved


def test_the_overview_reads_the_named_fields():
    """The table must read `time` / `phrase` / `context` / `api`, not tuple indexes."""
    source = _strip_comments(_read('overview.js'))
    assert 'normaliseIssue' in source, 'there is no normaliser for a recent entry'
    block = source.split('function normaliseIssue', 1)[1].split('\n}', 1)[0]
    for field in ('time', 'phrase', 'context', 'api'):
        assert field in block, f'{field} is not read by normaliseIssue'
    # The old bug: reading `row.phrase` off a tuple gives undefined. The normaliser
    # must handle the array shape explicitly.
    assert 'Array.isArray' in block, 'a legacy array entry is not handled'


def test_the_issue_table_renders_the_time_column():
    source = _strip_comments(_read('overview.js'))
    block = source.split('function issueTable', 1)[1].split('\n}', 1)[0]
    assert 'entry.time' in block, 'the Time column does not read the entry time'
    assert "'—'" in block or '"—"' in block, 'there is no fallback for a missing value'


# --- 2. the metrics poller runs in the web app ----------------------------

def test_the_engine_starts_the_metrics_poller():
    """The web app has no separate GUI process, so it must start the poller itself."""
    source = _read('../../engine.py')
    assert 'vllm_metrics' in source, 'the engine does not import the metrics module'
    assert 'start_polling' in source, (
        'the web app never starts the vLLM poller, so the Metrics graphs stay empty')
    assert 'stop_polling' in source, 'the poller is never stopped on shutdown'


def test_the_engine_can_skip_the_poller_for_a_test():
    """A test must not make HTTP requests, so the flag has to exist and default to on."""
    source = _read('../../engine.py')
    assert 'start_metrics' in source, 'there is no way to disable the poller'
    assert 'start_metrics=True' in source, 'the poller is off by default'


def test_a_poll_cycle_fills_the_graph_ring():
    """The table is written by the generation threads; the GRAPH ring only by a poll cycle.

    This is the bug in one assertion: `note_slot_usage` fills `slot_usage_rows()` (the
    table) and leaves `slot_history()` (the graph) empty until `poll_once` runs. The web app
    never called it, so the Metrics tab showed a full table above four empty graphs.

    `poll_once` is called with an explicit config and an unreachable host on purpose: the
    sample is recorded whether or not the fetch succeeds, and a test must not depend on a
    leftover `output/metrics_history.json` or on `app_state.global_config`.
    """
    class _Config(dict):
        def get(self, path, default=None):
            return default

    config = _Config({'api': {'apis': [{'url': 'http://127.0.0.1:9'}]}})
    vllm_metrics.reset_slot_history()
    vllm_metrics.reset_slot_usage()
    try:
        vllm_metrics.note_slot_usage(0, {'prompt_tokens': 100, 'completion_tokens': 300}, 2.0)
        assert vllm_metrics.slot_usage_rows(), 'the table should have a row'
        assert vllm_metrics.slot_history(0) == [], (
            'the graph ring should be empty before a poll -- this is the bug')

        vllm_metrics.poll_once(config)

        ring = vllm_metrics.slot_history(0)
        assert ring, 'the poll cycle did not append a sample to the graph ring'
        assert 'time' in ring[0], ring[0]
        assert 'tokens_per_second' in ring[0], (
            'the graph reads tokens_per_second, so the sample must carry the key')
    finally:
        vllm_metrics.reset_slot_history()
        vllm_metrics.reset_slot_usage()


# --- 3. download reports "nothing yet" instead of navigating to JSON -------

def test_the_download_button_preflights_the_row_count():
    """A plain navigation to a 404 replaces the app with raw JSON."""
    source = _strip_comments(_read('overview.js'))
    block = source.split('async function downloadDataset', 1)[1].split('\n}', 1)[0]
    assert "fetch('/api/dataset'" in block, (
        'the download button does not check the row count first, so a 404 navigates the page')
    assert 'total_rows' in block, block
    assert 'toast' in block, 'an empty dataset is not reported to the reader'


def test_the_download_route_explains_an_empty_dataset():
    """The 404 body is also what a direct visit shows, so it has to say what to do."""
    source = _read('../../dataset_routes.py')
    block = source.split("if not ok:", 1)[1].split('filename =', 1)[0]
    assert 'status=404' in block, block
    assert 'no dataset to download' in block.lower(), (
        'the 404 message does not tell the reader there is nothing to download yet')


# --- 4. the graph and the review header are not clipped ------------------

def test_the_graph_panel_has_a_minimum_height():
    """The chart is a fixed 132px; a shorter panel clips its x-axis labels."""
    css = _css()
    assert '.overview-side > .panel:last-child' in css, (
        'the graph panel has no min-height, so a short column clips the chart')
    block = _css_block('.overview-side > .panel:last-child')
    assert 'min-height' in block, block
    value = int(block.split('min-height:', 1)[1].split('px', 1)[0].strip())
    # 132px chart + 12px legend + 6px gap + 20px body padding + 37px head = 207.
    assert value >= 207, f'min-height {value}px is less than the chart needs'


def test_the_side_column_scrolls_rather_than_clipping():
    """The column must scroll when it is shorter than its contents."""
    block = _css_block('.overview-side')
    assert 'overflow-y: auto' in block, (
        'the side column cannot scroll, so a short viewport clips the graph')


def test_the_review_detail_header_does_not_shrink():
    """A long transcript made the flex algorithm shrink the header to a sliver."""
    css = _css()
    assert '.review-detail > .detail-head' in css, (
        'the detail header has no flex-shrink rule')
    block = _css_block('.review-detail > .detail-head')
    assert 'flex: 0 0 auto' in block, block


def test_the_review_detail_note_does_not_shrink():
    css = _css()
    assert '.review-detail > .detail-note' in css, (
        'the detail note has no flex-shrink rule')
    block = _css_block('.review-detail > .detail-note')
    assert 'flex: 0 0 auto' in block, block


def test_the_chart_host_cannot_collapse():
    """A zero-height host draws an SVG that overflows and is clipped."""
    block = _css_block('.chart-host')
    assert 'min-height' in block, block


# --- 5. a chart draws even without a frame --------------------------------

def test_the_graph_draws_without_a_frame():
    """`requestAnimationFrame` alone is not a guarantee.

    A background tab or a throttled renderer may not produce a frame for seconds, and the
    old code drew *only* from `requestAnimationFrame`, so the graph stayed blank until
    something else forced a paint. Both chart call sites must have a timeout fallback.
    """
    for name in ('overview.js', 'tabs.js'):
        source = _strip_comments(_read(name))
        assert 'function drawWhenReady' in source, f'{name} has no draw fallback'
        block = source.split('function drawWhenReady', 1)[1].split('\n}', 1)[0]
        assert 'requestAnimationFrame' in block, block
        assert 'setTimeout' in block, (
            f'{name} draws only on a frame, so a throttled tab leaves it blank')
        # The second caller must be a no-op, or the chart is drawn twice.
        assert 'childElementCount' in block or 'done' in block, block


def test_the_graph_panel_uses_the_fallback():
    """The helper existing is not enough; the panel must call it."""
    source = _strip_comments(_read('overview.js'))
    block = source.split('function graphPanel', 1)[1].split('\n}', 1)[0]
    assert 'drawWhenReady' in block, 'the issue graph does not use the fallback'


def test_the_metrics_graphs_use_the_fallback():
    source = _strip_comments(_read('tabs.js'))
    block = source.split('const graphs = slots.map', 1)[1].split('return el(', 1)[0]
    assert 'drawWhenReady' in block, 'the metrics graphs do not use the fallback'


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
