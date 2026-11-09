# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the Review tab.

Review is the one screen the maintainer named by hand, and it is the only screen that can
destroy data. So this file pins two different things:

**The layout decisions**, because they are what makes a flagged conversation
readable rather than a wall of numbers:

* the two-pane split is `1fr / 1.15fr`, list left, detail right;
* **selecting a row does not navigate away** -- the list and the detail are both in the DOM,
  and a click only repaints the detail pane;
* the score breakdown is a **footer strip** with the four dimensions plus Method, colour-coded
  `ok` / `warn` / `danger` / `muted` by the threshold.

**The destructive-operation rules**, because a bug here deletes a user's dataset:

* **Drop is a POST with the task id in the path**, so a link, a prefetch or a stale tab
  cannot fire it;
* Drop acts on **the selection only**, never on the filtered list;
* **Dismiss and Export never touch the dataset** -- they are separate routes, not a mode flag;
* `record_belongs_to_task` does not widen `abc` into `abcdef`;
* a **non-JSON line is never discarded** by a drop;
* `drop()` records `quality_dropped[task_id] == 'manual'` and clears the score;
* `dismiss_all()` clears the flags and deletes no data;
* `export_flagged()` always rewrites the file and reports the ids it could not find.

Everything runs against a **temporary directory** and a **fake `app_state`**. The real
`output/` is never opened, and no server is started.
"""

import json
import os
import re
import sys
import tempfile
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import review_data
import webapp.theme as theme

STATIC_JS = os.path.join(os.path.dirname(os.path.abspath(theme.__file__)), 'static', 'js')


# --- helpers -------------------------------------------------------------

def _read(name):
    with open(os.path.join(STATIC_JS, name), 'r', encoding='utf-8') as handle:
        return handle.read()


def _strip_comments(source):
    """Drop `/* ... */` and `//` comments, so a rule is about code, not prose."""
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


def _media_block(selector, query='@media (max-width: 1100px)'):
    """The body of the `query` block that actually contains `selector`.

    There is more than one `@media (max-width: 1100px)` block in the stylesheet -- Overview
    has one and Review has another -- so splitting on the query alone reads the wrong one.
    """
    css = _css()
    start = 0
    while True:
        index = css.find(query, start)
        if index == -1:
            raise AssertionError(f'no {query} block contains {selector!r}')
        opening = css.index('{', index)
        depth = 0
        end = opening
        while end < len(css):
            if css[end] == '{':
                depth += 1
            elif css[end] == '}':
                depth -= 1
                if depth == 0:
                    break
            end += 1
        block = css[opening + 1:end]
        if selector in block:
            return block
        start = end


def _routes(app):
    """`(path, methods)` for every route, walking into included routers.

    `app.include_router()` appends an `_IncludedRouter` wrapper with no `.path` and no
    `.routes`, so a flat scan of `app.routes` finds nothing. The real routes hang off
    `original_router`.
    """
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


class FakeAppState:
    """A stand-in for `app_state` with the attributes `review_data` touches.

    Real `threading.Lock` objects, real dicts and sets, and an `OUTPUT_DIR` pointing at a
    temporary directory -- so the locking and the file rewrite are exercised for real while the
    user's dataset is never opened.
    """

    def __init__(self, output_dir):
        self.OUTPUT_DIR = output_dir
        self.quality_lock = threading.Lock()
        self.quality_review_lock = threading.Lock()
        self.output_data_lock = threading.Lock()
        self.task_queue_ui_lock = threading.Lock()
        self.quality_scores = {}
        self.quality_review_ids = set()
        self.quality_dropped = {}
        self.dropped_task_ids = set()
        self.task_metadata = {}
        self.db_pool = None
        self.global_config = {'database.enabled': False}


class sandbox:
    """Swap `review_data.app_state` for a fake and silence the log, then restore.

    `review_data` does `import app_state` at module scope, so assigning the module attribute
    is what redirects every function in it. The originals are restored on exit even if a test
    raises, so one failure cannot poison the rest of the file.
    """

    def __enter__(self):
        self._directory = tempfile.TemporaryDirectory()
        self._saved = (review_data.app_state, review_data.log_message)
        self.state = FakeAppState(self._directory.name)
        review_data.app_state = self.state
        review_data.log_message = lambda *args, **kwargs: None
        return self.state

    def __exit__(self, *exc_info):
        review_data.app_state, review_data.log_message = self._saved
        self._directory.cleanup()
        return False


def _write_jsonl(directory, name, records):
    path = os.path.join(directory, name)
    with open(path, 'w', encoding='utf-8') as handle:
        for record in records:
            handle.write(json.dumps(record) + '\n')
    return path


def _read_lines(path):
    with open(path, 'r', encoding='utf-8') as handle:
        return handle.readlines()


# --- the two-pane split --------------------------------------------------

def test_the_review_grid_is_a_1fr_to_1_15fr_split():
    """The detail pane is slightly wider because it holds prose."""
    block = _css_block('.review-grid')
    assert '1fr 1.15fr' in block, block
    assert 'display: grid' in block, block


def test_both_panes_can_shrink_so_each_scrolls_on_its_own():
    """Without `min-height: 0` a long transcript pushes the footer strip off the screen."""
    block = _css_block('.review-grid')
    assert 'min-height: 0' in block, block


def test_the_split_collapses_to_one_column_on_a_narrow_screen():
    block = _media_block('.review-grid')
    assert '.review-grid { grid-template-columns: 1fr; }' in block, block


# --- selecting a row does not navigate away -------------------------------

def test_the_screen_holds_the_list_and_the_detail_at_once():
    """Selecting a row must not navigate away.

    `screen()` builds one `.review-grid` containing both `listPane()` and `detailPane()`, so
    both are in the DOM at the same time. A layout that swapped one for the other would be a
    navigation, not a selection.
    """
    source = _strip_comments(_read('review.js'))
    block = source.split('function screen()', 1)[1].split('\n}', 1)[0]
    assert "'review-grid'" in block, block
    assert 'listPane()' in block, block
    assert 'detailPane()' in block, block


def test_selecting_a_row_only_reloads_the_detail_pane():
    """`select()` sets the selection and loads the detail -- it never changes the URL."""
    source = _strip_comments(_read('review.js'))
    block = source.split('async function select(', 1)[1].split('\n}', 1)[0]
    assert 'state.selected = taskId' in block, block
    assert 'loadDetail(taskId)' in block, block
    for navigation in ('location.href', 'location.assign', 'location.replace',
                       'window.open'):
        assert navigation not in block, f'select() navigates via {navigation}'


def test_repaint_replaces_the_panes_rather_than_the_page():
    """A full re-render would clear the filter input's focus and the list's scroll."""
    source = _strip_comments(_read('review.js'))
    block = source.split('function repaint()', 1)[1].split('\n}', 1)[0]
    assert 'replaceChildren' in block, block
    assert 'location' not in block, 'repaint() navigates instead of repainting'


def test_the_selection_survives_a_stats_tick():
    """The selection lives at module scope, so a tick does not reset the reader's place.

    This is the v11.4 bug the Tk tab records: rebuilding the list on every tick made reading a
    flagged conversation impossible.
    """
    source = _strip_comments(_read('review.js'))
    assert re.search(r'^const state = \{', source, re.MULTILINE), (
        'the Review state is not at module scope')
    block = source.split('export function renderReview(', 1)[1].split('\n}', 1)[0]
    assert 'if (!mounted)' in block, (
        'renderReview() refreshes on every tick instead of only on mount')
    assert 'refresh()' in block, block


def test_a_slow_detail_response_cannot_overwrite_a_newer_selection():
    """The reader may click a second row before the first response arrives."""
    source = _strip_comments(_read('review.js'))
    block = source.split('async function loadDetail(', 1)[1].split('\n}', 1)[0]
    assert 'state.selected !== taskId' in block, block


# --- the score footer strip ---------------------------------------------

def test_the_footer_strip_has_the_four_dimensions():
    """Coherence / Character / Prose / Overall, in the Tk order."""
    source = _strip_comments(_read('review.js'))
    block = source.split('const FOOTER_DIMENSIONS = [', 1)[1].split('];', 1)[0]
    keys = re.findall(r"key: '([a-z_]+)'", block)
    assert keys == ['coherence', 'character', 'prose', 'overall'], keys
    for label in ("'Coherence'", "'Character'", "'Prose'", "'Overall'"):
        assert label in block, f'the footer strip lost {label}'


def test_the_footer_strip_shows_the_method():
    source = _strip_comments(_read('review.js'))
    block = source.split('function scoreFooter(', 1)[1].split('\n}', 1)[0]
    assert "'Method'" in block, block
    assert 'detail.method' in block, block


def test_the_footer_strip_is_colour_coded_by_threshold():
    """Below 50 is danger, below 70 warn, otherwise ok; an unknown value is muted."""
    source = _strip_comments(_read('review.js'))
    block = source.split('function scoreFooter(', 1)[1].split('\n}', 1)[0]
    assert "!known ? 'muted'" in block, block
    assert "Number(value) < threshold ? 'danger'" in block, block
    assert "Number(value) < 70 ? 'warn' : 'ok'" in block, block
    assert 'const threshold = 50' in block, block


def test_the_footer_strip_is_pinned_below_the_transcript():
    """The transcript scrolls; the breakdown does not."""
    block = _css_block('.score-strip')
    assert 'flex: 0 0 auto' in block, block
    assert 'border-top' in block, block


def test_every_score_level_has_a_colour():
    css = _css()
    for level in ('ok', 'warn', 'danger', 'muted'):
        assert f'.score-cell.{level} .score-value' in css, f'{level} has no colour'


def test_the_footer_strip_only_appears_when_there_is_a_score():
    source = _strip_comments(_read('review.js'))
    block = source.split('function detailPane()', 1)[1].split('\n}', 1)[0]
    assert 'state.detail.score !== null' in block, block
    assert 'scoreFooter(state.detail)' in block, block


# --- drop is a POST with the id in the path ------------------------------

def test_drop_is_a_post_with_the_task_id_in_the_path():
    """A link, a prefetch or a stale tab cannot fire a destructive GET."""
    source = _strip_comments(_read('review.js'))
    block = source.split('async function onDrop()', 1)[1].split('\n}', 1)[0]
    assert '/drop`' in block, block
    assert 'encodeURIComponent(taskId)' in block, block
    # `act()` is the only POST helper, and it hard-codes the method.
    act = source.split('async function act(', 1)[1].split('\n}', 1)[0]
    assert "method: 'POST'" in act, act


def test_drop_acts_on_the_selection_only():
    """Never on the filtered list, and never on a loop over rows."""
    source = _strip_comments(_read('review.js'))
    block = source.split('async function onDrop()', 1)[1].split('\n}', 1)[0]
    assert 'state.selected' in block, block
    assert 'for (' not in block, 'onDrop() iterates; it must act on one task id'
    assert 'state.rows' not in block, 'onDrop() reads the list; it must act on the selection'


def test_drop_asks_before_destroying_data():
    """`onDrop` passes the confirmation text; `act` is what shows it."""
    source = _strip_comments(_read('review.js'))
    block = source.split('async function onDrop()', 1)[1].split('\n}', 1)[0]
    assert 'cannot be undone' in block, 'the confirmation does not warn that it is permanent'
    assert 'Permanently remove' in block, block
    # The third argument to act() is the confirmation text, and act() is what prompts.
    assert re.search(r'await act\(`/api/review/\$\{encodeURIComponent\(taskId\)\}/drop`, '
                     r'\{\},', block), block
    act = source.split('async function act(', 1)[1].split('\n}', 1)[0]
    assert 'window.confirm(confirmText)' in act, 'act() does not confirm a destructive call'
    assert 'if (confirmText && !window.confirm(confirmText)) return' in act, act


def test_drop_refuses_to_guess_a_target():
    """With no selection it says so instead of picking a row."""
    source = _strip_comments(_read('review.js'))
    block = source.split('async function onDrop()', 1)[1].split('\n}', 1)[0]
    assert 'if (!state.selected)' in block, block


# --- dismiss and export never touch the dataset ----------------------------

def test_dismiss_and_export_are_separate_routes():
    """A mode flag on one endpoint is one typo away from deleting data."""
    source = _strip_comments(_read('review.js'))
    export_block = source.split('async function onExport()', 1)[1].split('\n}', 1)[0]
    dismiss_block = source.split('async function onDismiss()', 1)[1].split('\n}', 1)[0]
    assert "'/api/review/export'" in export_block, export_block
    assert "'/api/review/dismiss'" in dismiss_block, dismiss_block
    for block in (export_block, dismiss_block):
        assert '/drop' not in block, 'a non-destructive action reaches the drop route'


def test_dismiss_says_it_deletes_nothing():
    source = _strip_comments(_read('review.js'))
    block = source.split('async function onDismiss()', 1)[1].split('\n}', 1)[0]
    assert 'does NOT delete any output data' in block, block


def test_the_routes_are_installed_with_the_right_methods():
    """Enumerated from the router, not spot-checked from the source text."""
    from fastapi import FastAPI

    from webapp.review_routes import install_review

    app = install_review(FastAPI())
    routes = {path: methods for path, methods in _routes(app).items()
              if path.startswith('/api/review')}

    assert routes.get('/api/review') == {'GET'}, routes
    assert routes.get('/api/review/{task_id}') == {'GET'}, routes
    assert routes.get('/api/review/{task_id}/drop') == {'POST'}, routes
    assert routes.get('/api/review/dismiss') == {'POST'}, routes
    assert routes.get('/api/review/export') == {'POST'}, routes
    # Nothing destructive is reachable with a GET.
    for path, methods in routes.items():
        if path.endswith('/drop'):
            assert 'GET' not in methods, f'{path} answers a GET'


# --- record_belongs_to_task ---------------------------------------------

def test_an_exact_id_belongs():
    assert review_data.record_belongs_to_task('task_0007', 'task_0007')


def test_a_duplication_row_belongs():
    """`<task_id>_api…` rows are the same conversation written per API."""
    assert review_data.record_belongs_to_task('task_0007_api0', 'task_0007')


def test_a_longer_id_does_not_belong():
    """A task id comes from a file name, so `abc` must not claim `abcdef`."""
    assert not review_data.record_belongs_to_task('abcdef', 'abc')
    assert not review_data.record_belongs_to_task('task_00070', 'task_0007')


def test_an_empty_id_belongs_to_nothing():
    assert not review_data.record_belongs_to_task('', 'task_0007')
    assert not review_data.record_belongs_to_task('task_0007', '')
    assert not review_data.record_belongs_to_task(None, 'task_0007')


# --- a drop is scoped and lossless --------------------------------------

def test_a_drop_removes_only_that_task():
    with sandbox() as state:
        path = _write_jsonl(state.OUTPUT_DIR, 'output.jsonl', [
            {'id': 'task_a', 'conversations': []},
            {'id': 'task_ab', 'conversations': []},
            {'id': 'task_a_api0', 'conversations': []},
            {'id': 'task_b', 'conversations': []},
        ])
        result = review_data.drop('task_a')

        remaining = [json.loads(line)['id'] for line in _read_lines(path)]
        assert remaining == ['task_ab', 'task_b'], remaining
        assert result['rows_removed'] == 2, result
        assert result['files_changed'] == 1, result


def test_a_drop_never_discards_a_line_it_cannot_parse():
    """An unparseable line is data loss we cannot attribute to this task."""
    with sandbox() as state:
        path = os.path.join(state.OUTPUT_DIR, 'output.jsonl')
        with open(path, 'w', encoding='utf-8') as handle:
            handle.write(json.dumps({'id': 'task_a'}) + '\n')
            handle.write('this is not json\n')
            handle.write(json.dumps({'id': 'task_b'}) + '\n')

        review_data.drop('task_a')

        lines = _read_lines(path)
        assert 'this is not json\n' in lines, lines
        assert len(lines) == 2, lines


def test_a_drop_leaves_no_temporary_file_behind():
    with sandbox() as state:
        _write_jsonl(state.OUTPUT_DIR, 'output.jsonl', [{'id': 'task_a'}])
        review_data.drop('task_a')
        leftovers = [name for name in os.listdir(state.OUTPUT_DIR)
                     if name.endswith('.droptmp')]
        assert not leftovers, leftovers


def test_a_drop_records_the_manual_reason_and_clears_the_score():
    """The dashboard, the Quality tab and a resumed run must all agree it is gone."""
    with sandbox() as state:
        _write_jsonl(state.OUTPUT_DIR, 'output.jsonl', [{'id': 'task_a'}])
        state.quality_scores['task_a'] = {'composite': 22, 'dimensions': {}}
        state.quality_review_ids.add('task_a')

        review_data.drop('task_a')

        assert state.quality_dropped['task_a'] == 'manual', state.quality_dropped
        assert 'task_a' in state.dropped_task_ids
        assert 'task_a' not in state.quality_scores, 'the score survived the drop'
        assert 'task_a' not in state.quality_review_ids, 'the flag survived the drop'


def test_a_drop_of_a_task_with_no_output_row_still_clears_the_flag():
    """A judge-rejected row is never written, so there is nothing to delete."""
    with sandbox() as state:
        state.quality_scores['task_a'] = {'composite': 10, 'dimensions': {}}
        state.quality_review_ids.add('task_a')

        result = review_data.drop('task_a')

        assert result['total_removed'] == 0, result
        assert state.quality_dropped['task_a'] == 'manual'
        assert 'task_a' not in state.quality_review_ids


def test_a_drop_needs_a_task_id():
    with sandbox():
        try:
            review_data.drop('')
        except ValueError:
            return
        raise AssertionError('drop() accepted an empty task id')


# --- dismiss -------------------------------------------------------------

def test_dismiss_clears_the_flags_and_deletes_nothing():
    with sandbox() as state:
        path = _write_jsonl(state.OUTPUT_DIR, 'output.jsonl', [
            {'id': 'task_a'}, {'id': 'task_b'}])
        before = _read_lines(path)
        state.quality_review_ids.update({'task_a', 'task_b'})
        state.quality_scores['task_a'] = {'composite': 22, 'dimensions': {}}

        result = review_data.dismiss_all()

        assert result['dismissed'] == 2, result
        assert not state.quality_review_ids, state.quality_review_ids
        assert _read_lines(path) == before, 'dismiss deleted output data'
        assert 'task_a' in state.quality_scores, 'dismiss cleared a score'


# --- export --------------------------------------------------------------

def test_export_always_rewrites_the_file():
    """A stale export must never look like a successful one."""
    with sandbox() as state:
        _write_jsonl(state.OUTPUT_DIR, 'output.jsonl', [{'id': 'task_a'}])
        state.quality_review_ids.add('task_a')
        state.quality_scores['task_a'] = {'composite': 22, 'dimensions': {}}

        target = os.path.join(state.OUTPUT_DIR, 'flagged.jsonl')
        with open(target, 'w', encoding='utf-8') as handle:
            handle.write('stale content that must be replaced\n')

        result = review_data.export_flagged(target)

        assert result['exported'] == 1, result
        lines = _read_lines(target)
        assert 'stale content' not in ''.join(lines), lines
        assert json.loads(lines[0])['id'] == 'task_a', lines


def test_export_reports_the_ids_it_could_not_find():
    with sandbox() as state:
        _write_jsonl(state.OUTPUT_DIR, 'output.jsonl', [{'id': 'task_a'}])
        state.quality_review_ids.update({'task_a', 'task_missing'})
        state.quality_scores['task_a'] = {'composite': 22, 'dimensions': {}}

        result = review_data.export_flagged()

        assert result['exported'] == 1, result
        assert result['missing'] == ['task_missing'], result


def test_export_carries_the_quality_verdict():
    with sandbox() as state:
        _write_jsonl(state.OUTPUT_DIR, 'output.jsonl', [{'id': 'task_a'}])
        state.quality_review_ids.add('task_a')
        state.quality_scores['task_a'] = {'composite': 22, 'dimensions': {}}

        result = review_data.export_flagged()

        record = json.loads(_read_lines(result['path'])[0])
        assert record['_quality_score']['composite'] == 22, record


def test_export_of_nothing_writes_nothing():
    with sandbox() as state:
        result = review_data.export_flagged()
        assert result['exported'] == 0, result
        assert result['path'] is None, result
        assert not os.listdir(state.OUTPUT_DIR), os.listdir(state.OUTPUT_DIR)


# --- the list and the detail ---------------------------------------------

def test_the_list_sorts_the_worst_row_first():
    with sandbox() as state:
        state.quality_review_ids.update({'task_a', 'task_b'})
        state.quality_scores['task_a'] = {'composite': 60, 'dimensions': {'prose': 60}}
        state.quality_scores['task_b'] = {'composite': 22, 'dimensions': {'prose': 22}}

        rows = review_data.list_flagged()

        assert [row['task_id'] for row in rows] == ['task_b', 'task_a'], rows


def test_the_list_marks_a_row_below_40_as_critical():
    with sandbox() as state:
        state.quality_review_ids.update({'task_a', 'task_b'})
        state.quality_scores['task_a'] = {'composite': 60, 'dimensions': {}}
        state.quality_scores['task_b'] = {'composite': 22, 'dimensions': {}}

        rows = {row['task_id']: row for row in review_data.list_flagged()}

        assert rows['task_b']['severity'] == 'critical', rows['task_b']
        assert rows['task_a']['severity'] == 'warning', rows['task_a']


def test_the_list_filters_by_task_id():
    with sandbox() as state:
        state.quality_review_ids.update({'task_a', 'task_b'})
        state.quality_scores['task_a'] = {'composite': 22, 'dimensions': {}}
        state.quality_scores['task_b'] = {'composite': 30, 'dimensions': {}}

        rows = review_data.list_flagged('task_a')

        assert [row['task_id'] for row in rows] == ['task_a'], rows


def test_the_counts_report_the_two_drop_reasons_apart():
    """v12.8.2: mixing them read as if the judge rejected a row the user removed."""
    with sandbox() as state:
        state.quality_review_ids.update({'task_a'})
        state.quality_dropped['task_a'] = 'manual'
        state.quality_dropped['task_b'] = 'judge_failed'

        counts = review_data.counts()

        assert counts['flagged'] == 1, counts
        assert counts['dropped_by_user'] == 1, counts
        assert counts['dropped_by_judge'] == 1, counts


def test_the_detail_explains_an_empty_transcript():
    """A blank pane is not an explanation; the note says why it is empty."""
    with sandbox() as state:
        state.quality_scores['task_a'] = {'composite': 22, 'dimensions': {}}

        detail = review_data.detail('task_a')

        assert detail['turns'] == [], detail
        assert detail['note'], 'an empty transcript has no note'
        assert 'not in the output files' in detail['note'], detail['note']


def test_the_detail_returns_the_transcript_from_the_output_file():
    with sandbox() as state:
        _write_jsonl(state.OUTPUT_DIR, 'output.jsonl', [{
            'id': 'task_a',
            'conversations': [
                {'from': 'human', 'value': 'hello'},
                {'from': 'gpt', 'value': 'hi there'},
            ],
        }])
        state.quality_scores['task_a'] = {'composite': 22, 'dimensions': {}}

        detail = review_data.detail('task_a')

        assert detail['turns'] == [
            {'role': 'USER', 'text': 'hello'},
            {'role': 'ASSISTANT', 'text': 'hi there'},
        ], detail['turns']
        assert detail['note'] is None, detail['note']


def test_the_detail_never_mutates_the_source_record():
    """The database path may share the record, so export copies before annotating."""
    with sandbox() as state:
        _write_jsonl(state.OUTPUT_DIR, 'output.jsonl', [{'id': 'task_a'}])
        state.quality_review_ids.add('task_a')
        state.quality_scores['task_a'] = {'composite': 22, 'dimensions': {}}

        review_data.export_flagged()

        on_disk = json.loads(_read_lines(
            os.path.join(state.OUTPUT_DIR, 'output.jsonl'))[0])
        assert '_quality_score' not in on_disk, on_disk


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
