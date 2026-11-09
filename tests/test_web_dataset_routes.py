# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the dataset routes: download, clear and the backup list.

The point of this file is that **the browser can get the dataset out and can start fresh**,
which it could not do before these routes existed:

* **Download streams the consolidated dataset**, not a directory listing. The rows are spread
  over one file per API slot, so a route that served `output/*.jsonl` directly would hand the
  browser one arbitrary file and call it "the dataset".
* **Download never leaves a copy behind.** It consolidates into a temporary file that is
  deleted once the response has been sent, so a download does not get swept into the next
  export and double the row count.
* **Clear is destructive, so it needs `confirm`.** A stale tab must not be able to wipe a
  dataset, which is why the route refuses without it rather than trusting the browser.
* **Clear is not "stop & clear".** It wipes the dataset without touching a run, so a finished
  dataset can be cleared while the app is idle.

Everything runs against a temporary directory. The real `output/` is never read or written.
"""

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run_config
from fastapi import FastAPI
from fastapi.testclient import TestClient

from webapp.dataset_routes import install_dataset


# --- helpers -----------------------------------------------------------------

class FakeEngine:
    """A stand-in for `Engine` that records the commands it is given."""

    def __init__(self, result=None):
        self.calls = []
        self.result = result or {'ok': True, 'data': {'target': 'both'}}

    def command(self, cmd, args=None):
        self.calls.append((cmd, args))
        return dict(self.result)


def _app(engine=None):
    """A FastAPI app with only the dataset routes, so no auth gate is needed."""
    app = install_dataset(FastAPI())
    app.state.engine = engine
    app.state.engine_factory = lambda: engine
    return app


def _client(engine=None):
    return TestClient(_app(engine), follow_redirects=False)


def _sandbox():
    """Point `run_config.OUTPUT_DIR` at a temporary directory for one test."""
    return tempfile.TemporaryDirectory()


def _write(path, rows):
    """Write `rows` as JSONL lines."""
    with open(path, 'w', encoding='utf-8') as handle:
        for row in rows:
            handle.write(json.dumps(row) + '\n')


# --- the routes exist ------------------------------------------------------

def test_the_four_dataset_routes_are_registered():
    app = _app()
    found = set()
    pending = list(app.routes)
    while pending:
        route = pending.pop()
        original = getattr(route, 'original_router', None)
        if original is not None:
            pending.extend(original.routes)
            continue
        path = getattr(route, 'path', None)
        if path:
            found.add(path)
    for path in ('/api/dataset', '/api/dataset/download',
                 '/api/dataset/clear', '/api/dataset/backups'):
        assert path in found, f'{path} is not registered'


def test_download_is_a_get_and_clear_is_a_post():
    """Download reads, so it is a GET; clear deletes, so it is a POST."""
    app = _app()
    methods = {}
    pending = list(app.routes)
    while pending:
        route = pending.pop()
        original = getattr(route, 'original_router', None)
        if original is not None:
            pending.extend(original.routes)
            continue
        path = getattr(route, 'path', None)
        if path:
            methods.setdefault(path, set()).update(getattr(route, 'methods', None) or set())
    assert 'GET' in methods['/api/dataset/download']
    assert 'POST' in methods['/api/dataset/clear']
    # A destructive action must not be reachable with a GET.
    assert 'GET' not in methods['/api/dataset/clear']


# --- summary --------------------------------------------------------------

def test_the_summary_counts_every_row_across_every_file():
    with _sandbox() as tmp:
        _write(os.path.join(tmp, 'output_1.jsonl'), [{'a': 1}, {'a': 2}])
        _write(os.path.join(tmp, 'output_2.jsonl'), [{'a': 3}])
        original = run_config.OUTPUT_DIR
        run_config.OUTPUT_DIR = tmp
        try:
            body = _client().get('/api/dataset').json()
        finally:
            run_config.OUTPUT_DIR = original
    assert body['ok'] is True
    assert body['total_rows'] == 3
    names = {entry['name'] for entry in body['files']}
    assert names == {'output_1.jsonl', 'output_2.jsonl'}


def test_the_summary_ignores_derived_files():
    """A previous export and a prompt trace are not dataset rows."""
    with _sandbox() as tmp:
        _write(os.path.join(tmp, 'output_1.jsonl'), [{'a': 1}])
        _write(os.path.join(tmp, 'dataset_export_20260101_000000.jsonl'), [{'a': 2}])
        _write(os.path.join(tmp, 'debug_prompt_1.jsonl'), [{'a': 3}])
        original = run_config.OUTPUT_DIR
        run_config.OUTPUT_DIR = tmp
        try:
            body = _client().get('/api/dataset').json()
        finally:
            run_config.OUTPUT_DIR = original
    assert body['total_rows'] == 1
    assert [entry['name'] for entry in body['files']] == ['output_1.jsonl']


def test_the_summary_is_empty_rather_than_an_error_with_no_dataset():
    with _sandbox() as tmp:
        original = run_config.OUTPUT_DIR
        run_config.OUTPUT_DIR = tmp
        try:
            response = _client().get('/api/dataset')
        finally:
            run_config.OUTPUT_DIR = original
    assert response.status_code == 200
    assert response.json()['total_rows'] == 0
    assert response.json()['files'] == []


# --- download -------------------------------------------------------------

def test_download_returns_every_row_as_one_attachment():
    with _sandbox() as tmp:
        _write(os.path.join(tmp, 'output_1.jsonl'), [{'a': 1}, {'a': 2}])
        _write(os.path.join(tmp, 'output_2.jsonl'), [{'a': 3}])
        original = run_config.OUTPUT_DIR
        run_config.OUTPUT_DIR = tmp
        try:
            response = _client().get('/api/dataset/download')
        finally:
            run_config.OUTPUT_DIR = original
    assert response.status_code == 200
    assert response.headers['content-type'].startswith('application/x-ndjson')
    assert 'attachment' in response.headers['content-disposition']
    assert response.headers['x-row-count'] == '3'
    lines = [line for line in response.text.splitlines() if line.strip()]
    assert len(lines) == 3


def test_download_leaves_no_copy_behind():
    """The temporary file is deleted, so a download cannot double the next export."""
    with _sandbox() as tmp:
        _write(os.path.join(tmp, 'output_1.jsonl'), [{'a': 1}])
        original = run_config.OUTPUT_DIR
        run_config.OUTPUT_DIR = tmp
        try:
            before = set(os.listdir(tmp))
            _client().get('/api/dataset/download')
            after = set(os.listdir(tmp))
        finally:
            run_config.OUTPUT_DIR = original
    assert before == after, f'a download left a file behind: {after - before}'


def test_download_with_no_dataset_is_a_404_not_a_500():
    with _sandbox() as tmp:
        original = run_config.OUTPUT_DIR
        run_config.OUTPUT_DIR = tmp
        try:
            response = _client().get('/api/dataset/download')
        finally:
            run_config.OUTPUT_DIR = original
    assert response.status_code == 404
    assert response.json()['ok'] is False


# --- clear ----------------------------------------------------------------

def test_clear_refuses_without_confirm():
    """A stale tab must not be able to wipe a dataset."""
    engine = FakeEngine()
    response = _client(engine).post('/api/dataset/clear', json={})
    assert response.status_code == 400
    assert response.json()['error']['code'] == 'refused'
    assert engine.calls == [], 'clear reached the engine without confirm'


def test_clear_refuses_a_false_confirm():
    engine = FakeEngine()
    response = _client(engine).post('/api/dataset/clear', json={'confirm': False})
    assert response.status_code == 400
    assert engine.calls == []


def test_clear_passes_confirm_and_target_to_the_engine():
    engine = FakeEngine()
    response = _client(engine).post('/api/dataset/clear',
                                   json={'confirm': True, 'target': 'jsonl'})
    assert response.status_code == 200
    assert engine.calls == [('clear', {'confirm': True, 'target': 'jsonl',
                                      'backup': True})]


def test_clear_defaults_to_both_and_a_backup():
    engine = FakeEngine()
    _client(engine).post('/api/dataset/clear', json={'confirm': True})
    assert engine.calls[0][1]['target'] == 'both'
    assert engine.calls[0][1]['backup'] is True


def test_clear_can_skip_the_backup():
    engine = FakeEngine()
    _client(engine).post('/api/dataset/clear',
                         json={'confirm': True, 'backup': False})
    assert engine.calls[0][1]['backup'] is False


def test_clear_rejects_an_unknown_target():
    engine = FakeEngine()
    response = _client(engine).post('/api/dataset/clear',
                                   json={'confirm': True, 'target': 'everything'})
    assert response.status_code == 400
    assert engine.calls == []


def test_clear_rejects_a_non_json_body():
    response = _client(FakeEngine()).post(
        '/api/dataset/clear', content=b'not json',
        headers={'Content-Type': 'application/json'})
    assert response.status_code == 400


def test_clear_reports_an_engine_refusal_as_403():
    engine = FakeEngine({'ok': False, 'error': {'code': 'refused',
                                               'message': 'nope'}})
    response = _client(engine).post('/api/dataset/clear', json={'confirm': True})
    assert response.status_code == 403
    assert response.json()['ok'] is False


# --- backups --------------------------------------------------------------

def test_backups_are_listed_newest_first():
    with _sandbox() as tmp:
        for name, when in (('preclear_backup_1.zip', 1000),
                           ('preclear_backup_2.zip', 2000)):
            path = os.path.join(tmp, name)
            with open(path, 'wb') as handle:
                handle.write(b'zip')
            os.utime(path, (when, when))
        original = run_config.OUTPUT_DIR
        run_config.OUTPUT_DIR = tmp
        try:
            body = _client().get('/api/dataset/backups').json()
        finally:
            run_config.OUTPUT_DIR = original
    names = [entry['name'] for entry in body['backups']]
    assert names == ['preclear_backup_2.zip', 'preclear_backup_1.zip']


def test_backups_ignores_a_non_zip_and_a_non_backup():
    with _sandbox() as tmp:
        for name in ('preclear_backup_1.zip', 'preclear_backup_2.txt', 'notes.zip'):
            with open(os.path.join(tmp, name), 'wb') as handle:
                handle.write(b'x')
        original = run_config.OUTPUT_DIR
        run_config.OUTPUT_DIR = tmp
        try:
            body = _client().get('/api/dataset/backups').json()
        finally:
            run_config.OUTPUT_DIR = original
    assert [entry['name'] for entry in body['backups']] == ['preclear_backup_1.zip']


def test_backups_with_no_directory_is_empty_not_an_error():
    original = run_config.OUTPUT_DIR
    run_config.OUTPUT_DIR = os.path.join(tempfile.gettempdir(), 'nope-not-here-12345')
    try:
        response = _client().get('/api/dataset/backups')
    finally:
        run_config.OUTPUT_DIR = original
    assert response.status_code == 200
    assert response.json()['backups'] == []


if __name__ == '__main__':
    import traceback
    failures = 0
    for name, function in sorted(globals().items()):
        if not name.startswith('test_') or not callable(function):
            continue
        try:
            function()
            print(f'PASS {name}')
        except Exception:
            failures += 1
            print(f'FAIL {name}')
            traceback.print_exc()
    print(f'\n{failures} failure(s)')
    sys.exit(1 if failures else 0)
