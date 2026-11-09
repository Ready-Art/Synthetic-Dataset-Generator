# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""The dataset's own routes: download it, and clear it.

Four routes, all behind the session gate:

``GET  /api/dataset``           what is in ``output/`` right now: the files, their row
                                counts and the total
``GET  /api/dataset/download``   stream the dataset back as one JSONL attachment
``POST /api/dataset/clear``      delete stored conversations (destructive)
``GET  /api/dataset/backups``    the pre-clear backup zips, newest first

**Download is a GET because it reads.** It never mutates a row, so a link, a refresh or a
prefetch is harmless. Clear is a POST that requires an explicit ``confirm`` in the body, for the
same reason ``control.cmd_clear`` requires one: it deletes data, and a stale tab must not be able
to trigger it.

**Clear is not "stop & clear".** ``stop`` with ``clear=true`` stops the run *and* wipes the
dataset; this route only wipes, so a finished dataset can be cleared without touching a run that is
already idle. The two share ``control.cmd_clear`` underneath, so the backup zip and the target
choices behave identically either way.

Why download does not just serve ``output/*.jsonl`` directly
------------------------------------------------------------
The rows are spread over one file per API slot, plus the prompt traces and any previous export.
Serving a directory listing would hand the browser a file the operator did not ask for, and would
let a path traversal reach outside ``output/``. So the route consolidates through
``run_config.consolidate_jsonl`` into a temporary file and streams *that*, which is the same
function ``cli export`` uses. One code path, one row count, and no path is ever taken from the
request.
"""

import json
import os
import tempfile
import time

import run_config
from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.background import BackgroundTask

from .app import _status_for, error_response


def install_dataset(app):
    """Attach the dataset routes to ``app``. Called from ``create_app``."""
    router = APIRouter(prefix='/api/dataset')

    @router.get('')
    def summary():
        """What is on disk right now: the files, their row counts and the total.

        A row count is read by streaming the file rather than loading it, because a finished
        dataset can be larger than memory and the Overview only needs a number.
        """
        try:
            files = run_config.jsonl_output_files()
        except OSError as exc:
            return error_response('failed', f'Could not read output: {exc}', status=500)

        entries = []
        total = 0
        for path in files:
            rows = _count_rows(path)
            total += rows
            entries.append({
                'name': os.path.basename(path),
                'rows': rows,
                'bytes': _safe_size(path),
            })
        return {
            'ok': True,
            'files': entries,
            'total_rows': total,
            'output_dir': run_config.OUTPUT_DIR,
        }

    @router.get('/download')
    def download():
        """Stream the whole dataset back as one JSONL attachment.

        The consolidation goes to a temporary file that is deleted once the response has been
        sent, so a download does not leave a copy of the dataset behind in ``output/`` and does
        not get swept into the next ``export``.
        """
        try:
            handle, temp_path = tempfile.mkstemp(suffix='.jsonl', prefix='download_')
            os.close(handle)
        except OSError as exc:
            return error_response('failed', f'Could not prepare the download: {exc}',
                                  status=500)

        try:
            ok, message, rows = run_config.consolidate_jsonl(temp_path)
        except Exception as exc:  # noqa: BLE001 - a route must answer, not crash
            _remove(temp_path)
            return error_response('failed', f'Could not build the dataset: {exc}',
                                  status=500)

        if not ok:
            _remove(temp_path)
            # "No output JSONL files found" is a 404, not a 500: nothing is broken,
            # there is simply no dataset yet. The message says so in the words a reader
            # needs, because this response is also what a direct visit to the URL shows
            # -- the Overview button preflights `GET /api/dataset` first, but a bookmark
            # or a hand-typed URL still lands here.
            return error_response(
                'empty',
                f'{message} There is no dataset to download yet -- start a run, then '
                'use Download dataset on the Overview tab.',
                status=404)

        filename = _download_name()
        return FileResponse(
            temp_path,
            media_type='application/x-ndjson',
            filename=filename,
            background=BackgroundTask(_remove, temp_path),
            headers={'X-Row-Count': str(rows)},
        )

    @router.post('/clear')
    async def clear(request: Request):
        """Delete stored conversations. Destructive, so it needs ``confirm``.

        ``target`` is ``db``, ``jsonl``, ``both`` (the default) or ``state``, and matches
        ``control.cmd_clear`` exactly -- the browser sends the same words the CLI's ``--target``
        accepts. A backup zip is taken first unless ``backup`` is false.
        """
        try:
            body = await request.json()
        except (ValueError, json.JSONDecodeError):
            return error_response('bad_request', 'The body must be JSON.', status=400)
        if not isinstance(body, dict):
            return error_response('bad_request', 'The body must be a JSON object.',
                                  status=400)
        if body.get('confirm') is not True:
            return error_response(
                'refused',
                'clear is destructive and needs confirm=true.',
                status=400)

        target = body.get('target', 'both')
        if target not in ('db', 'jsonl', 'both', 'state'):
            return error_response(
                'bad_request',
                'target must be one of db, jsonl, both, state.',
                status=400)

        try:
            engine = _engine(app)
        except Exception as exc:  # noqa: BLE001 - report, do not crash
            return error_response('failed', str(exc), status=500)

        try:
            result = engine.command('clear', {
                'confirm': True,
                'target': target,
                'backup': body.get('backup', True) is not False,
            })
        except Exception as exc:  # noqa: BLE001 - a route must answer, not crash
            return error_response('failed', str(exc), status=500)

        result.pop('id', None)
        status = 200 if result.get('ok') else _status_for(result)
        return JSONResponse(result, status_code=status)

    @router.get('/backups')
    def backups():
        """The pre-clear backup zips in ``output/``, newest first."""
        try:
            found = _backup_files()
        except OSError as exc:
            return error_response('failed', f'Could not read output: {exc}', status=500)
        return {'ok': True, 'backups': found}

    app.include_router(router)
    return app


# --- helpers -----------------------------------------------------------------

def _engine(app):
    """The started engine, through ``app``'s own accessor.

    Imported lazily so this module does not import ``webapp.app`` at module scope, which
    would be a cycle: ``app`` imports this module to install the routes.
    """
    from .app import _get_engine
    return _get_engine(app)


def _count_rows(path):
    """Count the non-blank lines in a JSONL file without loading it."""
    rows = 0
    with open(path, 'r', encoding='utf-8', errors='replace') as handle:
        for line in handle:
            if line.strip():
                rows += 1
    return rows


def _safe_size(path):
    """The file's size in bytes, or 0 if it vanished between listing and reading."""
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _download_name():
    """A filename that says what it is and when it was made."""
    return f"dataset_{time.strftime('%Y%m%d_%H%M%S')}.jsonl"


def _backup_files():
    """Every pre-clear backup zip in ``output/``, newest first."""
    directory = run_config.OUTPUT_DIR
    if not os.path.isdir(directory):
        return []
    found = []
    for name in os.listdir(directory):
        if not name.startswith('preclear_backup') or not name.endswith('.zip'):
            continue
        path = os.path.join(directory, name)
        try:
            stat = os.stat(path)
        except OSError:
            continue
        found.append({
            'name': name,
            'bytes': stat.st_size,
            'modified': stat.st_mtime,
        })
    found.sort(key=lambda entry: entry['modified'], reverse=True)
    return found


def _remove(path):
    """Delete a temporary file, ignoring a failure: it is a temp file either way."""
    try:
        os.remove(path)
    except OSError:
        pass



