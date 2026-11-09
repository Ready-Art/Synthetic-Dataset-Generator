# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""The Review tab's routes.

Four routes, all behind the session gate:

``GET  /api/review``            the flagged list plus the count line
``GET  /api/review/{task_id}``   one conversation's score breakdown and transcript
``POST /api/review/{task_id}/drop``  remove that ONE conversation from the dataset
``POST /api/review/dismiss``     clear every flag (no data is deleted)
``POST /api/review/export``      write the flagged rows to a JSONL file on the server
``GET  /api/review/export/download``  export and stream it to the browser as an attachment

**Drop is a POST with the task id in the path**, not a query parameter and not a body
field, because it is destructive: a link, a prefetch or a stale tab cannot trigger it, and the
id it acts on is the one the reader selected. The browser asks for confirmation first; the route
does not rely on that.

**Dismiss and Export never touch the dataset.** Only Drop removes a row, which is why they are
separate routes rather than one "action" endpoint with a mode flag -- a mode flag is one typo away
from deleting data.
"""

import review_data
from fastapi import APIRouter
from fastapi.responses import Response

from .app import error_response


def install_review(app):
    """Attach the Review routes to ``app``. Called from ``create_app``."""
    router = APIRouter(prefix='/api/review')

    @router.get('')
    def list_flagged(filter: str = ''):
        return {
            'ok': True,
            'rows': review_data.list_flagged(filter),
            'counts': review_data.counts(),
        }

    @router.get('/{task_id}')
    def detail(task_id: str):
        return {'ok': True, 'detail': review_data.detail(task_id)}

    @router.post('/{task_id}/drop')
    def drop(task_id: str):
        try:
            result = review_data.drop(task_id)
        except ValueError as exc:
            return error_response('bad_request', str(exc), status=400)
        except OSError as exc:
            return error_response('failed', f'Could not drop: {exc}', status=500)
        return {'ok': True, 'data': result}

    @router.post('/dismiss')
    def dismiss():
        return {'ok': True, 'data': review_data.dismiss_all()}

    @router.post('/export')
    def export():
        """Write the flagged rows to a file on the server. Kept for the CLI and for a script.

        The browser no longer uses this: a POST that answers JSON cannot start a download, which
        is why the button appeared to do nothing but toast a server path (v15.1.0). See
        :func:`export_download`.
        """
        try:
            result = review_data.export_flagged()
        except OSError as exc:
            return error_response('failed', f'Could not export: {exc}', status=500)
        return {'ok': True, 'data': result}

    @router.get('/export/download')
    def export_download():
        """Export the flagged rows and stream them back as a JSONL attachment.

        **This is the route the Export button uses.** A `POST` that writes a file on the server
        and returns its path is not a download: the browser has no reason to save anything, so the
        old button only produced a toast naming a path the reader could not reach from a browser on
        another machine. Answering with `Content-Disposition: attachment` hands the file to the
        browser's own download handling, which is what "Export" means to a reader.

        The file is still written to the output directory first -- `export_flagged()` is the one
        implementation, and `cli`/a script may want the on-disk copy -- and the same bytes are then
        read back and returned. Reading it back rather than building the body twice means the
        download and the on-disk export can never disagree.

        A GET, deliberately: it changes no dataset row and deletes nothing, and a download that a
        browser can prefetch is not a hazard here. The rows are the flagged conversations, which the
        reader is already looking at.
        """
        try:
            result = review_data.export_flagged()
        except OSError as exc:
            return error_response('failed', f'Could not export: {exc}', status=500)

        path = result.get('path')
        if not result.get('exported') or not path:
            # An empty export is not a file. Answering with a zero-byte attachment would look like
            # a successful download of nothing, so say what happened instead.
            return error_response(
                'empty',
                result.get('message') or 'No flagged conversations to export.',
                status=404)

        try:
            with open(path, 'rb') as handle:
                body = handle.read()
        except OSError as exc:
            return error_response('failed', f'Could not read {path}: {exc}', status=500)

        return Response(
            content=body,
            media_type='application/x-ndjson',
            headers={
                'Content-Disposition': 'attachment; filename="quality_review_flagged.jsonl"',
                # The count is useful to a script and harmless to a browser.
                'X-Export-Rows': str(result.get('exported', 0)),
                'Cache-Control': 'no-store',
            },
        )

    app.include_router(router)
    return app
