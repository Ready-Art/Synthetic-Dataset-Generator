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
``POST /api/review/export``      write the flagged rows to a JSONL file

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
from fastapi.responses import JSONResponse

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
        try:
            result = review_data.export_flagged()
        except OSError as exc:
            return error_response('failed', f'Could not export: {exc}', status=500)
        return {'ok': True, 'data': result}

    app.include_router(router)
    return app
