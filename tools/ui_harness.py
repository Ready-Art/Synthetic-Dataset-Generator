"""A throwaway UI harness: serve the real static bundle against a seeded payload.

Not part of the app. It exists so a layout bug can be *seen* in a real browser instead of
argued about from the CSS. It mounts `webapp/static` and answers the handful of JSON routes
the screens read, with data shaped exactly like the engine's, and with no auth gate.

    python tools/ui_harness.py --port 8799

Then screenshot `http://127.0.0.1:8799/` with chromium headless.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from webapp import theme

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          'webapp', 'static')


def _stats():
    """A payload shaped like `control.build_stats_payload`, with data in every panel."""
    def issue(time, phrase, context, api):
        return {'time': time, 'phrase': phrase, 'context': context, 'api': api}

    def counter(total, recent):
        return {'total': total, 'per_api': {'0': total}, 'recent': recent,
                'recent_per_api': {'0': recent}, 'label': 'x'}

    return {
        'ok': True,
        'generated_at': '2026-11-09 16:00:00',
        'generator': {'version': '15.0.0', 'pid': 1234, 'uptime_s': 91.2,
                      'phase': 'running', 'config_path': 'config/config.yml',
                      'protocol': 1},
        'run': {
            'active': True, 'generating': True, 'paused': False, 'stopping': False,
            'threads': 4, 'alive_threads': 4, 'duplication_mode': False,
            'tasks': {'pending': 12, 'completed': 40, 'failed': 2},
            'progress': {'processed': 40, 'total': 54, 'percent': 74.07, 'queued': 12},
            'run_started_at': '16:00:00', 'run_elapsed_s': 91.2,
        },
        'output': {'sink': 'jsonl', 'jsonl_files': [{'name': 'api_1.jsonl', 'rows': 40}],
                   'jsonl_rows': 40,
                   'database': {'enabled': False, 'connected': False, 'rows': None}},
        'tokens': {'input': 120000, 'output': 480000, 'estimated_cost': 0.42},
        'attempts': {'total': 54, 'per_api': {'0': 54}},
        'detected': {
            # The engine stores `recent` as (phrase, context, api_slot) TUPLES, which JSON
            # turns into three-element arrays. The harness reproduces that exactly, because
            # that shape is the bug.
            'refusals': counter(3, [issue('16:00:03', 'I cannot help with that', 'Sure, here is how you do it', 0),
                                    issue('16:00:19', 'As an AI I cannot', 'The character said nothing', 1),
                                    issue('16:00:41', 'I am unable to', 'She turned away slowly', 0)]),
            'out_of_character': counter(2, [issue('16:00:07', 'OOC:', 'The narrator broke in', 0),
                                           issue('16:00:23', 'as an AI', 'He nodded', 1)]),
            'slop': counter(4, [issue('16:00:02', 'a testament to', 'It is a testament to her skill', 0),
                                issue('16:00:11', 'barely above a whisper', 'She said barely above a whisper', 1),
                                issue('16:00:29', 'a mix of', 'A mix of fear and hope', 0),
                                issue('16:00:47', 'orbs', 'Her eyes were orbs', 1)]),
            'logic_issues': counter(1, [issue('16:00:33', 'logic error', 'He stood up twice', 0)]),
            'regex_watch': counter(2, [issue('16:00:01', '\\bvery\\b', 'It was very good', 0),
                                       issue('16:00:13', '\\bsuddenly\\b', 'Suddenly she ran', 1)]),
            'errors': counter(1, [issue('16:00:53', 'timeout after 30s', '', 0)]),
            'user_speaking': counter(2, [issue('16:00:17', 'I said', 'I said nothing', 0),
                                         issue('16:00:37', 'she said', 'she said quietly', 1)]),
            'anti_slop': counter(0, []),
            'non_latin': counter(1, [issue('16:00:43', 'Cyrillic', 'Привет', 0)]),
            'scene_continuity': counter(1, [issue('16:00:49', 'cast drift', 'A new name appeared', 0)]),
        },
        'filtered': {'total': 2, 'by_reason': {'judge': 2}, 'task_ids': {},
                     'review_queue': 2, 'write_failures': 0, 'write_failure_ids': []},
        'quality': {'enabled': True, 'scored': 40, 'avg_composite': 68.4,
                    'min_composite': 31.0, 'max_composite': 94.0,
                    'dimension_averages': {'prose': 70.1, 'logicitz': 66.0},
                    'flag_counts': {}, 'threshold_failures': 2, 'threshold': 50},
        'detection_config': {'slop_enabled': True, 'regex_watch_enabled': True},
        'regex_watch_log': [
            {'time': '16:00:01', 'pattern': '\\bvery\\b', 'context': 'It was very good'},
            {'time': '16:00:11', 'pattern': '\\bsuddenly\\b', 'context': 'Suddenly she ran'},
            {'time': '16:00:21', 'pattern': '\\bvery\\b', 'context': 'Very slowly'},
            {'time': '16:00:31', 'pattern': '\\bvery\\b', 'context': 'A very long day'},
            {'time': '16:00:41', 'pattern': '\\bvery\\b', 'context': 'Very much so'},
        ],
        'cast_events_log': [],
    }


def _metrics():
    """Per-slot usage plus a throughput ring, the shape `/api/metrics` returns."""
    history = [{'time': 1762700000 + i * 2, 'tokens_per_second': 40 + i * 3}
               for i in range(20)]
    return {
        'ok': True,
        'slots': [
            {'slot': 0, 'label': 'API 1',
             'usage': {'prompt_tokens': 1200, 'completion_tokens': 4800,
                       'total_tokens': 6000, 'tokens_per_second': 97.0,
                       'requests': 40, 'seconds': 61.8},
             'history': history},
            {'slot': 1, 'label': 'API 2',
             'usage': {'prompt_tokens': 900, 'completion_tokens': 3100,
                       'total_tokens': 4000, 'tokens_per_second': 64.5,
                       'requests': 14, 'seconds': 62.0},
             'history': [{'time': p['time'], 'tokens_per_second': p['tokens_per_second'] * 0.6}
                         for p in history]},
        ],
        'antislop_columns': False,
    }


def _review():
    return {
        'ok': True,
        'counts': {'flagged': 1, 'dropped_by_judge': 0, 'dropped_by_user': 0},
        'rows': [{'task_id': 'task-0001', 'display_id': 'task-0001', 'composite': 31.0,
                  'severity': 'critical', 'lowest_dim': 'logic', 'lowest_value': 22.0,
                  'flags': ['contradiction']}],
        'detail': {
            'task_id': 'task-0001',
            'note': 'Scored 31.0, below the threshold of 50.',
            'turns': [
                {'role': 'user', 'text': 'She walked into the room and sat down.'},
                {'role': 'assistant', 'text': 'She sat down, then stood up and sat down again. '
                                             'The contradiction is the point of this fixture: '
                                             'it is long enough to overflow a short pane.'},
                {'role': 'user', 'text': 'What did she do next?'},
                {'role': 'assistant', 'text': 'She left.'},
            ],
        },
    }


def build():
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @app.get('/static/theme.css')
    def css():
        return PlainTextResponse(theme.STYLESHEET, media_type='text/css')

    app.mount('/static', StaticFiles(directory=STATIC_DIR), name='static')

    @app.get('/', response_class=HTMLResponse)
    def index():
        with open(os.path.join(STATIC_DIR, 'index.html'), encoding='utf-8') as handle:
            return HTMLResponse(handle.read())

    @app.get('/api/stats')
    def stats():
        return _stats()

    @app.get('/api/metrics')
    def metrics():
        return _metrics()

    @app.get('/api/review')
    def review():
        return _review()

    @app.get('/api/dataset')
    def dataset():
        # Rows present, so the Download button takes the success path. Set
        # HARNESS_EMPTY=1 to exercise the "nothing to download" branch instead.
        rows = 0 if os.environ.get('HARNESS_EMPTY') else 40
        return {'ok': True, 'files': [], 'total_rows': rows, 'output_dir': 'output'}

    @app.get('/api/dataset/download')
    def download():
        from fastapi.responses import JSONResponse
        return JSONResponse({'ok': False, 'error': {'code': 'empty',
                                                   'message': 'No output JSONL files found.'}},
                            status_code=404)

    def stub():
        return {'ok': True}

    for path in ('/api/apis', '/api/quality', '/api/regex', '/api/prompts', '/api/queue',
                 '/api/config', '/api/datasets', '/api/profiles'):
        app.add_api_route(path, stub, methods=['GET'])

    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8799)
    args = parser.parse_args()
    import uvicorn
    uvicorn.run(build(), host='127.0.0.1', port=args.port, log_level='warning')
