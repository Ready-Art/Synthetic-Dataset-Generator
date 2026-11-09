# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

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
    """Per-slot usage, the server rows and the proxy's figures -- the `/api/metrics` shape.

    Set ``HARNESS_PLAIN=1`` to serve a plain-vLLM endpoint with no `antislop:` metrics, which is
    the case the antislop toggle has to explain rather than draw empty axes for.
    """
    history = [{'time': 1762700000 + i * 2, 'tokens_per_second': 40 + i * 3}
               for i in range(20)]
    plain = bool(os.environ.get('HARNESS_PLAIN'))

    def server_history(scale):
        return [{'time': 1762700000 + i * 2,
                 'prompt_tps': (180 + i * 4) * scale,
                 'generation_tps': (52 + i * 2) * scale,
                 'load_percent': min(96, 22 + i * 3),
                 'running': 3 + (i % 4),
                 'waiting': i % 3,
                 'antislop_requests': None if plain else 100 + i * 7,
                 'antislop_ban_rate': None if plain else (i % 4) * 0.4,
                 'antislop_backtrack_rate': None if plain else (i % 3) * 0.2,
                 'antislop_generation_tps': None if plain else 48 + i * 1.5}
                for i in range(20)]

    endpoints = [
        {'slot_idx': 0, 'label': 'API 1', 'model': 'qwen2.5-32b-instruct',
         'metrics_url': 'http://127.0.0.1:8000/metrics', 'error': None,
         'is_antislop': not plain, 'prompt_tps': 254.0, 'generation_tps': 90.0,
         'load_percent': 79.0, 'running': 6, 'waiting': 2,
         'history': server_history(1.0), 'slot_history': history},
        {'slot_idx': 1, 'label': 'API 2', 'model': 'qwen2.5-32b-instruct',
         'metrics_url': 'http://127.0.0.1:8001/metrics', 'error': None,
         'is_antislop': not plain, 'prompt_tps': 141.0, 'generation_tps': 55.0,
         'load_percent': 44.0, 'running': 3, 'waiting': 0,
         'history': server_history(0.6), 'slot_history': history},
    ]

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
        'endpoints': endpoints,
        'vllm_available': True,
        'antislop_columns': not plain,
        'antislop_available': not plain,
    }


def _live_prompts():
    """The Live Prompt Viewer's ring, newest first."""
    entries = []
    for index in range(6):
        entries.append({
            'seq': 6 - index,
            'time': f'16:00:{10 + index:02d}',
            'recorded_at': 1762700000 + index,
            'thread_id': index % 4,
            'api_slot': index % 2,
            'message_type': ['Question', 'Answer', 'Slop fix'][index % 3],
            'attempt': 1,
            'chars': 1840 + index * 90,
            'approx_tokens': (1840 + index * 90) // 4,
            'truncated': False,
            'turns': [
                {'role': 'system',
                 'content': 'You are a roleplay partner. Stay in character, never speak for '
                            'the user, and never mention that you are an AI.'},
                {'role': 'user',
                 'content': f'Write the next reply in the scene. Turn {index + 1}.\n\n'
                            'The rain had not stopped for three days, and the inn was full.'},
            ],
        })
    return {'ok': True, 'entries': entries, 'count': len(entries),
            'capacity': 40, 'hooked': True}


def _queue():
    """The three queue tables. Pending is deliberately long: that is the layout case."""
    def rows(prefix, count):
        return [{'task_id': f'{prefix}-{i:04d}',
                 'file': f'source_{i % 7}.jsonl',
                 'retries': i % 3} for i in range(count)]
    return {
        'ok': True,
        'tables': {'pending': rows('task', 240),
                   'completed': rows('done', 12),
                   'failed': rows('fail', 4)},
        'counts': {'pending': 240, 'completed': 12, 'failed': 4},
        'purged': 0,
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

    @app.get('/api/live-prompts')
    def live_prompts():
        return _live_prompts()

    @app.get('/api/queue')
    def queue():
        return _queue()

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

    for path in ('/api/apis', '/api/quality', '/api/regex', '/api/prompts',
                 '/api/config', '/api/datasets', '/api/profiles'):
        app.add_api_route(path, stub, methods=['GET'])

    return app


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8799)
    args = parser.parse_args()
    import uvicorn
    uvicorn.run(build(), host='127.0.0.1', port=args.port, log_level='warning')
