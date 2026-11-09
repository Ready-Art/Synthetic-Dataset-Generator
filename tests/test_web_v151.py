# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the v15.1.0 web-UI round.

Nine defects and two features, one section each. Every check is written against the failure mode
rather than the fix, because the failure modes are what a future change will reintroduce:

1. **The Metrics tab lost the server and proxy figures.** The tab only ever rendered
   `slot_usage_rows()`, so the vLLM endpoint table and the antislop proxy panels -- which the Tk tab
   had -- were gone. Two independent toggles now switch them on, and the route carries the rows.
2. **The y-axis labels were squeezed together.** The chart drew into a hard-coded 640-wide viewBox with
   `preserveAspectRatio="none"`, so a narrower panel squashed every glyph horizontally.
3. **The Live Prompt Viewer was gone.** `app_state.live_prompt_preview_hook` was only ever set by
   `generate_tk.py`, so headless it stayed `None` and nothing was captured.
4. **Recent issues scrolled back to the top on every stats tick.** The Overview re-render replaced the
   panel and threw its scroll offset away.
5. **Estimated cost read 0.0000 forever.** It read `app_state.estimated_cost`, which no run writes.
6. **The queue could not be cleared with no run.** Stop & Clear needs a live run; Purge only touches
   pending.
7. **Export flagged JSONL saved to the server and never reached the browser.**
8. **The Queue tab put pending first**, so a list of thousands squashed the two short tables.
9. **The UI was plain.** A polish pass, asserted as "the tokens are used", not "it looks nice".

No display, no browser, no network: the JS modules are read as text and the CSS is parsed out of
``theme.py``, exactly as ``test_web_overview.py`` does.
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import webapp.theme as theme

STATIC_JS = os.path.join(os.path.dirname(os.path.abspath(theme.__file__)), 'static', 'js')
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(name):
    with open(os.path.join(STATIC_JS, name), 'r', encoding='utf-8') as handle:
        return handle.read()


def _read_root(relative):
    with open(os.path.join(ROOT, relative), 'r', encoding='utf-8') as handle:
        return handle.read()


def _strip_comments(source):
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


# --- 1. the Metrics tab has two independent toggles ------------------------

def test_the_metrics_tab_has_two_separate_toggles():
    """One for the per-slot throughput, one for the server and proxy figures.

    The brief asked for two separate toggles, not one combined switch: a reader may want the
    generator's own numbers without the server's, or the reverse.
    """
    source = _strip_comments(_read('tabs.js'))
    block = source.split('const metricsTab = makeTab(', 1)[1].split('\n});', 1)[0]
    assert block.count('toggle(') == 2, block
    assert 'metricsView.vllm' in block, block
    assert 'metricsView.antislop' in block, block


def test_the_two_metrics_views_are_independent():
    """Flipping one must not flip the other."""
    source = _strip_comments(_read('tabs.js'))
    block = source.split('const metricsView =', 1)[1].split('};', 1)[0]
    assert 'vllm:' in block and 'antislop:' in block, block


def test_the_metrics_tab_renders_the_server_panels_when_switched_on():
    source = _strip_comments(_read('tabs.js'))
    assert 'function vllmServerPanels' in source
    assert 'function antislopPanels' in source
    assert 'function endpointTable' in source
    block = source.split('const metricsTab = makeTab(', 1)[1].split('\n});', 1)[0]
    assert 'vllmServerPanels(' in block, block
    assert 'antislopPanels(' in block, block


def test_the_metrics_route_carries_the_endpoint_rows():
    """The panels have nothing to draw without them."""
    source = _read_root('webapp/tabs_routes.py')
    block = source.split("def metrics():", 1)[1].split('# --- Prompts', 1)[0]
    assert 'endpoint_rows()' in block, block
    assert "'endpoints'" in block, block
    assert "'vllm_available'" in block, block
    assert "'antislop_available'" in block, block


def test_the_metrics_route_never_hands_out_the_pollers_own_lists():
    """A copy, or a tick can mutate the ring the poller is appending to."""
    source = _read_root('webapp/tabs_routes.py')
    block = source.split("def metrics():", 1)[1].split('# --- Prompts', 1)[0]
    assert "entry['history'] = [dict(sample)" in block, block
    assert "entry['slot_history'] = [dict(sample)" in block, block


# --- 2. the axis labels are not squeezed -----------------------------------

def test_the_chart_viewbox_is_measured_not_fixed():
    """`preserveAspectRatio="none"` is what squashed the axis text.

    A hard-coded 640-wide viewBox stretched to a 300px panel scales every glyph by 0.47
    horizontally, so the y-axis numbers run into each other. The viewBox now matches the host's own
    width, so one user unit is one pixel.
    """
    source = _strip_comments(_read('chart.js'))
    assert "preserveAspectRatio: 'xMinYMin meet'" in source, (
        'the chart still stretches its viewBox non-uniformly, so the axis text is squashed')
    assert 'preserveAspectRatio: \'none\'' not in source
    assert 'hostWidth(' in source, 'the chart does not measure its host'


def test_the_chart_gutter_is_sized_to_its_labels():
    """A four-digit token count must not overlap the plot."""
    source = _strip_comments(_read('chart.js'))
    assert 'textWidth(' in source, source
    block = source.split('function textWidth', 1)[1].split('\n}', 1)[0]
    assert 'CHAR_RATIO' in block, block
    assert 'gutter' in source, 'the y-axis gutter is not computed from the labels'


def test_the_axis_ticks_are_whole_numbers():
    """A count axis with `0.5` on it is a broken axis."""
    source = _strip_comments(_read('chart.js'))
    block = source.split('function axisTop', 1)[1].split('\n}', 1)[0]
    assert 'Math.ceil' in block, block
    assert 'Math.max(1' in block, block


def test_the_chart_redraws_on_a_resize():
    """A panel that changes width must not keep a viewBox from its old width."""
    source = _strip_comments(_read('chart.js'))
    assert 'ResizeObserver' in source, source
    block = source.split('function watchResize', 1)[1].split('\n}', 1)[0]
    assert 'disconnect()' in block, 'a detached host is never unobserved'


# --- 3. the Live Prompt Viewer ------------------------------------------

def test_the_live_prompt_ring_caps_itself():
    """An unbounded ring grows by one entry per request for the length of a run."""
    source = _read_root('webapp/live_prompts.py')
    assert 'RING_LENGTH' in source
    block = source.split('def record(', 1)[1].split('\ndef ', 1)[0]
    assert 'overflow' in block, block
    assert 'del _ring[:overflow]' in block, block


def test_the_live_prompt_hook_never_raises_into_the_request_path():
    """It runs between building a payload and sending it."""
    source = _read_root('webapp/live_prompts.py')
    block = source.split('def record(', 1)[1].split('\ndef ', 1)[0]
    assert 'except Exception' in block, block


def test_the_live_prompt_hook_copies_the_callers_messages():
    """`generation.py` reuses its list across attempts, so a stored reference rewrites history."""
    source = _read_root('webapp/live_prompts.py')
    block = source.split('def record(', 1)[1].split('\ndef ', 1)[0]
    assert "'role': role" in block, block
    assert "'content': content" in block, block


def test_the_engine_installs_the_live_prompt_hook():
    """Without this the hook stays `None` and the tab is permanently empty.

    The same class of bug as the metrics poller never being started (v15.0.1): the engine exists,
    the route exists, and nothing connects them.
    """
    source = _read_root('webapp/engine.py')
    assert 'live_prompts.install()' in source, source
    assert 'live_prompts.uninstall()' in source, source


def test_the_live_prompt_hook_is_installed_and_removed_cleanly():
    import app_state
    import webapp.live_prompts as live_prompts

    saved = app_state.live_prompt_preview_hook
    try:
        app_state.live_prompt_preview_hook = None
        live_prompts.uninstall()
        live_prompts.install()
        assert app_state.live_prompt_preview_hook is live_prompts.record
        live_prompts.uninstall()
        assert app_state.live_prompt_preview_hook is None
    finally:
        app_state.live_prompt_preview_hook = saved


def test_uninstall_leaves_someone_elses_hook_alone():
    """A Tk window sharing the process must not have its preview silently disabled."""
    import app_state
    import webapp.live_prompts as live_prompts

    saved = app_state.live_prompt_preview_hook
    theirs = lambda *a, **k: None  # noqa: E731 - a stand-in hook
    try:
        app_state.live_prompt_preview_hook = theirs
        live_prompts.uninstall()
        assert app_state.live_prompt_preview_hook is theirs
    finally:
        app_state.live_prompt_preview_hook = saved


def test_a_recorded_prompt_is_readable_newest_first():
    import webapp.live_prompts as live_prompts

    live_prompts.clear()
    live_prompts.record([{'role': 'system', 'content': 'S'},
                         {'role': 'user', 'content': 'U'}],
                        {'thread_id': 7, 'api_slot_idx': 2, 'message_type': 'Question',
                         'attempt': 1, 'timestamp': '12:00:00'})
    live_prompts.record([{'role': 'user', 'content': 'second'}],
                        {'thread_id': 8, 'api_slot_idx': 1, 'message_type': 'Answer',
                         'attempt': 1, 'timestamp': '12:00:01'})
    entries = live_prompts.snapshot()
    try:
        assert len(entries) == 2, entries
        assert entries[0]['turns'][0]['content'] == 'second', entries
        assert entries[1]['api_slot'] == 2, entries
        assert entries[1]['chars'] == 2, entries
        assert entries[1]['approx_tokens'] == 0, entries
    finally:
        live_prompts.clear()


def test_a_recorded_prompt_is_a_copy():
    import webapp.live_prompts as live_prompts

    live_prompts.clear()
    messages = [{'role': 'user', 'content': 'original'}]
    live_prompts.record(messages, {})
    try:
        messages[0]['content'] = 'mutated'
        entries = live_prompts.snapshot()
        assert entries[0]['turns'][0]['content'] == 'original', entries
    finally:
        live_prompts.clear()


def test_a_huge_prompt_is_truncated_and_says_so():
    import webapp.live_prompts as live_prompts

    live_prompts.clear()
    live_prompts.record([{'role': 'user', 'content': 'x' * (live_prompts.MAX_TEXT_CHARS + 50)}],
                        {})
    try:
        entries = live_prompts.snapshot()
        assert entries[0]['truncated'] is True, entries
        assert entries[0]['chars'] <= live_prompts.MAX_TEXT_CHARS + 50, entries
    finally:
        live_prompts.clear()


# --- 3a. the shadowing bug (v15.1.1) ---------------------------------

def test_no_route_function_shadows_a_module_it_imports():
    """A route function named after a module shadows that module for its whole body.

    `def live_prompts(limit: int = 0)` shadowed the imported `live_prompts` module, so
    `live_prompts.snapshot(...)` called the function itself and raised
    `AttributeError: 'function' object has no attribute 'snapshot'`. FastAPI turned that into a 500
    with a plain-text body, and the browser reported it as a JSON parse error -- a message about
    the parser for a bug that was never about JSON.

    This scans every route module for the whole class, not just the one instance.
    """
    import ast
    import pathlib

    webapp_dir = pathlib.Path(ROOT) / 'webapp'
    modules = {path.stem for path in webapp_dir.glob('*.py')}
    offenders = []
    for path in sorted(webapp_dir.glob('*_routes.py')):
        tree = ast.parse(path.read_text(encoding='utf-8'))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in modules:
                offenders.append(f'{path.name}: def {node.name}()')
    assert not offenders, (
        'a route function shadows a module it imports, so the module is unreachable inside '
        f'its own body: {offenders}')


def test_the_live_prompt_routes_answer_json():
    """The exact symptom: a 500 with a text/plain body, read as a JSON parse error."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from webapp.tabs_routes import install_tabs

    client = TestClient(install_tabs(FastAPI()), raise_server_exceptions=False)
    for method, path in (('get', '/api/live-prompts'),
                        ('post', '/api/live-prompts/clear')):
        response = getattr(client, method)(path)
        assert response.status_code == 200, (path, response.status_code, response.text[:200])
        assert 'json' in response.headers.get('content-type', ''), (
            f'{path} answered {response.headers.get("content-type")}; the browser would '
            'report a JSON parse error instead of the real failure')
        assert response.json()['ok'] is True, response.text[:200]


def test_the_tab_client_never_parses_a_non_json_body():
    """A 500 body is `Internal Server Error`, so `response.json()` throws a SyntaxError."""
    source = _strip_comments(_read('tabs.js'))
    assert 'async function readJSON' in source, source[:400]
    block = source.split('async function readJSON', 1)[1].split('\n}', 1)[0]
    assert "content-type" in block, block
    assert 'includes(\'json\')' in block, block
    # The helper must be what both callers use, or a route can bypass the guard.
    for name in ('getJSON', 'postJSON'):
        caller = source.split(f'async function {name}', 1)[1].split('\n}', 1)[0]
        assert 'await readJSON(response)' in caller, (
            f'{name} still calls response.json() directly, so a 500 surfaces as a JSON error')
        assert 'await response.json()' not in caller, caller


def test_the_live_prompt_tab_polls_while_mounted():
    """A debugging view that only updates on a click is not a live view."""
    source = _strip_comments(_read('tabs.js'))
    block = source.split('const livePromptsTab = makeTab(', 1)[1].split('\n});', 1)[0]
    assert 'interval:' in block, block
    assert 'clearInterval' in source, 'the poll is never stopped when the tab is left'


def test_the_live_prompt_tab_keeps_its_scroll_position():
    """The same complaint the Overview's Recent issues had, in the new tab."""
    source = _strip_comments(_read('tabs.js'))
    assert 'liveScrollTop' in source, source
    block = source.split('const livePromptsTab = makeTab(', 1)[1].split('\n});', 1)[0]
    assert 'liveScrollTop' in block, block


# --- 4. Recent issues keeps its scroll position ---------------------------

def test_the_overview_re_render_preserves_scroll():
    """A tick must not snap the issue panel to the top once a second."""
    source = _strip_comments(_read('overview.js'))
    assert 'captureScroll' in source, source
    assert 'restoreScroll' in source, source
    block = source.split('export function repaintOverview', 1)[1].split('\n}', 1)[0]
    assert 'captureScroll(' in block, block
    assert 'restoreScroll(' in block, block


def test_the_issue_panel_carries_a_stable_scroll_key():
    """The key must be stable across renders, so it cannot be an index."""
    source = _strip_comments(_read('overview.js'))
    block = source.split('function issuePanel', 1)[1].split('\n}', 1)[0]
    assert "data-scroll-key" in block, block
    assert "'overview-issues'" in block, block


def test_the_stats_tick_uses_the_scroll_preserving_repaint():
    """The helper existing is not enough; the tick must call it."""
    source = _strip_comments(_read('main.js'))
    assert 'repaintOverview(state)' in source, source


def test_capture_and_restore_only_touch_keyed_scrollers():
    source = _strip_comments(_read('dom.js'))
    block = source.split('export function captureScroll', 1)[1].split('\n}', 1)[0]
    assert 'data-scroll-key' in block, block
    block = source.split('export function restoreScroll', 1)[1].split('\n}', 1)[0]
    assert 'requestAnimationFrame' in block, (
        'the offset is set before the new content has a height, so it clamps to zero')
    assert 'setTimeout' in block, 'a throttled renderer may not produce a frame'


# --- 5. Estimated cost ---------------------------------------------------

def test_the_estimated_cost_is_computed_from_the_tokens():
    """It read `app_state.estimated_cost`, which no run ever writes."""
    source = _read_root('control.py')
    assert 'def _estimated_cost(' in source, source
    block = source.split('def _estimated_cost(', 1)[1].split('\ndef ', 1)[0]
    assert 'cost_per_1k_tokens' in block, block
    assert 'total_input_tokens' in block, block
    assert 'total_output_tokens' in block, block
    payload = source.split("'tokens': {", 1)[1].split('},', 1)[0]
    assert '_estimated_cost(cfg)' in payload, payload


def test_the_estimated_cost_matches_the_budget_check():
    """Two implementations of a number is two numbers that can disagree."""
    source = _read_root('generation.py')
    block = source.split('def check_budget_limit(', 1)[1].split('\ndef ', 1)[0]
    assert 'cost_per_1k_tokens' in block, block
    assert '/ 1000.0' in block, block


def test_the_estimated_cost_is_zero_without_a_price():
    """The shipped default price is 0, and 0 is the honest answer: nothing to estimate from."""
    import control

    class _Cfg:
        def get(self, path, default=None):
            return {'api.pricing.cost_per_1k_tokens': 0.0}.get(path, default)

    assert control._estimated_cost(_Cfg()) == 0.0


def test_the_estimated_cost_uses_the_configured_rate():
    import app_state
    import control

    class _Cfg:
        def get(self, path, default=None):
            return {'api.pricing.cost_per_1k_tokens': 2.0}.get(path, default)

    saved = (app_state.total_input_tokens, app_state.total_output_tokens)
    try:
        app_state.total_input_tokens = 1000
        app_state.total_output_tokens = 500
        # (1000 + 500) * 2.0 / 1000 = 3.0
        assert control._estimated_cost(_Cfg()) == 3.0
    finally:
        app_state.total_input_tokens, app_state.total_output_tokens = saved


def test_the_cost_card_does_not_round_a_tiny_spend_to_zero():
    """`num(value, 4)` rendered 0.00004 as 0.0000, which looks identical to broken."""
    source = _strip_comments(_read('overview.js'))
    assert 'function costText' in source, source
    block = source.split('function costText', 1)[1].split('\n}', 1)[0]
    assert 'toFixed(6)' in block, block
    assert '0.01' in block, block


# --- 6. Clear the queue with no run -------------------------------------

def test_the_queue_clear_route_exists_and_is_a_post():
    from fastapi import FastAPI

    from webapp.tabs_routes import install_tabs

    app = install_tabs(FastAPI())
    routes = {}
    pending = list(app.routes)
    while pending:
        route = pending.pop()
        original = getattr(route, 'original_router', None)
        if original is not None:
            pending.extend(original.routes)
            continue
        path = getattr(route, 'path', None)
        if path:
            routes[path] = set(getattr(route, 'methods', set()) or set())
    assert routes.get('/api/queue/clear') == {'POST'}, routes.get('/api/queue/clear')


def test_clear_queue_touches_all_three_sets_and_the_queue_object():
    """Purge only clears pending, so a finished run's rows survive it forever."""
    source = _read_root('webapp/tabs_routes.py')
    block = source.split('def clear_queue():', 1)[1].split('@router', 1)[0]
    assert 'pending_task_ids.clear()' in block, block
    assert 'completed_task_ids.clear()' in block, block
    assert 'failed_task_ids.clear()' in block, block
    assert 'get_nowait()' in block, (
        'a stale item left in the queue object is picked up by the next run')


def test_clear_queue_never_deletes_a_dataset_row():
    """It must not reach the dataset routes or the drop path."""
    source = _read_root('webapp/tabs_routes.py')
    block = source.split('def clear_queue():', 1)[1].split('@router', 1)[0]
    for forbidden in ('review_data', 'drop(', 'clear_jsonl', 'clear_database'):
        assert forbidden not in block, f'clear_queue reaches {forbidden}'


def test_the_overview_has_a_clear_queue_button():
    """Stop & Clear needs a live run, so it cannot be the only way to empty a stale queue."""
    source = _strip_comments(_read('overview.js'))
    assert "'Clear queue'" in source, source
    block = source.split('async function clearQueueNow', 1)[1].split('\n}', 1)[0]
    assert "'/api/queue/clear'" in block, block
    assert 'window.confirm(' in block, 'a destructive action does not confirm'


def test_the_queue_tab_has_a_clear_queue_button():
    source = _strip_comments(_read('tabs.js'))
    block = source.split('const queueTab = makeTab(', 1)[1].split('\n});', 1)[0]
    assert "'Clear queue'" in block, block
    assert "'/api/queue/clear'" in block, block


# --- 7. Export flagged downloads in the browser ---------------------------

def test_the_export_download_route_answers_an_attachment():
    source = _read_root('webapp/review_routes.py')
    assert 'def export_download()' in source, source
    block = source.split('def export_download()', 1)[1].split('@router', 1)[0]
    assert 'Content-Disposition' in block, block
    assert 'attachment' in block, block
    assert 'Response(' in block, block


def test_an_empty_export_is_a_404_not_an_empty_file():
    """A zero-byte attachment looks like a successful download of nothing."""
    source = _read_root('webapp/review_routes.py')
    block = source.split('def export_download()', 1)[1].split('@router', 1)[0]
    assert 'status=404' in block, block


# --- 8. the Queue tab does not put pending first -------------------------

def test_the_queue_tab_puts_failed_before_pending():
    """A pending list of thousands pushed the two short tables off the screen."""
    source = _strip_comments(_read('tabs.js'))
    block = source.split('const queueTab = makeTab(', 1)[1].split('\n});', 1)[0]
    match = re.search(r"const order = \[([^\]]+)\]", block)
    assert match, block
    order = [part.strip().strip("'") for part in match.group(1).split(',')]
    assert order == ['failed', 'pending', 'completed'], order


def test_each_queue_table_scrolls_in_its_own_panel():
    """Otherwise a long pending list squashes the columns of the short tables.

    A `minmax(120px, 1fr)` floor looks reasonable and is wrong: three 120px floors plus the gaps
    exceed a short viewport, so the third panel was clipped. The floor must be `0`.
    """
    block = _css_block('.queue-grid')
    assert 'repeat(3, minmax(0, 1fr))' in block, block
    assert 'min-height: 0' in block, block
    body = _css_block('.queue-grid > .panel > .panel-body')
    assert 'overflow-y: auto' in body, body


# --- 9. the polish pass --------------------------------------------------

def test_the_switch_is_a_real_checkbox():
    """A div with a click handler is not a switch to a screen reader or the space bar."""
    source = _strip_comments(_read('tabs.js'))
    block = source.split('function toggle(', 1)[1].split('\n}', 1)[0]
    assert "type: 'checkbox'" in block, block


def test_the_switch_has_a_visible_checked_state():
    css = _css()
    assert '.switch-input:checked + .switch-track' in css, (
        'the switch has no checked style, so it never looks on')
    assert '.switch-input:focus-visible + .switch-track' in css, (
        'the switch has no focus ring, so it is unusable from the keyboard')


def test_the_polish_uses_palette_tokens_not_new_colours():
    """The contrast table is measured; a new literal would bypass it."""
    css = _css()
    for selector in ('.metrics-switches', '.section-heading', '.live-list-scroll',
                     '.queue-grid'):
        block = _css_block(selector)
        assert not re.search(r'#[0-9a-fA-F]{6}\b', block), f'{selector} has a hex literal'


def test_the_polish_respects_reduced_motion():
    """A pulsing dot is decoration and must never override an accessibility setting."""
    css = _css()
    assert 'prefers-reduced-motion' in css, css[:200]
    block = css.split('prefers-reduced-motion', 1)[1].split('}', 1)[0]
    assert 'animation: none' in block, block


def test_the_tab_strip_wraps_below_1100px():
    """Twelve tabs do not fit one row at 900px, and a scrolled strip hides the one you want."""
    css = _css()
    block = css.split('@media (max-width: 1100px)', 1)[1]
    # The last media block is the topbar one; it must set the strip to wrap.
    assert '.topbar .tabs' in block, block[:400]
    tail = block.split('.topbar .tabs', 1)[1].split('}', 1)[0]
    assert 'flex-wrap: wrap' in tail, tail


def test_the_new_modules_have_no_hex_literal():
    for name in ('tabs.js', 'chart.js', 'overview.js', 'dom.js', 'review.js', 'main.js'):
        source = _read(name)
        assert not re.search(r'#[0-9a-fA-F]{6}\b', source), (
            f'{name} contains a hex literal instead of a palette token')


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
