# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Headless tests for the Overview screen.

The layout decisions are the point of this file, because they are what fixes the
"top half is a wall of text" complaint:

* **Six metric cards in one row**, not sixteen in three rows. The rest are behind a
  disclosure. This is asserted from the source, because there is no DOM here.
* The issue table and the progress panel sit **side by side** at `1.55fr / 1fr`.
* Progress is **one row per slot** with a label, a track and a percentage.
* **No `wraplength`** anywhere: that plus a 22px value inside a 180px grid cell was the
  actual layout bug, so a regression here would reintroduce it.
* Long values **ellipsize with a `title` tooltip** rather than wrapping.

No display, no browser, no network: the JS modules are read as text and the CSS is parsed
out of ``theme.py``. The rendered screenshots are the manual gate; this file is the
regression net.
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import webapp.theme as theme

STATIC_JS = os.path.join(os.path.dirname(os.path.abspath(theme.__file__)), 'static', 'js')


def _read(name):
    with open(os.path.join(STATIC_JS, name), 'r', encoding='utf-8') as handle:
        return handle.read()


def _strip_comments(source):
    """Drop `/* ... */` and `//` comments.

    Every rule here is about what the CODE does, not what a comment says. Without this,
    a comment that explains the layout bug ("there is no wraplength here") would fail the
    wraplength check -- which is exactly what happened the first time this file ran.
    """
    source = re.sub(r'/\*.*?\*/', '', source, flags=re.DOTALL)
    source = re.sub(r'^\s*//.*$', '', source, flags=re.MULTILINE)
    return source


def _css():
    """The stylesheet with comments removed."""
    return _strip_comments(theme.STYLESHEET)


def _css_block(selector):
    """The body of the TOP-LEVEL rule for `selector`, ignoring @media blocks.

    A plain `split('.overview-grid {')` finds the media-query override first, because
    the responsive rules are written before the base rules in the file. This walks the
    stylesheet at depth 0 so the base rule is the one that is read.
    """
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


# --- the six-card row ----------------------------------------------------

def test_the_primary_metric_row_has_exactly_six_cards():
    """Six cards in one row, not sixteen in three rows."""
    source = _read('overview.js')
    block = source.split('const PRIMARY_METRICS = [', 1)[1].split('];', 1)[0]
    keys = re.findall(r"key: '([a-z_]+)'", block)
    assert len(keys) == 6, f'PRIMARY_METRICS has {len(keys)} entries: {keys}'
    assert keys == ['refusals', 'out_of_character', 'slop', 'logic_issues',
                    'regex_watch', 'errors'], keys


def test_the_remaining_metrics_are_behind_a_disclosure():
    """The other counters exist, but not in the first row."""
    source = _read('overview.js')
    block = source.split('const SECONDARY_METRICS = [', 1)[1].split('];', 1)[0]
    keys = re.findall(r"key: '([a-z_]+)'", block)
    assert keys, 'the secondary metrics are gone entirely'
    for key in keys:
        assert key not in source.split('const PRIMARY_METRICS = [', 1)[1].split('];', 1)[0], (
            f'{key} is in both rows')
    assert 'More metrics' in source, 'there is no disclosure to reach them'


def test_the_metric_row_is_a_six_column_grid():
    block = _css_block('.metric-row')
    assert 'repeat(6, minmax(0, 1fr))' in block, block


def test_the_metric_row_is_responsive():
    """Below 1100px six across would clip a 28px value, so it drops to three."""
    css = _css()
    assert '@media (max-width: 1100px)' in css
    block = css.split('@media (max-width: 1100px)', 1)[1].split('}\n}', 1)[0]
    assert 'repeat(3, minmax(0, 1fr))' in block, block


# --- the two-column body ------------------------------------------------

def test_the_body_is_a_155_to_1_split():
    """The issue table and the progress panel sit side by side."""
    block = _css_block('.overview-grid')
    assert '1.55fr 1fr' in block, block


def test_the_side_column_stacks_progress_over_the_graph():
    block = _css_block('.overview-side')
    assert 'grid-template-rows' in block, block
    assert 'minmax(0, 1fr)' in block, (
        'the graph row must be able to shrink, or it pushes the progress panel off')


def test_the_default_view_does_not_scroll():
    """Every screen fits 1440x900 with no page scroll."""
    css = _css()
    assert '.screen.active { overflow: hidden; }' in css
    assert '.main.expanded .screen.active { overflow-y: auto; }' in css


def test_no_magic_viewport_arithmetic():
    """A `calc(100vh - Npx)` drifts the moment the header height changes."""
    css = _css()
    assert 'calc(100vh' not in css, (
        'the layout uses a hard-coded viewport subtraction; use flex and min-height: 0')


# --- progress ------------------------------------------------------------

def test_progress_is_one_row_per_slot():
    """A label, a track and a percentage -- not a stack of full-width bars."""
    source = _read('overview.js')
    assert 'progressRow(' in source
    assert 'progress-track' in source
    assert 'progress-fill' in source
    assert 'progress-pct' in source
    # A stacked full-width bar must not come back.
    assert 'progress-bar-full' not in source


def test_progress_rows_are_labelled():
    source = _read('overview.js')
    for label in ("'Overall'", "'Queue'", "'Threads'"):
        assert label in source, f'the progress rows lost {label}'


# --- the "massive text" fix ---------------------------------------------

def test_no_wraplength_anywhere():
    """The bug was a 22px value inside wraplength=220 inside a 180px cell."""
    for name in ('overview.js', 'dom.js', 'chart.js', 'main.js', 'stats.js'):
        source = _strip_comments(_read(name))
        assert 'wraplength' not in source, f'{name} reintroduced wraplength'
    assert 'wraplength' not in _css()


def test_long_values_ellipsize_with_a_tooltip():
    """The card grows and the value ellipsizes with a `title`."""
    block = _css_block('.card-value')
    assert 'text-overflow: ellipsis' in block, block
    assert 'white-space: nowrap' in block, block
    source = _read('dom.js')
    assert 'title: String(value)' in source, (
        'a metric card no longer carries a title tooltip for its full value')


def test_the_card_value_is_28px():
    """The type scale: a 28px card value, not the old 22px."""
    assert theme.SIZE_CARD_VALUE == '28px'
    assert '--size-card-value: 28px' in _css()


def test_the_card_caption_is_10px_uppercase_tracked():
    block = _css_block('.card-caption')
    assert 'text-transform: uppercase' in block, block
    assert 'letter-spacing' in block, block
    assert theme.SIZE_CAPTION == '10px'


# --- the disclosures ----------------------------------------------------

def test_a_disclosure_toggles_locally_not_by_re_render():
    """A full re-render replaces the button that was clicked, so a second click
    lands on a detached node and does nothing."""
    source = _read('overview.js')
    block = source.split('function disclosureBlock(', 1)[1].split('\n}', 1)[0]
    assert 'openState[id] = !openState[id]' in block, block
    assert 'replaceChildren' in block, block
    assert 'renderOverview(' not in block, (
        'the disclosure re-renders the whole screen instead of toggling itself')


def test_the_disclosure_state_survives_a_stats_re_render():
    """The open flags live at module scope, not inside the render function."""
    source = _read('overview.js')
    assert re.search(r"^const openState = \{", source, re.MULTILINE), (
        'openState is not at module scope, so a stats tick closes the disclosure')


# --- the action bar -----------------------------------------------------

def test_the_action_bar_has_the_four_core_actions():
    source = _read('overview.js')
    for label in ("'Start'", "'Stop'", "'Stop & Clear'"):
        assert label in source, f'the action bar lost {label}'
    assert "'Pause'" in source and "'Resume'" in source


def test_actions_are_disabled_rather_than_left_to_fail():
    """A disabled button is better than a click that returns a 409."""
    source = _read('overview.js')
    assert 'start.disabled = Boolean(run.active)' in source
    assert 'stop.disabled = !run.active' in source


def test_stop_and_clear_asks_before_destroying_the_dataset():
    source = _read('overview.js')
    assert 'window.confirm(' in source, 'Stop & Clear does not confirm'


# --- the issue panels ---------------------------------------------------

def test_the_overview_has_seven_issue_panels():
    """The old Totals set: seven panels, Errors excluded."""
    source = _read('overview.js')
    block = source.split('const ISSUE_PANELS = [', 1)[1].split('];', 1)[0]
    keys = re.findall(r"key: '([a-z_]+)'", block)
    assert len(keys) == 7, f'ISSUE_PANELS has {len(keys)}: {keys}'
    assert 'errors' not in keys, 'Overview must exclude the Errors panel'
    assert 'refusals' in keys and 'regex_watch' in keys


def test_the_issue_table_shows_time_api_phrase_and_context():
    """The Overview column set."""
    source = _read('overview.js')
    block = source.split('function issueTable(', 1)[1].split('\n}', 1)[0]
    for column in ("'Time'", "'API'", "'Phrase'", "'Context'"):
        assert column in block, f'the issue table lost {column}'


def test_the_issue_table_has_an_empty_state():
    source = _read('overview.js')
    assert 'No issues recorded yet' in source


# --- the graph ---------------------------------------------------------

def test_the_graph_is_hand_rolled_svg():
    """No Chart.js, no vendored bundle."""
    source = _read('chart.js')
    assert 'createElementNS' in source
    assert 'polyline' in source
    # index.html lives in static/, not static/js/.
    index = os.path.join(os.path.dirname(STATIC_JS), 'index.html')
    with open(index, 'r', encoding='utf-8') as handle:
        html = handle.read()
    assert 'chart.js' not in html.lower().replace('/js/chart.js', ''), (
        'a charting library is loaded from index.html')


def test_the_graph_colours_come_from_the_palette():
    """No hex literal in the chart: it reads the CSS custom properties."""
    source = _read('chart.js')
    assert 'getPropertyValue' in source
    assert not re.search(r'#[0-9a-fA-F]{6}\b', source), (
        'chart.js contains a hex literal instead of a palette token')


def test_the_graph_draws_only_after_insertion():
    """`getComputedStyle` returns empty strings for a detached node, so a draw
    before insertion produces an invisible chart rather than an error."""
    source = _read('overview.js')
    assert 'requestAnimationFrame' in source
    assert 'isConnected' in source, (
        'the chart is drawn before the panel is in the document')


def test_the_graph_has_an_empty_state():
    source = _read('chart.js')
    assert 'empty' in source
    assert 'No data yet' in source or 'fills in as a run' in _read('overview.js')


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
