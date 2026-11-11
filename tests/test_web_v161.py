# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Headless tests for the v16.1.0 web-UI round -- two complaints, one section each.

1. **The Metrics tab never refreshed.** The per-slot throughput ring is appended by the
   generation threads and the server/proxy rings by the vLLM poller, both on their own
   schedule, so a tab that only re-fetched on a click showed a snapshot of a run that had
   already moved on. The tab now polls, and an **Auto refresh** switch in the toolbar stops it.
2. **The Live Prompt Viewer scrolled away from the prompt being read.** The list is
   newest-first, so every new prompt is PREPENDED; restoring a raw `scrollTop` therefore
   showed a different row each poll and the prompt under the cursor drifted down the viewport
   until it was gone. The list is now anchored to the selected ROW.

Both are asserted against the failure mode rather than the fix, because the failure modes are
what a future change will reintroduce. No display, no browser, no network: the JS modules are
read as text, exactly as ``test_web_v151.py`` does.
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
    source = re.sub(r'/\*.*?\*/', '', source, flags=re.DOTALL)
    source = re.sub(r'^\s*//.*$', '', source, flags=re.MULTILINE)
    return source


def _make_tab_block():
    """The whole `makeTab` factory, which is where the polling lives."""
    source = _strip_comments(_read('tabs.js'))
    return source.split('function makeTab(', 1)[1].split('\n}\n', 1)[0]


def _tab_block(name):
    source = _strip_comments(_read('tabs.js'))
    return source.split(f'const {name} = makeTab(', 1)[1].split('\n});', 1)[0]


# --- 1. the Metrics tab refreshes itself ---------------------------------

def test_the_metrics_tab_polls_while_it_is_mounted():
    """The figures are written by other threads, so a click-only refresh is a stale snapshot."""
    block = _tab_block('metricsTab')
    assert 'interval:' in block, (
        'the Metrics tab has no interval, so it only updates on a click')


def test_the_metrics_poll_matches_the_vllm_poll_cadence():
    """Polling slower than the poller means the page is always behind what it could show."""
    import vllm_metrics

    block = _tab_block('metricsTab')
    match = re.search(r'interval:\s*(\d+)', block)
    assert match, block
    assert int(match.group(1)) == int(vllm_metrics.POLL_INTERVAL_SECONDS * 1000), (
        f'the tab polls every {match.group(1)} ms but the poller every '
        f'{vllm_metrics.POLL_INTERVAL_SECONDS} s')


def test_the_polling_tab_offers_an_auto_refresh_switch():
    """A page that repaints under the reader must be stoppable."""
    block = _make_tab_block()
    assert "'Auto refresh'" in block, (
        'a polling tab has no way to stop the polling')


def test_the_auto_refresh_switch_starts_and_stops_the_timer():
    """The switch has to reach the interval, not just change its own label."""
    block = _make_tab_block()
    assert 'startTimer()' in block, block
    assert 'stopTimer()' in block, block
    assert 'clearInterval' in block, 'the poll is never stopped'
    # Flipping the switch must actually start or stop it, not merely record the choice.
    handler = block.split('auto.on = value;', 1)[1].split('},', 1)[0]
    assert 'startTimer()' in handler, handler
    assert 'stopTimer()' in handler, handler


def test_the_auto_refresh_switch_defaults_to_on_for_a_polling_tab():
    """A tab with an interval that ships switched off is a tab that still does not refresh."""
    block = _make_tab_block()
    assert 'autoRefresh = Boolean(interval)' in block, block
    assert 'on: Boolean(interval) && Boolean(autoRefresh)' in block, block


def test_a_tab_with_no_interval_gets_no_auto_refresh_switch():
    """Only a polling tab has anything to switch off."""
    block = _make_tab_block()
    toolbar = block.split("el('div', { class: 'tab-toolbar' }", 1)[1]
    assert 'interval' in toolbar, (
        'the Auto refresh switch is not gated on the tab actually polling')


def test_the_poll_does_not_run_after_the_tab_is_left():
    block = _make_tab_block()
    unmount = block.split('unmount()', 1)[1]
    assert 'stopTimer()' in unmount, (
        'leaving the tab leaves the interval running against a detached screen')


def test_the_poll_does_not_fight_a_repaint_in_flight():
    block = _make_tab_block()
    assert '!state.loading' in block, (
        'a poll can start while a fetch is already running')


# --- 1a. a repaint must not throw the scroll position away -----------------

def test_a_tab_repaint_preserves_every_keyed_scroller():
    """The same class of bug the Overview's Recent issues had, on every polling tab."""
    block = _make_tab_block()
    repaint = block.split('function repaint()', 1)[1].split('\n  }', 1)[0]
    assert 'captureScroll(' in repaint, repaint
    assert 'restoreScroll(' in repaint, repaint


def test_the_tab_body_carries_a_stable_scroll_key():
    """The key must be stable across renders, so it is derived from the tab id."""
    block = _make_tab_block()
    assert 'scrollKey: `tab-body-${id}`' in block, block


# --- 2. the Live Prompt Viewer keeps the prompt you are reading -----------

def test_the_live_list_anchors_to_the_selected_row_not_a_raw_offset():
    """A raw `scrollTop` is the bug: the list is newest-first, so new entries are prepended
    above the row being read and the same offset shows a different row."""
    block = _tab_block('livePromptsTab')
    assert 'liveAnchor' in block, block
    assert 'readAnchor' in block, block
    # The anchor is a row position, so it must be measured from the row, not from a number.
    reader = block.split('function readAnchor()', 1)[1].split('\n    }', 1)[0]
    assert 'getBoundingClientRect' in reader, reader
    assert 'data-seq' in reader, reader


def test_the_live_list_rows_carry_their_sequence_number():
    """Without `data-seq` the anchor cannot find the row again after a repaint."""
    block = _tab_block('livePromptsTab')
    assert 'dataset: { seq: String(entry.seq) }' in block, block


def test_the_live_list_restores_the_anchor_after_a_repaint():
    block = _tab_block('livePromptsTab')
    restore = block.split('requestAnimationFrame(() => {', 1)[1].split('\n    });', 1)[0]
    assert 'anchor.offset' in restore, restore
    assert 'scrollTop +=' in restore, (
        'the list is set to an absolute offset instead of being shifted by the row delta')


def test_the_live_list_follows_the_tail_only_when_the_reader_is_at_the_tail():
    """A reader watching the newest prompt should see it arrive; a reader reading must not move."""
    block = _tab_block('livePromptsTab')
    restore = block.split('requestAnimationFrame(() => {', 1)[1].split('\n    });', 1)[0]
    assert 'following' in restore, restore
    assert 'liveScrollTop === 0' in restore, restore
    assert 'entries[0].seq' in restore, restore


def test_clicking_a_live_prompt_does_not_move_the_list():
    """The reader picked that row; it is already where they want it."""
    block = _tab_block('livePromptsTab')
    handler = block.split("row.addEventListener('click'", 1)[1].split('});', 1)[0]
    assert 'liveSelected = entry.seq' in handler, handler
    assert 'liveAnchor = null' in handler, (
        'a click keeps a stale anchor, so the list jumps on the next poll')


def test_the_live_preview_keeps_its_place_in_a_long_prompt():
    """A poll repainting the SAME entry must not scroll the prompt body back to the top."""
    block = _tab_block('livePromptsTab')
    assert 'scrollKey: `live-preview-${selected.seq}`' in block, block


def test_the_viewer_follows_the_tail_until_the_reader_takes_hold():
    """An unpinned viewer shows the newest prompt; a pinned one keeps the reader's choice.

    Without the pin the viewer cannot tell "the reader is watching the live tail" from "the
    reader's selection is simply the newest entry that was newest two polls ago", so a click on an
    older prompt would be undone by the next poll.
    """
    source = _strip_comments(_read('tabs.js'))
    assert 'let livePinned' in source, 'there is no pin, so the poll can undo a click'
    block = _tab_block('livePromptsTab')
    selection = block.split('if (!livePinned', 1)[1].split('}', 1)[0]
    assert 'liveSelected = entries[0].seq' in selection, selection


def test_clicking_a_prompt_pins_the_viewer():
    block = _tab_block('livePromptsTab')
    handler = block.split("row.addEventListener('click'", 1)[1].split('});', 1)[0]
    assert 'livePinned = true' in handler, (
        'a click does not pin the viewer, so the next poll moves the list')


def test_scrolling_the_list_pins_the_viewer_too():
    """Scrolling back through a retry sequence is the reader taking hold of the list."""
    block = _tab_block('livePromptsTab')
    listener = block.split("addEventListener('scroll'", 1)[1].split('});', 1)[0]
    assert 'livePinned = true' in listener, listener


def test_clearing_the_history_unpins_the_viewer():
    """An emptied ring has nothing to hold on to."""
    block = _tab_block('livePromptsTab')
    assert 'livePinned = false' in block, block


def test_the_live_list_still_remembers_a_plain_scroll_offset():
    """The v15.1.0 behaviour stays: a scrolled list is not thrown to the top by a poll."""
    block = _tab_block('livePromptsTab')
    assert 'liveScrollTop' in block, block
    listener = block.split("addEventListener('scroll'", 1)[1].split('});', 1)[0]
    assert 'liveScrollTop = scroller.scrollTop' in listener, listener
    assert 'liveAnchor = readAnchor()' in listener, listener


# --- the module still parses ---------------------------------------------

def test_the_tab_module_still_parses():
    """A syntax error here is a blank page, not a failing assertion."""
    import shutil
    import subprocess

    node = shutil.which('node')
    if not node:
        return
    source = _read('tabs.js')
    result = subprocess.run([node, '--input-type=module', '--check'],
                            input=source, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr[:400]


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
