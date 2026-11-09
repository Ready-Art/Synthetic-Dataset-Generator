# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for Phase 8 -- polish.

Phase 8 is the phase with no new features, which makes it the easiest one to fake. Every rule
here is written so that a *cosmetic* regression fails a test rather than passing review:

* **A toast must be readable.** It pauses while hovered, it can be dismissed, the stack is
  capped, and a failure is announced by a screen reader. A toast that vanishes while being read
  is the same bug as a toast that never appeared.
* **A tablist must be operable without a mouse.** Arrow keys, Home and End, a roving tabindex
  and a `tabpanel` on the other end of `aria-controls`. A tablist that only responds to clicks
  is a list of buttons wearing the wrong role.
* **An empty state must say what is empty.** A bare grey line reads as a bug; the lesson from
  the Tk front end was that a blank area is indistinguishable from a broken one.
* **A reload must not lose the reader's place, and must not invent one.** The tab is remembered;
  nothing else is, because a run outlives the browser tab.
* **The first paint comes from the server.** A reload during a run must not show an empty
  Overview until the first socket tick.
* **Emoji are verified, not ported.** The browser's own fallback was checked before deciding
  not to port `EMOJI_FALLBACKS`. The finding is that the web front end uses **no emoji at
  all** -- only typographic punctuation -- so there is nothing to fall back from, and this file
  pins that so a future emoji cannot arrive without the decision being made again.

The JS is asserted by reading the source, the same way the other web test files do: there is no
build step and no JS runtime in the test environment, so the source *is* the artefact.
"""

import os
import re
import sys
import unicodedata

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import webapp.theme as theme

STATIC_JS = os.path.join(os.path.dirname(os.path.abspath(theme.__file__)), 'static', 'js')
STATIC_DIR = os.path.dirname(STATIC_JS)


def _read(name):
    with open(os.path.join(STATIC_JS, name), 'r', encoding='utf-8') as handle:
        return handle.read()


def _read_html():
    with open(os.path.join(STATIC_DIR, 'index.html'), 'r', encoding='utf-8') as handle:
        return handle.read()


def _strip_comments(source):
    """Drop `/* ... */` and `//` comments.

    Every rule below is about what the CODE does, not what a comment says. Without this, a
    comment that explains the Tk toast bug (\"it vanished while being read\") would fail the
    hover-pause check -- the same trap `test_web_overview.py` documents for `wraplength`.
    """
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


# --- 8a: the toast manager ------------------------------------------------

def test_a_toast_can_be_dismissed_by_the_reader():
    """A toast that only leaves on a timer cannot be kept, and one that never leaves is a modal."""
    source = _strip_comments(_read('dom.js'))
    assert 'toast-close' in source, 'the toast has no dismiss button'
    assert "'aria-label': 'Dismiss notification'" in source, (
        'the dismiss button has no accessible name')
    assert 'close.addEventListener' in source, 'the dismiss button is not wired'


def test_a_toast_pauses_while_hovered_or_focused():
    """A long error must not vanish while it is being read."""
    source = _strip_comments(_read('dom.js'))
    for event in ('mouseenter', 'mouseleave', 'focusin', 'focusout'):
        assert f"'{event}'" in source, f'the toast does not pause on {event}'


def test_a_toast_stack_is_capped():
    """Twenty toasts run off the top of the viewport and the oldest is unreadable anyway."""
    source = _strip_comments(_read('dom.js'))
    assert re.search(r'const TOAST_MAX = \d+', source), 'no stack cap constant'
    assert 'TOAST_MAX' in source.split('const TOAST_MAX')[1], 'the cap is declared but unused'


def test_a_failure_is_announced_and_a_success_is_not():
    """`role=alert` interrupts; `role=status` waits. A success must not interrupt."""
    source = _strip_comments(_read('dom.js'))
    assert "urgent ? 'alert' : 'status'" in source, (
        'the toast role does not depend on the level')


def test_both_the_tk_and_the_web_level_vocabulary_are_styled():
    """The Tk `ToastManager` said success/error/warning; the web callers say ok/warn/danger.

    A caller written against the Tk vocabulary must not get an unstyled toast -- an unstyled
    toast is the silent-failure class this front end keeps finding.
    """
    source = _strip_comments(_read('dom.js'))
    for level in ('success', 'error', 'warning', 'ok', 'warn', 'danger', 'info'):
        assert f"{level}:" in source, f'the {level} level is not mapped'
    for kind in ('ok', 'warn', 'danger'):
        assert f'.toast.{kind}' in _css(), f'.toast.{kind} is not styled'


def test_the_toast_container_is_a_live_region():
    """`#toasts` is the polite host; a per-toast role alone is not enough in some readers."""
    html = _read_html()
    assert 'id="toasts"' in html
    assert 'aria-live="polite"' in html, 'the toast container is not a live region'


# --- 8b: empty states -----------------------------------------------------

def test_an_empty_state_says_what_is_empty_and_what_to_do():
    """A bare grey line reads as a bug. The helper carries a title and a hint."""
    source = _strip_comments(_read('dom.js'))
    assert 'export function emptyState' in source, 'there is no shared empty-state helper'
    assert 'empty-title' in source, 'the empty state has no title'
    assert 'empty-hint' in source, 'the empty state has no hint'
    assert '.empty-title' in _css(), 'the empty-state title is unstyled'
    assert '.empty-hint' in _css(), 'the empty-state hint is unstyled'


def test_the_empty_state_keeps_the_class_every_caller_and_test_expects():
    """`empty` stays on the wrapper, so the existing selectors and tests keep working."""
    source = _strip_comments(_read('dom.js'))
    block = source.split('export function emptyState', 1)[1].split('\n}', 1)[0]
    assert "class: 'empty'" in block, 'the empty state dropped the `empty` class'


# A transient state is not an empty state: it is a state that is about to end. "Loading…"
# and "Reading the profile…" are progress, and dressing them up with a title and a hint would
# be noise. Everything else -- no data, no selection, a failed fetch -- is a real empty state
# and has to say what is empty.
TRANSIENT_MARKERS = ('Loading', 'Reading the profile')


def test_every_screen_that_can_be_empty_uses_the_helper():
    """A screen that still draws a bare `empty` div is a screen that did not get the polish."""
    for name in ('overview.js', 'review.js', 'datasets.js', 'profiles.js', 'tabs.js'):
        source = _strip_comments(_read(name))
        assert 'emptyState' in source, f'{name} has no rich empty state'
        bare = [line for line in source.splitlines()
                if "class: 'empty'" in line
                and not any(marker in line for marker in TRANSIENT_MARKERS)]
        assert not bare, f'{name} still draws a bare empty state: {bare[:2]}'


# --- 8c: keyboard navigation ---------------------------------------------

def test_the_tablist_answers_the_arrow_keys_home_and_end():
    source = _strip_comments(_read('main.js'))
    for key in ('ArrowLeft', 'ArrowRight', 'Home', 'End'):
        assert f"'{key}'" in source, f'the tablist ignores {key}'


def test_the_tablist_uses_a_roving_tabindex():
    """Only the selected tab is in the tab order, or Tab walks eleven tabs before the content."""
    source = _strip_comments(_read('main.js'))
    assert re.search(r"tabindex: selected \? '0' : '-1'", source), (
        'the tab does not set a roving tabindex')
    assert re.search(r"setAttribute\('tabindex', selected \? '0' : '-1'\)", source), (
        'the tabindex is not updated when the selection moves')


def test_focus_follows_the_arrow_keys():
    """A second arrow press must work without tabbing back to the list."""
    source = _strip_comments(_read('main.js'))
    assert re.search(r"getElementById\(`tab-\$\{id\}`\)\?\.focus\(\)", source), (
        'focus does not follow the selection')


def test_the_tablist_points_at_a_real_tabpanel():
    """`aria-controls` is one-sided unless the screen carries `role=tabpanel`."""
    source = _strip_comments(_read('main.js'))
    assert "'aria-controls': `screen-${tab.id}`" in source, (
        'the tab does not declare what it controls')
    assert "setAttribute('role', 'tabpanel')" in source, (
        'the screen is not a tabpanel')
    assert "setAttribute('aria-labelledby', `tab-${tab.id}`)" in source, (
        'the tabpanel does not point back at its tab')


def test_a_focus_ring_is_drawn_for_the_keyboard():
    """A ring only the browser draws is invisible on the raised surface in some engines."""
    css = _css()
    for selector in ('.tab:focus-visible', 'button:focus-visible',
                     'table.grid tbody tr:focus-visible'):
        assert selector in css, f'{selector} has no focus ring'


# --- 8d: window state ----------------------------------------------------

def test_the_selected_tab_is_remembered_across_a_reload():
    source = _strip_comments(_read('main.js'))
    assert 'localStorage' in source, 'the tab is not remembered'
    assert 'readStoredTab' in source and 'storeTab' in source, (
        'the tab is not read back and written')


def test_a_remembered_tab_that_no_longer_exists_falls_back():
    """A tab renamed or removed between releases must not leave a blank screen."""
    source = _strip_comments(_read('main.js'))
    block = source.split('function readStoredTab', 1)[1].split('\n}', 1)[0]
    assert 'TABS.some' in block, 'a stale tab id is not validated against the tab list'
    assert 'return null' in block, 'a stale tab id does not fall back'


def test_storage_is_never_allowed_to_break_the_page():
    """A browser with storage disabled throws on access. That must not be fatal."""
    source = _strip_comments(_read('main.js'))
    for name in ('readStoredTab', 'storeTab'):
        block = source.split(f'function {name}', 1)[1].split('\n}', 1)[0]
        assert 'try' in block and 'catch' in block, f'{name} does not guard storage'


def test_no_session_token_is_ever_put_in_storage():
    """Cookie only. The remembered tab is a UI preference, not a credential."""
    source = _strip_comments(_read('main.js'))
    storage_lines = [line for line in source.splitlines() if 'localStorage' in line]
    for line in storage_lines:
        lowered = line.lower()
        assert 'token' not in lowered and 'session' not in lowered, (
            f'a credential looks like it is being stored: {line.strip()}')


# --- 8e: reconnect on reload ---------------------------------------------

def test_the_first_paint_is_primed_from_the_server():
    """A reload during a run must not show an empty Overview until the first socket tick."""
    source = _strip_comments(_read('main.js'))
    assert 'primeStats' in source, 'there is no prime'
    block = source.split('async function primeStats', 1)[1].split('\n}', 1)[0]
    assert "'/api/stats'" in block, 'the prime does not read the stats route'
    assert 'onStats(' in block, 'the prime does not feed the same handler the socket does'


def test_the_prime_handles_an_expired_session():
    """A 401 on the prime must go to the login page, not render an empty Overview."""
    source = _strip_comments(_read('main.js'))
    block = source.split('async function primeStats', 1)[1].split('\n}', 1)[0]
    assert '401' in block, 'the prime ignores a 401'
    assert "/login" in block, 'the prime does not redirect to login'


def test_coming_back_online_or_to_the_tab_re_primes():
    """A sleeping laptop leaves the socket dead until its backoff fires."""
    source = _strip_comments(_read('main.js'))
    assert "'online'" in source, 'coming back online does not re-prime'
    assert 'visibilitychange' in source, 'returning to the tab does not re-prime'


def test_the_status_line_reports_a_disconnect():
    """A line that only changes on success looks identical to one that never updates."""
    source = _strip_comments(_read('stats.js'))
    assert 'onDisconnect' in source, 'the socket never reports a drop'
    main = _strip_comments(_read('main.js'))
    assert 'reconnecting' in main, 'the status line does not say it is reconnecting'


def test_the_disconnect_handler_is_cleared_on_stop():
    """A stopped socket must not fire a callback into a screen that is gone."""
    source = _strip_comments(_read('stats.js'))
    block = source.split('export function stop', 1)[1].split('\n}', 1)[0]
    assert 'onDisconnectHandler = null' in block, 'stop() leaves the handler attached'


# --- 8f: emoji verification ----------------------------------------------

# Every non-ASCII character the front end draws, with the reason it is safe. These are
# typographic punctuation, not emoji: every font the app names has them, so there is nothing
# to fall back from. The browser's fallback was verified before deciding not to port
# `EMOJI_FALLBACKS`; the verification is this list.
SAFE_PUNCTUATION = {
    '\u00b7',  # ·  middle dot -- a separator
    '\u2014',  # —  em dash
    '\u201c',  # “  left double quote
    '\u201d',  # ”  right double quote
    '\u2022',  # •  bullet
    '\u2026',  # …  ellipsis
    '\u203a',  # ›  single right angle quote
    '\u2192',  # →  rightwards arrow
    '\u25b8',  # ▸  right-pointing triangle -- a disclosure marker
    '\u25be',  # ▾  down-pointing triangle -- a disclosure marker
    '\u00d7',  # ×  multiplication sign -- the toast dismiss glyph
}


# A JS string may spell a glyph as `\u00d7` instead of drawing it. A scanner that only looks
# at raw characters would miss it and report a clean inventory that is not clean, so the
# escapes are decoded before the characters are counted.
_JS_ESCAPE_RE = re.compile(r'\\u([0-9a-fA-F]{4})|\\u\{([0-9a-fA-F]{1,6})\}')


def _decode_escapes(line):
    def replace(match):
        code = match.group(1) or match.group(2)
        try:
            return chr(int(code, 16))
        except (ValueError, OverflowError):
            return match.group(0)
    return _JS_ESCAPE_RE.sub(replace, line)


def _non_ascii_in_front_end():
    found = {}
    for dirpath, dirnames, filenames in os.walk(os.path.dirname(STATIC_JS)):
        dirnames[:] = [d for d in dirnames if d not in ('__pycache__', 'state')]
        for name in filenames:
            if not name.endswith(('.js', '.html', '.css')):
                continue
            path = os.path.join(dirpath, name)
            with open(path, 'r', encoding='utf-8') as handle:
                for number, line in enumerate(handle, 1):
                    for char in _decode_escapes(line):
                        if ord(char) > 127:
                            found.setdefault(char, []).append(
                                f'{os.path.relpath(path, STATIC_DIR)}:{number}')
    return found


def test_the_front_end_draws_no_emoji():
    """The browser's own fallback is fine for punctuation; emoji are the thing to verify.

    An emoji here means someone added a glyph that needs a colour-emoji font, which is the
    exact situation `EMOJI_FALLBACKS` exists for on the Tk side. The web front end has no
    such fallback, so an emoji would be a tofu box on a machine without the font.
    """
    offenders = {}
    for char, where in _non_ascii_in_front_end().items():
        if char not in SAFE_PUNCTUATION:
            offenders[char] = (unicodedata.name(char, '?'), where[:2])
    assert not offenders, f'non-punctuation glyphs in the front end: {offenders}'


def test_the_safe_punctuation_list_is_not_stale():
    """A character in the list that nothing draws is a list that has drifted."""
    drawn = set(_non_ascii_in_front_end())
    unused = SAFE_PUNCTUATION - drawn
    assert not unused, f'SAFE_PUNCTUATION lists characters nothing draws: {unused}'


def test_emoji_fallbacks_were_not_ported():
    """Verify, do not port. There is no `EMOJI_FALLBACKS` in the web app."""
    for dirpath, dirnames, filenames in os.walk(os.path.dirname(STATIC_JS)):
        dirnames[:] = [d for d in dirnames if d not in ('__pycache__', 'state')]
        for name in filenames:
            if not name.endswith(('.js', '.py', '.html')):
                continue
            with open(os.path.join(dirpath, name), 'r', encoding='utf-8') as handle:
                source = handle.read()
            assert 'EMOJI_FALLBACKS' not in source, (
                f'{name} ports EMOJI_FALLBACKS; the browser handles its own fallback')


# --- the modules still parse ---------------------------------------------

def test_every_es_module_parses():
    """A syntax error in a module is a blank tab, and there is no build step to catch it."""
    import shutil
    import subprocess

    node = shutil.which('node')
    if not node:
        return
    for name in sorted(os.listdir(STATIC_JS)):
        if not name.endswith('.js'):
            continue
        with open(os.path.join(STATIC_JS, name), 'r', encoding='utf-8') as handle:
            source = handle.read()
        result = subprocess.run([node, '--input-type=module', '--check'],
                                input=source, capture_output=True, text=True)
        assert result.returncode == 0, f'{name}: {result.stderr[:400]}'


def test_the_polished_modules_have_no_hex_literal():
    """No hex outside `theme.py`."""
    for name in ('main.js', 'dom.js', 'stats.js'):
        source = _read(name)
        assert not re.search(r'#[0-9a-fA-F]{6}\b', source), (
            f'{name} contains a hex literal instead of a palette token')
