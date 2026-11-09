# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the design-token module.

``ui_theme`` must be importable without a display and without creating any Tk
object, because ``generate_tk.py`` builds its whole window at import time and every
test that imports it would otherwise pay for a second Tk root.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ui_theme


HEX_RE = re.compile(r'^#[0-9a-fA-F]{6}$')


def test_import_creates_no_tk_object():
    """Importing ui_theme must not instantiate a Tk root or a ttk.Style."""
    import tkinter
    # If the module had created a root, tkinter._default_root would be set.
    assert tkinter._default_root is None, (
        "ui_theme created a Tk root at import time; it must be pure data"
    )


def test_colour_constants_are_valid_hex():
    bad = []
    for name in dir(ui_theme):
        if name.startswith('_'):
            continue
        value = getattr(ui_theme, name)
        if isinstance(value, str) and value.startswith('#'):
            if not HEX_RE.match(value):
                bad.append((name, value))
        elif isinstance(value, tuple) and value and isinstance(value[0], str) \
                and value[0].startswith('#'):
            for item in value:
                if not HEX_RE.match(item):
                    bad.append((name, item))
    assert not bad, f"invalid colour constants: {bad}"


def test_issue_colour_map_covers_every_key():
    for key in ('refusals', 'user_speak', 'slop', 'anti_slop',
                'out_of_character', 'errors', 'non_latin', 'scene_continuity'):
        assert key in ui_theme.ISSUE_COLORS, f"missing issue colour for {key}"
        assert HEX_RE.match(ui_theme.ISSUE_COLORS[key])


def test_font_stacks_are_tuples_of_strings():
    for name in ('UI_FONT_STACK', 'MONO_FONT_STACK', 'EMOJI_FONT_STACK'):
        stack = getattr(ui_theme, name)
        assert isinstance(stack, tuple) and stack, f"{name} must be a non-empty tuple"
        assert all(isinstance(item, str) for item in stack), f"{name} must hold strings"


def test_spacing_scale_is_ordered():
    assert ui_theme.SPACE_XS < ui_theme.SPACE_S < ui_theme.SPACE_M < ui_theme.SPACE_L
    assert ui_theme.ROW_SLACK >= 0


def test_theme_name_is_a_string():
    assert isinstance(ui_theme.THEME_NAME, str) and ui_theme.THEME_NAME


def test_every_emoji_in_the_source_has_a_fallback():
    """No emoji may reach a widget without an ASCII substitute available.

    The front end uses emoji as icons. Where no colour-emoji font is installed they
    render as tofu boxes, so every one of them is routed through
    ui_theme.icon_text(), which substitutes from EMOJI_FALLBACKS. A new emoji added
    without a map entry would silently reintroduce tofu, so scan the sources.
    """
    import os
    import re

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    emoji = re.compile('[\U0001F300-\U0001FAFF\u2600-\u27BF\u2B00-\u2BFF]')
    missing = {}
    for filename in ('generate_tk.py', 'config_editor.py', 'dashboard.py'):
        path = os.path.join(repo, filename)
        with open(path, encoding='utf-8') as handle:
            source = handle.read()
        # A glyph in a comment can never reach a widget, so it cannot render as
        # tofu. Only count emoji that appear in actual code.
        code = '\n'.join(line for line in source.splitlines()
                         if not line.lstrip().startswith('#'))
        for glyph in set(emoji.findall(code)):
            if glyph not in ui_theme.EMOJI_FALLBACKS:
                missing.setdefault(glyph, set()).add(filename)
    assert not missing, (
        "emoji with no EMOJI_FALLBACKS entry: "
        + ", ".join(f"{glyph!r} in {sorted(files)}"
                     for glyph, files in sorted(missing.items())))


def test_icon_text_leaves_text_alone_when_emoji_render():
    """With a colour-emoji font present the caption must be unchanged."""
    class FakeRoot:
        def winfo_toplevel(self):
            return self
        _ui_theme_emoji_font = True

    assert ui_theme.icon_text(FakeRoot(), "\U0001f680 Start") == "\U0001f680 Start"
    assert ui_theme.icon(FakeRoot(), "\U0001f680") == "\U0001f680"


def test_icon_text_substitutes_when_emoji_are_missing():
    """Without the font, every glyph becomes its ASCII fallback."""
    class FakeRoot:
        def winfo_toplevel(self):
            return self
        _ui_theme_emoji_font = False

    assert ui_theme.icon_text(FakeRoot(), "\U0001f680 Start") == "[>] Start"
    # A multi-codepoint sequence must not be half-replaced by its bare form.
    assert ui_theme.icon_text(FakeRoot(), "\u26a0\ufe0f Warning") == "[!] Warning"
    assert ui_theme.icon(FakeRoot(), "\U0001f680") == "[>]"


def test_minimum_window_size_is_one_token():
    """The main window, the Config Editor and the harness must agree on the floor.

    fit_window() was called with the literal (760, 520) in generate_tk.py and
    config_editor.py, and tools/check_gui_layout.py asserted against its own copy.
    Raising the app's minimum without raising the harness's bound made the harness
    pass a window it should have failed.
    """
    import ast
    import os

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    minimum = ui_theme.MINIMUM_WINDOW_SIZE
    assert isinstance(minimum, tuple) and len(minimum) == 2
    assert all(isinstance(value, int) and value > 0 for value in minimum)

    for filename in ('generate_tk.py', 'config_editor.py'):
        path = os.path.join(repo, filename)
        with open(path, encoding='utf-8') as handle:
            source = handle.read()
        assert 'minimum=(760, 520)' not in source, (
            f"{filename}: fit_window() still hard-codes the minimum")
        assert 'ui_theme.MINIMUM_WINDOW_SIZE' in source, (
            f"{filename}: fit_window() does not read ui_theme.MINIMUM_WINDOW_SIZE")

    harness = os.path.join(repo, 'tools', 'check_gui_layout.py')
    with open(harness, encoding='utf-8') as handle:
        harness_source = handle.read()
    assert 'SUPPORTED_MINIMUM_AT_96_DPI = _ui_theme.MINIMUM_WINDOW_SIZE' in harness_source, (
        "the layout harness no longer derives its bound from ui_theme")


def test_spacing_aliases_are_the_token_not_a_copy():
    """The three SPACING constants must alias SPACE_S, not repeat the number.

    generate_tk.py, dashboard.py and config_editor.py each used to define their own
    ``SPACING = 8``. Two of them were labelled "mirrors generate_tk.py", which is
    exactly the kind of comment that stops being true. They are now aliases, so this
    test fails the moment someone writes a literal back in.
    """
    import ast
    import os

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for filename in ('generate_tk.py', 'dashboard.py', 'config_editor.py'):
        path = os.path.join(repo, filename)
        with open(path, encoding='utf-8') as handle:
            tree = ast.parse(handle.read(), filename=path)
        assignments = [node for node in tree.body
                       if isinstance(node, ast.Assign)
                       and any(isinstance(t, ast.Name) and t.id == 'SPACING'
                               for t in node.targets)]
        assert len(assignments) == 1, f"{filename}: expected one module-level SPACING"
        value = assignments[0].value
        assert isinstance(value, ast.Attribute) and value.attr == 'SPACE_S', (
            f"{filename}: SPACING must be ui_theme.SPACE_S, got {ast.unparse(value)!r}")


def test_resolve_font_and_row_height_under_a_display():
    """The two helpers need a live Tk root; skip cleanly when headless."""
    try:
        import tkinter as tk
        from tkinter import font as tkfont
        root = tk.Tk()
    except Exception:
        print("SKIP: no display available for resolve_font/row_height")
        return

    try:
        resolved = ui_theme.resolve_font(root, ui_theme.UI_FONT_STACK, 10)
        assert isinstance(resolved, tuple) and len(resolved) == 2
        family, size = resolved
        assert size == 10
        assert family in set(tkfont.families(root)), (
            f"resolve_font returned {family!r}, which Tk does not have"
        )

        bold = ui_theme.resolve_font(root, ui_theme.UI_FONT_STACK, 12, 'bold')
        assert len(bold) == 3 and bold[2] == 'bold'

        mono = ui_theme.resolve_font(root, ui_theme.MONO_FONT_STACK, 9)
        assert mono[0] in set(tkfont.families(root))

        linespace = tkfont.Font(root=root, font=resolved).metrics('linespace')
        height = ui_theme.row_height(root, resolved)
        assert height >= linespace + ui_theme.ROW_SLACK, (
            f"row_height {height} < linespace {linespace} + slack {ui_theme.ROW_SLACK}"
        )

        # An unknown stack must fall back to its last entry rather than raise.
        fallback = ui_theme.resolve_font(root, ('NoSuchFamilyXYZ',), 10)
        assert fallback[0] == 'NoSuchFamilyXYZ'
    finally:
        root.destroy()


if __name__ == '__main__':
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    print(f"\n{'OK' if not failures else str(failures) + ' FAILED'}")
    sys.exit(1 if failures else 0)
