# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Design tokens for the Tk front end.

Pure data: importing this module must never create a Tk object, so it is safe to
import from a headless test, the CLI and the layout harness.

Every colour, font family, font size, spacing step and corner radius the GUI uses
lives here. Call sites reference the semantic names below; they never spell out a
raw ``#rrggbb`` literal or a bare font family name.

Two helpers resolve values that depend on the running Tk instance:

``resolve_font(root, stack, size, weight)``
    Walks a font *stack* and returns the first family Tk actually has. This is the
    fix for the Windows-only ``Segoe UI`` / ``Consolas`` problem: on Linux and
    macOS those families are silently substituted with different metrics, so the
    layout tuned on Windows is not the layout anyone else sees.

``row_height(root, font_tuple, slack)``
    The ``Treeview`` row height for a given font, derived from the font's own
    ``linespace``. Every table style must set an explicit ``rowheight`` built this
    way, because ttk leaves ``rowheight`` empty on every theme.
"""

# --- Palette -------------------------------------------------------------
# Semantic names only. Never reference a raw hex outside this module.
#
# Tuned for the `one-dark` theme (bg #282c34, fg #abb2bf). The surfaces below are
# deliberately a few steps away from the theme bg so cards and table bodies read as
# distinct layers instead of "slightly wrong" background.
SURFACE = '#282c34'           # window / page background (== one-dark bg)
SURFACE_RAISED = '#2d313b'    # cards, table bodies (== one-dark inputbg)
SURFACE_SUNKEN = '#21252b'    # table headings, toolbars
SURFACE_INPUT = '#2d313b'     # entry / spinbox fields
SURFACE_SELECTED = '#383d49'   # selected table row (== one-dark active)
SURFACE_PLOT = '#21252b'      # matplotlib / canvas backdrop
SURFACE_DISABLED = '#343943'  # disabled field background
BORDER = '#4a4e54'            # == one-dark border
BORDER_STRONG = '#5c6370'

TEXT = '#abb2bf'              # == one-dark fg
TEXT_MUTED = '#949ca8'             # secondary text (5.05:1 on SURFACE, 4.70:1 on SURFACE_RAISED)
TEXT_SUBTLE = '#6b717d'
TEXT_INVERSE = '#ffffff'
TEXT_DIM = '#c8ccd4'
TEXT_FAINT = '#8b919c'
TEXT_DISABLED = '#9aa0aa'     # text on a disabled field (4.4:1 on SURFACE_DISABLED)

# `one-dark`'s own primary is #6693f5, which is only 2.98:1 behind white text and
# fails WCAG AA. A filled primary button therefore uses this darker accent instead
# (#3a63c0 behind white is 5.64:1). Do not swap this for the theme's own primary:
# a filled primary button below 4.5:1 is unreadable.
ACCENT = '#3a63c0'            # primary action, active tab
ACCENT_HOVER = '#4a76d4'
ACCENT_PRESSED = '#2f5299'
ACCENT_TEXT = '#ffffff'       # text on an ACCENT fill

# Non-button accent used for headings and highlights where no fill is involved.
ACCENT_SOFT = '#6693f5'

OK = '#73b472'
WARN = '#cd9d34'
DANGER = '#e9786d'
DANGER_DEEP = '#b64242'   # error text that must stay readable on SURFACE
INFO = '#349dc9'

# Toast levels (ToastManager)
TOAST_SUCCESS = '#28a745'
TOAST_ERROR = '#dc3545'
TOAST_WARNING = '#ffc107'
TOAST_INFO = '#17a2b8'

# --- Per-issue highlight colours -----------------------------------------
# Keyed by the issue key used in generate.py's panel loop.
ISSUE_REFUSALS = '#ff4d6d'
ISSUE_USER_SPEAK = '#4dabf7'
ISSUE_SLOP = '#9775fa'
ISSUE_ANTI_SLOP = '#ffd43b'
ISSUE_OUT_OF_CHARACTER = '#ff922b'
ISSUE_ERRORS = '#fc8181'
ISSUE_NON_LATIN = '#20c997'
ISSUE_SCENE_CONTINUITY = '#e599f7'

ISSUE_COLORS = {
    'refusals': ISSUE_REFUSALS,
    'user_speak': ISSUE_USER_SPEAK,
    'slop': ISSUE_SLOP,
    'anti_slop': ISSUE_ANTI_SLOP,
    'out_of_character': ISSUE_OUT_OF_CHARACTER,
    'errors': ISSUE_ERRORS,
    'non_latin': ISSUE_NON_LATIN,
    'scene_continuity': ISSUE_SCENE_CONTINUITY,
}
ISSUE_FALLBACK = DANGER

# --- Prompt / transcript syntax highlighting ------------------------------
SYNTAX_SYSTEM = '#bb86fc'
SYNTAX_SYSTEM_CONTENT = '#ce93d8'
SYNTAX_USER = '#4fc3f7'
SYNTAX_USER_CONTENT = '#81d4fa'
SYNTAX_ASSISTANT = '#69f0ae'
SYNTAX_ASSISTANT_CONTENT = '#b2dfdb'
SYNTAX_HEADING = '#bb86fc'
SYNTAX_WARN = '#ffd43b'
SYNTAX_RULE = '#444466'
TEXT_SELECT_BG = '#2a4a6a'      # selection inside a Text widget
TEXT_ON_HIGHLIGHT = '#000000'    # text on a SYNTAX_WARN search highlight

# --- Scrollbars ----------------------------------------------------------
SCROLL_THUMB = '#7a7a9a'
SCROLL_THUMB_ACTIVE = '#b0b0d0'
SCROLL_TROUGH = '#555555'
SCROLL_THUMB_LIGHT = '#bbbbbb'
SCROLL_THUMB_DIM = '#5a5a70'      # the narrower scrollbars in the config editor
SCROLL_THUMB_DIM_ACTIVE = '#8a8aa0'

# --- Misc semantic colours ----------------------------------------------
WARN_SOFT = '#ffb86b'           # "attention" amber used by the vLLM panels

# --- Chart (matplotlib) colours -------------------------------------------
# The issue graph and the vLLM panels draw on their own Figure, so they need a
# palette that does not come from ttk.
CHART_BG = SURFACE_SUNKEN
CHART_AXES_BG = SURFACE_RAISED
CHART_TEXT = TEXT
CHART_TEXT_TITLE = TEXT_INVERSE
CHART_TEXT_TICK = TEXT_DIM
CHART_GRID = '#555555'
CHART_LEGEND_EDGE = '#444444'
CHART_LEGEND_FACE = SURFACE_RAISED
CHART_LEGEND_TEXT = TEXT

# One colour per API slot index (0-6), matching the slot colours used elsewhere.
SERIES_COLORS = ('#4dabf7', '#51cf66', '#ffd43b', '#ff922b',
                  '#b197fc', '#f783ac', '#20c997')

# Extra issue-graph series that have no per-issue token above.
ISSUE_LOGIC_ISSUES = '#38d9a9'
ISSUE_REGEX_WATCH = '#b197fc'

# Graph series key -> colour. Superset of ISSUE_COLORS with the two graph-only keys.
# The issue graph uses `user_speaking` while the panels use `user_speak`, so both
# spellings are present rather than renaming either call site.
GRAPH_SERIES_COLORS = dict(ISSUE_COLORS,
                           user_speaking=ISSUE_USER_SPEAK,
                           logic_issues=ISSUE_LOGIC_ISSUES,
                           regex_watch=ISSUE_REGEX_WATCH)

SEARCH_HIGHLIGHT_BG = '#ffd700'
SEARCH_HIGHLIGHT_FG = '#000000'

# --- Progress-bar stage colours ------------------------------------------
PROGRESS_TROUGH = SURFACE_RAISED
PROGRESS_BORDER = SURFACE_SUNKEN
PROGRESS_LOW = ('#3498db', '#2980b9', '#5dade2')
PROGRESS_MEDIUM = ('#17a2b8', '#138496', '#4dc8e8')
PROGRESS_PROGRESSING = ('#28a745', '#1e7e34', '#5cb85c')
PROGRESS_HIGH = ('#ffc107', '#e0a800', '#ffd54f')
PROGRESS_COMPLETE = ('#51cf66', '#40c057', '#69db7c')
PROGRESS_PULSE = ('#74c0fc', '#4dabf7', '#a5d8ff')
PROGRESS_ERROR = ('#ff6b6b', '#fa5252', '#ffa8a8')

# --- Typography ----------------------------------------------------------
# A font STACK, resolved at runtime (see resolve_font), never a bare family name.
UI_FONT_STACK = ('Segoe UI', 'Inter', 'Cantarell', 'DejaVu Sans')
MONO_FONT_STACK = ('Cascadia Mono', 'Consolas', 'DejaVu Sans Mono')
EMOJI_FONT_STACK = ('Segoe UI Emoji', 'Noto Color Emoji', 'DejaVu Sans')

SIZE_BODY = 10
SIZE_SMALL = 9
SIZE_HEADING = 12
SIZE_TITLE = 16
SIZE_METRIC = 22

# --- Spacing / radius ----------------------------------------------------
SPACE_XS = 4
SPACE_S = 8
SPACE_M = 12
SPACE_L = 16
RADIUS = 6
ROW_SLACK = 6             # the MIN_ROW_SLACK the table tests already use

# --- Theme ---------------------------------------------------------------
THEME_NAME = 'one-dark'

# --- Window geometry ------------------------------------------------------
# The smallest window the layout is designed to survive, at 96 DPI. The main window
# and the Config Editor both pass this to fit_window(), and the layout harness asserts
# against it (SUPPORTED_MINIMUM_AT_96_DPI). It used to be the literal (760, 520) in
# three files; raising it in one place and not the others silently invalidated the
# harness's bounds check.
MINIMUM_WINDOW_SIZE = (760, 520)
PREFERRED_WINDOW_SIZE = (1280, 900)


def resolve_font(root, stack, size, weight=None):
    """Return the first family in *stack* that Tk actually has.

    Falls back to the last entry of the stack when none of them resolve, so the
    caller always gets a usable family name. Returns a Tk font tuple
    ``(family, size)`` or ``(family, size, weight)``.
    """
    from tkinter import font as tkfont

    try:
        available = set(tkfont.families(root))
    except Exception:
        available = set()

    family = stack[-1] if stack else 'TkDefaultFont'
    for candidate in stack:
        if candidate in available:
            family = candidate
            break

    if weight:
        return (family, size, weight)
    return (family, size)


def row_height(root, font_tuple, slack=ROW_SLACK):
    """Treeview row height for *font_tuple*: its linespace plus *slack* pixels."""
    from tkinter import font as tkfont

    try:
        linespace = tkfont.Font(root=root, font=font_tuple).metrics('linespace')
    except Exception:
        linespace = 16
    return int(linespace) + int(slack)


# --- Emoji iconography ----------------------------------------------------
# The front end uses emoji as icons in buttons, card titles and checkboxes. That is
# fine where a colour-emoji font is installed and renders as tofu boxes where it is
# not, which is most bare Linux boxes and many minimal Windows images. Rather than
# delete ~100 glyphs, resolve once at startup whether the font exists and fall back to
# a plain ASCII marker when it does not.
#
# The fallback is deliberately a bracketed word or a single ASCII symbol, not a
# different emoji: a missing glyph is the problem being solved, so the substitute has
# to be something every font has.
EMOJI_FALLBACKS = {
    '\u2705': '[OK]',        # ✅
    '\u274c': '[X]',         # ❌
    '\u26a0\ufe0f': '[!]',   # ⚠️
    '\u26a0': '[!]',         # ⚠
    '\u2139\ufe0f': '[i]',   # ℹ️
    '\u2139': '[i]',         # ℹ
    '\U0001f504': '[R]',     # 🔄
    '\U0001f50d': '[?]',     # 🔍
    '\U0001f50e': '[?]',     # 🔎
    '\u2b50': '[*]',         # ⭐
    '\U0001f4cb': '[C]',     # 📋
    '\U0001f4e4': '[>]',     # 📤
    '\U0001f5d1\ufe0f': '[D]',  # 🗑️
    '\U0001f5d1': '[D]',     # 🗑
    '\U0001f4ca': '[#]',     # 📊
    '\U0001f4cc': '[P]',     # 📌
    '\U0001f4dd': '[T]',     # 📝
    '\U0001f41b': '[B]',     # 🐛
    '\U0001f6a9': '[F]',     # 🚩
    '\U0001f3f3\ufe0f': '[F]',  # 🏳️
    '\U0001f3f3': '[F]',     # 🏳 (bare, no VS16)
    '\U0001f9f9': '[C]',     # 🧹
    '\U0001f9f5': '[T]',     # 🧵
    '\U0001f522': '[N]',     # 🔢
    '\U0001f4b0': '[$]',     # 💰
    '\U0001f4c9': '[v]',     # 📉
    '\U0001f4c8': '[^]',     # 📈
    '\U0001f9e0': '[L]',     # 🧠
    '\U0001f3ad': '[A]',     # 🎭
    '\U0001f5e3\ufe0f': '[S]',  # 🗣️
    '\U0001f5e3': '[S]',     # 🗣 (bare, no VS16)
    '\U0001f6ab': '[N]',     # 🚫
    '\U0001f524': '[A]',     # 🔤
    '\U0001f5c4\ufe0f': '[DB]',  # 🗄️
    '\U0001f5c4': '[DB]',    # 🗄 (bare, no VS16)
    '\u26a1': '[~]',         # ⚡
    '\U0001f680': '[>]',     # 🚀
    '\U0001f6d1': '[#]',     # 🛑
    '\U0001f525': '[H]',     # 🔥
    '\U0001f6e1\ufe0f': '[S]',  # 🛡️
    '\U0001f6e1': '[S]',     # 🛡
    '\U0001f3a8': '[A]',     # 🎨
    '\u2699\ufe0f': '[C]',   # ⚙️
    '\u2699': '[C]',         # ⚙
    '\u23f8\ufe0f': '[||]',  # ⏸️
    '\u23f8': '[||]',        # ⏸
    '\u2795': '[+]',         # ➕
    '\u2796': '[-]',         # ➖
    '\U0001f9ea': '[T]',     # 🧪
    '\u2726': '*',           # ✦
    '\u2694': '[x]',         # ⚔
    '\u2715': '[x]',         # ✕
    '\U0001f534': '(R)',     # 🔴
    '\U0001f7e1': '(Y)',     # 🟡
    '\U0001f7e0': '(O)',     # 🟠
    '\U0001f7e2': '(G)',     # 🟢
}


def emoji_font_available(root):
    """True when a colour-emoji family is actually installed for Tk.

    Probes the same stack :data:`EMOJI_FONT_STACK` uses. Called once at startup; the
    result is cached on the root window so a per-widget call cannot re-probe.
    """
    from tkinter import font as tkfont

    # Cache on the toplevel, not on whatever widget was passed: the Config Editor
    # passes `self` (a Toplevel) and the main window passes app_state.root, and both
    # must share one answer rather than probing the font list twice.
    try:
        anchor = root.winfo_toplevel()
    except Exception:
        anchor = root
    cached = getattr(anchor, '_ui_theme_emoji_font', None)
    if cached is not None:
        return cached
    try:
        available = set(tkfont.families(anchor))
    except Exception:
        available = set()
    # The last stack entry is a plain text font and always resolves, so it must not
    # count as evidence that emoji render. Only the real emoji families do.
    result = any(name in available for name in EMOJI_FONT_STACK[:-1])
    try:
        anchor._ui_theme_emoji_font = result
    except Exception:
        pass
    return result


def icon(root, emoji):
    """Return *emoji* when the font can draw it, else its ASCII fallback.

    Every icon in the front end goes through here so a machine without a colour-emoji
    font gets readable text instead of tofu boxes.
    """
    if emoji_font_available(root):
        return emoji
    return EMOJI_FALLBACKS.get(emoji, '')


def icon_text(root, text):
    """Replace every emoji in *text* with its ASCII fallback when needed.

    The single entry point for label and button captions. When a colour-emoji font is
    installed this returns *text* unchanged, so the appearance on Windows is exactly
    what it was. Longer keys are replaced first so a multi-codepoint sequence such as
    ``\u26a0\ufe0f`` is not half-replaced by the bare ``\u26a0`` entry.
    """
    if emoji_font_available(root):
        return text
    for emoji in sorted(EMOJI_FALLBACKS, key=len, reverse=True):
        if emoji in text:
            text = text.replace(emoji, EMOJI_FALLBACKS[emoji])
    return text


def configure_global_styles(style, root):
    """Configure the app-wide ttk styles from tokens.

    Called once, right after the theme is applied. Every font here is resolved
    through :func:`resolve_font`, so the same code produces the intended family on
    Windows, Linux and macOS instead of silently substituting one.
    """
    body = resolve_font(root, UI_FONT_STACK, SIZE_BODY)
    heading = resolve_font(root, UI_FONT_STACK, SIZE_HEADING, 'bold')
    small = resolve_font(root, UI_FONT_STACK, SIZE_SMALL)
    code = resolve_font(root, MONO_FONT_STACK, SIZE_SMALL)

    # Default body text for all standard widgets
    style.configure('.', font=body)

    # Section headers, status labels, and important metrics
    style.configure('Header.TLabel', font=heading)

    # Helper text, footnotes, and secondary info
    style.configure('Small.TLabel', font=small, foreground=TEXT_MUTED)

    # Code blocks, prompts, and raw logs
    style.configure('Code.TLabel', font=code)

    # Dashboard tabs & notebook styling live in configure_notebook_styles(),
    # which needs the PhotoImages for the selected-tab underline.

    # The headline number on an Overview metric card. This style was referenced by
    # _create_ttkbs_metric() but never defined anywhere, so the number rendered at
    # the same 10pt as its caption and the "cards" were sixteen identical blobs.
    style.configure('MetricValue.TLabel',
                    font=resolve_font(root, UI_FONT_STACK, SIZE_METRIC, 'bold'),
                    foreground=TEXT,
                    background=SURFACE_RAISED)


# --- Widget style builders (phase 4) ---------------------------------------
# ttkbootstrap's variant button styles are inert on every theme tested: under
# both `superhero` and `one-dark`, TButton, Accent.TButton, primary/secondary/
# success/danger/info/outline-primary.TButton all resolve to one identical colour
# pair. The button hierarchy therefore has to be built by hand with explicit
# configure() + map() calls, which is what configure_button_styles() does.

BUTTON_STYLES = ('Primary.TButton', 'Secondary.TButton', 'Ghost.TButton',
                 'Danger.TButton')


def configure_button_styles(style, root):
    """Build the four-level button hierarchy from tokens.

    ``Primary``   filled ACCENT, white text -- the one primary action on screen.
    ``Secondary`` raised surface with a BORDER edge -- everything else that acts.
    ``Ghost``     flat, muted text -- low-emphasis and toolbar actions.
    ``Danger``    filled DANGER -- destructive actions only.
    """
    body = resolve_font(root, UI_FONT_STACK, SIZE_BODY)
    pad = [SPACE_M, SPACE_XS]

    def build(name, bg, fg, active_bg, pressed_bg, disabled_bg, disabled_fg,
              border=None, relief='flat'):
        style.configure(name, font=body, background=bg, foreground=fg,
                        bordercolor=border or bg, focuscolor=ACCENT,
                        relief=relief, padding=pad, borderwidth=1 if border else 0)
        style.map(name,
                  background=[('disabled', disabled_bg), ('pressed', pressed_bg),
                              ('active', active_bg)],
                  foreground=[('disabled', disabled_fg), ('pressed', fg),
                              ('active', fg)],
                  bordercolor=[('disabled', disabled_bg), ('active', border or bg)],
                  lightcolor=[('disabled', disabled_bg), ('pressed', pressed_bg),
                              ('active', active_bg)],
                  darkcolor=[('disabled', disabled_bg), ('pressed', pressed_bg),
                             ('active', active_bg)])

    build('Primary.TButton', ACCENT, ACCENT_TEXT, ACCENT_HOVER, ACCENT_PRESSED,
          SURFACE_DISABLED, TEXT_DISABLED)
    build('Secondary.TButton', SURFACE_RAISED, TEXT, SURFACE_SELECTED, SURFACE_SUNKEN,
          SURFACE_DISABLED, TEXT_DISABLED, border=BORDER)
    build('Ghost.TButton', SURFACE, TEXT_MUTED, SURFACE_RAISED, SURFACE_SUNKEN,
          SURFACE, TEXT_DISABLED)
    build('Danger.TButton', DANGER, TEXT_INVERSE, '#f08d83', '#d9695e',
          SURFACE_DISABLED, TEXT_DISABLED)

    # Card surfaces. A card is a raised panel with a 1px BORDER edge; the value
    # label is the big number, the title and unit are the small ones around it.
    style.configure('Card.TFrame', background=SURFACE_RAISED, relief='flat',
                    borderwidth=1, bordercolor=BORDER)
    style.configure('CardTitle.TLabel', background=SURFACE_RAISED,
                    foreground=TEXT_MUTED,
                    font=resolve_font(root, UI_FONT_STACK, SIZE_SMALL, 'bold'))
    style.configure('CardValue.TLabel', background=SURFACE_RAISED, foreground=TEXT,
                    font=resolve_font(root, UI_FONT_STACK, SIZE_METRIC, 'bold'))
    style.configure('CardUnit.TLabel', background=SURFACE_RAISED,
                    foreground=TEXT_MUTED,
                    font=resolve_font(root, UI_FONT_STACK, SIZE_SMALL))

    # LabelFrames: one global restyle instead of touching 32 call sites.
    # Colours only: adding borderwidth/relief here costs a pixel of height per
    # LabelFrame, which is enough to push the Config Editor's character page
    # selector out of view at 1024x700. The theme's own border geometry stays.
    style.configure('TLabelframe', background=SURFACE, bordercolor=BORDER)
    # Bold the theme's own label font rather than substituting a size: changing
    # the size changes the LabelFrame's height, which is what pushed the Config
    # Editor's character page selector out of view.
    from tkinter import font as _tkfont
    _lf_font = _tkfont.Font(root=root, font=style.lookup('TLabelframe.Label', 'font')
                            or 'TkDefaultFont')
    _lf_font.configure(weight='bold')
    root._ui_theme_labelframe_font = _lf_font
    style.configure('TLabelframe.Label', background=SURFACE,
                    foreground=TEXT_MUTED, font=_lf_font)


def configure_interaction_styles(style, root):
    """Give the widgets that Phase 4 did not cover a visible focus and hover state.

    The four ``*.TButton`` styles are already fully mapped by
    :func:`configure_button_styles`; this function covers the rest:

    ``TButton``
        The 47 bare ``ttk.Button`` widgets. The theme maps ``hover``/``pressed`` but has
        **no ``focus`` entry**, and its ``focuscolor`` is ``#ffffff`` -- white on a
        near-white button face, so keyboard focus is invisible. This adds a ``focus``
        entry and repoints ``focuscolor`` at ACCENT.

    ``TCheckbutton`` / ``TRadiobutton``
        Both have a completely **empty** ``map()`` on this theme and no
        ``indicatorcolor``, so they give no feedback at all -- not on hover, not on
        focus, not on press. The indicator is what the user is aiming at, so it gets the
        accent; the label brightens to TEXT on hover.

    ``TCombobox`` and ``TEntry`` are deliberately left alone: the theme already maps
    focus to ``#6693f5`` and invalid to ``#e9786d``, which is correct.
    """
    # --- Bare TButton: add the missing focus state -------------------------
    style.configure('TButton', focuscolor=ACCENT)
    style.map('TButton',
              focuscolor=[('focus', ACCENT), ('!focus', '')],
              # Keep the theme's own hover/pressed colours; only add focus.
              background=[('disabled', SURFACE_DISABLED),
                          ('pressed', '!disabled', SURFACE_SUNKEN),
                          ('hover', '!disabled', SURFACE_SELECTED)],
              foreground=[('disabled', TEXT_DISABLED)])

    # --- Check and radio buttons: hover + focus + indicator --------------
    for name in ('TCheckbutton', 'TRadiobutton'):
        style.configure(name,
                        background=SURFACE,
                        foreground=TEXT,
                        indicatorcolor=SURFACE_RAISED,
                        focuscolor=ACCENT,
                        bordercolor=BORDER)
        style.map(name,
                  # The indicator fill is what the user is aiming at.
                  indicatorcolor=[('disabled', SURFACE_DISABLED),
                                  ('selected', ACCENT),
                                  ('active', ACCENT_HOVER)],
                  background=[('active', SURFACE_RAISED)],
                  foreground=[('disabled', TEXT_DISABLED),
                              ('active', TEXT_INVERSE)],
                  focuscolor=[('focus', ACCENT), ('!focus', '')])


def configure_scrollbar_styles(style, root):
    """Make the ttk scrollbar thumb visible.

    Under ``one-dark`` the theme leaves ``background`` and ``troughcolor`` both at the
    window background (``#282c34``), so the thumb is drawn in exactly the colour of the
    trough behind it and the scrollbar is invisible. That is a defect, not a taste
    question: the only way to scroll a long table was the ``wide_scrollbar=True``
    workaround, which swaps in an explicit ``tk.Scrollbar``.

    The thumb is painted from ``darkcolor``/``lightcolor`` on the
    ``Vertical.TScrollbar.thumb`` element, **not** from ``background``, so setting
    ``background`` alone changes nothing. Both are set here.

    ``width`` is raised from the theme's 8px because an 8px target is below the
    comfortable minimum for a mouse drag.
    """
    width = 12
    for orient in ('Vertical', 'Horizontal'):
        name = f'{orient}.TScrollbar'
        style.configure(name,
                        background=SCROLL_THUMB,
                        troughcolor=SURFACE_SUNKEN,
                        bordercolor=SURFACE_SUNKEN,
                        darkcolor=SCROLL_THUMB,
                        lightcolor=SCROLL_THUMB,
                        arrowcolor=TEXT_MUTED,
                        arrowsize=width,
                        width=width,
                        relief='flat',
                        borderwidth=0)
        style.map(name,
                  background=[('pressed', SCROLL_THUMB_ACTIVE),
                              ('active', SCROLL_THUMB_ACTIVE)],
                  darkcolor=[('pressed', SCROLL_THUMB_ACTIVE),
                             ('active', SCROLL_THUMB_ACTIVE)],
                  lightcolor=[('pressed', SCROLL_THUMB_ACTIVE),
                              ('active', SCROLL_THUMB_ACTIVE)],
                  arrowcolor=[('pressed', TEXT_INVERSE), ('active', TEXT)])
    # The bare TScrollbar name is what a ttk.Scrollbar() with no style= resolves to.
    style.configure('TScrollbar', background=SCROLL_THUMB,
                    troughcolor=SURFACE_SUNKEN, bordercolor=SURFACE_SUNKEN,
                    darkcolor=SCROLL_THUMB, lightcolor=SCROLL_THUMB,
                    arrowcolor=TEXT_MUTED, width=width, relief='flat',
                    borderwidth=0)
    style.map('TScrollbar',
              background=[('pressed', SCROLL_THUMB_ACTIVE),
                          ('active', SCROLL_THUMB_ACTIVE)],
              darkcolor=[('pressed', SCROLL_THUMB_ACTIVE),
                         ('active', SCROLL_THUMB_ACTIVE)],
              lightcolor=[('pressed', SCROLL_THUMB_ACTIVE),
                          ('active', SCROLL_THUMB_ACTIVE)])


def configure_table_style(root, style, name, body_font=None, heading_font=None,
                         background=None, heading_background=None,
                         foreground=None, selected_background=None,
                         selected_foreground=None, slack=ROW_SLACK):
    """Configure one Treeview style with an explicit font and row height.

    Every table in the app goes through this helper, because ttk leaves
    ``rowheight`` empty on every theme: without it rows are a flat 20px for a 10pt
    font whose linespace is 17px, which clips ascenders and descenders.

    Returns the style name so a caller can pass it straight to ``Treeview(style=...)``.
    """
    body_font = body_font or resolve_font(root, UI_FONT_STACK, SIZE_SMALL)
    heading_font = heading_font or resolve_font(root, UI_FONT_STACK,
                                               SIZE_BODY, 'bold')
    background = background or SURFACE_RAISED
    heading_background = heading_background or SURFACE_SUNKEN
    foreground = foreground or TEXT
    selected_background = selected_background or SURFACE_SELECTED
    selected_foreground = selected_foreground or TEXT_INVERSE

    style.configure(name, background=background, fieldbackground=background,
                    foreground=foreground, font=body_font,
                    rowheight=row_height(root, body_font, slack),
                    borderwidth=0, relief='flat')
    style.configure(name + '.Heading', background=heading_background,
                    foreground=selected_foreground, font=heading_font,
                    relief='flat', borderwidth=0)
    style.map(name,
              background=[('selected', selected_background)],
              foreground=[('selected', selected_foreground)])
    return name


def configure_notebook_styles(style, root):
    """Restyle the notebook tab strip: padding scale, accent underline, muted text.

    The underline is a 2px image element added to the tab layout and mapped to the
    ``selected`` state, so only the active tab carries it. The vertical padding is
    reduced from 6 to 4 at the same time: the strip must not grow, because the
    Config Editor's page selector has to stay fully visible at 1024x700 and the
    layout harness asserts that. Measured strip height is 133px with these values
    against 135px before the underline existed.

    ``font`` and ``padding`` must both stay readable through ``style.lookup``:
    NotebookNavigator (ui_layout.py) measures them to decide when the strip has to
    collapse into a combobox, and removing either key makes the strip never collapse.
    """
    import tkinter as tk

    tab = resolve_font(root, UI_FONT_STACK, SIZE_BODY, 'bold')
    bar_height = 2

    accent_bar = tk.PhotoImage(master=root, width=1, height=bar_height)
    accent_bar.put(ACCENT, to=(0, 0, 1, bar_height))
    clear_bar = tk.PhotoImage(master=root, width=1, height=bar_height)
    clear_bar.put(SURFACE_SUNKEN, to=(0, 0, 1, bar_height))

    # Keep the images alive for the lifetime of the root: a PhotoImage that is
    # garbage collected takes its element's pixels with it.
    root._ui_theme_tab_bars = (accent_bar, clear_bar)

    style.element_create('Accent.Underline', 'image', accent_bar,
                         sticky='ew', border=0)
    style.element_create('Clear.Underline', 'image', clear_bar,
                         sticky='ew', border=0)
    style.layout('TNotebook.Tab', [
        ('Notebook.tab', {'sticky': 'nswe', 'children': [
            ('Notebook.padding', {'side': 'top', 'sticky': 'nswe', 'children': [
                ('Notebook.label', {'side': 'top', 'sticky': ''}),
            ]}),
            ('Accent.Underline', {'side': 'bottom', 'sticky': 'ew'}),
        ]}),
    ])
    style.configure('TNotebook.Tab', font=tab, padding=[SPACE_L, SPACE_XS])
    style.configure('TNotebook', font=resolve_font(root, UI_FONT_STACK, SIZE_BODY),
                    background=SURFACE, bordercolor=BORDER)
    style.map('TNotebook.Tab',
              **{'Accent.Underline_image': [('selected', accent_bar), ('', clear_bar)]},
              foreground=[('selected', ACCENT_SOFT), ('', TEXT_MUTED)],
              background=[('selected', SURFACE_RAISED), ('', SURFACE_SUNKEN)])


# --- Tab names ------------------------------------------------------------
# The dashboard notebook's tab labels live here, not as literals in generate.py and
# dashboard.py. Five separate hard-coded copies of "🔍 Review" existed before this;
# importing one constant is what stops a rename from half-breaking.
TAB_OVERVIEW = 'Overview'
TAB_TOTALS = 'Totals'
TAB_QUALITY = 'Quality'
TAB_REVIEW = 'Review'
TAB_REGEX_WATCH = 'Regex Watch'
TAB_METRICS = 'Metrics'
TAB_PROMPTS = 'Prompts'
TAB_QUEUE = 'Queue'
TAB_APIS = 'APIs'


def api_tab_name(slot_index):
    """Tab label for a 1-based API slot, e.g. api_tab_name(0) -> 'API 1'."""
    return f'API {slot_index + 1}'
