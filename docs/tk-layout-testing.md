<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 🐛 Tk GUI layout testing

This page covers `tools/check_gui_layout.py`, which tests the **deprecated Tk
window** only. The web UI is checked by `tests/test_web_overview.py`, which
asserts every screen fits 1440x900 and 900x600 with no page scroll.

For the symptom → fix table, see [Troubleshooting](troubleshooting.md).

## GUI Layout Testing (deprecated Tk window)

The `tools/check_gui_layout.py` utility tests the **deprecated Tk window** on various display sizes. It does not test the web UI, which is checked by `tests/test_web_overview.py` instead:

```bash
# Test with default sizes (900×600 through 1916×1126)
python tools/check_gui_layout.py --check --config-editor --screenshots

# Test specific sizes (excluding title bars)
python tools/check_gui_layout.py --check --config-editor --size 760x520 --size 1366x700

# Test with maximum character count (100 profiles)
python tools/check_gui_layout.py --check --config-editor --character-count 100 --screenshots

# Custom output directory
python tools/check_gui_layout.py --check --config-editor --output /path/to/results
```

**Features:**
- Tests primary controls, dashboard space, view selection, and scrolling
- Exercises the configuration editor's page navigation, resizing, editing, adding, removing, and searching
- Validates prompt text box behavior (mouse clicks, keyboard navigation, Ctrl+Tab focus movement)
- Records actual window sizes, fonts, Tk scaling, and dependency versions
- Generates screenshots and a detailed report in `layout-check/` (or custom `--output` path)
- Exits with nonzero code if any assertion fails (when `--check` is used)

**Notes:**
- Window minima are DPI-aware and limited to available screen area
- Use `--scale` to change Tk's pixels-per-point setting for diagnostics
- On Linux, run under `xvfb-run -a` when Xvfb is available for headless testing
- Application configuration and generation state are not modified

This opens the actual GUI with temporary fixture configuration, exercises idle,
single-API, and four-API progress layouts, and resizes from 900×600 through
1024×700, 1366×700, and 1916×1126 before returning to the smallest size. It checks
primary controls, dashboard space, view selection, scrolling, and the editor's
Save/Validate footer. The editor fixture includes 10 character profiles and checks
page navigation, resizing, editing, adding, removing, searching, and reloading.
Prompt checks exercise mouse clicks and text editing without moving the outer
form, as well as keyboard navigation to fields outside the visible area.
It does not start generation or connect to APIs/databases.
Application configuration and generation state are not saved.

The report and optional screenshots are written to `layout-check/`; use
`--output PATH` to choose another directory. `--check` exits with a nonzero code
if an assertion fails. To target particular client sizes (excluding title bars),
repeat `--size`, for example:

```bash
python tools/check_gui_layout.py --check --config-editor --size 760x520 --size 1366x700
```

Clicking a prompt box leaves the outer form in place so the insertion cursor
stays at the clicked text. Keyboard navigation still brings the destination
control into view. In a prompt text box, Ctrl+Tab moves focus forward and
Ctrl+Shift+Tab moves it backward; Tab retains its text-editing behavior.

Character Profiles displays up to three cards per page. Use Previous, Next, or
the page selector to reach the remaining cards; unsaved edits are retained when
changing pages, and Save includes every profile. Adding a character opens its
page. This bounds the height of native drawing surfaces: stacking large numbers
of full cards in one frame can terminate Tk on Linux/X11 with
`BadAlloc` or `BadValue` from `X_CreatePixmap`, even when the visible window is
small. To test the supported maximum collection size:

```bash
python tools/check_gui_layout.py --check --config-editor --character-count 100 --screenshots
```

`--character-count` accepts 1–100 and uses synthetic profiles in temporary files;
it does not load or change your saved characters. The report records the largest
native widget dimensions on each page as well as checking that every profile is
reachable and that off-page edits are included when gathering configuration data.

Window minima are DPI-aware and limited to the available screen area. The report
records actual sizes, fonts, Tk scaling, and dependency versions, so a clamped
size is not mistaken for a successful test at the requested size. `--scale`
changes Tk's pixels-per-point setting for diagnostics; it does not emulate
Windows display scaling. Validate on Windows at the affected display scaling,
including moving the application between monitors, before claiming native DPI
coverage. On Linux, the same check can run under `xvfb-run -a` when Xvfb is
installed.
