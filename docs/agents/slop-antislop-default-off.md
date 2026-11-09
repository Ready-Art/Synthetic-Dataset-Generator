<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Slop & Anti-Slop default-off (v12.7.6) — regression notes

`tests/test_slop_antislop_default_off.py` guards the v12.7.6 change that made
`detection.slop.enabled` and `detection.anti_slop.enabled` ship `false`. See
[`slop-and-antislop.md`](../slop-and-antislop.md) for why.

## What it pins

**Shipped config.** Both keys exist and are `false` in `config/config.yml`. The phrase and
fix lists are still present, so switching a layer on restores the saved list rather than
losing it.

**The gate in `generate_tk.py`.** The test reads `generate_tk.py` from disk and asserts the
`current_slop_phrases` / `current_anti_slop_phrases` assignments are conditional on the
enabled flags, and that `current_slop_to_anti_slop_fallback` requires **both** layers. It
does not import the Tk app.

**The gate's behaviour.** A stub `global_config` with a populated phrase list but
`enabled: false` must produce an EMPTY phrase list, so the worker's existing
"no phrases = no detection" path skips the layer and the dashboard counters stay at zero.
Flipping `enabled: true` must produce the populated list again — that is the check that the
gate is a gate and not a deletion.

**The Config Editor round trip** (skipped without a display). Both checkboxes default to
unchecked on load, and a save writes `enabled` back into `detection.slop` / `detection.anti_slop`.

## Run

```sh
python -B tests/test_slop_antislop_default_off.py
```

The Config Editor half needs a display and skips cleanly without one:

```sh
xvfb-run -a python3 -B tests/test_slop_antislop_default_off.py
```

## Edit for

Adding a third slop-like layer, or changing how a disabled layer is expressed to the worker.
