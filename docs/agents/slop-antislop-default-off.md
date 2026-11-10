<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# Slop & Anti-Slop default-off (v12.7.6) — regression notes

`tests/test_slop_antislop_default_off.py` guards the v12.7.6 change that made
`detection.slop.enabled` and `detection.anti_slop.enabled` ship `false`. See
[`slop-and-antislop.md`](../slop-and-antislop.md) for why.

## What it pins

**Shipped config.** Both keys exist and are `false` in `config/config.yml`. The phrase and
fix lists are still present, so switching a layer on restores the saved list rather than
losing it.

**The gate in `run_config.build_run_settings`.** The test reads `run_config.py` from disk and
asserts the `settings.slop_phrases` / `settings.anti_slop_phrases` assignments are conditional
on the enabled flags, and that `settings.slop_to_anti_slop_fallback` requires **both** layers.

**The gate's behaviour.** A stub `global_config` with a populated phrase list but
`enabled: false` must produce an EMPTY phrase list, so the worker's existing
"no phrases = no detection" path skips the layer and the stats counters stay at zero.
Flipping `enabled: true` must produce the populated list again — that is the check that the
gate is a gate and not a deletion.

**The Configuration screen's fields.** `webapp/config_schema.py` declares
`detection.slop.enabled` and `detection.anti_slop.enabled` as booleans, so the screen loads
them unchecked and a save writes `enabled` back into `detection.slop` / `detection.anti_slop`.
The schema is checked headlessly; nothing here needs a display.

## Run

```sh
python -B tests/test_slop_antislop_default_off.py
```

Every check in the file is headless — no display, no model, no network.

## Edit for

Adding a third slop-like layer, or changing how a disabled layer is expressed to the worker.
