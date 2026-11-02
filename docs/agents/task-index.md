<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Task index (start here for "how do I add X?")

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you know WHAT you want to change and need the list of files to edit.

**Related:** [`docs/agents/config.md`](config.md), [`docs/agents/core-modules.md`](core-modules.md), [`docs/agents/generation.md`](generation.md), [`docs/agents/detection.md`](detection.md), [`docs/agents/quality.md`](quality.md), [`docs/agents/generate-ui.md`](generate-ui.md), [`docs/agents/config-editor.md`](config-editor.md), [`docs/agents/dashboard.md`](dashboard.md), [`docs/agents/datasets.md`](datasets.md), [`docs/agents/tools-and-docs.md`](tools-and-docs.md)

## Where is my task?

Each "files to edit" row is stored ONCE, in the topic file that owns the first
module it names. So you read the task list and the file reference for one area
together. Find your task below, then open that file.

A row that names modules from other areas too is still complete — it lists every
file to edit — so you only need that one row, not the other topic files.

| Topic file | Tasks |
|------------|-------|
| [`config.md`](config.md) | Add new API slot; Add detection type; Add config option; Add slop/anti-slop phrase; Enable/disable Slop or Anti-Slop (v12.7.6); Add/modify a Regex Watch pattern; Change rate limit; Add API profile; Modify API profile params; Add character profile; Add character field; Modify opening mode prompts; Add random names feature; Add random settings pool; Add/modify persona breakthrough or emotional arc; Add/modify user-side emotional states/arcs (v10.6.0); Toggle conversation history for fixers |
| [`slop-antislop-default-off.md`](slop-antislop-default-off.md) | Enable/disable Slop or Anti-Slop (v12.7.6); Change the default state of a detection layer; Change where slop/anti-slop documentation points |
| [`core-modules.md`](core-modules.md) | Fix quote balancing; Add UI metric; Add a style measurement / style metadata column; Add/modify an always-on deterministic text cleanup (v12.7.3) |
| [`generate-ui.md`](generate-ui.md) | Change the Review tab; Fix a cramped / clipped Treeview table |
| [`config-editor.md`](config-editor.md) | Fix a label rendering one character per line (a "skyscraper"); Fix an unreadable / white-on-white spinbox; Add dataset column→alias mapping |
| [`generation.md`](generation.md) | Add/modify scene cast (v12.0); Modify output format; Add/modify output metadata field; Change what a request contains (prompt text, message order or role); Add/modify the answer prompt's native variables (v12.5, widened v12.7); Add/modify the repeated-reply guard (v12.7); Trim the duplicated continuation history (v12.7) |
| [`generation-retries.md`](generation-retries.md) | Modify retry logic; Change how Regex Watch rewrites text; Add/modify the repeated-opener guard (v12.5) |
| [`detection.md`](detection.md) | Add/modify the non-Latin script check (v11.10.0); Add/modify missing-quotations retry (v11.7.1, apostrophe guard v11.7.2); Add/modify the repeated-opener guard (v12.5) |
| [`quality.md`](quality.md) | Add quality dimension; Change judge retry/drop policy (v11.4); Change/add the judge's vLLM priority (v11.8.x); Change how the judge's JSON is parsed (v11.5) |
| [`datasets.md`](datasets.md) | Add dataset source type; Modify dataset placeholder rendering; Add/modify dataset variables in `input/questions.txt` (v11.9.0); Change per-task row allocation |
| [`tools-and-docs.md`](tools-and-docs.md) | Add or change the license header on every file; Run dataset tests; Run GUI layout tests; Fix Gemma 4 thinking in an axolotl fine-tune (**out of scope, not this app's code**) |

## Cross-cutting tasks

These name modules from three or more areas. Each is stored in the first area
it names; follow the file name to read the full step list:

- Add new API slot
- Add detection type
- Add/modify the non-Latin script check (v11.10.0)
- Add/modify scene cast (v12.0)
- Add config option
- Add quality dimension
- Add/modify a Regex Watch pattern
- Change how Regex Watch rewrites text
- Add/modify missing-quotations retry (v11.7.1, apostrophe guard v11.7.2)
- Add/modify the repeated-opener guard (v12.5)
- Modify retry logic
- Add UI metric
- Change rate limit
- Change judge retry/drop policy (v11.4)
- Change/add the judge's vLLM priority (v11.8.x)
- Change how the judge's JSON is parsed (v11.5)
- Add character field
- Add random names feature
- Add random settings pool
- Add/modify persona breakthrough or emotional arc
- Add/modify user-side emotional states/arcs (v10.6.0)
- Toggle conversation history for fixers
- Add/modify dataset variables in `input/questions.txt` (v11.9.0)
- Enable/disable Slop or Anti-Slop (v12.7.6)
- Change the default state of a detection layer
- Change where slop/anti-slop documentation points
