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

**Related:** [`docs/agents/config.md`](config.md), [`docs/agents/core-modules.md`](core-modules.md), [`docs/agents/generation.md`](generation.md), [`docs/agents/detection.md`](detection.md), [`docs/agents/quality.md`](quality.md), [`docs/agents/generate-ui.md`](generate-ui.md), [`docs/agents/config-editor.md`](config-editor.md), [`docs/agents/dashboard.md`](dashboard.md), [`docs/agents/vllm-metrics.md`](vllm-metrics.md), [`docs/agents/datasets.md`](datasets.md), [`docs/agents/tools-and-docs.md`](tools-and-docs.md)

## Where is my task?

Each "files to edit" row is stored ONCE, in the topic file that owns the first
module it names. So you read the task list and the file reference for one area
together. Find your task below, then open that file.

A row that names modules from other areas too is still complete — it lists every
file to edit — so you only need that one row, not the other topic files.

| Topic file | Tasks |
|------------|-------|
| [`config.md`](config.md) | Add new API slot; Add detection type; Add config option; Add slop/anti-slop phrase; Enable/disable Slop or Anti-Slop (v12.7.6); Add/modify a Regex Watch pattern; Change rate limit; Add API profile; Modify API profile params; Add character profile; Add character field; Modify opening mode prompts; Add random names feature; Add random settings pool; Add/modify persona breakthrough or emotional arc; Add/modify user-side emotional states/arcs (v10.6.0); Toggle conversation history for fixers; **Change which requests send `chat_template_kwargs` / add a per-request `enable_thinking` override (v13.0.4)** |
| [`slop-antislop-default-off.md`](slop-antislop-default-off.md) | Enable/disable Slop or Anti-Slop (v12.7.6); Change the default state of a detection layer; Change where slop/anti-slop documentation points |
| [`core-modules.md`](core-modules.md) | Fix quote balancing; Add UI metric; Add a style measurement / style metadata column; Add/modify an always-on deterministic text cleanup (v12.7.3) |
| [`generate-ui.md`](generate-ui.md) | Change the Review tab; Fix a cramped / clipped Treeview table; Call a module object (`<module>.<attr>()`) from `generate.py` / fix a `NameError` on an unbound global (v12.9.1) |
| [`vllm-metrics.md`](vllm-metrics.md) | **Add a metric to the 📊 Metrics tab (v13.1.0)**; Change how a vLLM speed or load is computed; Add a new API endpoint source to the metrics poller; Record a new slot's own token usage |
| [`config-editor.md`](config-editor.md) | Fix a label rendering one character per line (a "skyscraper"); Fix an unreadable / white-on-white spinbox; Add dataset column→alias mapping |
| [`generation.md`](generation.md) | Fix the progress bar stalling or oscillating / change how progress is counted (v13.0.5); Add/modify scene cast (v12.0); Modify output format; Add/modify output metadata field; Change what a request contains (prompt text, message order or role); Add/modify the answer prompt's native variables (v12.5, widened v12.7); Add/modify the repeated-reply guard (v12.7); Trim the duplicated continuation history (v12.7) |
| [`generation-retries.md`](generation-retries.md) | Modify retry logic; Change how Regex Watch rewrites text; Add/modify the repeated-opener guard (v12.5); Add/modify the first-reply opener caps |
| [`detection.md`](detection.md) | Add/modify the non-Latin script check (v11.10.0); Add/modify missing-quotations retry (v11.7.1, apostrophe guard v11.7.2); Add/modify the repeated-opener guard (v12.5); Add/modify the first-reply opener caps |
| [`quality.md`](quality.md) | Add quality dimension; Change judge retry/drop policy (v11.4); Change/add the judge's vLLM priority (v11.8.x); Change how the judge's JSON is parsed (v11.5); **Change the judge's thinking mode / fix a slow judge on a reasoning model (v13.0.4)** |
| [`quality-queue.md`](quality-queue.md) | Change the judge's concurrency (v12.9); Change the judge's rate-limit slot (v12.9); Change the cost of one judge conversation; Fix a judge queue stall or a `judge_failed` drop |
| [`quality-queue.md`](quality-queue.md) | **Run the judge as a queue instead of a gate (`quality.async_scoring`, v12.9); Change where/when a row is written; Fix rows lost to a judge timeout** |
| [`quality-queue.md`](quality-queue.md) | **Fix generation freezing on the judge (workers blocked waiting for a verdict); Move the per-turn logic gate off the scoring queue (v12.9.2); Fix `async_scoring` being ignored in duplication mode; Change when an async-scored task credits progress (v13.0.5)** |
| [`datasets.md`](datasets.md) | Add dataset source type; Modify dataset placeholder rendering; Add/modify dataset variables in `input/questions.txt` (v11.9.0); Change per-task row allocation |
| [`cli.md`](cli.md) | Add a CLI verb; Add a control command; Add a statistics field; Add a detection counter; Add a worker option (v13.0); Fix a `stop` that hangs or reports a working stop as a timeout; Fix `status` reporting `idle` while workers are alive; Fix a controller process that survives `shutdown` and spins at high CPU (v13.0.1); Change where output files are written/cleared; Expose the control interface over TCP |
| [`tools-and-docs.md`](tools-and-docs.md) | Add or change the license header on every file; Run dataset tests; Run GUI layout tests; Fix Gemma 4 thinking in an axolotl fine-tune (**out of scope, not this app's code**); Add/modify a cleaner-pipeline stage or rule (v12.8.0) |

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
- Add/modify the first-reply opener caps
- Modify retry logic
- Add UI metric
- Add a metric to the 📊 Metrics tab (v13.1.0)
- Change rate limit
- Change judge retry/drop policy (v11.4)
- Change/add the judge's vLLM priority (v11.8.x)
- Change how the judge's JSON is parsed (v11.5)
- Change the judge's concurrency or rate-limit slot (v12.9)
- Make the queue PROCESSOR write rows instead of gating them (`quality.async_scoring`, v12.9). The judge still only scores — it never writes a row
- Add character field
- Add random names feature
- Change how random names are dealt across a run (v12.8.1)
- Add random settings pool
- Add/modify persona breakthrough or emotional arc
- Add/modify user-side emotional states/arcs (v10.6.0)
- Toggle conversation history for fixers
- Add/modify dataset variables in `input/questions.txt` (v11.9.0)
- Enable/disable Slop or Anti-Slop (v12.7.6)
- Change the default state of a detection layer
- Change where slop/anti-slop documentation points
- Add/modify a cleaner-pipeline stage or rule (v12.8.0)
- Call a module object from `generate.py` / fix a `NameError` on an unbound global (v12.9.1)
