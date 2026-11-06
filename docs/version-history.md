<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 📝 Version History

The changelog is split by major series so the current release notes stay short.

| Series | Covers |
|--------|--------|
| [v13.x](changelog-v13.md) | Headless / CLI control: a GUI-free `run_config` + `control` + `cli` layer so the generator can be launched, driven, inspected, exported and shut down from a terminal (by a human or an LLM) against the existing `config/config.yml`, with a JSON statistics payload covering both meanings of "filtering" (v13.0), plus the v13.0.1 fix for a `shutdown` that reported success while leaving the controller process spinning, and the v13.0.2 fix for a `Start processing:` line that announced the legacy `api.threads` scalar instead of the number of threads actually launched, the v13.0.3 fix for an async-scoring write that skipped `app_state.output_data_lock`, and the v13.0.4 fix for the Slop Fixer, Anti-Slop Fixer and Quality Judge never sending `chat_template_kwargs` (so a reasoning model thought on every rewrite and score request regardless of the Samplers-tab Thinking Mode) plus the per-request `enable_thinking` overrides, and the v13.0.5 fix for progress accounting under `quality.async_scoring` (the worker refunded every async-scored task's turns, so a completed task contributed zero and the bar stalled and oscillated around 60 turns), and the v13.1.0 **📊 Metrics** dashboard tab, which reads vLLM's own `/metrics` endpoint and shows prompt-processing speed, token-generation speed and endpoint load **per API endpoint, never aggregated**, plus a per-endpoint graph, and the v13.1.1 fix for the state save that made a long run slow down in bursts (the whole state document was re-serialised synchronously on every completed task, from every worker and every judge thread, on one global lock — 0.19 s per save at 500 tasks, 4.3 s at 12,000) plus the opt-in `generation.debug_prompt_log` for the per-request prompt traces, and the v13.2.0 `{style}` variable, named by every shipped prompt and filled from a run-wide deck dealt WITHOUT replacement so about 18% of rows no longer share their first three or four words (**current: v13.2.0**) |
| [v12.x](changelog-v12.md) | The README split into per-topic pages, Scene Cast and scene continuity, the `cast_continuity` judge dimension, metadata schema v7, the duplicated-turn / repeated-opener / repeated-reply guards, the always-on repeated-word collapse, and the v12.7.6 slop/anti-slop default-off change that steers users to AntiSlop-vLLM's dev branch, the v12.8.0 optional cleaner pipeline, the v12.8.1 random-names deck, the v12.8.2 **Drop Selected** action in the Review tab, and the v12.8.3 first-reply opener caps plus the fix that had both repeat guards silently off in single-API mode, and the v12.9 configurable judge concurrency, judge rate-limit slot and async judge queue, the v12.9.1 fix for the `NameError` that made async scoring crash on Start, and the v12.9.2 fix for the per-turn logic gate freezing generation behind the judge |
| [v11.x and earlier](changelog-v11.md) | Non-Latin script check, questions-file datasets, roleplay and tic-density dimensions, judge robustness, dataset bindings, the v11 GUI work |
