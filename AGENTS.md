<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# AGENTS.md - Codebase Cheat Sheet (hub)

This file is the **entry point** and stays short on purpose: it holds only the rules
that apply to every change. The per-file reference is split into small topic files
under `docs/agents/`, so you never have to hold the whole cheat sheet in context.

## How to use this doc set

1. Read this file first.
2. Open the ONE topic file below that matches what you are editing, and read only that.
3. If you know the task but not the files, open `docs/agents/task-index.md` and find
   the row. Each topic file also ends with the task rows that touch its files.
4. If a topic file is still long, read it in sections — its `## Contents` list is the map.
5. Only open a second topic file when a change genuinely spans two areas.

## Running the generator (read this before using it, not before editing it)

The topic files below are for CHANGING the code. To actually RUN a generation from a
terminal — launch it, start and stop generation, read the statistics, export or clear the
dataset — read **[`docs/cli.md`](docs/cli.md)**, the user page for the terminal front end.

The whole loop, with no display and no GUI:

```sh
python -m cli launch              # start the background controller
python -m cli start --dry-run       # validate config/config.yml, send nothing
python -m cli start                 # generate
python -m cli stats --filters-only  # what is being filtered, and why
python -m cli stop                  # stop, keep the dataset
python -m cli save --output out.jsonl # write the dataset to one file
python -m cli shutdown              # stop the run and exit the controller
```

Three things worth knowing before you type a verb:

- **JSON is the default when stdout is not a terminal**, so parse it directly; `--text`
  gives the human table and `--json` forces JSON on a terminal. The global flags
  (`--json`, `--text`, `--config`, `--socket`) work before OR after the verb.
- **The exit code is the contract**, not the wording: `0` ok, `1` failed, `2` usage,
  `3` nothing to talk to, `4` already running, `5` refused, `6` timeout. Branch on it.
- **`stats` reports "filtering" in two senses.** `detected` means caught and handled (the
  row is still written); `filtered` means dropped and never written; `detection_config`
  means which checks are armed. Do not conflate them.

`python generate.py` still opens the ordinary Tk window, and `control.enabled` is `false`
by default, so nothing above changes a default GUI run.

If you are editing the control layer rather than using it, the topic file is
[`docs/agents/cli.md`](docs/agents/cli.md) — the row in the table below.

| Topic file | Read it when |
|------------|--------------|
| [`docs/agents/task-index.md`](docs/agents/task-index.md) | you know WHAT you want to change and need the list of files to edit. |
| [`docs/agents/config.md`](docs/agents/config.md) | you are changing `config/config.yml`, `config/api_profiles.yml` or the dependency pins. |
| [`docs/agents/core-modules.md`](docs/agents/core-modules.md) | you are touching `api_handler.py`, `api_profiles.py`, `app_state.py`, `config_loader.py`, `logging_config.py`, `text_utils.py`, `ui_layout.py` or `style_metrics.py`. |
| [`docs/agents/generate-ui.md`](docs/agents/generate-ui.md) | you are touching the main Tk window: tabs, metric cards, issue panels, worker launch, table styles, the Review tab or the Regex Watch tab — **or changing any colour, font, spacing value, tab name, emoji icon or empty state, all of which live in `ui_theme.py`**. |
| [`docs/agents/config-editor.md`](docs/agents/config-editor.md) | you are adding a config field to the editor, or fixing editor layout/readability. |
| [`docs/agents/dashboard.md`](docs/agents/dashboard.md) | you are adding a metric card, issue panel or issue-graph series. |
| [`docs/agents/vllm-metrics.md`](docs/agents/vllm-metrics.md) | you are touching `vllm_metrics.py`, the 📊 Metrics tab, the per-endpoint vLLM graph, or the per-slot token accounting that feeds it. |
| [`docs/agents/generation.md`](docs/agents/generation.md) | you are touching the worker loop, prompts, cast/arc helpers, metadata or output writing. |
| [`docs/agents/generation-retries.md`](docs/agents/generation-retries.md) | you are adding or moving a check that can resend an answer (it records the retry ORDER). |
| [`docs/agents/detection.md`](docs/agents/detection.md) | you are adding or fixing a refusal/slop/OOC/Regex Watch/non-Latin/scene-continuity check, or the v12.7.6 slop/anti-slop default-off gate ([`slop-antislop-default-off.md`](docs/agents/slop-antislop-default-off.md)). |
| [`docs/agents/quality.md`](docs/agents/quality.md) | you are touching scoring dimensions, the judge request/JSON parsing or the drop policy. |
| [`docs/agents/quality-queue.md`](docs/agents/quality-queue.md) | you are touching the scoring queue, the judge's concurrency (`quality.judge_concurrency`), its rate-limit slot (`quality.scoring_api.slot_idx`) or the cost of one judge conversation. |
| [`docs/agents/datasets.md`](docs/agents/datasets.md) | you are touching `dataset_*.py`, `datasets_panel.py`, `docs/dataset-bindings.md` or `docs/input-folder.md`. |
| [`docs/agents/cli.md`](docs/agents/cli.md) | you are touching `cli.py`, `control.py`, `run_config.py`, the control socket/protocol, the `control:` config block, or the GUI's control-server hook. |
| [`docs/agents/tools-and-docs.md`](docs/agents/tools-and-docs.md) | you are touching `tools/*.py`, `docs/gemma4-chat-template.md` or `docs/scene-cast.md`. |

Cross-cutting tasks (a new detection type, a new quality dimension, scene cast) are
listed in every topic file they touch.

## Setup and Tests

The application targets Python 3.11 (see `docs/installation.md`), so `match` and PEP 604 `X | Y` unions are
allowed and preferred over `typing.Optional`/`Union`; nothing newer (PEP 695 `type`/`class Foo[T]`).
All test scripts live in `tests/` (run them from the repository root, e.g. `python -B tests/test_api_profiles.py`).
Tests that import `generation.py` (`tests/test_opening_mode.py`, `tests/test_persona_arc.py`,
`tests/test_dataset_compatibility.py`) or the Datasets panel need Tk; `tests/test_datasets_panel.py` and
`tests/test_dataset_column_menu.py` also need a display; `tests/test_review_tab_ui.py` and
`tests/test_regex_watch_tab_ui.py` need a display too (they build the real `generate.py` UI) and skip
cleanly without one. `tests/test_quality_judge_settings_ui.py`, `tests/test_missing_quotations_ui.py`
and `tests/test_user_emotional_states_ui.py` also need a display (they build the real `ConfigEditor`)
and skip cleanly without one, as does `tests/test_editor_readability_ui.py` (it also needs the
'superhero' theme, so run it under `xvfb-run`). `tests/test_questions_dataset_ui.py` (v11.9.0
questions-file dataset variables) also needs a display and skips cleanly without one.

```sh
python -m pip install -r requirements.txt            # the application
python -m pip install -r requirements-datasets.txt   # the application + Hugging Face, Parquet and saved-HF sources
```

Run tests from the repository root. Every test file runs on its own and exits non-zero on a failure; none needs a
model, a network connection or a running server. While working, run the test for the module you change, where the
topic file for that module names one. Before finishing, and before opening a pull request, run all of them and say in
the pull request which ones you ran and whether they passed:

```sh
for t in tests/test_*.py; do python -B "$t" || echo "FAILED: $t"; done
```

- `tests/test_dataset_backends.py` skips its checks unless `requirements-datasets.txt` is installed. A skip does not count
  as a pass.
- `tests/test_dataset_compatibility.py` runs the current worker twice per scenario, once with values typed in and once
  with the same values bound from CSV/JSONL sources, and requires the two runs to send identical requests. A failure
  means dataset binding changed what the worker sends.
- Importing `logging_config` creates `output/` in the working directory, and `logging_config.LOG_FILE_PATH` is
  relative to it, so `tests/test_api_profiles.py` appends to the repository's `output/log.txt`. `tests/test_opening_mode.py` and
  `tests/test_persona_arc.py` change into a temporary directory first.
- `tools/check_gui_layout.py` and `tools/check_dataset_hub.py` are not part of the test run (see `docs/agents/tools-and-docs.md`).
- `tools/gemma4_thought_fix.py` is **outside this project**: nothing here imports, reads or tests it. It is a
  standalone axolotl helper for Gemma 4 fine-tunes, documented in `docs/gemma4-chat-template.md`. **It was written
  by Karki (a user on Discord), not by this repository** — we only applied robustness fixes, so keep his credit in
  the script docstring and `--help` epilog. Do not wire it into
  the app, the tests or the dependency graph, and do not "fix" its output format to match `write_conversation()`.
- `tests/test_quality_judge_policy.py` is headless: it stubs `requests.post` and never touches the network.
- `tests/test_quality_json_robustness.py` is headless too: it stubs `requests.post` and covers the judge's JSON extraction/repair ladder, the format-repair round, truncation retry and the `response_format` fallback.
- `tests/test_progress_accounting.py` is headless and runs the real worker against a fake main-API server with a stubbed judge: it pins the v13.0.5 rule that `processed_tasks` is credited once per COMPLETED TASK, idempotently, and never refunded (see `docs/agents/generation.md`).
- `tests/test_state_save_coalescing.py` and `tests/test_prompt_trace_gate.py` are headless and pin the two v13.1.1 performance fixes: the per-task state save must COALESCE onto one background writer, and the prompt traces must be opt-in.
- `tests/test_undefined_globals.py` is headless and imports nothing: it parses the application modules with `symtable` and fails on any global a function references but its module never binds (the v12.9.1 `NameError` class). Add a new top-level module to its `CHECKED_MODULES`.

## Boundaries

- `.gitignore` covers local output: `output/` (generated rows, `log.txt`, run state), `__pycache__/`,
  `.pytest_cache/`, `layout-check/` (once the layout tool has run), and local config copies
  (`custom.yml`, `custom*.yml`, `config/profiles/`). Stage files by name; never `git add -A`
  or `git add .`.
- **Never commit an API key.** `config/config.yml` is tracked and ships with every `key: ''` empty.
- **Only `config/config.yml` and `config/api_profiles.yml` ship.** A user's `custom.yml` (or any other
  saved copy) is their own working config: it holds their character cards, prompts, lore and database
  credentials, and it must never be added to the repo. Read one only to debug a report; never copy its
  contents into a tracked file, a test, a doc or a docstring. Verify with
  `git status --porcelain` before you finish.
- **Ask before changing the wording of a shipped prompt** in `config/config.yml` (`prompts.*`). The prompts are
  short and general on purpose, and every output row depends on them.
- **New worker options go in the `kwargs=` dict** of the worker launch in `generate.py`, never in `args=` (see `docs/agents/generate-ui.md`).
- **Dataset settings hold names only.** Source selectors, column names and aliases go in `datasets.sources[]`;
  record values and credentials never do, and the Datasets panel never shows cell values (`dataset_settings.py`,
  `datasets_panel.py` docstrings).

## Core Architecture
```
generate.py (UI entry) → generation.py (worker logic) → api_handler.py (API calls)
                    ↓                              ↓
              app_state.py (shared state) ← detection.py (issue checks)
                    ↓
              dashboard.py (display updates)
```

## API Slots Index
Indexes count from 0 in code (`api_slot_idx`, `*_per_api` keys). The GUI, the README and most log messages count from 1, so index 4 is "API Slot 5"; debug-log file names (`_api_slot_<index>`) use the index.
- **0-3:** Main generation APIs (duplication mode) — API Slots 1-4
- **4:** Slop Fixer LLM — API Slot 5
- **5:** Anti-Slop Fixer LLM (independent) — API Slot 6
- **6:** Quality scoring LLM (if configured separately; `quality.scoring_api.slot_idx`) — API Slot 7

## Runtime state, locks and config paths

The counter / recent-list / timestamp / lock / flag inventory of `app_state.py`, the
thread-safety pattern and the `config_loader.py` paths are in
[`docs/agents/core-modules.md`](docs/agents/core-modules.md) — read it before you add a
counter, a lock or a timestamp key.

## More rules that are easy to break

- **Regex Watch patterns exist twice**: `config/config.yml` and
  `detection.DEFAULT_REGEX_WATCH_PATTERNS` must stay identical, and
  `style_metrics.TRAILER_PATTERN` is the one definition of the absolute-trailer
  construction (see `docs/agents/detection.md`).
- **Every request builder that sends `messages` must call
  `api_handler.apply_thinking_mode()`** (v13.0.4): the fixers, the judge and the Regex Watch
  rewrite used to send no `chat_template_kwargs`, so a reasoning model thought on every one of
  them regardless of the Samplers tab. `inherit` is the default for the auxiliary three and must
  stay so. The keys are SIBLINGS of `slop_fixer_params` / `anti_slop_params` /
  `sampler_settings`, never entries in them (those dicts use an all-or-nothing
  `params or main_sampler_settings` fallback). See `docs/agents/core-modules.md` and
  `tests/test_thinking_mode.py`.
- **A config-editor text field must be `normal` before you write into it** — this Tk
  build drops `delete`/`insert` on a disabled Text widget silently
  (see `docs/agents/config-editor.md`).
- **Keep `tools/gemma4_thought_fix.py` out of the app** — it is a third-party helper
  written by Karki (Discord), not this repository's code
  (see `docs/agents/tools-and-docs.md`).
- **Keep `tools/cleaner_pipeline/` out of the app too** — an optional, standalone JSONL
  post-processor: nothing imports it, no config key controls it, no test runs it. Its rules
  are deliberately one-corpus-specific and its warnings must stay
  (see `docs/agents/tools-and-docs.md`).
- **Keep features opt-in by default** — a default run must stay byte-identical to the
  previous release.
- **Slop/Anti-Slop are OFF by default (v12.7.6)** — `detection.{slop,anti_slop}.enabled` ship `false`
  and `generate.py` gates the phrase lists on them. Never make a phrase list its own enable condition (a
  saved list must survive a toggle). Slop docs point at the fork's **`dev`** branch, never `main`.
- **All code is AGPLv3 (never MIT/dual); every text/code file carries the SPDX header.** New
  file? `python -B tools/add_license_header.py` (`--check` to verify). The tool owns that
  block and skips Karki's script. See [`docs/licensing.md`](docs/licensing.md).

## Adding or changing docs

When a change touches a module, update the topic file that owns it (and only that
file). Add a row to `docs/agents/task-index.md` when a new kind of task becomes
possible.

**User docs are split too.** `README.md` is the master index for end users (pitch,
quick start, documentation map) and holds no reference material of its own. The
reference pages live in `docs/` — `configuration.md`, `prompts-and-characters.md`,
`detection-and-quality.md`, `samplers-and-infrastructure.md`, `usage.md`,
`output-formats.md`, `how-it-works.md`, `troubleshooting.md`, `performance.md`,
`key-configuration-details.md`, `features.md`, `architecture.md`, `installation.md`,
`file-structure.md`, `license-and-credits.md`, `cleaner-pipeline.md`, `dataset-comparison.md` (the
`depth0` vs `system` output comparison — **preliminary findings; it needs further testing to be
conclusive**, so do not cite it as settled or tune defaults to it), plus `version-history.md` over
`changelog-v12.md` / `changelog-v11.md`. A feature change updates the ONE user page
that owns it (and the matching `docs/agents/` topic file); a new user-visible feature
also needs a changelog entry. Keep `README.md` an index: if a page grows past ~25,000
characters, split it along its own headings and add the new page to the README map
and to `docs/file-structure.md`.

Size budget: keep this hub under ~10,000 characters (the mandatory license header is
not counted) and every topic file under ~18,000. The largest topic files (`config.md`, `generation.md`, `quality.md`,
`detection.md`) are near that ceiling; if one grows past it, split it along the
module lines in its `## Contents` list rather than appending more prose. Do not
re-merge the files — one big document is what this layout exists to avoid.
