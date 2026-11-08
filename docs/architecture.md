<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 🏗 Architecture & Modules

```
┌─────────────────────────────┐   ┌─────────────────────────────┐
│  Tkinter GUI (Main Thread)   │   │  CLI (cli.py)                │
│  ┌──────────┐ ┌───────────┐  │   │  launch / start / stop       │
│  │ Controls  │ │ Dashboard │  │   │  stats / save / clear        │
│  │ & Toasts  │ │ & Config  │  │   │  JSON + exit codes          │
│  └──────────┘ └───────────┘  │   └──────────────┬──────────────┘
└──────────────┬──────────────┘                  │
               │  control.py: Unix socket, one   │
               │  JSON object per line          │
               └───────────────┬─────────────────┘
                               ▼
                     ┌───────────────────┐
                     │  run_config.py    │
                     │  config -> run    │
                     └─────────┬─────────┘
                               ▼
          ┌────────────┼────────────┐
          ▼            ▼            ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │ Worker 0 │ │ Worker 1 │ │ Worker N │  (Thread Pool)
    └────┬─────┘ └────┬─────┘ └────┬─────┘
         │             │             │
         ▼             ▼             ▼
    ┌──────────────────────────────────────┐
    │           Task Queue (FIFO)          │
    └──────────────────┬───────────────────┘
                       │
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
    ┌─────────┐  ┌─────────┐  ┌──────────┐
    │ API 1-4 │  │ API 5   │  │  API 6   │
    │ (Gen)   │  │(SlopFix)│  │(AntiSlop)│
    └────┬────┘  └────┬────┘  └────┬─────┘
         │             │             │
         └─────────────┼─────────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │ Circuit  │ │  Rate    │ │  Budget  │
    │ Breaker  │ │ Limiter  │ │  Check   │
    └──────────┘ └──────────┘ └──────────┘
                       │
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
    ┌─────────┐  ┌─────────┐  ┌──────────┐
    │ JSONL   │  │PostgreSQL│  │  Valkey  │
    │ Files   │  │  (Opt.)  │  │  Cache   │
    └─────────┘  └─────────┘  └──────────┘
                       │
                       ▼
    ┌──────────────────────────────────────┐
    │     Quality Scoring Engine           │
    │  (Heuristic + Optional LLM, 6 dims) │
    └──────────────────────────────────────┘
```

## 🔄 Module Architecture

The application is organized into focused modules with one-way dependencies to prevent circular imports:

| Module | Purpose | Imports From |
|--------|---------|--------------|
| `generate.py` | Main GUI, orchestration, worker thread management, startup | All modules (root of dependency tree) |
| `run_config.py` | Config -> run resolution, output/DB/JSONL plumbing, optional control server | `generation`, `quality`, `app_state`, `api_profiles`, `dataset_tasks` |
| `control.py` | Control protocol, Unix/TCP socket server, `HeadlessController` (one `cmd_*` per verb) | `run_config`, `generation`, `quality`, `app_state` |
| `cli.py` | argparse surface, socket client, daemon launcher | `control` (+ `generate` only in `_launch_gui`) |
| `generation.py` | Worker loop, LLM calls, answer generation, task processing | `api_profiles`, `app_state`, `api_handler`, `detection`, `text_utils`, `quality`, `logging_config` |
| `quality.py` | Quality scoring engine (hybrid: heuristic + optional LLM, 6 core + 3 opt-in roleplay dimensions) | `app_state`, `text_utils`, `api_handler`, `logging_config` |
| `config_editor.py` | Configuration editor GUI (tabbed interface, validation, search) | `api_profiles`, `config_loader`, `logging_config`, `api_handler` |
| `dashboard.py` | Dashboard UI (graphs, progress bars, metrics, queue view, prompt preview) | `app_state`, `api_handler`, `logging_config` |
| `vllm_metrics.py` | v13.1.0: vLLM `/metrics` reader for the 📊 Metrics tab (per-endpoint speeds, load, poller) | stdlib + `requests` (no Tk, no project modules) |
| `detection.py` | Issue detection (refusals, user speaking, slop, anti-slop, incomplete quotes, non-Latin script) | `app_state` (for timestamps, issue tracking) |
| `text_utils.py` | Text post-processing (quote normalization, reasoning removal, markdown stripping, always-on repeated-word collapse) | None (stdlib only) |
| `config_loader.py` | Configuration management, profiles, YAML load/save | `logging_config` |
| `api_handler.py` | Rate limiting, Valkey/Redis caching, response time tracking | `logging_config` |
| `api_profiles.py` | Per-endpoint payload compatibility profiles (thread-safe, auto-detection) | `logging_config` |
| `app_state.py` | Shared runtime state (counters, locks, task queue, quality scores, circuit breaker) | `config_loader` |
| `logging_config.py` | Centralized logging with colorama (colored console + file output) | None (stdlib only) |
| `tests/test_api_profiles.py` | Unit tests for API compatibility profiles | `api_profiles` |
| `tests/test_opening_mode.py` | Integration tests for opening mode (worker run against fake server) | `generation`, `app_state`, `logging_config` |
| `tests/test_persona_arc.py` | Unit tests for the persona-breakthrough + emotional-arc helpers (parsing, expansion, injection) | `generation` |
| `tests/test_non_latin.py` | Headless tests for the non-Latin script check (Unicode allow-list detector + the answer/question/continuation/opening-mode retry paths) | `detection`, `generation`, `app_state` |
| `tools/check_gui_layout.py` | GUI layout testing utility for small displays | `tkinter`, `ttkbootstrap`, `tempfile` |
| `tools/gemma4_thought_fix.py` | **OUT OF SCOPE — unrelated to this application.** Standalone axolotl helper that fixes the empty Gemma 4 thought block during fine-tuning. **Written by Karki (a user on Discord), not by this repository.** Not imported or tested by anything here; see [Gemma 4 chat template](gemma4-chat-template.md) | `yaml`, `jinja2` (+ `datasets`, `tokenizers` for `verify`) |
| `tools/cleaner_pipeline/` | **OPTIONAL, STANDALONE — not part of this application.** Nine-stage JSONL post-processor run by hand on a finished `output/*.jsonl`; not imported, not configured, not tested by anything here. Standard library only. See [Cleaner Pipeline](cleaner-pipeline.md) | none (stdlib only; `bash` for `run_all.sh`) |

**Dependency rule:** No module imports `generate.py`. The dependency graph is a tree with `generate.py` at the root, ensuring clean separation of concerns and testability. All modules import only from `logging_config` for logging, never from each other for logging functionality.

### Developer Documentation (for LLM agents and contributors)

The developer cheat sheet is split so a modest-context model can follow it in one pass:

1. Read **[`AGENTS.md`](../AGENTS.md)** — the hub. It holds only the rules that apply to every
   change: the Python 3.11 target, how to run the tests, the boundaries, the architecture
   diagram, the API slot indices and the thread-safety pattern.
2. Open **one** file in [`docs/agents/`](agents/) — the one that owns the code you are
   editing. If you know the task but not the files, start at
   [`docs/agents/task-index.md`](agents/task-index.md).

| Topic file | Covers |
|------------|--------|
| [`task-index.md`](agents/task-index.md) | "how do I add X?" → the files to edit |
| [`config.md`](agents/config.md) | `config/config.yml`, `config/api_profiles.yml`, dependency pins |
| [`core-modules.md`](agents/core-modules.md) | `api_handler.py`, `api_profiles.py`, `app_state.py`, `config_loader.py`, `logging_config.py`, `text_utils.py`, `ui_layout.py`, `style_metrics.py`, the state/lock inventory and the config paths |
| [`generate-ui.md`](agents/generate-ui.md) | `generate.py` — tabs, metric cards, issue panels, worker launch, table styles, Regex Watch tab, Metrics tab |
| [`ui-theme.md`](agents/ui-theme.md) | `ui_theme.py` — every colour, font, spacing value, tab name, emoji icon, empty state and style builder |
| [`review-tab.md`](agents/review-tab.md) | the Review tab, its count badge, the flagged-id set, and any code that identifies a tab by its label |
| [`config-editor.md`](agents/config-editor.md) | `config_editor.py` — editor fields, label wrapping, spinbox styling, panel pitfalls |
| [`dashboard.md`](agents/dashboard.md) | `dashboard.py` — metric cards, issue panels, issue-graph series |
| [`vllm-metrics.md`](agents/vllm-metrics.md) | `vllm_metrics.py` — the 📊 Metrics tab, per-endpoint vLLM speeds and load, per-slot token accounting |
| [`generation.md`](agents/generation.md) | `generation.py` — worker loop, retries, prompts, cast/arc helpers, metadata, output writing |
| [`detection.md`](agents/detection.md) | `detection.py` — refusal/slop/OOC/Regex Watch/non-Latin/scene-continuity checks |
| [`quality.md`](agents/quality.md) | `quality.py` — scoring dimensions, the judge request/JSON ladder, the drop policy |
| [`datasets.md`](agents/datasets.md) | `dataset_*.py`, `datasets_panel.py`, the dataset docs |
| [`tools-and-docs.md`](agents/tools-and-docs.md) | `tools/*.py`, `tools/cleaner_pipeline/`, `docs/gemma4-chat-template.md`, `docs/cleaner-pipeline.md`, `docs/scene-cast.md` |

Each topic file is self-contained: the file reference, the "edit for" notes, the tests that
guard those modules, and the task rows that touch them. A change that spans two areas is the
only reason to open a second file.
