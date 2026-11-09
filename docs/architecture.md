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

Three front ends drive one engine. The web UI and the CLI are equal citizens; the Tk
window is a deprecated escape hatch for one release.

```
┌──────────────────────────────────────┐
│  python -m webapp       (browser)     │
│  python -m cli          (shell)       │
│  python generate_tk.py  (deprecated)  │
└──────────────────────────────────────┘
                    │  control.py: Unix socket,
                    │  one JSON object per line
                    ▼
┌──────────────────────────────────────┐
│  run_config.py                       │
│  config → run settings               │
└──────────────────────────────────────┘
                    ▼
┌──────────────────────────────────────┐
│  Worker 0 … Worker N                 │
│  (thread pool)                       │
└──────────────────────────────────────┘
                    ▼
┌──────────────────────────────────────┐
│  Task Queue (FIFO)                   │
└──────────────────────────────────────┘
                    ▼
┌──────────────────────────────────────┐
│  API 1-4  generation                 │
│  API 5    Slop Fixer                 │
│  API 6    Anti-Slop Fixer            │
│  API 7    quality judge (optional)   │
└──────────────────────────────────────┘
                    ▼
┌──────────────────────────────────────┐
│  JSONL files                         │
│  PostgreSQL  (optional)              │
│  Valkey      (cache)                 │
└──────────────────────────────────────┘
```

The web app **owns the `HeadlessController`** and serves the control socket, so a second
controller is refused by `ControlServer._prepare_socket_path` rather than racing the first.
That is what makes the socket the singleton guard, with no lock file.

## 🔄 Module Architecture

The application is organized into focused modules with one-way dependencies to prevent
circular imports:

| Module | Purpose | Imports From |
|--------|---------|--------------|
| `webapp/` | v15.0: the web front end — FastAPI app, auth gate, in-process `HeadlessController` wrapper, the config editor and its writer, the Datasets and Profiles panels, the ES modules | `control`, `run_config`, `dataset_settings`, `config_loader`, `review_data` (never `generate_tk.py`) |
| `generate_tk.py` | **DEPRECATED** Tk GUI, orchestration, worker thread management, startup | All modules (root of dependency tree) |
| `run_config.py` | Config -> run resolution, output/DB/JSONL plumbing, optional control server | `generation`, `quality`, `app_state`, `api_profiles`, `dataset_tasks` |
| `control.py` | Control protocol, Unix/TCP socket server, `HeadlessController` (one `cmd_*` per verb) | `run_config`, `generation`, `quality`, `app_state` |
| `cli.py` | argparse surface, socket client, daemon launcher | `control` (+ `generate_tk` only in `_launch_tk`) |
| `generation.py` | Worker loop, LLM calls, answer generation, task processing | `api_profiles`, `app_state`, `api_handler`, `detection`, `text_utils`, `quality`, `logging_config` |
| `quality.py` | Quality scoring engine (hybrid: heuristic + optional LLM, 7 core + 3 opt-in dimensions) | `app_state`, `text_utils`, `api_handler`, `logging_config` |
| `review_data.py` | Flagged-conversation list, transcript detail, drop, dismiss and export for the Review screen | `app_state`, `run_config`, `quality` |
| `config_editor.py` | Configuration editor GUI (tabbed interface, validation, search) | `api_profiles`, `config_loader`, `logging_config`, `api_handler` |
| `dashboard.py` | Dashboard UI (graphs, progress bars, metrics, queue view, prompt preview) | `app_state`, `api_handler`, `logging_config` |
| `vllm_metrics.py` | v13.1.0: vLLM `/metrics` reader for the 📊 Metrics screen (per-endpoint speeds, load, poller) | stdlib + `requests` (no Tk, no project modules) |
| `detection.py` | Issue detection (refusals, user speaking, slop, anti-slop, incomplete quotes, non-Latin script) | `app_state` (for timestamps, issue tracking) |
| `text_utils.py` | Text post-processing (quote normalization, reasoning removal, markdown stripping, always-on repeated-word collapse) | None (stdlib only) |
| `config_loader.py` | Configuration management, profiles, YAML load/save | `logging_config` |
| `api_handler.py` | Rate limiting, Valkey/Redis caching, response time tracking | `logging_config` |
| `api_profiles.py` | Per-endpoint payload compatibility profiles (thread-safe, auto-detection) | `logging_config` |
| `app_state.py` | Shared runtime state (counters, locks, task queue, quality scores, circuit breaker) | `config_loader` |
| `logging_config.py` | Centralized logging with colorama (colored console + file output) | None (stdlib only) |
| `tools/check_gui_layout.py` | GUI layout testing utility for small displays | `tkinter`, `ttkbootstrap`, `tempfile` |
| `tools/gemma4_thought_fix.py` | **OUT OF SCOPE — unrelated to this application.** Standalone axolotl helper that fixes the empty Gemma 4 thought block during fine-tuning. **Written by Karki (a user on Discord), not by this repository.** Not imported or tested by anything here; see [Gemma 4 chat template](gemma4-chat-template.md) | `yaml`, `jinja2` (+ `datasets`, `tokenizers` for `verify`) |
| `tools/cleaner_pipeline/` | **OPTIONAL, STANDALONE — not part of this application.** Nine-stage JSONL post-processor run by hand on a finished `output/*.jsonl`; not imported, not configured, not tested by anything here. Standard library only. See [Cleaner Pipeline](cleaner-pipeline.md) | none (stdlib only; `bash` for `run_all.sh`) |

**Dependency rule:** No module imports `generate_tk.py`. The dependency graph is a tree
with `generate_tk.py` at the root, ensuring clean separation of concerns and testability.
All modules import only from `logging_config` for logging, never from each other for logging
functionality.

**The web front end follows the same rule, and it matters more there.** `webapp/` attaches
to `control.HeadlessController` — the same seam `cli.py` uses — so it is a *second client of
the same engine*, not a rewrite of it. It must never import `generate_tk.py`, which builds the
entire Tk window at **import** time and would make every headless path need a display;
`tests/test_web_engine.py` asserts it. `import webapp.app` must also not start a server or open
a socket, so a test can build the app without binding a port.

### The `webapp/` package

Every module here is behind the session gate except the static assets, `/login`,
`/setup`, `/logout` and `/api/health`.

| Module | Purpose |
|--------|---------|
| `webapp/app.py` | The FastAPI app: the route table, the error shape, the static mount and the lazy engine accessor |
| `webapp/__main__.py` | `python -m webapp` — takes the control socket, then hands the app to uvicorn |
| `webapp/config.py` | `WebConfig`: the two deployment shapes (`local` and `proxied`) and the loopback check |
| `webapp/engine.py` | The engine seam: `Engine`, `EngineError` and `BROWSER_COMMANDS`, the one connection to the generator. `Engine.start()` also starts the vLLM metrics poller (`vllm_metrics.start_polling`), because the web app has no Tk window to do it — without that the Metrics table fills in and every graph stays empty. `start_metrics=False` is for tests |
| `webapp/theme.py` | Design tokens and the whole stylesheet — the **Storm** palette, in one place so it cannot drift |
| `webapp/security/` | `policy.py` (password rules), `session.py` (argon2id + signed cookie), `ratelimit.py` (per-IP lockout), `routes.py` (the login/setup pages and the gate) |
| `webapp/config_tree.py` | The config tree: the storage model behind the Config editor, and the comment-preserving writer |
| `webapp/config_text.py` | A YAML writer that keeps the bytes it did not change |
| `webapp/config_routes.py` | The Config editor's routes: serve the tree, apply a patch, validate before the write |
| `webapp/config_schema.py` | The Config editor's schema: labels, help text, choice lists and the server-side validators |
| `webapp/datasets_routes.py` | The Datasets panel's routes: sources, aliases, column visibility and import |
| `webapp/profiles_routes.py` | The Profiles panel's routes: save, load (spliced) and delete a whole-config copy |
| `webapp/review_routes.py` | The Review screen's routes: list, detail, drop, dismiss and export flagged rows |
| `webapp/tabs_routes.py` | The six data screens' routes: APIs, Quality, Regex Watch, Metrics, Prompts, Queue |
| `webapp/dataset_routes.py` | The dataset itself: download it as one JSONL, clear it, list the backups |
| `webapp/static/js/` | The ES modules, one per screen: `main.js` (tab strip), `overview.js`, `review.js`, `tabs.js`, `config.js`, `datasets.js`, `profiles.js`, `chart.js`, `dom.js`, `stats.js` |

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
| [`webapp.md`](agents/webapp.md) | `webapp/` — the login, the theme, the engine seam, the config editor and its writer, the Datasets and Profiles panels, every screen |
| [`generate-ui.md`](agents/generate-ui.md) | `generate_tk.py` — **deprecated** Tk window: tabs, metric cards, issue panels, worker launch, table styles |
| [`ui-theme.md`](agents/ui-theme.md) | `ui_theme.py` — every colour, font, spacing value, tab name, emoji icon, empty state and style builder |
| [`review-tab.md`](agents/review-tab.md) | the Review screen, its count badge, the flagged-id set, and any code that identifies a screen by its label |
| [`config-editor.md`](agents/config-editor.md) | `config_editor.py` — editor fields, label wrapping, spinbox styling, panel pitfalls |
| [`dashboard.md`](agents/dashboard.md) | `dashboard.py` — metric cards, issue panels, issue-graph series |
| [`vllm-metrics.md`](agents/vllm-metrics.md) | `vllm_metrics.py` — the 📊 Metrics screen, per-endpoint vLLM speeds and load, per-slot token accounting |
| [`generation.md`](agents/generation.md) | `generation.py` — worker loop, retries, prompts, cast/arc helpers, metadata, output writing |
| [`detection.md`](agents/detection.md) | `detection.py` — refusal/slop/OOC/Regex Watch/non-Latin/scene-continuity checks |
| [`quality.md`](agents/quality.md) | `quality.py` — scoring dimensions, the judge request/JSON ladder, the drop policy |
| [`datasets.md`](agents/datasets.md) | `dataset_*.py`, `datasets_panel.py`, the dataset docs |
| [`datasets-panel-reference.md`](agents/datasets-panel-reference.md) | the Datasets subsystem's data model and module-by-module reference, for reimplementing the panel |
| [`tools-and-docs.md`](agents/tools-and-docs.md) | `tools/*.py`, `tools/cleaner_pipeline/`, `docs/gemma4-chat-template.md`, `docs/cleaner-pipeline.md`, `docs/scene-cast.md` |

Each topic file is self-contained: the file reference, the "edit for" notes, the tests that
guard those modules, and the task rows that touch them. A change that spans two areas is the
only reason to open a second file.

## 🧪 The web test suite

Thirteen headless files guard `webapp/`. None needs a display, a config file, a model or a
network connection: each builds its own app over a temporary directory.

| Test file | Guards |
|-----------|--------|
| `tests/test_web_engine.py` | the engine seam and `BROWSER_COMMANDS`, the import graph, the socket singleton guard |
| `tests/test_web_auth.py` | the password policy, the session cookie, the lockout and the route gate |
| `tests/test_web_theme.py` | the contrast table and the design tokens |
| `tests/test_web_config_tree.py` | the byte-for-byte round trip of `config/config.yml` |
| `tests/test_web_config_editor.py` | the generic renderer: every leaf reachable, no secret in a payload |
| `tests/test_web_config_schema.py` | the schema as decoration, and the validators running before a save |
| `tests/test_web_datasets.py` | the Datasets panel's routes and the names-only model |
| `tests/test_web_dataset_routes.py` | download, clear and the backup list |
| `tests/test_web_profiles.py` | the Profiles panel's save, spliced load and name safety |
| `tests/test_web_overview.py` | the Overview layout rules |
| `tests/test_web_ui_regressions.py` | the four v15.0.1 Web UI defects: the `recent` entry shape, the metrics poller, the download preflight, and the two clipped panels |
| `tests/test_web_review.py` | the Review screen's list/detail split and the drop action |
| `tests/test_web_tabs.py` | the six data screens, and that no secret reaches a payload |
| `tests/test_web_polish.py` | toasts, empty states, keyboard navigation, emoji verification |
