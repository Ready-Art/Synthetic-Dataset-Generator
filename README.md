<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# ReadyArt Synthetic Dataset Generator v15.0.1

A multi-threaded application for generating high-quality synthetic conversational datasets from LLM APIs. It runs in your browser (`python -m webapp`) or headless from a shell (`python -m cli`). Both front ends drive the same engine against the same `config/config.yml` and write the same output, so a run is identical either way.

The engine orchestrates up to six API slots — four generation slots plus a dedicated Slop Fixer and Anti-Slop Fixer, with an optional seventh for the quality judge — in **duplication mode** (the same conversation generated across several models, for dataset diversity) or **collaborative mode** (one shared queue, for throughput). Each slot carries its own thread count, rate limit and payload compatibility profile, behind a circuit breaker, budget enforcement and Valkey/Redis response caching.

Around that engine sit three layers:

- **Detection & repair** — out-of-character replies and scene-continuity violations are caught and regenerated out of the box; refusal, user-impersonation, non-Latin-script, foreign-word, Regex Watch, slop and anti-slop checks are configurable and ship off or with empty phrase lists. Unbalanced quotes are repaired deterministically, and the slop and anti-slop fixers can rewrite a flagged reply through their own API slots.
- **A character engine** — multi-character scenes with a generator-owned cast schedule (characters join and leave on a plan the model only narrates), emotional states and multi-turn arcs, persona breakthrough, lore, and a run-wide `{style}` deck dealt without replacement so openings stop repeating across rows.
- **A hybrid quality scorer** — heuristic measurements plus an optional LLM judge over seven core dimensions (coherence, naturalness, engagement, diversity, consistency, logic, technical), with opt-in roleplay, tic-density and cast-continuity dimensions; below-threshold conversations are flagged in the Review screen or dropped from the dataset.

Inputs come from `input/*.txt` or from bound dataset sources (CSV, TSV, JSON, JSONL, Parquet, Hugging Face) mapped to prompt variables. Output is trainer-safe ShareGPT JSONL with a flat provenance-and-quality `metadata` block, optionally mirrored to PostgreSQL. Real-time dashboards cover throughput, cost, issues, quality scores and per-endpoint vLLM metrics, and the run survives a crash through state save/resume.

**LICENSE:** All code in this repository is **AGPLv3**, and has been for some time. Very early versions were MIT by Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that it is unlikely any of that code survived. Unless you have the original MIT-era code to compare against, treat every file here as AGPLv3-only. Every text and code file carries an SPDX header saying so. See [License & Credits](docs/license-and-credits.md).

---

## 🚀 Quick Start

1. **Install** — Python 3.11+ and the dependencies. The web UI needs both requirement files:

   ```bash
   python -m pip install -r requirements.txt -r requirements-web.txt
   ```

   Full details in [Requirements & Installation](docs/installation.md).
2. **Start it** — `python -m webapp`, then open `http://127.0.0.1:8765`. The first start prints a one-time setup token to the console; paste it and choose a password. See [Web Front End](docs/webapp.md).
3. **Add input** — drop `.txt` files into `input/`. The format is described in [the `input/` folder](docs/input-folder.md).
4. **Configure** — use the **Config** screen in the browser, or edit `config/config.yml`. Start with the API slots in [Configuration](docs/configuration.md).
5. **Run** — press **Start Generation**. Usage, dashboards and crash recovery are in [Usage](docs/usage.md).
6. **Collect output** — **Download dataset** on the Overview action bar streams the whole dataset back as one JSONL file; it also lands in `output/`, plus an optional PostgreSQL table. **Clear dataset** deletes it again (after a confirmation, with a backup zip taken first) so you can start fresh without stopping a run. The row format and the metadata block are in [Output Formats](docs/output-formats.md).

Prefer a terminal? Every step above has a headless equivalent — see [Command Line](docs/cli.md).

---

## 📚 Documentation Map

The README is a master index: each topic below lives in its own file under [`docs/`](docs/), so you can read only the page you need.

### Getting started

| Page | What it covers |
|------|----------------|
| [Features](docs/features.md) | Generation, quality scoring, detection, the character engine, post-processing, dataset sources, metadata |
| [Features — Infrastructure & UI](docs/features-ui.md) | The dashboard, the queue, cost controls, crash recovery, and the web front end's screens |
| [Requirements & Installation](docs/installation.md) | Python/PostgreSQL/Valkey requirements, both dependency files, first run, `input/` setup |
| [Configuration](docs/configuration.md) | `config/config.yml`: dataset sources and prompt variables, the 6 API slots and compatibility profiles, generation settings |
| [Usage](docs/usage.md) | Starting a run, resuming and crash recovery, duplication vs. collaborative mode, dashboards, queue management, prompt preview, connection tests |
| [Web Front End](docs/webapp.md) | The browser UI: `python -m webapp`, the password login, the eleven screens, the Config editor, nginx vs. local-only, and what to do when it will not start |
| [Command Line](docs/cli.md) | Headless / CLI control: launch, start, stop, pause, stats (including what is being filtered), save/export, clear, shutdown. JSON output and exit codes for scripts and LLM agents, plus the `control:` config block |

### Configuration reference

| Page | What it covers |
|------|----------------|
| [Prompts & Character Engine](docs/prompts-and-characters.md) | Question/answer/continuation prompts, opening mode, character profiles, emotional states and arcs, persona breakthrough, lore, variable system prompts |
| [Dataset Comparison: depth-0 vs. system](docs/dataset-comparison.md) | What the `depth0` and `system` placements actually do to the generated prompts: a side-by-side of two 287-conversation sets. **Preliminary** — needs further testing to be conclusive, but initial results show promise |
| [Detection & Quality Control](docs/detection-and-quality.md) | Refusal and user-speaking detection, slop and anti-slop, Regex Watch, Scene Cast, the non-Latin script check, quality scoring and its opt-in dimensions |
| [Slop & Anti-Slop — Use the Fork](docs/slop-and-antislop.md) | Why the built-in slop/anti-slop functions are off by default, twelve reasons to use AntiSlop-vLLM's dev branch instead, and when the built-in path is still the right call |
| [Samplers & Infrastructure](docs/samplers-and-infrastructure.md) | Sampler parameters, database and Valkey caching, budget and cost control, the circuit breaker, rate limiting |
| [Key Configuration Details](docs/key-configuration-details.md) | How character/persona/emotion/lore blocks are injected, the question cache, per-API debug logs, quality score storage |

### Output and internals

| Page | What it covers |
|------|----------------|
| [Output Formats](docs/output-formats.md) | ShareGPT rows and the flat record `metadata` block (provenance + quality), trainer-safe by design |
| [Cleaner Pipeline](docs/cleaner-pipeline.md) | **Optional** standalone post-processor in `tools/cleaner_pipeline/`: nine inspectable stages that strip chat-template sentinels, invisible characters and double-encoded text, drop foreign-script and unbalanced-quote records, and repair truncated turns in a finished `.jsonl`, writing a report for every decision. Not part of the app; read its warnings first |
| [How It Works](docs/how-it-works.md) | The generation and quality pipelines, multi-character mode, slop/anti-slop fix flows, quote repair, circuit breaker, requeue, budget enforcement, malformed-response handling |
| [Architecture & Modules](docs/architecture.md) | Runtime diagram, the module dependency table, and the developer documentation index |
| [File Structure](docs/file-structure.md) | The full repository layout, including `docs/` and the per-module developer notes |

### Help

| Page | What it covers |
|------|----------------|
| [Troubleshooting](docs/troubleshooting.md) | Symptom → fix table for the engine, the web UI and the dataset, the Gemma 4 template helper, environment variables, debug logging |
| [Tk GUI layout testing](docs/tk-layout-testing.md) | `tools/check_gui_layout.py` and its flags, for the deprecated Tk window only |
| [Performance Tips](docs/performance.md) | Throughput, caching, database choice, and cost tuning |

### Project

| Page | What it covers |
|------|----------------|
| [Version History](docs/version-history.md) | Index of the changelogs |
| [Changelog — v15.x](docs/changelog-v15.md) | Current release notes |
| [Changelog — v14.x](docs/changelog-v14.md) | v14.x release notes |
| [Changelog — v13.x](docs/changelog-v13.md) | v13.x release notes |
| [Changelog — v12.x](docs/changelog-v12.md) | v12.x release notes |
| [Changelog — v11.x and earlier](docs/changelog-v11.md) | The rest of the history |
| [License & Credits](docs/license-and-credits.md) | AGPLv3, the early MIT provenance, file headers, credits, third-party code |

Deeper, task-specific pages also live in `docs/`:

- [`input-folder.md`](docs/input-folder.md) — the accepted format of `input/*.txt`
- [`dataset-bindings.md`](docs/dataset-bindings.md) — dataset sources and `{{ds:alias}}` variables
- [`docs/agents/datasets-model.md`](docs/agents/datasets-model.md) — the Datasets subsystem's data model and pure model layer, for anyone changing it
- [`scene-cast.md`](docs/scene-cast.md) — the Scene Cast design notes
- [`dataset-comparison.md`](docs/dataset-comparison.md) — what `depth0` vs. `system` placement does to the generated prompts, from a two-dataset comparison (preliminary: promising initial results, needs further testing to be conclusive)
- [`postgresql.md`](docs/postgresql.md) — the optional database backend
- [`licensing.md`](docs/licensing.md) — the licensing reasoning in detail
- [`slop-and-antislop.md`](docs/slop-and-antislop.md) — why slop/anti-slop are off by default and where to run them instead, carrying the dev-branch warning
- [`gemma4-chat-template.md`](docs/gemma4-chat-template.md) — the out-of-scope axolotl helper
- [`cleaner-pipeline.md`](docs/cleaner-pipeline.md) — the optional `tools/cleaner_pipeline/` JSONL post-processor

---

## 🏗 At a Glance

Three front ends drive one engine. The browser and the terminal are equal citizens: `python -m webapp` serves the whole application at `http://127.0.0.1:8765`, `python -m cli` drives the same run from a shell, and both read the same `config/config.yml` and write the same output. `python generate_tk.py` still opens the old Tk window for one deprecation release, but it is no longer the supported GUI.

```
┌──────────────────────────────────────┐
│  python -m webapp       (browser)    │
│  python -m cli          (shell)      │
│  python generate_tk.py  (deprecated) │
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

The web app owns the `HeadlessController` and serves the control socket, so a second controller is refused rather than racing the first. The full diagram, the module table and the dependency rule are in [Architecture & Modules](docs/architecture.md). See [Command Line](docs/cli.md) for the terminal front end.

---

## 🧑‍💻 Contributing

- **Developers / LLM agents:** start at [`AGENTS.md`](AGENTS.md), the short hub. It holds only the rules that apply to every change and a table telling you which topic file in [`docs/agents/`](docs/agents/) to open for the code you are editing.
- **Tests:** run them from the repository root, e.g. `python -B tests/test_api_profiles.py`, or all of them with `for t in tests/test_*.py; do python -B "$t" || echo "FAILED: $t"; done`.
- **License headers:** `python -B tools/add_license_header.py` writes or refreshes them; `--check` exits non-zero if any file lacks one.

---

## 📜 License

All code in this repository is **AGPLv3**, and has been for some time. See [`LICENSE`](LICENSE) for the full text, [`LICENSE-AGPLv3`](LICENSE-AGPLv3) for the project notice, and [License & Credits](docs/license-and-credits.md) for the early-MIT provenance, the file-header policy, the credits and the third-party code.
