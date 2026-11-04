<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 📁 File Structure

```
readyart-dataset-generator/
├── README.md                # Master README: overview + index of the docs/ pages
├── AGENTS.md                # Developer docs HUB for LLM agents (setup, tests, boundaries, architecture)
├── generate.py              # Main application (GUI, orchestration, worker threads)
├── generation.py            # Generation engine (worker loop, LLM calls, answer generation)
├── detection.py             # Issue detection (refusals, user speaking, slop, anti-slop, quotes, non-Latin script)
├── text_utils.py            # Text post-processing (reasoning removal, quote normalization, markdown stripping, em dash removal, repeated-word collapse)
├── quality.py               # Quality scoring engine (hybrid: heuristic + optional LLM, 6 core + 3 opt-in roleplay dimensions)
├── config_loader.py         # Configuration management, profiles, YAML load/save
├── api_handler.py           # Rate limiting, Valkey/Redis caching, response time tracking
├── api_profiles.py          # Per-endpoint payload compatibility profiles (thread-safe, auto-detection)
├── logging_config.py        # Centralized logging with colorama (colored console + file output)
├── config_editor.py         # Configuration editor GUI (tabbed interface with search, validation)
├── dashboard.py             # Dashboard UI (graphs, progress bars, metrics, queue view, prompt preview)
├── app_state.py             # Shared runtime state (counters, locks, task queue, quality scores, circuit breaker)
├── tests/                   # Test scripts (run from the repository root: python -B tests/<name>.py)
│   ├── test_api_profiles.py # Unit tests for API compatibility profiles
│   ├── test_opening_mode.py # Integration tests for opening mode
│   ├── test_persona_arc.py  # Unit tests for the persona-breakthrough + emotional-arc helpers
│   └── ...                  # Remaining suites: dataset_*, quality_roleplay, random_settings, worker_launch
├── docs/                    # User documentation (one page per topic; README.md is the index)
│   ├── features.md          # Everything the generator does
│   ├── architecture.md      # Runtime diagram + module dependency table
│   ├── installation.md      # Requirements, dependencies, first run
│   ├── configuration.md     # config.yml reference: datasets, API slots, generation settings
│   ├── prompts-and-characters.md  # Prompts, character engine, opening mode, samplers note
│   ├── detection-and-quality.md   # Detection, Regex Watch, scene cast, quality scoring
│   ├── slop-and-antislop.md       # Why slop/anti-slop ship off (v12.7.6) + use the fork's dev branch
│   ├── samplers-and-infrastructure.md  # Sampler params, DB/cache, budget, circuit breaker, rate limits
│   ├── usage.md             # Running a job, dashboards, queue, recovery
│   ├── output-formats.md    # ShareGPT rows + the record metadata block
│   ├── how-it-works.md      # Pipelines and fix/fallback flows
│   ├── troubleshooting.md   # Symptom -> fix table, GUI layout testing, env vars, logging
│   ├── performance.md       # Throughput and cost tuning
│   ├── key-configuration-details.md  # Injection formats and storage details
│   ├── version-history.md   # Index of the changelogs
│   ├── changelog-v12.md     # Current release notes
│   ├── changelog-v11.md     # v11.x and earlier
│   ├── license-and-credits.md  # AGPLv3, provenance, credits
│   ├── input-folder.md      # The input/ folder format
│   ├── dataset-bindings.md  # Dataset sources and {{ds:...}} variables
│   ├── scene-cast.md        # Scene Cast design notes
│   ├── dataset-comparison.md  # depth0 vs. system placement: output comparison (preliminary)
│   ├── postgresql.md        # PostgreSQL backend
│   ├── licensing.md         # Licensing reasoning
│   ├── cleaner-pipeline.md  # The optional tools/cleaner_pipeline/ JSONL post-processor
│   ├── gemma4-chat-template.md  # Out-of-scope axolotl helper
│   └── agents/              # Developer docs TOPIC FILES (read the one for your area)
│       ├── task-index.md    # "how do I add X?" -> files to edit
│       ├── generate-ui.md   # generate.py (tabs, worker launch, table styles)
│       ├── config-editor.md # config_editor.py (editor fields, layout, readability)
│       ├── dashboard.md     # dashboard.py (metric cards, panels, issue graph)
│       ├── generation.md    # generation.py (worker loop, retries, metadata)
│       ├── detection.md     # detection.py (issue checks)
│       ├── quality.md       # quality.py (scoring, judge)
│       ├── quality-queue.md # quality.py (scoring queue, judge concurrency, judge rate-limit slot)
│       ├── config.md        # config/config.yml + api_profiles.yml reference
│       ├── core-modules.md  # app_state, api_handler, config_loader, state/locks
│       ├── datasets.md      # dataset_*.py, datasets_panel.py
│       └── tools-and-docs.md # tools/*.py, user docs
├── tools/
│   ├── check_gui_layout.py  # GUI layout testing utility for small displays
│   ├── check_dataset_hub.py # Opt-in live Hugging Face Hub reader check
│   ├── gemma4_thought_fix.py # OUT OF SCOPE: axolotl helper for Gemma 4 fine-tunes by Karki (Discord); see docs/gemma4-chat-template.md
│   └── cleaner_pipeline/    # OPTIONAL standalone JSONL post-processor (not imported, not tested); see docs/cleaner-pipeline.md
│       ├── run_all.sh       # Runs the nine stages in order (--from N, $SOURCE)
│       ├── common.py        # The shared rules: tokens, control chars, whitespace, quote tests
│       ├── 01_quote_guard.py ... 09_drop_unbalanced_quotes.py  # The nine stages
│       ├── README.md        # The tool's own full reference and warnings
│       └── build/           # Scratch output, gitignored
├── config/
│   ├── config.yml           # Main configuration file
│   └── api_profiles.yml     # API compatibility profile definitions (OpenAI, Mistral, xAI, Gemini, Anthropic, custom)
├── input/
│   └── *.txt                # Source text files for chunking
├── output/
│   ├── output.jsonl         # Generated dataset (collaborative mode)
│   ├── output_api_slot_*.jsonl  # Per-API output (duplication mode)
│   ├── generation_state.json    # Crash recovery state (includes quality scores)
│   ├── quality_review_flagged.jsonl  # Exported flagged conversations
│   ├── log.txt              # Application log
│   ├── debug_prompt*.jsonl  # Per-API debug logs
│   ├── queue_export.csv     # Queue state export
│   └── output_data_backup_*.zip # Auto-backups
└── requirements.txt         # Python dependencies
```
