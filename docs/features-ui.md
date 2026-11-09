<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# ✨ Features — Infrastructure & UI

The dashboard, the queue, the cost controls and the web front end's screens.
The generation, quality and detection features are in [Features](features.md).

## Infrastructure & UI
- **Budget & Cost Control** — Set a spending limit per run; generation automatically stops when the budget is reached, with real-time cost tracking
- **Circuit Breaker** — Automatically disables API slots after consecutive failures with exponential backoff cooldown (60s → 120s → 240s → 480s → 600s max) and re-enables after cooldown
- **Task Requeue on Host Failure** — Tasks assigned to a downed API host are automatically requeued (up to 50 times) rather than discarded, ensuring no work is lost during outages
- **Valkey/Redis Caching** — Cache LLM responses (1-hour TTL, MD5-keyed per API slot) to avoid redundant API calls
- **PostgreSQL Storage** — Optional database backend with connection pooling, automatic initialization at startup, and JSONL export
- **Crash Recovery** — Automatic state saving/loading with configuration change detection and incompatibility warnings (now includes quality scores)
- **Configuration Profiles** — Save, load, and delete named configuration profiles
- **Web front end (v15.0.0)** — The GUI is a browser application: `python -m webapp` serves it on `127.0.0.1:8765` and drives the same `HeadlessController` the CLI uses, so it needs no display and can sit behind nginx. Eleven screens — Overview, Review, APIs, Quality, Regex Watch, Metrics, Prompts, Queue, Config, Datasets, Profiles — each fitting 1440x900 with no page scroll. See [Web Front End](webapp.md)
- **Download and clear the dataset from the browser (v15.0.0)** — **Download dataset** on the Overview action bar streams the whole dataset back as one JSONL attachment, consolidated from the per-API files through the same function `cli export` uses, so a download leaves no copy behind and cannot double the next export. **Clear dataset** deletes the stored conversations after a confirmation and takes a backup zip first, so a mistake stays recoverable. It clears without touching a run, unlike **Stop & Clear**, so a finished dataset can be cleared while the app sits idle. The pre-clear backups are listed with their sizes and timestamps
- **Password login (v15.0.0)** — One password, no username, no multi-user. Stored as an argon2id hash in a `0600` gitignored file, session as an HMAC-signed `HttpOnly` cookie with a 12-hour TTL, and weak passwords refused when they are **set**. Five failed attempts lock an address for five minutes. The first run prints a one-time setup token to the console, never over HTTP
- **The Config editor covers the whole tree (v15.0.0)** — All 1,291 leaves, not the 151 the Tk editor curated, with a 12-section left nav, labels and help text from a schema, native dropdowns for enums, and the validators running **server-side before a save** so a rejected save writes nothing. Saving splices only the changed spans, so an unchanged save is byte-identical and your comments survive
- **API Compatibility Profiles** — Per-slot payload profiles (OpenAI, Mistral, xAI, Gemini, Anthropic-compat, or user-defined) that trim the request body to what each endpoint accepts, so hosted APIs don't 400/422 on sampler params they don't support; profiles are editable data in `config/api_profiles.yml`, with URL-based auto-detection
- **API Connection Testing** — Test API connectivity directly from the configuration editor
- **Valkey Connection Testing** — Test Valkey/Redis connectivity from the main UI with detailed success/failure feedback
- **Real-Time Dashboard** — Monitor refusals, slop, errors, quality scores, and API response times with time-series graphs, search, and copy functionality
- **📊 vLLM Metrics tab (v13.1.0)** — Reads every vLLM backend's own `/metrics` endpoint and shows **prompt-processing speed**, **token-generation speed** and **endpoint load** (KV-cache usage) **per API endpoint, never aggregated**: API Slots 1-4, the Slop Fixer, the Anti-Slop Fixer and the Quality Judge each get their own row and their own graph line, with two slots on one server still listed separately (and their own measured throughput still differing). Unreachable endpoints keep their row, marked as such, and nothing is polled unless the GUI is running
- **📊 Proxy metrics for the AntiSlop-vLLM fork (v13.3.0, opt-in)** — Set `metrics.antislop_columns: true` to add **Proxy Requests**, **Bans (fixed/total)**, **Proxy Errors** and **Proxy Gen Speed** to the 📊 Metrics tab. The fork's `GET /metrics` serves its own `antislop:` counters alongside the upstream vLLM body, so one URL gives both sets of figures and the vLLM columns are unchanged. **Bans (fixed/total)** is the ban-list tuning figure: a large total with a small fixed count means the list is firing on things the model cannot avoid, and each of those costs a back-track and a restart. A fork whose upstream vLLM is down still counts as **reachable** and keeps its row, showing the proxy figures with the vLLM ones as `-`, instead of reading `unreachable`. Off by default, so a plain vLLM's tab is exactly what it was before
- **📊 The metrics history is polled every 2 s and saved to disk (v13.5.0)** — the poller used to run
  every 10 s, which was far too coarse: several requests finish between two polls, so a burst of bans
  landed as one spike and a short stall vanished. It now polls every **2 s** and keeps **300** samples
  (10 minutes), and it fetches the endpoints **concurrently** so one dead host no longer delays every
  other endpoint's sample. The history is written to **`output/metrics_history.json`** every 30 s and once
  on shutdown, so the figures survive the window closing — open the tab later, or read the file with
  `jq`, and see what the endpoints were doing when the run slowed down. `metrics.save_history: false`
  turns the file off
- **📊 The Metrics tab was overhauled in v13.4.0** — a **four-card KPI strip** above the table (**⚡ Proxy Throughput**, **🔥 Busiest Endpoint**, **🛡️ Bans Fired**, **⚠️ Errors**) answers "is this run healthy?" without reading a row, and **three more graph panels** each answer one question: **This Slot's Achieved Throughput** (our own per-slot measurement, which the server cannot provide because vLLM's `/metrics` has no caller label), **Proxy Activity** (requests, bans and back-tracks per minute) and **Requests Running vs Waiting** (a high `waiting` line with a flat `running` one means the server is queueing, so the bottleneck is admission, not the GPU). The table grew from 14 columns to 22 and now has **wide, grabbable scrollbars** — the theme's ~6px thumb could not be dragged at all. The proxy's columns also **show themselves** when a slot is actually serving `antislop:` metrics, so pointing a slot at the fork no longer needs a config edit first
- **Queue Management** — View pending, completed, and failed tasks; purge pending tasks, retry failed tasks, and export queue state to CSV
- **Live Prompt Preview** — View prompts being sent to APIs in real-time with role-based color coding, search & highlight, section copy, and auto-scroll toggle
- **Toast Notifications** — Non-blocking, auto-dismissing notification popups for status updates (success, error, warning, info)
- **Token & Cost Tracking** — Track input/output tokens and estimate API costs with budget enforcement
- **Debug Logging Toggle** — Enable/disable verbose debug logging from the UI with a single checkbox
- **Adaptive GUI Updates** — The dashboard pushes a stats tick every second over a WebSocket, and the page re-primes itself from the server after a dropped connection
- **Per-API Debug Logs** — Separate debug log files per API slot in duplication mode for easier troubleshooting (now includes quality scoring requests)
- **Progress rows per slot** — One row per API slot with a label, a track and a percentage, rather than a stack of full-width bars, so a slow slot is visible on its own instead of averaged into one bar
- **Stop & Clear Job** — Stop the current generation job, clear all progress, and reset for a fresh start
- **Force Recovery** — Bypass configuration compatibility checks and force-load a previous generation state
- **Clear Database** — Clear the PostgreSQL `generated_conversations` table from the UI with confirmation dialog
- **Export DB → JSONL** — Export stored conversations from PostgreSQL to a JSONL file (runs in background thread)
- **Rate Limit Status Display** — Real-time per-API rate limit usage indicators with color-coded status (green/orange/red)
- **API Response Times** — Average, min, max response times and sample count per API slot displayed in the UI
- **Thread Status Display** — Shows spawned and active thread counts in the metrics bar
- **Storm palette (v15.0.0)** — The web UI's palette, with every foreground/background pair measured rather than chosen by eye. Two mockup values failed WCAG AA on the raised card surface and were corrected; a test recomputes the whole contrast table from the token constants and fails on any pair under 4.5:1
- **Keyboard-navigable tabs (v15.0.0)** — Arrow keys, Home and End move between screens with the focus following, and the selected screen survives a reload
- **Toasts and empty states (v15.0.0)** — A toast dismisses, pauses while hovered or focused, caps at four, and announces a failure as `role=alert` and a success as `role=status`. Every empty table says what is empty and what to do
