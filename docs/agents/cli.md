<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# CLI / headless control (`cli.py`, `control.py`, `run_config.py`)

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are adding a CLI verb, a control command, a statistics field, or anything that touches the config→run path without the GUI.

**Related:** [`docs/agents/generate-ui.md`](generate-ui.md), [`docs/agents/config.md`](config.md), [`docs/agents/core-modules.md`](core-modules.md), [`docs/agents/generation.md`](generation.md)

## Contents

- `run_config.py`
- `control.py`
- `cli.py`

---

## The dependency rule (the one that matters)

```
cli.py  ->  control.py  ->  run_config.py  ->  generation.py / quality.py / app_state.py
generate.py (GUI)  ->  run_config.py
```

**`run_config.py`, `control.py` and `cli.py` must NEVER import `generate.py`.** `generate.py`
builds the entire Tk UI at module scope, so importing it from the control layer would make every
headless verb require a display. The one exception is `cli._launch_gui()`, which imports `generate`
for its side effect because opening the window is that command's entire job.
`tests/test_cli_control.py::test_control_layer_never_imports_the_gui_module` enforces this by
scanning the source with `ast`.

`generation.py` is already GUI-free (it never imports `generate.py`); its only GUI callback is
`app_state.live_prompt_preview_hook`, which is None-safe. `quality.py` has no Tk usage at all.

---

### `run_config.py`
**Purpose:** Resolve `config/config.yml` into a run, and own the output/DB/JSONL plumbing
**Key:** `build_run_settings()`, `RunSettings`, `validate_run_shape()`, `count_worker_threads()`, `total_threads_for_run()`, `populate_task_queue()`, `jsonl_output_files()`, `consolidate_jsonl()`, `clear_jsonl()`, `clear_database()`, `export_db_to_jsonl()`, `init_db_pool()`
**Edit for:**
- **A new worker option** → add a field to `RunSettings` and a read in `build_run_settings()`, then add the name to `WORKER_POSITIONAL_FIELDS` or `WORKER_KEYWORD_FIELDS`. Nothing else. `tests/test_cli_control.py::test_run_settings_expose_every_field_the_worker_launch_needs` fails if a declared field is never assigned.
- **A new output file kind** → add its prefix to `DERIVED_OUTPUT_PREFIXES`. That one tuple is used by BOTH `save` and `clear`, so they cannot disagree about which files are derived output. A derived file that is missing from it gets swept back in as source data and doubles the row count on every save.
- **Thread counting** → `count_worker_threads(api_configs, active_api_configs, master_duplication_enabled)` is THE one definition: it sums the per-API `threads` of the entries that will do work, and `total_threads_for_run(settings)` is a thin wrapper over it. `api.threads` is legacy and is **not** the total — it is only the fallback for when the per-slot counts are absent, so never report it as a run's worker count. Every caller that shows or uses a thread total (the GUI's `Start processing:` line and its launch loop, the CLI's `start` plan, `control.reported_thread_count()` in the status/stats payloads) must go through this helper, or the number shown stops matching the number started (that was the v13.0.2 bug: 30 threads launched, "12 threads" logged).
- **Config reads** → `build_run_settings()` reads `app_state.global_config`, which is a cache. A caller must `app_state.global_config.load()` first (the CLI's `cmd_start` does) or it reads whatever was loaded last.

### `control.py`
**Purpose:** The command protocol, the socket server, and `HeadlessController` (one `cmd_*` per verb)
**Key:** `PROTOCOL_VERSION`, `ERROR_EXIT_CODES`, `CommandError`, `StaticVar`, `build_stats_payload()`, `DETECTION_COUNTERS`, `ControlServer`, `HeadlessController`, `send_request()`, `is_server_running()`. `ControlServer` owns `_stopped` (teardown begun) and `_stopped_done` (teardown finished, released last) — see the one-accept-loop rule below
**Edit for:**
- **A new verb** → add `cmd_<verb>` to `HeadlessController` AND the name to `COMMANDS`. `handle()` dispatches on `getattr(self, f"cmd_{command}")` and rejects anything not in `COMMANDS`, so a method without a `COMMANDS` entry is unreachable over the wire (but still callable in-process, which hides the bug from a unit test that calls it directly).
- **A new error code** → add it to `ERROR_EXIT_CODES` or the CLI falls back to exit 1. Several codes deliberately share a status (all usage errors are 2; `not_running` and `no_daemon` are both 3), so do not assume the mapping is one-to-one.
- **A new statistics counter** → add a row to `DETECTION_COUNTERS`, a 6-tuple of `(key, label, total_attr, per_api_attr, recent_total_attr, recent_per_api_attr)` naming the `app_state` attributes explicitly. **Do not derive the attribute name from a stem.** The `app_state` names are inconsistent (`refusal_count_total` is singular, `ooc_count_total` is abbreviated, `logic_issue_count_total` / `regex_watch_count_total` sit next to `recent_logic_total` / `recent_regex_total`), so a derived name silently produced `per_api: {}` for four of the ten counters. `tests/test_cli_control.py::test_every_detection_counter_reports_a_populated_per_api_map` checks that every declared row resolves to a real attribute.
- **A new worker parameter** → `_worker_args(thread_id, settings)` / `_worker_kwargs(settings)`. `worker()` has 92 parameters and **no keyword-only tail**: all are `POSITIONAL_OR_KEYWORD`. The controller fills a 52-argument positional prefix and passes the rest by keyword. `tests/test_cli_control.py::test_worker_arguments_bind_to_the_real_signature` binds them with `inspect.Signature.bind()` and fails on an unknown name, a duplicate (positional + keyword), or a missing required parameter.
- **`stop` semantics** → `_join_workers()` uses ONE shared deadline for all workers and RETURNS the ones still alive. A per-thread timeout made the real budget `timeout x threads` (10 workers × 30 s = 300 s), so the client's own socket timeout fired first and reported a timeout for a stop that was working. `cmd_stop()` reports `still_stopping` instead of failing, and `phase()` treats a live worker as `stopping` rather than `idle` — a worker still writing rows means the dataset is not final.
- **TCP safety** → `ControlServer(allow_remote=False)` refuses any non-loopback bind via `_check_tcp_host()` / `_is_loopback_host()`. This is a safety limit, not a feature: the channel can TRUNCATE the database.
- **One accept loop, never two** → `ControlServer.start()` puts the ONLY `serve_forever` loop on its own thread; `ControlServer.serve_forever()` then parks the calling thread on `_stopped_done` and must **never** call `_unix_server.serve_forever()` a second time. Two loops over one `BaseServer` share a single `__shutdown_request` / `__is_shut_down` pair, so whichever exits first resets the flag and the other is left in `accept()` on a closed socket — an EINVAL spin that keeps the process at ~90% CPU forever. `stop()` sets `_stopped_done` **last**, after the socket is unlinked, so `serve_forever()` cannot return before teardown is complete. The leak is invisible to the CLI (the socket is already gone, so `status` says `no_daemon`) and only shows up as a stray process in `ps`. `tests/test_cli_control.py::test_serve_forever_returns_when_the_server_is_stopped` asserts exactly one loop is started — a timing-only assertion is not enough, because the race passes by luck roughly a third of the time.
- **The wire format** → one JSON object per line each way. `handle()` never raises; every failure becomes `{"ok": false, "error": {"code": ..., "message": ...}}`. The renderer hint is passed to `_request(..., kind=...)` as a LOCAL argument and never put on the wire.

### `cli.py`
**Purpose:** The argparse surface, the client, and the daemon launcher
**Key:** `_request()`, `_hoist_globals()`, `verb_*`, `_render_*`, `PROGRAM`
**Edit for:**
- **A new verb** → add a subparser in `build_parser()`, a `verb_<name>()`, and a `_render_<kind>()`. `_request()` returns the exit code; do not `sys.exit()` inside a verb.
- **A new global flag** → add it to `_hoist_globals()`, which moves `--json`/`--text`/`--config`/`--socket` in front of the verb. argparse only accepts them before the subcommand and an LLM cannot be relied on to remember that.
- **Output format** → JSON is the default when stdout is not a TTY; `--text` forces the table. `_kind` is stripped before `json.dumps`, and must never reach the wire.
- **A client timeout** → `_request(..., _client_timeout=...)`. For `stop` it must EXCEED the server's stop budget, or the client gives up while the server is still legitimately waiting.

---

## Tests

`tests/test_cli_control.py` — 33 headless checks, no display/network/DB. It covers config
resolution, worker-arg binding, the socket round-trip, every verb, the stats contract, exit
codes, the `_kind`-must-not-leak rule, save idempotency, stop semantics, TCP safety,
one-accept-loop-per-server, and default-GUI-unchanged.

**When a check in that file fails, first confirm the real signature or return shape with
`inspect` before changing the product.** Six of the nine initial failures were wrong assumptions
in the test, not bugs.

## Traps

- **`app_state.master_duplication_enabled_var` is `None` headlessly.** The engine calls `.get()`
  on it on every write path, so `HeadlessController` installs `control.StaticVar` before any worker
  runs. Without it every headless run raises `AttributeError` on its first successful conversation.
- **`generation.save_generation_state()` and `generation.worker()` are imported by name in
  `generate.py`**, so the control layer uses the module attribute (`generation.save_generation_state()`)
  rather than importing the symbol.
- **`api_handler.py` is CRLF.** `replace_file_content` normalises newlines and will not match it; patch
  it with a small Python script that opens in binary and writes bytes back. Keep the file CRLF.
- **The rate limiter must stay interruptible.** `RateLimiter.wait_if_needed()` sleeps in 0.25 s slices
  via `_interruptible_sleep()` and returns as soon as `app_state.stop_processing` is set. A plain
  `time.sleep()` made a Stop take up to 60 s on a rate-limited slot, so the generator looked
  unstoppable.
- **The GUI hook must never be able to stop the GUI opening.** `generate.start_control_server_if_enabled()`
  swallows every failure to a log line. A busy port must not be a startup regression.
