<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# 📝 Changelog — v15.x

Newest first. [v14](changelog-v14.md) holds the rest of the history.

### v15.1.1
- **The Live Prompts tab was a 500, reported as a JSON parse error.** The route function was named `live_prompts`, which **shadowed the `live_prompts` module for the whole body**, so `live_prompts.snapshot(...)` called the function itself and raised `AttributeError: 'function' object has no attribute 'snapshot'`. FastAPI answered `500` with the plain-text body `Internal Server Error`, and the browser's `response.json()` turned that into `SyntaxError: Unexpected token 'I', "Internal S..." is not valid JSON` — a message about the *parser* for a bug that was never about JSON, which is why the real cause was invisible. The function is now `live_prompts_route`. **A test scans every route module for a function that shadows a module it imports**, so the whole class is caught rather than this one instance
- **A non-JSON error body no longer surfaces as a JSON parse error.** `tabs.js` called `response.json()` unconditionally, so any `500` from any route became an unreadable `SyntaxError` instead of the failure it was. `readJSON()` now checks `Content-Type` first and throws an error that names the status and the body, so the next server-side failure is legible on the first try. Both `getJSON()` and `postJSON()` go through it
- **No config key changed and nothing about what is sent to an API or written to an output file changed**, so a default run is byte-identical to v15.1.0
- **`tests/test_web_v151.py`** gains 3 checks (47 total): the shadowing scan, the two live-prompt routes answering JSON, and the client never parsing a non-JSON body. **544 web checks green**

### v15.1.0
- **Nine Web UI defects and two features, all in the browser.** No config key changed and nothing about what is sent to an API or written to an output file changed, so a default run is byte-identical to v15.0.1
- **The Metrics tab had lost the vLLM and antislop-vllm figures entirely.** It rendered only `slot_usage_rows()` -- the generator's own per-slot throughput -- so the server's `/metrics` table and the proxy's request/ban/back-track panels that the Tk tab had were simply gone. The route now also serves `vllm_metrics.endpoint_rows()`, and the tab has **two independent toggles**: *Per-slot throughput* (on by default) and *vLLM & antislop-vLLM server metrics* (off by default, because a plain vLLM serves no `antislop:` metrics and an empty panel is worse than no panel). The server toggle adds the endpoint table plus six graphs: server prompt/generation tok/s, KV cache load, running-vs-waiting, proxy requests per minute, bans and back-tracks per minute, and proxy generation speed. `vllm_available` and `antislop_available` let each panel say *why* it is empty instead of drawing empty axes
- **The y-axis numbers were squeezed together.** `chart.js` drew into a hard-coded `640x160` viewBox with `preserveAspectRatio="none"`, so a panel narrower than 640px scaled every glyph horizontally and the labels ran into each other. The viewBox is now **measured from the host** (one user unit is one CSS pixel), `preserveAspectRatio` is `xMinYMin meet`, the left gutter is sized to the widest tick label, the ticks are whole numbers, and a `ResizeObserver` redraws when the panel changes width
- **The Live Prompt Viewer is back, as its own tab.** `generation.py` already called `update_live_prompt_preview()` before every request, but the only hook that ever consumed it was `generate_tk.py`'s Text widget, so headless `app_state.live_prompt_preview_hook` stayed `None` and nothing was captured. `webapp/live_prompts.py` is a capped ring (40 entries, 120k chars each) that `Engine.start()` installs as the hook and `Engine.stop()` removes, and the new **Live Prompts** tab polls `/api/live-prompts` every two seconds. It shows the metadata header (thread, slot, type, attempt, time), the char and approximate-token counts, and every turn colour-coded by role, with a *Clear history* button. The ring keeps the reader's scroll offset, so a poll cannot throw them to the top mid-read
- **Recent issues no longer snaps to the top when it updates.** The Overview re-rendered the whole screen on every one-second stats tick, which replaced the panel and threw its scroll offset away -- so a row could not be read during a run, which is the only time the list has anything in it. `dom.captureScroll()` / `dom.restoreScroll()` save and restore the offset of every `[data-scroll-key]` scroller around the swap (restored on the next frame, with a timeout fallback for a throttled renderer), and `main.js` now calls `overview.repaintOverview()` instead of `render()`
- **Estimated Cost works.** It read `app_state.estimated_cost`, which is declared and reset but **never written by a run** -- the only code that ever set it was the Tk dashboard's own label refresh, which does not exist headless. So the card read `0.0000` forever. `control._estimated_cost()` now computes `(input + output) * cost_per_1k_tokens / 1000`, byte for byte the expression `generation.check_budget_limit()` uses, so the card and the budget stop cannot disagree. The card also formats money rather than a four-decimal number, so a tiny but real spend is not rounded to `$0.00`
- **The queue can be cleared with no run in progress.** *Stop & Clear* needs a live run (`cmd_stop` answers `not_running` when there is none) and *Purge pending* only touches `pending_task_ids`, so a queue left behind by a finished or crashed run could not be emptied from the browser at all. `POST /api/queue/clear` empties pending, completed and failed, clears `task_metadata`, and drains the in-memory `task_queue` so a stale item cannot be picked up by the next run's first worker. It never stops a run and never deletes a dataset row. There is a **Clear queue** button on the Overview action bar and on the Queue tab, both with a confirmation that names the three counts
- **Export flagged JSONL now downloads in the browser.** The button POSTed to `/api/review/export`, which wrote a file in the output directory and answered with its path -- nothing reached the browser, so from a reader's point of view it did nothing but toast a path they could not open, and on a machine reached over nginx that path is not even on their computer. `GET /api/review/export/download` exports through the same `review_data.export_flagged()` and answers with `Content-Disposition: attachment`, and the button turns the body into a Blob and clicks an anchor. An empty export is a 404 with a message, not a zero-byte attachment that looks like a successful download of nothing
- **The Queue tab no longer puts pending first.** A pending list can be thousands of rows, and it pushed the two short tables off the screen and squashed their columns into slivers. The order is now **failed, pending, completed** -- failed is the table a reader acts on -- and each table scrolls inside its own panel (`minmax(0, 1fr)` rows with a zero `flex-basis`, because a `120px` floor on three rows exceeds a short viewport and clipped the third panel)
- **The UI is no longer bare.** A soft radial wash behind the page and a hairline accent under the header, a gradient brand and a gradient primary button, an accent underline on the selected tab, a lift on card hover, a phase dot that pulses while a run is live, palette-styled scrollbars, and a `prefers-reduced-motion` block that turns the pulse and every transition off. Every value is a palette token or a `background-image` over one, so the measured contrast table is unchanged. Below 1100px the tab strip now **wraps to a second row** instead of scrolling horizontally, because twelve tabs do not fit one row at 900px and a scrolled strip hides the section you are looking for
- **`tests/test_web_v151.py`** is new: 44 checks, one per failure mode above. **541 web checks green**

### v15.0.1
- **Four Web UI defects fixed. All four were silent — the screen rendered, the data was just wrong or missing.** No config key changed and nothing about what is sent to an API or written to an output file changed, so a default run is byte-identical to v15.0.0
- **Recent issues showed the right number of rows, all blank.** The engine stored each `recent` entry as a bare tuple `(phrase, context, api_slot)` with **no timestamp at all**, and `overview.js` read `row.time` / `row.phrase` / `row.context` off it, so every cell was `undefined` and rendered as an em dash. The fix is one recorder, `app_state.record_recent()`, which appends `{time, phrase, context, api}`, trims the ring in one place instead of at each of the ~20 call sites, and is now used by every detection path in `generation.py`. `control._recent_entry()` normalises on the way out as well, so a process started before the change (still holding tuples) renders correctly instead of blanking the table
- **The Metrics graphs were empty while the table above them had data.** The table is written by the generation threads (`vllm_metrics.note_slot_usage`), but the ring the graphs draw (`slot_history`) is only appended to by the vLLM poller — and the web app never started it, because `start_polling()` lived in `generate_tk.py`. `webapp.engine.Engine.start()` now starts it (and `stop()` stops it), with a `start_metrics=False` flag so a test makes no HTTP requests
- **Download dataset was a 404 page.** The route was correct; the button navigated the whole tab to it, so a run with no rows yet replaced the app with a raw JSON error document. The button now preflights `GET /api/dataset` and says "there is no dataset to download yet" in a toast, and the 404 body itself names the next step for a direct visit
- **The Issues-over-time graph and the Review detail header were clipped.** Both were flex/grid children with no `flex: 0 0 auto` / `min-height`, so a short column squeezed them and `.panel { overflow: hidden }` cut the bottom off. The graph panel now has a `min-height` that fits the fixed 132px chart plus its legend, the side column scrolls rather than clipping, and the Review header and note no longer shrink (a long transcript had collapsed the task-id line from 23px to 7px)
- **The Tk dashboard was updated with the same change, not left behind.** `dashboard.update_modern_issue_panel` reads the same rings, so it now understands the dict shape (and still the old tuples, for a process started before the fix). Without this the change would have moved the bug from the browser to the Tk window, where every row would have read "N/A"
- **A chart now draws even when no frame arrives.** Both chart call sites drew *only* from `requestAnimationFrame`, so a background tab or a throttled renderer could leave the graph blank indefinitely. `drawWhenReady()` keeps the frame as the fast path and adds a 250ms timeout as the safety net, with a guard so the chart is never drawn twice
- **`tests/test_web_ui_regressions.py`** is new: 20 checks, one per failure mode above, including the exact legacy tuple shape that caused the blank table

### v15.0.0
- **The web UI can now get the dataset out, and can start fresh.** Two buttons on the Overview action bar: **Download dataset** streams the whole dataset back as one JSONL attachment, and **Clear dataset** deletes the stored conversations. Neither existed before — the Queue tab's Export writes the *queue*, not the dataset, and the only way to clear was `cli clear --yes` or Stop & Clear, which also stops a run. The routes are `GET /api/dataset/download`, `POST /api/dataset/clear`, `GET /api/dataset` (the file list and row counts) and `GET /api/dataset/backups`, all behind the session gate
- **Download consolidates rather than serving a file.** The rows are spread over one file per API slot, so the route goes through `run_config.consolidate_jsonl` into a temporary file and streams that, which is the same function `cli export` uses. The temporary file is deleted once the response is sent, so a download does not leave a copy behind and does not get swept into the next export. A path is never taken from the request, so there is no traversal surface
- **Clear needs `confirm`, and is not Stop & Clear.** `POST /api/dataset/clear` refuses without `confirm: true` in the body, so a stale tab cannot wipe a dataset. It takes the same `target` (`db`/`jsonl`/`both`/`state`) and `backup` arguments as `control.cmd_clear`, and takes a backup zip first unless `backup` is false — but it never touches a run, so a finished dataset can be cleared while the app is idle
- **No config key changed and no change to what is sent or written**, so a default run is byte-identical
- **The Tk entry point is retired: `generate.py` is now `generate_tk.py`.** The rename is the whole change, and it is what closes the last gap in the two-controller guard. `python generate_tk.py` still opens the old window, `cli launch --tk` still reaches it, and both carry a deprecation notice saying so — but the supported GUI is `python -m webapp`, and the Tk window is removed in the release after v15.0.0. Nothing about a run changed: the two front ends drive the same engine, so what is sent to an API and written to an output file is identical either way
- **Why the rename closes the guard.** The web app owns the `HeadlessController` and serves the control socket, so a second controller is refused by `ControlServer._prepare_socket_path`. The one hole was that the Tk window only takes the socket when `control.enabled` is true, so `python generate.py` could start a second controller beside a running web app. With the Tk entry point renamed and the flag the only way to reach it, the socket is the only way to own a controller and the guard is total
- **Every reference moved with it** — the module docstrings, the `import generate` in `cli._launch_tk()`, the `generate.py` reads in `tests/test_python311_target.py`, `test_slop_antislop_default_off.py`, `test_worker_launch.py`, `test_undefined_globals.py`, `test_ui_theme.py` and `test_cli_control.py`, and the prose in `AGENTS.md`, `docs/architecture.md`, `docs/file-structure.md`, `docs/cli.md`, `docs/usage.md`, `docs/installation.md`, `docs/licensing.md`, `docs/license-and-credits.md`, `docs/cleaner-pipeline.md`, `docs/input-folder.md`, `docs/scene-cast.md` and the `docs/agents/` topic files. The historical changelogs (`changelog-v11`..`v14`) are records and were **not** rewritten
- **Four tests had to change with it, and each was a real assertion rather than a string match:** `test_cli_control.py` now asserts `--gui` does not import `generate_tk` and `--tk` does, and its GUI-free allowlist matches `generate_tk`; `test_web_engine.py`'s `sys.modules` check now looks for `generate_tk` (it looked for `generate`, so it passed whether or not the Tk window had been imported — a green that measured nothing); `test_web_config_schema.py`'s renderer check now looks for `generate_tk` instead of stripping `generation`/`generator` out of the source first, which is the same check without the false-negative
- **No config key changed and no change to what is sent or written**, so a default run is byte-identical to v14.3.0
- **Version stays at v15.0.0** — this lands inside the v15.0.0 release, not after it
- **The Tk GUI is replaced by a web front end.** `python -m webapp` serves the whole
  application in a browser on `http://127.0.0.1:8765` — no display, no Tk, no
  `ttkbootstrap`, and the same engine underneath. `cli launch --gui` now starts this instead
  of the Tk window; `cli launch --tk` (and `python generate_tk.py`) still reach the old window
  for one deprecation release. The CLI itself is unchanged: every verb, flag, exit code and
  JSON shape is the same, and it still drives the same controller over the same socket
- **Nothing about a run changed.** `generation.py`, `detection.py`, `quality.py`,
  `api_handler.py`, `run_config.py`, `app_state.py`, `vllm_metrics.py` and
  `config/config.yml` are untouched, so a default run is byte-identical to v14.3.0. The web
  app is a **second client of the same engine**, not a rewrite of it: it owns a
  `HeadlessController` in-process and serves the control socket, exactly as the Tk window did
  when `control.enabled` was true
- **New: a password login.** One password, no username, no multi-user. It is stored as an
  **argon2id** hash in `webapp/state/auth.json` (mode `0600`, gitignored) and the session is
  an **HMAC-SHA256 signed cookie** with a 12-hour TTL — `HttpOnly`, `SameSite=Lax`, and
  `Secure` only when the request actually arrived over HTTPS. Weak passwords are refused when
  they are **set**, not only when they are tried: a minimum of 12 characters, at least two
  character classes, and a rejection list covering common passwords, one repeated character,
  keyboard/alphabet runs and the app's own name. Five failed attempts lock an address out for
  five minutes. On first start the server prints a one-time setup token **to the console** —
  never over HTTP — so an unauthenticated visitor cannot claim the instance
- **New: `webapp/`** — `app.py` (the shell and the auth gate), `engine.py` (the in-process
  controller wrapper), `theme.py` (the Storm palette and the whole stylesheet, the only place a
  hex literal is allowed), `security/` (policy, session, rate limiter, routes), the six route
  modules, and `static/` with vanilla ES modules and no build step. `requirements-web.txt`
  lists the three new dependencies (`fastapi`, `uvicorn[standard]`, `argon2-cffi`);
  `requirements.txt` is untouched, so the Tk app still installs
- **New: the Storm palette**, `#232830` base. Every foreground/background pair was measured
  rather than chosen by eye, and two values from the mockup were **corrected** because they
  failed WCAG AA on the surface they are actually drawn on: the accent `#6b9bf5` was 4.42:1 and
  the danger `#e06c75` was 3.81:1 against the raised card surface, and became `#7aa6f7`
  (5.00:1) and `#e8838a` (4.66:1). `tests/test_web_theme.py` recomputes the whole contrast
  table from the token constants and fails on any pair under 4.5:1, so the palette cannot drift
  from the measurement
- **Fixed: the "massive text" Overview.** The Tk bug was structural, not a font size: a 22px
  value inside a `wraplength=220` label inside a 180px grid cell, so `Threads: 0 spawned, 0
  active` wrapped into a block inside a card too narrow for it. The web Overview shows **six
  metric cards in one row** with the rest behind a "more metrics" disclosure, and a card sizes to
  its content while a long value ellipsizes with a `title` tooltip. Every screen fits 1440x900
  and 900x600 with no page scroll, asserted by a browser probe rather than by eye
- **New: the whole dashboard as eleven screens** (twelve from v15.1.0, with Live Prompts) — Overview (metric cards, per-slot progress,
  issue panels, issue graph, collapsible diagnostics), Review (two-pane flagged list and
  conversation detail with a colour-coded score footer, plus export/drop/dismiss), APIs, Quality,
  Regex Watch, Metrics, Prompts, Queue, Config, Datasets and Profiles. Toasts, empty states,
  keyboard-navigable tabs and a remembered tab are shared across all of them
- **New: the Config editor covers the whole tree.** The Tk editor's 151 curated keys become a
  schema-driven renderer over **all 1,291 leaves**, with a 12-section left nav instead of a tab
  strip, labels and help text from `config_schema.py`, native `<select>` for enums, and the
  validators running **server-side before a save** so a rejected save writes nothing and every
  bad field is reported at once. Saving splices only the changed spans, so an unchanged save is
  byte-identical to the file on disk and a one-key edit is a four-line diff — `yaml.dump` would
  have deleted all 113 comment lines including the SPDX header
- **New: the Datasets and Profiles panels** as dedicated screens rather than raw config groups.
  Datasets is a two-pane source list and detail with an alias editor, a column visibility picker
  (filter, Invert, Only matching / Hide matching) and import; Profiles saves and loads a copy of
  the whole config with a diff preview before a destructive load
- **Deployment is optional nginx.** Local-only on `127.0.0.1` is a first-class zero-config mode
  with no TLS, no proxy headers and no warnings. Behind nginx, `--trust-proxy` is required before
  `X-Forwarded-For` / `X-Forwarded-Proto` are believed, because both are client-spoofable; the
  `Secure` cookie flag is derived from the actual request scheme, so a local-only user never gets a
  cookie that cannot stick. A non-loopback bind without TLS is refused unless
  `--allow-insecure-http` is passed out loud
- **The two-controller hazard is closed without a lock file.** The web app starts the control
  socket, so a second `python -m webapp` and a `cli launch` are both refused with
  `already_running` by the guard `control.py` already had
- **Version bumped to v15.0.0** — a **major** bump: the front end is replaced. No config key was
  added, removed or redefaulted, and no change was made to what is sent to an API or written to an
  output file, so a default run is byte-identical to v14.3.0
