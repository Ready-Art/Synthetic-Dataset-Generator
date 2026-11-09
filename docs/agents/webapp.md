<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# `webapp/` — the web front end

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules.

**Read this when:** you are touching anything under `webapp/`, the login, the theme, the config
editor's routes or its writer, the Datasets or Profiles panel, or the `cli launch --gui` branch.

**Related:** [`docs/agents/cli.md`](cli.md) (the socket and the controller the web app owns),
[`docs/agents/config.md`](config.md) (the config file it edits),
[`docs/agents/config-editor.md`](config-editor.md) (the Tk editor this replaced),
[`docs/agents/ui-theme.md`](ui-theme.md) (the Tk token module this succeeded).

## Contents

- [The one rule that matters](#the-one-rule-that-matters)
- [The file map](#the-file-map)
- [The engine seam](#the-engine-seam)
- [The auth gate](#the-auth-gate)
- [The theme](#the-theme)
- [The config writer](#the-config-writer)
- [The tests](#the-tests)
- [Traps](#traps)

## The one rule that matters

**The web app is a second client of the same engine, not a rewrite of it.** It constructs a
`HeadlessController` in-process and serves the control socket, exactly as `generate_tk.py` did when
`control.enabled` was true. Nothing about a run — what is sent to an API, what is written to an
output file, any config default — is allowed to change. A default run must stay byte-identical to
v14.3.0.

Two corollaries, both load-bearing:

- **Never import `generate_tk.py`.** It builds the entire Tk window at *import* time, so importing it
  here would make every headless path need a display. `tests/test_web_engine.py` asserts it by
  inspecting `sys.modules` and by grepping the source.
- **`import webapp.app` must not start a server, open a socket or touch the network.** A test
  builds the app and asserts nothing bound.

## The file map

| File | What it is |
|------|------------|
| `webapp/__main__.py` | The only place that starts uvicorn. Parses the flags, validates the combination, prints the setup token, takes the socket **before** binding. |
| `webapp/app.py` | `create_app()` — the shell, the static mount, `/api/health`, `/`, `/api/stats`, `/api/cmd`, `/ws/stats`, and the auth install. |
| `webapp/config.py` | `WebConfig`: pure data plus `check()`, `mode`, `url()`. Importing it binds nothing. |
| `webapp/engine.py` | `Engine`, the thin in-process wrapper over `HeadlessController`. `BROWSER_COMMANDS` is the allow-list. |
| `webapp/theme.py` | The Storm palette, the type scale and the whole `STYLESHEET`. **The only file allowed a hex literal.** |
| `webapp/security/policy.py` | The password policy. Runs when a password is **set**. |
| `webapp/security/session.py` | argon2id hash, HMAC-signed cookie, TTL, constant-time compare, `cookie_secure()`. |
| `webapp/security/ratelimit.py` | Per-IP failure counter and lockout. |
| `webapp/security/routes.py` | `/login`, `/setup`, `/logout`, and `SessionGate` — a **middleware**, not a per-route dependency. |
| `webapp/review_routes.py` | The Review tab's six routes, including `GET /export/download` — the one the Export button uses, because a POST that writes a server file is not a download. |
| `webapp/tabs_routes.py` | The seven data tabs, plus the four Queue actions and the live-prompt routes. |
| `webapp/live_prompts.py` | The Live Prompt Viewer's capped ring. `Engine.start()` installs `record()` as `app_state.live_prompt_preview_hook`; `Engine.stop()` removes it. |
| `webapp/config_routes.py` | The config tree, the save and the delete. The only route that **writes** the config. |
| `webapp/config_tree.py` | Load/get/set/add/delete by dot-path. |
| `webapp/config_text.py` | The comment-preserving writer. Splices changed spans; never `yaml.dump`. |
| `webapp/config_schema.py` | Labels, help, enums and the validators. A missing entry degrades to a generic render. |
| `webapp/datasets_routes.py` | Eight routes over `dataset_settings`. Does not reimplement the model. |
| `webapp/profiles_routes.py` | Five routes over the saved profiles. |
| `webapp/static/index.html` | The one page. Everything else is an ES module. |
| `webapp/static/js/main.js` | The tablist, the router, window state, the first paint. |
| `webapp/static/js/dom.js` | `toast()`, `emptyState()`, and `captureScroll()` / `restoreScroll()` — shared by every screen. |
| `webapp/static/js/stats.js` | The `/ws/stats` client and its reconnect. |
| `webapp/static/js/overview.js`, `review.js`, `tabs.js`, `config.js`, `datasets.js`, `profiles.js`, `chart.js` | One module per screen. |

## The engine seam

`webapp/engine.py` wraps `HeadlessController` and exposes only `BROWSER_COMMANDS`. The
controller is built **lazily** on first use, so importing `webapp.app` stays free of the engine's
dependency graph.

**The web app starts `ControlServer` on the existing socket path.** That is not a nicety — it is
what keeps `cli.py` working *and* what makes the socket the singleton guard. `control.py` already
refuses to serve a socket another process is listening on, so a second `python -m webapp` and a
`cli launch` both fail with `already_running` and **no lock file is needed**. A web app that owned a
controller without serving the socket would leave the CLI blind *and* remove the guard.

`__main__.main()` takes the socket **before** uvicorn binds, so a second instance fails at boot
rather than on its first page load, and releases it in a `finally` so the next start is not refused by
a stale file.

`build_stats_payload()` is the contract. The web app **reads** it and never reshapes it.

## The auth gate

Four paths are public and **nothing else is**: `/login`, `/setup`, `/logout`, `/api/health`, plus
`/static/*` by prefix (the login page cannot render without the stylesheet).

`SessionGate` is an **ASGI middleware**, not a per-route dependency, so a route added later cannot
forget to declare it. `tests/test_web_auth.py` enumerates the route table and asserts the public set is
exactly that list — do not replace it with spot checks.

Four rules that are easy to break:

- **`Secure` is derived from the actual request scheme** (`session.cookie_secure()`), never from a
  constant. A `Secure` cookie on plain-HTTP loopback is never stored and the user sees a login loop.
- **`SameSite=Lax`, never `Strict`** — `Strict` breaks a reload after an external link.
- **The policy runs when the password is set**, so a weak one can never be stored. Enforcing only at
  login would leave a weak hash on disk.
- **The setup token goes to the console, never over HTTP.** It is the only thing stopping an
  unauthenticated visitor from claiming the instance.

`X-Forwarded-For` and `X-Forwarded-Proto` are **only believed when `--trust-proxy` is passed**,
because both are client-spoofable. Without that, the rate limiter would key every request to
`127.0.0.1` and one lockout would apply to everyone.

## The theme

`webapp/theme.py` is the web successor to `ui_theme.py`, and the rule is the same: **no raw
`#rrggbb` literal outside this module.** `tests/test_web_theme.py` greps for one.

The palette is **measured, not chosen by eye.** Two values from the mockup failed WCAG AA on the raised
card surface — the accent `#6b9bf5` at 4.42:1 and the danger `#e06c75` at 3.81:1 — and became
`#7aa6f7` (5.00:1) and `#e8838a` (4.66:1). The test recomputes the whole `CONTRAST_PAIRS` table from
the token constants and fails on any pair under 4.5:1, so the table cannot drift from the palette.

**Accent buttons use `ACCENT_FG`, never white.** White on `--accent` is 2.43:1 and fails badly.

The stylesheet is served from `/static/theme.css` out of `theme.STYLESHEET` rather than a `.css` file, so
there is exactly one definition of every colour. A second copy on disk would drift. Note that the sheet is a
**printf template** — a literal `%` has to be doubled.

## The config writer

`webapp/config_text.py` exists because `yaml.dump` **deletes all 113 comment lines including the SPDX
header** and re-wraps 122 lines. It splices only the changed spans, so an unchanged save is byte-identical
and a one-key edit is a four-line diff.

**Four shapes have to be handled, and three of them were bugs found by a gate:**

| Shape | What went wrong |
|-------|-----------------|
| Growing a **list** from empty | produced invalid YAML |
| Growing a **mapping** from empty | the same bug class — and `column_aliases: {}` is the shipped default on every new source, so it was the dangerous one |
| **Emptying** a block mapping | left a bare key that parsed as `None` |
| A container that becomes a **scalar**, or is **deleted** | **no code path at all** — `visible_columns: []` → `None` was silently dropped, and a block sequence set to a scalar **corrupted the file** |

The last one is fixed with `_collapsed_nodes` + `_collapse_edit`. If you touch the writer, run
`tests/test_web_config_tree.py` — it round-trips the shipped `config/config.yml` byte for byte.

**Validation runs server-side before a save.** A rejected save writes nothing and reports every bad field
at once. Do not move that check into the browser.

## The tests

Fifteen suites, all headless, all runnable with `python -B tests/<name>.py` or under pytest:

| Suite | Covers |
|-------|--------|
| `test_web_theme.py` | Tokens unique, no hex outside `theme.py`, the whole contrast table |
| `test_web_auth.py` | The policy cases, the session cases, the route-table enumeration, the lockout |
| `test_web_engine.py` | Every verb headless, the CLI against a web-owned controller, the singleton guard, no `generate_tk.py` import, the version agreement |
| `test_web_overview.py` | The cards, the progress, the issue panels, the graph |
| `test_web_review.py` | List, detail, export, drop, dismiss |
| `test_web_tabs.py` | The seven data tabs |
| `test_web_config_tree.py` | The tree and the byte-for-byte round trip |
| `test_web_config_editor.py` | All 1,291 leaves reach the browser exactly once |
| `test_web_config_schema.py` | Every leaf decorated; every validator |
| `test_web_datasets.py` | The panel, the alias editor, the column picker |
| `test_web_profiles.py` | Save, load, the diff preview |
| `test_web_polish.py` | Toasts, empty states, the tablist, window state, reconnect, emoji |
| `test_web_ui_regressions.py` | The four v15.0.1 defects: the `recent` shape, the metrics poller, the download preflight, the two clipped panels |
| `test_web_v151.py` | The v15.1.0 round: the two Metrics toggles, the measured viewBox, the live-prompt ring, scroll preservation, the computed cost, Clear queue, the export download, the Queue order, the polish |

## Traps

- **Never import `generate_tk.py`.** It builds the Tk window at import time.
- **Do not add a lock file.** The socket is the guard and it already exists.
- **`app.include_router()` appends an `_IncludedRouter`** with no `.path` and no `.routes`. A flat
  scan of `app.routes` finds nothing; the real routes hang off `original_router`. Walk into it.
- **An unannotated `request` parameter is a *query* field to FastAPI.** `async def save_config(request)`
  made every save fail with a 422 before the body was read, and a direct-call test could not see it because
  it passes a fake `Request` straight to the endpoint. Drive a real `TestClient` for anything that reads a body.
- **A `list` node carries `items`, not `value`.** Reading `value` renders every list control **empty** —
  including the 1,000-name list, which drew as a blank 4-row textarea. The control existed and was silently
  blank, which is the worst kind of failure.
- **There are two `@media (max-width: 1100px)` blocks.** Splitting on the query alone reads Overview's; match
  the block that contains the selector.
- **A probe that coerces is worse than no probe.** `record(name, ok)` must **raise** on a non-`bool`. A
  JSON-stringified result is always truthy, so a probe that passes one measures nothing and produces a green
  nobody questions.
- **A headless page is not the focused window**, so `:focus` never matches and `:focus-visible` cannot either.
  `Emulation.setFocusEmulationEnabled` fixes it.
- **Chromium's `--screenshot` fires before ES modules run**, so it captures the pre-JS shell. Drive CDP instead.
- **Kill by PID.** `pkill -f "python -m webapp"` matches the shell running it and kills your own session.
- **`pkill -f "generate_tk.py"` kills your own shell.** Use `pkill -f "python generate_tk\\.py$"`.
- **A re-render throws away every scroll position.** `main.js` replaces the whole screen on a stats
  tick, so a panel that scrolls must carry a stable `data-scroll-key` and the repaint must go through
  `dom.captureScroll()` / `restoreScroll()`. The Overview's Recent issues is the case that shipped broken.
- **`preserveAspectRatio="none"` squashes SVG text.** The chart's viewBox must match the host's own
  width, or every glyph is scaled horizontally and the axis labels run together.
- **A hook nobody installs is a feature nobody has.** `app_state.live_prompt_preview_hook` was only ever
  set by `generate_tk.py`, so headless it stayed `None` and the viewer was permanently empty. The same
  class of bug as the metrics poller never being started. If a feature needs a hook, `Engine.start()`
  installs it and `Engine.stop()` removes it, and a test asserts both.
- **A route function named after a module shadows that module for its whole body.** `def live_prompts(limit: int = 0)` shadowed the imported `live_prompts` module, so `live_prompts.snapshot(...)` called the function itself and raised `AttributeError: 'function' object has no attribute 'snapshot'`. FastAPI turned that into a `500` with a plain-text body, and the browser reported it as a **JSON parse error** — a message about the parser for a bug that was never about JSON. `tests/test_web_v151.py` scans every `*_routes.py` for a function whose name matches a module, so the whole class is caught. Name a route function `<thing>_route`.
- **`response.json()` on a non-JSON body is a `SyntaxError`, not the failure.** A `500` answers `Internal Server Error` as text, so an unguarded `response.json()` replaces the real error with "Unexpected character at line 1 column 1". `tabs.js` `readJSON()` checks `Content-Type` first; every new fetch helper must go through it.
- **`app_state.estimated_cost` is never written by a run.** It is declared and reset, and the only
  writer was the Tk dashboard's label refresh. Compute a derived figure where it is read.

## Tasks that touch this file

- Add a screen or a tab → `main.js` `TABS`, one module per screen, `theme.py` for any new class.
- Add a route → the owning `*_routes.py`, `install_*()` from `app.py`, and a check in the matching suite.
  If it is not in `PUBLIC_PATHS`, the gate covers it automatically.
- Change a colour, font or spacing value → `theme.py` only, then run `test_web_theme.py`.
- Add or change a config field's label, help or validator → `config_schema.py`, then
  `test_web_config_schema.py`.
- Change how the config file is written → `config_text.py`, then `test_web_config_tree.py`.
- Change the password policy, the session or the lockout → `security/`, then `test_web_auth.py`.
- Change what the browser may ask the engine → `engine.py` `BROWSER_COMMANDS`, then
  `test_web_engine.py`.
