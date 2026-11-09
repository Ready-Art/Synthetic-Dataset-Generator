<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 🌐 Web Front End

The generator's GUI is a **web application**. `python -m webapp` starts a small server on your
own machine and you drive the run from a browser. There is no window, no display requirement and
nothing to install beyond one requirements file — and because it is a normal HTTP server, it can also
sit behind nginx and be reached from another machine.

The engine is unchanged. The web app is a **second client of the same `HeadlessController`** that
`cli.py` already talks to, so what is sent to an API, what is written to an output file and every
config default are identical to the previous release.

## Contents

- [Start it](#start-it)
- [The first run: setup and login](#the-first-run-setup-and-login)
- [The screens](#the-screens)
- [The Config editor](#the-config-editor)
- [Two ways to deploy](#two-ways-to-deploy)
- [nginx](#nginx)
- [The CLI still works](#the-cli-still-works)
- [Troubleshooting](#troubleshooting)
- [Security notes](#security-notes)

## Start it

```bash
pip install -r requirements.txt -r requirements-web.txt
python -m webapp
```

Then open **<http://127.0.0.1:8765>**. That is the whole quick start: no certificate, no proxy,
no configuration file, no environment variable.

| Flag | Effect |
|------|--------|
| `--host` | Bind address. Default `127.0.0.1` — **loopback only**. |
| `--port` | Bind port. Default `8765`. |
| `--config` | The config file the engine runs from. Default `config/config.yml`. |
| `--socket` | The control socket path. Default: the config's `control.socket_path`. |
| `--trust-proxy` | Believe `X-Forwarded-For` / `X-Forwarded-Proto`. **Required behind nginx.** |
| `--allow-insecure-http` | Permit a non-loopback bind without TLS. Off by default. |
| `--auth-file` | Where the password hash and session secret live. Default `webapp/state/auth.json`. |
| `--open-browser` | Open the default browser at the served URL on startup. |
| `--log-level` | uvicorn log level. Default `info`. |

`requirements-web.txt` adds three packages — `fastapi`, `uvicorn[standard]` and `argon2-cffi` — and
nothing else. There is **no Node toolchain and no build step**: the front end is plain ES modules
served straight from `webapp/static/`.

## The first run: setup and login

There is **one password and no username**. It is not multi-user and does not try to be.

**On the very first start**, with no `webapp/state/auth.json`, the server generates a one-time setup
token and **prints it to the console**:

```
webapp: first-run setup token: 7f3c9a...
```

Open <http://127.0.0.1:8765/setup>, paste that token, and choose a password twice. The token is
**never served over HTTP** — you have to be able to read the server's terminal — so a stranger who
reaches the port cannot claim the instance. It is invalidated after use, and setup cannot be re-run
without deleting `webapp/state/auth.json` on the host, which needs filesystem access.

**The password is checked when it is set, not only when it is tried**, so a weak one can never be
stored. It must be at least 12 characters, use at least two of lower / upper / digit / punctuation,
and must not be a common password, one repeated character (`aaaaaaaaaaaa`), a keyboard or alphabet run
(`qwertyuiop12`, `0123456789`), or the app's own name with digits on the end (`ReadyArt2026`).
`correct horse battery staple` and `my-S3cure-passphrase` both pass.

**After that**, every visit shows the login form. Five failed attempts from one address lock that
address out for five minutes; the rule is printed under the field rather than hidden in a manual. The
lockout is per-address, so a locked client cannot lock the real user out.

The session is a signed cookie with a **12-hour lifetime**. It is `HttpOnly` and `SameSite=Lax`, and it
is marked `Secure` **only when the request actually arrived over HTTPS** — so a local-only user on
`http://127.0.0.1` is never handed a cookie the browser would refuse to store. Logging out clears it.

To change the password later, stop the server, delete `webapp/state/auth.json`, and start it again: you
get a fresh setup token. There is no in-app password change, because there is no account to manage.

## The screens

Eleven screens, in the tab strip. **Every one fits 1440x900 and 900x600 with no page scroll** — if a
screen needs a scrollbar at that size, the layout is wrong.

| Screen | What it is |
|--------|------------|
| **Overview** | Six metric cards in one row (the rest behind a "more metrics" disclosure), one progress row per slot, the issue panels, the issue graph, and a collapsible Diagnostics section with the database status and API response times. Start / Pause / Stop & Clear are in the action bar, plus **Download dataset**
(the whole dataset as one JSONL file) and **Clear dataset** (delete the stored
conversations, after a confirmation and with a backup zip taken first). |
| **Review** | Flagged conversations on the left, the conversation detail on the right. The score breakdown is a footer strip (Coherence / Character / Prose / Overall) colour-coded by threshold, so "why was this flagged" is answerable at a glance. Export, drop and dismiss act on the selection. |
| **APIs** | Per-API issue panels, with a slot selector. |
| **Quality** | The dimension summary and the latest-50 scores. |
| **Regex Watch** | The patterns and their hits. |
| **Metrics** | The per-slot vLLM table and its graphs. The table is written by the generation threads; the graphs read a separate ring that only the vLLM poller appends to, so `Engine.start()` starts that poller — without it the table fills in and every graph stays empty. |
| **Prompts** | The live prompt preview, with search and copy. |
| **Queue** | The pending and failed tables, with purge, retry and export. |
| **Config** | The config editor. See below. |
| **Datasets** | Dataset sources: a two-pane list and detail, the alias editor, column visibility and import. |
| **Profiles** | Save and load a copy of the whole config, with a diff preview before a destructive load. |

The tabs are keyboard-operable: arrow keys move between them, Home and End jump to the ends, and the
selected tab survives a reload. A dropped connection says `reconnecting…` and the page re-primes itself
from the server, so a long run that outlives the tab is never lost — the state lives on the server, not in
the page.

## The Config editor

The web Config editor is the **only** editor, and it covers the **whole config tree** — all 1,291 leaves,
not the 151 the old Tk editor curated. The left nav has one entry per section; a field is two columns with
its help text underneath.

Three things are worth knowing:

- **A save writes only what changed.** The writer splices the changed spans into the file rather than
  re-serialising it, so an unchanged save is byte-identical and a one-key edit is a four-line diff. Your
  comments and the SPDX header survive — `yaml.dump` would have deleted all 113 comment lines.
- **Validation runs on the server before anything is written.** A rejected save writes nothing and reports
  every bad field at once, rather than saving half a file.
- **A secret is never sent to the browser.** An API key field shows as masked; the value stays in
  `os.environ` on the server, exactly as the Tk editor did.

A field with no schema entry still renders, generically, from its value's type — so a key added to
`config/config.yml` tomorrow is editable today.

## Two ways to deploy

Both are first-class. **Neither is a degraded mode.**

| Shape | How | When |
|-------|-----|------|
| **Local only** | `python -m webapp`, open `http://127.0.0.1:8765` | One machine. **No TLS, no nginx, no config, no warnings.** |
| **Behind nginx (HTTPS)** | Bind loopback, nginx proxies `location /` | Remote access, TLS terminated by nginx. |

The bind address defaults to **loopback** for the same reason `control.py` refuses a non-loopback control
listener: this process can clear a database, so it must not be reachable from the network by accident.

A non-loopback bind **without** `--trust-proxy` and **without** `--allow-insecure-http` is refused at
startup, because it would serve a password over the network in the clear. The message says which flag to
add.

## nginx

nginx is **optional**. If you use it, it terminates HTTPS and forwards to the loopback bind.

```nginx
server {
    listen 443 ssl http2;
    server_name generator.example.com;

    ssl_certificate     /etc/letsencrypt/live/generator.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/generator.example.com/privkey.pem;

    client_max_body_size 16m;

    location / {
        proxy_pass http://127.0.0.1:8765;
        proxy_http_version 1.1;

        # WebSocket upgrade for the live stats feed
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_read_timeout 3600s;
    }
}
```

Start the app with `--trust-proxy` when this is in front of it. Four things nginx has to get right, and
each one is a real failure if it is wrong:

1. **`X-Forwarded-For`** — the login rate limiter keys on the client address. Behind a proxy every
   request arrives from `127.0.0.1`, so without the header one lockout would apply to everyone. The header
   is only believed when `--trust-proxy` is passed, because otherwise a client could spoof it.
2. **`X-Forwarded-Proto`** — decides whether the session cookie gets `Secure`. The wrong value means
   either a cookie that never sticks or one sent over plain HTTP.
3. **The `Upgrade` headers** — the live stats use a WebSocket. Without them nginx buffers the connection
   and the page silently stops updating.
4. **`proxy_read_timeout`** — a run can last hours. The default 60 s closes an idle WebSocket and the page
   silently stops updating. Set it to `3600s`.

## The CLI still works

The web app **owns the engine**: it constructs the `HeadlessController` and serves the control socket, exactly
as the Tk window did. So the CLI attaches to it and every verb keeps working while the web app is running:

```bash
python -m webapp &            # the web app owns the controller
python -m cli status          # attaches to the same controller
python -m cli stats           # the same statistics the browser shows
```

`cli launch --gui` now starts the **web app**; `cli launch --tk` reaches the old Tk window for one
deprecation release. Every other verb, flag, exit code and JSON shape is unchanged.

**A second controller is refused, and no lock file is involved.** The control socket is the guard: a second
`python -m webapp` and a `cli launch` both fail with `already_running`, because `control.py` already refuses
to serve a socket another process is listening on. That is also why you should not run the Tk window and the web
app at the same time — the Tk window only takes the socket when `control.enabled` is true.

## Troubleshooting

| Symptom | Cause and fix |
|---------|---------------|
| `Refusing to bind 0.0.0.0:8765` | A non-loopback bind serves a password in the clear. Pass `--trust-proxy` behind nginx, or `--allow-insecure-http` to accept plain HTTP on purpose. |
| The login page reloads forever and never logs in | A `Secure` cookie on plain HTTP never sticks. This cannot happen with the shipped code — `Secure` is derived from the actual request scheme — but a proxy that rewrites the scheme without `--trust-proxy` will do it. |
| `already_running` on startup | Another controller owns the socket. Run `python -m cli status` to see it, then `python -m cli shutdown`, or start the web app with a different `--socket`. |
| The page says `reconnecting…` and stops updating | The WebSocket dropped. Behind nginx this is almost always a missing `Upgrade` header or a `proxy_read_timeout` under the length of your run. The page re-primes itself when the connection returns. |
| `the web front end is not installed` from `cli launch --gui` | `requirements-web.txt` is not installed. `pip install -r requirements-web.txt`, or use `--tk`. |
| A save is rejected with several field errors | Validation runs server-side before anything is written, so nothing was saved. Fix the listed fields and save again. |
| I forgot the password | Stop the server, delete `webapp/state/auth.json`, start it again, and use the new console setup token. |

## Security notes

- The password is an **argon2id** hash in `webapp/state/auth.json`, mode `0600`, and that directory is
  gitignored. The session secret is beside it, 32 random bytes, also `0600`.
- The session is an **HMAC-SHA256 signed cookie**. There is no session in a URL and no token in
  `localStorage`; the only thing the page stores is which tab you were on.
- The comparison is constant-time, and a failed login is deliberately slow — argon2 verification is already
  ~50-100 ms — and returns a generic message with no timing signal.
- **No API key ever reaches the browser.** Keys stay in `os.environ` on the server.
- The password is never logged, never written to `config/config.yml`, and never sent to the browser.
- `--trust-proxy` is **off by default**. Only set it when a reverse proxy you control is in front of the
  app, or a client can spoof its own address and its own scheme.
