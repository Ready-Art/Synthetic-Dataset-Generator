<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 💻 Command Line

The generator can run without a window. `cli.py` drives the same engine, the same
`config/config.yml` and the same output files as the GUI, from a terminal — so a shell script,
a cron job, or an LLM agent can launch it, start and stop generation, read the statistics, and
export or clear the dataset.

The GUI is now a **web application**: `python -m webapp` serves it in a browser (see
[Web Front End](webapp.md)). `cli launch --gui` starts that. The old Tk window is still reachable for one
deprecation release via `cli launch --tk` or `python generate_tk.py`, and its control interface is **off by
default** and opt-in.

## Two ways to run it

**Headless (recommended for automation).** `launch` starts a background controller process that
owns the run. Every later verb talks to that process over a local Unix socket, so the commands
return immediately and you can query the run from another terminal.

```bash
python -m cli launch          # start the background controller
python -m cli start           # begin generating
python -m cli stats           # what is happening, and what is being filtered
python -m cli stop            # stop generating, keep the dataset
python -m cli shutdown        # stop and exit the controller
```

**Foreground.** `serve` runs the same controller in the terminal you are looking at, which is the
mode to use under `systemd` or inside a container. `Ctrl-C` stops it.

**The GUI.** `launch --gui` starts the **web application** instead of the headless controller, and
serves it on `http://127.0.0.1:8765`. `launch --tk` opens the old Tk window, which is deprecated and
removed in a later release.

## Machine-readable by default

Every verb prints **one JSON object** when stdout is not a terminal, so a script or an LLM can
parse it directly with no flags. On a terminal you get a readable table instead. Two flags
override the guess:

| Flag | Effect |
|---|---|
| `--json` | print JSON even on a terminal |
| `--text` | print the human table even when piped |

The global flags `--json`, `--text`, `--config` and `--socket` work **before or after** the
verb, because an LLM cannot be relied on to remember that argparse wants them first.

## Exit codes

Branch on the exit status, not on the wording of a message:

| Code | Meaning |
|---|---|
| `0` | the command succeeded |
| `1` | the command failed |
| `2` | bad usage (unknown verb, bad argument, unknown command, protocol mismatch) |
| `3` | nothing to talk to — no controller is running, or no run is in progress |
| `4` | a run is already in progress |
| `5` | refused, and nothing was done (e.g. `clear` without `--yes`) |
| `6` | timed out |

Exit code 3 covers both "no daemon is listening" and "the daemon is idle"; the JSON
`error.code` distinguishes them (`no_daemon` vs `not_running`) when the difference matters.

## The verbs

### `launch` — start the controller

```bash
python -m cli launch                     # background controller
python -m cli launch --gui                # the web UI instead
python -m cli launch --tk                 # the old Tk window (deprecated)
python -m cli launch --foreground         # same as `serve`
python -m cli launch --tcp-host 127.0.0.1 # also listen on TCP (see below)
```

`launch` waits until the controller answers before returning, so a `launch` followed by a
`start` cannot race. Use `--timeout` to change how long it waits (default 30 s).

### `serve` — run the controller in this terminal

```bash
python -m cli serve
```

Identical to `launch --foreground`. This is the mode for `systemd` and for containers.

### `status` — is it running, and what is it doing?

```bash
python -m cli status
```

Reports `phase` as one of `idle`, `running`, `paused` or `stopping`, plus the run's progress,
how many workers are alive, and which config file is in use. `stopping` means a stop has been
accepted but a worker is still finishing an in-flight request — it is still writing rows, so the
dataset is not final yet. `status` also reports a narrower `generating` flag for "the queue is still
feeding workers", so the two states can be told apart.

### `start` — begin generating

```bash
python -m cli start
python -m cli start --dry-run               # validate the config, send nothing
python -m cli start --turns 20 --threads 8   # override for this run only
python -m cli start --fresh                 # ignore saved state, start over
python -m cli start --wait                  # block until the run finishes
```

`--dry-run` resolves the whole config, checks that the run can start, and prints the plan
without launching a single worker — the right first command when you have edited
`config.yml` and want to know it is valid.

`--wait` blocks until the run finishes. `--timeout 0` (the default) waits forever; give it
seconds to bound the wait. The exit code is 6 if the timeout is reached.

### `stop` — stop generating, keep the dataset

```bash
python -m cli stop
python -m cli stop --clear        # also wipe run progress (the GUI's Stop & Clear Job)
```

`stop` returns as soon as the stop is accepted. If a worker is blocked inside a slow HTTP
request it cannot be interrupted, so the reply says `still_stopping` and explains that the run is
winding down. That is **not** an error: the run is stopping, and `status` reports `stopping` until
the last worker exits. `--timeout` bounds the whole wait for all workers together (default 30 s),
not each worker separately.

`--clear` is the GUI's **Stop & Clear Job**: it stops the run and then forgets the progress, so
the next `start` begins from the beginning.

### `pause` and `resume`

```bash
python -m cli pause     # finish the current task, then idle
python -m cli resume    # continue, reloading config.yml first
```

`resume` reloads the config, so you can adjust rate limits and other settings while paused,
exactly as the GUI does.

### `stats` — statistics, including what is being filtered

```bash
python -m cli stats
python -m cli stats --filters-only    # just the filter sections
python -m cli stats --no-recent       # omit the recent-issue lists
```

The payload reports progress, task counts, token use and estimated cost, the quality scores,
and the two distinct senses of "filtering":

* **`detected`** — a problem was *caught and handled* (rewritten, or retried). The conversation
  is still written to the dataset. One entry per check: refusals, user speaking, slop, anti-slop,
  errors, out of character, logic issues, regex watch, non-Latin script and scene continuity. Each
  has a total, a per-API breakdown, and a short recent list.
* **`filtered`** — the conversation was *dropped and never written*, keyed by reason
  (`invalid_json`, `logic_zero`, `judge_failed`, `manual`), with the affected task IDs and any write
  failures.
* **`detection_config`** — which checks are actually **armed**. A counter reading zero means either
  "nothing was caught" or "the check is switched off", and this section is what tells them apart.

Keeping `detected` and `filtered` apart matters: a reader who conflates them concludes the dataset is
smaller, or cleaner, than it really is.

### `export` / `save` — write the dataset to one file

```bash
python -m cli export
python -m cli save                        # alias of export
python -m cli export --output out/all.jsonl --source db
python -m cli save --append                # add to the destination
```

`--source auto` (the default) reads the database when it is enabled and has rows, and the output
JSONL files otherwise. Re-saving to the same destination is safe and does **not** duplicate rows: the
destination is excluded from the sources, so a file is never read while it is being written.

### `clear` — delete stored conversations

```bash
python -m cli clear --yes                   # both the database and the JSONL files
python -m cli clear --yes --target jsonl      # just the output JSONL files
python -m cli clear --yes --target state      # just forget run progress
python -m cli clear --yes --no-backup        # skip the backup zip
```

`clear` is destructive and refuses to run without `--yes` (exit code 5). By default it takes a
timestamped backup zip **before** deleting anything, so a mistake stays recoverable.

### `wait` — block until the run finishes

```bash
python -m cli wait
python -m cli wait --timeout 3600
```

### `shutdown` — stop the run and exit the controller

```bash
python -m cli shutdown
python -m cli shutdown --force    # exit even if the run will not stop cleanly
```

`shutdown` replies before it tears anything down, because the caller is waiting on the same
socket — replying after the close would turn a successful shutdown into a connection reset. The
process then exits on its own; you do not need to kill it.

### `shell` — one verb per line, from stdin

```bash
python -m cli shell
```

Reads one verb per line until `quit`. This is the mode for an LLM or an interactive session that
wants to avoid process startup for every command.

## Driving a running GUI from a terminal

**The web app always serves the control socket**, so `status`, `stats`, `stop` and the rest work against a
run started with `python -m webapp` with nothing to configure. It owns the `HeadlessController` and serves
the socket itself, which is also what makes a second controller impossible: `cli launch` is refused with
`already_running` while the web app is up.

The old Tk window is the exception. Set `control.enabled: true` in `config/config.yml` (or tick **Serve
control commands** in **Edit Config → Control Interface**) and it serves the same commands. This is off by
default, which is what keeps a default `python generate_tk.py` unchanged — and it is why you should not run the
Tk window and the web app at the same time.

## TCP instead of a Unix socket

The socket is a file under `output/`, which is the right transport for anything that shares the
filesystem. For a container that cannot mount it, add a loopback TCP listener:

```bash
python -m cli launch --tcp-host 127.0.0.1 --tcp-port 8765
```

A **non-loopback** address is refused unless `control.allow_remote: true` is set, because this
control channel can truncate the database. Enabling TCP must not be the same thing as exposing the
generator to the network.

## Configuration

The `control:` block in `config/config.yml`:

```yaml
control:
  enabled: false        # let a RUNNING GUI serve commands too
  socket_path: ''       # '' -> output/generator-control.sock
  tcp_host: ''          # '' -> TCP disabled; '127.0.0.1' to add it
  tcp_port: 8765
  allow_remote: false   # refuse a non-loopback TCP bind unless true
```

`launch`, `serve` and `shutdown` always use the socket — serving is their whole job.
`control.enabled` only decides whether the **GUI** also serves.

## Files the CLI writes

The control layer adds no new state of its own. It uses what the GUI already uses:

| Path | What it is |
|---|---|
| `output/generator-control.sock` | the control socket (removed on shutdown) |
| `output/control.log` | the controller's own log |
| `output/output.jsonl` | the dataset in single-API mode |
| `output/output_api_slot_N.jsonl` | one file per slot in duplication mode |
| `output/generation_state.json` | run progress, for resume |
| `output/dataset_export_<timestamp>.jsonl` | what `export` writes |
| `output/preclear_backup_*.zip` | what `clear` takes first |

## A typical session

```bash
python -m cli launch
python -m cli start --dry-run          # is the config sane?
python -m cli start
python -m cli status                   # watch it
python -m cli stats --filters-only      # what is being dropped, and why
python -m cli stop
python -m cli save --output final.jsonl
python -m cli shutdown
```

If a command fails, read the exit code first (see the table above) and the JSON
`error.code` second. Every message is written to be read by a person as well, so `--text` is
there when you want to look at it yourself.

## Troubleshooting

**`no_daemon` / exit 3 on every verb.** Nothing is listening. Run `launch` first, or check that
`--socket` names the same socket the controller is using.

**`already_running` / exit 4 on `launch`.** A controller is already listening on that socket.
`status` will tell you what it is doing; `shutdown` will stop it.

**`stop` says `still_stopping`.** A worker is finishing an in-flight request. This is normal — poll
`status` until the phase is `idle`. Use `--timeout` to wait longer inside `stop` itself.

**`refused` / exit 5 on `launch` with a TCP host.** The address is not loopback. Set
`control.allow_remote: true` if you really mean to listen on the network.
