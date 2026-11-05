# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Command line interface for the generator (v13.0).

The same generator, driven from a terminal instead of the Tk window. Two audiences, one
program: a human at a shell, and an LLM issuing commands and reading the JSON back. That
shapes every decision here.

    generator launch                 # start the background controller
    generator start                  # begin generating
    generator stats                  # what is happening, and what is being filtered
    generator stop                   # stop generating, keep the dataset
    generator save --output out.jsonl  # write the dataset to one file
    generator shutdown               # stop the controller

**Machine-readable by default.** When stdout is not a TTY, every verb prints exactly one JSON
object and nothing else, and the exit code carries the outcome. That means
``generator stats | jq .filtered`` and an LLM parsing ``stdout`` both work without a flag.
Pass ``--text`` for the aligned human table even when piped, or ``--json`` to force JSON on a
terminal.

**Exit codes are part of the contract** (see ``control.ERROR_EXIT_CODES``)::

    0  ok             3  not running / no daemon    6  timeout
    1  failed         4  already running
    2  usage          5  refused (needs --yes)

**No TTY assumptions.** Nothing prompts, nothing paginates, nothing waits for a keypress.
Anything destructive needs ``--yes`` on the command line, so a script can never be surprised by
a prompt it cannot answer.

**Why the daemon.** ``start`` has to keep running after the command that started it returns,
so ``launch`` spawns a detached controller process and ``serve`` runs the same controller in
the foreground (for systemd, or a terminal you want to watch). Every other verb is a client that
connects to it over the local control socket (``control.py``).
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time

import control
import run_config
from logging_config import log_message

PROGRAM = "generator"

# Set by --json / --text before dispatch; None means "decide from the TTY".
_FORCE_JSON = None


# =======================================================================================
# Output
# =======================================================================================

def _json_mode():
    """True when this invocation should print JSON instead of a human table."""
    if _FORCE_JSON is not None:
        return _FORCE_JSON
    try:
        return not sys.stdout.isatty()
    except Exception:  # noqa: BLE001 - a closed stdout is not a TTY
        return True


def emit(payload, exit_code=control.EXIT_OK):
    """Print the payload -- JSON or human text -- and return the exit code."""
    if _json_mode():
        # `_kind` is a private hint for the text renderer; it must not appear in the
        # JSON contract, which is what a script or an LLM parses.
        clean = {key: value for key, value in payload.items() if key != "_kind"} \
            if isinstance(payload, dict) else payload
        sys.stdout.write(json.dumps(clean, default=str, indent=None) + "\n")
    else:
        _print_human(payload)
    sys.stdout.flush()
    return exit_code


def emit_error(code, message, exit_code=None):
    """Print an error object (JSON) or an error line (text) and return the exit code."""
    exit_code = control.ERROR_EXIT_CODES.get(code, control.EXIT_FAILURE) \
        if exit_code is None else exit_code
    if _json_mode():
        sys.stdout.write(json.dumps(
            {"ok": False, "error": {"code": code, "message": message}},
            default=str) + "\n")
    else:
        sys.stderr.write(f"{PROGRAM}: {code}: {message}\n")
    sys.stdout.flush()
    return exit_code


def _print_human(payload):
    """Render a response payload for a person at a terminal."""
    if not payload.get("ok", True):
        error = payload.get("error") or {}
        sys.stderr.write(f"{PROGRAM}: {error.get('code', 'error')}: "
                          f"{error.get('message', '')}\n")
        return
    data = payload.get("data", payload)
    renderer = _RENDERERS.get(data.get("_kind"))
    if renderer is not None:
        renderer(data)
        return
    # A response with no specialised renderer: print its message, or the raw dict.
    message = data.get("message") or data.get("note")
    if message:
        print(message)
        return
    print(json.dumps(data, indent=2, default=str))


def _render_filters(data):
    """Just the two senses of "filtered" plus the armed settings."""
    print("DETECTED AND HANDLED (rewritten or retried, row still kept)")
    detected = data.get("detected", {})
    if not any(detected.get(stem, {}).get("total") for stem in detected):
        print("  (nothing detected)")
    for stem, payload in detected.items():
        total = payload.get("total", 0)
        if not total:
            continue
        per_api = payload.get("per_api") or {}
        spread = ", ".join(f"slot {key}: {value}"
                             for key, value in per_api.items() if value)
        print(f"  {payload.get('label', stem):<20} {total:>7}"
              f"{('   ' + spread) if spread else ''}")

    print("\nFILTERED (dropped, never written to the dataset)")
    filtered = data.get("filtered", {})
    if not filtered.get("total"):
        print("  (nothing dropped)")
    else:
        for reason, count in sorted(filtered.get("by_reason", {}).items()):
            print(f"  {reason:<20} {count:>7}")
    print(f"  review queue         {filtered.get('review_queue', 0)}")
    print(f"  write failures       {filtered.get('write_failures', 0)}")

    print("\nDETECTION SETTINGS (what is armed)")
    settings = data.get("detection_config", {})
    for key in ("slop_enabled", "anti_slop_enabled", "out_of_character_enabled",
                  "regex_watch_enabled", "non_latin_enabled",
                  "scene_continuity_enabled", "retry_missing_quotations",
                  "repeat_opener_guard", "repeat_reply_guard",
                  "quality_output_filter", "quality_per_turn_logic_check",
                  "discard_refused_conversations"):
        print(f"  {key:<30} {settings.get(key)}")
    print(f"  {'regex_watch_patterns':<30} {settings.get('regex_watch_patterns', 0)}")
    print(f"  {'refusal_phrases':<30} {settings.get('refusal_phrases', 0)}")
    print(f"  {'slop_phrases':<30} {settings.get('slop_phrases', 0)}")
    print(f"  {'first_reply_first_word_cap':<30} "
          f"{settings.get('first_reply_first_word_cap', 0)}")
    print(f"  {'first_reply_opener_cap':<30} "
          f"{settings.get('first_reply_opener_cap', 0)}")
    print()


def _render_stats(data):
    """The statistics table: run state, counters, and both senses of "filtered"."""
    gen = data.get("generator", {})
    run = data.get("run", {})
    out = data.get("output", {})
    tokens = data.get("tokens", {})
    progress = run.get("progress", {})

    print("=" * 72)
    print(f"Generator v{gen.get('version', '?')}  pid {gen.get('pid', '?')}  "
          f"phase {gen.get('phase', '?')}")
    print(f"Config: {gen.get('config_path', '?')}")
    print("=" * 72)

    print("\nRUN")
    print(f"  active           {run.get('active')}"
          f"{'  (paused)' if run.get('paused') else ''}")
    print(f"  duplication mode {run.get('duplication_mode')}")
    print(f"  threads          {run.get('alive_threads')}/{run.get('threads')} alive")
    tasks = run.get("tasks", {})
    print(f"  tasks            {tasks.get('completed', 0)} completed, "
          f"{tasks.get('pending', 0)} pending, {tasks.get('failed', 0)} failed")
    print(f"  progress         {progress.get('processed', 0)}/"
          f"{progress.get('total', 0)} turns "
          f"({progress.get('percent', 0)}%)")

    print("\nOUTPUT")
    print(f"  sink             {out.get('sink')}")
    for entry in out.get("jsonl_files") or []:
        print(f"  jsonl            {entry.get('rows', 0):>7} rows  {entry.get('path')}")
    if not out.get("jsonl_files"):
        print("  jsonl            (no output files yet)")
    database = out.get("database", {})
    rows = database.get("rows")
    print(f"  database         enabled={database.get('enabled')} "
          f"connected={database.get('connected')} "
          f"rows={'(unknown)' if rows is None else rows}")

    print("\nTOKENS")
    print(f"  input            {tokens.get('input', 0)}")
    print(f"  output           {tokens.get('output', 0)}")
    print(f"  estimated cost   {tokens.get('estimated_cost', 0.0)}")
    print(f"  attempts         {data.get('attempts', {}).get('total', 0)}")

    print("\nDETECTED AND HANDLED (rewritten or retried, row still kept)")
    detected = data.get("detected", {})
    if not any(detected.get(stem, {}).get("total") for stem in detected):
        print("  (nothing detected)")
    for stem, payload in detected.items():
        total = payload.get("total", 0)
        if not total:
            continue
        per_api = payload.get("per_api") or {}
        spread = ", ".join(f"slot {key}: {value}"
                            for key, value in per_api.items() if value)
        print(f"  {payload.get('label', stem):<20} {total:>7}"
              f"{('   ' + spread) if spread else ''}")

    print("\nFILTERED (dropped, never written to the dataset)")
    filtered = data.get("filtered", {})
    if not filtered.get("total"):
        print("  (nothing dropped)")
    else:
        for reason, count in sorted(filtered.get("by_reason", {}).items()):
            print(f"  {reason:<20} {count:>7}")
    print(f"  review queue         {filtered.get('review_queue', 0)}")
    print(f"  write failures       {filtered.get('write_failures', 0)}")

    quality = data.get("quality", {})
    if quality.get("enabled"):
        print("\nQUALITY")
        print(f"  scored           {quality.get('scored', 0)}")
        print(f"  composite        avg {quality.get('avg_composite', 0)}  "
              f"min {quality.get('min_composite', 0)}  "
              f"max {quality.get('max_composite', 0)}")
        print(f"  below threshold  {quality.get('threshold_failures', 0)} "
              f"(threshold {quality.get('threshold', 0)})")
        for flag, count in sorted((quality.get("flag_counts") or {}).items()):
            print(f"  flag {flag:<24} {count}")

    print("\nDETECTION SETTINGS (what is armed)")
    settings = data.get("detection_config", {})
    for key in ("slop_enabled", "anti_slop_enabled", "out_of_character_enabled",
                  "regex_watch_enabled", "non_latin_enabled",
                  "scene_continuity_enabled", "retry_missing_quotations",
                  "repeat_opener_guard", "repeat_reply_guard",
                  "quality_output_filter", "quality_per_turn_logic_check"):
        print(f"  {key:<30} {settings.get(key)}")
    print(f"  {'regex_watch_patterns':<30} {settings.get('regex_watch_patterns', 0)}")
    print(f"  {'refusal_phrases':<30} {settings.get('refusal_phrases', 0)}")
    print(f"  {'slop_phrases':<30} {settings.get('slop_phrases', 0)}")
    print(f"  {'first_reply_first_word_cap':<30} "
          f"{settings.get('first_reply_first_word_cap', 0)}")
    print(f"  {'first_reply_opener_cap':<30} "
          f"{settings.get('first_reply_opener_cap', 0)}")
    print()


def _render_status(data):
    phase = data.get("phase")
    if phase == "stopping":
        state = "stopping (workers still finishing)"
    elif data.get("generating"):
        state = "generating"
    elif data.get("running"):
        state = "busy"
    else:
        state = "idle"
    print(f"{PROGRAM}: {state} (phase {phase}, pid {data.get('pid')}, "
          f"v{data.get('version')}, up {data.get('uptime_s')}s)")
    progress = data.get("progress", {})
    if data.get("generating"):
        print(f"  {progress.get('processed', 0)}/{progress.get('total', 0)} turns "
              f"({progress.get('percent', 0)}%)")
    if data.get("alive_threads"):
        print(f"  {data.get('alive_threads')}/{data.get('threads')} workers alive")
    print(f"  config {data.get('config_path')}")


def _render_start(data):
    if data.get("dry_run"):
        print(f"{PROGRAM}: dry run -- the run WOULD start with:")
    else:
        print(f"{PROGRAM}: generation started")
    print(f"  threads          {data.get('threads')}")
    print(f"  turns            {data.get('turns')}")
    print(f"  active APIs      {data.get('active_apis')}")
    print(f"  duplication      {data.get('duplication_mode')}")
    print(f"  opening mode     {data.get('opening_mode')}")
    print(f"  questions file   {data.get('use_questions_file')}")
    print(f"  output sink      {data.get('output_sink')}")
    if data.get("dry_run"):
        print(f"  chunks           {data.get('num_random_chunks')}")
    else:
        print(f"  tasks queued     {data.get('tasks_queued')}")
        print(f"  turns total      {data.get('turns_total')}")
        print(f"  resumed          {data.get('resumed')}")


def _render_stop(data):
    if data.get("still_stopping"):
        print(f"{PROGRAM}: stopping (still winding down)")
    elif data.get("stopped"):
        print(f"{PROGRAM}: generation stopped")
    else:
        print(f"{PROGRAM}: {data.get('note') or 'nothing was running'}")
    print(f"  completed tasks  {data.get('completed_tasks')}")
    print(f"  pending tasks    {data.get('pending_tasks')}")
    if data.get("cleared"):
        print("  progress cleared")
    if data.get("note"):
        print(f"  {data['note']}")


def _render_export(data):
    print(f"{PROGRAM}: {data.get('message')}")
    print(f"  source  {data.get('source')}")
    print(f"  path    {data.get('path')}")
    print(f"  rows    {data.get('rows')}")


def _render_clear(data):
    print(f"{PROGRAM}: cleared ({data.get('target')})")
    if data.get("backup_path"):
        print(f"  backup   {data.get('backup_path')}")
    if data.get("db_cleared"):
        print(f"  database {data.get('db_message')}")
    if data.get("jsonl_removed"):
        print(f"  jsonl    {data.get('jsonl_message')}")
    if data.get("state_removed"):
        print("  state    removed")


def _render_shutdown(data):
    if data.get("stopped_run"):
        print(f"{PROGRAM}: stopped the run and shut down")
    else:
        print(f"{PROGRAM}: shut down")


def _render_pause(data):
    print(f"{PROGRAM}: {'paused' if data.get('paused') else 'resumed'}")


def _render_wait(data):
    if not data.get("waited", True):
        print(f"{PROGRAM}: {data.get('note') or 'no run to wait for'}")
        return
    print(f"{PROGRAM}: the run finished")
    print(f"  completed tasks  {data.get('completed_tasks')}")
    print(f"  failed tasks    {data.get('failed_tasks')}")
    progress = data.get("progress", {})
    print(f"  progress         {progress.get('processed', 0)}/"
          f"{progress.get('total', 0)} turns")


_RENDERERS = {
    "stats": _render_stats,
    "stats_filters": _render_filters,
    "status": _render_status,
    "start": _render_start,
    "stop": _render_stop,
    "export": _render_export,
    "clear": _render_clear,
    "shutdown": _render_shutdown,
    "pause": _render_pause,
    "wait": _render_wait,
}


def _tag(kind, data):
    """Attach the verb to a payload so the renderer can find it."""
    if isinstance(data, dict):
        data.setdefault("_kind", kind)
    return data


# =======================================================================================
# Client verbs
# =======================================================================================

def _socket_path(args):
    """The socket to talk to, honouring --socket and the config."""
    if getattr(args, "socket", None):
        return args.socket
    if getattr(args, "config", None):
        import app_state
        app_state.global_config.path = args.config
        app_state.global_config.load()
    return control.default_socket_path()


def _request(args, cmd, kind=None, _client_timeout=None, **kwargs):
    """Send one command and return its exit code after printing the response.

    ``kind`` picks the text renderer and ``_client_timeout`` the socket timeout; both
    are LOCAL and never put on the wire, so the server's accepted-argument set stays
    exactly the command's own.
    """
    payload = {"cmd": cmd, "args": kwargs}
    client_timeout = _client_timeout or getattr(args, "timeout", 30.0) or 30.0
    try:
        response = control.send_request(payload, socket_path=_socket_path(args),
                                         timeout=client_timeout)
    except control.CommandError as exc:
        return emit_error(exc.code, exc.message)
    if not response.get("ok"):
        error = response.get("error") or {}
        return emit_error(error.get("code", "failed"),
                           error.get("message", "the command failed"))
    return emit(_tag(kind or cmd, response.get("data") or {}))


def verb_status(args):
    return _request(args, "status")


def verb_version(args):
    return _request(args, "version")


def verb_start(args):
    code = _request(args, "start", dry_run=bool(args.dry_run),
                     turns=args.turns, chunks=args.chunks, threads=args.threads,
                     force_fresh=bool(args.fresh))
    if code != control.EXIT_OK or not args.wait:
        return code
    # --wait turns "start and block until it is done" into one command, which is what a
    # script or an LLM wants: it should not have to poll between two calls.
    return _request(args, "wait", kind="wait",
                     timeout=float(args.timeout or 0.0))


def verb_stop(args):
    # The socket timeout must exceed the stop budget, or the CLIENT gives up while
    # the server is still legitimately waiting for in-flight requests and reports a
    # timeout for a stop that is working.
    budget = float(args.timeout)
    return _request(args, "stop", clear=bool(args.clear), timeout=budget,
                     _client_timeout=budget + 15.0)


def verb_pause(args):
    return _request(args, "pause")


def verb_resume(args):
    return _request(args, "resume")


def verb_stats(args):
    return _request(args, "stats", include_recent=not args.no_recent,
                     filters_only=bool(args.filters_only),
                     kind=("stats_filters" if args.filters_only else "stats"))


def verb_export(args):
    return _request(args, "export", output=args.output, source=args.source,
                     append=bool(args.append))


def verb_clear(args):
    if not args.yes:
        return emit_error(
            "refused",
            "clear deletes stored conversations. Re-run with --yes to confirm.")
    return _request(args, "clear", confirm=True, target=args.target,
                     backup=not args.no_backup)


def verb_wait(args):
    return _request(args, "wait", timeout=float(args.timeout or 0.0))


def verb_shutdown(args):
    return _request(args, "shutdown", force=bool(args.force),
                     timeout=float(args.timeout))


def verb_launch(args):
    """Start the controller: detached by default, foreground with --foreground."""
    if args.gui:
        return _launch_gui(args)
    if args.foreground:
        return _serve(args)
    return _launch_daemon(args)


def _launch_gui(args):
    """Hand the terminal to the Tk application, unchanged."""
    import generate  # noqa: F401 - imported for its side effect: it builds the UI
    return control.EXIT_OK


def _serve(args):
    """Run the controller in THIS process (foreground; the systemd-friendly mode)."""
    controller = control.HeadlessController(
        config_path=args.config,
        serve_socket=True,
        socket_path=args.socket,
        tcp_host=args.tcp_host,
        tcp_port=args.tcp_port,
    )
    controller._socket_path = args.socket or control.default_socket_path()

    def _stop(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _stop)
    try:
        controller.serve_forever()
    except KeyboardInterrupt:
        log_message("Controller interrupted; shutting down.", "INFO")
    finally:
        if controller._server is not None:
            controller._server.stop()
        run_config.close_db_pool()
    return control.EXIT_OK


def _launch_daemon(args):
    """Spawn a detached ``serve`` process and wait until it answers."""
    socket_path = args.socket or control.default_socket_path()
    if control.is_server_running(socket_path):
        if _json_mode():
            return emit({"ok": False,
                          "error": {"code": "already_running",
                                    "message": f"A controller is already listening on "
                                               f"{socket_path}."}},
                          control.EXIT_ALREADY_RUNNING)
        print(f"{PROGRAM}: a controller is already listening on {socket_path}.")
        return control.EXIT_ALREADY_RUNNING

    log_path = os.path.join(run_config.OUTPUT_DIR, "control.log")
    os.makedirs(run_config.OUTPUT_DIR, exist_ok=True)
    command = [sys.executable, "-m", "cli"]
    if args.config:
        command += ["--config", args.config]
    command += ["--socket", socket_path, "serve"]
    if args.tcp_host:
        command += ["--tcp-host", args.tcp_host,
                    "--tcp-port", str(args.tcp_port or 8765)]

    with open(log_path, "ab") as log:
        process = subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, start_new_session=True,
            cwd=os.getcwd(), env=dict(os.environ))

    deadline = time.time() + (args.timeout or 30.0)
    while time.time() < deadline:
        if control.is_server_running(socket_path):
            payload = {"ok": True,
                        "message": f"Controller started (pid {process.pid}).",
                        "pid": process.pid, "socket": socket_path,
                        "log": log_path}
            return emit(_tag("launch", payload))
        if process.poll() is not None:
            return emit_error(
                "failed",
                f"The controller exited immediately (code {process.returncode}). "
                f"See {log_path}.")
        time.sleep(0.1)
    return emit_error(
        "timeout",
        f"The controller did not answer on {socket_path} within "
        f"{args.timeout or 30}s. See {log_path}.")


def _render_launch(data):
    print(f"{PROGRAM}: {data.get('message')}")
    print(f"  socket  {data.get('socket')}")
    print(f"  log     {data.get('log')}")


_RENDERERS["launch"] = _render_launch


# =======================================================================================
# Argument parser
# =======================================================================================

# Global options and whether each takes a value. Used by _hoist_globals() so a caller may
# write them before OR after the verb: `generator --json stats` and `generator stats --json`
# both work, which matters because a human types one and a script generator tends to write
# the other, and an LLM cannot be relied on to remember argparse's ordering rule.
GLOBAL_FLAGS = {
    "--json": False,
    "--text": False,
    "--config": True,
    "--socket": True,
}


def _hoist_globals(argv):
    """Move global options ahead of the verb so their position does not matter.

    ``argparse`` only accepts a top-level option before the subcommand, so
    ``generator status --json`` would otherwise fail with "unrecognized arguments".
    Rewriting the list here keeps the parser definition simple and the surface
    forgiving. ``--help``/``--version`` are left where they are: they terminate.
    """
    if not argv:
        return argv
    head, tail = [], []
    index = 0
    while index < len(argv):
        token = argv[index]
        flag = token.split("=", 1)[0]
        if flag in GLOBAL_FLAGS:
            head.append(token)
            if GLOBAL_FLAGS[flag] and "=" not in token and index + 1 < len(argv):
                head.append(argv[index + 1])
                index += 1
        else:
            tail.append(token)
        index += 1
    return head + tail


def build_parser():
    """The full command surface. One subcommand per verb, no hidden flags."""
    parser = argparse.ArgumentParser(
        prog=PROGRAM,
        description="Run and control the ReadyArt Synthetic Dataset Generator from a "
                    "terminal.",
        epilog=(
            "Every verb prints one JSON object when stdout is not a TTY, so a script or "
            "an LLM can parse it directly; pass --text for the human table, --json to force "
            "JSON on a terminal. Exit codes: 0 ok, 1 failed, 2 usage, 3 not running, "
            "4 already running, 5 refused (needs --yes), 6 timeout. "
            "Docs: docs/cli.md"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--json", action="store_true", dest="force_json",
                        help="print JSON even on a terminal")
    parser.add_argument("--text", action="store_true", dest="force_text",
                        help="print the human table even when piped")
    parser.add_argument("--config", metavar="PATH",
                        help="config file to use (default config/config.yml)")
    parser.add_argument("--socket", metavar="PATH",
                        help="control socket to use (default "
                             "output/generator-control.sock)")
    parser.add_argument("--version", action="version",
                        version=f"{PROGRAM} (headless control) "
                                f"{control.GENERATOR_VERSION}")

    subparsers = parser.add_subparsers(dest="verb", metavar="VERB")

    def add(name, help_text, handler):
        sub = subparsers.add_parser(name, help=help_text, description=help_text)
        sub.set_defaults(handler=handler)
        return sub

    launch = add("launch", "Start the controller (background), or the Tk GUI.",
                 verb_launch)
    launch.add_argument("--gui", action="store_true",
                         help="open the Tk window instead of the headless controller")
    launch.add_argument("--foreground", action="store_true",
                         help="run the controller in this terminal (same as serve)")
    launch.add_argument("--tcp-host", metavar="HOST", default=None,
                         help="also listen on this TCP address (e.g. 127.0.0.1)")
    launch.add_argument("--tcp-port", metavar="PORT", type=int, default=8765,
                         help="TCP port for --tcp-host (default 8765)")
    launch.add_argument("--timeout", type=float, default=30.0,
                         help="seconds to wait for the controller to answer "
                              "(default 30)")

    serve = add("serve", "Run the controller in this terminal (foreground).", _serve)
    serve.add_argument("--tcp-host", metavar="HOST", default=None,
                        help="also listen on this TCP address")
    serve.add_argument("--tcp-port", metavar="PORT", type=int, default=8765,
                        help="TCP port for --tcp-host (default 8765)")

    add("status", "Is it running, and what is it doing?", verb_status)
    add("version", "The controller's version and protocol.", verb_version)

    start = add("start", "Start generating.", verb_start)
    start.add_argument("--dry-run", action="store_true",
                         help="validate the config and report the plan, send nothing")
    start.add_argument("--turns", type=int, default=None,
                         help="override generation.num_turns for this run")
    start.add_argument("--chunks", type=int, default=None,
                         help="override generation.num_random_chunks for this run")
    start.add_argument("--threads", type=int, default=None,
                         help="override api.threads for this run")
    start.add_argument("--fresh", action="store_true",
                         help="ignore a saved state and start from the beginning")
    start.add_argument("--wait", action="store_true",
                         help="block until the run finishes (see --timeout)")
    start.add_argument("--timeout", type=float, default=0.0,
                         help="with --wait, seconds to wait; 0 (default) waits forever")

    stop = add("stop", "Stop generating (keep the dataset).", verb_stop)
    stop.add_argument("--clear", action="store_true",
                        help="also wipe run progress (the GUI's Stop & Clear Job)")
    stop.add_argument("--timeout", type=float, default=30.0,
                        help="seconds to wait for the workers to join (default 30)")

    add("pause", "Pause generating after the current task.", verb_pause)
    add("resume", "Resume a paused run and reload the config.", verb_resume)

    stats = add("stats", "Statistics: progress, counters and what is being filtered.",
                verb_stats)
    stats.add_argument("--no-recent", action="store_true",
                         help="omit the recent-issue lists and the log ring buffers")
    stats.add_argument("--filters-only", action="store_true",
                         help="only the detected / filtered / settings sections")

    export = add("export", "Write the dataset to one JSONL file.", verb_export)
    export.add_argument("--output", metavar="PATH", default=None,
                          help="destination file (default "
                               "output/dataset_export_<timestamp>.jsonl)")
    export.add_argument("--source", choices=("auto", "db", "jsonl"), default="auto",
                          help="where to read from: the database, the output JSONL "
                               "files, or auto (default)")
    export.add_argument("--append", action="store_true",
                          help="append instead of overwriting the destination")

    save = add("save", "Alias of export (save the DB or JSONL dataset).", verb_export)
    save.add_argument("--output", metavar="PATH", default=None)
    save.add_argument("--source", choices=("auto", "db", "jsonl"), default="auto")
    save.add_argument("--append", action="store_true")

    clear = add("clear", "Delete stored conversations. Destructive: needs --yes.",
                verb_clear)
    clear.add_argument("--yes", action="store_true",
                         help="confirm the deletion (required)")
    clear.add_argument("--target", choices=("db", "jsonl", "both", "state"),
                         default="both",
                         help="what to delete: the database, the output JSONL "
                              "files, both (default), or just run progress")
    clear.add_argument("--no-backup", action="store_true",
                         help="skip the pre-clear backup zip")

    wait = add("wait", "Block until the current run finishes.", verb_wait)
    wait.add_argument("--timeout", type=float, default=0.0,
                        help="seconds to wait; 0 (default) waits forever")

    shutdown = add("shutdown", "Stop the run and shut the controller down.",
                   verb_shutdown)
    shutdown.add_argument("--force", action="store_true",
                           help="shut down even if the run will not stop cleanly")
    shutdown.add_argument("--timeout", type=float, default=30.0,
                           help="seconds to wait for the workers (default 30)")

    add("shell", "Read verbs from stdin, one per line (for a human or an LLM).",
        verb_shell)

    return parser


# =======================================================================================
# Shell
# =======================================================================================

def verb_shell(args):
    """Read one verb per line from stdin and print each response as JSON.

    The point is a single long-lived conversation with the controller that an LLM can drive
    turn by turn: it writes ``stats``, reads a JSON line, writes ``stop``, reads a JSON line.
    An empty line or ``#`` comment is ignored; ``quit``/``exit`` ends the shell.
    """
    import shlex

    parser = build_parser()
    emit({"ok": True,
            "message": "Reading verbs from stdin, one per line. "
                       "Type 'quit' to leave.",
            "_kind": None})
    exit_code = control.EXIT_OK
    for line in sys.stdin:
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        if text in ("quit", "exit", "q"):
            break
        try:
            argv = shlex.split(text)
        except ValueError as exc:
            sys.stdout.write(json.dumps(
                {"ok": False, "input": text,
                 "error": {"code": "bad_request", "message": str(exc)}}) + "\n")
            sys.stdout.flush()
            exit_code = control.EXIT_USAGE
            continue
        if argv and argv[0] == PROGRAM:
            argv = argv[1:]
        try:
            namespace = parser.parse_args(argv)
        except SystemExit:
            sys.stdout.write(json.dumps(
                {"ok": False, "input": text,
                 "error": {"code": "bad_request", "message": "bad arguments"}}) + "\n")
            sys.stdout.flush()
            exit_code = control.EXIT_USAGE
            continue
        handler = getattr(namespace, "handler", None)
        if handler is None:
            sys.stdout.write(json.dumps(
                {"ok": False, "input": text,
                 "error": {"code": "bad_request", "message": "no verb given"}}) + "\n")
            sys.stdout.flush()
            exit_code = control.EXIT_USAGE
            continue
        handler(namespace)
    return exit_code


# =======================================================================================
# Entry point
# =======================================================================================

def main(argv=None):
    """Parse `argv`, run the verb, return the process exit code."""
    global _FORCE_JSON
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    args = parser.parse_args(_hoist_globals(list(argv)))

    if getattr(args, "force_json", False) and getattr(args, "force_text", False):
        return emit_error("bad_request", "--json and --text are mutually exclusive.",
                            control.EXIT_USAGE)
    if getattr(args, "force_json", False):
        _FORCE_JSON = True
    elif getattr(args, "force_text", False):
        _FORCE_JSON = False

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help(sys.stderr)
        return control.EXIT_USAGE

    try:
        return handler(args) or control.EXIT_OK
    except control.CommandError as exc:
        return emit_error(exc.code, exc.message)
    except KeyboardInterrupt:
        return emit_error("failed", "interrupted.", control.EXIT_FAILURE)
    except Exception as exc:  # noqa: BLE001 - never leak a traceback to a script
        log_message(f"CLI verb {args.verb!r} failed: {exc}", "ERROR")
        import traceback
        log_message(traceback.format_exc(), "ERROR")
        return emit_error("failed", str(exc))


if __name__ == "__main__":
    sys.exit(main())
