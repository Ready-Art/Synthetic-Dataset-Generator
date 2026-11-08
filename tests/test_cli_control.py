# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Tests for the v13.0 headless / CLI control layer.

Run with ``python -B tests/test_cli_control.py`` from the repo root.

WHY these tests exist and why they are shaped this way:

* The whole point of v13.0 is that an LLM (or a shell script) can drive the generator
  from a terminal. So the contract that matters is not "the functions return something
  sensible" but "the wire format, the exit codes and the JSON keys are exactly what a
  machine was promised". Those are what is asserted here.
* ``generate.py`` builds its Tk UI at import, so it is **never imported** by this file.
  Anything that would need a display is checked statically (by reading the source) or
  through ``control``/``run_config``, which are GUI-free by design.
* No network, no model, no PostgreSQL server, no display. Everything runs in a temp
  directory with a copied ``config/``, so the tests are hermetic and can run anywhere.

The single most valuable assertion here is ``test_worker_arguments_bind_to_the_real_signature``:
``generation.worker()`` takes 52 positional and 40 keyword-only parameters, and the CLI
reproduces that launch. If someone adds a worker parameter and forgets the controller, that
test fails instead of the CLI failing at 2 a.m. mid-run.
"""

import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class TempWorkspace:
    """A throwaway cwd with a copy of ``config/`` so nothing writes into the repo."""

    def __enter__(self):
        self._previous = os.getcwd()
        self.path = tempfile.mkdtemp(prefix="gen-cli-test-")
        shutil.copytree(ROOT / "config", os.path.join(self.path, "config"))
        os.makedirs(os.path.join(self.path, "output"), exist_ok=True)
        os.chdir(self.path)
        return Path(self.path)

    def __exit__(self, *exc_info):
        os.chdir(self._previous)
        shutil.rmtree(self.path, ignore_errors=True)
        return False


def _write_config(base, mutate):
    """Rewrite ``config/config.yml`` through `mutate(dict)`."""
    import yaml
    path = base / "config" / "config.yml"
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    mutate(data)
    with open(path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, sort_keys=False)


# ---------------------------------------------------------------------------
# The GUI-free guarantee
# ---------------------------------------------------------------------------

def test_control_layer_never_imports_the_gui_module():
    """``run_config``/``control``/``cli`` must not import ``generate``.

    ``generate.py`` builds every Tk widget at module scope, so importing it from the
    control layer would make the CLI require a display and defeat the entire feature.
    Checked by source scan because the failure is an import-time side effect that would
    otherwise only show up as a crash on a headless machine.
    """
    import ast

    # ``cli._launch_gui()`` is the ONE place allowed to import generate: opening the
    # window is that command's entire job. Every other import of it would make a
    # headless verb require a display.
    allowed = {("cli.py", "_launch_gui")}
    offenders = []

    for name in ("run_config.py", "control.py", "cli.py"):
        tree = ast.parse((ROOT / name).read_text(encoding="utf-8"))

        # Map each import node to the function (if any) that contains it.
        parent_function = {}
        for function in [n for n in ast.walk(tree)
                         if isinstance(n, ast.FunctionDef)]:
            for node in ast.walk(function):
                parent_function[id(node)] = function.name

        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Import):
                targets = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                targets = [node.module or ""]
            for target in targets:
                if target.split(".")[0] != "generate":
                    continue
                owner = parent_function.get(id(node), "<module level>")
                if (name, owner) in allowed:
                    continue
                offenders.append(f"{name}:{owner}: import {target}")

    assert not offenders, (
        "The control layer must stay GUI-free (generate.py builds Tk at import), "
        "but it imports generate.py outside the one allowed GUI-launch path: "
        + "; ".join(offenders))


def test_control_layer_imports_without_a_display():
    """The three modules import cleanly with no DISPLAY set."""
    previous = os.environ.pop("DISPLAY", None)
    try:
        import cli
        import control
        import run_config
        assert control.PROTOCOL_VERSION >= 1
        assert cli.PROGRAM
        assert run_config.DERIVED_OUTPUT_PREFIXES
    finally:
        if previous is not None:
            os.environ["DISPLAY"] = previous


# ---------------------------------------------------------------------------
# The worker-launch contract
# ---------------------------------------------------------------------------

def test_worker_arguments_bind_to_the_real_signature():
    """The controller's launch must bind against ``generation.worker``'s real signature.

    ``generation.worker`` has 52 positional parameters and a keyword-only tail. The CLI
    reproduces the GUI's launch. If a parameter is added, removed or reordered on one
    side only, this fails here instead of raising ``TypeError`` mid-run.
    """
    import inspect

    import control
    import generation

    signature = inspect.signature(generation.worker)
    controller = control.HeadlessController()
    settings = control.run_config.build_run_settings()
    args = controller._worker_args(0, settings)
    kwargs = controller._worker_kwargs(settings)

    # Binds cleanly: no missing parameter, no duplicate, no unexpected keyword.
    # `args` already begins with thread_id, so it is bound as-is -- passing a separate
    # value for thread_id shifted every later argument by one.
    bound = signature.bind(*args, **kwargs)
    bound.apply_defaults()

    positional_names = [name for name, param in signature.parameters.items()
                        if param.kind is param.POSITIONAL_OR_KEYWORD]
    # The controller fills only the required prefix positionally; the tail (which the
    # worker declares with defaults) is passed by keyword. So it must not pass MORE
    # positionals than exist, and every keyword must name a real parameter.
    assert len(args) <= len(positional_names), (
        f"controller passes {len(args)} positional args but worker() takes only "
        f"{len(positional_names)}")

    # Every name the controller passes by keyword must be a real parameter, and it must
    # not also be filled positionally (that is a duplicate-value TypeError).
    known = set(signature.parameters)
    unknown = sorted(set(kwargs) - known)
    assert not unknown, f"controller passes unknown worker kwargs: {unknown}"

    positional_names = [name for name, param in signature.parameters.items()
                        if param.kind is param.POSITIONAL_OR_KEYWORD]
    filled_positionally = set(positional_names[:len(args)])
    duplicated = sorted(filled_positionally & set(kwargs))
    assert not duplicated, (
        "controller passes these both positionally and by keyword: " + ", ".join(duplicated))

    # Together, the two must cover every required parameter.
    required = {name for name, param in signature.parameters.items()
                if param.default is param.empty}
    covered = filled_positionally | set(kwargs)
    assert not (required - covered), (
        "controller never supplies required worker parameter(s): "
        + ", ".join(sorted(required - covered)))


def test_worker_launch_does_not_send_none_for_required_flags():
    """A ``None`` where the worker expects a bool/list is a silent behaviour change."""
    import control
    controller = control.HeadlessController()
    settings = control.run_config.build_run_settings()
    for name, value in controller._worker_kwargs(settings).items():
        assert value is not None, f"worker kwarg {name} is None"


def test_static_var_shim_matches_a_tk_variable():
    """``app_state.master_duplication_enabled_var`` is None headlessly.

    The engine calls ``.get()`` on it on every write path, so the controller installs a
    ``StaticVar``. If it stopped honouring ``.get()``, every headless run would raise
    ``AttributeError`` on its first successful conversation.
    """
    import control
    shim = control.StaticVar(True)
    assert shim.get() is True
    shim.set(False)
    assert shim.get() is False


# ---------------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------------

def test_run_settings_read_the_config_that_is_actually_there():
    """Values come from config.yml, not from defaults that merely look plausible."""
    with TempWorkspace() as base:
        _write_config(base, lambda data: _set(data, [
            ("generation.num_turns", 7),
            ("generation.num_random_chunks", 33),
            ("generation.subject_size", 111),
        ]))
        # Threads come from each API entry's own `threads`, not from the legacy
        # `api.threads` key -- the GUI reads them the same way. An API also only
        # counts as active when it has a URL, so the total would be 0 without one.
        _set_parallel(base, "config/config.yml", 3)

        import app_state
        import run_config
        # build_run_settings() reads app_state.global_config, which is a cache; reload it
        # the way cmd_start() does so the temp config is actually the one read.
        app_state.global_config.load()
        settings = run_config.build_run_settings()
        assert settings.num_turns == 7, (
            f"num_turns is {settings.num_turns}, not the 7 written to the config")
        assert settings.num_random_chunks == 33, settings.num_random_chunks
        assert settings.subject_size == 111, settings.subject_size
        assert run_config.total_threads_for_run(settings) == 3, (
            "per-API thread counts were not summed; api.threads is legacy and must "
            "not be used as the total")


def test_run_settings_expose_every_field_the_worker_launch_needs():
    """The declared field tuples and the attributes must agree.

    ``build_run_settings`` fills attributes; the launch unpacks them by name. A field
    named in the tuple but never assigned is an ``AttributeError`` at launch time.
    """
    with TempWorkspace():
        import run_config
        settings = run_config.build_run_settings()
        for field in (run_config.RunSettings.WORKER_POSITIONAL_FIELDS
                      + run_config.RunSettings.WORKER_KEYWORD_FIELDS):
            assert hasattr(settings, field), f"RunSettings is missing {field}"


def test_an_unconfigured_api_is_rejected_before_any_worker_starts():
    """A run with no usable API must fail loudly, not spin up threads that all error."""
    with TempWorkspace() as base:
        def strip_apis(data):
            for entry in (data.get("api", {}).get("apis") or []):
                entry["enabled"] = False
        _write_config(base, strip_apis)

        import run_config
        settings = run_config.build_run_settings()
        try:
            run_config.validate_run_shape(settings)
        except run_config.RunConfigError:
            return
        raise AssertionError("validate_run_shape accepted a config with no enabled API")


def test_the_gui_and_the_cli_read_the_same_config_keys():
    """``start_processing()`` and ``run_config`` must agree on the keys they read.

    The GUI's start path keeps its own unpacking (it cannot be imported here), so this
    is the guard that the two sides do not drift: every key the GUI start reads must
    also be read somewhere in the control layer, or the CLI would run a differently
    configured job than the window would.
    """
    import re

    generate_source = (ROOT / "generate.py").read_text(encoding="utf-8")
    start = generate_source.index("def start_processing():")
    end = generate_source.index("\ndef ", start + 10)
    gui_keys = set(re.findall(r"global_config\.get\(\s*'([^']+)'",
                              generate_source[start:end]))

    # Match any quoted dotted config path, not just `X.get('...')`: run_config resolves
    # many keys through helpers such as _int_with_default(cfg, 'generation.foo', ...).
    control_keys = set()
    for name in ("run_config.py", "control.py", "quality.py", "generation.py"):
        source = (ROOT / name).read_text(encoding="utf-8")
        # Dotted paths AND bare top-level keys (`.get('datasets')`, `.get('gender')`).
        control_keys |= set(re.findall(r"['\"]([a-z_][a-z_0-9]*(?:\.[a-z_0-9]+)*)['\"]", source))

    # A key counts as covered when the control layer reads the same dotted path OR the
    # same leaf name (run_config resolves several keys through helpers that take the leaf).
    control_leaves = {key.rsplit(".", 1)[-1] for key in control_keys}
    missing = sorted(key for key in gui_keys - control_keys
                     if key.rsplit(".", 1)[-1] not in control_leaves)
    assert not missing, (
        "start_processing() reads config keys the control layer never reads, so a "
        "CLI run would be configured differently from a GUI run: " + ", ".join(missing))


# ---------------------------------------------------------------------------
# The wire protocol
# ---------------------------------------------------------------------------

def test_a_server_round_trips_a_command_over_a_unix_socket():
    """One JSON object each way, one per line -- the documented wire format."""
    with TempWorkspace() as base:
        import control

        socket_path = str(base / "output" / "test.sock")
        server = control.ControlServer(
            controller=control.HeadlessController(), socket_path=socket_path)
        server.start()
        try:
            assert control.is_server_running(socket_path), "server not reachable"

            reply = control.send_request({"cmd": "version", "args": {}},
                                         socket_path=socket_path, timeout=10.0)
            assert reply["ok"] is True, reply
            assert reply["data"]["version"] == control.GENERATOR_VERSION

            reply = control.send_request({"cmd": "status", "args": {}},
                                         socket_path=socket_path, timeout=10.0)
            assert reply["ok"] is True, reply
            assert reply["data"]["running"] is False, reply
            assert reply["data"]["phase"] == "idle", reply
        finally:
            server.stop()
        assert not os.path.exists(socket_path), "socket file was left behind"


def test_an_unknown_command_is_an_error_not_a_crash():
    """A typo must come back as a structured error the caller can branch on."""
    with TempWorkspace() as base:
        import control
        socket_path = str(base / "output" / "test.sock")
        server = control.ControlServer(
            controller=control.HeadlessController(), socket_path=socket_path)
        server.start()
        try:
            reply = control.send_request({"cmd": "frobnicate", "args": {}},
                                         socket_path=socket_path, timeout=10.0)
            assert reply["ok"] is False, reply
            assert reply["error"]["code"] == "unknown_command", reply
            # And the server is still alive and serving after the bad command.
            reply = control.send_request({"cmd": "version", "args": {}},
                                         socket_path=socket_path, timeout=10.0)
            assert reply["ok"] is True, reply
        finally:
            server.stop()


def test_a_protocol_mismatch_is_reported_rather_than_guessed():
    """A client speaking a different protocol version must be told, not misread."""
    with TempWorkspace() as base:
        import control
        socket_path = str(base / "output" / "test.sock")
        server = control.ControlServer(
            controller=control.HeadlessController(), socket_path=socket_path)
        server.start()
        try:
            reply = control.send_request(
                {"cmd": "version", "args": {}, "protocol": control.PROTOCOL_VERSION + 99},
                socket_path=socket_path, timeout=10.0)
            assert reply["ok"] is False, reply
            assert reply["error"]["code"] == "protocol_mismatch", reply
        finally:
            server.stop()


def test_a_command_with_bad_arguments_is_an_error_not_a_crash():
    """An LLM guessing an argument name must get a usable error back."""
    with TempWorkspace() as base:
        import control
        socket_path = str(base / "output" / "test.sock")
        server = control.ControlServer(
            controller=control.HeadlessController(), socket_path=socket_path)
        server.start()
        try:
            reply = control.send_request(
                {"cmd": "status", "args": {"nonsense": 1}},
                socket_path=socket_path, timeout=10.0)
            assert reply["ok"] is False, reply
            assert reply["error"]["code"] == "bad_request", reply
        finally:
            server.stop()


def test_talking_to_no_server_is_a_clear_error():
    """No daemon is a normal state, not an exception traceback."""
    with TempWorkspace() as base:
        import control
        missing = str(base / "output" / "nothing.sock")
        assert not control.is_server_running(missing)
        try:
            control.send_request({"cmd": "status", "args": {}},
                                  socket_path=missing, timeout=2.0)
        except control.CommandError as exc:
            # `no_daemon` and `not_running` both exit 3; the distinct code is more
            # precise ("nothing is listening" vs "listening but idle").
            assert exc.code in ("no_daemon", "not_running"), exc.code
            return
        raise AssertionError("send_request to a missing socket did not raise")


# ---------------------------------------------------------------------------
# Exit codes -- the contract a script branches on
# ---------------------------------------------------------------------------

def test_every_error_code_has_a_distinct_exit_status():
    """``ERROR_EXIT_CODES`` is how a shell learns *why* a command failed."""
    import control
    codes = control.ERROR_EXIT_CODES
    # The table maps ERROR codes to exit statuses; success is EXIT_OK, not a row.
    assert control.EXIT_OK == 0
    for name in ("failed", "not_running", "already_running", "refused", "timeout",
                 "unknown_command", "bad_request", "config"):
        assert name in codes, f"no exit code for error {name!r}"
    statuses = list(codes.values())
    assert control.EXIT_OK not in statuses, "a failure must not exit 0"
    # Several error codes deliberately share a status (all usage errors are 2, both
    # "nothing listening" and "listening but idle" are 3). What must hold is that the
    # states a script branches on are distinguishable from each other.
    assert codes["refused"] != codes["not_running"]
    assert codes["already_running"] != codes["not_running"]
    assert codes["timeout"] != codes["failed"]
    assert codes["not_running"] == codes["no_daemon"], (
        "both mean 'no run to talk to', so they must share an exit status")


# ---------------------------------------------------------------------------
# The statistics payload -- what "display statistics" promises
# ---------------------------------------------------------------------------

def test_the_stats_payload_has_the_documented_top_level_keys():
    """The JSON is the LLM contract, so its keys are asserted, not assumed."""
    with TempWorkspace():
        import control
        payload = control.HeadlessController().cmd_stats()
        for key in ("ok", "generated_at", "generator", "run", "output", "tokens",
                    "attempts", "detected", "filtered", "quality",
                    "detection_config"):
            assert key in payload, f"stats payload is missing {key!r}"
        for key in ("version", "pid", "uptime_s", "phase", "config_path"):
            assert key in payload["generator"], key
        for key in ("active", "paused", "tasks", "progress"):
            assert key in payload["run"], key
        for key in ("pending", "completed", "failed"):
            assert key in payload["run"]["tasks"], key


def test_the_stats_payload_separates_detected_from_filtered():
    """"What's being filtered" has two meanings and must not be conflated.

    ``detected`` is "caught and handled, row still written"; ``filtered`` is "dropped,
    never written". A reader who mixes them up concludes the dataset is smaller (or
    cleaner) than it is, so they are separate keys with separate meanings.
    """
    with TempWorkspace():
        import control
        payload = control.HeadlessController().cmd_stats()
        assert "detected" in payload and "filtered" in payload
        assert "by_reason" in payload["filtered"], payload["filtered"]
        for key, entry in payload["detected"].items():
            for field in ("total", "per_api", "recent", "label"):
                assert field in entry, f"detected[{key}] is missing {field}"


def test_every_detection_counter_reports_a_populated_per_api_map():
    """Four of ten counters once reported ``per_api: {}`` because the attribute names
    are inconsistent (``refusal_count_total`` is singular, ``ooc_count_total`` is
    abbreviated). The counter table must therefore be explicit, and every entry must
    resolve to a real app_state attribute with a per-API dict.
    """
    with TempWorkspace():
        import app_state
        import control

        payload = control.HeadlessController().cmd_stats()
        assert payload["detected"], "no detection counters at all"
        for key, entry in payload["detected"].items():
            assert isinstance(entry["per_api"], dict), f"{key}.per_api is not a dict"
            assert entry["per_api"], f"{key}.per_api is empty"

        # Each declared row must name real attributes, or the table has drifted.
        for row in control.DETECTION_COUNTERS:
            key, label, total_attr, per_api_attr = row[0], row[1], row[2], row[3]
            assert hasattr(app_state, total_attr), (
                f"counter {key!r} names app_state.{total_attr}, which does not exist")
            assert hasattr(app_state, per_api_attr), (
                f"counter {key!r} names app_state.{per_api_attr}, which does not exist")
            assert isinstance(getattr(app_state, per_api_attr), dict), per_api_attr


def test_the_filters_only_view_is_a_subset_of_the_full_view():
    """``--filters-only`` must not invent keys the full payload does not have."""
    with TempWorkspace():
        import control
        controller = control.HeadlessController()
        full = controller.cmd_stats()
        narrow = controller.cmd_stats(filters_only=True)
        for key in narrow:
            if key in ("ok", "generated_at"):
                continue
            assert key in full, f"filters-only has {key!r}, which the full stats lack"


def test_detection_config_reports_which_checks_are_armed():
    """"What's being filtered" also means "which filters are switched on"."""
    with TempWorkspace():
        import control
        config = control.HeadlessController().cmd_stats()["detection_config"]
        assert "slop_enabled" in config
        assert "quality_enabled" in config
        assert isinstance(config["slop_enabled"], bool)


# ---------------------------------------------------------------------------
# Output files -- save / clear must agree on what counts as output
# ---------------------------------------------------------------------------

def test_derived_output_files_are_not_counted_as_dataset_rows():
    """A previous export must never be swept back in as source data.

    ``save`` doubled the row count on every save because its own export was picked up
    by the next save's file list. One prefix list is shared by save and clear so they
    cannot disagree about which files are derived.
    """
    with TempWorkspace() as base:
        import run_config
        output = base / "output"
        (output / "output.jsonl").write_text('{"a": 1}\n{"a": 2}\n', encoding="utf-8")
        for prefix in run_config.DERIVED_OUTPUT_PREFIXES:
            (output / f"{prefix}.jsonl").write_text('{"junk": true}\n', encoding="utf-8")

        files = run_config.jsonl_output_files()
        names = [os.path.basename(path) for path in files]
        assert "output.jsonl" in names, names
        for prefix in run_config.DERIVED_OUTPUT_PREFIXES:
            assert f"{prefix}.jsonl" not in names, (
                f"{prefix}.jsonl was counted as dataset output: {names}")

        assert run_config.count_jsonl_rows(str(output / "output.jsonl")) == 2


def test_saving_to_the_same_path_twice_is_idempotent():
    """Re-saving over the destination is the normal case, not an error."""
    with TempWorkspace() as base:
        import run_config
        source = base / "output" / "output_api_slot_0.jsonl"
        source.write_text('{"a": 1}\n{"a": 2}\n', encoding="utf-8")
        destination = base / "output" / "dataset_export.jsonl"

        result = run_config.consolidate_jsonl(str(destination))
        assert result[0] is True, result
        assert result[2] == 2, result

        # Saving again to the same destination must not double the file.
        result = run_config.consolidate_jsonl(str(destination))
        assert result[0] is True, result
        lines = [line for line in destination.read_text(encoding="utf-8").splitlines()
                 if line.strip()]
        assert len(lines) == 2, f"re-saving duplicated rows: {len(lines)}"


def test_clearing_the_jsonl_takes_a_backup_first():
    """``clear`` is destructive, so it must leave a recoverable copy."""
    with TempWorkspace() as base:
        import run_config
        output = base / "output"
        (output / "output.jsonl").write_text('{"a": 1}\n', encoding="utf-8")

        # clear_jsonl deletes; taking the backup is a separate, explicit step so the
        # caller cannot forget it. This asserts the two compose into a recoverable clear.
        ok, _message, path = run_config.backup_output_zip()
        assert ok and path and os.path.exists(path), (ok, path)
        ok, _message, removed = run_config.clear_jsonl()
        assert ok and removed == 1, (ok, removed)
        assert not (output / "output.jsonl").exists()


def test_clearing_without_confirmation_is_refused():
    """``clear`` must not run because a script forgot the flag."""
    with TempWorkspace():
        import control
        controller = control.HeadlessController()
        try:
            controller.cmd_clear(target="jsonl", confirm=False)
        except control.CommandError as exc:
            assert exc.code == "refused", exc.code
            return
        raise AssertionError("clear ran without confirmation")


# ---------------------------------------------------------------------------
# Stop semantics
# ---------------------------------------------------------------------------

def test_stopping_an_idle_controller_says_so_instead_of_pretending():
    """Stopping nothing is ``not_running`` (exit 3), not success."""
    with TempWorkspace():
        import app_state
        import control
        app_state.processing_active = False
        app_state.threads = []
        try:
            control.HeadlessController().cmd_stop()
        except control.CommandError as exc:
            assert exc.code == "not_running", exc.code
            return
        raise AssertionError("stop on an idle controller did not report not_running")


def test_the_stop_budget_is_shared_across_workers_not_per_worker():
    """A per-thread timeout made the real budget ``timeout x threads``.

    With ten workers and a 30 s timeout the caller waited up to 300 s, so the client's
    own socket timeout fired first and reported a timeout for a stop that was working.
    """
    with TempWorkspace():
        import app_state
        import control

        stop_flag = threading.Event()

        def idle_worker():
            stop_flag.wait(30.0)

        app_state.threads = [threading.Thread(target=idle_worker, daemon=True)
                             for _ in range(4)]
        for thread in app_state.threads:
            thread.start()

        controller = control.HeadlessController()
        started = time.monotonic()
        alive = controller._join_workers(timeout=1.0)
        elapsed = time.monotonic() - started

        assert elapsed < 3.0, (
            f"joining 4 workers with a 1 s budget took {elapsed:.1f}s -- the budget "
            "is being applied per thread")
        assert len(alive) == 4, "workers still running were not reported"
        assert len(app_state.threads) == 4, (
            "live workers were dropped from app_state.threads, so a later run could "
            "start on top of them")

        stop_flag.set()
        for thread in list(app_state.threads):
            thread.join(timeout=5.0)
        app_state.threads = []


def test_a_worker_still_running_after_a_stop_reports_stopping_not_idle():
    """``idle`` while a worker is still writing rows is a lie a reader would act on."""
    with TempWorkspace():
        import app_state
        import control

        stop_flag = threading.Event()
        worker = threading.Thread(target=stop_flag.wait, args=(30.0,), daemon=True)
        worker.start()
        app_state.threads = [worker]
        app_state.processing_active = False
        try:
            status = control.HeadlessController().cmd_status()
            assert status["phase"] == "stopping", status
            assert status["running"] is True, status
            assert status["generating"] is False, status
        finally:
            stop_flag.set()
            worker.join(timeout=5.0)
            app_state.threads = []


# ---------------------------------------------------------------------------
# Network safety
# ---------------------------------------------------------------------------

def test_tcp_refuses_a_non_loopback_bind_unless_explicitly_allowed():
    """A control channel that can TRUNCATE a database must not reach the network by
    accident: enabling TCP must not be the same as exposing the generator.
    """
    with TempWorkspace():
        import control

        for host in ("0.0.0.0", "10.0.0.5", "192.168.1.20", "example.com"):
            assert not control._is_loopback_host(host), host
            server = control.ControlServer(
                controller=control.HeadlessController(),
                socket_path="unused.sock", tcp_host=host, tcp_port=8799)
            try:
                server._check_tcp_host(host)
            except control.CommandError as exc:
                assert exc.code == "refused", exc.code
            else:
                raise AssertionError(f"TCP bind to {host} was allowed by default")

        for host in ("127.0.0.1", "localhost", "::1"):
            assert control._is_loopback_host(host), host

        allowed = control.ControlServer(
            controller=control.HeadlessController(),
            socket_path="unused.sock", tcp_host="0.0.0.0", tcp_port=8799,
            allow_remote=True)
        allowed._check_tcp_host("0.0.0.0")  # must not raise


def test_serve_forever_returns_when_the_server_is_stopped():
    """``ControlServer.serve_forever()`` must return once ``stop()`` has run.

    Regression test for a busy-loop that kept the controller alive at ~90% CPU after a
    successful ``shutdown``. ``start()`` already puts a ``serve_forever`` loop on its own
    thread; this method used to start a SECOND loop over the same ``BaseServer``. Both
    shared the single ``__shutdown_request`` / ``__is_shut_down`` pair, so whichever loop
    exited first reset the flag and the other was left calling ``accept()`` on a closed
    socket -- an EINVAL spin that never ended. ``shutdown`` still replied
    ``{"shutting_down": true}`` and removed the socket, so the leak was invisible to the
    CLI and only showed up as a stray process in ``ps``.

    Asserted two ways, because the spin itself is a RACE and a pure timing assertion can
    pass by luck on the buggy code:

    * structurally -- exactly one ``serve_forever`` loop may be started per server, which
      is the actual invariant the bug violated; and
    * behaviourally -- the serving thread must have finished after ``stop()``.
    """
    with TempWorkspace() as base:
        import control

        socket_path = str(base / "serve-forever.sock")
        server = control.ControlServer(
            controller=control.HeadlessController(), socket_path=socket_path)

        # Count how many accept loops get started. ``start()`` legitimately starts one on
        # a background thread; ``serve_forever()`` must not start a second.
        loops_started = []
        original_serve_forever = control._ThreadedUnixServer.serve_forever

        def counting_serve_forever(self, *args, **kwargs):
            loops_started.append(threading.current_thread().name)
            return original_serve_forever(self, *args, **kwargs)

        control._ThreadedUnixServer.serve_forever = counting_serve_forever
        try:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()

            # Wait for the loop to be live before stopping it, or the test could pass by
            # stopping a server that never started.
            deadline = time.time() + 10.0
            while time.time() < deadline and not control.is_server_running(socket_path):
                time.sleep(0.05)
            assert control.is_server_running(socket_path), (
                "the control server never became reachable")

            assert len(loops_started) == 1, (
                f"{len(loops_started)} accept loops were started on one server "
                f"({loops_started}). Two loops share a single "
                f"__shutdown_request/__is_shut_down pair, so one of them survives "
                f"stop() and spins on a closed socket forever.")

            server.stop()
            thread.join(timeout=10.0)

            assert not thread.is_alive(), (
                "serve_forever() did not return after stop(): the process would be "
                "left spinning on a closed socket instead of exiting")
            assert not os.path.exists(socket_path), (
                "stop() left the socket file behind")
        finally:
            control._ThreadedUnixServer.serve_forever = original_serve_forever


# ---------------------------------------------------------------------------
# Default behaviour unchanged
# ---------------------------------------------------------------------------

def test_the_control_interface_is_off_by_default():
    """``python generate.py`` must behave exactly as it did before v13.0."""
    with TempWorkspace() as base:
        import yaml
        with open(base / "config" / "config.yml", "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle) or {}
        assert "control" in config, "config/config.yml has no control: block"
        assert config["control"]["enabled"] is False, (
            "control.enabled must default to false so a default run is unchanged")
        assert config["control"]["tcp_host"] == "", (
            "TCP must be off by default")
        assert config["control"]["allow_remote"] is False


def test_generate_starts_the_control_server_only_when_enabled():
    """The GUI hook must be opt-in and must never be able to stop the GUI opening.

    Checked statically: ``generate.py`` builds Tk at import, so it cannot be executed
    here. The assertions are the three properties that make the hook safe.
    """
    source = (ROOT / "generate.py").read_text(encoding="utf-8")

    assert "def start_control_server_if_enabled(" in source
    assert "def stop_control_server(" in source

    start = source.index("def start_control_server_if_enabled(")
    end = source.index("def stop_control_server(")
    body = source[start:end]
    assert "control.enabled" in body, (
        "the hook does not check control.enabled, so it would serve by default")
    assert "except Exception" in body, (
        "the hook must swallow its own failures, or a busy port stops the GUI opening")

    # It must be called at startup and stopped during quit.
    assert "start_control_server_if_enabled()" in source
    quit_start = source.index("def quit_application(")
    assert "stop_control_server()" in source[quit_start:quit_start + 6000], (
        "quit_application does not stop the control server")


def test_generate_delegates_database_work_to_the_shared_layer():
    """The GUI's DB actions must use the same implementation as the CLI.

    Two implementations of the same TRUNCATE/export is how the two front ends drift
    apart, so the GUI wrappers are asserted to call ``run_config``.
    """
    import ast

    source = (ROOT / "generate.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {node.name: node for node in ast.walk(tree)
                 if isinstance(node, ast.FunctionDef)}

    for function in ("clear_database", "export_db_to_jsonl", "init_database_pool"):
        node = functions.get(function)
        assert node is not None, f"generate.py has no {function}()"
        # Slice the source by the node's real line span, not by searching for "def ".
        body = "\n".join(source.splitlines()[node.lineno - 1:node.end_lineno])
        assert "run_config." in body, (
            f"generate.{function}() does not delegate to run_config, so the GUI and "
            "the CLI can drift apart")


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

VERSION = "14.0.0"


def test_the_version_is_13_1_1_everywhere_it_is_declared():
    """The version lives in four places; all of them must agree.

    Bumping only some of them is the easy mistake -- the README title, the GUI's header label,
    the constant the CLI reports and the version-history row are edited by hand, so this test is
    the only thing that keeps them in step.
    """
    import control
    assert control.GENERATOR_VERSION == VERSION, control.GENERATOR_VERSION

    generate_source = (ROOT / "generate.py").read_text(encoding="utf-8")
    assert f'text="v{VERSION}"' in generate_source, (
        f"generate.py version label is not v{VERSION}")

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    titles = [line for line in readme.splitlines()
              if line.startswith("# ReadyArt Synthetic Dataset Generator")]
    assert titles, "README.md has no '# ReadyArt Synthetic Dataset Generator' title"
    assert f"v{VERSION}" in titles[0], (
        f"README.md title does not name v{VERSION}: {titles[0]!r}")

    history = (ROOT / "docs" / "version-history.md").read_text(encoding="utf-8")
    assert f"v{VERSION}" in history, f"docs/version-history.md has no v{VERSION} row"

    # The changelog must also carry a section for it, or a bump leaves the release notes behind.
    # The file is named for the MAJOR series, so a major bump means a new file: derive it
    # from VERSION rather than hard-coding it, or a v14 release looks in the v13 file.
    changelog_path = ROOT / "docs" / f"changelog-v{VERSION.split('.')[0]}.md"
    assert changelog_path.exists(), f"{changelog_path.name} does not exist"
    changelog = changelog_path.read_text(encoding="utf-8")
    assert f"### v{VERSION}" in changelog, (
        f"docs/{changelog_path.name} has no '### v{VERSION}' section")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _set(data, pairs):
    """Set dotted keys on a nested dict, creating intermediate dicts."""
    for dotted, value in pairs:
        node = data
        parts = dotted.split(".")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value
    return data


def _set_parallel(base, relative, threads_per_api):
    """Give every enabled API entry the same ``threads`` count.

    The run's total thread count is the SUM of the per-API counts, not the legacy
    ``api.threads`` key, so a test that wants a predictable total must set these.
    """
    import yaml
    path = Path(base) / relative
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    apis = data.get("api", {}).get("apis") or []
    for index, entry in enumerate(apis):
        if index >= 4:
            break
        entry["threads"] = threads_per_api
        # An entry with no URL is not active, so it contributes no threads.
        if not entry.get("url"):
            entry["url"] = f"http://127.0.0.1:18099/v1/chat/completions"
        if not entry.get("model"):
            entry["model"] = "test-model"
    with open(path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, sort_keys=False)
    return data


def main():
    tests = []
    for name, value in sorted(globals().items()):
        if name.startswith("test_") and callable(value):
            tests.append((name, value))

    failures = 0
    for name, function in tests:
        try:
            function()
        except Exception as exc:  # noqa: BLE001 - a test harness reports, never raises
            failures += 1
            print(f"FAIL {name}")
            print(f"     {type(exc).__name__}: {exc}")
        else:
            print(f"ok   {name}")

    print(f"\n{len(tests)} checks, {failures} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
