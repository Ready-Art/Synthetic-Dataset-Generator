# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the v13.1.1 opt-in prompt traces (``generation.debug_prompt_log``).

The per-request dumps in ``output/debug_prompt*.jsonl`` used to be written UNCONDITIONALLY, one
append per request from every worker and every judge thread, with no size cap. On a long run (40
generation threads plus a 12-wide async judge, thousands of chunks) that is tens of thousands of
appends of multi-kilobyte lines to a handful of shared files, on top of the dataset and the log --
pure diagnostic overhead on the request path, and nothing reads those files during a run.

These tests pin the gate itself:

  * it ships OFF, so a default run writes no trace files at all;
  * ``write_debug_prompt`` is the only writer and it honours the flag;
  * the flag is read at CALL time, so a toggle takes effect without a restart;
  * a trace failure never raises into the request path;
  * ``config/config.yml`` ships the key ``false`` and the editor round-trips it.

No model, network or display is needed.

Run with ``python -B tests/test_prompt_trace_gate.py``.
"""
import json
import os
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
# Import in a scratch directory so logging_config's output/ side effect stays out of the repo.
_tmpdir = tempfile.TemporaryDirectory(prefix="prompt_trace_")
os.chdir(_tmpdir.name)
sys.path.insert(0, str(REPO))

import logging_config

FAILURES = []


def check(name, condition, detail=""):
    if condition:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        FAILURES.append(name)


def main():
    print("prompt trace gate")

    # --- 1. ships OFF ---
    check("defaults to disabled", logging_config.debug_prompt_logging_enabled() is False,
          logging_config.DEBUG_PROMPT_LOGGING_ENABLED)

    # --- 2. disabled writes nothing, and does not create the file ---
    path = os.path.join(_tmpdir.name, "trace_off.jsonl")
    wrote = logging_config.write_debug_prompt(path, {"type": "answer_request"})
    check("write returns False when disabled", wrote is False, wrote)
    check("no file created when disabled", not os.path.exists(path))

    # --- 3. enabled writes one JSON line per call ---
    logging_config.DEBUG_PROMPT_LOGGING_ENABLED = True
    check("enabled reports True", logging_config.debug_prompt_logging_enabled() is True)
    wrote = logging_config.write_debug_prompt(path, {"type": "answer_request", "n": 1})
    logging_config.write_debug_prompt(path, {"type": "answer_request", "n": 2})
    check("write returns True when enabled", wrote is True, wrote)
    with open(path, encoding="utf-8") as handle:
        lines = [line for line in handle.read().splitlines() if line.strip()]
    check("two lines appended", len(lines) == 2, len(lines))
    parsed = [json.loads(line) for line in lines]
    check("lines are valid JSON with the payload intact",
          [p.get("n") for p in parsed] == [1, 2], parsed)

    # --- 4. the flag is read at CALL time, not captured at import ---
    logging_config.DEBUG_PROMPT_LOGGING_ENABLED = False
    before = len(lines)
    logging_config.write_debug_prompt(path, {"type": "answer_request", "n": 3})
    with open(path, encoding="utf-8") as handle:
        after = len([ln for ln in handle.read().splitlines() if ln.strip()])
    check("turning it off stops further writes immediately", after == before,
          f"{before} -> {after}")

    # --- 5. a write failure never raises into the request path ---
    logging_config.DEBUG_PROMPT_LOGGING_ENABLED = True
    unwritable = os.path.join(_tmpdir.name, "no_such_dir", "trace.jsonl")
    try:
        result = logging_config.write_debug_prompt(unwritable, {"type": "x"})
        raised = None
    except Exception as exc:  # noqa: BLE001 - that is the point of the test
        result, raised = None, exc
    check("a failing trace write does not raise", raised is None, raised)
    check("a failing trace write reports False", result is False, result)

    # --- 6. the shipped config carries the key, set to false ---
    import yaml
    with open(REPO / "config" / "config.yml", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    value = cfg.get("generation", {}).get("debug_prompt_log", "<missing>")
    check("config.yml ships generation.debug_prompt_log: false", value is False, value)

    # --- 7. no write site bypasses the gate ---
    # A bare ``open(..., 'a')`` on a debug path is exactly the regression this guards:
    # it would keep dumping traces after the flag was turned off.
    offenders = []
    for module_name in ("generation.py", "quality.py"):
        source = (REPO / module_name).read_text(encoding="utf-8")
        for lineno, line in enumerate(source.splitlines(), 1):
            if "open(" in line and "'a'" in line and "debug" in line.lower():
                offenders.append(f"{module_name}:{lineno}")
    check("no unguarded debug-log open() remains", not offenders, offenders)

    # --- 8. both front ends apply the key at run start ---
    for module_name in ("generate.py", "control.py"):
        source = (REPO / module_name).read_text(encoding="utf-8")
        check(f"{module_name} applies generation.debug_prompt_log",
              "generation.debug_prompt_log" in source)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) failed: {', '.join(FAILURES)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
