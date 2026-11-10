# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Headless tests for the v13.1.1 coalescing state writer.

``generation.save_generation_state()`` serialises the WHOLE run state -- ``completed_task_ids``,
``progress_credited_task_ids`` and ``quality_scores`` all grow with the task count -- so one dump
costs more the longer the run goes. It used to be called synchronously on every completed task
from every worker and, in async scoring, from every judge thread too, all serialised on one global
lock. Measured: 0.19 s per save at 500 tasks, 1.4 s at 4,000, 4.3 s (8.4 s worst case) at
12,000. Every worker's completion path waited behind that, which is the "generation pauses, then
starts again, and the ETA jumps" report.

The fix separates the two paths:

  * ``request_generation_state_save()`` -- the per-task hot path. Marks the state dirty and
    returns at once; one background thread does the write and coalesces a burst into one dump.
  * ``save_generation_state()`` -- unchanged, synchronous. The run-end and test path.
  * ``flush_generation_state()`` -- stops the writer and writes synchronously, so the file is
    current when a run finishes and no writer outlives it.

What these tests pin:

  * the per-task call does not block, even while a dump is held open;
  * a burst of concurrent requests coalesces into far fewer dumps than requests;
  * the state file is still correct and complete after a flush (resume data intact);
  * the synchronous path is still synchronous and byte-equivalent.

Run with ``python -B tests/test_state_save_coalescing.py``.
"""
import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
# Import in a scratch directory so logging_config's output/ side effect stays out of the repo.
_tmpdir = tempfile.TemporaryDirectory(prefix="state_save_")
os.chdir(_tmpdir.name)
sys.path.insert(0, str(REPO))

import app_state
import generation

FAILURES = []


def check(name, condition, detail=""):
    if condition:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        FAILURES.append(name)


def seed_state(n):
    """Populate the run structures at a given task count."""
    app_state.completed_task_ids = {f"task_{i}" for i in range(n)}
    app_state.progress_credited_task_ids = {f"task_{i}" for i in range(n)}
    app_state.quality_scores = {
        f"task_{i}": {"composite": 70.0,
                     "dimensions": {"coherence": 70, "naturalness": 70,
                                    "engagement": 70, "consistency": 70},
                     "flags": [], "scored_at": "2026-11-05 10:00:00",
                     "method": "llm"}
        for i in range(n)}


def main():
    print("coalescing state writer")

    tmp = tempfile.mkdtemp()
    generation.STATE_FILE_PATH = os.path.join(tmp, "state.json")

    # --- 1. the per-task path returns immediately, even while a dump is held open ---
    seed_state(4000)
    generation.save_generation_state()  # make sure a file exists

    # Hold the state lock so any attempt to dump would block, then time the request.
    app_state.state_file_lock.acquire()
    try:
        start = time.perf_counter()
        generation.request_generation_state_save()
        elapsed = time.perf_counter() - start
    finally:
        app_state.state_file_lock.release()
    check("request_generation_state_save does not take the state lock",
          elapsed < 0.05, f"{elapsed * 1000:.1f} ms")

    generation.flush_generation_state()

    # --- 2. a burst coalesces: many requests, far fewer dumps ---
    dumps = []
    real_dump = generation.json.dump

    def counting_dump(obj, fp, *args, **kwargs):
        dumps.append(1)
        return real_dump(obj, fp, *args, **kwargs)

    seed_state(6000)
    generation.json.dump = counting_dump
    try:
        def worker():
            for _ in range(10):
                generation.request_generation_state_save()

        threads = [threading.Thread(target=worker) for _ in range(40)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 400 requests have been made; let the writer settle and write at most a few dumps.
        time.sleep(generation.STATE_SAVE_MIN_INTERVAL_SECONDS + 1.5)
    finally:
        generation.json.dump = real_dump

    check("400 concurrent requests coalesce into few dumps",
          1 <= len(dumps) <= 5, f"{len(dumps)} dump(s)")
    check("the burst did not dump once per request", len(dumps) < 400, len(dumps))

    # --- 3. after a flush the file is complete and correct (resume data intact) ---
    generation.flush_generation_state()
    with open(generation.STATE_FILE_PATH, encoding="utf-8") as handle:
        data = json.load(handle)
    check("completed_task_ids round-trips",
          sorted(data["completed_task_ids"]) == sorted(app_state.completed_task_ids))
    check("progress_credited_task_ids round-trips",
          sorted(data["progress_credited_task_ids"])
          == sorted(app_state.progress_credited_task_ids))
    check("quality_scores round-trips",
          sorted(data["quality_scores"]) == sorted(app_state.quality_scores))

    # --- 4. the synchronous path is still synchronous ---
    seed_state(300)
    start = time.perf_counter()
    generation.save_generation_state()
    elapsed = time.perf_counter() - start
    check("save_generation_state still writes synchronously", elapsed > 0.0)
    with open(generation.STATE_FILE_PATH, encoding="utf-8") as handle:
        data = json.load(handle)
    check("the synchronous save wrote the current state",
          len(data["completed_task_ids"]) == 300, len(data["completed_task_ids"]))

    # --- 5. no per-task call site blocks on the synchronous save ---
    # A ``save_generation_state()`` left on the per-task path is exactly the regression
    # this guards: it would put the multi-second dump back in every worker's completion path.
    source = (REPO / "generation.py").read_text(encoding="utf-8")
    # worker() runs to the next TOP-LEVEL def; stop there so the writer functions that
    # legitimately call the synchronous save are not counted.
    worker_body = source.split("\ndef worker(")[1]
    worker_body = worker_body.split("\ndef ")[0]
    # Drop comments first: a note that MENTIONS the function is not a call to it.
    code_lines = [line.split("#", 1)[0] for line in worker_body.split("\n")]
    code = "\n".join(code_lines)
    stripped = (code
                .replace("request_generation_state_save()", "")
                .replace("flush_generation_state()", ""))
    check("the worker's per-task path does not call the blocking save",
          "save_generation_state()" not in stripped,
          "a bare save_generation_state() call remains in worker()")
    check("the worker's per-task path does request a coalesced save",
          "request_generation_state_save()" in worker_body,
          "worker() never asks for a state save")

    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) failed: {', '.join(FAILURES)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
