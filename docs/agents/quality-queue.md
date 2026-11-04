<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# The quality queue and judge concurrency (`quality.py`)

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching the scoring queue, the judge's concurrency, its rate-limit slot, the cost of one conversation, or **async scoring** (`quality.async_scoring`). For the scoring dimensions, the judge request/JSON parsing and the drop policy, read [`quality.md`](quality.md) instead.

**Related:** [`docs/agents/quality.md`](quality.md), [`docs/agents/core-modules.md`](core-modules.md), [`docs/agents/generation.md`](generation.md)

## Contents

- The queue in `quality.py`
- Two modes: GATE and QUEUE (`quality.async_scoring`)
- Judge concurrency (v12.9)
- The judge's rate-limit slot (v12.9)
- Cost of one conversation
- Tests

---

## The queue in `quality.py`

Every judge request goes through one queue, so the judge's concurrency is capped by
`quality.judge_concurrency` rather than by how many workers happen to be running.

Structure:

- `_quality_scoring_queue` — a plain `Queue()`, **unbounded**, so a job is never refused.
- Jobs are 11-tuples `(conversation_history, task_id, thread_id, api_slot_idx, file_name,
  character_profile_text, scene_characters, logic_findings, cast_schedule, cast_events, job)`.
  The consumer unpacks `item[:10]` with `job = item[10]`, so **adding a field means updating
  both the producer and that unpack**. Mode-specific data goes INSIDE `job` for that reason —
  `write_job` (the async row writer) lives in `job["write_job"]`, not in a 12th tuple slot.
- `_quality_processor_threads` — a list of processor threads (v12.9; was a single handle).
- `_quality_queue_lock` — guards the pool's start/stop and the thread list, **not** the queue.
- `_quality_processor_running` / `_quality_draining` — the stop flag every processor checks.

`enqueue_quality_scoring()` falls back to **inline** scoring when `_quality_processor_running`
is false (tests, or scoring disabled). `_record_quality_result()` stores a score in
`app_state.quality_scores`, or a drop in `app_state.quality_dropped` / `app_state.dropped_task_ids`
— a drop is never stored as a score and never flagged for review. A later successful verdict
supersedes an earlier drop for the same task (a dropped task stays retryable).

## Two modes: GATE and QUEUE (`quality.async_scoring`)

**GATE (default, `false`).** The judge is a gatekeeper: the worker blocks on its job's
`threading.Event` until the verdict arrives, because a conversation the judge rejects must never
reach the dataset (v11.4). The wait is bounded by `quality.result_timeout` (600 s), which is why a
slow judge DROPS rows as `judge_failed`. `_process_gate_job()` serves this mode.

**QUEUE (`true`).** The worker hands over a COMPLETE write job and returns immediately;
`_score_and_write_job()` scores it and calls the injected writer itself. Consequences:

- **Generation never waits for a verdict**, so a slow judge cannot hold up generation.
- **Nothing can be lost to a timeout** — there is no wait to time out of. `quality.result_timeout`
  is not consulted in this mode at all. A queued job stays queued until it is scored.
- **The row is written only AFTER its verdict**, so a written row is always complete and a
  rejected conversation never appears in the dataset. This is the mode's whole point: rows land later
  and out of order, but never unscored and never lost.
- **The run does not end until the queue is drained** (`wait_for_quality_queue()`), which is what
  makes "the judge continues until it is finished" true. `generate.wait_for_completion()` calls it
  after the workers join, and deliberately **not** gated on `stop_processing`: a Stop means "stop
  generating", not "throw away conversations the judge already owes us".
- A rejected conversation is a DELIBERATE removal, not a loss: it is recorded with its
  `drop_reason` and the row is never written.
- A scored row that cannot be WRITTEN (disk full, DB down) is counted in
  `app_state.quality_write_failures` and logged at ERROR. The verdict is still recorded in
  `quality_scores`, so the judge's work is not thrown away; the row is just absent.
- A Stop still drains first (`wait_for_quality_queue` returns False only if it is shut down
  mid-drain, which does not happen because the drain runs before shutdown).

The writer is **injected** with `set_quality_write_handler(handler)`; `generation._install_quality_write_handler()`
registers it at run start, before `initialize_quality_queue()`. It is injected rather than imported
because `generation.py` already imports `quality`, so `quality` importing `generation` would be circular.

**`generate.py` must import the generation MODULE, not only names from it.** The call site is
`generation._install_quality_write_handler()`, and `from generation import worker, ...` binds `worker`
but never the name `generation`. v12.9 shipped without the plain `import generation` and every async
run died on the first Start click with `NameError: name 'generation' is not defined` inside the Tk
callback — invisible to the default run and to every import-time check, because the branch is gated on
`async_scoring` **and** `use_llm_scoring`. The install must also stay BEFORE
`initialize_quality_queue()`, so a processor can never pick up a job with no writer.
`tests/test_undefined_globals.py` (v12.9.1) fails on any global a function references but its module
never binds, and additionally pins this call site, the attribute it names and the module import.

`generation._make_quality_write_job(write_conversation_kwargs, on_written)` binds everything the
worker would have passed to `write_conversation` EXCEPT `quality_result`, and the helper fills that in.
The row is therefore byte-identical to the worker's own write (guarded by
`tests/test_quality_async_queue.py`). `on_written` (`_mark_task_written`) marks the task complete
**only after the row is on disk** — marking it earlier would let an interrupted run skip it on resume
with no row to show.

## Judge concurrency (v12.9)

`quality.judge_concurrency` (**1-32**, default **1**; read by `judge_concurrency()`) sizes the pool.
`initialize_quality_queue()` starts that many `_quality_queue_processor()` threads;
`shutdown_quality_queue()` joins them all with a budget of `max(2.0, 0.5 * n)` each.

**Only the pool's width changed.** The queue is already thread-safe and every job carries its own
`threading.Event`, so a new thread needs no per-job coordination — do not add any to the loop body.
The default of 1 keeps a default run byte-identical (`AGENTS.md`: features stay opt-in).

**Size it to the judge SERVER, not the worker count:**

```
W  >=  threads * judge_latency / seconds_per_conversation
```

capped at the server's `--max-num-seqs`. A local vLLM that degrades past 2-3 concurrent sequences
wants `2` or `3`, not `8` — a larger `W` only relocates the queue into vLLM's scheduler, where it is
harder to see and costs the same latency. Context matters too: one judge request carries
`quality.max_chars_for_scoring` plus the profile block, so a wide `W` can exhaust KV cache and trigger
preemption, making latency *worse* than the single-threaded case.

Why the pool matters at all: with `T` workers and `W` judge slots, the last waiter in line waits about
`(T / W) * L`. Once that passes `quality.result_timeout` the row is dropped as `judge_failed`, so at
`T=30, W=1` a 20-30 s judge drops healthy conversations purely for queueing behind each other. In
QUEUE mode this arithmetic does not apply, because nothing waits.

## The judge's rate-limit slot (v12.9)

The judge calls `global_rate_limiter.wait_if_needed(slot)` and
`_record_judge_response_time(slot, ...)` with `scoring_api_slot`, which defaults to **6**
(`quality._score_with_llm()`, `quality.check_logic()`).

`api_handler` sizes `api_response_times_per_slot`, `RateLimiter.requests_per_slot` and
`RateLimiter.rates_per_slot` from `api_handler.RATE_LIMITER_SLOT_COUNT` (**7**, 0-6). Before v12.9
that constant did not exist and all three were **6 wide (0-5)**, so every judge request raised
`KeyError` inside a bare `try/except: pass`. The judge was therefore **never rate-limited and its
latency was never recorded** — a saturated judge was invisible on the dashboard and `rate_limit_rpm`
on it did nothing. The limiter failure is now logged at `WARNING` rather than swallowed.

`quality.scoring_api.slot_idx` (0-6, default 6) is a real config key with an editor field. Index 6 is the
judge's own **logical** slot — it has no `api.apis` entry, because the judge is configured under
`quality.scoring_api`. Point it at a generation slot to make the judge share that slot's `rate_limit_rpm`.

**If you add another logical slot, widen `RATE_LIMITER_SLOT_COUNT`** (and the `*_per_api` dicts in
`app_state.py`, which are widened to match). A slot outside those structures silently disables both
throttling and latency telemetry, because both call sites are exception-tolerant by design.

## Cost of one conversation

One conversation costs `judge_max_attempts * (1 + judge_json_repair_attempts)` requests of
`judge_request_timeout` each. The defaults (3 attempts, 1 repair round, 120 s) allow **~723 s of
occupancy — longer than `result_timeout` (600 s)**, so a single flaky conversation could stall the queue
past every other waiter's deadline. Lower `judge_max_attempts` to `1` and the repair rounds to `0` before
raising any of the three, and raise `judge_concurrency` instead.

In QUEUE mode this cost still matters (it is throughput, not a drop), but a long conversation no longer
drops anyone else's row.

`quality.per_turn_logic_check` sends `check_logic()` requests through **this same queue** via
`submit_judge_request()`, so they share `judge_concurrency`, the rate limiter and the drain. They are
NOT conversation scores: nothing is written and nothing is recorded in `app_state` — the caller decides
what to do with the reply. Each still costs one extra judge call per turn, so leave the feature off
unless you have deliberately sized the judge for it.

## Tests

- `tests/test_quality_judge_policy.py` — `judge_concurrency()` defaulting and clamping, N
  processors serving N jobs truly in parallel, and `judge_concurrency: 1` still serialising.
- `tests/test_quality_json_robustness.py` — `test_judge_slot_is_rate_limited_and_measured()`: the
  judge's slot is inside the limiter's range, a request is recorded for throttling, a response time is
  recorded, and `quality.scoring_api.slot_idx` is honoured.
- `tests/test_quality_async_queue.py` — async mode end to end: generation returns immediately, the
  drain waits, nothing is lost to a short timeout, a rejected conversation is never written, a write failure
  is counted, all queued jobs are written, and `async_scoring: false` keeps the blocking gate.
- `tests/test_undefined_globals.py` — the wiring itself: no application module references a global it
  never binds (the v12.9.1 `NameError` class), and `generate.start_processing()`'s
  `generation._install_quality_write_handler()` call resolves to a bound module object whose attribute
  really exists.
