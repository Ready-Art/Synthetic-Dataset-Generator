<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# 🎛 Samplers & Infrastructure

```yaml
samplers:
  priority:
    - repetition_penalty
    - top_p
    - top_k
    - temperature
    - max_tokens_answer
  temperature: 0.5
  top_p: 0.9
  top_k: 50
  min_p: 0.0
  repetition_penalty: 1.0
  max_tokens_question: 1024
  max_tokens_answer: 1024
  max_tokens_user_reply: 1024
  enable_thinking: "default"    # "default", "enable", or "disable"
  logit_bias: ""                # JSON format, e.g., {"15": 100}
  use_bad_words: false          # v11.1: toggle -- send `bad_words` with every request
  bad_words: []                 # e.g., [" For Example", "For Example"]

  # Slop Fixer LLM overrides (API Slot 5)
  slop_fixer_params:
    temperature: 0.5
    top_p: 0.95
    min_p: 0.0
    top_k: 50
    repetition_penalty: 1.0
    max_tokens: 200

  # Anti-Slop Fixer LLM overrides (API Slot 6)
  anti_slop_params:
    temperature: 0.5
    top_p: 0.95
    min_p: 0.0
    top_k: 50
    repetition_penalty: 1.0
    max_tokens: 200

  # v13.0.4 thinking mode for the two fixers. SIBLING keys of the blocks above --
  # see the enable_thinking note below for why they cannot go inside them.
  slop_fixer_enable_thinking: "inherit"       # inherit|default|enable|disable
  anti_slop_fixer_enable_thinking: "inherit"   # inherit|default|enable|disable
```

- **`logit_bias`** — JSON object mapping token IDs to bias values. Passed directly to the API payload. Leave empty (`""`) to disable.
- **`use_bad_words` / `bad_words`** (v11.1) — When `use_bad_words: true`, every outgoing request (question, answer, user continuation, opening-mode greeting and first line, plus the Slop Fixer on API Slot 5 and the Anti-Slop Fixer on API Slot 6) gets a top-level `bad_words` list passed straight through to the endpoint, matching vLLM's `chat/completions` field of the same name. In the Config editor this lives in the **Samplers** section as a toggle above the text box, right under the Logit Bias field. Entries keep their leading whitespace exactly as typed, so `For Example` (forbids the phrase anywhere) and ` For Example` (forbids the phrase after a space — i.e., *not* at the start of a turn) can be listed independently:

  ```json
  // vLLM-equivalent request sent by the generator when use_bad_words is on
  {
    "model": "meta-llama/Meta-Llama-3-8B-Instruct",
    "messages": [{"role": "user", "content": "Write a short sentence about the weather."}],
    "temperature": 0.7,
    "bad_words": ["sunny", "rain"]
  }
  ```

  Endpoints that don't understand `bad_words` typically ignore it; use the API compatibility profile (`custom_allowed_params`) to filter it out per slot if your hosted provider rejects unknown fields.
- **`enable_thinking`** — Controls chain-of-thought output in reasoning models. Applies to the question, answer, user-continuation and opening-mode requests, and is the fallback for the Slop Fixer, Anti-Slop Fixer and Quality Judge:
  - `"default"` — Don't send `chat_template_kwargs` parameter (the server's own default applies)
  - `"enable"` — Send `{"chat_template_kwargs": {"enable_thinking": true}}`
  - `"disable"` — Send `{"chat_template_kwargs": {"enable_thinking": false}}`

  **This setting now reaches every request, not just the main four (v13.0.4).** Before v13.0.4 the Slop Fixer, the Anti-Slop Fixer and the Quality Judge sent **no** `chat_template_kwargs` at all, so a reasoning model thought on every rewrite and every score request no matter what this setting said — and on a local vLLM that chain of thought dominated their runtime (users reported roughly a **5x** overall generation speedup after switching thinking off). Each of those three now has its **own** `enable_thinking` key which accepts one extra value:
  - `"inherit"` (the shipped default) — Follow the `samplers.enable_thinking` above

  Set them in the Config editor: **Thinking Mode (Slop Fixer)** and **Thinking Mode (Anti-Slop)** at the bottom of their sampler-override groups in the **Samplers** section, and **Judge Thinking Mode** under the Quality Scoring API fields in the **Generation** section. `"inherit"` plus the shipped `enable_thinking: "default"` resolves to "send nothing", so a config that never mentions these keys behaves exactly as it did in v13.0.3.

  The three keys are `samplers.slop_fixer_enable_thinking`, `samplers.anti_slop_fixer_enable_thinking` and `quality.scoring_api.enable_thinking`. **They are SIBLING keys, never entries inside `slop_fixer_params` / `anti_slop_params` / `sampler_settings`.** Those dicts are read with an all-or-nothing `params or main_sampler_settings` fallback, so a single extra key inside one makes it truthy and stops it inheriting the main sampler defaults — the Slop Fixer's `top_p` would silently jump from the main `0.9` to its hardcoded `0.95`, and `repetition_penalty` from `1.0` to `1.1`, on every rewrite. `tests/test_thinking_mode.py` pins this.

  For a reasoning model, `"disable"` is usually the right answer for the judge and the two fixers: they are short, structured jobs (a small JSON verdict, one rewritten passage) where a chain of thought is pure cost.
- **`max_tokens_user_reply`** — Maximum tokens for LLM-generated user continuation messages.
- **`slop_fixer_params` / `anti_slop_params`** — Leave individual fields blank to inherit from the main sampler defaults.

## Database & Caching

**PostgreSQL:**
```yaml
database:
  enabled: false
  host: "localhost"
  port: 5432
  dbname: "dataset_gen"
  user: "postgres"
  password: "password"
  pool_size: 10
```

When PostgreSQL is enabled, the application initializes a connection pool at startup and stores conversations in the `generated_conversations` table. File writing is skipped entirely when the database is enabled. Use the **Export DB → JSONL** button to export. The application does not create the table; [docs/postgresql.md](postgresql.md) gives the one-time setup.

**Valkey/Redis Caching:**
```yaml
valkey:
  enabled: true
  host: "localhost"
  port: 6379
  db: 0
  password: null
```

When caching is enabled, identical prompts are cached using an MD5 hash of the message content with a 1-hour TTL, avoiding redundant API calls. Cache keys include the API slot index to allow different models to produce different cached responses. The **Test Valkey** button in the main UI allows you to verify connectivity and see server information.

## Budget & Cost Control

The application tracks token usage and estimated costs in real-time. You can set a budget limit to automatically stop generation when spending exceeds the threshold.

```yaml
api:
  pricing:
    cost_per_1k_tokens: 0.002   # Cost per 1,000 tokens (input + output combined)
    budget_limit: 50.0           # Maximum spend in USD (set to 0 to disable)
```

- **`cost_per_1k_tokens`** — The price per 1,000 tokens used to estimate cost
- **`budget_limit`** — When estimated cost reaches this value, generation automatically stops. Set to `0` or leave empty to disable budget enforcement

The budget status is displayed in the metrics bar at the top of the application, showing current spend vs. limit. The label turns red when the budget is exceeded. Budget is checked at the start of each worker loop iteration with thread-safe access to token counters.

## Circuit Breaker

The application includes an automatic **circuit breaker** for each API slot that prevents cascading failures with **exponential backoff**:

```python
API_CIRCUIT_BREAKER = {
    "max_consecutive_failures": 5,   # Failures before circuit opens
    "base_cooldown_seconds": 60,     # Initial cooldown (doubles each failure)
    "max_cooldown_seconds": 600,     # Maximum cooldown cap
    ...
}
```

When an API slot experiences 5 consecutive failures (HTTP errors, timeouts, or exceptions), the circuit breaker **opens** and skips requests to that slot. The cooldown period uses exponential backoff: 60s → 120s → 240s → 480s → 600s (max). After the cooldown period, the circuit **closes** and requests resume. Successful requests reset the failure counter.

Circuit breaker events are logged:
- `API Slot X circuit OPEN after 5 consecutive failures. Backoff: 60s (exponential)` (WARNING)
- `API Slot X circuit closed after 60s. Resuming requests with base cooldown.` (INFO)

## Rate Limiting

Each API slot has a configurable requests-per-minute (RPM) limit. The rate limiter:

1. Tracks request timestamps per slot in a sliding 60-second window
2. When the limit is reached, automatically waits until the oldest request in the window expires
3. Displays real-time usage in the UI with color-coded indicators:
   - 🟢 Green: 0–40% of limit used
   - 🟠 Orange: 40–70% of limit used
   - 🟡 Yellow: 70–90% of limit used
   - 🔴 Red: 90–100% of limit used

Rate limits can be adjusted mid-run by pausing generation, editing the config, and resuming — the new limits are applied automatically.

## HTTP Connection Reuse

Every request the generator makes to an API endpoint reuses one pooled HTTP connection instead of opening a new one. This is automatic — there is no setting.

Before v14.2.0 each request was a bare `requests.post(...)` call, which builds a throwaway session for that one call: a new TCP connection, a new TLS handshake, one request, then close. From every generation thread and every judge thread. Against a local vLLM behind nginx that is **one connection per request**, thousands per minute, and the server's socket table fills with `ESTAB` entries against `:443`:

```
tcp  ESTAB  0  0  192.168.7.12:443  10.7.9.104:56994  users:(("nginx",pid=1117,fd=51))
tcp  ESTAB  0  0  192.168.7.12:443  10.7.9.104:57024  users:(("nginx",pid=1112,fd=37))
tcp  ESTAB  0  0  192.168.7.12:443  10.7.9.104:57236  users:(("nginx",pid=1119,fd=50))
```

That looks like the client spamming connections. It is not extra requests — it is the same requests each paying for a new connection, and the handshake cost was paid on every single one.

**If you see that table, it is not a bug in your server.** Check whether the connections are *accumulating* (a leak, which the pool fixes) or simply *many* (normal for a wide run). Since v14.2.0 the pool keeps at most 64 connections and reuses them, so a 40-thread run holds roughly 40 sockets steady instead of opening thousands.

Measured against a keep-alive server, 120 requests across 8 threads:

| | Connections opened | Wall time |
|---|---|---|
| v14.1.0 (one per request) | 120 | 1.04 s |
| v14.2.0 (pooled) | 8 | 0.67 s |

So the fix is also faster: the TLS handshake is no longer paid per request.

Two details worth knowing if you tune your server:

- **Retries are connect-only.** A failed *connection* is retried (twice). A read timeout is **not** retried, because on a generation request the model may already have produced tokens — resending it would silently duplicate work and bill the slot twice. A read timeout still fails the request and is handled by the normal retry/fix ladder.
- **Idle connections are safe.** nginx closes idle keep-alive connections after `keepalive_timeout` (75 s by default) and the rate limiter can park a slot for up to 60 s, so a pooled connection can be dead when it is next used. That is detected and the connection re-opened transparently; you do not need to raise `keepalive_timeout`.
