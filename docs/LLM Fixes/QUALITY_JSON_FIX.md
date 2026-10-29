# Quality Scoring JSON Robustness

## Problem

The quality judge occasionally returns something that is not valid JSON, and a judge reply that
cannot be parsed drops the conversation (`invalid_json`). With a small local judge (Qwen 3.5 4B and
similar) this happened on roughly one conversation in five, even though the *evaluation* in the reply
was usually correct — only the *packaging* was wrong.

Observed shapes:

```
Unterminated string starting at: line 1 column 2348 (char 2347)   <- reply cut off at max_tokens
Sure! Here is my evaluation: {...}                                 <- prose before/after the object
```json {...} ```                                                  <- fences anywhere in the reply
{"coherence": 80, "logic": 88,}                                    <- trailing comma
{"coherence": "85", "logic": "ninety"}                             <- scores as strings/prose
{"coherence": 80, "naturalness": 78, "engag                          <- cut off mid-key
<thinking>...{...}...</thinking> then the answer                     <- reasoning block before the object
```

## Root causes

1. **A single `json.loads`** on the raw reply, with one regex (`\{[\s\S]*\}`) to "find" the object.
   The regex is greedy and brace-counting-free, so a `{` inside a thinking block matched to the last
   `}` of the reply and produced a string that could never parse.
2. **A token budget that was too small** (`512 + 64`/dimension). Evidence lists and the rewrite
   instruction regularly pushed the reply past the limit; the reply was then cut off mid-string, which
   is what the "Unterminated string" errors actually were one and the same.
3. **No second chance for a format-only failure.** Invalid JSON was final, so a reply with the right
   numbers in the wrong wrapper threw the conversation away.
4. **No diagnostics.** Only a 500-character preview was logged, so the real reply was never seen.

## Solution (v11.5)

### 1. An extraction ladder instead of one `json.loads`

`quality._extract_json_object(content, preferred_keys=dims)` returns `(dict_or_None, how)` and tries,
in order:

| Rung | Function | Handles |
|------|----------|---------|
| direct | `json.loads` | a bare object |
| scan | `_json_objects_in()` + `json.JSONDecoder().raw_decode` at every `{` | thinking blocks, prose before/after, fences anywhere; a candidate carrying the score keys wins over a decoy `{...}` |
| repaired | `_repair_json()` | trailing commas, unescaped quotes in strings, unclosed braces, newlines in strings |
| lenient | `_lenient_json()` | smart quotes, Python literals (`True`/`False`/`None`) |
| truncated | `_repair_truncated_json()` | a tail cut off mid-member (trimmed to the last complete member, then closed) |
| salvaged | `_salvage_scores_from_text()` | `"dimension": 85` read out of text that is not JSON; **every** expected dimension must be present |

`_coerce_score()` turns `"85"`, `"85/100"` and `about 72 out of 100` into 85/85/72 instead of
defaulting to 50.

### 2. A format-repair round before the drop

When nothing parses, `_call_logic_judge()` sends the model's own broken text back (`repair_of=`) with
the original prompt plus "reply again with ONLY the JSON object for the same evaluation". This is not a
transient-failure retry: it is a dedicated format repair, bounded by
`quality.judge_json_repair_attempts` (default **1**, clamped to 0-3; `0` = v11.4 immediate drop). Only
a reply that still fails is a genuine `invalid_json` drop.

### 3. Truncation is transient

`finish_reason: length` means the reply was cut off, not that the model cannot write JSON:

- scores already written → salvaged with `_repair_truncated_json()`, no second API call,
- nothing usable → retried with a **doubled** `max_tokens` (up to 8192).

`quality.judge_max_tokens` (default `0` = automatic `512 + 64`/dimension) sets the budget explicitly;
`quality.scoring_api.sampler_settings.max_tokens` still overrides it. `quality.judge_request_timeout`
(default 120 s, was a hard-coded 60 s) bounds one request; a timeout is transient and only burns a
retry cycle.

### 4. Optional structured output

`quality.judge_structured_output` (`auto` by default) sends `response_format: {"type": "json_object"}`.
An endpoint that rejects the field (a 400 naming `response_format` / `json_object` / `json_schema`) is
retried once without it and remembered in `quality._JUDGE_NO_RESPONSE_FORMAT` for the rest of the run.
`json_schema` builds a real schema from the active dimensions; `off` sends v11.4 payloads.

The judge system prompt also states the format rules explicitly (double quotes, closed strings, bare
numbers, no double quotes inside a value, no markdown).

### 5. Diagnostics

The raw judge reply, its `finish_reason`, and which rung recovered the JSON are appended to the per-slot
debug log (`output/debug_api_slot_<n>.jsonl`, records of type `quality_scoring_response`). A remaining
failure now has the actual text next to it.

### 6. Transient vs final

| Flag | Class | Effect |
|------|-------|--------|
| `llm_timeout`, `llm_exception`, `llm_error_<status>`, `llm_bad_envelope`, `llm_empty_reply`, `llm_truncated` | transient | retried up to `judge_max_attempts` |
| `llm_invalid_json` | final | format-repair rounds, then drop |
| `llm_not_configured` | skip | heuristic scores stand, row kept |

## Compatibility

- Record schema, drop policy and score weights are unchanged.
- `judge_json_repair_attempts: 0`, `judge_max_tokens: 0`, `judge_structured_output: off` reproduce
  v11.4 behaviour exactly.
- All new keys are optional.

## Tests

- `tests/test_quality_json_robustness.py` — extraction ladder, salvage, coercion, format-repair rounds,
  truncation retry, `response_format` fallback, envelope errors.
- `tests/test_quality_judge_policy.py` — retry/drop policy, including the "one repair round, then drop"
  rule and `judge_json_repair_attempts: 0`.
- `tests/test_quality_logic.py`, `tests/test_quality_roleplay.py` — the judge prompts and the per-turn
  logic gate are unaffected by the parsing layer.

## Files

1. `quality.py` — extraction ladder, repair helpers, retry/repair policy, structured output, logging.
2. `config/config.yml` — `quality.judge_json_repair_attempts`, `judge_max_tokens`, `judge_request_timeout`, `judge_structured_output`.
3. `config_editor.py` — the four settings in the Quality Scoring section (save/load/validate).
4. `tests/test_quality_json_robustness.py` — new coverage.
5. `tests/test_quality_judge_policy.py` — updated for the format-repair round.
6. `tests/test_quality_judge_settings_ui.py` — editor save/reopen round trip for the new settings.
