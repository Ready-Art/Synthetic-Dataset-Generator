<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# 🚨 Slop & Anti-Slop — Read This Before You Configure Them

> ## ⚠️ Use AntiSlop-vLLM instead — and use the **`dev`** branch, not `main`
>
> **Recommended tool:** <https://github.com/Ready-Art/antislop-vllm-fork/tree/dev>
>
> The generator's built-in **Slop** and **Anti-Slop** functions are **disabled by default as of
> v12.7.6**, and this page exists to explain why. They are still present and still work if you
> deliberately turn them on, but they are the wrong tool for removing slop from a dataset.
>
> **If you want anti-slop, run it in [AntiSlop-vLLM](https://github.com/Ready-Art/antislop-vllm-fork/tree/dev)
> — and clone the `dev` branch.** The `main` branch of that fork is **12 commits behind `dev`** and is
> missing every bug fix, hang fix and concurrency fix listed in [Why the `dev` branch and not `main`](#why-the-dev-branch-and-not-main).
>
> | | |
> |---|---|
> | ✅ **Use this** | `https://github.com/Ready-Art/antislop-vllm-fork/tree/dev` |
> | ❌ **Not this** | `https://github.com/Ready-Art/antislop-vllm-fork/tree/main` |
>
> ```bash
> git clone --branch dev https://github.com/Ready-Art/antislop-vllm-fork.git
> cd antislop-vllm-fork
> git log --oneline -1   # should be 061a5be or later, NOT 13d7c21
> ```

This page is the canonical reference for **why**. It is written for two audiences: users who are
about to spend an afternoon typing a thousand phrases into the Detection tab, and developers who are
about to "fix" the slop loop by adding another retry.

---

## Contents

- [What the two tools actually do](#what-the-two-tools-actually-do)
- [Twelve reasons to use AntiSlop-vLLM instead](#twelve-reasons-to-use-antislop-vllm-instead)
- [Why the `dev` branch and not `main`](#why-the-dev-branch-and-not-main)
- [What changed in v12.7.6](#what-changed-in-v1276)
- [If you still want the built-in functions](#if-you-still-want-the-built-in-functions)
- [When the built-in functions are the right answer](#when-the-built-in-functions-are-the-right-answer)

---

## What the two tools actually do

They solve the same problem in **opposite directions**, and that difference is the whole argument.

**The generator's built-in functions are a post-hoc detector plus an LLM rewriter.** A reply arrives in
full, `detection.is_slop()` scans it sentence by sentence against your phrase list, and if anything
matches, the reply is sent to a *second* LLM (API Slot 5 for Slop, Slot 6 for Anti-Slop) with an
instruction to rewrite the offending sentence. The model has already committed to the phrase; you are paying
a second model to talk it out of the text after the fact.

**AntiSlop-vLLM is a token-level sampler.** It bans the phrase's **first token** before it can be emitted.
The banned words never appear in the text at all, so there is nothing to rewrite afterwards. It uses the
`logprobs` the API already returns, picks a different token at the moment of the violation, and carries on.
No second model, no rewrite pass, no risk of the rewrite introducing new slop.

That single architectural difference is the source of most of the reasons below.

---

## Twelve reasons to use AntiSlop-vLLM instead

### 1. The built-in fixers cost a whole extra LLM call per detected phrase

Every slop hit sends the reply to API Slot 5. `generation.max_slop_sentence_fix_iterations` ships at **5**
in `config/config.yml`; the anti-slop budget, `generation.max_anti_slop_fix_iterations`, is not a shipped
key at all and falls back to the **10** hard-coded in `generation.py`. A reply that keeps tripping the list
can therefore cost **up to 15 extra full LLM round trips on top of the one that generated it**, per reply —
and `slop_to_anti_slop_fallback` adds more on top. AntiSlop-vLLM does its backtracking **from the logprobs
the generating call already returned**, so a violation costs no additional request.

### 2. Detection cost scales with phrases × sentences, on every reply

`detection.is_slop()` compiles one pattern per phrase and searches it against **every sentence** of **every
reply** (`for phrase_lower, pattern in compiled_patterns: for i, cleaned_sentence in enumerate(...)`). A 1,000-phrase
list against a 40-sentence reply is 40,000 searches, repeated for the question, the answer and every user
continuation, on every turn, forever. This is exactly the "very inefficient especially with a large
slop/antislop list" the README has been warning about. AntiSlop-vLLM resolves a phrase to a **single token ID**
and checks one token position, so the list size barely touches per-token cost.

### 3. A post-hoc rewrite can introduce *new* slop, and nothing catches that

The fixer LLM is told to rewrite one sentence. Nothing constrains the replacement text, so the rewrite can
land on a different banned phrase, a new rhetorical shape, or a fresh tic. The generator does re-check after each
rewrite, which is precisely why it needs `max_*_fix_iterations` of up to 10 — it is chasing its own tail. A
token-level ban cannot produce the banned token in the first place, so the failure mode does not exist.

### 4. The rewrite changes the author's voice, sentence by sentence

Every fix is a separate model call rewriting a separate sentence, each with its own rotating fix instruction from
`detection.*.fixes[]`. Ten rewrites in one reply means ten different instructions from ten different sampling
contexts. The result reads like ten authors. AntiSlop-vLLM's substitution is a **local token swap** inside the
original generation — the sentence keeps its rhythm and its author.

### 5. No n-gram banning at all in the generator

The fork ships `NGramValidator` (uni-, bi-, tri-gram and upgrams, with `remove_stopwords` and `language` options)
and `banlists/banned_ngrams.json`. The generator has **no n-gram validator of any kind** — `detection.py` contains
`is_slop`, `is_anti_slop`, `is_refusal`, `is_user_speaking`, `is_out_of_character`, `is_regex_watch`,
`is_non_latin`, `is_incomplete_quote`, `is_missing_quotation` and `check_scene_continuity`, and none of them is an
n-gram check. A phrase list can only name the slop you already thought of; n-grams catch the repetition you did not.

### 6. Regex is a separate, uncoordinated feature in the generator

Regex Watch (v11.6) is a good feature, but it is a **separate opt-in layer** with its own `enabled` flag, its own
`max_rounds`, and its own retry path back to the **same** main API slot. In the fork, `RegexValidator` runs in the
**same single pass** as the phrase and n-gram validators, so one backtrack satisfies all three. Here, a reply can be
rewritten by Regex Watch and then rewritten again by the Slop Fixer, in two independent loops, for the same sentence.

### 7. Three validators, one pass, versus three loops, three passes

The fork's `core/sampler.py` runs `SlopPhraseValidator`, `RegexValidator` and `NGramValidator` over the same chunk
and backtracks once. The generator runs phrase checks, then Regex Watch, then the Slop Fixer, then possibly the
Anti-Slop Fixer, then the Slop → Anti-Slop fallback — four sequential stages, each with its own retry budget, each
able to undo or re-break what the previous one did.

### 8. The ban lists can be built and validated for your tokenizer

`utils/phrase_token_ids.py` converts a plain JSON list of phrases into a token-ID banlist for a **specific tokenizer**,
de-duplicating and validating each entry — rejecting phrases that encode to zero tokens, produce IDs outside the vocabulary,
or fail a decode round-trip — and writes a `--report` explaining what was kept, dropped and why. The generator has no
equivalent: a phrase that does not tokenize the way you assumed silently never matches, and you have no way to find out.

### 9. The ban lists can be refined automatically

`auto_unslop.py` runs generate → analyse → update banlists → repeat, comparing against a human writing profile and
emitting DPO pairs. The generator's lists are hand-maintained: the only feedback loop is you, reading the dashboard and
typing more phrases into the Config editor's text box.

### 10. The fork produces training data; the generator only removes it

FTPO pair generation (`--ftpo-pairs-jsonl`) captures the `(chosen, rejected)` token pairs from each backtrack — a
preference dataset for free, produced by the filtering you were already doing. `auto_unslop.py` can emit DPO pairs from
successive iterations. The generator's fixers produce a rewritten string and nothing else; the rejected alternative is discarded.

### 11. The fork is a drop-in server, so it filters everything, not just this GUI

`--openai-api --openai-api-port 8080` puts an OpenAI-compatible proxy in front of your backend, so *any* client —
this generator, a notebook, another app — gets anti-slop filtering. The generator's fixers only apply to rows this GUI writes.
If your dataset is assembled from more than one source, the fork is the only one of the two that covers all of it.

### 12. `stream` mode removes the round-trip cost the generator cannot avoid

In `request_mode: chunk` the fork warns when `max_new_tokens / chunk_size >= 10` and recommends `request_mode: stream`,
which removes the repeated prefill cost entirely. The generator has no equivalent: every fixer call is a full new request that
re-sends the whole prompt and the whole conversation history (`use_conversation_history_for_fixers` defaults to `true`, so the
last six messages go too). A fix therefore costs *more* than the generation it is fixing.

---

## Why the `dev` branch and not `main`

**Clone `dev`.** At the time of writing, `dev` is **12 commits ahead of `main` and 0 behind**, and every one of those
commits is a fix:

| Commit | Message |
|--------|---------|
| `061a5be` | Various fixes |
| `b8ec7d4` | **Concurrency fixes** |
| `f4d5a28` | Add files via upload (`utils/phrase_token_ids.py`) |
| `2449963` | Delete `phrase_token_ids.py` (moved into `utils/`) |
| `34cff9a` | Add files via upload |
| `40db7fc` | **Fix Hanging** |
| `9258d23` | Updates |
| `70993f5` | Updates |
| `84fa92a` | Various bug fixes |
| `29edcb4` | Various bug fixes |
| `aa05f3e` | Delete `core/__pycache__` directory |
| `d016b43` | Various bug fixes |

Comparing the two READMEs directly shows what `main` is missing. `dev` documents and ships:

- **Early end-of-turn recovery** (`end_of_turn_markers`, `max_consecutive_stops`) — strips premature `<end_of_turn>` /
  `<turn|>` markers that Gemma-class models emit mid-response, so the marker never leaks into your dataset and the
  response is not truncated mid-sentence. This is a *data-corrupting* defect, and `main`'s README does not mention the feature.
- **Tunable server concurrency** (`max_concurrent_requests`) — lifts the silent 40-thread ceiling in FastAPI's
  `run_in_threadpool()`. Past 40 concurrent requests, `main` **queues them silently**: no error, no `503`, latency climbs
  while throughput stays flat. On a dataset run that looks like "it got slower", not like a bug.
- **The banlist builder** (`utils/phrase_token_ids.py`) — absent from `main`'s project structure and README entirely.
- **Chunk-mode throughput guidance** — `dev` warns when `max_new_tokens / chunk_size >= 10` and recommends `stream`.
- **Known Issues** — `dev` documents the `ApiClient` session-lifetime hazard and the process-global limiter.

So `main` is not "the stable one". For this fork, `main` is the **older** one, and the things it is older than are the
fixes. Use `dev`.

---

## What changed in v12.7.6

- **`detection.slop.enabled`** and **`detection.anti_slop.enabled`** are new keys, and both ship **`false`**.
  A default run therefore performs no slop detection and no slop fixer calls at all.
- **The default run is unchanged in practice.** The shipped `detection.slop.phrases` and `detection.anti_slop.phrases`
  were already `[]`, and `is_slop()` returns `False` for an empty list, so the default run was *already* inert. The
  new flags make that explicit, and stop the loop costs from being paid by someone who pastes a phrase list in
  without realising what that switches on.
- **The phrase lists and fix instructions are untouched.** `phrases`, `fixes`, `max_slop_sentence_fix_iterations`,
  `max_anti_slop_fix_iterations`, `slop_to_anti_slop_fallback`, `use_conversation_history_for_fixers` and the
  `slop_fixer_params` / `anti_slop_params` sampler overrides all keep their current meaning and their current values.
  Turning the flags back on restores the previous behaviour exactly.

---

## If you still want the built-in functions

They work, and they are still supported. Turn them on explicitly:

```yaml
detection:
  slop:
    enabled: true          # default: false (v12.7.6)
    phrases: ["test phrase", "another phrase"]
    fixes: ["Rewrite without using that phrase."]
  anti_slop:
    enabled: true         # default: false (v12.7.6)
    phrases: ["additional phrase"]
    fixes: ["Rephrase to avoid this pattern."]
```

or tick **Enable Slop Detection** / **Enable Anti-Slop Detection** in the Config editor's Detection panel.

Then, in order of how much pain they save:

1. **Configure API Slot 5 and Slot 6.** Without a URL and model on the fixer slot, a detection is logged and the reply is
   kept unfixed. See [Troubleshooting](troubleshooting.md).
2. **Keep the phrase lists short.** Cost is phrases × sentences × turns. A 50-phrase list that catches the slop you
   actually see is worth more than a 2,000-phrase list that catches everything and costs an hour per run.
3. **Leave `slop_to_anti_slop_fallback` off.** It is the most expensive path in the pipeline.
4. **Consider `use_conversation_history_for_fixers: false`** if token cost matters more than rewrite coherence.
5. **Prefer Regex Watch for rhetorical *shapes*.** A phrase list cannot describe `"it is not just a sword, it is a promise"`.
   Regex Watch catches the shape with one expression and one retry, and needs no fixer slot. See
   [Detection & Quality Control](detection-and-quality.md).

---

## When the built-in functions are the right answer

Be honest about the trade-off: the built-in path is the right choice when

- you need the fix to happen **inside this GUI's run**, with its dashboard counters, per-API breakdowns and
  `recent_slop_total` panels, and you do not want a second service to operate;
- your backend **cannot return top-token `logprobs`**. AntiSlop-vLLM's backtracking depends on them and cannot work
  without them; the generator's phrase matching needs nothing but the text;
- you are fixing **one specific phrase in one specific prompt** and the phrase list is a handful of entries, so the
  cost is negligible;
- you are not generating at scale and the extra fixer calls are affordable.

In every other case — a large list, a long run, a dataset you intend to train on — use the fork.
