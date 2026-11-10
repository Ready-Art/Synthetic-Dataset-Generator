<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# Gemma 4 Chat Template Fix for axolotl

**Tool:** [`tools/gemma4_thought_fix.py`](../tools/gemma4_thought_fix.py)
**Written by:** **Karki**, a user on Discord
**Applies to:** `gemma-4-26B-A4B-it` and `gemma-4-31B-it` fine-tunes **with thinking turned off**
**Runs against:** your own axolotl config and dataset — nothing in this repository

---

## Credit

**This script is Karki's work, not ours.** Karki, a user on Discord, wrote it and shared it with the
community. This repository did not write it.

What we did was apply a few fixes for robustness — missing paths, unreadable configs and unparsable
YAML now print a readable message instead of a traceback, `--out` creates its parent folder, an empty
data file says so, and a few messages agree with their counts. The design, the two-step structure, the
template patch and the token-level `verify` step are all Karki's, and credit for them belongs to Karki.

If you find this useful, thank Karki, not the maintainers of this generator.

---

## Scope: this is not part of this project

> **This document and `tools/gemma4_thought_fix.py` are outside the scope of the Synthetic Dataset
> Generator, and unrelated to it.** Nothing in this application reads them, imports them, tests them or
> depends on them, and no generated output is affected by them. The tool is not part of the generation
> pipeline, the Quality Scoring engine, the detection passes, the Datasets tab, or the axolotl-style
> output format this application writes.

It is shipped here **only to help people who are training Gemma 4**. If you generate a dataset with
this application and then fine-tune Gemma 4 on it with axolotl, this tool is the fix for a specific
Gemma 4 / axolotl interaction that will otherwise quietly damage your model. If you are not training
Gemma 4, you can ignore this document completely.

The tool is standalone: standard library plus `PyYAML`, `jinja2`, and (for the `verify` step only)
`datasets` and `tokenizers`. It is not covered by `tests/`, not run by the test loop in `AGENTS.md`,
and not part of the dependency graph described there.

---

## The problem in one paragraph

Gemma 4 26B and 31B expect **every** reply to open with a thought channel. With thinking off, that
channel is empty:

```
<|channel>thought\n<channel|>
```

Gemma's chat template writes that empty block only for the reply **being generated** — the reply after
the generation prompt. It never writes it for the replies **inside** a conversation. axolotl, however,
trains every reply in the conversation. So each reply gets trained straight after `<|turn>model\n`,
with no thought block in front of it. The model learns, from every single training row, that a reply
starts immediately with prose. Then you turn thinking on at inference and it does not think.

Google says as much in the Gemma 4 prompt-formatting guide
(<https://ai.google.dev/gemma/docs/core/prompt-formatting-gemma4>):

> When fine-tuning gemma-4-26B-A4B-it and gemma-4-31B-it with a dataset that does not include
> thinking, you can achieve better results by adding the empty channel to your training prompts.

The fix is therefore not in the data and not in the weights — it is in the **template that turns the
data into tokens**.

---

## The fix, and why it is split in two

The tool does two things, because the empty block can be written in two places and only one of them
is safe.

### Part 1 — the template writes the block (this is the fix)

A **training copy** of your chat template writes the empty block before **every** model reply. The
block is written by the template, so it lands *outside* the reply text, and axolotl masks it. The
model sees the block in front of every reply during training, and is never trained to produce it —
exactly matching what the base model already does at inference.

Two consequences worth being explicit about:

- No reply is trained straight after `<|turn>model\n` any more. That is the whole point.
- The block is **masked**, not trained. If it were trained, you would teach the model to emit the
  block as ordinary text and it would show up verbatim in your outputs.

### Part 2 — the data carries no block (this keeps it correct)

If a reply's **text** starts with a thought block, axolotl would train on it (it is inside the
labels), or Gemma's stock template would delete it — either way the result is wrong. So the rule the
tool enforces on your data is:

> **Reply text carries no thought block. The template writes it.**

Any block found at the start of a reply's text is removed, and the rest of the line is left byte for
byte identical. The original file is never overwritten.

---

## Step by step

Run both steps **inside your axolotl environment**, from the folder you run axolotl from. No GPU is
needed, and no model is loaded.

### Step 1 — check and fix the data and the template

```sh
python3 tools/gemma4_thought_fix.py data.jsonl --config your_axolotl_config.yml
```

`data.jsonl` is the file you are about to train on. The tool prints five numbered sections. Here is
what each one means.

#### `== 1. Data format`

Every line must be one JSON object, in one of two shapes:

```json
{"conversations": [{"from": "system", "value": "..."}, {"from": "human", "value": "..."}, {"from": "gpt", "value": "..."}]}
{"messages": [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]}
```

Roles are mapped the way axolotl maps them (`human`/`user` → user, `gpt`/`assistant` → assistant,
`system` → system), unless your config's dataset entry has its own `roles:` block, in which case that
is used instead. Only roles mapped to **assistant** are trained.

The check also reports:

| Finding | What it means |
|---|---|
| `not valid JSON` | The line does not parse. Not checked further; fix or remove it. |
| `a turn lacks "from" or "value"` (or `"role"`/`"content"`) | A turn is missing the field axolotl reads. |
| `a turn's "value" is not plain text` | The content is a list/dict, not a string. |
| `role 'model' renders as a reply, but axolotl never trains it` | `model` is what the *template* calls the assistant, not what axolotl calls it. Rename to `assistant`/`gpt`, or map it under `roles:`. |
| `unknown role '…'` | Not one of user/assistant/system/tool (or your `roles:` mapping). |
| `no model reply in the sample` / `the sample does not end with a model reply` | Nothing to train, or the last turn is not a reply. |

If your samples use `messages/role/content` but the dataset entry says
`field_messages: conversations`, the tool says so and prints the two config keys to add. That is a
config bug, not a data bug, and it is reported as a `NOTE`.

#### `== 2. Thought tokens in the data`

This is the part-2 rule above, applied to every reply. Six findings, only the first of which is
fixable automatically:

| Finding | Fixable? | What it means |
|---|---|---|
| `block_in_text` | **yes** | A reply's text starts with an **empty** block. The template writes that block itself, outside the text, where axolotl masks it. The fix removes it. |
| `real_thought` | no | A reply's text starts with a **non-empty** thought. Gemma's template deletes thoughts written there, so the model never sees it. Move it to a `reasoning_content` field on the final reply, or delete it. |
| `empty_reply` | no | A reply is empty, or is only a thought block. axolotl skips an empty reply, so nothing in it is trained and the model sees an empty turn. |
| `stray` | no | A lone `<|channel>` or `<channel|>` inside a reply (e.g. `draft<channel|>reply`, a generation leak). Gemma's template deletes the token and joins the text around it. |
| `nonmodel` | no | A thought token in a user or system message. |
| `think` | no | `<|think|>` — the thinking-**on** switch — appears somewhere. In data with no thoughts this teaches the model to skip thinking when asked to think. |

Everything the tool will not fix by itself is written to `<data>.review.txt`, one line per sample with
its reason, so you can work through it.

If any sample ends in a reply carrying `reasoning_content` (a real thought), the tool notes it: that
reply renders its thought **instead of** the empty block. That is correct for a thinking sample, and it
is why people who mix thinking and non-thinking data train only the last reply.

#### `== 3. Fix the data`

If any `block_in_text` samples were found, the tool asks before writing `<data>.fixed.jsonl` (or
`--out PATH`). It refuses to overwrite anything, including the input file. Every line it does not
change is copied byte for byte, including the byte-order mark and the original line endings, and the
JSON style of each changed line is preserved.

#### `== 4. Does every reply get the empty block in training?`

This is where the tool stops guessing and **renders your samples through the template** axolotl would
actually use. It finds that template from your config, following axolotl's own rules:

1. The dataset entry's own `chat_template`, if it names one;
2. otherwise the top-level `chat_template` / `chat_template_jinja`;
3. otherwise `tokenizer_default`, i.e. the base model's own template.

For `jinja`, the `chat_template_jinja` path is opened from the current folder — the same folder axolotl
opens it from. If that file is not there, the tool stops with an explanation, because axolotl would
silently use the path text *itself* as the template, which is a trap worth catching before a run.

Each sample is rendered with the same Jinja environment transformers' `apply_chat_template` uses, and
every reply is inspected on its own: does it start with exactly the empty block, and does it contain
exactly one thought block?

If `chat_template_kwargs` are set in your config, they are passed through and printed, because a
`enable_thinking: true` there switches thinking **on** and the check reports that as the failure
reason rather than as a missing block.

#### `== 5. Fix the template`

If section 4 failed, the tool patches the template. The patch recognises the exact code shape every
Gemma 4 template revision uses to render a message's reasoning as its thought:

```jinja
{%- if thinking_text and <condition> -%}
    {{- '<|channel>thought\n' + thinking_text + '\n<channel|>' -}}
{%- endif -%}
```

and adds an `elif` that writes the empty block when a reply renders no thought:

```jinja
{#- TRAINING PATCH (gemma4_thought_fix.py): the empty thought block before every model reply that renders no thought -#}
{%- elif role == 'model' and not add_generation_prompt and not continue_same_model_turn -%}
    {{- '<|channel>thought\n<channel|>' -}}
```

The three conditions matter:

- `role == 'model'` — only replies, never user or system turns.
- `not add_generation_prompt` — the generation prompt is not a reply being trained, and the template
  already handles it.
- `not continue_same_model_turn` — a continuation of the same model turn must not restart with a new
  thought block.

The patched file is written as `chat_template.train.jinja` **beside your data**, never into axolotl's
package. Your original template is untouched, and the tool prints the two config lines that use it:

```yaml
chat_template: jinja
chat_template_jinja: /absolute/path/to/chat_template.train.jinja
```

Then it re-renders every sample through the patched template to prove the patch works.

The tool refuses to patch in three cases, and says so instead of guessing:

- The template is **already patched** — so whatever failed above is something the patch does not
  address.
- The template is from an **earlier version of this tool**, which put the block on the final reply
  only. Point `chat_template_jinja` back at your original template and run again.
- The thought-rendering code is **not the Gemma 4 form**. Download `chat_template.jinja` from
  `google/gemma-4-31B-it` and pass it with `--template`.

### Step 2 — the proof, on the tokens axolotl actually produced

Step 1 read your template. Step 2 reads axolotl's output. Only this step can say the data is ready.

```sh
axolotl preprocess your_axolotl_config.yml
python3 tools/gemma4_thought_fix.py verify last_run_prepared --tokenizer /path/to/base_model
```

`last_run_prepared` is axolotl's default `dataset_prepared_path`; pass your own if you set one. Step 1
prints this command with your config's paths already filled in.

For every row, and every reply in it, `verify` checks four things:

| Check | Failure message |
|---|---|
| The reply starts with the empty block | `a reply does not start with the empty thought block` |
| The block is **masked** (all labels `-100`) | `the empty block is trained on; it should be masked` — check `train_on_inputs`, and that `eot_tokens` lists only `<turn|>` |
| The reply after the block is trained | `a reply is trained, but not its first token after the block` / `no reply in the row is trained on` |
| Nothing switches thinking on | `<|think|> (thinking ON) in a row with no thought` |

It also reports `a stray <|channel> or <channel|> inside a reply`.

A final reply carrying a real thought is accepted as a reasoning sample, and counted separately.

axolotl writes a **new** prepared folder for every config it prepares. `verify` checks the newest one
and lists the others with their timestamps, so an older run is never mistaken for the current one.
That is also why you must re-run `axolotl preprocess` after changing the config, and train with that
same config: any change makes axolotl prepare a folder that `verify` has not checked.

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Step 1: `RESULT: PASS`. Step 2: `RESULT: FIXED`. Ready to train. |
| `1` | `NOT READY` (lines need a hand check), `NOT FIXED`, or `NOT CHECKED` (no `--config`/`--template`, so the template was never examined). |
| `2` | The data file has no usable conversation at all. |

`NOT READY` is not a failure of the fix: everything else passed, and the lines listed in
`<data>.review.txt` are the ones only you can decide about.

---

## Options

### `gemma4_thought_fix.py <data>`

| Flag | Meaning |
|---|---|
| `--config PATH` | Your axolotl config. The tool reads which chat template it trains this file with, and the config's `chat_template_kwargs`, `roles:`, `field_messages`, `message_property_mappings`, `roles_to_train`, `train_on_inputs`, `dataset_prepared_path` and `base_model`/`tokenizer_config`. |
| `--template PATH` | Check a template you choose instead of reading the config: a `.jinja` file, a `tokenizer_config.json`, or a model folder. With `--config`, this **overrides** it — make sure it is the same template the config trains with. |
| `--out PATH` | Where to write the fixed data (default `<data>.fixed.jsonl`). Parent folders are created. |
| `--yes` | Answer yes to every "write this file?" question. Without it, a non-interactive run writes nothing and says so. |

### `gemma4_thought_fix.py verify <prepared> --tokenizer <model>`

| Argument | Meaning |
|---|---|
| `<prepared>` | Your config's `dataset_prepared_path` (default `last_run_prepared`). |
| `--tokenizer` | The base model folder, its `tokenizer.json`, or a Hub repo id. |

`verify` needs the `datasets` and `tokenizers` packages; run it inside your axolotl environment.

---

## A worked run

Input `data.jsonl` — two samples, the second of which has a block in its reply text:

```
{"conversations": [{"from": "system", "value": "You are a helpful assistant."}, {"from": "human", "value": "What is a stack?"}, {"from": "gpt", "value": "A stack is a last-in-first-out list."}, {"from": "human", "value": "And a queue?"}, {"from": "gpt", "value": "First-in-first-out."}]}
{"conversations": [{"from": "human", "value": "Say hi."}, {"from": "gpt", "value": "<|channel>thought\n<channel|>Hi!"}]}
```

```sh
python3 tools/gemma4_thought_fix.py data.jsonl --config axolotl.yml --yes
```

```
== 1. Data format: data.jsonl
  OK   2 of 2 samples are conversations in the expected format.

== 2. Thought tokens in the data
  Rule: reply text carries no thought block. The training template writes the empty block
  before every reply, outside the text, where axolotl masks it.
  FAIL 1 of 2 samples follow the rule.
  FAIL 1 sample: a reply's text starts with an empty thought block. … (lines 2)

== 3. Fix the data
  The fix removes the thought block from the reply text of 1 sample and copies every other line byte for byte.
  OK   Wrote data.fixed.jsonl. Your original file is untouched. See every change with:  diff data.jsonl data.fixed.jsonl

== 4. Does every reply get the empty block in training?
  Template: chat_template.jinja
  Found from: chat_template_jinja: chat_template.jinja

  Rendered 2 samples through chat_template.jinja:
  FAIL 0 of 2: every reply starts with the empty thought block.
  FAIL 2 samples: a reply does not start with the empty thought block  (lines 1, 2)

== 5. Fix the template
  Your template does not write the empty block on the replies inside a conversation (Gemma's
  stock template and axolotl's gemma4 write it only for a reply being generated). …
  OK   Wrote chat_template.train.jinja. Your original template is untouched.

  Rendered 2 samples through chat_template.train.jinja:
  OK   2 of 2: every reply starts with the empty thought block.

RESULT: PASS with the training template.
  In your axolotl config, at the top level, set:
    chat_template: jinja
    chat_template_jinja: /absolute/path/chat_template.train.jinja
  That applies to every dataset in the config without a template of its own. Leave any
  chat_template: tokenizer_default on a dataset entry as it is.
  Train on data.fixed.jsonl. axolotl saves this training template into the trained model's folder
  (chat_template.jinja, or "chat_template" in tokenizer_config.json); before uploading, replace it
  with the template you normally ship, the base model's chat_template.jinja.

LAST STEP, the proof: …
    axolotl preprocess axolotl.yml
    python3 gemma4_thought_fix.py verify last_run_prepared --tokenizer google/gemma-4-31B-it
```

`diff data.jsonl data.fixed.jsonl` shows exactly one change: the block is gone from the second reply.

---

## One thing to remember before you upload

axolotl copies the chat template it trained with into the trained model's folder — as
`chat_template.jinja`, or as `chat_template` in `tokenizer_config.json`. **Replace it with the
template you normally ship** (the base model's `chat_template.jinja`) before publishing. The training
template writes an empty thought block before every reply, which is what you want during training and
not what you want in the artefact you distribute.

---

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `Your config's chat_template_jinja 'x.jinja' is not found from this folder` | axolotl opens that path from the folder you run axolotl in, and when the file is not there it silently uses the path text itself as the template. Run the tool from that folder, or make the path absolute. |
| `Your config's datasets differ in chat template or roles, and none of them is data.jsonl` | More than one dataset entry in the config and the tool cannot tell which one this file belongs to. Use a config holding only this file's entry, or `--template`. |
| `Your config uses axolotl's built-in chat_template: gemma4` | Run inside your axolotl environment so the built-in template can be read, or pass `--template` with the `.jinja` file. |
| `--config needs PyYAML` / `The render check needs jinja2` / `verify needs the datasets and tokenizers packages` | Run the tool inside your axolotl environment; it has all of them. |
| `RESULT: NOT FIXED` and the reason is `thinking is switched ON` | Remove `enable_thinking` from `chat_template_kwargs` in your config. |
| Step 1 says `PASS` but `verify` says `NOT FIXED` | The config does not point at the training template, or `axolotl preprocess` ran before the change. Fix the config and preprocess again. |
| `verify` checks an old prepared folder | axolotl writes one folder per config. `verify` checks the newest and lists the rest with timestamps; re-run `axolotl preprocess` after every config change. |
| `a reply is trained, but not its first token after the block` | The mask boundary is off by a token. Usually `eot_tokens` or a custom `train_on_inputs` setting. |
| `the empty block is trained on; it should be masked` | `train_on_inputs` is on, or `eot_tokens` lists more than `<turn|>`. `== 1` also warns about this from the config alone. |

---

## Code map

`tools/gemma4_thought_fix.py` — a single file, standard library plus the optional packages above.
**Author: Karki (Discord).** The fixes this repository applied are confined to the last row.

| Function / constant | Role |
|---|---|
| `EMPTY`, `THOUGHT_START`, `MODEL_TURN`, `THINK` | The token strings: `<|channel>thought\n<channel|>`, `<|turn>model\n`, `<|think|>`. |
| `AXOLOTL_ROLES`, `ROLE_MAP`, `role_of()`, `is_reply()` | axolotl's default role mapping, overridden by a dataset entry's `roles:`. |
| `read_jsonl()`, `dump_like()` | Split on `\n` only (not `str.splitlines`, which splits inside a JSON string at U+2028), keep the BOM and each line's own ending, and re-serialise a changed line in its original JSON style. |
| `format_errors()`, `shape()` | Format and role validation. |
| `check_sample()` | The part-2 rule: find a block in reply text, remove it, report what it cannot fix. |
| `DATA_ISSUES` | The six findings, each with fixability, review reason and explanation. |
| `effective_template()`, `pick_dataset()`, `template_from_config()` | Reproduce axolotl's own decision about which template trains this file. |
| `renderer()`, `survives()`, `survival_check()` | Render through the real Jinja environment and inspect each reply on its own. |
| `THOUGHT_IF`, `EVERY`, `MARK`, `patch_template()` | The template patch and its three refusals. |
| `cmd_check()`, `cmd_verify()`, `main()` | The two steps. |
| `need_path()`, `verb()` | **Added by this repository** (not Karki's): a missing path is reported as a message rather than a traceback, and a helper so singular counts read correctly. The other fixes are inline: a directory passed as data, an empty data file, a non-mapping YAML config, an invalid YAML config, a `--out` folder that does not exist, and write failures. |

---

## Related reading

- Google, *Prompt formatting — Gemma 4*: <https://ai.google.dev/gemma/docs/core/prompt-formatting-gemma4>
- axolotl, `prompt_strategies/chat_template.py` — the role map and masking rules this tool reproduces.
