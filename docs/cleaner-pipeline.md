<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 🧹 Cleaner Pipeline (optional)

**Status:** optional, standalone, **not part of the application**
**Lives in:** [`tools/cleaner_pipeline/`](../tools/cleaner_pipeline/)
**Needs:** Python 3.6+ and `bash`. No third-party packages, no config keys, no GUI.

The cleaner pipeline is a nine-step, inspectable post-processor for JSONL conversation
datasets. You point it at a `.jsonl` file, it writes a cleaned copy plus a report for every
record it changed or dropped, and you train on the result.

**The generator does not run it, import it or test it.** It is a separate program that ships
in `tools/` so you do not need a second checkout. A default run of `generate_tk.py` behaves
exactly as it did before this tool was added — nothing in the generation path, the config
schema, the GUI or the dependency list changed.

---

## Contents

- [Read this before you use it](#read-this-before-you-use-it)
- [What it is for](#what-it-is-for)
- [Quick start](#quick-start)
- [Input and output](#input-and-output)
- [The nine steps](#the-nine-steps)
- [What it cleans and what it does not](#what-it-cleans-and-what-it-does-not)
- [Adapting it to your data](#adapting-it-to-your-data)
- [Where it fits in a generator workflow](#where-it-fits-in-a-generator-workflow)
- [Troubleshooting](#troubleshooting)
- [Code map](#code-map)

---

## Read this before you use it

> ### ⚠️ These rules were reverse-engineered from ONE corpus
>
> The nine stages encode the specific defects of one particular dataset. A rule that is
> correct for that corpus can silently destroy yours.
>
> **The single most dangerous default is step 01.** It assumes *every* turn contains a quoted
> line of spoken dialogue. Run unmodified on ordinary prose, documentation, code or most
> assistant data, it will drop your entire dataset.
>
> The intended workflow is to characterise *your* data first (ask an agent or an LLM to
> profile a few hundred records and report what is actually wrong with them), then edit the
> rules in `common.py` and the thresholds in steps 01, 06, 08 and 09 to match. The pipeline
> is a worked example and a starting point, **not a drop-in product**. See
> [Adapting it to your data](#adapting-it-to-your-data).
>
> Always run it against a **copy**. Nothing writes to your source file, but a stage that drops
> records is still a destructive edit of your *dataset*.

---

## What it is for

Generated and scraped chat data collects a predictable class of damage:

- chat-template sentinels left inside the text (`<turn|>`, `<|im_start|>`, `<|channel>`)
- invisible characters — zero-width spaces, BOMs, soft hyphens
- double-encoded text, where `\u2014` appears as six literal characters instead of an em dash
- the same sentinel with spaces injected (`< turn | >`), so a plain find-and-replace misses it
- turns that stop mid-sentence because the source was chunked at a byte offset
- fragments of another writing system spliced into English sentences

The pipeline handles that class of problem in a fixed, repeatable order and writes a report for
every record it changes or drops, so you can audit the decisions afterwards.

It is deliberately boring: standard-library Python, no dependencies, no config files, no
framework. Each stage does one thing, verifies its own output before writing, and can be re-run
on its own.

**It is a defect filter, not a quality filter.** It does not judge coherence, factual accuracy,
safety, toxicity, PII or whether a conversation is interesting. A perfectly formatted record
about nothing passes every stage.

---

## Quick start

The tool resolves paths relative to its own directory, so `cd` into it first.

```sh
cd tools/cleaner_pipeline

# 1. Put the dataset you want to clean here, named input.jsonl
cp ../../output/output.jsonl ./input.jsonl

# 2. Run everything
bash run_all.sh

# 3. Your cleaned dataset
ls build/09_final.jsonl
```

`run_all.sh` is not executable in a fresh checkout — call it through `bash`, or
`chmod +x run_all.sh` once.

**Run a single stage** (each reads a fixed input path, so the previous stage's output must
exist):

```sh
python3 05_normalise_whitespace.py
```

**Resume partway through** — useful after you have edited one script:

```sh
bash run_all.sh --from 6
```

**Point at a dataset somewhere else** — the source is resolved from `$SOURCE`, then
`./input.jsonl`, then `../input.jsonl`:

```sh
SOURCE=/data/corpus.jsonl bash run_all.sh
```

The pipeline stops at the first stage that fails. A non-zero exit code means a stage's own
self-check caught a problem — do not ignore it and do not continue past it.

### What you get

| File | What it is |
|---|---|
| **`build/09_final.jsonl`** | **The cleaned dataset. Train on this one.** |
| `build/01_quotes.jsonl` | After the quote guard, nothing edited yet |
| `build/05_whitespace.jsonl` | Text fully cleaned, no records dropped yet |
| `build/06_foreign_dropped.jsonl` | Foreign-script records removed, truncation still present |
| `build/08_repaired.jsonl` | Truncation repaired, unbalanced quotes still present |
| `build/quote_guard_report.txt` | Every record dropped by step 01, and why |
| `build/foreign_script_report.txt` | Every record dropped by step 06, with the offending word |
| `build/truncation_report.txt` | Every repair and drop by step 08, with the removed text |
| `build/unbalanced_quote_report.txt` | Every record dropped by step 09, and why |

`build/` is created inside `tools/cleaner_pipeline/` and is not committed — see
[`.gitignore`](../.gitignore), which ignores `tools/cleaner_pipeline/build/`.

**Read the reports before you trust the output.** They are the record of what the pipeline
decided about *your* data and the fastest way to notice that a rule is wrong for it.

---

## Input and output

**Input: JSONL, one JSON object per line, ShareGPT style.**

```json
{"id": "abc123", "conversations": [
  {"from": "human", "value": "..."},
  {"from": "gpt",   "value": "..."}
]}
```

This is the same shape the generator writes by default, so `output/output.jsonl` (collaborative
mode) or one `output/output_api_slot_*.jsonl` (duplication mode) can be used directly.

**Schema support is uneven — know which stage you are running:**

| Stage | Schema handling |
|---|---|
| 01 (quote guard) | Accepts ShareGPT (`conversations`), OpenAI chat (`messages`) and flat `{"human": …, "gpt": …}` records. Falls back to scanning every string in an unrecognised structure. |
| 02–09 | **Assume ShareGPT.** They read and write `record["conversations"][i]["value"]` directly. |

If your data is in OpenAI `messages` format, convert it first:

```python
rec["conversations"] = [{"from": t["role"], "value": t["content"]}
                        for t in rec.pop("messages")]
```

**Output:** `build/09_final.jsonl`, same schema as the input, one record per line.

---

## The nine steps

The ordering is the design: several stages would produce wrong answers in a different sequence.

```
input.jsonl
   │
   ├─ 01  quote guard ............ drops records with unpaired dialogue quotes
   ├─ 02  strip special tokens ... removes chat-template sentinels
   ├─ 03  strip control chars ... removes invisible characters
   ├─ 04  strip escapes/markup ... removes literal \uXXXX, HTML entities, tags
   ├─ 05  normalise whitespace ... collapses stray spaces
   ├─ 06  drop foreign script ... drops records with non-English intrusions
   ├─ 07  verify ................. independent check of 02–05
   ├─ 08  repair truncation ...... trims mid-sentence cut-offs
   └─ 09  drop unbalanced quotes  drops records with odd quote counts
          │
          └─> build/09_final.jsonl
```

| Step | Does | Can drop records | Runs first because |
|---|---|---|---|
| 01 quote guard | Curly quotes checked with a positional walk, straight quotes for parity, plus a minimum-dialogue test | ✅ | every later step rewrites text, so a quote imbalance could no longer be attributed to the source |
| 02 special tokens | Removes 41 chat-template sentinels, tolerating injected whitespace and case; never welds words together | ❌ | removal is what creates the stray whitespace step 05 tidies |
| 03 control chars | Removes C0 controls (except newline), soft hyphen, zero-width and directional marks, BOM, U+FFFD | ❌ | — |
| 04 escapes/markup | Removes literal `\uXXXX` / `\xNN`, common HTML entities, a fixed tag list | ❌ | — |
| 05 whitespace | Collapses runs of spaces, space-before-punctuation, trailing/leading space, 3+ newlines | ❌ | it tidies what 02–04 left behind |
| 06 foreign script | Drops records containing a word proven not to be English (dictionary lookup, not a character-range test) | ✅ | record-dropping, so it runs before 07 verifies the survivors |
| 07 verify | Re-derives every expectation from the source rather than trusting the filters | ❌ (exits non-zero) | it verifies the token-cleaned set before 08 repairs anything |
| 08 repair truncation | Trims a turn cut mid-clause back to the last complete sentence; only ever deletes a suffix | ✅ | runs last because it is the only stage that *repairs* |
| 09 unbalanced quotes | Parity check on double quotes after step 08 | ✅ | proves no later stage introduced an imbalance |

The full rationale for each stage — including why each one drops instead of repairing, why
apostrophes are never counted as quotes, why step 08 rejects mid-word fragments, and why there are
two separate quote stages — is in the tool's own README:
[`tools/cleaner_pipeline/README.md`](../tools/cleaner_pipeline/README.md).

---

## What it cleans and what it does not

**Cleaned as it ships:** chat-template sentinels (including spaced and upper-case variants, and the
whole `<|channel>thought<channel|>` block as a unit), invisible and control characters, literal
`\uXXXX` / `\xNN` and the common HTML entities, a specific set of markup tags, stray whitespace,
foreign-script intrusions (record dropped), mid-clause truncation (repaired), unpaired dialogue quotes
(record dropped), and word welding from token removal.

**Deliberately left alone** — this is the list to read before you run it:

| Thing | Cleaned? |
|---|---|
| Markdown emphasis (`*asterisks*`, `**bold**`, `_italic_`) | ❌ untouched — no rule touches `*` or `_` |
| Em dash `—`, en dash `–`, ellipsis `…` | ❌ untouched (deliberate: replacing them is a content edit) |
| Markdown links, code fences, headings, blockquotes, tables | ❌ untouched |
| `&hellip;`, `&mdash;` and other unlisted HTML entities | ❌ untouched |
| `<a>`, `<code>`, `<table>` and other unlisted tags | ❌ untouched |
| Curly vs straight quote normalisation | ❌ untouched |
| Non-breaking space (U+00A0) | ❌ untouched |
| Emoji | ⚠️ **record dropped**, not cleaned |
| Duplicates / near-duplicates | ❌ not handled |
| Boilerplate / template stripping | ❌ not handled |
| PII / toxicity / safety filtering | ❌ not handled |
| Non-English languages | ⚠️ **records dropped** |
| Datasets where step 04 removes anything | ⚠️ **pipeline halts at step 07** (known bug) |

Two of these are worth calling out because they surprise people:

- **Emoji and non-Latin names are dropped by step 06**, not cleaned. Emoji are not in the
  dictionary, so any record containing one is removed. If your dataset has emoji, or is multilingual,
  step 06 is wrong for you as written.
- **Step 04 can weld words together.** `alpha<br>beta` becomes `alphabeta`. Step 02 guards against
  this; step 04 does not. If your data has markup adjacent to text, fix step 04 before running it.

---

## Adapting it to your data

**Use an agent or an LLM for this.** Nine rules reverse-engineered from one corpus will not
describe another corpus's defects, and characterising a new corpus is exactly the tedious
pattern-finding work an LLM is good at. The intended workflow, in short:

1. **Profile your data.** Ask an agent to read a few hundred records and report the schema, whether
   every turn contains quoted dialogue, which special tokens and invisible characters appear, how often
   turns end on complete sentences, whether non-English script or emoji appears, and whether markdown or
   HTML appears.
2. **Compare that profile against the assumptions** in the tool's README and decide explicitly which
   stages apply. The big ones: every turn contains quoted dialogue (step 01), the corpus is English
   (steps 01, 06, 08), no emoji (step 06), and exactly 8 turns per record (step 07).
3. **Edit the rules in `common.py`, not the stages.** `SPECIAL_TOKENS`, `CONTROL_CHARS`,
   `WHITESPACE_RULES`, `MIN_QUOTES` / `MIN_QUOTES_ROLES` all live there, and both the filters and
   the verifier import them — so a token step 02 removes can never be "verified" as absent by step 07
   while actually remaining. Keep that property.
4. **Dry-run on ~200 records and read all four reports** before scaling up.
5. **Do not skip step 07.** It exists to catch exactly the mistake of changing a filter and forgetting
   what it guaranteed.

---

## Where it fits in a generator workflow

```
input/*.txt ──> generate_tk.py ──> output/*.jsonl ──> tools/cleaner_pipeline ──> train
                (generate)      (one row per          (optional, on a copy)
                                 conversation)
```

The generator already applies its own post-processing to each turn (`text_utils.py`: reasoning
removal, quote repair and normalisation, markdown stripping, em-dash removal, the always-on repeated-word
collapse — see [Features](features.md#text-post-processing)) and its own detection layer
(`detection.py`). The cleaner pipeline is **not a replacement for either** and does not know about them.
It is a second, independent pass over a finished file, useful when:

- you are mixing rows from several runs or several models and want one uniform cleanup pass
- your rows came from somewhere else entirely and need the sentinel/invisible-character sweep
- you want an auditable, per-record report of exactly what was changed, which a per-turn cleanup
  inside the generator does not produce

It is **not** useful when a rule contradicts what you deliberately configured the generator to emit. If you
have `remove_asterisks` off because you want `*action*` markup in your dataset, do not run a stage that
strips it — the pipeline does not strip markdown today, but keep the principle in mind when you adapt one.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| A stage dropped every record | Almost certainly step 01 (no quoted dialogue in your data) or step 06 (emoji, non-Latin text, or a missing dictionary) | Read the report, then set `--min-quotes 0` or rewrite step 06's detection |
| `Permission denied` running `run_all.sh` | The script is not executable in the checkout | `bash run_all.sh`, or `chmod +x run_all.sh` once |
| Step 06 prints `WARNING: system dictionary unavailable` | `/usr/share/dict/american-english` is missing, so it falls back to a 96-word allowlist | `apt install wamerican` — the fallback is much weaker |
| Pipeline stops at step 06 with `lost 8-turn human/gpt shape` | Step 07/06 hard-code an expectation of 8 turns per record, from the source corpus | Relax the check in `06_drop_foreign_script.py` / `07_verify.py` for your shape |
| Pipeline stops at step 07 with `every change is an intended removal` | Known bug: step 07's `replay()` mirrors steps 02, 03 and 05 but not 04, so it cannot account for anything step 04 removed | Add step 04's substitutions to `replay()` in `07_verify.py`; the output itself is fine |
| `ERROR: no source dataset found` | No `input.jsonl`, no `../input.jsonl`, no `$SOURCE` | Name the file `input.jsonl`, or pass `SOURCE=/path/to/file.jsonl` |
| `KeyError` in steps 02–09 | Your rows are not ShareGPT shape | Convert to `conversations`/`from`/`value` first; only step 01 is schema-tolerant |

---

## Code map

Everything lives in [`tools/cleaner_pipeline/`](../tools/cleaner_pipeline/):

| File | What it is |
|---|---|
| `run_all.sh` | Runs the nine stages in order, stops at the first failure, supports `--from N` and `$SOURCE` |
| `common.py` | **The shared rules** — `SPECIAL_TOKENS`, `CONTROL_CHARS`, `WHITESPACE_RULES`, the quote tests, `resolve_source()`, `sub_guarded()`. Edit rules here, never inline in a stage |
| `01_quote_guard.py` … `09_drop_unbalanced_quotes.py` | The nine stages, each independently runnable |
| `README.md` | The tool's own full reference: per-stage rationale, the complete "what this will not clean" list, the adaptation loop, design rules and FAQ |

**Not part of the application:** nothing in `generate_tk.py`, `generation.py`, `config_loader.py` or any other
module imports this package, no config key controls it, no test in `tests/` runs it, and it is not in
`requirements.txt`. It is a script collection you run by hand, after a run, on a copy of the output.
