<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# JSONL Dataset Cleanup Pipeline

> **Optional tool, not part of the generator.** This directory is vendored into the
> ReadyArt Synthetic Dataset Generator as `tools/cleaner_pipeline/` and is documented
> for end users in [`docs/cleaner-pipeline.md`](../../docs/cleaner-pipeline.md). Nothing
> in the application imports, reads or runs it, and no test in `tests/` invokes it — the
> generator produces `output/*.jsonl`, and this pipeline is one option for cleaning a
> copy of that file afterwards. It is a separate, standalone, standard-library-only
> program that happens to live here so it ships with the release instead of needing its
> own checkout. Its own rules were reverse-engineered from one corpus's defects, so read
> the warning below before you point it at your data.

A nine-step, inspectable pipeline for cleaning conversational training data stored as JSONL.

Every step is a small, separate Python script. Each one reads the previous step's output and writes a new file into `build/`. Nothing is overwritten and nothing is hidden: you can open any intermediate file, diff two of them, or re-run a single stage on its own.

> ## ⚠️ READ THIS FIRST
>
> ### **THIS PIPELINE WAS BUILT FOR ONE SPECIFIC DATASET AND HAS BEEN ADAPTED FOR GENERAL PUBLIC RELEASE.**
>
> **The rules in these scripts are not universal truths about "clean data". They are the specific defects that one particular corpus had, encoded as fixes. A rule that is correct for that corpus can silently destroy yours.**
>
> ### **BEFORE YOU RUN THIS ON YOUR OWN DATA, YOU SHOULD USE AN AI AGENT OR LLM TO ANALYSE YOUR DATASET AND MODIFY THESE SCRIPTS TO SUIT IT.**
>
> Concretely: give an agent a sample of your JSONL, ask it to characterise what is actually wrong with your data, then ask it to rewrite the rules in `common.py` and the thresholds in steps 01, 06, 08 and 09 to match what it found. The scripts are a worked example and a starting point, not a drop-in product.
>
> The single most dangerous default is **step 01**, which assumes *every* turn contains a quoted line of spoken dialogue. Run unmodified on ordinary prose, it will drop your entire dataset. See [What this will not clean](#what-this-will-not-clean) and [Adapting it to your data](#adapting-it-to-your-data).

---

## Contents

- [What this is](#what-this-is)
- [Quick start](#quick-start)
- [Requirements](#requirements)
- [Input format](#input-format)
- [The process, step by step](#the-process-step-by-step)
- [What this will clean](#what-this-will-clean)
- [What this will not clean](#what-this-will-not-clean)
- [Adapting it to your data](#adapting-it-to-your-data)
- [Reading the reports](#reading-the-reports)
- [Design rules](#design-rules)
- [FAQ](#faq)

---

## What this is

If you have ever tried to fine-tune a model on scraped or generated chat data, you have met the usual mess:

- chat-template sentinels like `<turn|>` or `<|im_start|>` left inside the text
- zero-width spaces, BOMs and soft hyphens that are invisible in an editor
- double-encoded text, where `\u2014` appears as six literal characters instead of an em dash
- the same token with spaces injected into it (`< turn | >`), so a naive find-and-replace misses it
- turns that stop mid-sentence because the source was chunked at a byte offset
- fragments of other scripts spliced into English sentences

This pipeline handles that class of problem in a fixed, repeatable order, and writes a report for every record it changes or drops so you can audit the decisions afterwards.

It is deliberately boring: standard-library Python, no dependencies, no config files, no framework. Each stage does one thing and verifies its own work before writing its output.

### The one-paragraph summary

Nine scripts run in sequence. Steps 02–05 edit text in place and never delete a record. Steps 01, 06, 08 and 09 can drop whole records. Step 08 is the only stage that *repairs* rather than filters, and it can only ever delete a suffix. Step 07 is an independent verifier that re-derives every expectation from scratch.

---

## Quick start

```bash
# 1. Put your dataset here, named input.jsonl
cp /path/to/your/data.jsonl ./input.jsonl

# 2. Run everything
./run_all.sh

# 3. Your cleaned dataset
ls build/09_final.jsonl
```

That is the whole interface. Everything else is optional.

**Run a single stage:**

```bash
python3 05_normalise_whitespace.py
```

**Resume partway through** (useful after you have edited one script):

```bash
./run_all.sh --from 6
```

**Point at a dataset somewhere else:**

```bash
SOURCE=/data/corpus.jsonl ./run_all.sh
```

The pipeline stops at the first stage that fails. A non-zero exit code means a stage's own self-check caught a problem — do not ignore it, and do not continue past it.

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

Read the reports before you trust the output. They are the record of what the pipeline decided about *your* data, and they are the fastest way to notice that a rule is wrong for it.

---

## Requirements

- **Python 3.6 or newer.** No third-party packages. Every step is standard library only.
- **A POSIX shell** (`bash`) for `run_all.sh`.
- **Optional:** `/usr/share/dict/american-english` for step 06. If it is missing, step 06 falls back to a short built-in allowlist of English loanwords and prints a warning — the fallback is much weaker, so install the dictionary if you can (`apt install wamerican`).

```bash
python3 --version    # 3.6+
```

No virtualenv, no `pip install`, no `requirements.txt`.

---

## Input format

The pipeline expects **JSONL** — one JSON object per line. The format it was built around is ShareGPT style:

```json
{"id": "abc123", "conversations": [
  {"from": "human", "value": "..."},
  {"from": "gpt",   "value": "..."}
]}
```

**How the file is found.** Every stage resolves the source the same way, in this order:

1. an explicit path (`--input` on steps 01 and 08)
2. the `$SOURCE` environment variable
3. `./input.jsonl`, then `../input.jsonl`
4. `./unclean.jsonl`, then `../unclean.jsonl` — a legacy name, still accepted but it prints a warning

Rename your file to `input.jsonl`. Nothing in the pipeline writes to that name, so it stays pristine and re-runnable.

**Schema support is uneven — know which stage you are running.** This matters:

| Stage | Schema handling |
|---|---|
| 01 (quote guard) | Accepts ShareGPT (`conversations`), OpenAI chat (`messages`) and flat `{"human": …, "gpt": …}` records. Falls back to scanning every string in an unrecognised structure. |
| 02–09 | **Assume ShareGPT.** They read and write `record["conversations"][i]["value"]` directly. |

If your data is in OpenAI `messages` format, convert it to `conversations`/`from`/`value` first. The conversion is about six lines:

```python
rec["conversations"] = [{"from": t["role"], "value": t["content"]}
                        for t in rec.pop("messages")]
```

Role labels are normalised to `human` / `gpt` / `system` (`user`→`human`, `assistant`/`model`/`bot`→`gpt`).

---

## The process, step by step

Here is what happens, in order, and *why* the order is what it is. The ordering is the design: several stages would produce wrong answers if they ran in a different sequence.

### The short version

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

### Step 01 — Quote guard: drop records with unpaired dialogue quotes

**What it does.** Checks each turn's dialogue quotation marks and drops any record where they do not pair up. Three tests:

- **Curly quotes** (`"` `"`, U+201C/U+201D) are checked with a left-to-right *positional walk*, not a count. Counting is not enough: `"a" "b"` and `"a "b"` both contain two of each, and only the second is broken. The walk reports `missing_open` (a close with nothing open) and `missing_close` (an open that never closes).
- **Straight quotes** (`"`) are checked for parity — an odd number left over means one is unpaired. Inch and feet marks (`6"`, `5'9"`) are stripped *first*, so a measurement is never mistaken for a quotation mark.
- **Minimum dialogue.** Each checked turn must contain at least two quotation marks (one open/close pair). `"It's great," she said.` passes; `It's great, she said.` does not.

Apostrophes are **never** counted. `don't`, `she'd`, `his brother's` are ordinary text, and counting them would make almost everything look balanced by accident.

**Why it runs first.** Every later step rewrites text. Once characters have been deleted and truncation has been repaired, a quote imbalance can no longer be attributed to the source — a repair that closes an open quotation can *mask* a genuine defect, and a quote the repair added can look like a *new* one. Measuring on the pristine input keeps the report a measurement of your corpus rather than a measurement of the pipeline.

> **⚠️ This is the rule most likely to be wrong for your data.** The minimum-dialogue test assumes every turn contains quoted speech. That is true of the corpus this was written for. It is **not** true of prose, documentation, code, or most assistant data. On such a dataset, step 01 will drop everything. Set `--min-quotes 0` to disable that one test, or delete the stage.

**Why drop instead of repair.** The defect sits mid-string. There is no way to know which of the quotes is the stray one without inventing a reading of the source: deleting the first turns dialogue into unmarked prose, deleting the last leaves it unclosed. Either choice silently rewrites the scene. The whole record is dropped, and survivors are copied through byte-for-byte.

### Step 02 — Strip chat-template special tokens

**What it does.** Removes chat-template sentinels that leaked into the text: `<turn|>`, `<|channel>`, `<channel|>`, and a fixed list of 41 in total, covering ChatML/Qwen, Llama 2/3, Mistral, Phi, Gemma and generic role templates.

Two details make this harder than a find-and-replace:

- **Matching tolerates injected whitespace and case.** `<turn|>`, `<turn |>`, `<turn | >`, `< turn |  >` and `<TURN | >` are all caught by the same pass. A literal `str.replace("<turn|>")` misses every spaced form and leaves the token in your corpus.
- **Removal never welds words together.** A plain `re.sub("", text)` concatenates whatever sat on either side of the token. This corpus has a live case: `'Thud.<turn|>…The next morning'` would become `'Thud.The next morning'`, two sentences welded into one. Wherever a removal would join two non-whitespace characters, a single space is inserted instead.

The `<|channel>thought<channel|>` block is removed **as a whole unit, first**, because `thought` is otherwise an ordinary English word appearing hundreds of times in normal prose. Only the one occurrence inside the template block is removed.

**Why here.** It must run before whitespace normalisation, because removal is what creates the stray whitespace that step 05 tidies.

### Step 03 — Strip control and invisible characters

**What it does.** Removes characters that are never legitimate prose:

- C0 control characters other than newline (tab, vertical tab, form feed, NUL, …)
- soft hyphen (U+00AD)
- zero-width space, joiners and directional marks (U+200B–U+200F)
- line and paragraph separators (U+2028, U+2029)
- word joiner (U+2060)
- BOM (U+FEFF)
- replacement character (U+FFFD)

Tabs become a single space; everything else is deleted. **Newline is preserved** — it carries the paragraph structure of the text.

### Step 04 — Strip escaped-unicode text and HTML artefacts

**What it does.** Removes double-encoded leftovers:

- literal `\uXXXX` escape sequences (six characters of text that were meant to be one character)
- literal `\xNN` hex escapes
- HTML entities: `&nbsp;` `&amp;` `&quot;` `&apos;` `&lt;` `&gt;` `&#39;` and numeric `&#NNNN;`
- stray markup tags: `<br>` `<p>` `</i>` `<b>` `<em>` `<strong>` `<span>` `<div>` `<html>` `<body>`

**Note a real limitation:** unlike step 02, this stage does **not** guard against word welding. `alpha<br>beta` becomes `alphabeta`. It is safe on the corpus it was written for (which had no such tags adjacent to text) but it is not safe in general. See [What this will not clean](#what-this-will-not-clean).

### Step 05 — Normalise whitespace

**What it does.** Tidies the whitespace that step 02's removals left behind. Five rules:

| Rule | Pattern | Becomes |
|---|---|---|
| run of spaces | two or more spaces | one space |
| space before punctuation | `word .` | `word.` |
| trailing spaces | spaces at end of line | removed |
| leading spaces | indented wrapped lines | de-indented |
| excess blank lines | three or more newlines | two newlines |

**Newlines are never collapsed**, and the `leading spaces` rule carries a negative lookbehind so it fires only on genuine wrapped-line indents. Without that guard it would also match the single space after a blank line — the deliberate paragraph indent used before dialogue — and silently destroy the paragraph structure of thousands of turns.

Because the rules are safe, this stage normalises **every** turn, not only the ones steps 02–04 touched. It verifies before writing that no turn lost its paragraph break.

### Step 06 — Drop records containing foreign script

**What it does.** Drops any record containing fragments of another writing system spliced into English text. In the source corpus: Thai, Bengali, CJK/Japanese, Hangul, Cyrillic, Arabic, Devanagari, Kannada and Telugu — plus accented Latin lookalikes that are not English words (`först`, `précision`, `gözler`).

**Detection is positive, not negative** — a word is flagged only when it is *proven* not to be English:

1. every character is ASCII → English
2. the character is ordinary English typography (curly quotes, en/em dash, ellipsis, bullet, non-breaking hyphen, nbsp) → English
3. otherwise the **whole word, accents included**, is in the system English dictionary → English (`café` passes)
4. anything else → **foreign**

Rule 3 is what makes it safe. A character-range test alone cannot separate `précision` (corruption of "precision") from `café` (a real English loanword) — both are Latin script with a diacritic. Only a dictionary lookup distinguishes them.

**Why drop instead of delete.** The intrusions sit *inside* English words and phrases, so deleting the characters does not leave the sentence intact. `"her blue眼睛 dart up"` → `"her blue dart up"` is a rewrite, not a repair. Deleting would silently corrupt the text; keeping would teach the model to emit foreign script.

> **⚠️ Emoji and non-Latin names will trip this.** Emoji are not in the dictionary, so a record containing one is dropped. If your data is multilingual or contains emoji, this stage is wrong for you as written.

### Step 07 — Verify

**What it does.** An independent check of steps 02–05, written so that a bug in a filter cannot hide itself. It re-derives every expectation from the source rather than trusting the filters. It checks:

- no BOM, every line valid JSON
- record count, ids and order preserved against step 06's survivors
- turn count and role sequence preserved per record
- no empty turns
- zero residual special tokens — matched with the *same* whitespace-tolerant, case-insensitive patterns step 02 uses, so `<turn | >` cannot slip through
- zero control or invisible characters
- zero escaped-unicode or HTML artefacts
- **every difference from the source is an intended removal** — each changed turn is re-stripped from the original and must match the result, so an unintended edit cannot pass silently
- **no removal welded two sentences together** — the source has zero `lowercase.Capital` junctions, so any in the output would be a bug

Non-zero exit on any failure. `run_all.sh` stops at the first bad stage.

> **Note:** step 07 hard-codes an expectation of 8 turns per record (4 human/gpt pairs), because the source corpus had that shape. See [Adapting it to your data](#adapting-it-to-your-data).

### Step 08 — Repair truncated turns

**What it does.** Finds turns that stop mid-clause and trims them back to something complete. The source text was chunked at byte offsets with no sentence-boundary snapping, producing turns like:

```
"...the warm water filling the basin makes her skin"      cut mid-phrase
"...she lets out a loud gutt"                            cut mid-word
'...garlic on your skin," I mutter as'                   cut mid-clause
```

Left alone, these teach the model that stopping mid-clause is acceptable. But deleting every affected record would cost far more data than the damage warrants — truncation touched 20% of records, and almost all of them lost only a trailing clause. So this stage **repairs** instead of filtering.

Two rules, because the two roles break differently:

| Role | How it breaks | Repair |
|---|---|---|
| `gpt` | cut mid-sentence or mid-word | trim back to the last complete sentence |
| `human` | the trailing `," I <verb> as …` clause is severed | cut the severed clause, close the sentence |

**A repair only ever deletes a suffix.** The only characters it can add are terminal punctuation and, when a cut lands inside an open quotation, one closing quote at the cut point. No word is invented, so nothing that was not in the source can enter the corpus. The stage verifies that every surviving turn is a trimmed prefix of its source.

**Mid-word fragments are rejected.** Vocabulary is learned from the complete turns, and a cut whose final token is not a whole word of that vocabulary is refused. Without this, `… the hotel rest` (from "restaurant") and `… loud gutt` (from "guttural") get a period bolted on and enter the training set as mangled words.

**Cuts are ranked by kind, not length.** A longer cut is not a better cut if it leaves a severed clause behind, so the stage picks the best *kind* of cut first — complete sentence, then complete attribution, then subclause start, then last closing quote — and only then the longest option.

**When a record is dropped instead:**

| Reason | Why |
|---|---|
| the truncation is in turn 0 | the design assumes a complete opening prompt; there is no earlier turn to carry the scene |
| nothing complete survives the trim | there is no usable text left |
| repairs would remove more than 25% of the record | what remains no longer resembles the source |

### Step 09 — Drop records with unbalanced quotes

**What it does.** Drops any record containing a turn whose double-quote characters (`"`, `"`, `"`) number odd, so the dialogue markers never pair up.

Detection is **parity**, not pattern matching: parity is a property of the defect itself, so spacing cannot hide it and no per-case exception list is needed. Apostrophes are not counted.

**Why drop and not repair.** Unlike truncation, this defect sits mid-string: there is no way to know which of three quotes is the stray one without inventing a reading of the source. Deleting the first turns dialogue into unmarked prose; deleting the last leaves it unclosed. Either way the scene is silently rewritten, which breaks step 08's guarantee that a repair only deletes a suffix. Since the broken turn is usually the model's *input prompt*, shipping it teaches the model to mis-close quotes.

**Not redundant with step 01.** Step 01 is the full positional check on the *pristine* source; step 09 is a parity check on the text *after* step 08. Step 01 proves the defect was never in the source; step 09 proves no later stage introduced it.

---

## What this will clean

Everything below is removed by the pipeline as it ships. These are the defects it was actually written for.

### Chat-template sentinels

`<turn|>`, `<|channel>`, `<channel|>`, and the full template set, including spaced variants (`< turn | >`) and upper-case variants (`<TURN | >`). The whole `<|channel>thought<channel|>` block is removed as a unit.

### Invisible and control characters

Zero-width space, zero-width joiner/non-joiner, BOM, soft hyphen, directional marks, line/paragraph separators, word joiner, replacement character, and C0 controls other than newline. Tabs become a space.

### Double-encoded text

Literal `\uXXXX` and `\xNN` sequences, and the common HTML entities (`&nbsp;` `&amp;` `&quot;` `&apos;` `&lt;` `&gt;` and numeric `&#NN;` / `&#xNN;`).

### A specific set of markup tags

`<br>` `<p>` `</p>` `<i>` `<b>` `<em>` `<strong>` `<span>` `<div>` `<html>` `<body>`, case-insensitively, with or without a self-closing slash.

### Stray whitespace

Runs of two or more spaces, a space before `, . ; : ! ?`, trailing spaces at end of line, indentation on wrapped lines, and runs of three or more newlines. Newlines themselves are never collapsed.

### Foreign-script intrusions

Fragments of Thai, Bengali, CJK/Japanese, Hangul, Cyrillic, Arabic, Devanagari, Kannada, Telugu, and accented Latin non-words spliced into English sentences. **The whole record is dropped** — the intrusion cannot be deleted without rewriting the sentence.

### Mid-clause truncation

Turns that stop mid-sentence or mid-word are trimmed back to the last complete sentence (assistant turns) or had their severed trailing clause removed (user turns). Mid-word fragments are refused rather than patched with a period.

### Unpaired dialogue quotes

Records where a turn's double quotes number odd, so the dialogue markers never pair up.

### Word welding from token removal

Step 02 inserts a space where a removal would otherwise join two words (`Thud.<turn|>The` → `Thud. The`, not `Thud.The`). Step 07 verifies no welding occurred.

---

## What this will **not** clean

This is the section to read carefully. Everything here is *left exactly as it is* by the pipeline as it ships. Some of it you may want to keep; some of it you will need to add a stage for.

### ❗ Markdown emphasis is untouched

`*asterisks*`, `**bold**`, `***bold italic***`, `_underscores_`, `~~strikethrough~~` — all preserved byte-for-byte. There is no markdown parser anywhere in the pipeline and no rule that touches `*` or `_`. If your corpus contains roleplay-style `*action*` markup and you want it removed or converted, **you must add a stage**; nothing here will do it.

Verified: `A *asterisk* and **bold** here.` passes through unchanged.

### ❗ Em dashes and en dashes are untouched

`—` (U+2014), `–` (U+2013) and `…` (U+2026) are all **preserved**. They are explicitly listed as ordinary English typography in step 06's `ALLOWED_PUNCT`, so they are never treated as foreign script, and no other stage touches them.

This is a deliberate choice, not an oversight. Em dashes are a common AI-writing tell, so if your goal is to *de-emphasise* them (for style or for de-contaminating a dataset), **nothing here will do that** — you need your own normalisation stage. Note that replacing them is a *content edit*, not a cleanup: it changes the author's punctuation, so do it knowingly.

Verified: `an em dash — plus a hyphen - and ellipsis … ok.` passes through unchanged.

### ❗ Markdown syntax and links are untouched

`[text](url)`, `` `code` ``, ` ```fenced blocks``` `, `# headings`, `> blockquotes`, `- list items`, `| tables |` — none of it is parsed or altered.

### ❗ Most HTML entities and most markup tags are untouched

Only the short list in step 04 is removed. `&hellip;`, `&mdash;`, `&rsquo;` and other named entities survive. `<a>`, `<code>`, `<table>`, `<ul>`, `<img>`, `<iframe>` and anything not in step 04's fixed list survive untouched.

### ❗ Word welding from step 04 is possible

Step 02 guards against welding; step 04 does **not**. `alpha<br>beta` becomes `alphabeta` and `one<b>two</b>three` becomes `onetwothree`. This did not occur in the source corpus, so the stage was left unguarded. **If your data contains markup adjacent to text, fix step 04 before running it** — either add the same guard step 02 uses, or accept the damage knowingly.

### ❗ Emoji are treated as foreign script

Emoji are not in the English dictionary, so a record containing one is **dropped** by step 06, not cleaned. If your dataset contains emoji, step 06 is wrong for you as written.

### ❗ Non-Latin languages are treated as corruption

If your dataset is genuinely multilingual, or contains transliterated names, honorifics, or quoted foreign phrases, step 06 will drop those records. The check is built on the assumption that *non-English text in an English corpus is corruption*.

### ❗ Curly quotes are preserved, not normalised

`"` and `"` are never converted to `"`, and `"` is never converted to `'`. If you want typographic quotes flattened to ASCII, **you must add a stage**.

### ❗ Non-breaking spaces and other odd spaces are preserved

U+00A0 (non-breaking space) is explicitly allowed by step 06 and is not touched by any whitespace rule (which only match ordinary spaces and tabs). Tabs become spaces; nbsp does not.

### ❗ Other invisible characters outside the fixed list are preserved

Step 03 removes a specific list. Other unusual invisible characters — for example U+180E, U+2061–U+2064, U+206A–U+206F — are not in it and pass through. If your data has unusual invisible characters, extend `CONTROL_CHARS` in `common.py`.

### ❗ Duplicates, near-duplicates and boilerplate are not handled

There is no deduplication, no near-duplicate detection (MinHash, SimHash), and no boilerplate or template stripping. Two identical records both survive. If you need this, **add a stage** — none of the nine does it.

### ❗ Content quality is not judged

The pipeline is a *defect* filter, not a *quality* filter. It does not assess coherence, factual accuracy, safety, toxicity, PII, NSFW content, or whether a conversation is interesting. A perfectly formatted record about nothing passes every stage.

### ❗ Schema is not repaired

Records missing an `id`, missing `conversations`, containing non-string `value`s, or in an unrecognised shape are not fixed. Steps 02–09 assume the ShareGPT shape and will raise `KeyError` rather than migrate. Step 01 is the only schema-tolerant stage.

### ❗ Languages other than English are not supported

The entire design assumption is an English corpus. Step 01's quote rules, step 06's dictionary lookup and step 08's vocabulary checks are all English-specific. There is no language detection, so a French or German corpus will be mangled rather than rejected.

### ❗ Line-level JSON damage is not repaired

A malformed JSON line causes the stage to raise, not to skip or repair. Step 01 collects and reports bad lines; later stages do not.

### ❗ Known bug: step 07 aborts if step 04 removes anything

Step 07 re-applies the allowed transformations to the source and requires the result to match byte-for-byte. Its `replay()` function mirrors steps 02, 03 and 05 — but **it omits step 04**. So if step 04 actually removes an escaped-unicode sequence, an HTML entity or a markup tag, step 07 reports `FAIL every change is an intended removal` and `run_all.sh` stops.

This never triggered on the source corpus, because that corpus contained nothing for step 04 to remove. On any dataset that does, the pipeline halts at step 07 even though the output is correct.

**Fix before relying on this pipeline.** Add step 04's substitutions to `replay()` in `07_verify.py`:

```python
# step 04 - must mirror 04_strip_escaped_and_markup.py exactly
for pattern in (ESC_U, ESC_X, HTML_ENTITY, MARKUP_TAG):
    text = pattern.sub("", text)
```

Put these patterns in `common.py` and import them in both places, so the verifier and the filter can never disagree.

### Summary table

| Thing | Cleaned? |
|---|---|
| Chat-template sentinels (incl. spaced/case variants) | ✅ yes |
| Zero-width chars, BOM, soft hyphen, C0 controls | ✅ yes |
| Literal `\uXXXX` / `\xNN`, common HTML entities | ✅ yes |
| `<br>` `<p>` `<i>` `<b>` `<em>` `<strong>` `<span>` `<div>` | ✅ yes |
| Stray runs of spaces, space-before-punctuation, wrapped-line indent | ✅ yes |
| Foreign-script intrusions | ✅ record dropped |
| Mid-clause truncation | ✅ repaired |
| Odd double-quote counts | ✅ record dropped |
| **Markdown emphasis (`*asterisks*`, `**bold**`, `_italic_`)** | ❌ **untouched** |
| **Em dash `—`, en dash `–`, ellipsis `…`** | ❌ **untouched** |
| **Markdown links, code fences, headings, blockquotes, tables** | ❌ **untouched** |
| **`&hellip;` `&mdash;` and other unlisted HTML entities** | ❌ **untouched** |
| **`<a>` `<code>` `<table>` and other unlisted tags** | ❌ **untouched** |
| **Curly vs straight quote normalisation** | ❌ **untouched** |
| **Non-breaking space (U+00A0)** | ❌ **untouched** |
| **Emoji** | ⚠️ **record dropped**, not cleaned |
| **Duplicates / near-duplicates** | ❌ **not handled** |
| **Boilerplate / template stripping** | ❌ **not handled** |
| **PII / toxicity / safety filtering** | ❌ **not handled** |
| **Non-English languages** | ⚠️ **records dropped** |
| **Datasets where step 04 removes anything** | ⚠️ **pipeline halts at step 07** (known bug) |

---

## Adapting it to your data

**Use an agent or LLM for this. It is the intended workflow, not a shortcut.** You have a pipeline of nine rules that were reverse-engineered from one corpus's specific defects; characterising a *different* corpus's defects is exactly the kind of broad, tedious, pattern-finding work an LLM is good at, and exactly the kind of thing that is easy to get subtly wrong by hand.

A concrete loop that works:

**1. Profile your data first.** Ask an agent to read a few hundred records and report, with counts and examples:

- the schema, and whether every record matches it
- whether every turn contains quoted dialogue (this decides whether step 01 is usable at all)
- which special tokens appear, and in what spacing variants
- which invisible or control characters appear
- whether turns end on complete sentences, and how often
- whether non-English script or emoji appears
- whether markdown, emoji, or HTML appear — and whether you *want* them

**2. Compare that profile against the assumptions above.** Each of these is a decision you must make explicitly:

| Assumption | Where | If it is false for your data |
|---|---|---|
| every turn contains quoted dialogue | step 01 | set `--min-quotes 0` or drop the stage |
| the corpus is English | steps 01, 06, 08 | step 06 will drop your data; rewrite it |
| no emoji | step 06 | records will be dropped; add an emoji exemption |
| no markdown to preserve | steps 02–05 | nothing to do — it is already preserved |
| no markup adjacent to text | step 04 | add step 02's welding guard to step 04 |
| exactly 8 turns per record | step 07 | the verify stage will fail; relax the check |
| truncation means "lost a suffix" | step 08 | mid-string damage is not repairable; drop instead |
| `/usr/share/dict/american-english` exists | step 06 | the fallback allowlist is far weaker |

**3. Modify the rules, not the stages.** The shared rules live in `common.py`, deliberately in one place:

- `SPECIAL_TOKENS` — the token list step 02 removes
- `CONTROL_CHARS` — the character set step 03 removes
- `WHITESPACE_RULES` — the five rules step 05 applies
- `MIN_QUOTES`, `MIN_QUOTES_ROLES` — step 01's dialogue requirement
- `ALLOWED_PUNCT`, `FALLBACK_ALLOW` — step 06's typography and loanword exemptions, in `06_drop_foreign_script.py`
- `MAX_RECORD_LOSS`, `MIN_HUMAN_WORDS`, `MIN_GPT_WORDS` — step 08's thresholds

Because both the filters and the verifier import from `common.py`, a token step 02 can remove can never be "verified" as absent by step 07 while actually remaining. Keep that property: put new rules in `common.py`, not inline in a stage.

**4. Dry-run on a sample, and read the reports.** Copy 200 records into `input.jsonl`, run the pipeline, and read all four reports. A rule that is wrong for your data shows up as a large drop count or as obviously mangled text in `truncation_report.txt`. Scale up only after the sample looks right.

**5. Keep the structural guarantees.** They are the reason this pipeline is worth using instead of a pile of regexes:

- text-editing stages (02–05) never change the record count
- record-dropping stages (01, 06, 09) copy survivors through verbatim and verify that the dropped set is *exactly* the flagged set
- repairs (08) only ever delete a suffix and must leave a trimmed prefix
- every stage verifies its own output before writing
- every stage is re-runnable in isolation

**6. Do not skip the verification stage.** If you change a rule, run `07_verify.py`. It exists to catch exactly the mistake of changing a filter and forgetting what it guaranteed.

---

## Reading the reports

Each record-dropping stage writes a plain-text report listing every decision and why. **Read them.** They are the fastest way to find out that a rule is wrong for your data.

```bash
head -60 build/quote_guard_report.txt
head -60 build/foreign_script_report.txt
head -60 build/truncation_report.txt
head -60 build/unbalanced_quote_report.txt
```

In `truncation_report.txt`, every entry shows the text that was removed and the tail that was kept, so you can see at a glance whether repairs are sensible or are lopping off good prose.

A healthy run on a dataset the pipeline suits looks like this:

```
source                       1,474 records  11,792 turns
after 01 quote guard         1,474 records  11,792 turns
after 05 whitespace         1,474 records  11,792 turns
after 06 foreign dropped     1,416 records  11,328 turns
after 08 repaired            1,410 records  11,280 turns
final                        1,410 records  11,280 turns
```

Steps that drop **nothing** are not necessarily broken — on the source corpus, steps 01 and 09 both dropped zero records. They remain in the pipeline because the defects are one generator glitch away and the checks are cheap.

---

## Design rules

The choices that make this pipeline trustworthy, and that you should preserve when you adapt it.

**Whole-token removal, never character stripping.** Tokens are removed as complete units. The original broken cleaner stripped `<` and `>`, which turned `<turn|>` into `turn|` and `<|channel>` into `|channel` — leaving the payload behind and gluing it to the sentence.

**Removals never weld words together.** A plain `sub("")` can concatenate the characters on either side of a removed token. `sub_guarded()` inserts a space instead, and step 07 checks the result directly.

**`thought` is never stripped as a word.** It appears hundreds of times as ordinary English. Only the occurrence inside a template block is removed, by matching the block as a unit before any single-token pass runs.

**Pure filters, isolated normalisation.** Steps 02–04 only delete. All whitespace tidying lives in step 05, so each filter can be verified in isolation.

**Content filters never delete records.** Steps 02–05 preserve record count, ids, order and turn structure, and check them after every stage.

**Record-dropping stages preserve survivors byte for byte.** Steps 01, 06 and 09 only delete whole lines, and verify that the dropped set is exactly the flagged set.

**One implementation per rule.** The quote tests live in `common.py` and step 01 imports them, so there is exactly one implementation and no second caller can drift out of sync.

**Repairs only delete.** Step 08 can add terminal punctuation and at most one closing quote. No word is invented.

---

## FAQ

**Can I run just one stage?**
Yes. `python3 05_normalise_whitespace.py`. Each stage reads a fixed input path, so it needs the previous stage's output to exist.

**Can I re-run the pipeline?**
Yes. Every stage overwrites only its own file in `build/` and reads `input.jsonl`, which nothing writes to.

**Why does step 02 read `build/01_quotes.jsonl` instead of the source?**
Because re-reading the source would resurrect exactly the records step 01 dropped. It falls back to the source only when run standalone.

**A stage dropped all my records. What happened?**
Almost certainly step 01 (your data has no quoted dialogue) or step 06 (your data has emoji, non-Latin text, or a missing dictionary). Check the reports, then adjust `--min-quotes` or rewrite step 06's detection.

**My data is OpenAI `messages` format. Will it work?**
Only step 01 understands it. Convert to `conversations`/`from`/`value` before steps 02–09.

**Is this safe to run on a dataset I cannot replace?**
Keep the original untouched and run the pipeline against a copy. Nothing writes to the source file, but a stage that drops records is still a destructive edit of your *dataset*, not of your file.

**Why are there two quote stages?**
Step 01 is a full positional check on the pristine source; step 09 is a parity check after repairs. Step 01 proves the defect was never in the source; step 09 proves no later stage introduced it. A repair that closes a quote would only be visible to the latter.

**Why does step 07 expect 8 turns?**
Because the source corpus had 4 human/gpt pairs. It is a dataset-specific check, not a universal invariant — relax it for your data.

**Can I use this for non-English data?**
Not as written. Steps 01, 06 and 08 are all built on English-specific assumptions. Step 06 will drop records rather than pass them through.

**The pipeline stopped at step 07 saying "every change is an intended removal". Why?**
Known bug. Step 07's `replay()` re-applies steps 02, 03 and 05 but not step 04, so it cannot account for anything step 04 removed. See [Known bug](#-known-bug-step-07-aborts-if-step-04-removes-anything). The output is fine; the verifier is wrong.

---

## License

AGPLv3, like the rest of this repository — the SPDX header at the top of every file in this directory says so, and `tools/add_license_header.py` stamps and checks it. This differs from the upstream standalone copy of the pipeline, which shipped without a licence; vendoring it here brings it under the generator's licence.

Check the licence of any dataset you run through the pipeline before you publish the result.
