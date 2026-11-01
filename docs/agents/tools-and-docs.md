<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Tools and user docs

Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) for the shared rules (setup, tests, boundaries, state, slots, thread safety).

**Read this when:** you are touching `tools/*.py`, `docs/gemma4-chat-template.md`, `docs/licensing.md` or `docs/scene-cast.md`.

**Related:** [`docs/agents/task-index.md`](task-index.md)

## Contents

- `tools/add_license_header.py`
- `tools/check_gui_layout.py`
- `tools/check_dataset_hub.py`
- `tools/gemma4_thought_fix.py`
- `docs/licensing.md`
- `docs/gemma4-chat-template.md`
- `docs/scene-cast.md`
- `Tools (CLI flags and exit codes)`
- Tasks that touch these files

---

### `tools/add_license_header.py`
**Purpose:** Owns the AGPLv3 SPDX header on every text/code file. Idempotent — it rewrites only its own managed block, so a stale header is replaced rather than stacked
**Key:** `HEADER_LINES` (the one definition of the block), `MARKER` / `END_MARKER` (what identifies an existing block), `SKIP_PATHS`, `SKIP_DIRECTORIES`, `HASH_EXTENSIONS` / `MD_EXTENSIONS`, `split_preamble()` (keeps a shebang and a PEP 263 line first), `strip_existing_header()`
**Usage:** `python -B tools/add_license_header.py` (write), `--check` (exit 1 if any file lacks it — the pre-finish check), `--verbose` (list every file examined)
**Edit for:**
- Change the header wording → `HEADER_LINES` only, then rerun the tool; never hand-edit a file's block
- Add a file type → `HASH_EXTENSIONS` (commented with `#`) or `MD_EXTENSIONS` (an HTML comment, so a `#` does not render as a heading). `.json`/`.jsonl` are deliberately excluded: neither format has comments
- Skip a new third-party or generated file → `SKIP_PATHS` or `SKIP_DIRECTORIES`
**Never:** stamp the header on `gemma4_thought_fix.py` / `tools/gemma4_thought_fix.py` (written by **Karki**, Discord — this project only applied robustness fixes, so stamping it would be a misattribution), or on `LICENSE` (verbatim FSF text) / `LICENSE-AGPLv3` / `LICENSE-MIT`. All are in `SKIP_PATHS`; keep them there
**Why the block is safe:** it is a comment, so it is invisible to Python, YAML and Markdown, and it is placed after a shebang and a PEP 263 line so a module docstring stays the first statement and `__doc__` still works. Guarded by the full test run plus `python -m compileall`

---

### `tools/check_gui_layout.py` [3]
**Purpose:** GUI layout testing tool for checking UI at various window sizes without running generation
**Key:** `run()`, `check_gui_layout()`, `widget_info()`, `check_navigation()`, `check_character_profiles()`, `check_opening_mode_round_trip()`, `check_prompt_interactions()`
**Edit for:**
- Add new layout check → New `check_*()` function + call in `run()`
- Modify test sizes → `DEFAULT_SIZES` list or `--size` CLI arg
- Add screenshot capture → `--screenshots` flag + `take_screenshot()` calls
- Test character profiles → `--character-count` flag + `check_character_profiles()`
- Test opening mode → `check_opening_mode_round_trip()` validates greeting/prompt persistence
- Test prompt interactions → `check_prompt_interactions()` validates caret placement, scrolling, keyboard nav
**CLI:** `--check` (exit nonzero on fail), `--config-editor`, `--screenshots`, `--scale`, `--allow-below-minimum`

---

### `tools/check_dataset_hub.py` [41]
**Purpose:** Opt-in live Hub reader check — prints schema/provenance and verifies live-row binding, same-job reuse, snapshot round trip, and literal rendering; never prints dataset cells, calls a model, or mutates app config
**Usage:** `python -B tools/check_dataset_hub.py --repository owner/dataset --split train --column text [--config X] [--revision Y] [--rows N]` (install `requirements-datasets.txt` first; unit tests never invoke this)

---

### `tools/gemma4_thought_fix.py` [45]
**Purpose:** **OUTSIDE THIS PROJECT — nothing here imports, reads or tests it.** **Written by Karki (a user on Discord), not by this repository** — we only applied robustness fixes (see below); credit the idea and the implementation to Karki. Standalone helper for people fine-tuning Gemma 4 26B/31B with axolotl: fixes the empty thought block (`<|channel>thought\n<channel|>`) that Gemma's chat template writes only for the reply being generated, never for the replies inside a conversation — so axolotl trains every reply straight after `<|turn>model\n` and teaches the model to skip thinking. Two steps: `cmd_check()` checks the data format, removes any thought block from reply TEXT (the template writes it outside the text, where axolotl masks it), finds the template the config trains with (entry `chat_template` → top-level → `tokenizer_default`), renders every sample through it, and if replies lack the block writes a patched `chat_template.train.jinja` beside the data plus the two config lines to use it; `cmd_verify()` then reads axolotl's own tokenized `dataset_prepared_path` and asserts every reply starts with the MASKED empty block, the reply after it is trained, and nothing switches thinking on. Key: `EMPTY`, `MODEL_TURN`, `THINK`, `check_sample()`, `DATA_ISSUES`, `effective_template()`, `pick_dataset()`, `template_from_config()`, `renderer()`, `survives()`, `survival_check()`, `THOUGHT_IF`, `patch_template()`, `MARK`
**Usage:** `python3 tools/gemma4_thought_fix.py data.jsonl --config axolotl.yml` then `axolotl preprocess axolotl.yml && python3 tools/gemma4_thought_fix.py verify last_run_prepared --tokenizer /path/to/base_model` (run both inside the axolotl environment; no GPU). Flags: `--template`, `--out`, `--yes`. Exit 0 = PASS/FIXED, 1 = NOT READY/NOT FIXED/NOT CHECKED, 2 = no usable conversation
**Edit for:** The template patch must stay limited to the one known Gemma 4 thought-rendering shape (`THOUGHT_IF`) and refuse to guess — never widen it into a generic rewrite. Every file write must stay non-destructive (a new path, never the input) and every failure path must end in a readable message, not a traceback. **The docstring's AUTHORSHIP block and the `--help` epilog credit Karki — keep them; this repository did not write the script.** Not covered by `tests/` and not part of the test loop; see `docs/gemma4-chat-template.md`

---

### `docs/gemma4-chat-template.md` [46]
**Purpose:** **OUTSIDE THIS PROJECT** — user guide to `tools/gemma4_thought_fix.py`, **a script written by Karki (a user on Discord)**, for people training Gemma 4 with axolotl: why the empty thought block matters, why the fix is split between the template (writes the block, masked) and the data (carries none), the five numbered sections of step 1 and what every finding means, the template-resolution order, the patch and its three refusals, the four `verify` checks, exit codes, all flags, a worked run, the "replace the template before you upload" warning, a troubleshooting table and a code map. Says explicitly and first that it is unrelated to this generator, and credits Karki for the script (this repository only applied robustness fixes)
**Edit for:** Update when the tool's output, findings, flags, patch shape or `verify` checks change; keep the out-of-scope notice AND the Karki credit at the top

---

### `docs/scene-cast.md` [43]
**Purpose:** User/design guide for the v12.0 Scene Cast feature: what it is for, the one idea ("the generator owns the cast, the model only narrates it"), the five-state table, the deliberate exit-cancelled / arrival-forced asymmetry and WHY it is asymmetric, what the code-only continuity check catches and what it deliberately does not (`"Where is Melody?"`, `Cole nodded at Melody`, possessives), cache safety (no per-reply counter), how to turn it on, a per-character/`min_present` feasibility table, reading `cast_events` to measure whether the model writes its exits, worked examples, a troubleshooting table, and a code map
**Edit for:** Update when the cast state machine, the note text, `min_present` feasibility, the metadata keys or the config keys change

---

### `tools/check_dataset_hub.py`
**Purpose:** Opt-in live Hugging Face Hub reader verification (schema/provenance printout + binding invariants); no cells, no model, no config mutation
**Usage:** `python -B tools/check_dataset_hub.py --repository owner/dataset --split train --column text --rows 2`
**Flags:** `--config`, `--revision`, `--rows N` (≥1), repeatable distinct `--column`; exits 1 on any invariant failure

### `tools/check_gui_layout.py`
**Purpose:** Isolated GUI layout testing without running generation or saving production state
**Usage:** `python tools/check_gui_layout.py --check --config-editor --screenshots`
**Flags:**
- `--check`: Exit nonzero if any layout assertion fails
- `--config-editor`: Include config editor tab and character profile tests
- `--screenshots`: Capture PNG screenshots of each resize scenario
- `--size WIDTHxHEIGHT`: Custom window size (repeatable, default: 900x600, 1024x700, 1366x700, 1916x1126)
- `--scale FLOAT`: Tk pixels/point scale (diagnostic, not Windows DPI emulation)
- `--character-count N`: Number of fixture character profiles (1-100, default: 10)
- `--allow-below-minimum`: Override production window minima for diagnostics
**Output:** `./layout-check/layout_measurements.json` with widget visibility, overlaps, scroll region, and check results

### `tools/gemma4_thought_fix.py`
**Purpose:** **OUT OF SCOPE — unrelated to this project. Written by Karki (a user on Discord), not by this repository.** Standalone axolotl helper for Gemma 4 26B/31B no-thinking fine-tunes: makes the chat template write the empty thought block before every reply (outside the reply text, so axolotl masks it), strips any block from reply text, and then proves it on axolotl's own tokenized output. Not imported, read or tested by this application; see `docs/gemma4-chat-template.md`
**Usage:** `python3 tools/gemma4_thought_fix.py data.jsonl --config axolotl.yml` (step 1), then `axolotl preprocess axolotl.yml && python3 tools/gemma4_thought_fix.py verify last_run_prepared --tokenizer /path/to/base_model` (step 2). Run both inside the axolotl environment; no GPU
**Flags:** `--config PATH`, `--template PATH` (overrides `--config`), `--out PATH`, `--yes`; subcommand `verify <prepared> --tokenizer <model>`
**Exit codes:** 0 = PASS / FIXED, 1 = NOT READY / NOT FIXED / NOT CHECKED, 2 = no usable conversation

---

## Tasks that touch these files

| Task | Files to edit |
|------|---------------|
| Add or change the license header on every file | `tools/add_license_header.py` (`HEADER_LINES` is the only place the wording lives — edit it there and rerun, never hand-edit a file's block; `--check` is the pre-finish gate), `docs/licensing.md`, `LICENSE-AGPLv3`, `docs/license-and-credits.md` (the License page; `README.md` only links to it). **Never** add `gemma4_thought_fix.py`, `LICENSE`, `LICENSE-AGPLv3` or `LICENSE-MIT` to the stamped set — they are in `SKIP_PATHS` on purpose |
| Run dataset tests | `for t in tests/test_dataset_sources.py tests/test_dataset_settings.py tests/test_dataset_bindings.py tests/test_dataset_templates.py tests/test_dataset_tasks.py tests/test_dataset_backends.py tests/test_dataset_column_menu.py tests/test_datasets_panel.py tests/test_dataset_pipeline.py tests/test_dataset_compatibility.py tests/test_questions_dataset_ui.py; do python -B "$t"; done` (live Hub check: `tools/check_dataset_hub.py`, opt-in only) |
| Run GUI layout tests | `tools/check_gui_layout.py --check --config-editor --screenshots` |
| Fix Gemma 4 thinking in an axolotl fine-tune (**out of scope, not this app's code**) | `tools/gemma4_thought_fix.py` (**script by Karki, Discord** — keep the docstring AUTHORSHIP block and `--help` epilog crediting him; keep the patch limited to `THOUGHT_IF`'s one template shape, keep every write non-destructive, keep every failure a readable message), `docs/gemma4-chat-template.md` |
