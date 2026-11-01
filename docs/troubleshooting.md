<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| **"No APIs enabled"** error | Ensure at least one API slot (1-4) has a valid URL and is enabled |
| **High refusal rate** | Add more jailbreak fixes in Detection → Refusal → Fixes |
| **Slop not being fixed** | Ensure API Slot 5 (Slop Fixer) is configured with URL and model (and key, if the endpoint needs one) |
| **Anti-slop not being fixed** | Ensure API Slot 6 (Anti-Slop Fixer) is configured with URL and model (and key, if the endpoint needs one) |
| **Rate limit errors (429)** | Lower `rate_limit_rpm` for the affected API slot; check rate limit status indicators in the UI |
| **Every request fails with HTTP 400/422** (e.g. `top_k sampling is not supported`) | The endpoint rejects a sampler param the payload sends. Set that slot's **API Compatibility** profile (API tab) to a matching provider, or click **Detect**. See [API Compatibility Profiles](configuration.md#api-compatibility-profiles). |
| **API slot circuit opens frequently** | Check the API endpoint health; circuit opens after 5 consecutive failures and auto-recovers with exponential backoff (60s–600s) |
| **Malformed responses** | Adjust `max_newlines_malformed` and `max_text_length_malformed` in generation settings |
| **Threads stuck/frozen** | Use **Stop & Clear Job** to reset; check rate limits aren't causing excessive waits |
| **Database connection failed** | Verify PostgreSQL is running and credentials are correct; check that the database exists |
| **Valkey connection failed** | Click **Test Valkey** to diagnose; caching will be disabled automatically if unavailable |
| **Open file limit warning** | Reduce number of threads or increase system ulimit (`ulimit -n`) |
| **Queue size warning** | Large queue sizes are normal for high thread counts; reduce threads if memory is constrained |
| **Config validation errors** | Check that all numeric fields contain valid numbers |
| **Budget exceeded unexpectedly** | Review `cost_per_1k_tokens` setting; ensure it matches your API provider's pricing |
| **API connection test fails** | Verify URL format (must include scheme like `https://`), model name, and API key |
| **Resume incompatibility warning** | Critical settings changed since last run; choose to start fresh or proceed with caution |
| **Need to force resume** | Click **🔄 Force Recovery** to bypass config checks and reload state |
| **Want to clear all data** | Use **Stop & Clear Job** to reset progress, or **Clear Database** for PostgreSQL data |
| **Debug logs too verbose** | Uncheck **🐛 Debug Logs** checkbox in **Overview → Tools** to disable verbose logging |
| **Tasks being lost during outages** | Normal behavior — tasks are requeued up to 50 times when an API host is down; if issue persists, check circuit breaker logs |
| **Logit bias not working** | Ensure `logit_bias` is valid JSON (e.g., `{"15": 100}`); check debug logs for JSON parse errors |
| **Reasoning models outputting thinking blocks** | Set `enable_thinking` to `"disable"` in samplers config to add `chat_template_kwargs` to API payload |
| **Characters not getting classes** | Ensure `class_enabled: true` in the character config and that `class` field is populated in character entries |
| **Characters not getting settings** | Ensure `setting_enabled: true` in the character config. Empty per-character `setting` fields fall back to the default indoor environment unless `random_settings.enabled: true` with a populated `settings[]` pool, in which case a setting is sampled from the pool. For multi-character scenes, settings are unified to one shared location automatically (see `random_settings`) |
| **Characters feeling flat or one-note** | Fill both the `persona` (inner personality) and `personality` (outward behavior) fields; `persona` drives the hidden interior life while `personality` drives observed behavior |
| **Characters not getting personalities** | Add the `personality` field to character entries in the config |
| **Characters not getting traits** | Add the `traits` field to character entries in the config (e.g., `"quick-witted, detail-oriented"`) |
| **Multi-character conversations not working** | Set `num_characters` to a value greater than 1 in the character config (max 10) |
| **Anti-slop not triggering** | Verify `anti_slop.phrases` list is populated and API Slot 6 is configured |
| **Too many character cards in editor** | Adjust `max_character_cards` in generation settings (default: 10, max: 100) |
| **Progress bars not animating** | Ensure ttkbootstrap is installed; progress bars use color-coded styles that change with completion percentage |
| **Character traits not appearing in prompts** | Verify the `traits` field is populated in character entries; it's injected alongside personality in the system prompt |
| **Inner persona never shows up** | The persona is intentionally withheld until `persona_breakthrough_turn` (default 2), so on a 1-turn conversation you will only ever see the outward profile. Bump `generation.num_turns` above the breakthrough slot, or set `persona_breakthrough_turn: 1`, to expose the persona sooner |
| **Persona or emotional state missing from the system prompt in the Prompt Viewer** | With the default `depth0` placement they are not in the system prompt: they are at the end of the last USER message of each Answer (and Greeting) request. Question and User Continuation requests never carry them, and the persona appears only from `persona_breakthrough_turn`. Set `persona_breakthrough_placement` or `emotional_states.placement` to `system` to send them in the system prompt, at the cost of prefix-cache reuse |
| **Emotional arc doesn't seem to shift** | Arcs (`a -> b -> c`) need multiple assistant turns to spread across. Outside opening mode, a 1-turn conversation resolves an arc to just its start stage, and a 2-turn one to its start and end stages (opening mode adds the greeting as one more reply); raise `generation.num_turns` so the middle stages have turns to land on. Flat states are constant by design |
| **Stray Chinese / Cyrillic / other non-Latin characters in the output** | Enable `detection.non_latin.enabled` (Detection tab → **Non-Latin Script Check**). The check is a Unicode range allow-list, so it needs no LLM and no phrase list. It regenerates assistant answers and re-rolls generated questions, continuations and opening-mode text; add your own instruction under `detection.non_latin.fixes` if you want a stronger one than the built-in default |
| **A reply with an emoji or a fancy Unicode bullet is being rejected** | The allow-list deliberately keeps a star, a heart and the common punctuation/symbol blocks, but flags the emoji planes. If your run legitimately needs emoji, that is a code change to `detection.LATIN_ALLOWED_PATTERN`, not a config change |
| **Slop fixer breaking quotes** | This is expected behavior — the fixer checks for unbalanced quotes after each rewrite and skips replacements that would break quote structure |
| **Characters with empty names** | Characters with empty or whitespace-only names are automatically skipped with a warning; ensure all character entries have valid names |
| **Character ages out of range** | The editor validates ages to be between 18 and 60; invalid ages are auto-corrected at runtime |
| **Tasks stuck in Failed state** | Use **🔄 Retry Failed** in the Queue tab to re-queue them, or **🗑️ Purge Queue** to discard pending tasks |
| **Queue tab not updating** | Click **🔄 Refresh View** in the Queue tab; the view auto-updates during active processing |
| **Quality scoring not running** | Verify `quality.enabled: true` in config; check that the Quality tab in the dashboard shows scores |
| **Quality LLM scoring failing** | Check `quality.scoring_api` URL/model/key are correct; the system falls back to heuristic estimates and logs a warning |
| **Too many conversations flagged for review** | Increase `quality.min_score_threshold` or improve prompt quality; use the Review tab to inspect flagged items |
| **Quality scores seem too low** | Heuristic-only scoring (no LLM) gives conservative estimates for coherence/naturalness/engagement; enable `use_llm_scoring` for more accurate scores |
| **Review tab empty but scores exist** | The Review tab only shows conversations below `min_score_threshold`; check the Quality tab for all scores |
| **Quality scoring slow** | LLM scoring adds an API call per conversation; disable `use_llm_scoring` for heuristic-only (instant) scoring, or use a fast/cheap model for the scoring API |
| **A fine-tuned Gemma 4 stopped thinking** | This is an axolotl/template problem, not a generator problem — see [Gemma 4 Chat Template Fix for axolotl](#gemma-4-chat-template-fix-for-axolotl-outside-this-project). Gemma's template writes the empty thought block only for the reply being generated, so axolotl trains every reply without it |

## GUI Layout Testing

The `tools/check_gui_layout.py` utility tests the GUI layout on various display sizes to ensure compatibility with smaller screens:

```bash
# Test with default sizes (900×600 through 1916×1126)
python tools/check_gui_layout.py --check --config-editor --screenshots

# Test specific sizes (excluding title bars)
python tools/check_gui_layout.py --check --config-editor --size 760x520 --size 1366x700

# Test with maximum character count (100 profiles)
python tools/check_gui_layout.py --check --config-editor --character-count 100 --screenshots

# Custom output directory
python tools/check_gui_layout.py --check --config-editor --output /path/to/results
```

**Features:**
- Tests primary controls, dashboard space, view selection, and scrolling
- Exercises the configuration editor's page navigation, resizing, editing, adding, removing, and searching
- Validates prompt text box behavior (mouse clicks, keyboard navigation, Ctrl+Tab focus movement)
- Records actual window sizes, fonts, Tk scaling, and dependency versions
- Generates screenshots and a detailed report in `layout-check/` (or custom `--output` path)
- Exits with nonzero code if any assertion fails (when `--check` is used)

**Notes:**
- Window minima are DPI-aware and limited to available screen area
- Use `--scale` to change Tk's pixels-per-point setting for diagnostics
- On Linux, run under `xvfb-run -a` when Xvfb is available for headless testing
- Application configuration and generation state are not modified

This opens the actual GUI with temporary fixture configuration, exercises idle,
single-API, and four-API progress layouts, and resizes from 900×600 through
1024×700, 1366×700, and 1916×1126 before returning to the smallest size. It checks
primary controls, dashboard space, view selection, scrolling, and the editor's
Save/Validate footer. The editor fixture includes 10 character profiles and checks
page navigation, resizing, editing, adding, removing, searching, and reloading.
Prompt checks exercise mouse clicks and text editing without moving the outer
form, as well as keyboard navigation to fields outside the visible area.
It does not start generation or connect to APIs/databases.
Application configuration and generation state are not saved.

The report and optional screenshots are written to `layout-check/`; use
`--output PATH` to choose another directory. `--check` exits with a nonzero code
if an assertion fails. To target particular client sizes (excluding title bars),
repeat `--size`, for example:

```bash
python tools/check_gui_layout.py --check --config-editor --size 760x520 --size 1366x700
```

Clicking a prompt box leaves the outer form in place so the insertion cursor
stays at the clicked text. Keyboard navigation still brings the destination
control into view. In a prompt text box, Ctrl+Tab moves focus forward and
Ctrl+Shift+Tab moves it backward; Tab retains its text-editing behavior.

Character Profiles displays up to three cards per page. Use Previous, Next, or
the page selector to reach the remaining cards; unsaved edits are retained when
changing pages, and Save includes every profile. Adding a character opens its
page. This bounds the height of native drawing surfaces: stacking large numbers
of full cards in one frame can terminate Tk on Linux/X11 with
`BadAlloc` or `BadValue` from `X_CreatePixmap`, even when the visible window is
small. To test the supported maximum collection size:

```bash
python tools/check_gui_layout.py --check --config-editor --character-count 100 --screenshots
```

`--character-count` accepts 1–100 and uses synthetic profiles in temporary files;
it does not load or change your saved characters. The report records the largest
native widget dimensions on each page as well as checking that every profile is
reachable and that off-page edits are included when gathering configuration data.

Window minima are DPI-aware and limited to the available screen area. The report
records actual sizes, fonts, Tk scaling, and dependency versions, so a clamped
size is not mistaken for a successful test at the requested size. `--scale`
changes Tk's pixels-per-point setting for diagnostics; it does not emulate
Windows display scaling. Validate on Windows at the affected display scaling,
including moving the application between monitors, before claiming native DPI
coverage. On Linux, the same check can run under `xvfb-run -a` when Xvfb is
installed.

## Gemma 4 Chat Template Fix for axolotl (outside this project)

> **Out of scope.** `tools/gemma4_thought_fix.py` is **unrelated to this application** — nothing here
> imports, reads or tests it, and no generated output depends on it. It is included only to help people
> who fine-tune **Gemma 4 26B / 31B** with axolotl on a dataset produced by this tool.

**The script was written by Karki, a user on Discord — not by us.** We only applied a few robustness
fixes; the design and the implementation are his. Thank Karki if you find it useful.

Gemma 4 expects every reply to open with a thought channel, which is empty when thinking is off:
`<|channel>thought\n<channel|>`. Gemma's chat template writes that block only for the reply **being
generated**, never for the replies **inside** a conversation, so axolotl trains every reply straight
after `<|turn>model\n` — and the model learns to skip its thought.

The tool fixes both halves of that: it patches a training copy of your chat template to write the empty
block before every reply (outside the reply text, where axolotl masks it), and it strips any block from
the reply text itself, where axolotl would train on it. It then renders every sample through the
patched template, and a second `verify` step reads axolotl's own tokenized `dataset_prepared_path` to
prove the block is present and masked.

```bash
# Step 1 — check and fix the data and the template
python3 tools/gemma4_thought_fix.py data.jsonl --config your_axolotl_config.yml

# Step 2 — the proof, on the tokens axolotl actually produced
axolotl preprocess your_axolotl_config.yml
python3 tools/gemma4_thought_fix.py verify last_run_prepared --tokenizer /path/to/base_model
```

Run both inside your axolotl environment, from the folder you run axolotl from. No GPU is needed.
`RESULT: PASS` (step 1) and `RESULT: FIXED` (step 2) mean the data is ready.

See **[docs/gemma4-chat-template.md](gemma4-chat-template.md)** for what every finding means, the
template-resolution order, the patch and its refusals, all flags, a worked run and a troubleshooting
table.

## Environment Variables

API credentials can be set via environment variables, which take precedence over `config.yml`:

```bash
export API_URL_1="https://api.example.com/v1/chat/completions"
export MODEL_NAME_1="gpt-4"
export API_KEY_1="sk-..."
export API_PROFILE_1="openai"   # optional: API compatibility profile for slot 1
```

`API_URL_n`, `MODEL_NAME_n`, `API_KEY_n` and `API_PROFILE_n` are supported for slots 1–6.

## Debug Logging

The application supports two levels of logging:

- **Normal** — INFO, WARNING, ERROR, and CRITICAL messages are always logged
- **Debug** — Verbose DEBUG messages are hidden by default; enable with the **🐛 Debug Logs** checkbox in **Overview → Tools**

All logs are written to `output/log.txt` regardless of the debug toggle.
