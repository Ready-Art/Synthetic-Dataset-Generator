<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| **Slop/Anti-Slop not running at all** | Expected since v12.7.6 — `detection.slop.enabled` and `detection.anti_slop.enabled` ship `false`. Turn them on deliberately, or better, use https://github.com/Ready-Art/antislop-vllm-fork/tree/dev (dev branch) — see [Slop & Anti-Slop](slop-and-antislop.md) |
| **"No APIs enabled"** error | Ensure at least one API slot (1-4) has a valid URL and is enabled |
| **Cleaner pipeline dropped every record** | Almost certainly step 01 (your data has no quoted dialogue) or step 06 (emoji, non-Latin text, or a missing `/usr/share/dict/american-english`). Read the report in `build/`, then set `--min-quotes 0` or rewrite step 06 — see [Cleaner Pipeline](cleaner-pipeline.md#troubleshooting) |
| **`tools/cleaner_pipeline/run_all.sh`: Permission denied** | It is not executable in the checkout | `bash run_all.sh` from inside `tools/cleaner_pipeline/`, or `chmod +x` it once |
| **High refusal rate** | Add more jailbreak fixes in Detection → Refusal → Fixes |
| **Slop not being fixed** | Ensure API Slot 5 (Slop Fixer) is configured with URL and model (and key, if the endpoint needs one) |
| **Anti-slop not being fixed** | Ensure API Slot 6 (Anti-Slop Fixer) is configured with URL and model (and key, if the endpoint needs one) |
| **Rate limit errors (429)** | Lower `rate_limit_rpm` for the affected API slot; check rate limit status indicators in the UI |
| **Every request fails with HTTP 400/422** (e.g. `top_k sampling is not supported`) | The endpoint rejects a sampler param the payload sends. Set that slot's **API Compatibility** profile (API tab) to a matching provider, or click **Detect**. See [API Compatibility Profiles](configuration.md#api-compatibility-profiles). |
| **API slot circuit opens frequently** | Check the API endpoint health; circuit opens after 5 consecutive failures and auto-recovers with exponential backoff (60s–600s) |
| **Malformed responses** | Adjust `max_newlines_malformed` and `max_text_length_malformed` in generation settings |
| **Threads stuck/frozen** | Use **Stop & Clear** on the Overview action bar to reset; check rate limits aren't causing excessive waits |
| **Database connection failed** | Verify PostgreSQL is running and credentials are correct; check that the database exists |
| **Valkey connection failed** | Click **Test Valkey** to diagnose; caching will be disabled automatically if unavailable |
| **Open file limit warning** | Reduce number of threads or increase system ulimit (`ulimit -n`) |
| **Queue size warning** | Large queue sizes are normal for high thread counts; reduce threads if memory is constrained |
| **Config validation errors** | Check that all numeric fields contain valid numbers |
| **Budget exceeded unexpectedly** | Review `cost_per_1k_tokens` setting; ensure it matches your API provider's pricing |
| **API connection test fails** | Verify URL format (must include scheme like `https://`), model name, and API key |
| **Resume incompatibility warning** | Critical settings changed since last run; choose to start fresh or proceed with caution |
| **Need to force resume** | Use **Force Recovery** (`cli launch --force-recovery`) to bypass config checks and reload state |
| **Want to clear all data** | Use **Clear dataset** on the Overview action bar to delete the stored conversations (a backup zip is taken first, and it does not stop a run), or **Stop & Clear** to stop the run as well. `cli clear --yes` does the same from a shell |
| **Downloaded dataset is empty or has fewer rows than expected** | **Download dataset** consolidates the per-API files, so a row count lower than the Overview's total means a file is still being written. Stop the run first, then download. If the count is still low, check `output/log.txt` for a `drop_reason` |
| **Download dataset does nothing** | It is a GET that streams an attachment, so a browser that blocks downloads silently drops it. Check the browser's download settings, then retry; the row count is also sent as the `X-Row-Count` header |
| **Download dataset shows a 404 page** | Before v15.0.1 the button navigated the whole tab to the route, so a run with no rows yet replaced the app with a raw JSON error. It now preflights `GET /api/dataset` and says "there is no dataset to download yet" in a toast. If you still see a 404 document, you opened the URL directly: there is nothing on disk yet, so start a run first |
| **Recent issues shows rows but every cell is an em dash** | Fixed in v15.0.1. The engine stored `recent` entries as `(phrase, context, api_slot)` tuples with no timestamp, and the browser read named fields off them. Update; a process started before the fix still renders, because `control._recent_entry()` normalises the old shape on the way out |
| **The Metrics tab's table has data but its graphs are empty** | Fixed in v15.0.1. The table is written by the generation threads, but the ring the graphs draw is only appended to by the vLLM poller, which the web app never started. `Engine.start()` now starts it. If it is still empty, the poller cannot reach any `/metrics` endpoint — check that each `api.apis[].url` points at a vLLM server and that `metrics.enabled` is not off |
| **The Metrics tab's y-axis numbers overlap, or the graph text looks squashed** | Fixed in v15.1.0. The chart drew into a hard-coded 640-wide viewBox with `preserveAspectRatio="none"`, so a narrower panel scaled every glyph horizontally. The viewBox is now measured from the host, so one user unit is one CSS pixel. If you still see it, hard-reload the page — the old `chart.js` is cached |
| **The Live Prompts tab shows "JSON parse unexpected character at line 1 column 1"** | Fixed in v15.1.1. The route function was named `live_prompts`, which shadowed the `live_prompts` module inside its own body, so the route raised `AttributeError` and answered `500` with the plain text `Internal Server Error`. The browser reported that as a JSON parse error because it parsed the body unconditionally. The function is now `live_prompts_route`, and the client checks `Content-Type` before parsing, so a future server error names itself instead. Update and hard-reload |
| **A tab says "the server answered 500 with a non-JSON body"** | That is the new, honest version of the message above: the server failed and the body was not JSON. The text after the colon is the server's own body, which is usually enough to identify the failure. Check `output/log.txt` for the traceback |
| **The Live Prompts tab says the preview is not hooked up** | The engine installs `app_state.live_prompt_preview_hook` when it starts. Restart the web app (`python -m webapp`); the viewer then fills in as requests are sent |
| **Estimated cost stays at `$0.00` while tokens are being counted** | `api.pricing.cost_per_1k_tokens` is `0`, which is the shipped default. Set a real price in the Config tab; the card and the budget stop use the same formula |
| **The queue will not clear, and Stop & Clear says `not_running`** | Stop & Clear needs a live run and Purge pending only touches pending tasks. Use **Clear queue** on the Overview action bar or the Queue tab — it works with no run in progress and never deletes a dataset row |
| **Export flagged JSONL does nothing, or only names a path** | Fixed in v15.1.0. The button POSTed and the route wrote a file on the server, so nothing reached the browser. It now downloads through `GET /api/review/export/download`, which answers an attachment. An empty export is a 404 with a message, not a zero-byte file |
| **Recent issues jumps back to the top while I am reading it** | Fixed in v15.1.0. The Overview re-rendered on every stats tick and threw the panel's scroll offset away. Hard-reload to pick up the new `dom.js` |
| **The Issues-over-time graph is cut off at the bottom, or the Review detail header is a sliver** | Fixed in v15.0.1. Both were flex/grid children that could shrink, so a short column squeezed them and the panel's `overflow: hidden` clipped the result. The graph panel now has a `min-height` and the side column scrolls; the Review header and note no longer shrink. If you see it again, the window is shorter than the graph needs — scroll the right-hand column |
| **Clear dataset refused** | It needs `confirm: true` in the request body, so a stale tab cannot wipe a dataset. Reload the page and confirm again |
| **A cleared dataset cannot be recovered** | **Clear dataset** takes a backup zip first, listed by `GET /api/dataset/backups` (newest first). Unzip it into `output/` to restore. Only `backup: false` skips it |
| **Debug logs too verbose** | Uncheck **Debug logs** in the Overview screen's Diagnostics section to disable verbose logging |
| **Tasks being lost during outages** | Normal behavior — tasks are requeued up to 50 times when an API host is down; if issue persists, check circuit breaker logs |
| **Logit bias not working** | Ensure `logit_bias` is valid JSON (e.g., `{"15": 100}`); check debug logs for JSON parse errors |
| **Reasoning models outputting thinking blocks** | Set `enable_thinking` to `"disable"` in samplers config to add `chat_template_kwargs` to API payload. This applies to the question/answer/continuation/opening-mode requests and is the fallback for the fixers and the judge |
| **Scoring or slop/anti-slop fixing is very slow on a reasoning model** | Before v13.0.4 the Slop Fixer, Anti-Slop Fixer and Quality Judge sent no `chat_template_kwargs`, so the model thought on every rewrite and every score no matter what the Thinking Mode said. Update, then set **Thinking Mode (Slop Fixer)**, **Thinking Mode (Anti-Slop)** and **Judge Thinking Mode** to `"disable"` (or set the main **Thinking Mode** to `"disable"`, which they inherit). See [performance.md](performance.md) |
| **Characters not getting classes** | Ensure `class_enabled: true` in the character config and that `class` field is populated in character entries |
| **Characters not getting settings** | Ensure `setting_enabled: true` in the character config. Empty per-character `setting` fields fall back to the default indoor environment unless `random_settings.enabled: true` with a populated `settings[]` pool, in which case a setting is sampled from the pool. For multi-character scenes, settings are unified to one shared location automatically (see `random_settings`) |
| **Characters feeling flat or one-note** | Fill both the `persona` (inner personality) and `personality` (outward behavior) fields; `persona` drives the hidden interior life while `personality` drives observed behavior |
| **Characters not getting personalities** | Add the `personality` field to character entries in the config |
| **Characters not getting traits** | Add the `traits` field to character entries in the config (e.g., `"quick-witted, detail-oriented"`) |
| **Multi-character conversations not working** | Set `num_characters` to a value greater than 1 in the character config (max 10) |
| **Anti-slop not triggering** | Verify `detection.anti_slop.enabled: true` (it ships `false` since v12.7.6), that `anti_slop.phrases` is populated, and that API Slot 6 is configured — or skip the fixer entirely and use the fork's [dev branch](https://github.com/Ready-Art/antislop-vllm-fork/tree/dev) (see [Slop & Anti-Slop](slop-and-antislop.md)) |
| **Too many character cards in editor** | Adjust `max_character_cards` in generation settings (default: 10, max: 100) |
| **Progress rows not moving** | Each slot has its own row, so a stuck row means that slot is not being fed. Check its rate limit and circuit state |
| **Character traits not appearing in prompts** | Verify the `traits` field is populated in character entries; it's injected alongside personality in the system prompt |
| **Inner persona never shows up** | The persona is intentionally withheld until `persona_breakthrough_turn` (default 2), so on a 1-turn conversation you will only ever see the outward profile. Bump `generation.num_turns` above the breakthrough slot, or set `persona_breakthrough_turn: 1`, to expose the persona sooner |
| **Persona or emotional state missing from the system prompt in the Prompt Viewer** | With the default `depth0` placement they are not in the system prompt: they are at the end of the last USER message of each Answer (and Greeting) request. Question and User Continuation requests never carry them, and the persona appears only from `persona_breakthrough_turn`. Set `persona_breakthrough_placement` or `emotional_states.placement` to `system` to send them in the system prompt, at the cost of prefix-cache reuse |
| **Emotional arc doesn't seem to shift** | Arcs (`a -> b -> c`) need multiple assistant turns to spread across. Outside opening mode, a 1-turn conversation resolves an arc to just its start stage, and a 2-turn one to its start and end stages (opening mode adds the greeting as one more reply); raise `generation.num_turns` so the middle stages have turns to land on. Flat states are constant by design |
| **Stray Chinese / Cyrillic / other non-Latin characters in the output** | Enable `detection.non_latin.enabled` (Detection tab → **Non-Latin Script Check**). The check is a Unicode range allow-list, so it needs no LLM and no phrase list. It regenerates assistant answers and re-rolls generated questions, continuations and opening-mode text; add your own instruction under `detection.non_latin.fixes` if you want a stronger one than the built-in default |
| **A reply with an emoji or a fancy Unicode bullet is being rejected** | The allow-list deliberately keeps a star, a heart and the common punctuation/symbol blocks, but flags the emoji planes. If your run legitimately needs emoji, that is a code change to `detection.LATIN_ALLOWED_PATTERN`, not a config change |
| **Slop fixer breaking quotes** | This is expected behavior — the fixer checks for unbalanced quotes after each rewrite and skips replacements that would break quote structure |
| **Characters with empty names** | Characters with empty or whitespace-only names are automatically skipped with a warning; ensure all character entries have valid names |
| **Character ages out of range** | The editor validates ages to be between 18 and 60; invalid ages are auto-corrected at runtime |
| **Tasks stuck in Failed state** | Use **Retry failed** in the Queue screen to re-queue them, or **Purge pending** to discard pending tasks |
| **Queue screen not updating** | Click **Refresh** in the Queue screen; the view auto-updates during active processing |
| **Quality scoring not running** | Verify `quality.enabled: true` in config; check that the Quality screen shows scores |
| **`Scene continuity violation ... fixes exhausted` on almost every reply** | Before v14.3, a character scheduled to arrive on reply N was still `absent` when reply N's cast note was built (the state only flips after the reply comes back), so `_cast_names_for_slot()` put it in the **blocked** list *and* the **expected** list on the same reply. The note said `Dani ARRIVES in this reply` and the check then flagged the model for writing exactly that, so every arrival reply was regenerated to exhaustion. Upgrade to v14.3+, or set `detection.scene_continuity.enabled: false` to stop the check entirely. If it still happens on v14.3+, check `output/log.txt` for the character named in the finding — a name that is genuinely blocked is a real violation |
| **A conversation vanished from the dataset with `scene_continuity` as its drop reason** | This is v14.3 behaviour and it is deliberate. A scene-continuity violation that survived `detection.scene_continuity.max_rounds` rewrites is the one check that does **not** keep its best-effort text: the reply has a character who has left the scene still speaking or acting, so writing it would put a row in the dataset that argues with itself. A style miss (slop, a repeated opener) is kept; a cast contradiction is dropped. To keep such rows anyway, set `detection.scene_continuity.enabled: false` (the check then never fires) or raise `max_rounds` so the fix rounds get more chances. The drop is recorded in `quality_dropped` and the task is still credited for progress |
| **A character is flagged for acting when someone else did it** | Before v14.3, `find_character_actions()` charged the next action verb in the sentence to a name that was the **object of a preposition** — `She looks at Dani and smiles` flagged Dani. Upgrade to v14.3+ |
| **Every reply is dropped when quality scoring is on** | Before v12.3, enabling `quality.cast_continuity` made `score_conversation()` raise `KeyError: 'cast_continuity'` while weighting the composite; the worker's error handler turned that into a `judge_failed` drop for **every** row, and the log said the judge had failed even though the judge was never called successfully. Upgrade to v12.3+ (see the changelog). If it still happens on v12.3+, check `output/log.txt` for the real `drop_reason` — `judge_failed` means the judge endpoint itself is unreachable, `invalid_json` means its replies are unparsable |
| **Quality LLM scoring failing** | Check `quality.scoring_api` URL/model/key are correct; the system falls back to heuristic estimates and logs a warning |
| **Too many conversations flagged for review** | Increase `quality.min_score_threshold` or improve prompt quality; use the Review screen to inspect flagged items |
| **Quality scores seem too low** | Heuristic-only scoring (no LLM) gives conservative estimates for coherence/naturalness/engagement; enable `use_llm_scoring` for more accurate scores |
| **Review screen empty but scores exist** | The Review screen only shows conversations below `min_score_threshold`; check the Quality screen for all scores |
| **Quality scoring slow** | LLM scoring adds an API call per conversation; disable `use_llm_scoring` for heuristic-only (instant) scoring, or use a fast/cheap model for the scoring API |
| **A fine-tuned Gemma 4 stopped thinking** | This is an axolotl/template problem, not a generator problem — see [Gemma 4 Chat Template Fix for axolotl](#gemma-4-chat-template-fix-for-axolotl-outside-this-project). Gemma's template writes the empty thought block only for the reply being generated, so axolotl trains every reply without it |

## The web front end

| Issue | Solution |
|-------|----------|
| **`python -m webapp` will not start** | It needs `requirements-web.txt` as well as `requirements.txt`. `python-multipart` is a hard dependency: the login and setup routes read the password form with `await request.form()`, and FastAPI raises at request time without it. Install both files, then retry |
| **I lost the first-run setup token** | It is printed to the console **once**, never over HTTP. Stop the process and start it again with no `webapp/state/auth.json` present, and a fresh token is printed. Delete that file to re-open setup deliberately |
| **Locked out after failed logins** | Five failed attempts lock that address for five minutes, in memory. Restarting the process clears the lockout; there is no unlock command, and the lockout is per-IP so a different address is unaffected |
| **The session expires every 12 hours** | Expected: the cookie has a 12-hour TTL. Log in again. The cookie is `HttpOnly`, `SameSite=Lax`, and `Secure` only when the request actually arrived over HTTPS |
| **Login loops forever behind nginx** | The cookie is marked `Secure` from `X-Forwarded-Proto`, so a proxy that does not set it over HTTPS produces a cookie the browser refuses to store. Set the header in nginx, and pass `--trust-proxy` so the header is believed |
| **A non-loopback bind refuses to start** | Deliberate: binding off loopback without `--trust-proxy` is refused, because the forwarded header would be spoofable and every client would share one rate-limit bucket. Add `--trust-proxy` once the proxy really sets the header |
| **A screen needs a scrollbar at 1440x900** | The layout is wrong, not the screen. Every screen must fit 1440x900 and 900x600 with no page scroll, and `tests/test_web_overview.py` asserts it |
| **The page says `reconnecting…` and stops updating** | The stats socket dropped (a server restart, a proxy that buffers the upgrade, a laptop waking). The page falls back to polling `/api/stats` and retries with a backoff, so it recovers on its own; a reload re-primes it from the server |
| **A tab shows a stale list after a run** | Each screen re-fetches on mount and on **Refresh**, so leaving and returning to a screen re-fetches. Nothing is cached in the page: the state lives on the server, because a run outlives the tab |
| **A config save is rejected and nothing is written** | The validators run server-side **before** the write, so a rejected save changes nothing. The error names every bad field at once; fix them all and save again |
| **A config save dropped my comments** | It should not: the writer splices only the changed spans, so an unchanged save is byte-identical. If comments vanish, the save went through `yaml.dump` somewhere else — use the Config screen, not a hand edit, and check `git diff` |
| **An `/api/*` call returns 401 in a script** | Every route except `/login`, `/setup`, `/logout` and `/api/health` needs the session cookie. Log in first and send the cookie; there is no API token |
| **`AttributeError: module 'socketserver' has no attribute 'ThreadingUnixStreamServer'` on Windows** | Fixed in v16.0.1. Windows has no unix domain sockets, so the control channel is a loopback TCP listener there and the class is only defined where the address family exists. Update to v16.0.1 or later; there is nothing to configure |
| **`python -m cli status` says `no_daemon` on Windows while the web app is running** | The web app publishes its loopback port in `output/generator-control.sock`. Check that `--socket` names that same file, and that nothing deleted it while the controller was live |

## GUI Layout Testing

Every screen must fit 1440x900 and 900x600 with no page scroll, and
`tests/test_web_overview.py` asserts it. There is no separate layout tool.

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
- **Debug** — Verbose DEBUG messages are hidden by default; enable with **Debug logs** in the Overview screen's Diagnostics section

All logs are written to `output/log.txt` regardless of the debug toggle.
