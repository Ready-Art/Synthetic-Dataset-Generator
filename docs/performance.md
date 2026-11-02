<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 📊 Performance Tips

- **Increase threads per API** for higher throughput (watch rate limits — the rate limit status indicators show real-time usage)
- **Enable Valkey caching** to avoid redundant API calls for identical prompts
- **Use PostgreSQL** for large datasets — it's more efficient than appending to JSONL
- **Disable unused detection** features to reduce API calls (e.g., `no_user_impersonation: true`)
- **Lower `max_attempts`** if you prefer faster generation over quality
- **Use collaborative mode** with multiple APIs for maximum throughput
- **Use duplication mode** when you need diverse responses from different models for the same prompts
- **Set a budget limit** to prevent unexpected API costs during long runs
- **Test API connections** in the config editor before starting a generation run
- **Test Valkey connectivity** from the main UI to ensure caching is working
- **Use the Live Prompt Preview** to verify your prompt templates are working correctly before committing to a full run
- **Pause and adjust** rate limits mid-run — configuration is reloaded when you resume
- **Monitor circuit breaker** — if an API slot's circuit opens frequently, the endpoint may be experiencing issues
- **Use Force Recovery** if you need to resume with intentionally changed settings, but be aware of potential incompatibilities
- **Configure anti-slop** — Set up API Slot 6 and populate `anti_slop.phrases` for secondary quality filtering

> **⚠️ Read this first:** the built-in slop/anti-slop path is the *expensive* option — every detected phrase
> costs an extra full LLM round trip through the fixers, and it is **off by default since v12.7.6**
> (`detection.slop.enabled` / `detection.anti_slop.enabled` ship `false`). Moving anti-slop to
> [AntiSlop-vLLM](https://github.com/Ready-Art/antislop-vllm-fork/tree/dev) — the **`dev` branch, not
> `main`** — removes the fixer round trips entirely, because it bans the token during generation instead of
> rewriting the text afterwards. See [Slop & Anti-Slop](slop-and-antislop.md).

- **Use multi-character mode** — Set `num_characters` > 1 for richer, multi-party dialogues
- **Use emotional states** — Enable `emotional_states` to add tonal variety to conversations
- **Use emotional arcs** — Replace flat states with progressions (`calm -> wary -> tense`) so the emotional register shifts across the conversation instead of being one static mood; pair with `generation.num_turns` ≥ the number of stages so every stage has a turn
- **Lean on the persona/personality split** — Keep `persona` (the private self, shown only after `persona_breakthrough_turn`) distinct from `personality` (the outward mask, always shown) so early turns read as surface behavior and later turns reveal the interior
- **Add character traits** — Use the `traits` field in character entries to add more depth to character personas
- **Enable slop → anti-slop fallback** — Set `slop_to_anti_slop_fallback: true` to use the Anti-Slop Fixer as a final attempt when the Slop Fixer fails
- **Toggle conversation history for fixers** — Set `generation.use_conversation_history_for_fixers: false` to disable passing conversation context to slop/anti-slop fixer LLMs (reduces token usage but may affect coherence)
- **Adjust malformed response thresholds** — Tune `max_newlines_malformed` and `max_text_length_malformed` to catch poorly formatted LLM output without rejecting valid responses
- **Use the `enable_thinking` sampler option** — Set to `"disable"` for reasoning models that output unwanted thinking tags
- **Use the Queue tab** — Monitor task progress in real-time; use **Purge Queue** to abandon bad batches without stopping the entire job, or **Retry Failed** to recover from transient issues
- **Export queue state** — Use **📤 Export Queue** to get a CSV snapshot of task status for external analysis
- **Enable quality scoring** — Set `quality.enabled: true` to get per-conversation quality metrics; use heuristic-only mode (default) for zero overhead, or enable LLM scoring for deeper analysis
- **Tune the quality threshold** — Start with `min_score_threshold: 50` and adjust based on the distribution of scores you see in the Quality tab; too low flags nothing, too high flags everything
- **Use a dedicated cheap model for scoring** — Point `quality.scoring_api` at a fast, inexpensive model to minimize the cost impact of LLM-based scoring
- **Export flagged conversations** — Use **📤 Export Flagged → JSONL** in the Review tab to get a clean file of low-quality outputs for manual inspection or re-processing
- **Dismiss reviewed items** — After manually handling flagged conversations, use **✅ Dismiss All** to clear the review queue and keep the tab focused on new items
- **Test GUI layouts** — Use `tools/check_gui_layout.py` to validate the UI on smaller display sizes before deployment
