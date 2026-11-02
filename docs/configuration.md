<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# ⚙ Configuration

All configuration is managed through `config/config.yml` and can be edited via the built-in Configuration Editor (GUI). The editor provides a tabbed interface with validation and a search bar for quickly finding tabs and sections.

## Dataset Sources and Prompt Variables

The Configuration Editor's **Datasets** tab imports column names from local files,
saved Hugging Face datasets, or Hub repositories and maps them to aliases such as
`{{ds:nvd_f_name}}`. It supports multiple sources, a bounded column table, and a
searchable column dropdown that defaults to **All**. Record values are not shown
in the configuration GUI.

Each existing generation task receives one frozen row per mapped source. Its
values remain available through turns, repairs, and retries on the same queue.
The integration prepares authored inputs while retaining the existing task IDs,
generation/refinement methods, random-setting selection, duplication, and output
metadata schema. Task counts still use the generator's existing controls.

**Dataset attachments are in-memory for the current queue/run.** They are not
saved by the existing recovery mechanism; restarting or starting a new queue
does not preserve the previous row assignments.

**Dataset variables in the questions file (v11.9.0).** A line in `input/questions.txt` is
authored text, so it may carry `{{ds:alias}}` placeholders, and one alias value can be
appended to every line instead of being written into each one:

```yaml
prompts:
  use_questions_file: true
  questions_dataset:
    append_alias: chapter_body   # '' = placeholders only, append nothing
    append_separator: "\n\n"    # real characters; the editor shows it escaped
```

Configure both in the Configuration Editor's **Prompts** tab, under **Questions File Dataset
Variables**. Each task is expanded with its own frozen row, so the placeholder and the appended
value always come from the same dataset row, and the task's ID is untouched. An unmapped alias
is an error rather than literal text, and an empty cell appends nothing. Chunk-mode source `.txt`
files are deliberately **not** rendered — a chunk is cut at a random character offset, so a
placeholder there could be split in half. See
[The `input/` folder](input-folder.md).

CSV/TSV/JSON/JSONL need no additional dependencies. For Hub, saved-HF, and Parquet
sources, install `requirements-datasets.txt`.
See [Dataset sources and task metadata](dataset-bindings.md) for setup,
placeholder behavior, limits, and the upstream comparison tests.

## API Setup

The application supports **6 API slots**:

| Slot | Purpose | Part of Duplication? | Configurable Threads? | Configurable Rate Limit? |
|------|---------|---------------------|----------------------|------------------------|
| 1-4  | Main generation APIs | Yes | Yes | Yes |
| 5    | Slop Fixer LLM | No | Yes | Yes |
| 6    | Anti-Slop Fixer LLM (independent) | No | Yes | Yes |

```yaml
api:
  master_duplication_mode: false
  threads: 10
  pricing:
    cost_per_1k_tokens: 0.002
    budget_limit: 50.0
  apis:
    - url: "https://api.example.com/v1/chat/completions"
      model: "model-name"
      key: "your-api-key"
      enabled: true
      threads: 10
      rate_limit_rpm: 60
      api_profile: openai_compatible   # payload compatibility profile (see below)
    # ... repeat for slots 2-6
```

- **`url`**: OpenAI-compatible chat completions endpoint
- **`model`**: Model identifier for the API
- **`key`**: API key (can also be set via environment variables `API_KEY_1` through `API_KEY_6`)
- **`enabled`**: Whether this slot is active for generation (slots 1-4 only)
- **`threads`**: Number of worker threads dedicated to this API (all slots)
- **`rate_limit_rpm`**: Requests per minute limit for rate limiting (all slots)
- **`api_profile`**: API compatibility profile for this slot (default `openai_compatible`; see below)
- **`custom_allowed_params`**: list of body params to send — only used when `api_profile: custom`

> **Tip:** Environment variables (`API_URL_1`, `MODEL_NAME_1`, `API_KEY_1`, `API_PROFILE_1`, etc.) take precedence over config.yml values.

### API Compatibility Profiles

The request builders emit a permissive, vLLM/OpenAI-style JSON body (`top_k`, `min_p`,
`repetition_penalty`, `chat_template_kwargs`, `logit_bias`, `bad_words`, …). Local servers ignore params they
don't recognise, but several hosted APIs **reject any unknown field with HTTP 400/422** — so a run
that works against vLLM fails on every request against, e.g., Mistral.

Each API slot picks a **compatibility profile** that trims the outgoing body to what that endpoint
accepts. In the config editor's **API** tab each slot has an *API Compatibility* dropdown and a
**Detect** button (guesses the profile from the URL host). Profiles are plain data in
`config/api_profiles.yml` — add or adjust one there, no code change required.

| Profile | Use for |
|---------|---------|
| `openai_compatible` *(default)* | vLLM, TGI, llama.cpp, koboldcpp, most local/self-hosted servers — sends everything, filters nothing |
| `openai` | OpenAI API |
| `mistral` | Mistral La Plateforme |
| `xai_grok` | xAI Grok |
| `gemini_openai` | Google Gemini's OpenAI-compatible endpoint |
| `anthropic_openai` | Anthropic Claude via its OpenAI-compatible endpoint / a proxy |
| `custom` | Pick the exact params to send via `custom_allowed_params` |

Notes:
- `model`, `messages` and `stream` are always sent regardless of profile.
- `repetition_penalty` is remapped to `frequency_penalty` (`value − 1`, clamped to `[-2, 2]`) for
  profiles that support the latter but not the former.
- A slot left on the default profile whose URL host matches a known provider is auto-detected at
  run start, so an existing config pointed at Mistral is handled without editing it.
- The named provider profiles assume an OpenAI-**shaped** endpoint (same URL style, `Bearer` auth,
  `choices[].message.content` response). Native Anthropic / native Gemini APIs are a different
  request/response shape and are **not** covered by profile filtering alone.
- Only `mistral` and `openai_compatible` have been verified against a live endpoint. The `openai`,
  `xai_grok`, `gemini_openai` and `anthropic_openai` allow-lists are built from each provider's API
  docs — they filter exactly as listed, but if a provider actually accepts a param that isn't in
  its list, that param is dropped rather than sent. Adjust the list in `config/api_profiles.yml` if
  you hit this; no code change is needed.

**Params the request builders send** — a profile whose `allow` list omits one of these drops it
before the request: `temperature`, `top_p`, `top_k`, `min_p`, `repetition_penalty`, `max_tokens`,
`logit_bias`, `chat_template_kwargs`, `bad_words`.

**Also recognised in an `allow` list** (passed straight through when a builder or a future change
includes them): `frequency_penalty`, `presence_penalty`, `max_completion_tokens`, `stop`, `seed`,
`random_seed`, `response_format`, `n`, `tools`, `tool_choice`, `safe_prompt`.

**Using the `custom` profile:** set `api_profile: custom` for the slot and list exactly the params
you want sent in `custom_allowed_params` — it is an allow-list, and each slot's list is independent
(in the editor, the comma-separated box under the dropdown):

```yaml
apis:
  - url: "https://my-endpoint/v1/chat/completions"
    api_profile: custom
    custom_allowed_params: [temperature, top_p, max_tokens]
```

**Adding or changing a profile:** edit `config/api_profiles.yml` — no code change needed. Each entry:

```yaml
my_provider:
  label: "My Provider"                    # shown in the editor dropdown
  allow: [temperature, top_p, max_tokens] # accepted body keys; or `all` (send everything) / `from_config`
  rename: {repetition_penalty: frequency_penalty}   # optional; applied before filtering
  detect_hosts: [api.myprovider.com]      # optional; used by the Detect button and auto-detection
```

**Profile Types:**
- `all` — Send the payload untouched (no filtering or renaming)
- `from_config` — Use the slot's `custom_allowed_params` list from the config editor
- `[list]` — Explicit allow-list of parameter names to include in the request

**Key Renames:** The only value-aware rename is `repetition_penalty` → `frequency_penalty`, which transforms the value using `(v - 1)` clamped to `[-2, 2]`. All other renames are direct key substitutions.

## Generation Settings

```yaml
generation:
  num_random_chunks: 12000        # Total tasks to generate per run (also opening mode's task count without input files)
  subject_size: 1000              # Characters for the "subject" portion
  context_size: 3000              # Total characters (subject + surrounding context)
  max_attempts: 5                 # Retries per Q/A turn
  num_turns: 1                    # Q/A pairs per conversation
  history_size: 10               # Recent OPENING QUESTIONS to avoid repetition (does not touch the assistant turns)
  api_request_timeout: 300       # Seconds for API connect/read timeout
  max_newlines_malformed: 32      # Max newlines before considering response malformed
  max_text_length_malformed: 5000 # Max text length before considering response malformed
  max_slop_sentence_fix_iterations: 4  # Iterations for sentence-level slop fixing (only runs when detection.slop.enabled: true)
  # max_anti_slop_fix_iterations: 10   # NOT a shipped key -- the worker defaults to 10 in generation.py;
                                       # set it here to override, and it only runs when
                                       # detection.anti_slop.enabled: true
  max_character_cards: 10        # Maximum character profile cards in the editor
  output_format: "sharegpt"       # Output format for JSONL files
  sanitize_input_max_length: 100000000  # Max input text length for sanitization
  slop_to_anti_slop_fallback: false  # Use Anti-Slop API as final attempt if Slop Fixer fails (requires both enabled flags)

  # v12.7.6: Slop and Anti-Slop are OFF BY DEFAULT. Nothing above runs unless you opt in:
  # detection:
  #   slop:
  #     enabled: false            # default false since v12.7.6
  #   anti_slop:
  #     enabled: false            # default false since v12.7.6
  discard_refused_conversations: false  # Don't save a conversation if any reply needed a jailbreak
  use_conversation_history_for_fixers: true  # Pass conversation context to slop/anti-slop fixer LLMs for coherent rephrasing
  include_metadata: true           # Append a flat, schema-stable provenance+quality "metadata" block to each output record (see Output Formats)
  placeholder_share: 0.0          # Opening mode: share of rows (0 to 1) that start with a [Start a new chat] user turn

  # Text post-processing flags
  remove_reasoning: false
  remove_em_dash: false
  remove_asterisks: false
  remove_asterisk_space_asterisk: false
  remove_all_asterisks: false
  ensure_space_after_line_break: false
  remove_markdown: false
  retry_missing_quotations: false     # v11.7.1 Experiment: send a quote-less spoken line back to the same main API for one retry
  repeat_opener_guard: false          # v12.5 Experiment: send a reply that reuses an earlier reply's opener back to the same main API
  repeat_opener_fixes: []             # Instructions appended to the ANSWER PROMPT on a retry (cycled; empty = built-in default)
  repeat_opener_max_retries: 1         # Retries per reply for a fresh opener; 0 detects but never retries
```

> **Note (v12.7.6):** the slop/anti-slop fixer settings above are inert by default. `detection.slop.enabled`
> and `detection.anti_slop.enabled` both ship `false`, so `max_slop_sentence_fix_iterations`,
> `max_anti_slop_fix_iterations`, `slop_to_anti_slop_fallback` and `use_conversation_history_for_fixers`
> only matter once you deliberately turn a layer on. If you want anti-slop on your dataset, the recommended
> path is [AntiSlop-vLLM](https://github.com/Ready-Art/antislop-vllm-fork/tree/dev) — the **`dev` branch,
> not `main`** — which filters at the token level during generation and removes the fixer round trips
> entirely. Full reasoning in [Slop & Anti-Slop](slop-and-antislop.md).
