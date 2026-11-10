<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# 📄 Output Formats

## ShareGPT Format (default)
```json
{
  "id": "filename_chunk_at_1234",
  "conversations": [
    {"from": "human", "value": "What is the main theme of..."},
    {"from": "gpt", "value": "The main theme revolves around..."}
  ]
}
```

In **opening mode**, a row starts with the character's greeting, marked `"train": false`; with `placeholder_share` above 0, that share of rows puts a `[Start a new chat]` user turn first, as SillyTavern's Strict prompt post-processing sends:
```json
{
  "id": "opening_0",
  "conversations": [
    {"from": "gpt", "value": "Rain drums on the ferry's tin roof. Ines looks up from the mooring rope. \"Last crossing. You coming?\"", "train": false},
    {"from": "human", "value": "I hurry down the ramp. \"Wait for me!\""},
    {"from": "gpt", "value": "..."}
  ]
}
```

`"train": false` keeps the greeting in the model's input but out of the loss only when the trainer reads it. In Axolotl, add `message_field_training: train` to the dataset entry; without it, Axolotl trains on the greeting like any other `gpt` turn:
```yaml
datasets:
  - path: output.jsonl
    type: chat_template
    field_messages: conversations
    message_property_mappings: {role: from, content: value}
    roles: {user: [human], assistant: [gpt]}
    roles_to_train: [assistant]
    message_field_training: train
train_on_inputs: false
```

## Record Metadata Block (NEW in v9.9.0)

When `generation.include_metadata: true` (the default), every record also carries a flat, schema-stable `metadata` block with provenance and quality information:
```json
{
  "id": "opening_0",
  "conversations": [
    {"from": "gpt", "value": "Rain drums on the ferry's tin roof. ...", "train": false},
    {"from": "human", "value": "I hurry down the ramp."},
    {"from": "gpt", "value": "..."}
  ],
  "metadata": {
    "meta_version": 6,
    "characters": ["Ines"],
    "emotional_state": "anxious",
    "user_emotional_state": "curious -> defensive",
    "setting": "A dockside ferry terminal at dusk.",
    "num_characters": 1,
    "opening_mode": true,
    "has_greeting": true,
    "duplication_turn": 0,
    "quality_method": "hybrid",
    "quality_composite": 87.3,
    "quality_coherence": 90.0,
    "quality_naturalness": 85.0,
    "quality_engagement": 82.0,
    "quality_diversity": 95.0,
    "quality_consistency": 88.0,
    "quality_logic": 90.0,
    "quality_technical": 92.0,
    "quality_voice_distinctiveness": -1.0,
    "quality_grounding": -1.0,
    "quality_persona_alignment": -1.0,
    "quality_tic_density": -1.0,
    "generated_at": "2026-01-15 14:32:00"
  }
}
```

Field notes:
- **`characters`** — list of character names in the scene, as the generator assigned them (the random names when random names are on, recorded even when `include_names_in_prompt` is off; `[""]` when no character engine profile was selected). Fixed-shape list of strings.
- **`emotional_state` / `setting`** — the state or arc assigned to the conversation, and the first non-empty setting the prompt carried; `""` when none (`setting` is `""` whenever setting selection is off).
- **`user_emotional_state`** — (NEW in v10.6.0) the simulated user's assigned emotional state/arc label when `prompts.user_emotional_states.enabled` is on; `""` when the user-side feature is off.
- **`opening_mode` / `has_greeting`** — whether opening mode was active and the record starts with a greeting turn.
- **`duplication_turn`** — in Duplication Mode, the 1-based turn number for per-turn rows; `0` for full-conversation rows.
- **`quality_*`** — the quality score of this exact conversation (scoring runs *before* the write so the numbers always describe the row). All `-1.0` when the conversation was not scored (scoring disabled or failed, or an empty conversation); the three roleplay fields are additionally `-1.0` whenever `quality.roleplay_dimensions` is off; `quality_tic_density` (v11.7) is `-1.0` whenever `quality.tic_dimension` is off. `quality_method` is `"heuristic_only"`, `"hybrid"`, `"none"` (empty conversation) or `""` (not scored).
- **`style_*`** — (NEW in v11.7, only when `generation.include_style_metrics: true`) the flat scotoma style measurements for this record's assistant turns, aggregated into means (`style_commas_per_sent`, `style_denial_correction`, `style_absolute_trailer`, `style_tail_affect_lift`, … plus `style_turns`). Same-key/same-type on every record; `-1.0` where a metric is unmeasured (e.g. `style_grounding_per_100w` with no context). Off by default, so the default record keeps exactly the keys shown above.
- **`generated_at`** — when the row was written, local time.

**Trainer safety:** the block is a sibling of `id` and `conversations` — it is never injected into `conversations[]`, so it does not enter any chat template. Every record carries the same keys with the same scalar types (23 with the style-metrics block off, a fixed additional ~24 `style_*` columns when `include_style_metrics` is on), and none is ever `null` (gaps are `""`, `0` or `-1.0`), so Hugging Face column inference and Axolotl's `load_dataset` + template mapping are unaffected, exactly like the pre-existing `id` field. In your Axolotl dataset entry, nothing changes:
```yaml
datasets:
  - path: output.jsonl
    type: chat_template
    field_messages: conversations
    message_property_mappings: {role: from, content: value}
    roles: {user: [human], assistant: [gpt]}
    roles_to_train: [assistant]
    message_field_training: train
```
You *can* use the extra columns for free post-generation filtering in the training repo, e.g. keep only high-scoring rows:
```python
from datasets import load_dataset
ds = load_dataset("json", data_files="output.jsonl", split="train")
ds = ds.filter(lambda r: r["metadata"]["quality_composite"] >= 80)
```

Set `generation.include_metadata: false` (Generation tab in the Configuration Editor) to restore the legacy two-field records (`id` + `conversations` only) byte-for-byte.

In **Duplication Mode**, output files are named per API slot:
- `output/output_api_slot_0.jsonl`
- `output/output_api_slot_1.jsonl`
- etc.

In **Collaborative Mode**, a single file is produced:
- `output/output.jsonl`

When **PostgreSQL** is enabled, conversations are stored in the `generated_conversations` table and file writing is skipped entirely. Use the **Export DB → JSONL** button to export.

When **quality scoring is enabled**, flagged conversations exported via the Review tab include a `_quality_score` field:
```json
{
  "id": "filename_chunk_at_1234",
  "conversations": [...],
  "_quality_score": {
    "composite": 42.5,
    "dimensions": {"coherence": 38, "naturalness": 55, "engagement": 40, "diversity": 62, "consistency": 45, "technical": 58},
    "flags": ["below_threshold_50", "low_coherence"],
    "scored_at": "2025-06-15 14:32:00",
    "method": "hybrid"
  }
}
```

> **Note:** A conversation in which a reply was refused and a jailbreak then fixed it is saved. Set `discard_refused_conversations: true` to discard and requeue such conversations instead. Incomplete conversations (fewer turns than configured) are also excluded and requeued for retry.
