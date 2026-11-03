<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Same prompt, different address — One made a mask. One made a person.

**Placement experiment · 574 conversations · 4,592 turns**

These two datasets were built from the same prompt pool, with the same hidden-personality
block, and the same emotional arc simulated on both sides. The only meaningful difference is
**where the personality block was placed** in the prompt. That single decision changed what
kind of character the data describes.

> ### ⚠️ Preliminary — read this first
>
> **These results need further testing to be conclusive.** The comparison below is a single
> experiment on one prompt pool, one model and one pair of 287-conversation datasets. It has
> not been replicated across models, prompt pools, conversation lengths or sampler settings,
> and the effect sizes have not been validated against human preference judgements.
>
> **Initial results show promise** — the direction of the effect is consistent across the
> measured registers and the structural controls came out equivalent — but treat it as a
> strong hypothesis worth testing further, not as a settled finding. Do not tune production
> defaults around it on the strength of this page alone.

## At a glance

| Metric | Value |
|--------|-------|
| Conversations in each set | 287 |
| Exchanges per conversation | 4 |
| Words per assistant turn | ~185 |
| Words per user turn | ~36 |

Structurally identical. Both sets are the same length, the same shape, the same
turn-for-turn rhythm — right down to the same sentence counts and the same proportion of
quoted speech (13.6% vs 14.4% of words).

## The two placements

| Set | Placement of the personality block | What the model sees at generation time |
|-----|------------------------------------|----------------------------------------|
| **Dataset A** | **System prompt** | The block sits at the top of the prompt, thousands of tokens away from the reply, competing with the profile and the full history. |
| **Dataset B** | **Depth-0** | The block sits at the very end of the prompt, immediately before the tokens being generated. |

Same text. Same instruction. Different distance from the point of generation — and,
crucially, a different *arrival*: in the depth-0 set the block enters the prompt partway
through the conversation, so the model experiences it as a change rather than a constant.

In the generator this is the `depth0` vs `system` choice on the per-reply blocks —
`prompts.character.persona_breakthrough_placement` and `prompts.emotional_states.placement`
(and `prompts.scene_cast.placement` for the cast note). `depth0` is the default and appends
the block to the end of the reply's final user message, after the history; `system` appends
it to that reply's system prompt. See
[Prompts & Character Engine](prompts-and-characters.md) and
[Scene Cast](scene-cast.md) for the configuration side. This page is about what the *output*
looks like when you choose `depth0`.

## What we measured

Every assistant turn was scored across all four exchanges of every conversation, tracking
how much the character **interprets** her own experience, how much she **inhabits** it
physically, how much she refers to a mask at all, and how that balance moves from the
opening line to the last.

The headline is not that one dataset is more emotional. By raw emotional volume they are a
dead heat — the same fear, the same wanting, the same grief, rising on the same schedule.
The headline is that **in one dataset the interiority grows, and in the other it fades.**
Both open at exactly the same level. Then they part ways.

### Interiority density across the scene

Language of inner life, self-description and psychological framing, per 1,000 words of
assistant text.

| Exchange | A · system | B · depth-0 |
|----------|-----------:|------------:|
| Opening | 0.81 | 0.79 |
| Exchange 2 | 0.64 | 1.20 |
| Exchange 3 | 0.66 | 1.52 |
| Final | 0.61 | 1.57 |

Both placements open at the same level (0.81 vs 0.79). From the second exchange they
separate completely: the system-prompt set drifts down, the depth-0 set climbs. Trend
difference +0.33 per turn, *p* = 0.0004.

With the block in the system prompt, the character's inner life is **front-loaded and then
spent**. She arrives already self-aware, and as the scene intensifies she has less and less
to say about herself — she is doing, not explaining. With the block at depth-0 the reverse
happens: she starts opaque and becomes progressively more articulate about what is happening
inside her. By the final exchange she is at roughly twice the introspective density she
began with.

> The system prompt spends its interiority. Depth-0 accrues it. Both reach the same emotional
> pitch — they simply arrive from opposite directions.

## Where the two diverge

| Signal | A · system | B · depth-0 | Reading |
|--------|-----------:|------------:|---------|
| Interiority trend across the scene | −0.06 / turn | +0.27 / turn | Flat-and-falling vs. steadily rising |
| Conversations where it rises | 54 / 287 | 111 / 287 | Depth-0 builds twice as often |
| Self-narration ("like a scene", "she narrates") | 0.02 / 1k | 0.17 / 1k | Depth-0's character watches herself |
| Mask / facade vocabulary, final turn | 0.40 / 1k | 0.73 / 1k | System's mask dissolves; depth-0's persists |
| Physical & sensory density, final turn | 28.4 / 1k | 24.5 / 1k | System stays in the body longer |
| Lexical range (type–token ratio) | 0.0244 | 0.0279 | Depth-0 is measurably more varied |
| Distinct emotions per turn | 2.00 → 2.44 | 2.08 → 2.60 | Both broaden; depth-0 slightly more |

Registers measured as density per 1,000 words. Trend figures are mean per-conversation
slopes across the four exchanges. Differences marked in the source comparison are
significant at *p* < 0.01 unless noted — again, pending replication.

### Two ways to describe a person feeling something

**Dataset A · System prompt — Emotion as event**

- Feeling is rendered through **sensation and action** — breath, grip, temperature, the room, the noise.
- The character rarely names her own state; the reader is left to infer it.
- Interiority is strongest at the opening and **thins as the scene escalates**, giving way to pure momentum.
- Mask language appears early and then **drops away** — the performance ends rather than deepens.
- Reads as a character who has already decided who she is, and stops explaining.

**Dataset B · Depth-0 — Emotion as interpretation**

- Feeling is rendered through **self-description** — the character explains her own contradiction to herself.
- She names her state directly and often: restraint, vulnerability, what she is hiding.
- Interiority **accumulates** — every exchange adds another layer of self-interpretation.
- Mask language **persists and broadens** — the mask becomes the subject rather than the obstacle.
- Occasionally frames her own experience as a scene being watched — the block landing as a *lens*, not just an inner state.

## Why placement does this

The pattern is consistent with a simple mechanism, and it is worth stating plainly because
it is the actionable part.

1. **Distance from the generation point.** At depth-0 the block is the last thing in the
   prompt, immediately before the reply — maximum influence on the next tokens. In the
   system prompt it sits thousands of tokens earlier, competing with the profile, the lore
   and the whole history for the model's attention.

2. **The block works as an event, not a standing condition.** In the depth-0 set the block
   physically enters the prompt partway through, so something *changes* and the model
   visibly reacts. In the system set it was present from the first token, so no event marks
   the breakthrough — the voice is established and simply continues.

3. **The system set spends its persona budget early.** Its strongest interiority is at the
   opening, before the character has any reason to be vulnerable, and it declines from
   there. That is the opposite of the intended "mask holds, then slips" arc — which is
   exactly what the depth-0 curve delivers.

**A note on what this does not explain.** Raw emotional volume, sentence counts, dialogue
share and emotion-category breadth are equivalent across both sets. Placement changed *how
the character relates to her own feelings* — not how much feeling there is, and not how the
turns are shaped. It also did not change the simulated user: in both sets the user's turns
carry almost no interiority (under 2.5%) and correlate near-zero with the assistant's
register (r = 0.004 system, 0.072 depth-0). The arc is driven entirely from the assistant
side.

## What this means for a trained model

The two sets will produce models that are equally fluent and equally warm, but that resolve
the same dramatic moment in opposite directions. The difference shows up in the *last third*
of a scene — precisely where users judge whether a character feels real.

### Train on Dataset A (system prompt) → the character who acts

The model learns that intensity is expressed by **doing**. As a scene heats up it gets more
concrete, more physical, more specific about the room and the body, and less willing to
explain itself. Emotion arrives as a fact the reader witnesses rather than a report the
reader receives.

The strength is restraint and immersion: the character never breaks frame to tell you how
she feels, so the user is trusted to read her. The weakness is that the interior can thin
out. Under sustained intensity the model may run out of inner life and substitute motion for
meaning — producing characters who are vivid but not quite *inhabited*.

### Train on Dataset B (depth-0) → the character who explains

The model learns that intensity is expressed by **articulating** it. Interiority compounds
turn over turn, so the character grows more legible, more self-aware and more emotionally
fluent as the scene proceeds — a genuinely rare and valuable quality in a model.

The strength is a character with a continuous inner life and a real arc of self-knowledge.
The cost is **over-narration**. A model trained here is prone to telling the user what the
character feels instead of letting them feel it, and to a self-conscious, slightly
theatrical register in which the character seems aware of her own scene. That reads as
performance rather than presence, and it caps how intimate the experience can get.

## What the end user actually experiences

A user cannot see which placement produced the model. They experience it as a **texture** —
a vague but unmistakable sense of whether the person they are talking to is *present* or
*performing*.

| Moment in the conversation | Model trained on A · system | Model trained on B · depth-0 |
|----------------------------|-----------------------------|------------------------------|
| Opening | Feels grounded and specific | Feels slightly withheld, opaque |
| Middle | Momentum builds; detail does the work | The character begins to explain herself |
| Peak | Vivid, physical, *present* | Articulate, layered, *legible* |
| Afterwards | May feel emotionally thin, resolved by action | May feel over-explained, slightly staged |
| The tell | "I could feel what she wasn't saying." | "She told me exactly what she felt." |

## Which placement to ship

**Depth-0 is the stronger placement, and the gap is not marginal** — on this evidence. It
doubles the rate at which interiority accumulates, it produces the intended "mask holds,
then slips" arc instead of its inverse, and it yields measurably more varied prose. The
system prompt trades all of that away and returns a character who peaks emotionally in her
first line and then flattens. *Subject to the caveat at the top: promising, not yet
conclusive.*

If your product's value is immersion — a character the user forgets is generated — the
**system-prompt** set is the better raw material. Its restraint is the harder skill, and its
failure mode (emotional thinness at peak) is quieter and easier to mask.

If your product's value is companionship — a character the user feels *known by* —
**depth-0** is the better raw material, with a budget for a de-narration pass. Its
accumulating interiority is the single most valuable property in either set, and its failure
mode (a character who explains herself) is the more visible and more fixable of the two.

The strongest option is neither alone. **Depth-0's depth with the system prompt's
discipline** is the target: interiority that compounds without ever announcing itself.

## Suggested follow-up testing

To turn this from a promising signal into a conclusive result, the obvious next steps are:

- Replicate on a second and third base model, including at least one model family not used here.
- Vary conversation length (2, 4, 8, 12 exchanges) — the trend is a slope, so length may change it.
- Vary `persona_breakthrough_turn` and the number of arc stages, to separate "arrival as event" from "content of the block".
- Blind human preference ratings on the last third of each scene, scored for presence vs. performance.
- Re-run the same analysis with the built-in quality scoring and detection layers enabled, to confirm the effect is not an artefact of a particular sampling configuration.

---

## Methodology notes

Analysis of `Dataset-A.jsonl` (system-prompt placement) and `Dataset-B.jsonl` (depth-0
placement) — 287 conversations and 2,296 turns per set. Registers measured as lexical
density per 1,000 words; trend figures are mean per-conversation slopes across four
exchanges; significance from permutation tests (10,000 resamples) on per-conversation
values. Structural controls (turn count, word counts, sentence counts, quoted-speech share,
emotion-category breadth) were verified as equivalent between sets before any comparison was
drawn.
