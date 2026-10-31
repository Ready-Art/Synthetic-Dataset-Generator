<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Scene Cast — Characters That Join and Leave

**Version:** v12.0
**Applies to:** multi-character scenes (2 or more characters selected)
**Default state:** OFF

---

## What this feature is for

Every other character feature in this generator assumes the cast is fixed: whoever is in the
character profile block is in the scene for the whole conversation, and the model writes all of them
every turn.

That makes the user the centre of the world. In a real room, people come and go. Someone leaves to
catch a shift. Someone else arrives because they heard raised voices through the wall. The scene keeps
going without them, and the people who stayed notice that they left.

**Scene Cast** produces those beats. But its real job is harder than "make characters leave": it is
making sure the cast never drifts out of sync with the transcript. A character who walked out in reply 3
cannot be talking again in reply 5. That is the failure this feature exists to prevent.

---

## The one idea you need to understand

> **The generator owns the cast. The model only narrates it.**

This is the entire design, and everything else follows from it.

The schedule — who is in the scene at each reply — is computed **before any request is sent**. The
model is then told the state and asked to write the transition naturally. It never gets to decide whether
a character has left.

Why invert it? Because the alternative is unenforceable. If you ask a model to "have someone leave
naturally when it feels right", three things can happen:

1. It never writes an exit at all, and the scene goes stale.
2. It writes an exit, then forgets and has the character speak again two replies later.
3. It writes a character vanishing mid-sentence with no reason.

Only the first is a quality problem. The second is a *corruption* problem — it makes the transcript
incoherent, and it is exactly what "a character that left cannot suddenly speak again" means.

So the generator decides the schedule. The model writes the prose.

---

## The state machine

Each selected character is in exactly one state at any moment:

| State | Meaning | May speak or act? |
|---|---|---|
| `absent` | Not yet in the scene (scheduled to arrive later) | **No** |
| `arriving` | Arrival directive issued this reply, not yet narrated | **Yes** — that is the beat |
| `present` | In the scene | **Yes** |
| `exiting` | Exit directive issued this reply, not yet narrated | **Yes** — that is the beat |
| `exited` | Left the scene | **No** |

`absent` and `exited` are the two states that may not speak. Everything else may.

---

## What happens when the model forgets

This is the part worth reading carefully, because it is not symmetric, and the asymmetry is
deliberate.

### A forgotten exit is CANCELLED, never assumed

The generator schedules an exit for reply 4. The model writes reply 4 without mentioning the character
leaving.

The generator does **not** assume the character left anyway. It **cancels the exit** and puts the character
back to `present`.

Why? Because assuming the exit would be catastrophic in a quiet way. The very next reply, the character
would legitimately speak — and the continuity check would flag it as a violation. The generator would be
punishing the model for a mistake the generator itself made. You would get a row dropped for a departure that
never appeared in the text.

Cancelling is the only direction that cannot produce an incoherent dataset. The character simply stays, the
scene continues, and the metadata records `exit_cancelled` so you can see it happened.

### A forgotten arrival is FORCED to present

The mirror case goes the other way. If the model never narrates a scheduled arrival, the generator **forces**
the character to `present` after `max_deferrals` replies.

Being in the scene is the default. It cannot corrupt the transcript. The character is simply there, and the
next reply treats them as present. Nothing was ever said that contradicts it.

### Summary

| Model wrote… | Generator does | Recorded as |
|---|---|---|
| the scheduled exit | character becomes `exited` | `exited` |
| **nothing** on a scheduled exit | **exit CANCELLED**, character stays `present` | `exit_cancelled` |
| the scheduled arrival | character becomes `present` | `arrived` |
| **nothing** on a scheduled arrival | **arrival FORCED** to `present` | `arrival_forced` |
| nothing yet, still deferring | state unchanged, directive re-issued | `exit_deferred` / `arrival_deferred` |

`max_deferrals` (default 1) is how many replies a transition may slip before the generator gives up
and reverts the state.

---

## The hard continuity check

A character who has `exited` or is still `absent` **may not speak or act**. This is enforced by
`detection.check_scene_continuity()` — with no LLM call, because it is an objective fact that can be
detected mechanically.

### What it catches

**An attributed spoken line.** A quoted span is attributed to a character when the narration around it names
them and carries a speech verb, or when a bare speaker prefix sits right before the quote:

```
Melody said, "I am still here."          <- caught
"I am still here," Melody said.           <- caught
Melody: "Still here."                    <- caught (script-style prefix, no verb needed)
```

**A narration action.** A blocked name followed by an action verb in the same sentence:

```
Melody nodded slowly.                    <- caught
Melody walked away.                      <- caught
```

### What it deliberately does NOT catch

```
Cole asked, "Where is Melody?"            <- Melody is only MENTIONED inside Cole's line
Cole nodded at Melody.                    <- the action belongs to Cole, not Melody
Melody's hands trembled.                  <- possessive, not a spoken line
Melody. Cole nodded.                      <- the action is in Cole's sentence
```

Quoted dialogue is blanked out before the action match, so a character being *discussed* is never
mistaken for a character being *present*. This is the same narration-only rule the OOC drift check uses.

### On a hit

The reply is sent back to the **same main API slot** that produced it, with one fix appended to the
system prompt — exactly like the refusal, slop and OOC checks. No fixer slot, no extra model.

Once `max_rounds` (default 2) fixes are exhausted, the best-effort answer is **kept**. A continuity
miss is a scheduling miss, not a reason to lose the row.

### A missing entrance or exit is reported, never regenerated

If the model forgets to write a beat, that is recorded as a soft finding (`expected_missing`) but it
**never** triggers a rewrite. The state machine handles it for free by deferring and then cancelling or
forcing. Rewriting for it would burn an API call to fix something the generator can settle
deterministically.

---

## The cast note, and why the cache survives

The generator tells the model who is in the scene through a per-reply note. By default it goes in the
`depth0` position — the end of the reply's final user message, after the history — beside the persona and
emotion notes.

That placement matters. The **system prompt never changes** between replies, so a server that keeps a prefix
cache (llama-server, vLLM) reads the history from its cache instead of recomputing it.

The note itself is written to be cache-safe:

```
SCENE CAST:
PRESENT: Melody, Cole.
NOT IN THE SCENE: Ines.
Do not give the absent characters dialogue, actions, thoughts or feelings. They are not here.
```

There is **no "reply 3 of 5" counter** in it. The note depends only on the present/absent *sets* and
this reply's transitions — never on the slot index. Two replies with no transition produce **byte-identical**
notes.

On a transition reply, one directive is appended:

```
Cole LEAVES the scene in this reply. Have Cole exit naturally -- give them a concrete reason
to go -- somewhere to be, someone waiting, a shift ending -- and let them actually leave the
room. After this reply Cole is gone: do not write Cole again.
```

If you set `placement: system`, the note moves to the end of the system prompt instead. The generator
then warns you at Start with an estimate of how much history the server will recompute. `depth0` is the
default for a reason.

---

## Turning it on

Config Editor → **Character Engine** tab → **Scene Cast — Characters Join and Leave**.

```yaml
prompts:
  scene_cast:
    enabled: true             # the master toggle
    shape: minimal            # minimal | churn | none
    min_present: 2            # never fewer than this many characters present
    max_churn: 2              # only used by shape: churn
    max_deferrals: 1          # replies an un-narrated exit may slip before cancelling
    placement: depth0          # depth0 (cache-friendly) | system
    exit_guidance: []         # one per line; empty = built-in defaults
    arrival_guidance: []
    allow_cutaway: false      # a short moment elsewhere, rejoining the main thread
    cutaway_note: ''
```

### `shape`

| Value | What it does |
|---|---|
| `minimal` (default) | At most one join and one leave per conversation. Cheapest to steer, hardest for the model to get wrong, and enough to satisfy the feature. |
| `churn` | Up to `max_churn` joins and leaves, never dropping below `min_present`. For longer conversations where a static cast feels wrong. |
| `none` | Never schedule. Useful to A/B the feature without clearing the config. |

### `min_present` — read this before wondering why nothing happens

**A join and a leave each cost one present character.** A joiner starts `absent`; a leaver ends
`absent`. Either transition momentarily removes one character from the scene.

So with `min_present: 2` and only 2 characters selected, **no change is possible**. There is no
spare character to spend. The worker logs:

```
Scene cast enabled but inactive for task ... (2 character(s), min_present=2, shape=minimal,
6 slot(s)) -- no join or leave is possible in this scene.
```

and runs a static cast. It does not fail, and it does not silently do nothing — but if you see that
line, the feature is working exactly as designed and you need 3+ characters, or a lower `min_present`.

| Characters | `min_present` | Joins possible? | Leaves possible? |
|---|---|---|---|
| 2 | 2 | no | no |
| 2 | 1 | yes | yes |
| 3 | 2 | yes | yes |
| 3 | 3 | no | no |
| 4 | 2 | yes | yes |

The conversation also needs enough replies. A leave needs at least 2 assistant slots; a join needs at
least 3, so the newcomer has a reply to be present in.

### `exit_guidance` / `arrival_guidance`

One instruction per line. The worker rotates through them so consecutive exits do not read the same. Empty
means the built-in defaults are used, so you can switch the feature on without authoring anything.

The guidance is saved **verbatim** — quotes, apostrophes and backslashes are preserved exactly as typed.

```
exit_guidance:
- has somewhere to be and says so plainly before going
- leaves without explanation, and the silence afterwards says it for them
arrival_guidance:
- comes in from the street and does not apologise for it
```

### `allow_cutaway`

When on, a transition reply may open on a brief moment **elsewhere** while the user is not
present, as long as the reply rejoins the main thread before it ends. The cast note then carries:

```
CUT-AWAY: You may open on a brief moment elsewhere in this world, while the user is not
present. Keep it short, keep it to the characters who are actually there, and rejoin the
main thread before the reply ends.
```

This is a prompt-level nudge, not a new generation path. If it proves hard to steer, leave it off —
the join/leave feature works without it.

---

## Reading the results

### The dashboard

The main window gains a **Cast Drift** metric card and a **Recent Cast Drift** panel on the Totals
and per-API tabs, plus a `Cast Drift` series in the issue graph. These count **hard** violations only —
an exited character who spoke or acted and triggered a regeneration.

### The metadata

Every record carries three flat keys (schema version 7):

| Key | Example | Meaning |
|---|---|---|
| `cast_schedule` | `Melody:enter 0 exit 4; Cole:enter 2 exit -` | The plan this task ran. `enter 0` = present from the opening; `exit -` = never leaves. `""` when the feature was off. |
| `cast_events` | `Melody exited; Cole arrived` | What actually happened, in order. `""` when nothing happened. |
| `quality_cast_continuity` | `88.0` | The Judge's score, or `-1.0` when the dimension is off or unscored. |

### How to tell whether the model is actually writing its exits

`cast_events` is the answer. Look for the balance of events:

| What you see | What it means |
|---|---|
| `exited` / `arrived` | The model wrote the beat. Working as intended. |
| `exit_cancelled` | The model **never** wrote a scheduled departure. The character stayed. Count these. |
| `arrival_forced` | The model never wrote a scheduled arrival. The character was placed anyway. |
| `exit_deferred` / `arrival_deferred` | Normal — the first reply of a transition usually defers. |

**A high `exit_cancelled` rate means the steering is not landing.** The transcript is still
coherent — that is the point of cancelling — but the feature is not delivering the beats you asked
for. Levers, in order:

1. **Read the `exit_guidance` you wrote.** Vague guidance ("leave naturally") is the most common
   cause. Say what the reason is and that they actually go.
2. **Give the scene more room.** `num_turns: 4` with a leave scheduled on reply 4 leaves no reply to
   show the aftermath. Raise `num_turns`, or use `shape: minimal` with fewer transitions.
3. **Raise `max_deferrals`** so a transition gets a second reply to land. Costs nothing but time.
4. **Check `min_present`** — if the schedule is empty, nothing is scheduled to cancel at all. Look for
   the `WARNING` in the log.

`exit_cancelled` is a *measurement*, not a failure. It tells you the model's steering needs work
rather than the transcript being broken.

### The Judge dimension

`quality.cast_continuity` (off by default) asks the Judge what code cannot: were the entrances and
exits **motivated and natural**, or did a character vanish mid-sentence? Did an arrival have a reason to
be there? Was an exited character brought back?

It is **LLM-only, with no heuristic**. There is no measurable proxy for "was that exit earned?", and
the objective half of the question — an exited character speaking at all — is already answered for free by
the code check. Paying a judge to re-derive a regex result would only add latency and a new drop reason.

The judge is given `cast_schedule` and `cast_events` so it can see what was supposed to happen, rather
than guessing.

---

## Worked examples

### Example 1: a 3-character scene, `shape: minimal`, `min_present: 2`, 6 replies

Schedule: `Melody:enter 0 exit -; Cole:enter 0 exit 4; Ines:enter 5 exit -`

| Reply | Present | Note says | Model should write |
|---|---|---|---|
| 1 | Melody, Cole | `NOT IN THE SCENE: Ines` | normal scene, Ines not mentioned |
| 2 | Melody, Cole | `NOT IN THE SCENE: Ines` | normal scene |
| 3 | Melody, Cole | `NOT IN THE SCENE: Ines` | normal scene |
| 4 | Melody, Cole | `Cole LEAVES...` | Cole says goodbye and goes |
| 5 | Melody, Ines | `Ines ARRIVES...` | Ines comes in; Cole is gone |
| 6 | Melody, Ines | `NOT IN THE SCENE: Cole` | Ines is here; Cole is not |

If the model writes reply 4 with no exit:

- reply 4 → `exit_deferred`, Cole is `exiting`
- reply 5 → `exit_cancelled`, Cole is `present` again
- reply 6 → Cole is legitimately present

The transcript is coherent. `cast_events` reads
`Cole exit_deferred; Cole exit_cancelled` — and that is your signal that the guidance needs work.

### Example 2: the same schedule, and the model brings Cole back

Suppose the model *does* write Cole's exit on reply 4, so Cole is `exited`. Then on reply 6 it writes:

```
Cole leaned in the doorway. "Still here, then."
```

`detection.check_scene_continuity()` attributes the quoted line to Cole, finds Cole in `exited`,
and flags `exited_speaks`. The reply goes back to the same main API slot with:

> A character who has left the scene cannot speak, act, be seen or be described as present again.
> Remove every line, action and reaction belonging to a character who is no longer in the scene...

This is the failure the whole feature exists to prevent, and it is caught without any LLM call.

### Example 3: two characters, `min_present: 2`

The plan is empty and the log says so. This is not a bug — a join and a leave each cost one present
character and there are none to spare. Use 3+ characters, or set `min_present: 1`.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Nothing happens, no `cast_schedule` in metadata | Feature off, fewer than 2 characters, or an infeasible scene | Check the log for the `Scene cast enabled but...` warning |
| `WARNING ... no join or leave is possible in this scene` | `min_present` leaves no spare character | Add a character, or lower `min_present` |
| Lots of `exit_cancelled` | The model is not writing the exits | Sharpen `exit_guidance`; raise `max_deferrals`; raise `num_turns` |
| `Cast Drift` counts climbing | The model is bringing exited characters back | Expected — the check is working. Consider a stronger system prompt or the `cast_continuity` judge dimension |
| The system prompt changes every reply | `placement: system` | Switch to `depth0` (the default) |
| A character is flagged for speaking who never left | Impossible — `exited`/`absent` are the only blocked states | Check `cast_events`; the state machine never blocks a `present` character |

---

## Where it lives in the code

| Layer | File | What |
|---|---|---|
| Schedule + state machine | `generation.py` | `_build_scene_cast_plan()`, `_scene_cast_feasible()`, `_cast_state_from_plan()`, `_cast_slot_transitions()`, `_cast_names_for_slot()`, `_advance_cast_state()`, `_cast_presence_note()` |
| The hard check | `detection.py` | `check_scene_continuity()`, `find_attributed_speakers()`, `find_character_actions()` |
| The retry branch | `generation.py` | `generate_answer_with_retries()` — after OOC, before Regex Watch |
| The Judge dimension | `quality.py` | `CAST_SCORING_DIMENSIONS`, `is_cast_scoring_enabled()`, the cast block in `_score_with_llm()` |
| GUI | `config_editor.py` | Scene Cast panel (Character Engine), Scene Continuity Check panel (Detection), cast toggle (Quality Scoring) |
| Dashboard | `generate.py`, `dashboard.py`, `app_state.py` | Cast Drift metric card, issue panel, graph series, cast event log |
| Config | `config/config.yml` | `prompts.scene_cast.*`, `detection.scene_continuity.*`, `quality.cast_continuity` |
| Guard | `tests/test_scene_cast.py` | schedule invariants, the asymmetry, note cache stability, the detector, metadata v7, dimension gating |
