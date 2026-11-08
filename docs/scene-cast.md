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

**Version:** v12.0, transition detection and slot placement revised in v14.1, scheduling and note
correctness revised in v14.2, arrival-slot blocking and the drop policy revised in v14.3
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

## How the generator decides a beat has LANDED (v14.1)

Before v14.1 the state machine asked one question: **does the character's name appear anywhere in
the reply?** That test was wrong in both directions, and both halves were visible in the same run.

**Too loose.** `Cole is still here, arguing.` contains `Cole`, so the exit was recorded as landed.
The very next reply then blocked a character who had never left the room — the transcript was made
incoherent by the generator itself, which is the exact failure this feature exists to prevent.

**Too strict.** `She grabbed her coat and headed for the door.` contains no name at all, because
prose uses pronouns. A perfectly good exit was recorded as `exit_deferred` and then
`exit_cancelled`. **This is why characters appeared never to leave the scene**, and it was the
common case, not the edge case.

v14.1 replaces the name test with **evidence of the transition**:

> A departure (or arrival) verb in a sentence that is *about* the character.

Attribution comes from the name in the same sentence, or — when exactly one character is
mid-transition — from a pronoun. A negated sentence is never evidence.

| Reply | Lands the exit? | Why |
|---|---|---|
| `Cole said goodbye and left.` | yes | name + departure verb |
| `She grabbed her coat and headed for the door.` | yes | one candidate, so `she` is unambiguous |
| `"I have to go," Cole said.` | yes | attributed dialogue with a departure phrase |
| `Cole is still here, arguing.` | **no** | no departure verb |
| `Cole did not leave.` | **no** | negated |
| `Cole nodded slowly.` | **no** | `nodded` is not a departure |
| `Cole watched Ines leave.` | Ines, not Cole | the verb belongs to the object |

A pronoun is only accepted when **one** character is mid-transition. With two candidates it would be
a guess, so it is ignored and the beat defers.

### `exit_detection`

| Value | Behaviour |
|---|---|
| `evidence` (default) | The verb test above. This is what makes exits actually land. |
| `name` | The old v12.0 name-anywhere test, kept so the change can be A/B'd without editing code. |
| `always` | Every scheduled transition lands on its slot, narrated or not. **Use with care:** it can mark a character gone who is still visibly in the room, which the continuity check then reports. |

An unrecognised value falls back to `evidence`.

---

## Where the beats land, and why a short conversation needs help (v14.1)

The automatic schedule anchors the arrival around the **first third** and the departure at the
**halfway point**. That suits a long conversation. In a short one it does not:

| Replies | Automatic exit | What you see |
|---|---|---|
| 4 | reply 3 | the scene ends on reply 4, so the departure has one reply of aftermath |
| 6 | reply 4 | workable |
| 12 | reply 6 | comfortable |

With 4 replies the exit lands on the second-to-last thing that happens, so the scene is over before
the departure reads as a departure — which is the other reason characters seemed never to leave.

`join_slot` and `exit_slot` place the beats directly (0 = automatic):

```yaml
prompts:
  scene_cast:
    join_slot: 2     # the newcomer arrives on reply 2
    exit_slot: 3     # the departure happens on reply 3, leaving reply 4 for the aftermath
```

Both are **clamped, never trusted**:

- an arrival needs a reply left to be present in, and cannot be the opening reply;
- a departure needs a reply left for the aftermath, so an exit on the last reply is pulled back one;
- an exit is never scheduled on or before the arrival it follows.

Slot numbers are reply numbers, counting the greeting in opening mode.

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
She looks at Melody and smiles.            <- Melody is the OBJECT of "at"; the smile is hers
Melody's hands trembled.                  <- possessive, not a spoken line
Melody. Cole nodded.                      <- the action is in Cole's sentence
```

Quoted dialogue is blanked out before the action match, so a character being *discussed* is never
mistaken for a character being *present*. This is the same narration-only rule the OOC drift check uses.

A name that is the **object of a preposition** (`at`, `to`, `with`, `beside`, …) is not the actor, so the
next action verb in that sentence belongs to whoever the sentence is about. Before v14.3 the detector
blamed the object, which was the single largest source of false hits on real prose: on one operator run
27% of the replies that named a present character were flagged, most of them this shape.

### A character mid-transition is never blocked on the reply that narrates the beat

The check is driven by `generation._cast_names_for_slot()`, which returns the names that may NOT appear.
A character whose entrance or exit **this reply is asked to narrate** is never in that list, even though
its stored state is still `absent`/`present` (the state only flips after the reply comes back).

Before v14.3 the plain `absent`/`exited` test put the arriving character in **both** the blocked list and
the expected list, so the note said `Dani ARRIVES in this reply` and the check then flagged the model for
writing exactly that. Every arrival reply was regenerated to exhaustion. If you change this function, keep the
invariant: **no name may be both blocked and expected on the same slot.**

### On a hit

The reply is sent back to the **same main API slot** that produced it, with one fix appended to the
system prompt — exactly like the refusal, slop and OOC checks. No fixer slot, no extra model.

Once `max_rounds` (default 2) fixes are exhausted, the conversation is **DROPPED**. This is the one check
that does not keep its best-effort text, and the asymmetry is deliberate:

- a slop phrase or a repeated opener is a **style** miss — the transcript still makes sense, so losing the
  row would be the bigger loss;
- a continuity miss is a **cast contradiction** — the text has a character who has left the scene still
  speaking or acting, which is the exact corruption this feature exists to prevent. Writing it would put a row
  in the dataset that argues with itself.

The drop is recorded in `app_state.quality_dropped` with the reason `scene_continuity`, so the dashboard
reports it and a resumed run does not retry it, and the task is still credited for progress so the bar can
reach 100%. Before v14.3 the reply was kept and written.

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

### The note is not the same for both sides (v14.2)

The **assistant** is being asked to *narrate* this reply's beat, so a character mid-arrival is listed as
arriving and the directive below puts her in the room:

```
SCENE CAST:
PRESENT: Melody.
ARRIVING IN THIS REPLY: Ines.
Ines ARRIVES in this reply. Bring Ines into the scene with a natural entrance -- ...
```

An `ARRIVING` character is **never** also listed under `NOT IN THE SCENE` in the same note. The two lines
used to appear together, and a model handed a contradiction resolves it by doing neither.

The **simulated user** gets a presence-only note with no directive (see `transitions=False` below), and for
him a character mid-arrival has **not arrived yet** — the reply that was meant to walk her in did not do it,
so she is still outside. She is therefore listed under `NOT IN THE SCENE`, and his note adds one line:

```
Do not speak to them, call out to them, summon them or wait for an answer from them -- they cannot
hear you and they are not in this room.
```

Without that, a first-person narrator reads `PRESENT: Teresa` and writes *"Come here, Teresa"* one reply
before she is written entering the room — and the next reply then narrates her walking in from outside, as
if she had heard him through the wall. An `exiting` character stays `PRESENT` for both readers: the exit has
not been narrated yet, so she is still here.

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
    exit_detection: evidence  # evidence | name | always  (v14.1)
    join_slot: 0              # 0 = automatic; else the reply the arrival lands on
    exit_slot: 0              # 0 = automatic; else the reply the departure lands on
    exit_guidance:            # one per line; empty = built-in defaults
    - has somewhere to be and says so plainly before going
    - leaves without explanation, and the silence afterwards says it for them
    - is called away by someone at the door and does not come back
    - gets what she came for and simply stops being interested
    - is sent away by one of the others, not by the user
    - leaves in anger and slams something on the way out
    - finishes what she was doing, tidies up, and walks out mid-sentence
    - is already halfway out when the reply begins, and does not turn around
    arrival_guidance:
    - comes in from the street and does not apologise for it
    - was already in the building and finally shows up
    - is brought in by one of the characters already present
    - arrives late and has to explain herself
    - lets herself in and waits to be noticed
    - interrupts something already in progress
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

One instruction per line. Empty means the built-in defaults are used, so you can switch the feature
on without authoring anything.

**Since v13.5.1 the shipped config populates both lists**, and the worker picks the entry by
`(slot number - 1 + a per-task offset)`, so two rows of the same run do not share their exit or
arrival beat. Before the offset existed the entry was chosen by slot number alone, and because every
row of a run has the same `num_turns` the exit landed on the same slot number in every row — every
row's exit was the same beat (the character checks her phone, takes a work call, and leaves). The
offset is derived from the task id and is constant for the whole task, so it costs no prefix-cache
reuse within a row.

**Write the entries so they differ in KIND, not just in wording.** Six ways of saying "she has an
errand" still read as the same exit. The shipped list deliberately mixes a stated reason, a silent
exit, an interruption from outside, a goal met, a third party sending her away, and an angry exit.

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

This is a prompt-level nudge, not a new generation path. **It ships `false` and should usually stay
that way:** in practice it produced disconnected filler rather than a scene — replies opening on a stray
dog barking at a truck, a stray cat in a courtyard, or a stranger asleep in a basement suite —
because the model treats "a brief moment elsewhere" as licence to cut away entirely. The join/leave
feature works without it.

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
| 5 | Melody, Ines | `ARRIVING IN THIS REPLY: Ines` + `Ines ARRIVES...` | Ines comes in; Cole is gone |
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
| Nothing happens, no `cast_schedule` in metadata | Feature off, fewer than 2 characters, or an infeasible scene | Check the log for the `Scene cast enabled but...` warning; the panel now states the character count you need |
| Characters never leave, even with a long conversation | Pre-v14.1: exits were cancelled because the reply used a pronoun | Fixed in v14.1 by `exit_detection: evidence`. Check `cast_events` for `exit_cancelled` |
| `Scene continuity violation ... fixes exhausted` on almost every reply | Pre-v14.3: the arriving character was in the blocked list AND the expected list on its arrival slot, so the check flagged the model for writing the entrance the note asked for | Fixed in v14.3: `_cast_names_for_slot()` never blocks a character it is asking the model to walk in or out. Check the `Scene cast plan ...` DEBUG line |
| A character is flagged for acting when someone else did it | Pre-v14.3: `find_character_actions()` blamed the name in a preposition's phrase (`She looks at Dani and smiles`) | Fixed in v14.3: an object of `at`/`to`/`with`/… is not the actor |
| Rows in the dataset where an exited character still talks | Pre-v14.3: the exhausted continuity check kept the best-effort answer and the row was written | Fixed in v14.3: an unfixable continuity miss DROPS the conversation (`scene_continuity` in `quality_dropped`) |
| Characters never leave in a **2-character** scene, whatever `exit_slot` says | Pre-v14.2: the leave budget was computed against the *leavable* set (`len(leavable) - 1`), which is 0 when only one character can leave, so the exit was never scheduled at all | Fixed in v14.2: the budget is `spare` (`cast_size - min_present`). Check the `Scene cast plan ...` DEBUG line for `exit=<slot>` |
| A newcomer is introduced twice, or arrives with no prompt to do so | Pre-v14.2: the transition scan skipped every non-`present` state, so the `ARRIVES` directive was never emitted on the slot it was scheduled for | Fixed in v14.2. Check the `Scene cast plan ...` DEBUG line and the note's `ARRIVING IN THIS REPLY` line |
| The user calls a character over, and the next reply has her walking in from outside | Pre-v14.2: the man's presence-only note listed a not-yet-arrived character as `PRESENT` | Fixed in v14.2: she is `NOT IN THE SCENE` for him, and his note forbids summoning her |
| A character leaves who is still in the room | `exit_detection: name` (a bare mention counted) or `always` | Set `exit_detection: evidence` |
| The departure lands too late to have an aftermath | The automatic anchor, in a short conversation | Set `exit_slot` explicitly (e.g. `3` for a 4-reply scene) |
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
| Transition evidence (v14.1) | `detection.py` | `find_transition_evidence()`, `_DEPARTURE_VERBS`, `_ARRIVAL_VERBS`, `_NEGATED_TRANSITION_RE` |
| Slot placement (v14.1) | `generation.py` | the `join_slot` / `exit_slot` override block in `_build_scene_cast_plan()` |
| The retry branch | `generation.py` | `generate_answer_with_retries()` — after OOC, before Regex Watch |
| The drop (v14.3) | `generation.py`, `quality.py` | the 4th element of `generate_answer_with_retries()`'s result, the `cast_violation_this_task` gate in `worker()`, `quality.DROP_REASON_SCENE_CONTINUITY` |
| The Judge dimension | `quality.py` | `CAST_SCORING_DIMENSIONS`, `is_cast_scoring_enabled()`, the cast block in `_score_with_llm()` |
| GUI | `config_editor.py` | Scene Cast panel (Character Engine), Scene Continuity Check panel (Detection), cast toggle (Quality Scoring) |
| Dashboard | `generate.py`, `dashboard.py`, `app_state.py` | Cast Drift metric card, issue panel, graph series, cast event log |
| Config | `config/config.yml` | `prompts.scene_cast.*`, `detection.scene_continuity.*`, `quality.cast_continuity` |
| Guard | `tests/test_scene_cast.py` | schedule invariants, the asymmetry, note cache stability, the detector, metadata v7, dimension gating, transition evidence, slot overrides, the v14.3 arrival-slot and preposition-object regressions |
| Guard (drop) | `tests/test_scene_continuity_drop.py` | the real worker against a fake server: an unfixable violation writes no row, is recorded as a `scene_continuity` drop, and a clean conversation is still written |
| Guard (GUI) | `tests/test_scene_cast_settings_ui.py` | the three v14.1 controls survive a save/reopen round trip through the real `ConfigEditor` |
