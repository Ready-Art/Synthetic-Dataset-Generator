# CHANGE-PLAN.md — Scene Cast: Characters That Join and Leave

**Status:** PLANNING ONLY. No code has been changed. This document is the implementation
specification to be executed later, when explicitly prompted.

**Target version:** v11.11.0 (follows v11.10.0 Non-Latin Script Check).

**Scope:** One feature, three layers:

1. **GUI toggle** — a Scene Cast option in the Config Editor's Character Engine tab, effective
   only in multi-character mode (2+ selected characters).
2. **Generator steering** — the worker decides *deterministically* when a character enters or leaves
   the scene, and steers the model to narrate that beat naturally.
3. **Continuity enforcement** — a check that stops an exited character from speaking again, plus an
   opt-in Judge LLM review of how naturally the cast changes played out.

---

## 0. The problem this plan solves

From the design conversation:

> a very very difficult feature to add... "the world doesnt revolve around the player"
> making samples that has cut-away, but still follows the main story lol
> and continuity check... a character that left... cannot suddenly speak again lol

> the tricky part is making sure they leave the scene
> like i can code it in so it sends characters consistently and makes them leave
> but i dont know how to make sure they leave the scene / steer it

### The central design decision

**The generator owns the cast state. The model only narrates it.**

The generator decides the schedule (who is in the scene at assistant slot *k*) *before* the request
is sent. The model is then *told* the state in a per-turn note and asked to write the transition
beat naturally. The model never gets to decide whether a character has left.

This inverts the failure mode the user was worried about. If the model forgets to write the exit, the
character does **not** silently stay in the scene (which would make every later note a lie) and does not
silently vanish (which corrupts the transcript). Instead the generator keeps the character in `exiting`
state, re-issues the directive on the next turn, and if the beat never lands it **cancels the exit**
and the character remains legitimately present. The transcript stays coherent in every branch; only the
schedule degrades.

Two consequences that drive the whole design:

- **The character profile block stays static for the whole task.** No profile is added to or removed from
  the system prompt mid-conversation. All cast information rides in the per-turn **depth-0 note** that
  already exists for persona/emotion (`slot_notes()`), so the system prompt — and therefore the server's
  prefix cache — is untouched between replies. This is the same reasoning already documented for
  `emotion_placement`/`persona_placement` in `AGENTS.md` ("With both on `depth0`, from one answer
  request to the next the system prompt and the history are the same text, which lets a server's prefix
  cache reuse them; keep the default that way").
- **The hard continuity rule is enforced by code, not by prompting.** An exited character with an
  attributed spoken line is an objective, mechanically detectable fact, so it is a `detection.py` check that
  regenerates the turn — exactly like OOC drift and the non-Latin check. The Judge LLM is used only for
  what code cannot judge: whether the entrance/exit *read naturally*.

---

## 1. Design summary

### 1.1 Cast states (one state machine per task)

Each selected character is in exactly one state at any moment:

| State | Meaning | May speak/act? |
|---|---|---|
| `absent` | Not yet in the scene (scheduled to arrive later) | **No** |
| `arriving` | Arrival directive issued this turn, not yet narrated | **Yes** (that is the beat) |
| `present` | In the scene | **Yes** |
| `exiting` | Exit directive issued this turn, not yet narrated | **Yes** (that is the beat) |
| `exited` | Left the scene | **No** |

`absent` and `exited` are the two "cannot speak" states; `present`, `arriving` and `exiting` may speak.

### 1.2 The schedule

Computed once per task, after the character profile loop (see §3.1), as an immutable plan:

```python
# One entry per VALID selected character, in scene order.
cast_plan = {
    "members": [
        {"number": 1, "name": "Melody", "enter_slot": 0,    "exit_slot": 4},
        {"number": 2, "name": "Cole",   "enter_slot": 2,    "exit_slot": None},
        {"number": 3, "name": "Ines",   "enter_slot": 3,    "exit_slot": None},
    ],
    "min_present": 2,
}
```

- `enter_slot` / `exit_slot` are **1-based assistant slots**, the same numbering
  `slot_notes(slot_no)` and `_persona_visible_at(slot_no)` already use. `enter_slot == 0`
  means "present from the opening"; `exit_slot is None` means "never leaves".
- Slot 1 is the greeting in opening mode (`greeting_offset` already accounts for this), so the plan
  is generated against `num_turns + 1 if opening_mode else num_turns` total assistant slots.
- **Feasibility rule:** an exit needs `cast_size >= min_present + 1`; a join needs at least one
  character with `enter_slot > 0`. If neither is possible the plan is empty, the feature is a no-op
  for that task, and a `DEBUG` line records why. A feature that silently does nothing on a 2-character
  scene with `min_present: 2` must say so in the log rather than appear broken.

### 1.3 Scheduling shapes

`prompts.scene_cast.shape` selects how the plan is drawn:

- `"minimal"` (default) — at most one join and one leave across the whole conversation. Cheapest to
  steer, hardest for the model to get wrong, and enough to satisfy the feature.
- `"churn"` — up to `max_churn` joins and leaves, never dropping below `min_present`. For longer
  conversations where a static cast feels wrong.
- `"none"` — plan is empty (useful to A/B the feature without clearing the config).

Selection of *which* character leaves/arrives is random but biased to keep the scene readable: the
character with the highest `number` (the last profile in the block) is the preferred exit, because a
late-arriving character leaving early reads as a cameo rather than a hole.

---

## 2. Config schema (`config/config.yml`)

Three new blocks. None of them alters an existing shipped prompt (see the AGENTS.md boundary:
*"Ask before changing the wording of a shipped prompt"* — so **no edit to `prompts.answer`,
`prompts.system.base` or any other existing key**; all new text is a new key).

### 2.1 `prompts.scene_cast` — scheduling + steering

Sits beside `prompts.emotional_states` and `prompts.random_names` (same `prompts:` level).

```yaml
prompts:
  scene_cast:
    # v11.11.0: characters that join and leave the scene. Only acts in multi-character
    # mode (2+ selected characters); off by default. The GENERATOR decides the schedule --
    # the model is told who is present and asked to narrate the entrance/exit naturally.
    enabled: false
    # minimal = at most one join + one leave per conversation; churn = up to max_churn of each;
    # none = never schedule (the feature stays configured but does nothing).
    shape: minimal
    # Never fewer than this many characters present at any moment.
    min_present: 2
    # Only used by shape: churn.
    max_churn: 2
    # Assistant slots a scheduled exit/arrival may slip before the generator gives up and
    # reverts the character's state (an un-narrated exit is CANCELLED, never assumed).
    max_deferrals: 1
    # Where the per-turn cast note goes: depth0 (default, cache-friendly) or system.
    placement: depth0
    # How a character leaves. One instruction per line; the worker rotates through them
    # so consecutive exits do not read the same. Empty = built-in default.
    exit_guidance: []
    # How a character arrives. Same rules as exit_guidance.
    arrival_guidance: []
    # Optional cut-away: allow a short scene between characters while the user is
    # elsewhere, as long as the reply rejoins the main thread before it ends.
    allow_cutaway: false
    # Instruction appended on a cut-away turn. Empty = built-in default.
    cutaway_note: ''
```

### 2.2 `detection.scene_continuity` — the hard check

Sits beside `detection.out_of_character` (same shape: `enabled` + `fixes[]`).

```yaml
detection:
  scene_continuity:
    # v11.11.0: an EXITED character may not speak or act again, and an ABSENT one may
    # not speak before arriving. Objective and code-only: attributed quoted lines plus a
    # curated narration action-verb list. A hard violation sends the reply back to the
    # SAME main API slot with one of these fixes appended to the system prompt.
    enabled: true
    fixes: []
    # Regeneration rounds per reply before the best-effort answer is kept.
    max_rounds: 2
```

Note `enabled: true` here is deliberate and matches `detection.out_of_character.enabled: true`:
the block is inert unless `prompts.scene_cast.enabled` is also on, so the default config
behaves exactly as v11.10.0.

### 2.3 `quality.cast_continuity` — the Judge review

```yaml
quality:
  # v11.11.0: opt-in Judge dimension 'cast_continuity' -- scores whether entrances and
  # exits READ naturally (motivated, not abrupt) and whether an exited character was
  # brought back. Complements the code-only detection.scene_continuity check, which
  # catches objective violations the judge would only restate.
  cast_continuity: false
  # Per-turn judge confirmation of the cast note. Extra judge call per turn; off by default.
  cast_continuity_per_turn: false
```

---

## 3. `generation.py` — the worker

### 3.1 New module-scope helpers (near `_split_emotional_arc_entry`, line ~400)

Pure functions, no Tk, no network, mirroring the existing emotional-arc helpers so they are
unit-testable headlessly:

```python
DEFAULT_EXIT_GUIDANCE = (...)      # 3-4 rotating exit beats, e.g.
                                   # "have them excuse themselves with a concrete reason"
DEFAULT_ARRIVAL_GUIDANCE = (...)
DEFAULT_CUTAWAY_NOTE = (...)
SCENE_CAST_SHAPES = ("minimal", "churn", "none")

def _scene_cast_feasible(cast_size, total_slots, min_present, shape, max_churn):
    """(can_join, can_leave) for this scene. A leave needs cast_size >= min_present + 1."""

def _build_scene_cast_plan(names, total_slots, config):
    """The immutable schedule for one task: {"members": [...], "min_present": n}.
    Returns an empty plan ({"members": [], ...}) when the feature is off, infeasible, or
    shape is 'none'."""

def _cast_state_from_plan(plan):
    """Initial per-character state dict, keyed by profile number:
    0 -> 'present' (present from the opening), >0 -> 'absent'."""

def _cast_slot_transitions(plan, state, slot_no):
    """Which characters START arriving / exiting at this slot (mutates nothing)."""

def _cast_presence_note(plan, state, slot_no, guidance_rotation):
    """The depth-0/system note text for this slot: who is present, who just arrived,
    who is leaving now, and the hard instruction not to voice anyone else."""

def _advance_cast_state(plan, state, slot_no, answer_text):
    """Apply one reply to the state machine. Returns (new_state, events).
    events is a list of ("arrived"|"exited"|"arrival_deferred"|"exit_deferred"|
    "arrival_forced"|"exit_cancelled", name)."""
```

`_advance_cast_state` is the heart of the "make sure they leave" answer:

```python
# per character, evaluated in plan order
if state[n] == "arriving" and _narrated_in(answer_text, name):
    state[n] = "present"; events.append(("arrived", name))
elif state[n] == "arriving":
    deferrals[n] += 1
    if deferrals[n] > max_deferrals:
        state[n] = "present"            # in the scene, arrival never narrated
        events.append(("arrival_forced", name))
elif state[n] == "exiting" and _narrated_in(answer_text, name):
    state[n] = "exited"; events.append(("exited", name))
elif state[n] == "exiting":
    deferrals[n] += 1
    if deferrals[n] > max_deferrals:
        state[n] = "present"            # CANCEL the exit: never assume a departure
        events.append(("exit_cancelled", name))
```

**Why the asymmetry** (document it in the docstring, it will be questioned):
an arrival that never narrates can be forced to `present` because being in the scene is the
default and cannot corrupt the transcript. An exit that never narrates must be **cancelled**, not
forced, because assuming the character is gone would make the very next legitimate line of theirs a
continuity violation — the generator would flag text it had just made wrong. Cancelling is the only
direction that cannot produce an incoherent dataset.

### 3.2 Where the plan is computed

In `worker()`, **after** the character profile loop closes (the loop that fills
`character_profiles` / `scene_character_names` / `selected_profile_numbers`, ends ~line 1226) and
**before** `base_scene_prompt = current_system_prompt_for_task + character_injection` (line 1232).
It must be after that loop because `scene_character_names` is only complete at its end, and the plan
keys on those names.

```python
# --- Scene Cast (v11.11.0): who is in the scene at each assistant slot ---
scene_cast_plan = {"members": [], "min_present": 0}
scene_cast_state = {}
if scene_cast_enabled_local and len(scene_character_names) >= 2:
    total_cast_slots = current_num_turns + (1 if opening_mode_local else 0)
    scene_cast_plan = _build_scene_cast_plan(
        scene_character_names, total_cast_slots, scene_cast_config_local)
    scene_cast_state = _cast_state_from_plan(scene_cast_plan)
    log_message(f"Thread {thread_id}: Scene cast plan for task {task_id}: "
                 f"{[(m['number'], m['name'], m['enter_slot'], m['exit_slot']) for m in scene_cast_plan['members']]}",
                 "DEBUG")
```

Note: `_build_scene_cast_plan` receives **names in scene order**, and maps them to
`selected_profile_numbers[i]` so `CHARACTER N` references stay correct. This works because the
profile loop appends to `scene_character_names` and `selected_profile_numbers` in lockstep.

### 3.3 Wiring the note into `slot_notes()` (line ~1350)

`slot_notes(slot_no)` currently returns `(system note, depth0 note)` built from the persona
breakthrough block and the emotional-state plans. Add the cast note as a third contribution,
placed **after** the emotion note so the last thing the model reads is who may speak:

```python
def slot_notes(slot_no):
    notes = {"system": "", "depth0": ""}
    if _persona_visible_at(slot_no) and persona_breakthrough_block:
        notes[persona_placement] += persona_breakthrough_block
    for who, slot_state, plan_stages in _emotional_slot_states(slot_no):
        if slot_state:
            notes[emotion_placement] += _emotional_state_injection(slot_state, plan_stages, who=who)
    # v11.11.0: scene cast note -- who is in the scene for THIS reply
    if scene_cast_plan["members"]:
        notes[scene_cast_placement] += _cast_presence_note(
            scene_cast_plan, scene_cast_state, slot_no, cast_guidance_rotation)
    return notes["system"], notes["depth0"]
```

`scene_cast_placement = note_placement(scene_cast_config_local.get("placement")) or
DEFAULT_NOTE_PLACEMENT` — computed once next to `persona_placement`/`emotion_placement`
(line ~1341). Reusing `note_placement()` keeps the placement contract and the
`placement_cache_warning()` accounting unchanged (the cast note is one more block whose
`system` placement changes between replies; it must be included in the warning estimate — see §3.8).

### 3.4 The note text (`_cast_presence_note`)

The note has to do three jobs in a few lines, and must be **byte-identical between slots
where nothing changes** so a `system` placement does not thrash the prefix cache:

```
SCENE CAST (reply 3 of 5): PRESENT: Melody, Cole. ABSENT: Ines.
Ines is not in the scene: do not give Ines dialogue, actions, thoughts or feelings.
```

plus, on a transition slot only:

```
Ines ARRIVES in this reply. Bring Ines into the scene with a natural entrance -- <guidance>.
```

or

```
Cole LEAVES the scene in this reply. Have Cole exit naturally -- <guidance>. After this
reply Cole is gone: do not write Cole again.
```

plus, on a cut-away slot only (when `allow_cutaway`):

```
CUT-AWAY: you may show a brief moment elsewhere while the user is not present, but the
reply must rejoin the main thread before it ends. <cutaway_note or default>
```

When no character is `absent` and no transition is pending, emit only the first line —
so a scene with a static cast produces a stable note and the cache is preserved.

### 3.5 The continuity check in `generate_answer_with_retries()` (line 3729)

Add two keyword-only params, appended to the existing tail (which is already
keyword-only and documented as such — the same pattern v11.7 used):

```python
                                 non_latin_enabled_local=False,
                                 non_latin_fixes_local=(),
                                 scene_continuity_enabled_local=False,
                                 scene_continuity_fixes_local=(),
                                 scene_continuity_max_rounds=SCENE_CONTINUITY_MAX_ROUNDS,
                                 cast_blocked_names_local=(),
                                 cast_expected_names_local=()):
```

`cast_blocked_names_local` is the tuple of names currently `absent` or `exited` — the
characters who may **not** speak. `cast_expected_names_local` is the tuple that MUST
appear this reply (an `arriving` or `exiting` character on their transition turn); used only
to decide whether the beat landed, never to regenerate.

Placement in the retry ladder — **after** the OOC check, **before** Regex Watch:

```
non-Latin -> refusal -> user speaking -> slop -> anti-slop -> OOC -> SCENE CONTINUITY -> Regex Watch -> missing quotations
```

Rationale: continuity is a character-level wrongness like OOC (the text is in-character but
the character is not in the scene), so it sits beside OOC; Regex Watch and the quotation retry
are shape/format passes that must run on text that is about to be kept, so continuity must come
first — the same argument the non-Latin check's docstring already makes for running first.

The check itself, modelled on the OOC branch:

```python
if scene_continuity_enabled_local and cast_blocked_names_local:
    for round_no in range(scene_continuity_max_rounds):
        violation, findings = detection.check_scene_continuity(
            current_answer, list(cast_blocked_names_local), list(cast_expected_names_local))
        if not violation:
            break
        if round_no >= scene_continuity_max_rounds - 1:
            # Out of fixes: keep the best-effort answer, never drop the turn
            break
        fix = (scene_continuity_fixes_local[round_no % len(scene_continuity_fixes_local)]
               if scene_continuity_fixes_local else generation.DEFAULT_SCENE_CONTINUITY_FIX)
        current_system_prompt_iter = current_system_prompt_iter + "\n\n" + fix
        # regenerate via the same slot, exactly like the OOC branch
```

Because the fix is appended to the system prompt it invalidates the prefix cache for that
retry only — identical to every other retry check in this file, so no new behaviour class.

### 3.6 Advancing the state after an accepted answer

After `assistant_answer` is accepted and **before** it is appended to
`current_llm_conversation_context` (line ~1840, next to the existing
`conversation_history_for_output.append(...)` block):

```python
if scene_cast_plan["members"] and assistant_answer:
    scene_cast_state, cast_events = _advance_cast_state(
        scene_cast_plan, scene_cast_state, turn_num + 1 + greeting_offset, assistant_answer)
    for event, name in cast_events:
        log_message(f"Thread {thread_id}: Task {task_id}: cast event {event} ({name})", "DEBUG")
    app_state.record_cast_events(task_id, cast_events)
```

**Duplication mode:** the cast must advance from the **primary** API's answer only, so the
duplicated rows stay comparable. In the duplication branch the primary answer is already isolated
(`primary_api_answer_for_conv_flow`); advance from that, not from each `dup_api_idx`.

### 3.7 Blocked/expected name sets for the next turn

Before each turn's `generate_answer_with_retries(...)` call (both the duplication call at
line ~1548 and the non-duplication call at line ~1725), compute:

```python
cast_blocked_names, cast_expected_names = _cast_names_for_slot(
    scene_cast_plan, scene_cast_state, turn_num + 1 + greeting_offset)
```

and pass them as `cast_blocked_names_local=cast_blocked_names,
cast_expected_names_local=cast_expected_names`. The transition must be applied to the state
**before** the request (a character in `arriving`/`exiting` is allowed to speak) — so
`_cast_names_for_slot` treats `arriving`/`exiting` as allowed and only `absent`/`exited` as blocked.

### 3.8 `placement_cache_warning()` (line ~560)

The signature currently takes `persona_active, emotional_states_enabled, emotional_states,
persona_placement, emotion_placement`. Extend with `cast_active=False, cast_placement=None` and
count the cast note in the same sum. Update the two call sites: `generate.start_processing`
(log + toast) and `config_editor._refresh_placement_cache_warning()`. Defaults must keep the
existing behaviour for any caller that does not pass the new args (tests).

### 3.9 `opening_speaker()` (line 218)

With a cast plan, the greeting must only introduce characters present at slot 1. Change the call
site (line ~1414) to pass the slot-1 present names:

```python
opening_speaker(_cast_names_for_slot(scene_cast_plan, scene_cast_state, 1)[2], include_names)
```

where the third element of that helper's return is the `present` name tuple. `opening_speaker()`
itself needs no change — it already handles 0/1/many names — so its unit tests stay valid.

### 3.10 Metadata (`_build_record_metadata`, line 4625) — schema bump 6 → 7

Three new flat, type-stable keys (same rule as every other key: always present, never `None`,
`-1.0`/`""` gaps):

```python
"cast_schedule": "Melody:1-4; Cole:2-; Ines:3-",   # "" when the feature is off
"cast_events": "Melody exited; Cole arrived",        # "" when nothing happened
"quality_cast_continuity": score(dims.get("cast_continuity")),  # -1.0 when off/unscored
```

- `_METADATA_SCHEMA_VERSION = 7`.
- Update `tests/test_opening_mode.py` line ~1240 (`meta_version is 6` → `7`) and the comment
  block above it describing what each version added.

### 3.11 `_score_and_store_quality` / `quality.score_conversation`

Thread `cast_events` (or a compact `cast_schedule` string) through so the Judge can see the
schedule it is judging — see §5.3. `_score_and_store_quality` (line 4707) gains
`cast_schedule=""`, `cast_events=None`; both are forwarded to `quality.enqueue_quality_scoring`.

---

## 4. `detection.py` — the hard continuity check

New section after `is_missing_quotation()` (end of file), mirroring the established `is_*()`
contract: `(bool, [(label, sentence), ...])`, a timestamp into
`issue_timestamps['scene_continuity']` on a 1-hour rolling cutoff.

### 4.1 Name attribution

```python
_SPEECH_VERBS = frozenset((...))   # says, said, whispers, asks, replies, mutters,
                                    # calls, answers, snaps, breathes, tells, ...
_ACTION_VERBS = frozenset((...))    # nods, smiles, laughs, walks, steps, turns,
                                    # shrugs, sighs, leans, reaches, stands, sits, ...

def find_attributed_speakers(text, names):
    """[(name, sentence), ...] for each quoted span attributed to one of `names`.

    Attribution = an explicit speaker prefix immediately before the quote
    (``Melody: "..."`` / ``Melody said, "..."``) or the name inside ~60 chars of
    narration before / ~40 after the span, with a speech verb nearby.
    """

def find_character_actions(text, names):
    """[(name, sentence), ...] for a blocked name in NARRATION performing an action.

    Quoted dialogue is stripped FIRST (``_strip_quoted_dialogue``), so a character
    merely being *mentioned in someone else's line* ("Where is Melody?") never flags --
    exactly the narration-only rule ``is_out_of_character`` already uses.
    """
```

`find_attributed_speakers` deliberately duplicates ~30 lines of
`quality._spoken_lines_by_character` rather than importing it, because `quality.py`
imports `app_state` and is the heavier module; `detection.py` stays a leaf. Note this
duplication in the docstring so it is not "fixed" later into a circular import.

### 4.2 The check

```python
def check_scene_continuity(text, blocked_names, expected_names=()):
    """(violation, [(kind, name, sentence), ...]).

    kind is one of:
      'exited_speaks'   -- a blocked name has an attributed quoted line (HARD)
      'exited_acts'     -- a blocked name performs a narration action (HARD)
      'expected_missing'-- a name that MUST appear this reply does not (SOFT:
                          reported only, never regenerates; the state machine
                          defers/cancels, it never forces a rewrite for this)
    """
```

Only `exited_speaks` / `exited_acts` trigger a regeneration. `expected_missing` is
recorded (dashboard + metadata + judge context) but never rewrites the turn — the
model failing to write a beat is handled by the deferral logic in §3.1, which is
deterministic and free.

### 4.3 `DEFAULT_SCENE_CONTINUITY_FIX`

Module scope, in the same style as `DEFAULT_NON_LATIN_FIX` (line 39 is where
`REGEX_WATCH_MAX_ROUNDS` lives — put the new default next to it):

> "A character who has left the scene cannot speak, act, be seen or be described as
> present again. Remove every line, action and reaction belonging to a character who is
> no longer in the scene, and do not bring them back unless someone explicitly
> summons them."

---

## 5. `quality.py` — the Judge LLM review

The user asked: *"You may need to make the Judge LLM review this as well, not sure how
you would implement it. I'll leave it up to you."* Answer: the Judge reviews the
**naturalness** of the cast changes, not their legality — legality is already
objective and free in `detection.py`, and paying a judge to re-derive it would only add
latency and a new drop reason. So the Judge gets an opt-in dimension.

### 5.1 New dimension constant (next to `ROLEPLAY_SCORING_DIMENSIONS`, line ~66)

```python
# --- Cast-continuity dimension (v11.11.0, opt-in via quality.cast_continuity; off by
# default). The Judge reviews what code cannot: did the entrances and exits read as
# motivated and natural, and was an exited character brought back anyway? The OBJECTIVE
# half of that question is already answered for free by
# detection.check_scene_continuity(), so this dimension deliberately scores
# NATURALNESS and motivation rather than re-listing violations.
CAST_SCORING_DIMENSIONS = {"cast_continuity": 0.10}

def is_cast_scoring_enabled(config=None):
    """Driven by ``quality.cast_continuity`` (GUI toggle)."""
```

### 5.2 `active_scoring_dimensions()` (line 207)

```python
if is_cast_scoring_enabled(config):
    dims.update(CAST_SCORING_DIMENSIONS)
```

Register `cast_continuity` in **`LLM_DIMENSIONS` only** — never in
`HEURISTIC_DIMENSIONS`. It is a judgement call with no measurable proxy, and the
`tic_density` comment already establishes the rule: a dimension with no real heuristic
must not be given a fabricated one.

### 5.3 `_score_with_llm()` (line 914) — new parameters

```python
def _score_with_llm(conversation_history, task_id, thread_id, api_slot_idx, file_name="",
                    character_profile_text="", logic_findings=None,
                    cast_schedule="", cast_events=None):
```

New prompt block, appended only when the toggle is on (same place the roleplay block is
appended, line ~1030):

```
- cast_continuity: The scene's cast changes over the conversation. The schedule below
  lists who was in the scene at each reply and when they arrived or left. Judge how
  NATURALLY those changes read: was an exit motivated and earned, or did the character
  simply vanish mid-sentence? Did an arrival have a reason to be there, or appear out of
  nowhere? Penalise a character who left and was then brought back, and penalise a
  character who was absent and spoke anyway. Do NOT penalise an exit or arrival you find
  abrupt-but-plausible; score only what a reader would call a continuity error.
  CAST SCHEDULE: <cast_schedule>
  CAST EVENTS: <cast_events>
```

Add `"cast_continuity": <0-100>` to the JSON format line and to `_judge_dims`
(the `expected_dims` list at line ~1034) so the extraction ladder treats it as a
first-class dimension.

### 5.4 `score_conversation()` (line 222)

Add `cast_schedule=""`, `cast_events=None` to the signature and forward to
`_score_with_llm(...)`. Defaults keep every existing caller and test working.

### 5.5 `enqueue_quality_scoring()` (line 2155) and the processor

Add the two parameters to the signature, the queue tuple (line ~2287 unpacks `item[:8]` —
extend to `item[:10]` and update both the tuple construction and the unpack), and the
synchronous fallback path. **This tuple-shape change is the most error-prone step**; grep
`item[:8]` and every `enqueue_quality_scoring(` call site before editing.

### 5.6 Optional per-turn judge (`quality.check_cast_continuity`, after `check_logic`)

Only when `quality.cast_continuity_per_turn` is on. Mirrors `check_logic()` exactly
(same envelope, same `_call_logic_judge` call, same `{"failed", "score", "evidence",
"rewrite_instruction", "success", "flags"}` return) with a prompt that asks only about the
current reply against the schedule. Wired into the worker beside `_per_turn_logic_gate`
(line 1765) as a second, independent gate. Off by default because it doubles judge load.

---

## 6. `generate.py` — the GUI

### 6.1 Config read in `start_processing()` (next to the emotional-states block, ~line 1530)

```python
scene_cast_config = global_config.get('prompts.scene_cast', {}) or {}
scene_cast_enabled = bool(scene_cast_config.get('enabled', False))
scene_continuity_config = global_config.get('detection.scene_continuity', {}) or {}
scene_continuity_enabled = bool(scene_continuity_config.get('enabled', True))
```

Log one line in the existing style (`log_message(f"Scene cast: enabled=..., shape=...,
min_present=...", "INFO")`) and, when the feature is on with fewer than 2 characters
configured, log a `WARNING` that it will be a no-op — the AGENTS.md style of making a
silently-inactive feature visible.

### 6.2 Worker launch — `kwargs=` ONLY

Add to the `kwargs={...}` dict (line 2033), **never** `args=`:

```python
'scene_cast_enabled_local': scene_cast_enabled,
'scene_cast_config_local': scene_cast_config,
'scene_continuity_enabled_local': scene_continuity_enabled,
'scene_continuity_fixes_local': scene_continuity_fixes,
'scene_continuity_max_rounds': scene_continuity_max_rounds,
```

`tests/test_opening_mode.py` line ~860 asserts the **exact** kwarg key set
(`passed == {...}`), so that set literal must be extended with these five names, with a
`# v11.11.0` comment, or the test fails.

### 6.3 Metric card, issue panel, graph series

Following the v11.10.0 Non-Latin recipe exactly (the AGENTS.md checklist for it):

- `app_state.scene_continuity_label = _create_ttkbs_metric(metrics_frame, "Cast Drift", "🎭")`
- Add `"Cast Drift"` to `issue_types` (line 2936) and `"scene_continuity"` to `issue_keys` (line 2937).
- Add `color_map["scene_continuity"] = "#e599f7"` (line 3200).
- `dashboard.py`: add the series to **both** `counts` dicts (lines 175 and 326), **both**
  `colors` dicts (203 and 350) and **both** `labels` dicts (206 and 353), plus the
  `_graph_last_draw` signature tuple (line 305). Missing any one of these is a `KeyError`
  or `IndexError` at run time — this is the documented trap.
- `dashboard.update_dashboard()`: feed `app_state.recent_scene_continuity_total` /
  `_per_api` into the panel, exactly like `recent_non_latin_*`.

### 6.4 `app_state.py`

```python
# --- Scene cast continuity (v11.11.0) ---
scene_continuity_count_total = 0
scene_continuity_counts_per_api = {i: 0 for i in range(6)}
recent_scene_continuity_per_api = {i: [] for i in range(6)}
recent_scene_continuity_total = []
MAX_CAST_EVENTS_LOG = 500
cast_events_log = []          # (task_id, event, name, api_slot_idx) ring buffer
cast_events_log_lock = Lock()
def record_cast_events(task_id, events, api_slot_idx=0): ...
```

Add `'scene_continuity': []` to `issue_timestamps` (line 22). Add the counters to
`generation.save_generation_state()` (line 275) and their restore in
`generate.load_generation_state()` (line ~228) **and** to
`generate.reset_all_stats_and_history()` (line 269) — the AGENTS.md rule is that a new
counter needs both.

---

## 7. `config_editor.py` — the GUI toggle

Character Engine tab, immediately **after** the per-character-emotions checkbox block
(ends line ~955) and **before** the user-emotional-states block (~line 957). A new
`LabelFrame` — this is the feature's visible home, so it gets a titled frame rather than a
bare checkbox.

```python
# --- Scene Cast (v11.11.0): characters joining and leaving the scene ---
scene_cast_lf = ttk.LabelFrame(self.character_engine_content_frame,
                               text="Scene Cast — Characters Join and Leave")
scene_cast_lf.grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="ew")
self.scene_cast_lf = scene_cast_lf
character_engine_row_idx += 1
```

Contents, all inside `scene_cast_lf` (which owns a real width, so `wrap_label` is safe
against it — never against a frame sized by its own label):

1. `self.enable_scene_cast_var_editor = tk.BooleanVar()`,
   `wrapped_checkbutton(scene_cast_lf, text="Enable Scene Cast (multi-character scenes:
   characters may leave and join)", variable=..., command=self._toggle_scene_cast_fields)`.
2. Help label, `wraplength=1` + `wrap_label(..., scene_cast_lf, ...)`:
   *"Applies only when 2 or more characters are selected. Off by default."*
3. `self.scene_cast_shape_var_editor` — `ttk.Combobox(values=("minimal", "churn",
   "none"), state="readonly", width=10)`.
4. `self.scene_cast_min_present_var_editor` — `ttk.Spinbox(from_=1, to=10, width=6,
   style=self.spinbox_style)` **with `style=self.spinbox_style`** (the theme makes a bare
   spinbox white-on-white — the documented trap).
5. `self.scene_cast_max_churn_var_editor` — same spinbox treatment.
6. `self.scene_cast_max_deferrals_var_editor` — same.
7. `self.scene_cast_placement_var_editor` — reuse `self._add_placement_combobox(row, "Cast
   Note Placement:", var)`, which already wires the cache warning refresh.
8. `self.scene_cast_allow_cutaway_var_editor` — `wrapped_checkbutton(...)`.
9. `self.scene_cast_exit_guidance_text` / `self.scene_cast_arrival_guidance_text` —
   `scrolledtext.ScrolledText(height=4, width=1)`.
10. `self.scene_cast_cutaway_note_text` — `ScrolledText(height=3)`.

Then a **second** LabelFrame "Scene Continuity Check" in the Detection tab's `col2_frame`,
next to the OOC frame, following the v11.10.0 non-Latin recipe exactly:
`scene_continuity_enabled_var_editor` + `_toggle_scene_continuity_fields()`,
`scene_continuity_fixes_text` (**created enabled, only disabled by the toggle** — this Tk
build drops `delete`/`insert` on a disabled Text widget silently, the documented pitfall),
`scene_continuity_max_rounds_spinbox` (`from_=1, to=5`, `style=self.spinbox_style`).

And a third toggle in the Quality Scoring section, next to the roleplay-dimensions
checkbox: `quality_cast_continuity_var` + `quality_cast_continuity_per_turn_var`.

### Save (`save_config_handler`, line ~1924 / ~1958)

```python
'scene_cast': {
    'enabled': self.enable_scene_cast_var_editor.get(),
    'shape': self.scene_cast_shape_var_editor.get(),
    'min_present': int(self.scene_cast_min_present_var_editor.get()),
    'max_churn': int(self.scene_cast_max_churn_var_editor.get()),
    'max_deferrals': int(self.scene_cast_max_deferrals_var_editor.get()),
    'placement': self.scene_cast_placement_var_editor.get(),
    'allow_cutaway': self.scene_cast_allow_cutaway_var_editor.get(),
    'exit_guidance': [l.strip() for l in self.scene_cast_exit_guidance_text.get("1.0", tk.END).split('\n') if l.strip()],
    'arrival_guidance': [...],
    'cutaway_note': self.scene_cast_cutaway_note_text.get("1.0", "end-1c"),
},
```

plus `detection.scene_continuity` and the two `quality.cast_continuity*` booleans.
Guidance lists and the cut-away note are saved **verbatim**, like the opening-mode
prompts and the regex patterns — `sanitize_input()` strips backslashes and quotes and would
corrupt authored text.

### Load (`load_config_handler`, line ~2631)

Set every var from `prompts_config.get('scene_cast', {})`, clamp `shape` to
`SCENE_CAST_SHAPES` (unknown → `"minimal"`), clamp `placement` via `note_placement(...)`,
and **call `self._toggle_scene_cast_fields()` at the end of the load path** so a config saved
with the toggle on reopens editable — the exact rule documented for the Judge priority field.

### Validate (`validate_config_handler`, line ~2887)

```python
assert 1 <= scene_cast_min_present <= 10, "Scene Cast min_present must be between 1 and 10"
assert 0 <= scene_cast_max_churn <= 10, "Scene Cast max_churn must be between 0 and 10"
assert 0 <= scene_cast_max_deferrals <= 5, "Scene Cast max_deferrals must be between 0 and 5"
assert scene_cast_shape in SCENE_CAST_SHAPES, ...
assert scene_cast_max_rounds in range(1, 6), ...
# touch the text fields so an empty-but-present widget is still validated
get_text_as_list(self.scene_cast_exit_guidance_text)
get_text_as_list(self.scene_cast_arrival_guidance_text)
```

### `_toggle_scene_cast_fields()`

Mirror `_toggle_bad_words_fields()` (line ~2304): flip each field between `normal`
and `disabled`. **Set `state='normal'` before any write** (the documented Tk trap).

### `_refresh_placement_cache_warning()` (line ~2290)

Extend the `placement_cache_warning(...)` call with
`cast_active=self.enable_scene_cast_var_editor.get(),
cast_placement=self.scene_cast_placement_var_editor.get()`.

---

## 8. Tests

Every new test must be headless (`python -B tests/test_x.py`), Python 3.9-clean (no
`match`, no `X | Y` unions), and must not need a model, network or server.

### 8.1 NEW `tests/test_scene_cast.py` (headless)

- **Schedule invariants** (`_build_scene_cast_plan`): never schedules an exit when
  `cast_size < min_present + 1`; never leaves fewer than `min_present` present at any slot
  (simulate every slot and assert); `shape: "none"` yields an empty plan; `shape: "minimal"`
  yields at most one join and one leave; every member has a valid `enter_slot` in
  `0..total_slots` and `exit_slot` in `1..total_slots` with `exit_slot > enter_slot` when set;
  the plan is deterministic for a fixed `random.seed` and covers every selected character
  exactly once.
- **State machine** (`_advance_cast_state`): an `exiting` character whose name is absent from
  the answer stays `exiting` and defers, then **cancels back to `present`** after
  `max_deferrals` (assert the asymmetry explicitly); an `arriving` character whose name is
  absent is **forced to `present`**; a narrated exit becomes `exited`; a narrated arrival
  becomes `present`; no character ever leaves the legal state set.
- **Note text** (`_cast_presence_note`): a static slot produces a note with no transition
  sentence (cache stability — assert two consecutive static slots produce byte-identical notes);
  an exit slot names the character and the guidance; `allow_cutaway` adds the cut-away block;
  the note never contains a `None`.
- **Detector** (`detection.check_scene_continuity`): an exited character with a speaker
  prefix flags; an exited character with a name-adjacent quote flags; a name inside *someone
  else's* quoted line ("Where is Melody?") does **not** flag; a name in narration with a
  speech verb flags; an exited character merely mentioned with no verb does not flag; accented
  names and names containing regex metacharacters do not raise; `expected_names` produces
  `expected_missing` without ever being treated as a hard violation.
- **Worker integration** (reuse the `run_worker` / `worker_kwargs` harness from
  `tests/test_opening_mode.py`, with `requests.post` stubbed exactly like
  `tests/test_quality_judge_policy.py`): a multi-character task with the feature on sends a
  cast note in the depth-0 note; the system prompt is **byte-identical across replies**
  (prefix-cache guarantee); an exited character who speaks causes exactly one regeneration; the
  state advances only from the primary answer in duplication mode; metadata carries
  `cast_schedule` / `cast_events` / `quality_cast_continuity`.

### 8.2 Extend existing tests

- `tests/test_opening_mode.py`: the `passed == {...}` kwarg set (line ~860) gains the five
  new names; `meta_version is 6` → `7` (line ~1240); add a case that the greeting speaker
  is drawn from slot-1 present names only.
- `tests/test_worker_launch.py`: no change needed (it binds the signature) but must still pass —
  run it to prove the `kwargs=` placement is legal.
- `tests/test_quality_judge_settings_ui.py`: add the two new quality toggles to the save/load
  round trip if it enumerates them.
- `tests/test_editor_readability_ui.py` (**needs `xvfb-run` and the 'superhero' theme**): the
  new spinboxes and wrapped labels are exactly what this test exists to catch — the Scene Cast
  frame must not introduce a "skyscraper" label or a white-on-white spinbox.
- `tests/test_dataset_compatibility.py`: must still pass unchanged — the feature is off by
  default, so both runs must send identical requests. This is the guard that proves the default
  path is byte-identical to v11.10.0.

### 8.3 Full run (per AGENTS.md)

```sh
for t in tests/test_*.py; do python -B "$t" || echo "FAILED: $t"; done
xvfb-run -a python -B tests/test_editor_readability_ui.py
python tools/check_gui_layout.py --check --config-editor
```

---

## 9. Documentation

- `AGENTS.md` — required by its own conventions:
  - `config/config.yml` entry: the three new blocks.
  - `detection.py` entry: `check_scene_continuity` / `find_attributed_speakers` /
    `find_character_actions` in the Key line and an "Edit for:" bullet.
  - `generation.py` entry: the helpers, the retry-ladder position, the state machine
    asymmetry rule (so it is not "simplified" later into forcing exits).
  - `quality.py` entry: `CAST_SCORING_DIMENSIONS` / `is_cast_scoring_enabled`, the
    "LLM-only, never heuristic" rule, and the `item[:10]` tuple change.
  - `generate.py` entry: the kwarg names and the "kwargs= only" rule.
  - `config_editor.py` entry: the three panels, the disabled-Text trap, the spinbox
    style, the toggle-on-load rule.
  - `dashboard.py` entry: the "four dicts + signature list" rule.
  - `app_state.py` entry: the counters and the `record_cast_events` log.
  - **Common Tasks Quick Reference** — one row: *"Add/modify scene cast (join/leave)"*
    listing every file in dependency order.
- `README.md`: a `### Scene Cast — Characters Join and Leave (v11.11.0)` section under
  *Multi-Character Conversation Mode*, a TOC entry, a Features bullet, the config YAML
  block, and a v11.11.0 changelog entry at the top.

---

## 10. Implementation order (each step leaves the repo green)

| # | Step | Verify |
|---|---|---|
| 1 | `detection.py`: attribution helpers + `check_scene_continuity` + `DEFAULT_SCENE_CONTINUITY_FIX` | `python -B tests/test_scene_cast.py` (detector cases only) |
| 2 | `generation.py`: module-scope plan/state helpers, pure and unused | same file, schedule + state-machine cases |
| 3 | `generation.py`: `_cast_presence_note`, wire into `slot_notes()`; feature still off by default | `tests/test_opening_mode.py`, `tests/test_persona_arc.py` |
| 4 | `generation.py`: retry-ladder branch + worker kwargs + `_advance_cast_state` + metadata (bump to 7) | `tests/test_opening_mode.py`, `tests/test_dataset_compatibility.py` |
| 5 | `app_state.py` + `dashboard.py` (four dicts + signature + metric card) | `tools/check_gui_layout.py --check` |
| 6 | `config_editor.py` (three panels + save/load/validate/toggle) | `xvfb-run -a python -B tests/test_editor_readability_ui.py` |
| 7 | `quality.py` (dimension, judge prompt, tuple plumbing) | `tests/test_quality_roleplay.py`, `tests/test_quality_json_robustness.py` |
| 8 | `config/config.yml` defaults (all off/inert) | `tests/test_dataset_compatibility.py` |
| 9 | Docs (`AGENTS.md`, `README.md`) | full suite |

Steps 1-2 are pure and testable before anything is wired. Step 8 lands last so the
shipped config never enables a half-implemented feature.

---

## 11. Acceptance criteria

1. With the toggle **off**, every request is byte-identical to v11.10.0 — proven by
   `tests/test_dataset_compatibility.py`.
2. With the toggle **on** and 2+ characters, at least one join and one leave occur across
   a conversation long enough to hold them, and **never fewer than `min_present`** characters
   are present at any slot.
3. An exited character who speaks is detected and the reply regenerated at most
   `max_rounds` times, then kept (the turn is never dropped).
4. A character whose exit was never narrated is **cancelled back to present**, never assumed
   gone; a character whose arrival was never narrated is forced present. The transcript never
   contains a character who silently vanished or silently appeared.
5. The system prompt is identical between replies when `placement: depth0` and no note
   changes — the prefix cache is preserved.
6. The Judge `cast_continuity` dimension appears only when `quality.cast_continuity` is on,
   carries `-1.0` otherwise, and never fabricates a heuristic score.
7. `meta_version` is 7 and every record has the same keys with the same types.

---

## 12. Risks and open questions

- **The model may not narrate the beat.** Mitigated by deferral + cancel (§3.1); the
  transcript stays coherent, but a cancelled exit means the feature under-delivers on that
  task. `cast_events` in the metadata makes the rate measurable, and a run log line
  reports cancelled/forced events so the rate is visible rather than silent.
- **Small `num_turns` (default 4) leaves little room.** With `min_present: 2` a 2-character
  scene can never change cast. The plan logs a `WARNING` and no-ops; this is documented in
  the editor help text so it does not read as a bug.
- **Duplication mode ordering.** The state must advance from the primary answer only, or the
  duplicated rows diverge. Flagged in §3.6.
- **Judge cost.** `cast_continuity` is LLM-only and adds one dimension to an existing call
  (no extra request); `cast_continuity_per_turn` adds a request per turn and stays off by default.
- **Cut-away scope.** Implemented as a prompt-level opt-in only (§2.1 `allow_cutaway`), not as
  a new generation path. If it proves too hard to steer, it can ship disabled in v11.11.0 without
  blocking the join/leave feature, which is the part the user actually asked for.
- **Naming.** `scene_cast` (scheduling/steering) vs `scene_continuity` (the check) vs
  `cast_continuity` (the judge dimension) — three similar names for three different layers.
  Keep them distinct; do not merge, or the config block and the detection block will collide.
