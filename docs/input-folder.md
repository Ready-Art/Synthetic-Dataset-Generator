# The `input/` folder

The `input/` folder is where the generator looks for its source text. The application creates the
folder on first run (`generate.py`, `app_state.INPUT_DIR`); it is not in `.gitignore`, so anything you
put there is a candidate for being committed. **Only put text there that you have the right to use.**

There are two ways to feed a run, and they are mutually exclusive:

| Mode | How it is switched on | What one task is |
|------|----------------------|------------------|
| **Chunk mode** (default) | `prompts.use_questions_file: false` and at least one `.txt` file in `input/` | One random passage cut out of one file |
| **Questions file** | `prompts.use_questions_file: true` | One line of `input/questions.txt` |

**Opening mode** (`prompts.opening_mode: true`) reads the folder the same way chunk mode does, but it
also runs with an empty folder, and it refuses to start with the questions file or duplication mode on.

---

## 1. Chunk mode: `.txt` files

### Which files are picked up

`generate.py` builds the file list with

```python
input_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.txt') and f != os.path.basename(QUESTIONS_FILE_PATH)]
```

so the rules are exact and unforgiving:

- **The extension must be lowercase `.txt`.** `chapter.TXT` and `chapter.text` are silently ignored.
- **`input/questions.txt` is always excluded**, even when the questions file is switched off. Do not
  name a source file `questions.txt`.
- **The folder is not searched recursively.** `input/novels/chapter1.txt` is never read; only files
  directly inside `input/`.
- **Only `.txt` counts.** `.md`, `.rtf`, `.docx`, `.epub` and `.pdf` are ignored. Convert to plain
  UTF-8 text first.
- A *directory* whose name ends in `.txt` matches the filter and then fails to read. Avoid it.
- An empty folder in chunk mode stops the run with "No input .txt files found." rather than
  generating anything.

### Encoding

Files are opened with `encoding='utf-8'`. Save as **UTF-8 without a byte-order mark**.

- A file saved as Windows-1252 or Latin-1 raises `UnicodeDecodeError` and the whole file is skipped.
- A UTF-8 BOM is not stripped. It survives into the first chunk as a stray `\ufeff` character and is
  sent to the model. Re-save without a BOM if your editor adds one.

### Characters that are removed on load

Every file is passed through `config_loader.sanitize_input()` when it is read, which deletes
`<`, `>`, `"`, `'` and `\` outright — they are removed, not escaped or replaced:

```text
'"Where are we going?" she asked. It's O'Brien's.'   ->   'Where are we going? she asked. Its OBriens.'
```

This is worth planning around:

- Straight double and single quotes are gone, so quoted dialogue in your source arrives unquoted.
- Apostrophes are gone, so `it's`, `don't` and `O'Brien` become `its`, `dont`, `OBrien`.
- **Curly quotes and dashes survive** (`“ ” ‘ ’ —`) because they are not in the strip set. If you want
  punctuation preserved, write the source with typographic quotes.
- Angle brackets and backslashes are gone, so code snippets, markup and Windows paths do not survive.

Also note the length guard: a file longer than `generation.sanitize_input_max_length` (default
100000000) raises `ValueError` and is skipped.

### Minimum size: a file must be longer than `context_size`

This is the rule that surprises people. For each attempt the generator computes

```python
max_valid_start_index = max(0, file_content_len - context_size)
if max_valid_start_index <= 0:
    continue    # file is too small, skip it
```

so a file of exactly `context_size` characters yields **nothing at all** — the check is `<= 0`, not
`< 0`. Measured with the defaults (`context_size: 3000`):

| File length | Chunks queued |
|-------------|---------------|
| 2999 | 0 |
| 3000 | 0 |
| 3001 | 1 distinct start (offset 1) |
| 4001 | 3 distinct starts |

A folder of short files (a few paragraphs each) therefore produces an empty run and the log line
"File ... is too small for the requested context size. Skipping." Keep source files comfortably
larger than `context_size` — a few thousand words each is plenty.

### How a chunk is cut

For each queued task the generator picks a file at random, reads the whole file, and picks a random
character offset in `[0, len - context_size]`:

1. **Subject** = `subject_size` characters (default 1000) starting at that offset.
2. **Context** = `context_size` characters (default 3000) centred on the subject: half the remaining
   space before it, half after, then nudged at the file's edges so the window is as close to
   `context_size` as possible.

Consequences to design around:

- **Cuts are at arbitrary character offsets, not sentence, paragraph or word boundaries.** A chunk can
  begin and end mid-word. The model is tolerant of this, but the first and last fragment of every
  chunk is ragged.
- The **subject is contained inside the context**, so the two fields overlap.
- `context_size` smaller than `subject_size` is corrected upward to `subject_size`, with a warning.
- The start offset is bounded by `len - context_size`, and `context_size` is at least `subject_size`, so
  the subject is **always exactly `subject_size` characters** and never truncated by the file's end. A
  subject that is entirely whitespace is skipped.

### How many tasks you get

`generation.num_random_chunks` (default 12000) is the **target number of tasks per run**. The loop
stops at that target or after `num_random_chunks * 5` attempts, whichever comes first. Because the
starting offset is random, the same offset can be drawn twice; duplicates within one run are not
filtered, so a small file asked for many tasks produces repeated or near-repeated passages.

Each task's ID is `"<file name>_chunk_at_<offset>"`, so:

- The IDs are stable across runs, which is what lets **Resume** skip finished work.
- **Editing or re-saving a source file shifts every offset.** Chunks you already completed no longer
  line up with the same text, and resume cannot tell.
- Changing `subject_size`, `context_size`, `num_turns`, `use_questions_file`, opening mode or
  duplication mode between runs triggers the configuration-incompatibility warning on resume.
- Every file is re-read in full on every attempt. A corpus of very large files is re-read constantly;
  split it into moderate files instead.

---

## 2. The questions file: `input/questions.txt`

Set `prompts.use_questions_file: true` (Config Editor → **Prompts** → *Use 'questions.txt' for
questions*). Subject/context chunking is switched off entirely and each line becomes one task.

Format rules, from `read_txt()`:

- **One question per line.** Each non-blank line is one task.
- **Blank and whitespace-only lines are dropped.** They are not tasks and not errors.
- Each line is `.strip()`ed, so leading and trailing spaces never reach the model.
- **Each line is passed through `sanitize_input()` too**, so quotes, apostrophes, `<`, `>` and `\` are
  removed here as well.
- A question must fit on **one physical line**. Wrapping a long question across two lines silently
  turns it into two separate tasks; there is no continuation marker.
- File order is preserved: line *n* becomes task `q_<n>`.
- The file must be UTF-8. A missing or empty file stops the run with "Questions file is empty or not
  found."

---

## 3. Dataset variables in the questions file (v11.9.0)

A questions-file line is **authored text**, not source material, so it can carry the same
dataset placeholders the Datasets tab maps (see `docs/dataset-bindings.md`). Two independent
forms are available, and either or both can be used:

| Form | Where it is configured | What it does |
|------|------------------------|--------------|
| **Placeholder in the line** | the line itself | `{{ds:alias}}` is replaced with that task's value for the alias |
| **Appended alias** | `prompts.questions_dataset.append_alias` | one alias value is appended to **every** line, after `append_separator` |

```yaml
prompts:
  use_questions_file: true
  questions_dataset:
    append_alias: chapter_body      # '' = placeholders only, append nothing
    append_separator: "\n\n"       # real characters: two newlines
```

```text
input/questions.txt:

Ask {{ds:name}} what she saw at the harbour.
Where did {{ds:name}} go after the bell?
```

With `append_alias: chapter_body`, the first line becomes

```text
Ask Ada what she saw at the harbour.

<that task's chapter_body value>
```

Rules worth knowing:

- **Each task gets its own row.** The values come from the same frozen binding the run already
  uses for the authored prompts, so the placeholder and the appended value always belong to the
  same dataset row. The row advances once per new task, in worker-arrival order.
- **Expansion is single-pass.** A dataset cell that happens to contain `{{ds:other}}` or
  `{context}` is inserted literally and never scanned again. A `{context}` you write into a line is
  also left alone, because questions-file lines carry no native placeholders.
- **An unmapped alias is an error**, reported against the task instead of reaching the model as
  literal text. `{{ ds:name }}` (with spaces) is a malformed placeholder and is also an error.
- **An empty appended value appends nothing**, so a row with a blank cell never leaves a dangling
  separator. An empty appended value on an empty line produces an empty question, which the
  generator skips the way it skips any blank line.
- **`append_alias` needs the questions file on.** The Configuration Editor refuses to save it while
  `use_questions_file` is off, and Start logs a warning and ignores it if the file was hand-edited.
- **Chunk-mode source `.txt` files are NOT rendered.** A chunk is cut at an arbitrary character
  offset (see "How a chunk is cut" above), so a placeholder there could be sliced in half and reach
  the model as literal garbage. Put dataset variables in the questions file or in an authored prompt.
- **`sanitize_input()` still runs first**, as it does for every questions-file line, so `{` and `}`
  survive but `"`, `'`, `<`, `>` and `\` do not. Write the placeholder without quotes.
- **A dataset-free run is unchanged.** With no enabled sources the line is used exactly as read and
  `append_alias` appends nothing.

---

## 4. Examples

All sample text below is written for this guide. Use it freely, or replace it with your own.

### `input/harbour.txt` — an original source file for chunk mode

```text
The harbour bell rang twice at dusk, and the gulls went quiet. Mira counted the
boats the way she counted everything: the two trawlers with their lamps already
lit, the flat barge that had been moored at the east wall since spring, and the
little blue dinghy that belonged to nobody she could name.

She had come down to the water because the shop was too quiet. The clocks she
had spent the afternoon repairing ticked in different rhythms, and the sound
had begun to feel like an argument she was not part of. Outside, the wind came
off the sea with salt in it and the smell of rope.

The lamp in the harbourmaster's window was on. That was unusual for a Tuesday.
Mira stood at the edge of the quay with her hands in her pockets and watched
the light move behind the glass, once, twice, and then settle.

"Late for you," a voice said from the shadows by the crane.

She did not turn around. "It usually is."

The speaker stepped out far enough to be lit: a coat too heavy for the season,
a hat that had seen better decades, and a clipboard held against the chest like
a shield. He tapped it twice with one finger.

"I need someone who can read a logbook," he said. "Someone who won't ask why."

Mira looked at the clipboard and then at the water. A logbook was a small thing
to be careful about, and he was being very careful. The tide was going out. Some
way past the breakwater a single light showed and vanished and showed again, the
way a lamp does when someone is carrying it along a deck.

"What kind of logbook," she said.

"The kind that gets written in twice." He held the clipboard out, then thought
better of it and pulled it back against his chest. "Not here. Not standing in
the open."

She had known men like him before. They came down to the harbour with a story
already folded up small in their pocket, and they unfolded it only as far as
they needed to. Usually the story was about a boat. Sometimes it was about a
debt. Once it had been about a body, and that had taken the constable three
weeks to sort out.

"Your lamp's on," she said. "The one in the window. It's Tuesday."

"It is."

"You don't light it on a Tuesday."

"No." He looked up at the window as though he had not noticed it before, and
something in the way he did it told her he had noticed it a great deal. "I
suppose I don't."

The wind came again, harder, and the barge at the east wall pulled at its
mooring with a long low sound. Mira thought about the shop, and the clocks, and
the way the afternoon had gone quiet around her like a room she had walked into
by mistake. She thought about the ledger on the counter that she had not
balanced since spring.

"Come up to the shop," she said. "I'll put the kettle on. You can tell me about
the logbook, and I'll decide whether I want to hear the rest of it."

He followed her along the quay without saying anything, and his boots made a
different sound on the stones than hers did. At the door of the shop he stopped
and looked back once at the harbour, at the lamp in the window, at the single
light past the breakwater that was still there and still moving.

"One thing," he said. "If I leave the clipboard with you, you don't open it
tonight. Tomorrow is soon enough."

Mira took the kettle off the shelf and set it on the stove, and did not answer
him straight away. The clocks behind her ticked, all of them out of step, and
the shop smelled of brass and lamp oil and the sea.

"Tomorrow," she said, "is a long way off."
```

That text is 3461 characters, so it clears the 3000-character minimum with room to spare and chunk
mode will queue tasks from it. Save it as UTF-8 without a BOM. Notice that it is written to be read in
fragments: no passage depends on a line break to make sense, because the cuts land at arbitrary
character offsets (see "How a chunk is cut" above).

### `input/questions.txt` — the questions-file format

```text
Where does the ferry go after the last bell?
Why is the harbourmaster's lamp still burning on a Tuesday?
Ask the clockmaker what she found inside the stopped watch.
"You were down at the water again," she says. "What did you see?"
```

Read back, those four lines become four tasks. Note what the sanitizer does to the fourth one — it
arrives as `You were down at the water again, she says. What did you see?`, with the quotes gone.

### Checking a folder before a run

The rules above are cheap to check by hand, and much cheaper than discovering them after a 12000-task
run has queued nothing. From the repository root:

```sh
python3 - <<'PY'
import os
CONTEXT, MAX = 3000, 100000000
for name in sorted(os.listdir('input')):
    path = os.path.join('input', name)
    if os.path.isdir(path):
        print('warn', name, '- directory'); continue
    if not name.endswith('.txt'):
        print('warn', name, '- not lowercase .txt'); continue
    if name == 'questions.txt':
        continue
    try:
        text = open(path, encoding='utf-8').read()
    except UnicodeDecodeError:
        print('warn', name, '- not UTF-8'); continue
    if text.startswith('\ufeff'):
        print('warn', name, '- has a BOM')
    print('ok  ' if len(text) > CONTEXT else 'warn', name, len(text), 'chars')
PY
```

Every line should read `ok`. A `warn` naming a length at or below 3000 means that file cannot produce
a single chunk.

---

## 5. Troubleshooting

| Symptom in the log | Cause | Fix |
|---|---|---|
| `No input .txt files found for chunking.` | Folder empty, or files use another extension or uppercase `.TXT` | Rename to lowercase `.txt`; put them directly in `input/` |
| `File X is too small for the requested context size. Skipping.` | File length ≤ `context_size` | Use a longer file, or lower `context_size` |
| `Attempted to queue N random tasks. Successfully queued 0 new unique tasks.` | Every candidate chunk was rejected — usually all files are too small | Same as above |
| `questions.txt enabled but empty/not found.` | File missing, or only blank lines | Add at least one non-blank line, or switch the mode off |
| `Error reading input/questions.txt: ...` | Not UTF-8 | Re-save as UTF-8 |
| `UnicodeDecodeError` on a source file | Not UTF-8 | Re-save as UTF-8 |
| Dialogue and apostrophes are missing from generated text | `sanitize_input()` stripped `"` and `'` | Expected; use curly quotes if you need them to survive |
| A run queues far fewer tasks than `num_random_chunks` | Small files, or repeated offsets already completed | Add larger files; check the file lengths |
| `No dataset column is mapped to alias 'x'` in the log | A `{{ds:x}}` in a questions-file line has no alias on an enabled source | Map the column in the Datasets tab, or remove the placeholder |
| The appended value never appears in the generated question | `append_alias` is empty, the questions file is off, or the row's cell is blank | Check the Config Editor's Questions File Dataset Variables panel |
| A `{{ds:...}}` placeholder reached the model as literal text | The line came from a chunk-mode source `.txt`, which is never rendered | Put the placeholder in `input/questions.txt` instead |

---

## 6. Configuration reference

All of these live in `config/config.yml` and in the Configuration Editor.

| Key | Default | Effect on `input/` |
|-----|---------|--------------------|
| `prompts.use_questions_file` | `false` | Read `input/questions.txt` instead of chunking |
| `prompts.questions_dataset.append_alias` | `''` | One alias whose value is appended to every questions-file line (needs the questions file) |
| `prompts.questions_dataset.append_separator` | `"\n\n"` | Text placed between the line and the appended value |
| `generation.num_random_chunks` | `12000` | Target tasks per chunk-mode run |
| `generation.subject_size` | `1000` | Characters in the subject passage |
| `generation.context_size` | `3000` | Total characters in the context window; also the minimum usable file length |
| `generation.sanitize_input_max_length` | `100000000` | A file longer than this raises and is skipped |
| `prompts.opening_mode` | `false` | Uses chunks as scene material; runs with no files at all |
| `prompts.question` | — | The prompt that receives `{subject}` and `{context}` |
| `prompts.greeting_context` | — | In opening mode, receives the chunk in `{chunk}` |

`input/` is read only at the moment a run starts. Adding or editing files mid-run has no effect on the
current queue — stop, edit, and start again.
