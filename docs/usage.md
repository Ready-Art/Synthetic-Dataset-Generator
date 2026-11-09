<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 🎮 Usage

## Starting the application

The GUI is a **web application**. Start it and open the URL:

```bash
python -m webapp            # then open http://127.0.0.1:8765
```

The first start prints a one-time setup token to the console; paste it at `/setup` and choose a
password. See [Web Front End](webapp.md) for the flags, the login, nginx and the screens.

`cli launch --gui` starts the same web app. The old Tk window is still reachable for one
deprecation release via `cli launch --tk` or `python generate_tk.py`, but the web app is the
supported GUI.

## Starting a Generation Run

1. Place source `.txt` files in the `input/` directory (or create `input/questions.txt` for predefined questions). Opening mode also runs with no input files. It refuses to start with the questions file or duplication mode on.
2. Configure APIs and settings on the **Config** screen
3. Click **Start Generation** in the Overview action bar
4. If a previous state file exists, you'll be prompted to resume or start fresh

## Resuming & Crash Recovery

The application automatically saves generation state to `output/generation_state.json` after each completed task. On restart:

- **Resume** — Continues from where the previous run left off, skipping already-completed tasks
- **Start Fresh** — Backs up existing output files to a timestamped ZIP archive and begins anew

The state file tracks:
- Completed task IDs
- System prompt counter
- Question history
- All statistics (refusals, slop, errors, tokens, costs, etc.)
- Per-API progress (in duplication mode)
- Quality scores and review flags
- Configuration snapshot (warns if settings changed since last run)

**Configuration Incompatibility Detection:** When resuming, the application compares critical settings (`use_questions_file`, `num_turns`, `subject_size`, `context_size`, `master_duplication_mode`, `opening_mode`) between the saved state and current config. If they differ, a warning dialog presents the differences and lets you choose whether to proceed or start fresh.

**Pause & Reload:** When you pause generation and resume, the configuration is automatically reloaded from `config.yml`, allowing you to make mid-run adjustments to rate limits and other settings without restarting.

## Duplication vs. Collaborative Mode

| Feature | Duplication Mode | Collaborative Mode |
|---------|-----------------|-------------------|
| Task distribution | Same task to all enabled APIs | Different tasks distributed across APIs |
| Output files | Per-API JSONL files | Single combined JSONL file |
| Debug logs | Per-API debug log files | Single debug log file |
| Use case | Dataset diversity from multiple models | Higher throughput |
| Progress bars | One per API | Single overall bar |
| Question/continuation gen | Always uses API Slot 1 | Uses the worker's assigned API |
| Thread allocation | Sum of threads from all enabled APIs | Sum of threads from all enabled APIs |

## Dashboard & Monitoring

The run controls (**Start Generation**, **Pause**, **Stop & Clear**) sit in the Overview
action bar — **Start Generation** is the only filled, accent-coloured button, so the primary
action is obvious at a glance. The **Config** screen is a tab, not a modal.

**Every screen fits 1440x900 and 900x600 with no page scroll.** The tabs are
keyboard-operable (arrow keys, Home, End), the selected tab survives a reload, and a
dropped connection says `reconnecting…` and re-primes itself from the server, so a long run
that outlives the browser tab is never lost.

The real-time dashboard provides:

- **Overview** — Six metric cards in one row with the rest behind a **more metrics**
disclosure, one progress row per slot, the aggregate issue panels, and the time-series graph
of issues over the last 60 minutes (10-minute bins). The database/cache status and API response
times are folded into one collapsed **Diagnostics** section, because they are only interesting
when something looks wrong
- **Review** — Flagged conversations on the left, the conversation detail on the right, with a
score footer (Coherence / Character / Prose / Overall) colour-coded by threshold, plus export,
drop and dismiss. Selecting a row never navigates away
- **APIs** — Per-slot statistics and recent issues for each generation slot, behind a slot selector
- **Quality** — Quality scoring summary, dimension averages, and a table of the latest 50 scored conversations
- **Regex Watch** — The patterns and their hits
- **Metrics** — The per-slot vLLM table and its graphs
- **Prompts** — Real-time view of prompts being sent to APIs with rich formatting
- **Queue** — Live view of pending and failed tasks with purge, retry and export
- **Config** — The config editor, covering the whole config tree
- **Datasets** — Dataset sources, aliases and column visibility
- **Profiles** — Save and load a copy of the whole config

Every table says something when it is empty rather than showing a blank box, and every
button shows a hover and keyboard-focus state. The web UI draws **no emoji at all**, so there
is no font to be missing.
- **Overview metrics** — Refusal rate, user speaking rate, slop rate, error rate, token count, estimated cost
- **Quality metrics** — Average composite score (color-coded), below-threshold count, top flag
- **Budget indicator** — Current spend vs. budget limit with color-coded status (turns red when exceeded)
- **Rate limit status** — Current usage vs. limit per API slot with color-coded indicators (green/orange/red based on usage percentage)
- **API response times** — Average, min, max response times and sample count per slot
- **Thread status** — Shows spawned and active thread counts
- **Search** — The Prompts screen has a search box; the Datasets column picker has its own
filter, which is the difference between a list and a scroll on a wide source. The browser's
`Ctrl+F` covers the rest of the page, so the Tk per-panel search bar and **Copy All** were
dropped rather than reimplemented

## Quality Dashboard & Review

The **Quality** and **Review** screens provide post-generation quality analysis:

**Quality screen:**
- **Quality Summary panel** — Average score per dimension with visual bar indicators
- **Recent Scores table** — Latest 50 scored conversations showing Task ID, composite score, per-dimension scores (Coherence, Naturalness, Engagement, Diversity, Consistency, Technical), flags, and scoring method
- **Metrics bar indicators** — Average composite score with color coding (green ≥ 80, amber ≥ 60, red < 60), count of conversations below threshold, and most frequent flag

**Review screen:**
- **Flagged Conversations table** — All conversations below the minimum score threshold, showing Task ID, score, lowest-scoring dimension, flags, source file, and timestamp
- **Color coding** — Critical (red) for scores below 40, warning (amber) for scores 40–threshold
- **Filter** — Type a search term to filter flagged items by task ID
- **Single-click detail** — Selecting a row immediately opens its conversation below: the score breakdown with dimension bars and flags, then the full transcript with colour-coded USER / ASSISTANT speakers
- **Readable rows** — The table uses roomy row heights, stretches its Task ID and Flags columns to the window width, and keeps its own horizontal/vertical scrollbars
- **Scrollable transcript** — The detail pane scrolls on its own (wheel and scrollbar) without moving the page, and keeps the reader's position across dashboard refreshes
- **📤 Export Flagged → JSONL** — Writes every flagged conversation (with its quality score attached as `_quality_score`) to `output/quality_review_flagged.jsonl`, reading the main output, the per-API duplication files and the database
- **🗑 Drop Selected** — Permanently removes the **one** selected conversation from the dataset: its output row, its per-API duplication rows and its database row. This is the action for a flagged conversation that is simply bad data and should not be trained on. It asks for confirmation first, refuses to act on an empty selection (it never falls back to the whole set), and leaves every other conversation — including one whose task id happens to be a prefix of the dropped one — untouched. If the conversation has no output row yet (it may never have been written) the flag is cleared and nothing is deleted. Export first if you want a copy
- **✅ Dismiss All** — Clears all review flags after manual handling (does not delete output data)
- **🔄 Refresh** — Manually refreshes the flagged items list
- **Count line** — Reports how many conversations are currently flagged, how many the quality judge dropped and, separately, how many you dropped yourself. The count survives a resume along with the flags themselves

> **Dropped conversations never appear here.** A conversation the judge rejected (invalid JSON, a `logic` score of 0, or every retry failed) is not written to the dataset, so there is nothing to review — the log records the reason.

## Queue Management

The **Queue** screen provides real-time visibility and control over task processing:

- **Pending panel** — Tasks currently in the queue waiting to be processed
- **Completed panel** — Tasks that have been successfully processed
- **Failed panel** — Tasks that exceeded their retry limit or were discarded

Each panel displays: Task ID, Source File, and Retry count.

**Actions:**
- **🗑️ Purge Queue** — Marks all pending tasks as purged; workers will skip them. Useful for abandoning a bad batch without stopping the job.
- **🔄 Retry Failed** — Re-queues all failed tasks for another processing attempt (resets retry counters).
- **📤 Export Queue** — Exports the full queue state (all three panels) to a CSV file in the output directory.
- **🔄 Refresh View** — Manually refreshes the queue display.

## Animated Progress Bars

Progress bars use color-coded styles that change based on completion percentage:

| Stage | Percentage | Color | Description |
|-------|-----------|-------|-------------|
| Low | 0–25% | Blue | Just starting |
| Medium | 25–50% | Cyan/Teal | Progressing steadily |
| Progressing | 50–75% | Green | Over halfway |
| High | 75–90% | Amber/Yellow | Almost done |
| Complete | 90–100% | Bright Green | Finished |

When a milestone (25%, 50%, 75%, 90%, 100%) is crossed, the progress bar briefly pulses to a brighter color before reverting. Error states are shown in red.

**What the bar counts:** turns belonging to **completed tasks**, not turns in progress. A task's turns are credited once, when its row is actually written — including under **Quality → Async scoring**, where the row is written by a queue processor after the worker has already moved on. A conversation the quality judge **rejects** still credits, because the work is finished even though the row is not kept, so the bar can always reach 100%. A task that is retried or abandoned credits nothing until it finally succeeds, so the number never goes backwards.

## Live Prompt Preview

The **Prompts** screen shows the exact messages being sent to the API in real-time, with rich formatting and utility controls:

**Display Features:**
- **Role-based color coding** — System (purple), User (blue), Assistant (green) with bold role headers
- **Metadata header** — Shows thread ID, API slot, message type (Question/Answer/User Continuation/Slop Fix/Anti-Slop Fix), attempt number, and timestamp
- **Stats bar** — Message count, total character count, and estimated token count (~4 chars/token)
- **Separator lines** — Visual dividers between messages for readability

**Utility Controls:**
- **🔍 Search** — Case-insensitive search with yellow highlighting of all matches; auto-scrolls to first match
- **📋 System** — Copies only the system prompt section to clipboard
- **📋 Latest** — Copies the most recent message to clipboard
- **📋 All** — Copies the full prompt content to clipboard
- **Auto-scroll toggle** — Enable/disable automatic scrolling to the latest prompt

This is useful for:
- Debugging prompt templates and variable substitution
- Verifying character engine injections and emotional state assignments
- Confirming system prompt construction and jailbreak additions
- Monitoring the conversation context being passed to the LLM
- Verifying slop/anti-slop fixer prompts

The preview updates thread-safely from worker threads and respects the dashboard pause state.

## Configuration Profiles

Save and load complete configurations as named profiles:

1. Configure all settings in the editor
2. Enter a profile name and click **Save Current Editor Config As Profile**
3. Profiles are stored in `config/profiles/` as YAML files
4. Load a profile to overwrite `config.yml` and apply those settings
5. Delete profiles you no longer need

## API Connection Testing

The Configuration Editor includes a **Test Connection** button for each API slot. Clicking it sends a minimal test request (`"Reply with 'OK'."`) in a background thread to verify:

- The API URL is reachable and correctly formatted
- The API key is valid
- The model name is accepted by the endpoint

Results are displayed next to the button with color-coded status (green for success, red for failure with error details). The test runs asynchronously so the UI remains responsive.

## Valkey Connection Testing

The main UI includes a **Test Valkey** button that verifies Valkey/Redis connectivity. It:

- Tests the connection with a PING command
- Displays server version and connected client information on success
- Shows detailed error messages on failure
- Automatically updates the connection status indicator in the UI

## Stop & Clear Job

The **Stop & Clear Job** button stops the current generation, clears all progress, removes the state file, and resets all statistics. This allows you to start a completely fresh generation run. Output files are not deleted by this action (they are backed up on the next fresh start).

## Force Recovery

The **🔄 Force Recovery** button bypasses configuration compatibility checks and forces the application to reload a previous generation state. This is useful when:

- You intentionally changed critical settings but still want to resume
- The normal resume dialog is blocking you due to detected incompatibilities
- You want to continue from a saved state without starting fresh

Use with caution, as incompatible settings may lead to unexpected behavior.

## Database Management

- **Export DB → JSONL** — Exports all conversations stored in PostgreSQL to a JSONL file. A file dialog prompts for the save location. The export runs in a background thread to prevent UI freeze and uses a server-side cursor for large datasets.
- **Clear Database** — Truncates the `generated_conversations` table in PostgreSQL. A confirmation dialog prevents accidental data loss.

## Toast Notifications

The application uses a toast notification system for non-blocking status updates. Toasts appear in the bottom-right corner with an icon and color based on severity:

- ✅ **Success** (green) — e.g., "Database cleared successfully!"
- ❌ **Error** (red) — e.g., "Export failed: DB pool not initialized."
- ⚠️ **Warning** (yellow) — e.g., "Questions file is empty or not found."
- ℹ️ **Info** (blue) — e.g., "All tasks already completed."

Toasts auto-dismiss after 4 seconds and stack vertically.
