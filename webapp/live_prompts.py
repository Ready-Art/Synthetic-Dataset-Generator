# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""The Live Prompt Viewer's data layer -- a ring of the last prompts sent to an API.

``generation.py`` already calls ``update_live_prompt_preview(messages, metadata)`` before every
request, and that shim forwards to whatever ``app_state.live_prompt_preview_hook`` holds. The Tk
window registers a hook that writes into a Text widget. The web app has no widget, so it
registers this module's :func:`record` instead and the browser reads a route.

**Why a ring and not a queue.** This is a debugging view of what is being sent *right now*. A
reader who opens the tab wants the most recent request, not a backlog from ten minutes ago, and
an unbounded list would grow by one entry per request for the length of a run. The ring is
capped, oldest dropped first, and the browser shows newest first.

**Why the hook is installed from ``Engine.start()`` and not at import.** Importing
``webapp.live_prompts`` must stay free of side effects, exactly like every other module in this
package: a test imports it to check the ring without a run, and installing a hook at import time
would make that test mutate global state.

**The hook must never raise into the request path.** It runs on a worker thread between building
a payload and sending it, so an exception here would fail a generation attempt for a reason that
has nothing to do with the API. Every entry point catches.

What this module must never do
------------------------------
* **Never import ``generate_tk.py``.** It builds the whole Tk window at import time.
* **Never keep a reference to the caller's list.** ``generation.py`` reuses its ``messages`` list
  across attempts in some paths, so a stored reference would silently rewrite history.
"""

import threading
import time

import app_state

# How many prompts the viewer keeps. The Tk tab showed one at a time; a ring of 40 is enough to
# see a retry sequence without holding a whole run in memory.
RING_LENGTH = 40

# The largest body a single entry will carry. A prompt is normally a few KB; a runaway one
# (a 1,000-name list pasted into a system prompt) must not be able to fill the ring with
# megabytes. The text is truncated and the entry says so.
MAX_TEXT_CHARS = 120_000

_lock = threading.Lock()
_ring = []
_sequence = 0
_installed = False


def _flatten_content(content):
    """A message body as text.

    A multimodal message carries a list of parts. The text parts are joined and anything else is
    named, so the viewer shows something rather than ``[object Object]``.
    """
    if content is None:
        return ''
    if isinstance(content, str):
        return content
    parts = []
    for part in content if isinstance(content, list) else [content]:
        if isinstance(part, dict) and isinstance(part.get('text'), str):
            parts.append(part['text'])
        else:
            parts.append(f'[{type(part).__name__}]')
    return '\n'.join(parts)


def record(messages, metadata=None):
    """Append one prompt to the ring. Called from a worker thread, never raises.

    ``messages`` is the OpenAI-style list the request will carry. Each turn is copied into a plain
    ``{role, content}`` dict so nothing here aliases the caller's objects.
    """
    global _sequence
    try:
        if not isinstance(messages, (list, tuple)):
            return
        metadata = metadata or {}

        turns = []
        total_chars = 0
        for message in messages:
            if not isinstance(message, dict):
                continue
            role = str(message.get('role', 'unknown') or 'unknown')
            content = _flatten_content(message.get('content'))
            total_chars += len(content)
            turns.append({'role': role, 'content': content})

        truncated = False
        if total_chars > MAX_TEXT_CHARS:
            truncated = True
            budget = MAX_TEXT_CHARS
            trimmed = []
            for turn in turns:
                if budget <= 0:
                    break
                text = turn['content'][:budget]
                budget -= len(text)
                trimmed.append({'role': turn['role'], 'content': text})
            turns = trimmed

        try:
            api_slot = int(metadata.get('api_slot_idx', 0) or 0)
        except (TypeError, ValueError):
            api_slot = 0

        with _lock:
            _sequence += 1
            entry = {
                'seq': _sequence,
                'time': str(metadata.get('timestamp') or time.strftime('%H:%M:%S')),
                'recorded_at': time.time(),
                'thread_id': metadata.get('thread_id'),
                'api_slot': api_slot,
                'message_type': str(metadata.get('message_type') or 'Unknown'),
                'attempt': metadata.get('attempt'),
                'turns': turns,
                'chars': total_chars,
                # The Tk tab's own estimate, kept identical so the two views agree.
                'approx_tokens': total_chars // 4,
                'truncated': truncated,
            }
            _ring.append(entry)
            overflow = len(_ring) - RING_LENGTH
            if overflow > 0:
                del _ring[:overflow]
    except Exception:  # noqa: BLE001 - a preview must never fail a request
        return


def snapshot(limit=None):
    """The ring, newest first, as copies."""
    with _lock:
        entries = [dict(entry) for entry in _ring]
    entries.reverse()
    if limit:
        entries = entries[:int(limit)]
    return entries


def clear():
    """Empty the ring. Returns how many entries were dropped."""
    with _lock:
        count = len(_ring)
        _ring.clear()
    return count


def installed():
    """Whether this module currently owns the engine's preview hook."""
    return _installed


def install():
    """Point ``app_state.live_prompt_preview_hook`` at :func:`record`. Idempotent."""
    global _installed
    app_state.live_prompt_preview_hook = record
    _installed = True


def uninstall():
    """Detach the hook, but only if it is still ours.

    A test (or a future Tk window sharing the process) may have replaced it; clearing someone
    else's hook would silently disable their preview.
    """
    global _installed
    if app_state.live_prompt_preview_hook is record:
        app_state.live_prompt_preview_hook = None
    _installed = False
