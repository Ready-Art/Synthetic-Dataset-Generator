# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""The Review tab's data layer, with no Tk in it.

The Review tab is the one screen the maintainer named by hand, and it is the one screen
``control.py`` cannot serve: ``HeadlessController`` has no verb for "list the flagged
conversations", "read this one", "drop it" or "dismiss them all". Those four operations
live in ``generate_tk.py`` today, which builds the whole Tk window at import time and so cannot
be reached from the web app.

This module is the GUI-free home for them. It is the same logic, moved rather than
rewritten, and it is what ``webapp/review_routes.py`` calls. ``generate_tk.py`` keeps its own
copy for now; Phase 10 removes the Tk entry point and this becomes the only one.

**Why this is a separate module and not a ``control.py`` verb.** ``control.py`` is the
engine seam and its verb list is a contract the CLI depends on. Adding four verbs there
would change that contract and the socket protocol for a feature only the browser uses. A separate
module keeps the contract intact.

What it must never do
---------------------
* **Never drop more than the selected task.** The scope is one task id, matched exactly
  (plus its own ``<task_id>_api…`` duplication rows). A filtered list, a stale tick or a
  shared id prefix must never widen it into a dataset-wide delete.
* **Never discard a line it cannot parse.** A line that is not valid JSON is data loss we
  cannot attribute to this task, so it is copied through byte for byte.
* **Never leave a half-written file.** Each rewrite goes to a temporary neighbour and is
  moved into place, so an interrupted drop cannot corrupt the dataset.
* **Never interleave with a worker's write.** The rewrite holds
  ``app_state.output_data_lock``, the same lock a worker holds while appending.
"""

import json
import os

import app_state
import quality
import run_config
from logging_config import log_message


# --- reading -------------------------------------------------------------

def record_belongs_to_task(record_id, task_id):
    """Whether an output row's ``id`` belongs to ``task_id``.

    An exact match, or the ``<task_id>_api…`` duplication rows. Deliberately not a
    prefix match: a task id is derived from a source file name, so ``abc`` must not claim
    ``abcdef``'s rows.
    """
    if not record_id or not task_id:
        return False
    return record_id == task_id or record_id.startswith(task_id + '_api')


def output_files():
    """Every output JSONL file a flagged row can live in (main + per-API duplication)."""
    files = []
    output_dir = app_state.OUTPUT_DIR
    if not os.path.isdir(output_dir):
        return files
    for name in sorted(os.listdir(output_dir)):
        if name.startswith('output') and name.endswith('.jsonl'):
            files.append(os.path.join(output_dir, name))
    return files


def iter_records(task_ids):
    """Yield ``(task_id, record)`` for every output row of ``task_ids``.

    Reads the file output and, when enabled, the database, so the tab works in both
    modes. Records are matched by full id, never by a truncated display value.
    """
    task_ids = list(task_ids)
    if not task_ids:
        return
    for path in output_files():
        try:
            with open(path, 'r', encoding='utf-8') as source:
                for line in source:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(entry, dict):
                        continue
                    entry_id = entry.get('id', '')
                    for task_id in task_ids:
                        if record_belongs_to_task(entry_id, task_id):
                            yield task_id, entry
                            break
        except OSError as exc:
            log_message(f'Review: could not read {path}: {exc}', 'WARNING')

    if app_state.global_config.get('database.enabled', False) and app_state.db_pool:
        conn = None
        try:
            conn = app_state.db_pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute(
                    'SELECT task_id, conversation_data FROM generated_conversations')
                for task_id_db, data in cursor:
                    if isinstance(data, str):
                        try:
                            data = json.loads(data)
                        except json.JSONDecodeError:
                            continue
                    for task_id in task_ids:
                        if record_belongs_to_task(task_id_db, task_id):
                            yield task_id, (data if isinstance(data, dict) else {})
                            break
        except Exception as exc:  # noqa: BLE001 - a read must not break the tab
            log_message(f'Review: database read failed: {exc}', 'ERROR')
        finally:
            if conn is not None:
                try:
                    app_state.db_pool.putconn(conn)
                except Exception:  # noqa: BLE001
                    pass


def find_record(task_id):
    """The output row for ``task_id`` (first match), or ``None`` if never written."""
    for _task, entry in iter_records([task_id]):
        return entry
    return None


def format_conversation(record):
    """A readable transcript of one output row: ShareGPT ``conversations[]`` or ``messages[]``.

    Returns a list of ``(label, text)`` pairs, with the role mapped to the label the Tk
    tab shows (``USER`` / ``ASSISTANT`` / ``SYSTEM``).
    """
    turns = []
    if not isinstance(record, dict):
        return turns
    for turn in record.get('conversations') or record.get('messages') or []:
        if not isinstance(turn, dict):
            continue
        role = turn.get('from') or turn.get('role') or '?'
        text = turn.get('value')
        if text is None:
            text = turn.get('content', '')
        label = {'human': 'USER', 'gpt': 'ASSISTANT',
                 'system': 'SYSTEM'}.get(str(role).lower(), str(role).upper())
        turns.append((label, str(text)))
    return turns


def signature(review_ids, scores, dropped):
    """A cheap value that changes only when the Review list's content would change.

    The dashboard refreshes on every tick. Rebuilding the list on every tick would clear the
    reader's selection and reset the detail pane -- the v11.4 bug that made reading a flagged
    conversation impossible. The list is rebuilt only when this changes.
    """
    parts = []
    for task_id in sorted(review_ids):
        score_data = scores.get(task_id)
        if not score_data:
            continue
        parts.append((task_id,
                       score_data.get('composite'),
                       score_data.get('scored_at', ''),
                       tuple(score_data.get('flags') or ())))
    return (tuple(parts), tuple(sorted(dropped.items())))


def list_flagged(filter_text=''):
    """The flagged conversations, newest score first, as plain dicts for the browser.

    Each row is ``{task_id, display_id, composite, lowest_dim, lowest_value, flags,
    source_file, scored_at, severity}``. ``severity`` is ``critical`` below 40 and
    ``warning`` otherwise -- the same two tags the Tk table colours.
    """
    with app_state.quality_review_lock:
        review_ids = sorted(app_state.quality_review_ids)
    with app_state.quality_lock:
        scores = dict(app_state.quality_scores)
        dropped = dict(app_state.quality_dropped)

    needle = (filter_text or '').strip().lower()
    rows = []
    for task_id in review_ids:
        if needle and needle not in task_id.lower():
            continue
        score_data = scores.get(task_id)
        if not score_data:
            continue
        dimensions = score_data.get('dimensions') or {}
        if dimensions:
            lowest_dim = min(dimensions, key=dimensions.get)
            lowest_value = dimensions.get(lowest_dim, 0)
        else:
            lowest_dim = 'N/A'
            lowest_value = 0
        composite = score_data.get('composite', 0)
        with app_state.task_queue_ui_lock:
            meta = app_state.task_metadata.get(task_id, {})
            source_file = meta.get('file', 'N/A')
        rows.append({
            'task_id': task_id,
            'display_id': task_id if len(task_id) <= 44 else task_id[:41] + '…',
            'composite': composite,
            'lowest_dim': lowest_dim,
            'lowest_value': lowest_value,
            'flags': list(score_data.get('flags') or []),
            'source_file': source_file,
            'scored_at': score_data.get('scored_at', ''),
            'severity': 'critical' if composite < 40 else 'warning',
        })

    # Lowest score first: the worst row is the one a reader wants to see first.
    rows.sort(key=lambda row: (row['composite'], row['task_id']))
    return rows


def counts():
    """The Review tab's count line: flagged, dropped by the judge, dropped by the user.

    The two drop counts are reported apart on purpose (v12.8.2): one is the operator's
    decision, the other the judge's, and mixing them made the line read as if the judge had
    rejected a row the user removed by hand.
    """
    with app_state.quality_review_lock:
        flagged = len(app_state.quality_review_ids)
    with app_state.quality_lock:
        dropped = dict(app_state.quality_dropped)
    manual = sum(1 for reason in dropped.values()
                 if reason == quality.DROP_REASON_MANUAL)
    return {
        'flagged': flagged,
        'dropped_by_judge': len(dropped) - manual,
        'dropped_by_user': manual,
    }


def detail(task_id):
    """The detail pane for one flagged conversation.

    Returns ``{task_id, score, dimensions, flags, method, scored_at, turns, note}``.
    ``turns`` is the transcript from the output files, so the tab shows what was actually
    written rather than pointing at a path. ``note`` explains an empty transcript instead of
    leaving a blank pane.
    """
    with app_state.quality_lock:
        score_data = dict(app_state.quality_scores.get(task_id, {}) or {})

    payload = {
        'task_id': task_id,
        'score': score_data.get('composite'),
        'dimensions': score_data.get('dimensions') or {},
        'flags': list(score_data.get('flags') or []),
        'method': score_data.get('method'),
        'scored_at': score_data.get('scored_at'),
        'turns': [],
        'note': None,
    }

    record = find_record(task_id)
    if record is None:
        payload['note'] = (
            'This conversation is not in the output files yet. Rows are written when a '
            'conversation completes; a row the quality judge rejected is never written. '
            'Check the Quality tab and output/log.txt.')
        return payload

    turns = format_conversation(record)
    if not turns:
        payload['note'] = 'This output row has no conversation turns.'
        return payload
    payload['turns'] = [{'role': label, 'text': text} for label, text in turns]
    return payload


# --- writing -------------------------------------------------------------

def _drop_task_output_rows(task_id):
    """Remove this task's rows from every output JSONL file. Returns ``(rows, files)``.

    Only rows whose ``id`` is the task id or one of its ``<task_id>_api…`` duplication
    rows are removed -- the rest of the file is copied through byte for byte, and a line that
    is not valid JSON is never discarded. Each file is rewritten to a temporary neighbour and
    moved into place, so an interrupted drop cannot leave a half-written dataset behind.
    """
    rows_removed = 0
    files_changed = 0
    for path in output_files():
        try:
            with open(path, 'r', encoding='utf-8') as source:
                lines = source.readlines()
        except OSError as exc:
            log_message(f'Review: could not read {path} to drop {task_id}: {exc}',
                         'WARNING')
            continue

        kept = []
        removed_here = 0
        for line in lines:
            stripped = line.strip()
            if stripped:
                try:
                    entry = json.loads(stripped)
                except json.JSONDecodeError:
                    entry = None
                if isinstance(entry, dict) and record_belongs_to_task(
                        entry.get('id', ''), task_id):
                    removed_here += 1
                    continue
            kept.append(line)

        if not removed_here:
            continue

        temporary = path + '.droptmp'
        try:
            with open(temporary, 'w', encoding='utf-8') as destination:
                destination.writelines(kept)
            os.replace(temporary, path)
        except OSError as exc:
            log_message(f'Review: could not rewrite {path} to drop {task_id}: {exc}',
                         'ERROR')
            try:
                if os.path.exists(temporary):
                    os.remove(temporary)
            except OSError:
                pass
            continue

        rows_removed += removed_here
        files_changed += 1
        log_message(f'Review: dropped {removed_here} row(s) for {task_id} from {path}',
                     'INFO')
    return rows_removed, files_changed


def _drop_task_db_rows(task_id):
    """Remove this task's rows from the database. Returns the number of rows deleted.

    Uses the same id matching as the file path (exact id plus the ``<task_id>_api…``
    duplication rows) and nothing else, so a drop never truncates the table.
    """
    if not app_state.global_config.get('database.enabled', False) or not app_state.db_pool:
        return 0
    conn = None
    deleted = 0
    try:
        conn = app_state.db_pool.getconn()
        with conn.cursor() as cursor:
            # Deliberately NO LIKE: a task id is derived from a source file name, so ``%``
            # and ``_`` in an unescaped pattern could match another task's rows. An exact
            # id plus an exact ``<task_id>_api`` prefix is the same test
            # ``record_belongs_to_task()`` applies to the file path.
            cursor.execute(
                'DELETE FROM generated_conversations '
                'WHERE task_id = %s OR substring(task_id from 1 for %s) = %s',
                (task_id, len(task_id) + 4, task_id + '_api'))
            deleted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            conn.commit()
        log_message(f'Review: dropped {deleted} database row(s) for {task_id}', 'INFO')
    except Exception as exc:  # noqa: BLE001 - a failed drop must not crash the tab
        log_message(f'Review: database drop failed for {task_id}: {exc}', 'ERROR')
        deleted = 0
    finally:
        if conn is not None:
            try:
                app_state.db_pool.putconn(conn)
            except Exception:  # noqa: BLE001
                pass
    return deleted


def drop(task_id):
    """Drop ONE flagged conversation from the dataset.

    Dismiss removes the flag and export copies the row elsewhere; neither changes the
    dataset. A flagged conversation that is simply bad data has to leave the dataset, so this
    removes that ONE task's rows -- its main output row, its per-API duplication rows and its
    database row -- and nothing else.

    The rewrite runs under ``app_state.output_data_lock``, the same lock a worker holds while
    it appends a finished conversation, so a drop cannot interleave with a write. The task is
    then recorded in ``app_state.quality_dropped`` (reason ``manual``) and removed from
    ``quality_scores`` / ``quality_review_ids``, so the dashboard, the Quality tab, a resumed
    run and the Review list all agree that the row is gone on purpose.

    Returns ``{task_id, rows_removed, files_changed, db_rows_removed, total_removed}``.
    """
    if not task_id:
        raise ValueError('A task id is required to drop a conversation.')

    with app_state.output_data_lock:
        rows_removed, files_changed = _drop_task_output_rows(task_id)
        db_rows_removed = _drop_task_db_rows(task_id)
    total_removed = rows_removed + db_rows_removed

    with app_state.quality_lock:
        app_state.quality_scores.pop(task_id, None)
        app_state.quality_dropped[task_id] = quality.DROP_REASON_MANUAL
        app_state.dropped_task_ids.add(task_id)
    with app_state.quality_review_lock:
        app_state.quality_review_ids.discard(task_id)

    log_message(
        f'Review: user dropped {task_id} from the dataset '
        f'({rows_removed} row(s) in {files_changed} file(s), '
        f'{db_rows_removed} database row(s)).',
        'WARNING' if total_removed else 'INFO')

    return {
        'task_id': task_id,
        'rows_removed': rows_removed,
        'files_changed': files_changed,
        'db_rows_removed': db_rows_removed,
        'total_removed': total_removed,
    }


def dismiss_all():
    """Clear every review flag. Does NOT delete any output data."""
    with app_state.quality_review_lock:
        cleared = len(app_state.quality_review_ids)
        app_state.quality_review_ids.clear()
    log_message(f'Review: dismissed {cleared} flag(s).', 'INFO')
    return {'dismissed': cleared}


def export_flagged(output=None):
    """Write every flagged conversation to a JSONL file. Returns the result dict.

    Each row carries its quality verdict in ``_quality_score``. Reads the main output file, the
    per-API duplication files and the database, so the button works in every mode. The file is
    always (re)written, so a stale export can never look like a successful one.
    """
    with app_state.quality_review_lock:
        review_ids = set(app_state.quality_review_ids)

    if not review_ids:
        return {'exported': 0, 'path': None, 'missing': [],
                'message': 'No flagged items to export.'}

    with app_state.quality_lock:
        scores = dict(app_state.quality_scores)

    path = output or os.path.join(app_state.OUTPUT_DIR, 'quality_review_flagged.jsonl')
    exported = 0
    found = set()

    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as destination:
            for task_id, entry in iter_records(review_ids):
                # Never mutate the source record: the database path may share it.
                record = dict(entry)
                record['_quality_score'] = scores.get(task_id)
                destination.write(json.dumps(record) + '\n')
                exported += 1
                found.add(task_id)
    except OSError as exc:
        log_message(f'Quality review export failed: {exc}', 'ERROR')
        raise

    missing = sorted(review_ids - found)
    return {'exported': exported, 'path': path, 'missing': missing,
            'message': (f'Exported {exported} flagged conversation(s) to {path}'
                        if exported else
                        f'No flagged conversations found in the output files '
                        f'(0 written to {path})')}
