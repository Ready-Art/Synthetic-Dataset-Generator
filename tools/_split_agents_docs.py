#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""One-shot helper: split AGENTS.md into a hub + per-area topic files.

Not part of the test loop. Run from the repository root:
    python -B tools/_split_agents_docs.py
"""
import os
import re

SRC = 'AGENTS.md'
ORIG = '/tmp/AGENTS.md.orig'
OUT_DIR = 'docs/agents'

# filename -> (title, "read this when" line, cross-links)
TOPICS = [
    ('task-index.md', 'Task index (start here for "how do I add X?")',
     'you know WHAT you want to change and need the list of files to edit.',
     ['config.md', 'core-modules.md', 'generation.md', 'detection.md', 'quality.md',
      'generate-ui.md', 'config-editor.md', 'dashboard.md', 'datasets.md', 'tools-and-docs.md']),
    ('config.md', 'Configuration files',
     'you are changing `config/config.yml`, `config/api_profiles.yml` or the dependency pins.',
     ['config-editor.md', 'task-index.md']),
    ('core-modules.md', 'Core modules (API calls, state, config loading, text, layout, style metrics)',
     'you are touching `api_handler.py`, `api_profiles.py`, `app_state.py`, `config_loader.py`, '
     '`logging_config.py`, `text_utils.py`, `ui_layout.py` or `style_metrics.py`.',
     ['generation.md', 'quality.md']),
    ('generate-ui.md', 'Main window (`generate_tk.py`)',
     'you are touching the main Tk window: tabs, metric cards, issue panels, worker launch, '
     'table styles, the Review tab or the Regex Watch tab.',
     ['generation.md', 'dashboard.md', 'config-editor.md']),
    ('config-editor.md', 'Configuration Editor (`config_editor.py`)',
     'you are adding a config field to the editor, or fixing editor layout/readability.',
     ['config.md', 'generate-ui.md']),
    ('dashboard.md', 'Dashboard (`dashboard.py`)',
     'you are adding a metric card, issue panel or issue-graph series.',
     ['generate-ui.md', 'core-modules.md']),
    ('generation.md', 'Generation pipeline (`generation.py`)',
     'you are touching the worker loop, retries, prompts, cast/arc helpers, metadata or output writing.',
     ['detection.md', 'quality.md', 'generate-ui.md']),
    ('detection.md', 'Detection checks (`detection.py`)',
     'you are adding or fixing a refusal/slop/OOC/Regex Watch/non-Latin/scene-continuity check.',
     ['generation.md', 'config.md']),
    ('quality.md', 'Quality scoring (`quality.py`)',
     'you are touching scoring dimensions, the judge request/JSON parsing or the drop policy.',
     ['generation.md', 'dashboard.md']),
    ('datasets.md', 'Dataset binding',
     'you are touching `dataset_*.py`, `datasets_panel.py`, `docs/dataset-bindings.md` or '
     '`docs/input-folder.md`.',
     ['generate-ui.md', 'config-editor.md', 'generation.md']),
    ('tools-and-docs.md', 'Tools and user docs',
     'you are touching `tools/*.py`, `docs/gemma4-chat-template.md` or `docs/scene-cast.md`.',
     ['task-index.md']),
]

# module name -> topic file
MODULE_MAP = {
    'config/config.yml': 'config.md',
    'config/api_profiles.yml': 'config.md',
    'requirements-datasets.txt': 'config.md',
    'api_handler.py': 'core-modules.md',
    'api_profiles.py': 'core-modules.md',
    'app_state.py': 'core-modules.md',
    'config_loader.py': 'core-modules.md',
    'logging_config.py': 'core-modules.md',
    'text_utils.py': 'core-modules.md',
    'ui_layout.py': 'core-modules.md',
    'style_metrics.py': 'core-modules.md',
    'generate_tk.py': 'generate-ui.md',
    'config_editor.py': 'config-editor.md',
    'dashboard.py': 'dashboard.md',
    'generation.py': 'generation.md',
    'detection.py': 'detection.md',
    'quality.py': 'quality.md',
    'dataset_sources.py': 'datasets.md',
    'dataset_settings.py': 'datasets.md',
    'dataset_bindings.py': 'datasets.md',
    'dataset_templates.py': 'datasets.md',
    'dataset_tasks.py': 'datasets.md',
    'datasets_panel.py': 'datasets.md',
    'dataset_column_menu.py': 'datasets.md',
    'docs/dataset-bindings.md': 'datasets.md',
    'docs/input-folder.md': 'datasets.md',
    'tools/check_gui_layout.py': 'tools-and-docs.md',
    'tools/check_dataset_hub.py': 'tools-and-docs.md',
    'tools/gemma4_thought_fix.py': 'tools-and-docs.md',
    'docs/gemma4-chat-template.md': 'tools-and-docs.md',
    'docs/scene-cast.md': 'tools-and-docs.md',
    # tests: filed with the module they guard
    'tests/test_api_profiles.py': 'core-modules.md',
    'tests/test_style_metrics.py': 'core-modules.md',
    'tests/test_quality_tic_density.py': 'quality.md',
    'tests/test_quality_roleplay.py': 'quality.md',
    'tests/test_quality_judge_policy.py': 'quality.md',
    'tests/test_quality_json_robustness.py': 'quality.md',
    'tests/test_worker_launch.py': 'generate-ui.md',
    'tests/test_review_tab_ui.py': 'generate-ui.md',
    'tests/test_regex_watch_tab_ui.py': 'generate-ui.md',
    'tests/test_editor_readability_ui.py': 'config-editor.md',
    'tests/test_user_emotional_states_ui.py': 'config-editor.md',
    'tests/test_missing_quotations_ui.py': 'config-editor.md',
    'tests/test_regex_watch_ui.py': 'config-editor.md',
    'tests/test_questions_dataset_ui.py': 'config-editor.md',
    'tests/test_opening_mode.py': 'generation.md',
    'tests/test_persona_arc.py': 'generation.md',
    'tests/test_random_settings.py': 'generation.md',
    'tests/test_scene_cast.py': 'generation.md',
    'tests/test_missing_quotations.py': 'detection.md',
    'tests/test_non_latin.py': 'detection.md',
    'tests/test_regex_watch.py': 'detection.md',
    'tests/test_dataset_sources.py': 'datasets.md',
    'tests/test_dataset_settings.py': 'datasets.md',
    'tests/test_dataset_bindings.py': 'datasets.md',
    'tests/test_dataset_templates.py': 'datasets.md',
    'tests/test_dataset_tasks.py': 'datasets.md',
    'tests/test_dataset_backends.py': 'datasets.md',
    'tests/test_dataset_column_menu.py': 'datasets.md',
    'tests/test_datasets_panel.py': 'datasets.md',
    'tests/test_dataset_pipeline.py': 'datasets.md',
    'tests/test_dataset_compatibility.py': 'datasets.md',
}


def read_lines(path):
    with open(path, encoding='utf-8') as fh:
        return fh.read().replace('\r\n', '\n').split('\n')


def blocks(lines):
    """Split on '### ' headings outside fenced code blocks."""
    result = []
    in_fence = False
    cur = None
    for line in lines:
        if line.startswith('```'):
            in_fence = not in_fence
        if not in_fence and line.startswith('### '):
            cur = {'heading': line, 'lines': [line]}
            result.append(cur)
            continue
        if cur is not None:
            cur['lines'].append(line)
    for b in result:
        while b['lines'] and b['lines'][-1].strip() == '':
            b['lines'].pop()
    return result


def module_of(heading):
    m = re.search(r'`([^`]+)`', heading)
    if not m:
        raise SystemExit('no module name in heading: ' + heading)
    return m.group(1)


def find(lines, prefix):
    for i, line in enumerate(lines):
        if line.startswith(prefix):
            return i
    raise SystemExit('missing heading: ' + prefix)


def section(lines, start_prefix, end_prefix):
    start = find(lines, start_prefix)
    end = find(lines, end_prefix) if end_prefix else len(lines)
    return lines[start:end]


def trim(lines):
    out = list(lines)
    while out and out[-1].strip() == '':
        out.pop()
    while out and out[0].strip() == '':
        out.pop(0)
    return out


def task_rows(quick_ref):
    """Return [(task, files_cell)] from the Common Tasks table."""
    rows = []
    for line in quick_ref:
        if not line.startswith('| ') or line.startswith('| Task ') or line.startswith('|---'):
            continue
        cells = line.strip().strip('|').split(' | ')
        if len(cells) < 2:
            continue
        rows.append((cells[0].strip(), ' | '.join(cells[1:]).strip()))
    return rows


def row_modules(files_cell):
    """Module names mentioned in a task row (backticked token, flags stripped)."""
    found = []
    for match in re.finditer(r'`([^`]+)`', files_cell):
        name = match.group(1).split()[0] if match.group(1).split() else ''
        if name in MODULE_MAP:
            found.append(MODULE_MAP[name])
    return found


def topic_for_task(files_cell):
    """Pick the topic file that owns the FIRST module named in the row."""
    found = row_modules(files_cell)
    if not found:
        raise SystemExit('task row with no known module: ' + files_cell[:80])
    return found[0]


def _unused(files_cell):
    votes = {}
    for match in re.finditer(r'`([^`]+)`', files_cell):
        topic = MODULE_MAP.get(match.group(1))
        if topic:
            votes[topic] = votes.get(topic, 0) + 1
    return max(votes.items(), key=lambda kv: kv[1])[0]


def clean_name(heading):
    name = heading.lstrip('# ').strip()
    name = re.sub(r'\s*\[\d+\]$', '', name)
    return name.strip('`')


def main():
    lines = read_lines(ORIG)

    setup = trim(section(lines, '## Setup and Tests', '## Boundaries'))
    boundaries = trim(section(lines, '## Boundaries', '## Core Architecture'))
    architecture = trim(section(lines, '## Core Architecture', '## File Reference'))
    file_ref = section(lines, '## File Reference', '## Common Tasks Quick Reference')
    quick_ref = trim(section(lines, '## Common Tasks Quick Reference',
                             '## Key State Variables (`app_state.py`)'))
    state_vars = trim(section(lines, '## Key State Variables (`app_state.py`)',
                              '## API Slots Index'))
    slots = trim(section(lines, '## API Slots Index', '## Thread Safety Pattern'))
    threads = trim(section(lines, '## Thread Safety Pattern',
                          '## Config Paths (`config_loader.py`)'))
    config_paths = trim(section(lines, '## Config Paths (`config_loader.py`)', '## Tools'))
    tools_cli = trim([l for l in section(lines, '## Tools', None)
                      if not l.startswith('## Tools')])

    grouped = {}
    for b in blocks(file_ref):
        topic = MODULE_MAP.get(module_of(b['heading']))
        if topic is None:
            raise SystemExit('unmapped section: ' + b['heading'])
        grouped.setdefault(topic, []).append(b)

    # Task rows are filed with the module they edit most, so a small model reads the
    # task list and the file reference for one area in a single pass.
    tasks = {}
    for task, files_cell in task_rows(quick_ref):
        # Each row is stored ONCE, in the file that owns the first module it names. Rows
        # are long; copying them into every file they mention doubled the file sizes and
        # made the small files large again. Cross-references are listed instead.
        tasks.setdefault(topic_for_task(files_cell), []).append((task, files_cell))

    # The old file repeated the Tools section at the bottom; keep the CLI flags there.
    grouped.setdefault('tools-and-docs.md', []).append(
        {'heading': '## Tools (CLI flags and exit codes)', 'lines': tools_cli})

    # app_state.py and config_loader.py detail lives with those modules, not in the hub.
    grouped.setdefault('core-modules.md', []).append(
        {'heading': '## Key State Variables (`app_state.py`)', 'lines': state_vars})
    grouped.setdefault('core-modules.md', []).append(
        {'heading': '## Thread Safety Pattern', 'lines': threads})
    grouped.setdefault('core-modules.md', []).append(
        {'heading': '## Config Paths (`config_loader.py`)', 'lines': config_paths})


    os.makedirs(OUT_DIR, exist_ok=True)

    written = []
    for filename, title, when, links in TOPICS:
        parts = ['# ' + title, '',
                  'Part of the `AGENTS.md` doc set. Start at [`AGENTS.md`](../../AGENTS.md) '
                  'for the shared rules (setup, tests, boundaries, state, slots, thread safety).', '',
                  '**Read this when:** ' + when, '']
        if links:
            parts.append('**Related:** ' + ', '.join(
                '[`docs/agents/%s`](%s)' % (l, l) for l in links))
            parts.append('')
        if filename == 'task-index.md':
            parts.append('## Where is my task?')
            parts.append('')
            parts.append('Each "files to edit" row is stored ONCE, in the topic file that owns the first')
            parts.append('module it names. So you read the task list and the file reference for one area')
            parts.append('together. Find your task below, then open that file.')
            parts.append('')
            parts.append('A row that names modules from other areas too is still complete — it lists every')
            parts.append('file to edit — so you only need that one row, not the other topic files.')
            parts.append('')
            parts.append('| Topic file | Tasks |')
            parts.append('|------------|-------|')
            for topic_file, title, _when, _links in TOPICS:
                rows = tasks.get(topic_file)
                if not rows:
                    continue
                parts.append('| [`%s`](%s) | %s |' % (
                    topic_file, topic_file, '; '.join(t for t, _c in rows)))
            parts.append('')
            parts.append('## Cross-cutting tasks')
            parts.append('')
            parts.append('These name modules from three or more areas. Each is stored in the first area')
            parts.append('it names; follow the file name to read the full step list:')
            parts.append('')
            for task, files_cell in task_rows(quick_ref):
                if len(set(row_modules(files_cell))) > 2:
                    parts.append('- ' + task)
            parts.append('')
        else:
            entries = grouped.get(filename, [])
            parts.append('## Contents')
            parts.append('')
            for b in entries:
                parts.append('- `%s`' % clean_name(b['heading']))
            if tasks.get(filename):
                parts.append('- Tasks that touch these files')
            parts.append('')
            for b in entries:
                parts.append('---')
                parts.append('')
                parts.extend(b['lines'])
                parts.append('')
            rows = tasks.get(filename)
            if rows:
                parts.append('---')
                parts.append('')
                parts.append('## Tasks that touch these files')
                parts.append('')
                parts.append('| Task | Files to edit |')
                parts.append('|------|---------------|')
                for task, files_cell in rows:
                    parts.append('| %s | %s |' % (task, files_cell))
                parts.append('')
        path = os.path.join(OUT_DIR, filename)
        text = '\n'.join(trim(parts)) + '\n'
        with open(path, 'w', encoding='utf-8') as fh:
            fh.write(text)
        written.append((path, len(text), text.count('\n')))

    hub = build_hub(setup, boundaries, architecture, slots, grouped)
    with open(SRC, 'w', encoding='utf-8') as fh:
        fh.write('\n'.join(trim(hub)) + '\n')
    hub_text = '\n'.join(trim(hub)) + '\n'
    written.insert(0, (SRC, len(hub_text), hub_text.count('\n')))

    print('%-38s %8s %6s' % ('file', 'chars', 'lines'))
    for path, size, nlines in written:
        print('%-38s %8d %6d' % (path, size, nlines))


def fix_pointers(lines):
    """Repoint the old in-file 'see below' references at the topic files."""
    text = '\n'.join(lines)
    text = text.replace('File Reference below names one',
                        'topic file for that module names one')
    text = text.replace('(see\n  `generate_tk.py` below)', '(see `docs/agents/generate-ui.md`)')
    text = text.replace('(see Tools)', '(see `docs/agents/tools-and-docs.md`)')
    return text.split('\n')


def build_hub(setup, boundaries, architecture, slots, grouped):
    out = ['# AGENTS.md - Codebase Cheat Sheet (hub)', '']
    out.append('This file is the **entry point** and stays short on purpose: it holds only the rules')
    out.append('that apply to every change. The per-file reference is split into small topic files')
    out.append('under `docs/agents/`, so you never have to hold the whole cheat sheet in context.')
    out.append('')
    out.append('## How to use this doc set')
    out.append('')
    out.append('1. Read this file first.')
    out.append('2. Open the ONE topic file below that matches what you are editing, and read only that.')
    out.append('3. If you know the task but not the files, open `docs/agents/task-index.md` and find')
    out.append('   the row. Each topic file also ends with the task rows that touch its files.')
    out.append('4. If a topic file is still long, read it in sections — its `## Contents` list is the map.')
    out.append('5. Only open a second topic file when a change genuinely spans two areas.')
    out.append('')
    out.append('| Topic file | Read it when |')
    out.append('|------------|--------------|')
    for filename, title, when, _links in TOPICS:
        out.append('| [`docs/agents/%s`](docs/agents/%s) | %s |' % (filename, filename, when))
    out.append('')
    out.append('Cross-cutting tasks (a new detection type, a new quality dimension, scene cast) are')
    out.append('listed in every topic file they touch.')
    out.append('')
    out.extend(fix_pointers(setup))
    out.append('')
    out.extend(fix_pointers(boundaries))
    out.append('')
    out.extend(architecture)
    out.append('')
    out.extend(slots)
    out.append('')
    out.append('## Runtime state, locks and config paths')
    out.append('')
    out.append('The counter / recent-list / timestamp / lock / flag inventory of `app_state.py`, the')
    out.append('thread-safety pattern and the `config_loader.py` paths are in')
    out.append('[`docs/agents/core-modules.md`](docs/agents/core-modules.md) — read it before you add a')
    out.append('counter, a lock or a timestamp key.')
    out.append('')
    out.append('## More rules that are easy to break')
    out.append('')
    out.append('- **Regex Watch patterns exist twice**: `config/config.yml` and')
    out.append('  `detection.DEFAULT_REGEX_WATCH_PATTERNS` must stay identical, and')
    out.append('  `style_metrics.TRAILER_PATTERN` is the one definition of the absolute-trailer')
    out.append('  construction (see `docs/agents/detection.md`).')
    out.append('- **A config-editor text field must be `normal` before you write into it** — this Tk')
    out.append('  build drops `delete`/`insert` on a disabled Text widget silently')
    out.append('  (see `docs/agents/config-editor.md`).')
    out.append('- **Keep `tools/gemma4_thought_fix.py` out of the app** — it is a third-party helper')
    out.append('  written by Karki (Discord), not this repository\'s code')
    out.append('  (see `docs/agents/tools-and-docs.md`).')
    out.append('- **Keep features opt-in by default** — a default run must stay byte-identical to the')
    out.append('  previous release.')
    out.append('')
    out.append('## Adding or changing docs')
    out.append('')
    out.append('When a change touches a module, update the topic file that owns it (and only that')
    out.append('file). Add a row to `docs/agents/task-index.md` when a new kind of task becomes')
    out.append('possible.')
    out.append('')
    out.append('Size budget: keep this hub under ~10,000 characters and every topic file under')
    out.append('~18,000. The largest topic files (`config.md`, `generation.md`, `quality.md`,')
    out.append('`detection.md`) are near that ceiling; if one grows past it, split it along the')
    out.append('module lines in its `## Contents` list rather than appending more prose. Do not')
    out.append('re-merge the files — one big document is what this layout exists to avoid.')
    return out


if __name__ == '__main__':
    main()
