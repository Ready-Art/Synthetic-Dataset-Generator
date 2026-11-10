# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Headless tests for the config tree.

This is the gate that guarantees the web Config editor **cannot lose a leaf**. An editor
that builds a fresh literal and dumps it over ``config.yml`` with no merge silently drops
every key it does not know about on save. The web editor edits the loaded tree and writes
the tree back, and this file is what proves it.

The assertions, in order of how much they matter:

1. **Byte-for-byte round trip of the shipped `config/config.yml`.** Load, save, compare the
   bytes. This is the one that matters: 1,291 leaves, 113 comment lines and the SPDX header all
   survive. A plain ``yaml.dump`` fails this -- it deletes every comment and re-wraps 122 lines
   -- which is why :mod:`webapp.config_text` exists.
2. **A one-key edit is a small diff.** Not a correctness rule, a usability one: a save that
   rewrites the whole file is unreviewable in git.
3. **Add works.** A path that does not exist yet is created, at the right indentation, in the
   right block -- including a whole new section.
4. **Delete works, and only deletes.**
5. **The 1,000-name list is addressable one entry at a time** and survives a round trip.
6. **Secrets never leave the server**: `redact` masks all nine, and `merge_secrets` puts the
   stored value back so an untouched masked field cannot blank an API key.
7. **`kind_of` covers every leaf in the real file**, so a leaf with no schema entry still
   renders.

Everything runs against a **temporary directory**. The real `config/config.yml` is read but
never written.
"""

import copy
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml

import webapp.config_text as config_text
import webapp.config_tree as config_tree

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHIPPED = os.path.join(REPO, 'config', 'config.yml')


# --- helpers -------------------------------------------------------------

def _read_bytes(path):
    with open(path, 'rb') as handle:
        return handle.read()


def _read_text(path):
    with open(path, 'r', encoding='utf-8') as handle:
        return handle.read()


def sandbox():
    """A temporary directory holding a copy of the shipped config."""
    directory = tempfile.mkdtemp(prefix='config-tree-')
    target = os.path.join(directory, 'config.yml')
    shutil.copy(SHIPPED, target)
    return directory, target


def diff_lines(before, after):
    """How many lines `diff` would report between two texts."""
    import difflib
    return sum(1 for line in difflib.unified_diff(before.splitlines(),
                                                  after.splitlines(), lineterm='')
               if line.startswith(('+', '-')) and not line.startswith(('+++', '---')))


# --- 1. the byte-for-byte round trip ------------------------------------

def test_the_shipped_config_round_trips_byte_for_byte():
    """Load then save the real file: not one byte may move.

    This is the Phase 6a gate. It fails on any writer that re-dumps the document, because
    PyYAML drops all 113 comments and re-wraps 122 lines.
    """
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.save(tree, target)
        assert _read_bytes(target) == _read_bytes(SHIPPED), (
            f'{diff_lines(_read_text(SHIPPED), _read_text(target))} lines changed '
            'on a save that changed nothing')
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_the_round_trip_keeps_every_comment_and_the_spdx_header():
    """Every comment line survives, including the licence header.

    `AGENTS.md` requires the SPDX header on every text file, and `config.yml` is a text file.
    A writer that drops comments deletes the licence notice, which is why this is asserted
    separately from the byte comparison -- so a failure says *what* was lost.
    """
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.save(tree, target)
        before = _read_text(SHIPPED)
        after = _read_text(target)
        assert after.startswith('# SPDX-License-Identifier: AGPL-3.0-or-later'), (
            'the SPDX header was dropped by a save')
        comments_before = [line for line in before.splitlines()
                           if line.lstrip().startswith('#')]
        comments_after = [line for line in after.splitlines()
                          if line.lstrip().startswith('#')]
        assert len(comments_before) >= 8, len(comments_before)
        assert comments_after == comments_before, (
            f'{len(comments_before)} comments in, {len(comments_after)} out')
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_the_round_trip_keeps_all_1291_leaves():
    """The count the plan measured, asserted so a regression is a number, not a diff."""
    tree = config_tree.load(SHIPPED)
    assert len(list(config_tree.leaves(tree))) == 1291, len(list(config_tree.leaves(tree)))
    reloaded = config_tree.loads(config_tree.dumps(tree))
    assert list(config_tree.leaves(reloaded)) == list(config_tree.leaves(tree)), (
        'the leaf list changed across a round trip')


def test_the_round_trip_keeps_the_section_counts():
    """The 12 sections and their leaf counts, as measured from the shipped file."""
    tree = config_tree.load(SHIPPED)
    counts = {entry['key']: entry['leaves'] for entry in config_tree.sections(tree)}
    assert counts == {
        'api': 44, 'valkey': 5, 'database': 7, 'generation': 36, 'quality': 29,
        'prompts': 1116, 'detection': 28, 'samplers': 18, 'gender': 1, 'metrics': 2,
        'datasets': 0, 'control': 5,
    }, counts


def test_the_round_trip_keeps_the_thousand_names():
    """`prompts.random_names.names` is 1,000 entries -- 77% of the file."""
    tree = config_tree.load(SHIPPED)
    names = config_tree.get(tree, 'prompts.random_names.names')
    assert isinstance(names, list) and len(names) == 1000, len(names)
    assert names[0] == 'Clementine', names[0]
    assert names[-1] == 'Ila', names[-1]
    reloaded = config_tree.loads(config_tree.dumps(tree))
    assert config_tree.get(reloaded, 'prompts.random_names.names') == names


def test_an_unchanged_save_is_idempotent():
    """A second save of an unchanged tree is still byte-identical."""
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.save(tree, target)
        first = _read_bytes(target)
        config_tree.save(tree, target)
        assert _read_bytes(target) == first
    finally:
        shutil.rmtree(directory, ignore_errors=True)


# --- 2. an edit is a small diff -----------------------------------------

def test_editing_one_key_changes_only_that_key():
    """A one-key edit must be a handful of diff lines, not a whole-file rewrite.

    The number is not the rule -- the rule is that the file is spliced, not re-dumped. Four
    lines is what a single changed scalar costs (the line, its neighbour, and the two diff
    markers).
    """
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.set_value(tree, 'api.threads', 42)
        config_tree.save(tree, target)
        after = _read_text(target)
        assert config_tree.get(config_tree.load(target), 'api.threads') == 42
        changed = diff_lines(_read_text(SHIPPED), after)
        assert changed <= 6, f'{changed} diff lines for a one-key edit'
        assert '  threads: 42' in after
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_editing_a_prompt_keeps_the_comments_around_it():
    """A long textarea value is re-wrapped, but its neighbours are untouched."""
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.set_value(tree, 'prompts.question', 'A brand new question.')
        config_tree.save(tree, target)
        after = _read_text(target)
        assert config_tree.get(config_tree.load(target), 'prompts.question') == (
            'A brand new question.')
        assert 'the licence header' not in after  # sanity: not the header text
        assert after.startswith('# SPDX-License-Identifier: AGPL-3.0-or-later')
        assert '  # How many sentences the Anti-Slop Fixer may rewrite' in after
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_a_multiline_value_survives_a_round_trip():
    """Newlines and blank lines in a prompt are the easiest thing to mangle."""
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        value = 'First line.\n\nSecond paragraph.\nThird line.'
        config_tree.set_value(tree, 'prompts.answer', value)
        config_tree.save(tree, target)
        assert config_tree.get(config_tree.load(target), 'prompts.answer') == value
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_a_value_that_looks_like_a_number_stays_a_string():
    """`yaml.dump` quotes `'123'`; a writer that forgets turns it into an int."""
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.set_value(tree, 'api.apis.0.model', '123')
        config_tree.save(tree, target)
        value = config_tree.get(config_tree.load(target), 'api.apis.0.model')
        assert value == '123' and isinstance(value, str), repr(value)
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_an_empty_string_stays_an_empty_string():
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.set_value(tree, 'api.apis.0.url', '')
        config_tree.save(tree, target)
        assert config_tree.get(config_tree.load(target), 'api.apis.0.url') == ''
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_a_boolean_and_a_float_keep_their_type():
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.set_value(tree, 'api.master_duplication_mode', True)
        config_tree.set_value(tree, 'api.pricing.cost_per_1k_tokens', 0.125)
        config_tree.save(tree, target)
        reloaded = config_tree.load(target)
        assert config_tree.get(reloaded, 'api.master_duplication_mode') is True
        assert config_tree.get(reloaded, 'api.pricing.cost_per_1k_tokens') == 0.125
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_editing_a_name_in_the_thousand_keeps_the_other_999():
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        original = list(config_tree.get(tree, 'prompts.random_names.names'))
        config_tree.set_value(tree, 'prompts.random_names.names.500', 'Zebediah')
        config_tree.save(tree, target)
        reloaded = config_tree.get(config_tree.load(target), 'prompts.random_names.names')
        assert len(reloaded) == 1000
        assert reloaded[500] == 'Zebediah'
        assert reloaded[:500] == original[:500]
        assert reloaded[501:] == original[501:]
    finally:
        shutil.rmtree(directory, ignore_errors=True)


# --- 3. add -------------------------------------------------------------

def test_adding_a_nested_key_creates_it_in_the_right_block():
    """`set_value` creates the parents, so add is the same operation as edit."""
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.set_value(tree, 'generation.my_new_flag', True)
        config_tree.save(tree, target)
        reloaded = config_tree.load(target)
        assert config_tree.get(reloaded, 'generation.my_new_flag') is True
        # It landed inside `generation`, not at the end of the file.
        text = _read_text(target)
        assert '  my_new_flag: true' in text
        assert text.index('my_new_flag') < text.index('quality:')
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_adding_a_whole_new_section_works():
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.set_value(tree, 'brand_new', {'a': 1, 'b': 'two'})
        config_tree.save(tree, target)
        reloaded = config_tree.load(target)
        assert config_tree.get(reloaded, 'brand_new') == {'a': 1, 'b': 'two'}
        assert _read_text(target).startswith('# SPDX-License-Identifier')
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_adding_a_key_whose_parent_is_also_new_works():
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.set_value(tree, 'zzz.deep.x', 5)
        config_tree.save(tree, target)
        reloaded = config_tree.load(target)
        assert config_tree.get(reloaded, 'zzz.deep.x') == 5
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_appending_to_a_list_works():
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        names = config_tree.get(tree, 'prompts.random_names.names')
        names.append('Newname')
        config_tree.save(tree, target)
        reloaded = config_tree.get(config_tree.load(target), 'prompts.random_names.names')
        assert len(reloaded) == 1001 and reloaded[-1] == 'Newname'
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_adding_to_a_list_of_dicts_works():
    """`api.apis` is a list of six dicts; a seventh slot must be addable."""
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.set_value(tree, 'api.apis.6', {'url': 'https://x', 'model': 'm'})
        config_tree.save(tree, target)
        reloaded = config_tree.load(target)
        assert len(config_tree.get(reloaded, 'api.apis')) == 7
        assert config_tree.get(reloaded, 'api.apis.6.url') == 'https://x'
    finally:
        shutil.rmtree(directory, ignore_errors=True)


# --- 4. delete ----------------------------------------------------------

def test_deleting_a_key_removes_it_and_nothing_else():
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        before = len(list(config_tree.leaves(tree)))
        assert config_tree.delete(tree, 'generation.debug_prompt_log') is True
        config_tree.save(tree, target)
        reloaded = config_tree.load(target)
        assert config_tree.get(reloaded, 'generation.debug_prompt_log', 'gone') == 'gone'
        assert len(list(config_tree.leaves(reloaded))) == before - 1
        # The comment above it is left alone: removing prose is worse than a stray comment.
        assert '# it is OFF by default' in _read_text(target)
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_deleting_a_missing_path_reports_false_and_changes_nothing():
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        assert config_tree.delete(tree, 'generation.no_such_key') is False
        config_tree.save(tree, target)
        assert _read_bytes(target) == _read_bytes(SHIPPED)
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_deleting_a_list_entry_shifts_the_rest():
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        names = config_tree.get(tree, 'prompts.random_names.names')
        second = names[1]
        assert config_tree.delete(tree, 'prompts.random_names.names.0') is True
        config_tree.save(tree, target)
        reloaded = config_tree.get(config_tree.load(target), 'prompts.random_names.names')
        assert len(reloaded) == 999 and reloaded[0] == second
    finally:
        shutil.rmtree(directory, ignore_errors=True)


# --- 5. paths -----------------------------------------------------------

def test_get_reads_a_nested_path_and_a_list_index():
    tree = config_tree.load(SHIPPED)
    assert config_tree.get(tree, 'api.apis.0.api_profile') == 'openai_compatible'
    assert config_tree.get(tree, 'valkey.port') == 6379
    assert config_tree.get(tree, 'nope.nope', 'fallback') == 'fallback'


def test_a_bad_path_is_rejected_with_a_message():
    tree = config_tree.load(SHIPPED)
    for path in ['', 'a..b', None]:
        try:
            config_tree.get(tree, path)
        except config_tree.ConfigTreeError:
            continue
        raise AssertionError(f'{path!r} was accepted')


def test_set_value_creates_intermediate_dicts():
    tree = {}
    config_tree.set_value(tree, 'a.b.c', 1)
    assert tree == {'a': {'b': {'c': 1}}}, tree


def test_set_value_creates_intermediate_lists_for_numeric_segments():
    tree = {}
    config_tree.set_value(tree, 'a.0.b', 1)
    assert tree == {'a': [{'b': 1}]}, tree


def test_a_missing_file_is_an_error_not_an_empty_tree():
    try:
        config_tree.load('/nonexistent/nope.yml')
    except config_tree.ConfigTreeError as exc:
        assert 'No config file' in str(exc), exc
        return
    raise AssertionError('a missing file was accepted')


def test_invalid_yaml_is_an_error():
    try:
        config_tree.loads('a: [unclosed')
    except config_tree.ConfigTreeError as exc:
        assert 'not valid YAML' in str(exc), exc
        return
    raise AssertionError('invalid YAML was accepted')


def test_a_non_mapping_document_is_an_error():
    try:
        config_tree.loads('- just\n- a list\n')
    except config_tree.ConfigTreeError as exc:
        assert 'not a mapping' in str(exc), exc
        return
    raise AssertionError('a list document was accepted')


# --- 6. secrets ---------------------------------------------------------

SECRET_PATHS = [
    'api.apis.0.key', 'api.apis.1.key', 'api.apis.2.key',
    'api.apis.3.key', 'api.apis.4.key', 'api.apis.5.key',
    'quality.scoring_api.key', 'valkey.password', 'database.password',
]


def test_there_are_exactly_nine_secret_leaves():
    """There are nine. A tenth appearing means a new key needs a decision."""
    tree = config_tree.load(SHIPPED)
    found = [path for path, _value in config_tree.leaves(tree)
             if config_tree.is_secret(path)]
    assert sorted(found) == sorted(SECRET_PATHS), found


def test_redact_masks_every_secret_and_leaves_the_rest_alone():
    tree = config_tree.load(SHIPPED)
    config_tree.set_value(tree, 'api.apis.0.key', 'sk-real-key')
    config_tree.set_value(tree, 'valkey.password', 'hunter2')
    masked = config_tree.redact(tree)
    assert config_tree.get(masked, 'api.apis.0.key') == config_tree.REDACTED
    assert config_tree.get(masked, 'valkey.password') == config_tree.REDACTED
    assert 'sk-real-key' not in str(masked)
    assert 'hunter2' not in str(masked)
    # The tree itself still holds the real value, because a save writes it back.
    assert config_tree.get(tree, 'api.apis.0.key') == 'sk-real-key'
    # A non-secret neighbour is untouched.
    assert config_tree.get(masked, 'api.apis.0.url') == config_tree.get(tree, 'api.apis.0.url')


def test_redact_does_not_mutate_the_tree():
    tree = config_tree.load(SHIPPED)
    config_tree.set_value(tree, 'api.apis.0.key', 'sk-real-key')
    config_tree.redact(tree)
    assert config_tree.get(tree, 'api.apis.0.key') == 'sk-real-key'


def test_merge_secrets_restores_an_untouched_masked_field():
    """The one mistake that would make the editor worse than a text editor."""
    stored = config_tree.load(SHIPPED)
    config_tree.set_value(stored, 'api.apis.0.key', 'sk-real-key')
    incoming = config_tree.redact(stored)
    merged = config_tree.merge_secrets(incoming, stored)
    assert config_tree.get(merged, 'api.apis.0.key') == 'sk-real-key'


def test_merge_secrets_keeps_a_changed_secret():
    stored = config_tree.load(SHIPPED)
    config_tree.set_value(stored, 'api.apis.0.key', 'sk-old')
    incoming = config_tree.redact(stored)
    config_tree.set_value(incoming, 'api.apis.0.key', 'sk-new')
    merged = config_tree.merge_secrets(incoming, stored)
    assert config_tree.get(merged, 'api.apis.0.key') == 'sk-new'


def test_a_masked_save_does_not_blank_the_stored_key():
    """End to end: redact, save the redacted tree, and the key must survive."""
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.set_value(tree, 'api.apis.0.key', 'sk-real-key')
        config_tree.save(tree, target)

        stored = config_tree.load(target)
        browser_copy = config_tree.redact(stored)
        config_tree.set_value(browser_copy, 'api.threads', 7)
        merged = config_tree.merge_secrets(browser_copy, stored)
        config_tree.save(config_tree.ConfigTree(merged, source_text=stored.source_text,
                                                source_tree=stored.source_tree), target)

        reloaded = config_tree.load(target)
        assert config_tree.get(reloaded, 'api.apis.0.key') == 'sk-real-key'
        assert config_tree.get(reloaded, 'api.threads') == 7
    finally:
        shutil.rmtree(directory, ignore_errors=True)


# --- 7. widget kinds ----------------------------------------------------

def test_kind_of_covers_every_leaf_in_the_real_file():
    """A leaf with no schema entry still has to render."""
    known = {'text', 'textarea', 'number', 'bool', 'list', 'group'}
    tree = config_tree.load(SHIPPED)
    unknown = []
    for path, value in config_tree.leaves(tree):
        kind = config_tree.kind_of(value)
        if kind not in known:
            unknown.append((path, kind))
    assert not unknown, unknown


def test_kind_of_names_the_obvious_types():
    assert config_tree.kind_of(True) == 'bool'
    assert config_tree.kind_of(3) == 'number'
    assert config_tree.kind_of(0.5) == 'number'
    assert config_tree.kind_of(['a']) == 'list'
    assert config_tree.kind_of({'a': 1}) == 'group'
    assert config_tree.kind_of('short') == 'text'
    assert config_tree.kind_of('line\nline') == 'textarea'
    assert config_tree.kind_of('x' * 200) == 'textarea'


def test_the_long_prompts_are_textareas():
    """`prompts.question` is 886 chars and `prompts.answer` 1,979 -- both need a textarea."""
    tree = config_tree.load(SHIPPED)
    for path in ['prompts.question', 'prompts.answer']:
        value = config_tree.get(tree, path)
        assert config_tree.kind_of(value) == 'textarea', (path, len(value))


# --- the text writer's own contract -------------------------------------

def test_the_writer_refuses_invalid_yaml():
    try:
        config_text.parse('a: [unclosed')
    except config_text.ConfigTextError as exc:
        assert 'Not valid YAML' in str(exc), exc
        return
    raise AssertionError('invalid YAML was accepted')


def test_the_writer_reports_a_span_for_every_leaf():
    text = _read_text(SHIPPED)
    _tree, spans, _nodes = config_text.parse(text)
    assert len(spans) == 1291, len(spans)
    for path, span in spans.items():
        assert 0 <= span.value_start <= span.value_end <= len(text), path


def test_the_writer_does_not_reorder_keys():
    directory, target = sandbox()
    try:
        tree = config_tree.load(target)
        config_tree.set_value(tree, 'api.threads', 42)
        config_tree.save(tree, target)
        before = [line.split(':')[0] for line in _read_text(SHIPPED).splitlines()
                  if line and not line.startswith((' ', '#', '-'))]
        after = [line.split(':')[0] for line in _read_text(target).splitlines()
                 if line and not line.startswith((' ', '#', '-'))]
        assert before == after, 'a save reordered the top-level keys'
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def test_dumps_without_a_source_is_still_valid_yaml():
    """A caller with no file on disk gets a plain dump -- correct, just not pretty."""
    tree = {'a': 1, 'b': [1, 2]}
    text = config_tree.dumps(tree)
    assert yaml.safe_load(text) == tree


def test_a_bare_dict_still_works_everywhere():
    """Nothing may require a ConfigTree; the dict is the config."""
    tree = {'a': {'b': 1}}
    assert config_tree.get(tree, 'a.b') == 1
    config_tree.set_value(tree, 'a.c', 2)
    assert tree == {'a': {'b': 1, 'c': 2}}
    assert config_tree.dumps(tree)
    assert config_tree.redact(tree) == tree


def test_the_tree_is_a_dict():
    tree = config_tree.load(SHIPPED)
    assert isinstance(tree, dict)
    assert copy.deepcopy(dict(tree)) == dict(tree)


# --- the writer's container-to-scalar cases (Phase 7) --------------------
#
# Phase 7's "Show all" sets `visible_columns` from `[]` back to `None`, and that edit was
# **silently dropped**: the leaf loop only walks leaves (this path is a sequence in the original
# text) and the regrow helpers only fire when the new value is still a list or a mapping. The
# probe found it because the toast said "All columns shown" and the panel still read "0 of 40".
#
# Chasing it turned up four more shapes with the same hole, one of which **corrupted the file**:
# a block sequence set to a scalar left the old items' text behind as stray YAML. These tests pin
# all of them, in both the flow and the block shape, because the two take different code paths.

_FLOW = 'a:\n  b: []\n  c: [1, 2]\n  d: {}\n  e: {p: q}\n  f: 1\n'
_BLOCK = 'a:\n  b:\n  - x\n  - y\n  c: 1\n  d:\n    p: q\n    r: s\n  e: 2\n'


def _spliced(text, mutate):
    """Apply `mutate` to a parsed copy of `text`, splice, and return the new text."""
    from webapp import config_text
    tree, _, _ = config_text.parse(text)
    original = copy.deepcopy(tree)
    mutate(tree)
    return config_text.splice(text, tree, original), tree


def _round_trips(text, mutate):
    """True when the spliced text parses back to exactly the mutated tree."""
    out, tree = _spliced(text, mutate)
    try:
        return yaml.safe_load(out) == tree, out
    except yaml.YAMLError:
        return False, out


def test_a_sequence_set_to_none_is_written():
    """The Phase 7 bug: `visible_columns: []` -> `None` was silently dropped."""
    for text in (_FLOW, _BLOCK):
        ok, out = _round_trips(text, lambda t: t['a'].__setitem__('b', None))
        assert ok, out
        assert 'b: null' in out, out


def test_a_sequence_set_to_a_scalar_is_written():
    """The corrupting one: a block sequence left its old items behind as stray YAML."""
    for text in (_FLOW, _BLOCK):
        ok, out = _round_trips(text, lambda t: t['a'].__setitem__('b', 'gone'))
        assert ok, out
        assert 'b: gone' in out, out
        assert '- x' not in out, out


def test_a_mapping_set_to_none_is_written():
    for text in (_FLOW, _BLOCK):
        ok, out = _round_trips(text, lambda t: t['a'].__setitem__('d', None))
        assert ok, out
        assert 'd: null' in out, out


def test_a_deleted_sequence_is_removed_whole():
    """Taking only the key's line would leave the items behind as orphaned YAML."""
    for text in (_FLOW, _BLOCK):
        ok, out = _round_trips(text, lambda t: t['a'].pop('b'))
        assert ok, out
        assert 'b:' not in out, out
        assert '- x' not in out, out


def test_a_deleted_mapping_is_removed_whole():
    """Every child goes with the key -- `p: q` alone is not enough to assert on.

    The flow fixture has a *second* `{p: q}` mapping under `e`, so asserting that `p: q` is gone
    would fail on a correct splice. `r: s` exists only under `d`, which is the key being removed.
    """
    for text in (_FLOW, _BLOCK):
        ok, out = _round_trips(text, lambda t: t['a'].pop('d'))
        assert ok, out
        assert 'd:' not in out, out
        assert 'r: s' not in out, out


def test_emptying_a_list_of_mappings_keeps_the_next_key():
    """The overlap bug: the items looked "collapsed" and were deleted a second time.

    A sequence of mappings being emptied is the case where `_collapsed_nodes` and
    `_regrown_sequences` both wanted the same paths, and the double edit removed the key's own line.
    """
    text = 'a:\n  b:\n  - x: 1\n  - x: 2\n  c: 1\n'
    ok, out = _round_trips(text, lambda t: t['a'].__setitem__('b', []))
    assert ok, out
    assert out == 'a:\n  b: []\n  c: 1\n', out


def test_a_block_container_keeps_its_indentation_when_it_collapses():
    """`key_start` points at the key token, not the line start; the indent must survive."""
    text = 'outer:\n  inner:\n  - x\n  - y\n  after: 1\n'
    ok, out = _round_trips(text, lambda t: t['outer'].__setitem__('inner', None))
    assert ok, out
    assert '\n  inner: null' in out, out


def test_the_shipped_file_still_round_trips_byte_for_byte():
    """The whole point of the writer, re-asserted after the Phase 7 fix."""
    from webapp import config_text
    text = open(SHIPPED, encoding='utf-8').read()
    tree, _, _ = config_text.parse(text)
    assert config_text.splice(text, tree, copy.deepcopy(tree)) == text


if __name__ == '__main__':
    failures = 0
    for name, function in sorted(globals().items()):
        if name.startswith('test_') and callable(function):
            try:
                function()
            except AssertionError as exc:
                failures += 1
                print(f'FAIL {name}: {exc}')
            except Exception as exc:  # noqa: BLE001 - a harness reports, never raises
                failures += 1
                print(f'FAIL {name}: {type(exc).__name__}: {exc}')
            else:
                print(f'ok   {name}')
    print(f"\n{'OK' if not failures else str(failures) + ' FAILED'}")
    sys.exit(1 if failures else 0)
