# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""The config tree -- the storage model behind the web Config editor.

**The tree is the source of truth, not a schema.** The web editor is the only editor
so it has to be able to reach every one of the 1,291 leaves in
``config/config.yml`` and to add new ones. A schema-driven form over a curated subset could do
neither, so this module holds the tree and the schema is only decoration on top of it.

Why this exists at all
---------------------
A naive editor builds a **fresh** ``config_data = {...}`` literal and dumps it over
``config.yml`` with **no merge**, so every key it does not know about is silently dropped on save.
This module does the opposite: it loads the file into a dict, edits that dict in place, and writes
the whole dict back, so a round trip is lossless. ``tests/test_web_config_tree.py`` asserts that
byte for byte.

Why it is not just ``yaml.dump``
-------------------------------
Because ``yaml.dump`` is not lossless. Measured on the shipped file, a load-then-dump cycle deletes
all 113 comment lines -- **including the SPDX header** that ``AGENTS.md`` requires -- and re-wraps
122 lines of prose. So a :class:`ConfigTree` remembers the text it was loaded from and
:func:`save` splices only the leaves that actually changed (see :mod:`webapp.config_text`). An
unchanged save is byte-identical by construction, and a one-key edit is a four-line diff.

What a path looks like
----------------------
A path is dot-separated: ``api.apis.0.url``, ``prompts.random_names.names.5``. A numeric segment
indexes a list, exactly as ``ConfigLoader.get`` already does. ``set_value`` creates the intermediate
dicts a path needs, so adding a key that does not exist yet is the same operation as editing one
that does.

What it must never do
---------------------
* **Never drop a leaf it does not understand.** An unknown key is still a key; it is carried through
  a round trip untouched.
* **Never lose a comment.** The source text is kept and spliced, not re-dumped.
* **Never reorder the file.** A save is not a 1,291-line diff.
* **Never write a secret to the browser.** ``redact`` is what the routes call; the tree itself holds
  the real value because that is what gets written back.
* **Never write a half-written file.** A save goes to a temporary neighbour and is moved into place.
"""

import copy
import os
import tempfile

import yaml

from webapp import config_text

# The leaves whose value is a secret. A route must never send one to the browser; the tree holds
# the real value because a save writes it back. Matched on the last path segment, so
# `api.apis.3.key` and `quality.scoring_api.key` are both covered.
SECRET_LEAF_NAMES = frozenset({'key', 'api_key', 'token', 'secret', 'password'})

# What a redacted secret is replaced with. A single constant so a test can assert the real value
# appears nowhere in a payload.
REDACTED = '***'


class ConfigTreeError(Exception):
    """A path that cannot be read or written, with a message a user can act on."""


class ConfigTree(dict):
    """A loaded config, plus the text it came from.

    The dict *is* the config -- every function here takes a plain mapping and works on it, so a
    caller can pass a bare dict and lose nothing but the byte-exact save. The two attributes are
    what make a save minimal:

    ``source_text``
        The file as it was read. :func:`save` splices changes into this.
    ``source_tree``
        A snapshot of the values at load time, so :func:`save` can tell a changed leaf from an
        untouched one without the caller tracking edits.
    """

    def __init__(self, *args, source_text=None, source_tree=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_text = source_text
        self.source_tree = source_tree if source_tree is not None else copy.deepcopy(dict(self))

    def rebase(self):
        """Treat the current contents as the new baseline.

        Called after a save, so a second save in the same request does not re-apply the first
        one's edits against stale offsets.
        """
        self.source_tree = copy.deepcopy(dict(self))
        return self


# --- loading and saving ---------------------------------------------------

def load(path):
    """Load a YAML config file into a :class:`ConfigTree`.

    An empty file becomes ``{}`` rather than ``None``, so a caller never has to special-case it.
    A missing file is an error here rather than a silent empty tree: the editor is the only way to
    edit the config, so a missing file is a real problem worth reporting.
    """
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            text = handle.read()
    except FileNotFoundError:
        raise ConfigTreeError(f'No config file at {path}.') from None
    except OSError as exc:
        raise ConfigTreeError(f'Could not read {path}: {exc}') from None
    return loads(text, path=path)


def loads(text, path=None):
    """Parse `text` into a :class:`ConfigTree` that remembers it."""
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        where = f'{path} is' if path else 'This document is'
        raise ConfigTreeError(f'{where} not valid YAML: {exc}') from None
    data = {} if data is None else data
    if not isinstance(data, dict):
        where = f'{path} is' if path else 'This document is'
        raise ConfigTreeError(f'{where} not a mapping at the top level.')
    return ConfigTree(data, source_text=text)


def dumps(tree):
    """Serialise `tree`, keeping the original text wherever nothing changed."""
    source_text = getattr(tree, 'source_text', None)
    source_tree = getattr(tree, 'source_tree', None)
    return config_text.dumps(dict(tree), source_text, source_tree)


def save(tree, path):
    """Write `tree` to `path`, preserving comments, key order and every untouched byte.

    The write goes to a temporary neighbour and is moved into place, so an interrupted save cannot
    leave a half-written config behind -- the same rule `review_data.drop` follows for the dataset.
    """
    text = dumps(tree)
    directory = os.path.dirname(os.path.abspath(path)) or '.'
    os.makedirs(directory, exist_ok=True)
    temporary = None
    try:
        descriptor, temporary = tempfile.mkstemp(dir=directory, prefix='.config-',
                                                suffix='.tmp')
        with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
            handle.write(text)
        os.replace(temporary, path)
        temporary = None
    except OSError as exc:
        raise ConfigTreeError(f'Could not write {path}: {exc}') from None
    finally:
        if temporary and os.path.exists(temporary):
            try:
                os.remove(temporary)
            except OSError:
                pass
    if isinstance(tree, ConfigTree):
        tree.source_text = text
        tree.rebase()
    return text


# --- reading and writing one path ----------------------------------------

def _segments(path):
    """Split a dot path, rejecting the shapes that cannot mean anything."""
    if not path or not isinstance(path, str):
        raise ConfigTreeError('A path is required.')
    parts = path.split('.')
    if any(part == '' for part in parts):
        raise ConfigTreeError(f'{path!r} has an empty segment.')
    return parts


def get(tree, path, default=None):
    """Read `path` from `tree`, or `default` when any segment is missing.

    A numeric segment indexes a list, matching ``ConfigLoader.get`` so a path means the same thing
    to the editor as it does to the engine.
    """
    value = tree
    for part in _segments(path):
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            return default
    return value


def set_value(tree, path, value):
    """Set `path` in `tree`, creating the intermediate dicts it needs.

    This is what makes **adding** a key the same operation as editing one: a path that does not
    exist yet is created rather than rejected. A numeric segment that runs past the end of a list
    appends, so a list can grow from the editor too.
    """
    parts = _segments(path)
    current = tree
    for index, part in enumerate(parts[:-1]):
        following = parts[index + 1]
        if isinstance(current, dict):
            if part not in current or not isinstance(current[part], (dict, list)):
                current[part] = [] if following.isdigit() else {}
            current = current[part]
        elif isinstance(current, list):
            if not part.isdigit():
                raise ConfigTreeError(
                    f'{path!r} indexes a list with {part!r}, which is not a number.')
            position = int(part)
            while len(current) <= position:
                current.append({} if not following.isdigit() else [])
            if not isinstance(current[position], (dict, list)):
                current[position] = [] if following.isdigit() else {}
            current = current[position]
        else:
            raise ConfigTreeError(
                f'{path!r} runs through a value, not a mapping.')

    last = parts[-1]
    if isinstance(current, dict):
        current[last] = value
    elif isinstance(current, list):
        if not last.isdigit():
            raise ConfigTreeError(
                f'{path!r} indexes a list with {last!r}, which is not a number.')
        position = int(last)
        while len(current) <= position:
            current.append(None)
        current[position] = value
    else:
        raise ConfigTreeError(f'{path!r} ends on a value, not a mapping.')
    return tree


def delete(tree, path):
    """Remove `path` from `tree`. Returns whether anything was removed.

    A delete is how a key the user added is taken back out. It refuses the root, because an empty
    config is not a useful thing to save.

    A comment that sat above the deleted key is left where it is. Removing prose the user wrote is
    worse than leaving a comment that has lost its subject, and the comment may describe the block
    rather than the single key.
    """
    parts = _segments(path)
    parent = tree
    for part in parts[:-1]:
        if isinstance(parent, dict) and part in parent:
            parent = parent[part]
        elif isinstance(parent, list) and part.isdigit() and int(part) < len(parent):
            parent = parent[int(part)]
        else:
            return False
    last = parts[-1]
    if isinstance(parent, dict) and last in parent:
        del parent[last]
        return True
    if isinstance(parent, list) and last.isdigit() and int(last) < len(parent):
        del parent[int(last)]
        return True
    return False


# --- walking -------------------------------------------------------------

def leaves(tree, prefix=''):
    """Yield ``(path, value)`` for every scalar leaf, in file order.

    A list of scalars yields one entry per index, which is what makes
    ``prompts.random_names.names`` (1,000 entries) addressable one at a time.
    """
    if isinstance(tree, dict):
        for key, value in tree.items():
            child = f'{prefix}.{key}' if prefix else str(key)
            yield from leaves(value, child)
    elif isinstance(tree, list):
        for index, value in enumerate(tree):
            child = f'{prefix}.{index}' if prefix else str(index)
            yield from leaves(value, child)
    else:
        yield prefix, tree


def sections(tree):
    """The top-level keys, in file order, with a leaf count for each."""
    out = []
    for key, value in tree.items():
        out.append({'key': str(key), 'leaves': sum(1 for _ in leaves(value, str(key)))})
    return out


def is_secret(path):
    """Whether `path` names a secret leaf."""
    return path.rsplit('.', 1)[-1].lower() in SECRET_LEAF_NAMES


def redact(tree):
    """A copy of `tree` with every secret leaf replaced by :data:`REDACTED`.

    This is what a route sends. The tree itself keeps the real value, because a save writes it
    back; only the browser is kept in the dark.
    """
    out = copy.deepcopy(dict(tree))
    for path, _value in list(leaves(out)):
        if is_secret(path):
            set_value(out, path, REDACTED)
    return out


def merge_secrets(incoming, stored):
    """Put the stored secret back wherever `incoming` still holds :data:`REDACTED`.

    A masked field the user did not touch must not blank the stored key. Without this, every save
    from the editor would wipe every API key, which is the one mistake that would make the editor
    worse than editing the file by hand.
    """
    merged = copy.deepcopy(dict(incoming))
    for path, value in list(leaves(merged)):
        if not is_secret(path):
            continue
        if value != REDACTED:
            continue
        original = get(stored, path, None)
        if original is not None:
            set_value(merged, path, original)
    return merged


def kind_of(value):
    """The widget kind a value should render as, inferred from its type.

    This is what lets a leaf with **no schema entry** still render correctly. A missing schema
    entry degrades to a generic field, never to a missing one.
    """
    if isinstance(value, bool):
        return 'bool'
    if isinstance(value, (int, float)):
        return 'number'
    if isinstance(value, list):
        return 'list'
    if isinstance(value, dict):
        return 'group'
    if value is None:
        return 'text'
    text = str(value)
    if '\n' in text or len(text) > 120:
        return 'textarea'
    return 'text'
