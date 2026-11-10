# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""A YAML writer that keeps the bytes it did not change.

Why this exists
---------------
``yaml.dump`` cannot round-trip ``config/config.yml``. Measured on the shipped file: a
load-then-dump cycle **deletes all 113 comment lines** -- including the SPDX header, which
``AGENTS.md`` requires on every text file -- and re-wraps 122 lines of prose because PyYAML
re-flows at its own width. That is a 160-line diff for a save that changed nothing.

``ruamel.yaml`` in round-trip mode keeps the comments, but it still re-wraps long scalars
(measured: 97 lines of diff at its default width, 160 at ``width=10**9``) and it is a new
dependency for a project that has none.

So the writer here does the opposite of a dump: it **keeps the original text and splices
only the spans that changed.** An unchanged leaf is never re-rendered, so an unchanged save is
byte-identical by construction rather than by luck. A changed leaf is re-rendered at its own
column with its own continuation indent, so the surrounding file does not move.

What it can and cannot do
------------------------
* **Change a leaf** -- spliced in place.
* **Add a leaf** -- inserted after the last entry of its parent block, or after the deepest
  existing ancestor when the whole parent is new.
* **Delete a leaf** -- its lines are removed.
* **Re-order nothing.** Key order is whatever the file already had.

The one thing it cannot do is preserve a comment *inside* a value that was itself changed;
that comment belongs to the old text of that value and goes with it. Nothing else is touched.
"""

import copy

import yaml

# A line is wrapped at this column, counting the indent, matching the shipped file's own
# wrapping closely enough that a changed value does not look out of place.
WRAP_WIDTH = 80


class ConfigTextError(Exception):
    """A document that cannot be parsed or spliced."""


class LeafSpan:
    """Where one scalar leaf lives in the source text.

    ``key_start`` is the first character of the value, ``value_start``/``value_end``
    bracket the value itself. ``key_column`` is the value's column, which is what a wrapped
    continuation line is indented to. ``end_line`` is the index of the last line the value
    occupies, so a delete can take whole lines.

    ``owning_key_start`` is the first character of the **key** that owns this value, which is not
    the same thing: for a scalar the value begins after the colon, so `key_start` is past the key
    token. A value that has to be re-rendered as a block hanging off its key -- a null scalar
    grown into a list or a mapping -- has to start from the key, not from the empty value, and its
    children have to be indented to ``owning_key_column``, not to the value's own column.
    """

    __slots__ = ('key_start', 'value_start', 'value_end', 'key_column',
                 'key_line', 'end_line', 'owning_key_start', 'owning_key_column')

    def __init__(self, key_start, value_start, value_end, key_column, key_line,
                 end_line, owning_key_start=None, owning_key_column=None):
        self.key_start = key_start
        self.value_start = value_start
        self.value_end = value_end
        self.key_column = key_column
        self.key_line = key_line
        self.end_line = end_line
        self.owning_key_start = key_start if owning_key_start is None else owning_key_start
        self.owning_key_column = (key_column if owning_key_column is None
                                  else owning_key_column)


class NodeSpan:
    """Where a mapping or sequence lives, for inserting a new child into it.

    ``key_column`` is the indentation of the **key that owns this value**, not of the value
    itself, because that is the column a block sequence's items must line up with.
    ``value_start``/``value_end`` bracket the value, so a sequence whose length changed can be
    re-rendered as a whole instead of grown one item at a time. ``key_start`` is the first
    character of the owning key, which is what a replacement has to start from when the old value
    did not occupy a line of its own.
    """

    __slots__ = ('kind', 'key_column', 'start_line', 'end_line',
                 'value_start', 'value_end', 'inline', 'key_start')

    def __init__(self, kind, key_column, start_line, end_line,
                 value_start=None, value_end=None, inline=False, key_start=None):
        self.kind = kind
        self.key_column = key_column
        self.start_line = start_line
        self.end_line = end_line
        self.value_start = value_start
        self.value_end = value_end
        self.inline = inline
        self.key_start = key_start


# --- line arithmetic -----------------------------------------------------

def _line_start(text, index):
    """The index of the first character of the line containing `index`."""
    return text.rfind('\n', 0, index) + 1


def _line_end(text, index):
    """The index just past the newline of the line containing `index`."""
    newline = text.find('\n', index)
    return len(text) if newline < 0 else newline + 1


def _indent_of(text, index):
    """How many leading spaces the line containing `index` has."""
    start = _line_start(text, index)
    column = 0
    while start + column < len(text) and text[start + column] == ' ':
        column += 1
    return column


# --- parsing ------------------------------------------------------------

def parse(text):
    """Return ``(tree, leaves, nodes)`` for `text`.

    ``tree`` is a plain dict. ``leaves`` maps a dot path to a :class:`LeafSpan` and
    ``nodes`` maps a container path to a :class:`NodeSpan`. Both are keyed the same way
    ``config_tree.leaves`` walks, so a path means the same thing everywhere.
    """
    try:
        root = yaml.compose(text)
    except yaml.YAMLError as exc:
        raise ConfigTextError(f'Not valid YAML: {exc}') from None
    try:
        tree = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigTextError(f'Not valid YAML: {exc}') from None
    tree = {} if tree is None else tree

    leaves = {}
    nodes = {}

    def walk(node, path, key_column, key_start):
        if isinstance(node, yaml.ScalarNode):
            leaves[path] = LeafSpan(
                key_start=node.start_mark.index,
                value_start=node.start_mark.index,
                value_end=node.end_mark.index,
                key_column=node.start_mark.column,
                key_line=node.start_mark.line,
                end_line=node.end_mark.line,
                owning_key_start=key_start,
                owning_key_column=key_column,
            )
            return
        if isinstance(node, yaml.MappingNode):
            # A flow mapping (`{p: q}`) starts at the `{`, so its own column is the value's,
            # not the indent a child key must use. A block mapping starts at its first child
            # key, which *is* that indent. `inline` marks the flow shape, where a block render
            # has to start on a new line under the owning key rather than splice in place.
            flow = bool(node.flow_style)
            nodes[path] = NodeSpan('mapping',
                                   key_column + 2 if flow else node.start_mark.column,
                                   node.start_mark.line, node.end_mark.line,
                                   node.start_mark.index, node.end_mark.index,
                                   inline=flow, key_start=key_start)
            for key, value in node.value:
                child = f'{path}.{key.value}' if path else str(key.value)
                walk(value, child, key.start_mark.column, key.start_mark.index)
            return
        if isinstance(node, yaml.SequenceNode):
            # A sequence's items line up with the key that owns it, so the owning key's
            # column is what a re-render needs. `inline` marks a flow sequence (`[]`,
            # `[a, b]`) or one written on the key's own line, where a block render has to
            # start on a new line rather than replace the value in place.
            nodes[path] = NodeSpan('sequence', key_column,
                                   node.start_mark.line, node.end_mark.line,
                                   node.start_mark.index, node.end_mark.index,
                                   inline=bool(node.flow_style)
                                   or node.start_mark.column != key_column,
                                   key_start=key_start)
            for index, value in enumerate(node.value):
                child = f'{path}.{index}' if path else str(index)
                walk(value, child, key_column, key_start)

    if root is not None:
        walk(root, '', 0, 0)
    return tree, leaves, nodes


# --- rendering one value ------------------------------------------------

def render_value(value, column, continuation_indent):
    """Render `value` so it reads back identically when placed at `column`.

    The first line has no indent of its own -- the caller has already written the key and
    the space after the colon. Continuation lines are indented to
    `continuation_indent`, which is what the shipped file does for a wrapped scalar.
    """
    width = max(40, WRAP_WIDTH - column)
    dumped = yaml.dump({'_': value}, default_flow_style=False, width=width,
                       indent=2, allow_unicode=True, sort_keys=False)
    body = dumped[len('_: '):]
    if body.endswith('\n'):
        body = body[:-1]
    lines = body.split('\n')
    if len(lines) == 1:
        return lines[0]
    # PyYAML indents a nested value by two relative to the key; move it to where the
    # file actually puts it.
    return lines[0] + ''.join('\n' + ' ' * continuation_indent + line[2:]
                              for line in lines[1:])


def render_block(key, value, indent):
    """Render ``key: value`` as a block at `indent`, newline-terminated.

    Used to insert a key that did not exist before, and to insert a whole new subtree when
    its parent did not exist either.
    """
    dumped = yaml.dump({key: value}, default_flow_style=False,
                       width=max(40, WRAP_WIDTH - indent), indent=2,
                       allow_unicode=True, sort_keys=False)
    if not dumped.endswith('\n'):
        dumped += '\n'
    prefix = ' ' * indent
    return ''.join(prefix + line if line.strip() else line
                   for line in dumped.splitlines(keepends=True))


def render_sequence_item(value, indent):
    """Render one list item as a block at `indent`, newline-terminated."""
    dumped = yaml.dump([value], default_flow_style=False,
                       width=max(40, WRAP_WIDTH - indent), indent=2,
                       allow_unicode=True, sort_keys=False)
    if not dumped.endswith('\n'):
        dumped += '\n'
    prefix = ' ' * indent
    return ''.join(prefix + line if line.strip() else line
                   for line in dumped.splitlines(keepends=True))


def render_sequence_block(values, indent):
    """Render a whole list as block items at `indent`, each newline-terminated.

    Used when a list's **length** changed. Growing a list one item at a time cannot work for
    every shape the file can hold: the shipped `datasets.sources` is `sources: []`, a flow
    sequence, and inserting an item after it produced

        datasets:
                   - source_id: ...
          sources: []

    which is not valid YAML at all -- the first dataset added through the editor would have
    corrupted `config.yml`. Re-rendering the value as a block is the one shape that is correct
    whether the old value was `[]`, `[a, b]`, or an empty key with nothing after it.
    """
    if not values:
        return '[]'
    dumped = yaml.dump(list(values), default_flow_style=False,
                       width=max(40, WRAP_WIDTH - indent), indent=2,
                       allow_unicode=True, sort_keys=False)
    if not dumped.endswith('\n'):
        dumped += '\n'
    prefix = ' ' * indent
    return ''.join(prefix + line if line.strip() else line
                   for line in dumped.splitlines(keepends=True))


def render_mapping_block(values, indent):
    """Render a whole mapping as block keys at `indent`, each newline-terminated.

    The sibling of :func:`render_sequence_block`, and needed for the same reason. Growing a
    mapping one key at a time cannot work for every shape the file can hold, because the old
    value may not have owned a line of its own:

    * `column_aliases: {}` is a **flow** mapping. Inserting a key after it put the new key at the
      flow mapping's own column -- which is the column of the `{`, not of a child -- and left the
      `{}` in place, producing

          column_aliases: {}
                          a: first

      which is not valid YAML. `column_aliases: {}` is the shipped default on **every** new
      source, so mapping the first alias would have corrupted `config.yml`.
    * `column_aliases:` with nothing after it is a **null scalar**, not a mapping, so the key has
      no child lines to append to at all.

    Re-rendering the value as a block is the one shape that is correct whether the old value was
    `{}`, `{p: q}`, or an empty key with nothing after it.
    """
    if not values:
        return '{}'
    dumped = yaml.dump(dict(values), default_flow_style=False,
                       width=max(40, WRAP_WIDTH - indent), indent=2,
                       allow_unicode=True, sort_keys=False)
    if not dumped.endswith('\n'):
        dumped += '\n'
    prefix = ' ' * indent
    return ''.join(prefix + line if line.strip() else line
                   for line in dumped.splitlines(keepends=True))


# --- splicing -----------------------------------------------------------

def _leaf_edits(text, tree, original_tree, leaves, nodes):
    """The edits that change, add or remove leaves, as ``(start, end, text)``."""
    # A sequence whose length changed is re-rendered whole, and nothing inside it is edited
    # separately -- otherwise an item would be both re-rendered and appended.
    regrown = _regrown_sequences(tree, original_tree, nodes)
    # A mapping whose **key set** changed is re-rendered whole for the same reason: a key added
    # to a flow mapping (`{}`, `{p: q}`) or to an empty value has no child line to splice into.
    # A mapping whose keys are unchanged is still spliced leaf-by-leaf, which is the case that
    # already works and must not regress.
    regrown += _regrown_mappings(tree, original_tree, nodes)
    # A leaf that became a list or a mapping (`sources:` / `column_aliases:` with no value parses
    # as a null scalar) is the same situation: the whole value is replaced, so its new children
    # must not also be inserted.
    converted = _converted_leaves(tree, original_tree, leaves)
    # A container that became a **scalar**, or that vanished, is invisible to both loops above:
    # the leaf loop only walks leaves (this path is a node in the original text) and the regrow
    # helpers only fire when the new value is still a list or a mapping. Without this pass the
    # edit was silently dropped -- `visible_columns: []` set back to `None` stayed `[]`, and
    # `[1, 2]` set to a string left the old items' text behind as stray YAML.
    collapsed = _collapsed_nodes(tree, original_tree, nodes, regrown)

    def covered(path):
        # Strictly *inside* one, not the path itself: a converted leaf is still a leaf, and it is
        # the leaf loop that has to re-render it.
        return any(path.startswith(seq + '.')
                   for seq in list(regrown) + list(converted) + list(collapsed))

    edits = []
    for path, span in leaves.items():
        if covered(path):
            continue
        new = _lookup(tree, path)
        old = _lookup(original_tree, path)
        if new is _MISSING:
            start = _line_start(text, span.key_start)
            end = _line_end(text, max(span.value_end - 1, span.value_start))
            edits.append((start, end, ''))
            continue
        if new == old and type(new) is type(old):
            continue
        if isinstance(new, list) and not isinstance(old, list):
            # A key written with no value at all (`sources:`) parses as a null scalar, not as a
            # sequence, so it is a leaf here rather than a node. Growing it into a list still
            # has to be rendered as a block hanging off its key.
            body = render_sequence_block(new, span.owning_key_column)
            if not new:
                edits.append((span.owning_key_start, span.value_end,
                              _key_text(text, span.owning_key_start, span.value_end) + ' []'))
            else:
                edits.append((span.owning_key_start, span.value_end,
                              _key_text(text, span.owning_key_start, span.value_end)
                              + '\n' + body.rstrip('\n')))
            continue
        if isinstance(new, dict) and not isinstance(old, dict):
            # The mapping case, and the one that ships on every new source: `column_aliases:`
            # parses as a null scalar, so it is a leaf here. Same treatment as the list above.
            body = render_mapping_block(new, span.owning_key_column + 2)
            if not new:
                edits.append((span.owning_key_start, span.value_end,
                              _key_text(text, span.owning_key_start, span.value_end) + ' {}'))
            else:
                edits.append((span.owning_key_start, span.value_end,
                              _key_text(text, span.owning_key_start, span.value_end)
                              + '\n' + body.rstrip('\n')))
            continue
        replacement = render_value(new, span.key_column + 2, span.key_column + 2)
        edits.append((span.value_start, span.value_end, replacement))

    for path in regrown:
        span = nodes[path]
        if span.kind == 'mapping':
            edits.append(_mapping_edit(text, tree, span, path))
        else:
            edits.append(_sequence_edit(text, tree, span, path))

    for path in collapsed:
        edits.append(_collapse_edit(text, tree, span_for(nodes, path), path))

    for path in _added_paths(tree, original_tree):
        if covered(path):
            continue
        edits.append(_insertion(text, tree, nodes, path))
    return [edit for edit in edits if edit is not None]


def _sequence_edit(text, tree, span, path):
    """Replace a whole sequence with its new contents.

    Three shapes, because the old value may not have owned a line of its own:

    * **Block sequence** (`sources:` then `- x: 1` lines) -- replaced from the first character of
      its first item's line to the first character of the line after its last. The surrounding
      newlines are already right, so the replacement only supplies its own items.
    * **Flow or inline sequence** (`sources: []`, `[a, b]`, or one opened on the key's own line) --
      replaced from the owning key, because the block form has to start on a new line and the old
      value's trailing space has to go with it.
    * **Empty value** (`sources:` with nothing after it) -- replaced from the owning key too, for
      the same reason: there is no value text to splice into.
    """
    values = _lookup(tree, path)
    body = render_sequence_block(values, span.key_column)
    if not values:
        # Collapse to `key: []`. The old items occupied lines the key does not, so the
        # replacement has to start at the key or the `[]` lands on a line of its own. It has to
        # end past the last item's newline, or the next key is glued onto the `[]`.
        #
        # A **block** sequence ends at the start of the line after its last item, not at the end
        # of the line holding its last character: `value_end` points at the next key, so using it
        # deleted the key that followed the sequence.
        if span.inline or span.value_start == span.value_end:
            end = _line_end(text, max(span.value_end - 1, span.value_start))
        else:
            end = _index_of_line(text, span.end_line)
        return (span.key_start, end,
                _key_text(text, span.key_start, span.value_end) + ' []\n')
    if span.inline or span.value_start == span.value_end:
        start = span.key_start if span.key_start is not None else span.value_start
        return (start, span.value_end, _block_from_key(text, start, span, body))
    start = _index_of_line(text, span.start_line)
    end = _index_of_line(text, span.end_line)
    return (start, end, body)


def _mapping_edit(text, tree, span, path):
    """Replace a whole mapping with its new contents.

    The mapping sibling of :func:`_sequence_edit`, with the same three shapes, because the old
    value may not have owned a line of its own:

    * **Block mapping** (`column_aliases:` then `  p: q` lines) -- replaced from the first
      character of its first child key's line to the first character of the line after its last.
      The surrounding newlines are already right, so the replacement only supplies its own keys.
    * **Flow or inline mapping** (`column_aliases: {}`, `{p: q}`) -- replaced from the owning key,
      because the block form has to start on a new line and the old value's text has to go with it.
    * **Empty value** (`column_aliases:` with nothing after it) -- replaced from the owning key
      too, for the same reason: there is no value text to splice into.
    """
    values = _lookup(tree, path)
    body = render_mapping_block(values, span.key_column)
    if not values:
        # Collapse to `key: {}`. The old children occupied lines the key does not, so the
        # replacement has to start at the key or the `{}` lands on a line of its own. It has to
        # end past the last child's newline, or the next key is glued onto the `{}`.
        #
        # A **block** mapping ends at the start of the line after its last child, for the same
        # reason as `_sequence_edit`.
        start = span.key_start if span.key_start is not None else span.value_start
        if span.inline or span.value_start == span.value_end:
            end = _line_end(text, max(span.value_end - 1, span.value_start))
        else:
            end = _index_of_line(text, span.end_line)
        return (start, end,
                _key_text(text, start, span.value_end) + ' {}\n')
    if span.inline or span.value_start == span.value_end:
        start = span.key_start if span.key_start is not None else span.value_start
        return (start, span.value_end, _block_from_key(text, start, span, body))
    start = _index_of_line(text, span.start_line)
    end = _index_of_line(text, span.end_line)
    return (start, end, body)


def _collapse_edit(text, tree, span, path):
    """Replace a whole container with a scalar, or remove it.

    The third shape a container can end in, after "re-rendered" and "emptied": the new value is
    no longer a list or a mapping at all. Two cases, and both were silently dropped before this
    existed:

    * **Gone** -- the key was deleted. The whole span goes, from the first character of the key's
      line to the first character of the line after the container's last line. Taking only the
      key's own line would leave the children behind as orphaned YAML.
    * **A scalar** -- `[1, 2]` became `'gone'`. The value is re-rendered in place of the whole
      container, again from the key's line, because a block sequence's items occupy lines the key
      does not.

    A **block** container is removed line-wise; a **flow** one (`[]`, `{p: q}`) is spliced from the
    key, because it shares a line with the key and removing the line would remove the key too.

    A block container that becomes a scalar starts at ``key_start``, **not** at the line start: the
    leading indentation belongs to the key and has to survive, or the replacement lands at column 0
    and detaches from its parent. A *deletion* does start at the line start, because the whole line
    has to go.

    The end of a block container is the **start of the line after its last line**, not the end of
    the line containing its last character: `value_end` points at the first character *after* the
    container, which is the next key, so using it ate the following key's line.
    """
    new = _lookup(tree, path)
    key_start = span.key_start if span.key_start is not None else span.value_start
    flow = span.inline or span.value_start == span.value_end
    replacement = render_value(new, span.key_column + 2, span.key_column + 2)
    if new is _MISSING:
        if flow:
            # A flow container shares a line with its key, so only the value goes -- and the rest
            # of that line (the newline) goes with it, or the deletion leaves a blank line behind.
            return (_line_start(text, key_start),
                    _line_end(text, max(span.value_end - 1, span.value_start)), '')
        return (_line_start(text, key_start),
                _index_of_line(text, span.end_line), '')
    if flow:
        return (key_start, span.value_end,
                _key_text(text, key_start, span.value_end) + ' ' + replacement)
    return (key_start, _index_of_line(text, span.end_line),
            _key_text(text, key_start, span.value_end) + ' ' + replacement + '\n')


def _collapsed_nodes(tree, original_tree, nodes, already=()):
    """The outermost containers whose new value is a scalar or is gone entirely.

    Outermost only, for the same reason as :func:`_regrown_sequences`: a nested container inside
    one already being replaced would be written twice, and the inner one's offsets are inside the
    outer one's span.

    `already` is the paths the regrow helpers have claimed. Their **children** are gone from the
    new tree whenever the container was emptied, so without this they would look "collapsed" and be
    deleted a second time -- which is how emptying a list of mappings produced an edit that removed
    the key's own line and left the file unparseable.

    A container whose new value is still a list or a mapping is **not** here -- that is the regrow
    helpers' job, and they know how to keep the comments inside it.
    """
    claimed = list(already)
    changed = []
    for path, span in nodes.items():
        if not path:
            continue
        if any(path == other or path.startswith(other + '.') for other in claimed):
            continue
        new = _lookup(tree, path)
        if new is _MISSING or not isinstance(new, (list, dict)):
            changed.append(path)
    changed.sort(key=lambda path: path.count('.'))
    outermost = []
    for path in changed:
        if any(path.startswith(other + '.') for other in outermost):
            continue
        outermost.append(path)
    return outermost


def span_for(nodes, path):
    """The `NodeSpan` for `path`, raising a clear error rather than a `KeyError`."""
    try:
        return nodes[path]
    except KeyError:
        raise ConfigTextError(f'No span for {path!r}.') from None


def _key_text(text, key_start, value_end):
    """The owning key's own text, colon included, exactly as the file wrote it."""
    colon = text.find(':', key_start, value_end)
    return text[key_start:colon + 1] if colon >= 0 else text[key_start:value_end].rstrip()


def _block_from_key(text, key_start, span, body):
    """Re-emit `key:` followed by a block sequence, for a value that had no line of its own.

    The key's own text is kept verbatim -- only the value after its colon is replaced -- so a
    quoted key or a key with unusual spacing survives.
    """
    colon = text.find(':', key_start, span.value_end)
    if colon < 0:
        return body
    key_text = text[key_start:colon + 1]
    if body.startswith('[]'):
        return key_text + ' []'
    return key_text + '\n' + body.rstrip('\n')


def _converted_leaves(tree, original_tree, leaves):
    """Leaves in the original text whose new value is a list or a mapping.

    `sources:` and `column_aliases:` with nothing after them are null scalars to the parser but a
    sequence and a mapping to the caller, so they are leaves here and nodes after the edit. Their
    whole value is re-rendered, and their new children must not also be appended by `_insertion`.
    """
    out = []
    for path in leaves:
        new = _lookup(tree, path)
        old = _lookup(original_tree, path)
        if isinstance(new, (list, dict)) and not isinstance(old, (list, dict)):
            out.append(path)
    return out


def _regrown_sequences(tree, original_tree, nodes):
    """The outermost sequences whose length differs from the original text.

    Outermost only: a nested sequence inside one that is already being re-rendered would be
    written twice, and the inner one's offsets are inside the outer one's span.
    """
    changed = []
    for path, span in nodes.items():
        if span.kind != 'sequence':
            continue
        new = _lookup(tree, path)
        old = _lookup(original_tree, path)
        if not isinstance(new, list):
            continue
        if isinstance(old, list) and len(old) == len(new):
            continue
        changed.append(path)
    changed.sort(key=lambda path: path.count('.'))
    outermost = []
    for path in changed:
        if any(path.startswith(other + '.') for other in outermost):
            continue
        outermost.append(path)
    return outermost


def _regrown_mappings(tree, original_tree, nodes):
    """The outermost **flow** mappings whose key set differs from the original text.

    Only a flow mapping (`{}`, `{p: q}`) is regrown, plus a block mapping that has been **emptied**.
    Any other block mapping is left alone even when its key set changes, because the existing
    machinery already handles it correctly and *keeps the comments inside it*: an added key is
    spliced in by :func:`_insertion` after the last sibling, and a deleted key is removed by the
    leaf loop. Re-rendering a block mapping whole would delete every comment in it -- which is
    exactly what `yaml.dump` does, and the reason this module exists.
    `test_deleting_a_key_removes_it_and_nothing_else` pins that.

    A block mapping emptied to `{}` is the exception, and it has to be regrown: deleting its last
    child leaves the bare key `column_aliases:` behind, which parses as **None**, not as `{}`. The
    model rejects that (`"Column aliases must be an object"`), so clearing the last alias would
    have written a config the panel could no longer read. No comment can be lost here, because the
    mapping is being emptied.

    Keyed on the key **set** rather than the length, because a rename is a change too.

    Outermost only, for the same reason as :func:`_regrown_sequences`: a nested mapping inside one
    already being re-rendered would be written twice, and the inner one's offsets are inside the
    outer one's span.
    """
    changed = []
    for path, span in nodes.items():
        if span.kind != 'mapping':
            continue
        new = _lookup(tree, path)
        old = _lookup(original_tree, path)
        if not isinstance(new, dict) or not isinstance(old, dict):
            continue
        emptied = bool(old) and not new
        if not span.inline and not emptied:
            continue
        if set(old.keys()) == set(new.keys()):
            continue
        changed.append(path)
    changed.sort(key=lambda path: path.count('.'))
    outermost = []
    for path in changed:
        if any(path.startswith(other + '.') for other in outermost):
            continue
        outermost.append(path)
    return outermost


def _insertion(text, tree, nodes, path):
    """Where and what to insert for a path that is not in the original text."""
    parent = path.rsplit('.', 1)[0] if '.' in path else ''
    key = path.rsplit('.', 1)[-1]
    value = _lookup(tree, path)

    if parent in nodes:
        span = nodes[parent]
        anchor = _index_of_line(text, span.end_line)
        if span.kind == 'sequence':
            return (anchor, anchor, render_sequence_item(value, span.key_column))
        return (anchor, anchor, render_block(key, value, span.key_column))

    # The parent is new too: find the deepest ancestor that does exist and hang the
    # whole new subtree off the end of it.
    ancestor = parent
    while ancestor and ancestor not in nodes:
        ancestor = ancestor.rsplit('.', 1)[0] if '.' in ancestor else ''
    if ancestor not in nodes:
        return None
    span = nodes[ancestor]
    anchor = _index_of_line(text, span.end_line)
    remainder = path[len(ancestor) + 1:] if ancestor else path
    subtree = _lookup(tree, ancestor)
    for part in remainder.split('.'):
        subtree = subtree[int(part)] if isinstance(subtree, list) else subtree[part]
    if span.kind == 'sequence':
        return (anchor, anchor, render_sequence_item(subtree, span.key_column))
    return (anchor, anchor, render_block(remainder.split('.')[0], subtree,
                                        span.key_column))


def _index_of_line(text, line):
    """The character index where line number `line` starts."""
    index = 0
    for _ in range(line):
        newline = text.find('\n', index)
        if newline < 0:
            return len(text)
        index = newline + 1
    return index


_MISSING = object()


def _lookup(tree, path):
    value = tree
    for part in path.split('.'):
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            return _MISSING
    return value


def _added_paths(tree, original_tree, prefix=''):
    """Every path in `tree` that `original_tree` does not have."""
    out = []
    if isinstance(tree, dict):
        for key, value in tree.items():
            child = f'{prefix}.{key}' if prefix else str(key)
            if not isinstance(original_tree, dict) or key not in original_tree:
                out.append(child)
                continue
            out.extend(_added_paths(value, original_tree[key], child))
    elif isinstance(tree, list):
        for index, value in enumerate(tree):
            child = f'{prefix}.{index}' if prefix else str(index)
            if not isinstance(original_tree, list) or index >= len(original_tree):
                out.append(child)
                continue
            out.extend(_added_paths(value, original_tree[index], child))
    return out


def splice(text, tree, original_tree):
    """Return `text` with every difference between `tree` and `original_tree` applied.

    Edits are applied from the end of the file backwards, so an earlier edit never moves a
    later one's offsets.
    """
    _tree, leaves, nodes = parse(text)
    edits = _leaf_edits(text, tree, original_tree, leaves, nodes)
    if not edits:
        return text
    out = text
    for start, end, replacement in sorted(edits, key=lambda e: e[0], reverse=True):
        out = out[:start] + replacement + out[end:]
    return out


def dumps(tree, original_text=None, original_tree=None):
    """Serialise `tree`, keeping `original_text` wherever nothing changed.

    With no original text this is a plain ``yaml.dump`` -- correct, just not pretty. Every
    caller that has a file on disk should pass the text it loaded.
    """
    if original_text is None:
        return yaml.dump(tree, default_flow_style=False, indent=2,
                         allow_unicode=True, sort_keys=False)
    if original_tree is None:
        original_tree = parse(original_text)[0]
    return splice(original_text, tree, original_tree)


def copy_tree(tree):
    """A deep copy that keeps a dict subclass's own attributes."""
    return copy.deepcopy(tree)
