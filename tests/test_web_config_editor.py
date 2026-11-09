# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the Config editor's generic renderer -- Phase 6b.

Phase 6a proved the tree could round-trip without losing a byte. This file proves the
**renderer over that tree** meets the maintainer's requirement: *every* option can be
modified from the web editor, and new ones can be added, with no schema in existence yet.

The assertions, in order of how much they matter:

1. **Every leaf in the real `config/config.yml` reaches the browser exactly once.** This is the
   whole point of 6b. A curated form over 151 keys would leave ~1,140 leaves unreachable, which
   is what the requirement rules out. The test counts the payload's leaves against
   `config_tree.leaves(tree)` and fails on a single missing or duplicated path.
2. **No secret leaves the server.** All nine secret leaves arrive as `***` with `has_value`, and
   the real values appear nowhere in the serialised payload.
3. **A save writes what it was given, and only that.** One changed number is one changed line;
   the other 1,290 leaves and all 113 comments survive.
4. **A save cannot corrupt a type.** `"10"` for an int field is stored as `10`; `"abc"` is
   rejected with the field named and **nothing is written**.
5. **An untouched masked field does not blank a stored key.**
6. **Add works through the route**, not just through `config_tree.set_value`.
7. **The renderer has no type logic of its own** -- the kinds come from the server, so there is
   one implementation of "what is this value".

Everything runs against a **temporary copy** of the shipped config. The real file is read but
never written.
"""

import copy
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import webapp.config_routes as config_routes
import webapp.config_schema as config_schema
import webapp.config_tree as config_tree

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHIPPED = os.path.join(REPO, 'config', 'config.yml')
STATIC_JS = os.path.join(REPO, 'webapp', 'static', 'js')


# --- helpers -------------------------------------------------------------

class FakeConfig:
    """A stand-in for `ConfigLoader`: a path, a dict, and `get` / `load`."""

    def __init__(self, path):
        self.path = path
        self.config = config_tree.load(path)

    def get(self, path, default=None):
        return config_tree.get(self.config, path, default)

    def load(self):
        self.config = config_tree.load(self.path)


class sandbox:
    """Point `config_routes` at a temporary copy of the shipped config.

    `config_routes` reads `app_state.global_config.path`, so swapping that one module attribute
    redirects every route. The originals are restored on exit even if a test raises.
    """

    def __enter__(self):
        self._directory = tempfile.TemporaryDirectory()
        self.path = os.path.join(self._directory.name, 'config.yml')
        shutil.copyfile(SHIPPED, self.path)
        self._saved = config_routes.app_state
        self.state = type('FakeAppState', (), {'global_config': FakeConfig(self.path)})()
        config_routes.app_state = self.state
        return self

    def __exit__(self, *exc_info):
        config_routes.app_state = self._saved
        self._directory.cleanup()
        return False

    def read(self):
        with open(self.path, 'r', encoding='utf-8') as handle:
            return handle.read()

    def tree(self):
        return config_tree.load(self.path)


def _app():
    """A FastAPI app with only the config routes, so no auth gate or engine is needed."""
    from fastapi import FastAPI

    return config_routes.install_config(FastAPI())


def _routes(app):
    """`(path, methods)` for every route, walking into included routers."""
    found = {}
    pending = list(app.routes)
    while pending:
        route = pending.pop()
        original = getattr(route, 'original_router', None)
        if original is not None:
            pending.extend(original.routes)
            continue
        path = getattr(route, 'path', None)
        if path:
            found.setdefault(path, set()).update(getattr(route, 'methods', None) or set())
    return found


def _find(app, path, method):
    """The endpoint callable for `path` + `method`."""
    pending = list(app.routes)
    while pending:
        route = pending.pop()
        original = getattr(route, 'original_router', None)
        if original is not None:
            pending.extend(original.routes)
            continue
        if getattr(route, 'path', None) == path:
            if method in (getattr(route, 'methods', None) or set()):
                return route.endpoint
    raise AssertionError(f'no route {method} {path}')


def _get(path='/api/config'):
    return _find(_app(), path, 'GET')()


class FakeRequest:
    """Just enough of a `Request` for `await request.json()`."""

    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        if self._payload is _NOT_JSON:
            raise ValueError('not json')
        return self._payload


_NOT_JSON = object()


def _post(path, payload):
    """Call a POST route with `payload` as its JSON body."""
    import asyncio

    endpoint = _find(_app(), path, 'POST')
    return asyncio.run(endpoint(FakeRequest(payload)))


def _body(response):
    """The decoded JSON body of a `JSONResponse`, or the dict a route returned."""
    if isinstance(response, dict):
        return response
    return json.loads(response.body)


def _walk(node, out):
    """Collect `(path, kind)` for every leaf-ish node in a payload tree.

    A `list` node stands for **every index of that list**, because that is the whole point of
    collapsing a flat list into one control: `prompts.random_names.names` is one node covering
    1,000 leaves. So the expansion here is what the payload genuinely covers, not a shortcut.
    """
    if node.get('kind') == 'group':
        for child in node.get('children') or []:
            _walk(child, out)
        return
    if node.get('kind') == 'list':
        for index in range(len(node.get('items') or [])):
            out.append((f"{node['path']}.{index}", 'list-item'))
        # An empty list covers no leaf -- `config_tree.leaves` yields nothing for it -- but it
        # still renders a control, which `test_an_empty_list_still_renders` asserts.
        return
    out.append((node['path'], node['kind']))


def _payload_leaves(payload):
    """Every leaf path in a `/api/config` payload, with its kind."""
    out = []
    for node in payload.get('nodes') or []:
        _walk(node, out)
    return out


# --- 1. every leaf reaches the browser -----------------------------------

def test_every_leaf_in_the_real_config_reaches_the_payload():
    """The 6b requirement: no leaf is unreachable, schema or not."""
    with sandbox():
        payload = _get()
    assert payload['ok'], payload

    tree = config_tree.load(SHIPPED)
    expected = {path for path, _value in config_tree.leaves(tree)}
    got = {path for path, _kind in _payload_leaves(payload)}

    missing = expected - got
    extra = got - expected
    assert not missing, f'{len(missing)} leaves never reach the browser: {sorted(missing)[:5]}'
    assert not extra, f'{len(extra)} payload leaves are not in the file: {sorted(extra)[:5]}'
    assert len(got) == len(expected), (len(got), len(expected))


def test_the_leaf_count_matches_the_tree():
    with sandbox():
        payload = _get()
    tree = config_tree.load(SHIPPED)
    assert payload['leaf_count'] == sum(1 for _ in config_tree.leaves(tree)), payload['leaf_count']


def test_no_leaf_is_sent_twice():
    """A duplicated path would render two controls writing one key."""
    with sandbox():
        payload = _get()
    paths = [path for path, _kind in _payload_leaves(payload)]
    duplicates = {path for path in paths if paths.count(path) > 1}
    assert not duplicates, f'duplicated paths: {sorted(duplicates)[:5]}'


def test_the_twelve_sections_are_all_present():
    with sandbox():
        payload = _get()
    keys = [section['key'] for section in payload['sections']]
    tree = config_tree.load(SHIPPED)
    assert keys == list(tree.keys()), (keys, list(tree.keys()))
    assert len(keys) == 12, keys


def test_every_section_has_a_node_and_a_label():
    with sandbox():
        payload = _get()
    node_paths = {node['path'] for node in payload['nodes']}
    for section in payload['sections']:
        assert section['key'] in node_paths, section['key']
        assert section['label'], section['key']
        assert section['label'] != section['key'] or section['key'].isupper(), section


def test_a_leaf_with_no_schema_entry_still_gets_a_kind():
    """A leaf's kind comes from the value's own type, or from the schema.

    Phase 6b asserted the kind was *always* `kind_of`. Phase 6c adds a schema that may refine a
    **scalar** leaf's kind -- a long prompt body becomes a `textarea` even when its length alone
    would not say so, and a fixed-choice string becomes an `enum`. The contract is therefore: a
    leaf's kind is `kind_of` **or** a schema refinement of it, and never anything else.
    """
    with sandbox():
        payload = _get()
    kinds = {kind for _path, kind in _payload_leaves(payload)}
    assert kinds <= {'text', 'textarea', 'number', 'bool', 'list-item', 'secret', 'enum'}, kinds
    # The real file has every one of those shapes, so all of them must appear.
    assert kinds == {'text', 'textarea', 'number', 'bool', 'list-item', 'secret', 'enum'}, kinds
    # And the two node kinds that are not leaves still exist in the payload.
    # `gender` is a bare string at the top level, so a section node may itself be a leaf.
    node_kinds = {node['kind'] for node in payload['nodes']}
    assert node_kinds <= {'group', 'text', 'enum'}, node_kinds
    assert 'group' in node_kinds, node_kinds
    assert _has_kind(payload, 'list'), 'no list node in the payload'
    assert _has_kind(payload, 'secret'), 'no secret node in the payload'


def _has_kind(payload, kind):
    """Whether any node anywhere in the payload has `kind`."""
    def walk(node):
        if node.get('kind') == kind:
            return True
        return any(walk(child) for child in node.get('children') or [])
    return any(walk(node) for node in payload.get('nodes') or [])


def test_an_empty_list_still_renders_a_control():
    """`generation.repeat_opener_fixes` is `[]`; it must still be addable from the editor."""
    with sandbox():
        payload = _get()
    node = next(node for node in payload['nodes'] if node['path'] == 'generation')
    empty = _find_node(node, 'generation.repeat_opener_fixes')
    assert empty is not None, 'an empty list vanished from the payload'
    assert empty['kind'] == 'list', empty['kind']
    assert empty['items'] == [], empty['items']
    assert empty['count'] == 0, empty['count']


def test_the_kind_matches_kind_of_for_every_leaf():
    """One implementation of "what is this value": the server's, not a second one in JS.

    The schema may refine a **scalar** leaf's kind, so the assertion is that the kind is
    `kind_of` or the schema's refinement of it -- never a third thing, and never a refinement of
    a group, a flat list or a secret, whose shape the renderer and the save path agree on.
    """
    with sandbox():
        payload = _get()
    tree = config_tree.load(SHIPPED)
    for path, kind in _payload_leaves(payload):
        if kind == 'list-item':
            # A collapsed flat list: the kind is the list's, checked by the test below.
            continue
        if config_tree.is_secret(path):
            assert kind == 'secret', path
            continue
        value = config_tree.get(tree, path)
        if isinstance(value, list):
            assert kind == 'list', (path, kind)
            continue
        natural = config_tree.kind_of(value)
        if kind == natural:
            continue
        refined = config_schema.describe(path, natural).get('kind')
        assert kind == refined, (path, kind, natural, refined)
        # A refinement stays a scalar kind: it may turn a long body into a textarea, a fixed
        # choice into an enum, or an escaped separator back into a single-line text field. It
        # may never turn a leaf into a group, a list or a secret.
        assert kind in ('text', 'textarea', 'number', 'bool', 'enum'), (path, kind)


def test_the_thousand_name_list_is_one_node_not_a_thousand():
    """`prompts.random_names.names` is 1,000 entries; it must be one list control."""
    with sandbox():
        payload = _get()
    node = next(node for node in payload['nodes'] if node['path'] == 'prompts')
    names = _find_node(node, 'prompts.random_names.names')
    assert names is not None, 'the name list is missing from the payload'
    assert names['kind'] == 'list', names['kind']
    assert names['count'] == 1000, names['count']
    assert len(names['items']) == 1000, len(names['items'])


def _find_node(node, path):
    """Depth-first search for `path` in a payload node tree."""
    if node.get('path') == path:
        return node
    for child in node.get('children') or []:
        found = _find_node(child, path)
        if found is not None:
            return found
    return None


# --- 2. no secret reaches the browser ------------------------------------

def test_no_secret_value_appears_in_the_payload():
    """An API key must never be sent to a browser.

    The shipped config ships with no key set, so this test **sets one first** -- otherwise it
    would pass by having nothing to leak, which is the most dangerous kind of green.
    """
    with sandbox() as box:
        tree = box.tree()
        config_tree.set_value(tree, 'api.apis.0.key', 'sk-live-do-not-leak-0123456789')
        config_tree.set_value(tree, 'database.password', 'hunter2-not-in-a-page')
        config_tree.save(tree, box.path)

        secrets = [(path, value) for path, value in config_tree.leaves(box.tree())
                   if config_tree.is_secret(path) and value]
        assert len(secrets) >= 2, secrets
        payload = _get()

    serialised = json.dumps(payload)
    for path, value in secrets:
        assert str(value) not in serialised, f'{path} leaked into the payload'
    assert 'sk-live-do-not-leak' not in serialised
    assert 'hunter2-not-in-a-page' not in serialised


def test_every_secret_is_masked_and_reports_has_value():
    with sandbox() as box:
        tree = box.tree()
        expected = {path for path, _value in config_tree.leaves(tree)
                    if config_tree.is_secret(path)}
        payload = _get()

    got = {}
    for node in payload['nodes']:
        _collect_secrets(node, got)
    assert set(got) == expected, (sorted(got), sorted(expected))
    for path, node in got.items():
        assert node['value'] == config_tree.REDACTED, (path, node['value'])
        assert 'has_value' in node, path


def _collect_secrets(node, out):
    if node.get('kind') == 'secret':
        out[node['path']] = node
        return
    for child in node.get('children') or []:
        _collect_secrets(child, out)


def test_the_secret_count_is_reported():
    with sandbox():
        payload = _get()
    tree = config_tree.load(SHIPPED)
    expected = sum(1 for path, _value in config_tree.leaves(tree)
                   if config_tree.is_secret(path))
    assert payload['secret_count'] == expected, (payload['secret_count'], expected)


# --- 3. a save writes what it was given, and only that -------------------

def test_saving_one_number_changes_one_line():
    with sandbox() as box:
        before = box.read()
        response = _post('/api/config/save', {'values': {'generation.num_turns': 7}})
        body = _body(response)
        assert body['ok'], body
        assert body['data']['changed'] == ['generation.num_turns'], body['data']
        after = box.read()

    changed_lines = [(a, b) for a, b in zip(before.splitlines(), after.splitlines())
                     if a != b]
    assert len(before.splitlines()) == len(after.splitlines()), 'the line count moved'
    assert len(changed_lines) == 1, changed_lines
    assert 'num_turns: 7' in changed_lines[0][1], changed_lines[0]


def test_a_save_keeps_every_comment_and_the_spdx_header():
    with sandbox() as box:
        before = box.read()
        _post('/api/config/save', {'values': {'generation.num_turns': 3}})
        after = box.read()

    before_comments = [line for line in before.splitlines() if line.strip().startswith('#')]
    after_comments = [line for line in after.splitlines() if line.strip().startswith('#')]
    assert after_comments == before_comments, 'a comment was lost or reordered'
    assert 'SPDX-License-Identifier: AGPL-3.0-or-later' in after
    assert len(before_comments) == 113, len(before_comments)


def test_a_save_keeps_every_leaf_it_did_not_touch():
    """The Tk editor drops unknown keys; this one must not."""
    with sandbox() as box:
        before = {path: value for path, value in config_tree.leaves(box.tree())}
        _post('/api/config/save', {'values': {'generation.num_turns': 9}})
        after = {path: value for path, value in config_tree.leaves(box.tree())}

    assert set(before) == set(after), (set(before) - set(after), set(after) - set(before))
    for path, value in before.items():
        if path == 'generation.num_turns':
            assert after[path] == 9, after[path]
            continue
        assert after[path] == value, path


def test_an_unchanged_value_is_not_reported_as_changed():
    with sandbox() as box:
        current = box.tree()
        same = config_tree.get(current, 'generation.num_turns')
        body = _body(_post('/api/config/save', {'values': {'generation.num_turns': same}}))
    assert body['ok'], body
    assert body['data']['changed'] == [], body['data']


def test_a_save_of_several_keys_applies_all_of_them():
    with sandbox() as box:
        body = _body(_post('/api/config/save', {'values': {
            'generation.num_turns': 5,
            'generation.max_attempts': 4,
            'samplers.temperature': 0.75,
            'metrics.antislop_columns': True,
        }}))
        assert body['ok'], body
        assert sorted(body['data']['changed']) == [
            'generation.max_attempts', 'generation.num_turns',
            'metrics.antislop_columns', 'samplers.temperature'], body['data']
        tree = box.tree()
    assert config_tree.get(tree, 'generation.num_turns') == 5
    assert config_tree.get(tree, 'generation.max_attempts') == 4
    assert config_tree.get(tree, 'samplers.temperature') == 0.75
    assert config_tree.get(tree, 'metrics.antislop_columns') is True


# --- 4. a save cannot corrupt a type -----------------------------------

def test_a_number_field_accepts_a_string_and_stores_a_number():
    """A browser input always sends a string; storing it as one would break arithmetic."""
    with sandbox() as box:
        body = _body(_post('/api/config/save', {'values': {'generation.num_turns': '6'}}))
        assert body['ok'], body
        value = config_tree.get(box.tree(), 'generation.num_turns')
    assert value == 6, value
    assert isinstance(value, int), type(value)


def test_a_float_field_accepts_a_string():
    with sandbox() as box:
        body = _body(_post('/api/config/save', {'values': {'samplers.top_p': '0.85'}}))
        assert body['ok'], body
        value = config_tree.get(box.tree(), 'samplers.top_p')
    assert value == 0.85, value
    assert isinstance(value, float), type(value)


def test_a_bool_field_accepts_a_string():
    with sandbox() as box:
        body = _body(_post('/api/config/save',
                           {'values': {'metrics.antislop_columns': 'true'}}))
        assert body['ok'], body
        assert config_tree.get(box.tree(), 'metrics.antislop_columns') is True


def test_a_bad_number_is_rejected_and_nothing_is_written():
    """A rejected field must not half-apply the rest of the patch."""
    with sandbox() as box:
        before = box.read()
        response = _post('/api/config/save', {'values': {
            'generation.num_turns': 8,
            'generation.max_attempts': 'not a number',
        }})
        body = _body(response)
        after = box.read()

    assert not body['ok'], body
    assert body['error']['code'] == 'invalid', body['error']
    assert 'generation.max_attempts' in body['error']['fields'], body['error']
    assert 'generation.max_attempts' in body['error']['message'], body['error']['message']
    assert after == before, 'a rejected save wrote something anyway'


def test_every_rejected_field_is_reported_at_once():
    """Three bad numbers should highlight three fields, not one per attempt."""
    with sandbox():
        body = _body(_post('/api/config/save', {'values': {
            'generation.num_turns': 'x',
            'generation.max_attempts': 'y',
            'samplers.top_p': 'z',
        }}))
    assert not body['ok'], body
    fields = body['error']['fields']
    assert set(fields) == {'generation.num_turns', 'generation.max_attempts',
                           'samplers.top_p'}, fields


def test_a_fractional_value_for_an_int_field_is_rejected():
    with sandbox():
        body = _body(_post('/api/config/save', {'values': {'generation.num_turns': '2.5'}}))
    assert not body['ok'], body
    assert body['error']['code'] == 'invalid', body['error']


def test_an_empty_number_field_is_rejected_rather_than_stored_as_zero():
    with sandbox():
        body = _body(_post('/api/config/save', {'values': {'generation.num_turns': ''}}))
    assert not body['ok'], body
    assert 'generation.num_turns' in body['error']['fields'], body['error']


def test_a_text_field_keeps_prose_intact():
    """`sanitize_input` strips `<>` but keeps `->`; a save must not mangle a prompt."""
    prose = "A -> B -> C, and don't lose the apostrophe."
    with sandbox() as box:
        body = _body(_post('/api/config/save', {'values': {'prompts.lore': prose}}))
        assert body['ok'], body
        value = config_tree.get(box.tree(), 'prompts.lore')
    assert value == prose, value


def test_a_list_field_round_trips_through_a_save():
    with sandbox() as box:
        names = config_tree.get(box.tree(), 'prompts.random_names.names')
        edited = list(names)
        edited[0] = 'Zyzzyva'
        edited.append('Brandnew')
        body = _body(_post('/api/config/save',
                           {'values': {'prompts.random_names.names': edited}}))
        assert body['ok'], body
        after = config_tree.get(box.tree(), 'prompts.random_names.names')
    assert after[0] == 'Zyzzyva', after[0]
    assert after[-1] == 'Brandnew', after[-1]
    assert len(after) == len(edited), (len(after), len(edited))


# --- 5. an untouched masked field keeps the stored key -------------------

def test_an_untouched_secret_is_not_blanked():
    """Every save would otherwise wipe every API key."""
    with sandbox() as box:
        tree = box.tree()
        config_tree.set_value(tree, 'api.apis.0.key', 'sk-the-stored-key')
        config_tree.save(tree, box.path)

        body = _body(_post('/api/config/save', {'values': {
            'api.apis.0.key': config_tree.REDACTED,
            'generation.num_turns': 4,
        }}))
        assert body['ok'], body
        after = config_tree.get(box.tree(), 'api.apis.0.key')
    assert after == 'sk-the-stored-key', after


def test_a_changed_secret_is_written():
    with sandbox() as box:
        body = _body(_post('/api/config/save',
                           {'values': {'api.apis.0.key': 'sk-a-new-key'}}))
        assert body['ok'], body
        after = config_tree.get(box.tree(), 'api.apis.0.key')
    assert after == 'sk-a-new-key', after


def test_the_redacted_marker_never_reaches_the_file():
    with sandbox() as box:
        _post('/api/config/save', {'values': {
            'api.apis.0.key': config_tree.REDACTED,
            'quality.scoring_api.key': config_tree.REDACTED,
        }})
        text = box.read()
    # The marker may legitimately appear in a comment, but never as a value.
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith('#'):
            continue
        assert config_tree.REDACTED not in stripped, line


# --- 6. add works through the route ------------------------------------

def test_adding_a_new_leaf_creates_it():
    with sandbox() as box:
        body = _body(_post('/api/config/save',
                           {'values': {'generation.a_brand_new_option': 42}}))
        assert body['ok'], body
        assert body['data']['created'] == ['generation.a_brand_new_option'], body['data']
        value = config_tree.get(box.tree(), 'generation.a_brand_new_option')
    assert value == 42, value


def test_adding_a_new_section_creates_it():
    with sandbox() as box:
        body = _body(_post('/api/config/save',
                           {'values': {'brand_new_section.a_key': 'hello'}}))
        assert body['ok'], body
        tree = box.tree()
        assert 'brand_new_section' in tree, list(tree.keys())
        assert config_tree.get(tree, 'brand_new_section.a_key') == 'hello'


def test_adding_a_leaf_keeps_the_file_valid_yaml():
    with sandbox() as box:
        _post('/api/config/save', {'values': {'generation.a_brand_new_option': 42}})
        text = box.read()
    import yaml
    parsed = yaml.safe_load(text)
    assert parsed['generation']['a_brand_new_option'] == 42


def test_adding_a_leaf_keeps_every_existing_leaf():
    with sandbox() as box:
        before = {path for path, _value in config_tree.leaves(box.tree())}
        _post('/api/config/save', {'values': {'generation.a_brand_new_option': 42}})
        after = {path for path, _value in config_tree.leaves(box.tree())}
    assert before <= after, before - after
    assert after - before == {'generation.a_brand_new_option'}, after - before


def test_a_save_with_no_values_is_rejected():
    with sandbox():
        body = _body(_post('/api/config/save', {'values': {}}))
    assert not body['ok'], body
    assert body['error']['code'] == 'bad_request', body['error']


def test_a_save_with_no_body_is_rejected():
    with sandbox():
        body = _body(_post('/api/config/save', _NOT_JSON))
    assert not body['ok'], body
    assert body['error']['code'] == 'bad_request', body['error']


# --- delete --------------------------------------------------------------

def test_deleting_a_key_removes_it():
    with sandbox() as box:
        _post('/api/config/save', {'values': {'generation.a_brand_new_option': 42}})
        body = _body(_post('/api/config/delete',
                           {'path': 'generation.a_brand_new_option'}))
        assert body['ok'], body
        tree = box.tree()
    assert config_tree.get(tree, 'generation.a_brand_new_option', _MISSING) is _MISSING


def test_deleting_a_missing_key_is_a_404():
    with sandbox():
        response = _post('/api/config/delete', {'path': 'generation.no_such_key'})
        body = _body(response)
    assert not body['ok'], body
    assert body['error']['code'] == 'not_found', body['error']


def test_deleting_a_whole_section_is_refused():
    """A section delete would take 1,116 leaves with one click."""
    with sandbox() as box:
        before = box.read()
        response = _post('/api/config/delete', {'path': 'prompts'})
        body = _body(response)
        after = box.read()
    assert not body['ok'], body
    assert body['error']['code'] == 'refused', body['error']
    assert after == before


_MISSING = object()


# --- routes --------------------------------------------------------------

def test_the_three_routes_exist():
    routes = _routes(_app())
    assert '/api/config' in routes, routes
    assert 'GET' in routes['/api/config'], routes['/api/config']
    assert '/api/config/save' in routes, routes
    assert 'POST' in routes['/api/config/save'], routes['/api/config/save']
    assert '/api/config/delete' in routes, routes
    assert 'POST' in routes['/api/config/delete'], routes['/api/config/delete']


def test_no_config_route_is_a_get_that_writes():
    """A save must never be reachable by a link, a prefetch or a stale tab."""
    routes = _routes(_app())
    for path, methods in routes.items():
        if path in ('/api/config/save', '/api/config/delete'):
            assert 'GET' not in methods, (path, methods)


def test_a_save_accepts_a_real_request_body():
    """The route must read the JSON body, not treat `request` as a query parameter.

    This is the bug the first live save caught: an unannotated `request` argument is read by
    FastAPI as a **query** field, so every save was rejected with a 422 before the body was
    ever looked at. The direct-call tests above cannot see it, because they pass a fake
    `Request` straight to the endpoint and never go through FastAPI's parameter binding. So
    this test goes through a real `TestClient`.
    """
    from fastapi.testclient import TestClient

    with sandbox() as box:
        client = TestClient(_app())
        response = client.post('/api/config/save', json={'values': {'generation.num_turns': 7}})
        assert response.status_code == 200, (response.status_code, response.text)
        body = response.json()
        assert body['ok'], body
        assert body['data']['changed'] == ['generation.num_turns'], body['data']
        assert config_tree.get(box.tree(), 'generation.num_turns') == 7


def test_a_delete_accepts_a_real_request_body():
    from fastapi.testclient import TestClient

    with sandbox() as box:
        client = TestClient(_app())
        client.post('/api/config/save', json={'values': {'generation.a_temp_key': 1}})
        response = client.post('/api/config/delete', json={'path': 'generation.a_temp_key'})
        assert response.status_code == 200, (response.status_code, response.text)
        assert response.json()['ok'], response.text


def test_a_get_of_the_config_works_through_a_real_client():
    from fastapi.testclient import TestClient

    with sandbox():
        response = TestClient(_app()).get('/api/config')
        assert response.status_code == 200, (response.status_code, response.text)
        body = response.json()
    assert body['ok'], body
    assert body['leaf_count'] == 1291, body['leaf_count']


# --- 7. the renderer has no type logic of its own -----------------------

def test_the_renderer_has_no_hex_literal():
    """No hex outside theme.py."""
    import re

    with open(os.path.join(STATIC_JS, 'config.js'), 'r', encoding='utf-8') as handle:
        source = handle.read()
    offenders = re.findall(r'#[0-9a-fA-F]{3,8}\b', source)
    assert not offenders, f'config.js contains a hex literal: {offenders[:5]}'


def test_the_renderer_switches_only_on_the_kinds_the_server_sends():
    """If the renderer invented a kind, a leaf would render as a bare text box."""
    with open(os.path.join(STATIC_JS, 'config.js'), 'r', encoding='utf-8') as handle:
        source = handle.read()
    for kind in ('bool', 'number', 'secret', 'list', 'textarea', 'text', 'group'):
        assert f"'{kind}'" in source, f'config.js never mentions {kind!r}'


def test_the_renderer_does_not_recompute_a_kind():
    """`kind_of` lives in config_tree.py; a second copy in JS would drift."""
    with open(os.path.join(STATIC_JS, 'config.js'), 'r', encoding='utf-8') as handle:
        source = handle.read()
    assert 'typeof' not in source, 'config.js infers a type instead of using the server kind'
    assert 'Array.isArray' not in source, 'config.js infers a type instead of using the server kind'


def test_the_renderer_reads_a_list_from_items_not_value():
    """A `list` node carries `items`; reading `value` renders every list empty.

    This is the bug the first live screenshot caught: the 1,000-name list drew as an empty
    4-row textarea because `valueOf` only knew about `value`. A list node has no `value` key at
    all, so the failure was silent -- the control existed and was simply blank.
    """
    with open(os.path.join(STATIC_JS, 'config.js'), 'r', encoding='utf-8') as handle:
        source = handle.read()
    start = source.index('function valueOf(node)')
    block = source[start:source.index('\n}', start)]
    assert "node.kind === 'list'" in block, block
    assert 'node.items' in block, block


def test_a_list_node_carries_items_and_no_value():
    """The server's half of the same contract: `items`, never `value`."""
    with sandbox():
        payload = _get()
    node = next(node for node in payload['nodes'] if node['path'] == 'prompts')
    names = _find_node(node, 'prompts.random_names.names')
    assert names['kind'] == 'list', names['kind']
    assert 'items' in names, sorted(names)
    assert 'value' not in names, 'a list node must not also carry a scalar value'
    assert len(names['items']) == 1000, len(names['items'])


def test_the_renderer_parses():
    """A syntax error in an ES module is a blank screen, not a stack trace."""
    import subprocess

    node = shutil.which('node')
    if not node:
        return
    result = subprocess.run([node, '--check', os.path.join(STATIC_JS, 'config.js')],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_the_renderer_never_imports_generate():
    """`generate_tk.py` builds the Tk window at import time."""
    with open(os.path.join(STATIC_JS, 'config.js'), 'r', encoding='utf-8') as handle:
        source = handle.read()
    assert 'generate' not in source, 'config.js mentions generate_tk.py'


def test_the_config_tab_is_registered():
    with open(os.path.join(STATIC_JS, 'main.js'), 'r', encoding='utf-8') as handle:
        source = handle.read()
    assert 'CONFIG_TAB' in source, 'main.js never registers the Config tab'
    assert 'config.js' in source, 'main.js never imports config.js'


# --- the stylesheet ------------------------------------------------------

def test_the_stylesheet_has_the_config_layout():
    import webapp.theme as theme

    for selector in ('.config-body', '.config-nav-item', '.config-row',
                     '.config-group-head', '.config-help', '.config-error'):
        assert selector in theme.STYLESHEET, f'{selector} is missing from the stylesheet'


def test_the_config_layout_fits_two_columns():
    """Left nav, right fields -- the layout decision, asserted at depth 0."""
    import webapp.theme as theme

    start = theme.STYLESHEET.index('.config-body {')
    block = theme.STYLESHEET[start:theme.STYLESHEET.index('}', start)]
    assert 'grid-template-columns: 250px minmax(0, 1fr)' in block, block


def test_the_config_layout_has_no_fixed_viewport_height():
    """A `calc(100vh - Npx)` drifts the moment the header changes."""
    import webapp.theme as theme

    start = theme.STYLESHEET.index('.config-body {')
    end = theme.STYLESHEET.index('@media (max-width: 1100px)', start)
    assert 'calc(100vh' not in theme.STYLESHEET[start:end], 'a fixed viewport height crept in'


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
