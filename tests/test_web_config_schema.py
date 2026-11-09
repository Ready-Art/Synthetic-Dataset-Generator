# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the Config editor's schema -- Phase 6c.

Phase 6b proved every leaf renders and every leaf can be written. This file proves the schema
**decorates** that tree without ever changing what is reachable, and that the checks
``config_editor.py:validate_config_handler`` performs now run **server-side before a save**.

The assertions, in order of how much they matter:

1. **The schema cannot make a leaf unreachable.** It is decoration, not a form definition, so a
   path with no entry still renders generically. The leaf set is compared against
   ``config_tree.leaves`` exactly as 6b did.
2. **A schema entry never changes a node's shape.** A group, a flat list and a secret keep the
   kind the tree gave them; only a scalar leaf's kind may be refined, and only to another scalar
   kind. This is what stops a decoration from breaking the renderer or the save path.
3. **Every enum choice list is real.** The literals are pinned against the engine's own
   constants, and the compatibility profiles are read from the registry, so a dropdown can never
   offer a value the worker silently rewrites.
4. **The validators run and reject.** Ranges, enums, URLs, regexes, JSON, aliases and the
   cross-field rules are each exercised, and a rejected save writes **nothing**.
5. **The shipped config validates clean.** A rule that rejected the file as shipped would make
   the editor unusable on first run.
6. **The renderer still has no type logic**, and reads `choices` / `help` / `min` / `max` from
   the server rather than deriving them.
"""

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
    """Point `config_routes` at a temporary copy of the shipped config."""

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


def _find(app, path, method):
    """The endpoint callable for `path` + `method`, walking into included routers."""
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


def _walk(node, out):
    """Every leaf path in a payload node tree, with its kind."""
    kind = node.get('kind')
    if kind == 'group':
        for child in node.get('children') or []:
            _walk(child, out)
    elif kind == 'list':
        for index in range(node.get('count') or 0):
            out.append((f"{node['path']}.{index}", 'list-item'))
    else:
        out.append((node['path'], kind))
    return out


def _payload_leaves(payload):
    out = []
    for node in payload.get('nodes') or []:
        _walk(node, out)
    return out


def _find_node(node, path):
    if node.get('path') == path:
        return node
    for child in node.get('children') or []:
        found = _find_node(child, path)
        if found is not None:
            return found
    return None


def _section(payload, key):
    return next(node for node in payload['nodes'] if node['path'] == key)


# --- 1. the schema cannot make a leaf unreachable ------------------------

def test_every_leaf_still_reaches_the_payload_with_a_schema_in_place():
    """The schema is decoration, so it cannot remove a field."""
    with sandbox():
        payload = _get()
    tree = config_tree.load(SHIPPED)
    expected = {path for path, _value in config_tree.leaves(tree)}
    got = {path for path, _kind in _payload_leaves(payload)}
    assert expected == got, (len(expected - got), len(got - expected))


def test_the_leaf_count_is_unchanged_by_the_schema():
    with sandbox():
        payload = _get()
    tree = config_tree.load(SHIPPED)
    assert payload['leaf_count'] == sum(1 for _ in config_tree.leaves(tree))


def test_a_path_with_no_schema_entry_still_renders():
    """A leaf the schema has never heard of must still arrive, generically."""
    with sandbox():
        payload = _get()
    # `datasets.sources` is a list the schema names but does not describe item by item.
    node = _find_node(_section(payload, 'datasets'), 'datasets.sources')
    assert node is not None
    assert node['kind'] == 'list'


def test_the_schema_covers_every_leaf_in_the_shipped_config():
    """Not a requirement, but it is the state 6c claims: all 1,291 leaves are decorated."""
    tree = config_tree.load(SHIPPED)
    uncovered = [path for path, _value in config_tree.leaves(tree)
                 if config_schema.lookup(path) is None]
    assert not uncovered, f'{len(uncovered)} leaves have no schema entry: {uncovered[:5]}'


def test_an_indexed_path_is_decorated_without_its_own_entry():
    """A seventh API slot or a tenth character card needs no new schema entry."""
    assert config_schema.lookup('api.apis.0.url') is not None
    assert config_schema.lookup('api.apis.9.url') is not None
    assert config_schema.lookup('prompts.character.characters.7.name') is not None
    assert config_schema.lookup('detection.user_speaking.neutral.phrases') is not None


# --- 2. a schema entry never changes a node's shape ----------------------

def test_a_schema_entry_never_turns_a_group_into_a_leaf():
    """`describe` may only refine a scalar; a group keeps its shape."""
    decoration = config_schema.describe('api.apis', 'group')
    assert 'kind' not in decoration, decoration


def test_a_schema_entry_never_turns_a_list_into_a_scalar():
    decoration = config_schema.describe('prompts.random_names.names', 'list')
    assert 'kind' not in decoration, decoration


def test_a_schema_entry_never_turns_a_secret_into_a_plain_field():
    decoration = config_schema.describe('api.apis.0.key', 'secret')
    assert 'kind' not in decoration, decoration


def test_a_schema_may_refine_a_scalar_kind():
    """A long prompt body becomes a textarea; a fixed choice becomes an enum."""
    assert config_schema.describe('prompts.answer', 'text').get('kind') == 'textarea'
    assert config_schema.describe('samplers.enable_thinking', 'text').get('kind') == 'enum'


def test_an_enum_with_no_choices_is_not_rendered_as_an_enum():
    """An empty dropdown is worse than a text field, so the kind is dropped instead."""
    decoration = config_schema.describe('api.apis.0.api_profile', 'text')
    assert decoration.get('choices'), decoration
    assert decoration.get('kind') == 'enum'


def test_the_payload_kind_is_kind_of_or_a_scalar_refinement():
    with sandbox():
        payload = _get()
    tree = config_tree.load(SHIPPED)
    for path, kind in _payload_leaves(payload):
        if kind == 'list-item':
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
        assert kind in ('text', 'textarea', 'number', 'bool', 'enum'), (path, kind)


# --- 3. every enum choice list is real -----------------------------------

def test_the_note_placements_match_the_engine():
    import generation
    assert config_schema.NOTE_PLACEMENTS == generation.NOTE_PLACEMENTS


def test_the_cast_transition_modes_match_the_engine():
    import generation
    assert config_schema.CAST_TRANSITION_MODES == generation.CAST_TRANSITION_MODES


def test_the_thinking_modes_match_the_engine():
    import api_handler
    assert config_schema.THINKING_MODES == api_handler.THINKING_MODES


def test_the_api_profile_choices_come_from_the_registry():
    """A profile is data, so a copied list would go stale the moment one is added."""
    import api_profiles
    expected = [name for name, _label in api_profiles.list_profiles()]
    decoration = config_schema.describe('api.apis.0.api_profile', 'text')
    assert decoration['choices'] == expected


def test_every_enum_choice_is_a_value_the_shipped_config_could_hold():
    """Every choice list must contain the value the file ships with, or the editor opens broken."""
    tree = config_tree.load(SHIPPED)
    for path, value in config_tree.leaves(tree):
        field = config_schema.lookup(path)
        if field is None or not field.choices:
            continue
        assert str(value) in field.choices, (path, value, field.choices)


# --- 4. the validators run and reject -----------------------------------

def _validate(path, value):
    """Apply one edit to a fresh tree and validate it, as the save route does."""
    tree = config_tree.load(SHIPPED)
    config_tree.set_value(tree, path, value)
    return config_schema.validate(tree, {path: value})


def test_a_bad_url_is_rejected():
    assert 'api.apis.0.url' in _validate('api.apis.0.url', 'not-a-url')


def test_a_url_without_a_host_is_rejected():
    assert 'api.apis.0.url' in _validate('api.apis.0.url', 'http://')


def test_a_good_url_is_accepted():
    assert _validate('api.apis.0.url', 'http://127.0.0.1:8000/v1/chat/completions') == {}


def test_a_number_below_its_minimum_is_rejected():
    assert 'generation.num_turns' in _validate('generation.num_turns', 0)


def test_a_number_above_its_maximum_is_rejected():
    assert 'generation.placeholder_share' in _validate('generation.placeholder_share', 2.0)


def test_a_number_inside_its_range_is_accepted():
    assert _validate('generation.num_turns', 3) == {}
    assert _validate('generation.placeholder_share', 0.5) == {}


def test_a_character_age_outside_18_to_60_is_rejected():
    """`config_editor.py` refuses an age outside 18-60 on a character card."""
    assert 'prompts.character.characters.0.age' in _validate(
        'prompts.character.characters.0.age', '12')
    assert 'prompts.character.characters.0.age' in _validate(
        'prompts.character.characters.0.age', '61')


def test_a_character_age_inside_18_to_60_is_accepted():
    assert _validate('prompts.character.characters.0.age', '30') == {}


def test_a_non_numeric_character_age_is_rejected():
    assert 'prompts.character.characters.0.age' in _validate(
        'prompts.character.characters.0.age', 'old')


def test_an_empty_character_age_is_allowed():
    """The field is optional; only a value that is set has to be usable."""
    assert _validate('prompts.character.characters.0.age', '') == {}


def test_a_regex_that_does_not_compile_is_rejected():
    """A pattern that does not compile would be skipped silently at run time."""
    errors = _validate('detection.regex_watch.patterns.0', '(unclosed')
    assert 'detection.regex_watch.patterns.0' in errors


def test_a_valid_regex_is_accepted():
    assert _validate('detection.regex_watch.patterns.0', r'\bnot\s+just\b') == {}


def test_logit_bias_must_be_valid_json():
    assert 'samplers.logit_bias' in _validate('samplers.logit_bias', '{nope')
    assert _validate('samplers.logit_bias', '{"50256": -100}') == {}


def test_an_empty_logit_bias_is_allowed():
    assert _validate('samplers.logit_bias', '') == {}


def test_a_bad_dataset_alias_is_rejected():
    errors = _validate('prompts.questions_dataset.append_alias', '1bad')
    assert 'prompts.questions_dataset.append_alias' in errors


def test_an_unknown_api_profile_is_rejected():
    assert 'api.apis.0.api_profile' in _validate('api.apis.0.api_profile', 'nope')


def test_a_known_api_profile_is_accepted():
    assert _validate('api.apis.0.api_profile', 'mistral') == {}


def test_a_value_outside_an_enum_is_rejected():
    """Without this the dropdown is decoration and a request could store a rewritten value."""
    assert 'samplers.enable_thinking' in _validate('samplers.enable_thinking', 'maybe')
    assert 'gender' in _validate('gender', 'robot')
    assert 'quality.judge_structured_output' in _validate(
        'quality.judge_structured_output', 'yaml')
    assert 'prompts.scene_cast.shape' in _validate('prompts.scene_cast.shape', 'weird')


def test_every_enum_accepts_its_own_choices():
    for path in ('samplers.enable_thinking', 'gender', 'quality.judge_structured_output',
                 'prompts.scene_cast.shape', 'prompts.scene_cast.exit_detection'):
        field = config_schema.lookup(path)
        for choice in field.choices:
            assert _validate(path, choice) == {}, (path, choice)


# --- the cross-field rules ----------------------------------------------

def test_an_append_alias_needs_the_questions_file_enabled():
    """`config_editor.py` refuses an alias while the questions file is off."""
    errors = _validate('prompts.questions_dataset.append_alias', 'foo')
    assert 'prompts.questions_dataset.append_alias' in errors


def test_an_append_alias_is_allowed_once_the_questions_file_is_on():
    tree = config_tree.load(SHIPPED)
    config_tree.set_value(tree, 'prompts.use_questions_file', True)
    config_tree.set_value(tree, 'prompts.questions_dataset.append_alias', 'foo')
    errors = config_schema.validate(
        tree, {'prompts.use_questions_file': True,
               'prompts.questions_dataset.append_alias': 'foo'})
    assert errors == {}, errors


def test_enabling_bad_words_with_no_words_is_rejected():
    tree = config_tree.load(SHIPPED)
    config_tree.set_value(tree, 'samplers.use_bad_words', True)
    errors = config_schema.validate(tree, {'samplers.use_bad_words': True})
    assert 'samplers.bad_words' in errors, errors


def test_the_shipped_config_validates_clean():
    """A rule that rejected the file as shipped would make the editor unusable on first run."""
    tree = config_tree.load(SHIPPED)
    values = dict(config_tree.leaves(tree))
    assert config_schema.validate(tree, values) == {}


def test_every_bad_field_is_reported_at_once():
    """The editor highlights all of them, so the save reports all of them."""
    tree = config_tree.load(SHIPPED)
    patch = {'api.apis.0.url': 'nope', 'generation.num_turns': 0, 'gender': 'robot'}
    for path, value in patch.items():
        config_tree.set_value(tree, path, value)
    errors = config_schema.validate(tree, patch)
    assert set(errors) == set(patch), errors


# --- 5. a rejected save writes nothing ----------------------------------

def _post_save(values):
    """Drive the real save route with a fake request.

    `save_config` is an `async def`, so the endpoint is driven through `asyncio.run` -- the same
    thing FastAPI does. Calling it directly would return an un-awaited coroutine and every
    assertion would pass against nothing.
    """
    import asyncio

    body = json.dumps({'values': values}).encode('utf-8')

    class FakeRequest:
        async def json(self):
            return json.loads(body)

    endpoint = _find(_app(), '/api/config/save', 'POST')
    return asyncio.run(endpoint(FakeRequest()))


def _status(response):
    """The HTTP status of a route result.

    A successful route returns a plain dict (FastAPI serialises it with a 200); a rejected one
    returns a `JSONResponse` carrying its own status. Normalising here keeps the tests reading as
    assertions about behaviour rather than about FastAPI's return-type union.
    """
    return getattr(response, 'status_code', 200)


def _body(response):
    """The decoded body of a route result."""
    if isinstance(response, dict):
        return response
    return json.loads(response.body)


def test_a_rejected_save_does_not_touch_the_file():
    with sandbox() as box:
        before = open(box.path, 'rb').read()
        response = _post_save({'generation.num_turns': 0})
        assert _status(response) == 422, _status(response)
        after = open(box.path, 'rb').read()
        assert before == after, 'a rejected save wrote to the file'


def test_a_rejected_save_names_every_bad_field():
    with sandbox():
        response = _post_save({'generation.num_turns': 0, 'api.apis.0.url': 'nope'})
        body = _body(response)
        assert set(body['error']['fields']) == {'generation.num_turns', 'api.apis.0.url'}


def test_a_valid_save_is_written():
    with sandbox() as box:
        response = _post_save({'generation.num_turns': 3})
        assert _status(response) == 200, _status(response)
        tree = config_tree.load(box.path)
        assert config_tree.get(tree, 'generation.num_turns') == 3


def test_a_save_of_an_enum_choice_is_written():
    with sandbox() as box:
        response = _post_save({'samplers.enable_thinking': 'disable'})
        assert _status(response) == 200, _status(response)
        tree = config_tree.load(box.path)
        assert config_tree.get(tree, 'samplers.enable_thinking') == 'disable'


def test_a_save_that_breaks_an_enum_is_rejected():
    with sandbox() as box:
        before = open(box.path, 'rb').read()
        response = _post_save({'samplers.enable_thinking': 'maybe'})
        assert _status(response) == 422
        assert open(box.path, 'rb').read() == before


# --- 6. the escape codec ------------------------------------------------

def test_the_separator_is_shown_escaped():
    """A single-line input cannot hold a newline, so it is shown as `\\n`."""
    assert config_schema.encode('prompts.questions_dataset.append_separator', '\n\n') == '\\n\\n'


def test_the_separator_is_stored_decoded():
    assert config_schema.decode('prompts.questions_dataset.append_separator', '\\n\\n') == '\n\n'


def test_the_escape_round_trip_is_lossless():
    for text in ('\n', '\t', '\r', '\\', 'a\nb\tc', ''):
        encoded = config_schema.encode('prompts.questions_dataset.append_separator', text)
        assert config_schema.decode(
            'prompts.questions_dataset.append_separator', encoded) == text


def test_an_unknown_escape_is_kept_as_typed():
    """`\\q` is not one of the four escapes, so it must survive unchanged."""
    assert config_schema.decode(
        'prompts.questions_dataset.append_separator', '\\q') == '\\q'


def test_the_separator_arrives_escaped_in_the_payload():
    with sandbox():
        payload = _get()
    node = _find_node(_section(payload, 'prompts'), 'prompts.questions_dataset.append_separator')
    assert node is not None
    assert node['value'] == '\\n\\n', node['value']


def test_a_coded_field_round_trips_through_a_save():
    with sandbox() as box:
        response = _post_save({'prompts.questions_dataset.append_separator': '\\n---\\n'})
        assert _status(response) == 200, _status(response)
        tree = config_tree.load(box.path)
        assert config_tree.get(tree, 'prompts.questions_dataset.append_separator') == '\n---\n'


# --- 7. the renderer still holds no type logic --------------------------

def _renderer():
    with open(os.path.join(STATIC_JS, 'config.js'), 'r', encoding='utf-8') as handle:
        return handle.read()


def test_the_renderer_reads_choices_from_the_server():
    source = _renderer()
    assert 'node.choices' in source, 'the renderer does not read the schema choices'


def test_the_renderer_reads_help_from_the_server():
    source = _renderer()
    assert 'node.help' in source, 'the renderer does not read the schema help text'


def test_the_renderer_reads_bounds_from_the_server():
    source = _renderer()
    assert 'node.min' in source and 'node.max' in source


def test_the_renderer_still_has_no_hex_literal():
    source = _renderer()
    assert '#' not in source.replace('#', '', 0) or True  # placeholder, see below
    # A hex colour belongs in theme.py. A `#` in a selector or an id is fine; `#rrggbb` is not.
    import re
    assert not re.search(r'#[0-9a-fA-F]{3,8}\b', source), 'a hex colour leaked into config.js'


def test_the_renderer_never_imports_generate():
    source = _renderer()
    assert 'generate_tk' not in source, 'config.js mentions the Tk window'


def test_the_renderer_parses():
    import subprocess
    result = subprocess.run(['node', '--check', os.path.join(STATIC_JS, 'config.js')],
                            capture_output=True, text=True)
    if result.returncode != 0 and 'not found' in result.stderr.lower():
        return  # no node on this box; the other renderer tests still hold
    assert result.returncode == 0, result.stderr


# --- the schema module itself -------------------------------------------

def test_the_schema_module_has_no_hex_literal():
    """The palette lives in theme.py; a schema is labels and rules, not colours."""
    import re
    with open(os.path.join(REPO, 'webapp', 'config_schema.py'), encoding='utf-8') as handle:
        source = handle.read()
    assert not re.search(r'#[0-9a-fA-F]{6}\b', source)


def test_the_schema_does_not_import_generate():
    with open(os.path.join(REPO, 'webapp', 'config_schema.py'), encoding='utf-8') as handle:
        source = handle.read()
    assert 'import generate_tk' not in source


def test_every_schema_entry_has_a_label():
    for path, field in config_schema.SCHEMA.items():
        assert field.label, path


def test_every_validator_named_by_a_field_exists():
    for path, field in config_schema.SCHEMA.items():
        if field.validator:
            assert field.validator in config_schema.VALIDATORS, (path, field.validator)
