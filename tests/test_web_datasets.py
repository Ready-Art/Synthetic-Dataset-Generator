# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the Datasets panel -- Phase 6d-a.

The Tk panel is 696 lines and its model layer (`dataset_settings.py`) is already GUI-free.
This file proves the web panel is a **thin client over that same model**, not a second
implementation of it:

1. **The persisted shape is the model's.** Every write goes through
   `dataset_settings.normalize_config`, so a source has exactly the 13 keys and an unknown
   key is dropped -- the same source `configure_queue_datasets` reads at run start.
2. **`source_id` is never regenerated.** It is the identity a saved task binding records,
   so a round trip that re-keyed a source would orphan every row already generated from it.
3. **A field edit that moves the selector invalidates the import.** Otherwise
   `validate_config` rejects the save with "import columns again" and the user cannot tell
   which field moved.
4. **A save keeps the file's comments.** `config.yml` carries 113 comment lines including
   the SPDX header; `yaml.dump` deletes all of them, so the writer must splice.
5. **Adding the first dataset produces valid YAML.** The shipped file is
   `datasets: {sources: []}`, which is the case that caught the writer bug.
6. **An import reads names only, and closes its reader.** A leaked reader holds a file
   handle for the life of the process.
7. **A stale import is refused.** A read can take seconds; the source can move under it.
8. **No route touches a source without a session** -- asserted by the route table, not by
   spot-checking.

Everything runs against a **temporary copy** of the shipped config. The real file is read but
never written.
"""

import asyncio
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dataset_settings
import webapp.datasets_routes as datasets_routes
from webapp import config_tree

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHIPPED = os.path.join(REPO, 'config', 'config.yml')
STATIC_JS = os.path.join(REPO, 'webapp', 'static', 'js')

CHECKS = []


def check(name):
    """Register a test, so a crash is reported as a failure rather than a traceback."""
    def wrap(function):
        CHECKS.append((name, function))
        return function
    return wrap


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
    """Point `datasets_routes` at a temporary copy of the shipped config."""

    def __enter__(self):
        self._directory = tempfile.TemporaryDirectory()
        self.path = os.path.join(self._directory.name, 'config.yml')
        shutil.copyfile(SHIPPED, self.path)
        self._saved = datasets_routes.app_state
        self.state = type('FakeAppState', (), {'global_config': FakeConfig(self.path)})()
        datasets_routes.app_state = self.state
        return self

    def __exit__(self, *exc_info):
        datasets_routes.app_state = self._saved
        self._directory.cleanup()
        return False

    def read(self):
        with open(self.path, 'r', encoding='utf-8') as handle:
            return handle.read()

    def tree(self):
        return config_tree.load(self.path)

    def sources(self):
        return dataset_settings.normalize_config(
            config_tree.get(self.tree(), 'datasets', {}))['sources']


def _app():
    """A FastAPI app with only the datasets routes, so no auth gate or engine is needed.

    Built with the docs routes off, exactly as `create_app` does. A bare `FastAPI()` adds `/docs`,
    `/redoc` and `/openapi.json`, which would make the "every route is under /api/datasets"
    assertion fail on routes this module never added.
    """
    from fastapi import FastAPI

    return datasets_routes.install_datasets(
        FastAPI(docs_url=None, redoc_url=None, openapi_url=None))


def _routes(app):
    """`(path, methods)` for every route, walking into included routers.

    `app.include_router()` appends an `_IncludedRouter` with no `.path` and no
    `.routes`, so a flat scan of `app.routes` finds nothing.
    """
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


class FakeRequest:
    """Just enough of a `Request` for `await request.json()`."""

    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        if self._payload is _NOT_JSON:
            raise ValueError('not json')
        return self._payload


_NOT_JSON = object()


class Body(dict):
    """A decoded response body that also remembers the HTTP status it came with.

    `_call` has to decode the body for the tests to read `body['data']`, but decoding alone throws
    the status away -- and `_status` then reports 200 for every response, including a 422. That is
    how six assertions in this file came to pass a rejected request: they compared `_status(body)`
    against 422 and got 200, or against 400 and got 200. Carrying the status on the body keeps both
    readings available from one call.
    """

    def __init__(self, payload, status):
        super().__init__(payload)
        self.status = status


def _body(response):
    """The decoded JSON body of a `JSONResponse`, or the dict a route returned."""
    if isinstance(response, dict):
        return response
    return json.loads(response.body)


def _call(method, path, payload=None, **path_params):
    """Call one route the way FastAPI would, and return its decoded body.

    A `request` argument is passed **only** to endpoints that declare one. `import_columns` and
    `validate` are `def f()` with no body, so handing them a request raises `TypeError` before the
    route runs -- which is a bug in this helper, not in the route.
    """
    import inspect

    endpoint = _find(_app(), path, method)
    args = dict(path_params)
    if 'request' in inspect.signature(endpoint).parameters:
        args['request'] = FakeRequest(payload if payload is not None else {})
    result = endpoint(**args)
    if asyncio.iscoroutine(result):
        result = asyncio.run(result)
    return Body(_body(result), _status(result))


def _status(response):
    """The HTTP status of a route's return value."""
    if isinstance(response, Body):
        return response.status
    if isinstance(response, dict):
        return 200
    return response.status_code


# --- 1. the persisted shape is the model's -------------------------------

@check('test_a_new_source_has_exactly_the_thirteen_model_keys')
def test_a_new_source_has_exactly_the_thirteen_model_keys():
    """The route returns what `new_source` returns, not a dict assembled here."""
    with sandbox():
        body = _call('POST', '/api/datasets/sources')
        assert body['ok'], body
        source = body['data']['source']
        expected = set(dataset_settings.new_source([]).keys())
        assert set(source.keys()) == expected, (set(source.keys()) ^ expected)
        assert len(source) == 13, len(source)


@check('test_an_unknown_key_is_dropped_by_the_model')
def test_an_unknown_key_is_dropped_by_the_model():
    """A hand-added key must not survive a round trip, or the file drifts."""
    with sandbox() as box:
        body = _call('POST', '/api/datasets/sources')
        source_id = body['data']['source']['source_id']
        tree = box.tree()
        sources = box.sources()
        sources[0]['not_a_real_key'] = 'x'
        config_tree.set_value(tree, 'datasets.sources', sources)
        config_tree.save(tree, box.path)
        assert 'not_a_real_key' not in box.sources()[0]
        assert box.sources()[0]['source_id'] == source_id


@check('test_a_source_id_is_never_regenerated')
def test_a_source_id_is_never_regenerated():
    """The id is what a saved task binding records, so it has to survive a round trip."""
    with sandbox() as box:
        created = _call('POST', '/api/datasets/sources')['data']['source']['source_id']
        _call('PATCH', '/api/datasets/sources/{source_id}',
              {'fields': {'name': 'Names', 'location': '/data/n.csv'}},
              source_id=created)
        assert box.sources()[0]['source_id'] == created
        _call('PUT', '/api/datasets/sources/{source_id}/visibility',
              {'columns': None}, source_id=created)
        assert box.sources()[0]['source_id'] == created


@check('test_two_adds_get_distinct_ids_and_names')
def test_two_adds_get_distinct_ids_and_names():
    with sandbox() as box:
        first = _call('POST', '/api/datasets/sources')['data']['source']
        second = _call('POST', '/api/datasets/sources')['data']['source']
        assert first['source_id'] != second['source_id']
        assert first['name'] == 'Dataset 1', first['name']
        assert second['name'] == 'Dataset 2', second['name']
        assert len(box.sources()) == 2


# --- 2. a field edit that moves the selector invalidates the import --------

@check('test_a_selector_change_clears_the_imported_schema')
def test_a_selector_change_clears_the_imported_schema():
    """Otherwise the save is rejected with "import columns again" and the user cannot
    tell which field moved."""
    with sandbox() as box:
        created = _call('POST', '/api/datasets/sources')['data']['source']
        source_id = created['source_id']
        # Forge an imported schema, as an import would have written it.
        tree = box.tree()
        sources = box.sources()
        sources[0].update({
            'kind': 'csv', 'location': '/data/old.csv',
            'columns': ['a', 'b'],
            'column_aliases': {'a': 'first'},
            'schema_selector': dataset_settings.source_selector(sources[0]),
            'schema_fingerprint': 'deadbeef',
        })
        config_tree.set_value(tree, 'datasets.sources', sources)
        config_tree.save(tree, box.path)
        assert box.sources()[0]['columns'] == ['a', 'b']

        body = _call('PATCH', '/api/datasets/sources/{source_id}',
                      {'fields': {'location': '/data/new.csv'}}, source_id=source_id)
        assert body['ok'], body
        source = body['data']['source']
        assert source['location'] == '/data/new.csv'
        assert source['columns'] == [], source['columns']
        assert source['column_aliases'] == {}, source['column_aliases']
        assert source['schema_selector'] is None
        assert source['schema_fingerprint'] is None
        assert body['data']['status']['level'] == 'muted'


@check('test_an_edit_that_does_not_move_the_selector_keeps_the_import')
def test_an_edit_that_does_not_move_the_selector_keeps_the_import():
    """Renaming a source must not throw away an import that is still valid."""
    with sandbox() as box:
        _call('POST', '/api/datasets/sources')
        tree = box.tree()
        sources = box.sources()
        sources[0].update({
            'kind': 'csv', 'location': '/data/n.csv',
            'columns': ['a', 'b'],
            'schema_selector': dataset_settings.source_selector(sources[0]),
            'schema_fingerprint': 'deadbeef',
        })
        config_tree.set_value(tree, 'datasets.sources', sources)
        config_tree.save(tree, box.path)
        source_id = box.sources()[0]['source_id']

        body = _call('PATCH', '/api/datasets/sources/{source_id}',
                      {'fields': {'name': 'Renamed'}}, source_id=source_id)
        source = body['data']['source']
        assert source['name'] == 'Renamed'
        assert source['columns'] == ['a', 'b'], source['columns']
        assert source['schema_fingerprint'] == 'deadbeef'


@check('test_a_derived_field_cannot_be_edited')
def test_a_derived_field_cannot_be_edited():
    """`columns` and `schema_fingerprint` come from an import; accepting them here
    would let the browser forge a schema."""
    with sandbox():
        _call('POST', '/api/datasets/sources')
        source_id = _call('GET', '/api/datasets')['sources'][0]['source_id']
        for field in ('columns', 'schema_fingerprint', 'column_aliases',
                      'visible_columns', 'source_id'):
            body = _call('PATCH', '/api/datasets/sources/{source_id}',
                          {'fields': {field: []}}, source_id=source_id)
            assert _status(body) == 400, (field, body)
            assert field in body['error']['message'], body


@check('test_a_bad_field_type_is_rejected_and_nothing_is_written')
def test_a_bad_field_type_is_rejected_and_nothing_is_written():
    """`enabled` must be a real bool; `normalize_config` rejects `1`."""
    with sandbox() as box:
        _call('POST', '/api/datasets/sources')
        source_id = box.sources()[0]['source_id']
        before = box.read()
        body = _call('PATCH', '/api/datasets/sources/{source_id}',
                      {'fields': {'enabled': 1}}, source_id=source_id)
        assert _status(body) == 422, body
        assert box.read() == before, 'a rejected edit wrote the file'


# --- 3. a save keeps the file's comments --------------------------------

@check('test_adding_the_first_dataset_produces_valid_yaml')
def test_adding_the_first_dataset_produces_valid_yaml():
    """The shipped file is `datasets: {sources: []}`, the case that caught the
    writer bug. Invalid YAML here means the app cannot start."""
    with sandbox() as box:
        before = box.read()
        assert '  sources: []' in before, 'the shipped file changed shape'
        _call('POST', '/api/datasets/sources')
        after = box.read()
        import yaml
        parsed = yaml.safe_load(after)
        assert isinstance(parsed['datasets']['sources'], list), parsed['datasets']
        assert len(parsed['datasets']['sources']) == 1
        assert parsed['control']['enabled'] is False, 'a neighbour key was lost'


@check('test_a_save_keeps_every_comment_and_the_spdx_header')
def test_a_save_keeps_every_comment_and_the_spdx_header():
    """`yaml.dump` deletes all 113 comment lines, including the SPDX header."""
    with sandbox() as box:
        before = box.read()
        _call('POST', '/api/datasets/sources')
        after = box.read()
        assert after.count('#') == before.count('#'), (
            before.count('#'), after.count('#'))
        assert 'SPDX-License-Identifier: AGPL-3.0-or-later' in after


@check('test_a_save_is_a_small_diff')
def test_a_save_is_a_small_diff():
    """A save is not a 1,291-line diff."""
    with sandbox() as box:
        before = box.read().splitlines()
        _call('POST', '/api/datasets/sources')
        after = box.read().splitlines()
        added = [line for line in after if line not in before]
        removed = [line for line in before if line not in after]
        assert len(removed) == 1, removed  # only the `sources: []` line
        assert len(added) <= 20, len(added)


@check('test_removing_every_source_restores_the_empty_list')
def test_removing_every_source_restores_the_empty_list():
    with sandbox() as box:
        source_id = _call('POST', '/api/datasets/sources')['data']['source']['source_id']
        _call('DELETE', '/api/datasets/sources/{source_id}', source_id=source_id)
        assert box.sources() == []
        assert '  sources: []' in box.read()


@check('test_a_delete_keeps_the_other_sources_and_their_order')
def test_a_delete_keeps_the_other_sources_and_their_order():
    with sandbox() as box:
        first = _call('POST', '/api/datasets/sources')['data']['source']['source_id']
        second = _call('POST', '/api/datasets/sources')['data']['source']['source_id']
        third = _call('POST', '/api/datasets/sources')['data']['source']['source_id']
        _call('DELETE', '/api/datasets/sources/{source_id}', source_id=second)
        ids = [source['source_id'] for source in box.sources()]
        assert ids == [first, third], ids


# --- 4. aliases and visibility -----------------------------------------

@check('test_an_alias_is_validated_by_the_model')
def test_an_alias_is_validated_by_the_model():
    """`validate_alias` is the model's rule; a bad alias must be refused."""
    with sandbox() as box:
        _call('POST', '/api/datasets/sources')
        source_id = box.sources()[0]['source_id']
        tree = box.tree()
        sources = box.sources()
        sources[0].update({'kind': 'csv', 'location': '/data/n.csv',
                            'columns': ['a', 'b'],
                            'schema_selector': dataset_settings.source_selector(sources[0]),
                            'schema_fingerprint': 'deadbeef'})
        config_tree.set_value(tree, 'datasets.sources', sources)
        config_tree.save(tree, box.path)

        body = _call('PUT', '/api/datasets/sources/{source_id}/aliases/{column}',
                      {'alias': 'not an alias'}, source_id=source_id, column='a')
        assert _status(body) == 422, body
        assert 'aliases must start with a letter' in body['error']['message'], body
        assert box.sources()[0]['column_aliases'] == {}

        body = _call('PUT', '/api/datasets/sources/{source_id}/aliases/{column}',
                      {'alias': 'first'}, source_id=source_id, column='a')
        assert body['ok'], body
        assert box.sources()[0]['column_aliases'] == {'a': 'first'}


@check('test_an_empty_alias_removes_the_mapping')
def test_an_empty_alias_removes_the_mapping():
    """A cleared GUI entry is an unmapped column, not an alias of `''`."""
    with sandbox() as box:
        _call('POST', '/api/datasets/sources')
        source_id = box.sources()[0]['source_id']
        tree = box.tree()
        sources = box.sources()
        sources[0].update({'kind': 'csv', 'location': '/data/n.csv',
                            'columns': ['a'],
                            'column_aliases': {'a': 'first'},
                            'schema_selector': dataset_settings.source_selector(sources[0]),
                            'schema_fingerprint': 'deadbeef'})
        config_tree.set_value(tree, 'datasets.sources', sources)
        config_tree.save(tree, box.path)

        _call('PUT', '/api/datasets/sources/{source_id}/aliases/{column}',
              {'alias': '   '}, source_id=source_id, column='a')
        assert box.sources()[0]['column_aliases'] == {}


@check('test_an_alias_for_a_column_that_was_not_imported_is_refused')
def test_an_alias_for_a_column_that_was_not_imported_is_refused():
    with sandbox() as box:
        _call('POST', '/api/datasets/sources')
        source_id = box.sources()[0]['source_id']
        body = _call('PUT', '/api/datasets/sources/{source_id}/aliases/{column}',
                      {'alias': 'first'}, source_id=source_id, column='a')
        assert _status(body) == 422, body
        assert 'Import columns first' in body['error']['message'], body


@check('test_visibility_never_touches_an_alias')
def test_visibility_never_touches_an_alias():
    """Visibility is presentation only: a hidden column keeps its alias active."""
    with sandbox() as box:
        _call('POST', '/api/datasets/sources')
        source_id = box.sources()[0]['source_id']
        tree = box.tree()
        sources = box.sources()
        sources[0].update({'kind': 'csv', 'location': '/data/n.csv',
                            'columns': ['a', 'b'],
                            'column_aliases': {'a': 'first'},
                            'schema_selector': dataset_settings.source_selector(sources[0]),
                            'schema_fingerprint': 'deadbeef'})
        config_tree.set_value(tree, 'datasets.sources', sources)
        config_tree.save(tree, box.path)

        _call('PUT', '/api/datasets/sources/{source_id}/visibility',
              {'columns': ['b']}, source_id=source_id)
        source = box.sources()[0]
        assert source['visible_columns'] == ['b'], source['visible_columns']
        assert source['column_aliases'] == {'a': 'first'}, source['column_aliases']


@check('test_a_full_visibility_selection_collapses_to_all')
def test_a_full_visibility_selection_collapses_to_all():
    """`None` means All, including a column imported later."""
    with sandbox() as box:
        _call('POST', '/api/datasets/sources')
        source_id = box.sources()[0]['source_id']
        tree = box.tree()
        sources = box.sources()
        sources[0].update({'kind': 'csv', 'location': '/data/n.csv',
                            'columns': ['a', 'b'],
                            'schema_selector': dataset_settings.source_selector(sources[0]),
                            'schema_fingerprint': 'deadbeef'})
        config_tree.set_value(tree, 'datasets.sources', sources)
        config_tree.save(tree, box.path)

        _call('PUT', '/api/datasets/sources/{source_id}/visibility',
              {'columns': ['a', 'b']}, source_id=source_id)
        assert box.sources()[0]['visible_columns'] is None


@check('test_a_visibility_change_needs_the_columns_key')
def test_a_visibility_change_needs_the_columns_key():
    """`null` (all) and a missing key are different requests."""
    with sandbox():
        _call('POST', '/api/datasets/sources')
        source_id = _call('GET', '/api/datasets')['sources'][0]['source_id']
        body = _call('PUT', '/api/datasets/sources/{source_id}/visibility',
                      {}, source_id=source_id)
        assert _status(body) == 400, body


# --- 4b. Phase 7: the column picker --------------------------------------
#
# `dataset_column_menu.py` is a 445-line searchable popup. Most of it was already
# covered by 6d-a; Phase 7 adds the three things that make a 40-column source usable
# rather than a scroll -- a name filter, invert, and the two filter-scoped
# selections. **All three are client-side**: the visibility route already takes an
# arbitrary column list, so the model did not change and these tests prove that.

def _seeded(box, columns, aliases=None, visible=None):
    """Write one source with `columns` straight into the temp config, and return its id."""
    _call('POST', '/api/datasets/sources')
    source_id = box.sources()[0]['source_id']
    tree = box.tree()
    sources = box.sources()
    sources[0].update({
        'kind': 'csv', 'location': '/data/n.csv',
        'columns': list(columns),
        'column_aliases': dict(aliases or {}),
        'visible_columns': visible,
        'schema_selector': dataset_settings.source_selector(sources[0]),
        'schema_fingerprint': 'deadbeef',
    })
    config_tree.set_value(tree, 'datasets.sources', sources)
    config_tree.save(tree, box.path)
    return source_id


@check('test_invert_shows_exactly_the_columns_that_were_hidden')
def test_invert_shows_exactly_the_columns_that_were_hidden():
    """The client computes the complement; the route stores it in schema order.

    This is the whole contract of the Invert button: `columns.filter(name => !current.has(name))`
    sent as a plain visibility selection. Nothing about the model had to change for it.
    """
    with sandbox() as box:
        source_id = _seeded(box, ['a', 'b', 'c', 'd'], visible=['a', 'c'])
        # What the renderer sends for Invert: every column not currently visible.
        inverted = [name for name in ['a', 'b', 'c', 'd'] if name not in {'a', 'c'}]
        _call('PUT', '/api/datasets/sources/{source_id}/visibility',
              {'columns': inverted}, source_id=source_id)
        assert box.sources()[0]['visible_columns'] == ['b', 'd'], box.sources()[0]


@check('test_invert_from_all_hides_everything_and_keeps_the_aliases')
def test_invert_from_all_hides_everything_and_keeps_the_aliases():
    """Inverting "all" is "none" -- and a hidden column keeps its mapping."""
    with sandbox() as box:
        source_id = _seeded(box, ['a', 'b'], aliases={'a': 'first'}, visible=None)
        # From `null` (all) the complement is the empty set.
        _call('PUT', '/api/datasets/sources/{source_id}/visibility',
              {'columns': []}, source_id=source_id)
        source = box.sources()[0]
        assert source['visible_columns'] == [], source['visible_columns']
        assert source['column_aliases'] == {'a': 'first'}, source['column_aliases']


@check('test_only_matching_is_a_plain_subset_selection')
def test_only_matching_is_a_plain_subset_selection():
    """"Only matching" sends the filtered names; the route needs no filter concept."""
    with sandbox() as box:
        source_id = _seeded(box, ['user_name', 'user_age', 'assistant_text'])
        matching = [name for name in ['user_name', 'user_age', 'assistant_text']
                    if 'user' in name.lower()]
        _call('PUT', '/api/datasets/sources/{source_id}/visibility',
              {'columns': matching}, source_id=source_id)
        assert box.sources()[0]['visible_columns'] == ['user_name', 'user_age']


@check('test_hide_matching_is_the_complement_of_the_filter')
def test_hide_matching_is_the_complement_of_the_filter():
    """"Hide matching" sends every column the query does *not* match."""
    with sandbox() as box:
        source_id = _seeded(box, ['user_name', 'user_age', 'assistant_text'])
        keep = [name for name in ['user_name', 'user_age', 'assistant_text']
                if 'user' not in name.lower()]
        _call('PUT', '/api/datasets/sources/{source_id}/visibility',
              {'columns': keep}, source_id=source_id)
        assert box.sources()[0]['visible_columns'] == ['assistant_text']


@check('test_a_filtered_selection_never_moves_the_schema_selector')
def test_a_filtered_selection_never_moves_the_schema_selector():
    """Visibility is presentation only: filtering must not look like a re-import.

    If a visibility change moved `schema_selector`, the panel would report the source as stale
    and `validate_config` would refuse the save -- for hiding a column.
    """
    with sandbox() as box:
        source_id = _seeded(box, ['a', 'b', 'c'])
        before = box.sources()[0]['schema_selector']
        _call('PUT', '/api/datasets/sources/{source_id}/visibility',
              {'columns': ['b']}, source_id=source_id)
        source = box.sources()[0]
        assert source['schema_selector'] == before, (before, source['schema_selector'])
        assert source['schema_fingerprint'] == 'deadbeef'


@check('test_the_filter_added_no_route')
def test_the_filter_added_no_route():
    """The filter is client-side, so the route table is still exactly eight.

    A ninth route would mean the filter had been pushed into the server, which is where it would
    have to be re-derived from the model -- and a second implementation of "which columns are
    visible" is a second answer that can disagree.

    Counted as **(path, method) pairs**, not as paths: `PATCH` and `DELETE` share
    `/sources/{source_id}`, so `len(_routes(...))` is 7 and the pairs are 8. Counting paths here
    would have been a test that fails on a correct route table.
    """
    pairs = {(path, method) for path, methods in _routes(_app()).items() for method in methods}
    assert len(pairs) == 8, sorted(pairs)


@check('test_the_renderer_filters_client_side_and_restores_the_caret')
def test_the_renderer_filters_client_side_and_restores_the_caret():
    """The filter is a `type=search` input whose value lives in module state.

    A full repaint replaces the input node, so the caret has to be put back or the field is
    unusable after one keystroke. `repaintKeepingFilter` is that, and the value is restored from
    `state.columnFilter` rather than read off the node that is about to be destroyed.
    """
    source = open(os.path.join(STATIC_JS, 'datasets.js'), encoding='utf-8').read()
    assert "type: 'search'" in source, 'the filter is not a search input'
    assert 'state.columnFilter' in source, 'the filter value is not kept in state'
    assert 'function repaintKeepingFilter' in source, 'the caret is not restored'
    assert 'datasets-column-search' in source, 'the filter input has no id to re-focus'
    assert 'setSelectionRange' in source, 'the caret is not put back at the end'


@check('test_the_filter_scoped_buttons_only_exist_with_a_query')
def test_the_filter_scoped_buttons_only_exist_with_a_query():
    """"Only matching" and "Hide matching" are gated on `query`, and Show all/none are not.

    That is the safety rule: a control that silently acted on the filtered subset while being
    labelled "Show all" would hide columns the user cannot see.
    """
    source = open(os.path.join(STATIC_JS, 'datasets.js'), encoding='utf-8').read()
    assert "query ? el('button'" in source, 'the filter-scoped buttons are not gated'
    # Show all and Show none must send the whole-schema values, never `matching`.
    assert 'setVisibility(source, null)' in source, 'Show all does not mean all'
    assert 'setVisibility(source, [])' in source, 'Show none does not mean none'
    assert 'setVisibility(source, matching)' in source, 'Only matching does not use the filter'


@check('test_an_empty_filter_result_is_a_state_not_a_blank_table')
def test_an_empty_filter_result_is_a_state_not_a_blank_table():
    """A blank table reads as "this source has no columns", which is a different thing."""
    source = open(os.path.join(STATIC_JS, 'datasets.js'), encoding='utf-8').read()
    assert 'datasets-no-match' in source, 'an empty filter result renders as a blank table'
    assert 'No column matches' in source, 'the empty state has no message'
    from webapp import theme
    assert '.datasets-no-match' in theme.STYLESHEET, 'the empty state is unstyled'


@check('test_the_filter_is_case_insensitive')
def test_the_filter_is_case_insensitive():
    """A column named `User_Name` must match `user`, which is what a user expects."""
    source = open(os.path.join(STATIC_JS, 'datasets.js'), encoding='utf-8').read()
    assert 'toLowerCase().includes(' in source, 'the filter is case-sensitive'


@check('test_the_filter_resets_on_a_source_change_but_survives_a_refresh')
def test_the_filter_resets_on_a_source_change_but_survives_a_refresh():
    """Every mutation ends in a refresh, so clearing the filter there would be hostile.

    It is cleared when the *selected source changes*, because a query that matched nothing in the
    old source reads as "this source has no columns" in the new one.
    """
    source = open(os.path.join(STATIC_JS, 'datasets.js'), encoding='utf-8').read()
    assert 'if (state.selected !== previous) state.columnFilter' in source, \
        'the filter is not reset on a source change'
    # And the refresh path must not clear it unconditionally. The guard above *contains* the
    # substring `state.columnFilter = ''`, so the assertion has to be on the bare statement.
    refresh_body = source.split('async function refresh()', 1)[1].split('\n}', 1)[0]
    assert '\n    state.columnFilter = \'\';' not in refresh_body, \
        'a refresh clears the filter, so every alias edit loses it'


@check('test_the_stylesheet_has_the_filter_rules')
def test_the_stylesheet_has_the_filter_rules():
    from webapp import theme
    assert '.datasets-column-filter' in theme.STYLESHEET
    assert '.datasets-column-filter input' in theme.STYLESHEET
    # A literal percent must be doubled: STYLESHEET is a printf template.
    assert 'width: 100%;' in theme.STYLESHEET


# --- 5. the import route ------------------------------------------------

class FakeReader:
    """A reader that records whether it was closed, and returns names only."""

    def __init__(self, spec, columns=('a', 'b'), fail=None):
        self.spec = spec
        self._columns = columns
        self._fail = fail
        self.closed = False

    def schema(self):
        from dataset_sources import DatasetSchema
        if self._fail:
            raise self._fail
        return DatasetSchema(columns=tuple(self._columns), fingerprint='abc123')

    def close(self):
        self.closed = True


class reader_patch:
    """Replace `datasets_routes.open_source` with a fake, and record what it saw."""

    def __init__(self, reader):
        self.reader = reader
        self.seen = []

    def __enter__(self):
        self._saved = datasets_routes.open_source

        def fake(spec):
            self.seen.append(spec)
            return self.reader

        datasets_routes.open_source = fake
        return self

    def __exit__(self, *exc_info):
        datasets_routes.open_source = self._saved
        return False


@check('test_an_import_stores_the_names_and_the_fingerprint')
def test_an_import_stores_the_names_and_the_fingerprint():
    with sandbox() as box:
        _call('POST', '/api/datasets/sources')
        source_id = box.sources()[0]['source_id']
        _call('PATCH', '/api/datasets/sources/{source_id}',
              {'fields': {'kind': 'csv', 'location': '/data/n.csv'}},
              source_id=source_id)
        reader = FakeReader(None)
        with reader_patch(reader):
            body = _call('POST', '/api/datasets/sources/{source_id}/import',
                          source_id=source_id)
        assert body['ok'], body
        assert body['data']['columns'] == ['a', 'b'], body['data']
        source = box.sources()[0]
        assert source['columns'] == ['a', 'b'], source['columns']
        assert source['schema_fingerprint'] == 'abc123'
        assert source['schema_selector'] == dataset_settings.source_selector(source)


@check('test_an_import_returns_the_selector_it_read_with')
def test_an_import_returns_the_selector_it_read_with():
    """The client drops a result whose selector has moved on since."""
    with sandbox() as box:
        _call('POST', '/api/datasets/sources')
        source_id = box.sources()[0]['source_id']
        _call('PATCH', '/api/datasets/sources/{source_id}',
              {'fields': {'kind': 'csv', 'location': '/data/n.csv'}},
              source_id=source_id)
        with reader_patch(FakeReader(None)):
            body = _call('POST', '/api/datasets/sources/{source_id}/import',
                          source_id=source_id)
        assert body['data']['selector'] == {
            'kind': 'csv', 'location': '/data/n.csv',
            'config': None, 'split': None, 'revision': None}, body['data']


@check('test_an_import_closes_its_reader_even_when_the_read_fails')
def test_an_import_closes_its_reader_even_when_the_read_fails():
    """A leaked reader holds a file handle for the life of the process."""
    with sandbox():
        _call('POST', '/api/datasets/sources')
        source_id = _call('GET', '/api/datasets')['sources'][0]['source_id']
        _call('PATCH', '/api/datasets/sources/{source_id}',
              {'fields': {'kind': 'csv', 'location': '/data/n.csv'}},
              source_id=source_id)
        reader = FakeReader(None, fail=RuntimeError('boom'))
        with reader_patch(reader):
            body = _call('POST', '/api/datasets/sources/{source_id}/import',
                          source_id=source_id)
        assert _status(body) == 422, body
        assert 'boom' in body['error']['message'], body
        assert reader.closed, 'the reader was not closed'


@check('test_an_import_refuses_a_source_that_moved_while_it_read')
def test_an_import_refuses_a_source_that_moved_while_it_read():
    """A read can take seconds; applying its names to a moved source would attach
    the old file's columns to the new location."""
    with sandbox() as box:
        _call('POST', '/api/datasets/sources')
        source_id = box.sources()[0]['source_id']
        _call('PATCH', '/api/datasets/sources/{source_id}',
              {'fields': {'kind': 'csv', 'location': '/data/n.csv'}},
              source_id=source_id)

        class MovingReader(FakeReader):
            """Move the source while its own read is in flight."""

            def schema(self):
                tree = config_tree.load(box.path)
                sources = box.sources()
                sources[0]['location'] = '/data/moved.csv'
                config_tree.set_value(tree, 'datasets.sources', sources)
                config_tree.save(tree, box.path)
                return super().schema()

        reader = MovingReader(None)
        with reader_patch(reader):
            body = _call('POST', '/api/datasets/sources/{source_id}/import',
                          source_id=source_id)
        assert _status(body) == 409, body
        assert 'changed' in body['error']['message'], body
        assert box.sources()[0]['columns'] == [], box.sources()[0]['columns']
        assert reader.closed, 'the reader was not closed'


@check('test_an_import_never_returns_a_row_value')
def test_an_import_never_returns_a_row_value():
    """The route reads `schema()`, never `iter_rows()`; a dataset's contents must
    not be able to reach the browser through it."""
    source = open(os.path.join(REPO, 'webapp', 'datasets_routes.py'),
                  encoding='utf-8').read()
    assert 'iter_rows' not in source, 'the import route mentions iter_rows'
    assert '.schema()' in source, 'the import route does not read a schema'


# --- 6. validate --------------------------------------------------------

@check('test_validate_reports_a_stale_mapping')
def test_validate_reports_a_stale_mapping():
    """The panel calls this before a save, so a mapping that cannot be used is
    caught in the editor rather than at run start."""
    with sandbox() as box:
        _call('POST', '/api/datasets/sources')
        tree = box.tree()
        sources = box.sources()
        sources[0].update({'kind': 'csv', 'location': '/data/n.csv',
                            'columns': ['a'], 'column_aliases': {'a': 'first'},
                            'schema_fingerprint': 'deadbeef'})
        config_tree.set_value(tree, 'datasets.sources', sources)
        config_tree.save(tree, box.path)
        body = _call('POST', '/api/datasets/validate')
        assert _status(body) == 422, body
        assert 'import columns again' in body['error']['message'], body


@check('test_validate_accepts_the_shipped_file')
def test_validate_accepts_the_shipped_file():
    """A rule that rejected the file as shipped would make the panel unusable."""
    with sandbox():
        body = _call('POST', '/api/datasets/validate')
        assert body['ok'], body


# --- 7. the route table ------------------------------------------------

@check('test_the_eight_routes_exist')
def test_the_eight_routes_exist():
    routes = _routes(_app())
    expected = {
        ('/api/datasets', 'GET'),
        ('/api/datasets/sources', 'POST'),
        ('/api/datasets/sources/{source_id}', 'PATCH'),
        ('/api/datasets/sources/{source_id}', 'DELETE'),
        ('/api/datasets/sources/{source_id}/aliases/{column}', 'PUT'),
        ('/api/datasets/sources/{source_id}/visibility', 'PUT'),
        ('/api/datasets/sources/{source_id}/import', 'POST'),
        ('/api/datasets/validate', 'POST'),
    }
    for path, method in expected:
        assert path in routes, path
        assert method in routes[path], (path, method, routes[path])


@check('test_every_route_is_behind_the_session_gate')
def test_every_route_is_behind_the_session_gate():
    """The gate is middleware, so a route cannot forget it -- but a route added to a
    *second* app would not be gated, so assert the paths are under /api/."""
    routes = _routes(_app())
    for path in routes:
        assert path.startswith('/api/datasets'), path


@check('test_no_route_is_reachable_by_a_get_that_writes')
def test_no_route_is_reachable_by_a_get_that_writes():
    """A link, a prefetch or a stale tab must not be able to mutate a source."""
    routes = _routes(_app())
    for path, methods in routes.items():
        if path in ('/api/datasets',):
            assert methods == {'GET'}, (path, methods)
    assert 'GET' not in routes['/api/datasets/sources/{source_id}/import']
    assert 'GET' not in routes['/api/datasets/sources/{source_id}']


# --- 8. the renderer holds no model logic --------------------------------

@check('test_the_renderer_has_no_hex_literal')
def test_the_renderer_has_no_hex_literal():
    """No hex literal outside `theme.py`, the same rule `ui_theme.py` records."""
    import re
    source = open(os.path.join(STATIC_JS, 'datasets.js'), encoding='utf-8').read()
    assert not re.search(r'#[0-9a-fA-F]{3,8}\b', source), 'a hex literal in datasets.js'


@check('test_the_renderer_never_mentions_generate')
def test_the_renderer_never_mentions_generate():
    """Importing `generate_tk.py` builds the whole Tk window.

    The assertion is on the **import**, not on the word: the renderer's own prose says "rows already
    generated from it", which is a sentence about the user's data, not a reference to the module.
    """
    source = open(os.path.join(STATIC_JS, 'datasets.js'), encoding='utf-8').read()
    assert 'import generate_tk' not in source, 'datasets.js imports generate'
    assert 'generate_tk.py' not in source, 'datasets.js mentions generate_tk.py'


@check('test_the_renderer_does_not_recompute_the_status')
def test_the_renderer_does_not_recompute_the_status():
    """Whether an import is stale is the model's rule; a second copy in JS drifts.

    Comments are stripped first: `datasets.js` names `schema_selector` in its docstring explaining
    **why it does not compute it**, and that explanation is the point of the rule, not a violation.
    """
    import re
    source = open(os.path.join(STATIC_JS, 'datasets.js'), encoding='utf-8').read()
    code = re.sub(r'/\*.*?\*/', '', source, flags=re.S)
    code = re.sub(r'//[^\n]*', '', code)
    assert 'schema_selector' not in code, 'datasets.js recomputes staleness'
    assert 'source_selector' not in code, 'datasets.js recomputes staleness'


@check('test_the_renderer_parses')
def test_the_renderer_parses():
    """`node --check` on the module, skipped when node is absent."""
    import shutil as _shutil
    import subprocess
    node = _shutil.which('node')
    if not node:
        return
    result = subprocess.run([node, '--check', os.path.join(STATIC_JS, 'datasets.js')],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@check('test_the_tab_is_registered')
def test_the_tab_is_registered():
    source = open(os.path.join(STATIC_JS, 'main.js'), encoding='utf-8').read()
    assert 'DATASETS_TAB' in source, 'the Datasets tab is not in the tab list'


@check('test_the_stylesheet_has_the_datasets_rules')
def test_the_stylesheet_has_the_datasets_rules():
    from webapp import theme
    for selector in ('.datasets-body', '.datasets-item', '.datasets-columns-table',
                     '.datasets-alias-editor', '.datasets-visibility'):
        assert selector in theme.STYLESHEET, selector
    # A literal percent must be doubled: STYLESHEET is a printf template.
    assert '100%;' in theme.STYLESHEET


# --- the runner ---------------------------------------------------------

def main():
    failures = 0
    for name, function in CHECKS:
        try:
            function()
        except Exception as exc:  # noqa: BLE001 - a test reports its own failure
            failures += 1
            print(f'FAIL {name}')
            print(f'     {type(exc).__name__}: {exc}')
        else:
            print(f'ok   {name}')
    print()
    if failures:
        print(f'{failures} of {len(CHECKS)} checks FAILED')
        return 1
    print(f'OK -- {len(CHECKS)} checks')
    return 0


if __name__ == '__main__':
    sys.exit(main())
