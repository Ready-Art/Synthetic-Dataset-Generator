# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the Profiles panel -- Phase 6d-b.

The Tk Profiles tab is three buttons over `ConfigLoader`. This file proves the web panel is
the same three operations **without the two hazards the model layer carries**:

1. **A load does not `yaml.dump` `config.yml`.** `load_profile_to_main_config` does, and that
   deletes all 113 comment lines including the SPDX header. A load here splices, so the
   comments survive and the diff is small.
2. **A profile name cannot be a path.** `delete_profile` and `load_profile_to_main_config`
   do not sanitize the name, so `../../config/config` reaches the live config. The route
   refuses it instead.
3. **A profile never holds an API key.** `config/profiles/` is not gitignored, and a profile
   is a whole config document, so the secret leaves are written as `***`.
4. **A load puts the live key back.** A masked secret must not blank a stored key.
5. **A load is validated first, and writes nothing when it fails.** Every bad field is
   reported at once.
6. **A load never regenerates a `source_id`.** It is the identity a saved task binding
   records.
7. **A preview writes nothing, and the load is not reachable by a GET.** A link or a prefetch
   must not be able to replace the running config.
8. **Every route is under `/api/profiles`**, asserted over the route table.

Everything runs against a **temporary copy** of the shipped config and a **temporary profiles
directory**. The real files are read but never written.
"""

import asyncio
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dataset_settings
import webapp.profiles_routes as profiles_routes
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
    """Point `profiles_routes` at a temporary config and a temporary profiles directory."""

    def __enter__(self):
        self._directory = tempfile.TemporaryDirectory()
        self.path = os.path.join(self._directory.name, 'config.yml')
        shutil.copyfile(SHIPPED, self.path)
        self.profiles = os.path.join(self._directory.name, 'profiles')
        os.makedirs(self.profiles, exist_ok=True)
        self._saved = profiles_routes.app_state
        self.state = type('FakeAppState', (), {'global_config': FakeConfig(self.path)})()
        profiles_routes.app_state = self.state
        return self

    def __exit__(self, *exc_info):
        profiles_routes.app_state = self._saved
        self._directory.cleanup()
        return False

    def read(self):
        with open(self.path, 'r', encoding='utf-8') as handle:
            return handle.read()

    def tree(self):
        return config_tree.load(self.path)

    def profile_path(self, name):
        return os.path.join(self.profiles, f'{name}.yml')

    def profile_text(self, name):
        with open(self.profile_path(name), 'r', encoding='utf-8') as handle:
            return handle.read()

    def write_profile(self, name, text):
        with open(self.profile_path(name), 'w', encoding='utf-8') as handle:
            handle.write(text)

    def write_config(self, text):
        with open(self.path, 'w', encoding='utf-8') as handle:
            handle.write(text)


def _app():
    """A FastAPI app with only the profiles routes, so no auth gate or engine is needed.

    Built with the docs routes off, exactly as `create_app` does. A bare `FastAPI()` adds
    `/docs`, `/redoc` and `/openapi.json`, which would make the "every route is under
    /api/profiles" assertion fail on routes this module never added.
    """
    from fastapi import FastAPI

    return profiles_routes.install_profiles(
        FastAPI(docs_url=None, redoc_url=None, openapi_url=None))


def _routes(app):
    """`(path, methods)` for every route, walking into included routers.

    `app.include_router()` appends an `_IncludedRouter` with no `.path` and no `.routes`, so a
    flat scan of `app.routes` finds nothing.
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

    Decoding alone throws the status away, and a status-blind assertion is how six checks in
    `test_web_datasets.py` came to pass a rejected request. Carrying the status on the body
    keeps both readings available from one call.
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

    A `request` argument is passed **only** to endpoints that declare one; `load_profile` and
    `delete_profile` take none, so handing them a request raises `TypeError` before the route
    runs -- a bug in this helper, not in the route.
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


# --- 1. a save writes a commented copy, with the secrets masked ----------

@check('test_a_saved_profile_keeps_every_comment_and_the_spdx_header')
def test_a_saved_profile_keeps_every_comment_and_the_spdx_header():
    """A profile is a slice of config.yml, so it carries the same 113 comment lines."""
    with sandbox() as box:
        body = _call('POST', '/api/profiles', {'name': 'Baseline'})
        assert body['ok'], body
        text = box.profile_text('Baseline')
        assert text.count('#') == box.read().count('#'), (
            box.read().count('#'), text.count('#'))
        assert 'SPDX-License-Identifier: AGPL-3.0-or-later' in text


@check('test_a_saved_profile_holds_no_api_key')
def test_a_saved_profile_holds_no_api_key():
    """`config/profiles/` is not gitignored, and a profile is a whole config document.

    The key is set first, so the test cannot pass by having nothing to leak -- the same rule
    Phase 6b's secret test follows.
    """
    with sandbox() as box:
        tree = box.tree()
        config_tree.set_value(tree, 'api.apis.0.key', 'sk-live-SECRET-12345')
        config_tree.set_value(tree, 'quality.scoring_api.key', 'sk-judge-SECRET-67890')
        config_tree.save(tree, box.path)
        assert 'sk-live-SECRET-12345' in box.read()

        body = _call('POST', '/api/profiles', {'name': 'Leaky'})
        assert body['ok'], body
        text = box.profile_text('Leaky')
        assert 'sk-live-SECRET-12345' not in text, 'the profile holds a live API key'
        assert 'sk-judge-SECRET-67890' not in text, 'the profile holds a judge API key'
        assert config_tree.REDACTED in text


@check('test_a_profile_of_an_unset_key_does_not_claim_to_have_one')
def test_a_profile_of_an_unset_key_does_not_claim_to_have_one():
    """A profile is a file the user reads, so masking an **unset** key would be a lie.

    `config_tree.redact` masks every secret unconditionally, which is right for the Config
    editor (it needs a field per secret and `has_value` to say whether it is set). Applied to a
    profile it writes `key: '***'` for all nine, so a config with no API keys produces a
    profile that appears to have nine. Only a secret that has a value is masked.
    """
    with sandbox() as box:
        # The shipped config has no keys set.
        tree = box.tree()
        assert config_tree.get(tree, 'api.apis.0.key') == ''
        body = _call('POST', '/api/profiles', {'name': 'Empty'})
        assert body['ok'], body
        text = box.profile_text('Empty')
        for line in text.splitlines():
            if 'key:' in line or 'password:' in line:
                assert config_tree.REDACTED not in line, (
                    'a profile of an unset secret claims to have one', line)

        # Set one, and only that one is masked.
        tree = box.tree()
        config_tree.set_value(tree, 'api.apis.0.key', 'sk-live-SECRET-12345')
        config_tree.save(tree, box.path)
        _call('POST', '/api/profiles', {'name': 'One key', 'overwrite': True})
        text = box.profile_text('One key')
        masked = [line for line in text.splitlines() if config_tree.REDACTED in line]
        assert len(masked) == 1, masked
        assert 'sk-live-SECRET-12345' not in text


@check('test_a_saved_profile_is_a_small_diff_from_the_config')
def test_a_saved_profile_is_a_small_diff_from_the_config():
    """The profile is the config's own text with the secret leaves re-rendered."""
    with sandbox() as box:
        _call('POST', '/api/profiles', {'name': 'Copy'})
        before = box.read().splitlines()
        after = box.profile_text('Copy').splitlines()
        assert len(before) == len(after), (len(before), len(after))
        differing = [index for index, line in enumerate(before) if line != after[index]]
        # Only the 9 secret leaves can differ, and most are already empty.
        assert len(differing) <= 9, differing


@check('test_saving_an_existing_name_needs_an_explicit_overwrite')
def test_saving_an_existing_name_needs_an_explicit_overwrite():
    """A save that silently replaced a tuned profile is the one mistake that makes
    profiles untrustworthy."""
    with sandbox() as box:
        assert _call('POST', '/api/profiles', {'name': 'Mine'})['ok']
        before = box.profile_text('Mine')

        body = _call('POST', '/api/profiles', {'name': 'Mine'})
        assert _status(body) == 409, body
        assert box.profile_text('Mine') == before

        body = _call('POST', '/api/profiles', {'name': 'Mine', 'overwrite': True})
        assert body['ok'], body
        assert body['data']['overwrite'] is True


@check('test_a_missing_profiles_directory_reads_as_empty')
def test_a_missing_profiles_directory_reads_as_empty():
    """A fresh checkout has no `config/profiles/`. That is an empty list, not a 500."""
    with sandbox() as box:
        shutil.rmtree(box.profiles)
        body = _call('GET', '/api/profiles')
        assert body['ok'], body
        assert body['profiles'] == [], body
        assert body['counts'] == {'profiles': 0, 'broken': 0}, body['counts']


@check('test_a_save_creates_the_profiles_directory')
def test_a_save_creates_the_profiles_directory():
    """A fresh checkout has no `config/profiles/`, and a save must not fail on that."""
    with sandbox() as box:
        shutil.rmtree(box.profiles)
        assert not os.path.isdir(box.profiles)
        body = _call('POST', '/api/profiles', {'name': 'First'})
        assert body['ok'], body
        assert os.path.isfile(box.profile_path('First'))


# --- 2. a profile name cannot be a path ----------------------------------

@check('test_a_traversal_name_is_refused_on_every_route')
def test_a_traversal_name_is_refused_on_every_route():
    """`delete_profile` and `load_profile_to_main_config` do not sanitize the name.

    `../../config/config` resolves to the live `config.yml`, so an unsanitized name is a
    delete of the running config or a load of it. The route refuses the name instead.
    """
    with sandbox() as box:
        before = box.read()
        for name in ('../config/config', '..', '../../etc/passwd', 'a/b', 'a.b'):
            body = _call('POST', '/api/profiles', {'name': name})
            assert _status(body) == 422, (name, body)
            body = _call('GET', '/api/profiles/{name}', name=name)
            assert _status(body) == 422, (name, body)
            body = _call('POST', '/api/profiles/{name}/load', name=name)
            assert _status(body) == 422, (name, body)
            body = _call('DELETE', '/api/profiles/{name}', name=name)
            assert _status(body) == 422, (name, body)
        assert box.read() == before, 'a traversal name wrote the config'


@check('test_a_name_is_never_silently_rewritten')
def test_a_name_is_never_silently_rewritten():
    """`ConfigLoader.save_profile` sanitizes; the route refuses instead.

    A silent rename means the name the user typed is not the name on disk, which is how a
    profile appears to vanish.
    """
    with sandbox() as box:
        body = _call('POST', '/api/profiles', {'name': 'My Profile!'})
        assert _status(body) == 422, body
        assert not os.path.exists(os.path.join(box.profiles, 'My Profile.yml'))
        # The characters the model keeps are accepted unchanged.
        body = _call('POST', '/api/profiles', {'name': 'My Profile_2-a'})
        assert body['ok'], body
        assert body['data']['name'] == 'My Profile_2-a'
        assert os.path.isfile(box.profile_path('My Profile_2-a'))


@check('test_an_empty_or_overlong_name_is_refused')
def test_an_empty_or_overlong_name_is_refused():
    with sandbox():
        for name in ('', '   ', 'a' * (profiles_routes.MAX_NAME_LENGTH + 1), None, 7):
            body = _call('POST', '/api/profiles', {'name': name})
            assert _status(body) == 422, (name, body)


@check('test_the_name_check_is_exact')
def test_the_name_check_is_exact():
    """The unit-level contract, so a route change cannot quietly widen it."""
    for good in ('A', 'My Profile', 'a-b_c', 'Profile 2', 'trail-'):
        assert profiles_routes._check_name(good) == good, good
    for bad in ('', ' ', '..', '.', 'a.b', 'a/b', 'a\\b', 'a' * 65, 'a\tb'):
        try:
            profiles_routes._check_name(bad)
        except profiles_routes._BadName:
            continue
        raise AssertionError(f'{bad!r} was accepted')


# --- 3. a load does not yaml.dump the config -----------------------------

@check('test_a_load_keeps_every_comment_and_the_spdx_header')
def test_a_load_keeps_every_comment_and_the_spdx_header():
    """`load_profile_to_main_config` is `yaml.dump` over config.yml, which deletes all 113
    comment lines including the SPDX header. The route splices instead."""
    with sandbox() as box:
        _call('POST', '/api/profiles', {'name': 'Baseline'})
        before = box.read()
        # Change one value in the live config, then load the profile back.
        tree = box.tree()
        config_tree.set_value(tree, 'generation.num_turns', 99)
        config_tree.save(tree, box.path)
        assert 'num_turns: 99' in box.read()

        body = _call('POST', '/api/profiles/{name}/load', name='Baseline')
        assert body['ok'], body
        after = box.read()
        assert after.count('#') == before.count('#'), (
            before.count('#'), after.count('#'))
        assert 'SPDX-License-Identifier: AGPL-3.0-or-later' in after
        assert 'num_turns: 99' not in after


@check('test_a_load_is_a_small_diff')
def test_a_load_is_a_small_diff():
    """A load is not a 1,291-line rewrite."""
    with sandbox() as box:
        _call('POST', '/api/profiles', {'name': 'Baseline'})
        tree = box.tree()
        config_tree.set_value(tree, 'generation.num_turns', 99)
        config_tree.save(tree, box.path)

        before = box.read().splitlines()
        _call('POST', '/api/profiles/{name}/load', name='Baseline')
        after = box.read().splitlines()
        assert len(before) == len(after), (len(before), len(after))
        changed = [index for index, line in enumerate(before) if line != after[index]]
        assert len(changed) == 1, changed


@check('test_a_load_of_the_current_config_changes_nothing')
def test_a_load_of_the_current_config_changes_nothing():
    """Loading a profile that matches the config must be a no-op, byte for byte."""
    with sandbox() as box:
        _call('POST', '/api/profiles', {'name': 'Same'})
        before = box.read()
        body = _call('POST', '/api/profiles/{name}/load', name='Same')
        assert body['ok'], body
        assert body['data']['diff'] == {}, body['data']['diff']
        assert box.read() == before


# --- 4. a load puts the live key back ------------------------------------

@check('test_a_load_does_not_blank_a_stored_api_key')
def test_a_load_does_not_blank_a_stored_api_key():
    """A profile holds `***`; a load must put the live key back, not write `***`."""
    with sandbox() as box:
        tree = box.tree()
        config_tree.set_value(tree, 'api.apis.0.key', 'sk-live-SECRET-12345')
        config_tree.save(tree, box.path)

        # The profile is saved with the key masked.
        _call('POST', '/api/profiles', {'name': 'Masked'})
        assert 'sk-live-SECRET-12345' not in box.profile_text('Masked')

        # Change the key, then load the profile back: the key must survive the round trip.
        tree = box.tree()
        config_tree.set_value(tree, 'api.apis.0.key', 'sk-other-SECRET-99999')
        config_tree.save(tree, box.path)

        body = _call('POST', '/api/profiles/{name}/load', name='Masked')
        assert body['ok'], body
        loaded = config_tree.load(box.path)
        assert config_tree.get(loaded, 'api.apis.0.key') == 'sk-other-SECRET-99999', (
            'a load blanked or replaced the stored key')


@check('test_a_load_never_writes_the_redaction_marker_as_a_key')
def test_a_load_never_writes_the_redaction_marker_as_a_key():
    """`***` must never reach config.yml as a value."""
    with sandbox() as box:
        _call('POST', '/api/profiles', {'name': 'Masked'})
        _call('POST', '/api/profiles/{name}/load', name='Masked')
        text = box.read()
        assert f'key: {config_tree.REDACTED}' not in text, 'the redaction marker was written'


# --- 5. a load is validated first ---------------------------------------

@check('test_an_invalid_profile_writes_nothing_and_reports_every_field')
def test_an_invalid_profile_writes_nothing_and_reports_every_field():
    """Every bad field at once, and nothing written -- validate before the write."""
    with sandbox() as box:
        # A profile with three out-of-range values, written as a plain document.
        box.write_profile('Broken', (
            'generation:\n'
            '  num_turns: -5\n'
            '  max_character_cards: -1\n'
            'api:\n'
            '  apis:\n'
            '    - url: not-a-url\n'
        ))
        before = box.read()
        body = _call('POST', '/api/profiles/{name}/load', name='Broken')
        assert _status(body) == 422, body
        assert box.read() == before, 'a rejected load wrote the config'
        fields = body['error'].get('fields') or {}
        assert len(fields) >= 3, fields
        assert 'generation.num_turns' in fields, fields
        assert 'api.apis.0.url' in fields, fields


@check('test_a_preview_reports_the_same_errors_and_writes_nothing')
def test_a_preview_reports_the_same_errors_and_writes_nothing():
    """The preview is a GET, so it must not write -- and it must agree with the load."""
    with sandbox() as box:
        box.write_profile('Broken', 'generation:\n  num_turns: -5\n')
        before = box.read()
        body = _call('GET', '/api/profiles/{name}', name='Broken')
        assert body['ok'], body
        assert body['data']['valid'] is False
        assert 'generation.num_turns' in body['data']['errors'], body['data']['errors']
        assert box.read() == before, 'a preview wrote the config'


@check('test_a_profile_with_a_stale_dataset_mapping_is_refused')
def test_a_profile_with_a_stale_dataset_mapping_is_refused():
    """`dataset_settings.validate_config` is the engine's own rule, so a profile that would
    fail at `configure_queue_datasets` fails here instead."""
    with sandbox() as box:
        box.write_profile('Stale', (
            'datasets:\n'
            '  sources:\n'
            '    - source_id: src_1\n'
            '      name: Names\n'
            '      enabled: true\n'
            '      kind: csv\n'
            '      location: /data/n.csv\n'
            '      columns: [a]\n'
            '      column_aliases: {a: first}\n'
            '      schema_fingerprint: deadbeef\n'
        ))
        before = box.read()
        body = _call('POST', '/api/profiles/{name}/load', name='Stale')
        assert _status(body) == 422, body
        assert 'datasets' in (body['error'].get('fields') or {}), body['error']
        assert box.read() == before


@check('test_a_broken_profile_is_reported_not_hidden')
def test_a_broken_profile_is_reported_not_hidden():
    """A profile the user saved and can no longer see is worse than one that says why."""
    with sandbox() as box:
        box.write_profile('Garbage', 'this: is: not: yaml: [\n')
        body = _call('GET', '/api/profiles')
        entry = next(item for item in body['profiles'] if item['name'] == 'Garbage')
        assert entry['broken'] is True, entry
        assert entry['error'], entry
        assert body['counts']['broken'] == 1

        body = _call('GET', '/api/profiles/{name}', name='Garbage')
        assert _status(body) == 422, body


# --- 6. a load never regenerates a source_id -----------------------------

@check('test_a_load_never_regenerates_a_source_id')
def test_a_load_never_regenerates_a_source_id():
    """The id is what a saved task binding records, so a load must not re-key a source."""
    with sandbox() as box:
        # A valid, fully imported source in the live config.
        tree = box.tree()
        source = dataset_settings.new_source([])
        source.update({
            'name': 'Names', 'enabled': True, 'kind': 'csv', 'location': '/data/n.csv',
            'columns': ['a'], 'column_aliases': {'a': 'first'},
            'schema_fingerprint': 'deadbeef',
        })
        source['schema_selector'] = dataset_settings.source_selector(source)
        config_tree.set_value(tree, 'datasets.sources', [source])
        config_tree.save(tree, box.path)
        original_id = source['source_id']

        _call('POST', '/api/profiles', {'name': 'WithData'})
        # Wipe the sources, then load the profile back.
        tree = box.tree()
        config_tree.set_value(tree, 'datasets.sources', [])
        config_tree.save(tree, box.path)

        body = _call('POST', '/api/profiles/{name}/load', name='WithData')
        assert body['ok'], body
        loaded = config_tree.load(box.path)
        sources = dataset_settings.normalize_config(
            config_tree.get(loaded, 'datasets', {}))['sources']
        assert len(sources) == 1, sources
        assert sources[0]['source_id'] == original_id, (
            original_id, sources[0]['source_id'])


# --- 7. the route table --------------------------------------------------

@check('test_the_five_routes_exist')
def test_the_five_routes_exist():
    routes = _routes(_app())
    expected = {
        ('/api/profiles', 'GET'),
        ('/api/profiles', 'POST'),
        ('/api/profiles/{name}', 'GET'),
        ('/api/profiles/{name}', 'DELETE'),
        ('/api/profiles/{name}/load', 'POST'),
    }
    for path, method in expected:
        assert path in routes, path
        assert method in routes[path], (path, method, routes[path])


@check('test_every_route_is_under_the_profiles_prefix')
def test_every_route_is_under_the_profiles_prefix():
    routes = _routes(_app())
    for path in routes:
        assert path.startswith('/api/profiles'), path


@check('test_a_load_is_not_reachable_by_a_get')
def test_a_load_is_not_reachable_by_a_get():
    """A link, a prefetch or a stale tab must not be able to replace the config."""
    routes = _routes(_app())
    assert routes['/api/profiles/{name}/load'] == {'POST'}, routes['/api/profiles/{name}/load']
    assert 'GET' not in routes['/api/profiles/{name}/load']
    assert routes['/api/profiles/{name}'] == {'GET', 'DELETE'}, routes['/api/profiles/{name}']


@check('test_a_preview_writes_nothing')
def test_a_preview_writes_nothing():
    """Asserted at the source: the preview route never calls a writer."""
    import inspect

    source = inspect.getsource(profiles_routes.install_profiles)
    start = source.index('def preview_profile')
    end = source.index('def load_profile')
    preview = source[start:end]
    assert 'config_tree.save' not in preview, 'the preview writes the config'
    assert 'os.remove' not in preview, 'the preview deletes a file'
    assert '_write_atomic' not in preview, 'the preview writes a profile'


# --- 8. the renderer holds no model logic --------------------------------

@check('test_the_renderer_has_no_hex_literal')
def test_the_renderer_has_no_hex_literal():
    """No hex literal outside `theme.py`, the same rule `ui_theme.py` records."""
    import re
    source = open(os.path.join(STATIC_JS, 'profiles.js'), encoding='utf-8').read()
    assert not re.search(r'#[0-9a-fA-F]{3,8}\b', source), 'a hex literal in profiles.js'


@check('test_the_renderer_never_mentions_generate')
def test_the_renderer_never_mentions_generate():
    """Importing `generate_tk.py` builds the whole Tk window."""
    source = open(os.path.join(STATIC_JS, 'profiles.js'), encoding='utf-8').read()
    assert 'import generate_tk' not in source, 'profiles.js imports generate'
    assert 'generate_tk.py' not in source, 'profiles.js mentions generate_tk.py'


@check('test_the_renderer_does_not_reimplement_the_name_rule')
def test_the_renderer_does_not_reimplement_the_name_rule():
    """The name rule is the server's; a second copy in JS would drift.

    Comments are stripped first: the module's docstring explains *why* the rule is not here,
    and that explanation is the point of the rule, not a violation.
    """
    import re
    source = open(os.path.join(STATIC_JS, 'profiles.js'), encoding='utf-8').read()
    code = re.sub(r'/\*.*?\*/', '', source, flags=re.S)
    code = re.sub(r'//[^\n]*', '', code)
    assert 'isalnum' not in code, 'profiles.js reimplements the name rule'
    assert 'realpath' not in code, 'profiles.js reimplements the path check'


@check('test_the_renderer_parses')
def test_the_renderer_parses():
    """`node --check` on the module, skipped when node is absent."""
    import shutil as _shutil
    import subprocess
    node = _shutil.which('node')
    if not node:
        return
    result = subprocess.run([node, '--check', os.path.join(STATIC_JS, 'profiles.js')],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@check('test_the_tab_is_registered')
def test_the_tab_is_registered():
    source = open(os.path.join(STATIC_JS, 'main.js'), encoding='utf-8').read()
    assert 'PROFILES_TAB' in source, 'the Profiles tab is not in the tab list'


@check('test_the_stylesheet_has_the_profiles_rules')
def test_the_stylesheet_has_the_profiles_rules():
    from webapp import theme
    for selector in ('.profiles-body', '.profiles-item', '.profiles-diff-table',
                     '.profiles-save', '.profiles-errors'):
        assert selector in theme.STYLESHEET, selector
    # A literal percent must be doubled: STYLESHEET is a printf template.
    assert '100%;' in theme.STYLESHEET


@check('test_the_routes_module_is_in_the_undefined_globals_list')
def test_the_routes_module_is_in_the_undefined_globals_list():
    """Every `webapp/` module is imported by a long-running server, so an unbound name is a
    500 on a route rather than a traceback in a terminal."""
    source = open(os.path.join(REPO, 'tests', 'test_undefined_globals.py'),
                  encoding='utf-8').read()
    assert 'webapp/profiles_routes.py' in source


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
