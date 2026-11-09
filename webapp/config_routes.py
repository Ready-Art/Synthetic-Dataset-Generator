# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""The Config editor's routes -- Phase 6b, the generic renderer's server half.

The web Config editor is the **only** editor, so it has to reach
every one of the 1,291 leaves in ``config/config.yml`` and be able to add new ones. This module
serves the tree in a shape one renderer can draw, and writes a patch back through
:mod:`webapp.config_tree` so nothing is lost.

Why the tree is pre-walked here rather than in JavaScript
-------------------------------------------------------
``config_tree.kind_of`` already decides what a value should render as, and it is tested. If the
browser re-derived that from the JSON type there would be two implementations of one rule, and
they would drift -- the same reasoning that keeps the data tabs reading
``build_stats_payload`` instead of recomputing a statistic. So this module walks the tree once
and sends a **renderable description**: groups, flat lists opped as one node, and leaves carrying
their own ``kind``, ``label`` and (redacted) ``value``. ``webapp/static/js/config.js`` is then a
dumb renderer with no type logic in it at all.

What the payload guarantees
---------------------------
* **Every leaf is present exactly once.** A test counts them against ``config_tree.leaves``.
  This is what "all options can be modified" means in practice.
* **No secret leaves the server.** ``api.apis.*.key``, ``quality.scoring_api.key``,
  ``valkey.password`` and ``database.password`` arrive as ``***`` with a ``has_value`` flag.
* **A leaf with no schema entry still renders.** There is no schema yet (that is 6c), and the
  renderer does not need one: the kind comes from the value's own type.

What a save must never do
-------------------------
* **Never write a value of the wrong type.** The browser sends strings from inputs; a number
  field that came back as ``"10"`` must be stored as ``10`` or the engine's arithmetic breaks.
  :func:`coerce` converts against the *current* value's type and rejects what cannot convert.
* **Never blank an untouched secret.** ``merge_secrets`` puts the stored value back where the
  browser still holds ``***``.
* **Never drop a key the editor did not touch.** The patch is applied to a freshly loaded tree
  and the whole tree is written back, so all 1,291 leaves survive.
* **Never leave a half-written file.** ``config_tree.save`` writes to a temporary neighbour and
  moves it into place.
"""

import os

import app_state
from fastapi import APIRouter, Request

from . import config_schema, config_tree
from .app import error_response

# The kind a secret leaf renders as. A secret is masked and write-only: the browser is told
# whether a value is set, never what it is.
SECRET_KIND = 'secret'

# How a leaf's label is derived when neither the schema nor the key itself gives a good one.
# The schema (`webapp/config_schema.py`) supplies the real labels; this is the fallback for a
# path it has no entry for, which is what keeps a new leaf reachable.
_LABEL_OVERRIDES = {
    'api': 'API',
    'apis': 'API slots',
    'url': 'URL',
    'dbname': 'Database name',
    'rpm': 'Requests per minute',
    'id': 'ID',
}


def install_config(app):
    """Attach the Config editor's routes to `app`. Called from `create_app`."""
    router = APIRouter(prefix='/api/config')

    @router.get('')
    def read_config():
        """The whole tree, redacted and pre-walked, plus the section list.

        One request, because the editor is one screen with a left nav: fetching a section at a
        time would mean the nav could not show a leaf count and a save could not be checked
        against the whole tree.
        """
        try:
            tree = config_tree.load(_config_path())
        except config_tree.ConfigTreeError as exc:
            return error_response('failed', str(exc), status=500)

        sections = []
        for entry in config_tree.sections(tree):
            sections.append({
                'key': entry['key'],
                'label': label_for(entry['key'], entry['key']),
                'leaves': entry['leaves'],
            })

        return {
            'ok': True,
            'path': _config_path(),
            'sections': sections,
            'nodes': [build_node(tree[entry['key']], entry['key'], entry['key'])
                      for entry in sections],
            'leaf_count': sum(entry['leaves'] for entry in sections),
            'secret_count': sum(1 for path, _value in config_tree.leaves(tree)
                                if config_tree.is_secret(path)),
        }

    @router.post('/save')
    async def save_config(request: Request):
        """Apply a patch of ``{path: value}`` and write the tree back.

        The patch is applied to a **freshly loaded** tree rather than to server-held state, so a
        save cannot resurrect a value another client changed in the meantime, and a restart
        cannot lose an edit that was never in the file.

        A path that does not exist yet is **created** -- that is what makes "add" the same
        operation as "edit". The created paths are reported so the UI can say so.
        """
        body = await _json_body(request)
        if body is None:
            return error_response('bad_request', 'The body must be a JSON object.', status=400)
        values = body.get('values')
        if not isinstance(values, dict) or not values:
            return error_response('bad_request',
                                  'A save needs a non-empty "values" object.', status=400)

        try:
            tree = config_tree.load(_config_path())
        except config_tree.ConfigTreeError as exc:
            return error_response('failed', str(exc), status=500)

        errors = {}
        changed = []
        created = []
        coerced = {}
        for path, raw in values.items():
            if not isinstance(path, str) or not path:
                errors[str(path)] = 'A path must be a non-empty string.'
                continue
            try:
                current = config_tree.get(tree, path, _MISSING)
                if current is _MISSING:
                    value = raw
                    created.append(path)
                else:
                    value = coerce(raw, current)
                    # A coded field is stored in its decoded form: the browser sends the display
                    # text (`\n` as two characters) and the file holds a real newline.
                    if isinstance(value, str):
                        value = config_schema.decode(path, value)
                coerced[path] = value
                if current is not _MISSING and value == current and type(value) is type(current):
                    continue
                config_tree.set_value(tree, path, value)
                changed.append(path)
            except (config_tree.ConfigTreeError, ValueError) as exc:
                errors[path] = str(exc)

        # The range, enum, URL, regex and cross-field checks the Tk editor runs before it writes.
        # They run against the tree with the patch applied, so a rule sees the state the
        # user is asking for. A rejected save writes nothing, and every bad field is reported.
        if not errors:
            errors.update(config_schema.validate(tree, coerced))

        if errors:
            # Nothing is written when anything is rejected: a half-applied patch is a config
            # the user cannot reason about, and the editor would show a lie.
            return error_response('invalid', _first_error(errors), status=422,
                                  extra={'fields': errors})

        # A masked field the browser did not touch must not blank the stored key.
        stored = config_tree.load(_config_path())
        merged = config_tree.merge_secrets(tree, stored)
        for path, value in list(config_tree.leaves(merged)):
            config_tree.set_value(tree, path, value)

        try:
            config_tree.save(tree, _config_path())
        except config_tree.ConfigTreeError as exc:
            return error_response('failed', str(exc), status=500)

        reloaded = _reload_engine_config()
        return {
            'ok': True,
            'data': {
                'path': _config_path(),
                'changed': changed,
                'created': created,
                'reloaded': reloaded,
            },
        }

    @router.post('/delete')
    async def delete_key(request: Request):
        """Remove one key. Used to take back a key that was added by mistake."""
        body = await _json_body(request)
        if body is None:
            return error_response('bad_request', 'The body must be a JSON object.', status=400)
        path = body.get('path')
        if not isinstance(path, str) or not path:
            return error_response('bad_request', 'A delete needs a "path".', status=400)
        if '.' not in path:
            return error_response('refused',
                                  'A whole section cannot be deleted from the editor.',
                                  status=403)
        try:
            tree = config_tree.load(_config_path())
            removed = config_tree.delete(tree, path)
            if not removed:
                return error_response('not_found', f'{path} is not in the config.', status=404)
            config_tree.save(tree, _config_path())
        except config_tree.ConfigTreeError as exc:
            return error_response('failed', str(exc), status=500)
        _reload_engine_config()
        return {'ok': True, 'data': {'path': path, 'deleted': True}}

    app.include_router(router)
    return app


# --- the renderable description ------------------------------------------

def build_node(value, path, key):
    """Describe `value` as one node the renderer can draw.

    Four shapes, and that is the whole vocabulary:

    ``group``
        A mapping, or one index of a list of mappings. Carries ``children``.
    ``list``
        A flat list of scalars. Carries ``items``. Rendered as one control, because 1,000
        individual inputs is not a form -- ``prompts.random_names.names`` is exactly this.
    ``secret``
        A masked leaf. Carries ``has_value`` and never the value.
    a scalar kind
        ``text`` / ``textarea`` / ``number`` / ``bool``, from :func:`config_tree.kind_of`.
    """
    label = label_for(key, path)

    if isinstance(value, dict):
        return {
            'kind': 'group',
            'path': path,
            'label': label,
            **_decoration(path, 'group'),
            'children': [build_node(child, f'{path}.{child_key}', str(child_key))
                         for child_key, child in value.items()],
        }

    if isinstance(value, list):
        if value and all(isinstance(item, dict) for item in value):
            # A list of mappings is a set of repeated groups -- API slots, for instance.
            return {
                'kind': 'group',
                'path': path,
                'label': label,
                **_decoration(path, 'group'),
                'count': len(value),
                'children': [build_node(item, f'{path}.{index}', f'#{index + 1}')
                             for index, item in enumerate(value)],
            }
        return {
            'kind': 'list',
            'path': path,
            'label': label,
            **_decoration(path, 'list'),
            'count': len(value),
            'items': ['' if item is None else str(item) for item in value],
        }

    if config_tree.is_secret(path):
        return {
            'kind': SECRET_KIND,
            'path': path,
            'label': label,
            **_decoration(path, SECRET_KIND),
            'value': config_tree.REDACTED,
            'has_value': bool(value),
        }

    kind = config_tree.kind_of(value)
    return {
        'kind': kind,
        'path': path,
        'label': label,
        **_decoration(path, kind),
        # A value with a codec is shown in its display form -- the append separator is stored as a
        # real newline and shown as `\n`, because a single-line input cannot hold a newline.
        'value': '' if value is None else config_schema.encode(path, value),
    }


def _decoration(path, kind):
    """The schema's label, help, choices and bounds for one node, minus the keys it repeats.

    `describe` may refine the kind of a scalar leaf (a long prompt body becomes a `textarea` even
    when its length alone would not say so), so the caller's `kind` is replaced when the schema has
    an opinion. A group, a flat list and a secret keep the shape the tree gave them.
    """
    decoration = config_schema.describe(path, kind)
    decoration.pop('label', None)  # the caller already resolved the label, with its fallback
    return decoration


def label_for(key, path=None):
    """A human label for a node: the schema's, else the key itself, title-cased.

    `path` is the full dot path when the caller has one, because the schema is keyed by path --
    `api.apis.0.url` and `api.apis.1.url` share one entry, and a bare `url` would not find it.
    """
    text = str(key)
    if path is not None:
        label = config_schema.label_for(path, '')
        if label:
            return label
    if text in _LABEL_OVERRIDES:
        return _LABEL_OVERRIDES[text]
    return text.replace('_', ' ').replace('-', ' ').strip().title()


# --- writing -------------------------------------------------------------

_MISSING = object()


def coerce(raw, current):
    """Convert `raw` (from a browser input) to the type `current` already has.

    This is the one rule that keeps a save from corrupting the config: a number field arrives as
    the string ``"10"`` and must be stored as ``10``, or ``run_config``'s arithmetic gets a
    string. A value that cannot convert raises ``ValueError`` with a message naming the field,
    and the whole save is rejected rather than half-applied.
    """
    if isinstance(current, bool):
        if isinstance(raw, bool):
            return raw
        text = str(raw).strip().lower()
        if text in ('true', '1', 'yes', 'on'):
            return True
        if text in ('false', '0', 'no', 'off', ''):
            return False
        raise ValueError(f'{raw!r} is not a yes/no value.')

    if isinstance(current, int):
        if isinstance(raw, bool):
            raise ValueError(f'{raw!r} is not a whole number.')
        if isinstance(raw, int):
            return raw
        if isinstance(raw, float):
            if raw != int(raw):
                raise ValueError(f'{raw!r} is not a whole number.')
            return int(raw)
        text = str(raw).strip()
        try:
            return int(text)
        except ValueError:
            pass
        try:
            number = float(text)
        except ValueError:
            raise ValueError(f'{raw!r} is not a whole number.') from None
        if number != int(number):
            raise ValueError(f'{raw!r} is not a whole number.')
        return int(number)

    if isinstance(current, float):
        if isinstance(raw, bool):
            raise ValueError(f'{raw!r} is not a number.')
        try:
            return float(raw)
        except (TypeError, ValueError):
            raise ValueError(f'{raw!r} is not a number.') from None

    if isinstance(current, str):
        if isinstance(raw, (dict, list)):
            raise ValueError('A text field cannot hold a list or a mapping.')
        return '' if raw is None else str(raw)

    if isinstance(current, list):
        if not isinstance(raw, list):
            raise ValueError('A list field needs a list.')
        return raw

    if isinstance(current, dict):
        if not isinstance(raw, dict):
            raise ValueError('A group field needs a mapping.')
        return raw

    return raw


def _first_error(errors):
    """One sentence naming the first rejected field, for the response's `message`."""
    path = sorted(errors)[0]
    return f'{path}: {errors[path]}'


def _config_path():
    """The config file the engine is actually running from."""
    return app_state.global_config.path


def _reload_engine_config():
    """Re-read the config into the running engine, and re-apply the rate limits.

    A save that only changed the file would leave the engine running the old values until the
    next restart, which reads as "the editor does not work". The Tk editor reloads for the same
    reason, and pushes the API keys into ``os.environ`` because ``run_config`` reads them from
    there first.
    """
    try:
        app_state.global_config.load()
    except Exception:  # noqa: BLE001 - a reload must not turn a good save into an error
        return False
    _push_api_keys()
    try:
        import run_config
        run_config.apply_rate_limits(
            run_config.build_api_configs(app_state.global_config)[0])
    except Exception:  # noqa: BLE001 - rate limits are a nicety, not the save
        pass
    return True


def _push_api_keys():
    """Mirror the API slots into ``API_URL_n`` / ``MODEL_NAME_n`` / ``API_KEY_n``.

    ``run_config.build_api_configs`` prefers these environment variables over the file, so
    without this a key edited in the browser would be shadowed by a stale value in the
    environment. The Tk editor does exactly this on save. Keys stay server-side; nothing
    here is ever sent to the browser.
    """
    slots = app_state.global_config.get('api.apis', []) or []
    for index, entry in enumerate(slots):
        if not isinstance(entry, dict):
            continue
        number = index + 1
        os.environ[f'API_URL_{number}'] = str(entry.get('url') or '')
        os.environ[f'MODEL_NAME_{number}'] = str(entry.get('model') or '')
        os.environ[f'API_KEY_{number}'] = str(entry.get('key') or '')


async def _json_body(request: Request):
    """The request body as a dict, or ``None`` when it is not one.

    The parameter must be annotated ``Request``: an unannotated one is read by FastAPI as a
    **query** field, so every save would be rejected with a 422 before the body was looked at.
    That is exactly what happened on the first live save.
    """
    import json

    try:
        body = await request.json()
    except (ValueError, json.JSONDecodeError):
        return None
    return body if isinstance(body, dict) else None
