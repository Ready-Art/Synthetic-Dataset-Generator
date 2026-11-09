# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""The Datasets panel's routes -- Phase 6d-a.

The Tk panel (`datasets_panel.py`, 696 lines) is replaced by this module plus
`webapp/static/js/datasets.js`. **The model layer is not reimplemented**: `dataset_settings.py`
is already GUI-free and import-clean, and it is the only thing that defines the persisted shape,
so every route here calls into it rather than assembling a source dict by hand.

Why the model layer is reused rather than reimplemented
-----------------------------------------------------
`dataset_settings.normalize_config` is the one definition of the wire shape -- exactly 13 keys per
source, unknown keys dropped, `source_id` never regenerated. A second implementation here would drift
from the one `configure_queue_datasets` reads at run start, and the failure would be silent: a source that
looks right in the editor and binds nothing at run time. So the routes are thin, and the rules live where
they already lived.

Why every write goes through `config_tree`
----------------------------------------
`config/config.yml` carries 113 comment lines including the SPDX header. `yaml.dump` deletes all of
them, so a save here uses `config_tree.save`, which splices only the changed span. Adding the first
dataset to the shipped `datasets: {sources: []}` is the case that caught a real bug.

What this module must never do
-----------------------------
* **Never regenerate `source_id`.** It is the identity a saved task binding records, so a round trip that
  re-keys a source orphans every row already generated from it.
* **Never write an invalid subtree.** `validate_config` runs before the write, because the generic
  `/api/config/save` route does **not** validate this path (`config_schema.RULES` has no datasets entry),
  so a bad source written through it would only fail later, at `configure_queue_datasets`.
* **Never import `datasets_panel.py`.** It imports `tkinter`, which would make the web package need a
  display. `_KINDS` is duplicated here from `dataset_sources` instead.
* **Never open a source on a read.** `open_source(...).schema()` performs real I/O (it opens files and
  memory-maps parquet), so it is confined to the one import route.
"""

import app_state
from fastapi import APIRouter, Request

import dataset_settings
import dataset_templates
from dataset_sources import DatasetSourceError, open_source

from . import config_tree
from .app import error_response

# The seven kinds, with the label the Tk combo shows. The model stores the kind, the UI shows the
# label, exactly as `datasets_panel._KINDS` does. Duplicated from `dataset_sources._KINDS` rather
# than imported from `datasets_panel`, because that module imports `tkinter`.
KINDS = (
    ('csv', 'CSV'),
    ('tsv', 'TSV'),
    ('json', 'JSON array'),
    ('jsonl', 'JSONL'),
    ('parquet', 'Parquet'),
    ('huggingface', 'Hugging Face Hub'),
    ('hf_disk', 'Saved HF dataset'),
)

# The keys a field edit may set. Everything else in a source is derived: `columns` and
# `schema_fingerprint` come from an import, `visible_columns` from the visibility route, and
# `column_aliases` from the alias route. Accepting them here would let the browser forge a schema.
EDITABLE_FIELDS = ('name', 'enabled', 'kind', 'location', 'config', 'split', 'revision')

# Which selector fields each kind accepts. Mirrors `datasets_panel._update_kind_controls` and, more
# importantly, `dataset_sources.SourceSpec.__post_init__`, which rejects the others.
KIND_FIELDS = {
    'huggingface': ('config', 'split', 'revision'),
    'hf_disk': ('split',),
}

# The status lines the Tk panel shows, kept verbatim so the two front ends read the same. A user who
# has used the Tk panel recognises them.
STATUS = {
    'empty': 'Add a dataset, import its column names, then choose aliases for the columns you need.',
    'importing': 'Reading column names. Dataset records are not displayed.',
    'stale': 'Source settings changed. Import columns again before using these mappings.',
    'has_columns': 'Select a column to edit its alias. Hiding a column keeps its alias mapping.',
    'no_columns': 'This source has no columns.',
    'hub': 'Enter owner/dataset as the location. Set a config or split when the source has '
           'multiple choices.',
    'default': 'Import column names to create mappings. Dataset records are not displayed.',
}


def install_datasets(app):
    """Attach the Datasets panel's routes to `app`. Called from `create_app`."""
    router = APIRouter(prefix='/api/datasets')

    @router.get('')
    def read_datasets():
        """Every source, normalized, plus the kind list and the per-source status line.

        The status is computed here rather than in the browser because it depends on
        `source_selector(source) != schema_selector`, which is the model's own staleness rule.
        """
        try:
            tree = config_tree.load(_config_path())
        except config_tree.ConfigTreeError as exc:
            return error_response('failed', str(exc), status=500)
        sources = _sources(tree)
        return {
            'ok': True,
            'path': _config_path(),
            'sources': sources,
            'kinds': [{'kind': kind, 'label': label} for kind, label in KINDS],
            'status': [_status_for(source) for source in sources],
            'counts': _counts(sources),
        }

    @router.post('/sources')
    async def add_source(request: Request):
        """Append one blank source. **No I/O**: no metadata read, no records.

        `new_source` picks the lowest unused `Dataset N` name and a fresh `source_id`, so two
        adds in a row cannot collide.
        """
        body = await _json_body(request)
        if body is None:
            return error_response('bad_request', 'The body must be a JSON object.', status=400)
        try:
            tree = config_tree.load(_config_path())
            sources = _sources(tree)
            source = dataset_settings.new_source(sources)
            sources.append(source)
            _write_sources(tree, sources)
        except (config_tree.ConfigTreeError, dataset_settings.DatasetSettingsError) as exc:
            return error_response('failed', str(exc), status=500)
        return {'ok': True, 'data': {'source': source, 'sources': sources}}

    @router.patch('/sources/{source_id}')
    async def edit_source(source_id: str, request: Request):
        """Set the seven editable fields on one source.

        A field edit that changes `source_selector` **invalidates the import**: the cached columns
        and fingerprint describe the old location, and `validate_config` would reject the save with
        "import columns again" without saying which field moved. Clearing them here makes the panel
        show the stale status instead of a mapping that cannot be saved.
        """
        body = await _json_body(request)
        if body is None:
            return error_response('bad_request', 'The body must be a JSON object.', status=400)
        fields = body.get('fields')
        if not isinstance(fields, dict) or not fields:
            return error_response('bad_request',
                                  'An edit needs a non-empty "fields" object.', status=400)
        unknown = sorted(set(fields) - set(EDITABLE_FIELDS))
        if unknown:
            return error_response('bad_request',
                                  f'These fields cannot be edited: {", ".join(unknown)}.',
                                  status=400)
        try:
            tree = config_tree.load(_config_path())
            sources = _sources(tree)
            source = _find(sources, source_id)
            before = dataset_settings.source_selector(source)
            for key, value in fields.items():
                source[key] = value
            # `normalize_config` is what enforces the type of each field (a bool is not 1, a
            # name is a string), so the edit is normalized rather than trusted.
            normalized = dataset_settings.normalize_config({'sources': sources})['sources']
            source = _find(normalized, source_id)
            if dataset_settings.source_selector(source) != before:
                source['columns'] = []
                source['column_aliases'] = {}
                source['visible_columns'] = None
                source['schema_selector'] = None
                source['schema_fingerprint'] = None
            _write_sources(tree, normalized)
        except _NotFound as exc:
            return error_response('not_found', str(exc), status=404)
        except (config_tree.ConfigTreeError, dataset_settings.DatasetSettingsError) as exc:
            return error_response('invalid', str(exc), status=422)
        return {'ok': True, 'data': {'source': source, 'status': _status_for(source)}}

    @router.delete('/sources/{source_id}')
    def delete_source(source_id: str):
        """Remove one source. The others keep their order and their ids."""
        try:
            tree = config_tree.load(_config_path())
            sources = _sources(tree)
            _find(sources, source_id)
            remaining = [source for source in sources if source['source_id'] != source_id]
            _write_sources(tree, remaining)
        except _NotFound as exc:
            return error_response('not_found', str(exc), status=404)
        except (config_tree.ConfigTreeError, dataset_settings.DatasetSettingsError) as exc:
            return error_response('failed', str(exc), status=500)
        return {'ok': True, 'data': {'sources': remaining}}

    @router.put('/sources/{source_id}/aliases/{column}')
    async def set_alias(source_id: str, column: str, request: Request):
        """Map one column to one alias, or clear the mapping when the alias is empty.

        An empty alias **deletes** the mapping rather than storing `''`, matching
        `normalize_config`'s own rule that a cleared GUI entry is an unmapped column.
        """
        body = await _json_body(request)
        if body is None:
            return error_response('bad_request', 'The body must be a JSON object.', status=400)
        alias = body.get('alias')
        if not isinstance(alias, str):
            return error_response('bad_request', 'An alias must be a string.', status=400)
        try:
            tree = config_tree.load(_config_path())
            sources = _sources(tree)
            source = _find(sources, source_id)
            if column not in source['columns']:
                return error_response(
                    'invalid',
                    f'{column!r} is not an imported column of this source. '
                    'Import columns first.', status=422)
            if alias.strip():
                # `validate_alias` is the model's own rule, so the route calls it rather than
                # carrying a second copy of the pattern. Without this a bad alias was accepted and
                # written, and only failed later at `configure_queue_datasets` -- or at
                # `validate_config`, which would blame the whole subtree.
                dataset_templates.validate_alias(alias)
                source['column_aliases'][column] = alias
            else:
                source['column_aliases'].pop(column, None)
            sources = _write_sources(tree, sources)
        except _NotFound as exc:
            return error_response('not_found', str(exc), status=404)
        except (config_tree.ConfigTreeError, dataset_settings.DatasetSettingsError,
                dataset_templates.DatasetTemplateError) as exc:
            return error_response('invalid', str(exc), status=422)
        updated = _find(sources, source_id)
        return {'ok': True, 'data': {'source': updated, 'status': _status_for(updated)}}

    @router.put('/sources/{source_id}/visibility')
    async def set_visibility(source_id: str, request: Request):
        """Show all, none, or a chosen set of columns. **Never touches an alias.**

        Visibility is presentation only: a hidden column keeps its alias active, which is why
        `with_visible_columns` is used rather than rebuilding the source.
        """
        body = await _json_body(request)
        if body is None:
            return error_response('bad_request', 'The body must be a JSON object.', status=400)
        if 'columns' not in body:
            return error_response('bad_request',
                                  'A visibility change needs a "columns" key '
                                  '(null for all, [] for none).', status=400)
        selection = body.get('columns')
        try:
            tree = config_tree.load(_config_path())
            sources = _sources(tree)
            source = _find(sources, source_id)
            updated = dataset_settings.with_visible_columns(source, selection)
            sources = [updated if entry['source_id'] == source_id else entry
                        for entry in sources]
            sources = _write_sources(tree, sources)
        except _NotFound as exc:
            return error_response('not_found', str(exc), status=404)
        except (config_tree.ConfigTreeError, dataset_settings.DatasetSettingsError) as exc:
            return error_response('invalid', str(exc), status=422)
        updated = _find(sources, source_id)
        return {'ok': True, 'data': {'source': updated, 'status': _status_for(updated)}}

    @router.post('/sources/{source_id}/import')
    def import_columns(source_id: str):
        """Read the source's column names. **The only route here that performs I/O.**

        Names only: `open_source(spec).schema()` never returns a row value, so a dataset's contents
        cannot reach the browser through this route.

        The response carries the `selector` the read used. A read can take seconds, and the source can
        be edited while it runs, so the client compares the returned selector against the source's
        current one and drops the result when they differ -- the same guard the Tk panel's `token` +
        `selector` check provides.
        """
        try:
            tree = config_tree.load(_config_path())
            sources = _sources(tree)
            source = _find(sources, source_id)
            selector = dataset_settings.source_selector(source)
            spec = dataset_settings.source_spec(source)
        except _NotFound as exc:
            return error_response('not_found', str(exc), status=404)
        except (config_tree.ConfigTreeError, dataset_settings.DatasetSettingsError) as exc:
            return error_response('invalid', str(exc), status=422)

        reader = None
        try:
            reader = open_source(spec)
            schema = reader.schema()
        except DatasetSourceError as exc:
            return error_response('failed', f'Import failed: {exc}', status=422)
        except Exception as exc:  # noqa: BLE001 - a reader can raise anything
            return error_response('failed',
                                  f'Import failed: {exc or type(exc).__name__}', status=422)
        finally:
            if reader is not None:
                try:
                    reader.close()
                except Exception:  # noqa: BLE001 - a close failure must not mask a read
                    pass

        # Re-read the tree: the read may have taken seconds, and the source may have moved. The
        # update is applied to the source as it is **now**, and only when its selector still
        # matches the one the read used.
        try:
            tree = config_tree.load(_config_path())
            sources = _sources(tree)
            current = _find(sources, source_id)
            if dataset_settings.source_selector(current) != selector:
                return error_response(
                    'stale',
                    'The source settings changed while the columns were being read. '
                    'Import again.', status=409)
            updated = dataset_settings.apply_schema(current, schema)
            sources = [updated if entry['source_id'] == source_id else entry
                        for entry in sources]
            sources = _write_sources(tree, sources, validate=True)
        except _NotFound as exc:
            return error_response('not_found', str(exc), status=404)
        except (config_tree.ConfigTreeError, dataset_settings.DatasetSettingsError) as exc:
            return error_response('invalid', str(exc), status=422)

        updated = _find(sources, source_id)
        return {
            'ok': True,
            'data': {
                'source': updated,
                'selector': selector,
                'columns': list(schema.columns),
                'status': _status_for(updated),
            },
        }

    @router.post('/validate')
    def validate():
        """Run the engine's own `validate_config` against the file as it stands.

        The panel shows this before a save so a mapping that cannot be used is caught in the editor
        rather than at run start. It is the same call `configure_queue_datasets` makes, so the two
        cannot disagree.
        """
        try:
            tree = config_tree.load(_config_path())
            dataset_settings.validate_config({'sources': _sources(tree)})
        except config_tree.ConfigTreeError as exc:
            return error_response('failed', str(exc), status=500)
        except dataset_settings.DatasetSettingsError as exc:
            return error_response('invalid', str(exc), status=422)
        return {'ok': True, 'data': {'valid': True}}

    app.include_router(router)
    return app


# --- reading and writing the subtree --------------------------------------

class _NotFound(Exception):
    """A `source_id` that is not in the config."""


def _config_path():
    """The config file the engine is actually running from."""
    return app_state.global_config.path


def _sources(tree):
    """The normalized source list out of a loaded tree.

    `normalize_config` is the only definition of the shape, so a source read here is the same
    source `configure_queue_datasets` would see. A malformed subtree raises rather than being
    silently repaired, because a repair would hide a hand-edited config that is wrong.
    """
    datasets = config_tree.get(tree, 'datasets', {})
    return dataset_settings.normalize_config(datasets)['sources']


def _write_sources(tree, sources, validate=False):
    """Normalize the source list and write it back through the comment-preserving writer.

    `normalize_config` always runs: it is what enforces the shape and each field's type, so a
    source written here is the same source `configure_queue_datasets` will read.

    `validate_config` runs only where an edit can make a mapping **active** -- setting an alias.
    It is deliberately not run on add, delete, a field edit or a visibility change, because
    `validate_config` rejects the whole subtree when **any** enabled source has a stale import, so
    running it there would let one unfinished source block adding a second. The `/validate` route
    runs the full check on demand, and the panel calls it before a save.
    """
    normalized = dataset_settings.normalize_config({'sources': sources})['sources']
    if validate:
        dataset_settings.validate_config({'sources': normalized})
    config_tree.set_value(tree, 'datasets.sources', normalized)
    config_tree.save(tree, _config_path())
    _reload_engine_config()
    return normalized


def _find(sources, source_id):
    """The source with `source_id`, or `_NotFound`."""
    for source in sources:
        if source.get('source_id') == source_id:
            return source
    raise _NotFound(f'No dataset source with id {source_id!r}.')


def _status_for(source):
    """The status line for one source, in the Tk panel's own words.

    Computed server-side because the staleness rule is the model's
    (`source_selector(source) != schema_selector`), and a second copy in JS would drift.
    """
    if not source:
        return {'text': STATUS['empty'], 'level': 'muted'}
    if (source.get('schema_selector') is not None
            and dataset_settings.source_selector(source) != source['schema_selector']):
        return {'text': STATUS['stale'], 'level': 'warn'}
    if source.get('columns'):
        return {'text': STATUS['has_columns'], 'level': 'muted'}
    if source.get('schema_fingerprint'):
        return {'text': STATUS['no_columns'], 'level': 'muted'}
    if source.get('kind') == 'huggingface':
        return {'text': STATUS['hub'], 'level': 'muted'}
    return {'text': STATUS['default'], 'level': 'muted'}


def _counts(sources):
    """The three numbers the panel's header shows."""
    return {
        'sources': len(sources),
        'enabled': sum(1 for source in sources if source.get('enabled')),
        'mapped': sum(len(source.get('column_aliases') or {}) for source in sources),
        'columns': sum(len(source.get('columns') or []) for source in sources),
    }


def _reload_engine_config():
    """Re-read the config into the running engine, so a save applies without a restart.

    A save that only changed the file would leave the engine running the old datasets until the next
    restart, which reads as "the panel does not work". The Config editor reloads for the same reason.
    """
    try:
        app_state.global_config.load()
    except Exception:  # noqa: BLE001 - a reload must not turn a good save into an error
        return False
    return True


async def _json_body(request: Request):
    """The request body as a dict, or ``None`` when it is not one.

    The parameter must be annotated ``Request``: an unannotated one is read by FastAPI as a **query**
    field, so every request would be rejected with a 422 before the body was looked at. That is the
    trap Phase 6b found on the first live save.
    """
    import json

    try:
        body = await request.json()
    except (ValueError, json.JSONDecodeError):
        return None
    return body if isinstance(body, dict) else None
