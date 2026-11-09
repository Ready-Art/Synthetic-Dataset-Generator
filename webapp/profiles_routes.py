# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""The Profiles panel's routes -- Phase 6d-b.

The last bespoke widget. The Tk editor's Profiles tab is three buttons over
`ConfigLoader.list_profiles` / `save_profile` / `delete_profile` /
`load_profile_to_main_config`; this module is the same three operations, plus the preview a
destructive load needs and the two safety rules the model layer does not have.

Why this is not a thin wrapper over `ConfigLoader`
--------------------------------------------------
It is a wrapper over `ConfigLoader`'s *contracts*, not its code, because two of its four
methods are unsafe informationaly:

* **`load_profile_to_main_config` is `yaml.dump` over `config.yml`.** That is the exact
  operation measured as deleting all 113 comment lines -- including the SPDX header --
  and re-wrapping 122 lines of prose. Loading any profile through it would turn a
  41,992-byte file into a 33,059-byte one. So a load here **splices** through
  `config_tree`, and an unchanged leaf is never re-rendered.
* **`delete_profile` and `load_profile_to_main_config` do not sanitize the name.** Only
  `save_profile` does. Both build `os.path.join(profiles_dir, f'{name}.yml')` from the raw
  argument, so `name = '../../config/config'` resolves to the live `config.yml` -- a delete
  of the running config, or a load of it. `_check_name` closes that here by requiring the
  name to survive `ConfigLoader`'s own sanitization **unchanged** and by checking the
  resolved path's parent. The underlying `config_loader.py` bug is not fixed here: that file
  is shared with the Tk editor.

Why a profile is written with the secrets masked
------------------------------------------------
`config_editor._get_current_editor_config_data` puts `key` into `api.apis.*` and
`quality.scoring_api`, so a profile is a **whole config document including the 9 secret
leaves** -- and `config/profiles/` is not gitignored. A profile saved from here therefore
holds `***` where a secret was, and a load puts the *current* real value back through
`config_tree.merge_secrets`. That keeps a key out of a directory that can be committed
without blanking the key when the profile is applied.

What this module must never do
------------------------------
* **Never `yaml.dump` `config.yml`.** A load is a splice, so it is a small diff.
* **Never regenerate a `source_id`.** A profile carries `datasets.sources`, and the id is the
  identity a saved task binding records.
* **Never write a profile that failed validation**, and never apply one. Every bad field is
  reported at once, and nothing is written.
* **Never accept a name that is a path.** See `_check_name`.
"""

import copy
import os
import time

import app_state
from fastapi import APIRouter, Request

import dataset_settings

from . import config_schema, config_text, config_tree
from .app import error_response

# A profile name is a filename stem, so it is capped well below any filesystem's limit and
# far below anything a user would type.
MAX_NAME_LENGTH = 64

# The characters `ConfigLoader.save_profile` keeps. A name that this would change is refused
# rather than silently rewritten, so the name the user typed is the name on disk.
NAME_ALLOWED_EXTRA = (' ', '_', '-')


def install_profiles(app):
    """Attach the Profiles panel's routes to `app`. Called from `create_app`."""
    router = APIRouter(prefix='/api/profiles')

    @router.get('')
    def read_profiles():
        """Every profile on disk, with enough metadata to choose one.

        A file that will not parse is reported as `broken` rather than omitted: a profile the
        user saved and can no longer see is worse than one that says why it cannot be used.
        """
        directory = _profiles_dir()
        try:
            names = sorted(entry[:-4] for entry in os.listdir(directory)
                           if entry.endswith('.yml'))
        except FileNotFoundError:
            # A fresh checkout has no `config/profiles/` until something writes one. An absent
            # directory is an empty list, not a 500: the panel has to be able to say "no
            # profiles yet" and offer the save form.
            names = []
        except OSError as exc:
            return error_response('failed', f'Could not read {directory}: {exc}', status=500)

        profiles = [_describe(directory, name) for name in names]
        return {
            'ok': True,
            'path': directory,
            'profiles': profiles,
            'counts': {
                'profiles': len(profiles),
                'broken': sum(1 for entry in profiles if entry['broken']),
            },
        }

    @router.post('')
    async def save_profile(request: Request):
        """Write the live config to `profiles/<name>.yml`, with the secrets masked.

        The file is a **commented copy of `config.yml`**, not a `yaml.dump` of it: the source
        text is reused and only the 9 secret leaves are re-rendered, so a profile is readable
        and diffable and keeps the SPDX header.

        `overwrite` must be explicit. A save that silently replaced a profile the user had
        tuned would be the one mistake that makes profiles untrustworthy.
        """
        body = await _json_body(request)
        if body is None:
            return error_response('bad_request', 'The body must be a JSON object.', status=400)
        try:
            name = _check_name(body.get('name'))
        except _BadName as exc:
            return error_response('invalid', str(exc), status=422)
        overwrite = bool(body.get('overwrite'))

        directory = _profiles_dir()
        path = os.path.join(directory, f'{name}.yml')
        if os.path.exists(path) and not overwrite:
            return error_response('exists', f'A profile named {name!r} already exists.',
                                  status=409)

        try:
            tree = config_tree.load(_config_path())
        except config_tree.ConfigTreeError as exc:
            return error_response('failed', str(exc), status=500)

        # Mask the secrets and splice the masked leaves into the live file's own text, so the
        # profile is a commented copy rather than a re-dump. `config_text.dumps` wants a plain
        # dict, which is what `_mask` returns.
        masked = _mask(tree)
        try:
            text = config_text.dumps(masked, tree.source_text, tree.source_tree)
        except config_text.ConfigTextError as exc:
            return error_response('failed', f'Could not render the profile: {exc}', status=500)

        try:
            os.makedirs(directory, exist_ok=True)
            _write_atomic(path, text)
        except OSError as exc:
            return error_response('failed', f'Could not write {path}: {exc}', status=500)

        return {'ok': True, 'data': {'name': name, 'path': path,
                                     'overwrite': overwrite,
                                     'profile': _describe(directory, name)}}

    @router.get('/{name}')
    def preview_profile(name: str):
        """What loading this profile **would** change. Writes nothing.

        A load replaces the whole config, so the user has to be able to see the diff and any
        validation error before committing to it. This is a GET because it changes nothing;
        the load is the POST below, so a link or a prefetch cannot fire it.
        """
        try:
            safe = _check_name(name)
        except _BadName as exc:
            return error_response('invalid', str(exc), status=422)
        directory = _profiles_dir()
        path = os.path.join(directory, f'{safe}.yml')
        if not os.path.exists(path):
            return error_response('not_found', f'No profile named {safe!r}.', status=404)

        try:
            profile = config_tree.load(path)
        except config_tree.ConfigTreeError as exc:
            return error_response('invalid', f'{safe!r} is not a usable profile: {exc}',
                                  status=422)
        try:
            current = config_tree.load(_config_path())
        except config_tree.ConfigTreeError as exc:
            return error_response('failed', str(exc), status=500)

        candidate = _candidate(profile, current)
        errors = _validate(candidate)
        return {
            'ok': True,
            'data': {
                'name': safe,
                'path': path,
                'diff': _diff(current, candidate),
                'errors': errors,
                'valid': not errors,
                'leaf_count': sum(1 for _ in config_tree.leaves(candidate)),
            },
        }

    @router.post('/{name}/load')
    def load_profile(name: str):
        """Apply a profile to `config.yml`, splicing only the leaves that changed.

        **This is the destructive route.** It validates the candidate tree first -- the
        `datasets` subtree through the engine's own `validate_config`, and every leaf through
        `config_schema.validate` -- and writes nothing when anything is rejected.
        """
        try:
            safe = _check_name(name)
        except _BadName as exc:
            return error_response('invalid', str(exc), status=422)
        directory = _profiles_dir()
        path = os.path.join(directory, f'{safe}.yml')
        if not os.path.exists(path):
            return error_response('not_found', f'No profile named {safe!r}.', status=404)

        try:
            profile = config_tree.load(path)
        except config_tree.ConfigTreeError as exc:
            return error_response('invalid', f'{safe!r} is not a usable profile: {exc}',
                                  status=422)
        try:
            tree = config_tree.load(_config_path())
        except config_tree.ConfigTreeError as exc:
            return error_response('failed', str(exc), status=500)

        candidate = _candidate(profile, tree)
        errors = _validate(candidate)
        if errors:
            return error_response(
                'invalid',
                f'{safe!r} cannot be loaded: {_first(errors)}',
                status=422,
                extra={'fields': errors})

        diff = _diff(tree, candidate)
        # Apply the candidate's leaves onto the live tree, so the save splices against the
        # live file's own text and every untouched byte survives.
        for leaf_path, value in list(config_tree.leaves(candidate)):
            config_tree.set_value(tree, leaf_path, value)
        try:
            config_tree.save(tree, _config_path())
        except config_tree.ConfigTreeError as exc:
            return error_response('failed', str(exc), status=500)

        reloaded = _reload_engine_config()
        return {'ok': True, 'data': {'name': safe, 'diff': diff, 'reloaded': reloaded}}

    @router.delete('/{name}')
    def delete_profile(name: str):
        """Remove one profile file. The config is untouched."""
        try:
            safe = _check_name(name)
        except _BadName as exc:
            return error_response('invalid', str(exc), status=422)
        path = os.path.join(_profiles_dir(), f'{safe}.yml')
        if not os.path.exists(path):
            return error_response('not_found', f'No profile named {safe!r}.', status=404)
        try:
            os.remove(path)
        except OSError as exc:
            return error_response('failed', f'Could not delete {path}: {exc}', status=500)
        return {'ok': True, 'data': {'name': safe, 'deleted': True}}

    app.include_router(router)
    return app


# --- names ---------------------------------------------------------------

class _BadName(Exception):
    """A profile name that cannot be a filename in the profiles directory."""


def _check_name(name):
    """Return `name` if it is a safe filename stem, or raise `_BadName`.

    The check is deliberately **exact**: the name must be unchanged by
    `ConfigLoader.save_profile`'s own sanitizer, so the name the user typed is the name on
    disk and there is no second, quieter rename. That sanitizer keeps `[alnum _ - space]`, so
    a name containing `.`, `/` or `\\` -- the three characters a traversal needs -- is refused
    rather than rewritten.

    The resolved path is checked as well, because a name check and a path check are different
    claims and the second one is the one that matters.
    """
    if not isinstance(name, str):
        raise _BadName('A profile name must be a string.')
    candidate = name.strip()
    if not candidate:
        raise _BadName('A profile name cannot be empty.')
    if len(candidate) > MAX_NAME_LENGTH:
        raise _BadName(f'A profile name must be at most {MAX_NAME_LENGTH} characters.')
    sanitized = ''.join(char for char in candidate
                        if char.isalnum() or char in NAME_ALLOWED_EXTRA).rstrip()
    if sanitized != candidate:
        raise _BadName(
            'A profile name may only contain letters, digits, spaces, hyphens and '
            'underscores, and cannot end with a space or a hyphen.')
    if candidate in ('.', '..'):
        raise _BadName('A profile name cannot be "." or "..".')
    directory = os.path.realpath(_profiles_dir())
    resolved = os.path.realpath(os.path.join(directory, f'{candidate}.yml'))
    if os.path.dirname(resolved) != directory:
        raise _BadName('A profile name cannot point outside the profiles directory.')
    return candidate


# --- reading the directory ----------------------------------------------

def _describe(directory, name):
    """One profile's metadata, and whether it parses.

    A broken file is reported rather than hidden: the user saved it, so they should see that it
    cannot be used and be able to delete it.
    """
    path = os.path.join(directory, f'{name}.yml')
    entry = {'name': name, 'path': path, 'broken': False, 'error': None,
             'leaves': 0, 'sections': 0, 'size': 0, 'modified': None}
    try:
        entry['size'] = os.path.getsize(path)
        entry['modified'] = time.strftime('%Y-%m-%d %H:%M',
                                          time.localtime(os.path.getmtime(path)))
    except OSError:
        pass
    try:
        tree = config_tree.load(path)
    except config_tree.ConfigTreeError as exc:
        entry['broken'] = True
        # A YAML parser's message is a multi-line traceback-shaped dump. The first line says
        # what is wrong; the rest is a caret diagram, which belongs in a log rather than in a
        # status line beside a profile name.
        entry['error'] = _first_line(str(exc))
        return entry
    entry['leaves'] = sum(1 for _ in config_tree.leaves(tree))
    entry['sections'] = len(config_tree.sections(tree))
    return entry


# --- the candidate tree --------------------------------------------------

def _candidate(profile, current):
    """The config a load would produce: the profile's values over the live ones.

    Two rules:

    * **The profile's masked secrets take the live value.** A profile holds `***` where a key
      was, so `merge_secrets` puts the current key back. Without it, loading a profile would
      blank every API key -- the one mistake that would make profiles worse than useless.
    * **A key the profile does not mention is left alone**, because a profile is a whole
      document but an older one may predate a key the live config has. The candidate is built
      from the live tree with the profile's leaves applied, not from the profile alone.
    """
    candidate = config_tree.ConfigTree(copy.deepcopy(dict(current)))
    merged = config_tree.merge_secrets(profile, current)
    for path, value in list(config_tree.leaves(merged)):
        if value == config_tree.REDACTED and config_tree.is_secret(path):
            # `merge_secrets` only restores a secret whose live value is **not None**, so a
            # secret the shipped config leaves null -- `valkey.password` is one -- would keep
            # the marker and a load would write `password: "***"` into config.yml. The marker
            # means "unchanged", so it resolves to the live value whatever that value is.
            value = config_tree.get(current, path, None)
        config_tree.set_value(candidate, path, value)
    return candidate


def _validate(candidate):
    """Every reason `candidate` cannot be the live config, as ``{path: message}``.

    Two layers, because the config has two kinds of rule:

    * `dataset_settings.validate_config` for the `datasets` subtree -- the same call
      `configure_queue_datasets` makes at run start, so a profile that would fail there fails
      here instead.
    * `config_schema.validate` for every leaf -- the ranges, enums, URLs, regexes and
      cross-field rules the editor runs before a save.

    Both collect rather than raise, so a profile with three problems reports three.
    """
    errors = {}
    datasets = config_tree.get(candidate, 'datasets', {})
    try:
        dataset_settings.validate_config(datasets)
    except dataset_settings.DatasetSettingsError as exc:
        errors['datasets'] = str(exc)
    except Exception as exc:  # noqa: BLE001 - a malformed subtree can raise anything
        errors['datasets'] = str(exc) or type(exc).__name__

    values = {path: value for path, value in config_tree.leaves(candidate)}
    try:
        errors.update(config_schema.validate(candidate, values))
    except Exception as exc:  # noqa: BLE001 - a validator must not turn a 422 into a 500
        errors.setdefault('config', str(exc) or type(exc).__name__)
    return errors


def _first(errors):
    """The first message, for the one-line summary an error response carries."""
    for path in sorted(errors):
        return f'{path}: {errors[path]}'
    return 'unknown error'


def _diff(before, after):
    """The leaves that differ, as ``{path: {from, to}}``.

    Computed from the two trees rather than from the text, so it describes the change a user
    cares about (this value becomes that one) rather than the lines a splice moved.
    """
    old = dict(config_tree.leaves(before))
    new = dict(config_tree.leaves(after))
    changed = {}
    for path in sorted(set(old) | set(new)):
        was = old.get(path, _MISSING)
        now = new.get(path, _MISSING)
        if was is _MISSING and now is _MISSING:
            continue
        if was is not _MISSING and now is not _MISSING and was == now and type(was) is type(now):
            continue
        secret = config_tree.is_secret(path)
        if secret:
            # A secret is never rendered, only named. The preview says a key would change and
            # says nothing about either value -- a diff is still a payload the browser receives.
            changed[path] = {'from': '***' if was is not _MISSING else None,
                             'to': '***' if now is not _MISSING else None,
                             'secret': True}
            continue
        changed[path] = {
            'from': None if was is _MISSING else _render(was),
            'to': None if now is _MISSING else _render(now),
            'secret': False,
        }
    return changed


def _mask(tree):
    """A copy of `tree` with every secret that **has a value** replaced by the marker.

    `config_tree.redact` masks every secret leaf unconditionally, which is right for the
    Config editor -- it needs a field for each one and `has_value` to say whether it is set.
    It is wrong for a profile, because a profile is a **file the user reads**: masking an
    unset key writes `key: '***'` into it, so a config with no API keys produces a profile
    that appears to have nine. Masking only a secret that has a value keeps the file honest
    -- an unset key stays empty, and the marker means exactly "a key is here, and it is not
    in this file".
    """
    masked = copy.deepcopy(dict(tree))
    for path, value in list(config_tree.leaves(masked)):
        if not config_tree.is_secret(path):
            continue
        if value is None or value == '':
            continue
        config_tree.set_value(masked, path, config_tree.REDACTED)
    return masked


def _first_line(message):
    """The first line of a multi-line error, trimmed to something a status line can hold."""
    line = str(message).strip().splitlines()[0] if str(message).strip() else 'unreadable'
    return line if len(line) <= 160 else line[:157] + '…'


def _render(value):
    """A leaf as the preview shows it."""
    if value is None:
        return None
    if isinstance(value, bool):
        return 'true' if value else 'false'
    text = str(value)
    return text if len(text) <= 200 else text[:197] + '…'


# --- the filesystem ------------------------------------------------------

_MISSING = object()


def _profiles_dir():
    """The profiles directory beside the live config, matching `ConfigLoader.profiles_dir`."""
    return os.path.join(os.path.dirname(os.path.abspath(_config_path())), 'profiles')


def _config_path():
    """The config file the engine is actually running from."""
    return app_state.global_config.path


def _write_atomic(path, text):
    """Write `text` to `path` through a temporary neighbour.

    An interrupted save must not leave a half-written profile behind -- the same rule
    `config_tree.save` follows for the config itself.
    """
    directory = os.path.dirname(os.path.abspath(path)) or '.'
    temporary = None
    try:
        import tempfile

        descriptor, temporary = tempfile.mkstemp(dir=directory, prefix='.profile-',
                                                suffix='.tmp')
        with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
            handle.write(text)
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary and os.path.exists(temporary):
            try:
                os.remove(temporary)
            except OSError:
                pass


def _reload_engine_config():
    """Re-read the config into the running engine, so a load applies without a restart."""
    try:
        app_state.global_config.load()
    except Exception:  # noqa: BLE001 - a reload must not turn a good load into an error
        return False
    return True


async def _json_body(request: Request):
    """The request body as a dict, or ``None`` when it is not one.

    The parameter must be annotated ``Request``: an unannotated one is read by FastAPI as a
    **query** field, so every request would be rejected with a 422 before the body was looked
    at. That is the trap Phase 6b found on the first live save.
    """
    import json

    try:
        body = await request.json()
    except (ValueError, json.JSONDecodeError):
        return None
    return body if isinstance(body, dict) else None
