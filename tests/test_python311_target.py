# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Pin the Python 3.11 runtime target: python -B tests/test_python311_target.py.

The project used to claim Python 3.9 but is retargeted to 3.11. The dataset modules
(`dataset_sources`, `dataset_templates`, `dataset_bindings`, `dataset_settings`) now use
PEP 604 ``X | None`` annotations, which are 3.10+ syntax, and they import their mapping
helpers from ``collections.abc`` rather than the deprecated ``typing`` aliases. On an older
interpreter those modules fail with a confusing SyntaxError or AttributeError deep inside
the app, so `cli.py` carries a hard, early version guard.

These checks pin three things that are easy to lose in a refactor:

1. the running interpreter really is 3.11+;
2. the four dataset modules still import cleanly and their source text stays modern (no
   ``Optional[`` / ``Tuple[`` / ``Dict[`` / ``List[`` and no ``typing.`` reference);
3. the guard in `cli.py` runs BEFORE the 3.11-only modules are imported.

`cli.py` is never imported here: it imports `control`, which imports `generation`.
Its source text is read from disk instead, so this file is headless — no display, no
network, no model and no running server.
"""

# Repo root (app modules) onto sys.path so the suite runs from the repository root.
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))

import collections.abc
import inspect
import os
import sys

import dataset_bindings
import dataset_settings
import dataset_sources
import dataset_templates

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GENERATE_PATH = os.path.join(ROOT, "cli.py")

# The modules whose annotations must stay on the 3.11 target.
DATASET_MODULES = (dataset_sources, dataset_templates, dataset_bindings, dataset_settings)

# Legacy typing spellings that must not come back: they are what the retarget removed.
LEGACY_ANNOTATIONS = ("Optional[", "Tuple[", "Dict[", "List[", "typing.")

# The first imports that need 3.11-only syntax; the guard must precede all of them.
# Each family lists the spellings that count, so ``from dataset_x import y`` and
# ``import dataset_x`` are both recognised.
GUARDED_IMPORT_FAMILIES = (
    ("control", ("import control", "from control import")),
    ("run_config", ("import run_config", "from run_config import")),
)


def _module_source(module):
    """Return a module's source text, derived from its own __file__ (no hardcoded paths)."""
    path = getattr(module, "__file__", None)
    if path is None:
        raise AssertionError("{} has no __file__ to inspect".format(module.__name__))
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def _generate_source():
    with open(GENERATE_PATH, "r", encoding="utf-8") as handle:
        return handle.read()


def test_running_interpreter_meets_the_311_target():
    assert sys.version_info >= (3, 11), (
        "This project targets Python 3.11+, but the running interpreter is {}"
        .format(sys.version.replace("\n", " "))
    )


def test_dataset_modules_import_cleanly():
    for module in DATASET_MODULES:
        assert inspect.ismodule(module), "{} did not import as a module".format(module)
        assert module.__file__, "{} has no __file__".format(module.__name__)


def test_dataset_module_source_has_no_legacy_typing_spellings():
    for module in DATASET_MODULES:
        source = _module_source(module)
        for legacy in LEGACY_ANNOTATIONS:
            assert legacy not in source, (
                "{} still contains {!r}; the dataset modules must use PEP 604 "
                "``X | None`` annotations and collections.abc, not typing".format(
                    module.__name__, legacy)
            )


def test_collections_abc_isinstance_still_works():
    assert isinstance({}, collections.abc.Mapping) is True
    assert isinstance([], collections.abc.Mapping) is False
    assert isinstance("text", collections.abc.Iterable) is True


def test_dataset_settings_accepts_a_plain_dict():
    normalized = dataset_settings.normalize_config({})
    assert normalized == {"sources": []}, "normalize_config({}) returned {!r}".format(normalized)
    # A plain dict must also pass validation without raising.
    dataset_settings.validate_config({})
    dataset_settings.validate_config(None)


def test_dataset_bindings_mapping_helpers_accept_a_plain_dict():
    # cell_to_text() is the public mapping/cell helper; a plain dict must serialize.
    assert dataset_bindings.cell_to_text({"a": 1}) == '{"a":1}'
    assert dataset_bindings.cell_to_text("text") == "text"
    assert dataset_bindings.cell_to_text(None) == ""
    # The frozen dataclasses copy a plain dict of aliases through collections.abc.Mapping.
    binding = dataset_bindings.SourceBinding(None, {"column": "alias"})
    assert dict(binding.column_aliases) == {"column": "alias"}
    task = dataset_bindings.TaskBinding("run", "task", {}, ())
    assert dict(task.values) == {}


def test_generate_guard_compares_against_python_311():
    source = _generate_source()
    assert "sys.version_info" in source, "cli.py has no sys.version_info guard"
    assert "(3, 11)" in source, "cli.py does not compare against (3, 11)"
    assert "sys.version_info < (3, 11)" in source, (
        "the guard must be a hard ``sys.version_info < (3, 11)`` comparison"
    )
    assert "sys.exit(1)" in source, "the guard must exit non-zero via sys.exit(1)"
    assert "sys.stderr" in source, "the guard must report the failure on stderr"


def test_generate_guard_precedes_the_311_only_imports():
    source = _generate_source()
    guard_offset = source.find("sys.version_info < (3, 11)")
    assert guard_offset != -1, "cli.py has no ``sys.version_info < (3, 11)`` guard"

    for family, spellings in GUARDED_IMPORT_FAMILIES:
        offsets = [source.find(spelling) for spelling in spellings]
        offsets = [offset for offset in offsets if offset != -1]
        assert offsets, (
            "expected an import of {} in cli.py; the ordering check needs updating"
            .format(family)
        )
        import_offset = min(offsets)
        assert guard_offset < import_offset, (
            "The Python 3.11 guard must precede the dataset imports: the {} import "
            "appears at offset {} but the guard is at offset {}. Those modules use PEP 604 "
            "``X | None`` annotations, so an older interpreter must fail with the guard's "
            "clear message instead of a SyntaxError deep in the import.".format(
                family, import_offset, guard_offset)
        )


def main():
    tests = [value for name, value in sorted(globals().items())
             if name.startswith("test_") and callable(value)]
    failures = 0
    for test in tests:
        try:
            test()
        except AssertionError as exc:
            failures += 1
            print("FAIL {}: {}".format(test.__name__, exc))
        except Exception as exc:  # noqa: BLE001 - report, do not abort the run
            failures += 1
            print("ERROR {}: {!r}".format(test.__name__, exc))
        else:
            print("ok   {}".format(test.__name__))
    print("\n{} checks, {} failure(s)".format(len(tests), failures))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
