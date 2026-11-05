# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Catch a global name that is used but never bound (v12.9.1).

Run with ``python -B tests/test_undefined_globals.py``. Headless: no display, no
configuration, no network, no model and no running server. Nothing here imports the
application modules -- their source text is parsed with ``symtable`` instead, so this test
cannot build the Tk UI, read a user's ``custom.yml`` or touch ``output/``.

Why this file exists
--------------------

v12.9 wired the async judge writer into ``generate.start_processing()`` with

    generation._install_quality_write_handler()

but ``generate.py`` only ever did ``from generation import worker, ...``, which binds the
*names* it imports and NOT the module name ``generation``. The call therefore raised
``NameError: name 'generation' is not defined`` -- but only on a run with
``quality.async_scoring: true`` and LLM scoring on, so the default run, the default test
run and every import-time check all passed. It surfaced as a Tkinter callback traceback
after the first Start click of an async run.

No test caught it because the name is perfectly legal Python: it is a global lookup that
only fails when that branch actually executes. A whole-file symbol-table pass does catch it,
and catches the same mistake in any future branch of any module.

What this file pins:

1. every global name referenced by a module-level function in the checked modules is bound
   somewhere at module level (or is a builtin) -- an unbound one is a latent ``NameError``
   on whichever branch reaches it;
2. the async-judge wiring specifically: the call in ``start_processing()`` resolves to a
   module-level name that is bound to a module object, and that object really has the
   attribute the call site uses;
3. the module attribute is a real, public-enough-to-call function in ``generation.py``.
"""

import ast
import builtins
import os
import symtable
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Names the interpreter provides, plus the module dunders a module body may reference
# without assigning them itself.
BUILTIN_NAMES = set(dir(builtins)) | {
    "__file__", "__name__", "__doc__", "__builtins__", "__package__",
    "__loader__", "__spec__", "__path__", "__annotations__", "__debug__",
    "WindowsError",
}

# The application modules checked by the generic sweep. `generate.py` is the one that
# regressed; the rest are here so the check is a class guard, not a single-file patch.
CHECKED_MODULES = (
    "generate.py",
    "generation.py",
    "quality.py",
    "api_handler.py",
    "api_profiles.py",
    "app_state.py",
    "config_loader.py",
    "config_editor.py",
    "dashboard.py",
    "detection.py",
    "logging_config.py",
    "style_metrics.py",
    "text_utils.py",
    "ui_layout.py",
    "dataset_bindings.py",
    "dataset_settings.py",
    "dataset_sources.py",
    "dataset_tasks.py",
    "dataset_templates.py",
    "datasets_panel.py",
    # v13.0: the headless / CLI control layer.
    "run_config.py",
    "control.py",
    "cli.py",
    # v13.1.0: the 📊 Metrics tab's vLLM /metrics reader.
    "vllm_metrics.py",
)


def _source(filename):
    with open(os.path.join(ROOT, filename), "r", encoding="utf-8") as handle:
        return handle.read()


def _module_level_names(table):
    """Every name the module body binds: assignments, imports, defs, classes, params."""
    bound = set(BUILTIN_NAMES)
    for symbol in table.get_symbols():
        if (symbol.is_assigned() or symbol.is_imported() or symbol.is_parameter()
                or symbol.is_namespace()):
            bound.add(symbol.get_name())
    return bound


def _global_declarations(table):
    """Names a nested scope binds after a ``global`` statement (e.g. a counter)."""
    declared = set()

    def walk(current):
        for symbol in current.get_symbols():
            if symbol.is_declared_global() and (symbol.is_assigned() or symbol.is_imported()):
                declared.add(symbol.get_name())
        for child in current.get_children():
            walk(child)

    walk(table)
    return declared


def unbound_globals(filename):
    """Map each referenced-but-never-bound global name to the functions that use it."""
    table = symtable.symtable(_source(filename), filename, "exec")
    bound = _module_level_names(table) | _global_declarations(table)

    missing = {}

    def walk(current):
        for symbol in current.get_symbols():
            if symbol.is_global() and symbol.is_referenced():
                if symbol.get_name() not in bound:
                    missing.setdefault(symbol.get_name(), set()).add(current.get_name())
        for child in current.get_children():
            walk(child)

    walk(table)
    return missing


def test_no_module_references_an_unbound_global():
    """The generic guard: an unbound global is a NameError waiting for its branch."""
    problems = []
    for filename in CHECKED_MODULES:
        for name, functions in sorted(unbound_globals(filename).items()):
            problems.append("{}: '{}' is used in {}() but never bound at module level"
                            .format(filename, name, ", ".join(sorted(functions))))
    assert not problems, (
        "Unbound global name(s) found -- each is a NameError on the branch that reaches "
        "it, and neither an import nor the default run executes that branch:\n  "
        + "\n  ".join(problems))


def _function_node(filename, name):
    tree = ast.parse(_source(filename), filename=filename)
    matches = [node for node in tree.body
               if isinstance(node, ast.FunctionDef) and node.name == name]
    assert len(matches) == 1, "expected exactly one {}() in {}".format(name, filename)
    return matches[0]


def _module_bindings(filename):
    """Name -> AST node of the module-level statement that binds it (imports only)."""
    tree = ast.parse(_source(filename), filename=filename)
    bindings = {}
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    bindings[alias.asname] = alias.name
                else:
                    bindings[alias.name.split(".")[0]] = alias.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                bindings[alias.asname or alias.name] = "{}.{}".format(node.module, alias.name)
    return bindings


def test_async_judge_wiring_calls_a_bound_module_attribute():
    """v12.9.1: the async writer install in start_processing() must resolve."""
    start = _function_node("generate.py", "start_processing")

    # The call site: <module>._install_quality_write_handler()
    calls = [node for node in ast.walk(start)
             if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute)
             and node.func.attr == "_install_quality_write_handler"]
    assert calls, (
        "generate.start_processing() no longer installs the async-judge writer; if the "
        "wiring moved, move this check with it")

    bindings = _module_bindings("generate.py")
    for call in calls:
        owner = call.func.value
        assert isinstance(owner, ast.Name), (
            "line {}: the async writer install is not called on a plain module name"
            .format(call.lineno))
        assert owner.id in bindings, (
            "line {}: '{}._install_quality_write_handler()' is called, but generate.py "
            "never binds the name '{}' at module level -- `from {} import x` binds x, not "
            "the module, so this raises NameError on any run with async scoring on"
            .format(call.lineno, owner.id, owner.id, owner.id))
        target = bindings[owner.id]
        assert target.endswith("generation") or target == "generation", (
            "line {}: '{}' is bound to {!r}, which is not the generation module"
            .format(call.lineno, owner.id, target))


def test_the_installed_handler_exists_in_generation():
    """The attribute the call site uses must really exist in generation.py."""
    tree = ast.parse(_source("generation.py"), filename="generation.py")
    defined = {node.name for node in tree.body
               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    defined.update(
        target.id
        for node in tree.body if isinstance(node, ast.Assign)
        for target in node.targets if isinstance(target, ast.Name)
    )
    assert "_install_quality_write_handler" in defined, (
        "generation._install_quality_write_handler() is called by generate.py but is not "
        "defined at module level in generation.py")


def test_generate_imports_the_generation_module_object():
    """Pin the fix itself: the module object, not only names from it."""
    bindings = _module_bindings("generate.py")
    assert "generation" in bindings, (
        "generate.py must bind the name 'generation' (import generation) for the "
        "async-judge wiring in start_processing(); `from generation import x` alone "
        "does not create it")
    assert bindings["generation"] == "generation", (
        "the name 'generation' in generate.py must be the module itself, not a re-export")


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
