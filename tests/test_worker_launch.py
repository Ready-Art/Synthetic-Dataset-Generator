# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Check actual worker-launch argument wiring without importing the application.

Run with ``python -B tests/test_worker_launch.py``. Source expressions remain
AST nodes: no configuration, worker, network, or model code is evaluated.

The launch lives in ``control.HeadlessController.cmd_start``, so both checks parse
``control.py``.
"""

import ast
import inspect
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def _read_function(filename, name):
    tree = ast.parse((ROOT / filename).read_text(encoding="utf-8"), filename=filename)
    # ``ast.walk``, not ``tree.body``: the launch is a method of
    # ``HeadlessController`` now, so it is not a top-level statement.
    matches = [node for node in ast.walk(tree)
               if isinstance(node, ast.FunctionDef) and node.name == name]
    if len(matches) != 1:
        raise AssertionError("Expected exactly one {} in {}".format(name, filename))
    return matches[0]


def _signature(function):
    """Preserve Python argument kinds and required/default status from the AST."""
    args = function.args
    positional = args.posonlyargs + args.args
    first_default = len(positional) - len(args.defaults)
    parameters = []
    for index, argument in enumerate(positional):
        kind = (inspect.Parameter.POSITIONAL_ONLY if index < len(args.posonlyargs)
                else inspect.Parameter.POSITIONAL_OR_KEYWORD)
        default = (args.defaults[index - first_default] if index >= first_default
                   else inspect.Parameter.empty)
        parameters.append(inspect.Parameter(argument.arg, kind, default=default))
    if args.vararg is not None:
        parameters.append(inspect.Parameter(args.vararg.arg, inspect.Parameter.VAR_POSITIONAL))
    for argument, default in zip(args.kwonlyargs, args.kw_defaults):
        parameters.append(inspect.Parameter(
            argument.arg, inspect.Parameter.KEYWORD_ONLY,
            default=inspect.Parameter.empty if default is None else default))
    if args.kwarg is not None:
        parameters.append(inspect.Parameter(args.kwarg.arg, inspect.Parameter.VAR_KEYWORD))
    return inspect.Signature(parameters)


class WorkerLaunchTests(unittest.TestCase):
    def test_start_processing_binds_worker_arguments(self):
        """``_worker_args`` + ``_worker_kwargs`` must bind ``generation.worker``.

        The launch builds the two lists in two methods on ``HeadlessController``, so
        this binds their returned literals against the real signature instead of the
        launch call site.
        """
        signature = _signature(_read_function("generation.py", "worker"))

        args_function = _read_function("control.py", "_worker_args")
        positional = next(node for node in ast.walk(args_function)
                         if isinstance(node, ast.Return)).value
        self.assertIsInstance(positional, (ast.Tuple, ast.List))
        self.assertFalse(any(isinstance(value, ast.Starred) for value in positional.elts),
                         "Expanded worker args need explicit inspection")

        kwargs_function = _read_function("control.py", "_worker_kwargs")
        keywords = next(node for node in ast.walk(kwargs_function)
                        if isinstance(node, ast.Return)).value
        self.assertIsInstance(keywords, ast.Dict)

        keyword_values = {}
        for key, value in zip(keywords.keys, keywords.values):
            self.assertIsInstance(key, ast.Constant,
                                   "Expanded worker kwargs need explicit inspection")
            self.assertIsInstance(key.value, str)
            keyword_values[key.value] = value

        try:
            bound = signature.bind(*positional.elts, **keyword_values)
        except TypeError as exc:
            self.fail("Worker launch does not match worker signature: {}".format(exc))

        # The kwarg name carries the ``_local`` suffix; the settings attribute does not.
        for name in ("current_ooc_phrases", "current_ooc_fixes", "current_ooc_enabled"):
            value = bound.arguments.get(name)
            self.assertIsNotNone(value, "{} was not supplied".format(name))
            self.assertEqual(getattr(value, "attr", None), name.removeprefix("current_"),
                             "Incorrect source for {}".format(name))

    def test_reported_thread_count_is_the_launched_thread_count(self):
        """The "Started N worker threads" line must report what is actually spawned.

        Regression: it printed the legacy ``api.threads`` scalar, so a run with 30 threads
        split over the API slots announced "12 threads" next to the output format -- a
        number that matched neither the front end nor the threads that started. Both the log
        line and the launch loop must read ONE resolved total, and that total must come
        from ``run_config.total_threads_for_run``.
        """
        start = _read_function("control.py", "cmd_start")

        # 1. The total is resolved through the shared helper, not from the scalar.
        resolves = [
            node for node in ast.walk(start)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "total_threads_for_run"
        ]
        self.assertTrue(
            resolves,
            "cmd_start does not call run_config.total_threads_for_run(); the thread total "
            "must come from the one shared definition")
        total_line = min(node.lineno for node in resolves)

        # 2. The launch loop counts to that same variable.
        launch_ranges = []
        for node in ast.walk(start):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                    and node.func.id == "range" and len(node.args) == 1:
                argument = node.args[0]
                if isinstance(argument, ast.Name) and argument.id == "total_threads":
                    launch_ranges.append(node.lineno)
        self.assertTrue(launch_ranges, "No `for i in range(total_threads)` launch loop found")

        # 3. The log line the user reads names that same total, and is logged AFTER it is
        #    resolved -- reading the scalar is what produced the wrong number.
        reported = []
        for node in ast.walk(start):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "log_message"):
                continue
            if not node.args:
                continue
            template = node.args[0]
            if not (isinstance(template, ast.JoinedStr) and template.values
                    and isinstance(template.values[0], ast.Constant)):
                continue
            if not str(template.values[0].value).startswith("Started "):
                continue
            # Collect attribute names too: the buggy version read
            # `app_state.num_threads`, whose tail is an Attribute attr, not a Name.
            names = {value.id for value in ast.walk(template)
                     if isinstance(value, ast.Name)}
            names |= {value.attr for value in ast.walk(template)
                      if isinstance(value, ast.Attribute)}
            reported.append((node.lineno, names))

        self.assertTrue(reported, "The 'Started N worker threads' log line is gone")
        for line, names in reported:
            with self.subTest(line=line):
                self.assertIn("total_threads", names,
                              "the worker-count log line does not report the resolved total")
                self.assertNotIn("num_threads", names,
                                 "the worker-count log line still reports the legacy "
                                 "api.threads scalar instead of the run total")
                self.assertGreater(line, total_line,
                                   "the worker-count log line is logged before the total "
                                   "is resolved, so it would report a stale value")


if __name__ == "__main__":
    unittest.main()
