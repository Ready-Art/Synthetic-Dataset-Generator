"""Check actual worker-launch argument wiring without importing the application.

Run with ``python -B -m unittest test_worker_launch``. Source expressions remain
AST nodes: no configuration, GUI, worker, network, or model code is evaluated.
"""

import ast
import inspect
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def _read_function(filename, name):
    tree = ast.parse((ROOT / filename).read_text(encoding="utf-8"), filename=filename)
    matches = [node for node in tree.body
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
        signature = _signature(_read_function("generation.py", "worker"))
        start = _read_function("generate.py", "start_processing")
        launches = []
        for node in ast.walk(start):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "threading"
                    and node.func.attr == "Thread"):
                continue
            fields = {keyword.arg: keyword.value for keyword in node.keywords}
            target = fields.get("target")
            if isinstance(target, ast.Name) and target.id == "worker":
                launches.append((node.lineno, fields))

        self.assertTrue(launches, "No threading.Thread(target=worker) launch found")
        for line, fields in launches:
            with self.subTest(line=line):
                positional = fields.get("args")
                keywords = fields.get("kwargs")
                self.assertIsInstance(positional, (ast.Tuple, ast.List))
                self.assertIsInstance(keywords, ast.Dict)
                self.assertFalse(any(isinstance(value, ast.Starred) for value in positional.elts),
                                 "Expanded worker args need explicit inspection")
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

                for name in ("current_ooc_phrases", "current_ooc_fixes", "current_ooc_enabled"):
                    value = bound.arguments.get(name)
                    self.assertIsInstance(value, ast.Name, "{} was not supplied".format(name))
                    self.assertEqual(value.id, name, "Incorrect source for {}".format(name))


if __name__ == "__main__":
    unittest.main()
