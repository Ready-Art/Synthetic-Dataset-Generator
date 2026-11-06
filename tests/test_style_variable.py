# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Headless tests for the v13.2.0 ``{style}`` variable and its run-wide deck.

The defect this feature removes is a REPETITION one: measured on the operator's dataset,
about 18% of saved rows shared their first three or four words with another row, because
every request carried the same instruction and so the model's most probable opening was the
same one every time. Naming a style per conversation breaks the tie -- but only if the styles
are dealt WITHOUT replacement, which is what these tests pin.

Covered here:

1. ``deal_style()`` walks the whole pool before repeating anything, and a run of N deals over
   an M-entry pool gives every entry the same share (the property independent
   ``random.choice`` draws do NOT have, and the reason the deck exists).
2. ``avoid`` is honoured, including the reshuffle-on-exhausted-tail path.
3. An empty pool yields ``""`` and never raises, so a style-free config still runs.
4. ``fill_style_variable()`` fills the token, resolves an empty style to nothing (never the
   literal ``{style}``), and leaves a template without the token byte-identical.
5. Every prompt formatter fills ``{style}``: ``fill_answer_prompt()``,
   ``generate_question()``, ``generate_user_continuation()``, ``fill_greeting_prompt()`` and
   ``generate_first_line()``.
6. The style pool key changes when the list is edited, so a stale deck is never dealt from a
   replaced list.
7. The shipped ``config/config.yml`` carries the block, ships it ON, names ``{style}`` in every
   prompt that receives it, and its list matches ``DEFAULT_STYLE_VARIANTS``.
8. ``run_config`` reads the block, defaults it ON, and falls back to the built-in list.
9. The worker deals ONE style per task and passes it to every prompt call site (parsed from
   ``generation.py``, so a new call site that forgets the kwarg fails here).

Run from the repository root: ``python -B tests/test_style_variable.py``
"""

import ast
import io
import os
import random
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

FAILURES = []


def check(label, condition):
    if condition:
        print(f"  ok   {label}")
    else:
        print(f"  FAIL {label}")
        FAILURES.append(label)


import generation as g  # noqa: E402


# --- 1. the deck ---------------------------------------------------------------

def test_deal_style_walks_the_whole_pool():
    """A pool must be exhausted before any entry repeats: that IS the feature."""
    pool = [f"style-{i}" for i in range(12)]
    # A fresh deck for a pool identity the state has not seen.
    import app_state
    app_state.style_deck = []
    app_state.styles_pool_key = None

    dealt = [g.deal_style(pool) for _ in range(12)]
    check("twelve deals over a twelve-entry pool are twelve distinct styles",
          len(set(dealt)) == 12)
    check("twelve deals over a twelve-entry pool use every entry",
          set(dealt) == set(pool))

    # The next pass repeats the pool, but as a NEW shuffled pass rather than a redraw.
    more = [g.deal_style(pool) for _ in range(12)]
    check("the second pass over the same pool is again every entry",
          set(more) == set(pool))


def test_deal_style_is_evenly_distributed():
    """Every entry must get the same share -- the property random.choice does NOT have.

    Over 1,000 rows a 12-entry pool drawn with replacement leaves some entries dealt four
    times more often than others, and those are the rows that still share an opening.
    """
    import app_state
    pool = [f"style-{i}" for i in range(12)]
    app_state.style_deck = []
    app_state.styles_pool_key = None

    counts = {}
    for _ in range(1200):
        style = g.deal_style(pool)
        counts[style] = counts.get(style, 0) + 1
    check("1200 deals over a 12-entry pool touch every entry", len(counts) == 12)
    check("1200 deals over a 12-entry pool give every entry exactly 100",
          set(counts.values()) == {100})


def test_deal_style_honours_avoid():
    import app_state
    pool = ["alpha", "beta", "gamma"]
    app_state.style_deck = []
    app_state.styles_pool_key = None

    deals = [g.deal_style(pool, avoid=("alpha",)) for _ in range(6)]
    check("an avoided style is never dealt while the pool has another", "alpha" not in deals)
    check("avoiding one entry still reaches the others", set(deals) == {"beta", "gamma"})


def test_deal_style_falls_back_when_every_entry_is_avoided():
    """A repeat is better than a blank style, so a fully-avoided pool still deals."""
    import app_state
    pool = ["only"]
    app_state.style_deck = []
    app_state.styles_pool_key = None
    check("a pool entirely inside avoid still deals an entry",
          g.deal_style(pool, avoid=("only",)) == "only")


def test_deal_style_handles_an_empty_pool():
    check("an empty pool deals \"\" and does not raise", g.deal_style([]) == "")
    check("a None pool deals \"\" and does not raise", g.deal_style(None) == "")
    check("a whitespace-only pool deals \"\"", g.deal_style(["   ", ""]) == "")


def test_the_pool_key_changes_with_the_list():
    """A replaced list must start a fresh deck, not deal from the old one."""
    first = ["a", "b"]
    second = ["a", "b", "c"]
    check("a different pool has a different key",
          g._styles_pool_key(first) != g._styles_pool_key(second))
    check("the same list has the same key",
          g._styles_pool_key(first) == g._styles_pool_key(first))


# --- 2. fill_style_variable ---------------------------------------------------

def test_fill_style_variable():
    check("the token is replaced",
          g.fill_style_variable("Begin: {style}.", "mid-action") == "Begin: mid-action.")
    check("an empty style resolves to nothing, never the literal token",
          g.fill_style_variable("Begin: {style}.", "") == "Begin: .")
    check("a template without the token is byte-identical",
          g.fill_style_variable("No token here.", "mid-action") == "No token here.")
    check("every occurrence is replaced",
          g.fill_style_variable("{style} then {style}", "x") == "x then x")


# --- 3. the prompt formatters --------------------------------------------------

def test_fill_answer_prompt_fills_style():
    filled = g.fill_answer_prompt("ANSWER {style} END", "", "")
    check("fill_answer_prompt fills {style}", filled == "ANSWER  END")
    filled = g.fill_answer_prompt("ANSWER {style} END", "", "", style="mid-action")
    check("fill_answer_prompt fills the given style",
          filled == "ANSWER mid-action END")
    check("{style} is in the answer prompt's native variable list",
          "{style}" in g.ANSWER_PROMPT_NATIVE_VARIABLES)
    check("a style entry naming another token is inserted literally, not re-substituted",
          g.fill_answer_prompt("{style}", "chunk", "ctx", style="{context}") == "{context}")


class _FakeResponse:
    status_code = 200
    text = ""

    def __init__(self, content):
        self._content = content

    def json(self):
        return {"choices": [{"message": {"content": self._content}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1}}


class _StubPost:
    """Capture the outgoing payload and answer with a fixed reply."""

    def __init__(self, content="A reply."):
        self.content = content
        self.calls = []

    def __call__(self, url, headers=None, data=None, timeout=None, **kwargs):
        self.calls.append({"url": url, "headers": headers, "data": data, **kwargs})
        return _FakeResponse(self.content)

    def messages(self):
        import json as _json
        raw = self.calls[-1]["data"]
        if isinstance(raw, str):
            return _json.loads(raw)["messages"]
        return raw["messages"]


def _with_stub(content="A reply."):
    """Context manager that stubs generation's own ``requests.post``."""
    import contextlib

    @contextlib.contextmanager
    def _cm():
        stub = _StubPost(content)
        original = g.requests.post
        g.requests.post = stub
        try:
            yield stub
        finally:
            g.requests.post = original

    return _cm()


def _capture_question_prompt(template="Q {style} {recent_questions}", style=""):
    """Run generate_question() against a stub API and return the request's user text."""
    import app_state
    app_state.question_history = []
    with _with_stub("A question.") as stub:
        g.generate_question(
            "SYS", template, "SUBJ", "CTX", 1, {}, "http://stub/v1/chat/completions",
            "model", "key", 5, "raw-subj", "raw-ctx", 0, 1, 30,
            style=style)
    return stub.messages()[1]["content"]


def test_generate_question_fills_style():
    text = _capture_question_prompt("STYLE-{style}-END {recent_questions}", style="mid-action")
    check("generate_question fills {style}", "STYLE-mid-action-END" in text)


def test_generate_question_without_style_leaves_no_literal_token():
    text = _capture_question_prompt("STYLE-{style}-END", style="")
    check("generate_question with no style leaves no literal {style}", "{style}" not in text)


def test_fill_greeting_prompt_fills_style():
    filled = g.fill_greeting_prompt("GREET {speaker} {style}", "Melody", style="mid-action")
    check("fill_greeting_prompt fills {style}", filled == "GREET Melody mid-action")
    filled = g.fill_greeting_prompt("GREET {speaker} {style}", "Melody", style="")
    check("fill_greeting_prompt with no style leaves no literal {style}", "{style}" not in filled)


def test_generate_first_line_prefers_the_dealt_style():
    """The conversation's style wins over the legacy per-call first_line_styles draw."""
    with _with_stub("A line.") as stub:
        g.generate_first_line(
            "GREETING", "FIRST {style}", 1, {}, "http://stub/v1/chat/completions",
            "model", "key", 0, 1, 30, styles=["legacy-style"], style="dealt-style")
    check("generate_first_line uses the conversation's dealt style",
          stub.messages()[0]["content"] == "FIRST dealt-style")


def test_generate_first_line_falls_back_to_styles_when_no_style_is_given():
    """A config without the style block must send exactly what it always did."""
    with _with_stub("A line.") as stub:
        g.generate_first_line(
            "GREETING", "FIRST {style}", 1, {}, "http://stub/v1/chat/completions",
            "model", "key", 0, 1, 30, styles=["legacy-style"])
    check("without a dealt style, generate_first_line draws from first_line_styles",
          stub.messages()[0]["content"] == "FIRST legacy-style")


def test_generate_user_continuation_fills_style():
    with _with_stub("A reply.") as stub:
        g.generate_user_continuation(
            "SYS", [{"role": "assistant", "content": "Hi."}],
            "CONT {style}", 1, {}, "http://stub/v1/chat/completions",
            "model", "key", 0, 1, 30, style="mid-action")
    check("generate_user_continuation fills {style}",
          stub.messages()[-1]["content"] == "CONT mid-action")


# --- 4. the shipped config ----------------------------------------------------

def test_shipped_config_carries_the_style_block():
    import yaml
    with open(os.path.join(REPO, "config", "config.yml"), encoding="utf-8") as handle:
        shipped = yaml.safe_load(handle)
    prompts = shipped["prompts"]
    check("the shipped config has a prompts.style block", "style" in prompts)
    style_block = prompts.get("style") or {}
    check("the shipped style block is ON", style_block.get("enabled") is True)
    check("the shipped style list matches DEFAULT_STYLE_VARIANTS",
          style_block.get("styles") == list(g.DEFAULT_STYLE_VARIANTS))
    for key in ("question", "answer", "user_continuation_prompt", "greeting", "first_line"):
        check(f"the shipped {key} prompt names {{style}}", "{style}" in prompts[key])
    # The style block must not have displaced the legacy first-line styles.
    check("the shipped config keeps its first_line_styles",
          prompts.get("first_line_styles") == list(g.DEFAULT_FIRST_LINE_STYLES))


def test_default_style_variants_are_beginning_instructions():
    """Entries describe HOW a message begins, not a mood -- the whole point."""
    check("there are at least 8 built-in style variants",
          len(g.DEFAULT_STYLE_VARIANTS) >= 8)
    check("every built-in style variant starts with 'open'",
          all(entry.lower().startswith("open") for entry in g.DEFAULT_STYLE_VARIANTS))
    check("the built-in style variants are distinct",
          len(set(g.DEFAULT_STYLE_VARIANTS)) == len(g.DEFAULT_STYLE_VARIANTS))


# --- 5. the plumbing ----------------------------------------------------------

def _worker_kwarg_names():
    """The kwargs start_processing passes to generation.worker(), parsed from the AST."""
    with open(os.path.join(REPO, "generate.py"), encoding="utf-8") as handle:
        tree = ast.parse(handle.read())

    def find(node, name):
        for child in ast.walk(node):
            if isinstance(child, ast.FunctionDef) and child.name == name:
                return child
        return None

    start = find(tree, "start_processing")
    passed = set()
    for call in ast.walk(start):
        if not isinstance(call, ast.Call):
            continue
        # threading.Thread(...) is an Attribute call; a bare Thread(...) is a Name call.
        func = call.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if name != "Thread":
            continue
        for kw in call.keywords:
            if kw.arg == "kwargs" and isinstance(kw.value, ast.Dict):
                passed |= {k.value for k in kw.value.keys if isinstance(k, ast.Constant)}
    return passed


def test_the_worker_is_launched_with_the_style_kwargs():
    passed = _worker_kwarg_names()
    check("start_processing passes style_enabled_local to the worker",
          "style_enabled_local" in passed)
    check("start_processing passes style_list_local to the worker",
          "style_list_local" in passed)


def test_the_worker_signature_accepts_the_style_kwargs():
    import inspect
    params = inspect.signature(g.worker).parameters
    check("worker() accepts style_enabled_local", "style_enabled_local" in params)
    check("worker() accepts style_list_local", "style_list_local" in params)
    check("style_enabled_local defaults to off (a bare worker call stays style-free)",
          params["style_enabled_local"].default is False)


def test_the_worker_deals_one_style_per_task():
    """One style per CONVERSATION: a per-turn deal would change voice mid-scene."""
    with open(os.path.join(REPO, "generation.py"), encoding="utf-8") as handle:
        tree = ast.parse(handle.read())

    def find(node, name):
        for child in ast.walk(node):
            if isinstance(child, ast.FunctionDef) and child.name == name:
                return child
        return None

    worker = find(tree, "worker")
    deals = [n for n in ast.walk(worker)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
             and n.func.id == "deal_style"]
    check("worker() calls deal_style exactly once (once per task)", len(deals) == 1)
    check("the deal happens before the turn loop",
          deals and deals[0].lineno < max(
              n.lineno for n in ast.walk(worker)
              if isinstance(n, ast.For)))


def test_every_prompt_call_site_receives_the_style():
    """The five prompt calls must all be handed the task's style."""
    with open(os.path.join(REPO, "generation.py"), encoding="utf-8") as handle:
        tree = ast.parse(handle.read())

    def find(node, name):
        for child in ast.walk(node):
            if isinstance(child, ast.FunctionDef) and child.name == name:
                return child
        return None

    worker = find(tree, "worker")
    wanted = {
        "generate_greeting": "style",
        "generate_first_line": "style",
        "generate_question": "style",
        "generate_user_continuation": "style",
        "fill_answer_prompt": "style",
    }
    for name, kwarg in wanted.items():
        calls = [n for n in ast.walk(worker)
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                 and n.func.id == name]
        check(f"worker() calls {name}()", len(calls) >= 1)
        for call in calls:
            passed = {k.arg for k in call.keywords}
            check(f"worker()'s {name}() call at line {call.lineno} passes style=",
                  kwarg in passed)


# --- 6. run_config ------------------------------------------------------------

def test_run_config_defaults_and_reads_the_style_block():
    """A config with no style block must default ON with the built-in list."""
    import tempfile
    import yaml

    original_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmp:
        os.makedirs(os.path.join(tmp, "config"), exist_ok=True)
        with open(os.path.join(REPO, "config", "config.yml"), encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
        data["prompts"].pop("style", None)
        with open(os.path.join(tmp, "config", "config.yml"), "w", encoding="utf-8") as handle:
            yaml.safe_dump(data, handle, sort_keys=False)
        os.chdir(tmp)
        try:
            import app_state
            app_state.global_config.load()
            import importlib
            import config_loader
            import run_config
            importlib.reload(config_loader)
            importlib.reload(run_config)
            settings = run_config.build_run_settings()
            check("a config with no style block defaults the feature ON",
                  settings.style_enabled is True)
            check("a config with no style block uses the built-in list",
                  settings.style_list == list(g.DEFAULT_STYLE_VARIANTS))
        finally:
            os.chdir(original_cwd)
            import app_state
            app_state.global_config.load()
            import importlib
            import config_loader
            import run_config
            importlib.reload(config_loader)
            importlib.reload(run_config)


def test_run_config_reads_an_explicit_style_block():
    import tempfile
    import yaml

    original_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmp:
        os.makedirs(os.path.join(tmp, "config"), exist_ok=True)
        with open(os.path.join(REPO, "config", "config.yml"), encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
        data["prompts"]["style"] = {"enabled": False, "styles": ["one", "two"]}
        with open(os.path.join(tmp, "config", "config.yml"), "w", encoding="utf-8") as handle:
            yaml.safe_dump(data, handle, sort_keys=False)
        os.chdir(tmp)
        try:
            import app_state
            app_state.global_config.load()
            import importlib
            import config_loader
            import run_config
            importlib.reload(config_loader)
            importlib.reload(run_config)
            settings = run_config.build_run_settings()
            check("run_config reads prompts.style.enabled: false",
                  settings.style_enabled is False)
            check("run_config reads the configured style list",
                  settings.style_list == ["one", "two"])
        finally:
            os.chdir(original_cwd)
            import app_state
            app_state.global_config.load()
            import importlib
            import config_loader
            import run_config
            importlib.reload(config_loader)
            importlib.reload(run_config)


def test_an_empty_style_list_falls_back_to_the_builtins():
    import tempfile
    import yaml

    original_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmp:
        os.makedirs(os.path.join(tmp, "config"), exist_ok=True)
        with open(os.path.join(REPO, "config", "config.yml"), encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
        data["prompts"]["style"] = {"enabled": True, "styles": []}
        with open(os.path.join(tmp, "config", "config.yml"), "w", encoding="utf-8") as handle:
            yaml.safe_dump(data, handle, sort_keys=False)
        os.chdir(tmp)
        try:
            import app_state
            app_state.global_config.load()
            import importlib
            import config_loader
            import run_config
            importlib.reload(config_loader)
            importlib.reload(run_config)
            settings = run_config.build_run_settings()
            check("an empty style list falls back to the built-in list",
                  settings.style_list == list(g.DEFAULT_STYLE_VARIANTS))
        finally:
            os.chdir(original_cwd)
            import app_state
            app_state.global_config.load()
            import importlib
            import config_loader
            import run_config
            importlib.reload(config_loader)
            importlib.reload(run_config)


# --- 7. app_state -------------------------------------------------------------

def test_app_state_has_the_deck_and_resets_it():
    import app_state
    check("app_state has a style deck", hasattr(app_state, "style_deck"))
    check("app_state has a style deck lock", hasattr(app_state, "style_deck_lock"))
    check("app_state has a style pool key", hasattr(app_state, "styles_pool_key"))

    app_state.style_deck = ["x"]
    app_state.styles_pool_key = ("k",)
    # The reset is a line in generate.reset_all_stats_and_history(); assert the attribute
    # names it clears so a rename cannot silently leave the deck populated across runs.
    with open(os.path.join(REPO, "generate.py"), encoding="utf-8") as handle:
        source = handle.read()
    check("reset_all_stats_and_history clears the style deck",
          "app_state.style_deck = []" in source
          and "app_state.styles_pool_key = None" in source)


def main():
    sections = [
        ("(1) deal_style: dealt without replacement", [
            test_deal_style_walks_the_whole_pool,
            test_deal_style_is_evenly_distributed,
            test_deal_style_honours_avoid,
            test_deal_style_falls_back_when_every_entry_is_avoided,
            test_deal_style_handles_an_empty_pool,
            test_the_pool_key_changes_with_the_list,
        ]),
        ("(2) fill_style_variable", [test_fill_style_variable]),
        ("(3) every prompt formatter fills {style}", [
            test_fill_answer_prompt_fills_style,
            test_generate_question_fills_style,
            test_generate_question_without_style_leaves_no_literal_token,
            test_fill_greeting_prompt_fills_style,
            test_generate_first_line_prefers_the_dealt_style,
            test_generate_first_line_falls_back_to_styles_when_no_style_is_given,
            test_generate_user_continuation_fills_style,
        ]),
        ("(4) the shipped config", [
            test_shipped_config_carries_the_style_block,
            test_default_style_variants_are_beginning_instructions,
        ]),
        ("(5) the plumbing", [
            test_the_worker_is_launched_with_the_style_kwargs,
            test_the_worker_signature_accepts_the_style_kwargs,
            test_the_worker_deals_one_style_per_task,
            test_every_prompt_call_site_receives_the_style,
        ]),
        ("(6) run_config", [
            test_run_config_defaults_and_reads_the_style_block,
            test_run_config_reads_an_explicit_style_block,
            test_an_empty_style_list_falls_back_to_the_builtins,
        ]),
        ("(7) app_state", [test_app_state_has_the_deck_and_resets_it]),
    ]

    for title, tests in sections:
        print(title)
        for test in tests:
            test()

    print()
    if FAILURES:
        print(f"{len(FAILURES)} style-variable test(s) failed")
        return 1
    print("All style-variable tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
