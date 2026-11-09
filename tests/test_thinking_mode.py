# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""v13.0.4: the Samplers-tab Thinking Mode must reach the fixers and the judge.

Before this release only the four main generation request builders in ``generation.py``
read ``samplers.enable_thinking``. The Slop Fixer, the Anti-Slop Fixer and the Quality
Judge never sent ``chat_template_kwargs`` at all, so a reasoning model thought on every
rewrite and score request no matter what the GUI said -- on a local vLLM that dominated
their runtime (the user reported roughly a 5x generation speedup with thinking off).

This test pins three things:

1. ``api_handler.resolve_thinking_mode`` / ``apply_thinking_mode``: the resolution rules
   (own setting first, ``inherit`` follows the main setting, ``default`` sends nothing,
   a legacy boolean still works, an unknown value falls back safely).
2. The payloads the three auxiliary request builders actually send. The judge is exercised
   through a stubbed ``requests.post``; the two fixers are called directly with stubbed
   ``requests.post`` and ``app_state`` flags.
3. A default config still sends NOTHING (byte-identical to v13.0.3): the shipped
   ``enable_thinking`` values must resolve to ``default``.

Run headless: ``python -B tests/test_thinking_mode.py``. No model, network or display.
"""

import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.mkdtemp(prefix="thinking_mode_")
os.chdir(_TMP)
os.makedirs("config", exist_ok=True)
with open("config/config.yml", "w", encoding="utf-8") as f:
    f.write("api:\n  apis: []\n")

import api_handler                                    # noqa: E402
import app_state                                      # noqa: E402
import quality                                        # noqa: E402
from app_state import global_config                   # noqa: E402

FAILURES = []


def check(label, ok):
    print(("  ok   " if ok else "  FAIL ") + label)
    if not ok:
        FAILURES.append(label)


# ---------------------------------------------------------------------------
# 1. Resolution rules (pure, no requests involved)
# ---------------------------------------------------------------------------

def test_resolution_rules():
    print("\n-- resolve_thinking_mode / apply_thinking_mode --")
    resolve = api_handler.resolve_thinking_mode

    check("inherit follows a 'disable' main setting", resolve("inherit",
                                                             {"enable_thinking": "disable"}) == "disable")
    check("absent own setting follows the main setting", resolve(None, {"enable_thinking": "disable"}) == "disable")
    check("absent everywhere resolves to 'default'", resolve(None, {}) == "default")
    check("own 'enable' beats a 'disable' main setting",
          resolve("enable", {"enable_thinking": "disable"}) == "enable")
    check("own 'disable' beats an 'enable' main setting",
          resolve("disable", {"enable_thinking": "enable"}) == "disable")
    check("own 'default' beats a 'disable' main setting",
          resolve("default", {"enable_thinking": "disable"}) == "default")
    check("main 'inherit' resolves to 'default'", resolve(None, {"enable_thinking": "inherit"}) == "default")
    check("legacy boolean True maps to 'disable'", resolve(True, {}) == "disable")
    check("legacy boolean False maps to 'default'", resolve(False, {}) == "default")
    check("an unknown value falls back to the main setting",
          resolve("BOGUS", {"enable_thinking": "disable"}) == "disable")
    check("mode names are case/space insensitive", resolve("  DISABLE ", {}) == "disable")

    check("'default' fragment is empty", api_handler.thinking_payload_fragment("default") == {})
    check("'disable' fragment is a false enable_thinking",
          api_handler.thinking_payload_fragment("disable")
          == {"chat_template_kwargs": {"enable_thinking": False}})
    check("'enable' fragment is a true enable_thinking",
          api_handler.thinking_payload_fragment("enable")
          == {"chat_template_kwargs": {"enable_thinking": True}})

    payload = {"model": "m"}
    api_handler.apply_thinking_mode(payload, "disable", {})
    check("apply_thinking_mode writes the fragment", payload.get("chat_template_kwargs")
          == {"enable_thinking": False})

    payload = {"model": "m", "chat_template_kwargs": {"enable_thinking": True}}
    api_handler.apply_thinking_mode(payload, "disable", {})
    check("apply_thinking_mode does not overwrite existing kwargs",
          payload["chat_template_kwargs"] == {"enable_thinking": True})

    payload = {"model": "m"}
    api_handler.apply_thinking_mode(payload, None, {})
    check("apply_thinking_mode with no setting sends nothing", "chat_template_kwargs" not in payload)


# ---------------------------------------------------------------------------
# 2. The judge request
# ---------------------------------------------------------------------------

class _StubResponse:
    def __init__(self, payload):
        self.status_code = 200
        self._payload = payload

    def json(self):
        return self._payload


def _capture_post(store, reply):
    def fake_post(url, headers=None, data=None, timeout=None, **kwargs):
        store.append(json.loads(data))
        return _StubResponse(reply)
    return fake_post


def _capture_post_text(store, reply_text="A rewritten passage."):
    """A separate capture helper for the fixers, which return plain text, not a JSON verdict.

    Deliberately NOT the same function as ``_capture_post``: an earlier draft reused the
    name and the second definition shadowed the first, so the judge's stub started returning
    a bare string and the judge failed to parse it.
    """
    def fake_post(url, headers=None, data=None, timeout=None, **kwargs):
        store.append(json.loads(data))
        return _StubResponse({"choices": [{"finish_reason": "stop",
                                          "message": {"content": reply_text}}]})
    return fake_post


JUDGE_REPLY = {"choices": [{"finish_reason": "stop", "message": {"content":
                json.dumps({"coherence": 80, "naturalness": 80, "engagement": 80,
                            "diversity": 80, "consistency": 80, "logic": 90})}}]}


def _judge_config(judge_thinking=None):
    scoring_api = {"url": "http://judge.invalid/v1/chat/completions",
                   "model": "judge-model", "key": "", "slot_idx": 6}
    if judge_thinking is not None:
        # v13.0.4: a SIBLING key of sampler_settings, not an entry in it.
        scoring_api["enable_thinking"] = judge_thinking
    return {
        "quality": {
            "use_llm_scoring": True, "judge_retry_delay": 0, "judge_max_attempts": 1,
            "judge_json_repair_attempts": 0, "judge_structured_output": "off",
            "judge_priority_enabled": False, "scoring_api": scoring_api,
        },
        "samplers": {},
    }


def _run_judge(judge_thinking=None, main_thinking=None):
    cfg = _judge_config(judge_thinking)
    if main_thinking is not None:
        cfg["samplers"] = {"enable_thinking": main_thinking}
    global_config.config = cfg
    captured = []
    original_post = quality.requests.post
    quality.requests.post = _capture_post(captured, JUDGE_REPLY)
    try:
        quality._call_logic_judge_once(
            "http://judge.invalid/v1/chat/completions", "judge-model", "", 6,
            1, "task-1", "file.jsonl", {},
            "Score this.", expected_dims=["logic"])
    finally:
        quality.requests.post = original_post
    return captured[0] if captured else None


def test_judge_payload():
    print("\n-- judge request --")
    body = _run_judge(judge_thinking="disable")
    check("judge sends enable_thinking=False when its own mode is 'disable'",
          body is not None and body.get("chat_template_kwargs") == {"enable_thinking": False})

    body = _run_judge(judge_thinking="enable")
    check("judge sends enable_thinking=True when its own mode is 'enable'",
          body is not None and body.get("chat_template_kwargs") == {"enable_thinking": True})

    body = _run_judge(judge_thinking="inherit", main_thinking="disable")
    check("judge 'inherit' follows a 'disable' main setting",
          body is not None and body.get("chat_template_kwargs") == {"enable_thinking": False})

    body = _run_judge(judge_thinking=None, main_thinking="disable")
    check("judge with no own setting follows the main setting",
          body is not None and body.get("chat_template_kwargs") == {"enable_thinking": False})

    body = _run_judge(judge_thinking=None, main_thinking=None)
    check("judge sends NO chat_template_kwargs by default (v13.0.3 byte-identical)",
          body is not None and "chat_template_kwargs" not in body)

    body = _run_judge(judge_thinking="default", main_thinking="disable")
    check("judge 'default' overrides a 'disable' main setting",
          body is not None and "chat_template_kwargs" not in body)


# ---------------------------------------------------------------------------
# 3. The two fixers
# ---------------------------------------------------------------------------

class _FalseVar:
    """Stands in for the tk.BooleanVar generate_tk.py installs on app_state."""

    @staticmethod
    def get():
        return False


def _run_fixer(which, fixer_thinking=None, main_thinking=None, empty_fixer_params=False):
    """Call the Slop Fixer or the Anti-Slop Fixer and return the body it sent."""
    import generation
    # The full main sampler set, so the fallback test below can see whether the fixer
    # inherits top_p / repetition_penalty or falls back to its own hardcoded defaults.
    main_settings = {"temperature": 0.5, "top_p": 0.9, "repetition_penalty": 1.0}
    if main_thinking is not None:
        main_settings["enable_thinking"] = main_thinking
    api_config = {"url": "http://fixer.invalid/v1/chat/completions",
                  "model": "fixer-model", "key": "",
                  "sampler_settings": {} if empty_fixer_params else {"temperature": 0.5}}
    captured = []
    original_post = generation.requests.post
    original_config = app_state.global_config.config
    # The Slop Fixer reads app_state.master_duplication_enabled_var (a tk.BooleanVar that
    # generate_tk.py normally installs) to choose its debug-log path; a headless run has none.
    original_dup_var = app_state.master_duplication_enabled_var
    app_state.master_duplication_enabled_var = _FalseVar()
    generation.requests.post = _capture_post_text(captured)
    fixer_cfg = {"use_bad_words": False}
    if fixer_thinking is not None:
        # v13.0.4: SIBLING keys, never entries in slop_fixer_params / anti_slop_params.
        fixer_cfg["slop_fixer_enable_thinking" if which == "slop"
                  else "anti_slop_fixer_enable_thinking"] = fixer_thinking
    if main_thinking is not None:
        fixer_cfg["enable_thinking"] = main_thinking
    app_state.global_config.config = {"samplers": fixer_cfg}
    try:
        if which == "slop":
            generation.call_slop_fixer_llm(
                "Some text with slop in it.", "slop", api_config, main_settings, 1,
                current_max_attempts_param=1)
        else:
            generation.call_anti_slop_llm(
                "Some text with anti slop in it.", "anti slop", api_config, main_settings, 1,
                current_max_attempts_param=1)
    finally:
        generation.requests.post = original_post
        app_state.global_config.config = original_config
        app_state.master_duplication_enabled_var = original_dup_var
    return captured[0] if captured else None


def test_fixer_payloads():
    print("\n-- slop / anti-slop fixer requests --")
    for which in ("slop", "anti_slop"):
        label = "Slop Fixer" if which == "slop" else "Anti-Slop Fixer"

        body = _run_fixer(which, fixer_thinking="disable")
        check("%s sends enable_thinking=False for its own 'disable'" % label,
              body is not None and body.get("chat_template_kwargs") == {"enable_thinking": False})

        body = _run_fixer(which, fixer_thinking="enable")
        check("%s sends enable_thinking=True for its own 'enable'" % label,
              body is not None and body.get("chat_template_kwargs") == {"enable_thinking": True})

        body = _run_fixer(which, fixer_thinking="inherit", main_thinking="disable")
        check("%s 'inherit' follows a 'disable' main setting" % label,
              body is not None and body.get("chat_template_kwargs") == {"enable_thinking": False})

        body = _run_fixer(which, fixer_thinking=None, main_thinking="disable")
        check("%s with no own setting follows the main setting" % label,
              body is not None and body.get("chat_template_kwargs") == {"enable_thinking": False})

        body = _run_fixer(which, fixer_thinking=None, main_thinking=None)
        check("%s sends NO chat_template_kwargs by default (v13.0.3 byte-identical)" % label,
              body is not None and "chat_template_kwargs" not in body)

        body = _run_fixer(which, fixer_thinking="default", main_thinking="disable")
        check("%s 'default' overrides a 'disable' main setting" % label,
              body is not None and "chat_template_kwargs" not in body)


# ---------------------------------------------------------------------------
# 4. The fixer sampler fallback must survive the new setting
# ---------------------------------------------------------------------------

def test_fixer_fallback_is_not_broken():
    """The new thinking keys must NOT live inside slop_fixer_params / anti_slop_params.

    `call_slop_fixer_llm()` / `call_anti_slop_llm()` read their overrides with an
    ALL-OR-NOTHING `params or main_sampler_settings` fallback. Putting a single
    `enable_thinking` key inside `slop_fixer_params` makes that dict truthy, so it stops
    falling back to the main settings and the fixer's top_p jumps from the main 0.9 to its
    hardcoded 0.95 (and repetition_penalty 1.0 -> 1.1). That silently changes every
    rewrite request, so the thinking setting has to be a SIBLING key. This test pins the
    behaviour, not just the config shape: it runs the fixer with an EMPTY
    slop_fixer_params and asserts the main values survive.
    """
    print("\n-- fixer sampler fallback --")
    main = {"temperature": 0.5, "top_p": 0.9, "repetition_penalty": 1.0}
    body = _run_fixer("slop", main_thinking="disable", empty_fixer_params=True)
    check("Slop Fixer still inherits the main top_p when slop_fixer_params is empty",
          body is not None and body.get("top_p") == 0.9)
    check("Slop Fixer still inherits the main repetition_penalty",
          body is not None and body.get("repetition_penalty") == 1.0)
    # Sanity: with an explicit override the fallback is deliberately bypassed, so this
    # test would notice if the empty-vs-non-empty distinction disappeared entirely.
    body = _run_fixer("slop", main_thinking="disable")
    check("Slop Fixer honours an explicit slop_fixer_params top_p override",
          body is not None and body.get("top_p") == 0.95)

    # And the config shape itself: the thinking keys must be siblings, not children.
    import yaml
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(repo_root, "config", "config.yml"), "r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    samplers = cfg.get("samplers", {}) or {}
    for key in ("slop_fixer_params", "anti_slop_params"):
        params = samplers.get(key) or {}
        check("shipped %s does not carry enable_thinking (it must be a sibling)" % key,
              "enable_thinking" not in params)
    check("shipped samplers carries slop_fixer_enable_thinking",
          "slop_fixer_enable_thinking" in samplers)
    check("shipped samplers carries anti_slop_fixer_enable_thinking",
          "anti_slop_fixer_enable_thinking" in samplers)
    scoring_api = (cfg.get("quality", {}).get("scoring_api", {}) or {})
    check("shipped scoring_api does not carry enable_thinking inside sampler_settings",
          "enable_thinking" not in (scoring_api.get("sampler_settings") or {}))
    check("shipped scoring_api carries enable_thinking as a sibling",
          "enable_thinking" in scoring_api)


# ---------------------------------------------------------------------------
# 4. The shipped config must stay neutral
# ---------------------------------------------------------------------------

def test_shipped_config():
    print("\n-- shipped config/config.yml --")
    import yaml
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(repo_root, "config", "config.yml"), "r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)

    samplers = cfg.get("samplers", {}) or {}
    main_mode = samplers.get("enable_thinking", "default")
    check("shipped samplers.enable_thinking is 'default'", main_mode == "default")

    for key in ("slop_fixer_enable_thinking", "anti_slop_fixer_enable_thinking"):
        own = samplers.get(key, "inherit")
        check("shipped samplers.%s is 'inherit'" % key, own == "inherit")
        resolved = api_handler.resolve_thinking_mode(own, {"enable_thinking": main_mode})
        check("shipped samplers.%s resolves to 'default' (sends nothing)" % key,
              resolved == "default")

    judge_own = (cfg.get("quality", {}).get("scoring_api", {}) or {}) \
        .get("enable_thinking", "inherit")
    check("shipped quality.scoring_api.enable_thinking is 'inherit'", judge_own == "inherit")
    check("shipped judge resolves to 'default' (sends nothing)",
          api_handler.resolve_thinking_mode(judge_own,
                                          {"enable_thinking": main_mode}) == "default")


# ---------------------------------------------------------------------------
# 5. Every auxiliary builder must call the helper
# ---------------------------------------------------------------------------

def test_source_wiring():
    print("\n-- source wiring --")
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def read(name):
        with open(os.path.join(repo_root, name), "r", encoding="utf-8") as handle:
            return handle.read()

    generation_src = read("generation.py")
    quality_src = read("quality.py")

    check("generation.py imports apply_thinking_mode",
          "apply_thinking_mode" in generation_src)
    # Assert the CALL SITES structurally, not by whitespace: normalise runs of
    # whitespace so a reindent does not fail the test.
    def flat(text):
        return " ".join(text.split())

    gen_flat = flat(generation_src)
    qual_flat = flat(quality_src)

    check("Slop Fixer applies the thinking mode",
          "apply_thinking_mode( payload_data, global_config.get(SLOP_FIXER_THINKING_KEY), main_sampler_settings)"
          in gen_flat)
    check("Anti-Slop Fixer applies the thinking mode",
          "apply_thinking_mode( payload_data, global_config.get(ANTI_SLOP_FIXER_THINKING_KEY), main_sampler_settings)"
          in gen_flat)
    check("Regex Watch rewrite applies the thinking mode",
          "apply_thinking_mode(payload_dict, sampler_settings.get('enable_thinking'))" in gen_flat)
    check("quality.py imports apply_thinking_mode", "apply_thinking_mode" in quality_src)
    check("the judge applies the thinking mode",
          "apply_thinking_mode( payload_dict, judge_thinking_mode(), global_config.get('samplers', {}))"
          in qual_flat)

    # The four main builders must still read the main setting directly.
    check("main generation builders still read enable_thinking",
          generation_src.count("thinking_mode = sampler_settings_local.get('enable_thinking', 'default')") == 4)


# ---------------------------------------------------------------------------
# 6. The config editor round-trips the three dropdowns (needs a display)
# ---------------------------------------------------------------------------

def test_editor_round_trip():
    """Save -> load round trip for the three new dropdowns, and the params dicts stay empty.

    Skipped cleanly without a display, like the other editor UI tests.
    """
    print("\n-- config editor round trip --")
    try:
        import tkinter as tk
    except Exception as exc:                       # pragma: no cover - environment dependent
        print("  skip (no tkinter: %s)" % exc)
        return
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    try:
        root = tk.Tk()
        root.withdraw()
    except Exception as exc:                       # pragma: no cover - environment dependent
        print("  skip (no display: %s)" % exc)
        return
    try:
        from config_loader import ConfigLoader
        import app_state
        loader = ConfigLoader(os.path.join(repo_root, "config", "config.yml"))
        app_state.global_config = loader
        from config_editor import ConfigEditor
        editor = ConfigEditor(root, loader, tk.BooleanVar(value=False),
                              tk.BooleanVar(value=False))
        check("editor builds and loads the shipped defaults",
              editor.slop_fixer_thinking_var.get() == "inherit"
              and editor.anti_slop_fixer_thinking_var.get() == "inherit"
              and editor.quality_judge_thinking_var.get() == "inherit"
              and editor.enable_thinking_mode_var.get() == "default")

        editor.slop_fixer_thinking_var.set("disable")
        editor.anti_slop_fixer_thinking_var.set("enable")
        editor.quality_judge_thinking_var.set("disable")
        data = editor._get_current_editor_config_data()
        samplers = data["samplers"]
        check("editor saves slop_fixer_enable_thinking as a SIBLING",
              samplers.get("slop_fixer_enable_thinking") == "disable")
        check("editor saves anti_slop_fixer_enable_thinking as a SIBLING",
              samplers.get("anti_slop_fixer_enable_thinking") == "enable")
        check("editor saves the judge mode as a SIBLING of sampler_settings",
              data["quality"]["scoring_api"].get("enable_thinking") == "disable")
        check("editor leaves slop_fixer_params empty",
              samplers.get("slop_fixer_params") == {})
        check("editor leaves anti_slop_params empty",
              samplers.get("anti_slop_params") == {})

        loader.config = data
        editor.load_config_handler()
        check("editor reloads the slop fixer mode",
              editor.slop_fixer_thinking_var.get() == "disable")
        check("editor reloads the anti-slop mode",
              editor.anti_slop_fixer_thinking_var.get() == "enable")
        check("editor reloads the judge mode",
              editor.quality_judge_thinking_var.get() == "disable")
        editor.validate_config_handler(show_success_message=False)
    finally:
        try:
            root.destroy()
        except Exception:
            pass


def main():
    test_resolution_rules()
    test_judge_payload()
    test_fixer_payloads()
    test_fixer_fallback_is_not_broken()
    test_shipped_config()
    test_source_wiring()
    test_editor_round_trip()

    print("\n" + ("ALL PASSED" if not FAILURES else "FAILURES: %d" % len(FAILURES)))
    for label in FAILURES:
        print("  - " + label)
    return 1 if FAILURES else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        os.chdir("/")
        shutil.rmtree(_TMP, ignore_errors=True)
