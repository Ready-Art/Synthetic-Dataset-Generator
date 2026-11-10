#!/usr/bin/env python3
"""Fix the empty thought block for a Gemma 4 (26B / 31B) no-thinking fine-tune in axolotl, and prove it.

AUTHORSHIP: this script was written by Karki (a user on Discord), not by the maintainers of this
repository. It is redistributed here unchanged in design, with a few robustness fixes applied (missing
paths and unreadable configs now report a message instead of a traceback). Credit for the idea and the
implementation belongs to Karki.

WHY: Gemma 4 26B and 31B expect every reply to open with a thought channel, which is empty when
thinking is off: <|channel>thought\\n<channel|>. Gemma's chat template writes that empty block only for
the reply being generated, never for the replies inside a conversation. axolotl trains every reply, so
it trains each one straight after `<|turn>model\\n`, which teaches the model to skip its thought: with
thinking turned on, it stops thinking. Google: "When fine-tuning gemma-4-26B-A4B-it and gemma-4-31B-it
with a dataset that does not include thinking, you can achieve better results by adding the empty
channel to your training prompts" (https://ai.google.dev/gemma/docs/core/prompt-formatting-gemma4).

THE FIX is a training copy of your chat template that writes the empty block before every reply.
The template writes it outside the reply text, so axolotl masks it: the model sees the block before
every reply and is never trained to write it, and no reply is trained straight after `<|turn>model\\n`.
Inside the reply text axolotl would train on the block, so the data carries none; this tool removes
any it finds.

Run both steps inside your axolotl environment, from the folder you run axolotl from. No GPU.

STEP 1, check and fix:

    python3 gemma4_thought_fix.py data.jsonl --config your_axolotl_config.yml

  It (1) checks the data format, (2) checks the thought tokens in every reply, writes a copy with any
  thought block removed from reply text (never overwriting), and lists anything to check by hand,
  (3) finds the chat template your config trains this file with, (4) renders every sample through it
  the way axolotl does, and (5) if replies lack the empty block (Gemma's stock template and axolotl's
  built-in gemma4 never write it), writes chat_template.train.jinja beside the data, re-checks, and
  prints the two config lines that use it. It ends with PASS, NOT READY (lines to check by hand) or
  NOT FIXED. --template FILE checks a template you choose instead of reading the config.

STEP 2, the proof, on the tokens axolotl actually produced:

    axolotl preprocess your_axolotl_config.yml
    python3 gemma4_thought_fix.py verify last_run_prepared --tokenizer /path/to/base_model

  It checks every reply in every row: it starts with the empty block, the block is masked, the reply
  after it is trained, and nothing switches thinking on. Step 1 prints this command with your config's
  paths filled in. RESULT: FIXED there means the data is ready to train.

Formats: ShareGPT ({"conversations": [{"from": ..., "value": ...}]}) and OpenAI
({"messages": [{"role": ..., "content": ...}]}), one JSON object per line, text content. Roles map as
axolotl maps them: human/user, gpt/assistant, system, or the config entry's roles:.
Exit status 0 means PASS (step 1) or FIXED (step 2).

Credit: Karki (Discord).
"""
import argparse, json, os, re, sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

OPEN, CLOSE, THINK = "<|channel>", "<channel|>", "<|think|>"
EMPTY = "<|channel>thought\n<channel|>"
THOUGHT_START = "<|channel>thought\n"
LEAD = re.compile(r"\A\s*<\|channel>thought(.*?)<channel\|>", re.S)
MODEL_TURN = "<|turn>model\n"

# axolotl's default role map (prompt_strategies/chat_template.py). A dataset entry's `roles:` replaces
# it, and a role the map does not name passes through unchanged. Only roles mapped to "assistant" are
# trained (roles_to_train defaults to ["assistant"]).
AXOLOTL_ROLES = {"human": "user", "user": "user", "assistant": "assistant", "gpt": "assistant",
                 "system": "system", "tool": "tool"}
ROLE_MAP = dict(AXOLOTL_ROLES)
RENDERED = {"user", "assistant", "system", "developer"}


def role_of(r):
    return ROLE_MAP.get(r, r)


def is_reply(t, rk):
    return role_of(t[rk]) == "assistant"

GOOD, BAD, WARN = "  OK   ", "  FAIL ", "  NOTE "


def say(*lines):
    for l in lines:
        print(l)


def plural(n, word):
    return f"{n} {word}{'' if n == 1 else 's'}"


def lines_list(ns, k=8):
    return ", ".join(map(str, ns[:k])) + (", ..." if len(ns) > k else "")


def ask(question, assume_yes):
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        say(f"  (not asked: no terminal. Re-run with --yes to answer yes.)")
        return False
    return input(f"  {question} [y/N] ").strip().lower() in ("y", "yes")


def need_path(p, what):
    """The path, or a readable message instead of a traceback when it is missing."""
    if not Path(p).exists():
        sys.exit(f"{what} {str(p)!r} does not exist. Check the path, or run this from the folder you run "
                 "axolotl from.")
    return Path(p)


def verb(n):
    return "is" if n == 1 else "are"


def new_path(src, suffix):
    p = src.with_name(src.stem + suffix)
    i = 2
    while p.exists():
        p = src.with_name(f"{src.stem}{suffix.rsplit('.', 1)[0]}-{i}.{suffix.rsplit('.', 1)[1]}")
        i += 1
    return p


def read_jsonl(src):
    """(has a byte-order mark, lines with their own endings). Splits on \\n only: str.splitlines would
    also split inside a JSON string at U+2028, U+0085 and the like."""
    if src.is_dir():
        sys.exit(f"{src} is a folder. Pass the .jsonl file that holds your training data.")
    try:
        text = src.read_bytes().decode("utf-8")
    except UnicodeDecodeError as e:
        sys.exit(f"{src} is not UTF-8 text ({e}).")
    except OSError as e:
        sys.exit(f"Could not read {src} ({e}).")
    bom = text.startswith("\ufeff")
    parts = (text[1:] if bom else text).split("\n")
    return bom, [p + "\n" for p in parts[:-1]] + ([parts[-1]] if parts[-1] else [])


def dump_like(obj, orig, line):
    """Serialise a fixed sample in the same JSON style as its original line."""
    body = line.rstrip("\r\n")
    for ea in (False, True):
        for seps in ((", ", ": "), (",", ":")):
            if json.dumps(orig, ensure_ascii=ea, separators=seps) == body:
                return json.dumps(obj, ensure_ascii=ea, separators=seps)
    return json.dumps(obj, ensure_ascii=all(ord(c) < 128 for c in body),
                      separators=(", ", ": ") if '": ' in body else (",", ":"))


def load_cfg(path):
    try:
        import yaml
    except ImportError:
        sys.exit("--config needs PyYAML (axolotl has it): run this inside your axolotl environment, "
                 "or pass --template instead.")
    try:
        cfg = yaml.safe_load(need_path(path, "The config file").read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as e:
        sys.exit(f"{path} is not valid YAML ({e}).")
    if not isinstance(cfg, dict):
        sys.exit(f"{path} is not an axolotl config: its top level is a {type(cfg).__name__}, not a mapping "
                 "of keys.")
    return cfg


# ------------------------------------------------------------------ data: format and the rule

def shape(s):
    if isinstance(s.get("conversations"), list):
        return "conversations", "from", "value"
    if isinstance(s.get("messages"), list):
        return "messages", "role", "content"
    return None


def format_errors(s):
    if not isinstance(s, dict):
        return ["line is not a JSON object"]
    sh = shape(s)
    if sh is None:
        return ['no "conversations" (ShareGPT) or "messages" (OpenAI) list']
    key, rk, tk = sh
    errs, roles = [], []
    for t in s[key]:
        if not isinstance(t, dict) or rk not in t or tk not in t:
            errs.append(f'a turn lacks "{rk}" or "{tk}"')
            continue
        if not isinstance(t[tk], str):
            errs.append(f'a turn\'s "{tk}" is not plain text')
        r = role_of(t[rk])
        if r == "model":
            errs.append(f"role {t[rk]!r} renders as a reply, but axolotl never trains it (it trains roles mapped "
                        "to 'assistant'): rename it to 'assistant' or 'gpt', or map it under roles: in your config")
        elif r not in RENDERED:
            errs.append(f"unknown role {t[rk]!r}")
        roles.append(r)
    if not errs:
        if "assistant" not in roles:
            errs.append("no model reply in the sample")
        elif roles[-1] != "assistant":
            errs.append("the sample does not end with a model reply")
    return errs


def check_sample(s):
    """(issues, fixed sample, final reply has reasoning_content). The rule: reply text carries no
    thought block. Fixable: block_in_text. The rest are for a hand check."""
    key, rk, tk = shape(s)
    fixed = json.loads(json.dumps(s))
    issues = set()
    for t in fixed[key]:
        txt = t[tk]
        if THINK in txt:
            issues.add("think")
        if not is_reply(t, rk):
            if OPEN in txt or CLOSE in txt:
                issues.add("nonmodel")
            continue
        m = LEAD.match(txt)
        if m and m.group(1).strip():
            issues.add("real_thought")
            continue
        rest = txt[m.end():].lstrip() if m else txt
        if not rest.strip():
            issues.add("empty_reply")
        elif OPEN in rest or CLOSE in rest:
            issues.add("stray")
        elif m:
            issues.add("block_in_text")
            t[tk] = rest
    last = [t for t in fixed[key] if is_reply(t, rk)][-1]
    return issues, fixed, bool(last.get("reasoning_content"))


DATA_ISSUES = {  # code: (fixable, reason in the review file, what it means)
    "block_in_text": (True, "", "a reply's text starts with an empty thought block. The training template writes "
                      "that block itself, outside the text, where axolotl masks it; inside the text axolotl "
                      "would train on it (or Gemma's stock template deletes it). The fix removes it"),
    "real_thought": (False, "a real thought written inside the reply text",
                     "a reply's text starts with a real (non-empty) thought. Gemma's template deletes thoughts "
                     "written there, so the model never sees it. Move it to a \"reasoning_content\" field on "
                     "the final reply, or delete it"),
    "empty_reply": (False, "an empty reply",
                    "a reply is empty, or is only a thought block. axolotl skips an empty reply, so nothing in it "
                    "is trained, and the model sees an empty turn. Write the reply or remove the sample"),
    "stray": (False, "a stray <|channel> or <channel|> inside a reply",
              "a lone <|channel> or <channel|> sits inside a reply (e.g. 'draft<channel|>reply', a "
              "generation leak). Gemma's template deletes the token and joins the text around it"),
    "nonmodel": (False, "a thought token in a user or system message",
                 "a thought token sits in a user or system message"),
    "think": (False, "<|think|> in the sample",
              "<|think|> (the thinking-ON switch) appears in the sample. In data with no thoughts it "
              "teaches the model to skip thinking when asked to think"),
}


# ------------------------------------------------------------------ rendering through the template

def load_template(arg):
    p = need_path(arg, "The template")
    if p.is_dir():
        if (p / "chat_template.jinja").exists():
            return (p / "chat_template.jinja").read_text(encoding="utf-8"), p / "chat_template.jinja"
        if not (p / "tokenizer_config.json").exists():
            sys.exit(f"{p} holds neither chat_template.jinja nor tokenizer_config.json, so there is no chat "
                     "template to read. Pass the model folder that has one, or the template file itself.")
        p = p / "tokenizer_config.json"
    if p.suffix == ".json":
        t = json.loads(p.read_text(encoding="utf-8")).get("chat_template")
        if isinstance(t, list):
            t = next((x["template"] for x in t if x.get("name") == "default"), None)
        if not t:
            sys.exit(f"{p} has no chat_template")
        return t, p
    return p.read_text(encoding="utf-8"), p


def template_from_hub_or_dir(base):
    """The chat template of a base model given as a local folder or a Hugging Face repo id."""
    if Path(base).exists():
        return load_template(base)
    try:
        from huggingface_hub import hf_hub_download
    except ImportError:
        sys.exit(f"Your config's base model {base!r} is not a local folder, and huggingface_hub is not "
                 "installed to fetch its template. Pass --template <its chat_template.jinja> instead.")
    for fn in ("chat_template.jinja", "tokenizer_config.json"):
        try:
            return load_template(hf_hub_download(base, fn))
        except SystemExit:
            raise
        except Exception:
            continue
    sys.exit(f"Could not fetch a chat template for {base!r}. Pass --template <its chat_template.jinja>.")


def axolotl_choice(c):
    """axolotl's config validators: chat_template_jinja on its own means chat_template: jinja."""
    ct, ctj = c.get("chat_template"), c.get("chat_template_jinja")
    return (str(ct) if ct else ("jinja" if ctj else None)), ctj


def uses_file(d, data):
    files = d.get("data_files")
    for p in [d.get("path")] + (files if isinstance(files, list) else [files]):
        if isinstance(p, str) and p and (Path(p).name == data.name
                                         or (Path(p).exists() and Path(p).resolve() == data.resolve())):
            return True
    return False


def effective_template(cfg, d):
    """(chat_template, chat_template_jinja, where it is set) that axolotl trains dataset entry `d` with.
    axolotl uses an entry's own chat_template when it names one; otherwise the tokenizer's template,
    which it replaces with the top-level chat_template when that is set."""
    ct, ctj = axolotl_choice(d or {})
    if ct and not ct.startswith("tokenizer_default"):
        return ct, ctj, "dataset"
    top = axolotl_choice(cfg)
    return top[0] or "tokenizer_default", top[1], "top"


def pick_dataset(cfg, data):
    """The datasets entry axolotl reads `data` from (None if the config lists none)."""
    dsets = [d for d in cfg.get("datasets") or [] if isinstance(d, dict)]
    mine = [d for d in dsets if uses_file(d, data)]
    pool = mine or dsets or [None]
    sig = lambda d: (effective_template(cfg, d)[:2], json.dumps((d or {}).get("roles"), sort_keys=True))
    if len({sig(d) for d in pool}) > 1:
        listing = "\n".join(f"    {d.get('path')}: chat_template {effective_template(cfg, d)[0]}"
                            + (f" ({effective_template(cfg, d)[1]})" if effective_template(cfg, d)[0] == "jinja" else "")
                            + (f", roles {d.get('roles')}" if d.get("roles") else "") for d in dsets)
        sys.exit(f"Your config's datasets differ in chat template or roles, and "
                 f"{'more than one' if mine else 'none'} of them is {data.name}:\n{listing}\n"
                 "Run again with a config holding only this file's entry, or with --template.")
    return pool[0]


def template_from_config(cfg_path, cfg, entry):
    """(template text, path, how it was found, where in the config it is set) for the template axolotl
    trains dataset entry `entry` with."""
    ct, ctj, where = effective_template(cfg, entry)
    if ct == "jinja":
        if not ctj:
            sys.exit("Your config says chat_template: jinja but gives no chat_template_jinja. Pass --template.")
        if os.path.isfile(ctj):
            return *load_template(ctj), f"chat_template_jinja: {ctj}", where
        if not os.path.isabs(ctj) and (Path(cfg_path).parent / ctj).is_file():
            sys.exit(f"Your config's chat_template_jinja {ctj!r} is not found from this folder ({Path.cwd()}).\n"
                     "axolotl opens that path from the folder you run axolotl in, and when the file is not there\n"
                     f"it silently uses the text {ctj!r} itself as the template. Run this from the folder you run\n"
                     "axolotl from, or make the path absolute.")
        if "{{" in ctj or "{%" in ctj:
            return ctj, Path(cfg_path), "chat_template_jinja (written inline in the config)", where
        sys.exit(f"Your config's chat_template_jinja {ctj!r} is not a file in this folder ({Path.cwd()}), and\n"
                 "axolotl would silently use that text itself as the template. Fix the path, or run this from\n"
                 "the folder you run axolotl from.")
    if ct.startswith("tokenizer_default"):
        base = cfg.get("tokenizer_config") or cfg.get("base_model")
        if not base:
            sys.exit("Your config names no base_model to take the template from. Pass --template.")
        text, p = template_from_hub_or_dir(base)
        return text, p, f"chat_template: {ct} (the base model's own template)", where
    try:
        import axolotl
    except ImportError:
        sys.exit(f"Your config uses axolotl's built-in chat_template: {ct}. Run this inside your axolotl "
                 "environment so it can read that template, or pass --template <the file>.")
    p = Path(axolotl.__file__).parent / "utils" / "chat_templates" / "templates" / f"{ct}.jinja"
    if not p.exists():
        sys.exit(f"axolotl's built-in template {ct!r} is not at {p}. Pass --template <the file>.")
    return *load_template(p), f"chat_template: {ct} (axolotl's built-in template)", where


def renderer(template_text, kwargs=None):
    try:
        import jinja2
        from jinja2.sandbox import ImmutableSandboxedEnvironment
    except ImportError:
        sys.exit("The render check needs jinja2: pip install jinja2 (axolotl already has it).")

    def raise_exception(msg):
        raise jinja2.exceptions.TemplateError(msg)

    # the environment transformers' apply_chat_template uses, which is what axolotl calls
    env = ImmutableSandboxedEnvironment(trim_blocks=True, lstrip_blocks=True,
                                        extensions=["jinja2.ext.loopcontrols"])
    env.filters["tojson"] = lambda x, indent=None, **k: json.dumps(x, ensure_ascii=False, indent=indent)
    env.globals["raise_exception"] = raise_exception
    env.globals["strftime_now"] = lambda f: datetime.now().strftime(f)
    tpl = env.from_string(template_text)

    def render(sample):
        key, rk, tk = shape(sample)
        msgs = []
        for t in sample[key]:
            m = {"role": role_of(t[rk]), "content": t[tk]}
            if "reasoning_content" in t:            # the one extra field axolotl passes by default
                m["reasoning_content"] = t["reasoning_content"]
            msgs.append(m)
        args = {"bos_token": "<bos>", "eos_token": "<eos>", **(kwargs or {})}
        return tpl.render(**args, messages=msgs, add_generation_prompt=False)
    return render


THINK_ON = ("thinking is switched ON (<|think|> in the system turn) for samples with no thought, which "
            "teaches the model to skip thinking when asked to think. Remove enable_thinking from "
            "chat_template_kwargs in your config")


def survives(rendered, reasoning=False):
    """Does every reply in the render start with exactly the empty block (the final one with the
    sample's own thought, if it has reasoning_content), with no other thought token in it?"""
    if THINK in rendered and not reasoning:
        return False, THINK_ON
    parts = [p.split("<turn|>")[0] for p in rendered.split(MODEL_TURN)[1:]]   # each reply alone
    if not parts:
        return False, "no model turn in the render"
    for i, p in enumerate(parts):
        if reasoning and i == len(parts) - 1:
            if not p.startswith(THOUGHT_START) or p.startswith(EMPTY):
                return False, "the final reply's reasoning_content did not render as its thought"
        elif not p.startswith(EMPTY):
            return False, "a reply does not start with the empty thought block"
        if p.count(OPEN) != 1 or p.count(CLOSE) != 1:
            return False, "a reply holds more than one thought block, or a stray thought token"
    return True, ""


def survival_check(samples, render, label, review=()):
    """(all pass, set of failure reasons) over every sample not held for a hand check."""
    held = [n for n, _, _ in samples if n in review]
    samples = [x for x in samples if x[0] not in review]
    if held:
        say(f"\n{WARN}{plural(len(held), 'sample')} held for a hand check {verb(len(held))} left out of "
            "this check.")
    if not samples:
        say(f"{BAD}No samples left to check.")
        return False, set()
    fails, errors = defaultdict(list), []
    for n, s, reasoning in samples:
        try:
            ok, why = survives(render(s), reasoning)
        except Exception as e:                       # template refused the sample
            errors.append((n, str(e)[:120]))
            continue
        if not ok:
            fails[why].append(n)
    passed = len(samples) - sum(map(len, fails.values())) - len(errors)
    say(f"\n  Rendered {plural(len(samples), 'sample')} through {label}:")
    say(f"{GOOD if passed == len(samples) else BAD}{passed} of {len(samples)}: every reply starts with the "
        "empty thought block" + (" (a final reply with reasoning_content, with its thought)"
                                 if any(r for *_, r in samples) else "") + ".")
    for why, ns in fails.items():
        say(f"{BAD}{plural(len(ns), 'sample')}: {why}  (lines {lines_list(ns)})")
    for n, e in errors[:5]:
        say(f"{BAD}line {n}: the template raised an error: {e}")
    if len(errors) > 5:
        say(f"{BAD}... and {len(errors) - 5} more template errors")
    return passed == len(samples), set(fails) | ({"template error"} if errors else set())


# The block every Gemma 4 template revision uses to render a message's reasoning as its thought:
#   {%- if thinking_text and <condition> -%} {{- '<|channel>thought\n' + thinking_text + ... -}} {%- endif -%}
# (stock R4, and axolotl's built-in gemma4 / gemma4_unified, which follow an earlier revision).
# The patch adds an {%- elif -%} that writes the empty block when the message renders no thought.
THOUGHT_IF = re.compile(
    r"(?P<head>\{%-\s*if thinking_text and [^%]*?-%\}\s*"
    r"\{\{-\s*'<\|channel>thought\\n' \+ thinking_text \+ '\\n<channel\|>'\s*-\}\}\s*?\n)"
    r"(?P<indent>[ \t]*)(?P<endif>\{%-\s*endif\s*-%\})")
MARK = "TRAINING PATCH (gemma4_thought_fix.py)"
OLD_MARK = "TRAINING PATCH: empty thought block on the final model turn"
EVERY = ("{indent}{{#- " + MARK + ": the empty thought block before every model reply that renders no thought -#}}\n"
         "{indent}{{%- elif role == 'model' and not add_generation_prompt and not continue_same_model_turn -%}}\n"
         "{indent}    {{{{- '<|channel>thought\\n<channel|>' -}}}}\n")
PATCH_HEADER = ("{#- TRAINING TEMPLATE made by gemma4_thought_fix.py: your Gemma 4 template, plus the empty\n"
                "    thought block before every model reply, outside the reply text so axolotl masks it.\n"
                "    For training only; ship the model with the normal template. -#}\n")


def patch_template(text):
    if OLD_MARK in text:
        return None, ("this is a training template from an earlier version of this tool, which put the block on "
                      "the final reply only. Point chat_template_jinja back at your original template (or remove "
                      "those two lines) and run again")
    if "TRAINING PATCH" in text:
        return None, "this template is already patched, so what fails above is something the patch does not address"
    hits = list(THOUGHT_IF.finditer(text))
    if len(hits) != 1 or not all(v in text for v in ("set role", "continue_same_model_turn")):
        return None, ("this template's thought-rendering code is not the Gemma 4 form the patch knows, so it "
                      "can't be patched automatically. Download chat_template.jinja from google/gemma-4-31B-it "
                      "and run again with --template pointing at it")
    m = hits[0]
    return PATCH_HEADER + text[:m.start("indent")] + EVERY.format(indent=m.group("indent")) + text[m.start("indent"):], ""


# ------------------------------------------------------------------ step 1

def config_notes(cfg, entry, good):
    """What in the config's entry for this file would stop axolotl training it the way step 1 checks."""
    notes = []
    rtt = entry.get("roles_to_train")
    if rtt is not None and "assistant" not in rtt:
        notes.append(f"roles_to_train is {rtt}, which leaves the replies untrained.")
    if cfg.get("train_on_inputs"):
        notes.append("train_on_inputs is on, so axolotl trains every token, the empty block included; verify will fail.")
    mpm = entry.get("message_property_mappings") or {}
    want = (entry.get("field_messages") or "messages", mpm.get("role") or entry.get("message_field_role") or "role",
            mpm.get("content") or entry.get("message_field_content") or "content")
    for sh in sorted({shape(s) for _, s, _ in good} - {want}):
        notes.append(f"samples here use {sh[0]} / {sh[1]} / {sh[2]}, but this file's datasets entry has axolotl read "
                     f"{want[0]} / {want[1]} / {want[2]}. Set field_messages: {sh[0]} and message_property_mappings: "
                     f"{{role: {sh[1]}, content: {sh[2]}}} on that entry.")
    return notes


def cmd_check(a):
    global ROLE_MAP
    src = need_path(a.data, "The data file")
    bom, raw = read_jsonl(src)
    cfg, entry = {}, None
    if a.config:
        cfg = load_cfg(a.config)
        entry = pick_dataset(cfg, src)
        if entry and entry.get("roles"):
            ROLE_MAP = {s: t for t, srcs in entry["roles"].items() for s in srcs}
    say(f"\n== 1. Data format: {src}")
    fmt, good = defaultdict(list), []
    for n, line in enumerate(raw, 1):
        if not line.strip():
            continue
        try:
            s = json.loads(line)
        except json.JSONDecodeError:
            fmt["not valid JSON"].append(n)
            continue
        errs = format_errors(s)
        for e in dict.fromkeys(errs):
            fmt[e].append(n)
        if not errs:
            good.append((n, s, line))
    review = {n: e for e, ns in fmt.items() for n in ns}          # line -> reason it needs a hand check
    total = len(good) + len(review)
    if not good and total == 0:
        say(f"{BAD}{src} has no lines. Point this at the .jsonl file that holds your training data.",
            "\nRESULT: NOT READY.")
        return 2
    if not good:
        say(f"{BAD}None of the {total} lines is a usable conversation:",
            *[f"{BAD}{plural(len(ns), 'sample')}: {e}  (lines {lines_list(ns)})" for e, ns in fmt.items()],
            '  Expected one JSON object per line: {"conversations": [{"from": "human"|"gpt"|"system", "value": "..."}]}',
            '  or {"messages": [{"role": "user"|"assistant"|"system", "content": "..."}]}.')
        return 2
    say(f"{GOOD if not review else BAD}{len(good)} of {total} samples are conversations in the expected format.")
    for e, ns in fmt.items():
        say(f"{BAD}{plural(len(ns), 'sample')}: {e}  (lines {lines_list(ns)}). Not checked; fix them by hand.")
    for note in config_notes(cfg, entry, good) if entry is not None else ():
        say(f"{WARN}Config: {note}")

    say("\n== 2. Thought tokens in the data",
        "  Rule: reply text carries no thought block. The training template writes the empty block",
        "  before every reply, outside the text, where axolotl masks it.")
    found, fixed_by_line, reasoning, orig = defaultdict(list), {}, {}, {}
    for n, s, line in good:
        issues, fixed, reasoning[n] = check_sample(s)
        orig[n] = (s, line)
        for i in issues:
            found[i].append(n)
        if fixed != s:
            fixed_by_line[n] = fixed
    flagged = {n for v in found.values() for n in v}
    say(f"{GOOD if not flagged else BAD}{len(good) - len(flagged)} of {len(good)} samples follow the rule.")
    for code, (fixable, short, what) in DATA_ISSUES.items():
        if code in found:
            say(f"{BAD if fixable else WARN}{plural(len(found[code]), 'sample')}: {what}.  "
                f"(lines {lines_list(found[code])})")
            if not fixable:
                for n in found[code]:
                    review.setdefault(n, short)
    if any(reasoning.values()):
        n = sum(reasoning.values())
        say(f"{WARN}{plural(n, 'sample')} {'ends' if n == 1 else 'end'} in a reply with reasoning_content (a "
            "real thought), which renders in place of the empty block. Tuners who mix thinking samples in "
            "train only their last reply.")
    review_file = None
    if review:
        review_file = new_path(src, ".review.txt")
        review_file.write_text("".join(f"line {n}: {review[n]}\n" for n in sorted(review)), encoding="utf-8")
        say(f"{WARN}Every line to check by hand, with its reason, is listed in {review_file}")

    samples = [(n, s, reasoning[n]) for n, s, _ in good]
    data_label = src.name
    say("\n== 3. Fix the data")
    if fixed_by_line:
        out = Path(a.out) if a.out else new_path(src, ".fixed.jsonl")
        if out.exists() or out.resolve() == src.resolve():
            sys.exit(f"  {out} exists; refusing to overwrite. Choose another --out.")
        try:
            out.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            sys.exit(f"  Could not create the folder for {out} ({e}). Choose another --out.")
        say(f"  The fix removes the thought block from the reply text of {plural(len(fixed_by_line), 'sample')} "
            "and copies every other line byte for byte.")
        if ask(f"Write the fixed data to {out}?", a.yes):
            body = [dump_like(fixed_by_line[n], orig[n][0], line) + line[len(line.rstrip("\r\n")):]
                    if n in fixed_by_line else line for n, line in enumerate(raw, 1)]
            try:
                out.write_bytes((("\ufeff" if bom else "") + "".join(body)).encode("utf-8"))
            except OSError as e:
                sys.exit(f"  Could not write {out} ({e}). Choose another --out.")
            say(f"{GOOD}Wrote {out}. Your original file is untouched. See every change with:  diff {src} {out}")
            samples = [(n, fixed_by_line.get(n, s), reasoning[n]) for n, s, _ in good]
            data_label = out.name
        else:
            say("  Not written. The check below uses the data as it is now.")
    else:
        say(f"{GOOD}Nothing to fix in the data.")

    say("\n== 4. Does every reply get the empty block in training?")
    if not (a.template or a.config):
        say(f"{WARN}Skipped: no --config or --template given. The template writes the block, so the template",
            "        decides. Run again with --config <your axolotl config.yml>.",
            "\nRESULT: NOT CHECKED.")
        return 1
    kwargs = cfg.get("chat_template_kwargs") or {}
    if a.template:
        tpl_text, tpl_path = load_template(a.template)
        how, where = "--template", "top"
    else:
        tpl_text, tpl_path, how, where = template_from_config(a.config, cfg, entry)
    say(f"  Template: {tpl_path}", f"  Found from: {how}")
    if kwargs:
        say(f"  chat_template_kwargs from your config: {kwargs}")
    if a.template and a.config:
        say(f"{WARN}--template overrides your config; make sure it is the template the config trains with.")
    held = set(review)
    ok, reasons = survival_check(samples, renderer(tpl_text, kwargs), tpl_path.name, held)
    if ok:
        return finish(review, review_file, "PASS. Rendered through that template, every reply starts with the "
                      "empty thought block.", [], f"Train on {data_label} with your current template.", a)
    if reasons == {THINK_ON}:
        say(f"\nRESULT: NOT FIXED. {THINK_ON[0].upper() + THINK_ON[1:]}, then run this again.")
        return 1

    say("\n== 5. Fix the template",
        "  Your template does not write the empty block on the replies inside a conversation (Gemma's",
        "  stock template and axolotl's gemma4 write it only for a reply being generated). The fix is a",
        "  training copy that writes it before every reply. It sits outside the reply text, so axolotl masks",
        "  it: the model is never trained to start a reply straight after <|turn>model\\n.")
    patched, why = patch_template(tpl_text)
    if patched is None:
        say(f"{BAD}Can't: {why}.", "\nRESULT: NOT FIXED.")
        return 1
    tout = new_path(src.with_name("chat_template.jinja"), ".train.jinja")   # beside the data, never in axolotl
    if not ask(f"Write the training template to {tout}?", a.yes):
        say("  Not written.", "\nRESULT: NOT FIXED.")
        return 1
    try:
        tout.write_text(patched, encoding="utf-8")
    except OSError as e:
        sys.exit(f"{BAD}Could not write {tout} ({e}).\n\nRESULT: NOT FIXED.")
    say(f"{GOOD}Wrote {tout}. Your original template is untouched.")
    ok, reasons = survival_check(samples, renderer(patched, kwargs), tout.name, held)
    if not ok:
        say("\nRESULT: NOT FIXED. The training template still fails on the samples above.")
        return 1
    place = "under this file's entry in datasets:" if where == "dataset" else "at the top level"
    lines = [f"  In your axolotl config, {place}, set:", "    chat_template: jinja",
             f"    chat_template_jinja: {tout.resolve()}"]
    if where == "top":
        lines += ["  That applies to every dataset in the config without a template of its own. Leave any",
                  "  chat_template: tokenizer_default on a dataset entry as it is."]
    return finish(review, review_file, "PASS with the training template.", lines,
                  f"Train on {data_label}. axolotl saves this training template into the trained model's folder\n"
                  "  (chat_template.jinja, or \"chat_template\" in tokenizer_config.json); before uploading, replace it\n"
                  "  with the template you normally ship, the base model's chat_template.jinja.", a)


def finish(review, review_file, verdict, config_lines, train_line, a):
    if review:
        say(f"\nRESULT: NOT READY. {plural(len(review), 'line')} "
            f"{'needs' if len(review) == 1 else 'need'} a hand check before training (listed in",
            f"  {review_file}). Everything else passes. Fix or remove those lines, then run this again.")
        if config_lines:
            say("  The training template is ready either way.", *config_lines)
        return 1
    say(f"\nRESULT: {verdict}", *config_lines, "  " + train_line, *verify_hint(a))
    return 0


def verify_hint(a):
    """The last step: the same check on axolotl's own tokens, with the config's paths filled in."""
    prepared, base = "<dataset_prepared_path>", "<base model folder>"
    if a.config:
        cfg = load_cfg(a.config)
        prepared = cfg.get("dataset_prepared_path") or "last_run_prepared"     # axolotl's default
        base = cfg.get("tokenizer_config") or cfg.get("base_model") or base
    me = Path(sys.argv[0]).name
    return ("\nLAST STEP, the proof: this check read your template, not axolotl's output. After updating the",
            "  config, run axolotl's preprocessing and check the tokens it will actually train on:",
            f"    axolotl preprocess {a.config or 'config.yml'}",
            f"    python3 {me} verify {prepared} --tokenizer {base}",
            "  axolotl writes a new folder there for each config; verify checks the newest. Train with that",
            "  same config: any change to it makes axolotl prepare a new folder that verify has not checked.",
            "  Only when that prints RESULT: FIXED is the data ready to train.")


# ------------------------------------------------------------------ step 2

def load_tokenizer(arg):
    from tokenizers import Tokenizer
    p = Path(arg)
    if p.is_dir():
        p = p / "tokenizer.json"
    if not p.exists():
        try:
            from huggingface_hub import hf_hub_download
            p = Path(hf_hub_download(arg, "tokenizer.json"))
        except Exception as e:
            sys.exit(f"Could not find tokenizer.json for {arg!r} ({e}). Pass the base model folder.")
    return Tokenizer.from_file(str(p))


def cmd_verify(a):
    try:
        from datasets import load_from_disk
        import tokenizers  # noqa: F401
    except ImportError:
        sys.exit("verify needs the `datasets` and `tokenizers` packages (run it inside your axolotl environment).")
    tok = load_tokenizer(a.tokenizer)
    ids = lambda s: tok.encode(s, add_special_tokens=False).ids
    model_turn, empty, thought_start = ids(MODEL_TURN), ids(EMPTY), ids(THOUGHT_START)
    turn_open, turn_close = ids("<|turn>")[0], ids("<turn|>")[0]
    ch_open, ch_close, think = ids(OPEN)[0], ids(CLOSE)[0], ids(THINK)[0]

    root = Path(a.prepared)
    if not root.exists():
        sys.exit(f"{root} does not exist. Point verify at your config's dataset_prepared_path.")
    found = {p.parent for p in root.rglob("dataset_info.json")}
    found = {d.parent if (d.parent / "dataset_dict.json").exists() else d for d in found}
    if not found:
        sys.exit(f"No tokenized dataset found under {root}. Run `axolotl preprocess` first, and point verify at "
                 "your config's dataset_prepared_path.")
    stamp = lambda d: max(f.stat().st_mtime for f in d.rglob("*") if f.is_file())
    dirs = sorted(found, key=stamp, reverse=True)
    d, when = dirs[0], datetime.fromtimestamp(stamp(dirs[0])).strftime("%Y-%m-%d %H:%M")
    try:
        ds = load_from_disk(str(d))
    except Exception as e:
        sys.exit(f"Could not load {d}: {e}")
    if hasattr(ds, "keys") and not hasattr(ds, "column_names"):          # a DatasetDict
        from datasets import concatenate_datasets
        ds = concatenate_datasets(list(ds.values()))
    say(f"\n== Verify axolotl's tokenized dataset: {d}  ({len(ds)} rows, written {when})")
    if len(dirs) > 1:
        say(f"{WARN}{root} holds {len(dirs)} prepared datasets, one per config axolotl has prepared. This checks "
            "the newest; the others are older runs:",
            *[f"        {o}  (written {datetime.fromtimestamp(stamp(o)).strftime('%Y-%m-%d %H:%M')})" for o in dirs[1:]])
    for col in ("input_ids", "labels"):
        if col not in ds.column_names:
            sys.exit(f"{d}: no {col} column (columns: {ds.column_names})")
    if len(ds) == 0:
        sys.exit(f"{d} has no rows.")

    bad, replies, trained, reasoning_rows = defaultdict(list), 0, 0, 0
    for r, row in enumerate(ds):
        x, lab = row["input_ids"], row["labels"]
        starts = [i for i in range(len(x) - len(model_turn) + 1) if x[i:i + len(model_turn)] == model_turn]
        if not starts:
            bad["no model turn"].append(r)
            continue
        why, row_trained, row_reasoning = None, False, False
        for k, s0 in enumerate(starts):
            b = s0 + len(model_turn)
            e = next((i for i in range(b, len(x)) if x[i] in (turn_close, turn_open)), len(x))
            replies += 1
            if x[b:b + len(empty)] == empty:
                blk = len(empty)
                if any(v != -100 for v in lab[b:b + blk]):
                    why = ("the empty block is trained on; it should be masked (check train_on_inputs, and that "
                           "eot_tokens lists only <turn|>)")
                    break
            elif k == len(starts) - 1 and x[b:b + len(thought_start)] == thought_start and ch_close in x[b:e]:
                blk, row_reasoning = x.index(ch_close, b) + 1 - b, True
            else:
                why = "a reply does not start with the empty thought block"
                break
            if ch_open in x[b + blk:e] or ch_close in x[b + blk:e]:
                why = "a stray <|channel> or <channel|> inside a reply"
                break
            reply = lab[b + blk:e]
            if any(v != -100 for v in reply):
                if reply[0] == -100:
                    why = "a reply is trained, but not its first token after the block"
                    break
                trained += 1
                row_trained = True
        if why is None and think in x and not row_reasoning:
            why = "<|think|> (thinking ON) in a row with no thought"
        if why is None and not row_trained:
            why = "no reply in the row is trained on (every label is -100)"
        if why:
            bad[why].append(r)
        reasoning_rows += row_reasoning
    ok = len(ds) - sum(map(len, bad.values()))
    say(f"{GOOD if ok == len(ds) else BAD}{ok} of {len(ds)} rows: every reply starts with the masked empty "
        "thought block, and the reply after it is trained on.")
    for why, rs in bad.items():
        say(f"{BAD}{plural(len(rs), 'row')}: {why}  (rows {lines_list(rs)})")
    if ok == len(ds):
        say(f"{WARN}{replies} {'reply' if replies == 1 else 'replies'} in all, {trained} trained on"
            + (" (the rest are masked: fine if your config trains only some turns)" if trained < replies else "")
            + (f"; {plural(reasoning_rows, 'row')} end in a real thought (reasoning samples)." if reasoning_rows else "."))
        say("\nRESULT: FIXED. Every reply axolotl will train on comes after a masked empty thought block, so",
            "  training no longer teaches the model to skip its thought.")
        return 0
    say("\nRESULT: NOT FIXED in what axolotl will train on.",
        f"  This checked {d}, written {when}.",
        "  If step 1 said PASS, make sure your config points at the training template and that",
        "  `axolotl preprocess` ran after that change.")
    return 1


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "verify":
        ap = argparse.ArgumentParser(prog="gemma4_thought_fix.py verify")
        ap.add_argument("_", help=argparse.SUPPRESS)
        ap.add_argument("prepared", help="axolotl's dataset_prepared_path (default last_run_prepared)")
        ap.add_argument("--tokenizer", required=True, help="base model folder, its tokenizer.json, or its hub id")
        sys.exit(cmd_verify(ap.parse_args()))
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog="Written by Karki (Discord); redistributed here with a few robustness "
                                        "fixes. Not part of the Synthetic Dataset Generator.")
    ap.add_argument("data", help="your training data, .jsonl")
    ap.add_argument("--config", help="your axolotl config .yml; the tool reads which chat template it trains with")
    ap.add_argument("--template", help="the chat template to check against, if not using --config (.jinja, "
                                       "tokenizer_config.json, or a model folder)")
    ap.add_argument("--out", help="where to write the fixed data (default: <data>.fixed.jsonl)")
    ap.add_argument("--yes", action="store_true", help="answer yes to every 'write this file?' question")
    sys.exit(cmd_check(ap.parse_args()))


if __name__ == "__main__":
    main()
