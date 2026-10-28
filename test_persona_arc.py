"""Tests for the persona/emotional-arc engine helpers -- run directly: `python test_persona_arc.py`.

Covers _split_emotional_arc_entry / _expand_emotional_arc / _emotional_state_injection /
_persona_block_for_characters (generation.py). These drive the deliberate persona/personality
inner/outer split: inner persona is withheld until the "breakthrough" turn, and emotional states
can be flat or a progression expanded across the conversation's assistant turns.

No test framework, network or display. Copies config/ into a temporary directory and runs there so
`import app_state`/`generation` resolve paths the same way the app does. Exits non-zero on failure.
"""
import atexit
import os
import shutil
import sys
import tempfile

REPO = os.path.dirname(os.path.abspath(__file__))
SCRATCH = tempfile.mkdtemp(prefix="sdg-persona-test-")
atexit.register(shutil.rmtree, SCRATCH, ignore_errors=True)
shutil.copytree(os.path.join(REPO, "config"), os.path.join(SCRATCH, "config"))
os.chdir(SCRATCH)
sys.path.insert(0, REPO)

import generation as g  # noqa: E402

FAILURES = []


class _Var:
    """Stands in for a Tk variable or text widget: get() returns the stored value whatever its arguments."""
    def __init__(self, value="0"):
        self.value = value

    def get(self, *args):
        return self.value

    def set(self, value):
        self.value = value


class _EditorStub:
    """Stands in for a ConfigEditor: attributes not given return a _Var holding "0"."""
    def __init__(self, **attrs):
        self.__dict__.update(attrs)
        self.datasets_panel = type("DatasetPanelStub", (), {"get_config": lambda _self: {"sources": []}})()

    def __getattr__(self, name):
        var = _Var()
        setattr(self, name, var)
        return var


def check(name, cond):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}")
        FAILURES.append(name)


def main():
    split = g._split_emotional_arc_entry
    expand = g._expand_emotional_arc
    inject = g._emotional_state_injection
    persona = g._persona_block_for_characters

    print("Arc entry parsing (_split_emotional_arc_entry):")
    check("flat state -> single stage", split("anxious") == ("anxious",))
    check("whitespace is stripped", split("  happy  ") == ("happy",))
    check("ascii arrow splits stages", split("neutral -> anxious") == ("neutral", "anxious"))
    check("multi-stage ascii", split("calm -> tense -> angry") == ("calm", "tense", "angry"))
    check("unicode arrow splits stages", split("calm \u2192 tense \u2192 angry") == ("calm", "tense", "angry"))
    check("mixed arrows normalise", split("a -> b \u2192 c") == ("a", "b", "c"))
    check("empty string -> empty tuple", split("") == ())
    check("None -> empty tuple", split(None) == ())
    check("lone arrow -> no usable stages", split("->") == ())
    check("trailing arrow drops blanks", split("a -> ") == ("a",))

    print("Arc expansion (_expand_emotional_arc):")
    check("flat stays constant", expand(("happy",), 3) == ["happy", "happy", "happy"])
    check("no stages -> empty", expand((), 3) == [])
    check("single-turn arc holds start", expand(("a", "b", "c"), 1) == ["a"])
    check("3 stages over 3 turns", expand(("calm", "tense", "angry"), 3) == ["calm", "tense", "angry"])
    check("always starts on first stage", expand(("s1", "s2", "s3", "s4"), 5)[0] == "s1")
    check("always ends on last stage", expand(("s1", "s2", "s3", "s4"), 5)[-1] == "s4")
    check("4 stages over 5 turns distribution",
          expand(("s1", "s2", "s3", "s4"), 5) == ["s1", "s2", "s3", "s3", "s4"])
    check("2 stages over 2 turns", expand(("a", "b"), 2) == ["a", "b"])
    check("4 stages over 4 turns", expand(("a", "b", "c", "d"), 4) == ["a", "b", "c", "d"])
    plan = expand(("neutral", "tense", "upset"), 6)
    check("long conversation still spans all stages", set(plan) == {"neutral", "tense", "upset"})
    check("plan length matches turn count", len(plan) == 6)

    print("Emotional-state injection text (_emotional_state_injection):")
    flat = inject("anxious")
    check("flat uses EMOTIONAL STATE header", "EMOTIONAL STATE: ANXIOUS" in flat)
    check("flat keeps its throughout instruction", "throughout your responses" in flat)
    check("flat with its one-stage tuple is the same text", inject("anxious", ("anxious",)) == flat)
    check("empty label -> empty text", inject("") == "")
    arc = split("calm -> wary -> tense")
    arc_txt = inject("wary", arc)
    check("arc uses PROGRESSION header", "PROGRESSION" in arc_txt)
    check("arc header lists every stage", "CALM -> WARY -> TENSE" in arc_txt)
    check("arc names the slot's stage, not the last one", "you are in the wary state" in arc_txt)
    check("arc drops the flat throughout instruction", "throughout your responses" not in arc_txt)
    check("arc tells model not to announce the shift", "announc" in arc_txt.lower())
    check("unicode-arrow arc gives the same text", inject("wary", split("calm → wary → tense")) == arc_txt)
    plan = expand(arc, 3)
    check("each slot of the worker's plan names its own stage",
          [f"you are in the {s} state" in inject(s, arc) for s in plan] == [True, True, True]
          and plan == ["calm", "wary", "tense"])

    print("Persona breakthrough block (_persona_block_for_characters):")
    check("no personas -> empty block", persona([]) == "")
    one = persona([(1, "a hidden grief")], char_count_label=False)
    check("block has header", "INNER PERSONA BREAKTHROUGH" in one)
    check("single char uses generic 'the character'", "the character's inner persona" in one)
    check("persona text is included", "a hidden grief" in one)
    two = persona([(1, "inner A"), (2, "inner B")], char_count_label=True)
    check("multi-char labels CHARACTER 1", "CHARACTER 1's inner persona" in two)
    check("multi-char labels CHARACTER 2", "CHARACTER 2's inner persona" in two)
    check("both persona texts present", "inner A" in two and "inner B" in two)
    gap = persona([(2, "inner B")], char_count_label=True)
    check("a persona keeps its profile number when an earlier one has none",
          "CHARACTER 2's inner persona" in gap and "CHARACTER 1" not in gap)

    print("Config editor save (ConfigEditor._get_current_editor_config_data):")
    import config_editor
    import yaml
    typed = "She's \"Mel\" <the clockmaker> \\ can't"
    fields = ['name', 'age', 'gender', 'race', 'job', 'eye_color', 'hair', 'hair_color', 'clothing', 'jewelry',
              'appearance', 'traits', 'backstory', 'persona', 'personality', 'setting', 'class', 'greeting']
    editor = _EditorStub(
        character_entries=[{k: _Var(typed) for k in fields}],
        emotional_states_text=_Var("calm -> wary -> tense\ncan't settle\n"),
        enable_setting_selection_var_editor=_Var(True),
        _sync_character_text_values=lambda: None,
        on_gender_change_editor_handler=lambda save_current=True: None,
        _profile_label_to_name=lambda label: label,
        global_config=_EditorStub(config={}),
    )
    saved = config_editor.ConfigEditor._get_current_editor_config_data(editor)['prompts']
    states = saved['emotional_states']['states']
    check("arc saved as typed", states[0] == "calm -> wary -> tense")
    check("saved arc still splits into stages", split(states[0]) == ("calm", "wary", "tense"))
    check("state with an apostrophe saved as typed", states[1] == "can't settle")
    profile = saved['character']['characters'][0]
    check("every profile field saved as typed", all(profile[k] == typed for k in fields))
    # Same dump arguments as ConfigEditor.save_config_handler
    reloaded = yaml.safe_load(yaml.dump(saved, sort_keys=False, indent=2, default_flow_style=False))
    check("typed text survives the YAML file",
          reloaded['emotional_states']['states'] == states and reloaded['character']['characters'][0] == profile)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        return 1
    print("All persona/emotional-arc checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
