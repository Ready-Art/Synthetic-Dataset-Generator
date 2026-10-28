"""Unit tests for the opt-in roleplay quality dimensions in quality.py.

Run separately: ``python -B test_quality_roleplay.py``. No model, network or display
is used: heuristics run directly, and the LLM-judge path is exercised through a
stubbed ``requests.post``. Covers:

  * the quality.roleplay_dimensions toggle (off by default; active sets/weights)
  * voice_distinctiveness (n-gram overlap between characters' spoken lines)
  * grounding (profile-keyword coverage)
  * persona_alignment (outward vs inner persona timing)
  * LLM-judge prompt construction + refinement when the toggle is on
  * composite/weight normalization and dashboard stat keys following the toggle
"""

import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Run from a temp dir so the debug-log write in _score_with_llm stays out of the repo.
_TMP = tempfile.mkdtemp(prefix="quality_test_")
os.chdir(_TMP)
os.makedirs("config", exist_ok=True)
with open("config/config.yml", "w", encoding="utf-8") as f:
    f.write("api:\n  apis: []\n")

import app_state
import quality
from app_state import global_config

FAILURES = []


def check(label, ok):
    print(("  ok   " if ok else "  FAIL ") + label)
    if not ok:
        FAILURES.append(label)


def set_config(cfg):
    global_config.config = cfg


ALICE_BOB_PROFILE = (
    "\n--- CHARACTER 1 PROFILE ---\n"
    "Name: Alice\nGender: Female\nRace: Human\nAge: 58\nJob: Lighthouse keeper\n"
    "Appearance: Weathered hands, sun-bleached coat\n"
    "Backstory: Guarded the Dunmere lighthouse for forty years through every gale\n"
    "Personality (outward behavior): Curt, blunt, unsentimental\n"
    "Persona: Secretly tender, keeps her dead husband's journal\n"
    "Setting: The black cliffs of Dunmere lighthouse\n"
    "--- END CHARACTER 1 PROFILE ---\n"
    "--- CHARACTER 2 PROFILE ---\n"
    "Name: Bob\nGender: Male\nRace: Human\nAge: 30\nJob: Fisherman\n"
    "Appearance: Broad shoulders, tarred coat\n"
    "Backstory: Raised on the gulf fishing boats since he was ten\n"
    "Personality (outward behavior): Easygoing, cracks jokes under stress\n"
    "Setting: The black cliffs of Dunmere lighthouse\n"
    "--- END CHARACTER 2 PROFILE ---\n"
)

DISTINCT_CONV = [
    {"role": "user", "content": "What do you see out there?"},
    {"role": "assistant", "content": (
        "Alice said, \u201cForty years of gales have burned every scrap of softness out of me. "
        "The brine wind never stopped, and neither did I.\u201d Bob replied, \u201cHey, easy on the "
        "doom and gloom, mate! You ever try the foghorn solo at dusk? Proper spooky tune, I swear!\u201d")},
    {"role": "user", "content": "go on"},
    {"role": "assistant", "content": (
        "Alice: \u201cThe storm breaks by dawn. I have faced worse on these cliffs.\u201d "
        "Bob: \u201cAye, and we'll still be here whistling sea shanties over a cup of grog, "
        "bet yer bottom dollars on it!\u201d")},
]

SAME_VOICE_CONV = [
    {"role": "user", "content": "a"},
    {"role": "assistant", "content": (
        "Alice: \u201cThe rain falls hard tonight and we stand together in the cold and wait for dawn.\u201d "
        "Bob: \u201cThe rain falls hard tonight and we stand together in the cold and wait for dawn.\u201d")},
    {"role": "user", "content": "b"},
    {"role": "assistant", "content": (
        "Alice: \u201cThe rain falls harder still and we cannot escape the cold anywhere at all.\u201d "
        "Bob: \u201cThe rain falls harder still and we cannot escape the cold anywhere at all.\u201d")},
]


def test_toggle_off_is_default():
    print("(1) toggle off (default)")
    set_config({"quality": {"enabled": True}})
    check("is_roleplay_scoring_enabled defaults off", quality.is_roleplay_scoring_enabled() is False)
    dims = quality.active_scoring_dimensions()
    check("active set is the six generic dimensions", list(dims) == list(quality.SCORING_DIMENSIONS))
    r = quality.score_conversation(DISTINCT_CONV, "t-off", 1,
                                   character_profile_text=ALICE_BOB_PROFILE,
                                   scene_characters=["Alice", "Bob"])
    check("no roleplay keys in scored dimensions",
          not any(k in r["dimensions"] for k in quality.ROLEPLAY_SCORING_DIMENSIONS))
    stats = quality.get_quality_stats()
    check("stat averages carry no roleplay keys",
          not any(k in stats["dimension_averages"] for k in quality.ROLEPLAY_SCORING_DIMENSIONS))


def test_toggle_on_scores_three_more():
    print("(2) toggle on: composite + stats follow")
    app_state.quality_scores = {}
    set_config({"quality": {"enabled": True, "roleplay_dimensions": True},
                "prompts": {"character": {"persona_breakthrough_turn": 2}}})
    check("is_roleplay_scoring_enabled true from config", quality.is_roleplay_scoring_enabled() is True)
    dims = quality.active_scoring_dimensions()
    check("active set is six generic + three roleplay",
          list(dims) == list(quality.SCORING_DIMENSIONS) + list(quality.ROLEPLAY_SCORING_DIMENSIONS))
    # The composite normalizes by the total active weight, so the defaults need not sum
    # to exactly 1 (six generic dims sum to 1.0; adding the three roleplay dims gives 1.3).
    check("roleplay weights present for all three dims",
          all(dims[d] == 0.10 for d in quality.ROLEPLAY_SCORING_DIMENSIONS))

    r = quality.score_conversation(DISTINCT_CONV, "t-on", 1,
                                   character_profile_text=ALICE_BOB_PROFILE,
                                   scene_characters=["Alice", "Bob"])
    check("all nine dimensions present", len(r["dimensions"]) == 9)
    check("roleplay dimensions scored 0-100",
          all(0 <= r["dimensions"][d] <= 100 for d in quality.ROLEPLAY_SCORING_DIMENSIONS))
    # Weight check: recompute the composite from the reported dimensions and weights.
    expected = sum(r["dimensions"][d] * dims[d] / sum(dims.values()) for d in dims)
    check("composite is the weighted average of active dims", abs(expected - r["composite"]) < 0.1)
    check("method remains heuristic_only without LLM", r["method"] == "heuristic_only")

    stats = quality.get_quality_stats()
    check("stat averages now carry the roleplay keys",
          all(k in stats["dimension_averages"] for k in quality.ROLEPLAY_SCORING_DIMENSIONS))

    # Off again after: stats shrink back and scores stay keyed to the active set.
    set_config({"quality": {"enabled": True}})
    stats2 = quality.get_quality_stats()
    check("stats shrink back when toggled off again",
          not any(k in stats2["dimension_averages"] for k in quality.ROLEPLAY_SCORING_DIMENSIONS))
    app_state.quality_scores = {}


def test_voice_distinctiveness():
    print("(3) voice_distinctiveness heuristic")
    # Line attribution: 'X said, "...' (proximity) and 'X: "...' (speaker prefix) both
    # attribute each quoted line to the closest named character.
    lines, ratio = quality._spoken_lines_by_character(
        [{"role": "assistant",
          "content": "Alice said, \u201cThe storm breaks by dawn.\u201d Bob replied, \u201cWe will still be here whistling at dusk.\u201d"}],
        ["Alice", "Bob"])
    check("proximity attribution splits a two-speaker turn correctly",
          ratio == 1.0 and lines["Alice"] and lines["Bob"])
    lines2, _ = quality._spoken_lines_by_character(
        [{"role": "assistant",
          "content": "Alice: \u201cThe storm breaks by dawn.\u201d Bob: \u201cWe will still be here whistling at dusk.\u201d"}],
        ["Alice", "Bob"])
    check("speaker-prefix attribution splits a two-speaker turn correctly",
          lines2["Alice"] and lines2["Bob"])
    s_high, f_high = quality._score_voice_distinctiveness(DISTINCT_CONV, ["Alice", "Bob"])
    s_low, f_low = quality._score_voice_distinctiveness(SAME_VOICE_CONV, ["Alice", "Bob"])
    check("distinct voices score higher than identical voices", s_high > s_low)
    check("identical voices flag interchangeability",
          "interchangeable_character_voices" in f_low or "similar_character_voices" in f_low)
    # Single character: neutral, with the insufficiency flag rather than a guess.
    s_one, f_one = quality._score_voice_distinctiveness(
        [{"role": "user", "content": "x"},
         {"role": "assistant", "content": "Alice: \u201cI keep the light burning through every gale on these cliffs.\u201d"}],
        ["Alice"])
    check("single-character scene is neutral and flagged",
          50 <= s_one <= 75 and any("character" in fl for fl in f_one))
    # Empty conversation: no crash.
    s_empty, _ = quality._score_voice_distinctiveness([], ["Alice", "Bob"])
    check("empty conversation is safe", 0 <= s_empty <= 100)


def test_grounding():
    print("(4) grounding heuristic")
    grounded, fg = quality._score_grounding(DISTINCT_CONV, ALICE_BOB_PROFILE)
    ungrounded_conv = [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "That is really interesting. The weather seems nice today outside."},
    ]
    filler, ff = quality._score_grounding(ungrounded_conv, ALICE_BOB_PROFILE)
    check("grounded scene beats generic filler", grounded > filler)
    check("generic filler is flagged", any("filler" in fl or "ungrounded" in fl for fl in ff))
    noprof, fnf = quality._score_grounding(DISTINCT_CONV, "")
    check("no profile is neutral with a flag", noprof == 70 and "no_profile_for_grounding" in fnf)


def test_persona_alignment():
    print("(5) persona_alignment heuristic")
    set_config({"quality": {"roleplay_dimensions": True},
                "prompts": {"character": {"persona_breakthrough_turn": 2}}})
    # Breakthrough happens: late turn carries the inner-persona keyword 'journal'.
    aligned = [
        {"role": "user", "content": "x"},
        {"role": "assistant", "content": "Alice said curtly, \u201cGo on, the light needs trimming.\u201d"},
        {"role": "user", "content": "y"},
        {"role": "assistant", "content": "Alice finally opens her coat and pulls out the old journal. \u201cThis belonged to Thomas.\u201d"},
    ]
    s_aligned, _ = quality._score_persona_alignment(aligned, ALICE_BOB_PROFILE, ["Alice", "Bob"])
    # No breakthrough: nothing surfaces after the withheld slot.
    s_never, f_never = quality._score_persona_alignment(DISTINCT_CONV, ALICE_BOB_PROFILE, ["Alice", "Bob"])
    check("persona surfacing after breakthrough scores well", s_aligned >= 80)
    check("persona never surfacing is penalised", s_never <= s_aligned and "no_inner_persona_signals" in f_never)
    # Early leak needs an actual outward phase before the breakthrough slot, so use
    # breakthrough turn 3 with a neutral first reply followed by the inner-persona line.
    set_config({"quality": {"roleplay_dimensions": True},
                "prompts": {"character": {"persona_breakthrough_turn": 3}}})
    leaked = [
        {"role": "user", "content": "x"},
        {"role": "assistant", "content": "Alice trims the lamp. \u201cThe wick is worn thin again.\u201d"},
        {"role": "user", "content": "y"},
        {"role": "assistant", "content": "Alice clutches the old journal to her chest. \u201cThomas would not forgive me if the light went out.\u201d"},
    ]
    s_leak, f_leak = quality._score_persona_alignment(leaked, ALICE_BOB_PROFILE, ["Alice", "Bob"])
    check("early inner-persona talk is flagged and penalised",
          "early_inner_persona_leak" in f_leak and s_leak <= 75)
    set_config({"quality": {"roleplay_dimensions": True},
                "prompts": {"character": {"persona_breakthrough_turn": 2}}})
    # No persona fields configured: degrades to the emotional-range fallback.
    no_persona_profile = (
        "\n--- CHARACTER 1 PROFILE ---\nName: Eve\nBackstory: A quiet ferry captain of the gulf.\n"
        "Personality (outward behavior): Blunt\n--- END CHARACTER 1 PROFILE ---\n"
    )
    s_fb, f_fb = quality._score_persona_alignment(aligned, no_persona_profile, ["Eve"])
    check("no inner persona configured -> neutral fallback flag",
          s_fb <= 70 and "no_inner_persona_to_align" in f_fb)


class _StubResponse:
    def __init__(self, content):
        self.status_code = 200
        self._content = content

    def json(self):
        return {"choices": [{"message": {"content": self._content}}]}


def test_llm_judge_prompt_and_refinement():
    print("(6) LLM judge: prompt content + refinement when toggle on")
    captured = {}

    def fake_post(url, headers=None, data=None, timeout=None):
        captured["data"] = json.loads(data)
        payload = {
            "coherence": 80, "naturalness": 82, "engagement": 75, "consistency": 70,
            "voice_distinctiveness": 35, "grounding": 90, "persona_alignment": 88,
            "ooc_evidence": [], "notes": "judge done",
        }
        return _StubResponse(json.dumps(payload))

    real_post = quality.requests.post
    quality.requests.post = fake_post
    try:
        set_config({"quality": {"use_llm_scoring": True, "roleplay_dimensions": True,
                                "scoring_api": {"url": "http://example.invalid/v1/chat/completions",
                                                 "model": "judge-model"}}})
        conv = [dict(t) for t in DISTINCT_CONV]
        scores, flags, ok = quality._score_with_llm(conv, "t-judge", 1, None, "",
                                                    character_profile_text=ALICE_BOB_PROFILE)
        check("stubbed judge call succeeds", ok is True)
        check("judge returns all seven dimensions", set(scores) == set(
            quality.LLM_DIMENSIONS | quality.ROLEPLAY_LLM_DIMENSIONS))
        check("low judge score is flagged", "low_voice_distinctiveness" in flags)

        prompt = captured["data"]["messages"][1]["content"]
        check("prompt includes roleplay dimension definitions",
              "voice_distinctiveness:" in prompt and "grounding:" in prompt and "persona_alignment:" in prompt)
        check("prompt asks for the extra JSON fields",
              '"voice_distinctiveness": <0-100>' in prompt and '"persona_alignment": <0-100>' in prompt)
        check("max_tokens headroom grows with roleplay dims",
              captured["data"]["max_tokens"] >= 384 + 3 * 64)

        # Refinement end-to-end: score_conversation must use judge values over the heuristics.
        r = quality.score_conversation(DISTINCT_CONV, "t-judge2", 1,
                                       character_profile_text=ALICE_BOB_PROFILE,
                                       scene_characters=["Alice", "Bob"])
        check("hybrid method when judge succeeds", r["method"] == "hybrid")
        check("judge values override roleplay heuristics",
              r["dimensions"]["voice_distinctiveness"] == 35
              and r["dimensions"]["grounding"] == 90
              and r["dimensions"]["persona_alignment"] == 88)
        check("low_voice_distinctiveness carried into flags", "low_voice_distinctiveness" in r["flags"])

        # Toggle off: prompt has no roleplay section and judge scores only the four core dims.
        set_config({"quality": {"use_llm_scoring": True,
                                "scoring_api": {"url": "http://example.invalid/v1/chat/completions",
                                                 "model": "judge-model"}}})
        scores2, _, ok2 = quality._score_with_llm(conv, "t-judge3", 1, None, "",
                                                  character_profile_text=ALICE_BOB_PROFILE)
        prompt2 = captured["data"]["messages"][1]["content"]
        check("toggle off: no roleplay dims in prompt", "voice_distinctiveness" not in prompt2)
        check("toggle off: judge scores the four core dims only",
              ok2 and set(scores2) == set(quality.LLM_DIMENSIONS))
        check("toggle off: default max_tokens", captured["data"]["max_tokens"] == 384)
    finally:
        quality.requests.post = real_post
    app_state.quality_scores = {}


def main():
    test_toggle_off_is_default()
    test_toggle_on_scores_three_more()
    test_voice_distinctiveness()
    test_grounding()
    test_persona_alignment()
    test_llm_judge_prompt_and_refinement()
    print()
    if FAILURES:
        print("{} quality roleplay test(s) failed".format(len(FAILURES)))
        sys.exit(1)
    print("all quality roleplay tests passed")


if __name__ == "__main__":
    main()
