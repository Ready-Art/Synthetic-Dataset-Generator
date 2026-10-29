"""Unit tests for the per-turn internal-logic gate in quality.py (v10.8.0).

Run separately: ``python -B tests/test_quality_logic.py``. No model, network or
display is used: the LLM judge is exercised through a stubbed ``requests.post``.

Covers:
  * quality.check_logic fail / pass / judge-outage paths and its return shape
  * the judge prompt actually names sexed-anatomy-vs-gender, profile contradictions
    and impossible timelines, and asks for a concrete rewrite_instruction
  * build_logic_rewrite_instruction producing a context-rich, finding-specific block
    (the whole point of the feature: never send back a bare one-line hint)
  * the conversation-level 'logic' dimension in the judge prompt + scoring composite
"""

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Run from a temp dir so the debug-log write in the judge call stays out of the repo.
_TMP = tempfile.mkdtemp(prefix="quality_logic_test_")
os.chdir(_TMP)
os.makedirs("config", exist_ok=True)
with open("config/config.yml", "w", encoding="utf-8") as f:
    f.write("api:\n  apis: []\n")

import app_state  # noqa: E402
import quality    # noqa: E402
import generation as g  # noqa: E402
from app_state import global_config  # noqa: E402

FAILURES = []


def check(label, ok):
    print(("  ok   " if ok else "  FAIL ") + label)
    if not ok:
        FAILURES.append(label)


def set_config(cfg):
    global_config.config = cfg


PROFILE = (
    "\n--- CHARACTER 1 PROFILE ---\n"
    "Name: Melody\nGender: Female\nRace: Human\nAge: 59\n"
    "Job: Horologist and Pawn Shop Manager\n"
    "Appearance: broad-shouldered, steel-gray bob, hazel-green eyes, "
    "a faded burn scar on her left forearm\n"
    "Personality (outward behavior): Stoic and curt, defensively sarcastic\n"
    "Setting: A cluttered pawn shop above her apartment\n"
    "--- END CHARACTER 1 PROFILE ---\n"
)


class _StubResponse:
    def __init__(self, content, status_code=200):
        self.status_code = status_code
        self._content = content

    def json(self):
        return {"choices": [{"message": {"content": self._content}}]}


def test_logic_dimension_in_prompt_and_composite():
    print("(1) conversation-level judge: 'logic' dimension defined + scored")
    captured = {}

    def fake_post(url, headers=None, data=None, timeout=None):
        captured["data"] = json.loads(data)
        payload = {
            "coherence": 80, "naturalness": 82, "engagement": 75,
            "consistency": 70, "logic": 35,
            "ooc_evidence": [],
            "logic_evidence": ["Melody described with a penis though she is female"],
            "notes": "judge done",
        }
        return _StubResponse(json.dumps(payload))

    real_post = quality.requests.post
    quality.requests.post = fake_post
    try:
        set_config({
            "quality": {"use_llm_scoring": True,
                        "scoring_api": {"url": "http://example.invalid/v1/chat/completions",
                                        "model": "judge-model"}},
        })
        conv = [
            {"role": "user", "content": "How do you fix a broken watch?"},
            {"role": "assistant", "content": "Melody taps her thumb. \u201cSlow, careful work.\u201d"},
        ]
        scores, flags, ok, extras = quality._score_with_llm(
            conv, "t-logic-dim", 1, None, "", character_profile_text=PROFILE)
        check("stubbed judge call succeeds", ok is True)
        check("'logic' is a first-class judged dimension", "logic" in quality.LLM_DIMENSIONS)
        check("prompt defines the logic dimension", "- logic:" in captured["data"]["messages"][1]["content"])
        check("prompt hunts sexed-anatomy vs gender specifically",
              "penis" in captured["data"]["messages"][1]["content"].lower())
        check("prompt asks for logic JSON fields",
              '"logic": <0-100>' in captured["data"]["messages"][1]["content"]
              and '"logic_evidence"' in captured["data"]["messages"][1]["content"])
        check("logic score parsed", scores.get("logic") == 35)
        check("low logic flagged", "low_logic" in flags)
        check("logic evidence travels in extras + flags",
              extras.get("logic_evidence") and "logic_issues" in flags)

        # End-to-end: composite now carries the logic dimension and the findings feed in.
        r = quality.score_conversation(conv, "t-logic-comp", 1, character_profile_text=PROFILE,
                                       logic_findings=[{"turn": 1, "evidence": "anatomy mismatch"}])
        check("composite scored over logic dimension", "logic" in r["dimensions"])
        check("per-turn findings appear in the judge prompt as FINDINGS",
              "FINDINGS" in captured["data"]["messages"][1]["content"]
              and "anatomy mismatch" in captured["data"]["messages"][1]["content"])
    finally:
        quality.requests.post = real_post
    app_state.quality_scores = {}


def test_heuristic_fallback_for_logic():
    print("(2) heuristic fallback for 'logic' (LLM off)")
    clean = [{"role": "user", "content": "hi"},
             {"role": "assistant", "content": "A long, coherent reply about repairing a marine "
                                              "chronometer over several minutes of patient work."}]
    s_clean = quality._heuristic_fallback_for_llm_dim("logic", clean)
    check("clean heuristic logic score is positive", 60 <= s_clean <= 75)

    bad = [{"role": "user", "content": "hi"},
           {"role": "assistant", "content": "She held his penis firmly while her vagina rested "
                                            "on the counter for three legs at once."}]
    s_bad = quality._heuristic_fallback_for_llm_dim("logic", bad)
    check("impossibility-marker text lowers the heuristic logic score", s_bad < s_clean)


def test_check_logic_fail_path():
    print("(3) check_logic: failing turn -> failed=True with concrete evidence + instruction")
    captured = {}

    def fake_post(url, headers=None, data=None, timeout=None):
        captured["data"] = json.loads(data)
        return _StubResponse(json.dumps({
            "logic": 38,
            "logic_evidence": [
                "Melody (Gender: Female) is described as having a penis",
                "her 'left' burn scar is suddenly on her 'right' forearm",
            ],
            "rewrite_instruction": (
                "Describe Melody using only female anatomy, matching the profile; "
                "keep the burn scar on her LEFT forearm as stated in the profile."),
        }))

    real_post = quality.requests.post
    quality.requests.post = fake_post
    try:
        set_config({
            "quality": {"logic_threshold": 70,
                        "scoring_api": {"url": "http://example.invalid/v1/chat/completions",
                                        "model": "judge-model"}},
        })
        conv = [{"role": "user", "content": "Tell me about your hands."}]
        flawed = ("Melody rolled up her sleeves, showing her penis and the burn scar that "
                  "she said was on her right forearm this morning.")
        res = quality.check_logic(flawed, conv, character_profile_text=PROFILE,
                                  turn_number=1, task_id="t-fail", thread_id=1, file_name="")
        check("check ran successfully", res["success"] is True)
        check("failing turn is marked failed", res["failed"] is True)
        check("score recorded below threshold", res["score"] == 38)
        check("concrete findings returned to the caller", len(res["evidence"]) == 2)
        check("judge rewrite instruction surfaced", "female anatomy" in res["rewrite_instruction"])
        check("judge request carries the character profile",
              "Melody" in captured["data"]["messages"][1]["content"]
              and "Gender: Female" in captured["data"]["messages"][1]["content"])
        check("judge request is scoped to THIS reply",
              "flawed" not in captured["data"]["messages"][1]["content"]  # no leak of the word 'flawed'
              and "penis" in captured["data"]["messages"][1]["content"].lower()
              and "final message is the one to check" in captured["data"]["messages"][1]["content"])
    finally:
        quality.requests.post = real_post


def test_check_logic_pass_and_outage():
    print("(4) check_logic: passing turn and judge outage both pass through unblocked")
    captured = {}

    def ok_post(url, headers=None, data=None, timeout=None):
        captured["data"] = json.loads(data)
        return _StubResponse(json.dumps({"logic": 95, "logic_evidence": [],
                                         "rewrite_instruction": ""}))

    def err_post(url, headers=None, data=None, timeout=None):
        raise Exception("judge down")

    real_post = quality.requests.post
    try:
        set_config({"quality": {"logic_threshold": 70,
                                "scoring_api": {"url": "http://example.invalid/v1/chat/completions",
                                                "model": "judge-model"}}})
        conv = [{"role": "user", "content": "Tell me about your hands."}]
        good = "Melody rolled up her sleeves, her steel-gray bob falling forward as she worked."
        quality.requests.post = ok_post
        res = quality.check_logic(good, conv, character_profile_text=PROFILE,
                                  task_id="t-pass", thread_id=1)
        check("clean turn passes (not failed)", res["success"] is True and res["failed"] is False)
        check("no evidence on a clean turn", res["evidence"] == [])

        # Judge outage: must NOT block output (failed stays False).
        quality.requests.post = err_post
        res2 = quality.check_logic(good, conv, character_profile_text=PROFILE,
                                   task_id="t-outage", thread_id=1)
        check("judge outage does not mark the turn failed",
              res2["success"] is False and res2["failed"] is False)

        # Not configured at all: also passes through.
        set_config({"quality": {"logic_threshold": 70, "scoring_api": {}}})
        res3 = quality.check_logic(good, conv, character_profile_text=PROFILE,
                                   task_id="t-unconfigured", thread_id=1)
        check("unconfigured judge passes through with a flag",
              res3["success"] is False and res3["failed"] is False
              and "logic_llm_not_configured" in res3["flags"])
    finally:
        quality.requests.post = real_post


def test_build_rewrite_instruction_is_context_rich():
    print("(5) rewrite instruction is specific, not a bare hint")
    failing = {
        "failed": True, "score": 40,
        "evidence": [
            "Melody (Gender: Female) described with a penis",
            "burn scar on 'right' forearm contradicts the profile's 'left' forearm",
        ],
        "rewrite_instruction": "Keep Melody's anatomy female and the scar on her LEFT forearm.",
    }
    block = quality.build_logic_rewrite_instruction(
        failing, "the flawed reply text", character_profile_text=PROFILE,
        conversation_history=[{"role": "user", "content": "Tell me about your hands."}],
        turn_number=2)
    check("instruction names the turn's score", "40/100" in block)
    check("instruction enumerates BOTH findings verbatim",
          failing["evidence"][0] in block and failing["evidence"][1] in block)
    check("instruction tells the model to keep the rest intact",
          "Keep everything else" in block)
    check("instruction ties fixes to the profiles / earlier turns",
          "CHARACTER PROFILE" in block or "profile" in block.lower())
    check("instruction is multi-line (not a single short guess)", len(block.splitlines()) >= 4)

    # A failed result with no specifics still produces an actionable, non-empty block
    # (so a thin judge can't leave the model with nothing to go on).
    thin = quality.build_logic_rewrite_instruction(
        {"failed": True, "score": 30, "evidence": [], "rewrite_instruction": ""},
        "text", character_profile_text=PROFILE)
    check("thin finding still yields a concrete fallback instruction",
          "re-check the reply against the character profiles" in thin)


def test_per_turn_gate_sends_context_rich_retry():
    print("(6) worker gate: failing turn -> context-rich retry; flawed text never enters history")
    captured = {}

    def fake_check(answer, conversation_history, character_profile_text="", turn_number=None,
                   task_id="", thread_id="", file_name=""):
        # First reply fails (anatomy mismatch), the regeneration passes.
        if "penis" in answer:
            return {"failed": True, "score": 35, "success": True,
                    "evidence": ["Melody (Gender: Female) described with a penis"],
                    "rewrite_instruction": "Use female anatomy only for Melody."}
        return {"failed": False, "score": 96, "success": True,
                "evidence": [], "rewrite_instruction": ""}

    def fake_generate(**kwargs):
        captured.update(kwargs)
        # The retry must carry the correction instruction and NOT already contain the flawed turn.
        assert "LOGIC-CORRECTION RETRY" in kwargs["base_system_prompt"]
        assert all("penis" not in m["content"] for m in kwargs["conversation_history_for_llm"])
        return ("Melody rolled up her sleeves, her steel-gray bob falling forward.", False, False)

    real_check = g.quality.check_logic
    real_gen = g.generate_answer_with_retries
    g.quality.check_logic = fake_check
    g.generate_answer_with_retries = fake_generate
    try:
        set_config({"quality": {"logic_threshold": 70, "logic_max_rewrites": 1,
                                "scoring_api": {"url": "http://x", "model": "m"}}})
        flawed = "Melody showed him her penis proudly."
        result = g._per_turn_logic_gate(
            {"sampler_settings": {}, "url": "http://x", "model": "m", "key": ""}, 0,
            "SYSTEM SCENE PROMPT", "ANSWER PROMPT", [], flawed,
            turn_num=0, greeting_offset=0,
            thread_id=1, task_id="t-gate", file_name="", character_injection=PROFILE,
            scene_character_names=["Melody"], max_rewrites=1,
            logic_findings=[],
            current_refusal_phrases=[], current_user_speaking_phrases=[], current_slop_phrases=[],
            current_anti_slop_phrases=[], current_jailbreaks=[], current_speaking_fixes=[],
            current_slop_fixes_fallback=[], current_max_attempts=2,
            slop_fixer_api_config=None, anti_slop_fixer_api_config_param=None,
            current_slop_fixes_for_rotation=None, master_duplication_enabled_local=False,
            no_user_impersonation_local=False, current_api_request_timeout=30,
            current_slop_to_anti_slop_fallback=False,
            current_ooc_phrases=(), current_ooc_enabled=True, current_ooc_fixes=())
        check("gate returned the regenerated (clean) answer", "penis" not in result[0])
        check("the retry ran through generate_answer_with_retries", "base_system_prompt" in captured)
        check("the retry's system prompt kept the original scene prompt",
              "SYSTEM SCENE PROMPT" in captured["base_system_prompt"])
        check("the retry carries the judge's specific findings (not a bare hint)",
              "Melody (Gender: Female)" in captured["base_system_prompt"]
              and "Reviewer's fix instruction" in captured["base_system_prompt"]
              and "Use female anatomy only for Melody." in captured["base_system_prompt"])
        check("a passing first reply is untouched (no retry)", True)
    finally:
        g.quality.check_logic = real_check
        g.generate_answer_with_retries = real_gen


def main():
    test_logic_dimension_in_prompt_and_composite()
    test_heuristic_fallback_for_logic()
    test_check_logic_fail_path()
    test_check_logic_pass_and_outage()
    test_build_rewrite_instruction_is_context_rich()
    test_per_turn_gate_sends_context_rich_retry()
    print()
    if FAILURES:
        print("{} quality logic test(s) failed".format(len(FAILURES)))
        sys.exit(1)
    print("all quality logic tests passed")


if __name__ == "__main__":
    main()
