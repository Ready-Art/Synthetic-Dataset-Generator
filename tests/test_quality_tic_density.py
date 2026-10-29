"""Unit tests for the opt-in tic_density quality dimension (v11.7, plan.md 3.3).

Run separately: ``python -B tests/test_quality_tic_density.py``. No model, network or
display is used: the heuristic runs directly against fixture conversations, and the
toggle behaviour is exercised through ``global_config`` exactly like
``tests/test_quality_roleplay.py``.

Covers:
  * the quality.tic_dimension toggle (off by default; active set/weights when on)
  * registration: HEURISTIC_DIMENSIONS yes, LLM_DIMENSIONS no (real measurement,
    no judge fabrication)
  * a clean conversation scores high with no tic flags
  * denial-correction / negation-run / comma-stacking / stacked-qualifier /
    warm-closer / shallow-narration fixtures each carry their flag and score lower
  * empty input returns 0 with no_assistant_text; the score is clamped to 0..100
  * composite/weight normalisation follows the toggle; dashboard stats do too
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Run from a temp dir so any debug-log write stays out of the repo.
_TMP = tempfile.mkdtemp(prefix="quality_tic_test_")
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


def u(text):
    return {"role": "user", "content": text}


def a(text):
    return {"role": "assistant", "content": text}


# --- Fixtures ------------------------------------------------------------------------------

# Natural prose: interiority present, commas sparse, no tics. Expect a clean 100.
CLEAN_CONV = [
    u("The crossing is late again."),
    a("She checks the mooring lines and ties the last of them off. The rain steadies into "
      "something bearable. She remembers the first winter she kept this landing, and how the "
      "\u201cYou\u2019ll make it across,\u201d she says."),
    u("Are we going tonight?"),
    a("She looks out at the water and thinks about the fuel, the weather, the hour. "
      "\u201cWhen the lamp on the headland comes back on,\u201d she says. "
      "\u201cWe\u2019ll be ready by then.\u201d"),
]

# Denial-then-correction couplets in every assistant turn (the scotoma signature tic).
DENIAL_CONV = [
    u("what happened"),
    a("It wasn't anger. It was fear. The lights came back on one by one along the quay."),
    u("go on"),
    a("He didn't run. He walked, slow and steady, toward the gate."),
]

# Consecutive negation runs in narration -- NOT denial-corrections (asserted in
# tests/test_style_metrics.py: the correction half there is itself negated).
RUNS_CONV = [
    u("tell me"),
    a("She didn't move. She didn't speak. The room stayed exactly as it was."),
    u("and then?"),
    a("They didn't come. They didn't call. They didn't write."),
]

# Comma clause stacking: every sentence carries 3+ commas.
COMMA_CONV = [
    u("describe the harbor"),
    a("The harbor, dark, wet, and crowded with gulls, went quiet at last. "
      "A boat, small, red, and leaning hard to starboard, rocked against its moorings."),
    u("more"),
    a("The street, empty, grey, and humming with cold, stretched past the chandlery. "
      "A dog, thin, alert, and utterly alone, watched the water from the breakwater."),
]

# Formulaic warm closers: the final sentence of each turn is a warm/significance closer
# while the body carries none (clamped lift >= 3 trips the band).
WARM_CLOSER_CONV = [
    u("end it gently"),
    # Body carries no warm/significance words, so the final closer lifts to the clamp.
    a("She finished the mending work and set the lamp down. The boat rocked against the "
      "mooring while the night pressed around the little landing. Everything settled "
      "into soft, quiet warmth."),
]

# Stacked comma-appended absolutes (qualification stacks) plus the comma stacking they drag
# in; expect the qualification_stack flag and a lowered score.
STACKED_CONV = [
    u("set the scene"),
    a("She stood at the rail, her hands white, her knuckles pale, her breath fogging in "
      "the dark. The engine coughed, its valves loose, its oil low, its timer broken."),
]

# Shallow narration: nearly all of the turn wrapped in asterisks with no interiority signal.
SHALLOW_NARRATION_CONV = [
    u("keep moving"),
    a("*She pushes through the door and crosses the rain-soaked yard to the dock. The ferry "
      "horn sounds twice in the dark.*"),
]

# Everything at once: must clamp at 0, never go negative.
KITCHEN_SINK_CONV = [
    u("one"),
    a("It wasn't calm. It was dread. She didn't move. She didn't breathe. "
      "She stood at the rail, her hands white, her knuckles pale, her breath fogging in "
      "the dark, and nothing, nothing else, seemed possible anymore. Everything settled "
      "into soft, quiet warmth."),
    u("two"),
    a("He didn't come. He didn't call. He didn't write. It wasn't refusal. It was distance. "
      "He leaned on the gate, his jaw tight, his fingers cold, his shadow long, and waited "
      "for the rain to end."),
]


def test_toggle_off_is_default():
    print("(1) toggle off (default)")
    set_config({"quality": {"enabled": True}})
    check("is_tic_scoring_enabled defaults off", quality.is_tic_scoring_enabled() is False)
    check("tic_density is not a base dimension (opt-in, like the roleplay set)",
          "tic_density" not in quality.SCORING_DIMENSIONS
          and "tic_density" in quality.TIC_SCORING_DIMENSIONS
          and quality.TIC_SCORING_DIMENSIONS["tic_density"] == 0.10)
    dims = quality.active_scoring_dimensions()
    check("active set has no tic_density while the toggle is off", "tic_density" not in dims)
    r = quality.score_conversation(CLEAN_CONV, "t-off", 1)
    check("no tic_density key in scored dimensions while off",
          "tic_density" not in r["dimensions"])
    stats = quality.get_quality_stats()
    check("stat averages carry no tic_density key while off",
          "tic_density" not in stats["dimension_averages"])


def test_registration_is_heuristic_only():
    print("(2) registration: measured, never judge-fabricated")
    check("tic_density is in HEURISTIC_DIMENSIONS", "tic_density" in quality.HEURISTIC_DIMENSIONS)
    check("tic_density is NOT in LLM_DIMENSIONS (no judge fallback number)",
          "tic_density" not in quality.LLM_DIMENSIONS)


def test_clean_conversation_scores_high():
    print("(3) toggle on: clean prose scores high with no tic flags")
    set_config({"quality": {"enabled": True, "tic_dimension": True}})
    dims = quality.active_scoring_dimensions()
    check("active set is the base dimensions + tic_density",
          list(dims) == list(quality.SCORING_DIMENSIONS) + ["tic_density"])
    check("tic weight is 0.10 in the active set", dims["tic_density"] == 0.10)

    score, flags = quality._score_tic_density(CLEAN_CONV)
    check("clean conversation: no tic flags",
          not any(f in flags for f in (
              "denial_correction_tic", "negation_runs", "comma_clause_stacking",
              "qualification_stack", "formulaic_warm_closer", "shallow_narration")))
    check("clean conversation: heuristic scores high", score >= 90)

    r = quality.score_conversation(CLEAN_CONV, "t-on", 1)
    check("tic_density present in scored dimensions when the toggle is on",
          "tic_density" in r["dimensions"])
    check("method remains heuristic_only without LLM", r["method"] == "heuristic_only")
    expected = sum(r["dimensions"][d] * dims[d] / sum(dims.values()) for d in dims)
    check("composite is the weighted average of the active dims", abs(expected - r["composite"]) < 0.1)

    stats = quality.get_quality_stats()
    check("stat averages now carry the tic_density key",
          "tic_density" in stats["dimension_averages"])

    # Off again: the set shrinks back and the composite reverts to base-only weights.
    set_config({"quality": {"enabled": True}})
    r2 = quality.score_conversation(CLEAN_CONV, "t-on2", 1)
    check("toggling off removes the dimension and restores the base composite",
          "tic_density" not in r2["dimensions"] and r2["composite"] == r_base_composite(CLEAN_CONV))


def r_base_composite(conv):
    """The composite the pre-v11.7 base set would produce for this conversation."""
    base_dims = dict(quality.SCORING_DIMENSIONS)
    total = sum(base_dims.values()) or 1.0
    result = quality.score_conversation(conv, "t-base", 1)
    return round(sum(result["dimensions"].get(d, 0) * w / total
                     for d, w in base_dims.items()), 1)


def test_denial_correction_fixture():
    print("(4) denial-correction couplets are flagged and penalised")
    set_config({"quality": {"enabled": True, "tic_dimension": True}})
    score, flags = quality._score_tic_density(DENIAL_CONV)
    check("denial_correction_tic flag present", "denial_correction_tic" in flags)
    check("no negation-run flag (a couplet is not a run)", "negation_runs" not in flags)
    check("score is penalised but not a hard drop", 0 <= score < 100)
    clean_score, _ = quality._score_tic_density(CLEAN_CONV)
    check("stuffed conversation scores below the clean one", score < clean_score)


def test_negation_runs_fixture():
    print("(5) negation runs are flagged, and NOT misread as denial-corrections")
    score, flags = quality._score_tic_density(RUNS_CONV)
    check("negation_runs flag present", "negation_runs" in flags)
    check("denial_correction_tic absent for pure runs", "denial_correction_tic" not in flags)
    check("runs fixture is penalised", score < 100)


def test_comma_stacking_fixture():
    print("(6) comma clause stacking is flagged")
    score, flags = quality._score_tic_density(COMMA_CONV)
    check("comma_clause_stacking flag present", "comma_clause_stacking" in flags)
    check("staying below the other tics, the comma band alone deducts", 100 - score >= 10)


def test_warm_closer_fixture():
    print("(7) formulaic warm closers are flagged")
    score, flags = quality._score_tic_density(WARM_CLOSER_CONV)
    check("formulaic_warm_closer flag present", "formulaic_warm_closer" in flags)
    check("warm-closer deduction applied", 100 - score >= 10)


def test_stacked_qualifiers_fixture():
    print("(8) stacked absolute trailers are flagged")
    score, flags = quality._score_tic_density(STACKED_CONV)
    check("qualification_stack flag present", "qualification_stack" in flags)
    check("stacked qualifiers deduct", 100 - score >= 15)


def test_shallow_narration_fixture():
    print("(9) shallow (interiority-free) narration is flagged")
    score, flags = quality._score_tic_density(SHALLOW_NARRATION_CONV)
    check("shallow_narration flag present", "shallow_narration" in flags)
    check("shallow narration deducts", 100 - score >= 10)


def test_empty_and_clamped():
    print("(10) empty input and clamping")
    score, flags = quality._score_tic_density([])
    check("empty conversation returns 0 with no_assistant_text",
          score == 0 and flags == ["no_assistant_text"])
    score, flags = quality._score_tic_density([u("q"), a("   ")])
    check("whitespace-only assistant text returns 0 with no_assistant_text",
          score == 0 and flags == ["no_assistant_text"])
    score, flags = quality._score_tic_density(KITCHEN_SINK_CONV)
    # Four of the five measurable bands fire on this fixture (the comma band needs a
    # higher share of 3+-comma sentences than these carry); shallow_narration needs
    # asterisks and has its own dedicated fixture above.
    check("kitchen-sink conversation trips denial, runs, stacks and warm closers",
          {"denial_correction_tic", "negation_runs", "qualification_stack",
           "formulaic_warm_closer"}.issubset(flags))
    check("heavily-ticked conversation stays far below the clean one",
          score < 50 and score >= 0)
    clean_score, _ = quality._score_tic_density(CLEAN_CONV)
    check("kitchen sink scores much lower than clean prose", score <= clean_score - 50)
    set_config({"quality": {"enabled": True, "tic_dimension": True}})
    r = quality.score_conversation([u("only a question")], "t-empty", 1)
    check("under-scoped conversation (method none) records tic_density as 0 when active",
          r["method"] == "none" and r["dimensions"].get("tic_density") == 0)


def main():
    test_toggle_off_is_default()
    test_registration_is_heuristic_only()
    test_clean_conversation_scores_high()
    test_denial_correction_fixture()
    test_negation_runs_fixture()
    test_comma_stacking_fixture()
    test_warm_closer_fixture()
    test_stacked_qualifiers_fixture()
    test_shallow_narration_fixture()
    test_empty_and_clamped()
    if FAILURES:
        print("\n%d FAILED check(s):" % len(FAILURES))
        for name in FAILURES:
            print("  - %s" % name)
        return 1
    print("\nAll tic_density checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
