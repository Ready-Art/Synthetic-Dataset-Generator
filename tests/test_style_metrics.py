#!/usr/bin/env python3
"""Headless tests for style_metrics.py (no model, network or display).

Run from the repository root:  python -B tests/test_style_metrics.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import style_metrics as sm

FAILS = []


def check(label, condition):
    if condition:
        print("  ok    %s" % label)
    else:
        print("  FAIL  %s" % label)
        FAILS.append(label)


# The owner's own flagged samples (denial_correction.py --selftest POSITIVES).
DENIAL_POS = [
    "The ground didn't tremble. It screamed.",
    "The heat didn't build. It detonated.",
    "The ash around you didn't burn; it vanished.",
    "Your skin didn't char. It sublimated.",
    "She doesn't offer a hand; she simply plants herself in her line of sight.",
    "The word isn't a request; it is a physical push.",
]
# The same file's NEGATIVES: scalar dosing, subject drift, chain-of-thought,
# a plain negation, and a negation RUN (which is a different target).
DENIAL_NEG = [
    "She walked to the door and opened it. The room was cold.",
    "He gripped her not tightly enough to crush, but enough to hem her in.",
    "She didn't flinch; in fact, the flames seemed to curl around her like tame pets.",
    "It wouldn't just drop character because the user did. I should challenge them.",
    "Kasuga had never been good with emotions.",
    "She didn't move. She didn't speak.",
    "He wasn't watching her at all.",
]


def test_denial_correction():
    print("denial_correction_hits")
    for text in DENIAL_POS:
        check("positives fire: %s" % text[:52], bool(sm.denial_correction_hits(text)))
    for text in DENIAL_NEG:
        check("negatives do not fire: %s" % text[:52], not sm.denial_correction_hits(text))
    check("empty text yields no spans", sm.denial_correction_hits("") == [])
    check("None yields no spans", sm.denial_correction_hits(None) == [])


def test_narration_segments():
    print("segments")
    asterisk = "*She steps forward.* \"Come here,\" she says."
    segs = sm.segments(asterisk)
    kinds = [k for _s, _e, k in segs]
    check("asterisks mark narration when present", kinds[0] == "narr")
    check("unmarked text beside asterisks is dialogue", "dial" in kinds)
    quoted = '"Come here," she says, and steps forward.'
    kinds2 = [k for _s, _e, k in sm.segments(quoted)]
    check("quotes mark dialogue when no asterisks exist", kinds2[0] == "dial")
    check("no markup means the whole reply is narration",
          sm.segments("She walks in.") == [(0, 13, "narr")])
    # offsets must tile the string exactly, or splicing would corrupt text
    total = sum(e - s for s, e, _k in segs)
    check("segments tile the whole text", total == len(asterisk))


def test_negation_profile():
    print("negation_profile")
    # narration-only, no asterisks at all -> the whole reply is narration
    run = "She didn't move. She didn't speak. She didn't look up."
    prof = sm.negation_profile(run)
    check("a consecutive run is counted", prof["run_ge2"] >= 1)
    check("max_run reports the run length", prof["max_run"] == 3)
    # with asterisks present, unmarked text is DIALOGUE (narrative_neg's rule), so a
    # bare run beside an asterisk block must not be counted as narration.
    mixed = "She didn't move. She didn't speak. *She didn't look up.*"
    check("unmarked text beside asterisks is dialogue, not narration",
          sm.negation_profile(mixed)["run_ge2"] == 0)
    single = sm.negation_profile("*She didn't move. She smiled instead.*")
    check("a single negation is not a run", single["run_ge2"] == 0)
    dialogue = 'She smiled. "I am not going hiking with you," she said.'
    check("dialogue negation is not counted as narration",
          sm.negation_profile(dialogue)["narr_neg"] == 0)
    check("empty text is all zeros", sm.negation_profile("")["narr_neg"] == 0)


def test_trailers():
    print("trailer_hits")
    one = sm.trailer_hits("*She stepped closer, her lips parted.*")
    check("a possessive+noun absolute is a trailer", len(one) == 1 and one[0]["n_trailers"] == 1)
    stacked = sm.trailer_hits("*She stepped closer, her lips parted, head lolling, breath shallow.*")
    check("three comma-appended absolutes are stacked", stacked and stacked[0]["stacked"] == 1)
    check("clean prose has no trailer",
          not sm.trailer_hits("She walked to the door and opened it."))


def test_question_handback():
    print("question_handback")
    check("a reply ending on '?' is a handback",
          sm.question_handback("She looks up. What do you think?"))
    check("a trailing quote after '?' still counts",
          sm.question_handback('She looks up. "What do you think?"'))
    check("a statement is not a handback",
          not sm.question_handback("She looks up and says nothing."))


def test_comma_stats():
    print("comma_stats")
    # two sentences: "a, b, c, d." (3 commas) and "e, f." (1 comma) -> mean 2.0
    s = sm.comma_stats(["a, b, c, d. e, f."])
    check("commas per sentence is the mean of per-response means",
          abs(s["commas_per_sent"] - 2.0) < 1e-9)
    check("the 3+ comma tail is a share of sentences",
          abs(s["sents_3plus_commas_pct"] - 50.0) < 1e-9)
    check("empty input does not divide by zero", sm.comma_stats([])["n"] == 0)
    check("blank rows are skipped", sm.comma_stats(["", "   "])["n"] == 0)


def test_tail_positivity():
    print("tail_positivity")
    t = sm.tail_positivity("He walked in. She looked up. The night was still young.")
    check("a significance closer lifts the final sentence", t["sig_final"] == 100.0)
    check("lift is clamped, never unbounded", t["sig_lift"] <= 10.0)
    short = sm.tail_positivity("One sentence only.")
    check("fewer than three sentences yields no measurement", short["n"] == 1)


def test_depth_proxies():
    print("depth_proxies")
    text = "*She steps forward.* \"Come here.\""
    d = sm.depth_proxies(text)
    check("narration share is a percentage of characters",
          abs(d["narration_pct"] - 100.0 * 18 / len(text)) < 0.5)
    check("no context leaves grounding unmeasured", d["grounding_per_100w"] == -1.0)
    g = sm.depth_proxies("the alley behind the tavern", context_text="the alley behind the tavern")
    check("a fully grounded reply scores 100", g["grounding_per_100w"] == 100.0)


def test_coherence_defects():
    print("coherence_defects")
    check("script contamination is flagged",
          sm.coherence_defects("They're\u5f00\u59cb to view the other")["script_mix"] == 1)
    check("a foreign article is flagged",
          sm.coherence_defects("The gesture is la gesture")["foreign_article"] == 1)
    check("two determiners in a row are flagged",
          sm.coherence_defects("the a man walked in")["det_stutter"] == 1)
    check("a duplicated sentence is flagged",
          sm.coherence_defects("This sentence is long enough to be counted here. "
                               "This sentence is long enough to be counted here.")["dup_sentence"] == 1)
    check("clean prose raises nothing",
          sm.coherence_defects("She walked to the door and opened it.")["any"] == 0)
    check("kaomoji are not script contamination",
          not sm.has_script_contamination("She smiled. (\uff89\u25d5\u30ee\u25d5)\uff89"))
    check("a real CJK word is script contamination",
          sm.has_script_contamination("They're\u5f00\u59cb to view"))


def test_response_metrics_contract():
    print("response_metrics / aggregate")
    row = sm.response_metrics("*She doesn't move away. She stays.* \"You came back,\" she says.")
    check("every value is a number", all(isinstance(v, (int, float)) for v in row.values()))
    check("every key is a plain identifier", all(k.isidentifier() for k in row))
    agg = sm.aggregate([row, sm.response_metrics("")])
    check("aggregate reports the row count", agg["n"] == 2)
    check("unmeasured values are excluded from means",
          agg["grounding_per_100w"] == -1.0)
    check("aggregate of nothing is not an error", sm.aggregate([]) == {"n": 0})
    for bad in ("", None, "   ", "One."):
        m = sm.response_metrics(bad or "")
        check("degenerate input %r yields a full row" % (bad,), len(m) == len(row))


def main():
    for fn in (test_denial_correction, test_narration_segments, test_negation_profile,
               test_trailers, test_question_handback, test_comma_stats,
               test_tail_positivity, test_depth_proxies, test_coherence_defects,
               test_response_metrics_contract):
        fn()
        print("")
    if FAILS:
        print("%d FAILURE(S):" % len(FAILS))
        for f in FAILS:
            print("  - %s" % f)
        return 1
    print("all style_metrics checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
