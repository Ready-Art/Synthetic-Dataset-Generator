# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

"""Regression tests for random names (prompts.random_names) -- `python tests/test_random_names.py`.

Random names exist to keep a dataset from repeating itself, so the pool must actually be
spread over the run. These tests pin the guarantees `generation.deal_random_names` makes:

  1. the pool is dealt WITHOUT replacement, so a long run uses every configured name instead
     of redrawing with replacement (the v12.8.0 bug: ~740 distinct names from a 1,000-name
     pool over 1,410 scenes, ~260 names never used and each used name repeating 2-6 times);
  2. the names are distinct WITHIN one conversation, so two characters in a multi-character
     scene are never handed the same name;
  3. a card's own name is never handed back as a "random" name;
  4. a pool smaller than the scene degrades to as many distinct names as it has, never blanks
     and never duplicates;
  5. the draw is per conversation and never written back to the character cards;
  6. swapping the configured pool starts a fresh deck.

No test framework, no network, no display. Exits non-zero if any check fails.
"""
import os
import random
import shutil
import sys
import tempfile
from collections import Counter

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRATCH = tempfile.mkdtemp(prefix="sdg-random-names-test-")
os.makedirs(os.path.join(SCRATCH, "config"))
shutil.copy(os.path.join(REPO, "config", "config.yml"),
            os.path.join(SCRATCH, "config", "config.yml"))
os.chdir(SCRATCH)
os.makedirs("output", exist_ok=True)
sys.path.insert(0, REPO)

import app_state  # noqa: E402
import generation as g  # noqa: E402

FAILURES = []


def check(name, cond):
    if cond:
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}")
        FAILURES.append(name)


def fresh_deck():
    """Forget the dealt deck so each scenario starts from a full pool."""
    app_state.random_name_deck = []
    app_state.random_names_pool_key = None


POOL = ["Name%04d" % i for i in range(1000)]


def test_no_replacement_across_the_run():
    fresh_deck()
    random.seed(1)
    dealt = []
    for _ in range(1410):
        dealt.extend(g.deal_random_names(POOL, 1))
    counts = Counter(dealt)
    check("1,410 single-character scenes use all 1,000 names",
          len(set(dealt)) == 1000)
    check("no name is used more than twice in 1,410 scenes", max(counts.values()) <= 2)
    check("the first 1,000 deals contain no repeat at all",
          len(set(dealt[:1000])) == 1000)

    fresh_deck()
    random.seed(2)
    dealt = []
    for _ in range(2000):
        dealt.extend(g.deal_random_names(POOL, 1))
    counts = Counter(dealt)
    check("2,000 scenes use all 1,000 names exactly twice",
          len(set(dealt)) == 1000 and max(counts.values()) == 2 and min(counts.values()) == 2)


def test_distinct_within_a_scene():
    fresh_deck()
    random.seed(3)
    dupes = 0
    for _ in range(20000):
        a, b = g.deal_random_names(POOL, 2)
        if a == b:
            dupes += 1
    check("a 2-character scene never repeats a name", dupes == 0)

    fresh_deck()
    random.seed(4)
    dupes = 0
    for _ in range(5000):
        names = g.deal_random_names(POOL, 4)
        if len(set(names)) != len(names):
            dupes += 1
    check("a 4-character scene never repeats a name", dupes == 0)


def test_card_name_is_never_reused():
    fresh_deck()
    random.seed(5)
    avoid = POOL[:5]
    leaks = 0
    for _ in range(5000):
        got = g.deal_random_names(POOL, 1, avoid=avoid)
        if got and got[0] in avoid:
            leaks += 1
    check("a card's own name is never dealt as a random name", leaks == 0)


def test_small_pools_degrade_safely():
    fresh_deck()
    got = g.deal_random_names(["A", "B"], 3)
    check("a pool smaller than the scene yields its distinct names, not duplicates",
          got == ["A", "B"] or got == ["B", "A"])

    fresh_deck()
    got = g.deal_random_names(["Only"], 1)
    check("a one-name pool still names one character", got == ["Only"])

    fresh_deck()
    check("an empty pool deals nothing", g.deal_random_names([], 2) == [])
    check("a None pool deals nothing", g.deal_random_names(None, 2) == [])
    check("a zero count deals nothing", g.deal_random_names(POOL, 0) == [])

    fresh_deck()
    got = g.deal_random_names(["A", "B"], 2, avoid=["A", "B"])
    check("a pool that is entirely card names still names every character",
          len(got) == 2 and len(set(got)) == 2)


def test_draw_does_not_touch_the_cards():
    cards = [{"name": "Ines"}, {"name": "Oren"}]
    snapshot = [dict(c) for c in cards]
    fresh_deck()
    g.deal_random_names(POOL, 2, avoid=[c["name"] for c in cards])
    check("dealing random names does not mutate the character cards", cards == snapshot)


def test_swapping_the_pool_starts_a_fresh_deck():
    fresh_deck()
    random.seed(6)
    for _ in range(10):
        g.deal_random_names(POOL, 1)
    before = app_state.random_names_pool_key
    new_pool = ["Z%03d" % i for i in range(50)]
    got = g.deal_random_names(new_pool, 1)
    check("a replaced pool deals from the new pool", got and got[0].startswith("Z"))
    check("a replaced pool resets the deck", app_state.random_names_pool_key != before)


def test_worker_metadata_matches_the_dealt_names():
    """The names recorded on the row must be the names the profile block carried."""
    src = os.path.join(REPO, "generation.py")
    with open(src, encoding="utf-8") as f:
        text = f.read()
    check("the worker deals random names once per scene, not per character",
          "scene_random_names = deal_random_names(" in text)
    check("the old per-character random.choice override is gone",
          "random_name = random.choice(random_names_list_local)" not in text)
    check("the deal passes the cards' own names as avoid",
          "avoid=[str(c.get('name', '') or '').strip() for c in selected_chars]" in text)


def main():
    print("(1) no replacement across the run")
    test_no_replacement_across_the_run()
    print("(2) distinct within a scene")
    test_distinct_within_a_scene()
    print("(3) a card's own name is never reused")
    test_card_name_is_never_reused()
    print("(4) small pools degrade safely")
    test_small_pools_degrade_safely()
    print("(5) the draw does not touch the cards")
    test_draw_does_not_touch_the_cards()
    print("(6) swapping the pool starts a fresh deck")
    test_swapping_the_pool_starts_a_fresh_deck()
    print("(7) worker wiring")
    test_worker_metadata_matches_the_dealt_names()
    if FAILURES:
        print(f"\n{len(FAILURES)} random-names test(s) failed")
        sys.exit(1)
    print("\nall random-names tests passed")


if __name__ == "__main__":
    main()
