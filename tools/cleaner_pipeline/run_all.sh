#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

# Run the full cleanup pipeline, step by step.
#
# Each step reads the previous step's output and writes a new file into build/,
# so any stage can be inspected or re-run independently.
#
# Usage:  ./run_all.sh            (from this directory)
#         ./run_all.sh --from 3   (resume at step 03)

set -uo pipefail

cd "$(dirname "$0")"

START=1
if [[ "${1:-}" == "--from" && -n "${2:-}" ]]; then
    START="$2"
fi

# The source dataset. Steps 01, 05 and 07 resolve it themselves through
# common.resolve_source(), so we only need it here to fail fast with a clear
# message. Resolution order: $SOURCE, then input.jsonl in this directory, then
# ../input.jsonl. The legacy name unclean.jsonl is still accepted as a fallback
# (with a warning) so old checkouts keep working.
#   SOURCE=../cleaned.jsonl ./run_all.sh
export SOURCE
if [[ -z "${SOURCE:-}" ]]; then
    for candidate in input.jsonl ../input.jsonl unclean.jsonl ../unclean.jsonl; do
        if [[ -f "$candidate" ]]; then SOURCE="$candidate"; break; fi
    done
fi
if [[ -z "${SOURCE:-}" || ! -f "$SOURCE" ]]; then
    echo "ERROR: no source dataset found"
    echo "       looked for: input.jsonl, ../input.jsonl, unclean.jsonl, ../unclean.jsonl"
    echo "       override with: SOURCE=/path/to/dataset.jsonl ./run_all.sh"
    exit 1
fi
if [[ "$(basename "$SOURCE")" == "unclean.jsonl" ]]; then
    echo "WARNING: source is named unclean.jsonl; rename it to input.jsonl"
fi

mkdir -p build

STEPS=(
    # Step 01 runs FIRST, on the pristine source, before anything edits text.
    # It drops records whose dialogue quotes do not pair up. It has to run first
    # because every later step rewrites text: once tokens are stripped and
    # truncation is repaired, a quote imbalance can no longer be attributed to
    # the source. It is the enforcement of what a separate hand-run checker
    # used to do by hand.
    "01_quote_guard.py"
    "02_strip_special_tokens.py"
    "03_strip_control_chars.py"
    "04_strip_escaped_and_markup.py"
    "05_normalise_whitespace.py"
    # Step 06 drops whole records, so it runs before 07 verifies the survivors.
    "06_drop_foreign_script.py"
    "07_verify.py"
    # Steps 08 and 09 also drop records, so they run last. 07 verifies the
    # token-cleaned dataset, then 08 repairs truncation, then 09 drops any record
    # whose dialogue quotes still do not pair up.
    "08_repair_truncation.py"
    "09_drop_unbalanced_quotes.py"
)

echo "================================================================"
echo " cleanup pipeline"
echo " source: $SOURCE"
echo " output: build/"
echo "================================================================"

for script in "${STEPS[@]}"; do
    num="${script%%_*}"
    if (( 10#$num < 10#$START )); then
        echo ""
        echo "--- skipping $script (before --from $START)"
        continue
    fi

    echo ""
    echo ">>> $script"
    if ! python3 "$script"; then
        echo ""
        echo "!!! $script FAILED - stopping pipeline"
        exit 1
    fi
done

echo ""
echo "================================================================"
echo " pipeline complete"
echo " quote-guarded : build/01_quotes.jsonl"
echo " token-cleaned : build/05_whitespace.jsonl"
echo " no foreign    : build/06_foreign_dropped.jsonl"
echo " repaired      : build/08_repaired.jsonl"
echo " final dataset : build/09_final.jsonl      <-- train on this"
echo " reports       : build/quote_guard_report.txt"
echo "                 build/foreign_script_report.txt"
echo "                 build/truncation_report.txt"
echo "                 build/unbalanced_quote_report.txt"
echo "================================================================"
