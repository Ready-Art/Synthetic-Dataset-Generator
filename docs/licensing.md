<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Licensing

**All code in this repository is AGPLv3, and has been for some time.**

If you are looking for a short answer: use the AGPLv3. There is no MIT code here that
you can reasonably rely on finding.

- Full license text: [`LICENSE`](../LICENSE) (GNU AGPLv3, verbatim)
- Project notice: [`LICENSE-AGPLv3`](../LICENSE-AGPLv3)
- Historical MIT notice: [`LICENSE-MIT`](../LICENSE-MIT)
- Credits: [`CONTRIBUTORS.md`](../CONTRIBUTORS.md)

## The short version

| Question | Answer |
|----------|--------|
| What license is this project? | AGPLv3 (GNU Affero General Public License, version 3 or later). |
| Has it always been AGPLv3? | No. Very early versions were MIT. It has been AGPLv3 for a long time since. |
| Is there still MIT code in the tree? | Almost certainly not. The generator has been rewritten to such an extent that it is unlikely any of the original MIT code survives. |
| Can I use a specific file under MIT? | Only if you have the original MIT-era code and can show that file is unchanged from it. Otherwise treat it as AGPLv3. |
| What if a file has no SPDX header? | It is still AGPLv3. The header is a convenience, not the license. |

## What actually happened

The project began as a small MIT-licensed tool by **Sleep Deprived** and
**FrenzyBiscuit (WestFox35)**. That early code is the only MIT-licensed code this
project has ever had.

The generator was then relicensed to the AGPLv3 and has remained there. Since the
relicense it has grown from a single script into a multi-module application — the
worker engine, detection layer, quality scoring, dashboard, dataset bindings, config
editor and GUI were all written or rewritten after the relicense. Whole files were
split out of `generate.py` into `app_state.py`, `generation.py`, `dashboard.py` and
the `dataset_*.py` modules; the parts that were not split out were edited heavily in
place.

The practical consequence: **the overlap between the current tree and the MIT-era code
is not something anyone can rely on.** You would need the original MIT snapshot to
establish that a given file still contains MIT-licensed lines. Absent that snapshot,
the safe and correct reading is that everything here is AGPLv3.

This is a statement of fact about provenance, not a legal opinion. If you need one,
consult a lawyer.

## What this means for you

**Using the generator.** The AGPLv3 applies. If you run a modified version to provide
a service over a network, you must offer the corresponding source to the users of that
service. Running it locally to build a dataset does not attach the license to your
dataset output.

**Reusing code from this repository.** Copy it under the AGPLv3. If you want to use a
piece under MIT terms, you must have the MIT-era code and demonstrate the piece is
unchanged from it — and that burden is on you, not on this project.

**Contributing.** By contributing you agree your contribution is licensed under the
AGPLv3.

## Every file carries a header

Every text and code file in the repository starts with a managed SPDX header stating
the license, the copyright line and the credit line. The header is generated and
checked by [`tools/add_license_header.py`](../tools/add_license_header.py):

```sh
python -B tools/add_license_header.py           # write/refresh headers
python -B tools/add_license_header.py --check   # CI-style check, exits 1 if any file lacks one
```

The header is a comment block, so it does not affect parsing, `__doc__`, or YAML. It is
placed after a shebang and after a PEP 263 encoding line when a file has those.

Deliberately **not** stamped, and why:

| File | Reason |
|------|--------|
| `LICENSE` | Verbatim FSF text. The AGPL forbids modifying it. |
| `LICENSE-AGPLv3`, `LICENSE-MIT` | These *are* the license notices. |
| `gemma4_thought_fix.py`, `tools/gemma4_thought_fix.py` | Written by **Karki** (a user on Discord), not by this repository. Stamping this project's license on someone else's script would be a misattribution. It keeps its own authorship notice. |
| `taskbar.png`, `docs/images/*.png` | Binary assets. |

## The credit line

Every header carries:

```
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
```

The "and other contributors" is deliberate and load-bearing. This is a GitHub project,
so there are likely contributors — including from the MIT era — whose names are not in
the commit history and cannot be recovered. They are credited collectively rather than
dropped. [`CONTRIBUTORS.md`](../CONTRIBUTORS.md) names everyone identifiable and
explains how to get added.
