<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
-->

# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

# Licensing

**All code in this repository is AGPLv3.**

If you are looking for a short answer: use the AGPLv3.

- Full license text: [`LICENSE`](../LICENSE) (GNU AGPLv3, verbatim)
- Project notice: [`LICENSE-AGPLv3`](../LICENSE-AGPLv3)
- Credits: [`CONTRIBUTORS.md`](../CONTRIBUTORS.md)

## The short version

| Question | Answer |
|----------|--------|
| What license is this project? | AGPLv3 (GNU Affero General Public License, version 3 or later). |
| Is every file AGPLv3? | Yes. Every text and code file carries the same SPDX header. |
| Can I use a specific file under a different license? | No. There is one license here. |
| What if a file has no SPDX header? | It is still AGPLv3. The header is a convenience, not the license. |

## What this means for you

**Using the generator.** The AGPLv3 applies. If you run a modified version to provide
a service over a network, you must offer the corresponding source to the users of that
service. Running it locally to build a dataset does not attach the license to your
dataset output.

**Reusing code from this repository.** Copy it under the AGPLv3.

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

The managed block is:

```
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
under the GNU Affero General Public License v3.0 or later. See LICENSE.
```

Deliberately **not** stamped, and why:

| File | Reason |
|------|--------|
| `LICENSE` | Verbatim FSF text. The AGPL forbids modifying it. |
| `gemma4_thought_fix.py`, `tools/gemma4_thought_fix.py` | Written by **Karki** (a user on Discord), not by this repository. Stamping this project's license on someone else's script would be a misattribution. It keeps its own authorship notice. |
| `taskbar.png`, `docs/images/*.png` | Binary assets. |

## The credit line

Every header carries:

```
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
```

The "and other contributors" is deliberate and load-bearing. This is a GitHub project,
so there are likely contributors whose names are not in the commit history and cannot be
recovered. They are credited collectively rather than dropped.
[`CONTRIBUTORS.md`](../CONTRIBUTORS.md) names everyone identifiable and explains how
to get added.
