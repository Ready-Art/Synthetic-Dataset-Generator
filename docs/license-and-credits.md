<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 📜 License & Credits

## AGPLv3 — all of it

**All code in this repository is AGPLv3, and has been for some time.**

This project is licensed under the GNU Affero General Public License, version 3 or later. See [`LICENSE`](../LICENSE) for the full text and [`LICENSE-AGPLv3`](../LICENSE-AGPLv3) for the project notice.

## About the early MIT code

Very early versions of this project were released under the MIT license by **Sleep Deprived** and **FrenzyBiscuit (WestFox35)**. That is the only MIT-era code that has ever existed here.

The generator has changed to such an extent since then that it is unlikely any of the original MIT code survived. The worker engine, detection layer, quality scoring, dashboard, dataset bindings, config editor and GUI were all written or rewritten after the relicense, and `generate_tk.py` was split into `app_state.py`, `generation.py`, `dashboard.py` and the `dataset_*.py` modules.

**Unless you hold a copy of the original MIT-era code to compare against, treat every file here as AGPLv3-only.** Do not assume MIT terms apply to any part of the current generator. The old MIT notice is retained in [`LICENSE-MIT`](../LICENSE-MIT) for provenance only, and says as much at the top.

## Headers on every file

Every text and code file starts with an SPDX header stating the license, the copyright line and the credit line. The header is generated and checked by [`tools/add_license_header.py`](../tools/add_license_header.py):

```sh
python -B tools/add_license_header.py           # write/refresh headers
python -B tools/add_license_header.py --check   # exits 1 if any file lacks one
```

A file without the header is still AGPLv3 — the header is a convenience, not the license.

## Credits

- **Sleep Deprived** — original author, co-author of the early MIT code
- **FrenzyBiscuit (WestFox35)** — original author and principal maintainer, co-author of the early MIT code
- **andyoneal**, **serialhex (Justin Patera)**, **schonsense**, **strangedove** — contributors
- **All other contributors, including those not named here.** This is a GitHub project, so there are likely more, and the early MIT-era uploads may contain work by people never recorded in the commit history. They are credited collectively on every file header rather than dropped. If you contributed and are not listed, please open an issue or a pull request and you will be added.

See [`CONTRIBUTORS.md`](../CONTRIBUTORS.md) for the full list and [`docs/licensing.md`](licensing.md) for the reasoning in detail.

## Third-party code

`gemma4_thought_fix.py` and `tools/gemma4_thought_fix.py` were written by **Karki** (a user on Discord), not by this repository. They are a standalone axolotl helper for Gemma 4 fine-tunes, are not part of this application, and are not covered by this project's copyright. They keep their own authorship notice and are deliberately excluded from the license headers above.
