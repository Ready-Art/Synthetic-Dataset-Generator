<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# Contributors

ReadyArt Synthetic Dataset Generator is AGPLv3. See [LICENSE](LICENSE),
[LICENSE-AGPLv3](LICENSE-AGPLv3) and [docs/licensing.md](docs/licensing.md).

This list credits everyone whose work is in this repository. It is not exhaustive:
this is a GitHub project, so there are likely further contributors whose changes
arrived through pull requests, patches, or the early MIT-era uploads and are not
attributed in the commit history. **If you contributed and are not named here, that
is an omission, not a judgement — please open an issue or a pull request and you will
be added.**

## Project authors

| Name | Role |
|------|------|
| **Sleep Deprived** | Original author. Co-author of the early MIT-licensed code. |
| **FrenzyBiscuit (WestFox35)** | Original author and principal maintainer. Co-author of the early MIT-licensed code. |

Both are credited on every source file's SPDX header and in the license notices.

## Contributors

Named from the repository's commit history, in rough order of contribution size.

| Name | Handle / contact |
|------|------------------|
| **andyoneal** | [@andyoneal](https://github.com/andyoneal) — dataset compatibility, PostgreSQL docs, `.gitignore`, test hygiene |
| **serialhex (Justin Patera)** | [@serialhex](https://github.com/serialhex) — dashboard refresh gating, API compatibility profiles, issue-graph redraw |
| **schonsense** | [@schonsense](https://github.com/schonsense) — upstream merges, OOC detection, dataset compatibility checks |
| **strangedove** | [@strangedove](https://github.com/strangedove) — module refactor: `app_state`, `generation`, `dashboard` extraction |

## Third-party credit (not this project's code)

| Name | What |
|------|------|
| **Karki** (a user on Discord) | Author of `gemma4_thought_fix.py` and `tools/gemma4_thought_fix.py`, a standalone axolotl helper for Gemma 4 fine-tunes. It is **not part of this application** and is not covered by this project's copyright; it is redistributed here with robustness fixes only, and it keeps its own authorship notice. See [docs/gemma4-chat-template.md](docs/gemma4-chat-template.md). |

## Tooling

Parts of the refactor, documentation and test suites were written with AI coding
assistants, credited in the commit trailers as `Co-Authored-By: Claude`. They are
tools, not copyright holders of this project.

## Unlisted contributors

This project has taken pull requests and patches from people who are not in the table
above, and the early MIT-era uploads may contain work by people whose names were never
recorded. All of them are covered by the blanket credit in every file header:

> Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

If that is you, say so and the list will be corrected.

## Licensing status of contributions

By contributing to this repository you agree that your contribution is licensed under
the AGPLv3, as described in [docs/licensing.md](docs/licensing.md).
