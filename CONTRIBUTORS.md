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

# Contributors

ReadyArt Synthetic Dataset Generator is AGPLv3. See [LICENSE](LICENSE),
[LICENSE-AGPLv3](LICENSE-AGPLv3) and [docs/licensing.md](docs/licensing.md).

This list credits everyone whose work is in this repository. It is not exhaustive:
this is a GitHub project, so there are likely further contributors whose changes
arrived through pull requests or patches and are not attributed in the commit
history. **If you contributed and are not named here, that is an omission, not a
judgement — please open an issue or a pull request and you will be added.**

## Project authors

| Name | Role |
|------|------|
| **Sleep Deprived** | Original author. |
| **FrenzyBiscuit (WestFox35)** | Original author and principal maintainer. |

Both are credited on every source file's SPDX header and in the license notices.

## Contributors

Named from the repository's commit history, in rough order of contribution size.

| Name | Handle / contact |
|------|------------------|
| **andyoneal** | [@andyoneal](https://github.com/andyoneal) — dataset compatibility, PostgreSQL docs, `.gitignore`, test hygiene |
| **serialhex (Justin Patera)** | [@serialhex](https://github.com/serialhex) — API compatibility profiles, issue-graph redraw |
| **schonsense** | [@schonsense](https://github.com/schonsense) — upstream merges, OOC detection, dataset compatibility checks |
| **strangedove** | [@strangedove](https://github.com/strangedove) — module refactor: `app_state`, `generation` extraction |

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
above. All of them are covered by the blanket credit in every file header:
