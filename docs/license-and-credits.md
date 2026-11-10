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

# License and credits

## License

**The ReadyArt Synthetic Dataset Generator is licensed under the GNU Affero
General Public License, version 3 or later (AGPLv3).**

- Full license text: [`LICENSE`](../LICENSE)
- Project notice: [`LICENSE-AGPLv3`](../LICENSE-AGPLv3)
- The full licensing page, including what the license means for you:
  [`docs/licensing.md`](licensing.md)

Every text and code file in the repository carries the same SPDX header. There is
one license here, and it applies to the whole tree.

## Credits

### Project authors

- **Sleep Deprived** — original author
- **FrenzyBiscuit (WestFox35)** — original author and principal maintainer

Both are credited on every source file's SPDX header.

### Contributors

Named from the repository's commit history, in rough order of contribution size:

- **andyoneal** — dataset compatibility, PostgreSQL docs, `.gitignore`, test hygiene
- **serialhex (Justin Patera)** — API compatibility profiles, issue-graph redraw
- **schonsense** — upstream merges, OOC detection, dataset compatibility checks
- **strangedove** — module refactor: `app_state`, `generation` extraction

- **All other contributors, including those not named here.** This is a GitHub
  project, so there are likely more. They are credited collectively on every file
  header rather than dropped. If you contributed and are not listed, please open an
  issue or a pull request and you will be added.

See [`CONTRIBUTORS.md`](../CONTRIBUTORS.md) for the full list and how to get added.

### Third-party credit (not this project's code)

- **Karki** (a user on Discord) — author of `gemma4_thought_fix.py` and
  `tools/gemma4_thought_fix.py`, a standalone axolotl helper for Gemma 4
  fine-tunes. It is **not part of this application**, is not covered by this
  project's copyright, and keeps its own authorship notice. See
  [`docs/gemma4-chat-template.md`](gemma4-chat-template.md).

### Tooling

Parts of the refactor, documentation and test suites were written with AI coding
assistants, credited in the commit trailers as `Co-Authored-By: Claude`. They are
tools, not copyright holders of this project.
