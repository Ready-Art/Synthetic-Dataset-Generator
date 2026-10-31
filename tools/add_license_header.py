# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Add or refresh the AGPLv3 license header on every text/code file in this repository.

Run from the repository root:

    python -B tools/add_license_header.py            # write the header where it is missing
    python -B tools/add_license_header.py --check    # exit 1 if any file lacks it (no writes)
    python -B tools/add_license_header.py --verbose  # list every file it looked at

The header is a plain comment block, so it is invisible to every parser this project
uses (Python, YAML, Markdown, the test scripts). It is inserted at the very top of the
file, but after a shebang line and after a PEP 263 encoding declaration when those
exist, so a module docstring stays the first statement and ``__doc__`` keeps working.

The tool is idempotent. A file that already carries the header is left byte-identical;
a file whose header block is out of date has just that block replaced.

Deliberately skipped:

* ``LICENSE`` — the verbatim FSF text; the AGPL forbids modifying it.
* ``LICENSE-AGPLv3`` / ``LICENSE-MIT`` — they *are* the license notices.
* ``gemma4_thought_fix.py`` and ``tools/gemma4_thought_fix.py`` — written by Karki
  (a user on Discord), not by this repository. Stamping this project's license on
  someone else's script would be a misattribution (see AGENTS.md).
* binary assets, and generated/ignored output (``output/``, ``layout-check/``).
"""

import argparse
import os
import sys

# The managed block, shared by every file. The SPDX line is the marker a later run
# looks for; the closing credits line bounds the block.
HEADER_LINES = [
    "SPDX-License-Identifier: AGPL-3.0-or-later",
    "",
    "ReadyArt Synthetic Dataset Generator",
    "Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors",
    "",
    "All code here is AGPLv3, and has been for some time. Very early versions were MIT by",
    "Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that",
    "it is unlikely any of that code survives. Unless you have the original MIT-era code to",
    "compare against, treat this file as AGPLv3-only. See docs/licensing.md.",
]

MARKER = "SPDX-License-Identifier: AGPL-3.0-or-later"
END_MARKER = "docs/licensing.md."

# Extensions commented with '#'. Deliberately excludes .json/.jsonl, which have no comments.
HASH_EXTENSIONS = {".py", ".yml", ".yaml", ".txt", ".cfg", ".ini", ".toml", ".sh"}
MD_EXTENSIONS = {".md"}

# Extension-less files that are still text.
HASH_FILENAMES = {".gitignore"}

SKIP_PATHS = {
    "LICENSE",
    "LICENSE-AGPLv3",
    "LICENSE-MIT",
    # Third-party script: written by Karki (Discord), not by this repository.
    "gemma4_thought_fix.py",
    "tools/gemma4_thought_fix.py",
}

SKIP_DIRECTORIES = {".git", "__pycache__", ".pytest_cache", "output", "layout-check"}


def repo_root():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def comment_block(path):
    """Return the header as a list of source lines (no line endings) for `path`."""
    if os.path.splitext(path)[1].lower() in MD_EXTENSIONS:
        return ["<!--"] + HEADER_LINES + ["-->"]
    return ["# " + line if line else "#" for line in HEADER_LINES]


def iter_text_files(root):
    """Yield (relative path, absolute path) for every file that should carry the header."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRECTORIES)
        for name in sorted(filenames):
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            if rel in SKIP_PATHS:
                continue
            ext = os.path.splitext(name)[1].lower()
            if ext not in HASH_EXTENSIONS and ext not in MD_EXTENSIONS and name not in HASH_FILENAMES:
                continue
            yield rel, full


def split_preamble(lines):
    """Split off a shebang line and a PEP 263 encoding line, which must stay first."""
    prefix = []
    index = 0
    if lines and lines[0].startswith("#!"):
        prefix.append(lines[0])
        index = 1
    if len(lines) > index and lines[index].startswith("#") and "coding" in lines[index]:
        prefix.append(lines[index])
        index += 1
    return prefix, lines[index:]


def strip_existing_header(body, is_markdown):
    """Drop a managed header already at the start of `body`, if there is one."""
    if not body:
        return body
    if is_markdown:
        if body[0].strip() != "<!--":
            return body
        for index, line in enumerate(body):
            if line.strip() == "-->":
                block = body[: index + 1]
                if any(MARKER in entry for entry in block):
                    rest = body[index + 1:]
                    # Drop the single blank separator line the tool writes after a header.
                    if rest and rest[0].strip() == "":
                        rest = rest[1:]
                    return rest
                return body
        return body

    # Hash comment: the managed block is a contiguous run of '#' lines at the top.
    end = -1
    for index, line in enumerate(body):
        if not line.strip().startswith("#"):
            break
        end = index
    if end < 0:
        return body
    block = body[: end + 1]
    if not any(MARKER in entry for entry in block) or not any(END_MARKER in entry for entry in block):
        return body
    rest = body[end + 1:]
    if rest and rest[0].strip() == "":
        rest = rest[1:]
    return rest


def process(rel, full, check_only):
    """Return one of 'unchanged', 'written', 'missing', 'binary', 'non-utf8'."""
    with open(full, "rb") as handle:
        raw = handle.read()
    if b"\x00" in raw[:8192]:
        return "binary"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return "non-utf8"

    newline = "\r\n" if b"\r\n" in raw else "\n"
    ends_with_newline = text.endswith("\n")
    lines = text.split("\n")
    if ends_with_newline:
        lines.pop()
    lines = [line[:-1] if line.endswith("\r") else line for line in lines]

    is_markdown = os.path.splitext(rel)[1].lower() in MD_EXTENSIONS
    prefix, body = split_preamble(lines)
    body = strip_existing_header(body, is_markdown)

    new_lines = prefix + comment_block(rel) + ([""] if body else []) + body
    new_text = newline.join(new_lines)
    if body or ends_with_newline:
        new_text += newline

    if new_text == text:
        return "unchanged"
    if check_only:
        return "missing"
    with open(full, "w", encoding="utf-8", newline="") as handle:
        handle.write(new_text)
    return "written"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true",
                        help="report files missing the header and exit 1; write nothing")
    parser.add_argument("--verbose", action="store_true", help="list every file examined")
    args = parser.parse_args(argv)

    root = repo_root()
    counts = {}
    problems = []
    for rel, full in iter_text_files(root):
        result = process(rel, full, args.check)
        counts[result] = counts.get(result, 0) + 1
        if result in ("written", "missing"):
            problems.append((rel, result))
        if args.verbose:
            print("{:9} {}".format(result, rel))

    for rel, result in problems:
        print("{}: {}".format("MISSING" if result == "missing" else "UPDATED", rel))

    summary = ", ".join("{} {}".format(count, name) for name, count in sorted(counts.items()))
    print("\n{} file(s) examined: {}".format(sum(counts.values()), summary or "none"))
    if args.check and problems:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
