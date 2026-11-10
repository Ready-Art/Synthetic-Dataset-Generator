# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# This file is part of the ReadyArt Synthetic Dataset Generator and is licensed
# under the GNU Affero General Public License v3.0 or later. See LICENSE.

# logging_config.py
import json
import time
import os
from colorama import init, Fore, Style

# Initialize colorama for colored console output
init()

# Log file path - same for all modules
LOG_FILE_PATH = os.path.join('output', 'log.txt')
os.makedirs('output', exist_ok=True)

DEBUG_LOGGING_ENABLED = False

# --- Prompt-trace dumps (opt-in) ---------------------------------------------
#
# ``output/debug_prompt*.jsonl`` is a trace of every request the generator sends: the full
# message list, the payload and (for the judge) the raw reply. It is enormously useful when a
# prompt is wrong, and it used to be written UNCONDITIONALLY -- one append per request, from
# all 40 workers plus the judge pool, to a file with no size cap.
#
# At the scale this application is actually run at (thousands of chunks, 40 generation threads
# plus a 12-wide async judge) that is tens of thousands of appends of multi-kilobyte lines to a
# handful of shared files, on top of the dataset and the log. It is pure diagnostic overhead on
# every single request, and nothing reads those files during a run.
#
# So it is OFF unless asked for, like every other expensive feature in this project (AGENTS.md:
# a default run stays byte-identical and features are opt-in). Turn it on with
# ``generation.debug_prompt_log: true`` in the config, or the "Prompt Trace" checkbox next to
# Debug Logs in the Tools panel. ``debug_prompt_logging_enabled()`` is the ONE place that
# decision is made, so the writers in ``generation.py`` and ``quality.py`` cannot drift apart.
DEBUG_PROMPT_LOGGING_ENABLED = False


def debug_prompt_logging_enabled():
    """Whether the per-request prompt traces should be written.

    Read by every debug-log write site. Kept as a function (not a bare flag read) so a caller
    cannot accidentally capture the value at import time and keep writing after a toggle.
    """
    return DEBUG_PROMPT_LOGGING_ENABLED


def write_debug_prompt(path, payload):
    """Append one prompt-trace line, or do nothing when tracing is off.

    Callers pass an already-serialisable ``payload``; the JSON encoding and the append happen
    here so every trace line is produced the same way. Exceptions are swallowed deliberately:
    a trace is diagnostic, and losing one must never fail a generation request.
    """
    if not DEBUG_PROMPT_LOGGING_ENABLED:
        return False
    try:
        with open(path, 'a', encoding='utf-8') as handle:
            handle.write(json.dumps(payload) + '\n')
        return True
    except Exception:
        return False

def log_message(message, level="INFO"):
    """
    Writes a message to the log file and prints to console with color.
    Use this function in ALL your Python files.
    """

    if level == "DEBUG" and not DEBUG_LOGGING_ENABLED:
        return

    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    log_entry = f"{timestamp} [{level}] - {message}"

    # Colors for different log levels
    color_map = {
        "INFO": Fore.CYAN,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "DEBUG": Fore.GREEN,
        "CRITICAL": Fore.MAGENTA
    }
    color = color_map.get(level, Fore.WHITE)

    try:
        with open(LOG_FILE_PATH, 'a', encoding='utf-8') as log_file:
            log_file.write(log_entry + '\n')
    except Exception as e:
        print(f"{Fore.RED}CRITICAL: Failed to write to log file: {e}{Style.RESET_ALL}")

    print(f"{color}{log_entry}{Style.RESET_ALL}")
