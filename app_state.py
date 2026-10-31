"""Shared runtime state for the Synthetic Dataset Generator (refactor step 2).

Single source of truth for the cross-module mutable globals — counters, per-API stat dicts, the
recent-event ring buffers, locks, run flags, and shared handles (root window, task queue, db pool).
Import it as a namespace and reference attributes (``import app_state`` / ``app_state.total_input_tokens += 1``)
so mutations are visible app-wide; do NOT ``from app_state import x`` for the scalars (that copies the
binding and breaks cross-module updates). This module imports nothing project-local, to stay a leaf in the
dependency graph (generate.py / generation.py / dashboard.py import it, never the reverse).
"""
import os
import threading
from threading import Lock
from config_loader import ConfigLoader
import json

_previous_progress_values = {}
anti_slop_count_total = 0
completed_task_ids = set() # Set of IDs for tasks that have been successfully processed
db_pool = None
error_count_total = 0
error_counts_per_api = {i: 0 for i in range(6)}
issue_timestamps = {
    'refusals': [],
    'user_speaking': [],
    'slop': [],
    'errors': [],
    'anti_slop': [],
    'incomplete_quotes': [],
    'out_of_character': [],
    'logic_issues': [],
    'regex_watch': [],
    'non_latin': [],
    'scene_continuity': []
}
issue_timestamps_lock = Lock()
loaded_api_processed_tasks_snapshot = None # Snapshot of per-API progress loaded from state file
num_threads = 10 # Default number of worker threads, configurable via UI
pause_processing = False # Flag to signal threads to pause
processing_active = False # Flag indicating if a generation job is currently running
question_history = [] # Stores recently generated initial questions to avoid repetition
question_history_lock = Lock()  # For question_history list
questions_list = [] # List of questions if using questions.txt
rate_limit_labels = {}
recent_anti_slop_per_api = {i: [] for i in range(6)}
recent_anti_slop_total = []
recent_errors_per_api = {i: [] for i in range(6)}
recent_errors_total = []
recent_refusals_per_api = {i: [] for i in range(6)}
recent_refusals_total = []
recent_slop_per_api = {i: [] for i in range(6)}
recent_slop_total = []
recent_user_speaking_per_api = {i: [] for i in range(6)}
recent_user_speaking_total = []
recent_ooc_per_api = {i: [] for i in range(6)}
recent_ooc_total = []
recent_logic_per_api = {i: [] for i in range(6)}
recent_logic_total = []
recent_regex_per_api = {i: [] for i in range(6)}
recent_regex_total = []
recent_non_latin_per_api = {i: [] for i in range(6)}
recent_non_latin_total = []
recent_scene_continuity_per_api = {i: [] for i in range(6)}
recent_scene_continuity_total = []
refusal_count_total = 0
refusal_counts_per_api = {i: 0 for i in range(6)}
slop_count_total = 0
slop_counts_per_api = {i: 0 for i in range(6)}
state_file_lock = Lock() # Lock for thread-safe access to the generation_state.json file
stats_lock = Lock()  # For counters (attempts, errors, tokens, refusals, slop, etc.)
stop_processing = False # Flag to signal threads to stop processing
system_prompt_counter = 0 # Counter for cycling through variable system prompts
system_prompt_lock = Lock() # Lock for thread-safe access to system_prompt_counter
system_prompts_list = [] # List of system prompts (base or variations)
task_queue = None # Queue for distributing tasks to worker threads
threads = [] # List to hold active worker thread objects
total_attempts_global = 0 # Total LLM calls made across all APIs for generation tasks
total_attempts_per_api = {i: 0 for i in range(6)} # Total LLM calls per specific API slot
total_input_tokens = 0
total_output_tokens = 0
user_speaking_count_total = 0
user_speaking_counts_per_api = {i: 0 for i in range(6)}
ooc_count_total = 0
ooc_counts_per_api = {i: 0 for i in range(6)}
logic_issue_count_total = 0
logic_issue_counts_per_api = {i: 0 for i in range(6)}
regex_watch_count_total = 0
regex_watch_counts_per_api = {i: 0 for i in range(6)}
# --- Non-Latin script check (v11.10.0) ---
# A Unicode-range allow-list (detection.is_non_latin) flags stray characters from another
# script. Purely character-class based: no LLM call, no phrase list, no network request.
non_latin_count_total = 0
non_latin_counts_per_api = {i: 0 for i in range(6)}
# --- Scene cast continuity (v12.0) ---
# A character who has LEFT the scene (or has not arrived yet) may not speak or act. The check
# is code-only (detection.check_scene_continuity): attributed quoted lines plus a curated
# narration action-verb list, so no LLM is involved in the DETECTION itself. These counters
# track hard violations that triggered a regeneration.
scene_continuity_count_total = 0
scene_continuity_counts_per_api = {i: 0 for i in range(6)}
# --- Regex Watch (v11.6) ---
# Every pattern hit that led to a rewrite, kept for the Regex Watch dashboard area so the user
# can see WHICH pattern fires and what it matched. Bounded ring buffer, guarded by
# regex_watch_lock. Entries are (pattern, matched_text, api_slot_idx, rewrite_status).
regex_watch_log = []
regex_watch_log_lock = Lock()
# --- Scene cast event log (v12.0) ---
# Every cast transition the state machine applied, kept so a run can be audited after the fact:
# entries are (task_id, event, name, api_slot_idx) where event is one of 'arrived', 'exited',
# 'arrival_deferred', 'exit_deferred', 'arrival_forced', 'exit_cancelled'. 'exit_cancelled' is
# the important one: it means the model never narrated a scheduled departure, so the character
# legitimately stayed. Bounded ring buffer, guarded by cast_events_log_lock.
MAX_CAST_EVENTS_LOG = 500
cast_events_log = []
cast_events_log_lock = Lock()


def record_cast_events(task_id, events, api_slot_idx=0):
    """Append this task's cast transitions to the bounded ring buffer (thread-safe).

    ``events`` is the (event, name) list returned by generation._advance_cast_state.
    A no-op for an empty list so the feature costs nothing when it is off.
    """
    if not events:
        return
    with cast_events_log_lock:
        for event, name in events:
            cast_events_log.append((task_id, event, name, api_slot_idx))
        overflow = len(cast_events_log) - MAX_CAST_EVENTS_LOG
        if overflow > 0:
            del cast_events_log[:overflow]
MAX_REGEX_WATCH_LOG = 200
character_counter = 0  # Round-robin counter for character selection
character_counter_lock = Lock()  # Thread-safe access
pending_task_ids = set()
failed_task_ids = set()
purged_task_ids = set()
task_metadata = {}  # {task_id: {'file': str, 'retries': int, 'task_data': tuple}}
task_queue_ui_lock = Lock()
queue_trees = {}    # Holds UI Treeview widgets



# --- engine-shared config / constants / per-API state (step 3a; read-only or item-mutated only) ---
STATE_FILE_PATH = os.path.join('output', 'generation_state.json')
global_config = ConfigLoader() # Instantiate the config loader globally
INPUT_DIR = 'input' # Directory for input text files
OUTPUT_DIR = 'output' # Directory for output files (dataset, logs, state)
BASE_OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIR, 'output') 
BASE_DEBUG_LOG_PATH = os.path.join(OUTPUT_DIR, 'debug_prompt')
estimated_cost = 0.0
MAX_RECENT = 10 # Max number of recent issues to store and display
anti_slop_counts_per_api = {i: 0 for i in range(6)}
API_CIRCUIT_BREAKER = {
    "max_consecutive_failures": 5,
    "base_cooldown_seconds": 60,
    "max_cooldown_seconds": 600,
    "failures": {i: 0 for i in range(6)},
    "last_failure_time": {i: 0.0 for i in range(6)},
    "is_open": {i: False for i in range(6)},
    "current_cooldown": {i: 60 for i in range(6)}
}
api_circuit_breaker_lock = threading.Lock()
MAX_TASK_REQUEUES = 50
task_retry_counts = {}
task_retry_lock = threading.Lock()
quality_lock = Lock()
quality_scores = {}  # {task_id: {"composite": float, "dimensions": {...}, "flags": [...], "scored_at": str, "method": str}}
quality_enabled = True  # Master toggle (overridden by config)
quality_output_filter = False  # If True, only write conversations meeting the threshold to output
use_conversation_history_for_fixers = True  # If True, pass conversation history to slop/anti-slop fixers
# --- Quality Review Queue ---
quality_review_ids = set()  # task_ids flagged for human review
quality_review_lock = Lock()
# --- Judge drops (v11.4) ---
# Conversations the quality judge rejected (invalid JSON, a logic score of 0, or every
# retry exhausted). They are never written to the dataset; the reasons are kept so the
# dashboard can report them. Guarded by quality_lock.
quality_dropped = {}  # {task_id: reason}
dropped_task_ids = set()
master_duplication_enabled_var = None  # tk.BooleanVar — assigned in generate.py at GUI build
live_prompt_preview_hook = None  # set by generate.py; generation.py calls it for the live preview

# --- dashboard widget handles (step 4) ---

slot_widgets = {}        # per-API response-time labels, keyed by name (was generate.py globals())
budget_label = None
cost_label = None
dashboard_notebook = None
error_percent_label = None
refusal_percent_label = None
slop_label = None
status_bar = None
thread_status_label = None
token_label = None
user_speaking_label = None
ooc_label = None
logic_label = None
regex_watch_label = None
non_latin_label = None
# ^ dashboard widget handles — assigned in generate.py when the main window is built


# --- GUI-runtime handles: constructed in generate.py once the main window exists; None until then ---
root = None              # ttkbs.Window(...) — assigned in generate.py
db_status_widgets = None # dict of DB/Valkey status widgets — assigned in generate.py after the widgets exist
