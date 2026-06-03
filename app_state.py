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
    'incomplete_quotes': []
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
master_duplication_enabled_var = None  # tk.BooleanVar — assigned in generate.py at GUI build
live_prompt_preview_hook = None  # set by generate.py; generation.py calls it for the live preview

# --- GUI-runtime handles: constructed in generate.py once the main window exists; None until then ---
root = None              # ttkbs.Window(...) — assigned in generate.py
db_status_widgets = None # dict of DB/Valkey status widgets — assigned in generate.py after the widgets exist
