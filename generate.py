# generate.py
"""
DEVELOPER NOTE FOR LLMs:
When modifying this file, always reference AGENTS.md first for:
- File architecture and dependencies
- Key functions and their purposes
- Common tasks and which files to edit
- Thread safety patterns and state variable conventions
- API slot indices and their purposes
- Config file locations (config/config.yml, config/api_profiles.yml)

This ensures changes align with the project's established patterns and don't break existing functionality.
"""
import os
import requests
import json
import redis
import hashlib
import tkinter as tk
import ttkbootstrap as ttkbs
import psycopg2
from psycopg2 import pool
from tkinter import ttk, scrolledtext, font, messagebox, filedialog
from colorama import init, Fore, Style
import threading
from threading import Lock
import time
import sys
from queue import Queue, Empty, Full
import random
import yaml
import re
import zipfile  # For backing up output files
import shutil  # For file operations
import text_utils
import detection
from config_loader import ConfigLoader, sanitize_input
import psutil
import matplotlib
import matplotlib.ticker as ticker
import api_handler
import api_profiles
import app_state
from dataset_tasks import configure_queue_datasets
from generation import worker, check_budget_limit, estimate_time_remaining, save_generation_state
from generation import (DEFAULT_FIRST_LINE_PROMPT, DEFAULT_FIRST_LINE_STYLES, DEFAULT_GREETING_CONTEXT,
                        DEFAULT_GREETING_PROMPT, enqueue_opening_tasks,
                        opening_mode_conflict, opening_mode_mismatch,
                        DEFAULT_NOTE_PLACEMENT, note_placement, placement_cache_warning)
from dashboard import clear_dashboard, clear_dashboard_search, configure_animated_progress_styles, copy_dashboard_tab, create_modern_issue_panel, draw_issue_graph, pulse_progress_bar, search_in_dashboard_tab, update_dashboard, update_dashboard_safe, update_progress_bar_style, update_thread_status_display
from app_state import (
    global_config, API_CIRCUIT_BREAKER, api_circuit_breaker_lock, task_retry_counts, task_retry_lock, BASE_DEBUG_LOG_PATH, BASE_OUTPUT_FILE_PATH, MAX_RECENT, MAX_TASK_REQUEUES, STATE_FILE_PATH, OUTPUT_DIR, INPUT_DIR, anti_slop_counts_per_api, estimated_cost,
)
from config_editor import ConfigEditor
from ui_layout import fit_window, ScrollableFrame, ResponsiveGrid, NotebookNavigator, wrap_label
matplotlib.use('TkAgg')
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from urllib.parse import urlparse
from api_handler import RateLimiter, global_rate_limiter, get_cached_response, set_cached_response, api_response_times_per_slot, api_response_times_lock, MAX_RESPONSE_TIMES_TO_TRACK
import logging_config
from logging_config import log_message, LOG_FILE_PATH
import quality

init()


# --- Global Constants and Setup ---
from logging_config import log_message, LOG_FILE_PATH  # Import shared logger AND log file path


# Base paths for output files; actual filenames will be constructed with suffixes (e.g., _api_slot_X)
QUESTIONS_FILE_PATH = os.path.join(INPUT_DIR, 'questions.txt') # Optional file for predefined questions
os.makedirs(INPUT_DIR, exist_ok=True) # Ensure input directory exists

# --- Global Variables for Application State ---
system_prompt_counter_lock = Lock()  # For system_prompt_counter

# --- UI Spacing System ---
SPACING = 8  # Unified padding constant for all frames, widgets, and separators

# --- Statistics Counters ---
# These track various events during generation for monitoring and analysis.

# NEW: Token tracking variables

# Per-API statistics (API slots 0-4, where 0-3 are main generation, 4 is slop fixer)

# Lists to store recent occurrences of issues for display in the dashboard

# Per-API recent issues (for APIs 0-3, the main generation slots)

# Anti-Slop Statistics

# NEW: Add these timestamp tracking variables
# Share these with detection.py module
detection.issue_timestamps = app_state.issue_timestamps
detection.issue_timestamps_lock = app_state.issue_timestamps_lock
prompt_preview_text = None






# --- Resilience: requeue tasks that fail while their API host is down ---
# When a host goes down the circuit breaker (above) opens for that slot. Without
# requeueing, a worker would consume each pulled task, fail it, and discard it,
# draining the queue so nothing is left to process when the host recovers. These
# helpers let the worker put such tasks back, bounded so a genuinely-bad task
# (one that fails for content reasons while the host is up) can't loop forever.


class ToastManager:
    def __init__(self, root):
        self.root = root
        self._toasts = []
        self._offset_y = 10

    def show(self, message, level="info", duration=4000):
        colors = {
            "success": ("#28a745", "✅"),
            "error": ("#dc3545", "❌"),
            "warning": ("#ffc107", "⚠️"),
            "info": ("#17a2b8", "ℹ️")
        }
        bg_color, icon = colors.get(level, colors["info"])

        toast = tk.Toplevel(self.root)
        toast.wm_overrideredirect(True)
        toast.attributes("-topmost", True)
        toast.attributes("-alpha", 0.95)

        self.root.update_idletasks()
        rw = self.root.winfo_width()
        rh = self.root.winfo_height()
        sx = self.root.winfo_x() + rw - 320
        sy = self.root.winfo_y() + rh - 60 - self._offset_y

        toast.geometry(f"300x50+{sx}+{sy}")

        frame = tk.Frame(toast, bg=bg_color)
        frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        tk.Label(frame, text=f"{icon} {message}", bg=bg_color, fg="white",
                 font=('Segoe UI', 10), anchor="w").pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self._toasts.append(toast)
        self._offset_y += 60

        def dismiss():
            if toast.winfo_exists():
                toast.destroy()
                self._toasts.remove(toast)
                self._offset_y -= 60
        self.root.after(duration, dismiss)

# --- Crash Recovery Functions ---

def load_generation_state():
    """
    Loads a previously saved generation state from STATE_FILE_PATH.
    Prompts the user if critical configuration settings have changed since the state was saved.
    Returns True if state was successfully loaded (and user agreed to resume if incompatible), False otherwise.
    """
    global loaded_processed_tasks_snapshot
    global api_response_times_per_slot

    loaded_processed_tasks_snapshot = None # Initialize for non-duplication mode

    def load_per_api_stat(stat_name, default_val_constructor):
        """Helper to load per-API stats, converting string keys from JSON back to int"""
        loaded_stat_str_keys = state_data.get(stat_name, {str(i): default_val_constructor() for i in range(5)})
        return {int(k): v for k, v in loaded_stat_str_keys.items()}

    with app_state.state_file_lock: # Ensure thread-safe file reading
        try:
            if os.path.exists(STATE_FILE_PATH):
                with open(STATE_FILE_PATH, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                
                # Compare critical settings from saved state with current config
                saved_config_snapshot = state_data.get('config_snapshot', {})
                current_use_questions_file = global_config.get('prompts.use_questions_file')
                current_num_turns = global_config.get('generation.num_turns', 1)
                current_subject_size = global_config.get('generation.subject_size', 1000)
                current_context_size = global_config.get('generation.context_size', 3000)
                current_master_duplication_mode = global_config.get('api.master_duplication_mode', False)

                incompatible_settings = []
                if saved_config_snapshot.get('prompts.use_questions_file') != current_use_questions_file:
                    incompatible_settings.append(f"Use Questions File (Saved: {saved_config_snapshot.get('prompts.use_questions_file')}, Current: {current_use_questions_file})")
                if saved_config_snapshot.get('generation.num_turns') != current_num_turns:
                    incompatible_settings.append(f"Number of Turns (Saved: {saved_config_snapshot.get('generation.num_turns')}, Current: {current_num_turns})")
                if saved_config_snapshot.get('api.master_duplication_mode') != current_master_duplication_mode:
                    incompatible_settings.append(f"Master Duplication Mode (Saved: {saved_config_snapshot.get('api.master_duplication_mode')}, Current: {current_master_duplication_mode})")
                opening_mode_change = opening_mode_mismatch(saved_config_snapshot, global_config.get('prompts.opening_mode', False))
                if opening_mode_change:
                    incompatible_settings.append(opening_mode_change)

                # Only check subject/context size if not using questions file (as they are irrelevant otherwise)
                if not current_use_questions_file:
                    if saved_config_snapshot.get('generation.subject_size') != current_subject_size:
                        incompatible_settings.append(f"Subject Size (Saved: {saved_config_snapshot.get('generation.subject_size')}, Current: {current_subject_size})")
                    if saved_config_snapshot.get('generation.context_size') != current_context_size:
                        incompatible_settings.append(f"Context Size (Saved: {saved_config_snapshot.get('generation.context_size')}, Current: {current_context_size})")
                
                if incompatible_settings:
                    msg = "The saved state seems to be from a run with different critical settings:\n" + \
                          "\n".join([f"- {s}" for s in incompatible_settings]) + \
                          "\n\nResuming might lead to unexpected behavior or reprocessing of already completed items with new settings. " + \
                          "Do you want to attempt to resume anyway? (Choosing 'No' will start fresh, deleting the old state)."
                    if not messagebox.askyesno("Resume Incompatibility", msg):
                        log_message(f"Resumption aborted due to config incompatibility on: {', '.join(incompatible_settings)}. Old state file will be removed.", "WARNING")
                        # User chose not to resume with incompatible settings, so remove old state.
                        if os.path.exists(STATE_FILE_PATH):
                            try: os.remove(STATE_FILE_PATH)
                            except Exception as e_del: log_message(f"Could not remove incompatible state file: {e_del}", "ERROR")
                        return False # Do not load state

                # Load state data into global variables
                app_state.completed_task_ids = set(state_data.get('completed_task_ids', []))
                app_state.system_prompt_counter = state_data.get('system_prompt_counter', 0)
                app_state.quality_scores = state_data.get('quality_scores', {})
                app_state.quality_dropped = state_data.get('quality_dropped', {})
                app_state.dropped_task_ids = set(app_state.quality_dropped)
                app_state.character_counter = state_data.get('character_counter', 0)
                app_state.question_history = state_data.get('question_history', [])
                
                app_state.total_attempts_global = state_data.get('total_attempts_global', 0)
                app_state.refusal_count_total = state_data.get('refusal_count_total', 0)
                app_state.user_speaking_count_total = state_data.get('user_speaking_count_total', 0)
                app_state.slop_count_total = state_data.get('slop_count_total', 0)
                app_state.error_count_total = state_data.get('error_count_total', 0)
                app_state.ooc_count_total = state_data.get('ooc_count_total', 0)
                anti_slop_count_total = state_data.get('anti_slop_count_total', 0)
                anti_slop_counts_per_api = load_per_api_stat('anti_slop_counts_per_api', lambda: 0)

                app_state.refusal_counts_per_api = load_per_api_stat('refusal_counts_per_api', lambda: 0)
                app_state.user_speaking_counts_per_api = load_per_api_stat('user_speaking_counts_per_api', lambda: 0)
                app_state.slop_counts_per_api = load_per_api_stat('slop_counts_per_api', lambda: 0)
                app_state.error_counts_per_api = load_per_api_stat('error_counts_per_api', lambda: 0)
                app_state.ooc_counts_per_api = load_per_api_stat('ooc_counts_per_api', lambda: 0)
                app_state.total_attempts_per_api = load_per_api_stat('total_attempts_per_api', lambda: 0)

                if current_master_duplication_mode and 'api_processed_tasks_snapshot' in state_data:
                    # Convert string keys from JSON snapshot back to int for API indices
                    app_state.loaded_api_processed_tasks_snapshot = {int(k): v for k, v in state_data['api_processed_tasks_snapshot'].items()}
                elif not current_master_duplication_mode and 'processed_tasks_snapshot' in state_data:
                    loaded_processed_tasks_snapshot = state_data['processed_tasks_snapshot']
                else:
                    app_state.loaded_api_processed_tasks_snapshot = None
                    loaded_processed_tasks_snapshot = None
                
                log_message(f"Generation state loaded. {len(app_state.completed_task_ids)} unique tasks previously completed.", "INFO")
                return True # State loaded successfully
            return False # State file does not exist
        except Exception as e:
            log_message(f"Error loading generation state: {e}. Starting fresh.", "ERROR")
            reset_all_stats_and_history() # Reset everything if loading fails
            return False # Failed to load state
# --- End of Crash Recovery Functions ---


# --- Helper Functions ---
def reset_all_stats_and_history():

    """Resets all global statistics, history, and progress trackers to their initial states."""
    global loaded_processed_tasks_snapshot
    global api_response_times_per_slot

    app_state.completed_task_ids = set()
    app_state.system_prompt_counter = 0
    app_state.character_counter = 0
    app_state.question_history = []

    with api_response_times_lock:
        for i in range(6):
            api_response_times_per_slot[i] = []

    app_state.total_attempts_global = 0
    app_state.refusal_count_total = 0
    app_state.user_speaking_count_total = 0
    app_state.slop_count_total = 0
    app_state.error_count_total = 0
    app_state.ooc_count_total = 0
    app_state.logic_issue_count_total = 0
    app_state.regex_watch_count_total = 0
    anti_slop_count_total = 0

    for i in range(6): # For all 6 API slots
        app_state.refusal_counts_per_api[i] = 0
        app_state.user_speaking_counts_per_api[i] = 0
        app_state.slop_counts_per_api[i] = 0
        app_state.error_counts_per_api[i] = 0
        app_state.ooc_counts_per_api[i] = 0
        app_state.logic_issue_counts_per_api[i] = 0
        app_state.regex_watch_counts_per_api[i] = 0
        anti_slop_counts_per_api[i] = 0
        app_state.total_attempts_per_api[i] = 0
    
    for i in range(4): # For API slots 0-3 (main generation)
        app_state.recent_refusals_per_api[i] = []
        app_state.recent_user_speaking_per_api[i] = []
        app_state.recent_slop_per_api[i] = []
        app_state.recent_errors_per_api[i] = []
        app_state.recent_anti_slop_per_api[i] = []
        app_state.recent_ooc_per_api[i] = []
        app_state.recent_logic_per_api[i] = []
        app_state.recent_regex_per_api[i] = []

    app_state.recent_refusals_total = []
    app_state.recent_user_speaking_total = []
    app_state.recent_slop_total = []
    recent_anti_slop_total = []
    app_state.recent_errors_total = []
    app_state.recent_ooc_total = []
    app_state.recent_logic_total = []
    app_state.recent_regex_total = []
    with app_state.regex_watch_log_lock:
        app_state.regex_watch_log = []

    app_state.loaded_api_processed_tasks_snapshot = None # Clear any loaded snapshot
    loaded_processed_tasks_snapshot = None # Clear snapshot for non-duplication
    with app_state.quality_lock:
        app_state.quality_scores = {}
        app_state.quality_dropped = {}
        app_state.dropped_task_ids = set()
    log_message("All global statistics, history, and progress trackers have been reset.", "INFO")
    with app_state.quality_review_lock:
        app_state.quality_review_ids = set()


def cleanup_old_files_and_backup_output():
    """
    Removes old state and log files.
    Backs up all existing *.jsonl output files from the 'output' directory
    into a single timestamped zip archive, then deletes the original .jsonl files.
    This is typically called when starting a completely fresh generation run.
    """
    app_state.completed_task_ids = set() # Ensure this is reset as part of cleanup

    # Files to remove directly without backup (log and state are transient)
    files_to_remove_directly = [STATE_FILE_PATH, LOG_FILE_PATH] 
    # Also remove per-API debug logs if they exist
    for i in range(5):
        files_to_remove_directly.append(BASE_DEBUG_LOG_PATH + f"_api_slot_{i}.jsonl")
    files_to_remove_directly.append(BASE_DEBUG_LOG_PATH + ".jsonl") # Main debug log

    for f_path in files_to_remove_directly:
        if os.path.exists(f_path):
            try:
                os.remove(f_path)
                log_message(f"Removed old file: {f_path}", "INFO")
            except Exception as e_rem:
                log_message(f"Error removing old file {f_path}: {e_rem}", "WARNING")

    # Identify all .jsonl files in the output directory for backup (main output and per-API outputs)
    jsonl_files_to_backup = [
        os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR) if f.endswith(".jsonl")
    ]
    
    files_successfully_archived = []

    if not jsonl_files_to_backup:
        log_message("No .jsonl files found in output directory to backup.", "INFO")
    else:
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        backup_zip_name = os.path.join(OUTPUT_DIR, f"output_data_backup_{timestamp}.zip")
        
        try:
            with zipfile.ZipFile(backup_zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for f_path in jsonl_files_to_backup:
                    if os.path.exists(f_path): # Double check existence before adding
                        zipf.write(f_path, os.path.basename(f_path)) # Add file to zip using its base name
                        files_successfully_archived.append(f_path)
                        log_message(f"Added {f_path} to backup archive {backup_zip_name}", "INFO")
            
            log_message(f"Successfully created backup archive: {backup_zip_name}", "INFO")

            # After successful backup of all found .jsonl files, remove them
            for f_path_to_delete in files_successfully_archived:
                try:
                    os.remove(f_path_to_delete)
                    log_message(f"Removed old file {f_path_to_delete} after backup.", "INFO")
                except Exception as e_del_backup:
                    log_message(f"Error removing backed-up file {f_path_to_delete}: {e_del_backup}", "ERROR")
        except Exception as e_zip:
            log_message(f"Error creating or populating backup archive {backup_zip_name}: {e_zip}. Original .jsonl files NOT deleted.", "ERROR")




def clear_database():
    """Clears all data from the PostgreSQL generated_conversations table."""

    # Confirmation dialog: returns True for "Yes", False for "No"
    if not messagebox.askyesno("Confirm Clear Database", "Are you sure you want to clear the database? This action cannot be undone."):
        return  # User selected "No", so exit without doing anything

    if not app_state.db_pool:
        toast.show("Database pool is not initialized.", "warning")
        log_message("Database pool not initialized.", "ERROR")
        return

    try:
        conn = app_state.db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE generated_conversations RESTART IDENTITY;")
            conn.commit()
        app_state.db_pool.putconn(conn)
        toast.show("Database cleared successfully!", "success")
        log_message("Database cleared successfully.", "INFO")
    except Exception as e:
        log_message(f"Failed to clear database: {e}", "ERROR")
        toast.show(f"Failed to clear database: {e}", "error")
        if conn:
            app_state.db_pool.putconn(conn)

# --- Core Worker Logic ---

def export_db_to_jsonl(output_path):
    if not app_state.db_pool:
        toast.show("Export failed: DB pool not initialized.", "error")
        return False

    try:
        conn = app_state.db_pool.getconn()
        with conn.cursor(name='export_cursor') as cur: # Server-side cursor for large datasets
            cur.itersize = 1000
            cur.execute("SELECT conversation_data FROM generated_conversations ORDER BY created_at")

            with open(output_path, 'w', encoding='utf-8') as f:
                for row in cur:
                    f.write(json.dumps(row[0]) + '\n')
        conn.commit()
        app_state.db_pool.putconn(conn)
        return True
    except Exception as e:
        log_message(f"Export failed: {e}", "ERROR")
        if conn: app_state.db_pool.putconn(conn)
        return False

# --- Tkinter UI Update and Control Functions ---

def update_database_status():
    """Refreshes the PostgreSQL and Valkey status icons WITHOUT blocking the Tk main thread.

    The blocking health checks (psycopg2 SELECT 1, Valkey PING) run in a short-lived daemon thread;
    only the widget .config() updates are posted back to the main thread via root.after(0, ...).
    This keeps the mainloop responsive when the DB/cache is slow or unreachable -- which is what
    was intermittently tripping the X server (a stalled mainloop fails X's client-liveness check).
    """
    root = app_state.root
    if not root or not root.winfo_exists():
        return

    def _check():
        # --- PostgreSQL (blocking, off the main thread) ---
        postgres_connected = False
        postgres_active = False
        if app_state.db_pool:
            conn = None
            try:
                conn = app_state.db_pool.getconn()
                if conn:
                    postgres_connected = True
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                    postgres_active = True
            except Exception as e:
                postgres_connected = False
                postgres_active = False
                log_message(f"PostgreSQL status check failed: {e}", "DEBUG")
            finally:
                if conn is not None:
                    try:
                        app_state.db_pool.putconn(conn)
                    except Exception:
                        pass

        # --- Valkey/Redis (blocking, off the main thread) ---
        valkey_connected = False
        valkey_active = False
        if getattr(api_handler, 'valkey_client', None) is not None:
            try:
                if api_handler.valkey_client.ping():
                    valkey_connected = True
                    valkey_active = True
            except Exception as e:
                valkey_connected = False
                valkey_active = False
                log_message(f"Valkey status check failed: {e}", "DEBUG")

        pg_ok = postgres_connected and postgres_active
        vk_ok = valkey_connected and valkey_active

        def _apply():
            if not root.winfo_exists():
                return
            widgets = app_state.db_status_widgets or {}

            pi = widgets.get('postgres_icon'); ps = widgets.get('postgres_status')
            if pi and ps and pi.winfo_exists() and ps.winfo_exists():
                if pg_ok:
                    pi.config(text="✅", foreground="green"); ps.config(text="PostgreSQL: Connected & Active", foreground="green")
                elif postgres_connected:
                    pi.config(text="⚠️", foreground="orange"); ps.config(text="PostgreSQL: Connected (Inactive)", foreground="orange")
                else:
                    pi.config(text="❌", foreground="gray"); ps.config(text="PostgreSQL: Disconnected", foreground="gray")

            vi = widgets.get('valkey_icon'); vs = widgets.get('valkey_status')
            if vi and vs and vi.winfo_exists() and vs.winfo_exists():
                if vk_ok:
                    vi.config(text="✅", foreground="green"); vs.config(text="Valkey: Connected & Active", foreground="green")
                elif valkey_connected:
                    vi.config(text="⚠️", foreground="orange"); vs.config(text="Valkey: Connected (Inactive)", foreground="orange")
                else:
                    vi.config(text="❌", foreground="gray"); vs.config(text="Valkey: Disconnected", foreground="gray")

        root.after(0, _apply)

    threading.Thread(target=_check, daemon=True).start()

def _active_dashboard_tab_text():
    """Text label of the currently selected dashboard tab ('' if unavailable).

    Used to skip expensive per-tick refreshes (Treeview rebuilds, matplotlib
    redraws) for tabs the user isn't looking at. Those refreshes each allocate
    X server resources (pixmaps / GCs); run continuously for hours across every
    tab at once they eventually exhaust the X server, which drops the connection
    (``XIO: fatal IO error`` / errno 0)."""
    try:
        nb = app_state.dashboard_notebook
        selected = nb.select()
        return nb.tab(selected, "text") if selected else ""
    except Exception:
        return ""


def _record_belongs_to_task(record_id, task_id):
    """Whether an output row's id belongs to the flagged task id."""
    if not record_id or not task_id:
        return False
    return record_id == task_id or record_id.startswith(task_id + "_api")


def _review_output_files():
    """Every output JSONL file a flagged row can live in (main + per-API duplication)."""
    files = []
    output_dir = app_state.OUTPUT_DIR
    if not os.path.isdir(output_dir):
        return files
    for name in sorted(os.listdir(output_dir)):
        if name.startswith("output") and name.endswith(".jsonl"):
            files.append(os.path.join(output_dir, name))
    return files


def _iter_review_records(task_ids):
    """Yields ``(task_id, record)`` for every output row of the given task ids.

    Reads the file output and, when enabled, the database, so the Review tab works in both
    modes. Records are matched by full id, never by a truncated display value.
    """
    task_ids = list(task_ids)
    if not task_ids:
        return
    for path in _review_output_files():
        try:
            with open(path, 'r', encoding='utf-8') as src:
                for line in src:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(entry, dict):
                        continue
                    entry_id = entry.get('id', '')
                    for task_id in task_ids:
                        if _record_belongs_to_task(entry_id, task_id):
                            yield task_id, entry
                            break
        except OSError as e:
            log_message(f"Review: could not read {path}: {e}", "WARNING")

    if global_config.get('database.enabled', False) and app_state.db_pool:
        conn = None
        try:
            conn = app_state.db_pool.getconn()
            with conn.cursor() as cur:
                cur.execute("SELECT task_id, conversation_data FROM generated_conversations")
                for task_id_db, data in cur:
                    if isinstance(data, str):
                        try:
                            data = json.loads(data)
                        except json.JSONDecodeError:
                            continue
                    for task_id in task_ids:
                        if _record_belongs_to_task(task_id_db, task_id):
                            yield task_id, (data if isinstance(data, dict) else {})
                            break
        except Exception as e:
            log_message(f"Review: database read failed: {e}", "ERROR")
        finally:
            if conn is not None:
                try:
                    app_state.db_pool.putconn(conn)
                except Exception:
                    pass


def _find_review_record(task_id):
    """The output row for ``task_id`` (first match), or None when it was never written."""
    for _task, entry in _iter_review_records([task_id]):
        return entry
    return None


def _review_signature(review_ids, scores, dropped):
    """A cheap value that changes only when the Review list's content would change.

    The dashboard refreshes every 500 ms while a run is active. Rebuilding the Treeview on
    every tick deleted the user's selection and reset the detail pane, which is why reading
    a flagged conversation was impossible before v11.4. The list is rebuilt only when this
    signature actually changes.
    """
    parts = []
    for task_id in sorted(review_ids):
        score_data = scores.get(task_id)
        if not score_data:
            continue
        parts.append((task_id,
                      score_data.get("composite"),
                      score_data.get("scored_at", ""),
                      tuple(score_data.get("flags") or ())))
    return (tuple(parts), tuple(sorted(dropped.items())))


def _update_review_table(force=False):
    """Refreshes the Review tab table with currently flagged items.

    Each row's Treeview item id is the FULL task id, so selecting a row can never resolve
    to the wrong conversation (the visible Task ID cell is shortened only for display).
    The table is only rebuilt when its content changed (``force=True`` for an explicit
    Refresh click), so a per-tick refresh never clears the selection or the detail pane.
    """
    review_tab = "🔍 Review"
    if review_tab not in app_state.dashboard_notebook.tabs_widgets:
        return
    # Skip the Treeview rebuild unless the Review tab is actually on screen.
    if "Review" not in _active_dashboard_tab_text():
        return
    widgets = app_state.dashboard_notebook.tabs_widgets[review_tab]
    review_tree = widgets.get('review_tree')
    count_label = widgets.get('review_count_label')
    filter_var = widgets.get('review_filter_var')

    if not review_tree or not hasattr(review_tree, 'winfo_exists') or not review_tree.winfo_exists():
        return

    filter_text = (filter_var.get().strip().lower() if filter_var else "")

    with app_state.quality_review_lock:
        review_ids = sorted(app_state.quality_review_ids)

    with app_state.quality_lock:
        scores = dict(app_state.quality_scores)
        dropped = dict(app_state.quality_dropped)

    visible = [tid for tid in review_ids
               if (not filter_text or filter_text in tid.lower()) and scores.get(tid)]
    signature = (filter_text, _review_signature(visible, scores, dropped))
    if not force and widgets.get('review_signature') == signature:
        return
    widgets['review_signature'] = signature

    # Keep the current selection across a rebuild so the row the user is reading stays open.
    previous = review_tree.selection()
    previous_id = previous[0] if previous else None

    for item in review_tree.get_children():
        review_tree.delete(item)

    shown = 0
    for task_id in visible:
        score_data = scores[task_id]

        composite = score_data.get("composite", 0)
        dims = score_data.get("dimensions", {}) or {}
        flags = score_data.get("flags", []) or []
        scored_at = score_data.get("scored_at", "")

        # Find the lowest-scoring dimension
        lowest_dim = min(dims, key=dims.get) if dims else "N/A"
        lowest_val = dims.get(lowest_dim, 0)

        # Get source file from task metadata
        source_file = "N/A"
        with app_state.task_queue_ui_lock:
            meta = app_state.task_metadata.get(task_id, {})
            source_file = meta.get('file', 'N/A')

        display_id = task_id if len(task_id) <= 44 else task_id[:41] + "…"
        tag = "critical" if composite < 40 else "warning"
        review_tree.insert("", tk.END, iid=task_id, values=(
            display_id, f"{composite}", f"{lowest_dim} ({lowest_val})",
            ", ".join(flags[:3]), source_file, scored_at
        ), tags=(tag,))
        shown += 1

    if previous_id and review_tree.exists(previous_id):
        review_tree.selection_set(previous_id)
    elif shown:
        # Open the first row so the detail pane is never an empty box on arrival.
        first = review_tree.get_children()[0]
        review_tree.selection_set(first)
        review_tree.focus(first)
    # Render the detail for whatever is selected now (a rebuild clears the Text's binding
    # state, so this is done explicitly rather than relying on <<TreeviewSelect>>).
    _show_review_detail(review_tree)

    if count_label and hasattr(count_label, 'winfo_exists') and count_label.winfo_exists():
        text = f"{shown} flagged"
        if dropped:
            text += f"  ·  {len(dropped)} dropped by the judge (not in the dataset)"
        count_label.config(text=text)


def _format_review_conversation(record):
    """Readable transcript of one output record: ShareGPT ``conversations[]`` or ``messages[]``."""
    turns = []
    if isinstance(record, dict):
        for turn in record.get("conversations") or record.get("messages") or []:
            if not isinstance(turn, dict):
                continue
            role = turn.get("from") or turn.get("role") or "?"
            text = turn.get("value")
            if text is None:
                text = turn.get("content", "")
            label = {"human": "USER", "gpt": "ASSISTANT", "system": "SYSTEM"}.get(
                str(role).lower(), str(role).upper())
            turns.append((label, str(text)))
    return turns


def _show_review_detail(review_tree, force=False):
    """Shows the score breakdown AND the full conversation for the selected review item.

    The transcript is read back from the run's output files (or the database), so the tab
    shows what was actually written instead of pointing at a file path. Re-rendering the same
    selection is skipped unless ``force`` is set, so a refresh tick never resets the reader's
    scroll position.
    """
    review_tab = "🔍 Review"
    widgets = app_state.dashboard_notebook.tabs_widgets.get(review_tab, {})
    detail_text = widgets.get('detail_text')
    if not detail_text or not hasattr(detail_text, 'winfo_exists') or not detail_text.winfo_exists():
        return

    selected = review_tree.selection()
    if not selected:
        return
    task_id = selected[0]
    if not force and widgets.get('review_detail_task') == task_id \
            and detail_text.get("1.0", tk.END).strip():
        return
    widgets['review_detail_task'] = task_id

    with app_state.quality_lock:
        score_data = app_state.quality_scores.get(task_id, {})

    detail_text.configure(state=tk.NORMAL)
    detail_text.delete(1.0, tk.END)

    detail_text.insert(tk.END, f"TASK: {task_id}\n", "detail_head")
    if score_data:
        detail_text.insert(tk.END,
            f"SCORE: {score_data.get('composite', 'N/A')}/100   "
            f"METHOD: {score_data.get('method', 'N/A')}   "
            f"SCORED: {score_data.get('scored_at', 'N/A')}\n", "detail_meta")
        detail_text.insert(tk.END, "\nDIMENSIONS\n", "detail_head")
        for dim, val in (score_data.get('dimensions') or {}).items():
            try:
                filled = max(0, min(20, int(float(val) // 5)))
            except (TypeError, ValueError):
                filled = 0
            bar = "█" * filled + "░" * (20 - filled)
            detail_text.insert(tk.END, f"  {dim:<22}{bar} {val}\n")
        flags = score_data.get('flags') or []
        if flags:
            detail_text.insert(tk.END, f"\nFLAGS: {', '.join(flags)}\n", "detail_meta")
    else:
        detail_text.insert(tk.END, "No stored score for this task.\n", "detail_meta")

    detail_text.insert(tk.END, "\n" + "─" * 72 + "\n\n", "detail_rule")

    record = _find_review_record(task_id)
    if record is None:
        detail_text.insert(
            tk.END,
            "This conversation is not in the output files yet.\n\n"
            "Rows are written when a conversation completes; a row the quality judge "
            "rejected is never written. Check the Quality tab and output/log.txt.\n",
            "detail_meta")
    else:
        turns = _format_review_conversation(record)
        if not turns:
            detail_text.insert(tk.END, "(This output row has no conversation turns.)\n", "detail_meta")
        for label, text in turns:
            detail_text.insert(tk.END, f"{label}\n", "role_" + label.lower())
            detail_text.insert(tk.END, text.rstrip() + "\n\n")

    detail_text.configure(state=tk.DISABLED)
    detail_text.yview_moveto(0.0)


def _export_flagged_review():
    """Exports all flagged conversations to a separate JSONL file for manual review.

    Writes every flagged conversation to ``output/quality_review_flagged.jsonl``, each row
    carrying its quality verdict in ``_quality_score``. Reads the main output file, the
    per-API duplication files and the database, so the button works in every mode; the file
    is always (re)written, so a stale export can never look like a successful one.
    """
    with app_state.quality_review_lock:
        review_ids = set(app_state.quality_review_ids)

    if not review_ids:
        toast.show("No flagged items to export.", "info")
        return

    with app_state.quality_lock:
        scores = dict(app_state.quality_scores)

    export_path = os.path.join(app_state.OUTPUT_DIR, 'quality_review_flagged.jsonl')
    exported = 0
    found_ids = set()

    try:
        os.makedirs(app_state.OUTPUT_DIR, exist_ok=True)
        with open(export_path, 'w', encoding='utf-8') as dst:
            for task_id, entry in _iter_review_records(review_ids):
                # Never mutate the source record: the caller may share it with the database path.
                record = dict(entry)
                record['_quality_score'] = scores.get(task_id)
                dst.write(json.dumps(record) + '\n')
                exported += 1
                found_ids.add(task_id)
    except OSError as e:
        log_message(f"Quality review export failed: {e}", "ERROR")
        toast.show(f"Export failed: {e}", "error")
        return

    missing = sorted(review_ids - found_ids)
    if exported:
        toast.show(f"Exported {exported} flagged conversations to {export_path}", "success")
    else:
        toast.show(f"No flagged conversations found in the output files (0 written to {export_path})", "warning")
    log_message(
        f"Quality review export: {exported} items → {export_path}"
        + (f" ({len(missing)} flagged task(s) had no output row)" if missing else ""),
        "INFO")


def _dismiss_all_review():
    """Clears all review flags (user has handled them)."""
    if not messagebox.askyesno("Dismiss All", "Remove all review flags? This does NOT delete the output data."):
        return
    with app_state.quality_review_lock:
        app_state.quality_review_ids.clear()
    toast.show("All review flags dismissed.", "success")
    _update_review_table()

def update_quality_display():
    """Updates the quality metrics labels in the UI."""
    if not app_state.root.winfo_exists():
        return

    stats = quality.get_quality_stats()

    if stats["count"] == 0:
        if hasattr(app_state.quality_score_label, 'winfo_exists') and app_state.quality_score_label.winfo_exists():
            app_state.quality_score_label.config(text="N/A")
            app_state.quality_threshold_label.config(text="0")
            app_state.quality_flag_label.config(text="None")
        return

    # Composite score with color coding
    avg = stats["avg_composite"]
    if avg >= 80:
        color = "#51cf66"  # Green
    elif avg >= 60:
        color = "#fcc419"  # Amber
    else:
        color = "#ff6b6b"  # Red

    if hasattr(app_state.quality_score_label, 'winfo_exists') and app_state.quality_score_label.winfo_exists():
        app_state.quality_score_label.config(
            text=f"{avg}/100 (n={stats['count']})",
            foreground=color
        )
        app_state.quality_threshold_label.config(
            text=f"{stats['threshold_failures']} below min"
        )

    # Top flag
    if stats["flag_counts"]:
        top_flag = max(stats["flag_counts"], key=stats["flag_counts"].get)
        app_state.quality_flag_label.config(
            text=f"{top_flag} ({stats['flag_counts'][top_flag]}x)"
        )
    else:
        app_state.quality_flag_label.config(text="None")
    # Update the Review tab label with a count badge
    with app_state.quality_review_lock:
        review_count = len(app_state.quality_review_ids)

    # Find the review tab widget by its text (since we don't store the widget ref)
    review_tab_widget = None
    for tab_id in app_state.dashboard_notebook.tabs():
        if "Review" in app_state.dashboard_notebook.tab(tab_id, "text"):
            review_tab_widget = tab_id
            break

    if review_tab_widget:
        label = f"🔍 Review ({review_count})" if review_count > 0 else "🔍 Review"
        if app_state.dashboard_notebook.tab(review_tab_widget, "text") != label:
            app_state.dashboard_notebook.tab(review_tab_widget, text=label)
            dashboard_navigator.refresh()

def _rebuild_quality_roleplay_layout(force=False):
    """Rebuilds the Quality tab's summary labels and score-table columns to match the
    current ``quality.roleplay_dimensions`` toggle (Configuration Editor).

    Runs once at tab creation (``force=True``) and again, at most, whenever the toggle
    changes at runtime (tracked by ``quality_roleplay_active`` in the tab's widget
    registry). The generic six dimensions are always shown; the three roleplay
    dimensions are appended when the toggle is on.
    """
    widgets = app_state.dashboard_notebook.tabs_widgets.get("Quality", {})
    parent_frame = widgets.get("scrollable_frame")
    active = bool(global_config.get('quality.roleplay_dimensions', False))
    if not force and "quality_roleplay_active" in widgets \
            and widgets["quality_roleplay_active"] == active:
        return
    widgets["quality_roleplay_active"] = active
    dims = list(quality.active_scoring_dimensions())

    for key in ("dimension_labels",):
        widgets.pop(key, None)
    for w in ("quality_summary_lf", "quality_table_lf", "quality_tree", "dimension_layout", "table_frame"):
        old = widgets.pop(w, None)
        if isinstance(old, dict):
            continue
        if hasattr(old, "grid_forget"):
            try:
                old.grid_forget()
            except tk.TclError:
                pass
        elif isinstance(old, ttk.Frame):
            try:
                old.destroy()
            except tk.TclError:
                pass

    # Quality summary panel: one label per active dimension, reflowing 1-3 columns wide.
    quality_summary_lf = ttk.LabelFrame(parent_frame, text="Quality Summary")
    quality_summary_lf.grid(row=0, column=0, padx=SPACING, pady=SPACING, sticky="ew")
    dimension_labels = []
    dim_labels = {}
    for dim_name in dims:
        dim_label = ttk.Label(quality_summary_lf, text=f"{dim_name.title()}: N/A")
        dimension_labels.append(dim_label)
        dim_labels[dim_name] = dim_label
    widgets['dimension_labels'] = dim_labels
    widgets["dimension_layout"] = ResponsiveGrid(
        quality_summary_lf, dimension_labels, min_width=180, max_columns=3)

    # Recent-scores table: one score column per active dimension + Task ID/Flags/Method.
    quality_table_lf = ttk.LabelFrame(parent_frame, text="Recent Scores (latest 50)")
    quality_table_lf.grid(row=1, column=0, padx=SPACING, pady=SPACING, sticky="nsew")
    table_frame = ttk.Frame(quality_table_lf)
    quality_cols = ("Task ID", "Score") + tuple(d.replace("_", " ").title() for d in dims) + ("Flags", "Method")
    quality_tree = ttk.Treeview(table_frame, columns=quality_cols, show="headings", height=15)
    for col in quality_cols:
        quality_tree.heading(col, text=col)
        quality_tree.column(col, width=100, minwidth=100, stretch=False, anchor="w")
    quality_tree.column("Task ID", width=200, minwidth=160)
    quality_tree.column("Flags", width=200, minwidth=160)
    widgets["table_frame"] = table_frame
    _grid_scrollable_table(quality_table_lf, quality_tree)
    widgets["quality_tree"] = quality_tree


def _update_regex_watch_tab(force=False):
    """Populates the Regex Watch area: the configured patterns with their hit counts, and the
    recent hits with the rewrite outcome for each one.

    Gated to the visible tab (like ``_update_review_table``) and rebuilt only when its content
    actually changed, so a 500 ms refresh tick never clears the user's selection. ``force=True``
    is for the Refresh button and for a tab switch.
    """
    tab_name = "🔎 Regex Watch"
    if tab_name not in app_state.dashboard_notebook.tabs_widgets:
        return
    if "Regex Watch" not in _active_dashboard_tab_text():
        return
    widgets = app_state.dashboard_notebook.tabs_widgets[tab_name]
    pattern_tree = widgets.get('regex_pattern_tree')
    hit_tree = widgets.get('regex_hit_tree')
    if (pattern_tree is None or not pattern_tree.winfo_exists()
            or hit_tree is None or not hit_tree.winfo_exists()):
        return

    configured = detection.validate_regex_patterns(
        global_config.get('detection.regex_watch.patterns', []) or [])[0]
    enabled = bool(global_config.get('detection.regex_watch.enabled', False))
    with app_state.regex_watch_log_lock:
        log_entries = list(app_state.regex_watch_log)

    # Aggregate the log per pattern: total hits and the most recent status.
    per_pattern = {}
    for pattern, sentence, api_slot_idx, status in log_entries:
        entry = per_pattern.setdefault(pattern, {"hits": 0, "status": ""})
        entry["hits"] += 1
        entry["status"] = status

    signature = (enabled, tuple(configured), tuple(log_entries[-40:]))
    if not force and widgets.get('regex_signature') == signature:
        return
    widgets['regex_signature'] = signature

    status_label = getattr(app_state, 'regex_watch_status_label', None)
    if status_label is not None and status_label.winfo_exists():
        if enabled:
            status_label.configure(
                text="Regex Watch is ON with %d pattern(s). A hit is rewritten by the main API "
                     "slot that produced the text, then re-checked." % len(configured),
                foreground="lightgray")
        else:
            status_label.configure(
                text="Regex Watch is off. Enable it in the Configuration Editor (Detection tab) "
                     "to rewrite regex-shaped prose.",
                foreground="#ffb86b")

    for item in pattern_tree.get_children():
        pattern_tree.delete(item)
    if not configured:
        pattern_tree.insert("", tk.END, values=("-", 0, "no patterns",
                                              "Add patterns in the Configuration Editor."),
                            tags=("unresolved",))
    for index, pattern in enumerate(configured, start=1):
        stats = per_pattern.get(pattern, {"hits": 0, "status": "-"})
        pattern_tree.insert("", tk.END,
                            values=(index, stats["hits"], stats["status"], pattern),
                            tags=(stats["status"],) if stats["status"] in ("rewritten", "unresolved") else ())

    for item in hit_tree.get_children():
        hit_tree.delete(item)
    for pattern, sentence, api_slot_idx, status in log_entries[-40:]:
        hit_tree.insert("", tk.END,
                        values=(time.strftime("%H:%M:%S"), f"Slot {api_slot_idx + 1}",
                                status, pattern[:120], sentence[:200]),
                        tags=(status,) if status in ("rewritten", "unresolved") else ())
    children = hit_tree.get_children()
    if children:
        hit_tree.see(children[-1])


def update_quality_tab():
    """Populates the Quality dashboard tab: per-dimension summary labels and the
    recent-scores table (latest 50).

    Like the other per-tick Treeview rebuilds, the work is gated to the currently
    selected tab to keep X server resource use bounded (see
    ``_active_dashboard_tab_text``). The per-dimension labels/columns follow the
    ``quality.roleplay_dimensions`` toggle (rebuilt on change).
    """
    if not app_state.root.winfo_exists():
        return

    # Skip the rebuild unless the Quality tab is actually on screen.
    if _active_dashboard_tab_text() != "Quality":
        return

    widgets = app_state.dashboard_notebook.tabs_widgets.get("Quality", {})

    # Pick up a roleplay-dimensions toggle change made in the Config Editor mid-run.
    _rebuild_quality_roleplay_layout()

    quality_tree = widgets.get("quality_tree")
    if not quality_tree or not hasattr(quality_tree, 'winfo_exists') or not quality_tree.winfo_exists():
        return

    dim_labels = widgets.get("dimension_labels", {})

    # --- Quality Summary: average per dimension ---
    stats = quality.get_quality_stats()
    if stats["count"] == 0:
        for dim_name, label in dim_labels.items():
            label.config(text=f"{dim_name.title()}: N/A")
    else:
        for dim_name, label in dim_labels.items():
            avg = stats["dimension_averages"].get(dim_name, 0)
            bar = "█" * int(avg // 5) + "░" * (20 - int(avg // 5))
            label.config(text=f"{dim_name.title()}: {avg} {bar}")

    # --- Recent Scores table (latest 50) ---
    for item in quality_tree.get_children():
        quality_tree.delete(item)

    with app_state.quality_lock:
        scores = app_state.quality_scores

    latest = sorted(scores.items(), key=lambda kv: kv[1].get("scored_at", ""), reverse=True)[:50]
    # Column order mirrors active_scoring_dimensions(); scored rows predate the new dims
    # (or were scored with the toggle off) and simply show N/A for the missing columns.
    for task_id, score_data in latest:
        dim_scores = score_data.get("dimensions", {})
        row_dims = [dim_scores.get(d, "N/A") for d in quality.active_scoring_dimensions()]
        quality_tree.insert("", tk.END, values=(
            task_id[:50],
            score_data.get("composite", "N/A"),
            *row_dims,
            ", ".join(score_data.get("flags", [])[:4]),
            score_data.get("method", "N/A"),
        ))

def update_live_prompt_preview(messages_list, metadata=None):
    """Thread-safe function to update the Rich Prompt Viewer from worker threads."""
    if dashboard_pause_var.get():
        return

    if not app_state.root.winfo_exists() or prompt_preview_text is None:
        return

    if metadata is None:
        metadata = {}

    def _apply_update():
        if not app_state.root.winfo_exists() or prompt_preview_text is None:
            return
        if dashboard_pause_var.get():
            return

        # --- Update Metadata Header ---
        if prompt_preview_meta_var is not None:
            thread_id = metadata.get('thread_id', 'N/A')
            api_slot = metadata.get('api_slot_idx', 'N/A')
            msg_type = metadata.get('message_type', 'Unknown')
            attempt = metadata.get('attempt', 'N/A')
            timestamp = metadata.get('timestamp', time.strftime('%H:%M:%S'))
            prompt_preview_meta_var.set(
                f"Thread: {thread_id}  |  API Slot: {api_slot}  |  Type: {msg_type}  |  "
                f"Attempt: {attempt}  |  {timestamp}"
            )

        # --- Update Message Count & Token Estimate ---
        if prompt_preview_stats_var is not None:
            msg_count = len(messages_list)
            total_chars = sum(len(m.get('content', '')) for m in messages_list)
            approx_tokens = total_chars // 4  # Rough 4 chars per token
            prompt_preview_stats_var.set(
                f"Messages: {msg_count}  |  Chars: {total_chars:,}  |  Est. Tokens: ~{approx_tokens:,}"
            )

        # --- Build Rich Text Content ---
        prompt_preview_text.config(state=tk.NORMAL)
        prompt_preview_text.delete(1.0, tk.END)

        for i, msg in enumerate(messages_list):
            role = msg.get('role', 'unknown')
            content = msg.get('content', '')

            # Add a separator between messages (except before the first)
            if i > 0:
                prompt_preview_text.insert(tk.END, "\n" + "─" * 80 + "\n\n", "separator")

            # Role header
            role_tag = f"{role}_role"
            prompt_preview_text.insert(tk.END, f"📌 {role.upper()}", role_tag)
            prompt_preview_text.insert(tk.END, "\n", "separator")

            # Content (indented)
            content_tag = f"{role}_content"
            indented_content = content
            # Simple indent for readability
            lines = indented_content.split('\n')
            indented_lines = [f"   {line}" for line in lines]
            prompt_preview_text.insert(tk.END, "\n".join(indented_lines), content_tag)
            prompt_preview_text.insert(tk.END, "\n")

        # Auto-scroll if not paused
        if not prompt_preview_autoscroll_var.get():
            prompt_preview_text.see(tk.END)

        prompt_preview_text.config(state=tk.DISABLED)

    # Schedule UI update on the main Tkinter thread
    app_state.root.after(0, _apply_update)

app_state.live_prompt_preview_hook = update_live_prompt_preview

def generation_running():
    """True while any worker thread of the current run is alive."""
    return bool(app_state.threads) and any(t.is_alive() for t in app_state.threads if t)

def start_processing():
    """Initiates the data generation process based on current configurations."""
    global loaded_processed_tasks_snapshot

    # A second start while workers are alive would replace the queue and spawn a second set of workers
    if generation_running():
        log_message("Start Generation ignored: a generation run is already in progress.", "WARNING")
        toast.show("Generation is already running.", "warning")
        start_button.config(state=tk.DISABLED)
        return

    global_config.load() # Ensure latest config.yml is loaded
    log_message("DEBUG: start_processing() function has been called.", "INFO")

    # --- Initialize Valkey Connection ---
    if global_config.get('valkey.enabled', True):
        try:
            valkey_host = global_config.get('valkey.host', 'localhost')
            valkey_port = global_config.get('valkey.port', 6379)
            valkey_db = global_config.get('valkey.db', 0)
            valkey_password = global_config.get('valkey.password')

            log_message(f"Valkey config - Host: {valkey_host}, Port: {valkey_port}, DB: {valkey_db}", "DEBUG")

            # Create the client and assign to api_handler module
            api_handler.valkey_client = redis.Redis(
                host=valkey_host,
                port=valkey_port,
                db=valkey_db,
                password=valkey_password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True
            )

            # Test connection immediately
            try:
                ping_result = api_handler.valkey_client.ping()
                log_message(f"Valkey PING result during init: {ping_result}", "INFO")
                if ping_result:
                    log_message("✅ Valkey connected successfully during initialization.", "INFO")
                else:
                    log_message("⚠️ Valkey PING returned False", "WARNING")
                    api_handler.valkey_client = None
            except redis.ConnectionError as ce:
                log_message(f"Valkey connection error during init: {ce}", "ERROR")
                api_handler.valkey_client = None
            except redis.TimeoutError as te:
                log_message(f"Valkey timeout error during init: {te}", "ERROR")
                api_handler.valkey_client = None
            except Exception as e:
                log_message(f"Valkey unexpected error during init: {e}", "ERROR")
                api_handler.valkey_client = None

        except Exception as e:
            log_message(f"Failed to initialize Valkey client: {e}. Caching will be disabled.", "WARNING")
            api_handler.valkey_client = None
    else:
        api_handler.valkey_client = None
        log_message("Valkey caching is disabled in config.", "INFO")

    # Update status after Valkey initialization - give it time to complete
    if app_state.root.winfo_exists():
        app_state.root.after(500, update_database_status)  # Increased delay to 500ms
    # --- End Valkey Initialization ---

    should_resume = False # ✅ Correctly indented (function scope)
    if os.path.exists(STATE_FILE_PATH):
        if load_generation_state():
            should_resume = True
            log_message("Auto-resuming previous generation (config matched).", "INFO")
        else:
            log_message("State incompatible or failed to load. Starting fresh.", "WARNING")
            cleanup_old_files_and_backup_output()
            reset_all_stats_and_history()
    else:
        log_message("No state file found. Starting fresh.", "INFO")
        cleanup_old_files_and_backup_output()
        reset_all_stats_and_history()

    # --- Load API Configurations ---
    all_apis_config_from_yml = global_config.get('api.apis', [])
    if not all_apis_config_from_yml or not isinstance(all_apis_config_from_yml, list):
        toast.show("API configuration missing or malformed.", "error")
        log_message("API configuration missing/malformed in config.yml.", "ERROR"); return

    all_api_configs_runtime = [] 
    active_enabled_api_configs_for_worker_list = [] 

    for i in range(len(all_apis_config_from_yml)):
        if i >= 6: break
        api_conf_yml = all_apis_config_from_yml[i] if isinstance(all_apis_config_from_yml[i], dict) else {}
        api_runtime = {
            'url': os.getenv(f'API_URL_{i+1}', api_conf_yml.get('url', '')),
            'model': os.getenv(f'MODEL_NAME_{i+1}', api_conf_yml.get('model', '')),
            'key': os.getenv(f'API_KEY_{i+1}', api_conf_yml.get('key', '')),
            'sampler_settings': global_config.get('samplers', {}),
            'threads': api_conf_yml.get('threads', 10),  # Get threads from API config
            'rate_limit_rpm': api_conf_yml.get('rate_limit_rpm', 60),
            'api_profile': os.getenv(f'API_PROFILE_{i+1}', api_conf_yml.get('api_profile', api_profiles.DEFAULT_PROFILE)),
            'custom_allowed_params': api_conf_yml.get('custom_allowed_params', [])
        }
        enabled_in_config = api_conf_yml.get('enabled', (i==0)) 
        api_runtime['enabled'] = enabled_in_config
        
        if i < 4: 
            if enabled_in_config and api_runtime['url']:  # Removed key requirement to allow APIs without keys
                active_enabled_api_configs_for_worker_list.append({'config': api_runtime, 'original_slot_idx': i})
        elif i == 4:
            api_runtime['sampler_settings'] = global_config.get('samplers.slop_fixer_params', global_config.get('samplers', {}))
        elif i == 5:
            api_runtime['sampler_settings'] = global_config.get('samplers.anti_slop_params', global_config.get('samplers.slop_fixer_params', global_config.get('samplers', {})))
        all_api_configs_runtime.append(api_runtime)
    
    while len(all_api_configs_runtime) < 6:
        all_api_configs_runtime.append({'enabled': False, 'url':'', 'model':'', 'key':'', 'sampler_settings':{}})

    # Register each slot's API compatibility profile so generation.py can trim outgoing payloads
    # to what that endpoint accepts (see api_profiles.py / config/api_profiles.yml).
    api_profiles.load_profiles()
    api_profiles.reset_slot_profiles()
    for slot_idx, conf in enumerate(all_api_configs_runtime[:6]):
        resolved = api_profiles.set_slot_profile(
            slot_idx, conf.get('api_profile'), conf.get('custom_allowed_params'), conf.get('url'))
        if conf.get('url'):
            log_message(f"API Slot {slot_idx+1} compatibility profile: {resolved}", "INFO")

    master_duplication_enabled = app_state.master_duplication_enabled_var.get() 

    if not master_duplication_enabled and not active_enabled_api_configs_for_worker_list:
        toast.show("No APIs enabled/configured for non-duplication mode.", "error")
        log_message("Non-Duplication: No APIs 1-4 enabled/configured for work.", "ERROR"); return
    
    slop_fixer_api_config_runtime = all_api_configs_runtime[4]
    # NEW: Define Anti-Slop Fixer API config (Slot 6, index 5)
    anti_slop_fixer_api_config_runtime = all_api_configs_runtime[5]
    # Define the param variable to match the runtime one
    anti_slop_fixer_api_config_param = anti_slop_fixer_api_config_runtime
    if slop_fixer_api_config_runtime.get('url') and not slop_fixer_api_config_runtime.get('model'):
        log_message("Warning: Slop Fixer API URL (API Slot 5) is set, but Model is missing. Sentence-level slop fixing will be disabled.", "WARNING")
        slop_fixer_api_config_runtime['url'] = None 
    elif not slop_fixer_api_config_runtime.get('url'):
        log_message("Info: Slop Fixer API (API Slot 5) not configured. Fallback system prompt slop handling will be used if slop is detected.", "INFO")

    log_message(f"Master Duplication Mode (from UI Var): {master_duplication_enabled}", "INFO")
    if not master_duplication_enabled:
        log_message(f"Non-Duplication Mode: {len(active_enabled_api_configs_for_worker_list)} active APIs for collaborative work.", "INFO")
    else: 
        num_enabled_dup_apis = sum(1 for idx, conf in enumerate(all_api_configs_runtime) if idx < 4 and conf.get('enabled') and conf.get('url'))
        if num_enabled_dup_apis == 0:
            toast.show("Duplication mode on, but no APIs configured.", "error")
            log_message("Duplication mode on, but no APIs 0-3 enabled/configured for work.", "ERROR"); return
        log_message(f"Duplication Mode: {num_enabled_dup_apis} APIs (Slots 1-4) will duplicate tasks.", "INFO")


    # Apply rate limits from config to the global rate limiter
    for i in range(6):
        if i < len(all_api_configs_runtime):
            rpm = all_api_configs_runtime[i].get('rate_limit_rpm', 60)
            global_rate_limiter.set_rate_limit(i, rpm)
            log_message(f"API Slot {i+1} rate limit set to {rpm} RPM from config", "INFO")


    # --- Load Generation and Prompt Configurations ---
    try:
        # Get threads from config if available, otherwise use UI value
        config_threads = global_config.get('api.threads')
        if config_threads is not None:
            app_state.num_threads = config_threads
            num_threads_var.set(str(app_state.num_threads))  # Update UI to match config
        else:
            app_state.num_threads = int(num_threads_var.get())
        
        if app_state.num_threads <=0: raise ValueError("Number of threads must be positive.")
    except ValueError:
        toast.show("Invalid thread count specified.", "error")
        log_message(f"Invalid number of threads in UI: {num_threads_var.get()}", "ERROR"); return

    subject_size_conf = global_config.get('generation.subject_size', 1000)
    context_size_conf = global_config.get('generation.context_size', 3000)
    current_max_attempts = global_config.get('generation.max_attempts', global_config.get('samplers.max_attempts', 5))
    current_history_size = global_config.get('generation.history_size', global_config.get('samplers.history_size',10))
    current_remove_reasoning = global_config.get('generation.remove_reasoning', False)
    current_remove_em_dash = global_config.get('generation.remove_em_dash', False)
    current_remove_asterisks = global_config.get('generation.remove_asterisks', False) # NEW
    current_remove_asterisk_space_asterisk = global_config.get('generation.remove_asterisk_space_asterisk', False) #
    current_remove_all_asterisks = global_config.get('generation.remove_all_asterisks', False) # NEW ADDITION
    current_remove_markdown = global_config.get('generation.remove_markdown', False)
    current_ensure_space_after_line_break = global_config.get('generation.ensure_space_after_line_break', False) # NEW
    current_output_format = 'sharegpt'
    current_num_turns = global_config.get('generation.num_turns', 1) 
    if current_num_turns <= 0: current_num_turns = 1 

    current_use_questions_file = global_config.get('prompts.use_questions_file', False)
    current_use_variable_system = global_config.get('prompts.system.variable', False)
    active_gender = global_config.get('gender', 'female') 
    default_question_prompt = "Generate a question based on the provided text. Recent questions to avoid: {recent_questions}\n\nSubject: {subject}\n\nContext: {context}"
    current_question_prompt = global_config.get('prompts.question', default_question_prompt)
    if not current_question_prompt or not current_question_prompt.strip():
        current_question_prompt = default_question_prompt
        log_message("Question prompt was empty in config. Using default prompt.", "WARNING")
    current_question_prompt = global_config.get('prompts.question', "Generate a question based on the provided text. Recent questions to avoid: {recent_questions}\n\nSubject: {subject}\n\nContext: {context}")
    current_answer_prompt = global_config.get('prompts.answer', "Provide an answer to the last question.")
    current_api_request_timeout = global_config.get('generation.api_request_timeout', 300)
    current_lore = global_config.get('prompts.lore', '')
    # Opening mode: samples start with the character's greeting and a first line from the user's seat
    current_opening_mode = bool(global_config.get('prompts.opening_mode', False))
    current_greeting_prompt = global_config.get('prompts.greeting', DEFAULT_GREETING_PROMPT)
    if not current_greeting_prompt or not current_greeting_prompt.strip():
        current_greeting_prompt = DEFAULT_GREETING_PROMPT
        if current_opening_mode: log_message("Greeting prompt was empty in config. Using default prompt.", "WARNING")
    current_first_line_prompt = global_config.get('prompts.first_line', DEFAULT_FIRST_LINE_PROMPT)
    if not current_first_line_prompt or not current_first_line_prompt.strip():
        current_first_line_prompt = DEFAULT_FIRST_LINE_PROMPT
        if current_opening_mode: log_message("First line prompt was empty in config. Using default prompt.", "WARNING")
    current_greeting_context = global_config.get('prompts.greeting_context', DEFAULT_GREETING_CONTEXT)
    if not isinstance(current_greeting_context, str) or not current_greeting_context.strip():
        current_greeting_context = DEFAULT_GREETING_CONTEXT
        if current_opening_mode: log_message("Greeting context was empty in config. Using default.", "WARNING")
    current_first_line_styles = global_config.get('prompts.first_line_styles', list(DEFAULT_FIRST_LINE_STYLES))
    if isinstance(current_first_line_styles, str):
        current_first_line_styles = [current_first_line_styles]
    current_first_line_styles = [str(s).strip() for s in (current_first_line_styles or []) if str(s).strip()]
    if not current_first_line_styles:
        current_first_line_styles = list(DEFAULT_FIRST_LINE_STYLES)
        if current_opening_mode: log_message("First line styles were empty in config. Using defaults.", "WARNING")
    try:
        current_placeholder_share = min(1.0, max(0.0, float(global_config.get('generation.placeholder_share', 0.0) or 0.0)))
    except (TypeError, ValueError):
        log_message("generation.placeholder_share is not a number. Using 0.", "WARNING")
        current_placeholder_share = 0.0
    app_state.quality_enabled = global_config.get('quality.enabled', True)
    app_state.quality_output_filter = global_config.get('quality.output_filter', False)
    app_state.use_conversation_history_for_fixers = global_config.get('generation.use_conversation_history_for_fixers', True)
    # Per-turn internal-logic gate (v10.8.0): when on, every freshly generated assistant
    # reply is checked by the quality LLM for objective contradictions (sexed anatomy vs.
    # stated gender, profile-contradicting body parts/clothing, impossible timelines). A
    # failing turn is sent back to the SAME main API with the original system prompt, full
    # conversation context and the judge's specific findings, so the model knows exactly
    # what to fix instead of guessing from a bare hint. Requires a configured quality LLM
    # (falls back to the primary API, like conversation-level scoring); off by default.
    per_turn_logic_check = bool(
        global_config.get('quality.enabled', True)
        and global_config.get('quality.per_turn_logic_check', False)
    )
    try:
        per_turn_logic_max_rewrites = max(0, int(global_config.get('quality.logic_max_rewrites', 1)))
    except (TypeError, ValueError):
        per_turn_logic_max_rewrites = 1
    log_message(f"Per-turn logic gate: enabled={per_turn_logic_check}, "
                f"max_rewrites={per_turn_logic_max_rewrites}", "INFO")
    include_metadata = global_config.get('generation.include_metadata', True)
    # v11.7: flat style_* provenance columns inside the metadata block (scotoma measurements
    # via style_metrics); off by default so the default record stays small. It only makes
    # sense when the metadata block itself is on, so it inherits its gate.
    include_style_metrics = bool(
        include_metadata and global_config.get('generation.include_style_metrics', False))
    log_message(f"Quality scoring: enabled={app_state.quality_enabled}, output_filter={app_state.quality_output_filter}", "INFO")
    log_message(f"Conversation history for fixers: enabled={app_state.use_conversation_history_for_fixers}", "INFO")
    log_message(f"Output metadata block: enabled={include_metadata}, style_metrics={include_style_metrics}", "INFO")
    if current_lore:
        log_message(f"Lore loaded ({len(current_lore)} chars).", "INFO")
    else:
        log_message("No lore configured.", "INFO")
    character_config = global_config.get('prompts.character', {})
    enable_character_engine_local = character_config.get('enabled', True)
    enable_class_selection_local = character_config.get('class_enabled', False)
    enable_setting_selection_local = character_config.get('setting_enabled', False)
    # Persona breakthrough: which assistant slot (1 = first reply) starts injecting the inner persona.
    # Slot 1 is always outward-behaviour-only; the persona 'breaks through' from this slot onward.
    try:
        persona_breakthrough_turn = max(1, int(character_config.get('persona_breakthrough_turn', 2)))
    except (TypeError, ValueError):
        persona_breakthrough_turn = 2

    num_characters_local = character_config.get('num_characters', 1)  # Default to 1 for backward compatibility
    if num_characters_local < 1:
        num_characters_local = 1

    # Load the new list-of-dicts format
    character_list = character_config.get('characters', [])

    # Fallback for old config format (separate lists)
    if not character_list:
        old_names = character_config.get('name', [])
        old_ages = character_config.get('age', [])
        old_genders = character_config.get('gender', [])
        old_races = character_config.get('race', [])
        old_jobs = character_config.get('job', [])
        old_clothing = character_config.get('clothing', [])
        old_jewelry = character_config.get('jewelry', [])
        old_appearance = character_config.get('appearance', [])
        old_backstory = character_config.get('backstory', [])
        old_persona = character_config.get('persona', [])
        old_personality = character_config.get('personality', [])
        old_setting = character_config.get('setting', [])
        old_class = character_config.get('class', [])

        if any([old_names, old_races, old_jobs, old_clothing, old_jewelry, old_appearance, old_backstory, old_persona, old_personality, old_setting, old_class, old_genders]):
            max_len = max(
                len(old_names), len(old_races), len(old_jobs), len(old_clothing),
                len(old_jewelry), len(old_appearance), len(old_backstory),
                len(old_persona), len(old_personality), len(old_setting), len(old_class), len(old_genders)
            )
            for i in range(max_len):
                character_list.append({
                    'name': old_names[i] if i < len(old_names) else '',
                    'age': old_ages[i] if i < len(old_ages) else '',
                    'gender': old_genders[i] if i < len(old_genders) else '',
                    'race': old_races[i] if i < len(old_races) else '',
                    'job': old_jobs[i] if i < len(old_jobs) else '',
                    'clothing': old_clothing[i] if i < len(old_clothing) else '',
                    'jewelry': old_jewelry[i] if i < len(old_jewelry) else '',
                    'appearance': old_appearance[i] if i < len(old_appearance) else '',
                    'backstory': old_backstory[i] if i < len(old_backstory) else '',
                    'persona': old_persona[i] if i < len(old_persona) else '',
                    'personality': old_personality[i] if i < len(old_personality) else '',
                    'setting': old_setting[i] if i < len(old_setting) else '',
                    'class': old_class[i] if i < len(old_class) else ''
                })
            log_message(f"Converted old character config format ({max_len} characters) to new format.", "INFO")
    current_user_continuation_prompt = global_config.get('prompts.user_continuation_prompt', "Continue the conversation naturally based on the assistant's last response: {last_assistant_message}")
    # --- NEW: Load Top Level System Prompt ---
    current_top_level_system_prompt = global_config.get('prompts.system.top_level_system_prompt', '')

    # NEW: Load emotional states configuration
    emotional_states_config = global_config.get('prompts.emotional_states', {})
    enable_emotional_states = emotional_states_config.get('enabled', False)
    emotional_states_list = emotional_states_config.get('states', [])
    # When on, a multi-character scene gives each character its own random state/arc instead of one shared.
    per_char_random_emotions = bool(emotional_states_config.get('per_character', False))

    if enable_emotional_states and not emotional_states_list:
        log_message("Warning: Emotional states enabled but no states defined. Disabling emotional states.", "WARNING")
        enable_emotional_states = False
        emotional_states_list = []

    log_message(f"Emotional states enabled: {enable_emotional_states}, States: {emotional_states_list}", "INFO")

    # NEW (v10.6.0): Load user-side emotional states configuration. Same flat-state / '->' arc format as
    # the character's states, but steered onto the SIMULATED USER's messages (opening question/first line
    # and each user continuation) instead of the character's replies. Off by default; optional in the GUI.
    user_emotional_states_config = global_config.get('prompts.user_emotional_states', {})
    enable_user_emotional_states = user_emotional_states_config.get('enabled', False)
    user_emotional_states_list = user_emotional_states_config.get('states', [])

    if enable_user_emotional_states and not user_emotional_states_list:
        log_message("Warning: User emotional states enabled but no states defined. Disabling user emotional states.", "WARNING")
        enable_user_emotional_states = False
        user_emotional_states_list = []

    log_message(f"User emotional states enabled: {enable_user_emotional_states}, States: {user_emotional_states_list}", "INFO")

    # Where the persona breakthrough and the emotional state are sent: 'depth0' (after the history)
    # or 'system' (end of the reply's system prompt).
    persona_placement = note_placement(character_config.get('persona_breakthrough_placement'))
    if persona_placement is None:
        log_message("prompts.character.persona_breakthrough_placement is not 'depth0' or 'system'. Using depth0.", "WARNING")
        persona_placement = DEFAULT_NOTE_PLACEMENT
    emotion_placement = note_placement(emotional_states_config.get('placement'))
    if emotion_placement is None:
        log_message("prompts.emotional_states.placement is not 'depth0' or 'system'. Using depth0.", "WARNING")
        emotion_placement = DEFAULT_NOTE_PLACEMENT

    # NEW: Load random names configuration
    random_names_config = global_config.get('prompts.random_names', {})
    enable_random_names = random_names_config.get('enabled', False)
    random_names_list = random_names_config.get('names', [])

    if enable_random_names and not random_names_list:
        log_message("Warning: Random names enabled but no names defined. Disabling random names.", "WARNING")
        enable_random_names = False
        random_names_list = []

    log_message(f"Random names enabled: {enable_random_names}, Names count: {len(random_names_list)}", "INFO")

    # NEW: Load random settings configuration (pool of settings sampled when a character's own
    # setting is empty; also used to unify settings across multi-character ensembles).
    random_settings_config = global_config.get('prompts.random_settings', {})
    enable_random_settings = random_settings_config.get('enabled', False)
    random_settings_list = random_settings_config.get('settings', [])

    if enable_random_settings and not random_settings_list:
        log_message("Warning: Random settings enabled but no settings defined. Disabling random settings.", "WARNING")
        enable_random_settings = False
        random_settings_list = []

    log_message(f"Random settings enabled: {enable_random_settings}, Settings count: {len(random_settings_list)}", "INFO")

    base_sys_prompt = global_config.get('prompts.system.base', "You are a helpful assistant.")
    app_state.system_prompts_list = []
    if current_use_variable_system:
        app_state.system_prompts_list = global_config.get('prompts.system.variations', [])
    if not app_state.system_prompts_list:
        log_message("Warning: Use variable system prompts ON, but no variations in config. Using base system prompt.", "WARNING")
        app_state.system_prompts_list = [base_sys_prompt]
    # Fixed: Removed the else block that was overwriting variations with the base prompt
    # Optional: Uncomment the line below if you want the base prompt to also be an option in the random selection pool
    # system_prompts_list.append(base_sys_prompt)

    if not any(p.strip() for p in app_state.system_prompts_list):
        log_message("Warning: No valid system prompts loaded (all empty). Using a default.", "WARNING")
        app_state.system_prompts_list = ["You are a helpful assistant."]

    # --- Load Detection Configurations ---
    current_refusal_phrases = global_config.get('detection.refusal.phrases', [])
    user_speaking_gender_config = global_config.get(f'detection.user_speaking.{active_gender}', {}) 
    current_user_speaking_phrases = user_speaking_gender_config.get('phrases', [])
    current_speaking_fixes = user_speaking_gender_config.get('fixes', [])

    current_anti_slop_phrases = global_config.get('detection.anti_slop.phrases', [])
    current_anti_slop_fixes = global_config.get('detection.anti_slop.fixes', [])

    current_ooc_phrases = global_config.get('detection.out_of_character.phrases', [])
    current_ooc_fixes = global_config.get('detection.out_of_character.fixes', [])
    current_ooc_enabled = bool(global_config.get('detection.out_of_character.enabled', True))

    # v11.6 Regex Watch: user-editable regular expressions for rhetorical shapes. An invalid
    # pattern is dropped here (with a log line) rather than silently skipped on every reply.
    current_regex_patterns, _regex_pattern_errors = detection.validate_regex_patterns(
        global_config.get('detection.regex_watch.patterns', []) or [])
    for _bad_pattern, _bad_reason in _regex_pattern_errors:
        log_message(f"Regex Watch: ignoring invalid pattern '{_bad_pattern[:60]}': {_bad_reason}", "WARNING")
    current_regex_watch_enabled = bool(global_config.get('detection.regex_watch.enabled', False))
    try:
        current_regex_watch_max_rounds = int(global_config.get('detection.regex_watch.max_rounds', 3))
    except (TypeError, ValueError):
        current_regex_watch_max_rounds = 3
    current_regex_watch_max_rounds = max(1, min(5, current_regex_watch_max_rounds))

    current_slop_phrases = global_config.get('detection.slop.phrases', [])
    current_jailbreaks = global_config.get('detection.refusal.fixes', []) 
    current_slop_fixes_fallback = global_config.get('detection.slop.fixes', []) 
    current_slop_fixes_for_rotation = global_config.get('detection.slop.fixes', [])
    current_slop_to_anti_slop_fallback = global_config.get('generation.slop_to_anti_slop_fallback', False)

    # v11.7.1 "Experiment: Retry missing quotations" (contraction-safe since v11.7.2 -- the
    # detector discounts word-internal apostrophes, so "It's not funny she says" is caught):
    # when a generated reply comes back with a
    # spoken line but NO quotation marks at all ("Its funny she says"), send that same answer
    # back to the SAME main API for one retry before keeping it. Off by default.
    current_retry_missing_quotations = bool(global_config.get('generation.retry_missing_quotations', False))

    # --- Initialize Processing State ---
    app_state.stop_processing = False 
    app_state.pause_processing = False 
    app_state.processing_active = True 
    update_dashboard() 

    log_message(f"Start processing: {app_state.num_threads} threads. Output: {current_output_format}. Turns: {current_num_turns}. Remove Reasoning: {current_remove_reasoning}. Gender: {active_gender}. Use QFile: {current_use_questions_file}. Use VarSys: {current_use_variable_system}", "INFO")
    if should_resume: log_message(f"Resuming with {len(app_state.completed_task_ids)} previously completed unique tasks.", "INFO")

    for widget in progress_frame.winfo_children(): 
        widget.destroy()
    app_state.task_queue = Queue() 
    app_state.task_queue.api_widgets = {} 

    # Optional task attachments. Configuration only: readers open on a worker's
    # first bind. The existing queue, task IDs, retries and recovery stay in charge.
    try:
        configure_queue_datasets(app_state.task_queue, global_config.get('datasets', {}))
    except ValueError as exc:
        toast.show(f"Dataset configuration: {exc}", "error")
        log_message(f"Dataset configuration: {exc}", "ERROR")
        app_state.processing_active = False; start_button.config(state=tk.NORMAL); return

    # --- Define Number of Tasks to Generate (New) ---
    NUM_RANDOM_CHUNKS = global_config.get('generation.num_random_chunks', 12000) # Change this number to generate more or fewer tasks per run

    opening_conflict = opening_mode_conflict(current_opening_mode, current_use_questions_file, master_duplication_enabled)
    if opening_conflict:
        toast.show(opening_conflict, "error")
        log_message(opening_conflict, "ERROR")
        app_state.processing_active = False; start_button.config(state=tk.NORMAL); return

    cache_loss, cache_warning = placement_cache_warning(
        current_num_turns, current_opening_mode, persona_breakthrough_turn,
        enable_character_engine_local and any(str(c.get('persona') or '').strip() for c in character_list),
        enable_emotional_states, emotional_states_list, persona_placement, emotion_placement)
    if cache_warning:
        toast.show(f"System-prompt placement: ~{cache_loss:.0%} of history recomputed. See log.", "warning", 8000)
        log_message(cache_warning, "WARNING")

    # --- Populate Task Queue ---
    if current_use_questions_file:
        try:
            app_state.questions_list = read_txt(QUESTIONS_FILE_PATH)
            if not app_state.questions_list:
                toast.show("Questions file is empty or not found.", "warning")
                log_message(f"{QUESTIONS_FILE_PATH} enabled but empty/not found.", "WARNING")
                app_state.processing_active = False; start_button.config(state=tk.NORMAL); return
        except Exception as e:
            messagebox.showerror("File Error", f"Error reading {QUESTIONS_FILE_PATH}: {e}")
            log_message(f"Error reading {QUESTIONS_FILE_PATH}: {e}", "ERROR")
            app_state.processing_active = False; start_button.config(state=tk.NORMAL); return
    else: 
        app_state.questions_list = [] 

    input_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.txt') and f != os.path.basename(QUESTIONS_FILE_PATH)]
    if not current_use_questions_file and not input_files and not current_opening_mode: # Opening mode runs without a book
        toast.show("No input .txt files found.", "warning")
        log_message("No input .txt files found for chunking.", "WARNING")
        app_state.processing_active = False; start_button.config(state=tk.NORMAL); return

    total_tasks_to_queue = 0
    if current_use_questions_file:
        for i, q_text in enumerate(app_state.questions_list):
            task_id = f"q_{i}"
            if task_id not in app_state.completed_task_ids:
                task_tuple = (task_id, os.path.basename(QUESTIONS_FILE_PATH), i, q_text)
                app_state.task_queue.put(task_tuple)
                total_tasks_to_queue += 1
                with app_state.task_queue_ui_lock:
                    app_state.pending_task_ids.add(task_id)
                    app_state.task_metadata[task_id] = {'file': os.path.basename(QUESTIONS_FILE_PATH), 'retries': 0, 'task_data': task_tuple}
    elif current_opening_mode and not input_files: # Opening mode without a book: num_random_chunks tasks
        total_tasks_to_queue = enqueue_opening_tasks(NUM_RANDOM_CHUNKS)
        log_message(f"Opening mode without input files: {total_tasks_to_queue} tasks for this run.", "INFO")
    else: # Chunk input files (Randomized)
        subject_size = subject_size_conf
        context_size = context_size_conf
        if context_size < subject_size:
            log_message(f"Warning: context_size ({context_size}) is less than subject_size ({subject_size}). Setting context_size = subject_size.", "WARNING")
            context_size = subject_size

        # We will try to add NUM_RANDOM_CHUNKS to the queue.
        # We use a loop with a safety counter to prevent infinite loops if files are very small.
        tasks_queued_count = 0
        max_attempts = NUM_RANDOM_CHUNKS * 5 # Safety break if we can't find enough unique chunks

        if not input_files:
            log_message("No input files found for random chunking.", "WARNING")

        attempt_count = 0
        while tasks_queued_count < NUM_RANDOM_CHUNKS and attempt_count < max_attempts:
            attempt_count += 1
            if len(input_files) == 0: break

            # 1. Select a random file from the input directory
            random_file_name = random.choice(input_files)
            file_path = os.path.join(INPUT_DIR, random_file_name)

            try:
                # 2. Read the entire content of the chosen file
                full_file_content = read_book(file_path)
                file_content_len = len(full_file_content)

                # 3. Select a random starting position for the chunk
                # We ensure we don't pick a position too close to the end where context would be missing
                max_valid_start_index = max(0, file_content_len - context_size)
                if max_valid_start_index <= 0:
                    log_message(f"File {random_file_name} is too small for the requested context size. Skipping.", "DEBUG")
                    continue

                random_start_index = random.randint(0, max_valid_start_index)

                # 4. Create a unique Task ID based on file name and the random start index
                # Opening-mode IDs differ, so finished chunk-mode tasks never suppress them
                task_id = ("opening_" if current_opening_mode else "") + f"{random_file_name}_chunk_at_{random_start_index}"

                # 5. Check if this specific chunk has already been completed (e.g., from a previous run)
                if task_id not in app_state.completed_task_ids:

                    # --- Perform the Chunking ---
                    subject_actual_end = min(random_start_index + subject_size, file_content_len)
                    current_subject_content = full_file_content[random_start_index:subject_actual_end]

                    if not current_subject_content.strip() or len(current_subject_content) < subject_size / 2 :
                        log_message(f"Random chunk at {random_start_index} in {random_file_name} is too short. Skipping.", "DEBUG")
                        continue

                    current_subject_len = len(current_subject_content)
                    context_needed_total_for_subject = context_size - current_subject_len

                    buffer_before = context_needed_total_for_subject // 2
                    buffer_after = context_needed_total_for_subject - buffer_before

                    context_start_index = max(0, random_start_index - buffer_before)
                    context_end_index = min(file_content_len, subject_actual_end + buffer_after)

                    # Final context boundary checks (copied from original logic)
                    current_context_len = context_end_index - context_start_index
                    if current_context_len < context_size:
                        if context_start_index == 0 and context_end_index < file_content_len:
                            context_end_index = min(file_content_len, context_start_index + context_size)
                        elif context_end_index == file_content_len and context_start_index > 0:
                            context_start_index = max(0, context_end_index - context_size)

                    current_context_text = full_file_content[context_start_index:context_end_index]

                    # 6. Add the task to the queue
                    task_tuple = (task_id, random_file_name, random_start_index, current_subject_content, current_context_text)
                    app_state.task_queue.put(task_tuple)
                    tasks_queued_count += 1
                    total_tasks_to_queue += 1
                    with app_state.task_queue_ui_lock:
                        app_state.pending_task_ids.add(task_id)
                        app_state.task_metadata[task_id] = {'file': random_file_name, 'retries': 0, 'task_data': task_tuple}
                else:
                    # Task already exists, just count it for the progress bar total
                    total_tasks_to_queue += 1

            except Exception as e:
                log_message(f"Error processing random chunk from file {random_file_name}: {e}", "ERROR")
                continue

        log_message(f"Attempted to queue {NUM_RANDOM_CHUNKS} random tasks. Successfully queued {tasks_queued_count} new unique tasks.", "INFO")

    if total_tasks_to_queue == 0 and not app_state.completed_task_ids: 
        toast.show("No tasks to process.", "warning")
        log_message("No tasks to queue.", "WARNING"); app_state.processing_active = False; start_button.config(state=tk.NORMAL); return
    elif total_tasks_to_queue == 0 and app_state.completed_task_ids: # All tasks were already done
        toast.show("All tasks already completed.", "info")
        log_message("All tasks already completed. Nothing new to queue.", "INFO"); app_state.processing_active = False; start_button.config(state=tk.NORMAL); return

    app_state.task_queue.all_tasks_queued = True 
    
    # **FIX ISSUE 1**: Adjust total_tasks_for_progress to account for turns
    num_unique_tasks_for_run = total_tasks_to_queue + len(app_state.completed_task_ids)
    # current_num_turns is already loaded from config
    actual_total_for_progress_bars = num_unique_tasks_for_run * current_num_turns
    
    log_message(f"Queued {total_tasks_to_queue} new unique tasks. Total unique tasks for run: {num_unique_tasks_for_run}. Effective total for progress bars (considering {current_num_turns} turns): {actual_total_for_progress_bars}", "INFO")

    app_state.task_queue.total_tasks_for_progress = actual_total_for_progress_bars # This is Y in X/Y
    app_state.task_queue.processed_tasks_lock = Lock() 

    # --- Setup Progress Bars ---
    if master_duplication_enabled:
        app_state.task_queue.api_processed_tasks = {i: 0 for i in range(4)} 
        app_state.task_queue.api_start_times_list = {i: [] for i in range(4)} 
        active_api_count_for_progress_ui = 0

        if should_resume and app_state.loaded_api_processed_tasks_snapshot is not None:
            for api_idx_resume in range(4): 
                if api_idx_resume in app_state.loaded_api_processed_tasks_snapshot:
                    # Snapshot stores turns processed, which is correct for the new total
                    app_state.task_queue.api_processed_tasks[api_idx_resume] = app_state.loaded_api_processed_tasks_snapshot[api_idx_resume]
        elif should_resume: 
             for api_idx_resume in range(4):
                if api_idx_resume < len(all_api_configs_runtime) and all_api_configs_runtime[api_idx_resume].get('enabled'):
                    # Each completed unique task means current_num_turns were processed by this API
                    app_state.task_queue.api_processed_tasks[api_idx_resume] = len(app_state.completed_task_ids) * current_num_turns


        for api_idx, api_conf in enumerate(all_api_configs_runtime):
            if api_idx < 4 and api_conf.get('enabled', False) and api_conf.get('url'): 
                active_api_count_for_progress_ui +=1
                api_name_label = ttk.Label(progress_frame, text=f"API Slot {api_idx+1} ({api_conf.get('model', 'N/A')}):",
                                           style='Header.TLabel')
                api_name_label.pack(pady=(5,0), anchor='w', fill='x')
                wrap_label(api_name_label, progress_frame, padding=2 * SPACING)
                bar = ttk.Progressbar(progress_frame, orient="horizontal", length=600, mode="determinate",
                                       style="Low.Horizontal.TProgressbar")
                bar.pack(pady=SPACING, fill='x', expand=True)
                percent_label = ttk.Label(progress_frame, text="0.0%", foreground='#868e96',
                                         font=('Segoe UI', 9, 'bold'))
                percent_label.pack(anchor='e', padx=(0, SPACING))
                time_label = ttk.Label(progress_frame, text="Time Rem: Estimating...", foreground="lightgray")
                time_label.pack(pady=(0,5), anchor='w')
                app_state.task_queue.api_widgets[api_idx] = {
                    'bar': bar,
                    'time_label': time_label,
                    'name_label': api_name_label,
                    'percent_label': percent_label
                }
                
                current_api_processed_turns = app_state.task_queue.api_processed_tasks.get(api_idx, 0)
                if app_state.task_queue.total_tasks_for_progress > 0:
                    bar['value'] = (current_api_processed_turns / app_state.task_queue.total_tasks_for_progress) * 100
                else:
                    bar['value'] = 0
        
        if active_api_count_for_progress_ui == 0 and master_duplication_enabled :
            messagebox.showerror("Config Error", "Master Duplication Mode is ON, but no APIs (Slots 1-4) are enabled or fully configured.")
            log_message("Duplication mode on, but no APIs 0-3 enabled/configured for UI progress bars.", "ERROR")
            app_state.processing_active = False; start_button.config(state=tk.NORMAL); return
    else: # Single overall progress bar for non-duplication mode
        overall_progress_bar = ttk.Progressbar(progress_frame, orient="horizontal", length=600, mode="determinate",
                                                style="Low.Horizontal.TProgressbar")
        overall_progress_bar.pack(pady=SPACING, fill='x', expand=True)
        overall_percent_label = ttk.Label(progress_frame, text="0.0%", foreground='#868e96',
                                           font=('Segoe UI', 9, 'bold'))
        overall_percent_label.pack(anchor='e', padx=(0, SPACING))
        overall_time_label = ttk.Label(progress_frame, text="Time Rem: Estimating...", foreground="lightgray")
        overall_time_label.pack(pady=SPACING)
        app_state.task_queue.overall_progress_bar = overall_progress_bar
        app_state.task_queue.overall_time_label = overall_time_label
        app_state.task_queue.overall_percent_label = overall_percent_label
        # Initialize processed_tasks (turns)
        if should_resume and loaded_processed_tasks_snapshot is not None:
            app_state.task_queue.processed_tasks = loaded_processed_tasks_snapshot
        elif should_resume:
            app_state.task_queue.processed_tasks = len(app_state.completed_task_ids) * current_num_turns
        else:
            app_state.task_queue.processed_tasks = 0
        
        app_state.task_queue.start_times_list = [] 
        if app_state.task_queue.total_tasks_for_progress > 0: 
            overall_progress_bar['value'] = (app_state.task_queue.processed_tasks / app_state.task_queue.total_tasks_for_progress) * 100
        elif app_state.task_queue.total_tasks_for_progress == 0: 
            overall_progress_bar['value'] = 0


    start_button.config(state=tk.DISABLED)
    pause_button.config(state=tk.NORMAL)
    stop_clear_button.config(state=tk.NORMAL)
    quit_button.config(state=tk.NORMAL)

    # --- Start Worker Threads ---
    app_state.threads = []
    output_data_lock = Lock()
    
    # Determine total number of threads based on API configurations
    total_threads = 0
    if master_duplication_enabled:
        # In duplication mode, use the sum of threads from all enabled APIs
        for api_idx, api_conf in enumerate(all_api_configs_runtime):
            if api_idx < 4 and api_conf.get('enabled', False) and api_conf.get('url'):
                total_threads += api_conf.get('threads', 10)
    else:
        # In non-duplication mode, use threads from each API for distribution
        for api_config in active_enabled_api_configs_for_worker_list:
            total_threads += api_config['config'].get('threads', 10)
    
    # Ensure at least one thread
    if total_threads <= 0:
        total_threads = 10
        log_message(f"Warning: No valid thread count found in API configs. Using default of {total_threads}.", "WARNING")
    
    # v11.2: Initialize the quality scoring queue system before starting worker threads
    # This ensures quality scoring requests are processed asynchronously without blocking workers
    if app_state.quality_enabled and global_config.get('quality.use_llm_scoring', False):
        import quality
        quality.initialize_quality_queue()
        log_message("Quality scoring queue system initialized.", "INFO")
    
    log_message(f"Starting {total_threads} worker threads based on API configurations.", "INFO")
    
    for i in range(total_threads):
        thread = threading.Thread(target=worker, args=(
            i, app_state.task_queue, output_data_lock,
            current_use_questions_file,
            current_use_variable_system,
            all_api_configs_runtime,
            active_enabled_api_configs_for_worker_list,
            current_question_prompt, current_answer_prompt, current_user_continuation_prompt,
            current_num_turns,
            app_state.system_prompts_list,
            current_refusal_phrases, current_user_speaking_phrases, current_slop_phrases,
            current_anti_slop_phrases,
            current_anti_slop_fixes,
            current_jailbreaks, current_speaking_fixes, current_slop_fixes_fallback,
            current_max_attempts, current_history_size, current_remove_reasoning,
            current_remove_em_dash,
            current_remove_asterisks,
            current_remove_asterisk_space_asterisk,
            current_remove_all_asterisks,
            current_ensure_space_after_line_break,
            current_remove_markdown,
            current_output_format,
            slop_fixer_api_config_runtime,
            anti_slop_fixer_api_config_runtime,
            anti_slop_fixer_api_config_param,
            current_slop_fixes_for_rotation,
            current_top_level_system_prompt,
            master_duplication_enabled,
            enable_character_engine_local,
            enable_class_selection_local,
            enable_setting_selection_local,
            character_list,
            enable_emotional_states,
            emotional_states_list,
            num_characters_local,
            no_user_impersonation_var.get(),
            current_api_request_timeout,
            current_slop_to_anti_slop_fallback,
            current_lore,
            enable_random_names,
            random_names_list,
            include_metadata,
            enable_random_settings,
            random_settings_list,
        ), kwargs={
            'opening_mode_local': current_opening_mode,
            'greeting_prompt_local': current_greeting_prompt,
            'first_line_prompt_local': current_first_line_prompt,
            'placeholder_share_local': current_placeholder_share,
            'greeting_context_local': current_greeting_context,
            'first_line_styles_local': current_first_line_styles,
            'persona_breakthrough_turn_local': persona_breakthrough_turn,
            'persona_placement_local': persona_placement,
            'emotion_placement_local': emotion_placement,
            'current_ooc_phrases': current_ooc_phrases,
            'current_ooc_fixes': current_ooc_fixes,
            'current_ooc_enabled': current_ooc_enabled,
            'per_turn_logic_check_local': per_turn_logic_check,
            'per_turn_logic_max_rewrites_local': per_turn_logic_max_rewrites,
            'user_emotional_states_enabled_local': enable_user_emotional_states,
            'user_emotional_states_list_local': user_emotional_states_list,
            'per_char_random_emotions_local': per_char_random_emotions,
            'current_regex_patterns': current_regex_patterns,
            'current_regex_watch_enabled': current_regex_watch_enabled,
            'current_regex_watch_max_rounds': current_regex_watch_max_rounds,
            'include_style_metrics_local': include_style_metrics,
            'retry_missing_quotations_local': current_retry_missing_quotations,
        }, name=f"Worker-{i}")
        app_state.threads.append(thread)
        thread.start()

    log_message(f"Started {total_threads} worker threads.", "INFO")

    _psutil_state = {"last_check": 0.0}
    PSUTIL_CHECK_INTERVAL = 10.0  # seconds

    # --- GUI Progress Update Loop ---
    def update_gui_progress():
        if dashboard_pause_var.get():
            app_state.root.after(1000, update_gui_progress) # Check again in 1s
            return
        if app_state.processing_active and not app_state.stop_processing:
            check_budget_limit()
            try:
                if time.time() - _psutil_state["last_check"] > PSUTIL_CHECK_INTERVAL:
                    _psutil_state["last_check"] = time.time()
                    def _check_open_files():
                        try:
                            p = psutil.Process()
                            of = p.open_files()
                            if len(of) > 300:
                                log_message(f"Warning: {len(of)} open files", "WARNING")
                        except Exception:
                            pass
                    threading.Thread(target=_check_open_files, daemon=True).start()
                master_duplication_current = app_state.master_duplication_enabled_var.get()

                if app_state.task_queue and hasattr(app_state.task_queue, 'qsize'):
                    if app_state.task_queue.qsize() > 30000:
                        log_message(f"Queue size: {app_state.task_queue.qsize()}", "WARNING")

                if master_duplication_current and hasattr(app_state.task_queue, 'api_widgets'):
                    for api_idx, widgets in app_state.task_queue.api_widgets.items():
                        if widgets['bar'].winfo_exists():
                            with app_state.task_queue.processed_tasks_lock:
                                processed_count_api_turns = app_state.task_queue.api_processed_tasks.get(api_idx, 0)
                                times_list_api = app_state.task_queue.api_start_times_list.get(api_idx, [])

                            if app_state.task_queue.total_tasks_for_progress > 0:
                                progress_val = (processed_count_api_turns / app_state.task_queue.total_tasks_for_progress) * 100
                                if progress_val > 100: progress_val = 100
                                widgets['bar']['value'] = progress_val
                                update_progress_bar_style(widgets['bar'], progress_val)
                                pulse_progress_bar(widgets['bar'], f"api_{api_idx}", app_state.root)
                                if 'percent_label' in widgets and widgets['percent_label'].winfo_exists():
                                    widgets['percent_label'].config(text=f"{progress_val:.1f}%")
                                time_rem_str = estimate_time_remaining(processed_count_api_turns, app_state.task_queue.total_tasks_for_progress, times_list_api)
                                widgets['time_label'].config(text=f"Time Rem: {time_rem_str} ({processed_count_api_turns}/{app_state.task_queue.total_tasks_for_progress} Turns)")
                            else:
                                widgets['time_label'].config(text="Time Rem: No tasks")
                                if 'percent_label' in widgets and widgets['percent_label'].winfo_exists():
                                    widgets['percent_label'].config(text="N/A")

                elif hasattr(app_state.task_queue, 'overall_progress_bar') and app_state.task_queue.overall_progress_bar.winfo_exists():
                    with app_state.task_queue.processed_tasks_lock:
                        processed_count_overall_turns = app_state.task_queue.processed_tasks
                        times_list_overall = app_state.task_queue.start_times_list

                    if app_state.task_queue.total_tasks_for_progress > 0:
                        progress_val = (processed_count_overall_turns / app_state.task_queue.total_tasks_for_progress) * 100
                        if progress_val > 100: progress_val = 100
                        app_state.task_queue.overall_progress_bar['value'] = progress_val
                        update_progress_bar_style(app_state.task_queue.overall_progress_bar, progress_val)
                        pulse_progress_bar(app_state.task_queue.overall_progress_bar, "overall", app_state.root)
                        if hasattr(app_state.task_queue, 'overall_percent_label') and app_state.task_queue.overall_percent_label.winfo_exists():
                            app_state.task_queue.overall_percent_label.config(text=f"{progress_val:.1f}%")
                        time_rem_str = estimate_time_remaining(processed_count_overall_turns, app_state.task_queue.total_tasks_for_progress, times_list_overall)
                        app_state.task_queue.overall_time_label.config(text=f"Time Rem: {time_rem_str} ({processed_count_overall_turns}/{app_state.task_queue.total_tasks_for_progress} Turns)")
                    else:
                        app_state.task_queue.overall_time_label.config(text="Time Rem: No tasks")
                        if hasattr(app_state.task_queue, 'overall_percent_label') and app_state.task_queue.overall_percent_label.winfo_exists():
                            app_state.task_queue.overall_percent_label.config(text="N/A")

                update_dashboard()
                update_api_response_time_labels()
                update_database_status()
                update_queue_ui()
                update_quality_display()
                _update_review_table()
                update_quality_tab()
                _update_regex_watch_tab()
            except Exception as e_gui:
                log_message(f"GUI update error: {str(e_gui)}", "ERROR")
            finally:
                # Always reschedule so a transient X/Tcl error doesn't kill the loop
                if app_state.root.winfo_exists():
                    is_active = (app_state.processing_active and not app_state.stop_processing
                                 and not app_state.pause_processing)
                    has_work = (app_state.task_queue and hasattr(app_state.task_queue, 'qsize')
                                and app_state.task_queue.qsize() > 0)
                    delay = 500 if (is_active and has_work) else 2000
                    app_state.root.after(delay, update_gui_progress)
        else: # Processing stopped or completed
            start_button.config(state=tk.NORMAL)
            pause_button.config(state=tk.DISABLED); pause_button.config(text="Pause")
            stop_clear_button.config(state=tk.NORMAL)
            log_message("Processing stopped/completed. GUI updates halted.", "INFO")
            update_dashboard()
            master_duplication_final_check = app_state.master_duplication_enabled_var.get()
            if hasattr(app_state.task_queue, 'total_tasks_for_progress') and app_state.task_queue.total_tasks_for_progress > 0:
                if master_duplication_final_check and hasattr(app_state.task_queue, 'api_widgets'):
                    for api_idx, widgets in app_state.task_queue.api_widgets.items():
                        if widgets['bar'].winfo_exists():
                            with app_state.task_queue.processed_tasks_lock:
                                processed_api_turns = app_state.task_queue.api_processed_tasks.get(api_idx, 0)
                            if processed_api_turns >= app_state.task_queue.total_tasks_for_progress:
                                widgets['bar']['value'] = 100
                                update_progress_bar_style(widgets['bar'], 100)
                                if 'percent_label' in widgets and widgets['percent_label'].winfo_exists():
                                    widgets['percent_label'].config(text="100%", foreground='#51cf66')
                                widgets['time_label'].config(text="Time Rem: Done!")
                elif hasattr(app_state.task_queue, 'overall_progress_bar') and app_state.task_queue.overall_progress_bar.winfo_exists():
                    with app_state.task_queue.processed_tasks_lock:
                        processed_overall_turns = app_state.task_queue.processed_tasks
                    if processed_overall_turns >= app_state.task_queue.total_tasks_for_progress:
                        app_state.task_queue.overall_progress_bar['value'] = 100
                        update_progress_bar_style(app_state.task_queue.overall_progress_bar, 100)
                        if hasattr(app_state.task_queue, 'overall_percent_label') and app_state.task_queue.overall_percent_label.winfo_exists():
                            app_state.task_queue.overall_percent_label.config(text="100%", foreground='#51cf66')
                        app_state.task_queue.overall_time_label.config(text="Time Remaining: Done!")
            save_generation_state()

    if app_state.root.winfo_exists(): 
        app_state.root.after(100, update_gui_progress) # Start the GUI update loop

    # --- Wait for Threads Completion (in a separate thread to not block UI) ---
    def wait_for_completion():
        for t_item in app_state.threads: 
            if t_item.is_alive(): 
                t_item.join() # Wait for each worker thread to finish

        if app_state.task_queue: 
            app_state.task_queue.join() # Wait for all tasks in the queue to be processed

        log_message("All tasks completed or processing stopped. All threads joined.", "INFO")
        app_state.processing_active = False 
        
        if not app_state.stop_processing: 
            all_done = False 
            master_duplication_at_end = app_state.master_duplication_enabled_var.get()
            if hasattr(app_state.task_queue, 'total_tasks_for_progress') and app_state.task_queue.total_tasks_for_progress > 0:
                if master_duplication_at_end and hasattr(app_state.task_queue, 'api_processed_tasks'):
                    all_apis_finished = True
                    for api_idx_check, api_conf_check in enumerate(all_api_configs_runtime):
                        if api_idx_check < 4 and api_conf_check.get('enabled') and api_conf_check.get('url') and api_conf_check.get('key'):
                            if app_state.task_queue.api_processed_tasks.get(api_idx_check, 0) < app_state.task_queue.total_tasks_for_progress:
                                all_apis_finished = False; break
                    if all_apis_finished : all_done = True
                elif not master_duplication_at_end and hasattr(app_state.task_queue, 'processed_tasks'):
                    if app_state.task_queue.processed_tasks >= app_state.task_queue.total_tasks_for_progress:
                        all_done = True
            elif hasattr(app_state.task_queue, 'total_tasks_for_progress') and app_state.task_queue.total_tasks_for_progress == 0: 
                all_done = True # No tasks were queued, so technically "done"

            if app_state.root.winfo_exists(): 
                if all_done:
                    app_state.root.after(0, lambda: messagebox.showinfo("Processing Complete", "All tasks have been processed successfully!"))
                else: 
                    app_state.root.after(0, lambda: messagebox.showinfo("Processing Finished", "Processing has finished. Some tasks may not have completed fully. Check logs."))
        # If stop_processing was true, the quit_application or stop_and_clear_job will handle messages.

    completion_thread = threading.Thread(target=wait_for_completion, name="CompletionWaiter")
    completion_thread.start()


def toggle_pause():
    """Toggles the pause state of the generation process."""
    app_state.pause_processing = not app_state.pause_processing
    if app_state.pause_processing:
        pause_button.config(text="Resume")
        log_message("Processing paused.", "INFO")
    else:
        # When resuming, reload the configuration to apply any changes made while paused
        global_config.load()
        log_message("Configuration reloaded from config.yml.", "INFO")
        pause_button.config(text="Pause")
        log_message("Processing resumed.", "INFO")

        #Apply rate limits from config to the global rate limiter
        all_apis_config = global_config.get('api.apis', [])
        for i in range(5):
            if i < len(all_apis_config):
                api_conf = all_apis_config[i] if isinstance(all_apis_config[i], dict) else {}
                rpm = api_conf.get('rate_limit_rpm', 60)  # Default to 60 if not set
                global_rate_limiter.set_rate_limit(i, rpm)
                log_message(f"API Slot {i+1} rate limit updated to {rpm} RPM from config", "INFO")

def update_num_threads(event=None): 
    """Updates the number of worker threads based on UI input, effective on next 'Start'."""
    try:
        new_num = int(num_threads_var.get())
        if new_num <= 0: raise ValueError("Threads must be > 0.")
        app_state.num_threads = new_num # This will be used when start_processing is next called
        log_message(f"Number of threads set to {new_num} (effective on next Start).", "INFO")
    except ValueError as e:
        log_message(f"Invalid num_threads value entered: {num_threads_var.get()}. Error: {e}", "ERROR")
        num_threads_var.set(str(app_state.num_threads)) # Revert to last valid number

def read_book(file_path):
    """Reads the entire content of a text file."""
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        max_len = global_config.get('generation.sanitize_input_max_length', 100000000)
        return sanitize_input(content, max_length=max_len)

def read_txt(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            # Sanitize each line as it's read
            max_len = global_config.get('generation.sanitize_input_max_length', 100000000)
            return [sanitize_input(line.strip(), max_length=max_len) for line in f if line.strip()]

def open_config_editor():
    global_config.load()
    try:
        editor = ConfigEditor(app_state.root, global_config, app_state.master_duplication_enabled_var, no_user_impersonation_var, on_config_saved=update_dashboard)
        editor.grab_set()
    except Exception as e:
        log_message(f"ConfigEditor failed to initialize: {e}", "ERROR")
        import traceback
        log_message(traceback.format_exc(), "ERROR")
        messagebox.showerror("Editor Error", f"Failed to open config editor:\n{e}")

def test_valkey_connection():
    """Test Valkey connection and show detailed results."""
    if not app_state.root.winfo_exists():
        return

    valkey_enabled = global_config.get('valkey.enabled', True)
    valkey_host = global_config.get('valkey.host', 'localhost')
    valkey_port = global_config.get('valkey.port', 6379)
    valkey_db = global_config.get('valkey.db', 0)
    valkey_password = global_config.get('valkey.password')

    log_message(f"=== Valkey Connection Test ===", "INFO")
    log_message(f"Enabled: {valkey_enabled}", "DEBUG")
    log_message(f"Host: {valkey_host}", "DEBUG")
    log_message(f"Port: {valkey_port}", "DEBUG")
    log_message(f"DB: {valkey_db}", "DEBUG")
    log_message(f"Password set: {'Yes' if valkey_password else 'No'}", "DEBUG")

    if not valkey_enabled:
        log_message("Valkey is disabled in config.", "WARNING")
        messagebox.showinfo("Valkey Test", "Valkey is disabled in config.yml")
        return

    try:
        # Create test client
        test_client = redis.Redis(
            host=valkey_host,
            port=valkey_port,
            db=valkey_db,
            password=valkey_password,
            socket_timeout=5,
            socket_connect_timeout=5
        )

        ping_result = test_client.ping()
        log_message(f"Valkey PING successful: {ping_result}", "INFO")

        # IMPORTANT: Update the global api_handler.valkey_client with working connection
        api_handler.valkey_client = test_client
        log_message("Updated api_handler.valkey_client with working connection", "DEBUG")

        # Update GUI status immediately after successful test
        if app_state.root.winfo_exists():
            app_state.root.after(100, update_database_status)

        # Try to get server info
        try:
            info = test_client.info()
            log_message(f"Valkey version: {info.get('redis_version', 'Unknown')}", "INFO")
            messagebox.showinfo(
                "Valkey Test",
                f"✅ Connection Successful!\n\n"
                f"Host: {valkey_host}:{valkey_port}\n"
                f"Version: {info.get('redis_version', 'Unknown')}\n"
                f"Connected Clients: {info.get('connected_clients', 'N/A')}\n\n"
                f"GUI status will update in 1 second."
            )
        except Exception as e:
            log_message(f"Valkey INFO command failed: {e}", "WARNING")
            messagebox.showinfo(
                "Valkey Test",
                f"✅ PING Successful!\n⚠️ INFO command failed: {e}"
            )

    except redis.ConnectionError as e:
        log_message(f"Valkey Connection Error: {e}", "ERROR")
        messagebox.showerror(
            "Valkey Test Failed",
            f"❌ Could not connect to Valkey/Redis\n\n"
            f"Host: {valkey_host}:{valkey_port}\n"
            f"Error: {e}"
        )
    except Exception as e:
        log_message(f"Valkey Test Error: {e}", "ERROR")
        messagebox.showerror("Valkey Test Failed", f"Error: {e}")

# --- Color Constants ---
ACCENT_CYAN = '#17a2b8'  # Teal/cyan accent for superhero theme

app_state.root = ttkbs.Window(themename="superhero")
app_state.root.title("Main UI")
fit_window(app_state.root, preferred=(1280, 900), minimum=(760, 520))
toast = ToastManager(app_state.root)
app_state.root.grid_columnconfigure(0, weight=1)
app_state.root.grid_rowconfigure(2, weight=1)
icon_path = "taskbar.png"
if os.path.exists(icon_path):
    try:
        icon_img = tk.PhotoImage(file=icon_path)
        # False = apply only to root window. True forces it on all children (causes X11 crashes)
        app_state.root.iconphoto(False, icon_img)  # True applies it to all child windows/dialogs
    except Exception as e:
        log_message(f"Failed to load taskbar icon: {e}", "WARNING")
else:
    log_message("taskbar.png not found in main directory. Using default system icon.", "WARNING")

style = ttk.Style()
available_themes = style.theme_names()
try:
    style.theme_use('superhero')
    log_message(f"Using theme: superhero", "INFO")
except tk.TclError:
    log_message(f"Could not apply superhero theme. Using system default.", "WARNING")



# Configure the styles on startup
configure_animated_progress_styles(style)
# --- End Animated Progress Bar Styling ---

# 🎨 1. Set a Global Typography Hierarchy
# Default body text for all standard widgets
style.configure('.', font=('Segoe UI', 10))

# Section headers, status labels, and important metrics
style.configure('Header.TLabel', font=('Segoe UI', 12, 'bold'))

# Helper text, footnotes, and secondary info
style.configure('Small.TLabel', font=('Segoe UI', 9), foreground='#8b949e')

# Code blocks, prompts, and raw logs
style.configure('Code.TLabel', font=('Consolas', 9))

# Dashboard tabs & notebook styling
style.configure('TNotebook.Tab', font=('Segoe UI', 10, 'bold'), padding=[16, 6])
style.configure('TNotebook', font=('Segoe UI', 10))

# --- Global Tkinter Variables ---
num_threads_var = tk.StringVar(value=str(global_config.get('threads', 10))) # Default from config or 10
no_user_impersonation_var = tk.BooleanVar(value=global_config.get('detection.no_user_impersonation', False))
app_state.master_duplication_enabled_var = tk.BooleanVar(value=global_config.get('api.master_duplication_mode', False))
debug_logging_var = tk.BooleanVar(value=False)
use_conversation_history_var = tk.BooleanVar(value=global_config.get('generation.use_conversation_history_for_fixers', True))

log_message("Application started. UI initializing.", "INFO")

# --- Database Connection Status Variables ---
postgres_connected_var = tk.BooleanVar(value=False)
postgres_active_var = tk.BooleanVar(value=False)
valkey_connected_var = tk.BooleanVar(value=False)
valkey_active_var = tk.BooleanVar(value=False)
app_state.db_status_widgets = {}

header_frame = ttk.Frame(app_state.root)
header_frame.grid(row=0, column=0, sticky="ew", padx=SPACING, pady=(SPACING, 4))
header_frame.columnconfigure(0, weight=1)

title_label = ttk.Label(
    header_frame,
    text="🧠 ReadyArt Synthetic Dataset Generator",
    font=('Segoe UI', 16, 'bold'),
    foreground=ACCENT_CYAN
)
title_label.grid(row=0, column=0, sticky="w")
wrap_label(title_label, header_frame, padding=90)

version_label = ttk.Label(
    header_frame,
    text="v11.8.0",
    font=('Segoe UI', 10),
    foreground='#868e96'
)
version_label.grid(row=0, column=1, sticky="e", padx=(10, 0))

ttk.Separator(header_frame, orient='horizontal').grid(row=1, column=0, columnspan=2, sticky="ew", pady=(4, 0))

# The working area owns the expanding root row. Monitoring details and progress
# scroll inside Overview, so adding progress bars cannot displace run controls.
dashboard_outer_frame = ttk.Frame(app_state.root)
dashboard_outer_frame.grid(row=2, column=0, sticky="nsew", padx=SPACING, pady=(4, SPACING))
dashboard_outer_frame.columnconfigure(0, weight=1)
dashboard_outer_frame.rowconfigure(1, weight=1)
app_state.dashboard_notebook = ttk.Notebook(dashboard_outer_frame)
app_state.dashboard_notebook.grid(row=1, column=0, sticky="nsew")
app_state.dashboard_notebook.tabs_widgets = {}

overview_tab = ScrollableFrame(app_state.dashboard_notebook)
app_state.dashboard_notebook.add(overview_tab, text="Overview")
overview_content = overview_tab.content
app_state.dashboard_notebook.tabs_widgets['Overview'] = {'canvas': overview_tab.canvas}


# --- Metrics Display Frame ---
metrics_frame = ttk.Frame(overview_content)
metrics_frame.pack(pady=SPACING, padx=SPACING, fill="x")

def _create_ttkbs_metric(parent, title, icon, default_text="0 (0.0%)"):
    """Helper to create consistent ttkbootstrap Card metrics"""
    card = ttk.Frame(parent, borderwidth=0, padding=10)
    if not hasattr(parent, '_metric_cards'):
        parent._metric_cards = []
    parent._metric_cards.append(card)

    ttk.Label(card, text=f"{icon} {title}", style='Header.TLabel').pack(pady=(0, 2))
    val_lbl = ttk.Label(card, text=default_text, style='MetricValue.TLabel', wraplength=220)
    val_lbl.pack(pady=(0, 4), fill="x")
    wrap_label(val_lbl, card, padding=20)
    return val_lbl

app_state.refusal_percent_label = _create_ttkbs_metric(metrics_frame, "Refusals", "🚫")
app_state.user_speaking_label = _create_ttkbs_metric(metrics_frame, "User Speak", "🗣️")
app_state.ooc_label = _create_ttkbs_metric(metrics_frame, "OOC", "🎭")
app_state.logic_label = _create_ttkbs_metric(metrics_frame, "Logic", "🧠")
app_state.slop_label = _create_ttkbs_metric(metrics_frame, "Slop", "🧹")
app_state.regex_watch_label = _create_ttkbs_metric(metrics_frame, "Regex Watch", "🔎")
app_state.error_percent_label = _create_ttkbs_metric(metrics_frame, "Errors", "⚠️")

# Secondary metrics row
metrics_row2 = ttk.Frame(overview_content)
metrics_row2.pack(pady=(0, SPACING), padx=SPACING, fill="x")

app_state.token_label = _create_ttkbs_metric(metrics_row2, "Tokens", "🔢", default_text="0")
app_state.cost_label = _create_ttkbs_metric(metrics_row2, "Est. Cost", "💰", default_text="$0.0000")
app_state.budget_label = _create_ttkbs_metric(metrics_row2, "Budget", "📊", default_text="Budget: Disabled")
app_state.thread_status_label = _create_ttkbs_metric(metrics_row2, "Threads", "🧵", default_text="Threads: 0 spawned, 0 active")

# --- Quality Metrics Row ---
metrics_row3 = ttk.Frame(overview_content)
metrics_row3.pack(pady=(0, SPACING), padx=SPACING, fill="x")

app_state.quality_score_label = _create_ttkbs_metric(metrics_row3, "Quality Score", "⭐", default_text="N/A")
app_state.quality_threshold_label = _create_ttkbs_metric(metrics_row3, "Below Threshold", "📉", default_text="0")
app_state.quality_flag_label = _create_ttkbs_metric(metrics_row3, "Top Flag", "🏳️", default_text="None")

metric_layouts = [ResponsiveGrid(frame, frame._metric_cards, min_width=180, max_columns=4)
                  for frame in (metrics_frame, metrics_row2, metrics_row3)]
# --- End of Metrics Display Frame ---

# --- Database Connection Status Frame ---
db_status_frame = ttk.LabelFrame(overview_content, text="🗄️ Database & Cache Status")
db_status_frame.pack(pady=SPACING, padx=SPACING, fill="x")

# Add refresh button to db_status_frame
db_refresh_btn = ttk.Button(
    db_status_frame,
    text="🔄 Refresh Status",
    command=update_database_status
)


db_test_btn = ttk.Button(
    db_status_frame,
    text="🔍 Test Valkey",
    command=test_valkey_connection
)


# PostgreSQL Status
postgres_status_frame = ttk.Frame(db_status_frame)


postgres_icon_label = ttk.Label(
    postgres_status_frame,
    text="❌",
    font=('Segoe UI Emoji', 14),
    foreground="gray"
)
postgres_icon_label.pack(side=tk.LEFT, padx=SPACING)

postgres_status_label = ttk.Label(
    postgres_status_frame,
    text="PostgreSQL: Disconnected",
    style="Header.TLabel",
    foreground="gray"
)
postgres_status_label.pack(side=tk.LEFT, padx=SPACING)

# Valkey/Redis Status
valkey_status_frame = ttk.Frame(db_status_frame)


valkey_icon_label = ttk.Label(
    valkey_status_frame,
    text="❌",
    font=('Segoe UI Emoji', 14),
    foreground="gray"
)
valkey_icon_label.pack(side=tk.LEFT, padx=SPACING)

valkey_status_label = ttk.Label(
    valkey_status_frame,
    text="Valkey: Disconnected",
    style="Header.TLabel",
    foreground="gray"
)
valkey_status_label.pack(side=tk.LEFT, padx=SPACING)

database_layout = ResponsiveGrid(db_status_frame,
    [postgres_status_frame, valkey_status_frame, db_test_btn, db_refresh_btn],
    min_width=180, max_columns=4, pady=SPACING)
wrap_label(postgres_status_label, postgres_status_frame, padding=48)
wrap_label(valkey_status_label, valkey_status_frame, padding=48)

# Store references for updates
app_state.db_status_widgets = {
    'postgres_icon': postgres_icon_label,
    'postgres_status': postgres_status_label,
    'valkey_icon': valkey_icon_label,
    'valkey_status': valkey_status_label
}

# --- API Response Time Display Frame ---
api_response_times_frame = ttk.LabelFrame(overview_content, text="⚡ API Response Times")
api_response_times_frame.pack(pady=SPACING, padx=SPACING, fill="x")

api_response_cards = []
for slot_idx in range(6):
    card = ttk.Frame(api_response_times_frame, padding=6)
    api_response_cards.append(card)

    slot_label_name = f"api_response_time_label_{slot_idx+1}"
    slot_label = ttk.Label(
        card,
        text=f"⚡ API {slot_idx+1}: No data yet",
        font=('Segoe UI', 9),
        foreground='#868e96',
        anchor="w"
    )
    slot_label.pack(fill="x")
    wrap_label(slot_label, card, padding=12)
    app_state.slot_widgets[slot_label_name] = slot_label
api_response_layout = ResponsiveGrid(api_response_times_frame, api_response_cards,
                                    min_width=200, max_columns=3)
# --- End of API Response Time Display Frame ---

# --- API Response Time Dynamic Color Coding ---
API_RT_THRESHOLDS_MS = (500, 1500)  # (good, medium) — above medium = slow

API_RT_COLORS = {
    "good":    "#51cf66",   # Green  — snappy
    "medium":  "#fcc419",   # Amber  — acceptable
    "slow":    "#ff6b6b",   # Red    — needs attention
    "no_data": "#868e96",   # Muted  — idle
}

def _get_api_rt_color(avg_ms):
    """Return the appropriate color for a given average response time in ms."""
    if avg_ms is None or avg_ms <= 0:
        return API_RT_COLORS["no_data"]
    if avg_ms < API_RT_THRESHOLDS_MS[0]:
        return API_RT_COLORS["good"]
    elif avg_ms < API_RT_THRESHOLDS_MS[1]:
        return API_RT_COLORS["medium"]
    else:
        return API_RT_COLORS["slow"]

def _format_api_rt(avg_ms):
    """Format a response time in ms into a human-friendly string."""
    if avg_ms >= 1000:
        return f"{avg_ms / 1000:.2f}s"
    return f"{avg_ms:.0f}ms"

def update_api_response_time_labels():
    """
    Refreshes all 6 API response-time labels with dynamic color coding.
    Call this from the GUI update loop alongside update_dashboard().
    """
    if not app_state.root.winfo_exists():
        return

    with api_response_times_lock:
        for slot_idx in range(6):
            label_name = f"api_response_time_label_{slot_idx + 1}"
            label = app_state.slot_widgets.get(label_name)
            if label is None or not label.winfo_exists():
                continue

            times = api_response_times_per_slot[slot_idx] if slot_idx < len(api_response_times_per_slot) else []

            if not times:
                label.config(
                    text=f"⚡ API {slot_idx + 1}: No data yet",
                    foreground=API_RT_COLORS["no_data"]
                )
            else:
                avg_ms = sum(times) / len(times)
                color = _get_api_rt_color(avg_ms)
                label.config(
                    text=f"⚡ API {slot_idx + 1}: {_format_api_rt(avg_ms)} avg",
                    foreground=color
                )
# --- End API Response Time Dynamic Color Coding ---

# --- Progress Bars Frame ---
progress_frame = ttk.Frame(overview_content)
progress_frame.pack(pady=SPACING, padx=SPACING, fill=tk.X)
maintenance_frame = ttk.LabelFrame(overview_content, text="Tools")
maintenance_frame.pack(pady=SPACING, padx=SPACING, fill=tk.X)

# --- Main Action Buttons Frame ---
button_frame = ttkbs.Frame(app_state.root)
button_frame.grid(row=1, column=0, sticky="ew", padx=SPACING)

start_button = ttk.Button(button_frame, text="🚀 Start Generation", command=start_processing, style="Accent.TButton")


pause_button = ttk.Button(button_frame, text="⏸️ Pause", command=toggle_pause, state=tk.DISABLED)


dashboard_pause_var = tk.BooleanVar(value=False)
pause_updates_check = ttk.Checkbutton(maintenance_frame, text="⏸️ Pause UI Updates", variable=dashboard_pause_var)

def toggle_debug_logging():
    # Update the flag in the logging module
    logging_config.DEBUG_LOGGING_ENABLED = debug_logging_var.get()
    log_message(f"Debug logging {'enabled' if debug_logging_var.get() else 'disabled'}.", "INFO")

# --- Stop and Clear Job Functionality ---
def stop_and_clear_processing_job():
    """Stops the current job, clears its progress, and resets UI for a new start."""

    if not app_state.processing_active and (not app_state.threads or not any(t.is_alive() for t in app_state.threads if t)):
        log_message("No active processing job to stop and clear. Resetting for fresh start.", "INFO")
        if os.path.exists(STATE_FILE_PATH):
            try: os.remove(STATE_FILE_PATH); log_message(f"Removed state file: {STATE_FILE_PATH}", "INFO")
            except Exception as e: log_message(f"Error removing state file {STATE_FILE_PATH}: {e}", "WARNING")
        reset_all_stats_and_history()
        update_dashboard()
        for widget in progress_frame.winfo_children(): widget.destroy() # Clear progress bars
        start_button.config(state=tk.NORMAL)
        pause_button.config(text="Pause", state=tk.DISABLED)
        if 'stop_clear_button' in globals() and stop_clear_button.winfo_exists(): # Check if button exists
            stop_clear_button.config(state=tk.NORMAL) # Re-enable itself
        log_message("Stats and state cleared. Ready for a new job.", "INFO")
        return

    if messagebox.askokcancel("Stop & Clear Job", "Stop current job and clear its progress? This allows starting a new job fresh. Output files won't be deleted by this action."):
        log_message("Stop & Clear Job pressed. Initiating stop and clear.", "INFO")
        app_state.stop_processing = True # Signal threads to stop

        start_button.config(state=tk.DISABLED)
        pause_button.config(text="Pause", state=tk.DISABLED)
        if 'stop_clear_button' in globals() and stop_clear_button.winfo_exists():
            stop_clear_button.config(state=tk.DISABLED)

        wait_thread = threading.Thread(target=wait_for_threads_to_stop_for_clear, name="ClearJobWaiter")
        wait_thread.start()


def wait_for_threads_to_stop_for_clear():
    """Helper function to join threads and clear state after stop_and_clear_job is initiated."""

    if app_state.task_queue and app_state.threads: # Send sentinels to worker threads
        active_thread_count = sum(1 for t in app_state.threads if t.is_alive())
        num_sentinels = active_thread_count if active_thread_count > 0 else len(app_state.threads)
        log_message(f"Stop & Clear: Attempting to stop threads by queueing {num_sentinels} sentinels.", "DEBUG")
        for _ in range(num_sentinels):
            try:
                if app_state.task_queue: app_state.task_queue.put(None, block=False, timeout=0.05)
            except Full: log_message("Stop & Clear: Queue full while putting sentinel.", "WARNING"); break
            except Exception as e: log_message(f"Stop & Clear: Error putting sentinel: {e}", "WARNING")

    if app_state.threads: # Join threads
        log_message(f"Stop & Clear: Waiting for {len(app_state.threads)} worker threads to join...", "INFO")
        for t in app_state.threads:
            if t.is_alive():
                try:
                    t.join(timeout=1.0)
                    if t.is_alive(): log_message(f"Stop & Clear: Thread {t.name} did not join in time.", "WARNING")
                except Exception as e: log_message(f"Stop & Clear: Error joining thread {t.name}: {e}", "WARNING")
        log_message("Stop & Clear: All worker threads joined or timed out.", "INFO")
        app_state.threads = [] # Clear the list of threads

    # v11.2: Shutdown the quality scoring queue processor
    try:
        import quality
        quality.shutdown_quality_queue()
        log_message("Quality scoring queue processor shut down.", "INFO")
    except Exception as e:
        log_message(f"Error shutting down quality queue: {e}", "WARNING")

    app_state.processing_active = False # Mark processing as fully stopped

    if app_state.task_queue: # Clear and reinitialize the task queue
        while not app_state.task_queue.empty():
            try: app_state.task_queue.get_nowait()
            except Empty: break
        app_state.task_queue = Queue()
        log_message("Stop & Clear: Task queue cleared and reinitialized.", "INFO")

    if os.path.exists(STATE_FILE_PATH): # Remove the state file for a fresh start next time
        try: os.remove(STATE_FILE_PATH); log_message(f"Stop & Clear: Removed state file: {STATE_FILE_PATH}", "INFO")
        except Exception as e: log_message(f"Stop & Clear: Error removing state file {STATE_FILE_PATH}: {e}", "WARNING")

    reset_all_stats_and_history() # Reset all counters, completed_task_ids, etc.
    log_message("Stop & Clear: All statistics and in-memory progress reset.", "INFO")

    if app_state.root.winfo_exists(): # Schedule UI finalization on the main thread
        app_state.root.after(0, finalize_stop_and_clear_ui)

def finalize_stop_and_clear_ui():
    """Finalizes UI updates after a 'Stop & Clear Job' operation."""
    log_message("Stop & Clear: Finalizing UI updates.", "INFO")
    update_dashboard() # Refresh dashboard with reset stats

    for widget in progress_frame.winfo_children(): widget.destroy() # Clear progress bars

    start_button.config(state=tk.NORMAL)
    pause_button.config(text="Pause", state=tk.DISABLED)
    if 'stop_clear_button' in globals() and stop_clear_button.winfo_exists():
        stop_clear_button.config(state=tk.NORMAL) # Re-enable Stop & Clear button
    quit_button.config(state=tk.NORMAL)
    log_message("Stop & Clear: UI reset. Ready for a new job.", "INFO")

stop_clear_button = ttk.Button(button_frame, text="🛑 Stop & Clear Job", command=stop_and_clear_processing_job, state=tk.DISABLED)

# --- End of Stop and Clear Job Functionality ---

config_button = ttk.Button(button_frame, text="⚙️ Edit Config", command=open_config_editor)


debug_log_check = ttk.Checkbutton(maintenance_frame, text="🐛 Debug Logs", variable=debug_logging_var, command=toggle_debug_logging)
primary_actions_layout = ResponsiveGrid(button_frame,
    [start_button, pause_button, stop_clear_button, config_button],
    min_width=170, max_columns=4, pady=4)

def quit_application():
    """Handles graceful shutdown of the application when Quit button or window X is clicked."""
    if messagebox.askokcancel("Quit", "Are you sure you want to quit? This will stop any ongoing generation and save progress."):
        log_message("Quit button pressed. Initiating shutdown.", "INFO")
        app_state.stop_processing = True # Signal threads to stop

        start_button.config(state=tk.DISABLED)
        pause_button.config(state=tk.DISABLED)
        if 'stop_clear_button' in globals() and stop_clear_button.winfo_exists():
            stop_clear_button.config(state=tk.DISABLED)
        quit_button.config(state=tk.DISABLED)


        if app_state.task_queue and app_state.threads and any(t.is_alive() for t in app_state.threads):
            active_thread_count = sum(1 for t in app_state.threads if t.is_alive())
            num_sentinels = active_thread_count if active_thread_count > 0 else len(app_state.threads)
            log_message(f"Quit: Attempting to stop threads by queueing {num_sentinels} sentinels.", "DEBUG")
            for _ in range(num_sentinels):
                try:
                    if app_state.task_queue: app_state.task_queue.put(None, block=False, timeout=0.05)
                except Full:
                    log_message("Quit: Queue full while trying to put sentinel. Threads might be stuck.", "WARNING")
                    break
                except Exception as e:
                    log_message(f"Quit: Error putting sentinel in queue: {e}", "WARNING")

        if app_state.threads:
            log_message(f"Quit: Waiting for {len(app_state.threads)} worker threads to join...", "INFO")
            for t in app_state.threads:
                if t.is_alive():
                    try:
                        t.join(timeout=1.0) # Short timeout for joining
                        if t.is_alive():
                            log_message(f"Quit: Thread {t.name} did not join in time.", "WARNING")
                    except Exception as e:
                        log_message(f"Quit: Error joining thread {t.name}: {e}", "WARNING")
            log_message("Quit: All worker threads joined or timed out.", "INFO")

        # v11.2: Shutdown the quality scoring queue processor
        try:
            import quality
            quality.shutdown_quality_queue()
            log_message("Quality scoring queue processor shut down.", "INFO")
        except Exception as e:
            log_message(f"Error shutting down quality queue: {e}", "WARNING")

        app_state.processing_active = False # Mark processing as fully stopped

        log_message("Quit: Saving generation state before exiting...", "INFO")
        save_generation_state() # Save final progress

        log_message("Quit: Destroying Tkinter root window...", "INFO")
        if app_state.root and hasattr(app_state.root, 'winfo_exists') and app_state.root.winfo_exists():
            app_state.root.destroy()
        if app_state.db_pool:
            app_state.db_pool.closeall()
            log_message("PostgreSQL pool closed.", "INFO")
        log_message("Application shutdown sequence complete. Exiting process.", "INFO")
        sys.exit(0) # Terminate the script

quit_button = ttk.Button(maintenance_frame, text="❌ Quit Application", command=quit_application)

app_state.root.protocol("WM_DELETE_WINDOW", quit_application) # Handle window close (X) button

app_state.status_bar = ttk.Label(app_state.root, text="Ready", foreground="lightgray", anchor="w")
app_state.status_bar.grid(row=3, column=0, sticky="ew", padx=SPACING, pady=(0, 4))
wrap_label(app_state.status_bar, app_state.root, padding=2 * SPACING)

def force_recovery():
    """Bypasses config checks and forces state reload + thread restart."""
    if not app_state.processing_active and not any(t.is_alive() for t in app_state.threads):
        if os.path.exists(STATE_FILE_PATH):
            if load_generation_state():
                log_message("Force recovery: State loaded. Ready to resume.", "INFO")
                reset_all_stats_and_history()
                update_dashboard()
                toast.show("State recovered. Click Start to resume.", "success")
            else:
                toast.show("Could not load state. Check logs.", "warning")
        else:
            toast.show("No previous state found.", "info")
    else:
        toast.show("Cannot force recovery while processing.", "warning")

def trigger_export():
    file_path = filedialog.asksaveasfilename(
        defaultextension=".jsonl",
        filetypes=[("JSONL Files", "*.jsonl")],
        title="Export Conversations to JSONL"
    )
    if not file_path: return

    # Run export in background thread to prevent UI freeze
    def run_export():
        success = export_db_to_jsonl(file_path)
        app_state.root.after(0, lambda: app_state.status_bar.config(text="Export complete!" if success else "Export failed.",
                                                foreground="green" if success else "red"))
        # Re-enable Start only if no generation run began or is still running
        app_state.root.after(0, lambda: start_button.config(state=tk.DISABLED if generation_running() else tk.NORMAL))

    start_button.config(state=tk.DISABLED)
    app_state.status_bar.config(text="Exporting from PostgreSQL...", foreground="blue")
    threading.Thread(target=run_export, daemon=True).start()

export_button = ttk.Button(maintenance_frame, text="📤 Export DB → JSONL", command=trigger_export)


clear_db_button = ttk.Button(maintenance_frame, text="🗑️ Clear Database", command=clear_database)


recovery_button = ttk.Button(maintenance_frame, text="🔄 Force Recovery", command=force_recovery)


# Secondary actions share the scrollable Overview area.
clear_dash_btn = ttk.Button(maintenance_frame, text="🧹 Clear Dashboard", command=clear_dashboard)
maintenance_layout = ResponsiveGrid(maintenance_frame,
    [export_button, clear_db_button, recovery_button, clear_dash_btn,
     quit_button, debug_log_check, pause_updates_check], min_width=190, max_columns=3)

# Narrow windows use a view selector so every page remains reachable.
dashboard_navigator = NotebookNavigator(dashboard_outer_frame, app_state.dashboard_notebook)
dashboard_navigator.grid(row=0, column=0, sticky="ew", pady=(0, 4))

highlight_colors = {
    "highlight_refusal": {"foreground": "#FF6B6B", "font": ('TkDefaultFont', 9, 'bold')},  # Bright red
    "highlight_user_speak": {"foreground": "#4DABF7", "font": ('TkDefaultFont', 9, 'bold')},  # Bright blue
    "highlight_slop": {"foreground": "#9775FA", "font": ('TkDefaultFont', 9, 'bold')},  # Bright purple
    "highlight_anti_slop": {"foreground": "#FFD43B", "font": ('TkDefaultFont', 9, 'bold')},  # Bright yellow
    "highlight_ooc": {"foreground": "#FF922B", "font": ('TkDefaultFont', 9, 'bold')},  # Bright orange (OOC drift)
    "highlight_error": {"foreground": "#FC8181", "font": ('TkDefaultFont', 9, 'bold')}  # Bright red-orange (errors)
}

tab_names = ["Totals"] + [f"API {i+1}" for i in range(4)] + ["Quality", "🔍 Review", "🔎 Regex Watch"]
issue_types = ["Refusals", "User Speak", "Slop", "Anti-Slop", "OOC Drift", "Errors"]
issue_keys = ["refusals", "user_speak", "slop", "anti_slop", "out_of_character", "errors"] # Keys for accessing data and widgets

# --- Rich Prompt Viewer Tab Setup ---
preview_tab = ttk.Frame(app_state.dashboard_notebook)
app_state.dashboard_notebook.add(preview_tab, text="🔍 Prompt Viewer")

# Top toolbar: metadata + stats
preview_toolbar = ttk.Frame(preview_tab)
preview_toolbar.pack(fill=tk.X, padx=SPACING, pady=(SPACING, 2))

# Metadata label (thread, slot, type, attempt, timestamp)
prompt_preview_meta_var = tk.StringVar(value="Waiting for prompt generation...")
meta_label = ttk.Label(preview_toolbar, textvariable=prompt_preview_meta_var,
                       font=('Segoe UI', 9, 'bold'), foreground=ACCENT_CYAN, anchor='w')
meta_label.pack(side=tk.LEFT, fill=tk.X, expand=True)

# Stats label (message count, chars, tokens)
prompt_preview_stats_var = tk.StringVar(value="")
stats_label = ttk.Label(preview_toolbar, textvariable=prompt_preview_stats_var,
                        font=('Segoe UI', 9), foreground='#868e96', anchor='e')
stats_label.pack(side=tk.RIGHT)

# Second toolbar: search + actions
preview_actions_bar = ttk.Frame(preview_tab)
preview_actions_bar.pack(fill=tk.X, padx=SPACING, pady=(2, SPACING))
preview_search_group = ttk.Frame(preview_actions_bar)
preview_copy_group = ttk.Frame(preview_actions_bar)

# Search
preview_search_var = tk.StringVar()
preview_search_entry = ttk.Entry(preview_search_group, textvariable=preview_search_var, width=20)
preview_search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))

def _preview_search():
    """Highlight all occurrences of the search term in the prompt viewer."""
    if prompt_preview_text is None:
        return
    prompt_preview_text.config(state=tk.NORMAL)
    # Remove old highlights
    for tag in prompt_preview_text.tag_names():
        if tag.startswith('search_'):
            prompt_preview_text.tag_delete(tag)
    term = preview_search_var.get().strip()
    if not term:
        prompt_preview_text.config(state=tk.DISABLED)
        return
    # Find all occurrences
    start = '1.0'
    idx = prompt_preview_text.search(term, start, stopindex=tk.END)
    tag_name = f'search_{id(term)}'
    prompt_preview_text.tag_configure(tag_name, background='#ffd43b', foreground='#000000')
    count = 0
    while idx:
        end_idx = f"{idx}+{len(term)}c"
        prompt_preview_text.tag_add(tag_name, idx, end_idx)
        count += 1
        start = end_idx
        idx = prompt_preview_text.search(term, start, stopindex=tk.END)
    if count > 0:
        # Scroll to first match
        prompt_preview_text.see(idx)
    prompt_preview_text.config(state=tk.DISABLED)

preview_search_entry.bind('<Return>', lambda e: _preview_search())
ttk.Button(preview_search_group, text="🔍", width=3, command=_preview_search).pack(side=tk.LEFT, padx=2)
ttk.Button(preview_search_group, text="Clear", width=5,
           command=lambda: (preview_search_var.set(''), _preview_search())).pack(side=tk.LEFT, padx=2)

# Copy buttons
def _copy_system_prompt():
    if prompt_preview_text is None:
        return
    content = prompt_preview_text.get(1.0, tk.END)
    # Extract system section
    sys_start = content.find('📌 SYSTEM')
    if sys_start == -1:
        toast.show("No system prompt found.", "warning")
        return
    # Find end of system section (next separator or end)
    next_sep = content.find('─' * 80, sys_start + 10)
    if next_sep == -1:
        next_sep = len(content)
    sys_text = content[sys_start + 12:next_sep].strip()
    app_state.root.clipboard_clear()
    app_state.root.clipboard_append(sys_text)
    toast.show("System prompt copied.", "success")

def _copy_latest_message():
    if prompt_preview_text is None:
        return
    content = prompt_preview_text.get(1.0, tk.END)
    # Find last role marker
    last_marker = max(
        content.rfind('📌 SYSTEM'),
        content.rfind('📌 USER'),
        content.rfind('📌 ASSISTANT')
    )
    if last_marker == -1:
        toast.show("No messages found.", "warning")
        return
    latest_text = content[last_marker + 12:].strip()
    app_state.root.clipboard_clear()
    app_state.root.clipboard_append(latest_text)
    toast.show("Latest message copied.", "success")

def _copy_all():
    if prompt_preview_text is None:
        return
    content = prompt_preview_text.get(1.0, tk.END).strip()
    app_state.root.clipboard_clear()
    app_state.root.clipboard_append(content)
    toast.show("Full prompt copied.", "success")

ttk.Button(preview_copy_group, text="📋 System", width=10, command=_copy_system_prompt).pack(side=tk.RIGHT, padx=2)
ttk.Button(preview_copy_group, text="📋 Latest", width=10, command=_copy_latest_message).pack(side=tk.RIGHT, padx=2)
ttk.Button(preview_copy_group, text="📋 All", width=8, command=_copy_all).pack(side=tk.RIGHT, padx=2)

# Auto-scroll toggle
prompt_preview_autoscroll_var = tk.BooleanVar(value=True)
ttk.Checkbutton(preview_copy_group, text="Auto-scroll", variable=prompt_preview_autoscroll_var).pack(side=tk.RIGHT, padx=8)
preview_actions_layout = ResponsiveGrid(preview_actions_bar,
    [preview_search_group, preview_copy_group], min_width=300, max_columns=2, padx=0, pady=2)
wrap_label(meta_label, preview_toolbar, padding=200)

# Main rich text area
prompt_preview_text = scrolledtext.ScrolledText(
    preview_tab, wrap=tk.WORD, state=tk.NORMAL, width=1, height=8,
    font=('Consolas', 10), spacing1=2, spacing3=2,
    background='#1a1a2e', foreground='#e0e0e0',
    insertbackground='#ffffff', selectbackground='#2a4a6a',
    relief=tk.FLAT, borderwidth=0
)
prompt_preview_text.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=(0, SPACING))

# Configure text tags for rich syntax highlighting
prompt_preview_text.tag_configure('system_role', foreground='#bb86fc', font=('Consolas', 11, 'bold'))
prompt_preview_text.tag_configure('system_content', foreground='#ce93d8')
prompt_preview_text.tag_configure('user_role', foreground='#4fc3f7', font=('Consolas', 11, 'bold'))
prompt_preview_text.tag_configure('user_content', foreground='#81d4fa')
prompt_preview_text.tag_configure('assistant_role', foreground='#69f0ae', font=('Consolas', 11, 'bold'))
prompt_preview_text.tag_configure('assistant_content', foreground='#b2dfdb')
prompt_preview_text.tag_configure('separator', foreground='#444466')

# Initial placeholder text
prompt_preview_text.insert(tk.END,
    "🔍 Rich Prompt Viewer\n\n"
    "Waiting for prompt generation...\n\n"
    "Prompts will appear here in real-time as they are queued for the API.\n\n"
    "Features:\n"
    "  • Color-coded by role (System / User / Assistant)\n"
    "  • Metadata header (thread, slot, type, attempt)\n"
    "  • Token estimation\n"
    "  • Search & highlight\n"
    "  • Copy individual sections\n"
    "  • Auto-scroll toggle\n"
)
prompt_preview_text.config(state=tk.DISABLED)
# --- End Rich Prompt Viewer Tab Setup ---

def _grid_scrollable_table(container, tree):
    """Retain readable columns while allowing either table axis to overflow."""
    container.columnconfigure(0, weight=1)
    container.rowconfigure(0, weight=1)
    vertical = ttk.Scrollbar(container, orient="vertical", command=tree.yview)
    horizontal = ttk.Scrollbar(container, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=vertical.set, xscrollcommand=horizontal.set)
    tree.grid(row=0, column=0, sticky="nsew")
    vertical.grid(row=0, column=1, sticky="ns")
    horizontal.grid(row=1, column=0, sticky="ew")


# --- Regex Watch table styling -------------------------------------------------------
# The "superhero" theme leaves Treeview without an explicit row height, so Tk falls back to a
# flat 20px for a 10pt font whose linespace is 17px. Rows then show 3px of slack, which clips
# ascenders/descenders and makes the Regex Watch tables read as scrunched, overlapping text.
# Give the tables their own style with an explicit font and a row height measured from that
# font, and size the columns from the heading text so no header is truncated either.
REGEX_TREE_STYLE = 'RegexWatch.Treeview'
REGEX_TREE_BODY_FONT = ('Segoe UI', 10)
REGEX_TREE_HEADING_FONT = ('Segoe UI', 10, 'bold')


def _configure_regex_tree_style():
    """Define ``RegexWatch.Treeview`` with a row height that fits its font."""
    style = ttk.Style()
    root = getattr(app_state, 'root', None)
    body = font.Font(root=root, font=REGEX_TREE_BODY_FONT)
    scale = root.winfo_fpixels("1i") / 96.0 if root is not None else 1.0
    row_height = body.metrics("linespace") + max(8, round(10 * scale))
    style.configure(REGEX_TREE_STYLE, font=REGEX_TREE_BODY_FONT, rowheight=row_height)
    style.configure(REGEX_TREE_STYLE + '.Heading', font=REGEX_TREE_HEADING_FONT)
    return row_height


def _configure_regex_tree_columns(tree, columns, widths, stretch_columns=()):
    """Size columns from their heading text so no header is clipped.

    ``widths`` is the preferred width at 96 DPI; a column never goes below the width its
    own heading needs, so short headings such as ``#`` stay compact while ``Matched Text``
    gets room for real content.
    """
    scale = tree.winfo_fpixels("1i") / 96.0
    heading_font = font.Font(root=tree, font=REGEX_TREE_HEADING_FONT)
    for col in columns:
        preferred = max(1, round(widths.get(col, 110) * scale))
        # Heading text plus the theme's sort indicator/arrow padding.
        minimum = heading_font.measure(col) + round(24 * scale)
        tree.heading(col, text=col)
        tree.column(col, width=max(preferred, minimum), minwidth=minimum,
                    stretch=col in stretch_columns, anchor="w")


_configure_regex_tree_style()


for tab_name in tab_names:
    tab_frame = ttk.Frame(app_state.dashboard_notebook)
    app_state.dashboard_notebook.add(tab_frame, text=tab_name)
    app_state.dashboard_notebook.tabs_widgets[tab_name] = {}

    # Base grid setup for the tab
    tab_frame.columnconfigure(0, weight=1)
    tab_frame.columnconfigure(1, weight=1)
    tab_frame.rowconfigure(0, weight=0)  # Search bar row (fixed height)

    # 1. Create Search Bar Frame (applies to ALL tabs)
    search_frame = ttk.Frame(tab_frame)
    search_frame.grid(row=0, column=0, columnspan=2, sticky="ew", padx=SPACING, pady=(5, 2))

    search_var = tk.StringVar()
    search_entry = ttk.Entry(search_frame, textvariable=search_var)
    search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
    search_entry.bind("<Return>", lambda e, t=tab_name: search_in_dashboard_tab(t))

    ttk.Button(search_frame, text="🔍 Search", command=lambda t=tab_name: search_in_dashboard_tab(t)).pack(side=tk.LEFT, padx=SPACING)
    ttk.Button(search_frame, text="❌ Clear", command=lambda t=tab_name: clear_dashboard_search(t)).pack(side=tk.LEFT, padx=SPACING)
    ttk.Button(search_frame, text="📋 Copy All", command=lambda t=tab_name: copy_dashboard_tab(t)).pack(side=tk.LEFT, padx=SPACING)

    app_state.dashboard_notebook.tabs_widgets[tab_name]['search_var'] = search_var
    app_state.dashboard_notebook.tabs_widgets[tab_name]['search_entry'] = search_entry

    # Each page has an independent scroll region and one expanding viewport.
    page_scroll = ScrollableFrame(tab_frame)
    page_scroll.grid(row=1, column=0, columnspan=2, sticky="nsew")
    tab_frame.rowconfigure(1, weight=1)
    canvas = page_scroll.canvas
    scrollable_frame = page_scroll.content
    scrollable_frame.columnconfigure(0, weight=1)
    parent_frame = scrollable_frame
    app_state.dashboard_notebook.tabs_widgets[tab_name]['scrollable_frame'] = scrollable_frame
    app_state.dashboard_notebook.tabs_widgets[tab_name]['canvas'] = canvas

    # 3. Create Issue Panels (skip for custom tabs)
    if tab_name not in ("Quality", "🔍 Review", "🔎 Regex Watch"):
        panels_frame = ttk.Frame(parent_frame)
        panels_frame.grid(row=0, column=0, sticky="ew")
        panels = []
        for idx, issue_type_title in enumerate(issue_types):
            if tab_name == "Totals" and idx == 5:  # Skip "Errors" for Totals tab
                continue

            key = issue_keys[idx]
            columns = ("Time", "API", "Phrase", "Context") if tab_name == "Totals" else ("Time", "Phrase", "Context")
            color_map = {"refusals": "#ff4d6d", "user_speak": "#4dabf7", "slop": "#9775fa",
                         "anti_slop": "#ffd43b", "out_of_character": "#ff922b", "errors": "#fc8181"}

            panel = ttk.LabelFrame(panels_frame, text=f"Recent {issue_type_title}")
            panels.append(panel)

            panel_tree = create_modern_issue_panel(panel, columns, highlight_color=color_map.get(key, "#ff6b6b"))
            app_state.dashboard_notebook.tabs_widgets[tab_name][key] = panel_tree
        app_state.dashboard_notebook.tabs_widgets[tab_name]['panel_layout'] = ResponsiveGrid(
            panels_frame, panels, min_width=540, max_columns=2, padx=SPACING, pady=SPACING)

    # 4. Add Graph to Totals Tab
    if tab_name == "Totals":
        graph_frame = ttk.LabelFrame(scrollable_frame, text="Issue Detection Over Time (Last 60 Minutes)")
        graph_frame.grid(row=1, column=0, padx=SPACING, pady=(12, 5), sticky="ew")

        graph_canvas_widget = tk.Canvas(graph_frame, width=1, height=320, highlightthickness=0, bg='#1a1a1a')
        graph_canvas_widget.pack_propagate(False)
        graph_canvas_widget.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=SPACING)
        app_state.dashboard_notebook.tabs_widgets[tab_name]['graph_canvas'] = graph_canvas_widget
        draw_issue_graph(graph_canvas_widget)

        # FIX: Explicitly refresh the parent canvas scrollregion after the graph renders
        app_state.root.update_idletasks()
        parent_canvas = app_state.dashboard_notebook.tabs_widgets[tab_name]['canvas']
        parent_canvas.configure(scrollregion=parent_canvas.bbox("all"))

    elif tab_name == "Quality":
        # Summary labels + score-table columns follow the roleplay-dimensions toggle;
        # rebuilt on change by _rebuild_quality_roleplay_layout() from the update loop.
        _rebuild_quality_roleplay_layout(force=True)

    elif tab_name == "🔍 Review":
        # The Review tab has no issue panels, so its search bar would only search nothing.
        search_frame.grid_remove()
        # The Review tab is a fixed, non-scrolling split view: the flagged list on top and
        # the conversation detail below. Nesting a scrolling table and a scrolling Text in
        # the page's ScrollableFrame made both unusable (the page canvas swallowed the
        # wheel and squeezed the panes to their minimum heights), so this tab bypasses the
        # page scroll region and fills the tab frame directly.
        page_scroll.grid_remove()
        review_page = ttk.Frame(tab_frame)
        review_page.grid(row=1, column=0, columnspan=2, sticky="nsew")
        review_page.columnconfigure(0, weight=1)
        review_page.rowconfigure(1, weight=1, minsize=90)
        review_page.rowconfigure(3, weight=1, minsize=150)
        tab_frame.rowconfigure(1, weight=1)
        app_state.dashboard_notebook.tabs_widgets[tab_name]['review_page'] = review_page
        app_state.dashboard_notebook.tabs_widgets[tab_name]['scrollable_frame'] = review_page

        # --- Toolbar: filter + actions ---
        review_toolbar = ttk.Frame(review_page)
        review_toolbar.grid(row=0, column=0, sticky="ew", padx=SPACING, pady=(5, 4))
        review_toolbar.columnconfigure(0, weight=1)

        review_filter_var = tk.StringVar()
        review_filter_entry = ttk.Entry(review_toolbar, textvariable=review_filter_var, width=30)
        review_filter_entry.grid(row=0, column=0, sticky="ew", padx=(0, 5))
        review_filter_entry.bind("<Return>", lambda e: _update_review_table(force=True))
        ttk.Button(review_toolbar, text="🔍 Filter", command=lambda: _update_review_table(force=True)).grid(row=0, column=1)

        review_actions = ttk.Frame(review_toolbar)
        review_actions.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(4, 0))
        review_action_buttons = [
            ttk.Button(review_actions, text="📤 Export Flagged → JSONL", command=_export_flagged_review),
            ttk.Button(review_actions, text="✅ Dismiss All", command=_dismiss_all_review),
            ttk.Button(review_actions, text="🔄 Refresh", command=lambda: _update_review_table(force=True)),
        ]
        app_state.dashboard_notebook.tabs_widgets[tab_name]['action_layout'] = ResponsiveGrid(
            review_actions, review_action_buttons, min_width=190, max_columns=3)
        review_count_label = ttk.Label(review_toolbar, text="", foreground="#ff6b6b",
                                       font=('Segoe UI', 10, 'bold'))
        review_count_label.grid(row=2, column=0, columnspan=3, sticky="w", pady=(2, 0))
        app_state.dashboard_notebook.tabs_widgets[tab_name]['review_count_label'] = review_count_label

        # --- Flagged items table ---
        review_table_lf = ttk.LabelFrame(review_page, text="Flagged Conversations (Below Threshold)")
        review_table_lf.grid(row=1, column=0, sticky="nsew", padx=SPACING, pady=(0, 4))

        review_cols = ("Task ID", "Score", "Lowest Dim", "Flags", "Source File", "Scored At")
        review_tree = ttk.Treeview(review_table_lf, columns=review_cols, show="headings",
                                   height=6, selectmode="browse")
        for col in review_cols:
            review_tree.heading(col, text=col)
            review_tree.column(col, width=130, minwidth=90, stretch=False, anchor="w")
        review_tree.column("Task ID", width=300, minwidth=200, stretch=True)
        review_tree.column("Flags", width=230, minwidth=150, stretch=True)
        # Readable row height: the theme's default is cramped and clips descenders.
        style.configure('Review.Treeview', font=('Segoe UI', 10),
                        rowheight=font.Font(family='Segoe UI', size=10).metrics('linespace') + 10)
        style.configure('Review.Treeview.Heading', font=('Segoe UI', 10, 'bold'))
        review_tree.configure(style='Review.Treeview')

        _grid_scrollable_table(review_table_lf, review_tree)

        review_tree.tag_configure("critical", foreground="#ff6b6b")
        review_tree.tag_configure("warning", foreground="#fcc419")

        # A single click opens the conversation, not only a double click: the old binding
        # made rows feel dead until the user guessed they had to double-click.
        review_tree.bind("<<TreeviewSelect>>", lambda e: _show_review_detail(review_tree))

        app_state.dashboard_notebook.tabs_widgets[tab_name]['review_tree'] = review_tree
        app_state.dashboard_notebook.tabs_widgets[tab_name]['review_filter_var'] = review_filter_var

        # --- Detail panel ---
        detail_lf = ttk.LabelFrame(review_page, text="Conversation Detail (select a row above)")
        detail_lf.grid(row=3, column=0, sticky="nsew", padx=SPACING, pady=(0, SPACING))
        detail_lf.columnconfigure(0, weight=1)
        detail_lf.rowconfigure(0, weight=1)

        detail_text = scrolledtext.ScrolledText(detail_lf, wrap=tk.WORD, width=1, height=6,
                                                font=('Consolas', 10),
                                                bg='#1a1a2e', fg='#e0e0e0', relief=tk.FLAT,
                                                padx=8, pady=6)
        detail_text.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
        detail_text.tag_configure("detail_head", foreground="#bb86fc", font=('Consolas', 10, 'bold'))
        detail_text.tag_configure("detail_meta", foreground="#8b949e")
        detail_text.tag_configure("detail_rule", foreground="#444466")
        detail_text.tag_configure("role_user", foreground="#4fc3f7", font=('Consolas', 10, 'bold'))
        detail_text.tag_configure("role_assistant", foreground="#69f0ae", font=('Consolas', 10, 'bold'))
        detail_text.tag_configure("role_system", foreground="#ffd43b", font=('Consolas', 10, 'bold'))
        detail_text.insert(tk.END, "Select a flagged conversation above to read it here.", "detail_meta")
        detail_text.configure(state=tk.DISABLED)
        app_state.dashboard_notebook.tabs_widgets[tab_name]['detail_text'] = detail_text

    elif tab_name == "🔎 Regex Watch":
        # Regex Watch is a diagnostics area, not an issue panel: it shows which configured
        # expressions are actually firing, how often, and what the rewrite did with each hit.
        # It has no search bar (the pattern table has its own filter) and no issue panels.
        search_frame.grid_remove()
        regex_page = ttk.Frame(tab_frame)
        regex_page.grid(row=1, column=0, columnspan=2, sticky="nsew")
        regex_page.columnconfigure(0, weight=1)
        regex_page.rowconfigure(2, weight=1, minsize=120)
        regex_page.rowconfigure(4, weight=1, minsize=120)
        tab_frame.rowconfigure(1, weight=1)
        page_scroll.grid_remove()
        app_state.dashboard_notebook.tabs_widgets[tab_name]['regex_page'] = regex_page
        app_state.dashboard_notebook.tabs_widgets[tab_name]['scrollable_frame'] = regex_page

        regex_header = ttk.Frame(regex_page)
        regex_header.grid(row=0, column=0, sticky="ew", padx=SPACING, pady=(5, 4))
        regex_header.columnconfigure(0, weight=1)
        app_state.regex_watch_status_label = ttk.Label(
            regex_header, text="Regex Watch is off. Enable it in the Configuration Editor "
                               "(Detection tab) to rewrite regex-shaped prose.",
            style='Small.TLabel', justify="left", wraplength=1)
        app_state.regex_watch_status_label.grid(row=0, column=0, sticky="ew")
        wrap_label(app_state.regex_watch_status_label, regex_header)
        ttk.Button(regex_header, text="🔄 Refresh",
                   command=lambda: _update_regex_watch_tab(force=True)).grid(row=0, column=1, padx=(SPACING, 0))

        # --- Configured patterns and their hit counts ---
        # Both tables below use the dedicated RegexWatch.Treeview style (built once, above this
        # loop): the theme default packs a 10pt font into a 20px row, so every cell was clipped
        # and the text looked scrunched. The style sets an explicit font and a row height derived
        # from the font's linespace, the same fix the Review and Queue tables already use.
        patterns_lf = ttk.LabelFrame(regex_page, text="Configured Patterns (from the Configuration Editor)")
        patterns_lf.grid(row=2, column=0, sticky="nsew", padx=SPACING, pady=(0, 4))
        pattern_cols = ("#", "Hits", "Last Status", "Pattern")
        regex_pattern_tree = ttk.Treeview(patterns_lf, columns=pattern_cols, show="headings",
                                         height=7, selectmode="browse", style=REGEX_TREE_STYLE)
        _configure_regex_tree_columns(regex_pattern_tree, pattern_cols,
                                      widths={"#": 56, "Hits": 70, "Last Status": 130, "Pattern": 520},
                                      stretch_columns=("Pattern",))
        regex_pattern_tree.tag_configure("unresolved", foreground="#ff6b6b")
        regex_pattern_tree.tag_configure("rewritten", foreground="#69f0ae")
        _grid_scrollable_table(patterns_lf, regex_pattern_tree)
        app_state.dashboard_notebook.tabs_widgets[tab_name]['regex_pattern_tree'] = regex_pattern_tree

        # --- Recent hits and what the rewrite did ---
        hits_lf = ttk.LabelFrame(regex_page, text="Recent Hits (newest last)")
        hits_lf.grid(row=4, column=0, sticky="nsew", padx=SPACING, pady=(0, SPACING))
        hit_cols = ("Time", "API", "Status", "Pattern", "Matched Text")
        regex_hit_tree = ttk.Treeview(hits_lf, columns=hit_cols, show="headings",
                                      height=7, selectmode="browse", style=REGEX_TREE_STYLE)
        _configure_regex_tree_columns(regex_hit_tree, hit_cols,
                                      widths={"Time": 90, "API": 80, "Status": 130,
                                              "Pattern": 260, "Matched Text": 340},
                                      stretch_columns=("Pattern", "Matched Text"))
        regex_hit_tree.tag_configure("unresolved", foreground="#ff6b6b")
        regex_hit_tree.tag_configure("rewritten", foreground="#69f0ae")
        _grid_scrollable_table(hits_lf, regex_hit_tree)
        app_state.dashboard_notebook.tabs_widgets[tab_name]['regex_hit_tree'] = regex_hit_tree


# --- Queue Management Tab ---
queue_tab = ttk.Frame(app_state.dashboard_notebook)
app_state.dashboard_notebook.add(queue_tab, text="📦 Queue")


def _on_dashboard_tab_changed(_event=None):
    """Refresh the newly-visible tab at once.

    The per-tick issue-panel / graph / review-table refreshes are gated to the
    currently selected tab (to keep X server resource use bounded), so on a tab
    switch the incoming tab would otherwise show stale content until the next
    update cycle.
    """
    try:
        update_dashboard()
        _update_review_table()
        update_quality_tab()
        _update_regex_watch_tab(force=True)
    except Exception as e:
        log_message(f"Dashboard tab-change refresh error: {e}", "ERROR")


app_state.dashboard_notebook.bind("<<NotebookTabChanged>>", _on_dashboard_tab_changed, add="+")

# The queue shares the same scoped scroll/focus behavior as other pages.
queue_scroll = ScrollableFrame(queue_tab)
queue_scroll.pack(fill=tk.BOTH, expand=True)
queue_canvas = queue_scroll.canvas
queue_scrollable_frame = queue_scroll.content

def create_queue_panel(parent, title, columns):
    frame = ttk.LabelFrame(parent, text=title)
    frame.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=5)

    # Inner frame to cleanly hold the tree + scrollbars
    inner = ttk.Frame(frame)
    inner.pack(fill=tk.BOTH, expand=True)
    inner.columnconfigure(0, weight=1)
    inner.rowconfigure(0, weight=1)

    # Override theme compression with explicit font and row spacing
    style.configure('Queue.Treeview', font=('Segoe UI', 10),
                    rowheight=font.Font(family='Segoe UI', size=10).metrics('linespace') + 8)
    style.configure('Queue.Treeview.Heading', font=('Segoe UI', 10, 'bold'))

    tree = ttk.Treeview(inner, columns=columns, show="headings", height=12, style='Queue.Treeview')

    for col in columns:
        tree.heading(col, text=col)
        # stretch=False prevents columns from shrinking/compressing on window resize
        if col == "Retries":
            tree.column(col, width=80, minwidth=50, stretch=True, anchor="center")
        else:
            tree.column(col, width=400, minwidth=150, stretch=True, anchor="w")

    vsb = tk.Scrollbar(inner, orient="vertical", command=tree.yview,
                       width=20, highlightthickness=0, bd=0,
                       bg='#999999', troughcolor='#555555',
             activebackground='#bbbbbb', relief='flat')
    hsb = tk.Scrollbar(inner, orient="horizontal", command=tree.xview,
                       width=20, highlightthickness=0, bd=0,
                       bg='#999999', troughcolor='#555555',
             activebackground='#bbbbbb', relief='flat')
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

    # Grid layout is significantly more stable for Treeviews + Scrollbars than pack
    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")

    return tree

def update_queue_ui():
    if not app_state.root.winfo_exists() or not app_state.queue_trees: return
    for tree in app_state.queue_trees.values():
        for item in tree.get_children(): tree.delete(item)

    with app_state.task_queue_ui_lock:
        for tid in app_state.pending_task_ids:
            m = app_state.task_metadata.get(tid, {})
            app_state.queue_trees['pending'].insert("", tk.END, values=(tid, m.get('file', 'N/A'), m.get('retries', 0)))
        for tid in app_state.completed_task_ids:
            m = app_state.task_metadata.get(tid, {})
            app_state.queue_trees['completed'].insert("", tk.END, values=(tid, m.get('file', 'N/A'), m.get('retries', 0)))
        for tid in app_state.failed_task_ids:
            m = app_state.task_metadata.get(tid, {})
            app_state.queue_trees['failed'].insert("", tk.END, values=(tid, m.get('file', 'N/A'), m.get('retries', 0)))

def purge_queue():
    if messagebox.askyesno("Purge Queue", "Remove all pending tasks? They will be skipped by workers."):
        with app_state.task_queue_ui_lock:
            app_state.purged_task_ids.update(app_state.pending_task_ids)
            app_state.pending_task_ids.clear()
        log_message("Purged all pending tasks from UI tracking.", "INFO")
        update_queue_ui()

def retry_failed_tasks():
    if not app_state.failed_task_ids:
        messagebox.showinfo("Info", "No failed tasks to retry.")
        return
    if messagebox.askyesno("Retry Failed", f"Retry {len(app_state.failed_task_ids)} failed tasks?"):
        with app_state.task_queue_ui_lock:
            to_retry = list(app_state.failed_task_ids)
            app_state.failed_task_ids.clear()
            for tid in to_retry:
                app_state.pending_task_ids.add(tid)
                meta = app_state.task_metadata.get(tid, {})
                meta['retries'] = 0
                app_state.task_metadata[tid] = meta
                if 'task_data' in meta:
                    app_state.task_queue.put(meta['task_data'])
        log_message(f"Re-queued {len(to_retry)} failed tasks.", "INFO")
        update_queue_ui()

def export_queue():
    import csv
    out_path = os.path.join(app_state.OUTPUT_DIR, 'queue_export.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Status', 'Task ID', 'Source File', 'Retries'])
        with app_state.task_queue_ui_lock:
            for tid in app_state.pending_task_ids:
                m = app_state.task_metadata.get(tid, {})
                writer.writerow(['Pending', tid, m.get('file', 'N/A'), m.get('retries', 0)])
            for tid in app_state.completed_task_ids:
                m = app_state.task_metadata.get(tid, {})
                writer.writerow(['Completed', tid, m.get('file', 'N/A'), m.get('retries', 0)])
            for tid in app_state.failed_task_ids:
                m = app_state.task_metadata.get(tid, {})
                writer.writerow(['Failed', tid, m.get('file', 'N/A'), m.get('retries', 0)])
    toast.show(f"Queue exported to {out_path}", "success")

queue_btn_frame = ttk.Frame(queue_scrollable_frame)
queue_btn_frame.pack(fill=tk.X, padx=SPACING, pady=SPACING)
queue_actions = [
    ttk.Button(queue_btn_frame, text="🗑️ Purge Queue", command=purge_queue),
    ttk.Button(queue_btn_frame, text="🔄 Retry Failed", command=retry_failed_tasks),
    ttk.Button(queue_btn_frame, text="📤 Export Queue", command=export_queue),
    ttk.Button(queue_btn_frame, text="🔄 Refresh View", command=update_queue_ui),
]
queue_action_layout = ResponsiveGrid(queue_btn_frame, queue_actions, min_width=150, max_columns=4)

queue_cols = ("Task ID", "Source File", "Retries")
app_state.queue_trees['pending']   = create_queue_panel(queue_scrollable_frame, "Pending", queue_cols)
app_state.queue_trees['completed'] = create_queue_panel(queue_scrollable_frame, "Completed", queue_cols)
app_state.queue_trees['failed']    = create_queue_panel(queue_scrollable_frame, "Failed", queue_cols)
# --- End Queue Management Tab ---

dashboard_navigator.refresh()

ConfigEditor.update_dashboard_safe = update_dashboard_safe # Make it accessible from ConfigEditor instance

def init_database_pool():
    """Initializes PostgreSQL connection pool at app startup."""
    if global_config.get('database.enabled', False):
        try:
            from psycopg2 import pool
            app_state.db_pool = pool.ThreadedConnectionPool(
                minconn=2, maxconn=global_config.get('database.pool_size', 10),
                connect_timeout=5,
                host=global_config.get('database.host'), port=global_config.get('database.port'),
                dbname=global_config.get('database.dbname'), user=global_config.get('database.user'),
                password=global_config.get('database.password')
            )
            log_message("PostgreSQL connection pool initialized at startup.", "INFO")
            # Update status after initialization
            if app_state.root.winfo_exists():
                app_state.root.after(100, update_database_status)
        except Exception as e:
            log_message(f"Failed to init DB pool at startup: {e}", "ERROR")
            app_state.db_pool = None
            # Update status to show disconnected
            if app_state.root.winfo_exists():
                app_state.root.after(100, update_database_status)
    else:
        app_state.db_pool = None
        log_message("PostgreSQL database disabled in config.", "INFO")
        if app_state.root.winfo_exists():
            app_state.root.after(100, update_database_status)

reset_all_stats_and_history() # Initialize stats on startup
update_dashboard() # Initial dashboard display
init_database_pool() # <-- NEW: Initialize DB on launch
update_database_status()  # NEW: Initial status update

app_state.root.after(1000, update_thread_status_display)

if __name__ == "__main__":
    try:
        app_state.root.mainloop() # Start the Tkinter event loop
    except Exception as e: 
        error_message = f"Critical error in main execution: {str(e)}"
        log_message(error_message, "CRITICAL")
        with open(os.path.join(OUTPUT_DIR, "CRITICAL_ERROR.txt"), "w", encoding='utf-8') as f_err:
            f_err.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {error_message}\n")
            import traceback
            traceback.print_exc(file=f_err)
    finally: 
        log_message("Application exiting via main finally block.", "INFO")
        if not app_state.stop_processing: # If not already stopped (e.g., by Quit button)
            app_state.stop_processing = True # Signal threads to stop
            if app_state.task_queue and app_state.threads:
                for _ in range(len(app_state.threads)): 
                    try:
                        if app_state.task_queue: app_state.task_queue.put(None, block=False, timeout=0.05)
                    except: pass # Ignore errors here, best effort to stop threads
        
        log_message("Main finally block attempting to save state.", "INFO")
        save_generation_state() # Final attempt to save state
        time.sleep(0.1) # Brief pause to allow file operations
        log_message("Main finally block executed. Process should exit if not already via sys.exit().", "INFO")
