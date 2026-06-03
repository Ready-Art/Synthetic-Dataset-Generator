# generate.py
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
import app_state
from app_state import (
    global_config, API_CIRCUIT_BREAKER, api_circuit_breaker_lock, task_retry_counts, task_retry_lock, BASE_DEBUG_LOG_PATH, BASE_OUTPUT_FILE_PATH, MAX_RECENT, MAX_TASK_REQUEUES, STATE_FILE_PATH, OUTPUT_DIR, INPUT_DIR, anti_slop_counts_per_api, estimated_cost,
)
from config_editor import ConfigEditor
matplotlib.use('TkAgg')
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from urllib.parse import urlparse
from api_handler import RateLimiter, global_rate_limiter, get_cached_response, set_cached_response, api_response_times_per_slot, api_response_times_lock, MAX_RESPONSE_TIMES_TO_TRACK
import logging_config
from logging_config import log_message, LOG_FILE_PATH

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

def check_budget_limit():
    if app_state.stop_processing:
        return False

    budget_limit = global_config.get('api.pricing.budget_limit', 0.0)
    if budget_limit <= 0:
        return False  # Budget disabled

    with app_state.stats_lock:  # 🔒 Safer concurrent read
        price_per_1k = global_config.get('api.pricing.cost_per_1k_tokens', 0)
        current_cost = (app_state.total_input_tokens + app_state.total_output_tokens) * (price_per_1k / 1000.0)

    if current_cost >= budget_limit:
        log_message(f"API budget limit of ${budget_limit:.2f} reached. Current cost: ${current_cost:.2f}. Stopping generation.", "WARNING")
        app_state.stop_processing = True
        return True
    return False


def check_circuit_breaker(api_slot_idx):
    """Returns True if API slot is allowed to make requests, False if circuit is open."""
    with api_circuit_breaker_lock:
        if API_CIRCUIT_BREAKER["is_open"][api_slot_idx]:
            elapsed = time.time() - API_CIRCUIT_BREAKER["last_failure_time"][api_slot_idx]
            # NEW: Use per-slot cooldown instead of global cooldown
            current_cooldown = API_CIRCUIT_BREAKER["current_cooldown"].get(api_slot_idx, 60)

            if elapsed >= current_cooldown:
                API_CIRCUIT_BREAKER["is_open"][api_slot_idx] = False
                API_CIRCUIT_BREAKER["failures"][api_slot_idx] = 0
                API_CIRCUIT_BREAKER["current_cooldown"][api_slot_idx] = API_CIRCUIT_BREAKER["base_cooldown_seconds"]
                log_message(
                    f"API Slot {api_slot_idx+1} circuit closed after {elapsed:.0f}s. "
                    f"Resuming requests with base cooldown.",
                    "INFO"
                )
                return True
            return False
        return True

def record_api_failure(api_slot_idx):
    with api_circuit_breaker_lock:
        failures = API_CIRCUIT_BREAKER["failures"][api_slot_idx] + 1
        API_CIRCUIT_BREAKER["failures"][api_slot_idx] = failures
        API_CIRCUIT_BREAKER["last_failure_time"][api_slot_idx] = time.time()

        # NEW: Calculate exponential backoff (60s, 120s, 240s, 480s, max 600s)
        backoff = min(
            API_CIRCUIT_BREAKER["max_cooldown_seconds"],
            API_CIRCUIT_BREAKER["base_cooldown_seconds"] * (2 ** (failures - 1))
        )
        API_CIRCUIT_BREAKER["current_cooldown"][api_slot_idx] = backoff

        if failures >= API_CIRCUIT_BREAKER["max_consecutive_failures"]:
            API_CIRCUIT_BREAKER["is_open"][api_slot_idx] = True
            log_message(
                f"API Slot {api_slot_idx+1} circuit OPEN after {failures} consecutive failures. "
                f"Backoff: {backoff}s (exponential)",
                "WARNING"
            )

def record_api_success(api_slot_idx):
    """Reset failure count on successful API call."""
    with api_circuit_breaker_lock:
        if API_CIRCUIT_BREAKER["failures"][api_slot_idx] > 0:
            API_CIRCUIT_BREAKER["failures"][api_slot_idx] = 0
            API_CIRCUIT_BREAKER["current_cooldown"][api_slot_idx] = API_CIRCUIT_BREAKER["base_cooldown_seconds"]
            log_message(f"API Slot {api_slot_idx+1} success - failure count reset.", "DEBUG")

# --- Resilience: requeue tasks that fail while their API host is down ---
# When a host goes down the circuit breaker (above) opens for that slot. Without
# requeueing, a worker would consume each pulled task, fail it, and discard it,
# draining the queue so nothing is left to process when the host recovers. These
# helpers let the worker put such tasks back, bounded so a genuinely-bad task
# (one that fails for content reasons while the host is up) can't loop forever.

def api_host_is_down(api_slot_idx):
    """Read-only: True if this slot's circuit is currently open (host down).
    Unlike check_circuit_breaker(), this does NOT close the circuit on cooldown."""
    if api_slot_idx is None or api_slot_idx < 0:
        return False
    with api_circuit_breaker_lock:
        return bool(API_CIRCUIT_BREAKER["is_open"].get(api_slot_idx, False))

def requeue_task(q, task, task_id):
    """Put a task back on the queue for a later retry (host-outage recovery).
    Returns True if requeued, False once it has exhausted MAX_TASK_REQUEUES."""
    with task_retry_lock:
        n = task_retry_counts.get(task_id, 0) + 1
        task_retry_counts[task_id] = n
        if n > MAX_TASK_REQUEUES:
            return False
    q.put(task)
    return True

# --- Crash Recovery Functions ---
def save_generation_state():
    """Saves the current generation state to a JSON file for potential recovery."""
    global api_response_times_per_slot

    with app_state.state_file_lock: # Ensure thread-safe file writing
        try:
            state_data = {
                'completed_task_ids': list(app_state.completed_task_ids), # Convert set to list for JSON
                'system_prompt_counter': app_state.system_prompt_counter,
                'question_history': app_state.question_history,
                'total_attempts_global': app_state.total_attempts_global,
                'refusal_count_total': app_state.refusal_count_total,
                'user_speaking_count_total': app_state.user_speaking_count_total,
                'slop_count_total': app_state.slop_count_total,
                'error_count_total': app_state.error_count_total,
                'refusal_counts_per_api': app_state.refusal_counts_per_api,
                'total_input_tokens': app_state.total_input_tokens,
                'total_output_tokens': app_state.total_output_tokens,
                'estimated_cost': estimated_cost,
                'user_speaking_counts_per_api': app_state.user_speaking_counts_per_api,
                'slop_counts_per_api': app_state.slop_counts_per_api,
                'error_counts_per_api': app_state.error_counts_per_api,
                'total_attempts_per_api': app_state.total_attempts_per_api,
                'anti_slop_count_total': app_state.anti_slop_count_total,
                'anti_slop_counts_per_api': anti_slop_counts_per_api,
                # Snapshot of critical config settings at the time of saving state
                'config_snapshot': {
                    'prompts.use_questions_file': global_config.get('prompts.use_questions_file'),
                    'generation.num_turns': global_config.get('generation.num_turns', 1),
                    'generation.subject_size': global_config.get('generation.subject_size', 1000),
                    'generation.context_size': global_config.get('generation.context_size', 3000),
                    'api.master_duplication_mode': global_config.get('api.master_duplication_mode', False)
                }
            }
            # If in master duplication mode and task queue has per-API progress, save it
            # Also save overall progress if not in duplication mode
            if app_state.task_queue:
                if global_config.get('api.master_duplication_mode', False) and hasattr(app_state.task_queue, 'api_processed_tasks'):
                    state_data['api_processed_tasks_snapshot'] = dict(app_state.task_queue.api_processed_tasks)
                elif not global_config.get('api.master_duplication_mode', False) and hasattr(app_state.task_queue, 'processed_tasks'):
                    state_data['processed_tasks_snapshot'] = app_state.task_queue.processed_tasks


            with open(STATE_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, indent=4)
            log_message("Generation state saved.", "INFO")
        except Exception as e:
            log_message(f"Error saving generation state: {e}", "ERROR")

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
                app_state.question_history = state_data.get('question_history', [])
                
                app_state.total_attempts_global = state_data.get('total_attempts_global', 0)
                app_state.refusal_count_total = state_data.get('refusal_count_total', 0)
                app_state.user_speaking_count_total = state_data.get('user_speaking_count_total', 0)
                app_state.slop_count_total = state_data.get('slop_count_total', 0)
                app_state.error_count_total = state_data.get('error_count_total', 0)
                anti_slop_count_total = state_data.get('anti_slop_count_total', 0)
                anti_slop_counts_per_api = load_per_api_stat('anti_slop_counts_per_api', lambda: 0)

                app_state.refusal_counts_per_api = load_per_api_stat('refusal_counts_per_api', lambda: 0)
                app_state.user_speaking_counts_per_api = load_per_api_stat('user_speaking_counts_per_api', lambda: 0)
                app_state.slop_counts_per_api = load_per_api_stat('slop_counts_per_api', lambda: 0)
                app_state.error_counts_per_api = load_per_api_stat('error_counts_per_api', lambda: 0)
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
    app_state.question_history = []

    with api_response_times_lock:
        for i in range(6):
            api_response_times_per_slot[i] = []

    app_state.total_attempts_global = 0
    app_state.refusal_count_total = 0
    app_state.user_speaking_count_total = 0
    app_state.slop_count_total = 0
    app_state.error_count_total = 0
    anti_slop_count_total = 0

    for i in range(6): # For all 6 API slots
        app_state.refusal_counts_per_api[i] = 0
        app_state.user_speaking_counts_per_api[i] = 0
        app_state.slop_counts_per_api[i] = 0
        app_state.error_counts_per_api[i] = 0
        anti_slop_counts_per_api[i] = 0
        app_state.total_attempts_per_api[i] = 0
    
    for i in range(4): # For API slots 0-3 (main generation)
        app_state.recent_refusals_per_api[i] = []
        app_state.recent_user_speaking_per_api[i] = []
        app_state.recent_slop_per_api[i] = []
        app_state.recent_errors_per_api[i] = []
        app_state.recent_anti_slop_per_api[i] = []

    app_state.recent_refusals_total = []
    app_state.recent_user_speaking_total = []
    app_state.recent_slop_total = []
    recent_anti_slop_total = []
    app_state.recent_errors_total = []
    
    app_state.loaded_api_processed_tasks_snapshot = None # Clear any loaded snapshot
    loaded_processed_tasks_snapshot = None # Clear snapshot for non-duplication
    log_message("All global statistics, history, and progress trackers have been reset.", "INFO")


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


def update_question_history(question, current_history_size):
    """Adds a question to the history and ensures it doesn't exceed the configured size."""
    app_state.question_history.append(question)
    if len(app_state.question_history) > current_history_size:
        app_state.question_history.pop(0) # Remove the oldest question

def estimate_time_remaining(processed_items, total_items, times_list):
    """Estimates the time remaining using a robust trimmed mean for stability."""
    if not times_list or processed_items < 1 or total_items == 0:
        return "Estimating..."

    # Require a minimum number of samples for a reliable estimate
    if len(times_list) < 3:
        return "Estimating..."

    # 1. Filter out extreme outliers to prevent "wonky" jumps
    # We keep only values within 0.5x to 3.0x the median
    sorted_times = sorted(times_list)
    median = sorted_times[len(sorted_times) // 2]
    filtered_times = [t for t in times_list if 0.5 * median <= t <= 3.0 * median]

    # Fallback to original list if filtering accidentally removes everything
    if not filtered_times:
        filtered_times = times_list

    # 2. Use a simple average of the filtered times
    # A filtered mean is significantly smoother for ETAs than a raw EMA
    avg_time_per_item = sum(filtered_times) / len(filtered_times)

    remaining_items = total_items - processed_items
    if remaining_items <= 0:
        return "Done!"

    remaining_time_seconds = remaining_items * avg_time_per_item

    # Format as H:M:S
    return time.strftime('%H:%M:%S', time.gmtime(remaining_time_seconds))

def clear_database():
    """Clears all data from the PostgreSQL generated_conversations table."""

    # Confirmation dialog: returns True for "Yes", False for "No"
    if not messagebox.askyesno("Confirm Clear Database", "Are you sure you want to clear the database? This action cannot be undone."):
        return  # User selected "No", so exit without doing anything

    if not app_state.db_pool:
        messagebox.showwarning("Database Error", "Database pool is not initialized.")
        log_message("Database pool not initialized.", "ERROR")
        return

    try:
        conn = app_state.db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE generated_conversations RESTART IDENTITY;")
            conn.commit()
        app_state.db_pool.putconn(conn)
        messagebox.showinfo("Success", "Database cleared successfully!")
        log_message("Database cleared successfully.", "INFO")
    except Exception as e:
        log_message(f"Failed to clear database: {e}", "ERROR")
        messagebox.showerror("Database Error", f"Failed to clear database: {e}")
        if conn:
            app_state.db_pool.putconn(conn)

# --- Core Worker Logic ---
def worker(thread_id, q, output_data_lock, use_questions_file_local,
           use_variable_system_local,
           all_api_configs_local,
           active_enabled_api_configs_for_worker,
           current_question_prompt, current_answer_prompt, current_user_continuation_prompt,
           current_num_turns,
           current_system_prompts_for_worker,
           current_refusal_phrases, current_user_speaking_phrases, current_slop_phrases,
           current_anti_slop_phrases,
           current_anti_slop_fixes,
           current_jailbreaks, current_speaking_fixes, current_slop_fixes_fallback,
           current_max_attempts, current_history_size_local, current_remove_reasoning,
           current_remove_em_dash,
           current_remove_asterisks,
           current_remove_asterisk_space_asterisk,
           current_remove_all_asterisks,
           current_ensure_space_after_line_break,
           current_remove_markdown,
           current_output_format,
           slop_fixer_api_config,
           anti_slop_fixer_api_config_runtime,
           anti_slop_fixer_api_config_param,
           current_slop_fixes_for_rotation_worker,
           current_top_level_system_prompt,
           master_duplication_enabled_local,
           enable_character_engine_local,
           enable_class_selection_local,
           character_list,
           enable_emotional_states_local,
           emotional_states_list_local,
           num_characters_local,
           no_user_impersonation_local,
           current_api_request_timeout):
    """
    The main function executed by each worker thread.
    It fetches tasks from the queue, processes them by interacting with LLMs 
    (handling duplication or collaborative API use based on settings),
    and manages retries, issue detection (refusals, user speaking, slop), and output writing.
    """

    log_message(f"Thread {thread_id}: Worker started.", "DEBUG")

    # Resilience: in non-duplication mode each worker is pinned to one API slot
    # (thread_id % num_active_apis). Capture it up-front so we can pause -- not
    # drain -- the queue when that host's circuit is open. -1 = duplication/unknown.
    worker_api_slot = -1
    if not master_duplication_enabled_local and active_enabled_api_configs_for_worker:
        worker_api_slot = active_enabled_api_configs_for_worker[
            thread_id % len(active_enabled_api_configs_for_worker)]['original_slot_idx']

    while not app_state.stop_processing:
        if check_budget_limit():
            log_message(f"Thread {thread_id}: Budget limit reached. Exiting worker.", "INFO")
            break
        # Enhanced pause check: threads sleep briefly while paused, allowing GUI to remain responsive
        while app_state.pause_processing and not app_state.stop_processing:
            time.sleep(0.1) 
            if hasattr(app_state.root, 'update') and app_state.root.winfo_exists(): # Keep Tkinter main loop alive if paused
                try: app_state.root.update()
                except tk.TclError: log_message(f"Thread {thread_id}: Root window closed during pause.", "DEBUG"); pass
            if app_state.stop_processing: break

        if app_state.stop_processing: break # Exit if stop signal received during pause

        try:
            task = q.get(timeout=0.05) # Fetch a task from the queue with a short timeout
        except Empty:
            # If queue is empty and all tasks have been added, and processing_active is false (e.g. due to stop signal)
            if q.empty() and getattr(q, 'all_tasks_queued', False) and not app_state.processing_active:
                log_message(f"Thread {thread_id}: Queue empty, all tasks queued, processing not active. Exiting.", "DEBUG")
                break 
            continue # Continue to next iteration if queue is temporarily empty

        log_message(f"Thread {thread_id}: pause_processing={app_state.pause_processing}, stop_processing={app_state.stop_processing}", "DEBUG")

        if task is None: # Sentinel value received, indicating thread should terminate
            q.task_done()
            log_message(f"Thread {thread_id}: Sentinel received. Exiting.", "DEBUG")
            break 
        if app_state.stop_processing: 
            q.task_done(); break # Exit if stop signal received after fetching a task

        task_id, file_name, *_ = task # Unpack task data
        start_time_task_overall = time.time() # For timing the whole task processing

        with output_data_lock:
            if task_id in app_state.completed_task_ids: # Skip if task was already completed (e.g., from a resumed session)
                log_message(f"Thread {thread_id}: Skipping already completed task {task_id}.", "INFO")
                q.task_done()
                # If resuming, need to update progress bars for skipped tasks
                if hasattr(q, 'processed_tasks_lock'):
                    with q.processed_tasks_lock:
                        # A completed task means all its turns are done.
                        # current_num_turns is passed to worker and available here.
                        increment_amount = current_num_turns
                        if master_duplication_enabled_local:
                            # In duplication mode, increment progress for each enabled API (0-3)
                            for api_idx_skip, api_conf_skip in enumerate(all_api_configs_local):
                                if api_idx_skip < 4 and api_conf_skip.get('enabled', False): # Only for enabled generation APIs
                                    if api_idx_skip not in q.api_processed_tasks: q.api_processed_tasks[api_idx_skip] = 0
                                    # Increment by num_turns, ensuring not to over-increment against the new total_tasks_for_progress
                                    target_processed_for_this_api = q.api_processed_tasks[api_idx_skip] + increment_amount
                                    q.api_processed_tasks[api_idx_skip] = min(target_processed_for_this_api, q.total_tasks_for_progress)
                        else:
                            # In non-duplication mode, increment overall progress
                            if not hasattr(q, 'processed_tasks'): q.processed_tasks = 0
                            target_processed_overall = q.processed_tasks + increment_amount
                            q.processed_tasks = min(target_processed_overall, q.total_tasks_for_progress)
                continue

        # --- Resilience gate: if this worker's API host is down (circuit open),
        # put the task back and back off instead of consuming it. This pauses the
        # queue during an outage so the run resumes automatically when the host
        # returns, rather than draining (and discarding) tasks while it's down. ---
        if worker_api_slot >= 0 and not check_circuit_breaker(worker_api_slot):
            q.put(task)
            q.task_done()
            log_message(f"Thread {thread_id}: API Slot {worker_api_slot+1} down (circuit open); "
                        f"requeued task {task_id} and backing off.", "DEBUG")
            time.sleep(min(5.0, max(1.0, API_CIRCUIT_BREAKER['base_cooldown_seconds'] / 4.0)))
            continue

        try:
            if app_state.stop_processing: q.task_done(); continue # Check again before intensive processing

            # Determine system prompt for this task
            current_system_prompt_for_task = ""

            # FIXED: Properly handle system prompt selection with character engine
            if use_variable_system_local and current_system_prompts_for_worker:
                # Use a random system prompt variation
                current_system_prompt_for_task = get_next_system_prompt(current_system_prompts_for_worker)
                log_message(f"Thread {thread_id}: Selected random system prompt variation for task {task_id}", "DEBUG")
            elif current_system_prompts_for_worker and len(current_system_prompts_for_worker) > 0:
                # Use the base system prompt (first in list)
                current_system_prompt_for_task = current_system_prompts_for_worker[0]
                log_message(f"Thread {thread_id}: Using base system prompt for task {task_id}", "DEBUG")
            else: # Fallback if no system prompts are configured
                current_system_prompt_for_task = "You are a helpful assistant."
                log_message(f"Thread {thread_id}: Using fallback system prompt for task {task_id}", "DEBUG")

            # --- NEW: Prepend Top Level System Prompt ---
            if current_top_level_system_prompt:
                current_system_prompt_for_task = current_top_level_system_prompt + "\n\n" + current_system_prompt_for_task
                log_message(f"Thread {thread_id}: Prepending Top Level System Prompt for task {task_id}", "DEBUG")

            character_injection = ""
            if enable_character_engine_local and character_list:
                # Select multiple random characters based on num_characters_local
                num_chars_to_select = min(num_characters_local, len(character_list))
                selected_chars = random.sample(character_list, num_chars_to_select)

                character_profiles = []
                for idx, selected_char in enumerate(selected_chars):
                    # Extract attributes with defaults
                    random_name = selected_char.get('name', f'Character{idx+1}')
                    random_race = selected_char.get('race', 'Unknown')
                    random_job = selected_char.get('job', 'Unknown')
                    random_clothing = selected_char.get('clothing', 'Unknown')
                    random_appearance = selected_char.get('appearance', 'Unknown')
                    random_backstory = selected_char.get('backstory', 'Unknown')
                    random_personality = selected_char.get('personality', 'Unknown')
                    random_setting = selected_char.get('setting', 'A standard indoor environment.')
                    random_class = selected_char.get('class', '')

                    # Ensure at least name is present to make it valid
                    if random_name and random_name != 'Unknown':
                        random_age = random.randint(18, 50)

                        class_injection = ""
                        if enable_class_selection_local and random_class:
                            class_injection = f"\nClass: {random_class}\n"

                        personality_injection = ""
                        if random_personality and random_personality != 'Unknown':
                            personality_injection = f"\nPersonality: {random_personality}\n"

                        character_profile = (
                            f"\n--- CHARACTER {idx+1} PROFILE ---\n"
                            f"Name: {random_name}\n"
                            f"Race: {random_race}\n"
                            f"Age: {random_age}\n"
                            f"Job: {random_job}\n"
                            f"Clothing: {random_clothing}\n"
                            f"Appearance: {random_appearance}\n"
                            f"Backstory: {random_backstory}\n"
                            f"{personality_injection}"
                            f"Setting: {random_setting}\n"
                            f"{class_injection}"
                            f"--- END CHARACTER {idx+1} PROFILE ---\n"
                        )
                        character_profiles.append(character_profile)
                    else:
                        log_message(f"Thread {thread_id}: Character engine enabled, but selected character lacked a valid name. Skipping this character.", "WARNING")

                if character_profiles:
                    character_injection = "\n\nMULTI-CHARACTER CONVERSATION MODE:\n" + "\n".join(character_profiles)
                    character_injection += "\nMaintain all character personas throughout the conversation. Each character should have distinct voices and personalities.\n"
                    log_message(f"Thread {thread_id}: Adding {len(character_profiles)} character profiles to system prompt for task {task_id}", "DEBUG")
                else:
                    if enable_character_engine_local:
                        log_message(f"Thread {thread_id}: Character engine enabled but no valid characters selected. Skipping character injection.", "WARNING")

            current_system_prompt_for_task += character_injection

            # Handle emotional states
            current_emotional_state = ""
            if enable_emotional_states_local and emotional_states_list_local:
                current_emotional_state = random.choice(emotional_states_list_local)
                emotional_state_injection = (
                    f"\n\nEMOTIONAL STATE: {current_emotional_state.upper()}\n"
                    f"Express this emotional state throughout your responses. "
                    f"Use appropriate tone, word choice, and emotional expression that reflects {current_emotional_state} feelings.\n"
                )
                current_system_prompt_for_task += emotional_state_injection
                log_message(f"Thread {thread_id}: Assigned emotional state '{current_emotional_state}' for task {task_id}", "DEBUG")

            conversation_history_for_output = [] # Stores the full conversation for writing (mainly for non-duplication mode)
            current_llm_conversation_context = [] # Stores the conversation history passed to the LLM for context
            refusal_detected_in_task = False

            initial_user_question = None
            raw_subject_content_for_debug = "" # For debug logging
            raw_context_content_for_debug = "" # For debug logging

            # --- API Selection Logic ---
            api_config_for_this_task = None # Holds the config for the API used by this task/thread in non-duplication
            api_slot_idx_for_this_task = -1 # Original slot index of the API used in non-duplication

            if not master_duplication_enabled_local:
                # Non-duplication mode: assign an API to this thread for this task based on thread_id
                if active_enabled_api_configs_for_worker: # List of {config, original_slot_idx}
                    selected_api_details = active_enabled_api_configs_for_worker[thread_id % len(active_enabled_api_configs_for_worker)]
                    api_config_for_this_task = selected_api_details['config']
                    api_slot_idx_for_this_task = selected_api_details['original_slot_idx']
                    log_message(f"Thread {thread_id} (Non-Duplication): Assigned API Slot {api_slot_idx_for_this_task+1} for task {task_id}", "DEBUG")
                else: # Should not happen if start_processing validates correctly
                    log_message(f"Thread {thread_id}: CRITICAL - No active/enabled APIs for non-duplication mode. Skipping task.", "ERROR")
                    q.task_done(); continue
            # In Duplication mode, api_config_for_this_task and api_slot_idx_for_this_task are not used directly for answers.
            # Instead, the answer generation loop iterates through all_api_configs_local.
            # Question/Continuation generation in duplication mode uses the primary API (slot 0).

            # --- Initial Question Generation ---
            if use_questions_file_local:
                question_as_segment = task[3] # The question text is part of the task tuple
                initial_user_question = question_as_segment 
            else: # Generate question from subject/context
                if app_state.stop_processing: q.task_done(); continue
                subject_content_for_task = task[3]
                context_content_for_task = task[4]
                raw_subject_content_for_debug = subject_content_for_task 
                raw_context_content_for_debug = context_content_for_task 
                
                # Determine API for question generation: primary (slot 0) in duplication, or assigned API in non-duplication
                q_gen_api_conf = all_api_configs_local[0] if master_duplication_enabled_local else api_config_for_this_task
                q_gen_api_slot_idx = 0 if master_duplication_enabled_local else api_slot_idx_for_this_task

                initial_user_question = generate_question(
                    current_system_prompt_for_task, current_question_prompt, 
                    subject_content_for_task, context_content_for_task,
                    thread_id, q_gen_api_conf.get('sampler_settings', {}), 
                    q_gen_api_conf.get('url'), q_gen_api_conf.get('model'), q_gen_api_conf.get('key'),
                    current_history_size_local,
                    raw_subject_content_for_debug, raw_context_content_for_debug, 
                    api_slot_idx=q_gen_api_slot_idx, # Pass the correct API slot index for stats/logging
                    current_max_attempts_param=current_max_attempts, # Pass max attempts for retries
                    api_request_timeout_param=current_api_request_timeout
                )
            
            if not initial_user_question:
                log_message(f"Thread {thread_id}: Failed to generate initial question for task {task_id}. Skipping.", "ERROR")
                q.task_done(); continue

            current_llm_conversation_context.append({"role": "user", "content": initial_user_question})
            # For non-duplication mode, this will be part of the final output history
            if not master_duplication_enabled_local:
                conversation_history_for_output.append({"role": "user", "content": initial_user_question})


            # --- Multi-Turn Conversation Loop ---
            for turn_num in range(current_num_turns):
                if app_state.stop_processing or app_state.pause_processing: break # Check before starting a new turn

                assistant_answer = None # This will hold the assistant's response for the current turn

                # --- Assistant Answer Generation ---
                if master_duplication_enabled_local:
                    primary_api_answer_for_conv_flow = None # Answer from primary/first successful API to drive conversation flow
                    all_duplicated_answers_for_output = [] # Stores (answer_text, original_api_slot_idx) for writing

                    # Iterate through enabled APIs (Slots 1-4, indices 0-3) for duplication
                    for dup_api_idx, dup_api_conf_item in enumerate(all_api_configs_local):
                        if dup_api_idx < 4 and dup_api_conf_item.get('enabled', False): 
                            if app_state.stop_processing or app_state.pause_processing: break
                            log_message(f"Thread {thread_id}, Task {task_id}, Turn {turn_num+1}: Duplicating with API Slot {dup_api_idx+1}", "DEBUG")
                            start_time_api_task = time.time()
                            
                            answer_result = generate_answer_with_retries(
                                base_system_prompt=current_system_prompt_for_task,
                                conversation_history_for_llm=list(current_llm_conversation_context), # Pass a copy
                                answer_prompt_template=current_answer_prompt,
                                thread_id=thread_id, q=q,
                                sampler_settings_local=dup_api_conf_item.get('sampler_settings', {}),
                                api_url_local=dup_api_conf_item.get('url'),
                                model_name_local=dup_api_conf_item.get('model'),
                                api_key_local=dup_api_conf_item.get('key'),
                                refusal_phrases_local=current_refusal_phrases,
                                user_speaking_phrases_local=current_user_speaking_phrases,
                                slop_phrases_local=current_slop_phrases,
                                jailbreaks_local=current_jailbreaks,
                                speaking_fixes_local=current_speaking_fixes,
                                slop_fixes_fallback_local=current_slop_fixes_fallback,
                                max_attempts_local=current_max_attempts, # This is for main answer generation logic
                                slop_fixer_api_config_param=slop_fixer_api_config,
                                current_slop_fixes_for_rotation_param=current_slop_fixes_for_rotation_worker,
                                api_slot_idx=dup_api_idx, # Pass the specific API slot index being used
                                current_max_attempts_for_slop_fixer_call=current_max_attempts, # Pass for slop_fixer's own API call retries
                                master_duplication_enabled_local=master_duplication_enabled_local,
                                current_anti_slop_phrases_param=current_anti_slop_phrases, # (Prevents future error)
                                anti_slop_fixer_api_config_param=anti_slop_fixer_api_config_param,
                                api_request_timeout_param=current_api_request_timeout,
                            )
                            if answer_result and answer_result[0]:  # Check if answer is not None
                                duplicated_answer_text = answer_result[0]
                                issue_in_this_call = answer_result[1] if len(answer_result) > 1 else False
                                refusal_in_this_call = answer_result[2] if len(answer_result) > 2 else False

                                # Track if issue was detected in this task
                                if issue_in_this_call:
                                    any_issue_detected_in_task = True
                                if refusal_in_this_call:
                                    refusal_detected_in_task = True
                                    log_message(f"Thread {thread_id}: Task {task_id}, Turn {turn_num+1}: Issue detected (refusal/user_speak/slop). Setting refusal_detected_in_task=True", "WARNING")
                            end_time_api_task = time.time()
                            api_task_duration = end_time_api_task - start_time_api_task

                            if answer_result and answer_result[0]:  # Check if answer is not None
                                duplicated_answer_text = answer_result[0]
                                refusal_in_this_call = answer_result[1] if len(answer_result) > 1 else False

                                # Track if refusal was detected in this task
                                if refusal_in_this_call:
                                    refusal_detected_in_task = True
                                all_duplicated_answers_for_output.append((duplicated_answer_text, dup_api_idx))
                                # Update progress for this specific API in duplication mode
                                with q.processed_tasks_lock:
                                    if dup_api_idx not in q.api_processed_tasks: q.api_processed_tasks[dup_api_idx] = 0
                                    # Increment per successful turn generation
                                    # Ensure not to exceed total_tasks_for_progress for this API
                                    if q.api_processed_tasks[dup_api_idx] < q.total_tasks_for_progress:
                                        q.api_processed_tasks[dup_api_idx] += 1 
                                    if dup_api_idx not in q.api_start_times_list: q.api_start_times_list[dup_api_idx] = []
                                    q.api_start_times_list[dup_api_idx].append(api_task_duration)
                                    if len(q.api_start_times_list[dup_api_idx]) > 50: q.api_start_times_list[dup_api_idx].pop(0)
                                
                                # Determine which answer drives the conversation flow (primary or first successful)
                                if dup_api_idx == 0 and duplicated_answer_text: # Primary API (Slot 1) successful
                                    primary_api_answer_for_conv_flow = duplicated_answer_text
                                elif not primary_api_answer_for_conv_flow and duplicated_answer_text: # Fallback to first other successful API
                                    primary_api_answer_for_conv_flow = duplicated_answer_text
                            else:
                                log_message(f"Thread {thread_id}, Task {task_id}, Turn {turn_num+1}: API Slot {dup_api_idx+1} failed to generate answer.", "WARNING")
                        if app_state.stop_processing or app_state.pause_processing: break # Check inside duplication loop
                    
                    assistant_answer = primary_api_answer_for_conv_flow # Use this for the main conversation flow

                    # Write all successful duplicated answers to their respective per-API files for this turn
                    if assistant_answer: # Only proceed if at least one API gave an answer to continue the flow
                        for ans_text, original_slot_idx_for_file in all_duplicated_answers_for_output:
                            # CRITICAL FIX: Check if this specific answer had a refusal
                            # We need to track refusals per API call, not just overall
                            # For now, skip writing if any refusal was detected in this task
                            if not refusal_detected_in_task:
                                # Create a temporary history for this specific API's output for this turn
                                temp_conv_history_for_api_output_turn = list(current_llm_conversation_context) # Contains user's current message
                                temp_conv_history_for_api_output_turn.append({"role": "assistant", "content": ans_text})
                                with output_data_lock:
                                    write_conversation(None, temp_conv_history_for_api_output_turn, current_remove_reasoning,
                                                    current_remove_em_dash,
                                                    current_remove_asterisks, # NEW
                                                    current_remove_asterisk_space_asterisk,  # NEW ADDITION
                                                    current_remove_all_asterisks,  # NEW ADDITION
                                                    current_ensure_space_after_line_break, # NEW
                                                    current_remove_markdown,
                                                    current_output_format, task_id,
                                                    api_slot_idx_for_output_file=original_slot_idx_for_file, # Write to specific API's file
                                                    is_duplication_turn=True, turn_number_for_duplication=turn_num + 1) # Mark as duplication turn
                            else:
                                log_message(f"Thread {thread_id}: Skipping turn {turn_num+1} output for API Slot {original_slot_idx_for_file+1} due to previous refusal in task {task_id}", "DEBUG")
                    # Note: completed_task_ids.add() and save_generation_state() are handled once per task_id at the end of the worker.
                
                else: # --- Non-Duplication Mode: Single API call for answer ---
                    if app_state.stop_processing or app_state.pause_processing: break
                    start_time_api_task = time.time()
                    answer_result = generate_answer_with_retries(
                        base_system_prompt=current_system_prompt_for_task,
                        conversation_history_for_llm=list(current_llm_conversation_context),
                        answer_prompt_template=current_answer_prompt,
                        thread_id=thread_id, q=q,
                        sampler_settings_local=api_config_for_this_task.get('sampler_settings', {}),
                        api_url_local=api_config_for_this_task.get('url'),
                        model_name_local=api_config_for_this_task.get('model'),
                        api_key_local=api_config_for_this_task.get('key'),
                        refusal_phrases_local=current_refusal_phrases,
                        user_speaking_phrases_local=current_user_speaking_phrases,
                        slop_phrases_local=current_slop_phrases,
                        current_anti_slop_phrases_param=current_anti_slop_phrases,
                        jailbreaks_local=current_jailbreaks,
                        speaking_fixes_local=current_speaking_fixes,
                        slop_fixes_fallback_local=current_slop_fixes_fallback,
                        max_attempts_local=current_max_attempts,
                        slop_fixer_api_config_param=slop_fixer_api_config,
                        current_slop_fixes_for_rotation_param=current_slop_fixes_for_rotation_worker,
                        api_slot_idx=api_slot_idx_for_this_task, # API slot used by this worker
                        current_max_attempts_for_slop_fixer_call=current_max_attempts, # Pass for slop_fixer's own API call retries
                        no_user_impersonation_local=no_user_impersonation_local,
                        master_duplication_enabled_local=master_duplication_enabled_local,
                        anti_slop_fixer_api_config_param=anti_slop_fixer_api_config_param,
                        api_request_timeout_param=current_api_request_timeout,
                    )
                    if answer_result and answer_result[0]:  # Check if answer is not None
                        assistant_answer = answer_result[0]
                        issue_in_this_call = answer_result[1] if len(answer_result) > 1 else False
                        refusal_in_this_call = answer_result[2] if len(answer_result) > 2 else False

                        # Track if issue was detected in this task (refusal, user speaking, slop, OR anti-slop)
                        if issue_in_this_call:
                            any_issue_detected_in_task = True
                        if refusal_in_this_call:
                            refusal_detected_in_task = True
                            log_message(f"Thread {thread_id}: Task {task_id}, Turn {turn_num+1}: Issue detected (refusal/user_speak/slop). Setting refusal_detected_in_task=True", "WARNING")
                    end_time_api_task = time.time()
                    api_task_duration = end_time_api_task - start_time_api_task

                    if answer_result and answer_result[0]:  # Check if answer is not None
                        assistant_answer = answer_result[0]
                        refusal_in_this_call = answer_result[1] if len(answer_result) > 1 else False

                        # Track if refusal was detected in this task
                        if refusal_in_this_call:
                            refusal_detected_in_task = True
                        # Update overall progress for non-duplication mode (per turn)
                        with q.processed_tasks_lock:
                            if not hasattr(q, 'processed_tasks'): q.processed_tasks = 0
                            if q.processed_tasks < q.total_tasks_for_progress: # Check against total expected turns
                                q.processed_tasks += 1 
                            if not hasattr(q, 'start_times_list'): q.start_times_list = []
                            q.start_times_list.append(api_task_duration)
                            if len(q.start_times_list) > 50: q.start_times_list.pop(0)
                        
                        # In non-duplication, add the assistant's answer to the main conversation history for output
                        conversation_history_for_output.append({"role": "assistant", "content": assistant_answer})

                # --- End of Assistant Answer Generation for the turn ---

                if not assistant_answer: # If no answer (primary failed in dup, or single API failed in non-dup)
                    log_message(f"Thread {thread_id}: Failed to get any assistant answer for turn {turn_num + 1} of task {task_id}. Ending this conversation.", "ERROR")
                    break # End this task's conversation

                current_llm_conversation_context.append({"role": "assistant", "content": assistant_answer})
                # conversation_history_for_output is handled above based on duplication mode for this turn's assistant answer.

                if turn_num == current_num_turns - 1: # If this was the last turn
                    break 
                if app_state.stop_processing or app_state.pause_processing: break

                # --- User Continuation Generation (if not the last turn) ---
                if not current_user_continuation_prompt: 
                    log_message(f"Thread {thread_id}: No user continuation prompt set. Ending conversation after assistant's turn {turn_num + 1}.", "INFO")
                    break
                
                # Determine API for user continuation: primary (slot 0) in duplication, or assigned API in non-duplication
                cont_gen_api_conf = all_api_configs_local[0] if master_duplication_enabled_local else api_config_for_this_task
                cont_gen_api_slot_idx = 0 if master_duplication_enabled_local else api_slot_idx_for_this_task

                user_continuation_reply = generate_user_continuation(
                    system_prompt=current_system_prompt_for_task, 
                    conversation_history_for_llm=list(current_llm_conversation_context), 
                    user_continuation_prompt_template=current_user_continuation_prompt,
                    thread_id=thread_id, 
                    sampler_settings_local=cont_gen_api_conf.get('sampler_settings', {}),
                    api_url_local=cont_gen_api_conf.get('url'), 
                    model_name_local=cont_gen_api_conf.get('model'), 
                    api_key_local=cont_gen_api_conf.get('key'),
                    api_slot_idx=cont_gen_api_slot_idx, # Pass correct API slot for stats/logging
                    current_max_attempts_param=current_max_attempts, # Pass max attempts for retries
                    api_request_timeout_param=current_api_request_timeout
                )

                if not user_continuation_reply:
                    log_message(f"Thread {thread_id}: Failed to get user continuation for turn {turn_num + 1} of task {task_id}. Ending this conversation.", "ERROR")
                    break 
                
                current_llm_conversation_context.append({"role": "user", "content": user_continuation_reply})
                # In non-duplication, add user's continuation to the main history for final output
                if not master_duplication_enabled_local:
                    conversation_history_for_output.append({"role": "user", "content": user_continuation_reply})
            
            # --- End of Multi-Turn Loop ---
            if app_state.stop_processing: q.task_done(); continue 

            # --- Write Completed Conversation (Non-Duplication Mode) or Mark Task Complete (Duplication Mode) ---
            if not master_duplication_enabled_local:
                # In non-duplication mode, conversation_history_for_output contains the full conversation.
                if conversation_history_for_output and len(conversation_history_for_output) >= 2: # Ensure at least one Q/A pair
                    # CRITICAL FIX: Don't save if a refusal was detected at any point
                    if refusal_detected_in_task:
                        log_message(f"Thread {thread_id}: Task {task_id} contained a refusal. NOT saving to output.jsonl.", "WARNING")
                        # Do NOT add to completed_task_ids - this task should not be marked complete
                    elif len(conversation_history_for_output) < (current_num_turns * 2):
                        log_message(f"Thread {thread_id}: Task {task_id} incomplete. Expected {current_num_turns * 2} messages, got {len(conversation_history_for_output)}. NOT saving to output.jsonl.", "WARNING")
                        # Do NOT add to completed_task_ids - this task should be retried.
                        # If the host is down, requeue it so it's retried once the host recovers
                        # (otherwise it would just be discarded and lost).
                        if api_host_is_down(worker_api_slot):
                            if requeue_task(q, task, task_id):
                                log_message(f"Thread {thread_id}: API Slot {worker_api_slot+1} down; requeued incomplete task {task_id} for retry.", "WARNING")
                            else:
                                log_message(f"Thread {thread_id}: Task {task_id} exceeded {MAX_TASK_REQUEUES} requeues; giving up.", "ERROR")
                    else:
                        with output_data_lock:
                            write_conversation(None, conversation_history_for_output, current_remove_reasoning,
                                current_remove_em_dash,
                                current_remove_asterisks,
                                current_remove_asterisk_space_asterisk,
                                current_remove_all_asterisks,
                                current_ensure_space_after_line_break,
                                current_remove_markdown,
                                current_output_format, task_id,
                                api_slot_idx_for_output_file=None)
                        app_state.completed_task_ids.add(task_id)

                    save_generation_state()
                    log_message(f"Thread {thread_id}: Processed task {task_id} (API Slot {api_slot_idx_for_this_task+1}) from file {file_name}. Turns: {len(conversation_history_for_output)//2}", "INFO")
                else:
                    log_message(f"Thread {thread_id}: No valid conversation generated for task {task_id} (API Slot {api_slot_idx_for_this_task+1}). Not writing to output.", "WARNING")
                    # If this failed because the host is down (circuit open), requeue it so
                    # it's retried after the host recovers instead of being silently dropped.
                    if api_host_is_down(worker_api_slot):
                        if requeue_task(q, task, task_id):
                            log_message(f"Thread {thread_id}: API Slot {worker_api_slot+1} down; requeued task {task_id} for retry.", "WARNING")
                        else:
                            log_message(f"Thread {thread_id}: Task {task_id} exceeded {MAX_TASK_REQUEUES} requeues; giving up.", "ERROR")
            else: # Master duplication mode - mark task complete if it went through turns
                # Individual API outputs were already handled per turn inside the loop.
                if conversation_history_for_output or current_llm_conversation_context: # Check if at least initial question was made
                    # CRITICAL FIX: Don't save if a refusal was detected at any point
                    if refusal_detected_in_task:
                        log_message(f"Thread {thread_id}: Task {task_id} contained a refusal (duplication mode). NOT saving to output.jsonl. refusal_detected_in_task={refusal_detected_in_task}", "WARNING")
                        log_message(f"Thread {thread_id}: Skipping task {task_id} (duplication mode) - refusal detected during generation", "INFO")
                        # Do NOT add to completed_task_ids
                    else:
                        with output_data_lock: # Lock for completed_task_ids and save_generation_state
                            app_state.completed_task_ids.add(task_id)
                            save_generation_state()
                        log_message(f"Thread {thread_id}: Completed processing (duplication mode) for task {task_id} from file {file_name}. Individual API outputs handled per turn.", "INFO")

        except Exception as e: # Catch-all for errors during task processing
            error_message_gen = f"Thread {thread_id}: Error processing task {task_id} from {file_name}: {str(e)}"
            log_message(error_message_gen, "ERROR")
            import traceback
            log_message(traceback.format_exc(), "ERROR") 
            # Record this as a general error for the task
            with output_data_lock: # Use the lock for modifying global error counters
                app_state.error_count_total +=1 
                # For general task errors, we don't assign to a specific API unless the error originated there.
                # Here, it's a task-level error, so log it for the "Totals" dashboard.
                err_summary = f"T{thread_id} TaskErr: {str(e)[:30]}" # Short summary
                if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                app_state.recent_errors_total.append((err_summary, -1)) # -1 indicates a general task error not tied to a specific API call error
        
        end_time_task_overall = time.time()
        task_duration_overall = end_time_task_overall - start_time_task_overall

        # In duplication mode, we track overall task time for a general estimate,
        # though per-API times are more granular for progress bars.
        if master_duplication_enabled_local:
             with q.processed_tasks_lock: # This lock is also used for overall_task_times_list
                if not hasattr(q, 'overall_task_times_list'): q.overall_task_times_list = []
                q.overall_task_times_list.append(task_duration_overall)
                if len(q.overall_task_times_list) > 50 : q.overall_task_times_list.pop(0)
                # This doesn't directly update a progress bar but could be used for overall ETA if needed.

        q.task_done() # Signal that this task is complete

    log_message(f"Thread {thread_id} completed its run.", "INFO")

def get_next_system_prompt(prompts_list_local):
    """Randomly selects a system prompt from the list if variable system prompts are enabled."""
    if not prompts_list_local:
        return "You are a helpful assistant."  # Fallback
    return random.choice(prompts_list_local)

def generate_question(system_prompt, question_prompt_template, subject, context, thread_id,
                      sampler_settings_local, api_url_local, model_name_local, api_key_local,
                      history_size_local_param,
                      raw_subject_chunk, raw_context_chunk,
                      api_slot_idx, current_max_attempts_param, api_request_timeout_param):
    """Generates an initial question using the LLM, with retries for API call failures."""

    if not api_url_local:
        log_message(f"Thread {thread_id}: API URL missing for question generation (API Slot {api_slot_idx+1}). Cannot proceed.", "ERROR")
        return None

    for attempt_num in range(current_max_attempts_param):
        if app_state.stop_processing or app_state.pause_processing:
            return None
            MAX_TOTAL_RETRY_WAIT = 20
            current_attempt_wait = 0

        # FIX: Use stats_lock instead of system_prompt_lock, with timeout
        lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
        if lock_acquired:
            try:
                app_state.total_attempts_global += 1
                app_state.total_attempts_per_api[api_slot_idx] += 1
            finally:
                app_state.system_prompt_lock.release()
        else:
            log_message(f"Thread {thread_id}: Skipped stat update (system_prompt_lock busy)", "DEBUG")

        try:
            # FIX: Use question_history_lock with timeout
            recent_questions_str = ""
            lock_acquired_qh = app_state.question_history_lock.acquire(timeout=0.05)
            if lock_acquired_qh:
                try:
                    recent_questions_str = "\n- ".join(app_state.question_history[-history_size_local_param:]) if app_state.question_history else "None"
                finally:
                    app_state.question_history_lock.release()
            else:
                log_message(f"Thread {thread_id}: WARNING - Could not acquire question_history_lock. Using empty history.", "WARNING")
                recent_questions_str = "None"

            # Format the question prompt with placeholders
            final_formatted_user_prompt = question_prompt_template.replace("{recent_questions}", recent_questions_str)
            final_formatted_user_prompt = final_formatted_user_prompt.replace("{subject}", subject if subject else "N/A")
            final_formatted_user_prompt = final_formatted_user_prompt.replace("{context}", context if context else "N/A")

            messages_for_llm = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": final_formatted_user_prompt}
            ]

            update_live_prompt_preview(messages_for_llm)

            # Prepare payload for LLM API
            payload_dict = {
                "model": model_name_local,
                "messages": messages_for_llm,
                **sampler_settings_local.get("generation_params", {
                    "temperature": sampler_settings_local.get("temperature",0.7),
                    "top_p": sampler_settings_local.get("top_p",0.9),
                    "top_k": sampler_settings_local.get("top_k",50),
                    "repetition_penalty": sampler_settings_local.get("repetition_penalty",1.1),
                    "max_tokens": sampler_settings_local.get("max_tokens_question", global_config.get('samplers.max_tokens_question', 256))
                }),
                "stream": False
            }
            logit_bias_str = sampler_settings_local.get('logit_bias', '')
            if logit_bias_str:
                try:
                    payload_dict['logit_bias'] = json.loads(logit_bias_str)
                except json.JSONDecodeError:
                    log_message(f"Thread {thread_id}: Invalid logit_bias JSON. Skipping.", "WARNING")
            thinking_mode = sampler_settings_local.get('enable_thinking', 'default')
            if thinking_mode == 'enable':
                payload_dict['chat_template_kwargs'] = {"enable_thinking": True}
            elif thinking_mode == 'disable':
                payload_dict['chat_template_kwargs'] = {"enable_thinking": False}
            # else 'default': do not send the parameter

            payload = json.dumps(payload_dict)
            headers = {
                'Content-Type': 'application/json'
            }
            if api_key_local:
                headers['Authorization'] = f"Bearer {api_key_local}"

            current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{api_slot_idx}.jsonl" if app_state.master_duplication_enabled_var.get() else BASE_DEBUG_LOG_PATH + ".jsonl"

            debug_log_entry = {
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "thread_id": thread_id, "type": "question_request", "api_slot_idx": api_slot_idx, "attempt": attempt_num + 1,
                "api_url": api_url_local, "model": model_name_local,
                "raw_subject_chunk_length": len(raw_subject_chunk),
                "raw_context_chunk_length": len(raw_context_chunk),
                "messages": messages_for_llm,
                "sampler_settings": sampler_settings_local,
                "payload_dict": payload_dict
            }
            with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
                debug_log.write(json.dumps(debug_log_entry) + '\n')

            # Apply rate limiting before making the API call
            global_rate_limiter.wait_if_needed(api_slot_idx)

            # Circuit Breaker Check
            if not check_circuit_breaker(api_slot_idx):
                log_message(f"Thread {thread_id}: API Slot {api_slot_idx+1} circuit open. Skipping question generation.", "DEBUG")
                return None

            # Track API response time
            api_call_start_time = time.time()

            # Create a unique hash of the prompt to use as a cache key
            prompt_content = json.dumps(messages_for_llm, sort_keys=True)
            prompt_hash = hashlib.md5(prompt_content.encode()).hexdigest()

            # Check cache
            cached_response, is_cached = get_cached_response(prompt_hash, api_slot_idx)
            if is_cached:
                generated_question_text = cached_response
                # Update question history if cached
                lock_acquired_qh_update = app_state.question_history_lock.acquire(timeout=7.0)
                if lock_acquired_qh_update:
                    try:
                        update_question_history(generated_question_text, history_size_local_param)
                    finally:
                        app_state.question_history_lock.release()
                return generated_question_text

            response = requests.post(api_url_local, headers=headers, data=payload, timeout=(api_request_timeout_param, api_request_timeout_param))

            api_response_time = time.time() - api_call_start_time

            # Store response time (thread-safe)
            with api_response_times_lock:
                api_response_times_per_slot[api_slot_idx].append(api_response_time)
                if len(api_response_times_per_slot[api_slot_idx]) > MAX_RESPONSE_TIMES_TO_TRACK:
                    api_response_times_per_slot[api_slot_idx] = api_response_times_per_slot[api_slot_idx][-MAX_RESPONSE_TIMES_TO_TRACK:]

            if response.status_code == 200:
                record_api_success(api_slot_idx)
                response_data = response.json()
                content = response_data['choices'][0]['message'].get('content')

                # NEW: Extract token usage if available
                usage = response_data.get('usage', {})
                input_tokens = usage.get('prompt_tokens', 0)
                output_tokens = usage.get('completion_tokens', 0)

                # Update global counters with timeout
                lock_acquired_tokens = app_state.stats_lock.acquire(timeout=7.0)
                if lock_acquired_tokens:
                    try:
                        app_state.total_input_tokens += input_tokens
                        app_state.total_output_tokens += output_tokens
                    finally:
                        app_state.stats_lock.release()
                else:
                    log_message(f"Thread {thread_id}: WARNING - Could not acquire stats_lock for token update.", "WARNING")

                if content is None:
                    log_message(f"Thread {thread_id}: API returned None for content (API Slot {api_slot_idx+1}, Attempt {attempt_num+1})", "WARNING")
                    if attempt_num < current_max_attempts_param - 1:
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None, text_context
                generated_question_text = content.strip()
                newline_count = generated_question_text.count('\n')
                text_length = len(generated_question_text)

                max_newlines = global_config.get('generation.max_newlines_malformed', 16)
                max_text_length = global_config.get('generation.max_text_length_malformed', 5000)

                if newline_count > max_newlines or text_length > max_text_length:
                    log_message(
                        f"Thread {thread_id}: Question response appears malformed. "
                        f"Newlines: {newline_count} (Max: {max_newlines}), Length: {text_length} (Max: {max_text_length}). "
                        f"Snippet: '{generated_question_text[:100]}...'",
                        "WARNING"
                    )
                    if attempt_num < current_max_attempts_param - 1:
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None
                generated_question_text = content.strip()
                if not generated_question_text or len(generated_question_text) < 5:
                    log_message(f"Thread {thread_id}: API returned empty/very short question. Content: '{generated_question_text}'", "WARNING")
                    if attempt_num < current_max_attempts_param - 1:
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None

                # Update question history with timeout
                lock_acquired_qh_update = app_state.question_history_lock.acquire(timeout=7.0)
                if lock_acquired_qh_update:
                    try:
                        update_question_history(generated_question_text, history_size_local_param)
                        set_cached_response(prompt_hash, api_slot_idx, generated_question_text)
                        log_message(f"Thread {thread_id}: Cache SET for API Slot {api_slot_idx+1}.", "DEBUG")
                    finally:
                        app_state.question_history_lock.release()
                return generated_question_text
            else: # API call failed
                error_message = f"Thread {thread_id}: Error generating question (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param}, Status: {response.status_code}): {response.text[:200]}"
                log_message(error_message, "ERROR")
                record_api_failure(api_slot_idx)
                # Update error counters with timeout
                lock_acquired_err = app_state.stats_lock.acquire(timeout=7.0)
                if lock_acquired_err:
                    try:
                        app_state.error_count_total += 1
                        app_state.error_counts_per_api[api_slot_idx] += 1
                        err_summary = f"T{thread_id} Q-Err (API{api_slot_idx+1}): S{response.status_code} A{attempt_num+1}"
                        if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                        app_state.recent_errors_total.append((err_summary, api_slot_idx))
                        with app_state.issue_timestamps_lock:
                            app_state.issue_timestamps['errors'].append(time.time())
                            cutoff = time.time() - 3600
                            app_state.issue_timestamps['errors'] = [t for t in app_state.issue_timestamps['errors'] if t > cutoff]
                        if api_slot_idx < 6 :
                            if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                            app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                    finally:
                        app_state.stats_lock.release()
                if attempt_num < current_max_attempts_param - 1:
                    time.sleep(random.uniform(0.5, 1.5))
                    continue
                else:
                    return None
        except requests.exceptions.Timeout:
            error_message = f"Thread {thread_id}: Timeout generating question (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param})."
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx)
            lock_acquired_err = app_state.stats_lock.acquire(timeout=7.0)
            if lock_acquired_err:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx] += 1
                    err_summary = f"T{thread_id} Q-Timeout (API{api_slot_idx+1}) A{attempt_num+1}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx))
                    if api_slot_idx < 4:
                        if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                        app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                finally:
                    app_state.stats_lock.release()
            if attempt_num < current_max_attempts_param - 1:
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None
        except Exception as e:
            error_message = f"Thread {thread_id}: Exception in generate_question (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param}): {str(e)}"
            log_message(error_message, "ERROR")
            import traceback
            log_message(traceback.format_exc(), "ERROR")
            record_api_failure(api_slot_idx)
            lock_acquired_err = app_state.stats_lock.acquire(timeout=7.0)
            if lock_acquired_err:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx] += 1
                    err_summary = f"T{thread_id} Q-Exc (API{api_slot_idx+1}) A{attempt_num+1}: {str(e)[:20]}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx))
                    if api_slot_idx < 4:
                        if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                        app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                finally:
                    app_state.stats_lock.release()
            if attempt_num < current_max_attempts_param - 1:
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None
    return None

def generate_user_continuation(system_prompt, conversation_history_for_llm, user_continuation_prompt_template,
                               thread_id, sampler_settings_local, api_url_local, model_name_local, api_key_local,
                               api_slot_idx, current_max_attempts_param, api_request_timeout_param): # API slot index and max_attempts
    """Generates the user's continuation reply, with retries for API call failures."""


    if not api_url_local:
        log_message(f"Thread {thread_id}: API URL missing for user continuation (API Slot {api_slot_idx+1}). Cannot proceed.", "ERROR")
        return None

    for attempt_num in range(current_max_attempts_param):
        if app_state.stop_processing or app_state.pause_processing: return None
        MAX_TOTAL_RETRY_WAIT = 20
        current_attempt_wait = 0

        lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
        if lock_acquired:
            try:
                app_state.total_attempts_global += 1
                app_state.total_attempts_per_api[api_slot_idx] += 1
            finally:
                app_state.system_prompt_lock.release()

        try:
            # Get the last assistant message for the prompt template
            last_assistant_message = ""
            if conversation_history_for_llm and conversation_history_for_llm[-1]["role"] == "assistant":
                last_assistant_message = conversation_history_for_llm[-1]["content"]
            
            final_user_continuation_prompt = user_continuation_prompt_template.replace("{last_assistant_message}", last_assistant_message)

            messages = [{"role": "system", "content": system_prompt}] + \
                       conversation_history_for_llm + \
                       [{"role": "user", "content": final_user_continuation_prompt}]

            update_live_prompt_preview(messages)

            payload_dict = {
                "model": model_name_local,
                "messages": messages,
                **sampler_settings_local.get("generation_params", { 
                    "temperature": sampler_settings_local.get("temperature", 0.6), 
                    "top_p": sampler_settings_local.get("top_p", 0.9),
                    "top_k": sampler_settings_local.get("top_k", 50),
                    "repetition_penalty": sampler_settings_local.get("repetition_penalty", 1.1),
                    "max_tokens": sampler_settings_local.get("max_tokens_user_reply", global_config.get('samplers.max_tokens_user_reply', 256))
                }),
                "stream": False
            }
            logit_bias_str = sampler_settings_local.get('logit_bias', '')
            if logit_bias_str:
                try:
                    payload_dict['logit_bias'] = json.loads(logit_bias_str)
                except json.JSONDecodeError:
                    log_message(f"Thread {thread_id}: Invalid logit_bias JSON. Skipping.", "WARNING")
            thinking_mode = sampler_settings_local.get('enable_thinking', 'default')
            if thinking_mode == 'enable':
                payload_dict['chat_template_kwargs'] = {"enable_thinking": True}
            elif thinking_mode == 'disable':
                payload_dict['chat_template_kwargs'] = {"enable_thinking": False}
            # else 'default': do not send the parameter

            payload = json.dumps(payload_dict)
            headers = {
                'Content-Type': 'application/json'
            }

            if api_key_local:
                headers['Authorization'] = f"Bearer {api_key_local}"
            
            current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{api_slot_idx}.jsonl" if app_state.master_duplication_enabled_var.get() else BASE_DEBUG_LOG_PATH + ".jsonl"
            with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
                debug_log.write(json.dumps({"timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "thread_id": thread_id, "type": "user_continuation_request", "api_slot_idx": api_slot_idx, "attempt": attempt_num + 1, "api_url": api_url_local, "model": model_name_local, "messages": messages, "payload_dict": payload_dict}) + '\n')

            # NEW: Apply rate limiting before making the API call
            global_rate_limiter.wait_if_needed(api_slot_idx)

            # Circuit Breaker Check
            if not check_circuit_breaker(api_slot_idx):
                log_message(f"Thread {thread_id}: API Slot {api_slot_idx+1} circuit open. Skipping user continuation.", "DEBUG")
                return None

            # Track API response time
            api_call_start_time = time.time()
            response = requests.post(api_url_local, headers=headers, data=payload, timeout=(api_request_timeout_param, api_request_timeout_param))  # (connect_timeout, read_timeout)
            api_response_time = time.time() - api_call_start_time

            # Store response time (thread-safe)
            with api_response_times_lock:
                api_response_times_per_slot[api_slot_idx].append(api_response_time)
                if len(api_response_times_per_slot[api_slot_idx]) > MAX_RESPONSE_TIMES_TO_TRACK:
                    api_response_times_per_slot[api_slot_idx] = api_response_times_per_slot[api_slot_idx][-MAX_RESPONSE_TIMES_TO_TRACK:]

            if response.status_code == 200:
                # --- FIX START: Handle None content ---
                record_api_success(api_slot_idx)
                content = response.json()['choices'][0]['message'].get('content')
                if content is None:
                    log_message(f"Thread {thread_id}: API returned None for user continuation content (API Slot {api_slot_idx+1}, Attempt {attempt_num+1})", "WARNING")
                    sleep_dur = random.uniform(0.5, 1.5)
                    if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                        time.sleep(sleep_dur)
                        current_attempt_wait += sleep_dur
                        continue
                    else:
                        return None  # Cap reached, abort continuation
                user_reply_text = content.strip()
                if not user_reply_text or len(user_reply_text) < 5:
                    log_message(f"Thread {thread_id}: API returned empty/very short user reply. Content: '{user_reply_text}'", "WARNING")
                    if attempt_num < current_max_attempts_param - 1:
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None

                    newline_count = user_reply_text.count('\n')
                    text_length = len(user_reply_text)

                    # --- NEW SETTINGS START ---
                    max_newlines = global_config.get('generation.max_newlines_malformed', 16)
                    max_text_length = global_config.get('generation.max_text_length_malformed', 5000)
                    # --- NEW SETTINGS END ---

                    if newline_count > max_newlines or text_length > max_text_length:
                        log_message(
                            f"Thread {thread_id}: User reply response appears malformed. "
                            f"Newlines: {newline_count} (Max: {max_newlines}), Length: {text_length} (Max: {max_text_length}). "
                            f"Snippet: '{user_reply_text[:100]}...'",
                            "WARNING"
                        )
                        sleep_dur = random.uniform(0.5, 1.5)
                        if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                            time.sleep(sleep_dur)
                            current_attempt_wait += sleep_dur
                            continue
                        else:
                            return None  # Cap reached, abort continuation
                # --- FIX END ---
                return user_reply_text
            else: # API call failed
                record_api_failure(api_slot_idx)
                error_message = f"Thread {thread_id}: Error generating user continuation (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param}, Status: {response.status_code}): {response.text[:200]}"
                log_message(error_message, "ERROR")
                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired:
                    try:
                        app_state.error_count_total += 1
                        app_state.error_counts_per_api[api_slot_idx] += 1
                        err_summary = f"T{thread_id} Q-Err (API{api_slot_idx+1}): S{response.status_code} A{attempt_num+1}"
                        if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                        app_state.recent_errors_total.append((err_summary, api_slot_idx))
                        if api_slot_idx < 6:
                            if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                            app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                    finally:
                        app_state.system_prompt_lock.release()

                with app_state.issue_timestamps_lock:
                    app_state.issue_timestamps['errors'].append(time.time())
                    cutoff = time.time() - 3600
                    app_state.issue_timestamps['errors'] = [t for t in app_state.issue_timestamps['errors'] if t > cutoff]
                    if api_slot_idx < 4:
                        if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                        app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                sleep_dur = random.uniform(0.5, 1.5)
                if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                    time.sleep(sleep_dur)
                    current_attempt_wait += sleep_dur
                    continue
                else:
                    return None  # Cap reached, abort continuation
        except requests.exceptions.Timeout:
            error_message = f"Thread {thread_id}: Timeout generating user continuation (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param})."
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx)
            lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
            if lock_acquired:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx] += 1
                    err_summary = f"T{thread_id} UserCont-Timeout (API{api_slot_idx+1}) A{attempt_num+1}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx))
                    if api_slot_idx < 4:
                        if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                        app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                finally:
                    app_state.system_prompt_lock.release()
            sleep_dur = random.uniform(0.5, 1.5)
            if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                time.sleep(sleep_dur)
                current_attempt_wait += sleep_dur
                continue
            else:
                return None  # Cap reached, abort continuation
        except Exception as e: # Catch any other exceptions
            error_message = f"Thread {thread_id}: Exception in generate_user_continuation (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param}): {str(e)}"
            log_message(error_message, "ERROR")
            import traceback
            log_message(traceback.format_exc(), "ERROR")
            record_api_failure(api_slot_idx)
            lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
            if lock_acquired:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx] += 1
                    err_summary = f"T{thread_id} UserCont-Exc (API{api_slot_idx+1}) A{attempt_num+1}: {str(e)[:20]}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx))
                    if api_slot_idx < 4:
                        if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                        app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                finally:
                    app_state.system_prompt_lock.release()
            sleep_dur = random.uniform(0.5, 1.5)
            if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                time.sleep(sleep_dur)
                current_attempt_wait += sleep_dur
                continue
            else:
                return None  # Cap reached, abort continuation
    return None

def call_slop_fixer_llm(text_context, slop_phrase,
                        slop_fixer_api_config,
                        main_sampler_settings, thread_id, additional_fix_instructions="",
                        current_max_attempts_param=5, api_request_timeout_param=300):
    """Calls a dedicated LLM (API Slot 5, index 4) to rewrite a sentence containing "slop", with retries."""

    api_slot_idx_slop_fixer = 4 # Slop fixer is always API slot 5 (index 4)

    if not slop_fixer_api_config or not slop_fixer_api_config.get('url') or \
       not slop_fixer_api_config.get('model') or not slop_fixer_api_config.get('key'):
        log_message(f"Thread {thread_id}: Slop Fixer LLM (API Slot {api_slot_idx_slop_fixer+1}) not fully configured. Cannot call.", "WARNING")
        return None, text_context # Return None for rewritten, and original sentence

    api_url = slop_fixer_api_config['url']
    model_name = slop_fixer_api_config['model']
    api_key = slop_fixer_api_config['key']

    # Added validation for main_sampler_settings to prevent NameError-like issues from bad config
    if not main_sampler_settings or not isinstance(main_sampler_settings, dict):
        log_message(f"Thread {thread_id}: main_sampler_settings passed to call_slop_fixer_llm is invalid. Expected a dictionary.", "ERROR")
        return None, text_context

    for attempt_num in range(current_max_attempts_param):
        if app_state.stop_processing or app_state.pause_processing: return None, text_context

        with app_state.system_prompt_lock:
            app_state.total_attempts_global +=1
            app_state.total_attempts_per_api[api_slot_idx_slop_fixer] +=1

        try:
            # UPDATED PROMPT: Explicitly instruct to preserve quotes & provide paragraph context
            user_rewrite_instruction = (
                f"The following text contains an undesirable phrase: '{slop_phrase}'. "
                f"Rewrite the text to remove or rephrase this specific undesirable phrase while preserving the original meaning, tone, and ALL quotation marks. "
                f"CRITICAL: Do not drop any opening or closing quotation marks. Ensure quotes remain perfectly balanced. "
                f"Only output the rewritten text. Do not include any preamble or explanation. Just the rewritten text."
            )
            if additional_fix_instructions:
                user_rewrite_instruction += f"\n\nImportant instruction to follow: {additional_fix_instructions}"

            user_rewrite_instruction += (
                "\n\nCRITICAL RULES:\n"
                "1. Preserve ALL existing quotation marks exactly where they appear structurally.\n"
                "2. If you must add or remove a quote, ensure every opening mark has a matching closing mark.\n"
                "3. Do not wrap the entire output in quotes."
            )

            user_rewrite_instruction += f"\n\n<original_text>\n{text_context}\n</original_text>"

            messages = [
                {"role": "system", "content": "You are an expert editor. Rewrite the given text to remove the specified undesirable phrase, ensuring the core meaning is kept. Output only the rewritten text."},
                {"role": "user", "content": user_rewrite_instruction}
            ]

            # Use dedicated Slop Fixer sampler settings from the API config
            slop_fixer_sampler_overrides = slop_fixer_api_config.get('sampler_settings', {}) or main_sampler_settings

            # Renamed to match the function context
            final_slop_fixer_params = {
                "temperature": slop_fixer_sampler_overrides.get("temperature", 0.5),
                "top_p": slop_fixer_sampler_overrides.get("top_p", 0.95),
                "min_p": slop_fixer_sampler_overrides.get("min_p", 0.0),
                "top_k": slop_fixer_sampler_overrides.get("top_k", 50),
                "repetition_penalty": slop_fixer_sampler_overrides.get("repetition_penalty", 1.1),
                "max_tokens": slop_fixer_sampler_overrides.get("max_tokens", len(text_context.split()) * 3 + 70),
            }

            payload_data = {
                "model": model_name,
                "messages": messages,
                **final_slop_fixer_params,
                "stream": False
            }
            payload = json.dumps(payload_data)
            headers = {
                'Content-Type': 'application/json'
            }

            if api_key:
                headers['Authorization'] = f"Bearer {api_key}"

            current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{api_slot_idx_slop_fixer}.jsonl" if app_state.master_duplication_enabled_var.get() else BASE_DEBUG_LOG_PATH + ".jsonl"

            with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
                debug_log.write(json.dumps({"timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "thread_id": thread_id, "type": "slop_fix_request", "api_slot_idx": api_slot_idx_slop_fixer, "attempt": attempt_num + 1, "api_url": api_url, "model": model_name, "messages": messages, "payload_data": payload_data }) + '\n')

            # NEW: Apply rate limiting before making the API call
            global_rate_limiter.wait_if_needed(api_slot_idx_slop_fixer)

            # Track API response time
            api_call_start_time = time.time()
            response = requests.post(api_url, headers=headers, data=payload, timeout=(api_request_timeout_param, api_request_timeout_param))  # (connect_timeout, read_timeout)
            api_response_time = time.time() - api_call_start_time

            # Store response time (thread-safe)
            with api_response_times_lock:
                api_response_times_per_slot[api_slot_idx_slop_fixer].append(api_response_time)
            if len(api_response_times_per_slot[api_slot_idx_slop_fixer]) > MAX_RESPONSE_TIMES_TO_TRACK:
                api_response_times_per_slot[api_slot_idx_slop_fixer] = api_response_times_per_slot[api_slot_idx_slop_fixer][-MAX_RESPONSE_TIMES_TO_TRACK:]

            if response.status_code == 200:
                # --- FIX START: Handle None content ---
                record_api_success(api_slot_idx_slop_fixer)
                response_data = response.json()
                content = response_data['choices'][0]['message'].get('content')

                # NEW: Extract token usage if available
                usage = response_data.get('usage', {})
                input_tokens = usage.get('prompt_tokens', 0)
                output_tokens = usage.get('completion_tokens', 0)

                # Update global counters safely using the lock
                with app_state.system_prompt_lock:
                    app_state.total_input_tokens += input_tokens
                    app_state.total_output_tokens += output_tokens

                if content is None:
                    log_message(f"Thread {thread_id}: API returned None for content (API Slot {api_slot_idx_slop_fixer+1}, Attempt {attempt_num+1})", "WARNING")
                    if attempt_num < current_max_attempts_param - 1:
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None, text_context

                rewritten_sentence = content.strip()

                # FIXED: Don't return None for content issues - let the caller handle retries
                if not rewritten_sentence or len(rewritten_sentence) < 5:
                    log_message(f"Thread {thread_id}: Slop fixer returned empty/very short response. Original: '{text_context}'", "WARNING")
                    return None, text_context

                if rewritten_sentence.count('\n') > 1 or len(rewritten_sentence) > len(text_context) * 2:
                    log_message(f"Thread {thread_id}: Slop fixer response appears malformed. Using original.", "WARNING")
                    return None, text_context

                # Only strip wrapper quotes the fixer ADDED around its rewrite.
                # If the ORIGINAL sentence was itself a fully-quoted line of
                # dialogue (e.g. "Get out of here!"), the LLM correctly returning
                # it still-quoted must NOT be unwrapped here -- stripping would
                # eat the real opening/closing quotes and is a direct source of
                # "missing a quote on one side of dialogue".
                _orig = text_context.strip()
                _orig_wrapped = _orig.startswith('"') and _orig.endswith('"') and len(_orig) > 2
                if (rewritten_sentence.startswith('"') and rewritten_sentence.endswith('"')
                        and len(rewritten_sentence) > 2 and not _orig_wrapped):
                    rewritten_sentence = rewritten_sentence[1:-1]

                if not rewritten_sentence or len(rewritten_sentence) < 0.5 * len(text_context):
                    log_message(f"Thread {thread_id}: Slop fixer returned very short/empty sentence: '{rewritten_sentence}'. Original: '{text_context}'", "WARNING")
                    return None, text_context

                return rewritten_sentence, text_context
            else: # API call failed
                error_message = f"Thread {thread_id}: Slop Fixer LLM Error (API Slot {api_slot_idx_slop_fixer+1}, Attempt {attempt_num+1}/{current_max_attempts_param}, Status: {response.status_code}): {response.text[:200]}"
                log_message(error_message, "ERROR")
                record_api_failure(api_slot_idx_slop_fixer)
                with app_state.system_prompt_lock:
                    app_state.error_count_total +=1
                    app_state.error_counts_per_api[api_slot_idx_slop_fixer] += 1
                    err_summary = f"T{thread_id} SlopFix-API (API{api_slot_idx_slop_fixer+1}): S{response.status_code} A{attempt_num+1}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx_slop_fixer))
                if attempt_num < current_max_attempts_param - 1:
                    time.sleep(random.uniform(0.5, 1.5))
                    continue
                else:
                    return None, text_context
        except requests.exceptions.Timeout:
            error_message = f"Thread {thread_id}: Slop Fixer LLM request timed out (API Slot {api_slot_idx_slop_fixer+1}, Attempt {attempt_num+1}/{current_max_attempts_param})."
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx_slop_fixer)
            with app_state.system_prompt_lock:
                app_state.error_count_total +=1
                app_state.error_counts_per_api[api_slot_idx_slop_fixer] += 1
                err_summary = f"T{thread_id} SlopFix-Timeout (API{api_slot_idx_slop_fixer+1}) A{attempt_num+1}"
                if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                app_state.recent_errors_total.append((err_summary, api_slot_idx_slop_fixer))
            if attempt_num < current_max_attempts_param - 1:
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None, text_context
        except Exception as e: # Catch any other exceptions
            error_message = f"Thread {thread_id}: Exception in call_slop_fixer_llm (API Slot {api_slot_idx_slop_fixer+1}, Attempt {attempt_num+1}/{current_max_attempts_param}): {str(e)}"
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx_slop_fixer)
            with app_state.system_prompt_lock:
                app_state.error_count_total +=1
                app_state.error_counts_per_api[api_slot_idx_slop_fixer] += 1
                err_summary = f"T{thread_id} SlopFix-Exc (API{api_slot_idx_slop_fixer+1}) A{attempt_num+1}: {str(e)[:20]}"
                if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                app_state.recent_errors_total.append((err_summary, api_slot_idx_slop_fixer))
            if attempt_num < current_max_attempts_param - 1:
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None, text_context
    return None, text_context

def call_anti_slop_llm(text_context, anti_slop_phrase,
                       anti_slop_api_config,
                       main_sampler_settings, thread_id, additional_fix_instructions="",
                       current_max_attempts_param=5,
                       master_duplication_enabled=False,
                       api_request_timeout_param=300):
    """Calls a dedicated LLM to rewrite a sentence containing anti-slop phrases."""
    api_slot_idx_anti_slop = 5

    if not anti_slop_api_config or not anti_slop_api_config.get('url') or \
       not anti_slop_api_config.get('model') or not anti_slop_api_config.get('key'):
        log_message(f"Thread {thread_id}: Anti-Slop LLM not fully configured. Cannot call.", "WARNING")
        return None, text_context

    api_url = anti_slop_api_config['url']
    model_name = anti_slop_api_config['model']
    api_key = anti_slop_api_config['key']

    for attempt_num in range(current_max_attempts_param):
        if app_state.stop_processing or app_state.pause_processing: return None, text_context

        # FIX 2 & 3: Move rate limiter BEFORE lock, and add timeout to lock acquisition
        global_rate_limiter.wait_if_needed(api_slot_idx_anti_slop)

        # Circuit Breaker Check
        if not check_circuit_breaker(api_slot_idx_anti_slop):
            log_message(f"Thread {thread_id}: API Slot {api_slot_idx_anti_slop+1} circuit open. Skipping anti-slop fixer.", "DEBUG")
            return None, text_context

        lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
        if lock_acquired:
            try:
                app_state.total_attempts_global += 1
                app_state.total_attempts_per_api[api_slot_idx_anti_slop] += 1
            finally:
                app_state.system_prompt_lock.release()
        else:
            log_message(f"Thread {thread_id}: WARNING - Could not acquire system_prompt_lock", "WARNING")
            return None, text_context

        try:
            # UPDATED PROMPT
            user_rewrite_instruction = (
                f"The following text contains an undesirable phrase: '{anti_slop_phrase}'. "
                f"Rewrite the text to remove or rephrase this specific undesirable phrase while preserving the original meaning, tone, and ALL quotation marks. "
                f"CRITICAL: Do not drop any opening or closing quotation marks. Ensure quotes remain perfectly balanced. "
                f"ONLY output the rewritten text. Do not include any preamble, explanation, quotes, or other text."
            )
            if additional_fix_instructions:
                user_rewrite_instruction += f"\n\nAdditional instruction: {additional_fix_instructions}"

            user_rewrite_instruction += (
                "\n\nCRITICAL RULES:\n"
                "1. Preserve ALL existing quotation marks exactly where they appear structurally.\n"
                "2. If you must add or remove a quote, ensure every opening mark has a matching closing mark.\n"
                "3. Do not wrap the entire output in quotes."
            )

            user_rewrite_instruction += f"\n\n<original_text>\n{text_context}\n</original_text>"

            messages = [
                {"role": "system", "content": "You are an expert editor. Rewrite the given text to remove the specified undesirable phrase, ensuring the core meaning is kept. Output only the rewritten text."},
                {"role": "user", "content": user_rewrite_instruction}
            ]

            # Use dedicated Anti-Slop sampler settings from the API config
            anti_slop_sampler_overrides = anti_slop_api_config.get('sampler_settings', {}) or main_sampler_settings

            final_anti_slop_params = {
                "temperature": anti_slop_sampler_overrides.get("temperature", 0.5),
                "top_p": anti_slop_sampler_overrides.get("top_p", 0.95),
                "min_p": anti_slop_sampler_overrides.get("min_p", 0.0),
                "top_k": anti_slop_sampler_overrides.get("top_k", 50),
                "repetition_penalty": anti_slop_sampler_overrides.get("repetition_penalty", 1.1),
                "max_tokens": anti_slop_sampler_overrides.get("max_tokens", len(text_context.split()) * 3 + 70),
            }

            payload_data = {
                "model": model_name,
                "messages": messages,
                **final_anti_slop_params,
                "stream": False
            }
            payload = json.dumps(payload_data)
            headers = {
                'Content-Type': 'application/json'
            }
            if api_key:
                headers['Authorization'] = f"Bearer {api_key}"

            current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{api_slot_idx_anti_slop}.jsonl" if master_duplication_enabled else BASE_DEBUG_LOG_PATH + ".jsonl"
            with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
                debug_log.write(json.dumps({"timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "thread_id": thread_id, "type": "anti_slop_request", "api_slot_idx": api_slot_idx_anti_slop, "attempt": attempt_num + 1, "api_url": api_url, "model": model_name, "messages": messages, "payload_data": payload_data }) + '\n')

            # FIX 2: Rate limiter already called BEFORE lock acquisition above
            # Track API response time
            api_call_start_time = time.time()
            response = requests.post(api_url, headers=headers, data=payload, timeout=(api_request_timeout_param, api_request_timeout_param))

            api_response_time = time.time() - api_call_start_time
            with api_response_times_lock:
                api_response_times_per_slot[api_slot_idx_anti_slop].append(api_response_time)
                if len(api_response_times_per_slot[api_slot_idx_anti_slop]) > MAX_RESPONSE_TIMES_TO_TRACK:
                    api_response_times_per_slot[api_slot_idx_anti_slop] = api_response_times_per_slot[api_slot_idx_anti_slop][-MAX_RESPONSE_TIMES_TO_TRACK:]

            if response.status_code == 200:
                record_api_success(api_slot_idx_anti_slop)
                response_data = response.json()
                content = response_data['choices'][0]['message'].get('content')
                usage = response_data.get('usage', {})
                input_tokens = usage.get('prompt_tokens', 0)
                output_tokens = usage.get('completion_tokens', 0)

                lock_acquired_tokens = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired_tokens:
                    try:
                        app_state.total_input_tokens += input_tokens
                        app_state.total_output_tokens += output_tokens
                    finally:
                        app_state.system_prompt_lock.release()
                else:
                    log_message(f"Thread {thread_id}: WARNING - Could not acquire system_prompt_lock for token update.", "WARNING")

                if content is None:
                    log_message(f"Thread {thread_id}: API returned None for anti-slop content (Attempt {attempt_num+1})", "WARNING")
                    if attempt_num < current_max_attempts_param - 1:
                        if app_state.stop_processing or app_state.pause_processing: return None, text_context
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None, text_context

                rewritten_sentence = content.strip()
                if not rewritten_sentence or len(rewritten_sentence) < 5:
                    log_message(f"Thread {thread_id}: Anti-slop fixer returned empty/very short sentence. Content: '{rewritten_sentence}'", "WARNING")
                    return None, text_context

                if rewritten_sentence.count('\n') > 1 or len(rewritten_sentence) > len(text_context) * 2:
                    log_message(f"Thread {thread_id}: Anti-slop fixer response appears malformed. Using original.", "WARNING")
                    return None, text_context

                # Only strip wrapper quotes the fixer ADDED around its rewrite.
                # If the ORIGINAL sentence was itself a fully-quoted line of
                # dialogue (e.g. "Get out of here!"), the LLM correctly returning
                # it still-quoted must NOT be unwrapped here -- stripping would
                # eat the real opening/closing quotes and is a direct source of
                # "missing a quote on one side of dialogue".
                _orig = text_context.strip()
                _orig_wrapped = _orig.startswith('"') and _orig.endswith('"') and len(_orig) > 2
                if (rewritten_sentence.startswith('"') and rewritten_sentence.endswith('"')
                        and len(rewritten_sentence) > 2 and not _orig_wrapped):
                    rewritten_sentence = rewritten_sentence[1:-1]

                if not rewritten_sentence or len(rewritten_sentence) < 0.5 * len(text_context):
                    log_message(f"Thread {thread_id}: Anti-slop fixer returned very short/empty sentence", "WARNING")
                    return None, text_context

                return rewritten_sentence, text_context

            else:
                error_message = f"Thread {thread_id}: Anti-Slop LLM Error (Attempt {attempt_num+1}/{current_max_attempts_param}, Status: {response.status_code}): {response.text[:200]}"
                log_message(error_message, "ERROR")
                record_api_failure(api_slot_idx_anti_slop)
                lock_acquired_err = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired_err:
                    try:
                        app_state.error_count_total += 1
                        app_state.error_counts_per_api[api_slot_idx_anti_slop] += 1
                        err_summary = f"T{thread_id} AntiSlop-API: S{response.status_code} A{attempt_num+1}"
                        if len(app_state.recent_errors_total) >= MAX_RECENT:
                            app_state.recent_errors_total.pop(0)
                        app_state.recent_errors_total.append((err_summary, api_slot_idx_anti_slop))
                    finally:
                        app_state.system_prompt_lock.release()

                if attempt_num < current_max_attempts_param - 1:
                    if app_state.stop_processing or app_state.pause_processing: return None, text_context
                    time.sleep(random.uniform(0.5, 1.5))
                    continue
                else:
                    return None, text_context

        except requests.exceptions.Timeout:
            error_message = f"Thread {thread_id}: Anti-Slop LLM request timed out (Attempt {attempt_num+1}/{current_max_attempts_param})."
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx_anti_slop)
            lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
            if lock_acquired:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx_anti_slop] += 1
                    err_summary = f"T{thread_id} AntiSlop-Timeout A{attempt_num+1}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT:
                        app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx_anti_slop))
                finally:
                    app_state.system_prompt_lock.release()

            if attempt_num < current_max_attempts_param - 1:
                if app_state.stop_processing or app_state.pause_processing: return None, text_context
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None, text_context

        except Exception as e:
            error_message = f"Thread {thread_id}: Exception in call_anti_slop_llm (Attempt {attempt_num+1}/{current_max_attempts_param}): {str(e)}"
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx_anti_slop)
            lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
            if lock_acquired:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx_anti_slop] += 1
                    err_summary = f"T{thread_id} AntiSlop-Exc A{attempt_num+1}: {str(e)[:20]}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT:
                        app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx_anti_slop))
                finally:
                    app_state.system_prompt_lock.release()

            if attempt_num < current_max_attempts_param - 1:
                if app_state.stop_processing or app_state.pause_processing: return None, text_context
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None, text_context

    return None, text_context


def generate_answer_with_retries(base_system_prompt, conversation_history_for_llm, answer_prompt_template,
                                 thread_id, q, sampler_settings_local, api_url_local, model_name_local, api_key_local,
                                 refusal_phrases_local, user_speaking_phrases_local, slop_phrases_local,
                                 current_anti_slop_phrases_param,
                                 jailbreaks_local, speaking_fixes_local, slop_fixes_fallback_local,
                                 max_attempts_local,
                                 slop_fixer_api_config_param,
                                 current_slop_fixes_for_rotation_param,
                                 api_slot_idx,
                                 current_max_attempts_for_slop_fixer_call,
                                 anti_slop_fixer_api_config_param,
                                 master_duplication_enabled_local,
                                 no_user_impersonation_local,
                                 api_request_timeout_param):
    """
    Generates an assistant's answer, handling retries for refusals, user speaking, and slop.
    Applies jailbreaks, speaking fixes, and slop fixes (system prompt or dedicated LLM).
    Returns the generated answer or None if all attempts fail.
    """

    refusal_detected_this_main_api_call = False
    issue_ever_detected_this_task = False
    refusal_ever_detected_this_task = False

    if not api_url_local:
        log_message(f"Thread {thread_id}: API URL missing for answer generation (API Slot {api_slot_idx+1}). Cannot proceed.", "ERROR")
        return None

    current_system_prompt_iter = base_system_prompt

    for attempt in range(max_attempts_local):
        if app_state.stop_processing or app_state.pause_processing: return None

        api_call_retries_for_this_iteration = current_max_attempts_for_slop_fixer_call
        fix_attempts_specific = {'refusal': 0, 'user_speaking': 0, 'slop_fallback': 0, 'incomplete_quote': 0}
        issue_detected_this_main_api_call = False

        while True:
            if app_state.stop_processing or app_state.pause_processing: return None

            # --- API Call with Retries for API Failures ---
            answer = None
            response_text_content = ""
            response_status_code = -1

            for api_call_attempt_num in range(api_call_retries_for_this_iteration):
                if app_state.stop_processing or app_state.pause_processing: return None
                MAX_TOTAL_RETRY_WAIT = 30  # Cap total sleep per outer attempt
                current_attempt_wait = 0

                # FIX 3: Add timeout to lock acquisition
                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired:
                    try:
                        app_state.total_attempts_global += 1
                        app_state.total_attempts_per_api[api_slot_idx] += 1
                    finally:
                        app_state.system_prompt_lock.release()
                else:
                    log_message(f"Thread {thread_id}: Skipped stat update (system_prompt_lock busy)", "DEBUG")

                messages = [{"role": "system", "content": current_system_prompt_iter}] + \
                           conversation_history_for_llm + \
                           [{"role": "user", "content": answer_prompt_template}]

                # --- LIVE PREVIEW HOOK ---
                update_live_prompt_preview(messages)
                # --- END HOOK ---

                payload_dict_ans = {
                    "model": model_name_local,
                    "messages": messages,
                    **sampler_settings_local.get("generation_params", {
                        "temperature": sampler_settings_local.get("temperature",0.5),
                        "top_p": sampler_settings_local.get("top_p",0.9),
                        "min_p": sampler_settings_local.get("min_p", 0.0),
                        "top_k": sampler_settings_local.get("top_k",50),
                        "repetition_penalty": sampler_settings_local.get("repetition_penalty",1.1),
                        "max_tokens": sampler_settings_local.get("max_tokens_answer", global_config.get('samplers.max_tokens_answer',1024))
                    }),
                    "stream": False
                }

                logit_bias_str = sampler_settings_local.get('logit_bias', '')
                if logit_bias_str:
                    try:
                        payload_dict_ans['logit_bias'] = json.loads(logit_bias_str)
                    except json.JSONDecodeError:
                        log_message(f"Thread {thread_id}: Invalid logit_bias JSON. Skipping.", "WARNING")

                thinking_mode = sampler_settings_local.get('enable_thinking', 'default')
                if thinking_mode == 'enable':
                    payload_dict_ans['chat_template_kwargs'] = {"enable_thinking": True}
                elif thinking_mode == 'disable':
                    payload_dict_ans['chat_template_kwargs'] = {"enable_thinking": False}
                # else 'default': do not send the parameter

                payload = json.dumps(payload_dict_ans)
                headers = {
                    'Content-Type': 'application/json'
                }

                if api_key_local:
                    headers['Authorization'] = f"Bearer {api_key_local}"

                current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{api_slot_idx}.jsonl" if app_state.master_duplication_enabled_var.get() else BASE_DEBUG_LOG_PATH + ".jsonl"
                with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
                    debug_log.write(json.dumps({"timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "thread_id": thread_id, "type": "answer_request", "api_slot_idx": api_slot_idx, "outer_attempt": attempt +1, "inner_api_call_attempt": api_call_attempt_num + 1, "fix_attempts_specific": fix_attempts_specific, "current_system_prompt_iter_len": len(current_system_prompt_iter), "messages_len": len(messages), "payload_dict_ans": payload_dict_ans}) + '\n')

                global_rate_limiter.wait_if_needed(api_slot_idx)

                # Circuit Breaker Check
                if not check_circuit_breaker(api_slot_idx):
                    log_message(f"Thread {thread_id}: API Slot {api_slot_idx+1} circuit open. Skipping answer generation.", "DEBUG")
                    break  # Exit inner retry loop; outer loop handles fallback

                api_call_start_time = time.time()
                prompt_content = json.dumps(messages, sort_keys=True)
                prompt_hash = hashlib.md5(prompt_content.encode()).hexdigest()

                cached_response, is_cached = get_cached_response(prompt_hash, api_slot_idx)
                if is_cached:
                    answer = cached_response
                    log_message(f"Cache HIT for answer generation (API Slot {api_slot_idx+1}).", "DEBUG")
                    break

                try:
                    response = requests.post(api_url_local, headers=headers, data=payload, timeout=(api_request_timeout_param, api_request_timeout_param))
                    api_response_time = time.time() - api_call_start_time

                    with api_response_times_lock:
                        api_response_times_per_slot[api_slot_idx].append(api_response_time)
                    if len(api_response_times_per_slot[api_slot_idx]) > MAX_RESPONSE_TIMES_TO_TRACK:
                        api_response_times_per_slot[api_slot_idx] = api_response_times_per_slot[api_slot_idx][-MAX_RESPONSE_TIMES_TO_TRACK:]

                    response_status_code = response.status_code
                    response_text_content = response.text

                    if response.status_code in [503, 429]:
                        retry_after = response.headers.get('Retry-After', 60)
                        try:
                            retry_after = int(retry_after)
                        except (ValueError, TypeError):
                            retry_after = 60
                        log_message(f"Thread {thread_id}: API Slot {api_slot_idx+1} is overloaded (Status {response.status_code}). Waiting {retry_after}s before retry.", "WARNING")
                        time.sleep(retry_after)
                        if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                            continue
                        else:
                            break

                    if response.status_code == 200:
                        record_api_success(api_slot_idx)
                        response_data = response.json()
                        content = response_data['choices'][0]['message'].get('content')

                        usage = response_data.get('usage', {})
                        input_tokens = usage.get('prompt_tokens', 0)
                        output_tokens = usage.get('completion_tokens', 0)

                        lock_acquired_tokens = app_state.system_prompt_lock.acquire(timeout=0.05)
                        if lock_acquired_tokens:
                            try:
                                app_state.total_input_tokens += input_tokens
                                app_state.total_output_tokens += output_tokens
                            finally:
                                app_state.system_prompt_lock.release()
                        else:
                            log_message(f"Thread {thread_id}: WARNING - Could not acquire system_prompt_lock for token update.", "WARNING")

                        if content is None:
                            log_message(f"Thread {thread_id}: API returned None for answer content (API Slot {api_slot_idx+1}, OuterAttempt {attempt + 1}, API Call Attempt {api_call_attempt_num+1})", "WARNING")
                            if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                                sleep_dur = random.uniform(0.5, 1.5)
                                if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                                    time.sleep(sleep_dur)
                                    current_attempt_wait += sleep_dur
                                    continue
                                else:
                                    return None  # Cap reached, abort question generation
                        answer = content.strip()

                        if not answer or len(answer) < 10:
                            log_message(f"Thread {thread_id}: API returned empty/very short answer. Content: '{answer}'", "WARNING")
                            if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                                sleep_dur = random.uniform(0.5, 1.5)
                                if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                                    time.sleep(sleep_dur)
                                    current_attempt_wait += sleep_dur
                                    continue
                                else:
                                    return None  # Cap reached, abort question generation

                        newline_count = answer.count('\n')
                        text_length = len(answer)

                        max_newlines = global_config.get('generation.max_newlines_malformed', 16)
                        max_text_length = global_config.get('generation.max_text_length_malformed', 5000)

                        if newline_count > max_newlines or text_length > max_text_length:
                            log_message(
                                f"Thread {thread_id}: Answer response appears malformed. "
                                f"Newlines: {newline_count} (Max: {max_newlines}), Length: {text_length} (Max: {max_text_length}). "
                                f"Snippet: '{answer[:100]}...'",
                                "WARNING"
                            )
                            sleep_dur = random.uniform(0.5, 1.5)
                            if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                                time.sleep(sleep_dur)
                                current_attempt_wait += sleep_dur
                                continue
                            else:
                                return None  # Cap reached, abort question generation
                        break
                    else:
                        log_message(f"Thread {thread_id}: Error generating answer (API Slot {api_slot_idx+1}, OuterAttempt {attempt + 1}, API Call Attempt {api_call_attempt_num+1}/{api_call_retries_for_this_iteration}), Status {response_status_code}: {response_text_content[:200]}", "ERROR")
                        record_api_failure(api_slot_idx)
                        lock_acquired_err = app_state.system_prompt_lock.acquire(timeout=0.05)
                        if lock_acquired_err:
                            try:
                                app_state.error_count_total += 1
                                app_state.error_counts_per_api[api_slot_idx] += 1
                                err_summary = f"T{thread_id} Ans-Err (API{api_slot_idx+1}): S{response_status_code} A{api_call_attempt_num+1}"
                                if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                                app_state.recent_errors_total.append((err_summary, api_slot_idx))
                                with app_state.issue_timestamps_lock:
                                    app_state.issue_timestamps['errors'].append(time.time())
                                    cutoff = time.time() - 3600
                                    app_state.issue_timestamps['errors'] = [t for t in app_state.issue_timestamps['errors'] if t > cutoff]
                                if api_slot_idx < 6:
                                    if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                                    app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                            finally:
                                app_state.system_prompt_lock.release()
                        if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                            sleep_dur = random.uniform(0.5, 1.5)
                            if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                                time.sleep(sleep_dur)
                                current_attempt_wait += sleep_dur
                                continue
                            else:
                                return None  # Cap reached, abort question generation

                except requests.exceptions.Timeout:
                    log_message(f"Thread {thread_id}: API Timeout generating answer (API Slot {api_slot_idx+1}, OuterAttempt {attempt + 1}, API Call Attempt {api_call_attempt_num+1}/{api_call_retries_for_this_iteration}).", "ERROR")
                    record_api_failure(api_slot_idx)
                    lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                    if lock_acquired:
                        try:
                            app_state.error_count_total += 1; app_state.error_counts_per_api[api_slot_idx] += 1
                            err_summary = f"T{thread_id} Ans-Timeout (API{api_slot_idx+1}) A{api_call_attempt_num+1}"
                            if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                            app_state.recent_errors_total.append((err_summary, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                                app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                        finally:
                            app_state.system_prompt_lock.release()
                    if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                        time.sleep(random.uniform(0.5, 1.5)); continue
                    else: break
                except requests.exceptions.RequestException as e_req:
                    log_message(f"Thread {thread_id}: RequestException generating answer (API Slot {api_slot_idx+1}, OuterAttempt {attempt + 1}, API Call Attempt {api_call_attempt_num+1}/{api_call_retries_for_this_iteration}): {str(e_req)}", "ERROR")
                    record_api_failure(api_slot_idx)
                    lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                    if lock_acquired:
                        try:
                            app_state.error_count_total += 1; app_state.error_counts_per_api[api_slot_idx] += 1
                            err_summary = f"T{thread_id} Ans-ReqExc (API{api_slot_idx+1}) A{api_call_attempt_num+1}"
                            if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                            app_state.recent_errors_total.append((err_summary, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                                app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                        finally:
                            app_state.system_prompt_lock.release()
                    if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                        time.sleep(random.uniform(0.5, 1.5)); continue
                    else: break
                except Exception as e_gen:
                    log_message(f"Thread {thread_id}: Exception in answer generation (API Slot {api_slot_idx+1}, OuterAttempt {attempt + 1}, API Call Attempt {api_call_attempt_num+1}/{api_call_retries_for_this_iteration}): {str(e_gen)}", "ERROR")
                    import traceback; log_message(traceback.format_exc(), "ERROR")
                    record_api_failure(api_slot_idx)
                    lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                    if lock_acquired:
                        try:
                            app_state.error_count_total += 1; app_state.error_counts_per_api[api_slot_idx] += 1
                            err_summary = f"T{thread_id} Ans-GenExc (API{api_slot_idx+1}) A{api_call_attempt_num+1}: {str(e_gen)[:20]}"
                            if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                            app_state.recent_errors_total.append((err_summary, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                                app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                        finally:
                            app_state.system_prompt_lock.release()
                    if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                        time.sleep(random.uniform(0.5, 1.5)); continue
                    else: break

            if answer is None:
                log_message(f"Thread {thread_id}: All API call attempts failed for current content iteration (OuterAttempt {attempt+1}, API Slot {api_slot_idx+1}).", "WARNING")
                break

            # --- Issue Detection ---
            issue_detected_this_main_api_call = False
            refusal_detected, refusal_info = detection.is_refusal(answer, refusal_phrases_local)
            user_speaking_detected, user_speaking_info = False, []
            if not no_user_impersonation_local:
                user_speaking_detected, user_speaking_info = detection.is_user_speaking(answer, user_speaking_phrases_local)
            slop_detected, slop_info = detection.is_slop(answer, slop_phrases_local)

            if refusal_detected:
                issue_detected_this_main_api_call = True
                issue_ever_detected_this_task = True
                refusal_detected_this_main_api_call = True
                refusal_ever_detected_this_task = True
                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired:
                    try:
                        app_state.refusal_count_total += 1
                        app_state.refusal_counts_per_api[api_slot_idx] += 1
                        if refusal_info:
                            detected_phrase, detected_sentence = refusal_info[0]
                            if len(app_state.recent_refusals_total) >= MAX_RECENT: app_state.recent_refusals_total.pop(0)
                            app_state.recent_refusals_total.append((detected_phrase, detected_sentence, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_refusals_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_refusals_per_api[api_slot_idx].pop(0)
                                app_state.recent_refusals_per_api[api_slot_idx].append((detected_phrase, detected_sentence))
                    finally:
                        app_state.system_prompt_lock.release()
                if fix_attempts_specific['refusal'] < len(jailbreaks_local):
                    current_system_prompt_iter += f" {jailbreaks_local[fix_attempts_specific['refusal']]}"
                    fix_attempts_specific['refusal'] += 1
                    log_message(f"Thread {thread_id}: Refusal detected (API Slot {api_slot_idx+1}). Applying jailbreak {fix_attempts_specific['refusal']}. Retrying API call.", "DEBUG")
                    continue
                else:
                    log_message(f"Thread {thread_id}: Refusal detected (API Slot {api_slot_idx+1}), jailbreaks exhausted for this attempt {attempt+1}.", "WARNING")
                    break

            if user_speaking_detected:
                issue_detected_this_main_api_call = True
                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired:
                    try:
                        app_state.user_speaking_count_total += 1
                        app_state.user_speaking_counts_per_api[api_slot_idx] += 1
                        if user_speaking_info:
                            detected_phrase, detected_sentence = user_speaking_info[0]
                            if len(app_state.recent_user_speaking_total) >= MAX_RECENT: app_state.recent_user_speaking_total.pop(0)
                            app_state.recent_user_speaking_total.append((detected_phrase, detected_sentence, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_user_speaking_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_user_speaking_per_api[api_slot_idx].pop(0)
                                app_state.recent_user_speaking_per_api[api_slot_idx].append((detected_phrase, detected_sentence))
                    finally:
                        app_state.system_prompt_lock.release()
                if fix_attempts_specific['user_speaking'] < len(speaking_fixes_local):
                    current_system_prompt_iter += f" {speaking_fixes_local[fix_attempts_specific['user_speaking']]}"
                    fix_attempts_specific['user_speaking'] += 1
                    log_message(f"Thread {thread_id}: User speaking detected (API Slot {api_slot_idx+1}). Applying fix {fix_attempts_specific['user_speaking']}. Retrying API call.", "DEBUG")
                    continue
                else:
                    log_message(f"Thread {thread_id}: User speaking detected (API Slot {api_slot_idx+1}), fixes exhausted for this attempt {attempt+1}.", "WARNING")
                    break

            if slop_detected:
                issue_detected_this_main_api_call = True
                log_message(f"Thread {thread_id}: Initial slop detected in answer (API Slot {api_slot_idx+1}). Snippet: {answer[:70]}...", "DEBUG")

                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired:
                    try:
                        app_state.slop_count_total += 1
                        app_state.slop_counts_per_api[api_slot_idx] += 1
                        if slop_info:
                            detected_phrase, detected_sentence = slop_info[0]
                            if len(app_state.recent_slop_total) >= MAX_RECENT: app_state.recent_slop_total.pop(0)
                            app_state.recent_slop_total.append((detected_phrase, detected_sentence, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_slop_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_slop_per_api[api_slot_idx].pop(0)
                                app_state.recent_slop_per_api[api_slot_idx].append((detected_phrase, detected_sentence))
                    finally:
                        app_state.system_prompt_lock.release()

                if slop_fixer_api_config_param and slop_fixer_api_config_param.get('url'):
                    current_answer_being_fixed = answer
                    MAX_SENTENCE_FIX_ITERATIONS = global_config.get('generation.max_slop_sentence_fix_iterations', 4)
                    slop_fully_resolved_by_sentence_fixer = False
                    slop_fix_instruction_rotation_idx = 0

                    for slop_iter_num in range(MAX_SENTENCE_FIX_ITERATIONS):
                        if app_state.stop_processing or app_state.pause_processing: return None
                        current_slop_check_needed, current_slop_details_iter = detection.is_slop(current_answer_being_fixed, slop_phrases_local)
                        if not current_slop_check_needed:
                            log_message(f"Thread {thread_id}: Slop paragraph fully resolved after {slop_iter_num} iterations.", "INFO")
                            answer = current_answer_being_fixed
                            issue_detected_this_main_api_call = False
                            slop_fully_resolved_by_sentence_fixer = True
                            break

                        phrase_to_fix_iter, sentence_to_fix_iter = current_slop_details_iter[0]

                        phrase_to_fix_iter, sentence_to_fix_iter = current_slop_details_iter[0]

                        # Extract the full paragraph context to preserve quotes
                        paragraphs = current_answer_being_fixed.split('\n\n')
                        context_for_fixer = next((p for p in paragraphs if sentence_to_fix_iter in p), sentence_to_fix_iter)

                        additional_instructions_for_llm_fixer = ""
                        if slop_iter_num >= 2 and current_slop_fixes_for_rotation_param:
                            additional_instructions_for_llm_fixer = current_slop_fixes_for_rotation_param[slop_fix_instruction_rotation_idx % len(current_slop_fixes_for_rotation_param)]
                            slop_fix_instruction_rotation_idx +=1
                            log_message(f"Thread {thread_id}: SlopFixer iter {slop_iter_num+1}. Adding rotating fix: '{additional_instructions_for_llm_fixer}'", "DEBUG")

                        log_message(f"Thread {thread_id}: Fixing slop paragraph (Iter {slop_iter_num+1}): '{phrase_to_fix_iter}' in context...", "DEBUG")

                        rewritten_sentence_part, original_sentence_part = call_slop_fixer_llm(
                            context_for_fixer, phrase_to_fix_iter, # Pass paragraph instead of single sentence
                            slop_fixer_api_config_param,
                            sampler_settings_local,
                            thread_id,
                            additional_fix_instructions=additional_instructions_for_llm_fixer,
                            current_max_attempts_param=current_max_attempts_for_slop_fixer_call
                        )

                        if rewritten_sentence_part and original_sentence_part:
                            has_incomplete_quote, _ = detection.is_incomplete_quote(rewritten_sentence_part)
                            if has_incomplete_quote:
                                log_message(f"Thread {thread_id}: Slop fixer returned sentence with unbalanced quotes. Skipping replacement for this sentence.", "WARNING")
                                slop_fully_resolved_by_sentence_fixer = False
                                break # Stop this sentence's fix attempt to prevent propagating malformed quotes
                            if original_sentence_part in current_answer_being_fixed:
                                if rewritten_sentence_part.strip() == original_sentence_part.strip():
                                    log_message(f"Thread {thread_id}: Slop fixer returned same part for '{phrase_to_fix_iter}'. Iter {slop_iter_num+1}.", "DEBUG")
                                    slop_fully_resolved_by_sentence_fixer = False
                                    break
                                else:
                                    current_answer_being_fixed = current_answer_being_fixed.replace(original_sentence_part, rewritten_sentence_part, 1)
                                    log_message(f"Thread {thread_id}: Sentence part rewritten. New snippet: {current_answer_being_fixed[:70]}...", "DEBUG")
                            else:
                                log_message(f"Thread {thread_id}: Original sentence for slop fix ('{original_sentence_part[:70]}...') not found in current answer. Iter {slop_iter_num+1}.", "WARNING")
                                slop_fully_resolved_by_sentence_fixer = False
                                break
                        else:
                            log_message(f"Thread {thread_id}: Slop fixer LLM failed rewrite for '{phrase_to_fix_iter}'. Aborting sentence fixing.", "WARNING")
                            slop_fully_resolved_by_sentence_fixer = False
                            break

                    if not slop_fully_resolved_by_sentence_fixer:
                        log_message(f"Thread {thread_id}: Sentence-level slop fixing failed or max iters for API {api_slot_idx+1}. Slop may remain. Attempting fallback system prompt fix.", "WARNING")
                    else:
                        answer = current_answer_being_fixed
                        issue_detected_this_main_api_call = False

            # --- Anti-Slop Detection and Fixing (Sentence-Level, Like Regular Slop) ---
            anti_slop_detected, anti_slop_info = detection.is_anti_slop(answer, current_anti_slop_phrases_param)

            # FIX 1: Initialize BEFORE the conditional block
            anti_slop_fully_resolved = False

            if anti_slop_detected:
                issue_detected_this_main_api_call = True
                log_message(f"Thread {thread_id}: Anti-slop detected in answer (API Slot {api_slot_idx+1}). Snippet: {answer[:70]}...", "DEBUG")

                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired:
                    try:
                        app_state.anti_slop_count_total += 1
                        anti_slop_counts_per_api[api_slot_idx] += 1
                        if anti_slop_info:
                            detected_phrase, detected_sentence = anti_slop_info[0]
                            if len(app_state.recent_anti_slop_total) >= MAX_RECENT:
                                app_state.recent_anti_slop_total.pop(0)
                            app_state.recent_anti_slop_total.append((detected_phrase, detected_sentence, api_slot_idx))
                            if api_slot_idx < 4:
                                if len(app_state.recent_anti_slop_per_api[api_slot_idx]) >= MAX_RECENT:
                                    app_state.recent_anti_slop_per_api[api_slot_idx].pop(0)
                                    app_state.recent_anti_slop_per_api[api_slot_idx].append((detected_phrase, detected_sentence))
                    finally:
                        app_state.system_prompt_lock.release()

                # Try to fix using anti-slop LLM - fix individual SENTENCES (like regular slop)
                if slop_fixer_api_config_param and slop_fixer_api_config_param.get('url'):
                    current_answer_being_fixed = answer
                    MAX_ANTI_SLOP_FIX_ITERATIONS = global_config.get('generation.max_anti_slop_fix_iterations', 10)
                    # anti_slop_fully_resolved = False  # REMOVED - already initialized above
                    anti_slop_fix_instruction_rotation_idx = 0

                    for anti_slop_iter_num in range(MAX_ANTI_SLOP_FIX_ITERATIONS):
                        if app_state.stop_processing or app_state.pause_processing:
                            return None

                        current_anti_slop_check, current_anti_slop_details = detection.is_anti_slop(current_answer_being_fixed, current_anti_slop_phrases_param)

                        if not current_anti_slop_check:
                            log_message(f"Thread {thread_id}: All anti-slop fixed (API Slot {api_slot_idx+1}) after {anti_slop_iter_num} rewrites.", "INFO")
                            answer = current_answer_being_fixed
                            issue_detected_this_main_api_call = False
                            anti_slop_fully_resolved = True
                            break

                        phrase_to_fix = current_anti_slop_details[0][0]
                        sentence_to_fix = current_anti_slop_details[0][1]

                        # Extract the full paragraph context to preserve quotes
                        paragraphs = current_answer_being_fixed.split('\n\n')
                        context_for_fixer = next((p for p in paragraphs if sentence_to_fix in p), sentence_to_fix)

                        additional_instructions = ""
                        if anti_slop_iter_num >= 1 and current_slop_fixes_for_rotation_param:
                            additional_instructions = current_slop_fixes_for_rotation_param[anti_slop_iter_num % len(current_slop_fixes_for_rotation_param)]
                            log_message(f"Thread {thread_id}: AntiSlop iter {anti_slop_iter_num+1}. Adding fix: '{additional_instructions}'", "DEBUG")

                        log_message(f"Thread {thread_id}: Fixing anti-slop paragraph (Iter {anti_slop_iter_num+1}): '{phrase_to_fix}' in context...", "DEBUG")


                        rewritten_sentence, original_sentence = call_anti_slop_llm(
                            context_for_fixer, # Pass paragraph instead of single sentence
                            phrase_to_fix,
                            anti_slop_fixer_api_config_param,
                            sampler_settings_local,
                            thread_id,
                            additional_fix_instructions=additional_instructions,
                            current_max_attempts_param=current_max_attempts_for_slop_fixer_call,
                            master_duplication_enabled=master_duplication_enabled_local
                        )

                        if rewritten_sentence and original_sentence:
                            # NEW: Check for unbalanced quotes before applying the rewrite
                            has_incomplete_quote, _ = detection.is_incomplete_quote(rewritten_sentence)
                            if has_incomplete_quote:
                                log_message(f"Thread {thread_id}: Anti-slop fixer returned sentence with unbalanced quotes. Skipping replacement.", "WARNING")
                                anti_slop_fully_resolved = False
                                break
                            if original_sentence in current_answer_being_fixed:
                                if rewritten_sentence.strip() == original_sentence.strip():
                                    log_message(f"Thread {thread_id}: Anti-slop fixer returned same sentence for '{phrase_to_fix}'. Iter {anti_slop_iter_num+1}.", "DEBUG")
                                    anti_slop_fully_resolved = False
                                    break
                                else:
                                    current_answer_being_fixed = current_answer_being_fixed.replace(original_sentence, rewritten_sentence, 1)
                                    log_message(f"Thread {thread_id}: Sentence rewritten. New snippet: {current_answer_being_fixed[:70]}...", "DEBUG")
                            else:
                                log_message(f"Thread {thread_id}: Original sentence for anti-slop fix not found in current answer. Iter {anti_slop_iter_num+1}.", "WARNING")
                                anti_slop_fully_resolved = False
                                break
                        else:
                            log_message(f"Thread {thread_id}: Anti-slop LLM failed rewrite for '{phrase_to_fix}'. Aborting fix.", "WARNING")
                            anti_slop_fully_resolved = False
                            break

                    if not anti_slop_fully_resolved:
                        log_message(f"Thread {thread_id}: Anti-slop sentence fixing failed or max iters for API {api_slot_idx+1}.", "WARNING")
                    else:
                        answer = current_answer_being_fixed
                        issue_detected_this_main_api_call = False

                if issue_detected_this_main_api_call:
                    slop_check_after_sentence_fix, _ = detection.is_slop(answer, slop_phrases_local)
                    if slop_check_after_sentence_fix:
                        if fix_attempts_specific['slop_fallback'] < len(slop_fixes_fallback_local):
                            current_system_prompt_iter += f" {slop_fixes_fallback_local[fix_attempts_specific['slop_fallback']]}"
                            fix_attempts_specific['slop_fallback'] += 1
                            log_message(f"Thread {thread_id}: Applying fallback slop fix (system prompt) {fix_attempts_specific['slop_fallback']} for API {api_slot_idx+1}. Retrying API call.", "DEBUG")
                            continue
                        else:
                            log_message(f"Thread {thread_id}: Slop detected (API {api_slot_idx+1}), sentence fixer failed/skipped, and fallback system prompt fixes exhausted for attempt {attempt+1}.", "WARNING")
                            break
                    else:
                        if anti_slop_detected and not anti_slop_fully_resolved:
                            log_message(f"Thread {thread_id}: Anti-slop still unresolved after fixer attempt.", "WARNING")
                        else:
                            issue_detected_this_main_api_call = False
                incomplete_quote_detected, _ = detection.is_incomplete_quote(answer)
                if incomplete_quote_detected:
                    issue_detected_this_main_api_call = True
                    log_message(f"Thread {thread_id}: Incomplete quote detected (API Slot {api_slot_idx+1}). Retrying with fix instruction.", "DEBUG")

                    if fix_attempts_specific['incomplete_quote'] < 3:
                        current_system_prompt_iter += " CRITICAL INSTRUCTION: All dialogue quotes must be properly paired. Ensure every opening quote has a matching closing quote."
                        fix_attempts_specific['incomplete_quote'] += 1
                        continue
                    else:
                        log_message(f"Thread {thread_id}: Incomplete quote detected, max retries reached. Applying programmatic fix.", "WARNING")

                        # ROBUST PROGRAMMATIC FALLBACK: Auto-fix unbalanced quotes
                        # Uses the upgraded structural balancer instead of simple parity counting
                        answer = text_utils.normalize_quotes(answer.strip())
                        issue_detected_this_main_api_call = False  # Mark as resolved so it saves

            if not issue_detected_this_main_api_call:
                log_message(f"Thread {thread_id}: Successfully generated answer for attempt {attempt + 1} (API Slot {api_slot_idx+1}).", "INFO")
                return answer, issue_ever_detected_this_task, refusal_ever_detected_this_task

            break

        current_system_prompt_iter = base_system_prompt
        log_message(f"Thread {thread_id}: Main attempt {attempt + 1} failed for API {api_url_local} (Slot {api_slot_idx+1}). Resetting system prompt for next attempt if any.", "WARNING")
        if attempt < max_attempts_local - 1 :
            time.sleep(random.uniform(0.5, 1.5))

    log_message(f"Thread {thread_id}, API Slot {api_slot_idx+1}: All {max_attempts_local} attempts failed to generate a valid answer for the current turn. Returning None.", "ERROR")
    return None, issue_ever_detected_this_task, refusal_ever_detected_this_task

def write_conversation(output_file_path_base, # Not used directly, BASE_OUTPUT_FILE_PATH is used
                       conversation_history,
                       remove_reasoning_flag,
                       remove_em_dash_flag,
                       remove_asterisks_flag,
                       remove_asterisk_space_asterisk_flag,
                       remove_all_asterisks_flag,
                       ensure_space_after_line_break_flag,
                       remove_markdown_flag,
                       output_format_local,
                       task_id_for_output="unknown",
                       api_slot_idx_for_output_file=None, # For per-API files in duplication mode
                       is_duplication_turn=False, # Flag if this write is for a single turn in duplication mode
                       turn_number_for_duplication=0 # Turn number (1-based) for duplication mode output ID
                       ):
    """
    Writes a completed conversation (or a single turn in duplication mode) to the output JSONL file.
    - If api_slot_idx_for_output_file is provided AND master duplication is ON, writes to a per-API file.
    - Otherwise (non-duplication, or duplication off), writes to the main output.jsonl.
    """
    processed_conversation_turns = []
    for turn in conversation_history:
        role = turn.get("role")
        content = turn.get("content", "")
        # DEBUG: Log content before processing to check if issue starts at API response
        if role == "assistant" and (content.islower() and not any(c in content for c in '.!?')):
            log_message(f"DEBUG: API response already lowercase with no punctuation! Task ID: {task_id_for_output}", "WARNING")
            log_message(f"DEBUG: Raw content: {content[:200]}", "DEBUG")
        processed_content = text_utils.remove_reasoning_text(content) if remove_reasoning_flag else content
        processed_content = text_utils.remove_em_dash(processed_content) if remove_em_dash_flag else processed_content
        processed_content = text_utils.remove_excessive_asterisks(processed_content) if remove_asterisks_flag else processed_content
        processed_content = text_utils.remove_asterisk_space_asterisk(processed_content) if remove_asterisk_space_asterisk_flag else processed_content
        processed_content = text_utils.remove_all_asterisks(processed_content) if remove_all_asterisks_flag else processed_content
        processed_content = text_utils.ensure_space_after_line_break(processed_content) if ensure_space_after_line_break_flag else processed_content
        processed_content = text_utils.remove_markdown(processed_content) if remove_markdown_flag else processed_content
        processed_content = text_utils.normalize_quotes(processed_content)

        # Convert roles for ShareGPT format
        sg_role = "human" if role == "user" else "gpt" if role == "assistant" else role
        processed_conversation_turns.append({"from": sg_role, "value": processed_content})

    output_data_id = task_id_for_output
    if is_duplication_turn and app_state.master_duplication_enabled_var.get(): # Check global var for safety
        output_data_id = f"{task_id_for_output}_api{api_slot_idx_for_output_file}_turn{turn_number_for_duplication}"

    output_data = {
        "id": output_data_id,
        "conversations": processed_conversation_turns
    }

    use_db = global_config.get('database.enabled', False)

    if use_db and app_state.db_pool:
        try:
            conn = app_state.db_pool.getconn()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO generated_conversations (task_id, conversation_data, api_slot_idx)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (task_id) DO NOTHING
                """, (output_data_id, json.dumps(output_data), api_slot_idx_for_output_file))
                conn.commit()
            app_state.db_pool.putconn(conn)
            log_message(f"Saved task {task_id_for_output} to PostgreSQL.", "DEBUG")
        except Exception as e:
            log_message(f"DB insert failed for {task_id_for_output}: {e}", "ERROR")
            if 'conn' in locals(): app_state.db_pool.putconn(conn)

        # 🔑 CRITICAL: Exit function immediately after DB save.
        # This guarantees the file-writing code below NEVER runs when DB is enabled.
        return

    # --- FILE WRITING (Only reached if DB is DISABLED or pool failed to init) ---
    actual_output_file_path = ""
    if api_slot_idx_for_output_file is not None and app_state.master_duplication_enabled_var.get():
        actual_output_file_path = f"{BASE_OUTPUT_FILE_PATH}_api_slot_{api_slot_idx_for_output_file}.jsonl"
    else:
        actual_output_file_path = BASE_OUTPUT_FILE_PATH + ".jsonl"

    try:
        with open(actual_output_file_path, 'a', encoding='utf-8') as file:
            file.write(json.dumps(output_data) + '\n')
        log_message(f"Successfully wrote task {task_id_for_output} to {actual_output_file_path}", "DEBUG")
    except PermissionError as e:
        log_message(f"Permission error writing to {actual_output_file_path}: {e}", "ERROR")
    except OSError as e:
        log_message(f"OS error writing to {actual_output_file_path}: {e}", "ERROR")
    except Exception as e:
        log_message(f"Unexpected error writing to {actual_output_file_path}: {e}", "ERROR")
        import traceback
        log_message(traceback.format_exc(), "ERROR")

def export_db_to_jsonl(output_path):
    if not app_state.db_pool:
        messagebox.showerror("Export Error", "Database pool not initialized.")
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

def draw_issue_graph(canvas_widget, height=400):
    """Draws a modern, detailed time-series graph showing issue counts over the last 60 minutes."""
    canvas_widget.delete("all")

    # Modern dark theme setup
    fig = Figure(figsize=(12, 4.5), dpi=120, facecolor='#1e1e24')
    fig.patch.set_facecolor('#1e1e24')
    ax = fig.add_subplot(111)
    ax.set_facecolor('#2a2a35')

    # Time bins setup
    now = time.time()
    sixty_minutes_ago = now - 3600
    num_bins = 6
    bin_size = 3600 / num_bins

    # Initialize counts
    counts = {'refusals': [0]*num_bins, 'user_speaking': [0]*num_bins,
              'slop': [0]*num_bins, 'errors': [0]*num_bins, 'anti_slop': [0]*num_bins}

    with app_state.issue_timestamps_lock:
        for key in counts.keys():
            for ts in app_state.issue_timestamps.get(key, []):
                if sixty_minutes_ago <= ts <= now:
                    idx = min(int((ts - sixty_minutes_ago) / bin_size), num_bins - 1)
                    counts[key][idx] += 1

    # X-axis labels (stacked for readability)
    x_labels = []
    for i in range(num_bins):
        start = time.strftime('%H:%M', time.localtime(sixty_minutes_ago + i * bin_size))
        end = time.strftime('%H:%M', time.localtime(sixty_minutes_ago + (i + 1) * bin_size))
        x_labels.append(f"{start}\n{end}")

    x = range(num_bins)
    width = 0.15
    offsets = [-2*width, -width, 0, width, 2*width]

    # Cohesive, accessible color palette
    colors = {'refusals': '#ff4d6d', 'user_speaking': '#4dabf7', 'slop': '#9775fa',
              'anti_slop': '#ffd43b', 'errors': '#fc8181'}
    labels = {'refusals': 'Refusals', 'user_speaking': 'User Speak', 'slop': 'Slop',
              'anti_slop': 'Anti-Slop', 'errors': 'Errors'}

    # Plot bars & add value labels
    for i, key in enumerate(counts.keys()):
        bar = ax.bar([j + offsets[i] for j in x], counts[key], width, label=labels[key],
                     color=colors[key], edgecolor='#1e1e24', linewidth=0.8, alpha=0.9)
        for rect in bar:
            h = rect.get_height()
            if h > 0:
                ax.annotate(f'{int(h)}', xy=(rect.get_x() + rect.get_width()/2, h),
                            xytext=(0, 4), textcoords="offset points",
                            ha='center', va='bottom', fontsize=8, color='#e0e0e0', fontweight='bold')

    # Styling & Layout
    max_val = max(max(c) for c in counts.values()) if any(any(c) for c in counts.values()) else 5
    ax.set_ylim(0, max_val + 2)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    ax.set_xlabel('Time Window (Last 60 Minutes)', fontsize=12, fontweight='bold', color='#e0e0e0', labelpad=10)
    ax.set_ylabel('Issue Count', fontsize=12, fontweight='bold', color='#e0e0e0', labelpad=10)
    ax.set_title('Issue Detection Dashboard', fontsize=15, fontweight='bold', color='#ffffff', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=0, ha='center', fontsize=10, color='#c0c0c0')

    ax.grid(axis='y', linestyle='--', alpha=0.25, color='#555555')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color('#555555')
    ax.spines['left'].set_color('#555555')
    ax.tick_params(axis='both', colors='#c0c0c0')

    ax.legend(loc='upper right', fontsize=10, framealpha=0.85, facecolor='#2a2a35',
              edgecolor='#444444', labelcolor='#e0e0e0')
    fig.tight_layout()

    # Embed in Tkinter
    canvas = FigureCanvasTkAgg(fig, master=canvas_widget)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    canvas_widget.graph_canvas = canvas
    canvas_widget.graph_fig = fig
    canvas_widget.graph_ax = ax

def update_issue_graph(canvas_widget):
    """Updates an existing issue graph with new data without recreating the figure."""
    if not hasattr(canvas_widget, 'graph_canvas'):
        draw_issue_graph(canvas_widget)
        return

    fig = canvas_widget.graph_fig
    ax = canvas_widget.graph_ax
    ax.clear()

    # Reuse the exact same drawing logic from draw_issue_graph
    # (We extract it into a shared helper in production, but for drop-in replacement:)
    fig.patch.set_facecolor('#1e1e24')
    ax.set_facecolor('#2a2a35')

    now = time.time()
    sixty_minutes_ago = now - 3600
    num_bins = 6
    bin_size = 3600 / num_bins
    counts = {'refusals': [0]*num_bins, 'user_speaking': [0]*num_bins,
              'slop': [0]*num_bins, 'errors': [0]*num_bins, 'anti_slop': [0]*num_bins}

    with app_state.issue_timestamps_lock:
        for key in counts.keys():
            for ts in app_state.issue_timestamps.get(key, []):
                if sixty_minutes_ago <= ts <= now:
                    idx = min(int((ts - sixty_minutes_ago) / bin_size), num_bins - 1)
                    counts[key][idx] += 1

    x_labels = []
    for i in range(num_bins):
        start = time.strftime('%H:%M', time.localtime(sixty_minutes_ago + i * bin_size))
        end = time.strftime('%H:%M', time.localtime(sixty_minutes_ago + (i + 1) * bin_size))
        x_labels.append(f"{start}\n{end}")

    x = range(num_bins)
    width = 0.15
    offsets = [-2*width, -width, 0, width, 2*width]
    colors = {'refusals': '#ff4d6d', 'user_speaking': '#4dabf7', 'slop': '#9775fa',
              'anti_slop': '#ffd43b', 'errors': '#fc8181'}
    labels = {'refusals': 'Refusals', 'user_speaking': 'User Speak', 'slop': 'Slop',
              'anti_slop': 'Anti-Slop', 'errors': 'Errors'}

    for i, key in enumerate(counts.keys()):
        bar = ax.bar([j + offsets[i] for j in x], counts[key], width, label=labels[key],
                     color=colors[key], edgecolor='#1e1e24', linewidth=0.8, alpha=0.9)
        for rect in bar:
            h = rect.get_height()
            if h > 0:
                ax.annotate(f'{int(h)}', xy=(rect.get_x() + rect.get_width()/2, h),
                            xytext=(0, 4), textcoords="offset points",
                            ha='center', va='bottom', fontsize=8, color='#e0e0e0', fontweight='bold')

    max_val = max(max(c) for c in counts.values()) if any(any(c) for c in counts.values()) else 5
    ax.set_ylim(0, max_val + 2)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    ax.set_xlabel('Time Window (Last 60 Minutes)', fontsize=12, fontweight='bold', color='#e0e0e0', labelpad=10)
    ax.set_ylabel('Issue Count', fontsize=12, fontweight='bold', color='#e0e0e0', labelpad=10)
    ax.set_title('Issue Detection Dashboard', fontsize=15, fontweight='bold', color='#ffffff', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=0, ha='center', fontsize=10, color='#c0c0c0')

    ax.grid(axis='y', linestyle='--', alpha=0.25, color='#555555')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color('#555555')
    ax.spines['left'].set_color('#555555')
    ax.tick_params(axis='both', colors='#c0c0c0')

    ax.legend(loc='upper right', fontsize=10, framealpha=0.85, facecolor='#2a2a35',
              edgecolor='#444444', labelcolor='#e0e0e0')
    fig.tight_layout()
    canvas_widget.graph_canvas.draw()

def update_rate_limit_status():
    """Updates the rate limit status labels with progress-style visualization."""
    if not app_state.rate_limit_labels:
        return

    # Color constants matching the dark theme
    ACCENT_GREEN = "#51cf66"
    ACCENT_ORANGE = "#ff922b"
    ACCENT_RED = "#ff6b6b"

    with global_rate_limiter.lock:
        for slot_idx in range(6):
            try:
                limit = global_rate_limiter.rates_per_slot[slot_idx]
                used = len(global_rate_limiter.requests_per_slot[slot_idx])
                remaining = max(0, limit - used)
                usage_percent = (used / limit) * 100 if limit > 0 else 0

                if slot_idx in app_state.rate_limit_labels:
                    if usage_percent > 90:
                        icon = "🔴"
                        color = ACCENT_RED
                    elif usage_percent > 70:
                        icon = "🟡"
                        color = ACCENT_ORANGE
                    elif usage_percent > 40:
                        icon = "🟠"
                        color = ACCENT_ORANGE
                    else:
                        icon = "🟢"
                        color = ACCENT_GREEN

                    app_state.rate_limit_labels[slot_idx].config(
                        text=f"{icon} API {slot_idx+1}: {remaining}/{limit} ({usage_percent:.0f}% used)",
                        foreground=color
                    )
            except Exception as e:
                log_message(f"Error updating rate limit status for slot {slot_idx}: {e}", "ERROR")

# --- Tkinter UI Update and Control Functions ---
def update_dashboard():
    """Updates the dashboard labels and text areas with current statistics and recent issues."""

    # NEW: Update rate limit status
    update_rate_limit_status()

    total_attempts_for_calc = app_state.total_attempts_global if app_state.total_attempts_global > 0 else 1 
    
    refusal_percent = (app_state.refusal_count_total / total_attempts_for_calc) * 100
    user_speaking_percent = (app_state.user_speaking_count_total / total_attempts_for_calc) * 100
    slop_percent = (app_state.slop_count_total / total_attempts_for_calc) * 100 
    error_percent = (app_state.error_count_total / total_attempts_for_calc) * 100

    # Calculate cost (ensure you have the price per 1k tokens from config)
    price_per_token = global_config.get('api.pricing.cost_per_1k_tokens', 0) / 1000
    estimated_cost = (app_state.total_input_tokens + app_state.total_output_tokens) * price_per_token

    budget_limit = global_config.get('api.pricing.budget_limit', 0.0)
    if budget_label.winfo_exists():
        if budget_limit > 0:
            budget_label.config(text=f"Budget: ${estimated_cost:.2f} / ${budget_limit:.2f}",
                                foreground="red" if estimated_cost >= budget_limit else "lightgray")
        else:
            budget_label.config(text="Budget: Disabled", foreground="lightgray")

    if api_handler.valkey_client:
        try:
            api_handler.valkey_client.set("stats:refusal_count", app_state.refusal_count_total)
            api_handler.valkey_client.set("stats:total_attempts", app_state.total_attempts_global)
        except Exception as e:
            # Log the error but don't stop the dashboard from updating
            log_message(f"Error updating stats in Valkey: {e}", "WARNING")

    if hasattr(refusal_percent_label, 'winfo_exists') and refusal_percent_label.winfo_exists():
        refusal_percent_label.config(text=f"{app_state.refusal_count_total} Refusals encountered ({refusal_percent:.1f}%)")
        user_speaking_label.config(text=f"{app_state.user_speaking_count_total} User Speak instances ({user_speaking_percent:.1f}%)")
        slop_label.config(text=f"{app_state.slop_count_total} Slop instances detected ({slop_percent:.1f}%)")
        error_percent_label.config(text=f"{app_state.error_count_total} Total Errors logged ({error_percent:.1f}%)")

        # NEW: Update token and cost labels (you need to create these labels in the UI first)
        token_label.config(text=f"Tokens: {app_state.total_input_tokens + app_state.total_output_tokens}")
        cost_label.config(text=f"Est. Cost: ${estimated_cost:.4f}")
        # NEW: Update API response time labels
        for slot_idx in range(6):
            slot_label_name = f"api_response_time_label_{slot_idx+1}"
            if hasattr(globals().get(slot_label_name), 'winfo_exists') and globals().get(slot_label_name).winfo_exists():
                with api_response_times_lock:
                    response_times = api_response_times_per_slot[slot_idx].copy()

                if response_times:
                    avg_response_time = sum(response_times) / len(response_times)
                    min_response_time = min(response_times)
                    max_response_time = max(response_times)
                    globals()[slot_label_name].config(
                        text=f"API {slot_idx+1}: {avg_response_time:.2f}s (min: {min_response_time:.2f}s, max: {max_response_time:.2f}s, samples: {len(response_times)})"
                    )
                else:
                    globals()[slot_label_name].config(text=f"API {slot_idx+1}: No data yet")

    def update_scrolled_text_widget_content(text_widget, recent_items_list, tag_name="highlight", is_total_tab_list=False):
        if not (hasattr(text_widget, 'winfo_exists') and text_widget.winfo_exists()): return

        text_widget.config(state=tk.NORMAL)
        text_widget.delete(1.0, tk.END)
        for item_idx, item in enumerate(recent_items_list):
            phrase_to_highlight = None
            sentence_context = None
            api_origin_idx = -1 

            if is_total_tab_list: 
                if isinstance(item, tuple) and len(item) == 3 and isinstance(item[0], str) and isinstance(item[1], str) and isinstance(item[2], int): 
                    phrase_to_highlight, sentence_context, api_origin_idx = item
                elif isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str) and isinstance(item[1], int): 
                    sentence_context = item[0] 
                    api_origin_idx = item[1]
                else: 
                    sentence_context = str(item)
            else: 
                if isinstance(item, tuple) and len(item) == 2: 
                    phrase_to_highlight, sentence_context = item
                else: 
                    sentence_context = str(item)
            
            prefix = f"- "
            if api_origin_idx != -1: 
                prefix += f"[API {api_origin_idx+1}] "
            
            if phrase_to_highlight and sentence_context: 
                start_idx = -1; end_idx = -1
                try: 
                    match = re.search(r'\b' + re.escape(phrase_to_highlight) + r'\b', sentence_context, re.IGNORECASE)
                    if match:
                        start_idx = match.start(); end_idx = match.end()
                except re.error: 
                    start_idx = sentence_context.lower().find(phrase_to_highlight.lower())
                    if start_idx != -1: end_idx = start_idx + len(phrase_to_highlight)
                
                text_widget.insert(tk.END, prefix)
                if start_idx != -1 and end_idx != -1: 
                    text_widget.insert(tk.END, sentence_context[:start_idx])
                    text_widget.insert(tk.END, sentence_context[start_idx:end_idx], (tag_name, f"item_{item_idx}"))
                    text_widget.insert(tk.END, f"{sentence_context[end_idx:]}\n")
                else: 
                    text_widget.insert(tk.END, f"{sentence_context} (Highlight failed for '{phrase_to_highlight}')\n")
            elif sentence_context: 
                text_widget.insert(tk.END, f"{prefix}{sentence_context}\n")
            
        text_widget.config(state=tk.DISABLED)
        text_widget.yview(tk.END)

    update_scrolled_text_widget_content(dashboard_notebook.tabs_widgets["Totals"]["refusals"], app_state.recent_refusals_total, "highlight_refusal", is_total_tab_list=True)
    update_scrolled_text_widget_content(dashboard_notebook.tabs_widgets["Totals"]["user_speak"], app_state.recent_user_speaking_total, "highlight_user_speak", is_total_tab_list=True)
    update_scrolled_text_widget_content(dashboard_notebook.tabs_widgets["Totals"]["slop"], app_state.recent_slop_total, "highlight_slop", is_total_tab_list=True)
    update_scrolled_text_widget_content(dashboard_notebook.tabs_widgets["Totals"]["anti_slop"], app_state.recent_anti_slop_total, "highlight_anti_slop", is_total_tab_list=True)

    for i in range(6):
        api_tab_name = f"API {i+1}"
        if api_tab_name in dashboard_notebook.tabs_widgets:
            update_scrolled_text_widget_content(dashboard_notebook.tabs_widgets[api_tab_name]["refusals"], app_state.recent_refusals_per_api.get(i,[]), "highlight_refusal")
            update_scrolled_text_widget_content(dashboard_notebook.tabs_widgets[api_tab_name]["user_speak"], app_state.recent_user_speaking_per_api.get(i,[]), "highlight_user_speak")
            update_scrolled_text_widget_content(dashboard_notebook.tabs_widgets[api_tab_name]["slop"], app_state.recent_slop_per_api.get(i,[]), "highlight_slop")
            update_scrolled_text_widget_content(dashboard_notebook.tabs_widgets[api_tab_name]["anti_slop"], app_state.recent_anti_slop_per_api.get(i,[]), "highlight_anti_slop")
            update_scrolled_text_widget_content(dashboard_notebook.tabs_widgets[api_tab_name]["errors"], app_state.recent_errors_per_api.get(i,[]), "highlight_error")

    # NEW: Update the graph on the Totals tab
    if "Totals" in dashboard_notebook.tabs_widgets:
        graph_canvas_widget = dashboard_notebook.tabs_widgets["Totals"].get("graph_canvas")
        if graph_canvas_widget and hasattr(graph_canvas_widget, 'winfo_exists') and graph_canvas_widget.winfo_exists():
            try:
                update_issue_graph(graph_canvas_widget)
            except Exception as e_graph:
                log_message(f"Error updating issue graph: {e_graph}", "ERROR")

def update_database_status():
    """Updates the PostgreSQL and Valkey connection status icons and labels."""

    if not app_state.root.winfo_exists():
        return

    # --- PostgreSQL Status ---
    postgres_connected = False
    postgres_active = False

    if app_state.db_pool:
        try:
            conn = app_state.db_pool.getconn()
            if conn:
                postgres_connected = True
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
                postgres_active = True
                app_state.db_pool.putconn(conn)
        except Exception as e:
            postgres_connected = False
            postgres_active = False
            log_message(f"PostgreSQL status check failed: {e}", "DEBUG")

    # Update PostgreSQL icon and label
    if 'postgres_icon' in app_state.db_status_widgets and app_state.db_status_widgets['postgres_icon'].winfo_exists():
        if postgres_connected and postgres_active:
            app_state.db_status_widgets['postgres_icon'].config(text="✅", foreground="green")
            app_state.db_status_widgets['postgres_status'].config(text="PostgreSQL: Connected & Active", foreground="green")
        elif postgres_connected:
            app_state.db_status_widgets['postgres_icon'].config(text="⚠️", foreground="orange")
            app_state.db_status_widgets['postgres_status'].config(text="PostgreSQL: Connected (Inactive)", foreground="orange")
        else:
            app_state.db_status_widgets['postgres_icon'].config(text="❌", foreground="gray")
            app_state.db_status_widgets['postgres_status'].config(text="PostgreSQL: Disconnected", foreground="gray")

    # --- Valkey/Redis Status ---
    valkey_connected = False
    valkey_active = False

    # Check if valkey_client exists in api_handler module
    if hasattr(api_handler, 'valkey_client') and api_handler.valkey_client is not None:
        try:
            ping_result = api_handler.valkey_client.ping()
            log_message(f"Valkey status check PING result: {ping_result}", "DEBUG")
            if ping_result:
                valkey_connected = True
                valkey_active = True
        except Exception as e:
            valkey_connected = False
            valkey_active = False
            log_message(f"Valkey status check failed: {e}", "DEBUG")
    else:
        log_message(f"Valkey client not available: hasattr={hasattr(api_handler, 'valkey_client')}, client={api_handler.valkey_client if hasattr(api_handler, 'valkey_client') else 'N/A'}", "DEBUG")

    # Update Valkey icon and label
    if 'valkey_icon' in app_state.db_status_widgets and app_state.db_status_widgets['valkey_icon'].winfo_exists():
        if valkey_connected and valkey_active:
            app_state.db_status_widgets['valkey_icon'].config(text="✅", foreground="green")
            app_state.db_status_widgets['valkey_status'].config(text="Valkey: Connected & Active", foreground="green")
        elif valkey_connected:
            app_state.db_status_widgets['valkey_icon'].config(text="⚠️", foreground="orange")
            app_state.db_status_widgets['valkey_status'].config(text="Valkey: Connected (Inactive)", foreground="orange")
        else:
            app_state.db_status_widgets['valkey_icon'].config(text="❌", foreground="gray")
            app_state.db_status_widgets['valkey_status'].config(text="Valkey: Disconnected", foreground="gray")

def update_live_prompt_preview(messages_list):
    """Thread-safe function to update the Live Prompt Preview widget from worker threads."""
    # Check if paused - don't update preview if UI updates are paused
    if dashboard_pause_var.get():
        return

    if not app_state.root.winfo_exists() or prompt_preview_text is None:
        return

    # Format payload for clean JSON readability
    preview_json = json.dumps({"messages": messages_list}, indent=2, ensure_ascii=False)

    def _apply_update():
        if not app_state.root.winfo_exists() or prompt_preview_text is None:
            return
        # Double-check pause state in main thread callback
        if dashboard_pause_var.get():
            return
        prompt_preview_text.config(state=tk.NORMAL)
        prompt_preview_text.delete(1.0, tk.END)
        prompt_preview_text.insert(tk.END, preview_json)
        prompt_preview_text.config(state=tk.DISABLED)
        prompt_preview_text.see(tk.END)  # Auto-scroll to bottom

    # Schedule UI update on the main Tkinter thread
    app_state.root.after(0, _apply_update)

def start_processing():
    """Initiates the data generation process based on current configurations."""
    global loaded_processed_tasks_snapshot

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
        messagebox.showerror("Config Error", "API configuration is missing or malformed in config.yml.")
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
            'rate_limit_rpm': api_conf_yml.get('rate_limit_rpm', 60)
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

    master_duplication_enabled = app_state.master_duplication_enabled_var.get() 

    if not master_duplication_enabled and not active_enabled_api_configs_for_worker_list:
        messagebox.showerror("Config Error", "Non-Duplication mode is selected, but no APIs (Slots 1-4) are enabled or configured (URL needed).")
        log_message("Non-Duplication: No APIs 1-4 enabled/configured for work.", "ERROR"); return
    
    slop_fixer_api_config_runtime = all_api_configs_runtime[4]
    # NEW: Define Anti-Slop Fixer API config (Slot 6, index 5)
    anti_slop_fixer_api_config_runtime = all_api_configs_runtime[5]
    # Define the param variable to match the runtime one
    anti_slop_fixer_api_config_param = anti_slop_fixer_api_config_runtime
    if slop_fixer_api_config_runtime.get('url') and (not slop_fixer_api_config_runtime.get('model') or not slop_fixer_api_config_runtime.get('key')):
        log_message("Warning: Slop Fixer API URL (API Slot 5) is set, but Model or Key is missing. Sentence-level slop fixing will be disabled.", "WARNING")
        slop_fixer_api_config_runtime['url'] = None 
    elif not slop_fixer_api_config_runtime.get('url'):
        log_message("Info: Slop Fixer API (API Slot 5) not configured. Fallback system prompt slop handling will be used if slop is detected.", "INFO")

    log_message(f"Master Duplication Mode (from UI Var): {master_duplication_enabled}", "INFO")
    if not master_duplication_enabled:
        log_message(f"Non-Duplication Mode: {len(active_enabled_api_configs_for_worker_list)} active APIs for collaborative work.", "INFO")
    else: 
        num_enabled_dup_apis = sum(1 for idx, conf in enumerate(all_api_configs_runtime) if idx < 4 and conf.get('enabled') and conf.get('url'))
        if num_enabled_dup_apis == 0:
            messagebox.showerror("Config Error", "Master Duplication Mode is ON, but no APIs (Slots 1-4) are enabled or fully configured (URL needed).")
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
        messagebox.showerror("Config Error", "Invalid number of threads specified in UI.")
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
    character_config = global_config.get('prompts.character', {})
    enable_character_engine_local = character_config.get('enabled', True)
    enable_class_selection_local = character_config.get('class_enabled', False)

    num_characters_local = character_config.get('num_characters', 1)  # Default to 1 for backward compatibility
    if num_characters_local < 1:
        num_characters_local = 1

    # Load the new list-of-dicts format
    character_list = character_config.get('characters', [])

    # Fallback for old config format (separate lists)
    if not character_list:
        old_names = character_config.get('name', [])
        old_races = character_config.get('race', [])
        old_jobs = character_config.get('job', [])
        old_clothing = character_config.get('clothing', [])
        old_appearance = character_config.get('appearance', [])
        old_backstory = character_config.get('backstory', [])
        old_personality = character_config.get('personality', [])  # NEW
        old_setting = character_config.get('setting', [])
        old_class = character_config.get('class', [])

        if any([old_names, old_races, old_jobs, old_clothing, old_appearance, old_backstory, old_personality, old_setting, old_class]):  # NEW
            max_len = max(
                len(old_names), len(old_races), len(old_jobs), len(old_clothing),
                len(old_appearance), len(old_backstory), len(old_personality),  # NEW
                len(old_setting), len(old_class)
            )
            for i in range(max_len):
                character_list.append({
                    'name': old_names[i] if i < len(old_names) else '',
                    'race': old_races[i] if i < len(old_races) else '',
                    'job': old_jobs[i] if i < len(old_jobs) else '',
                    'clothing': old_clothing[i] if i < len(old_clothing) else '',
                    'appearance': old_appearance[i] if i < len(old_appearance) else '',
                    'backstory': old_backstory[i] if i < len(old_backstory) else '',
                    'personality': old_personality[i] if i < len(old_personality) else '',  # NEW
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

    if enable_emotional_states and not emotional_states_list:
        log_message("Warning: Emotional states enabled but no states defined. Disabling emotional states.", "WARNING")
        enable_emotional_states = False
        emotional_states_list = []

    log_message(f"Emotional states enabled: {enable_emotional_states}, States: {emotional_states_list}", "INFO")

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
    
    current_slop_phrases = global_config.get('detection.slop.phrases', [])
    current_jailbreaks = global_config.get('detection.refusal.fixes', []) 
    current_slop_fixes_fallback = global_config.get('detection.slop.fixes', []) 
    current_slop_fixes_for_rotation = global_config.get('detection.slop.fixes', []) 

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

    # --- Define Number of Tasks to Generate (New) ---
    NUM_RANDOM_CHUNKS = global_config.get('generation.num_random_chunks', 12000) # Change this number to generate more or fewer tasks per run

    # --- Populate Task Queue ---
    if current_use_questions_file:
        try:
            app_state.questions_list = read_txt(QUESTIONS_FILE_PATH)
            if not app_state.questions_list:
                messagebox.showwarning("Input Error", f"{QUESTIONS_FILE_PATH} is enabled but empty/not found.")
                log_message(f"{QUESTIONS_FILE_PATH} enabled but empty/not found.", "WARNING")
                app_state.processing_active = False; start_button.config(state=tk.NORMAL); return
        except Exception as e:
            messagebox.showerror("File Error", f"Error reading {QUESTIONS_FILE_PATH}: {e}")
            log_message(f"Error reading {QUESTIONS_FILE_PATH}: {e}", "ERROR")
            app_state.processing_active = False; start_button.config(state=tk.NORMAL); return
    else: 
        app_state.questions_list = [] 

    input_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.txt') and f != os.path.basename(QUESTIONS_FILE_PATH)]
    if not current_use_questions_file and not input_files:
        messagebox.showwarning("Input Error", "No input .txt files found in 'input' folder (and not using questions.txt).")
        log_message("No input .txt files found for chunking.", "WARNING")
        app_state.processing_active = False; start_button.config(state=tk.NORMAL); return

    total_tasks_to_queue = 0
    if current_use_questions_file:
        for i, q_text in enumerate(app_state.questions_list):
            task_id = f"q_{i}" 
            if task_id not in app_state.completed_task_ids: 
                app_state.task_queue.put((task_id, os.path.basename(QUESTIONS_FILE_PATH), i, q_text))
                total_tasks_to_queue += 1
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
                task_id = f"{random_file_name}_chunk_at_{random_start_index}"

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
                    app_state.task_queue.put((task_id, random_file_name, random_start_index, current_subject_content, current_context_text))
                    tasks_queued_count += 1
                    total_tasks_to_queue += 1 # Update the global counter used for progress bars
                else:
                    # Task already exists, just count it for the progress bar total
                    total_tasks_to_queue += 1

            except Exception as e:
                log_message(f"Error processing random chunk from file {random_file_name}: {e}", "ERROR")
                continue

        log_message(f"Attempted to queue {NUM_RANDOM_CHUNKS} random tasks. Successfully queued {tasks_queued_count} new unique tasks.", "INFO")

    if total_tasks_to_queue == 0 and not app_state.completed_task_ids: 
        messagebox.showwarning("Processing Error", "No tasks to process (all inputs might be empty, or no new tasks found).")
        log_message("No tasks to queue.", "WARNING"); app_state.processing_active = False; start_button.config(state=tk.NORMAL); return
    elif total_tasks_to_queue == 0 and app_state.completed_task_ids: # All tasks were already done
        messagebox.showinfo("Processing Complete", "All tasks were already completed in a previous session.")
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
                api_name_label.pack(pady=(5,0), anchor='w')
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
            character_list,
            enable_emotional_states,
            emotional_states_list,
            num_characters_local,
            no_user_impersonation_var.get(),
            current_api_request_timeout
        ), name=f"Worker-{i}")
        app_state.threads.append(thread)
        thread.start()

    log_message(f"Started {total_threads} worker threads.", "INFO")

    # --- GUI Progress Update Loop ---
    def update_gui_progress():
        if dashboard_pause_var.get():
            app_state.root.after(1000, update_gui_progress) # Check again in 1s
            return
        if app_state.processing_active and not app_state.stop_processing: 
            check_budget_limit()
            try:
                process = psutil.Process()
                open_files = process.open_files()
                if len(open_files) > 300:  # Threshold
                    log_message(f"Warning: {len(open_files)} open files", "WARNING")
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
                                # Animated progress bar: update style and percentage label
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
                        # Animated progress bar: update style and percentage label
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
                
                update_dashboard() # Refresh dashboard stats
                update_database_status()
                if app_state.root.winfo_exists():
                    # ADAPTIVE UPDATE FREQUENCY
                    is_active = app_state.processing_active and not app_state.stop_processing and not app_state.pause_processing
                    has_work = app_state.task_queue and hasattr(app_state.task_queue, 'qsize') and app_state.task_queue.qsize() > 0

                    delay = 500 if (is_active and has_work) else 2000
                    app_state.root.after(delay, update_gui_progress)
            except Exception as e_gui: # Catch errors during GUI update
                log_message(f"GUI update error: {str(e_gui)}", "ERROR")
                if app_state.processing_active and not app_state.stop_processing and app_state.root.winfo_exists(): 
                    app_state.root.after(1000, update_gui_progress) 
        else: # Processing stopped or completed
            start_button.config(state=tk.NORMAL)
            pause_button.config(state=tk.DISABLED); pause_button.config(text="Pause")
            stop_clear_button.config(state=tk.NORMAL)
            log_message("Processing stopped/completed. GUI updates halted.", "INFO")
            update_dashboard() # Final dashboard update
            master_duplication_final_check = app_state.master_duplication_enabled_var.get()
            if hasattr(app_state.task_queue, 'total_tasks_for_progress') and app_state.task_queue.total_tasks_for_progress > 0:
                if master_duplication_final_check and hasattr(app_state.task_queue, 'api_widgets'):
                    for api_idx, widgets in app_state.task_queue.api_widgets.items():
                        if widgets['bar'].winfo_exists():
                            with app_state.task_queue.processed_tasks_lock:
                                processed_api_turns = app_state.task_queue.api_processed_tasks.get(api_idx,0)
                            if processed_api_turns >= app_state.task_queue.total_tasks_for_progress:
                                widgets['bar']['value'] = 100
                                # Animated: set to complete style
                                update_progress_bar_style(widgets['bar'], 100)
                                if 'percent_label' in widgets and widgets['percent_label'].winfo_exists():
                                    widgets['percent_label'].config(text="100%", foreground='#51cf66')
                                widgets['time_label'].config(text="Time Rem: Done!")
                elif hasattr(app_state.task_queue, 'overall_progress_bar') and app_state.task_queue.overall_progress_bar.winfo_exists():
                    with app_state.task_queue.processed_tasks_lock:
                        processed_overall_turns = app_state.task_queue.processed_tasks
                    if processed_overall_turns >= app_state.task_queue.total_tasks_for_progress:
                        app_state.task_queue.overall_progress_bar['value'] = 100
                        # Animated: set to complete style
                        update_progress_bar_style(app_state.task_queue.overall_progress_bar, 100)
                        if hasattr(app_state.task_queue, 'overall_percent_label') and app_state.task_queue.overall_percent_label.winfo_exists():
                            app_state.task_queue.overall_percent_label.config(text="100%", foreground='#51cf66')
                        app_state.task_queue.overall_time_label.config(text="Time Remaining: Done!")
            save_generation_state() # Save final state

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
app_state.root.minsize(1100, 700)  # Prevents layout breakage on resize
app_state.root.grid_columnconfigure(0, weight=1)
app_state.root.grid_rowconfigure(0, weight=1)
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

# --- Animated Progress Bar Styling ---
def configure_animated_progress_styles(style_obj):
    """Configure custom progress bar styles with color-coded stages and animation support."""
    # Trough (background track) - dark theme consistent
    trough_color = '#2a2a35'
    border_color = '#1e1e24'

    # Stage 1: Low progress (0-25%) - Cool Blue
    style_obj.configure("Low.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#3498db',
                        darkcolor='#2980b9',
                        lightcolor='#5dade2',
                        bordercolor=border_color,
                        thickness=22)

    # Stage 2: Medium progress (25-50%) - Cyan / Teal
    style_obj.configure("Medium.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#17a2b8',
                        darkcolor='#138496',
                        lightcolor='#4dc8e8',
                        bordercolor=border_color,
                        thickness=22)

    # Stage 3: Progressing (50-75%) - Green
    style_obj.configure("Progressing.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#28a745',
                        darkcolor='#1e7e34',
                        lightcolor='#5cb85c',
                        bordercolor=border_color,
                        thickness=22)

    # Stage 4: High progress (75-90%) - Amber / Yellow
    style_obj.configure("High.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#ffc107',
                        darkcolor='#e0a800',
                        lightcolor='#ffd54f',
                        bordercolor=border_color,
                        thickness=22)

    # Stage 5: Complete (90-100%) - Bright Green with pulse
    style_obj.configure("Complete.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#51cf66',
                        darkcolor='#40c057',
                        lightcolor='#69db7c',
                        bordercolor=border_color,
                        thickness=22)

    # Pulse state (briefly flashes brighter when milestones are hit)
    style_obj.configure("Pulse.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#74c0fc',
                        darkcolor='#4dabf7',
                        lightcolor='#a5d8ff',
                        bordercolor=border_color,
                        thickness=22)

    # Error / Stalled state - Red
    style_obj.configure("Error.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#ff6b6b',
                        darkcolor='#fa5252',
                        lightcolor='#ffa8a8',
                        bordercolor=border_color,
                        thickness=22)

    log_message("Animated progress bar styles configured.", "DEBUG")


def get_progress_style_name(progress_percent):
    """Returns the appropriate style name based on progress percentage."""
    if progress_percent < 0:
        return "Low.Horizontal.TProgressbar"
    elif progress_percent < 25:
        return "Low.Horizontal.TProgressbar"
    elif progress_percent < 50:
        return "Medium.Horizontal.TProgressbar"
    elif progress_percent < 75:
        return "Progressing.Horizontal.TProgressbar"
    elif progress_percent < 90:
        return "High.Horizontal.TProgressbar"
    else:
        return "Complete.Horizontal.TProgressbar"


def update_progress_bar_style(bar, progress_percent, error_state=False):
    """Updates the style of a progress bar based on its current progress percentage."""
    if not bar or not hasattr(bar, 'winfo_exists') or not bar.winfo_exists():
        return
    if error_state:
        style_name = "Error.Horizontal.TProgressbar"
    else:
        style_name = get_progress_style_name(progress_percent)
    try:
        bar.configure(style=style_name)
    except tk.TclError:
        pass  # Style might not exist yet during early init


# Track the previous progress percentage for each bar to detect milestone changes


def pulse_progress_bar(bar, bar_key, root_window):
    """Briefly flashes a progress bar to a brighter color when a milestone is crossed,
    then reverts it back after a short delay."""
    if not bar or not hasattr(bar, 'winfo_exists') or not bar.winfo_exists():
        return

    current_value = bar['value']
    previous_value = app_state._previous_progress_values.get(bar_key, 0)

    # Define milestones at which to pulse (every 25%)
    milestones = [25, 50, 75, 90, 100]

    for milestone in milestones:
        if previous_value < milestone <= current_value:
            # Milestone crossed! Apply pulse
            try:
                bar.configure(style="Pulse.Horizontal.TProgressbar")
                # Schedule revert after 600ms
                revert_style = get_progress_style_name(current_value)
                root_window.after(600, lambda b=bar, s=revert_style: (
                    b.configure(style=s) if b.winfo_exists() else None
                ))
            except tk.TclError:
                pass
            break

    app_state._previous_progress_values[bar_key] = current_value


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

log_message("Application started. UI initializing.", "INFO")

# --- Database Connection Status Variables ---
postgres_connected_var = tk.BooleanVar(value=False)
postgres_active_var = tk.BooleanVar(value=False)
valkey_connected_var = tk.BooleanVar(value=False)
valkey_active_var = tk.BooleanVar(value=False)
app_state.db_status_widgets = {}

header_frame = ttk.Frame(app_state.root)
header_frame.pack(pady=(10, 5), padx=SPACING, fill="x")

title_label = ttk.Label(
    header_frame,
    text="🧠 ReadyArt Synthetic Dataset Generator",
    font=('Segoe UI', 16, 'bold'),
    foreground=ACCENT_CYAN
)
title_label.pack(side=tk.LEFT)

version_label = ttk.Label(
    header_frame,
    text="v8.8.6",
    font=('Segoe UI', 10),
    foreground='#868e96'
)
version_label.pack(side=tk.LEFT, padx=(10, 0))

ttk.Separator(app_state.root, orient='horizontal').pack(fill='x', padx=SPACING, pady=(0, SPACING))

# --- UI Controls Frame ---
controls_frame = ttk.Frame(app_state.root); controls_frame.pack(pady=SPACING, padx=SPACING, fill="x")
# Threads input removed from main window - now configured per API in the config editor

# --- Metrics Display Frame ---

# Replace your current metrics_frame section with:

metrics_frame = ttk.Frame(app_state.root)
metrics_frame.pack(pady=SPACING, padx=SPACING, fill="x")

# Helper to create a metric card
def create_metric_card(parent, title, icon, style_suffix=''):
    card = ttk.LabelFrame(parent, text=f" {icon} {title}")
    card.pack(side=tk.LEFT, padx=SPACING, fill="both", expand=True)
    value_label = ttk.Label(card, text="0 (0.0%)", style=f'MetricValue{style_suffix}.TLabel')
    value_label.pack(padx=SPACING, pady=(5, 2))
    return value_label

refusal_percent_label = create_metric_card(metrics_frame, "Refusals", "🚫")
user_speaking_label = create_metric_card(metrics_frame, "User Speak", "🗣️")
slop_label = create_metric_card(metrics_frame, "Slop", "🧹")
error_percent_label = create_metric_card(metrics_frame, "Errors", "⚠️")

# Secondary metrics row
metrics_row2 = ttk.Frame(app_state.root)
metrics_row2.pack(pady=(0, SPACING), padx=SPACING, fill="x")

token_label = create_metric_card(metrics_row2, "Tokens", "🔢")
cost_label = create_metric_card(metrics_row2, "Est. Cost", "💰")
budget_label = create_metric_card(metrics_row2, "Budget", "📊")
thread_status_label = create_metric_card(metrics_row2, "Threads", "🧵")

# Rate Limit Status Labels
rate_limit_frame = ttk.LabelFrame(app_state.root, text="Rate Limit Status (Requests/Min)")
rate_limit_frame.pack(pady=SPACING, padx=SPACING, fill="x")
for slot_idx in range(6):
    label = ttk.Label(rate_limit_frame, text=f"API {slot_idx+1}: --/--")
    label.pack(side=tk.LEFT, padx=SPACING, pady=SPACING)
    app_state.rate_limit_labels[slot_idx] = label
# --- End of Metrics Display Frame ---

# --- Database Connection Status Frame ---
db_status_frame = ttk.LabelFrame(app_state.root, text="🗄️ Database & Cache Status")
db_status_frame.pack(pady=SPACING, padx=SPACING, fill="x")

# Add refresh button to db_status_frame
db_refresh_btn = ttk.Button(
    db_status_frame,
    text="🔄 Refresh Status",
    command=update_database_status
)
db_refresh_btn.pack(side=tk.RIGHT, padx=SPACING, pady=SPACING)

db_test_btn = ttk.Button(
    db_status_frame,
    text="🔍 Test Valkey",
    command=test_valkey_connection
)
db_test_btn.pack(side=tk.RIGHT, padx=SPACING, pady=SPACING)

# PostgreSQL Status
postgres_status_frame = ttk.Frame(db_status_frame)
postgres_status_frame.pack(side=tk.LEFT, padx=SPACING, pady=SPACING)

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
valkey_status_frame.pack(side=tk.LEFT, padx=SPACING, pady=SPACING)

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

# Store references for updates
app_state.db_status_widgets = {
    'postgres_icon': postgres_icon_label,
    'postgres_status': postgres_status_label,
    'valkey_icon': valkey_icon_label,
    'valkey_status': valkey_status_label
}

# --- API Response Time Display Frame ---
api_response_times_frame = tk.LabelFrame(app_state.root, text="API Response Times"); api_response_times_frame.pack(pady=SPACING, padx=SPACING, fill="x")
for slot_idx in range(6):
    slot_label_name = f"api_response_time_label_{slot_idx+1}"
    slot_label = ttk.Label(api_response_times_frame, text=f"API {slot_idx+1}: No data yet", font=('TkDefaultFont', 8))
    slot_label.pack(side=tk.LEFT, padx=SPACING, pady=SPACING)
    globals()[slot_label_name] = slot_label  # Store reference in globals for update_dashboard to access
# --- End of API Response Time Display Frame ---

# --- Progress Bars Frame ---
progress_frame = ttk.Frame(app_state.root); progress_frame.pack(pady=SPACING, padx=SPACING, fill=tk.X)

# --- Main Action Buttons Frame ---
button_frame = ttk.Frame(app_state.root); button_frame.pack(pady=SPACING)

start_button = ttk.Button(
    button_frame,
    text="🚀 Start Generation",
    command=start_processing,
    style="Accent.TButton"  # Optional: use custom style
)
start_button.pack(side=tk.LEFT, padx=SPACING)

pause_button = ttk.Button(
    button_frame,
    text="⏸️ Pause",
    command=toggle_pause,
    state=tk.DISABLED
)
pause_button.pack(side=tk.LEFT, padx=SPACING)

dashboard_pause_var = tk.BooleanVar(value=False)
ttk.Checkbutton(metrics_frame, text="⏸️ Pause UI Updates", variable=dashboard_pause_var).pack(side=tk.RIGHT, padx=SPACING)

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

def update_thread_status_display():
    """Periodically updates the thread count label in the GUI."""
    try:
        # Safely count spawned and active threads
        spawned = len(app_state.threads) if 'threads' in globals() and app_state.threads else 0
        active = sum(1 for t in app_state.threads if t.is_alive()) if spawned > 0 else 0
        thread_status_label.config(text=f"Threads: {spawned} spawned, {active} active")
    except Exception:
        thread_status_label.config(text="Threads: 0 spawned, 0 active")

    # Schedule next update every 1 second
    if app_state.root.winfo_exists():
        app_state.root.after(1000, update_thread_status_display)

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

stop_clear_button = ttk.Button(
    button_frame,
    text="🛑 Stop & Clear Job",
    command=stop_and_clear_processing_job,
    state=tk.DISABLED
)
stop_clear_button.pack(side=tk.LEFT, padx=SPACING)
# --- End of Stop and Clear Job Functionality ---

config_button = ttk.Button(
    button_frame,
    text="⚙️ Edit Config",
    command=open_config_editor
)
config_button.pack(side=tk.LEFT, padx=SPACING)

debug_log_check = ttk.Checkbutton(
    button_frame,
    text="🐛 Debug Logs",
    variable=debug_logging_var,
    command=toggle_debug_logging
)
debug_log_check.pack(side=tk.LEFT, padx=SPACING)

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

quit_button = ttk.Button(
    button_frame,
    text="❌ Quit Application",
    command=quit_application
)
quit_button.pack(side=tk.LEFT, padx=SPACING)
app_state.root.protocol("WM_DELETE_WINDOW", quit_application) # Handle window close (X) button

status_bar = ttk.Label(app_state.root, text="Ready", foreground="lightgray", anchor="w")
status_bar.pack(side=tk.BOTTOM, fill=tk.X, padx=SPACING, pady=SPACING)

def force_recovery():
    """Bypasses config checks and forces state reload + thread restart."""
    if not app_state.processing_active and not any(t.is_alive() for t in app_state.threads):
        if os.path.exists(STATE_FILE_PATH):
            if load_generation_state():
                log_message("Force recovery: State loaded. Ready to resume.", "INFO")
                reset_all_stats_and_history()
                update_dashboard()
                messagebox.showinfo("Recovery", "State recovered. Click Start to resume.")
            else:
                messagebox.showwarning("Recovery Failed", "Could not load state. Check logs.")
        else:
            messagebox.showinfo("No State", "No previous state found to recover.")
    else:
        messagebox.showwarning("Busy", "Cannot force recovery while processing is active.")

def trigger_export():
    file_path = filedialog.asksaveasfilename(
        defaultextension=".jsonl",
        filetypes=[("JSONL Files", "*.jsonl")],
        title="Export Conversations to JSONL"
    )
    if not file_path: return

    # Run export in background thread to prevent UI freeze
    def run_export():
        start_button.config(state=tk.DISABLED)
        status_bar.config(text="Exporting from PostgreSQL...", foreground="blue")
        success = export_db_to_jsonl(file_path)
        app_state.root.after(0, lambda: status_bar.config(text="Export complete!" if success else "Export failed.",
                                                foreground="green" if success else "red"))
        app_state.root.after(0, lambda: start_button.config(state=tk.NORMAL))

    threading.Thread(target=run_export, daemon=True).start()

export_button = ttk.Button(
    button_frame,
    text="📤 Export DB → JSONL",
    command=trigger_export
)
export_button.pack(side=tk.LEFT, padx=SPACING)

clear_db_button = ttk.Button(
    button_frame,
    text="🗑️ Clear Database",
    command=clear_database
)
clear_db_button.pack(side=tk.LEFT, padx=SPACING)

recovery_button = ttk.Button(
    button_frame,
    text="🔄 Force Recovery",
    command=force_recovery
)
recovery_button.pack(side=tk.LEFT, padx=SPACING)


def clear_dashboard():

    # Clear total recent lists
    app_state.recent_refusals_total = []
    app_state.recent_user_speaking_total = []
    app_state.recent_slop_total = []
    app_state.recent_anti_slop_total = []
    app_state.recent_errors_total = []

    # Clear per-API recent lists
    for i in range(6):
        app_state.recent_refusals_per_api[i] = []
        app_state.recent_user_speaking_per_api[i] = []
        app_state.recent_slop_per_api[i] = []
        app_state.recent_anti_slop_per_api[i] = []
        app_state.recent_errors_per_api[i] = []

    # Clear graph timestamps safely
    with app_state.issue_timestamps_lock:
        for key in app_state.issue_timestamps:
            app_state.issue_timestamps[key] = []

    # Refresh the UI
    update_dashboard()
    log_message("Dashboard and issue graph cleared.", "INFO")

# --- Dashboard Setup ---

# --- Dashboard Setup ---
dashboard_outer_frame = ttk.Frame(app_state.root); dashboard_outer_frame.pack(pady=SPACING, padx=SPACING, fill=tk.BOTH, expand=True)

dashboard_toolbar = ttk.Frame(dashboard_outer_frame)
dashboard_toolbar.pack(fill=tk.X, pady=(0, 5))
clear_dash_btn = ttk.Button(dashboard_toolbar, text="🧹 Clear Dashboard", command=clear_dashboard)
clear_dash_btn.pack(side=tk.RIGHT, padx=SPACING)

dashboard_notebook = ttk.Notebook(dashboard_outer_frame)
dashboard_notebook.pack(fill=tk.BOTH, expand=True)
dashboard_notebook.tabs_widgets = {} # To store references to text areas in tabs

highlight_colors = {
    "highlight_refusal": {"foreground": "#FF6B6B", "font": ('TkDefaultFont', 9, 'bold')},  # Bright red
    "highlight_user_speak": {"foreground": "#4DABF7", "font": ('TkDefaultFont', 9, 'bold')},  # Bright blue
    "highlight_slop": {"foreground": "#9775FA", "font": ('TkDefaultFont', 9, 'bold')},  # Bright purple
    "highlight_anti_slop": {"foreground": "#FFD43B", "font": ('TkDefaultFont', 9, 'bold')},  # Bright yellow
    "highlight_error": {"foreground": "#FC8181", "font": ('TkDefaultFont', 9, 'bold')}  # Bright orange
}

tab_names = ["Totals"] + [f"API {i+1}" for i in range(4)]
issue_types = ["Refusals", "User Speak", "Slop", "Anti-Slop", "Errors"]
issue_keys = ["refusals", "user_speak", "slop", "anti_slop", "errors"] # Keys for accessing data and widgets

# --- Live Prompt Preview Tab Setup ---
preview_tab = ttk.Frame(dashboard_notebook)
dashboard_notebook.add(preview_tab, text="Live Prompt Preview")

prompt_preview_text = scrolledtext.ScrolledText(preview_tab, wrap=tk.WORD, state=tk.NORMAL)
prompt_preview_text.config(font=('Consolas', 9))
prompt_preview_text.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=SPACING)
prompt_preview_text.insert(tk.END, "Waiting for prompt generation...\n\n(Prompts will appear here in real-time as they are queued for the API)")
prompt_preview_text.config(state=tk.DISABLED)
# --- End Preview Tab Setup ---

for tab_name in tab_names:
    tab_frame = ttk.Frame(dashboard_notebook)
    dashboard_notebook.add(tab_frame, text=tab_name)
    dashboard_notebook.tabs_widgets[tab_name] = {}

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

    dashboard_notebook.tabs_widgets[tab_name]['search_var'] = search_var
    dashboard_notebook.tabs_widgets[tab_name]['search_entry'] = search_entry

    # 2. Determine parent frame and configure grids
    if tab_name == "Totals":
        canvas = tk.Canvas(tab_frame)
        scrollbar = ttk.Scrollbar(tab_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas_window_id = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        def _on_canvas_configure(event, c=canvas, wid=canvas_window_id):
            if event.width > 1:
                c.itemconfig(wid, width=event.width)
        canvas.bind("<Configure>", _on_canvas_configure)

        tab_frame.columnconfigure(0, weight=1)
        tab_frame.columnconfigure(1, weight=0)  # Pin scrollbar to right
        tab_frame.rowconfigure(1, weight=1)

        canvas.grid(row=1, column=0, sticky="nsew")
        scrollbar.grid(row=1, column=1, sticky="ns")

        scrollable_frame.columnconfigure(0, weight=1)
        scrollable_frame.columnconfigure(1, weight=1)
        for r in range(3): scrollable_frame.rowconfigure(r, weight=1)

        parent_frame = scrollable_frame
        panel_row_offset = 0
        dashboard_notebook.tabs_widgets[tab_name]['scrollable_frame'] = scrollable_frame
        dashboard_notebook.tabs_widgets[tab_name]['canvas'] = canvas
        canvas.bind("<MouseWheel>", lambda e, c=canvas: c.yview_scroll(int(-1*(e.delta/120)), "units"))

    elif tab_name.startswith("API "):
        # Wrap API tabs in Canvas + Scrollbar
        canvas = tk.Canvas(tab_frame)
        scrollbar = ttk.Scrollbar(tab_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas_window_id = canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        def _on_canvas_configure(event, c=canvas, wid=canvas_window_id):
            if event.width > 1:
                c.itemconfig(wid, width=event.width)
        canvas.bind("<Configure>", _on_canvas_configure)

        # Grid setup: Canvas expands, scrollbar pinned to far right
        tab_frame.columnconfigure(0, weight=1)
        tab_frame.columnconfigure(1, weight=0)
        tab_frame.rowconfigure(0, weight=0)  # Search bar (fixed height)
        tab_frame.rowconfigure(1, weight=1)  # Canvas area (expands)

        canvas.grid(row=1, column=0, sticky="nsew")
        scrollbar.grid(row=1, column=1, sticky="ns")

        scrollable_frame.columnconfigure(0, weight=1)
        scrollable_frame.columnconfigure(1, weight=1)
        for r in range(3): scrollable_frame.rowconfigure(r, weight=1)

        parent_frame = scrollable_frame
        panel_row_offset = 0  # Panels now start at row 0 inside scrollable_frame
        dashboard_notebook.tabs_widgets[tab_name]['scrollable_frame'] = scrollable_frame
        dashboard_notebook.tabs_widgets[tab_name]['canvas'] = canvas
        canvas.bind("<MouseWheel>", lambda e, c=canvas: c.yview_scroll(int(-1*(e.delta/120)), "units"))

    else:
        parent_frame = tab_frame
        panel_row_offset = 1
        tab_frame.rowconfigure(1, weight=1)
        tab_frame.rowconfigure(2, weight=1)
        tab_frame.rowconfigure(3, weight=1)

    # 3. Create Issue Panels
    for idx, issue_type_title in enumerate(issue_types):
        if tab_name == "Totals" and idx == 4:  # Skip "Errors" for Totals tab
            continue

        key = issue_keys[idx]
        panel = ttk.LabelFrame(parent_frame, text=f"Recent {issue_type_title}")
        base_row, col = divmod(idx, 2)
        panel.grid(row=base_row + panel_row_offset, column=col, padx=SPACING, pady=SPACING, sticky="nsew")

        text_area = scrolledtext.ScrolledText(panel, wrap=tk.WORD, height=6)
        text_area.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=SPACING)
        text_area.insert(tk.END, f"No recent {key}.")
        text_area.config(state=tk.DISABLED)
        dashboard_notebook.tabs_widgets[tab_name][key] = text_area

        for tag_name_cfg, config_cfg in highlight_colors.items():
            text_area.tag_configure(tag_name_cfg, foreground=config_cfg["foreground"], font=config_cfg["font"])

    # 4. Add Graph to Totals Tab
    if tab_name == "Totals":
        graph_frame = ttk.LabelFrame(scrollable_frame, text="Issue Detection Over Time (Last 60 Minutes)")
        graph_frame.grid(row=2, column=0, columnspan=2, padx=SPACING, pady=(20, 5), sticky="nsew")

        graph_canvas_widget = tk.Canvas(graph_frame, height=400, bg='#1a1a1a')
        graph_canvas_widget.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=SPACING)
        dashboard_notebook.tabs_widgets[tab_name]['graph_canvas'] = graph_canvas_widget
        draw_issue_graph(graph_canvas_widget)



def search_in_dashboard_tab(tab_name):
    """Highlights matching text across all issue panels in the active tab."""
    query = dashboard_notebook.tabs_widgets[tab_name].get('search_var', tk.StringVar()).get().strip()
    if not query:
        clear_dashboard_search(tab_name)
        return

    clear_dashboard_search(tab_name)
    issue_keys = ["refusals", "user_speak", "slop", "anti_slop", "errors"]

    for key in issue_keys:
        text_widget = dashboard_notebook.tabs_widgets[tab_name].get(key)
        if text_widget and text_widget.winfo_exists():
            # Configure highlight tag (doesn't interfere with existing issue tags)
            text_widget.tag_configure("search_match", background="#FFD700", foreground="#000000", underline=False)

            start_pos = "1.0"
            while True:
                # Case-insensitive search
                pos = text_widget.search(query, start_pos, stopindex=tk.END, nocase=True)
                if not pos:
                    break
                end_pos = f"{pos}+{len(query)}c"
                text_widget.tag_add("search_match", pos, end_pos)
                start_pos = end_pos

            # Auto-scroll to first match if found
            if text_widget.tag_ranges("search_match"):
                text_widget.see(text_widget.tag_ranges("search_match")[0])

def clear_dashboard_search(tab_name):
    """Removes search highlights from all panels in the specified tab."""
    for key in ["refusals", "user_speak", "slop", "anti_slop", "errors"]:
        text_widget = dashboard_notebook.tabs_widgets[tab_name].get(key)
        if text_widget and text_widget.winfo_exists():
            text_widget.tag_remove("search_match", "1.0", tk.END)

def copy_dashboard_tab(tab_name):
    """Copies all text from the active tab's panels to clipboard."""
    clipboard_text = []
    for key in ["refusals", "user_speak", "slop", "anti_slop", "errors"]:
        text_widget = dashboard_notebook.tabs_widgets[tab_name].get(key)
        if text_widget and text_widget.winfo_exists():
            content = text_widget.get("1.0", tk.END).strip()
            if content and content != f"No recent {key}.":
                clipboard_text.append(f"--- {key.upper()} ---\n{content}\n")

    if clipboard_text:
        app_state.root.clipboard_clear()
        app_state.root.clipboard_append("\n".join(clipboard_text))
        app_state.root.update()
        status_bar.config(text="Dashboard text copied to clipboard!", foreground="green")
    else:
        status_bar.config(text="No data to copy.", foreground="orange")

def update_dashboard_safe(): 
    """Safely updates the dashboard, checking if the root window still exists. Called from ConfigEditor."""
    if app_state.root.winfo_exists(): 
        update_dashboard()
ConfigEditor.update_dashboard_safe = update_dashboard_safe # Make it accessible from ConfigEditor instance

def init_database_pool():
    """Initializes PostgreSQL connection pool at app startup."""
    if global_config.get('database.enabled', False):
        try:
            from psycopg2 import pool
            app_state.db_pool = pool.ThreadedConnectionPool(
                minconn=2, maxconn=global_config.get('database.pool_size', 10),
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
