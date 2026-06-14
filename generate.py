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
from generation import worker, check_budget_limit, estimate_time_remaining, save_generation_state
from dashboard import clear_dashboard, clear_dashboard_search, configure_animated_progress_styles, copy_dashboard_tab, create_metric_card, draw_issue_graph, pulse_progress_bar, search_in_dashboard_tab, update_dashboard, update_dashboard_safe, update_progress_bar_style, update_thread_status_display
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






# --- Resilience: requeue tasks that fail while their API host is down ---
# When a host goes down the circuit breaker (above) opens for that slot. Without
# requeueing, a worker would consume each pulled task, fail it, and discard it,
# draining the queue so nothing is left to process when the host recovers. These
# helpers let the worker put such tasks back, bounded so a genuinely-bad task
# (one that fails for content reasons while the host is up) can't loop forever.



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
                app_state.character_counter = state_data.get('character_counter', 0)
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




# --- Tkinter UI Update and Control Functions ---

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
app_state.live_prompt_preview_hook = update_live_prompt_preview

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
    current_lore = global_config.get('prompts.lore', '')
    if current_lore:
        log_message(f"Lore loaded ({len(current_lore)} chars).", "INFO")
    else:
        log_message("No lore configured.", "INFO")
    character_config = global_config.get('prompts.character', {})
    enable_character_engine_local = character_config.get('enabled', True)
    enable_class_selection_local = character_config.get('class_enabled', False)
    enable_setting_selection_local = character_config.get('setting_enabled', False)

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
        old_appearance = character_config.get('appearance', [])
        old_backstory = character_config.get('backstory', [])
        old_personality = character_config.get('personality', [])
        old_setting = character_config.get('setting', [])
        old_class = character_config.get('class', [])

        if any([old_names, old_races, old_jobs, old_clothing, old_appearance, old_backstory, old_personality, old_setting, old_class, old_genders]):
            max_len = max(
                len(old_names), len(old_races), len(old_jobs), len(old_clothing),
                len(old_appearance), len(old_backstory), len(old_personality),
                len(old_setting), len(old_class), len(old_genders)
            )
            for i in range(max_len):
                character_list.append({
                    'name': old_names[i] if i < len(old_names) else '',
                    'age': old_ages[i] if i < len(old_ages) else '',
                    'gender': old_genders[i] if i < len(old_genders) else '',
                    'race': old_races[i] if i < len(old_races) else '',
                    'job': old_jobs[i] if i < len(old_jobs) else '',
                    'clothing': old_clothing[i] if i < len(old_clothing) else '',
                    'appearance': old_appearance[i] if i < len(old_appearance) else '',
                    'backstory': old_backstory[i] if i < len(old_backstory) else '',
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
    current_slop_to_anti_slop_fallback = global_config.get('generation.slop_to_anti_slop_fallback', False)

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
            enable_setting_selection_local,
            character_list,
            enable_emotional_states,
            emotional_states_list,
            num_characters_local,
            no_user_impersonation_var.get(),
            current_api_request_timeout,
            current_slop_to_anti_slop_fallback,
            current_lore,
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






# Track the previous progress percentage for each bar to detect milestone changes




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
    text="v9.0.2",
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

app_state.refusal_percent_label = create_metric_card(metrics_frame, "Refusals", "🚫")
app_state.user_speaking_label = create_metric_card(metrics_frame, "User Speak", "🗣️")
app_state.slop_label = create_metric_card(metrics_frame, "Slop", "🧹")
app_state.error_percent_label = create_metric_card(metrics_frame, "Errors", "⚠️")

# Secondary metrics row
metrics_row2 = ttk.Frame(app_state.root)
metrics_row2.pack(pady=(0, SPACING), padx=SPACING, fill="x")

app_state.token_label = create_metric_card(metrics_row2, "Tokens", "🔢")
app_state.cost_label = create_metric_card(metrics_row2, "Est. Cost", "💰")
app_state.budget_label = create_metric_card(metrics_row2, "Budget", "📊")
app_state.thread_status_label = create_metric_card(metrics_row2, "Threads", "🧵")

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
    app_state.slot_widgets[slot_label_name] = slot_label  # Store reference in globals for update_dashboard to access
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

app_state.status_bar = ttk.Label(app_state.root, text="Ready", foreground="lightgray", anchor="w")
app_state.status_bar.pack(side=tk.BOTTOM, fill=tk.X, padx=SPACING, pady=SPACING)

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
        app_state.status_bar.config(text="Exporting from PostgreSQL...", foreground="blue")
        success = export_db_to_jsonl(file_path)
        app_state.root.after(0, lambda: app_state.status_bar.config(text="Export complete!" if success else "Export failed.",
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



# --- Dashboard Setup ---

# --- Dashboard Setup ---
dashboard_outer_frame = ttk.Frame(app_state.root); dashboard_outer_frame.pack(pady=SPACING, padx=SPACING, fill=tk.BOTH, expand=True)

dashboard_toolbar = ttk.Frame(dashboard_outer_frame)
dashboard_toolbar.pack(fill=tk.X, pady=(0, 5))
clear_dash_btn = ttk.Button(dashboard_toolbar, text="🧹 Clear Dashboard", command=clear_dashboard)
clear_dash_btn.pack(side=tk.RIGHT, padx=SPACING)

app_state.dashboard_notebook = ttk.Notebook(dashboard_outer_frame)
app_state.dashboard_notebook.pack(fill=tk.BOTH, expand=True)
app_state.dashboard_notebook.tabs_widgets = {} # To store references to text areas in tabs

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
preview_tab = ttk.Frame(app_state.dashboard_notebook)
app_state.dashboard_notebook.add(preview_tab, text="Live Prompt Preview")

prompt_preview_text = scrolledtext.ScrolledText(preview_tab, wrap=tk.WORD, state=tk.NORMAL)
prompt_preview_text.config(font=('Consolas', 9))
prompt_preview_text.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=SPACING)
prompt_preview_text.insert(tk.END, "Waiting for prompt generation...\n\n(Prompts will appear here in real-time as they are queued for the API)")
prompt_preview_text.config(state=tk.DISABLED)
# --- End Preview Tab Setup ---

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
        app_state.dashboard_notebook.tabs_widgets[tab_name]['scrollable_frame'] = scrollable_frame
        app_state.dashboard_notebook.tabs_widgets[tab_name]['canvas'] = canvas
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
        app_state.dashboard_notebook.tabs_widgets[tab_name]['scrollable_frame'] = scrollable_frame
        app_state.dashboard_notebook.tabs_widgets[tab_name]['canvas'] = canvas
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
        app_state.dashboard_notebook.tabs_widgets[tab_name][key] = text_area

        for tag_name_cfg, config_cfg in highlight_colors.items():
            text_area.tag_configure(tag_name_cfg, foreground=config_cfg["foreground"], font=config_cfg["font"])

    # 4. Add Graph to Totals Tab
    if tab_name == "Totals":
        graph_frame = ttk.LabelFrame(scrollable_frame, text="Issue Detection Over Time (Last 60 Minutes)")
        graph_frame.grid(row=2, column=0, columnspan=2, padx=SPACING, pady=(20, 5), sticky="nsew")

        graph_canvas_widget = tk.Canvas(graph_frame, height=400, bg='#1a1a1a')
        graph_canvas_widget.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=SPACING)
        app_state.dashboard_notebook.tabs_widgets[tab_name]['graph_canvas'] = graph_canvas_widget
        draw_issue_graph(graph_canvas_widget)






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
