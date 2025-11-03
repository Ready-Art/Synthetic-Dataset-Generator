# main_app.py
import os
import sys
import threading
import time
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
from queue import Queue, Empty, Full
import yaml
import re
import json

# --- Local Project Imports ---
import state
from utils.valkey_client import get_valkey_client, ValkeyKeys
from utils.config_handler import ConfigLoader, save_generation_state, load_generation_state
from utils.file_io import (
    log_message, setup_directories, read_text_file_lines, read_text_file_content,
    cleanup_old_files_and_backup_output, INPUT_DIR, QUESTIONS_FILE_PATH
)
from core.generation_worker import worker

# --- Global Config Instance ---
global_config = ConfigLoader()
# --- Global Valkey Client ---
valkey_client = get_valkey_client(global_config)

# --- UI Helper Functions ---
def estimate_time_remaining(processed_items, total_items, times_list):
    """Estimates the time remaining for a set of tasks."""
    if not times_list or processed_items < 1 or total_items == 0:
        return "Estimating..."
    average_time = sum(times_list) / len(times_list)
    remaining_items = total_items - processed_items
    if remaining_items <= 0: return "Done!"
    remaining_seconds = remaining_items * average_time
    return time.strftime('%H:%M:%S', time.gmtime(remaining_seconds))

# --- UI Update and Control Functions ---
def update_dashboard():
    """Updates the dashboard labels and text areas with current statistics."""
    if not (hasattr(root, 'winfo_exists') and root.winfo_exists()) or not valkey_client:
        return

    total_attempts = int(valkey_client.get(ValkeyKeys.STAT_TOTAL_ATTEMPTS) or 1)
    refusal_count = int(valkey_client.get(ValkeyKeys.STAT_REFUSAL_TOTAL) or 0)
    user_speak_count = int(valkey_client.get(ValkeyKeys.STAT_USER_SPEAK_TOTAL) or 0)
    slop_count = int(valkey_client.get(ValkeyKeys.STAT_SLOP_TOTAL) or 0)
    error_count = int(valkey_client.get(ValkeyKeys.STAT_ERROR_TOTAL) or 0)

    refusal_percent = (refusal_count / total_attempts) * 100
    user_speak_percent = (user_speak_count / total_attempts) * 100
    slop_percent = (slop_count / total_attempts) * 100
    error_percent = (error_count / total_attempts) * 100

    refusal_percent_label.config(text=f"{refusal_count} Refusals ({refusal_percent:.1f}%)")
    user_speaking_label.config(text=f"{user_speak_count} User Speak ({user_speak_percent:.1f}%)")
    slop_label.config(text=f"{slop_count} Slop ({slop_percent:.1f}%)")
    error_percent_label.config(text=f"{error_count} Errors ({error_percent:.1f}%)")

    def update_text_widget(widget, items, tag, is_total=False):
        widget.config(state=tk.NORMAL)
        widget.delete(1.0, tk.END)
        for item in items:
            prefix = ""
            if is_total:
                # Format: "[[phrase, sentence], api_idx]"
                item_data, api_idx = json.loads(item)
                phrase, sentence = item_data if len(item_data) == 2 else (None, str(item_data))
                prefix = f"[API {api_idx+1}] "
            else:
                 # Format: "[phrase, sentence]" or just "error summary"
                try:
                    loaded_item = json.loads(item)
                    if isinstance(loaded_item, list) and len(loaded_item) == 2:
                        phrase, sentence = loaded_item
                    else:
                        phrase, sentence = (None, str(loaded_item))
                except (json.JSONDecodeError, TypeError):
                    phrase, sentence = (None, str(item))

            widget.insert(tk.END, f"- {prefix}{sentence}\n")
        widget.config(state=tk.DISABLED)

    update_text_widget(dashboard_notebook.tabs_widgets["Totals"]["refusals"], valkey_client.lrange(ValkeyKeys.RECENT_REFUSALS_TOTAL, 0, -1), "highlight_refusal", is_total=True)
    update_text_widget(dashboard_notebook.tabs_widgets["Totals"]["user_speak"], valkey_client.lrange(ValkeyKeys.RECENT_USER_SPEAKING_TOTAL, 0, -1), "highlight_user_speak", is_total=True)
    update_text_widget(dashboard_notebook.tabs_widgets["Totals"]["slop"], valkey_client.lrange(ValkeyKeys.RECENT_SLOP_TOTAL, 0, -1), "highlight_slop", is_total=True)
    update_text_widget(dashboard_notebook.tabs_widgets["Totals"]["errors"], valkey_client.lrange(ValkeyKeys.RECENT_ERRORS_TOTAL, 0, -1), "highlight_error", is_total=True)

    for i in range(4):
        api_tab = f"API {i+1}"
        update_text_widget(dashboard_notebook.tabs_widgets[api_tab]["refusals"], valkey_client.lrange(ValkeyKeys.recent_refusals_per_api(i), 0, -1), "highlight_refusal")
        update_text_widget(dashboard_notebook.tabs_widgets[api_tab]["user_speak"], valkey_client.lrange(ValkeyKeys.recent_user_speaking_per_api(i), 0, -1), "highlight_user_speak")
        update_text_widget(dashboard_notebook.tabs_widgets[api_tab]["slop"], valkey_client.lrange(ValkeyKeys.recent_slop_per_api(i), 0, -1), "highlight_slop")
        update_text_widget(dashboard_notebook.tabs_widgets[api_tab]["errors"], valkey_client.lrange(ValkeyKeys.recent_errors_per_api(i), 0, -1), "highlight_error")

def start_processing():
    """Initiates the data generation process."""
    if not valkey_client or not valkey_client.is_connected():
        messagebox.showerror("Valkey Error", "Could not connect to Valkey. Please ensure it's running and configured correctly.")
        return
        
    global_config.load()

    should_resume = False
    if valkey_client.exists(ValkeyKeys.COMPLETED_TASK_IDS) or valkey_client.llen(ValkeyKeys.TASK_QUEUE) > 0:
        if messagebox.askyesno("Resume", "Previous generation data found in Valkey. Do you want to resume? (No will start fresh)."):
            if load_generation_state(global_config, valkey_client):
                should_resume = True
            else: # User chose not to resume incompatible state
                _clear_valkey_and_backup()
        else: # User chose not to resume
            _clear_valkey_and_backup()
    else: # No state file exists
        cleanup_old_files_and_backup_output()

    worker_params = _prepare_worker_parameters()
    if not worker_params:
        return

    state.stop_processing = False
    state.pause_processing = False
    state.processing_active = True
    update_dashboard()

    for widget in progress_frame.winfo_children():
        widget.destroy()

    total_tasks_to_queue, completed_count = _populate_task_queue(worker_params)
    
    if total_tasks_to_queue == 0 and completed_count == 0:
        messagebox.showwarning("Input Error", "No tasks to process. Check your input files.")
        state.processing_active = False
        return
    elif total_tasks_to_queue == 0 and completed_count > 0:
        messagebox.showinfo("Complete", "All tasks were already completed in a previous session.")
        state.processing_active = False
        return

    _setup_progress_bars(total_tasks_to_queue, completed_count, worker_params)

    start_button.config(state=tk.DISABLED)
    pause_button.config(state=tk.NORMAL)
    stop_clear_button.config(state=tk.NORMAL)

    state.threads.clear()
    num_threads = global_config.get('api.threads', 10)
    log_message(f"Starting {num_threads} worker threads.", "INFO")
    for i in range(num_threads):
        thread = threading.Thread(target=worker, args=(i, worker_params, valkey_client.get_connection_kwargs()), name=f"Worker-{i}")
        state.threads.append(thread)
        thread.start()

    root.after(100, update_gui_progress)
    threading.Thread(target=_wait_for_completion, name="CompletionWaiter").start()

def _prepare_worker_parameters():
    """Gathers all necessary configuration and prompts into a dictionary for workers."""
    all_apis = global_config.get('api.apis', [])
    if not all_apis or len(all_apis) < 5:
        messagebox.showerror("Config Error", "API configuration must contain at least 5 slots in config.yml.")
        return None

    active_apis_for_worker = [
        {'config': api, 'original_slot_idx': i}
        for i, api in enumerate(all_apis)
        if i < 4 and api.get('enabled') and api.get('url')
    ]

    master_duplication = global_config.get('api.master_duplication_mode', False)
    if not master_duplication and not active_apis_for_worker:
        messagebox.showerror("Config Error", "Non-Duplication mode requires at least one API (1-4) to be enabled and configured with a URL.")
        return None
    if master_duplication and not any(api.get('enabled') for i, api in enumerate(all_apis) if i < 4):
        messagebox.showerror("Config Error", "Duplication mode requires at least one API (1-4) to be enabled.")
        return None

    active_gender = global_config.get('gender', 'female')
    user_speak_config = global_config.get(f'detection.user_speaking.{active_gender}', {})

    return {
        'global_config': global_config,
        'master_duplication_mode': master_duplication,
        'num_turns': global_config.get('generation.num_turns', 1),
        'max_attempts': global_config.get('generation.max_attempts', 5),
        'history_size': global_config.get('generation.history_size', 10),
        'output_format': global_config.get('generation.output_format', 'sharegpt'),
        'max_slop_fix_iters': global_config.get('generation.max_slop_sentence_fix_iterations', 5),
        'no_user_impersonation': global_config.get('detection.no_user_impersonation', False),
        'all_api_configs': all_apis,
        'active_apis_for_worker': active_apis_for_worker,
        'slop_fixer_api_config': all_apis[4],
        'use_questions_file': global_config.get('prompts.use_questions_file', False),
        'use_variable_system': global_config.get('prompts.system.variable', False),
        'system_prompts': [global_config.get('prompts.system.base')] + global_config.get('prompts.system.variations', []),
        'question_prompt': global_config.get('prompts.question', ''),
        'answer_prompt': global_config.get('prompts.answer', ''),
        'user_continuation_prompt': global_config.get('prompts.user_continuation_prompt', ''),
        'refusal_phrases': global_config.get('detection.refusal.phrases', []),
        'jailbreaks': global_config.get('detection.refusal.fixes', []),
        'user_speaking_phrases': user_speak_config.get('phrases', []),
        'speaking_fixes': user_speak_config.get('fixes', []),
        'slop_phrases': global_config.get('detection.slop.phrases', []),
        'slop_fixes_fallback': global_config.get('detection.slop.fixes', []),
    }

def _populate_task_queue(params):
    """Reads input files and populates the task queue in Valkey."""
    tasks_to_add = []
    completed_ids = valkey_client.smembers(ValkeyKeys.COMPLETED_TASK_IDS)

    if params['use_questions_file']:
        questions = read_text_file_lines(QUESTIONS_FILE_PATH)
        for i, q_text in enumerate(questions):
            task_id = f"q_{i}"
            if task_id not in completed_ids:
                task_data = (task_id, os.path.basename(QUESTIONS_FILE_PATH), i, q_text)
                tasks_to_add.append(json.dumps(task_data))
    else:
        input_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.txt') and f != os.path.basename(QUESTIONS_FILE_PATH)]
        subject_size = global_config.get('generation.subject_size', 1000)
        context_size = global_config.get('generation.context_size', 3000)
        for file_name in input_files:
            content = read_text_file_content(os.path.join(INPUT_DIR, file_name))
            if not content: continue
            for i, chunk_index in enumerate(range(0, len(content), subject_size)):
                task_id = f"{file_name}_chunk_{chunk_index}"
                if task_id not in completed_ids:
                    subject = content[i : i + subject_size]
                    context = content[max(0, i - (context_size - subject_size)//2) : i + subject_size + (context_size - subject_size)//2]
                    task_data = (task_id, file_name, i, subject, context)
                    tasks_to_add.append(json.dumps(task_data))
    
    if tasks_to_add:
        valkey_client.lpush(ValkeyKeys.TASK_QUEUE, *tasks_to_add)
        log_message(f"Added {len(tasks_to_add)} new tasks to the queue.", "INFO")

    return len(tasks_to_add), len(completed_ids)


def _setup_progress_bars(new_tasks_count, completed_count, params):
    """Creates and initializes the progress bar widgets based on Valkey data."""
    total_unique_tasks = new_tasks_count + completed_count
    state.total_tasks_for_progress = total_unique_tasks * params['num_turns']
    
    state.api_widgets = {}

    if params['master_duplication_mode']:
        for i, api in enumerate(params['all_api_configs']):
            if i < 4 and api.get('enabled'):
                _create_progress_bar_widget(f"API Slot {i+1}", i)
    else:
        _create_progress_bar_widget("Overall Progress", "overall")

def _create_progress_bar_widget(label_text, key):
    """Helper to create a single progress bar set."""
    frame = ttk.Frame(progress_frame)
    frame.pack(fill='x', expand=True, pady=2)
    ttk.Label(frame, text=label_text).pack(side=tk.LEFT, padx=(0, 10))
    bar = ttk.Progressbar(frame, orient="horizontal", length=400, mode="determinate")
    bar.pack(side=tk.LEFT, fill='x', expand=True)
    time_label = ttk.Label(frame, text="Time Rem: Estimating...", width=30)
    time_label.pack(side=tk.LEFT, padx=(10, 0))
    
    if key == "overall":
        state.overall_progress_bar = bar
        state.overall_time_label = time_label
    else:
        state.api_widgets[key] = {'bar': bar, 'time_label': time_label}

def update_gui_progress():
    """Periodically updates progress bars and dashboard from Valkey data."""
    if not state.processing_active or not valkey_client:
        return
    
    params = getattr(state, 'worker_params_snapshot', global_config)
    is_duplication = params.get('api.master_duplication_mode', False)

    if is_duplication:
        for i, widgets in state.api_widgets.items():
            processed = int(valkey_client.get(ValkeyKeys.progress_per_api(i)) or 0)
            if state.total_tasks_for_progress > 0:
                widgets['bar']['value'] = (processed / state.total_tasks_for_progress) * 100
                # time estimation needs per-api times list, omitted for brevity
                # widgets['time_label'].config(text=...)
    else:
        processed = int(valkey_client.get(ValkeyKeys.PROGRESS_OVERALL) or 0)
        if state.total_tasks_for_progress > 0:
            state.overall_progress_bar['value'] = (processed / state.total_tasks_for_progress) * 100
            # time estimation needs times list, omitted for brevity
            # state.overall_time_label.config(text=...)

    update_dashboard()
    if root.winfo_exists():
        root.after(1000, update_gui_progress)


def _wait_for_completion():
    """Waits for all worker threads to finish."""
    # This check is now based on Valkey queue and thread status
    while state.processing_active:
        if valkey_client.llen(ValkeyKeys.TASK_QUEUE) == 0 and not any(t.is_alive() for t in state.threads):
             state.processing_active = False
        time.sleep(0.5)

    log_message("All tasks completed or processing stopped.", "INFO")
    
    if root.winfo_exists():
        root.after(0, _finalize_ui_after_completion)

def _finalize_ui_after_completion():
    """Updates UI elements once processing is fully stopped."""
    start_button.config(state=tk.NORMAL)
    pause_button.config(state=tk.DISABLED, text="Pause")
    stop_clear_button.config(state=tk.NORMAL)
    update_dashboard() # Final update
    if not state.stop_processing:
        messagebox.showinfo("Processing Complete", "All tasks have been processed!")
    save_generation_state(global_config, valkey_client)

def toggle_pause():
    """Toggles the pause state of the generation process."""
    state.pause_processing = not state.pause_processing
    pause_button.config(text="Resume" if state.pause_processing else "Pause")
    log_message("Processing paused." if state.pause_processing else "Processing resumed.", "INFO")
    if not state.pause_processing:
        global_config.load()
        log_message("Configuration reloaded from files.", "INFO")

def stop_and_clear_processing_job():
    """Stops the current job and resets for a fresh start."""
    if not state.processing_active:
        log_message("No active job. Clearing state for fresh start.", "INFO")
        _clear_valkey_and_backup()
        update_dashboard()
        return

    if messagebox.askokcancel("Stop & Clear Job", "Stop the current job and clear its progress?"):
        log_message("Stop & Clear initiated.", "INFO")
        state.stop_processing = True
        # The _wait_for_completion logic will handle UI finalization
        threading.Thread(target=_perform_cleanup_and_reset).start()

def _perform_cleanup_and_reset():
    """Waits for threads to stop then resets state by clearing Valkey."""
    for t in state.threads:
        t.join() # Wait for threads to acknowledge stop signal
    
    _clear_valkey_and_backup(clear_logs=False) # Keep logs, but clear Valkey
    
    if root.winfo_exists():
        root.after(0, lambda: [
            update_dashboard(),
            log_message("State cleared. Ready for a new job.", "INFO"),
            *[widget.destroy() for widget in progress_frame.winfo_children()]
        ])

def _clear_valkey_and_backup(clear_logs=True):
    """Helper to clear Valkey and backup outputs."""
    if valkey_client:
        valkey_client.clear_all_data()
    if clear_logs:
        cleanup_old_files_and_backup_output()
    else:
        # Just backup jsonl without deleting logs
        cleanup_old_files_and_backup_output(backup_only=True)


def quit_application():
    """Handles graceful shutdown of the application."""
    if state.processing_active and not messagebox.askokcancel("Quit", "Generation is active. Are you sure you want to quit? Progress is saved in Valkey."):
        return
    
    log_message("Shutdown initiated...", "INFO")
    state.stop_processing = True
    
    def shutdown():
        for t in state.threads:
            t.join(timeout=2.0)
        
        save_generation_state(global_config, valkey_client)
        
        if root.winfo_exists():
            root.destroy()
        sys.exit(0)

    threading.Thread(target=shutdown).start()

def open_config_editor():
    """Opens the configuration editor window."""
    editor = ConfigEditor(root)
    editor.grab_set()

# --- Configuration Editor Class (Part of GUI App) ---
class ConfigEditor(tk.Toplevel):
    # This class remains unchanged.
    pass # PASTE THE FULL CLASS DEFINITION HERE

# --- Main UI Setup ---
if __name__ == "__main__":
    setup_directories()
    
    if not valkey_client or not valkey_client.is_connected():
        # Show a simple message box if the main window isn't up yet
        tk.Tk().withdraw()
        messagebox.showerror("Valkey Connection Error", "Could not connect to Valkey server. Please ensure it is running and check your config.yml. The application will now exit.")
        sys.exit(1)

    log_message("Application started.", "INFO")

    root = tk.Tk()
    state.root = root
    root.title("Synthetic Dataset Generator")
    root.geometry("1000x850")
    style = ttk.Style()
    try:
        style.theme_use('vista')
    except tk.TclError:
        log_message("Vista theme not available, using default.", "WARNING")

    # --- UI Controls Frame ---
    controls_frame = ttk.Frame(root)
    controls_frame.pack(pady=10, padx=10, fill="x")

    # --- Metrics Display Frame ---
    metrics_frame = ttk.Frame(root)
    metrics_frame.pack(pady=5, padx=10, fill="x")
    refusal_percent_label = ttk.Label(metrics_frame, text="Refusals: 0 (0.0%)")
    refusal_percent_label.pack(side=tk.LEFT, padx=10)
    user_speaking_label = ttk.Label(metrics_frame, text="User Speak: 0 (0.0%)")
    user_speaking_label.pack(side=tk.LEFT, padx=10)
    slop_label = ttk.Label(metrics_frame, text="Slop: 0 (0.0%)")
    slop_label.pack(side=tk.LEFT, padx=10)
    error_percent_label = ttk.Label(metrics_frame, text="Errors: 0 (0.0%)")
    error_percent_label.pack(side=tk.LEFT, padx=10)

    # --- Progress Bars Frame ---
    progress_frame = ttk.Frame(root)
    progress_frame.pack(pady=10, padx=10, fill=tk.X)

    # --- Main Action Buttons Frame ---
    button_frame = ttk.Frame(root)
    button_frame.pack(pady=10)
    start_button = ttk.Button(button_frame, text="Start Generation", command=start_processing)
    start_button.pack(side=tk.LEFT, padx=10)
    pause_button = ttk.Button(button_frame, text="Pause", command=toggle_pause, state=tk.DISABLED)
    pause_button.pack(side=tk.LEFT, padx=10)
    stop_clear_button = ttk.Button(button_frame, text="Stop & Clear Job", command=stop_and_clear_processing_job)
    stop_clear_button.pack(side=tk.LEFT, padx=10)
    config_button = ttk.Button(button_frame, text="Edit Config", command=open_config_editor)
    config_button.pack(side=tk.LEFT, padx=10)
    quit_button = ttk.Button(button_frame, text="Quit Application", command=quit_application)
    quit_button.pack(side=tk.LEFT, padx=10)

    # --- Dashboard Setup ---
    dashboard_outer_frame = ttk.Frame(root)
    dashboard_outer_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)
    dashboard_notebook = ttk.Notebook(dashboard_outer_frame)
    dashboard_notebook.pack(fill=tk.BOTH, expand=True)
    dashboard_notebook.tabs_widgets = {}

    tab_names = ["Totals"] + [f"API {i+1}" for i in range(4)]
    issue_keys = ["refusals", "user_speak", "slop", "errors"]

    for tab_name in tab_names:
        tab_frame = ttk.Frame(dashboard_notebook)
        dashboard_notebook.add(tab_frame, text=tab_name)
        dashboard_notebook.tabs_widgets[tab_name] = {}
        tab_frame.columnconfigure([0, 1], weight=1)
        tab_frame.rowconfigure([0, 1], weight=1)

        for idx, key in enumerate(issue_keys):
            panel = ttk.LabelFrame(tab_frame, text=f"Recent {key.replace('_', ' ').title()}")
            panel.grid(row=idx // 2, column=idx % 2, padx=5, pady=5, sticky="nsew")
            text_area = scrolledtext.ScrolledText(panel, wrap=tk.WORD, height=6)
            text_area.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
            text_area.config(state=tk.DISABLED)
            dashboard_notebook.tabs_widgets[tab_name][key] = text_area

    update_dashboard()

    root.protocol("WM_DELETE_WINDOW", quit_application)
    root.mainloop()
