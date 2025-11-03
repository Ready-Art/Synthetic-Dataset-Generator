# state.py
from threading import Lock

# --- Application Control Flags (remain in memory) ---
stop_processing = False
pause_processing = False
processing_active = False

# --- Main Application UI Objects (remain in memory) ---
root = None  # Tkinter root window
threads = []
api_widgets = {} # For progress bars
overall_progress_bar = None
overall_time_label = None
total_tasks_for_progress = 0
worker_params_snapshot = {}

# --- Locks (for UI or specific non-Valkey actions) ---
output_data_lock = Lock()
