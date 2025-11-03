# utils/file_io.py
import os
import time
import json
import zipfile
import shutil
import state

# --- Directory Constants ---
INPUT_DIR = 'inputs'
OUTPUT_DIR = 'outputs'
PROMPTS_DIR = 'prompts'

LOG_FILE_PATH = os.path.join(OUTPUT_DIR, 'log.txt')
STATE_FILE_PATH = os.path.join(OUTPUT_DIR, 'generation_state.json') # Kept for legacy compatibility but not primary
BASE_OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIR, 'output')
BASE_DEBUG_LOG_PATH = os.path.join(OUTPUT_DIR, 'debug_prompt')
QUESTIONS_FILE_PATH = os.path.join(INPUT_DIR, 'questions.txt')

def setup_directories():
    """Ensures all necessary directories exist."""
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(PROMPTS_DIR, exist_ok=True)

def log_message(message, level="INFO"):
    """Writes a message to the log file and prints it to the console."""
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    log_entry = f"{timestamp} [{level}] - {message}"
    try:
        with open(LOG_FILE_PATH, 'a', encoding='utf-8') as log_file:
            log_file.write(log_entry + '\n')
    except Exception as e:
        print(f"CRITICAL: Failed to write to log file '{LOG_FILE_PATH}': {e}")
    print(log_entry)

def read_text_file_lines(file_path):
    """Reads lines from a text file, stripping whitespace and skipping empty lines."""
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    return []

def read_text_file_content(file_path):
    """Reads the entire content of a text file."""
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    return ""

def write_conversation(conversation_history, output_format, task_id, master_duplication_mode, api_slot_idx=None, is_duplication_turn=False, turn_number=0):
    """Writes a conversation to the appropriate JSONL output file."""
    from utils.text_processing import remove_reasoning_text # Local import to avoid circular dependency
    
    processed_turns = []
    for turn in conversation_history:
        content = remove_reasoning_text(turn.get("content", ""))
        if content is None: # Invalid output (e.g., unclosed <think> tag)
            log_message(f"Task {task_id}: Invalid content with unclosed reasoning tag found. Skipping write.", "WARNING")
            return

        if output_format == 'sharegpt':
            role = "human" if turn["role"] == "user" else "gpt"
            processed_turns.append({"from": role, "value": content})
        else: # OpenAI format
            processed_turns.append({"role": turn["role"], "content": content})

    output_data_id = task_id
    if is_duplication_turn:
        output_data_id = f"{task_id}_api{api_slot_idx}_turn{turn_number}"

    output_data = {
        "id": output_data_id,
        "conversations" if output_format == 'sharegpt' else "messages": processed_turns
    }

    file_path = BASE_OUTPUT_FILE_PATH + ".jsonl"
    if master_duplication_mode and api_slot_idx is not None:
        file_path = f"{BASE_OUTPUT_FILE_PATH}_api_slot_{api_slot_idx}.jsonl"

    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(output_data) + '\n')
    except Exception as e:
        log_message(f"Error writing to output file {file_path}: {e}", "ERROR")

def cleanup_old_files_and_backup_output(backup_only=False):
    """Removes old state/log files and backs up previous .jsonl output."""
    if not backup_only:
        files_to_remove = [STATE_FILE_PATH, LOG_FILE_PATH]
        for i in range(5):
            files_to_remove.append(f"{BASE_DEBUG_LOG_PATH}_api_slot_{i}.jsonl")
        files_to_remove.append(f"{BASE_DEBUG_LOG_PATH}.jsonl")

        for f_path in files_to_remove:
            if os.path.exists(f_path):
                try:
                    os.remove(f_path)
                    log_message(f"Removed old file: {f_path}", "INFO")
                except Exception as e:
                    log_message(f"Error removing old file {f_path}: {e}", "WARNING")

    jsonl_files = [os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR) if f.endswith(".jsonl")]
    if not jsonl_files:
        log_message("No .jsonl files found to backup.", "INFO")
        return

    timestamp = time.strftime('%Y%m%d_%H%M%S')
    backup_zip_path = os.path.join(OUTPUT_DIR, f"output_backup_{timestamp}.zip")
    
    try:
        with zipfile.ZipFile(backup_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for f_path in jsonl_files:
                zipf.write(f_path, os.path.basename(f_path))
        log_message(f"Successfully created backup: {backup_zip_path}", "INFO")

        for f_path in jsonl_files:
            os.remove(f_path)
    except Exception as e:
        log_message(f"Error creating backup archive: {e}. Original .jsonl files NOT deleted.", "ERROR")
