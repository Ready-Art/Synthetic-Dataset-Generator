# logging_config.py
import time
import os
from colorama import init, Fore, Style

# Initialize colorama for colored console output
init()

# Log file path - same for all modules
LOG_FILE_PATH = os.path.join('output', 'log.txt')
os.makedirs('output', exist_ok=True)

def log_message(message, level="INFO"):
    """
    Writes a message to the log file and prints to console with color.
    Use this function in ALL your Python files.
    """
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
