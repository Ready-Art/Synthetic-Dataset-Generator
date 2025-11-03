# utils/config_handler.py
import os
import yaml
import json
from tkinter import messagebox

from utils.valkey_client import ValkeyKeys
from utils.file_io import log_message, read_text_file_lines, read_text_file_content, PROMPTS_DIR, STATE_FILE_PATH

class ConfigLoader:
    def __init__(self, path='configs/config.yml'):
        self.path = path
        self.profiles_dir = os.path.join(os.path.dirname(path), 'profiles')
        self.config = {}
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        os.makedirs(self.profiles_dir, exist_ok=True)
        self.load()

    def load(self):
        """Loads the main configuration from the YAML file."""
        try:
            with open(self.path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f) or {}
        except FileNotFoundError:
            self.config = {}
            log_message(f"Config file not found at {self.path}. Creating a default one.", "WARNING")
            self.save_config_data({}) # Create a default empty file
        except yaml.YAMLError as e:
            self.config = {}
            log_message(f"Error parsing YAML in {self.path}: {e}. Using empty config.", "ERROR")
        
        if 'valkey' not in self.config:
            self.config['valkey'] = {'host': 'localhost', 'port': 6379, 'db': 0}
            
        self.load_prompts_from_files()

    def load_prompts_from_files(self):
        """Loads prompts from text files in the prompts/ directory."""
        if 'prompts' not in self.config: self.config['prompts'] = {}
        if 'detection' not in self.config: self.config['detection'] = {}

        prompt_map = {
            'system_base.txt': ('prompts', 'system', 'base'),
            'system_variations.txt': ('prompts', 'system', 'variations'),
            'question_prompt.txt': ('prompts', 'question'),
            'answer_prompt.txt': ('prompts', 'answer'),
            'user_continuation_prompt.txt': ('prompts', 'user_continuation_prompt'),
            'jailbreaks.txt': ('detection', 'refusal', 'fixes'),
            'slop_fixes.txt': ('detection', 'slop', 'fixes'),
            'speaking_fixes_female.txt': ('detection', 'user_speaking', 'female', 'fixes'),
            'speaking_fixes_male.txt': ('detection', 'user_speaking', 'male', 'fixes'),
            'speaking_fixes_neutral.txt': ('detection', 'user_speaking', 'neutral', 'fixes'),
        }

        for filename, path_keys in prompt_map.items():
            file_path = os.path.join(PROMPTS_DIR, filename)
            is_list = filename.endswith('s.txt') # variations, jailbreaks, fixes
            
            content = read_text_file_lines(file_path) if is_list else read_text_file_content(file_path)
            
            if content:
                d = self.config
                for key in path_keys[:-1]:
                    d = d.setdefault(key, {})
                d[path_keys[-1]] = content
            elif not self.get('.'.join(path_keys)):
                d = self.config
                for key in path_keys[:-1]:
                    d = d.setdefault(key, {})
                d[path_keys[-1]] = [] if is_list else ""


    def get(self, path, default=None):
        keys = path.split('.')
        value = self.config
        for key in keys:
            try:
                if isinstance(value, dict):
                    value = value[key]
                elif isinstance(value, list) and key.isdigit():
                    value = value[int(key)]
                else:
                    return default
            except (KeyError, IndexError):
                return default
        return value

    def save_config_data(self, config_data):
        """Saves the provided dictionary to the main config.yml file."""
        try:
            with open(self.path, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, sort_keys=False, indent=2, default_flow_style=False)
            return True, "Config saved."
        except Exception as e:
            log_message(f"Error saving main config: {e}", "ERROR")
            return False, f"Error saving config: {e}"

    def save_profile(self, profile_name, config_data):
        safe_name = "".join(c for c in profile_name if c.isalnum() or c in (' ', '_', '-')).rstrip()
        if not safe_name:
            return False, "Invalid profile name."
        profile_path = os.path.join(self.profiles_dir, f"{safe_name}.yml")
        try:
            with open(profile_path, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, sort_keys=False, indent=2, default_flow_style=False)
            log_message(f"Profile '{safe_name}' saved.", "INFO")
            return True, f"Profile '{safe_name}' saved."
        except Exception as e:
            return False, f"Error saving profile: {e}"

    def load_profile_to_main_config(self, profile_name):
        profile_path = os.path.join(self.profiles_dir, f"{profile_name}.yml")
        if not os.path.exists(profile_path):
            return False, f"Profile '{profile_name}' not found."
        try:
            with open(profile_path, 'r', encoding='utf-8') as f_profile:
                profile_config = yaml.safe_load(f_profile)
            with open(self.path, 'w', encoding='utf-8') as f_main:
                yaml.dump(profile_config, f_main, sort_keys=False, indent=2, default_flow_style=False)
            self.load()
            return True, f"Profile '{profile_name}' loaded."
        except Exception as e:
            return False, f"Error loading profile: {e}"

    def list_profiles(self):
        try:
            return sorted([f.replace('.yml', '') for f in os.listdir(self.profiles_dir) if f.endswith('.yml')])
        except Exception:
            return []

    def delete_profile(self, profile_name):
        profile_path = os.path.join(self.profiles_dir, f"{profile_name}.yml")
        if not os.path.exists(profile_path):
            return False, "Profile not found."
        try:
            os.remove(profile_path)
            return True, f"Profile '{profile_name}' deleted."
        except Exception as e:
            return False, f"Error deleting profile: {e}"

# --- State Persistence Functions ---
def save_generation_state(global_config, valkey_client):
    """Saves a snapshot of the current configuration to Valkey for resume compatibility checks."""
    if not valkey_client or not valkey_client.is_connected():
        return
    try:
        config_snapshot = {
            'prompts.use_questions_file': global_config.get('prompts.use_questions_file'),
            'generation.num_turns': global_config.get('generation.num_turns', 1),
            'generation.subject_size': global_config.get('generation.subject_size', 1000),
            'generation.context_size': global_config.get('generation.context_size', 3000),
            'api.master_duplication_mode': global_config.get('api.master_duplication_mode', False)
        }
        valkey_client.set(ValkeyKeys.CONFIG_SNAPSHOT, json.dumps(config_snapshot))
    except Exception as e:
        log_message(f"Error saving config snapshot to Valkey: {e}", "ERROR")

def load_generation_state(global_config, valkey_client):
    """Loads a previously saved state from Valkey and checks for compatibility."""
    if not valkey_client or not valkey_client.is_connected():
        return False
        
    saved_config_json = valkey_client.get(ValkeyKeys.CONFIG_SNAPSHOT)
    if not saved_config_json:
        return True # No previous state, so it's fine to proceed

    try:
        saved_config = json.loads(saved_config_json)
        current_config = {
            'prompts.use_questions_file': global_config.get('prompts.use_questions_file'),
            'generation.num_turns': global_config.get('generation.num_turns', 1),
            'api.master_duplication_mode': global_config.get('api.master_duplication_mode', False)
        }
        if not global_config.get('prompts.use_questions_file'):
            current_config['generation.subject_size'] = global_config.get('generation.subject_size', 1000)
            current_config['generation.context_size'] = global_config.get('generation.context_size', 3000)

        diff = {k: (saved_config.get(k), current_config.get(k)) for k in current_config if saved_config.get(k) != current_config.get(k)}
        
        if diff:
            msg = "Saved state has different critical settings:\n" + "\n".join([f"- {k.split('.')[-1]}: {v[0]} -> {v[1]}" for k, v in diff.items()]) + "\n\nResume anyway?"
            if not messagebox.askyesno("Resume Incompatibility", msg):
                return False

        log_message(f"Generation state loaded from Valkey. {valkey_client.scard(ValkeyKeys.COMPLETED_TASK_IDS)} tasks previously completed.", "INFO")
        return True
    except Exception as e:
        log_message(f"Error loading generation state from Valkey: {e}. Starting fresh.", "ERROR")
        return False
