# config_loader.py
import os
import yaml
import re
from logging_config import log_message

##
#input length limit. Modify if you run into input length issues.
##
def sanitize_input(text, max_length=100000000):
    if not text:
        return text  # Allow empty strings
    if len(text) > max_length:
        raise ValueError(f"Invalid input length: {len(text)} exceeds {max_length}")
    return re.sub(r'[<>\"\'\\]', '', text)

# --- ConfigLoader Class ---
# Manages loading, accessing, and saving application configurations,
# including main config (config.yml), profiles, and .env for API keys.
class ConfigLoader:
    def __init__(self, path='config/config.yml'):
        """
        Initializes the ConfigLoader.

        Args:
            path (str): Path to the main configuration YAML file.
        """
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
                self.config = yaml.safe_load(f)
            if self.config is None: # If file is empty or just whitespace/comments
                self.config = {}
                log_message(f"Warning: Config file {self.path} was empty or invalid. Initialized to empty config.", "WARNING")
        except FileNotFoundError:
            self.config = {}
            log_message(f"Config file {self.path} not found. Using default/empty config. Save from editor to create.", "WARNING")
            try:
                # Attempt to create an empty config file if it doesn't exist
                with open(self.path, 'w', encoding='utf-8') as f:
                    yaml.dump({}, f)
                log_message(f"Created empty config file at {self.path}.", "INFO")
            except Exception as e_create:
                log_message(f"Could not create empty config file {self.path}: {e_create}", "ERROR")
        except yaml.YAMLError as e_yaml:
            self.config = {} # Reset to empty on parse error
            log_message(f"Error parsing YAML in {self.path}: {e_yaml}. Using default/empty config.", "ERROR")
        except Exception as e:
            self.config = {} # Fallback for any other loading errors
            log_message(f"Config load failed: {str(e)}. Using default/empty config.", "ERROR")

    def get(self, path, default=None):
        """
        Retrieves a configuration value using a dot-separated path.
        Example: get('api.apis.0.url')
        """
        keys = path.split('.')
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            elif isinstance(value, list) and key.isdigit() and int(key) < len(value): # Access list elements by index
                value = value[int(key)]
            else:
                return default
        return value

    def set(self, path, value_to_set):
        """
        Sets a configuration value using a dot-separated path.
        Creates nested dictionaries if they don't exist.
        """
        keys = path.split('.')
        current_level = self.config
        for i, key in enumerate(keys[:-1]):
            if key not in current_level or not isinstance(current_level[key], dict):
                current_level[key] = {}
            current_level = current_level[key]
        current_level[keys[-1]] = value_to_set

    def save_profile(self, profile_name, config_data):
        """Saves the provided configuration data as a named profile YAML file."""
        if not profile_name:
            log_message("Profile name cannot be empty.", "ERROR")
            return False, "Profile name cannot be empty."
        # Sanitize profile name to be file-system friendly
        safe_profile_name = "".join(c for c in profile_name if c.isalnum() or c in (' ', '_', '-')).rstrip()
        if not safe_profile_name:
            log_message("Invalid profile name after sanitization (e.g., all special characters).", "ERROR")
            return False, "Invalid profile name (becomes empty after sanitization)."

        profile_path = os.path.join(self.profiles_dir, f"{safe_profile_name}.yml")
        try:
            with open(profile_path, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, sort_keys=False, indent=2, default_flow_style=False)
            log_message(f"Profile '{safe_profile_name}' saved to {profile_path}", "INFO")
            return True, f"Profile '{safe_profile_name}' saved."
        except Exception as e:
            log_message(f"Error saving profile '{safe_profile_name}': {e}", "ERROR")
            return False, f"Error saving profile: {e}"

    def load_profile_to_main_config(self, profile_name):
        """
        Loads a named profile's content into the main configuration file (config.yml)
        and then reloads the application's runtime configuration from this main file.
        """
        profile_path = os.path.join(self.profiles_dir, f"{profile_name}.yml")
        if not os.path.exists(profile_path):
            log_message(f"Profile '{profile_name}' not found at {profile_path}", "ERROR")
            return False, f"Profile '{profile_name}' not found."
        try:
            with open(profile_path, 'r', encoding='utf-8') as f_profile:
                profile_config = yaml.safe_load(f_profile)
            if profile_config is None: # Check if profile YAML was empty or invalid
                log_message(f"Profile '{profile_name}' is empty or invalid.", "ERROR")
                return False, f"Profile '{profile_name}' is empty or invalid."

            # Overwrite the main config.yml with the profile's content
            with open(self.path, 'w', encoding='utf-8') as f_main:
                yaml.dump(profile_config, f_main, sort_keys=False, indent=2, default_flow_style=False)

            # Reload the main configuration into the application's runtime
            self.load()
            log_message(f"Profile '{profile_name}' loaded into main config and reloaded.", "INFO")
            return True, f"Profile '{profile_name}' loaded."
        except Exception as e:
            log_message(f"Error loading profile '{profile_name}': {e}", "ERROR")
            return False, f"Error loading profile: {e}"

    def list_profiles(self):
        """Lists available configuration profiles by scanning the profiles directory."""
        try:
            profiles = [f.replace('.yml', '') for f in os.listdir(self.profiles_dir) if f.endswith('.yml')]
            return sorted(profiles)
        except Exception as e:
            log_message(f"Error listing profiles: {e}", "ERROR")
            return []

    def delete_profile(self, profile_name):
        """Deletes a named configuration profile file."""
        profile_path = os.path.join(self.profiles_dir, f"{profile_name}.yml")
        if not os.path.exists(profile_path):
            log_message(f"Profile '{profile_name}' not found for deletion.", "WARNING")
            return False, "Profile not found."
        try:
            os.remove(profile_path)
            log_message(f"Profile '{profile_name}' deleted.", "INFO")
            return True, f"Profile '{profile_name}' deleted."
        except Exception as e:
            log_message(f"Error deleting profile '{profile_name}': {e}", "ERROR")
            return False, f"Error deleting profile: {e}"
# --- End of ConfigLoader Class ---
