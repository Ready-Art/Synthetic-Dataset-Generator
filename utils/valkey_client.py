# utils/valkey_client.py
import valkey
import json
from .file_io import log_message

class ValkeyKeys:
    """Centralized key management for Valkey."""
    PREFIX = "sdg" # Synthetic Dataset Generator
    
    # --- State and Control ---
    CONFIG_SNAPSHOT = f"{PREFIX}:config_snapshot"
    SYSTEM_PROMPT_COUNTER = f"{PREFIX}:system_prompt_counter"
    QUESTION_HISTORY = f"{PREFIX}:question_history"
    
    # --- Task Management ---
    TASK_QUEUE = f"{PREFIX}:task_queue"
    COMPLETED_TASK_IDS = f"{PREFIX}:completed_task_ids"

    # --- Progress Tracking ---
    PROGRESS_OVERALL = f"{PREFIX}:progress:overall"
    @staticmethod
    def progress_per_api(api_idx):
        return f"{ValkeyKeys.PREFIX}:progress:api:{api_idx}"

    # --- Global Statistics ---
    STAT_TOTAL_ATTEMPTS = f"{PREFIX}:stats:total_attempts"
    STAT_REFUSAL_TOTAL = f"{PREFIX}:stats:refusal:total"
    STAT_USER_SPEAK_TOTAL = f"{PREFIX}:stats:user_speak:total"
    STAT_SLOP_TOTAL = f"{PREFIX}:stats:slop:total"
    STAT_ERROR_TOTAL = f"{PREFIX}:stats:error:total"

    # --- Per-API Statistics ---
    @staticmethod
    def stat_total_attempts_per_api(api_idx):
        return f"{ValkeyKeys.PREFIX}:stats:total_attempts:api:{api_idx}"
    @staticmethod
    def stat_refusal_per_api(api_idx):
        return f"{ValkeyKeys.PREFIX}:stats:refusal:api:{api_idx}"
    @staticmethod
    def stat_user_speaking_per_api(api_idx):
        return f"{ValkeyKeys.PREFIX}:stats:user_speak:api:{api_idx}"
    @staticmethod
    def stat_slop_per_api(api_idx):
        return f"{ValkeyKeys.PREFIX}:stats:slop:api:{api_idx}"
    @staticmethod
    def stat_error_per_api(api_idx):
        return f"{ValkeyKeys.PREFIX}:stats:error:api:{api_idx}"
    
    # --- Recent Issues ---
    RECENT_REFUSALS_TOTAL = f"{PREFIX}:recent:refusals:total"
    RECENT_USER_SPEAKING_TOTAL = f"{PREFIX}:recent:user_speaking:total"
    RECENT_SLOP_TOTAL = f"{PREFIX}:recent:slop:total"
    RECENT_ERRORS_TOTAL = f"{PREFIX}:recent:errors:total"
    @staticmethod
    def recent_refusals_per_api(api_idx):
        return f"{ValkeyKeys.PREFIX}:recent:refusals:api:{api_idx}"
    @staticmethod
    def recent_user_speaking_per_api(api_idx):
        return f"{ValkeyKeys.PREFIX}:recent:user_speaking:api:{api_idx}"
    @staticmethod
    def recent_slop_per_api(api_idx):
        return f"{ValkeyKeys.PREFIX}:recent:slop:api:{api_idx}"
    @staticmethod
    def recent_errors_per_api(api_idx):
        return f"{ValkeyKeys.PREFIX}:recent:errors:api:{api_idx}"

class ValkeyClient:
    def __init__(self, host='localhost', port=6379, db=0):
        self.connection_kwargs = {'host': host, 'port': port, 'db': db, 'decode_responses': True}
        try:
            self.client = valkey.StrictRedis(**self.connection_kwargs)
            self.client.ping()
            log_message(f"Successfully connected to Valkey at {host}:{port}/{db}.", "INFO")
        except valkey.exceptions.ConnectionError as e:
            log_message(f"Could not connect to Valkey: {e}", "ERROR")
            self.client = None

    def get_connection_kwargs(self):
        return self.connection_kwargs
        
    def is_connected(self):
        return self.client is not None

    def get_client(self):
        return self.client

    def clear_all_data(self):
        """Deletes all keys managed by this application."""
        if not self.client: return
        keys_to_delete = self.client.keys(f"{ValkeyKeys.PREFIX}:*")
        if keys_to_delete:
            self.client.delete(*keys_to_delete)
            log_message(f"Cleared {len(keys_to_delete)} Valkey keys for a fresh start.", "INFO")

    # --- Wrapper methods for convenience ---
    def get(self, key):
        return self.client.get(key) if self.client else None

    def set(self, key, value):
        if self.client: self.client.set(key, value)

    def incr(self, key, amount=1):
        return self.client.incr(key, amount) if self.client else None

    def sadd(self, key, value):
        if self.client: self.client.sadd(key, value)

    def sismember(self, key, value):
        return self.client.sismember(key, value) if self.client else False

    def smembers(self, key):
        return self.client.smembers(key) if self.client else set()
        
    def lpush(self, key, *values):
        if self.client: self.client.lpush(key, *values)

    def brpop(self, key, timeout=1):
        # brpop returns a tuple (key, value), we just want the value
        if not self.client: return None
        result = self.client.brpop(key, timeout=timeout)
        return result[1] if result else None
    
    def lrange(self, key, start, end):
        return self.client.lrange(key, start, end) if self.client else []
        
    def llen(self, key):
        return self.client.llen(key) if self.client else 0
        
    def exists(self, key):
        return self.client.exists(key) if self.client else False

    def push_to_list_with_trim(self, key, value, max_length):
        """LPUSHes to a list and keeps it trimmed to a max size."""
        if not self.client: return
        p = self.client.pipeline()
        p.lpush(key, value)
        p.ltrim(key, 0, max_length - 1)
        p.execute()

# --- Global Instance ---
_valkey_connection_instance = None

def get_valkey_client(config=None):
    """Initializes and returns the global Valkey client instance."""
    global _valkey_connection_instance
    if _valkey_connection_instance is None and config:
        host = config.get('valkey.host', 'localhost')
        port = int(config.get('valkey.port', 6379))
        db = int(config.get('valkey.db', 0))
        _valkey_connection_instance = ValkeyClient(host=host, port=port, db=db)
    return _valkey_connection_instance
