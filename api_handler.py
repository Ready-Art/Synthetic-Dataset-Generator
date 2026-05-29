#api_handler.py
import time
from threading import Lock
import redis
import hashlib
from logging_config import log_message

valkey_client = None

api_response_times_per_slot = {i: [] for i in range(6)}  # Store response times for each API slot (0-5)
api_response_times_lock = Lock()  # Thread-safe access to response times
MAX_RESPONSE_TIMES_TO_TRACK = 100  # Keep last 100 response times per API

class RateLimiter:
    def __init__(self):
        """Initializes the rate limiter with empty request logs for each API slot."""
        self.requests_per_slot = {i: [] for i in range(6)}  # Store timestamps per API slot (0-5)
        self.rates_per_slot = {i: 60 for i in range(6)}  # Default 60 requests per minute per slot
        self.lock = Lock()  # Thread-safe access to request logs

    def set_rate_limit(self, slot_idx, rpm):
        """Sets the requests-per-minute limit for a specific API slot."""
        with self.lock:
            self.rates_per_slot[slot_idx] = rpm
            log_message(f"Rate limit set for API Slot {slot_idx+1}: {rpm} RPM", "INFO")

    def wait_if_needed(self, slot_idx):
        """
        Checks if we've exceeded the rate limit for this API slot.
        FIX: Sleep and Log are OUTSIDE the lock to prevent thread freezing.
        """
        wait_time = 0
        with self.lock:
            current_time = time.time()
            # Keep only requests from the last minute
            one_minute_ago = current_time - 60
            self.requests_per_slot[slot_idx] = [
                t for t in self.requests_per_slot[slot_idx] if t > one_minute_ago
            ]

            current_rate = self.rates_per_slot.get(slot_idx, 60)

            if len(self.requests_per_slot[slot_idx]) >= current_rate:
                # We've hit the limit, need to wait
                oldest_request = min(self.requests_per_slot[slot_idx])
                wait_time = 60 - (current_time - oldest_request)
                if wait_time < 0:
                    wait_time = 0

        # FIX: Sleep and Log OUTSIDE the lock
        if wait_time > 0:
            log_message(f"API Slot {slot_idx+1} rate limit reached. Waiting {wait_time:.2f}s", "DEBUG")
            time.sleep(wait_time)

        # Re-acquire lock to record this request timestamp safely
        with self.lock:
            self.requests_per_slot[slot_idx].append(time.time())

global_rate_limiter = RateLimiter()

def get_cached_response(prompt_hash, api_slot_idx):
    """
    Checks Valkey for a cached response.
    Returns (response_text, is_cached)
    """
    global valkey_client
    if valkey_client is None:
        return None, False

    cache_key = f"cache:{prompt_hash}:{api_slot_idx}"
    try:
        cached_data = valkey_client.get(cache_key)
        if cached_data:
            log_message(f"Cache HIT for API Slot {api_slot_idx+1}. Skipping API call.", "DEBUG")
            return cached_data, True
        return None, False
    except Exception as e:
        log_message(f"Error checking Valkey cache: {e}", "ERROR")
        return None, False

def set_cached_response(prompt_hash, api_slot_idx, response_text, ttl=3600):
    """
    Saves a response to Valkey with a Time-To-Live (TTL) in seconds.
    """
    global valkey_client
    if valkey_client is None or not response_text:
        return

    cache_key = f"cache:{prompt_hash}:{api_slot_idx}"
    try:
        # Store for 1 hour (3600 seconds)
        valkey_client.set(cache_key, response_text, ex=ttl)
        log_message(f"Cache SET for API Slot {api_slot_idx+1}.", "DEBUG")
    except Exception as e:
        log_message(f"Error saving to Valkey cache: {e}", "ERROR")

