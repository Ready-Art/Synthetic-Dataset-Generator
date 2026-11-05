# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

#api_handler.py
import time
from threading import Lock
import redis
import hashlib
from logging_config import log_message

valkey_client = None

# Slots 0-5 are the configured API slots (1-4 generation, 5 slop fixer, 6 anti-slop fixer).
# Slot 6 is the LOGICAL quality-judge slot: it has no ``api.apis`` entry (the judge is configured
# under ``quality.scoring_api``) but the judge calls the rate limiter and the response-time tracker
# with ``slot_idx=6``, so those structures must be 7 wide or the judge is silently unthrottled and
# unmeasured. ``quality.scoring_api.slot_idx`` can point the judge at another slot instead; the
# width covers every value that setting is allowed to take.
RATE_LIMITER_SLOT_COUNT = 7

api_response_times_per_slot = {i: [] for i in range(RATE_LIMITER_SLOT_COUNT)}  # Response times per slot
api_response_times_lock = Lock()  # Thread-safe access to response times
MAX_RESPONSE_TIMES_TO_TRACK = 100  # Keep last 100 response times per API


# ---------------------------------------------------------------------------
# Thinking mode (v13.0.4)
#
# A reasoning model that is left on its server-side default will think on EVERY
# request, including the slop fixer, the anti-slop fixer and the quality judge.
# Those are short, structured rewrite/score jobs where a chain of thought is pure
# cost: on vLLM a judge request that thinks can take several times longer than the
# same request with thinking off, which is what made scoring look like the
# bottleneck.
#
# Before v13.0.4 only the four main generation request builders in generation.py
# read ``samplers.enable_thinking``. The fixers and the judge never sent
# ``chat_template_kwargs`` at all, so the server default (thinking ON) applied no
# matter what the Samplers tab said. These helpers are shared by generation.py and
# quality.py so one definition decides what is sent.
#
# Each auxiliary request reads its OWN setting first and falls back to the main
# ``samplers.enable_thinking`` when that setting is ``inherit`` (the shipped
# default). ``inherit`` therefore keeps a default run byte-identical to v13.0.3 --
# the main setting ships ``default``, which sends nothing -- while a user who has
# already set the Samplers tab to ``disable`` gets thinking switched off for the
# fixers and the judge too, without configuring anything new.
THINKING_MODES = ("inherit", "default", "enable", "disable")

# The per-request thinking settings, in one place so the config editor, the docs and
# the tests agree on the spelling.
#
# These are deliberately SIBLING keys, NOT entries inside `slop_fixer_params` /
# `anti_slop_params` / `scoring_api.sampler_settings`. Those dicts are read with an
# all-or-nothing `params or main_sampler_settings` fallback, so putting a single
# `enable_thinking` key inside one of them makes the dict TRUTHY and silently stops it
# inheriting the main sampler defaults -- e.g. the Slop Fixer's top_p would jump from the
# main 0.9 to its hardcoded 0.95. A sibling key cannot do that.
SLOP_FIXER_THINKING_KEY = "samplers.slop_fixer_enable_thinking"
ANTI_SLOP_FIXER_THINKING_KEY = "samplers.anti_slop_fixer_enable_thinking"
JUDGE_THINKING_KEY = "quality.scoring_api.enable_thinking"


def _normalise_thinking_mode(value):
    """Map a raw config value onto one of THINKING_MODES.

    Accepts the pre-v11 boolean form as well (True meant "send disable", False
    meant "send nothing"), matching what the config editor does on load, and falls
    back to ``inherit`` for anything unrecognised so a typo cannot silently turn
    thinking back on for a model the user asked to keep quiet.
    """
    if isinstance(value, bool):
        return "disable" if value else "default"
    mode = str(value if value is not None else "inherit").strip().lower()
    return mode if mode in THINKING_MODES else "inherit"


def resolve_thinking_mode(own_mode, main_sampler_settings=None):
    """Resolve the effective thinking mode for one auxiliary request.

    ``own_mode`` is that request's RAW setting (``samplers.slop_fixer_enable_thinking``,
    ``samplers.anti_slop_fixer_enable_thinking`` or
    ``quality.scoring_api.enable_thinking``). When it is ``inherit`` -- or missing, or
    unrecognised -- the main ``samplers.enable_thinking`` value is used instead.

    Returns ``"default"`` (send nothing), ``"enable"`` or ``"disable"``.
    """
    mode = _normalise_thinking_mode(own_mode)
    if mode != "inherit":
        return mode
    main = _normalise_thinking_mode((main_sampler_settings or {}).get("enable_thinking", "default"))
    return "default" if main == "inherit" else main


def thinking_payload_fragment(mode):
    """The ``chat_template_kwargs`` fragment for a resolved thinking mode.

    ``{}`` for ``default``, so a request whose thinking mode is unset sends exactly
    the body it sent before this option existed.
    """
    if mode == "enable":
        return {"chat_template_kwargs": {"enable_thinking": True}}
    if mode == "disable":
        return {"chat_template_kwargs": {"enable_thinking": False}}
    return {}


def apply_thinking_mode(payload_dict, own_mode, main_sampler_settings=None):
    """Attach the resolved thinking fragment to ``payload_dict`` in place.

    ``own_mode`` is the request's raw own setting (see ``resolve_thinking_mode``).
    A payload that already carries ``chat_template_kwargs`` is left alone: the main
    generation builders set it from the main setting directly, and a caller that
    composed its own kwargs is not this helper's business.
    """
    if "chat_template_kwargs" in payload_dict:
        return payload_dict
    mode = resolve_thinking_mode(own_mode, main_sampler_settings)
    payload_dict.update(thinking_payload_fragment(mode))
    return payload_dict

class RateLimiter:
    def __init__(self):
        """Initializes the rate limiter with empty request logs for each API slot."""
        self.requests_per_slot = {i: [] for i in range(RATE_LIMITER_SLOT_COUNT)}  # Timestamps per slot
        self.rates_per_slot = {i: 60 for i in range(RATE_LIMITER_SLOT_COUNT)}  # Default 60 RPM per slot
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
            self._interruptible_sleep(wait_time)

        # Re-acquire lock to record this request timestamp safely
        with self.lock:
            self.requests_per_slot[slot_idx].append(time.time())

    def _interruptible_sleep(self, seconds):
        """Sleep up to `seconds`, waking early when a stop is requested (v13.0).

        A rate-limited slot can wait almost a full minute before its next request
        is allowed. A plain ``time.sleep`` is not interruptible, so a Stop had to
        wait out that minute before the worker noticed it -- on a rate-limited
        slot the generator looked unstoppable for up to 60 s after the Stop was
        accepted, which is what made ``generator stop`` report the run as still
        winding down for that whole time.

        Sleeps in short slices and returns as soon as
        ``app_state.stop_processing`` is set, so a Stop takes effect within one
        slice. The caller still records this slot's request timestamp, so nothing
        is over-sent: a stop only means the next request is not made.

        Returns True when the whole wait elapsed, False when it was cut short.
        """
        # Imported lazily: this module is imported very early, and the sleep
        # path is the only place that needs the run flags.
        import app_state
        remaining = float(seconds)
        slice_seconds = 0.25
        while remaining > 0:
            if app_state.stop_processing:
                return False
            time.sleep(min(slice_seconds, remaining))
            remaining -= slice_seconds
        return True


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

