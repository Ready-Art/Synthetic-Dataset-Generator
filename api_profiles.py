"""API compatibility profiles for outgoing LLM request payloads.

The payload builders in generation.py emit a permissive, vLLM/OpenAI-style JSON body (top_k,
min_p, repetition_penalty, chat_template_kwargs, logit_bias, ...). Some hosted APIs reject any
field outside their documented set with HTTP 400/422, so a request that works against a local
vLLM server fails outright against, e.g., Mistral.

A *profile* is an allow-list of body keys (plus optional key renames) applied to the payload
just before it is sent, chosen per API slot. Profiles are data -- see config/api_profiles.yml --
so supporting another endpoint is a YAML edit, not a code change.

THREAD SAFETY
-------------
Every API call from every worker thread funnels through apply_profile_for_slot(), while the main
thread can re-enter load_profiles()/reset_slot_profiles()/set_slot_profile() (resume, recovery,
restart). All module state (_profiles, _slot_profiles, _loaded) is therefore guarded by a single
lock. The lock is a plain (non-reentrant) threading.Lock, so the public load_profiles() acquires
it and delegates to a lock-free _load_unlocked(); _ensure_loaded() uses double-checked locking so
it never calls a locking function while already holding the lock.

This module is a dependency leaf (stdlib + PyYAML + logging_config only) so both generate.py and
generation.py can import it without creating a cycle.
"""
import os
import threading
import urllib.parse

import yaml

from logging_config import log_message

PROFILES_PATH = os.path.join("config", "api_profiles.yml")

# The default profile sends the payload untouched -- identical to the pre-profile behaviour, so a
# config that never mentions api_profile keeps working exactly as before.
DEFAULT_PROFILE = "openai_compatible"

# Structural keys that carry the request itself, never sampler tuning -- kept regardless of profile.
_ALWAYS_ALLOWED = frozenset({"model", "messages", "stream"})

# Value transforms for specific key renames. repetition_penalty is multiplicative around 1.0;
# frequency_penalty is additive around 0.0 and valid in [-2, 2]. Pairs not listed here are copied
# through unchanged.
_RENAME_TRANSFORMS = {
    ("repetition_penalty", "frequency_penalty"): lambda v: round(max(-2.0, min(2.0, float(v) - 1.0)), 4),
}

# Sampler keys the request builders can emit; used to seed the "custom" allow-list menu.
_BUILDER_EMITTED_KEYS = frozenset({
    "temperature", "top_p", "top_k", "min_p", "repetition_penalty", "max_tokens",
    "frequency_penalty", "presence_penalty", "logit_bias", "chat_template_kwargs",
})

_profiles = {}        # name -> {"label": str, "allow": set|None|"from_config", "rename": {}, "detect_hosts": []}
_slot_profiles = {}   # slot_idx -> (profile_name, custom_allow_set_or_None)
_loaded = False

# Single lock guarding ALL module state above. Non-reentrant on purpose; see module docstring.
_LOCK = threading.Lock()


def _builtin_profiles():
    """Fallback registry used when config/api_profiles.yml is missing or unreadable."""
    return {
        DEFAULT_PROFILE: {
            "label": "Broad / OpenAI-compatible (vLLM, TGI, llama.cpp, local servers)",
            "allow": None,
            "rename": {},
            "detect_hosts": [],
        },
    }


def _coerce_allow(raw, profile_name):
    if raw in (None, "all"):
        return None
    if raw == "from_config":
        return "from_config"
    if isinstance(raw, (list, tuple, set)):
        return {str(k).strip() for k in raw if str(k).strip()}
    log_message(f"api_profiles: profile '{profile_name}' has an invalid 'allow'; treating as passthrough.", "WARNING")
    return None


def _build_profiles_from_yaml(path):
    """Parse the YAML file into a fresh profiles dict. Pure; takes no lock. Returns {} on failure."""
    parsed = None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            parsed = yaml.safe_load(fh)
    except FileNotFoundError:
        log_message(f"api_profiles: {path} not found; using built-in defaults.", "WARNING")
    except Exception as exc:
        log_message(f"api_profiles: failed to read {path}: {exc}; using built-in defaults.", "ERROR")

    profiles = {}
    if isinstance(parsed, dict):
        for name, spec in parsed.items():
            if not isinstance(spec, dict):
                continue
            rename = spec.get("rename", {})
            if not isinstance(rename, dict):
                rename = {}
            hosts = spec.get("detect_hosts", []) or []
            if isinstance(hosts, str):
                hosts = [hosts]
            profiles[str(name)] = {
                "label": str(spec.get("label", name)),
                "allow": _coerce_allow(spec.get("allow", None), name),
                "rename": {str(k): str(v) for k, v in rename.items()},
                "detect_hosts": [str(h).strip().lower() for h in hosts if str(h).strip()],
            }

    if not profiles:
        profiles = _builtin_profiles()
    if DEFAULT_PROFILE not in profiles:
        profiles[DEFAULT_PROFILE] = _builtin_profiles()[DEFAULT_PROFILE]
    return profiles


def _load_unlocked(path=PROFILES_PATH):
    """Rebuild and install the profile registry. CALLER MUST HOLD _LOCK."""
    global _profiles, _loaded
    _profiles = _build_profiles_from_yaml(path)
    _loaded = True
    return _profiles


def load_profiles(path=PROFILES_PATH):
    """(Re)load the profile registry from YAML. Safe to call repeatedly (e.g. after an editor save)."""
    with _LOCK:
        return _load_unlocked(path)


def _ensure_loaded():
    """Idempotent, race-free lazy load (double-checked locking)."""
    if not _loaded:
        with _LOCK:
            if not _loaded:
                _load_unlocked(PROFILES_PATH)


def list_profiles():
    """[(name, label), ...] for a dropdown.

    Order: the default (passthrough) profile first, then 'custom' so the two most-reached-for
    options sit at the top, then every other profile alphabetically by label.
    """
    _ensure_loaded()
    with _LOCK:
        names = list(_profiles)
        labels = {n: _profiles[n]["label"] for n in names}

    def sort_key(name):
        if name == DEFAULT_PROFILE:
            return (0, "")
        if name == "custom":
            return (1, "")
        return (2, labels[name].lower())

    return [(n, labels[n]) for n in sorted(names, key=sort_key)]


def get_profile(name):
    """Return the spec dict for `name`, falling back to the default profile."""
    _ensure_loaded()
    with _LOCK:
        return _profiles.get(name) or _profiles[DEFAULT_PROFILE]


def known_param_keys():
    """Sorted union of every key any profile explicitly allows -- the menu for a 'custom' list."""
    _ensure_loaded()
    with _LOCK:
        keys = set(_BUILDER_EMITTED_KEYS)
        for spec in _profiles.values():
            if isinstance(spec["allow"], set):
                keys |= spec["allow"]
    return sorted(keys)


def detect_profile(api_url):
    """Best-guess profile name from a URL's host via each profile's detect_hosts, or None."""
    _ensure_loaded()
    if not api_url:
        return None
    try:
        host = (urllib.parse.urlparse(api_url).hostname or "").lower()
    except Exception:
        return None
    if not host:
        return None
    with _LOCK:
        items = list(_profiles.items())
    for name, spec in items:
        for dh in spec["detect_hosts"]:
            if host == dh or host.endswith("." + dh):
                return name
    return None


def set_slot_profile(slot_idx, profile_name, custom_allowed=None, api_url=None):
    """Record the compatibility profile for one API slot (0-5).

    Called by generate.py before workers start. An explicit non-default profile always wins; an
    unset/default profile falls back to host auto-detection (so a config pointed at Mistral still
    gets filtered even if it predates this feature), and a detection miss leaves it on the default
    passthrough profile.
    """
    _ensure_loaded()
    with _LOCK:
        known = set(_profiles)
    name = profile_name if profile_name in known else ""
    if not name or name == DEFAULT_PROFILE:
        detected = detect_profile(api_url)
        if detected:
            if name != detected:
                log_message(f"api_profiles: API slot {slot_idx + 1} auto-detected profile '{detected}' from URL host.", "INFO")
            name = detected
    if not name:
        name = DEFAULT_PROFILE

    custom = None
    if custom_allowed:
        custom = {str(k).strip() for k in custom_allowed if str(k).strip()}
    with _LOCK:
        _slot_profiles[slot_idx] = (name, custom)
    return name


def reset_slot_profiles():
    """Forget all per-slot profile registrations (called at the start of each generation run)."""
    with _LOCK:
        _slot_profiles.clear()


def _is_passthrough(spec):
    return spec["allow"] is None and not spec["rename"]


def _apply(payload_dict, spec, custom_allow):
    """Pure transform (no module state, no lock). Returns a new dict, or a filtered copy."""
    allow = spec["allow"]
    if allow == "from_config":
        allow = custom_allow  # None -> passthrough; set -> filter

    out = dict(payload_dict)

    # Renames first, so a value survives its source key being filtered out below.
    for old_key, new_key in spec["rename"].items():
        if old_key in out and new_key not in out:
            transform = _RENAME_TRANSFORMS.get((old_key, new_key), lambda v: v)
            try:
                out[new_key] = transform(out[old_key])
            except (TypeError, ValueError):
                pass

    if allow is None:
        return out
    return {k: v for k, v in out.items() if k in _ALWAYS_ALLOWED or k in allow}


def apply_profile(payload_dict, profile_name, custom_allowed=None):
    """Return payload_dict filtered for the named profile (a new dict), or the same object unchanged
    when the profile does no filtering or renaming."""
    _ensure_loaded()
    spec = get_profile(profile_name)
    if _is_passthrough(spec):
        return payload_dict
    custom = None
    if custom_allowed is not None:
        custom = {str(k).strip() for k in custom_allowed if str(k).strip()}
    return _apply(payload_dict, spec, custom)


def apply_profile_for_slot(payload_dict, slot_idx, api_url=None):
    """Filter payload_dict for the profile registered to API slot `slot_idx`.

    Falls back to host auto-detection (then the default passthrough profile) when the slot has no
    registered profile -- e.g. when generation code runs outside a normal generate.py worker start.
    Returns the same object unchanged when the resolved profile does no filtering or renaming.
    """
    _ensure_loaded()
    with _LOCK:
        entry = _slot_profiles.get(slot_idx)
    if entry is not None:
        name, custom = entry
    else:
        name, custom = (detect_profile(api_url) or DEFAULT_PROFILE), None

    spec = get_profile(name)
    if _is_passthrough(spec):
        return payload_dict
    return _apply(payload_dict, spec, custom)


load_profiles()
