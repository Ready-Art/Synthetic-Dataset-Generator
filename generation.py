"""Generation engine for the Synthetic Dataset Generator (refactor step 3b).

The worker loop and all LLM-call/answer-generation logic, extracted from generate.py. Shares runtime
state through app_state (counters, circuit breaker, config, path constants) and calls the existing helper
modules (api_handler / detection / text_utils / logging_config). The only call back toward the GUI — the
live prompt preview — is routed through app_state.live_prompt_preview_hook (registered by generate.py),
so this module never imports generate.py: the dependency is one-way (generate.py imports generation).
"""
import hashlib
import json
import random
import requests
import time
import tkinter as tk

import api_profiles
import app_state
import detection
import text_utils
import quality
from queue import Queue, Empty, Full
from logging_config import log_message, LOG_FILE_PATH
from api_handler import (
    RateLimiter, global_rate_limiter, get_cached_response, set_cached_response,
    api_response_times_per_slot, api_response_times_lock, MAX_RESPONSE_TIMES_TO_TRACK,
)
from app_state import (
    API_CIRCUIT_BREAKER, BASE_DEBUG_LOG_PATH, BASE_OUTPUT_FILE_PATH, MAX_RECENT, MAX_TASK_REQUEUES,
    STATE_FILE_PATH, anti_slop_counts_per_api, api_circuit_breaker_lock, estimated_cost,
    global_config, task_retry_counts, task_retry_lock,
)


def update_live_prompt_preview(messages_list, metadata=None):
    """Engine-side shim: forward the live prompt preview to the GUI if generate.py registered a hook."""
    hook = app_state.live_prompt_preview_hook
    if hook:
        hook(messages_list, metadata)


def sanitize_payload_for_endpoint(payload_dict, api_url, slot_idx):
    """Filter the outgoing request body to what API slot `slot_idx`'s compatibility profile allows.

    Thin wrapper over api_profiles (see config/api_profiles.yml). The builders emit a permissive
    vLLM/OpenAI-style payload; some hosted APIs (Mistral, OpenAI, ...) reject fields outside their
    documented set with HTTP 400/422. The default profile does no filtering and returns the same
    object unchanged, so untouched configs behave exactly as before. `api_url` is used only as a
    fallback to auto-detect the profile when the slot has no registered one.
    """
    return api_profiles.apply_profile_for_slot(payload_dict, slot_idx, api_url)


def check_budget_limit():
    if app_state.stop_processing:
        return False

    budget_limit = global_config.get('api.pricing.budget_limit', 0.0)
    if budget_limit <= 0:
        return False  # Budget disabled

    with app_state.stats_lock:  # 🔒 Safer concurrent read
        price_per_1k = global_config.get('api.pricing.cost_per_1k_tokens', 0)
        current_cost = (app_state.total_input_tokens + app_state.total_output_tokens) * (price_per_1k / 1000.0)

    if current_cost >= budget_limit:
        log_message(f"API budget limit of ${budget_limit:.2f} reached. Current cost: ${current_cost:.2f}. Stopping generation.", "WARNING")
        app_state.stop_processing = True
        return True
    return False


def check_circuit_breaker(api_slot_idx):
    """Returns True if API slot is allowed to make requests, False if circuit is open."""
    with api_circuit_breaker_lock:
        if API_CIRCUIT_BREAKER["is_open"][api_slot_idx]:
            elapsed = time.time() - API_CIRCUIT_BREAKER["last_failure_time"][api_slot_idx]
            # NEW: Use per-slot cooldown instead of global cooldown
            current_cooldown = API_CIRCUIT_BREAKER["current_cooldown"].get(api_slot_idx, 60)

            if elapsed >= current_cooldown:
                API_CIRCUIT_BREAKER["is_open"][api_slot_idx] = False
                API_CIRCUIT_BREAKER["failures"][api_slot_idx] = 0
                API_CIRCUIT_BREAKER["current_cooldown"][api_slot_idx] = API_CIRCUIT_BREAKER["base_cooldown_seconds"]
                log_message(
                    f"API Slot {api_slot_idx+1} circuit closed after {elapsed:.0f}s. "
                    f"Resuming requests with base cooldown.",
                    "INFO"
                )
                return True
            return False
        return True


def record_api_failure(api_slot_idx):
    with api_circuit_breaker_lock:
        failures = API_CIRCUIT_BREAKER["failures"][api_slot_idx] + 1
        API_CIRCUIT_BREAKER["failures"][api_slot_idx] = failures
        API_CIRCUIT_BREAKER["last_failure_time"][api_slot_idx] = time.time()

        # NEW: Calculate exponential backoff (60s, 120s, 240s, 480s, max 600s)
        backoff = min(
            API_CIRCUIT_BREAKER["max_cooldown_seconds"],
            API_CIRCUIT_BREAKER["base_cooldown_seconds"] * (2 ** (failures - 1))
        )
        API_CIRCUIT_BREAKER["current_cooldown"][api_slot_idx] = backoff

        if failures >= API_CIRCUIT_BREAKER["max_consecutive_failures"]:
            API_CIRCUIT_BREAKER["is_open"][api_slot_idx] = True
            log_message(
                f"API Slot {api_slot_idx+1} circuit OPEN after {failures} consecutive failures. "
                f"Backoff: {backoff}s (exponential)",
                "WARNING"
            )


def record_api_success(api_slot_idx):
    """Reset failure count on successful API call."""
    with api_circuit_breaker_lock:
        if API_CIRCUIT_BREAKER["failures"][api_slot_idx] > 0:
            API_CIRCUIT_BREAKER["failures"][api_slot_idx] = 0
            API_CIRCUIT_BREAKER["current_cooldown"][api_slot_idx] = API_CIRCUIT_BREAKER["base_cooldown_seconds"]
            log_message(f"API Slot {api_slot_idx+1} success - failure count reset.", "DEBUG")


def api_host_is_down(api_slot_idx):
    """Read-only: True if this slot's circuit is currently open (host down).
    Unlike check_circuit_breaker(), this does NOT close the circuit on cooldown."""
    if api_slot_idx is None or api_slot_idx < 0:
        return False
    with api_circuit_breaker_lock:
        return bool(API_CIRCUIT_BREAKER["is_open"].get(api_slot_idx, False))


def requeue_task(q, task, task_id):
    """Put a task back on the queue for a later retry (host-outage recovery)."""
    with app_state.task_queue_ui_lock:
        if task_id in app_state.task_metadata:
            app_state.task_metadata[task_id]['retries'] += 1
    with task_retry_lock:
        n = task_retry_counts.get(task_id, 0) + 1
        task_retry_counts[task_id] = n
        if n > MAX_TASK_REQUEUES:
            return False
    q.put(task)
    return True


def save_generation_state():
    """Saves the current generation state to a JSON file for potential recovery."""
    global api_response_times_per_slot

    with app_state.state_file_lock: # Ensure thread-safe file writing
        try:
            state_data = {
                'completed_task_ids': list(app_state.completed_task_ids), # Convert set to list for JSON
                'system_prompt_counter': app_state.system_prompt_counter,
                'question_history': app_state.question_history,
                'total_attempts_global': app_state.total_attempts_global,
                'refusal_count_total': app_state.refusal_count_total,
                'user_speaking_count_total': app_state.user_speaking_count_total,
                'slop_count_total': app_state.slop_count_total,
                'error_count_total': app_state.error_count_total,
                'refusal_counts_per_api': app_state.refusal_counts_per_api,
                'total_input_tokens': app_state.total_input_tokens,
                'total_output_tokens': app_state.total_output_tokens,
                'estimated_cost': estimated_cost,
                'user_speaking_counts_per_api': app_state.user_speaking_counts_per_api,
                'slop_counts_per_api': app_state.slop_counts_per_api,
                'error_counts_per_api': app_state.error_counts_per_api,
                'total_attempts_per_api': app_state.total_attempts_per_api,
                'anti_slop_count_total': app_state.anti_slop_count_total,
                'anti_slop_counts_per_api': anti_slop_counts_per_api,
                'character_counter': app_state.character_counter,
                # Snapshot of critical config settings at the time of saving state
                'config_snapshot': {
                    'prompts.use_questions_file': global_config.get('prompts.use_questions_file'),
                    'generation.num_turns': global_config.get('generation.num_turns', 1),
                    'generation.subject_size': global_config.get('generation.subject_size', 1000),
                    'generation.context_size': global_config.get('generation.context_size', 3000),
                    'api.master_duplication_mode': global_config.get('api.master_duplication_mode', False)
                },
                'quality_scores': {k: v for k, v in app_state.quality_scores.items()},
            }
            # If in master duplication mode and task queue has per-API progress, save it
            # Also save overall progress if not in duplication mode
            if app_state.task_queue:
                if global_config.get('api.master_duplication_mode', False) and hasattr(app_state.task_queue, 'api_processed_tasks'):
                    state_data['api_processed_tasks_snapshot'] = dict(app_state.task_queue.api_processed_tasks)
                elif not global_config.get('api.master_duplication_mode', False) and hasattr(app_state.task_queue, 'processed_tasks'):
                    state_data['processed_tasks_snapshot'] = app_state.task_queue.processed_tasks


            with open(STATE_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, indent=4)
            log_message("Generation state saved.", "INFO")
        except Exception as e:
            log_message(f"Error saving generation state: {e}", "ERROR")


def update_question_history(question, current_history_size):
    """Adds a question to the history and ensures it doesn't exceed the configured size."""
    app_state.question_history.append(question)
    if len(app_state.question_history) > current_history_size:
        app_state.question_history.pop(0) # Remove the oldest question


def estimate_time_remaining(processed_items, total_items, times_list):
    """Estimates the time remaining using a robust trimmed mean for stability."""
    if not times_list or processed_items < 1 or total_items == 0:
        return "Estimating..."

    # Require a minimum number of samples for a reliable estimate
    if len(times_list) < 3:
        return "Estimating..."

    # 1. Filter out extreme outliers to prevent "wonky" jumps
    # We keep only values within 0.5x to 3.0x the median
    sorted_times = sorted(times_list)
    median = sorted_times[len(sorted_times) // 2]
    filtered_times = [t for t in times_list if 0.5 * median <= t <= 3.0 * median]

    # Fallback to original list if filtering accidentally removes everything
    if not filtered_times:
        filtered_times = times_list

    # 2. Use a simple average of the filtered times
    # A filtered mean is significantly smoother for ETAs than a raw EMA
    avg_time_per_item = sum(filtered_times) / len(filtered_times)

    remaining_items = total_items - processed_items
    if remaining_items <= 0:
        return "Done!"

    remaining_time_seconds = remaining_items * avg_time_per_item

    # Format as H:M:S
    return time.strftime('%H:%M:%S', time.gmtime(remaining_time_seconds))

def verify_turn_completion(conversation_history, expected_turns):
    """
    Verify conversation has the expected number of complete turns.
    Each turn should have 1 user + 1 assistant message.
    Returns: (is_complete, actual_turns, expected_turns)
    """
    if not conversation_history:
        return False, 0, expected_turns

    user_messages = sum(1 for msg in conversation_history if msg.get('role') == 'user')
    assistant_messages = sum(1 for msg in conversation_history if msg.get('role') == 'assistant')

    # Each turn should have 1 user + 1 assistant message
    complete_turns = min(user_messages, assistant_messages)

    return complete_turns >= expected_turns, complete_turns, expected_turns


def worker(thread_id, q, output_data_lock, use_questions_file_local,
           use_variable_system_local,
           all_api_configs_local,
           active_enabled_api_configs_for_worker,
           current_question_prompt, current_answer_prompt, current_user_continuation_prompt,
           current_num_turns,
           current_system_prompts_for_worker,
           current_refusal_phrases, current_user_speaking_phrases, current_slop_phrases,
           current_anti_slop_phrases,
           current_anti_slop_fixes,
           current_jailbreaks, current_speaking_fixes, current_slop_fixes_fallback,
           current_max_attempts, current_history_size_local, current_remove_reasoning,
           current_remove_em_dash,
           current_remove_asterisks,
           current_remove_asterisk_space_asterisk,
           current_remove_all_asterisks,
           current_ensure_space_after_line_break,
           current_remove_markdown,
           current_output_format,
           slop_fixer_api_config,
           anti_slop_fixer_api_config_runtime,
           anti_slop_fixer_api_config_param,
           current_slop_fixes_for_rotation_worker,
           current_top_level_system_prompt,
           master_duplication_enabled_local,
           enable_character_engine_local,
           enable_class_selection_local,
           enable_setting_selection_local,
           character_list,
           enable_emotional_states_local,
           emotional_states_list_local,
           num_characters_local,
           no_user_impersonation_local,
           current_api_request_timeout,
           current_slop_to_anti_slop_fallback,
           current_lore):
    """
    The main function executed by each worker thread.
    It fetches tasks from the queue, processes them by interacting with LLMs 
    (handling duplication or collaborative API use based on settings),
    and manages retries, issue detection (refusals, user speaking, slop), and output writing.
    """

    log_message(f"Thread {thread_id}: Worker started.", "DEBUG")

    # Resilience: in non-duplication mode each worker is pinned to one API slot
    # (thread_id % num_active_apis). Capture it up-front so we can pause -- not
    # drain -- the queue when that host's circuit is open. -1 = duplication/unknown.
    worker_api_slot = -1
    if not master_duplication_enabled_local and active_enabled_api_configs_for_worker:
        worker_api_slot = active_enabled_api_configs_for_worker[
            thread_id % len(active_enabled_api_configs_for_worker)]['original_slot_idx']

    while not app_state.stop_processing:
        if check_budget_limit():
            log_message(f"Thread {thread_id}: Budget limit reached. Exiting worker.", "INFO")
            break
        # Enhanced pause check: threads sleep briefly while paused, allowing GUI to remain responsive
        while app_state.pause_processing and not app_state.stop_processing:
            time.sleep(0.1)
            if app_state.stop_processing:
                break
        if app_state.stop_processing: break # Exit if stop signal received during pause

        try:
            task = q.get(timeout=0.05) # Fetch a task from the queue with a short timeout
        except Empty:
            # If queue is empty and all tasks have been added, and processing_active is false (e.g. due to stop signal)
            if q.empty() and getattr(q, 'all_tasks_queued', False) and not app_state.processing_active:
                log_message(f"Thread {thread_id}: Queue empty, all tasks queued, processing not active. Exiting.", "DEBUG")
                break 
            continue # Continue to next iteration if queue is temporarily empty

        log_message(f"Thread {thread_id}: pause_processing={app_state.pause_processing}, stop_processing={app_state.stop_processing}", "DEBUG")

        if task is None: # Sentinel value received, indicating thread should terminate
            q.task_done()
            log_message(f"Thread {thread_id}: Sentinel received. Exiting.", "DEBUG")
            break 
        if app_state.stop_processing: 
            q.task_done(); break # Exit if stop signal received after fetching a task

        task_id, file_name, *_ = task # Unpack task data

        # --- PURGE CHECK ---
        if task_id in app_state.purged_task_ids:
            with app_state.task_queue_ui_lock:
                app_state.pending_task_ids.discard(task_id)
            q.task_done()
            log_message(f"Thread {thread_id}: Skipping purged task {task_id}.", "INFO")
            continue
        # --- END PURGE CHECK ---

        start_time_task_overall = time.time() # For timing the whole task processing

        # ✅ FIX 4: PER-TASK CONVERSATION ISOLATION
        # Each task gets fresh, isolated conversation lists (NOT shared across threads/tasks)
        conversation_history_for_output = []      # Stores full conversation for writing to file
        current_llm_conversation_context = []     # Stores conversation history passed to LLM
        refusal_detected_in_task = False          # Track refusals for this specific task
        any_issue_detected_in_task = False        # Track any issues for this specific task

        initial_user_question = None
        raw_subject_content_for_debug = ""
        raw_context_content_for_debug = ""
        start_time_task_overall = time.time() # For timing the whole task processing

        with output_data_lock:
            if task_id in app_state.completed_task_ids: # Skip if task was already completed (e.g., from a resumed session)
                log_message(f"Thread {thread_id}: Skipping already completed task {task_id}.", "INFO")
                q.task_done()
                # If resuming, need to update progress bars for skipped tasks
                if hasattr(q, 'processed_tasks_lock'):
                    with q.processed_tasks_lock:
                        # A completed task means all its turns are done.
                        # current_num_turns is passed to worker and available here.
                        increment_amount = current_num_turns
                        if master_duplication_enabled_local:
                            # In duplication mode, increment progress for each enabled API (0-3)
                            for api_idx_skip, api_conf_skip in enumerate(all_api_configs_local):
                                if api_idx_skip < 4 and api_conf_skip.get('enabled', False): # Only for enabled generation APIs
                                    if api_idx_skip not in q.api_processed_tasks: q.api_processed_tasks[api_idx_skip] = 0
                                    # Increment by num_turns, ensuring not to over-increment against the new total_tasks_for_progress
                                    target_processed_for_this_api = q.api_processed_tasks[api_idx_skip] + increment_amount
                                    q.api_processed_tasks[api_idx_skip] = min(target_processed_for_this_api, q.total_tasks_for_progress)
                        else:
                            # In non-duplication mode, increment overall progress
                            if not hasattr(q, 'processed_tasks'): q.processed_tasks = 0
                            target_processed_overall = q.processed_tasks + increment_amount
                            q.processed_tasks = min(target_processed_overall, q.total_tasks_for_progress)
                continue

        # --- Resilience gate: if this worker's API host is down (circuit open),
        # put the task back and back off instead of consuming it. This pauses the
        # queue during an outage so the run resumes automatically when the host
        # returns, rather than draining (and discarding) tasks while it's down. ---
        if worker_api_slot >= 0 and not check_circuit_breaker(worker_api_slot):
            q.put(task)
            q.task_done()
            log_message(f"Thread {thread_id}: API Slot {worker_api_slot+1} down (circuit open); "
                        f"requeued task {task_id} and backing off.", "DEBUG")
            time.sleep(min(5.0, max(1.0, API_CIRCUIT_BREAKER['base_cooldown_seconds'] / 4.0)))
            continue

        try:
            if app_state.stop_processing: q.task_done(); continue # Check again before intensive processing

            # Determine system prompt for this task
            current_system_prompt_for_task = ""

            # FIXED: Properly handle system prompt selection with character engine
            if use_variable_system_local and current_system_prompts_for_worker:
                # Use a random system prompt variation
                current_system_prompt_for_task = get_next_system_prompt(current_system_prompts_for_worker)
                log_message(f"Thread {thread_id}: Selected random system prompt variation for task {task_id}", "DEBUG")
            elif current_system_prompts_for_worker and len(current_system_prompts_for_worker) > 0:
                # Use the base system prompt (first in list)
                current_system_prompt_for_task = current_system_prompts_for_worker[0]
                log_message(f"Thread {thread_id}: Using base system prompt for task {task_id}", "DEBUG")
            else: # Fallback if no system prompts are configured
                current_system_prompt_for_task = "You are a helpful assistant."
                log_message(f"Thread {thread_id}: Using fallback system prompt for task {task_id}", "DEBUG")

            # --- NEW: Prepend Top Level System Prompt ---
            if current_top_level_system_prompt:
                current_system_prompt_for_task = current_top_level_system_prompt + "\n\n" + current_system_prompt_for_task
                log_message(f"Thread {thread_id}: Prepending Top Level System Prompt for task {task_id}", "DEBUG")

            # --- NEW: Inject Lore ---
            if current_lore:
                current_system_prompt_for_task = current_system_prompt_for_task.rstrip() + "\n\n--- WORLD LORE ---\n" + current_lore + "\n--- END WORLD LORE ---\n"
                log_message(f"Thread {thread_id}: Injected lore ({len(current_lore)} chars) for task {task_id}", "DEBUG")

            character_injection = ""
            if enable_character_engine_local and character_list:
                # Select multiple characters based on num_characters_local using round-robin
                num_chars_to_select = min(num_characters_local, len(character_list))

                # Safety check: ensure we actually have characters to select
                if num_chars_to_select > 0:
                    # Round-robin selection for even distribution across tasks
                    with app_state.character_counter_lock:
                        start_idx = app_state.character_counter % len(character_list)
                        app_state.character_counter += num_chars_to_select

                    selected_indices = [(start_idx + i) % len(character_list) for i in range(num_chars_to_select)]
                    seen = set()
                    selected_chars = []
                    for idx in selected_indices:
                        if idx not in seen:
                            seen.add(idx)
                            selected_chars.append(character_list[idx])

                    # Fill remaining slots if needed (when num_chars > unique available)
                    remaining_needed = num_chars_to_select - len(selected_chars)
                    if remaining_needed > 0:
                        available = [c for i, c in enumerate(character_list) if i not in seen]
                        if available:
                            selected_chars.extend(random.sample(available, min(remaining_needed, len(available))))

                    character_profiles = []

                    for idx, selected_char in enumerate(selected_chars):
                        # EXPLICIT NAME VALIDATION: Get name, strip whitespace, check if empty
                        random_name = selected_char.get('name', '').strip()

                        if not random_name:
                            log_message(f"Thread {thread_id}: Character {idx+1} has empty/whitespace name. Skipping.", "WARNING")
                            continue

                        # Extract other attributes with defaults
                        random_age = selected_char.get('age', '25')
                        random_gender = selected_char.get('gender', 'Unknown')
                        random_race = selected_char.get('race', 'Unknown')
                        random_job = selected_char.get('job', 'Unknown')
                        random_clothing = selected_char.get('clothing', 'Unknown')
                        random_appearance = selected_char.get('appearance', 'Unknown')
                        random_backstory = selected_char.get('backstory', 'Unknown')
                        random_personality = selected_char.get('personality', 'Unknown')
                        random_setting = selected_char.get('setting', 'Unknown') if enable_setting_selection_local else 'A standard indoor environment.'
                        random_class = selected_char.get('class', '')

                        # Validate Age
                        try:
                            random_age = int(random_age) if random_age else random.randint(18, 60)
                            if random_age < 18 or random_age > 60:
                                random_age = random.randint(18, 60)
                        except (ValueError, TypeError):
                            random_age = random.randint(18, 60)

                        # Build Injections
                        class_injection = f"\nClass: {random_class}\n" if enable_class_selection_local and random_class else ""
                        personality_injection = f"\nPersonality: {random_personality}\n" if random_personality and random_personality != 'Unknown' else ""

                        # Check if names should be included in prompt
                        include_names = global_config.get('prompts.character.include_names_in_prompt', True)

                        # Build name line conditionally
                        name_line = f"Name: {random_name}\n" if include_names else ""

                        character_profile = (
                            f"\n--- CHARACTER {idx+1} PROFILE ---\n"
                            f"{name_line}"
                            f"Gender: {random_gender}\n"
                            f"Race: {random_race}\n"
                            f"Age: {random_age}\n"
                            f"Job: {random_job}\n"
                            f"Clothing: {random_clothing}\n"
                            f"Appearance: {random_appearance}\n"
                            f"Backstory: {random_backstory}\n"
                            f"{personality_injection}"
                            f"Setting: {random_setting}\n" if not enable_setting_selection_local else ""
                            f"{class_injection}"
                            f"--- END CHARACTER {idx+1} PROFILE ---\n"
                        )
                        character_profiles.append(character_profile)

                    # Finalize Injection
                    if character_profiles:
                        character_injection = "\n\nMULTI-CHARACTER CONVERSATION MODE:\n" + "\n".join(character_profiles)
                        character_injection += "\nMaintain all character personas throughout the conversation. Each character should have distinct voices and personalities.\n"
                        log_message(f"Thread {thread_id}: Successfully added {len(character_profiles)} character profiles for task {task_id}", "DEBUG")
                    else:
                        log_message(f"Thread {thread_id}: Character engine enabled but no VALID characters selected (all had empty names).", "WARNING")
                else:
                    log_message(f"Thread {thread_id}: Character engine enabled but character_list is empty or num_characters is 0.", "WARNING")

            current_system_prompt_for_task += character_injection

            # Handle emotional states
            current_emotional_state = ""
            if enable_emotional_states_local and emotional_states_list_local:
                current_emotional_state = random.choice(emotional_states_list_local)
                emotional_state_injection = (
                    f"\n\nEMOTIONAL STATE: {current_emotional_state.upper()}\n"
                    f"Express this emotional state throughout your responses. "
                    f"Use appropriate tone, word choice, and emotional expression that reflects {current_emotional_state} feelings.\n"
                )
                current_system_prompt_for_task += emotional_state_injection
                log_message(f"Thread {thread_id}: Assigned emotional state '{current_emotional_state}' for task {task_id}", "DEBUG")

            conversation_history_for_output = [] # Stores the full conversation for writing (mainly for non-duplication mode)
            current_llm_conversation_context = [] # Stores the conversation history passed to the LLM for context
            refusal_detected_in_task = False

            initial_user_question = None
            raw_subject_content_for_debug = "" # For debug logging
            raw_context_content_for_debug = "" # For debug logging

            # --- API Selection Logic ---
            api_config_for_this_task = None # Holds the config for the API used by this task/thread in non-duplication
            api_slot_idx_for_this_task = -1 # Original slot index of the API used in non-duplication

            if not master_duplication_enabled_local:
                # Non-duplication mode: assign an API to this thread for this task based on thread_id
                if active_enabled_api_configs_for_worker: # List of {config, original_slot_idx}
                    selected_api_details = active_enabled_api_configs_for_worker[thread_id % len(active_enabled_api_configs_for_worker)]
                    api_config_for_this_task = selected_api_details['config']
                    api_slot_idx_for_this_task = selected_api_details['original_slot_idx']
                    log_message(f"Thread {thread_id} (Non-Duplication): Assigned API Slot {api_slot_idx_for_this_task+1} for task {task_id}", "DEBUG")
                else: # Should not happen if start_processing validates correctly
                    log_message(f"Thread {thread_id}: CRITICAL - No active/enabled APIs for non-duplication mode. Skipping task.", "ERROR")
                    q.task_done(); continue
            # In Duplication mode, api_config_for_this_task and api_slot_idx_for_this_task are not used directly for answers.
            # Instead, the answer generation loop iterates through all_api_configs_local.
            # Question/Continuation generation in duplication mode uses the primary API (slot 0).

            # --- Initial Question Generation ---
            if use_questions_file_local:
                question_as_segment = task[3] # The question text is part of the task tuple
                initial_user_question = question_as_segment 
            else: # Generate question from subject/context
                if app_state.stop_processing: q.task_done(); continue
                subject_content_for_task = task[3]
                context_content_for_task = task[4]
                raw_subject_content_for_debug = subject_content_for_task 
                raw_context_content_for_debug = context_content_for_task 
                
                # Determine API for question generation: primary (slot 0) in duplication, or assigned API in non-duplication
                q_gen_api_conf = all_api_configs_local[0] if master_duplication_enabled_local else api_config_for_this_task
                q_gen_api_slot_idx = 0 if master_duplication_enabled_local else api_slot_idx_for_this_task

                initial_user_question = generate_question(
                    current_system_prompt_for_task, current_question_prompt,
                    subject_content_for_task, context_content_for_task,
                    thread_id, q_gen_api_conf.get('sampler_settings', {}),
                    q_gen_api_conf.get('url'), q_gen_api_conf.get('model'), q_gen_api_conf.get('key'),
                    current_history_size_local,
                    raw_subject_content_for_debug, raw_context_content_for_debug,
                    api_slot_idx=q_gen_api_slot_idx,
                    current_max_attempts_param=current_max_attempts,
                    api_request_timeout_param=current_api_request_timeout,
                    file_name=file_name
                )
            
            if not initial_user_question:
                log_message(f"Thread {thread_id}: Failed to generate initial question for task {task_id}. Skipping.", "ERROR")
                q.task_done(); continue

            current_llm_conversation_context.append({"role": "user", "content": initial_user_question})
            # For non-duplication mode, this will be part of the final output history
            if not master_duplication_enabled_local:
                conversation_history_for_output.append({"role": "user", "content": initial_user_question})


            # --- Multi-Turn Conversation Loop ---
            for turn_num in range(current_num_turns):
                if app_state.stop_processing or app_state.pause_processing: break # Check before starting a new turn

                assistant_answer = None # This will hold the assistant's response for the current turn

                # --- Assistant Answer Generation ---
                if master_duplication_enabled_local:
                    primary_api_answer_for_conv_flow = None # Answer from primary/first successful API to drive conversation flow
                    all_duplicated_answers_for_output = [] # Stores (answer_text, original_api_slot_idx) for writing

                    # Iterate through enabled APIs (Slots 1-4, indices 0-3) for duplication
                    for dup_api_idx, dup_api_conf_item in enumerate(all_api_configs_local):
                        if dup_api_idx < 4 and dup_api_conf_item.get('enabled', False): 
                            if app_state.stop_processing or app_state.pause_processing: break
                            log_message(f"Thread {thread_id}, Task {task_id}, Turn {turn_num+1}: Duplicating with API Slot {dup_api_idx+1}", "DEBUG")
                            start_time_api_task = time.time()
                            
                            answer_result = generate_answer_with_retries(
                                base_system_prompt=current_system_prompt_for_task,
                                conversation_history_for_llm=list(current_llm_conversation_context),
                                answer_prompt_template=current_answer_prompt,
                                thread_id=thread_id, q=q,
                                sampler_settings_local=dup_api_conf_item.get('sampler_settings', {}),
                                api_url_local=dup_api_conf_item.get('url'),
                                model_name_local=dup_api_conf_item.get('model'),
                                api_key_local=dup_api_conf_item.get('key'),
                                refusal_phrases_local=current_refusal_phrases,
                                user_speaking_phrases_local=current_user_speaking_phrases,
                                slop_phrases_local=current_slop_phrases,
                                current_anti_slop_phrases_param=current_anti_slop_phrases,
                                jailbreaks_local=current_jailbreaks,
                                speaking_fixes_local=current_speaking_fixes,
                                slop_fixes_fallback_local=current_slop_fixes_fallback,
                                max_attempts_local=current_max_attempts,
                                slop_fixer_api_config_param=slop_fixer_api_config,
                                current_slop_fixes_for_rotation_param=current_slop_fixes_for_rotation_worker,
                                api_slot_idx=dup_api_idx,
                                current_max_attempts_for_slop_fixer_call=current_max_attempts,
                                master_duplication_enabled_local=master_duplication_enabled_local,
                                no_user_impersonation_local=no_user_impersonation_local,
                                anti_slop_fixer_api_config_param=anti_slop_fixer_api_config_param,
                                api_request_timeout_param=current_api_request_timeout,
                                file_name=file_name,
                                slop_to_anti_slop_fallback=current_slop_to_anti_slop_fallback,
                            )
                            if answer_result and answer_result[0]:  # Check if answer is not None
                                duplicated_answer_text = answer_result[0]
                                issue_in_this_call = answer_result[1] if len(answer_result) > 1 else False
                                refusal_in_this_call = answer_result[2] if len(answer_result) > 2 else False

                                # Track if issue was detected in this task
                                if issue_in_this_call:
                                    any_issue_detected_in_task = True
                                if refusal_in_this_call:
                                    refusal_detected_in_task = True
                                    log_message(f"Thread {thread_id}: Task {task_id}, Turn {turn_num+1}: Issue detected (refusal/user_speak/slop). Setting refusal_detected_in_task=True", "WARNING")
                            end_time_api_task = time.time()
                            api_task_duration = end_time_api_task - start_time_api_task

                            if answer_result and answer_result[0]:  # Check if answer is not None
                                duplicated_answer_text = answer_result[0]
                                refusal_in_this_call = answer_result[1] if len(answer_result) > 1 else False

                                # Track if refusal was detected in this task
                                if refusal_in_this_call:
                                    refusal_detected_in_task = True
                                all_duplicated_answers_for_output.append((duplicated_answer_text, dup_api_idx))
                                # Update progress for this specific API in duplication mode
                                with q.processed_tasks_lock:
                                    if dup_api_idx not in q.api_processed_tasks: q.api_processed_tasks[dup_api_idx] = 0
                                    # Increment per successful turn generation
                                    # Ensure not to exceed total_tasks_for_progress for this API
                                    if q.api_processed_tasks[dup_api_idx] < q.total_tasks_for_progress:
                                        q.api_processed_tasks[dup_api_idx] += 1 
                                    if dup_api_idx not in q.api_start_times_list: q.api_start_times_list[dup_api_idx] = []
                                    q.api_start_times_list[dup_api_idx].append(api_task_duration)
                                    if len(q.api_start_times_list[dup_api_idx]) > 50: q.api_start_times_list[dup_api_idx].pop(0)
                                
                                # Determine which answer drives the conversation flow (primary or first successful)
                                if dup_api_idx == 0 and duplicated_answer_text: # Primary API (Slot 1) successful
                                    primary_api_answer_for_conv_flow = duplicated_answer_text
                                elif not primary_api_answer_for_conv_flow and duplicated_answer_text: # Fallback to first other successful API
                                    primary_api_answer_for_conv_flow = duplicated_answer_text
                            else:
                                log_message(f"Thread {thread_id}, Task {task_id}, Turn {turn_num+1}: API Slot {dup_api_idx+1} failed to generate answer.", "WARNING")
                        if app_state.stop_processing or app_state.pause_processing: break # Check inside duplication loop
                    
                    assistant_answer = primary_api_answer_for_conv_flow # Use this for the main conversation flow

                    # Write all successful duplicated answers to their respective per-API files for this turn
                    if assistant_answer: # Only proceed if at least one API gave an answer to continue the flow
                        for ans_text, original_slot_idx_for_file in all_duplicated_answers_for_output:
                            # CRITICAL FIX: Check if this specific answer had a refusal
                            # We need to track refusals per API call, not just overall
                            # For now, skip writing if any refusal was detected in this task
                            if not refusal_detected_in_task:
                                # Create a temporary history for this specific API's output for this turn
                                temp_conv_history_for_api_output_turn = list(current_llm_conversation_context) # Contains user's current message
                                temp_conv_history_for_api_output_turn.append({"role": "assistant", "content": ans_text})
                                with output_data_lock:
                                    write_conversation(None, temp_conv_history_for_api_output_turn, current_remove_reasoning,
                                                    current_remove_em_dash,
                                                    current_remove_asterisks, # NEW
                                                    current_remove_asterisk_space_asterisk,  # NEW ADDITION
                                                    current_remove_all_asterisks,  # NEW ADDITION
                                                    current_ensure_space_after_line_break, # NEW
                                                    current_remove_markdown,
                                                    current_output_format, task_id,
                                                    api_slot_idx_for_output_file=original_slot_idx_for_file, # Write to specific API's file
                                                    is_duplication_turn=True, turn_number_for_duplication=turn_num + 1) # Mark as duplication turn
                            else:
                                log_message(f"Thread {thread_id}: Skipping turn {turn_num+1} output for API Slot {original_slot_idx_for_file+1} due to previous refusal in task {task_id}", "DEBUG")
                    # --- Quality Scoring (Duplication Mode: score the primary flow) ---
                    if app_state.quality_enabled and assistant_answer:
                        try:
                            primary_conv_for_scoring = list(current_llm_conversation_context)
                            primary_conv_for_scoring.append({"role": "assistant", "content": assistant_answer})
                            quality_result = quality.score_conversation(
                                primary_conv_for_scoring,
                                task_id,
                                thread_id,
                                api_slot_idx=0,
                                file_name=file_name
                            )
                            with app_state.quality_lock:
                                # Store with task_id (overwrites per-API scores with primary)
                                if task_id not in app_state.quality_scores:
                                    app_state.quality_scores[task_id] = quality_result

                            # ✅ FIX: Flag for review if below threshold (was missing entirely)
                            min_threshold = global_config.get('quality.min_score_threshold', 50)
                            if quality_result['composite'] < min_threshold:
                                with app_state.quality_review_lock:
                                    app_state.quality_review_ids.add(task_id)
                                log_message(
                                    f"Thread {thread_id}: Task {task_id} flagged for review "
                                    f"(score {quality_result['composite']} < threshold {min_threshold}).",
                                    "WARNING"
                                )

                            log_message(
                                f"Thread {thread_id}: Quality score (dup, primary) for {task_id}: "
                                f"{quality_result['composite']}/100",
                                "INFO"
                            )
                        except Exception as e_quality:
                            log_message(f"Thread {thread_id}: Quality scoring error (dup) for {task_id}: {e_quality}", "ERROR")
                        # Note: completed_task_ids.add() and save_generation_state() are handled once per task_id at the end of the worker.
                
                else: # --- Non-Duplication Mode: Single API call for answer ---
                    if app_state.stop_processing or app_state.pause_processing: break
                    start_time_api_task = time.time()
                    answer_result = generate_answer_with_retries(
                        base_system_prompt=current_system_prompt_for_task,
                        conversation_history_for_llm=list(current_llm_conversation_context),
                        answer_prompt_template=current_answer_prompt,
                        thread_id=thread_id, q=q,
                        sampler_settings_local=api_config_for_this_task.get('sampler_settings', {}),
                        api_url_local=api_config_for_this_task.get('url'),
                        model_name_local=api_config_for_this_task.get('model'),
                        api_key_local=api_config_for_this_task.get('key'),
                        refusal_phrases_local=current_refusal_phrases,
                        user_speaking_phrases_local=current_user_speaking_phrases,
                        slop_phrases_local=current_slop_phrases,
                        current_anti_slop_phrases_param=current_anti_slop_phrases,
                        jailbreaks_local=current_jailbreaks,
                        speaking_fixes_local=current_speaking_fixes,
                        slop_fixes_fallback_local=current_slop_fixes_fallback,
                        max_attempts_local=current_max_attempts,
                        slop_fixer_api_config_param=slop_fixer_api_config,
                        current_slop_fixes_for_rotation_param=current_slop_fixes_for_rotation_worker,
                        api_slot_idx=api_slot_idx_for_this_task,
                        current_max_attempts_for_slop_fixer_call=current_max_attempts,
                        master_duplication_enabled_local=master_duplication_enabled_local,
                        no_user_impersonation_local=no_user_impersonation_local,
                        anti_slop_fixer_api_config_param=anti_slop_fixer_api_config_param,
                        api_request_timeout_param=current_api_request_timeout,
                        slop_to_anti_slop_fallback=current_slop_to_anti_slop_fallback,
                        file_name=file_name,
                    )
                    if answer_result and answer_result[0]:  # Check if answer is not None
                        assistant_answer = answer_result[0]
                        issue_in_this_call = answer_result[1] if len(answer_result) > 1 else False
                        refusal_in_this_call = answer_result[2] if len(answer_result) > 2 else False

                        # Track if issue was detected in this task (refusal, user speaking, slop, OR anti-slop)
                        if issue_in_this_call:
                            any_issue_detected_in_task = True
                        if refusal_in_this_call:
                            refusal_detected_in_task = True
                            log_message(f"Thread {thread_id}: Task {task_id}, Turn {turn_num+1}: Issue detected (refusal/user_speak/slop). Setting refusal_detected_in_task=True", "WARNING")
                    end_time_api_task = time.time()
                    api_task_duration = end_time_api_task - start_time_api_task

                    if answer_result and answer_result[0]:  # Check if answer is not None
                        assistant_answer = answer_result[0]
                        refusal_in_this_call = answer_result[1] if len(answer_result) > 1 else False

                        # Track if refusal was detected in this task
                        if refusal_in_this_call:
                            refusal_detected_in_task = True
                        # Update overall progress for non-duplication mode (per turn)
                        turn_incremented = False
                        retry_count = 0
                        while not turn_incremented and retry_count < 5:
                            lock_acquired = q.processed_tasks_lock.acquire(timeout=1.0)
                            if lock_acquired:
                                try:
                                    if not hasattr(q, 'processed_tasks'): q.processed_tasks = 0
                                    if q.processed_tasks < q.total_tasks_for_progress:
                                        q.processed_tasks += 1
                                        turn_incremented = True
                                finally:
                                    q.processed_tasks_lock.release()
                            else:
                                retry_count += 1
                                time.sleep(0.1)

                        if not turn_incremented:
                            log_message(f"Thread {thread_id}: CRITICAL - Failed to increment turn counter after 5 retries!", "ERROR")
                            if not hasattr(q, 'start_times_list'): q.start_times_list = []
                            q.start_times_list.append(api_task_duration)
                            if len(q.start_times_list) > 50: q.start_times_list.pop(0)
                        
                        # In non-duplication, add the assistant's answer to the main conversation history for output
                        conversation_history_for_output.append({"role": "assistant", "content": assistant_answer})

                # --- End of Assistant Answer Generation for the turn ---

                if not assistant_answer: # If no answer (primary failed in dup, or single API failed in non-dup)
                    log_message(f"Thread {thread_id}: Failed to get any assistant answer for turn {turn_num + 1} of task {task_id}. Ending this conversation.", "ERROR")
                    break # End this task's conversation

                current_llm_conversation_context.append({"role": "assistant", "content": assistant_answer})
                # conversation_history_for_output is handled above based on duplication mode for this turn's assistant answer.

                if turn_num == current_num_turns - 1: # If this was the last turn
                    break 
                if app_state.stop_processing or app_state.pause_processing: break

                # --- User Continuation Generation (if not the last turn) ---
                if not current_user_continuation_prompt: 
                    log_message(f"Thread {thread_id}: No user continuation prompt set. Ending conversation after assistant's turn {turn_num + 1}.", "INFO")
                    break
                
                # Determine API for user continuation: primary (slot 0) in duplication, or assigned API in non-duplication
                cont_gen_api_conf = all_api_configs_local[0] if master_duplication_enabled_local else api_config_for_this_task
                cont_gen_api_slot_idx = 0 if master_duplication_enabled_local else api_slot_idx_for_this_task

                user_continuation_reply = generate_user_continuation(
                    system_prompt=current_system_prompt_for_task,
                    conversation_history_for_llm=list(current_llm_conversation_context),
                    user_continuation_prompt_template=current_user_continuation_prompt,
                    thread_id=thread_id,
                    sampler_settings_local=cont_gen_api_conf.get('sampler_settings', {}),
                    api_url_local=cont_gen_api_conf.get('url'),
                    model_name_local=cont_gen_api_conf.get('model'),
                    api_key_local=cont_gen_api_conf.get('key'),
                    api_slot_idx=cont_gen_api_slot_idx,
                    current_max_attempts_param=current_max_attempts,
                    api_request_timeout_param=current_api_request_timeout,
                    file_name=file_name,
                )

                if not user_continuation_reply:
                    log_message(f"Thread {thread_id}: Failed to get user continuation for turn {turn_num + 1} of task {task_id}. Ending this conversation.", "ERROR")
                    break 
                
                current_llm_conversation_context.append({"role": "user", "content": user_continuation_reply})
                # In non-duplication, add user's continuation to the main history for final output
                if not master_duplication_enabled_local:
                    conversation_history_for_output.append({"role": "user", "content": user_continuation_reply})
            
            # --- End of Multi-Turn Loop ---
            if app_state.stop_processing: q.task_done(); continue 

            # --- Write Completed Conversation (Non-Duplication Mode) or Mark Task Complete (Duplication Mode) ---
            if not master_duplication_enabled_local:
                # In non-duplication mode, conversation_history_for_output contains the full conversation.
                if conversation_history_for_output and len(conversation_history_for_output) >= 2: # Ensure at least one Q/A pair
                    # CRITICAL FIX: Don't save if a refusal was detected at any point
                    if refusal_detected_in_task:
                        log_message(f"Thread {thread_id}: Task {task_id} contained a refusal. NOT saving to output.jsonl.", "WARNING")
                        # Do NOT add to completed_task_ids - this task should not be marked complete
                        # Requeue the task for retry
                        if requeue_task(q, task, task_id):
                            log_message(f"Thread {thread_id}: Requeued task {task_id} due to refusal detection", "WARNING")
                        else:
                            log_message(f"Thread {thread_id}: Task {task_id} exceeded {MAX_TASK_REQUEUES} requeues; giving up.", "ERROR")
                            with app_state.task_queue_ui_lock:
                                app_state.pending_task_ids.discard(task_id)
                                app_state.failed_task_ids.add(task_id)
                    else:
                        # ✅ VERIFY TURN COUNT BEFORE MARKING COMPLETE
                        expected_messages = current_num_turns * 2
                        actual_messages = len(conversation_history_for_output)

                        if actual_messages >= expected_messages:
                            with output_data_lock:
                                if task_id not in app_state.completed_task_ids:
                                    write_conversation(None, conversation_history_for_output, current_remove_reasoning,
                                        current_remove_em_dash, current_remove_asterisks,
                                        current_remove_asterisk_space_asterisk, current_remove_all_asterisks,
                                        current_ensure_space_after_line_break, current_remove_markdown,
                                        current_output_format, task_id, api_slot_idx_for_output_file=None)
                                    app_state.completed_task_ids.add(task_id)
                                    log_message(f"Thread {thread_id}: Task {task_id} marked complete ({actual_messages}/{expected_messages} messages)", "INFO")
                                    # --- Quality Scoring ---
                                    if app_state.quality_enabled:
                                        try:
                                            quality_result = quality.score_conversation(
                                                conversation_history_for_output,
                                                task_id,
                                                thread_id,
                                                api_slot_idx=api_slot_idx_for_this_task,
                                                file_name=file_name
                                            )
                                            with app_state.quality_lock:
                                                app_state.quality_scores[task_id] = quality_result
                                            # Flag for review if below threshold
                                            min_threshold = global_config.get('quality.min_score_threshold', 50)
                                            if quality_result['composite'] < min_threshold:
                                                with app_state.quality_review_lock:
                                                    app_state.quality_review_ids.add(task_id)
                                            log_message(
                                                f"Thread {thread_id}: Quality score for {task_id}: "
                                                f"{quality_result['composite']}/100 "
                                                f"(method: {quality_result['method']}, flags: {quality_result['flags']})",
                                                "INFO"
                                            )

                                            # Optional: filter output by quality threshold
                                            if app_state.quality_output_filter:
                                                min_threshold = global_config.get('quality.min_score_threshold', 50)
                                                if quality_result['composite'] < min_threshold:
                                                    log_message(
                                                        f"Thread {thread_id}: Task {task_id} score {quality_result['composite']} "
                                                        f"below threshold {min_threshold}. Marking as quality-rejected.",
                                                        "WARNING"
                                                    )
                                                    # The conversation was already written, but we flag it.
                                                    # If you want to DELETE it from the output file, you'd need
                                                    # a post-processing step. For now, the flag is stored.
                                        except Exception as e_quality:
                                            log_message(f"Thread {thread_id}: Quality scoring error for {task_id}: {e_quality}", "ERROR")
                                else:
                                    log_message(f"Thread {thread_id}: Task {task_id} already marked complete, skipping", "DEBUG")
                            # Update UI tracking on success
                            with app_state.task_queue_ui_lock:
                                app_state.pending_task_ids.discard(task_id)
                            save_generation_state()
                        else:
                            log_message(f"Thread {thread_id}: Task {task_id} incomplete. Expected {expected_messages} messages, got {actual_messages}. NOT saving to output.jsonl.", "WARNING")
                            # Do NOT add to completed_task_ids - this task should be retried.
                            # If the host is down, requeue it so it's retried once the host recovers
                            if api_host_is_down(worker_api_slot):
                                if requeue_task(q, task, task_id):
                                    log_message(f"Thread {thread_id}: API Slot {worker_api_slot+1} down; requeued incomplete task {task_id} for retry.", "WARNING")
                                else:
                                    log_message(f"Thread {thread_id}: Task {task_id} exceeded {MAX_TASK_REQUEUES} requeues; giving up.", "ERROR")
                            else:
                                # Requeue anyway for any incomplete task
                                if requeue_task(q, task, task_id):
                                    log_message(f"Thread {thread_id}: Requeued incomplete task {task_id} ({actual_messages}/{expected_messages} messages)", "WARNING")
                else:
                    log_message(f"Thread {thread_id}: No valid conversation generated for task {task_id} (API Slot {api_slot_idx_for_this_task+1}). Not writing to output.", "WARNING")
                    # Requeue task
                    if requeue_task(q, task, task_id):
                        log_message(f"Thread {thread_id}: Requeued task {task_id} - no valid conversation", "WARNING")
                    else:
                        log_message(f"Thread {thread_id}: Task {task_id} exceeded {MAX_TASK_REQUEUES} requeues; giving up.", "ERROR")
                        with app_state.task_queue_ui_lock:
                            app_state.pending_task_ids.discard(task_id)
                            app_state.failed_task_ids.add(task_id)

        except Exception as e: # Catch-all for errors during task processing
            error_message_gen = f"Thread {thread_id}: Error processing task {task_id} from {file_name}: {str(e)}"
            log_message(error_message_gen, "ERROR")
            import traceback
            log_message(traceback.format_exc(), "ERROR") 
            # Record this as a general error for the task
            with output_data_lock: # Use the lock for modifying global error counters
                app_state.error_count_total +=1 
                # For general task errors, we don't assign to a specific API unless the error originated there.
                # Here, it's a task-level error, so log it for the "Totals" dashboard.
                err_summary = f"T{thread_id} TaskErr: {str(e)[:30]}" # Short summary
                if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                app_state.recent_errors_total.append((err_summary, -1)) # -1 indicates a general task error not tied to a specific API call error
        
        end_time_task_overall = time.time()
        task_duration_overall = end_time_task_overall - start_time_task_overall

        # In duplication mode, we track overall task time for a general estimate,
        # though per-API times are more granular for progress bars.
        if master_duplication_enabled_local:
             with q.processed_tasks_lock: # This lock is also used for overall_task_times_list
                if not hasattr(q, 'overall_task_times_list'): q.overall_task_times_list = []
                q.overall_task_times_list.append(task_duration_overall)
                if len(q.overall_task_times_list) > 50 : q.overall_task_times_list.pop(0)
                # This doesn't directly update a progress bar but could be used for overall ETA if needed.

        q.task_done() # Signal that this task is complete

    log_message(f"Thread {thread_id} completed its run.", "INFO")


def get_next_system_prompt(prompts_list_local):
    """Selects the next system prompt in round-robin order for even distribution."""
    if not prompts_list_local:
        return "You are a helpful assistant."  # Fallback

    # Use the thread-safe counter for round-robin cycling
    with app_state.system_prompt_lock:
        idx = app_state.system_prompt_counter % len(prompts_list_local)
        app_state.system_prompt_counter += 1
        selected = prompts_list_local[idx]

    return selected


def generate_question(system_prompt, question_prompt_template, subject, context, thread_id,
                      sampler_settings_local, api_url_local, model_name_local, api_key_local,
                      history_size_local_param,
                      raw_subject_chunk, raw_context_chunk,
                      api_slot_idx, current_max_attempts_param, api_request_timeout_param,
                      file_name=""):
    """Generates an initial question using the LLM, with retries for API call failures."""

    if not api_url_local:
        log_message(f"Thread {thread_id}: API URL missing for question generation (API Slot {api_slot_idx+1}). Cannot proceed.", "ERROR")
        return None

    for attempt_num in range(current_max_attempts_param):
        if app_state.stop_processing or app_state.pause_processing:
            return None
            MAX_TOTAL_RETRY_WAIT = 20
            current_attempt_wait = 0

        # FIX: Use stats_lock instead of system_prompt_lock, with timeout
        lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.50)
        if lock_acquired:
            try:
                app_state.total_attempts_global += 1
                app_state.total_attempts_per_api[api_slot_idx] += 1
            finally:
                app_state.system_prompt_lock.release()
        else:
            log_message(f"Thread {thread_id}: Skipped stat update (system_prompt_lock busy)", "DEBUG")

        try:
            # FIX: Use question_history_lock with timeout
            recent_questions_str = ""
            lock_acquired_qh = app_state.question_history_lock.acquire(timeout=0.05)
            if lock_acquired_qh:
                try:
                    recent_questions_str = "\n- ".join(app_state.question_history[-history_size_local_param:]) if app_state.question_history else "None"
                finally:
                    app_state.question_history_lock.release()
            else:
                log_message(f"Thread {thread_id}: WARNING - Could not acquire question_history_lock. Using empty history.", "WARNING")
                recent_questions_str = "None"

            # Format the question prompt with placeholders
            final_formatted_user_prompt = question_prompt_template.replace("{recent_questions}", recent_questions_str)
            final_formatted_user_prompt = final_formatted_user_prompt.replace("{subject}", subject if subject else "N/A")
            final_formatted_user_prompt = final_formatted_user_prompt.replace("{context}", context if context else "N/A")

            messages_for_llm = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": final_formatted_user_prompt}
            ]

            update_live_prompt_preview(messages_for_llm, {
                'thread_id': thread_id,
                'api_slot_idx': api_slot_idx + 1,
                'message_type': 'Question',
                'attempt': attempt_num + 1,
                'timestamp': time.strftime('%H:%M:%S'),
            })

            # Prepare payload for LLM API
            payload_dict = {
                "model": model_name_local,
                "messages": messages_for_llm,
                **sampler_settings_local.get("generation_params", {
                    "temperature": sampler_settings_local.get("temperature",0.7),
                    "top_p": sampler_settings_local.get("top_p",0.9),
                    "top_k": sampler_settings_local.get("top_k",50),
                    "repetition_penalty": sampler_settings_local.get("repetition_penalty",1.1),
                    "max_tokens": sampler_settings_local.get("max_tokens_question", global_config.get('samplers.max_tokens_question', 256))
                }),
                "stream": False
            }
            logit_bias_str = sampler_settings_local.get('logit_bias', '')
            if logit_bias_str:
                try:
                    payload_dict['logit_bias'] = json.loads(logit_bias_str)
                except json.JSONDecodeError:
                    log_message(f"Thread {thread_id}: Invalid logit_bias JSON. Skipping.", "WARNING")
            thinking_mode = sampler_settings_local.get('enable_thinking', 'default')
            if thinking_mode == 'enable':
                payload_dict['chat_template_kwargs'] = {"enable_thinking": True}
            elif thinking_mode == 'disable':
                payload_dict['chat_template_kwargs'] = {"enable_thinking": False}
            # else 'default': do not send the parameter

            payload_dict = sanitize_payload_for_endpoint(payload_dict, api_url_local, api_slot_idx)
            payload = json.dumps(payload_dict)
            headers = {
                'Content-Type': 'application/json'
            }
            if api_key_local:
                headers['Authorization'] = f"Bearer {api_key_local}"

            current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{api_slot_idx}.jsonl" if app_state.master_duplication_enabled_var.get() else BASE_DEBUG_LOG_PATH + ".jsonl"

            debug_log_entry = {
                "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "thread_id": thread_id, "type": "question_request", "api_slot_idx": api_slot_idx, "attempt": attempt_num + 1,
                "api_url": api_url_local, "model": model_name_local,
                "source_file": file_name,
                "raw_subject_chunk_length": len(raw_subject_chunk),
                "raw_context_chunk_length": len(raw_context_chunk),
                "messages": messages_for_llm,
                "sampler_settings": sampler_settings_local,
                "payload_dict": payload_dict
            }
            with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
                debug_log.write(json.dumps(debug_log_entry) + '\n')

            # Apply rate limiting before making the API call
            global_rate_limiter.wait_if_needed(api_slot_idx)

            # Circuit Breaker Check
            if not check_circuit_breaker(api_slot_idx):
                log_message(f"Thread {thread_id}: API Slot {api_slot_idx+1} circuit open. Skipping question generation.", "DEBUG")
                return None

            # Track API response time
            api_call_start_time = time.time()

            # Create a unique hash of the prompt to use as a cache key
            prompt_content = json.dumps(messages_for_llm, sort_keys=True)
            prompt_hash = hashlib.md5(prompt_content.encode()).hexdigest()

            # Check cache
            cached_response, is_cached = get_cached_response(prompt_hash, api_slot_idx)
            if is_cached:
                generated_question_text = cached_response
                # Update question history if cached
                lock_acquired_qh_update = app_state.question_history_lock.acquire(timeout=7.0)
                if lock_acquired_qh_update:
                    try:
                        update_question_history(generated_question_text, history_size_local_param)
                    finally:
                        app_state.question_history_lock.release()
                return generated_question_text

            response = requests.post(api_url_local, headers=headers, data=payload, timeout=(api_request_timeout_param, api_request_timeout_param))

            api_response_time = time.time() - api_call_start_time

            # Store response time (thread-safe)
            with api_response_times_lock:
                api_response_times_per_slot[api_slot_idx].append(api_response_time)
                if len(api_response_times_per_slot[api_slot_idx]) > MAX_RESPONSE_TIMES_TO_TRACK:
                    api_response_times_per_slot[api_slot_idx] = api_response_times_per_slot[api_slot_idx][-MAX_RESPONSE_TIMES_TO_TRACK:]

            if response.status_code == 200:
                record_api_success(api_slot_idx)
                response_data = response.json()
                content = response_data['choices'][0]['message'].get('content')

                # NEW: Extract token usage if available
                usage = response_data.get('usage', {})
                input_tokens = usage.get('prompt_tokens', 0)
                output_tokens = usage.get('completion_tokens', 0)

                # Update global counters with timeout
                lock_acquired_tokens = app_state.stats_lock.acquire(timeout=7.0)
                if lock_acquired_tokens:
                    try:
                        app_state.total_input_tokens += input_tokens
                        app_state.total_output_tokens += output_tokens
                    finally:
                        app_state.stats_lock.release()
                else:
                    log_message(f"Thread {thread_id}: WARNING - Could not acquire stats_lock for token update.", "WARNING")

                if content is None:
                    log_message(f"Thread {thread_id}: API returned None for content (API Slot {api_slot_idx+1}, Attempt {attempt_num+1})", "WARNING")
                    if attempt_num < current_max_attempts_param - 1:
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None
                generated_question_text = content.strip()
                newline_count = generated_question_text.count('\n')
                text_length = len(generated_question_text)

                max_newlines = global_config.get('generation.max_newlines_malformed', 16)
                max_text_length = global_config.get('generation.max_text_length_malformed', 5000)

                if newline_count > max_newlines or text_length > max_text_length:
                    log_message(
                        f"Thread {thread_id}: Question response appears malformed. "
                        f"Newlines: {newline_count} (Max: {max_newlines}), Length: {text_length} (Max: {max_text_length}). "
                        f"Snippet: '{generated_question_text[:100]}...'",
                        "WARNING"
                    )
                    if attempt_num < current_max_attempts_param - 1:
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None
                generated_question_text = content.strip()
                if not generated_question_text or len(generated_question_text) < 5:
                    log_message(f"Thread {thread_id}: API returned empty/very short question. Content: '{generated_question_text}'", "WARNING")
                    if attempt_num < current_max_attempts_param - 1:
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None

                # Update question history with timeout
                lock_acquired_qh_update = app_state.question_history_lock.acquire(timeout=7.0)
                if lock_acquired_qh_update:
                    try:
                        update_question_history(generated_question_text, history_size_local_param)
                        set_cached_response(prompt_hash, api_slot_idx, generated_question_text)
                        log_message(f"Thread {thread_id}: Cache SET for API Slot {api_slot_idx+1}.", "DEBUG")
                    finally:
                        app_state.question_history_lock.release()
                return generated_question_text
            else: # API call failed
                error_message = f"Thread {thread_id}: Error generating question (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param}, Status: {response.status_code}): {response.text[:200]}"
                log_message(error_message, "ERROR")
                record_api_failure(api_slot_idx)
                # Update error counters with timeout
                lock_acquired_err = app_state.stats_lock.acquire(timeout=7.0)
                if lock_acquired_err:
                    try:
                        app_state.error_count_total += 1
                        app_state.error_counts_per_api[api_slot_idx] += 1
                        err_summary = f"T{thread_id} Q-Err (API{api_slot_idx+1}): S{response.status_code} A{attempt_num+1}"
                        if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                        app_state.recent_errors_total.append((err_summary, api_slot_idx))
                        with app_state.issue_timestamps_lock:
                            app_state.issue_timestamps['errors'].append(time.time())
                            cutoff = time.time() - 3600
                            app_state.issue_timestamps['errors'] = [t for t in app_state.issue_timestamps['errors'] if t > cutoff]
                        if api_slot_idx < 6 :
                            if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                            app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                    finally:
                        app_state.stats_lock.release()
                if attempt_num < current_max_attempts_param - 1:
                    time.sleep(random.uniform(0.5, 1.5))
                    continue
                else:
                    return None
        except requests.exceptions.Timeout:
            error_message = f"Thread {thread_id}: Timeout generating question (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param})."
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx)
            lock_acquired_err = app_state.stats_lock.acquire(timeout=7.0)
            if lock_acquired_err:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx] += 1
                    err_summary = f"T{thread_id} Q-Timeout (API{api_slot_idx+1}) A{attempt_num+1}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx))
                    if api_slot_idx < 4:
                        if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                        app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                finally:
                    app_state.stats_lock.release()
            if attempt_num < current_max_attempts_param - 1:
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None
        except Exception as e:
            error_message = f"Thread {thread_id}: Exception in generate_question (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param}): {str(e)}"
            log_message(error_message, "ERROR")
            import traceback
            log_message(traceback.format_exc(), "ERROR")
            record_api_failure(api_slot_idx)
            lock_acquired_err = app_state.stats_lock.acquire(timeout=7.0)
            if lock_acquired_err:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx] += 1
                    err_summary = f"T{thread_id} Q-Exc (API{api_slot_idx+1}) A{attempt_num+1}: {str(e)[:20]}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx))
                    if api_slot_idx < 4:
                        if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                        app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                finally:
                    app_state.stats_lock.release()
            if attempt_num < current_max_attempts_param - 1:
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None
    return None


def generate_user_continuation(system_prompt, conversation_history_for_llm, user_continuation_prompt_template,
                               thread_id, sampler_settings_local, api_url_local, model_name_local, api_key_local,
                               api_slot_idx, current_max_attempts_param, api_request_timeout_param,
                               file_name=""):
    """Generates the user's continuation reply, with retries for API call failures."""


    if not api_url_local:
        log_message(f"Thread {thread_id}: API URL missing for user continuation (API Slot {api_slot_idx+1}). Cannot proceed.", "ERROR")
        return None

    for attempt_num in range(current_max_attempts_param):
        if app_state.stop_processing or app_state.pause_processing: return None
        MAX_TOTAL_RETRY_WAIT = 20
        current_attempt_wait = 0

        lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.50)
        if lock_acquired:
            try:
                app_state.total_attempts_global += 1
                app_state.total_attempts_per_api[api_slot_idx] += 1
            finally:
                app_state.system_prompt_lock.release()

        try:
            # Get the last assistant message for the prompt template
            last_assistant_message = ""
            if conversation_history_for_llm and conversation_history_for_llm[-1]["role"] == "assistant":
                last_assistant_message = conversation_history_for_llm[-1]["content"]

            final_user_continuation_prompt = user_continuation_prompt_template.replace("{last_assistant_message}", last_assistant_message)

            messages = [{"role": "system", "content": system_prompt}] + \
                       conversation_history_for_llm + \
                       [{"role": "user", "content": final_user_continuation_prompt}]

            update_live_prompt_preview(messages, {
                'thread_id': thread_id,
                'api_slot_idx': api_slot_idx + 1,
                'message_type': 'User Continuation',
                'attempt': attempt_num + 1,
                'timestamp': time.strftime('%H:%M:%S'),
            })

            payload_dict = {
                "model": model_name_local,
                "messages": messages,
                **sampler_settings_local.get("generation_params", {
                    "temperature": sampler_settings_local.get("temperature", 0.6),
                    "top_p": sampler_settings_local.get("top_p", 0.9),
                    "top_k": sampler_settings_local.get("top_k", 50),
                    "repetition_penalty": sampler_settings_local.get("repetition_penalty", 1.1),
                    "max_tokens": sampler_settings_local.get("max_tokens_user_reply", global_config.get('samplers.max_tokens_user_reply', 256))
                }),
                "stream": False
            }
            logit_bias_str = sampler_settings_local.get('logit_bias', '')
            if logit_bias_str:
                try:
                    payload_dict['logit_bias'] = json.loads(logit_bias_str)
                except json.JSONDecodeError:
                    log_message(f"Thread {thread_id}: Invalid logit_bias JSON. Skipping.", "WARNING")
            thinking_mode = sampler_settings_local.get('enable_thinking', 'default')
            if thinking_mode == 'enable':
                payload_dict['chat_template_kwargs'] = {"enable_thinking": True}
            elif thinking_mode == 'disable':
                payload_dict['chat_template_kwargs'] = {"enable_thinking": False}
            # else 'default': do not send the parameter

            payload_dict = sanitize_payload_for_endpoint(payload_dict, api_url_local, api_slot_idx)
            payload = json.dumps(payload_dict)
            headers = {
                'Content-Type': 'application/json'
            }

            if api_key_local:
                headers['Authorization'] = f"Bearer {api_key_local}"

            current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{api_slot_idx}.jsonl" if app_state.master_duplication_enabled_var.get() else BASE_DEBUG_LOG_PATH + ".jsonl"
            with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
                debug_log.write(json.dumps({"timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "thread_id": thread_id, "type": "user_continuation_request", "api_slot_idx": api_slot_idx, "attempt": attempt_num + 1, "source_file": file_name, "api_url": api_url_local, "model": model_name_local, "messages": messages, "payload_dict": payload_dict}) + '\n')

            # NEW: Apply rate limiting before making the API call
            global_rate_limiter.wait_if_needed(api_slot_idx)

            # Circuit Breaker Check
            if not check_circuit_breaker(api_slot_idx):
                log_message(f"Thread {thread_id}: API Slot {api_slot_idx+1} circuit open. Skipping user continuation.", "DEBUG")
                return None

            # Track API response time
            api_call_start_time = time.time()
            response = requests.post(api_url_local, headers=headers, data=payload, timeout=(api_request_timeout_param, api_request_timeout_param))  # (connect_timeout, read_timeout)
            api_response_time = time.time() - api_call_start_time

            # Store response time (thread-safe)
            with api_response_times_lock:
                api_response_times_per_slot[api_slot_idx].append(api_response_time)
                if len(api_response_times_per_slot[api_slot_idx]) > MAX_RESPONSE_TIMES_TO_TRACK:
                    api_response_times_per_slot[api_slot_idx] = api_response_times_per_slot[api_slot_idx][-MAX_RESPONSE_TIMES_TO_TRACK:]

            if response.status_code == 200:
                # --- FIX START: Handle None content ---
                record_api_success(api_slot_idx)
                content = response.json()['choices'][0]['message'].get('content')
                if content is None:
                    log_message(f"Thread {thread_id}: API returned None for user continuation content (API Slot {api_slot_idx+1}, Attempt {attempt_num+1})", "WARNING")
                    sleep_dur = random.uniform(0.5, 1.5)
                    if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                        time.sleep(sleep_dur)
                        current_attempt_wait += sleep_dur
                        continue
                    else:
                        return None  # Cap reached, abort continuation
                user_reply_text = content.strip()
                if not user_reply_text or len(user_reply_text) < 5:
                    log_message(f"Thread {thread_id}: API returned empty/very short user reply. Content: '{user_reply_text}'", "WARNING")
                    if attempt_num < current_max_attempts_param - 1:
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None

                    newline_count = user_reply_text.count('\n')
                    text_length = len(user_reply_text)

                    # --- NEW SETTINGS START ---
                    max_newlines = global_config.get('generation.max_newlines_malformed', 16)
                    max_text_length = global_config.get('generation.max_text_length_malformed', 5000)
                    # --- NEW SETTINGS END ---

                    if newline_count > max_newlines or text_length > max_text_length:
                        log_message(
                            f"Thread {thread_id}: User reply response appears malformed. "
                            f"Newlines: {newline_count} (Max: {max_newlines}), Length: {text_length} (Max: {max_text_length}). "
                            f"Snippet: '{user_reply_text[:100]}...'",
                            "WARNING"
                        )
                        sleep_dur = random.uniform(0.5, 1.5)
                        if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                            time.sleep(sleep_dur)
                            current_attempt_wait += sleep_dur
                            continue
                        else:
                            return None  # Cap reached, abort continuation
                # --- FIX END ---
                return user_reply_text
            else: # API call failed
                record_api_failure(api_slot_idx)
                error_message = f"Thread {thread_id}: Error generating user continuation (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param}, Status: {response.status_code}): {response.text[:200]}"
                log_message(error_message, "ERROR")
                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.50)
                if lock_acquired:
                    try:
                        app_state.error_count_total += 1
                        app_state.error_counts_per_api[api_slot_idx] += 1
                        err_summary = f"T{thread_id} Q-Err (API{api_slot_idx+1}): S{response.status_code} A{attempt_num+1}"
                        if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                        app_state.recent_errors_total.append((err_summary, api_slot_idx))
                        if api_slot_idx < 6:
                            if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                            app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                    finally:
                        app_state.system_prompt_lock.release()

                with app_state.issue_timestamps_lock:
                    app_state.issue_timestamps['errors'].append(time.time())
                    cutoff = time.time() - 3600
                    app_state.issue_timestamps['errors'] = [t for t in app_state.issue_timestamps['errors'] if t > cutoff]
                    if api_slot_idx < 4:
                        if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                        app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                sleep_dur = random.uniform(0.5, 1.5)
                if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                    time.sleep(sleep_dur)
                    current_attempt_wait += sleep_dur
                    continue
                else:
                    return None  # Cap reached, abort continuation
        except requests.exceptions.Timeout:
            error_message = f"Thread {thread_id}: Timeout generating user continuation (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param})."
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx)
            lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.50)
            if lock_acquired:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx] += 1
                    err_summary = f"T{thread_id} UserCont-Timeout (API{api_slot_idx+1}) A{attempt_num+1}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx))
                    if api_slot_idx < 4:
                        if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                        app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                finally:
                    app_state.system_prompt_lock.release()
            sleep_dur = random.uniform(0.5, 1.5)
            if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                time.sleep(sleep_dur)
                current_attempt_wait += sleep_dur
                continue
            else:
                return None  # Cap reached, abort continuation
        except Exception as e: # Catch any other exceptions
            error_message = f"Thread {thread_id}: Exception in generate_user_continuation (API Slot {api_slot_idx+1}, Attempt {attempt_num+1}/{current_max_attempts_param}): {str(e)}"
            log_message(error_message, "ERROR")
            import traceback
            log_message(traceback.format_exc(), "ERROR")
            record_api_failure(api_slot_idx)
            lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.50)
            if lock_acquired:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx] += 1
                    err_summary = f"T{thread_id} UserCont-Exc (API{api_slot_idx+1}) A{attempt_num+1}: {str(e)[:20]}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx))
                    if api_slot_idx < 4:
                        if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                        app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                finally:
                    app_state.system_prompt_lock.release()
            sleep_dur = random.uniform(0.5, 1.5)
            if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                time.sleep(sleep_dur)
                current_attempt_wait += sleep_dur
                continue
            else:
                return None  # Cap reached, abort continuation
    return None


def call_slop_fixer_llm(text_context, slop_phrase,
                        slop_fixer_api_config,
                        main_sampler_settings, thread_id, additional_fix_instructions="",
                        current_max_attempts_param=5, api_request_timeout_param=300,
                        file_name=""):
    """Calls a dedicated LLM (API Slot 5, index 4) to rewrite a sentence containing "slop", with retries."""

    api_slot_idx_slop_fixer = 4 # Slop fixer is always API slot 5 (index 4)

    if not slop_fixer_api_config or not slop_fixer_api_config.get('url') or \
       not slop_fixer_api_config.get('model') or not slop_fixer_api_config.get('key'):
        log_message(f"Thread {thread_id}: Slop Fixer LLM (API Slot {api_slot_idx_slop_fixer+1}) not fully configured. Cannot call.", "WARNING")
        return None, text_context # Return None for rewritten, and original sentence

    api_url = slop_fixer_api_config['url']
    model_name = slop_fixer_api_config['model']
    api_key = slop_fixer_api_config['key']

    # Added validation for main_sampler_settings to prevent NameError-like issues from bad config
    if not main_sampler_settings or not isinstance(main_sampler_settings, dict):
        log_message(f"Thread {thread_id}: main_sampler_settings passed to call_slop_fixer_llm is invalid. Expected a dictionary.", "ERROR")
        return None, text_context

    for attempt_num in range(current_max_attempts_param):
        if app_state.stop_processing or app_state.pause_processing: return None, text_context

        with app_state.system_prompt_lock:
            app_state.total_attempts_global +=1
            app_state.total_attempts_per_api[api_slot_idx_slop_fixer] +=1

        try:
            # UPDATED PROMPT: Explicitly instruct to preserve quotes & provide paragraph context
            user_rewrite_instruction = (
                f"The following text contains an undesirable phrase: '{slop_phrase}'. "
                f"Rewrite the text to remove or rephrase this specific undesirable phrase while preserving the original meaning, tone, and ALL quotation marks. "
                f"CRITICAL: Do not drop any opening or closing quotation marks. Ensure quotes remain perfectly balanced. "
                f"Only output the rewritten text. Do not include any preamble or explanation. Just the rewritten text."
            )
            if additional_fix_instructions:
                user_rewrite_instruction += f"\n\nImportant instruction to follow: {additional_fix_instructions}"

            user_rewrite_instruction += (
                "\n\nCRITICAL RULES:\n"
                "1. Preserve ALL existing quotation marks exactly where they appear structurally.\n"
                "2. If you must add or remove a quote, ensure every opening mark has a matching closing mark.\n"
                "3. Do not wrap the entire output in quotes."
            )

            user_rewrite_instruction += f"\n\n<original_text>\n{text_context}\n</original_text>"

            messages = [
                {"role": "system", "content": "You are an expert editor. Rewrite the given text to remove the specified undesirable phrase, ensuring the core meaning is kept. Output only the rewritten text."},
                {"role": "user", "content": user_rewrite_instruction}
            ]

            update_live_prompt_preview(messages, {
                'thread_id': thread_id,
                'api_slot_idx': api_slot_idx_slop_fixer + 1,
                'message_type': 'Slop Fix',
                'attempt': attempt_num + 1,
                'timestamp': time.strftime('%H:%M:%S'),
            })

            # Use dedicated Slop Fixer sampler settings from the API config
            slop_fixer_sampler_overrides = slop_fixer_api_config.get('sampler_settings', {}) or main_sampler_settings

            # Renamed to match the function context
            final_slop_fixer_params = {
                "temperature": slop_fixer_sampler_overrides.get("temperature", 0.5),
                "top_p": slop_fixer_sampler_overrides.get("top_p", 0.95),
                "min_p": slop_fixer_sampler_overrides.get("min_p", 0.0),
                "top_k": slop_fixer_sampler_overrides.get("top_k", 50),
                "repetition_penalty": slop_fixer_sampler_overrides.get("repetition_penalty", 1.1),
                "max_tokens": slop_fixer_sampler_overrides.get("max_tokens", len(text_context.split()) * 3 + 70),
            }

            payload_data = {
                "model": model_name,
                "messages": messages,
                **final_slop_fixer_params,
                "stream": False
            }
            payload_data = sanitize_payload_for_endpoint(payload_data, api_url, api_slot_idx_slop_fixer)
            payload = json.dumps(payload_data)
            headers = {
                'Content-Type': 'application/json'
            }

            if api_key:
                headers['Authorization'] = f"Bearer {api_key}"

            current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{api_slot_idx_slop_fixer}.jsonl" if app_state.master_duplication_enabled_var.get() else BASE_DEBUG_LOG_PATH + ".jsonl"

            with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
                debug_log.write(json.dumps({"timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "thread_id": thread_id, "type": "slop_fix_request", "api_slot_idx": api_slot_idx_slop_fixer, "attempt": attempt_num + 1, "source_file": file_name, "api_url": api_url, "model": model_name, "messages": messages, "payload_data": payload_data }) + '\n')

            # NEW: Apply rate limiting before making the API call
            global_rate_limiter.wait_if_needed(api_slot_idx_slop_fixer)

            # Track API response time
            api_call_start_time = time.time()
            response = requests.post(api_url, headers=headers, data=payload, timeout=(api_request_timeout_param, api_request_timeout_param))  # (connect_timeout, read_timeout)
            api_response_time = time.time() - api_call_start_time

            # Store response time (thread-safe)
            with api_response_times_lock:
                api_response_times_per_slot[api_slot_idx_slop_fixer].append(api_response_time)
            if len(api_response_times_per_slot[api_slot_idx_slop_fixer]) > MAX_RESPONSE_TIMES_TO_TRACK:
                api_response_times_per_slot[api_slot_idx_slop_fixer] = api_response_times_per_slot[api_slot_idx_slop_fixer][-MAX_RESPONSE_TIMES_TO_TRACK:]

            if response.status_code == 200:
                # --- FIX START: Handle None content ---
                record_api_success(api_slot_idx_slop_fixer)
                response_data = response.json()
                content = response_data['choices'][0]['message'].get('content')

                # NEW: Extract token usage if available
                usage = response_data.get('usage', {})
                input_tokens = usage.get('prompt_tokens', 0)
                output_tokens = usage.get('completion_tokens', 0)

                # Update global counters safely using the lock
                with app_state.system_prompt_lock:
                    app_state.total_input_tokens += input_tokens
                    app_state.total_output_tokens += output_tokens

                if content is None:
                    log_message(f"Thread {thread_id}: API returned None for content (API Slot {api_slot_idx_slop_fixer+1}, Attempt {attempt_num+1})", "WARNING")
                    if attempt_num < current_max_attempts_param - 1:
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None, text_context

                rewritten_sentence = content.strip()

                # FIXED: Don't return None for content issues - let the caller handle retries
                if not rewritten_sentence or len(rewritten_sentence) < 5:
                    log_message(f"Thread {thread_id}: Slop fixer returned empty/very short response. Original: '{text_context}'", "WARNING")
                    return None, text_context

                if rewritten_sentence.count('\n') > 1 or len(rewritten_sentence) > len(text_context) * 2:
                    log_message(f"Thread {thread_id}: Slop fixer response appears malformed. Using original.", "WARNING")
                    return None, text_context

                # Only strip wrapper quotes the fixer ADDED around its rewrite.
                # If the ORIGINAL sentence was itself a fully-quoted line of
                # dialogue (e.g. "Get out of here!"), the LLM correctly returning
                # it still-quoted must NOT be unwrapped here -- stripping would
                # eat the real opening/closing quotes and is a direct source of
                # "missing a quote on one side of dialogue".
                _orig = text_context.strip()
                _orig_wrapped = _orig.startswith('"') and _orig.endswith('"') and len(_orig) > 2
                if (rewritten_sentence.startswith('"') and rewritten_sentence.endswith('"')
                        and len(rewritten_sentence) > 2 and not _orig_wrapped):
                    rewritten_sentence = rewritten_sentence[1:-1]

                if not rewritten_sentence or len(rewritten_sentence) < 0.5 * len(text_context):
                    log_message(f"Thread {thread_id}: Slop fixer returned very short/empty sentence: '{rewritten_sentence}'. Original: '{text_context}'", "WARNING")
                    return None, text_context

                return rewritten_sentence, text_context
            else: # API call failed
                error_message = f"Thread {thread_id}: Slop Fixer LLM Error (API Slot {api_slot_idx_slop_fixer+1}, Attempt {attempt_num+1}/{current_max_attempts_param}, Status: {response.status_code}): {response.text[:200]}"
                log_message(error_message, "ERROR")
                record_api_failure(api_slot_idx_slop_fixer)
                with app_state.system_prompt_lock:
                    app_state.error_count_total +=1
                    app_state.error_counts_per_api[api_slot_idx_slop_fixer] += 1
                    err_summary = f"T{thread_id} SlopFix-API (API{api_slot_idx_slop_fixer+1}): S{response.status_code} A{attempt_num+1}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx_slop_fixer))
                if attempt_num < current_max_attempts_param - 1:
                    time.sleep(random.uniform(0.5, 1.5))
                    continue
                else:
                    return None, text_context
        except requests.exceptions.Timeout:
            error_message = f"Thread {thread_id}: Slop Fixer LLM request timed out (API Slot {api_slot_idx_slop_fixer+1}, Attempt {attempt_num+1}/{current_max_attempts_param})."
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx_slop_fixer)
            with app_state.system_prompt_lock:
                app_state.error_count_total +=1
                app_state.error_counts_per_api[api_slot_idx_slop_fixer] += 1
                err_summary = f"T{thread_id} SlopFix-Timeout (API{api_slot_idx_slop_fixer+1}) A{attempt_num+1}"
                if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                app_state.recent_errors_total.append((err_summary, api_slot_idx_slop_fixer))
            if attempt_num < current_max_attempts_param - 1:
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None, text_context
        except Exception as e: # Catch any other exceptions
            error_message = f"Thread {thread_id}: Exception in call_slop_fixer_llm (API Slot {api_slot_idx_slop_fixer+1}, Attempt {attempt_num+1}/{current_max_attempts_param}): {str(e)}"
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx_slop_fixer)
            with app_state.system_prompt_lock:
                app_state.error_count_total +=1
                app_state.error_counts_per_api[api_slot_idx_slop_fixer] += 1
                err_summary = f"T{thread_id} SlopFix-Exc (API{api_slot_idx_slop_fixer+1}) A{attempt_num+1}: {str(e)[:20]}"
                if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                app_state.recent_errors_total.append((err_summary, api_slot_idx_slop_fixer))
            if attempt_num < current_max_attempts_param - 1:
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None, text_context
    return None, text_context


def call_anti_slop_llm(text_context, anti_slop_phrase,
                       anti_slop_api_config,
                       main_sampler_settings, thread_id, additional_fix_instructions="",
                       current_max_attempts_param=5,
                       master_duplication_enabled=False,
                       api_request_timeout_param=300,
                       file_name=""):
    """Calls a dedicated LLM to rewrite a sentence containing anti-slop phrases."""
    api_slot_idx_anti_slop = 5

    if not anti_slop_api_config or not anti_slop_api_config.get('url') or \
       not anti_slop_api_config.get('model') or not anti_slop_api_config.get('key'):
        log_message(f"Thread {thread_id}: Anti-Slop LLM not fully configured. Cannot call.", "WARNING")
        return None, text_context

    api_url = anti_slop_api_config['url']
    model_name = anti_slop_api_config['model']
    api_key = anti_slop_api_config['key']

    for attempt_num in range(current_max_attempts_param):
        if app_state.stop_processing or app_state.pause_processing: return None, text_context

        # FIX 2 & 3: Move rate limiter BEFORE lock, and add timeout to lock acquisition
        global_rate_limiter.wait_if_needed(api_slot_idx_anti_slop)

        # Circuit Breaker Check
        if not check_circuit_breaker(api_slot_idx_anti_slop):
            log_message(f"Thread {thread_id}: API Slot {api_slot_idx_anti_slop+1} circuit open. Skipping anti-slop fixer.", "DEBUG")
            return None, text_context

        lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.50)
        if lock_acquired:
            try:
                app_state.total_attempts_global += 1
                app_state.total_attempts_per_api[api_slot_idx_anti_slop] += 1
            finally:
                app_state.system_prompt_lock.release()
        else:
            log_message(f"Thread {thread_id}: WARNING - Could not acquire system_prompt_lock", "WARNING")
            return None, text_context

        try:
            # UPDATED PROMPT
            user_rewrite_instruction = (
                f"The following text contains an undesirable phrase: '{anti_slop_phrase}'. "
                f"Rewrite the text to remove or rephrase this specific undesirable phrase while preserving the original meaning, tone, and ALL quotation marks. "
                f"CRITICAL: Do not drop any opening or closing quotation marks. Ensure quotes remain perfectly balanced. "
                f"ONLY output the rewritten text. Do not include any preamble, explanation, quotes, or other text."
            )
            if additional_fix_instructions:
                user_rewrite_instruction += f"\n\nAdditional instruction: {additional_fix_instructions}"

            user_rewrite_instruction += (
                "\n\nCRITICAL RULES:\n"
                "1. Preserve ALL existing quotation marks exactly where they appear structurally.\n"
                "2. If you must add or remove a quote, ensure every opening mark has a matching closing mark.\n"
                "3. Do not wrap the entire output in quotes."
            )

            user_rewrite_instruction += f"\n\n<original_text>\n{text_context}\n</original_text>"

            messages = [
                {"role": "system", "content": "You are an expert editor. Rewrite the given text to remove the specified undesirable phrase, ensuring the core meaning is kept. Output only the rewritten text."},
                {"role": "user", "content": user_rewrite_instruction}
            ]

            update_live_prompt_preview(messages, {
                'thread_id': thread_id,
                'api_slot_idx': api_slot_idx_anti_slop + 1,
                'message_type': 'Anti-Slop Fix',
                'attempt': attempt_num + 1,
                'timestamp': time.strftime('%H:%M:%S'),
            })

            # Use dedicated Anti-Slop sampler settings from the API config
            anti_slop_sampler_overrides = anti_slop_api_config.get('sampler_settings', {}) or main_sampler_settings

            final_anti_slop_params = {
                "temperature": anti_slop_sampler_overrides.get("temperature", 0.5),
                "top_p": anti_slop_sampler_overrides.get("top_p", 0.95),
                "min_p": anti_slop_sampler_overrides.get("min_p", 0.0),
                "top_k": anti_slop_sampler_overrides.get("top_k", 50),
                "repetition_penalty": anti_slop_sampler_overrides.get("repetition_penalty", 1.1),
                "max_tokens": anti_slop_sampler_overrides.get("max_tokens", len(text_context.split()) * 3 + 70),
            }

            payload_data = {
                "model": model_name,
                "messages": messages,
                **final_anti_slop_params,
                "stream": False
            }
            payload_data = sanitize_payload_for_endpoint(payload_data, api_url, api_slot_idx_anti_slop)
            payload = json.dumps(payload_data)
            headers = {
                'Content-Type': 'application/json'
            }
            if api_key:
                headers['Authorization'] = f"Bearer {api_key}"

            current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{api_slot_idx_anti_slop}.jsonl" if master_duplication_enabled else BASE_DEBUG_LOG_PATH + ".jsonl"
            with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
                debug_log.write(json.dumps({"timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "thread_id": thread_id, "type": "anti_slop_request", "api_slot_idx": api_slot_idx_anti_slop, "attempt": attempt_num + 1, "source_file": file_name, "api_url": api_url, "model": model_name, "messages": messages, "payload_data": payload_data }) + '\n')

            # FIX 2: Rate limiter already called BEFORE lock acquisition above
            # Track API response time
            api_call_start_time = time.time()
            response = requests.post(api_url, headers=headers, data=payload, timeout=(api_request_timeout_param, api_request_timeout_param))

            api_response_time = time.time() - api_call_start_time
            with api_response_times_lock:
                api_response_times_per_slot[api_slot_idx_anti_slop].append(api_response_time)
                if len(api_response_times_per_slot[api_slot_idx_anti_slop]) > MAX_RESPONSE_TIMES_TO_TRACK:
                    api_response_times_per_slot[api_slot_idx_anti_slop] = api_response_times_per_slot[api_slot_idx_anti_slop][-MAX_RESPONSE_TIMES_TO_TRACK:]

            if response.status_code == 200:
                record_api_success(api_slot_idx_anti_slop)
                response_data = response.json()
                content = response_data['choices'][0]['message'].get('content')
                usage = response_data.get('usage', {})
                input_tokens = usage.get('prompt_tokens', 0)
                output_tokens = usage.get('completion_tokens', 0)

                lock_acquired_tokens = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired_tokens:
                    try:
                        app_state.total_input_tokens += input_tokens
                        app_state.total_output_tokens += output_tokens
                    finally:
                        app_state.system_prompt_lock.release()
                else:
                    log_message(f"Thread {thread_id}: WARNING - Could not acquire system_prompt_lock for token update.", "WARNING")

                if content is None:
                    log_message(f"Thread {thread_id}: API returned None for anti-slop content (Attempt {attempt_num+1})", "WARNING")
                    if attempt_num < current_max_attempts_param - 1:
                        if app_state.stop_processing or app_state.pause_processing: return None, text_context
                        time.sleep(random.uniform(0.5, 1.5))
                        continue
                    else:
                        return None, text_context

                rewritten_sentence = content.strip()
                if not rewritten_sentence or len(rewritten_sentence) < 5:
                    log_message(f"Thread {thread_id}: Anti-slop fixer returned empty/very short sentence. Content: '{rewritten_sentence}'", "WARNING")
                    return None, text_context

                if rewritten_sentence.count('\n') > 1 or len(rewritten_sentence) > len(text_context) * 2:
                    log_message(f"Thread {thread_id}: Anti-slop fixer response appears malformed. Using original.", "WARNING")
                    return None, text_context

                # Only strip wrapper quotes the fixer ADDED around its rewrite.
                # If the ORIGINAL sentence was itself a fully-quoted line of
                # dialogue (e.g. "Get out of here!"), the LLM correctly returning
                # it still-quoted must NOT be unwrapped here -- stripping would
                # eat the real opening/closing quotes and is a direct source of
                # "missing a quote on one side of dialogue".
                _orig = text_context.strip()
                _orig_wrapped = _orig.startswith('"') and _orig.endswith('"') and len(_orig) > 2
                if (rewritten_sentence.startswith('"') and rewritten_sentence.endswith('"')
                        and len(rewritten_sentence) > 2 and not _orig_wrapped):
                    rewritten_sentence = rewritten_sentence[1:-1]

                if not rewritten_sentence or len(rewritten_sentence) < 0.5 * len(text_context):
                    log_message(f"Thread {thread_id}: Anti-slop fixer returned very short/empty sentence", "WARNING")
                    return None, text_context

                return rewritten_sentence, text_context

            else:
                error_message = f"Thread {thread_id}: Anti-Slop LLM Error (Attempt {attempt_num+1}/{current_max_attempts_param}, Status: {response.status_code}): {response.text[:200]}"
                log_message(error_message, "ERROR")
                record_api_failure(api_slot_idx_anti_slop)
                lock_acquired_err = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired_err:
                    try:
                        app_state.error_count_total += 1
                        app_state.error_counts_per_api[api_slot_idx_anti_slop] += 1
                        err_summary = f"T{thread_id} AntiSlop-API: S{response.status_code} A{attempt_num+1}"
                        if len(app_state.recent_errors_total) >= MAX_RECENT:
                            app_state.recent_errors_total.pop(0)
                        app_state.recent_errors_total.append((err_summary, api_slot_idx_anti_slop))
                    finally:
                        app_state.system_prompt_lock.release()

                if attempt_num < current_max_attempts_param - 1:
                    if app_state.stop_processing or app_state.pause_processing: return None, text_context
                    time.sleep(random.uniform(0.5, 1.5))
                    continue
                else:
                    return None, text_context

        except requests.exceptions.Timeout:
            error_message = f"Thread {thread_id}: Anti-Slop LLM request timed out (Attempt {attempt_num+1}/{current_max_attempts_param})."
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx_anti_slop)
            lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.50)
            if lock_acquired:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx_anti_slop] += 1
                    err_summary = f"T{thread_id} AntiSlop-Timeout A{attempt_num+1}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT:
                        app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx_anti_slop))
                finally:
                    app_state.system_prompt_lock.release()

            if attempt_num < current_max_attempts_param - 1:
                if app_state.stop_processing or app_state.pause_processing: return None, text_context
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None, text_context

        except Exception as e:
            error_message = f"Thread {thread_id}: Exception in call_anti_slop_llm (Attempt {attempt_num+1}/{current_max_attempts_param}): {str(e)}"
            log_message(error_message, "ERROR")
            record_api_failure(api_slot_idx_anti_slop)
            lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.50)
            if lock_acquired:
                try:
                    app_state.error_count_total += 1
                    app_state.error_counts_per_api[api_slot_idx_anti_slop] += 1
                    err_summary = f"T{thread_id} AntiSlop-Exc A{attempt_num+1}: {str(e)[:20]}"
                    if len(app_state.recent_errors_total) >= MAX_RECENT:
                        app_state.recent_errors_total.pop(0)
                    app_state.recent_errors_total.append((err_summary, api_slot_idx_anti_slop))
                finally:
                    app_state.system_prompt_lock.release()

            if attempt_num < current_max_attempts_param - 1:
                if app_state.stop_processing or app_state.pause_processing: return None, text_context
                time.sleep(random.uniform(0.5, 1.5))
                continue
            else:
                return None, text_context

    return None, text_context


def generate_answer_with_retries(base_system_prompt, conversation_history_for_llm, answer_prompt_template,
                                 thread_id, q, sampler_settings_local, api_url_local, model_name_local, api_key_local,
                                 refusal_phrases_local, user_speaking_phrases_local, slop_phrases_local,
                                 current_anti_slop_phrases_param,
                                 jailbreaks_local, speaking_fixes_local, slop_fixes_fallback_local,
                                 max_attempts_local,
                                 slop_fixer_api_config_param,
                                 current_slop_fixes_for_rotation_param,
                                 api_slot_idx,
                                 current_max_attempts_for_slop_fixer_call,
                                 anti_slop_fixer_api_config_param,
                                 master_duplication_enabled_local,
                                 no_user_impersonation_local,
                                 api_request_timeout_param,
                                 slop_to_anti_slop_fallback=False,
                                 file_name=""):
    """
    Generates an assistant's answer, handling retries for refusals, user speaking, and slop.
    Applies jailbreaks, speaking fixes, and slop fixes (system prompt or dedicated LLM).
    Returns the generated answer or None if all attempts fail.
    """

    refusal_detected_this_main_api_call = False
    issue_ever_detected_this_task = False
    refusal_ever_detected_this_task = False

    if not api_url_local:
        log_message(f"Thread {thread_id}: API URL missing for answer generation (API Slot {api_slot_idx+1}). Cannot proceed.", "ERROR")
        return None

    current_system_prompt_iter = base_system_prompt

    for attempt in range(max_attempts_local):
        if app_state.stop_processing or app_state.pause_processing: return None

        api_call_retries_for_this_iteration = current_max_attempts_for_slop_fixer_call
        fix_attempts_specific = {'refusal': 0, 'user_speaking': 0, 'slop_fallback': 0, 'incomplete_quote': 0}
        issue_detected_this_main_api_call = False

        while True:
            if app_state.stop_processing or app_state.pause_processing: return None

            # --- API Call with Retries for API Failures ---
            answer = None
            response_text_content = ""
            response_status_code = -1

            for api_call_attempt_num in range(api_call_retries_for_this_iteration):
                if app_state.stop_processing or app_state.pause_processing: return None
                MAX_TOTAL_RETRY_WAIT = 30  # Cap total sleep per outer attempt
                current_attempt_wait = 0

                # FIX 3: Add timeout to lock acquisition
                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.50)
                if lock_acquired:
                    try:
                        app_state.total_attempts_global += 1
                        app_state.total_attempts_per_api[api_slot_idx] += 1
                    finally:
                        app_state.system_prompt_lock.release()
                else:
                    log_message(f"Thread {thread_id}: Skipped stat update (system_prompt_lock busy)", "DEBUG")

                messages = [{"role": "system", "content": current_system_prompt_iter}] + \
                           conversation_history_for_llm + \
                           [{"role": "user", "content": answer_prompt_template}]

                # --- LIVE PREVIEW HOOK ---
                update_live_prompt_preview(messages, {
                    'thread_id': thread_id,
                    'api_slot_idx': api_slot_idx + 1,
                    'message_type': 'Answer',
                    'attempt': f"{attempt + 1}/{max_attempts_local}",
                    'timestamp': time.strftime('%H:%M:%S'),
                })
                # --- END HOOK ---

                payload_dict_ans = {
                    "model": model_name_local,
                    "messages": messages,
                    **sampler_settings_local.get("generation_params", {
                        "temperature": sampler_settings_local.get("temperature",0.5),
                        "top_p": sampler_settings_local.get("top_p",0.9),
                        "min_p": sampler_settings_local.get("min_p", 0.0),
                        "top_k": sampler_settings_local.get("top_k",50),
                        "repetition_penalty": sampler_settings_local.get("repetition_penalty",1.1),
                        "max_tokens": sampler_settings_local.get("max_tokens_answer", global_config.get('samplers.max_tokens_answer',1024))
                    }),
                    "stream": False
                }

                logit_bias_str = sampler_settings_local.get('logit_bias', '')
                if logit_bias_str:
                    try:
                        payload_dict_ans['logit_bias'] = json.loads(logit_bias_str)
                    except json.JSONDecodeError:
                        log_message(f"Thread {thread_id}: Invalid logit_bias JSON. Skipping.", "WARNING")

                thinking_mode = sampler_settings_local.get('enable_thinking', 'default')
                if thinking_mode == 'enable':
                    payload_dict_ans['chat_template_kwargs'] = {"enable_thinking": True}
                elif thinking_mode == 'disable':
                    payload_dict_ans['chat_template_kwargs'] = {"enable_thinking": False}
                # else 'default': do not send the parameter

                payload_dict_ans = sanitize_payload_for_endpoint(payload_dict_ans, api_url_local, api_slot_idx)
                payload = json.dumps(payload_dict_ans)
                headers = {
                    'Content-Type': 'application/json'
                }

                if api_key_local:
                    headers['Authorization'] = f"Bearer {api_key_local}"

                current_debug_log_path = BASE_DEBUG_LOG_PATH + f"_api_slot_{api_slot_idx}.jsonl" if app_state.master_duplication_enabled_var.get() else BASE_DEBUG_LOG_PATH + ".jsonl"
                with open(current_debug_log_path, 'a', encoding='utf-8') as debug_log:
                    debug_log.write(json.dumps({"timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "thread_id": thread_id, "type": "answer_request", "api_slot_idx": api_slot_idx, "outer_attempt": attempt +1, "inner_api_call_attempt": api_call_attempt_num + 1, "source_file": file_name, "fix_attempts_specific": fix_attempts_specific, "current_system_prompt_iter_len": len(current_system_prompt_iter), "messages_len": len(messages), "payload_dict_ans": payload_dict_ans}) + '\n')

                global_rate_limiter.wait_if_needed(api_slot_idx)

                # Circuit Breaker Check
                if not check_circuit_breaker(api_slot_idx):
                    log_message(f"Thread {thread_id}: API Slot {api_slot_idx+1} circuit open. Skipping answer generation.", "DEBUG")
                    break  # Exit inner retry loop; outer loop handles fallback

                api_call_start_time = time.time()
                prompt_content = json.dumps(messages, sort_keys=True)
                prompt_hash = hashlib.md5(prompt_content.encode()).hexdigest()

                cached_response, is_cached = get_cached_response(prompt_hash, api_slot_idx)
                if is_cached:
                    answer = cached_response
                    log_message(f"Cache HIT for answer generation (API Slot {api_slot_idx+1}).", "DEBUG")
                    break

                try:
                    response = requests.post(api_url_local, headers=headers, data=payload, timeout=(api_request_timeout_param, api_request_timeout_param))
                    api_response_time = time.time() - api_call_start_time

                    with api_response_times_lock:
                        api_response_times_per_slot[api_slot_idx].append(api_response_time)
                    if len(api_response_times_per_slot[api_slot_idx]) > MAX_RESPONSE_TIMES_TO_TRACK:
                        api_response_times_per_slot[api_slot_idx] = api_response_times_per_slot[api_slot_idx][-MAX_RESPONSE_TIMES_TO_TRACK:]

                    response_status_code = response.status_code
                    response_text_content = response.text

                    if response.status_code in [503, 429]:
                        retry_after = response.headers.get('Retry-After', 60)
                        try:
                            retry_after = int(retry_after)
                        except (ValueError, TypeError):
                            retry_after = 60
                        log_message(f"Thread {thread_id}: API Slot {api_slot_idx+1} is overloaded (Status {response.status_code}). Waiting {retry_after}s before retry.", "WARNING")
                        time.sleep(retry_after)
                        if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                            continue
                        else:
                            break

                    if response.status_code == 200:
                        record_api_success(api_slot_idx)
                        response_data = response.json()
                        content = response_data['choices'][0]['message'].get('content')

                        usage = response_data.get('usage', {})
                        input_tokens = usage.get('prompt_tokens', 0)
                        output_tokens = usage.get('completion_tokens', 0)

                        lock_acquired_tokens = app_state.system_prompt_lock.acquire(timeout=0.05)
                        if lock_acquired_tokens:
                            try:
                                app_state.total_input_tokens += input_tokens
                                app_state.total_output_tokens += output_tokens
                            finally:
                                app_state.system_prompt_lock.release()
                        else:
                            log_message(f"Thread {thread_id}: WARNING - Could not acquire system_prompt_lock for token update.", "WARNING")

                        if content is None:
                            log_message(f"Thread {thread_id}: API returned None for answer content (API Slot {api_slot_idx+1}, OuterAttempt {attempt + 1}, API Call Attempt {api_call_attempt_num+1})", "WARNING")
                            if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                                sleep_dur = random.uniform(0.5, 1.5)
                                if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                                    time.sleep(sleep_dur)
                                    current_attempt_wait += sleep_dur
                                    continue
                                else:
                                    return None  # Cap reached, abort question generation
                        answer = content.strip()

                        if not answer or len(answer) < 10:
                            log_message(f"Thread {thread_id}: API returned empty/very short answer. Content: '{answer}'", "WARNING")
                            if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                                sleep_dur = random.uniform(0.5, 1.5)
                                if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                                    time.sleep(sleep_dur)
                                    current_attempt_wait += sleep_dur
                                    continue
                                else:
                                    return None  # Cap reached, abort question generation

                        newline_count = answer.count('\n')
                        text_length = len(answer)

                        max_newlines = global_config.get('generation.max_newlines_malformed', 16)
                        max_text_length = global_config.get('generation.max_text_length_malformed', 5000)

                        if newline_count > max_newlines or text_length > max_text_length:
                            log_message(
                                f"Thread {thread_id}: Answer response appears malformed. "
                                f"Newlines: {newline_count} (Max: {max_newlines}), Length: {text_length} (Max: {max_text_length}). "
                                f"Snippet: '{answer[:100]}...'",
                                "WARNING"
                            )
                            sleep_dur = random.uniform(0.5, 1.5)
                            if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                                time.sleep(sleep_dur)
                                current_attempt_wait += sleep_dur
                                continue
                            else:
                                return None  # Cap reached, abort question generation
                        break
                    else:
                        log_message(f"Thread {thread_id}: Error generating answer (API Slot {api_slot_idx+1}, OuterAttempt {attempt + 1}, API Call Attempt {api_call_attempt_num+1}/{api_call_retries_for_this_iteration}), Status {response_status_code}: {response_text_content[:200]}", "ERROR")
                        record_api_failure(api_slot_idx)
                        lock_acquired_err = app_state.system_prompt_lock.acquire(timeout=0.05)
                        if lock_acquired_err:
                            try:
                                app_state.error_count_total += 1
                                app_state.error_counts_per_api[api_slot_idx] += 1
                                err_summary = f"T{thread_id} Ans-Err (API{api_slot_idx+1}): S{response_status_code} A{api_call_attempt_num+1}"
                                if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                                app_state.recent_errors_total.append((err_summary, api_slot_idx))
                                with app_state.issue_timestamps_lock:
                                    app_state.issue_timestamps['errors'].append(time.time())
                                    cutoff = time.time() - 3600
                                    app_state.issue_timestamps['errors'] = [t for t in app_state.issue_timestamps['errors'] if t > cutoff]
                                if api_slot_idx < 6:
                                    if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                                    app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                            finally:
                                app_state.system_prompt_lock.release()
                        if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                            sleep_dur = random.uniform(0.5, 1.5)
                            if current_attempt_wait + sleep_dur <= MAX_TOTAL_RETRY_WAIT:
                                time.sleep(sleep_dur)
                                current_attempt_wait += sleep_dur
                                continue
                            else:
                                return None  # Cap reached, abort question generation

                except requests.exceptions.Timeout:
                    log_message(f"Thread {thread_id}: API Timeout generating answer (API Slot {api_slot_idx+1}, OuterAttempt {attempt + 1}, API Call Attempt {api_call_attempt_num+1}/{api_call_retries_for_this_iteration}).", "ERROR")
                    record_api_failure(api_slot_idx)
                    lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.50)
                    if lock_acquired:
                        try:
                            app_state.error_count_total += 1; app_state.error_counts_per_api[api_slot_idx] += 1
                            err_summary = f"T{thread_id} Ans-Timeout (API{api_slot_idx+1}) A{api_call_attempt_num+1}"
                            if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                            app_state.recent_errors_total.append((err_summary, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                                app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                        finally:
                            app_state.system_prompt_lock.release()
                    if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                        time.sleep(random.uniform(0.5, 1.5)); continue
                    else: break
                except requests.exceptions.RequestException as e_req:
                    log_message(f"Thread {thread_id}: RequestException generating answer (API Slot {api_slot_idx+1}, OuterAttempt {attempt + 1}, API Call Attempt {api_call_attempt_num+1}/{api_call_retries_for_this_iteration}): {str(e_req)}", "ERROR")
                    record_api_failure(api_slot_idx)
                    lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.50)
                    if lock_acquired:
                        try:
                            app_state.error_count_total += 1; app_state.error_counts_per_api[api_slot_idx] += 1
                            err_summary = f"T{thread_id} Ans-ReqExc (API{api_slot_idx+1}) A{api_call_attempt_num+1}"
                            if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                            app_state.recent_errors_total.append((err_summary, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                                app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                        finally:
                            app_state.system_prompt_lock.release()
                    if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                        time.sleep(random.uniform(0.5, 1.5)); continue
                    else: break
                except Exception as e_gen:
                    log_message(f"Thread {thread_id}: Exception in answer generation (API Slot {api_slot_idx+1}, OuterAttempt {attempt + 1}, API Call Attempt {api_call_attempt_num+1}/{api_call_retries_for_this_iteration}): {str(e_gen)}", "ERROR")
                    import traceback; log_message(traceback.format_exc(), "ERROR")
                    record_api_failure(api_slot_idx)
                    lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                    if lock_acquired:
                        try:
                            app_state.error_count_total += 1; app_state.error_counts_per_api[api_slot_idx] += 1
                            err_summary = f"T{thread_id} Ans-GenExc (API{api_slot_idx+1}) A{api_call_attempt_num+1}: {str(e_gen)[:20]}"
                            if len(app_state.recent_errors_total) >= MAX_RECENT: app_state.recent_errors_total.pop(0)
                            app_state.recent_errors_total.append((err_summary, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_errors_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_errors_per_api[api_slot_idx].pop(0)
                                app_state.recent_errors_per_api[api_slot_idx].append(err_summary)
                        finally:
                            app_state.system_prompt_lock.release()
                    if api_call_attempt_num < api_call_retries_for_this_iteration - 1:
                        time.sleep(random.uniform(0.5, 1.5)); continue
                    else: break

            if answer is None:
                log_message(f"Thread {thread_id}: All API call attempts failed for current content iteration (OuterAttempt {attempt+1}, API Slot {api_slot_idx+1}).", "WARNING")
                break

            # --- Issue Detection ---
            issue_detected_this_main_api_call = False
            refusal_detected, refusal_info = detection.is_refusal(answer, refusal_phrases_local)
            user_speaking_detected, user_speaking_info = False, []
            if not no_user_impersonation_local:
                user_speaking_detected, user_speaking_info = detection.is_user_speaking(answer, user_speaking_phrases_local)
            slop_detected, slop_info = detection.is_slop(answer, slop_phrases_local)

            if refusal_detected:
                issue_detected_this_main_api_call = True
                issue_ever_detected_this_task = True
                refusal_detected_this_main_api_call = True
                refusal_ever_detected_this_task = True
                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired:
                    try:
                        app_state.refusal_count_total += 1
                        app_state.refusal_counts_per_api[api_slot_idx] += 1
                        if refusal_info:
                            detected_phrase, detected_sentence = refusal_info[0]
                            if len(app_state.recent_refusals_total) >= MAX_RECENT: app_state.recent_refusals_total.pop(0)
                            app_state.recent_refusals_total.append((detected_phrase, detected_sentence, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_refusals_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_refusals_per_api[api_slot_idx].pop(0)
                                app_state.recent_refusals_per_api[api_slot_idx].append((detected_phrase, detected_sentence))
                    finally:
                        app_state.system_prompt_lock.release()
                if fix_attempts_specific['refusal'] < len(jailbreaks_local):
                    current_system_prompt_iter += f" {jailbreaks_local[fix_attempts_specific['refusal']]}"
                    fix_attempts_specific['refusal'] += 1
                    log_message(f"Thread {thread_id}: Refusal detected (API Slot {api_slot_idx+1}). Applying jailbreak {fix_attempts_specific['refusal']}. Retrying API call.", "DEBUG")
                    continue
                else:
                    log_message(f"Thread {thread_id}: Refusal detected (API Slot {api_slot_idx+1}), jailbreaks exhausted for this attempt {attempt+1}.", "WARNING")
                    break

            if user_speaking_detected:
                issue_detected_this_main_api_call = True
                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired:
                    try:
                        app_state.user_speaking_count_total += 1
                        app_state.user_speaking_counts_per_api[api_slot_idx] += 1
                        if user_speaking_info:
                            detected_phrase, detected_sentence = user_speaking_info[0]
                            if len(app_state.recent_user_speaking_total) >= MAX_RECENT: app_state.recent_user_speaking_total.pop(0)
                            app_state.recent_user_speaking_total.append((detected_phrase, detected_sentence, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_user_speaking_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_user_speaking_per_api[api_slot_idx].pop(0)
                                app_state.recent_user_speaking_per_api[api_slot_idx].append((detected_phrase, detected_sentence))
                    finally:
                        app_state.system_prompt_lock.release()
                if fix_attempts_specific['user_speaking'] < len(speaking_fixes_local):
                    current_system_prompt_iter += f" {speaking_fixes_local[fix_attempts_specific['user_speaking']]}"
                    fix_attempts_specific['user_speaking'] += 1
                    log_message(f"Thread {thread_id}: User speaking detected (API Slot {api_slot_idx+1}). Applying fix {fix_attempts_specific['user_speaking']}. Retrying API call.", "DEBUG")
                    continue
                else:
                    log_message(f"Thread {thread_id}: User speaking detected (API Slot {api_slot_idx+1}), fixes exhausted for this attempt {attempt+1}.", "WARNING")
                    break

            # Initialize slop fix tracking BEFORE slop detection so fallback logic can reference them
            slop_fully_resolved_by_sentence_fixer = False
            current_answer_being_fixed = answer

            if slop_detected:
                issue_detected_this_main_api_call = True
                log_message(f"Thread {thread_id}: Initial slop detected in answer (API Slot {api_slot_idx+1}). Snippet: {answer[:70]}...", "DEBUG")

                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired:
                    try:
                        app_state.slop_count_total += 1
                        app_state.slop_counts_per_api[api_slot_idx] += 1
                        if slop_info:
                            detected_phrase, detected_sentence = slop_info[0]
                            if len(app_state.recent_slop_total) >= MAX_RECENT: app_state.recent_slop_total.pop(0)
                            app_state.recent_slop_total.append((detected_phrase, detected_sentence, api_slot_idx))
                            if api_slot_idx < 6:
                                if len(app_state.recent_slop_per_api[api_slot_idx]) >= MAX_RECENT: app_state.recent_slop_per_api[api_slot_idx].pop(0)
                                app_state.recent_slop_per_api[api_slot_idx].append((detected_phrase, detected_sentence))
                    finally:
                        app_state.system_prompt_lock.release()

                if slop_fixer_api_config_param and slop_fixer_api_config_param.get('url'):
                    MAX_SENTENCE_FIX_ITERATIONS = global_config.get('generation.max_slop_sentence_fix_iterations', 4)
                    # slop_fully_resolved_by_sentence_fixer and current_answer_being_fixed already initialized above
                    slop_fix_instruction_rotation_idx = 0

                    for slop_iter_num in range(MAX_SENTENCE_FIX_ITERATIONS):
                        if app_state.stop_processing or app_state.pause_processing: return None
                        current_slop_check_needed, current_slop_details_iter = detection.is_slop(current_answer_being_fixed, slop_phrases_local)
                        if not current_slop_check_needed:
                            log_message(f"Thread {thread_id}: Slop paragraph fully resolved after {slop_iter_num} iterations.", "INFO")
                            answer = current_answer_being_fixed
                            issue_detected_this_main_api_call = False
                            slop_fully_resolved_by_sentence_fixer = True
                            break

                        phrase_to_fix_iter, sentence_to_fix_iter = current_slop_details_iter[0]

                        phrase_to_fix_iter, sentence_to_fix_iter = current_slop_details_iter[0]

                        # Extract the full paragraph context to preserve quotes
                        paragraphs = current_answer_being_fixed.split('\n\n')
                        context_for_fixer = next((p for p in paragraphs if sentence_to_fix_iter in p), sentence_to_fix_iter)

                        additional_instructions_for_llm_fixer = ""
                        if slop_iter_num >= 2 and current_slop_fixes_for_rotation_param:
                            additional_instructions_for_llm_fixer = current_slop_fixes_for_rotation_param[slop_fix_instruction_rotation_idx % len(current_slop_fixes_for_rotation_param)]
                            slop_fix_instruction_rotation_idx +=1
                            log_message(f"Thread {thread_id}: SlopFixer iter {slop_iter_num+1}. Adding rotating fix: '{additional_instructions_for_llm_fixer}'", "DEBUG")

                        log_message(f"Thread {thread_id}: Fixing slop paragraph (Iter {slop_iter_num+1}): '{phrase_to_fix_iter}' in context...", "DEBUG")

                        rewritten_sentence_part, original_sentence_part = call_slop_fixer_llm(
                            context_for_fixer, phrase_to_fix_iter, # Pass paragraph instead of single sentence
                            slop_fixer_api_config_param,
                            sampler_settings_local,
                            thread_id,
                            additional_fix_instructions=additional_instructions_for_llm_fixer,
                            current_max_attempts_param=current_max_attempts_for_slop_fixer_call,
                            file_name=file_name
                        )

                        if rewritten_sentence_part and original_sentence_part:
                            has_incomplete_quote, _ = detection.is_incomplete_quote(rewritten_sentence_part)
                            if has_incomplete_quote:
                                log_message(f"Thread {thread_id}: Slop fixer returned sentence with unbalanced quotes. Skipping replacement for this sentence.", "WARNING")
                                slop_fully_resolved_by_sentence_fixer = False
                                break # Stop this sentence's fix attempt to prevent propagating malformed quotes
                            if original_sentence_part in current_answer_being_fixed:
                                if rewritten_sentence_part.strip() == original_sentence_part.strip():
                                    log_message(f"Thread {thread_id}: Slop fixer returned same part for '{phrase_to_fix_iter}'. Iter {slop_iter_num+1}.", "DEBUG")
                                    slop_fully_resolved_by_sentence_fixer = False
                                    break
                                else:
                                    current_answer_being_fixed = current_answer_being_fixed.replace(original_sentence_part, rewritten_sentence_part, 1)
                                    log_message(f"Thread {thread_id}: Sentence part rewritten. New snippet: {current_answer_being_fixed[:70]}...", "DEBUG")
                            else:
                                log_message(f"Thread {thread_id}: Original sentence for slop fix ('{original_sentence_part[:70]}...') not found in current answer. Iter {slop_iter_num+1}.", "WARNING")
                                slop_fully_resolved_by_sentence_fixer = False
                                break
                        else:
                            log_message(f"Thread {thread_id}: Slop fixer LLM failed rewrite for '{phrase_to_fix_iter}'. Aborting sentence fixing.", "WARNING")
                            slop_fully_resolved_by_sentence_fixer = False
                            break

                    if not slop_fully_resolved_by_sentence_fixer:
                        log_message(f"Thread {thread_id}: Sentence-level slop fixing failed or max iters for API {api_slot_idx+1}. Slop may remain. Attempting fallback system prompt fix.", "WARNING")
                    else:
                        answer = current_answer_being_fixed
                        issue_detected_this_main_api_call = False

            # --- NEW: Slop → Anti-Slop Fallback ---
            # If slop was detected but not fully resolved by the Slop Fixer,
            # try the Anti-Slop API as a final attempt (1 attempt only).
            if slop_to_anti_slop_fallback and slop_detected and not slop_fully_resolved_by_sentence_fixer:
                if anti_slop_fixer_api_config_param and anti_slop_fixer_api_config_param.get('url'):
                    log_message(f"Thread {thread_id}: Slop fixer failed to fully resolve slop. Attempting Anti-Slop fallback (1 attempt) for API Slot {api_slot_idx+1}.", "INFO")

                    # Use the best available version of the answer
                    fallback_answer = current_answer_being_fixed

                    # Check for remaining slop phrases
                    remaining_slop_check, remaining_slop_details = detection.is_slop(fallback_answer, slop_phrases_local)
                    if remaining_slop_check and remaining_slop_details:
                        fb_phrase = remaining_slop_details[0][0]
                        fb_sentence = remaining_slop_details[0][1]

                        # Extract paragraph context to preserve quotes
                        fb_paragraphs = fallback_answer.split('\n\n')
                        fb_context = next((p for p in fb_paragraphs if fb_sentence in p), fb_sentence)

                        log_message(f"Thread {thread_id}: Anti-Slop fallback attempting to fix: '{fb_phrase}' in context...", "DEBUG")

                        rewritten_fb, original_fb = call_anti_slop_llm(
                            fb_context, fb_phrase,
                            anti_slop_fixer_api_config_param,
                            sampler_settings_local, thread_id,
                            additional_fix_instructions="This is a SLOP phrase that needs to be rephrased. Remove or rephrase this undesirable phrase while preserving the original meaning, tone, and ALL quotation marks. ONLY output the rewritten text.",
                            current_max_attempts_param=current_max_attempts_for_slop_fixer_call,
                            master_duplication_enabled=master_duplication_enabled_local,
                            file_name=file_name
                        )

                        if rewritten_fb and original_fb:
                            # Check for unbalanced quotes before applying
                            has_incomplete_quote_fb, _ = detection.is_incomplete_quote(rewritten_fb)
                            if has_incomplete_quote_fb:
                                log_message(f"Thread {thread_id}: Anti-Slop fallback returned unbalanced quotes. Skipping replacement.", "WARNING")
                            elif original_fb in fallback_answer:
                                if rewritten_fb.strip() == original_fb.strip():
                                    log_message(f"Thread {thread_id}: Anti-Slop fallback returned identical text for '{fb_phrase}'. Fallback failed.", "WARNING")
                                else:
                                    fallback_answer = fallback_answer.replace(original_fb, rewritten_fb, 1)
                                    log_message(f"Thread {thread_id}: Anti-Slop fallback replacement applied. Re-checking for slop...", "INFO")

                                    # Re-check if slop is fully resolved
                                    final_slop_check, _ = detection.is_slop(fallback_answer, slop_phrases_local)
                                    if not final_slop_check:
                                        answer = fallback_answer
                                        issue_detected_this_main_api_call = False
                                        slop_fully_resolved_by_sentence_fixer = True
                                        log_message(f"Thread {thread_id}: Slop fully resolved by Anti-Slop fallback.", "INFO")
                                    else:
                                        log_message(f"Thread {thread_id}: Slop still present after Anti-Slop fallback.", "WARNING")
                                        answer = fallback_answer  # Use partially fixed version
                            else:
                                log_message(f"Thread {thread_id}: Anti-Slop fallback - original sentence not found in answer. Fallback failed.", "WARNING")
                    else:
                        log_message(f"Thread {thread_id}: Anti-Slop fallback - no remaining slop detected (may have been resolved during iteration).", "DEBUG")
                        # Slop was actually resolved during iterations but flag wasn't updated
                        slop_fully_resolved_by_sentence_fixer = True
                        answer = fallback_answer
                        issue_detected_this_main_api_call = False
                else:
                    log_message(f"Thread {thread_id}: Slop → Anti-Slop fallback enabled but Anti-Slop API (Slot 6) not configured.", "WARNING")
            # --- END: Slop → Anti-Slop Fallback ---

            # --- Anti-Slop Detection and Fixing (Sentence-Level, Like Regular Slop) ---
            anti_slop_detected, anti_slop_info = detection.is_anti_slop(answer, current_anti_slop_phrases_param)

            # FIX 1: Initialize BEFORE the conditional block
            anti_slop_fully_resolved = False

            if anti_slop_detected:
                issue_detected_this_main_api_call = True
                log_message(f"Thread {thread_id}: Anti-slop detected in answer (API Slot {api_slot_idx+1}). Snippet: {answer[:70]}...", "DEBUG")

                lock_acquired = app_state.system_prompt_lock.acquire(timeout=0.05)
                if lock_acquired:
                    try:
                        app_state.anti_slop_count_total += 1
                        anti_slop_counts_per_api[api_slot_idx] += 1
                        if anti_slop_info:
                            detected_phrase, detected_sentence = anti_slop_info[0]
                            if len(app_state.recent_anti_slop_total) >= MAX_RECENT:
                                app_state.recent_anti_slop_total.pop(0)
                            app_state.recent_anti_slop_total.append((detected_phrase, detected_sentence, api_slot_idx))
                            if api_slot_idx < 4:
                                if len(app_state.recent_anti_slop_per_api[api_slot_idx]) >= MAX_RECENT:
                                    app_state.recent_anti_slop_per_api[api_slot_idx].pop(0)
                                    app_state.recent_anti_slop_per_api[api_slot_idx].append((detected_phrase, detected_sentence))
                    finally:
                        app_state.system_prompt_lock.release()

                # Try to fix using anti-slop LLM - fix individual SENTENCES (like regular slop)
                if slop_fixer_api_config_param and slop_fixer_api_config_param.get('url'):
                    current_answer_being_fixed = answer
                    MAX_ANTI_SLOP_FIX_ITERATIONS = global_config.get('generation.max_anti_slop_fix_iterations', 10)
                    # anti_slop_fully_resolved = False  # REMOVED - already initialized above
                    anti_slop_fix_instruction_rotation_idx = 0

                    for anti_slop_iter_num in range(MAX_ANTI_SLOP_FIX_ITERATIONS):
                        if app_state.stop_processing or app_state.pause_processing:
                            return None

                        current_anti_slop_check, current_anti_slop_details = detection.is_anti_slop(current_answer_being_fixed, current_anti_slop_phrases_param)

                        if not current_anti_slop_check:
                            log_message(f"Thread {thread_id}: All anti-slop fixed (API Slot {api_slot_idx+1}) after {anti_slop_iter_num} rewrites.", "INFO")
                            answer = current_answer_being_fixed
                            issue_detected_this_main_api_call = False
                            anti_slop_fully_resolved = True
                            break

                        phrase_to_fix = current_anti_slop_details[0][0]
                        sentence_to_fix = current_anti_slop_details[0][1]

                        # Extract the full paragraph context to preserve quotes
                        paragraphs = current_answer_being_fixed.split('\n\n')
                        context_for_fixer = next((p for p in paragraphs if sentence_to_fix in p), sentence_to_fix)

                        additional_instructions = ""
                        if anti_slop_iter_num >= 1 and current_slop_fixes_for_rotation_param:
                            additional_instructions = current_slop_fixes_for_rotation_param[anti_slop_iter_num % len(current_slop_fixes_for_rotation_param)]
                            log_message(f"Thread {thread_id}: AntiSlop iter {anti_slop_iter_num+1}. Adding fix: '{additional_instructions}'", "DEBUG")

                        log_message(f"Thread {thread_id}: Fixing anti-slop paragraph (Iter {anti_slop_iter_num+1}): '{phrase_to_fix}' in context...", "DEBUG")


                        rewritten_sentence, original_sentence = call_anti_slop_llm(
                            context_for_fixer, # Pass paragraph instead of single sentence
                            phrase_to_fix,
                            anti_slop_fixer_api_config_param,
                            sampler_settings_local,
                            thread_id,
                            additional_fix_instructions=additional_instructions,
                            current_max_attempts_param=current_max_attempts_for_slop_fixer_call,
                            master_duplication_enabled=master_duplication_enabled_local,
                            file_name=file_name
                        )

                        if rewritten_sentence and original_sentence:
                            # NEW: Check for unbalanced quotes before applying the rewrite
                            has_incomplete_quote, _ = detection.is_incomplete_quote(rewritten_sentence)
                            if has_incomplete_quote:
                                log_message(f"Thread {thread_id}: Anti-slop fixer returned sentence with unbalanced quotes. Skipping replacement.", "WARNING")
                                anti_slop_fully_resolved = False
                                break
                            if original_sentence in current_answer_being_fixed:
                                if rewritten_sentence.strip() == original_sentence.strip():
                                    log_message(f"Thread {thread_id}: Anti-slop fixer returned same sentence for '{phrase_to_fix}'. Iter {anti_slop_iter_num+1}.", "DEBUG")
                                    anti_slop_fully_resolved = False
                                    break
                                else:
                                    current_answer_being_fixed = current_answer_being_fixed.replace(original_sentence, rewritten_sentence, 1)
                                    log_message(f"Thread {thread_id}: Sentence rewritten. New snippet: {current_answer_being_fixed[:70]}...", "DEBUG")
                            else:
                                log_message(f"Thread {thread_id}: Original sentence for anti-slop fix not found in current answer. Iter {anti_slop_iter_num+1}.", "WARNING")
                                anti_slop_fully_resolved = False
                                break
                        else:
                            log_message(f"Thread {thread_id}: Anti-slop LLM failed rewrite for '{phrase_to_fix}'. Aborting fix.", "WARNING")
                            anti_slop_fully_resolved = False
                            break

                    if not anti_slop_fully_resolved:
                        log_message(f"Thread {thread_id}: Anti-slop sentence fixing failed or max iters for API {api_slot_idx+1}.", "WARNING")
                    else:
                        answer = current_answer_being_fixed
                        issue_detected_this_main_api_call = False

                if issue_detected_this_main_api_call:
                    slop_check_after_sentence_fix, _ = detection.is_slop(answer, slop_phrases_local)
                    if slop_check_after_sentence_fix:
                        if fix_attempts_specific['slop_fallback'] < len(slop_fixes_fallback_local):
                            current_system_prompt_iter += f" {slop_fixes_fallback_local[fix_attempts_specific['slop_fallback']]}"
                            fix_attempts_specific['slop_fallback'] += 1
                            log_message(f"Thread {thread_id}: Applying fallback slop fix (system prompt) {fix_attempts_specific['slop_fallback']} for API {api_slot_idx+1}. Retrying API call.", "DEBUG")
                            continue
                        else:
                            log_message(f"Thread {thread_id}: Slop detected (API {api_slot_idx+1}), sentence fixer failed/skipped, and fallback system prompt fixes exhausted for attempt {attempt+1}.", "WARNING")
                            break
                    else:
                        if anti_slop_detected and not anti_slop_fully_resolved:
                            log_message(f"Thread {thread_id}: Anti-slop still unresolved after fixer attempt.", "WARNING")
                        else:
                            issue_detected_this_main_api_call = False
                incomplete_quote_detected, _ = detection.is_incomplete_quote(answer)
                if incomplete_quote_detected:
                    issue_detected_this_main_api_call = True
                    log_message(f"Thread {thread_id}: Incomplete quote detected (API Slot {api_slot_idx+1}). Retrying with fix instruction.", "DEBUG")

                    if fix_attempts_specific['incomplete_quote'] < 3:
                        current_system_prompt_iter += " CRITICAL INSTRUCTION: All dialogue quotes must be properly paired. Ensure every opening quote has a matching closing quote."
                        fix_attempts_specific['incomplete_quote'] += 1
                        continue
                    else:
                        log_message(f"Thread {thread_id}: Incomplete quote detected, max retries reached. Applying programmatic fix.", "WARNING")

                        # ROBUST PROGRAMMATIC FALLBACK: Auto-fix unbalanced quotes
                        # Uses the upgraded structural balancer instead of simple parity counting
                        answer = text_utils.normalize_quotes(answer.strip())
                        issue_detected_this_main_api_call = False  # Mark as resolved so it saves

            if not issue_detected_this_main_api_call:
                log_message(f"Thread {thread_id}: Successfully generated answer for attempt {attempt + 1} (API Slot {api_slot_idx+1}).", "INFO")
                return answer, issue_ever_detected_this_task, refusal_ever_detected_this_task
    # Safety exit: an issue was flagged but no specific fix path triggered
    # a break/continue (e.g., slop detected but no Slop Fixer API configured).
    # Without this, the while True would loop indefinitely.
            break

        current_system_prompt_iter = base_system_prompt
        log_message(f"Thread {thread_id}: Main attempt {attempt + 1} failed for API {api_url_local} (Slot {api_slot_idx+1}). Resetting system prompt for next attempt if any.", "WARNING")
        if attempt < max_attempts_local - 1 :
            time.sleep(random.uniform(0.5, 1.5))

    log_message(f"Thread {thread_id}, API Slot {api_slot_idx+1}: All {max_attempts_local} attempts failed to generate a valid answer for the current turn. Returning None.", "ERROR")
    return None, issue_ever_detected_this_task, refusal_ever_detected_this_task


def write_conversation(output_file_path_base, # Not used directly, BASE_OUTPUT_FILE_PATH is used
                       conversation_history,
                       remove_reasoning_flag,
                       remove_em_dash_flag,
                       remove_asterisks_flag,
                       remove_asterisk_space_asterisk_flag,
                       remove_all_asterisks_flag,
                       ensure_space_after_line_break_flag,
                       remove_markdown_flag,
                       output_format_local,
                       task_id_for_output="unknown",
                       api_slot_idx_for_output_file=None, # For per-API files in duplication mode
                       is_duplication_turn=False, # Flag if this write is for a single turn in duplication mode
                       turn_number_for_duplication=0 # Turn number (1-based) for duplication mode output ID
                       ):
    """
    Writes a completed conversation (or a single turn in duplication mode) to the output JSONL file.
    - If api_slot_idx_for_output_file is provided AND master duplication is ON, writes to a per-API file.
    - Otherwise (non-duplication, or duplication off), writes to the main output.jsonl.
    """
    processed_conversation_turns = []
    for turn in conversation_history:
        role = turn.get("role")
        content = turn.get("content", "")
        # DEBUG: Log content before processing to check if issue starts at API response
        if role == "assistant" and (content.islower() and not any(c in content for c in '.!?')):
            log_message(f"DEBUG: API response already lowercase with no punctuation! Task ID: {task_id_for_output}", "WARNING")
            log_message(f"DEBUG: Raw content: {content[:200]}", "DEBUG")
        processed_content = text_utils.remove_reasoning_text(content) if remove_reasoning_flag else content
        processed_content = text_utils.remove_em_dash(processed_content) if remove_em_dash_flag else processed_content
        processed_content = text_utils.remove_excessive_asterisks(processed_content) if remove_asterisks_flag else processed_content
        processed_content = text_utils.remove_asterisk_space_asterisk(processed_content) if remove_asterisk_space_asterisk_flag else processed_content
        processed_content = text_utils.remove_all_asterisks(processed_content) if remove_all_asterisks_flag else processed_content
        processed_content = text_utils.ensure_space_after_line_break(processed_content) if ensure_space_after_line_break_flag else processed_content
        processed_content = text_utils.remove_markdown(processed_content) if remove_markdown_flag else processed_content
        processed_content = text_utils.repair_straight_quotes(processed_content)
        processed_content = text_utils.normalize_quotes(processed_content)

        # Convert roles for ShareGPT format
        sg_role = "human" if role == "user" else "gpt" if role == "assistant" else role
        processed_conversation_turns.append({"from": sg_role, "value": processed_content})

    output_data_id = task_id_for_output
    if is_duplication_turn and app_state.master_duplication_enabled_var.get(): # Check global var for safety
        output_data_id = f"{task_id_for_output}_api{api_slot_idx_for_output_file}_turn{turn_number_for_duplication}"

    output_data = {
        "id": output_data_id,
        "conversations": processed_conversation_turns
    }

    use_db = global_config.get('database.enabled', False)

    if use_db and app_state.db_pool:
        try:
            conn = app_state.db_pool.getconn()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO generated_conversations (task_id, conversation_data, api_slot_idx)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (task_id) DO NOTHING
                """, (output_data_id, json.dumps(output_data), api_slot_idx_for_output_file))
                conn.commit()
            app_state.db_pool.putconn(conn)
            log_message(f"Saved task {task_id_for_output} to PostgreSQL.", "DEBUG")
        except Exception as e:
            log_message(f"DB insert failed for {task_id_for_output}: {e}", "ERROR")
            if 'conn' in locals(): app_state.db_pool.putconn(conn)

        # 🔑 CRITICAL: Exit function immediately after DB save.
        # This guarantees the file-writing code below NEVER runs when DB is enabled.
        return

    # --- FILE WRITING (Only reached if DB is DISABLED or pool failed to init) ---
    actual_output_file_path = ""
    if api_slot_idx_for_output_file is not None and app_state.master_duplication_enabled_var.get():
        actual_output_file_path = f"{BASE_OUTPUT_FILE_PATH}_api_slot_{api_slot_idx_for_output_file}.jsonl"
    else:
        actual_output_file_path = BASE_OUTPUT_FILE_PATH + ".jsonl"

    try:
        with open(actual_output_file_path, 'a', encoding='utf-8') as file:
            file.write(json.dumps(output_data) + '\n')
        log_message(f"Successfully wrote task {task_id_for_output} to {actual_output_file_path}", "DEBUG")
    except PermissionError as e:
        log_message(f"Permission error writing to {actual_output_file_path}: {e}", "ERROR")
    except OSError as e:
        log_message(f"OS error writing to {actual_output_file_path}: {e}", "ERROR")
    except Exception as e:
        log_message(f"Unexpected error writing to {actual_output_file_path}: {e}", "ERROR")
        import traceback
        log_message(traceback.format_exc(), "ERROR")
