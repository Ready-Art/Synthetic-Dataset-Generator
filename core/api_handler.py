# core/api_handler.py
import requests
import json
import time
import random
import traceback
import state
from utils.valkey_client import ValkeyKeys
from utils.file_io import log_message, BASE_DEBUG_LOG_PATH
from utils.text_processing import is_refusal, is_user_speaking, is_slop, update_question_history

def _make_api_call(api_config, messages, sampler_settings, thread_id, call_type, api_slot_idx, attempt_num, max_attempts, valkey_client):
    """A generic, centralized function to make an API call with error handling and logging."""
    if not api_config.get('url'):
        log_message(f"Thread {thread_id}: API URL missing for {call_type} (Slot {api_slot_idx+1}).", "ERROR")
        return None

    valkey_client.incr(ValkeyKeys.STAT_TOTAL_ATTEMPTS)
    valkey_client.incr(ValkeyKeys.stat_total_attempts_per_api(api_slot_idx))

    payload = {
        "model": api_config.get('model'),
        "messages": messages,
        "stream": False,
        **sampler_settings
    }
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {api_config.get('key')}"
    }

    debug_log_path = f"{BASE_DEBUG_LOG_PATH}_api_slot_{api_slot_idx}.jsonl"
    with open(debug_log_path, 'a', encoding='utf-8') as f:
        log_entry = {"timestamp": time.time(), "thread_id": thread_id, "type": call_type, "api_slot_idx": api_slot_idx, "attempt": attempt_num, "payload": payload}
        f.write(json.dumps(log_entry) + '\n')

    try:
        response = requests.post(api_config['url'], headers=headers, json=payload, timeout=120)
        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content'].strip()
        else:
            error_msg = f"API Error {call_type} (Slot {api_slot_idx+1}, Att {attempt_num}/{max_attempts}, Status {response.status_code}): {response.text[:200]}"
            log_message(f"Thread {thread_id}: {error_msg}", "ERROR")
            _record_error(thread_id, api_slot_idx, f"{call_type}-Err S{response.status_code}", valkey_client)
            return None
    except requests.exceptions.Timeout:
        log_message(f"Thread {thread_id}: API Timeout on {call_type} (Slot {api_slot_idx+1}, Att {attempt_num}/{max_attempts})", "ERROR")
        _record_error(thread_id, api_slot_idx, f"{call_type}-Timeout", valkey_client)
        return None
    except Exception as e:
        log_message(f"Thread {thread_id}: Exception in {call_type} (Slot {api_slot_idx+1}, Att {attempt_num}/{max_attempts}): {e}", "ERROR")
        log_message(traceback.format_exc(), "DEBUG")
        _record_error(thread_id, api_slot_idx, f"{call_type}-Exc", valkey_client)
        return None

def generate_question(system_prompt, question_prompt_template, subject, context, api_config, history_size, max_attempts, thread_id, api_slot_idx, valkey_client):
    """Generates an initial question using the LLM."""
    for attempt in range(1, max_attempts + 1):
        if state.stop_processing: return None
        
        recent_questions = "\n- ".join(valkey_client.lrange(ValkeyKeys.QUESTION_HISTORY, 0, history_size))
        formatted_prompt = question_prompt_template.replace("{recent_questions}", recent_questions).replace("{subject}", subject).replace("{context}", context)
        
        messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": formatted_prompt}]
        
        sampler_settings = api_config.get('sampler_settings', {}).get('generation_params', {})
        sampler_settings['max_tokens'] = api_config.get('sampler_settings', {}).get('max_tokens_question', 256)
        
        question = _make_api_call(api_config, messages, sampler_settings, thread_id, "question", api_slot_idx, attempt, max_attempts, valkey_client)
        
        if question:
            update_question_history(question, history_size, valkey_client)
            return question
        time.sleep(random.uniform(0.5, 1.5))
    return None

def generate_user_continuation(system_prompt, conversation_history, continuation_prompt_template, api_config, max_attempts, thread_id, api_slot_idx, valkey_client):
    """Generates the user's continuation reply."""
    for attempt in range(1, max_attempts + 1):
        if state.stop_processing: return None

        last_assistant_msg = conversation_history[-1]['content'] if conversation_history and conversation_history[-1]['role'] == 'assistant' else ""
        formatted_prompt = continuation_prompt_template.replace("{last_assistant_message}", last_assistant_msg)
        
        messages = [{"role": "system", "content": system_prompt}] + conversation_history + [{"role": "user", "content": formatted_prompt}]

        sampler_settings = api_config.get('sampler_settings', {}).get('generation_params', {})
        sampler_settings['max_tokens'] = api_config.get('sampler_settings', {}).get('max_tokens_user_reply', 256)

        reply = _make_api_call(api_config, messages, sampler_settings, thread_id, "user_continuation", api_slot_idx, attempt, max_attempts, valkey_client)
        
        if reply:
            return reply
        time.sleep(random.uniform(0.5, 1.5))
    return None

def call_slop_fixer_llm(original_sentence, slop_phrase, slop_fixer_config, main_sampler_settings, thread_id, max_attempts, valkey_client, additional_fix=""):
    """Calls the dedicated Slop Fixer LLM (API Slot 5)."""
    if not slop_fixer_config or not slop_fixer_config.get('url'):
        return None

    api_slot_idx = 4 # Slop fixer is always API slot 5 (index 4)
    for attempt in range(1, max_attempts + 1):
        if state.stop_processing: return None

        instruction = (
            f"The sentence contains an undesirable phrase: '{slop_phrase}'. "
            f"Rewrite the sentence to remove this phrase, preserving the original meaning. "
            f"Only output the rewritten sentence. No preamble."
        )
        if additional_fix: instruction += f"\n\nInstruction: {additional_fix}"
        instruction += f"\n\nOriginal sentence: \"{original_sentence}\""

        messages = [
            {"role": "system", "content": "You are an expert editor. Rewrite the sentence as instructed."},
            {"role": "user", "content": instruction}
        ]
        
        sampler_overrides = main_sampler_settings.get("slop_fixer_params", {})
        sampler_settings = {
            "temperature": sampler_overrides.get("temperature", 0.5),
            "top_p": sampler_overrides.get("top_p", 0.95),
            "max_tokens": sampler_overrides.get("max_tokens", len(original_sentence.split()) * 3 + 70),
        }

        rewritten = _make_api_call(slop_fixer_config, messages, sampler_settings, thread_id, "slop_fix", api_slot_idx, attempt, max_attempts, valkey_client)

        if rewritten:
            # Clean up quotes that models sometimes add
            if rewritten.startswith('"') and rewritten.endswith('"'):
                rewritten = rewritten[1:-1]
            return rewritten
        time.sleep(random.uniform(0.5, 1.5))
    return None

def generate_answer_with_retries(params):
    """
    Generates an assistant's answer, handling retries for content issues.
    This is a complex state machine for ensuring response quality.
    """
    base_system_prompt = params['base_system_prompt']
    api_config = params['api_config']
    api_slot_idx = params['api_slot_idx']
    thread_id = params['thread_id']
    max_attempts = params['max_attempts']
    valkey_client = params['valkey_client']
    
    current_system_prompt = base_system_prompt
    
    for attempt in range(1, max_attempts + 1):
        if state.stop_processing: return None

        messages = [{"role": "system", "content": current_system_prompt}] + params['conversation_history'] + [{"role": "user", "content": params['answer_prompt_template']}]
        
        sampler_settings = api_config.get('sampler_settings', {}).get('generation_params', {})
        sampler_settings['max_tokens'] = api_config.get('sampler_settings', {}).get('max_tokens_answer', 1024)
        
        answer = _make_api_call(api_config, messages, sampler_settings, thread_id, "answer", api_slot_idx, attempt, max_attempts, valkey_client)

        if not answer:
            time.sleep(random.uniform(0.5, 1.5))
            continue # API call failed, try next main attempt

        # --- Issue Detection & Handling ---
        refusal, ref_info = is_refusal(answer, params['refusal_phrases'])
        user_speak, speak_info = is_user_speaking(answer, params['user_speaking_phrases'])
        slop, slop_info = is_slop(answer, params['slop_phrases'])

        if refusal:
            _record_issue('refusal', thread_id, api_slot_idx, ref_info, valkey_client)
            if params['jailbreaks']:
                current_system_prompt += f" {random.choice(params['jailbreaks'])}"
                log_message(f"T{thread_id} API{api_slot_idx+1}: Refusal detected. Applying jailbreak. Re-attempting.", "DEBUG")
                continue
        
        if user_speak and not params['no_user_impersonation']:
            _record_issue('user_speaking', thread_id, api_slot_idx, speak_info, valkey_client)
            if params['speaking_fixes']:
                current_system_prompt += f" {random.choice(params['speaking_fixes'])}"
                log_message(f"T{thread_id} API{api_slot_idx+1}: User speaking detected. Applying fix. Re-attempting.", "DEBUG")
                continue

        if slop:
            _record_issue('slop', thread_id, api_slot_idx, slop_info, valkey_client)
            fixed_answer = _handle_slop_fixing(answer, slop_info, params)
            if fixed_answer:
                return fixed_answer # Slop was successfully fixed
            else: # Slop fixing failed, try a system prompt fix and regenerate
                if params['slop_fixes_fallback']:
                    current_system_prompt += f" {random.choice(params['slop_fixes_fallback'])}"
                    log_message(f"T{thread_id} API{api_slot_idx+1}: Slop fixing failed. Applying fallback system prompt fix. Re-attempting.", "DEBUG")
                    continue
        
        if not refusal and not (user_speak and not params['no_user_impersonation']) and not slop:
            return answer # Good response

    log_message(f"T{thread_id} API{api_slot_idx+1}: All {max_attempts} attempts failed to generate a valid answer.", "ERROR")
    return None

def _handle_slop_fixing(answer, slop_info, params):
    """Iteratively tries to fix slop using the Slop Fixer LLM."""
    valkey_client = params['valkey_client']
    if not params.get('slop_fixer_api_config') or not params['slop_fixer_api_config'].get('url'):
        return None # No fixer configured

    current_answer = answer
    max_fix_iters = params.get('max_slop_fix_iters', 5)

    for i in range(max_fix_iters):
        is_slop_present, current_slop_info = is_slop(current_answer, params['slop_phrases'])
        if not is_slop_present:
            return current_answer # All slop has been fixed

        phrase, sentence = current_slop_info[0]
        rewritten_sentence = call_slop_fixer_llm(
            sentence, phrase, params['slop_fixer_api_config'], 
            params['api_config'].get('sampler_settings', {}),
            params['thread_id'], params['max_attempts'], valkey_client
        )
        if rewritten_sentence and rewritten_sentence != sentence:
            current_answer = current_answer.replace(sentence, rewritten_sentence, 1)
        else:
            return None # Fixer failed to rewrite
    return None # Max iterations reached, slop likely remains

def _record_issue(issue_type, thread_id, api_slot_idx, issue_info, valkey_client):
    """Helper to record statistics for a detected issue in Valkey."""
    MAX_RECENT = 10
    issue_data = issue_info[0] # [phrase, sentence]

    if issue_type == 'refusal':
        valkey_client.incr(ValkeyKeys.STAT_REFUSAL_TOTAL)
        valkey_client.incr(ValkeyKeys.stat_refusal_per_api(api_slot_idx))
        valkey_client.push_to_list_with_trim(ValkeyKeys.RECENT_REFUSALS_TOTAL, json.dumps([issue_data, api_slot_idx]), MAX_RECENT)
        if api_slot_idx < 4:
            valkey_client.push_to_list_with_trim(ValkeyKeys.recent_refusals_per_api(api_slot_idx), json.dumps(issue_data), MAX_RECENT)

    elif issue_type == 'user_speaking':
        valkey_client.incr(ValkeyKeys.STAT_USER_SPEAK_TOTAL)
        valkey_client.incr(ValkeyKeys.stat_user_speaking_per_api(api_slot_idx))
        valkey_client.push_to_list_with_trim(ValkeyKeys.RECENT_USER_SPEAKING_TOTAL, json.dumps([issue_data, api_slot_idx]), MAX_RECENT)
        if api_slot_idx < 4:
            valkey_client.push_to_list_with_trim(ValkeyKeys.recent_user_speaking_per_api(api_slot_idx), json.dumps(issue_data), MAX_RECENT)

    elif issue_type == 'slop':
        valkey_client.incr(ValkeyKeys.STAT_SLOP_TOTAL)
        valkey_client.incr(ValkeyKeys.stat_slop_per_api(api_slot_idx))
        valkey_client.push_to_list_with_trim(ValkeyKeys.RECENT_SLOP_TOTAL, json.dumps([issue_data, api_slot_idx]), MAX_RECENT)
        if api_slot_idx < 4:
            valkey_client.push_to_list_with_trim(ValkeyKeys.recent_slop_per_api(api_slot_idx), json.dumps(issue_data), MAX_RECENT)

def _record_error(thread_id, api_slot_idx, summary, valkey_client):
    """Helper to record statistics for an API error in Valkey."""
    MAX_RECENT = 10
    valkey_client.incr(ValkeyKeys.STAT_ERROR_TOTAL)
    valkey_client.incr(ValkeyKeys.stat_error_per_api(api_slot_idx))
    err_summary = f"T{thread_id} (API{api_slot_idx+1}): {summary}"
    
    valkey_client.push_to_list_with_trim(ValkeyKeys.RECENT_ERRORS_TOTAL, json.dumps([err_summary, api_slot_idx]), MAX_RECENT)
    if api_slot_idx < 4:
        valkey_client.push_to_list_with_trim(ValkeyKeys.recent_errors_per_api(api_slot_idx), err_summary, MAX_RECENT)
